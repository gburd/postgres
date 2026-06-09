--
-- HOT_INDEXED_UPDATES
-- Test HOT-indexed update (hot-indexed), aka HOT-indexed, behaviour
--
-- Every UPDATE in this file modifies at least one non-summarizing
-- indexed attribute.  On a pre-hot-indexed server all of these would be
-- non-HOT; on the hot-indexed branch each eligible update stays on-page and
-- inserts into only the indexes whose attributes actually changed.
--
-- We verify four things:
--   (A) pg_stat counters: HOT and hot-indexed counts increment as expected
--   (B) index lookups return the new value and not the stale value
--       for EQUALITY queries (the read-side leaf-key recheck drops a
--       leaf whose covered attribute changed on the way to the live tuple)
--   (C) pg_relation_hot_indexed_stats reports the HOT-indexed versions we expect
--   (D) **RANGE/INEQUALITY** queries return the correct number of
--       tuples -- this is the class of bugs where a stale btree
--       entry's key is still reachable via a looser scan key; the
--       leaf-key recheck drops the stale arrival because the index's
--       attribute changed between that leaf's target and the live tuple
--

CREATE EXTENSION IF NOT EXISTS pageinspect;

CREATE OR REPLACE FUNCTION get_hot_count(rel_name text)
RETURNS TABLE (updates BIGINT, hot BIGINT) AS $$
DECLARE rel_oid oid;
BEGIN
    rel_oid := rel_name::regclass::oid;
    updates := COALESCE(pg_stat_get_tuples_updated(rel_oid), 0) +
               COALESCE(pg_stat_get_xact_tuples_updated(rel_oid), 0);
    hot := COALESCE(pg_stat_get_tuples_hot_updated(rel_oid), 0) +
           COALESCE(pg_stat_get_xact_tuples_hot_updated(rel_oid), 0);
    RETURN NEXT;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION get_hi_count(rel_name text)
RETURNS TABLE (updates BIGINT, hot BIGINT, hot_idx BIGINT) AS $$
DECLARE rel_oid oid;
BEGIN
    rel_oid := rel_name::regclass::oid;
    updates := COALESCE(pg_stat_get_tuples_updated(rel_oid), 0) +
               COALESCE(pg_stat_get_xact_tuples_updated(rel_oid), 0);
    hot := COALESCE(pg_stat_get_tuples_hot_updated(rel_oid), 0) +
           COALESCE(pg_stat_get_xact_tuples_hot_updated(rel_oid), 0);
    hot_idx := COALESCE(pg_stat_get_tuples_hot_indexed_updated(rel_oid), 0) +
           COALESCE(pg_stat_get_xact_tuples_hot_indexed_updated(rel_oid), 0);
    RETURN NEXT;
END;
$$ LANGUAGE plpgsql;


-- ---------------------------------------------------------------------------
-- 1. Basic hot-indexed: modifying an indexed column stays HOT and counts as hot-indexed
-- ---------------------------------------------------------------------------
CREATE TABLE hi_basic (
    id int PRIMARY KEY,
    indexed_col int,
    non_indexed_col text
) WITH (fillfactor = 50);
CREATE INDEX hi_basic_idx ON hi_basic(indexed_col);

INSERT INTO hi_basic VALUES (1, 100, 'initial');

-- Pre-hot-indexed this would be non-HOT.  Under hot-indexed it's HOT-indexed; both the
-- HOT counter and the hot-indexed counter advance.
UPDATE hi_basic SET indexed_col = 150 WHERE id = 1;
SELECT pg_stat_force_next_flush();
SELECT * FROM get_hi_count('hi_basic');

-- The new value is reachable via the index.
SET enable_seqscan = off;
EXPLAIN (COSTS OFF) SELECT id, indexed_col FROM hi_basic WHERE indexed_col = 150;
SELECT id, indexed_col FROM hi_basic WHERE indexed_col = 150;

-- The old value is not reachable through this index: the stale btree
-- entry (indexed_col=100) walks to the current tuple via the hot-indexed hop,
-- nodeIndexscan re-evaluates `indexed_col = 100` against the current
-- tuple (indexed_col=150), and the row is correctly dropped.  This is
-- the equality-lookup case the leaf-key recheck handles.
EXPLAIN (COSTS OFF) SELECT id FROM hi_basic WHERE indexed_col = 100;
SELECT id FROM hi_basic WHERE indexed_col = 100;
RESET enable_seqscan;

-- pg_relation_hot_indexed_stats sees one HOT-indexed version, zero HOT redirects (the
-- chain has not yet been pruned so no LP_REDIRECT exists).
SELECT n_hot_indexed, n_chains, avg_chain_len, max_chain_len
FROM pg_relation_hot_indexed_stats('hi_basic');

DROP TABLE hi_basic;

-- ---------------------------------------------------------------------------
-- 2. RANGE/INEQUALITY correctness after hot-indexed on an indexed column
--
-- This is the test class that catches the hot-indexed false-dup bug: a stale
-- btree entry whose key value still satisfies the range predicate,
-- reachable via the hot-indexed chain hop.
--
-- To exercise the bug we must force an IndexScan plan (the
-- IndexOnlyScan path permissively drops every hot-indexed-reachable index-only
-- hit; the BitmapHeapScan path dedups by TID).  We include a payload
-- column not present in the PK so the planner must heap-fetch.
--
-- The read-side leaf-key recheck makes the IndexScan return the correct
-- count of 1: the stale entry ('1','5') chain-walks to the live tuple across
-- the b-changing hop, and because the PK covers b the overlap is non-empty, so
-- the stale leaf is dropped.  The fresh entry ('1','15') points directly at the
-- live tuple (no hop after it) and is kept.  The ORDER BY likewise returns the
-- single live row.
-- ---------------------------------------------------------------------------
CREATE TABLE hi_range (
    a int,
    b int,
    payload text,
    PRIMARY KEY (a, b)
) WITH (fillfactor = 50);

INSERT INTO hi_range VALUES (1, 5, 'hi');

-- hot-indexed update on the second PK column: stale btree entry ('1','5')
-- remains, new entry ('1','15') inserted.  The stale entry points at
-- the chain root; the fresh entry points directly at the new
-- heap-only tuple.
UPDATE hi_range SET b = 15 WHERE a = 1 AND b = 5;

SET enable_seqscan = off;
SET enable_bitmapscan = off;

-- IndexScan: payload IS NOT NULL forces heap fetch, no IndexOnlyScan.
-- The stale ('1','5') leaf is dropped by the leaf-key recheck, so this
-- returns 1.
EXPLAIN (COSTS OFF)
SELECT count(*) FROM hi_range WHERE a = 1 AND b < 100 AND payload IS NOT NULL;
SELECT count(*) FROM hi_range WHERE a = 1 AND b < 100 AND payload IS NOT NULL;
SELECT a, b FROM hi_range WHERE a = 1 AND payload IS NOT NULL ORDER BY b;

-- IndexOnlyScan: the page holds a preserved HOT-indexed member so it is never all-visible; IOS
-- performs the heap fetch and the leaf-key recheck drops the stale ('1','5')
-- leaf, so count = 1.
EXPLAIN (COSTS OFF) SELECT count(*) FROM hi_range WHERE a = 1 AND b < 100;
SELECT count(*) FROM hi_range WHERE a = 1 AND b < 100;

-- BitmapHeapScan: TID dedup collapses the stale and fresh hits.
SET enable_indexscan = off;
SET enable_indexonlyscan = off;
RESET enable_bitmapscan;
EXPLAIN (COSTS OFF) SELECT count(*) FROM hi_range WHERE a = 1 AND b < 100;
SELECT count(*) FROM hi_range WHERE a = 1 AND b < 100;
RESET enable_indexscan;
RESET enable_indexonlyscan;

-- SeqScan: reads the heap directly, sees exactly one live tuple.
RESET enable_seqscan;
SET enable_indexscan = off;
SET enable_indexonlyscan = off;
SET enable_bitmapscan = off;
EXPLAIN (COSTS OFF) SELECT count(*) FROM hi_range WHERE a = 1 AND b < 100;
SELECT count(*) FROM hi_range WHERE a = 1 AND b < 100;
RESET enable_indexscan;
RESET enable_indexonlyscan;
RESET enable_bitmapscan;

-- Same shape on a secondary (non-PK) btree: another hot-indexed update on b.
CREATE INDEX hi_range_b_idx ON hi_range(b);
UPDATE hi_range SET b = 25 WHERE a = 1 AND b = 15;

SET enable_seqscan = off;
SET enable_bitmapscan = off;
-- IndexScan path on the secondary index; same fix applies.
SELECT count(*) FROM hi_range WHERE b BETWEEN 0 AND 100 AND payload IS NOT NULL;
RESET enable_seqscan;
RESET enable_bitmapscan;

DROP TABLE hi_range;

-- ---------------------------------------------------------------------------
-- 3. All-or-none on a multi-indexed table: hot-indexed only touches indexes
--    whose attributes changed
-- ---------------------------------------------------------------------------
CREATE TABLE hi_multi (
    id int PRIMARY KEY,
    col_a int,
    col_b int,
    col_c int,
    non_indexed text
) WITH (fillfactor = 50);
CREATE INDEX hi_multi_a_idx ON hi_multi(col_a);
CREATE INDEX hi_multi_b_idx ON hi_multi(col_b);
CREATE INDEX hi_multi_c_idx ON hi_multi(col_c);

INSERT INTO hi_multi VALUES (1, 10, 20, 30, 'initial');

-- col_a only: under hot-indexed this is HOT-indexed, and only hi_multi_a_idx
-- gets a new entry.  hi_multi_b_idx / hi_multi_c_idx keep pointing
-- at the chain root.
UPDATE hi_multi SET col_a = 15 WHERE id = 1;
SELECT pg_stat_force_next_flush();
SELECT * FROM get_hi_count('hi_multi');

-- Lookups on all three indexes return the row.
SET enable_seqscan = off;
SELECT id FROM hi_multi WHERE col_a = 15;
SELECT id FROM hi_multi WHERE col_b = 20;
SELECT id FROM hi_multi WHERE col_c = 30;

-- Old col_a value is unreachable by equality (stale entry dropped by the
-- read-side leaf-key recheck).
SELECT id FROM hi_multi WHERE col_a = 10;
RESET enable_seqscan;

DROP TABLE hi_multi;

-- ---------------------------------------------------------------------------
-- 4. Multi-column btree: hot-indexed on part of a composite key
-- ---------------------------------------------------------------------------
CREATE TABLE hi_composite (
    id int PRIMARY KEY,
    col_a int,
    col_b int,
    data text
) WITH (fillfactor = 50);
CREATE INDEX hi_composite_ab_idx ON hi_composite(col_a, col_b);

INSERT INTO hi_composite VALUES (1, 10, 20, 'data');

-- col_a is part of the composite key: hot-indexed.
UPDATE hi_composite SET col_a = 15;
SELECT pg_stat_force_next_flush();
SELECT * FROM get_hi_count('hi_composite');

-- Reset and then update col_b (also part of the key).
UPDATE hi_composite SET col_a = 10;
UPDATE hi_composite SET col_b = 25;
SELECT pg_stat_force_next_flush();
SELECT * FROM get_hi_count('hi_composite');

DROP TABLE hi_composite;

-- ---------------------------------------------------------------------------
-- 5. Partial index: status transition out-of-predicate
--
-- Both old and new status values are outside the partial predicate,
-- so the index does not need a new entry.  Under hot-indexed the update is
-- HOT-indexed and no index insert occurs.
-- ---------------------------------------------------------------------------
CREATE TABLE hi_partial (
    id int PRIMARY KEY,
    status text,
    data text
) WITH (fillfactor = 50);
CREATE INDEX hi_partial_active_idx ON hi_partial(status) WHERE status = 'active';

INSERT INTO hi_partial VALUES (1, 'active', 'data1');
INSERT INTO hi_partial VALUES (2, 'inactive', 'data2');
INSERT INTO hi_partial VALUES (3, 'deleted', 'data3');

-- out -> out transition on status.  hot-indexed keeps this on-page; the
-- partial index is not touched.
UPDATE hi_partial SET status = 'deleted' WHERE id = 2;
SELECT pg_stat_force_next_flush();
SELECT * FROM get_hi_count('hi_partial');

-- The partial index still correctly answers "active" queries.
SELECT id, status FROM hi_partial WHERE status = 'active';

DROP TABLE hi_partial;

-- ---------------------------------------------------------------------------
-- 6. Partition: hot-indexed inside one partition
-- ---------------------------------------------------------------------------
CREATE TABLE hi_part (
    id int,
    partition_key int,
    indexed_col int,
    data text,
    PRIMARY KEY (id, partition_key)
) PARTITION BY RANGE (partition_key);
CREATE TABLE hi_part_1 PARTITION OF hi_part
    FOR VALUES FROM (1) TO (100) WITH (fillfactor = 50);
CREATE INDEX hi_part_idx ON hi_part(indexed_col);

INSERT INTO hi_part VALUES (1, 50, 100, 'data');

UPDATE hi_part SET indexed_col = 150 WHERE id = 1;
SELECT pg_stat_force_next_flush();
SELECT * FROM get_hi_count('hi_part_1');

SET enable_seqscan = off;
SELECT id FROM hi_part WHERE indexed_col = 150;
SELECT id FROM hi_part WHERE indexed_col = 100;
RESET enable_seqscan;

DROP TABLE hi_part CASCADE;

-- ---------------------------------------------------------------------------
-- 7. Trigger modifies indexed column: hot-indexed, not non-HOT
-- ---------------------------------------------------------------------------
CREATE TABLE hi_trigger (
    id int PRIMARY KEY,
    triggered_col int,
    data text
) WITH (fillfactor = 50);
CREATE INDEX hi_trigger_idx ON hi_trigger(triggered_col);

CREATE OR REPLACE FUNCTION hi_trigger_bump()
RETURNS TRIGGER AS $$
BEGIN
    NEW.triggered_col = NEW.triggered_col + 1;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER before_update_bump
    BEFORE UPDATE ON hi_trigger
    FOR EACH ROW
    EXECUTE FUNCTION hi_trigger_bump();

INSERT INTO hi_trigger VALUES (1, 100, 'initial');

-- UPDATE's SET clause doesn't touch the indexed column, but the
-- trigger modifies it via heap_modify_tuple.  hot-indexed must detect this
-- and keep the tuple on-page (HEAP_INDEXED_UPDATED) plus a new btree entry.
UPDATE hi_trigger SET data = 'updated' WHERE id = 1;
SELECT pg_stat_force_next_flush();
SELECT * FROM get_hi_count('hi_trigger');
SELECT triggered_col FROM hi_trigger WHERE id = 1;

-- New value reachable.
SET enable_seqscan = off;
SELECT id FROM hi_trigger WHERE triggered_col = 101;
SELECT id FROM hi_trigger WHERE triggered_col = 100;
RESET enable_seqscan;

DROP TABLE hi_trigger CASCADE;
DROP FUNCTION hi_trigger_bump();

-- ---------------------------------------------------------------------------
-- 8. JSONB expression index: HOT-indexed is not yet supported on expression
--    indexes, so the update falls back to a non-HOT update (hot_idx = 0).
--    Reads stay correct.
-- ---------------------------------------------------------------------------
CREATE TABLE hi_jsonb (
    id int PRIMARY KEY,
    data jsonb
) WITH (fillfactor = 50);
CREATE INDEX hi_jsonb_name_idx ON hi_jsonb ((data->>'name'));

INSERT INTO hi_jsonb VALUES (1, '{"name":"Alice","age":30}');

-- Changing the indexed expression's value (name): expression indexes are not
-- yet supported, so this is a non-HOT update.
UPDATE hi_jsonb SET data = jsonb_set(data, '{name}', '"Alice2"') WHERE id = 1;
SELECT pg_stat_force_next_flush();
SELECT * FROM get_hi_count('hi_jsonb');

SET enable_seqscan = off;
SELECT id FROM hi_jsonb WHERE data->>'name' = 'Alice2';
SELECT id FROM hi_jsonb WHERE data->>'name' = 'Alice';
RESET enable_seqscan;

DROP TABLE hi_jsonb;

-- ---------------------------------------------------------------------------
-- 9. GIN index with changed extracted keys: hot-indexed
-- ---------------------------------------------------------------------------
CREATE TABLE hi_gin (
    id int PRIMARY KEY,
    tags text[]
) WITH (fillfactor = 50);
CREATE INDEX hi_gin_tags_idx ON hi_gin USING gin (tags);

INSERT INTO hi_gin VALUES (1, ARRAY['tag1', 'tag2']);

-- Adding a tag yields a different extracted-key set: hot-indexed.
UPDATE hi_gin SET tags = ARRAY['tag1', 'tag2', 'tag5'] WHERE id = 1;
SELECT pg_stat_force_next_flush();
SELECT * FROM get_hi_count('hi_gin');

SET enable_seqscan = off;
SELECT id FROM hi_gin WHERE tags @> ARRAY['tag5'];
RESET enable_seqscan;

DROP TABLE hi_gin;

-- ---------------------------------------------------------------------------
-- 10. Per-index HOT-indexed counters: skipped vs matched
--
-- A table with two independent secondary indexes.  An UPDATE touches a
-- column covered by only one of them; the HOT-indexed path must insert
-- into that one index and skip the other.  pg_stat_all_indexes reports
-- matched>0 on the updated index and skipped>0 on the untouched index.
-- ---------------------------------------------------------------------------
CREATE TABLE hotidx_perindex (
    id int PRIMARY KEY,
    a int,
    b int
) WITH (fillfactor = 50);
CREATE INDEX hotidx_perindex_a ON hotidx_perindex(a);
CREATE INDEX hotidx_perindex_b ON hotidx_perindex(b);

INSERT INTO hotidx_perindex VALUES (1, 100, 200);

-- Modify only column a.  HOT-indexed inserts into hotidx_perindex_a and
-- skips hotidx_perindex_b (primary key indrelid is the table itself and
-- also unchanged, so it counts as skipped too).
UPDATE hotidx_perindex SET a = 101 WHERE id = 1;

-- Force flush of pending stats to the shared entry.
SELECT pg_stat_force_next_flush();

SELECT indexrelname,
       n_tup_hot_indexed_upd_matched AS matched,
       n_tup_hot_indexed_upd_skipped AS skipped
  FROM pg_stat_all_indexes
 WHERE relname = 'hotidx_perindex'
 ORDER BY indexrelname;

-- A second UPDATE touching only b inverts the assignment.
UPDATE hotidx_perindex SET b = 201 WHERE id = 1;
SELECT pg_stat_force_next_flush();

SELECT indexrelname,
       n_tup_hot_indexed_upd_matched AS matched,
       n_tup_hot_indexed_upd_skipped AS skipped
  FROM pg_stat_all_indexes
 WHERE relname = 'hotidx_perindex'
 ORDER BY indexrelname;

-- Invariant: matched + skipped == owning table's n_tup_hot_indexed_upd.
SELECT indexrelname,
       n_tup_hot_indexed_upd_matched + n_tup_hot_indexed_upd_skipped AS total,
       (SELECT n_tup_hot_indexed_upd FROM pg_stat_all_tables
         WHERE relname = 'hotidx_perindex') AS table_hot_idx_upd
  FROM pg_stat_all_indexes
 WHERE relname = 'hotidx_perindex'
 ORDER BY indexrelname;

-- Boolean assertion of the same invariant.  This is the canonical form
-- reviewers asked for: every index entry is either matched (the index
-- got a fresh insert this UPDATE) or skipped (HOT-indexed correctly
-- avoided an insert because the index's attrs did not change).  If the
-- two counters drift apart from the table-level n_tup_hot_indexed_upd we
-- have either lost a per-index increment or double-counted one.
SELECT bool_and((n_tup_hot_indexed_upd_matched + n_tup_hot_indexed_upd_skipped) =
                (SELECT n_tup_hot_indexed_upd FROM pg_stat_all_tables
                  WHERE relname = 'hotidx_perindex'))
         AS perindex_invariant_holds
  FROM pg_stat_all_indexes
 WHERE relname = 'hotidx_perindex';

DROP TABLE hotidx_perindex;

-- ---------------------------------------------------------------------------
-- 11. Long hot-loop UPDATE stays compact and HOT-indexed
--
-- A long run of HOT-indexed UPDATEs to a single row stays compact: prune
-- collapses each dead version to a redirect to the live tuple and reuses its
-- slot, so the row never leaves its original page and the chain does not grow
-- unbounded.  Every UPDATE that changes the indexed column (and leaves another
-- index, here the PK, unchanged) takes the HOT-indexed path.
-- ---------------------------------------------------------------------------
CREATE TABLE hi_chaincap (
    id int PRIMARY KEY,
    a int
) WITH (fillfactor = 10);
CREATE INDEX hi_chaincap_a_idx ON hi_chaincap(a);

INSERT INTO hi_chaincap VALUES (1, 0);

DO $$
DECLARE
    i int;
BEGIN
    FOR i IN 1 .. 200 LOOP
        UPDATE hi_chaincap SET a = i WHERE id = 1;
    END LOOP;
END $$;

-- After 200 UPDATEs the row's value is 200.
SELECT a FROM hi_chaincap WHERE id = 1;

-- Every UPDATE took the HOT-indexed path (the PK index is unchanged, so it is
-- skipped), so n_tup_hot_indexed_upd advanced.
SELECT pg_stat_force_next_flush();
SELECT hot_idx > 0 AS hot_indexed_fired
  FROM get_hi_count('hi_chaincap');

-- The heap stayed compact: prune+collapse reclaimed the dead versions, so the
-- single live row still occupies just one page.
SELECT relpages <= 1 AS heap_stayed_compact
  FROM pg_class WHERE relname = 'hi_chaincap';

DROP TABLE hi_chaincap;

-- ---------------------------------------------------------------------------
-- 12. Reclamation of a collapsed HOT-indexed chain by prune
--
-- A dead HOT-indexed chain member is preserved at prune time (its stale leaf
-- may still exist) and the chain collapses to an LP_REDIRECT forwarder; the
-- index cleanup pass then sweeps the stale leaf, and a second VACUUM reclaims
-- the now-unreferenced member and re-points the redirect.  So the chain is
-- fully reclaimed after the second VACUUM, not the first.
-- ---------------------------------------------------------------------------
CREATE TABLE hi_reclaim (
    id int PRIMARY KEY,
    a int
) WITH (fillfactor = 50);
CREATE INDEX hi_reclaim_a_idx ON hi_reclaim(a);

INSERT INTO hi_reclaim VALUES (1, 100);
-- Generate a collapsed chain via a HOT-indexed update.
UPDATE hi_reclaim SET a = 200 WHERE id = 1;
SELECT n_hot_indexed >= 1 AS hot_indexed_present_before_reclaim
  FROM pg_relation_hot_indexed_stats('hi_reclaim');

-- Delete the live tuple.  The first VACUUM collapses the dead chain and sweeps
-- the stale leaf; the second reclaims the now-unreferenced members.
DELETE FROM hi_reclaim WHERE id = 1;
VACUUM hi_reclaim;
VACUUM hi_reclaim;

SELECT n_hot_indexed AS hot_indexed_after_reclaim,
       n_chains AS chains_after_reclaim
  FROM pg_relation_hot_indexed_stats('hi_reclaim');

DROP TABLE hi_reclaim;

-- ---------------------------------------------------------------------------
-- 13. Page with a preserved HOT-indexed member is never marked all-visible
--
-- pruneheap deliberately leaves PD_ALL_VISIBLE clear on any page that still
-- carries a preserved HOT-indexed member: an index-only scan must heap-fetch
-- through the chain so the read-side leaf-key recheck can filter stale btree
-- entries.
--
-- We force the freeze path with VACUUM (FREEZE, DISABLE_PAGE_SKIPPING) and
-- then read pd_flags via pageinspect.page_header.  The page must still carry
-- a HOT-indexed member (n_hot_indexed > 0) AND must not have PD_ALL_VISIBLE
-- (0x0004).
-- ---------------------------------------------------------------------------
CREATE TABLE hi_vm (
    id int PRIMARY KEY,
    a int
) WITH (fillfactor = 50);
CREATE INDEX hi_vm_a_idx ON hi_vm(a);

INSERT INTO hi_vm VALUES (1, 1);
-- Two HOT-indexed updates leave a multi-hop chain, so a preserved HOT-indexed
-- member remains on the page after prune, which is what this test needs.
UPDATE hi_vm SET a = 2 WHERE id = 1;
UPDATE hi_vm SET a = 3 WHERE id = 1;

-- Force the all-visible bit decision: VACUUM with DISABLE_PAGE_SKIPPING
-- considers every page; FREEZE pushes hint bits hard.  After this, any
-- page bearing a preserved HOT-indexed member must still report all_visible = 0.
VACUUM (FREEZE, DISABLE_PAGE_SKIPPING) hi_vm;

SELECT n_hot_indexed >= 1 AS hot_indexed_present
  FROM pg_relation_hot_indexed_stats('hi_vm');

-- PD_ALL_VISIBLE = 0x0004.  Must be 0 on a page with a preserved member.
SELECT (flags & 4) = 0 AS not_marked_all_visible
  FROM page_header(get_raw_page('hi_vm', 0));

DROP TABLE hi_vm;

-- ---------------------------------------------------------------------------
-- 14. Cycle-key dedup: column rename a -> b -> a stays correct
--
-- A rename does not rewrite heap or index entries; it only updates the
-- catalog.  The relcache invalidation must trigger a fresh attribute
-- bitmap and the HOT-indexed predicate must compare attribute *numbers*,
-- not attribute *names*.  After two renames that net to identity, every
-- subsequent UPDATE must continue to drive the HOT-indexed path.
-- ---------------------------------------------------------------------------
CREATE TABLE hi_cycle (
    id int PRIMARY KEY,
    a int
) WITH (fillfactor = 50);
CREATE INDEX hi_cycle_a_idx ON hi_cycle(a);

INSERT INTO hi_cycle VALUES (1, 100);

-- Cycle the column name and confirm both intermediate forms drive HOT-indexed.
ALTER TABLE hi_cycle RENAME COLUMN a TO b;
UPDATE hi_cycle SET b = 200 WHERE id = 1;
SELECT pg_stat_force_next_flush();
SELECT hot_idx > 0 AS hot_indexed_after_first_rename
  FROM get_hi_count('hi_cycle');

ALTER TABLE hi_cycle RENAME COLUMN b TO a;
UPDATE hi_cycle SET a = 300 WHERE id = 1;
-- Lookup via the index returns the current value, not any of the
-- pre-rename values.
SET enable_seqscan = off;
SELECT id, a FROM hi_cycle WHERE a = 300;
SELECT id FROM hi_cycle WHERE a = 100;
SELECT id FROM hi_cycle WHERE a = 200;
RESET enable_seqscan;

DROP TABLE hi_cycle;

-- ---------------------------------------------------------------------------
-- 15. Summarizing-only column UPDATE produces CLASSIC, not INDEXED
--
-- HeapUpdateHotAllowable returns HEAP_HOT_MODE_CLASSIC when every
-- modified indexed attribute is covered only by summarizing indexes.
-- A BRIN-only column is the canonical case: the BRIN index gets a
-- new summary entry via aminsert, but no per-update btree entry is
-- needed and HOT-indexed does not fire.  The signal is
-- n_tup_hot_upd > 0 with n_tup_hot_indexed_upd unchanged.
-- ---------------------------------------------------------------------------
CREATE TABLE hi_brin (
    id int PRIMARY KEY,
    bcol int
) WITH (fillfactor = 50);
CREATE INDEX hi_brin_idx ON hi_brin USING brin(bcol);

INSERT INTO hi_brin VALUES (1, 100);

-- Capture the HOT-indexed counter before, drive a BRIN-only update,
-- and assert that classic HOT advanced while HOT-indexed did not.
SELECT pg_stat_force_next_flush();
SELECT hot_idx AS hot_idx_before FROM get_hi_count('hi_brin') \gset
UPDATE hi_brin SET bcol = 200 WHERE id = 1;
SELECT pg_stat_force_next_flush();
SELECT (hot - 0) > 0 AS classic_hot_fired,
       hot_idx = :hot_idx_before AS hot_indexed_did_not_fire
  FROM get_hi_count('hi_brin');

-- The BRIN index sees the new value via aminsert.
SELECT bcol FROM hi_brin WHERE id = 1;

DROP TABLE hi_brin;

-- ---------------------------------------------------------------------------
-- 16. UNIQUE index on a type where image equality != operator equality
--
-- numeric 1.0 and 1.00 are equal under the btree opclass but have
-- different on-disk images.  A HOT-indexed update 1.0 -> 1.00 inserts a
-- fresh leaf carrying the live image and leaves a stale leaf for 1.0
-- (the hop's modified-attrs bitmap marks k changed, since modified-column
-- detection is image-based).  A later INSERT of a value equal under the
-- opclass must still be detected as a duplicate: the unique check reaches
-- the live tuple through the fresh leaf, which points directly at it (no hop
-- after it, so the overlap is empty and the leaf is a genuine conflict); the
-- stale 1.0 leaf is skipped because the k-changing hop overlaps the unique
-- index's attribute.
-- ---------------------------------------------------------------------------
CREATE TABLE hi_unum (k numeric UNIQUE, j int) WITH (fillfactor = 50);
CREATE INDEX hi_unum_j ON hi_unum(j);             -- 2nd indexed attr, kept fixed
INSERT INTO hi_unum VALUES (1.0, 100);
UPDATE hi_unum SET k = 1.00 WHERE j = 100;        -- HOT-indexed: 1.0 -> 1.00
SELECT n_hot_indexed > 0 AS made_hot_indexed
  FROM pg_relation_hot_indexed_stats('hi_unum');
-- A numerically-equal insert must conflict (the fresh leaf catches it):
INSERT INTO hi_unum VALUES (1.0, 1);              -- expect duplicate key error
-- A genuinely different value is accepted:
INSERT INTO hi_unum VALUES (2.0, 2);
SELECT k, j FROM hi_unum ORDER BY j;
DROP TABLE hi_unum;

-- ---------------------------------------------------------------------------
-- 17. CREATE INDEX and REINDEX over live HOT-indexed chains
--
-- A freshly built or rebuilt index must reflect current values, never a
-- stale chain member: the build scans live tuples only and points each
-- HOT-indexed live tuple's entry at its own TID, so the new entries have no
-- hop after them and the leaf-key recheck keeps them.
-- ---------------------------------------------------------------------------
CREATE TABLE hi_reindex (id int PRIMARY KEY, a int, b int) WITH (fillfactor = 50);
CREATE INDEX hi_reindex_a ON hi_reindex(a);
INSERT INTO hi_reindex SELECT g, g, g FROM generate_series(1, 6) g;
UPDATE hi_reindex SET a = a + 100;                -- HOT-indexed on a
UPDATE hi_reindex SET a = a + 100;                -- again -> longer chains
SELECT n_hot_indexed > 0 AS made_hot_indexed
  FROM pg_relation_hot_indexed_stats('hi_reindex');
-- Build a NEW index and REINDEX the existing one over the live chains.
CREATE INDEX hi_reindex_b ON hi_reindex(b);
REINDEX INDEX hi_reindex_a;
SET enable_seqscan = off;
SELECT id, a FROM hi_reindex WHERE a = 204;       -- current value -> id 4
SELECT count(*) FROM hi_reindex WHERE a = 4;      -- obsolete value -> 0
SELECT id FROM hi_reindex WHERE b = 2;            -- via freshly built index -> 2
RESET enable_seqscan;
DROP TABLE hi_reindex;

-- ---------------------------------------------------------------------------
-- 18. DROP every index over live HOT-indexed chains, then VACUUM
--
-- After all indexes are dropped, heap pages may still carry preserved
-- HOT-indexed members left by earlier updates.  VACUUM of such a no-index
-- relation must complete without error, and reads must stay correct via the
-- redirect forwarders.
-- ---------------------------------------------------------------------------
CREATE TABLE hi_dropidx (id int PRIMARY KEY, a int) WITH (fillfactor = 50);
CREATE INDEX hi_dropidx_a ON hi_dropidx(a);
INSERT INTO hi_dropidx SELECT g, g FROM generate_series(1, 6) g;
UPDATE hi_dropidx SET a = a + 100;                -- HOT-indexed on a
UPDATE hi_dropidx SET a = a + 100;                -- again -> longer chains
SELECT n_hot_indexed > 0 AS made_hot_indexed
  FROM pg_relation_hot_indexed_stats('hi_dropidx');
-- Drop every index, leaving preserved HOT-indexed members with no index to sweep.
DROP INDEX hi_dropidx_a;
ALTER TABLE hi_dropidx DROP CONSTRAINT hi_dropidx_pkey;
-- Must not crash on the no-index path; two passes exercise the second-pass
-- reclaim guard as well.
VACUUM hi_dropidx;
VACUUM hi_dropidx;
-- Reads remain correct after the indexes are gone.
SELECT id, a FROM hi_dropidx ORDER BY id;
DROP TABLE hi_dropidx;

-- ---------------------------------------------------------------------------
-- 19. Re-collapse of a data-redirect chain across partial VACUUMs
--
-- A chain that collapses to a HOT-indexed data redirect, is vacuumed with
-- INDEX_CLEANUP off (so the stale leaves and the redirect survive), then
-- receives further HOT-indexed updates that re-collapse the chain and
-- re-point the redirect at a new live tuple, must not leave the redirect
-- dangling.  A subsequent full VACUUM must complete without error, leave the
-- heap consistent (verify_heapam reports nothing), and reads must stay
-- correct.  (Regression: an earlier revision crashed reclaiming a mid-chain
-- member while a data redirect still pointed past it.)
-- ---------------------------------------------------------------------------
CREATE EXTENSION IF NOT EXISTS amcheck;
CREATE TABLE hi_recollapse (id int PRIMARY KEY, a int) WITH (fillfactor = 50);
CREATE INDEX hi_recollapse_a ON hi_recollapse(a);
INSERT INTO hi_recollapse VALUES (1, 1);
-- First chain: two HOT-indexed updates, then prune to a data redirect while
-- leaving the stale btree leaves in place (INDEX_CLEANUP off).
UPDATE hi_recollapse SET a = 2 WHERE id = 1;
UPDATE hi_recollapse SET a = 3 WHERE id = 1;
VACUUM (INDEX_CLEANUP off) hi_recollapse;
-- Re-collapse: more HOT-indexed updates extend the chain past the redirect
-- target; the next prune re-points the data redirect at the new first live
-- tuple and extends its union.
UPDATE hi_recollapse SET a = 4 WHERE id = 1;
UPDATE hi_recollapse SET a = 5 WHERE id = 1;
VACUUM (INDEX_CLEANUP off) hi_recollapse;
-- Full vacuum now reclaims the dead chain; the re-pointed redirect must not
-- dangle.  Two passes also exercise the redirect re-point second pass.
VACUUM hi_recollapse;
VACUUM hi_recollapse;
-- Heap must be structurally consistent (no rows == no corruption).
SELECT * FROM verify_heapam('hi_recollapse');
SET enable_seqscan = off;
SELECT id, a FROM hi_recollapse WHERE a = 5;     -- current value -> id 1
SELECT count(*) FROM hi_recollapse WHERE a = 3;  -- obsolete value -> 0
RESET enable_seqscan;
SELECT id, a FROM hi_recollapse ORDER BY id;
DROP TABLE hi_recollapse;

-- ---------------------------------------------------------------------------
-- 20. Index deletion over an entry that points at a data-redirect root
--
-- A data redirect is an LP_REDIRECT that carries a bitmap, so it reports
-- lp_len > 0 (ItemIdHasStorage true) even though it is not a normal tuple.
-- index_delete_check_htid must treat it as a redirect, not read its blob as a
-- HeapTupleHeader.  Reproduce: collapse a chain root to a data redirect while
-- keeping the stale leaf that points at it (INDEX_CLEANUP off), then insert
-- many duplicates of the stale key so btree bottom-up deletion runs
-- heap_index_delete_tuples over that stale entry.
-- ---------------------------------------------------------------------------
CREATE TABLE hi_iddel (id int, a int) WITH (fillfactor = 50);
CREATE INDEX hi_iddel_a ON hi_iddel(a);
INSERT INTO hi_iddel VALUES (1, 1);
UPDATE hi_iddel SET a = a + 1 WHERE id = 1;          -- HOT-indexed
UPDATE hi_iddel SET a = a + 1 WHERE id = 1;          -- multi-hop chain
VACUUM (INDEX_CLEANUP off) hi_iddel;                 -- root -> data redirect, keep stale a=1 leaf
-- Many duplicates of the stale key fill the leaf and trigger bottom-up
-- deletion, which feeds the stale a=1 entry (htid -> the data-redirect root)
-- to heap_index_delete_tuples.  Must not crash or misread the blob.
INSERT INTO hi_iddel SELECT g, 1 FROM generate_series(2, 3000) g;
VACUUM hi_iddel;
SELECT * FROM verify_heapam('hi_iddel');
SET enable_seqscan = off;
SELECT id, a FROM hi_iddel WHERE id = 1;             -- current value -> a = 3
RESET enable_seqscan;
DROP TABLE hi_iddel;

-- ---------------------------------------------------------------------------
-- Cleanup
-- ---------------------------------------------------------------------------
DROP FUNCTION get_hi_count(text);
DROP FUNCTION get_hot_count(text);
DROP EXTENSION pageinspect;
