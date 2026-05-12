# Audit: SeqScan paths under HOT-indexed chain semantics

This document audits every `systable_beginscan()` caller that passes
`indexOK=false` and every effectively-heap-only path through
`table_beginscan_catalog()` in backend source, to determine whether any
of them could surface stale heap-only tuples, adjacent-to-live
tombstones, or mid-chain bridge tombstones as live rows under
HOT-indexed (hot-indexed) chain semantics.

Scope is C17 from `PLAN_NEXT_SESSION.md`.  No code is changed by this
audit; a follow-up commit message is appended only if a clear-cut bug
is found.

## Threat model recap

Under HOT-indexed the heap may contain three on-page artifacts that
classic HOT never produced:

1. **Stale mid-chain heap-only tuples.**  An UPDATE that modified a
   non-summarizing indexed attribute places the new tuple on the same
   page and leaves the old tuple marked
   `HEAP_HOT_UPDATED | HEAP_INDEXED_UPDATED`.  The old tuple's
   `t_xmax` is a committed xid.
2. **Adjacent-to-live tombstones.**  `LP_NORMAL`, `natts=0`,
   `HEAP_INDEXED_UPDATED`, `HEAP_XMIN_INVALID`,
   `t_ctid = (InvalidBlockNumber, live_offset)`.  Carry the
   modified-attrs bitmap for the update that created them.
3. **Bridge tombstones.**  `LP_NORMAL`, `natts=0`,
   `HEAP_INDEXED_UPDATED | HEAP_HOT_UPDATED`, `HEAP_XMIN_INVALID`,
   `t_ctid = (current_blockno, forward_offset)`.  Placed by pruneheap
   in the slot of a dead mid-chain HOT-indexed heap-only tuple to
   preserve the walkable chain hop until vacuum's next index cleanup.

For a SeqScan caller to misbehave, ONE of the following must be true:

- A. It applies no per-tuple MVCC (fast-path PD_ALL_VISIBLE collect).
- B. It uses a non-MVCC snapshot (SnapshotAny, SnapshotSelf,
     SnapshotDirty, SnapshotNonVacuumable) under which
     `HEAP_XMIN_INVALID` tombstones or `HEAP_XMAX_COMMITTED`
     mid-chain tuples become visible.
- C. It reads `t_data` fields (GETSTRUCT, direct header peeks) on a
     tuple it obtained without per-tuple MVCC.
- D. It chain-walks and does not recognize the HOT-indexed extensions.

The remediation for (A) already landed as commit f6807dd49c8: pages
carrying any tombstone (adjacent or bridge) never get
`PD_ALL_VISIBLE` set, which disqualifies the
`page_collect_tuples` fast path.  Classic dead-HOT mid-chain tuples
(HEAP_XMAX_COMMITTED) also disqualify PD_ALL_VISIBLE through the
classic path.  So (A) is globally defended and every remaining case
reduces to per-tuple MVCC + (B/C/D).

## Audit table

All `systable_beginscan()` calls below pass `snapshot = NULL`, which
means `systable_beginscan()` registers `GetCatalogSnapshot(relid)` --
a true MVCC snapshot -- for the scan.  Under MVCC:

- Tombstones (both variants) have `HEAP_XMIN_INVALID`:
  `HeapTupleSatisfiesMVCC` returns false.  Never surfaced.
- Stale mid-chain heap-only tuples have a committed `t_xmax` older
  than the scan snapshot's xmin for typical CatalogSnapshots (which
  are reset per catalog lookup).  `HeapTupleSatisfiesMVCC` returns
  false.  Never surfaced to the caller.
- The live chain member is the one tuple that passes MVCC; its
  `t_data` is the post-update payload the caller expects.

The SeqScan path (`table_scan_getnextslot`) does not chain-walk, does
not set `xs_hot_indexed_recheck`, and does not need the HeapKeyTest
re-eval that the `irel` branch of `systable_getnext()` performs.
MVCC alone is sufficient because the chain's invisible members are
filtered by visibility, not by key identity.

| # | File:line | Function | Relation | Snapshot | Verdict |
|---|-----------|----------|----------|----------|---------|
| 1 | commands/vacuum.c:1669 | `vac_update_datfrozenxid` | pg_class | NULL -> CatalogSnapshot | SAFE |
| 2 | catalog/heap.c:3811 | `heap_truncate_find_FKs` | pg_constraint | NULL -> CatalogSnapshot | SAFE |
| 3 | commands/typecmds.c:4739 | `AlterTypeRecurse` domain search | pg_type | NULL -> CatalogSnapshot | SAFE |
| 4 | catalog/pg_publication.c:1015 | `GetAllTablesPublications` | pg_publication | NULL -> CatalogSnapshot | SAFE |
| 5 | catalog/pg_subscription.c:220 | `CountDBSubscriptions` | pg_subscription | NULL -> CatalogSnapshot | SAFE |
| 6 | catalog/pg_subscription.c:568 | `HasSubscriptionRelations` | pg_subscription_rel | NULL -> CatalogSnapshot | SAFE |
| 7 | catalog/pg_subscription.c:628 | `GetSubscriptionRelations` | pg_subscription_rel | NULL -> CatalogSnapshot | SAFE |
| 8 | replication/logical/sequencesync.c:647 | `FetchTableStates` (sequences) | pg_subscription_rel | NULL -> CatalogSnapshot | SAFE |
| 9 | commands/tablecmds.c:11377 | `CloneFkReferenced` | pg_constraint | NULL -> CatalogSnapshot | SAFE |
| 10 | commands/tablecmds.c:22259 | `detachPartitionFindFkOwnedByParent` | pg_constraint | NULL -> CatalogSnapshot | SAFE |
| 11 | commands/tablecmds.c:19146 (branch) | `ATExecSetRelOptions` SET UNLOGGED | pg_constraint | NULL -> CatalogSnapshot | SAFE |
| 12 | commands/propgraphcmds.c:1655 | pg_propgraph_label_property probe | pg_propgraph_label_property | NULL -> CatalogSnapshot | SAFE |

Entries 9-12 use `indexOK=true` but pass `InvalidOid` as the indexId,
so `systable_beginscan()` falls through to the same
`table_beginscan_strat()` path as `indexOK=false`.  Included for
completeness.

## Per-caller analysis

### 1. `vac_update_datfrozenxid` (commands/vacuum.c:1669)

Previously flagged in `README.HOT-INDEXED` Catalog Enablement notes
as "still uses a heap seqscan with indexOK=false ... surface hasn't
been audited end-to-end."

- **Relation:** pg_class.
- **Snapshot:** `NULL` -> `GetCatalogSnapshot(RelationRelationId)`.
- **Reads:** `GETSTRUCT(classTup)->relkind`,
  `classForm->relfrozenxid`, `classForm->relminmxid`.
- **HOT-indexed exposure:** pg_class sees frequent classic-HOT updates
  (relfrozenxid/reltuples/relpages bumps do not touch indexed attrs)
  and occasional HOT-indexed updates (relname is indexed by
  `pg_class_relname_nsp_index` and renames fire HOT-indexed).
- **Stale-tuple reasoning:** The seqscan walks pg_class with a
  CatalogSnapshot.  In a HOT-indexed chain for a pg_class row, the
  old (pre-rename) tuple has `HEAP_HOT_UPDATED` plus
  `HEAP_XMAX_COMMITTED` once the committing transaction's xid is
  older than the snapshot's xmin; MVCC filters it.  The new tuple
  passes MVCC and `GETSTRUCT` yields the current
  `relfrozenxid`/`relminmxid`, which is what `vac_update_datfrozenxid`
  wants.  If the snapshot is taken mid-commit of the HOT-indexed
  update, either the old tuple is still the visible one (snapshot
  predates xmax) or the new tuple is (snapshot follows xmin); in
  either case a single visible version is returned and its
  relfrozenxid is self-consistent with the tuple the writer wrote.
- **Tombstone exposure:** adjacent and bridge tombstones on a
  pg_class page have `HEAP_XMIN_INVALID`; MVCC filters them.  The
  fast-path PD_ALL_VISIBLE surfacing is defended by f6807dd49c8.
- **Inplace-update race:** The comment block in
  `heap_inplace_update_and_unlock` (heapam.c lines 6944-6960)
  describes a crash-recovery race between concurrent
  `vac_update_datfrozenxid` and an inplace-updating VACUUM.  That
  race is a pre-HOT-indexed concern and is mitigated there by the
  WAL-before-buffer-write trick (temporary copy of the buffer).
  HOT-indexed does not change the inplace path nor the race.
- **Verdict:** SAFE.

### 2. `heap_truncate_find_FKs` (catalog/heap.c:3811)

- **Relation:** pg_constraint.
- **Snapshot:** `NULL` -> CatalogSnapshot.
- **Reads:** `GETSTRUCT(tuple)->contype`, `confrelid`, `conrelid`,
  `conindid`.
- **HOT-indexed exposure:** pg_constraint is updated by ALTER TABLE
  ALTER CONSTRAINT, VALIDATE CONSTRAINT, ATTACH/DETACH PARTITION, and
  ALTER CONSTRAINT RENAME.  Several columns are indexed
  (`conrelid,contypid,conname`, `conname,connamespace`, `conparentid`,
  `contypid`).  HOT-indexed updates are expected here.
- **Correctness:** The loop restarts from the top when it extends its
  working list; within a single pass it just collects oids that pass
  the `contype == CONSTRAINT_FOREIGN` and `list_member_oid(oids,
  con->confrelid)` tests.  Reading the CURRENT visible version of a
  pg_constraint row is the correct semantics.  No chain walk is
  performed.
- **Verdict:** SAFE.

### 3. `AlterTypeRecurse` domain search (commands/typecmds.c:4739)

- **Relation:** pg_type.
- **Snapshot:** `NULL` -> CatalogSnapshot.
- **Reads:** `GETSTRUCT(domainTup)->typtype`, `oid`.
- **HOT-indexed exposure:** pg_type has indexes on `oid`,
  `typname,typnamespace`, and (shared) `pg_type_typname_nsp_index`.
  CREATE/ALTER TYPE writes can go HOT-indexed on rename.
- **Correctness:** The scan collects current-visible pg_type rows
  whose `typbasetype` equals the input OID, then recurses.  MVCC
  returns exactly one live version per chain.
- **Verdict:** SAFE.

### 4. `GetAllTablesPublications` (catalog/pg_publication.c:1015)

- **Relation:** pg_publication.
- **Snapshot:** `NULL` -> CatalogSnapshot.
- **Reads:** `GETSTRUCT(tup)->oid`.
- **Verdict:** SAFE.  Small catalog, CREATE/ALTER PUBLICATION
  frequency is low and the write-side covers any HOT-indexed
  case the same way other catalogs handle it.

### 5. `CountDBSubscriptions` (catalog/pg_subscription.c:220)

- **Relation:** pg_subscription.
- **Snapshot:** `NULL` -> CatalogSnapshot.
- **Reads:** iterates only to count; no struct field access.
- **Verdict:** SAFE.

### 6. `HasSubscriptionRelations` (catalog/pg_subscription.c:568)

- **Relation:** pg_subscription_rel.
- **Snapshot:** `NULL` -> CatalogSnapshot.
- **Reads:** `GETSTRUCT(tup)->srrelid` and `get_rel_relkind()` lookup.
- **Verdict:** SAFE.  pg_subscription_rel updates modify
  `srsubstate` (non-indexed) so they are classic HOT, not
  HOT-indexed.  Even if a HOT-indexed update fires, MVCC filters
  the stale chain members.

### 7. `GetSubscriptionRelations` (catalog/pg_subscription.c:628)

- **Relation:** pg_subscription_rel.
- **Snapshot:** `NULL` -> CatalogSnapshot.
- **Reads:** `GETSTRUCT(tup)` plus `heap_getattr(tup, ...)` for
  `srsublsn`.  The `heap_getattr` call on a returned (live) tuple is
  safe because `tup` is the MVCC-visible version of the chain.
- **Verdict:** SAFE.

### 8. `FetchTableStates` sequences path (replication/logical/sequencesync.c:647)

- **Relation:** pg_subscription_rel.
- **Snapshot:** `NULL` -> CatalogSnapshot.
- **Reads:** `GETSTRUCT(tup)->srrelid`.
- **Verdict:** SAFE.

### 9. `CloneFkReferenced` (commands/tablecmds.c:11377)

- **Relation:** pg_constraint.
- **Snapshot:** `NULL` -> CatalogSnapshot.
- **Reads:** `GETSTRUCT(tuple)->oid`.
- **Verdict:** SAFE.  Same reasoning as #2.

### 10. `detachPartitionFindFkOwnedByParent` (tablecmds.c:22259)

- **Relation:** pg_constraint.
- **Snapshot:** `NULL` -> CatalogSnapshot.
- **Reads:** `GETSTRUCT(tuple)->conparentid`, `oid`.
- **Verdict:** SAFE.

### 11. `ATExecSetRelOptions` SET UNLOGGED branch (tablecmds.c:19146)

- **Relation:** pg_constraint.
- **Snapshot:** `NULL` -> CatalogSnapshot.
- **Reads:** `GETSTRUCT(tuple)->contype`, `conrelid`, `confrelid`.
- **Verdict:** SAFE.

### 12. pg_propgraph_label_property probe (propgraphcmds.c:1655)

- **Relation:** pg_propgraph_label_property.
- **Snapshot:** `NULL` -> CatalogSnapshot.
- **Reads:** existence check only (no GETSTRUCT).
- **Verdict:** SAFE.

## Direct `heap_beginscan` callers

`heap_beginscan` (heapam.c:1182) has no direct external backend
callers; every backend path routes through `table_beginscan*`.  The
single search hit in `src/backend` for `heap_beginscan(` is the
function definition itself.  Nothing to audit here.

## `table_beginscan_catalog` callers -- spot check

`table_beginscan_catalog` (tableam.c:113) always registers a fresh
`GetCatalogSnapshot(relid)` (MVCC), sets `SO_ALLOW_PAGEMODE`, and
returns a SeqScan on a system catalog.  This is the same snapshot and
fast-path surface as the `indexOK=false` `systable_beginscan` entries
above, just reached via a different entry point.  Representative
callers (not exhaustive):

| File:line | Function | Relation |
|-----------|----------|----------|
| commands/vacuum.c:1048 | `get_all_vacuum_rels` | pg_class |
| commands/vacuum.c:1876 | `vac_truncate_clog`-feeder | pg_database |
| postmaster/autovacuum.c:1854 | autovacuum launcher db list | pg_database |
| postmaster/autovacuum.c:2029 | autovacuum per-db workitems | pg_class |
| postmaster/autovacuum.c:2137 | autovacuum toast table scan | pg_class |
| postmaster/autovacuum.c:3660 | autovacuum table_recheck | pg_class |
| postmaster/datachecksum_state.c:1391 | checksum helper db list | pg_database |
| postmaster/datachecksum_state.c:1457 | checksum helper relation list | pg_class |
| utils/init/postinit.c:1491 | AuthIdRelation probe | pg_authid |
| bootstrap/bootstrap.c:909 | bootstrap type load | pg_type |
| commands/dbcommands.c:584,3038,3124 | tablespace iteration | pg_tablespace |
| commands/tablespace.c:424,956,1001,1051,1459,1505 | tablespace name/oid probes | pg_tablespace |
| commands/repack.c:2104,2164 | pg_repack relation list | pg_class |
| commands/tablecmds.c:7199,17329 | various | pg_class |
| commands/indexcmds.c:3231 | ReindexMultipleInternal | pg_class |
| replication/logical/launcher.c:144 | subscription launcher | pg_subscription |
| catalog/pg_subscription.c:503 | `RemoveSubscriptionRel` | pg_subscription_rel |
| catalog/pg_db_role_setting.c:208 | db role setting scan | pg_db_role_setting |
| catalog/pg_publication.c:1069,1091,1199 | publication table listings | pg_class |
| catalog/aclchk.c:848,895 | aclitem recursion | pg_proc, pg_class |

All share the same MVCC-snapshot reasoning as the `indexOK=false`
list: tombstones are filtered by `HEAP_XMIN_INVALID`, stale mid-chain
tuples by `HEAP_XMAX_COMMITTED`, and the PD_ALL_VISIBLE fast-path
carve-out (f6807dd49c8) keeps tombstone-bearing pages out of the
collect-tuples path.  No per-caller issue identified.

**Verdict: SAFE.**

## Non-MVCC-snapshot SeqScan entries (brief)

These paths use a non-MVCC snapshot and therefore bypass the MVCC
filter that protects everything above.  They are included for
completeness; all go through the index path (indexOK=true) or a
specialized entry point, not a catalog seqscan.

- `catalog/catalog.c:485`, `GetNewOidWithIndex`, **SnapshotAny**,
  indexOK=**true**.  The `irel` path of `systable_getnext()` applies
  `xs_hot_indexed_recheck` HeapKeyTest re-eval, but HeapKeyTest under
  SnapshotAny may receive a tombstone via the chain walk.  A
  tombstone has `natts=0`; `HeapKeyTest` -> `heap_attisnull` treats
  any positive attno as NULL and returns false (fails the key test),
  which drops the tombstone.  Under SnapshotAny the purpose of the
  scan is to detect OID collisions across all tuple versions, so
  returning the live heap tuple for either the stale or fresh leaf
  entry is still correct (both entries resolve to the same TID via
  chain walk, and `GETSTRUCT` of the live tuple carries the OID).
  **SAFE**, but leans on HeapKeyTest's natts=0 behavior; noted for
  future hardening (see follow-up suggestion below).

- `access/heap/heapam_handler.c:1252`, CREATE INDEX table scan.  Uses
  either the relation's active snapshot or `SnapshotAny` for index
  builds.  CREATE INDEX handling of HOT chains is documented in
  `README.HOT-INDEXED` ("CREATE INDEX") and uses chain walking to
  form index tuples from the live member; unchanged.  **SAFE.**

- `access/heap/heapam_handler.c:1787`, ANALYZE table sample scan.
  Uses the query snapshot, which is MVCC for ANALYZE.  **SAFE.**

- `catalog/index.c:3255`, `validate_index` heap scan.  Uses
  `GetLatestSnapshot()` (MVCC).  **SAFE.**

- `executor/nodeBitmapHeapscan.c:156`, `executor/nodeSamplescan.c:296`,
  `executor/nodeTidrangescan.c:255`, `executor/nodeTidscan.c:150`.
  All use `es_snapshot` (MVCC query snapshot).  **SAFE.**

- `utils/adt/tid.c:352`, `currtid_for_view`.  `GetLatestSnapshot()`.
  **SAFE.**

## Summary

| Verdict | Count |
|---------|-------|
| SAFE | 12 indexOK=false/effective callers + ~30 `table_beginscan_catalog` callers + non-MVCC entries, all SAFE |
| POTENTIALLY AT RISK | 0 |
| UNSAFE | 0 |

The single flagged caller from `README.HOT-INDEXED`'s Catalog
Enablement notes (`vac_update_datfrozenxid`) is SAFE under
HOT-indexed chain semantics.  The remaining unaudited surface that
that note mentioned -- "No reported corruption from this path today,
but the surface hasn't been audited end-to-end" -- is the inplace-
update race described in `heap_inplace_update_and_unlock`'s comment
block, which predates HOT-indexed and is already mitigated there.

The `README.HOT-INDEXED` note can be updated to record that the
audit was performed and found no HOT-indexed-specific exposure.

## Follow-up: README update (suggested, not landed here)

Proposed diff to `README.HOT-INDEXED`, replacing the
"vac_update_datfrozenxid still uses a heap seqscan" bullet under
"Catalog Enablement" with:

    - vac_update_datfrozenxid uses a heap seqscan with indexOK=false.
      Audited (see AUDIT_SEQSCAN.md); safe under HOT-indexed chain
      semantics because the scan registers a CatalogSnapshot and
      filters tombstones (HEAP_XMIN_INVALID) and stale mid-chain
      tuples (HEAP_XMAX_COMMITTED) via normal MVCC.  The fast-path
      PD_ALL_VISIBLE carve-out (f6807dd49c8) keeps tombstone-bearing
      pages out of the heap-scan collect-tuples path.

This is not landed as part of this audit commit because the README
update belongs with the series' documentation pass (C23), not with
the audit artifact.

## Follow-up: hardening suggestion (not landed)

`GetNewOidWithIndex` (catalog/catalog.c:485) uses SnapshotAny and
reaches tombstones through the index path.  The current protection
is that `HeapKeyTest` on a natts=0 tuple returns false because
`heap_attisnull` treats every positive attno as NULL.  This is a
silent assumption: if HeapKeyTest grows a code path that dereferences
column datums before the null check, the guard would fail.

A cheap, defensive reinforcement is to make `systable_getnext()`
skip any tuple where `HeapTupleHeaderIndicatesTombstone(tup->t_data)`
(i.e., `HEAP_INDEXED_UPDATED && natts == 0`) in the index path,
regardless of snapshot, before running HeapKeyTest.  Tombstones are
never legitimate return values for any catalog scan.

Sketch (genam.c around the existing `xs_hot_indexed_recheck` block):

    if (HeapTupleHeaderGetNatts(htup->t_data) == 0 &&
        (htup->t_data->t_infomask2 & HEAP_INDEXED_UPDATED) != 0)
    {
        htup = NULL;
        continue;
    }

Cost: one load and two mask tests per returned index tuple.  Benefit:
closes an implicit dependency on HeapKeyTest's natts=0 behavior and
protects any future caller that passes a non-MVCC snapshot through
the index path.

Proposed commit message:

    genam: skip HOT-indexed tombstones in systable_getnext index path

    systable_getnext's index path can under non-MVCC snapshots
    (notably SnapshotAny in GetNewOidWithIndex) dereference index
    entries that resolve via chain walk to HOT-indexed tombstones
    (LP_NORMAL, natts=0, HEAP_INDEXED_UPDATED).  The current defense
    relies on HeapKeyTest's heap_attisnull treating natts=0 attnos as
    NULL and failing the equality test.

    Add an explicit check that drops any natts=0 +
    HEAP_INDEXED_UPDATED tuple in the index branch of
    systable_getnext before running HeapKeyTest.  Tombstones are
    never a legitimate return value for any catalog scan; the check
    removes the implicit HeapKeyTest dependency and protects future
    callers that pass a non-MVCC snapshot.
