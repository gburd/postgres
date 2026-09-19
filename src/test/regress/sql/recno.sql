--
-- RECNO table access method: CRUD, stable-ctid indexed UPDATE, rollback.
--
CREATE TABLE recno_t (id int, v text) USING recno;

-- confirm the AM
SELECT a.amname FROM pg_class c JOIN pg_am a ON a.oid = c.relam
WHERE c.relname = 'recno_t';

INSERT INTO recno_t VALUES (1, 'one'), (2, 'two'), (3, 'three');
SELECT id, v FROM recno_t ORDER BY id;

CREATE INDEX recno_t_id_idx ON recno_t (id);

-- ctid before an indexed-column UPDATE
SELECT ctid, id FROM recno_t WHERE id = 2;

-- indexed-column UPDATE must stay IN PLACE (stable ctid) via delete-marking
UPDATE recno_t SET id = 22 WHERE id = 2;

-- ctid after: must be unchanged
SELECT ctid, id, v FROM recno_t WHERE id = 22;

-- index correctness: old key gone, new key reachable via index scan
SET enable_seqscan = off;
SELECT id, v FROM recno_t WHERE id = 2;
SELECT id, v FROM recno_t WHERE id = 22;
RESET enable_seqscan;

-- DELETE
DELETE FROM recno_t WHERE id = 3;
SELECT id, v FROM recno_t ORDER BY id;

-- ROLLBACK must revert INSERT + UPDATE + DELETE
BEGIN;
  INSERT INTO recno_t VALUES (99, 'temp');
  UPDATE recno_t SET v = 'CHANGED' WHERE id = 1;
  DELETE FROM recno_t WHERE id = 22;
  SELECT id, v FROM recno_t ORDER BY id;
ROLLBACK;
SELECT id, v FROM recno_t ORDER BY id;

DROP TABLE recno_t;

--
-- BUG 2 guard: range predicate after in-place SHRINK must NOT shift rows.
-- An in-place UPDATE that shrinks a row keeps the on-page slot length but
-- records the true payload in t_len; the reader must deform by t_natts /
-- per-attribute lengths, not the padded slot length, so WHERE id BETWEEN a
-- AND b returns exactly a..b with each row's own data, and index and seq
-- scans agree.
--
CREATE TABLE recno_shrink (id int PRIMARY KEY, t text) USING recno;
INSERT INTO recno_shrink SELECT g, repeat('W', 150) FROM generate_series(1, 12) g;
-- shrink every row in place to a DIFFERENT shorter length (length = id)
UPDATE recno_shrink SET t = repeat('a', id) WHERE id BETWEEN 1 AND 12;

-- count is unaffected
SELECT count(*) FROM recno_shrink;

-- seq scan: exactly ids 1..5, each with length = id and correct data
SET enable_seqscan = on; SET enable_indexscan = off; SET enable_bitmapscan = off;
SELECT id, length(t), t FROM recno_shrink WHERE id BETWEEN 1 AND 5 ORDER BY id;

-- index scan: identical result (no shift, index == heap)
SET enable_seqscan = off; SET enable_indexscan = on; SET enable_bitmapscan = off;
SELECT id, length(t), t FROM recno_shrink WHERE id BETWEEN 1 AND 5 ORDER BY id;

-- whole-table consistency: every row's length equals its id and data matches
SET enable_seqscan = on; SET enable_indexscan = off;
SELECT id, length(t) = id AS len_ok, t = repeat('a', id) AS data_ok
  FROM recno_shrink ORDER BY id;
RESET enable_seqscan; RESET enable_indexscan; RESET enable_bitmapscan;

DROP TABLE recno_shrink;
