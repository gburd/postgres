-- Test - dropping a relation must not let its cached pages resurface through
-- a later relation that reuses the same relfilenode / blocks.

CREATE EXTENSION IF NOT EXISTS pg_buffercache;
DROP TABLE IF EXISTS dr_old, dr_new, dr_flood;
CREATE TABLE dr_old (id int, payload text) WITH (autovacuum_enabled = off);
INSERT INTO dr_old SELECT g, 'old-' || md5(g::text) FROM generate_series(1, 5000) g;
SELECT count(*) FROM dr_old;   -- cache dr_old's pages
DROP TABLE dr_old;
-- New relation plus flood pressure; the new relation may land on dr_old's
-- recycled relfilenode/blocks.
CREATE TABLE dr_new (id int, payload text) WITH (autovacuum_enabled = off);
INSERT INTO dr_new SELECT g, 'new-' || md5(g::text) FROM generate_series(1, 5000) g;
CREATE TABLE dr_flood (id int, payload text) WITH (autovacuum_enabled = off);
INSERT INTO dr_flood SELECT g, md5(g::text) FROM generate_series(1, 30000) g;
SELECT count(*) FROM dr_flood;
-- No dr_new row may carry a dropped-relation payload; all must be correct.
SELECT count(*) AS stale_old_rows FROM dr_new WHERE payload LIKE 'old-%';
SELECT count(*) AS wrong_new_rows FROM dr_new WHERE payload <> 'new-' || md5(id::text);

DROP TABLE dr_new, dr_flood;
