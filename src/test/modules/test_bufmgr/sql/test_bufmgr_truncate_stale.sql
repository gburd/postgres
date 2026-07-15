-- Test - TRUNCATE must not leave stale cached pages that later become visible.
CREATE EXTENSION IF NOT EXISTS pg_buffercache;
DROP TABLE IF EXISTS tr_rel;
CREATE TABLE tr_rel (id int, payload text) WITH (autovacuum_enabled = off);
INSERT INTO tr_rel SELECT g, 'v1-' || md5(g::text) FROM generate_series(1, 5000) g;
SELECT count(*) FROM tr_rel;   -- cache the v1 pages
TRUNCATE tr_rel;
-- Repopulate with DIFFERENT (v2) contents, extending the relation again.
INSERT INTO tr_rel SELECT g, 'v2-' || md5(g::text) FROM generate_series(1, 5000) g;
-- No row may carry a v1 payload, and every row must be its correct v2 value.
SELECT count(*) AS stale_v1_rows FROM tr_rel WHERE payload LIKE 'v1-%';
SELECT count(*) AS wrong_v2_rows FROM tr_rel WHERE payload <> 'v2-' || md5(id::text);

DROP TABLE tr_rel;
