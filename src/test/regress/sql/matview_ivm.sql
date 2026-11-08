--
-- Incremental maintenance of materialized views
--

CREATE TABLE ivm_t (k int, v int);

-- The option is accepted on a materialized view and stored.
CREATE MATERIALIZED VIEW ivm_m WITH (incremental_view_maintenance) AS
  SELECT k, count(*) FROM ivm_t GROUP BY k;
SELECT reloptions FROM pg_class WHERE oid = 'ivm_m'::regclass;

-- It is rejected on anything else.
CREATE TABLE ivm_x WITH (incremental_view_maintenance) AS SELECT 1 AS a;
CREATE TABLE ivm_y (a int) WITH (incremental_view_maintenance);
ALTER TABLE ivm_t SET (incremental_view_maintenance = true);
-- An explicit false is the default, and harmless anywhere.
CREATE TABLE ivm_z (a int) WITH (incremental_view_maintenance = false);
DROP TABLE ivm_z;

-- It cannot be changed after creation.
ALTER MATERIALIZED VIEW ivm_m SET (incremental_view_maintenance = false);
ALTER MATERIALIZED VIEW ivm_m RESET (incremental_view_maintenance);
CREATE MATERIALIZED VIEW ivm_n AS SELECT k FROM ivm_t;
ALTER MATERIALIZED VIEW ivm_n SET (incremental_view_maintenance = true);
DROP MATERIALIZED VIEW ivm_n;

-- Other options can still be changed.
ALTER MATERIALIZED VIEW ivm_m SET (fillfactor = 50);
SELECT reloptions FROM pg_class WHERE oid = 'ivm_m'::regclass;
ALTER MATERIALIZED VIEW ivm_m RESET (fillfactor);

-- The view is compiled: one pg_matview_ivm row, its base table recorded and
-- flagged.
SELECT ivmclass, ivmbaserels[0]::regclass
  FROM pg_matview_ivm WHERE ivmrelid = 'ivm_m'::regclass;
SELECT relhasivm FROM pg_class WHERE oid = 'ivm_t'::regclass;
-- The base table cannot be dropped while the view needs it.
DROP TABLE ivm_t;
-- Dropping the view removes its row; the flag stays, set lazily.
DROP MATERIALIZED VIEW ivm_m;
SELECT count(*) FROM pg_matview_ivm;
SELECT relhasivm FROM pg_class WHERE oid = 'ivm_t'::regclass;

--
-- What the compiler accepts
--
CREATE TABLE ivm_s (a int, b text, c int, n numeric, d date, f float8);
INSERT INTO ivm_s VALUES (1, 'x', 10, 1.5, '2026-01-01', 0.5),
                         (1, 'y', 20, 2.5, '2026-01-02', 1.5),
                         (2, 'x', 30, NULL, NULL, NULL);

CREATE FUNCTION ivm_classes() RETURNS TABLE (view regclass, ivmclass int2)
LANGUAGE sql AS $$
  SELECT ivmrelid::regclass, ivmclass FROM pg_matview_ivm ORDER BY 1::text
$$;

-- invertible aggregates, two grouping columns: class 1
CREATE MATERIALIZED VIEW ivm_a1 WITH (incremental_view_maintenance) AS
  SELECT a, b, count(*) AS n_all, count(c) AS n_c, sum(c), avg(n), sum(n) AS sum_n
  FROM ivm_s GROUP BY a, b;
-- grouping columns in any order
CREATE MATERIALIZED VIEW ivm_a2 WITH (incremental_view_maintenance) AS
  SELECT sum(c), b, a FROM ivm_s GROUP BY b, a;
-- a grouping column repeated, GROUP BY by position, an expression argument
CREATE MATERIALIZED VIEW ivm_a8 WITH (incremental_view_maintenance) AS
  SELECT a, a AS a2, sum(c * 2 + 1) FROM ivm_s GROUP BY 1;
-- a column alias list
CREATE MATERIALIZED VIEW ivm_a9 (k, total) WITH (incremental_view_maintenance) AS
  SELECT a, count(*) FROM ivm_s GROUP BY a;
-- no GROUP BY: one row
CREATE MATERIALIZED VIEW ivm_a3 WITH (incremental_view_maintenance) AS
  SELECT count(*), sum(c) FROM ivm_s;
-- a WHERE clause
CREATE MATERIALIZED VIEW ivm_a4 WITH (incremental_view_maintenance) AS
  SELECT a, count(*) FROM ivm_s WHERE c > 10 AND b <> 'z' GROUP BY a;
-- min and max are recomputed per group: class 2
CREATE MATERIALIZED VIEW ivm_a5 WITH (incremental_view_maintenance) AS
  SELECT a, min(c), max(d) FROM ivm_s GROUP BY a;
-- interval sums and averages
CREATE MATERIALIZED VIEW ivm_a6 WITH (incremental_view_maintenance) AS
  SELECT a, sum((d - date '2026-01-01') * interval '1 day') AS s,
            avg(interval '1 hour' * c) AS m
  FROM ivm_s GROUP BY a;
-- WITH NO DATA: compiled, not populated; REFRESH populates it
CREATE MATERIALIZED VIEW ivm_a7 WITH (incremental_view_maintenance) AS
  SELECT a, count(*) FROM ivm_s GROUP BY a WITH NO DATA;
SELECT * FROM ivm_a7;
REFRESH MATERIALIZED VIEW ivm_a7;
SELECT * FROM ivm_a7 ORDER BY a;

SELECT * FROM ivm_classes();
SELECT * FROM ivm_a1 ORDER BY a, b;
SELECT * FROM ivm_a3;
SELECT * FROM ivm_a6 ORDER BY a;
SELECT * FROM ivm_a8 ORDER BY a;
-- an aborted CREATE leaves no catalog row
BEGIN;
CREATE MATERIALIZED VIEW ivm_a10 WITH (incremental_view_maintenance) AS
  SELECT a, count(*) FROM ivm_s GROUP BY a;
ROLLBACK;
SELECT count(*) FROM pg_matview_ivm WHERE ivmrelid::regclass::text = 'ivm_a10';

--
-- What the compiler rejects
--
CREATE TABLE ivm_u (a int, c int);
CREATE VIEW ivm_v AS SELECT a, c FROM ivm_s;
CREATE TABLE ivm_p (a int) PARTITION BY LIST (a);
CREATE UNLOGGED TABLE ivm_ul (a int);

CREATE MATERIALIZED VIEW ivm_r WITH (incremental_view_maintenance) AS
  SELECT ivm_s.a, count(*) FROM ivm_s JOIN ivm_u USING (a) GROUP BY ivm_s.a;
CREATE MATERIALIZED VIEW ivm_r WITH (incremental_view_maintenance) AS
  SELECT ivm_s.a, count(*) FROM ivm_s, ivm_u GROUP BY ivm_s.a;
CREATE MATERIALIZED VIEW ivm_r WITH (incremental_view_maintenance) AS
  SELECT 1 AS one, count(*) FROM (SELECT 1) s;
CREATE MATERIALIZED VIEW ivm_r WITH (incremental_view_maintenance) AS
  SELECT a, count(*) FROM ivm_v GROUP BY a;
CREATE MATERIALIZED VIEW ivm_r WITH (incremental_view_maintenance) AS
  SELECT a, count(*) FROM (SELECT a FROM ivm_s) s GROUP BY a;
CREATE MATERIALIZED VIEW ivm_r WITH (incremental_view_maintenance) AS
  SELECT a, count(*) FROM ivm_p GROUP BY a;
CREATE MATERIALIZED VIEW ivm_r WITH (incremental_view_maintenance) AS
  SELECT a, count(*) FROM ivm_ul GROUP BY a;
CREATE MATERIALIZED VIEW ivm_r WITH (incremental_view_maintenance) AS
  SELECT count(*) FROM generate_series(1, 3);
CREATE MATERIALIZED VIEW ivm_r WITH (incremental_view_maintenance) AS
  SELECT count(*) FROM (VALUES (1)) v(x);
CREATE MATERIALIZED VIEW ivm_r WITH (incremental_view_maintenance) AS
  WITH w AS (SELECT a FROM ivm_s) SELECT a, count(*) FROM w GROUP BY a;
CREATE MATERIALIZED VIEW ivm_r WITH (incremental_view_maintenance) AS
  SELECT a FROM ivm_s UNION SELECT a FROM ivm_u;
CREATE MATERIALIZED VIEW ivm_r WITH (incremental_view_maintenance) AS
  SELECT DISTINCT a FROM ivm_s;
CREATE MATERIALIZED VIEW ivm_r WITH (incremental_view_maintenance) AS
  SELECT DISTINCT ON (a) a, c FROM ivm_s;
CREATE MATERIALIZED VIEW ivm_r WITH (incremental_view_maintenance) AS
  SELECT a, count(*) FROM ivm_s GROUP BY a HAVING count(*) > 1;
CREATE MATERIALIZED VIEW ivm_r WITH (incremental_view_maintenance) AS
  SELECT a, count(*) FROM ivm_s GROUP BY a ORDER BY a;
CREATE MATERIALIZED VIEW ivm_r WITH (incremental_view_maintenance) AS
  SELECT a, count(*) FROM ivm_s GROUP BY a LIMIT 1;
CREATE MATERIALIZED VIEW ivm_r WITH (incremental_view_maintenance) AS
  SELECT a, count(*) OVER () FROM ivm_s;
CREATE MATERIALIZED VIEW ivm_r WITH (incremental_view_maintenance) AS
  SELECT a, b, count(*) FROM ivm_s GROUP BY GROUPING SETS ((a), (b));
CREATE MATERIALIZED VIEW ivm_r WITH (incremental_view_maintenance) AS
  SELECT a, count(*) FROM ivm_s WHERE c IN (SELECT c FROM ivm_u) GROUP BY a;
CREATE MATERIALIZED VIEW ivm_r WITH (incremental_view_maintenance) AS
  SELECT a, count(*) FROM ivm_s TABLESAMPLE system (50) GROUP BY a;
CREATE MATERIALIZED VIEW ivm_r WITH (incremental_view_maintenance) AS
  SELECT a, c FROM ivm_s;
CREATE MATERIALIZED VIEW ivm_r WITH (incremental_view_maintenance) AS
  SELECT a + 1, count(*) FROM ivm_s GROUP BY a + 1;
CREATE MATERIALIZED VIEW ivm_r WITH (incremental_view_maintenance) AS
  SELECT a, c, count(*) FROM ivm_s GROUP BY a, c, b;
CREATE MATERIALIZED VIEW ivm_r WITH (incremental_view_maintenance) AS
  SELECT a, count(*) + 1 FROM ivm_s GROUP BY a;
CREATE MATERIALIZED VIEW ivm_r WITH (incremental_view_maintenance) AS
  SELECT a, string_agg(b, ',') FROM ivm_s GROUP BY a;
CREATE MATERIALIZED VIEW ivm_r WITH (incremental_view_maintenance) AS
  SELECT a, sum(f) FROM ivm_s GROUP BY a;
CREATE MATERIALIZED VIEW ivm_r WITH (incremental_view_maintenance) AS
  SELECT a, count(DISTINCT c) FROM ivm_s GROUP BY a;
CREATE MATERIALIZED VIEW ivm_r WITH (incremental_view_maintenance) AS
  SELECT a, sum(c ORDER BY c) FROM ivm_s GROUP BY a;
CREATE MATERIALIZED VIEW ivm_r WITH (incremental_view_maintenance) AS
  SELECT a, count(*) FILTER (WHERE c > 1) FROM ivm_s GROUP BY a;
CREATE MATERIALIZED VIEW ivm_r WITH (incremental_view_maintenance) AS
  SELECT a, percentile_cont(0.5) WITHIN GROUP (ORDER BY c) FROM ivm_s GROUP BY a;
CREATE MATERIALIZED VIEW ivm_r WITH (incremental_view_maintenance) AS
  SELECT a, count(*) FROM ivm_s WHERE c > random() GROUP BY a;
CREATE MATERIALIZED VIEW ivm_r WITH (incremental_view_maintenance) AS
  SELECT a, count(*) FROM ivm_s WHERE d > now() GROUP BY a;
CREATE MATERIALIZED VIEW ivm_r WITH (incremental_view_maintenance) AS
  SELECT a, max(ctid) FROM ivm_s GROUP BY a;
CREATE MATERIALIZED VIEW ivm_r WITH (incremental_view_maintenance) AS
  SELECT n, count(*) FROM ivm_s GROUP BY n;
CREATE MATERIALIZED VIEW ivm_r WITH (incremental_view_maintenance) AS
  SELECT f, count(*) FROM ivm_s GROUP BY f;
CREATE MATERIALIZED VIEW ivm_r WITH (incremental_view_maintenance) AS
  SELECT generate_series(1, a), count(*) FROM ivm_s GROUP BY a;
-- nothing was created by the failures
SELECT * FROM ivm_classes();

DROP MATERIALIZED VIEW ivm_a1, ivm_a2, ivm_a3, ivm_a4, ivm_a5, ivm_a6, ivm_a7,
  ivm_a8, ivm_a9;
DROP FUNCTION ivm_classes();
DROP VIEW ivm_v;
DROP TABLE ivm_s, ivm_u, ivm_p, ivm_ul, ivm_t;
