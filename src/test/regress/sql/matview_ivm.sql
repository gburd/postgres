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
  SELECT ivmrelid::regclass, ivmclass FROM pg_matview_ivm
  ORDER BY ivmrelid::regclass::text
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
-- no GROUP BY and no aggregates: the view would be one row per base row
CREATE MATERIALIZED VIEW ivm_r WITH (incremental_view_maintenance) AS
  SELECT FROM ivm_s;
-- a WINDOW clause, even unused by any window function
CREATE MATERIALIZED VIEW ivm_r WITH (incremental_view_maintenance) AS
  SELECT count(*) AS c FROM ivm_s WINDOW w AS (ORDER BY count(*));
-- a user-defined aggregate, even one with a sort operator
CREATE FUNCTION ivm_larger(int, int) RETURNS int LANGUAGE sql
  AS 'SELECT greatest($1, $2)';
CREATE AGGREGATE ivm_max (int) (sfunc = ivm_larger, stype = int, sortop = >);
CREATE MATERIALIZED VIEW ivm_r WITH (incremental_view_maintenance) AS
  SELECT a, ivm_max(c) FROM ivm_s GROUP BY a;
DROP AGGREGATE ivm_max (int);
DROP FUNCTION ivm_larger(int, int);
-- a system catalog
CREATE MATERIALIZED VIEW ivm_r WITH (incremental_view_maintenance) AS
  SELECT relkind, count(*) FROM pg_class GROUP BY relkind;
-- an inheritance parent with children, and a partition
CREATE TABLE ivm_par (a int);
CREATE TABLE ivm_chi () INHERITS (ivm_par);
CREATE MATERIALIZED VIEW ivm_r WITH (incremental_view_maintenance) AS
  SELECT a, count(*) FROM ivm_par GROUP BY a;
-- ONLY the parent is fine
CREATE MATERIALIZED VIEW ivm_r WITH (incremental_view_maintenance) AS
  SELECT a, count(*) FROM ONLY ivm_par GROUP BY a;
DROP MATERIALIZED VIEW ivm_r;
CREATE TABLE ivm_p1 PARTITION OF ivm_p FOR VALUES IN (1);
CREATE MATERIALIZED VIEW ivm_r WITH (incremental_view_maintenance) AS
  SELECT a, count(*) FROM ivm_p1 GROUP BY a;
-- row-level security enabled, whether or not policies apply to the creator
CREATE TABLE ivm_rls (a int, c int);
ALTER TABLE ivm_rls ENABLE ROW LEVEL SECURITY;
CREATE MATERIALIZED VIEW ivm_r WITH (incremental_view_maintenance) AS
  SELECT a, count(*) FROM ivm_rls GROUP BY a;
-- a virtual generated column, as a key or an argument
CREATE TABLE ivm_gen (a int, g int GENERATED ALWAYS AS (a * 2) VIRTUAL,
                      s int GENERATED ALWAYS AS (a * 3) STORED);
CREATE MATERIALIZED VIEW ivm_r WITH (incremental_view_maintenance) AS
  SELECT g, count(*) FROM ivm_gen GROUP BY g;
CREATE MATERIALIZED VIEW ivm_r WITH (incremental_view_maintenance) AS
  SELECT a, sum(g) FROM ivm_gen GROUP BY a;
-- a stored generated column is an ordinary column; changing its expression
-- rewrites the table outside the executor, so it is refused while the view
-- exists (a plain materialized view over it allows it)
CREATE MATERIALIZED VIEW ivm_r WITH (incremental_view_maintenance) AS
  SELECT s, count(*) FROM ivm_gen GROUP BY s;
ALTER TABLE ivm_gen ALTER COLUMN s SET EXPRESSION AS (a * 4);
DROP MATERIALIZED VIEW ivm_r;
CREATE MATERIALIZED VIEW ivm_r AS SELECT s, count(*) FROM ivm_gen GROUP BY s;
ALTER TABLE ivm_gen ALTER COLUMN s SET EXPRESSION AS (a * 4);
DROP MATERIALIZED VIEW ivm_r;
-- nothing was created by the failures
SELECT * FROM ivm_classes();

--
-- Privileges: the creator needs MAINTAIN on (or ownership of) the base
-- table, since maintenance runs in, and can fail, other users' writes.
--
CREATE ROLE regress_ivm_owner;
CREATE ROLE regress_ivm_user;
CREATE TABLE ivm_priv (a int, c int);
ALTER TABLE ivm_priv OWNER TO regress_ivm_owner;
GRANT CREATE ON SCHEMA public TO regress_ivm_user;
GRANT SELECT ON ivm_priv TO regress_ivm_user;
SET ROLE regress_ivm_user;
-- SELECT is enough for an ordinary materialized view ...
CREATE MATERIALIZED VIEW ivm_pm AS SELECT a, count(*) FROM ivm_priv GROUP BY a;
DROP MATERIALIZED VIEW ivm_pm;
-- ... but not for an incremental one
CREATE MATERIALIZED VIEW ivm_pm WITH (incremental_view_maintenance) AS
  SELECT a, count(*) FROM ivm_priv GROUP BY a;
RESET ROLE;
GRANT MAINTAIN ON ivm_priv TO regress_ivm_user;
SET ROLE regress_ivm_user;
CREATE MATERIALIZED VIEW ivm_pm WITH (incremental_view_maintenance) AS
  SELECT a, count(*) FROM ivm_priv GROUP BY a;
DROP MATERIALIZED VIEW ivm_pm;
RESET ROLE;
REVOKE MAINTAIN ON ivm_priv FROM regress_ivm_user;
-- membership in pg_maintain grants MAINTAIN on every table
GRANT pg_maintain TO regress_ivm_user;
SET ROLE regress_ivm_user;
CREATE MATERIALIZED VIEW ivm_pm WITH (incremental_view_maintenance) AS
  SELECT a, count(*) FROM ivm_priv GROUP BY a;
DROP MATERIALIZED VIEW ivm_pm;
RESET ROLE;
REVOKE pg_maintain FROM regress_ivm_user;
-- MAINTAIN without SELECT is not enough either, even WITH NO DATA
GRANT pg_maintain TO regress_ivm_user;
REVOKE SELECT ON ivm_priv FROM regress_ivm_user;
SET ROLE regress_ivm_user;
CREATE MATERIALIZED VIEW ivm_pm WITH (incremental_view_maintenance) AS
  SELECT a, count(*) FROM ivm_priv GROUP BY a WITH NO DATA;
RESET ROLE;
REVOKE pg_maintain FROM regress_ivm_user;
-- the owner needs nothing granted
GRANT CREATE ON SCHEMA public TO regress_ivm_owner;
SET ROLE regress_ivm_owner;
CREATE MATERIALIZED VIEW ivm_pm WITH (incremental_view_maintenance) AS
  SELECT a, count(*) FROM ivm_priv GROUP BY a;
DROP MATERIALIZED VIEW ivm_pm;
RESET ROLE;
DROP TABLE ivm_priv;
REVOKE CREATE ON SCHEMA public FROM regress_ivm_user, regress_ivm_owner;
DROP ROLE regress_ivm_user, regress_ivm_owner;

DROP MATERIALIZED VIEW ivm_a1, ivm_a2, ivm_a3, ivm_a4, ivm_a5, ivm_a6, ivm_a7,
  ivm_a8, ivm_a9;
DROP FUNCTION ivm_classes();
DROP VIEW ivm_v;
--
-- DDL that would make a base table something a view could not be created
-- over is refused while the view exists
--
CREATE TABLE ivm_ddl (a int, c int);
CREATE MATERIALIZED VIEW ivm_ddl_m WITH (incremental_view_maintenance) AS
  SELECT a, count(*) FROM ivm_ddl GROUP BY a;
ALTER TABLE ivm_ddl SET UNLOGGED;
ALTER TABLE ivm_ddl ENABLE ROW LEVEL SECURITY;
CREATE TABLE ivm_ddl_c () INHERITS (ivm_ddl);
CREATE TABLE ivm_ddl_c (a int, c int);
ALTER TABLE ivm_ddl_c INHERIT ivm_ddl;
CREATE TABLE ivm_ddl_p (a int, c int) PARTITION BY LIST (a);
ALTER TABLE ivm_ddl_p ATTACH PARTITION ivm_ddl FOR VALUES IN (1);
-- the view itself can still be indexed, and the table altered otherwise
CREATE INDEX ON ivm_ddl_m (a);
ALTER TABLE ivm_ddl ADD COLUMN d int;
ALTER TABLE ivm_ddl SET LOGGED;
-- after the view is dropped, all of it is allowed
DROP MATERIALIZED VIEW ivm_ddl_m;
ALTER TABLE ivm_ddl ENABLE ROW LEVEL SECURITY;
ALTER TABLE ivm_ddl_c ADD COLUMN d int;
ALTER TABLE ivm_ddl_c INHERIT ivm_ddl;
DROP TABLE ivm_ddl, ivm_ddl_c, ivm_ddl_p;

DROP TABLE ivm_s, ivm_u, ivm_p, ivm_ul, ivm_t, ivm_par, ivm_chi, ivm_rls, ivm_gen;

-- leave an incrementally maintained view for the pg_upgrade test
CREATE TABLE ivm_upgrade (k int, v int);
INSERT INTO ivm_upgrade VALUES (1, 10), (1, 20), (2, 5);
CREATE MATERIALIZED VIEW ivm_upgrade_m WITH (incremental_view_maintenance) AS
  SELECT k, count(*), sum(v) FROM ivm_upgrade GROUP BY k;
