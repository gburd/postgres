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

-- The catalog and the base-table flag exist.  The compiler that fills
-- them comes later; for now nothing is recorded.
SELECT count(*) FROM pg_matview_ivm;
SELECT relhasivm FROM pg_class WHERE oid = 'ivm_t'::regclass;

-- Other options can still be changed.
ALTER MATERIALIZED VIEW ivm_m SET (fillfactor = 50);
SELECT reloptions FROM pg_class WHERE oid = 'ivm_m'::regclass;
ALTER MATERIALIZED VIEW ivm_m RESET (fillfactor);

DROP MATERIALIZED VIEW ivm_m;
DROP TABLE ivm_t;
