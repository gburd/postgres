--
-- Validate full HEAP feature compatibility for RECNO
-- Tests all features that HEAP supports to ensure RECNO works identically
--

-- =============================================
-- Window functions
-- =============================================

CREATE TABLE recno_window (
    id serial,
    department text,
    salary numeric(10,2),
    name text
) USING recno;

INSERT INTO recno_window (department, salary, name) VALUES
    ('eng', 100000, 'Alice'),
    ('eng', 120000, 'Bob'),
    ('eng', 110000, 'Charlie'),
    ('sales', 80000, 'Dave'),
    ('sales', 90000, 'Eve'),
    ('sales', 85000, 'Frank'),
    ('hr', 70000, 'Grace'),
    ('hr', 75000, 'Heidi');

-- ROW_NUMBER, RANK, DENSE_RANK
SELECT name, department, salary,
    ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS row_num,
    RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS rank,
    DENSE_RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS dense_rank
FROM recno_window ORDER BY department, salary DESC;

-- LAG, LEAD
SELECT name, salary,
    LAG(salary) OVER (ORDER BY salary) AS prev_salary,
    LEAD(salary) OVER (ORDER BY salary) AS next_salary
FROM recno_window ORDER BY salary;

-- Running totals
SELECT name, department, salary,
    SUM(salary) OVER (PARTITION BY department ORDER BY salary) AS running_total,
    AVG(salary) OVER (PARTITION BY department) AS dept_avg
FROM recno_window ORDER BY department, salary;

-- NTILE
SELECT name, salary,
    NTILE(4) OVER (ORDER BY salary DESC) AS quartile
FROM recno_window ORDER BY salary DESC;

-- Frame clause
SELECT name, salary,
    AVG(salary) OVER (ORDER BY salary ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS moving_avg
FROM recno_window ORDER BY salary;

DROP TABLE recno_window;

-- =============================================
-- Grouping sets, CUBE, ROLLUP
-- =============================================

CREATE TABLE recno_grouping (
    region text,
    product text,
    year integer,
    amount numeric(10,2)
) USING recno;

INSERT INTO recno_grouping VALUES
    ('US', 'Widget', 2024, 100),
    ('US', 'Widget', 2025, 150),
    ('US', 'Gadget', 2024, 200),
    ('US', 'Gadget', 2025, 250),
    ('EU', 'Widget', 2024, 80),
    ('EU', 'Widget', 2025, 120),
    ('EU', 'Gadget', 2024, 180),
    ('EU', 'Gadget', 2025, 220);

-- GROUPING SETS
SELECT region, product, SUM(amount) AS total
FROM recno_grouping
GROUP BY GROUPING SETS ((region, product), (region), (product), ())
ORDER BY region NULLS LAST, product NULLS LAST;

-- ROLLUP
SELECT region, product, SUM(amount) AS total
FROM recno_grouping
GROUP BY ROLLUP (region, product)
ORDER BY region NULLS LAST, product NULLS LAST;

-- CUBE
SELECT region, product, SUM(amount) AS total
FROM recno_grouping
GROUP BY CUBE (region, product)
ORDER BY region NULLS LAST, product NULLS LAST;

-- GROUPING() function
SELECT region, product,
    GROUPING(region) AS grp_region,
    GROUPING(product) AS grp_product,
    SUM(amount)
FROM recno_grouping
GROUP BY CUBE (region, product)
ORDER BY GROUPING(region), GROUPING(product), region NULLS LAST, product NULLS LAST;

DROP TABLE recno_grouping;

-- =============================================
-- LATERAL joins
-- =============================================

CREATE TABLE recno_lateral_orders (
    id serial PRIMARY KEY,
    customer_id integer,
    amount numeric(10,2),
    ordered_at date
) USING recno;

CREATE TABLE recno_lateral_customers (
    id serial PRIMARY KEY,
    name text
) USING recno;

INSERT INTO recno_lateral_customers (name) VALUES ('Alice'), ('Bob'), ('Charlie');
INSERT INTO recno_lateral_orders (customer_id, amount, ordered_at) VALUES
    (1, 100, '2025-01-01'), (1, 200, '2025-02-01'), (1, 50, '2025-03-01'),
    (2, 300, '2025-01-15'), (2, 150, '2025-02-15'),
    (3, 500, '2025-01-20');

-- LATERAL subquery: top 2 orders per customer
SELECT c.name, o.amount, o.ordered_at
FROM recno_lateral_customers c,
     LATERAL (
         SELECT amount, ordered_at
         FROM recno_lateral_orders
         WHERE customer_id = c.id
         ORDER BY amount DESC
         LIMIT 2
     ) o
ORDER BY c.name, o.amount DESC;

-- LATERAL with aggregation
SELECT c.name, stats.total, stats.max_order
FROM recno_lateral_customers c,
     LATERAL (
         SELECT SUM(amount) AS total, MAX(amount) AS max_order
         FROM recno_lateral_orders
         WHERE customer_id = c.id
     ) stats
ORDER BY c.name;

DROP TABLE recno_lateral_orders;
DROP TABLE recno_lateral_customers;

-- =============================================
-- Row-Level Security (RLS)
-- =============================================

CREATE TABLE recno_rls (
    id serial PRIMARY KEY,
    owner_name text,
    data text,
    is_public boolean DEFAULT false
) USING recno;

INSERT INTO recno_rls (owner_name, data, is_public) VALUES
    ('alice', 'alice private data', false),
    ('alice', 'alice public data', true),
    ('bob', 'bob private data', false),
    ('bob', 'bob public data', true);

-- Enable RLS
ALTER TABLE recno_rls ENABLE ROW LEVEL SECURITY;

-- Create policy: users see their own rows plus public rows
CREATE POLICY recno_rls_policy ON recno_rls
    USING (owner_name = current_user OR is_public = true);

-- As superuser, we can still see everything (BYPASSRLS)
SELECT id, owner_name, is_public FROM recno_rls ORDER BY id;

-- Disable RLS for cleanup
ALTER TABLE recno_rls DISABLE ROW LEVEL SECURITY;

DROP TABLE recno_rls;

-- =============================================
-- Table inheritance
-- =============================================

CREATE TABLE recno_parent_inh (
    id serial,
    name text,
    created_at timestamp DEFAULT now()
) USING recno;

CREATE TABLE recno_child_inh (
    extra_data text
) INHERITS (recno_parent_inh) USING recno;

INSERT INTO recno_parent_inh (name) VALUES ('parent_only');
INSERT INTO recno_child_inh (name, extra_data) VALUES ('child_row', 'extra');

-- Query parent sees all rows (inheritance)
SELECT name FROM recno_parent_inh ORDER BY name;

-- ONLY parent_inh excludes children
SELECT name FROM ONLY recno_parent_inh ORDER BY name;

-- Query child table
SELECT name, extra_data FROM recno_child_inh;

DROP TABLE recno_child_inh;
DROP TABLE recno_parent_inh;

-- =============================================
-- TABLESAMPLE
-- =============================================

CREATE TABLE recno_sample (
    id serial PRIMARY KEY,
    data text
) USING recno;

INSERT INTO recno_sample (data)
SELECT 'sample_' || i FROM generate_series(1, 1000) i;

-- BERNOULLI sampling
SELECT COUNT(*) AS approx_10pct
FROM recno_sample TABLESAMPLE BERNOULLI (10) REPEATABLE (42);

-- SYSTEM sampling
SELECT COUNT(*) AS system_sample
FROM recno_sample TABLESAMPLE SYSTEM (10) REPEATABLE (42);

DROP TABLE recno_sample;

-- =============================================
-- Generated columns
-- =============================================

CREATE TABLE recno_generated (
    id serial PRIMARY KEY,
    first_name text,
    last_name text,
    full_name text GENERATED ALWAYS AS (first_name || ' ' || last_name) STORED,
    area numeric,
    perimeter numeric,
    ratio numeric GENERATED ALWAYS AS (area / NULLIF(perimeter, 0)) STORED
) USING recno;

INSERT INTO recno_generated (first_name, last_name, area, perimeter)
VALUES ('John', 'Doe', 100, 40);

SELECT full_name, ratio FROM recno_generated;

-- Update source columns; generated columns should update
UPDATE recno_generated SET first_name = 'Jane', area = 200;
SELECT full_name, ratio FROM recno_generated;

DROP TABLE recno_generated;

-- =============================================
-- Identity columns
-- =============================================

CREATE TABLE recno_identity (
    id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    data text
) USING recno;

INSERT INTO recno_identity (data) VALUES ('first'), ('second'), ('third');

SELECT id, data FROM recno_identity ORDER BY id;

-- GENERATED BY DEFAULT
CREATE TABLE recno_identity_default (
    id integer GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    data text
) USING recno;

INSERT INTO recno_identity_default (data) VALUES ('auto');
INSERT INTO recno_identity_default (id, data) VALUES (100, 'manual');

SELECT id, data FROM recno_identity_default ORDER BY id;

DROP TABLE recno_identity;
DROP TABLE recno_identity_default;

-- =============================================
-- RETURNING clause
-- =============================================

CREATE TABLE recno_returning (
    id serial PRIMARY KEY,
    name text,
    value integer
) USING recno;

-- INSERT ... RETURNING
INSERT INTO recno_returning (name, value) VALUES ('test', 42)
RETURNING id, name, value;

-- UPDATE ... RETURNING
UPDATE recno_returning SET value = value * 2 WHERE name = 'test'
RETURNING id, value AS new_value;

-- DELETE ... RETURNING
DELETE FROM recno_returning RETURNING *;

DROP TABLE recno_returning;

-- =============================================
-- UPSERT (INSERT ... ON CONFLICT)
-- =============================================

CREATE TABLE recno_upsert (
    key text PRIMARY KEY,
    value integer,
    updated_count integer DEFAULT 0
) USING recno;

-- Initial insert
INSERT INTO recno_upsert VALUES ('a', 1, 0);

-- Upsert: conflict on key
INSERT INTO recno_upsert VALUES ('a', 100, 0)
ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_count = recno_upsert.updated_count + 1;

-- Upsert: no conflict
INSERT INTO recno_upsert VALUES ('b', 2, 0)
ON CONFLICT (key) DO NOTHING;

-- ON CONFLICT DO NOTHING (with conflict)
INSERT INTO recno_upsert VALUES ('a', 999, 0)
ON CONFLICT (key) DO NOTHING;

SELECT * FROM recno_upsert ORDER BY key;

DROP TABLE recno_upsert;

-- =============================================
-- Common Table Expressions (recursive)
-- =============================================

CREATE TABLE recno_tree (
    id serial PRIMARY KEY,
    parent_id integer REFERENCES recno_tree(id),
    name text
) USING recno;

INSERT INTO recno_tree (id, parent_id, name) VALUES
    (1, NULL, 'root'),
    (2, 1, 'child1'),
    (3, 1, 'child2'),
    (4, 2, 'grandchild1'),
    (5, 2, 'grandchild2'),
    (6, 3, 'grandchild3');

-- Recursive CTE to traverse tree
WITH RECURSIVE tree_path AS (
    SELECT id, name, parent_id, 0 AS depth, name::text AS path
    FROM recno_tree WHERE parent_id IS NULL
    UNION ALL
    SELECT t.id, t.name, t.parent_id, tp.depth + 1, tp.path || ' > ' || t.name
    FROM recno_tree t JOIN tree_path tp ON t.parent_id = tp.id
)
SELECT depth, path FROM tree_path ORDER BY path;

DROP TABLE recno_tree;

-- =============================================
-- MERGE statement
-- =============================================

CREATE TABLE recno_target (
    id integer PRIMARY KEY,
    value text,
    counter integer DEFAULT 0
) USING recno;

CREATE TABLE recno_source_merge (
    id integer PRIMARY KEY,
    value text
) USING recno;

INSERT INTO recno_target VALUES (1, 'existing', 0), (2, 'old', 0);
INSERT INTO recno_source_merge VALUES (1, 'updated'), (3, 'new');

MERGE INTO recno_target t
USING recno_source_merge s ON t.id = s.id
WHEN MATCHED THEN
    UPDATE SET value = s.value, counter = t.counter + 1
WHEN NOT MATCHED THEN
    INSERT (id, value) VALUES (s.id, s.value);

SELECT * FROM recno_target ORDER BY id;

DROP TABLE recno_target;
DROP TABLE recno_source_merge;

-- =============================================
-- Triggers
-- =============================================

CREATE TABLE recno_trigger_test (
    id serial PRIMARY KEY,
    name text,
    audit_log text DEFAULT ''
) USING recno;

CREATE TABLE recno_audit (
    id serial PRIMARY KEY,
    operation text,
    row_id integer,
    ts timestamp DEFAULT now()
) USING recno;

-- Trigger function
CREATE FUNCTION recno_audit_func() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO recno_audit (operation, row_id)
    VALUES (TG_OP, COALESCE(NEW.id, OLD.id));
    RETURN COALESCE(NEW, OLD);
END;
$$;

CREATE TRIGGER recno_after_trigger
    AFTER INSERT OR UPDATE OR DELETE ON recno_trigger_test
    FOR EACH ROW EXECUTE FUNCTION recno_audit_func();

INSERT INTO recno_trigger_test (name) VALUES ('trigger_test');
UPDATE recno_trigger_test SET name = 'updated' WHERE id = 1;
DELETE FROM recno_trigger_test WHERE id = 1;

SELECT operation, row_id FROM recno_audit ORDER BY id;

DROP TABLE recno_trigger_test CASCADE;
DROP TABLE recno_audit;
DROP FUNCTION recno_audit_func();

-- =============================================
-- Views and materialized views
-- =============================================

CREATE TABLE recno_view_source (
    id serial PRIMARY KEY,
    category text,
    amount numeric(10,2)
) USING recno;

INSERT INTO recno_view_source (category, amount) VALUES
    ('A', 100), ('A', 200), ('B', 300), ('B', 400), ('C', 500);

-- Regular view
CREATE VIEW recno_summary_view AS
SELECT category, SUM(amount) AS total, COUNT(*) AS cnt
FROM recno_view_source GROUP BY category;

SELECT * FROM recno_summary_view ORDER BY category;

-- Materialized view
CREATE MATERIALIZED VIEW recno_mat_view AS
SELECT category, SUM(amount) AS total
FROM recno_view_source GROUP BY category;

SELECT * FROM recno_mat_view ORDER BY category;

-- Refresh after data change
INSERT INTO recno_view_source (category, amount) VALUES ('A', 50);
REFRESH MATERIALIZED VIEW recno_mat_view;

SELECT * FROM recno_mat_view ORDER BY category;

-- Concurrent refresh
CREATE UNIQUE INDEX ON recno_mat_view (category);
REFRESH MATERIALIZED VIEW CONCURRENTLY recno_mat_view;

DROP MATERIALIZED VIEW recno_mat_view;
DROP VIEW recno_summary_view;
DROP TABLE recno_view_source;

-- =============================================
-- JSON/JSONB operations
-- =============================================

CREATE TABLE recno_json (
    id serial PRIMARY KEY,
    data jsonb
) USING recno;

INSERT INTO recno_json (data) VALUES
    ('{"name": "Alice", "age": 30, "tags": ["developer", "manager"]}'),
    ('{"name": "Bob", "age": 25, "tags": ["designer"]}'),
    ('{"name": "Charlie", "age": 35, "tags": ["developer"], "address": {"city": "NYC"}}');

-- JSONB operators
SELECT id, data->>'name' AS name, data->'age' AS age FROM recno_json ORDER BY id;

-- Containment
SELECT id, data->>'name' FROM recno_json WHERE data @> '{"tags": ["developer"]}' ORDER BY id;

-- Path query
SELECT id, data #>> '{address,city}' AS city FROM recno_json WHERE data ? 'address';

-- GIN index on JSONB
CREATE INDEX idx_recno_json ON recno_json USING gin (data);
SET enable_seqscan = off;
SELECT data->>'name' FROM recno_json WHERE data @> '{"age": 30}';
RESET enable_seqscan;

-- JSONB update
UPDATE recno_json SET data = data || '{"role": "admin"}' WHERE id = 1;
SELECT data->>'role' FROM recno_json WHERE id = 1;

DROP TABLE recno_json;

-- =============================================
-- Full-text search
-- =============================================

CREATE TABLE recno_fts (
    id serial PRIMARY KEY,
    title text,
    body text,
    tsv tsvector GENERATED ALWAYS AS (to_tsvector('english', title || ' ' || body)) STORED
) USING recno;

CREATE INDEX idx_recno_fts ON recno_fts USING gin (tsv);

INSERT INTO recno_fts (title, body) VALUES
    ('PostgreSQL Performance', 'How to optimize PostgreSQL database queries for speed'),
    ('RECNO Storage', 'The RECNO access method provides timestamp-based MVCC'),
    ('Index Tuning', 'B-tree and GIN indexes improve query performance');

-- Full-text search
SELECT id, title FROM recno_fts WHERE tsv @@ to_tsquery('english', 'performance');
SELECT id, title FROM recno_fts WHERE tsv @@ to_tsquery('english', 'recno & mvcc');

-- Ranking
SELECT id, title, ts_rank(tsv, q) AS rank
FROM recno_fts, to_tsquery('english', 'performance | optimize') q
WHERE tsv @@ q ORDER BY rank DESC;

DROP TABLE recno_fts;

-- =============================================
-- Array operations
-- =============================================

CREATE TABLE recno_arrays (
    id serial PRIMARY KEY,
    int_arr integer[],
    text_arr text[],
    nested_arr integer[][]
) USING recno;

INSERT INTO recno_arrays (int_arr, text_arr, nested_arr) VALUES
    ('{1,2,3,4,5}', '{"hello","world"}', '{{1,2},{3,4}}'),
    ('{10,20,30}', '{"foo","bar","baz"}', '{{5,6},{7,8}}');

-- Array operations
SELECT id, array_length(int_arr, 1) AS arr_len,
    int_arr[1] AS first, int_arr[array_length(int_arr, 1)] AS last
FROM recno_arrays ORDER BY id;

-- Array containment
SELECT id FROM recno_arrays WHERE int_arr @> ARRAY[2, 3];

-- Array unnest
SELECT id, unnest(text_arr) AS elem FROM recno_arrays WHERE id = 1;

-- Array aggregation
SELECT array_agg(id ORDER BY id) FROM recno_arrays;

DROP TABLE recno_arrays;

-- =============================================
-- Domain types
-- =============================================

CREATE DOMAIN positive_int AS integer CHECK (VALUE > 0);
CREATE DOMAIN email_text AS text CHECK (VALUE LIKE '%@%');

CREATE TABLE recno_domains (
    id serial PRIMARY KEY,
    quantity positive_int,
    contact email_text
) USING recno;

INSERT INTO recno_domains (quantity, contact) VALUES (5, 'test@example.com');

-- Should fail
\set ON_ERROR_STOP off
INSERT INTO recno_domains (quantity, contact) VALUES (-1, 'test@example.com');
INSERT INTO recno_domains (quantity, contact) VALUES (1, 'invalid');
\set ON_ERROR_STOP on

SELECT * FROM recno_domains;

DROP TABLE recno_domains;
DROP DOMAIN email_text;
DROP DOMAIN positive_int;

-- =============================================
-- Sequences (explicit)
-- =============================================

CREATE SEQUENCE recno_seq START 1000 INCREMENT 5;

CREATE TABLE recno_seq_test (
    id integer DEFAULT nextval('recno_seq') PRIMARY KEY,
    data text
) USING recno;

INSERT INTO recno_seq_test (data) VALUES ('first'), ('second'), ('third');
SELECT id, data FROM recno_seq_test ORDER BY id;

DROP TABLE recno_seq_test;
DROP SEQUENCE recno_seq;

-- =============================================
-- Statistics and pg_stat integration
-- =============================================

CREATE TABLE recno_stat_test (
    id serial PRIMARY KEY,
    category text,
    value integer
) USING recno;

INSERT INTO recno_stat_test (category, value)
SELECT CASE i % 3 WHEN 0 THEN 'A' WHEN 1 THEN 'B' ELSE 'C' END, i
FROM generate_series(1, 1000) i;

ANALYZE recno_stat_test;

-- Verify pg_class integration
SELECT c.relname, c.reltuples::integer, c.relpages, am.amname
FROM pg_class c JOIN pg_am am ON c.relam = am.oid
WHERE c.relname = 'recno_stat_test';

-- Verify pg_stats integration
SELECT attname, n_distinct, null_frac,
    most_common_vals IS NOT NULL AS has_mcv
FROM pg_stats
WHERE tablename = 'recno_stat_test' AND attname IN ('category', 'value')
ORDER BY attname;

-- Verify pg_stat_user_tables
SELECT relname, n_live_tup, n_dead_tup
FROM pg_stat_user_tables
WHERE relname = 'recno_stat_test';

UPDATE recno_stat_test SET value = value + 1 WHERE id <= 100;
DELETE FROM recno_stat_test WHERE id > 900;

SELECT relname, n_tup_upd, n_tup_del
FROM pg_stat_user_tables
WHERE relname = 'recno_stat_test';

DROP TABLE recno_stat_test;

-- =============================================
-- EXPLAIN output
-- =============================================

CREATE TABLE recno_explain (
    id serial PRIMARY KEY,
    name text,
    value integer
) USING recno;

CREATE INDEX idx_explain_name ON recno_explain (name);

INSERT INTO recno_explain (name, value)
SELECT 'item_' || i, i FROM generate_series(1, 5000) i;

ANALYZE recno_explain;

-- Verify EXPLAIN shows RECNO scan methods
EXPLAIN (BUFFERS OFF, COSTS OFF) SELECT * FROM recno_explain;
EXPLAIN (BUFFERS OFF, COSTS OFF) SELECT * FROM recno_explain WHERE name = 'item_100';
EXPLAIN (BUFFERS OFF, COSTS OFF) SELECT * FROM recno_explain WHERE id BETWEEN 100 AND 200;

-- Verify EXPLAIN ANALYZE works
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, TIMING OFF, SUMMARY OFF)
SELECT COUNT(*) FROM recno_explain WHERE value > 4000;

DROP TABLE recno_explain;

-- =============================================
-- Mixed HEAP and RECNO operations
-- =============================================

CREATE TABLE heap_partner (
    id serial PRIMARY KEY,
    data text
) USING heap;

CREATE TABLE recno_partner (
    id serial PRIMARY KEY,
    heap_id integer REFERENCES heap_partner(id),
    data text
) USING recno;

INSERT INTO heap_partner (data) VALUES ('heap1'), ('heap2'), ('heap3');
INSERT INTO recno_partner (heap_id, data) VALUES (1, 'recno1'), (2, 'recno2'), (3, 'recno3');

-- Cross-storage JOIN
SELECT h.data AS heap_data, r.data AS recno_data
FROM heap_partner h JOIN recno_partner r ON h.id = r.heap_id
ORDER BY h.id;

-- INSERT from heap to recno
INSERT INTO recno_partner (heap_id, data)
SELECT id, 'copied_' || data FROM heap_partner;

-- INSERT from recno to heap
INSERT INTO heap_partner (data)
SELECT data FROM recno_partner WHERE heap_id IS NULL;

SELECT COUNT(*) FROM recno_partner;

DROP TABLE recno_partner;
DROP TABLE heap_partner;

-- =============================================
-- TRUNCATE variants
-- =============================================

CREATE TABLE recno_trunc_parent (
    id serial PRIMARY KEY,
    data text
) USING recno;

CREATE TABLE recno_trunc_child (
    id serial PRIMARY KEY,
    parent_id integer REFERENCES recno_trunc_parent(id),
    data text
) USING recno;

INSERT INTO recno_trunc_parent (data) VALUES ('p1'), ('p2');
INSERT INTO recno_trunc_child (parent_id, data) VALUES (1, 'c1'), (2, 'c2');

-- TRUNCATE CASCADE
TRUNCATE recno_trunc_parent CASCADE;
SELECT COUNT(*) FROM recno_trunc_parent;
SELECT COUNT(*) FROM recno_trunc_child;

-- TRUNCATE RESTART IDENTITY
INSERT INTO recno_trunc_parent (data) VALUES ('new');
TRUNCATE recno_trunc_parent RESTART IDENTITY CASCADE;
INSERT INTO recno_trunc_parent (data) VALUES ('reset');
SELECT id FROM recno_trunc_parent;

DROP TABLE recno_trunc_child;
DROP TABLE recno_trunc_parent;

-- =============================================
-- CLUSTER
-- =============================================

CREATE TABLE recno_cluster (
    id serial PRIMARY KEY,
    sort_key integer,
    data text
) USING recno;

CREATE INDEX idx_cluster_sort ON recno_cluster (sort_key);

INSERT INTO recno_cluster (sort_key, data)
SELECT (random() * 1000)::integer, 'data_' || i
FROM generate_series(1, 500) i;

CLUSTER recno_cluster USING idx_cluster_sort;

-- Verify data is intact after CLUSTER
SELECT COUNT(*) FROM recno_cluster;

DROP TABLE recno_cluster;

-- =============================================
-- ALTER TABLE operations
-- =============================================

CREATE TABLE recno_alter (
    id serial PRIMARY KEY,
    col1 text,
    col2 integer
) USING recno;

INSERT INTO recno_alter (col1, col2) VALUES ('test', 42);

-- Add column with default
ALTER TABLE recno_alter ADD COLUMN col3 text DEFAULT 'default_val';
SELECT col3 FROM recno_alter WHERE id = 1;

-- Add column with NOT NULL + default
ALTER TABLE recno_alter ADD COLUMN col4 integer NOT NULL DEFAULT 0;
SELECT col4 FROM recno_alter WHERE id = 1;

-- Change column type
ALTER TABLE recno_alter ALTER COLUMN col2 TYPE bigint;
INSERT INTO recno_alter (col1, col2) VALUES ('big', 9223372036854775807);
SELECT col2 FROM recno_alter WHERE col1 = 'big';

-- Set/drop default
ALTER TABLE recno_alter ALTER COLUMN col1 SET DEFAULT 'new_default';
INSERT INTO recno_alter (col2) VALUES (1);
SELECT col1 FROM recno_alter WHERE col2 = 1;

-- Add constraint
ALTER TABLE recno_alter ADD CONSTRAINT positive_col2 CHECK (col2 > 0);

-- Should fail
\set ON_ERROR_STOP off
INSERT INTO recno_alter (col1, col2) VALUES ('bad', -1);
\set ON_ERROR_STOP on

DROP TABLE recno_alter;
