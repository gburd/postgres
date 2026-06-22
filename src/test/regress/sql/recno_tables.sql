--
-- Test RECNO table DDL, DML, data types, constraints, and partitioning
--

-- =============================================
-- Basic DDL
-- =============================================

-- Create a basic RECNO table
CREATE TABLE recno_ddl_basic (
    id serial PRIMARY KEY,
    name text NOT NULL,
    value integer
) USING recno;

-- Verify access method
SELECT c.relname, am.amname
FROM pg_class c JOIN pg_am am ON c.relam = am.oid
WHERE c.relname = 'recno_ddl_basic';

-- ALTER TABLE: add column
ALTER TABLE recno_ddl_basic ADD COLUMN description text;

-- ALTER TABLE: drop column
ALTER TABLE recno_ddl_basic DROP COLUMN description;

-- ALTER TABLE: rename column
ALTER TABLE recno_ddl_basic RENAME COLUMN name TO full_name;

-- ALTER TABLE: set default
ALTER TABLE recno_ddl_basic ALTER COLUMN value SET DEFAULT 0;

-- ALTER TABLE: set NOT NULL
ALTER TABLE recno_ddl_basic ALTER COLUMN value SET NOT NULL;

-- ALTER TABLE: drop NOT NULL
ALTER TABLE recno_ddl_basic ALTER COLUMN value DROP NOT NULL;

-- ALTER TABLE: rename table
ALTER TABLE recno_ddl_basic RENAME TO recno_ddl_renamed;
ALTER TABLE recno_ddl_renamed RENAME TO recno_ddl_basic;

-- ALTER TABLE: add/drop column type
ALTER TABLE recno_ddl_basic ADD COLUMN temp_col integer;
ALTER TABLE recno_ddl_basic ALTER COLUMN temp_col SET DATA TYPE bigint;
ALTER TABLE recno_ddl_basic DROP COLUMN temp_col;

-- TRUNCATE
INSERT INTO recno_ddl_basic (full_name, value) VALUES ('truncate_me', 1);
SELECT COUNT(*) FROM recno_ddl_basic;
TRUNCATE recno_ddl_basic;
SELECT COUNT(*) FROM recno_ddl_basic;

DROP TABLE recno_ddl_basic;

-- =============================================
-- Storage parameters
-- =============================================

-- Create with fillfactor
CREATE TABLE recno_fillfactor (
    id serial PRIMARY KEY,
    data text
) USING recno WITH (fillfactor = 70);

-- Verify storage parameter
SELECT reloptions FROM pg_class WHERE relname = 'recno_fillfactor';

INSERT INTO recno_fillfactor (data)
SELECT 'fill_' || i FROM generate_series(1, 100) i;

SELECT COUNT(*) FROM recno_fillfactor;

DROP TABLE recno_fillfactor;

-- fillfactor must be honored on insert, not merely stored: a sparse
-- fillfactor reserves free space per page, so the same rows span more
-- pages than a dense fillfactor.  Compare relative page counts to stay
-- independent of BLCKSZ.
CREATE TABLE recno_ff_dense (id int, pad char(80)) USING recno
    WITH (fillfactor = 100);
CREATE TABLE recno_ff_sparse (id int, pad char(80)) USING recno
    WITH (fillfactor = 20);
INSERT INTO recno_ff_dense SELECT g, 'x' FROM generate_series(1, 2000) g;
INSERT INTO recno_ff_sparse SELECT g, 'x' FROM generate_series(1, 2000) g;
SELECT pg_relation_size('recno_ff_sparse') > pg_relation_size('recno_ff_dense')
    AS sparse_uses_more_pages;
DROP TABLE recno_ff_dense;
DROP TABLE recno_ff_sparse;

-- ALTER TABLE SET (fillfactor) updates the stored reloption.
CREATE TABLE recno_ff_alter (id int) USING recno WITH (fillfactor = 90);
ALTER TABLE recno_ff_alter SET (fillfactor = 60);
SELECT reloptions FROM pg_class WHERE relname = 'recno_ff_alter';
DROP TABLE recno_ff_alter;

-- Out-of-range fillfactor is rejected by reloption validation.
CREATE TABLE recno_ff_bad (id int) USING recno WITH (fillfactor = 5);

-- Create with autovacuum settings
CREATE TABLE recno_autovac (
    id serial PRIMARY KEY,
    data text
) USING recno WITH (
    autovacuum_vacuum_threshold = 50,
    autovacuum_vacuum_scale_factor = 0.1
);

SELECT reloptions FROM pg_class WHERE relname = 'recno_autovac';

DROP TABLE recno_autovac;

-- =============================================
-- ALTER TABLE SET ACCESS METHOD
-- =============================================

-- Create a heap table and convert to recno
CREATE TABLE recno_convert_test (
    id serial PRIMARY KEY,
    name text,
    value integer
) USING heap;

-- Verify initial access method is heap
SELECT c.relname, am.amname
FROM pg_class c JOIN pg_am am ON c.relam = am.oid
WHERE c.relname = 'recno_convert_test';

-- Insert data into heap table
INSERT INTO recno_convert_test (name, value)
SELECT 'item_' || i, i FROM generate_series(1, 50) i;

-- Switch from heap to recno
ALTER TABLE recno_convert_test SET ACCESS METHOD recno;

-- Verify access method changed
SELECT c.relname, am.amname
FROM pg_class c JOIN pg_am am ON c.relam = am.oid
WHERE c.relname = 'recno_convert_test';

-- Verify data survived the conversion
SELECT COUNT(*) FROM recno_convert_test;
SELECT name, value FROM recno_convert_test WHERE id = 1;
SELECT name, value FROM recno_convert_test WHERE id = 50;

-- Verify DML still works after conversion
INSERT INTO recno_convert_test (name, value) VALUES ('after_convert', 999);
UPDATE recno_convert_test SET value = value + 1 WHERE id = 1;
DELETE FROM recno_convert_test WHERE id = 2;
SELECT COUNT(*) FROM recno_convert_test;

-- Switch back from recno to heap
ALTER TABLE recno_convert_test SET ACCESS METHOD heap;

SELECT c.relname, am.amname
FROM pg_class c JOIN pg_am am ON c.relam = am.oid
WHERE c.relname = 'recno_convert_test';

-- Verify data survived both conversions
SELECT COUNT(*) FROM recno_convert_test;

DROP TABLE recno_convert_test;

-- =============================================
-- All supported data types
-- =============================================

CREATE TABLE recno_datatypes (
    -- Integer types
    col_bool boolean,
    col_int2 smallint,
    col_int4 integer,
    col_int8 bigint,
    -- Floating point types
    col_float4 real,
    col_float8 double precision,
    col_numeric numeric(15,4),
    -- Character types
    col_char char(20),
    col_varchar varchar(100),
    col_text text,
    -- Binary
    col_bytea bytea,
    -- Date/time types
    col_date date,
    col_time time,
    col_timetz time with time zone,
    col_timestamp timestamp,
    col_timestamptz timestamptz,
    col_interval interval,
    -- Other types
    col_uuid uuid,
    col_json json,
    col_jsonb jsonb,
    col_xml xml,
    col_inet inet,
    col_cidr cidr,
    col_macaddr macaddr,
    -- Array types
    col_int_array integer[],
    col_text_array text[]
) USING recno;

-- Insert a row with all types populated
INSERT INTO recno_datatypes VALUES (
    true,
    32767,
    2147483647,
    9223372036854775807,
    3.14159,
    2.718281828459045,
    12345678.1234,
    'fixed char value',
    'variable length string',
    'This is a longer text value for testing the TEXT data type in RECNO storage',
    E'\\xDEADBEEFCAFE',
    '2025-06-15',
    '14:30:00',
    '14:30:00+05:30',
    '2025-06-15 14:30:00',
    '2025-06-15 14:30:00+00',
    '1 year 2 months 3 days 4 hours',
    'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11',
    '{"key": "value", "nested": {"a": 1}}',
    '{"key": "value", "nested": {"a": 1}}',
    '<root><child>text</child></root>',
    '192.168.1.0/24',
    '10.0.0.0/8',
    '08:00:2b:01:02:03',
    '{1, 2, 3, 4, 5}',
    '{"hello", "world"}'
);

-- Insert a row with all NULLs
INSERT INTO recno_datatypes DEFAULT VALUES;

-- Verify retrieval of all types
SELECT col_bool, col_int2, col_int4, col_int8 FROM recno_datatypes WHERE col_bool IS NOT NULL;
SELECT col_float4, col_float8, col_numeric FROM recno_datatypes WHERE col_float4 IS NOT NULL;
SELECT col_char, col_varchar, col_text FROM recno_datatypes WHERE col_text IS NOT NULL;
SELECT col_date, col_time, col_timestamp FROM recno_datatypes WHERE col_date IS NOT NULL;
SELECT col_uuid, col_json, col_jsonb FROM recno_datatypes WHERE col_uuid IS NOT NULL;
SELECT col_inet, col_cidr, col_macaddr FROM recno_datatypes WHERE col_inet IS NOT NULL;
SELECT col_int_array, col_text_array FROM recno_datatypes WHERE col_int_array IS NOT NULL;

-- Verify NULL row
SELECT COUNT(*) AS null_row_count FROM recno_datatypes
WHERE col_bool IS NULL AND col_int2 IS NULL AND col_text IS NULL;

-- Update each data type and re-read
UPDATE recno_datatypes SET col_bool = false WHERE col_bool IS NOT NULL;
UPDATE recno_datatypes SET col_int4 = -1 WHERE col_int4 IS NOT NULL;
UPDATE recno_datatypes SET col_text = 'updated text value' WHERE col_text IS NOT NULL;
UPDATE recno_datatypes SET col_jsonb = '{"updated": true}' WHERE col_jsonb IS NOT NULL;
UPDATE recno_datatypes SET col_int_array = '{10, 20, 30}' WHERE col_int_array IS NOT NULL;

SELECT col_bool, col_int4, col_text FROM recno_datatypes WHERE col_bool IS NOT NULL;
SELECT col_jsonb, col_int_array FROM recno_datatypes WHERE col_jsonb IS NOT NULL;

DROP TABLE recno_datatypes;

-- =============================================
-- Boundary and edge-case values
-- =============================================

CREATE TABLE recno_edge_cases (
    id serial,
    val_int2 smallint,
    val_int4 integer,
    val_int8 bigint,
    val_text text
) USING recno;

-- Boundary integer values
INSERT INTO recno_edge_cases (val_int2, val_int4, val_int8, val_text) VALUES
    (-32768, -2147483648, -9223372036854775808, ''),
    (32767, 2147483647, 9223372036854775807, 'max values'),
    (0, 0, 0, NULL);

SELECT val_int2, val_int4, val_int8, val_text FROM recno_edge_cases ORDER BY id;

-- Empty string vs NULL
INSERT INTO recno_edge_cases (val_text) VALUES (''), (NULL);
SELECT id, val_text IS NULL AS is_null, val_text = '' AS is_empty
FROM recno_edge_cases WHERE id > 3 ORDER BY id;

-- Very long text
INSERT INTO recno_edge_cases (val_text) VALUES (repeat('A', 10000));
SELECT id, length(val_text) AS text_len FROM recno_edge_cases WHERE length(val_text) > 100;

DROP TABLE recno_edge_cases;

-- =============================================
-- DML operations
-- =============================================

CREATE TABLE recno_dml (
    id serial PRIMARY KEY,
    name text,
    value integer,
    data bytea
) USING recno WITH (fillfactor = 80);

-- INSERT: single row
INSERT INTO recno_dml (name, value, data) VALUES ('row1', 100, 'data1');

-- INSERT: multiple rows
INSERT INTO recno_dml (name, value, data) VALUES
    ('row2', 200, 'data2'),
    ('row3', 300, 'data3'),
    ('row4', 400, 'data4');

-- INSERT ... SELECT (bulk)
INSERT INTO recno_dml (name, value, data)
SELECT 'bulk_' || i::text, i * 10, ('bulk_data_' || i::text)::bytea
FROM generate_series(1, 100) i;

SELECT COUNT(*) FROM recno_dml;

-- INSERT ... RETURNING
INSERT INTO recno_dml (name, value) VALUES ('returning_test', 555) RETURNING id, name, value;

-- UPDATE: single row
UPDATE recno_dml SET value = 999 WHERE name = 'row1';
SELECT name, value FROM recno_dml WHERE name = 'row1';

-- UPDATE: multiple rows
UPDATE recno_dml SET value = value + 1 WHERE name LIKE 'bulk_%';
SELECT COUNT(*) FROM recno_dml WHERE value > 0;

-- UPDATE: change type-length (short text to longer text)
UPDATE recno_dml SET name = 'updated_with_a_much_longer_name_than_before' WHERE id = 1;
SELECT name FROM recno_dml WHERE id = 1;

-- UPDATE ... RETURNING
UPDATE recno_dml SET value = 777 WHERE name = 'row3' RETURNING id, name, value;

-- DELETE: single row
DELETE FROM recno_dml WHERE name = 'row2';
SELECT COUNT(*) FROM recno_dml WHERE name = 'row2';

-- DELETE ... RETURNING
DELETE FROM recno_dml WHERE name = 'row4' RETURNING id, name;

-- DELETE: multiple rows
DELETE FROM recno_dml WHERE name LIKE 'bulk_%' AND value < 500;
SELECT COUNT(*) FROM recno_dml;

-- DELETE: all rows
DELETE FROM recno_dml;
SELECT COUNT(*) FROM recno_dml;

DROP TABLE recno_dml;

-- =============================================
-- Constraints
-- =============================================

-- PRIMARY KEY constraint (already tested above, but explicit)
CREATE TABLE recno_pk (
    id integer PRIMARY KEY,
    data text
) USING recno;

INSERT INTO recno_pk VALUES (1, 'a'), (2, 'b');

-- Should fail: duplicate PK
\set ON_ERROR_STOP off
INSERT INTO recno_pk VALUES (1, 'duplicate');
\set ON_ERROR_STOP on

DROP TABLE recno_pk;

-- CHECK constraint
CREATE TABLE recno_check (
    id serial PRIMARY KEY,
    value integer CHECK (value > 0),
    status text CHECK (status IN ('active', 'inactive', 'pending'))
) USING recno;

INSERT INTO recno_check (value, status) VALUES (1, 'active');
INSERT INTO recno_check (value, status) VALUES (100, 'pending');

-- These should fail
\set ON_ERROR_STOP off
INSERT INTO recno_check (value, status) VALUES (-1, 'active');
INSERT INTO recno_check (value, status) VALUES (1, 'invalid');
\set ON_ERROR_STOP on

SELECT id, value, status FROM recno_check ORDER BY id;

DROP TABLE recno_check;

-- UNIQUE constraint
CREATE TABLE recno_unique (
    id serial PRIMARY KEY,
    email text UNIQUE,
    code integer
) USING recno;

INSERT INTO recno_unique (email, code) VALUES ('a@test.com', 1);
INSERT INTO recno_unique (email, code) VALUES ('b@test.com', 2);

-- This should fail
\set ON_ERROR_STOP off
INSERT INTO recno_unique (email, code) VALUES ('a@test.com', 3);
\set ON_ERROR_STOP on

-- NULL in UNIQUE is allowed (multiple NULLs)
INSERT INTO recno_unique (email, code) VALUES (NULL, 4);
INSERT INTO recno_unique (email, code) VALUES (NULL, 5);
SELECT COUNT(*) FROM recno_unique WHERE email IS NULL;

DROP TABLE recno_unique;

-- FOREIGN KEY constraint
CREATE TABLE recno_fk_parent (
    id serial PRIMARY KEY,
    name text NOT NULL
) USING recno;

CREATE TABLE recno_fk_child (
    id serial PRIMARY KEY,
    parent_id integer REFERENCES recno_fk_parent(id) ON DELETE CASCADE,
    description text
) USING recno;

INSERT INTO recno_fk_parent (name) VALUES ('Parent A'), ('Parent B');
INSERT INTO recno_fk_child (parent_id, description) VALUES (1, 'Child of A'), (2, 'Child of B');

-- CASCADE delete
DELETE FROM recno_fk_parent WHERE id = 1;
SELECT COUNT(*) FROM recno_fk_child WHERE parent_id = 1;

-- Referential integrity violation
\set ON_ERROR_STOP off
INSERT INTO recno_fk_child (parent_id, description) VALUES (999, 'orphan');
\set ON_ERROR_STOP on

-- Cross-AM foreign key: recno child referencing heap parent
CREATE TABLE heap_parent (
    id serial PRIMARY KEY,
    name text
) USING heap;

INSERT INTO heap_parent (name) VALUES ('heap_parent_1');

CREATE TABLE recno_fk_cross (
    id serial PRIMARY KEY,
    parent_id integer REFERENCES heap_parent(id),
    data text
) USING recno;

INSERT INTO recno_fk_cross (parent_id, data) VALUES (1, 'cross-am child');
SELECT rfc.data, hp.name
FROM recno_fk_cross rfc JOIN heap_parent hp ON rfc.parent_id = hp.id;

DROP TABLE recno_fk_cross;
DROP TABLE heap_parent;
DROP TABLE recno_fk_child;
DROP TABLE recno_fk_parent;

-- EXCLUDE constraint
CREATE TABLE recno_exclude_test (
    id serial PRIMARY KEY,
    range_val int4range,
    EXCLUDE USING gist (range_val WITH &&)
) USING recno;

INSERT INTO recno_exclude_test (range_val) VALUES ('[1, 5)');
INSERT INTO recno_exclude_test (range_val) VALUES ('[10, 20)');

-- Should fail (overlapping)
\set ON_ERROR_STOP off
INSERT INTO recno_exclude_test (range_val) VALUES ('[3, 8)');
\set ON_ERROR_STOP on

DROP TABLE recno_exclude_test;

-- =============================================
-- Table partitioning
-- =============================================

-- Range partitioning
CREATE TABLE recno_part_range (
    id serial,
    created_at date NOT NULL,
    value integer
) PARTITION BY RANGE (created_at) USING recno;

CREATE TABLE recno_part_range_2024 PARTITION OF recno_part_range
    FOR VALUES FROM ('2024-01-01') TO ('2025-01-01') USING recno;
CREATE TABLE recno_part_range_2025 PARTITION OF recno_part_range
    FOR VALUES FROM ('2025-01-01') TO ('2026-01-01') USING recno;
CREATE TABLE recno_part_range_2026 PARTITION OF recno_part_range
    FOR VALUES FROM ('2026-01-01') TO ('2027-01-01') USING recno;

INSERT INTO recno_part_range (created_at, value) VALUES
    ('2024-06-15', 100),
    ('2025-03-01', 200),
    ('2026-01-15', 300);

-- Verify partition routing
SELECT tableoid::regclass, id, created_at, value
FROM recno_part_range ORDER BY created_at;

-- Verify each partition uses recno
SELECT c.relname, am.amname
FROM pg_class c JOIN pg_am am ON c.relam = am.oid
WHERE c.relname LIKE 'recno_part_range_%' ORDER BY c.relname;

DROP TABLE recno_part_range;

-- List partitioning
CREATE TABLE recno_part_list (
    id serial,
    region text NOT NULL,
    amount numeric
) PARTITION BY LIST (region) USING recno;

CREATE TABLE recno_part_list_us PARTITION OF recno_part_list
    FOR VALUES IN ('US', 'CA') USING recno;
CREATE TABLE recno_part_list_eu PARTITION OF recno_part_list
    FOR VALUES IN ('UK', 'DE', 'FR') USING recno;

INSERT INTO recno_part_list (region, amount) VALUES
    ('US', 100.00), ('CA', 200.00),
    ('UK', 300.00), ('DE', 400.00);

SELECT tableoid::regclass, region, amount
FROM recno_part_list ORDER BY region;

DROP TABLE recno_part_list;

-- Hash partitioning
CREATE TABLE recno_part_hash (
    id serial,
    data text
) PARTITION BY HASH (id) USING recno;

CREATE TABLE recno_part_hash_0 PARTITION OF recno_part_hash
    FOR VALUES WITH (MODULUS 4, REMAINDER 0) USING recno;
CREATE TABLE recno_part_hash_1 PARTITION OF recno_part_hash
    FOR VALUES WITH (MODULUS 4, REMAINDER 1) USING recno;
CREATE TABLE recno_part_hash_2 PARTITION OF recno_part_hash
    FOR VALUES WITH (MODULUS 4, REMAINDER 2) USING recno;
CREATE TABLE recno_part_hash_3 PARTITION OF recno_part_hash
    FOR VALUES WITH (MODULUS 4, REMAINDER 3) USING recno;

INSERT INTO recno_part_hash (data)
SELECT 'item_' || i FROM generate_series(1, 100) i;

-- Verify distribution across partitions (all should have rows)
SELECT tableoid::regclass, COUNT(*) FROM recno_part_hash GROUP BY tableoid ORDER BY 1;

DROP TABLE recno_part_hash;

-- =============================================
-- COPY operations
-- =============================================

CREATE TABLE recno_copy (
    id integer,
    name text,
    value numeric
) USING recno;

-- COPY FROM (inline)
COPY recno_copy FROM stdin;
1	Alice	100.50
2	Bob	200.75
3	Charlie	300.25
\.

SELECT * FROM recno_copy ORDER BY id;

-- COPY TO
COPY recno_copy TO stdout;

-- COPY with CSV format
COPY recno_copy TO stdout WITH (FORMAT csv, HEADER true);

DROP TABLE recno_copy;

-- =============================================
-- CTEs, subqueries, and JOINs
-- =============================================

CREATE TABLE recno_orders (
    id serial PRIMARY KEY,
    customer_id integer NOT NULL,
    amount numeric(10,2)
) USING recno;

CREATE TABLE recno_customers (
    id serial PRIMARY KEY,
    name text NOT NULL
) USING recno;

INSERT INTO recno_customers (name) VALUES ('Alice'), ('Bob'), ('Charlie');
INSERT INTO recno_orders (customer_id, amount) VALUES
    (1, 100.00), (1, 200.00), (2, 150.00), (3, 300.00), (3, 50.00);

-- JOIN
SELECT c.name, SUM(o.amount) AS total
FROM recno_customers c JOIN recno_orders o ON c.id = o.customer_id
GROUP BY c.name ORDER BY total DESC;

-- CTE
WITH customer_totals AS (
    SELECT customer_id, SUM(amount) AS total
    FROM recno_orders GROUP BY customer_id
)
SELECT c.name, ct.total
FROM recno_customers c JOIN customer_totals ct ON c.id = ct.customer_id
ORDER BY ct.total DESC;

-- Subquery
SELECT name FROM recno_customers
WHERE id IN (SELECT customer_id FROM recno_orders WHERE amount > 100)
ORDER BY name;

-- LEFT JOIN (includes customers with no orders)
INSERT INTO recno_customers (name) VALUES ('Dave');
SELECT c.name, COALESCE(SUM(o.amount), 0) AS total
FROM recno_customers c LEFT JOIN recno_orders o ON c.id = o.customer_id
GROUP BY c.name ORDER BY c.name;

-- Window function
SELECT c.name, o.amount,
    SUM(o.amount) OVER (PARTITION BY c.name ORDER BY o.id) AS running_total
FROM recno_customers c JOIN recno_orders o ON c.id = o.customer_id
ORDER BY c.name, o.id;

DROP TABLE recno_orders;
DROP TABLE recno_customers;

-- =============================================
-- ON CONFLICT (UPSERT)
-- =============================================

CREATE TABLE recno_upsert (
    id integer PRIMARY KEY,
    value text,
    update_count integer DEFAULT 0
) USING recno;

INSERT INTO recno_upsert VALUES (1, 'initial', 0);

-- UPSERT: conflict triggers update
INSERT INTO recno_upsert VALUES (1, 'conflict', 0)
ON CONFLICT (id) DO UPDATE SET value = 'upserted', update_count = recno_upsert.update_count + 1;

SELECT * FROM recno_upsert;

-- UPSERT: no conflict triggers insert
INSERT INTO recno_upsert VALUES (2, 'new_row', 0)
ON CONFLICT (id) DO UPDATE SET value = 'should_not_happen';

SELECT * FROM recno_upsert ORDER BY id;

-- ON CONFLICT DO NOTHING
INSERT INTO recno_upsert VALUES (1, 'ignored', 0)
ON CONFLICT (id) DO NOTHING;

SELECT * FROM recno_upsert WHERE id = 1;

DROP TABLE recno_upsert;

-- =============================================
-- Temporary tables and CTAS
-- =============================================

CREATE TABLE recno_source (id serial, data text) USING recno;
INSERT INTO recno_source (data) SELECT 'item_' || i FROM generate_series(1, 50) i;

-- CREATE TABLE ... AS
CREATE TABLE recno_ctas USING recno AS SELECT * FROM recno_source WHERE id <= 10;
SELECT COUNT(*) FROM recno_ctas;

-- Verify CTAS table uses recno
SELECT c.relname, am.amname
FROM pg_class c JOIN pg_am am ON c.relam = am.oid
WHERE c.relname = 'recno_ctas';

-- SELECT INTO (uses default AM, not recno)
SELECT * INTO recno_select_into FROM recno_source WHERE id > 40;
SELECT COUNT(*) FROM recno_select_into;

DROP TABLE recno_ctas;
DROP TABLE recno_select_into;
DROP TABLE recno_source;

-- =============================================
-- Unlogged tables
-- =============================================

CREATE UNLOGGED TABLE recno_unlogged (
    id serial PRIMARY KEY,
    data text
) USING recno;

INSERT INTO recno_unlogged (data) SELECT 'unlogged_' || i FROM generate_series(1, 20) i;
SELECT COUNT(*) FROM recno_unlogged;

-- DML on unlogged table
UPDATE recno_unlogged SET data = 'updated' WHERE id = 1;
DELETE FROM recno_unlogged WHERE id = 2;
SELECT COUNT(*) FROM recno_unlogged;

DROP TABLE recno_unlogged;

-- =============================================
-- Table with generated columns
-- =============================================

CREATE TABLE recno_generated (
    id serial PRIMARY KEY,
    price numeric(10,2),
    quantity integer,
    total numeric(10,2) GENERATED ALWAYS AS (price * quantity) STORED
) USING recno;

INSERT INTO recno_generated (price, quantity) VALUES (10.50, 3), (25.00, 2);
SELECT id, price, quantity, total FROM recno_generated ORDER BY id;

-- Update should recompute generated column
UPDATE recno_generated SET quantity = 5 WHERE id = 1;
SELECT id, price, quantity, total FROM recno_generated WHERE id = 1;

DROP TABLE recno_generated;

-- =============================================
-- Table with defaults and sequences
-- =============================================

CREATE SEQUENCE recno_custom_seq START 1000;

CREATE TABLE recno_defaults (
    id integer DEFAULT nextval('recno_custom_seq') PRIMARY KEY,
    created_at timestamp DEFAULT now(),
    status text DEFAULT 'pending',
    data text
) USING recno;

INSERT INTO recno_defaults (data) VALUES ('test1'), ('test2');
SELECT id, status, data FROM recno_defaults ORDER BY id;

DROP TABLE recno_defaults;
DROP SEQUENCE recno_custom_seq;

-- =============================================
-- Constraint tests
-- =============================================

-- PRIMARY KEY constraint
CREATE TABLE recno_pk (
    id serial PRIMARY KEY,
    value text
) USING recno;

INSERT INTO recno_pk (value) VALUES ('first'), ('second');

-- Should fail: duplicate PK
\set ON_ERROR_STOP off
INSERT INTO recno_pk VALUES (1, 'duplicate');
\set ON_ERROR_STOP on

DROP TABLE recno_pk;

-- CHECK constraint
CREATE TABLE recno_check (
    id serial PRIMARY KEY,
    value integer CHECK (value > 0),
    status text CHECK (status IN ('active', 'inactive', 'pending'))
) USING recno;

INSERT INTO recno_check (value, status) VALUES (1, 'active');
INSERT INTO recno_check (value, status) VALUES (100, 'pending');

-- These should fail
\set ON_ERROR_STOP off
INSERT INTO recno_check (value, status) VALUES (-1, 'active');
INSERT INTO recno_check (value, status) VALUES (1, 'invalid');
\set ON_ERROR_STOP on

SELECT id, value, status FROM recno_check ORDER BY id;

DROP TABLE recno_check;

-- UNIQUE constraint
CREATE TABLE recno_unique (
    id serial PRIMARY KEY,
    email text UNIQUE,
    code integer
) USING recno;

INSERT INTO recno_unique (email, code) VALUES ('a@test.com', 1);
INSERT INTO recno_unique (email, code) VALUES ('b@test.com', 2);

-- This should fail
\set ON_ERROR_STOP off
INSERT INTO recno_unique (email, code) VALUES ('a@test.com', 3);
\set ON_ERROR_STOP on

-- NULL in UNIQUE is allowed (multiple NULLs)
INSERT INTO recno_unique (email, code) VALUES (NULL, 4);
INSERT INTO recno_unique (email, code) VALUES (NULL, 5);

SELECT COUNT(*) FROM recno_unique WHERE email IS NULL;

DROP TABLE recno_unique;

-- FOREIGN KEY constraint
CREATE TABLE recno_fk_parent (
    id serial PRIMARY KEY,
    name text NOT NULL
) USING recno;

CREATE TABLE recno_fk_child (
    id serial PRIMARY KEY,
    parent_id integer REFERENCES recno_fk_parent(id) ON DELETE CASCADE,
    description text
) USING recno;

INSERT INTO recno_fk_parent (name) VALUES ('Parent A'), ('Parent B');
INSERT INTO recno_fk_child (parent_id, description) VALUES (1, 'Child of A'), (2, 'Child of B');

-- CASCADE delete
DELETE FROM recno_fk_parent WHERE id = 1;
SELECT COUNT(*) FROM recno_fk_child WHERE parent_id = 1;

-- Referential integrity violation
\set ON_ERROR_STOP off
INSERT INTO recno_fk_child (parent_id, description) VALUES (999, 'orphan');
\set ON_ERROR_STOP on

-- Cross-AM foreign key: recno child referencing heap parent
CREATE TABLE heap_parent (
    id serial PRIMARY KEY,
    name text
) USING heap;

INSERT INTO heap_parent (name) VALUES ('heap_parent_1');

CREATE TABLE recno_fk_cross (
    id serial PRIMARY KEY,
    parent_id integer REFERENCES heap_parent(id),
    data text
) USING recno;

INSERT INTO recno_fk_cross (parent_id, data) VALUES (1, 'cross-am child');

SELECT rfc.data, hp.name
FROM recno_fk_cross rfc JOIN heap_parent hp ON rfc.parent_id = hp.id;

DROP TABLE recno_fk_cross;
DROP TABLE heap_parent;
DROP TABLE recno_fk_child;
DROP TABLE recno_fk_parent;

-- EXCLUDE constraint
CREATE TABLE recno_exclude_test (
    id serial PRIMARY KEY,
    range_val int4range,
    EXCLUDE USING gist (range_val WITH &&)
) USING recno;

INSERT INTO recno_exclude_test (range_val) VALUES ('[1, 5)');
INSERT INTO recno_exclude_test (range_val) VALUES ('[10, 20)');

-- Should fail (overlapping)
\set ON_ERROR_STOP off
INSERT INTO recno_exclude_test (range_val) VALUES ('[3, 8)');
\set ON_ERROR_STOP on

DROP TABLE recno_exclude_test;
