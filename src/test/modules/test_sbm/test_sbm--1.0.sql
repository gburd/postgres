/* src/test/modules/test_sbm/test_sbm--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION test_sbm" to load this file. \quit

-- A set is passed and returned as a bigint[] of its members; SQL NULL means
-- the empty set.

-- Queries
CREATE FUNCTION test_sbm_cardinality(bigint[])
RETURNS bigint
AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_sbm_contains(bigint[], bigint)
RETURNS boolean
AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_sbm_is_empty(bigint[])
RETURNS boolean
AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_sbm_minimum(bigint[])
RETURNS bigint
AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_sbm_maximum(bigint[])
RETURNS bigint
AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_sbm_fill_factor(bigint[])
RETURNS float8
AS 'MODULE_PATHNAME' LANGUAGE C;

-- Mutations (return the resulting set)
CREATE FUNCTION test_sbm_add(bigint[], bigint)
RETURNS bigint[]
AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_sbm_remove(bigint[], bigint)
RETURNS bigint[]
AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_sbm_assign(bigint[], bigint, boolean)
RETURNS bigint[]
AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_sbm_add_range(bigint[], bigint, bigint)
RETURNS bigint[]
AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_sbm_remove_range(bigint[], bigint, bigint)
RETURNS bigint[]
AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_sbm_flip_range(bigint[], bigint, bigint)
RETURNS bigint[]
AS 'MODULE_PATHNAME' LANGUAGE C;

-- Rank / select / span
CREATE FUNCTION test_sbm_rank(bigint[], bigint, bigint, boolean)
RETURNS bigint
AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_sbm_select(bigint[], bigint, boolean)
RETURNS bigint
AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_sbm_span(bigint[], bigint, bigint, boolean)
RETURNS bigint
AS 'MODULE_PATHNAME' LANGUAGE C;

-- Set operations
CREATE FUNCTION test_sbm_union(bigint[], bigint[])
RETURNS bigint[]
AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_sbm_intersection(bigint[], bigint[])
RETURNS bigint[]
AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_sbm_difference(bigint[], bigint[])
RETURNS bigint[]
AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_sbm_xor(bigint[], bigint[])
RETURNS bigint[]
AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_sbm_union_cardinality(bigint[], bigint[])
RETURNS bigint
AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_sbm_intersection_cardinality(bigint[], bigint[])
RETURNS bigint
AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_sbm_difference_cardinality(bigint[], bigint[])
RETURNS bigint
AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_sbm_xor_cardinality(bigint[], bigint[])
RETURNS bigint
AS 'MODULE_PATHNAME' LANGUAGE C;

-- Comparisons / relations
CREATE FUNCTION test_sbm_equals(bigint[], bigint[])
RETURNS boolean
AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_sbm_compare(bigint[], bigint[])
RETURNS integer
AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_sbm_is_subset(bigint[], bigint[])
RETURNS boolean
AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_sbm_is_superset(bigint[], bigint[])
RETURNS boolean
AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_sbm_overlap(bigint[], bigint[])
RETURNS boolean
AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_sbm_nonempty_difference(bigint[], bigint[])
RETURNS boolean
AS 'MODULE_PATHNAME' LANGUAGE C;

-- Returns: 0 EQUAL, 1 A-subset-of-B, 2 B-subset-of-A, 3 DIFFERENT
CREATE FUNCTION test_sbm_subset_compare(bigint[], bigint[])
RETURNS integer
AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_sbm_jaccard_index(bigint[], bigint[])
RETURNS float8
AS 'MODULE_PATHNAME' LANGUAGE C;

-- Membership classification and navigation
-- Returns: 0 EMPTY, 1 SINGLETON, 2 MULTIPLE
CREATE FUNCTION test_sbm_membership(bigint[])
RETURNS integer
AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_sbm_singleton_member(bigint[])
RETURNS bigint
AS 'MODULE_PATHNAME' LANGUAGE C;

-- Second arg is the exclusive lower bound; a negative value starts at the
-- first member.
CREATE FUNCTION test_sbm_next_member(bigint[], bigint)
RETURNS bigint
AS 'MODULE_PATHNAME' LANGUAGE C;

-- Second arg is the exclusive upper bound; a negative value starts at the
-- last member.
CREATE FUNCTION test_sbm_prev_member(bigint[], bigint)
RETURNS bigint
AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_sbm_pop_first(bigint[])
RETURNS bigint
AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_sbm_pop_last(bigint[])
RETURNS bigint
AS 'MODULE_PATHNAME' LANGUAGE C;

-- Structural transforms
CREATE FUNCTION test_sbm_offset(bigint[], bigint)
RETURNS bigint[]
AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_sbm_extract_range(bigint[], bigint, bigint)
RETURNS bigint[]
AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_sbm_hash(bigint[])
RETURNS bigint
AS 'MODULE_PATHNAME' LANGUAGE C;

-- Splits the set at the given index and internally verifies the split
-- invariants; returns {|left|, |right|}.
CREATE FUNCTION test_sbm_split(bigint[], bigint)
RETURNS bigint[]
AS 'MODULE_PATHNAME' LANGUAGE C;

-- Round-trips
CREATE FUNCTION test_sbm_roundtrip(bigint[])
RETURNS bigint[]
AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_sbm_serialize_roundtrip(bigint[])
RETURNS bigint[]
AS 'MODULE_PATHNAME' LANGUAGE C;

-- Randomized differential test against a sorted-array oracle
CREATE FUNCTION test_sbm_random_operations(bigint, integer, integer, integer)
RETURNS integer
AS 'MODULE_PATHNAME' LANGUAGE C;

-- sbm_removal_bound against random subsets of random sets
CREATE FUNCTION test_sbm_removal_bound(bigint, integer)
RETURNS integer
AS 'MODULE_PATHNAME' LANGUAGE C STRICT;

-- Aliases, in-place ops, constructors, bulk ops, introspection, buffer
-- lifecycle, scan, and the locator family (additional coverage).
CREATE FUNCTION test_sbm_or(bigint[], bigint[]) RETURNS bigint[]
AS 'MODULE_PATHNAME' LANGUAGE C;
CREATE FUNCTION test_sbm_and(bigint[], bigint[]) RETURNS bigint[]
AS 'MODULE_PATHNAME' LANGUAGE C;
CREATE FUNCTION test_sbm_andnot(bigint[], bigint[]) RETURNS bigint[]
AS 'MODULE_PATHNAME' LANGUAGE C;
CREATE FUNCTION test_sbm_union_inplace(bigint[], bigint[]) RETURNS bigint[]
AS 'MODULE_PATHNAME' LANGUAGE C;
CREATE FUNCTION test_sbm_intersection_inplace(bigint[], bigint[]) RETURNS bigint[]
AS 'MODULE_PATHNAME' LANGUAGE C;
CREATE FUNCTION test_sbm_difference_inplace(bigint[], bigint[]) RETURNS bigint[]
AS 'MODULE_PATHNAME' LANGUAGE C;
CREATE FUNCTION test_sbm_xor_inplace(bigint[], bigint[]) RETURNS bigint[]
AS 'MODULE_PATHNAME' LANGUAGE C;
CREATE FUNCTION test_sbm_create_singleton(bigint) RETURNS bigint[]
AS 'MODULE_PATHNAME' LANGUAGE C;
CREATE FUNCTION test_sbm_create_from_range(bigint, bigint) RETURNS bigint[]
AS 'MODULE_PATHNAME' LANGUAGE C;
CREATE FUNCTION test_sbm_create_from_array(bigint[]) RETURNS bigint[]
AS 'MODULE_PATHNAME' LANGUAGE C;
CREATE FUNCTION test_sbm_add_many(bigint[]) RETURNS bigint[]
AS 'MODULE_PATHNAME' LANGUAGE C;
CREATE FUNCTION test_sbm_to_array(bigint[]) RETURNS bigint[]
AS 'MODULE_PATHNAME' LANGUAGE C;
CREATE FUNCTION test_sbm_contains_many(bigint[], bigint[]) RETURNS integer
AS 'MODULE_PATHNAME' LANGUAGE C;
CREATE FUNCTION test_sbm_scan_cardinality(bigint[]) RETURNS bigint
AS 'MODULE_PATHNAME' LANGUAGE C;
CREATE FUNCTION test_sbm_capacity_remaining(bigint[]) RETURNS float8
AS 'MODULE_PATHNAME' LANGUAGE C;
CREATE FUNCTION test_sbm_shrink_to_fit(bigint[]) RETURNS bigint[]
AS 'MODULE_PATHNAME' LANGUAGE C;
-- Returns {chunks_total, bits_set}
CREATE FUNCTION test_sbm_statistics(bigint[]) RETURNS bigint[]
AS 'MODULE_PATHNAME' LANGUAGE C;
CREATE FUNCTION test_sbm_add_grow_cursor_check(bigint[]) RETURNS bigint[]
AS 'MODULE_PATHNAME' LANGUAGE C;
CREATE FUNCTION test_sbm_wrap_roundtrip(bigint[]) RETURNS bigint[]
AS 'MODULE_PATHNAME' LANGUAGE C;
CREATE FUNCTION test_sbm_owned_copy_roundtrip(bigint[]) RETURNS bigint[]
AS 'MODULE_PATHNAME' LANGUAGE C;
-- Builds a locator, cross-checks contains/rank/select, returns cardinality
CREATE FUNCTION test_sbm_locator_check(bigint[]) RETURNS bigint
AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_sbm_fill_factor_val(bigint[]) RETURNS float8
AS 'MODULE_PATHNAME' LANGUAGE C;
CREATE FUNCTION test_sbm_contains_cached(bigint[], bigint[]) RETURNS integer
AS 'MODULE_PATHNAME' LANGUAGE C;


CREATE FUNCTION test_sbm_benchmark(
    n bigint DEFAULT 10000,
    universe bigint DEFAULT 1000000,
    seed bigint DEFAULT 42,
    OUT structure text,
    OUT build_ns_per_op double precision,
    OUT scan_ns_per_op double precision,
    OUT bytes bigint)
RETURNS SETOF record
AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_sbm_add_grow_from_null() RETURNS bigint[]
AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_sbm_internals() RETURNS boolean
AS 'MODULE_PATHNAME' LANGUAGE C STRICT;

COMMENT ON EXTENSION test_sbm IS 'Test code for the sparse bitmap set (sbm) ADT';
