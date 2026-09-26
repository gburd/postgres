-- Tests for the sparse bitmap set (sbm) ADT
CREATE EXTENSION test_sbm;

-- A set is represented as a bigint[] of its members; NULL means the empty set.
-- Members must be non-negative.

-- ------------------------------------------------------------------
-- Cardinality, emptiness, min/max
-- ------------------------------------------------------------------
SELECT test_sbm_cardinality(NULL);
SELECT test_sbm_cardinality('{}');
SELECT test_sbm_cardinality('{42}');
SELECT test_sbm_cardinality('{1,3,5,7,9}');
-- duplicates collapse
SELECT test_sbm_cardinality('{5,5,5,5}');

SELECT test_sbm_is_empty(NULL);
SELECT test_sbm_is_empty('{}');
SELECT test_sbm_is_empty('{0}');

SELECT test_sbm_minimum(NULL);
SELECT test_sbm_minimum('{7,3,9,1}');
SELECT test_sbm_maximum(NULL);
SELECT test_sbm_maximum('{7,3,9,1}');
-- across chunk boundaries (chunk window is 2048 bits)
SELECT test_sbm_minimum('{2047,2048,4096,100000}');
SELECT test_sbm_maximum('{2047,2048,4096,100000}');

-- errors: negative members
SELECT test_sbm_cardinality('{-1}');
SELECT test_sbm_add('{}', -5);

-- ------------------------------------------------------------------
-- contains
-- ------------------------------------------------------------------
SELECT test_sbm_contains('{1,3,5}', 1);
SELECT test_sbm_contains('{1,3,5}', 2);
SELECT test_sbm_contains('{1,3,5}', 5);
SELECT test_sbm_contains(NULL, 1);
SELECT test_sbm_contains('{1,3,5}', -1);
-- large / cross-chunk indexes
SELECT test_sbm_contains('{2048,1000000}', 2048);
SELECT test_sbm_contains('{2048,1000000}', 1000000);
SELECT test_sbm_contains('{2048,1000000}', 999999);

-- ------------------------------------------------------------------
-- add / remove / assign  (returns the resulting set, sorted)
-- ------------------------------------------------------------------
SELECT test_sbm_add(NULL, 5);
SELECT test_sbm_add('{1,3}', 2);
-- idempotent add
SELECT test_sbm_add('{1,2,3}', 2);
-- add across a chunk boundary
SELECT test_sbm_add('{5}', 2048);
-- allocate-on-first-use: the three grow entry points build from a NULL Sbm *
SELECT test_sbm_add_grow_from_null();   -- expect {1,2,100,200,300}
SELECT test_sbm_remove('{1,2,3}', 2);
SELECT test_sbm_remove('{1,2,3}', 4);   -- absent, no-op
SELECT test_sbm_remove(NULL, 4);
SELECT test_sbm_assign('{1,3}', 2, true);
SELECT test_sbm_assign('{1,2,3}', 2, false);

-- ------------------------------------------------------------------
-- ranges
-- ------------------------------------------------------------------
SELECT test_sbm_add_range('{}', 5, 7);   -- half-open [lo,hi): {5,6}
SELECT test_sbm_add_range('{100}', 5, 5);   -- empty range [5,5) -> unchanged
-- range spanning a chunk boundary
SELECT test_sbm_cardinality(test_sbm_add_range('{}', 2040, 2060));
-- a long run (more than one full 2048-bit chunk)
SELECT test_sbm_cardinality(test_sbm_add_range('{}', 0, 5000));
SELECT test_sbm_remove_range('{1,2,3,4,5}', 2, 4);
SELECT test_sbm_flip_range('{1,3,5}', 1, 5);
SELECT test_sbm_add_range('{}', 7, 3);   -- error: hi < lo

-- ------------------------------------------------------------------
-- rank / select / span
-- ------------------------------------------------------------------
-- rank(set, x, y, true) = count of set bits in [x, y]
SELECT test_sbm_rank('{1,3,5,7,9}', 0, 10, true);
SELECT test_sbm_rank('{1,3,5,7,9}', 0, 4, true);
SELECT test_sbm_rank('{1,3,5,7,9}', 4, 10, true);
-- select(set, n, true) = the (0-based) n-th set bit
SELECT test_sbm_select('{1,3,5,7,9}', 0, true);
SELECT test_sbm_select('{1,3,5,7,9}', 2, true);
SELECT test_sbm_select('{1,3,5,7,9}', 4, true);
SELECT test_sbm_select('{1,3,5,7,9}', 5, true);   -- out of range -> NULL
-- span(set, start, len, true) = first index where len consecutive set bits start
SELECT test_sbm_span(test_sbm_add_range('{}', 10, 20), 0, 5, true);
SELECT test_sbm_span('{1,3,5}', 0, 3, true);       -- no run of 3 -> NULL

-- rank/select/span with value=false (unset-bit variants)
SELECT test_sbm_rank('{1,3,5,7,9}', 0, 10, false);   -- unset bits in [0,10]
SELECT test_sbm_rank('{1,3,5,7,9}', 2, 8, false);
SELECT test_sbm_select('{1,3,5,7,9}', 0, false);      -- first unset bit (0)
SELECT test_sbm_select('{1,3,5,7,9}', 3, false);
SELECT test_sbm_span(test_sbm_add_range('{}', 0, 10), 0, 3, false);  -- no unset run early
SELECT test_sbm_span('{5}', 0, 3, false);             -- unset run at 0

-- ------------------------------------------------------------------
-- set operations
-- ------------------------------------------------------------------
SELECT test_sbm_union('{1,3,5}', '{3,5,7}');
SELECT test_sbm_union('{1,3,5}', NULL);
SELECT test_sbm_union(NULL, NULL);
-- union spanning chunks
SELECT test_sbm_union('{1,2}', '{2048,4096}');
SELECT test_sbm_intersection('{1,3,5}', '{3,5,7}');
SELECT test_sbm_intersection('{1,3,5}', '{2,4,6}');
SELECT test_sbm_intersection('{1,3,5}', NULL);
SELECT test_sbm_difference('{1,3,5,7}', '{3,7}');
SELECT test_sbm_difference('{1,3,5}', '{1,3,5}');
SELECT test_sbm_difference(NULL, '{1}');
SELECT test_sbm_xor('{1,2,3}', '{2,3,4}');
SELECT test_sbm_xor('{1,2,3}', '{1,2,3}');

SELECT test_sbm_union_cardinality('{1,3,5}', '{3,5,7}');
SELECT test_sbm_intersection_cardinality('{1,3,5}', '{3,5,7}');
SELECT test_sbm_difference_cardinality('{1,3,5,7}', '{3,7}');
SELECT test_sbm_xor_cardinality('{1,2,3}', '{2,3,4}');

-- ------------------------------------------------------------------
-- comparisons / relations
-- ------------------------------------------------------------------
SELECT test_sbm_equals('{1,3,5}', '{1,3,5}');
SELECT test_sbm_equals('{1,3,5}', '{1,3,5,7}');
SELECT test_sbm_equals(NULL, '{}');
SELECT test_sbm_equals(NULL, NULL);
SELECT test_sbm_compare('{1,3}', '{1,3}');
SELECT test_sbm_compare('{1,3}', '{1,3,5}');
SELECT test_sbm_compare('{1,3,5}', '{1,3}');
SELECT test_sbm_is_subset('{1,3}', '{1,3,5}');
SELECT test_sbm_is_subset('{1,3,5}', '{1,3}');
SELECT test_sbm_is_subset(NULL, '{1}');
SELECT test_sbm_is_superset('{1,3,5}', '{1,3}');
SELECT test_sbm_overlap('{1,3,5}', '{5,7,9}');
SELECT test_sbm_overlap('{1,3,5}', '{2,4,6}');
SELECT test_sbm_nonempty_difference('{1,3,5}', '{1,3}');
SELECT test_sbm_nonempty_difference('{1,3}', '{1,3,5}');
-- subset_compare: 0 EQUAL, 1 A-subset-of-B, 2 B-subset-of-A, 3 DIFFERENT
SELECT test_sbm_subset_compare('{1,3}', '{1,3}');
SELECT test_sbm_subset_compare('{1,3}', '{1,3,5}');
SELECT test_sbm_subset_compare('{1,3,5}', '{1,3}');
SELECT test_sbm_subset_compare('{1,3}', '{2,4}');
-- jaccard: |A∩B| / |A∪B|
SELECT test_sbm_jaccard_index('{1,2,3,4}', '{3,4,5,6}');
SELECT test_sbm_jaccard_index('{1,2}', '{1,2}');
SELECT test_sbm_jaccard_index('{1}', '{2}');

-- ------------------------------------------------------------------
-- membership classification and navigation
-- ------------------------------------------------------------------
-- membership: 0 EMPTY, 1 SINGLETON, 2 MULTIPLE
SELECT test_sbm_membership(NULL);
SELECT test_sbm_membership('{42}');
SELECT test_sbm_membership('{1,2}');
SELECT test_sbm_singleton_member('{42}');
SELECT test_sbm_singleton_member('{1,2}');    -- not singleton -> NULL
SELECT test_sbm_singleton_member(NULL);       -- empty -> NULL
-- next_member: exclusive lower bound; -1 starts at the first member
SELECT test_sbm_next_member('{5,10,15,20}', -1);
SELECT test_sbm_next_member('{5,10,15,20}', 5);
SELECT test_sbm_next_member('{5,10,15,20}', 20);   -- past end -> NULL
-- prev_member: exclusive upper bound; -1 starts at the last member
SELECT test_sbm_prev_member('{5,10,15,20}', -1);
SELECT test_sbm_prev_member('{5,10,15,20}', 20);
SELECT test_sbm_prev_member('{5,10,15,20}', 5);    -- past beginning -> NULL
SELECT test_sbm_pop_first('{5,10,15}');
SELECT test_sbm_pop_last('{5,10,15}');
SELECT test_sbm_pop_first(NULL);

-- ------------------------------------------------------------------
-- structural transforms
-- ------------------------------------------------------------------
SELECT test_sbm_offset('{1,3,5}', 10);
SELECT test_sbm_offset('{11,13,15}', -10);
-- offset that drops members below 0
SELECT test_sbm_offset('{1,3,5}', -2);
-- large |offset| (would overflow a signed src_start+offset intermediate):
-- a big negative shift that drops every member returns the empty set
SELECT test_sbm_offset('{1,3,5}', -9000000000000000000);
-- a big negative shift where the source bit is high enough to survive:
-- bit 9000000000000000100 shifted down by 9e18 lands at 100
SELECT test_sbm_offset('{9000000000000000100}', -9000000000000000000);
SELECT test_sbm_extract_range('{1,3,5,7,9}', 3, 7);   -- half-open [3,7): {3,5}
SELECT test_sbm_extract_range('{1,3,5,7,9}', 100, 200);   -- empty

-- hash is stable and order-independent
SELECT test_sbm_hash('{1,3,5}') = test_sbm_hash('{5,3,1}') AS hash_order_independent;
SELECT test_sbm_hash('{1,3,5}') <> test_sbm_hash('{2,4,6}') AS hash_distinguishes;
SELECT test_sbm_hash(NULL) = test_sbm_hash('{}') AS hash_empty_equal;

-- split: returns {|left|, |right|} and internally checks the split invariants
SELECT test_sbm_split('{1,3,5,7,9}', 5);
SELECT test_sbm_split(test_sbm_add_range('{}', 0, 100), 50);
SELECT test_sbm_split('{1,2,3}', 0);     -- everything moves right
SELECT test_sbm_split('{1,2,3}', 100);   -- everything stays left

-- ------------------------------------------------------------------
-- round-trips (array -> sbm -> array, and through serialization)
-- ------------------------------------------------------------------
SELECT test_sbm_roundtrip('{1,3,5,7,9}');
SELECT test_sbm_roundtrip('{2047,2048,2049,100000}');
SELECT test_sbm_roundtrip(NULL);
SELECT test_sbm_serialize_roundtrip('{1,3,5,7,9}');
SELECT test_sbm_cardinality(test_sbm_serialize_roundtrip(test_sbm_add_range('{}', 0, 3000)));
SELECT test_sbm_serialize_roundtrip(NULL);

-- ------------------------------------------------------------------
-- randomized differential test against a sorted-array oracle
-- (fixed seeds for reproducibility; each returns the final cardinality)
-- ------------------------------------------------------------------
-- dense, small universe
SELECT test_sbm_random_operations(1, 20000, 0, 1024) >= 0 AS ok;
-- sparse, large universe (many chunks, clustered gaps)
SELECT test_sbm_random_operations(2, 20000, 0, 1000000) >= 0 AS ok;
-- very sparse, huge universe
SELECT test_sbm_random_operations(3, 10000, 0, 100000000) >= 0 AS ok;
-- long contiguous runs (indexes clustered near each other)
SELECT test_sbm_random_operations(4, 20000, 0, 256) >= 0 AS ok;

-- ------------------------------------------------------------------
-- encoding transitions (small <-> chunk, sparse <-> RLE), every
-- allocation lineage, splits at every interesting position, and every
-- binary operation over operands in each encoding, each step checked in C
-- against an independent bool-array oracle
-- ------------------------------------------------------------------
SELECT test_sbm_internals();

-- ------------------------------------------------------------------
-- aliases (or/and/andnot) -- each verifies it matches its primary in C
-- ------------------------------------------------------------------
SELECT test_sbm_or('{1,3,5}', '{3,5,7}');
SELECT test_sbm_and('{1,3,5}', '{3,5,7}');
SELECT test_sbm_andnot('{1,3,5,7}', '{3,7}');
SELECT test_sbm_or(NULL, '{2,4}');

-- ------------------------------------------------------------------
-- in-place set ops -- each verifies it matches the out-of-place result
-- ------------------------------------------------------------------
SELECT test_sbm_union_inplace('{1,3,5}', '{3,5,7}');
SELECT test_sbm_union_inplace(NULL, '{2,4}');
SELECT test_sbm_intersection_inplace('{1,3,5}', '{3,5,7}');
SELECT test_sbm_intersection_inplace('{1,3,5}', '{2,4,6}');
SELECT test_sbm_difference_inplace('{1,3,5,7}', '{3,7}');
SELECT test_sbm_difference_inplace('{1,3,5}', '{1,3,5}');
SELECT test_sbm_xor_inplace('{1,2,3}', '{2,3,4}');
-- in-place across chunk boundaries and long runs (exercises replace_buffer)
SELECT test_sbm_cardinality(test_sbm_union_inplace(
         test_sbm_add_range('{}', 0, 3000), test_sbm_add_range('{}', 2500, 5000)));

-- ------------------------------------------------------------------
-- constructors
-- ------------------------------------------------------------------
SELECT test_sbm_create_singleton(42);
SELECT test_sbm_create_singleton(100000);
SELECT test_sbm_create_from_range(5, 10);      -- half-open [5,10): {5..9}
SELECT test_sbm_cardinality(test_sbm_create_from_range(0, 5000));  -- long run
-- from_array sorts internally (exercises the compare helper)
SELECT test_sbm_create_from_array('{9,3,7,1,5,3,9}');   -- unsorted + duplicates
SELECT test_sbm_create_from_array('{}');
SELECT test_sbm_create_from_array('{2048,1,4096,2}');   -- across chunks

-- ------------------------------------------------------------------
-- bulk add / to_array / contains_many
-- ------------------------------------------------------------------
SELECT test_sbm_add_many('{5,1,3,1,9,7}');          -- unsorted + duplicate
SELECT test_sbm_cardinality(test_sbm_add_many(
         (SELECT array_agg(g) FROM generate_series(0, 5000) g)::bigint[]));
SELECT test_sbm_to_array('{9,3,7,1,5}');
SELECT test_sbm_to_array(NULL);
-- contains_many: how many of the probes are present (ascending probes)
SELECT test_sbm_contains_many('{1,3,5,7,9}', '{0,1,4,5,10}');
SELECT test_sbm_contains_many('{2048,4096}', '{100,2048,3000,4096}');
SELECT test_sbm_contains_many(NULL, '{1,2,3}');

-- ------------------------------------------------------------------
-- scan / introspection
-- ------------------------------------------------------------------
-- scan sums popcounts; must equal cardinality
SELECT test_sbm_scan_cardinality('{1,3,5,7,9}');
SELECT test_sbm_scan_cardinality(test_sbm_add_range('{}', 0, 3000));
SELECT test_sbm_scan_cardinality(NULL);
SELECT test_sbm_capacity_remaining('{1,2,3}') BETWEEN 0 AND 100 AS ok;
SELECT test_sbm_capacity_remaining(NULL) BETWEEN 0 AND 100 AS ok;
-- shrink_to_fit preserves the set
SELECT test_sbm_shrink_to_fit('{1,3,5,7,9}');
SELECT test_sbm_shrink_to_fit(test_sbm_add_range('{}', 0, 3000)) IS NOT NULL AS ok;
SELECT test_sbm_shrink_to_fit(NULL);
-- statistics: {chunks_total, bits_set}; bits_set == cardinality (checked in C)
SELECT test_sbm_statistics('{1,3,5,7,9}');
SELECT test_sbm_statistics(test_sbm_add_range('{}', 0, 5000)) AS stats_long_run;
SELECT test_sbm_statistics(NULL);

-- ------------------------------------------------------------------
-- add_grow_cursor: ascending batch through one cursor, with growth
-- ------------------------------------------------------------------
SELECT test_sbm_add_grow_cursor_check('{1,3,5,7,9}');
SELECT test_sbm_cardinality(test_sbm_add_grow_cursor_check(
         (SELECT array_agg(g) FROM generate_series(0, 4000) g)::bigint[]));

-- ------------------------------------------------------------------
-- buffer lifecycle: wrap / open / open_copy / owned_copy round-trips
-- ------------------------------------------------------------------
SELECT test_sbm_wrap_roundtrip('{1,3,5,7,9}');
SELECT test_sbm_wrap_roundtrip(test_sbm_add_range('{}', 0, 3000)) IS NOT NULL AS ok;
SELECT test_sbm_wrap_roundtrip(NULL);
SELECT test_sbm_owned_copy_roundtrip('{2047,2048,2049,100000}');
SELECT test_sbm_owned_copy_roundtrip(NULL);

-- ------------------------------------------------------------------
-- locator: order-statistic index; contains/rank/select cross-checked in C
-- ------------------------------------------------------------------
SELECT test_sbm_locator_check('{1,3,5,7,9}');
SELECT test_sbm_locator_check(test_sbm_add_range('{}', 0, 5000));
SELECT test_sbm_locator_check('{2048,4096,1000000}');
SELECT test_sbm_locator_check(NULL);

-- ------------------------------------------------------------------
-- fill_factor and cursor-cached contains
-- ------------------------------------------------------------------
-- fill_factor: set bits / span [min,max]; a dense range approaches 1.0
SELECT test_sbm_fill_factor_val('{1,3,5,7,9}');
SELECT test_sbm_fill_factor_val(test_sbm_add_range('{}', 0, 100)) = 1.0 AS full;
SELECT test_sbm_fill_factor_val(NULL);
-- contains_cached: ascending probes threading one cached cursor
SELECT test_sbm_contains_cached('{1,3,5,7,9}', '{0,1,4,5,10}');
SELECT test_sbm_contains_cached(test_sbm_add_range('{}', 0, 5000),
                               '{0,100,2048,4096,4999,5000}');
SELECT test_sbm_contains_cached(NULL, '{1,2,3}');

-- ------------------------------------------------------------------
-- multi-chunk all-ONES runs through the run emitter (xor / extract_range)
-- ------------------------------------------------------------------
-- xor of two long ranges yields a long run in the result (emit_ones_run)
SELECT test_sbm_cardinality(test_sbm_xor(test_sbm_add_range('{}', 0, 6000),
                                         test_sbm_add_range('{}', 3000, 3001)));
-- extract a multi-chunk sub-run (emit_ones_run + append_ones_chunks)
SELECT test_sbm_cardinality(test_sbm_extract_range(
         test_sbm_add_range('{}', 0, 10000), 1000, 8000));
-- broad randomized battery across universe shapes (drives chunk transitions,
-- splits/merges, capacity edges, cursor paths); each verifies against the oracle
SELECT test_sbm_random_operations(10, 60000, 0, 512) >= 0 AS ok;
SELECT test_sbm_random_operations(11, 60000, 0, 4096) >= 0 AS ok;
SELECT test_sbm_random_operations(12, 60000, 0, 65536) >= 0 AS ok;
SELECT test_sbm_random_operations(13, 60000, 0, 20000) >= 0 AS ok;
SELECT test_sbm_random_operations(14, 40000, 1000000, 1010000) >= 0 AS ok;  -- high, offset window
SELECT test_sbm_random_operations(15, 80000, 0, 2048) >= 0 AS ok;           -- exactly one chunk
SELECT test_sbm_random_operations(16, 80000, 0, 6144) >= 0 AS ok;           -- three chunks

-- single-chunk churn: heavy add/remove within one 2048-bit window creates
-- reduced-capacity (NONE-flagged) chunks and then re-adds into them, exercising
-- the capacity-increase path.
SELECT test_sbm_random_operations(5, 40000, 0, 2047) >= 0 AS ok;
SELECT test_sbm_random_operations(6, 40000, 1024, 3071) >= 0 AS ok;

-- a NONE-flagged chunk that must grow capacity (chunk_increase_capacity):
-- set a high bit in a chunk, then fill the lower slots so the chunk's
-- reduced capacity has to be increased.
SELECT test_sbm_cardinality(test_sbm_add_range(test_sbm_add('{}', 2000), 0, 2048));

DROP EXTENSION test_sbm;
