CREATE EXTENSION IF NOT EXISTS test_bitmapset;

-- make_singleton(42)
SELECT test_bms_make_singleton(42) = '(b 42)' as result;
-- make_singleton(0)
SELECT test_bms_make_singleton(0) = '(b 0)' as result;
-- make_singleton(1000)
SELECT test_bms_make_singleton(1000) = '(b 1000)' as result;

-- add_member(NULL, 10)
SELECT test_bms_add_member('(b)', 10) = '(b 10)' as result;
-- add_member consistency
SELECT test_bms_add_member('(b)', 10) as result;
-- add_member to existing
SELECT test_bms_add_member('(b 5)', 10) = '(b 5 10)' as result;
-- add_member sorted
SELECT test_bms_add_member('(b 10)', 5) = '(b 5 10)' as result;
-- add_member idempotent
SELECT test_bms_add_member('(b 10)', 10) as result;


-- del_member from NULL
SELECT test_bms_del_member('(b)', 10) IS NULL as result;
-- del_member singleton becomes empty
SELECT test_bms_del_member('(b 10)', 10) IS NULL as result;
-- del_member no change
SELECT test_bms_del_member('(b 10)', 5) as result;
-- del_member from middle
SELECT test_bms_del_member('(b 1 2 3)', 2) = '(b 1 3)' as result;
-- del_member triggers realloc
SELECT test_bms_del_member(test_bms_del_member('(b 0 31 32 63 64)', 32), 63) as result;
-- del_member word boundary
SELECT test_bms_del_member(test_bms_add_range('(b)', 30, 34), 32) as result;

-- union operations
SELECT 'union overlapping sets' as test,
       test_bms_union('(b 1 3 5)', '(b 3 5 7)') = '(b 1 3 5 7)' as result
UNION ALL
SELECT 'union with NULL' as test,
       test_bms_union('(b 1 3 5)', '(b)') = '(b 1 3 5)' as result
UNION ALL
SELECT 'union NULL with NULL' as test,
       test_bms_union('(b)', '(b)') IS NULL as result;
SELECT 'overlapping ranges' as test,
       test_bms_union(
           test_bms_add_range('(b)', 0, 15),
           test_bms_add_range('(b)', 10, 20)
       ) as result;

-- intersection operations
SELECT 'intersect overlapping sets' as test,
       test_bms_intersect('(b 1 3 5)', '(b 3 5 7)') = '(b 3 5)' as result
UNION ALL
SELECT 'intersect disjoint sets' as test,
       test_bms_intersect('(b 1 3 5)', '(b 2 4 6)') IS NULL as result
UNION ALL
SELECT 'intersect with NULL' as test,
       test_bms_intersect('(b 1 3 5)', '(b)') IS NULL as result;

-- bms_int_members
SELECT 'int(ersect) overlapping sets' as test,
       test_bms_int_members('(b 1 3 5)', '(b 3 5 7)') = '(b 3 5)' as result
UNION ALL
SELECT 'int(ersect) disjoint sets' as test,
       test_bms_int_members('(b 1 3 5)', '(b 2 4 6)') IS NULL as result
UNION ALL
SELECT 'int(ersect) with NULL' as test,
       test_bms_int_members('(b 1 3 5)', '(b)') IS NULL as result
UNION ALL
SELECT 'int(ersect) members' as test,
	test_bms_int_members('(b 0 31 32 63 64)', '(b 31 32 64 65)') = '(b 31 32 64)' as result;

-- difference operations
SELECT 'difference overlapping sets' as test,
       test_bms_difference('(b 1 3 5)', '(b 3 5 7)') = '(b 1)' as result
UNION ALL
SELECT 'difference disjoint sets' as test,
       test_bms_difference('(b 1 3 5)', '(b 2 4 6)') = '(b 1 3 5)' as result
UNION ALL
SELECT 'difference identical sets' as test,
       test_bms_difference('(b 1 3 5)', '(b 1 3 5)') IS NULL as result;
SELECT 'difference subtraction edge case' as test,
       test_bms_difference(
           test_bms_add_range('(b)', 0, 100),
           test_bms_add_range('(b)', 50, 150)
       ) as result;
SELECT 'difference subtract to empty' as test,
       test_bms_difference('(b 42)', '(b 42)') as result;

-- membership tests
SELECT 'is_member existing' as test, test_bms_is_member('(b 1 3 5)', 1) = true as result
UNION ALL
SELECT 'is_member missing' as test, test_bms_is_member('(b 1 3 5)', 2) = false as result
UNION ALL
SELECT 'is_member existing middle' as test, test_bms_is_member('(b 1 3 5)', 3) = true as result
UNION ALL
SELECT 'is_member NULL set' as test, test_bms_is_member('(b)', 1) = false as result;

-- num_members NULL
SELECT test_bms_num_members('(b)') = 0 as result;
-- num_members small set
SELECT test_bms_num_members('(b 1 3 5)') = 3 as result;
-- num_members larger set
SELECT test_bms_num_members('(b 2 4 6 8 10)') = 5 as result;

-- set equality and comparison
SELECT 'equal NULL NULL' as test, test_bms_equal('(b)', '(b)') = true as result
UNION ALL
SELECT 'equal NULL set' as test, test_bms_equal('(b)', '(b 1 3 5)') = false as result
UNION ALL
SELECT 'equal set NULL' as test, test_bms_equal('(b 1 3 5)', '(b)') = false as result
UNION ALL
SELECT 'equal identical sets' as test, test_bms_equal('(b 1 3 5)', '(b 1 3 5)') = true as result
UNION ALL
SELECT 'equal different sets' as test, test_bms_equal('(b 1 3 5)', '(b 2 4 6)') = false as result
UNION ALL
SELECT 'compare NULL NULL' as test, test_bms_compare('(b)', '(b)') = 0 as result
UNION ALL
SELECT 'compare NULL set' as test, test_bms_compare('(b)', '(b 1 3)') = -1 as result
UNION ALL
SELECT 'compare set NULL' as test, test_bms_compare('(b 1 3)', '(b)') = 1 as result
UNION ALL
SELECT 'compare equal sets' as test, test_bms_compare('(b 1 3)', '(b 1 3)') = 0 as result
UNION ALL
SELECT 'compare subset superset' as test, test_bms_compare('(b 1 3)', '(b 1 3 5)') = -1 as result
UNION ALL
SELECT 'compare superset subset' as test, test_bms_compare('(b 1 3 5)', '(b 1 3)') = 1 as result;
SELECT 'compare edge case' as test,
       test_bms_compare(
           test_bms_add_range('(b)', 0, 63),
           test_bms_add_range('(b)', 0, 64)
       ) as result;

-- add_range basic
SELECT test_bms_add_range('(b)', 5, 7) = '(b 5 6 7)' as result;
-- add_range single element
SELECT test_bms_add_range('(b)', 5, 5) = '(b 5)' as result;
-- add_range to existing
SELECT test_bms_add_range('(b 1 10)', 5, 7) = '(b 1 5 6 7 10)' as result;
-- add_range at word boundary 31
SELECT test_bms_add_range('(b)', 30, 34) as result;
-- add_range at word boundary 63
SELECT test_bms_add_range('(b)', 62, 66) as result;
-- add_range large range
SELECT length(test_bms_add_range('(b)', 0, 1000)) = 3898 as result;
-- add_range force realloc test 1
SELECT length(test_bms_add_range('(b)', 0, 200)) = 697 as result;
-- add_range foce realloc test 2
SELECT length(test_bms_add_range('(b)', 1000, 1100)) = 508 as result;

-- membership empty
SELECT test_bms_membership('(b)') = 0 as result;
-- membership singleton
SELECT test_bms_membership('(b 42)') = 1 as result;
-- membership multiple
SELECT test_bms_membership('(b 1 2)') = 2 as result;

-- singleton_member valid
SELECT test_bms_singleton_member('(b 42)') = 42 as result;

-- set iteration
SELECT 'next_member first' as test, test_bms_next_member('(b 5 10 15 20)', -1) = 5 as result
UNION ALL
SELECT 'next_member second' as test, test_bms_next_member('(b 5 10 15 20)', 5) = 10 as result
UNION ALL
SELECT 'next_member past end' as test, test_bms_next_member('(b 5 10 15 20)', 20) = -2 as result
UNION ALL
SELECT 'next_member empty set' as test, test_bms_next_member('(b)', -1) = -2 as result
UNION ALL
SELECT 'prev_member last' as test, test_bms_prev_member('(b 5 10 15 20)', 21) = 20 as result
UNION ALL
SELECT 'prev_member penultimate' as test, test_bms_prev_member('(b 5 10 15 20)', 20) = 15 as result
UNION ALL
SELECT 'prev_member past beginning' as test, test_bms_prev_member('(b 5 10 15 20)', 5) = -2 as result
UNION ALL
SELECT 'prev_member empty set' as test, test_bms_prev_member('(b)', 100) = -2 as result;

-- hash functions
SELECT 'hash NULL' as test, test_bms_hash_value('(b)') = 0 as result
UNION ALL
SELECT 'hash consistency' as test, test_bms_hash_value('(b 1 3 5)') = test_bms_hash_value('(b 1 3 5)')
UNION ALL
SELECT 'hash different sets' as test, test_bms_hash_value('(b 1 3 5)') != test_bms_hash_value('(b 2 4 6)');

-- set overlap
SELECT 'overlap existing' as test, test_bms_overlap('(b 1 3 5)', '(b 3 5 7)') = true as result
UNION ALL
SELECT 'overlap none' as test, test_bms_overlap('(b 1 3 5)', '(b 2 4 6)') = false as result
UNION ALL
SELECT 'overlap with NULL' as test, test_bms_overlap('(b)', '(b 1 3 5)') = false as result;

-- subset relations
SELECT 'subset NULL is subset of all' as test, test_bms_is_subset('(b)', '(b 1 3 5)') = true as result
UNION ALL
SELECT 'subset proper subset' as test, test_bms_is_subset('(b 1 3)', '(b 1 3 5)') = true as result
UNION ALL
SELECT 'subset improper subset' as test, test_bms_is_subset('(b 1 3 5)', '(b 1 3)') = false as result
UNION ALL
SELECT 'subset disjoint sets' as test, test_bms_is_subset('(b 1 3)', '(b 2 4)') = false as result
UNION ALL
SELECT 'subset comparison edge' as test,
	test_bms_is_subset(
		test_bms_add_range(NULL, 0, 31),
		test_bms_add_range(NULL, 0, 63))
	= true as result;

-- copy operations
WITH test_set AS (SELECT '(b 1 3 5 7)' AS original)
SELECT 'copy NULL' as test, test_bms_copy(NULL) IS NULL as result
UNION ALL
SELECT 'copy equality' as test, test_bms_equal(original, test_bms_copy(original)) = true as result FROM test_set;

-- add members operation
SELECT test_bms_add_members('(b 1 3)', '(b 5 7)') = '(b 1 3 5 7)' as result;
-- add members complex
SELECT test_bms_add_members('(b 1 3 5)', '(b 100 200 300)') as result;

-- hash consistency
SELECT 'bitmap_hash NULL' as test,
       test_bitmap_hash('(b)') = 0 as result
UNION ALL
SELECT 'bitmap_hash consistency' as test,
       test_bitmap_hash('(b 1 3 5)') = test_bitmap_hash('(b 1 3 5)') as result
UNION ALL
SELECT 'bitmap_hash vs bms_hash_value' as test,
       test_bitmap_hash('(b 1 3 5)') = test_bms_hash_value('(b 1 3 5)') as result
UNION ALL
SELECT 'bitmap_hash different sets' as test,
       test_bitmap_hash('(b 1 3 5)') != test_bitmap_hash('(b 2 4 6)') as result;

-- match function
SELECT 'bitmap_match NULL NULL (should be 0)' as test,
       test_bitmap_match('(b)', '(b)') = 0 as result
UNION ALL
SELECT 'bitmap_match NULL set (should be 1)' as test,
       test_bitmap_match('(b)', '(b 1 3 5)') = 1 as result
UNION ALL
SELECT 'bitmap_match set NULL (should be 1)' as test,
       test_bitmap_match('(b 1 3 5)', '(b)') = 1 as result
UNION ALL
SELECT 'bitmap_match identical sets (should be 0)' as test,
       test_bitmap_match('(b 1 3 5)', '(b 1 3 5)') = 0 as result
UNION ALL
SELECT 'bitmap_match different sets (should be 1)' as test,
       test_bitmap_match('(b 1 3 5)', '(b 2 4 6)') = 1 as result
UNION ALL
SELECT 'bitmap_match subset/superset (should be 1)' as test,
       test_bitmap_match('(b 1 3)', '(b 1 3 5)') = 1 as result;

-- match relationship with bms_equal
SELECT 'bitmap_match vs bms_equal (equal sets)' as test,
       (test_bitmap_match('(b 1 3 5)', '(b 1 3 5)') = 0) = test_bms_equal('(b 1 3 5)', '(b 1 3 5)') as result
UNION ALL
SELECT 'bitmap_match vs bms_equal (different sets)' as test,
       (test_bitmap_match('(b 1 3 5)', '(b 2 4 6)') = 0) = test_bms_equal('(b 1 3 5)', '(b 2 4 6)') as result
UNION ALL
SELECT 'bitmap_match vs bms_equal (NULL cases)' as test,
       (test_bitmap_match('(b)', '(b)') = 0) = test_bms_equal('(b)', '(b)') as result;

-- bitmap_match [1,3,5] vs [1,3,5]
SELECT test_bitmap_match('(b 1 3 5)', '(b 1 3 5)') as match_value;
-- bitmap_match [1,3,5] vs [2,4,6]
SELECT test_bitmap_match('(b 1 3 5)', '(b 2 4 6)') as match_value;

-- bitmap_match empty arrays
SELECT test_bitmap_match('(b)', '(b)') = 0 as result;

-- overlap with lists
WITH test_lists AS (
    SELECT
        ARRAY[0] AS a0,
        ARRAY[1,2] AS a12,
        ARRAY[3,4,5] AS a345,
        ARRAY[6,7,8,9] AS a6789
)
SELECT 'overlap list 0' as test,
       test_bms_overlap_list('(b 0)', a0) as result
FROM test_lists
UNION ALL
SELECT 'overlap list 12' as test,
       test_bms_overlap_list('(b 2 3)', a12) as result
FROM test_lists
UNION ALL
SELECT 'overlap list 345' as test,
       test_bms_overlap_list('(b 3 4)', a345) as result
FROM test_lists
UNION ALL
SELECT 'overlap list 6789' as test,
       test_bms_overlap_list('(b 7 10)', a6789) as result
FROM test_lists
UNION ALL
SELECT 'overlap list 6789 no overlap' as test,
       test_bms_overlap_list('(b 1 5)', a6789) as result
FROM test_lists;

-- overlap empty list
SELECT test_bms_overlap_list('(b 1)', ARRAY[]::integer[]) as result;

-- random operations
SELECT test_random_operations(-1, 10000, 81920, 0) > 0 as result;

-- these should produce ERRORs
SELECT test_bms_make_singleton(-1);
SELECT test_bms_add_member('(b 1)', -1);
SELECT test_bms_is_member('(b)', -5);
SELECT test_bms_add_member('(b)', -10);
SELECT test_bms_del_member('(b)', -20);
SELECT test_bms_add_range('(b)', -5, 10);
SELECT test_bms_singleton_member('(b 1 2)');
SELECT test_bms_add_member('(b)', 1000);

DROP EXTENSION test_bitmapset;
