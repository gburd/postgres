/*
 * test_bitmapset.c
 *      Test module for bitmapset data structure
 *
 * This module tests the bitmapset implementation in PostgreSQL,
 * covering all public API functions, edge cases, and memory usage.
 *
 * src/test/modules/test_bitmapset/test_bitmapset.c
 */

#include "postgres.h"

#include "fmgr.h"
#include "nodes/bitmapset.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(test_bitmapset);

/* Test assertion macros following test_bitmapset pattern */
#define EXPECT_TRUE(expr) \
    do { \
        if (!(expr)) \
            elog(ERROR, \
                 "%s was unexpectedly false in file \"%s\" line %u", \
                 #expr, __FILE__, __LINE__); \
    } while (0)

#define EXPECT_FALSE(expr) \
    do { \
        if (expr) \
            elog(ERROR, \
                 "%s was unexpectedly true in file \"%s\" line %u", \
                 #expr, __FILE__, __LINE__); \
    } while (0)

#define EXPECT_EQ_INT(result_expr, expected_expr) \
    do { \
        int _result = (result_expr); \
        int _expected = (expected_expr); \
        if (_result != _expected) \
            elog(ERROR, \
                 "%s yielded %d, expected %d (%s) in file \"%s\" line %u", \
                 #result_expr, _result, _expected, #expected_expr, __FILE__, __LINE__); \
    } while (0)

#define EXPECT_NULL(expr) \
    do { \
        if ((expr) != NULL) \
            elog(ERROR, \
                 "%s was unexpectedly non-NULL in file \"%s\" line %u", \
                 #expr, __FILE__, __LINE__); \
    } while (0)

#define EXPECT_NOT_NULL(expr) \
    do { \
        if ((expr) == NULL) \
            elog(ERROR, \
                 "%s was unexpectedly NULL in file \"%s\" line %u", \
                 #expr, __FILE__, __LINE__); \
    } while (0)

/* Test empty bitmapset operations */
static void
test_empty_bitmapset(void)
{
	Bitmapset  *result,
			   *bms = NULL;

	elog(NOTICE, "testing empty bitmapset operations");

	/* Test operations on NULL bitmapset */
	EXPECT_TRUE(bms_is_empty(bms));
	EXPECT_EQ_INT(bms_num_members(bms), 0);
	EXPECT_FALSE(bms_is_member(0, bms));
	EXPECT_FALSE(bms_is_member(1, bms));
	EXPECT_FALSE(bms_is_member(100, bms));
	EXPECT_EQ_INT(bms_next_member(bms, -1), -2);
	EXPECT_EQ_INT(bms_prev_member(bms, -1), -2);

	/* Test copy of empty set */
	result = bms_copy(bms);
	EXPECT_NULL(result);

	/* Test union with empty set */
	result = bms_union(bms, NULL);
	EXPECT_NULL(result);

	/* Test intersection with empty set */
	result = bms_intersect(bms, NULL);
	EXPECT_NULL(result);

	/* Test difference with empty set */
	result = bms_difference(bms, NULL);
	EXPECT_NULL(result);

	/* Test equal comparison */
	EXPECT_TRUE(bms_equal(bms, NULL));
	EXPECT_TRUE(bms_equal(NULL, bms));

	/* Test subset operations */
	EXPECT_TRUE(bms_is_subset(bms, NULL));
	EXPECT_TRUE(bms_is_subset(NULL, bms));

	/* Test overlap */
	EXPECT_FALSE(bms_overlap(bms, NULL));
	EXPECT_FALSE(bms_overlap(NULL, bms));
}

/* Test single element bitmapset operations */
static void
test_single_element(void)
{
	Bitmapset  *result,
			   *bms = NULL;
	int			test_member = 42;

	elog(NOTICE, "testing single element bitmapset operations");

	/* Add single element */
	bms = bms_add_member(bms, test_member);
	EXPECT_NOT_NULL(bms);
	EXPECT_FALSE(bms_is_empty(bms));
	EXPECT_EQ_INT(bms_num_members(bms), 1);
	EXPECT_TRUE(bms_is_member(test_member, bms));
	EXPECT_FALSE(bms_is_member(test_member - 1, bms));
	EXPECT_FALSE(bms_is_member(test_member + 1, bms));

	/* Test iteration */
	EXPECT_EQ_INT(bms_next_member(bms, -1), test_member);
	EXPECT_EQ_INT(bms_next_member(bms, test_member), -2);
	EXPECT_EQ_INT(bms_prev_member(bms, test_member + 1), test_member);
	EXPECT_EQ_INT(bms_prev_member(bms, test_member), -2);

	/* Test copy */
	result = bms_copy(bms);
	EXPECT_NOT_NULL(result);
	EXPECT_TRUE(bms_equal(bms, result));
	EXPECT_TRUE(bms_is_member(test_member, result));

	/* Test remove member */
	result = bms_del_member(result, test_member);
	EXPECT_NULL(result);

	/* Test remove non-existent member */
	result = bms_copy(bms);
	EXPECT_NOT_NULL(result);
	result = bms_del_member(result, test_member + 1);
	EXPECT_NOT_NULL(result);
	EXPECT_TRUE(bms_equal(bms, result));

	bms_free(bms);
	bms_free(result);
}

/* Test multiple elements and set operations */
static void
test_multiple_elements(void)
{
	Bitmapset  *bms1 = NULL;
	Bitmapset  *bms2 = NULL;
	Bitmapset  *result = NULL;
	int			elements1[] = {1, 5, 10, 15, 20, 100};
	int			elements2[] = {3, 5, 12, 15, 25, 200};
	int			num_elements1 = sizeof(elements1) / sizeof(elements1[0]);
	int			num_elements2 = sizeof(elements2) / sizeof(elements2[0]);

	elog(NOTICE, "testing multiple elements and set operations");

	/* Build first set */
	for (int i = 0; i < num_elements1; i++)
		bms1 = bms_add_member(bms1, elements1[i]);

	EXPECT_EQ_INT(bms_num_members(bms1), num_elements1);

	/* Build second set */
	for (int i = 0; i < num_elements2; i++)
		bms2 = bms_add_member(bms2, elements2[i]);

	EXPECT_EQ_INT(bms_num_members(bms2), num_elements2);

	/* Test membership */
	for (int i = 0; i < num_elements1; i++)
		EXPECT_TRUE(bms_is_member(elements1[i], bms1));

	for (int i = 0; i < num_elements2; i++)
		EXPECT_TRUE(bms_is_member(elements2[i], bms2));

	/* Test union */
	result = bms_union(bms1, bms2);
	EXPECT_NOT_NULL(result);
	for (int i = 0; i < num_elements1; i++)
		EXPECT_TRUE(bms_is_member(elements1[i], result));
	for (int i = 0; i < num_elements2; i++)
		EXPECT_TRUE(bms_is_member(elements2[i], result));
	EXPECT_EQ_INT(bms_num_members(result), 10); /* 1,3,5,10,12,15,20,25,100,200 */
	bms_free(result);

	/* Test intersection */
	result = bms_intersect(bms1, bms2);
	EXPECT_NOT_NULL(result);
	EXPECT_TRUE(bms_is_member(5, result));
	EXPECT_TRUE(bms_is_member(15, result));
	EXPECT_EQ_INT(bms_num_members(result), 2);	/* 5, 15 */
	bms_free(result);

	/* Test difference */
	result = bms_difference(bms1, bms2);
	EXPECT_NOT_NULL(result);
	EXPECT_TRUE(bms_is_member(1, result));
	EXPECT_TRUE(bms_is_member(10, result));
	EXPECT_TRUE(bms_is_member(20, result));
	EXPECT_TRUE(bms_is_member(100, result));
	EXPECT_FALSE(bms_is_member(5, result));
	EXPECT_FALSE(bms_is_member(15, result));
	EXPECT_EQ_INT(bms_num_members(result), 4);	/* 1, 10, 20, 100 */
	bms_free(result);

	/* Test overlap */
	EXPECT_TRUE(bms_overlap(bms1, bms2));

	/* Test subset operations */
	result = NULL;
	result = bms_add_member(result, 5);
	result = bms_add_member(result, 15);
	EXPECT_TRUE(bms_is_subset(result, bms1));
	EXPECT_TRUE(bms_is_subset(result, bms2));
	EXPECT_FALSE(bms_is_subset(bms1, result));
	bms_free(result);

	bms_free(bms1);
	bms_free(bms2);
}

/* Test edge cases and boundary conditions */
static void
test_edge_cases(void)
{
	int			large_element = 10000;
	int			count;
	int			member;
	Bitmapset  *result;
	Bitmapset  *bms = NULL;

	elog(NOTICE, "testing edge cases and boundary conditions");

	/* Test element 0 */
	bms = bms_add_member(bms, 0);
	EXPECT_TRUE(bms_is_member(0, bms));
	EXPECT_EQ_INT(bms_num_members(bms), 1);
	EXPECT_EQ_INT(bms_next_member(bms, -1), 0);
	bms_free(bms);
	bms = NULL;

	/* Test large element numbers */
	bms = bms_add_member(bms, large_element);
	EXPECT_TRUE(bms_is_member(large_element, bms));
	EXPECT_EQ_INT(bms_num_members(bms), 1);
	bms_free(bms);
	bms = NULL;

	/* Test adding same element multiple times */
	bms = bms_add_member(bms, 42);
	bms = bms_add_member(bms, 42);
	EXPECT_EQ_INT(bms_num_members(bms), 1);
	EXPECT_TRUE(bms_is_member(42, bms));
	bms_free(bms);
	bms = NULL;

	/* Test removing from single-element set */
	bms = bms_add_member(bms, 99);
	result = bms_del_member(bms, 99);
	EXPECT_NULL(result);
	bms_free(bms);
	bms = NULL;

	/* Test dense range */
	for (int i = 0; i < 64; i++)
		bms = bms_add_member(bms, i);
	EXPECT_EQ_INT(bms_num_members(bms), 64);

	/* Test iteration over dense range */
	count = 0;
	member = -1;
	while ((member = bms_next_member(bms, member)) >= 0)
	{
		EXPECT_EQ_INT(member, count);
		count++;
	}
	EXPECT_EQ_INT(count, 64);

	bms_free(bms);
}

/* Test iterator functions thoroughly */
static void
test_iteration(void)
{
	Bitmapset  *bms = NULL;
	int			member;
	int			index;
	int			elements[] = {2, 7, 15, 31, 63, 127, 255, 511, 1023};
	int			num_elements = sizeof(elements) / sizeof(elements[0]);

	elog(NOTICE, "testing iteration functions");

	/* Build test set */
	for (int i = 0; i < num_elements; i++)
		bms = bms_add_member(bms, elements[i]);

	/* Test forward iteration */
	member = -1;
	index = 0;
	while ((member = bms_next_member(bms, member)) >= 0)
	{
		EXPECT_EQ_INT(member, elements[index]);
		index++;
	}
	EXPECT_EQ_INT(index, num_elements);

	/* Test backward iteration */
	member = bms->nwords * BITS_PER_BITMAPWORD;
	index = num_elements - 1;
	while ((member = bms_prev_member(bms, member)) >= 0)
	{
		EXPECT_EQ_INT(member, elements[index]);
		index--;
	}
	EXPECT_EQ_INT(index, -1);

	/* Test iteration bounds */
	EXPECT_EQ_INT(bms_next_member(bms, 1023), -2);
	EXPECT_EQ_INT(bms_prev_member(bms, 2), -2);

	bms_free(bms);
}

/* Test set operations with various combinations */
static void
test_set_operations(void)
{
	Bitmapset  *empty = NULL;
	Bitmapset  *single = NULL;
	Bitmapset  *multi = NULL;
	Bitmapset  *result = NULL;
	Bitmapset  *single_copy = NULL;

	elog(NOTICE, "testing comprehensive set operations");

	single = bms_add_member(single, 10);
	multi = bms_add_member(multi, 5);
	multi = bms_add_member(multi, 10);
	multi = bms_add_member(multi, 15);

	/* Union operations */
	result = bms_union(empty, single);
	EXPECT_TRUE(bms_equal(result, single));
	bms_free(result);

	result = bms_union(single, empty);
	EXPECT_TRUE(bms_equal(result, single));
	bms_free(result);

	result = bms_union(single, multi);
	EXPECT_EQ_INT(bms_num_members(result), 3);
	EXPECT_TRUE(bms_is_member(5, result));
	EXPECT_TRUE(bms_is_member(10, result));
	EXPECT_TRUE(bms_is_member(15, result));
	bms_free(result);

	/* Intersection operations */
	result = bms_intersect(empty, single);
	EXPECT_NULL(result);

	result = bms_intersect(single, multi);
	EXPECT_EQ_INT(bms_num_members(result), 1);
	EXPECT_TRUE(bms_is_member(10, result));
	bms_free(result);

	/* Difference operations */
	result = bms_difference(single, multi);
	EXPECT_NULL(result);

	result = bms_difference(multi, single);
	EXPECT_EQ_INT(bms_num_members(result), 2);
	EXPECT_TRUE(bms_is_member(5, result));
	EXPECT_TRUE(bms_is_member(15, result));
	EXPECT_FALSE(bms_is_member(10, result));
	bms_free(result);

	/* Equality tests */
	EXPECT_FALSE(bms_equal(single, multi));
	EXPECT_TRUE(bms_equal(empty, NULL));

	single_copy = bms_copy(single);
	EXPECT_TRUE(bms_equal(single, single_copy));
	bms_free(single_copy);

	bms_free(single);
	bms_free(multi);
}

/* Main test function */
Datum
test_bitmapset(PG_FUNCTION_ARGS)
{
	elog(NOTICE, "starting bitmapset tests");

	test_empty_bitmapset();
	test_single_element();
	test_multiple_elements();
	test_edge_cases();
	test_iteration();
	test_set_operations();

	elog(NOTICE, "all bitmapset tests passed");

	PG_RETURN_VOID();
}
