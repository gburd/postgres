/*-------------------------------------------------------------------------
 *
 * test_skiplist.c
 *	  Test module for src/include/lib/skiplist.h
 *
 * Exercises core skip-list operations: init, insert, search, delete,
 * navigation, ordering, position variants, stress, and validation.
 * Uses single-threaded mode (no atomics) since PostgreSQL backends
 * are single-threaded.
 *
 * Copyright (c) 2024-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/modules/test_skiplist/test_skiplist.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "fmgr.h"
#include "funcapi.h"

PG_MODULE_MAGIC;

/* ---------- assertion helpers ---------- */
#define EXPECT_TRUE(expr) \
	do { \
		if (!(expr)) \
			elog(ERROR, "%s:%d: expected true: %s", __FILE__, __LINE__, #expr); \
	} while (0)

#define EXPECT_FALSE(expr) \
	do { \
		if (expr) \
			elog(ERROR, "%s:%d: expected false: %s", __FILE__, __LINE__, #expr); \
	} while (0)

#define EXPECT_EQ_INT(a, b) \
	do { \
		int _a = (a); \
		int _b = (b); \
		if (_a != _b) \
			elog(ERROR, "%s:%d: expected %d == %d (%s == %s)", \
				 __FILE__, __LINE__, _a, _b, #a, #b); \
	} while (0)

#define EXPECT_EQ_SZ(a, b) \
	do { \
		size_t _a = (a); \
		size_t _b = (b); \
		if (_a != _b) \
			elog(ERROR, "%s:%d: expected %zu == %zu (%s == %s)", \
				 __FILE__, __LINE__, _a, _b, #a, #b); \
	} while (0)

#define EXPECT_NOT_NULL(ptr) \
	do { \
		if ((ptr) == NULL) \
			elog(ERROR, "%s:%d: expected non-NULL: %s", __FILE__, __LINE__, #ptr); \
	} while (0)

#define EXPECT_NULL(ptr) \
	do { \
		if ((ptr) != NULL) \
			elog(ERROR, "%s:%d: expected NULL: %s", __FILE__, __LINE__, #ptr); \
	} while (0)

/* ---------- skip-list instantiation ---------- */

/*
 * Use single-threaded mode: replaces all C11 atomics with plain loads/stores.
 * This is the mode PostgreSQL backends will use.
 */
#define SKIPLIST_SINGLE_THREADED
#include "lib/skiplist.h"

/* Test node structure */
struct test_node
{
	int			key;
	char	   *value;
	SKIPLIST_ENTRY(test) entries;
};

/*
 * Helper: create a palloc'd value string for a key.
 */
static char *
make_value(int key)
{
	char	   *buf = palloc(32);

	snprintf(buf, 32, "val_%d", key);
	return buf;
}

/* Generate the core skiplist for our test node type */
SKIPLIST_DECL(
	test, sl_, entries,
	/* compare */
	{
		(void) list;
		(void) aux;
		if (a->key < b->key)
			return -1;
		if (a->key > b->key)
			return 1;
		return 0;
	},
	/* free entry */
	{
		if (node->value)
		{
			pfree(node->value);
			node->value = NULL;
		}
	},
	/* update entry */
	{
		char	   *new_value = (char *) value;

		if (node->value)
			pfree(node->value);
		node->value = new_value;
	},
	/* archive entry */
	{
		dest->key = src->key;
		if (src->value)
		{
			dest->value = palloc(strlen(src->value) + 1);
			strcpy(dest->value, src->value);
		}
		else
			dest->value = NULL;
	},
	/* sizeof entry */
	{
		bytes = sizeof(struct test_node);
		if (node->value)
			bytes += strlen(node->value) + 1;
	})

/* Generate access convenience functions */
SKIPLIST_DECL_ACCESS(
	test, sl_, key, int, value, char *,
	/* query block */ { query.key = key; },
	/* return block */ { return node->value; })

/* Generate validation functions */
SKIPLIST_DECL_VALIDATE(test, sl_, entries)

/* ---------- test functions ---------- */

static void
test_init(void)
{
	test_t	   *list;
	int			rc;

	elog(NOTICE, "testing init and empty list operations");

	list = palloc0(sizeof(test_t));
	rc = sl_skip_init_test(list);
	EXPECT_EQ_INT(rc, 0);
	EXPECT_EQ_SZ(sl_skip_length_test(list), 0);
	EXPECT_TRUE(sl_skip_is_empty_test(list));
	EXPECT_NULL(sl_skip_head_test(list));
	EXPECT_NULL(sl_skip_tail_test(list));

	sl_skip_free_test(list);
	pfree(list);
}

static void
test_insert_and_search(void)
{
	test_t	   *list;
	test_node_t *node,
			   *found;
	int			rc;

	elog(NOTICE, "testing insert and search");

	list = palloc0(sizeof(test_t));
	sl_skip_init_test(list);

	/* Insert a single node */
	rc = sl_skip_alloc_node_test(&node);
	EXPECT_EQ_INT(rc, 0);
	node->key = 42;
	node->value = make_value(42);
	rc = sl_skip_insert_test(list, node);
	EXPECT_EQ_INT(rc, 0);
	EXPECT_EQ_SZ(sl_skip_length_test(list), 1);
	EXPECT_FALSE(sl_skip_is_empty_test(list));

	/* Search for existing key */
	{
		test_node_t query;

		query.key = 42;
		found = sl_skip_position_eq_test(list, &query);
		EXPECT_NOT_NULL(found);
		EXPECT_EQ_INT(found->key, 42);
	}

	/* Search for non-existent key */
	{
		test_node_t query;

		query.key = 99;
		found = sl_skip_position_eq_test(list, &query);
		EXPECT_NULL(found);
	}

	sl_skip_free_test(list);
	pfree(list);
}

static void
test_insert_ordering(void)
{
	test_t	   *list;
	test_node_t *current;
	int			keys[] = {5, 2, 8, 1, 9, 3, 7, 4, 6};
	int			n_keys = sizeof(keys) / sizeof(keys[0]);
	int			prev_key = 0;
	int			count = 0;

	elog(NOTICE, "testing insertion ordering");

	list = palloc0(sizeof(test_t));
	sl_skip_init_test(list);

	for (int i = 0; i < n_keys; i++)
	{
		test_node_t *node;

		sl_skip_alloc_node_test(&node);
		node->key = keys[i];
		node->value = make_value(keys[i]);
		sl_skip_insert_test(list, node);
	}

	EXPECT_EQ_SZ(sl_skip_length_test(list), (size_t) n_keys);

	/* Verify forward traversal is sorted */
	current = sl_skip_head_test(list);
	while (current)
	{
		EXPECT_TRUE(current->key > prev_key);
		prev_key = current->key;
		count++;
		current = sl_skip_next_node_test(list, current);
	}
	EXPECT_EQ_INT(count, n_keys);

	sl_skip_free_test(list);
	pfree(list);
}

static void
test_duplicate_insert(void)
{
	test_t	   *list;
	test_node_t *node1,
			   *node2;
	int			rc;

	elog(NOTICE, "testing duplicate insert rejection");

	list = palloc0(sizeof(test_t));
	sl_skip_init_test(list);

	sl_skip_alloc_node_test(&node1);
	node1->key = 10;
	node1->value = make_value(10);
	sl_skip_insert_test(list, node1);

	/* Duplicate should be rejected */
	sl_skip_alloc_node_test(&node2);
	node2->key = 10;
	node2->value = make_value(10);
	rc = sl_skip_insert_test(list, node2);
	EXPECT_TRUE(rc != 0);		/* returns non-zero for duplicate */
	EXPECT_EQ_SZ(sl_skip_length_test(list), 1);

	/* Duplicate with dup flag should succeed */
	rc = sl_skip_insert_dup_test(list, node2);
	EXPECT_EQ_INT(rc, 0);
	EXPECT_EQ_SZ(sl_skip_length_test(list), 2);

	sl_skip_free_test(list);
	pfree(list);
}

static void
test_remove(void)
{
	test_t	   *list;
	test_node_t query;
	test_node_t *found;
	int			rc;

	elog(NOTICE, "testing remove");

	list = palloc0(sizeof(test_t));
	sl_skip_init_test(list);

	for (int i = 1; i <= 5; i++)
	{
		test_node_t *node;

		sl_skip_alloc_node_test(&node);
		node->key = i;
		node->value = make_value(i);
		sl_skip_insert_test(list, node);
	}
	EXPECT_EQ_SZ(sl_skip_length_test(list), 5);

	/* Remove middle element */
	query.key = 3;
	rc = sl_skip_remove_node_test(list, &query);
	EXPECT_EQ_INT(rc, 0);
	EXPECT_EQ_SZ(sl_skip_length_test(list), 4);

	/* Verify it's gone */
	found = sl_skip_position_eq_test(list, &query);
	EXPECT_NULL(found);

	/* Remove non-existent */
	query.key = 99;
	rc = sl_skip_remove_node_test(list, &query);
	EXPECT_TRUE(rc != 0);
	EXPECT_EQ_SZ(sl_skip_length_test(list), 4);

	sl_skip_free_test(list);
	pfree(list);
}

static void
test_access_api(void)
{
	test_t	   *list;
	char	   *retrieved;
	int			rc;

	elog(NOTICE, "testing access API (put/get/contains/del)");

	list = palloc0(sizeof(test_t));
	sl_skip_init_test(list);

	/* put */
	rc = sl_skip_put_test(list, 100, make_value(100));
	EXPECT_EQ_INT(rc, 0);

	/* get */
	retrieved = sl_skip_get_test(list, 100);
	EXPECT_NOT_NULL(retrieved);
	EXPECT_TRUE(strcmp(retrieved, "val_100") == 0);

	/* contains */
	EXPECT_TRUE(sl_skip_contains_test(list, 100));
	EXPECT_FALSE(sl_skip_contains_test(list, 200));

	/* del */
	rc = sl_skip_del_test(list, 100);
	EXPECT_EQ_INT(rc, 0);
	EXPECT_FALSE(sl_skip_contains_test(list, 100));

	sl_skip_free_test(list);
	pfree(list);
}

static void
test_navigation(void)
{
	test_t	   *list;
	test_node_t *head,
			   *tail,
			   *current;

	elog(NOTICE, "testing navigation (head/tail/next/prev)");

	list = palloc0(sizeof(test_t));
	sl_skip_init_test(list);

	for (int i = 1; i <= 5; i++)
	{
		test_node_t *node;

		sl_skip_alloc_node_test(&node);
		node->key = i * 10;
		node->value = make_value(i * 10);
		sl_skip_insert_test(list, node);
	}

	/* Forward */
	head = sl_skip_head_test(list);
	EXPECT_NOT_NULL(head);
	EXPECT_EQ_INT(head->key, 10);

	current = sl_skip_next_node_test(list, head);
	EXPECT_NOT_NULL(current);
	EXPECT_EQ_INT(current->key, 20);

	/* Backward */
	tail = sl_skip_tail_test(list);
	EXPECT_NOT_NULL(tail);
	EXPECT_EQ_INT(tail->key, 50);

	current = sl_skip_prev_node_test(list, tail);
	EXPECT_NOT_NULL(current);
	EXPECT_EQ_INT(current->key, 40);

	/* Boundaries */
	EXPECT_NULL(sl_skip_prev_node_test(list, head));
	EXPECT_NULL(sl_skip_next_node_test(list, tail));

	sl_skip_free_test(list);
	pfree(list);
}

static void
test_position_variants(void)
{
	test_t	   *list;
	test_node_t *found;

	elog(NOTICE, "testing position variants (gte/gt/lte/lt)");

	list = palloc0(sizeof(test_t));
	sl_skip_init_test(list);

	/* Insert 10, 20, 30, 40, 50 */
	for (int i = 1; i <= 5; i++)
	{
		test_node_t *node;

		sl_skip_alloc_node_test(&node);
		node->key = i * 10;
		node->value = make_value(i * 10);
		sl_skip_insert_test(list, node);
	}

	/* GTE: find >= 25 should return 30 */
	found = sl_skip_pos_test(list, SKIP_GTE, 25);
	EXPECT_NOT_NULL(found);
	EXPECT_EQ_INT(found->key, 30);

	/* GTE: find >= 30 should return 30 */
	found = sl_skip_pos_test(list, SKIP_GTE, 30);
	EXPECT_NOT_NULL(found);
	EXPECT_EQ_INT(found->key, 30);

	/* GT: find > 30 should return 40 */
	found = sl_skip_pos_test(list, SKIP_GT, 30);
	EXPECT_NOT_NULL(found);
	EXPECT_EQ_INT(found->key, 40);

	/* LTE: find <= 25 should return 20 */
	found = sl_skip_pos_test(list, SKIP_LTE, 25);
	EXPECT_NOT_NULL(found);
	EXPECT_EQ_INT(found->key, 20);

	/* LTE: find <= 30 should return 30 */
	found = sl_skip_pos_test(list, SKIP_LTE, 30);
	EXPECT_NOT_NULL(found);
	EXPECT_EQ_INT(found->key, 30);

	/* LT: find < 30 should return 20 */
	found = sl_skip_pos_test(list, SKIP_LT, 30);
	EXPECT_NOT_NULL(found);
	EXPECT_EQ_INT(found->key, 20);

	sl_skip_free_test(list);
	pfree(list);
}

static void
test_edge_cases(void)
{
	test_t	   *list;
	int			rc;

	elog(NOTICE, "testing edge cases");

	list = palloc0(sizeof(test_t));
	sl_skip_init_test(list);

	/* Operations on empty list */
	EXPECT_NULL(sl_skip_get_test(list, 1));
	EXPECT_FALSE(sl_skip_contains_test(list, 1));
	rc = sl_skip_del_test(list, 1);
	EXPECT_TRUE(rc != 0);

	/* Insert and delete single element, then reuse */
	rc = sl_skip_put_test(list, 42, make_value(42));
	EXPECT_EQ_INT(rc, 0);
	EXPECT_EQ_SZ(sl_skip_length_test(list), 1);

	rc = sl_skip_del_test(list, 42);
	EXPECT_EQ_INT(rc, 0);
	EXPECT_EQ_SZ(sl_skip_length_test(list), 0);
	EXPECT_NULL(sl_skip_head_test(list));
	EXPECT_NULL(sl_skip_tail_test(list));

	/* Should be able to insert again */
	rc = sl_skip_put_test(list, 99, make_value(99));
	EXPECT_EQ_INT(rc, 0);
	EXPECT_TRUE(sl_skip_contains_test(list, 99));

	sl_skip_free_test(list);
	pfree(list);
}

static void
test_tail_regression(void)
{
	test_t	   *list;
	test_node_t *node1,
			   *node2,
			   *tail,
			   *head;

	elog(NOTICE, "testing tail regression (0, 1, 2 elements)");

	list = palloc0(sizeof(test_t));
	sl_skip_init_test(list);

	/* Empty: tail NULL */
	EXPECT_NULL(sl_skip_tail_test(list));

	/* Single element */
	sl_skip_alloc_node_test(&node1);
	node1->key = 10;
	node1->value = make_value(10);
	sl_skip_insert_test(list, node1);
	tail = sl_skip_tail_test(list);
	EXPECT_NOT_NULL(tail);
	EXPECT_EQ_INT(tail->key, 10);

	/* Two elements */
	sl_skip_alloc_node_test(&node2);
	node2->key = 20;
	node2->value = make_value(20);
	sl_skip_insert_test(list, node2);
	tail = sl_skip_tail_test(list);
	EXPECT_NOT_NULL(tail);
	EXPECT_EQ_INT(tail->key, 20);

	head = sl_skip_head_test(list);
	EXPECT_NOT_NULL(head);
	EXPECT_EQ_INT(head->key, 10);

	sl_skip_free_test(list);
	pfree(list);
}

static void
test_stress(void)
{
	test_t	   *list;
	test_node_t *current;
	int			n = 1000;
	int			prev_key;
	int			count;

	elog(NOTICE, "testing stress (insert 1000, remove odds, verify evens)");

	list = palloc0(sizeof(test_t));
	sl_skip_init_test(list);

	/* Insert n elements */
	for (int i = 0; i < n; i++)
		sl_skip_put_test(list, i, make_value(i));

	EXPECT_EQ_SZ(sl_skip_length_test(list), (size_t) n);

	/* Remove odd elements */
	for (int i = 1; i < n; i += 2)
		sl_skip_del_test(list, i);

	EXPECT_EQ_SZ(sl_skip_length_test(list), (size_t) n / 2);

	/* Verify even elements remain, odds gone */
	for (int i = 0; i < n; i += 2)
		EXPECT_TRUE(sl_skip_contains_test(list, i));
	for (int i = 1; i < n; i += 2)
		EXPECT_FALSE(sl_skip_contains_test(list, i));

	/* Verify ordering */
	current = sl_skip_head_test(list);
	prev_key = -1;
	count = 0;
	while (current)
	{
		EXPECT_TRUE(current->key > prev_key);
		EXPECT_TRUE(current->key % 2 == 0);
		prev_key = current->key;
		count++;
		current = sl_skip_next_node_test(list, current);
	}
	EXPECT_EQ_INT(count, n / 2);

	sl_skip_free_test(list);
	pfree(list);
}

static void
test_validation(void)
{
	test_t	   *list;
	int			errors;

	elog(NOTICE, "testing integrity validation");

	list = palloc0(sizeof(test_t));
	sl_skip_init_test(list);

	/* Validate empty list (single-threaded mode, flags=1) */
	errors = _skip_integrity_check_test(list, 1);
	EXPECT_EQ_INT(errors, 0);

	/* Insert elements */
	for (int i = 1; i <= 20; i++)
	{
		test_node_t *node;

		sl_skip_alloc_node_test(&node);
		node->key = i;
		node->value = make_value(i);
		sl_skip_insert_test(list, node);
	}
	EXPECT_EQ_SZ(sl_skip_length_test(list), 20);

	/* Validate populated list */
	errors = _skip_integrity_check_test(list, 1);
	EXPECT_EQ_INT(errors, 0);

	/* Remove some and validate again */
	for (int i = 1; i <= 10; i++)
		sl_skip_del_test(list, i);

	EXPECT_EQ_SZ(sl_skip_length_test(list), 10);
	errors = _skip_integrity_check_test(list, 1);
	EXPECT_EQ_INT(errors, 0);

	sl_skip_free_test(list);
	pfree(list);
}

static void
test_head_height(void)
{
	test_t	   *list;
	size_t		initial_height,
				grown_height;

	elog(NOTICE, "testing head height growth and shrinkage");

	list = palloc0(sizeof(test_t));
	sl_skip_init_test(list);

	initial_height = list->slh_head->entries.sle_height;
	EXPECT_EQ_SZ(initial_height, 1);

	/* Insert many to grow height */
	for (int i = 0; i < 1000; i++)
	{
		test_node_t *node;

		sl_skip_alloc_node_test(&node);
		node->key = i;
		node->value = make_value(i);
		sl_skip_insert_test(list, node);
	}

	grown_height = list->slh_head->entries.sle_height;
	EXPECT_TRUE(grown_height > initial_height);
	EXPECT_TRUE(grown_height <= SKIPLIST_MAX_HEIGHT);

	/* Head and tail heights should match */
	EXPECT_EQ_SZ(grown_height, list->slh_tail->entries.sle_height);

	/* Delete all */
	for (int i = 0; i < 1000; i++)
		sl_skip_del_test(list, i);

	EXPECT_EQ_SZ(sl_skip_length_test(list), 0);
	EXPECT_TRUE(sl_skip_is_empty_test(list));

	/* Re-insert to prove list works after full drain */
	{
		test_node_t *node;

		sl_skip_alloc_node_test(&node);
		node->key = 42;
		node->value = make_value(42);
		sl_skip_insert_test(list, node);
	}
	EXPECT_EQ_SZ(sl_skip_length_test(list), 1);
	EXPECT_TRUE(sl_skip_contains_test(list, 42));

	sl_skip_free_test(list);
	pfree(list);
}

static void
test_update(void)
{
	test_t	   *list;
	test_node_t query;
	char	   *retrieved;
	int			rc;

	elog(NOTICE, "testing update");

	list = palloc0(sizeof(test_t));
	sl_skip_init_test(list);

	/* Insert initial value */
	sl_skip_put_test(list, 10, make_value(10));

	/* Update value */
	query.key = 10;
	rc = sl_skip_update_test(list, &query, make_value(999));
	EXPECT_EQ_INT(rc, 0);

	/* Verify updated value */
	retrieved = sl_skip_get_test(list, 10);
	EXPECT_NOT_NULL(retrieved);
	EXPECT_TRUE(strcmp(retrieved, "val_999") == 0);

	sl_skip_free_test(list);
	pfree(list);
}

static void
test_foreach(void)
{
	test_t	   *list;
	test_node_t *elm;
	size_t		iter;
	int			prev_key;

	elog(NOTICE, "testing foreach iteration macros");

	list = palloc0(sizeof(test_t));
	sl_skip_init_test(list);

	for (int i = 1; i <= 10; i++)
	{
		test_node_t *node;

		sl_skip_alloc_node_test(&node);
		node->key = i;
		node->value = make_value(i);
		sl_skip_insert_test(list, node);
	}

	/* Head-to-tail */
	prev_key = 0;
	SKIPLIST_FOREACH_H2T(test, sl_, entries, list, elm, iter)
	{
		EXPECT_TRUE(elm->key > prev_key);
		prev_key = elm->key;
	}
	EXPECT_EQ_SZ(iter, 10);

	/* Tail-to-head */
	prev_key = 11;
	SKIPLIST_FOREACH_T2H(test, sl_, entries, list, elm, iter)
	{
		EXPECT_TRUE(elm->key < prev_key);
		prev_key = elm->key;
	}

	sl_skip_free_test(list);
	pfree(list);
}

/* ---------- entry point ---------- */

PG_FUNCTION_INFO_V1(test_skiplist);

Datum
test_skiplist(PG_FUNCTION_ARGS)
{
	test_init();
	test_insert_and_search();
	test_insert_ordering();
	test_duplicate_insert();
	test_remove();
	test_access_api();
	test_navigation();
	test_position_variants();
	test_edge_cases();
	test_tail_regression();
	test_stress();
	test_validation();
	test_head_height();
	test_update();
	test_foreach();

	PG_RETURN_VOID();
}
