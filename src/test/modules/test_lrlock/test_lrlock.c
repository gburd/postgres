/*--------------------------------------------------------------------------
 *
 * test_lrlock.c
 *		Test code for the left-right lock primitive.
 *
 * This extension provides SQL-callable functions to exercise and
 * validate the LRLock implementation.  The protected data structure
 * is a simple integer counter, which allows straightforward
 * verification of the apply/sync/publish semantics.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/test/modules/test_lrlock/test_lrlock.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "storage/lrlock.h"
#include "storage/shmem.h"

PG_MODULE_MAGIC;

/*
 * The protected data structure: a simple counter with a few fields.
 */
typedef struct TestLRData
{
	int64		counter;
	int64		secondary;
}			TestLRData;

/*
 * Operation types for the operation log.
 */
typedef enum TestLROpType
{
	TEST_LR_OP_INCREMENT,
	TEST_LR_OP_DECREMENT,
	TEST_LR_OP_SET,
	TEST_LR_OP_ADD,
}			TestLROpType;

typedef struct TestLROp
{
	TestLROpType type;
	int64		value;
}			TestLROp;

/* The shared LRLock instance */
static LRLock * test_lr_lock = NULL;

/* Hooks */
static shmem_request_hook_type prev_shmem_request_hook = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

/*
 * Apply callback: apply a single operation to one copy.
 */
static void
test_lr_apply(void *data, const void *operation, Size op_size)
{
	TestLRData *d = (TestLRData *) data;
	const		TestLROp *op = (const TestLROp *) operation;

	Assert(op_size == sizeof(TestLROp));

	switch (op->type)
	{
		case TEST_LR_OP_INCREMENT:
			d->counter++;
			break;
		case TEST_LR_OP_DECREMENT:
			d->counter--;
			break;
		case TEST_LR_OP_SET:
			d->counter = op->value;
			break;
		case TEST_LR_OP_ADD:
			d->counter += op->value;
			break;
	}
}

/*
 * Sync callback: copy one data structure to another.
 */
static void
test_lr_sync(void *dst, const void *src, Size data_size)
{
	Assert(data_size == sizeof(TestLRData));
	memcpy(dst, src, data_size);
}

/*
 * Shared memory request hook: request space for the LRLock.
 */
static void
test_lrlock_shmem_request(void)
{
	if (prev_shmem_request_hook)
		prev_shmem_request_hook();

	RequestAddinShmemSpace(LRLockShmemSize(sizeof(TestLRData),
										   MaxBackends,
										   4096));
}

/*
 * Shared memory startup hook: create the LRLock.
 */
static void
test_lrlock_shmem_startup(void)
{
	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	test_lr_lock = LRLockCreate(sizeof(TestLRData),
								test_lr_apply,
								test_lr_sync,
								"test_lrlock");
}

/*
 * Module initialization.
 */
void
_PG_init(void)
{
	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = test_lrlock_shmem_request;

	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = test_lrlock_shmem_startup;
}

/* ----------------------------------------------------------------
 *		SQL-callable test functions
 * ----------------------------------------------------------------
 */

/*
 * test_lrlock_read() -- read the counter value via wait-free read path.
 */
PG_FUNCTION_INFO_V1(test_lrlock_read);
Datum
test_lrlock_read(PG_FUNCTION_ARGS)
{
	const		TestLRData *data;
	int64		val;

	if (test_lr_lock == NULL)
		ereport(ERROR,
				(errmsg("test_lrlock: shared memory not initialized"),
				 errhint("Add test_lrlock to shared_preload_libraries.")));

	data = (const TestLRData *) LRLockReadBegin(test_lr_lock);
	val = data->counter;
	LRLockReadEnd(test_lr_lock);

	PG_RETURN_INT64(val);
}

/*
 * test_lrlock_write_increment(n) -- increment the counter n times and publish.
 */
PG_FUNCTION_INFO_V1(test_lrlock_write_increment);
Datum
test_lrlock_write_increment(PG_FUNCTION_ARGS)
{
	int64		n = PG_GETARG_INT64(0);
	int64		i;
	TestLROp	op;

	if (test_lr_lock == NULL)
		ereport(ERROR,
				(errmsg("test_lrlock: shared memory not initialized"),
				 errhint("Add test_lrlock to shared_preload_libraries.")));

	(void) LRLockWriteBegin(test_lr_lock);

	op.type = TEST_LR_OP_INCREMENT;
	op.value = 0;
	for (i = 0; i < n; i++)
		LRLockApplyOp(test_lr_lock, &op, sizeof(op));

	LRLockPublish(test_lr_lock);
	LRLockWriteEnd(test_lr_lock);

	PG_RETURN_VOID();
}

/*
 * test_lrlock_write_set(value) -- set the counter to a specific value.
 */
PG_FUNCTION_INFO_V1(test_lrlock_write_set);
Datum
test_lrlock_write_set(PG_FUNCTION_ARGS)
{
	int64		value = PG_GETARG_INT64(0);
	TestLROp	op;

	if (test_lr_lock == NULL)
		ereport(ERROR,
				(errmsg("test_lrlock: shared memory not initialized"),
				 errhint("Add test_lrlock to shared_preload_libraries.")));

	(void) LRLockWriteBegin(test_lr_lock);

	op.type = TEST_LR_OP_SET;
	op.value = value;
	LRLockApplyOp(test_lr_lock, &op, sizeof(op));

	LRLockPublish(test_lr_lock);
	LRLockWriteEnd(test_lr_lock);

	PG_RETURN_VOID();
}

/*
 * test_lrlock_write_add(value) -- add a value to the counter.
 */
PG_FUNCTION_INFO_V1(test_lrlock_write_add);
Datum
test_lrlock_write_add(PG_FUNCTION_ARGS)
{
	int64		value = PG_GETARG_INT64(0);
	TestLROp	op;

	if (test_lr_lock == NULL)
		ereport(ERROR,
				(errmsg("test_lrlock: shared memory not initialized"),
				 errhint("Add test_lrlock to shared_preload_libraries.")));

	(void) LRLockWriteBegin(test_lr_lock);

	op.type = TEST_LR_OP_ADD;
	op.value = value;
	LRLockApplyOp(test_lr_lock, &op, sizeof(op));

	LRLockPublish(test_lr_lock);
	LRLockWriteEnd(test_lr_lock);

	PG_RETURN_VOID();
}

/*
 * test_lrlock_write_no_publish(n) -- increment n times WITHOUT publishing.
 * Used to test that unpublished writes are not visible to readers.
 */
PG_FUNCTION_INFO_V1(test_lrlock_write_no_publish);
Datum
test_lrlock_write_no_publish(PG_FUNCTION_ARGS)
{
	int64		n = PG_GETARG_INT64(0);
	int64		i;
	TestLROp	op;

	if (test_lr_lock == NULL)
		ereport(ERROR,
				(errmsg("test_lrlock: shared memory not initialized"),
				 errhint("Add test_lrlock to shared_preload_libraries.")));

	(void) LRLockWriteBegin(test_lr_lock);

	op.type = TEST_LR_OP_INCREMENT;
	op.value = 0;
	for (i = 0; i < n; i++)
		LRLockApplyOp(test_lr_lock, &op, sizeof(op));

	/* Note: intentionally not calling LRLockPublish */
	LRLockWriteEnd(test_lr_lock);

	PG_RETURN_VOID();
}

/*
 * test_lrlock_publish() -- publish pending operations.
 */
PG_FUNCTION_INFO_V1(test_lrlock_publish);
Datum
test_lrlock_publish(PG_FUNCTION_ARGS)
{
	if (test_lr_lock == NULL)
		ereport(ERROR,
				(errmsg("test_lrlock: shared memory not initialized"),
				 errhint("Add test_lrlock to shared_preload_libraries.")));

	(void) LRLockWriteBegin(test_lr_lock);
	LRLockPublish(test_lr_lock);
	LRLockWriteEnd(test_lr_lock);

	PG_RETURN_VOID();
}

/*
 * test_lrlock_stress(nops) -- rapidly alternate reads and writes.
 * Returns the final counter value.
 */
PG_FUNCTION_INFO_V1(test_lrlock_stress);
Datum
test_lrlock_stress(PG_FUNCTION_ARGS)
{
	int			nops = PG_GETARG_INT32(0);
	int			i;
	int64		read_val = 0;

	if (test_lr_lock == NULL)
		ereport(ERROR,
				(errmsg("test_lrlock: shared memory not initialized"),
				 errhint("Add test_lrlock to shared_preload_libraries.")));

	for (i = 0; i < nops; i++)
	{
		/* Write: increment by 1 */
		{
			TestLROp	op;

			(void) LRLockWriteBegin(test_lr_lock);
			op.type = TEST_LR_OP_INCREMENT;
			op.value = 0;
			LRLockApplyOp(test_lr_lock, &op, sizeof(op));
			LRLockPublish(test_lr_lock);
			LRLockWriteEnd(test_lr_lock);
		}

		/* Read: verify consistency */
		{
			const		TestLRData *data;

			data = (const TestLRData *) LRLockReadBegin(test_lr_lock);
			read_val = data->counter;
			LRLockReadEnd(test_lr_lock);
		}
	}

	PG_RETURN_INT64(read_val);
}
