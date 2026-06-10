/*--------------------------------------------------------------------------
 *
 * test_dsm_registry.c
 *	  Test the dynamic shared memory registry.
 *
 * Copyright (c) 2024-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/test/modules/test_dsm_registry/test_dsm_registry.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"
#include "storage/dsm.h"
#include "storage/dsm_registry.h"
#include "storage/lwlock.h"
#include "utils/builtins.h"

PG_MODULE_MAGIC;

typedef struct TestDSMRegistryStruct
{
	int			val;
	int			detach_count;
	LWLock		lck;
} TestDSMRegistryStruct;

typedef struct TestDSMRegistryHashEntry
{
	char		key[64];
	dsa_pointer val;
} TestDSMRegistryHashEntry;

static TestDSMRegistryStruct *tdr_dsm;
static dsa_area *tdr_dsa;
static dshash_table *tdr_hash;

static const dshash_parameters dsh_params = {
	offsetof(TestDSMRegistryHashEntry, val),
	sizeof(TestDSMRegistryHashEntry),
	dshash_strcmp,
	dshash_strhash,
	dshash_strcpy
};

static void tdr_count_dsm_detach(dsm_segment *seg, Datum arg);

static void
init_tdr_dsm(void *ptr, void *arg)
{
	TestDSMRegistryStruct *dsm = (TestDSMRegistryStruct *) ptr;

	if ((int) (intptr_t) arg != 5432)
		elog(ERROR, "unexpected arg value %d", (int) (intptr_t) arg);

	LWLockInitialize(&dsm->lck, LWLockNewTrancheId("test_dsm_registry"));
	dsm->val = 0;
	dsm->detach_count = 0;
}

static void
tdr_attach_shmem(void)
{
	bool		found;

	tdr_dsm = GetNamedDSMSegment("test_dsm_registry_dsm",
								 sizeof(TestDSMRegistryStruct),
								 init_tdr_dsm,
								 &found, (void *) (intptr_t) 5432);

	if (tdr_dsa == NULL)
		tdr_dsa = GetNamedDSA("test_dsm_registry_dsa", &found);

	if (tdr_hash == NULL)
		tdr_hash = GetNamedDSHash("test_dsm_registry_hash", &dsh_params, &found);
}

PG_FUNCTION_INFO_V1(set_val_in_shmem);
Datum
set_val_in_shmem(PG_FUNCTION_ARGS)
{
	tdr_attach_shmem();

	LWLockAcquire(&tdr_dsm->lck, LW_EXCLUSIVE);
	tdr_dsm->val = PG_GETARG_INT32(0);
	LWLockRelease(&tdr_dsm->lck);

	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(get_val_in_shmem);
Datum
get_val_in_shmem(PG_FUNCTION_ARGS)
{
	int			ret;

	tdr_attach_shmem();

	LWLockAcquire(&tdr_dsm->lck, LW_SHARED);
	ret = tdr_dsm->val;
	LWLockRelease(&tdr_dsm->lck);

	PG_RETURN_INT32(ret);
}

PG_FUNCTION_INFO_V1(set_val_in_hash);
Datum
set_val_in_hash(PG_FUNCTION_ARGS)
{
	TestDSMRegistryHashEntry *entry;
	char	   *key = TextDatumGetCString(PG_GETARG_DATUM(0));
	char	   *val = TextDatumGetCString(PG_GETARG_DATUM(1));
	bool		found;

	if (strlen(key) >= offsetof(TestDSMRegistryHashEntry, val))
		ereport(ERROR,
				(errmsg("key too long")));

	tdr_attach_shmem();

	entry = dshash_find_or_insert(tdr_hash, key, &found);
	if (found)
		dsa_free(tdr_dsa, entry->val);

	entry->val = dsa_allocate(tdr_dsa, strlen(val) + 1);
	strcpy(dsa_get_address(tdr_dsa, entry->val), val);

	dshash_release_lock(tdr_hash, entry);

	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(get_val_in_hash);
Datum
get_val_in_hash(PG_FUNCTION_ARGS)
{
	TestDSMRegistryHashEntry *entry;
	char	   *key = TextDatumGetCString(PG_GETARG_DATUM(0));
	text	   *val = NULL;

	tdr_attach_shmem();

	entry = dshash_find(tdr_hash, key, false);
	if (entry == NULL)
		PG_RETURN_NULL();

	val = cstring_to_text(dsa_get_address(tdr_dsa, entry->val));

	dshash_release_lock(tdr_hash, entry);

	PG_RETURN_TEXT_P(val);
}

static void
tdr_count_dsm_detach(dsm_segment *seg, Datum arg)
{
	Assert(dsm_segment_handle(seg) == DatumGetUInt32(arg));
	Assert(tdr_dsm != NULL);

	LWLockAcquire(&tdr_dsm->lck, LW_EXCLUSIVE);
	tdr_dsm->detach_count++;
	LWLockRelease(&tdr_dsm->lck);
}

PG_FUNCTION_INFO_V1(reset_dsm_detach_count);
Datum
reset_dsm_detach_count(PG_FUNCTION_ARGS)
{
	tdr_attach_shmem();

	LWLockAcquire(&tdr_dsm->lck, LW_EXCLUSIVE);
	tdr_dsm->detach_count = 0;
	LWLockRelease(&tdr_dsm->lck);

	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(register_dsm_detach_for_backend_exit);
Datum
register_dsm_detach_for_backend_exit(PG_FUNCTION_ARGS)
{
	dsm_segment *seg;

	tdr_attach_shmem();

	/* Leave this mapping for backend-exit cleanup via dsm_detach_all(). */
	seg = dsm_create(1024, 0);
	dsm_pin_mapping(seg);
	on_dsm_detach(seg, tdr_count_dsm_detach,
				  UInt32GetDatum(dsm_segment_handle(seg)));

	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(get_dsm_detach_count);
Datum
get_dsm_detach_count(PG_FUNCTION_ARGS)
{
	int			ret;

	tdr_attach_shmem();

	LWLockAcquire(&tdr_dsm->lck, LW_SHARED);
	ret = tdr_dsm->detach_count;
	LWLockRelease(&tdr_dsm->lck);

	PG_RETURN_INT32(ret);
}
