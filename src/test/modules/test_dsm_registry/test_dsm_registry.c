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

#include <sys/stat.h>
#include <unistd.h>

#include "fmgr.h"
#include "miscadmin.h"
#include "storage/dsm.h"
#include "storage/dsm_registry.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "utils/builtins.h"
#include "utils/wait_event.h"

PG_MODULE_MAGIC;

#define TDR_EXIT_ORDER_FILE "global/test_dsm_registry_exit_order"
#define TDR_EXIT_ORDER_MAXLEN 256
#define TDR_TEMP_FILE_PATH_FILE "global/test_dsm_registry_temp_path"

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
static void tdr_record_exit_callback(int code, Datum arg);
static void tdr_record_dsm_detach_callback(dsm_segment *seg, Datum arg);
static void tdr_append_exit_order(const char *event);
static void tdr_write_text_file(const char *path, const char *contents);
static bool tdr_try_read_text_file(const char *path, char *buf, Size buflen,
								   bool missing_ok);
static void tdr_read_text_file(const char *path, char *buf, Size buflen);

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

static void
tdr_append_exit_order(const char *event)
{
	int			save_errno = errno;
	int			fd;
	const char *ptr;
	size_t		remaining;

	fd = BasicOpenFile(TDR_EXIT_ORDER_FILE,
					   O_WRONLY | O_CREAT | O_APPEND | PG_BINARY);
	if (fd < 0)
	{
		errno = save_errno;
		return;
	}

	/* Keep tracing from leaking errno changes into exit cleanup. */
	ptr = event;
	remaining = strlen(event);
	while (remaining > 0)
	{
		ssize_t		written = write(fd, ptr, remaining);

		if (written <= 0)
			goto done;
		ptr += written;
		remaining -= written;
	}
	{ ssize_t w = write(fd, "\n", 1); (void) w; }

done:
	(void) close(fd);
	errno = save_errno;
}

static void
tdr_write_text_file(const char *path, const char *contents)
{
	int			fd;
	size_t		len;

	fd = BasicOpenFile(path, O_WRONLY | O_CREAT | O_TRUNC | PG_BINARY);
	if (fd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m", path)));

	len = strlen(contents);
	if (write(fd, contents, len) != len)
	{
		int			save_errno = errno;

		close(fd);
		errno = save_errno;
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write file \"%s\": %m", path)));
	}

	if (close(fd) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not close file \"%s\": %m", path)));
}

static bool
tdr_try_read_text_file(const char *path, char *buf, Size buflen,
					   bool missing_ok)
{
	int			fd;
	ssize_t		nread;

	Assert(buflen > 0);
	buf[0] = '\0';

	fd = BasicOpenFile(path, O_RDONLY | PG_BINARY);
	if (fd < 0)
	{
		if (missing_ok && errno == ENOENT)
			return false;

		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m", path)));
	}

	nread = read(fd, buf, buflen - 1);
	if (nread < 0)
	{
		int			save_errno = errno;

		close(fd);
		errno = save_errno;
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read file \"%s\": %m", path)));
	}

	if (close(fd) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not close file \"%s\": %m", path)));

	while (nread > 0 && buf[nread - 1] == '\n')
		nread--;
	buf[nread] = '\0';

	return true;
}

static void
tdr_read_text_file(const char *path, char *buf, Size buflen)
{
	(void) tdr_try_read_text_file(path, buf, buflen, false);
}

static void
tdr_record_exit_callback(int code, Datum arg)
{
	(void) code;

	tdr_append_exit_order(DatumGetCString(arg));
}

static void
tdr_record_dsm_detach_callback(dsm_segment *seg, Datum arg)
{
	(void) seg;

	tdr_append_exit_order(DatumGetCString(arg));
}

PG_FUNCTION_INFO_V1(reset_exit_callback_order);
Datum
reset_exit_callback_order(PG_FUNCTION_ARGS)
{
	if (unlink(TDR_EXIT_ORDER_FILE) != 0 && errno != ENOENT)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not remove file \"%s\": %m",
						TDR_EXIT_ORDER_FILE)));

	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(register_exit_callback_order);
Datum
register_exit_callback_order(PG_FUNCTION_ARGS)
{
	dsm_segment *seg;

	/*
	 * Register each callback type twice.  The expected trace proves the
	 * category order and the LIFO order inside each callback stack.
	 */
	before_shmem_exit(tdr_record_exit_callback,
					  CStringGetDatum("before_shmem_1"));
	before_shmem_exit(tdr_record_exit_callback,
					  CStringGetDatum("before_shmem_2"));

	seg = dsm_create(1024, 0);
	dsm_pin_mapping(seg);
	on_dsm_detach(seg, tdr_record_dsm_detach_callback,
				  CStringGetDatum("dsm_detach"));

	on_shmem_exit(tdr_record_exit_callback,
				  CStringGetDatum("on_shmem_1"));
	on_shmem_exit(tdr_record_exit_callback,
				  CStringGetDatum("on_shmem_2"));

	on_proc_exit(tdr_record_exit_callback,
				 CStringGetDatum("on_proc_1"));
	on_proc_exit(tdr_record_exit_callback,
				 CStringGetDatum("on_proc_2"));

	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(get_exit_callback_order);
Datum
get_exit_callback_order(PG_FUNCTION_ARGS)
{
	char		buf[TDR_EXIT_ORDER_MAXLEN];
	int			i;

	/*
	 * pg_regress reconnects immediately after closing the previous backend.
	 * The old backend can still be finishing proc_exit callbacks, so wait
	 * briefly for the final marker before treating the trace as complete.
	 */
	for (i = 0; i < 500; i++)
	{
		if (tdr_try_read_text_file(TDR_EXIT_ORDER_FILE, buf, sizeof(buf),
								   true) &&
			strstr(buf, "on_proc_1") != NULL)
			PG_RETURN_TEXT_P(cstring_to_text(buf));

		CHECK_FOR_INTERRUPTS();
		pg_usleep(10000L);
	}

	tdr_read_text_file(TDR_EXIT_ORDER_FILE, buf, sizeof(buf));

	PG_RETURN_TEXT_P(cstring_to_text(buf));
}

PG_FUNCTION_INFO_V1(reset_backend_exit_temp_file_path);
Datum
reset_backend_exit_temp_file_path(PG_FUNCTION_ARGS)
{
	if (unlink(TDR_TEMP_FILE_PATH_FILE) != 0 && errno != ENOENT)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not remove file \"%s\": %m",
						TDR_TEMP_FILE_PATH_FILE)));

	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(create_temp_file_for_backend_exit);
Datum
create_temp_file_for_backend_exit(PG_FUNCTION_ARGS)
{
	File		file;
	const char *path;
	const char *payload = "backend exit temp file cleanup\n";

	/*
	 * Inter-transaction temp files are not cleaned at statement or
	 * transaction end.  Leaving this one open proves backend-exit cleanup
	 * closes the VFD and unlinks the underlying file.
	 */
	file = OpenTemporaryFile(true);
	if (FileWrite(file, payload, strlen(payload), 0,
				  WAIT_EVENT_BUFFILE_WRITE) != strlen(payload))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write temporary file \"%s\": %m",
						FilePathName(file))));

	path = FilePathName(file);
	tdr_write_text_file(TDR_TEMP_FILE_PATH_FILE, path);

	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(backend_exit_temp_file_removed);
Datum
backend_exit_temp_file_removed(PG_FUNCTION_ARGS)
{
	char		path[MAXPGPATH];
	struct stat statbuf;

	tdr_read_text_file(TDR_TEMP_FILE_PATH_FILE, path, sizeof(path));

	if (stat(path, &statbuf) == 0)
		PG_RETURN_BOOL(false);
	if (errno != ENOENT)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not stat file \"%s\": %m", path)));

	PG_RETURN_BOOL(true);
}
