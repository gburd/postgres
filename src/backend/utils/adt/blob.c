/*-------------------------------------------------------------------------
 *
 * blob.c
 *	  External BLOB/CLOB types with filesystem storage
 *
 * This module implements the blob and clob data types, which store
 * a 40-byte inline reference (ExternalBlobRef) in the heap tuple and
 * actual content on the filesystem using content-addressable storage
 * with SHA-256 hashing.  Updates use binary diffs (deltas) to avoid
 * rewriting the full content.
 *
 * All file writes use the transactional FILEOPS API so that files
 * created within a transaction are automatically deleted if the
 * transaction aborts, and files scheduled for deletion are removed
 * only at commit time.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/blob.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <sys/stat.h>
#include <unistd.h>

#include "access/xact.h"
#include "catalog/pg_type.h"
#include "common/cryptohash.h"
#include "common/sha2.h"
#include "funcapi.h"
#include "lib/stringinfo.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "port/pg_crc32c.h"
#include "storage/fd.h"
#include "storage/fileops.h"
#include "utils/blob.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"
#include "varatt.h"

/* GUC parameters */
int			blob_delta_threshold = EXTBLOB_DEFAULT_DELTA_THRESHOLD;
int			blob_compaction_threshold = EXTBLOB_DEFAULT_COMPACTION_THRESHOLD;
int			blob_worker_naptime = EXTBLOB_DEFAULT_WORKER_NAPTIME;
bool		enable_blob_compression = true;
char	   *blob_directory = NULL;	/* Default set below */

/* PG_FUNCTION_INFO_V1 declarations for all SQL-callable functions */
PG_FUNCTION_INFO_V1(blob_in);
PG_FUNCTION_INFO_V1(blob_out);
PG_FUNCTION_INFO_V1(blob_recv);
PG_FUNCTION_INFO_V1(blob_send);
PG_FUNCTION_INFO_V1(clob_in);
PG_FUNCTION_INFO_V1(clob_out);
PG_FUNCTION_INFO_V1(clob_recv);
PG_FUNCTION_INFO_V1(clob_send);
PG_FUNCTION_INFO_V1(blob_from_bytea);
PG_FUNCTION_INFO_V1(bytea_from_blob);
PG_FUNCTION_INFO_V1(clob_from_text);
PG_FUNCTION_INFO_V1(text_from_clob);
PG_FUNCTION_INFO_V1(blob_eq);
PG_FUNCTION_INFO_V1(blob_ne);
PG_FUNCTION_INFO_V1(blob_lt);
PG_FUNCTION_INFO_V1(blob_le);
PG_FUNCTION_INFO_V1(blob_gt);
PG_FUNCTION_INFO_V1(blob_ge);
PG_FUNCTION_INFO_V1(blob_cmp);
PG_FUNCTION_INFO_V1(clob_eq);
PG_FUNCTION_INFO_V1(clob_ne);
PG_FUNCTION_INFO_V1(clob_lt);
PG_FUNCTION_INFO_V1(clob_le);
PG_FUNCTION_INFO_V1(clob_gt);
PG_FUNCTION_INFO_V1(clob_ge);
PG_FUNCTION_INFO_V1(clob_cmp);

/* Forward declarations */
static void write_blob_file(const char *path, const void *data, Size size,
							const ExternalBlobFileHeader *header);
static void *read_blob_file(const char *path, Size *size_out,
							ExternalBlobFileHeader *header_out);
static bool blob_file_exists(const char *path);
static const char *get_blob_directory(void);
static void hash_to_hex(const uint8 *hash, int nbytes, char *hex_out);

/* ----------------------------------------------------------------
 * Helper: return the effective blob storage directory
 * ----------------------------------------------------------------
 */
static const char *
get_blob_directory(void)
{
	return (blob_directory && blob_directory[0] != '\0')
		? blob_directory
		: EXTBLOB_DIRECTORY;
}

/* ----------------------------------------------------------------
 * Hash / path utilities
 * ----------------------------------------------------------------
 */

/*
 * hash_to_hex - Convert nbytes of binary hash to lowercase hex.
 * hex_out must hold at least nbytes*2 + 1 bytes.
 */
static void
hash_to_hex(const uint8 *hash, int nbytes, char *hex_out)
{
	static const char hexdigits[] = "0123456789abcdef";
	int			i;

	for (i = 0; i < nbytes; i++)
	{
		hex_out[i * 2] = hexdigits[(hash[i] >> 4) & 0x0F];
		hex_out[i * 2 + 1] = hexdigits[hash[i] & 0x0F];
	}
	hex_out[nbytes * 2] = '\0';
}

/*
 * ExternalBlobComputeHash - SHA-256 content hash
 */
void
ExternalBlobComputeHash(const void *data, Size size, uint8 *hash_out)
{
	pg_cryptohash_ctx *ctx;

	ctx = pg_cryptohash_create(PG_SHA256);
	if (ctx == NULL)
		elog(ERROR, "out of memory creating SHA-256 context");
	if (pg_cryptohash_init(ctx) < 0)
		elog(ERROR, "could not initialize SHA-256 context: %s",
			 pg_cryptohash_error(ctx));
	if (pg_cryptohash_update(ctx, (const uint8 *) data, size) < 0)
		elog(ERROR, "could not update SHA-256 hash: %s",
			 pg_cryptohash_error(ctx));
	if (pg_cryptohash_final(ctx, hash_out, PG_SHA256_DIGEST_LENGTH) < 0)
		elog(ERROR, "could not finalize SHA-256 hash: %s",
			 pg_cryptohash_error(ctx));
	pg_cryptohash_free(ctx);
}

/*
 * ExternalBlobHashToHex - Full hash to hex string
 */
void
ExternalBlobHashToHex(const uint8 *hash, char *hex_out)
{
	hash_to_hex(hash, EXTERNAL_BLOB_HASH_LEN, hex_out);
}

/*
 * ExternalBlobGetDirPath - Subdirectory for a given hash
 *
 * Returns path like "pg_external_blobs/a3" (using first byte as prefix).
 */
void
ExternalBlobGetDirPath(const uint8 *hash, char *path_out, Size path_len)
{
	snprintf(path_out, path_len, "%s/%02x",
			 get_blob_directory(), hash[0]);
}

/*
 * ExternalBlobGetBasePath - Full path to .base file
 */
void
ExternalBlobGetBasePath(const uint8 *hash, char *path_out, Size path_len)
{
	char		suffix_hex[63]; /* 31 bytes * 2 + 1 */

	hash_to_hex(hash + EXTBLOB_DIR_PREFIX_BYTES,
				EXTERNAL_BLOB_HASH_LEN - EXTBLOB_DIR_PREFIX_BYTES,
				suffix_hex);

	snprintf(path_out, path_len, "%s/%02x/%s%s",
			 get_blob_directory(), hash[0], suffix_hex, EXTBLOB_BASE_SUFFIX);
}

/*
 * ExternalBlobGetDeltaPath - Full path to .delta.N file
 */
void
ExternalBlobGetDeltaPath(const uint8 *hash, uint16 version,
						 char *path_out, Size path_len)
{
	char		suffix_hex[63];

	Assert(version >= 1);

	hash_to_hex(hash + EXTBLOB_DIR_PREFIX_BYTES,
				EXTERNAL_BLOB_HASH_LEN - EXTBLOB_DIR_PREFIX_BYTES,
				suffix_hex);

	snprintf(path_out, path_len, "%s/%02x/%s%s.%u",
			 get_blob_directory(), hash[0], suffix_hex,
			 EXTBLOB_DELTA_SUFFIX, (unsigned int) version);
}

/*
 * ExternalBlobEnsureDirectory - Create storage directory tree
 *
 * Creates the base directory and 256 hash-prefix subdirectories.
 * Uses MakePGDirectory which is safe for crash recovery.
 */
void
ExternalBlobEnsureDirectory(void)
{
	const char *blob_dir = get_blob_directory();
	char		path[MAXPGPATH];
	int			i;

	/* Create base directory */
	if (MakePGDirectory(blob_dir) < 0 && errno != EEXIST)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create directory \"%s\": %m", blob_dir)));

	/* Create 256 hash-prefix subdirectories (00..ff) */
	for (i = 0; i < 256; i++)
	{
		snprintf(path, sizeof(path), "%s/%02x", blob_dir, i);
		if (MakePGDirectory(path) < 0 && errno != EEXIST)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not create directory \"%s\": %m", path)));
	}
}

/* ----------------------------------------------------------------
 * File I/O helpers
 * ----------------------------------------------------------------
 */

/*
 * write_blob_file - Write header + data to a blob file atomically.
 *
 * Uses PathNameOpenFilePerm for creation, then registers delete-on-abort
 * via FILEOPS to ensure transactional cleanup.
 */
static void
write_blob_file(const char *path, const void *data, Size size,
				const ExternalBlobFileHeader *header)
{
	File		fd;
	ssize_t		written;
	pgoff_t		offset = 0;

	fd = PathNameOpenFilePerm(path,
							 O_RDWR | O_CREAT | O_EXCL | PG_BINARY,
							 0600);
	if (fd < 0)
	{
		if (errno == EEXIST)
			return;				/* Dedup race: another backend wrote it */
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create external blob file \"%s\": %m",
						path)));
	}

	/* Write header */
	written = FileWrite(fd, header, sizeof(*header), offset,
						WAIT_EVENT_DATA_FILE_WRITE);
	if (written != (ssize_t) sizeof(*header))
	{
		FileClose(fd);
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write header to \"%s\": %m", path)));
	}
	offset += written;

	/* Write data */
	if (size > 0)
	{
		written = FileWrite(fd, data, size, offset,
							WAIT_EVENT_DATA_FILE_WRITE);
		if (written != (ssize_t) size)
		{
			FileClose(fd);
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not write data to \"%s\": %m", path)));
		}
	}

	FileClose(fd);

	/*
	 * Register delete-on-abort via FILEOPS so the file is cleaned up if the
	 * transaction aborts.
	 */
	if (IsTransactionState())
		FileOpsDelete(path, false);	/* delete on abort */
}

/*
 * read_blob_file - Read a blob file, returning header and data.
 *
 * Returns palloc'd data buffer, or NULL if the file does not exist.
 */
static void *
read_blob_file(const char *path, Size *size_out,
			   ExternalBlobFileHeader *header_out)
{
	File		fd;
	struct stat st;
	void	   *data;
	ssize_t		nread;
	pgoff_t		offset = 0;
	Size		data_size;

	fd = PathNameOpenFile(path, O_RDONLY | PG_BINARY);
	if (fd < 0)
		return NULL;

	/* Get file size via stat */
	if (stat(path, &st) < 0)
	{
		FileClose(fd);
		return NULL;
	}

	/* Validate minimum size */
	if (st.st_size < (off_t) sizeof(ExternalBlobFileHeader))
	{
		FileClose(fd);
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("external blob file \"%s\" is too small (%lld bytes)",
						path, (long long) st.st_size)));
	}

	/* Read header */
	nread = FileRead(fd, header_out, sizeof(*header_out), offset,
					 WAIT_EVENT_DATA_FILE_READ);
	if (nread != (ssize_t) sizeof(*header_out))
	{
		FileClose(fd);
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("could not read header from \"%s\": %m", path)));
	}
	offset += nread;

	/* Verify magic number */
	if (header_out->magic != EXTBLOB_MAGIC &&
		header_out->magic != EXTBLOB_DELTA_MAGIC)
	{
		FileClose(fd);
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("invalid magic 0x%08x in external blob file \"%s\"",
						header_out->magic, path)));
	}

	/* Read data */
	data_size = st.st_size - sizeof(ExternalBlobFileHeader);
	if (data_size == 0)
	{
		FileClose(fd);
		*size_out = 0;
		return palloc(1);		/* Return valid pointer for zero-length data */
	}

	data = palloc(data_size);
	nread = FileRead(fd, data, data_size, offset,
					 WAIT_EVENT_DATA_FILE_READ);
	if (nread != (ssize_t) data_size)
	{
		FileClose(fd);
		pfree(data);
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("short read from \"%s\": expected %zu, got %zd",
						path, data_size, nread)));
	}

	/* Verify checksum */
	{
		pg_crc32c	actual_crc;

		actual_crc = ExternalBlobComputeChecksum((const uint8 *) data,
												 data_size);
		if (!EQ_CRC32C(actual_crc, header_out->checksum))
		{
			FileClose(fd);
			pfree(data);
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("checksum mismatch in \"%s\": expected %08x, got %08x",
							path, header_out->checksum, actual_crc)));
		}
	}

	FileClose(fd);
	*size_out = data_size;
	return data;
}

/*
 * blob_file_exists - Check if a file exists on disk
 */
static bool
blob_file_exists(const char *path)
{
	struct stat st;

	return (stat(path, &st) == 0 && S_ISREG(st.st_mode));
}

/* ----------------------------------------------------------------
 * Core BLOB operations
 * ----------------------------------------------------------------
 */

/*
 * ExternalBlobCreate - Create a new external blob
 *
 * Computes SHA-256 hash, checks for deduplication, writes file if new.
 * Returns a palloc'd ExternalBlobRef.
 */
ExternalBlobRef *
ExternalBlobCreate(const void *data, Size size, bool is_clob,
				   UndoRecPtr undo_ptr)
{
	ExternalBlobRef *ref;
	uint8		hash[EXTERNAL_BLOB_HASH_LEN];
	char		path[MAXPGPATH];
	ExternalBlobFileHeader header;

	ref = (ExternalBlobRef *) palloc0(sizeof(ExternalBlobRef));

	/* Compute content hash */
	ExternalBlobComputeHash(data, size, hash);
	memcpy(ref->hash, hash, EXTERNAL_BLOB_HASH_LEN);

	ref->size = size;
	ref->version = 0;
	ref->flags = is_clob ? EXTBLOB_FLAG_CLOB : 0;

	/* Check for deduplication */
	ExternalBlobGetBasePath(hash, path, sizeof(path));
	if (blob_file_exists(path))
		return ref;

	/* Ensure directory structure exists */
	ExternalBlobEnsureDirectory();

	/* Build file header */
	memset(&header, 0, sizeof(header));
	header.undo_ptr = undo_ptr;
	header.magic = EXTBLOB_MAGIC;
	header.data_size = size;
	header.checksum = ExternalBlobComputeChecksum((const uint8 *) data, size);
	header.flags = ref->flags;
	header.format_version = EXTBLOB_FORMAT_VERSION;

	write_blob_file(path, data, size, &header);

	return ref;
}

/*
 * ExternalBlobRead - Read the full content of an external BLOB
 *
 * Reads base file and applies any delta chain to reconstruct
 * the current version.  Returns palloc'd data.
 */
void *
ExternalBlobRead(const ExternalBlobRef *ref, Size *size_out)
{
	char		path[MAXPGPATH];
	void	   *data;
	Size		size;
	ExternalBlobFileHeader header;
	uint16		v;

	/* Read base file */
	ExternalBlobGetBasePath(ref->hash, path, sizeof(path));
	data = read_blob_file(path, &size, &header);

	if (data == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("external blob base file not found: \"%s\"", path)));

	/* Apply delta chain */
	for (v = 1; v <= ref->version; v++)
	{
		void	   *delta_data;
		Size		delta_size;
		void	   *new_data;
		Size		new_size;

		ExternalBlobGetDeltaPath(ref->hash, v, path, sizeof(path));
		delta_data = read_blob_file(path, &delta_size, &header);

		if (delta_data == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_EXCEPTION),
					 errmsg("external blob delta file not found: \"%s\"",
							path)));

		new_data = ExternalBlobApplyDelta(data, size,
										  delta_data, delta_size,
										  &new_size);
		pfree(data);
		pfree(delta_data);

		data = new_data;
		size = new_size;
	}

	*size_out = size;
	return data;
}

/*
 * ExternalBlobUpdate - Update a BLOB with new content
 *
 * Reads the old version, computes a binary diff, and writes a delta
 * file if the delta is smaller than the full content.  Otherwise
 * writes a new base file.
 */
ExternalBlobRef *
ExternalBlobUpdate(const ExternalBlobRef *old_ref, const void *new_data,
				   Size new_size, UndoRecPtr undo_ptr)
{
	ExternalBlobRef *new_ref;
	void	   *old_data;
	Size		old_size;
	StringInfoData delta;
	char		path[MAXPGPATH];
	ExternalBlobFileHeader header;

	/* Read current version for delta computation */
	old_data = ExternalBlobRead(old_ref, &old_size);

	/*
	 * If the size difference is small or the old data is below threshold,
	 * skip delta and create a full new version.
	 */
	if (old_size < (Size) blob_delta_threshold ||
		new_size < (Size) blob_delta_threshold)
	{
		pfree(old_data);
		return ExternalBlobCreate(new_data, new_size,
								 (old_ref->flags & EXTBLOB_FLAG_CLOB) != 0,
								 undo_ptr);
	}

	/* Compute delta */
	initStringInfo(&delta);
	ExternalBlobComputeDelta(old_data, old_size,
							new_data, new_size,
							&delta);

	/*
	 * If the delta is larger than the new data, just create a new base
	 * version instead.
	 */
	if ((Size) delta.len >= new_size)
	{
		pfree(old_data);
		pfree(delta.data);
		return ExternalBlobCreate(new_data, new_size,
								 (old_ref->flags & EXTBLOB_FLAG_CLOB) != 0,
								 undo_ptr);
	}

	/* Build new ref with incremented version */
	new_ref = (ExternalBlobRef *) palloc(sizeof(ExternalBlobRef));
	memcpy(new_ref, old_ref, sizeof(ExternalBlobRef));
	new_ref->version++;
	new_ref->size = new_size;

	/* Write delta file */
	ExternalBlobGetDeltaPath(new_ref->hash, new_ref->version,
							 path, sizeof(path));

	memset(&header, 0, sizeof(header));
	header.undo_ptr = undo_ptr;
	header.magic = EXTBLOB_DELTA_MAGIC;
	header.data_size = delta.len;
	header.checksum = ExternalBlobComputeChecksum((const uint8 *) delta.data,
												  delta.len);
	header.flags = new_ref->flags;
	header.format_version = EXTBLOB_FORMAT_VERSION;

	write_blob_file(path, delta.data, delta.len, &header);

	pfree(old_data);
	pfree(delta.data);

	return new_ref;
}

/*
 * ExternalBlobDelete - Mark a BLOB for garbage collection
 *
 * Writes a tombstone file containing the UNDO pointer so the background
 * worker can determine visibility, and schedules the base file for
 * deletion at transaction commit.
 */
void
ExternalBlobDelete(const ExternalBlobRef *ref, UndoRecPtr undo_ptr)
{
	char		tombstone_path[MAXPGPATH];
	char		base_path[MAXPGPATH];
	char		suffix_hex[63];
	File		fd;
	ssize_t		written;

	hash_to_hex(ref->hash + EXTBLOB_DIR_PREFIX_BYTES,
				EXTERNAL_BLOB_HASH_LEN - EXTBLOB_DIR_PREFIX_BYTES,
				suffix_hex);

	snprintf(tombstone_path, sizeof(tombstone_path), "%s/%02x/%s%s",
			 get_blob_directory(), ref->hash[0],
			 suffix_hex, EXTBLOB_TOMBSTONE_SUFFIX);

	/* Write tombstone with UNDO pointer */
	fd = PathNameOpenFilePerm(tombstone_path,
							 O_RDWR | O_CREAT | O_TRUNC | PG_BINARY,
							 0600);
	if (fd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create tombstone file \"%s\": %m",
						tombstone_path)));

	written = FileWrite(fd, &undo_ptr, sizeof(UndoRecPtr), 0,
						WAIT_EVENT_DATA_FILE_WRITE);
	if (written != (ssize_t) sizeof(UndoRecPtr))
	{
		FileClose(fd);
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write tombstone file \"%s\": %m",
						tombstone_path)));
	}
	FileClose(fd);

	/* Schedule base file for deletion at commit */
	ExternalBlobGetBasePath(ref->hash, base_path, sizeof(base_path));
	if (IsTransactionState())
		FileOpsDelete(base_path, true);
}

/*
 * ExternalBlobExists - Check whether the base file for a ref exists
 */
bool
ExternalBlobExists(const ExternalBlobRef *ref)
{
	char		path[MAXPGPATH];

	ExternalBlobGetBasePath(ref->hash, path, sizeof(path));
	return blob_file_exists(path);
}

/* ----------------------------------------------------------------
 * Type I/O functions
 * ----------------------------------------------------------------
 */

/*
 * blob_in - Parse bytea-format input and create an external BLOB.
 */
Datum
blob_in(PG_FUNCTION_ARGS)
{
	char	   *input_str = PG_GETARG_CSTRING(0);
	ExternalBlobRef *ref;
	bytea	   *data;
	UndoRecPtr	undo_ptr;

	undo_ptr = GetCurrentTransactionUndoRecPtr();

	/* Parse as bytea hex/escape format */
	data = DatumGetByteaP(DirectFunctionCall1(byteain,
											  CStringGetDatum(input_str)));

	ref = ExternalBlobCreate(VARDATA_ANY(data), VARSIZE_ANY_EXHDR(data),
							 false, undo_ptr);

	pfree(data);
	PG_RETURN_POINTER(ref);
}

/*
 * blob_out - Output BLOB data in bytea hex format.
 */
Datum
blob_out(PG_FUNCTION_ARGS)
{
	ExternalBlobRef *ref = (ExternalBlobRef *) PG_GETARG_POINTER(0);
	void	   *data;
	Size		size;
	bytea	   *bval;
	char	   *result;

	data = ExternalBlobRead(ref, &size);

	bval = (bytea *) palloc(size + VARHDRSZ);
	SET_VARSIZE(bval, size + VARHDRSZ);
	memcpy(VARDATA(bval), data, size);
	pfree(data);

	result = DatumGetCString(DirectFunctionCall1(byteaout,
												 PointerGetDatum(bval)));
	pfree(bval);

	PG_RETURN_CSTRING(result);
}

/*
 * blob_recv - Binary receive for BLOB.
 */
Datum
blob_recv(PG_FUNCTION_ARGS)
{
	StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);
	ExternalBlobRef *ref;
	int			nbytes;
	const char *data;
	UndoRecPtr	undo_ptr;

	undo_ptr = GetCurrentTransactionUndoRecPtr();

	nbytes = buf->len - buf->cursor;
	data = pq_getmsgbytes(buf, nbytes);

	ref = ExternalBlobCreate(data, nbytes, false, undo_ptr);

	PG_RETURN_POINTER(ref);
}

/*
 * blob_send - Binary send for BLOB.
 */
Datum
blob_send(PG_FUNCTION_ARGS)
{
	ExternalBlobRef *ref = (ExternalBlobRef *) PG_GETARG_POINTER(0);
	void	   *data;
	Size		size;
	StringInfoData buf;

	data = ExternalBlobRead(ref, &size);

	pq_begintypsend(&buf);
	pq_sendbytes(&buf, data, size);
	pfree(data);

	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

/*
 * clob_in - Parse text input and create an external CLOB.
 */
Datum
clob_in(PG_FUNCTION_ARGS)
{
	char	   *input_str = PG_GETARG_CSTRING(0);
	ExternalBlobRef *ref;
	UndoRecPtr	undo_ptr;

	undo_ptr = GetCurrentTransactionUndoRecPtr();

	ref = ExternalBlobCreate(input_str, strlen(input_str), true, undo_ptr);

	PG_RETURN_POINTER(ref);
}

/*
 * clob_out - Output CLOB data as text string.
 */
Datum
clob_out(PG_FUNCTION_ARGS)
{
	ExternalBlobRef *ref = (ExternalBlobRef *) PG_GETARG_POINTER(0);
	void	   *data;
	Size		size;
	char	   *result;

	data = ExternalBlobRead(ref, &size);

	result = (char *) palloc(size + 1);
	memcpy(result, data, size);
	result[size] = '\0';
	pfree(data);

	PG_RETURN_CSTRING(result);
}

/*
 * clob_recv - Binary receive for CLOB.
 */
Datum
clob_recv(PG_FUNCTION_ARGS)
{
	StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);
	ExternalBlobRef *ref;
	int			nbytes;
	const char *data;
	UndoRecPtr	undo_ptr;

	undo_ptr = GetCurrentTransactionUndoRecPtr();

	nbytes = buf->len - buf->cursor;
	data = pq_getmsgbytes(buf, nbytes);

	ref = ExternalBlobCreate(data, nbytes, true, undo_ptr);

	PG_RETURN_POINTER(ref);
}

/*
 * clob_send - Binary send for CLOB.
 */
Datum
clob_send(PG_FUNCTION_ARGS)
{
	ExternalBlobRef *ref = (ExternalBlobRef *) PG_GETARG_POINTER(0);
	void	   *data;
	Size		size;
	StringInfoData buf;

	data = ExternalBlobRead(ref, &size);

	pq_begintypsend(&buf);
	pq_sendbytes(&buf, data, size);
	pfree(data);

	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

/* ----------------------------------------------------------------
 * Cast functions
 * ----------------------------------------------------------------
 */

Datum
blob_from_bytea(PG_FUNCTION_ARGS)
{
	bytea	   *data = PG_GETARG_BYTEA_P(0);
	ExternalBlobRef *ref;
	UndoRecPtr	undo_ptr;

	undo_ptr = GetCurrentTransactionUndoRecPtr();

	ref = ExternalBlobCreate(VARDATA_ANY(data), VARSIZE_ANY_EXHDR(data),
							 false, undo_ptr);

	PG_RETURN_POINTER(ref);
}

Datum
bytea_from_blob(PG_FUNCTION_ARGS)
{
	ExternalBlobRef *ref = (ExternalBlobRef *) PG_GETARG_POINTER(0);
	void	   *data;
	Size		size;
	bytea	   *result;

	data = ExternalBlobRead(ref, &size);

	result = (bytea *) palloc(size + VARHDRSZ);
	SET_VARSIZE(result, size + VARHDRSZ);
	memcpy(VARDATA(result), data, size);
	pfree(data);

	PG_RETURN_BYTEA_P(result);
}

Datum
clob_from_text(PG_FUNCTION_ARGS)
{
	text	   *data = PG_GETARG_TEXT_P(0);
	ExternalBlobRef *ref;
	UndoRecPtr	undo_ptr;

	undo_ptr = GetCurrentTransactionUndoRecPtr();

	ref = ExternalBlobCreate(VARDATA_ANY(data), VARSIZE_ANY_EXHDR(data),
							 true, undo_ptr);

	PG_RETURN_POINTER(ref);
}

Datum
text_from_clob(PG_FUNCTION_ARGS)
{
	ExternalBlobRef *ref = (ExternalBlobRef *) PG_GETARG_POINTER(0);
	void	   *data;
	Size		size;
	text	   *result;

	data = ExternalBlobRead(ref, &size);

	result = (text *) palloc(size + VARHDRSZ);
	SET_VARSIZE(result, size + VARHDRSZ);
	memcpy(VARDATA(result), data, size);
	pfree(data);

	PG_RETURN_TEXT_P(result);
}

/* ----------------------------------------------------------------
 * Comparison operators
 *
 * For equality, use hash-based short-circuit: identical hashes at
 * the same version are guaranteed identical (content-addressable).
 * For ordering, read and compare byte-by-byte.
 * ----------------------------------------------------------------
 */

/*
 * blob_compare_internal - shared comparison logic
 * Returns negative, 0, or positive like memcmp.
 */
static int
blob_compare_internal(ExternalBlobRef *ref1, ExternalBlobRef *ref2)
{
	void	   *data1;
	void	   *data2;
	Size		size1;
	Size		size2;
	int			cmp;

	data1 = ExternalBlobRead(ref1, &size1);
	data2 = ExternalBlobRead(ref2, &size2);

	cmp = memcmp(data1, data2, Min(size1, size2));
	if (cmp == 0 && size1 != size2)
		cmp = (size1 < size2) ? -1 : 1;

	pfree(data1);
	pfree(data2);

	return cmp;
}

Datum
blob_eq(PG_FUNCTION_ARGS)
{
	ExternalBlobRef *ref1 = (ExternalBlobRef *) PG_GETARG_POINTER(0);
	ExternalBlobRef *ref2 = (ExternalBlobRef *) PG_GETARG_POINTER(1);

	if (ref1->size != ref2->size)
		PG_RETURN_BOOL(false);
	if (memcmp(ref1->hash, ref2->hash, EXTERNAL_BLOB_HASH_LEN) == 0 &&
		ref1->version == ref2->version)
		PG_RETURN_BOOL(true);

	PG_RETURN_BOOL(blob_compare_internal(ref1, ref2) == 0);
}

Datum
blob_ne(PG_FUNCTION_ARGS)
{
	ExternalBlobRef *ref1 = (ExternalBlobRef *) PG_GETARG_POINTER(0);
	ExternalBlobRef *ref2 = (ExternalBlobRef *) PG_GETARG_POINTER(1);

	if (ref1->size != ref2->size)
		PG_RETURN_BOOL(true);
	if (memcmp(ref1->hash, ref2->hash, EXTERNAL_BLOB_HASH_LEN) == 0 &&
		ref1->version == ref2->version)
		PG_RETURN_BOOL(false);

	PG_RETURN_BOOL(blob_compare_internal(ref1, ref2) != 0);
}

Datum
blob_lt(PG_FUNCTION_ARGS)
{
	ExternalBlobRef *ref1 = (ExternalBlobRef *) PG_GETARG_POINTER(0);
	ExternalBlobRef *ref2 = (ExternalBlobRef *) PG_GETARG_POINTER(1);

	PG_RETURN_BOOL(blob_compare_internal(ref1, ref2) < 0);
}

Datum
blob_le(PG_FUNCTION_ARGS)
{
	ExternalBlobRef *ref1 = (ExternalBlobRef *) PG_GETARG_POINTER(0);
	ExternalBlobRef *ref2 = (ExternalBlobRef *) PG_GETARG_POINTER(1);

	PG_RETURN_BOOL(blob_compare_internal(ref1, ref2) <= 0);
}

Datum
blob_gt(PG_FUNCTION_ARGS)
{
	ExternalBlobRef *ref1 = (ExternalBlobRef *) PG_GETARG_POINTER(0);
	ExternalBlobRef *ref2 = (ExternalBlobRef *) PG_GETARG_POINTER(1);

	PG_RETURN_BOOL(blob_compare_internal(ref1, ref2) > 0);
}

Datum
blob_ge(PG_FUNCTION_ARGS)
{
	ExternalBlobRef *ref1 = (ExternalBlobRef *) PG_GETARG_POINTER(0);
	ExternalBlobRef *ref2 = (ExternalBlobRef *) PG_GETARG_POINTER(1);

	PG_RETURN_BOOL(blob_compare_internal(ref1, ref2) >= 0);
}

Datum
blob_cmp(PG_FUNCTION_ARGS)
{
	ExternalBlobRef *ref1 = (ExternalBlobRef *) PG_GETARG_POINTER(0);
	ExternalBlobRef *ref2 = (ExternalBlobRef *) PG_GETARG_POINTER(1);

	PG_RETURN_INT32(blob_compare_internal(ref1, ref2));
}

/* CLOB comparison operators -- same logic, different type name */

Datum
clob_eq(PG_FUNCTION_ARGS)
{
	ExternalBlobRef *ref1 = (ExternalBlobRef *) PG_GETARG_POINTER(0);
	ExternalBlobRef *ref2 = (ExternalBlobRef *) PG_GETARG_POINTER(1);

	if (ref1->size != ref2->size)
		PG_RETURN_BOOL(false);
	if (memcmp(ref1->hash, ref2->hash, EXTERNAL_BLOB_HASH_LEN) == 0 &&
		ref1->version == ref2->version)
		PG_RETURN_BOOL(true);

	PG_RETURN_BOOL(blob_compare_internal(ref1, ref2) == 0);
}

Datum
clob_ne(PG_FUNCTION_ARGS)
{
	ExternalBlobRef *ref1 = (ExternalBlobRef *) PG_GETARG_POINTER(0);
	ExternalBlobRef *ref2 = (ExternalBlobRef *) PG_GETARG_POINTER(1);

	if (ref1->size != ref2->size)
		PG_RETURN_BOOL(true);
	if (memcmp(ref1->hash, ref2->hash, EXTERNAL_BLOB_HASH_LEN) == 0 &&
		ref1->version == ref2->version)
		PG_RETURN_BOOL(false);

	PG_RETURN_BOOL(blob_compare_internal(ref1, ref2) != 0);
}

Datum
clob_lt(PG_FUNCTION_ARGS)
{
	ExternalBlobRef *ref1 = (ExternalBlobRef *) PG_GETARG_POINTER(0);
	ExternalBlobRef *ref2 = (ExternalBlobRef *) PG_GETARG_POINTER(1);

	PG_RETURN_BOOL(blob_compare_internal(ref1, ref2) < 0);
}

Datum
clob_le(PG_FUNCTION_ARGS)
{
	ExternalBlobRef *ref1 = (ExternalBlobRef *) PG_GETARG_POINTER(0);
	ExternalBlobRef *ref2 = (ExternalBlobRef *) PG_GETARG_POINTER(1);

	PG_RETURN_BOOL(blob_compare_internal(ref1, ref2) <= 0);
}

Datum
clob_gt(PG_FUNCTION_ARGS)
{
	ExternalBlobRef *ref1 = (ExternalBlobRef *) PG_GETARG_POINTER(0);
	ExternalBlobRef *ref2 = (ExternalBlobRef *) PG_GETARG_POINTER(1);

	PG_RETURN_BOOL(blob_compare_internal(ref1, ref2) > 0);
}

Datum
clob_ge(PG_FUNCTION_ARGS)
{
	ExternalBlobRef *ref1 = (ExternalBlobRef *) PG_GETARG_POINTER(0);
	ExternalBlobRef *ref2 = (ExternalBlobRef *) PG_GETARG_POINTER(1);

	PG_RETURN_BOOL(blob_compare_internal(ref1, ref2) >= 0);
}

Datum
clob_cmp(PG_FUNCTION_ARGS)
{
	ExternalBlobRef *ref1 = (ExternalBlobRef *) PG_GETARG_POINTER(0);
	ExternalBlobRef *ref2 = (ExternalBlobRef *) PG_GETARG_POINTER(1);

	PG_RETURN_INT32(blob_compare_internal(ref1, ref2));
}

/*
 * ExternalBlobPerformVacuum - Perform blob maintenance during VACUUM
 *
 * This function is called by the VACUUM command to perform blob-specific
 * maintenance tasks:
 *   1. Garbage collection of unreferenced blob files
 *   2. Delta chain compaction
 *   3. Statistics collection
 *
 * Returns statistics about work performed, which VACUUM VERBOSE will report.
 */
void
ExternalBlobPerformVacuum(bool verbose, ExternalBlobVacuumStats *stats)
{
	DIR		   *dir;
	DIR		   *prefix_dir;
	DIR		   *count_dir;
	struct dirent *entry;
	struct dirent *file_entry;
	struct dirent *count_entry;
	const char *blob_dir;
	char		prefix_path[MAXPGPATH];
	uint64		compactions_performed = 0;
	uint64		files_removed = 0;
	uint64		bytes_reclaimed = 0;
	uint64		total_storage_bytes = 0;
	uint64		gc_start_files = 0;
	int64		start_time = 0;
	int64		end_time;
	struct stat dir_st_before;
	struct stat dir_st_after;

	/* Initialize stats */
	if (stats)
		memset(stats, 0, sizeof(ExternalBlobVacuumStats));

	/* Track timing if verbose */
	if (verbose)
		start_time = GetCurrentTimestamp();

	blob_dir = blob_directory ? blob_directory : EXTBLOB_DIRECTORY;

	/* Open blob directory */
	dir = opendir(blob_dir);
	if (dir == NULL)
	{
		/* Directory doesn't exist yet - nothing to do */
		if (stats)
		{
			stats->files_removed = 0;
			stats->bytes_reclaimed = 0;
			stats->compactions_performed = 0;
		}
		return;
	}

	ereport(verbose ? INFO : DEBUG1,
			(errmsg("vacuuming external blob storage")));

	/*
	 * Phase 1: Scan through hash prefix subdirectories and perform compaction
	 * on blobs with long delta chains
	 */
	while ((entry = readdir(dir)) != NULL)
	{
		/* Skip . and .. */
		if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
			continue;

		/* Process subdirectory */
		snprintf(prefix_path, sizeof(prefix_path), "%s/%s", blob_dir, entry->d_name);
		prefix_dir = opendir(prefix_path);
		if (prefix_dir == NULL)
			continue;

		/* Scan for blob files that need compaction */
		while ((file_entry = readdir(prefix_dir)) != NULL)
		{
			struct stat st;
			char	   *dot_pos;
			char		filepath[MAXPGPATH];
			uint8		hash[EXTERNAL_BLOB_HASH_LEN];
			int			delta_count = 0;

			if (strcmp(file_entry->d_name, ".") == 0 ||
				strcmp(file_entry->d_name, "..") == 0)
				continue;

			/* Count .delta files for each blob */
			dot_pos = strstr(file_entry->d_name, ".delta.");
			if (dot_pos != NULL)
			{
				/* Parse hash from filename */
				if (strlen(file_entry->d_name) >= EXTERNAL_BLOB_HASH_LEN * 2)
				{
					char	hash_hex[EXTERNAL_BLOB_HASH_LEN * 2 + 1];

					memcpy(hash_hex, file_entry->d_name, EXTERNAL_BLOB_HASH_LEN * 2);
					hash_hex[EXTERNAL_BLOB_HASH_LEN * 2] = '\0';

					/* Convert hex to binary */
					for (int i = 0; i < EXTERNAL_BLOB_HASH_LEN; i++)
					{
						sscanf(hash_hex + (i * 2), "%2hhx", &hash[i]);
					}

					/* Count deltas for this blob */
					count_dir = opendir(prefix_path);
					if (count_dir)
					{
						while ((count_entry = readdir(count_dir)) != NULL)
						{
							if (strncmp(count_entry->d_name, hash_hex, EXTERNAL_BLOB_HASH_LEN * 2) == 0 &&
								strstr(count_entry->d_name, ".delta.") != NULL)
								delta_count++;
						}
						closedir(count_dir);
					}

					/* If delta chain is long enough, trigger compaction */
					if (delta_count >= blob_compaction_threshold)
					{
						PG_TRY();
						{
							ExternalBlobCompactDeltas(hash, 0);
							compactions_performed++;

							if (verbose)
								ereport(INFO,
										(errmsg("compacted blob delta chain: %d deltas merged",
												delta_count)));
						}
						PG_CATCH();
						{
							/* Log error but continue with other blobs */
							EmitErrorReport();
							FlushErrorState();
						}
						PG_END_TRY();
					}
				}
			}

			/* Accumulate total storage used */
			snprintf(filepath, sizeof(filepath), "%s/%s", prefix_path, file_entry->d_name);
			if (stat(filepath, &st) == 0)
				total_storage_bytes += st.st_size;
		}

		closedir(prefix_dir);

		/* Check for shutdown request periodically */
		CHECK_FOR_INTERRUPTS();
	}

	/* Rewind directory for garbage collection pass */
	rewinddir(dir);

	/*
	 * Phase 2: Garbage collection - call the existing ExternalBlobVacuum()
	 */

	/* Get directory size before GC (approximate) */
	if (stat(blob_dir, &dir_st_before) == 0)
		gc_start_files = dir_st_before.st_size;

	/* Perform GC via existing worker function */
	ExternalBlobVacuum();

	/* Estimate bytes reclaimed (rough approximation) */
	if (stat(blob_dir, &dir_st_after) == 0 && dir_st_after.st_size < gc_start_files)
		bytes_reclaimed = gc_start_files - dir_st_after.st_size;

	closedir(dir);

	/* Calculate elapsed time */
	if (verbose)
	{
		end_time = GetCurrentTimestamp();
		stats->elapsed_ms = (end_time - start_time) / 1000;
	}

	/* Fill in statistics */
	if (stats)
	{
		stats->files_removed = files_removed;
		stats->bytes_reclaimed = bytes_reclaimed;
		stats->compactions_performed = compactions_performed;
		stats->total_storage_bytes = total_storage_bytes;
	}

	/* Report results */
	if (verbose || compactions_performed > 0 || files_removed > 0)
	{
		if (compactions_performed > 0)
			ereport(INFO,
					(errmsg("compacted %lu blob delta chains", compactions_performed)));

		if (bytes_reclaimed > 0)
			ereport(INFO,
					(errmsg("reclaimed %lu bytes from blob storage", bytes_reclaimed)));

		ereport(INFO,
				(errmsg("external blob storage: %.2f MB total",
						total_storage_bytes / (1024.0 * 1024.0))));
	}
}
