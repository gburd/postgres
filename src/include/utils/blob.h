/*-------------------------------------------------------------------------
 *
 * blob.h
 *	  External BLOB/CLOB types with filesystem storage
 *
 * This module provides the blob and clob data types which store a
 * fixed-size 40-byte inline reference (ExternalBlobRef) in the heap
 * tuple and actual content on the filesystem.  Storage uses a
 * content-addressable model with SHA-256 hashing and binary diffs
 * (deltas) for efficient updates.
 *
 * Features:
 *   - Content-addressable storage with SHA-256 hashing
 *   - Deduplication (identical content shares the same file)
 *   - Delta encoding for updates (bsdiff-inspired algorithm)
 *   - Transactional operations via FILEOPS integration
 *   - UNDO-based visibility and garbage collection
 *   - Background worker for delta compaction and vacuuming
 *
 * File layout in pg_external_blobs/:
 *   <hash[0:1] hex>/<hash[1:32] hex>.base      - Base version
 *   <hash[0:1] hex>/<hash[1:32] hex>.delta.N   - Nth delta
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/blob.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef BLOB_H
#define BLOB_H

#include "access/undodefs.h"
#include "common/cryptohash.h"
#include "common/sha2.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "port/pg_crc32c.h"

/* ----------------------------------------------------------------
 * Content hash
 * ----------------------------------------------------------------
 */
#define EXTERNAL_BLOB_HASH_LEN		PG_SHA256_DIGEST_LENGTH		/* 32 bytes */

/* ----------------------------------------------------------------
 * ExternalBlobRef - 40-byte inline tuple reference
 *
 * Stored directly in the heap tuple.  The SHA-256 hash provides
 * content-addressable lookup and deduplication.
 * ----------------------------------------------------------------
 */
typedef struct ExternalBlobRef
{
	uint8		hash[EXTERNAL_BLOB_HASH_LEN];	/* SHA-256 content hash */
	uint32		size;			/* Uncompressed content size (bytes) */
	uint16		version;		/* Delta chain position (0 = base) */
	uint16		flags;			/* EXTBLOB_FLAG_* */
} ExternalBlobRef;

#define EXTERNAL_BLOB_REF_SIZE	40
StaticAssertDecl(sizeof(ExternalBlobRef) == EXTERNAL_BLOB_REF_SIZE,
				 "ExternalBlobRef must be exactly 40 bytes");

/* ExternalBlobRef flags */
#define EXTBLOB_FLAG_CLOB			0x0001	/* Character data (CLOB) */
#define EXTBLOB_FLAG_COMPRESSED		0x0002	/* Delta uses LZ4 compression */
#define EXTBLOB_FLAG_TOMBSTONE		0x0004	/* Marked for GC deletion */

/* ----------------------------------------------------------------
 * File format constants
 * ----------------------------------------------------------------
 */
#define EXTBLOB_MAGIC				0x45424C42	/* "EBLB" */
#define EXTBLOB_DELTA_MAGIC			0x45424C44	/* "EBLD" */
#define EXTBLOB_FORMAT_VERSION		1

/* ----------------------------------------------------------------
 * ExternalBlobFileHeader - On-disk header for .base and .delta files
 *
 * Layout (24 bytes, uint64 first for natural alignment):
 *   undo_ptr(8) + magic(4) + data_size(4) + checksum(4)
 *   + flags(2) + format_version(2)
 * ----------------------------------------------------------------
 */
typedef struct ExternalBlobFileHeader
{
	UndoRecPtr	undo_ptr;		/* UNDO record pointer for visibility */
	uint32		magic;			/* EXTBLOB_MAGIC or EXTBLOB_DELTA_MAGIC */
	uint32		data_size;		/* Size of data following the header */
	pg_crc32c	checksum;		/* CRC-32C of the data (not header) */
	uint16		flags;			/* EXTBLOB_FLAG_* */
	uint16		format_version; /* EXTBLOB_FORMAT_VERSION */
} ExternalBlobFileHeader;

#define EXTBLOB_FILE_HEADER_SIZE	24
StaticAssertDecl(sizeof(ExternalBlobFileHeader) == EXTBLOB_FILE_HEADER_SIZE,
				 "ExternalBlobFileHeader must be exactly 24 bytes");

/* ----------------------------------------------------------------
 * Delta structures
 * ----------------------------------------------------------------
 */

/* Delta operation types */
typedef enum ExternalBlobDeltaOpType
{
	DELTA_OP_COPY = 1,			/* Copy from old version */
	DELTA_OP_ADD = 2			/* Add new data */
} ExternalBlobDeltaOpType;

/*
 * ExternalBlobDeltaOp - Single delta operation (in-memory)
 *
 * On disk, serialized as 9 packed bytes: type(1) + offset(4) + length(4).
 */
typedef struct ExternalBlobDeltaOp
{
	uint8		type;			/* DELTA_OP_COPY or DELTA_OP_ADD */
	uint32		offset;			/* Position in old data or delta add-data */
	uint32		length;			/* Byte count */
} ExternalBlobDeltaOp;

#define EXTBLOB_DELTA_OP_PACKED_SIZE	9

/*
 * ExternalBlobDeltaHeader - Follows ExternalBlobFileHeader in .delta files
 */
typedef struct ExternalBlobDeltaHeader
{
	uint32		old_size;		/* Size of previous version */
	uint32		new_size;		/* Size after applying delta */
	uint32		num_ops;		/* Number of delta operations */
	uint32		reserved;		/* Padding / future use */
} ExternalBlobDeltaHeader;

#define EXTBLOB_DELTA_HEADER_SIZE	16
StaticAssertDecl(sizeof(ExternalBlobDeltaHeader) == EXTBLOB_DELTA_HEADER_SIZE,
				 "ExternalBlobDeltaHeader must be exactly 16 bytes");

/* ----------------------------------------------------------------
 * Storage directory layout
 *
 * pg_external_blobs/<hash[0] hex>/<hash[1:32] hex>.base
 *
 * First byte of SHA-256 = 2 hex chars = 256 subdirectories.
 * ----------------------------------------------------------------
 */
#define EXTBLOB_DIRECTORY			"pg_external_blobs"
#define EXTBLOB_DIR_PREFIX_BYTES	1
#define EXTBLOB_HASH_HEX_LEN		(EXTERNAL_BLOB_HASH_LEN * 2)

#define EXTBLOB_BASE_SUFFIX			".base"
#define EXTBLOB_DELTA_SUFFIX		".delta"
#define EXTBLOB_TOMBSTONE_SUFFIX	".tombstone"

/* ----------------------------------------------------------------
 * GUC parameter defaults
 * ----------------------------------------------------------------
 */
#define EXTBLOB_DEFAULT_DELTA_THRESHOLD			1024	/* 1 KB */
#define EXTBLOB_DEFAULT_COMPACTION_THRESHOLD		10
#define EXTBLOB_DEFAULT_WORKER_NAPTIME			60000	/* 60 s */

/* Binary diff algorithm constants */
#define EXTBLOB_MIN_MATCH_LENGTH	32
#define EXTBLOB_MAX_SEARCH_DISTANCE	(64 * 1024)

/* ----------------------------------------------------------------
 * GUC variables (defined in blob.c)
 * ----------------------------------------------------------------
 */
extern int	blob_delta_threshold;
extern int	blob_compaction_threshold;
extern int	blob_worker_naptime;
extern bool enable_blob_compression;
extern char *blob_directory;

/* ----------------------------------------------------------------
 * fmgr interface macros
 * ----------------------------------------------------------------
 */
static inline ExternalBlobRef *
DatumGetExternalBlobRefP(Datum X)
{
	return (ExternalBlobRef *) DatumGetPointer(X);
}

static inline Datum
ExternalBlobRefPGetDatum(const ExternalBlobRef *X)
{
	return PointerGetDatum(X);
}

#define PG_GETARG_BLOB_P(n)		DatumGetExternalBlobRefP(PG_GETARG_DATUM(n))
#define PG_RETURN_BLOB_P(x)		return ExternalBlobRefPGetDatum(x)

/* ----------------------------------------------------------------
 * CRC-32C helper
 * ----------------------------------------------------------------
 */
static inline pg_crc32c
ExternalBlobComputeChecksum(const uint8 *data, Size len)
{
	pg_crc32c	crc;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, data, len);
	FIN_CRC32C(crc);
	return crc;
}

/* ----------------------------------------------------------------
 * Type I/O functions
 * ----------------------------------------------------------------
 */
extern Datum blob_in(PG_FUNCTION_ARGS);
extern Datum blob_out(PG_FUNCTION_ARGS);
extern Datum blob_recv(PG_FUNCTION_ARGS);
extern Datum blob_send(PG_FUNCTION_ARGS);

extern Datum clob_in(PG_FUNCTION_ARGS);
extern Datum clob_out(PG_FUNCTION_ARGS);
extern Datum clob_recv(PG_FUNCTION_ARGS);
extern Datum clob_send(PG_FUNCTION_ARGS);

/* ----------------------------------------------------------------
 * Cast functions
 * ----------------------------------------------------------------
 */
extern Datum blob_from_bytea(PG_FUNCTION_ARGS);
extern Datum bytea_from_blob(PG_FUNCTION_ARGS);
extern Datum clob_from_text(PG_FUNCTION_ARGS);
extern Datum text_from_clob(PG_FUNCTION_ARGS);

/* ----------------------------------------------------------------
 * Comparison operators
 * ----------------------------------------------------------------
 */
extern Datum blob_eq(PG_FUNCTION_ARGS);
extern Datum blob_ne(PG_FUNCTION_ARGS);
extern Datum blob_lt(PG_FUNCTION_ARGS);
extern Datum blob_le(PG_FUNCTION_ARGS);
extern Datum blob_gt(PG_FUNCTION_ARGS);
extern Datum blob_ge(PG_FUNCTION_ARGS);
extern Datum blob_cmp(PG_FUNCTION_ARGS);

extern Datum clob_eq(PG_FUNCTION_ARGS);
extern Datum clob_ne(PG_FUNCTION_ARGS);
extern Datum clob_lt(PG_FUNCTION_ARGS);
extern Datum clob_le(PG_FUNCTION_ARGS);
extern Datum clob_gt(PG_FUNCTION_ARGS);
extern Datum clob_ge(PG_FUNCTION_ARGS);
extern Datum clob_cmp(PG_FUNCTION_ARGS);

/* ----------------------------------------------------------------
 * BLOB operations
 * ----------------------------------------------------------------
 */
extern ExternalBlobRef *ExternalBlobCreate(const void *data, Size size,
										   bool is_clob,
										   UndoRecPtr undo_ptr);
extern void *ExternalBlobRead(const ExternalBlobRef *ref, Size *size_out);
extern ExternalBlobRef *ExternalBlobUpdate(const ExternalBlobRef *old_ref,
										   const void *new_data, Size new_size,
										   UndoRecPtr undo_ptr);
extern void ExternalBlobDelete(const ExternalBlobRef *ref,
							   UndoRecPtr undo_ptr);
extern bool ExternalBlobExists(const ExternalBlobRef *ref);

/* ----------------------------------------------------------------
 * Path and hash functions
 * ----------------------------------------------------------------
 */
extern void ExternalBlobComputeHash(const void *data, Size size,
									uint8 *hash_out);
extern void ExternalBlobHashToHex(const uint8 *hash, char *hex_out);
extern void ExternalBlobGetBasePath(const uint8 *hash, char *path_out,
									Size path_len);
extern void ExternalBlobGetDeltaPath(const uint8 *hash, uint16 version,
									 char *path_out, Size path_len);
extern void ExternalBlobGetDirPath(const uint8 *hash, char *path_out,
								   Size path_len);
extern void ExternalBlobEnsureDirectory(void);

/* ----------------------------------------------------------------
 * Delta compaction
 * ----------------------------------------------------------------
 */
extern void ExternalBlobCompactDeltas(const uint8 *hash,
									  uint16 max_version);

/* ----------------------------------------------------------------
 * Binary diff algorithm (blob_diff.c)
 * ----------------------------------------------------------------
 */
extern void ExternalBlobComputeDelta(const void *old_data, Size old_size,
									 const void *new_data, Size new_size,
									 StringInfo delta_out);
extern void *ExternalBlobApplyDelta(const void *old_data, Size old_size,
									const void *delta_data, Size delta_size,
									Size *new_size_out);

/* ----------------------------------------------------------------
 * Background worker (blob_worker.c)
 * ----------------------------------------------------------------
 */
extern void ExternalBlobWorkerMain(Datum main_arg);
extern void ExternalBlobWorkerRegister(void);
extern void ExternalBlobVacuum(void);

/* ----------------------------------------------------------------
 * Statistics
 * ----------------------------------------------------------------
 */
typedef struct ExternalBlobStats
{
	int64		num_blobs;
	int64		total_size;
	int64		num_deltas;
	int64		avg_delta_chain_len;
	int64		num_compactions;
	int64		num_gc_files;
} ExternalBlobStats;

typedef struct ExternalBlobVacuumStats
{
	uint64		files_removed;
	uint64		bytes_reclaimed;
	uint64		compactions_performed;
	uint64		total_storage_bytes;
	int64		elapsed_ms;
} ExternalBlobVacuumStats;

extern void ExternalBlobGetStats(ExternalBlobStats *stats);
extern void ExternalBlobPerformVacuum(bool verbose, ExternalBlobVacuumStats *stats);

#endif							/* BLOB_H */
