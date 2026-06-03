/*-------------------------------------------------------------------------
 *
 * recno_dict.h
 *	  Persistent storage for RECNO trained compression dictionaries
 *
 * Trained ZSTD/LZ4 dictionaries are stored in the relation's own
 * RECNO_DICT_FORKNUM fork so that any backend, and any backend after a
 * restart, can decompress a datum by the dictionary id embedded in its
 * compression header.  Dictionaries are append-only and never deleted:
 * a previously written compressed datum must always remain decompressable
 * by its embedded id even after ANALYZE selects a newer default dictionary
 * for new writes.
 *
 * Fork layout:
 *	 Block 0:  Directory metapage (PageHeaderData + RecnoDictMeta)
 *	 Block 1+: Serialized dictionary blobs, each a chain of data pages
 *			   (PageHeaderData + RecnoDictPageHeader + payload bytes)
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/recno_dict.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef RECNO_DICT_H
#define RECNO_DICT_H

#include "storage/block.h"
#include "storage/bufmgr.h"
#include "utils/rel.h"

/* Directory metapage identity */
#define RECNO_DICT_METAPAGE_MAGIC	0x52444943	/* "RDIC" */
#define RECNO_DICT_METAPAGE_VERSION 1

/* Maximum number of coexisting dictionaries per relation */
#define RECNO_DICT_MAX_DIRECTORY	256

/* Dictionary id 0 means "no dictionary" (plain, non-dict codec) */
#define RECNO_DICT_INVALID_ID		0

/*
 * One directory entry per trained dictionary.  Entries are append-only and
 * indexed positionally; dictid is the stable identifier embedded in
 * compressed datums.
 */
typedef struct RecnoDictDirEntry
{
	uint32		dictid;			/* stable id, RECNO_DICT_INVALID_ID = unused */
	uint8		codec;			/* RecnoCompressionType of trained dict */
	uint8		_pad[3];
	BlockNumber start_blkno;	/* first data block of the serialized blob */
	uint32		length;			/* total serialized blob length in bytes */
	uint32		orig_sample_size;	/* total sample bytes used to train it */
	uint64		trained_ts;		/* HLC timestamp when trained */
} RecnoDictDirEntry;

/*
 * Directory metapage contents (lives in block 0, after PageHeaderData).
 */
typedef struct RecnoDictMeta
{
	uint32		magic;
	uint32		version;
	uint32		count;			/* number of valid directory entries */
	uint32		next_dictid;	/* next id to assign (monotonic, starts at 1) */
	RecnoDictDirEntry entries[RECNO_DICT_MAX_DIRECTORY];
} RecnoDictMeta;

#define RecnoDictMetaSize	(offsetof(RecnoDictMeta, entries) + \
							 RECNO_DICT_MAX_DIRECTORY * sizeof(RecnoDictDirEntry))

/*
 * Per-page header for a serialized blob's data pages.  Blobs larger than a
 * page payload are chained via next_blkno.
 */
typedef struct RecnoDictPageHeader
{
	BlockNumber next_blkno;		/* next block of this blob, or invalid */
	uint32		bytes_on_page;	/* payload bytes following this header */
} RecnoDictPageHeader;

/* Payload bytes available per data page */
#define RecnoDictPagePayload \
	(BLCKSZ - MAXALIGN(SizeOfPageHeaderData) - \
	 MAXALIGN(sizeof(RecnoDictPageHeader)))

/*
 * recno_dict.c
 */
extern uint32 recno_dict_append(Relation rel, uint8 codec,
								const char *blob, uint32 length,
								uint32 orig_sample_size, uint64 trained_ts);
extern char *recno_dict_read(Relation rel, uint32 dictid,
							 uint8 *codec, uint32 *length);
extern uint32 recno_dict_count(Relation rel);

#endif							/* RECNO_DICT_H */
