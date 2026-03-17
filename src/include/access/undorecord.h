/*-------------------------------------------------------------------------
 *
 * undorecord.h
 *	  UNDO record format and insertion API
 *
 * This file defines the generic UNDO record format that can be used by
 * heap and other table access methods. UNDO records capture information
 * needed to undo operations during transaction rollback or to recover
 * pruned tuple versions.
 *
 * Design principles:
 * - Generic: Usable by any table AM
 * - Compact: Variable-length format to minimize space
 * - Chained: Records form backward chains via urec_prev pointer
 * - Batch-oriented: API encourages batching for performance
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/undorecord.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef UNDORECORD_H
#define UNDORECORD_H

#include "access/htup.h"
#include "access/undolog.h"
#include "access/xlogdefs.h"
#include "storage/block.h"
#include "utils/rel.h"
#include "storage/itemptr.h"

/*
 * UNDO record types
 *
 * These identify what kind of operation the UNDO record represents.
 * The type determines how to interpret the payload and how to apply
 * the UNDO during rollback.
 */
#define UNDO_INSERT		0x0001	/* INSERT operation - just mark, no data */
#define UNDO_DELETE		0x0002	/* DELETE operation - store full old tuple */
#define UNDO_UPDATE		0x0003	/* UPDATE operation - store old tuple or delta */
#define UNDO_PRUNE		0x0004	/* PRUNE operation - store pruned tuple versions */
#define UNDO_INPLACE	0x0005	/* In-place UPDATE - store old tuple */

/*
 * UNDO record info flags
 *
 * These flags provide additional metadata about the UNDO record.
 */
#define UNDO_INFO_HAS_TUPLE		0x01	/* Record contains tuple data */
#define UNDO_INFO_HAS_DELTA		0x02	/* Record contains column delta */
#define UNDO_INFO_HAS_TOAST		0x04	/* Tuple has TOAST references */
#define UNDO_INFO_XID_VALID		0x08	/* urec_xid is valid */

/*
 * UndoRecordHeader - Fixed header for all UNDO records
 *
 * Every UNDO record starts with this header, followed by variable-length
 * payload data. The format is inspired by ZHeap and BerkeleyDB designs.
 *
 * Size: 32 bytes (optimized for alignment)
 */
typedef struct UndoRecordHeader
{
	uint16		urec_type;		/* UNDO_INSERT/DELETE/UPDATE/PRUNE/etc */
	uint16		urec_info;		/* Flags (UNDO_INFO_*) */
	uint32		urec_len;		/* Total length including header */

	TransactionId urec_xid;		/* Transaction that created this */
	UndoRecPtr	urec_prev;		/* Previous UNDO for same xact (chain) */

	Oid			urec_reloid;	/* Relation OID */
	BlockNumber urec_blkno;		/* Block number */
	OffsetNumber urec_offset;	/* Tuple offset */

	uint16		urec_payload_len; /* Length of payload data */

	/* Followed by variable-length payload data */
} UndoRecordHeader;

#define SizeOfUndoRecordHeader	(offsetof(UndoRecordHeader, urec_payload_len) + sizeof(uint16))

/*
 * UndoRecordSet - Batch container for UNDO records
 *
 * This structure accumulates multiple UNDO records before writing them
 * to the UNDO log in a single operation. This improves performance by
 * reducing the number of I/O operations and lock acquisitions.
 *
 * The records are serialized into a contiguous buffer before being
 * written to the UNDO log.
 */
typedef struct UndoRecordSet
{
	TransactionId xid;			/* Transaction ID for all records */
	UndoRecPtr	prev_undo_ptr;	/* Previous UNDO pointer in chain */

	int			nrecords;		/* Number of records in set */
	int			max_records;	/* Allocated capacity */

	char	   *buffer;			/* Serialized record buffer */
	Size		buffer_size;	/* Current buffer size */
	Size		buffer_capacity; /* Allocated buffer capacity */

	MemoryContext mctx;			/* Memory context for allocations */
} UndoRecordSet;

/*
 * Public API for UNDO record management
 */

/* Create/destroy UNDO record sets */
extern UndoRecordSet *UndoRecordSetCreate(TransactionId xid);
extern void UndoRecordSetFree(UndoRecordSet *uset);

/* Add records to a set */
extern void UndoRecordAddTuple(UndoRecordSet *uset,
							   uint16 record_type,
							   Relation rel,
							   BlockNumber blkno,
							   OffsetNumber offset,
							   HeapTuple oldtuple);

/* Insert the accumulated records into UNDO log */
extern UndoRecPtr UndoRecordSetInsert(UndoRecordSet *uset);

/* Utility functions for record manipulation */
extern Size UndoRecordGetSize(uint16 record_type, HeapTuple tuple);
extern void UndoRecordSerialize(char *dest, UndoRecordHeader *header,
								const char *payload, Size payload_len);
extern bool UndoRecordDeserialize(const char *src, UndoRecordHeader *header,
								  char **payload);

/* Statistics and debugging */
extern Size UndoRecordSetGetSize(UndoRecordSet *uset);

#endif							/* UNDORECORD_H */
