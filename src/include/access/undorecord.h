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
 * - Physical: UNDO stores complete tuple data for direct memcpy restore
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
#include "access/undodefs.h"
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
#define UNDO_INSERT		0x0001	/* INSERT operation - store inserted tuple for
								 * physical removal */
#define UNDO_DELETE		0x0002	/* DELETE operation - store full old tuple for
								 * physical restoration */
#define UNDO_UPDATE		0x0003	/* UPDATE operation - store old tuple data for
								 * physical restoration */
#define UNDO_PRUNE		0x0004	/* PRUNE operation - store pruned tuple
								 * versions */
#define UNDO_INPLACE	0x0005	/* In-place UPDATE - store old tuple data */

/*
 * UNDO record info flags
 *
 * These flags provide additional metadata about the UNDO record.
 */
#define UNDO_INFO_HAS_TUPLE		0x01	/* Record contains complete tuple data */
#define UNDO_INFO_HAS_DELTA		0x02	/* Record contains column delta */
#define UNDO_INFO_HAS_TOAST		0x04	/* Tuple has TOAST references */
#define UNDO_INFO_XID_VALID		0x08	/* urec_xid is valid */
#define UNDO_INFO_HAS_INDEX		0x10	/* Relation has indexes (affects
										 * INSERT undo: dead vs unused) */
#define UNDO_INFO_HAS_CLR		0x20	/* CLR has been written for this
										 * record (urec_clr_ptr is valid) */

/*
 * UndoRecTupleData - Variable-length tuple data stored in UNDO records
 *
 * Physical UNDO stores complete tuple data so that rollback can restore
 * tuples via direct memcpy into shared buffer pages.  This is modeled
 * after ZHeap's uur_tuple field.
 *
 * For UNDO_DELETE and UNDO_UPDATE: contains the complete old tuple that
 * should be restored on rollback.
 *
 * For UNDO_INSERT: contains the tuple length (for ItemId adjustment)
 * but the data is not needed since we mark the slot dead/unused.
 *
 * For UNDO_INPLACE: contains the old tuple data to memcpy back.
 */
typedef struct UndoRecTupleData
{
	uint32		len;			/* Length of tuple data that follows */
	/* Followed by 'len' bytes of HeapTupleHeaderData + user data */
}			UndoRecTupleData;

/*
 * UndoRecordHeader - Fixed header for all UNDO records
 *
 * Every UNDO record starts with this header, followed by optional
 * UndoRecTupleData containing complete tuple bytes for physical restore.
 *
 * The physical approach stores enough information to restore the page
 * to its pre-operation state via memcpy, rather than using logical
 * operations like simple_heap_delete/insert.
 *
 * Size: 48 bytes (optimized for alignment)
 */
typedef struct UndoRecordHeader
{
	uint16		urec_type;		/* UNDO_INSERT/DELETE/UPDATE/PRUNE/etc */
	uint16		urec_info;		/* Flags (UNDO_INFO_*) */
	uint32		urec_len;		/* Total length including header and tuple
								 * data */

	TransactionId urec_xid;		/* Transaction that created this */
	UndoRecPtr	urec_prev;		/* Previous UNDO for same xact (chain) */

	Oid			urec_reloid;	/* Relation OID */
	BlockNumber urec_blkno;		/* Block number of target page */
	OffsetNumber urec_offset;	/* Item offset within page */

	uint16		urec_payload_len;	/* Length of payload/tuple data */

	/*
	 * Tuple data length stored in UNDO. For DELETE/UPDATE/INPLACE, this is
	 * the complete old tuple size. For INSERT, this is the size of the
	 * inserted tuple (used for ItemId manipulation during undo).
	 */
	uint32		urec_tuple_len; /* Length of tuple data in record */

	/*
	 * CLR (Compensation Log Record) pointer.  When this UNDO record is
	 * applied during rollback, the XLogRecPtr of the CLR WAL record is stored
	 * here.  This links the UNDO record to its compensation record in WAL,
	 * enabling crash recovery to determine which UNDO records have already
	 * been applied.  Set to InvalidXLogRecPtr until the record is applied.
	 *
	 * During crash recovery, if urec_clr_ptr is valid, the UNDO record has
	 * already been applied and can be skipped during re-rollback.  This
	 * prevents double-application of UNDO operations.
	 */
	XLogRecPtr	urec_clr_ptr;	/* CLR WAL pointer, InvalidXLogRecPtr if not
								 * yet applied */

	/* Followed by variable-length payload/tuple data */
}			UndoRecordHeader;

#define SizeOfUndoRecordHeader	(offsetof(UndoRecordHeader, urec_clr_ptr) + sizeof(XLogRecPtr))

/*
 * Access macros for tuple data following the header
 *
 * The tuple data immediately follows the fixed header in the serialized
 * record.  These macros provide typed access.
 */
#define UndoRecGetTupleData(header) \
	((char *)(header) + SizeOfUndoRecordHeader)

#define UndoRecGetTupleHeader(header) \
	((HeapTupleHeader) UndoRecGetTupleData(header))

/*
 * UndoRecordSetChunkHeader - Header at the start of each chunk.
 *
 * When an UndoRecordSet spans multiple undo logs (rare, since each log
 * is up to 1TB), the data is organized into chunks, each with a header
 * that records the chunk size and a back-pointer to the previous chunk.
 * This design follows the EDB undo-record-set branch architecture.
 */
typedef struct UndoRecordSetChunkHeader
{
	UndoLogOffset size;
	UndoRecPtr	previous_chunk;
	uint8		type;
}			UndoRecordSetChunkHeader;

#define SizeOfUndoRecordSetChunkHeader \
	(offsetof(UndoRecordSetChunkHeader, type) + sizeof(uint8))

/*
 * Possible undo record set types.
 */
typedef enum UndoRecordSetType
{
	URST_INVALID = 0,			/* Placeholder when there's no record set. */
	URST_TRANSACTION = 'T',		/* Normal xact undo; apply on abort. */
	URST_MULTI = 'M',			/* Informational undo. */
	URST_EPHEMERAL = 'E'		/* Ephemeral data for testing purposes. */
} UndoRecordSetType;

/*
 * UndoRecordSet - Batch container for UNDO records
 *
 * This structure accumulates multiple UNDO records before writing them
 * to the UNDO log in a single operation. This improves performance by
 * reducing the number of I/O operations and lock acquisitions.
 *
 * The records are serialized into a contiguous buffer that grows
 * dynamically. The design follows the EDB undo-record-set branch
 * architecture with chunk-based organization and per-persistence-level
 * separation.
 */
typedef struct UndoRecordSet
{
	TransactionId xid;			/* Transaction ID for all records */
	UndoRecPtr	prev_undo_ptr;	/* Previous UNDO pointer in chain */
	UndoPersistenceLevel persistence;	/* Persistence level of this set */
	UndoRecordSetType type;		/* Record set type */

	int			nrecords;		/* Number of records in set */

	/*
	 * Dynamic buffer for serialized records. Grows as needed; no fixed
	 * maximum. This replaces the old fixed-capacity max_records array.
	 */
	char	   *buffer;			/* Serialized record buffer */
	Size		buffer_size;	/* Current buffer size */
	Size		buffer_capacity;	/* Allocated buffer capacity */

	MemoryContext mctx;			/* Memory context for allocations */
}			UndoRecordSet;

/*
 * Public API for UNDO record management
 */

/* Create/destroy UNDO record sets */
extern UndoRecordSet * UndoRecordSetCreate(TransactionId xid,
										   UndoRecPtr prev_undo_ptr);
extern void UndoRecordSetFree(UndoRecordSet * uset);

/* Add records to a set */
extern void UndoRecordAddTuple(UndoRecordSet * uset,
							   uint16 record_type,
							   Relation rel,
							   BlockNumber blkno,
							   OffsetNumber offset,
							   HeapTuple oldtuple);

/* Insert the accumulated records into UNDO log */
extern UndoRecPtr UndoRecordSetInsert(UndoRecordSet * uset);

/* Utility functions for record manipulation */
extern Size UndoRecordGetSize(uint16 record_type, HeapTuple tuple);
extern void UndoRecordSerialize(char *dest, UndoRecordHeader * header,
								const char *payload, Size payload_len);
extern bool UndoRecordDeserialize(const char *src, UndoRecordHeader * header,
								  char **payload);

/* Statistics and debugging */
extern Size UndoRecordSetGetSize(UndoRecordSet * uset);

/* UNDO application during rollback */
extern void ApplyUndoChain(UndoRecPtr start_ptr);

#endif							/* UNDORECORD_H */
