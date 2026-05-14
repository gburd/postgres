/*-------------------------------------------------------------------------
 *
 * slog_flathash.h
 *	  LRLock-protected flat open-addressing hash for sLog tuple tracking.
 *
 * This provides wait-free read access to sLog tuple entries via the
 * left-right lock primitive.  The flat hash uses open addressing with
 * linear probing and tombstone markers for deletions.
 *
 * Architecture: The LRLock maintains TWO identical copies of the hash in
 * shared memory.  Readers access the "read copy" via atomic epoch counter
 * (wait-free -- no lock, no CAS on the hot path).  Writers apply mutations
 * to both copies sequentially via an oplog, serialized by an external
 * LWLock (SLogTupleWriterLock).  The two-copy invariant means a reader
 * always sees a consistent snapshot even while a writer is mid-mutation.
 *
 * Scan semantics: SLogFlatHashScanInit/ScanNext iterate all occupied
 * buckets linearly.  Scans must occur within an LRLock read-side or
 * write-side critical section.  For write operations that need global
 * scans (eviction, xid removal), the pattern is: read-side scan to
 * collect keys, then batch-apply write ops under the writer lock.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/slog_flathash.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SLOG_FLATHASH_H
#define SLOG_FLATHASH_H

#include "access/slog.h"
#include "storage/lrlock.h"
#include "storage/lwlock.h"

/*
 * Bucket states encoded via hash_val:
 *   0              = empty (never used)
 *   UINT32_MAX     = tombstone (deleted)
 *   anything else  = occupied with that hash value
 */
#define SLOG_FLAT_EMPTY		0
#define SLOG_FLAT_TOMBSTONE	UINT32_MAX

/*
 * SLogFlatBucket - One bucket in the flat open-addressing hash table.
 *
 * Layout: hash_val marks the state, key is the lookup key, entry contains
 * the full SLogTupleEntry data (ops array etc).
 */
typedef struct SLogFlatBucket
{
	uint32		hash_val;		/* 0=empty, TOMBSTONE=deleted, else hash */
	SLogTupleKey key;			/* (relid, tid) */
	uint16		padding;		/* alignment padding */
	SLogTupleEntry entry;		/* nops + ops[SLOG_MAX_TUPLE_OPS] */
}			SLogFlatBucket;

/*
 * SLogFlatHash - The flat hash table header, followed by buckets[].
 *
 * Allocated as the data payload of an LRLock (two copies in shared memory).
 */
typedef struct SLogFlatHash
{
	int32		capacity;		/* number of buckets (power of 2) */
	int32		num_entries;	/* current live entries */
	int32		num_tombstones; /* tombstone count (for load factor) */
	int32		padding;
	SLogFlatBucket buckets[FLEXIBLE_ARRAY_MEMBER];
}			SLogFlatHash;

/*
 * Operation kinds for the LRLock oplog.
 */
typedef enum SLogFlatOpKind
{
	SLOG_FLAT_OP_INSERT,		/* Insert/update a single op slot */
	SLOG_FLAT_OP_REMOVE_XID,	/* Remove all ops for xid from entry */
	SLOG_FLAT_OP_REMOVE_ENTRY,	/* Remove entire entry (tombstone it) */
	SLOG_FLAT_OP_MARK_ABORTED,	/* Mark all ops for xid as ABORTED */
	SLOG_FLAT_OP_UPDATE_OP,		/* Update a specific op slot in-place */
	SLOG_FLAT_OP_COMMIT_XID,	/* Handle commit retention for an entry */
	SLOG_FLAT_OP_CLEANUP_RETAINED,	/* Remove old retained entries */
	SLOG_FLAT_OP_CREATE_ABORTED,	/* Create a new ABORTED entry (for
									 * local-only) */
}			SLogFlatOpKind;

/*
 * SLogFlatOp - A single operation to be applied to the flat hash.
 *
 * This is serialized into the LRLock oplog and applied to both copies.
 */
typedef struct SLogFlatOp
{
	SLogFlatOpKind kind;
	SLogTupleKey key;			/* which entry */
	TransactionId xid;			/* target xid */
	TransactionId subxid;		/* for subxid operations */
	uint64		commit_hlc;		/* for commit retention */
	SLogTupleOp tuple_op;		/* the op to insert/update (for INSERT) */
	dsa_pointer before_image_dp;	/* DSA pointer for before-image attachment */
}			SLogFlatOp;

/* ----------------------------------------------------------------
 * Partitioned flat hash: 32-way sharding to reduce writer lock contention.
 *
 * Each partition has its own LRLock (two copies of the flat hash segment)
 * and its own writer lock.  Key routing: hash(key) % NUM_PARTITIONS.
 * This reduces writer lock contention proportionally to the number of
 * partitions.
 *
 * The partition count is determined at startup by the slog_num_partitions
 * GUC (default: 0 = auto-size based on CPU count).  The heuristic targets
 * 4× the number of CPUs, clamped to [16, 256], rounded to next power of 2.
 * This ensures that at peak concurrency each CPU core has ~4 partitions to
 * spread writes across, minimizing writer lock wait time.
 * ----------------------------------------------------------------
 */

/* Default partition count used only before SLogShmemInit sets the real value */
#define SLOG_FLAT_DEFAULT_PARTITIONS	32
#define SLOG_FLAT_MIN_PARTITIONS		16
#define SLOG_FLAT_MAX_PARTITIONS		256

/*
 * SLogFlatPartition - Per-partition state.
 *
 * Each partition owns a slice of the total flat hash capacity and has
 * independent locking for both reads (LRLock) and writes (LWLock).
 */
typedef struct SLogFlatPartition
{
	LRLock	   *lrlock;			/* per-partition LRLock (wait-free reads) */
	LWLockPadded writer_lock;	/* per-partition writer serialization */
}			SLogFlatPartition;

/*
 * Compute the shared memory size needed for the flat hash data
 * (one copy — the LRLock allocates two copies internally).
 */
extern Size SLogFlatHashDataSize(int capacity);

/*
 * Compute the total shared memory needed for the LRLock + flat hash.
 */
extern Size SLogFlatHashShmemSize(int capacity, int max_backends);

/*
 * Compute the total shared memory needed for all partitions.
 */
extern Size SLogFlatHashPartitionedShmemSize(int total_capacity,
											 int max_backends);

/*
 * Initialize the flat hash in a pre-allocated LRLock data block.
 * Called during SLogShmemInit to set up both copies.
 */
extern void SLogFlatHashInit(void *data, int capacity);

/*
 * LRLock callbacks for the flat hash.
 */
extern void SLogFlatHashApply(void *data, const void *operation, Size op_size);
extern void SLogFlatHashSync(void *dst, const void *src, Size data_size);

/*
 * Hash computation for SLogTupleKey.
 */
extern uint32 SLogFlatHashComputeHash(const SLogTupleKey *key);

/*
 * Runtime partition count — set during SLogShmemInit() based on
 * the slog_num_partitions GUC.  Declared in slog.c.
 */
extern int	SLogNumPartitions;

/*
 * Compute which partition a key belongs to.
 */
static inline int
SLogFlatHashPartitionIndex(const SLogTupleKey *key)
{
	return (int) (SLogFlatHashComputeHash(key) % (uint32) SLogNumPartitions);
}

/*
 * Probe the flat hash for a key. Returns pointer to the bucket if found,
 * NULL if not found. Only valid during a read-side or write-side critical
 * section.
 */
extern SLogFlatBucket * SLogFlatHashProbe(const SLogFlatHash * ht,
										  const SLogTupleKey *key);

/*
 * Find a bucket for insertion (first empty or tombstone slot on probe chain).
 * Returns NULL if the table is full (all slots on probe chain occupied).
 * Only valid during write-side critical section.
 */
extern SLogFlatBucket * SLogFlatHashProbeForInsert(SLogFlatHash * ht,
												   const SLogTupleKey *key,
												   uint32 hash_val);

/*
 * Scan API for iterating all occupied buckets.
 *
 * Used by global-scan operations (eviction, xid removal, cleanup) that
 * need to visit every entry.  The scan iterates linearly over the bucket
 * array, skipping EMPTY and TOMBSTONE slots.
 *
 * Usage pattern:
 *   SLogFlatHashScanState state;
 *   const SLogFlatBucket *bucket;
 *
 *   SLogFlatHashScanInit(&state);
 *   while ((bucket = SLogFlatHashScanNext(ht, &state)) != NULL)
 *   {
 *       // process bucket->entry
 *   }
 *
 * The scan must be performed within an LRLock read-side or write-side
 * critical section.  For write operations, collect keys during a read-side
 * scan, then apply batch LRLock ops under the writer lock.
 */
typedef struct SLogFlatHashScanState
{
	int32		current_index;
}			SLogFlatHashScanState;

extern void SLogFlatHashScanInit(SLogFlatHashScanState * state);
extern const SLogFlatBucket *SLogFlatHashScanNext(const SLogFlatHash * ht,
												  SLogFlatHashScanState * state);

#endif							/* SLOG_FLATHASH_H */
