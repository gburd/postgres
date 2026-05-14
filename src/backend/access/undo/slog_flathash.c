/*-------------------------------------------------------------------------
 *
 * slog_flathash.c
 *	  LRLock-protected flat open-addressing hash for sLog tuple tracking.
 *
 * Implements the flat hash table operations (probe, insert, remove) and
 * the LRLock apply/sync callbacks.  The hash uses linear probing with
 * a power-of-2 capacity and tombstone markers.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/undo/slog_flathash.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/slog_flathash.h"
#include "access/transam.h"
#include "common/hashfn.h"
#include "storage/lrlock.h"
#include "utils/dsa.h"

/*
 * Maximum probe distance before giving up.  With load factor < 0.7 and
 * power-of-2 sizing, typical probe chains are very short (< 5).
 * We cap at 128 to bound worst-case scan time.
 */
#define SLOG_FLAT_MAX_PROBE		128

/* ----------------------------------------------------------------
 * Size computation
 * ----------------------------------------------------------------
 */

/*
 * SLogFlatHashDataSize
 *		Size of one copy of the flat hash data structure.
 */
Size
SLogFlatHashDataSize(int capacity)
{
	return offsetof(SLogFlatHash, buckets) +
		(Size) capacity * sizeof(SLogFlatBucket);
}

/*
 * SLogFlatHashShmemSize
 *		Total shared memory needed for the LRLock + flat hash.
 *
 * The oplog is sized for MaxBackends * 4 ops between publishes.
 */
Size
SLogFlatHashShmemSize(int capacity, int max_backends)
{
	Size		data_size;
	Size		oplog_capacity;

	data_size = SLogFlatHashDataSize(capacity);

	/*
	 * Size oplog for burst writes: MaxBackends * 4 operations. Each operation
	 * is sizeof(SLogFlatOp) + sizeof(LRLockOpHeader).
	 */
	oplog_capacity = (Size) max_backends * 4 *
		(MAXALIGN(sizeof(SLogFlatOp)) + MAXALIGN(sizeof(Size)));
	oplog_capacity = Max(oplog_capacity, 65536);	/* minimum 64KB */

	return LRLockShmemSize(data_size, max_backends, oplog_capacity);
}

/*
 * SLogFlatHashPartitionedShmemSize
 *		Total shared memory needed for all partitions.
 *
 * Each partition gets capacity/N buckets and its own LRLock + writer lock.
 * The writer lock (LWLockPadded) is embedded in SLogFlatPartition in the
 * SLogSharedState, so only the LRLock blocks need to be sized here.
 */
Size
SLogFlatHashPartitionedShmemSize(int total_capacity, int max_backends)
{
	int			per_partition_capacity;
	Size		per_partition_size;
	Size		total;

	per_partition_capacity = total_capacity / SLogNumPartitions;
	if (per_partition_capacity < 64)
		per_partition_capacity = 64;

	per_partition_size = SLogFlatHashShmemSize(per_partition_capacity,
											   max_backends);
	total = (Size) SLogNumPartitions * MAXALIGN(per_partition_size);

	return total;
}

/* ----------------------------------------------------------------
 * Initialization
 * ----------------------------------------------------------------
 */

/*
 * SLogFlatHashInit
 *		Initialize a flat hash data block (one copy).
 *
 * Sets all buckets to EMPTY state.
 */
void
SLogFlatHashInit(void *data, int capacity)
{
	SLogFlatHash *ht = (SLogFlatHash *) data;
	int			i;

	ht->capacity = capacity;
	ht->num_entries = 0;
	ht->num_tombstones = 0;
	ht->padding = 0;

	for (i = 0; i < capacity; i++)
	{
		ht->buckets[i].hash_val = SLOG_FLAT_EMPTY;
		memset(&ht->buckets[i].key, 0, sizeof(SLogTupleKey));
		ht->buckets[i].padding = 0;
		ht->buckets[i].entry.nops = 0;
		memset(ht->buckets[i].entry.ops, 0,
			   sizeof(SLogTupleOp) * SLOG_MAX_TUPLE_OPS);
	}
}

/* ----------------------------------------------------------------
 * Hash function
 * ----------------------------------------------------------------
 */

/*
 * SLogFlatHashComputeHash
 *		Compute a 32-bit hash for an SLogTupleKey.
 *
 * The key must have been zeroed before population (for padding bytes).
 * Returns a non-zero, non-TOMBSTONE value (adjusts if hash_bytes returns
 * 0 or UINT32_MAX).
 */
uint32
SLogFlatHashComputeHash(const SLogTupleKey *key)
{
	uint32		h;

	h = hash_bytes((const unsigned char *) key, sizeof(SLogTupleKey));

	/* Ensure we never produce EMPTY or TOMBSTONE values */
	if (h == SLOG_FLAT_EMPTY)
		h = 1;
	else if (h == SLOG_FLAT_TOMBSTONE)
		h = SLOG_FLAT_TOMBSTONE - 1;

	return h;
}

/* ----------------------------------------------------------------
 * Probe operations
 * ----------------------------------------------------------------
 */

/*
 * SLogFlatHashProbe
 *		Look up a key in the flat hash. Returns bucket pointer if found,
 *		NULL if not present.
 *
 * Linear probing: start at hash_val % capacity, walk forward skipping
 * tombstones, stop at EMPTY (not found) or matching key (found).
 */
SLogFlatBucket *
SLogFlatHashProbe(const SLogFlatHash * ht, const SLogTupleKey *key)
{
	uint32		h;
	uint32		idx;
	int			probe;

	h = SLogFlatHashComputeHash(key);
	idx = h & (uint32) (ht->capacity - 1);

	for (probe = 0; probe < SLOG_FLAT_MAX_PROBE; probe++)
	{
		const		SLogFlatBucket *bucket = &ht->buckets[idx];

		if (bucket->hash_val == SLOG_FLAT_EMPTY)
			return NULL;		/* definitive miss */

		if (bucket->hash_val != SLOG_FLAT_TOMBSTONE &&
			bucket->hash_val == h &&
			memcmp(&bucket->key, key, sizeof(SLogTupleKey)) == 0)
		{
			return (SLogFlatBucket *) bucket;	/* found */
		}

		idx = (idx + 1) & (uint32) (ht->capacity - 1);
	}

	return NULL;				/* probe limit exceeded */
}

/*
 * SLogFlatHashProbeForInsert
 *		Find a slot for inserting a key. Returns the bucket to use.
 *
 * If the key already exists, returns that bucket (for update-in-place).
 * Otherwise returns the first EMPTY or TOMBSTONE slot encountered.
 * Returns NULL if probe limit exceeded without finding a slot.
 */
SLogFlatBucket *
SLogFlatHashProbeForInsert(SLogFlatHash * ht, const SLogTupleKey *key,
						   uint32 hash_val)
{
	uint32		idx;
	int			probe;
	SLogFlatBucket *first_free = NULL;

	idx = hash_val & (uint32) (ht->capacity - 1);

	for (probe = 0; probe < SLOG_FLAT_MAX_PROBE; probe++)
	{
		SLogFlatBucket *bucket = &ht->buckets[idx];

		if (bucket->hash_val == SLOG_FLAT_EMPTY)
		{
			/* Definitive miss — use first_free if we found one, else this */
			return first_free ? first_free : bucket;
		}

		if (bucket->hash_val == SLOG_FLAT_TOMBSTONE)
		{
			/* Remember first tombstone for potential reuse */
			if (first_free == NULL)
				first_free = bucket;
		}
		else if (bucket->hash_val == hash_val &&
				 memcmp(&bucket->key, key, sizeof(SLogTupleKey)) == 0)
		{
			/* Key already exists */
			return bucket;
		}

		idx = (idx + 1) & (uint32) (ht->capacity - 1);
	}

	/* Probe limit exceeded; return first_free if available */
	return first_free;
}

/* ----------------------------------------------------------------
 * Apply callback (for LRLock)
 * ----------------------------------------------------------------
 */

/*
 * flat_hash_apply_insert
 *		Apply an INSERT operation: find/create entry, add op to slot.
 */
static void
flat_hash_apply_insert(SLogFlatHash * ht, const SLogFlatOp * op)
{
	uint32		h;
	SLogFlatBucket *bucket;
	SLogTupleEntry *entry;
	int			i;
	bool		existing;

	h = SLogFlatHashComputeHash(&op->key);
	bucket = SLogFlatHashProbeForInsert(ht, &op->key, h);

	if (bucket == NULL)
		return;					/* table full, operation lost */

	/* Determine if this is an existing entry */
	existing = (bucket->hash_val != SLOG_FLAT_EMPTY &&
				bucket->hash_val != SLOG_FLAT_TOMBSTONE);

	if (!existing)
	{
		/* New entry */
		if (bucket->hash_val == SLOG_FLAT_TOMBSTONE)
			ht->num_tombstones--;

		bucket->hash_val = h;
		memcpy(&bucket->key, &op->key, sizeof(SLogTupleKey));
		bucket->entry.nops = 0;
		memset(&bucket->entry.key, 0, sizeof(SLogTupleKey));
		memcpy(&bucket->entry.key, &op->key, sizeof(SLogTupleKey));
		memset(bucket->entry.ops, 0, sizeof(bucket->entry.ops));
		ht->num_entries++;
	}

	entry = &bucket->entry;

	/* Check if this xid already has an op (overwrite) */
	for (i = 0; i < SLOG_MAX_TUPLE_OPS; i++)
	{
		if (entry->ops[i].in_use &&
			entry->ops[i].xid == op->tuple_op.xid)
		{
			/* Overwrite existing op for same xid */
			memcpy(&entry->ops[i], &op->tuple_op, sizeof(SLogTupleOp));
			return;
		}
	}

	/* Find a free slot */
	for (i = 0; i < SLOG_MAX_TUPLE_OPS; i++)
	{
		if (!entry->ops[i].in_use)
		{
			memcpy(&entry->ops[i], &op->tuple_op, sizeof(SLogTupleOp));
			entry->nops++;
			return;
		}
	}

	/*
	 * No free slot — reclaim the oldest retained committed UPDATE entry.
	 *
	 * Hot rows (e.g., TPC-C district) accumulate one retained UPDATE entry
	 * per committed transaction.  With SLOG_MAX_TUPLE_OPS=32, the ops array
	 * fills after 32 committed updates to the same TID.  Rather than losing
	 * the new operation, we evict the oldest retained entry — it's the least
	 * likely to be needed by any active reader (readers with older snapshots
	 * will use the on-page t_commit_ts which was restored at commit time).
	 *
	 * We identify "oldest retained" as: in_use=true, op_type=SLOG_OP_UPDATE,
	 * commit_hlc != 0 (committed), with the smallest commit_hlc value.
	 */
	{
		int		oldest_idx = -1;
		uint64	oldest_hlc = PG_UINT64_MAX;

		for (i = 0; i < SLOG_MAX_TUPLE_OPS; i++)
		{
			if (!entry->ops[i].in_use)
				continue;
			if (entry->ops[i].op_type != SLOG_OP_UPDATE)
				continue;
			if (entry->ops[i].commit_hlc == 0)
				continue;	/* uncommitted, can't reclaim */
			if (entry->ops[i].commit_hlc < oldest_hlc)
			{
				oldest_hlc = entry->ops[i].commit_hlc;
				oldest_idx = i;
			}
		}

		if (oldest_idx >= 0)
		{
			/*
			 * Snapshot-horizon guard: don't reclaim if an active reader
			 * still needs this before-image.  op->commit_hlc carries the
			 * cached oldest_snapshot_hlc from SLogTupleInsert.
			 */
			uint64	reclaim_horizon = op->commit_hlc;

			if (reclaim_horizon > 0 && oldest_hlc >= reclaim_horizon)
			{
				/*
				 * An active reader's snapshot predates this entry's
				 * commit — cannot reclaim safely.  The new operation
				 * is lost (acceptable: hot-row overflow with active
				 * long-running reader is an extreme edge case).
				 */
				return;
			}

			/* Safe to reclaim — no active reader needs this entry */
			entry->ops[oldest_idx].in_use = false;
			entry->ops[oldest_idx].before_image_dp = InvalidDsaPointer;
			entry->ops[oldest_idx].commit_hlc = 0;
			entry->nops--;

			/* Now insert the new op in the freed slot */
			memcpy(&entry->ops[oldest_idx], &op->tuple_op, sizeof(SLogTupleOp));
			entry->nops++;
			return;
		}
	}

	/* Truly no room (all slots are in-progress, non-UPDATE ops) — lost */
}

/*
 * flat_hash_apply_remove_xid
 *		Remove all ops for a given xid from an entry.
 *		Remove the entry entirely if nops reaches 0.
 */
static void
flat_hash_apply_remove_xid(SLogFlatHash * ht, const SLogFlatOp * op)
{
	SLogFlatBucket *bucket;
	SLogTupleEntry *entry;
	int			i;

	bucket = SLogFlatHashProbe(ht, &op->key);
	if (bucket == NULL)
		return;

	entry = &bucket->entry;

	for (i = 0; i < SLOG_MAX_TUPLE_OPS; i++)
	{
		if (entry->ops[i].in_use &&
			entry->ops[i].xid == op->xid)
		{
			entry->ops[i].in_use = false;
			entry->nops--;
		}
	}

	if (entry->nops == 0)
	{
		bucket->hash_val = SLOG_FLAT_TOMBSTONE;
		ht->num_entries--;
		ht->num_tombstones++;
	}
}

/*
 * flat_hash_apply_remove_entry
 *		Remove an entire entry (tombstone it).
 */
static void
flat_hash_apply_remove_entry(SLogFlatHash * ht, const SLogFlatOp * op)
{
	SLogFlatBucket *bucket;

	bucket = SLogFlatHashProbe(ht, &op->key);
	if (bucket == NULL)
		return;

	bucket->hash_val = SLOG_FLAT_TOMBSTONE;
	ht->num_entries--;
	ht->num_tombstones++;
}

/*
 * flat_hash_apply_mark_aborted
 *		Mark all ops for a given xid as SLOG_OP_ABORTED.
 */
static void
flat_hash_apply_mark_aborted(SLogFlatHash * ht, const SLogFlatOp * op)
{
	SLogFlatBucket *bucket;
	SLogTupleEntry *entry;
	int			i;

	bucket = SLogFlatHashProbe(ht, &op->key);
	if (bucket == NULL)
		return;

	entry = &bucket->entry;

	for (i = 0; i < SLOG_MAX_TUPLE_OPS; i++)
	{
		if (entry->ops[i].in_use &&
			entry->ops[i].xid == op->xid)
		{
			entry->ops[i].op_type = SLOG_OP_ABORTED;
			entry->ops[i].before_image_dp = InvalidDsaPointer;
			entry->ops[i].commit_hlc = 0;
		}
	}
}

/*
 * flat_hash_apply_update_op
 *		Update a specific op slot (e.g., attach before_image_dp or
 *		change subxid).
 */
static void
flat_hash_apply_update_op(SLogFlatHash * ht, const SLogFlatOp * op)
{
	SLogFlatBucket *bucket;
	SLogTupleEntry *entry;
	int			i;

	bucket = SLogFlatHashProbe(ht, &op->key);
	if (bucket == NULL)
		return;

	entry = &bucket->entry;

	for (i = 0; i < SLOG_MAX_TUPLE_OPS; i++)
	{
		if (entry->ops[i].in_use &&
			entry->ops[i].xid == op->xid)
		{
			/*
			 * Apply selective updates from the op.  We use the tuple_op
			 * fields as the source of truth for what to update.
			 */
			if (DsaPointerIsValid(op->before_image_dp) &&
				entry->ops[i].op_type == SLOG_OP_UPDATE)
			{
				entry->ops[i].before_image_dp = op->before_image_dp;
			}
			if (op->subxid != InvalidTransactionId)
			{
				/* Re-parent subxid */
				if (entry->ops[i].subxid == op->tuple_op.subxid)
					entry->ops[i].subxid = op->subxid;
			}
			break;
		}
	}
}

/*
 * flat_hash_apply_commit_xid
 *		Handle commit retention: stamp commit_hlc on UPDATE ops with
 *		before-images, remove other ops.
 */
static void
flat_hash_apply_commit_xid(SLogFlatHash * ht, const SLogFlatOp * op)
{
	SLogFlatBucket *bucket;
	SLogTupleEntry *entry;
	int			i;

	bucket = SLogFlatHashProbe(ht, &op->key);
	if (bucket == NULL)
		return;

	entry = &bucket->entry;

	for (i = 0; i < SLOG_MAX_TUPLE_OPS; i++)
	{
		if (!entry->ops[i].in_use)
			continue;
		if (entry->ops[i].xid != op->xid)
			continue;

		if (entry->ops[i].op_type == SLOG_OP_UPDATE &&
			DsaPointerIsValid(entry->ops[i].before_image_dp))
		{
			/* Retain: stamp commit_hlc */
			entry->ops[i].commit_hlc = op->commit_hlc;
		}
		else
		{
			/* Remove */
			entry->ops[i].in_use = false;
			entry->nops--;
		}
	}

	if (entry->nops == 0)
	{
		bucket->hash_val = SLOG_FLAT_TOMBSTONE;
		ht->num_entries--;
		ht->num_tombstones++;
	}
}

/*
 * flat_hash_apply_cleanup_retained
 *		Remove retained entries whose commit_hlc < the threshold.
 *		The threshold is passed via op->commit_hlc.
 */
static void
flat_hash_apply_cleanup_retained(SLogFlatHash * ht, const SLogFlatOp * op)
{
	SLogFlatBucket *bucket;
	SLogTupleEntry *entry;
	int			i;

	bucket = SLogFlatHashProbe(ht, &op->key);
	if (bucket == NULL)
		return;

	entry = &bucket->entry;

	for (i = 0; i < SLOG_MAX_TUPLE_OPS; i++)
	{
		if (!entry->ops[i].in_use)
			continue;
		if (entry->ops[i].op_type != SLOG_OP_UPDATE)
			continue;
		if (entry->ops[i].commit_hlc == 0)
			continue;
		if (entry->ops[i].commit_hlc >= op->commit_hlc)
			continue;

		/* Expired retained entry */
		entry->ops[i].in_use = false;
		entry->ops[i].before_image_dp = InvalidDsaPointer;
		entry->ops[i].commit_hlc = 0;
		entry->nops--;
	}

	if (entry->nops == 0)
	{
		bucket->hash_val = SLOG_FLAT_TOMBSTONE;
		ht->num_entries--;
		ht->num_tombstones++;
	}
}

/*
 * flat_hash_apply_create_aborted
 *		Create a new entry with an ABORTED op (for local-only INSERT abort).
 */
static void
flat_hash_apply_create_aborted(SLogFlatHash * ht, const SLogFlatOp * op)
{
	uint32		h;
	SLogFlatBucket *bucket;
	SLogTupleEntry *entry;
	int			i;
	bool		existing;

	h = SLogFlatHashComputeHash(&op->key);
	bucket = SLogFlatHashProbeForInsert(ht, &op->key, h);

	if (bucket == NULL)
		return;					/* table full */

	existing = (bucket->hash_val != SLOG_FLAT_EMPTY &&
				bucket->hash_val != SLOG_FLAT_TOMBSTONE);

	if (!existing)
	{
		if (bucket->hash_val == SLOG_FLAT_TOMBSTONE)
			ht->num_tombstones--;

		bucket->hash_val = h;
		memcpy(&bucket->key, &op->key, sizeof(SLogTupleKey));
		bucket->entry.nops = 0;
		memset(&bucket->entry.key, 0, sizeof(SLogTupleKey));
		memcpy(&bucket->entry.key, &op->key, sizeof(SLogTupleKey));
		memset(bucket->entry.ops, 0, sizeof(bucket->entry.ops));
		ht->num_entries++;
	}

	entry = &bucket->entry;

	/* Find a free slot for the ABORTED entry */
	for (i = 0; i < SLOG_MAX_TUPLE_OPS; i++)
	{
		if (!entry->ops[i].in_use)
		{
			entry->ops[i].xid = op->xid;
			entry->ops[i].subxid = op->subxid;
			entry->ops[i].op_type = SLOG_OP_ABORTED;
			entry->ops[i].in_use = true;
			entry->ops[i].commit_ts = 0;
			entry->ops[i].spec_token = 0;
			entry->ops[i].cid = InvalidCommandId;
			entry->ops[i].commit_hlc = 0;
			entry->ops[i].before_image_dp = InvalidDsaPointer;
			entry->nops++;
			return;
		}
	}
	/* No free slot — operation lost */
}

/*
 * SLogFlatHashApply
 *		LRLock apply callback. Dispatches to operation-specific handlers.
 */
void
SLogFlatHashApply(void *data, const void *operation, Size op_size)
{
	SLogFlatHash *ht = (SLogFlatHash *) data;
	const		SLogFlatOp *op = (const SLogFlatOp *) operation;

	Assert(op_size == sizeof(SLogFlatOp));

	switch (op->kind)
	{
		case SLOG_FLAT_OP_INSERT:
			flat_hash_apply_insert(ht, op);
			break;
		case SLOG_FLAT_OP_REMOVE_XID:
			flat_hash_apply_remove_xid(ht, op);
			break;
		case SLOG_FLAT_OP_REMOVE_ENTRY:
			flat_hash_apply_remove_entry(ht, op);
			break;
		case SLOG_FLAT_OP_MARK_ABORTED:
			flat_hash_apply_mark_aborted(ht, op);
			break;
		case SLOG_FLAT_OP_UPDATE_OP:
			flat_hash_apply_update_op(ht, op);
			break;
		case SLOG_FLAT_OP_COMMIT_XID:
			flat_hash_apply_commit_xid(ht, op);
			break;
		case SLOG_FLAT_OP_CLEANUP_RETAINED:
			flat_hash_apply_cleanup_retained(ht, op);
			break;
		case SLOG_FLAT_OP_CREATE_ABORTED:
			flat_hash_apply_create_aborted(ht, op);
			break;
	}
}

/*
 * SLogFlatHashSync
 *		LRLock sync callback. Full memcpy of the data structure.
 */
void
SLogFlatHashSync(void *dst, const void *src, Size data_size)
{
	memcpy(dst, src, data_size);
}

/* ----------------------------------------------------------------
 * Scan API
 * ----------------------------------------------------------------
 */

/*
 * SLogFlatHashScanInit
 *		Initialize a sequential scan over the flat hash.
 */
void
SLogFlatHashScanInit(SLogFlatHashScanState * state)
{
	state->current_index = 0;
}

/*
 * SLogFlatHashScanNext
 *		Return the next occupied bucket, or NULL when the scan is complete.
 *
 * Skips EMPTY and TOMBSTONE slots.  The returned pointer is valid only
 * within the current LRLock read/write critical section.
 */
const		SLogFlatBucket *
SLogFlatHashScanNext(const SLogFlatHash * ht, SLogFlatHashScanState * state)
{
	while (state->current_index < ht->capacity)
	{
		const		SLogFlatBucket *bucket = &ht->buckets[state->current_index];

		state->current_index++;

		if (bucket->hash_val != SLOG_FLAT_EMPTY &&
			bucket->hash_val != SLOG_FLAT_TOMBSTONE)
		{
			return bucket;
		}
	}

	return NULL;
}
