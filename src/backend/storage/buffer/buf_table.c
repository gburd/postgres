/*-------------------------------------------------------------------------
 *
 * buf_table.c
 *	  Routines for mapping BufferTags to buffer indexes.
 *
 * This implementation uses a single left-right lock (LRLock) to protect
 * a global open-addressing hash table.  Two copies of the hash table are
 * maintained; readers access the read copy wait-free (no locks, only an
 * atomic epoch counter increment), while writers modify the write copy
 * and periodically publish via pointer swap.
 *
 * The hash table uses open addressing with linear probing.  Entries are
 * fixed-size (BufferTag key + int buf_id).  Empty slots have buf_id = -1,
 * tombstone slots have buf_id = -2.
 *
 * Write operations (insert/delete) go through the LRLock operation log
 * so they are applied to both copies on publish.  The caller must call
 * BufTableWriteBegin/BufTablePublish/BufTableWriteEnd around write
 * operations, just as they previously used LWLockAcquire/LWLockRelease.
 *
 * Read operations (lookup) are self-contained: they internally call
 * LRLockReadBegin/LRLockReadEnd.
 *
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/buffer/buf_table.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "miscadmin.h"
#include "port/pg_bitutils.h"
#include "storage/buf_internals.h"
#include "storage/lrlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "storage/subsystems.h"

/* ----------------------------------------------------------------
 *		Hash table
 *
 * A simple open-addressing hash table with linear probing, designed
 * to be fully contained in a flat memory region (suitable for shared
 * memory and memcpy-based duplication).
 * ----------------------------------------------------------------
 */

/* Sentinel values for buf_id */
#define BUFLR_EMPTY		(-1)
#define BUFLR_TOMBSTONE	(-2)

/* Entry in the hash table */
typedef struct BufLRHashEntry
{
	BufferTag	key;
	int			id;
}			BufLRHashEntry;

/*
 * Hash table.  This is the "data" that the LRLock maintains two copies of.
 */
typedef struct BufLRHashTable
{
	int			capacity;		/* total slots (power of 2) */
	int			count;			/* number of live entries */
	int			mask;			/* capacity - 1, for fast modulo */
	BufLRHashEntry entries[FLEXIBLE_ARRAY_MEMBER];
}			BufLRHashTable;

/* Operation types for the LRLock operation log */
typedef enum BufLROpType
{
	BUFLR_OP_INSERT,
	BUFLR_OP_DELETE,
}			BufLROpType;

/* Operation descriptor logged by LRLockApplyOp */
typedef struct BufLROp
{
	BufLROpType type;
	BufferTag	tag;
	uint32		hashcode;
	int			buf_id;			/* for INSERT only */
}			BufLROp;

/* Single LRLock protecting the entire buffer mapping hash table */
static LRLock * BufMappingLock;

/* Raw shared memory block for BufMappingLock */
static void *BufMappingLockShmem;

/* Saved hash function from the initial dynahash (used for BufTableHashCode) */
static HTAB *SharedBufHashForHashCode;

static void BufTableShmemRequest(void *arg);
static void BufTableShmemInit(void *arg);

const ShmemCallbacks BufTableShmemCallbacks = {
	.request_fn = BufTableShmemRequest,
	.init_fn = BufTableShmemInit,
};


/* ----------------------------------------------------------------
 *		Hash table helpers
 * ----------------------------------------------------------------
 */

/*
 * Compute the table slot for a given hash code.
 */
static inline int
buflr_slot(const BufLRHashTable * ht, uint32 hashcode)
{
	return (int) (hashcode & (uint32) ht->mask);
}

/*
 * Look up a tag in the hash table.
 * Returns the buf_id if found, or -1 if not found.
 */
static int
buflr_lookup(const BufLRHashTable * ht, const BufferTag *tagPtr, uint32 hashcode)
{
	int			slot = buflr_slot(ht, hashcode);
	int			i;

	for (i = 0; i < ht->capacity; i++)
	{
		const		BufLRHashEntry *entry = &ht->entries[slot];

		if (entry->id == BUFLR_EMPTY)
			return -1;			/* not found */

		if (entry->id != BUFLR_TOMBSTONE &&
			BufferTagsEqual(&entry->key, tagPtr))
			return entry->id;	/* found */

		slot = (slot + 1) & ht->mask;
	}

	return -1;					/* table full, not found */
}

/*
 * Insert a tag/buf_id pair into the hash table.
 * Returns -1 on success, or the existing buf_id if tag already exists.
 */
static int
buflr_insert(BufLRHashTable * ht, const BufferTag *tagPtr, uint32 hashcode,
			 int buf_id)
{
	int			slot = buflr_slot(ht, hashcode);
	int			first_tombstone = -1;
	int			i;

	for (i = 0; i < ht->capacity; i++)
	{
		BufLRHashEntry *entry = &ht->entries[slot];

		if (entry->id == BUFLR_EMPTY)
		{
			/* Use tombstone slot if we passed one, otherwise use this slot */
			if (first_tombstone >= 0)
				entry = &ht->entries[first_tombstone];

			entry->key = *tagPtr;
			entry->id = buf_id;
			ht->count++;
			return -1;
		}

		if (entry->id == BUFLR_TOMBSTONE)
		{
			if (first_tombstone < 0)
				first_tombstone = slot;
		}
		else if (BufferTagsEqual(&entry->key, tagPtr))
		{
			return entry->id;	/* already exists */
		}

		slot = (slot + 1) & ht->mask;
	}

	/* Table full -- use tombstone if available */
	if (first_tombstone >= 0)
	{
		BufLRHashEntry *entry = &ht->entries[first_tombstone];

		entry->key = *tagPtr;
		entry->id = buf_id;
		ht->count++;
		return -1;
	}

	elog(ERROR, "buffer hash table full");
	return -1;					/* unreachable */
}

/*
 * Delete a tag from the hash table.
 * The entry is replaced with a tombstone.
 */
static void
buflr_delete(BufLRHashTable * ht, const BufferTag *tagPtr, uint32 hashcode)
{
	int			slot = buflr_slot(ht, hashcode);
	int			i;

	for (i = 0; i < ht->capacity; i++)
	{
		BufLRHashEntry *entry = &ht->entries[slot];

		if (entry->id == BUFLR_EMPTY)
			elog(ERROR, "shared buffer hash table corrupted");

		if (entry->id != BUFLR_TOMBSTONE &&
			BufferTagsEqual(&entry->key, tagPtr))
		{
			entry->id = BUFLR_TOMBSTONE;
			ht->count--;
			return;
		}

		slot = (slot + 1) & ht->mask;
	}

	elog(ERROR, "shared buffer hash table corrupted");
}


/* ----------------------------------------------------------------
 *		LRLock callbacks
 * ----------------------------------------------------------------
 */

/*
 * Apply a single operation (insert or delete) to one copy of the hash table.
 */
static void
buflr_apply_fn(void *data, const void *operation, Size op_size)
{
	BufLRHashTable *ht = (BufLRHashTable *) data;
	const		BufLROp *op = (const BufLROp *) operation;

	Assert(op_size == sizeof(BufLROp));

	switch (op->type)
	{
		case BUFLR_OP_INSERT:
			buflr_insert(ht, &op->tag, op->hashcode, op->buf_id);
			break;
		case BUFLR_OP_DELETE:
			buflr_delete(ht, &op->tag, op->hashcode);
			break;
	}
}

/*
 * Synchronize a destination hash table from a source hash table.
 * This is a byte-for-byte copy.
 */
static void
buflr_sync_fn(void *dst, const void *src, Size data_size)
{
	memcpy(dst, src, data_size);
}


/* ----------------------------------------------------------------
 *		Shared memory initialization
 * ----------------------------------------------------------------
 */

/*
 * Compute the capacity for the buffer mapping hash table.
 * We want a load factor of ~0.5, and capacity must be a power of 2.
 */
static int
buflr_capacity(void)
{
	int			capacity;

	/* Target load factor 0.5: double the number of buffers */
	capacity = (NBuffers + 2) * 2;

	/* Round up to next power of 2 */
	capacity = pg_nextpower2_32(capacity);

	/* Minimum useful size */
	if (capacity < 64)
		capacity = 64;

	return capacity;
}

/*
 * Compute the size of the hash table including the flexible array of entries.
 */
static Size
buflr_table_size(int capacity)
{
	return offsetof(BufLRHashTable, entries) +
		capacity * sizeof(BufLRHashEntry);
}

/*
 * Initialize a freshly allocated hash table.
 */
static void
buflr_init_table(BufLRHashTable * ht, int capacity)
{
	int			i;

	ht->capacity = capacity;
	ht->count = 0;
	ht->mask = capacity - 1;

	for (i = 0; i < capacity; i++)
	{
		ClearBufferTag(&ht->entries[i].key);
		ht->entries[i].id = BUFLR_EMPTY;
	}
}

/*
 * Request shared memory for the buffer lookup table.
 *
 * We also create a minimal dynahash solely for BufTableHashCode(),
 * which needs a consistent hash function.
 */
static void
BufTableShmemRequest(void *arg)
{
	int			size;

	/*
	 * We still need the dynahash for computing hash codes.  Request it with
	 * the same parameters as before.
	 */
	size = NBuffers + NUM_BUFFER_PARTITIONS;

	ShmemRequestHash(.name = "Shared Buffer Lookup Table",
					 .nelems = size,
					 .ptr = &SharedBufHashForHashCode,
					 .hash_info.keysize = sizeof(BufferTag),
					 .hash_info.entrysize = sizeof(BufLRHashEntry),
					 .hash_info.num_partitions = NUM_BUFFER_PARTITIONS,
					 .hash_flags = HASH_ELEM | HASH_BLOBS | HASH_PARTITION | HASH_FIXED_SIZE,
		);

	/*
	 * Request shared memory for the single BufMappingLock instance. The
	 * LRLock manages two copies of BufLRHashTable, plus epoch arrays and
	 * oplog.
	 */
	ShmemRequestStruct(.name = "Buffer Mapping LRLock",
					   .size = LRLockShmemSize(buflr_table_size(buflr_capacity()),
											   MaxBackends + NUM_AUXILIARY_PROCS,
											   sizeof(BufLROp) * 16),
					   .ptr = &BufMappingLockShmem);
}

/*
 * Initialize the BufMappingLock and the two hash table copies.
 */
static void
BufTableShmemInit(void *arg)
{
	int			capacity = buflr_capacity();
	Size		table_size = buflr_table_size(capacity);
	Size		oplog_capacity = sizeof(BufLROp) * 16;

	BufMappingLock = LRLockInitInPlace(BufMappingLockShmem,
									   table_size,
									   buflr_apply_fn,
									   buflr_sync_fn,
									   MaxBackends + NUM_AUXILIARY_PROCS,
									   oplog_capacity,
									   "BufMappingLock");

	/*
	 * Initialize both copies of the hash table directly. After
	 * LRLockInitInPlace both copies are zeroed; we need to set proper
	 * metadata and empty-slot markers.  This is safe because we're in
	 * postmaster startup with no concurrent readers.
	 */
	buflr_init_table((BufLRHashTable *) LRLockGetWriteData(BufMappingLock),
					 capacity);
	buflr_init_table((BufLRHashTable *) (void *) LRLockGetReadData(BufMappingLock),
					 capacity);
	LRLockMarkReady(BufMappingLock);
}


/* ----------------------------------------------------------------
 *		Public API
 * ----------------------------------------------------------------
 */

/*
 * BufTableHashCode
 *		Compute the hash code associated with a BufferTag
 *
 * This must be passed to the lookup/insert/delete routines along with the
 * tag.  We do it like this because the callers need to know the hash code
 * in order to determine which buffer partition to lock, and we don't want
 * to do the hash computation twice.
 */
uint32
BufTableHashCode(BufferTag *tagPtr)
{
	return get_hash_value(SharedBufHashForHashCode, tagPtr);
}

/*
 * BufTableLookup
 *		Lookup the given BufferTag; return buffer ID, or -1 if not found
 *
 * This is the wait-free read path.  Internally acquires and releases the
 * LRLock read epoch.
 */
int
BufTableLookup(BufferTag *tagPtr, uint32 hashcode)
{
	const		BufLRHashTable *ht;
	int			result;

	ht = (const BufLRHashTable *) LRLockReadBegin(BufMappingLock);
	result = buflr_lookup(ht, tagPtr, hashcode);
	LRLockReadEnd(BufMappingLock);

	return result;
}

/*
 * BufTableReadBegin
 *		Begin a read-side critical section on the buffer mapping table.
 *
 * The caller must call BufTableReadEnd() when the read section is over.
 * While the section is active, the writer cannot reclaim the read copy.
 * This is used to hold the read guard across both the lookup and the
 * subsequent PinBuffer, preventing the race where a buffer is invalidated
 * and reused between lookup and pin.
 *
 * BufTableLookup called within an active read section uses the LRLock
 * nested-read optimization (just increments the enters counter).
 */
void
BufTableReadBegin(void)
{
	LRLockReadBegin(BufMappingLock);
}

/*
 * BufTableReadEnd
 *		End a read-side critical section started by BufTableReadBegin().
 */
void
BufTableReadEnd(void)
{
	LRLockReadEnd(BufMappingLock);
}

/*
 * BufTableInsert
 *		Insert a hashtable entry for given tag and buffer ID,
 *		unless an entry already exists for that tag
 *
 * Returns -1 on successful insertion.  If a conflicting entry exists
 * already, returns the buffer ID in that entry.
 *
 * Caller must have called BufTableWriteBegin().
 * The operation is recorded in the LRLock oplog for replay on publish.
 */
int
BufTableInsert(BufferTag *tagPtr, uint32 hashcode, int buf_id)
{
	BufLRHashTable *ht;
	int			existing;

	Assert(buf_id >= 0);
	Assert(tagPtr->blockNum != P_NEW);

	/* First check if entry already exists in the write copy */
	ht = (BufLRHashTable *) LRLockGetWriteData(BufMappingLock);
	existing = buflr_lookup(ht, tagPtr, hashcode);
	if (existing >= 0)
		return existing;

	/* Not found -- record the insert via the operation log */
	{
		BufLROp		op;

		op.type = BUFLR_OP_INSERT;
		op.tag = *tagPtr;
		op.hashcode = hashcode;
		op.buf_id = buf_id;
		LRLockApplyOp(BufMappingLock, &op, sizeof(op));
	}

	return -1;
}

/*
 * BufTableDelete
 *		Delete the hashtable entry for given tag (which must exist)
 *
 * Caller must have called BufTableWriteBegin().
 * The operation is recorded in the LRLock oplog for replay on publish.
 */
void
BufTableDelete(BufferTag *tagPtr, uint32 hashcode)
{
	BufLROp		op;

	op.type = BUFLR_OP_DELETE;
	op.tag = *tagPtr;
	op.hashcode = hashcode;
	op.buf_id = 0;				/* unused for delete */

	LRLockApplyOp(BufMappingLock, &op, sizeof(op));
}

/*
 * BufTableWriteBegin
 *		Acquire exclusive write access to the buffer mapping hash table.
 *		Returns a pointer to the write copy for direct access if needed.
 */
void *
BufTableWriteBegin(void)
{
	return LRLockWriteBegin(BufMappingLock);
}

/*
 * BufTablePublish
 *		Make all pending write operations visible to readers.
 *		Swaps the read/write pointers and replays operations on the
 *		stale copy.
 */
void
BufTablePublish(void)
{
	LRLockPublish(BufMappingLock);
}

/*
 * BufTableWriteEnd
 *		Release exclusive write access to the buffer mapping hash table.
 */
void
BufTableWriteEnd(void)
{
	LRLockWriteEnd(BufMappingLock);
}
