/*-------------------------------------------------------------------------
 *
 * lrlock.h
 *	  Left-right lock: a concurrency primitive providing wait-free reads.
 *
 * A left-right lock maintains two copies of a data structure.  Readers
 * access the "read copy" without acquiring any lock (wait-free path via
 * atomic epoch counter increment + pointer load).  A single writer
 * modifies the "write copy" and periodically publishes changes by
 * swapping the read/write pointers and replaying queued operations to
 * the stale copy.
 *
 * Trade-offs vs LWLock:
 *   - Reads are wait-free (no atomic CAS, no spinlock)
 *   - 2x memory for the protected data structure
 *   - Writes are slower (applied twice, writer must wait for readers to depart)
 *   - Single writer only (external serialization required for multiple writers)
 *   - Operations must be deterministic and repeatable
 *
 * The algorithm is based on the left-right concurrency primitive described
 * by Pedro Ramalhete and Andreia Correia, and as implemented in Jon
 * Gjengset's Rust left-right crate.
 *
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/lrlock.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef LRLOCK_H
#define LRLOCK_H

#ifdef FRONTEND
#error "lrlock.h may not be included from frontend code"
#endif

#include "port/atomics.h"

/*
 * Opaque handle for a left-right lock instance.
 *
 * The full structure is defined in lrlock.c; callers interact only via
 * the functions declared below.
 */
typedef struct LRLock LRLock;

/*
 * Callback to apply a single operation to one copy of the data structure.
 *
 * 'data' points to the copy being mutated.
 * 'operation' points to a caller-defined operation descriptor.
 * 'op_size' is the size of that descriptor in bytes.
 *
 * This callback must be deterministic: applying the same operation to
 * two identical copies must produce identical results.
 */
typedef void (*LRLockApplyFn) (void *data, const void *operation, Size op_size);

/*
 * Callback to fully synchronize a destination copy from a source copy.
 *
 * Called during the first publish to bring the write copy in sync with
 * the read copy.  Must produce a byte-for-byte identical copy of the
 * data structure.
 */
typedef void (*LRLockSyncFn) (void *dst, const void *src, Size data_size);

/*
 * Create a new left-right lock in shared memory.
 *
 * 'data_size' is the size of each copy of the protected data structure.
 * 'apply_fn' is called to apply each operation to a copy.
 * 'sync_fn' is called to synchronize the write copy from the read copy.
 * 'name' is used for diagnostics (wait event reporting, error messages).
 *
 * Both data copies are zeroed initially.  The caller should initialize
 * the data structure (via the writer API) after creation.
 *
 * Returns a pointer to the new lock, allocated in shared memory.
 */
extern LRLock * LRLockCreate(Size data_size, LRLockApplyFn apply_fn,
							 LRLockSyncFn sync_fn, const char *name);

/*
 * Initialize an LRLock that has already been allocated in shared memory.
 *
 * This is useful when the LRLock is embedded in a larger shared memory
 * structure and was allocated via ShmemRequestStruct.  The data arrays
 * and epoch counters are allocated separately from shared memory.
 */
extern void LRLockInit(LRLock * lock, Size data_size, LRLockApplyFn apply_fn,
					   LRLockSyncFn sync_fn, int max_backends,
					   const char *name);

/*
 * Initialize an LRLock from a contiguous pre-allocated memory block.
 * All sub-structures are carved from 'block' with no ShmemAlloc calls.
 * The block must be at least LRLockShmemSize() bytes.  Returns a pointer
 * to the LRLock at the start of the block.
 */
extern LRLock * LRLockInitInPlace(void *block, Size data_size,
								  LRLockApplyFn apply_fn,
								  LRLockSyncFn sync_fn, int max_backends,
								  Size oplog_capacity, const char *name);

/*
 * Compute the shared memory size needed for an LRLock with the given
 * parameters.  This includes the LRLock struct itself, both data copies,
 * the epoch array, and the operation log.
 */
extern Size LRLockShmemSize(Size data_size, int max_backends,
							Size oplog_capacity);

/* ----------------------------------------------------------------
 *		Reader API - wait-free
 *
 * A reader calls LRLockReadBegin() to obtain a read-only pointer to
 * the current read copy of the data.  The pointer is valid until
 * LRLockReadEnd() is called.  Reads are wait-free: no locks are
 * acquired, only an atomic epoch counter increment.
 *
 * It is an error to modify data through the returned pointer.
 * Readers must not call LRLockReadEnd() without a matching Begin.
 * ----------------------------------------------------------------
 */
extern const void *LRLockReadBegin(LRLock * lock);
extern void LRLockReadEnd(LRLock * lock);

/* ----------------------------------------------------------------
 *		Writer API - single writer at a time
 *
 * A writer calls LRLockWriteBegin() to acquire exclusive write
 * access.  This acquires a spinlock, so only one writer can operate
 * at a time.  The returned pointer points to the write copy and
 * remains valid until LRLockWriteEnd().
 *
 * LRLockApplyOp() queues an operation to be applied to both copies.
 * LRLockPublish() makes all queued operations visible to readers by
 * swapping the read/write pointers and waiting for existing readers
 * to depart.
 *
 * LRLockWriteEnd() releases writer access.  Any operations queued
 * since the last Publish are NOT yet visible to readers.
 * ----------------------------------------------------------------
 */
extern void *LRLockWriteBegin(LRLock * lock);
extern void LRLockPublish(LRLock * lock);

/*
 * Like LRLockPublish(), but unconditionally syncs the stale copy via
 * sync_fn after the pointer swap.  Use this when the write copy was
 * directly modified (not via LRLockApplyOp) and the oplog is empty.
 * After this call both copies are identical; subsequent LRLockApplyOp()
 * calls can safely apply incremental operations.
 */
extern void LRLockPublishFullSync(LRLock * lock);
extern void LRLockWriteEnd(LRLock * lock);

/*
 * Queue an operation to be applied to both data copies.
 *
 * The operation is first applied to the current write copy immediately,
 * then recorded in the operation log.  On the next LRLockPublish(),
 * the operation will be replayed on the (then-stale) copy.
 *
 * The writer must hold write access (between WriteBegin/WriteEnd).
 * 'operation' is copied into the operation log.
 */
extern void LRLockApplyOp(LRLock * lock, const void *operation, Size op_size);

/*
 * Return the current read-side data pointer without epoch coordination.
 * This is only safe during writer access (between WriteBegin/WriteEnd)
 * or during initialization before any readers exist.
 */
extern const void *LRLockGetReadData(LRLock * lock);

/*
 * Return a mutable pointer to the write-side data.
 * Only safe during writer access (between WriteBegin/WriteEnd).
 */
extern void *LRLockGetWriteData(LRLock * lock);

/*
 * Mark a lock as ready after directly initializing both data copies.
 * This sets first_publish_done so the first real publish won't try to
 * sync from the (possibly stale) other copy.  Only call this during
 * initialization before any concurrent access.
 */
extern void LRLockMarkReady(LRLock * lock);

#endif							/* LRLOCK_H */
