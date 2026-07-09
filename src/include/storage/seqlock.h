/*-------------------------------------------------------------------------
 *
 * seqlock.h
 *	  Sequence lock: a low-overhead reader/writer primitive for
 *	  read-mostly, rarely-written shared data.
 *
 * A seqlock protects data with a single sequence counter (even = stable,
 * odd = write in progress).  Readers take no lock and pay no atomic
 * read-modify-write and no StoreLoad fence on the common path: they read
 * the counter, read the data into local variables, re-read the counter,
 * and retry if it changed.  A writer bumps the counter to odd, mutates the
 * data in place, and bumps it back to even.
 *
 * Trade-offs vs LWLock and vs the left-right lock:
 *   - Reads acquire no lock: two relaxed counter loads plus read barriers,
 *     no CAS, no spinlock, no StoreLoad fence.  This is cheaper than a
 *     left-right read (which requires a per-read SeqCst fence) and far
 *     cheaper than an LWLock shared acquire (a CAS on a shared counter).
 *   - Readers may RETRY if a writer intervenes, so a reader is not
 *     wait-free and can be starved by a continuous stream of writers.
 *     Seqlocks are therefore suited to read-mostly data with short,
 *     infrequent writes.
 *   - The protected data has a SINGLE copy (unlike left-right's two), so a
 *     reader must copy the fields it needs into locals inside the read
 *     section and only use them after a clean re-check; it must not retain
 *     pointers into the protected data across the re-check, because the
 *     writer mutates that data in place.
 *   - Writer serialization is the CALLER's responsibility (e.g. an LWLock
 *     or spinlock already held for other reasons).  The seqlock itself is
 *     only the reader/writer coordination counter; it provides no mutual
 *     exclusion between writers.
 *
 * Why a new primitive rather than LWLock or bare atomics:
 *   - The motivating consumer is a read-mostly, heavily-concurrent shared
 *     hash whose readers vastly outnumber writers and read several words
 *     (a struct) that must be observed as one consistent snapshot.  An
 *     LWLock shared-acquire is a CAS (read-modify-write) on a shared
 *     counter on every read; under many readers that shared cache line
 *     ping-pongs between cores and becomes the bottleneck, even though no
 *     reader mutates anything.  The seqlock read path issues only two
 *     relaxed loads of the counter plus read barriers -- no atomic RMW, no
 *     contended cache line -- so read throughput scales with cores.
 *   - Plain atomics alone cannot give a multi-word consistent snapshot: an
 *     atomic per field lets a reader observe a torn mix of one writer's
 *     old and new values across fields.  The seqlock's begin/retry frames
 *     the whole struct read as a single versioned transaction, so a reader
 *     either sees one writer's complete update or retries.  That is the
 *     property neither an LWLock (correct but not scalable for pure reads)
 *     nor per-field atomics (scalable but not snapshot-consistent) provide
 *     on their own.
 *
 * Memory ordering: the writer's odd store is followed by a write barrier
 * before the data mutation, and a write barrier precedes the even store;
 * the reader issues a read barrier after the initial (even) counter load
 * and before the re-read.  No StoreLoad (full/SeqCst) fence is required
 * because a reader never announces itself to the writer.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/seqlock.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SEQLOCK_H
#define SEQLOCK_H

#ifdef FRONTEND
#error "seqlock.h may not be included from frontend code"
#endif

#include "port/atomics.h"
#include "storage/s_lock.h"

/*
 * A sequence lock.  Embed one alongside the data it protects.  The counter
 * is even when the data is stable and odd while a writer is mutating it.
 */
typedef struct SeqLock
{
	pg_atomic_uint32 seq;
} SeqLock;

/*
 * Initialize a seqlock to the stable (even) state.  Call once before any
 * concurrent access.
 */
static inline void
SeqLockInit(SeqLock *lock)
{
	pg_atomic_init_u32(&lock->seq, 0);
}

/* ----------------------------------------------------------------
 *		Writer API
 *
 * The caller MUST serialize writers by other means (an LWLock or spinlock
 * held across the whole write).  SeqLockWriteBegin/End only publish the
 * write window to readers; they do not exclude concurrent writers.
 * ----------------------------------------------------------------
 */

/*
 * Begin a write.  Bumps the counter to odd so concurrent readers retry,
 * then a write barrier so the subsequent data mutation is not reordered
 * before the odd store becomes visible.
 */
static inline void
SeqLockWriteBegin(SeqLock *lock)
{
	uint32		s = pg_atomic_read_u32(&lock->seq);

	Assert((s & 1) == 0);
	pg_atomic_write_u32(&lock->seq, s + 1);
	pg_write_barrier();
}

/*
 * End a write.  A write barrier ensures the data mutation is visible
 * before the counter returns to even (+2 total from the matching Begin).
 */
static inline void
SeqLockWriteEnd(SeqLock *lock)
{
	uint32		s = pg_atomic_read_u32(&lock->seq);

	Assert((s & 1) == 1);
	pg_write_barrier();
	pg_atomic_write_u32(&lock->seq, s + 1);
}

/* ----------------------------------------------------------------
 *		Reader API
 *
 * Usage:
 *		uint32 seq;
 *		do {
 *			seq = SeqLockReadBegin(lock);
 *			... read protected fields into LOCAL variables only ...
 *		} while (!SeqLockReadRetry(lock, seq));
 *		... act on the locals here (a consistent snapshot) ...
 *
 * A reader must NOT act on values read inside the loop, nor retain
 * pointers into the protected data, until SeqLockReadRetry() has confirmed
 * a consistent read (returned true).
 * ----------------------------------------------------------------
 */

/*
 * Begin a read.  Spins while a writer is active (odd counter) and returns
 * the even counter value observed.  A read barrier orders the subsequent
 * data reads after the counter load.
 */
static inline uint32
SeqLockReadBegin(SeqLock *lock)
{
	uint32		s;

	for (;;)
	{
		s = pg_atomic_read_u32(&lock->seq);
		if ((s & 1) == 0)
			break;
		SPIN_DELAY();
	}
	pg_read_barrier();
	return s;
}

/*
 * Finish a read.  Returns true if the read was consistent (no writer
 * intervened since SeqLockReadBegin returned 'startseq'); false if the
 * caller must discard its locals and retry.
 */
static inline bool
SeqLockReadRetry(SeqLock *lock, uint32 startseq)
{
	pg_read_barrier();
	return pg_atomic_read_u32(&lock->seq) == startseq;
}

#endif							/* SEQLOCK_H */
