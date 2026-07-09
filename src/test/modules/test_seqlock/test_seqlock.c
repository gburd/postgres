/*--------------------------------------------------------------------------
 *
 * test_seqlock.c
 *		Test module for the sequence lock (storage/seqlock.h).
 *
 * These are single-process unit tests of the seqlock protocol contract:
 * the counter transitions, the read/retry handshake, and a simulated
 * writer-interleaving that must force a reader retry.  Cross-process
 * stress is covered by the sLog tuple-tracking tests that exercise the
 * seqlock under real concurrency.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/test/modules/test_seqlock/test_seqlock.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"
#include "storage/seqlock.h"

PG_MODULE_MAGIC;

#define EXPECT_TRUE(expr)	\
	do { \
		if (!(expr)) \
			elog(ERROR, "%s was unexpectedly false in file \"%s\" line %u", \
				 #expr, __FILE__, __LINE__); \
	} while (0)

#define EXPECT_EQ_U32(a, b) \
	do { \
		uint32 a_ = (a), b_ = (b); \
		if (a_ != b_) \
			elog(ERROR, "%u != %u (%s vs %s) in file \"%s\" line %u", \
				 a_, b_, #a, #b, __FILE__, __LINE__); \
	} while (0)

/*
 * A tiny protected payload: two fields a writer keeps in a known invariant
 * (b == a + 1) so a reader can detect a torn read.
 */
typedef struct GuardedData
{
	SeqLock		lock;
	uint64		a;
	uint64		b;
}			GuardedData;

static void
guarded_write(GuardedData *g, uint64 v)
{
	SeqLockWriteBegin(&g->lock);
	g->a = v;
	g->b = v + 1;
	SeqLockWriteEnd(&g->lock);
}

/* Consistent read; loops until it observes a torn-free snapshot. */
static void
guarded_read(GuardedData *g, uint64 *a_out, uint64 *b_out)
{
	uint32		seq;

	do
	{
		seq = SeqLockReadBegin(&g->lock);
		*a_out = g->a;
		*b_out = g->b;
	} while (!SeqLockReadRetry(&g->lock, seq));
}

PG_FUNCTION_INFO_V1(test_seqlock);
Datum
test_seqlock(PG_FUNCTION_ARGS)
{
	GuardedData g;
	uint64		a,
				b;
	uint32		s0,
				s1;

	/* Init: counter even (stable). */
	SeqLockInit(&g.lock);
	g.a = 0;
	g.b = 1;
	EXPECT_TRUE((pg_atomic_read_u32(&g.lock.seq) & 1) == 0);

	/* A begin/end pair advances the counter by 2 and leaves it even. */
	s0 = pg_atomic_read_u32(&g.lock.seq);
	SeqLockWriteBegin(&g.lock);
	EXPECT_TRUE((pg_atomic_read_u32(&g.lock.seq) & 1) == 1);	/* odd mid-write */
	SeqLockWriteEnd(&g.lock);
	s1 = pg_atomic_read_u32(&g.lock.seq);
	EXPECT_EQ_U32(s1, s0 + 2);
	EXPECT_TRUE((s1 & 1) == 0);

	/* A clean read (no interleaving writer) succeeds on the first try. */
	s0 = SeqLockReadBegin(&g.lock);
	a = g.a;
	b = g.b;
	EXPECT_TRUE(SeqLockReadRetry(&g.lock, s0));
	EXPECT_TRUE(b == a + 1);

	/* Values a writer stored are visible to a subsequent consistent read. */
	guarded_write(&g, 42);
	guarded_read(&g, &a, &b);
	EXPECT_EQ_U32((uint32) a, 42);
	EXPECT_EQ_U32((uint32) b, 43);
	EXPECT_TRUE(b == a + 1);

	/*
	 * Simulate a writer interleaving between a reader's begin and retry: the
	 * retry must FAIL (return false), forcing the reader to loop.  This is
	 * the core seqlock guarantee -- a snapshot spanning a write is rejected.
	 */
	s0 = SeqLockReadBegin(&g.lock);
	guarded_write(&g, 100);		/* writer completes a full cycle in the window */
	EXPECT_TRUE(!SeqLockReadRetry(&g.lock, s0));

	/* After the retry-forced loop, the reader gets the invariant-holding pair. */
	guarded_read(&g, &a, &b);
	EXPECT_EQ_U32((uint32) a, 100);
	EXPECT_TRUE(b == a + 1);

	/* Many begin/end cycles keep the counter even and monotonically rising. */
	{
		uint32		before = pg_atomic_read_u32(&g.lock.seq);

		for (int i = 0; i < 1000; i++)
			guarded_write(&g, (uint64) i);
		EXPECT_EQ_U32(pg_atomic_read_u32(&g.lock.seq), before + 2000);
	}

	PG_RETURN_VOID();
}
