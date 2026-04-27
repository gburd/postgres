/*-------------------------------------------------------------------------
 *
 * buf.h
 *	  Basic buffer manager data types.
 *
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/buf.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef BUF_H
#define BUF_H

/*
 * Buffer identifiers.
 *
 * Zero is invalid, positive is the index of a shared buffer (1..NBuffers),
 * negative is the index of a local buffer (-1 .. -NLocBuffer).
 */
typedef int Buffer;

#define InvalidBuffer	0

/*
 * BufferIsInvalid
 *		True iff the buffer is invalid.
 */
#define BufferIsInvalid(buffer) ((buffer) == InvalidBuffer)

/*
 * BufferIsLocal
 *		True iff the buffer is local (not visible to other backends).
 */
#define BufferIsLocal(buffer)	((buffer) < 0)

/*
 * BufferAccessIntent -- describes how a buffer access path is being used
 * by the executor (sequential scan, VACUUM, COPY, etc.).  Consulted by
 * the buffer manager to decide between the pool's normal replacement
 * policy, the RECYCLE pool, or a per-backend ring buffer.  See bufmgr.h
 * for the full description.
 *
 * If adding a new intent, also add a new IOContext so IO statistics
 * using this intent are tracked.
 *
 * Future work: BUF_INTENT_PREFETCH for read_stream lookahead reads,
 * so scan-resistant algorithms (LIRS, ARC, CAR, CLIR) can defer
 * promotion of speculatively-loaded blocks until they are actually
 * consumed by read_stream_next_buffer().  This requires a two-phase
 * signal in read_stream (allocate-as-prefetch, promote-on-demand)
 * and a new BufferPoolRoutine.promote_prefetch callback; not
 * implemented today because no caller distinguishes lookahead from
 * demand at the buffer-allocation level.
 */
typedef enum BufferAccessIntent
{
	BUF_INTENT_NORMAL = 0,		/* random access; pool's algorithm decides */
	BUF_INTENT_BULKREAD,		/* sequential scan (hint bit updates ok) */
	BUF_INTENT_BULKWRITE,		/* large multi-block write (e.g. COPY IN) */
	BUF_INTENT_VACUUM,			/* VACUUM */
}			BufferAccessIntent;

#endif							/* BUF_H */
