/*-------------------------------------------------------------------------
 *
 * bufpool.h
 *	  Pluggable buffer pool replacement strategy interface.
 *
 * This header defines the BufferPoolRoutine vtable that encapsulates
 * a buffer replacement algorithm.  The default implementation is
 * clock-sweep (in freelist.c).  Alternative algorithms (ARC, CAR, etc.)
 * can be loaded as extensions by providing their own BufferPoolRoutine.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/bufpool.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef BUFPOOL_H
#define BUFPOOL_H

#include "nodes/nodes.h"
#include "postgres_ext.h"
#include "storage/buf.h"

/*
 * We need BufferDesc and BufferTag pointer types in the vtable but don't
 * want to pull in all of buf_internals.h.  Forward-declare the struct tags;
 * callers that implement or invoke vtable methods include buf_internals.h
 * separately.
 */
struct buftag;
struct BufferDesc;

/*
 * BufferPoolRoutine -- vtable for a buffer pool replacement algorithm.
 *
 * This follows the same handler-function pattern used by table access
 * methods (tableam.h) and index access methods.  Each algorithm provides
 * a handler function that returns a pointer to a filled-in
 * BufferPoolRoutine struct.
 *
 * The struct is expected to be statically allocated and its lifetime must
 * exceed any pool that references it.
 */
typedef struct BufferPoolRoutine
{
	/*
	 * NodeTag field for structural compatibility with other handler vtables
	 * (e.g. tableam).  Set to T_Invalid because BufferPoolRoutine structs are
	 * statically allocated vtables that are never node-walked, copied, or
	 * serialized by the node infrastructure.
	 */
	NodeTag		type;

	/* ---- Access tracking ---- */

	/*
	 * Notify the algorithm of a cache hit.  buf_id is the buffer index, tag
	 * is the page identity.  Algorithms like ARC use this to promote pages
	 * from T1 to T2.  May be NULL if the algorithm doesn't track hits (e.g.
	 * clock-sweep uses usage_count instead).
	 */
	void		(*on_hit) (void *strategy_data,
						   int buf_id,
						   struct buftag *tag);

	/*
	 * Notify the algorithm of a cache miss before victim selection. tag is
	 * the requested page identity.  Algorithms like ARC use this to check
	 * ghost lists and adjust adaptive parameters. May be NULL if the
	 * algorithm doesn't need miss notifications.
	 */
	void		(*on_miss) (void *strategy_data,
							struct buftag *tag);

	/*
	 * Notify the algorithm that a buffer's old content is being evicted.
	 * buf_id is the buffer index, old_tag is the evicted page identity.
	 * Called from InvalidateVictimBuffer after removing the hash entry.
	 * Algorithms like ARC use this to move tracking entries to ghost lists.
	 * May be NULL if the algorithm doesn't track evictions.
	 */
	void		(*on_evict) (void *strategy_data,
							 int buf_id,
							 struct buftag *old_tag);

	/*
	 * Notify the algorithm that a buffer has been assigned a new page. buf_id
	 * is the buffer index, new_tag is the new page identity. vacuum_hint is
	 * true if the page was loaded by VACUUM. Called from BufferAlloc after
	 * the new tag is set on the victim. Algorithms like ARC use this to
	 * create cache directory entries. May be NULL if the algorithm doesn't
	 * track insertions.
	 */
	void		(*on_new_tag) (void *strategy_data,
							   int buf_id,
							   struct buftag *new_tag,
							   bool vacuum_hint);

	/* ---- Core eviction ---- */

	/*
	 * Select a victim buffer.  The buffer must be pinned (via
	 * TrackNewBufferPin) before returning.  *buf_state is set to the buffer's
	 * state after pinning.  *from_ring is set to true if the buffer came from
	 * a ring buffer strategy.
	 *
	 * strategy may be NULL for the default (no ring) strategy.
	 */
	struct BufferDesc *(*get_victim) (void *strategy_data,
									  BufferAccessStrategy access_strategy,
									  uint64 *buf_state,
									  bool *from_ring);

	/* ---- Trickle writer (background dirty-page flusher) ---- */

	/*
	 * Tell the trickle writer where to start scanning; returns the current
	 * clock/scan position.  complete_passes and num_buf_alloc are optional
	 * output parameters (may be NULL).
	 */
	int			(*sync_start) (void *strategy_data,
							   uint32 *complete_passes,
							   uint32 *num_buf_alloc);

	/*
	 * Register (bgwprocno >= 0) or deregister (bgwprocno == -1) a trickle
	 * writer process for wakeup on next buffer allocation.
	 */
	void		(*notify_trickle) (void *strategy_data,
								   int bgwprocno);

	/*
	 * Optional trickle writer iterator.  When non-NULL, the trickle writer
	 * uses these callbacks instead of a linear scan to find dirty pages worth
	 * flushing.  This lets algorithms direct flush order to their coldest
	 * pages (e.g., LRU tail, HIR entries).
	 *
	 * trickle_iter_begin: start iteration over at most max_candidates flush
	 * targets.  Returns an opaque iterator state. trickle_iter_next: return
	 * the next candidate buffer ID, or -1 when exhausted. trickle_iter_end:
	 * free the iterator state.
	 */
	void	   *(*trickle_iter_begin) (void *strategy_data,
									   int max_candidates);
	int			(*trickle_iter_next) (void *strategy_data,
									  void *iter);
	void		(*trickle_iter_end) (void *strategy_data,
									 void *iter);

	/* ---- Hints from higher layers ---- */

	/*
	 * Hint that VACUUM is starting (vacuum_active = true) or ending
	 * (vacuum_active = false).  Algorithms may adjust page placement to avoid
	 * cache pollution from sequential VACUUM scans. May be NULL if the
	 * algorithm doesn't care.
	 */
	void		(*hint_vacuum) (void *strategy_data,
								bool vacuum_active);

	/*
	 * Consider rejecting a dirty buffer that came from a ring strategy.
	 * Return true to tell the buffer manager to pick another victim. Return
	 * false to accept (write out and reuse) this buffer.
	 */
	bool		(*reject_buffer) (void *strategy_data,
								  BufferAccessStrategy access_strategy,
								  struct BufferDesc *buf,
								  bool from_ring);

	/*
	 * Pre-fetch hint: these pages will be needed soon.  Algorithms may
	 * pre-create tracking entries for better placement decisions. May be NULL
	 * if the algorithm doesn't support prefetch hints.
	 */
	void		(*prefetch_hint) (void *strategy_data,
								  struct buftag *tags,
								  int ntags);

	/* ---- Lifecycle ---- */

	/*
	 * Return the amount of shared memory needed for this algorithm with the
	 * given number of buffers.
	 */
	Size		(*shmem_size) (int nbuffers);

	/*
	 * Initialize algorithm state in the provided shared memory. If init is
	 * true, this is the first-time initialization; if false, it's a re-attach
	 * after postmaster restart.
	 */
	void		(*shmem_init) (void *strategy_data,
							   int nbuffers,
							   int first_buf_id,
							   bool init);

	/*
	 * Shutdown/cleanup callback.  May be NULL if no cleanup is needed.
	 */
	void		(*shutdown) (void *strategy_data);

} BufferPoolRoutine;

/*
 * Active buffer pool routine and its strategy data.
 *
 * For the default pool, ActivePoolRoutine points to clock_pool_routine
 * and ActivePoolData points to the clock-sweep shared state.  These are
 * set during shared memory initialization and currently do not change
 * after startup.
 */
extern PGDLLIMPORT const BufferPoolRoutine *ActivePoolRoutine;
extern PGDLLIMPORT void *ActivePoolData;

/*
 * True if the active DEFAULT-pool algorithm uses any per-access tracking hook
 * (on_hit/on_miss/on_new_tag).  False for the built-in clock-sweep, letting
 * the hot BufferAlloc path skip the hook dispatch with one predicted-false
 * branch.  Maintained alongside ActivePoolRoutine.
 */
extern PGDLLIMPORT bool ActivePoolHasAccessHooks;
extern PGDLLIMPORT bool ActivePoolProbationaryScan;

/* The built-in clock-sweep buffer pool routine */
extern PGDLLIMPORT const BufferPoolRoutine clock_pool_routine;

/* The NUMA-partitioned clock-sweep routine (default pool, multi-node only) */
extern PGDLLIMPORT const BufferPoolRoutine numa_clock_pool_routine;
extern PGDLLIMPORT const BufferPoolRoutine numa_cooling_pool_routine;

/*
 * GUC variable naming the replacement algorithm for the DEFAULT pool.
 *
 * The value is a string handler name (e.g. "clock").  Built-in algorithms
 * and extensions register themselves under a name via
 * RegisterDefaultPoolAlgorithm() during shared_preload_libraries
 * initialization; at shared-memory setup time the DEFAULT pool resolves
 * its routine by looking up this GUC's value in the registry.
 */
extern PGDLLIMPORT char *buffer_pool_algorithm;

/* The built-in KEEP buffer pool routine (never evicts) */
extern PGDLLIMPORT const BufferPoolRoutine keep_pool_routine;

/* The built-in RECYCLE pool routine (one-chance clock for bulk/VACUUM scans) */
extern PGDLLIMPORT const BufferPoolRoutine recycle_pool_routine;

/* ----------------------------------------------------------------
 * DEFAULT pool algorithm registration
 *
 * Extensions can register their BufferPoolRoutine for use with the
 * DEFAULT pool.  Extensions MUST be loaded via shared_preload_libraries
 * so they are available at startup for shared memory sizing, and the
 * name they register under must match buffer_pool_algorithm.
 * ----------------------------------------------------------------
 */

/* Built-in handler name (always registered). */
#define BP_ALGO_CLOCK_NAME	"clock"

/*
 * Register an algorithm for use with the DEFAULT pool.
 *
 * 'name' must be a statically allocated (or otherwise pointer-stable)
 * string that outlives the registry.  'routine' must likewise outlive
 * the registry; static storage is the norm.  Must be called from
 * _PG_init() of a shared_preload_libraries extension.
 */
extern void RegisterDefaultPoolAlgorithm(const char *name,
										 const BufferPoolRoutine *routine);

/* Look up a registered algorithm by name; returns NULL if not found. */
extern const BufferPoolRoutine *LookupDefaultPoolAlgorithm(const char *name);

/* VACUUM hint dispatch for pools */
extern void PoolHintVacuum(Oid pool_oid, bool vacuum_active);

/* GUC variables for trickle writer tuning */
extern PGDLLIMPORT int trickle_flush_after;
extern PGDLLIMPORT int trickle_write_batch_size;

/* GUC variable for RECYCLE pool sizing (0 = disabled) */
extern PGDLLIMPORT int recycle_pool_buffers;

/* GUC: max memory reservable across all buffer pools (blocks); 0 disables */
extern PGDLLIMPORT int max_buffer_pool_memory;

/* GUC: interleave pool memory across NUMA nodes (multi-node systems only) */
/* GUC: distribute buffer pool memory across NUMA nodes (multi-node systems) */
extern PGDLLIMPORT bool buffer_pool_numa;
extern PGDLLIMPORT bool buffer_pool_numa_cooling;

/* GUC (developer): force a logical NUMA node count for testing; 0 = auto */
extern PGDLLIMPORT int buffer_pool_numa_nodes;

#endif							/* BUFPOOL_H */
