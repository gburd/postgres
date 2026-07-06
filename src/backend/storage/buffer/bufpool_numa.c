/*-------------------------------------------------------------------------
 *
 * bufpool_numa.c
 *	  NUMA topology and placement layer for buffer pools.
 *
 * This is the algorithm-agnostic part of NUMA awareness: it answers "how many
 * nodes are there", "which node should buffer i live on", and "bind this
 * memory range to that node".  It sits BELOW the replacement algorithm, so
 * every pool and every algorithm (clock-sweep, ARC, ...) gets correct,
 * content-aware physical placement for free.
 *
 * What it deliberately does NOT do: victim-selection locality.  Making a
 * backend prefer to reuse a buffer on its own node requires the replacement
 * algorithm's victim search to be partitioned per node, which is inherently
 * algorithm-specific and cannot live below the vtable.  That part is provided
 * separately by the NUMA-partitioned clock-sweep routine (numa_clock_*), which
 * consults this layer for the topology.
 *
 * Design follows the content-aware mapping of Tomas Vondra's NUMA series
 * ("Adding basic NUMA awareness"): a buffer and its descriptor are assigned to
 * the SAME node, and buffers are mapped to nodes in large contiguous chunks
 * (not byte-interleaved), so a buffer never straddles a node boundary.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/storage/buffer/bufpool_numa.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_LIBNUMA
#include <numa.h>
#include <numaif.h>
#include <sched.h>
#endif
#include <unistd.h>

#include "miscadmin.h"
#include "port/pg_numa.h"
#include "storage/bufpool.h"
#include "storage/bufpool_internals.h"

/* GUC: distribute buffer pool memory across NUMA nodes */
bool		buffer_pool_numa = false;

/*
 * GUC: use per-core striped clock hands with blind-atomic cooling.  Only
 * meaningful when buffer_pool_numa is on and there is more than one node.
 */
bool		buffer_pool_numa_cooling = false;

/*
 * GUC (developer): force a specific node count for the buffer pool's NUMA
 * partitioning, overriding hardware detection.  0 = auto-detect via libnuma.
 * Lets the NUMA-partitioned clock sweep be exercised (and regression-tested)
 * on single-node hardware, where partitioning would otherwise stay inactive.
 * The forced partitions are logical (memory binding is still a no-op on a real
 * single node), but the per-node clock hands and victim routing run for real.
 */
int			buffer_pool_numa_nodes = 0;

/*
 * Cached topology, computed once per process from libnuma.  num_nodes == 1
 * means "treat as non-NUMA" (single node or NUMA unavailable), in which case
 * every entry point below is a cheap no-op.
 *
 * numa_chunk_buffers is the number of buffers assigned as one contiguous
 * chunk to a single NUMA node -- the SAME concept as Vondra's series
 * (choose_chunk_buffers / numa_chunk_buffers in buf_init.c).  Buffer->node is
 * ONE formula, (local_id / numa_chunk_buffers) % numa_nodes, used by BOTH the
 * placement binding (BufPoolNumaDistribute) and the partitioned clock sweep
 * (BufPoolNumaBufferRange), so placement and eviction can never disagree.
 *
 * When Vondra's series is applied, BufPoolBufferNode() is a thin wrapper that
 * should defer to his BufferGetNode(); until then it computes the same result
 * locally.  See BufPoolBufferNode().
 */
static bool numa_topology_done = false;
static int	numa_nodes = 1;
static int64 numa_chunk_buffers = -1;

/*
 * BufPoolNumaInit -- determine the NUMA topology for this process.
 *
 * Safe to call repeatedly; computes once.  Returns the number of NUMA nodes
 * the buffer pools will distribute across: 1 (non-NUMA / disabled / single
 * node) up to the system node count.
 */
int
BufPoolNumaInit(void)
{
	if (numa_topology_done)
		return numa_nodes;

	numa_nodes = 1;

#ifdef USE_LIBNUMA
	if (buffer_pool_numa && numa_available() >= 0)
	{
		int			maxnode = numa_max_node();

		if (maxnode > 0)
			numa_nodes = maxnode + 1;
	}
#endif

	/*
	 * Developer override: force a logical node count so the partitioned clock
	 * sweep can be tested on single-node hardware.  Only honored when
	 * buffer_pool_numa is on (so it never affects normal operation).
	 */
	if (buffer_pool_numa && buffer_pool_numa_nodes > 1)
		numa_nodes = buffer_pool_numa_nodes;

	numa_topology_done = true;
	return numa_nodes;
}

/*
 * BufPoolNumaNodes -- number of nodes buffers are distributed across.
 *
 * 1 means "not NUMA-distributed"; callers can fast-path on that.
 */
int
BufPoolNumaNodes(void)
{
	if (!numa_topology_done)
		return BufPoolNumaInit();
	return numa_nodes;
}

/*
 * BufPoolNumaActive -- is NUMA distribution actually in effect?
 *
 * True only when enabled AND the hardware has more than one node.
 */
bool
BufPoolNumaActive(void)
{
	return BufPoolNumaNodes() > 1;
}

/*
 * BufPoolNumaSetChunk -- fix the buffers-per-chunk for a pool of nbuffers.
 *
 * Establishes the single buffer->node layout used by both placement and the
 * partitioned clock sweep.  Mirrors Vondra's choose_chunk_buffers(): one
 * contiguous chunk per node, so buffer i is on node
 * (i / numa_chunk_buffers) % numa_nodes.  Called once when the default pool is
 * set up NUMA-aware.  Returns the chunk size in buffers.
 *
 * We use ceil(nbuffers / nodes) -- exactly one chunk per node, the minimal
 * chunking that keeps each node's buffers contiguous (required by the clock
 * sweep's per-node ranges).  Vondra may grow chunks for page-alignment of
 * descriptors; when his series is present we defer to his value via
 * BufPoolBufferNode(), so this local value is only the standalone-arc default.
 */
int64
BufPoolNumaSetChunk(int nbuffers)
{
	int			nodes = BufPoolNumaNodes();

	if (nodes <= 1 || nbuffers <= 0)
		numa_chunk_buffers = -1;
	else
		numa_chunk_buffers = (nbuffers + nodes - 1) / nodes;	/* ceil */

	return numa_chunk_buffers;
}

/*
 * BufPoolBufferNode -- the NUMA node that owns buffer local_id.
 *
 * THE single source of truth for buffer->node.  Both BufPoolNumaDistribute
 * (physical placement) and BufPoolNumaBufferRange (clock-sweep partitioning)
 * derive from this, so a buffer is always evicted by the same node's hand it
 * was placed on.
 *
 * When Vondra's NUMA series is applied, this should return BufferGetNode()
 * (his chunk math); until then it uses our numa_chunk_buffers, computed to the
 * same (i / chunk) % nodes shape.
 */
int
BufPoolBufferNode(int local_id, int nbuffers)
{
	int			nodes = BufPoolNumaNodes();

	if (nodes <= 1 || nbuffers <= 0)
		return 0;

	/*
	 * Deferral hook for Vondra's NUMA series: when his 0001 is applied it
	 * defines BufferGetNode() (buffer -> node using his choose_chunk_buffers
	 * layout).  At that point this whole body should become: return
	 * BufferGetNode(local_id); guarded by #ifdef so arc still builds
	 * standalone.  We keep our own chunk math below as the standalone
	 * default; both use the identical (i / chunk) % nodes shape, so switching
	 * does not change semantics, only the source of the chunk size.
	 */
#ifdef PG_HAVE_BUFFER_GET_NODE	/* defined once Vondra 0001 is in-tree */
	return BufferGetNode(local_id);
#else
	/* Lazily fix the chunk size if a caller reached us before setup. */
	if (numa_chunk_buffers <= 0)
		BufPoolNumaSetChunk(nbuffers);
	if (numa_chunk_buffers <= 0)
		return 0;

	return (int) ((local_id / numa_chunk_buffers) % nodes);
#endif
}

/*
 * BufPoolNumaBufferRange -- the [start, end) buffer range owned by node.
 *
 * Derived from the shared numa_chunk_buffers layout (BufPoolBufferNode), so
 * the clock sweep confines a node's hand to exactly the buffers that were
 * placed on that node.  With one chunk per node this is a single contiguous
 * run; if a future chunk scheme interleaves multiple chunks per node this
 * returns the node's first chunk (the sweep then relies on cross-node
 * fallback for the rest -- still correct, just less locality).
 */
void
BufPoolNumaBufferRange(int node, int nbuffers, int *start, int *end)
{
	int			nodes = BufPoolNumaNodes();

	if (nodes <= 1)
	{
		*start = 0;
		*end = nbuffers;
		return;
	}

	if (numa_chunk_buffers <= 0)
		BufPoolNumaSetChunk(nbuffers);

	*start = (int) (node * numa_chunk_buffers);
	*end = (int) Min((Size) (*start) + numa_chunk_buffers, (Size) nbuffers);
	if (*start > nbuffers)
		*start = nbuffers;
}

/*
 * BufPoolNumaBindRange -- bind a memory range to a specific NUMA node.
 *
 * Content-aware placement primitive: callers bind a buffer-block sub-range and
 * the matching descriptor sub-range to the same node.  Best-effort; on failure
 * the range simply stays wherever the kernel first-touch put it (correctness
 * is unaffected, only locality).  No-op unless NUMA distribution is active.
 */
void
BufPoolNumaBindRange(void *addr, Size size, int node)
{
#ifdef USE_LIBNUMA
	if (!BufPoolNumaActive())
		return;
	if (node < 0 || node >= numa_nodes)
		return;

	/*
	 * Skip the actual bind when the target node does not physically exist
	 * (e.g. under the buffer_pool_numa_nodes developer override on
	 * single-node hardware): binding to a nonexistent node is meaningless.
	 * The logical partitioning of the clock sweep still runs; only the
	 * physical placement is a no-op, which is correct here.
	 */
	if (numa_available() < 0 || node > numa_max_node())
		return;

	/*
	 * numa_tonode_memory sets an MPOL_BIND policy for the range to the single
	 * target node.  Pages migrate/fault there.  Round addr down and size up
	 * to page granularity, since mbind works on whole pages.
	 */
	{
		long		pgsz = sysconf(_SC_PAGESIZE);
		uintptr_t	a = (uintptr_t) addr;
		uintptr_t	aligned;

		if (pgsz <= 0)
			pgsz = 4096;
		aligned = a & ~((uintptr_t) pgsz - 1);
		size += (a - aligned);
		numa_tonode_memory((void *) aligned, size, node);
	}
#else
	(void) addr;
	(void) size;
	(void) node;
#endif
}

/*
 * BufPoolNumaDistribute -- bind a pool's buffer blocks and descriptors to
 *		nodes in contiguous chunks (content-aware, buffer+descriptor co-located).
 *
 * blocks points to nbuffers * BLCKSZ of buffer data; descriptors points to the
 * matching descriptor array (element size desc_elem_size).  Each node's chunk
 * of blocks and the same chunk of descriptors are bound to that node, so a
 * buffer and its header are always on the same node -- the property Vondra's
 * series highlights and that plain interleaving fails to guarantee.
 *
 * No-op unless NUMA distribution is active.
 */
void
BufPoolNumaDistribute(char *blocks, char *descriptors, Size desc_elem_size,
					  int nbuffers)
{
#ifdef USE_LIBNUMA
	int			nodes = BufPoolNumaNodes();

	if (nodes <= 1 || nbuffers <= 0)
		return;

	/* Establish the shared chunk layout used by placement AND the sweep. */
	BufPoolNumaSetChunk(nbuffers);

	/*
	 * Bind each node's contiguous buffer range (as defined by
	 * BufPoolNumaBufferRange, i.e. the same numa_chunk_buffers layout the
	 * clock sweep uses) and the matching descriptor range to that node.
	 * Placement and eviction therefore agree by construction.
	 */
	for (int node = 0; node < nodes; node++)
	{
		int			start,
					end;

		BufPoolNumaBufferRange(node, nbuffers, &start, &end);
		if (start >= end)
			continue;

		/* Bind this node's slice of buffer blocks. */
		BufPoolNumaBindRange(blocks + (Size) start * BLCKSZ,
							 (Size) (end - start) * BLCKSZ, node);

		/* Bind the SAME slice of descriptors to the same node. */
		BufPoolNumaBindRange(descriptors + (Size) start * desc_elem_size,
							 (Size) (end - start) * desc_elem_size, node);
	}
#else
	(void) blocks;
	(void) descriptors;
	(void) desc_elem_size;
	(void) nbuffers;
#endif
}

/*
 * BufPoolNumaNodeForProc -- the NUMA node the current backend is running on,
 *		or 0 if unknown / not NUMA.  Used by the partitioned clock sweep to
 *		pick the backend's preferred (local) partition to sweep first.
 */
int
BufPoolNumaNodeForProc(void)
{
#ifdef USE_LIBNUMA
	if (BufPoolNumaActive())
	{
		int			cpu = sched_getcpu();

		if (cpu >= 0)
		{
			int			node = numa_node_of_cpu(cpu);

			if (node >= 0 && node < numa_nodes)
				return node;
		}
	}
#endif
	return 0;
}
