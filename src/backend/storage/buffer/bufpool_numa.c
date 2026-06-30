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
 */
static bool numa_topology_done = false;
static int	numa_nodes = 1;

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
 * BufPoolNumaNodeForBuffer -- which node should buffer local_id (0-based
 *		within a pool of nbuffers) live on?
 *
 * Buffers are split into num_nodes contiguous chunks; buffer i belongs to
 * node (i / chunk).  Contiguous chunks (rather than round-robin) keep a
 * backend's sequential access node-local and let the partitioned clock sweep
 * own a contiguous buffer range per node.
 */
int
BufPoolNumaNodeForBuffer(int local_id, int nbuffers)
{
	int			nodes = BufPoolNumaNodes();
	int			per_node;

	if (nodes <= 1 || nbuffers <= 0)
		return 0;

	per_node = (nbuffers + nodes - 1) / nodes;	/* ceil */
	if (per_node <= 0)
		return 0;
	return Min(local_id / per_node, nodes - 1);
}

/*
 * BufPoolNumaBufferRange -- the [start, end) buffer range owned by node.
 *
 * Inverse of BufPoolNumaNodeForBuffer, used by the partitioned clock sweep to
 * confine a node's hand to its own buffers.
 */
void
BufPoolNumaBufferRange(int node, int nbuffers, int *start, int *end)
{
	int			nodes = BufPoolNumaNodes();
	int			per_node;

	if (nodes <= 1)
	{
		*start = 0;
		*end = nbuffers;
		return;
	}

	per_node = (nbuffers + nodes - 1) / nodes;
	*start = node * per_node;
	*end = Min(*start + per_node, nbuffers);
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
	 * (e.g. under the buffer_pool_numa_nodes developer override on single-node
	 * hardware): binding to a nonexistent node is meaningless.  The logical
	 * partitioning of the clock sweep still runs; only the physical placement
	 * is a no-op, which is correct here.
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
	int			per_node;

	if (nodes <= 1 || nbuffers <= 0)
		return;

	per_node = (nbuffers + nodes - 1) / nodes;

	for (int node = 0; node < nodes; node++)
	{
		int			start = node * per_node;
		int			end = Min(start + per_node, nbuffers);

		if (start >= end)
			break;

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
