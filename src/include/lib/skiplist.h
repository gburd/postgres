/*-------------------------------------------------------------------------
 *
 * skiplist.h
 *	  A lock-free, balanced, skip-list with template macro instantiation.
 *
 * This is a header-only template library for generating type-safe skip-lists.
 * It supports lock-free concurrent operations via C11 atomics, optional
 * splay-style rebalancing, epoch-based reclamation (EBR), MVCC snapshots,
 * and serialization.
 *
 * Usage follows the PostgreSQL template pattern: define configuration macros
 * before including this file. The SKIPLIST_DECL() macro generates all
 * type-specific functions for a given element type.
 *
 * Copyright (c) 2024 Gregory Burd <greg@burd.me>
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/include/lib/skiplist.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SKIPLIST_H
#define SKIPLIST_H

#include "postgres.h"

#include <math.h>
#include <stdarg.h>
#include <string.h>
#include <time.h>

#include "miscadmin.h"

/*
 * Multi-threaded mode uses PostgreSQL pg_atomic_* types (port/atomics.h)
 * rather than C11 <stdatomic.h>.  See the #else branch in the atomics
 * abstraction section below for the full mapping.
 */

/* getpid() portability */
#define _skip_getpid() MyProcPid

/* Compiler attribute portability */
#define _SKIP_PRINTF_ATTR(fmt, args) pg_attribute_printf(fmt, args)

/* typeof portability - PostgreSQL guarantees __typeof__ */
#define _SKIP_TYPEOF(x) __typeof__(x)

/* Static assert - PostgreSQL has StaticAssertStmt */
#define _SKIP_STATIC_ASSERT(cond, msg) StaticAssertDecl(cond, msg)

/* Alignment portability - palloc provides sufficient alignment */
#define _SKIP_ALIGNAS(n)		/* palloc provides maxaligned memory */
#define _skip_aligned_alloc(a, sz) palloc0(sz)
#define _skip_aligned_free(ptr) pfree(ptr)

/* =====================================================================
 * Compile-time feature flags (user-defined before including sl.h)
 * =====================================================================
 *
 * The following preprocessor symbols control optional features and
 * compilation modes.  Define them before #include "sl.h":
 *
 * SKIPLIST_SINGLE_THREADED
 *   When defined, all atomic operations are replaced with plain
 *   loads/stores (no <stdatomic.h> dependency).  This eliminates
 *   synchronization overhead for single-threaded use cases.
 *   Incompatible with SKIPLIST_DECL_EBR.
 *
 *   Usage:
 *     #define SKIPLIST_SINGLE_THREADED
 *     #include "sl.h"
 *
 * DEBUG
 *   When defined together with SKIPLIST_DIAGNOSTIC, enables debug
 *   diagnostic output to stderr via the _skip_diag() internal macro.
 *   Also enables assertion messages via _skip_assert().
 *
 *   Usage:
 *     cc -DDEBUG -DSKIPLIST_DIAGNOSTIC ...
 *
 * SKIPLIST_DIAGNOSTIC
 *   When defined together with DEBUG, enables verbose diagnostic
 *   output and runtime assertions.  Without DEBUG, has no effect.
 *   Typically used during development and testing, not in production.
 *
 *   Usage:
 *     cc -DSKIPLIST_DIAGNOSTIC -DDEBUG ...
 *
 * SKIPLIST_SPLAY_REBALANCE
 *   When defined, enables adaptive splay rebalancing.  Node heights
 *   adapt based on access frequency: popular nodes are promoted to
 *   higher levels and rarely-accessed nodes are demoted.  Without
 *   this flag, the skiplist uses standard randomized heights.
 *
 *   Usage:
 *     cc -DSKIPLIST_SPLAY_REBALANCE ...
 *
 * SNAPSHOTS
 *   Application-level flag indicating snapshot support is desired.
 *   Use SKIPLIST_DECL_SNAPSHOTS to generate the snapshot API.
 *   This flag is not checked by sl.h itself but is conventionally
 *   used in application code to conditionally compile snapshot usage.
 *
 * DOT
 *   Application-level flag indicating DOT visualization is desired.
 *   Use SKIPLIST_DECL_DOT to generate the DOT API.
 *   This flag is not checked by sl.h itself but is conventionally
 *   used in application code to conditionally compile DOT output.
 *
 * See also: SKIPLIST_MAX_HEIGHT, SKIPLIST_SPLAY_INTERVAL,
 *           SKIPLIST_EBR_MAX_THREADS (documented at their definitions below).
 * ===================================================================== */

/* ----- Atomics abstraction ----- */
#ifdef SKIPLIST_SINGLE_THREADED
/*
 * No <stdatomic.h> needed -- provide dummy memory-order constants.
 * Use #ifndef guards to avoid conflicts if system headers define them.
 */
#ifndef memory_order_relaxed
#define memory_order_relaxed 0
#define memory_order_consume 1
#define memory_order_acquire 2
#define memory_order_release 3
#define memory_order_acq_rel 4
#define memory_order_seq_cst 5
#endif
#define _SKIP_ATOMIC(T) T
#define _skip_atomic_load(p, order) (*(p))
#define _skip_atomic_store(p, v, order) ((void)(*(p) = (v)))
#define _skip_atomic_cas_strong(p, exp, des, s, f) (*(exp) == *(p) ? (*(p) = (des), 1) : (*(exp) = *(p), 0))
#define _skip_atomic_cas_weak _skip_atomic_cas_strong
#define _skip_atomic_fetch_add(p, v, order) _skip_st_fetch_add_sz((p), (v))
#define _skip_atomic_fetch_sub(p, v, order) _skip_st_fetch_sub_sz((p), (v))
#define _skip_atomic_exchange(p, v, order) _skip_st_exchange_int((p), (v))
#define _skip_atomic_thread_fence(order) ((void)0)
/* Helpers for single-threaded fetch_add/sub/exchange */
static inline size_t
_skip_st_fetch_add_sz(size_t *p, size_t v)
{
	size_t		o = *p;

	*p += v;
	return o;
}
static inline size_t
_skip_st_fetch_sub_sz(size_t *p, size_t v)
{
	size_t		o = *p;

	*p -= v;
	return o;
}
static inline int
_skip_st_exchange_int(int *p, int v)
{
	int			o = *p;

	*p = v;
	return o;
}
#else
/*
 * Multi-threaded atomics via PostgreSQL pg_atomic_* API.
 *
 * PostgreSQL provides pg_atomic_uint64 with full-barrier CAS, exchange,
 * fetch-add/sub, and read/write (with optional membarrier variants).
 * Since PG lacks atomic pointer types and explicit memory ordering,
 * we use pg_atomic_uint64 for ALL atomic fields -- including pointers
 * (via uintptr_t type-punning) and 32-bit integers (zero-extended).
 *
 * _SKIP_ATOMIC(T) wraps T in an anonymous union that pairs
 * pg_atomic_uint64 with the original type.  The _value member is
 * never accessed at runtime; it exists solely so __typeof__ can
 * recover the original type at load/store sites.
 *
 * Memory ordering: PG atomics are full-barrier (SEQ_CST) for CAS,
 * exchange, and fetch-add/sub.  Reads and writes have relaxed and
 * membarrier variants.  The order parameter is used to select between
 * pg_atomic_read_u64 (relaxed) and pg_atomic_read_membarrier_u64
 * (acquire/seq_cst), and similarly for writes.
 */
#include "port/atomics.h"

/* Dummy memory-order constants for API compatibility. */
#ifndef memory_order_relaxed
#define memory_order_relaxed 0
#define memory_order_consume 1
#define memory_order_acquire 2
#define memory_order_release 3
#define memory_order_acq_rel 4
#define memory_order_seq_cst 5
#endif

/*
 * _SKIP_ATOMIC(T): anonymous union pairing pg_atomic_uint64 with T.
 * Access the _pg member for atomic ops; _value for __typeof__ only.
 */
#define _SKIP_ATOMIC(T) union { pg_atomic_uint64 _pg; T _value; }

/*
 * Load: read pg_atomic_uint64, cast back to original type via __typeof__.
 * Relaxed ordering uses pg_atomic_read_u64; anything stronger uses
 * pg_atomic_read_membarrier_u64 (adds a full memory barrier).
 */
#define _skip_atomic_load(p, order) \
    ((__typeof__((p)->_value))(uintptr_t)( \
        ((order) <= memory_order_relaxed) \
        ? pg_atomic_read_u64(&(p)->_pg) \
        : pg_atomic_read_membarrier_u64(&(p)->_pg)))

/*
 * Store: convert value to uint64 via uintptr_t, write atomically.
 * Release/stronger ordering uses pg_atomic_write_membarrier_u64.
 */
#define _skip_atomic_store(p, v, order) \
    do { \
        if ((order) >= memory_order_release) \
            pg_atomic_write_membarrier_u64(&(p)->_pg, (uint64)(uintptr_t)(v)); \
        else \
            pg_atomic_write_u64(&(p)->_pg, (uint64)(uintptr_t)(v)); \
    } while (0)

/*
 * CAS (strong): pg_atomic_compare_exchange_u64 is always strong/SEQ_CST.
 * On failure, updates *exp to the current value (same as C11 semantics).
 */
#define _skip_atomic_cas_strong(p, exp, des, s, f) \
    ({ \
        uint64 _skip_e = (uint64)(uintptr_t)(*(exp)); \
        bool _skip_ok = pg_atomic_compare_exchange_u64( \
            &(p)->_pg, &_skip_e, (uint64)(uintptr_t)(des)); \
        if (!_skip_ok) \
            *(exp) = (__typeof__((p)->_value))(uintptr_t)_skip_e; \
        _skip_ok; \
    })

/* CAS weak: PG only provides strong CAS, so weak = strong. */
#define _skip_atomic_cas_weak _skip_atomic_cas_strong

/* fetch-add: PG fetch_add is SEQ_CST; cast result back to original type. */
#define _skip_atomic_fetch_add(p, v, order) \
    ((__typeof__((p)->_value))(uintptr_t) \
        pg_atomic_fetch_add_u64(&(p)->_pg, (uint64)(v)))

/* fetch-sub: PG fetch_sub is SEQ_CST; cast result back to original type. */
#define _skip_atomic_fetch_sub(p, v, order) \
    ((__typeof__((p)->_value))(uintptr_t) \
        pg_atomic_fetch_sub_u64(&(p)->_pg, (uint64)(v)))

/* exchange: PG exchange is SEQ_CST; cast result back to original type. */
#define _skip_atomic_exchange(p, v, order) \
    ((__typeof__((p)->_value))(uintptr_t) \
        pg_atomic_exchange_u64(&(p)->_pg, (uint64)(uintptr_t)(v)))

/* fence: pg_memory_barrier() is a full barrier (strongest available). */
#define _skip_atomic_thread_fence(order) pg_memory_barrier()
#endif

/*
 * This file defines a skip-list data structure written in C.  Implemented as
 * using macros this code provides a way to essentially "template" (as in C++)
 * and emit code with types and functions specific to your use case.  You can
 * apply these macros multiple times safely, once for each list type you need.
 *
 * A skip-list is a sorted list with O(log(n)) on average for most operations.
 * It is a probabilistic datastructure, meaning that it does not guarantee
 * O(log(n)), but it has been shown to approximate it over time.  This
 * implementation includes the re-balancing techniques that improve on that
 * approximation using an adaptive technique called "splay-list". It is similar
 * to a standard skip-list, with the key distinction that the height of each
 * element adapts dynamically to its access rate: popular elements increase in
 * height, whereas rarely-accessed elements decrease in height. See below for
 * the link to the research behind this adaptive technique.
 *
 * Conceptually, at a high level, a skip-list is arranged as follows:
 *
 * <head> ----------> [2] --------------------------------------------------> [9] ---------->
 * <head> ----------> [2] ------------------------------------[7] ----------> [9] ---------->
 * <head> ----------> [2] ----------> [4] ------------------> [7] ----------> [9] --> [10] ->
 * <head> --> [1] --> [2] --> [3] --> [4] --> [5] --> [6] --> [7] --> [8] --> [9] --> [10] ->
 *
 * Each node contains at the very least a link to the next element in the list
 * (corresponding to the lowest level in the above diagram), but it can randomly
 * contain more links which skip further down the list (the towers in the above
 * diagram). This allows for the algorithm to move down the list faster than
 * having to visit every element.
 *
 * A skip-list can be thought of as a stack of linked lists. At the very bottom
 * is a linked list with every element, and each layer above corresponds to a
 * linked list containing a random subset of the elements from the layer
 * immediately below it. The probability distribution that determines this
 * random subset can be customized, but typically a layer will contain half the
 * nodes from the layer below.
 *
 * This implementation maintains a doubly-linked list at the bottom layer to
 * support efficient iteration in either direction.  There is also a guard node
 * at the tail rather than simply pointing to NULL.
 *
 * <head> <-> [1] <-> [2] <-> [3] <-> [4] <-> [5] <-> [6] <-> [7] <-> <tail>
 *
 * Safety:
 *
 * The ordered skip-list relies on a well-behaved comparison
 * function. Specifically, given some ordering function f(a, b), it must satisfy
 * the following properties:
 *
 * 1) Be well defined: f(a, b) should always return the same value
 * 2) Be anti-symmetric: f(a, b) == Greater if and only if f(b, a) == Less, and
 *    f(a, b) == Equal == f(b, a).
 * 3) Be transitive: If f(a, b) == Greater and f(b, c) == Greater then f(a, c)
 *    == Greater.
 *
 * Failure to satisfy these properties can result in unexpected behavior at
 * best, and at worst will cause a segfault, null deref, or some other bad
 * behavior.
 *
 * References for this implementation include, but are not limited to:
 *
 *  - Skip lists: a probabilistic alternative to balanced trees
 *    @article{10.1145/78973.78977,
 *      author = {Pugh, William},
 *      title = {Skip lists: a probabilistic alternative to balanced trees},
 *      year = {1990}, issue_date = {June 1990},
 *      publisher = {Association for Computing Machinery},
 *      address = {New York, NY, USA},
 *      volume = {33}, number = {6}, issn = {0001-0782},
 *      url = {https://doi.org/10.1145/78973.78977},
 *      doi = {10.1145/78973.78977},
 *      journal = {Commun. ACM}, month = {jun}, pages = {668-676}, numpages = {9},
 *      keywords = {trees, searching, data structures},
 *      download = {https://www.cl.cam.ac.uk/teaching/2005/Algorithms/skiplists.pdf}
 *    }
 *
 *  - Tutorial: The Ubiquitous Skiplist, its Variants, and Applications in Modern Big Data Systems
 *    @article{Vadrevu2023TutorialTU,
 *      title={Tutorial: The Ubiquitous Skiplist, its Variants, and Applications in Modern Big Data Systems},
 *      author={Venkata Sai Pavan Kumar Vadrevu and Lu Xing and Walid G. Aref},
 *      journal={ArXiv},
 *      year={2023},
 *      volume={abs/2304.09983},
 *      url={https://api.semanticscholar.org/CorpusID:258236678},
 *      download={https://arxiv.org/pdf/2304.09983.pdf}
 *    }
 *
 *  - The Splay-List: A Distribution-Adaptive Concurrent Skip-List
 *    @misc{aksenov2020splaylist,
 *      title={The Splay-List: A Distribution-Adaptive Concurrent Skip-List},
 *      author={Vitaly Aksenov and Dan Alistarh and Alexandra Drozdova and Amirkeivan Mohtashami},
 *      year={2020},
 *      eprint={2008.01009},
 *      archivePrefix={arXiv},
 *      primaryClass={cs.DC},
 *      download={https://arxiv.org/pdf/2008.01009.pdf}
 *    }
 *
 *  - JellyFish: A Fast Skip List with MVCC},
 *    @article{Yeon2020JellyFishAF,
 *      title={JellyFish: A Fast Skip List with MVCC},
 *      author={Jeseong Yeon and Leeju Kim and Youil Han and Hyeon Gyu Lee and Eunji Lee and Bryan Suk Joon Kim},
 *      journal={Proceedings of the 21st International Middleware Conference},
 *      year={2020},
 *      url={https://api.semanticscholar.org/CorpusID:228086012}
 *    }
 */

/* Diagnostics and assertions adapted for PostgreSQL */
#ifdef USE_ASSERT_CHECKING
#define _skip_diag(format, ...) elog(DEBUG5, format, ##__VA_ARGS__)
#define _skip_assert(expr) Assert(expr)
#else
#define _skip_diag(format, ...) ((void)0)
#define _skip_assert(expr) ((void)0)
#endif

/*
 * Skiplist declarations.
 */

/**
 * SKIPLIST_MAX_HEIGHT -- Maximum height (number of levels) for any node.
 *
 * Controls the maximum number of forward-pointer levels a skiplist node
 * can have.  Higher values allow the skiplist to scale to more elements
 * with O(log n) performance, but each node's levels array is allocated
 * to this size.  The default of 64 supports lists of up to ~2^64 elements.
 *
 * Must be <= 64 to avoid stack overflow from path arrays used during
 * locate/insert/remove operations.  Each search/insert/remove allocates
 * ~2 KB on the stack at the default height of 64; reduce in
 * stack-constrained environments (e.g. embedded, deep recursion).
 *
 * Usage:
 *   #define SKIPLIST_MAX_HEIGHT 32   // before including sl.h
 *   #include "sl.h"
 */
#ifndef SKIPLIST_MAX_HEIGHT
#define SKIPLIST_MAX_HEIGHT 64
#endif
_SKIP_STATIC_ASSERT(SKIPLIST_MAX_HEIGHT <= 64, "SKIPLIST_MAX_HEIGHT > 64 risks stack overflow from path arrays");

/**
 * SKIPLIST_SPLAY_INTERVAL -- Number of operations between splay rebalances.
 *
 * When splay rebalancing is enabled (via -DSKIPLIST_SPLAY_REBALANCE), this
 * controls how frequently the adaptive rebalancing logic runs.  Every
 * SKIPLIST_SPLAY_INTERVAL accesses, the rebalance pass examines the search
 * path and promotes/demotes nodes based on their hit counts.
 *
 * Must be a power of two (the implementation uses a bitmask check).
 * Lower values rebalance more often (better adaptation, more overhead).
 * Higher values rebalance less often (less overhead, slower adaptation).
 *
 * Usage:
 *   #define SKIPLIST_SPLAY_INTERVAL 128   // before including sl.h
 *   #include "sl.h"
 */
#ifndef SKIPLIST_SPLAY_INTERVAL
#define SKIPLIST_SPLAY_INTERVAL 64
#endif

/**
 * SKIPLIST_ENTRY(decl) -- Embed skiplist metadata in a user-defined node struct.
 *
 * Every skiplist node must contain a SKIPLIST_ENTRY field, which stores the
 * internal bookkeeping data: the node's height, backward pointer, snapshot
 * era, and the array of forward-pointer/hit-count pairs for each level.
 *
 * The height is a zero-based count of levels: a height of 0 means one (1)
 * level and a height of 4 means five (5) forward pointers in the node [0-4).
 *
 * @param decl  The skiplist type name (must match the name used in SKIPLIST_DECL)
 *
 * Generated structure fields:
 *   sle_era         -- Snapshot era (epoch) when this node was created/modified
 *   sle_height      -- Current number of active levels (atomic)
 *   sle_prev        -- Backward pointer for doubly-linked level-0 list (atomic)
 *   sle_levels[]    -- Array of {next, hits} pairs per level (atomic);
 *                      `next` is the forward pointer, `hits` tracks access
 *                      frequency for splay rebalancing
 *
 * Usage:
 *   struct my_node {
 *       int key;
 *       int value;
 *       SKIPLIST_ENTRY(my_list) entries;  // skiplist metadata
 *   };
 */
#define SKIPLIST_ENTRY(decl)                         \
    struct _skiplist_##decl##_entry {                \
        size_t sle_era;                              \
        _SKIP_ATOMIC(size_t) sle_height;             \
        _SKIP_ATOMIC(struct decl##_node *) sle_prev; \
        struct _skiplist_##decl##_level {            \
            _SKIP_ATOMIC(struct decl##_node *) next; \
            _SKIP_ATOMIC(size_t) hits;               \
        } *sle_levels;                               \
    }

/**
 * SKIPLIST_FOREACH_H2T(decl, prefix, field, list, elm, iter) -- Iterate head-to-tail.
 *
 * Iterates over all nodes in the skiplist from smallest to largest (head to
 * tail) using the level-0 forward pointers.  The sentinel head and tail nodes
 * are excluded; only user-inserted nodes are visited.
 *
 * WARNING: Do not insert or remove nodes during iteration.  For mutation-safe
 * iteration, collect nodes into an array first (see skip_to_array_).
 *
 * @param decl   The skiplist type name
 * @param prefix The function prefix (must match SKIPLIST_DECL)
 * @param field  The SKIPLIST_ENTRY field name in the node struct
 * @param list   Pointer to the skiplist (decl##_t *)
 * @param elm    Loop variable receiving each node (decl##_node_t *)
 * @param iter   Loop counter variable (size_t), 0-based index of current node
 *
 * Usage:
 *   my_node_t *node;
 *   size_t i;
 *   SKIPLIST_FOREACH_H2T(my_list, api_, entries, &slist, node, i) {
 *       printf("node[%zu] key=%d\n", i, node->key);
 *   }
 */
#define SKIPLIST_FOREACH_H2T(decl, prefix, field, list, elm, iter) \
    for (iter = 0, (elm) = (list)->slh_head; ((elm) = _skip_atomic_load(&(elm)->field.sle_levels[0].next, memory_order_acquire)) != (list)->slh_tail; iter++)

/**
 * SKIPLIST_FOREACH_T2H(decl, prefix, field, list, elm, iter) -- Iterate tail-to-head.
 *
 * Iterates over all nodes in the skiplist from largest to smallest (tail to
 * head) using the backward (sle_prev) pointers at level 0.  The sentinel
 * head and tail nodes are excluded.
 *
 * WARNING: Do not insert or remove nodes during iteration.
 *
 * @param decl   The skiplist type name
 * @param prefix The function prefix (must match SKIPLIST_DECL)
 * @param field  The SKIPLIST_ENTRY field name in the node struct
 * @param list   Pointer to the skiplist (decl##_t *)
 * @param elm    Loop variable receiving each node (decl##_node_t *)
 * @param iter   Loop counter variable (size_t), counts down from list length
 *
 * Usage:
 *   my_node_t *node;
 *   size_t i;
 *   SKIPLIST_FOREACH_T2H(my_list, api_, entries, &slist, node, i) {
 *       printf("node key=%d\n", node->key);
 *   }
 */
#define SKIPLIST_FOREACH_T2H(decl, prefix, field, list, elm, iter)                                      \
    for (iter = _skip_atomic_load(&(list)->slh_length, memory_order_relaxed), (elm) = (list)->slh_tail; \
         ((elm) = _skip_atomic_load(&(elm)->field.sle_prev, memory_order_acquire)) != (list)->slh_head; iter--)

/* Iterate over the next pointers in a node from bottom to top (B2T) or top to bottom (T2B). */
#define _SKIP_ALL_ENTRIES_T2B(field, elm) for (size_t lvl = slist->slh_head->field.sle_height - 1; lvl != SIZE_MAX; lvl--)
#define _SKIP_ENTRIES_T2B(field, elm) for (size_t lvl = elm->field.sle_height; lvl != SIZE_MAX; lvl--)
#define _SKIP_ENTRIES_T2B_FROM(field, elm, off) for (size_t lvl = off; lvl != SIZE_MAX; lvl--)
#define _SKIP_IS_LAST_ENTRY_T2B() if (lvl == 0)

#define _SKIP_ALL_ENTRIES_B2T(field, elm) for (size_t lvl = 0; lvl < slist->slh_head->field.sle_height - 1; lvl++)
#define _SKIP_ENTRIES_B2T(field, elm) for (size_t lvl = 0; lvl <= elm->field.sle_height; lvl++)
#define _SKIP_ENTRIES_B2T_FROM(field, elm, off) for (size_t lvl = off; lvl <= elm->field.sle_height; lvl++)
#define _SKIP_IS_LAST_ENTRY_B2T() if (lvl + 1 == elm->field.sle_height)

/* Iterate over the left (v) subtree or right (u) subtree or "CHu" and "CHv". */
#define _SKIP_SUBTREE_CHv(decl, field, list, path, nth) \
    for (decl##_node_t *elm = path[nth].node; elm->field.sle_levels[path[nth].in].next == path[nth].node; elm = elm->field.sle_prev)
#define _SKIP_SUBTREE_CHu(decl, field, list, path, nth) \
    for (decl##_node_t *elm = path[nth].node; elm != path[nth].node->field.sle_levels[0].next; elm = elm->field.sle_levels[0].next)
/* Iterate over a subtree starting at provided path element, u = path.in */
#define _SKIP_SUBTREE_CHux(decl, field, list, node, level) \
    for (decl##_node_t *elm = node->field.sle_levels[level].next->field.sle_prev; elm != node->field.sle_prev; elm = elm->field.sle_prev)

/*
 * Marked pointer support for lock-free operations.
 * Uses the low bit of node pointers as a logical deletion flag.
 * Nodes are heap-allocated with >= 8-byte alignment, so the low bit
 * is always 0 for valid unmarked pointers.
 */
#define _SKIP_IS_MARKED(p) ((uintptr_t)(p) & 1)
#define _SKIP_MARK(p) ((_SKIP_TYPEOF(p))((uintptr_t)(p) | 1))
#define _SKIP_UNMARK(p) ((_SKIP_TYPEOF(p))((uintptr_t)(p) & ~(uintptr_t)1))

/*
 * Defensive path array clearing.
 * _skip_locate_ writes all path entries [0..height] before any are read,
 * so only path[0] requires pre-zeroing (it is the match slot that callers
 * check immediately after locate returns).
 */
#define _SKIP_PATH_CLEAR(path) memset((path), 0, sizeof(*(path)))

/*
 * Splay rebalancing: opt-in via -DSKIPLIST_SPLAY_REBALANCE at compile time.
 * When enabled, node heights adapt based on access frequency. When disabled
 * (default), the skiplist uses standard randomized heights with no rebalancing.
 */
#ifdef SKIPLIST_SPLAY_REBALANCE
#define SKIPLIST_SPLAY_IMPL(decl, slist, len, path)                                                         \
    do {                                                                                                    \
        uint32_t _splay_cnt = _skip_atomic_fetch_add(&(slist)->slh_splay_counter, 1, memory_order_relaxed); \
        if ((_splay_cnt & (SKIPLIST_SPLAY_INTERVAL - 1)) == 0) {                                            \
            _fix_skip_rebalance_##decl(slist, len, path);                                                   \
        }                                                                                                   \
    } while (0)
#else
#define SKIPLIST_SPLAY_IMPL(decl, slist, len, path) \
    do {                                            \
        (void)(slist);                              \
        (void)(len);                                \
        (void)(path);                               \
    } while (0)
#endif

/**
 * SKIPLIST_DECL(decl, prefix, field, compare_entries_blk, free_entry_blk,
 *               update_entry_blk, archive_entry_blk, sizeof_entry_blk)
 * -- Generate the core skiplist implementation for a specific type.
 *
 * This is the primary macro that generates all core skiplist data structures
 * and functions.  It must be invoked once per skiplist type, at file scope.
 * The macro uses C preprocessor token-pasting to emit type-safe code
 * specialized for your node structure, similar to C++ templates.
 *
 * @param decl                The skiplist type name.  Defines the struct name
 *                            for the list (decl) and node (decl##_node).
 * @param prefix              Function name prefix for all generated functions
 *                            (e.g., "api_" generates api_skip_insert_decl).
 * @param field               The name of the SKIPLIST_ENTRY member in the node
 *                            struct (e.g., "entries").
 * @param compare_entries_blk Code block comparing two nodes.  Receives
 *                            (decl##_t *list, decl##_node_t *a,
 *                            decl##_node_t *b, void *aux).  Must return
 *                            <0 if a<b, 0 if a==b, >0 if a>b.
 * @param free_entry_blk      Code block to free user resources in a node.
 *                            Receives (decl##_node_t *node).
 * @param update_entry_blk    Code block to update a node in place.  Receives
 *                            (decl##_node_t *node, void *value).
 *                            Set rc=non-zero on failure.
 * @param archive_entry_blk   Code block to deep-copy user data from src to
 *                            dest for snapshot preservation.  Receives
 *                            (decl##_node_t *dest, const decl##_node_t *src).
 *                            Set rc=non-zero on failure.
 * @param sizeof_entry_blk    Code block returning the serialized size of a
 *                            node's user data.  Receives (decl##_node_t *node).
 *                            Set bytes=size.
 *
 * Usage:
 *   SKIPLIST_DECL(my_list, api_, entries,
 *       { return (a->key < b->key) ? -1 : (a->key > b->key) ? 1 : 0; },
 *       { free(node->data); },
 *       { node->value = *(int *)value; },
 *       { dest->key = src->key; dest->data = strdup(src->data); },
 *       { bytes = sizeof(node->key) + strlen(node->data) + 1; });
 *
 * Generated types:
 *   decl##_t           -- The skiplist type
 *   decl##_node_t      -- The node type
 *   skip_pos_##decl_t  -- Position enum (SKIP_EQ, SKIP_LT, SKIP_LTE, SKIP_GT, SKIP_GTE)
 *
 * Generated functions:
 *   int    prefix##skip_init_##decl(decl##_t *slist)
 *            -- Initialize a skiplist.  Must be called before any other operation.
 *   void   prefix##skip_free_##decl(decl##_t *slist)
 *            -- Free all nodes and the head/tail sentinels.  List is invalid after.
 *   int    prefix##skip_alloc_node_##decl(decl##_node_t **node)
 *            -- Allocate a new node on the heap.  Returns 0 or ENOMEM.
 *   void   prefix##skip_free_node_##decl(decl##_t *slist, decl##_node_t *node)
 *            -- Free a single node (calls free_entry_blk then free).
 *   int    prefix##skip_insert_##decl(decl##_t *slist, decl##_node_t *n)
 *            -- Insert node; rejects duplicates (returns -1).
 *   int    prefix##skip_insert_dup_##decl(decl##_t *slist, decl##_node_t *n)
 *            -- Insert node; allows duplicates.
 *   int    prefix##skip_remove_node_##decl(decl##_t *slist, decl##_node_t *query)
 *            -- Remove the node matching query from the list and free it.
 *   decl##_node_t *prefix##skip_position_eq_##decl(decl##_t *slist, decl##_node_t *query)
 *            -- Find and return the node equal to query, or NULL.
 *   decl##_node_t *prefix##skip_position_gte_##decl(decl##_t *slist, decl##_node_t *query)
 *            -- Return the first node >= query, or NULL.
 *   decl##_node_t *prefix##skip_position_gt_##decl(decl##_t *slist, decl##_node_t *query)
 *            -- Return the first node > query, or NULL.
 *   decl##_node_t *prefix##skip_position_lte_##decl(decl##_t *slist, decl##_node_t *query)
 *            -- Return the last node <= query, or NULL.
 *   decl##_node_t *prefix##skip_position_lt_##decl(decl##_t *slist, decl##_node_t *query)
 *            -- Return the last node < query, or NULL.
 *   int    prefix##skip_update_##decl(decl##_t *slist, decl##_node_t *query, void *value)
 *            -- Find the node matching query and update it via update_entry_blk.
 *   decl##_node_t *prefix##skip_next_node_##decl(decl##_t *slist, decl##_node_t *n)
 *            -- Return the next node after n, or NULL if n is the last.
 *   decl##_node_t *prefix##skip_prev_node_##decl(decl##_t *slist, decl##_node_t *n)
 *            -- Return the previous node before n, or NULL if n is the first.
 *   size_t prefix##skip_length_##decl(decl##_t *slist)
 *            -- Return the number of nodes in the list.
 */
#define SKIPLIST_DECL(decl, prefix, field, compare_entries_blk, free_entry_blk, update_entry_blk, archive_entry_blk, sizeof_entry_blk)                       \
                                                                                                                                                             \
    /* Used when positioning a cursor within a Skiplist. */                                                                                                  \
    typedef enum { SKIP_EQ = 0, SKIP_LTE = -1, SKIP_LT = -2, SKIP_GTE = 1, SKIP_GT = 2 } skip_pos_##decl_t;                                                  \
                                                                                                                                                             \
    /* Skiplist node type */                                                                                                                                 \
    typedef struct decl##_node decl##_node_t;                                                                                                                \
                                                                                                                                                             \
    /* Skiplist type. */                                                                                                                                     \
    typedef struct decl decl##_t;                                                                                                                            \
                                                                                                                                                             \
    /* Skiplist structure */                                                                                                                                 \
    struct decl {                                                                                                                                            \
        _SKIP_ATOMIC(size_t) slh_length;                                                                                                                     \
        void *slh_aux;                                                                                                                                       \
        void *slh_ebr;                                                       /* EBR state (NULL for single-threaded use) */                                  \
        void (*slh_ebr_retire)(void *, struct decl *, struct decl##_node *); /* EBR retire callback */                                                       \
        _SKIP_ATOMIC(uint32_t) slh_prng_state;                                                                                                               \
        _SKIP_ATOMIC(uint32_t) slh_splay_counter;                                                                                                            \
        decl##_node_t *slh_head;                                                                                                                             \
        decl##_node_t *slh_tail;                                                                                                                             \
        struct {                                                                                                                                             \
            void (*free_entry)(decl##_node_t *);                                                                                                             \
            int (*update_entry)(decl##_node_t *, void *);                                                                                                    \
            int (*archive_entry)(decl##_node_t *, const decl##_node_t *);                                                                                    \
            size_t (*sizeof_entry)(decl##_node_t *);                                                                                                         \
            int (*compare_entries)(decl##_t *, decl##_node_t *, decl##_node_t *, void *);                                                                    \
                                                                                                                                                             \
            /* Optional: Snapshots */                                                                                                                        \
            int (*snapshot_preserve_node)(decl##_t * slist, const decl##_node_t *src, decl##_node_t **preserved);                                            \
            void (*snapshot_release)(decl##_t *);                                                                                                            \
        } slh_fns;                                                                                                                                           \
        struct {                                                                                                                                             \
            size_t cur_era;                                                                                                                                  \
            size_t pres_era;                                                                                                                                 \
            decl##_node_t *pres;                                                                                                                             \
        } slh_snap;                                                                                                                                          \
    };                                                                                                                                                       \
                                                                                                                                                             \
    typedef struct _skiplist_path_##decl {                                                                                                                   \
        decl##_node_t *node; /* predecessor node traversed during location */                                                                                \
        decl##_node_t *succ; /* successor node at this level (for lock-free CAS) */                                                                          \
        size_t in;           /* level at which the node was intersected */                                                                                   \
        size_t pu;           /* see "partial sums trick" */                                                                                                  \
    } _skiplist_path_##decl##_t;                                                                                                                             \
                                                                                                                                                             \
    /* Xorshift algorithm for PRNG (lock-free via CAS loop) */                                                                                               \
    static uint32_t _##decl##_xorshift32(_SKIP_ATOMIC(uint32_t) * state)                                                                                     \
    {                                                                                                                                                        \
        uint32_t old_state, new_state;                                                                                                                       \
        do {                                                                                                                                                 \
            old_state = _skip_atomic_load(state, memory_order_relaxed);                                                                                      \
            if (old_state == 0)                                                                                                                              \
                old_state = 123456789;                                                                                                                       \
            new_state = old_state;                                                                                                                           \
            new_state ^= new_state << 13;                                                                                                                    \
            new_state ^= new_state >> 17;                                                                                                                    \
            new_state ^= new_state << 5;                                                                                                                     \
        } while (!_skip_atomic_cas_weak(state, &old_state, new_state, memory_order_relaxed, memory_order_relaxed));                                          \
        return new_state;                                                                                                                                    \
    }                                                                                                                                                        \
                                                                                                                                                             \
    /**                                                                                                                                                      \
     * -- _skip_compare_entries_fn_                                                                                                                          \
     *                                                                                                                                                       \
     * Wraps the `compare_entries_blk` code into `slh_fns.compare_entries`.                                                                                  \
     */                                                                                                                                                      \
    static int _skip_compare_entries_fn_##decl(decl##_t *list, decl##_node_t *a, decl##_node_t *b, void *aux)                                                \
    {                                                                                                                                                        \
        compare_entries_blk;                                                                                                                                 \
    }                                                                                                                                                        \
                                                                                                                                                             \
    /**                                                                                                                                                      \
     * -- _skip_free_entry_fn                                                                                                                                \
     *                                                                                                                                                       \
     * Wraps the `free_entry_blk` code into `slh_fns.free_entry`.                                                                                            \
     */                                                                                                                                                      \
    static void _skip_free_entry_fn_##decl(decl##_node_t *node)                                                                                              \
    {                                                                                                                                                        \
        free_entry_blk;                                                                                                                                      \
    }                                                                                                                                                        \
                                                                                                                                                             \
    /**                                                                                                                                                      \
     * -- _skip_update_entry_fn_                                                                                                                             \
     *                                                                                                                                                       \
     * Wraps the `update_entry_blk` code into `slh_fns.update_entry`.                                                                                        \
     */                                                                                                                                                      \
    static int _skip_update_entry_fn_##decl(decl##_node_t *node, void *value)                                                                                \
    {                                                                                                                                                        \
        int rc = 0;                                                                                                                                          \
        update_entry_blk;                                                                                                                                    \
        return rc;                                                                                                                                           \
    }                                                                                                                                                        \
                                                                                                                                                             \
    /**                                                                                                                                                      \
     * -- _skip_archive_entry_fn_                                                                                                                            \
     *                                                                                                                                                       \
     * Wraps the `archive_entry_blk` code into `slh_fns.archive_entry`.                                                                                      \
     */                                                                                                                                                      \
    static int _skip_archive_entry_fn_##decl(decl##_node_t *dest, const decl##_node_t *src)                                                                  \
    {                                                                                                                                                        \
        int rc = 0;                                                                                                                                          \
        archive_entry_blk;                                                                                                                                   \
        return rc;                                                                                                                                           \
    }                                                                                                                                                        \
                                                                                                                                                             \
    /**                                                                                                                                                      \
     * -- _skip_sizeof_entry_fn_                                                                                                                             \
     *                                                                                                                                                       \
     * Wraps the `sizeof_entry_blk` code into `slh_fns.sizeof_entry`.                                                                                        \
     */                                                                                                                                                      \
    static size_t _skip_sizeof_entry_fn_##decl(decl##_node_t *node)                                                                                          \
    {                                                                                                                                                        \
        size_t bytes = 0;                                                                                                                                    \
        sizeof_entry_blk;                                                                                                                                    \
        return bytes;                                                                                                                                        \
    }                                                                                                                                                        \
                                                                                                                                                             \
    /**                                                                                                                                                      \
     * -- _skip_compare_nodes_                                                                                                                               \
     *                                                                                                                                                       \
     * This function takes four arguments:                                                                                                                   \
     *   - a reference to the Skiplist                                                                                                                       \
     *   - the two nodes to compare, `a` and `b`                                                                                                             \
     *   - `aux` an additional auxiliary argument                                                                                                            \
     * and returns:                                                                                                                                          \
     *   a  < b : return -1                                                                                                                                  \
     *   a == b : return 0                                                                                                                                   \
     *   a  > b : return 1                                                                                                                                   \
     */                                                                                                                                                      \
    static int _skip_compare_nodes_##decl(decl##_t *slist, decl##_node_t *a, decl##_node_t *b, void *aux)                                                    \
    {                                                                                                                                                        \
        if (a == b)                                                                                                                                          \
            return 0;                                                                                                                                        \
        if (a == NULL)                                                                                                                                       \
            return -1;                                                                                                                                       \
        if (b == NULL)                                                                                                                                       \
            return 1;                                                                                                                                        \
        if (a == slist->slh_head || b == slist->slh_tail)                                                                                                    \
            return -1;                                                                                                                                       \
        if (a == slist->slh_tail || b == slist->slh_head)                                                                                                    \
            return 1;                                                                                                                                        \
        return slist->slh_fns.compare_entries(slist, a, b, aux);                                                                                             \
    }                                                                                                                                                        \
                                                                                                                                                             \
    /**                                                                                                                                                      \
     * -- _skip_toss_                                                                                                                                        \
     *                                                                                                                                                       \
     * A "coin toss" function that is critical to the proper operation of the                                                                                \
     * Skiplist.  For example, when `max = 6` this function returns 0 with                                                                                   \
     * probability 0.5, 1 with 0.25, 2 with 0.125, etc. until 6 with 0.5^7.                                                                                  \
     */                                                                                                                                                      \
    static int _skip_toss_##decl(decl##_t *slist, size_t max)                                                                                                \
    {                                                                                                                                                        \
        size_t level = 0;                                                                                                                                    \
        double probability = 0.5;                                                                                                                            \
                                                                                                                                                             \
        double random_value = (double)_##decl##_xorshift32(&slist->slh_prng_state) / UINT32_MAX;                                                             \
        while (random_value < probability && level < max) {                                                                                                  \
            level++;                                                                                                                                         \
            probability *= 0.5;                                                                                                                              \
        }                                                                                                                                                    \
        return level;                                                                                                                                        \
    }                                                                                                                                                        \
                                                                                                                                                             \
    /**                                                                                                                                                      \
     * -- skip_alloc_node_                                                                                                                                   \
     *                                                                                                                                                       \
     * Allocates a new node on the heap and sets default values.                                                                                             \
     */                                                                                                                                                      \
    int prefix##skip_alloc_node_##decl(decl##_node_t **node)                                                                                                 \
    {                                                                                                                                                        \
        decl##_node_t *n;                                                                                                                                    \
        /* Calculate the size of the struct sle within decl##_node_t, multiply                                                                               \
           by array size. (16/24 bytes on 32/64 bit systems) */                                                                                              \
        size_t sle_arr_sz = sizeof(struct _skiplist_##decl##_level) * SKIPLIST_MAX_HEIGHT;                                                                   \
        n = (decl##_node_t *)palloc0(sizeof(decl##_node_t) + sle_arr_sz);                                                                                  \
        if (n == NULL)                                                                                                                                       \
            return ENOMEM;                                                                                                                                   \
        n->field.sle_height = 0;                                                                                                                             \
        n->field.sle_levels = (struct _skiplist_##decl##_level *)((uintptr_t)n + sizeof(decl##_node_t));                                                     \
        *node = n;                                                                                                                                           \
        return 0;                                                                                                                                            \
    }                                                                                                                                                        \
                                                                                                                                                             \
    /**                                                                                                                                                      \
     * -- skip_init_                                                                                                                                         \
     *                                                                                                                                                       \
     * Initializes a Skiplist to the default values, this must be                                                                                            \
     * called before using the list.                                                                                                                         \
     */                                                                                                                                                      \
    int prefix##skip_init_##decl(decl##_t *slist)                                                                                                            \
    {                                                                                                                                                        \
        int rc = 0;                                                                                                                                          \
        size_t i;                                                                                                                                            \
                                                                                                                                                             \
        slist->slh_length = 0;                                                                                                                               \
        slist->slh_ebr = NULL;                                                                                                                               \
        slist->slh_ebr_retire = NULL;                                                                                                                        \
        slist->slh_snap.cur_era = 0;                                                                                                                         \
        slist->slh_snap.pres_era = 0;                                                                                                                        \
        slist->slh_snap.pres = 0;                                                                                                                            \
        slist->slh_fns.free_entry = _skip_free_entry_fn_##decl;                                                                                              \
        slist->slh_fns.update_entry = _skip_update_entry_fn_##decl;                                                                                          \
        slist->slh_fns.archive_entry = _skip_archive_entry_fn_##decl;                                                                                        \
        slist->slh_fns.sizeof_entry = _skip_sizeof_entry_fn_##decl;                                                                                          \
        slist->slh_fns.compare_entries = _skip_compare_entries_fn_##decl;                                                                                    \
        rc = prefix##skip_alloc_node_##decl(&slist->slh_head);                                                                                               \
        if (rc)                                                                                                                                              \
            goto fail;                                                                                                                                       \
        rc = prefix##skip_alloc_node_##decl(&slist->slh_tail);                                                                                               \
        if (rc)                                                                                                                                              \
            goto fail;                                                                                                                                       \
                                                                                                                                                             \
        /* Initial height is 1 (level 0 active). Initialize ALL levels so that                                                                               \
           head->next[i] = tail for every i. This ensures the delete shrinkage                                                                               \
           loop (which scans for head->next[i] != tail) terminates correctly. */                                                                             \
        slist->slh_head->field.sle_height = 1;                                                                                                               \
        for (i = 0; i < SKIPLIST_MAX_HEIGHT; i++)                                                                                                            \
            slist->slh_head->field.sle_levels[i].next = slist->slh_tail;                                                                                     \
        slist->slh_head->field.sle_prev = NULL;                                                                                                              \
                                                                                                                                                             \
        /* Tail: all next pointers are NULL */                                                                                                               \
        slist->slh_tail->field.sle_height = slist->slh_head->field.sle_height;                                                                               \
        for (i = 0; i < SKIPLIST_MAX_HEIGHT; i++)                                                                                                            \
            slist->slh_tail->field.sle_levels[i].next = NULL;                                                                                                \
        slist->slh_tail->field.sle_prev = slist->slh_head;                                                                                                   \
        slist->slh_prng_state = ((uint32_t)time(NULL) ^ ((uint32_t)_skip_getpid() << 16) ^ (uint32_t)(uintptr_t)slist);                                      \
        slist->slh_splay_counter = 0;                                                                                                                        \
    fail:;                                                                                                                                                   \
        return rc;                                                                                                                                           \
    }                                                                                                                                                        \
                                                                                                                                                             \
    /**                                                                                                                                                      \
     * -- skip_free_node_                                                                                                                                    \
     *                                                                                                                                                       \
     * Properly releases heap memory allocated for use as a node.                                                                                            \
     * This function invokes the `free_node_blk` within which you                                                                                            \
     * should release any heap objects or other resources held by                                                                                            \
     * this node in the list.                                                                                                                                \
     */                                                                                                                                                      \
    void prefix##skip_free_node_##decl(decl##_t *slist, decl##_node_t *node)                                                                                 \
    {                                                                                                                                                        \
        slist->slh_fns.free_entry(node);                                                                                                                     \
        pfree(node);                                                                                                                                          \
    }                                                                                                                                                        \
                                                                                                                                                             \
    /**                                                                                                                                                      \
     * -- skip_length_                                                                                                                                       \
     *                                                                                                                                                       \
     * Returns the current length of the list.                                                                                                               \
     */                                                                                                                                                      \
    size_t prefix##skip_length_##decl(decl##_t *slist)                                                                                                       \
    {                                                                                                                                                        \
        return _skip_atomic_load(&slist->slh_length, memory_order_relaxed);                                                                                  \
    }                                                                                                                                                        \
                                                                                                                                                             \
    /**                                                                                                                                                      \
     * -- skip_is_empty_                                                                                                                                     \
     *                                                                                                                                                       \
     * Returns non-zero when the list is empty.                                                                                                              \
     */                                                                                                                                                      \
    int prefix##skip_is_empty_##decl(decl##_t *slist)                                                                                                        \
    {                                                                                                                                                        \
        return _skip_atomic_load(&slist->slh_length, memory_order_relaxed) == 0;                                                                             \
    }                                                                                                                                                        \
                                                                                                                                                             \
    /**                                                                                                                                                      \
     * -- skip_head_                                                                                                                                         \
     *                                                                                                                                                       \
     * Returns the node containing the first (smallest) element in the                                                                                       \
     * list which can be used to traverse the list.                                                                                                          \
     */                                                                                                                                                      \
    decl##_node_t *prefix##skip_head_##decl(decl##_t *slist)                                                                                                 \
    {                                                                                                                                                        \
        decl##_node_t *first = _skip_atomic_load(&slist->slh_head->field.sle_levels[0].next, memory_order_acquire);                                          \
        return first == slist->slh_tail ? NULL : first;                                                                                                      \
    }                                                                                                                                                        \
                                                                                                                                                             \
    /**                                                                                                                                                      \
     * -- skip_tail_                                                                                                                                         \
     *                                                                                                                                                       \
     * Returns the node containing the last (largest) element in the                                                                                         \
     * list which can be used to traverse the list.                                                                                                          \
     */                                                                                                                                                      \
    decl##_node_t *prefix##skip_tail_##decl(decl##_t *slist)                                                                                                 \
    {                                                                                                                                                        \
        if (slist == NULL)                                                                                                                                   \
            return NULL;                                                                                                                                     \
        decl##_node_t *last = _skip_atomic_load(&slist->slh_tail->field.sle_prev, memory_order_acquire);                                                     \
        return last == slist->slh_head ? NULL : last;                                                                                                        \
    }                                                                                                                                                        \
                                                                                                                                                             \
    /**                                                                                                                                                      \
     * -- skip_next_node_                                                                                                                                    \
     *                                                                                                                                                       \
     * A node reference can be thought of as a cursor.  This moves the cursor                                                                                \
     * to the next node in the list or returns NULL if the next is the tail.                                                                                 \
     */                                                                                                                                                      \
    decl##_node_t *prefix##skip_next_node_##decl(decl##_t *slist, decl##_node_t *n)                                                                          \
    {                                                                                                                                                        \
        if (slist == NULL || n == NULL)                                                                                                                      \
            return NULL;                                                                                                                                     \
        decl##_node_t *next = _skip_atomic_load(&n->field.sle_levels[0].next, memory_order_acquire);                                                         \
        if (next == slist->slh_tail)                                                                                                                         \
            return NULL;                                                                                                                                     \
        return next;                                                                                                                                         \
    }                                                                                                                                                        \
                                                                                                                                                             \
    /**                                                                                                                                                      \
     * -- skip_prev_node_                                                                                                                                    \
     *                                                                                                                                                       \
     * A node reference can be thought of as a cursor.  This moves the cursor                                                                                \
     * to the previous node in the list or returns NULL if the previous node                                                                                 \
     * is the head.                                                                                                                                          \
     */                                                                                                                                                      \
    decl##_node_t *prefix##skip_prev_node_##decl(decl##_t *slist, decl##_node_t *n)                                                                          \
    {                                                                                                                                                        \
        if (slist == NULL || n == NULL)                                                                                                                      \
            return NULL;                                                                                                                                     \
        decl##_node_t *prev = _skip_atomic_load(&n->field.sle_prev, memory_order_acquire);                                                                   \
        if (prev == slist->slh_head)                                                                                                                         \
            return NULL;                                                                                                                                     \
        return prev;                                                                                                                                         \
    }                                                                                                                                                        \
                                                                                                                                                             \
    /**                                                                                                                                                      \
     * -- skip_prev_validated_                                                                                                                               \
     *                                                                                                                                                       \
     * Validated backward traversal.  The sle_prev hint may be stale under                                                                                   \
     * concurrency, so this function validates it and falls back to a forward                                                                                \
     * scan from the head if the hint is incorrect.                                                                                                          \
     */                                                                                                                                                      \
    decl##_node_t *prefix##skip_prev_validated_##decl(decl##_t *slist, decl##_node_t *node)                                                                  \
    {                                                                                                                                                        \
        if (slist == NULL || node == NULL)                                                                                                                   \
            return NULL;                                                                                                                                     \
        if (node == slist->slh_head)                                                                                                                         \
            return NULL;                                                                                                                                     \
                                                                                                                                                             \
        /* Read the advisory prev hint. */                                                                                                                   \
        decl##_node_t *prev = _skip_atomic_load(&node->field.sle_prev, memory_order_acquire);                                                                \
                                                                                                                                                             \
        /* If prev is head, that is always valid. */                                                                                                         \
        if (prev == slist->slh_head) {                                                                                                                       \
            decl##_node_t *head_next = _skip_atomic_load(&slist->slh_head->field.sle_levels[0].next, memory_order_acquire);                                  \
            if (!_SKIP_IS_MARKED(head_next) && head_next == node)                                                                                            \
                return NULL;                                                                                                                                 \
        }                                                                                                                                                    \
                                                                                                                                                             \
        /* Validate: prev->next[0] should be node (unmarked). */                                                                                             \
        if (prev != NULL && prev != slist->slh_tail) {                                                                                                       \
            decl##_node_t *prev_next = _skip_atomic_load(&prev->field.sle_levels[0].next, memory_order_acquire);                                             \
            if (!_SKIP_IS_MARKED(prev_next) && prev_next == node) {                                                                                          \
                /* Hint was valid. */                                                                                                                        \
                return (prev == slist->slh_head) ? NULL : prev;                                                                                              \
            }                                                                                                                                                \
        }                                                                                                                                                    \
                                                                                                                                                             \
        /* Hint was stale; scan forward from head at level 0. */                                                                                             \
        decl##_node_t *scan = slist->slh_head;                                                                                                               \
        decl##_node_t *found_prev = slist->slh_head;                                                                                                         \
        for (;;) {                                                                                                                                           \
            decl##_node_t *next = _skip_atomic_load(&scan->field.sle_levels[0].next, memory_order_acquire);                                                  \
            if (_SKIP_IS_MARKED(next)) {                                                                                                                     \
                next = _SKIP_UNMARK(next);                                                                                                                   \
            }                                                                                                                                                \
            if (next == slist->slh_tail || next == NULL)                                                                                                     \
                break;                                                                                                                                       \
            if (next == node) {                                                                                                                              \
                found_prev = scan;                                                                                                                           \
                break;                                                                                                                                       \
            }                                                                                                                                                \
            scan = next;                                                                                                                                     \
        }                                                                                                                                                    \
                                                                                                                                                             \
        /* Best-effort CAS to update the stale hint. */                                                                                                      \
        decl##_node_t *expected_prev = prev;                                                                                                                 \
        _skip_atomic_cas_strong(&node->field.sle_prev, &expected_prev, found_prev, memory_order_release, memory_order_relaxed);                              \
                                                                                                                                                             \
        return (found_prev == slist->slh_head) ? NULL : found_prev;                                                                                          \
    }                                                                                                                                                        \
                                                                                                                                                             \
    /**                                                                                                                                                      \
     * -- skip_release_                                                                                                                                      \
     *                                                                                                                                                       \
     * Release all nodes and their associated heap objects, but not the list                                                                                 \
     * itself. The list is still valid, only empty.                                                                                                          \
     */                                                                                                                                                      \
    void prefix##skip_release_##decl(decl##_t *slist)                                                                                                        \
    {                                                                                                                                                        \
        decl##_node_t *node, *next;                                                                                                                          \
                                                                                                                                                             \
        if (slist == NULL)                                                                                                                                   \
            return;                                                                                                                                          \
        if (prefix##skip_is_empty_##decl(slist))                                                                                                             \
            return;                                                                                                                                          \
        node = _skip_atomic_load(&slist->slh_head->field.sle_levels[0].next, memory_order_acquire);                                                          \
        while (node != slist->slh_tail) {                                                                                                                    \
            next = _skip_atomic_load(&node->field.sle_levels[0].next, memory_order_acquire);                                                                 \
            prefix##skip_free_node_##decl(slist, node);                                                                                                      \
            node = next;                                                                                                                                     \
        }                                                                                                                                                    \
        /* Reset to empty-list state so the list is reusable. */                                                                                             \
        slist->slh_length = 0;                                                                                                                               \
        slist->slh_head->field.sle_height = 1;                                                                                                               \
        for (size_t _i = 0; _i < SKIPLIST_MAX_HEIGHT; _i++) {                                                                                                \
            slist->slh_head->field.sle_levels[_i].next = slist->slh_tail;                                                                                    \
            slist->slh_head->field.sle_levels[_i].hits = 0;                                                                                                  \
            slist->slh_tail->field.sle_levels[_i].next = NULL;                                                                                               \
            slist->slh_tail->field.sle_levels[_i].hits = 0;                                                                                                  \
        }                                                                                                                                                    \
        slist->slh_head->field.sle_prev = NULL;                                                                                                              \
        slist->slh_tail->field.sle_height = 1;                                                                                                               \
        slist->slh_tail->field.sle_prev = slist->slh_head;                                                                                                   \
        if (slist->slh_snap.pres_era > 0)                                                                                                                    \
            slist->slh_snap.cur_era++;                                                                                                                       \
        return;                                                                                                                                              \
    }                                                                                                                                                        \
                                                                                                                                                             \
    /**                                                                                                                                                      \
     * -- skip_to_array_                                                                                                                                     \
     *                                                                                                                                                       \
     * Returns a heap allocated array of nodes in the order they exist.                                                                                      \
     * This isn't maintained by the list, if you add/remove nodes it is                                                                                      \
     * no longer accurate. At [-1] is the length of the array.                                                                                               \
     * NOTE: Caller must deallocate.                                                                                                                         \
     * NOTE: Not safe for concurrent use. The caller must ensure no                                                                                          \
     * concurrent insertions or deletions during this call.                                                                                                  \
     */                                                                                                                                                      \
    decl##_node_t **prefix##skip_to_array_##decl(decl##_t *slist)                                                                                            \
    {                                                                                                                                                        \
        size_t nth, len = prefix##skip_length_##decl(slist);                                                                                                 \
        decl##_node_t *node, **nodes = NULL;                                                                                                                 \
        nodes = (decl##_node_t **)palloc0((len + 1) * (sizeof(decl##_node_t *)));                                                                                  \
        if (nodes != NULL) {                                                                                                                                 \
            nodes[0] = (decl##_node_t *)(uintptr_t)len;                                                                                                      \
            nodes++;                                                                                                                                         \
            SKIPLIST_FOREACH_H2T(decl, prefix, field, slist, node, nth)                                                                                      \
            {                                                                                                                                                \
                if (nth >= len)                                                                                                                              \
                    break;                                                                                                                                   \
                nodes[nth] = node;                                                                                                                           \
            }                                                                                                                                                \
        }                                                                                                                                                    \
        return nodes;                                                                                                                                        \
    }                                                                                                                                                        \
                                                                                                                                                             \
    /**                                                                                                                                                      \
     * -- _skip_adjust_hit_counts_                                                                                                                           \
     *                                                                                                                                                       \
     * When the total hit count (stored at slh_head's top+1 level) exceeds                                                                                   \
     * SIZE_MAX / 2, halve all hit counters across all nodes to prevent                                                                                      \
     * overflow while preserving relative ordering.                                                                                                          \
     */                                                                                                                                                      \
    /* Thread safety: safe under EBR (deferred nodes remain valid);                                                                                          \
       concurrent use without EBR is not supported for deletion. */                                                                                          \
    static void _skip_adjust_hit_counts_##decl(decl##_t *slist)                                                                                              \
    {                                                                                                                                                        \
        size_t total_hits, lvl, nth;                                                                                                                         \
        decl##_node_t *node;                                                                                                                                 \
                                                                                                                                                             \
        if (slist == NULL)                                                                                                                                   \
            return;                                                                                                                                          \
                                                                                                                                                             \
        total_hits = _skip_atomic_load(&slist->slh_head->field.sle_levels[_skip_atomic_load(&slist->slh_head->field.sle_height, memory_order_relaxed)].hits, \
            memory_order_acquire);                                                                                                                           \
        if (total_hits < SIZE_MAX / 2)                                                                                                                       \
            return;                                                                                                                                          \
                                                                                                                                                             \
        /* Halve all hit counters on every node at every level using CAS.                                                                                    \
           CAS ensures correctness under concurrency: if another thread                                                                                      \
           modifies a counter between our load and store, the CAS fails                                                                                      \
           and we retry with the updated value. */                                                                                                           \
        node = slist->slh_head;                                                                                                                              \
        for (lvl = 0; lvl <= _skip_atomic_load(&node->field.sle_height, memory_order_relaxed); lvl++) {                                                      \
            size_t old_val = _skip_atomic_load(&node->field.sle_levels[lvl].hits, memory_order_relaxed);                                                     \
            size_t new_val;                                                                                                                                  \
            do {                                                                                                                                             \
                new_val = (old_val + 1) / 2;                                                                                                                 \
            } while (!_skip_atomic_cas_weak(&node->field.sle_levels[lvl].hits, &old_val, new_val, memory_order_relaxed, memory_order_relaxed));              \
        }                                                                                                                                                    \
                                                                                                                                                             \
        SKIPLIST_FOREACH_H2T(decl, prefix, field, slist, node, nth)                                                                                          \
        {                                                                                                                                                    \
            (void)nth;                                                                                                                                       \
            for (lvl = 0; lvl <= _skip_atomic_load(&node->field.sle_height, memory_order_relaxed); lvl++) {                                                  \
                size_t old_val = _skip_atomic_load(&node->field.sle_levels[lvl].hits, memory_order_relaxed);                                                 \
                size_t new_val;                                                                                                                              \
                do {                                                                                                                                         \
                    new_val = (old_val + 1) / 2;                                                                                                             \
                } while (!_skip_atomic_cas_weak(&node->field.sle_levels[lvl].hits, &old_val, new_val, memory_order_relaxed, memory_order_relaxed));          \
            }                                                                                                                                                \
        }                                                                                                                                                    \
    }                                                                                                                                                        \
                                                                                                                                                             \
    static decl##_node_t *_skip_splay_find_pred_at_level_##decl(decl##_t *slist, decl##_node_t *target, size_t level)                                        \
    {                                                                                                                                                        \
        decl##_node_t *scan, *fwd;                                                                                                                           \
        size_t steps = 0;                                                                                                                                    \
        const size_t MAX_BACK_SCAN = 128;                                                                                                                    \
                                                                                                                                                             \
        scan = _skip_atomic_load(&target->field.sle_prev, memory_order_acquire);                                                                             \
        while (scan != slist->slh_head && steps < MAX_BACK_SCAN) {                                                                                           \
            if (_SKIP_IS_MARKED(scan)) {                                                                                                                     \
                scan = _SKIP_UNMARK(scan);                                                                                                                   \
                scan = _skip_atomic_load(&scan->field.sle_prev, memory_order_acquire);                                                                       \
                steps++;                                                                                                                                     \
                continue;                                                                                                                                    \
            }                                                                                                                                                \
            size_t scan_h = _skip_atomic_load(&scan->field.sle_height, memory_order_acquire);                                                                \
            if (scan_h >= level) {                                                                                                                           \
                fwd = _skip_atomic_load(&scan->field.sle_levels[level].next, memory_order_acquire);                                                          \
                if (fwd == target) {                                                                                                                         \
                    return scan;                                                                                                                             \
                }                                                                                                                                            \
            }                                                                                                                                                \
            scan = _skip_atomic_load(&scan->field.sle_prev, memory_order_acquire);                                                                           \
            steps++;                                                                                                                                         \
        }                                                                                                                                                    \
                                                                                                                                                             \
        /* Check head as last resort. */                                                                                                                     \
        if (_skip_atomic_load(&slist->slh_head->field.sle_height, memory_order_relaxed) >= level) {                                                          \
            fwd = _skip_atomic_load(&slist->slh_head->field.sle_levels[level].next, memory_order_acquire);                                                   \
            if (fwd == target)                                                                                                                               \
                return slist->slh_head;                                                                                                                      \
        }                                                                                                                                                    \
                                                                                                                                                             \
        return NULL;                                                                                                                                         \
    }                                                                                                                                                        \
                                                                                                                                                             \
    static void _fix_skip_rebalance_##decl(decl##_t *slist, size_t len, _skiplist_path_##decl##_t path[])                                                    \
    {                                                                                                                                                        \
        size_t i, node_height, delta_height;                                                                                                                 \
        size_t k_threshold, m_total_hits;                                                                                                                    \
        double asc_cond, dsc_cond;                                                                                                                           \
        decl##_node_t *node, *pred, *succ, *expected;                                                                                                        \
                                                                                                                                                             \
        if (len < 2)                                                                                                                                         \
            return;                                                                                                                                          \
                                                                                                                                                             \
        /* Read global counters. These are approximate under concurrency,                                                                                    \
         * which is fine: splay rebalancing is a heuristic optimization,                                                                                     \
         * not a correctness requirement. */                                                                                                                 \
        k_threshold = _skip_atomic_load(&slist->slh_head->field.sle_height, memory_order_acquire);                                                           \
        m_total_hits = _skip_atomic_load(&slist->slh_head->field.sle_levels[k_threshold].hits, memory_order_relaxed);                                        \
                                                                                                                                                             \
        /* Need at least some history to make meaningful decisions. */                                                                                       \
        if (m_total_hits < 4 || k_threshold < 1)                                                                                                             \
            return;                                                                                                                                          \
                                                                                                                                                             \
        /* Process each node in the search path. path[1..len] are the                                                                                        \
         * predecessors recorded during locate; path[0] is the match.                                                                                        \
         * We only rebalance the actual nodes on the path, skipping                                                                                          \
         * head and tail sentinels. */                                                                                                                       \
        for (i = 1; i <= len; i++) {                                                                                                                         \
            node = path[i].node;                                                                                                                             \
            if (node == NULL || node == slist->slh_head || node == slist->slh_tail)                                                                          \
                continue;                                                                                                                                    \
                                                                                                                                                             \
            /* Skip marked (logically deleted) nodes. */                                                                                                     \
            {                                                                                                                                                \
                decl##_node_t *lvl0_next = _skip_atomic_load(&node->field.sle_levels[0].next, memory_order_acquire);                                         \
                if (_SKIP_IS_MARKED(lvl0_next))                                                                                                              \
                    continue;                                                                                                                                \
            }                                                                                                                                                \
                                                                                                                                                             \
            node_height = _skip_atomic_load(&node->field.sle_height, memory_order_acquire);                                                                  \
                                                                                                                                                             \
            /* Read this node's hit count at level 0 (total accesses). */                                                                                    \
            size_t u_hits = _skip_atomic_load(&node->field.sle_levels[0].hits, memory_order_relaxed);                                                        \
                                                                                                                                                             \
            if (node_height >= k_threshold)                                                                                                                  \
                delta_height = 0;                                                                                                                            \
            else                                                                                                                                             \
                delta_height = k_threshold - node_height;                                                                                                    \
                                                                                                                                                             \
            /* ---- DEMOTION CHECK ----                                                                                                                      \
             *                                                                                                                                               \
             * Condition: u_hits <= m_total_hits / 2^delta_height                                                                                            \
             *                                                                                                                                               \
             * A node with few hits relative to its height is over-promoted.                                                                                 \
             * We remove it from its top level to push it down. */                                                                                           \
            dsc_cond = (double)m_total_hits / (double)(1ULL << delta_height);                                                                                \
            if (u_hits <= (size_t)dsc_cond && node_height > 0) {                                                                                             \
                size_t top = node_height;                                                                                                                    \
                                                                                                                                                             \
                /* Step 1: Find predecessor at the top level. */                                                                                             \
                pred = _skip_splay_find_pred_at_level_##decl(slist, node, top);                                                                              \
                if (pred == NULL) {                                                                                                                          \
                    /* Cannot find predecessor; skip demotion this round.                                                                                    \
                     * This is safe: the node just stays at its current                                                                                      \
                     * height until the next rebalance finds the pred. */                                                                                    \
                    goto _splay_check_ascent_##decl;                                                                                                         \
                }                                                                                                                                            \
                                                                                                                                                             \
                /* Step 2: CAS predecessor's next[top] to skip this node.                                                                                    \
                 *                                                                                                                                           \
                 * We expect: pred->levels[top].next == node                                                                                                 \
                 * We want:   pred->levels[top].next == node->levels[top].next                                                                               \
                 *                                                                                                                                           \
                 * Release ordering ensures the pointer update is visible                                                                                    \
                 * to any thread that subsequently reads this level. */                                                                                      \
                succ = _skip_atomic_load(&node->field.sle_levels[top].next, memory_order_acquire);                                                           \
                                                                                                                                                             \
                /* Don't demote if the successor is marked (concurrent delete). */                                                                           \
                if (_SKIP_IS_MARKED(succ))                                                                                                                   \
                    goto _splay_check_ascent_##decl;                                                                                                         \
                                                                                                                                                             \
                expected = node;                                                                                                                             \
                if (_skip_atomic_cas_strong(&pred->field.sle_levels[top].next, &expected, succ, memory_order_release, memory_order_relaxed)) {               \
                                                                                                                                                             \
                    /* Step 3: Atomically decrement the node's height.                                                                                       \
                     *                                                                                                                                       \
                     * We use a CAS rather than fetch_sub to ensure we                                                                                       \
                     * only decrement if the height hasn't changed since                                                                                     \
                     * we read it (another thread might have promoted or                                                                                     \
                     * demoted concurrently). */                                                                                                             \
                    size_t expected_h = node_height;                                                                                                         \
                    _skip_atomic_cas_strong(&node->field.sle_height, &expected_h, node_height - 1, memory_order_release, memory_order_relaxed);              \
                                                                                                                                                             \
                    /* Transfer hits from the demoted level to the predecessor.                                                                              \
                     * This preserves hit count totals approximately. */                                                                                     \
                    size_t demoted_hits = _skip_atomic_load(&node->field.sle_levels[top].hits, memory_order_relaxed);                                        \
                    _skip_atomic_fetch_add(&pred->field.sle_levels[top].hits, demoted_hits, memory_order_relaxed);                                           \
                    _skip_atomic_store(&node->field.sle_levels[top].hits, 0, memory_order_relaxed);                                                          \
                }                                                                                                                                            \
                /* If CAS fails, another thread modified the link. That's                                                                                    \
                 * fine: we just skip demotion this round. */                                                                                                \
                                                                                                                                                             \
                /* Re-read height after potential demotion for ascent check. */                                                                              \
                node_height = _skip_atomic_load(&node->field.sle_height, memory_order_acquire);                                                              \
                if (node_height >= k_threshold)                                                                                                              \
                    delta_height = 0;                                                                                                                        \
                else                                                                                                                                         \
                    delta_height = k_threshold - node_height;                                                                                                \
            }                                                                                                                                                \
                                                                                                                                                             \
            _splay_check_ascent_##decl : /* ---- PROMOTION CHECK ----                                                                                        \
                                          *                                                                                                                  \
                                          * Condition: u_hits > m_total_hits / 2^(delta_height - 1)                                                          \
                                          *                                                                                                                  \
                                          * A node with many hits relative to its height is under-promoted.                                                  \
                                          * We add a new level to bring it higher in the structure. */                                                       \
                                         if (delta_height < 1) continue;                                                                                     \
            asc_cond = (double)m_total_hits / (double)(1ULL << (delta_height - 1));                                                                          \
            if (u_hits <= (size_t)asc_cond)                                                                                                                  \
                continue;                                                                                                                                    \
            if (node_height >= SKIPLIST_MAX_HEIGHT - 1)                                                                                                      \
                continue;                                                                                                                                    \
            if (node_height >= k_threshold)                                                                                                                  \
                continue;                                                                                                                                    \
                                                                                                                                                             \
            /* Step 1: Grow list height if needed.                                                                                                           \
             *                                                                                                                                               \
             * If promoting this node would exceed the current list height,                                                                                  \
             * grow the head/tail first. This mirrors the logic in insert. */                                                                                \
            {                                                                                                                                                \
                size_t new_node_h = node_height + 1;                                                                                                         \
                size_t cur_head_h = _skip_atomic_load(&slist->slh_head->field.sle_height, memory_order_acquire);                                             \
                if (new_node_h >= cur_head_h) {                                                                                                              \
                    /* Check if total hits justify growing the list. */                                                                                      \
                    size_t expected_h = (size_t)floor(log2((double)m_total_hits));                                                                           \
                    if (expected_h > cur_head_h && expected_h < SKIPLIST_MAX_HEIGHT) {                                                                       \
                        size_t old_h = cur_head_h;                                                                                                           \
                        if (_skip_atomic_cas_strong(&slist->slh_head->field.sle_height, &old_h, expected_h, memory_order_release, memory_order_acquire)) {   \
                            /* Initialize the new head levels. */                                                                                            \
                            for (size_t h = old_h + 1; h <= expected_h; h++) {                                                                               \
                                _skip_atomic_store(&slist->slh_head->field.sle_levels[h].next, slist->slh_tail, memory_order_relaxed);                       \
                                _skip_atomic_store(&slist->slh_head->field.sle_levels[h].hits,                                                               \
                                    _skip_atomic_load(&slist->slh_head->field.sle_levels[old_h].hits, memory_order_relaxed), memory_order_relaxed);          \
                            }                                                                                                                                \
                            _skip_atomic_store(&slist->slh_tail->field.sle_height, expected_h, memory_order_release);                                        \
                            k_threshold = expected_h;                                                                                                        \
                        }                                                                                                                                    \
                    } else {                                                                                                                                 \
                        /* Hits don't justify growing; skip promotion. */                                                                                    \
                        continue;                                                                                                                            \
                    }                                                                                                                                        \
                }                                                                                                                                            \
            }                                                                                                                                                \
                                                                                                                                                             \
            /* Step 2: Atomically increment node height.                                                                                                     \
             *                                                                                                                                               \
             * Use CAS to ensure no concurrent height change happened.                                                                                       \
             * If it did, just skip -- another thread handled it. */                                                                                         \
            {                                                                                                                                                \
                size_t expected_h = node_height;                                                                                                             \
                size_t new_h = node_height + 1;                                                                                                              \
                if (!_skip_atomic_cas_strong(&node->field.sle_height, &expected_h, new_h, memory_order_release, memory_order_relaxed)) {                     \
                    /* Height was changed by another thread. Skip. */                                                                                        \
                    continue;                                                                                                                                \
                }                                                                                                                                            \
                                                                                                                                                             \
                /* Step 3: Link the new level into the skip chain.                                                                                           \
                 *                                                                                                                                           \
                 * We need to find a predecessor at level new_h and CAS                                                                                      \
                 * ourselves into the chain:                                                                                                                 \
                 *   pred->levels[new_h].next: old_succ -> node                                                                                              \
                 *   node->levels[new_h].next: (set to) old_succ                                                                                             \
                 *                                                                                                                                           \
                 * Use path[i].node as a hint for the predecessor since                                                                                      \
                 * path[i+1] (if it exists) would be the predecessor at                                                                                      \
                 * the next higher level from the locate traversal. */                                                                                       \
                pred = NULL;                                                                                                                                 \
                                                                                                                                                             \
                /* Try to use path information first. path[i+1] if valid                                                                                     \
                 * might be the predecessor at a higher level. However,                                                                                      \
                 * paths can be stale, so we fall back to backward scan. */                                                                                  \
                if (i + 1 <= len && path[i + 1].node != NULL) {                                                                                              \
                    decl##_node_t *cand = path[i + 1].node;                                                                                                  \
                    size_t cand_h = _skip_atomic_load(&cand->field.sle_height, memory_order_acquire);                                                        \
                    if (cand_h >= new_h) {                                                                                                                   \
                        /* Validate: cand's next at new_h should come                                                                                        \
                         * after node in sort order (or be tail). */                                                                                         \
                        decl##_node_t *cand_next = _skip_atomic_load(&cand->field.sle_levels[new_h].next, memory_order_acquire);                             \
                        if (cand_next != NULL && !_SKIP_IS_MARKED(cand_next)) {                                                                              \
                            int c = (cand_next == slist->slh_tail) ? 1 : _skip_compare_nodes_##decl(slist, cand_next, node, slist->slh_aux);                 \
                            if (c >= 0) {                                                                                                                    \
                                pred = cand;                                                                                                                 \
                            }                                                                                                                                \
                        }                                                                                                                                    \
                    }                                                                                                                                        \
                }                                                                                                                                            \
                                                                                                                                                             \
                /* Fall back to backward scan if path hint didn't work. */                                                                                   \
                if (pred == NULL) {                                                                                                                          \
                    pred = _skip_splay_find_pred_at_level_##decl(slist, node, new_h);                                                                        \
                }                                                                                                                                            \
                                                                                                                                                             \
                /* If no predecessor found, try head. */                                                                                                     \
                if (pred == NULL) {                                                                                                                          \
                    size_t head_h = _skip_atomic_load(&slist->slh_head->field.sle_height, memory_order_acquire);                                             \
                    if (head_h >= new_h) {                                                                                                                   \
                        pred = slist->slh_head;                                                                                                              \
                    }                                                                                                                                        \
                }                                                                                                                                            \
                                                                                                                                                             \
                if (pred == NULL) {                                                                                                                          \
                    /* Cannot link: revert the height increment.                                                                                             \
                     * This is safe because no pointer references new_h yet. */                                                                              \
                    size_t revert_exp = new_h;                                                                                                               \
                    _skip_atomic_cas_strong(&node->field.sle_height, &revert_exp, node_height, memory_order_release, memory_order_relaxed);                  \
                    continue;                                                                                                                                \
                }                                                                                                                                            \
                                                                                                                                                             \
                /* Read what pred currently points to at new_h. */                                                                                           \
                succ = _skip_atomic_load(&pred->field.sle_levels[new_h].next, memory_order_acquire);                                                         \
                                                                                                                                                             \
                if (_SKIP_IS_MARKED(succ)) {                                                                                                                 \
                    /* Pred is being deleted; revert height. */                                                                                              \
                    size_t revert_exp = new_h;                                                                                                               \
                    _skip_atomic_cas_strong(&node->field.sle_height, &revert_exp, node_height, memory_order_release, memory_order_relaxed);                  \
                    continue;                                                                                                                                \
                }                                                                                                                                            \
                                                                                                                                                             \
                /* Set our new level's next to succ before linking in.                                                                                       \
                 * Use relaxed: this is not yet visible to other threads                                                                                     \
                 * (they'll see it only after the CAS on pred succeeds). */                                                                                  \
                _skip_atomic_store(&node->field.sle_levels[new_h].next, succ, memory_order_relaxed);                                                         \
                _skip_atomic_store(&node->field.sle_levels[new_h].hits, 0, memory_order_relaxed);                                                            \
                                                                                                                                                             \
                /* CAS: pred->levels[new_h].next = succ -> node.                                                                                             \
                 * Release ordering publishes our new level's next pointer. */                                                                               \
                expected = succ;                                                                                                                             \
                if (!_skip_atomic_cas_strong(&pred->field.sle_levels[new_h].next, &expected, node, memory_order_release, memory_order_relaxed)) {            \
                    /* CAS failed: revert height. The node remains correct                                                                                   \
                     * at its old height; the stale next pointer at new_h                                                                                    \
                     * is harmless since no one links to it. */                                                                                              \
                    size_t revert_exp = new_h;                                                                                                               \
                    _skip_atomic_cas_strong(&node->field.sle_height, &revert_exp, node_height, memory_order_release, memory_order_relaxed);                  \
                }                                                                                                                                            \
            }                                                                                                                                                \
        }                                                                                                                                                    \
    }                                                                                                                                                        \
                                                                                                                                                             \
    static void _skip_rebalance_##decl(decl##_t *slist, size_t len, _skiplist_path_##decl##_t path[])                                                        \
    {                                                                                                                                                        \
        SKIPLIST_SPLAY_IMPL(decl, slist, len, path);                                                                                                         \
    }                                                                                                                                                        \
                                                                                                                                                             \
    /**                                                                                                                                                      \
     * -- _skip_locate_                                                                                                                                      \
     *                                                                                                                                                       \
     * Lock-free search: locates a node in the skiplist using Fraser's                                                                                       \
     * algorithm.  Helps physically unlink marked (logically deleted)                                                                                        \
     * nodes encountered during traversal.  Fills `path` with predecessors                                                                                   \
     * and successors, returning the path length and a match in path[0].                                                                                     \
     */                                                                                                                                                      \
    static size_t _skip_locate_##decl(decl##_t *slist, decl##_node_t *n, _skiplist_path_##decl##_t path[])                                                   \
    {                                                                                                                                                        \
        size_t len = 0;                                                                                                                                      \
        int cmp;                                                                                                                                             \
        decl##_node_t *pred, *curr = NULL, *succ;                                                                                                            \
                                                                                                                                                             \
        if (slist == NULL || n == NULL)                                                                                                                      \
            return 0;                                                                                                                                        \
                                                                                                                                                             \
        _skip_locate_retry_##decl : pred = slist->slh_head;                                                                                                  \
        len = 0;                                                                                                                                             \
                                                                                                                                                             \
        /* Traverse from the highest active level down to level 0.                                                                                           \
           Head's sle_height is the number of active levels (1 means only                                                                                    \
           level 0 is active; search starts at level sle_height - 1). */                                                                                     \
        for (size_t _lvl = _skip_atomic_load(&slist->slh_head->field.sle_height, memory_order_acquire); _lvl > 0; _lvl--) {                                  \
            size_t i = _lvl - 1; /* current level index */                                                                                                   \
                                                                                                                                                             \
            /* Read pred's next pointer at this level.  If pred was                                                                                          \
               concurrently logically deleted, its stored next pointers                                                                                      \
               are marked -- restart from the top in that case. */                                                                                          \
            curr = _skip_atomic_load(&pred->field.sle_levels[i].next, memory_order_acquire);                                                                 \
            if (_SKIP_IS_MARKED(curr)) {                                                                                                                     \
                goto _skip_locate_retry_##decl;                                                                                                              \
            }                                                                                                                                                \
                                                                                                                                                             \
            for (;;) {                                                                                                                                       \
                /* Read curr's next pointer.  Tail's next is always NULL. */                                                                                 \
                if (curr == slist->slh_tail) {                                                                                                               \
                    succ = NULL;                                                                                                                             \
                } else {                                                                                                                                     \
                    succ = _skip_atomic_load(&curr->field.sle_levels[i].next, memory_order_acquire);                                                         \
                }                                                                                                                                            \
                                                                                                                                                             \
                /* Help unlink any marked (logically deleted) nodes. */                                                                                      \
                while (succ != NULL && _SKIP_IS_MARKED(succ)) {                                                                                              \
                    decl##_node_t *unmarked_succ = _SKIP_UNMARK(succ);                                                                                       \
                    decl##_node_t *expected = curr;                                                                                                          \
                    if (!_skip_atomic_cas_strong(&pred->field.sle_levels[i].next, &expected, unmarked_succ, memory_order_release, memory_order_relaxed)) {   \
                        /* CAS failed: restart from the top. */                                                                                              \
                        goto _skip_locate_retry_##decl;                                                                                                      \
                    }                                                                                                                                        \
                    curr = unmarked_succ;                                                                                                                    \
                    if (curr == slist->slh_tail) {                                                                                                           \
                        succ = NULL;                                                                                                                         \
                    } else {                                                                                                                                 \
                        succ = _skip_atomic_load(&curr->field.sle_levels[i].next, memory_order_acquire);                                                     \
                    }                                                                                                                                        \
                }                                                                                                                                            \
                                                                                                                                                             \
                /* Both curr and succ are unmarked.  Compare curr against key. */                                                                            \
                cmp = _skip_compare_nodes_##decl(slist, curr, n, slist->slh_aux);                                                                            \
                if (cmp < 0) {                                                                                                                               \
                    pred = curr;                                                                                                                             \
                    curr = (succ == NULL) ? slist->slh_tail : succ;                                                                                          \
                } else {                                                                                                                                     \
                    break;                                                                                                                                   \
                }                                                                                                                                            \
            }                                                                                                                                                \
                                                                                                                                                             \
            /* Record predecessor and successor at level i. */                                                                                               \
            path[i + 1].node = pred;                                                                                                                         \
            path[i + 1].succ = curr;                                                                                                                         \
            path[i + 1].pu = 0;                                                                                                                              \
            len++;                                                                                                                                           \
        }                                                                                                                                                    \
                                                                                                                                                             \
        /* Check for an exact match at level 0. */                                                                                                           \
        if (curr != slist->slh_tail && _skip_compare_nodes_##decl(slist, curr, n, slist->slh_aux) == 0) {                                                    \
            path[0].node = curr;                                                                                                                             \
            /* Hit counting with relaxed atomics. */                                                                                                         \
            _skip_atomic_fetch_add(&curr->field.sle_levels[0].hits, 1, memory_order_relaxed);                                                                \
            _skip_atomic_fetch_add(&slist->slh_head->field.sle_levels[_skip_atomic_load(&slist->slh_head->field.sle_height, memory_order_relaxed)].hits, 1,  \
                memory_order_relaxed);                                                                                                                       \
            _skip_rebalance_##decl(slist, len, path);                                                                                                        \
        } else {                                                                                                                                             \
            path[0].node = NULL;                                                                                                                             \
        }                                                                                                                                                    \
                                                                                                                                                             \
        return len;                                                                                                                                          \
    }                                                                                                                                                        \
                                                                                                                                                             \
    /**                                                                                                                                                      \
     * -- _skip_insert_                                                                                                                                      \
     *                                                                                                                                                       \
     * Lock-free insert: atomically links `n` into the skiplist at                                                                                           \
     * all appropriate levels.  Level-0 CAS is the linearization point.                                                                                      \
     * When `flags` is 0, duplicates are rejected; when non-zero, they                                                                                       \
     * are allowed.                                                                                                                                          \
     */                                                                                                                                                      \
    static int _skip_insert_##decl(decl##_t *slist, decl##_node_t *n, int flags)                                                                             \
    {                                                                                                                                                        \
        /* Only path[0] is pre-zeroed (_SKIP_PATH_CLEAR); _skip_locate_ writes                                                                               \
           all entries [0..height] before they are read.  Upper entries may                                                                                  \
           contain stack garbage until locate or the fill-loop initializes them. */                                                                          \
        _skiplist_path_##decl##_t apath[SKIPLIST_MAX_HEIGHT + 1];                                                                                            \
        int rc = 0;                                                                                                                                          \
        size_t i, len, current_height, new_height;                                                                                                           \
        _skiplist_path_##decl##_t *path = apath;                                                                                                             \
                                                                                                                                                             \
        if (slist == NULL || n == NULL)                                                                                                                      \
            return EINVAL;                                                                                                                                   \
                                                                                                                                                             \
        _skip_insert_retry_##decl : _SKIP_PATH_CLEAR(path);                                                                                                  \
                                                                                                                                                             \
        /* Phase 1: Find the insertion point. */                                                                                                             \
        len = _skip_locate_##decl(slist, n, path);                                                                                                           \
        if (len == 0)                                                                                                                                        \
            return ENOENT;                                                                                                                                   \
                                                                                                                                                             \
        /* Reject duplicates unless flags is set. */                                                                                                         \
        if (path[0].node != NULL && flags == 0) {                                                                                                            \
            return EEXIST;                                                                                                                                   \
        }                                                                                                                                                    \
                                                                                                                                                             \
        /* Phase 2: Determine the new node's height via coin toss.                                                                                           \
           Use `len` (levels locate actually traversed) rather than a                                                                                        \
           fresh read of sle_height: another thread may have grown the                                                                                       \
           head between our locate and now, leaving a gap in path[]. */                                                                                      \
        current_height = len - 1;                                                                                                                            \
                                                                                                                                                             \
        {                                                                                                                                                    \
            size_t toss_max = current_height + 1;                                                                                                            \
            if (toss_max > SKIPLIST_MAX_HEIGHT - 2)                                                                                                          \
                toss_max = SKIPLIST_MAX_HEIGHT - 2;                                                                                                          \
            new_height = _skip_toss_##decl(slist, toss_max);                                                                                                 \
        }                                                                                                                                                    \
        _skip_atomic_store(&n->field.sle_height, new_height, memory_order_relaxed);                                                                          \
                                                                                                                                                             \
        /* Phase 3: Grow the head height if needed (CAS loop). */                                                                                            \
        if (new_height > current_height) {                                                                                                                   \
            size_t desired_head_h = new_height + 1;                                                                                                          \
            size_t cur_h = _skip_atomic_load(&slist->slh_head->field.sle_height, memory_order_acquire);                                                      \
            while (cur_h < desired_head_h) {                                                                                                                 \
                if (_skip_atomic_cas_weak(&slist->slh_head->field.sle_height, &cur_h, desired_head_h, memory_order_release, memory_order_acquire)) {         \
                    _skip_atomic_store(&slist->slh_tail->field.sle_height, desired_head_h, memory_order_release);                                            \
                    break;                                                                                                                                   \
                }                                                                                                                                            \
            }                                                                                                                                                \
        }                                                                                                                                                    \
                                                                                                                                                             \
        /* Fill path entries for levels above what locate traversed. */                                                                                      \
        for (i = len; i <= new_height; i++) {                                                                                                                \
            path[i + 1].node = slist->slh_head;                                                                                                              \
            path[i + 1].succ = slist->slh_tail;                                                                                                              \
        }                                                                                                                                                    \
                                                                                                                                                             \
        /* Phase 4: Pre-fill the new node's next pointers. */                                                                                                \
        for (i = 0; i <= new_height; i++) {                                                                                                                  \
            _skip_atomic_store(&n->field.sle_levels[i].next, path[i + 1].succ, memory_order_relaxed);                                                        \
        }                                                                                                                                                    \
                                                                                                                                                             \
        /* Phase 5: CAS at level 0 -- LINEARIZATION POINT. */                                                                                                \
        {                                                                                                                                                    \
            decl##_node_t *expected = path[1].succ;                                                                                                          \
            if (!_skip_atomic_cas_strong(&path[1].node->field.sle_levels[0].next, &expected, n, memory_order_release, memory_order_relaxed)) {               \
                goto _skip_insert_retry_##decl;                                                                                                              \
            }                                                                                                                                                \
        }                                                                                                                                                    \
                                                                                                                                                             \
        /* Phase 6: Link at higher levels (1 .. new_height). */                                                                                              \
        for (i = 1; i <= new_height; i++) {                                                                                                                  \
            for (;;) {                                                                                                                                       \
                decl##_node_t *pred_at_i = path[i + 1].node;                                                                                                 \
                decl##_node_t *succ_at_i = path[i + 1].succ;                                                                                                 \
                                                                                                                                                             \
                {                                                                                                                                            \
                    decl##_node_t *cur_next = _skip_atomic_load(&n->field.sle_levels[i].next, memory_order_acquire);                                         \
                    if (_SKIP_IS_MARKED(cur_next)) {                                                                                                         \
                        goto _skip_insert_done_##decl;                                                                                                       \
                    }                                                                                                                                        \
                    if (cur_next != succ_at_i) {                                                                                                             \
                        _skip_atomic_store(&n->field.sle_levels[i].next, succ_at_i, memory_order_relaxed);                                                   \
                    }                                                                                                                                        \
                }                                                                                                                                            \
                                                                                                                                                             \
                {                                                                                                                                            \
                    decl##_node_t *expected = succ_at_i;                                                                                                     \
                    if (_skip_atomic_cas_strong(&pred_at_i->field.sle_levels[i].next, &expected, n, memory_order_release, memory_order_relaxed)) {           \
                        break;                                                                                                                               \
                    }                                                                                                                                        \
                }                                                                                                                                            \
                                                                                                                                                             \
                /* CAS failed; re-find to get fresh preds/succs. */                                                                                          \
                _SKIP_PATH_CLEAR(path);                                                                                                                      \
                _skip_locate_##decl(slist, n, path);                                                                                                         \
                                                                                                                                                             \
                {                                                                                                                                            \
                    decl##_node_t *lvl0_next = _skip_atomic_load(&n->field.sle_levels[0].next, memory_order_acquire);                                        \
                    if (_SKIP_IS_MARKED(lvl0_next)) {                                                                                                        \
                        goto _skip_insert_done_##decl;                                                                                                       \
                    }                                                                                                                                        \
                }                                                                                                                                            \
            }                                                                                                                                                \
        }                                                                                                                                                    \
                                                                                                                                                             \
        _skip_insert_done_##decl : /* Phase 7: Set backward pointer (advisory, best-effort). */                                                              \
                                   _skip_atomic_store(&n->field.sle_prev, path[1].node, memory_order_relaxed);                                               \
        {                                                                                                                                                    \
            decl##_node_t *succ_at_0 = _skip_atomic_load(&n->field.sle_levels[0].next, memory_order_acquire);                                                \
            if (!_SKIP_IS_MARKED(succ_at_0) && succ_at_0 != slist->slh_tail) {                                                                               \
                decl##_node_t *old_prev = path[1].node;                                                                                                      \
                _skip_atomic_cas_strong(&succ_at_0->field.sle_prev, &old_prev, n, memory_order_relaxed, memory_order_relaxed);                               \
            } else if (!_SKIP_IS_MARKED(succ_at_0) && succ_at_0 == slist->slh_tail) {                                                                        \
                decl##_node_t *old_prev = path[1].node;                                                                                                      \
                _skip_atomic_cas_strong(&slist->slh_tail->field.sle_prev, &old_prev, n, memory_order_relaxed, memory_order_relaxed);                         \
            }                                                                                                                                                \
        }                                                                                                                                                    \
                                                                                                                                                             \
        /* Record era for snapshot support.                                                                                                                  \
           Non-atomic: snapshots are single-threaded only (see SKIPLIST_DECL_SNAPSHOTS). */                                                                  \
        if (slist->slh_snap.pres_era > 0) {                                                                                                                  \
            n->field.sle_era = slist->slh_snap.cur_era++;                                                                                                    \
        }                                                                                                                                                    \
                                                                                                                                                             \
        /* Initial hit count for splay rebalancing. */                                                                                                       \
        _skip_atomic_store(&n->field.sle_levels[new_height].hits, 1, memory_order_relaxed);                                                                  \
                                                                                                                                                             \
        /* Increment list length. */                                                                                                                         \
        _skip_atomic_fetch_add(&slist->slh_length, 1, memory_order_relaxed);                                                                                 \
                                                                                                                                                             \
        return rc;                                                                                                                                           \
    }                                                                                                                                                        \
                                                                                                                                                             \
    /**                                                                                                                                                      \
     * -- skip_insert_                                                                                                                                       \
     *                                                                                                                                                       \
     * Insert into the list `slist` the node `n`.                                                                                                            \
     */                                                                                                                                                      \
    int prefix##skip_insert_##decl(decl##_t *slist, decl##_node_t *n)                                                                                        \
    {                                                                                                                                                        \
        return _skip_insert_##decl(slist, n, 0);                                                                                                             \
    }                                                                                                                                                        \
                                                                                                                                                             \
    /**                                                                                                                                                      \
     * -- skip_insert_dup_                                                                                                                                   \
     *                                                                                                                                                       \
     * Inserts into `slist` the node `n` even if that node's key already                                                                                     \
     * exists in the list.                                                                                                                                   \
     */                                                                                                                                                      \
    int prefix##skip_insert_dup_##decl(decl##_t *slist, decl##_node_t *n)                                                                                    \
    {                                                                                                                                                        \
        return _skip_insert_##decl(slist, n, 1);                                                                                                             \
    }                                                                                                                                                        \
                                                                                                                                                             \
    /**                                                                                                                                                      \
     * -- skip_position_eq_                                                                                                                                  \
     *                                                                                                                                                       \
     * Find a node that matches the node `n`.  This differs from the locate()                                                                                \
     * API in that it does not return the path to the node, only the match.                                                                                  \
     */                                                                                                                                                      \
    decl##_node_t *prefix##skip_position_eq_##decl(decl##_t *slist, decl##_node_t *query)                                                                    \
    {                                                                                                                                                        \
        _skiplist_path_##decl##_t apath[SKIPLIST_MAX_HEIGHT + 1];                                                                                            \
        decl##_node_t *node = NULL;                                                                                                                          \
        _skiplist_path_##decl##_t *path = apath;                                                                                                             \
                                                                                                                                                             \
        _SKIP_PATH_CLEAR(path);                                                                                                                              \
                                                                                                                                                             \
        /* Find a `path` to `query` in the list and a match (`path[0]`) if it exists. */                                                                     \
        _skip_locate_##decl(slist, query, path);                                                                                                             \
        node = path[0].node;                                                                                                                                 \
                                                                                                                                                             \
        return node;                                                                                                                                         \
    }                                                                                                                                                        \
                                                                                                                                                             \
    /**                                                                                                                                                      \
     * -- skip_position_gte                                                                                                                                  \
     *                                                                                                                                                       \
     * Position and return a cursor at the first node that is equal to                                                                                       \
     * or greater than the provided node `n`, otherwise if the largest                                                                                       \
     * key is less than the key in `n` return NULL.                                                                                                          \
     */                                                                                                                                                      \
    decl##_node_t *prefix##skip_position_gte_##decl(decl##_t *slist, decl##_node_t *query)                                                                   \
    {                                                                                                                                                        \
        _skiplist_path_##decl##_t apath[SKIPLIST_MAX_HEIGHT + 1];                                                                                            \
        int cmp;                                                                                                                                             \
        decl##_node_t *node;                                                                                                                                 \
        _skiplist_path_##decl##_t *path = apath;                                                                                                             \
                                                                                                                                                             \
        _SKIP_PATH_CLEAR(path);                                                                                                                              \
                                                                                                                                                             \
        /* Find a `path` to `query` in the list and a match (`path[0]`) if it exists. */                                                                     \
        _skip_locate_##decl(slist, query, path);                                                                                                             \
        node = path[1].node;                                                                                                                                 \
        do {                                                                                                                                                 \
            node = _skip_atomic_load(&node->field.sle_levels[0].next, memory_order_acquire);                                                                 \
            cmp = _skip_compare_nodes_##decl(slist, node, query, slist->slh_aux);                                                                            \
        } while (cmp < 0);                                                                                                                                   \
                                                                                                                                                             \
        if (node == slist->slh_tail)                                                                                                                         \
            return NULL;                                                                                                                                     \
        return node;                                                                                                                                         \
    }                                                                                                                                                        \
                                                                                                                                                             \
    /**                                                                                                                                                      \
     * -- skip_position_gt_                                                                                                                                  \
     *                                                                                                                                                       \
     * Position and return a cursor at the first node that is greater than                                                                                   \
     * the provided node `n`. If the largest key is less than the key in `n`                                                                                 \
     * return NULL.                                                                                                                                          \
     */                                                                                                                                                      \
    decl##_node_t *prefix##skip_position_gt_##decl(decl##_t *slist, decl##_node_t *query)                                                                    \
    {                                                                                                                                                        \
        _skiplist_path_##decl##_t apath[SKIPLIST_MAX_HEIGHT + 1];                                                                                            \
        int cmp;                                                                                                                                             \
        decl##_node_t *node;                                                                                                                                 \
        _skiplist_path_##decl##_t *path = apath;                                                                                                             \
                                                                                                                                                             \
        _SKIP_PATH_CLEAR(path);                                                                                                                              \
                                                                                                                                                             \
        /* Find a `path` to `query` in the list and a match (`path[0]`) if it exists. */                                                                     \
        _skip_locate_##decl(slist, query, path);                                                                                                             \
        node = path[1].node;                                                                                                                                 \
        if (node == slist->slh_tail)                                                                                                                         \
            goto done;                                                                                                                                       \
        do {                                                                                                                                                 \
            node = _skip_atomic_load(&node->field.sle_levels[0].next, memory_order_acquire);                                                                 \
            cmp = _skip_compare_nodes_##decl(slist, node, query, slist->slh_aux);                                                                            \
        } while (cmp <= 0 && node != slist->slh_tail);                                                                                                       \
    done:;                                                                                                                                                   \
                                                                                                                                                             \
        return (node == slist->slh_tail) ? NULL : node;                                                                                                      \
    }                                                                                                                                                        \
                                                                                                                                                             \
    /**                                                                                                                                                      \
     * -- skip_position_lte                                                                                                                                  \
     *                                                                                                                                                       \
     * Position and return a cursor at the last node that is less than                                                                                       \
     * or equal to node `n`.                                                                                                                                 \
     * Return NULL if nothing is less than or equal.                                                                                                         \
     */                                                                                                                                                      \
    decl##_node_t *prefix##skip_position_lte_##decl(decl##_t *slist, decl##_node_t *query)                                                                   \
    {                                                                                                                                                        \
        _skiplist_path_##decl##_t apath[SKIPLIST_MAX_HEIGHT + 1];                                                                                            \
        decl##_node_t *node;                                                                                                                                 \
        _skiplist_path_##decl##_t *path = apath;                                                                                                             \
                                                                                                                                                             \
        _SKIP_PATH_CLEAR(path);                                                                                                                              \
                                                                                                                                                             \
        /* Find a `path` to `query` in the list and a match (`path[0]`) if it exists. */                                                                     \
        _skip_locate_##decl(slist, query, path);                                                                                                             \
        node = path[0].node;                                                                                                                                 \
        if (node)                                                                                                                                            \
            goto done;                                                                                                                                       \
        node = path[1].node;                                                                                                                                 \
        if (node == slist->slh_head)                                                                                                                         \
            node = NULL;                                                                                                                                     \
    done:;                                                                                                                                                   \
                                                                                                                                                             \
        return node;                                                                                                                                         \
    }                                                                                                                                                        \
                                                                                                                                                             \
    /**                                                                                                                                                      \
     * -- skip_position_lt_                                                                                                                                  \
     *                                                                                                                                                       \
     * Position and return a cursor at the last node that is less than                                                                                       \
     * to the node `n`. Return NULL if nothing is less than or equal.                                                                                        \
     */                                                                                                                                                      \
    decl##_node_t *prefix##skip_position_lt_##decl(decl##_t *slist, decl##_node_t *query)                                                                    \
    {                                                                                                                                                        \
        _skiplist_path_##decl##_t apath[SKIPLIST_MAX_HEIGHT + 1];                                                                                            \
        decl##_node_t *node;                                                                                                                                 \
        _skiplist_path_##decl##_t *path = apath;                                                                                                             \
                                                                                                                                                             \
        _SKIP_PATH_CLEAR(path);                                                                                                                              \
                                                                                                                                                             \
        /* Find a `path` to `query` in the list and a match (`path[0]`) if it exists. */                                                                     \
        _skip_locate_##decl(slist, query, path);                                                                                                             \
        node = path[1].node;                                                                                                                                 \
        if (node == slist->slh_head)                                                                                                                         \
            node = NULL;                                                                                                                                     \
                                                                                                                                                             \
        return node;                                                                                                                                         \
    }                                                                                                                                                        \
                                                                                                                                                             \
    /**                                                                                                                                                      \
     * -- skip_position_                                                                                                                                     \
     *                                                                                                                                                       \
     * Position a cursor relative to `n`.                                                                                                                    \
     */                                                                                                                                                      \
    decl##_node_t *prefix##skip_position_##decl(decl##_t *slist, skip_pos_##decl_t op, decl##_node_t *query)                                                 \
    {                                                                                                                                                        \
        decl##_node_t *node;                                                                                                                                 \
                                                                                                                                                             \
        switch (op) {                                                                                                                                        \
        case (SKIP_LT):                                                                                                                                      \
            node = prefix##skip_position_lt_##decl(slist, query);                                                                                            \
            break;                                                                                                                                           \
        case (SKIP_LTE):                                                                                                                                     \
            node = prefix##skip_position_lte_##decl(slist, query);                                                                                           \
            break;                                                                                                                                           \
        case (SKIP_GTE):                                                                                                                                     \
            node = prefix##skip_position_gte_##decl(slist, query);                                                                                           \
            break;                                                                                                                                           \
        case (SKIP_GT):                                                                                                                                      \
            node = prefix##skip_position_gt_##decl(slist, query);                                                                                            \
            break;                                                                                                                                           \
        default:                                                                                                                                             \
        case (SKIP_EQ):                                                                                                                                      \
            node = prefix##skip_position_eq_##decl(slist, query);                                                                                            \
            break;                                                                                                                                           \
        }                                                                                                                                                    \
        return node;                                                                                                                                         \
    }                                                                                                                                                        \
                                                                                                                                                             \
    /**                                                                                                                                                      \
     * -- skip_update_                                                                                                                                       \
     *                                                                                                                                                       \
     * Locates a node in the list that equals the `new` node and then                                                                                        \
     * uses the `update_entry_blk` to update the contents.                                                                                                   \
     *                                                                                                                                                       \
     * WARNING: Do not update the portion of the node used for ordering                                                                                      \
     * (e.g. `key`) unless you really know what you're doing.                                                                                                \
     */                                                                                                                                                      \
    int prefix##skip_update_##decl(decl##_t *slist, decl##_node_t *query, void *value)                                                                       \
    {                                                                                                                                                        \
        _skiplist_path_##decl##_t apath[SKIPLIST_MAX_HEIGHT + 1];                                                                                            \
        int rc = 0, np;                                                                                                                                      \
        decl##_node_t *node;                                                                                                                                 \
        _skiplist_path_##decl##_t *path = apath;                                                                                                             \
                                                                                                                                                             \
        if (slist == NULL)                                                                                                                                   \
            return EINVAL;                                                                                                                                   \
                                                                                                                                                             \
        _SKIP_PATH_CLEAR(path);                                                                                                                              \
                                                                                                                                                             \
        _skip_locate_##decl(slist, query, path);                                                                                                             \
        node = path[0].node;                                                                                                                                 \
                                                                                                                                                             \
        if (node == NULL)                                                                                                                                    \
            return ENOENT;                                                                                                                                   \
                                                                                                                                                             \
        /* If the optional snapshots feature is configured, use it now.                                                                                      \
           Snapshots preserve the node if it is older than our snapshot                                                                                      \
           and about to be mutated. */                                                                                                                       \
        if (slist->slh_snap.pres_era > 0) {                                                                                                                  \
            /* Preserve the node. */                                                                                                                         \
            np = slist->slh_fns.snapshot_preserve_node(slist, node, NULL);                                                                                   \
            if (np > 0)                                                                                                                                      \
                return np;                                                                                                                                   \
                                                                                                                                                             \
            /* Increase the list's era/age. */                                                                                                               \
            slist->slh_snap.cur_era++;                                                                                                                       \
        }                                                                                                                                                    \
                                                                                                                                                             \
        slist->slh_fns.update_entry(node, value);                                                                                                            \
                                                                                                                                                             \
        return rc;                                                                                                                                           \
    }                                                                                                                                                        \
                                                                                                                                                             \
    /**                                                                                                                                                      \
     * -- skip_remove_node_                                                                                                                                  \
     *                                                                                                                                                       \
     * Lock-free delete: logically removes a node by marking its next                                                                                        \
     * pointers (top-down), then physically unlinks by re-traversing.                                                                                        \
     * The level-0 mark is the linearization point.                                                                                                          \
     */                                                                                                                                                      \
    int prefix##skip_remove_node_##decl(decl##_t *slist, decl##_node_t *query)                                                                               \
    {                                                                                                                                                        \
        _skiplist_path_##decl##_t apath[SKIPLIST_MAX_HEIGHT + 1];                                                                                            \
        int np = 0;                                                                                                                                          \
        size_t height;                                                                                                                                       \
        decl##_node_t *node, *succ, *expected;                                                                                                               \
        _skiplist_path_##decl##_t *path = apath;                                                                                                             \
        int ok;                                                                                                                                              \
                                                                                                                                                             \
        if (slist == NULL || query == NULL)                                                                                                                  \
            return EINVAL;                                                                                                                                   \
                                                                                                                                                             \
        _SKIP_PATH_CLEAR(path);                                                                                                                              \
                                                                                                                                                             \
        /* Locate the node to be removed. */                                                                                                                 \
        _skip_locate_##decl(slist, query, path);                                                                                                             \
        node = path[0].node;                                                                                                                                 \
        if (node == NULL) {                                                                                                                                  \
            return ENOENT;                                                                                                                                   \
        }                                                                                                                                                    \
                                                                                                                                                             \
        /* Snapshot preservation (single-threaded feature).                                                                                                  \
           Non-atomic: snapshots are single-threaded only (see SKIPLIST_DECL_SNAPSHOTS). */                                                                  \
        if (slist->slh_snap.pres_era > 0) {                                                                                                                  \
            np = slist->slh_fns.snapshot_preserve_node(slist, node, NULL);                                                                                   \
            if (np > 0)                                                                                                                                      \
                return np;                                                                                                                                   \
            slist->slh_snap.cur_era++;                                                                                                                       \
        }                                                                                                                                                    \
                                                                                                                                                             \
        height = _skip_atomic_load(&node->field.sle_height, memory_order_acquire);                                                                           \
                                                                                                                                                             \
        /* Phase 1: Mark next pointers from top level down to level 1. */                                                                                    \
        {                                                                                                                                                    \
            size_t lvl = height;                                                                                                                             \
            while (lvl >= 1) {                                                                                                                               \
                succ = _skip_atomic_load(&node->field.sle_levels[lvl].next, memory_order_acquire);                                                           \
                while (!_SKIP_IS_MARKED(succ)) {                                                                                                             \
                    expected = succ;                                                                                                                         \
                    _skip_atomic_cas_strong(&node->field.sle_levels[lvl].next, &expected, _SKIP_MARK(succ), memory_order_release, memory_order_relaxed);     \
                    succ = _skip_atomic_load(&node->field.sle_levels[lvl].next, memory_order_acquire);                                                       \
                }                                                                                                                                            \
                lvl--;                                                                                                                                       \
            }                                                                                                                                                \
        }                                                                                                                                                    \
                                                                                                                                                             \
        /* Phase 2: Mark level 0 -- LINEARIZATION POINT. */                                                                                                  \
        succ = _skip_atomic_load(&node->field.sle_levels[0].next, memory_order_acquire);                                                                     \
        for (;;) {                                                                                                                                           \
            if (_SKIP_IS_MARKED(succ)) {                                                                                                                     \
                /* Another thread already marked level 0. */                                                                                                 \
                return 0;                                                                                                                                    \
            }                                                                                                                                                \
            expected = succ;                                                                                                                                 \
            ok = _skip_atomic_cas_strong(&node->field.sle_levels[0].next, &expected, _SKIP_MARK(succ), memory_order_acq_rel, memory_order_acquire);          \
            if (ok) {                                                                                                                                        \
                break;                                                                                                                                       \
            }                                                                                                                                                \
            succ = expected;                                                                                                                                 \
        }                                                                                                                                                    \
                                                                                                                                                             \
        /* Phase 3: Physical unlinking via find. */                                                                                                          \
        _SKIP_PATH_CLEAR(path);                                                                                                                              \
        _skip_locate_##decl(slist, query, path);                                                                                                             \
                                                                                                                                                             \
        /* Update backward pointer hint (best-effort). */                                                                                                    \
        {                                                                                                                                                    \
            decl##_node_t *unmarked_succ = _SKIP_UNMARK(_skip_atomic_load(&node->field.sle_levels[0].next, memory_order_acquire));                           \
            if (unmarked_succ != slist->slh_tail && unmarked_succ != NULL) {                                                                                 \
                decl##_node_t *exp_prev = node;                                                                                                              \
                decl##_node_t *new_prev = _skip_atomic_load(&node->field.sle_prev, memory_order_relaxed);                                                    \
                _skip_atomic_cas_strong(&unmarked_succ->field.sle_prev, &exp_prev, new_prev, memory_order_relaxed, memory_order_relaxed);                    \
            }                                                                                                                                                \
            if (unmarked_succ == slist->slh_tail) {                                                                                                          \
                decl##_node_t *exp_prev = node;                                                                                                              \
                decl##_node_t *new_prev = _skip_atomic_load(&node->field.sle_prev, memory_order_relaxed);                                                    \
                _skip_atomic_cas_strong(&slist->slh_tail->field.sle_prev, &exp_prev, new_prev, memory_order_relaxed, memory_order_relaxed);                  \
            }                                                                                                                                                \
        }                                                                                                                                                    \
                                                                                                                                                             \
        /* Decrement list length. */                                                                                                                         \
        _skip_atomic_fetch_sub(&slist->slh_length, 1, memory_order_relaxed);                                                                                 \
                                                                                                                                                             \
        /* Shrink head height if top levels are now empty. */                                                                                                \
        {                                                                                                                                                    \
            size_t h = _skip_atomic_load(&slist->slh_head->field.sle_height, memory_order_acquire);                                                          \
            while (h > 1) {                                                                                                                                  \
                decl##_node_t *top_next = _skip_atomic_load(&slist->slh_head->field.sle_levels[h - 1].next, memory_order_acquire);                           \
                if (top_next != slist->slh_tail)                                                                                                             \
                    break;                                                                                                                                   \
                if (_skip_atomic_cas_weak(&slist->slh_head->field.sle_height, &h, h - 1, memory_order_release, memory_order_acquire)) {                      \
                    _skip_atomic_store(&slist->slh_tail->field.sle_height, h - 1, memory_order_release);                                                     \
                    h = h - 1;                                                                                                                               \
                }                                                                                                                                            \
            }                                                                                                                                                \
        }                                                                                                                                                    \
                                                                                                                                                             \
        /* Free the node.  When EBR is attached, defer freeing via the                                                                                       \
           retire list; otherwise free immediately (single-threaded). */                                                                                     \
        if (slist->slh_ebr != NULL && slist->slh_ebr_retire != NULL) {                                                                                       \
            slist->slh_ebr_retire(slist->slh_ebr, slist, node);                                                                                              \
        } else {                                                                                                                                             \
            slist->slh_fns.free_entry(node);                                                                                                                 \
            pfree(node);                                                                                                                                      \
        }                                                                                                                                                    \
                                                                                                                                                             \
        _skip_adjust_hit_counts_##decl(slist);                                                                                                               \
                                                                                                                                                             \
        return 0;                                                                                                                                            \
    }                                                                                                                                                        \
                                                                                                                                                             \
    /**                                                                                                                                                      \
     * -- skip_free_                                                                                                                                         \
     *                                                                                                                                                       \
     * Release all nodes and their associated heap objects.  The list reference                                                                              \
     * is no longer valid after this call. To make it valid again call _init().                                                                              \
     */                                                                                                                                                      \
    void prefix##skip_free_##decl(decl##_t *slist)                                                                                                           \
    {                                                                                                                                                        \
        if (slist == NULL)                                                                                                                                   \
            return;                                                                                                                                          \
                                                                                                                                                             \
        if (slist->slh_snap.pres_era > 0 && slist->slh_fns.snapshot_release)                                                                                 \
            slist->slh_fns.snapshot_release(slist);                                                                                                          \
                                                                                                                                                             \
        prefix##skip_release_##decl(slist);                                                                                                                  \
                                                                                                                                                             \
        pfree(slist->slh_head);                                                                                                                               \
        pfree(slist->slh_tail);                                                                                                                               \
    }

/*
 * Epoch-Based Reclamation (EBR) for safe memory reclamation in lock-free
 * skip lists.  When multiple threads perform concurrent operations, deleted
 * nodes cannot be freed immediately because other threads may still hold
 * references.  EBR defers freeing until it is safe: a global epoch advances
 * when all active threads have observed the current epoch, and nodes retired
 * two epochs ago can then be reclaimed.
 *
 * Usage:
 *   1. Declare EBR with SKIPLIST_DECL_EBR(decl, prefix) after SKIPLIST_DECL.
 *   2. Allocate and init an EBR state: _skip_ebr_##decl##_t ebr;
 *      prefix##skip_ebr_init_##decl(&ebr);
 *   3. Attach it to a list: prefix##skip_ebr_attach_##decl(&slist, &ebr);
 *   4. Each thread registers: int tid = prefix##skip_ebr_register_##decl(&ebr);
 *   5. Before accessing the list: prefix##skip_ebr_pin_##decl(&ebr, tid);
 *   6. After done: prefix##skip_ebr_unpin_##decl(&ebr, tid);
 *   7. Deletions automatically retire nodes through the EBR retire lists.
 */

/**
 * SKIPLIST_EBR_MAX_THREADS -- Maximum number of concurrent threads for EBR.
 *
 * Defines the upper bound on how many threads can register with the
 * Epoch-Based Reclamation (EBR) subsystem.  Each registered thread gets
 * a per-thread state slot (local epoch + active flag).  Increasing this
 * value uses more memory but allows more concurrent threads.
 *
 * Usage:
 *   #define SKIPLIST_EBR_MAX_THREADS 256   // before including sl.h
 *   #include "sl.h"
 */
#ifndef SKIPLIST_EBR_MAX_THREADS
#define SKIPLIST_EBR_MAX_THREADS 128
#endif

#ifndef SKIPLIST_SINGLE_THREADED

/**
 * SKIPLIST_DECL_EBR(decl, prefix) -- Generate Epoch-Based Reclamation (EBR)
 * for safe memory reclamation in lock-free skip lists.
 *
 * When multiple threads perform concurrent operations, deleted nodes cannot
 * be freed immediately because other threads may still hold references.
 * EBR defers freeing until it is safe: a global epoch advances when all
 * active threads have observed the current epoch, and nodes retired two
 * epochs ago can then be reclaimed.
 *
 * This macro must be invoked AFTER SKIPLIST_DECL for the same decl/prefix.
 * It is incompatible with SKIPLIST_SINGLE_THREADED mode.
 *
 * @param decl    The skiplist type name (must match SKIPLIST_DECL)
 * @param prefix  The function prefix (must match SKIPLIST_DECL)
 *
 * Usage:
 *   SKIPLIST_DECL_EBR(my_list, api_)
 *
 *   // In main:
 *   _skip_ebr_my_list_t ebr;
 *   api_skip_ebr_init_my_list(&ebr);
 *   api_skip_ebr_attach_my_list(&slist, &ebr);
 *
 *   // Per thread:
 *   int tid = api_skip_ebr_register_my_list(&ebr);
 *   api_skip_ebr_pin_my_list(&ebr, tid);    // enter critical section
 *   // ... read/write operations on the skiplist ...
 *   api_skip_ebr_unpin_my_list(&ebr, tid);  // leave critical section
 *
 *   // At shutdown:
 *   api_skip_ebr_drain_my_list(&ebr);
 *
 * Generated functions:
 *   void prefix##skip_ebr_init_##decl(_skip_ebr_##decl##_t *ebr)
 *          -- Initialize EBR state.  Must be called first.
 *   int  prefix##skip_ebr_register_##decl(_skip_ebr_##decl##_t *ebr)
 *          -- Register a thread.  Returns thread ID (0-based), or -1 if full.
 *   void prefix##skip_ebr_pin_##decl(_skip_ebr_##decl##_t *ebr, int tid)
 *          -- Enter a critical section (pin).  Nodes will not be freed while pinned.
 *   void prefix##skip_ebr_unpin_##decl(_skip_ebr_##decl##_t *ebr, int tid)
 *          -- Leave a critical section (unpin).
 *   void prefix##skip_ebr_retire_##decl(_skip_ebr_##decl##_t *ebr, decl##_t *slist, decl##_node_t *node)
 *          -- Defer freeing a node until safe.  Called automatically by skip_remove_node_.
 *   void prefix##skip_ebr_attach_##decl(decl##_t *slist, _skip_ebr_##decl##_t *ebr)
 *          -- Attach EBR to a skiplist.  Removals will use deferred freeing.
 *   void prefix##skip_ebr_drain_##decl(_skip_ebr_##decl##_t *ebr)
 *          -- Force-drain all retire lists (call only when no threads are active).
 */
#define SKIPLIST_DECL_EBR(decl, prefix)                                                                                      \
                                                                                                                             \
    /* Per-thread EBR state. */                                                                                              \
    typedef struct _skip_ebr_thread_##decl {                                                                                 \
        _SKIP_ATOMIC(uint64_t) local_epoch;                                                                                  \
        _SKIP_ATOMIC(int) active;                                                                                            \
    } _skip_ebr_thread_##decl##_t;                                                                                           \
                                                                                                                             \
    /* A retired node waiting to be freed. */                                                                                \
    typedef struct _skip_ebr_retired_##decl {                                                                                \
        decl##_node_t *node;                                                                                                 \
        decl##_t *slist;                                                                                                     \
        struct _skip_ebr_retired_##decl *next;                                                                               \
    } _skip_ebr_retired_##decl##_t;                                                                                          \
                                                                                                                             \
    /* The EBR state. */                                                                                                     \
    typedef struct _skip_ebr_##decl {                                                                                        \
        _SKIP_ATOMIC(uint64_t) global_epoch;                                                                                 \
        _skip_ebr_thread_##decl##_t threads[SKIPLIST_EBR_MAX_THREADS];                                                       \
        _SKIP_ATOMIC(int) thread_count;                                                                                      \
        /* Three retire lists, one per epoch bucket (epoch % 3). */                                                          \
        _skip_ebr_retired_##decl##_t *retire_lists[3];                                                                       \
        _SKIP_ATOMIC(int) retire_locks[3];                                                                                   \
    } _skip_ebr_##decl##_t;                                                                                                  \
                                                                                                                             \
    /* Spinlock helpers for retire list access.                                                                              \
       NOTE: The retire path is NOT lock-free; a per-thread Treiber stack                                                    \
       would eliminate this bottleneck under high contention. */                                                             \
    static void _skip_ebr_lock_##decl(_SKIP_ATOMIC(int) * lock)                                                              \
    {                                                                                                                        \
        while (_skip_atomic_exchange(lock, 1, memory_order_acquire) != 0) {                                                  \
            /* spin */                                                                                                       \
        }                                                                                                                    \
    }                                                                                                                        \
                                                                                                                             \
    static void _skip_ebr_unlock_##decl(_SKIP_ATOMIC(int) * lock)                                                            \
    {                                                                                                                        \
        _skip_atomic_store(lock, 0, memory_order_release);                                                                   \
    }                                                                                                                        \
                                                                                                                             \
    /* Forward declaration for try_advance. */                                                                               \
    static void _skip_ebr_try_advance_##decl(_skip_ebr_##decl##_t *ebr);                                                     \
                                                                                                                             \
    /**                                                                                                                      \
     * -- skip_ebr_init_                                                                                                     \
     *                                                                                                                       \
     * Initialize EBR state.  Must be called before any other EBR operation.                                                 \
     */                                                                                                                      \
    void prefix##skip_ebr_init_##decl(_skip_ebr_##decl##_t *ebr)                                                             \
    {                                                                                                                        \
        memset(ebr, 0, sizeof(*ebr));                                                                                        \
        _skip_atomic_store(&ebr->global_epoch, 1, memory_order_relaxed);                                                     \
    }                                                                                                                        \
                                                                                                                             \
    /**                                                                                                                      \
     * -- skip_ebr_register_                                                                                                 \
     *                                                                                                                       \
     * Register a thread for EBR participation.  Returns a thread ID                                                         \
     * (0-based).  Must be called once per thread before pin/unpin.                                                          \
     * Returns -1 if the maximum number of threads has been reached.                                                         \
     */                                                                                                                      \
    int prefix##skip_ebr_register_##decl(_skip_ebr_##decl##_t *ebr)                                                          \
    {                                                                                                                        \
        int tid = _skip_atomic_fetch_add(&ebr->thread_count, 1, memory_order_relaxed);                                       \
        if (tid >= SKIPLIST_EBR_MAX_THREADS) {                                                                               \
            _skip_atomic_fetch_sub(&ebr->thread_count, 1, memory_order_relaxed);                                             \
            return -1;                                                                                                       \
        }                                                                                                                    \
        _skip_atomic_store(&ebr->threads[tid].local_epoch, 0, memory_order_relaxed);                                         \
        _skip_atomic_store(&ebr->threads[tid].active, 0, memory_order_relaxed);                                              \
        return tid;                                                                                                          \
    }                                                                                                                        \
                                                                                                                             \
    /**                                                                                                                      \
     * -- skip_ebr_pin_                                                                                                      \
     *                                                                                                                       \
     * Enter a critical section.  The calling thread announces that it                                                       \
     * is reading the data structure and nodes must not be freed until                                                       \
     * it unpins.                                                                                                            \
     */                                                                                                                      \
    void prefix##skip_ebr_pin_##decl(_skip_ebr_##decl##_t *ebr, int tid)                                                     \
    {                                                                                                                        \
        /* Announce active BEFORE reading global_epoch.  The seq_cst                                                         \
           fence pairs with the fence in try_advance() so that: if                                                           \
           try_advance reads active==0 and skips us, it committed                                                            \
           its epoch load before our fence, and our subsequent epoch                                                         \
           load will see that committed value (or later).  This is                                                           \
           the crossbeam-epoch pattern that prevents premature                                                               \
           reclamation. */                                                                                                   \
        _skip_atomic_store(&ebr->threads[tid].active, 1, memory_order_relaxed);                                              \
        _skip_atomic_thread_fence(memory_order_seq_cst);                                                                     \
        uint64_t ge = _skip_atomic_load(&ebr->global_epoch, memory_order_relaxed);                                           \
        _skip_atomic_store(&ebr->threads[tid].local_epoch, ge, memory_order_release);                                        \
    }                                                                                                                        \
                                                                                                                             \
    /**                                                                                                                      \
     * -- skip_ebr_unpin_                                                                                                    \
     *                                                                                                                       \
     * Exit a critical section.  The calling thread is no longer reading                                                     \
     * the data structure.                                                                                                   \
     */                                                                                                                      \
    void prefix##skip_ebr_unpin_##decl(_skip_ebr_##decl##_t *ebr, int tid)                                                   \
    {                                                                                                                        \
        _skip_atomic_store(&ebr->threads[tid].active, 0, memory_order_release);                                              \
    }                                                                                                                        \
                                                                                                                             \
    /**                                                                                                                      \
     * -- skip_ebr_retire_                                                                                                   \
     *                                                                                                                       \
     * Defer freeing a node.  The node is placed on a retire list tagged                                                     \
     * with the current epoch.  After all active threads have advanced                                                       \
     * past this epoch, the node will be reclaimed.                                                                          \
     */                                                                                                                      \
    void prefix##skip_ebr_retire_##decl(_skip_ebr_##decl##_t *ebr, decl##_t *slist, decl##_node_t *node)                     \
    {                                                                                                                        \
        uint64_t epoch = _skip_atomic_load(&ebr->global_epoch, memory_order_acquire);                                        \
        int bucket = (int)(epoch % 3);                                                                                       \
                                                                                                                             \
        _skip_ebr_retired_##decl##_t *entry = _skip_ebr_retired_##decl##_t *)palloc(sizeof(_skip_ebr_retired_##decl##_t));  \
        if (entry == NULL)                                                                                                   \
            return; /* best-effort; leak rather than crash */                                                                \
        entry->node = node;                                                                                                  \
        entry->slist = slist;                                                                                                \
                                                                                                                             \
        _skip_ebr_lock_##decl(&ebr->retire_locks[bucket]);                                                                   \
        entry->next = ebr->retire_lists[bucket];                                                                             \
        ebr->retire_lists[bucket] = entry;                                                                                   \
        _skip_ebr_unlock_##decl(&ebr->retire_locks[bucket]);                                                                 \
                                                                                                                             \
        /* Attempt to advance the epoch and reclaim old nodes. */                                                            \
        _skip_ebr_try_advance_##decl(ebr);                                                                                   \
    }                                                                                                                        \
                                                                                                                             \
    /**                                                                                                                      \
     * -- _skip_ebr_try_advance_                                                                                             \
     *                                                                                                                       \
     * Check whether all active threads have observed the current global                                                     \
     * epoch.  If so, advance the epoch and free all nodes retired two                                                       \
     * epochs ago.                                                                                                           \
     */                                                                                                                      \
    static void _skip_ebr_try_advance_##decl(_skip_ebr_##decl##_t *ebr)                                                      \
    {                                                                                                                        \
        uint64_t cur_epoch = _skip_atomic_load(&ebr->global_epoch, memory_order_acquire);                                    \
        /* Pair with the seq_cst fence in pin(): guarantees that if a                                                        \
           thread stored active=1 and then read global_epoch <=                                                              \
           cur_epoch, we will observe its active flag below. */                                                              \
        _skip_atomic_thread_fence(memory_order_seq_cst);                                                                     \
        int tc = _skip_atomic_load(&ebr->thread_count, memory_order_acquire);                                                \
                                                                                                                             \
        /* Check: every active thread must have local_epoch >= cur_epoch. */                                                 \
        for (int i = 0; i < tc; i++) {                                                                                       \
            if (_skip_atomic_load(&ebr->threads[i].active, memory_order_acquire)) {                                          \
                uint64_t le = _skip_atomic_load(&ebr->threads[i].local_epoch, memory_order_acquire);                         \
                if (le < cur_epoch)                                                                                          \
                    return; /* at least one thread hasn't caught up */                                                       \
            }                                                                                                                \
        }                                                                                                                    \
                                                                                                                             \
        /* All active threads are up to date; try to bump the epoch. */                                                      \
        uint64_t new_epoch = cur_epoch + 1;                                                                                  \
        if (!_skip_atomic_cas_strong(&ebr->global_epoch, &cur_epoch, new_epoch, memory_order_acq_rel, memory_order_relaxed)) \
            return; /* another thread advanced it first */                                                                   \
                                                                                                                             \
        /* Reclaim the bucket that is now 2 epochs behind.                                                                   \
           new_epoch - 2 is the epoch whose retire list is safe to free                                                      \
           because all threads have since observed at least cur_epoch. */                                                    \
        if (new_epoch < 2)                                                                                                   \
            return; /* not enough epochs have passed yet */                                                                  \
        int old_bucket = (int)((new_epoch - 2) % 3);                                                                         \
                                                                                                                             \
        _skip_ebr_lock_##decl(&ebr->retire_locks[old_bucket]);                                                               \
        _skip_ebr_retired_##decl##_t *list = ebr->retire_lists[old_bucket];                                                  \
        ebr->retire_lists[old_bucket] = NULL;                                                                                \
        _skip_ebr_unlock_##decl(&ebr->retire_locks[old_bucket]);                                                             \
                                                                                                                             \
        while (list != NULL) {                                                                                               \
            _skip_ebr_retired_##decl##_t *cur = list;                                                                        \
            list = cur->next;                                                                                                \
            cur->slist->slh_fns.free_entry(cur->node);                                                                       \
            pfree(cur->node);                                                                                                 \
            pfree(cur);                                                                                                       \
        }                                                                                                                    \
    }                                                                                                                        \
                                                                                                                             \
    /**                                                                                                                      \
     * -- _skip_ebr_retire_cb_                                                                                               \
     *                                                                                                                       \
     * Type-erased callback that bridges the void* function pointer                                                          \
     * stored in slh_ebr_retire to the typed retire function.                                                                \
     */                                                                                                                      \
    static void _skip_ebr_retire_cb_##decl(void *ebr_opaque, decl##_t *slist, decl##_node_t *node)                           \
    {                                                                                                                        \
        prefix##skip_ebr_retire_##decl((_skip_ebr_##decl##_t *)ebr_opaque, slist, node);                                     \
    }                                                                                                                        \
                                                                                                                             \
    /**                                                                                                                      \
     * -- skip_ebr_attach_                                                                                                   \
     *                                                                                                                       \
     * Attach an initialized EBR state to a skiplist.  After this call,                                                      \
     * skip_remove_node_ will defer node freeing through EBR rather                                                          \
     * than calling free() immediately.                                                                                      \
     */                                                                                                                      \
    void prefix##skip_ebr_attach_##decl(decl##_t *slist, _skip_ebr_##decl##_t *ebr)                                          \
    {                                                                                                                        \
        slist->slh_ebr = (void *)ebr;                                                                                        \
        slist->slh_ebr_retire = _skip_ebr_retire_cb_##decl;                                                                  \
    }                                                                                                                        \
                                                                                                                             \
    /**                                                                                                                      \
     * -- skip_ebr_drain_                                                                                                    \
     *                                                                                                                       \
     * Force-drain all retire lists, freeing every deferred node                                                             \
     * regardless of epoch.  Call only when no threads are accessing                                                         \
     * the data structure (e.g., during shutdown).                                                                           \
     */                                                                                                                      \
    void prefix##skip_ebr_drain_##decl(_skip_ebr_##decl##_t *ebr)                                                            \
    {                                                                                                                        \
        for (int b = 0; b < 3; b++) {                                                                                        \
            _skip_ebr_lock_##decl(&ebr->retire_locks[b]);                                                                    \
            _skip_ebr_retired_##decl##_t *list = ebr->retire_lists[b];                                                       \
            ebr->retire_lists[b] = NULL;                                                                                     \
            _skip_ebr_unlock_##decl(&ebr->retire_locks[b]);                                                                  \
                                                                                                                             \
            while (list != NULL) {                                                                                           \
                _skip_ebr_retired_##decl##_t *cur = list;                                                                    \
                list = cur->next;                                                                                            \
                cur->slist->slh_fns.free_entry(cur->node);                                                                   \
                pfree(cur->node);                                                                                             \
                pfree(cur);                                                                                                   \
            }                                                                                                                \
        }                                                                                                                    \
    }

#endif							/* !SKIPLIST_SINGLE_THREADED */

/**
 * SKIPLIST_DECL_SNAPSHOTS(decl, prefix, field) -- Generate MVCC point-in-time
 * snapshot and restore support.
 *
 * Adds the ability to take point-in-time snapshots of a skiplist and later
 * restore the list to any previously snapshotted state.  This implements a
 * form of Multi-Version Concurrency Control (MVCC): when a snapshot is
 * active, mutations (insert, remove, update) automatically preserve the
 * old version of affected nodes in a singly-linked preservation list.
 *
 * Snapshots are identified by an era number.  Multiple snapshots can be
 * taken; restoring to era N discards all changes made after era N and
 * re-inserts the preserved nodes from that era.
 *
 * This macro must be invoked AFTER SKIPLIST_DECL for the same decl/prefix.
 * The archive_entry_blk from SKIPLIST_DECL is used to deep-copy node data
 * during preservation.
 *
 * NOTE: Snapshots are a single-threaded feature.  Do not use with concurrent
 * lock-free operations.
 *
 * @param decl    The skiplist type name (must match SKIPLIST_DECL)
 * @param prefix  The function prefix (must match SKIPLIST_DECL)
 * @param field   The SKIPLIST_ENTRY field name (must match SKIPLIST_DECL)
 *
 * Usage:
 *   SKIPLIST_DECL_SNAPSHOTS(my_list, api_, entries)
 *
 *   // Enable snapshots on a list:
 *   api_skip_snapshots_init_my_list(&slist);
 *
 *   // ... insert some data ...
 *   uint64_t era = api_skip_snapshot_my_list(&slist);
 *   // ... modify the list ...
 *   api_skip_restore_snapshot_my_list(&slist, era);  // revert to snapshot
 *   api_skip_release_snapshots_my_list(&slist);      // free preserved nodes
 *
 * Generated functions:
 *   void     prefix##skip_snapshots_init_##decl(decl##_t *slist)
 *              -- Initialize snapshot support.  Must be called before taking snapshots.
 *   uint64_t prefix##skip_snapshot_##decl(decl##_t *slist)
 *              -- Take a snapshot.  Returns the era number identifying this snapshot.
 *   decl##_t *prefix##skip_restore_snapshot_##decl(decl##_t *slist, size_t era)
 *              -- Restore the list to the state at the given era.
 *   void     prefix##skip_release_snapshots_##decl(decl##_t *slist)
 *              -- Release all preserved snapshot data and disable snapshots.
 */
#define SKIPLIST_DECL_SNAPSHOTS(decl, prefix, field)                                                                    \
                                                                                                                        \
    /**                                                                                                                 \
     * -- skip_snapshot_                                                                                                \
     *                                                                                                                  \
     * A snapshot is a read-only view of a Skiplist at a point in time.  Once                                           \
     * taken, a snapshot must be restored or released. Any number of snapshots                                          \
     * can be created. Return the `era` of the snapshot.                                                                \
     */                                                                                                                 \
    uint64_t prefix##skip_snapshot_##decl(decl##_t *slist)                                                              \
    {                                                                                                                   \
        if (slist == NULL)                                                                                              \
            return 0;                                                                                                   \
                                                                                                                        \
        slist->slh_snap.pres_era = ++slist->slh_snap.cur_era;                                                           \
        slist->slh_snap.cur_era++;                                                                                      \
        return slist->slh_snap.pres_era;                                                                                \
    }                                                                                                                   \
                                                                                                                        \
    /**                                                                                                                 \
     * -- skip_release_snapshots_                                                                                       \
     *                                                                                                                  \
     */                                                                                                                 \
    void prefix##skip_release_snapshots_##decl(decl##_t *slist)                                                         \
    {                                                                                                                   \
        decl##_node_t *node, *next;                                                                                     \
                                                                                                                        \
        if (slist == NULL)                                                                                              \
            return;                                                                                                     \
                                                                                                                        \
        if (slist->slh_snap.pres_era == 0)                                                                              \
            return;                                                                                                     \
                                                                                                                        \
        node = slist->slh_snap.pres;                                                                                    \
        while (node) {                                                                                                  \
            next = node->field.sle_levels[0].next;                                                                      \
            prefix##skip_free_node_##decl(slist, node);                                                                 \
            node = next;                                                                                                \
        }                                                                                                               \
        slist->slh_snap.pres = NULL;                                                                                    \
        slist->slh_snap.pres_era = 0;                                                                                   \
    }                                                                                                                   \
                                                                                                                        \
    /**                                                                                                                 \
     * -- _skip_preserve_node_                                                                                          \
     *                                                                                                                  \
     * Preserve given node in the slh_snap.pres list.                                                                   \
     *                                                                                                                  \
     * ALGORITHM:                                                                                                       \
     *   a) allocate a new node                                                                                         \
     *   b) copy the node into the new node                                                                             \
     *   c) as necessary, allocate/copy any user-supplied items.                                                        \
     *   d) determine if this is a duplicate, if so in (d) we set                                                       \
     *      the sle.next[1] field to 0x1 as a reminder to re-insert                                                     \
     *      this element as a duplicate in the restore function.                                                        \
     *   e) zero out the next sle.prev/next[] pointers                                                                  \
     *   f) mark as duplicate, set sle.next[1] = 0x1                                                                    \
     *   g) insert the node's copy into the slh_pres singly-linked                                                      \
     *      list.                                                                                                       \
     */                                                                                                                 \
    static int _skip_preserve_node_##decl(decl##_t *slist, const decl##_node_t *src, decl##_node_t **preserved)         \
    {                                                                                                                   \
        int rc = 0;                                                                                                     \
        decl##_node_t *dest, *is_dup = 0;                                                                               \
                                                                                                                        \
        if (slist == NULL || src == NULL)                                                                               \
            return 0;                                                                                                   \
                                                                                                                        \
        /* Never preserve the head or the tail. */                                                                      \
        if (src == slist->slh_head || src == slist->slh_tail)                                                           \
            return 0;                                                                                                   \
                                                                                                                        \
        /* If the era into which the node `src` was born preceeded the latest                                           \
           snapshot era, then we need to preserve the older version of this                                             \
           node.  Said another way, we preserve anything with an era that is                                            \
           less than the slh_snap.cur_era. */                                                                           \
        if (src->field.sle_era > slist->slh_snap.pres_era)                                                              \
            return 0;                                                                                                   \
                                                                                                                        \
        /* (a) alloc, ... */                                                                                            \
        size_t sle_arr_sz = sizeof(struct _skiplist_##decl##_level) * SKIPLIST_MAX_HEIGHT;                              \
        rc = prefix##skip_alloc_node_##decl(&dest);                                                                     \
        if (rc)                                                                                                         \
            return rc;                                                                                                  \
                                                                                                                        \
        /* (b) shallow copy, copied sle_levels pointer is to the src list, so                                           \
           update that to point to the offset in this heap object, ... */                                               \
        memcpy(dest, src, sizeof(decl##_node_t) + sle_arr_sz);                                                          \
        dest->field.sle_levels = (struct _skiplist_##decl##_level *)((uintptr_t)dest + sizeof(decl##_node_t));          \
                                                                                                                        \
        /* (c) then user-supplied copy */                                                                               \
        rc = slist->slh_fns.archive_entry(dest, src);                                                                   \
        if (rc) {                                                                                                       \
            prefix##skip_free_node_##decl(slist, dest);                                                                 \
            return rc;                                                                                                  \
        }                                                                                                               \
                                                                                                                        \
        /* (d) is this a duplicate? */                                                                                  \
        if (_skip_compare_nodes_##decl(slist, dest, dest->field.sle_levels[0].next, slist->slh_aux) == 0 ||             \
            _skip_compare_nodes_##decl(slist, dest, dest->field.sle_prev, slist->slh_aux) == 0)                         \
            is_dup = (decl##_node_t *)0x1;                                                                              \
                                                                                                                        \
        /* (e) zero out the next pointers */                                                                            \
        dest->field.sle_prev = NULL;                                                                                    \
        _SKIP_ALL_ENTRIES_B2T(field, dest)                                                                              \
        {                                                                                                               \
            dest->field.sle_levels[lvl].next = NULL;                                                                    \
        }                                                                                                               \
                                                                                                                        \
        /* (f) set duplicate flag -- reuses sle_levels[1].next as a boolean;                                           \
           safe because all nodes are allocated with SKIPLIST_MAX_HEIGHT levels. */                                     \
        dest->field.sle_levels[1].next = is_dup;                                                                        \
                                                                                                                        \
        /* (g) insert node into slh_pres list at head */                                                                \
        if (slist->slh_snap.pres == NULL) {                                                                             \
            dest->field.sle_levels[0].next = NULL;                                                                      \
            slist->slh_snap.pres = dest;                                                                                \
        } else {                                                                                                        \
            /* The next[0] pointer forms the singly-linked list when                                                    \
               preserved. */                                                                                            \
            dest->field.sle_levels[0].next = slist->slh_snap.pres;                                                      \
            slist->slh_snap.pres = dest;                                                                                \
        }                                                                                                               \
                                                                                                                        \
        if (preserved)                                                                                                  \
            *preserved = dest;                                                                                          \
                                                                                                                        \
        rc = 1;                                                                                                         \
        return -rc;                                                                                                     \
    }                                                                                                                   \
                                                                                                                        \
    /**                                                                                                                 \
     * -- skip_restore_snapshot_                                                                                        \
     *                                                                                                                  \
     * Restores the Skiplist to generation `era`.  Once you restore `era` you                                           \
     * can no longer restore any [era, current era] only those earlier than                                             \
     * era.                                                                                                             \
     *                                                                                                                  \
     * ALGORITHM:                                                                                                       \
     * iterate over the preserved nodes (slist->slh_snap.pres)                                                          \
     *  a) remove/free nodes with node->era > era from slist                                                            \
     *  b) remove/free nodes > era from slh_pres                                                                        \
     *  c) restore nodes == era by...                                                                                   \
     *     i) remove node from slh_pres list                                                                            \
     *     ii) _insert(node) or                                                                                         \
     *         _insert_dup() if node->field.sle_levels[1].next != 0 (clear that)                                        \
     *  d) set slist's era to `era`                                                                                     \
     *                                                                                                                  \
     * NOTES:                                                                                                           \
     * - Starting with slh_pres, the `node->field.sle_levels[0].next` form a                                            \
     *   singly-linked list.                                                                                            \
     */                                                                                                                 \
    decl##_t *prefix##skip_restore_snapshot_##decl(decl##_t *slist, size_t era)                                         \
    {                                                                                                                   \
        size_t i, cur_era, n_remove = 0, n_discard = 0, n_restore = 0;                                                  \
        decl##_node_t *node, *next_node;                                                                                \
                                                                                                                        \
        if (slist == NULL)                                                                                              \
            return NULL;                                                                                                \
                                                                                                                        \
        if (era == 0 || era >= slist->slh_snap.cur_era)                                                                 \
            return slist;                                                                                               \
                                                                                                                        \
        cur_era = slist->slh_snap.cur_era;                                                                              \
                                                                                                                        \
        /* (a) Collect nodes to remove from the active list (era > target).                                             \
           We decouple iteration from mutation to avoid use-after-free. */                                              \
        decl##_node_t **to_remove = NULL;                                                                               \
        size_t cap_remove = 16;                                                                                         \
        to_remove = (decl##_node_t **)palloc(sizeof(decl##_node_t *) * cap_remove);                                     \
        if (to_remove == NULL)                                                                                          \
            return NULL;                                                                                                \
                                                                                                                        \
        SKIPLIST_FOREACH_H2T(decl, prefix, field, slist, node, i)                                                       \
        {                                                                                                               \
            (void)i;                                                                                                    \
            if (node->field.sle_era > era) {                                                                            \
                if (n_remove >= cap_remove) {                                                                           \
                    cap_remove *= 2;                                                                                    \
                    decl##_node_t **tmp = (decl##_node_t **)repalloc(to_remove, sizeof(decl##_node_t *) * cap_remove);   \
                    if (tmp == NULL) {                                                                                  \
                        pfree(to_remove);                                                                                \
                        return NULL;                                                                                    \
                    }                                                                                                   \
                    to_remove = tmp;                                                                                    \
                }                                                                                                       \
                to_remove[n_remove++] = node;                                                                           \
            }                                                                                                           \
        }                                                                                                               \
                                                                                                                        \
        /* Now remove them. */                                                                                          \
        for (i = 0; i < n_remove; i++)                                                                                  \
            prefix##skip_remove_node_##decl(slist, to_remove[i]);                                                       \
        pfree(to_remove);                                                                                                \
                                                                                                                        \
        /* (b) & (c) Walk the preserved list, collecting nodes to discard                                               \
           (era > target) and nodes to restore (era == target). */                                                      \
        decl##_node_t **to_discard = NULL;                                                                              \
        decl##_node_t **to_restore = NULL;                                                                              \
        size_t cap_discard = 16, cap_restore = 16;                                                                      \
        to_discard = (decl##_node_t **)palloc(sizeof(decl##_node_t *) * cap_discard);                                   \
        to_restore = (decl##_node_t **)palloc(sizeof(decl##_node_t *) * cap_restore);                                   \
        if (to_discard == NULL || to_restore == NULL) {                                                                 \
            pfree(to_discard);                                                                                           \
            pfree(to_restore);                                                                                           \
            return NULL;                                                                                                \
        }                                                                                                               \
                                                                                                                        \
        node = slist->slh_snap.pres;                                                                                    \
        while (node) {                                                                                                  \
            next_node = node->field.sle_levels[0].next;                                                                 \
            if (node->field.sle_era > era) {                                                                            \
                if (n_discard >= cap_discard) {                                                                         \
                    cap_discard *= 2;                                                                                   \
                    decl##_node_t **tmp = (decl##_node_t **)repalloc(to_discard, sizeof(decl##_node_t *) * cap_discard); \
                    if (tmp == NULL) {                                                                                  \
                        pfree(to_discard);                                                                               \
                        pfree(to_restore);                                                                               \
                        return NULL;                                                                                    \
                    }                                                                                                   \
                    to_discard = tmp;                                                                                   \
                }                                                                                                       \
                to_discard[n_discard++] = node;                                                                         \
            } else if (node->field.sle_era <= era) {                                                                    \
                if (n_restore >= cap_restore) {                                                                         \
                    cap_restore *= 2;                                                                                   \
                    decl##_node_t **tmp = (decl##_node_t **)repalloc(to_restore, sizeof(decl##_node_t *) * cap_restore); \
                    if (tmp == NULL) {                                                                                  \
                        pfree(to_discard);                                                                               \
                        pfree(to_restore);                                                                               \
                        return NULL;                                                                                    \
                    }                                                                                                   \
                    to_restore = tmp;                                                                                   \
                }                                                                                                       \
                to_restore[n_restore++] = node;                                                                         \
            }                                                                                                           \
            node = next_node;                                                                                           \
        }                                                                                                               \
                                                                                                                        \
        /* (b) Remove and free preserved nodes newer than era. */                                                       \
        for (i = 0; i < n_discard; i++) {                                                                               \
            /* Unlink from the preserved singly-linked list. */                                                         \
            decl##_node_t **pp = &slist->slh_snap.pres;                                                                 \
            while (*pp && *pp != to_discard[i])                                                                         \
                pp = (decl##_node_t **)&(*pp)->field.sle_levels[0].next;                                                \
            if (*pp)                                                                                                    \
                *pp = to_discard[i]->field.sle_levels[0].next;                                                          \
            prefix##skip_free_node_##decl(slist, to_discard[i]);                                                        \
        }                                                                                                               \
        pfree(to_discard);                                                                                               \
                                                                                                                        \
        /* (c) Restore preserved nodes matching era. */                                                                 \
        for (i = 0; i < n_restore; i++) {                                                                               \
            /* Unlink from the preserved singly-linked list. */                                                         \
            decl##_node_t **pp = &slist->slh_snap.pres;                                                                 \
            while (*pp && *pp != to_restore[i])                                                                         \
                pp = (decl##_node_t **)&(*pp)->field.sle_levels[0].next;                                                \
            if (*pp)                                                                                                    \
                *pp = to_restore[i]->field.sle_levels[0].next;                                                          \
                                                                                                                        \
            node = to_restore[i];                                                                                       \
            node->field.sle_prev = NULL;                                                                                \
            if (node->field.sle_levels[1].next != 0) {                                                                  \
                node->field.sle_levels[1].next = NULL;                                                                  \
                prefix##skip_insert_dup_##decl(slist, node);                                                            \
            } else {                                                                                                    \
                prefix##skip_insert_##decl(slist, node);                                                                \
            }                                                                                                           \
        }                                                                                                               \
        pfree(to_restore);                                                                                               \
                                                                                                                        \
        /* (d) set list's era */                                                                                        \
        slist->slh_snap.pres_era = slist->slh_snap.pres == NULL ? 0 : cur_era;                                          \
                                                                                                                        \
        return slist;                                                                                                   \
    }                                                                                                                   \
                                                                                                                        \
    /**                                                                                                                 \
     * -- skip_snapshots_init_                                                                                          \
     *                                                                                                                  \
     * Adds the ability to take a single stable snapshot to the Skiplist API.                                           \
     */                                                                                                                 \
    void prefix##skip_snapshots_init_##decl(decl##_t *slist)                                                            \
    {                                                                                                                   \
        if (slist != NULL) {                                                                                            \
            slist->slh_fns.snapshot_preserve_node = _skip_preserve_node_##decl;                                         \
            slist->slh_fns.snapshot_release = prefix##skip_release_snapshots_##decl;                                    \
        }                                                                                                               \
    }

/**
 * SKIPLIST_DECL_ARCHIVE(decl, prefix, field, write_entry_blk, read_entry_blk)
 * -- Generate binary serialization/deserialization for skiplists.
 *
 * Adds the ability to serialize a skiplist to a FILE stream and deserialize
 * it back.  The caller provides code blocks that handle reading/writing the
 * user-defined portion of each node.
 *
 * This macro must be invoked AFTER SKIPLIST_DECL for the same decl/prefix.
 *
 * @param decl             The skiplist type name (must match SKIPLIST_DECL)
 * @param prefix           The function prefix (must match SKIPLIST_DECL)
 * @param field            The SKIPLIST_ENTRY field name (must match SKIPLIST_DECL)
 * @param write_entry_blk  Code block to serialize a node.  Receives
 *                         (decl##_node_t *node, uint8_t *buf, uint64_t *bytes).
 *                         Write node data into buf and set bytes to the number
 *                         of bytes written.
 * @param read_entry_blk   Code block to deserialize a node.  Receives
 *                         (decl##_node_t *node, uint8_t *buf, uint64_t bytes).
 *                         Read node data from buf.
 *
 * Binary format:
 *   [4 bytes] magic "SKPL"
 *   [4 bytes] version (1, little-endian uint32)
 *   [8 bytes] node count (little-endian uint64)
 *   Per node:
 *     [8 bytes] entry size in bytes (little-endian uint64)
 *     [size bytes] entry data
 *
 * Usage:
 *   SKIPLIST_DECL_ARCHIVE(my_list, api_, entries,
 *       {
 *           memcpy(buf, &node->key, sizeof(node->key));
 *           memcpy(buf + sizeof(node->key), &node->value, sizeof(node->value));
 *           bytes = sizeof(node->key) + sizeof(node->value);
 *       },
 *       {
 *           memcpy(&node->key, buf, sizeof(node->key));
 *           memcpy(&node->value, buf + sizeof(node->key), sizeof(node->value));
 *       })
 *
 * Generated functions:
 *   int prefix##skip_serialize_##decl(decl##_t *slist, FILE *fp)
 *         -- Serialize the skiplist to fp.  Returns 0 on success, errno on failure.
 *   int prefix##skip_deserialize_##decl(decl##_t *slist, FILE *fp)
 *         -- Deserialize from fp into slist (must be initialized and empty).
 *            Returns 0 on success, errno on failure.
 */
/* Byte-order helpers for portable archive serialization (little-endian on wire). */
static inline void
_skip_write_le32(uint8_t *dst, uint32_t v)
{
	dst[0] = (uint8_t) (v);
	dst[1] = (uint8_t) (v >> 8);
	dst[2] = (uint8_t) (v >> 16);
	dst[3] = (uint8_t) (v >> 24);
}
static inline void
_skip_write_le64(uint8_t *dst, uint64_t v)
{
	dst[0] = (uint8_t) (v);
	dst[1] = (uint8_t) (v >> 8);
	dst[2] = (uint8_t) (v >> 16);
	dst[3] = (uint8_t) (v >> 24);
	dst[4] = (uint8_t) (v >> 32);
	dst[5] = (uint8_t) (v >> 40);
	dst[6] = (uint8_t) (v >> 48);
	dst[7] = (uint8_t) (v >> 56);
}
static inline uint32_t
_skip_read_le32(const uint8_t *src)
{
	return (uint32_t) src[0] | ((uint32_t) src[1] << 8) | ((uint32_t) src[2] << 16) | ((uint32_t) src[3] << 24);
}
static inline uint64_t
_skip_read_le64(const uint8_t *src)
{
	return (uint64_t) src[0] | ((uint64_t) src[1] << 8) | ((uint64_t) src[2] << 16) | ((uint64_t) src[3] << 24) | ((uint64_t) src[4] << 32) |
		((uint64_t) src[5] << 40) | ((uint64_t) src[6] << 48) | ((uint64_t) src[7] << 56);
}

#define SKIPLIST_DECL_ARCHIVE(decl, prefix, field, write_entry_blk, read_entry_blk)                 \
                                                                                                    \
    int prefix##skip_serialize_##decl(decl##_t *slist, FILE *fp)                                    \
    {                                                                                               \
        if (slist == NULL || fp == NULL)                                                            \
            return EINVAL;                                                                          \
                                                                                                    \
        /* Magic */                                                                                 \
        if (fwrite("SKPL", 1, 4, fp) != 4)                                                          \
            return EIO;                                                                             \
                                                                                                    \
        /* Version (little-endian) */                                                               \
        {                                                                                           \
            uint8_t vbuf[4];                                                                        \
            _skip_write_le32(vbuf, 1);                                                              \
            if (fwrite(vbuf, 1, 4, fp) != 4)                                                        \
                return EIO;                                                                         \
        }                                                                                           \
                                                                                                    \
        /* Node count (little-endian) */                                                            \
        {                                                                                           \
            uint8_t cbuf[8];                                                                        \
            uint64_t count = (uint64_t)_skip_atomic_load(&slist->slh_length, memory_order_relaxed); \
            _skip_write_le64(cbuf, count);                                                          \
            if (fwrite(cbuf, 1, 8, fp) != 8)                                                        \
                return EIO;                                                                         \
        }                                                                                           \
                                                                                                    \
        /* Per-node data */                                                                         \
        decl##_node_t *node;                                                                        \
        size_t i;                                                                                   \
        uint8_t entry_buf[4096];                                                                    \
        SKIPLIST_FOREACH_H2T(decl, prefix, field, slist, node, i)                                   \
        {                                                                                           \
            (void)i;                                                                                \
            uint8_t *buf = entry_buf;                                                               \
            uint64_t bytes = 0;                                                                     \
            const uint64_t bufsize = sizeof(entry_buf);                                             \
            (void)bufsize;                                                                          \
            write_entry_blk;                                                                        \
            if (bytes > sizeof(entry_buf))                                                          \
                return EOVERFLOW;                                                                   \
            {                                                                                       \
                uint8_t bbuf[8];                                                                    \
                _skip_write_le64(bbuf, bytes);                                                      \
                if (fwrite(bbuf, 1, 8, fp) != 8)                                                    \
                    return EIO;                                                                     \
            }                                                                                       \
            if (bytes > 0 && fwrite(buf, 1, (size_t)bytes, fp) != (size_t)bytes)                    \
                return EIO;                                                                         \
        }                                                                                           \
                                                                                                    \
        return 0;                                                                                   \
    }                                                                                               \
                                                                                                    \
    int prefix##skip_deserialize_##decl(decl##_t *slist, FILE *fp)                                  \
    {                                                                                               \
        if (slist == NULL || fp == NULL)                                                            \
            return EINVAL;                                                                          \
                                                                                                    \
        /* Magic */                                                                                 \
        char magic[4];                                                                              \
        if (fread(magic, 1, 4, fp) != 4)                                                            \
            return EIO;                                                                             \
        if (memcmp(magic, "SKPL", 4) != 0)                                                          \
            return EINVAL;                                                                          \
                                                                                                    \
        /* Version (little-endian) */                                                               \
        {                                                                                           \
            uint8_t vbuf[4];                                                                        \
            if (fread(vbuf, 1, 4, fp) != 4)                                                         \
                return EIO;                                                                         \
            uint32_t version = _skip_read_le32(vbuf);                                               \
            if (version != 1)                                                                       \
                return EINVAL;                                                                      \
        }                                                                                           \
                                                                                                    \
        /* Node count (little-endian) */                                                            \
        uint64_t count;                                                                             \
        {                                                                                           \
            uint8_t cbuf[8];                                                                        \
            if (fread(cbuf, 1, 8, fp) != 8)                                                         \
                return EIO;                                                                         \
            count = _skip_read_le64(cbuf);                                                          \
        }                                                                                           \
                                                                                                    \
        /* Per-node data */                                                                         \
        for (uint64_t n = 0; n < count; n++) {                                                      \
            uint64_t bytes;                                                                         \
            {                                                                                       \
                uint8_t bbuf[8];                                                                    \
                if (fread(bbuf, 1, 8, fp) != 8)                                                     \
                    return EIO;                                                                     \
                bytes = _skip_read_le64(bbuf);                                                      \
            }                                                                                       \
                                                                                                    \
            uint8_t *buf = NULL;                                                                    \
            if (bytes > 0) {                                                                        \
                buf = (uint8_t *)palloc((size_t)bytes);                                             \
                if (buf == NULL)                                                                    \
                    return ENOMEM;                                                                  \
                if (fread(buf, 1, (size_t)bytes, fp) != (size_t)bytes) {                            \
                    pfree(buf);                                                                      \
                    return EIO;                                                                     \
                }                                                                                   \
            }                                                                                       \
                                                                                                    \
            decl##_node_t *node;                                                                    \
            int rc = prefix##skip_alloc_node_##decl(&node);                                         \
            if (rc) {                                                                               \
                pfree(buf);                                                                          \
                return rc;                                                                          \
            }                                                                                       \
                                                                                                    \
            read_entry_blk;                                                                         \
            pfree(buf);                                                                              \
                                                                                                    \
            rc = prefix##skip_insert_##decl(slist, node);                                           \
            if (rc) {                                                                               \
                prefix##skip_free_node_##decl(slist, node);                                         \
                return rc;                                                                          \
            }                                                                                       \
        }                                                                                           \
                                                                                                    \
        return 0;                                                                                   \
    }

/**
 * SKIPLIST_DECL_VALIDATE(decl, prefix, field) -- Generate runtime integrity
 * checking for a skiplist.
 *
 * Adds a comprehensive validation function that checks the internal
 * consistency of a skiplist: head/tail sentinels, forward pointer chains,
 * backward pointers, node heights, sort order, marked pointers, and
 * length consistency.
 *
 * This macro must be invoked AFTER SKIPLIST_DECL for the same decl/prefix.
 *
 * @param decl    The skiplist type name (must match SKIPLIST_DECL)
 * @param prefix  The function prefix (must match SKIPLIST_DECL)
 * @param field   The SKIPLIST_ENTRY field name (must match SKIPLIST_DECL)
 *
 * Usage:
 *   SKIPLIST_DECL_VALIDATE(my_list, api_, entries)
 *
 *   int errors = _skip_integrity_check_my_list(&slist, 0);
 *   // errors == 0 means the list is consistent
 *
 * Generated functions:
 *   int _skip_integrity_check_##decl(decl##_t *slist, int flags)
 *         -- Validate internal consistency.  Returns 0 on success, or the
 *            count of errors found.
 *            flags:
 *              bit 0 (& 1): skip concurrent-specific checks (marked pointers,
 *                           forward-chain-to-tail) for single-threaded use.
 *              bit 1 (& 2): early-exit on first error.
 */
#define SKIPLIST_DECL_VALIDATE(decl, prefix, field)                                                                                                      \
    /**                                                                                                                                                  \
     * -- _skip_integrity_failure_                                                                                                                       \
     */                                                                                                                                                  \
    static void _SKIP_PRINTF_ATTR(1, 2) _skip_integrity_failure_##decl(const char *format, ...)                                                          \
    {                                                                                                                                                    \
        char buf[1024];                                                                                                                                  \
        va_list args;                                                                                                                                    \
        va_start(args, format);                                                                                                                          \
        vsnprintf(buf, sizeof(buf), format, args);                                                                                                       \
        va_end(args);                                                                                                                                    \
        elog(WARNING, "skiplist integrity: %s", buf);                                                                                                     \
    }                                                                                                                                                    \
                                                                                                                                                         \
    /**                                                                                                                                                  \
     * -- _skip_integrity_check_                                                                                                                         \
     *                                                                                                                                                   \
     * Validate the internal consistency of a skiplist.                                                                                                  \
     *                                                                                                                                                   \
     * flags:                                                                                                                                            \
     *   bit 0 (& 1): skip concurrent-specific checks (marked pointers,                                                                                  \
     *                 forward-chain-to-tail) for single-threaded use.                                                                                   \
     *   bit 1 (& 2): early-exit on first error.                                                                                                         \
     */                                                                                                                                                  \
    static int _skip_integrity_check_##decl(decl##_t *slist, int flags)                                                                                  \
    {                                                                                                                                                    \
        size_t n = 0;                                                                                                                                    \
        unsigned long nth, n_err = 0;                                                                                                                    \
        decl##_node_t *node, *prev, *next;                                                                                                               \
        struct _skiplist_##decl##_entry *this;                                                                                                           \
        int early_exit = (flags & 2);                                                                                                                    \
        int skip_concurrent = (flags & 1);                                                                                                               \
                                                                                                                                                         \
        if (slist == NULL) {                                                                                                                             \
            _skip_integrity_failure_##decl("slist was NULL, nothing to check\n");                                                                        \
            n_err++;                                                                                                                                     \
            return n_err;                                                                                                                                \
        }                                                                                                                                                \
                                                                                                                                                         \
        /* Check the Skiplist header (slh) */                                                                                                            \
                                                                                                                                                         \
        if (slist->slh_head == NULL) {                                                                                                                   \
            _skip_integrity_failure_##decl("skiplist slh_head is NULL\n");                                                                               \
            n_err++;                                                                                                                                     \
            return n_err;                                                                                                                                \
        }                                                                                                                                                \
                                                                                                                                                         \
        if (slist->slh_tail == NULL) {                                                                                                                   \
            _skip_integrity_failure_##decl("skiplist slh_tail is NULL\n");                                                                               \
            n_err++;                                                                                                                                     \
            return n_err;                                                                                                                                \
        }                                                                                                                                                \
                                                                                                                                                         \
        if (slist->slh_fns.free_entry == NULL) {                                                                                                         \
            _skip_integrity_failure_##decl("skiplist free_entry fn is NULL\n");                                                                          \
            n_err++;                                                                                                                                     \
            return n_err;                                                                                                                                \
        }                                                                                                                                                \
                                                                                                                                                         \
        if (slist->slh_fns.update_entry == NULL) {                                                                                                       \
            _skip_integrity_failure_##decl("skiplist update_entry fn is NULL\n");                                                                        \
            n_err++;                                                                                                                                     \
            return n_err;                                                                                                                                \
        }                                                                                                                                                \
                                                                                                                                                         \
        if (slist->slh_fns.archive_entry == NULL) {                                                                                                      \
            _skip_integrity_failure_##decl("skiplist archive_entry fn is NULL\n");                                                                       \
            n_err++;                                                                                                                                     \
            return n_err;                                                                                                                                \
        }                                                                                                                                                \
                                                                                                                                                         \
        if (slist->slh_fns.sizeof_entry == NULL) {                                                                                                       \
            _skip_integrity_failure_##decl("skiplist sizeof_entry fn is NULL\n");                                                                        \
            n_err++;                                                                                                                                     \
            return n_err;                                                                                                                                \
        }                                                                                                                                                \
                                                                                                                                                         \
        if (slist->slh_fns.compare_entries == NULL) {                                                                                                    \
            _skip_integrity_failure_##decl("skiplist compare_entries fn is NULL\n");                                                                     \
            n_err++;                                                                                                                                     \
            return n_err;                                                                                                                                \
        }                                                                                                                                                \
                                                                                                                                                         \
        /* Read head/tail heights atomically */                                                                                                          \
        size_t head_height = _skip_atomic_load(&slist->slh_head->field.sle_height, memory_order_acquire);                                                \
        size_t tail_height = _skip_atomic_load(&slist->slh_tail->field.sle_height, memory_order_acquire);                                                \
                                                                                                                                                         \
        if (head_height > SKIPLIST_MAX_HEIGHT) {                                                                                                         \
            _skip_integrity_failure_##decl("skiplist head height > SKIPLIST_MAX_HEIGHT\n");                                                              \
            n_err++;                                                                                                                                     \
            if (early_exit)                                                                                                                              \
                return n_err;                                                                                                                            \
        }                                                                                                                                                \
                                                                                                                                                         \
        if (tail_height > SKIPLIST_MAX_HEIGHT) {                                                                                                         \
            _skip_integrity_failure_##decl("skiplist tail height > SKIPLIST_MAX_HEIGHT\n");                                                              \
            n_err++;                                                                                                                                     \
            if (early_exit)                                                                                                                              \
                return n_err;                                                                                                                            \
        }                                                                                                                                                \
                                                                                                                                                         \
        if (head_height != tail_height) {                                                                                                                \
            _skip_integrity_failure_##decl("skiplist head & tail height are not equal\n");                                                               \
            n_err++;                                                                                                                                     \
            if (early_exit)                                                                                                                              \
                return n_err;                                                                                                                            \
        }                                                                                                                                                \
                                                                                                                                                         \
        /* TODO: slh_head->field.sle_height should == log(m) where m is the sum of all hits on all nodes */                                              \
                                                                                                                                                         \
        if (SKIPLIST_MAX_HEIGHT < 1) {                                                                                                                   \
            _skip_integrity_failure_##decl("SKIPLIST_MAX_HEIGHT cannot be less than 1\n");                                                               \
            n_err++;                                                                                                                                     \
            if (early_exit)                                                                                                                              \
                return n_err;                                                                                                                            \
        }                                                                                                                                                \
                                                                                                                                                         \
        /* Validate head node forward pointers */                                                                                                        \
        node = slist->slh_head;                                                                                                                          \
        for (size_t lvl = 0; lvl <= head_height; lvl++) {                                                                                                \
            decl##_node_t *head_next = _skip_atomic_load(&node->field.sle_levels[lvl].next, memory_order_acquire);                                       \
            decl##_node_t *head_next_unmarked = _SKIP_UNMARK(head_next);                                                                                 \
            if (head_next_unmarked == NULL) {                                                                                                            \
                _skip_integrity_failure_##decl("the head's %lu next node should not be NULL\n", lvl);                                                    \
                n_err++;                                                                                                                                 \
                if (early_exit)                                                                                                                          \
                    return n_err;                                                                                                                        \
            }                                                                                                                                            \
            /* Head node next pointers should never be marked */                                                                                         \
            if (!skip_concurrent && _SKIP_IS_MARKED(head_next)) {                                                                                        \
                _skip_integrity_failure_##decl("the head's %lu next pointer is marked (should never be)\n", lvl);                                        \
                n_err++;                                                                                                                                 \
                if (early_exit)                                                                                                                          \
                    return n_err;                                                                                                                        \
            }                                                                                                                                            \
            n = lvl;                                                                                                                                     \
            if (head_next_unmarked == slist->slh_tail)                                                                                                   \
                break;                                                                                                                                   \
        }                                                                                                                                                \
        n++;                                                                                                                                             \
        for (size_t lvl = n; lvl <= head_height; lvl++) {                                                                                                \
            decl##_node_t *head_next = _skip_atomic_load(&node->field.sle_levels[lvl].next, memory_order_acquire);                                       \
            decl##_node_t *head_next_unmarked = _SKIP_UNMARK(head_next);                                                                                 \
            if (head_next_unmarked == NULL) {                                                                                                            \
                _skip_integrity_failure_##decl("the head's %lu next node should not be NULL\n", lvl);                                                    \
                n_err++;                                                                                                                                 \
                if (early_exit)                                                                                                                          \
                    return n_err;                                                                                                                        \
            }                                                                                                                                            \
        }                                                                                                                                                \
                                                                                                                                                         \
        /* Check: tail->prev should not be head when list is non-empty */                                                                                \
        size_t list_len = _skip_atomic_load(&slist->slh_length, memory_order_relaxed);                                                                   \
        decl##_node_t *tail_prev = _skip_atomic_load(&slist->slh_tail->field.sle_prev, memory_order_acquire);                                            \
        if (list_len > 0 && tail_prev == slist->slh_head) {                                                                                              \
            _skip_integrity_failure_##decl("slist->slh_length is %lu, but tail->prev == head, not an internal node\n", list_len);                        \
            n_err++;                                                                                                                                     \
            if (early_exit)                                                                                                                              \
                return n_err;                                                                                                                            \
        }                                                                                                                                                \
                                                                                                                                                         \
        /* Forward pointer consistency: at each level, following next pointers                                                                           \
         * from head should eventually reach tail. Only checked in concurrent                                                                            \
         * mode (when !(flags & 1)) to verify no dangling chains exist. */                                                                               \
        if (!skip_concurrent) {                                                                                                                          \
            for (size_t lvl = 0; lvl <= head_height; lvl++) {                                                                                            \
                decl##_node_t *walk = slist->slh_head;                                                                                                   \
                size_t steps = 0;                                                                                                                        \
                size_t max_steps = list_len + 2; /* head + nodes + tail */                                                                               \
                while (walk != NULL && walk != slist->slh_tail && steps <= max_steps) {                                                                  \
                    decl##_node_t *walk_next = _skip_atomic_load(&walk->field.sle_levels[lvl].next, memory_order_acquire);                               \
                    walk = _SKIP_UNMARK(walk_next);                                                                                                      \
                    steps++;                                                                                                                             \
                }                                                                                                                                        \
                if (walk != slist->slh_tail) {                                                                                                           \
                    _skip_integrity_failure_##decl("forward chain at level %lu does not reach tail (cycle or NULL after %lu steps)\n", lvl, steps);      \
                    n_err++;                                                                                                                             \
                    if (early_exit)                                                                                                                      \
                        return n_err;                                                                                                                    \
                }                                                                                                                                        \
            }                                                                                                                                            \
        }                                                                                                                                                \
                                                                                                                                                         \
        /* Validate each node */                                                                                                                         \
        SKIPLIST_FOREACH_H2T(decl, prefix, field, slist, node, nth)                                                                                      \
        {                                                                                                                                                \
            this = &node->field;                                                                                                                         \
            size_t node_height = _skip_atomic_load(&this->sle_height, memory_order_acquire);                                                             \
                                                                                                                                                         \
            if (node_height > head_height) {                                                                                                             \
                _skip_integrity_failure_##decl("the %lu node's [%p] height %lu is > head %lu\n", nth, (void *)node, node_height, head_height);           \
                n_err++;                                                                                                                                 \
                if (early_exit)                                                                                                                          \
                    return n_err;                                                                                                                        \
            }                                                                                                                                            \
                                                                                                                                                         \
            if (this->sle_levels == NULL) {                                                                                                              \
                _skip_integrity_failure_##decl("the %lu node's [%p] next field should never be NULL\n", nth, (void *)node);                              \
                n_err++;                                                                                                                                 \
                if (early_exit)                                                                                                                          \
                    return n_err;                                                                                                                        \
            }                                                                                                                                            \
                                                                                                                                                         \
            decl##_node_t *node_prev = _skip_atomic_load(&this->sle_prev, memory_order_acquire);                                                         \
            if (node_prev == NULL) {                                                                                                                     \
                _skip_integrity_failure_##decl("the %lu node [%p] prev field should never be NULL\n", nth, (void *)node);                                \
                n_err++;                                                                                                                                 \
                if (early_exit)                                                                                                                          \
                    return n_err;                                                                                                                        \
            }                                                                                                                                            \
                                                                                                                                                         \
            /* Check forward pointers at each level of this node */                                                                                      \
            for (size_t lvl = 0; lvl <= node_height; lvl++) {                                                                                            \
                decl##_node_t *lvl_next = _skip_atomic_load(&this->sle_levels[lvl].next, memory_order_acquire);                                          \
                decl##_node_t *lvl_next_unmarked = _SKIP_UNMARK(lvl_next);                                                                               \
                                                                                                                                                         \
                /* No next pointer should be NULL (should at least point to tail) */                                                                     \
                if (lvl_next_unmarked == NULL) {                                                                                                         \
                    _skip_integrity_failure_##decl("the %lu node's next[%lu] should not be NULL\n", nth, lvl);                                           \
                    n_err++;                                                                                                                             \
                    if (early_exit)                                                                                                                      \
                        return n_err;                                                                                                                    \
                }                                                                                                                                        \
                                                                                                                                                         \
                /* In a quiescent list, no reachable next pointer should be marked */                                                                    \
                if (!skip_concurrent && _SKIP_IS_MARKED(lvl_next)) {                                                                                     \
                    _skip_integrity_failure_##decl("the %lu node's next[%lu] is marked in a quiescent list\n", nth, lvl);                                \
                    n_err++;                                                                                                                             \
                    if (early_exit)                                                                                                                      \
                        return n_err;                                                                                                                    \
                }                                                                                                                                        \
                                                                                                                                                         \
                n = lvl;                                                                                                                                 \
                if (lvl_next_unmarked == slist->slh_tail)                                                                                                \
                    break;                                                                                                                               \
            }                                                                                                                                            \
            n++;                                                                                                                                         \
            for (size_t lvl = n; lvl <= node_height; lvl++) {                                                                                            \
                decl##_node_t *lvl_next = _skip_atomic_load(&this->sle_levels[lvl].next, memory_order_acquire);                                          \
                decl##_node_t *lvl_next_unmarked = _SKIP_UNMARK(lvl_next);                                                                               \
                if (lvl_next_unmarked == NULL) {                                                                                                         \
                    _skip_integrity_failure_##decl("after the %lunth the %lu node's next[%lu] should not be NULL\n", n, nth, lvl);                       \
                    n_err++;                                                                                                                             \
                    if (early_exit)                                                                                                                      \
                        return n_err;                                                                                                                    \
                } else if (lvl_next_unmarked != slist->slh_tail) {                                                                                       \
                    _skip_integrity_failure_##decl("after the %lunth the %lu node's next[%lu] should point to the tail\n", n, nth, lvl);                 \
                    n_err++;                                                                                                                             \
                    if (early_exit)                                                                                                                      \
                        return n_err;                                                                                                                    \
                }                                                                                                                                        \
                /* Check for marked pointers in upper levels too */                                                                                      \
                if (!skip_concurrent && _SKIP_IS_MARKED(lvl_next)) {                                                                                     \
                    _skip_integrity_failure_##decl("the %lu node's next[%lu] is marked in a quiescent list\n", nth, lvl);                                \
                    n_err++;                                                                                                                             \
                    if (early_exit)                                                                                                                      \
                        return n_err;                                                                                                                    \
                }                                                                                                                                        \
            }                                                                                                                                            \
                                                                                                                                                         \
            decl##_node_t *a = (decl##_node_t *)(uintptr_t)this->sle_levels;                                                                             \
            decl##_node_t *b = (decl##_node_t *)(intptr_t)((uintptr_t)node + sizeof(decl##_node_t));                                                     \
            if (a != b) {                                                                                                                                \
                _skip_integrity_failure_##decl("the %lu node's [%p] next field isn't at the proper offset relative to the node\n", nth, (void *)node);   \
                n_err++;                                                                                                                                 \
                if (early_exit)                                                                                                                          \
                    return n_err;                                                                                                                        \
            }                                                                                                                                            \
                                                                                                                                                         \
            next = _SKIP_UNMARK(_skip_atomic_load(&this->sle_levels[0].next, memory_order_acquire));                                                     \
            prev = _SKIP_UNMARK(_skip_atomic_load(&this->sle_prev, memory_order_acquire));                                                               \
            if (_skip_compare_nodes_##decl(slist, node, node, slist->slh_aux) != 0) {                                                                    \
                _skip_integrity_failure_##decl("the %lu node [%p] is not equal to itself\n", nth, (void *)node);                                         \
                n_err++;                                                                                                                                 \
                if (early_exit)                                                                                                                          \
                    return n_err;                                                                                                                        \
            }                                                                                                                                            \
                                                                                                                                                         \
            if (_skip_compare_nodes_##decl(slist, node, prev, slist->slh_aux) < 0) {                                                                     \
                _skip_integrity_failure_##decl("the %lu node [%p] is not greater than the prev node [%p]\n", nth, (void *)node, (void *)prev);           \
                n_err++;                                                                                                                                 \
                if (early_exit)                                                                                                                          \
                    return n_err;                                                                                                                        \
            }                                                                                                                                            \
                                                                                                                                                         \
            if (_skip_compare_nodes_##decl(slist, node, next, slist->slh_aux) > 0) {                                                                     \
                _skip_integrity_failure_##decl("the %lu node [%p] is not less than the next node [%p]\n", nth, (void *)node, (void *)next);              \
                n_err++;                                                                                                                                 \
                if (early_exit)                                                                                                                          \
                    return n_err;                                                                                                                        \
            }                                                                                                                                            \
                                                                                                                                                         \
            if (_skip_compare_nodes_##decl(slist, prev, node, slist->slh_aux) > 0) {                                                                     \
                _skip_integrity_failure_##decl("the prev node [%p] is not less than the %lu node [%p]\n", (void *)prev, nth, (void *)node);              \
                n_err++;                                                                                                                                 \
                if (early_exit)                                                                                                                          \
                    return n_err;                                                                                                                        \
            }                                                                                                                                            \
                                                                                                                                                         \
            if (_skip_compare_nodes_##decl(slist, next, node, slist->slh_aux) < 0) {                                                                     \
                _skip_integrity_failure_##decl("the next node [%p] is not greater than the %lu node [%p]\n", (void *)next, nth, (void *)node);           \
                n_err++;                                                                                                                                 \
                if (early_exit)                                                                                                                          \
                    return n_err;                                                                                                                        \
            }                                                                                                                                            \
        }                                                                                                                                                \
                                                                                                                                                         \
        if (list_len != nth) {                                                                                                                           \
            _skip_integrity_failure_##decl("slist->slh_length (%lu) doesn't match the count (%lu) of nodes between the head and tail\n", list_len, nth); \
            n_err++;                                                                                                                                     \
            if (early_exit)                                                                                                                              \
                return n_err;                                                                                                                            \
        }                                                                                                                                                \
                                                                                                                                                         \
        return n_err;                                                                                                                                    \
    }

/**
 * SKIPLIST_DECL_ACCESS(decl, prefix, key, ktype, value, vtype, qblk, rblk)
 * -- Generate a high-level key/value API for a skiplist.
 *
 * Provides convenient get/put/del/contains/set/dup/pos functions that
 * operate on keys and values directly, rather than requiring the caller
 * to allocate and populate node structs manually.  The node struct must
 * have members named `key` and `value` matching the provided ktype/vtype.
 *
 * This macro must be invoked AFTER SKIPLIST_DECL for the same decl/prefix.
 *
 * @param decl    The skiplist type name (must match SKIPLIST_DECL)
 * @param prefix  The function prefix (must match SKIPLIST_DECL)
 * @param key     The name of the key field in the node struct (typically "key")
 * @param ktype   The C type of the key (e.g., int, const char *)
 * @param value   The name of the value field in the node struct (typically "value")
 * @param vtype   The C type of the value (e.g., int, void *)
 * @param qblk    Code block to initialize a stack-allocated query node from a
 *                key.  Receives (decl##_node_t query, ktype key).  Should set
 *                query.key = key (and zero other fields as needed).
 * @param rblk    Code block to extract and return a value from a found node.
 *                Receives (decl##_node_t *node).  Should contain
 *                "return node->value;" or equivalent.
 *
 * Usage:
 *   SKIPLIST_DECL_ACCESS(my_list, api_, key, int, value, int,
 *       { memset(&query, 0, sizeof(query)); query.key = key; },
 *       { return node->value; })
 *
 *   // Then:
 *   api_skip_put_my_list(&slist, 42, 100);
 *   int val = api_skip_get_my_list(&slist, 42);   // returns 100
 *   int has = api_skip_contains_my_list(&slist, 42);  // returns 1
 *   api_skip_set_my_list(&slist, 42, 200);        // update value
 *   api_skip_del_my_list(&slist, 42);             // remove
 *
 * Generated functions:
 *   vtype prefix##skip_get_##decl(decl##_t *slist, ktype key)
 *           -- Get the value for key.  Returns (vtype)0 if not found.
 *   int   prefix##skip_put_##decl(decl##_t *slist, ktype key, vtype value)
 *           -- Insert key/value.  Returns -1 on duplicate, 0 on success.
 *   int   prefix##skip_del_##decl(decl##_t *slist, ktype key)
 *           -- Remove the node with key.  Returns 0 on success.
 *   int   prefix##skip_contains_##decl(decl##_t *slist, ktype key)
 *           -- Returns 1 if key exists, 0 otherwise.
 *   int   prefix##skip_set_##decl(decl##_t *slist, ktype key, vtype value)
 *           -- Update the value for an existing key in place.
 *   int   prefix##skip_dup_##decl(decl##_t *slist, ktype key, vtype value)
 *           -- Insert key/value allowing duplicates.
 *   decl##_node_t *prefix##skip_pos_##decl(decl##_t *slist, skip_pos_##decl_t op, ktype key)
 *           -- Position a cursor relative to key using op (SKIP_EQ, SKIP_LT,
 *              SKIP_LTE, SKIP_GT, SKIP_GTE).  Returns NULL if no match.
 */
#define SKIPLIST_DECL_ACCESS(decl, prefix, key, ktype, value, vtype, qblk, rblk)             \
    /**                                                                                      \
     * skip_get_ --                                                                          \
     *                                                                                       \
     * Get the value for the given key. In the presence of duplicate keys this               \
     * returns the value from the first duplicate.                                           \
     */                                                                                      \
    vtype prefix##skip_get_##decl(decl##_t *slist, ktype key)                                \
    {                                                                                        \
        decl##_node_t *node, query;                                                          \
                                                                                             \
        qblk;                                                                                \
        node = prefix##skip_position_eq_##decl(slist, &query);                               \
        if (node) {                                                                          \
            rblk;                                                                            \
        }                                                                                    \
        return (vtype)0;                                                                     \
    }                                                                                        \
                                                                                             \
    /**                                                                                      \
     * skip_contains_ --                                                                     \
     *                                                                                       \
     * Returns true if there is at least one match for the `key` in the list.                \
     */                                                                                      \
    int prefix##skip_contains_##decl(decl##_t *slist, ktype key)                             \
    {                                                                                        \
        decl##_node_t *node, query;                                                          \
                                                                                             \
        qblk;                                                                                \
        node = prefix##skip_position_eq_##decl(slist, &query);                               \
        if (node)                                                                            \
            return 1;                                                                        \
        return 0;                                                                            \
    }                                                                                        \
                                                                                             \
    /**                                                                                      \
     * skip_pos_ --                                                                          \
     *                                                                                       \
     * Position a "cursor" (get a "node") from the list that satisfies the                   \
     * condition (`op`) or return NULL if the condition cannot be satisfied.                 \
     * The condition is a skip_pos_##decl_t enum type:                                       \
     *                                                                                       \
     * SKIP_GT  -> greater than                                                              \
     * SKIP_GTE -> greater than or equal to                                                  \
     * SKIP_EQ  -> equal to                                                                  \
     * SKIP_LTE -> less than or equal to                                                     \
     * SKIP_LT  -> less than                                                                 \
     *                                                                                       \
     */                                                                                      \
    decl##_node_t *prefix##skip_pos_##decl(decl##_t *slist, skip_pos_##decl_t op, ktype key) \
    {                                                                                        \
        decl##_node_t *node, query;                                                          \
                                                                                             \
        qblk;                                                                                \
        node = prefix##skip_position_##decl(slist, op, &query);                              \
        if (node != slist->slh_head && node != slist->slh_tail)                              \
            return node;                                                                     \
        return NULL;                                                                         \
    }                                                                                        \
                                                                                             \
    /**                                                                                      \
     * skip_put_ --                                                                          \
     *                                                                                       \
     * Inserts `key` into the list within a node that contains `value`.                      \
     */                                                                                      \
    int prefix##skip_put_##decl(decl##_t *slist, ktype key, vtype value)                     \
    {                                                                                        \
        int rc;                                                                              \
        decl##_node_t *node;                                                                 \
        rc = prefix##skip_alloc_node_##decl(&node);                                          \
        if (rc)                                                                              \
            return rc;                                                                       \
        node->key = key;                                                                     \
        node->value = value;                                                                 \
        rc = prefix##skip_insert_##decl(slist, node);                                        \
        if (rc)                                                                              \
            prefix##skip_free_node_##decl(slist, node);                                      \
        return rc;                                                                           \
    }                                                                                        \
                                                                                             \
    /**                                                                                      \
     * skip_dup_ --                                                                          \
     *                                                                                       \
     * Inserts `key` into the list allowing for duplicates within a node that                \
     * contains `value`.                                                                     \
     */                                                                                      \
    int prefix##skip_dup_##decl(decl##_t *slist, ktype key, vtype value)                     \
    {                                                                                        \
        int rc;                                                                              \
        decl##_node_t *node;                                                                 \
        rc = prefix##skip_alloc_node_##decl(&node);                                          \
        if (rc)                                                                              \
            return rc;                                                                       \
        node->key = key;                                                                     \
        node->value = value;                                                                 \
        rc = prefix##skip_insert_dup_##decl(slist, node);                                    \
        if (rc)                                                                              \
            prefix##skip_free_node_##decl(slist, node);                                      \
        return rc;                                                                           \
    }                                                                                        \
                                                                                             \
    /**                                                                                      \
     * skip_set_ --                                                                          \
     *                                                                                       \
     * Updates in-place the node to contain the new `value`. In the presence of              \
     * duplicate keys in the list, the first key's value will be updated.                    \
     */                                                                                      \
    int prefix##skip_set_##decl(decl##_t *slist, ktype key, vtype value)                     \
    {                                                                                        \
        decl##_node_t node;                                                                  \
        node.key = key;                                                                      \
        return prefix##skip_update_##decl(slist, &node, (void *)(uintptr_t)value);           \
    }                                                                                        \
                                                                                             \
    /**                                                                                      \
     * skip_del_ --                                                                          \
     *                                                                                       \
     * Removes the node from the list with a matching `key`. In the presence of              \
     * duplicate keys in the list, this will remove the first duplicate.                     \
     */                                                                                      \
    int prefix##skip_del_##decl(decl##_t *slist, ktype key)                                  \
    {                                                                                        \
        decl##_node_t node;                                                                  \
        node.key = key;                                                                      \
        return prefix##skip_remove_node_##decl(slist, &node);                                \
    }

/**
 * SKIPLIST_DECL_DOT(decl, prefix, field) -- Generate GraphViz DOT visualization
 * output for a skiplist.
 *
 * Adds functions to emit a DOT-language representation of the skiplist's
 * internal structure, including all nodes, their forward pointers at each
 * level, hit counts, and backward pointers.  The output can be rendered
 * to PDF/PS/PNG using the `dot` tool from GraphViz.
 *
 * Logically deleted (marked) nodes are rendered with dashed red borders,
 * and marked forward-pointer edges are shown as dashed red lines.
 *
 * Multiple skiplists can be rendered in the same DOT file using the
 * subgraph counter (nsg).
 *
 * NOTE: Under concurrency, the DOT output is a point-in-time snapshot.
 * Field reads are performed with atomic_load_explicit but the overall
 * picture may not be globally consistent if concurrent mutations are
 * in progress.  For a consistent diagram, quiesce all writers first.
 *
 * This macro must be invoked AFTER SKIPLIST_DECL for the same decl/prefix.
 *
 * @param decl    The skiplist type name (must match SKIPLIST_DECL)
 * @param prefix  The function prefix (must match SKIPLIST_DECL)
 * @param field   The SKIPLIST_ENTRY field name (must match SKIPLIST_DECL)
 *
 * Usage:
 *   SKIPLIST_DECL_DOT(my_list, api_, entries)
 *
 *   void my_sprintf(my_list_node_t *node, char *buf) {
 *       sprintf(buf, "key=%d val=%d", node->key, node->value);
 *   }
 *
 *   FILE *fp = fopen("/tmp/skiplist.dot", "w");
 *   size_t nsg = api_skip_dot_my_list(fp, &slist, 0, "after insert", my_sprintf);
 *   api_skip_dot_end_my_list(fp, nsg);
 *   fclose(fp);
 *   // Then: dot -Tpdf /tmp/skiplist.dot -o /tmp/skiplist.pdf
 *
 * Generated functions:
 *   int  prefix##skip_dot_##decl(FILE *os, decl##_t *slist, size_t nsg,
 *                                char *msg, skip_sprintf_node_##decl##_t fn)
 *          -- Write a DOT subgraph for the skiplist.  `nsg` is the subgraph
 *             counter (pass 0 for the first list).  `msg` is an optional label.
 *             `fn` is a callback that writes a node description into a char[2048]
 *             buffer.  Returns the next subgraph counter.
 *   void prefix##skip_dot_end_##decl(FILE *os, size_t nsg)
 *          -- Finalize the DOT file.  Call after all skip_dot_ calls.
 */
#define SKIPLIST_DECL_DOT(decl, prefix, field)                                                                                             \
                                                                                                                                           \
    /* A type for a function that writes into a char[2048] buffer                                                                          \
     * a description of the value within the node. */                                                                                      \
    typedef void (*skip_sprintf_node_##decl##_t)(decl##_node_t *, char *);                                                                 \
                                                                                                                                           \
    /* -- _skip_dot_width_                                                                                                                 \
     * Counts how many nodes lie between `from` and `to` via sle_prev.                                                                     \
     */                                                                                                                                    \
    static size_t _skip_dot_width_##decl(decl##_t *slist, decl##_node_t *from, decl##_node_t *to)                                          \
    {                                                                                                                                      \
        size_t w = 1;                                                                                                                      \
        decl##_node_t *n = to;                                                                                                             \
        size_t max_w = _skip_atomic_load(&slist->slh_length, memory_order_relaxed) + 2;                                                    \
                                                                                                                                           \
        if (from == NULL || to == NULL)                                                                                                    \
            return 0;                                                                                                                      \
                                                                                                                                           \
        while (_SKIP_UNMARK(_skip_atomic_load(&n->field.sle_prev, memory_order_acquire)) != from) {                                        \
            w++;                                                                                                                           \
            if (w > max_w)                                                                                                                 \
                return w;                                                                                                                  \
            n = prefix##skip_prev_node_##decl(slist, n);                                                                                   \
        }                                                                                                                                  \
                                                                                                                                           \
        return w;                                                                                                                          \
    }                                                                                                                                      \
                                                                                                                                           \
    static inline void _skip_dot_write_node_##decl(FILE *os, size_t nsg, decl##_node_t *node)                                              \
    {                                                                                                                                      \
        if (node)                                                                                                                          \
            fprintf(os, "\"node%lu %p\"", nsg, (void *)node);                                                                              \
        else                                                                                                                               \
            fprintf(os, "\"node%lu NULL\"", nsg);                                                                                          \
    }                                                                                                                                      \
                                                                                                                                           \
    /* -- _skip_dot_node_                                                                                                                  \
     * Writes out a fragment of a DOT file representing a node.                                                                            \
     * Marked (logically deleted) nodes are shown with dashed red border.                                                                  \
     * Marked next-pointer edges are shown as dashed red lines.                                                                            \
     */                                                                                                                                    \
    static void _skip_dot_node_##decl(FILE *os, decl##_t *slist, decl##_node_t *node, size_t nsg, skip_sprintf_node_##decl##_t fn)         \
    {                                                                                                                                      \
        char buf[2048];                                                                                                                    \
        decl##_node_t *raw_next, *next;                                                                                                    \
        size_t node_height = _skip_atomic_load(&node->field.sle_height, memory_order_acquire);                                             \
                                                                                                                                           \
        /* Check if this node is logically deleted (level 0 next is marked) */                                                             \
        decl##_node_t *lvl0_raw = _skip_atomic_load(&node->field.sle_levels[0].next, memory_order_acquire);                                \
        int node_is_marked = _SKIP_IS_MARKED(lvl0_raw);                                                                                    \
                                                                                                                                           \
        _skip_dot_write_node_##decl(os, nsg, node);                                                                                        \
        fprintf(os, " [label = \"");                                                                                                       \
        fflush(os);                                                                                                                        \
        for (size_t lvl = node_height; lvl != SIZE_MAX; lvl--) {                                                                           \
            raw_next = _skip_atomic_load(&node->field.sle_levels[lvl].next, memory_order_acquire);                                         \
            next = _SKIP_UNMARK(raw_next);                                                                                                 \
            next = (next == slist->slh_tail) ? NULL : next;                                                                                \
            size_t hits = _skip_atomic_load(&node->field.sle_levels[lvl].hits, memory_order_relaxed);                                      \
            fprintf(os, " { <w%lu> %lu | <f%lu> ", lvl, hits, lvl);                                                                        \
            if (_SKIP_IS_MARKED(raw_next))                                                                                                 \
                fprintf(os, "X ");                                                                                                         \
            if (next)                                                                                                                      \
                fprintf(os, "%p } |", (void *)next);                                                                                       \
            else                                                                                                                           \
                fprintf(os, "0x0 } |");                                                                                                    \
            fflush(os);                                                                                                                    \
        }                                                                                                                                  \
        if (fn) {                                                                                                                          \
            fn(node, buf);                                                                                                                 \
            fprintf(os, " <f0> \u219F %lu \u226B %s \"\n", node_height, buf);                                                              \
        } else {                                                                                                                           \
            fprintf(os, " <f0> \u219F %lu \"\n", node_height);                                                                             \
        }                                                                                                                                  \
        fprintf(os, "shape = \"record\"\n");                                                                                               \
        /* Render marked (logically deleted) nodes with dashed red border */                                                               \
        if (node_is_marked) {                                                                                                              \
            fprintf(os, "style = \"dashed\"\n");                                                                                           \
            fprintf(os, "color = \"red\"\n");                                                                                              \
            fprintf(os, "fontcolor = \"red\"\n");                                                                                          \
        }                                                                                                                                  \
        fprintf(os, "];\n");                                                                                                               \
        fflush(os);                                                                                                                        \
                                                                                                                                           \
        /* Now edges */                                                                                                                    \
        for (size_t lvl = 0; lvl <= node_height; lvl++) {                                                                                  \
            raw_next = _skip_atomic_load(&node->field.sle_levels[lvl].next, memory_order_acquire);                                         \
            int edge_marked = _SKIP_IS_MARKED(raw_next);                                                                                   \
            next = _SKIP_UNMARK(raw_next);                                                                                                 \
            next = (next == slist->slh_tail) ? NULL : next;                                                                                \
            _skip_dot_write_node_##decl(os, nsg, node);                                                                                    \
            fprintf(os, ":f%lu -> ", lvl);                                                                                                 \
            _skip_dot_write_node_##decl(os, nsg, next);                                                                                    \
            /* Render marked edges as dashed red lines */                                                                                  \
            if (edge_marked)                                                                                                               \
                fprintf(os, ":w%lu [style=dashed, color=red];\n", lvl);                                                                    \
            else                                                                                                                           \
                fprintf(os, ":w%lu [];\n", lvl);                                                                                           \
            fflush(os);                                                                                                                    \
        }                                                                                                                                  \
    }                                                                                                                                      \
                                                                                                                                           \
    /* -- _skip_dot_finish_                                                                                                                \
     * Finalize the DOT file of the internal representation.                                                                               \
     */                                                                                                                                    \
    void prefix##skip_dot_end_##decl(FILE *os, size_t nsg)                                                                                 \
    {                                                                                                                                      \
        size_t i;                                                                                                                          \
        if (nsg > 0) {                                                                                                                     \
            fprintf(os, "node0 [shape=record, label = \"");                                                                                \
            for (i = 0; i < nsg; ++i) {                                                                                                    \
                fprintf(os, "<f%lu> | ", i);                                                                                               \
            }                                                                                                                              \
            fprintf(os, "\", style=invis, width=0.01];\n");                                                                                \
                                                                                                                                           \
            for (i = 0; i < nsg; ++i) {                                                                                                    \
                fprintf(os, "node0:f%lu -> HeadNode%lu [style=invis];\n", i, i);                                                           \
            }                                                                                                                              \
            nsg = 0;                                                                                                                       \
        }                                                                                                                                  \
        fprintf(os, "}\n");                                                                                                                \
    }                                                                                                                                      \
                                                                                                                                           \
    /* -- skip_dot_                                                                                                                        \
     * Create a DOT file of the internal representation of the                                                                             \
     * Skiplist on the provided file descriptor (default: STDOUT).                                                                         \
     *                                                                                                                                     \
     * NOTE: Under concurrency, the DOT output represents a point-in-time                                                                  \
     * snapshot. Atomic loads are used for individual field reads, but the                                                                 \
     * overall diagram may not be globally consistent if concurrent                                                                        \
     * mutations are in progress. Quiesce all writers for a consistent view.                                                               \
     *                                                                                                                                     \
     * To view the output:                                                                                                                 \
     * $ dot -Tps filename.dot -o outfile.ps                                                                                               \
     */                                                                                                                                    \
    int prefix##skip_dot_##decl(FILE *os, decl##_t *slist, size_t nsg, char *msg, skip_sprintf_node_##decl##_t fn)                         \
    {                                                                                                                                      \
        int has_content = 0;                                                                                                               \
        size_t i;                                                                                                                          \
        decl##_node_t *node, *next;                                                                                                        \
                                                                                                                                           \
        if (slist == NULL || fn == NULL)                                                                                                   \
            return nsg;                                                                                                                    \
                                                                                                                                           \
        size_t dot_head_height = _skip_atomic_load(&slist->slh_head->field.sle_height, memory_order_acquire);                              \
                                                                                                                                           \
        if (nsg == 0) {                                                                                                                    \
            fprintf(os, "digraph Skiplist {\n");                                                                                           \
            fprintf(os, "label = \"Skiplist (point-in-time snapshot).\"\n");                                                               \
            fprintf(os, "graph [rankdir = \"LR\"];\n");                                                                                    \
            fprintf(os, "node [fontsize = \"12\" shape = \"ellipse\"];\n");                                                                \
            fprintf(os, "edge [];\n\n");                                                                                                   \
        }                                                                                                                                  \
        fprintf(os, "subgraph cluster%lu {\n", nsg);                                                                                       \
        fprintf(os, "style=dashed\n");                                                                                                     \
        fprintf(os, "label=\"Skiplist [%lu]", nsg);                                                                                        \
        if (msg)                                                                                                                           \
            fprintf(os, " %s", msg);                                                                                                       \
        fprintf(os, "\"\n\n");                                                                                                             \
        fprintf(os, "\"HeadNode%lu\" [\n", nsg);                                                                                           \
        fprintf(os, "label = \"");                                                                                                         \
                                                                                                                                           \
        decl##_node_t *head_lvl0_next = _SKIP_UNMARK(_skip_atomic_load(&slist->slh_head->field.sle_levels[0].next, memory_order_acquire)); \
        if (dot_head_height || head_lvl0_next != slist->slh_tail)                                                                          \
            has_content = 1;                                                                                                               \
                                                                                                                                           \
        /* Write out the head node fields */                                                                                               \
        node = slist->slh_head;                                                                                                            \
        if (has_content) {                                                                                                                 \
            for (size_t lvl = dot_head_height; lvl != SIZE_MAX; lvl--) {                                                                   \
                decl##_node_t *raw = _skip_atomic_load(&node->field.sle_levels[lvl].next, memory_order_acquire);                           \
                next = _SKIP_UNMARK(raw);                                                                                                  \
                next = (next == slist->slh_tail) ? NULL : next;                                                                            \
                size_t hits = _skip_atomic_load(&node->field.sle_levels[lvl].hits, memory_order_relaxed);                                  \
                fprintf(os, "{ %lu | <f%lu> ", hits, lvl);                                                                                 \
                if (next)                                                                                                                  \
                    fprintf(os, "%p }", (void *)next);                                                                                     \
                else                                                                                                                       \
                    fprintf(os, "0x0 }");                                                                                                  \
                if (lvl == 0)                                                                                                              \
                    continue;                                                                                                              \
                fprintf(os, " | ");                                                                                                        \
            }                                                                                                                              \
        } else {                                                                                                                           \
            fprintf(os, "Empty HeadNode");                                                                                                 \
        }                                                                                                                                  \
        fprintf(os, "\"\n");                                                                                                               \
        fprintf(os, "shape = \"record\"\n");                                                                                               \
        fprintf(os, "];\n");                                                                                                               \
        fflush(os);                                                                                                                        \
                                                                                                                                           \
        /* Edges for head node */                                                                                                          \
        node = slist->slh_head;                                                                                                            \
        if (has_content) {                                                                                                                 \
            node = slist->slh_head;                                                                                                        \
            for (size_t lvl = 0; lvl <= dot_head_height; lvl++) {                                                                          \
                decl##_node_t *raw = _skip_atomic_load(&node->field.sle_levels[lvl].next, memory_order_acquire);                           \
                int edge_marked = _SKIP_IS_MARKED(raw);                                                                                    \
                next = _SKIP_UNMARK(raw);                                                                                                  \
                next = (next == slist->slh_tail) ? NULL : next;                                                                            \
                fprintf(os, "\"HeadNode%lu\":f%lu -> ", nsg, lvl);                                                                         \
                _skip_dot_write_node_##decl(os, nsg, next);                                                                                \
                if (edge_marked)                                                                                                           \
                    fprintf(os, ":w%lu [style=dashed, color=red];\n", lvl);                                                                \
                else                                                                                                                       \
                    fprintf(os, ":w%lu [];\n", lvl);                                                                                       \
            }                                                                                                                              \
            fprintf(os, "\n");                                                                                                             \
        }                                                                                                                                  \
        fflush(os);                                                                                                                        \
                                                                                                                                           \
        /* Now all nodes via level 0, if non-empty */                                                                                      \
        node = slist->slh_head;                                                                                                            \
        if (has_content) {                                                                                                                 \
            SKIPLIST_FOREACH_H2T(decl, prefix, field, slist, next, i)                                                                      \
            {                                                                                                                              \
                ((void)i);                                                                                                                 \
                _skip_dot_node_##decl(os, slist, next, nsg, fn);                                                                           \
                fflush(os);                                                                                                                \
            }                                                                                                                              \
            fprintf(os, "\n");                                                                                                             \
        }                                                                                                                                  \
        fflush(os);                                                                                                                        \
                                                                                                                                           \
        /* The tail, sentinel node */                                                                                                      \
        if (has_content) {                                                                                                                 \
            _skip_dot_write_node_##decl(os, nsg, NULL);                                                                                    \
            fprintf(os, " [label = \"");                                                                                                   \
            node = slist->slh_tail;                                                                                                        \
            size_t th = dot_head_height;                                                                                                   \
            for (size_t lvl = th; lvl != SIZE_MAX; lvl--) {                                                                                \
                decl##_node_t *raw = _skip_atomic_load(&node->field.sle_levels[lvl].next, memory_order_acquire);                           \
                (void)raw; /* tail next pointers are unused in display */                                                                  \
                fprintf(os, "<w%lu> 0x0", lvl);                                                                                            \
                if (lvl == 0)                                                                                                              \
                    continue;                                                                                                              \
                fprintf(os, " | ");                                                                                                        \
            }                                                                                                                              \
            fprintf(os, "\" shape = \"record\"];\n");                                                                                      \
        }                                                                                                                                  \
                                                                                                                                           \
        /* End: "subgraph cluster0 {" */                                                                                                   \
        fprintf(os, "}\n\n");                                                                                                              \
        nsg += 1;                                                                                                                          \
        fflush(os);                                                                                                                        \
                                                                                                                                           \
        return nsg;                                                                                                                        \
    }

/**
 * SKIPLIST_DECL_POOL(decl, prefix, field, capacity_hint) -- Generate a
 * fixed-capacity, lock-free pre-allocation pool for skiplist nodes.
 *
 * Pre-allocates a contiguous block of memory for `capacity` node slots.
 * Each slot is cache-line aligned (64 bytes) to prevent false sharing.
 * The free list is managed via atomic CAS on an index (int32_t), avoiding
 * the ABA problem that plagues pointer-based lock-free stacks.
 *
 * Using a pool eliminates malloc/free overhead during skiplist operations
 * and improves cache locality.  When the pool is exhausted, the pool
 * alloc function returns ENOMEM; the caller can fall back to the standard
 * skip_alloc_node_ if desired.
 *
 * This macro must be invoked AFTER SKIPLIST_DECL for the same decl/prefix.
 *
 * @param decl           The skiplist type name (must match SKIPLIST_DECL)
 * @param prefix         The function prefix (must match SKIPLIST_DECL)
 * @param field          The SKIPLIST_ENTRY field name (must match SKIPLIST_DECL)
 * @param capacity_hint  Compile-time hint for pool capacity (the actual capacity
 *                       is passed to skip_pool_init_ at runtime)
 *
 * Usage:
 *   SKIPLIST_DECL_POOL(my_list, api_, entries, 1024)
 *
 *   _skip_pool_my_list_t pool;
 *   api_skip_pool_init_my_list(&pool, 1024);
 *   // ... use pool-aware alloc/free ...
 *   my_list_node_t *node;
 *   api_skip_pool_alloc_node_my_list(&pool, &node);
 *   node->key = 42;
 *   api_skip_insert_my_list(&slist, node);
 *   // ... at shutdown ...
 *   api_skip_pool_destroy_my_list(&pool);
 *
 * Generated functions:
 *   int  prefix##skip_pool_init_##decl(_skip_pool_##decl##_t *pool, size_t capacity)
 *          -- Initialize the pool with `capacity` slots.  Returns 0 or errno.
 *   decl##_node_t *prefix##skip_pool_alloc_##decl(_skip_pool_##decl##_t *pool)
 *          -- Pop a zeroed, initialized node from the pool.  Returns NULL if exhausted.
 *   void prefix##skip_pool_free_##decl(_skip_pool_##decl##_t *pool, decl##_node_t *node)
 *          -- Push a node back onto the pool free list.
 *   void prefix##skip_pool_destroy_##decl(_skip_pool_##decl##_t *pool)
 *          -- Free the contiguous slab.  All pool nodes become invalid.
 *   int  prefix##skip_pool_is_from_##decl(_skip_pool_##decl##_t *pool, decl##_node_t *node)
 *          -- Returns non-zero if node belongs to this pool.
 *   int  prefix##skip_pool_alloc_node_##decl(_skip_pool_##decl##_t *pool, decl##_node_t **node)
 *          -- Allocate a node from the pool into *node.  Returns 0 or ENOMEM.
 *   void prefix##skip_pool_free_node_##decl(_skip_pool_##decl##_t *pool, decl##_t *slist,
 *                                           decl##_node_t *node)
 *          -- Free a node: calls free_entry, then returns to pool or free().
 */
#define SKIPLIST_DECL_POOL(decl, prefix, field, capacity_hint)                                                       \
                                                                                                                     \
    /* ------------------------------------------------------------------ */                                         \
    /* Pool type definition                                                */                                        \
    /* ------------------------------------------------------------------ */                                         \
    typedef struct _skip_pool_##decl {                                                                               \
        size_t capacity;                   /* total number of slots */                                               \
        size_t slot_size;                  /* bytes per slot (aligned to 64) */                                      \
        _SKIP_ALIGNAS(64) char *slots;     /* contiguous allocation for all slots */                                 \
        _SKIP_ATOMIC(int32_t) free_head;   /* index of first free slot, -1 = empty */                                \
        _SKIP_ATOMIC(int32_t) * next_free; /* per-slot free-list links (separate from slot data) */                  \
    } _skip_pool_##decl##_t;                                                                                         \
                                                                                                                     \
    /* ------------------------------------------------------------------ */                                         \
    /* _skip_pool_slot_ptr_ -- Return a pointer to the start of slot `i` */                                          \
    /* ------------------------------------------------------------------ */                                         \
    static inline char *_skip_pool_slot_ptr_##decl(_skip_pool_##decl##_t *pool, int32_t i)                           \
    {                                                                                                                \
        return pool->slots + ((size_t)i * pool->slot_size);                                                          \
    }                                                                                                                \
                                                                                                                     \
    /* ------------------------------------------------------------------ */                                         \
    /* _skip_pool_next_free_ -- Read/write the next-free index for a     */                                          \
    /*   slot.  Stored in a separate atomic array (not in the slot       */                                          \
    /*   itself) to avoid data races between speculative free-list       */                                          \
    /*   reads and concurrent node-data writes after CAS.                */                                          \
    /* ------------------------------------------------------------------ */                                         \
    static inline int32_t _skip_pool_get_next_free_##decl(_skip_pool_##decl##_t *pool, int32_t i)                    \
    {                                                                                                                \
        return _skip_atomic_load(&pool->next_free[i], memory_order_relaxed);                                         \
    }                                                                                                                \
                                                                                                                     \
    static inline void _skip_pool_set_next_free_##decl(_skip_pool_##decl##_t *pool, int32_t i, int32_t next)         \
    {                                                                                                                \
        _skip_atomic_store(&pool->next_free[i], next, memory_order_relaxed);                                         \
    }                                                                                                                \
                                                                                                                     \
    /* ------------------------------------------------------------------ */                                         \
    /* _skip_pool_index_of_ -- Given a node pointer, return its slot     */                                          \
    /*   index (or -1 if the pointer is outside the pool).                */                                         \
    /* ------------------------------------------------------------------ */                                         \
    static inline int32_t _skip_pool_index_of_##decl(_skip_pool_##decl##_t *pool, decl##_node_t *node)               \
    {                                                                                                                \
        char *p = (char *)node;                                                                                      \
        if (p < pool->slots || p >= pool->slots + (pool->capacity * pool->slot_size))                                \
            return -1;                                                                                               \
        size_t offset = (size_t)(p - pool->slots);                                                                   \
        if (offset % pool->slot_size != 0)                                                                           \
            return -1;                                                                                               \
        return (int32_t)(offset / pool->slot_size);                                                                  \
    }                                                                                                                \
                                                                                                                     \
    /* ------------------------------------------------------------------ */                                         \
    /* skip_pool_init_ -- Initialize the pool with `capacity` slots.      */                                         \
    /*                                                                     */                                        \
    /* Each slot is sized to hold one decl##_node_t plus the sle_levels   */                                         \
    /* array for SKIPLIST_MAX_HEIGHT levels, rounded up to a multiple of  */                                         \
    /* 64 bytes for cache-line alignment.                                  */                                        \
    /* ------------------------------------------------------------------ */                                         \
    int prefix##skip_pool_init_##decl(_skip_pool_##decl##_t *pool, size_t capacity)                                  \
    {                                                                                                                \
        if (pool == NULL || capacity == 0)                                                                           \
            return EINVAL;                                                                                           \
                                                                                                                     \
        /* Compute raw slot size: node struct + levels array */                                                      \
        size_t raw_size = sizeof(decl##_node_t) + sizeof(struct _skiplist_##decl##_level) * SKIPLIST_MAX_HEIGHT;     \
                                                                                                                     \
        /* Round up to next multiple of 64 for cache-line alignment */                                               \
        size_t slot_size = (raw_size + 63u) & ~(size_t)63u;                                                          \
                                                                                                                     \
        pool->capacity = capacity;                                                                                   \
        pool->slot_size = slot_size;                                                                                 \
                                                                                                                     \
        /* Allocate the contiguous slab, aligned to 64 bytes */                                                      \
        pool->slots = (char *)_skip_aligned_alloc(64, slot_size * capacity);                                         \
        if (pool->slots == NULL)                                                                                     \
            return ENOMEM;                                                                                           \
                                                                                                                     \
        /* Allocate the separate free-list link array */                                                             \
        pool->next_free = (_SKIP_ATOMIC(int32_t) *)palloc0((capacity) * (sizeof(_SKIP_ATOMIC(int32_t))));                  \
        if (pool->next_free == NULL) {                                                                               \
            _skip_aligned_free(pool->slots);                                                                         \
            pool->slots = NULL;                                                                                      \
            return ENOMEM;                                                                                           \
        }                                                                                                            \
                                                                                                                     \
        /* Zero the entire slab */                                                                                   \
        memset(pool->slots, 0, slot_size *capacity);                                                                 \
                                                                                                                     \
        /* Build the free list: slot[0]->1, slot[1]->2, ..., slot[n-1]->-1 */                                        \
        for (size_t i = 0; i < capacity - 1; i++) {                                                                  \
            _skip_pool_set_next_free_##decl(pool, (int32_t)i, (int32_t)(i + 1));                                     \
        }                                                                                                            \
        _skip_pool_set_next_free_##decl(pool, (int32_t)(capacity - 1), -1);                                          \
                                                                                                                     \
        _skip_atomic_store(&pool->free_head, 0, memory_order_release);                                               \
                                                                                                                     \
        return 0;                                                                                                    \
    }                                                                                                                \
                                                                                                                     \
    /* ------------------------------------------------------------------ */                                         \
    /* skip_pool_alloc_ -- Pop a slot from the free list (lock-free).     */                                         \
    /*                                                                     */                                        \
    /* Returns a fully zeroed node with sle_levels pointing into the      */                                         \
    /* trailing portion of the same slot.  Returns NULL when the pool     */                                         \
    /* is exhausted.                                                       */                                        \
    /* ------------------------------------------------------------------ */                                         \
    decl##_node_t *prefix##skip_pool_alloc_##decl(_skip_pool_##decl##_t *pool)                                       \
    {                                                                                                                \
        int32_t head, next;                                                                                          \
        do {                                                                                                         \
            head = _skip_atomic_load(&pool->free_head, memory_order_acquire);                                        \
            if (head < 0)                                                                                            \
                return NULL; /* pool exhausted */                                                                    \
            next = _skip_pool_get_next_free_##decl(pool, head);                                                      \
        } while (!_skip_atomic_cas_weak(&pool->free_head, &head, next, memory_order_acq_rel, memory_order_acquire)); \
                                                                                                                     \
        /* Zero the slot and initialize the node */                                                                  \
        char *slot = _skip_pool_slot_ptr_##decl(pool, head);                                                         \
        memset(slot, 0, sizeof(decl##_node_t) + sizeof(struct _skiplist_##decl##_level) * SKIPLIST_MAX_HEIGHT);      \
                                                                                                                     \
        decl##_node_t *node = (decl##_node_t *)slot;                                                                 \
        node->field.sle_height = 0;                                                                                  \
        node->field.sle_levels = (struct _skiplist_##decl##_level *)((uintptr_t)node + sizeof(decl##_node_t));       \
                                                                                                                     \
        return node;                                                                                                 \
    }                                                                                                                \
                                                                                                                     \
    /* ------------------------------------------------------------------ */                                         \
    /* skip_pool_free_ -- Push a slot back onto the free list (lock-free).*/                                         \
    /* ------------------------------------------------------------------ */                                         \
    void prefix##skip_pool_free_##decl(_skip_pool_##decl##_t *pool, decl##_node_t *node)                             \
    {                                                                                                                \
        int32_t idx = _skip_pool_index_of_##decl(pool, node);                                                        \
        if (idx < 0)                                                                                                 \
            return; /* not from this pool, ignore */                                                                 \
                                                                                                                     \
        int32_t head;                                                                                                \
        do {                                                                                                         \
            head = _skip_atomic_load(&pool->free_head, memory_order_acquire);                                        \
            _skip_pool_set_next_free_##decl(pool, idx, head);                                                        \
        } while (!_skip_atomic_cas_weak(&pool->free_head, &head, idx, memory_order_acq_rel, memory_order_acquire));  \
    }                                                                                                                \
                                                                                                                     \
    /* ------------------------------------------------------------------ */                                         \
    /* skip_pool_is_from_ -- Check if a node belongs to this pool.        */                                         \
    /* ------------------------------------------------------------------ */                                         \
    int prefix##skip_pool_is_from_##decl(_skip_pool_##decl##_t *pool, decl##_node_t *node)                           \
    {                                                                                                                \
        return _skip_pool_index_of_##decl(pool, node) >= 0;                                                          \
    }                                                                                                                \
                                                                                                                     \
    /* ------------------------------------------------------------------ */                                         \
    /* skip_pool_destroy_ -- Free the contiguous slab.                    */                                         \
    /* ------------------------------------------------------------------ */                                         \
    void prefix##skip_pool_destroy_##decl(_skip_pool_##decl##_t *pool)                                               \
    {                                                                                                                \
        if (pool == NULL)                                                                                            \
            return;                                                                                                  \
        _skip_aligned_free(pool->slots);                                                                             \
        pool->slots = NULL;                                                                                          \
        pfree(pool->next_free);                                                                                       \
        pool->next_free = NULL;                                                                                      \
        pool->capacity = 0;                                                                                          \
        _skip_atomic_store(&pool->free_head, -1, memory_order_release);                                              \
    }                                                                                                                \
                                                                                                                     \
    /* ------------------------------------------------------------------ */                                         \
    /* Pool-aware alloc/free wrappers                                      */                                        \
    /*                                                                     */                                        \
    /* These replace skip_alloc_node_ and skip_free_node_ when a pool     */                                         \
    /* is attached to the skiplist.  They check the pool first, falling   */                                         \
    /* back to malloc/free when the pool is exhausted or the node is not  */                                         \
    /* from the pool.                                                      */                                        \
    /* ------------------------------------------------------------------ */                                         \
    int prefix##skip_pool_alloc_node_##decl(_skip_pool_##decl##_t *pool, decl##_node_t **node)                       \
    {                                                                                                                \
        decl##_node_t *n = prefix##skip_pool_alloc_##decl(pool);                                                     \
        if (n != NULL) {                                                                                             \
            *node = n;                                                                                               \
            return 0;                                                                                                \
        }                                                                                                            \
        /* Pool exhausted -- return ENOMEM.                                */                                        \
        /* If fallback-to-malloc is desired, the caller can try            */                                        \
        /* prefix##skip_alloc_node_##decl() instead.                       */                                        \
        return ENOMEM;                                                                                               \
    }                                                                                                                \
                                                                                                                     \
    void prefix##skip_pool_free_node_##decl(_skip_pool_##decl##_t *pool, decl##_t *slist, decl##_node_t *node)       \
    {                                                                                                                \
        /* Always call the user's free_entry to release user-held resources */                                       \
        slist->slh_fns.free_entry(node);                                                                             \
                                                                                                                     \
        /* If the node came from the pool, return it there; otherwise free */                                        \
        if (prefix##skip_pool_is_from_##decl(pool, node)) {                                                          \
            prefix##skip_pool_free_##decl(pool, node);                                                               \
        } else {                                                                                                     \
            pfree(node);                                                                                              \
        }                                                                                                            \
    }

#endif							/* SKIPLIST_H */
