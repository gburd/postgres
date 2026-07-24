/*-------------------------------------------------------------------------
 *
 * sparsemap.h
 *	  A sparse, compressed bitmap with run-length encoding (RLE).
 *
 * Sparsemap is a mutable, resizable, compressed bitmap optimized for workloads
 * that contain long runs of consecutive set or unset bits.
 *
 * Architecture
 * ------------
 * The implementation uses a 3-tier hierarchy:
 *
 * Tier 0 (bit vectors): Individual bits are stored in 64-bit words (uint64).
 *
 * Tier 1 (chunks): Groups of bit vectors are managed by chunk maps.
 * Chunks use one of two internal encodings:
 *
 *   Sparse encoding: A descriptor word holds 2-bit flags for up to 32
 *   bit vectors (2048 bits total).  Only vectors with a mix of set and unset
 *   bits are stored; uniform vectors (all-zero or all-one) are represented
 *   by their flag alone:
 *
 *       00  all zeros  -- vector not stored
 *       11  all ones   -- vector not stored
 *       10  mixed      -- vector stored after the descriptor
 *       01  unused     -- reduces chunk capacity
 *
 *   RLE encoding: A single 64-bit descriptor represents a contiguous
 *   run of set bits starting at index 0 within the chunk:
 *
 *       Bits 63:62 = 01  (RLE flag)
 *       Bits 61:31       chunk capacity in bits  (max ~2 billion)
 *       Bits 30:0        run length in bits      (max ~2 billion)
 *
 *     Bits [0, length) are set; bits [length, capacity) are unset.
 *
 * Tier 2 (map): The top-level sparsemap manages an ordered sequence of
 * chunks, each tagged with a 4-byte starting offset.  The map grows and
 * shrinks the underlying byte buffer as chunks are added or removed.
 *
 * Thread safety
 * -------------
 * Sparsemap is NOT thread-safe.  Concurrent reads are safe only when no
 * writer is active.  All mutating operations must be externally synchronized.
 *
 * Error handling
 * -------------
 * Functions that mutate the map return SM_IDX_MAX when the backing
 * buffer is full.  The caller can grow the buffer with
 * sm_set_data_size() and retry.
 *
 * Allocation functions (sm_create(), sm_copy(),
 * sm_owned_copy(), sm_wrap()) return NULL on allocation failure.
 *
 * Allocation lineage and disposal
 * --------------------------------
 * Every sparsemap_t has an internal allocation lineage tag that determines
 * which functions may safely realloc its data buffer and how it must be
 * disposed.  The lineage is set by the constructor:
 *
 * | Constructor              | Lineage              | Disposal               |
 * |--------------------------|----------------------|------------------------|
 * | sm_create()              | owned-contiguous     | sm_free()              |
 * | sm_copy()                | owned-contiguous     | sm_free()              |
 * | sm_owned_copy()          | owned-contiguous     | sm_free()              |
 * | sm_wrap()                | wrapped              | sm_free()              |
 * | sm_init()                | wrapped              | (caller frees both)    |
 * | sm_open()                | wrapped              | (caller frees both)    |
 *
 * Copyright (c) 2024 Gregory Burd <greg@burd.me>
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/include/lib/sparsemap.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SPARSEMAP_H
#define SPARSEMAP_H

/* Library version (kept in sync with upstream sparsemap v5.3.0). */
#define SM_VERSION_STRING "5.3.0"
#define SM_VERSION_MAJOR  5
#define SM_VERSION_MINOR  3
#define SM_VERSION_PATCH  0

/*
 * Symbol namespacing.
 *
 * Defining SPARSEMAP_PREFIX rewrites every public type, struct tag, and
 * function so it carries the given prefix at link time.  In the
 * PostgreSQL adaptation the prefix is "pg_", so the exported linker
 * symbols are pg_sm_create, pg_sm_add, struct pg_sparsemap, etc.  This
 * keeps the bitmap's symbols out of the way of any other "sm_*" code
 * that may be linked into the same backend.
 *
 * The macros rename whole tokens only, so source code (here and in
 * callers) keeps using the short names sm_create / sparsemap_t / etc.;
 * the preprocessor expands them consistently at both definition and
 * call sites because every translation unit includes this header.
 */
#define SPARSEMAP_PREFIX pg_

#ifdef SPARSEMAP_PREFIX
#define SM__CAT2(a, b) a##b
#define SM__CAT(a, b)  SM__CAT2(a, b)
#define SM__P(name)    SM__CAT(SPARSEMAP_PREFIX, name)

/* Public types (and the struct tag / deprecated noun constructor). */
#define sparsemap            SM__P(sparsemap)
#define sm_t                 SM__P(sm_t)
#define sm_allocator_t       SM__P(sm_allocator_t)
#define sm_cursor_t          SM__P(sm_cursor_t)
#define sm_membership_t      SM__P(sm_membership_t)
#define sm_stats_t           SM__P(sm_stats_t)
#define sm_subset_relation_t SM__P(sm_subset_relation_t)

/* Public functions. */
#define sm_add                      SM__P(sm_add)
#define sm_add_grow                 SM__P(sm_add_grow)
#define sm_add_grow_cursor          SM__P(sm_add_grow_cursor)
#define sm_add_many                 SM__P(sm_add_many)
#define sm_add_many_grow            SM__P(sm_add_many_grow)
#define sm_add_range                SM__P(sm_add_range)
#define sm_and                      SM__P(sm_and)
#define sm_andnot                   SM__P(sm_andnot)
#define sm_assign                   SM__P(sm_assign)
#define sm_capacity_remaining       SM__P(sm_capacity_remaining)
#define sm_cardinality              SM__P(sm_cardinality)
#define sm_clear                    SM__P(sm_clear)
#define sm_compare                  SM__P(sm_compare)
#define sm_contains                 SM__P(sm_contains)
#define sm_contains_cached          SM__P(sm_contains_cached)
#define sm_contains_many            SM__P(sm_contains_many)
#define sm_copy                     SM__P(sm_copy)
#define sm_create                   SM__P(sm_create)
#define sm_create_from_array        SM__P(sm_create_from_array)
#define sm_create_from_range        SM__P(sm_create_from_range)
#define sm_create_singleton         SM__P(sm_create_singleton)
#define sm_deserialize              SM__P(sm_deserialize)
#define sm_difference               SM__P(sm_difference)
#define sm_difference_cardinality   SM__P(sm_difference_cardinality)
#define sm_difference_inplace       SM__P(sm_difference_inplace)
#define sm_equals                   SM__P(sm_equals)
#define sm_extract_range            SM__P(sm_extract_range)
#define sm_fill_factor              SM__P(sm_fill_factor)
#define sm_flip_range               SM__P(sm_flip_range)
#define sm_free                     SM__P(sm_free)
#define sm_get_capacity             SM__P(sm_get_capacity)
#define sm_get_data                 SM__P(sm_get_data)
#define sm_get_size                 SM__P(sm_get_size)
#define sm_hash                     SM__P(sm_hash)
#define sm_init                     SM__P(sm_init)
#define sm_intersection             SM__P(sm_intersection)
#define sm_intersection_cardinality SM__P(sm_intersection_cardinality)
#define sm_intersection_inplace     SM__P(sm_intersection_inplace)
#define sm_is_empty                 SM__P(sm_is_empty)
#define sm_is_subset                SM__P(sm_is_subset)
#define sm_is_superset              SM__P(sm_is_superset)
#define sm_jaccard_index            SM__P(sm_jaccard_index)
#define sm_locator_build            SM__P(sm_locator_build)
#define sm_locator_contains         SM__P(sm_locator_contains)
#define sm_locator_free             SM__P(sm_locator_free)
#define sm_locator_rank             SM__P(sm_locator_rank)
#define sm_locator_select           SM__P(sm_locator_select)
#define sm_locator_t                SM__P(sm_locator_t)
#define sm_cursor_cached_t          SM__P(sm_cursor_cached_t)
#define sm_maximum                  SM__P(sm_maximum)
#define sm_membership               SM__P(sm_membership)
#define sm_minimum                  SM__P(sm_minimum)
#define sm_next_member              SM__P(sm_next_member)
#define sm_nonempty_difference      SM__P(sm_nonempty_difference)
#define sm_offset                   SM__P(sm_offset)
#define sm_open                     SM__P(sm_open)
#define sm_open_copy                SM__P(sm_open_copy)
#define sm_or                       SM__P(sm_or)
#define sm_overlap                  SM__P(sm_overlap)
#define sm_owned_copy               SM__P(sm_owned_copy)
#define sm_pop_first                SM__P(sm_pop_first)
#define sm_pop_last                 SM__P(sm_pop_last)
#define sm_prev_member              SM__P(sm_prev_member)
#define sm_rank                     SM__P(sm_rank)
#define sm_remove                   SM__P(sm_remove)
#define sm_remove_range             SM__P(sm_remove_range)
#define sm_scan                     SM__P(sm_scan)
#define sm_select                   SM__P(sm_select)
#define sm_serialize                SM__P(sm_serialize)
#define sm_serialized_size          SM__P(sm_serialized_size)
#define sm_set_allocator            SM__P(sm_set_allocator)
#define sm_set_data_size            SM__P(sm_set_data_size)
#define sm_shrink_to_fit            SM__P(sm_shrink_to_fit)
#define sm_singleton_member         SM__P(sm_singleton_member)
#define sm_span                     SM__P(sm_span)
#define sm_split                    SM__P(sm_split)
#define sm_statistics               SM__P(sm_statistics)
#define sm_subset_compare           SM__P(sm_subset_compare)
#define sm_to_array                 SM__P(sm_to_array)
#define sm_union                    SM__P(sm_union)
#define sm_union_cardinality        SM__P(sm_union_cardinality)
#define sm_union_inplace            SM__P(sm_union_inplace)
#define sm_validate                 SM__P(sm_validate)
#define sm_wrap                     SM__P(sm_wrap)
#define sm_xor                      SM__P(sm_xor)
#define sm_xor_cardinality          SM__P(sm_xor_cardinality)
#endif							/* SPARSEMAP_PREFIX */

/*
 * Custom allocator hooks (process-global, CRoaring-style).
 *
 * Sparsemap allocates memory at construction time (sm_create /
 * sm_wrap / sm_owned_copy / sm_union / etc.), at grow time
 * (sm_set_data_size, sm_*_inplace, sm_*_grow), and at free time.
 *
 * Embedders that need to route those allocations through a custom
 * allocator install one process-wide with sm_set_allocator().  There
 * is no per-map allocator: the hooks are global, and no allocator
 * state is stored in the map (keeping it to three machine words).
 *
 * In the PostgreSQL adaptation, the DEFAULT allocator (when a hook
 * pointer is NULL) routes through palloc/repalloc/pfree rather than
 * libc malloc/realloc/free.
 *
 * Contract for hook implementations:
 *   - malloc(n): at least n bytes of uninitialized memory, or NULL.
 *   - realloc(p, n): grow/shrink, or NULL on failure; p == NULL acts
 *     like malloc(n).
 *   - free(p): release allocation; must accept p == NULL as a no-op.
 *
 * Any individual hook may be NULL; sparsemap falls back to the libc
 * (PostgreSQL palloc family) equivalent for that operation.
 */
typedef struct sm_allocator
{
	void	   *(*malloc) (size_t n);
	void	   *(*realloc) (void *p, size_t n);
	void		(*free) (void *p);
}			sm_allocator_t;

/*
 * Sparsemap structure - three machine words.  The allocation-lineage
 * tag (how m_data was provisioned) is folded into the low 3 bits of
 * m_capacity (capacity is always rounded up to an 8-byte boundary).
 * There is no per-map allocator and no stored cursor; reads accelerate
 * through a caller-owned sm_cursor_t (below).  Nothing here is
 * serialized.
 *
 * Exposed here (SM_EXPOSE_STRUCT) so callers can embed the struct
 * directly by value (e.g. in shared-memory structs like slog.c's
 * SLogState).  The library's own translation unit defines SM_INTERNAL.
 */
#define SM_EXPOSE_STRUCT
#if defined(SM_INTERNAL) || defined(SM_EXPOSE_STRUCT)
struct __attribute__((aligned(8))) sparsemap
{
	size_t		m_capacity;		/* (capacity & ~7) bytes; low 3 bits = lineage */
	size_t		m_data_used;	/* used size of m_data, in bytes */
	uint8	   *m_data;			/* the serialized bitmap data */
};
#endif

/* Short alias used throughout the implementation. */
typedef struct sparsemap sm_t;

/*
 * Backward-compatible type alias.  Upstream v5 renamed the public type
 * to sm_t and dropped sparsemap_t, but the PostgreSQL declarations and
 * the sparsemap_* wrapper shim below keep using sparsemap_t.  This is a
 * plain typedef (never a linker symbol), so it is not namespaced; it
 * resolves to the same prefixed struct tag (struct pg_sparsemap).
 */
typedef struct sparsemap sparsemap_t;

/* Caller-owned read cursor for accelerated sequential lookups.
 *
 * A cursor caches the most-recently-located chunk so a sequence of
 * monotonically non-decreasing idx lookups on an unmutated map resumes
 * the chunk walk from there instead of from chunk 0, turning an
 * otherwise O(N^2) scan into O(N).  Only sm_contains(), sm_next_member(),
 * and sm_prev_member() accept one.  ANY mutation of the map invalidates
 * a cursor; reset it with SM_CURSOR_INIT.  Passing NULL means "no
 * acceleration" (walk from chunk 0); always safe.
 */
typedef struct sm_cursor
{
	size_t		offset;			/* byte offset of cached chunk; SIZE_MAX = invalid */
	uint64		start_idx;		/* cached chunk's start bit */
	size_t		prev_offset;	/* byte offset of the chunk immediately before
								 * `offset` in the buffer, or SIZE_MAX if the
								 * located chunk is the first (or unknown). Set
								 * during the forward walk; used as an O(1)
								 * left-neighbor hint for coalescing so a bulk
								 * ascending build never head-walks. */
}			sm_cursor_t;
#define SM_CURSOR_INIT { (size_t)-1, 0, (size_t)-1 }

/*
 * Caller-owned fixed 8-way MRU chunk cache for point lookups.
 *
 * Unlike sm_cursor_t (which caches ONE chunk and only helps a
 * monotonically non-decreasing scan), this caches the last
 * SM_CACHE_WAYS distinct chunks located, so a workload with a few hot
 * chunks probed in ANY order (clustered but not sorted) resolves most
 * lookups from the cache instead of walking from chunk 0.  The cache
 * is fixed-size and independent of the map's chunk count.
 *
 * Contract (identical to sm_cursor_t):
 *   - Initialize with `sm_cursor_cached_t c = SM_CURSOR_CACHED_INIT;`.
 *   - Pass `&c` to consecutive sm_contains_cached() calls.
 *   - ANY mutation of the map invalidates the cache; reset it with
 *     SM_CURSOR_CACHED_INIT before reusing.  A stale cache is undefined
 *     behavior (it caches byte offsets that a mutation can move).
 *   - Passing NULL means "no cache" (plain sm_contains); always safe.
 */
#define SM_CACHE_WAYS 8
typedef struct sm_cursor_cached
{
	uint64		start_idx[SM_CACHE_WAYS];	/* cached chunk start bits */
	uint64		end_idx[SM_CACHE_WAYS]; /* start + capacity (exclusive top) */
	size_t		offset[SM_CACHE_WAYS];	/* base-relative byte offset */
	uint8		mru;			/* next round-robin slot */
	uint8		valid;			/* bitmask of populated ways */
}			sm_cursor_cached_t;
#define SM_CURSOR_CACHED_INIT { {0}, {0}, {0}, 0, 0 }

/*
 * Transient two-level sqrt(n) directory over a map's chunks.
 *
 * A locator is a caller-owned, read-only acceleration index built once
 * over an UNMUTATED map with sm_locator_build().  It samples every
 * stride-th chunk (stride ~= sqrt(chunk count)) into a superblock
 * directory, so contains / rank / select run in O(sqrt n) instead of
 * O(n) chunks.  Nothing here is serialized and sm_t is unchanged.
 *
 * Contract:
 *   - Build with sm_locator_build(map); free with sm_locator_free().
 *   - ANY mutation of the map invalidates the locator: REBUILD it.
 *   - A stale locator still returns CORRECT results (it detects the
 *     mismatch and falls back to the plain O(n) sm_contains / sm_rank /
 *     sm_select path) but loses the speedup.
 *   - rank/select for value == false fall back to the plain path
 *     (correct, not accelerated); the sqrt speedup is for value==true.
 *
 * The struct layout is exposed only so the type name resolves; treat
 * it as opaque and use it only through the sm_locator_* functions.
 */
typedef struct sm_locator
{
	const sm_t *map;			/* the map this locator indexes */
	size_t		count;			/* chunk count at build time */
	uint64		first_start;	/* first chunk start (staleness fingerprint) */
	uint64		last_start;		/* last chunk start (staleness fingerprint) */
	size_t		last_offset;	/* base-relative offset of the last chunk */
	size_t		stride;			/* chunks per superblock (~sqrt(count)) */
	size_t		n_sb;			/* number of superblock entries */
	uint64	   *sb_start;		/* [n_sb] start index of each stride-th chunk */
	size_t	   *sb_offset;		/* [n_sb] base-relative byte offset of it */
	size_t	   *sb_prefix;		/* [n_sb] cumulative SET bits BEFORE it */
}			sm_locator_t;

/* Sentinel value returned when a lookup finds no matching bit */
#define SM_IDX_MAX UINT64_MAX

/* Evaluates to true when x represents a valid (found) index */
#define SM_FOUND(x) ((x) != SM_IDX_MAX)

/* Evaluates to true when x represents the not-found sentinel */
#define SM_NOT_FOUND(x) ((x) == SM_IDX_MAX)

/* Backward-compatible sentinel macros (used by slog.c and test code) */
#define SPARSEMAP_IDX_MAX SM_IDX_MAX
#define SPARSEMAP_FOUND(x) SM_FOUND(x)
#define SPARSEMAP_NOT_FOUND(x) SM_NOT_FOUND(x)

/* -------------------------------------------------------------------
 * Allocator
 * ------------------------------------------------------------------- */

/* Set the process-wide default allocator hooks */
extern void sm_set_allocator(sm_allocator_t a);

/* -------------------------------------------------------------------
 * Lifecycle
 * ------------------------------------------------------------------- */

/* Allocate a sparsemap with an internal buffer (single palloc block) */
extern sparsemap_t *sm_create(size_t size);

/* Deprecated alias for sm_create() */
extern sparsemap_t *sparsemap(size_t size);

/* Dispose of a sparsemap, regardless of allocation lineage */
extern void sm_free(sparsemap_t *map);

/* Create a deep copy of another sparsemap */
extern sparsemap_t *sm_copy(const sparsemap_t *other);

/* Return a guaranteed-owned, guaranteed-growable copy of any sparsemap */
extern sparsemap_t *sm_owned_copy(const sparsemap_t *map);

/* Allocate a sparsemap_t that wraps a caller-provided buffer */
extern sparsemap_t *sm_wrap(uint8 *data, size_t size);

/* Initialize a caller-allocated sparsemap_t with a buffer (clears to empty) */
extern void sm_init(sparsemap_t *map, uint8 *data, size_t size);

/* Attach to an existing (serialized) sparsemap buffer (does not clear) */
extern void sm_open(sparsemap_t *map, uint8 *data, size_t size);

/* Allocate and deserialize raw bytes into a fresh owned-contiguous map */
extern sparsemap_t *sm_open_copy(const uint8 *data, size_t n, size_t slack);

/* Reset the map to empty without freeing memory */
extern void sm_clear(sparsemap_t *map);

/* Resize the data buffer (may relocate the map if internally allocated) */
extern sparsemap_t *sm_set_data_size(sparsemap_t *map, uint8 *data,
									 size_t size);

/* -------------------------------------------------------------------
 * Capacity and size
 * ------------------------------------------------------------------- */

/* Estimate remaining buffer capacity as a percentage */
extern double sm_capacity_remaining(const sparsemap_t *map);

/* Return the total buffer capacity in bytes */
extern size_t sm_get_capacity(const sparsemap_t *map);

/* Return the number of buffer bytes currently in use */
extern size_t sm_get_size(sparsemap_t *map);

/* Return a pointer to the raw data buffer */
extern void *sm_get_data(const sparsemap_t *map);

/* -------------------------------------------------------------------
 * Single-bit operations
 * ------------------------------------------------------------------- */

/* Test whether the bit at idx is set (cur may be NULL) */
extern bool sm_contains(const sparsemap_t *map, uint64 idx, sm_cursor_t *cur);

/*
 * Test many bits in one left-to-right sweep (batched).  Equivalent to calling
 * sm_contains(map, idxs[i], NULL) for every i, but done in a single
 * O(chunks + n) pass.  idxs MUST be sorted ascending (hard precondition,
 * debug-asserted); unsorted input yields unspecified but memory-safe results.
 */
extern void sm_contains_many(const sparsemap_t *map, const uint64 *idxs,
							 bool *results, size_t n);

/*
 * Test a bit using a caller-owned 8-way MRU chunk cache.  Returns the same
 * value as sm_contains(map, idx, NULL); the cache accelerates repeated lookups
 * into a small working set of chunks probed in any order.  Pass NULL for cache
 * to fall back to plain sm_contains.  See sm_cursor_cached_t for the
 * invalidation contract.
 */
extern bool sm_contains_cached(const sparsemap_t *map, uint64 idx,
							   sm_cursor_cached_t *cache);

/* Set or clear the bit at idx */
extern uint64 sm_assign(sparsemap_t *map, uint64 idx, bool value);

/* Set the bit at idx to 1 */
extern uint64 sm_add(sparsemap_t *map, uint64 idx);

/* Add a bit, growing the map's buffer geometrically if needed */
extern uint64 sm_add_grow(sparsemap_t **map, uint64 idx);

/* Like sm_add_grow but threads a caller-owned cursor for O(N) ascending
 * (append-pattern) construction; see the implementation for the contract. */
extern uint64 sm_add_grow_cursor(sparsemap_t **map, uint64 idx,
								  sm_cursor_t *cur);

/* Clear the bit at idx (set to 0) */
extern uint64 sm_remove(sparsemap_t *map, uint64 idx);

/* -------------------------------------------------------------------
 * Aggregate queries
 * ------------------------------------------------------------------- */

/* Count the total number of set bits (cardinality) */
extern size_t sm_cardinality(sparsemap_t *map);

/* Return the position of the first set bit (minimum) */
extern uint64 sm_minimum(const sparsemap_t *map);

/* Return the position of the last set bit (maximum) */
extern uint64 sm_maximum(const sparsemap_t *map);

/* Return the fraction of bits that are set */
extern double sm_fill_factor(sparsemap_t *map);

/* -------------------------------------------------------------------
 * Rank, select, and span
 * ------------------------------------------------------------------- */

/* Count matching bits in the inclusive range [x, y] */
extern size_t sm_rank(sparsemap_t *map, uint64 x, uint64 y, bool value);

/* Find the position of the n'th matching bit (0-based) */
extern uint64 sm_select(sparsemap_t *map, uint64 n, bool value);

/* Find the first contiguous run of len bits matching value */
extern uint64 sm_span(sparsemap_t *map, uint64 start, size_t len,
					   bool value);

/* -------------------------------------------------------------------
 * sqrt(n) locator: transient O(sqrt n) contains / rank / select
 * ------------------------------------------------------------------- */

/*
 * Build a transient sqrt(n) locator over map.  O(chunk count) one-pass build.
 * The returned locator accelerates sm_locator_contains / _rank / _select to
 * O(sqrt n).  See sm_locator_t for the (re)build-on-mutation contract.  Returns
 * a heap-allocated locator (free with sm_locator_free), or NULL on allocation
 * failure or when map is NULL/empty.
 */
extern sm_locator_t *sm_locator_build(const sparsemap_t *map);

/* Release a locator built by sm_locator_build (NULL-safe) */
extern void sm_locator_free(sm_locator_t *loc);

/* O(sqrt n) membership test; equals sm_contains(map, idx, NULL) */
extern bool sm_locator_contains(const sm_locator_t *loc, uint64 idx);

/*
 * O(sqrt n) rank over inclusive [lo, hi]; equals sm_rank.  For value == true
 * this uses the superblock prefix directory; for value == false it falls back
 * to the plain sm_rank path (correct, not accelerated).
 */
extern size_t sm_locator_rank(const sm_locator_t *loc, uint64 lo, uint64 hi,
							  bool value);

/*
 * O(sqrt n) select; equals sm_select(map, n, value).  For value == true this
 * uses the superblock prefix directory; for value == false it falls back to the
 * plain sm_select path (correct, not accelerated).
 */
extern uint64 sm_locator_select(const sm_locator_t *loc, uint64 n, bool value);

/* -------------------------------------------------------------------
 * Iteration
 * ------------------------------------------------------------------- */

/*
 * Invoke a callback for every set bit in the map (batches of up to 64).
 * Each index is an absolute 64-bit bit position, so the callback array
 * is uint64.
 */
extern void sm_scan(const sparsemap_t *map,
					void (*scanner) (uint64 vec[], size_t n, void *aux),
					size_t skip, void *aux);

/* -------------------------------------------------------------------
 * Bulk operations
 * ------------------------------------------------------------------- */

/* Create a new sparsemap containing bits set in either a or b (OR) */
extern sparsemap_t *sm_union(const sparsemap_t *a, const sparsemap_t *b);

/* Create a new sparsemap containing bits set in both a and b (AND) */
extern sparsemap_t *sm_intersection(const sparsemap_t *a,
									const sparsemap_t *b);

/* Create a new sparsemap containing bits in a but not in b (AND NOT) */
extern sparsemap_t *sm_difference(const sparsemap_t *a,
								  const sparsemap_t *b);

/* Split the map at idx, moving higher bits to other */
extern uint64 sm_split(sparsemap_t *map, uint64 idx, sparsemap_t *other);

/* Create a new sparsemap with all bits shifted by offset */
extern sparsemap_t *sm_offset(const sparsemap_t *map, ssize_t offset);

/* -------------------------------------------------------------------
 * Predicates and comparisons
 * ------------------------------------------------------------------- */

/* Test whether a sparsemap is empty (has no set bits) */
extern bool sm_is_empty(const sparsemap_t *map);

/* Test bit-set equality of two sparsemaps */
extern bool sm_equals(const sparsemap_t *a, const sparsemap_t *b);

/* Test whether a's bits are a subset of b's bits */
extern bool sm_is_subset(const sparsemap_t *a, const sparsemap_t *b);

/* Test whether a's bits are a superset of b's bits */
extern bool sm_is_superset(const sparsemap_t *a, const sparsemap_t *b);

/* Test whether two sparsemaps share at least one set bit */
extern bool sm_overlap(const sparsemap_t *a, const sparsemap_t *b);

/* Membership classification */
typedef enum
{
	SM_EMPTY = 0,				/* no bits set */
	SM_SINGLETON = 1,			/* exactly one bit set */
	SM_MULTIPLE = 2				/* two or more bits set */
}			sm_membership_t;

/* Classify a sparsemap as empty, singleton, or multi-element */
extern sm_membership_t sm_membership(const sparsemap_t *map);

/* Return the sole member of a singleton sparsemap */
extern uint64 sm_singleton_member(const sparsemap_t *map);

/* -------------------------------------------------------------------
 * Member-by-member iteration
 * ------------------------------------------------------------------- */

/* Find the lowest set bit at index > prev_idx (cur may be NULL) */
extern uint64 sm_next_member(const sparsemap_t *map, uint64 prev_idx,
							 sm_cursor_t *cur);

/* Find the highest set bit at index < prev_idx (cur may be NULL) */
extern uint64 sm_prev_member(const sparsemap_t *map, uint64 prev_idx,
							 sm_cursor_t *cur);

/* -------------------------------------------------------------------
 * Cardinality without allocation
 * ------------------------------------------------------------------- */

/* Compute |a UNION b| without allocating the union */
extern size_t sm_union_cardinality(const sparsemap_t *a,
								   const sparsemap_t *b);

/* Compute |a INTERSECT b| without allocating the intersection */
extern size_t sm_intersection_cardinality(const sparsemap_t *a,
										  const sparsemap_t *b);

/* Compute |a \ b| without allocating the difference */
extern size_t sm_difference_cardinality(const sparsemap_t *a,
										const sparsemap_t *b);

/* Test whether a \ b has any set bits, without allocating */
extern bool sm_nonempty_difference(const sparsemap_t *a,
								   const sparsemap_t *b);

/* Jaccard similarity index: |a INTERSECT b| / |a UNION b| */
extern double sm_jaccard_index(const sparsemap_t *a, const sparsemap_t *b);

/* -------------------------------------------------------------------
 * Bulk add and array conversion
 * ------------------------------------------------------------------- */

/* Add N indices from an array */
extern bool sm_add_many(sparsemap_t *map, const uint64 *arr, size_t n);

/* Like sm_add_many but grows the buffer geometrically as needed */
extern bool sm_add_many_grow(sparsemap_t **map, const uint64 *arr, size_t n);

/* Materialize all set bits as a uint64 array */
extern void sm_to_array(const sparsemap_t *map, uint64 *out, size_t *n_out);

/* -------------------------------------------------------------------
 * Range manipulation and symmetric difference
 * ------------------------------------------------------------------- */

/* Set every bit in [lo, hi) */
extern bool sm_add_range(sparsemap_t *map, uint64 lo, uint64 hi);

/* Clear every bit in [lo, hi) */
extern bool sm_remove_range(sparsemap_t *map, uint64 lo, uint64 hi);

/* Extract a range of bits as a new sparsemap */
extern sparsemap_t *sm_extract_range(const sparsemap_t *map, uint64 lo,
									 uint64 hi);

/* Symmetric difference: bits set in exactly one of a, b */
extern sparsemap_t *sm_xor(const sparsemap_t *a, const sparsemap_t *b);

/* Synonym for sm_union (logical OR) */
extern sparsemap_t *sm_or(const sparsemap_t *a, const sparsemap_t *b);

/* Synonym for sm_intersection (logical AND) */
extern sparsemap_t *sm_and(const sparsemap_t *a, const sparsemap_t *b);

/* Synonym for sm_difference (logical AND-NOT) */
extern sparsemap_t *sm_andnot(const sparsemap_t *a, const sparsemap_t *b);

/* XOR cardinality without allocation */
extern size_t sm_xor_cardinality(const sparsemap_t *a,
								 const sparsemap_t *b);

/* -------------------------------------------------------------------
 * Constructors
 * ------------------------------------------------------------------- */

/* Create a sparsemap containing exactly the bit at idx */
extern sparsemap_t *sm_create_singleton(uint64 idx);

/* Create a sparsemap containing every bit in [lo, hi) */
extern sparsemap_t *sm_create_from_range(uint64 lo, uint64 hi);

/* Create a sparsemap from an array of indices */
extern sparsemap_t *sm_create_from_array(const uint64 *arr, size_t n);

/* -------------------------------------------------------------------
 * Hashing and comparison
 * ------------------------------------------------------------------- */

/* Stable content-based hash of the bit set */
extern uint64 sm_hash(const sparsemap_t *map);

/* Three-way compare for ordering bitmaps */
extern int sm_compare(const sparsemap_t *a, const sparsemap_t *b);

/* Subset-relation between two sparsemaps */
typedef enum
{
	SM_REL_EQUAL = 0,			/* a == b */
	SM_REL_SUBSET_A = 1,		/* a is a strict subset of b */
	SM_REL_SUBSET_B = 2,		/* b is a strict subset of a */
	SM_REL_DIFFERENT = 3		/* neither is a subset of the other */
}			sm_subset_relation_t;

/* Classify the subset relationship between a and b */
extern sm_subset_relation_t sm_subset_compare(const sparsemap_t *a,
											  const sparsemap_t *b);

/* -------------------------------------------------------------------
 * Destructive iteration
 * ------------------------------------------------------------------- */

/* Find the lowest set bit, clear it, and return it */
extern uint64 sm_pop_first(sparsemap_t *map);

/* Find the highest set bit, clear it, and return it */
extern uint64 sm_pop_last(sparsemap_t *map);

/* -------------------------------------------------------------------
 * In-place set operations
 * ------------------------------------------------------------------- */

/* In-place union: dst := dst U src */
extern sparsemap_t *sm_union_inplace(sparsemap_t *dst,
									 const sparsemap_t *src);

/* In-place intersection: dst := dst INT src */
extern sparsemap_t *sm_intersection_inplace(sparsemap_t *dst,
											const sparsemap_t *src);

/* In-place difference: dst := dst \ src */
extern sparsemap_t *sm_difference_inplace(sparsemap_t *dst,
										  const sparsemap_t *src);

/* -------------------------------------------------------------------
 * Range complement
 * ------------------------------------------------------------------- */

/* Complement every bit in [lo, hi) */
extern bool sm_flip_range(sparsemap_t *map, uint64 lo, uint64 hi);

/* -------------------------------------------------------------------
 * Maintenance and introspection
 * ------------------------------------------------------------------- */

/* Runtime self-check of internal consistency */
extern bool sm_validate(const sparsemap_t *map);

/* Statistics about a sparsemap's internal layout */
typedef struct sm_stats
{
	size_t		chunks_total;	/* total chunks */
	size_t		chunks_rle;		/* chunks using RLE encoding */
	size_t		chunks_sparse;	/* chunks using sparse encoding */
	size_t		bytes_used;		/* sm_get_size(map) */
	size_t		bytes_capacity; /* sm_get_capacity(map) */
	uint64		bits_set;		/* sm_cardinality(map) */
	uint64		bits_in_rle;	/* bits set within RLE chunks */
	uint64		bits_in_sparse; /* bits set within sparse chunks */
	double		bytes_per_set_bit;	/* bytes_used / bits_set */
}			sm_stats_t;

/* Fill an sm_stats_t with introspection data */
extern void sm_statistics(const sparsemap_t *map, sm_stats_t *stats);

/* Realloc the data buffer down to exactly m_data_used bytes */
extern sparsemap_t *sm_shrink_to_fit(sparsemap_t *map);

/* -------------------------------------------------------------------
 * Portable serialization
 * ------------------------------------------------------------------- */

/* Compute the buffer size needed to serialize map */
extern size_t sm_serialized_size(const sparsemap_t *map);

/* Serialize map into out (sm_serialized_size bytes) */
extern size_t sm_serialize(const sparsemap_t *map, uint8 *out,
						   size_t out_size);

/* Deserialize a previously-serialized buffer into a fresh map */
extern sparsemap_t *sm_deserialize(const uint8 *in, size_t n);

/* -------------------------------------------------------------------
 * Backward-compatible function names (sparsemap_ prefix)
 *
 * These inline wrappers allow existing callers (slog.c, test code) to
 * continue using the sparsemap_* names without modification.
 * ------------------------------------------------------------------- */

static inline sparsemap_t *
sparsemap_create(size_t size)
{
	return sm_create(size);
}

static inline void
sparsemap_free(sparsemap_t *map)
{
	sm_free(map);
}

static inline sparsemap_t *
sparsemap_copy(const sparsemap_t *other)
{
	return sm_copy(other);
}

static inline sparsemap_t *
sparsemap_owned_copy(const sparsemap_t *map)
{
	return sm_owned_copy(map);
}

static inline sparsemap_t *
sparsemap_wrap(uint8 *data, size_t size)
{
	return sm_wrap(data, size);
}

static inline void
sparsemap_init(sparsemap_t *map, uint8 *data, size_t size)
{
	sm_init(map, data, size);
}

static inline void
sparsemap_open(sparsemap_t *map, uint8 *data, size_t size)
{
	sm_open(map, data, size);
}

static inline void
sparsemap_clear(sparsemap_t *map)
{
	sm_clear(map);
}

static inline sparsemap_t *
sparsemap_set_data_size(sparsemap_t *map, uint8 *data, size_t size)
{
	return sm_set_data_size(map, data, size);
}

static inline double
sparsemap_capacity_remaining(const sparsemap_t *map)
{
	return sm_capacity_remaining(map);
}

static inline size_t
sparsemap_get_capacity(const sparsemap_t *map)
{
	return sm_get_capacity(map);
}

static inline size_t
sparsemap_get_size(sparsemap_t *map)
{
	return sm_get_size(map);
}

static inline void *
sparsemap_get_data(const sparsemap_t *map)
{
	return sm_get_data(map);
}

static inline bool
sparsemap_contains(const sparsemap_t *map, uint64 idx)
{
	return sm_contains(map, idx, NULL);
}

static inline uint64
sparsemap_assign(sparsemap_t *map, uint64 idx, bool value)
{
	return sm_assign(map, idx, value);
}

static inline uint64
sparsemap_add(sparsemap_t *map, uint64 idx)
{
	return sm_add(map, idx);
}

static inline uint64
sparsemap_remove(sparsemap_t *map, uint64 idx)
{
	return sm_remove(map, idx);
}

static inline size_t
sparsemap_cardinality(sparsemap_t *map)
{
	return sm_cardinality(map);
}

static inline uint64
sparsemap_minimum(const sparsemap_t *map)
{
	return sm_minimum(map);
}

static inline uint64
sparsemap_maximum(const sparsemap_t *map)
{
	return sm_maximum(map);
}

static inline double
sparsemap_fill_factor(sparsemap_t *map)
{
	return sm_fill_factor(map);
}

static inline size_t
sparsemap_rank(sparsemap_t *map, uint64 x, uint64 y, bool value)
{
	return sm_rank(map, x, y, value);
}

static inline uint64
sparsemap_select(sparsemap_t *map, uint64 n, bool value)
{
	return sm_select(map, n, value);
}

static inline uint64
sparsemap_span(sparsemap_t *map, uint64 start, size_t len, bool value)
{
	return sm_span(map, start, len, value);
}

static inline void
sparsemap_scan(const sparsemap_t *map,
			   void (*scanner) (uint64 vec[], size_t n, void *aux),
			   size_t skip, void *aux)
{
	sm_scan(map, scanner, skip, aux);
}

static inline sparsemap_t *
sparsemap_union(const sparsemap_t *a, const sparsemap_t *b)
{
	return sm_union(a, b);
}

static inline sparsemap_t *
sparsemap_intersection(const sparsemap_t *a, const sparsemap_t *b)
{
	return sm_intersection(a, b);
}

static inline sparsemap_t *
sparsemap_difference(const sparsemap_t *a, const sparsemap_t *b)
{
	return sm_difference(a, b);
}

static inline uint64
sparsemap_split(sparsemap_t *map, uint64 idx, sparsemap_t *other)
{
	return sm_split(map, idx, other);
}

static inline sparsemap_t *
sparsemap_offset(const sparsemap_t *map, ssize_t offset)
{
	return sm_offset(map, offset);
}

#endif							/* SPARSEMAP_H */
