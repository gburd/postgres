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

/* Library version (kept in sync with upstream sparsemap v2.3.0). */
#define SM_VERSION_STRING "2.3.0"
#define SM_VERSION_MAJOR  2
#define SM_VERSION_MINOR  3
#define SM_VERSION_PATCH  0

/*
 * Custom allocator hooks.
 *
 * Sparsemap allocates memory in three places: at construction time
 * (sm_create / sm_wrap / sm_owned_copy / sm_union / etc.), at grow
 * time (sm_set_data_size, sm_*_inplace, sm_*_grow), and at free time.
 *
 * Embedders that need to route those allocations through a custom
 * allocator (e.g. arena allocators, alternate memory contexts) can
 * supply a sm_allocator_t.  Two scopes:
 *
 *   sm_set_allocator(hooks)             process-wide default; affects
 *                                       every sparsemap created without
 *                                       an explicit override.
 *
 *   sm_create_with_allocator(n, hooks)  per-map override; the supplied
 *                                       hooks are copied into the map.
 *
 * In the PostgreSQL adaptation, the DEFAULT allocator (when all hook
 * pointers are NULL) routes through palloc/pfree/repalloc rather than
 * libc malloc/free/realloc.
 *
 * Contract for hook implementations:
 *   - alloc(n, aux): at least n bytes of uninitialized memory, or NULL.
 *   - alloc_zero(n, aux): at least n bytes zero-filled, or NULL.
 *   - realloc(p, n, aux): grow/shrink, or NULL on failure.
 *   - free(p, aux): release allocation; must accept p == NULL as no-op.
 *   - aligned_alloc / aligned_free: reserved for future SIMD work.
 */
typedef struct sm_allocator
{
	void	   *(*alloc) (size_t n, void *aux);
	void	   *(*alloc_zero) (size_t n, void *aux);
	void	   *(*realloc) (void *p, size_t n, void *aux);
	void		(*free) (void *p, void *aux);
	void	   *(*aligned_alloc) (size_t alignment, size_t n, void *aux);
	void		(*aligned_free) (void *p, void *aux);
	void	   *aux;
}			sm_allocator_t;

/*
 * Sparsemap structure - contains metadata and a pointer to the data buffer.
 * Exposed here so callers can embed the struct directly (e.g. in shared
 * memory structs like slog.c's SLogState).
 */
typedef struct sparsemap
{
	size_t		m_capacity;		/* total buffer capacity in bytes */
	size_t		m_data_used;	/* bytes currently used in the buffer */
	uint8	   *m_data;			/* pointer to the data buffer */
	uint8		m_alloc_kind;	/* allocation lineage tag */
	sm_allocator_t m_allocator; /* per-map allocator hooks (v2.2.0) */
}			sparsemap_t;

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

/* Allocate with a per-map allocator override */
extern sparsemap_t *sm_create_with_allocator(size_t size, sm_allocator_t a);

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

/* Test whether the bit at idx is set */
extern bool sm_contains(sparsemap_t *map, uint64 idx);

/* Set or clear the bit at idx */
extern uint64 sm_assign(sparsemap_t *map, uint64 idx, bool value);

/* Set the bit at idx to 1 */
extern uint64 sm_add(sparsemap_t *map, uint64 idx);

/* Add a bit, growing the map's buffer geometrically if needed */
extern uint64 sm_add_grow(sparsemap_t **map, uint64 idx);

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
 * Iteration
 * ------------------------------------------------------------------- */

/* Invoke a callback for every set bit in the map (batches of up to 64) */
extern void sm_scan(const sparsemap_t *map,
					void (*scanner) (uint32 vec[], size_t n, void *aux),
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

/* Find the lowest set bit at index > prev_idx */
extern uint64 sm_next_member(const sparsemap_t *map, uint64 prev_idx);

/* Find the highest set bit at index < prev_idx */
extern uint64 sm_prev_member(const sparsemap_t *map, uint64 prev_idx);

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
sparsemap_contains(sparsemap_t *map, uint64 idx)
{
	return sm_contains(map, idx);
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
			   void (*scanner) (uint32 vec[], size_t n, void *aux),
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
