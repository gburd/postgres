/*-------------------------------------------------------------------------
 *
 * sbm.h
 *	  Sparse bitmap set: a compressed, mutable set of uint64 indexes.
 *
 * A sparse bitmap set (Sbm) is a resizable, compressed bitmap for workloads
 * with long runs of consecutive set or unset bits.  For example the set of
 * heap TIDs in an index posting list, or an allocation bitmap.  It stores
 * only the parts of the universe that contain data, so a dense or clustered
 * set costs a small fraction of a raw bitmap while a scattered set costs no
 * more than one plus a constant.
 *
 * This is a plain helper ADT, like Bitmapset, integerset, and the radix tree
 * in src/backend/lib.  It is NOT a Node type and carries no type OID; callers
 * that need to persist one use sbm_serialize() / sbm_deserialize().
 *
 * Representation
 * --------------
 *
 * A map is in one of two modes, and switches between them transparently:
 *
 * Small-set mode.  While every member is below SBM_SMALL_MAX_BITS (1024),
 * the map may store a bare uint64 word array from bit 0, exactly like
 * Bitmapset, behind an 8-byte header.  It is promoted to chunk mode when a
 * member leaves that span (or when chunk mode would be smaller), and is
 * demoted back when removals or set operations bring every member under it.
 *
 * Chunk mode.  The universe is divided into chunks, stored in ascending
 * order, each prefixed by its 64-bit starting index.  A chunk uses one of
 * two encodings:
 *
 *	- Sparse: a 64-bit descriptor holds a 2-bit flag for each of 32 64-bit
 *	  vectors (2048 bits).  Only vectors with a mix of set and unset bits
 *	  are stored after the descriptor; all-zero and all-one vectors are
 *	  represented by their flag alone:
 *
 *		00	all zeros	(vector not stored)
 *		11	all ones	(vector not stored)
 *		10	mixed		(vector stored after the descriptor)
 *		01	unused		(reduces the chunk's capacity)
 *
 *	- RLE: a single descriptor word represents a run of set bits starting
 *	  at the chunk's first index:
 *
 *		bits 63:62	01 (RLE tag; a sparse chunk never uses 01 there)
 *		bits 61:31	chunk capacity in bits (up to 2^31 - 1)
 *		bits 30:0	run length in bits (up to 2^31 - 1)
 *
 *	  Bits [0, length) are set; bits [length, capacity) are unset.
 *
 * A sparse chunk that becomes all ones turns into RLE when an adjacent bit
 * is set; adjacent chunks that form one run are coalesced into a single RLE
 * chunk; and clearing a bit inside an RLE run separates it back into sparse
 * and smaller RLE chunks.  None of this is visible through the API: every
 * function behaves identically regardless of the encoding of its inputs.
 *
 * Memory management
 * -----------------
 *
 * Library-owned storage comes from CurrentMemoryContext via palloc, so, as
 * with the rest of the backend, allocation failure raises an ERROR rather
 * than returning NULL.  A NULL result from a constructor or set operation
 * therefore always means "the result is the empty set", never "out of
 * memory".
 *
 * Each map records how its buffer was provided (its lineage), which decides
 * what the library may repalloc or pfree:
 *
 *	Constructor						Disposal
 *	-------------------------------	-------------------------------------
 *	sbm_create, sbm_copy,			sbm_free() (or pfree(); the struct
 *	sbm_owned_copy, sbm_open_copy,	and buffer share one chunk)
 *	sbm_deserialize, set ops
 *	sbm_anchor					sbm_free() frees the struct only; the
 *									caller's buffer is untouched
 *	sbm_init, sbm_open				nothing: struct and buffer both belong
 *									to the caller
 *
 * Growing a wrapped map with sbm_set_data_size(map, NULL, larger) copies the
 * in-use bytes into a new library-owned buffer; afterwards the map must be
 * released with sbm_free().
 *
 * Errors
 * ------
 *
 * Point mutations on a caller-sized buffer (sbm_add, sbm_remove,
 * sbm_assign, sbm_split, ...) return SBM_IDX_MAX and set errno to ENOSPC
 * when the buffer is full; grow it with sbm_set_data_size() and retry, or
 * use the *_grow variants, which do so automatically.  Operations handed a
 * NULL map treat it as an empty, read-only set: queries return the value an
 * empty set would, and mutators return their failure value and set errno to
 * EINVAL.
 *
 * Sbm is not thread-safe.  Concurrent readers are safe only when no writer
 * is active.
 *
 * The encoding and algorithms are derived from the sparsemap library by
 * Christoph Rupp and Gregory Burd; the full MIT license notice is reproduced
 * at the top of sbm.c.
 *
 * Portions Copyright (c) 2014, Christoph Rupp
 * Portions Copyright (c) 2024, Gregory Burd
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/include/lib/sbm.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SBM_H
#define SBM_H

#include <sys/types.h>			/* for ssize_t */

/*
 * Handle to a sparse bitmap.
 *
 * Opaque by default: allocate with sbm_create() or sbm_anchor() and access it
 * only through the sbm_* functions.  Define SBM_EXPOSE_STRUCT before
 * including this header to see the layout, which is needed only to embed an
 * Sbm by value (for example in a shared-memory control block); such code
 * must be recompiled whenever the layout changes.  Nothing in the struct is
 * part of the serialized format.
 */
typedef struct Sbm Sbm;

#if defined(SBM_INTERNAL) || defined(SBM_EXPOSE_STRUCT)
/*
 * The allocation lineage is folded into the low three bits of m_capacity
 * (capacity is always a multiple of 8).  m_card_plus1 caches
 * sbm_cardinality() biased by one, so a zeroed struct reads as "unknown";
 * every mutation resets it.
 */
struct pg_attribute_aligned (8)
Sbm
{
	size_t		m_capacity;		/* capacity in bytes | lineage tag */
	size_t		m_data_used;	/* bytes of m_data in use */
	uint8	   *m_data;			/* the encoded bitmap */
	size_t		m_card_plus1;	/* cached cardinality + 1, or 0 */
};
#endif

/*
 * Caller-owned read cursor for sequential lookups.
 *
 * A cursor remembers the most recently located chunk, so a sequence of
 * lookups with non-decreasing indexes on an unmodified map resumes the walk
 * there instead of from the first chunk, turning an O(N^2) scan into O(N).
 * Initialize it with SBM_CURSOR_INIT.  Any mutation of the map invalidates
 * it; reset it with SBM_CURSOR_INIT before reuse.  Passing NULL wherever a
 * cursor is accepted means "no acceleration" and is always safe.
 */
typedef struct SbmCursor
{
	size_t		offset;			/* byte offset of cached chunk, or SIZE_MAX */
	uint64		start_idx;		/* cached chunk's first index */
	size_t		prev_offset;	/* chunk before offset, or SIZE_MAX */
} SbmCursor;

#define SBM_CURSOR_INIT		{SIZE_MAX, 0, SIZE_MAX}

/*
 * Caller-owned 8-way MRU chunk cache for point lookups.
 *
 * Unlike SbmCursor, which helps only an ascending scan, this remembers the
 * last SBM_CACHE_WAYS distinct chunks located, so lookups into a small hot
 * set of chunks in any order resolve from the cache.  Same invalidation
 * contract as SbmCursor; initialize with SBM_CURSOR_CACHED_INIT.
 */
#define SBM_CACHE_WAYS 8

typedef struct SbmCursorCached
{
	uint64		start_idx[SBM_CACHE_WAYS];	/* cached chunk first index */
	uint64		end_idx[SBM_CACHE_WAYS];	/* exclusive upper bound */
	size_t		offset[SBM_CACHE_WAYS]; /* byte offset of the chunk */
	uint8		mru;			/* next round-robin slot */
	uint8		valid;			/* bitmask of populated ways */
} SbmCursorCached;

#define SBM_CURSOR_CACHED_INIT	{{0}, {0}, {0}, 0, 0}

/*
 * Transient sqrt(n) directory over a map's chunks.
 *
 * Built once over an unmodified map with sbm_locator_build(), it samples
 * every stride-th chunk (stride ~ sqrt(chunk count)) so that membership,
 * rank and select run in O(sqrt n) chunks instead of O(n).  A locator that
 * has gone stale because the map was modified still returns correct
 * answers, by falling back to the plain functions, but loses the speedup;
 * rebuild it after mutating the map.  Treat the struct as opaque.
 */
typedef struct SbmLocator
{
	const Sbm  *map;			/* the map this locator indexes */
	size_t		count;			/* chunk count at build time */
	uint64		first_start;	/* first chunk start (staleness check) */
	uint64		last_start;		/* last chunk start (staleness check) */
	size_t		last_offset;	/* byte offset of the last chunk */
	size_t		stride;			/* chunks per superblock */
	size_t		n_sb;			/* number of superblocks */
	uint64	   *sb_start;		/* [n_sb] first index of each superblock */
	size_t	   *sb_offset;		/* [n_sb] byte offset of each superblock */
	size_t	   *sb_prefix;		/* [n_sb] set bits before each superblock */
} SbmLocator;

/* Result of sbm_membership() */
typedef enum SbmMembership
{
	SBM_EMPTY = 0,				/* no members */
	SBM_SINGLETON = 1,			/* exactly one member */
	SBM_MULTIPLE = 2,			/* two or more members */
} SbmMembership;

/* Result of sbm_subset_compare() */
typedef enum SbmSubsetRelation
{
	SBM_REL_EQUAL = 0,			/* a == b */
	SBM_REL_SUBSET_A = 1,		/* a is a strict subset of b */
	SBM_REL_SUBSET_B = 2,		/* b is a strict subset of a */
	SBM_REL_DIFFERENT = 3,		/* neither is a subset of the other */
} SbmSubsetRelation;

/* Layout statistics, filled in by sbm_statistics() */
typedef struct SbmStats
{
	size_t		chunks_total;	/* number of chunks */
	size_t		chunks_rle;		/* chunks using the RLE encoding */
	size_t		chunks_sparse;	/* chunks using the sparse encoding */
	size_t		bytes_used;		/* sbm_get_size() */
	size_t		bytes_capacity; /* sbm_get_capacity() */
	uint64		bits_set;		/* sbm_cardinality() */
	uint64		bits_in_rle;	/* members held in RLE chunks */
	uint64		bits_in_sparse; /* members held in sparse chunks */
	double		bytes_per_set_bit;	/* bytes_used / bits_set */
} SbmStats;

/* Sentinel returned by index-valued functions when there is no answer. */
#define SBM_IDX_MAX			PG_UINT64_MAX

/* True when an index-valued result is a real index. */
#define SBM_FOUND(x)		((x) != SBM_IDX_MAX)

/* True when an index-valued result is the SBM_IDX_MAX sentinel. */
#define SBM_NOT_FOUND(x)	((x) == SBM_IDX_MAX)


/*
 * Construction and disposal
 */

/*
 * Allocate an empty map with a size-byte buffer (0 means 1024 bytes); the
 * struct and buffer share one palloc chunk.
 */
pg_nodiscard extern Sbm *sbm_create(size_t size);

/*
 * Release a map and any buffer the library owns.  A wrapped map's buffer is
 * left alone.  Do not call on an sbm_init()/sbm_open() map, whose struct is
 * the caller's.  NULL is a no-op.
 */
extern void sbm_free(Sbm *map);

/* Deep copy, preserving capacity.  Returns NULL for a NULL input. */
pg_nodiscard extern Sbm *sbm_copy(const Sbm *other);

/*
 * Deep copy that is always library-owned and growable, whatever the
 * source's lineage.  Returns NULL for a NULL input.
 */
pg_nodiscard extern Sbm *sbm_owned_copy(const Sbm *map);

/*
 * Allocate a map struct around a caller-owned, 8-byte-aligned buffer of
 * size bytes.  The buffer is not cleared; call sbm_clear() to start empty,
 * or sbm_open() to adopt existing contents.
 *
 * Use this (rather than sbm_create()) when the buffer is contiguous with, or
 * embedded in, a larger caller object whose address can move.  After the
 * enclosing object is relocated, call sbm_reanchor(); grow such a map only
 * through the *_embedded entry points below.
 */
pg_nodiscard extern Sbm *sbm_anchor(uint8 *data, size_t size);

/*
 * Repoint an anchored map at a (possibly moved) data buffer, after the
 * object the buffer is embedded in has been relocated.  Leaves capacity,
 * used size and lineage unchanged.
 */
extern void sbm_reanchor(Sbm *map, uint8 *data);

/*
 * Initialize a caller-allocated struct over a caller-owned buffer and clear
 * it to the empty set.  Nothing is allocated.
 */
extern void sbm_init(Sbm *map, uint8 *data, size_t size);

/*
 * Like sbm_init(), but adopt the buffer's existing (previously encoded)
 * contents instead of clearing it.  A buffer that fails validation is reset
 * to the empty set.
 */
extern void sbm_open(Sbm *map, uint8 *data, size_t size);

/*
 * Allocate a library-owned map holding a copy of the n encoded bytes at
 * data, with slack extra bytes of capacity.  Returns NULL if the bytes are
 * not a valid encoding.
 */
pg_nodiscard extern Sbm *sbm_open_copy(const uint8 *data, size_t n,
									   size_t slack);

/* Reset to the empty set, keeping the buffer. */
extern void sbm_clear(Sbm *map);

/*
 * Resize the buffer.
 *
 * With data == NULL the library manages the buffer: a library-owned map is
 * repalloc'd (and may move -- always use the returned pointer), and a
 * wrapped map that must grow is copied into a new library-owned buffer.
 * With data != NULL the map is pointed at that caller-owned buffer, into
 * which the caller must already have copied the in-use bytes.  Returns NULL
 * only for a NULL map.
 */
pg_nodiscard extern Sbm *sbm_set_data_size(Sbm *map, uint8 *data,
										   size_t size);

/*
 * Repalloc a library-owned buffer down to the bytes in use.  Wrapped maps
 * are returned unchanged.  Always use the returned pointer.
 */
pg_nodiscard extern Sbm *sbm_shrink_to_fit(Sbm *map);

/* Map holding exactly idx. */
pg_nodiscard extern Sbm *sbm_create_singleton(uint64 idx);

/* Map holding every index in [lo, hi); an empty range gives an empty map. */
pg_nodiscard extern Sbm *sbm_create_from_range(uint64 lo, uint64 hi);

/*
 * Map holding the n indexes in arr (any order, duplicates allowed).  Returns
 * NULL with errno = EINVAL if arr is NULL and n > 0.
 */
pg_nodiscard extern Sbm *sbm_create_from_array(const uint64 *arr, size_t n);


/*
 * Buffer introspection
 */

/* Buffer capacity in bytes. */
extern size_t sbm_get_capacity(const Sbm *map);

/*
 * Bytes of the buffer in use; only this prefix of sbm_get_data() need be
 * stored to persist the map's raw encoding.
 */
extern size_t sbm_get_size(const Sbm *map);

/* Pointer to the raw encoded buffer (NULL for a NULL map). */
extern void *sbm_get_data(const Sbm *map);

/*
 * Rough percentage, in [0, 100], of the buffer still unused.  Because the
 * encoding is compressed it is not monotonic in the number of members.
 */
extern double sbm_capacity_remaining(const Sbm *map);


/*
 * Membership
 */

/* Is idx a member?  cur may be NULL; see SbmCursor. */
extern bool sbm_contains(const Sbm *map, uint64 idx, SbmCursor *cur);

/*
 * results[i] = sbm_contains(map, idxs[i]) for i in [0, n), in one O(chunks +
 * n) sweep.  idxs must be sorted ascending; unsorted input gives
 * unspecified (but memory-safe) results.
 */
extern void sbm_contains_many(const Sbm *map, const uint64 *idxs,
							  bool *results, size_t n);

/* sbm_contains() through an SbmCursorCached (which may be NULL). */
extern bool sbm_contains_cached(const Sbm *map, uint64 idx,
								SbmCursorCached *cache);


/*
 * Point mutation
 *
 * These return idx on success, or SBM_IDX_MAX with errno = ENOSPC if the
 * buffer is too small (or EINVAL for a NULL map).
 */

/* Add idx. */
extern uint64 sbm_add(Sbm *map, uint64 idx);

/* Remove idx. */
extern uint64 sbm_remove(Sbm *map, uint64 idx);

/* sbm_add() if value, else sbm_remove(). */
extern uint64 sbm_assign(Sbm *map, uint64 idx, bool value);

/*
 * Add idx, growing the buffer as needed and updating *map.  A NULL *map is
 * created on first use, as with bms_add_member(NULL, x).
 */
extern uint64 sbm_add_grow(Sbm **map, uint64 idx);

/*
 * sbm_add_grow() that threads a caller-owned cursor, making an ascending
 * build O(N).  The cursor is reset automatically when the map moves.
 */
extern uint64 sbm_add_grow_cursor(Sbm **map, uint64 idx, SbmCursor *cur);

/*
 * Remove and return the smallest (largest) member, or SBM_IDX_MAX if the
 * map is empty.
 */
extern uint64 sbm_pop_first(Sbm *map);
extern uint64 sbm_pop_last(Sbm *map);


/*
 * Bulk mutation
 */

/*
 * Add the n indexes in arr (any order) in one O(n log n + chunks) pass.
 * Returns false if the result does not fit in map's buffer, in which case
 * map is unchanged; use sbm_add_many_grow() to grow instead.
 */
extern bool sbm_add_many(Sbm *map, const uint64 *arr, size_t n);

/* sbm_add_many() that grows *map as needed; a NULL *map is created. */
extern bool sbm_add_many_grow(Sbm **map, const uint64 *arr, size_t n);

/*
 * Add, remove, or complement every index in [lo, hi); lo >= hi is a no-op.
 * Return false (errno = ENOSPC) if the buffer is too small.
 */
extern bool sbm_add_range(Sbm *map, uint64 lo, uint64 hi);
extern bool sbm_remove_range(Sbm *map, uint64 lo, uint64 hi);
extern bool sbm_flip_range(Sbm *map, uint64 lo, uint64 hi);


/*
 * Aggregate queries
 */

/* True if the map has no members; O(1). */
extern bool sbm_is_empty(const Sbm *map);

/* Number of members; O(1) after the first call following a mutation. */
extern size_t sbm_cardinality(const Sbm *map);

/* Smallest (largest) member, or 0 for an empty map. */
extern uint64 sbm_minimum(const Sbm *map);
extern uint64 sbm_maximum(const Sbm *map);

/*
 * Fraction of [minimum, maximum] that is set, in [0, 1]; 0 for an empty
 * map.
 */
extern double sbm_fill_factor(const Sbm *map);

/* Classify as empty, singleton, or multiple, without counting. */
extern SbmMembership sbm_membership(const Sbm *map);

/* The sole member of a singleton, else SBM_IDX_MAX. */
extern uint64 sbm_singleton_member(const Sbm *map);


/*
 * Rank, select, span
 */

/* Number of indexes in the inclusive range [x, y] whose bit equals value. */
extern size_t sbm_rank(const Sbm *map, uint64 x, uint64 y, bool value);

/*
 * Index of the n'th (0-based) index whose bit equals value, or SBM_IDX_MAX
 * if there are not that many.
 */
extern uint64 sbm_select(const Sbm *map, uint64 n, bool value);

/*
 * First index >= start that begins a run of len consecutive indexes whose
 * bits all equal value, or SBM_IDX_MAX.
 */
extern uint64 sbm_span(const Sbm *map, uint64 start, size_t len, bool value);

/*
 * Build a locator (see SbmLocator) for map.  Returns NULL for a NULL or
 * empty map.  The locator-based queries equal the plain functions.
 */
pg_nodiscard extern SbmLocator *sbm_locator_build(const Sbm *map);
extern void sbm_locator_free(SbmLocator *loc);
extern bool sbm_locator_contains(const SbmLocator *loc, uint64 idx);
extern size_t sbm_locator_rank(const SbmLocator *loc, uint64 lo, uint64 hi,
							   bool value);
extern uint64 sbm_locator_select(const SbmLocator *loc, uint64 n,
								 bool value);


/*
 * Iteration
 */

/*
 * Smallest member > prev_idx; pass SBM_IDX_MAX to start from the beginning.
 * Returns SBM_IDX_MAX when there are no more.  Typical loop:
 *
 *		SbmCursor	c = SBM_CURSOR_INIT;
 *		uint64		i = SBM_IDX_MAX;
 *
 *		while ((i = sbm_next_member(map, i, &c)) != SBM_IDX_MAX)
 *			...
 */
extern uint64 sbm_next_member(const Sbm *map, uint64 prev_idx,
							  SbmCursor *cur);

/*
 * Largest member < prev_idx; pass SBM_IDX_MAX to start from the end.  The
 * cursor is accepted for symmetry and currently unused.
 */
extern uint64 sbm_prev_member(const Sbm *map, uint64 prev_idx,
							  SbmCursor *cur);

/*
 * Call scanner with the members in ascending order, in batches of up to 64,
 * after skipping the first skip members.
 */
extern void sbm_scan(const Sbm *map,
					 void (*scanner) (uint64 vec[], size_t n, void *aux),
					 size_t skip, void *aux);

/*
 * Copy the members, ascending, into out.  On entry *n_out is out's capacity;
 * on return it is the number written.  With out == NULL only *n_out is set,
 * to the cardinality.  The output is O(cardinality), so bound untrusted maps
 * with sbm_extract_range() first.
 */
extern void sbm_to_array(const Sbm *map, uint64 *out, size_t *n_out);


/*
 * Set operations returning a new map
 *
 * The inputs are not modified.  A NULL input is the empty set, and a NULL
 * result is the empty set.
 */
pg_nodiscard extern Sbm *sbm_union(const Sbm *a, const Sbm *b);
pg_nodiscard extern Sbm *sbm_intersection(const Sbm *a, const Sbm *b);
pg_nodiscard extern Sbm *sbm_difference(const Sbm *a, const Sbm *b);
pg_nodiscard extern Sbm *sbm_xor(const Sbm *a, const Sbm *b);

/* Bitwise-named synonyms for the above. */
pg_nodiscard extern Sbm *sbm_or(const Sbm *a, const Sbm *b);
pg_nodiscard extern Sbm *sbm_and(const Sbm *a, const Sbm *b);
pg_nodiscard extern Sbm *sbm_andnot(const Sbm *a, const Sbm *b);

/* Members of map in [lo, hi), at their original indexes. */
pg_nodiscard extern Sbm *sbm_extract_range(const Sbm *map, uint64 lo,
										   uint64 hi);

/*
 * Every member shifted by offset; members that would fall below 0 are
 * dropped.  Returns NULL with errno = ERANGE if a member would overflow
 * past SBM_IDX_MAX.
 */
pg_nodiscard extern Sbm *sbm_offset(const Sbm *map, ssize_t offset);


/*
 * Set operations in place
 *
 * dst := dst OP src.  dst may be repalloc'd, so always assign the result:
 * "dst = sbm_union_inplace(dst, src);".  Return NULL only for a NULL dst.
 */
pg_nodiscard extern Sbm *sbm_union_inplace(Sbm *dst, const Sbm *src);
pg_nodiscard extern Sbm *sbm_intersection_inplace(Sbm *dst, const Sbm *src);
pg_nodiscard extern Sbm *sbm_difference_inplace(Sbm *dst, const Sbm *src);
pg_nodiscard extern Sbm *sbm_xor_inplace(Sbm *dst, const Sbm *src);

/*
 * Embedded maps
 *
 * These operate on an sbm_anchor()'d map `dst` whose buffer lives at
 * `outer + data_offset` inside a caller-owned palloc block.  When the result
 * needs more room they repalloc `outer` -- which may relocate it -- and
 * reanchor `dst` into the moved buffer.  Each returns the possibly-moved
 * `outer`, so always assign it:
 *
 *		node = sbm_add_grow_embedded(node, offsetof(Node, sbm),
 *									 offsetof(Node, data), &node->sbm, x);
 *
 * The map stores no pointer back to `outer`, so these are the only
 * operations permitted to grow an embedded map.  Because growing may
 * relocate `outer` (and therefore the embedded Sbm itself), callers must use
 * the returned pointer for both the node and any &node->sbm reference.
 */
pg_nodiscard extern void *sbm_add_grow_embedded(void *outer,
												size_t sbm_offset,
												size_t data_offset,
												uint64 idx);
pg_nodiscard extern void *sbm_add_range_embedded(void *outer,
												 size_t sbm_offset,
												 size_t data_offset,
												 uint64 lo,
												 uint64 hi);
pg_nodiscard extern void *sbm_union_into_embedded(void *outer,
												  size_t sbm_offset,
												  size_t data_offset,
												  const Sbm *src);
pg_nodiscard extern void *sbm_intersection_into_embedded(void *outer,
														 size_t sbm_offset,
														 size_t data_offset,
														 const Sbm *src);
pg_nodiscard extern void *sbm_difference_into_embedded(void *outer,
													   size_t sbm_offset,
													   size_t data_offset,
													   const Sbm *src);

/*
 * Move the members >= idx from map into other, which must be empty.  idx ==
 * SBM_IDX_MAX splits at the median member.  Returns the split index, or
 * SBM_IDX_MAX with errno = ENOSPC if either buffer is too small (in which
 * case both maps are unchanged).
 */
extern uint64 sbm_split(Sbm *map, uint64 idx, Sbm *other);


/*
 * Comparison
 *
 * All of these compare contents, never encodings.
 */
extern bool sbm_equals(const Sbm *a, const Sbm *b);
extern bool sbm_is_subset(const Sbm *a, const Sbm *b);	/* a <= b */
extern bool sbm_is_superset(const Sbm *a, const Sbm *b);	/* a >= b */
extern bool sbm_overlap(const Sbm *a, const Sbm *b);
extern bool sbm_nonempty_difference(const Sbm *a, const Sbm *b);
extern SbmSubsetRelation sbm_subset_compare(const Sbm *a, const Sbm *b);

/*
 * Total order: lexicographic over the ascending member sequence, so that a
 * proper prefix sorts first.  Returns <0, 0, or >0.
 */
extern int	sbm_compare(const Sbm *a, const Sbm *b);

/* Content hash; equal maps hash equally. */
extern uint64 sbm_hash(const Sbm *map);

/* Cardinality of a OP b, without materializing it. */
extern size_t sbm_union_cardinality(const Sbm *a, const Sbm *b);
extern size_t sbm_intersection_cardinality(const Sbm *a, const Sbm *b);
extern size_t sbm_difference_cardinality(const Sbm *a, const Sbm *b);
extern size_t sbm_xor_cardinality(const Sbm *a, const Sbm *b);

/* |a & b| / |a | b|, or 0 if both are empty. */
extern double sbm_jaccard_index(const Sbm *a, const Sbm *b);


/*
 * Maintenance
 */

/*
 * Structural self-check: chunk count, sizes, alignment and ordering all
 * consistent with m_data_used.  Use after adopting untrusted bytes.
 */
extern bool sbm_validate(const Sbm *map);

/* Fill *stats with layout statistics. */
extern void sbm_statistics(const Sbm *map, SbmStats *stats);


/*
 * Serialization
 *
 * The format is a 16-byte header (magic "sm10", version, endianness flag,
 * small-set flag, then reserved bytes written as zero) followed by the map's
 * encoding in host byte order.
 * Streams are portable only between hosts of the same byte order;
 * sbm_deserialize() rejects any other.
 */

/* Bytes sbm_serialize() will write. */
extern size_t sbm_serialized_size(const Sbm *map);

/*
 * Upper bound on sbm_serialized_size() of map and of every subset of it.
 * Removing members can enlarge an encoding, so storage that is rewritten in
 * place after removals must reserve this much.  The bound of a subset never
 * exceeds the bound of the set.
 */
extern size_t sbm_removal_bound(const Sbm *map);

/* Write map into out; returns bytes written, or 0 if out_size is short. */
extern size_t sbm_serialize(const Sbm *map, uint8 *out, size_t out_size);

/*
 * Read a serialized map into a new library-owned map.  Every read is
 * bounds-checked; returns NULL for malformed input.
 */
pg_nodiscard extern Sbm *sbm_deserialize(const uint8 *in, size_t n);

#endif							/* SBM_H */
