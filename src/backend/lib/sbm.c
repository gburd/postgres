/*-------------------------------------------------------------------------
 *
 * sbm.c
 *	  Sparse bitmap set: a compressed, mutable set of uint64 indexes.
 *
 * See sbm.h for what a sparse bitmap set is and when to use it.
 *
 * The encoding and algorithms are derived from the sparsemap library by
 * Christoph Rupp and Gregory Burd, used here under the MIT license reproduced
 * below.  Memory is obtained from the current memory context with palloc; bit
 * counting uses the shared pg_bitutils primitives; and internal invariants are
 * checked with Assert.
 *
 * Portions Copyright (c) 2014, Christoph Rupp
 * Portions Copyright (c) 2024, Gregory Burd
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 *
 * IDENTIFICATION
 *	  src/backend/lib/sbm.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <errno.h>

/* sbm.h exposes the full struct layout only when SBM_INTERNAL is defined. */
#define SBM_INTERNAL
#include "common/int.h"
#include "lib/sbm.h"
#include "port/pg_bitutils.h"
#include "port/simd.h"

typedef uint64 SbmBitvec;

/*
 * SbmIdx: the type of a chunk-start offset -- the absolute,
 * chunk-aligned bit index that prefixes every chunk in the serialized
 * buffer.  It is uint64 so the map addresses the full 64-bit index
 * space the public API advertises (SBM_IDX_MAX == PG_UINT64_MAX).
 *
 * This is an internal type, never exposed in sbm.h; the public API
 * uses uint64 for every bit location.  It exists as the single point
 * of control for the on-disk index width: SBM_SIZEOF_OVERHEAD and the
 * sbm_load_idx / sbm_store_idx helpers all derive their width from
 * sizeof(SbmIdx), so the serialized format width is defined in
 * exactly one place, and it must stay 64 bits to address the full
 * SBM_IDX_MAX index space.
 */
typedef uint64 SbmIdx;

/*
 * SbmBitvecUnaligned: a 64-bit unsigned alias that the compiler
 * treats as having 1-byte alignment, so loads and stores through a
 * pointer of this type emit unaligned-safe code.
 *
 * The on-disk layout prefixes each chunk with an 8-byte start offset
 * (and the map with an 8-byte chunk-count header), so chunk
 * descriptors are naturally 8-aligned within m_data.  This typedef is
 * retained as defense-in-depth: a caller-supplied wrap() buffer may be
 * arbitrarily aligned, and accessing the descriptor through a plain
 * uint64 * would be undefined behavior and trap on strict-alignment cpus.
 *
 * gcc and clang lower the unaligned access to whatever the platform
 * requires (a single load on x86_64, two byte-shuffled half-loads on
 * a strict-alignment cpu).  Zero overhead on the common targets.
 */
typedef uint64 pg_attribute_aligned(1)
SbmBitvecUnaligned;

/*
 * SbmChunk holds only a pointer; the unaligned-safe access is a
 * property of the pointee type (SbmBitvecUnaligned), so the
 * struct itself needs no special alignment.
 */
typedef struct
{
	SbmBitvecUnaligned *m_data;
} SbmChunk;


/*
 * Unaligned-safe load and store helpers.
 *
 * A sparse bitmap's on-disk layout places the 8-byte chunk-count header
 * and the 8-byte per-chunk start offsets at 8-byte boundaries within
 * m_data, naturally aligned for the cpu.  A caller-supplied wrap()
 * buffer, however, may be arbitrarily aligned, and a direct
 * `*(SbmIdx *) p` would both violate strict aliasing and trap on
 * strict-alignment cpus.  memcpy of a fixed small size compiles to a
 * single native load/store on the common targets.
 *
 * On-disk layout invariants: the buffer begins with a chunk count of
 * SBM_SIZEOF_OVERHEAD bytes, followed by that many chunks in ascending
 * start-offset order; each chunk is a start offset (SbmIdx, chunk-aligned)
 * plus its encoded body, and chunk start offsets are strictly increasing.
 */
static inline SbmIdx
sbm_load_idx(const uint8 *p)
{
	SbmIdx		v;

	memcpy(&v, p, sizeof(v));
	return v;
}

static inline void
sbm_store_idx(uint8 *p, const SbmIdx v)
{
	memcpy(p, &v, sizeof(v));
}

/*
 * The chunk-count header occupies SBM_SIZEOF_OVERHEAD (8) bytes at the
 * start of m_data as a native-endian uint64.  The count can never exceed
 * the number of addressable chunks, so it is never a binding limit.
 */
static inline uint64
sbm_load_u64(const uint8 *p)
{
	uint64		v;

	memcpy(&v, p, sizeof(v));
	return v;
}

static inline void
sbm_store_u64(uint8 *p, const uint64 v)
{
	memcpy(p, &v, sizeof(v));
}

enum
{
	/*
	 * metadata overhead: sizeof(SbmIdx) bytes for the chunk-start offset /
	 * chunk-count header (8 bytes)
	 */
	SBM_SIZEOF_OVERHEAD = sizeof(SbmIdx),

	/* number of bits that can be stored in a SbmBitvec */
	SBM_BITS_PER_VECTOR = sizeof(SbmBitvec) * 8,

	/* number of flags that can be stored in a single index byte */
	SBM_FLAGS_PER_INDEX_BYTE = 4,

	/* number of flags that can be stored in the index */
	SBM_FLAGS_PER_INDEX = sizeof(SbmBitvec) * SBM_FLAGS_PER_INDEX_BYTE,

	/* maximum capacity of a SbmChunk (in bits) */
	SBM_CHUNK_MAX_CAPACITY = SBM_BITS_PER_VECTOR * SBM_FLAGS_PER_INDEX,

	/* maximum capacity of a SbmChunk (31 bits of the RLE) */
	SBM_CHUNK_RLE_MAX_CAPACITY = 0x7FFFFFFF,

	/* minimum capacity of a SbmChunk (in bits) */
	SBM_CHUNK_MIN_CAPACITY = SBM_BITS_PER_VECTOR - 2,

	/* maximum length of a SbmChunk (31 bits of the RLE) */
	SBM_CHUNK_RLE_MAX_LENGTH = 0x7FFFFFFF,

	/* SbmBitvec payload is all zeros (2#00) */
	SBM_PAYLOAD_ZEROS = 0,

	/* SbmBitvec payload is all ones (2#11) */
	SBM_PAYLOAD_ONES = 3,

	/* SbmBitvec payload is mixed (2#10) */
	SBM_PAYLOAD_MIXED = 2,

	/* SbmBitvec is not used (2#01) */
	SBM_PAYLOAD_NONE = 1,

	/* a mask for checking flags (2 bits, 2#11) */
	SBM_FLAG_MASK = 3,

	/* return code for set(): ok, no further action required */
	SBM_OK = 0,

	/* return code for set(): needs to grow this SbmChunk */
	SBM_NEEDS_TO_GROW = 1,

	/* return code for set(): needs to shrink this SbmChunk */
	SBM_NEEDS_TO_SHRINK = 2
};

/* Used when separating an RLE chunk into 2-3 chunks */
typedef struct
{
	struct
	{
		uint8	   *p;			/* pointer into m_data */
		size_t		offset;		/* offset in m_data */
		SbmChunk   *chunk;		/* chunk to be split */
		SbmIdx		start;		/* start of chunk */
		size_t		length;		/* initial length of chunk */
		size_t		capacity;	/* the capacity of this RLE chunk */
	}			target;

	struct
	{
		uint8	   *p;			/* location in buf */
		uint64		idx;		/* chunk-aligned to idx */
		size_t		size;		/* byte size of this chunk */
	}			pivot;

	struct
	{
		SbmIdx		start;
		uint64		end;
		uint8	   *p;
		size_t		size;
		SbmChunk	c;
	}			ex[2];			/* 0 is "on the left", 1 is "on the right" */

	alignas(SbmBitvec) uint8 buf[(SBM_SIZEOF_OVERHEAD * (unsigned long) 3) +
						 (sizeof(SbmBitvec) * 6)];
	size_t		expand_by;
	size_t		count;
} SbmChunkSep;

/*
 * SBM_ENOUGH_SPACE: if growing m_data_used by `need` bytes would push
 * past m_capacity, return SBM_IDX_MAX with errno=ENOSPC.
 *
 * The +SBM_SIZEOF_OVERHEAD slack accounts for an off-by-4 read in
 * sbm_insert_data: the memmove length there is `m_data_used -
 * offset`, which over-counts by SBM_SIZEOF_OVERHEAD when m_data_used
 * includes the chunk-count header (the post-sbm_clear()
 * convention).  Without the slack the over-read writes
 * SBM_SIZEOF_OVERHEAD bytes past the buffer end at the boundary.
 * Fixing the off-by-4 in sbm_insert_data directly is preferable
 * but breaks the alternate convention used by
 * sbm_anchor()-without-clear callers, where m_data_used does
 * not include the header.
 */
#define SBM_ENOUGH_SPACE(need)                                        \
	do {                                                         \
		if (map->m_data_used + (need) + SBM_SIZEOF_OVERHEAD > \
		    sbm_cap(map)) {                                  \
			errno = ENOSPC;                              \
			return SBM_IDX_MAX;                         \
		}                                                    \
	} while (0)

#define SBM_CHUNK_GET_FLAGS(data, at) \
	((((data)) & ((SbmBitvec)SBM_FLAG_MASK << ((at) * 2))) >> ((at) * 2))
#define SBM_CHUNK_SET_FLAGS(data, at, to)                                    \
	((data) = ((data) & ~((SbmBitvec)SBM_FLAG_MASK << ((at) * 2))) | \
	        ((SbmBitvec)(to) << ((at) * 2)))
#define SBM_IS_CHUNK_RLE(chunk)                                       \
	(((*((SbmBitvecUnaligned *)(chunk)->m_data) &           \
	      (((SbmBitvec)0x3) << (SBM_BITS_PER_VECTOR - 2))) >> \
	     (SBM_BITS_PER_VECTOR - 2)) == SBM_PAYLOAD_NONE)

/*
 * RLE (Run-Length Encoding) Format
 *
 * RLE chunks encode a contiguous run of set bits (1s) starting at offset 0.
 * The entire chunk is represented by a single 64-bit descriptor word:
 *
 * Bits 63:62 = 01 (RLE flag, matches SBM_PAYLOAD_NONE to distinguish from sparse)
 * Bits 61:31 = Chunk capacity in bits (31 bits, max 2,147,483,647)
 * Bits 30:0  = Run length in bits (31 bits, max 2,147,483,647)
 *
 * Example: If length=1000 and capacity=2048, bits 0-999 are set (1), bits 1000-2047 are unset (0).
 *
 * RLE chunks are immutable by design - any modification that would create gaps or
 * partial runs causes the chunk to be converted to sparse encoding.
 */
#define SBM_RLE_FLAGS      UINT64CONST(0x4000000000000000)	/* Bits 63:62 = 01 */
#define SBM_RLE_FLAGS_MASK UINT64CONST(0xC000000000000000)	/* Mask for bits 63:62 */
#define SBM_RLE_CAPACITY_MASK \
	UINT64CONST(0x3FFFFFFF80000000) /* Mask for bits 61:31 (capacity) */
#define SBM_RLE_LENGTH_MASK UINT64CONST(0x7FFFFFFF) /* Mask for bits 30:0
													 * (length) */

/*
 * Checks if the given chunk is flagged as RLE encoded.
 *
 * This function examines the first element in the chunk's data array to determine
 * if the chunk is run-length encoded (RLE).
 *
 * `chunk` is the chunk to check.
 * Returns true if the chunk is flagged as RLE encoded, false otherwise.
 */
static pg_always_inline bool
sbm_chunk_is_rle(const SbmChunk *chunk)
{
	const SbmBitvec w = chunk->m_data[0];

	return (w & SBM_RLE_FLAGS_MASK) == SBM_RLE_FLAGS;
}

/*
 * Sets the Run-Length Encoding (RLE) flag on the specified chunk.
 *
 * This function modifies the first element in the chunk's data array to set
 * the RLE flag, indicating that the chunk is encoded using run-length encoding.
 *
 * `chunk` is the chunk to be flagged as RLE encoded.
 */
static void
sbm_chunk_set_rle(const SbmChunk *chunk)
{
	SbmBitvec	w = chunk->m_data[0];

	/* Clear flag bits, capacity bits, and length bits */
	w &= ~(SBM_RLE_FLAGS_MASK | SBM_RLE_CAPACITY_MASK | SBM_RLE_LENGTH_MASK);
	/* Set the RLE flag (01 in bits 63:62) */
	w |= ((((SbmBitvec) 1) << (SBM_BITS_PER_VECTOR - 2)) &
		  SBM_RLE_FLAGS_MASK);
	chunk->m_data[0] = w;
}

/*
 * Retrieves the capacity of a run-length encoded (RLE) chunk.
 *
 * This function extracts and returns the capacity of an RLE chunk by masking
 * the relevant bits from the first element of the chunk's data array.
 *
 * `chunk` is the chunk whose capacity is to be retrieved.
 * Returns the capacity of the RLE chunk.
 */
static size_t
sbm_chunk_rle_get_capacity(const SbmChunk *chunk)
{
	SbmBitvec	w =
		chunk->m_data[0] & (SbmBitvec) SBM_RLE_CAPACITY_MASK;

	w >>= 31;
	return w;
}

/*
 * Sets the capacity of an RLE encoded chunk.
 *
 * This function modifies the first element of the chunk's data array to set
 * the given capacity for a run-length encoded (RLE) chunk. The capacity is
 * masked and bit-shifted according to the RLE encoding specifications.
 *
 * This does not check the chunk type, if the chunk isn't RLE then this
 * function will overwrite flags data in a sparse chunk corrupting it.
 *
 * `chunk` is the chunk whose capacity is to be set; `capacity` is the
 * capacity to set for the RLE chunk.
 */
static void
sbm_chunk_rle_set_capacity(const SbmChunk *chunk, const size_t capacity)
{
	SbmBitvec	w;

	Assert(capacity <= SBM_CHUNK_RLE_MAX_CAPACITY);
	w = chunk->m_data[0];
	w &= ~SBM_RLE_CAPACITY_MASK;
	w |= ((SbmBitvec) capacity << 31) & SBM_RLE_CAPACITY_MASK;
	chunk->m_data[0] = w;
}

/*
 * Retrieves the run-length for a given RLE encoded chunk.
 *
 * This function extracts and returns the run-length information from the first
 * element of the chunk's data array using a predefined mask.
 *
 * A "run" is a set of adjacent ones that starts at the 0th bit of this
 * chunk. For an RLE chunk that's encoded in the descriptor.  For a sparse
 * chunk we must see how many flags are SBM_PAYLOAD_ONES and then if we find an
 * SBM_PAYLOAD_MIXED count the additional adjacent ones if they exist
 *
 * `chunk` is the RLE encoded chunk whose run-length is to be retrieved.
 * Returns the run-length of the given chunk.
 */
static size_t
sbm_chunk_rle_get_length(const SbmChunk *chunk)
{
	const SbmBitvec w =
		chunk->m_data[0] & (SbmBitvec) SBM_RLE_LENGTH_MASK;

	return w;
}

/*
 * Sets the length of a run-length encoded (RLE) chunk.
 *
 * This function updates the length field of a run-length encoded (RLE) chunk by
 * first validating that the new length is within the permissible maximum length,
 * then modifying the length bits within the chunk's data array accordingly.
 *
 * `chunk` is the chunk whose length is to be set; `length` is the new length
 * to set for the chunk.
 */
static void
sbm_chunk_rle_set_length(const SbmChunk *chunk, const size_t length)
{
	SbmBitvec	w;

	Assert(length <= SBM_CHUNK_RLE_MAX_LENGTH);
	Assert(length <= sbm_chunk_rle_get_capacity(chunk));
	w = chunk->m_data[0];
	w &= ~SBM_RLE_LENGTH_MASK;
	w |= length & SBM_RLE_LENGTH_MASK;
	chunk->m_data[0] = w;
}

/*
 * Gets the run length of a given chunk.
 *
 * This function calculates the run length of a given chunk. If the chunk is
 * run-length encoded (RLE), the length is obtained directly. Otherwise, it
 * calculates the run length by analyzing the bit vector data.
 *
 * `chunk` is the chunk to evaluate.
 * Returns the run length of the chunk. Returns 0 if the chunk is not RLE
 *  encoded and cannot be determined to have a valid run length.
 */
static size_t
sbm_chunk_get_run_length(const SbmChunk *chunk)
{
	size_t		length = 0;

	if (sbm_chunk_is_rle(chunk))
	{
		length = sbm_chunk_rle_get_length(chunk);
	}
	else
	{
		size_t		count = 0;
		int			j = SBM_FLAGS_PER_INDEX,
					k = SBM_BITS_PER_VECTOR;
		SbmBitvec	w = chunk->m_data[0];

		switch (w)
		{
			case 0:
				return 0;
			case ~(SbmBitvec) 0:

				/*
				 * This returns max capacity but actual run might be shorter.
				 * This is used during coalescing to determine if chunks can
				 * be merged. The caller must account for the actual chunk
				 * capacity.
				 */
				return SBM_CHUNK_MAX_CAPACITY;
			default:
				while (j && (w & SBM_PAYLOAD_ONES) == SBM_PAYLOAD_ONES)
				{
					count++;
					w >>= 2;
					j--;
				}
				if (count)
				{
					count *= SBM_BITS_PER_VECTOR;
					if ((w & SBM_PAYLOAD_MIXED) ==
						SBM_PAYLOAD_MIXED)
					{
						/*
						 * Only now is m_data[1] guaranteed to exist: a
						 * leading run of all-ones vectors followed by a MIXED
						 * vector means a payload word was stored. Loading it
						 * earlier would read past a single-word chunk.
						 */
						SbmBitvec	v = chunk->m_data[1];

						w >>= 2;
						j--;
						while (k && (v & 1) == 1)
						{
							count++;
							v >>= 1;
							k--;
						}
						while (k && (v & 1) == 0)
						{
							v >>= 1;
							k--;
						}
						if (k)
						{
							return 0;
						}
					}
					while (j--)
					{
						switch (w & 0x3)
						{
							case SBM_PAYLOAD_NONE:
							case SBM_PAYLOAD_ZEROS:
								w >>= 2;
								break;
							default:
								return 0;
						}
					}
					Assert(count < SBM_CHUNK_MAX_CAPACITY);
					length = count;
				}
		}
	}
	return length;
}

/*
 * struct Sbm is defined in sbm.h (visible here because this file
 * defines SBM_INTERNAL before including it; consumers get it via
 * SBM_EXPOSE_STRUCT).  Keeping the single definition in the header
 * lets callers embed an Sbm by value without the layout drifting
 * from the implementation.
 */

/*
 * Allocation lineage.  Tracked per Sbm so the grow / dispose
 * paths know what they may safely realloc or free.
 *
 * SBM_OWNED_CONTIGUOUS  Single palloc0(sizeof(Sbm) + size).
 *                      Both the struct and m_data live in one chunk;
 *                      m_data sits immediately after the struct.
 *                      Set by sbm_create() and sbm_copy().  May be
 *                      grown via repalloc, and disposed with pfree(map).
 *                      Default for zero-initialized memory.
 *
 * SBM_WRAPPED           m_data points to a buffer the caller owns.  Set
 *                      by sbm_anchor(), sbm_init(), and
 *                      sbm_open().  Cannot be realloc'd in place;
 *                      sbm_set_data_size with data == NULL will
 *                      transparently promote to SBM_OWNED_SPLIT by
 *                      allocating a fresh library-owned buffer and
 *                      copying the m_data_used prefix into it.  The
 *                      caller's original buffer is left untouched and
 *                      remains theirs to free.
 *
 * SBM_OWNED_SPLIT       The struct is heap-allocated; m_data is
 *                      separately heap-allocated and owned by the
 *                      library (typically the result of promoting an
 *                      SBM_WRAPPED map via grow).  Disposed with
 *                      sbm_free, which does free(m_data) +
 *                      free(map).
 */
enum SbmAllocKind
{
	SBM_OWNED_CONTIGUOUS = 0,
	SBM_WRAPPED = 1,
	SBM_OWNED_SPLIT = 2
};

/* -------------------------------------------------------------------
 * Capacity / lineage accessors
 *
 * The allocation-lineage tag (how m_data was provisioned) is folded
 * into the low 3 bits of m_capacity.  Capacity is always rounded up to
 * an 8-byte boundary before being stored, so those bits are free.
 * Read the byte capacity with sbm_cap() and the lineage with
 * sbm_kind(); never touch m_capacity directly.
 * -------------------------------------------------------------------
 */

static inline size_t
sbm_cap(const Sbm *m)
{
	return m->m_capacity & ~(size_t) 7;
}

static inline uint8
sbm_kind(const Sbm *m)
{
	return (uint8) (m->m_capacity & 7u);
}

static inline void
sbm_set_cap_kind(Sbm *m, size_t cap, uint8 kind)
{
	/*
	 * Round capacity DOWN to an 8-byte boundary so the tag bits are free and
	 * we never report more usable bytes than the buffer actually has.
	 * Library-owned allocators round the allocation size UP before calling
	 * here, so for them this is a no-op; for caller-supplied (wrapped)
	 * buffers a non-8-aligned size loses up to 7 trailing bytes, which is the
	 * documented behavior.
	 */
	m->m_capacity = (cap & ~(size_t) 7) | (kind & 7u);
}

static inline void
sbm_set_kind(Sbm *m, uint8 kind)
{
	m->m_capacity = (m->m_capacity & ~(size_t) 7) | (kind & 7u);
}

/* -------------------------------------------------------------------
 * Lazy cardinality cache (runtime-only; see the struct comment in sbm.h)
 *
 * m_card_plus1 caches sbm_cardinality() biased by one so that the
 * all-zero state means "unknown": 0 = invalid, otherwise the cached
 * count is (m_card_plus1 - 1).  The cache is derived purely from
 * m_data, so an invalid cache is always safe -- the next query just
 * recomputes it.  Every membership-changing path calls
 * sbm_card_invalidate(); sbm_cardinality fills it on the first query
 * after a mutation.  Never serialized, never sent to the wire.
 * -------------------------------------------------------------------
 */

static inline void
sbm_card_invalidate(Sbm *m)
{
	if (m != NULL)
		m->m_card_plus1 = 0;
}

static inline bool
sbm_card_is_valid(const Sbm *m)
{
	return m->m_card_plus1 != 0;
}

static inline size_t
sbm_card_get(const Sbm *m)
{
	return m->m_card_plus1 - 1u;
}

static inline void
sbm_card_store(Sbm *m, size_t count)
{
	/*
	 * count + 1 cannot wrap in practice (count <= SBM_IDX_MAX bits), but
	 * guard: a would-be-wrapping count simply stays uncached.
	 */
	if (m != NULL && count != SIZE_MAX)
		m->m_card_plus1 = count + 1u;
}


/*
 * Internal-invariant check.  No-op in production builds; under
 * USE_ASSERT_CHECKING it asserts:
 *
 *   - m_data is non-NULL when the byte capacity > 0
 *   - m_data_used <= byte capacity (no buffer overrun)
 *   - m_data is 8-byte aligned (the chunk codec assumes this)
 *   - the lineage tag (low bits of m_capacity) is one of the three
 *     known values
 *
 * The intent is to fail at the moment a corrupted map is touched,
 * rather than several operations later when the memory context
 * finally notices.  Called at the top of public entry points.
 */
static inline void
sbm_check_invariants(const Sbm *map)
{
	/* A NULL map is a valid empty, read-only set (see sbm.h). */
	if (map == NULL)
		return;
	Assert(sbm_cap(map) == 0 || map->m_data != NULL);
	Assert(map->m_data_used <= sbm_cap(map));
	Assert(PointerIsAligned(map->m_data, uint64));
	Assert(sbm_kind(map) == SBM_OWNED_CONTIGUOUS ||
		   sbm_kind(map) == SBM_WRAPPED ||
		   sbm_kind(map) == SBM_OWNED_SPLIT);
}

/*
 * Number of MIXED (2#10) vector flags among the four 2-bit flags packed into
 * descriptor byte b, i.e. how many payload words those four vectors occupy.
 * For example lookup[0x0A] (2#00001010) is 2.  The table is generated by the
 * program at the end of this file.
 */
static inline size_t
sbm_chunk_calc_vector_size(const uint8 b)
{
	static const uint8 lookup[256] = {
		0, 0, 1, 0, 0, 0, 1, 0, 1, 1, 2, 1, 0, 0, 1, 0,
		0, 0, 1, 0, 0, 0, 1, 0, 1, 1, 2, 1, 0, 0, 1, 0,
		1, 1, 2, 1, 1, 1, 2, 1, 2, 2, 3, 2, 1, 1, 2, 1,
		0, 0, 1, 0, 0, 0, 1, 0, 1, 1, 2, 1, 0, 0, 1, 0,
		0, 0, 1, 0, 0, 0, 1, 0, 1, 1, 2, 1, 0, 0, 1, 0,
		0, 0, 1, 0, 0, 0, 1, 0, 1, 1, 2, 1, 0, 0, 1, 0,
		1, 1, 2, 1, 1, 1, 2, 1, 2, 2, 3, 2, 1, 1, 2, 1,
		0, 0, 1, 0, 0, 0, 1, 0, 1, 1, 2, 1, 0, 0, 1, 0,
		1, 1, 2, 1, 1, 1, 2, 1, 2, 2, 3, 2, 1, 1, 2, 1,
		1, 1, 2, 1, 1, 1, 2, 1, 2, 2, 3, 2, 1, 1, 2, 1,
		2, 2, 3, 2, 2, 2, 3, 2, 3, 3, 4, 3, 2, 2, 3, 2,
		1, 1, 2, 1, 1, 1, 2, 1, 2, 2, 3, 2, 1, 1, 2, 1,
		0, 0, 1, 0, 0, 0, 1, 0, 1, 1, 2, 1, 0, 0, 1, 0,
		0, 0, 1, 0, 0, 0, 1, 0, 1, 1, 2, 1, 0, 0, 1, 0,
		1, 1, 2, 1, 1, 1, 2, 1, 2, 2, 3, 2, 1, 1, 2, 1,
		0, 0, 1, 0, 0, 0, 1, 0, 1, 1, 2, 1, 0, 0, 1, 0
	};

	return lookup[b];
}

/*
 * Extracts flag-byte n of a chunk descriptor, endian-neutrally.
 *
 * The sparse descriptor packs thirty-two 2-bit flags into one 64-bit
 * word, four flags per byte, with flag i occupying bits
 * [2*i, 2*i+1] of the word.  Flag byte n therefore covers flags
 * [4n, 4n+3], i.e. word bits [8n, 8n+7].
 *
 * Reading those bytes by walking a `uint8 *` over the word only works
 * on a little-endian host: on big-endian, byte 0 of the object is the
 * word's most-significant byte, so a byte walk visits the flags in
 * reverse.  That produced a correct sbm_contains (which shifts the word
 * directly) but wrong sbm_cardinality / minimum / maximum / rank /
 * select on big-endian hosts.  Shifting the word is correct everywhere
 * and compiles to the same single byte load on little-endian.
 *
 * `desc` is the chunk descriptor word; `n` is flag-byte index in [0,
 * sizeof(SbmBitvec)).
 * Returns Byte n of the logical flag sequence.
 */
static inline uint8
sbm_desc_flag_byte(const SbmBitvec desc, const size_t n)
{
	return (uint8) ((desc >> (n * 8)) & 0xFFu);
}

/*
 * Retrieves the position within the chunk corresponding to the specified bit vector index.
 *
 * This function calculates the position in the chunk's data array that
 * corresponds to the given bit vector index. It handles both run-length
 * encoded (RLE) and non-RLE chunks.
 *
 * `chunk` is the chunk from which to retrieve the position; `bv` is the bit
 * vector index within the chunk.
 * Returns the position within the chunk's data array corresponding to the specified bit vector index.
 */
static pg_always_inline size_t
sbm_chunk_get_position(const SbmChunk *chunk, size_t bv)
{
	/* Handle 4 indices (1 byte) at a time. */
	size_t		position = 0;

	/*
	 * Defense-in-depth: callers compute `bv` as `idx / SBM_BITS_PER_VECTOR`
	 * after subtracting the chunk's start offset; on a corrupt buffer
	 * (sbm_open of attacker-controlled bytes) the start offset can be wildly
	 * wrong, making `bv` arbitrarily large.  Clamp to the physical chunk
	 * capacity so the loop below never walks past the 8-byte header word.
	 * Returning 0 here causes the caller to read chunk->m_data[1] which is
	 * also bounded by the chunk_size that sbm_get_size_impl validated.
	 */
	if (bv >= SBM_FLAGS_PER_INDEX)
	{
		return 0;
	}

	/* Handle RLE by examining the first byte. */
	if (!sbm_chunk_is_rle(chunk))
	{
		const SbmBitvec desc = *chunk->m_data;
		const size_t num_bytes =
			bv / ((size_t) SBM_FLAGS_PER_INDEX_BYTE * SBM_BITS_PER_VECTOR);
		size_t		i;

		for (i = 0; i < num_bytes; i++)
		{
			position += sbm_chunk_calc_vector_size(
												   sbm_desc_flag_byte(desc, i));
		}

		bv -= num_bytes * SBM_FLAGS_PER_INDEX_BYTE;
		for (i = 0; i < bv; i++)
		{
			const size_t flags =
				SBM_CHUNK_GET_FLAGS(*chunk->m_data, i);

			if (flags == SBM_PAYLOAD_MIXED)
			{
				position++;
			}
		}
	}

	return position;
}

/*
 * Initializes an SbmChunk structure with the given data.
 *
 * This function sets the m_data member of the provided SbmChunk structure to point
 * to the given data, cast as a pointer to SbmBitvec.
 *
 * `chunk` is the chunk to initialize; `data` is the data to associate with
 * the chunk.
 */
static void
sbm_chunk_init(SbmChunk *chunk, uint8 *data)
{
	chunk->m_data = (SbmBitvecUnaligned *) data;
}

/*
 * Retrieves the capacity of the given chunk.
 *
 * This function calculates the total capacity of the specified chunk,
 * considering if the chunk is run-length encoded (RLE) or not. For RLE
 * encoded chunks, the capacity is directly retrieved from the chunk's data.
 * For non-RLE encoded chunks, the capacity is computed by examining the
 * data and assessing the available, unused sections.
 *
 * `chunk` is the chunk whose capacity is to be determined.
 * Returns the capacity of the chunk.
 */
static pg_always_inline size_t
sbm_chunk_get_capacity(const SbmChunk *chunk)
{
	size_t		capacity = SBM_CHUNK_MAX_CAPACITY;
	SbmBitvec	desc;
	size_t		i;

	/* Handle RLE which encodes the capacity in the vector. */
	if (unlikely(sbm_chunk_is_rle(chunk)))
	{
		return sbm_chunk_rle_get_capacity(chunk);
	}

	desc = *chunk->m_data;

	for (i = 0; i < sizeof(SbmBitvec); i++)
	{
		const uint8 b = sbm_desc_flag_byte(desc, i);
		int			j;

		if (!b || b == 0xff)
		{
			continue;
		}
		for (j = 0; j < SBM_FLAGS_PER_INDEX_BYTE; j++)
		{
			const size_t flags = SBM_CHUNK_GET_FLAGS(b, j);

			if (flags == SBM_PAYLOAD_NONE)
			{
				capacity -= SBM_BITS_PER_VECTOR;
			}
		}
	}
	return capacity;
}

/*
 * Increases the capacity of a chunk to the specified value.
 *
 * This function adjusts the capacity of a given chunk, ensuring that the new capacity
 * is a multiple of SBM_BITS_PER_VECTOR, does not exceed the maximum allowed capacity,
 * and is greater than the current capacity of the chunk. The capacity is increased by
 * marking payload bits in the chunk's data array.
 *
 * `chunk` is the chunk whose capacity is to be increased; `capacity` is the
 * new capacity to set for the chunk.
 */
static void
sbm_chunk_increase_capacity(const SbmChunk *chunk, const size_t capacity)
{
	const size_t initial_capacity = sbm_chunk_get_capacity(chunk);
	size_t		increased = 0;
	size_t		i;

	Assert(capacity % SBM_BITS_PER_VECTOR == 0);
	Assert(capacity <= SBM_CHUNK_MAX_CAPACITY);
	Assert(capacity > initial_capacity);

	if (capacity <= initial_capacity || capacity > SBM_CHUNK_MAX_CAPACITY)
	{
		return;
	}

	for (i = 0; i < sizeof(SbmBitvec); i++)
	{
		const uint8 b = sbm_desc_flag_byte(*chunk->m_data, i);
		int			j;

		if (!b || b == 0xff)
		{
			continue;
		}
		for (j = 0; j < SBM_FLAGS_PER_INDEX_BYTE; j++)
		{
			const size_t flags = SBM_CHUNK_GET_FLAGS(b, j);

			if (flags == SBM_PAYLOAD_NONE)
			{
				/*
				 * Flag (i * 4 + j) of the descriptor word; set it word-wise
				 * so the update is endian-neutral.
				 */
				SbmBitvec	desc = *chunk->m_data;

				SBM_CHUNK_SET_FLAGS(desc,
									(i * (size_t) SBM_FLAGS_PER_INDEX_BYTE) +
									(size_t) j,
									SBM_PAYLOAD_ZEROS);
				*chunk->m_data = desc;
				increased += SBM_BITS_PER_VECTOR;
				if (increased + initial_capacity == capacity)
				{
					Assert(sbm_chunk_get_capacity(
												  chunk) == capacity);
					return;
				}
			}
		}
	}
	Assert(sbm_chunk_get_capacity(chunk) == capacity);
}

/*
 * Determines if a given chunk is empty.
 *
 * This function checks if all flags within the chunk's data are either
 * SBM_PAYLOAD_ZEROS or SBM_PAYLOAD_NONE. If any flag doesn't meet these
 * criteria, the chunk is considered not empty.
 *
 * `chunk` is the chunk to be evaluated.
 * Returns true if the chunk is empty, otherwise false.
 */
static bool
sbm_chunk_is_empty(const SbmChunk *chunk)
{
	if (chunk->m_data[0] != 0)
	{
		/*
		 * A chunk is considered empty if all flags are SBM_PAYLOAD_ZERO or
		 * _NONE.
		 */
		const SbmBitvec desc = *chunk->m_data;
		size_t		i;

		for (i = 0; i < sizeof(SbmBitvec); i++)
		{
			const uint8 b = sbm_desc_flag_byte(desc, i);

			if (b)
			{
				int			j;

				for (j = 0; j < SBM_FLAGS_PER_INDEX_BYTE;
					 j++)
				{
					const size_t flags =
						SBM_CHUNK_GET_FLAGS(b, j);

					if (flags != SBM_PAYLOAD_NONE &&
						flags != SBM_PAYLOAD_ZEROS)
					{
						return false;
					}
				}
			}
		}
	}
	/* The SbmChunk is empty if all flags (in m_data[0]) are zero. */
	return true;
}

/*
 * Retrieves the size of the specified chunk.
 *
 * This function calculates the memory size required by the given chunk.
 * If the chunk is not run-length encoded (RLE), the function iterates
 * over the chunk's data array and computes the size using a lookup table.
 *
 * `chunk` is the chunk whose size is to be determined.
 * Returns the size of the chunk in bytes.
 */
static pg_always_inline size_t
sbm_chunk_get_size(const SbmChunk *chunk)
{
	/* At least one SbmBitvec is required for the flags (m_data[0]) */
	size_t		size = sizeof(SbmBitvec);

	if (likely(!sbm_chunk_is_rle(chunk)))
	{
		/* Use a lookup table for each byte of the flags */
		const SbmBitvec desc = *chunk->m_data;
		size_t		i;

		for (i = 0; i < sizeof(SbmBitvec); i++)
		{
			size += sizeof(SbmBitvec) *
				sbm_chunk_calc_vector_size(
										   sbm_desc_flag_byte(desc, i));
		}
	}
	return size;
}

/*
 * Checks if a specific bit is set in a given chunk.
 *
 * This function determines if a bit at a specific index within a chunk is set. The
 * chunk can be either run-length encoded (RLE) or contain a mixture of payloads.
 *
 * `chunk` is the chunk to check; `idx` is the index of the bit to check
 * within the chunk.
 * Returns true if the bit at the specified index is set, false otherwise.
 */
static pg_always_inline bool
sbm_chunk_is_set(const SbmChunk *chunk, const size_t idx)
{
	size_t		bv;
	size_t		flags;
	SbmBitvec	w;

	if (unlikely(sbm_chunk_is_rle(chunk)))
	{
		if (idx < sbm_chunk_rle_get_length(chunk))
		{
			return true;
		}
		return false;
	}

	/*
	 * Defense-in-depth: on a corrupt buffer (attacker-controlled chunk start
	 * offset) the caller's `idx - start` can wrap to a value way beyond
	 * SBM_CHUNK_MAX_CAPACITY.  Reject those without trying to compute `bv`.
	 */
	if (idx >= SBM_CHUNK_MAX_CAPACITY)
	{
		return false;
	}
	/* in which SbmBitvec is |idx| stored? */
	bv = idx / SBM_BITS_PER_VECTOR;
	Assert(bv < SBM_FLAGS_PER_INDEX);

	/* now retrieve the flags of that SbmBitvec */
	flags = SBM_CHUNK_GET_FLAGS(*chunk->m_data, bv);
	switch (flags)
	{
		case SBM_PAYLOAD_ZEROS:
		case SBM_PAYLOAD_NONE:
			return false;
		case SBM_PAYLOAD_ONES:
			return true;
		default:
			Assert(flags == SBM_PAYLOAD_MIXED);
	}

	/* get the SbmBitvec at |bv| */
	w = chunk->m_data[1 + sbm_chunk_get_position(chunk, bv)];
	/* and finally check the bit in that SbmBitvec */
	return (w & (SbmBitvec) 1 << idx % SBM_BITS_PER_VECTOR) > 0;
}

/*
 * Clears a specific bit in a chunk.
 *
 * This function attempts to clear a specified bit within a given chunk.
 * Based on the payload flags in the chunk, it will update the position of
 * the bit and handle transitions between different payload states
 * (ZEROS, ONES, MIXED). If the bit is already clear, it performs a no-op.
 * If the bit is set, it updates the relevant data structures accordingly,
 * possibly requiring the chunk to grow or shrink.
 *
 * `chunk` is the chunk in which to clear the bit; `idx` is the index of the
 * bit to be cleared; `pos` is the position of the bit to be cleared; updated
 * internally.
 * Returns an integer status code indicating the result:
 *         - SBM_OK if the operation was successful,
 *         - SBM_NEEDS_TO_GROW if the chunk needs to grow,
 *         - SBM_NEEDS_TO_SHRINK if the chunk needs to shrink.
 */
static int
sbm_chunk_clr_bit(const SbmChunk *chunk, const uint64 idx, size_t *pos)
{
	SbmBitvec	w;
	const size_t bv = idx / SBM_BITS_PER_VECTOR;

	Assert(bv < SBM_FLAGS_PER_INDEX);

	switch (SBM_CHUNK_GET_FLAGS(*chunk->m_data, bv))
	{
		case SBM_PAYLOAD_ZEROS:
			/* The bit is already clear, no-op. */
			*pos = 0;
			return SBM_OK;
			break;
		case SBM_PAYLOAD_ONES:

			/*
			 * What was all ones transitions to mixed, which requires another
			 * vector.
			 */
			if (*pos == 0)
			{
				*pos = (size_t) 1 + sbm_chunk_get_position(chunk, bv);
				return SBM_NEEDS_TO_GROW;
			}
			SBM_CHUNK_SET_FLAGS(*chunk->m_data, bv, SBM_PAYLOAD_MIXED);
			w = chunk->m_data[*pos];
			w &= ~((SbmBitvec) 1 << idx % SBM_BITS_PER_VECTOR);
			/* Update the mixed vector. */
			chunk->m_data[*pos] = w;
			return SBM_OK;
			break;
		case SBM_PAYLOAD_MIXED:
			*pos = 1 + sbm_chunk_get_position(chunk, bv);
			w = chunk->m_data[*pos];
			w &= ~((SbmBitvec) 1 << idx % SBM_BITS_PER_VECTOR);

			/*
			 * Did the vector transition from mixed to all zeros? If so,
			 * remove it.
			 */
			if (w == 0)
			{
				SBM_CHUNK_SET_FLAGS(*chunk->m_data, bv,
									SBM_PAYLOAD_ZEROS);
				return SBM_NEEDS_TO_SHRINK;
			}
			/* Update the mixed vector. */
			chunk->m_data[*pos] = w;
			break;
		case SBM_PAYLOAD_NONE:
			pg_fallthrough;
		default:
			Assert(!"shouldn't be here");
			break;
	}
	return SBM_OK;
}

/*
 * Sets a bit within a chunk at the specified index.
 *
 * This function sets a bit in the given chunk at the location specified by the index.
 * It handles different payload states (all ones, all zeros, and mixed) and updates
 * the chunk's data and flags accordingly.
 *
 * `chunk` is the chunk to modify; `idx` is the index within the chunk where
 * the bit should be set; `pos` is pointer to a size_t that will be set to
 * the position of the bit.
 * Returns an integer indicating the status of the operation. Possible return values are:
 *         - SBM_OK: The bit was successfully set.
 *         - SBM_NEEDS_TO_GROW: The chunk needs additional space.
 *         - SBM_NEEDS_TO_SHRINK: The chunk has excess space that can be reclaimed.
 */
static int
sbm_chunk_set_bit(const SbmChunk *chunk, const uint64 idx, size_t *pos)
{
	/*
	 * Where in the descriptor does this idx fall, which flag should we
	 * examine?
	 */
	const size_t bv = idx / SBM_BITS_PER_VECTOR;
	SbmBitvec	w;

	Assert(bv < SBM_FLAGS_PER_INDEX);
	Assert(sbm_chunk_is_rle(chunk) == false);

	switch (SBM_CHUNK_GET_FLAGS(*chunk->m_data, bv))
	{
		case SBM_PAYLOAD_ONES:
			/* The bit is already set, no-op. */
			*pos = 0;
			return SBM_OK;
		case SBM_PAYLOAD_ZEROS:

			/*
			 * What was all zeros transitions to mixed, which requires another
			 * vector.
			 */
			if (*pos == 0)
			{
				*pos = (size_t) 1 + sbm_chunk_get_position(chunk, bv);
				return SBM_NEEDS_TO_GROW;
			}
			SBM_CHUNK_SET_FLAGS(*chunk->m_data, bv, SBM_PAYLOAD_MIXED);
			pg_fallthrough;
		case SBM_PAYLOAD_MIXED:
			*pos = 1 + sbm_chunk_get_position(chunk, bv);
			w = chunk->m_data[*pos];
			w |= (SbmBitvec) 1 << idx % SBM_BITS_PER_VECTOR;

			/*
			 * Did the vector transition from mixed to all ones? If so, remove
			 * it.
			 */
			if (w == ~(SbmBitvec) 0)
			{
				SBM_CHUNK_SET_FLAGS(*chunk->m_data, bv, SBM_PAYLOAD_ONES);
				return SBM_NEEDS_TO_SHRINK;
			}
			/* Update the mixed vector. */
			chunk->m_data[*pos] = w;
			break;
		case SBM_PAYLOAD_NONE:
			pg_fallthrough;
		default:
			break;
	}
	return SBM_OK;
}

/*
 * Selects the nth bit with the specified value from a chunk.
 *
 * This function scans a chunk of data to find the nth occurrence of a bit
 * with the specified value (true for 1, false for 0) after skipping offset
 * bits (of any value).
 *
 * `chunk` is the chunk to scan for the bit; `n` is the number of bits of
 * value to count before returning; `offset` is the number of bits to skip
 * before starting to count; `value` is the bit value to search for (true for
 * 1, false for 0).
 * Returns the index within this chunk of the bit when found, otherwise the
 * number of bits scanned (at most SBM_BITS_PER_VECTOR).
 */
static size_t
sbm_chunk_select(const SbmChunk *chunk, ssize_t n, ssize_t *offset,
				 const bool value)
{
	size_t		ret = 0;
	SbmBitvec	sel_desc;
	size_t		i;

	/* RLE fast path */
	if (unlikely(sbm_chunk_is_rle(chunk)))
	{
		const size_t length = sbm_chunk_rle_get_length(chunk);
		const size_t capacity = sbm_chunk_rle_get_capacity(chunk);

		if (value)
		{
			/* Selecting nth set bit (1) */
			/* RLE has run of 1s from index 0 to length-1 */
			if (n < (ssize_t) length)
			{
				*offset = -1;
				return (size_t) n;	/* nth set bit is at index n */
			}
			else
			{
				*offset = n -
					(ssize_t) length;	/* propagate remainder to next chunk */
				return capacity;
			}
		}
		else
		{
			/* Selecting nth unset bit (0) */
			/* Unset bits start at index length */
			const size_t unset_count =
				(length >= capacity) ? 0 : capacity - length;

			if (length >= capacity)
			{
				/* No unset bits in this chunk */
				*offset = n;
				return capacity;
			}
			if (n < (ssize_t) unset_count)
			{
				*offset = -1;
				return length +
					(size_t) n; /* nth unset bit is at (length + n) */
			}
			else
			{
				*offset =
					n - (ssize_t) unset_count;	/* propagate remainder */
				return capacity;
			}
		}
	}

	/*
	 * Sparse encoding path
	 *
	 * Algorithm: Iterate through flag bytes examining 2-bit descriptors for
	 * each 64-bit vector. Skip vectors that can't contain the target value
	 * (ZEROS when searching for 1s, ONES when searching for 0s). For MIXED
	 * vectors, use popcount to quickly check if we need to scan individual
	 * bits. Accumulate bit positions until we've found the nth occurrence.
	 */
	sel_desc = *chunk->m_data;
	for (i = 0; i < sizeof(SbmBitvec); i++)
	{
		const uint8 b = sbm_desc_flag_byte(sel_desc, i);
		int			j;

		/*
		 * Quick skip: if flag byte is 0 (all NONE descriptors) and seeking
		 * 1s, skip 4 vectors
		 */
		if (b == 0 && value)
		{
			ret += (size_t) SBM_FLAGS_PER_INDEX_BYTE *
				SBM_BITS_PER_VECTOR;
			continue;
		}

		for (j = 0; j < SBM_FLAGS_PER_INDEX_BYTE; j++)
		{
			const size_t flags = SBM_CHUNK_GET_FLAGS(b, j);

			if (flags == SBM_PAYLOAD_NONE)
			{
				/*
				 * No payload, but the slot still occupies its index range:
				 * sbm_chunk_is_set addresses flags positionally as flags[idx
				 * / 64], so advance to keep the position we report in step
				 * with membership (see sbm_minimum).
				 */
				ret += SBM_BITS_PER_VECTOR;
				continue;
			}
			if (flags == SBM_PAYLOAD_ZEROS)
			{
				if (value == true)
				{
					ret += SBM_BITS_PER_VECTOR;
					continue;
				}

				/*
				 * This slot supplies exactly SBM_BITS_PER_VECTOR candidates,
				 * addressed n = 0 .. SBM_BITS_PER_VECTOR-1.  The guard must
				 * therefore be >=, not >: with > the n == SBM_BITS_PER_VECTOR
				 * case returned ret + 64, one position past the slot, instead
				 * of moving on to the next one.
				 */
				if (n >= SBM_BITS_PER_VECTOR)
				{
					n -= SBM_BITS_PER_VECTOR;
					ret += SBM_BITS_PER_VECTOR;
					continue;
				}
				*offset = -1;
				return ret + (size_t) n;
			}
			if (flags == SBM_PAYLOAD_ONES)
			{
				if (value == true)
				{
					/*
					 * Same off-by-one as the ZEROS arm above: an all-ones
					 * slot holds set bits n = 0 .. 63, so n == 64 belongs to
					 * a later slot.  With > this returned a position inside
					 * this slot for a bit that lives further on -- e.g. a map
					 * with bits [0,128) and [500,510) answered
					 * sbm_select(128, true) = 128 instead of 500.
					 */
					if (n >= SBM_BITS_PER_VECTOR)
					{
						n -= SBM_BITS_PER_VECTOR;
						ret += SBM_BITS_PER_VECTOR;
						continue;
					}
					*offset = -1;
					return ret + (size_t) n;
				}
				ret += SBM_BITS_PER_VECTOR;
				continue;
			}
			if (flags == SBM_PAYLOAD_MIXED)
			{
				const SbmBitvec w = chunk->m_data[1 +
												  sbm_chunk_get_position(chunk,
																		 (i * SBM_FLAGS_PER_INDEX_BYTE) + (size_t) j)];

				/* Use ctzll for fast bit extraction */
				SbmBitvec	target_bits = value ? w : ~w;
				SbmBitvec	remaining = target_bits;

				while (remaining)
				{
					int			k = pg_rightmost_one_pos64(remaining);

					if (n == 0)
					{
						*offset = -1;
						return ret + (size_t) k;
					}
					n--;
					remaining &= remaining -
						1;		/* clear lowest set bit */
				}
				ret += SBM_BITS_PER_VECTOR;
			}
		}
	}
	*offset = n;
	return ret;
}

/*
 * Count the set bits of a single chunk within the inclusive range [from, to],
 * where both bounds are chunk-relative (0 == the chunk's first index).  This
 * is the per-chunk kernel of sbm_rank; the unset count is derived by the
 * caller from the range width, so only set bits are counted here.
 */
static size_t
sbm_chunk_rank(const SbmChunk *chunk, size_t from, size_t to)
{
	size_t		amt = 0;
	const size_t cap = sbm_chunk_get_capacity(chunk);

	Assert(to >= from);
	if (from >= cap)
		return 0;
	if (to >= cap)
		to = cap - 1;

	if (unlikely(SBM_IS_CHUNK_RLE(chunk)))
	{
		/* RLE: set bits are exactly [0, length). */
		const size_t end = sbm_chunk_rle_get_length(chunk);

		if (from < end)
			amt = (to < end ? to + 1 : end) - from;
		return amt;
	}

	/*
	 * Sparse: walk the descriptor's per-vector flags.  ZEROS and NONE vectors
	 * contribute nothing, ONES contributes its overlap with the range, and
	 * MIXED is popcounted under a range mask.  Each vector covers
	 * SBM_BITS_PER_VECTOR consecutive positions, so track the running vector
	 * base against [from, to].
	 */
	{
		const SbmBitvec desc = *chunk->m_data;
		size_t		base = 0;	/* first position of the current vector */
		size_t		i;

		for (i = 0; i < SBM_FLAGS_PER_INDEX && base <= to; i++)
		{
			const size_t flags = SBM_CHUNK_GET_FLAGS(desc, i);
			const size_t vec_hi = base + SBM_BITS_PER_VECTOR - 1;

			if (base + SBM_BITS_PER_VECTOR > from &&
				(flags == SBM_PAYLOAD_ONES || flags == SBM_PAYLOAD_MIXED))
			{
				const size_t lo = from > base ? from - base : 0;
				const size_t hi =
					(to < vec_hi ? to : vec_hi) - base;

				if (flags == SBM_PAYLOAD_ONES)
					amt += hi - lo + 1;
				else
				{
					const SbmBitvec w =
						chunk->m_data[1 + sbm_chunk_get_position(chunk, i)];
					const uint64 lo_mask = ~(uint64) 0 << lo;
					const uint64 hi_mask = (hi == 63) ? ~(uint64) 0 :
						(((uint64) 1 << (hi + 1)) - 1);

					amt += (size_t) pg_popcount64(w & lo_mask & hi_mask);
				}
			}
			base += SBM_BITS_PER_VECTOR;
		}
	}
	return amt;
}

/*
 * Scans a chunk allowing the callee to process each vector.
 *
 * This function iterates through a chunk's data and processes these
 * payloads using the provided scanner function.
 *
 * `chunk` is the chunk to scan; `start` is the starting index for the scan;
 * `scanner` is the callback function to process discovered vectors; `skip`
 * is the number of vectors to skip before processing; `aux` is auxiliary
 * data to pass to the scanner function.
 * Returns the total number of processed vectors.
 */
static size_t
sbm_chunk_scan(const SbmChunk *chunk, const SbmIdx start,
			   void (*scanner) (uint64[], size_t, void *aux), size_t skip, void *aux)
{
	uint64		buffer[SBM_BITS_PER_VECTOR];
	size_t		i;
	size_t		pos = 0;
	size_t		skipped = 0;
	SbmBitvec	scan_desc;

	/* RLE fast path */
	if (unlikely(sbm_chunk_is_rle(chunk)))
	{
		const size_t length = sbm_chunk_rle_get_length(chunk);
		size_t		scan_start;

		/* RLE chunks only contain set bits from 0 to length-1 */
		if (skip >= length)
		{
			return length;		/* Skipped all bits in this chunk */
		}

		/* Skip first `skip` bits, then scan the rest */
		scan_start = skip;

		for (i = scan_start; i < length;)
		{
			size_t		batch_size = SBM_BITS_PER_VECTOR;
			size_t		j;

			if (i + batch_size > length)
			{
				batch_size = length - i;
			}

			/* Fill buffer with consecutive indices */
			for (j = 0; j < batch_size; j++)
			{
				buffer[j] = start + i + j;
			}

			scanner(&buffer[0], batch_size, aux);
			i += batch_size;
		}

		return skip;			/* Return number of bits skipped in this chunk */
	}

	/*
	 * Sparse encoding path. 'pos' tracks the bit offset within the chunk
	 * (each vector = SBM_BITS_PER_VECTOR). 'skip' counts set bits remaining
	 * to skip before scanning. Returns the number of set bits skipped in this
	 * chunk.
	 */
	scan_desc = *chunk->m_data;
	for (i = 0; i < sizeof(SbmBitvec); i++)
	{
		const uint8 b = sbm_desc_flag_byte(scan_desc, i);
		int			j;

		if (b == 0)
		{
			/*
			 * All 4 flag slots in this byte are ZEROS -- no set bits, advance
			 * position.
			 */
			pos += SBM_FLAGS_PER_INDEX_BYTE * SBM_BITS_PER_VECTOR;
			continue;
		}

		for (j = 0; j < SBM_FLAGS_PER_INDEX_BYTE; j++)
		{
			const size_t flags = SBM_CHUNK_GET_FLAGS(b, j);

			if (flags == SBM_PAYLOAD_NONE)
			{
				/* No capacity, but the slot still occupies its positions. */
				pos += SBM_BITS_PER_VECTOR;
			}
			else if (flags == SBM_PAYLOAD_ZEROS)
			{
				/* All zeroes -- no set bits to skip or scan. */
				pos += SBM_BITS_PER_VECTOR;
			}
			else if (flags == SBM_PAYLOAD_ONES)
			{
				if (skip >= SBM_BITS_PER_VECTOR)
				{
					skip -= SBM_BITS_PER_VECTOR;
					skipped += SBM_BITS_PER_VECTOR;
					pos += SBM_BITS_PER_VECTOR;
				}
				else if (skip > 0)
				{
					size_t		n = 0;
					size_t		bb;

					for (bb = skip;
						 bb < SBM_BITS_PER_VECTOR; bb++)
					{
						buffer[n++] = start + pos + bb;
					}
					skipped += skip;
					skip = 0;
					scanner(&buffer[0], n, aux);
					pos += SBM_BITS_PER_VECTOR;
				}
				else
				{
					size_t		bb;

					for (bb = 0;
						 bb < SBM_BITS_PER_VECTOR; bb++)
					{
						buffer[bb] = start + pos + bb;
					}
					scanner(&buffer[0], SBM_BITS_PER_VECTOR,
							aux);
					pos += SBM_BITS_PER_VECTOR;
				}
			}
			else if (flags == SBM_PAYLOAD_MIXED)
			{
				SbmBitvec	remaining = chunk->m_data[1 +
													  sbm_chunk_get_position(chunk,
																			 (i * SBM_FLAGS_PER_INDEX_BYTE) + (size_t) j)];
				size_t		n = 0;

				while (remaining)
				{
					int			bb = pg_rightmost_one_pos64(remaining);

					if (skip > 0)
					{
						skip--;
						skipped++;
					}
					else
					{
						buffer[n++] = start + pos +
							(size_t) bb;
					}
					remaining &= remaining -
						1;		/* clear lowest set bit */
				}
				if (n > 0)
				{
					scanner(&buffer[0], n, aux);
				}
				pos += SBM_BITS_PER_VECTOR;
			}
		}
	}
	return skipped;
}

/* -------------------------------------------------------------------
 * Map structure: chunk navigation, the tail cursor, and the
 * byte-level insert/remove/coalesce primitives
 * -------------------------------------------------------------------
 */

/*
 * Retrieves the count of chunks in the sparse map.
 *
 * This function reads the first 32-bit integer from the `m_data` array
 * of the given sparse map to determine and return the number of chunks.
 *
 * `map` is the sparse map from which to retrieve the chunk count.
 * Returns the number of chunks in the sparse map.
 */
static size_t
sbm_get_chunk_count(const Sbm *map)
{
	/*
	 * The chunk-count slot lives in the first SBM_SIZEOF_OVERHEAD bytes of
	 * m_data.  When m_data_used == 0 the slot has not been initialized (e.g.
	 * a freshly sbm_anchor'd buffer that has not yet been sbm_clear'd or
	 * sbm_open'd), so reading it would return whatever happened to be in the
	 * caller's buffer.  An uninitialized chunk-count slot therefore means "no
	 * chunks".
	 */
	if (map->m_data_used < SBM_SIZEOF_OVERHEAD)
	{
		return 0;
	}
	return (size_t) sbm_load_u64(&map->m_data[0]);
}

/*
 * Retrieves a pointer to the data at the specified offset within the sparse map.
 *
 * This function calculates the address of the data starting after a predefined
 * overhead and adds the provided offset to this start point. The resulting
 * pointer points to the actual data within the sparse map.
 *
 * `map` is a pointer to the sparse map; `offset` is the offset within the
 * sparse map where the data starts.
 * Returns a pointer to the data at the specified offset within the sparse map.
 */
static uint8 *
sbm_get_chunk_data(const Sbm *map, const size_t offset)
{
	return &map->m_data[SBM_SIZEOF_OVERHEAD + offset];
}

/*
 * Calculates the capacity limit for a run-length encoded (RLE) chunk.
 *
 * This function determines the capacity limit of a run-length encoded (RLE)
 * chunk in a sparse map, based on the provided map, start index, and offset.
 *
 * `map` is the sparse map containing the chunk; `start` is the starting
 * index of the chunk; `offset` is the offset within the sparse map's data.
 * Returns the capacity limit of the RLE chunk.
 */
static size_t
sbm_chunk_rle_capacity_limit(const Sbm *map, const SbmIdx start,
							 const size_t length, const size_t offset)
{
	/* Calculate where the data extends to */
	const size_t data_end = start + length;

	/* Round up to next VEC boundary (2048-aligned) */
	size_t		capacity =
		((data_end + SBM_CHUNK_MAX_CAPACITY - 1) / SBM_CHUNK_MAX_CAPACITY) *
		SBM_CHUNK_MAX_CAPACITY -
		start;

	/* Check if there's a next chunk that limits available space */
	const size_t next_offset =
		offset + SBM_SIZEOF_OVERHEAD + sizeof(SbmBitvec);

	if (next_offset <
		map->m_data_used - (SBM_SIZEOF_OVERHEAD + sizeof(SbmBitvec)))
	{
		uint8	   *p = sbm_get_chunk_data(map, next_offset);
		const SbmIdx next_start = sbm_load_idx((const uint8 *) p);
		const size_t available = next_start - start;

		/* Use whichever is smaller: VEC-aligned or available space */
		if (available < capacity)
		{
			capacity = available;
		}
	}

	/* Capacity must be large enough for the actual data */
	if (capacity < length)
	{
		capacity = length;
	}

	/* Clamp to RLE max */
	if (capacity > SBM_CHUNK_RLE_MAX_CAPACITY)
	{
		capacity = SBM_CHUNK_RLE_MAX_CAPACITY;
	}

	return capacity;
}

/*
 * Computes the aligned offset for a given index based on chunk capacity.
 *
 * This function calculates the offset for the provided index such that
 * it aligns with the chunk boundaries defined by the maximum chunk capacity.
 *
 * `idx` is the index for which the aligned offset is to be computed.
 * Returns the aligned offset corresponding to the given index.
 */
static SbmIdx
sbm_get_chunk_aligned_offset(const uint64 idx)
{
	const uint64 capacity = SBM_CHUNK_MAX_CAPACITY;

	return idx / capacity * capacity;
}

/*
 * Calculates the total size of the sparse map's used data.
 *
 * This function iterates through each chunk in the sparse map and computes
 * the total memory used by the map, including overhead.
 *
 * `map` is pointer to the sparse map.
 * Returns total size of the used data in the sparse map.
 *
 * Bounds-safe: when called on a possibly-corrupt buffer (after
 * sbm_open) the walker validates each chunk against m_capacity and
 * truncates the on-disk chunk count if any chunk would extend past
 * the buffer.  The returned size therefore corresponds to the
 * largest valid chunk-stream prefix; if the input is
 * well-formed, behavior is unchanged.
 */
static void sbm_set_chunk_count(const Sbm *map, size_t new_count);

static size_t
sbm_get_size_impl(const Sbm *map)
{
	uint8	   *start = sbm_get_chunk_data(map, 0);
	uint8	   *p = start;
	uint8	   *end = map->m_data + sbm_cap(map);
	size_t		count;
	size_t		valid_count = 0;
	size_t		i;

	/*
	 * Defensive: a chunk-data start outside the data buffer means the map
	 * header itself is corrupt.  Return the empty-map size.
	 */
	if (start < map->m_data || start > end)
	{
		return SBM_SIZEOF_OVERHEAD;
	}

	count = sbm_get_chunk_count(map);
	for (i = 0; i < count; i++)
	{
		SbmChunk	chunk;
		size_t		chunk_size;

		/*
		 * Each chunk needs at least SBM_SIZEOF_OVERHEAD bytes for its
		 * aligned-offset prefix plus sizeof(SbmBitvec) bytes for the
		 * mandatory chunk header word.  If less remains, the on-disk count is
		 * bogus.
		 */
		if ((size_t) (end - p) <
			SBM_SIZEOF_OVERHEAD + sizeof(SbmBitvec))
		{
			break;
		}
		p += SBM_SIZEOF_OVERHEAD;
		sbm_chunk_init(&chunk, p);
		chunk_size = sbm_chunk_get_size(&chunk);

		/*
		 * sbm_chunk_get_size returns at minimum sizeof(SbmBitvec). A chunk
		 * that claims to extend past `end` indicates corrupt flags; stop
		 * walking.
		 */
		if (chunk_size < sizeof(SbmBitvec) ||
			(size_t) (end - p) < chunk_size)
		{
			/*
			 * Roll back the SBM_SIZEOF_OVERHEAD we just advanced; we want to
			 * report the size up to the last *complete* chunk.
			 */
			p -= SBM_SIZEOF_OVERHEAD;
			break;
		}
		if (i + 1 < count)
		{
			pg_prefetch(p + chunk_size + SBM_SIZEOF_OVERHEAD);
		}
		p += chunk_size;
		valid_count++;
	}

	/*
	 * If the walker truncated, fix up the on-disk chunk count so subsequent
	 * operations see only the valid prefix.  This is the only place we mutate
	 * the map during what is logically a read; the const cast is intentional
	 * and the mutation is safe (we're correcting attacker-controlled
	 * corruption to a consistent, harmless state).
	 */
	if (valid_count != count)
	{
		sbm_set_chunk_count(unconstify(Sbm *, map), valid_count);
	}
	return SBM_SIZEOF_OVERHEAD + (size_t) (p - start);
}

/*
 * Retrieves the offset of a specified chunk within the sparse map.
 *
 * This function iterates through the chunks in the sparse map to find the
 * offset of the chunk that either contains or would logically contain the
 * given index.
 *
 * `map` is the sparse map to search within; `idx` is the index to find the
 * corresponding chunk offset for; `cur` is optional caller-owned cursor;
 * NULL = no acceleration.
 * Returns the offset of the chunk if found, otherwise -1 if no appropriate chunk is found.
 *
 * Read-cursor optimization:
 *
 * The naive implementation walks from chunk 0 on every call.  For a
 * map with N chunks this is O(N) per lookup, so a sequence of N
 * ascending lookups is O(N^2).  When the caller threads a cursor
 * (see SbmCursor) the walk resumes from the most-recently-located
 * chunk whenever the new idx is at or after that chunk's start,
 * making an ascending sequence O(N) overall.  Passing NULL (every
 * mutator does) walks from chunk 0.
 *
 * The cursor is caller-owned and purely an in-memory speedup; the
 * on-disk format is unchanged.  ANY mutation of the map invalidates
 * the caller's cursor (the caller must reset it); the library no
 * longer tracks cursor validity in the struct.
 */
static ssize_t
sbm_get_chunk_offset(const Sbm *map, const uint64 idx, SbmCursor *cur)
{
	const size_t count = sbm_get_chunk_count(map);
	uint8	   *base = sbm_get_chunk_data(map, 0);
	uint8	   *p = base;

	/*
	 * Offsets returned here are relative to `base` (the first chunk);
	 * m_data_used is relative to m_data and includes the SBM_SIZEOF_OVERHEAD
	 * chunk-count header, so the chunk stream occupies [0, stream_end) in
	 * base-relative offsets.  Bounding the walk by stream_end is correct no
	 * matter where we resume from (unlike an ordinal count, which would
	 * over-run when resuming partway through the chunk list).
	 */
	const size_t stream_end =
		(size_t) map->m_data_used - SBM_SIZEOF_OVERHEAD;

	/*
	 * Byte offset (base-relative) of the chunk immediately BEFORE the chunk
	 * we finally return, or SIZE_MAX if none.  Captured for free during the
	 * forward walk and handed back to the caller so the coalescing path can
	 * find the left neighbor without a head-walk.
	 */
	size_t		prev_off = SIZE_MAX;

	if (count == 0)
	{
		return -1;
	}

	/*
	 * Cursor fast-path.  If the caller passed a valid cursor whose cached
	 * chunk starts at or before idx, resume the walk from the cached byte
	 * offset instead of from the head.  Otherwise walk from chunk 0.  We
	 * never index chunks by ordinal here, so a relocated buffer is fine as
	 * long as the offset is still in range.
	 */
	if (cur != NULL && cur->offset != SIZE_MAX &&
		cur->offset + sizeof(SbmIdx) <= stream_end &&
		idx >= cur->start_idx)
	{
		/*
		 * Self-validate the cached offset before trusting it: a prior
		 * mutation (a chunk shrinking to RLE, a leftward coalesce, a
		 * separate) can shift or remove the cached chunk without the caller
		 * resetting.  If the chunk now at the cached offset no longer starts
		 * where we recorded, the cursor is stale -- walk from the head
		 * instead.
		 */
		const SbmIdx at = sbm_load_idx(base + cur->offset);

		if (at == cur->start_idx)
		{
			p = base + cur->offset;

			/*
			 * A resume that lands in this same chunk (no loop advance below)
			 * must still carry the left-neighbor hint the caller cached, or
			 * it would be lost.
			 */
			prev_off = cur->prev_offset;
		}
	}

	/*
	 * Walk to the chunk that contains idx, or the last chunk if idx is past
	 * the end.
	 */
	for (;;)
	{
		const SbmIdx s = sbm_load_idx((const uint8 *) p);
		SbmChunk	chunk;
		size_t		next_off;

		sbm_chunk_init(&chunk, p + SBM_SIZEOF_OVERHEAD);
		Assert(s == sbm_get_chunk_aligned_offset(s));
		next_off = (size_t) (p - base) +
			SBM_SIZEOF_OVERHEAD + sbm_chunk_get_size(&chunk);

		/*
		 * Move on only when idx lies beyond both this chunk's capacity and
		 * its window: a chunk may have less capacity than a full window (a
		 * short RLE run), but no other chunk can start inside that window, so
		 * idx still belongs to this chunk.
		 */
		if (idx >= s + sbm_chunk_get_capacity(&chunk) &&
			sbm_get_chunk_aligned_offset(idx) != s &&
			next_off < stream_end)
		{
			prev_off = (size_t) (p - base);
			p = base + next_off;
			continue;
		}
		if (cur != NULL)
		{
			cur->offset = (size_t) (p - base);
			cur->start_idx = s;
			cur->prev_offset = prev_off;
		}
		return p - base;
	}
}

/*
 * Sets the chunk count for the sparse bitmap to a new value.
 *
 * This function updates the chunk count stored in the map's data array
 * to the specified new count.
 *
 * `map` is the sparse bitmap in which to set the chunk count; `new_count` is
 * the new chunk count to set.
 */
static void
sbm_set_chunk_count(const Sbm *map, const size_t new_count)
{
	sbm_store_u64((uint8 *) &map->m_data[0], (uint64) new_count);
}

/*
 * Reset a caller-owned cursor to its invalid/initial state.  Field-wise
 * so it works as a plain assignment in C90 (a `(SbmCursor)SBM_CURSOR_INIT`
 * compound literal is C99).
 */
static inline void
sbm_cursor_reset(SbmCursor *c)
{
	c->offset = (size_t) -1;
	c->start_idx = 0;
	c->prev_offset = (size_t) -1;
}

/* -------------------------------------------------------------------
 * Small-set mode
 *
 * When every set index is below SBM_SMALL_MAX_BITS, the map stores a bare
 * uint64 bitmapword[] from bit 0 (bit i in word i/64) -- exactly like
 * PostgreSQL's Bitmapset -- behind an 8-byte header that ties
 * Bitmapset's 8-byte header, so the footprint matches or beats it for
 * near-zero sets.  Chunk mode keeps winning once indices spread.
 *
 * The mode is self-describing in the m_data byte stream: the same
 * 8-byte header word that holds the chunk count in chunk mode has its
 * top bit (SBM_SMALL_FLAG) set in small mode, with the bitmapword count
 * in the low 32 bits.  A chunk count never approaches 2^63, so the top
 * bit is free.  This map-level header word is distinct from every
 * per-chunk descriptor (which lives after the 8-byte chunk start), so
 * SBM_SMALL_FLAG can never collide with an RLE descriptor (bits 63:62 =
 * 01, SBM_RLE_FLAGS) or a real chunk count.  sbm_get_chunk_count / the
 * chunk walkers must never run on a small map; callers gate on
 * sbm_is_small() first.
 *
 * Layout (m_data):  [ SBM_SMALL_FLAG | nwords : 8 bytes ][ word[0..nwords) ]
 * m_data_used = SBM_SIZEOF_OVERHEAD + nwords * 8.
 * -------------------------------------------------------------------
 */

#define SBM_SMALL_FLAG   ((uint64)1 << 63)
#define SBM_SMALL_WMASK  (((uint64)1 << 32) - 1)
/*
 * Hard span cap: at 16 words the small form is at most 8 + 16*8 = 136
 * bytes.  It is preferred over the chunk form whenever it is not larger
 * and fits in place; in a tightly-sized buffer a growth-to-shrink is
 * skipped, so the small form can occasionally sit up to one word larger
 * than the chunk equivalent -- never larger than a Bitmapset.  Above
 * this cap the map is always in chunk mode.
 */
#define SBM_SMALL_MAX_WORDS 16u
#define SBM_SMALL_MAX_BITS  (SBM_SMALL_MAX_WORDS * 64u)

static inline bool
sbm_is_small(const Sbm *map)
{
	if (map == NULL || map->m_data == NULL ||
		map->m_data_used < SBM_SIZEOF_OVERHEAD)
	{
		return false;
	}
	return (sbm_load_u64(&map->m_data[0]) & SBM_SMALL_FLAG) != 0;
}

static inline size_t
sbm_small_nwords(const Sbm *map)
{
	return (size_t) (sbm_load_u64(&map->m_data[0]) & SBM_SMALL_WMASK);
}

static inline uint64 *
sbm_small_words(const Sbm *map)
{
	return (uint64 *) &map->m_data[SBM_SIZEOF_OVERHEAD];
}

static inline void
sbm_small_set_header(Sbm *map, size_t nwords)
{
	sbm_store_u64((uint8 *) &map->m_data[0],
				  SBM_SMALL_FLAG | (uint64) nwords);
}


/* True if idx is set in a small-mode map. */
static bool
sbm_small_contains(const Sbm *map, uint64 idx)
{
	const size_t w = (size_t) (idx / 64);

	if (w >= sbm_small_nwords(map))
	{
		return false;
	}
	return (sbm_small_words(map)[w] >> (idx % 64)) & 1u;
}

/* Population count of a small-mode map. */
static uint64
sbm_small_cardinality(const Sbm *map)
{
	const uint64 *w = sbm_small_words(map);
	const size_t n = sbm_small_nwords(map);
	uint64		c = 0;
	size_t		i;

	for (i = 0; i < n; i++)
	{
		c += (uint64) pg_popcount64(w[i]);
	}
	return c;
}

static bool
sbm_small_is_empty(const Sbm *map)
{
	const uint64 *w = sbm_small_words(map);
	const size_t n = sbm_small_nwords(map);
	size_t		i;

	for (i = 0; i < n; i++)
	{
		if (w[i] != 0)
		{
			return false;
		}
	}
	return true;
}

static uint64
sbm_small_minimum(const Sbm *map)
{
	const uint64 *w = sbm_small_words(map);
	const size_t n = sbm_small_nwords(map);
	size_t		i;

	for (i = 0; i < n; i++)
	{
		if (w[i] != 0)
		{
			return (uint64) i * 64 + (uint64) pg_rightmost_one_pos64(w[i]);
		}
	}
	return 0;
}

static uint64
sbm_small_maximum(const Sbm *map)
{
	const uint64 *w = sbm_small_words(map);
	const size_t n = sbm_small_nwords(map);
	size_t		i;

	for (i = n; i-- > 0;)
	{
		if (w[i] != 0)
		{
			return (uint64) i * 64 + (63 -
									  (63 - pg_leftmost_one_pos64(w[i])));
		}
	}
	return 0;
}

/*
 * Materialize a small-mode map into a freshly allocated chunk-mode map
 * (adding each set bit).  Returns NULL on allocation failure.  The
 * caller owns the result and disposes it with sbm_free().  Used to give
 * the chunk-walking read/algebra paths a uniform view of a small map
 * without mutating the (const) input.
 */
static Sbm *sbm_materialize(const Sbm *small);


/*
 * Appends data to the sparse bitmap's internal buffer.
 *
 * Copies buffer_size bytes to the end of the map's data region and
 * advances m_data_used.
 *
 * The caller must have established capacity first, because there is no
 * single correct response to "it does not fit" at this level: the
 * library-owned result maps in sbm_union() and friends grow (see
 * sbm_ensure_capacity(), which may reallocate and reassign the
 * caller's pointer), while operations on a caller-supplied buffer must
 * instead fail with errno=ENOSPC (see the SBM_ENOUGH_SPACE() macro).
 * A callee cannot pick between grow-and-continue and fail-fast, so the
 * policy stays with the caller.
 *
 * What this function *can* do is refuse to perform an overflowing copy
 * and say so, in every build, not only in assert-enabled ones.  The
 * result is pg_nodiscard so the compiler points at any caller that does
 * not check it.  Callers that have already reserved the space use
 * sbm_append_reserved(), which turns a failure into an elog(ERROR).
 *
 * `map` is map whose buffer is appended to; `buffer` is bytes to append;
 * `buffer_size` is number of bytes to append.
 * Returns true on success; false without copying anything if the bytes
 *         would not fit, which indicates a missing caller-side check.
 */
pg_nodiscard static bool
sbm_append_data(Sbm *map, const uint8 *buffer, const size_t buffer_size)
{
	if (unlikely(map->m_data_used + buffer_size > sbm_cap(map)))
	{
		Assert(map->m_data_used + buffer_size <= sbm_cap(map));
		errno = ENOSPC;
		return false;
	}

	memcpy(&map->m_data[map->m_data_used], buffer, buffer_size);
	map->m_data_used += buffer_size;
	return true;
}

/*
 * Append bytes whose space the caller has already reserved (typically via
 * sbm_ensure_capacity).  A failure here means the reservation and the write
 * disagree -- an internal bug -- so it is reported as an error rather than
 * silently returning a truncated result.
 */
static inline void
sbm_append_reserved(Sbm *map, const uint8 *buffer, size_t buffer_size)
{
	if (unlikely(!sbm_append_data(map, buffer, buffer_size)))
		elog(ERROR, "sparse bitmap append overran its reserved capacity");
}

/*
 * Inserts data into the sparse map at the specified offset.
 *
 * This function asserts that there is enough capacity in the map to accommodate
 * the new data, retrieves the appropriate chunk of data from the map, and then
 * inserts the provided buffer at the given offset. The existing data is moved
 * to make space for the new data, and the map's used data size is updated accordingly.
 *
 * `map` is pointer to the sparse map where data will be inserted; `offset`
 * is offset in the map where the data should be inserted; `buffer` is
 * pointer to the buffer containing the data to be inserted; `buffer_size` is
 * size of the buffer in bytes.
 */
static void
sbm_insert_data(Sbm *map, const size_t offset, const uint8 *buffer,
				const size_t buffer_size)
{
	uint8	   *p;

	Assert(map->m_data_used + buffer_size <= sbm_cap(map));
	Assert(offset <= map->m_data_used);

	p = sbm_get_chunk_data(map, offset);
	memmove(p + buffer_size, p, map->m_data_used - offset);
	memcpy(p, buffer, buffer_size);
	map->m_data_used += buffer_size;
}

/*
 * Removes a contiguous block of data from the sparse bitmap.
 *
 * This function removes a block of data from the sparse bitmap at the specified offset
 * and reduces the size of the data used accordingly.
 *
 * `map` is a pointer to the sparse bitmap from which data will be removed;
 * `offset` is the starting position of the block to be removed; `gap_size`
 * is the size of the block to be removed.
 */
static void
sbm_remove_data(Sbm *map, const size_t offset, const size_t gap_size)
{
	uint8	   *p;

	Assert(map->m_data_used >= gap_size);
	p = sbm_get_chunk_data(map, offset);
	memmove(p, p + gap_size, map->m_data_used - offset - gap_size);
	map->m_data_used -= gap_size;
}

/*
 * Coalesces the specified chunk with adjacent chunks if conditions are met.
 *
 * This function attempts to merge the provided chunk with its adjacent chunks
 * in a sparse map if they meet certain conditions. The goal is to reduce the
 * number of chunks by combining adjacent ones that form continuous runs.
 *
 * `map` is the sparse map that contains the chunk; `chunk` is the chunk to
 * be potentially coalesced; `offset` is the offset of the chunk in the
 * sparse map; `start` is the starting index of the chunk; `p` is pointer to
 * the chunk's data.
 * Returns the number of chunks that were removed during the coalescing process.
 */
static int
sbm_coalesce_chunk(Sbm *map, SbmChunk *chunk, size_t offset,
				   SbmIdx start, uint8 *p, uint64 idx, bool is_set_op,
				   size_t left_hint)
{
	/*
	 * This is called from sbm_chunk_set/unset/merge/split functions when a
	 * there is a chance that chunks should combine into runs to use less
	 * space in the map.
	 *
	 * The provided chunk may have two adjacent chunks, this function first
	 * processes the chunk to the left and then the one to the right.
	 *
	 * In the case that there is a chunk to the left (with a lower starting
	 * index) we examine its type and ending offset as well as it's run
	 * length.  Either type of chunk (sparse and RLE) can have a run.  In the
	 * case of an RLE chunk that's all it can express.  With a sparse chunk a
	 * run is defined as adjacent set bits starting at the 0th index of the
	 * chunk and extending up to at most the maximum size of a chunk without
	 * gaps ([1..SBM_CHUNK_MAX_CAPACITY] in length).  When the left chunk's
	 * run ends at the starting index of this chunk we can combine them.
	 * Combining these two will always result in an RLE chunk.
	 *
	 * Once that is finished... we may have something to the right as well. We
	 * look for an adjacent chunk, then determine if it has a run with a
	 * starting point adjacent to the end of a run in this chunk.  At this
	 * point we may have mutated and coalesced the left into the center chunk
	 * which we further mutate and combine with the right.  At most, we can
	 * combine three chunks into one in these two phases.
	 */
	int			num_removed = 0;
	const size_t run_length = sbm_chunk_get_run_length(chunk);
	const size_t capacity = sbm_chunk_get_capacity(chunk);
	const bool	is_rle = sbm_chunk_is_rle(chunk);

	/* Guard: do not coalesce an invalid RLE chunk */
	if (is_rle && run_length > capacity)
	{
		return num_removed;
	}
	/* Did this chunk become all ones, can we compact it with adjacent chunks? */
	if (run_length > 0)
	{
		SbmChunk	adj;

		/* Is there a previous chunk? */
		if (offset > 0)
		{
			/*
			 * Use the caller's left-neighbor hint when present and pointing
			 * strictly left of this chunk; otherwise fall back to a
			 * head-walk.  The hint is only a shortcut: the `adj_offset <
			 * offset` test below plus the `adj_start + adj_length == start`
			 * alignment guard still fully validate it, so a stale hint is
			 * slow (walks) or rejected, never a wrong merge.
			 */
			const size_t adj_offset =
				(left_hint != SIZE_MAX && left_hint < offset)
				? left_hint
				: (size_t) sbm_get_chunk_offset(map, start - 1,
												NULL);

			if (adj_offset < offset)
			{
				uint8	   *adj_p =
					sbm_get_chunk_data(map, adj_offset);
				const SbmIdx adj_start =
					sbm_load_idx((const uint8 *) adj_p);
				size_t		adj_length;

				sbm_chunk_init(&adj,
							   adj_p + SBM_SIZEOF_OVERHEAD);

				/*
				 * Is the adjacent chunk on the left RLE or a sparse chunk of
				 * all ones?
				 */
				adj_length = sbm_chunk_get_run_length(&adj);
				if (adj_length > 0)
				{
					/* Does it align with this chunk? */
					if (adj_start + adj_length == start)
					{
						if (SBM_CHUNK_MAX_CAPACITY +
							run_length <
							SBM_CHUNK_RLE_MAX_LENGTH)
						{
							/* Validate before coalescing */
							const size_t adj_capacity =
								sbm_chunk_get_capacity(
													   &adj);
							const bool	adj_is_rle =
								sbm_chunk_is_rle(
												 &adj);

							/*
							 * Calculate new length as span from adjacent
							 * start to end of current run
							 */
							const size_t new_length =
								(start +
								 run_length) -
								adj_start;

							/*
							 * Derive capacity from VEC-aligned boundaries,
							 * looking past the current chunk (being absorbed)
							 * to find the real next neighbor.
							 */
							const size_t
										merge_data_end =
								adj_start +
								new_length;
							bool		can_coalesce =
								true;
							size_t		new_capacity =
								((merge_data_end +
								  SBM_CHUNK_MAX_CAPACITY -
								  1) /
								 SBM_CHUNK_MAX_CAPACITY) *
								SBM_CHUNK_MAX_CAPACITY -
								adj_start;
							const size_t
										post_offset =
								offset +
								SBM_SIZEOF_OVERHEAD +
								sbm_chunk_get_size(
												   chunk);

							if (adj_is_rle &&
								adj_length >
								adj_capacity)
							{
								can_coalesce =
									false;
							}

							if (post_offset <
								map->m_data_used -
								(SBM_SIZEOF_OVERHEAD +
								 sizeof(
										SbmBitvec)))
							{
								const SbmIdx next_start =
									sbm_load_idx(
												 sbm_get_chunk_data(
																	map,
																	post_offset));
								const size_t avail =
									next_start -
									adj_start;

								if (avail <
									new_capacity)
								{
									new_capacity =
										avail;
								}
							}
							if (new_capacity <
								new_length)
							{
								new_capacity =
									new_length;
							}
							if (new_capacity >
								SBM_CHUNK_RLE_MAX_CAPACITY)
							{
								new_capacity =
									SBM_CHUNK_RLE_MAX_CAPACITY;
							}

							/*
							 * Validate that new length fits in available
							 * capacity
							 */
							if (can_coalesce &&
								new_length >
								new_capacity)
							{
								can_coalesce =
									false;
							}

							if (can_coalesce)
							{
								sbm_chunk_set_rle(
												  &adj);
								sbm_chunk_rle_set_capacity(
														   &adj,
														   new_capacity);
								sbm_chunk_rle_set_length(
														 &adj,
														 new_length);
								sbm_remove_data(
												map, offset,
												SBM_SIZEOF_OVERHEAD +
												sbm_chunk_get_size(
																   chunk));
								sbm_set_chunk_count(
													map,
													sbm_get_chunk_count(
																		map) -
													1);

								/*
								 * Now chunk is shifted to the left, it
								 * becomes the adjacent chunk.
								 */
								p = adj_p;
								offset =
									adj_offset;
								start =
									adj_start;
								sbm_chunk_init(
											   chunk,
											   p + SBM_SIZEOF_OVERHEAD);
								num_removed +=
									1;
							}
						}
					}
				}
			}
		}

		/* Is there a next chunk? */
		if (sbm_chunk_is_rle(chunk) ||
			chunk->m_data[0] == ~(SbmBitvec) 0)
		{
			/*
			 * The adjacent chunk begins after THIS chunk, so its offset
			 * depends on this chunk's real size.  An RLE chunk is just the
			 * descriptor, but the second arm of the condition above also
			 * matches an all-ONES *sparse* chunk, which carries 32 payload
			 * words too, so the stride must come from the chunk's real
			 * encoded size rather than being assumed to be one descriptor.
			 */
			const size_t adj_offset = offset + SBM_SIZEOF_OVERHEAD +
				sbm_chunk_get_size(chunk);

			if (adj_offset < map->m_data_used -
				(SBM_SIZEOF_OVERHEAD + sizeof(SbmBitvec)))
			{
				uint8	   *adj_p =
					sbm_get_chunk_data(map, adj_offset);
				const SbmIdx adj_start =
					sbm_load_idx((const uint8 *) adj_p);
				size_t		adj_length;

				sbm_chunk_init(&adj,
							   adj_p + SBM_SIZEOF_OVERHEAD);

				/*
				 * Is the adjacent right chunk RLE or a sparse with a run of
				 * ones?
				 */
				adj_length = sbm_chunk_get_run_length(&adj);

				/*
				 * If this is a SET operation and idx is valid and within the
				 * adjacent chunk, use it to calculate accurate run length
				 * (prevents overestimation)
				 */
				if (is_set_op && idx != SBM_IDX_MAX &&
					idx >= adj_start)
				{
					const size_t idx_based_length =
						idx - adj_start + 1;

					if (idx_based_length < adj_length)
					{
						adj_length = idx_based_length;
					}
				}
				if (adj_length)
				{
					/* Does it align with this full sparse chunk? */
					const size_t length =
						sbm_chunk_get_run_length(chunk);

					if (start + length == adj_start)
					{
						if (adj_length + length <
							SBM_CHUNK_RLE_MAX_LENGTH)
						{
							/* Validate adjacent chunk before coalescing */
							const size_t adj_capacity =
								sbm_chunk_get_capacity(
													   &adj);
							const bool	adj_is_rle =
								sbm_chunk_is_rle(
												 &adj);

							/*
							 * Calculate new length as span from this start to
							 * end of adjacent run
							 */
							const size_t new_length =
								(adj_start +
								 adj_length) -
								start;

							/*
							 * Derive capacity from VEC-aligned boundaries,
							 * looking past the adjacent chunk (being
							 * absorbed) to find the real next neighbor.
							 */
							const size_t
										r_data_end = start +
								new_length;
							const size_t r_adj_size =
								sbm_chunk_get_size(
												   &adj);
							const size_t r_post =
								adj_offset +
								SBM_SIZEOF_OVERHEAD +
								r_adj_size;
							bool		can_coalesce =
								true;
							size_t		new_capacity =
								((r_data_end +
								  SBM_CHUNK_MAX_CAPACITY -
								  1) /
								 SBM_CHUNK_MAX_CAPACITY) *
								SBM_CHUNK_MAX_CAPACITY -
								start;

							if (adj_is_rle &&
								adj_length >
								adj_capacity)
							{
								can_coalesce =
									false;
							}

							if (r_post <
								map->m_data_used -
								(SBM_SIZEOF_OVERHEAD +
								 sizeof(
										SbmBitvec)))
							{
								const SbmIdx nxt =
									sbm_load_idx(
												 sbm_get_chunk_data(
																	map,
																	r_post));
								const size_t
											avail =
									nxt -
									start;

								if (avail <
									new_capacity)
								{
									new_capacity =
										avail;
								}
							}
							if (new_capacity <
								new_length)
							{
								new_capacity =
									new_length;
							}
							if (new_capacity >
								SBM_CHUNK_RLE_MAX_CAPACITY)
							{
								new_capacity =
									SBM_CHUNK_RLE_MAX_CAPACITY;
							}

							/*
							 * Validate that new length fits in available
							 * capacity
							 */
							if (can_coalesce &&
								new_length >
								new_capacity)
							{
								can_coalesce =
									false;
							}

							if (can_coalesce)
							{
								/*
								 * `chunk` becomes RLE, which is
								 * descriptor-only: any sparse payload words
								 * it carried must go away too, not just the
								 * absorbed neighbour's bytes.  Leaving them
								 * behind desynchronised the chunk stream from
								 * m_data_used, so the coalesce walk kept
								 * re-reading the same bytes, reported
								 * progress forever and eventually read past
								 * the allocation.  Remove the neighbour and
								 * the payload in one contiguous span (the
								 * payload sits immediately before the
								 * neighbour).
								 */
								const size_t old_size =
									sbm_chunk_get_size(chunk);
								const size_t rle_size =
									sizeof(SbmBitvec);
								const size_t extra =
									(old_size > rle_size) ?
									old_size - rle_size :
									0;

								sbm_chunk_set_rle(
												  chunk);
								sbm_chunk_rle_set_capacity(
														   chunk,
														   new_capacity);
								sbm_chunk_rle_set_length(
														 chunk,
														 new_length);
								sbm_remove_data(
												map,
												adj_offset - extra,
												extra +
												SBM_SIZEOF_OVERHEAD +
												r_adj_size);
								sbm_set_chunk_count(
													map,
													sbm_get_chunk_count(
																		map) -
													1);
								num_removed +=
									1;
							}
						}
					}
				}
			}
		}
	}

	return num_removed;
}

/*
 * Coalesces adjacent chunks in a sparse map, optimizing its structure.
 *
 * This function iterates through the chunks in the provided sparse map and
 * attempts to coalesce adjacent chunks to reduce fragmentation and improve
 * efficiency.
 *
 * `map` is the sparse map to coalesce.
 * Returns the number of bytes coalesced during the operation.
 */
static size_t
sbm_coalesce_map(Sbm *map)
{
	SbmChunk	chunk;
	size_t		n = 0,
				count = sbm_get_chunk_count(map);

	/*
	 * `offset` must track `p`: sbm_coalesce_chunk derives the adjacent
	 * chunk's position from it, so it must track p as the walk advances.
	 */
	size_t		offset = 0;
	uint8	   *p = sbm_get_chunk_data(map, offset);

	while (count > 1)
	{
		SbmIdx		start;
		size_t		chunk_size;
		size_t		before;
		size_t		amt;
		size_t		after;

		/*
		 * The stored chunk count is the only bound on this walk, but a
		 * coalesce shrinks m_data_used (via sbm_remove_data) while the count
		 * can transiently over-report the chunks actually present in the
		 * buffer.  The buffer's true extent is m_data_used; stop when p
		 * reaches it regardless of the count.  A chunk needs at least the
		 * index word plus one descriptor word, so anything short of that is
		 * past the data.
		 */
		if ((size_t) (p - map->m_data) + SBM_SIZEOF_OVERHEAD +
			SBM_SIZEOF_OVERHEAD > map->m_data_used)
		{
			break;
		}
		start = sbm_load_idx((const uint8 *) p);
		sbm_chunk_init(&chunk, p + SBM_SIZEOF_OVERHEAD);
		chunk_size = sbm_chunk_get_size(&chunk);
		if (count > 1)
		{
			pg_prefetch(p + SBM_SIZEOF_OVERHEAD + chunk_size +
						SBM_SIZEOF_OVERHEAD);
		}
		before = sbm_get_chunk_count(map);
		amt = (size_t) sbm_coalesce_chunk(map, &chunk, offset,
										  start, p, SBM_IDX_MAX, false, SIZE_MAX);
		after = sbm_get_chunk_count(map);
		if (amt > 0 && after < before)
		{
			/*
			 * A neighbour was absorbed at this position; stay put and try to
			 * absorb the next one into the same chunk. The strict decrease
			 * guarantees termination.
			 */
			n += amt;
			count = after;
		}
		else
		{
			/*
			 * No progress here (or a coalesce that did not reduce the chunk
			 * count, which must not be retried -- doing so spun forever on
			 * the same bytes and eventually read past the allocation).  Move
			 * on.
			 */
			n += amt;
			p += SBM_SIZEOF_OVERHEAD + chunk_size;
			offset += SBM_SIZEOF_OVERHEAD + chunk_size;
			if (count == 0)
			{
				break;
			}
			count--;
		}
	}

	return n;
}

/*
 * Separates a run-length encoded (RLE) chunk into new chunks based on the provided parameters.
 *
 * This function is called from various chunk manipulation functions such as
 * set, unset, merge, and split when an RLE chunk needs to be mutated into one
 * or more new chunks. It determines the separation and alignment of the pivot
 * chunk with respect to the target chunk.
 *
 * `map` is the sparse map containing the chunks; `sep` is the separation
 * information required to perform the chunk separation; `idx` is the index
 * within the chunk where the separation or mutation is required; `state` is
 * the state representing the operation: 0 for clearing a bit, 1 for setting
 * a bit, and -1 for splitting without modifying the map.
 * Returns Integer value indicating the status of the operation:
 *         0 if the operation is successful,
 *         an error code otherwise.
 */
static int
sbm_separate_rle_chunk(Sbm *map, SbmChunkSep *sep, const uint64 idx,
					   const int state)
{
	/*
	 * This is called from sbm_chunk_set/unset/merge/split functions when a
	 * run-length encoded (RLE) chunk must be mutated into one or more new
	 * chunks.
	 *
	 * This function expects that the separation information is complete and
	 * that the pivot chunk has yet to be created.  The target will always be
	 * RLE and the pivot will always be a new sparse chunk.  The hard part is
	 * where the pivot lies in relation to the target.
	 *
	 * - left aligned - right aligned - centrally aligned
	 *
	 * When left aligned the chunk-aligned starting index of the pivot matches
	 * the starting index of the target. This results in two chunks, one new
	 * (the pivot) on the left, and one shortened RLE on the right.
	 *
	 * When right aligned there are two cases, the second more common one is
	 * when the chunk-aligned starting index of the pivot plus its length
	 * extends beyond the end of the run length of the target RLE chunk but is
	 * still within the capacity of the RLE chunk. This again results in two
	 * chunks, one on the left for the remainder of the run and one to the
	 * right.  In rare cases the end of the pivot chunk perfectly aligns with
	 * the end of the target's length.
	 *
	 * The last case is when the chunk-aligned starting index is somewhere
	 * within the body of the target.  This results in three chunks; left,
	 * right, and pivot (or center).
	 *
	 * In all three cases the new chunks (left and right) may be either RLE or
	 * sparse encoded, that's TBD based on their sizes after the pivot area is
	 * removed from the body of the run.
	 */

	SbmChunk	pivot_chunk;
	SbmChunk	lrc;
	uint64		aligned_idx;
	int			i;
	const size_t base = SBM_SIZEOF_OVERHEAD + sizeof(SbmBitvec);
	size_t		total;

	Assert(state == 0 || state == 1 || state == -1);
	Assert(SBM_IS_CHUNK_RLE(sep->target.chunk));

	if (state == 1)
	{
		/* setting a bit beyond the run but within capacity */
		Assert(idx >= sep->target.start);
		Assert(idx < sep->target.start + sep->target.capacity);
	}
	else if (state == 0)
	{
		/* clearing a bit */
		Assert(idx >= sep->target.start);
		Assert(idx < sep->target.length + sep->target.start);
	}
	else if (state == -1)
	{
		/* if `state == -1` we are splitting at idx but leaving map unmodified */
	}

	memset(sep->buf, 0,
		   (SBM_SIZEOF_OVERHEAD * (unsigned long) 3) +
		   (sizeof(SbmBitvec) * 6));

	/* Find the starting offset for our pivot chunk ... */
	aligned_idx = sbm_get_chunk_aligned_offset(idx);
	Assert(
		   idx >= aligned_idx && idx < aligned_idx + SBM_CHUNK_MAX_CAPACITY);
	/* avoid changing the map->m_data and for now work in our buf ... */
	sep->pivot.p = sep->buf;
	sbm_store_idx((uint8 *) sep->pivot.p, aligned_idx);
	sbm_chunk_init(&pivot_chunk, sep->pivot.p + SBM_SIZEOF_OVERHEAD);

	/* The pivot, extracted from a run, starts off as all 1s. */
	pivot_chunk.m_data[0] = ~(SbmBitvec) 0;

	if (state == 0)
	{
		/* To unset, change the flag at the position of the idx to "mixed" ... */
		const size_t vec_idx = (idx - aligned_idx) / SBM_BITS_PER_VECTOR;
		const size_t bit_pos = (idx - aligned_idx) % SBM_BITS_PER_VECTOR;

		SBM_CHUNK_SET_FLAGS(pivot_chunk.m_data[0], vec_idx,
							SBM_PAYLOAD_MIXED);
		/* and clear only the bit at that index in this chunk. */
		pivot_chunk.m_data[1] =
			~(SbmBitvec) 0 & ~((SbmBitvec) 1 << bit_pos);
		sep->pivot.size =
			SBM_SIZEOF_OVERHEAD + sizeof(SbmBitvec) * 2;
	}
	else if (state == 1)
	{
		if (idx >= sep->target.start &&
			idx < sep->target.start + sep->target.length)
		{
			/* It's a no-op to set a bit in a range of bits already set. */
			return 0;
		}
		sep->pivot.size =
			SBM_SIZEOF_OVERHEAD + sizeof(SbmBitvec) * 2;
	}
	else if (state == -1)
	{
		/* Unmodified */
		sep->pivot.size = SBM_SIZEOF_OVERHEAD + sizeof(SbmBitvec);
	}

	/* Where did the pivot chunk fall within the original chunk? */
	do
	{
		if (aligned_idx == sep->target.start &&
			sep->target.length > SBM_CHUNK_MAX_CAPACITY)
		{
			/*
			 * Left aligned and the run spills past this one window: two
			 * chunks -- the pivot on the left and a shortened RLE run on the
			 * right.  When the run instead fits within one window (length <=
			 * SBM_CHUNK_MAX_CAPACITY) there is no right remainder; that case
			 * falls through to the straddle branch below, which handles
			 * aligned_idx == start by emitting a single sparse chunk (no left
			 * chunk).
			 */
			sep->count = 2;
			sep->ex[1].start = aligned_idx + SBM_CHUNK_MAX_CAPACITY;
			sep->ex[1].end = aligned_idx + sep->target.length - 1;
			sep->ex[1].p =
				(uint8 *) ((uintptr_t) sep->buf + sep->pivot.size);
			Assert(sep->ex[1].start <= sep->ex[1].end);
			Assert(sep->ex[0].p == 0);
			break;
		}

		if (aligned_idx < sep->target.start + sep->target.length &&
			aligned_idx + SBM_CHUNK_MAX_CAPACITY >=
			sep->target.start + sep->target.length)
		{
			/*
			 * The pivot straddles the end of the run: its aligned start lies
			 * inside the run, but the run ends within this one window.  Two
			 * chunks total.  The extra `aligned_idx < start + length` guard
			 * is what keeps amt_over below one window: without it a pivot
			 * that sits ENTIRELY past the run end (aligned_idx >= start +
			 * length -- the case that belongs to the "beyond the run" branch
			 * below) would still enter here because MAX_CAPACITY > 0 makes
			 * the upper-bound test trivially true, and amt_over = aligned_idx
			 * + MAX_CAPACITY - (start + length) would exceed one window --
			 * driving amt_over/64*2 past 63 (shift UB) and first_zero =
			 * MAX_CAPACITY - amt_over negative (size underflow) on maps whose
			 * RLE capacity was widened well past the run.
			 */
			const uint64 amt_over = aligned_idx +
				SBM_CHUNK_MAX_CAPACITY -
				(sep->target.start + sep->target.length);

			sep->count = (aligned_idx > sep->target.start) ? 2 : 1;

			/*
			 * With the straddle guard above, 0 <= amt_over <
			 * SBM_CHUNK_MAX_CAPACITY, so amt_over / 64 * 2 <= 62 and
			 * first_zero stays in (0, MAX_CAPACITY].  Assert the bound so a
			 * future guard change that reintroduces an over-one-window pivot
			 * trips a test rather than shifts by >= 64.
			 */
			Assert(amt_over < SBM_CHUNK_MAX_CAPACITY);
			if (amt_over > 0)
			{
				/* The index of the first 0 bit. */
				const size_t first_zero =
					SBM_CHUNK_MAX_CAPACITY - amt_over;
				const size_t bv =
					first_zero / SBM_BITS_PER_VECTOR;

				/*
				 * Shorten the pivot chunk because it extends beyond the end
				 * of the run ...
				 */
				if (amt_over >= SBM_BITS_PER_VECTOR)
				{
					/*
					 * Clear the `amt_over / 64` whole ONES vectors that fall
					 * past the run end.  `>=` not `>`: when the run ends
					 * exactly one vector short of the window (amt_over == 64,
					 * a vector-boundary run tail with amt_over % 64 == 0)
					 * that one trailing ONES vector still has to be cleared
					 * -- with `>` it survived, leaving a full 64 bits set
					 * past the run end (e.g. a run [0, 1984) kept bits
					 * 1984..2047 set). `~0 >> (amt_over/64*2)` keeps exactly
					 * the SBM_FLAGS_PER_INDEX - amt_over/64 leading ONES
					 * flags; the straddle guard bounds amt_over <
					 * SBM_CHUNK_MAX_CAPACITY so the shift is always < 64.
					 */
					pivot_chunk.m_data[0] &=
						~(SbmBitvec) 0 >>
						amt_over / SBM_BITS_PER_VECTOR * 2;
				}
				if (amt_over % SBM_BITS_PER_VECTOR)
				{
					/* Partial run-tail vector: bits [0, first_zero%64) set. */
					const SbmBitvec tail_mask =
						~(~(SbmBitvec) 0 << first_zero %
						  SBM_BITS_PER_VECTOR);

					/*
					 * Change only the flag at the position of the last index
					 * to "mixed" ...
					 */
					SBM_CHUNK_SET_FLAGS(
										pivot_chunk.m_data[0], bv,
										SBM_PAYLOAD_MIXED);
					if (state == 0 && bv == (idx - aligned_idx) /
						SBM_BITS_PER_VECTOR)
					{
						/*
						 * The cleared bit shares the run-tail vector: keep
						 * the single MIXED payload already written by the
						 * state==0 setup (all-ones-minus-cleared-bit) and
						 * just mask off the bits past the run end.  No new
						 * payload, no size change.
						 */
						pivot_chunk.m_data[1] &= tail_mask;
					}
					else if (state == 0)
					{
						/*
						 * Distinct vectors (bv > vec_idx, since the cleared
						 * bit lies within the run).  The state==0 setup
						 * already placed the cleared bit's payload at
						 * m_data[1]; the run-tail MIXED needs its own payload
						 * at m_data[2] (higher flag index sorts after).
						 * Writing it to m_data[1] as the non-state==0 path
						 * does would clobber the cleared-bit payload and
						 * leave pivot.size one vector short -- corrupting the
						 * chunk stream.
						 */
						pivot_chunk.m_data[2] = tail_mask;
						sep->pivot.size +=
							sizeof(SbmBitvec);
					}
					else
					{
						/* and unset the bits beyond that. */
						pivot_chunk.m_data[1] = tail_mask;
						if (state == -1)
						{
							sep->pivot.size +=
								sizeof(SbmBitvec);
						}
					}
				}
			}

			/*
			 * Make room for the left chunk only when the pivot actually sits
			 * to the right of the run start.  When the pivot is left aligned
			 * (aligned_idx == start, reached via the fall-through for a run
			 * that fits in one window) there is no left chunk: the masked
			 * pivot is the whole result, a single sparse chunk, and it stays
			 * at sep->buf.
			 */
			if (aligned_idx > sep->target.start)
			{
				/*
				 * Move the pivot chunk over to make room for the new left
				 * chunk.
				 */
				memmove((uint8 *) ((uintptr_t) sep->buf +
								   SBM_SIZEOF_OVERHEAD +
								   (sizeof(SbmBitvec) * 2)),
						sep->buf, sep->pivot.size);
				memset(sep->buf, 0,
					   SBM_SIZEOF_OVERHEAD +
					   (sizeof(SbmBitvec) * 2));
				sep->pivot.p +=
					SBM_SIZEOF_OVERHEAD +
					(sizeof(SbmBitvec) * 2);
			}

			/* Re-initialize pivot_chunk (moved or not). */
			sbm_chunk_init(&pivot_chunk,
						   sep->pivot.p + SBM_SIZEOF_OVERHEAD);

			/*
			 * Are we setting a bit beyond the length where we partially
			 * overlap?
			 */
			if (state == 1 &&
				idx > sep->target.start + sep->target.length)
			{
				const size_t vec_idx =
					(idx - aligned_idx) / SBM_BITS_PER_VECTOR;
				const size_t bit_pos =
					(idx - aligned_idx) % SBM_BITS_PER_VECTOR;
				const size_t existing_mixed =
					sbm_chunk_get_size(&pivot_chunk) /
					sizeof(SbmBitvec) -
					1;
				const size_t cur_flags = SBM_CHUNK_GET_FLAGS(
															 pivot_chunk.m_data[0], vec_idx);

				if (cur_flags == SBM_PAYLOAD_MIXED)
				{
					/* Same vector as the partial run -- just OR the bit in. */
					const size_t pos = 1 +
						sbm_chunk_get_position(
											   &pivot_chunk, vec_idx);

					pivot_chunk.m_data[pos] |=
						(SbmBitvec) 1 << bit_pos;
				}
				else
				{
					size_t		pos;
					size_t		vecs_after;

					/*
					 * Different vector -- add a new MIXED flag and payload
					 * vector.
					 */
					SBM_CHUNK_SET_FLAGS(
										pivot_chunk.m_data[0], vec_idx,
										SBM_PAYLOAD_MIXED);
					pos = 1 +
						sbm_chunk_get_position(
											   &pivot_chunk, vec_idx);

					/*
					 * Shift existing vectors after this position to make
					 * room.
					 */
					vecs_after =
						existing_mixed - (pos - 1);
					if (vecs_after > 0)
					{
						memmove(&pivot_chunk
								.m_data[pos + 1],
								&pivot_chunk.m_data[pos],
								vecs_after *
								sizeof(SbmBitvec));
					}
					pivot_chunk.m_data[pos] =
						(SbmBitvec) 1 << bit_pos;
					sep->pivot.size +=
						sizeof(SbmBitvec);
				}

				/*
				 * The incremental size accounting above assumes the initial
				 * state==1 reservation (one payload vector) was consumed by a
				 * run-tail MIXED flag. When the run tail ended on a vector
				 * boundary (amt_over % SBM_BITS_PER_VECTOR == 0) there is no
				 * run-tail MIXED, the reserved slot is free, and the += above
				 * over-counts pivot.size by one vector -- inflating expand_by
				 * and inserting a stray 8 bytes that desync the sequential
				 * chunk walk.  Recompute the pivot size from the chunk's
				 * actual flags so it is exact regardless of which combination
				 * of run-tail / new-bit vectors is present.
				 */
				sep->pivot.size = SBM_SIZEOF_OVERHEAD +
					sbm_chunk_get_size(&pivot_chunk);
			}

			/*
			 * Record information necessary to construct the left chunk (only
			 * when there is one; a left-aligned pivot is a single chunk with
			 * no left remainder).
			 */
			if (aligned_idx > sep->target.start)
			{
				sep->ex[0].start = sep->target.start;
				sep->ex[0].end = aligned_idx - 1;
				sep->ex[0].p = sep->buf;
				Assert(sep->ex[0].start <= sep->ex[0].end);
			}
			Assert(sep->ex[1].p == 0);
			break;
		}

		if (aligned_idx >= sep->target.start + sep->target.length)
		{
			/*
			 * The pivot lies entirely beyond the run but within the chunk's
			 * capacity.  The run [start, start + length) is wholly to the
			 * left of the pivot window, so there is never a right remainder:
			 * the result is exactly two chunks -- the shortened RLE run on
			 * the left and the new sparse pivot holding the toggled bit.
			 *
			 * This used to be split into a `aligned_idx + MAX_CAPACITY <
			 * capacity` case plus an "unreachable" else that asserted and
			 * then fell through to the central three-chunk code.  That else
			 * IS reachable on a map whose RLE capacity was widened past
			 * roundup(start + length): when the pivot sits in the final,
			 * partial-capacity window (aligned_idx + MAX_CAPACITY > capacity)
			 * the fall-through built a bogus inverted right chunk
			 * (ex[1].start = aligned_idx + MAX_CAPACITY > ex[1].end = start +
			 * length - 1) and corrupted the stream.  Both windows want the
			 * identical two-chunk layout, so handle them together.
			 */
			sep->count = (sep->target.length > 0) ? 2 : 1;
			pivot_chunk.m_data[0] = (SbmBitvec) 0;

			/*
			 * Make room for the left (RLE run) chunk only when the run is
			 * non-empty.  A zero-length run (adversarial: the writer never
			 * emits one) has no left chunk, so the pivot is the whole result
			 * -- a single sparse chunk that stays at sep->buf.  Without this
			 * guard ex[0] below would be [start, start - 1], an inverted
			 * chunk.
			 */
			if (sep->target.length > 0)
			{
				/*
				 * Move the pivot chunk over to make room for the new left
				 * chunk.
				 */
				memmove((uint8 *) ((uintptr_t) sep->buf +
								   SBM_SIZEOF_OVERHEAD +
								   (sizeof(SbmBitvec) * 2)),
						sep->buf, sep->pivot.size);
				memset(sep->buf, 0,
					   SBM_SIZEOF_OVERHEAD +
					   (sizeof(SbmBitvec) * 2));
				sep->pivot.p +=
					SBM_SIZEOF_OVERHEAD +
					sizeof(SbmBitvec) * 2;
			}

			/* Re-initialize pivot_chunk (moved or not). */
			sbm_chunk_init(&pivot_chunk,
						   sep->pivot.p + SBM_SIZEOF_OVERHEAD);

			if (state == 1)
			{
				/*
				 * Change only the flag at the position of the index to
				 * "mixed" ...
				 */
				const size_t vec_idx =
					(idx - aligned_idx) / SBM_BITS_PER_VECTOR;
				const size_t bit_pos =
					(idx - aligned_idx) % SBM_BITS_PER_VECTOR;

				SBM_CHUNK_SET_FLAGS(pivot_chunk.m_data[0],
									vec_idx, SBM_PAYLOAD_MIXED);
				/* and set the bit at that index in this chunk. */
				pivot_chunk.m_data[1] |= (SbmBitvec) 1
					<< bit_pos;
			}

			/*
			 * Record information necessary to construct the left chunk (only
			 * when the run is non-empty; a zero-length run has no left
			 * chunk).
			 */
			if (sep->target.length > 0)
			{
				sep->ex[0].start = sep->target.start;
				sep->ex[0].end =
					sep->target.start + sep->target.length - 1;
				sep->ex[0].p = sep->buf;
			}
			break;
		}

		/* The pivot's range is central, there will be three chunks in total. */
		sep->count = 3;
		/* Move the pivot chunk over to make room for the new left chunk. */
		memmove((uint8 *) ((uintptr_t) sep->buf + SBM_SIZEOF_OVERHEAD +
						   (sizeof(SbmBitvec) * 2)),
				sep->buf, sep->pivot.size);
		memset(sep->buf, 0,
			   SBM_SIZEOF_OVERHEAD + (sizeof(SbmBitvec) * 2));
		sep->pivot.p +=
			SBM_SIZEOF_OVERHEAD + (sizeof(SbmBitvec) * 2);
		/* Record information necessary to construct the left & right chunks. */
		sep->ex[0].start = sep->target.start;
		sep->ex[0].end = aligned_idx - 1;
		sep->ex[0].p = sep->buf;
		sep->ex[1].start = aligned_idx + SBM_CHUNK_MAX_CAPACITY;
		sep->ex[1].end = sep->target.start + sep->target.length - 1;
		sep->ex[1].p = (uint8 *) ((uintptr_t) sep->buf +
								  (SBM_SIZEOF_OVERHEAD + sizeof(SbmBitvec) * 2) +
								  sep->pivot.size);
		Assert(sep->ex[0].start < sep->ex[0].end);
		Assert(sep->ex[1].start < sep->ex[1].end);
	} while (0);

	for (i = 0; i < 2; i++)
	{
		if (sep->ex[i].p)
		{
			/* First assign the starting offset ... */
			sbm_store_idx((uint8 *) sep->ex[i].p,
						  sep->ex[i].start);
			/* ... then, construct a chunk ... */
			sbm_chunk_init(&lrc,
						   sep->ex[i].p + SBM_SIZEOF_OVERHEAD);
			/* ... determine the type of chunk required ... */
			if (sep->ex[i].end - sep->ex[i].start + 1 >
				SBM_CHUNK_MAX_CAPACITY)
			{
				/* ... we need a run-length encoding (RLE), chunk ... */
				/* Capacity is set before length to satisfy the invariant */
				const size_t rle_length =
					sep->ex[i].end - sep->ex[i].start + 1;

				sbm_chunk_set_rle(&lrc);
				/* ... a few things differ left to right ... */
				if (i == 0)
				{
					/*
					 * ... left: extend capacity to the start of the pivot
					 * chunk ...
					 */
					sbm_chunk_rle_set_capacity(&lrc,
											   aligned_idx - sep->ex[i].start);

					/*
					 * ... and shift the pivot chunk and start of lr[1] left
					 * one vector ...
					 */
					memmove(
							(uint8 *) ((uintptr_t) sep->buf +
									   SBM_SIZEOF_OVERHEAD +
									   sizeof(SbmBitvec)),
							sep->pivot.p, sep->pivot.size);
					memset((uint8 *) ((uintptr_t) sep->buf +
									  SBM_SIZEOF_OVERHEAD +
									  sizeof(SbmBitvec) +
									  sep->pivot.size),
						   0, sizeof(SbmBitvec));
					if (sep->ex[1].p)
					{
						sep->ex[1].p =
							(uint8 *) ((uintptr_t) sep
									   ->ex[1]
									   .p -
									   sizeof(SbmBitvec));
					}
				}
				else
				{
					/*
					 * ... right: capacity spans from THIS chunk's start to
					 * the end of the original target's capacity.  Use
					 * ex[i].start (the right chunk's actual aligned start),
					 * NOT aligned_idx (the pivot's start): the two differ by
					 * SBM_CHUNK_MAX_CAPACITY whenever the pivot sits to the
					 * left of the right chunk (every left-aligned and central
					 * split). Using aligned_idx over-counts the capacity by
					 * one window, so the right RLE's capacity overruns into
					 * the following chunk's index range and the sequential
					 * walk resolves lookups against the wrong chunk.
					 */
					size_t		right_cap =
						(sep->target.start +
						 sep->target.capacity) -
						sep->ex[i].start;

					if (right_cap >
						SBM_CHUNK_RLE_MAX_CAPACITY)
					{
						right_cap =
							SBM_CHUNK_RLE_MAX_CAPACITY;
					}
					sbm_chunk_rle_set_capacity(&lrc,
											   right_cap);
				}
				sbm_chunk_rle_set_length(&lrc, rle_length);
				/* ... and record our chunk size. */
				sep->ex[i].size =
					SBM_SIZEOF_OVERHEAD + sizeof(SbmBitvec);
			}
			else
			{
				/* ... we need a new sparse chunk, how long should it be? ... */
				const size_t lrl =
					sep->ex[i].end - sep->ex[i].start + 1;

				/* ... how many flags can we mark as all ones? ... */
				if (lrl >= SBM_BITS_PER_VECTOR)
				{
					/*
					 * `>=` not `>`: a run of exactly one vector (lrl ==
					 * SBM_BITS_PER_VECTOR) still needs its single ONES flag
					 * set.  With `>` the lrl == 64 case fell through with an
					 * all-zero flags word, producing an empty chunk that
					 * dropped a full vector of set bits.  lrl < 64 is handled
					 * by the MIXED branch below, so it never reaches the
					 * UB-shift here.
					 */
					lrc.m_data[0] = ~(SbmBitvec) 0 >>
						(SBM_FLAGS_PER_INDEX -
						 lrl / SBM_BITS_PER_VECTOR) *
						2;
				}

				/*
				 * ... do we have a mixed flag to create and vector to assign?
				 * ...
				 */
				if (lrl % SBM_BITS_PER_VECTOR)
				{
					/*
					 * The vector index is *within* the chunk, not absolute:
					 * mixing the absolute aligned_idx with the chunk-relative
					 * length would produce shift exponents past 64.
					 */
					SBM_CHUNK_SET_FLAGS(lrc.m_data[0],
										lrl / SBM_BITS_PER_VECTOR,
										SBM_PAYLOAD_MIXED);
					lrc.m_data[1] |= ~(SbmBitvec) 0 >>
						(SBM_BITS_PER_VECTOR - lrl) %
						SBM_BITS_PER_VECTOR;
					/* ... record our chunk size ... */
					sep->ex[i].size = SBM_SIZEOF_OVERHEAD +
						sizeof(SbmBitvec) * 2;
				}
				else
				{
					/*
					 * ... earlier size estimates were all pessimistic, adjust
					 * them ...
					 */
					if (i == 0)
					{
						/*
						 * ... and shift the pivot chunk and start of lr[1]
						 * left one vector ...
						 */
						memmove(
								(uint8 *) ((uintptr_t)
										   sep->buf +
										   SBM_SIZEOF_OVERHEAD +
										   sizeof(SbmBitvec)),
								sep->pivot.p,
								sep->pivot.size);
						memset(
							   (uint8 *) ((uintptr_t)
										  sep->buf +
										  SBM_SIZEOF_OVERHEAD +
										  sizeof(SbmBitvec) +
										  sep->pivot.size),
							   0, sizeof(SbmBitvec));
						if (sep->ex[1].p)
						{
							sep->ex[1].p = (uint8
											*) ((uintptr_t) sep
												->ex[1]
												.p -
												sizeof(
													   SbmBitvec));
						}
					}
					/* ... record our chunk size ... */
					sep->ex[i].size = SBM_SIZEOF_OVERHEAD +
						sizeof(SbmBitvec);
				}
			}
		}
	}

	/* Determine if we have room for this construct. */

	/*
	 * Defense in depth: refuse to compute a negative (wrapped) size if the
	 * pivot/ex sizes were not populated.
	 */
	total = sep->pivot.size + sep->ex[0].size + sep->ex[1].size;
	if (total < base)
	{
		Assert(0 &&
			   "sbm_separate_rle_chunk: pivot/ex sizes uninitialized");
		errno = EINVAL;
		return -1;
	}
	sep->expand_by = total - base;

	/*
	 * sbm_insert_data's memmove length (m_data_used - offset) treats `offset`
	 * as m_data-relative while the caller passes a data-region offset, so the
	 * shift writes to m_data + m_data_used + expand_by + SBM_SIZEOF_OVERHEAD
	 * -- SBM_SIZEOF_OVERHEAD past m_data_used + expand_by.  The
	 * SBM_ENOUGH_SPACE macro carries the same slack for this reason; without
	 * it here the separate overruns the buffer by SBM_SIZEOF_OVERHEAD bytes
	 * at the exact-fit boundary (used + expand_by == cap) instead of cleanly
	 * returning ENOSPC so sbm_add_grow can grow and retry.
	 */
	if (map->m_data_used + sep->expand_by + SBM_SIZEOF_OVERHEAD >
		sbm_cap(map))
	{
		errno = ENOSPC;
		return -1;
	}

	/* Let's knit this into place within the map. */
	sbm_insert_data(map,
					sep->target.offset + SBM_SIZEOF_OVERHEAD + sizeof(SbmBitvec),
					sep->buf + SBM_SIZEOF_OVERHEAD + sizeof(SbmBitvec),
					sep->expand_by);
	memcpy(sep->target.p, sep->buf,
		   sep->expand_by + SBM_SIZEOF_OVERHEAD + sizeof(SbmBitvec));
	sbm_set_chunk_count(map, sbm_get_chunk_count(map) + (sep->count - 1));

	return 0;
}

/* -------------------------------------------------------------------
 * Lifecycle: construction, copy, disposal, and buffer resize
 * -------------------------------------------------------------------
 */

/*
 * Clears the given sparse map.
 *
 * This function resets the sparse map by setting all its data to zero and updating
 * its metadata to reflect an empty map.
 *
 * `map` is the sparse map to clear.
 */
void
sbm_clear(Sbm *map)
{
	if (map == NULL)
	{
		return;
	}
	memset(map->m_data, 0, sbm_cap(map));
	map->m_data_used = SBM_SIZEOF_OVERHEAD;
	sbm_set_chunk_count(map, 0);
	sbm_card_invalidate(map);
}

/*
 * Allocates and initializes a sparse bitmap of the given size.
 *
 * This function creates a new sparse bitmap structure with allocated memory.
 * If the specified size is zero, a default size of 1024 is used. The function
 * ensures that the internal data array is 8-byte aligned and initializes the sparse bitmap
 * structure.
 *
 * `size` is the size of the sparse bitmap to allocate.
 * Returns a pointer to the allocated sparse bitmap structure, or NULL if allocation fails.
 */
Sbm *
sbm_create(size_t size)
{
	size_t		data_size;
	size_t		total_size;
	size_t		padding;
	Sbm		   *map;
	uint8	   *data;

	if (size == 0)
	{
		size = 1024;
	}

	/*
	 * Round up to an 8-byte boundary so the data region we allocate and the
	 * (low-bit-tagged) stored capacity agree exactly.
	 */
	size = (size + 7u) & ~(size_t) 7;

	data_size = size * sizeof(uint8);

	/* Ensure that m_data is 8-byte aligned. */
	total_size = sizeof(Sbm) + data_size;
	padding = total_size % 8 == 0 ? 0 : 8 - (total_size % 8);
	total_size += padding;

	map = (Sbm *) palloc0(total_size);
	data = (uint8 *) (((uintptr_t) map + sizeof(Sbm)) & ~(uintptr_t) 7);
	sbm_init(map, data, size);

	/*
	 * sbm_init tags the map as SBM_WRAPPED (caller-supplied buffer); override
	 * here because the buffer is contiguous with the struct and we own both.
	 */
	sbm_set_kind(map, SBM_OWNED_CONTIGUOUS);
	Assert(PointerIsAligned(map->m_data, uint64));
	return map;
}

/*
 * Disposes of a sparse bitmap, regardless of allocation lineage.
 *
 * SBM_OWNED_CONTIGUOUS  free(map) -- the struct and buffer share one block.
 * SBM_OWNED_SPLIT       free(map->m_data) + free(map).
 * SBM_WRAPPED           free(map) only -- the data buffer is the caller's
 *                      and is left untouched.
 *
 * Calling with NULL is a no-op.
 */
void
sbm_free(Sbm *map)
{
	if (map == NULL)
	{
		return;
	}
	switch (sbm_kind(map))
	{
		case SBM_OWNED_SPLIT:
			pfree(map->m_data);
			pg_fallthrough;
		case SBM_OWNED_CONTIGUOUS:
		case SBM_WRAPPED:
		default:
			pfree(map);
			break;
	}
}

/*
 * Returns a guaranteed-owned, guaranteed-growable copy of map.
 *
 * The result is always SBM_OWNED_CONTIGUOUS (one palloc chunk holding
 * struct and buffer).  Use this when you have a sparse bitmap whose
 * lineage you don't trust and need a self-contained copy that's safe to
 * grow and dispose with sbm_free() or pfree().
 */
Sbm *
sbm_owned_copy(const Sbm *map)
{
	size_t		cap;
	Sbm		   *out;

	if (map == NULL)
	{
		return NULL;
	}
	cap = sbm_get_capacity(map);
	out = sbm_create(cap);
	out->m_data_used = map->m_data_used;
	/* m_capacity is already cap; lineage is SBM_OWNED_CONTIGUOUS. */
	if (cap > 0 && map->m_data != NULL)
	{
		memcpy(out->m_data, map->m_data, cap);
	}
	return out;
}

/*
 * Creates a copy of the given sparse map.
 *
 * This function duplicates the provided sparse map, allocating a new sparse
 * map instance with the same capacity and copying over the used data.
 *
 * `other` is the sparse map to be copied.
 * Returns a pointer to the newly created sparse map that is a copy of the input,
 *         or NULL if the memory allocation fails.
 */
Sbm *
sbm_copy(const Sbm *other)
{
	size_t		cap;
	Sbm		   *map;

	if (other == NULL)
	{
		errno = EINVAL;
		return NULL;
	}
	cap = sbm_get_capacity(other);
	map = sbm_create(cap);
	sbm_set_cap_kind(map, cap, SBM_OWNED_CONTIGUOUS);
	map->m_data_used = other->m_data_used;
	memcpy(map->m_data, other->m_data, cap);
	return map;
}

/*
 * Anchors a given data array into a sparse bitmap structure.
 *
 * Allocates and initializes an Sbm structure that manages a provided data
 * array.  The sparse bitmap points at the data array and tracks its capacity;
 * the caller owns the buffer.
 *
 * Use sbm_anchor() (rather than sbm_create()) when the data buffer is
 * contiguous with, or embedded inside, a larger caller-owned object whose
 * address can move -- for example a Bitmapset node that holds the Sbm by
 * value followed by a flexible data[] array.  After the enclosing object is
 * relocated (repalloc), call sbm_reanchor() to repoint the map at the moved
 * buffer, and grow such a map only through the embedded entry points
 * (sbm_add_grow_embedded, sbm_union_into_embedded, ...) which repalloc the
 * enclosing object and reanchor for you.
 *
 * `data` is a pointer to the data array to be managed by the sparse bitmap;
 * `size` is the size of the data array.
 * Returns a pointer to the initialized Sbm structure, or NULL on failure.
 */
Sbm *
sbm_anchor(uint8 *data, const size_t size)
{
	/*
	 * Anchor allocates only the struct (caller owns the data buffer); route
	 * through the global allocator so sbm_free works correctly.
	 */
	Sbm		   *map = (Sbm *) palloc0(sizeof(Sbm));

	map->m_data = data;
	map->m_data_used = 0;
	sbm_set_cap_kind(map, size, SBM_WRAPPED);
	sbm_card_invalidate(map);
	return map;
}

/*
 * Repoint an anchored map at a (possibly moved) data buffer.
 *
 * When the object that an sbm_anchor()'d buffer is embedded in is relocated
 * by repalloc, the map's m_data still points at the old address.  This
 * repoints it at the new buffer, leaving capacity, used size and lineage
 * untouched.  It is the only sanctioned way to fix up m_data; callers never
 * touch the field directly.
 *
 * `map` is the anchored map; `data` is the buffer's new address.
 */
void
sbm_reanchor(Sbm *map, uint8 *data)
{
	Assert(map != NULL);
	map->m_data = data;
}

/*
 * Initializes a sparse bitmap with the provided data and size.
 *
 * This function sets up the initial state of a sparse bitmap by assigning the given
 * data buffer and capacity. It also clears the sparse bitmap to ensure it starts empty.
 *
 * `map` is a pointer to the sparse bitmap to initialize; `data` is a pointer
 * to the data buffer to be used by the sparse bitmap; `size` is the size of
 * the data buffer in bytes.
 */
void
sbm_init(Sbm *map, uint8 *data, const size_t size)
{
	if (map == NULL)
	{
		errno = EINVAL;
		return;
	}
	map->m_data = data;
	map->m_data_used = 0;
	sbm_set_cap_kind(map, size, SBM_WRAPPED);
	sbm_card_invalidate(map);

	/*
	 * Caller-allocated struct + caller-allocated buffer.  The buffer is not
	 * owned by the library; sbm_set_data_size will treat any grow as a
	 * wrap-style promotion (allocate fresh, copy, transition to
	 * SBM_OWNED_SPLIT).  sbm_create() overrides this to SBM_OWNED_CONTIGUOUS
	 * after calling us.
	 */
	sbm_clear(map);
}

/*
 * Initializes a sparse map with given data and size.
 *
 * This function sets up the sparse map by assigning the provided data array and
 * size, and calculates the initial data usage.
 *
 * `map` is the sparse map to initialize; `data` is pointer to the data array
 * to be used by the sparse map; `size` is the capacity of the data array.
 */
void
sbm_open(Sbm *map, uint8 *data, const size_t size)
{
	size_t		claimed_count;
	size_t		walked_count;

	if (map == NULL)
	{
		errno = EINVAL;
		return;
	}
	map->m_data = data;
	sbm_card_invalidate(map);

	/*
	 * Set m_capacity and a temporary m_data_used = capacity *before* calling
	 * sbm_get_size_impl.  sbm_get_size_impl walks chunks via
	 * sbm_get_chunk_count, which short-circuits to 0 when m_data_used <
	 * SBM_SIZEOF_OVERHEAD (the empty-map guard).  Without the temporary,
	 * opening a fully-populated buffer would read its chunk count as 0.
	 *
	 * sbm_open is for deserializing into a caller-supplied struct + buffer;
	 * lineage matches sbm_init (SBM_WRAPPED).
	 */
	sbm_set_cap_kind(map, size, SBM_WRAPPED);
	map->m_data_used = sbm_cap(map);

	/*
	 * Small-set body: the header word's top bit is set.  Its size is fixed by
	 * the word count; don't run the chunk walk on it.
	 */
	if (size >= SBM_SIZEOF_OVERHEAD && sbm_is_small(map))
	{
		const size_t nwords = sbm_small_nwords(map);

		map->m_data_used =
			SBM_SIZEOF_OVERHEAD + nwords * sizeof(uint64);
		if (map->m_data_used > sbm_cap(map) || !sbm_validate(map))
		{
			sbm_store_u64(&map->m_data[0], 0);
			map->m_data_used = SBM_SIZEOF_OVERHEAD;
		}
		return;
	}

	/*
	 * The stored count as the buffer claims it, before sbm_get_size_impl
	 * silently truncates it to the valid prefix.  A mismatch means the stored
	 * count disagrees with the walk, i.e. the buffer is corrupt.
	 */
	claimed_count = sbm_get_chunk_count(map);
	map->m_data_used = sbm_get_size_impl(map);
	walked_count = sbm_get_chunk_count(map);

	/*
	 * An untrusted buffer must be structurally valid or it is replaced with
	 * an empty (valid) map -- the same contract sbm_deserialize already
	 * enforces.  size 0 is the documented "leave it empty" call
	 * (sbm_init/sbm_anchor of a fresh buffer), so don't validate that.
	 */
	if (size >= SBM_SIZEOF_OVERHEAD &&
		(claimed_count != walked_count || !sbm_validate(map)))
	{
		sbm_store_u64(&map->m_data[0], 0);
		map->m_data_used = SBM_SIZEOF_OVERHEAD;
	}
}

Sbm *
sbm_open_copy(const uint8 *data, size_t n, size_t slack)
{
	size_t		cap;
	Sbm		   *m;

	if (data == NULL && n > 0)
		return NULL;

	/*
	 * sbm_create needs at least SBM_SIZEOF_OVERHEAD bytes; bump up if the
	 * caller asked for less.
	 */
	cap = n + slack;
	if (cap < SBM_SIZEOF_OVERHEAD)
		cap = SBM_SIZEOF_OVERHEAD;
	m = sbm_create(cap);
	if (n > 0)
	{
		size_t		claimed_count;
		size_t		walked_count;

		memcpy(sbm_get_data(m), data, n);
		/* Small-set body: fixed size, no chunk walk. */
		if (n >= SBM_SIZEOF_OVERHEAD && sbm_is_small(m))
		{
			const size_t nwords = sbm_small_nwords(m);

			m->m_data_used =
				SBM_SIZEOF_OVERHEAD + nwords * sizeof(uint64);
			if (m->m_data_used > n || !sbm_validate(m))
			{
				sbm_free(m);
				return NULL;
			}
			sbm_set_kind(m, SBM_OWNED_CONTIGUOUS);
			return m;
		}

		/*
		 * sbm_open re-derives m_data_used from the chunk count + walk;
		 * temporarily set m_data_used = capacity so the empty-map guard in
		 * sbm_get_chunk_count doesn't short-circuit during the walk.
		 */
		m->m_data_used = sbm_cap(m);
		claimed_count = sbm_get_chunk_count(m);
		m->m_data_used = sbm_get_size_impl(m);
		walked_count = sbm_get_chunk_count(m);

		/*
		 * Untrusted bytes: reject anything not structurally valid rather than
		 * returning a half-parsed map.
		 */
		if (claimed_count != walked_count || !sbm_validate(m))
		{
			sbm_free(m);
			return NULL;
		}
	}

	/*
	 * sbm_open's regular implementation transitions the lineage to
	 * SBM_WRAPPED -- but here the buffer is contiguous with the struct
	 * because we got it from sbm_create.  Restore the correct lineage so
	 * sbm_free does the right thing and so subsequent grows can use the
	 * single-block realloc path.
	 */
	sbm_set_kind(m, SBM_OWNED_CONTIGUOUS);
	return m;
}

/*
 * Resizes the data buffer of the sparse bitmap.
 *
 * Behaviour depends on the calling form and the map's allocation
 * lineage:
 *
 *   sbm_set_data_size(map, NULL, size)
 *     Library-managed grow / shrink.  Always succeeds (returning a
 *     possibly-relocated map pointer) or returns NULL on allocation
 *     failure.  Never silently no-ops the resize.
 *
 *       SBM_OWNED_CONTIGUOUS -- realloc the single struct+buffer block.
 *                             Caller must update all map references to
 *                             the returned pointer.
 *       SBM_OWNED_SPLIT      -- realloc m_data; map struct stays put.
 *       SBM_WRAPPED          -- if size <= m_capacity, simply update
 *                             m_capacity (caller's buffer is still
 *                             theirs).  If size > m_capacity, allocate
 *                             a fresh library-owned buffer of the
 *                             requested size, memcpy the m_data_used
 *                             prefix into it, redirect m_data, and
 *                             transition lineage to SBM_OWNED_SPLIT.
 *                             The caller's original buffer is left
 *                             untouched and remains theirs.
 *
 *   sbm_set_data_size(map, data, size)  [data != NULL]
 *     Re-point the map at a caller-supplied buffer.  m_capacity is
 *     updated; copying any existing bits is the caller's
 *     responsibility.  Lineage transitions to SBM_WRAPPED -- the library
 *     does not own the new buffer and will not realloc/free it on the
 *     caller's behalf.
 *
 * `map` is the sparse bitmap to resize.  Must be non-NULL; `data` is
 * optional caller-supplied buffer; NULL means "library decides"; `size` is
 * new buffer size in bytes.
 * Returns the (possibly relocated) sparse bitmap pointer on success,
 *         or NULL on allocation failure.
 */
Sbm *
sbm_set_data_size(Sbm *map, uint8 *data, const size_t size)
{
	size_t		asize;
	size_t		cur_cap;

	if (map == NULL)
	{
		return NULL;
	}

	/*
	 * A resize can change the contents (a re-point adopts the caller's bytes,
	 * and a shrink can truncate), so forget the cached cardinality.
	 */
	sbm_card_invalidate(map);

	/* Caller-driven re-point: trust them, transition to SBM_WRAPPED. */
	if (data != NULL)
	{
		map->m_data = data;
		sbm_set_cap_kind(map, size, SBM_WRAPPED);
		return map;
	}

	/*
	 * Library-managed resize.  Round the requested size up to an 8-byte
	 * boundary so the allocated buffer and the stored (low-bit- tagged)
	 * capacity agree exactly; sbm_set_cap_kind rounds down, so an
	 * already-aligned size round-trips unchanged.
	 */
	asize = (size + 7u) & ~(size_t) 7;
	cur_cap = sbm_cap(map);
	switch (sbm_kind(map))
	{
		case SBM_OWNED_CONTIGUOUS:
			{
				size_t		total_size;
				size_t		padding;
				const size_t old_capacity = cur_cap;
				Sbm		   *m;

				if (size == cur_cap)
				{
					return map;
				}

				/*
				 * Realloc the single block.  Allocate room for the struct +
				 * the new data buffer + alignment padding so m_data lands on
				 * an 8-byte boundary.
				 */
				total_size = sizeof(Sbm) + size;
				padding =
					total_size % 8 == 0 ? 0 : 8 - (total_size % 8);
				total_size += padding;

				m = (Sbm *) repalloc(map, total_size);
				m->m_data =
					(uint8 *) (((uintptr_t) m + sizeof(Sbm)) & ~(uintptr_t) 7);
				if (size > old_capacity)
				{
					/*
					 * Zero the newly-acquired tail so chunk metadata stays
					 * clean.
					 */
					memset(m->m_data + old_capacity, 0,
						   size - old_capacity);
				}
				sbm_set_cap_kind(m, size, SBM_OWNED_CONTIGUOUS);

				/*
				 * m_data_used does not change on grow; on shrink the caller
				 * is responsible for ensuring m_data_used <= size before
				 * calling.
				 */
				if (m->m_data_used > sbm_cap(m))
				{
					m->m_data_used = sbm_cap(m);
				}
				Assert(PointerIsAligned(m->m_data, uint64));
				return m;
			}

		case SBM_OWNED_SPLIT:
			{
				uint8	   *new_data;

				if (size == cur_cap)
				{
					return map;
				}
				new_data = (uint8 *) repalloc(map->m_data, size);
				if (size > cur_cap)
				{
					memset(new_data + cur_cap, 0, size - cur_cap);
				}
				map->m_data = new_data;
				sbm_set_cap_kind(map, size, SBM_OWNED_SPLIT);
				if (map->m_data_used > sbm_cap(map))
				{
					map->m_data_used = sbm_cap(map);
				}
				return map;
			}

		case SBM_WRAPPED:
			{
				uint8	   *new_data;
				size_t		copy_bytes;

				/*
				 * Caller owns m_data.  Two cases:
				 *
				 * size <= capacity (shrink or same): We do not own the
				 * buffer, so we cannot realloc/free it.  Just update the
				 * recorded capacity to "use no more than `size` bytes of the
				 * caller's buffer".  The caller's buffer is unchanged and
				 * remains theirs to free.
				 *
				 * size > capacity (grow): Allocate a fresh library-owned
				 * buffer of the requested size, copy the in-use prefix
				 * (m_data_used bytes), redirect m_data, transition lineage to
				 * SBM_OWNED_SPLIT.  The caller's original buffer is untouched
				 * and remains theirs.
				 */
				if (size <= cur_cap)
				{
					sbm_set_cap_kind(map, size, SBM_WRAPPED);
					if (map->m_data_used > sbm_cap(map))
					{
						map->m_data_used = sbm_cap(map);
					}
					return map;
				}

				new_data = (uint8 *) palloc0(asize);
				copy_bytes = map->m_data_used <= cur_cap ?
					map->m_data_used :
					cur_cap;
				if (copy_bytes > 0 && map->m_data != NULL)
				{
					memcpy(new_data, map->m_data, copy_bytes);
				}
				map->m_data = new_data;
				sbm_set_cap_kind(map, asize, SBM_OWNED_SPLIT);
				return map;
			}
	}

	/* Unreachable. */
	Assert(0 && "unknown sparse bitmap allocation lineage");
	return NULL;
}

/* -------------------------------------------------------------------
 * Small-set <-> chunk transitions
 * -------------------------------------------------------------------
 */

static SbmIdx sbm_map_set(Sbm *map, uint64 idx, bool coalesce,
						  SbmCursor *cur);
static void sbm_expand_sparse_chunk(const SbmChunk *chunk,
									SbmBitvec words[32], int cap_flags[32]);

/*
 * True when the bits of a small-mode map form one contiguous run from
 * bit 0, i.e. the set is exactly {0, 1, ..., maxbit}.  Such a run is the
 * one case the RLE build can encode as a single descriptor-only RLE
 * chunk (start 0, length maxbit+1) -- see sbm_small_chunk_bytes and
 * sbm_promote.  A run that does not start at bit 0 (or has any gap)
 * cannot be an RLE chunk and must use the sparse form.
 */
static bool
sbm_small_is_run_from_zero(const Sbm *map, uint64 *run_len_out)
{
	const uint64 *w = sbm_small_words(map);
	const size_t n = sbm_small_nwords(map);
	uint64		len = 0;
	bool		ended = false;
	size_t		i;

	for (i = 0; i < n; i++)
	{
		const uint64 lo = w[i];

		if (ended)
		{
			if (lo != 0)
			{
				return false;	/* bits after the run: a gap */
			}
			continue;
		}
		if (lo == ~(uint64) 0)
		{
			len += 64;
			continue;
		}
		if (lo == 0)
		{
			ended = true;		/* run ends on a whole-word boundary */
			continue;
		}
		/* Partial word: must be a low-contiguous mask (1<<r)-1. */
		if ((lo & (lo + 1)) != 0)
		{
			return false;		/* not a low-contiguous prefix */
		}
		len += (uint64) pg_popcount64(lo);
		ended = true;
	}
	if (run_len_out != NULL)
	{
		*run_len_out = len;
	}
	return len > 0;
}

/*
 * Byte footprint the single low chunk would occupy for the bits
 * currently held in a small-mode map.  The RLE build has THREE candidate
 * chunk-mode encodings for a set whose max index is below one chunk's
 * 2048-bit window; this returns the cheapest:
 *
 *   sparse chunk  = 8 (count) + 8 (start) + 8 (descriptor)
 *                 + 8 per 64-bit word that is neither all-zero (a ZEROS
 *                   slot, not stored) nor all-one (a ONES slot, not
 *                   stored) -- i.e. one payload word per MIXED word;
 *   RLE chunk     = 8 (count) + 8 (start) + 8 (descriptor) = 24, with no
 *                   payload, but ONLY when the bits form a single run
 *                   from bit 0 (an RLE chunk encodes [start, start+len)).
 *
 * A dense-but-not-word-aligned run such as {0..1000} is 32 bytes sparse
 * (word 15 is MIXED, so its payload word is stored) but 24 bytes as an
 * RLE chunk; the RLE-aware minimum makes sbm_promote pick the 24-byte
 * form.  A word-aligned all-ones run such as {0..1023} is already 24
 * bytes sparse (every word is a ONES slot), so RLE ties and either form
 * is chosen.  A sparse scatter such as {0,5,70} has no run to compress,
 * so only the sparse cost applies and the flat small form usually wins.
 */
static size_t
sbm_small_chunk_bytes(const Sbm *map)
{
	const uint64 *w = sbm_small_words(map);
	const size_t n = sbm_small_nwords(map);
	size_t		mixed = 0;
	uint64		run_len = 0;
	const size_t sparse_base = SBM_SIZEOF_OVERHEAD + SBM_SIZEOF_OVERHEAD +
		sizeof(SbmBitvec);
	size_t		sparse;
	size_t		i;

	for (i = 0; i < n; i++)
	{
		if (w[i] != 0 && w[i] != ~(uint64) 0)
		{
			mixed++;
		}
	}
	sparse = sparse_base + mixed * sizeof(SbmBitvec);
	if (sbm_small_is_run_from_zero(map, &run_len) &&
		run_len <= (uint64) SBM_CHUNK_RLE_MAX_LENGTH)
	{
		const size_t rle = SBM_SIZEOF_OVERHEAD + SBM_SIZEOF_OVERHEAD +
			sizeof(SbmBitvec);

		return rle < sparse ? rle : sparse;
	}
	return sparse;
}

/* Byte footprint of a small map's own stored form. */
static size_t
sbm_small_bytes(const Sbm *map)
{
	return SBM_SIZEOF_OVERHEAD + sbm_small_nwords(map) * sizeof(uint64);
}

/*
 * Should a set whose maximum index is `maxbit` and whose single low
 * chunk would occupy `chunk_bytes` be stored in small-set mode?  Yes
 * when the index span fits the small cap AND the small form is no
 * larger than the chunk form.  (chunk_bytes == 0 means "not computed";
 * fall back to the span test alone, used when there are no bits yet.)
 */
static inline bool
sbm_small_is_better(uint64 maxbit, size_t small_bytes, size_t chunk_bytes)
{
	if (maxbit >= SBM_SMALL_MAX_BITS)
	{
		return false;
	}
	return chunk_bytes == 0 || small_bytes <= chunk_bytes;
}

/*
 * Emit the single contiguous run {0..run_len-1} of a small-mode map as
 * one descriptor-only RLE chunk (start 0), replacing the small body in
 * place within the existing capacity.  Returns true on success; false
 * (leaving the map unchanged) if the caller must fall back to the
 * bit-by-bit sparse promote.  Caller guarantees the bits are a run from
 * bit 0 and run_len <= SBM_CHUNK_RLE_MAX_LENGTH.
 */
static bool
sbm_promote_run_as_rle(Sbm *map, uint64 run_len)
{
	/* 8 (count) + 8 (start) + 8 (RLE descriptor). */
	const size_t need = SBM_SIZEOF_OVERHEAD + SBM_SIZEOF_OVERHEAD +
		sizeof(SbmBitvec);
	const size_t capacity = SBM_CHUNK_MAX_CAPACITY;
	SbmChunk	chunk;

	if (need > sbm_cap(map))
	{
		return false;
	}

	/*
	 * Capacity is the enclosing 2048-bit chunk window; run_len <=
	 * SBM_SMALL_MAX_BITS (1024) < SBM_CHUNK_MAX_CAPACITY (2048).
	 */
	sbm_set_chunk_count(map, 1);
	sbm_store_idx(&map->m_data[SBM_SIZEOF_OVERHEAD], 0);	/* start = 0 */
	sbm_chunk_init(&chunk,
				   &map->m_data[SBM_SIZEOF_OVERHEAD + SBM_SIZEOF_OVERHEAD]);
	chunk.m_data[0] = 0;
	sbm_chunk_set_rle(&chunk);
	sbm_chunk_rle_set_capacity(&chunk, capacity);
	sbm_chunk_rle_set_length(&chunk, (size_t) run_len);
	map->m_data_used = need;
	return true;
}

/*
 * Convert a small-mode map to chunk mode in place, within the existing
 * buffer capacity.  Returns true on success; false (ENOSPC) if the
 * chunk form would not fit -- the caller (sbm_add) then reports ENOSPC
 * and the sbm_add_grow wrapper grows and retries.  The map is left
 * unchanged on failure.
 *
 * When the bits form a single run from bit 0 and the RLE form is the
 * cheapest chunk encoding, emit one descriptor-only RLE chunk directly
 * (matching the RLE-aware cost in sbm_small_chunk_bytes); otherwise
 * re-add every set bit into an empty chunk-mode map.
 */
static bool
sbm_promote(Sbm *map)
{
	/* Snapshot the bits; the buffer is reused for the chunk form. */
	const size_t n = sbm_small_nwords(map);
	uint64		words[SBM_SMALL_MAX_WORDS];
	uint64		run_len = 0;
	size_t		pi;

	Assert(sbm_is_small(map));
	Assert(n <= SBM_SMALL_MAX_WORDS);
	memcpy(words, sbm_small_words(map), n * sizeof(uint64));

	/*
	 * RLE-cheapest single run from bit 0: emit one RLE chunk directly,
	 * matching the RLE-aware minimum in sbm_small_chunk_bytes.  Only when the
	 * RLE form is no larger than the sparse form (a word-aligned all-ones run
	 * is 24 bytes either way; a partial-tail run is 24 RLE vs 32+ sparse).
	 */
	if (sbm_small_is_run_from_zero(map, &run_len) &&
		run_len <= (uint64) SBM_CHUNK_RLE_MAX_LENGTH)
	{
		/*
		 * Sparse cost of this run: one payload word only if the run ends
		 * mid-word (a MIXED tail); all whole words are ONES.
		 */
		const size_t sparse = SBM_SIZEOF_OVERHEAD + SBM_SIZEOF_OVERHEAD +
			sizeof(SbmBitvec) +
			((run_len % 64) != 0 ? sizeof(SbmBitvec) : 0);
		const size_t rle = SBM_SIZEOF_OVERHEAD + SBM_SIZEOF_OVERHEAD +
			sizeof(SbmBitvec);

		if (rle < sparse)
		{
			if (sbm_promote_run_as_rle(map, run_len))
			{
				return true;
			}

			/*
			 * Restore small header before the fallback path rebuilds from
			 * `words`.
			 */
			sbm_small_set_header(map, n);
			map->m_data_used =
				SBM_SIZEOF_OVERHEAD + n * sizeof(uint64);
		}
	}

	/*
	 * Reset to an empty chunk-mode map and re-add every set bit.  Each add
	 * stays within [0, SBM_SMALL_MAX_BITS) == one chunk, so the peak
	 * footprint is one chunk; if that exceeds capacity, add returns ENOSPC
	 * and we restore the small header.
	 */
	map->m_data_used = SBM_SIZEOF_OVERHEAD;
	sbm_set_chunk_count(map, 0);
	for (pi = 0; pi < n; pi++)
	{
		uint64		bits = words[pi];

		while (bits != 0)
		{
			const int	b = pg_rightmost_one_pos64(bits);
			const uint64 idx = (uint64) pi * 64 + (uint64) b;

			bits &= bits - 1;
			if (sbm_map_set(map, idx, true, NULL) == SBM_IDX_MAX)
			{
				/* Out of space: restore the small form. */
				map->m_data_used =
					SBM_SIZEOF_OVERHEAD + n * sizeof(uint64);
				sbm_small_set_header(map, n);
				memcpy(sbm_small_words(map), words,
					   n * sizeof(uint64));
				return false;
			}
		}
	}
	return true;
}

/*
 * If a chunk-mode map now holds only indices below SBM_SMALL_MAX_BITS and
 * the small form would be no larger, convert it to small mode in place.
 * Always fits (small form is smaller than the chunk form it replaces).
 * A no-op for maps that should stay in chunk mode.
 */
static void
sbm_try_demote(Sbm *map)
{
	size_t		count;
	uint8	   *p;
	SbmIdx		start;
	SbmChunk	chunk;
	SbmBitvec	w32[SBM_FLAGS_PER_INDEX];
	int			cap[SBM_FLAGS_PER_INDEX];
	size_t		hi_word = 0;
	bool		any = false;
	int			iw;
	uint64		maxbit;
	size_t		nwords;
	size_t		small_bytes;
	uint64		out[SBM_SMALL_MAX_WORDS];
	size_t		iw2;

	if (map == NULL || sbm_is_small(map) || map->m_data == NULL)
	{
		return;
	}
	if (map->m_data_used < SBM_SIZEOF_OVERHEAD)
	{
		return;
	}
	count = sbm_get_chunk_count(map);
	if (count == 0)
	{
		/*
		 * Empty: leave as an empty chunk-mode map (size == overhead, same as
		 * small with zero words).
		 */
		return;
	}
	if (count > 1)
	{
		return;					/* spans >1 chunk => max index >= 2048 >
								 * threshold */
	}

	/*
	 * Single chunk: it must start at 0 for the small form (from bit 0) to
	 * represent it, and every index must be < SBM_SMALL_MAX_BITS.
	 */
	p = sbm_get_chunk_data(map, 0);
	start = sbm_load_idx((const uint8 *) p);
	if (start != 0)
	{
		return;
	}
	sbm_chunk_init(&chunk, p + SBM_SIZEOF_OVERHEAD);

	/*
	 * An RLE chunk cannot be word-expanded by sbm_expand_sparse_chunk; decode
	 * it as the run {0..length-1} it encodes.  A demote target only exists
	 * when the whole set is below the small cap, so an RLE chunk here holds a
	 * run shorter than one chunk.
	 */
	if (sbm_chunk_is_rle(&chunk))
	{
		const size_t length = sbm_chunk_rle_get_length(&chunk);
		size_t		bit;

		if (length == 0 || length > SBM_SMALL_MAX_BITS)
		{
			return;
		}
		maxbit = (uint64) length - 1;
		nwords = (size_t) (maxbit / 64) + 1;
		small_bytes = SBM_SIZEOF_OVERHEAD +
			nwords * sizeof(uint64);
		if (small_bytes > map->m_data_used)
		{
			return;				/* chunk (RLE) form is smaller; keep it */
		}
		memset(out, 0, sizeof(out));
		for (bit = 0; bit < length; bit++)
		{
			out[bit / 64] |= (uint64) 1 << (bit % 64);
		}
		sbm_small_set_header(map, nwords);
		memcpy(sbm_small_words(map), out,
			   nwords * sizeof(uint64));
		map->m_data_used = small_bytes;
		return;
	}
	sbm_expand_sparse_chunk(&chunk, w32, cap);
	/* Highest set bit within the chunk. */
	for (iw = 0; iw < (int) SBM_FLAGS_PER_INDEX; iw++)
	{
		if (cap[iw] && w32[iw] != 0)
		{
			hi_word = (size_t) iw;
			any = true;
		}
	}
	if (!any)
	{
		/* Chunk with no set bits: collapse to empty chunk-mode. */
		map->m_data_used = SBM_SIZEOF_OVERHEAD;
		sbm_set_chunk_count(map, 0);
		return;
	}
	maxbit = (uint64) hi_word * 64 +
		(63 - (63 - pg_leftmost_one_pos64(w32[hi_word])));
	if (maxbit >= SBM_SMALL_MAX_BITS)
	{
		return;
	}
	nwords = (size_t) (maxbit / 64) + 1;
	small_bytes = SBM_SIZEOF_OVERHEAD +
		nwords * sizeof(uint64);
	if (small_bytes > map->m_data_used)
	{
		return;					/* chunk form is already smaller; keep it */
	}

	/*
	 * Build the small form from the expanded words.  small_bytes <=
	 * m_data_used <= capacity, so it always fits.
	 */
	memset(out, 0, sizeof(out));
	for (iw2 = 0; iw2 < nwords; iw2++)
	{
		out[iw2] = w32[iw2];
	}
	sbm_small_set_header(map, nwords);
	memcpy(sbm_small_words(map), out, nwords * sizeof(uint64));
	map->m_data_used = small_bytes;
}

/*
 * Materialize a small-mode map into a fresh chunk-mode map so the
 * chunk-walking read/algebra paths get a uniform view without mutating
 * the const input.  Returns NULL on allocation failure.
 */
static Sbm *
sbm_materialize(const Sbm *small)
{
	const uint64 *w = sbm_small_words(small);
	const size_t n = sbm_small_nwords(small);

	/*
	 * Every small-mode index is below SBM_SMALL_MAX_BITS, which fits in one
	 * chunk window, so the chunk form needs at most one descriptor plus a
	 * full payload.  Sizing the buffer for that up front means neither the
	 * per-bit adds nor the final promotion can run out of space.
	 */
	Sbm		   *m = sbm_create(SBM_SIZEOF_OVERHEAD * 2 +
							   sizeof(SbmBitvec) * (SBM_FLAGS_PER_INDEX + 1) +
							   SBM_SIZEOF_OVERHEAD +
							   SBM_SMALL_MAX_WORDS * sizeof(uint64));

	StaticAssertStmt(SBM_SMALL_MAX_BITS <= SBM_CHUNK_MAX_CAPACITY,
					 "small-set span must fit in one chunk");

	for (size_t i = 0; i < n; i++)
	{
		uint64		bits = w[i];

		while (bits != 0)
		{
			const uint64 idx = (uint64) i * 64 +
				(uint64) pg_rightmost_one_pos64(bits);
			uint64		rc PG_USED_FOR_ASSERTS_ONLY;

			bits &= bits - 1;
			rc = sbm_add(m, idx);
			Assert(rc == idx);
		}
	}

	/*
	 * sbm_add keeps a near-zero set in small mode; force chunk mode so the
	 * caller (the chunk-walking read/algebra paths) gets a real chunk stream
	 * and sbm_chunk_view does not recurse forever.
	 */
	if (sbm_is_small(m) && !sbm_small_is_empty(m))
	{
		bool		promoted PG_USED_FOR_ASSERTS_ONLY;

		promoted = sbm_promote(m);
		Assert(promoted);
	}
	return m;
}

/*
 * Return a chunk-mode view of `map` for the raw-chunk-walking read and
 * set-algebra paths.  If the map is already chunk mode (or NULL) it is
 * returned as-is and *owned is false.  If it is small, a materialized
 * chunk-mode copy is returned and *owned is set true; the caller must
 * sbm_free() it.
 */
static const Sbm *
sbm_chunk_view(const Sbm *map, bool *owned)
{
	Sbm		   *m;

	*owned = false;
	if (map == NULL || !sbm_is_small(map))
	{
		return map;
	}
	m = sbm_materialize(map);
	*owned = true;
	return m;
}

/*
 * Calculates the remaining capacity of the sparse bitmap.
 *
 * This function returns the percentage of unused capacity in the sparse map.
 * If the used capacity is equal to or exceeds the total capacity, it returns 0.
 * If the total capacity is 0, it returns 100. Otherwise, it returns the
 * percentage of capacity remaining.
 *
 * `map` is the sparse bitmap for which the remaining capacity is calculated.
 * Returns the percentage of remaining capacity in the sparse bitmap.
 */
double
sbm_capacity_remaining(const Sbm *map)
{
	size_t		cap;

	if (map == NULL)
		return 0.0;
	cap = sbm_cap(map);
	if (map->m_data_used >= cap)
	{
		return 0;
	}
	if (cap == 0)
	{
		return 100.0;
	}
	return (1.0 - ((double) map->m_data_used / (double) cap)) * 100.0;
}

/*
 * Retrieves the capacity of the sparse map.
 *
 * This function returns the total capacity of the given sparse map, which is
 * the size of the underlying data structure.
 *
 * `map` is pointer to the sparse map.
 * Returns the capacity of the sparse map.
 */
size_t
sbm_get_capacity(const Sbm *map)
{
	if (map == NULL)
		return 0;
	return sbm_cap(map);
}

/* -------------------------------------------------------------------
 * Single-bit operations: test, set, and clear
 * -------------------------------------------------------------------
 */

/*
 * Checks if a specific bit is set in the sparse map.
 *
 * This function determines whether the bit at the given index is set in the
 * sparse map. It performs various checks and traverses to the appropriate
 * chunk to verify the bit's state.
 *
 * `map` is the sparse map to check; `idx` is the index of the bit to check.
 * Returns true if the bit is set, false otherwise.
 */
pg_attribute_hot bool
sbm_contains(const Sbm *map, uint64 idx, SbmCursor *cur)
{
	ssize_t		offset;
	uint8	   *p;
	SbmIdx		start;
	SbmChunk	chunk;

	/*
	 * Defensive: NULL or empty maps contain nothing.  Accepting NULL is cheap
	 * insurance for consumers that pass the result of sbm_intersection /
	 * sbm_difference / sbm_xor unchecked, which legitimately return NULL when
	 * the result is empty.
	 */
	if (map == NULL)
	{
		return false;
	}
	if (sbm_is_small(map))
	{
		return sbm_small_contains(map, idx);
	}
	Assert(map->m_data_used >= SBM_SIZEOF_OVERHEAD);

	/* Get the SbmChunk which manages this index */
	offset = sbm_get_chunk_offset(map, idx, cur);

	/* No SbmChunk's available -> the bit is not set */
	if (offset == -1)
	{
		return false;
	}

	/* Otherwise load the SbmChunk */
	p = sbm_get_chunk_data(map, (size_t) offset);
	start = sbm_load_idx((const uint8 *) p);
	sbm_chunk_init(&chunk, p + SBM_SIZEOF_OVERHEAD);

	/*
	 * Determine if the bit is out of bounds of the SbmChunk; if yes then the
	 * bit is not set.
	 */
	if (idx < start ||
		(SbmIdx) idx - start >= sbm_chunk_get_capacity(&chunk))
	{
		return false;
	}

	/* Otherwise ask the SbmChunk whether the bit is set. */
	return sbm_chunk_is_set(&chunk, idx - start);
}

/*
 * Unsets a bit at a specified index in the given sparse map.
 *
 * This function clears the bit at the given index in the sparse map. It handles
 * different scenarios, including chunks that do not exist for the specified index,
 * run-length encoded (RLE) chunks, and sparse chunks.
 *
 * The function also optionally performs chunk coalescing if the `coalesce` flag is set.
 *
 * `map` is the sparse map in which the bit needs to be unset; `idx` is the
 * index of the bit to be unset; `coalesce` is a flag indicating whether to
 * perform chunk coalescing.
 * Returns the index of the bit that was unset.
 */
/*
 * Sentinel stored in the size_t byte-offset variable `offset` to gate chunk
 * coalescing off (the chunk was never located or its pointers are now stale).
 * It MUST be size_t-width: a uint64 sentinel (SBM_IDX_MAX == PG_UINT64_MAX)
 * truncates to 0xFFFFFFFF on ILP32 targets, so the `!=` gate test (which
 * promotes offset back to 64 bits) never matches and coalescing runs on an
 * uninitialized chunk -- a 32-bit-only crash.
 */
#define SBM_UNSET_NO_COALESCE ((size_t)-1)
static SbmIdx
sbm_map_unset(Sbm *map, uint64 idx, const bool coalesce)
{
	const uint64 ret_idx = idx;
	size_t		offset;
	size_t		chunk_offset;
	uint8	   *p = NULL;
	SbmIdx		start = 0;
	SbmChunk	chunk;
	size_t		capacity;
	size_t		pos = 0;
	SbmBitvec	vec = ~(SbmBitvec) 0;

	sbm_chunk_init(&chunk, NULL);
	Assert(map->m_data_used >= SBM_SIZEOF_OVERHEAD);

	/*
	 * Clearing a bit could require an additional vector, let's ensure we have
	 * that space available in the buffer first, or ENOMEM now.
	 */
	SBM_ENOUGH_SPACE(SBM_SIZEOF_OVERHEAD + sizeof(SbmBitvec));

	/* Determine if there is a chunk that could contain this index. */
	offset = (size_t) sbm_get_chunk_offset(map, idx, NULL);
	chunk_offset = offset;

	if ((ssize_t) offset == -1)
	{
		/*
		 * There are no chunks in the map, there is nothing to clear, this is
		 * a no-op.
		 */
		offset =
			SBM_UNSET_NO_COALESCE;	/* gate coalesce off; chunk is
									 * uninitialized */
		goto done;
	}

	/*
	 * Try to locate a chunk for this idx.  We could find that: - the first
	 * chunk's offset is greater than the index, or - the index is beyond the
	 * end of the last chunk, or - we found a chunk that can contain this
	 * index.
	 */
	p = sbm_get_chunk_data(map, offset);
	start = sbm_load_idx((const uint8 *) p);
	Assert(start == sbm_get_chunk_aligned_offset(start));

	if (idx < start)
	{
		/*
		 * Our search resulted in the first chunk that starts after the index
		 * but that means there is no chunk that contains this index, so again
		 * this is a no-op.
		 */
		offset =
			SBM_UNSET_NO_COALESCE;	/* gate coalesce off; chunk is
									 * uninitialized */
		goto done;
	}

	sbm_chunk_init(&chunk, p + SBM_SIZEOF_OVERHEAD);
	capacity = sbm_chunk_get_capacity(&chunk);

	if (idx - start >= capacity)
	{
		/*
		 * Our search resulted in a chunk however it's capacity doesn't
		 * encompass this index, so again a no-op.
		 */
		offset = SBM_UNSET_NO_COALESCE; /* gate coalesce off; chunk untouched */
		goto done;
	}

	if (sbm_chunk_is_rle(&chunk))
	{
		/*
		 * Our search resulted in a chunk that is run-length encoded (RLE).
		 * There are three possibilities at this point: 1) the index is at the
		 * end of the run, so we just shorten then length; 2) the index is
		 * between start and end [start, end) so we have to split this chunk
		 * up; 3) the index is beyond the length but within the capacity, then
		 * clearing it is a no-op. If the chunk length shrinks to the max
		 * capacity of sparse encoding we have to transition its encoding.
		 */

		/* Is the 0-based index beyond the run length? */
		const size_t length = sbm_chunk_rle_get_length(&chunk);
		SbmChunkSep sep;

		if (idx >= start + length)
		{
			goto done;
		}

		/* Is the 0-based index referencing the last bit in the run? */
		if (idx - start + 1 == length)
		{
			/*
			 * Removing the sole bit of a length-1 run empties the chunk.
			 * Setting the RLE length to 0 would leave a length-0 RLE
			 * descriptor behind, which every RLE reader (rank/select/scan)
			 * decodes as a FULL-CAPACITY run (2048 bits) -- so sbm_remove of
			 * the last bit would report a cardinality of 2048 instead of 0.
			 * Length-1 runs are produced legitimately (a shrinking run passes
			 * through length 1) as well as by crafted wire input, so remove
			 * the chunk outright, matching the sparse SBM_NEEDS_TO_SHRINK arm
			 * below.  An RLE chunk is exactly overhead + one descriptor word.
			 */
			if (length == 1)
			{
				sbm_remove_data(map, offset,
								SBM_SIZEOF_OVERHEAD + sizeof(SbmBitvec));
				sbm_set_chunk_count(map,
									sbm_get_chunk_count(map) - 1);
				offset = SBM_UNSET_NO_COALESCE; /* chunk gone */
				goto done;
			}
			/* Should the run-length chunk transition into a sparse chunk? */
			if (length - 1 == SBM_CHUNK_MAX_CAPACITY)
			{
				chunk.m_data[0] = ~(SbmBitvec) 0;
			}
			else
			{
				sbm_chunk_rle_set_length(&chunk, length - 1);
			}
			goto done;
		}

		/*
		 * Now that we've addressed (1) and (3) we have to work on (2) where
		 * the index is within the body of this RLE chunk. Chunks must have an
		 * aligned starting offset, so let's first find what we'll call the
		 * "pivot" chunk wherein we'll find the index we need to clear. That
		 * chunk will be sparse.
		 */
		memset(&sep, 0, sizeof(sep));
		sep.target.p = p;
		sep.target.offset = offset;
		sep.target.chunk = &chunk;
		sep.target.start = start;
		sep.target.length = length;
		sep.target.capacity = capacity;
		if (sbm_separate_rle_chunk(map, &sep, idx, 0) != 0)
		{
			/*
			 * Out of space (or invalid): the map was left unmodified.
			 * Propagate ENOSPC so sbm_add_grow / sbm_remove callers can grow
			 * and retry.
			 */
			return SBM_IDX_MAX;
		}
		/* Skip coalescing after RLE separation - the pointers are now invalid */
		offset = SBM_UNSET_NO_COALESCE;
		goto done;
	}

	switch (sbm_chunk_clr_bit(&chunk, idx - start, &pos))
	{
		case SBM_OK:
			break;
		case SBM_NEEDS_TO_GROW:
			SBM_ENOUGH_SPACE(sizeof(SbmBitvec));
			offset += SBM_SIZEOF_OVERHEAD + pos * sizeof(SbmBitvec);
			sbm_insert_data(map, offset, (uint8 *) &vec,
							sizeof(SbmBitvec));
			sbm_chunk_clr_bit(&chunk, idx - start, &pos);
			break;
		case SBM_NEEDS_TO_SHRINK:
			/* The vector is empty, perhaps the entire chunk is empty? */
			if (sbm_chunk_is_empty(&chunk))
			{
				sbm_remove_data(map, offset,
								SBM_SIZEOF_OVERHEAD + (sizeof(SbmBitvec) * 2));
				sbm_set_chunk_count(map,
									sbm_get_chunk_count(map) - 1);
			}
			else
			{
				offset +=
					SBM_SIZEOF_OVERHEAD + pos * sizeof(SbmBitvec);
				sbm_remove_data(map, offset, sizeof(SbmBitvec));
			}
			break;
		default:
			Assert(!"shouldn't be here");
			break;
	}

done:;
	if (coalesce && offset != SBM_UNSET_NO_COALESCE)
	{
		sbm_coalesce_chunk(map, &chunk, chunk_offset, start, p, idx,
						   false, SIZE_MAX);
	}
	return ret_idx;
}

/*
 * Unsets the value at a specific index in the sparse map.
 *
 * This function calls the internal sbm_map_unset function with the coalesce parameter
 * set to true, which removes an entry at the specified index and attempts to merge adjacent
 * segments to maintain the map's sparsity.
 *
 * `map` is the sparse map in which the value will be unset; `idx` is the
 * index at which the value will be unset.
 * Returns the index that was unset.
 */
pg_attribute_hot uint64
sbm_remove(Sbm *map, const uint64 idx)
{
	if (map == NULL)
	{
		errno = EINVAL;
		return SBM_IDX_MAX;
	}
	sbm_card_invalidate(map);
	if (sbm_is_small(map))
	{
		const size_t w = (size_t) (idx / 64);

		if (w < sbm_small_nwords(map))
		{
			size_t		n;
			uint64	   *words;

			sbm_small_words(map)[w] &=
				~((uint64) 1 << (idx % 64));

			/*
			 * Shrink the trailing all-zero words so the footprint tracks the
			 * new maximum index.
			 */
			n = sbm_small_nwords(map);
			words = sbm_small_words(map);
			while (n > 0 && words[n - 1] == 0)
			{
				n--;
			}
			sbm_small_set_header(map, n);
			map->m_data_used = SBM_SIZEOF_OVERHEAD +
				n * sizeof(uint64);
		}
		return idx;
	}
	{
		const uint64 rc = sbm_map_unset(map, idx, true);

		sbm_try_demote(map);
		return rc;
	}
}

/*
 * Sets a bit in a chunk within the sparse map and manages chunk resizing.
 *
 * This function sets a bit in the chunk of a sparse map corresponding to the
 * given index. It handles the initialization, setting the bit, and necessary
 * memory adjustments for growing or shrinking chunks, including allocation and
 * deallocation of bit vectors.
 *
 * `map` is the sparse map where the bit will be set; `idx` is the index
 * within the sparse map where the bit will be set; `p` is a pointer to the
 * chunk data within the sparse map; `offset` is the offset within the sparse
 * map's data where the chunk is located; `v` is a bit vector, when non-NULL,
 * indicates that a new chunk has been added.
 *
 * Returns the index at which the bit was set.
 */
static SbmIdx
sbm_insert_bit(Sbm *map, const uint64 idx, uint8 *p, size_t offset,
			   const void *v)
{
	/*
	 * When v is non-NULL we've just added a new chunk, and we knew in advance
	 * that a new chunk would result in an SBM_PAYLOAD_MIXED which in turn
	 * requires space to store the bit pattern, so given that we allocated the
	 * space ahead of time we don't need to allocate it now.
	 */
	size_t		pos = v ? (size_t) -1 : 0;
	SbmChunk	chunk;
	const SbmIdx start = sbm_load_idx((const uint8 *) p);

	sbm_chunk_init(&chunk, p + SBM_SIZEOF_OVERHEAD);
	Assert(sbm_chunk_is_rle(&chunk) == false);

	switch (sbm_chunk_set_bit(&chunk, idx - start, &pos))
	{
		case SBM_OK:
			break;
		case SBM_NEEDS_TO_GROW:
			if (!v)
			{
				SbmBitvec	vec = 0;

				SBM_ENOUGH_SPACE(sizeof(SbmBitvec));
				offset +=
					SBM_SIZEOF_OVERHEAD + pos * sizeof(SbmBitvec);
				sbm_insert_data(map, offset, (uint8 *) &vec,
								sizeof(SbmBitvec));
				pos = (size_t) -1;
			}
			sbm_chunk_set_bit(&chunk, idx - start, &pos);
			break;
		case SBM_NEEDS_TO_SHRINK:
			/* The vector is empty, perhaps the entire chunk is empty? */
			if (sbm_chunk_is_empty(&chunk))
			{
				sbm_remove_data(map, offset,
								SBM_SIZEOF_OVERHEAD + (sizeof(SbmBitvec) * 2));
				sbm_set_chunk_count(map,
									sbm_get_chunk_count(map) - 1);
			}
			else
			{
				offset +=
					SBM_SIZEOF_OVERHEAD + pos * sizeof(SbmBitvec);
				sbm_remove_data(map, offset, sizeof(SbmBitvec));
			}
			break;
		default:
			Assert(!"shouldn't be here");
			break;
	}

	return idx;
}

/*
 * Sets a bit in the sparse bit map.
 *
 * This function sets a bit at the given index in the provided sparse bit map.
 * It performs various internal checks and operations to ensure the data integrity of the map,
 * including initializing, inserting new chunks, and transitioning chunk states when necessary.
 *
 * `map` is the sparse bit map to be modified; `idx` is the index of the bit
 * to set; `coalesce` is a flag indicating whether to attempt chunk
 * coalescing.
 * Returns Returns the adjusted index within the sparse bit map or the given index.
 */
static SbmIdx
sbm_map_set(Sbm *map, uint64 idx, const bool coalesce, SbmCursor *cur)
{
	SbmChunk	chunk;
	uint64		ret_idx = idx;
	SbmIdx		start;
	uint8	   *p;
	size_t		offset;
	size_t		left_hint;
	size_t		capacity;

	Assert(map->m_data_used >= SBM_SIZEOF_OVERHEAD);

	/*
	 * Setting a bit could require an additional vector, let's ensure we have
	 * that space available in the buffer first, or ENOMEM now.
	 */
	SBM_ENOUGH_SPACE(SBM_SIZEOF_OVERHEAD + sizeof(SbmBitvec));

	/* Determine if there is a chunk that could contain this index. */
	offset = (size_t) sbm_get_chunk_offset(map, idx, cur);

	/*
	 * Free left-neighbor hint for the coalescing path: the forward walk above
	 * already passed over the chunk immediately before the located chunk and
	 * recorded its byte offset.  It stays valid ONLY while the located chunk
	 * keeps its position; every path below that inserts, separates, or shifts
	 * chunk layout at/before `offset` resets it to SIZE_MAX so a stale hint
	 * is never produced.  A SIZE_MAX hint just makes sbm_coalesce_chunk fall
	 * back to a head-walk.
	 */
	left_hint = (cur != NULL) ? cur->prev_offset : SIZE_MAX;

	if ((ssize_t) offset == -1)
	{
		/*
		 * No chunks exist, the map is empty, so we must append a new chunk to
		 * the end of the buffer and initialize it so that it can contain this
		 * index.
		 */
		const uint8 buf[SBM_SIZEOF_OVERHEAD +
						(sizeof(SbmBitvec) * 2)] = {0};
		const SbmBitvecUnaligned *v;

		/*
		 * Capacity was established by the SBM_ENOUGH_SPACE() above; a failure
		 * here would mean that check and this size disagree, so propagate
		 * ENOSPC rather than corrupt the buffer.
		 */
		if (unlikely(!sbm_append_data(map, &buf[0], sizeof(buf))))
		{
			return SBM_IDX_MAX;
		}
		p = sbm_get_chunk_data(map, 0);
		sbm_store_idx((uint8 *) p,
					  sbm_get_chunk_aligned_offset(idx));
		sbm_set_chunk_count(map, 1);

		v = (SbmBitvecUnaligned *) ((uintptr_t) p +
									SBM_SIZEOF_OVERHEAD + sizeof(SbmBitvec));
		ret_idx = sbm_insert_bit(map, idx, p, 0, v);

		sbm_chunk_init(&chunk, p + SBM_SIZEOF_OVERHEAD);
		start = sbm_load_idx((const uint8 *) p);
		offset = 0;
		left_hint = SIZE_MAX;	/* fresh append; no left neighbor */
		goto done;
	}

	/*
	 * Try to locate a chunk for this idx.  We could find that: - the first
	 * chunk's offset is greater than the index, or - the index is beyond the
	 * end of the last chunk, or - we found a chunk that can contain this
	 * index.
	 */
	p = sbm_get_chunk_data(map, offset);
	start = sbm_load_idx((const uint8 *) p);
	Assert(start == sbm_get_chunk_aligned_offset(start));

	if (idx < start)
	{
		/*
		 * Our search resulted in the first chunk, but it starts after the
		 * index, so that means there is no chunk that can contain this index.
		 * We need to insert a new chunk before this one and initialize it so
		 * that it can contain this index.
		 */
		const uint8 buf[SBM_SIZEOF_OVERHEAD +
						(sizeof(SbmBitvec) * 2)] = {0};
		const SbmBitvecUnaligned *v;

		SBM_ENOUGH_SPACE(sizeof(buf));
		sbm_insert_data(map, offset, &buf[0], sizeof(buf));
		sbm_set_chunk_count(map, sbm_get_chunk_count(map) + 1);

		/* NOTE: insert moves the memory over meaning `p` is now the new chunk */
		sbm_store_idx((uint8 *) p,
					  sbm_get_chunk_aligned_offset(idx));
		sbm_chunk_init(&chunk, p + SBM_SIZEOF_OVERHEAD);

		v = (SbmBitvecUnaligned *) ((uintptr_t) p +
									SBM_SIZEOF_OVERHEAD + sizeof(SbmBitvec));
		ret_idx = sbm_insert_bit(map, idx, p, offset, v);
		left_hint = SIZE_MAX;	/* inserted a chunk before this one */
		goto done;
	}

	sbm_chunk_init(&chunk, p + SBM_SIZEOF_OVERHEAD);
	capacity = sbm_chunk_get_capacity(&chunk);

	if (!sbm_chunk_is_rle(&chunk) &&
		capacity < SBM_CHUNK_MAX_CAPACITY &&
		idx - start < SBM_CHUNK_MAX_CAPACITY)
	{
		/*
		 * Special case, we have a sparse chunk with one or more flags set to
		 * SBM_PAYLOAD_NONE which reduces the carrying capacity of the chunk.
		 * In this case we should remove those flags and try again.
		 *
		 * The `!sbm_chunk_is_rle` guard is essential: an RLE chunk
		 * legitimately reports a capacity below SBM_CHUNK_MAX_CAPACITY
		 * (sbm_offset emits RLE chunks whose window-aligned capacity is one
		 * window or less), and sbm_chunk_increase_capacity is a sparse-only
		 * routine that would scribble over the RLE descriptor -- corrupting
		 * the chunk, failing sbm_validate, and later overreading in
		 * sbm_split/sbm_maximum.  RLE chunks belong to the RLE-set / separate
		 * path below, which handles a bit inside, at, or past the run
		 * correctly.
		 */
		Assert(sbm_chunk_is_rle(&chunk) == false);
		sbm_chunk_increase_capacity(&chunk, SBM_CHUNK_MAX_CAPACITY);
		capacity = sbm_chunk_get_capacity(&chunk);
	}

	if (chunk.m_data[0] == ~(SbmBitvec) 0 &&
		idx - start == SBM_CHUNK_MAX_CAPACITY)
	{
		/*
		 * Our search resulted in a chunk that is full of ones and this index
		 * is the next one after the capacity, we have a run of ones longer
		 * than the capacity of the sparse encoding, let's transition this
		 * chunk to run-length encoding (RLE).
		 *
		 * NOTE: Keep in mind that idx is 0-based, so idx=2048 is the 2049th
		 * bit. When a chunk is at maximum capacity it is storing indexes [0,
		 * 2048).
		 *
		 * ALSO: Keep in mind the RLE "length" is the current length of 1s in
		 * the run, so in this case we transition from 2048 to a length of
		 * 2049. in this run.
		 */

		const size_t rle_length = SBM_CHUNK_MAX_CAPACITY + 1;

		sbm_chunk_set_rle(&chunk);
		sbm_chunk_rle_set_capacity(&chunk,
								   sbm_chunk_rle_capacity_limit(map, start, rle_length,
																offset));
		sbm_chunk_rle_set_length(&chunk, rle_length);
		goto done;
	}

	/* is this an RLE chunk */
	if (sbm_chunk_is_rle(&chunk))
	{
		const size_t length = sbm_chunk_rle_get_length(&chunk);

		/*
		 * An RLE chunk may have a capacity below one window (sbm_offset and
		 * the emitter produce tight capacities).  A new chunk must start on
		 * the next window boundary, so an index past the capacity but inside
		 * this chunk's window has nowhere else to go: widen the capacity to
		 * the window (or to the next chunk, whichever is nearer) first.
		 */
		if (idx - start >= capacity &&
			sbm_get_chunk_aligned_offset(idx) == start)
		{
			capacity = sbm_chunk_rle_capacity_limit(map, start,
													idx - start + 1,
													offset);
			sbm_chunk_rle_set_capacity(&chunk, capacity);
		}

		/* Is the index within its range, at the end, or just past the end? */
		if (idx >= start && idx - start <= capacity)
		{
			/*
			 * This RLE contains the bits in [start, start + length] so the
			 * index of the last bit in this RLE chunk is `start + length - 1`
			 * which is why we test index (0-based) against current length
			 * (1-based) below.
			 */
			if (idx - start < length)
			{
				/* Bit is already set within the run, no-op. */
				goto done;
			}
			if (idx - start == length)
			{
				/*
				 * Extend the run by one. If length == capacity, grow capacity
				 * first.
				 */
				if (length == capacity)
				{
					sbm_chunk_rle_set_capacity(&chunk,
											   sbm_chunk_rle_capacity_limit(map,
																			start, length + 1, offset));
				}
				sbm_chunk_rle_set_length(&chunk, length + 1);
				Assert(sbm_chunk_rle_get_length(&chunk) ==
					   length + 1);
				goto done;
			}
		}

		/*
		 * We've been asked to set a bit that is within this RLE chunk's
		 * capacity but not within its run.  That means this chunk's capacity
		 * must shrink, and we need a new sparse chunk to hold this value.
		 *
		 * If the bit is beyond the capacity, fall through to the generic
		 * "insert new chunk" path below.
		 */
		if (idx >= start && idx - start < capacity)
		{
			SbmChunkSep sep;

			memset(&sep, 0, sizeof(sep));
			sep.target.p = p;
			sep.target.offset = offset;
			sep.target.chunk = &chunk;
			sep.target.start = start;
			sep.target.length = length;
			sep.target.capacity = capacity;
			if (sbm_separate_rle_chunk(map, &sep, idx, 1) != 0)
			{
				/*
				 * Out of space (or invalid): the map was left unmodified.
				 * Propagate ENOSPC so sbm_add_grow can grow and retry.
				 */
				return SBM_IDX_MAX;
			}
			left_hint = SIZE_MAX;	/* separate shifted layout */
			goto done;
		}
	}

	if (idx - start >= capacity)
	{
		/*
		 * Our search resulted in a chunk however it's capacity doesn't
		 * encompass this index, so we need to insert a new chunk after this
		 * one and initialize it so that it can contain this index.
		 */
		const uint8 buf[SBM_SIZEOF_OVERHEAD +
						(sizeof(SbmBitvec) * 2)] = {0};
		const size_t size = sbm_chunk_get_size(&chunk);
		const SbmBitvecUnaligned *v;

		SBM_ENOUGH_SPACE(sizeof(buf));
		offset += SBM_SIZEOF_OVERHEAD + size;
		p += SBM_SIZEOF_OVERHEAD + size;
		sbm_insert_data(map, offset, &buf[0], sizeof(buf));

		start = sbm_get_chunk_aligned_offset(idx);
		sbm_store_idx((uint8 *) p, start);
		Assert(start == sbm_get_chunk_aligned_offset(start));
		sbm_set_chunk_count(map, sbm_get_chunk_count(map) + 1);

		v = (SbmBitvecUnaligned *) ((uintptr_t) p +
									SBM_SIZEOF_OVERHEAD + sizeof(SbmBitvec));
		ret_idx = sbm_insert_bit(map, idx, p, offset, v);
		sbm_chunk_init(&chunk, p + SBM_SIZEOF_OVERHEAD);
		left_hint = SIZE_MAX;	/* inserted a new chunk after this one; hint
								 * pointed at the old chunk's predecessor,
								 * wrong for the new offset */
		goto done;
	}

	ret_idx = sbm_insert_bit(map, idx, p, offset, NULL);
	if (ret_idx != idx)
	{
		goto done;
	}

done:;
	if (coalesce)
	{
		sbm_coalesce_chunk(map, &chunk, offset, start, p, idx, true,
						   left_hint);
	}

	/*
	 * Re-seat the caller's cursor at the chunk we just touched so an
	 * ascending bulk insert (sbm_add_many / sbm_add_many_grow) resumes the
	 * next sbm_get_chunk_offset walk here instead of from the head -- the
	 * difference between O(N) and O(N^2) when a hot trigram accumulates tens
	 * of thousands of TIDs.  We record the byte offset and the chunk's start
	 * index; sbm_get_chunk_offset self-validates this (re-walking from the
	 * head if a later mutation shifted the chunk), so a stale seat is merely
	 * slow, never wrong.  Coalescing may have moved the chunk, so seat AFTER
	 * it and let the next call's validation sort out any drift.
	 */
	if (cur != NULL)
	{
		cur->offset = offset;
		cur->start_idx = start;
	}
	return ret_idx;
}

/*
 * Small-mode bit set.  Grows the word array within the existing buffer
 * capacity; returns SBM_IDX_MAX (errno=ENOSPC) if the buffer is too
 * small, so sbm_add_grow can grow and retry.  The map stays in small
 * mode.  Caller guarantees idx < SBM_SMALL_MAX_BITS.
 */
static uint64
sbm_small_add(Sbm *map, uint64 idx)
{
	const size_t w = (size_t) (idx / 64);
	size_t		n = sbm_small_nwords(map);

	if (w >= n)
	{
		const size_t need = SBM_SIZEOF_OVERHEAD +
			(w + 1) * sizeof(uint64);
		uint64	   *words;
		size_t		i;

		if (need > sbm_cap(map))
		{
			errno = ENOSPC;
			return SBM_IDX_MAX;
		}
		/* Zero the newly-exposed words. */
		words = sbm_small_words(map);
		for (i = n; i <= w; i++)
		{
			words[i] = 0;
		}
		n = w + 1;
		sbm_small_set_header(map, n);
		map->m_data_used = need;
	}
	sbm_small_words(map)[w] |= (uint64) 1 << (idx % 64);
	return idx;
}

/*
 * Unified add path handling both small and chunk mode.
 *
 *  - A small-mode map (or an empty chunk-mode map) whose new max index
 *    stays below SBM_SMALL_MAX_BITS stays/goes small.  If, after the add,
 *    the equivalent single chunk would be strictly smaller, promote --
 *    that always fits, since the chunk form is the smaller one.
 *  - Otherwise (index out of small range, or already a multi-chunk map)
 *    promote any small map to chunk form and use the chunk setter, then
 *    try to demote the result back to small.
 *
 * The cursor accelerates the chunk-mode ascending path only; on any
 * mode transition it is reset (layout changed) so a stale seat is never
 * used.
 */
static uint64
sbm_add_dispatch(Sbm *map, uint64 idx, SbmCursor *cur)
{
	const bool	small = sbm_is_small(map);
	const bool	empty_chunk = !small &&
		(map->m_data_used < SBM_SIZEOF_OVERHEAD ||
		 sbm_get_chunk_count(map) == 0);

	sbm_card_invalidate(map);

	if ((small || empty_chunk) && idx < SBM_SMALL_MAX_BITS)
	{
		uint64		rc;
		uint64		maxbit;

		if (empty_chunk)
		{
			/*
			 * Turn the empty chunk-mode buffer into an empty small-mode map
			 * (zero words).
			 */
			if (SBM_SIZEOF_OVERHEAD > sbm_cap(map))
			{
				errno = ENOSPC;
				return SBM_IDX_MAX;
			}
			sbm_small_set_header(map, 0);
			map->m_data_used = SBM_SIZEOF_OVERHEAD;
		}
		rc = sbm_small_add(map, idx);
		if (rc == SBM_IDX_MAX)
		{
			return SBM_IDX_MAX; /* ENOSPC: caller may grow */
		}
		if (cur != NULL)
		{
			sbm_cursor_reset(cur);
		}

		/*
		 * Keep the smaller of the two forms.  Promote only when the chunk
		 * form is strictly smaller (it then always fits).
		 */
		maxbit = sbm_small_maximum(map);
		if (!sbm_small_is_better(maxbit, sbm_small_bytes(map),
								 sbm_small_chunk_bytes(map)))
		{
			(void) sbm_promote(map);
		}
		return idx;
	}

	/* Chunk-mode path (promote first if the map is still small). */
	if (small)
	{
		if (!sbm_promote(map))
		{
			return SBM_IDX_MAX; /* ENOSPC: caller may grow */
		}
		if (cur != NULL)
		{
			sbm_cursor_reset(cur);
		}
	}
	{
		const uint64 rc = sbm_map_set(map, idx, true, cur);

		if (rc != SBM_IDX_MAX)
		{
			sbm_try_demote(map);
			if (cur != NULL && sbm_is_small(map))
			{
				sbm_cursor_reset(cur);
			}
		}
		return rc;
	}
}

/*
 * Sets the specified index in the sparse bitmap.
 *
 * This function marks the given index in the sparse bitmap as set.
 * Internally, it calls the sbm_map_set function with coalesce set to true.
 *
 * `map` is the sparse bitmap to modify; `idx` is the index to set in the
 * sparse bitmap.
 * Returns the index that was set in the sparse bitmap.
 */
pg_attribute_hot uint64
sbm_add(Sbm *map, const uint64 idx)
{
	if (map == NULL)
	{
		errno = EINVAL;
		return SBM_IDX_MAX;
	}
	return sbm_add_dispatch(map, idx, NULL);
}

/*
 * Cursor-threading variant of sbm_add for O(N) bulk construction.
 * Internal only; the cursor accelerates ascending inserts.  See
 * sbm_add_many / sbm_add_many_grow.
 *
 * Inserting a new chunk or coalescing existing ones changes the chunk
 * layout at or before the cached chunk, which would leave a recorded
 * cursor offset pointing at the wrong place (or past the end after a
 * coalesce removes trailing chunks).  Detect that by comparing the
 * chunk count before and after: on any change, reset the cursor so the
 * next lookup walks from the head.  Pure in-place updates (the common
 * case in an ascending run) leave the count unchanged and keep the
 * cursor hot, preserving the amortized O(N) build cost.
 */
static uint64
sbm_add_c(Sbm *map, uint64 idx, SbmCursor *cur)
{
	/*
	 * sbm_map_set re-seats *cur at the touched chunk (see its done: label),
	 * so we no longer reset the cursor here on a chunk-count change -- that
	 * blanket reset defeated the ascending-append fast path (every new chunk
	 * forced the next lookup back to the head, making bulk insert O(N^2)).
	 * sbm_get_chunk_offset self-validates the seat, so an occasionally-stale
	 * cursor is safe.
	 */
	return sbm_add_dispatch(map, idx, cur);
}

uint64
sbm_add_grow(Sbm **mapp, uint64 idx)
{
	Sbm		   *m;
	uint64		rc;
	size_t		new_cap;
	Sbm		   *grown;

	if (mapp == NULL)
		return SBM_IDX_MAX;
	if (*mapp == NULL)
		*mapp = sbm_create(0);
	m = *mapp;
	rc = sbm_add(m, idx);
	if (rc != SBM_IDX_MAX)
		return rc;

	/* ENOSPC: grow geometrically with a 4 KiB floor. */
	new_cap = sbm_get_capacity(m) * 2;
	if (new_cap < 4096)
		new_cap = 4096;
	grown = sbm_set_data_size(m, NULL, new_cap);
	*mapp = grown;
	return sbm_add(grown, idx);
}

uint64
sbm_add_grow_cursor(Sbm **mapp, uint64 idx, SbmCursor *cur)
{
	Sbm		   *m;
	uint64		rc;
	size_t		new_cap;
	Sbm		   *grown;

	if (mapp == NULL)
		return SBM_IDX_MAX;
	if (*mapp == NULL)
		*mapp = sbm_create(0);
	m = *mapp;
	rc = sbm_add_c(m, idx, cur);
	if (rc != SBM_IDX_MAX)
		return rc;

	/* ENOSPC: grow geometrically with a 4 KiB floor. */
	new_cap = sbm_get_capacity(m) * 2;
	if (new_cap < 4096)
		new_cap = 4096;
	grown = sbm_set_data_size(m, NULL, new_cap);
	*mapp = grown;
	/* The grow relocated the buffer; the cursor's byte offset is stale. */
	if (cur != NULL)
		sbm_cursor_reset(cur);
	return sbm_add_c(grown, idx, cur);
}

/*
 * Sets or unsets a value in the sparse map at the specified index.
 *
 * This function assigns a value to the sparse map at the given index.
 * It either sets or unsets (clears) the bit at the index based on
 * the provided boolean value.
 *
 * `map` is pointer to the sparse bitmap structure; `idx` is the index at
 * which the value should be assigned; `value` is boolean value indicating
 * whether to set (true) or unset (false) the bit.
 * Returns the index at which the operation was performed.
 */
uint64
sbm_assign(Sbm *map, const uint64 idx, const bool value)
{
	if (map == NULL)
	{
		errno = EINVAL;
		return SBM_IDX_MAX;
	}
	sbm_check_invariants(map);
	return value ? sbm_add(map, idx) : sbm_remove(map, idx);
}

/* -------------------------------------------------------------------
 * Aggregate queries: minimum, maximum, fill factor, cardinality
 * -------------------------------------------------------------------
 */

/*
 * Retrieves the starting offset in a sparse map.
 *
 * This function determines the starting offset of a sparse map by analyzing
 * the chunks within the map. It iterates over the chunk data to find the first
 * payload of interest, either `ones` or `mixed`, and returns the corresponding
 * offset. If the chunk is run-length encoded (RLE), it shortcuts to this calculation.
 *
 * `map` is pointer to the sparse map to analyze.
 * Returns the starting offset within the sparse map.
 */
uint64
sbm_minimum(const Sbm *map)
{
	uint64		offset = 0;
	size_t		count;
	uint8	   *p;
	uint64		relative_position;
	SbmChunk	chunk;
	size_t		m;

	if (map == NULL)
		return 0;
	if (sbm_is_small(map))
		return sbm_small_minimum(map);
	sbm_check_invariants(map);
	count = sbm_get_chunk_count(map);
	if (count == 0)
	{
		return 0;
	}
	p = sbm_get_chunk_data(map, 0);
	relative_position = sbm_load_idx((const uint8 *) p);
	p += SBM_SIZEOF_OVERHEAD;
	sbm_chunk_init(&chunk, p);
	if (sbm_chunk_is_rle(&chunk))
	{
		offset = relative_position;
		goto done;
	}
	for (m = 0; m < sizeof(SbmBitvec); m++)
	{
		const uint8 fb = sbm_desc_flag_byte(*chunk.m_data, m);
		int			n;

		for (n = 0; n < SBM_FLAGS_PER_INDEX_BYTE; n++)
		{
			const size_t flags = SBM_CHUNK_GET_FLAGS(fb, n);

			if (flags == SBM_PAYLOAD_NONE)
			{
				/*
				 * A NONE slot carries no payload, but it still occupies its
				 * index slot: sbm_chunk_is_set locates a bit positionally as
				 * flags[idx / 64], so bit 64 lives in flag 1 whether or not
				 * flag 0 is NONE.  Skipping without advancing made this
				 * report a position 64 bits too low per leading NONE slot,
				 * contradicting sbm_contains and sbm_next_member on the same
				 * map.
				 */
				relative_position += SBM_BITS_PER_VECTOR;
				continue;
			}
			else if (flags == SBM_PAYLOAD_ZEROS)
			{
				relative_position += SBM_BITS_PER_VECTOR;
			}
			else if (flags == SBM_PAYLOAD_ONES)
			{
				offset = relative_position;
				goto done;
			}
			else if (flags == SBM_PAYLOAD_MIXED)
			{
				const SbmBitvec w = chunk.m_data[1 +
												 sbm_chunk_get_position(&chunk,
																		(m * SBM_FLAGS_PER_INDEX_BYTE) +
																		(size_t) n)];
				int			k;

				for (k = 0; k < SBM_BITS_PER_VECTOR; k++)
				{
					if (w & (SbmBitvec) 1 << k)
					{
						offset = relative_position +
							(uint64) k;
						goto done;
					}
				}
				relative_position += SBM_BITS_PER_VECTOR;
			}
		}
	}
done:;
	return offset;
}

/*
 * Retrieves the ending offset of a sparse map.
 *
 * This function calculates the ending offset of a sparse map by examining
 * each chunk within the map. If the map is empty, the offset is zero. For
 * maps with chunks, it iterates over the chunks, evaluating their data and
 * calculating the final offset.
 *
 * `map` is pointer to the sparse map structure.
 * Returns the calculated ending offset of the map.
 */
uint64
sbm_maximum(const Sbm *map)
{
	size_t		count;
	uint8	   *p;
	SbmIdx		start;
	SbmChunk	chunk;
	uint64		offset = 0;
	uint64		relative_position;
	size_t		i;
	size_t		m;

	if (map == NULL)
		return 0;
	if (sbm_is_small(map))
		return sbm_small_maximum(map);
	sbm_check_invariants(map);
	count = sbm_get_chunk_count(map);

	/* the ending offset of a map containing zero chunks is zero */
	if (count == 0)
	{
		return 0;
	}

	/* the ending offset will be the last offset in the last chunk */
	p = sbm_get_chunk_data(map, 0);
	for (i = 0; i < count - 1; i++)
	{
		p += SBM_SIZEOF_OVERHEAD;
		sbm_chunk_init(&chunk, p);
		p += sbm_chunk_get_size(&chunk);
	}

	/* examine the last chunk in the map */
	start = sbm_load_idx((const uint8 *) p);
	p += SBM_SIZEOF_OVERHEAD;
	sbm_chunk_init(&chunk, p);

	/* the ending offset of an RLE chunk is its starting offset + length */
	if (SBM_IS_CHUNK_RLE(&chunk))
	{
		return start + sbm_chunk_rle_get_length(&chunk) - 1;
	}

	/* the last chunk is not RLE, let's examine it further */
	relative_position = start;
	for (m = 0; m < sizeof(SbmBitvec); m++)
	{
		const uint8 fb = sbm_desc_flag_byte(*chunk.m_data, m);
		int			n;

		for (n = 0; n < SBM_FLAGS_PER_INDEX_BYTE; n++)
		{
			const size_t flags = SBM_CHUNK_GET_FLAGS(fb, n);

			switch (flags)
			{
				case SBM_PAYLOAD_ZEROS:
					relative_position += SBM_BITS_PER_VECTOR;
					break;
				case SBM_PAYLOAD_ONES:
					offset =
						relative_position + SBM_BITS_PER_VECTOR - 1;
					relative_position += SBM_BITS_PER_VECTOR;
					break;
				case SBM_PAYLOAD_MIXED:
					{
						const SbmBitvec w = chunk.m_data[1 +
														 sbm_chunk_get_position(&chunk,
																				(m * SBM_FLAGS_PER_INDEX_BYTE) +
																				(size_t) n)];
						int			idx = 0;
						int			k;

						for (k = 0; k < SBM_BITS_PER_VECTOR; k++)
						{
							if (w & (SbmBitvec) 1 << k)
							{
								idx = k;
							}
						}
						offset = relative_position + (uint64) idx;
						relative_position += SBM_BITS_PER_VECTOR;
						break;
					}
				case SBM_PAYLOAD_NONE:

					/*
					 * Occupies its index slot without carrying a payload;
					 * advance so positions stay in step with
					 * sbm_chunk_is_set's flags[idx / 64] addressing (see
					 * sbm_minimum).
					 */
					relative_position += SBM_BITS_PER_VECTOR;
					continue;
				default:
					continue;
			}
		}
	}
	return offset;
}

/*
 * Calculates the fill factor of a sparse map.
 *
 * This function computes the fill factor of a sparse map by determining
 * the proportion of occupied elements relative to its total offset.
 * The fill factor is expressed as a percentage.
 *
 * `map` is a pointer to the sparse map.
 * Returns the fill factor of the map as a percentage.
 */
double
sbm_fill_factor(const Sbm *map)
{
	size_t		rank;
	uint64		lo;
	uint64		hi;
	uint64		range;

	if (map == NULL)
		return 0.0;
	sbm_check_invariants(map);
	rank = sbm_rank(map, 0, SBM_IDX_MAX, true);
	if (rank == 0)
	{
		return 0.0;
	}
	lo = sbm_minimum(map);
	hi = sbm_maximum(map);
	/* range = hi - lo + 1 (the inclusive span containing all set bits). */
	range = hi - lo + 1;
	if (range == 0)
	{
		return 0.0;
	}
	return (double) rank / (double) range;
}

/*
 * Retrieves the serialized bitmap data from a sparse map.
 *
 * This function returns a pointer to the serialized data contained within
 * a given sparse map.
 *
 * `map` is pointer to the sparse map from which to retrieve the data.
 * Returns pointer to the serialized bitmap data.
 */
void *
sbm_get_data(const Sbm *map)
{
	if (map == NULL)
		return NULL;
	return map->m_data;
}

/*
 * Retrieves the size of the sparse map.
 *
 * This function calculates the utilized size of the sparse map. If the stored
 * size does not match the calculated size, it updates the stored size.
 *
 * `map` is pointer to the sparse map.
 * Returns the size of the sparse map.
 */
size_t
sbm_get_size(const Sbm *map)
{
	if (map == NULL)
		return 0;

	/*
	 * Small-set mode: the stored m_data_used is authoritative; the
	 * chunk-walking size recompute must not run on a small body.
	 */
	if (sbm_is_small(map))
		return map->m_data_used;

	/*
	 * m_data_used is a cache of the chunk walk; refreshing it does not change
	 * the set's contents, so it is safe to do on a logically-const map.
	 */
	unconstify(Sbm *, map)->m_data_used = sbm_get_size_impl(map);
	return map->m_data_used;
}

/*
 * Counts the number of elements in a sparse map.
 *
 * This function returns the total count of elements stored in a given
 * Sbm instance by invoking the sbm_rank function.
 *
 * `map` is a pointer to the Sbm instance to be counted.
 * Returns the total number of elements in the sparse map.
 */
size_t
sbm_cardinality(const Sbm *map)
{
	size_t		count;

	if (map == NULL)
		return 0;
	if (sbm_card_is_valid(map))
		return sbm_card_get(map);
	if (sbm_is_small(map))
		count = (size_t) sbm_small_cardinality(map);
	else
		count = sbm_rank(map, 0, SBM_IDX_MAX, true);

	/* The count is a pure function of m_data; caching it is logically const. */
	sbm_card_store(unconstify(Sbm *, map), count);
	return count;
}

/* -------------------------------------------------------------------
 * Iteration: batched callback scan of set bits
 * -------------------------------------------------------------------
 */

/*
 * Scans through each chunk in a sparse map and applies a scanning function to each chunk.
 *
 * This function iterates over all chunks in the provided sparse map, initializing each chunk
 * and applying a user-defined scanning function to it. The scan may optionally skip a specified
 * number of elements before commencing.
 *
 * `map` is pointer to the sparse map to scan; `scanner` is user-defined
 * scanning function to be applied to each chunk; `skip` is number of
 * elements to skip before starting the scan; `aux` is auxiliary data to pass
 * to the scanning function.
 */
void
sbm_scan(const Sbm *map, void (*scanner) (uint64[], size_t, void *aux),
		 size_t skip, void *aux)
{
	uint8	   *p;
	size_t		count;
	size_t		si;

	if (map == NULL)
		return;
	if (sbm_is_small(map))
	{
		Sbm		   *m = sbm_materialize(map);

		sbm_scan(m, scanner, skip, aux);
		sbm_free(m);
		return;
	}
	p = sbm_get_chunk_data(map, 0);
	count = sbm_get_chunk_count(map);

	for (si = 0; si < count; si++)
	{
		const SbmIdx start = sbm_load_idx((const uint8 *) p);
		SbmChunk	chunk;
		size_t		chunk_size;
		size_t		skipped;

		p += SBM_SIZEOF_OVERHEAD;
		sbm_chunk_init(&chunk, p);
		chunk_size = sbm_chunk_get_size(&chunk);
		if (si + 1 < count)
		{
			pg_prefetch(p + chunk_size + SBM_SIZEOF_OVERHEAD);
		}
		skipped =
			sbm_chunk_scan(&chunk, start, scanner, skip, aux);
		if (skip)
		{
			Assert(skip >= skipped);
			skip -= skipped;
		}
		p += chunk_size;
	}
}

/*
 * Creates a new sparse bitmap with all bits shifted by a given offset.
 *
 * Every set bit at position i in the source map appears at position i + offset
 * in the result. Bits shifted below 0 are silently dropped.
 *
 * Uses direct chunk copying and bit-vector shifting for performance.
 *
 * `map` is the source sparse bitmap; `offset` is signed shift amount
 * (positive = right, negative = left).
 * Returns a newly allocated sparse bitmap (caller must free()), or NULL if all
 *         bits are shifted away or on allocation failure.
 */

/* -------------------------------------------------------------------
 * Set operations: scratch-word codec, append helpers, and the
 * bitwise shift (sbm_offset)
 * -------------------------------------------------------------------
 */

/*
 * Expand a sparse chunk's descriptor into 32 full 64-bit words.
 *
 * For each of the 32 descriptor flag slots:
 *   ZEROS -> 0x0000000000000000
 *   ONES  -> 0xFFFFFFFFFFFFFFFF
 *   MIXED -> the stored bit-vector word
 *   NONE  -> 0x0000000000000000 (treated as zeros for shifting)
 *
 * `chunk` is the sparse chunk to expand; `words` is array of 32 uint64 to
 * receive expanded words; `cap_flags` is array of 32 flags: 1 if slot
 * contributes to capacity, 0 if NONE.
 */
static void
sbm_expand_sparse_chunk(const SbmChunk *chunk, SbmBitvec words[32],
						int cap_flags[32])
{
	const SbmBitvec desc = chunk->m_data[0];

	/*
	 * Pass 1: prefix-sum of MIXED flag counts to break serial vec_idx
	 * dependency.
	 */
	int			vec_offsets[SBM_FLAGS_PER_INDEX];
	int			running = 0;
	int			i;

	for (i = 0; i < (int) SBM_FLAGS_PER_INDEX; i++)
	{
		vec_offsets[i] = running;
		running +=
			(((desc >> (i * 2)) & SBM_FLAG_MASK) == SBM_PAYLOAD_MIXED);
	}

	/* Pass 2: each slot computed independently using precomputed offsets. */
	for (i = 0; i < (int) SBM_FLAGS_PER_INDEX; i++)
	{
		const unsigned f = (desc >> (i * 2)) & SBM_FLAG_MASK;

		cap_flags[i] = (f != SBM_PAYLOAD_NONE);
		words[i] = (f == SBM_PAYLOAD_MIXED) ?
			chunk->m_data[1 + vec_offsets[i]] :
			(f == SBM_PAYLOAD_ONES) ? ~(SbmBitvec) 0 :
			0;
	}
}

/*
 * Encode 32 expanded words back into a sparse chunk format.
 *
 * Builds a descriptor and vector array from the expanded words.
 * Only slots where cap_flags[i] == 1 contribute to capacity.
 *
 * `words` is array of 32 uint64 words; `cap_flags` is array of 32 flags
 * indicating capacity slots; `out_desc` is the output descriptor word;
 * `out_vecs` is output vector array (up to 32 words); `out_nvecs` is number
 * of output vectors written.
 * Returns true if the chunk has any set bits, false if completely empty.
 */
static bool
sbm_encode_sparse_chunk(SbmBitvec words[32], int cap_flags[32],
						SbmBitvec *out_desc, SbmBitvec out_vecs[32], int *out_nvecs)
{
	SbmBitvec	desc = 0;
	bool		has_bits = false;
	unsigned	flags[SBM_FLAGS_PER_INDEX];
	int			i;
	int			nvecs = 0;
	int			mi;

	/*
	 * Slot 31 (the highest) must never be NONE, because NONE in bits 63:62 of
	 * the descriptor would be misidentified as the RLE flag.  Force it to
	 * ZEROS (adding 64 bits of harmless zero capacity) when needed.
	 */
	if (!cap_flags[SBM_FLAGS_PER_INDEX - 1])
	{
		cap_flags[SBM_FLAGS_PER_INDEX - 1] = 1;
		words[SBM_FLAGS_PER_INDEX - 1] = 0;
	}

	/* Pass 1: compute flags for each slot (no inter-iteration dependency). */
	for (i = 0; i < (int) SBM_FLAGS_PER_INDEX; i++)
	{
		unsigned	f;

		if (!cap_flags[i])
		{
			f = SBM_PAYLOAD_NONE;
		}
		else if (words[i] == 0)
		{
			f = SBM_PAYLOAD_ZEROS;
		}
		else if (words[i] == ~(SbmBitvec) 0)
		{
			f = SBM_PAYLOAD_ONES;
			has_bits = true;
		}
		else
		{
			f = SBM_PAYLOAD_MIXED;
			has_bits = true;
		}
		flags[i] = f;
		desc |= (SbmBitvec) f << (i * 2);
	}

	/* Pass 2: compact MIXED vectors (serial but only touches MIXED slots). */
	for (mi = 0; mi < (int) SBM_FLAGS_PER_INDEX; mi++)
	{
		if (flags[mi] == SBM_PAYLOAD_MIXED)
		{
			out_vecs[nvecs++] = words[mi];
		}
	}

	*out_desc = desc;
	*out_nvecs = nvecs;
	return has_bits;
}

/*
 * Expand an RLE chunk's set bits into a 32-word array aligned at a
 *        target sparse chunk's start offset.
 *
 * For each of the 32 word slots at target_start + i*64:
 *   - If entirely within the RLE run -> words[i] = ~0ULL
 *   - If entirely outside -> words[i] = 0
 *   - If at boundary -> words[i] = partial bit mask
 *   - cap_flags[i] = 1 for slots within the target's capacity range
 *
 * `rle_chunk` is the RLE chunk; `rle_start` is the absolute start offset of
 * the RLE chunk; `target_start` is the aligned start offset of the target
 * sparse chunk; `words` is array of 32 uint64 words; `cap_flags` is array of
 * 32 capacity flags; `target_cap_flags` is if non-NULL, use these to
 * determine which slots have capacity (from the target sparse chunk). If
 * NULL, all 32 slots are considered to have capacity.
 */
static void
sbm_expand_rle_as_words(const SbmChunk *rle_chunk, SbmIdx rle_start,
						SbmIdx target_start, SbmBitvec words[32], int cap_flags[32],
						const int *target_cap_flags)
{
	const size_t rle_len = sbm_chunk_rle_get_length(rle_chunk);
	const size_t rle_set_start = (size_t) rle_start;
	const size_t rle_set_end = rle_set_start + rle_len;
	int			i;

	for (i = 0; i < (int) SBM_FLAGS_PER_INDEX; i++)
	{
		const size_t slot_start =
			(size_t) target_start + (size_t) i * SBM_BITS_PER_VECTOR;
		const size_t slot_end = slot_start + SBM_BITS_PER_VECTOR;

		if (target_cap_flags)
		{
			cap_flags[i] = target_cap_flags[i];
		}
		else
		{
			cap_flags[i] = 1;
		}

		if (slot_end <= rle_set_start || slot_start >= rle_set_end)
		{
			/* Slot entirely outside the RLE run */
			words[i] = 0;
		}
		else if (slot_start >= rle_set_start &&
				 slot_end <= rle_set_end)
		{
			/* Slot entirely within the RLE run */
			words[i] = ~(SbmBitvec) 0;
		}
		else
		{
			/* Boundary slot: partial overlap */
			SbmBitvec	mask = 0;
			size_t		lo = (rle_set_start > slot_start) ?
				(rle_set_start - slot_start) :
				0;
			size_t		hi = (rle_set_end < slot_end) ?
				(rle_set_end - slot_start) :
				SBM_BITS_PER_VECTOR;

			if (hi == SBM_BITS_PER_VECTOR)
			{
				mask = ~((SbmBitvec) 0) << lo;
			}
			else if (lo == 0)
			{
				mask = ((SbmBitvec) 1 << hi) - 1;
			}
			else
			{
				mask = (((SbmBitvec) 1 << hi) - 1) &
					(~((SbmBitvec) 0) << lo);
			}
			words[i] = mask;
		}
	}
}


/*
 * Word-level set operations over a 32-word (256-byte) chunk buffer.
 *
 * union/intersection/difference of two decoded chunks reduce to a bitwise OR,
 * AND, and AND-NOT over a fixed 32 x uint64 (256-byte) buffer.  This is a hot
 * path for the set-algebra operations, so it is vectorized with the portable
 * SIMD layer in port/simd.h, which compiles to SSE2 on x86-64 and Neon on
 * aarch64 -- both baseline ISAs, so no runtime dispatch is needed -- and to a
 * scalar uint64 loop elsewhere (USE_NO_SIMD), including RISC-V, which the
 * compiler is free to auto-vectorize.  A Vector8 holds 16 bytes, so the
 * buffer is 16 vector iterations.
 */
#define SBM_WORDS_BYTES		(SBM_FLAGS_PER_INDEX * sizeof(SbmBitvec))

static inline void
sbm_words_or(SbmBitvec dst[SBM_FLAGS_PER_INDEX],
			 const SbmBitvec a[SBM_FLAGS_PER_INDEX],
			 const SbmBitvec b[SBM_FLAGS_PER_INDEX])
{
#ifndef USE_NO_SIMD
	for (size_t off = 0; off < SBM_WORDS_BYTES; off += sizeof(Vector8))
	{
		Vector8		va;
		Vector8		vb;

		vector8_load(&va, (const uint8 *) a + off);
		vector8_load(&vb, (const uint8 *) b + off);
		vector8_store((uint8 *) dst + off, vector8_or(va, vb));
	}
#else
	for (int i = 0; i < SBM_FLAGS_PER_INDEX; i++)
		dst[i] = a[i] | b[i];
#endif
}

static inline void
sbm_words_and(SbmBitvec dst[SBM_FLAGS_PER_INDEX],
			  const SbmBitvec a[SBM_FLAGS_PER_INDEX],
			  const SbmBitvec b[SBM_FLAGS_PER_INDEX])
{
#ifndef USE_NO_SIMD
	for (size_t off = 0; off < SBM_WORDS_BYTES; off += sizeof(Vector8))
	{
		Vector8		va;
		Vector8		vb;

		vector8_load(&va, (const uint8 *) a + off);
		vector8_load(&vb, (const uint8 *) b + off);
		vector8_store((uint8 *) dst + off, vector8_and(va, vb));
	}
#else
	for (int i = 0; i < SBM_FLAGS_PER_INDEX; i++)
		dst[i] = a[i] & b[i];
#endif
}

static inline void
sbm_words_andnot(SbmBitvec dst[SBM_FLAGS_PER_INDEX],
				 const SbmBitvec a[SBM_FLAGS_PER_INDEX],
				 const SbmBitvec b[SBM_FLAGS_PER_INDEX])
{
	/* dst = a & ~b */
#ifndef USE_NO_SIMD
	for (size_t off = 0; off < SBM_WORDS_BYTES; off += sizeof(Vector8))
	{
		Vector8		va;
		Vector8		vb;

		vector8_load(&va, (const uint8 *) a + off);
		vector8_load(&vb, (const uint8 *) b + off);
		vector8_store((uint8 *) dst + off, vector8_andnot(va, vb));
	}
#else
	for (int i = 0; i < SBM_FLAGS_PER_INDEX; i++)
		dst[i] = a[i] & ~b[i];
#endif
}

#undef SBM_WORDS_BYTES

/*
 * Ensure the result map has enough capacity, growing if needed.
 *
 * `resultp` is pointer to result map pointer (may be reallocated); `needed`
 * is number of bytes needed beyond current usage.
 */
static void
sbm_ensure_capacity(Sbm **resultp, size_t needed)
{
	Sbm		   *result = *resultp;

	/*
	 * Defense in depth: the only callers of sbm_ensure_capacity are sbm_union
	 * / _intersection / _difference, which all allocate their result via
	 * sbm_create() (SBM_OWNED_CONTIGUOUS).  Any other lineage at this point
	 * indicates an internal API misuse -- fail loudly in assert-enabled
	 * builds so we catch it now rather than three operations downstream when
	 * the heap finally notices.
	 */
	Assert(sbm_kind(result) == SBM_OWNED_CONTIGUOUS ||
		   sbm_kind(result) == SBM_OWNED_SPLIT);
	if (result->m_data_used + needed <= sbm_cap(result))
	{
		return;
	}
	{
		size_t		cap = sbm_cap(result);
		size_t		new_cap =
			cap + (cap / 2 > needed ? cap / 2 : needed + 256);
		Sbm		   *grown = sbm_set_data_size(result, NULL, new_cap);

		*resultp = grown;
		return;
	}
}

/*
 * Append a sparse chunk (descriptor + vectors) to the result map.
 *
 * `resultp` is pointer to result map pointer (may grow); `start` is the
 * chunk start offset (SbmIdx); `desc` is the descriptor word; `vecs` is the
 * vector array; `nvecs` is number of vectors.
 */
static void
sbm_append_sparse_chunk(Sbm **resultp, SbmIdx start, SbmBitvec desc,
						SbmBitvec vecs[], int nvecs)
{
	const size_t chunk_size = SBM_SIZEOF_OVERHEAD + sizeof(SbmBitvec) +
		(size_t) nvecs * sizeof(SbmBitvec);
	Sbm		   *result;
	int			i;

	sbm_ensure_capacity(resultp, chunk_size);
	result = *resultp;

	/*
	 * Capacity for the whole chunk was reserved above, so these appends
	 * cannot fail; check anyway so the invariant is enforced by the compiler
	 * rather than by a comment.
	 */
	sbm_append_reserved(result, (const uint8 *) &start, SBM_SIZEOF_OVERHEAD);
	sbm_append_reserved(result, (const uint8 *) &desc, sizeof(SbmBitvec));
	for (i = 0; i < nvecs; i++)
	{
		sbm_append_reserved(result, (const uint8 *) &vecs[i], sizeof(SbmBitvec));
	}

	sbm_set_chunk_count(result, sbm_get_chunk_count(result) + 1);
}

/*
 * Append an RLE chunk to the result map.
 *
 * `resultp` is pointer to result map pointer (may grow); `start` is the
 * chunk start offset; `capacity` is RLE capacity; `length` is RLE length
 * (number of set bits from start).
 */
static void
sbm_append_rle_chunk(Sbm **resultp, SbmIdx start, size_t capacity,
					 size_t length)
{
	Sbm		   *result = *resultp;
	const size_t chunk_size = SBM_SIZEOF_OVERHEAD + sizeof(SbmBitvec);
	alignas(SbmBitvec) uint8 rle_buf[sizeof(SbmBitvec)] = {0};
	SbmChunk	tmp;

	/* Inline coalescing: try to merge with the last emitted chunk. */
	const size_t count = sbm_get_chunk_count(result);

	if (count > 0)
	{
		/* Find the last chunk in the result */
		uint8	   *p = sbm_get_chunk_data(result, 0);
		uint8	   *last_p = p;
		SbmIdx		last_start;
		SbmChunk	last_chunk;
		size_t		i;

		for (i = 0; i < count; i++)
		{
			SbmChunk	c;

			last_p = p;
			sbm_chunk_init(&c, p + SBM_SIZEOF_OVERHEAD);
			p += SBM_SIZEOF_OVERHEAD + sbm_chunk_get_size(&c);
		}

		last_start = sbm_load_idx((const uint8 *) last_p);
		sbm_chunk_init(&last_chunk, last_p + SBM_SIZEOF_OVERHEAD);

		if (sbm_chunk_is_rle(&last_chunk))
		{
			/* Last chunk is RLE -- check if this new RLE is contiguous */
			const size_t last_len =
				sbm_chunk_rle_get_length(&last_chunk);

			if ((size_t) last_start + last_len == (size_t) start)
			{
				/* Contiguous: extend the last chunk in place */
				size_t		new_len = last_len + length;
				size_t		new_cap = (size_t) start + capacity -
					(size_t) last_start;

				if (new_len <= SBM_CHUNK_RLE_MAX_LENGTH &&
					new_cap <= SBM_CHUNK_RLE_MAX_CAPACITY)
				{
					sbm_chunk_rle_set_capacity(&last_chunk,
											   new_cap);
					sbm_chunk_rle_set_length(&last_chunk,
											 new_len);
					return;		/* Merged -- no new chunk needed */
				}
			}
		}
		else
		{
			/* Last chunk is sparse -- check if it's all-ones and contiguous */
			const size_t last_run =
				sbm_chunk_get_run_length(&last_chunk);
			const size_t last_cap =
				sbm_chunk_get_capacity(&last_chunk);

			if (last_run == last_cap && last_run > 0 &&
				(size_t) last_start + last_run == (size_t) start)
			{
				/*
				 * All-ones sparse chunk contiguous with this RLE: replace
				 * sparse with RLE
				 */
				size_t		new_len = last_run + length;
				size_t		new_cap = (size_t) start + capacity -
					(size_t) last_start;

				if (new_len <= SBM_CHUNK_RLE_MAX_LENGTH &&
					new_cap <= SBM_CHUNK_RLE_MAX_CAPACITY)
				{
					/*
					 * Rewrite the last chunk as RLE in place.  An all-ones
					 * sparse chunk stores no payload words (every vector is a
					 * flag), so it is already exactly one descriptor word --
					 * the same size as an RLE chunk -- and no bytes need to
					 * be removed.
					 */
					Assert(sbm_chunk_get_size(&last_chunk) ==
						   sizeof(SbmBitvec));
					sbm_chunk_set_rle(&last_chunk);
					sbm_chunk_rle_set_capacity(&last_chunk,
											   new_cap);
					sbm_chunk_rle_set_length(&last_chunk,
											 new_len);
					return;
				}
			}
		}
	}

	/* No merge possible: append new RLE chunk */
	sbm_ensure_capacity(resultp, chunk_size);
	result = *resultp;

	/* Capacity for the whole chunk was reserved above. */
	sbm_append_reserved(result, (const uint8 *) &start, SBM_SIZEOF_OVERHEAD);

	/* Build and write the RLE word */
	sbm_chunk_init(&tmp, rle_buf);
	sbm_chunk_set_rle(&tmp);
	sbm_chunk_rle_set_capacity(&tmp, capacity);
	sbm_chunk_rle_set_length(&tmp, length);
	sbm_append_reserved(result, rle_buf, sizeof(SbmBitvec));

	sbm_set_chunk_count(result, sbm_get_chunk_count(result) + 1);
}


/*
 * Ordered, collision-free emitter for sbm_offset's output.
 *
 * sbm_offset shifts each source chunk into an aligned output chunk, but
 * a shift is not a bijection on chunk boundaries: several source chunks
 * (and, for a split RLE run, several pieces of one source chunk) can
 * land in the SAME output chunk.  The function used to have five
 * independent append sites plus a carry buffer, each deciding its own
 * start, so two of them could append chunks with identical start
 * offsets.  That breaks the ascending-start invariant every reader
 * assumes: sbm_validate rejects the map, sbm_contains misses bits that
 * sbm_next_member still yields, and sbm_deserialize refuses the map's own
 * serialized bytes.
 *
 * Everything now goes through one emitter that keeps at most one
 * pending sparse output chunk.  Emits for the pending start are merged
 * into it; an emit for a later start flushes the pending chunk first.
 * Since output starts are produced in ascending order, one slot is
 * enough, and each output chunk is appended exactly once.
 *
 * An RLE emit always begins a fresh output chunk, so it just flushes
 * whatever is pending; instrumenting the whole sweep showed the only
 * collisions that ever occur are sparse-into-sparse and
 * sparse-after-RLE, never anything into an RLE chunk.
 */
typedef struct sbm_emitter
{
	Sbm		  **resultp;
	SbmBitvec	words[32];
	int			cap[32];
	SbmIdx		start;
	bool		pending;

	/*
	 * Span of the RLE chunk emitted most recently.  Its capacity is rounded
	 * up to whole output chunks, so a later sparse emit can target a start
	 * that already lies inside it.
	 */
	SbmIdx		rle_start;
	size_t		rle_end;
	size_t		rle_len;
	bool		have_rle;
} SbmEmitter;

static void
sbm_emit_flush(SbmEmitter *e)
{
	if (!e->pending)
	{
		return;
	}
	{
		SbmBitvec	desc;
		SbmBitvec	vecs[32];
		int			nvecs;

		e->pending = false;
		if (!sbm_encode_sparse_chunk(e->words, e->cap, &desc, vecs,
									 &nvecs))
		{
			return;				/* nothing set: emit nothing */
		}

		/*
		 * Once a sparse chunk lands after the RLE chunk, that chunk is no
		 * longer the tail and its span must not absorb later emits.
		 */
		e->have_rle = false;
		sbm_append_sparse_chunk(e->resultp, e->start, desc,
								vecs, nvecs);
		return;
	}
}

/* Emit (or merge) a sparse output chunk given as expanded words. */
static void
sbm_emit_words(SbmEmitter *e, SbmIdx start,
			   const SbmBitvec words[32], const int cap[32])
{
	if (e->pending && e->start == start)
	{
		int			i;

		for (i = 0; i < (int) SBM_FLAGS_PER_INDEX; i++)
		{
			if (cap[i])
			{
				e->words[i] |= words[i];
				e->cap[i] = 1;
			}
		}
		return;
	}

	/*
	 * A start inside the last RLE chunk's span must never happen: an RLE
	 * chunk is emitted with capacity == its own (chunk-aligned) length, so it
	 * never advertises indices it does not own.  Assert it rather than trying
	 * to repair it here -- an overlap means an upstream caller computed the
	 * wrong output start.
	 */
	Assert(!(e->have_rle && (size_t) start >= (size_t) e->rle_start &&
			 (size_t) start < e->rle_end));

	sbm_emit_flush(e);
	memcpy(e->words, words, sizeof(e->words));
	memcpy(e->cap, cap, sizeof(e->cap));
	e->start = start;
	e->pending = true;
}

/*
 * Emit a pure RLE output chunk.
 *
 * An RLE chunk's capacity tells every reader how far it reaches, and the
 * callers round a partial run's capacity up to a whole number of output
 * chunks.  That leaves slack a later sparse emit can legitimately
 * target: the sparse chunk would then be appended after an RLE chunk
 * whose span already contains it, so its start is out of order and its
 * bits are unreachable -- e.g. shifting [0,8192)+[8192,9000) by -90
 * emitted RLE(start 0, cap 8192, len 8102) then a sparse chunk at 6144,
 * losing 90 bits.
 *
 * Remember the span so sbm_emit_words can detect a start that falls
 * inside it.  The capacity must stay a whole number of output chunks:
 * the coalesce pass derives an absorbed run's capacity by rounding up
 * relative to the chunk start, and a non-aligned capacity there
 * miscomputes how many bytes to remove and walks off the buffer.
 */
static void
sbm_emit_rle(SbmEmitter *e, SbmIdx start, size_t capacity,
			 size_t length)
{
	/*
	 * Never advertise capacity the run does not fill.  Callers round a
	 * partial run's capacity up to whole output chunks, which leaves indices
	 * this chunk claims but has no bits for -- and a later source piece can
	 * legitimately need one of them, producing either an out-of-order chunk
	 * (its bits unreachable) or, if the run were simply widened to cover
	 * them, a filled-in gap of spurious set bits.
	 *
	 * Emit only the whole output chunks the run actually fills as RLE, and
	 * pass any sub-chunk remainder to the words path, where a following emit
	 * for the same output chunk merges with it.  The coalesce pass at the end
	 * of sbm_offset folds a saturated remainder back into the RLE chunk, so
	 * the common case costs nothing.
	 *
	 * Capacity must stay a whole number of output chunks: coalesce derives an
	 * absorbed run's capacity by rounding up relative to the chunk start and
	 * miscomputes its byte arithmetic otherwise.
	 */
	(void) capacity;

	{
		const size_t full = (length / SBM_CHUNK_MAX_CAPACITY) *
			SBM_CHUNK_MAX_CAPACITY;
		const size_t rem = length - full;
		SbmBitvec	w[32];
		int			c[32];
		int			i;
		size_t		bit;

		/*
		 * One RLE chunk holds at most SBM_CHUNK_RLE_MAX_LENGTH bits, so a
		 * longer run becomes a sequence of RLE chunks, each a whole number of
		 * windows.
		 */
		const size_t rle_max = (SBM_CHUNK_RLE_MAX_LENGTH /
								SBM_CHUNK_MAX_CAPACITY) *
			SBM_CHUNK_MAX_CAPACITY;
		size_t		done = 0;

		while (done < full)
		{
			const size_t piece = Min(full - done, rle_max);
			const SbmIdx piece_start = (SbmIdx) ((size_t) start + done);

			sbm_emit_flush(e);
			sbm_append_rle_chunk(e->resultp, piece_start, piece, piece);
			e->rle_start = piece_start;
			e->rle_end = (size_t) piece_start + piece;
			e->rle_len = piece;
			e->have_rle = true;
			done += piece;
		}

		if (rem == 0)
		{
			return;
		}

		memset(w, 0, sizeof(w));

		/*
		 * Full 32-slot capacity (see sbm_emit_run): a partial trailing run
		 * still claims the whole chunk window, encoding the unset tail slots
		 * as ZEROS rather than NONE.
		 */
		for (i = 0; i < 32; i++)
			c[i] = 1;
		for (bit = 0; bit < rem; bit += SBM_BITS_PER_VECTOR)
		{
			const size_t slot = bit / SBM_BITS_PER_VECTOR;
			const size_t n = (rem - bit < SBM_BITS_PER_VECTOR) ?
				rem - bit :
				SBM_BITS_PER_VECTOR;

			w[slot] = (n == SBM_BITS_PER_VECTOR) ?
				~(SbmBitvec) 0 :
				(((SbmBitvec) 1 << n) - 1);
		}

		/*
		 * The remainder starts exactly where the RLE part ended, so it is
		 * outside that chunk's span; clear the marker so the assertion in
		 * sbm_emit_words (which forbids a start *inside* the span) is not
		 * confused by the boundary case.
		 */
		e->have_rle = false;
		sbm_emit_words(e,
					   (SbmIdx) ((size_t) start + full), w, c);
		return;
	}
}

/*
 * Emit an arbitrary half-open run [lo, hi) of set bits through the
 * ordered emitter in O(output chunks), NOT O(hi-lo).
 *
 * The set-algebra ops (sbm_xor, sbm_extract_range) consume operand runs
 * with sbm_run_next and used to materialise each survivor one bit at a
 * time via sbm_add -- so a single [0, 2^31) run cost 2^31 add calls and
 * the amplification DoS lived here, not in the run reader.  A run is
 * instead split at chunk boundaries: a sub-chunk head goes to the words
 * path, the whole output chunks it fills go out as RLE, and the
 * sub-chunk tail is handled by sbm_emit_rle's own remainder path.
 *
 * runs are delivered ascending and non-overlapping, so consecutive
 * emits for the same output chunk merge in sbm_emit_words and each
 * output chunk is appended exactly once.  Capacity stays chunk-aligned
 * (see sbm_emit_rle) so the coalesce pass never walks off the buffer.
 */
static void
sbm_emit_run(SbmEmitter *e, uint64 lo, uint64 hi)
{
	uint64		aligned;
	uint64		body_lo = lo;

	if (lo >= hi)
	{
		return;
	}

	/* Sub-chunk head: bits from lo up to the next chunk boundary. */
	aligned = ((lo + SBM_CHUNK_MAX_CAPACITY - 1) /
			   SBM_CHUNK_MAX_CAPACITY) *
		SBM_CHUNK_MAX_CAPACITY;
	if (aligned > lo)
	{
		const uint64 head_hi = aligned < hi ? aligned : hi;
		const SbmIdx chunk_start =
			(SbmIdx) (lo - (lo % SBM_CHUNK_MAX_CAPACITY));
		SbmBitvec	w[32];
		int			c[32];
		int			i;
		uint64		bit;

		memset(w, 0, sizeof(w));

		/*
		 * Full 32-slot capacity, matching sbm_expand_rle_as_words: a sparse
		 * chunk with front slots marked NONE (cap 0) instead of ZEROS
		 * confuses the reader, which then finds only the first set bit. Every
		 * emitted sparse chunk claims the whole 2048-bit window and encodes
		 * absent slots as ZEROS.
		 */
		for (i = 0; i < 32; i++)
			c[i] = 1;
		for (bit = lo; bit < head_hi; bit++)
		{
			const uint64 off = bit - chunk_start;
			const size_t slot = off / SBM_BITS_PER_VECTOR;

			w[slot] |= (SbmBitvec) 1
				<< (off % SBM_BITS_PER_VECTOR);
		}
		sbm_emit_words(e, chunk_start, w, c);
		body_lo = head_hi;
	}

	if (body_lo >= hi)
	{
		return;
	}

	/*
	 * body_lo is now chunk-aligned; sbm_emit_rle emits the whole output
	 * chunks as RLE and forwards its own sub-chunk tail to the words path.
	 * capacity == length keeps the RLE chunk-aligned.
	 */
	{
		const size_t len = (size_t) (hi - body_lo);

		sbm_emit_rle(e, (SbmIdx) body_lo, len, len);
		return;
	}
}

/*
 * Absolute shifted start of a chunk under sbm_offset, computed without a
 * signed-overflow intermediate.
 *
 * sbm_offset shifts every bit at position i to i + offset.  A chunk that
 * starts at src_start therefore lands at src_start + offset, which for a
 * large |offset| overflows the ssize_t used by the old `(ssize_t)src_start
 * + offset` expression even though the true value fits (the caller's
 * ERANGE / drop guards already bound the surviving range to
 * [0, SBM_IDX_MAX]).  Return the shifted start as an unsigned magnitude
 * plus a sign flag: *neg is true when the start falls below bit 0 (only
 * possible for offset < 0), in which case *mag is how far below 0 it is.
 * The whole computation stays in uint64 / ssize_t with explicit range
 * checks -- no __int128, no compiler builtins (MSVC-portable, two-file
 * style).
 */
static inline uint64
sbm_offset_abs_start(uint64 src_start, ssize_t offset, bool *neg)
{
	uint64		down;

	if (offset >= 0)
	{
		/*
		 * Non-negative shift.  The offset>0 ERANGE guard already proved max +
		 * offset <= SBM_IDX_MAX and src_start <= max, so this uint64 add
		 * cannot wrap.
		 */
		*neg = false;
		return src_start + (uint64) offset;
	}
	/* offset < 0: |offset| as an unsigned magnitude (SSIZE_MIN-safe). */
	down = (uint64) (-(offset + 1)) + 1;
	if (src_start >= down)
	{
		*neg = false;
		return src_start - down;
	}
	*neg = true;
	return down - src_start;
}

Sbm *
sbm_offset(const Sbm *map, ssize_t offset)
{
	size_t		count;
	size_t		chunk_bytes;
	size_t		cap;
	Sbm		   *result;
	uint8	   *p;
	size_t		i;
	SbmEmitter	em;

	sbm_check_invariants(map);
	if (map == NULL)
	{
		return NULL;
	}
	if (sbm_is_small(map))
	{
		Sbm		   *m = sbm_materialize(map);
		Sbm		   *r;

		if (m == NULL)
			return NULL;
		r = sbm_offset(m, offset);
		sbm_free(m);
		if (r != NULL)
			sbm_try_demote(r);
		return r;
	}

	/* offset == 0: just copy */
	if (offset == 0)
	{
		return sbm_copy(map);
	}

	count = sbm_get_chunk_count(map);
	if (count == 0)
	{
		return NULL;
	}

	/* Check for overflow: if shifting right and max bit would overflow */
	if (offset > 0)
	{
		uint64		max = sbm_maximum(map);

		if (max > SBM_IDX_MAX - (uint64) offset)
		{
			errno = ERANGE;
			return NULL;
		}
	}

	/* Check if all bits would be shifted below 0 */
	if (offset < 0)
	{
		uint64		max = sbm_maximum(map);

		/*
		 * Compare in unsigned to avoid the (ssize_t)max cast UB when a valid
		 * map holds a bit above 2^63: the whole map is dropped iff every bit
		 * shifts below 0, i.e. max < |offset|.
		 */
		const uint64 neg = (uint64) (-(offset + 1)) + 1;	/* |offset| */

		if (max < neg)
		{
			return NULL;		/* all bits shifted away */
		}
	}

	/*
	 * Allocate result.
	 *
	 * A shift is not a bijection on chunk boundaries: an unaligned offset
	 * splits each source chunk across two output chunks, so the result can
	 * need roughly twice the source's bytes plus a chunk of slack.  Sizing it
	 * at exactly map->m_data_used left the buffer completely full, and the
	 * coalesce pass at the end works in place on the byte stream and needs a
	 * chunk of headroom for its memmove.
	 */
	chunk_bytes = SBM_SIZEOF_OVERHEAD +
		sizeof(SbmBitvec) * (SBM_FLAGS_PER_INDEX + 1);
	cap = map->m_data_used * 2 + chunk_bytes;
	result = sbm_create(cap > 1024 ? cap : 1024);

	/*
	 * Single ordered emitter: holds at most one pending output chunk so that
	 * several source pieces landing in the same aligned output chunk are
	 * merged instead of appended twice.  Replaces the old carry buffer, which
	 * only serialised the forward-overflow case.
	 */
	memset(&em, 0, sizeof(em));
	em.resultp = &result;
	em.pending = false;

	/* Walk source chunks */
	p = sbm_get_chunk_data(map, 0);

	for (i = 0; i < count; i++)
	{
		const SbmIdx src_start = sbm_load_idx((const uint8 *) p);
		SbmChunk	chunk;
		size_t		chunk_size;

		p += SBM_SIZEOF_OVERHEAD;
		sbm_chunk_init(&chunk, p);
		chunk_size = sbm_chunk_get_size(&chunk);

		if (sbm_chunk_is_rle(&chunk))
		{
			const size_t rle_len =
				sbm_chunk_rle_get_length(&chunk);
			bool		neg;
			uint64		mag;
			uint64		clipped_start;
			size_t		new_len;
			SbmIdx		aligned_start;
			size_t		rle_offset_in_chunk;

			/*
			 * RLE set bits occupy [src_start, src_start + rle_len). After the
			 * shift they occupy [start, start + rle_len) where start =
			 * src_start + offset.  Compute start as an unsigned magnitude +
			 * sign so a large |offset| cannot overflow a signed intermediate;
			 * clip the part that falls below bit 0.
			 */
			mag = sbm_offset_abs_start(src_start, offset, &neg);
			if (neg)
			{
				/* Run starts mag bits below 0. */
				if (rle_len <= (size_t) mag)
				{
					goto next_chunk;	/* wholly below 0 */
				}
				clipped_start = 0;
				new_len = rle_len - (size_t) mag;
			}
			else
			{
				clipped_start = mag;
				new_len = rle_len;
			}
			if (new_len == 0)
			{
				goto next_chunk;
			}

			/* Align the start to chunk boundary */
			aligned_start =
				(SbmIdx) sbm_get_chunk_aligned_offset(
													  (size_t) clipped_start);
			rle_offset_in_chunk =
				(size_t) clipped_start - aligned_start;

			if (rle_offset_in_chunk == 0)
			{
				/* Starts on chunk boundary, emit as pure RLE */
				size_t		new_cap =
					((new_len + SBM_CHUNK_MAX_CAPACITY - 1) /
					 SBM_CHUNK_MAX_CAPACITY) *
					SBM_CHUNK_MAX_CAPACITY;

				if (new_cap < new_len)
				{
					new_cap = new_len;
				}
				sbm_emit_rle(&em, aligned_start, new_cap, new_len);
			}
			else
			{
				/* Emit first partial chunk as sparse */
				size_t		first_chunk_bits =
					SBM_CHUNK_MAX_CAPACITY - rle_offset_in_chunk;
				SbmBitvec	fw[32] = {0};
				int			fc[32] = {0};
				size_t		last_data_slot;
				size_t		bp;
				size_t		bl;
				size_t		remaining;
				SbmIdx		cur_start;
				size_t		s;

				if (first_chunk_bits > new_len)
				{
					first_chunk_bits = new_len;
				}

				/* Mark capacity for all slots up to and including the data */
				last_data_slot =
					(rle_offset_in_chunk + first_chunk_bits +
					 SBM_BITS_PER_VECTOR - 1) /
					SBM_BITS_PER_VECTOR;
				for (s = 0; s < last_data_slot && s < 32;
					 s++)
				{
					fc[s] = 1;
				}
				/* Set the actual bits */
				bp = rle_offset_in_chunk;
				bl = first_chunk_bits;
				while (bl > 0)
				{
					size_t		slot = bp / SBM_BITS_PER_VECTOR;
					size_t		bit_in_vec =
						bp % SBM_BITS_PER_VECTOR;
					size_t		can_set =
						SBM_BITS_PER_VECTOR - bit_in_vec;

					if (can_set > bl)
						can_set = bl;
					fc[slot] = 1;
					if (can_set == SBM_BITS_PER_VECTOR)
					{
						fw[slot] = ~(SbmBitvec) 0;
					}
					else
					{
						fw[slot] |= (((SbmBitvec) 1
									  << can_set) -
									 1)
							<< bit_in_vec;
					}
					bp += can_set;
					bl -= can_set;
				}

				sbm_emit_words(&em, aligned_start, fw, fc);

				remaining = new_len - first_chunk_bits;
				cur_start =
					aligned_start + SBM_CHUNK_MAX_CAPACITY;

				/* Emit middle RLE for full chunks */
				if (remaining >= SBM_CHUNK_MAX_CAPACITY)
				{
					size_t		rle_mid =
						(remaining /
						 SBM_CHUNK_MAX_CAPACITY) *
						SBM_CHUNK_MAX_CAPACITY;

					sbm_emit_rle(&em, cur_start, rle_mid, rle_mid);
					cur_start += (SbmIdx) rle_mid;
					remaining -= rle_mid;
				}

				/* Emit last partial chunk */
				if (remaining > 0)
				{
					SbmBitvec	lw[32] = {0};
					int			lc[32] = {0};
					size_t		lbit = 0,
								lrem = remaining;

					while (lrem > 0)
					{
						size_t		slot =
							lbit / SBM_BITS_PER_VECTOR;
						size_t		can_set =
							SBM_BITS_PER_VECTOR;

						if (can_set > lrem)
							can_set = lrem;
						lc[slot] = 1;
						if (can_set ==
							SBM_BITS_PER_VECTOR)
						{
							lw[slot] =
								~(SbmBitvec) 0;
						}
						else
						{
							lw[slot] =
								((SbmBitvec) 1
								 << can_set) -
								1;
						}
						lbit += can_set;
						lrem -= can_set;
					}
					sbm_emit_words(&em, cur_start, lw, lc);
				}
			}
		}
		else
		{
			/*
			 * Sparse chunk: expand to 32 words, compute final absolute
			 * positions, place into correct output chunk(s).
			 */
			SbmBitvec	words[32];
			int			cf[32];
			bool		neg;
			uint64		mag;
			ssize_t		intra_shift;
			uint64		out_aligned;
			SbmBitvec	main_words[32] = {0};
			int			main_cap[32] = {0};
			SbmBitvec	overflow_words[32] = {0};
			int			overflow_cap[32] = {0};
			bool		has_overflow = false;
			int			ow;

			sbm_expand_sparse_chunk(&chunk, words, cf);

			/*
			 * Each bit at absolute position src_start + slot*64 + bit_offset
			 * maps to src_start + offset + slot*64 + bit_offset in the
			 * output.
			 *
			 * The output chunk aligned start = align(src_start + offset). The
			 * intra-chunk shift = (src_start + offset) - aligned_start.
			 *
			 * If intra >= 0: right-shift within the 32-word array, overflow
			 * to carry. If intra < 0 (new start negative): left-shift,
			 * dropping low bits.
			 */

			mag = sbm_offset_abs_start(src_start, offset, &neg);

			/*
			 * Compute aligned output chunk start and intra-chunk shift. All
			 * arithmetic is unsigned; intra_shift for the non-negative case
			 * is a within-chunk remainder (0 .. SBM_CHUNK_MAX_CAPACITY-1), so
			 * it fits ssize_t with room to spare.  A start below bit 0 (neg)
			 * becomes a left-shift by `mag` bits, dropping the low bits.
			 */
			if (!neg)
			{
				const uint64 oa =
					sbm_get_chunk_aligned_offset((size_t) mag);

				out_aligned = oa;
				intra_shift = (ssize_t) (mag - oa);
			}
			else
			{
				/*
				 * start < 0: surviving bits begin at 0, low `mag` bits drop.
				 * A drop of a whole source chunk (mag >=
				 * SBM_CHUNK_MAX_CAPACITY bits) leaves nothing.  Otherwise
				 * represent it as a negative intra_shift whose magnitude is
				 * the drop amount (< 2048, so it fits ssize_t).
				 */
				out_aligned = 0;
				if (mag >= SBM_CHUNK_MAX_CAPACITY)
				{
					goto next_chunk;
				}
				intra_shift = -(ssize_t) mag;
			}

			if (intra_shift >= 0)
			{
				/* Right-shift by intra_shift bits */
				size_t		word_shift =
					(size_t) intra_shift / SBM_BITS_PER_VECTOR;
				size_t		bit_rem =
					(size_t) intra_shift % SBM_BITS_PER_VECTOR;
				int			w;
				size_t		wz;

				for (w = 31; w >= 0; w--)
				{
					size_t		dst;

					if (!cf[w] && words[w] == 0)
						continue;

					dst = (size_t) w + word_shift;
					if (bit_rem == 0)
					{
						if (dst < 32)
						{
							main_words[dst] |=
								words[w];
							main_cap[dst] = 1;
						}
						else if (dst < 64)
						{
							overflow_words[dst -
										   32] |= words[w];
							overflow_cap[dst - 32] =
								1;
						}
					}
					else
					{
						SbmBitvec	lo = words[w]
							<< bit_rem;
						SbmBitvec	hi = words[w] >>
							(SBM_BITS_PER_VECTOR -
							 bit_rem);
						size_t		dst1;

						if (dst < 32)
						{
							main_words[dst] |= lo;
							main_cap[dst] = 1;
						}
						else if (dst < 64)
						{
							overflow_words[dst -
										   32] |= lo;
							overflow_cap[dst - 32] =
								1;
						}

						dst1 = dst + 1;
						if (dst1 < 32)
						{
							main_words[dst1] |= hi;
							main_cap[dst1] = 1;
						}
						else if (dst1 < 64)
						{
							overflow_words[dst1 -
										   32] |= hi;
							overflow_cap[dst1 -
										 32] = 1;
						}
					}
				}

				/* Mark shifted-in zero slots as capacity */
				for (wz = 0; wz < word_shift && wz < 32;
					 wz++)
				{
					main_cap[wz] = 1;
				}
			}
			else
			{
				/*
				 * intra_shift < 0: left-shift by |intra_shift| bits (dropping
				 * low bits)
				 */
				size_t		drop = (size_t) (-intra_shift);
				size_t		word_drop = drop / SBM_BITS_PER_VECTOR;
				size_t		bit_drop = drop % SBM_BITS_PER_VECTOR;
				size_t		w;

				for (w = 0; w < 32; w++)
				{
					size_t		src_w = w + word_drop;

					if (src_w >= 32)
						break;
					main_cap[w] = 1;
					if (bit_drop == 0)
					{
						main_words[w] = words[src_w];
					}
					else
					{
						main_words[w] =
							words[src_w] >> bit_drop;
						if (src_w + 1 < 32)
						{
							main_words[w] |=
								words[src_w + 1]
								<< (SBM_BITS_PER_VECTOR -
									bit_drop);
						}
					}
				}
			}

			/*
			 * Emit the main chunk, then any overflow into the next one.  Both
			 * go through the ordered emitter, so a chunk that another source
			 * piece also targets is merged rather than appended a second
			 * time.
			 */
			sbm_emit_words(&em, (SbmIdx) out_aligned, main_words, main_cap);

			for (ow = 0; ow < (int) SBM_FLAGS_PER_INDEX; ow++)
			{
				if (overflow_cap[ow] && overflow_words[ow] != 0)
				{
					has_overflow = true;
					break;
				}
			}
			if (has_overflow)
			{
				sbm_emit_words(&em, (SbmIdx) out_aligned + SBM_CHUNK_MAX_CAPACITY, overflow_words, overflow_cap);
			}
		}

next_chunk:
		p += chunk_size;
	}

	/* Flush the last pending output chunk. */
	sbm_emit_flush(&em);
	result = *em.resultp;

	/* If no chunks were added, return NULL */
	if (sbm_get_chunk_count(result) == 0)
	{
		sbm_free(result);
		return NULL;
	}

	/* Coalesce adjacent chunks where possible */
	sbm_coalesce_map(result);

	return result;
}

/* -------------------------------------------------------------------
 * Predicates and member-by-member iteration
 * -------------------------------------------------------------------
 */

bool
sbm_is_empty(const Sbm *map)
{
	if (map == NULL)
	{
		return true;
	}
	if (sbm_is_small(map))
	{
		return sbm_small_is_empty(map);
	}
	sbm_check_invariants(map);
	return sbm_get_chunk_count(map) == 0;
}

/*
 * Iterate set bits in `chunk` (anchored at absolute `start`),
 * starting strictly after `lower_excl`.  Returns the first set bit
 * found, or SBM_IDX_MAX if none.  Pass PG_UINT64_MAX as lower_excl to
 * mean "start before bit 0" (return the first bit at or after start).
 */
static SbmIdx
sbm_chunk_next_set(const SbmChunk *chunk, uint64 start,
				   uint64 lower_excl)
{
	size_t		v;

	if (sbm_chunk_is_rle(chunk))
	{
		const size_t length = sbm_chunk_rle_get_length(chunk);
		uint64		run_lo;
		uint64		run_hi;

		if (length == 0)
		{
			return SBM_IDX_MAX;
		}
		run_lo = start;
		run_hi = start + length - 1;
		if (lower_excl != PG_UINT64_MAX && lower_excl >= run_hi)
		{
			return SBM_IDX_MAX;
		}
		if (lower_excl == PG_UINT64_MAX || lower_excl < run_lo)
		{
			return run_lo;
		}
		return lower_excl + 1;
	}

	for (v = 0; v < SBM_FLAGS_PER_INDEX; v++)
	{
		const uint64 vec_lo = start + v * SBM_BITS_PER_VECTOR;
		const uint64 vec_hi = vec_lo + SBM_BITS_PER_VECTOR - 1;
		const size_t flags =
			SBM_CHUNK_GET_FLAGS(chunk->m_data[0], v);
		SbmBitvec	w;
		uint64		skip = 0;
		SbmBitvec	masked;

		if (lower_excl != PG_UINT64_MAX && vec_hi <= lower_excl)
		{
			continue;
		}
		if (flags == SBM_PAYLOAD_NONE || flags == SBM_PAYLOAD_ZEROS)
		{
			continue;
		}
		if (flags == SBM_PAYLOAD_ONES)
		{
			if (lower_excl == PG_UINT64_MAX || lower_excl < vec_lo)
			{
				return vec_lo;
			}
			return lower_excl + 1;
		}
		/* SBM_PAYLOAD_MIXED: scan the payload word for a 1-bit > lower_excl. */
		w = chunk->m_data[1 + sbm_chunk_get_position(chunk, v)];
		if (lower_excl != PG_UINT64_MAX && lower_excl >= vec_lo)
		{
			skip = lower_excl - vec_lo + 1;
			if (skip >= SBM_BITS_PER_VECTOR)
				continue;
		}
		masked = w & (~(SbmBitvec) 0 << skip);
		if (masked == 0)
		{
			continue;
		}
		return vec_lo + (uint64) pg_rightmost_one_pos64(masked);
	}
	return SBM_IDX_MAX;
}

/*
 * Iterate set bits in `chunk` (anchored at absolute `start`),
 * looking for the highest set bit strictly less than `upper_excl`.
 */
static SbmIdx
sbm_chunk_prev_set(const SbmChunk *chunk, uint64 start,
				   uint64 upper_excl)
{
	ssize_t		v;

	if (sbm_chunk_is_rle(chunk))
	{
		const size_t length = sbm_chunk_rle_get_length(chunk);
		uint64		run_hi;

		if (length == 0 || upper_excl <= start)
		{
			return SBM_IDX_MAX;
		}
		run_hi = start + length - 1;
		return upper_excl - 1 < run_hi ? upper_excl - 1 : run_hi;
	}

	for (v = SBM_FLAGS_PER_INDEX - 1; v >= 0; v--)
	{
		const uint64 vec_lo =
			start + (uint64) v * SBM_BITS_PER_VECTOR;
		const size_t flags =
			SBM_CHUNK_GET_FLAGS(chunk->m_data[0], (size_t) v);
		const uint64 vec_hi = vec_lo + SBM_BITS_PER_VECTOR - 1;
		SbmBitvec	w;

		if (vec_lo >= upper_excl)
		{
			continue;
		}
		if (flags == SBM_PAYLOAD_NONE || flags == SBM_PAYLOAD_ZEROS)
		{
			continue;
		}
		if (flags == SBM_PAYLOAD_ONES)
		{
			return
				upper_excl - 1 < vec_hi ? upper_excl - 1 : vec_hi;
		}
		/* SBM_PAYLOAD_MIXED. */
		w = chunk
			->m_data[1 +
					 sbm_chunk_get_position(chunk, (size_t) v)];
		if (upper_excl - 1 < vec_hi)
		{
			const uint64 bits_to_keep = upper_excl - vec_lo;

			if (bits_to_keep == 0)
				continue;
			w &= (~(SbmBitvec) 0) >>
				(SBM_BITS_PER_VECTOR - bits_to_keep);
		}
		if (w == 0)
			continue;
		return vec_lo +
			(uint64) (SBM_BITS_PER_VECTOR - 1 - (63 - pg_leftmost_one_pos64(w)));
	}
	return SBM_IDX_MAX;
}

uint64
sbm_next_member(const Sbm *map, uint64 prev_idx, SbmCursor *cur)
{
	if (map == NULL)
		return SBM_IDX_MAX;
	if (sbm_is_small(map))
	{
		/*
		 * First set bit strictly greater than prev_idx (SBM_IDX_MAX means
		 * "from the start").
		 */
		const uint64 *words = sbm_small_words(map);
		const size_t n = sbm_small_nwords(map);
		const uint64 from = (prev_idx == SBM_IDX_MAX) ? 0
			: prev_idx + 1;
		size_t		w;
		uint64		word;

		if (prev_idx != SBM_IDX_MAX &&
			prev_idx >= (uint64) n * 64)
			return SBM_IDX_MAX;
		w = (size_t) (from / 64);
		if (w >= n)
			return SBM_IDX_MAX;
		word = words[w] & (~(uint64) 0 << (from % 64));
		for (;;)
		{
			if (word != 0)
				return (uint64) w * 64 +
					(uint64) pg_rightmost_one_pos64(word);
			if (++w >= n)
				return SBM_IDX_MAX;
			word = words[w];
		}
	}
	sbm_check_invariants(map);
	{
		const size_t count = sbm_get_chunk_count(map);
		uint8	   *base;
		uint8	   *p;
		size_t		stream_end;

		if (count == 0)
			return SBM_IDX_MAX;

		base = sbm_get_chunk_data(map, 0);
		p = base;
		stream_end = map->m_data_used - SBM_SIZEOF_OVERHEAD;

		/*
		 * Cursor fast-path.  Sequential forward iteration while ((i =
		 * sbm_next_member(map, i, &c)) != SBM_IDX_MAX) ... is the dominant
		 * scan-side hot path.  Without a cursor each call walks from chunk 0
		 * -- O(N) per call, O(N^2) per scan. Resume from the cached chunk
		 * when prev_idx is not earlier than that chunk's start.
		 */
		if (prev_idx != SBM_IDX_MAX && cur != NULL &&
			cur->offset != SIZE_MAX && cur->offset < stream_end &&
			cur->start_idx <= prev_idx)
		{
			p = base + cur->offset;
		}

		while ((size_t) (p - base) < stream_end)
		{
			const SbmIdx start =
				sbm_load_idx((const uint8 *) p);
			SbmChunk	chunk;
			size_t		cap;
			uint64		hit;

			sbm_chunk_init(&chunk, p + SBM_SIZEOF_OVERHEAD);
			cap = sbm_chunk_get_capacity(&chunk);
			/* Skip chunks entirely below the lower bound. */
			if (prev_idx != SBM_IDX_MAX &&
				start + cap - 1 <= prev_idx)
			{
				p += SBM_SIZEOF_OVERHEAD +
					sbm_chunk_get_size(&chunk);
				continue;
			}
			hit = sbm_chunk_next_set(&chunk, start, prev_idx);
			if (hit != SBM_IDX_MAX)
			{
				if (cur != NULL)
				{
					cur->offset = (size_t) (p - base);
					cur->start_idx = start;
				}
				return hit;
			}
			p += SBM_SIZEOF_OVERHEAD + sbm_chunk_get_size(&chunk);
		}
		return SBM_IDX_MAX;
	}
}

uint64
sbm_prev_member(const Sbm *map, uint64 prev_idx, SbmCursor *cur)
{
	/*
	 * The cursor accelerates forward (non-decreasing) lookups only; reverse
	 * iteration always walks from the head, so cur is accepted for API
	 * symmetry but unused.
	 */
	(void) cur;
	if (map == NULL)
		return SBM_IDX_MAX;
	if (sbm_is_small(map))
	{
		/*
		 * Highest set bit strictly less than prev_idx (SBM_IDX_MAX means
		 * "from the end").
		 */
		const uint64 *words = sbm_small_words(map);
		const size_t n = sbm_small_nwords(map);
		uint64		upper_excl;
		uint64		last;
		size_t		w;
		uint64		word;

		if (n == 0)
			return SBM_IDX_MAX;
		upper_excl =
			(prev_idx == SBM_IDX_MAX) ? (uint64) n * 64 : prev_idx;
		if (upper_excl == 0)
			return SBM_IDX_MAX;
		last = upper_excl - 1;
		if (last >= (uint64) n * 64)
			last = (uint64) n * 64 - 1;
		w = (size_t) (last / 64);
		word = words[w] & (~(uint64) 0 >> (63 - (last % 64)));
		for (;;)
		{
			if (word != 0)
				return (uint64) w * 64 +
					(63 - (63 - pg_leftmost_one_pos64(word)));
			if (w == 0)
				return SBM_IDX_MAX;
			w--;
			word = words[w];
		}
	}
	sbm_check_invariants(map);
	{
		const size_t count = sbm_get_chunk_count(map);

		/* SBM_IDX_MAX as input means "start past the end". */
		const uint64 upper_excl =
			(prev_idx == SBM_IDX_MAX) ? PG_UINT64_MAX : prev_idx;

		/*
		 * Walk forward to the last chunk that starts before upper_excl,
		 * remembering each chunk so we can step back if needed.
		 */
		uint8	   *p = sbm_get_chunk_data(map, 0);

		/* Track up to `count` candidate chunk pointers. */
		uint8	   *last = NULL;
		size_t		last_idx = 0;
		size_t		i;

		if (count == 0)
			return SBM_IDX_MAX;
		for (i = 0; i < count; i++)
		{
			const SbmIdx start =
				sbm_load_idx((const uint8 *) p);
			SbmChunk	tmp;

			if (start >= upper_excl)
				break;
			last = p;
			last_idx = i;
			sbm_chunk_init(&tmp, p + SBM_SIZEOF_OVERHEAD);
			p += SBM_SIZEOF_OVERHEAD + sbm_chunk_get_size(&tmp);
		}
		if (last == NULL)
			return SBM_IDX_MAX;

		/* Step back through chunks until we find a hit. */
		while (true)
		{
			const SbmIdx start =
				sbm_load_idx((const uint8 *) last);
			SbmChunk	chunk;
			uint64		hit;
			uint8	   *q;
			size_t		j;

			sbm_chunk_init(&chunk, last + SBM_SIZEOF_OVERHEAD);
			hit = sbm_chunk_prev_set(&chunk, start, upper_excl);
			if (hit != SBM_IDX_MAX)
				return hit;
			if (last_idx == 0)
				break;
			/* Walk forward to find the chunk preceding `last`. */
			q = sbm_get_chunk_data(map, 0);
			for (j = 0; j + 1 < last_idx; j++)
			{
				SbmChunk	tmp;

				sbm_chunk_init(&tmp, q + SBM_SIZEOF_OVERHEAD);
				q += SBM_SIZEOF_OVERHEAD +
					sbm_chunk_get_size(&tmp);
			}
			last = q;
			last_idx--;
		}
		return SBM_IDX_MAX;
	}
}



SbmMembership
sbm_membership(const Sbm *map)
{
	uint64		first,
				second;

	if (map == NULL || sbm_is_empty(map))
		return SBM_EMPTY;
	first = sbm_next_member(map, SBM_IDX_MAX, NULL);
	if (first == SBM_IDX_MAX)
		return SBM_EMPTY;
	second = sbm_next_member(map, first, NULL);
	return (second == SBM_IDX_MAX) ? SBM_SINGLETON : SBM_MULTIPLE;
}

uint64
sbm_singleton_member(const Sbm *map)
{
	uint64		first,
				second;

	if (map == NULL || sbm_is_empty(map))
		return SBM_IDX_MAX;
	first = sbm_next_member(map, SBM_IDX_MAX, NULL);
	if (first == SBM_IDX_MAX)
		return SBM_IDX_MAX;
	second = sbm_next_member(map, first, NULL);
	return (second == SBM_IDX_MAX) ? first : SBM_IDX_MAX;
}

/* -------------------------------------------------------------------
 * Cardinality without allocation, bulk add, array conversion
 * -------------------------------------------------------------------
 */

/*
 * Maximal-run iterator.
 *
 * The set-algebra and hashing helpers below used to walk bit-by-bit via
 * sbm_next_member, making them O(cardinality): a single 24-byte RLE
 * chunk declaring a 2^31-bit run turned sbm_xor / sbm_hash / the
 * *_cardinality family / sbm_jaccard_index / sbm_extract_range into
 * multi-second (or, for sbm_split, non-terminating) loops on an
 * attacker-sized input.
 *
 * This iterator instead yields half-open runs [lo, hi) of set bits, so
 * its cost tracks the ENCODED size: an RLE chunk is one run no matter
 * how long, and a sparse chunk yields at most a chunk's worth of runs
 * (<= 2048 bits, physically present).
 *
 * Runs are MAXIMAL and CANONICAL: sbm_run_next coalesces any two
 * adjacent runs whose spans touch (this run's hi == the next run's
 * lo), including across the per-chunk run-list seam and across chunk
 * boundaries.  This is required for correctness, not just tidiness:
 * the SAME logical set can be stored with a contiguous range
 * straddling a 2048-aligned chunk boundary (e.g. sbm_union stitching
 * two split halves back together, or an RLE run abutting the next
 * chunk) OR entirely within a single chunk.  Without seam-coalescing
 * those two encodings decompose into DIFFERENT run sequences
 * ([.,2048)+[2048,.) vs one run), which made sbm_equals / sbm_hash /
 * sbm_compare -- all of which require the decomposition to be canonical
 * -- disagree for logically-equal maps.  Coalescing stays O(chunks):
 * sbm_run_next_raw yields each physically-present run exactly once
 * and sbm_run_next only extends across a seam when the runs actually
 * abut, so a 2^31-bit RLE run is still a single yielded run.
 */
typedef struct
{
	const Sbm  *map;
	size_t		count;			/* total chunk count */
	size_t		idx;			/* next chunk ordinal to decode */
	uint8	   *p;				/* cursor into the chunk stream */

	/*
	 * Runs decoded from the current chunk, not yet yielded.  A sparse chunk
	 * with an alternating bit pattern is the worst case:
	 * SBM_CHUNK_MAX_CAPACITY / 2 single-bit runs, so size for that plus one.
	 */
	uint64		run_lo[SBM_CHUNK_MAX_CAPACITY / 2 + 1];
	uint64		run_hi[SBM_CHUNK_MAX_CAPACITY / 2 + 1];
	size_t		nruns;
	size_t		next_run;

	/*
	 * One-run lookahead for seam-coalescing in sbm_run_next: the first raw
	 * run that did NOT abut the run just yielded, held for the next call so
	 * no run is dropped.
	 */
	bool		have_peek;
	uint64		peek_lo;
	uint64		peek_hi;
} SbmRunIter;

static void
sbm_run_iter_init(SbmRunIter *it, const Sbm *map)
{
	/*
	 * Only the scalar bookkeeping fields need initializing; the run_lo/run_hi
	 * buffers (~16 KB) are written before they are read, so we must NOT
	 * memset the whole struct here -- doing so dominated the cost of the hot
	 * comparators (sbm_equals/overlap/subset_compare), which construct an
	 * iterator per call.
	 */
	it->map = map;
	it->idx = 0;
	it->nruns = 0;
	it->next_run = 0;
	it->have_peek = false;
	it->p = NULL;
	if (map == NULL || sbm_is_empty(map))
	{
		it->count = 0;
		return;
	}
	if (sbm_is_small(map))
	{
		/*
		 * Decode the whole small map into the run buffer up front and leave
		 * count == 0 so sbm_run_next just drains it.  A small map spans <
		 * SBM_SMALL_MAX_BITS (<= 1024) bits, whose worst case (alternating)
		 * is <= 512 runs -- well within the buffer. run_hi is exclusive here,
		 * matching sbm_run_decode_chunk.
		 */
		const uint64 *w = sbm_small_words(map);
		const size_t n = sbm_small_nwords(map);
		bool		open = false;
		uint64		cur_lo = 0,
					cur_hi = 0;
		size_t		i;

		it->count = 0;
		it->nruns = 0;
		it->next_run = 0;
		for (i = 0; i < n; i++)
		{
			const uint64 base = (uint64) i * 64;
			uint64		word = w[i];
			int			b;

			for (b = 0; b < 64; b++)
			{
				if ((word >> b) & 1u)
				{
					const uint64 bit = base + (uint64) b;

					if (open && cur_hi == bit)
					{
						cur_hi = bit + 1;
					}
					else
					{
						if (open)
						{
							it->run_lo[it->nruns] =
								cur_lo;
							it->run_hi[it->nruns++] =
								cur_hi;
						}
						cur_lo = bit;
						cur_hi = bit + 1;
						open = true;
					}
				}
			}
		}
		if (open)
		{
			it->run_lo[it->nruns] = cur_lo;
			it->run_hi[it->nruns++] = cur_hi;
		}
		return;
	}
	it->count = sbm_get_chunk_count(map);
	it->p = sbm_get_chunk_data(map, 0);
}

/*
 * Decompose one chunk (the one at it->p) into its runs, absolute bit
 * indices, into it->run_lo/run_hi.
 */
static void
sbm_run_decode_chunk(SbmRunIter *it, SbmIdx start)
{
	SbmChunk	chunk;
	SbmBitvec	desc;
	size_t		pos = 1;		/* payload-word cursor for MIXED slots */
	bool		open = false;
	uint64		cur_lo = 0,
				cur_hi = 0;
	size_t		v;

	sbm_chunk_init(&chunk, it->p + SBM_SIZEOF_OVERHEAD);
	it->nruns = 0;
	it->next_run = 0;

	if (sbm_chunk_is_rle(&chunk))
	{
		const size_t len = sbm_chunk_rle_get_length(&chunk);

		if (len > 0)
		{
			it->run_lo[0] = start;
			it->run_hi[0] = start + len;
			it->nruns = 1;
		}
		return;
	}

	/*
	 * Sparse: walk the 32 flags, coalescing adjacent set bits.  ONES is a
	 * full 64-bit run; MIXED decodes its payload word bit-by-bit (bounded, 64
	 * bits); ZEROS / NONE break any open run.
	 */
	desc = chunk.m_data[0];
	for (v = 0; v < SBM_FLAGS_PER_INDEX; v++)
	{
		const size_t flags = SBM_CHUNK_GET_FLAGS(desc, v);
		const uint64 base = start + (uint64) v * SBM_BITS_PER_VECTOR;

		if (flags == SBM_PAYLOAD_ONES)
		{
			if (open && cur_hi == base)
			{
				cur_hi = base + SBM_BITS_PER_VECTOR;
			}
			else
			{
				if (open)
				{
					it->run_lo[it->nruns] = cur_lo;
					it->run_hi[it->nruns++] = cur_hi;
				}
				cur_lo = base;
				cur_hi = base + SBM_BITS_PER_VECTOR;
				open = true;
			}
		}
		else if (flags == SBM_PAYLOAD_MIXED)
		{
			SbmBitvec	w = chunk.m_data[pos++];
			int			b;

			for (b = 0; b < SBM_BITS_PER_VECTOR; b++)
			{
				if ((w >> b) & 1u)
				{
					const uint64 bit = base + (uint64) b;

					if (open && cur_hi == bit)
					{
						cur_hi = bit + 1;
					}
					else
					{
						if (open)
						{
							it->run_lo[it->nruns] =
								cur_lo;
							it->run_hi[it->nruns++] =
								cur_hi;
						}
						cur_lo = bit;
						cur_hi = bit + 1;
						open = true;
					}
				}
			}
		}
		else
		{
			/* ZEROS / NONE: a gap ends any open run. */
			if (open)
			{
				it->run_lo[it->nruns] = cur_lo;
				it->run_hi[it->nruns++] = cur_hi;
				open = false;
			}
		}
	}
	if (open)
	{
		it->run_lo[it->nruns] = cur_lo;
		it->run_hi[it->nruns++] = cur_hi;
	}
}

/*
 * Yield the next physically-present run, exactly as decoded (per-chunk,
 * NOT coalesced across seams).  Returns false when exhausted.  This is
 * the raw producer; sbm_run_next layers seam-coalescing on top.
 */
static bool
sbm_run_next_raw(SbmRunIter *it, uint64 *lo, uint64 *hi)
{
	for (;;)
	{
		/* Drain runs already decoded from the current chunk. */
		if (it->next_run < it->nruns)
		{
			*lo = it->run_lo[it->next_run];
			*hi = it->run_hi[it->next_run];
			it->next_run++;
			return true;
		}
		/* Current chunk exhausted; decode the next one. */
		if (it->idx >= it->count)
		{
			return false;
		}
		{
			const SbmIdx start =
				sbm_load_idx((const uint8 *) it->p);
			SbmChunk	chunk;
			size_t		chunk_bytes;

			sbm_chunk_init(&chunk, it->p + SBM_SIZEOF_OVERHEAD);
			chunk_bytes =
				SBM_SIZEOF_OVERHEAD + sbm_chunk_get_size(&chunk);
			sbm_run_decode_chunk(it, start);
			it->p += chunk_bytes;
			it->idx++;
		}
		/* loop back to drain the freshly-decoded run list */
	}
}

/*
 * Yield the next MAXIMAL run.  Returns false when exhausted.
 *
 * Coalesces adjacent raw runs: once the current run [lo, hi) is in
 * hand, keep pulling the next raw run and, while it begins exactly
 * where the accumulated run ends (raw_lo == hi), absorb it by advancing
 * hi.  The first raw run that does NOT abut is stashed in the iterator
 * (it->have_peek) and returned on the next call, so nothing is dropped
 * and each raw run is examined once.  Raw runs are ascending and
 * disjoint within a map (per-chunk decode is ordered; chunks are
 * ordered), so a peeked run with raw_lo > hi genuinely starts a new
 * maximal run.  Cost stays O(raw runs) = O(chunks + physical sparse
 * runs); a giant RLE run is one raw run and one yielded run.
 */
static bool
sbm_run_next(SbmRunIter *it, uint64 *lo, uint64 *hi)
{
	uint64		cur_lo,
				cur_hi;

	if (it->have_peek)
	{
		cur_lo = it->peek_lo;
		cur_hi = it->peek_hi;
		it->have_peek = false;
	}
	else if (!sbm_run_next_raw(it, &cur_lo, &cur_hi))
	{
		return false;
	}
	for (;;)
	{
		uint64		nlo,
					nhi;

		if (!sbm_run_next_raw(it, &nlo, &nhi))
		{
			break;				/* no more raw runs; current run is final */
		}
		if (nlo == cur_hi)
		{
			cur_hi = nhi;		/* abuts: absorb and keep extending */
			continue;
		}
		/* Does not abut: stash for the next call and stop. */
		it->peek_lo = nlo;
		it->peek_hi = nhi;
		it->have_peek = true;
		break;
	}
	*lo = cur_lo;
	*hi = cur_hi;
	return true;
}

/*
 * The cardinality / set-algebra / hashing helpers below walk maps
 * run-by-run (see SbmRunIter) rather than bit-by-bit, so their
 * cost tracks the encoded size, not the popcount.  A 2^31-bit RLE run
 * is a single run.
 */

/*
 * Single lockstep pass over two maps' runs, accumulating the counts
 * every set-algebra cardinality wants: |a|, |b|, |a & b|, |a | b|.
 * Runs are maximal, ascending and non-overlapping within each map, so
 * a classic interval sweep is exact and O(runs_a + runs_b).
 */
static void
sbm_run_pair_counts(const Sbm *a, const Sbm *b, uint64 *cnt_a,
					uint64 *cnt_b, uint64 *inter, uint64 *uni)
{
	SbmRunIter	ia,
				ib;
	uint64		alo = 0,
				ahi = 0,
				blo = 0,
				bhi = 0;
	bool		have_a,
				have_b;
	uint64		ca = 0,
				cb = 0,
				ci = 0;

	sbm_run_iter_init(&ia, a);
	sbm_run_iter_init(&ib, b);
	have_a = sbm_run_next(&ia, &alo, &ahi);
	have_b = sbm_run_next(&ib, &blo, &bhi);

	/*
	 * Intersection by interval sweep: at each step add the overlap of the two
	 * active runs, then consume whichever ends first so the other can still
	 * overlap the consumed side's later runs.  Runs are ascending and
	 * disjoint within each map, so no overlap is double-counted.  Union
	 * follows from inclusion-exclusion: |a | b| = |a| + |b| - |a & b|.
	 * Cardinalities are accumulated once per run as it is consumed.
	 */
	while (have_a || have_b)
	{
		if (have_a && have_b)
		{
			const uint64 ov_lo = alo > blo ? alo : blo;
			const uint64 ov_hi = ahi < bhi ? ahi : bhi;

			if (ov_lo < ov_hi)
				ci += ov_hi - ov_lo;
		}
		if (have_a && (!have_b || ahi <= bhi))
		{
			ca += ahi - alo;
			have_a = sbm_run_next(&ia, &alo, &ahi);
		}
		else
		{
			cb += bhi - blo;
			have_b = sbm_run_next(&ib, &blo, &bhi);
		}
	}
	if (cnt_a)
		*cnt_a = ca;
	if (cnt_b)
		*cnt_b = cb;
	if (inter)
		*inter = ci;
	if (uni)
		*uni = ca + cb - ci;
}

size_t
sbm_union_cardinality(const Sbm *a, const Sbm *b)
{
	uint64		uni = 0;

	if (sbm_is_empty(a))
		return b ? sbm_cardinality(b) : 0;
	if (sbm_is_empty(b))
		return sbm_cardinality(a);
	sbm_run_pair_counts(a, b, NULL, NULL, NULL, &uni);
	return (size_t) uni;
}

size_t
sbm_intersection_cardinality(const Sbm *a, const Sbm *b)
{
	uint64		inter = 0;

	if (sbm_is_empty(a) || sbm_is_empty(b))
		return 0;
	sbm_run_pair_counts(a, b, NULL, NULL, &inter, NULL);
	return (size_t) inter;
}

size_t
sbm_difference_cardinality(const Sbm *a, const Sbm *b)
{
	uint64		ca = 0,
				inter = 0;

	if (sbm_is_empty(a))
		return 0;
	if (sbm_is_empty(b))
		return sbm_cardinality(a);
	sbm_run_pair_counts(a, b, &ca, NULL, &inter, NULL);
	return (size_t) (ca - inter);
}

bool
sbm_nonempty_difference(const Sbm *a, const Sbm *b)
{
	uint64		ca = 0,
				inter = 0;

	if (sbm_is_empty(a))
		return false;
	if (sbm_is_empty(b))
		return true;
	sbm_run_pair_counts(a, b, &ca, NULL, &inter, NULL);
	return ca > inter;
}

double
sbm_jaccard_index(const Sbm *a, const Sbm *b)
{
	uint64		inter = 0,
				uni = 0;

	if (sbm_is_empty(a) && sbm_is_empty(b))
		return 0.0;
	sbm_run_pair_counts(a, b, NULL, NULL, &inter, &uni);
	return uni == 0 ? 0.0 : (double) inter / (double) uni;
}

/* Ascending uint64 comparator for the bulk-insert sort below. */
static int
sbm_cmp_u64(const void *a, const void *b)
{
	return pg_cmp_u64(*(const uint64 *) a, *(const uint64 *) b);
}

/*
 * Defined further down with the in-place set ops; forward-declared so the
 * bulk builder can swap its freshly-emitted buffer into an existing map.
 */
static Sbm *sbm_replace_buffer(Sbm *dst, Sbm *result);

/*
 * Coalesce a sorted (ascending) index array into maximal half-open runs
 * [lo, hi), consecutive integers becoming one run, duplicates collapsing.
 * Writes the runs into run_lo/run_hi (each at most n long) and returns the
 * run count.  n must be > 0 and arr must be sorted ascending.
 */
static size_t
sbm_coalesce_sorted(const uint64 *arr, size_t n, uint64 *run_lo,
					uint64 *run_hi)
{
	size_t		nruns = 0;
	uint64		lo = arr[0];
	uint64		hi = arr[0] + 1;
	size_t		i;

	for (i = 1; i < n; i++)
	{
		const uint64 v = arr[i];

		if (v <= hi)
		{
			/* Duplicate (v < hi) or consecutive (v == hi): extend. */
			if (v == hi)
				hi = v + 1;
			continue;
		}
		run_lo[nruns] = lo;
		run_hi[nruns++] = hi;
		lo = v;
		hi = v + 1;
	}
	run_lo[nruns] = lo;
	run_hi[nruns++] = hi;
	return nruns;
}

/*
 * Build a fresh map that is the union of an existing map's set bits and a
 * list of ascending, non-overlapping runs, emitting the whole result in one
 * ordered pass through SbmEmitter -- the same machinery sbm_union and
 * sbm_add_range use.  This makes the merge O(#existing chunks + #runs)
 * regardless of where the new bits land.
 *
 * The per-element sbm_add_c path threads a cursor, so an ASCENDING bulk
 * build that only appends new chunks at the tail was already near-linear.
 * The quadratic case it does NOT cover is inserting sparse bits INTO an
 * already-large map: each insert byte-shifts the tail of the buffer
 * (O(#chunks) memmove), so N such inserts cost O(N * #chunks) -- measured
 * at ~37 s for 50k inserts into a 50k-chunk map, ~2750x the ~13 ms this
 * single-pass rebuild takes, and scaling ~4x per doubling (this path ~2x).
 *
 * Interval-union sweep of the map's run stream and the run array (identical
 * to sbm_xor's sweep but emitting wherever EITHER side is set).  Returns the
 * new buffer (caller swaps it in) or NULL on allocation failure.
 */
static Sbm *
sbm_union_runs(const Sbm *map, const uint64 *run_lo,
			   const uint64 *run_hi, size_t nruns)
{
	size_t		cap;
	Sbm		   *r;
	SbmEmitter	em;
	SbmRunIter	ia;
	uint64		alo = 0,
				ahi = 0;
	bool		have_a;
	size_t		bi = 0;
	uint64		pos = 0;
	bool		have_pos = false;

	/* Result upper bound: existing bytes plus ~24 per input run. */
	cap = sbm_is_empty(map) ? 0 : sbm_get_size(map);
	cap += nruns * 24 + 64;
	if (cap < 1024)
		cap = 1024;
	r = sbm_create(cap);

	memset(&em, 0, sizeof(em));
	em.resultp = &r;
	sbm_run_iter_init(&ia, map);
	have_a = sbm_run_next(&ia, &alo, &ahi);
	while (have_a || bi < nruns)
	{
		const bool	have_b = bi < nruns;
		uint64		blo = have_b ? run_lo[bi] : 0;
		uint64		bhi = have_b ? run_hi[bi] : 0;
		uint64		lo = have_a ? alo : blo;
		bool		in_a;
		bool		in_b;
		uint64		next = PG_UINT64_MAX;

		if (have_b && blo < lo)
			lo = blo;
		if (!have_pos || pos < lo)
		{
			pos = lo;
			have_pos = true;
		}
		in_a = have_a && pos >= alo && pos < ahi;
		in_b = have_b && pos >= blo && pos < bhi;
		if (have_a)
		{
			if (pos < alo && alo < next)
				next = alo;
			if (pos >= alo && ahi < next)
				next = ahi;
		}
		if (have_b)
		{
			if (pos < blo && blo < next)
				next = blo;
			if (pos >= blo && bhi < next)
				next = bhi;
		}

		/*
		 * Union: any span set in either side is emitted.  sbm_emit_run is the
		 * ordered emitter primitive sbm_add_run_grow wraps.
		 */
		if (in_a || in_b)
		{
			sbm_emit_run(&em, pos, next);
		}
		pos = next;
		if (have_a && pos >= ahi)
			have_a = sbm_run_next(&ia, &alo, &ahi);
		if (have_b && pos >= bhi)
			bi++;
	}
	sbm_emit_flush(&em);
	r = *em.resultp;
	sbm_coalesce_map(r);
	return r;
}

/*
 * Shared bulk-insert core for sbm_add_many / sbm_add_many_grow.  Sorts a
 * private copy of the input, coalesces it into runs, unions those runs with
 * the map's existing bits via the ordered emitter in one pass, and swaps the
 * result into *mapp with sbm_replace_buffer (which grows, demotes to
 * small-set mode when the result fits, and frees the scratch buffer).
 */
static bool
sbm_add_many_core(Sbm **mapp, const uint64 *arr, size_t n, bool may_grow)
{
	uint64	   *sorted;
	uint64	   *run_lo;
	uint64	   *run_hi;
	size_t		nruns;
	Sbm		   *result;

	sorted = (uint64 *) palloc(n * sizeof(uint64));
	run_lo = (uint64 *) palloc(n * sizeof(uint64));
	run_hi = (uint64 *) palloc(n * sizeof(uint64));
	memcpy(sorted, arr, n * sizeof(uint64));
	qsort(sorted, n, sizeof(uint64), sbm_cmp_u64);
	nruns = sbm_coalesce_sorted(sorted, n, run_lo, run_hi);
	pfree(sorted);

	result = sbm_union_runs(*mapp, run_lo, run_hi, nruns);
	pfree(run_lo);
	pfree(run_hi);

	/*
	 * Without growth permission the caller's pointer must stay valid, so
	 * refuse rather than let sbm_replace_buffer repalloc (and move) it.
	 */
	if (!may_grow && result->m_data_used > sbm_cap(*mapp))
	{
		sbm_free(result);
		errno = ENOSPC;
		return false;
	}
	*mapp = sbm_replace_buffer(*mapp, result);
	return true;
}

bool
sbm_add_many(Sbm *map, const uint64 *arr, size_t n)
{
	if (map == NULL || (arr == NULL && n > 0))
		return false;
	if (n == 0)
		return true;
	if (n == 1)
		return sbm_add(map, arr[0]) != SBM_IDX_MAX;

	/*
	 * Bulk build: coalesce the input into runs and union them with the
	 * existing bits in one O(N + #chunks) emitter pass, which stays linear
	 * even when the new bits land inside an existing map.  This variant must
	 * not move the caller's map, so a result that outgrows the buffer fails
	 * with ENOSPC; sbm_add_many_grow() grows instead.
	 */
	return sbm_add_many_core(&map, arr, n, false);
}

/*
 * Growing bulk insert: like sbm_add_many but takes Sbm** so
 * sbm_replace_buffer may relocate the buffer when the emitted result
 * outgrows it, instead of failing.
 */
bool
sbm_add_many_grow(Sbm **map, const uint64 *arr, size_t n)
{
	if (map == NULL || (arr == NULL && n > 0))
		return false;
	if (n == 0)
		return true;
	if (*map == NULL)
		*map = sbm_create(0);
	if (n == 1)
		return sbm_add_grow(map, arr[0]) != SBM_IDX_MAX;
	return sbm_add_many_core(map, arr, n, true);
}

/*
 * Set every bit in [lo, hi) on a result map that is being built through
 * the ordered emitter (SbmEmitter).  Runs arrive ascending and
 * non-overlapping, so this is O(output chunks): whole chunks go out as
 * RLE and only the sub-chunk head/tail touch the words path, so the cost
 * tracks the encoded size of the run, not its length in bits.
 */
static void
sbm_add_run_grow(SbmEmitter *e, uint64 lo, uint64 hi)
{
	sbm_emit_run(e, lo, hi);
}

void
sbm_to_array(const Sbm *map, uint64 *out, size_t *n_out)
{
	size_t		cap;
	size_t		written = 0;
	uint64		i = SBM_IDX_MAX;
	SbmCursor	cur = SBM_CURSOR_INIT;

	if (n_out == NULL)
		return;
	cap = (out == NULL) ? 0 : *n_out;

	if (out == NULL)
	{
		/* Query: just count. */
		*n_out = sbm_is_empty(map) ? 0 : sbm_cardinality(map);
		return;
	}

	while (written < cap &&
		   (i = sbm_next_member(map, i, &cur)) != SBM_IDX_MAX)
		out[written++] = i;
	*n_out = written;
}

/* -------------------------------------------------------------------
 * Range ops, symmetric difference, set-op synonyms, constructors,
 * hashing and ordering, destructive iteration
 * -------------------------------------------------------------------
 */

/*
 * add_range fast path: OR the literal run [lo, hi) into an existing
 * map's run stream and re-emit, instead of touching every bit.
 *
 * The old body was `for (i = lo; i < hi; i++) sbm_add(map, i)`, which is
 * O(hi - lo) -- a [0, 1e6) add cost ~14 ms even though the result is a
 * single 24-byte RLE run.  Here add_range is `map = union(map, [lo,hi))`:
 * we merge the map's existing (ascending, maximal) runs with the single
 * literal run using interval-union semantics, then feed each merged run
 * to the same ordered emitter set-ops already use (sbm_add_run_grow ->
 * sbm_emit_run), which lays whole output chunks as RLE and only touches
 * the sub-chunk head/tail bit-by-bit.  Cost is O(existing_chunks +
 * run_chunks), and a giant run stays a ~24-byte RLE map.
 *
 * The emitter demands runs delivered ascending and non-overlapping, so
 * the interval merge below coalesces the literal run against the map's
 * runs (and against each other across the seam) before emitting.
 *
 * Contract preserved from the old loop: the map is written in place and
 * does NOT relocate (sbm_add_range returns bool, so a relocated struct
 * pointer could not reach the caller).  If the merged result does not
 * fit the map's current buffer, we fail with errno = ENOSPC -- exactly
 * as the old per-bit loop's sbm_add did on a too-small SBM_OWNED_CONTIGUOUS
 * map.
 */
bool
sbm_add_range(Sbm *map, uint64 lo, uint64 hi)
{
	SbmEmitter	em;
	SbmRunIter	it;
	Sbm		   *r;
	size_t		cap;
	size_t		result_size;
	uint64		rlo = 0,
				rhi = 0;
	bool		have_run;

	/* Pending merged run being accumulated (interval union). */
	uint64		cur_lo = 0,
				cur_hi = 0;
	bool		have_cur = false;

	/*
	 * Track whether the whole merged result is a single run and, if so, its
	 * span -- so a run within one chunk starting at a chunk boundary can be
	 * laid down as a compact RLE chunk (matching the old per-bit loop's
	 * small-mode->sbm_promote_run_as_rle footprint) instead of the emitter's
	 * sparse encoding, which never RLEs a sub-chunk run.
	 */
	size_t		nemitted = 0;
	uint64		first_lo = 0,
				first_hi = 0;

	/*
	 * The single literal run [lo, hi), consumed once when the ascending merge
	 * reaches its position.
	 */
	bool		lit_pending;

	if (map == NULL || lo >= hi)
		return lo >= hi;		/* empty range = OK */
	lit_pending = true;

	sbm_check_invariants(map);

	/*
	 * Result capacity upper bound: the map's current encoding plus the new
	 * run's worst-case chunk footprint.  A run spanning K output chunks needs
	 * at most K * (SBM_SIZEOF_OVERHEAD + one descriptor + two vectors); RLE
	 * collapses most of that, so this is generous.
	 */
	cap = sbm_get_size(map) + 128;
	if (cap < 1024)
		cap = 1024;
	r = sbm_create(cap);

	memset(&em, 0, sizeof(em));
	em.resultp = &r;
	sbm_run_iter_init(&it, map);
	have_run = sbm_run_next(&it, &rlo, &rhi);

	/*
	 * Ascending interval-union merge of the map's runs with the single
	 * literal run.  At each step pick the next-starting interval among
	 * {current map run, literal run}, then either extend the pending merged
	 * run (overlap or abut) or flush it and start a new one.
	 */
	for (;;)
	{
		uint64		nlo,
					nhi;
		bool		take_lit;

		if (!have_run && !lit_pending)
			break;
		if (have_run && lit_pending)
			take_lit = (lo <= rlo);
		else
			take_lit = lit_pending;
		if (take_lit)
		{
			nlo = lo;
			nhi = hi;
			lit_pending = false;
		}
		else
		{
			nlo = rlo;
			nhi = rhi;
			have_run = sbm_run_next(&it, &rlo, &rhi);
		}
		if (!have_cur)
		{
			cur_lo = nlo;
			cur_hi = nhi;
			have_cur = true;
		}
		else if (nlo <= cur_hi)
		{
			/* Overlap or abut: extend. */
			if (nhi > cur_hi)
				cur_hi = nhi;
		}
		else
		{
			/* Gap: flush the pending run and open a new one. */
			if (nemitted == 0)
			{
				first_lo = cur_lo;
				first_hi = cur_hi;
			}
			nemitted++;
			sbm_add_run_grow(&em, cur_lo, cur_hi);
			cur_lo = nlo;
			cur_hi = nhi;
		}
	}
	if (have_cur)
	{
		if (nemitted == 0)
		{
			first_lo = cur_lo;
			first_hi = cur_hi;
		}
		nemitted++;
		sbm_add_run_grow(&em, cur_lo, cur_hi);
	}
	sbm_emit_flush(&em);
	r = *em.resultp;
	sbm_coalesce_map(r);

	/*
	 * Footprint parity with the old loop: a single run that starts on a chunk
	 * boundary but does not fill the chunk (0 < span < 2048) is emitted by
	 * sbm_emit_run as a sparse chunk (the emitter must not advertise RLE
	 * capacity it cannot fill, since a later source could land in that chunk
	 * -- but here the map is complete, so that concern does not apply).  The
	 * old loop routed such a run through sbm_promote_run_as_rle and got a
	 * 24-byte RLE chunk.  Reproduce it: if the whole result is one run
	 * [first_lo, first_hi) that lives in a single chunk aligned to its start,
	 * replace the buffer with one RLE chunk.  Multi-chunk runs already went
	 * out with an RLE body.
	 */
	if (nemitted == 1 &&
		(first_lo % SBM_CHUNK_MAX_CAPACITY) == 0 &&
		(first_hi - first_lo) < SBM_CHUNK_MAX_CAPACITY)
	{
		SbmChunk	chunk;
		const SbmIdx cstart = (SbmIdx) first_lo;
		const size_t run_len = (size_t) (first_hi - first_lo);
		const size_t need = SBM_SIZEOF_OVERHEAD + SBM_SIZEOF_OVERHEAD +
			sizeof(SbmBitvec);

		if (need <= sbm_cap(r))
		{
			sbm_set_chunk_count(r, 1);
			sbm_store_idx(&r->m_data[SBM_SIZEOF_OVERHEAD], cstart);
			sbm_chunk_init(&chunk,
						   &r->m_data[SBM_SIZEOF_OVERHEAD +
									  SBM_SIZEOF_OVERHEAD]);
			chunk.m_data[0] = 0;
			sbm_chunk_set_rle(&chunk);
			sbm_chunk_rle_set_capacity(&chunk,
									   SBM_CHUNK_MAX_CAPACITY);
			sbm_chunk_rle_set_length(&chunk, run_len);
			r->m_data_used = need;
		}
	}

	/*
	 * Copy the merged result into the map's existing buffer in place. Do NOT
	 * use sbm_replace_buffer here: it grows via sbm_set_data_size, which
	 * relocates an SBM_OWNED_CONTIGUOUS struct and returns a new pointer that
	 * sbm_add_range cannot hand back.  Refuse (ENOSPC) if the result does not
	 * fit, matching the old loop's sbm_add behaviour.
	 */
	result_size = r->m_data_used;
	if (result_size > sbm_cap(map))
	{
		sbm_free(r);
		errno = ENOSPC;
		return false;
	}
	memcpy(map->m_data, r->m_data, result_size);
	map->m_data_used = result_size;

	/*
	 * Membership changed: the run-emitter path swaps the buffer in place (it
	 * does NOT route through sbm_add_dispatch or sbm_replace_buffer, which
	 * are the other invalidation points), so invalidate the lazy cardinality
	 * cache here.
	 */
	sbm_card_invalidate(map);
	sbm_free(r);

	/*
	 * Keep a near-zero result in small mode (footprint parity with the old
	 * loop, whose per-bit sbm_add demotes).
	 */
	sbm_try_demote(map);
	return true;
}

bool
sbm_remove_range(Sbm *map, uint64 lo, uint64 hi)
{
	uint64		i;

	if (map == NULL || lo >= hi)
		return lo >= hi;
	for (i = lo; i < hi; i++)
	{
		if (sbm_remove(map, i) == SBM_IDX_MAX)
		{
			return false;
		}
	}
	return true;
}

Sbm *
sbm_xor(const Sbm *a, const Sbm *b)
{
	size_t		cap;
	Sbm		   *r;
	SbmEmitter	em;
	SbmRunIter	ia,
				ib;
	uint64		alo = 0,
				ahi = 0,
				blo = 0,
				bhi = 0;
	bool		have_a,
				have_b;

	/*
	 * pos = left edge of the not-yet-emitted portion of the current a/b runs;
	 * overlaps cancel, gaps in exactly one survive.
	 */
	uint64		pos = 0;
	bool		have_pos = false;

	if (sbm_is_empty(a) && sbm_is_empty(b))
		return NULL;
	if (sbm_is_empty(a))
		return sbm_copy(b);
	if (sbm_is_empty(b))
		return sbm_copy(a);

	/* Allocate a result big enough for the union (upper bound). */
	cap = sbm_get_capacity(a) + sbm_get_capacity(b);
	r = sbm_create(cap > 1024 ? cap : 1024);

	/*
	 * Walk both maps run-by-run and emit the symmetric-difference runs (bits
	 * set in exactly one map).  Cost tracks the encoded size of the operands,
	 * not their popcount, so a 2^31-bit run is one iteration rather than
	 * 2^31.  Each survivor is emitted whole through the ordered emitter, so a
	 * giant run costs O(chunks).
	 */
	memset(&em, 0, sizeof(em));
	em.resultp = &r;
	sbm_run_iter_init(&ia, a);
	sbm_run_iter_init(&ib, b);
	have_a = sbm_run_next(&ia, &alo, &ahi);
	have_b = sbm_run_next(&ib, &blo, &bhi);
	while (have_a || have_b)
	{
		/* The next boundary among the two active runs. */
		uint64		lo = have_a ? alo : blo;
		bool		in_a;
		bool		in_b;
		uint64		next = PG_UINT64_MAX;

		if (have_b && blo < lo)
			lo = blo;
		if (!have_pos || pos < lo)
		{
			pos = lo;
			have_pos = true;
		}
		in_a = have_a && pos >= alo && pos < ahi;
		in_b = have_b && pos >= blo && pos < bhi;
		/* End of the current homogeneous segment. */
		if (have_a)
		{
			if (pos < alo && alo < next)
				next = alo;
			if (pos >= alo && ahi < next)
				next = ahi;
		}
		if (have_b)
		{
			if (pos < blo && blo < next)
				next = blo;
			if (pos >= blo && bhi < next)
				next = bhi;
		}
		if (in_a != in_b)
		{
			/* Bits [pos, next) are in exactly one map. */
			sbm_add_run_grow(&em, pos, next);
		}
		pos = next;
		if (have_a && pos >= ahi)
			have_a = sbm_run_next(&ia, &alo, &ahi);
		if (have_b && pos >= bhi)
			have_b = sbm_run_next(&ib, &blo, &bhi);
	}
	sbm_emit_flush(&em);
	r = *em.resultp;
	sbm_coalesce_map(r);
	if (sbm_is_empty(r))
	{
		sbm_free(r);
		return NULL;
	}
	sbm_try_demote(r);
	return r;
}

Sbm *
sbm_or(const Sbm *a, const Sbm *b)
{
	return sbm_union(a, b);
}

Sbm *
sbm_and(const Sbm *a, const Sbm *b)
{
	return sbm_intersection(a, b);
}

Sbm *
sbm_andnot(const Sbm *a, const Sbm *b)
{
	return sbm_difference(a, b);
}

Sbm *
sbm_extract_range(const Sbm *map, uint64 lo, uint64 hi)
{
	size_t		cap;
	Sbm		   *r;
	SbmEmitter	em;
	SbmRunIter	it;
	uint64		rlo = 0,
				rhi = 0;

	if (map == NULL || sbm_is_empty(map) || lo >= hi)
		return NULL;

	/*
	 * Estimate result capacity from the input -- worst case is the same
	 * shape, capped to the requested range size.
	 */
	cap = sbm_get_size(map) + 64;
	if (cap < 1024)
		cap = 1024;
	r = sbm_create(cap);

	/*
	 * Walk set-bit runs and add each run's intersection with [lo, hi).
	 * Run-based, so a 2^31-bit run outside the window costs one iteration
	 * rather than 2^31 bit lookups, and a run inside the window is emitted
	 * whole in O(chunks) via the ordered emitter.
	 */
	memset(&em, 0, sizeof(em));
	em.resultp = &r;
	sbm_run_iter_init(&it, map);
	while (sbm_run_next(&it, &rlo, &rhi))
	{
		uint64		clip_lo;
		uint64		clip_hi;

		if (rhi <= lo)
			continue;
		if (rlo >= hi)
			break;				/* runs are ascending; nothing more overlaps */
		clip_lo = rlo < lo ? lo : rlo;
		clip_hi = rhi > hi ? hi : rhi;
		sbm_add_run_grow(&em, clip_lo, clip_hi);
	}
	sbm_emit_flush(&em);
	r = *em.resultp;
	sbm_coalesce_map(r);

	if (sbm_is_empty(r))
	{
		sbm_free(r);
		return NULL;
	}
	sbm_try_demote(r);
	return r;
}

size_t
sbm_xor_cardinality(const Sbm *a, const Sbm *b)
{
	uint64		inter = 0,
				uni = 0;

	if (sbm_is_empty(a) && sbm_is_empty(b))
		return 0;
	if (sbm_is_empty(a))
		return sbm_cardinality(b);
	if (sbm_is_empty(b))
		return sbm_cardinality(a);
	sbm_run_pair_counts(a, b, NULL, NULL, &inter, &uni);
	return (size_t) (uni - inter);
}

Sbm *
sbm_create_singleton(uint64 idx)
{
	Sbm		   *m = sbm_create(0);
	uint64		rc PG_USED_FOR_ASSERTS_ONLY;

	/* one member always fits in the default buffer */
	rc = sbm_add(m, idx);
	Assert(rc == idx);
	return m;
}

Sbm *
sbm_create_from_range(uint64 lo, uint64 hi)
{
	Sbm		   *m = sbm_create(0);

	while (!sbm_add_range(m, lo, hi))
		m = sbm_set_data_size(m, NULL, sbm_get_capacity(m) * 2);
	return m;
}

Sbm *
sbm_create_from_array(const uint64 *arr, size_t n)
{
	Sbm		   *m = sbm_create(0);

	if (arr == NULL && n > 0)
	{
		sbm_free(m);
		errno = EINVAL;
		return NULL;
	}
	(void) sbm_add_many_grow(&m, arr, n);
	return m;
}

uint64
sbm_hash(const Sbm *map)
{
	/*
	 * FNV-1a 64-bit over the sequence of maximal set-bit runs. Content-based
	 * (encoding-independent): two maps that compare equal under sbm_equals()
	 * decompose into the identical run sequence and so hash to the same
	 * value.  Hashing runs rather than individual bits keeps this O(runs), so
	 * a 2^31-bit run costs one iteration instead of 2^31.
	 */
	uint64		h = UINT64CONST(0xcbf29ce484222325);
	SbmRunIter	it;
	uint64		lo = 0,
				hi = 0;
	int			b;

	if (sbm_is_empty(map))
		return h;
	sbm_run_iter_init(&it, map);
	while (sbm_run_next(&it, &lo, &hi))
	{
		/* Mix both endpoints of the run (8 bytes each). */
		for (b = 0; b < 8; b++)
		{
			h ^= (lo >> (b * 8)) & UINT64CONST(0xff);
			h *= UINT64CONST(0x100000001b3);
		}
		for (b = 0; b < 8; b++)
		{
			h ^= (hi >> (b * 8)) & UINT64CONST(0xff);
			h *= UINT64CONST(0x100000001b3);
		}
	}
	return h;
}

/*
 * Small-set fast paths for the hot comparators.
 *
 * The run-iterator decode in sbm_run_iter_init() zeroes a multi-kilobyte
 * SbmRunIter and walks every bit of a small map, which is far too expensive
 * for the tiny sets (a handful of indexes) that dominate callers like the
 * planner's relid set-algebra, where these comparators run millions of
 * times.  When both operands are in small (word-array) mode we compare the
 * words directly with no allocation, exactly like the historical Bitmapset
 * word loops.  Callers fall back to the run-iterator only when a map is not
 * small.
 */
static inline bool
sbm_both_small(const Sbm *a, const Sbm *b)
{
	return sbm_is_small(a) && sbm_is_small(b);
}

/* Equality of two small maps by direct word comparison. */
static bool
sbm_small_equals(const Sbm *a, const Sbm *b)
{
	const uint64 *wa = sbm_small_words(a);
	const uint64 *wb = sbm_small_words(b);
	size_t		na = sbm_small_nwords(a);
	size_t		nb = sbm_small_nwords(b);
	size_t		common = Min(na, nb);
	size_t		i;

	for (i = 0; i < common; i++)
		if (wa[i] != wb[i])
			return false;
	/* any extra words in the longer map must be all-zero */
	for (; i < na; i++)
		if (wa[i] != 0)
			return false;
	for (; i < nb; i++)
		if (wb[i] != 0)
			return false;
	return true;
}

/* Nonempty intersection of two small maps. */
static bool
sbm_small_overlap(const Sbm *a, const Sbm *b)
{
	const uint64 *wa = sbm_small_words(a);
	const uint64 *wb = sbm_small_words(b);
	size_t		common = Min(sbm_small_nwords(a), sbm_small_nwords(b));

	for (size_t i = 0; i < common; i++)
		if ((wa[i] & wb[i]) != 0)
			return true;
	return false;
}

/* Subset relation of two small maps by direct word comparison. */
static SbmSubsetRelation
sbm_small_subset_compare(const Sbm *a, const Sbm *b)
{
	const uint64 *wa = sbm_small_words(a);
	const uint64 *wb = sbm_small_words(b);
	size_t		na = sbm_small_nwords(a);
	size_t		nb = sbm_small_nwords(b);
	size_t		common = Min(na, nb);
	bool		a_subset_b = true;	/* every bit of a is in b */
	bool		b_subset_a = true;	/* every bit of b is in a */
	size_t		i;

	for (i = 0; i < common; i++)
	{
		uint64		aw = wa[i];
		uint64		bw = wb[i];

		if (aw & ~bw)
			a_subset_b = false;
		if (bw & ~aw)
			b_subset_a = false;
	}
	/* extra words of the longer map hold bits the shorter one lacks */
	for (; i < na; i++)
		if (wa[i] != 0)
			a_subset_b = false;
	for (; i < nb; i++)
		if (wb[i] != 0)
			b_subset_a = false;

	if (a_subset_b && b_subset_a)
		return SBM_REL_EQUAL;
	if (a_subset_b)
		return SBM_REL_SUBSET_A;
	if (b_subset_a)
		return SBM_REL_SUBSET_B;
	return SBM_REL_DIFFERENT;
}

bool
sbm_equals(const Sbm *a, const Sbm *b)
{
	const bool	a_empty = (a == NULL) || sbm_is_empty(a);
	const bool	b_empty = (b == NULL) || sbm_is_empty(b);
	SbmRunIter	ia,
				ib;
	uint64		alo = 0,
				ahi = 0,
				blo = 0,
				bhi = 0;
	bool		have_a,
				have_b;

	if (a_empty && b_empty)
		return true;
	if (a_empty != b_empty)
		return false;

	/* small/small fast path: compare words directly, no allocation */
	if (sbm_both_small(a, b))
		return sbm_small_equals(a, b);

	/*
	 * Two sets are equal iff their maximal-run decompositions are the
	 * identical interval sequence.  Walk both run streams in lockstep (see
	 * SbmRunIter), so a 2^31-bit run is one comparison rather than 2^31 bit
	 * lookups.
	 */
	sbm_run_iter_init(&ia, a);
	sbm_run_iter_init(&ib, b);
	have_a = sbm_run_next(&ia, &alo, &ahi);
	have_b = sbm_run_next(&ib, &blo, &bhi);
	while (have_a && have_b)
	{
		if (alo != blo || ahi != bhi)
			return false;
		have_a = sbm_run_next(&ia, &alo, &ahi);
		have_b = sbm_run_next(&ib, &blo, &bhi);
	}
	return have_a == have_b;
}

int
sbm_compare(const Sbm *a, const Sbm *b)
{
	/*
	 * Lexicographic order on the ascending member sequences: at the first
	 * index where the two sequences differ, the map with the smaller index
	 * sorts first; if one sequence is a proper prefix of the other, the
	 * shorter one sorts first.  Walk both run streams (see SbmRunIter) with a
	 * cursor into the current run of each, advancing over shared prefixes a
	 * whole run at a time, so a 2^31-bit run costs O(chunks) rather than
	 * O(cardinality).
	 */
	SbmRunIter	ia,
				ib;
	uint64		alo = 0,
				ahi = 0,
				blo = 0,
				bhi = 0;
	bool		have_a,
				have_b;
	uint64		pa,
				pb;

	sbm_run_iter_init(&ia, a);
	sbm_run_iter_init(&ib, b);
	have_a = sbm_run_next(&ia, &alo, &ahi);
	have_b = sbm_run_next(&ib, &blo, &bhi);
	/* pa/pb are the next unconsumed member of the current a/b run. */
	pa = alo;
	pb = blo;
	while (have_a && have_b)
	{
		uint64		a_rem;
		uint64		b_rem;
		uint64		step;

		if (pa < pb)
			return -1;
		if (pa > pb)
			return 1;

		/*
		 * pa == pb: both runs share consecutive members up to the shorter
		 * run's end; skip that common prefix at once.
		 */
		a_rem = ahi - pa;
		b_rem = bhi - pb;
		step = a_rem < b_rem ? a_rem : b_rem;
		pa += step;
		pb += step;
		if (pa >= ahi)
		{
			have_a = sbm_run_next(&ia, &alo, &ahi);
			pa = alo;
		}
		if (pb >= bhi)
		{
			have_b = sbm_run_next(&ib, &blo, &bhi);
			pb = blo;
		}
	}
	if (!have_a && !have_b)
		return 0;
	return !have_a ? -1 : 1;	/* shorter sequence sorts first */
}

SbmSubsetRelation
sbm_subset_compare(const Sbm *a, const Sbm *b)
{
	bool		a_subset_b = true;	/* every bit in a is in b */
	bool		b_subset_a = true;	/* every bit in b is in a */
	SbmRunIter	ia,
				ib;
	uint64		alo = 0,
				ahi = 0,
				blo = 0,
				bhi = 0;
	bool		have_a,
				have_b;

	/* pos = left edge of the not-yet-classified region. */
	uint64		pos = 0;
	bool		have_pos = false;

	/*
	 * Interval sweep over the two run streams (see SbmRunIter): a span
	 * present in exactly one map witnesses that map is not a subset of the
	 * other.  Once both witnesses fire the answer is DIFFERENT.  Run-based,
	 * so a 2^31-bit run costs O(chunks) rather than O(cardinality).
	 */
	/* small/small fast path: compare words directly, no allocation */
	if (sbm_both_small(a, b))
		return sbm_small_subset_compare(a, b);

	sbm_run_iter_init(&ia, a);
	sbm_run_iter_init(&ib, b);
	have_a = sbm_run_next(&ia, &alo, &ahi);
	have_b = sbm_run_next(&ib, &blo, &bhi);
	while (have_a || have_b)
	{
		uint64		lo = have_a ? alo : blo;
		bool		in_a;
		bool		in_b;
		uint64		next = PG_UINT64_MAX;

		if (have_b && blo < lo)
			lo = blo;
		if (!have_pos || pos < lo)
		{
			pos = lo;
			have_pos = true;
		}
		in_a = have_a && pos >= alo && pos < ahi;
		in_b = have_b && pos >= blo && pos < bhi;
		/* End of the current homogeneous segment. */
		if (have_a)
		{
			if (pos < alo && alo < next)
				next = alo;
			if (pos >= alo && ahi < next)
				next = ahi;
		}
		if (have_b)
		{
			if (pos < blo && blo < next)
				next = blo;
			if (pos >= blo && bhi < next)
				next = bhi;
		}
		if (in_a && !in_b)
			a_subset_b = false; /* a has a bit b doesn't */
		else if (in_b && !in_a)
			b_subset_a = false; /* b has a bit a doesn't */
		if (!a_subset_b && !b_subset_a)
			return SBM_REL_DIFFERENT;
		pos = next;
		if (have_a && pos >= ahi)
			have_a = sbm_run_next(&ia, &alo, &ahi);
		if (have_b && pos >= bhi)
			have_b = sbm_run_next(&ib, &blo, &bhi);
	}
	if (a_subset_b && b_subset_a)
		return SBM_REL_EQUAL;
	if (a_subset_b)
		return SBM_REL_SUBSET_A;
	return SBM_REL_SUBSET_B;
}

/*
 * Subset tests are the one-sided case of sbm_subset_compare, which already
 * returns as soon as both directions are disproved; a run-based sweep keeps
 * them O(chunks) rather than O(members).
 */
bool
sbm_is_subset(const Sbm *a, const Sbm *b)
{
	const SbmSubsetRelation r = sbm_subset_compare(a, b);

	return r == SBM_REL_EQUAL || r == SBM_REL_SUBSET_A;
}

bool
sbm_is_superset(const Sbm *a, const Sbm *b)
{
	return sbm_is_subset(b, a);
}

bool
sbm_overlap(const Sbm *a, const Sbm *b)
{
	SbmRunIter	ia,
				ib;
	uint64		alo = 0,
				ahi = 0,
				blo = 0,
				bhi = 0;
	bool		have_a,
				have_b;

	/* small/small fast path: AND words directly, no allocation */
	if (sbm_both_small(a, b))
		return sbm_small_overlap(a, b);

	/*
	 * Sweep the two run streams, returning at the first pair of runs that
	 * overlap; otherwise advance whichever run ends first.
	 */
	sbm_run_iter_init(&ia, a);
	sbm_run_iter_init(&ib, b);
	have_a = sbm_run_next(&ia, &alo, &ahi);
	have_b = sbm_run_next(&ib, &blo, &bhi);
	while (have_a && have_b)
	{
		if (alo < bhi && blo < ahi)
			return true;
		if (ahi <= bhi)
			have_a = sbm_run_next(&ia, &alo, &ahi);
		else
			have_b = sbm_run_next(&ib, &blo, &bhi);
	}
	return false;
}

uint64
sbm_pop_first(Sbm *map)
{
	uint64		lowest;

	if (sbm_is_empty(map))
		return SBM_IDX_MAX;
	lowest = sbm_next_member(map, SBM_IDX_MAX, NULL);
	if (lowest == SBM_IDX_MAX)
		return SBM_IDX_MAX;
	if (sbm_remove(map, lowest) == SBM_IDX_MAX)
	{
		/*
		 * Should never happen on a populated map (remove only fails on ENOSPC
		 * for chunk separation, and we're removing not adding).
		 */
		return SBM_IDX_MAX;
	}
	return lowest;
}

uint64
sbm_pop_last(Sbm *map)
{
	uint64		highest;

	if (sbm_is_empty(map))
		return SBM_IDX_MAX;
	highest = sbm_prev_member(map, SBM_IDX_MAX, NULL);
	if (highest == SBM_IDX_MAX)
		return SBM_IDX_MAX;
	if (sbm_remove(map, highest) == SBM_IDX_MAX)
		return SBM_IDX_MAX;
	return highest;
}

/* -------------------------------------------------------------------
 * In-place set operations.  These mutate `dst` and return it (or a
 * possibly-relocated pointer if dst grew).
 * -------------------------------------------------------------------
 */

/*
 * In-place set ops are implemented as "compute via the chunk-pair-walk
 * in sbm_union/sbm_intersection/sbm_difference, then memcpy the result's
 * bytes back into dst's buffer".  This delegates the actual merge to
 * the chunk-aware out-of-place version, paying one allocation for the
 * temporary result.  An alternative would be a two-pointer chunk walk
 * that writes directly into dst's buffer; that's a substantial refactor
 * with minimal speedup over the current approach (sbm_union's own walk
 * is already chunk-aware and the memcpy is a single block copy).
 */
static Sbm *
sbm_replace_buffer(Sbm *dst, Sbm *result)
{
	size_t		result_size;

	if (result == NULL)
	{
		/* Empty result -- clear dst. */
		sbm_clear(dst);
		return dst;
	}
	result_size = result->m_data_used;
	if (sbm_cap(dst) < result_size)
	{
		Sbm		   *grown = sbm_set_data_size(dst, NULL, result_size + 64);

		dst = grown;
	}
	memcpy(dst->m_data, result->m_data, result_size);
	dst->m_data_used = result_size;
	sbm_card_invalidate(dst);
	sbm_free(result);
	sbm_try_demote(dst);
	return dst;
}

/* -------------------------------------------------------------------
 * Embedded maps: an Sbm whose data buffer lives at a fixed offset inside a
 * larger caller-owned palloc block (for example a Bitmapset node of the
 * shape { NodeTag; Sbm sbm; uint8 data[] }).  Growing such a map must
 * repalloc the *enclosing* block, which may relocate it, so the growing
 * entry points take the enclosing block pointer and its data offset and
 * return the (possibly moved) enclosing block; the map is reanchored into
 * the moved buffer internally.  The map itself stores no back-pointer to
 * the enclosing block (that would dangle the moment the block moved), so
 * these are the only operations allowed to grow an embedded map.
 * -------------------------------------------------------------------
 */

/*
 * Grow the embedded map so its buffer has room for `needed` total bytes.
 *
 * The Sbm lives at `outer + sbm_offset` and its buffer at `outer +
 * data_offset` inside the caller's palloc block.  Growing repallocs `outer`,
 * which may relocate the whole block -- including the Sbm itself -- so this
 * both reanchors the map's buffer and returns the possibly-moved `outer`;
 * the caller must recompute its own Sbm pointer from the return value (the
 * public entry points below do).
 */
static void *
sbm_grow_embedded(void *outer, size_t sbm_offset, size_t data_offset,
				  size_t needed)
{
	Sbm		   *dst = (Sbm *) ((uint8 *) outer + sbm_offset);
	size_t		oldcap = sbm_cap(dst);
	size_t		newcap;

	if (needed <= oldcap)
		return outer;

	/* Geometric growth with an 8-byte-aligned capacity, like sbm_create. */
	newcap = oldcap * 2;
	if (newcap < needed)
		newcap = needed;
	newcap = (newcap + 7u) & ~(size_t) 7;

	outer = repalloc(outer, data_offset + newcap);
	dst = (Sbm *) ((uint8 *) outer + sbm_offset);	/* block may have moved */
	sbm_reanchor(dst, (uint8 *) outer + data_offset);
	memset(dst->m_data + oldcap, 0, newcap - oldcap);
	sbm_set_cap_kind(dst, newcap, SBM_WRAPPED);
	return outer;
}

/*
 * Copy a freshly-computed `result` map into the embedded map, growing the
 * enclosing `outer` block if the result does not fit.  Frees `result`.
 * Returns the possibly-moved `outer`.  A NULL/empty result clears the map.
 *
 * This is the embedded analogue of sbm_replace_buffer(): instead of
 * reallocating the map's own block it grows the caller's enclosing object.
 */
static void *
sbm_replace_buffer_embedded(void *outer, size_t sbm_offset, size_t data_offset,
							Sbm *result)
{
	Sbm		   *dst = (Sbm *) ((uint8 *) outer + sbm_offset);
	size_t		result_size;

	/*
	 * A NULL result means the operation produced the empty set, so clear the
	 * map.  The set-op callers below filter out an empty src up front, so the
	 * only empty result we see is sbm_intersection/sbm_difference returning
	 * NULL when an operand has no chunks; sbm_union of non-empty operands is
	 * never empty.  (A non-NULL result is always non-empty here.)
	 */
	if (result == NULL)
	{
		sbm_clear(dst);
		return outer;
	}
	Assert(!sbm_is_empty(result));
	result_size = result->m_data_used;
	outer = sbm_grow_embedded(outer, sbm_offset, data_offset, result_size);
	dst = (Sbm *) ((uint8 *) outer + sbm_offset);	/* block may have moved */
	memcpy(dst->m_data, result->m_data, result_size);
	dst->m_data_used = result_size;
	sbm_card_invalidate(dst);
	sbm_free(result);
	sbm_try_demote(dst);
	return outer;
}

/*
 * Add `idx` to the embedded map, growing the enclosing `outer` block as
 * needed.  Returns the possibly-moved `outer`.
 */
void *
sbm_add_grow_embedded(void *outer, size_t sbm_offset, size_t data_offset,
					  uint64 idx)
{
	Sbm		   *dst;

	Assert(outer != NULL);

	/*
	 * Retry across a grow: sbm_add fails with ENOSPC when the embedded buffer
	 * is full, at which point we enlarge the enclosing block and try again.
	 * Growth at least doubles capacity, so this loops at most a couple times.
	 */
	for (;;)
	{
		dst = (Sbm *) ((uint8 *) outer + sbm_offset);
		if (sbm_add(dst, idx) != SBM_IDX_MAX)
			break;
		outer = sbm_grow_embedded(outer, sbm_offset, data_offset,
								  Max(sbm_cap(dst) * 2, 1024));
	}
	return outer;
}

/*
 * Add every member of [lo, hi) to the embedded map, growing the enclosing
 * `outer` block as needed.  Returns the possibly-moved `outer`.
 */
void *
sbm_add_range_embedded(void *outer, size_t sbm_offset, size_t data_offset,
					   uint64 lo, uint64 hi)
{
	Sbm		   *dst;

	Assert(outer != NULL);
	for (;;)
	{
		dst = (Sbm *) ((uint8 *) outer + sbm_offset);
		if (sbm_add_range(dst, lo, hi))
			break;
		outer = sbm_grow_embedded(outer, sbm_offset, data_offset,
								  Max(sbm_cap(dst) * 2, 1024));
	}
	return outer;
}

/*
 * map := map OP src for an embedded map, growing the enclosing `outer` block
 * when the result needs more room.  Each returns the possibly-moved `outer`.
 * These are the embedded, no-intermediate-node analogues of
 * sbm_union_inplace() / sbm_intersection_inplace() / sbm_difference_inplace().
 */
void *
sbm_union_into_embedded(void *outer, size_t sbm_offset, size_t data_offset,
						const Sbm *src)
{
	Sbm		   *dst = (Sbm *) ((uint8 *) outer + sbm_offset);

	Assert(outer != NULL);
	if (sbm_is_empty(src))
		return outer;
	return sbm_replace_buffer_embedded(outer, sbm_offset, data_offset,
									   sbm_union(dst, src));
}

void *
sbm_intersection_into_embedded(void *outer, size_t sbm_offset,
							   size_t data_offset, const Sbm *src)
{
	Sbm		   *dst = (Sbm *) ((uint8 *) outer + sbm_offset);

	Assert(outer != NULL);
	return sbm_replace_buffer_embedded(outer, sbm_offset, data_offset,
									   sbm_intersection(dst, src));
}

void *
sbm_difference_into_embedded(void *outer, size_t sbm_offset,
							 size_t data_offset, const Sbm *src)
{
	Sbm		   *dst = (Sbm *) ((uint8 *) outer + sbm_offset);

	Assert(outer != NULL);
	if (sbm_is_empty(src))
		return outer;
	return sbm_replace_buffer_embedded(outer, sbm_offset, data_offset,
									   sbm_difference(dst, src));
}


Sbm *
sbm_union_inplace(Sbm *dst, const Sbm *src)
{
	if (dst == NULL)
		return NULL;
	if (sbm_is_empty(src))
		return dst;
	if (sbm_is_empty(dst))
	{
		/* dst becomes a copy of src.  Use the chunk-aware copy path. */
		Sbm		   *copy = sbm_copy(src);

		if (copy == NULL)
			return NULL;
		return sbm_replace_buffer(dst, copy);
	}
	return sbm_replace_buffer(dst, sbm_union(dst, src));
}


Sbm *
sbm_intersection_inplace(Sbm *dst, const Sbm *src)
{
	if (dst == NULL)
		return NULL;
	if (sbm_is_empty(dst))
		return dst;
	if (sbm_is_empty(src))
	{
		sbm_clear(dst);
		return dst;
	}
	return sbm_replace_buffer(dst, sbm_intersection(dst, src));
}

Sbm *
sbm_difference_inplace(Sbm *dst, const Sbm *src)
{
	if (dst == NULL)
		return NULL;
	if (sbm_is_empty(dst) || sbm_is_empty(src))
		return dst;
	return sbm_replace_buffer(dst, sbm_difference(dst, src));
}

Sbm *
sbm_xor_inplace(Sbm *dst, const Sbm *src)
{
	if (dst == NULL)
		return NULL;
	/* XOR with nothing is a no-op; XOR into nothing is a copy of src. */
	if (sbm_is_empty(src))
		return dst;
	if (sbm_is_empty(dst))
		return sbm_replace_buffer(dst, sbm_copy(src));
	return sbm_replace_buffer(dst, sbm_xor(dst, src));
}

/* -------------------------------------------------------------------
 * Maintenance and introspection: range flip, validate, statistics,
 * shrink_to_fit
 * -------------------------------------------------------------------
 */

bool
sbm_flip_range(Sbm *map, uint64 lo, uint64 hi)
{
	uint64		i;

	if (map == NULL || lo >= hi)
		return lo >= hi;
	for (i = lo; i < hi; i++)
	{
		const bool	was_set = sbm_contains(map, i, NULL);

		if (sbm_assign(map, i, !was_set) == SBM_IDX_MAX)
		{
			return false;
		}
	}
	return true;
}

bool
sbm_validate(const Sbm *map)
{
	size_t		count;
	uint8	   *p;
	uint8	   *end;
	SbmIdx		prev_start = 0;
	uint64		prev_end = 0;	/* start + capacity of the previous chunk */
	bool		first = true;
	size_t		i;

	if (map == NULL)
		return true;
	if (map->m_data == NULL && sbm_cap(map) > 0)
		return false;
	if (map->m_data_used > sbm_cap(map))
		return false;
	if (map->m_data_used == 0)
	{
		return true;
	}
	if (map->m_data_used < SBM_SIZEOF_OVERHEAD)
		return false;

	/*
	 * Small-set mode: the header word's top bit is set and the low bits hold
	 * the word count.  Valid iff nwords <= the span cap and m_data_used
	 * exactly covers the header plus that many words.
	 */
	if (sbm_is_small(map))
	{
		const size_t nwords = sbm_small_nwords(map);

		if (nwords > SBM_SMALL_MAX_WORDS)
			return false;
		if (map->m_data_used !=
			SBM_SIZEOF_OVERHEAD + nwords * sizeof(uint64))
			return false;

		/*
		 * A trailing all-zero word would mean a non-canonical form (remove
		 * trims them); reject so equal sets have one encoding.
		 */
		if (nwords > 0 && sbm_small_words(map)[nwords - 1] == 0)
			return false;
		return true;
	}

	count = sbm_get_chunk_count(map);
	if (count == 0)
	{
		return map->m_data_used == SBM_SIZEOF_OVERHEAD;
	}

	p = sbm_get_chunk_data(map, 0);
	end = map->m_data + map->m_data_used;
	for (i = 0; i < count; i++)
	{
		const SbmIdx start = sbm_load_idx((const uint8 *) p);
		SbmChunk	chunk;
		size_t		chunk_size;
		size_t		capacity;
		uint64		chunk_end;

		if (p + SBM_SIZEOF_OVERHEAD + sizeof(SbmBitvec) > end)
		{
			return false;
		}
		if (!first && start <= prev_start)
		{
			return false;
		}
		/* (b) chunk starts are chunk-aligned bit indices. */
		if (start % SBM_CHUNK_MAX_CAPACITY != 0)
		{
			return false;
		}
		sbm_chunk_init(&chunk, p + SBM_SIZEOF_OVERHEAD);
		chunk_size = sbm_chunk_get_size(&chunk);
		if (p + SBM_SIZEOF_OVERHEAD + chunk_size > end)
		{
			return false;
		}
		capacity = sbm_chunk_get_capacity(&chunk);
		/* (a) an RLE chunk's run length cannot exceed its capacity. */
		if (sbm_chunk_is_rle(&chunk) &&
			sbm_chunk_rle_get_length(&chunk) > capacity)
		{
			return false;
		}

		/*
		 * (f) sparse descriptor shape: every data-bearing slot
		 * (SBM_PAYLOAD_ONES / SBM_PAYLOAD_MIXED) must lie within the chunk's
		 * measured capacity.  sbm_chunk_get_capacity reports
		 * SBM_CHUNK_MAX_CAPACITY minus SBM_BITS_PER_VECTOR per
		 * SBM_PAYLOAD_NONE flag wherever that flag sits, but the slot-indexed
		 * readers (rank / cardinality / select / minimum / maximum) place a
		 * slot's bits at its fixed position slot*SBM_BITS_PER_VECTOR while
		 * the capacity-bounded readers (contains / next_member) stop at
		 * start+capacity.  When a NONE flag sits below a data-bearing slot
		 * the two disagree: sbm_cardinality counts the high slot's bits,
		 * sbm_next_member / sbm_contains skip them. The encoder never emits
		 * such a chunk (every data-bearing slot it writes fits inside the
		 * reduced capacity), so reject any crafted buffer that violates this
		 * -- it is the one sparse shape sbm_validate used to accept while the
		 * readers answered inconsistently.  NONE in the last slot is the RLE
		 * marker and is handled by the RLE path above.
		 */
		if (!sbm_chunk_is_rle(&chunk))
		{
			const SbmBitvec desc = chunk.m_data[0];
			size_t		slot;
			int			highest_data = -1;

			for (slot = 0; slot < SBM_FLAGS_PER_INDEX; slot++)
			{
				const size_t fl = (size_t) SBM_CHUNK_GET_FLAGS(desc, slot);

				if (fl == SBM_PAYLOAD_ONES || fl == SBM_PAYLOAD_MIXED)
					highest_data = (int) slot;
			}
			if (highest_data >= 0 &&
				((size_t) (highest_data + 1) *
				 (size_t) SBM_BITS_PER_VECTOR) > capacity)
			{
				return false;
			}
		}

		/*
		 * (c) [start, start + capacity) must not extend past the addressable
		 * index space.  A chunk that ends exactly at 2^64 (start + capacity
		 * wraps to 0) is legal -- it holds the top bits [2^64 - capacity,
		 * 2^64).  Only a wrap to a nonzero end is an overflow.
		 */
		chunk_end = start + capacity;
		if (chunk_end != 0 && chunk_end < start)
		{
			return false;
		}

		/*
		 * (d) [start, start + capacity) must not overlap the span of the
		 * preceding chunk.  prev_end == 0 means the previous chunk reached
		 * 2^64; ascending starts already forbid a follower.
		 */
		if (!first && prev_end != 0 && start < prev_end)
		{
			return false;
		}
		p += SBM_SIZEOF_OVERHEAD + chunk_size;
		prev_start = start;
		prev_end = chunk_end;
		first = false;
	}

	/*
	 * (e) the stored chunk count must match the walk exactly: the walk
	 * consumed `count` chunks above, so leftover bytes mean the count
	 * disagrees with the encoded stream.
	 */
	return p == end;
}

void
sbm_statistics(const Sbm *map, SbmStats *stats)
{
	if (stats == NULL)
		return;
	memset(stats, 0, sizeof(*stats));
	if (map == NULL)
		return;

	if (sbm_is_small(map))
	{
		/*
		 * Report the small map's real footprint; derive the chunk breakdown
		 * from a materialized view (it would occupy those chunks were it
		 * promoted).
		 */
		Sbm		   *m = sbm_materialize(map);

		sbm_statistics(m, stats);
		sbm_free(m);
		stats->bytes_used = sbm_get_size(map);
		stats->bytes_capacity = sbm_get_capacity(map);
		return;
	}

	stats->bytes_used = sbm_get_size(map);
	stats->bytes_capacity = sbm_get_capacity(map);

	{
		const size_t count = sbm_get_chunk_count(map);
		uint8	   *p;
		size_t		i;

		stats->chunks_total = count;
		if (count == 0)
			return;

		p = sbm_get_chunk_data(map, 0);
		for (i = 0; i < count; i++)
		{
			SbmChunk	chunk;
			size_t		chunk_size;

			sbm_chunk_init(&chunk, p + SBM_SIZEOF_OVERHEAD);
			chunk_size = sbm_chunk_get_size(&chunk);
			if (sbm_chunk_is_rle(&chunk))
			{
				stats->chunks_rle++;
				stats->bits_in_rle +=
					sbm_chunk_rle_get_length(&chunk);
			}
			else
			{
				const SbmBitvec desc = chunk.m_data[0];
				size_t		pos = 1;
				size_t		v;

				stats->chunks_sparse++;
				for (v = 0; v < SBM_FLAGS_PER_INDEX; v++)
				{
					const size_t flags =
						SBM_CHUNK_GET_FLAGS(desc, v);

					if (flags == SBM_PAYLOAD_ONES)
					{
						stats->bits_in_sparse +=
							SBM_BITS_PER_VECTOR;
					}
					else if (flags ==
							 SBM_PAYLOAD_MIXED)
					{
						stats->bits_in_sparse +=
							(uint64) pg_popcount64(
												   chunk.m_data[pos]);
						pos++;
					}
				}
			}
			p += SBM_SIZEOF_OVERHEAD + chunk_size;
		}
	}
	stats->bits_set = stats->bits_in_rle + stats->bits_in_sparse;
	stats->bytes_per_set_bit = stats->bits_set == 0 ?
		0.0 :
		(double) stats->bytes_used / (double) stats->bits_set;
}

Sbm *
sbm_shrink_to_fit(Sbm *map)
{
	size_t		target;

	if (map == NULL)
		return NULL;
	if (sbm_kind(map) == SBM_WRAPPED)
		return map;

	target =
		map->m_data_used > 0 ? map->m_data_used : SBM_SIZEOF_OVERHEAD;
	if (target == sbm_cap(map))
		return map;

	return sbm_set_data_size(map, NULL, target);
}

/* -------------------------------------------------------------------
 * Portable serialization
 * -------------------------------------------------------------------
 */

#define SBM_WIRE_MAGIC      0x30316d73u /* "sm10" little-endian */
#define SBM_WIRE_VERSION    2u
#define SBM_WIRE_HEADER_LEN 16u
#define SBM_WIRE_FLAG_LE    0x01u
/*
 * Reserved header byte out[6]: a documented mirror of the body's own
 * small-set marker (the body's header top bit is authoritative).
 */
#define SBM_WIRE_FLAG_SMALL 0x01u

static bool
sbm_host_is_little_endian(void)
{
	const uint16 one = 1;

	return ((const uint8 *) &one)[0] == 1;
}

size_t
sbm_serialized_size(const Sbm *map)
{
	if (map == NULL)
		return SBM_WIRE_HEADER_LEN + SBM_SIZEOF_OVERHEAD;
	return SBM_WIRE_HEADER_LEN + sbm_get_size(map);
}

/*
 * The removal bound (see sbm_removal_bound) of a set with members in nwin
 * chunk windows and nvec 64-bit vectors, whose highest vector below
 * SBM_SMALL_MAX_BITS is top (-1 if none).
 */
static size_t
sbm_bound_from_counts(uint64 nwin, uint64 nvec, int top)
{
	size_t		chunkform;
	size_t		smallform = 0;

	chunkform = SBM_SIZEOF_OVERHEAD +
		nwin * (SBM_SIZEOF_OVERHEAD + sizeof(SbmBitvec)) +
		nvec * sizeof(SbmBitvec);
	if (top >= 0)
		smallform = SBM_SIZEOF_OVERHEAD + (top + 1) * sizeof(uint64);
	return SBM_WIRE_HEADER_LEN + Max(chunkform, smallform);
}

/*
 * Upper bound on sbm_serialized_size() of map and of every subset of it.
 *
 * Removing members can make an encoding larger.  An all-ones vector costs
 * only its 2-bit descriptor flag, and removing one of its members turns it
 * into a stored mixed vector (+8 bytes); clearing a bit inside an RLE run,
 * which costs one descriptor however long it is, splits it into sparse
 * chunks.  A caller that keeps a serialized map in a fixed-size slot and
 * rewrites it in place after removing members (a BARK POSTING entry under
 * VACUUM) reserves this many bytes when it first stores the map.
 *
 * The bound is a function of the set alone, not of its encoding.  Let nwin
 * be the number of chunk windows (SBM_CHUNK_MAX_CAPACITY bits, aligned)
 * that hold a member, nvec the number of aligned 64-bit vectors that hold a
 * member, and top the highest such vector below SBM_SMALL_MAX_BITS, if any.
 * Then:
 *
 *	- A chunk-mode encoding is at most 8 (chunk count) + 16 (start index and
 *	  descriptor) per chunk + 8 per stored vector.  Every chunk starts on a
 *	  window boundary, no two share a window, and none is empty (a sparse
 *	  chunk lies within its window; an RLE run begins at the chunk's start),
 *	  so there are at most nwin chunks; every stored vector is mixed, so it
 *	  holds a member, and there are at most nvec of them.  An RLE chunk is
 *	  charged here for every window and vector its run touches, which is
 *	  what it costs once removals have split it into sparse chunks.
 *	- A small-set encoding is exactly 8 + 8 * (top + 1) bytes, because the
 *	  words above the highest member are never stored.
 *
 * The bound is the larger of the two, plus the wire header.  Removing a
 * member can only lower nwin, nvec and top, so the bound of any subset is at
 * most the bound of the set: a slot sized for the set holds every subset of
 * it, whichever encoding the subset ends up with.
 */
size_t
sbm_removal_bound(const Sbm *map)
{
	uint64		nwin = 0;
	uint64		nvec = 0;
	int			top = -1;

	if (map != NULL && sbm_is_small(map))
	{
		const uint64 *w = sbm_small_words(map);
		const size_t n = sbm_small_nwords(map);

		for (size_t i = 0; i < n; i++)
		{
			if (w[i] != 0)
			{
				nvec++;
				top = (int) i;
			}
		}
		nwin = (nvec > 0) ? 1 : 0;
	}
	else if (map != NULL)
	{
		const size_t count = sbm_get_chunk_count(map);
		uint8	   *p = sbm_get_chunk_data(map, 0);

		sbm_check_invariants(map);
		for (size_t i = 0; i < count; i++)
		{
			const SbmIdx start = sbm_load_idx(p);
			SbmChunk	chunk;

			sbm_chunk_init(&chunk, p + SBM_SIZEOF_OVERHEAD);
			if (sbm_chunk_is_rle(&chunk))
			{
				const uint64 len = sbm_chunk_rle_get_length(&chunk);

				nwin += (len + SBM_CHUNK_MAX_CAPACITY - 1) / SBM_CHUNK_MAX_CAPACITY;
				nvec += (len + SBM_BITS_PER_VECTOR - 1) / SBM_BITS_PER_VECTOR;
				if (start == 0 && len > 0)
					top = (int) (Min(len, SBM_SMALL_MAX_BITS) - 1) /
						SBM_BITS_PER_VECTOR;
			}
			else
			{
				/*
				 * The high bit of a 2-bit flag is set for ONES (11) and MIXED
				 * (10) and clear for ZEROS (00) and NONE (01).
				 */
				const uint64 held = chunk.m_data[0] &
					UINT64CONST(0xAAAAAAAAAAAAAAAA);

				if (held != 0)
				{
					nwin++;
					nvec += pg_popcount64(held);
				}
				if (start == 0)
				{
					/* Flags 0 .. SBM_SMALL_MAX_WORDS-1, two bits each. */
					const uint64 low = held &
						((UINT64CONST(1) << (2 * SBM_SMALL_MAX_WORDS)) - 1);

					if (low != 0)
						top = pg_leftmost_one_pos64(low) / 2;
				}
			}
			p += SBM_SIZEOF_OVERHEAD + sbm_chunk_get_size(&chunk);
		}
	}

	return sbm_bound_from_counts(nwin, nvec, top);
}

/*
 * sbm_removal_bound of the set of the n members arr[], ascending (duplicates
 * allowed), computed from the members without building the map.  The bound
 * depends only on the set, so this equals sbm_removal_bound of any map
 * holding exactly those members.  A caller that builds a map only when its
 * bound is small enough (a BARK POSTING entry, which must beat the equivalent
 * flat TID list) can thus make the decision first.
 */
size_t
sbm_removal_bound_sorted(const uint64 *arr, size_t n)
{
	uint64		nwin = 0;
	uint64		nvec = 0;
	int			top = -1;

	for (size_t i = 0; i < n; i++)
	{
		const uint64 vec = arr[i] / SBM_BITS_PER_VECTOR;

		Assert(i == 0 || arr[i - 1] <= arr[i]);
		if (i > 0 && arr[i - 1] / SBM_BITS_PER_VECTOR == vec)
			continue;
		nvec++;
		if (i == 0 || arr[i - 1] / SBM_CHUNK_MAX_CAPACITY !=
			arr[i] / SBM_CHUNK_MAX_CAPACITY)
			nwin++;
		if (arr[i] < SBM_SMALL_MAX_BITS)
			top = (int) vec;
	}
	return sbm_bound_from_counts(nwin, nvec, top);
}

size_t
sbm_serialize(const Sbm *map, uint8 *out, size_t out_size)
{
	size_t		needed;
	uint64		reserved = 0;
	uint8		flags;
	uint32		magic = SBM_WIRE_MAGIC;

	if (out == NULL)
		return 0;
	needed = sbm_serialized_size(map);
	if (out_size < needed)
		return 0;

	flags = sbm_host_is_little_endian() ? SBM_WIRE_FLAG_LE : 0;

	/* Header: writes via memcpy so it works on strict-alignment cpus. */
	memcpy(out + 0, &magic, 4);
	out[4] = SBM_WIRE_VERSION;
	out[5] = flags;
	out[6] = (map != NULL && sbm_is_small(map)) ? SBM_WIRE_FLAG_SMALL : 0;
	out[7] = 0;

	/*
	 * Bytes 8-15 are reserved and written as zero.  They once carried the
	 * member count, but no reader used it, and computing it walked every
	 * chunk: a cost each caller that serializes after a mutation paid in
	 * full, because mutation invalidates the cardinality cache.
	 */
	memcpy(out + 8, &reserved, 8);

	/*
	 * Body: existing internal format (or just an SBM_SIZEOF_OVERHEAD zeroed
	 * header for NULL/empty maps).
	 */
	if (map == NULL || sbm_is_empty(map))
	{
		memset(out + SBM_WIRE_HEADER_LEN, 0, SBM_SIZEOF_OVERHEAD);
	}
	else
	{
		memcpy(out + SBM_WIRE_HEADER_LEN, sbm_get_data(map),
			   sbm_get_size(map));
	}
	return needed;
}

Sbm *
sbm_deserialize(const uint8 *in, size_t n)
{
	uint32		magic;
	uint8		version;
	uint8		flags;
	bool		wire_is_le;
	bool		host_is_le;
	size_t		body_len;
	Sbm		   *map;

	if (in == NULL || n < SBM_WIRE_HEADER_LEN + SBM_SIZEOF_OVERHEAD)
	{
		return NULL;
	}
	memcpy(&magic, in + 0, 4);
	if (magic != SBM_WIRE_MAGIC)
		return NULL;

	version = in[4];
	flags = in[5];
	if (version != SBM_WIRE_VERSION)
		return NULL;

	wire_is_le = (flags & SBM_WIRE_FLAG_LE) != 0;
	host_is_le = sbm_host_is_little_endian();
	if (wire_is_le != host_is_le)
	{
		/* Cross-endian read not yet supported. */
		return NULL;
	}

	/* Body: starts at offset SBM_WIRE_HEADER_LEN. */
	body_len = n - SBM_WIRE_HEADER_LEN;
	map = sbm_create(body_len + 64);

	/*
	 * Copy the body into the map's data buffer.  The first
	 * SBM_SIZEOF_OVERHEAD bytes are the chunk count; the rest is chunks.
	 */
	memcpy(map->m_data, in + SBM_WIRE_HEADER_LEN, body_len);

	/*
	 * Force m_data_used to its expected value: the first 4 bytes contain
	 * chunk_count, then we need to walk to compute total size. sbm_open's
	 * pattern handles this.
	 */
	map->m_data_used = body_len;

	/* Validate the result; reject malformed input. */
	if (!sbm_validate(map))
	{
		sbm_free(map);
		return NULL;
	}
	return map;
}

/*
 * Copy a raw chunk (start offset + descriptor + vectors) into result.
 */
static void
sbm_copy_chunk_to_result(Sbm **resultp, const uint8 *chunk_ptr)
{
	SbmChunk	chunk;
	size_t		chunk_bytes;

	chunk.m_data =
		(SbmBitvecUnaligned *) (chunk_ptr + SBM_SIZEOF_OVERHEAD);
	chunk_bytes = SBM_SIZEOF_OVERHEAD + sbm_chunk_get_size(&chunk);
	sbm_ensure_capacity(resultp, chunk_bytes);
	sbm_append_reserved(*resultp, chunk_ptr, chunk_bytes);
	sbm_set_chunk_count(*resultp, sbm_get_chunk_count(*resultp) + 1);
}

/* -------------------------------------------------------------------
 * Set operations: chunk-merge intersection, difference, union
 * -------------------------------------------------------------------
 */

/*
 * Create a new sparse bitmap containing the intersection of a and b.
 *
 * Uses a two-pointer chunk merge walk for O(chunks) performance instead
 * of the previous O(cardinality x chunks) bit-by-bit scan+contains.
 */
Sbm *
sbm_intersection(const Sbm *a, const Sbm *b)
{
	size_t		a_count;
	size_t		b_count;
	size_t		cap;
	Sbm		   *result;
	uint8	   *ap;
	uint8	   *bp;
	size_t		ai = 0,
				bi = 0;

	sbm_check_invariants(a);
	sbm_check_invariants(b);
	if (a == NULL || b == NULL)
	{
		return NULL;
	}
	if (sbm_is_small(a) || sbm_is_small(b))
	{
		bool		oa,
					ob;
		const Sbm  *va = sbm_chunk_view(a, &oa);
		const Sbm  *vb = sbm_chunk_view(b, &ob);
		Sbm		   *r = sbm_intersection(va, vb);

		if (oa)
			sbm_free(unconstify(Sbm *, va));
		if (ob)
			sbm_free(unconstify(Sbm *, vb));
		return r;
	}

	a_count = sbm_get_chunk_count(a);
	b_count = sbm_get_chunk_count(b);

	if (a_count == 0 || b_count == 0)
	{
		return NULL;
	}

	cap = a->m_data_used;
	{
		size_t		cap_b = b->m_data_used;

		if (cap_b > cap)
			cap = cap_b;
	}
	if (cap < 1024)
		cap = 1024;

	result = sbm_create(cap);

	ap = sbm_get_chunk_data(a, 0);
	bp = sbm_get_chunk_data(b, 0);

	while (ai < a_count && bi < b_count)
	{
		/* Read chunk a metadata */
		const SbmIdx a_start = sbm_load_idx((const uint8 *) ap);
		SbmChunk	a_chunk;
		bool		a_rle;
		size_t		a_cap;
		size_t		a_size;
		size_t		a_end;
		const SbmIdx b_start = sbm_load_idx((const uint8 *) bp);
		SbmChunk	b_chunk;
		bool		b_rle;
		size_t		b_cap;
		size_t		b_size;
		size_t		b_end;

		sbm_chunk_init(&a_chunk, ap + SBM_SIZEOF_OVERHEAD);
		a_rle = SBM_IS_CHUNK_RLE(&a_chunk);
		a_cap = sbm_chunk_get_capacity(&a_chunk);
		a_size = sbm_chunk_get_size(&a_chunk);
		a_end = (size_t) a_start + a_cap;	/* one past last bit */

		/* Read chunk b metadata */
		sbm_chunk_init(&b_chunk, bp + SBM_SIZEOF_OVERHEAD);
		b_rle = SBM_IS_CHUNK_RLE(&b_chunk);
		b_cap = sbm_chunk_get_capacity(&b_chunk);
		b_size = sbm_chunk_get_size(&b_chunk);
		b_end = (size_t) b_start + b_cap;

		/* Prefetch next chunks */
		if (ai + 1 < a_count)
		{
			pg_prefetch(ap + SBM_SIZEOF_OVERHEAD + a_size);
		}
		if (bi + 1 < b_count)
		{
			pg_prefetch(bp + SBM_SIZEOF_OVERHEAD + b_size);
		}

		/* No overlap: a is entirely before b */
		if (a_end <= b_start)
		{
			ap += SBM_SIZEOF_OVERHEAD + a_size;
			ai++;
			continue;
		}

		/* No overlap: b is entirely before a */
		if (b_end <= a_start)
		{
			bp += SBM_SIZEOF_OVERHEAD + b_size;
			bi++;
			continue;
		}

		/* Chunks overlap. Handle the common aligned sparse case fast. */
		if (!a_rle && !b_rle && a_start == b_start)
		{
			/* Word-level AND of two aligned sparse chunks */
			SbmBitvec	aw[32],
						bw[32];
			int			ac[32],
						bc[32];
			SbmBitvec	rw[32];
			int			rc[32];
			SbmBitvec	desc;
			SbmBitvec	vecs[32];
			int			nvecs;
			int			i;

			sbm_expand_sparse_chunk(&a_chunk, aw, ac);
			sbm_expand_sparse_chunk(&b_chunk, bw, bc);

			sbm_words_and(rw, aw, bw);
			for (i = 0; i < (int) SBM_FLAGS_PER_INDEX; i++)
			{
				rc[i] = (ac[i] && bc[i]) ? 1 : 0;
				if (!rc[i])
					rw[i] = 0;
			}

			if (sbm_encode_sparse_chunk(rw, rc, &desc, vecs,
										&nvecs))
			{
				sbm_append_sparse_chunk(&result, a_start, desc, vecs, nvecs);
			}
		}
		else if (a_rle && b_rle)
		{
			/* Both RLE: intersection is the overlap of two runs */
			const size_t a_len =
				sbm_chunk_rle_get_length(&a_chunk);
			const size_t b_len =
				sbm_chunk_rle_get_length(&b_chunk);

			/*
			 * a has set bits [a_start, a_start+a_len), b has [b_start,
			 * b_start+b_len)
			 */
			const size_t overlap_start =
				a_start > b_start ? a_start : b_start;
			const size_t a_set_end = (size_t) a_start + a_len;
			const size_t b_set_end = (size_t) b_start + b_len;
			const size_t overlap_end =
				a_set_end < b_set_end ? a_set_end : b_set_end;

			if (overlap_start < overlap_end)
			{
				const size_t run_len =
					overlap_end - overlap_start;
				const size_t run_cap =
					run_len;	/* tight capacity */

				sbm_append_rle_chunk(&result, (SbmIdx) overlap_start, run_cap, run_len);
			}
		}
		else
		{
			/*
			 * Mixed types: expand both to words, AND, encode. Use the sparse
			 * chunk's start as the target alignment.
			 */
			SbmBitvec	aw[SBM_FLAGS_PER_INDEX],
						bw[SBM_FLAGS_PER_INDEX];
			int			ac[SBM_FLAGS_PER_INDEX],
						bc[SBM_FLAGS_PER_INDEX];
			SbmIdx		result_start;
			SbmBitvec	rw[SBM_FLAGS_PER_INDEX];
			int			rc[SBM_FLAGS_PER_INDEX];
			SbmBitvec	desc;
			SbmBitvec	vecs[SBM_FLAGS_PER_INDEX];
			int			nvecs;
			int			i;

			/*
			 * Valid maps keep every chunk start on a chunk boundary and
			 * chunks never overlap, so overlapping sparse chunks share a
			 * start (handled above) and two overlapping RLE chunks are
			 * handled above too: exactly one of a and b is RLE here.
			 */
			Assert(a_rle != b_rle);
			if (a_rle)
			{
				/* a is RLE, b is sparse: expand a into b's alignment */
				sbm_expand_sparse_chunk(&b_chunk, bw, bc);
				sbm_expand_rle_as_words(&a_chunk, a_start,
										b_start, aw, ac, bc);
				result_start = b_start;
			}
			else
			{
				/* a is sparse, b is RLE: expand b into a's alignment */
				sbm_expand_sparse_chunk(&a_chunk, aw, ac);
				sbm_expand_rle_as_words(&b_chunk, b_start,
										a_start, bw, bc, ac);
				result_start = a_start;
			}

			sbm_words_and(rw, aw, bw);
			for (i = 0; i < (int) SBM_FLAGS_PER_INDEX; i++)
			{
				rc[i] = (ac[i] && bc[i]) ? 1 : 0;
				if (!rc[i])
					rw[i] = 0;
			}

			if (sbm_encode_sparse_chunk(rw, rc, &desc, vecs,
										&nvecs))
			{
				sbm_append_sparse_chunk(&result, result_start, desc, vecs, nvecs);
			}
		}

		/* Advance whichever chunk ends first */
		if (a_end <= b_end)
		{
			ap += SBM_SIZEOF_OVERHEAD + a_size;
			ai++;
		}
		if (b_end <= a_end)
		{
			bp += SBM_SIZEOF_OVERHEAD + b_size;
			bi++;
		}
	}

	if (sbm_get_chunk_count(result) == 0)
	{
		sbm_free(result);
		return NULL;
	}

	sbm_try_demote(result);
	return result;
}

/*
 * Emit set bits from a chunk within [from, to) into result.
 *
 * For sparse chunks, uses expand-mask-encode for bulk processing.
 * For RLE chunks, emits the clipped run through the ordered emitter.
 */
static void
sbm_emit_chunk_bits(Sbm **resultp, const SbmChunk *chunk, bool is_rle,
					SbmIdx chunk_start, size_t from, size_t to)
{
	SbmBitvec	words[SBM_FLAGS_PER_INDEX];
	int			cap_flags[SBM_FLAGS_PER_INDEX];
	size_t		rel_from;
	size_t		rel_to;
	int			start_word;
	int			end_word;
	SbmBitvec	desc;
	SbmBitvec	vecs[SBM_FLAGS_PER_INDEX];
	int			nvecs;
	int			i;

	if (from >= to)
		return;

	if (is_rle)
	{
		const size_t len = sbm_chunk_rle_get_length(chunk);
		const size_t set_start = (size_t) chunk_start;
		const size_t set_end = set_start + len;
		const size_t emit_start = from > set_start ? from : set_start;
		const size_t emit_end = to < set_end ? to : set_end;

		/*
		 * The clipped run [emit_start, emit_end) can start and end anywhere
		 * inside a chunk window, but every output chunk must start on a
		 * window boundary.  Lay it down through the ordered emitter the other
		 * set operations use (sbm_emit_run), which splits it at window
		 * boundaries, emits the whole windows as RLE with window-aligned
		 * capacity, and the partial head and tail as sparse chunks.  A fresh
		 * emitter per call is enough: the difference sweep appends output
		 * chunks in ascending, non-overlapping order.  (sparsemap 5.8.2
		 * made the same change.)
		 */
		if (emit_start < emit_end)
		{
			SbmEmitter	em;

			memset(&em, 0, sizeof(em));
			em.resultp = resultp;
			sbm_emit_run(&em, (uint64) emit_start, (uint64) emit_end);
			sbm_emit_flush(&em);
		}
		return;
	}

	/* Sparse: expand, mask to [from, to) range, encode and append */
	sbm_expand_sparse_chunk(chunk, words, cap_flags);

	/* Mask out bits outside [from, to) range relative to chunk_start */
	rel_from = from - (size_t) chunk_start;
	rel_to = to - (size_t) chunk_start;
	start_word = (int) (rel_from / SBM_BITS_PER_VECTOR);
	end_word =
		(int) ((rel_to + SBM_BITS_PER_VECTOR - 1) / SBM_BITS_PER_VECTOR);

	/* Zero words entirely before the range */
	for (i = 0; i < start_word && i < (int) SBM_FLAGS_PER_INDEX; i++)
	{
		words[i] = 0;
		cap_flags[i] = 0;
	}

	/* Mask partial start word */
	if (start_word < (int) SBM_FLAGS_PER_INDEX)
	{
		const size_t start_bit = rel_from % SBM_BITS_PER_VECTOR;

		if (start_bit > 0)
		{
			words[start_word] &= ~((SbmBitvec) 0) << start_bit;
		}
	}

	/* Zero words entirely after the range */
	for (i = end_word; i < (int) SBM_FLAGS_PER_INDEX; i++)
	{
		words[i] = 0;
		cap_flags[i] = 0;
	}

	/* Mask partial end word */
	if (end_word > 0 && end_word <= (int) SBM_FLAGS_PER_INDEX)
	{
		const size_t end_bit = rel_to % SBM_BITS_PER_VECTOR;

		if (end_bit > 0)
		{
			words[end_word - 1] &=
				((SbmBitvec) 1 << end_bit) - 1;
		}
	}

	if (sbm_encode_sparse_chunk(words, cap_flags, &desc, vecs, &nvecs))
	{
		sbm_append_sparse_chunk(resultp, chunk_start, desc, vecs, nvecs);
	}
}

/*
 * Create a new sparse bitmap containing the difference a \ b (bits in a but not in b).
 *
 * Uses a two-pointer chunk merge walk with a cursor to track progress
 * through each a chunk, preventing double-counting when one a chunk
 * overlaps with multiple b chunks.
 */
Sbm *
sbm_difference(const Sbm *a, const Sbm *b)
{
	size_t		a_count;
	size_t		b_count;
	size_t		cap;
	Sbm		   *result;
	uint8	   *ap;
	uint8	   *bp;
	size_t		ai = 0,
				bi = 0;

	sbm_check_invariants(a);
	sbm_check_invariants(b);
	if (a == NULL)
	{
		return NULL;
	}
	if (sbm_is_small(a) || sbm_is_small(b))
	{
		bool		oa,
					ob;
		const Sbm  *va = sbm_chunk_view(a, &oa);
		const Sbm  *vb = sbm_chunk_view(b, &ob);
		Sbm		   *r = sbm_difference(va, vb);

		if (oa)
			sbm_free(unconstify(Sbm *, va));
		if (ob)
			sbm_free(unconstify(Sbm *, vb));
		return r;
	}

	a_count = sbm_get_chunk_count(a);
	if (a_count == 0)
	{
		return NULL;
	}

	/* If b is NULL or empty, return a copy of a */
	if (b == NULL || sbm_get_chunk_count(b) == 0)
	{
		return sbm_copy(a);
	}

	b_count = sbm_get_chunk_count(b);

	cap = a->m_data_used;
	if (cap < 1024)
		cap = 1024;

	result = sbm_create(cap);

	ap = sbm_get_chunk_data(a, 0);
	bp = sbm_get_chunk_data(b, 0);

	while (ai < a_count)
	{
		/* Read chunk a metadata */
		const SbmIdx a_start = sbm_load_idx((const uint8 *) ap);
		SbmChunk	a_chunk;
		size_t		a_cursor;
		uint8	   *bp_save;
		size_t		bi_save;
		bool		a_rle;
		size_t		a_cap_bits;
		size_t		a_size;
		size_t		a_end;

		sbm_chunk_init(&a_chunk, ap + SBM_SIZEOF_OVERHEAD);
		a_rle = SBM_IS_CHUNK_RLE(&a_chunk);
		a_cap_bits = sbm_chunk_get_capacity(&a_chunk);
		a_size = sbm_chunk_get_size(&a_chunk);
		a_end = (size_t) a_start + a_cap_bits;

		/* Prefetch next a chunk */
		if (ai + 1 < a_count)
		{
			pg_prefetch(ap + SBM_SIZEOF_OVERHEAD + a_size);
		}

		/* If b is exhausted, copy remaining a chunks */
		if (bi >= b_count)
		{
			sbm_copy_chunk_to_result(&result, ap);
			ap += SBM_SIZEOF_OVERHEAD + a_size;
			ai++;
			continue;
		}

		/* Cursor: tracks how far into this a chunk we've processed */
		a_cursor = (size_t) a_start;

		/* Save b state so we can iterate b within this a chunk */
		bp_save = bp;
		bi_save = bi;

		/* Process all b chunks that overlap with this a chunk */
		while (bi < b_count)
		{
			const SbmIdx b_start =
				sbm_load_idx((const uint8 *) bp);
			SbmChunk	b_chunk;
			bool		b_rle;
			size_t		b_cap_bits;
			size_t		ov_start;
			size_t		ov_end;
			size_t		b_size;
			size_t		b_end;

			sbm_chunk_init(&b_chunk, bp + SBM_SIZEOF_OVERHEAD);
			b_rle = SBM_IS_CHUNK_RLE(&b_chunk);
			b_cap_bits = sbm_chunk_get_capacity(&b_chunk);
			b_size = sbm_chunk_get_size(&b_chunk);
			b_end = (size_t) b_start + b_cap_bits;

			/* b is past a: no more overlaps for this a chunk */
			if (a_end <= (size_t) b_start)
				break;

			/* b is entirely before cursor: skip b */
			if (b_end <= a_cursor)
			{
				bp += SBM_SIZEOF_OVERHEAD + b_size;
				bi++;
				continue;
			}

			/* Overlap region */
			ov_start = (size_t) b_start > a_cursor ?
				(size_t) b_start :
				a_cursor;
			ov_end = a_end < b_end ? a_end : b_end;

			/* Emit a's surviving bits in the gap [a_cursor, ov_start) */
			sbm_emit_chunk_bits(&result, &a_chunk, a_rle, a_start, a_cursor, ov_start);

			/* Process overlap: aligned sparse fast path */
			if (a_rle && b_rle)
			{
				/*
				 * Both RLE.  A run can be much longer than the 2048-bit word
				 * window, so this cannot be done by expanding into words.
				 * Work on the runs directly: within the overlap, a's set bits
				 * survive exactly where b's run does not reach.
				 */
				const size_t b_set_end = (size_t) b_start +
					sbm_chunk_rle_get_length(&b_chunk);

				/* a's bits before b's run starts. */
				if (ov_start < (size_t) b_start)
				{
					const size_t upto =
						ov_end < (size_t) b_start ?
						ov_end :
						(size_t) b_start;

					sbm_emit_chunk_bits(&result, &a_chunk, a_rle, a_start, ov_start, upto);
				}

				/* a's bits after b's run ends. */
				if (b_set_end < ov_end)
				{
					const size_t from =
						b_set_end > ov_start ? b_set_end :
						ov_start;

					sbm_emit_chunk_bits(&result, &a_chunk, a_rle, a_start, from, ov_end);
				}
				a_cursor = ov_end;
			}
			else if (!a_rle && !b_rle && a_start == b_start)
			{
				SbmBitvec	aw[32],
							bw[32];
				int			ac[32],
							bc[32];
				SbmBitvec	rw[32];
				int			rc[32];
				SbmBitvec	desc;
				SbmBitvec	vecs[32];
				int			nvecs;
				int			i;

				sbm_expand_sparse_chunk(&a_chunk, aw, ac);
				sbm_expand_sparse_chunk(&b_chunk, bw, bc);

				sbm_words_andnot(rw, aw, bw);
				for (i = 0; i < (int) SBM_FLAGS_PER_INDEX;
					 i++)
				{
					if (ac[i])
					{
						if (!bc[i])
							rw[i] = aw
								[i];	/* b has no cap: keep a unchanged */
						rc[i] = 1;
					}
					else
					{
						rw[i] = 0;
						rc[i] = 0;
					}
				}

				if (sbm_encode_sparse_chunk(rw, rc, &desc,
											vecs, &nvecs))
				{
					sbm_append_sparse_chunk(&result, a_start, desc, vecs, nvecs);
				}
				a_cursor =
					a_end;		/* entire a chunk handled by word-level op */
			}
			else
			{
				/* Mixed types: expand both to words, AND-NOT, encode */
				SbmBitvec	aw2[SBM_FLAGS_PER_INDEX],
							bw2[SBM_FLAGS_PER_INDEX];
				int			ac2[SBM_FLAGS_PER_INDEX],
							bc2[SBM_FLAGS_PER_INDEX];
				SbmIdx		result_start;
				SbmBitvec	rw2[SBM_FLAGS_PER_INDEX];
				int			rc2[SBM_FLAGS_PER_INDEX];
				SbmBitvec	desc2;
				SbmBitvec	vecs2[SBM_FLAGS_PER_INDEX];
				int			nvecs2;
				int			i;

				/* sparse chunks are aligned (see sbm_validate), so one is RLE */
				Assert(a_rle != b_rle);
				if (a_rle)
				{
					/* a is RLE, b is sparse */
					sbm_expand_sparse_chunk(&b_chunk, bw2,
											bc2);
					sbm_expand_rle_as_words(&a_chunk,
											a_start, b_start, aw2, ac2, bc2);
					result_start = b_start;
				}
				else
				{
					/* a is sparse, b is RLE */
					sbm_expand_sparse_chunk(&a_chunk, aw2,
											ac2);
					sbm_expand_rle_as_words(&b_chunk,
											b_start, a_start, bw2, bc2, ac2);
					result_start = a_start;
				}

				sbm_words_andnot(rw2, aw2, bw2);
				for (i = 0; i < (int) SBM_FLAGS_PER_INDEX;
					 i++)
				{
					if (ac2[i])
					{
						if (!bc2[i])
							rw2[i] = aw2
								[i];	/* b has no cap: keep a unchanged */
						rc2[i] = 1;
					}
					else
					{
						rw2[i] = 0;
						rc2[i] = 0;
					}
				}

				if (sbm_encode_sparse_chunk(rw2, rc2, &desc2,
											vecs2, &nvecs2))
				{
					sbm_append_sparse_chunk(&result, result_start, desc2, vecs2, nvecs2);
				}
				a_cursor = ov_end;
			}

			/* Advance b if it ends within or at a's boundary */
			if (b_end <= a_end)
			{
				bp += SBM_SIZEOF_OVERHEAD + b_size;
				bi++;
			}
			/* If a ends within b, we're done with this a chunk */
			if (a_end <= b_end)
				break;
		}

		/* Emit remaining a bits [a_cursor, a_end) that had no b overlap */
		if (a_cursor < a_end)
		{
			sbm_emit_chunk_bits(&result, &a_chunk, a_rle, a_start, a_cursor, a_end);
		}

		/*
		 * Restore b pointer: next a chunk may overlap with same b chunks. But
		 * we only need b chunks that haven't been fully passed yet. Keep
		 * bi/bp at the furthest b that still overlaps or is ahead.
		 */
		(void) bp_save;
		(void) bi_save;

		ap += SBM_SIZEOF_OVERHEAD + a_size;
		ai++;
	}

	if (sbm_get_chunk_count(result) == 0)
	{
		sbm_free(result);
		return NULL;
	}

	sbm_try_demote(result);
	return result;
}

/*
 * Create a new sparse bitmap containing the union of a and b.
 *
 * Uses a two-pointer chunk merge walk for O(chunks) performance instead
 * of the previous O(cardinality x chunks) in-place mutation.  Cursors
 * track partially-consumed chunks when one chunk extends past the other.
 *
 * Fast paths:
 *   - Aligned sparse chunks: word-level OR via expand/encode helpers.
 *   - Both RLE chunks: direct run merge (handles contiguous and gapped runs).
 *   - Mixed: bit-by-bit OR bounded by the sparse chunk's capacity.
 *
 * `a` is first input sparse bitmap; `b` is second input sparse bitmap.
 * Returns a newly allocated sparse bitmap (caller must free()), or NULL on
 *          allocation failure or if both inputs are empty/NULL.
 */
Sbm *
sbm_union(const Sbm *a, const Sbm *b)
{
	size_t		a_count;
	size_t		b_count;
	size_t		cap;
	Sbm		   *result;
	uint8	   *ap;
	uint8	   *bp;
	size_t		ai = 0,
				bi = 0;

	/*
	 * Cursors track how far into each current chunk we've already emitted. A
	 * value of 0 means "fresh chunk" (reset after advancing).  When a chunk
	 * is partially consumed, the cursor holds the absolute bit position up to
	 * which bits have been emitted.
	 */
	size_t		a_cursor = 0;
	size_t		b_cursor = 0;

	sbm_check_invariants(a);
	sbm_check_invariants(b);
	if (a == NULL && b == NULL)
	{
		return NULL;
	}
	if (sbm_is_small(a) || sbm_is_small(b))
	{
		bool		oa,
					ob;
		const Sbm  *va = sbm_chunk_view(a, &oa);
		const Sbm  *vb = sbm_chunk_view(b, &ob);
		Sbm		   *r = sbm_union(va, vb);

		if (oa)
			sbm_free(unconstify(Sbm *, va));
		if (ob)
			sbm_free(unconstify(Sbm *, vb));
		return r;
	}

	a_count = a ? sbm_get_chunk_count(a) : 0;
	b_count = b ? sbm_get_chunk_count(b) : 0;

	if (a_count == 0 && b_count == 0)
	{
		return NULL;
	}
	if (a_count == 0)
	{
		return sbm_copy(b);
	}
	if (b_count == 0)
	{
		return sbm_copy(a);
	}

	/* Allocate result with combined data size (worst case: no overlap). */
	cap = a->m_data_used + b->m_data_used;
	if (cap < 1024)
		cap = 1024;

	result = sbm_create(cap);

	ap = sbm_get_chunk_data(a, 0);
	bp = sbm_get_chunk_data(b, 0);

	while (ai < a_count && bi < b_count)
	{
		/* ---- Read chunk a metadata ---- */
		const SbmIdx a_start = sbm_load_idx((const uint8 *) ap);
		SbmChunk	a_chunk;
		bool		a_rle;
		size_t		a_cap_bits;
		size_t		a_size;
		size_t		a_end;
		const SbmIdx b_start = sbm_load_idx((const uint8 *) bp);
		SbmChunk	b_chunk;
		bool		b_rle;
		size_t		b_cap_bits;
		size_t		b_size;
		size_t		b_end;

		sbm_chunk_init(&a_chunk, ap + SBM_SIZEOF_OVERHEAD);
		a_rle = SBM_IS_CHUNK_RLE(&a_chunk);
		a_cap_bits = sbm_chunk_get_capacity(&a_chunk);
		a_size = sbm_chunk_get_size(&a_chunk);
		a_end = (size_t) a_start + a_cap_bits;

		/* Ensure cursor is at least at chunk start. */
		if (a_cursor < (size_t) a_start)
			a_cursor = (size_t) a_start;

		/* ---- Read chunk b metadata ---- */
		sbm_chunk_init(&b_chunk, bp + SBM_SIZEOF_OVERHEAD);
		b_rle = SBM_IS_CHUNK_RLE(&b_chunk);
		b_cap_bits = sbm_chunk_get_capacity(&b_chunk);
		b_size = sbm_chunk_get_size(&b_chunk);
		b_end = (size_t) b_start + b_cap_bits;

		if (b_cursor < (size_t) b_start)
			b_cursor = (size_t) b_start;

		/* Prefetch next chunks for the merge loop. */
		if (ai + 1 < a_count)
			pg_prefetch(ap + SBM_SIZEOF_OVERHEAD + a_size);
		if (bi + 1 < b_count)
			pg_prefetch(bp + SBM_SIZEOF_OVERHEAD + b_size);

		/* ---- No overlap: a's remaining range ends before b's ---- */
		if (a_end <= b_cursor)
		{
			if (a_cursor == (size_t) a_start)
			{
				sbm_copy_chunk_to_result(&result, ap);
			}
			else
			{
				sbm_emit_chunk_bits(&result, &a_chunk, a_rle, a_start, a_cursor, a_end);
			}
			ap += SBM_SIZEOF_OVERHEAD + a_size;
			ai++;
			a_cursor = 0;
			continue;
		}

		/* ---- No overlap: b's remaining range ends before a's ---- */
		if (b_end <= a_cursor)
		{
			if (b_cursor == (size_t) b_start)
			{
				sbm_copy_chunk_to_result(&result, bp);
			}
			else
			{
				sbm_emit_chunk_bits(&result, &b_chunk, b_rle, b_start, b_cursor, b_end);
			}
			bp += SBM_SIZEOF_OVERHEAD + b_size;
			bi++;
			b_cursor = 0;
			continue;
		}

		/* ---- Chunks overlap.  Compute overlap bounds. ---- */
		{
			const size_t ov_start =
				a_cursor > b_cursor ? a_cursor : b_cursor;
			const size_t ov_end = a_end < b_end ? a_end : b_end;

			/* ---- Fast path: both sparse, aligned ---- */

			/*
			 * When aligned, handle the full chunk with per-cursor masking.
			 * This avoids creating separate pre-overlap chunks at the same
			 * start.
			 */
			if (!a_rle && !b_rle && a_start == b_start)
			{
				SbmBitvec	aw[SBM_FLAGS_PER_INDEX],
							bw[SBM_FLAGS_PER_INDEX];
				int			ac[SBM_FLAGS_PER_INDEX],
							bc[SBM_FLAGS_PER_INDEX];
				SbmBitvec	rw[SBM_FLAGS_PER_INDEX];
				int			rc[SBM_FLAGS_PER_INDEX];
				SbmBitvec	desc;
				SbmBitvec	vecs[SBM_FLAGS_PER_INDEX];
				int			nvecs;
				int			i;

				/*
				 * A cursor moves into the middle of a chunk's window only
				 * when the other map's RLE chunk ends inside that window; the
				 * other map's next chunk then starts at a later window, so
				 * two aligned chunks are always consumed from the start.
				 */
				Assert(a_cursor == (size_t) a_start &&
					   b_cursor == (size_t) b_start);
				sbm_expand_sparse_chunk(&a_chunk, aw, ac);
				sbm_expand_sparse_chunk(&b_chunk, bw, bc);

				sbm_words_or(rw, aw, bw);
				for (i = 0; i < (int) SBM_FLAGS_PER_INDEX; i++)
				{
					rc[i] = (ac[i] || bc[i]) ? 1 : 0;
				}

				if (sbm_encode_sparse_chunk(rw, rc, &desc, vecs,
											&nvecs))
				{
					sbm_append_sparse_chunk(&result, a_start, desc, vecs, nvecs);
				}

				/* Both chunks fully consumed. */
				ap += SBM_SIZEOF_OVERHEAD + a_size;
				ai++;
				a_cursor = 0;
				bp += SBM_SIZEOF_OVERHEAD + b_size;
				bi++;
				b_cursor = 0;

			}
			else
			{
				/* Emit pre-overlap bits from whichever cursor is behind. */
				if (a_cursor < ov_start)
				{
					sbm_emit_chunk_bits(&result, &a_chunk, a_rle, a_start, a_cursor, ov_start);
					a_cursor = ov_start;
				}
				if (b_cursor < ov_start)
				{
					sbm_emit_chunk_bits(&result, &b_chunk, b_rle, b_start, b_cursor, ov_start);
					b_cursor = ov_start;
				}

				if (a_rle && b_rle)
				{
					/* ---- Both RLE: merge set-bit runs in [ov_start, ov_end) ---- */
					const size_t a_len =
						sbm_chunk_rle_get_length(&a_chunk);
					const size_t b_len =
						sbm_chunk_rle_get_length(&b_chunk);

					/* Clamp each run to the overlap window. */
					const size_t a_set_end =
						(size_t) a_start + a_len;
					const size_t b_set_end =
						(size_t) b_start + b_len;
					const size_t as = ov_start > (size_t) a_start ?
						ov_start :
						(size_t) a_start;
					const size_t ae =
						ov_end < a_set_end ? ov_end : a_set_end;
					const size_t bs = ov_start > (size_t) b_start ?
						ov_start :
						(size_t) b_start;
					const size_t be =
						ov_end < b_set_end ? ov_end : b_set_end;

					const bool	a_has = as < ae;
					const bool	b_has = bs < be;

					if (a_has && b_has)
					{
						const size_t min_s = as < bs ? as : bs;
						const size_t max_e = ae > be ? ae : be;

						/* Check if runs overlap or are adjacent. */
						const size_t earlier_e =
							as <= bs ? ae : be;
						const size_t later_s =
							as <= bs ? bs : as;

						/*
						 * Both runs begin at their chunk's first index, so in
						 * an overlapping window they share a start and always
						 * touch -- a gap between them is impossible.
						 */
						Assert(earlier_e >= later_s);
						sbm_append_rle_chunk(&result, (SbmIdx) min_s,
											 max_e - min_s, max_e - min_s);
					}
					else if (a_has)
					{
						sbm_append_rle_chunk(&result, (SbmIdx) as, ae - as, ae - as);
					}
					else if (b_has)
					{
						sbm_append_rle_chunk(&result, (SbmIdx) bs, be - bs, be - bs);
					}
					/* else: no set bits in overlap -- nothing to emit. */

					a_cursor = ov_end;
					b_cursor = ov_end;
					if (a_cursor >= a_end)
					{
						ap += SBM_SIZEOF_OVERHEAD + a_size;
						ai++;
						a_cursor = 0;
					}
					if (b_cursor >= b_end)
					{
						bp += SBM_SIZEOF_OVERHEAD + b_size;
						bi++;
						b_cursor = 0;
					}

				}
				else
				{
					/* ---- Mixed types: expand-OR-encode ---- */
					SbmBitvec	aw2[SBM_FLAGS_PER_INDEX],
								bw2[SBM_FLAGS_PER_INDEX];
					int			ac2[SBM_FLAGS_PER_INDEX],
								bc2[SBM_FLAGS_PER_INDEX];
					SbmIdx		result_start;
					SbmBitvec	rw2[SBM_FLAGS_PER_INDEX];
					int			rc2[SBM_FLAGS_PER_INDEX];
					SbmBitvec	desc2;
					SbmBitvec	vecs2[SBM_FLAGS_PER_INDEX];
					int			nvecs2;
					int			i;

					/*
					 * sparse chunks are aligned (see sbm_validate), so one is
					 * RLE
					 */
					Assert(a_rle != b_rle);
					if (a_rle)
					{
						sbm_expand_sparse_chunk(&b_chunk, bw2,
												bc2);
						sbm_expand_rle_as_words(&a_chunk,
												a_start, b_start, aw2, ac2, bc2);
						result_start = b_start;
					}
					else
					{
						sbm_expand_sparse_chunk(&a_chunk, aw2,
												ac2);
						sbm_expand_rle_as_words(&b_chunk,
												b_start, a_start, bw2, bc2, ac2);
						result_start = a_start;
					}

					sbm_words_or(rw2, aw2, bw2);
					for (i = 0; i < (int) SBM_FLAGS_PER_INDEX;
						 i++)
					{
						rc2[i] = (ac2[i] || bc2[i]) ? 1 : 0;
					}

					if (sbm_encode_sparse_chunk(rw2, rc2, &desc2,
												vecs2, &nvecs2))
					{
						sbm_append_sparse_chunk(&result, result_start, desc2, vecs2, nvecs2);
					}

					a_cursor = ov_end;
					b_cursor = ov_end;
					if (a_cursor >= a_end)
					{
						ap += SBM_SIZEOF_OVERHEAD + a_size;
						ai++;
						a_cursor = 0;
					}
					if (b_cursor >= b_end)
					{
						bp += SBM_SIZEOF_OVERHEAD + b_size;
						bi++;
						b_cursor = 0;
					}
				}
			}
		}
	}

	/* Copy remaining chunks from whichever map is not exhausted. */
	while (ai < a_count)
	{
		const SbmIdx start = sbm_load_idx((const uint8 *) ap);
		SbmChunk	c;
		size_t		sz;

		sbm_chunk_init(&c, ap + SBM_SIZEOF_OVERHEAD);
		sz = sbm_chunk_get_size(&c);
		if (a_cursor > 0 && a_cursor > (size_t) start)
		{
			/* Partially consumed: emit only remaining bits. */
			const bool	rle = SBM_IS_CHUNK_RLE(&c);
			const size_t cap_bits = sbm_chunk_get_capacity(&c);

			sbm_emit_chunk_bits(&result, &c, rle, start, a_cursor, (size_t) start + cap_bits);
		}
		else
		{
			sbm_copy_chunk_to_result(&result, ap);
		}
		ap += SBM_SIZEOF_OVERHEAD + sz;
		ai++;
		a_cursor = 0;
	}
	while (bi < b_count)
	{
		const SbmIdx start = sbm_load_idx((const uint8 *) bp);
		SbmChunk	c;
		size_t		sz;

		sbm_chunk_init(&c, bp + SBM_SIZEOF_OVERHEAD);
		sz = sbm_chunk_get_size(&c);
		if (b_cursor > 0 && b_cursor > (size_t) start)
		{
			const bool	rle = SBM_IS_CHUNK_RLE(&c);
			const size_t cap_bits = sbm_chunk_get_capacity(&c);

			sbm_emit_chunk_bits(&result, &c, rle, start, b_cursor, (size_t) start + cap_bits);
		}
		else
		{
			sbm_copy_chunk_to_result(&result, bp);
		}
		bp += SBM_SIZEOF_OVERHEAD + sz;
		bi++;
		b_cursor = 0;
	}

	if (sbm_get_chunk_count(result) == 0)
	{
		sbm_free(result);
		return NULL;
	}

	sbm_try_demote(result);
	return result;
}

/* -------------------------------------------------------------------
 * Split, select, rank, and span
 * -------------------------------------------------------------------
 */

/*
 * sbm_split helper: idx falls inside the RLE chunk at `src`.  Rewrite that
 * chunk in place as the equivalent two or three chunks produced by
 * sbm_separate_rle_chunk, so that idx now falls inside a sparse chunk.
 * Returns false (with errno set) if the map has no room for the expansion,
 * leaving the map unchanged.
 *
 * This lives in its own function so that sbm_split's subsequent recursion is
 * a genuine tail call: the scratch buffer and separation state below are
 * dead by the time sbm_split recurses.
 */
static bool
sbm_split_separate_rle(Sbm *map, uint64 idx, uint8 *src, SbmIdx src_start)
{
	SbmChunk	s_chunk;
	Sbm			stunt;
	SbmChunk	chunk;
	alignas(SbmBitvec) uint8
				buf[(SBM_SIZEOF_OVERHEAD * (unsigned long) 3) +
					(sizeof(SbmBitvec) * 6)] = {0};
	SbmChunkSep sep;
	int			sep_rc;
	size_t		src_offset;
	size_t		rle_data_off;

	sbm_chunk_init(&s_chunk, src + SBM_SIZEOF_OVERHEAD);

	/*
	 * sbm_separate_rle_chunk operates on a map, so copy the RLE chunk into a
	 * one-chunk scratch ("stunt") map, separate it there, and then splice the
	 * resulting chunks back into the real map.
	 */
	memcpy(buf + SBM_SIZEOF_OVERHEAD, src,
		   SBM_SIZEOF_OVERHEAD + sizeof(SbmBitvec));
	/* Set the number of chunks to 1 in our stunt map. */
	sbm_store_u64((uint8 *) buf, (uint64) 1);
	/* And initialize the stunt double chunk we need to split. */
	sbm_open(&stunt, buf,
			 (SBM_SIZEOF_OVERHEAD * (unsigned long) 3) +
			 (sizeof(SbmBitvec) * 6));
	sbm_chunk_init(&chunk, buf + (SBM_SIZEOF_OVERHEAD * 2));

	/* Finally, let's separate the RLE chunk at index. */
	memset(&sep, 0, sizeof(sep));
	sep.target.p = buf + SBM_SIZEOF_OVERHEAD;
	sep.target.offset = SBM_SIZEOF_OVERHEAD;
	sep.target.chunk = &chunk;
	sep.target.start = src_start;
	sep.target.length =
		sbm_chunk_rle_get_length(&s_chunk);
	sep.target.capacity =
		sbm_chunk_get_capacity(&s_chunk);

	/*
	 * If separation could not fit a pivot in the available space,
	 * sep.expand_by is not meaningful; propagate the failure.
	 */
	sep_rc =
		sbm_separate_rle_chunk(&stunt, &sep, idx, -1);
	if (sep_rc != 0)
	{
		return false;
	}

	/* Splice the separated, equivalent chunks into the source map. */
	if (map->m_data_used + sep.expand_by + SBM_SIZEOF_OVERHEAD > sbm_cap(map))
	{
		errno = ENOSPC;
		return false;
	}

	/*
	 * Save src offset before insert, as insert will invalidate the pointer
	 */
	src_offset = (size_t) (src - map->m_data);

	/*
	 * sbm_insert_data / sbm_get_chunk_data take a DATA-region-relative offset
	 * (they add SBM_SIZEOF_OVERHEAD for the chunk-count header themselves).
	 * `src_offset` above is m_data-relative -- it already includes the header
	 * -- so the RLE chunk's data-relative offset is `src_offset -
	 * SBM_SIZEOF_OVERHEAD`, the same data-relative convention
	 * sbm_separate_rle_chunk's own insert uses.
	 */
	rle_data_off =
		src_offset - SBM_SIZEOF_OVERHEAD;
	sbm_insert_data(map,
					rle_data_off + SBM_SIZEOF_OVERHEAD +
					sizeof(SbmBitvec),
					sep.buf + SBM_SIZEOF_OVERHEAD +
					sizeof(SbmBitvec),
					sep.expand_by);
	/* Recalculate src pointer after insert operation */
	src = map->m_data + src_offset;
	memcpy(src, sep.buf,
		   sep.expand_by + SBM_SIZEOF_OVERHEAD +
		   sizeof(SbmBitvec));
	sbm_set_chunk_count(map,
						sbm_get_chunk_count(map) + (sep.count - 1));
	return true;
}

uint64
sbm_split(Sbm *map, uint64 idx, Sbm *other)
{
	size_t		i;
	size_t		count;
	bool		in_middle = false;
	uint8	   *src;
	uint8	   *dst;
	size_t		split_offset;
	size_t		chunks_to_move;
	uint8	   *map_end;

	if (map == NULL || other == NULL)
	{
		errno = EINVAL;
		return SBM_IDX_MAX;
	}
	sbm_card_invalidate(map);
	sbm_card_invalidate(other);

	/*
	 * Split walks raw chunks; give it a chunk-mode map and an empty
	 * chunk-mode destination.  Promote in place (within capacity) if `map` is
	 * small; the destination is cleared to chunk-empty.
	 */
	if (sbm_is_small(map))
	{
		if (!sbm_promote(map))
		{
			errno = ENOSPC;
			return SBM_IDX_MAX;
		}
	}

	/*
	 * other must be empty on entry (see sbm.h); clearing it also leaves it in
	 * chunk mode, and makes its chunk stream end where its data begins.
	 */
	Assert(sbm_is_empty(other));
	sbm_clear(other);
	sbm_check_invariants(map);
	sbm_check_invariants(other);
	count = sbm_get_chunk_count(map);

	/*
	 * According to the API when idx is SBM_IDX_MAX the client is requesting
	 * that we divide the bits in two equal portions, so we calculate that
	 * index here.
	 */
	if (idx == SBM_IDX_MAX)
	{
		const uint64 begin = sbm_minimum(map);
		const uint64 end = sbm_maximum(map);

		if (begin != end)
		{
			const size_t rank = sbm_rank(map, begin, end, true);

			idx = sbm_select(map, rank / 2, true);
		}
		else
		{
			return SBM_IDX_MAX;
		}
	}

	/* Is the index beyond the last bit set in the source? */
	if (idx > sbm_maximum(map))
	{
		return idx;
	}

	/*
	 * Here's how this is going to work, there are three phases. 1) Skip over
	 * any chunks before the idx. 2) If the idx falls within a chunk, ... 2a)
	 * If that chunk is RLE, separate the RLE into two or three chunks 2b)
	 * Recursively call sbm_split() because now we have a sparse chunk 3)
	 * Split the sparse chunk 4) Keep half in the src and insert the other
	 * half into the dst 5) Move any remaining chunks to dst.
	 */
	src = sbm_get_chunk_data(map, 0);
	dst = sbm_get_chunk_data(other, 0);

	/* (1): skip over chunks that are entirely to the left. */
	for (i = 0; i < count; i++)
	{
		const SbmIdx start = sbm_load_idx((const uint8 *) src);
		SbmChunk	chunk;

		if (start == idx)
		{
			break;
		}
		sbm_chunk_init(&chunk, src + SBM_SIZEOF_OVERHEAD);
		if (start <= idx && start + sbm_chunk_get_capacity(&chunk) > idx)
		{
			in_middle = true;
			break;
		}
		if (start > idx)
		{
			/*
			 * This chunk begins past idx, and the loop already advanced `src`
			 * to it after the previous (below-idx) chunk, so `src`/`i`
			 * correctly mark where the moved region starts.  Rolling back to
			 * the previous chunk here would move a chunk lying entirely below
			 * idx into `other`, violating the [start, idx) / [idx, end]
			 * partition.
			 */
			break;
		}

		src += SBM_SIZEOF_OVERHEAD + sbm_chunk_get_size(&chunk);
	}

	/* (2): The idx falls within a chunk then it has to be split. */
	if (in_middle)
	{
		SbmChunk	s_chunk,
					d_chunk;
		SbmIdx		src_start;
		size_t		mid_off;
		size_t		j;

		src_start = sbm_load_idx((const uint8 *) src);

		/*
		 * (2a) Does the idx fall within the range of an RLE chunk?  If so,
		 * (2b) separate it so idx falls in a sparse chunk, then start over on
		 * the rewritten map.  Test the descriptor directly rather than via an
		 * SbmChunk so that no local's address is live across the tail call.
		 */
		if ((sbm_load_u64(src + SBM_SIZEOF_OVERHEAD) & SBM_RLE_FLAGS_MASK) ==
			SBM_RLE_FLAGS)
		{
			if (!sbm_split_separate_rle(map, idx, src, src_start))
				return SBM_IDX_MAX;
			pg_tailcall(sbm_split(map, idx, other));
		}

		sbm_chunk_init(&s_chunk, src + SBM_SIZEOF_OVERHEAD);
		sbm_chunk_init(&d_chunk, dst + SBM_SIZEOF_OVERHEAD);

		/*
		 * (3) We're in the middle of a sparse chunk, let's split it.
		 */

		/*
		 * The destination is caller-provided and may be too small for even
		 * the single chunk this phase splits off.  A split-off sparse chunk
		 * occupies at most the overhead word plus a full descriptor and 32
		 * payload words; refuse up front with the documented ENOSPC if it
		 * will not fit, leaving both maps untouched.
		 */
		{
			const size_t max_chunk = SBM_SIZEOF_OVERHEAD +
				sizeof(SbmBitvec) *
				(size_t) (1 + SBM_FLAGS_PER_INDEX);

			if (other->m_data_used + max_chunk > sbm_cap(other))
			{
				errno = ENOSPC;
				return SBM_IDX_MAX;
			}
		}

		/* Zero out the space we'll need at the proper location in dst. */
		{
			uint8		buf[SBM_SIZEOF_OVERHEAD +
							(sizeof(SbmBitvec) * 2)] = {0};

			memcpy(dst, &buf, sizeof(buf));
		}

		/* And add a chunk to the other map. */
		sbm_set_chunk_count(other, sbm_get_chunk_count(other) + 1);
		if (other->m_data_used != 0)
		{
			other->m_data_used +=
				SBM_SIZEOF_OVERHEAD + sizeof(SbmBitvec);
		}

		/*
		 * Copy the bits in the sparse chunk, at most SBM_CHUNK_MAX_CAPACITY.
		 * The unset loop below mutates `map`: it removes bits [idx, ...) from
		 * the middle chunk, which SHRINKS that chunk (changing its byte size)
		 * and, if the chunk ends up empty, REMOVES it and decrements the
		 * chunk count -- shifting every later chunk and potentially
		 * reallocating m_data.  So `src`, the middle chunk's size, and the
		 * chunk count must all be re-derived AFTER the loop from the current
		 * bytes; using the pre-mutation values left the move loop starting at
		 * the wrong offset (a whole run above idx stranded in `map`, or the
		 * retained map left non-canonical / invalid when the middle chunk
		 * vanished).  Chunks BEFORE the middle one are untouched, so its
		 * m_data offset `mid_off` is stable across the mutation.
		 */
		mid_off = (size_t) (src - map->m_data);
		sbm_store_idx((uint8 *) dst, src_start);
		for (j = idx; j < src_start + SBM_CHUNK_MAX_CAPACITY;
			 j++)
		{
			if (sbm_contains(map, j, NULL))
			{
				sbm_map_set(other, j, false, NULL);
				sbm_map_unset(map, j, false);
			}
		}
		dst += SBM_SIZEOF_OVERHEAD + sbm_chunk_get_size(&d_chunk);
		i++;

		/*
		 * Re-derive where the moved region begins.  If a chunk still lives at
		 * mid_off and it is the middle chunk (start == src_start), it kept
		 * bits [src_start, idx) and the move starts just past it; otherwise
		 * the middle chunk was removed and the move starts at mid_off.
		 */
		src = map->m_data + mid_off;
		if (mid_off + SBM_SIZEOF_OVERHEAD + sizeof(SbmBitvec) <=
			map->m_data_used &&
			sbm_load_idx((const uint8 *) src) == src_start)
		{
			SbmChunk	mid;

			sbm_chunk_init(&mid, src + SBM_SIZEOF_OVERHEAD);
			src += SBM_SIZEOF_OVERHEAD + sbm_chunk_get_size(&mid);
		}
	}

	/* Now continue with all remaining chunks. */
	/* Save the offset where moved chunks start, so we can truncate map later */
	split_offset = (size_t) (src - map->m_data);

	/*
	 * Upper bound on chunks to move: the mutation in the in_middle phase can
	 * have removed the middle chunk (dropping the count) or left `i` out of
	 * step, so `count - i` is unreliable.  The probe below re-derives the
	 * exact movable count by byte-walking from `src` to the current data end;
	 * seed the bound with the current chunk count, which can never
	 * undercount.
	 */
	chunks_to_move = sbm_get_chunk_count(map);

	/*
	 * The chunk stream ends here; the move must never read past it.  On a
	 * valid-but-adversarial map the RLE-separation and sparse-split phases
	 * above can leave `i` disagreeing with the bytes actually present, so
	 * `count - i` may claim more chunks than remain.  Bounding every walk by
	 * map_end keeps the move within the source buffer.
	 */
	map_end = map->m_data + map->m_data_used;

	/*
	 * The destination is caller-provided and may be far smaller than what we
	 * are about to move into it.  The documented contract is SBM_IDX_MAX with
	 * errno=ENOSPC when the buffer is too small, so total the bytes first and
	 * refuse up front, leaving both maps untouched.  The same pass also
	 * re-derives how many chunks actually fit in the source before its data
	 * end, so a desynced count can never drive the move loop past the buffer.
	 */
	{
		uint8	   *probe = src;
		size_t		need = 0;
		size_t		movable = 0;
		size_t		j;

		for (j = 0; j < chunks_to_move; j++)
		{
			SbmChunk	c;
			size_t		sz;

			if (probe + SBM_SIZEOF_OVERHEAD + sizeof(SbmBitvec) >
				map_end)
			{
				break;
			}
			sbm_chunk_init(&c, probe + SBM_SIZEOF_OVERHEAD);
			sz = SBM_SIZEOF_OVERHEAD +
				sbm_chunk_get_size(&c);
			if (probe + sz > map_end)
			{
				break;
			}
			need += sz;
			probe += sz;
			movable++;
		}
		chunks_to_move = movable;
		if (other->m_data_used + need > sbm_cap(other))
		{
			errno = ENOSPC;
			return SBM_IDX_MAX;
		}
	}

	{
		size_t		j;

		for (j = 0; j < chunks_to_move; j++)
		{
			SbmChunk	chunk;
			size_t		chunk_size;

			sbm_chunk_init(&chunk, src + SBM_SIZEOF_OVERHEAD);
			chunk_size =
				SBM_SIZEOF_OVERHEAD + sbm_chunk_get_size(&chunk);

			/*
			 * Copy chunk to other.  The total was reserved before the loop
			 * started, so this cannot fail; if it ever did, the move would
			 * already be half-applied, so treat it as unreachable rather than
			 * pretending it can be unwound.
			 */
			if (unlikely(!sbm_append_data(other, src,
										  chunk_size)))
			{
				Assert(!"sbm_split: capacity check disagreed with "
					   "the move loop");
				errno = ENOSPC;
				return SBM_IDX_MAX;
			}
			sbm_set_chunk_count(other,
								sbm_get_chunk_count(other) + 1);

			src += chunk_size;
		}
	}

	/* Update chunk counts and force recalculation of data sizes */
	sbm_set_chunk_count(map, sbm_get_chunk_count(map) - chunks_to_move);
	map->m_data_used = split_offset;

	Assert(map->m_data_used >= SBM_SIZEOF_OVERHEAD);
	Assert(other->m_data_used >= SBM_SIZEOF_OVERHEAD);

	sbm_coalesce_map(map);
	sbm_coalesce_map(other);

	sbm_try_demote(map);
	sbm_try_demote(other);
	return idx;
}

uint64
sbm_select(const Sbm *map, uint64 n, bool value)
{
	if (map == NULL)
	{
		/*
		 * Empty map: no set bits; unset bits are the whole line, so the n-th
		 * unset bit is n.  Matches the count == 0 path.
		 */
		return value ? SBM_IDX_MAX : n;
	}
	if (sbm_is_small(map))
	{
		Sbm		   *m = sbm_materialize(map);

		{
			const uint64 r = sbm_select(m, n, value);

			sbm_free(m);
			return r;
		}
	}
	sbm_check_invariants(map);
	Assert(map->m_data_used >= SBM_SIZEOF_OVERHEAD);
	{
		const size_t count = sbm_get_chunk_count(map);
		uint8	   *p;
		size_t		i;
		uint64		end = 0;	/* first index past the chunks seen */

		if (count == 0 && value == false)
		{
			return n;
		}

		p = sbm_get_chunk_data(map, 0);

		for (i = 0; i < count; i++)
		{
			const SbmIdx start =
				sbm_load_idx((const uint8 *) p);
			SbmChunk	chunk;
			ssize_t		new_n;
			size_t		index;

			/*
			 * Indexes between the previous chunk's coverage and this chunk
			 * are not stored and are all unset.
			 */
			if (!value)
			{
				const uint64 gap = start - end;

				if (n < gap)
					return end + n;
				n -= gap;
			}
			p += SBM_SIZEOF_OVERHEAD;
			sbm_chunk_init(&chunk, p);

			new_n = (ssize_t) n;
			index = sbm_chunk_select(&chunk, (ssize_t) n, &new_n,
									 value);
			if (new_n == -1)
			{
				return start + index;
			}
			n = (uint64) new_n;

			p += sbm_chunk_get_size(&chunk);
			end = start + sbm_chunk_get_capacity(&chunk);
		}

		/* Past the last chunk every index is unset. */
		if (!value && n <= SBM_IDX_MAX - 1 - end)
			return end + n;
		return SBM_IDX_MAX;
	}
}

static size_t
sbm_rank_range(const Sbm *map, uint64 begin, uint64 end, bool value)
{
	uint64		span_width;
	size_t		width;
	size_t		count;
	size_t		set = 0;
	uint8	   *p;
	size_t		i;

	Assert(map->m_data_used >= SBM_SIZEOF_OVERHEAD);

	if (begin > end)
	{
		return 0;
	}

	/*
	 * Range width as a count.  When [begin, end] spans the entire 64-bit
	 * universe (begin == 0, end == PG_UINT64_MAX) the +1 overflows to 0;
	 * size_t saturates instead so the derived unset count below stays
	 * meaningful.  A full-universe unset query is degenerate (the answer is
	 * ~2^64) but must not wrap.
	 */
	span_width = end - begin;
	width =
		(span_width == PG_UINT64_MAX) ? SIZE_MAX : (size_t) (span_width + 1);

	/*
	 * Rank is computed from the set-bit count only.  A bit is set iff some
	 * chunk covers it, so the number of set bits in the inclusive range
	 * [begin, end] is the sum, over every chunk, of the matching bits in the
	 * overlap of [begin, end] with that chunk's covered span [start, start +
	 * capacity).  The unset count is then width - set; there is no
	 * cross-chunk gap bookkeeping to get wrong.
	 *
	 * sbm_chunk_rank does the per-chunk work (it takes from/to positions
	 * relative to the chunk start and is validated by the get_position / RLE
	 * property tests).  We only ever ask it for set bits here; unset is
	 * derived once at the end.
	 */
	count = sbm_get_chunk_count(map);
	if (count == 0)
	{
		return value ? 0 : width;
	}

	p = sbm_get_chunk_data(map, 0);
	for (i = 0; i < count; i++)
	{
		const SbmIdx start = sbm_load_idx((const uint8 *) p);
		SbmChunk	chunk;
		size_t		chunk_size;
		size_t		cap;
		uint64		chunk_lo;
		uint64		span;
		uint64		chunk_hi_incl;
		uint64		ov_lo;
		uint64		ov_hi_incl;
		size_t		from;
		size_t		to;

		p += SBM_SIZEOF_OVERHEAD;
		sbm_chunk_init(&chunk, p);
		chunk_size = sbm_chunk_get_size(&chunk);
		if (i + 1 < count)
		{
			pg_prefetch(p + chunk_size + SBM_SIZEOF_OVERHEAD);
		}
		cap = sbm_chunk_get_capacity(&chunk);
		chunk_lo = start;

		/*
		 * Inclusive top of the chunk's covered span.  cap >= 1, and we form
		 * (cap - 1) as a distance so the comparison below never overflows
		 * even when chunk_lo is near PG_UINT64_MAX (the top-of-universe
		 * case).
		 */
		span = (uint64) cap - 1;
		chunk_hi_incl =
			(chunk_lo > PG_UINT64_MAX - span) ? PG_UINT64_MAX
			: chunk_lo + span;

		/*
		 * Chunks are ordered ascending.  Once a chunk starts past `end` no
		 * later chunk can overlap [begin, end].
		 */
		if (chunk_lo > end)
		{
			p += chunk_size;
			break;
		}
		/* Skip chunks entirely below `begin`. */
		if (chunk_hi_incl < begin)
		{
			p += chunk_size;
			continue;
		}

		/* Overlap of [begin, end] with [chunk_lo, chunk_hi_incl]. */
		ov_lo = begin > chunk_lo ? begin : chunk_lo;
		ov_hi_incl = (end < chunk_hi_incl) ? end : chunk_hi_incl;
		/* Positions relative to the chunk start. */
		from = (size_t) (ov_lo - chunk_lo);
		to = (size_t) (ov_hi_incl - chunk_lo);

		set += sbm_chunk_rank(&chunk, from, to);
		p += chunk_size;
	}

	if (value)
	{
		return set;
	}
	Assert((uint64) set <= width);
	return (size_t) (width - set);
}

size_t
sbm_rank(const Sbm *map, uint64 begin, uint64 end, bool value)
{
	/* A NULL map is the empty set: every index in the range is unset. */
	if (map == NULL)
	{
		if (!value && begin <= end)
			return end - begin == PG_UINT64_MAX ? SIZE_MAX :
				(size_t) (end - begin + 1);
		return 0;
	}
	if (sbm_is_small(map))
	{
		Sbm		   *m = sbm_materialize(map);

		{
			const size_t r = sbm_rank(m, begin, end, value);

			sbm_free(m);
			return r;
		}
	}
	sbm_check_invariants(map);
	return sbm_rank_range(map, begin, end, value);
}

uint64
sbm_span(const Sbm *map, uint64 idx, size_t len, bool value)
{
	SbmRunIter	it;
	uint64		lo = 0,
				hi = 0;
	uint64		gap_lo = idx;	/* start of the current run of unset bits */

	if (len == 0)
		return idx;

	/*
	 * Walk the maximal runs of set bits at or after idx.  A span of set bits
	 * is a run (clipped at idx) at least len long; a span of unset bits is a
	 * gap between runs (or after the last one) at least len long.  Either way
	 * the walk is O(chunks), not O(members).
	 */
	sbm_run_iter_init(&it, map);
	while (sbm_run_next(&it, &lo, &hi))
	{
		if (hi <= idx)
			continue;
		if (lo < idx)
			lo = idx;
		if (value)
		{
			if (hi - lo >= len)
				return lo;
		}
		else
		{
			if (lo - gap_lo >= len)
				return gap_lo;
			gap_lo = hi;
		}
	}

	/* Everything past the last run is unset. */
	if (!value && SBM_IDX_MAX - gap_lo >= len)
		return gap_lo;
	return SBM_IDX_MAX;
}

/* -------------------------------------------------------------------
 * Point-lookup / rank / select acceleration (Ideas 3, 4, 5)
 *
 * These add caller-owned, transient acceleration state on TOP of the
 * plain O(chunks) path.  None of them grow Sbm or touch the wire
 * format; every one falls back to the plain path when it cannot be
 * both fast and correct.  Correctness is the invariant: a stale or
 * degenerate accelerator returns the SAME answer as sbm_contains /
 * sbm_rank / sbm_select, just slower.
 * -------------------------------------------------------------------
 */

/* -------------------------------------------------------------------
 * Idea 5: sbm_contains_many -- batched point lookups in one sweep.
 * -------------------------------------------------------------------
 */

void
sbm_contains_many(const Sbm *map, const uint64 *idxs, bool *results,
				  size_t n)
{
	size_t		count;
	uint8	   *base;
	uint8	   *p;
	size_t		stream_end;
	size_t		q = 0;
	size_t		i;

	if (n == 0)
	{
		return;
	}
	if (idxs == NULL || results == NULL)
	{
		errno = EINVAL;
		return;
	}
	if (map == NULL)
	{
		for (q = 0; q < n; q++)
		{
			results[q] = false;
		}
		return;
	}

#ifdef USE_ASSERT_CHECKING
	/* Contract: idxs MUST be sorted ascending. */
	for (q = 1; q < n; q++)
	{
		Assert(idxs[q] >= idxs[q - 1]);
	}
#endif

	if (sbm_is_small(map))
	{
		for (q = 0; q < n; q++)
		{
			results[q] = sbm_small_contains(map, idxs[q]);
		}
		return;
	}

	count = sbm_get_chunk_count(map);
	if (count == 0)
	{
		for (q = 0; q < n; q++)
		{
			results[q] = false;
		}
		return;
	}

	base = sbm_get_chunk_data(map, 0);
	p = base;
	stream_end =
		(size_t) map->m_data_used - SBM_SIZEOF_OVERHEAD;
	q = 0;

	/*
	 * One left-to-right sweep.  Walk chunks in order while draining the query
	 * cursor q into idxs[].  For each chunk [start, start+cap): queries
	 * strictly below start fall in a gap (false); queries below start+cap are
	 * answered by the within-chunk test; queries at or above start+cap belong
	 * to a later chunk, so advance the chunk. O(chunks + n).
	 */
	for (i = 0; i < count && q < n; i++)
	{
		const SbmIdx s = sbm_load_idx((const uint8 *) p);
		SbmChunk	chunk;
		size_t		cap;
		uint64		hi;
		size_t		next_off;

		sbm_chunk_init(&chunk, p + SBM_SIZEOF_OVERHEAD);
		cap = sbm_chunk_get_capacity(&chunk);
		hi = (uint64) s + cap;	/* exclusive top */

		/* Drain queries that fall before this chunk (gap -> false). */
		while (q < n && idxs[q] < (uint64) s)
		{
			results[q] = false;
			q++;
		}
		/* Answer queries that fall inside this chunk's covered span. */
		while (q < n && idxs[q] < hi)
		{
			results[q] =
				sbm_chunk_is_set(&chunk, idxs[q] - (uint64) s);
			q++;
		}

		/* Advance to the next chunk. */
		next_off = (size_t) (p - base) +
			SBM_SIZEOF_OVERHEAD + sbm_chunk_get_size(&chunk);
		if (next_off >= stream_end)
		{
			break;
		}
		p = base + next_off;
	}

	/* Any queries past the last chunk are not set. */
	for (; q < n; q++)
	{
		results[q] = false;
	}
}

/* -------------------------------------------------------------------
 * Idea 4: SbmLocator -- transient two-level sqrt(n) directory.
 * -------------------------------------------------------------------
 */

/*
 * Integer floor(sqrt(x)); avoids pulling in <math.h> and float determinism
 * worries.  x <= chunk count, so this is cheap.
 */
static size_t
sbm_isqrt(size_t x)
{
	size_t		r = 0;

	if (x == 0)
	{
		return 0;
	}
	while ((r + 1) * (r + 1) <= x)
	{
		r++;
	}
	return r;
}

/*
 * True when the locator's cached shape no longer matches its map, i.e. the
 * caller mutated the map without rebuilding.  A stale locator is a usage
 * error; queries fall back to the plain path so results stay correct.
 */
static bool
sbm_locator_is_stale(const SbmLocator *loc)
{
	uint8	   *base;
	SbmIdx		first;
	SbmIdx		last;

	if (loc == NULL || loc->map == NULL || loc->n_sb == 0)
	{
		return true;
	}
	if (sbm_get_chunk_count(loc->map) != loc->count)
	{
		return true;
	}

	/*
	 * First and last chunk starts are cheap O(1) fingerprints: a mutation
	 * that preserves the chunk count but shifts, splits, or coalesces chunks
	 * almost always moves one of them.  This is a best-effort check, not a
	 * proof of freshness -- but any miss still yields a correct answer via
	 * the fine-walk, which self-validates against the actual chunk bytes it
	 * reads.
	 */
	base = sbm_get_chunk_data(loc->map, 0);
	first = sbm_load_idx((const uint8 *) base);
	if (first != loc->first_start)
	{
		return true;
	}
	if ((size_t) loc->last_offset + sizeof(SbmIdx) >
		(size_t) loc->map->m_data_used - SBM_SIZEOF_OVERHEAD)
	{
		return true;
	}
	last =
		sbm_load_idx((const uint8 *) (base + loc->last_offset));
	if (last != loc->last_start)
	{
		return true;
	}
	return false;
}

SbmLocator *
sbm_locator_build(const Sbm *map)
{
	if (map == NULL)
	{
		return NULL;
	}

	/*
	 * The locator indexes the chunk stream by byte offset; a small-set map
	 * has no chunk stream.  Return a degenerate locator (n_sb == 0) whose
	 * staleness check always trips, so every query falls back to the plain,
	 * small-set-aware sbm_contains / sbm_rank / sbm_select. (Returning NULL
	 * would violate the "NULL only for an empty map" contract that callers
	 * rely on.)
	 */
	if (sbm_is_small(map))
	{
		SbmLocator *loc = (SbmLocator *) palloc(sizeof(*loc));

		memset(loc, 0, sizeof(*loc));
		loc->map = map;			/* n_sb == 0 => sbm_locator_is_stale =>
								 * fallback */
		return loc;
	}
	{
		const size_t count = sbm_get_chunk_count(map);
		SbmLocator *loc;
		size_t		stride;
		size_t		n_sb;
		uint64	   *sb_start;
		size_t	   *sb_offset;
		size_t	   *sb_prefix;
		uint8	   *base;
		uint8	   *p;
		size_t		stream_end;
		size_t		running = 0;	/* set bits in chunks strictly before p */
		size_t		sb = 0;
		size_t		last_offset = 0;
		SbmIdx		last_start = 0;
		size_t		i;

		if (count == 0)
		{
			return NULL;
		}

		loc = (SbmLocator *) palloc(sizeof(*loc));

		stride = sbm_isqrt(count) > 0 ? sbm_isqrt(count) : 1;
		n_sb = (count + stride - 1) / stride;

		sb_start = (uint64 *) palloc(n_sb * sizeof(uint64));
		sb_offset = (size_t *) palloc(n_sb * sizeof(size_t));
		sb_prefix = (size_t *) palloc(n_sb * sizeof(size_t));

		/*
		 * One O(count) walk: sample every stride-th chunk into the superblock
		 * arrays and carry the running set-bit total.
		 */
		base = sbm_get_chunk_data(map, 0);
		p = base;
		stream_end =
			(size_t) map->m_data_used - SBM_SIZEOF_OVERHEAD;
		for (i = 0; i < count; i++)
		{
			const SbmIdx s = sbm_load_idx((const uint8 *) p);
			SbmChunk	chunk;
			size_t		off;
			size_t		cap;
			size_t		next_off;

			sbm_chunk_init(&chunk, p + SBM_SIZEOF_OVERHEAD);
			off = (size_t) (p - base);
			last_offset = off;
			last_start = s;
			if (i % stride == 0)
			{
				Assert(sb < n_sb);
				sb_start[sb] = (uint64) s;
				sb_offset[sb] = off;
				sb_prefix[sb] = running;
				sb++;
			}

			/*
			 * Accumulate this chunk's set-bit count into the running total so
			 * the NEXT superblock's prefix is correct.
			 */
			cap = sbm_chunk_get_capacity(&chunk);
			running += sbm_chunk_rank(&chunk, 0, cap - 1);

			next_off =
				off + SBM_SIZEOF_OVERHEAD + sbm_chunk_get_size(&chunk);
			if (next_off >= stream_end)
			{
				break;
			}
			p = base + next_off;
		}

		loc->map = map;
		loc->count = count;
		loc->first_start = (uint64) sb_start[0];
		loc->last_start = (uint64) last_start;
		loc->last_offset = last_offset;
		loc->stride = stride;
		loc->n_sb = n_sb;
		loc->sb_start = sb_start;
		loc->sb_offset = sb_offset;
		loc->sb_prefix = sb_prefix;
		return loc;
	}
}

void
sbm_locator_free(SbmLocator *loc)
{
	if (loc == NULL)
	{
		return;
	}
	/* A small-map locator has no directory arrays (see sbm_locator_build). */
	if (loc->sb_start != NULL)
		pfree(loc->sb_start);
	if (loc->sb_offset != NULL)
		pfree(loc->sb_offset);
	if (loc->sb_prefix != NULL)
		pfree(loc->sb_prefix);
	pfree(loc);
}

/*
 * Binary search sb_start[] for the largest superblock sb with
 * sb_start[sb] <= idx.  Returns 0 when idx precedes the first sample
 * (fine-walk from superblock 0 then still answers correctly).
 */
static size_t
sbm_locator_find_sb(const SbmLocator *loc, uint64 idx)
{
	size_t		lo = 0,
				hi = loc->n_sb; /* [lo, hi) */

	while (lo < hi)
	{
		const size_t mid = lo + (hi - lo) / 2;

		if (loc->sb_start[mid] <= idx)
		{
			lo = mid + 1;
		}
		else
		{
			hi = mid;
		}
	}
	return lo == 0 ? 0 : lo - 1;
}

/*
 * Set bits in [0, x] via the prefix table: jump to x's superblock, seed the
 * count with sb_prefix[sb] (set bits in every chunk before that superblock),
 * then fine-walk at most `stride` chunks -- from the superblock's first chunk
 * up to and including the chunk containing x -- adding each chunk's set bits
 * in its overlap with [0, x].  O(log n_sb + stride) = O(sqrt count).  Chunks
 * are window-aligned and ascending, so the superblock boundary never splits a
 * chunk and sb_prefix is exact.
 */
static size_t
sbm_locator_rank_upto(const SbmLocator *loc, uint64 x)
{
	const Sbm  *map = loc->map;
	uint8	   *base = sbm_get_chunk_data(map, 0);
	const size_t stream_end =
		(size_t) map->m_data_used - SBM_SIZEOF_OVERHEAD;
	const size_t sb = sbm_locator_find_sb(loc, x);
	size_t		set = loc->sb_prefix[sb];
	uint8	   *p = base + loc->sb_offset[sb];

	for (;;)
	{
		const SbmIdx s = sbm_load_idx((const uint8 *) p);
		SbmChunk	chunk;
		size_t		cap;
		uint64		chunk_lo;
		uint64		span;
		uint64		chunk_hi_incl;
		uint64		ov_hi_incl;
		size_t		to;
		size_t		next_off;

		if ((uint64) s > x)
		{
			break;				/* chunk starts past x: nothing more to count */
		}
		sbm_chunk_init(&chunk, p + SBM_SIZEOF_OVERHEAD);
		cap = sbm_chunk_get_capacity(&chunk);
		chunk_lo = (uint64) s;
		span = (uint64) cap - 1;
		chunk_hi_incl =
			(chunk_lo > PG_UINT64_MAX - span) ? PG_UINT64_MAX
			: chunk_lo + span;
		ov_hi_incl = (x < chunk_hi_incl) ? x : chunk_hi_incl;
		to = (size_t) (ov_hi_incl - chunk_lo);
		set += sbm_chunk_rank(&chunk, 0, to);
		next_off = (size_t) (p - base) +
			SBM_SIZEOF_OVERHEAD + sbm_chunk_get_size(&chunk);
		if (next_off >= stream_end)
		{
			break;
		}
		p = base + next_off;
	}
	return set;
}

bool
sbm_locator_contains(const SbmLocator *loc, uint64 idx)
{
	const Sbm  *map;
	uint8	   *base;
	size_t		stream_end;
	size_t		sb;
	uint8	   *p;

	/* A stale (or small-map) locator falls back to the plain lookup. */
	if (sbm_locator_is_stale(loc))
		return sbm_contains(loc ? loc->map : NULL, idx, NULL);

	map = loc->map;
	base = sbm_get_chunk_data(map, 0);
	stream_end =
		(size_t) map->m_data_used - SBM_SIZEOF_OVERHEAD;

	sb = sbm_locator_find_sb(loc, idx);
	p = base + loc->sb_offset[sb];

	/*
	 * Fine-walk at most `stride` chunks from the superblock's first chunk to
	 * the chunk covering idx (same shape as sbm_get_chunk_offset, but
	 * bounded).
	 */
	for (;;)
	{
		const SbmIdx s = sbm_load_idx((const uint8 *) p);
		SbmChunk	chunk;
		size_t		cap;
		size_t		next_off;

		sbm_chunk_init(&chunk, p + SBM_SIZEOF_OVERHEAD);
		cap = sbm_chunk_get_capacity(&chunk);
		if (idx < (uint64) s)
		{
			return false;		/* gap before this chunk */
		}
		if (idx < (uint64) s + cap)
		{
			return sbm_chunk_is_set(&chunk, idx - (uint64) s);
		}
		next_off = (size_t) (p - base) +
			SBM_SIZEOF_OVERHEAD + sbm_chunk_get_size(&chunk);
		if (next_off >= stream_end)
		{
			return false;		/* past the last chunk */
		}
		p = base + next_off;
	}
}

size_t
sbm_locator_rank(const SbmLocator *loc, uint64 lo, uint64 hi, bool value)
{
	size_t		hi_cnt;
	size_t		lo_cnt;

	/*
	 * value=false and staleness both fall back to the plain path: the unset
	 * count needs the range width (which the sqrt index does not carry), and
	 * a stale index must never return a wrong answer.
	 */
	if (value == false || sbm_locator_is_stale(loc))
	{

		/*
		 * A NULL (or map-less) locator has nothing to count.  Do not hand
		 * NULL to sbm_rank: it does not accept one, so the `loc ? loc->map :
		 * NULL` guard below used to turn a NULL locator into a NULL
		 * dereference inside sbm_rank_range. sbm_locator_contains already
		 * treats NULL as "no bits".
		 */
		if (loc == NULL || loc->map == NULL)
		{
			return 0;
		}
		return sbm_rank(loc->map, lo, hi, value);
	}
	if (lo > hi)
	{
		return 0;
	}

	/*
	 * Set bits in [lo, hi] = rank_upto(hi) - rank_upto(lo - 1).  Each
	 * rank_upto jumps straight to the target's superblock, seeds the count
	 * from sb_prefix[] (all set bits in chunks before that superblock -- the
	 * whole point of the prefix table), and fine-walks at most `stride`
	 * chunks from there.  That is the O(sqrt n) path; seeding from chunk 0 as
	 * the previous version did made this an O(chunks) no-op identical to
	 * plain sbm_rank.
	 */
	hi_cnt = sbm_locator_rank_upto(loc, hi);
	lo_cnt =
		(lo == 0) ? 0 : sbm_locator_rank_upto(loc, lo - 1);
	return hi_cnt - lo_cnt;
}

uint64
sbm_locator_select(const SbmLocator *loc, uint64 n, bool value)
{
	const Sbm  *map;
	uint8	   *base;
	size_t		stream_end;
	size_t		sb = 0;
	ssize_t		rem;
	uint8	   *p;

	/*
	 * value=false and staleness fall back to sbm_select: unset select needs
	 * the leading-zeros / cross-chunk gap accounting the sqrt prefix does not
	 * carry, and a stale index must stay correct.
	 */
	if (value == false || sbm_locator_is_stale(loc))
	{

		/*
		 * As in sbm_locator_rank: sbm_select does not accept a NULL map, so
		 * answer "not found" directly instead of passing one through.
		 */
		if (loc == NULL || loc->map == NULL)
		{
			return SBM_IDX_MAX;
		}
		return sbm_select(loc->map, n, value);
	}

	map = loc->map;
	base = sbm_get_chunk_data(map, 0);
	stream_end =
		(size_t) map->m_data_used - SBM_SIZEOF_OVERHEAD;

	/*
	 * Find the last superblock whose cumulative set-bit prefix is <= n,
	 * subtract that prefix, and fine-walk from its first chunk.  The
	 * per-chunk select semantics mirror sbm_select exactly.
	 */
	{
		size_t		l = 0,
					r = loc->n_sb;	/* largest sb with prefix<=n */

		while (l < r)
		{
			const size_t mid = l + (r - l) / 2;

			if ((uint64) loc->sb_prefix[mid] <= n)
			{
				l = mid + 1;
			}
			else
			{
				r = mid;
			}
		}
		sb = (l == 0) ? 0 : l - 1;
	}

	rem = (ssize_t) (n - (uint64) loc->sb_prefix[sb]);
	p = base + loc->sb_offset[sb];
	for (;;)
	{
		const SbmIdx s = sbm_load_idx((const uint8 *) p);
		SbmChunk	chunk;
		ssize_t		new_n;
		size_t		index;
		size_t		next_off;

		sbm_chunk_init(&chunk, p + SBM_SIZEOF_OVERHEAD);
		new_n = rem;
		index = sbm_chunk_select(&chunk, rem, &new_n, value);
		if (new_n == -1)
		{
			return (uint64) s + index;
		}
		rem = new_n;
		next_off = (size_t) (p - base) +
			SBM_SIZEOF_OVERHEAD + sbm_chunk_get_size(&chunk);
		if (next_off >= stream_end)
		{
			return SBM_IDX_MAX;
		}
		p = base + next_off;
	}
}

/* -------------------------------------------------------------------
 * Idea 3: SbmCursorCached -- fixed 8-way MRU chunk cache.
 * -------------------------------------------------------------------
 */

bool
sbm_contains_cached(const Sbm *map, uint64 idx, SbmCursorCached *cache)
{
	uint8		w;
	ssize_t		offset;
	uint8	   *p;
	SbmIdx		start;
	SbmChunk	chunk;
	size_t		cap;
	uint8		slot;

	if (map == NULL)
	{
		return false;
	}
	if (cache == NULL || sbm_is_small(map))
	{
		return sbm_contains(map, idx, NULL);
	}

	/* 1. Probe the <=8 valid ways for a covering chunk (a hit). */
	for (w = 0; w < SBM_CACHE_WAYS; w++)
	{
		if ((cache->valid & (uint8) (1u << w)) == 0)
		{
			continue;
		}
		if (idx >= cache->start_idx[w] && idx < cache->end_idx[w])
		{
			uint8	   *hp =
				sbm_get_chunk_data(map, cache->offset[w]);
			SbmChunk	hc;

			sbm_chunk_init(&hc, hp + SBM_SIZEOF_OVERHEAD);
			return sbm_chunk_is_set(&hc,
									idx - cache->start_idx[w]);
		}
	}

	/* 2. Miss: walk from the head, then insert the located chunk. */
	offset = sbm_get_chunk_offset(map, idx, NULL);
	if (offset == -1)
	{
		return false;
	}
	p = sbm_get_chunk_data(map, (size_t) offset);
	start = sbm_load_idx((const uint8 *) p);
	sbm_chunk_init(&chunk, p + SBM_SIZEOF_OVERHEAD);
	cap = sbm_chunk_get_capacity(&chunk);

	/* 3. Insert (start, start+cap, offset) at the round-robin slot. */
	slot = cache->mru;
	cache->start_idx[slot] = (uint64) start;
	cache->end_idx[slot] = (uint64) start + cap;
	cache->offset[slot] = (size_t) offset;
	cache->valid |= (uint8) (1u << slot);
	cache->mru = (uint8) ((slot + 1) % SBM_CACHE_WAYS);

	/*
	 * Out of bounds of the located chunk -> not set (matches sbm_contains).
	 */
	if (idx < (uint64) start || idx - (uint64) start >= cap)
	{
		return false;
	}
	return sbm_chunk_is_set(&chunk, idx - (uint64) start);
}

#ifdef SBM_GENERATE_VECTOR_SIZE_TABLE
/*
 * Program to regenerate the lookup table in sbm_chunk_calc_vector_size().
 * It uses only the C library, so it can be built straight from this file:
 *
 *		cc -DSBM_GENERATE_VECTOR_SIZE_TABLE -o gen -x c - < \
 *			<(sed -n '/^#ifdef SBM_GENERATE_VECTOR_SIZE_TABLE/,/^#endif/p' sbm.c)
 *		./gen
 *
 * and its output pasted between the braces of the table.
 */
#include <stdio.h>

int
main(void)
{
	for (int b = 0; b < 256; b++)
	{
		int			n = 0;

		/* count the SBM_PAYLOAD_MIXED (2#10) flags among the four in b */
		for (int f = 0; f < 4; f++)
			n += ((b >> (f * 2)) & 3) == 2;
		printf("%s%d%s", b % 16 == 0 ? "\t\t" : " ", n,
			   b == 255 ? "\n" : b % 16 == 15 ? ",\n" : ",");
	}
	return 0;
}
#endif							/* SBM_GENERATE_VECTOR_SIZE_TABLE */
