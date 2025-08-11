/*-------------------------------------------------------------------------
 *
 * bitmapset.c
 *	  PostgreSQL generic bitmap set package
 *
 * A bitmap set can represent any set of non-negative integers.
 *
 * Callers must ensure that the set returned by functions in this file which
 * adjust the members of an existing set is assigned to all pointers pointing
 * to that existing set.  No guarantees are made that we'll ever modify the
 * existing set in-place and return it.
 *
 * To help find bugs caused by callers failing to record the return value of
 * the function which manipulates an existing set, we support building with
 * REALLOCATE_BITMAPSETS.  This results in the set being reallocated each time
 * the set is altered and the existing being pfreed.  This is useful as if any
 * references still exist to the old set, we're more likely to notice as
 * any users of the old set will be accessing pfree'd memory.  This option is
 * only intended to be used for debugging.
 *
 * Copyright (c) 2003-2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/nodes/bitmapset.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <limits.h>

#ifdef __AVX2__
#include <immintrin.h>
#endif

#include "common/hashfn.h"
#include "nodes/bitmapset.h"
#include "nodes/pg_list.h"
#include "port/pg_bitutils.h"

/*
 * Platform-dependent bitvector configuration
 *
 * We use 64-bit vectors on 64-bit platforms for better performance,
 * falling back to 32-bit vectors on 32-bit platforms.
 */
#if SIZEOF_VOID_P >= 8
#define BMS_BITVEC_BITS         64
#define BMS_BITVEC_MAX          UINT64_MAX
#define bms_pg_popcount         pg_popcount64
typedef uint64 bms_bitvec_t;
#else
#define BMS_BITVEC_BITS         32
#define BMS_BITVEC_MAX          UINT32_MAX
#define bms_pg_popcount         pg_popcount32
typedef uint32 bms_bitvec_t;
#endif

#define BMS_ASSERT_ALIGNED(ptr) \
    Assert(((uintptr_t)(ptr) & (MAXIMUM_ALIGNOF - 1)) == 0)

#define BMS_IS_INTEGRATED_ALLOCATION(map) ((uintptr_t) map->data == MAXALIGN((uintptr_t) map + sizeof(Bitmapset)))

/*
 * Basic configuration constants
 */
#define BMS_DEFAULT_SIZE        1024

/*
 * Chunk storage layout constants
 *
 * Each chunk stores metadata overhead (chunk count) followed by
 * variable-length compressed chunk data.
 */
#define BMS_SIZEOF_OVERHEAD     sizeof(uint32)	/* chunk count storage */

/*
 * Flag encoding constants
 *
 * Each flag uses 2 bits to encode payload type, allowing 4 flags
 * per byte and BMS_BITVEC_BITS/2 flags per bitvector.
 */
#define BMS_CHUNK_FLAG_BYTES sizeof(bms_bitvec_t)
#define BMS_FLAGS_PER_INDEX_BYTE    4	/* 8 bits / 2 bits per flag */
#define BMS_FLAGS_PER_INDEX     (BMS_CHUNK_FLAG_BYTES * BMS_FLAGS_PER_INDEX_BYTE)
#define BMS_CHUNK_MAX_CAPACITY  (BMS_BITVEC_BITS * BMS_FLAGS_PER_INDEX)
#define BMS_CHUNK_INITIAL_SIZE (sizeof(chunk_off_t) + sizeof(bms_bitvec_t) * 2)

/*
 * Payload type flags (2 bits each)
 *
 * These values encode the type of data stored for each bitvector
 * within a chunk's compressed representation.
 */
#define BMS_PAYLOAD_ZEROS       0x0 /* all bits are 0 */
#define BMS_PAYLOAD_NONE        0x1 /* no data (unused vector) */
#define BMS_PAYLOAD_MIXED       0x2 /* mixed 0s and 1s (stored) */
#define BMS_PAYLOAD_ONES        0x3 /* all bits are 1 */

#define BMS_FLAG_MASK           0x3 /* mask for 2-bit flags */

/*
 * Operation result codes for chunk modification functions
 */
#define BMS_OK                  0
#define BMS_NEEDS_TO_GROW       1
#define BMS_NEEDS_TO_SHRINK     -1

/*
 * Size estimation and limits
 */
#define BMS_AVG_CHUNK_SIZE      (sizeof(chunk_off_t) + BMS_CHUNK_FLAG_BYTES * 3)
#define BMS_MAX_BITMAP_SIZE     ((size_t) UINT32_MAX)

/*
 * Type definitions
 */
typedef uint64 bms_idx_t;		/* bit index type */
typedef uint32 chunk_off_t;		/* chunk offset type */

/*
 * Chunk manipulation macros
 *
 * These macros provide efficient access to flag bits and chunk indexing.
 * Optimizing compilers should convert the division by a constant known to be a
 * power of two into a bitshift, so keep it simple for clarity here.
 */
#define BMS_VECTOR_INDEX(x) \
    ((x) / BMS_BITVEC_BITS)

#define BMS_CHUNK_EXTRACT_FLAGS(byte, x) \
    (((byte) & ((bms_bitvec_t) BMS_FLAG_MASK << ((x) * 2))) >> ((x) * 2))

#define BMS_CHUNK_FLAGS(chunk, x) \
    BMS_CHUNK_EXTRACT_FLAGS(*(chunk)->data, BMS_VECTOR_INDEX(x))

/*
 * Internal chunk structure
 *
 * Represents a view into compressed bitmap chunk data. The data pointer
 * points to the beginning of a chunk's flag bitvector, followed by
 * payload data for MIXED vectors.
 */
typedef struct bms_chunk_t
{
	bms_bitvec_t *data;			/* pointer to chunk's flag bitvector */
}			bms_chunk_t;

/*
 * Static function prototypes
 *
 * Organized by functional area for better maintainability.
 */

/* Chunk initialization and basic operations */
static void bms_chunk_init(bms_chunk_t * chunk, uint8 *data);
static size_t bms_chunk_get_capacity(const bms_chunk_t * chunk);
static size_t bms_chunk_get_size(const bms_chunk_t * chunk);
static bool bms_chunk_is_empty(const bms_chunk_t * chunk);
static bool bms_chunk_is_member(const bms_chunk_t * chunk, int bit_index);
static bms_bitvec_t bms_chunk_get_mixed_payload(bms_chunk_t * chunk, size_t byte_idx, size_t flag_idx);

/* Chunk capacity management */
static void bms_chunk_reduce_capacity(bms_chunk_t * chunk, size_t target);
static void bms_chunk_increase_capacity(bms_chunk_t * chunk, size_t target);

/* Chunk bit manipulation */
static int	bms_chunk_set_bit(bms_chunk_t * chunk, size_t bit_index, bool value,
							  size_t *pos, bms_bitvec_t * fill, bool retried);

/* Position calculation and optimization */
static size_t bms_chunk_get_position(const bms_chunk_t * chunk, uint32 n);
static size_t bms_chunk_count_mixed_vectors_lookup(const uint8 *flags_data,
												   size_t num_bytes);
static size_t bms_chunk_count_mixed_vectors_avx2(const uint8 *flags_data,
												 size_t num_bytes);
static size_t bms_chunk_count_mixed_flags_branchless(uint8 byte_flags,
													 size_t count);

/* Chunk scanning and selection operations */
static size_t bms_chunk_select(bms_chunk_t * chunk, size_t n, size_t *remaining,
							   bool value);
static size_t bms_chunk_rank_range(bms_chunk_t * chunk, size_t *start_offset,
								   size_t end_offset, size_t *bits_processed,
								   bms_bitvec_t * last_vector, bool value);
static void bms_scan(const Bitmapset *map,
					 void (*scanner) (bms_idx_t bit_indices[], size_t count, void *aux_data),
					 size_t skip_count,
					 void *aux_data);
static size_t bms_chunk_scan_set_bits(bms_chunk_t * chunk, bms_idx_t base_index,
									  void (*scanner) (bms_idx_t[], size_t, void *),
									  size_t skip_count, void *aux);

/* Bitmap structure management */
static size_t bms_get_chunk_count(const Bitmapset *map);
static void bms_set_chunk_count(Bitmapset *map, size_t new_count);
static uint8 *bms_get_chunk_data(const Bitmapset *map, size_t offset);
static uint8 *bms_get_chunk_end_ptr(Bitmapset *map);

/* Bitmap data manipulation */
static void bms_append_data(Bitmapset *map, const uint8 *buffer, size_t size);
static void bms_insert_data(Bitmapset *map, size_t offset, const uint8 *buffer,
							size_t size);
static void bms_remove_data(Bitmapset *map, size_t offset, size_t size);

/* Bitmap operations */
static size_t bms_find_chunk_offset(const Bitmapset *map, size_t target_bit_index);
static size_t bms_get_vector_aligned_offset(size_t bit_index);
static size_t bms_get_chunk_aligned_offset(size_t bit_index);

/* High-level bitmap operations */
static int	bms_set_bit(Bitmapset *map, int bit_index, bool new_state);
static void bms_merge_chunk_bits(Bitmapset *map, size_t src_start_bit,
								 size_t dst_start_bit, size_t merge_capacity,
								 bms_chunk_t * dst_chunk, const bms_chunk_t * src_chunk);

/* Utility functions */
static int	bms_chunk_calc_vector_size(uint8 b);

#ifdef USE_ASSERT_CHECKING
/* Debug and validation functions */
static void bms_chunk_verify_position_methods(const bms_chunk_t * chunk, size_t n);
static bool bms_is_valid_set(const Bitmapset *map);

bool
bms_is_valid_set(const Bitmapset *map)
{
	size_t		calculated_used;

	/* NULL is the correct representation of an empty set */
	if (map == NULL)
		return true;

	/* Check the node tag is set correctly */
	if (!IsA(map, Bitmapset))
		return false;

	/* Validate basic structure */
	if (map->size == 0 || map->data == NULL)
		return false;

	/* Ensure the used size doesn't exceed the allocated size */
	if (map->used > map->size)
		return false;

	/* Ensure minimum overhead is present */
	if (map->used < BMS_SIZEOF_OVERHEAD)
		return false;

	/* Validate chunk count consistency */
	//	calculated_used = bms_get_used_size(map);

	//	if (calculated_used != map->used)
	//		return false;

	return true;
}
#else
#define bms_is_valid_set(map)   true
#endif

/*
 * bms_chunk_calc_vector_size
 *      Calculate number of bms_bitvec_t's required by a single flag byte
 *
 * Uses a precomputed lookup table to determine how many MIXED payload
 * vectors are encoded in the given flag byte. Each byte contains 4 flags
 * (2 bits each), and only MIXED flags (value 0b10) require storage.
 */
static int
bms_chunk_calc_vector_size(uint8 b)
{
	/*
	 * Lookup table mapping each possible byte value (0-255) to the count of
	 * MIXED payload flags it contains. Each flag is 2 bits, so each byte
	 * contains 4 flags.
	 */
	static const int lookup[] = {
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
 * bms_chunk_get_position
 *      Calculate the byte position of a bit vector within compressed chunk data
 *
 * Each chunk stores bit vectors in a compressed format where only MIXED payload
 * vectors consume storage space. This function calculates the byte offset where
 * the n-th bit vector's data begins.
 *
 * The implementation uses multiple optimization strategies:
 * - AVX2 vectorization for bulk processing when available
 * - Branchless bit manipulation for improved performance
 * - Pre-computed per-byte lookup table
 *
 * In debug builds, all three methods are exercised and compared to ensure
 * consistent results.
 *
 * Parameters:
 *  chunk: The bitmap chunk containing compressed bit vectors
 *  n: Zero-based index of the bit vector to locate within data
 *
 * Returns:
 *  Byte position in data where the vector's data starts
 */
static size_t
bms_chunk_get_position(const bms_chunk_t * chunk, uint32 n)
{
	const uint8 *flags_data = (const uint8 *) chunk->data;
	size_t		complete_bytes;
	size_t		remaining_flags;
	size_t		position = 0;

#ifdef USE_ASSERT_CHECKING
	/* In debug builds, verify all methods produce identical results */
	bms_chunk_verify_position_methods(chunk, n);
#endif

	/*
	 * Calculate how many complete bytes and remaining flags to process.
	 */
	complete_bytes = n / BMS_FLAGS_PER_INDEX_BYTE;
	remaining_flags = n % BMS_FLAGS_PER_INDEX_BYTE;

	/* Process complete bytes using the most efficient method */
#ifdef __AVX2__
	position += bms_chunk_count_mixed_vectors_avx2(flags_data, complete_bytes);
#else
	position += bms_chunk_count_mixed_vectors_lookup(flags_data, complete_bytes);
#endif

	/* Process remaining individual flags */
	if (remaining_flags > 0)
	{
		const uint8 last_byte = flags_data[complete_bytes];

		position += bms_chunk_count_mixed_flags_branchless(last_byte,
														   remaining_flags);
	}

	return position;
}

/*
 * bms_chunk_get_position_precomputed
 *      Lookup table implementation used for verification of other algorithms
 *
 * This algorithm processes flags byte-by-byte using a lookup table for complete
 * bytes, then individual flag extraction for the remainder.
 */
static size_t
bms_chunk_get_position_precomputed(const bms_chunk_t * chunk, size_t n)
{
	size_t		num_bytes;
	size_t		position = 0;
	uint8	   *p;
	size_t		i;

	/* Handle 4 flags within a byte at a time */
	num_bytes = n / ((size_t) BMS_FLAGS_PER_INDEX_BYTE * BMS_BITVEC_BITS);

	p = (uint8 *) chunk->data;
	for (i = 0; i < num_bytes; i++, p++)
		position += bms_chunk_calc_vector_size(*p);

	n -= num_bytes * BMS_FLAGS_PER_INDEX_BYTE;
	for (i = 0; i < n; i++)
		if (BMS_CHUNK_FLAGS(chunk, i) == BMS_PAYLOAD_MIXED)
			position++;

	return position;
}

/*
 * bms_chunk_count_mixed_vectors_lookup
 *      Count MIXED vectors in flag bytes using lookup table
 *
 * Processes each byte using the precomputed lookup table to determine
 * how many MIXED payload vectors it contains. This is the fallback
 * method when AVX2 is not available.
 */
static size_t
bms_chunk_count_mixed_vectors_lookup(const uint8 *flags_data, size_t num_bytes)
{
	size_t		total_mixed = 0;
	size_t		i;

	for (i = 0; i < num_bytes; i++)
		total_mixed += bms_chunk_calc_vector_size(flags_data[i]);

	return total_mixed;
}

/*
 * bms_chunk_count_mixed_vectors_avx2
 *      Count MIXED vectors using AVX2 vectorization
 *
 * Processes 32 bytes at a time using AVX2 SIMD instructions for improved
 * performance on modern CPUs. Falls back to lookup table method for
 * remaining bytes that don't fill a complete AVX2 register.
 *
 * The MIXED flag pattern is 0b10 (value 2), which appears as 0xAA when
 * replicated across flag bit positions in a byte (10101010).
 */
static size_t
bms_chunk_count_mixed_vectors_avx2(const uint8 *flags_data, size_t num_bytes)
{
	size_t		total_mixed = 0;
	size_t		i = 0;

#ifdef __AVX2__
	const size_t avx_chunk_size = 32;

	/* Process 32 bytes at a time with AVX2 */
	for (; i + avx_chunk_size <= num_bytes; i += avx_chunk_size)
	{
		__m256i		data;
		__m256i		flag0,
					flag1;
		__m256i		mixed0,
					mixed1;
		uint32		mixed_mask0,
					mixed_mask1;

		/* Load 32 bytes of flag data */
		data = _mm256_loadu_si256((const __m256i *) (flags_data + i));

		/*
		 * Extract the two flag bits for each 2-bit flag. MIXED flags have the
		 * pattern 0b10, so we check bit 1 is set and bit 0 is clear.
		 */
		flag0 = _mm256_and_si256(data, _mm256_set1_epi8(0x55)); /* bit 0 of each flag */
		flag1 = _mm256_and_si256(data, _mm256_set1_epi8(0xAA)); /* bit 1 of each flag */

		/* MIXED flags: bit 1 set, bit 0 clear */
		mixed0 = _mm256_cmpeq_epi8(flag0, _mm256_setzero_si256());
		mixed1 = _mm256_cmpeq_epi8(flag1, _mm256_set1_epi8(0xAA));

		/* Combine conditions: both must be true for MIXED flag */
		mixed0 = _mm256_and_si256(mixed0, mixed1);

		/* Count matching positions */
		mixed_mask0 = _mm256_movemask_epi8(mixed0);
		total_mixed += pg_popount32(mixed_mask0);
	}
#endif

	/* Process remaining bytes with lookup table */
	total_mixed += bms_chunk_count_mixed_vectors_lookup(flags_data + i,
														num_bytes - i);

	return total_mixed;
}

/*
 * bms_chunk_count_mixed_flags_branchless
 *      Count MIXED flags in a byte using branchless bit manipulation
 *
 * Examines each 2-bit flag in the byte to count those with MIXED payload
 * (value 2 = 0b10). Uses branchless comparisons to avoid pipeline stalls
 * from conditional branches.
 *
 * Parameters:
 *  byte_flags: The flag byte containing up to 4 flags (2 bits each)
 *  count: Number of flags to examine (1-4)
 *
 * Returns:
 *  Number of MIXED flags found in the specified range
 */
static size_t
bms_chunk_count_mixed_flags_branchless(uint8 byte_flags, size_t count)
{
	size_t		mixed_count = 0;
	uint8		count_mask;

	/* Create mask for the flags we want to check */
	count_mask = (uint8) ((1 << (count * 2)) - 1);
	byte_flags &= count_mask;

	/*
	 * Check each 2-bit pair for the MIXED pattern (0b10). Use branchless
	 * comparisons to avoid conditional jumps.
	 */
	mixed_count += ((byte_flags & 0x03) == 0x02) ? 1 : 0;	/* bits 0-1 */
	if (count > 1)
		mixed_count += ((byte_flags & 0x0C) == 0x08) ? 1 : 0;	/* bits 2-3 */
	if (count > 2)
		mixed_count += ((byte_flags & 0x30) == 0x20) ? 1 : 0;	/* bits 4-5 */
	if (count > 3)
		mixed_count += ((byte_flags & 0xC0) == 0x80) ? 1 : 0;	/* bits 6-7 */

	return mixed_count;
}

#ifdef USE_ASSERT_CHECKING
/*
 * bms_chunk_verify_position_methods
 *      Verify that all position calculation methods return identical results
 *
 * In debug builds, this function tests all three implementation methods
 * (lookup, AVX2, branchless) to ensure they produce consistent results.
 * Any discrepancy indicates a bug in one of the implementations.
 */
static void
bms_chunk_verify_position_methods(const bms_chunk_t * chunk, size_t n)
{
	size_t		pos_original;
	size_t		pos_lookup;
	size_t		pos_avx2;
	const uint8 *flags_data = (const uint8 *) chunk->data;
	size_t		complete_bytes = n / BMS_FLAGS_PER_INDEX_BYTE;
	size_t		remaining_flags = n % BMS_FLAGS_PER_INDEX_BYTE;

	/* Test pre-computed lookup table method */
	pos_original = bms_chunk_get_position_precomputed(chunk, n);

	/* Test lookup table method */
	pos_lookup = bms_chunk_count_mixed_vectors_lookup(flags_data, complete_bytes);

	/* Test AVX2 method */
	pos_avx2 = bms_chunk_count_mixed_vectors_avx2(flags_data, complete_bytes);

	/* Count remaining flags and add to position */
	if (remaining_flags > 0)
	{
		size_t		remaining_count;
		const uint8 last_byte = flags_data[complete_bytes];

		remaining_count = bms_chunk_count_mixed_flags_branchless(last_byte,
																 remaining_flags);
		pos_lookup += remaining_count;
		pos_avx2 += remaining_count;
	}

	/* Verify all methods agree */
	if (pos_original != pos_lookup || pos_original != pos_avx2)
		elog(ERROR, "bms_chunk_get_position methods disagree: original=%zu, lookup=%zu, avx2=%zu, n=%zu",
			 pos_original, pos_lookup, pos_avx2, n);
}
#endif							/* USE_ASSERT_CHECKING */

/*
 * bms_chunk_init
 *      Initialize a bitmap chunk structure with the given data pointer
 *
 * This function sets up a bms_chunk_t to point to a specific location in
 * the bitmap's serialized data. The chunk acts as a view into the compressed
 * bitmap data, where flags and payload vectors are stored sequentially.
 *
 * The data layout within a chunk is:
 *   - First bms_bitvec_t: Flag bits (2 bits per vector, up to 32/64 vectors)
 *   - Subsequent data: Only MIXED payload vectors (compressed storage)
 */
static inline void
bms_chunk_init(bms_chunk_t * chunk, uint8 *data)
{
	Assert(chunk != NULL);
	Assert(data != NULL);

	chunk->data = (bms_bitvec_t *) data;
}

/*
 * bms_chunk_get_capacity
 *      Calculate the current usable capacity of a bitmap chunk
 *
 * Examines the flag bits in the chunk to determine how many bit positions are
 * currently available for use. The capacity is reduced by the number of vectors
 * marked as BMS_PAYLOAD_NONE (unused).
 *
 * The chunk starts with a theoretical maximum capacity of
 * BMS_CHUNK_MAX_CAPACITY bits, but unused vector positions (flagged as NONE)
 * reduce the effective capacity.
 */
static size_t
bms_chunk_get_capacity(const bms_chunk_t * chunk)
{
	const uint8 *flags_data;
	size_t		capacity;
	int			i;

	Assert(chunk != NULL);
	Assert(chunk->data != NULL);

	flags_data = (const uint8 *) chunk->data;
	capacity = BMS_CHUNK_MAX_CAPACITY;

	/*
	 * Scan through each flag byte in the chunk's flag bitvector. Each byte
	 * contains BMS_FLAGS_PER_INDEX_BYTE flag pairs.
	 */
	for (i = 0; i < BMS_CHUNK_FLAG_BYTES; i++)
	{
		uint8		flag_byte = flags_data[i];
		int			j;

		/*
		 * Skip bytes that are all zeros (all PAYLOAD_ZEROS) or all 0xFF (all
		 * PAYLOAD_ONES with some PAYLOAD_MIXED). Neither contains
		 * PAYLOAD_NONE flags that would reduce capacity.
		 */
		if (flag_byte == 0x00 || flag_byte == 0xFF)
			continue;

		/* Examine each 2-bit flag within this byte */
		for (j = 0; j < BMS_FLAGS_PER_INDEX_BYTE; j++)
		{
			uint8		flag_value = BMS_CHUNK_EXTRACT_FLAGS(flag_byte, i);

			if (flag_value == BMS_PAYLOAD_NONE)
				capacity -= BMS_BITVEC_BITS;
		}
	}

	return capacity;
}

/*
 * bms_chunk_reduce_capacity
 *      Shrink a chunk's capacity by marking trailing vectors as unused
 *
 * Reduces the chunk's effective capacity by marking vectors as BMS_PAYLOAD_NONE
 * starting from the highest-indexed positions and working backwards. This is
 * used when a chunk needs to be shrunk to accommodate bitmap resizing.
 *
 * The function works backwards through the flag bits, converting active vectors
 * (ZEROS, ONES, MIXED) to NONE until the target capacity is reached. Only
 * complete bitvector boundaries are supported (target must be aligned to
 * BMS_BITVEC_BITS).
 *
 * Note: This function modifies the chunk's flag bits but does not deallocate
 * any payload data. Payload data for converted MIXED vectors becomes
 * unreachable but remains in memory.
 */
static void
bms_chunk_reduce_capacity(bms_chunk_t * chunk, size_t target)
{
	uint8	   *flags_data;
	size_t		current_capacity;
	size_t		vectors_to_remove;
	int			byte_idx;
	int			flag_idx;

	Assert(chunk != NULL);
	Assert(chunk->data != NULL);
	Assert(target % BMS_BITVEC_BITS == 0);
	Assert(target <= BMS_CHUNK_MAX_CAPACITY);

	/* Early exit if no reduction needed */
	current_capacity = bms_chunk_get_capacity(chunk);
	if (target >= current_capacity)
		return;

	flags_data = (uint8 *) chunk->data;
	vectors_to_remove = (current_capacity - target) / BMS_BITVEC_BITS;

	/*
	 * Work backwards through the flag bits, converting vectors to NONE. Start
	 * from the highest byte and highest flag within each byte.
	 */
	for (byte_idx = BMS_CHUNK_FLAG_BYTES - 1;
		 byte_idx >= 0 && vectors_to_remove > 0;
		 byte_idx--)
	{
		for (flag_idx = BMS_FLAGS_PER_INDEX_BYTE - 1;
			 flag_idx >= 0 && vectors_to_remove > 0;
			 flag_idx--)
		{
			uint8		flag_mask;
			uint8		current_flag;

			/* Extract current flag value */
			flag_mask = (uint8) (BMS_FLAG_MASK << (flag_idx * 2));
			current_flag = (flags_data[byte_idx] & flag_mask) >> (flag_idx * 2);

			/* Skip if already NONE */
			if (current_flag == BMS_PAYLOAD_NONE)
				continue;

			/* Clear the flag bits and set to NONE */
			flags_data[byte_idx] &= ~flag_mask;
			flags_data[byte_idx] |= (uint8) (BMS_PAYLOAD_NONE << (flag_idx * 2));

			vectors_to_remove--;
		}
	}

	/* Verify we achieved the target capacity */
	Assert(bms_chunk_get_capacity(chunk) == target);
}

/*
 * bms_chunk_increase_capacity
 *      Grow a chunk's capacity by converting unused vectors to active ones
 *
 * Increases the chunk's effective capacity by converting BMS_PAYLOAD_NONE
 * vectors to BMS_PAYLOAD_ZEROS, starting from the lowest-indexed positions
 * and working forwards. This is used when a chunk needs to grow to
 * accommodate bitmap expansion.
 *
 * The function scans forward through flag bits, converting NONE vectors
 * to ZEROS until the target capacity is reached. Only complete bitvector
 * boundaries are supported (target must be aligned to BMS_BITVEC_BITS).
 *
 * Note: Converted vectors become active ZEROS vectors, consuming no additional
 * payload storage but making those bit positions available for use.
 */
static void
bms_chunk_increase_capacity(bms_chunk_t * chunk, size_t target)
{
	uint8	   *flags_data;
	size_t		current_capacity;
	size_t		vectors_to_add;
	size_t		byte_idx;
	int			flag_idx;

	Assert(chunk != NULL);
	Assert(chunk->data != NULL);
	Assert(target % BMS_BITVEC_BITS == 0);
	Assert(target <= BMS_CHUNK_MAX_CAPACITY);

	current_capacity = bms_chunk_get_capacity(chunk);

	/* Verify we actually need to grow */
	Assert(target > current_capacity);

	/* Early exit if target is already met or invalid */
	if (target <= current_capacity || target > BMS_CHUNK_MAX_CAPACITY)
		return;

	flags_data = (uint8 *) chunk->data;
	vectors_to_add = (target - current_capacity) / BMS_BITVEC_BITS;

	/*
	 * Work forwards through the flag bits, converting NONE vectors to ZEROS.
	 * Start from the lowest byte and lowest flag within each byte.
	 */
	for (byte_idx = 0;
		 byte_idx < BMS_CHUNK_FLAG_BYTES && vectors_to_add > 0;
		 byte_idx++)
	{
		uint8		flag_byte = flags_data[byte_idx];

		/*
		 * Skip bytes that are all zeros (no NONE flags) or all 0xFF (no NONE
		 * flags). We're only looking for NONE flags to convert.
		 */
		if (flag_byte == 0x00 || flag_byte == 0xFF)
			continue;

		/* Examine each 2-bit flag within this byte */
		for (flag_idx = 0;
			 flag_idx < BMS_FLAGS_PER_INDEX_BYTE && vectors_to_add > 0;
			 flag_idx++)
		{
			uint8		flag_mask;
			uint8		current_flag;

			/* Extract current flag value */
			flag_mask = (uint8) (BMS_FLAG_MASK << (flag_idx * 2));
			current_flag = (flag_byte & flag_mask) >> (flag_idx * 2);

			/* Convert NONE to ZEROS */
			if (current_flag == BMS_PAYLOAD_NONE)
			{
				/* Clear the flag bits and set to ZEROS */
				flags_data[byte_idx] &= ~flag_mask;
				flags_data[byte_idx] |= (uint8) (BMS_PAYLOAD_ZEROS << (flag_idx * 2));

				vectors_to_add--;
			}
		}
	}

	/* Verify we achieved the target capacity */
	Assert(bms_chunk_get_capacity(chunk) == target);
}

/*
 * bms_chunk_is_empty
 *      Determine if a bitmap chunk contains any set bits
 *
 * A chunk is considered empty if it contains no set bits across all its
 * active vectors. This occurs when all flag bits indicate either:
 *   - BMS_PAYLOAD_ZEROS (vector exists but contains all zeros)
 *   - BMS_PAYLOAD_NONE (vector position is unused)
 *
 * Any vectors marked as BMS_PAYLOAD_ONES or BMS_PAYLOAD_MIXED indicate
 * the chunk contains at least one set bit, making it non-empty.
 *
 * Returns:
 *  true if the chunk contains no set bits, false otherwise
 */
static bool
bms_chunk_is_empty(const bms_chunk_t * chunk)
{
	const uint8 *flags_data;
	int			byte_idx;

	Assert(chunk != NULL);
	Assert(chunk->data != NULL);

	flags_data = (const uint8 *) chunk->data;

	/*
	 * Quick check: if the entire flag bitvector is zero, all vectors are
	 * either ZEROS or NONE, so the chunk is definitely empty.
	 */
	if (chunk->data[0] == 0)
		return true;

	/*
	 * Scan each flag byte in the chunk's flag bitvector. We need to examine
	 * individual flags to distinguish between ZEROS (0b00) and other payload
	 * types.
	 */
	for (byte_idx = 0; byte_idx < BMS_CHUNK_FLAG_BYTES; byte_idx++)
	{
		uint8		flag_byte = flags_data[byte_idx];
		int			flag_idx;

		/* Skip zero bytes - they contain only ZEROS flags */
		if (flag_byte == 0x00)
			continue;

		/* Examine each 2-bit flag within this byte */
		for (flag_idx = 0; flag_idx < BMS_FLAGS_PER_INDEX_BYTE; flag_idx++)
		{
			uint8		flag_value = BMS_CHUNK_EXTRACT_FLAGS(flag_byte, flag_idx);

			/*
			 * If we find any ONES or MIXED flags, the chunk is not empty.
			 * ZEROS and NONE flags don't contribute any set bits.
			 */
			if (flag_value == BMS_PAYLOAD_ONES ||
				flag_value == BMS_PAYLOAD_MIXED)
				return false;
		}
	}

	return true;
}

/*
 * bms_chunk_get_size
 *      Calculate the total size in bytes of a bitmap chunk
 *
 * Determines the storage size of a chunk by counting the flag bitvector
 * plus all MIXED payload vectors. The chunk layout is:
 *   - First bms_bitvec_t: Flag bits (always present)
 *   - Variable data: One bms_bitvec_t per MIXED payload vector
 *
 * Only MIXED vectors consume storage space beyond the flags. ZEROS, ONES,
 * and NONE payload types are represented by flags only.
 *
 * Returns:
 *  Total size of the chunk in bytes
 */
static size_t
bms_chunk_get_size(const bms_chunk_t * chunk)
{
	const uint8 *flags_data;
	size_t		total_size;
	size_t		mixed_vectors = 0;

	Assert(chunk != NULL);
	Assert(chunk->data != NULL);

	flags_data = (const uint8 *) chunk->data;

	/* Start with the size of the flag bitvector (always present) */
	total_size = BMS_CHUNK_FLAG_BYTES;

	/* Count MIXED vectors across all flag bytes */
#ifdef __AVX2__
	mixed_vectors += bms_chunk_count_mixed_vectors_avx2(flags_data, BMS_FLAGS_PER_INDEX_BYTE);
#else
	mixed_vectors += bms_chunk_count_mixed_vectors_lookup(flags_data, BMS_FLAGS_PER_INDEX_BYTE);
#endif

	/* Add storage for all MIXED payload vectors */
	total_size += mixed_vectors * BMS_CHUNK_FLAG_BYTES;

	return total_size;
}

/*
 * bms_chunk_is_member
 *      Test if a specific bit is set in the bitmap chunk
 *
 * Checks whether the bit at the given index is set within the chunk.
 * The function first examines the payload type flag for the vector
 * containing the bit, then accesses the actual data if needed.
 *
 * Payload type handling:
 *   - BMS_PAYLOAD_ZEROS: All bits are 0, return false immediately
 *   - BMS_PAYLOAD_ONES: All bits are 1, return true immediately
 *   - BMS_PAYLOAD_NONE: Vector unused, return false immediately
 *   - BMS_PAYLOAD_MIXED: Check the actual bit in stored data
 *
 * Returns:
 *  true if the bit is set, false otherwise
 */
static bool
bms_chunk_is_member(const bms_chunk_t * chunk, int bit_index)
{
	int			vector_index;
	uint8		payload_flags;
	int			bit_offset;
	bms_bitvec_t vector_data;
	size_t		data_position;

	Assert(chunk != NULL);
	Assert(chunk->data != NULL);
	Assert(bit_index >= 0);

	/* Determine which vector contains this bit */
	vector_index = BMS_VECTOR_INDEX(bit_index);

	/* Extract the payload type flags for this vector */
	payload_flags = (uint8) BMS_CHUNK_FLAGS(chunk, vector_index);

	/* Handle non-MIXED payload types immediately */
	if (payload_flags == BMS_PAYLOAD_ZEROS || payload_flags == BMS_PAYLOAD_NONE)
		return false;

	if (payload_flags == BMS_PAYLOAD_ONES)
		return true;

	/*
	 * For MIXED payload, locate and examine the stored vector data. The data
	 * follows the flag bitvector, with MIXED vectors stored sequentially in
	 * the order they appear in the flags.
	 */
	data_position = bms_chunk_get_position(chunk, vector_index);
	vector_data = chunk->data[1 + data_position];

	/* Check the specific bit (x % BMS_BITVEC_BITS) within the vector */
	bit_offset = bit_index & (BMS_BITVEC_BITS - 1);
	return (vector_data & ((bms_bitvec_t) 1 << bit_offset)) != 0;
}

/*
 * bms_chunk_set_bit
 *      Set or clear a specific bit in the bitmap chunk
 *
 * This function handles the complex logic of modifying bits in a compressed
 * bitmap chunk. It may need to convert between different payload types:
 *   - ZEROS/ONES to MIXED (requires storage allocation)
 *   - MIXED to ZEROS/ONES (allows storage deallocation)
 *
 * The function uses a two-phase protocol for storage changes:
 *   1. First call (retried=false): Returns growth/shrink requirements
 *   2. Second call (retried=true): Performs the actual bit modification
 *
 * Returns:
 *  BMS_OK: Operation completed successfully
 *  BMS_NEEDS_TO_GROW: Storage must be allocated before retrying
 *  BMS_NEEDS_TO_SHRINK: Storage can be deallocated after operation
 */
static int
bms_chunk_set_bit(bms_chunk_t * chunk, size_t bit_index, bool value,
				  size_t *pos, bms_bitvec_t * fill, bool retried)
{
	size_t		vector_index;
	uint8		current_flags;
	size_t		data_position;
	bms_bitvec_t vector_data;
	int			bit_offset;
	uint8		flag_mask;

	Assert(chunk != NULL);
	Assert(chunk->data != NULL);
	Assert(pos != NULL);
	Assert(fill != NULL);

	/* Determine which vector contains this bit */
	vector_index = BMS_VECTOR_INDEX(bit_index);

	/* Extract current payload flags for this vector */
	current_flags = (uint8) BMS_CHUNK_FLAGS(chunk, vector_index);
	Assert(current_flags != BMS_PAYLOAD_NONE);

	/*
	 * Handle ZEROS payload type
	 */
	if (current_flags == BMS_PAYLOAD_ZEROS)
	{
		/* Setting bit to 0 in all-zeros vector - no change needed */
		if (!value)
		{
			*pos = 0;
			*fill = 0;
			return BMS_OK;
		}

		/* Setting bit to 1 requires converting to MIXED */
		if (!retried)
		{
			/* Request storage allocation for the new MIXED vector */
			*pos = 1 + bms_chunk_get_position(chunk, vector_index);
			*fill = 0;			/* Initialize with zeros */
			return BMS_NEEDS_TO_GROW;
		}

		/* Convert ZEROS (0b00) to MIXED (0b10) */
		flag_mask = (uint8) (BMS_FLAG_MASK << (vector_index * 2));
		chunk->data[0] &= ~((bms_bitvec_t) flag_mask);
		chunk->data[0] |= ((bms_bitvec_t) BMS_PAYLOAD_MIXED << (vector_index * 2));
		/* Fall through to handle MIXED case */
	}

	/*
	 * Handle ONES payload type
	 */
	else if (current_flags == BMS_PAYLOAD_ONES)
	{
		/* Setting bit to 1 in all-ones vector - no change needed */
		if (value)
		{
			*pos = 0;
			*fill = 0;
			return BMS_OK;
		}

		/* Setting bit to 0 requires converting to MIXED */
		if (!retried)
		{
			/* Request storage allocation for the new MIXED vector */
			*pos = 1 + bms_chunk_get_position(chunk, vector_index);
			*fill = BMS_BITVEC_MAX; /* Initialize with ones */
			return BMS_NEEDS_TO_GROW;
		}

		/* Convert ONES (0b11) to MIXED (0b10) */
		flag_mask = (uint8) (BMS_FLAG_MASK << (vector_index * 2));
		chunk->data[0] &= ~((bms_bitvec_t) flag_mask);
		chunk->data[0] |= ((bms_bitvec_t) BMS_PAYLOAD_MIXED << (vector_index * 2));
		/* Fall through to handle MIXED case */
	}

	/*
	 * Handle MIXED payload type (or converted from ZEROS/ONES above)
	 */

	/* Locate the stored vector data */
	data_position = 1 + bms_chunk_get_position(chunk, vector_index);
	vector_data = chunk->data[data_position];

	/* Modify the specific bit at (x % BMS_BITVEC_BITS) */
	bit_offset = bit_index & (BMS_BITVEC_BITS - 1);
	if (value)
		vector_data |= ((bms_bitvec_t) 1 << bit_offset);
	else
		vector_data &= ~((bms_bitvec_t) 1 << bit_offset);

	/*
	 * Check if the vector can be converted back to ZEROS or ONES. This allows
	 * us to reclaim storage space.
	 */
	if (vector_data == 0)
	{
		/* Convert MIXED back to ZEROS */
		flag_mask = (uint8) (BMS_FLAG_MASK << (vector_index * 2));
		chunk->data[0] &= ~((bms_bitvec_t) flag_mask);
		/* ZEROS is 0b00, so clearing the bits is sufficient */

		*pos = data_position;
		*fill = 0;
		return BMS_NEEDS_TO_SHRINK;
	}
	else if (vector_data == BMS_BITVEC_MAX)
	{
		/* Convert MIXED back to ONES */
		flag_mask = (uint8) (BMS_FLAG_MASK << (vector_index * 2));
		chunk->data[0] &= ~((bms_bitvec_t) flag_mask);
		chunk->data[0] |= ((bms_bitvec_t) BMS_PAYLOAD_ONES << (vector_index * 2));

		*pos = data_position;
		*fill = 0;
		return BMS_NEEDS_TO_SHRINK;
	}

	/* Store the modified vector data */
	chunk->data[data_position] = vector_data;
	*pos = 0;
	*fill = 0;

	return BMS_OK;
}

/*
 * bms_chunk_select
 *      Find the position of the n-th bit with the specified value
 *
 * Searches through the chunk to find the n-th occurrence of a bit with
 * the given value (0 or 1). The search considers all bit positions in
 * sequence, skipping over NONE vectors entirely.
 *
 * The function handles different payload types efficiently:
 *   - BMS_PAYLOAD_ZEROS: All bits are 0, count accordingly
 *   - BMS_PAYLOAD_ONES: All bits are 1, count accordingly
 *   - BMS_PAYLOAD_NONE: Skip entirely (no bits to count)
 *   - BMS_PAYLOAD_MIXED: Examine each bit individually
 *
 * Returns:
 *  Bit position of the n-th occurrence if found, or total bits examined
 *  if not found. Check *remaining to determine if target was found.
 *  *remaining == SIZE_MAX indicates target was found at returned position.
 */
static size_t
bms_chunk_select(bms_chunk_t * chunk, size_t n, size_t *remaining, bool value)
{
	const uint8 *flags_data;
	size_t		bit_position;
	size_t		target_count;
	int			byte_idx;

	Assert(chunk != NULL);
	Assert(chunk->data != NULL);
	Assert(remaining != NULL);

	flags_data = (const uint8 *) chunk->data;
	bit_position = 0;
	target_count = n;

	/*
	 * Scan through each flag byte in the chunk's flag bitvector. Each byte
	 * contains BMS_FLAGS_PER_INDEX_BYTE flag pairs.
	 */
	for (byte_idx = 0; byte_idx < BMS_CHUNK_FLAG_BYTES; byte_idx++)
	{
		uint8		flag_byte = flags_data[byte_idx];
		int			flag_idx;

		/*
		 * Optimization: Skip bytes that can't contain the target value. - If
		 * seeking 1s and byte is 0x00: all vectors are ZEROS - If seeking 0s
		 * and byte is 0xFF: all vectors are ONES
		 */
		if ((value && flag_byte == 0x00) || (!value && flag_byte == 0xFF))
		{
			bit_position += BMS_FLAGS_PER_INDEX_BYTE * BMS_BITVEC_BITS;
			continue;
		}

		/* Examine each vector in this flag byte */
		for (flag_idx = 0; flag_idx < BMS_FLAGS_PER_INDEX_BYTE; flag_idx++)
		{
			uint8		payload_flags;
			size_t		vector_index;

			payload_flags = (flag_byte >> (flag_idx * 2)) & BMS_FLAG_MASK;

			/* Skip unused vector positions */
			if (payload_flags == BMS_PAYLOAD_NONE)
				continue;

			/*
			 * Handle uniform vectors (all zeros or all ones)
			 */
			if (payload_flags == BMS_PAYLOAD_ZEROS)
			{
				if (value)
				{
					/* Seeking 1s in all-zeros vector: skip entirely */
					bit_position += BMS_BITVEC_BITS;
					continue;
				}
				else
				{
					/* Seeking 0s in all-zeros vector: all bits match */
					if (target_count < BMS_BITVEC_BITS)
					{
						*remaining = SIZE_MAX;	/* Found */
						return bit_position + target_count;
					}
					target_count -= BMS_BITVEC_BITS;
					bit_position += BMS_BITVEC_BITS;
					continue;
				}
			}
			else if (payload_flags == BMS_PAYLOAD_ONES)
			{
				if (value)
				{
					/* Seeking 1s in all-ones vector: all bits match */
					if (target_count < BMS_BITVEC_BITS)
					{
						*remaining = SIZE_MAX;	/* Found */
						return bit_position + target_count;
					}
					target_count -= BMS_BITVEC_BITS;
					bit_position += BMS_BITVEC_BITS;
					continue;
				}
				else
				{
					/* Seeking 0s in all-ones vector: skip entirely */
					bit_position += BMS_BITVEC_BITS;
					continue;
				}
			}
			else if (payload_flags == BMS_PAYLOAD_MIXED)
			{
				/*
				 * Mixed vector: examine each bit individually
				 */
				bms_bitvec_t vector_data;
				size_t		data_position;
				int			bit_idx;

				vector_index = byte_idx * BMS_FLAGS_PER_INDEX_BYTE + flag_idx;
				data_position = bms_chunk_get_position(chunk, vector_index);
				vector_data = chunk->data[1 + data_position];

				for (bit_idx = 0; bit_idx < BMS_BITVEC_BITS; bit_idx++)
				{
					bool		bit_is_set = (vector_data & ((bms_bitvec_t) 1 << bit_idx)) != 0;

					if (bit_is_set == value)
					{
						if (target_count == 0)
						{
							*remaining = SIZE_MAX;	/* Found */
							return bit_position;
						}
						target_count--;
					}
					bit_position++;
				}
			}
			else
			{
				elog(ERROR, "invalid payload flags: %u", payload_flags);
			}
		}
	}

	/* Target not found in this chunk */
	*remaining = target_count;
	return bit_position;
}

/*
 * bms_chunk_rank_range
 *      Count bits matching a value within a specified range in the chunk
 *
 * Counts bits with the specified value in the range [start_offset, end_offset]
 * within the chunk. The function handles partial ranges that may begin or end
 * in the middle of the chunk.
 *
 * The function processes different payload types efficiently:
 *   - BMS_PAYLOAD_ZEROS: Use arithmetic for counting zeros
 *   - BMS_PAYLOAD_ONES: Use arithmetic for counting ones
 *   - BMS_PAYLOAD_NONE: Skip entirely (no bits to count)
 *   - BMS_PAYLOAD_MIXED: Use bit manipulation and popcount
 *
 * Parameters:
 *  chunk: The bitmap chunk to examine
 *  start_offset: [in/out] Starting bit offset (updated on return)
 *  end_offset: Ending bit offset (inclusive)
 *  bits_processed: [out] Number of bit positions examined in this chunk
 *  last_vector: [out] Last vector data examined (for continuation)
 *  value: The bit value to count (true = 1, false = 0)
 *
 * Returns:
 *  Count of bits matching 'value' in the specified range within this chunk
 *
 * Note: start_offset is updated to reflect remaining offset for subsequent chunks
 */
static size_t
bms_chunk_rank_range(bms_chunk_t * chunk, size_t *start_offset, size_t end_offset,
					 size_t *bits_processed, bms_bitvec_t * last_vector, bool value)
{
	const uint8 *flags_data;
	size_t		match_count;
	size_t		chunk_position;
	size_t		remaining_end;
	int			byte_idx;

	Assert(chunk != NULL);
	Assert(chunk->data != NULL);
	Assert(start_offset != NULL);
	Assert(bits_processed != NULL);
	Assert(last_vector != NULL);

	flags_data = (const uint8 *) chunk->data;
	match_count = 0;
	chunk_position = 0;
	remaining_end = end_offset;

	*bits_processed = 0;

	/*
	 * Early exit if start offset exceeds chunk capacity
	 */
	if (*start_offset >= BMS_CHUNK_MAX_CAPACITY)
	{
		*bits_processed = BMS_CHUNK_MAX_CAPACITY;
		*start_offset -= BMS_CHUNK_MAX_CAPACITY;
		return 0;
	}

	/*
	 * Scan through each flag byte in the chunk's flag bitvector
	 */
	for (byte_idx = 0; byte_idx < BMS_CHUNK_FLAG_BYTES; byte_idx++)
	{
		uint8		flag_byte = flags_data[byte_idx];
		int			flag_idx;

		/* Process each vector in this flag byte */
		for (flag_idx = 0; flag_idx < BMS_FLAGS_PER_INDEX_BYTE; flag_idx++)
		{
			uint8		payload_flags;
			size_t		vector_bits_to_process;
			size_t		vector_end_bit;

			payload_flags = (flag_byte >> (flag_idx * 2)) & BMS_FLAG_MASK;

			/* Skip unused vector positions */
			if (payload_flags == BMS_PAYLOAD_NONE)
				continue;

			/*
			 * Calculate how many bits from this vector we need to process
			 */
			if (remaining_end >= BMS_BITVEC_BITS)
			{
				vector_bits_to_process = BMS_BITVEC_BITS;
				vector_end_bit = BMS_BITVEC_BITS - 1;
			}
			else
			{
				vector_bits_to_process = remaining_end + 1;
				vector_end_bit = remaining_end;
			}

			/*
			 * Handle uniform vectors (all zeros or all ones)
			 */
			if (payload_flags == BMS_PAYLOAD_ZEROS)
			{
				*last_vector = 0;

				if (vector_bits_to_process == BMS_BITVEC_BITS)
				{
					/* Processing entire vector */
					chunk_position += BMS_BITVEC_BITS;
					remaining_end -= BMS_BITVEC_BITS;

					if (*start_offset >= BMS_BITVEC_BITS)
					{
						*start_offset -= BMS_BITVEC_BITS;
					}
					else
					{
						if (!value) /* Counting zeros */
							match_count += BMS_BITVEC_BITS - *start_offset;
						*start_offset = 0;
					}
				}
				else
				{
					/* Processing partial vector - end of range */
					chunk_position += vector_bits_to_process;

					if (!value && *start_offset <= vector_end_bit)
					{
						size_t		count_start = (*start_offset > vector_end_bit) ?
							vector_end_bit + 1 : *start_offset;

						match_count += vector_end_bit + 1 - count_start;
					}

					*start_offset = (*start_offset > vector_end_bit) ?
						*start_offset - vector_end_bit - 1 : 0;
					*bits_processed = chunk_position;
					return match_count;
				}
			}
			else if (payload_flags == BMS_PAYLOAD_ONES)
			{
				*last_vector = BMS_BITVEC_MAX;

				if (vector_bits_to_process == BMS_BITVEC_BITS)
				{
					/* Processing entire vector */
					chunk_position += BMS_BITVEC_BITS;
					remaining_end -= BMS_BITVEC_BITS;

					if (*start_offset >= BMS_BITVEC_BITS)
					{
						*start_offset -= BMS_BITVEC_BITS;
					}
					else
					{
						if (value)	/* Counting ones */
							match_count += BMS_BITVEC_BITS - *start_offset;
						*start_offset = 0;
					}
				}
				else
				{
					/* Processing partial vector - end of range */
					chunk_position += vector_bits_to_process;

					if (value && *start_offset <= vector_end_bit)
					{
						size_t		count_start = (*start_offset > vector_end_bit) ?
							vector_end_bit + 1 : *start_offset;

						match_count += vector_end_bit + 1 - count_start;
					}

					*start_offset = (*start_offset > vector_end_bit) ?
						*start_offset - vector_end_bit - 1 : 0;
					*bits_processed = chunk_position;
					return match_count;
				}
			}
			else if (payload_flags == BMS_PAYLOAD_MIXED)
			{
				/*
				 * Mixed vector: use bit manipulation and popcount
				 */
				bms_bitvec_t vector_data;
				size_t		data_position;
				size_t		vector_index;
				bms_bitvec_t range_mask;
				bms_bitvec_t masked_data;
				int			bit_count;

				vector_index = byte_idx * BMS_FLAGS_PER_INDEX_BYTE + flag_idx;
				data_position = bms_chunk_get_position(chunk, vector_index);
				vector_data = chunk->data[1 + data_position];

				if (vector_bits_to_process == BMS_BITVEC_BITS)
				{
					/* Processing entire vector */
					chunk_position += BMS_BITVEC_BITS;
					remaining_end -= BMS_BITVEC_BITS;

					/* Create mask for the range we want to count */
					if (*start_offset == 0)
					{
						range_mask = BMS_BITVEC_MAX;
					}
					else
					{
						size_t		mask_start = (*start_offset >= BMS_BITVEC_BITS) ?
							BMS_BITVEC_BITS : *start_offset;

						range_mask = BMS_BITVEC_MAX << mask_start;
					}

					masked_data = value ? (vector_data & range_mask) :
						(~vector_data & range_mask);
					bit_count = bms_pg_popcount(masked_data);
					match_count += bit_count;

					*start_offset = (*start_offset > BMS_BITVEC_BITS) ?
						*start_offset - BMS_BITVEC_BITS : 0;
				}
				else
				{
					bms_bitvec_t start_mask,
								end_mask;

					/* Processing partial vector - end of range */
					chunk_position += vector_bits_to_process;

					/* Create mask for [start_offset, vector_end_bit] range */
					end_mask = (vector_end_bit == BMS_BITVEC_BITS - 1) ?
						BMS_BITVEC_MAX :
						((bms_bitvec_t) 1 << (vector_end_bit + 1)) - 1;
					start_mask = (*start_offset == 0) ?
						BMS_BITVEC_MAX :
						(BMS_BITVEC_MAX << *start_offset);

					range_mask = end_mask & start_mask;
					masked_data = value ? (vector_data & range_mask) :
						(~vector_data & range_mask);
					bit_count = bms_pg_popcount(masked_data);
					match_count += bit_count;

					*last_vector = masked_data >> *start_offset;
					*start_offset = (*start_offset > vector_end_bit) ?
						*start_offset - vector_end_bit - 1 : 0;
					*bits_processed = chunk_position;
					return match_count;
				}
			}
			else
			{
				elog(ERROR, "invalid payload flags: %u", payload_flags);
			}
		}
	}

	*bits_processed = chunk_position;
	return match_count;
}

/*
 * bms_chunk_scan_set_bits
 *      Scan chunk and invoke callback for each set bit
 *
 * Iterates through all vectors in the chunk, collecting the indices of
 * set bits and invoking the scanner callback function. The function
 * handles different payload types efficiently:
 *   - BMS_PAYLOAD_ZEROS/NONE: Skip entirely (no set bits)
 *   - BMS_PAYLOAD_ONES: Generate all indices in the vector
 *   - BMS_PAYLOAD_MIXED: Examine each bit individually
 *
 * The skip parameter allows starting the scan from a specific bit position,
 * which is useful for continuing scans across multiple chunks.
 *
 * Parameters:
 *  chunk: The bitmap chunk to scan
 *  base_index: Starting index for bit numbering
 *  scanner: Callback function to invoke for each batch of set bits
 *  skip_count: Number of set bits to skip before starting collection
 *  aux: Auxiliary data passed to the scanner callback
 *
 * Returns:
 *  Total number of set bits processed (including skipped ones)
 *
 * Note: The scanner callback receives an array of absolute bit indices,
 * the count of indices in the array, and the auxiliary data pointer.
 */
static size_t
bms_chunk_scan_set_bits(bms_chunk_t * chunk, bms_idx_t base_index,
						void (*scanner) (bms_idx_t[], size_t, void *aux),
						size_t skip_count, void *aux)
{
	const uint8 *flags_data;
	bms_idx_t	index_buffer[BMS_BITVEC_BITS];
	size_t		total_bits_processed;
	size_t		remaining_skip;
	int			byte_idx;

	Assert(chunk != NULL);
	Assert(chunk->data != NULL);
	Assert(scanner != NULL);
	Assert(base_index >= 0);

	flags_data = (const uint8 *) chunk->data;
	total_bits_processed = 0;
	remaining_skip = skip_count;

	/*
	 * Scan through each flag byte in the chunk's flag bitvector
	 */
	for (byte_idx = 0; byte_idx < BMS_CHUNK_FLAG_BYTES; byte_idx++)
	{
		uint8		flag_byte = flags_data[byte_idx];
		int			flag_idx;

		/*
		 * Optimization: Skip bytes that are all zeros (no set bits possible)
		 */
		if (flag_byte == 0x00)
		{
			/* All vectors in this byte are ZEROS - skip efficiently */
			size_t		skip_amount = Min(remaining_skip,
										  BMS_FLAGS_PER_INDEX_BYTE * BMS_BITVEC_BITS);

			remaining_skip -= skip_amount;
			continue;
		}

		/* Process each vector in this flag byte */
		for (flag_idx = 0; flag_idx < BMS_FLAGS_PER_INDEX_BYTE; flag_idx++)
		{
			uint8		payload_flags;
			size_t		vector_index;

			payload_flags = (flag_byte >> (flag_idx * 2)) & BMS_FLAG_MASK;

			/* Skip unused vectors and zero vectors */
			if (payload_flags == BMS_PAYLOAD_NONE ||
				payload_flags == BMS_PAYLOAD_ZEROS)
			{
				size_t		skip_amount = Min(remaining_skip, BMS_BITVEC_BITS);

				remaining_skip -= skip_amount;
				continue;
			}

			/*
			 * Handle all-ones vectors
			 */
			if (payload_flags == BMS_PAYLOAD_ONES)
			{
				if (remaining_skip >= BMS_BITVEC_BITS)
				{
					/* Skip this entire vector */
					remaining_skip -= BMS_BITVEC_BITS;
					total_bits_processed += BMS_BITVEC_BITS;
				}
				else
				{
					/* Collect indices, accounting for remaining skip */
					size_t		buffer_count = 0;
					int			bit_idx;

					for (bit_idx = 0; bit_idx < BMS_BITVEC_BITS; bit_idx++)
					{
						if (remaining_skip > 0)
						{
							remaining_skip--;
						}
						else
						{
							index_buffer[buffer_count++] = base_index +
								total_bits_processed + bit_idx;
						}
					}

					if (buffer_count > 0)
						scanner(index_buffer, buffer_count, aux);

					total_bits_processed += BMS_BITVEC_BITS;
				}
			}

			/*
			 * Handle mixed vectors
			 */
			else if (payload_flags == BMS_PAYLOAD_MIXED)
			{
				bms_bitvec_t vector_data;
				size_t		data_position;
				size_t		buffer_count;
				int			bit_idx;

				vector_index = byte_idx * BMS_FLAGS_PER_INDEX_BYTE + flag_idx;
				data_position = bms_chunk_get_position(chunk, vector_index);
				vector_data = chunk->data[1 + data_position];

				buffer_count = 0;

				/* Examine each bit in the mixed vector */
				for (bit_idx = 0; bit_idx < BMS_BITVEC_BITS; bit_idx++)
				{
					if (vector_data & ((bms_bitvec_t) 1 << bit_idx))
					{
						/* This bit is set */
						if (remaining_skip > 0)
						{
							remaining_skip--;
						}
						else
						{
							index_buffer[buffer_count++] = base_index +
								total_bits_processed + bit_idx;
						}
					}
				}

				/* Invoke callback if we collected any indices */
				if (buffer_count > 0)
					scanner(index_buffer, buffer_count, aux);

				total_bits_processed += BMS_BITVEC_BITS;
			}
			else
			{
				elog(ERROR, "invalid payload flags: %u", payload_flags);
			}
		}
	}

	return total_bits_processed;
}

/*
 * bms_get_chunk_count
 *      Return the number of chunks currently stored in the bitmap
 *
 * Retrieves the chunk count from the bitmap's header. The chunk count
 * is stored as the first 32-bit value in the bitmap's data array.
 *
 * Note: This assumes Bitmapset->data is properly aligned for uint32 access.
 * Alignment is left to the caller when data is not allocated using palloc().
 *
 * Returns:
 *  Number of chunks currently in the bitmap
 */
static size_t
bms_get_chunk_count(const Bitmapset *map)
{
	Assert(map != NULL);
	Assert(map->data != NULL);
	Assert(map->used >= sizeof(uint32));
	BMS_ASSERT_ALIGNED(map->data);

	return (size_t) *(const uint32 *) map->data;
}

/*
 * bms_get_chunk_data
 *      Return pointer to the start of a chunk's data within the bitmap
 *
 * Calculates the memory address where a specific chunk's data begins
 * within the bitmap's data array. The bitmap layout is:
 *   - Header: BMS_SIZEOF_OVERHEAD bytes (chunk count, etc.)
 *   - Chunk data: Variable-length compressed chunk structures
 *
 * Each chunk contains its own flag bitvector followed by payload data
 * for MIXED vectors. The offset parameter specifies the byte position
 * relative to the start of the chunk data area.
 *
 * Returns:
 *  Pointer to the chunk data at the specified offset
 */
static inline uint8 *
bms_get_chunk_data(const Bitmapset *map, size_t offset)
{
	Assert(map != NULL);
	Assert(map->data != NULL);
	Assert(BMS_SIZEOF_OVERHEAD + offset <= map->used);

	return &map->data[BMS_SIZEOF_OVERHEAD + offset];
}

/*
 * bms_get_used_size
 *      Calculate the total size in bytes of the bitmap's used data
 *
 * Computes the actual storage size by walking through all chunks and
 * summing their individual sizes. The total includes:
 *   - Header overhead (chunk count, etc.)
 *   - Chunk offset table (one offset per chunk)
 *   - Variable-length chunk data (flags + MIXED payload vectors)
 *
 * This represents the minimum storage needed to hold the current bitmap
 * data, which may be less than the allocated capacity.
 *
 * Returns:
 *  Total size in bytes of the bitmap's used data
 */
size_t
bms_get_used_size(const Bitmapset *map)
{
	const uint8 *chunk_data_start;
	const uint8 *current_pos;
	size_t		chunk_count;
	size_t		total_chunk_data_size;
	size_t		chunk_idx;

	Assert(map != NULL);
	Assert(map->data != NULL);

	chunk_count = bms_get_chunk_count(map);

	/* Handle empty bitmap */
	if (chunk_count == 0)
		return BMS_SIZEOF_OVERHEAD;

	/*
	 * Walk through all chunks to calculate their total size. Each chunk is
	 * preceded by a chunk_off_t offset value.
	 */
	chunk_data_start = bms_get_chunk_data((Bitmapset *) map, 0);
	current_pos = chunk_data_start;
	total_chunk_data_size = 0;

	for (chunk_idx = 0; chunk_idx < chunk_count; chunk_idx++)
	{
		bms_chunk_t chunk;
		size_t		chunk_size;

		/* Skip the chunk offset */
		current_pos += sizeof(chunk_off_t);

		/* Initialize chunk structure and calculate its size */
		bms_chunk_init(&chunk, (uint8 *) current_pos);
		chunk_size = bms_chunk_get_size(&chunk);

		/* Advance to next chunk */
		current_pos += chunk_size;
		total_chunk_data_size += sizeof(chunk_off_t) + chunk_size;
	}

	/* Verify we haven't exceeded the allocated space */
	Assert(BMS_SIZEOF_OVERHEAD + total_chunk_data_size <= map->used);

	return BMS_SIZEOF_OVERHEAD + total_chunk_data_size;
}

/*
 * bms_get_chunk_end_ptr
 *      Return pointer to the first unused byte after all chunk data
 *
 * Calculates the memory address immediately following the last chunk's
 * data within the bitmap. This represents the boundary between used
 * and unused space in the bitmap's data array.
 *
 * The function walks through all chunks, skipping over:
 *   - Chunk offset values (chunk_off_t for each chunk)
 *   - Variable-length chunk data (flags + MIXED payload vectors)
 *
 * This pointer can be used for:
 *   - Determining where to append new chunk data
 *   - Calculating remaining free space
 *   - Validating bitmap data integrity
 *
 * Returns:
 *  Pointer to the first unused byte after all chunk data
 */
static uint8 *
bms_get_chunk_end_ptr(Bitmapset *map)
{
	uint8	   *chunk_data_start;
	uint8	   *current_pos;
	size_t		chunk_count;
	size_t		chunk_idx;

	Assert(map != NULL);
	Assert(map->data != NULL);

	chunk_count = bms_get_chunk_count(map);

	/* Handle empty bitmap - return start of chunk data area */
	if (chunk_count == 0)
		return bms_get_chunk_data(map, 0);

	/*
	 * Walk through all chunks to find the end of the last one. Each chunk is
	 * preceded by a chunk_off_t offset value.
	 */
	chunk_data_start = bms_get_chunk_data(map, 0);
	current_pos = chunk_data_start;

	for (chunk_idx = 0; chunk_idx < chunk_count; chunk_idx++)
	{
		bms_chunk_t chunk;
		size_t		chunk_size;

		/* Skip the chunk offset */
		current_pos += sizeof(chunk_off_t);

		/* Initialize chunk structure and get its size */
		bms_chunk_init(&chunk, current_pos);
		chunk_size = bms_chunk_get_size(&chunk);

		/* Advance to the next chunk (or end if this is the last) */
		current_pos += chunk_size;
	}

	/* Verify we haven't exceeded the allocated space */
	Assert(current_pos <= &map->data[map->used]);

	return current_pos;
}

/*
 * bms_get_vector_aligned_offset
 *      Calculate the starting bit index for the vector containing the given bit
 *
 * Returns the bit index of the first bit in the vector that contains
 * the specified bit index. Vectors are aligned to BMS_BITVEC_BITS
 * boundaries, so this performs integer division and multiplication
 * to find the aligned boundary.
 *
 * For example, if BMS_BITVEC_BITS is 64:
 *   - Bit 0-63 -> Vector starts at bit 0
 *   - Bit 64-127 -> Vector starts at bit 64
 *   - Bit 150 -> Vector starts at bit 128
 *
 * Parameters:
 *  bit_index: The bit index to find the vector start for
 *
 * Returns:
 *  Starting bit index of the vector containing the given bit
 */
static size_t
bms_get_vector_aligned_offset(size_t bit_index)
{
	return (bit_index / BMS_BITVEC_BITS) * BMS_BITVEC_BITS;
}

/*
 * bms_get_chunk_aligned_offset
 *      Calculate the starting bit index for the chunk containing the given bit
 *
 * Returns the bit index of the first bit in the chunk that contains
 * the specified bit index. Chunks are aligned to BMS_CHUNK_MAX_CAPACITY
 * boundaries, so this performs integer division and multiplication
 * to find the aligned boundary.
 *
 * For example, if BMS_CHUNK_MAX_CAPACITY is 2048:
 *   - Bit 0-2047 -> Chunk starts at bit 0
 *   - Bit 2048-4095 -> Chunk starts at bit 2048
 *   - Bit 3000 -> Chunk starts at bit 2048
 *
 * Parameters:
 *  bit_index: The bit index to find the chunk start for
 *
 * Returns:
 *  Starting bit index of the chunk containing the given bit
 */
static size_t
bms_get_chunk_aligned_offset(size_t bit_index)
{
	return (bit_index / BMS_CHUNK_MAX_CAPACITY) * BMS_CHUNK_MAX_CAPACITY;
}

/*
 * bms_find_chunk_offset
 *      Find the byte offset of the chunk that should contain the given bit index
 *
 * Searches through the chunk offset table to locate the chunk that contains
 * (or should contain) the specified bit index. Each chunk has an associated
 * starting bit index stored as a chunk_off_t value.
 *
 * The function performs a linear search through chunks, comparing their
 * starting bit indices with the target index. It stops when it finds a
 * chunk whose range includes the target bit.
 *
 * Returns:
 *  Byte offset from start of chunk data area to the target chunk,
 *  or 0 if no chunks exist in the bitmap because the first chunk is never
 *  at the 0th offset.
 */
static size_t
bms_find_chunk_offset(const Bitmapset *map, size_t target_bit_index)
{
	const uint8 *chunk_data_start;
	const uint8 *current_pos;
	size_t		chunk_count;
	size_t		chunk_idx;

	Assert(map != NULL);
	Assert(map->data != NULL);

	chunk_count = bms_get_chunk_count(map);

	/* Return -1 if no chunks exist */
	if (chunk_count == 0)
		return 0;

	chunk_data_start = bms_get_chunk_data(map, 0);
	current_pos = chunk_data_start;

	/*
	 * Search through chunks to find the one containing target_bit_index. We
	 * examine all chunks except the last one in the loop, then handle the
	 * last chunk separately (it must contain any remaining bits).
	 */
	for (chunk_idx = 0; chunk_idx < chunk_count - 1; chunk_idx++)
	{
		chunk_off_t chunk_start_bit;
		bms_chunk_t chunk;
		size_t		chunk_capacity;

		/* Read the chunk's starting bit index */
		Assert(((uintptr_t) current_pos & (sizeof(chunk_off_t) - 1)) == 0);
		chunk_start_bit = *(const chunk_off_t *) current_pos;

		/* Verify chunk alignment */
		Assert(chunk_start_bit == bms_get_chunk_aligned_offset(chunk_start_bit));

		/* Initialize chunk to get its capacity */
		bms_chunk_init(&chunk, (uint8 *) (current_pos + sizeof(chunk_off_t)));
		chunk_capacity = bms_chunk_get_capacity(&chunk);

		/*
		 * Check if target bit falls within this chunk's range:
		 * [chunk_start_bit, chunk_start_bit + chunk_capacity)
		 */
		if (target_bit_index >= chunk_start_bit &&
			target_bit_index < chunk_start_bit + chunk_capacity)
			break;

		/* Move to next chunk */
		current_pos += sizeof(chunk_off_t) + bms_chunk_get_size(&chunk);
	}

	/* Return offset from start of chunk data area */
	return current_pos - chunk_data_start;
}

/*
 * bms_set_chunk_count
 *      Update the number of chunks stored in the bitmap header
 *
 * Sets the chunk count value in the bitmap's header area. The chunk count
 * is stored as the first chunk_off_t value in the bitmap's data array and
 * tracks how many compressed chunks are currently allocated.
 *
 * This function assumes the bitmap's data array has sufficient space to
 * hold the header information and that proper alignment is maintained.
 */
static void
bms_set_chunk_count(Bitmapset *map, size_t new_count)
{
	Assert(map != NULL);
	Assert(map->data != NULL);
	Assert(new_count <= UINT32_MAX);	/* Ensure fits in chunk_off_t */
	BMS_ASSERT_ALIGNED(map->data);

	*(uint32 *) map->data = new_count;
}

/*
 * bms_append_data
 *      Append raw data to the end of the bitmap's used data area
 *
 * Copies the specified buffer to the end of the currently used portion
 * of the bitmap's data array and updates the used size accordingly.
 * This is typically used to append new chunk data or extend existing
 * structures within the bitmap.
 *
 * The caller must ensure sufficient space is available before calling
 * this function.
 */
static void
bms_append_data(Bitmapset *map, const uint8 *buffer, size_t size)
{
	Assert(map != NULL);
	Assert(map->data != NULL);
	Assert(buffer != NULL);
	Assert(size > 0);
	Assert(map->used + size <= map->size);

	memcpy(&map->data[map->used], buffer, size);
	map->used += size;
}

/*
 * bms_insert_data
 *      Insert raw data at a specific offset within the bitmap's data area
 *
 * Inserts the specified buffer at the given offset within the bitmap's
 * chunk data area, shifting existing data to make room. This is typically
 * used to insert new chunk data or expand existing structures.
 *
 * The insertion point is relative to the start of the chunk data area
 * (after the bitmap header). Existing data at and after the insertion
 * point is moved to accommodate the new data.
 *
 * The caller must ensure sufficient space is available before calling
 * this function.
 *
 * Parameters:
 *  map: The bitmap to insert data into
 *  offset: Byte offset from start of chunk data area for insertion
 *  buffer: Source data to copy
 *  size: Number of bytes to copy from buffer
 */
static void
bms_insert_data(Bitmapset *map, size_t offset, const uint8 *buffer, size_t size)
{
	uint8	   *insertion_point;
	size_t		chunk_data_used;

	Assert(map != NULL);
	Assert(map->data != NULL);
	Assert(buffer != NULL);
	Assert(size > 0);
	Assert(map->used + size <= map->size);
	Assert(offset <= map->used - BMS_SIZEOF_OVERHEAD);

	/* Calculate how much chunk data exists after the insertion point */
	chunk_data_used = map->used - BMS_SIZEOF_OVERHEAD;

	/* Get pointer to insertion point within chunk data area */
	insertion_point = bms_get_chunk_data(map, offset);

	/*
	 * Move existing data to make room for the new data. We need to move
	 * (chunk_data_used - offset) bytes.
	 */
	if (offset < chunk_data_used)
	{
		memmove(insertion_point + size, insertion_point,
				chunk_data_used - offset);
	}

	/* Copy the new data into the created space */
	memcpy(insertion_point, buffer, size);

	/* Update the used size */
	map->used += size;
}

/*
 * bms_remove_data
 *      Remove a portion of data from within the bitmap's chunk data area
 *
 * Removes the specified number of bytes starting at the given offset
 * within the bitmap's chunk data area, shifting remaining data to fill
 * the gap. This is typically used to remove chunk data or shrink
 * existing structures.
 *
 * The removal point is relative to the start of the chunk data area
 * (after the bitmap header). Data after the removed section is moved
 * forward to eliminate the gap.
 */
static void
bms_remove_data(Bitmapset *map, size_t offset, size_t size)
{
	uint8	   *removal_point;
	size_t		chunk_data_used;
	size_t		bytes_to_move;

	Assert(map != NULL);
	Assert(map->data != NULL);
	Assert(size > 0);
	Assert(map->used >= BMS_SIZEOF_OVERHEAD + size);
	Assert(offset + size <= map->used - BMS_SIZEOF_OVERHEAD);

	/* Calculate chunk data area size and validate removal bounds */
	chunk_data_used = map->used - BMS_SIZEOF_OVERHEAD;

	/* Get pointer to removal point within chunk data area */
	removal_point = bms_get_chunk_data(map, offset);

	/*
	 * Calculate how many bytes need to be moved to fill the gap. This is all
	 * the data after the removed section.
	 */
	bytes_to_move = chunk_data_used - offset - size;

	/*
	 * Move remaining data forward to fill the gap, but only if there's data
	 * after the removed section to move.
	 */
	if (bytes_to_move > 0)
	{
		memmove(removal_point, removal_point + size, bytes_to_move);
	}

	/* Update the used size */
	map->used -= size;
}

/*
 * bms_merge_chunk_bits
 *      Merge set bits from source chunk into destination chunk
 *
 * Copies all set bits from the source chunk to the corresponding positions
 * in the destination chunk. The chunks may have different starting bit
 * indices, so a delta offset is calculated to map source positions to
 * destination positions.
 *
 * This function handles the complex storage management required when
 * setting bits causes payload type changes (e.g., ZEROS -> MIXED).
 * It may need to grow or shrink the bitmap's storage accordingly.
 *
 * TODO: Optimize to merge at the vector level rather than bit-by-bit
 *
 * Parameters:
 *  map: The bitmap containing the destination chunk
 *  src_start_bit: Starting bit index of the source chunk
 *  dst_start_bit: Starting bit index of the destination chunk
 *  merge_capacity: Number of bits to potentially merge
 *  dst_chunk: Destination chunk structure
 *  src_chunk: Source chunk structure (read-only)
 */
static void
bms_merge_chunk_bits(Bitmapset *map, size_t src_start_bit, size_t dst_start_bit,
					 size_t merge_capacity, bms_chunk_t * dst_chunk,
					 const bms_chunk_t * src_chunk)
{
	ssize_t		bit_offset_delta;
	size_t		bit_idx;

	Assert(map != NULL);
	Assert(dst_chunk != NULL);
	Assert(src_chunk != NULL);
	Assert(merge_capacity > 0);
	Assert(merge_capacity <= BMS_CHUNK_MAX_CAPACITY);

	/* Calculate offset between source and destination bit positions */
	bit_offset_delta = (ssize_t) src_start_bit - (ssize_t) dst_start_bit;

	/*
	 * Iterate through each bit position in the merge range. Only copy bits
	 * that are set in source but not in destination.
	 */
	for (bit_idx = 0; bit_idx < merge_capacity; bit_idx++)
	{
		size_t		src_bit_pos = bit_idx;
		size_t		dst_bit_pos = bit_idx + bit_offset_delta;
		ssize_t		chunk_offset;
		int			set_result;
		size_t		storage_position;
		bms_bitvec_t fill_value;

		/* Skip if source bit is not set */
		if (!bms_chunk_is_member(src_chunk, src_bit_pos))
			continue;

		/* Skip if destination bit is already set */
		if (bms_chunk_is_member(dst_chunk, dst_bit_pos))
			continue;

		/*
		 * Find the chunk's offset in the bitmap for potential storage changes
		 */
		chunk_offset = bms_find_chunk_offset(map, dst_start_bit + dst_bit_pos);
		if (chunk_offset == 0)
		{
			elog(ERROR, "failed to locate destination chunk for bit %zu",
				 dst_start_bit + dst_bit_pos);
		}

		/*
		 * Attempt to set the bit in the destination chunk. This may require
		 * storage changes if payload types change.
		 */
		set_result = bms_chunk_set_bit(dst_chunk, dst_bit_pos, true,
									   &storage_position, &fill_value, false);

		switch (set_result)
		{
			case BMS_NEEDS_TO_GROW:
				{
					size_t		insertion_offset = chunk_offset + sizeof(chunk_off_t) +
						storage_position * BMS_CHUNK_FLAG_BYTES;

					/* Insert new vector data */
					bms_insert_data(map, insertion_offset,
									(const uint8 *) &fill_value, BMS_CHUNK_FLAG_BYTES);

					/* Complete the set operation */
					set_result = bms_chunk_set_bit(dst_chunk, dst_bit_pos, true,
												   &storage_position, &fill_value, true);
					Assert(set_result == BMS_OK);
					break;
				}

			case BMS_NEEDS_TO_SHRINK:
				{
					if (bms_chunk_is_empty(dst_chunk))
					{
						/*
						 * Entire chunk became empty - remove it completely.
						 * This should be rare in a merge operation.
						 */
						size_t		removal_size = sizeof(chunk_off_t) +
							bms_chunk_get_size(dst_chunk);

						bms_remove_data(map, chunk_offset, removal_size);
						bms_set_chunk_count(map, bms_get_chunk_count(map) - 1);
					}
					else
					{
						/* Remove unused vector data */
						size_t		removal_offset = chunk_offset + sizeof(chunk_off_t) +
							storage_position * BMS_CHUNK_FLAG_BYTES;

						bms_remove_data(map, removal_offset, BMS_CHUNK_FLAG_BYTES);
					}
					break;
				}

			case BMS_OK:
				/* Bit was set successfully without storage changes */
				break;

			default:
				elog(ERROR, "unexpected result from bms_chunk_set_bit: %d", set_result);
				break;
		}
	}
}

/*
 * bms_empty
 *      Clear all bits in the bitmap, making it empty
 *
 * Resets the bitmap to an empty state by clearing all chunk data and
 * resetting the chunk count to zero. The bitmap's allocated capacity
 * remains unchanged, allowing for efficient reuse.
 *
 * This operation is more efficient than freeing and reallocating the
 * bitmap when the same bitmap will be reused with potentially similar
 * capacity requirements.
 */
void
bms_empty(Bitmapset *map)
{
	if (map == NULL)
		return;

	Assert(map->data != NULL);
	Assert(map->size >= BMS_SIZEOF_OVERHEAD);

	/* Reset chunk count to zero */
	bms_set_chunk_count(map, 0);

	/* Clear any remaining data for consistency */
	if (map->used > BMS_SIZEOF_OVERHEAD)
	{
		memset(&map->data[BMS_SIZEOF_OVERHEAD], 0,
			   map->used - BMS_SIZEOF_OVERHEAD);
	}

	/* Reset used size to just the header */
	map->used = BMS_SIZEOF_OVERHEAD;
}

/*
 * bms_copy
 *      Create a deep copy of the given bitmap
 *
 * Allocates a new bitmap with the same capacity as the source and
 * copies all data, including the header and chunk information.
 * The resulting bitmap is completely independent of the original.
 *
 * Parameters:
 *  source: The bitmap to copy (NULL returns NULL)
 *
 * Returns:
 *  New bitmap that is an exact copy of the source, or NULL if source is NULL
 */
Bitmapset *
bms_copy(const Bitmapset *source)
{
	Bitmapset  *copy;
	size_t		source_capacity;

	/* Handle NULL input gracefully */
	if (source == NULL)
		return NULL;

	Assert(source->data != NULL);
	Assert(source->used >= BMS_SIZEOF_OVERHEAD);
	Assert(source->used <= source->size);

	/* Get source capacity and create new bitmap with same capacity */
	source_capacity = bms_get_capacity(source);
	copy = bms_create(source_capacity);

	/* Copy all data from source to destination */
	memcpy(copy->data, source->data, source->used);
	copy->used = source->used;

	return copy;
}

/*
 * bms_init
 *      Initialize a bitmap structure with the provided data buffer
 *
 * Sets up the bitmap structure fields and initializes it to an empty state.
 * The data buffer must be properly aligned and have sufficient capacity
 * to hold at least the bitmap header.
 *
 * Parameters:
 *  map: The bitmap structure to initialize
 *  data: Aligned data buffer for bitmap storage
 *  size: Size of the data buffer in bytes
 */
static void
bms_init(Bitmapset *map, uint8 *data, size_t size)
{
	Assert(map != NULL);
	Assert(data != NULL);
	Assert(size >= BMS_SIZEOF_OVERHEAD);
	BMS_ASSERT_ALIGNED(data);

	map->type = T_Bitmapset;
	map->data = data;
	map->size = size;
	map->used = BMS_SIZEOF_OVERHEAD;

	/* Initialize to an empty state */
	bms_empty(map);
}

/*
 * bms_create
 *      Allocate and initialize a new bitmap with the specified capacity
 *
 * Creates a new bitmap with the given data capacity. The allocation
 * includes both the Bitmapset structure and the data buffer, with
 * proper alignment for efficient access.
 *
 * If size is 0, a default capacity is used. The actual usable capacity
 * may be slightly less due to header overhead.
 *
 * Parameters:
 *  capacity: Desired data buffer size in bytes (0 for default)
 *
 * Returns:
 *  Newly allocated and initialized bitmap
 */
/*
 * bms_create - create a new empty bitmap with specified capacity
 */
Bitmapset *
bms_create(size_t initial_size)
{
	Bitmapset  *result;
	size_t		total_size;
	uint8	   *data_ptr;

	if (initial_size < BMS_DEFAULT_SIZE)
		initial_size = BMS_DEFAULT_SIZE;

	/* Allocate structure and data together for efficiency */
	total_size = MAXALIGN(sizeof(Bitmapset)) + MAXALIGN(initial_size);
	result = (Bitmapset *) palloc0(total_size);

	/* Set up data pointer with proper alignment */
	data_ptr = (uint8 *) result + MAXALIGN(sizeof(Bitmapset));

	result->type = T_Bitmapset;
	result->size = initial_size;
	result->used = BMS_SIZEOF_OVERHEAD;
	result->data = data_ptr;

	/* Initialize with zero chunk count */
	bms_set_chunk_count(result, 0);

	BMS_ASSERT_ALIGNED(result);
	BMS_ASSERT_ALIGNED(result->data);

	return result;
}

/*
 * bms_get_capacity
 *      Return the total capacity of the bitmap's data buffer
 *
 * Returns the total size of the bitmap's data buffer in bytes.
 * This includes space for both the header and chunk data.
 * The actual usable space for chunks is capacity - BMS_SIZEOF_OVERHEAD.
 *
 * Parameters:
 *  map: The bitmap to examine
 *
 * Returns:
 *  Total data buffer capacity in bytes
 */
size_t
bms_get_capacity(const Bitmapset *map)
{
	Assert(map != NULL);

	return map->size;
}

/*
 * bms_create_with_buffer
 *      Create a bitmap using an externally provided data buffer
 *
 * Creates a new bitmap structure that uses the provided data buffer
 * for storage instead of allocating its own. The caller retains
 * ownership of the data buffer and must ensure it remains valid
 * for the lifetime of the bitmap.
 *
 * The data buffer must be properly aligned and have sufficient
 * capacity for at least the bitmap header.
 *
 * Parameters:
 *  data: Pre-allocated, aligned data buffer
 *  capacity: Size of the data buffer in bytes
 *
 * Returns:
 *  New bitmap structure using the provided buffer
 */
Bitmapset *
bms_create_with_buffer(uint8 *data, size_t capacity)
{
	Bitmapset  *map;

	Assert(data != NULL);
	Assert(capacity >= BMS_SIZEOF_OVERHEAD);
	BMS_ASSERT_ALIGNED(data);

	/* Allocate only the bitmap structure */
	map = (Bitmapset *) palloc0(sizeof(Bitmapset));

	/* Initialize using the provided buffer */
	bms_init(map, data, capacity);

	return map;
}

/*
 * bms_attach_buffer
 *      Attach a new data buffer to an existing bitmap
 *
 * Replaces the bitmap's current data buffer with a new one. This is
 * typically used when migrating bitmap data to a larger buffer or
 * when switching between different storage backends.
 *
 * The caller must ensure:
 *   - The new buffer is properly aligned
 *   - The new buffer has sufficient capacity
 *   - Any existing data is preserved if needed
 *
 * This function updates the bitmap's capacity but preserves the
 * current used size, assuming the data has been properly migrated.
 *
 * Parameters:
 *  map: The bitmap to modify
 *  data: New data buffer to attach
 *  capacity: Size of the new data buffer in bytes
 *
 * Returns:
 *  The modified bitmap (same as input parameter)
 */
Bitmapset *
bms_attach_buffer(Bitmapset *map, uint8 *data, size_t capacity)
{
	Assert(map != NULL);
	Assert(data != NULL);
	Assert(capacity >= BMS_SIZEOF_OVERHEAD);
	Assert(capacity >= map->used);	/* New buffer must fit existing data */
	BMS_ASSERT_ALIGNED(data);

	/* Update buffer and capacity */
	map->data = data;
	map->size = capacity;

	/*
	 * Note: We preserve map->used assuming the caller has properly migrated
	 * any existing data to the new buffer. If starting fresh, the caller
	 * should call bms_empty() after this function.
	 */

	return map;
}

/*
 * bms_resize
 *      Resize a bitmap's data buffer to accommodate the specified capacity
 *
 * Resizes the bitmap's storage capacity. The behavior depends on how the
 * bitmap was originally allocated:
 *
 * 1. If the bitmap was created with bms_create() (integrated allocation),
 *    and no external buffer is provided, the function will reallocate
 *    the entire structure to accommodate the new size.
 *
 * 2. If an external buffer is provided, or the bitmap uses an external
 *    buffer, the caller is responsible for buffer management and this
 *    function only updates the bitmap's metadata.
 *
 * Returns:
 *  Resized bitmap (may be different pointer if reallocated), or NULL on failure
 */
Bitmapset *
bms_resize(Bitmapset *map, uint8 *new_buffer, size_t new_capacity)
{
	size_t		current_capacity;
	bool		is_integrated_allocation;

	Assert(map != NULL);
	Assert(new_capacity >= BMS_SIZEOF_OVERHEAD);

	if (new_capacity < map->used)
		return map;

	current_capacity = map->size;

	/*
	 * Determine if this bitmap uses integrated allocation (structure and data
	 * allocated together) by checking if the data pointer follows immediately
	 * after the structure with proper alignment.
	 */
	is_integrated_allocation = BMS_IS_INTEGRATED_ALLOCATION(map);

	/*
	 * Handle integrated allocation resize when no external buffer provided
	 */
	if (new_buffer == NULL && is_integrated_allocation && new_capacity > current_capacity)
	{
		Bitmapset  *resized_map;
		size_t		total_size;
		size_t		aligned_offset;
		uint8	   *new_data_ptr;
		size_t		clear_start;
		size_t		clear_size;

		/* Calculate new total allocation size with proper alignment */
		total_size = MAXALIGN(sizeof(Bitmapset)) + new_capacity;

		/* Reallocate the entire structure */
		resized_map = (Bitmapset *) repalloc(map, total_size);
		if (resized_map == NULL)
		{
			elog(ERROR, "failed to resize bitmap to %zu bytes", new_capacity);
		}

		/* Recalculate aligned data pointer after reallocation */
		aligned_offset = MAXALIGN(sizeof(Bitmapset));
		new_data_ptr = (uint8 *) resized_map + aligned_offset;

		/*
		 * Clear the newly allocated portion of the data buffer. Only clear
		 * from the old capacity to the new capacity.
		 */
		clear_start = current_capacity;
		clear_size = new_capacity - current_capacity;
		memset(new_data_ptr + clear_start, 0, clear_size);

		/* Update bitmap metadata */
		resized_map->data = new_data_ptr;
		resized_map->size = new_capacity;

		return resized_map;
	}
	else
	{
		/*
		 * Handle external buffer cases: - Caller provided a new buffer, or -
		 * Bitmap uses external buffer management, or - Shrinking operation
		 * (no reallocation needed)
		 */
		if (new_buffer != NULL && new_buffer != map->data)
		{
			/* Verify new buffer alignment */
			BMS_ASSERT_ALIGNED(new_buffer);

			/* Switch to new external buffer */
			map->data = new_buffer;
		}

		/* Update capacity */
		map->size = new_capacity;

		/* Ensure used size doesn't exceed new capacity */
		if (map->used > new_capacity)
		{
			elog(WARNING, "truncating bitmap used size from %zu to %zu bytes",
				 map->used, new_capacity);
			map->used = new_capacity;
		}

		return map;
	}
}

/*
 * bms_get_utilization
 *      Calculate the storage utilization percentage of the bitmap
 *
 * Returns the percentage of the bitmap's allocated capacity that is
 * currently in use. This is useful for monitoring memory efficiency
 * and determining when resizing might be beneficial.
 *
 * The calculation is: (used_bytes / total_capacity) * 100
 *
 * Parameters:
 *  map: The bitmap to examine
 *
 * Returns:
 *  Utilization percentage (0.0 to 100.0)
 */
static double
bms_get_utilization(const Bitmapset *map)
{
	double		utilization_percent;

	Assert(map != NULL);
	Assert(map->size > 0);
	Assert(map->used >= BMS_SIZEOF_OVERHEAD);

	/* Handle edge case where used exceeds capacity (should not happen) */
	if (map->used >= map->size)
	{
		elog(WARNING, "bitmap used size (%zu) exceeds capacity (%zu)",
			 map->used, map->size);
		return 100.0;
	}

	/* Calculate utilization as percentage */
	utilization_percent = ((double) map->used / (double) map->size) * 100.0;

	return utilization_percent;
}

/*
 * bms_is_member
 *      Test whether a specific bit is set in the bitmap
 *
 * Checks if the bit at the specified index is set in the bitmap.
 * This involves locating the appropriate chunk and testing the bit
 * within that chunk's storage.
 *
 * Parameters:
 *  bit_index: The bit position to test (negative values return false)
 *  map: The bitmap to examine (NULL returns false)
 *
 * Returns:
 *  true if the bit is set, false otherwise
 */
bool
bms_is_member(int bit_index, const Bitmapset *map)
{
	ssize_t		chunk_offset;
	uint8	   *chunk_data_ptr;
	chunk_off_t chunk_start_bit;
	bms_chunk_t chunk;
	size_t		bit_offset_in_chunk;

	/* Handle invalid inputs gracefully */
	if (bit_index < 0 || map == NULL)
		return false;

	Assert(bms_is_valid_set(map));

	/*
	 * Find the chunk that would contain this bit index. If no such chunk
	 * exists, the bit is not set.
	 */
	chunk_offset = bms_find_chunk_offset(map, bit_index);
	if (chunk_offset == 0)
		return false;

	/*
	 * Load the chunk header and initialize the chunk structure. The chunk
	 * data starts after the chunk_off_t header.
	 */
	chunk_data_ptr = bms_get_chunk_data(map, chunk_offset);
	chunk_start_bit = *(chunk_off_t *) chunk_data_ptr;
	bms_chunk_init(&chunk, chunk_data_ptr + sizeof(chunk_off_t));

	/*
	 * Verify the bit index falls within this chunk's range. Each chunk covers
	 * a specific range of bit indices.
	 */
	if (bit_index < chunk_start_bit)
		return false;

	bit_offset_in_chunk = bit_index - chunk_start_bit;
	if (bit_offset_in_chunk >= bms_chunk_get_capacity(&chunk))
		return false;

	/* Test the specific bit within the chunk */
	return bms_chunk_is_member(&chunk, bit_offset_in_chunk);
}

/*
 * bms_set_bit
 *      Set or clear a specific bit in the bitmap
 *
 * Sets or clears the bit at the specified index. This is the core
 * operation for bitmap manipulation and handles all the complex
 * storage management required by the sparse bitmap format.
 *
 * The function may need to:
 * - Create new chunks if the bit falls outside existing ranges
 * - Grow or shrink chunk storage based on payload type changes
 * - Remove empty chunks to maintain sparsity
 *
 * Parameters:
 *  map: The bitmap to modify
 *  bit_index: The bit position to set or clear
 *  new_state: true to set the bit, false to clear it
 *
 * Returns:
 *  The bit index on success, -1 if insufficient space for growth
 */
static int
bms_set_bit(Bitmapset *map, int bit_index, bool new_state)
{
	uint8	   *chunk_data_ptr;
	chunk_off_t chunk_start_bit;
	size_t		chunk_aligned_start;
	bms_chunk_t chunk;
	ssize_t		chunk_offset;
	size_t		storage_position;
	bms_bitvec_t fill_value;
	int			set_result;
	bool		skip_growth = false;
	uint8		new_chunk_buffer[BMS_CHUNK_INITIAL_SIZE] = {0};

	Assert(map != NULL);
	Assert(bit_index >= 0);
	Assert(bms_get_capacity(map) >= BMS_SIZEOF_OVERHEAD);

	/* Check if we have space for potential chunk creation */
	if (map->used + BMS_CHUNK_INITIAL_SIZE > map->size)
		return -1;

	/* Find existing chunk that would contain this bit */
	chunk_offset = bms_find_chunk_offset(map, bit_index);

	/*
	 * Handle case where no chunks exist yet
	 */
	if (chunk_offset == 0)
	{
		/* Clearing a bit that doesn't exist is a no-op */
		if (!new_state)
			return bit_index;

		/* Create the first chunk */
		chunk_aligned_start = bms_get_chunk_aligned_offset(bit_index);
		*(chunk_off_t *) new_chunk_buffer = chunk_aligned_start;

		bms_append_data(map, new_chunk_buffer, sizeof(new_chunk_buffer));
		bms_set_chunk_count(map, 1);

		/* We pre-allocated storage, so skip growth operations */
		skip_growth = true;
		chunk_offset = BMS_SIZEOF_OVERHEAD;
	}

	/* Load the chunk at the found offset */
	chunk_data_ptr = bms_get_chunk_data(map, chunk_offset);
	chunk_start_bit = *(const chunk_off_t *) chunk_data_ptr;

	Assert(chunk_start_bit == bms_get_vector_aligned_offset(chunk_start_bit));

	/*
	 * Handle case where bit index is before the first chunk
	 */
	if (bit_index < chunk_start_bit)
	{
		/* Clearing a bit that doesn't exist is a no-op */
		if (!new_state)
			return bit_index;

		/* Insert new chunk before the existing one */
		chunk_aligned_start = bms_get_chunk_aligned_offset(bit_index);
		*(chunk_off_t *) new_chunk_buffer = chunk_aligned_start;

		bms_insert_data(map, chunk_offset, new_chunk_buffer, sizeof(new_chunk_buffer));

		/* Adjust existing chunk if ranges can be merged */
		if (chunk_start_bit - chunk_aligned_start < BMS_CHUNK_MAX_CAPACITY)
		{
			bms_chunk_init(&chunk, chunk_data_ptr + sizeof(chunk_off_t));
			bms_chunk_reduce_capacity(&chunk, chunk_start_bit - chunk_aligned_start);
		}

		/* Update the chunk start to the new aligned position */
		*(chunk_off_t *) chunk_data_ptr = chunk_start_bit = chunk_aligned_start;
		bms_set_chunk_count(map, bms_get_chunk_count(map) + 1);

		skip_growth = true;
	}
	else
	{
		/*
		 * Check if bit index exceeds current chunk capacity
		 */
		bms_chunk_init(&chunk, chunk_data_ptr + sizeof(chunk_off_t));

		if (bit_index - chunk_start_bit >= bms_chunk_get_capacity(&chunk))
		{
			size_t		current_chunk_size;
			size_t		next_chunk_offset;

			/* Clearing a bit that doesn't exist is a no-op */
			if (!new_state)
				return bit_index;

			/* Calculate position for new chunk after current one */
			current_chunk_size = bms_chunk_get_size(&chunk);
			next_chunk_offset = chunk_offset + sizeof(chunk_off_t) + current_chunk_size;

			/* Determine start position for new chunk */
			chunk_start_bit += bms_chunk_get_capacity(&chunk);
			if (chunk_start_bit + BMS_CHUNK_MAX_CAPACITY <= bit_index)
				chunk_start_bit = bms_get_chunk_aligned_offset(bit_index);

			*(chunk_off_t *) new_chunk_buffer = chunk_start_bit;

			/* Insert new chunk after current one */
			bms_insert_data(map, next_chunk_offset, new_chunk_buffer, sizeof(new_chunk_buffer));

			Assert(chunk_start_bit == bms_get_vector_aligned_offset(chunk_start_bit));

			bms_set_chunk_count(map, bms_get_chunk_count(map) + 1);
			skip_growth = true;

			/* Update pointers to point to the new chunk */
			chunk_data_ptr = bms_get_chunk_data(map, next_chunk_offset);
		}
	}

	/* Initialize chunk structure for the target chunk */
	bms_chunk_init(&chunk, chunk_data_ptr + sizeof(chunk_off_t));

	/* Attempt to set the bit in the chunk */
	set_result = bms_chunk_set_bit(&chunk, bit_index - chunk_start_bit, new_state,
								   &storage_position, &fill_value, false);

	/* Handle storage changes based on the result */
	switch (set_result)
	{
		case BMS_OK:
			/* Bit was set successfully without storage changes */
			break;

		case BMS_NEEDS_TO_GROW:
			if (!skip_growth)
			{
				size_t		insertion_offset = chunk_offset + sizeof(chunk_off_t) +
					storage_position * BMS_CHUNK_FLAG_BYTES;

				bms_insert_data(map, insertion_offset,
								(const uint8 *) &fill_value, BMS_CHUNK_FLAG_BYTES);
			}

			/* Complete the set operation */
			set_result = bms_chunk_set_bit(&chunk, bit_index - chunk_start_bit, new_state,
										   &storage_position, &fill_value, true);
			Assert(set_result == BMS_OK);
			break;

		case BMS_NEEDS_TO_SHRINK:
			if (bms_chunk_is_empty(&chunk))
			{
				/* Remove entire empty chunk */
				size_t		chunk_total_size = sizeof(chunk_off_t) + bms_chunk_get_size(&chunk);

				bms_remove_data(map, chunk_offset, chunk_total_size);
				bms_set_chunk_count(map, bms_get_chunk_count(map) - 1);
			}
			else
			{
				/* Remove unused vector data */
				size_t		removal_offset = chunk_offset + sizeof(chunk_off_t) +
					storage_position * BMS_CHUNK_FLAG_BYTES;

				bms_remove_data(map, removal_offset, BMS_CHUNK_FLAG_BYTES);
			}
			break;

		default:
			elog(ERROR, "unexpected result from bms_chunk_set_bit: %d", set_result);
			break;
	}

	Assert(bms_get_capacity(map) >= BMS_SIZEOF_OVERHEAD);
	return bit_index;
}

/*
 * bms_add_member
 *      Add a specified bit to the bitmap
 *
 * Sets the bit at the given index in the bitmap. If the bitmap is NULL,
 * a new singleton bitmap is created. If the bitmap lacks sufficient
 * capacity, it will be automatically resized.
 *
 * The input bitmap may be modified in place or replaced with a larger
 * allocation if resizing is required.
 *
 * Parameters:
 *  map: The bitmap to modify (NULL creates new bitmap)
 *  bit_index: The bit position to set (must be non-negative)
 *
 * Returns:
 *  The modified bitmap (may be different pointer if resized)
 */
Bitmapset *
bms_add_member(Bitmapset *a, int bit_index)
{
	int			set_result;

	/* Validate bit index */
	if (bit_index < 0)
		elog(ERROR, "negative bitmapset member not allowed");

	/* Create new singleton bitmap if input is NULL */
	if (a == NULL)
		return bms_make_singleton(bit_index);

	Assert(bms_is_valid_set(a));

	/* Attempt to set the bit */
	set_result = bms_set_bit(a, bit_index, true);

	/* If insufficient capacity, resize and retry */
	if (set_result == -1)
	{
		size_t		current_capacity = bms_get_capacity(a);
		size_t		new_capacity = current_capacity * 2;

		/* Ensure new capacity is sufficient for this bit */
		size_t		min_required = BMS_SIZEOF_OVERHEAD + BMS_CHUNK_INITIAL_SIZE;

		if (new_capacity < min_required)
			new_capacity = min_required;

		a = bms_resize(a, NULL, new_capacity);
		if (a == NULL)
			elog(ERROR, "failed to resize bitmap for bit %d", bit_index);

		/* Retry the set operation */
		set_result = bms_set_bit(a, bit_index, true);
		if (set_result == -1)
			elog(ERROR, "failed to set bit %d after resize", bit_index);
	}

	return a;
}

/*
 * bms_del_member
 *      Remove a specified bit from the bitmap
 *
 * Clears the bit at the given index in the bitmap. If the bit is not
 * currently set, this is a no-op. The bitmap structure is preserved
 * even if it becomes empty (use bms_is_empty to check).
 *
 * The input bitmap is modified in place when possible.
 *
 * Parameters:
 *  map: The bitmap to modify (NULL returns NULL)
 *  bit_index: The bit position to clear (must be non-negative)
 *
 * Returns:
 *  The modified bitmap (same as input for this operation)
 */
Bitmapset *
bms_del_member(Bitmapset *a, int bit_index)
{
	int			clear_result;

	/* Validate bit index */
	if (bit_index < 0)
		elog(ERROR, "negative bitmapset member not allowed");

	/* Handle NULL input gracefully */
	if (a == NULL)
		return NULL;

	Assert(bms_is_valid_set(a));

	/* Clear the bit - this operation cannot fail due to capacity */
	clear_result = bms_set_bit(a, bit_index, false);

	/*
	 * Note: bms_set_bit should never fail when clearing bits since clearing
	 * can only shrink or maintain storage requirements.
	 */
	Assert(clear_result != -1);

	/*
	 * TODO: Consider returning NULL if the bitmap becomes completely empty to
	 * maintain consistency with other bitmap operations. Currently we
	 * preserve the empty bitmap structure for potential reuse.
	 */

	return a;
}

/*
 * bms_chunk_get_mixed_payload - Get mixed payload vector data
 */
static bms_bitvec_t
bms_chunk_get_mixed_payload(bms_chunk_t * chunk, size_t byte_idx, size_t flag_idx)
{
	size_t		vector_index = byte_idx * BMS_FLAGS_PER_INDEX_BYTE + flag_idx;
	size_t		data_position = bms_chunk_get_position(chunk, vector_index);

	return chunk->data[1 + data_position];
}

/*
 * bms_first_member
 *      Return the index of the first set bit in the bitmap
 *
 * Finds the lowest-numbered bit that is set in the bitmap. This function
 * iterates through chunks in order and examines their payload to find
 * the first set bit efficiently.
 *
 * Parameters:
 *  map: The bitmap to examine (NULL is treated as empty)
 *
 * Returns:
 *  Index of the first set bit, or -1 if the bitmap is empty
 */
int
bms_first_member(const Bitmapset *map)
{
	uint8	   *chunk_data_ptr;
	chunk_off_t chunk_start_bit;
	uint8	   *payload_ptr;
	bms_chunk_t chunk;
	size_t		chunk_count;
	size_t		chunk_idx;
	size_t		byte_idx;
	size_t		flag_idx;
	size_t		bit_idx;
	size_t		payload_flags;
	size_t		current_bit_position;
	bms_bitvec_t mixed_vector;

	/* Handle NULL or empty bitmap */
	if (map == NULL)
		return -1;

	chunk_count = bms_get_chunk_count(map);
	if (chunk_count == 0)
		return -1;

	/*
	 * Examine each chunk in order to find the first set bit. Since chunks are
	 * stored in ascending bit order, the first chunk with any set bits
	 * contains the overall first bit.
	 */
	for (chunk_idx = 0; chunk_idx < chunk_count; chunk_idx++)
	{
		/* Load chunk header and initialize chunk structure */
		chunk_data_ptr = bms_get_chunk_data(map,
											BMS_SIZEOF_OVERHEAD +
											chunk_idx * BMS_AVG_CHUNK_SIZE);
		chunk_start_bit = *(const chunk_off_t *) chunk_data_ptr;
		payload_ptr = chunk_data_ptr + sizeof(chunk_off_t);

		bms_chunk_init(&chunk, payload_ptr);
		current_bit_position = chunk_start_bit;

		/*
		 * Scan through the chunk's payload flags to find set bits. Each byte
		 * contains multiple flag pairs describing payload types.
		 */
		for (byte_idx = 0; byte_idx < BMS_CHUNK_FLAG_BYTES; byte_idx++)
		{
			for (flag_idx = 0; flag_idx < BMS_FLAGS_PER_INDEX_BYTE; flag_idx++)
			{
				payload_flags = BMS_CHUNK_EXTRACT_FLAGS(payload_ptr[byte_idx], flag_idx);

				switch (payload_flags)
				{
					case BMS_PAYLOAD_NONE:
						/* No payload for this range - continue */
						break;

					case BMS_PAYLOAD_ZEROS:
						/* All zeros in this range - skip ahead */
						current_bit_position += BMS_BITVEC_BITS;
						break;

					case BMS_PAYLOAD_ONES:
						/* All ones in this range - first bit is our answer */
						return (int) current_bit_position;

					case BMS_PAYLOAD_MIXED:
						/* Mixed payload - examine individual bits */
						mixed_vector = bms_chunk_get_mixed_payload(&chunk, byte_idx, flag_idx);

						for (bit_idx = 0; bit_idx < BMS_BITVEC_BITS; bit_idx++)
						{
							if (mixed_vector & ((bms_bitvec_t) 1 << bit_idx))
							{
								return (int) (current_bit_position + bit_idx);
							}
						}
						current_bit_position += BMS_BITVEC_BITS;
						break;

					default:
						elog(ERROR, "invalid payload flags: %zu", payload_flags);
						break;
				}
			}
		}
	}

	/* No set bits found in any chunk */
	return -1;
}

/*
 * bms_last_member
 *      Return the index of the last set bit in the bitmap
 *
 * Finds the highest-numbered bit that is set in the bitmap. This function
 * examines chunks in reverse order and scans their payload to find
 * the last set bit efficiently.
 *
 * Parameters:
 *  map: The bitmap to examine (NULL is treated as empty)
 *
 * Returns:
 *  Index of the last set bit, or -1 if the bitmap is empty
 */
int
bms_last_member(const Bitmapset *map)
{
	uint8	   *chunk_data_ptr;
	chunk_off_t chunk_start_bit;
	uint8	   *payload_ptr;
	bms_chunk_t chunk;
	size_t		chunk_count;
	ssize_t		chunk_idx;
	ssize_t		byte_idx;
	ssize_t		flag_idx;
	ssize_t		bit_idx;
	size_t		payload_flags;
	size_t		current_bit_position;
	bms_bitvec_t mixed_vector;
	int			last_set_bit = -1;
	size_t		chunk_offset;

	/* Handle NULL or empty bitmap */
	if (map == NULL)
		return -1;

	chunk_count = bms_get_chunk_count(map);
	if (chunk_count == 0)
		return -1;

	/*
	 * Examine chunks in reverse order to find the last set bit. Since chunks
	 * are stored in ascending bit order, the last chunk with any set bits
	 * contains the overall last bit.
	 */
	chunk_offset = BMS_SIZEOF_OVERHEAD;

	/* First, locate the start of the last chunk by iterating forward */
	for (size_t i = 0; i < chunk_count - 1; i++)
	{
		chunk_data_ptr = bms_get_chunk_data(map, chunk_offset);
		payload_ptr = chunk_data_ptr + sizeof(chunk_off_t);
		bms_chunk_init(&chunk, payload_ptr);
		chunk_offset += sizeof(chunk_off_t) + bms_chunk_get_size(&chunk);
	}

	/* Now examine the last chunk */
	chunk_data_ptr = bms_get_chunk_data(map, chunk_offset);
	chunk_start_bit = *(const chunk_off_t *) chunk_data_ptr;
	payload_ptr = chunk_data_ptr + sizeof(chunk_off_t);
	bms_chunk_init(&chunk, payload_ptr);

	/*
	 * Scan through the chunk's payload flags in reverse order to find the
	 * last set bit. We need to examine all flags to find the highest-numbered
	 * set bit.
	 */
	for (byte_idx = BMS_CHUNK_FLAG_BYTES - 1; byte_idx >= 0; byte_idx--)
	{
		for (flag_idx = BMS_FLAGS_PER_INDEX_BYTE - 1; flag_idx >= 0; flag_idx--)
		{
			payload_flags = BMS_CHUNK_EXTRACT_FLAGS(payload_ptr[byte_idx], flag_idx);
			current_bit_position = chunk_start_bit +
				(byte_idx * BMS_FLAGS_PER_INDEX_BYTE + flag_idx) * BMS_BITVEC_BITS;

			switch (payload_flags)
			{
				case BMS_PAYLOAD_NONE:
					/* No payload for this range - continue */
					break;

				case BMS_PAYLOAD_ZEROS:
					/* All zeros in this range - no bits set */
					break;

				case BMS_PAYLOAD_ONES:
					/* All ones in this range - last bit is highest in range */
					return (int) (current_bit_position + BMS_BITVEC_BITS - 1);

				case BMS_PAYLOAD_MIXED:
					/* Mixed payload - examine individual bits in reverse */
					mixed_vector = bms_chunk_get_mixed_payload(&chunk, byte_idx, flag_idx);

					for (bit_idx = BMS_BITVEC_BITS - 1; bit_idx >= 0; bit_idx--)
					{
						if (mixed_vector & ((bms_bitvec_t) 1 << bit_idx))
						{
							return (int) (current_bit_position + bit_idx);
						}
					}
					break;

				default:
					elog(ERROR, "invalid payload flags: %zu", payload_flags);
					break;
			}
		}
	}

	/* No set bits found - should not happen if chunk exists */
	elog(WARNING, "no set bits found in non-empty chunk");
	return -1;
}

/*
 * bms_rank_range
 *      Count set or unset bits within a specified range
 *
 * Counts the number of bits with the specified value (set or unset) within
 * the given range [start_bit, end_bit] inclusive. This function handles
 * sparse bitmap storage efficiently by considering gaps between chunks
 * as containing all unset bits.
 *
 * The rank operation is fundamental for many bitmap algorithms and
 * provides efficient counting across potentially large sparse ranges.
 *
 * Parameters:
 *  map: The bitmap to examine
 *  start_bit: Starting bit index of the range (inclusive)
 *  end_bit: Ending bit index of the range (inclusive)
 *  target_value: true to count set bits, false to count unset bits
 *  work_vector: Temporary storage for chunk operations (may be NULL)
 *
 * Returns:
 *  Number of bits with target_value in the specified range
 */
static size_t
bms_rank_range(Bitmapset *map, size_t start_bit, size_t end_bit,
			   bool target_value, bms_bitvec_t * work_vector)
{
	size_t		chunk_count;
	size_t		chunk_idx;
	size_t		range_length;
	size_t		total_count = 0;
	size_t		gap_size;
	size_t		chunk_contribution;
	size_t		current_position = 0;
	size_t		previous_chunk_end = 0;
	uint8	   *chunk_data_ptr;
	chunk_off_t chunk_start_bit;
	bms_chunk_t chunk;
	size_t		adjusted_start;
	size_t		adjusted_end;

	Assert(map != NULL);
	Assert(bms_get_capacity(map) >= BMS_SIZEOF_OVERHEAD);

	/* Handle invalid range */
	if (start_bit > end_bit)
		return 0;

	range_length = end_bit - start_bit + 1;
	chunk_count = bms_get_chunk_count(map);

	/* Handle empty bitmap */
	if (chunk_count == 0)
	{
		/* Empty bitmap: all bits are unset */
		return target_value ? 0 : range_length;
	}

	/*
	 * Iterate through all chunks, handling gaps between them. Gaps contain
	 * only unset bits in sparse representation.
	 */
	for (chunk_idx = 0; chunk_idx < chunk_count; chunk_idx++)
	{
		/* Load chunk header */
		chunk_data_ptr = bms_get_chunk_data(map,
											BMS_SIZEOF_OVERHEAD +
											chunk_idx * BMS_AVG_CHUNK_SIZE);
		chunk_start_bit = *(const chunk_off_t *) chunk_data_ptr;

		/*
		 * Calculate gap size before this chunk. For the first chunk, gap is
		 * from bit 0 to chunk start. For subsequent chunks, gap is from end
		 * of previous chunk to start of current.
		 */
		if (chunk_idx == 0)
		{
			gap_size = chunk_start_bit;
		}
		else
		{
			/* Check if chunks are adjacent */
			if (previous_chunk_end + BMS_CHUNK_MAX_CAPACITY == chunk_start_bit)
			{
				gap_size = 0;
			}
			else
			{
				gap_size = chunk_start_bit - previous_chunk_end;
			}
		}

		/*
		 * If this chunk starts beyond our range, we're done. Handle any
		 * remaining unset bits if counting unset bits.
		 */
		if (chunk_start_bit > end_bit)
		{
			if (target_value)
			{
				/* Counting set bits: no more chunks can contribute */
				break;
			}
			else
			{
				/* Counting unset bits: add remaining range */
				if (chunk_idx == 0)
				{
					/* First chunk beyond range: entire range is unset */
					total_count += range_length;
				}
				else
				{
					/* Add unset bits from current position to end of range */
					if (current_position <= end_bit)
					{
						size_t		remaining = end_bit - current_position + 1;

						total_count += (remaining < gap_size) ? remaining : gap_size;
					}
				}
				break;
			}
		}

		/*
		 * Handle gap before this chunk if it overlaps with our range
		 */
		if (gap_size > 0)
		{
			size_t		gap_start = current_position;
			size_t		gap_end = gap_start + gap_size - 1;

			/* Check if gap overlaps with our target range */
			if (gap_end >= start_bit && gap_start <= end_bit)
			{
				size_t		overlap_start = (gap_start > start_bit) ? gap_start : start_bit;
				size_t		overlap_end = (gap_end < end_bit) ? gap_end : end_bit;
				size_t		overlap_size = overlap_end - overlap_start + 1;

				if (!target_value)
				{
					/* Counting unset bits: gaps contribute */
					total_count += overlap_size;
				}
				/* If counting set bits, gaps contribute nothing */
			}

			current_position += gap_size;
		}

		/* Initialize chunk structure */
		bms_chunk_init(&chunk, chunk_data_ptr + sizeof(chunk_off_t));

		/*
		 * Calculate the portion of this chunk that overlaps with our range
		 */
		adjusted_start = (start_bit > chunk_start_bit) ? start_bit - chunk_start_bit : 0;
		adjusted_end = (end_bit < chunk_start_bit + bms_chunk_get_capacity(&chunk) - 1) ?
			end_bit - chunk_start_bit : bms_chunk_get_capacity(&chunk) - 1;

		/* Count bits within this chunk */
		if (adjusted_start <= adjusted_end)
		{
			size_t		bits_processed = 0;

			chunk_contribution = bms_chunk_rank_range(&chunk, &adjusted_start,
													  adjusted_end, &bits_processed, work_vector, target_value);
			total_count += chunk_contribution;
		}

		/* Update position tracking */
		previous_chunk_end = chunk_start_bit + bms_chunk_get_capacity(&chunk);
		current_position = previous_chunk_end;
	}

	/*
	 * Handle any remaining unset bits after the last chunk
	 */
	if (!target_value && current_position <= end_bit)
	{
		total_count += end_bit - current_position + 1;
	}

	return total_count;
}

/*
 * bms_rank
 *      Count the number of set bits in the specified range
 *
 * Returns the number of set bits (1s) in the range [start_bit, end_bit]
 * inclusive. This is a convenience wrapper around the internal rank
 * function that handles parameter validation and provides a clean
 * public interface.
 *
 * Parameters:
 *  map: The bitmap to examine (NULL returns 0)
 *  start_bit: Starting bit index of the range (inclusive, must be >= 0)
 *  end_bit: Ending bit index of the range (inclusive)
 *
 * Returns:
 *  Number of set bits in the range [start_bit, end_bit], or 0 if invalid range
 */
int
bms_rank(const Bitmapset *map, int start_bit, int end_bit, bool target_value)
{
	size_t		rank_result;

	Assert(bms_is_valid_set(map));
	Assert(start_bit >= 0);

	/* Handle NULL bitmap or invalid range */
	if (map == NULL || end_bit < 0 || end_bit < start_bit)
		return 0;

	/* Call internal rank function to count set bits */
	rank_result = bms_rank_range((Bitmapset *) map,
								 (size_t) start_bit,
								 (size_t) end_bit,
								 target_value,
								 NULL);

	/* Ensure result fits in int return type */
	if (rank_result > INT_MAX)
	{
		elog(WARNING, "rank result %zu exceeds INT_MAX, truncating", rank_result);
		return INT_MAX;
	}

	return (int) rank_result;
}

/*
 * bms_num_members
 *      Count the total number of set bits in the bitmap
 *
 * Returns the total count of set bits across the entire bitmap.
 * This is equivalent to calling bms_rank on the full range of
 * possible bit indices.
 *
 * Parameters:
 *  map: The bitmap to examine (NULL returns 0)
 *
 * Returns:
 *  Total number of set bits in the bitmap
 */
int
bms_num_members(const Bitmapset *map)
{
	Assert(bms_is_valid_set(map));

	if (map == NULL)
		return 0;

	/*
	 * Count all set bits from 0 to the maximum possible bit index. Using
	 * INT_MAX ensures we cover all possible bit positions that can be
	 * represented in the return type.
	 */
	return bms_rank(map, 0, INT_MAX, true);
}

/*
 * bms_density
 *      Calculate the density of set bits in the bitmap
 *
 * Density is defined as the percentage of set bits within the range
 * from 0 to the highest set bit. This provides a measure of how
 * "sparse" or "dense" the bitmap is.
 *
 * Formula: (number_of_set_bits / (highest_bit_index + 1)) * 100
 *
 * Parameters:
 *  map: The bitmap to examine (NULL returns 0.0)
 *
 * Returns:
 *  Density percentage (0.0 to 100.0), or 0.0 if bitmap is empty
 */
double
bms_density(const Bitmapset *map)
{
	int			num_members;
	int			last_member;
	double		density_percent;

	if (map == NULL)
		return 0.0;

	/* Get the highest set bit index */
	last_member = bms_last_member(map);
	if (last_member == -1)
		return 0.0;				/* Empty bitmap */

	/* Count total set bits */
	num_members = bms_num_members(map);
	if (num_members == 0)
		return 0.0;				/* Should not happen if last_member != -1, but
								 * be safe */

	/*
	 * Calculate density as percentage. Range is [0, last_member] inclusive,
	 * so total possible positions is last_member + 1.
	 */
	density_percent = ((double) num_members / (double) (last_member + 1)) * 100.0;

	return density_percent;
}

/*
 * bms_scan
 *      Iterate through set bits in the bitmap, calling a function for each
 *
 * Scans through all set bits in the bitmap in ascending order, calling
 * the provided scanner function for groups of consecutive set bits.
 * The scanner function receives arrays of bit indices and can process
 * them efficiently in batches.
 *
 * The skip parameter allows starting the scan from a specific number
 * of set bits into the bitmap, which is useful for pagination or
 * resuming interrupted scans.
 *
 * Parameters:
 *  map: The bitmap to scan (NULL is treated as empty)
 *  scanner: Function to call for each batch of set bits
 *  skip_count: Number of set bits to skip before starting scan
 *  aux_data: Auxiliary data passed to scanner function
 */
static void
bms_scan(const Bitmapset *map,
		 void (*scanner) (bms_idx_t bit_indices[], size_t count, void *aux_data),
		 size_t skip_count,
		 void *aux_data)
{
	uint8	   *chunk_data_ptr;
	size_t		chunk_count;
	size_t		chunk_idx;
	size_t		remaining_skip;
	size_t		bits_skipped;
	chunk_off_t chunk_start_bit;
	bms_chunk_t chunk;
	size_t		chunk_offset;

	Assert(bms_is_valid_set(map));

	/* Handle NULL or empty bitmap */
	if (map == NULL)
		return;

	chunk_count = bms_get_chunk_count(map);
	if (chunk_count == 0)
		return;

	remaining_skip = skip_count;
	chunk_offset = BMS_SIZEOF_OVERHEAD;

	/*
	 * Iterate through all chunks in ascending order. Each chunk may contain
	 * multiple ranges of set bits.
	 */
	for (chunk_idx = 0; chunk_idx < chunk_count; chunk_idx++)
	{
		/* Load chunk header and initialize chunk structure */
		chunk_data_ptr = bms_get_chunk_data(map, chunk_offset);
		chunk_start_bit = *(const chunk_off_t *) chunk_data_ptr;
		bms_chunk_init(&chunk, chunk_data_ptr + sizeof(chunk_off_t));

		/*
		 * Scan this chunk, which may skip some bits if we haven't reached our
		 * starting position yet.
		 */
		bits_skipped = bms_chunk_scan_set_bits(&chunk, chunk_start_bit, scanner,
											   remaining_skip, aux_data);

		/*
		 * Update skip count based on how many bits this chunk skipped. Once
		 * remaining_skip reaches 0, all subsequent chunks will process all
		 * their set bits.
		 */
		if (remaining_skip > 0)
		{
			Assert(remaining_skip >= bits_skipped);
			remaining_skip -= bits_skipped;
		}

		/* Move to next chunk */
		chunk_offset += sizeof(chunk_off_t) + bms_chunk_get_size(&chunk);
	}
}

/*
 * bms_find_insertion_point - Find where to insert new chunk
 */
static size_t
bms_find_insertion_point(Bitmapset *map, chunk_off_t target_bit)
{
	size_t		chunk_count = bms_get_chunk_count(map);
	size_t		offset = 0;

	for (size_t i = 0; i < chunk_count; i++)
	{
		bms_chunk_t chunk;
		uint8	   *chunk_ptr = bms_get_chunk_data(map, offset);
		chunk_off_t chunk_start = *(chunk_off_t *) chunk_ptr;

		if (target_bit < chunk_start)
			return offset;

		bms_chunk_init(&chunk, chunk_ptr + sizeof(chunk_off_t));
		offset += sizeof(chunk_off_t) + bms_chunk_get_size(&chunk);
	}

	return offset;
}

/*
 * bms_merge_into
 *      Merge all set bits from source bitmap into destination bitmap
 *
 * Performs a union operation by merging all chunks from the source bitmap
 * into the destination bitmap. The destination bitmap is modified in place
 * and may be reallocated if additional capacity is needed.
 *
 * The merge operation handles various chunk overlap scenarios:
 * - Non-overlapping chunks are inserted directly
 * - Perfectly aligned chunks are merged using chunk-level operations
 * - Partially overlapping chunks require bit-level merging
 *
 * Parameters:
 *  dest_map: The destination bitmap to merge into (modified in place)
 *  source_map: The source bitmap to merge from (read-only)
 *
 * Returns:
 *  Number of chunks successfully merged, or negative value indicating
 *  the number of additional bytes needed in destination for the merge
 */
static int
bms_merge_into(Bitmapset *dest_map, const Bitmapset *source_map)
{
	uint8	   *source_chunk_ptr;
	uint8	   *dest_chunk_ptr;
	size_t		source_chunk_count;
	size_t		source_chunk_idx;
	size_t		chunks_merged = 0;
	ssize_t		required_space;
	ssize_t		available_space;
	chunk_off_t source_start_bit;
	chunk_off_t dest_start_bit;
	bms_chunk_t source_chunk;
	bms_chunk_t dest_chunk;
	ssize_t		dest_chunk_offset;
	size_t		source_chunk_size;
	size_t		dest_chunk_capacity;
	size_t		source_chunk_capacity;
	size_t		source_chunk_offset;

	Assert(dest_map != NULL);
	Assert(source_map != NULL);
	Assert(bms_is_valid_set(dest_map));
	Assert(bms_is_valid_set(source_map));

	source_chunk_count = bms_get_chunk_count(source_map);
	if (source_chunk_count == 0)
		return 0;

	/*
	 * Estimate space requirements for worst-case merge scenario. Each source
	 * chunk might need to be inserted as a new chunk.
	 */
	required_space = source_map->used +
		source_chunk_count * BMS_CHUNK_INITIAL_SIZE;
	available_space = dest_map->size - dest_map->used;

	if (available_space < required_space)
	{
		return -(required_space - available_space);
	}

	/*
	 * Process each source chunk in order, merging it into the destination.
	 */
	source_chunk_offset = BMS_SIZEOF_OVERHEAD;

	for (source_chunk_idx = 0; source_chunk_idx < source_chunk_count; source_chunk_idx++)
	{
		/* Load source chunk */
		source_chunk_ptr = bms_get_chunk_data(source_map, source_chunk_offset);
		source_start_bit = *(const chunk_off_t *) source_chunk_ptr;
		bms_chunk_init(&source_chunk, source_chunk_ptr + sizeof(chunk_off_t));

		source_chunk_size = bms_chunk_get_size(&source_chunk);
		source_chunk_capacity = bms_chunk_get_capacity(&source_chunk);

		/* Find corresponding destination chunk */
		dest_chunk_offset = bms_find_chunk_offset(dest_map, source_start_bit);

		if (dest_chunk_offset == 0)
		{
			/*
			 * No overlapping destination chunk found. Insert the entire
			 * source chunk.
			 */
			size_t		insertion_offset = bms_find_insertion_point(dest_map, source_start_bit);

			bms_insert_data(dest_map, insertion_offset, source_chunk_ptr,
							sizeof(chunk_off_t) + source_chunk_size);
			bms_set_chunk_count(dest_map, bms_get_chunk_count(dest_map) + 1);
			chunks_merged++;
		}
		else
		{
			/*
			 * Found overlapping destination chunk. Handle various overlap
			 * scenarios.
			 */
			dest_chunk_ptr = bms_get_chunk_data(dest_map, dest_chunk_offset);
			dest_start_bit = *(chunk_off_t *) dest_chunk_ptr;
			bms_chunk_init(&dest_chunk, dest_chunk_ptr + sizeof(chunk_off_t));
			dest_chunk_capacity = bms_chunk_get_capacity(&dest_chunk);

			if (source_start_bit == dest_start_bit &&
				source_chunk_capacity == dest_chunk_capacity)
			{
				/*
				 * Perfect alignment - merge chunks directly
				 */
				bms_merge_chunk_bits(dest_map, source_start_bit, dest_start_bit,
									 source_chunk_capacity, &dest_chunk, &source_chunk);
				chunks_merged++;
			}
			else if (source_start_bit + source_chunk_capacity <= dest_start_bit ||
					 dest_start_bit + dest_chunk_capacity <= source_start_bit)
			{
				/*
				 * No actual overlap - insert as separate chunk
				 */
				size_t		insertion_offset = (source_start_bit < dest_start_bit) ?
					dest_chunk_offset :
					dest_chunk_offset + sizeof(chunk_off_t) +
					bms_chunk_get_size(&dest_chunk);

				bms_insert_data(dest_map, insertion_offset, source_chunk_ptr,
								sizeof(chunk_off_t) + source_chunk_size);
				bms_set_chunk_count(dest_map, bms_get_chunk_count(dest_map) + 1);
				chunks_merged++;
			}
			else
			{
				/*
				 * Partial overlap - requires bit-level merging
				 */
				size_t		overlap_start = (source_start_bit > dest_start_bit) ?
					source_start_bit : dest_start_bit;
				size_t		overlap_end = ((source_start_bit + source_chunk_capacity) <
										   (dest_start_bit + dest_chunk_capacity)) ?
					(source_start_bit + source_chunk_capacity - 1) :
					(dest_start_bit + dest_chunk_capacity - 1);

				/* Merge the overlapping portion */
				for (size_t bit_idx = overlap_start; bit_idx <= overlap_end; bit_idx++)
				{
					if (bms_is_member(bit_idx, source_map))
					{
						bms_set_bit(dest_map, bit_idx, true);
					}
				}
				chunks_merged++;
			}
		}

		/* Move to next source chunk */
		source_chunk_offset += sizeof(chunk_off_t) + source_chunk_size;
	}

	return (int) chunks_merged;
}

/*
 * bms_union
 *      Create a new bitmap containing all bits set in either input bitmap
 *
 * Performs a union operation (logical OR) on two bitmaps, creating a new
 * bitmap that contains all bits that are set in either input bitmap.
 * Both input bitmaps are left unmodified.
 *
 * The implementation optimizes by copying the larger bitmap first and
 * then merging the smaller one into it, which minimizes the amount of
 * data that needs to be processed during the merge operation.
 *
 * Parameters:
 *  map_a: First input bitmap (NULL treated as empty)
 *  map_b: Second input bitmap (NULL treated as empty)
 *
 * Returns:
 *  New bitmap containing union of both inputs, or NULL if both inputs are NULL
 */
Bitmapset *
bms_union(const Bitmapset *map_a, const Bitmapset *map_b)
{
	Bitmapset  *result_map;
	const Bitmapset *source_map;
	int			merge_result;
	size_t		additional_space_needed;

	Assert(bms_is_valid_set(map_a));
	Assert(bms_is_valid_set(map_b));

	/* Handle cases where either input is NULL */
	if (map_a == NULL)
		return bms_copy(map_b);
	if (map_b == NULL)
		return bms_copy(map_a);

	/*
	 * Optimize by copying the larger bitmap first and merging the smaller one
	 * into it. This reduces the amount of data movement during merge.
	 */
	if (bms_get_capacity(map_a) >= bms_get_capacity(map_b))
	{
		result_map = bms_copy(map_a);
		source_map = map_b;
	}
	else
	{
		result_map = bms_copy(map_b);
		source_map = map_a;
	}

	/* Attempt to merge the source bitmap into the result */
	merge_result = bms_merge_into(result_map, source_map);

	if (merge_result < 0)
	{
		/*
		 * Merge failed due to insufficient capacity. Resize the result bitmap
		 * and retry the merge.
		 */
		additional_space_needed = (size_t) (-merge_result);

		result_map = bms_resize(result_map, NULL,
								bms_get_capacity(result_map) + additional_space_needed);
		if (result_map == NULL)
			elog(ERROR, "failed to resize bitmap for union operation");

		/* Retry the merge operation */
		merge_result = bms_merge_into(result_map, source_map);
		if (merge_result < 0)
			elog(ERROR, "bitmap union failed after resize: still need %d bytes",
				 -merge_result);
	}

	return result_map;
}

/*
 * bms_select
 *      Find the n-th occurrence of the specified bit value in the bitmap
 *
 * Returns the bit index of the n-th occurrence of 'value' (0 or 1).
 * If there are fewer than n occurrences, returns -2.
 * n is 0-based (0 means first occurrence).
 */
int
bms_select(const Bitmapset *map, int n, bool value)
{
	size_t		chunk_count;
	uint8	   *p;
	size_t		remaining;

	/* Handle NULL input */
	if (map == NULL)
	{
		/* For false bits, infinite zeros available */
		return value ? -2 : n;
	}

	Assert(bms_is_valid_set(map));

	/* Negative n is invalid */
	if (n < 0)
		return -2;

	chunk_count = bms_get_chunk_count(map);
	remaining = (size_t) n;

	/* Empty bitmap - only false bits available */
	if (chunk_count == 0)
		return value ? -2 : n;

	p = bms_get_chunk_data(map, 0);

	for (size_t i = 0; i < chunk_count; i++)
	{
		chunk_off_t chunk_start;
		bms_chunk_t chunk;
		size_t		result_index;
		size_t		gap_size;

		/* Read chunk start offset */
		chunk_start = *(chunk_off_t *) p;
		p += sizeof(chunk_off_t);

		/* Initialize chunk structure */
		bms_chunk_init(&chunk, p);

		/* Handle gap before first chunk */
		if (i == 0 && chunk_start > 0)
		{
			gap_size = chunk_start;

			if (!value)
			{
				/* Looking for false bits in the gap */
				if (remaining < gap_size)
					return (int) remaining;
				remaining -= gap_size;
			}
			/* For true bits, gap contributes nothing */
		}

		/* Search within this chunk */
		result_index = bms_chunk_select(&chunk, remaining, &remaining, value);

		/* Check if we found the n-th occurrence */
		if (remaining == SIZE_MAX)	/* bms_chunk_select uses SIZE_MAX for
									 * "found" */
		{
			size_t		final_index = chunk_start + result_index;

			/* Ensure result fits in int range */
			if (final_index > INT_MAX)
				return -2;

			return (int) final_index;
		}

		/* Move to next chunk */
		p += bms_chunk_get_size(&chunk);

		/* Handle gap after this chunk (if not the last chunk) */
		if (i + 1 < chunk_count && !value)
		{
			chunk_off_t next_chunk_start;
			size_t		next_gap_size;

			/* Peek at next chunk start to calculate gap */
			next_chunk_start = *(chunk_off_t *) p;
			next_gap_size = next_chunk_start - (chunk_start + bms_chunk_get_capacity(&chunk));

			if (next_gap_size > 0)
			{
				if (remaining < next_gap_size)
				{
					size_t		gap_result = chunk_start + bms_chunk_get_capacity(&chunk) + remaining;

					if (gap_result > INT_MAX)
						return -2;

					return (int) gap_result;
				}
				remaining -= next_gap_size;
			}
		}
	}

	/* Handle infinite trailing zeros for false bit searches */
	if (!value)
	{
		bms_chunk_t last_chunk;
		chunk_off_t last_chunk_start;
		size_t		last_chunk_end;
		size_t		final_result;

		/* Calculate where the last chunk ends */
		p = bms_get_chunk_data(map, 0);
		for (size_t i = 0; i < chunk_count - 1; i++)
		{
			bms_chunk_t temp_chunk;

			p += sizeof(chunk_off_t);
			bms_chunk_init(&temp_chunk, p);
			p += bms_chunk_get_size(&temp_chunk);
		}

		last_chunk_start = *(chunk_off_t *) p;
		p += sizeof(chunk_off_t);
		bms_chunk_init(&last_chunk, p);

		last_chunk_end = last_chunk_start + bms_chunk_get_capacity(&last_chunk);
		final_result = last_chunk_end + remaining;

		if (final_result > INT_MAX)
			return -2;

		return (int) final_result;
	}

	/* Not enough occurrences of the requested bit value */
	return -2;
}

/*
 * bms_split
 *      Split a bitmap at the specified bit position
 *
 * Splits the input bitmap into two parts: bits before the split point
 * remain in the original bitmap, while bits at and after the split point
 * are moved to the destination bitmap.
 *
 * If split_bit is INT_MAX, the split occurs at the median set bit position,
 * effectively creating two bitmaps with approximately equal numbers of set bits.
 *
 * Parameters:
 *  source_map: The bitmap to split (modified to contain first part)
 *  split_bit: Bit position to split at, or INT_MAX for median split
 *  dest_map: Destination bitmap to receive second part (must have capacity)
 *
 * Returns:
 *  Actual split position used, or -1 if split was not possible
 */
int
bms_split(Bitmapset *source_map, int split_bit, Bitmapset *dest_map)
{
	int			actual_split_bit;
	int			first_bit;
	int			last_bit;
	size_t		total_set_bits;
	size_t		median_position;
	size_t		source_chunk_count;
	size_t		chunk_idx;
	size_t		chunks_moved = 0;
	uint8	   *source_chunk_ptr;
	uint8	   *dest_chunk_ptr;
	chunk_off_t chunk_start_bit;
	bms_chunk_t source_chunk;
	bms_chunk_t dest_chunk;
	size_t		chunk_capacity;
	size_t		source_chunk_offset;
	size_t		dest_chunk_offset;
	bool		split_within_chunk = false;
	uint8		new_chunk_buffer[BMS_CHUNK_INITIAL_SIZE] = {0};

	Assert(source_map != NULL);
	Assert(dest_map != NULL);
	Assert(bms_is_valid_set(source_map));
	Assert(bms_is_valid_set(dest_map));

	/* Handle edge cases */
	if (split_bit != INT_MAX && split_bit >= bms_last_member(source_map))
		return -1;

	/* Determine actual split position */
	if (split_bit == INT_MAX)
	{
		/* Find median split position */
		first_bit = bms_first_member(source_map);
		last_bit = bms_last_member(source_map);

		if (first_bit == -1 || last_bit == -1 || first_bit == last_bit)
			return -1;			/* Cannot split empty or single-bit bitmap */

		total_set_bits = bms_num_members(source_map);
		median_position = total_set_bits / 2;
		actual_split_bit = bms_select(source_map, median_position, true);

		if (actual_split_bit == -1)
			return -1;
	}
	else
	{
		actual_split_bit = split_bit;
	}

	/*
	 * Find the chunk containing or following the split position
	 */
	source_chunk_count = bms_get_chunk_count(source_map);
	source_chunk_offset = BMS_SIZEOF_OVERHEAD;
	dest_chunk_offset = bms_get_used_size(dest_map);

	for (chunk_idx = 0; chunk_idx < source_chunk_count; chunk_idx++)
	{
		source_chunk_ptr = bms_get_chunk_data(source_map, source_chunk_offset);
		chunk_start_bit = *(const chunk_off_t *) source_chunk_ptr;
		bms_chunk_init(&source_chunk, source_chunk_ptr + sizeof(chunk_off_t));
		chunk_capacity = bms_chunk_get_capacity(&source_chunk);

		if (chunk_start_bit == actual_split_bit)
		{
			/* Split exactly at chunk boundary */
			break;
		}
		else if (chunk_start_bit + chunk_capacity > actual_split_bit)
		{
			/* Split falls within this chunk */
			split_within_chunk = true;
			break;
		}
		else if (chunk_start_bit > actual_split_bit)
		{
			/* Split falls before this chunk */
			break;
		}

		/* Move to next chunk */
		source_chunk_offset += sizeof(chunk_off_t) + bms_chunk_get_size(&source_chunk);
	}

	/* Handle case where split is beyond all chunks */
	if (chunk_idx == source_chunk_count)
		return actual_split_bit;

	/*
	 * Handle splitting within a chunk
	 */
	if (split_within_chunk)
	{
		size_t		bit_offset_in_chunk = actual_split_bit - chunk_start_bit;
		size_t		aligned_split_offset = bms_get_vector_aligned_offset(bit_offset_in_chunk);

		/* Create new chunk in destination for split portion */
		*(chunk_off_t *) new_chunk_buffer = bms_get_vector_aligned_offset(actual_split_bit);

		/* Ensure destination has space for new chunk */
		if (dest_map->size - dest_map->used < sizeof(new_chunk_buffer))
		{
			elog(ERROR, "insufficient space in destination bitmap for split");
		}

		/* Copy chunk header to destination */
		dest_chunk_ptr = dest_map->data + dest_chunk_offset;
		memcpy(dest_chunk_ptr, new_chunk_buffer, sizeof(new_chunk_buffer));

		/* Initialize destination chunk */
		bms_chunk_init(&dest_chunk, dest_chunk_ptr + sizeof(chunk_off_t));
		bms_chunk_reduce_capacity(&dest_chunk,
								  bms_get_vector_aligned_offset(chunk_capacity - aligned_split_offset));

		/* Copy bits from split point onwards to destination */
		for (size_t bit_idx = actual_split_bit;
			 bit_idx < chunk_start_bit + chunk_capacity;
			 bit_idx++)
		{
			if (bms_chunk_is_member(&source_chunk, bit_idx - chunk_start_bit))
			{
				bms_set_bit(dest_map, bit_idx, true);
				bms_set_bit(source_map, bit_idx, false);
			}
		}

		/* Reduce source chunk capacity to split point */
		bms_chunk_reduce_capacity(&source_chunk, aligned_split_offset);

		/* Update counters */
		bms_set_chunk_count(dest_map, bms_get_chunk_count(dest_map) + 1);
		dest_chunk_offset += sizeof(chunk_off_t) + bms_chunk_get_size(&dest_chunk);
		source_chunk_offset += sizeof(chunk_off_t) + bms_chunk_get_size(&source_chunk);
		chunk_idx++;
	}

	/*
	 * Move all remaining complete chunks to destination
	 */
	for (; chunk_idx < source_chunk_count; chunk_idx++)
	{
		size_t		chunk_total_size;

		source_chunk_ptr = bms_get_chunk_data(source_map, source_chunk_offset);
		chunk_start_bit = *(chunk_off_t *) source_chunk_ptr;
		bms_chunk_init(&source_chunk, source_chunk_ptr + sizeof(chunk_off_t));

		chunk_total_size = sizeof(chunk_off_t) + bms_chunk_get_size(&source_chunk);

		/* Ensure destination has space */
		if (dest_map->size - dest_map->used < chunk_total_size)
		{
			elog(ERROR, "insufficient space in destination bitmap for chunk move");
		}

		/* Copy entire chunk to destination */
		dest_chunk_ptr = dest_map->data + dest_chunk_offset;
		memcpy(dest_chunk_ptr, source_chunk_ptr, chunk_total_size);

		/* Update offsets and counters */
		dest_chunk_offset += chunk_total_size;
		source_chunk_offset += chunk_total_size;
		chunks_moved++;
	}

	/* Update bitmap metadata */
	bms_set_chunk_count(source_map, bms_get_chunk_count(source_map) - chunks_moved);
	bms_set_chunk_count(dest_map, bms_get_chunk_count(dest_map) + chunks_moved);

	/* Force recalculation of used sizes */
	source_map->used = 0;
	dest_map->used = 0;
	source_map->used = bms_get_used_size(source_map);
	dest_map->used = bms_get_used_size(dest_map);

	Assert(bms_get_capacity(source_map) >= BMS_SIZEOF_OVERHEAD);
	Assert(bms_get_capacity(dest_map) >= BMS_SIZEOF_OVERHEAD);

	return actual_split_bit;
}

/*
 * bms_find_span
 *      Find the first position where a consecutive span of bits has the target value
 *
 * Searches for the first occurrence of 'span_length' consecutive bits all having
 * the specified value (set or unset). This is useful for finding contiguous
 * regions of set or unset bits within the bitmap.
 *
 * The search starts from bit position 'start_bit' and uses an optimized
 * algorithm that combines rank and select operations to skip over regions
 * that cannot contain the desired span.
 *
 * Parameters:
 *  map: The bitmap to search
 *  start_bit: Starting bit position for the search
 *  span_length: Number of consecutive bits required
 *  target_value: true to find span of set bits, false for unset bits
 *
 * Returns:
 *  Bit index of the first position of the span, or SIZE_MAX if not found
 */
size_t
bms_find_span(Bitmapset *map, size_t start_bit, size_t span_length, bool target_value)
{
	size_t		current_rank;
	size_t		nth_occurrence;
	size_t		span_rank;
	size_t		current_offset;
	size_t		skip_amount;
	bms_bitvec_t work_vector = 0;
	int			consecutive_bits;
	int			max_check_bits;

	Assert(map != NULL);
	Assert(span_length > 0);

	if (span_length == 0)
		return SIZE_MAX;

	/*
	 * Optimize the starting position by calculating how many bits with
	 * target_value occur before start_bit. This allows us to begin our select
	 * operations from the appropriate position rather than from 0.
	 */
	if (start_bit == 0)
	{
		nth_occurrence = 0;
	}
	else
	{
		current_rank = bms_rank_range(map, 0, start_bit - 1, target_value, &work_vector);
		nth_occurrence = current_rank;
	}

	/* Find the first bit with target_value at or after start_bit */
	current_offset = bms_select(map, nth_occurrence, target_value);
	if (current_offset == SIZE_MAX)
		return SIZE_MAX;		/* No bits with target_value found */

	/*
	 * Search for a span of the required length. Use rank operations to
	 * efficiently check if a range contains the required number of
	 * consecutive bits with target_value.
	 */
	do
	{
		/*
		 * Check if the range [current_offset, current_offset + span_length -
		 * 1] contains exactly span_length bits with target_value.
		 */
		if (span_length == 1)
		{
			span_rank = 1;		/* Single bit case - we know it matches */
		}
		else
		{
			span_rank = bms_rank_range(map, current_offset,
									   current_offset + span_length - 1,
									   target_value, &work_vector);
		}

		if (span_rank >= span_length)
		{
			/*
			 * Found a span with at least the required number of bits. This is
			 * our answer.
			 */
			return current_offset;
		}

		/*
		 * Current position doesn't have a sufficient span. Try to skip
		 * forward efficiently by examining the work_vector returned from the
		 * rank operation.
		 */
		skip_amount = 1;		/* Default: advance by one position */

		if (work_vector > 0)
		{
			/*
			 * The work_vector contains information about bit patterns in the
			 * examined range. Use this to skip over consecutive bits that
			 * match our target, since we know they can't form a complete span
			 * of the required length.
			 */
			max_check_bits = (span_length > BMS_BITVEC_BITS) ?
				BMS_BITVEC_BITS : (int) span_length;
			consecutive_bits = 1;

			while (consecutive_bits < max_check_bits &&
				   (work_vector & ((bms_bitvec_t) 1 << consecutive_bits)))
			{
				consecutive_bits++;
			}

			skip_amount = consecutive_bits;
		}

		/* Move to the next candidate position */
		nth_occurrence += skip_amount;
		current_offset = bms_select(map, nth_occurrence, target_value);

	} while (current_offset != SIZE_MAX);

	/* No span of the required length was found */
	return SIZE_MAX;
}

/*
 * bms_equal
 *      Test whether two bitmaps are equal
 */
bool
bms_equal(const Bitmapset *map_a, const Bitmapset *map_b)
{
	Assert(bms_is_valid_set(map_a));
	Assert(bms_is_valid_set(map_b));

	if (map_a == NULL && map_b == NULL)
		return true;
	if (map_a == NULL || map_b == NULL)
		return false;

	if (map_a->used != map_b->used)
		return false;

	return memcmp(map_a->data, map_b->data, map_a->used) == 0;
}

/*
 * bms_compare
 *      Compare two bitmaps lexicographically
 */
int
bms_compare(const Bitmapset *map_a, const Bitmapset *map_b)
{
	int			first_a,
				first_b;
	int			member_a,
				member_b;

	Assert(bms_is_valid_set(map_a));
	Assert(bms_is_valid_set(map_b));

	if (map_a == map_b)
		return 0;
	if (map_a == NULL)
		return -1;
	if (map_b == NULL)
		return 1;

	/* Compare members in ascending order */
	first_a = bms_first_member(map_a);
	first_b = bms_first_member(map_b);
	member_a = first_a;
	member_b = first_b;

	while (member_a >= 0 && member_b >= 0)
	{
		if (member_a < member_b)
			return -1;
		if (member_a > member_b)
			return 1;

		member_a = bms_next_member(map_a, member_a);
		member_b = bms_next_member(map_b, member_b);
	}

	if (member_a >= 0)
		return 1;				/* map_a has more members */
	if (member_b >= 0)
		return -1;				/* map_b has more members */

	return 0;					/* equal */
}

/*
 * bms_make_singleton
 *      Create a bitmap containing a single member
 */
/*
 * bms_make_singleton - create a bitmap containing a single member
 */
Bitmapset *
bms_make_singleton(int member)
{
	Bitmapset  *result;

	if (member < 0)
		elog(ERROR, "negative bitmapset member not allowed");

	result = bms_create(BMS_DEFAULT_SIZE);
	if (result == NULL)
		return NULL;

	return bms_add_member(result, member);
}

/*
 * bms_free
 *      Free a bitmap and set pointer to NULL
 */
void
bms_free(Bitmapset *map)
{
	if (map != NULL)
	{
		/* Check if using integrated allocation */
		if ((uintptr_t) map->data == MAXALIGN((uintptr_t) map + sizeof(Bitmapset)))
		{
			/* Integrated allocation - free entire structure */
			pfree(map);
		}
		else
		{
			/* Separate allocations - caller owns free'ing data */
			pfree(map);
		}
	}
}

/*
 * bms_intersect
 *      Create intersection of two bitmaps
 */
Bitmapset *
bms_intersect(const Bitmapset *map_a, const Bitmapset *map_b)
{
	Bitmapset  *result;
	int			member;

	Assert(bms_is_valid_set(map_a));
	Assert(bms_is_valid_set(map_b));

	if (map_a == NULL || map_b == NULL)
		return NULL;

	if (bms_num_members(map_a) > bms_num_members(map_b))
	{
		result = bms_copy(map_a);
		bms_merge_into(result, map_b);
	}
	else
	{
		result = bms_copy(map_b);
		bms_merge_into(result, map_a);
	}

	if (bms_num_members(result) == 0)
	{
		bms_free(result);
		return NULL;
	}

	return result;
}

/*
 * bms_difference
 *      Create difference of two bitmaps (map_a - map_b)
 */
Bitmapset *
bms_difference(const Bitmapset *map_a, const Bitmapset *map_b)
{
	Bitmapset  *result;
	int			member;

	Assert(bms_is_valid_set(map_a));
	Assert(bms_is_valid_set(map_b));

	if (map_a == NULL)
		return NULL;
	if (map_b == NULL)
		return bms_copy(map_a);

	result = bms_create(BMS_DEFAULT_SIZE);
	if (result == NULL)
		return NULL;

	member = bms_first_member(map_a);
	while (member >= 0)
	{
		if (!bms_is_member(member, map_b))
			result = bms_add_member(result, member);
		member = bms_next_member(map_a, member);
	}

	if (bms_num_members(result) == 0)
	{
		bms_free(result);
		return NULL;
	}

	return result;
}

/*
 * bms_is_subset
 *      Test whether map_a is a subset of map_b
 */
bool
bms_is_subset(const Bitmapset *map_a, const Bitmapset *map_b)
{
	int			member;

	Assert(bms_is_valid_set(map_a));
	Assert(bms_is_valid_set(map_b));

	if (map_a == NULL)
		return true;			/* empty set is subset of any set */
	if (map_b == NULL)
		return false;			/* non-empty set is not subset of empty set */

	member = bms_first_member(map_a);
	while (member >= 0)
	{
		if (!bms_is_member(member, map_b))
			return false;
		member = bms_next_member(map_a, member);
	}

	return true;
}

/*
 * bms_subset_compare
 *      Compare two bitmaps for subset relationships
 */
BMS_Comparison
bms_subset_compare(const Bitmapset *map_a, const Bitmapset *map_b)
{
	bool		a_subset_b;
	bool		b_subset_a;

	Assert(bms_is_valid_set(map_a));
	Assert(bms_is_valid_set(map_b));

	if (map_a == NULL && map_b == NULL)
		return BMS_EQUAL;
	if (map_a == NULL)
		return BMS_SUBSET1;
	if (map_b == NULL)
		return BMS_SUBSET2;

	a_subset_b = bms_is_subset(map_a, map_b);
	b_subset_a = bms_is_subset(map_b, map_a);

	if (a_subset_b && b_subset_a)
		return BMS_EQUAL;
	if (a_subset_b)
		return BMS_SUBSET1;
	if (b_subset_a)
		return BMS_SUBSET2;

	return BMS_DIFFERENT;
}

/*
 * bms_member_index
 *      Return the 0-based index of member x in the bitmap
 */
int
bms_member_index(Bitmapset *map, int x)
{
	int			member;
	int			index = 0;

	Assert(bms_is_valid_set(map));

	if (map == NULL || x < 0)
		return -1;

	member = bms_first_member(map);
	while (member >= 0)
	{
		if (member == x)
			return index;
		if (member > x)
			break;				/* x is not in the set */
		index++;
		member = bms_next_member(map, member);
	}

	return -1;
}

/*
 * bms_overlap
 *      Test whether two bitmaps have any common members
 */
bool
bms_overlap(const Bitmapset *map_a, const Bitmapset *map_b)
{
	int			member;

	Assert(bms_is_valid_set(map_a));
	Assert(bms_is_valid_set(map_b));

	if (map_a == NULL || map_b == NULL)
		return false;

	/* Iterate through smaller set for efficiency */
	if (bms_num_members(map_a) <= bms_num_members(map_b))
	{
		member = bms_first_member(map_a);
		while (member >= 0)
		{
			if (bms_is_member(member, map_b))
				return true;
			member = bms_next_member(map_a, member);
		}
	}
	else
	{
		member = bms_first_member(map_b);
		while (member >= 0)
		{
			if (bms_is_member(member, map_a))
				return true;
			member = bms_next_member(map_b, member);
		}
	}

	return false;
}

/*
 * bms_overlap_list
 *      Test whether bitmap overlaps with any bitmap in a list
 */
bool
bms_overlap_list(const Bitmapset *map, const struct List *list)
{
	ListCell   *lc;

	Assert(bms_is_valid_set(map));

	if (map == NULL || list == NIL)
		return false;

	foreach(lc, list)
	{
		if (bms_overlap(map, (Bitmapset *) lfirst(lc)))
			return true;
	}

	return false;
}

/*
 * bms_nonempty_difference
 *      Test whether map_a - map_b is nonempty
 */
bool
bms_nonempty_difference(const Bitmapset *map_a, const Bitmapset *map_b)
{
	int			member;

	Assert(bms_is_valid_set(map_a));
	Assert(bms_is_valid_set(map_b));

	if (map_a == NULL)
		return false;
	if (map_b == NULL)
		return (map_a != NULL);

	member = bms_first_member(map_a);
	while (member >= 0)
	{
		if (!bms_is_member(member, map_b))
			return true;
		member = bms_next_member(map_a, member);
	}

	return false;
}

/*
 * bms_singleton_member
 *      Return the single member of a singleton set, or -1 if not singleton
 */
int
bms_singleton_member(const Bitmapset *map)
{
	int			member;
	int			next_member;

	Assert(bms_is_valid_set(map));

	if (map == NULL)
		return -1;

	member = bms_first_member(map);
	if (member < 0)
		return -1;				/* empty set */

	next_member = bms_next_member(map, member);
	if (next_member >= 0)
		return -1;				/* more than one member */

	return member;
}

/*
 * bms_get_singleton_member
 *      Return the single member of a singleton set via output parameter
 */
bool
bms_get_singleton_member(const Bitmapset *map, int *member)
{
	int			result;

	Assert(member != NULL);

	result = bms_singleton_member(map);
	if (result >= 0)
	{
		*member = result;
		return true;
	}

	return false;
}

/*
 * bms_membership
 *      Determine membership status of a bitmap
 */
BMS_Membership
bms_membership(const Bitmapset *map)
{
	int			num_members;

	Assert(bms_is_valid_set(map));

	if (map == NULL)
		return BMS_EMPTY_SET;

	num_members = bms_num_members(map);
	if (num_members == 0)
		return BMS_EMPTY_SET;
	if (num_members == 1)
		return BMS_SINGLETON;

	return BMS_MULTIPLE;
}

/*
 * bms_add_members
 *      Add all members from map_b to map_a
 */
Bitmapset *
bms_add_members(Bitmapset *map_a, const Bitmapset *map_b)
{
	int			member;

	Assert(bms_is_valid_set(map_a));
	Assert(bms_is_valid_set(map_b));

	if (map_b == NULL)
		return map_a;

	return bms_union(map_a, map_b);
}

/*
 * bms_replace_members
 *      Replace contents of map_a with contents of map_b
 */
Bitmapset *
bms_replace_members(Bitmapset *map_a, const Bitmapset *map_b)
{
	if (map_a != NULL)
		bms_empty(map_a);

	return bms_add_members(map_a, map_b);
}

/*
 * bms_add_range
 *      Add all integers from lower to upper (inclusive) to the bitmap
 */
Bitmapset *
bms_add_range(Bitmapset *map, int lower, int upper)
{
	int			i;

	if (lower < 0 || upper < lower)
		elog(ERROR, "invalid range for bitmap: [%d, %d]", lower, upper);

	for (i = lower; i <= upper; i++)
	{
		map = bms_add_member(map, i);
	}

	return map;
}

/*
 * bms_int_members
 *      Intersect map_a with map_b in place (modify map_a)
 */
Bitmapset *
bms_int_members(Bitmapset *map_a, const Bitmapset *map_b)
{
	Bitmapset  *result;

	result = bms_intersect(map_a, map_b);
	if (map_a != result)
		bms_free(map_a);

	return result;
}

/*
 * bms_del_members
 *      Remove all members of map_b from map_a
 */
Bitmapset *
bms_del_members(Bitmapset *map_a, const Bitmapset *map_b)
{
	int			member;

	Assert(bms_is_valid_set(map_a));
	Assert(bms_is_valid_set(map_b));

	if (map_a == NULL || map_b == NULL)
		return map_a;

	member = bms_first_member(map_b);
	while (member >= 0)
	{
		map_a = bms_del_member(map_a, member);
		member = bms_next_member(map_b, member);
	}

	return map_a;
}

/*
 * bms_join
 *      Union two bitmaps, freeing the inputs
 */
Bitmapset *
bms_join(Bitmapset *map_a, Bitmapset *map_b)
{
	Bitmapset  *result;

	result = bms_union(map_a, map_b);
	bms_free(map_a);
	bms_free(map_b);

	return result;
}

/*
 * bms_next_member
 *      Find the next set bit after prevbit
 */
int
bms_next_member(const Bitmapset *map, int prevbit)
{
	int			member;

	Assert(bms_is_valid_set(map));

	if (map == NULL || prevbit < -1)
		return -1;

	member = prevbit + 1;
	while (member <= bms_last_member(map))
	{
		if (bms_is_member(member, map))
			return member;
		member++;
	}

	return -1;
}

/*
 * bms_prev_member
 *      Find the previous set bit before prevbit
 */
int
bms_prev_member(const Bitmapset *map, int prevbit)
{
	int			member;

	Assert(bms_is_valid_set(map));

	if (map == NULL)
		return -1;

	if (prevbit <= 0)
		return -1;

	member = prevbit - 1;
	while (member >= 0)
	{
		if (bms_is_member(member, map))
			return member;
		member--;
	}

	return -1;
}

/*
 * bms_hash_value
 *      Compute hash value for a bitmap
 */
uint32
bms_hash_value(const Bitmapset *map)
{
	uint32		hash = 0;
	int			member;

	Assert(bms_is_valid_set(map));

	if (map == NULL)
		return 0;

	member = bms_first_member(map);
	while (member >= 0)
	{
		hash ^= DatumGetUInt32(hash_uint32((uint32) member));
		member = bms_next_member(map, member);
	}

	return hash;
}

/*
 * bitmap_hash
 *      Hash function for hashtables using Bitmapsets as keys
 */
uint32
bitmap_hash(const void *key, Size keysize)
{
	Assert(keysize == sizeof(Bitmapset *));
	return bms_hash_value(*((const Bitmapset *const *) key));
}

/*
 * bitmap_match
 *      Match function for hashtables using Bitmapsets as keys
 */
int
bitmap_match(const void *key1, const void *key2, Size keysize)
{
	Assert(keysize == sizeof(Bitmapset *));
	return bms_equal(*((const Bitmapset *const *) key1),
					 *((const Bitmapset *const *) key2)) ? 0 : 1;
}

/*
 * bms_is_empty
 *      Test whether a bitmapset is empty (contains no set bits)
 *
 * A bitmapset is empty if:
 * 1. It is NULL, or
 * 2. It has zero chunks, or
 * 3. All chunks contain only unset bits
 */
bool
bms_is_empty(const Bitmapset *a)
{
	uint32		chunk_count;

	/* NULL represents empty set */
	if (a == NULL)
		return true;

	/* Only check bitmaps with caller-supplied data */
	Assert(BMS_IS_INTEGRATED_ALLOCATION(a));

	chunk_count = bms_get_chunk_count(a);

	/* No chunks means empty */
	if (chunk_count != 0)
		return false;

	return true;
}
