/*-------------------------------------------------------------------------
 *
 * bitmapset.h
 *	  PostgreSQL generic bitmap set package
 *
 * A bitmap set can represent any set of non-negative integers, although
 * it is mainly intended for sets where the maximum value is not large,
 * say at most a few hundred.  By convention, we always represent the
 * empty set by a NULL pointer.
 *
 *
 * Copyright (c) 2003-2025, PostgreSQL Global Development Group
 *
 * src/include/nodes/bitmapset.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef BITMAPSET_H
#define BITMAPSET_H

#include "nodes/nodes.h"

/*
 * Forward decl to save including pg_list.h
 */
struct List;

/*
 * Data representation
 *
 * This is an implementation for a sparse, compressed bitmap. It is resizable
 * and mutable, with reasonable performance for random access modifications
 * and lookups.
 *
 * A Bitmapset consists of multiple chunks stored sequentially in memory.
 * Each chunk encodes a contiguous range of up to BMS_CHUNK_MAX_CAPACITY bits
 * (typically 2048 bits on 64-bit systems) using compression to minimize
 * storage for uniform bit patterns.
 *
 * Memory Layout:
 * - Header: 4-byte chunk count
 * - Chunk data: Variable-length compressed chunks stored sequentially
 *
 * Each chunk consists of:
 * - 4-byte starting bit offset (chunk_off_t)
 * - Flag bitvector (bms_bitvec_t): 2-bit flags describing each sub-range
 * - Payload vectors: Only stored for "mixed" sub-ranges
 *
 * Flag Encoding (2 bits per sub-range):
 * - 0b00 (BMS_PAYLOAD_ZEROS): All bits are 0 (no payload stored)
 * - 0b01 (BMS_PAYLOAD_NONE):  Sub-range unused (reduces chunk capacity)
 * - 0b10 (BMS_PAYLOAD_MIXED): Mixed 0s and 1s (payload vector stored)
 * - 0b11 (BMS_PAYLOAD_ONES):  All bits are 1 (no payload stored)
 *
 * Compression Efficiency:
 * - Minimum size: 12 bytes (4-byte offset + 8-byte flags) for uniform patterns
 * - Maximum size: 268 bytes when all sub-ranges are mixed
 * - Empty ranges: Represented as gaps between chunks (0 bytes)
 * - Sparse bitmaps achieve significant space savings
 *
 * Three-Tier Architecture:
 *
 * Tier 0 (Bit Level): Individual bits stored in bms_bitvec_t vectors
 *   - 64-bit vectors on 64-bit platforms, 32-bit on 32-bit platforms
 *   - Direct bit manipulation for mixed payload ranges
 *
 * Tier 1 (Chunk Level): Groups of vectors with compressed representation
 *   - Each chunk covers up to BMS_CHUNK_MAX_CAPACITY contiguous bits
 *   - Flag bitvector describes BMS_FLAGS_PER_INDEX sub-ranges
 *   - Only mixed sub-ranges require storage of actual bit vectors
 *   - Uniform sub-ranges (all 0s or all 1s) compressed into flags
 *
 * Tier 2 (Bitmap Level): Collection of chunks with gap handling
 *   - Chunks stored in ascending bit-offset order
 *   - Gaps between chunks implicitly represent unset bits
 *   - Dynamic memory management for chunk insertion/removal
 *   - Automatic chunk merging and splitting for optimal storage
 *
 * Performance Characteristics:
 * - Random access: O(log chunks + constant) for bit operations
 * - Sequential access: Optimized chunk-level iteration
 * - Memory usage: Proportional to set bit density and fragmentation
 * - Sparse patterns: Excellent compression (gaps cost nothing)
 * - Dense patterns: Competitive with traditional bitmap representations
 *
 * Below is a simplified representation.
 *
 *     00 11 22 33
 *     ^-- descriptor for bms_bitvec_t 1
 *        ^-- descriptor for bms_bitvec_t 2
 *           ^-- descriptor for bms_bitvec_t 3
 *              ^-- descriptor for bms_bitvec_t 4
 *
 *    The flags can have one of the following values:
 *
 *     00   The bms_bitvec_t is all zero -> no additional vectors required
 *     11   The bms_bitvec_t is all one -> no additional vectors required
 *     10   The bms_bitvec_t contains a bitmap -> no additional vectors required
 *     01   The bms_bitvec_t is not used, used to reduce capacity
 *
 *    The serialized size of a chunk in memory therefore is at least one
 *    bms_bitvec_t for the flags, and (optionally) additional bms_bitvec_t if
 *    they are required.
 */

typedef struct Bitmapset
{
	pg_node_attr(custom_copy_equal, special_read_write, no_query_jumble)

	NodeTag		type;

	size_t		size;			/* size of data in bytes */
	size_t		used;			/* amount of data used in bytes */
	uint8	   *data;			/* pointer to the chunks that describe the
								 * bitmap */
} Bitmapset;


/* result of bms_subset_compare */
typedef enum
{
	BMS_EQUAL,					/* sets are equal */
	BMS_SUBSET1,				/* first set is a subset of the second */
	BMS_SUBSET2,				/* second set is a subset of the first */
	BMS_DIFFERENT,				/* neither set is a subset of the other */
} BMS_Comparison;

/* result of bms_membership */
typedef enum
{
	BMS_EMPTY_SET,				/* 0 members */
	BMS_SINGLETON,				/* 1 member */
	BMS_MULTIPLE,				/* >1 member */
} BMS_Membership;

/*
 * function prototypes in nodes/bitmapset.c
 */

extern Bitmapset *bms_create(size_t initial_size);
extern Bitmapset *bms_create_with_buffer(uint8 *buffer, size_t buffer_size);
extern Bitmapset *bms_attach_buffer(Bitmapset *map, uint8 *buffer, size_t buffer_size);
extern Bitmapset *bms_resize(Bitmapset *map, uint8 *new_buffer, size_t new_size);

/* Capacity and size management */
extern size_t bms_get_capacity(const Bitmapset *map);
extern size_t bms_get_used_size(const Bitmapset *map);
extern size_t bms_calculate_used_size(const Bitmapset *map);

/* Bit manipulation functions */
extern Bitmapset *bms_add_member(Bitmapset *a, int x);
extern Bitmapset *bms_del_member(Bitmapset *a, int x);

/* Advanced query functions */
extern int	bms_rank(const Bitmapset *map, int start_bit, int end_bit, bool target_value);
extern int	bms_select(const Bitmapset *map, int nth_occurrence, bool target_value);
extern size_t bms_find_span(Bitmapset *map, size_t start_bit, size_t span_length, bool target_value);

/* Split operations */
extern int	bms_split(Bitmapset *source_map, int split_bit, Bitmapset *dest_map);

extern Bitmapset *bms_copy(const Bitmapset *a);
extern bool bms_equal(const Bitmapset *a, const Bitmapset *b);
extern int	bms_compare(const Bitmapset *a, const Bitmapset *b);
extern Bitmapset *bms_make_singleton(int x);
extern void bms_free(Bitmapset *a);

extern Bitmapset *bms_union(const Bitmapset *a, const Bitmapset *b);
extern Bitmapset *bms_intersect(const Bitmapset *a, const Bitmapset *b);
extern Bitmapset *bms_difference(const Bitmapset *a, const Bitmapset *b);
extern bool bms_is_subset(const Bitmapset *a, const Bitmapset *b);
extern BMS_Comparison bms_subset_compare(const Bitmapset *a, const Bitmapset *b);
extern bool bms_is_member(int x, const Bitmapset *a);
extern int	bms_member_index(Bitmapset *a, int x);
extern bool bms_overlap(const Bitmapset *a, const Bitmapset *b);
extern bool bms_overlap_list(const Bitmapset *a, const struct List *b);
extern bool bms_nonempty_difference(const Bitmapset *a, const Bitmapset *b);
extern int	bms_singleton_member(const Bitmapset *a);
extern bool bms_get_singleton_member(const Bitmapset *a, int *member);
extern int	bms_num_members(const Bitmapset *a);
extern double bms_density(const Bitmapset *a);

/* optimized tests when we don't need to know the exact membership count: */
extern BMS_Membership bms_membership(const Bitmapset *a);

extern void bms_empty(Bitmapset *map);

/* TODO: NULL is now the only allowed representation of an empty bitmapset */
extern bool bms_is_empty(const Bitmapset *a);

/* these routines recycle (modify or free) their non-const inputs: */

extern Bitmapset *bms_add_member(Bitmapset *a, int x);
extern Bitmapset *bms_del_member(Bitmapset *a, int x);
extern Bitmapset *bms_add_members(Bitmapset *a, const Bitmapset *b);
extern Bitmapset *bms_replace_members(Bitmapset *a, const Bitmapset *b);
extern Bitmapset *bms_add_range(Bitmapset *a, int lower, int upper);
extern Bitmapset *bms_int_members(Bitmapset *a, const Bitmapset *b);
extern Bitmapset *bms_del_members(Bitmapset *a, const Bitmapset *b);
extern Bitmapset *bms_join(Bitmapset *a, Bitmapset *b);

/* support for iterating through the integer elements of a set: */
extern int	bms_first_member(const Bitmapset *a);
extern int	bms_last_member(const Bitmapset *a);
extern int	bms_next_member(const Bitmapset *a, int prevbit);
extern int	bms_prev_member(const Bitmapset *a, int prevbit);

/* support for hashtables using Bitmapsets as keys: */
extern uint32 bms_hash_value(const Bitmapset *a);
extern uint32 bitmap_hash(const void *key, Size keysize);
extern int	bitmap_match(const void *key1, const void *key2, Size keysize);

#endif							/* BITMAPSET_H */
