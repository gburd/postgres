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
 * chunks, each tagged with an 8-byte starting offset.  The map grows and
 * shrinks the underlying byte buffer as chunks are added or removed.
 *
 * Thread safety
 * -------------
 * Sparsemap is NOT thread-safe.  Concurrent reads are safe only when no
 * writer is active.  All mutating operations must be externally synchronized.
 *
 * Error handling
 * -------------
 * Functions that mutate the map return SPARSEMAP_IDX_MAX when the backing
 * buffer is full.  The caller can grow the buffer with
 * sparsemap_set_data_size() and retry.
 *
 * Allocation functions (sparsemap_create(), sparsemap_copy()) return NULL
 * on allocation failure.
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

#include "postgres.h"

/* Sparsemap structure - contains metadata and a pointer to the data buffer */
typedef struct sparsemap
{
	uint8	   *m_data;			/* pointer to the data buffer */
	size_t		m_data_used;	/* bytes currently used in the buffer */
	size_t		m_capacity;		/* total buffer capacity in bytes */
} sparsemap_t;

/* Sentinel value returned when a lookup finds no matching bit */
#define SPARSEMAP_IDX_MAX UINT64_MAX

/* Evaluates to true when x represents a valid (found) index */
#define SPARSEMAP_FOUND(x) ((x) != SPARSEMAP_IDX_MAX)

/* Evaluates to true when x represents the not-found sentinel */
#define SPARSEMAP_NOT_FOUND(x) ((x) == SPARSEMAP_IDX_MAX)

/* -------------------------------------------------------------------
 * Lifecycle
 * ------------------------------------------------------------------- */

/* Allocate a sparsemap with an internal buffer (single palloc block) */
extern sparsemap_t *sparsemap_create(size_t size);

/* Create a deep copy of another sparsemap */
extern sparsemap_t *sparsemap_copy(const sparsemap_t *other);

/* Initialize a caller-allocated sparsemap_t with a buffer (clears to empty) */
extern void sparsemap_init(sparsemap_t *map, uint8 *data, size_t size);

/* Attach to an existing (serialized) sparsemap buffer (does not clear) */
extern void sparsemap_open(sparsemap_t *map, uint8 *data, size_t size);

/* Reset the map to empty without freeing memory */
extern void sparsemap_clear(sparsemap_t *map);

/* Resize the data buffer (may relocate the map if internally allocated) */
extern sparsemap_t *sparsemap_set_data_size(sparsemap_t *map, uint8 *data,
											size_t size);

/* Free a sparsemap created by sparsemap_create() or sparsemap_copy() */
extern void sparsemap_free(sparsemap_t *map);

/* -------------------------------------------------------------------
 * Capacity and size
 * ------------------------------------------------------------------- */

/* Estimate remaining buffer capacity as a percentage */
extern double sparsemap_capacity_remaining(const sparsemap_t *map);

/* Return the total buffer capacity in bytes */
extern size_t sparsemap_get_capacity(const sparsemap_t *map);

/* Return the number of buffer bytes currently in use */
extern size_t sparsemap_get_size(sparsemap_t *map);

/* Return a pointer to the raw data buffer */
extern void *sparsemap_get_data(const sparsemap_t *map);

/* -------------------------------------------------------------------
 * Single-bit operations
 * ------------------------------------------------------------------- */

/* Test whether the bit at idx is set */
extern bool sparsemap_contains(sparsemap_t *map, uint64 idx);

/* Set or clear the bit at idx */
extern uint64 sparsemap_assign(sparsemap_t *map, uint64 idx, bool value);

/* Set the bit at idx to 1 */
extern uint64 sparsemap_add(sparsemap_t *map, uint64 idx);

/* Clear the bit at idx (set to 0) */
extern uint64 sparsemap_remove(sparsemap_t *map, uint64 idx);

/* -------------------------------------------------------------------
 * Aggregate queries
 * ------------------------------------------------------------------- */

/* Count the total number of set bits (cardinality) */
extern size_t sparsemap_cardinality(sparsemap_t *map);

/* Return the position of the first set bit (minimum) */
extern uint64 sparsemap_minimum(const sparsemap_t *map);

/* Return the position of the last set bit (maximum) */
extern uint64 sparsemap_maximum(const sparsemap_t *map);

/* Return the fraction of bits that are set */
extern double sparsemap_fill_factor(sparsemap_t *map);

/* -------------------------------------------------------------------
 * Rank, select, and span
 * ------------------------------------------------------------------- */

/* Count matching bits in the inclusive range [x, y] */
extern size_t sparsemap_rank(sparsemap_t *map, uint64 x, uint64 y, bool value);

/* Find the position of the n'th matching bit (0-based) */
extern uint64 sparsemap_select(sparsemap_t *map, uint64 n, bool value);

/* Find the first contiguous run of len bits matching value */
extern uint64 sparsemap_span(sparsemap_t *map, uint64 start, size_t len,
							 bool value);

/* -------------------------------------------------------------------
 * Iteration
 * ------------------------------------------------------------------- */

/* Invoke a callback for every set bit in the map */
extern void sparsemap_scan(const sparsemap_t *map,
						   void (*scanner)(uint64 vec[], size_t n, void *aux),
						   size_t skip, void *aux);

/* -------------------------------------------------------------------
 * Bulk operations
 * ------------------------------------------------------------------- */

/* Create a new sparsemap containing bits set in either a or b (OR) */
extern sparsemap_t *sparsemap_union(const sparsemap_t *a,
									const sparsemap_t *b);

/* Create a new sparsemap containing bits set in both a and b (AND) */
extern sparsemap_t *sparsemap_intersection(const sparsemap_t *a,
										   const sparsemap_t *b);

/* Create a new sparsemap containing bits in a but not in b (AND NOT) */
extern sparsemap_t *sparsemap_difference(const sparsemap_t *a,
										 const sparsemap_t *b);

/* Split the map at idx, moving higher bits to other */
extern uint64 sparsemap_split(sparsemap_t *map, uint64 idx,
							  sparsemap_t *other);

/* Create a new sparsemap with all bits shifted by offset */
extern sparsemap_t *sparsemap_offset(const sparsemap_t *map, int64 offset);

#endif							/* SPARSEMAP_H */
