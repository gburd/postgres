/*
 * simple8b.h
 *		Simple-8b integer encoding/decoding
 *
 * Simple-8b packs between 1 and 240 unsigned integers into 64-bit codewords.
 * The number of integers packed into a single codeword depends on their
 * magnitude: small integers use fewer bits than large integers.
 *
 * These functions operate on raw integer values.  Callers that wish to use
 * delta encoding (as integerset.c does) must compute deltas before encoding
 * and reconstruct absolute values after decoding.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *		src/include/lib/simple8b.h
 */
#ifndef SIMPLE8B_H
#define SIMPLE8B_H

/*
 * Maximum number of integers that can be encoded in a single Simple-8b
 * codeword (mode 0: 240 zeroes).
 */
#define SIMPLE8B_MAX_VALUES_PER_CODEWORD 240

/*
 * EMPTY_CODEWORD is a special value, used to indicate "no values".
 * It is used if the first value is too large to be encoded with Simple-8b.
 *
 * This value looks like a mode-0 codeword, but we can distinguish it
 * because a regular mode-0 codeword would have zeroes in the unused bits.
 */
#define SIMPLE8B_EMPTY_CODEWORD		UINT64CONST(0x0FFFFFFFFFFFFFFF)

/*
 * Encode a number of unsigned integers into a Simple-8b codeword.
 *
 * The values in 'ints' are encoded directly (no delta computation).
 * 'num_ints' is the number of available input integers.
 *
 * Returns the encoded codeword, and sets *num_encoded to the number of
 * input integers that were encoded.  That can be zero, if the first
 * value is too large to be encoded (>= 2^60).
 */
extern uint64 simple8b_encode(const uint64 *ints, int num_ints,
							  int *num_encoded);

/*
 * Encode a run of integers where the first may differ from the rest.
 *
 * This is equivalent to calling simple8b_encode() with an input array:
 *   ints[0] = firstint
 *   ints[1..num_ints-1] = secondint
 *
 * This avoids constructing a temporary array for the common case of
 * encoding consecutive identical deltas.
 */
extern uint64 simple8b_encode_consecutive(uint64 firstint, uint64 secondint,
										  int num_ints, int *num_encoded);

/*
 * Decode a codeword into an array of integers.
 * Returns the number of integers decoded (0 for EMPTY_CODEWORD).
 * 'decoded' must have room for SIMPLE8B_MAX_VALUES_PER_CODEWORD elements.
 */
extern int	simple8b_decode(uint64 codeword, uint64 *decoded);

/*
 * Decode an array of codewords known to contain 'num_integers' integers.
 * This is a convenience wrapper around simple8b_decode().
 */
extern void simple8b_decode_words(uint64 *codewords, int num_codewords,
								  uint64 *dst, int num_integers);

#endif							/* SIMPLE8B_H */
