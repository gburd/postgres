/*
 * simple8b.c
 *		Simple-8b integer encoding/decoding
 *
 * The simple-8b algorithm packs between 1 and 240 integers into 64-bit words,
 * called "codewords".  The number of integers packed into a single codeword
 * depends on the integers being packed; small integers are encoded using
 * fewer bits than large integers.  A single codeword can store a single
 * 60-bit integer, or two 30-bit integers, for example.
 *
 * In Simple-8b, each codeword consists of a 4-bit selector, which indicates
 * how many integers are encoded in the codeword, and the encoded integers are
 * packed into the remaining 60 bits.  The selector allows for 16 different
 * ways of using the remaining 60 bits, called "modes".  The number of integers
 * packed into a single codeword in each mode is listed in the simple8b_modes
 * table below.
 *
 * Modes 0 and 1 are a bit special; they encode a run of 240 or 120 zeroes,
 * without using the rest of the codeword bits for anything.
 *
 * Simple-8b cannot encode integers larger than 60 bits.  If the first value
 * is >= 2^60, simple8b_encode() returns SIMPLE8B_EMPTY_CODEWORD with
 * *num_encoded == 0.
 *
 * References:
 *   Vo Ngoc Anh, Alistair Moffat, Index compression using 64-bit words,
 *   Software - Practice & Experience, v.40 n.2, p.131-147, February 2010
 *   (https://doi.org/10.1002/spe.948)
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/lib/simple8b.c
 */
#include "postgres.h"

#include "lib/simple8b.h"

/*
 * Mode table: for each selector value (0-15), the number of bits per integer
 * and the number of integers that fit in the 60-bit payload.
 */
static const struct
{
	uint8		bits_per_int;
	uint8		num_ints;
}			simple8b_modes[17] =
{
	{0, 240},					/* mode  0: 240 zeroes */
	{0, 120},					/* mode  1: 120 zeroes */
	{1, 60},					/* mode  2: sixty 1-bit integers */
	{2, 30},					/* mode  3: thirty 2-bit integers */
	{3, 20},					/* mode  4: twenty 3-bit integers */
	{4, 15},					/* mode  5: fifteen 4-bit integers */
	{5, 12},					/* mode  6: twelve 5-bit integers */
	{6, 10},					/* mode  7: ten 6-bit integers */
	{7, 8},						/* mode  8: eight 7-bit integers (four bits
								 * wasted) */
	{8, 7},						/* mode  9: seven 8-bit integers (four bits
								 * wasted) */
	{10, 6},					/* mode 10: six 10-bit integers */
	{12, 5},					/* mode 11: five 12-bit integers */
	{15, 4},					/* mode 12: four 15-bit integers */
	{20, 3},					/* mode 13: three 20-bit integers */
	{30, 2},					/* mode 14: two 30-bit integers */
	{60, 1},					/* mode 15: one 60-bit integer */

	{0, 0}						/* sentinel value */
};


/*
 * Encode a number of integers into a Simple-8b codeword.
 *
 * Returns the encoded codeword, and sets *num_encoded to the number of
 * input integers that were encoded.  That can be zero, if the first value
 * is too large to be encoded.
 */
uint64
simple8b_encode(const uint64 *ints, int num_ints, int *num_encoded)
{
	int			selector;
	int			nints;
	int			bits;
	uint64		val;
	uint64		codeword;
	int			i;

	/*
	 * Select the "mode" to use for this codeword.
	 *
	 * In each iteration, check if the next value can be represented in the
	 * current mode we're considering.  If it's too large, then step up the
	 * mode to a wider one, and repeat.  If it fits, move on to the next
	 * integer.  Repeat until the codeword is full, given the current mode.
	 *
	 * Note that we don't have any way to represent unused slots in the
	 * codeword, so we require each codeword to be "full".  It is always
	 * possible to produce a full codeword unless the very first value is too
	 * large to be encoded.  For example, if the first value is small but the
	 * second is too large to be encoded, we'll end up using the last "mode",
	 * which has nints == 1.
	 */
	selector = 0;
	nints = simple8b_modes[0].num_ints;
	bits = simple8b_modes[0].bits_per_int;
	val = ints[0];
	i = 0;						/* number of values we have accepted */
	for (;;)
	{
		if (val >= (UINT64CONST(1) << bits))
		{
			/* too large, step up to next mode */
			selector++;
			nints = simple8b_modes[selector].num_ints;
			bits = simple8b_modes[selector].bits_per_int;
			/* we might already have accepted enough values for this mode */
			if (i >= nints)
				break;
		}
		else
		{
			/* accept this value; then done if codeword is full */
			i++;
			if (i >= nints)
				break;
			/* examine next value */
			if (i < num_ints)
				val = ints[i];
			else
			{
				/*
				 * Reached end of input. Pretend that the next integer is a
				 * value that's too large to represent in Simple-8b, so that
				 * we fall out.
				 */
				val = PG_UINT64_MAX;
			}
		}
	}

	if (nints == 0)
	{
		/*
		 * The first value is too large to be encoded with Simple-8b.
		 *
		 * If there is at least one not-too-large integer in the input, we
		 * will encode it using mode 15 (or a more compact mode).  Hence, we
		 * can only get here if the *first* value is >= 2^60.
		 */
		Assert(i == 0);
		*num_encoded = 0;
		return SIMPLE8B_EMPTY_CODEWORD;
	}

	/*
	 * Encode the integers using the selected mode.  Note that we shift them
	 * into the codeword in reverse order, so that they will come out in the
	 * correct order in the decoder.
	 */
	codeword = 0;
	if (bits > 0)
	{
		for (i = nints - 1; i > 0; i--)
		{
			val = ints[i];
			codeword |= val;
			codeword <<= bits;
		}
		val = ints[0];
		codeword |= val;
	}

	/* add selector to the codeword, and return */
	codeword |= (uint64) selector << 60;

	*num_encoded = nints;
	return codeword;
}

/*
 * Encode a run of integers where the first may differ from the rest.
 *
 * This is equivalent to calling simple8b_encode() with an input array
 * where ints[0] = firstint and ints[1..num_ints-1] = secondint, but
 * avoids constructing a temporary array.
 */
uint64
simple8b_encode_consecutive(uint64 firstint, uint64 secondint,
							int num_ints, int *num_encoded)
{
	int			selector;
	int			nints;
	int			bits;
	uint64		val;
	uint64		codeword;
	int			i;

	selector = 0;
	nints = simple8b_modes[0].num_ints;
	bits = simple8b_modes[0].bits_per_int;
	val = firstint;
	i = 0;
	for (;;)
	{
		if (val >= (UINT64CONST(1) << bits))
		{
			selector++;
			nints = simple8b_modes[selector].num_ints;
			bits = simple8b_modes[selector].bits_per_int;
			if (i >= nints)
				break;
		}
		else
		{
			i++;
			if (i >= nints)
				break;
			if (i < num_ints)
				val = secondint;
			else
			{
				val = PG_UINT64_MAX;
			}
		}
	}

	if (nints == 0)
	{
		Assert(i == 0);
		*num_encoded = 0;
		return SIMPLE8B_EMPTY_CODEWORD;
	}

	codeword = 0;
	if (bits > 0)
	{
		for (i = nints - 1; i > 0; i--)
		{
			val = secondint;
			codeword |= val;
			codeword <<= bits;
		}
		val = firstint;
		codeword |= val;
	}

	codeword |= (uint64) selector << 60;

	*num_encoded = nints;
	return codeword;
}

/*
 * Decode a codeword into an array of integers.
 * Returns the number of integers decoded.
 */
int
simple8b_decode(uint64 codeword, uint64 *decoded)
{
	int			selector = (codeword >> 60);
	int			nints = simple8b_modes[selector].num_ints;
	int			bits = simple8b_modes[selector].bits_per_int;
	uint64		mask = (UINT64CONST(1) << bits) - 1;

	if (codeword == SIMPLE8B_EMPTY_CODEWORD)
		return 0;

	for (int i = 0; i < nints; i++)
	{
		uint64		val = codeword & mask;

		decoded[i] = val;
		codeword >>= bits;
	}

	return nints;
}

/*
 * Decode an array of Simple-8b codewords, known to contain 'num_integers'
 * integers total.
 */
void
simple8b_decode_words(uint64 *codewords, int num_codewords,
					  uint64 *dst, int num_integers)
{
	int			total_decoded = 0;

	for (int i = 0; i < num_codewords; i++)
	{
		int			num_decoded;

		num_decoded = simple8b_decode(codewords[i], &dst[total_decoded]);
		total_decoded += num_decoded;
	}

	if (total_decoded != num_integers)
		elog(ERROR, "number of integers in codewords did not match expected count");
}
