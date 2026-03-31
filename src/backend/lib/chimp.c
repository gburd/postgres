/*
 * chimp.c
 *		Chimp float compression for Noxu columnar storage
 *
 * Chimp is an XOR-based compression algorithm for floating-point data.
 * It extends the Gorilla algorithm (Facebook, 2015) with improved
 * handling of leading and trailing zeros in XOR residuals.
 *
 * Algorithm overview:
 *   1. The first value is stored verbatim.
 *   2. For each subsequent value, compute XOR with the previous value.
 *   3. If XOR == 0 (identical values), emit a single 0-bit.
 *   4. If XOR != 0, emit a 1-bit, then encode the leading-zero count
 *      and the meaningful (non-zero) bits.  We use the Chimp128
 *      variant: leading zeros are encoded in a small lookup table
 *      (3 bits for float4, 4 bits for float8) with bucket indices,
 *      and the meaningful-bits length is stored explicitly.
 *
 * The packed bit stream is stored LSB-first in successive bytes.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/lib/chimp.c
 */
#include "postgres.h"

#include <math.h>
#include <string.h>

#include "catalog/pg_type.h"
#include "lib/chimp.h"
#include "utils/builtins.h"

/*
 * Leading-zero bucket tables for Chimp encoding.
 *
 * Instead of storing the exact leading-zero count (which would take
 * 5-6 bits), we map it to a small set of representative values.
 * During encoding, we find the bucket whose representative value is
 * <= the actual leading-zero count (i.e., the tightest lower bound).
 * During decoding, we use the bucket index to recover the representative.
 *
 * For float4 (32-bit): 8 buckets, 3 bits per index.
 * For float8 (64-bit): 16 buckets, 4 bits per index.
 */

/* float4: 8 buckets covering 0..31 leading zeros */
static const uint8 chimp_lz_buckets_f32[8] = {
	0, 4, 8, 12, 16, 20, 24, 28
};
#define CHIMP_LZ_BITS_F32		3
#define CHIMP_LZ_NBUCKETS_F32	8

/* float8: 16 buckets covering 0..63 leading zeros */
static const uint8 chimp_lz_buckets_f64[16] = {
	0, 4, 8, 12, 16, 20, 24, 28, 32, 36, 40, 44, 48, 52, 56, 60
};
#define CHIMP_LZ_BITS_F64		4
#define CHIMP_LZ_NBUCKETS_F64	16

/*
 * Find the best bucket for a given leading-zero count.
 * Returns the bucket index whose representative is the largest value <= lz.
 */
static inline int
chimp_lz_to_bucket(int lz, const uint8 *buckets, int nbuckets)
{
	int			best = 0;

	for (int i = 1; i < nbuckets; i++)
	{
		if (buckets[i] <= lz)
			best = i;
		else
			break;
	}
	return best;
}

/*
 * Bit-stream writer: packs bits LSB-first into a byte buffer.
 *
 * Uses a 64-bit accumulator to batch bit operations, flushing complete
 * bytes to the output buffer.  This avoids the overhead of per-bit
 * branching and memory access that the naive approach requires.
 */
typedef struct BitWriter
{
	unsigned char *buf;
	int			capacity;		/* in bytes */
	int			byte_pos;
	int			bit_pos;		/* 0..63, bits accumulated */
	uint64		accum;			/* accumulator for pending bits */
} BitWriter;

static inline void
bitwriter_flush(BitWriter *bw)
{
	/* Flush complete bytes from the accumulator */
	while (bw->bit_pos >= 8)
	{
		if (bw->byte_pos >= bw->capacity)
		{
			elog(ERROR, "chimp bit writer overflow");
			return;
		}
		bw->buf[bw->byte_pos++] = (unsigned char)(bw->accum & 0xFF);
		bw->accum >>= 8;
		bw->bit_pos -= 8;
	}
}

static inline void
bitwriter_init(BitWriter *bw, unsigned char *buf, int capacity)
{
	bw->buf = buf;
	bw->capacity = capacity;
	bw->byte_pos = 0;
	bw->bit_pos = 0;
	bw->accum = 0;
	/* No memset needed: we write complete bytes from the accumulator */
}

static inline void
bitwriter_put(BitWriter *bw, uint64 value, int nbits)
{
	/*
	 * Mask the value to nbits (in case caller passes extra high bits)
	 * and shift it into position in the accumulator.
	 */
	if (nbits < 64)
		value &= ((1ULL << nbits) - 1);

	bw->accum |= (value << bw->bit_pos);
	bw->bit_pos += nbits;

	/* Flush when accumulator has enough bits for complete bytes */
	if (bw->bit_pos >= 56)
		bitwriter_flush(bw);
}

static inline int
bitwriter_bytes_written(BitWriter *bw)
{
	/* Flush any remaining bits in the accumulator */
	if (bw->bit_pos > 0)
	{
		bitwriter_flush(bw);
		/* Write partial byte if any bits remain */
		if (bw->bit_pos > 0)
		{
			if (bw->byte_pos < bw->capacity)
				bw->buf[bw->byte_pos++] = (unsigned char)(bw->accum & 0xFF);
			bw->accum = 0;
			bw->bit_pos = 0;
		}
	}
	return bw->byte_pos;
}

/*
 * Bit-stream reader: reads bits LSB-first from a byte buffer.
 *
 * Uses a 64-bit accumulator to read multiple bits at once, refilling
 * from the byte buffer as needed.
 */
typedef struct BitReader
{
	const unsigned char *buf;
	int			size;			/* total bytes available */
	int			byte_pos;
	int			bit_pos;		/* 0..63, bits available in accumulator */
	uint64		accum;			/* accumulator holding pre-read bits */
} BitReader;

static inline void
bitreader_refill(BitReader *br)
{
	/* Refill accumulator with as many complete bytes as possible */
	while (br->bit_pos <= 56 && br->byte_pos < br->size)
	{
		br->accum |= ((uint64) br->buf[br->byte_pos++]) << br->bit_pos;
		br->bit_pos += 8;
	}
}

static inline void
bitreader_init(BitReader *br, const unsigned char *buf, int size)
{
	br->buf = buf;
	br->size = size;
	br->byte_pos = 0;
	br->bit_pos = 0;
	br->accum = 0;
	bitreader_refill(br);
}

static inline uint64
bitreader_get(BitReader *br, int nbits)
{
	uint64		result;
	uint64		mask;

	/* Ensure we have enough bits */
	if (br->bit_pos < nbits)
	{
		bitreader_refill(br);
		if (br->bit_pos < nbits)
		{
			elog(ERROR, "chimp bit reader underflow");
			return 0;
		}
	}

	mask = (nbits < 64) ? ((1ULL << nbits) - 1) : UINT64_MAX;
	result = br->accum & mask;
	br->accum >>= nbits;
	br->bit_pos -= nbits;

	return result;
}

/*
 * Count leading zeros in a 32-bit value.
 * Returns 32 if value is 0.
 */
static inline int
clz32(uint32 v)
{
	if (v == 0)
		return 32;
#ifdef HAVE__BUILTIN_CLZ
	return __builtin_clz(v);
#else
	{
		int			n = 0;

		if (v <= 0x0000FFFF) { n += 16; v <<= 16; }
		if (v <= 0x00FFFFFF) { n += 8;  v <<= 8; }
		if (v <= 0x0FFFFFFF) { n += 4;  v <<= 4; }
		if (v <= 0x3FFFFFFF) { n += 2;  v <<= 2; }
		if (v <= 0x7FFFFFFF) { n += 1; }
		return n;
	}
#endif
}

/*
 * Count trailing zeros in a 32-bit value.
 * Returns 32 if value is 0.
 */
static inline int
ctz32(uint32 v)
{
	if (v == 0)
		return 32;
#ifdef HAVE__BUILTIN_CTZ
	return __builtin_ctz(v);
#else
	{
		int			n = 0;

		if (!(v & 0x0000FFFF)) { n += 16; v >>= 16; }
		if (!(v & 0x000000FF)) { n += 8;  v >>= 8; }
		if (!(v & 0x0000000F)) { n += 4;  v >>= 4; }
		if (!(v & 0x00000003)) { n += 2;  v >>= 2; }
		if (!(v & 0x00000001)) { n += 1; }
		return n;
	}
#endif
}

/*
 * Count leading zeros in a 64-bit value.
 * Returns 64 if value is 0.
 */
static inline int
clz64(uint64 v)
{
	if (v == 0)
		return 64;
#ifdef HAVE__BUILTIN_CLZ
	/* Use __builtin_clzll for 64-bit */
	return __builtin_clzll(v);
#else
	{
		int			n = clz32((uint32)(v >> 32));

		if (n == 32)
			n += clz32((uint32) v);
		return n;
	}
#endif
}

/*
 * Count trailing zeros in a 64-bit value.
 * Returns 64 if value is 0.
 */
static inline int
ctz64(uint64 v)
{
	if (v == 0)
		return 64;
#ifdef HAVE__BUILTIN_CTZ
	return __builtin_ctzll(v);
#else
	{
		int			n = ctz32((uint32) v);

		if (n == 32)
			n += ctz32((uint32)(v >> 32));
		return n;
	}
#endif
}


/* ----------
 * Float4 (32-bit) encoding/decoding
 * ----------
 */

/*
 * Reinterpret a float4 as uint32 without type-punning UB.
 */
static inline uint32
float4_to_bits(float f)
{
	uint32		bits;

	memcpy(&bits, &f, sizeof(uint32));
	return bits;
}

static inline float
bits_to_float4(uint32 bits)
{
	float		f;

	memcpy(&f, &bits, sizeof(float));
	return f;
}

/*
 * chimp_encode
 *		Encode float4 values using Chimp XOR compression.
 *
 * Returns number of bytes written to dst, or 0 if encoding fails or
 * would not save space.
 */
int
chimp_encode(Datum *datums, bool *isnulls,
			 int num_elements, char *dst, int dst_capacity)
{
	ChimpBlockHeader *hdr;
	BitWriter	bw;
	int			num_nonnull = 0;
	uint32		prev_bits;
	int			hdr_size = sizeof(ChimpBlockHeader);
	int			packed_bytes;
	bool		first = true;

	/* Count non-null values */
	for (int i = 0; i < num_elements; i++)
		if (!isnulls[i])
			num_nonnull++;

	if (num_nonnull < 2)
		return 0;

	if (dst_capacity < hdr_size + 4)
		return 0;

	/* Initialize header */
	hdr = (ChimpBlockHeader *) dst;
	hdr->chimp_value_width = 4;
	hdr->chimp_leading_bits = CHIMP_LZ_BITS_F32;
	hdr->chimp_num_values = num_nonnull;
	hdr->chimp_first_value_hi = 0;

	/* Start bit stream after the header */
	bitwriter_init(&bw, (unsigned char *) dst + hdr_size,
				   dst_capacity - hdr_size);

	prev_bits = 0;

	for (int i = 0; i < num_elements; i++)
	{
		uint32		cur_bits;
		uint32		xor_val;

		if (isnulls[i])
			continue;

		cur_bits = float4_to_bits(DatumGetFloat4(datums[i]));

		if (first)
		{
			hdr->chimp_first_value_lo = cur_bits;
			prev_bits = cur_bits;
			first = false;
			continue;
		}

		xor_val = cur_bits ^ prev_bits;

		if (xor_val == 0)
		{
			/* Identical to previous: single 0-bit */
			bitwriter_put(&bw, 0, 1);
		}
		else
		{
			int			leading = clz32(xor_val);
			int			trailing = ctz32(xor_val);
			int			meaningful_bits = 32 - leading - trailing;
			int			bucket_idx;
			int			bucket_lz;
			uint32		meaningful_val;

			/* Use bucket for leading zeros */
			bucket_idx = chimp_lz_to_bucket(leading,
											chimp_lz_buckets_f32,
											CHIMP_LZ_NBUCKETS_F32);
			bucket_lz = chimp_lz_buckets_f32[bucket_idx];

			/*
			 * Adjust meaningful bits to account for bucket rounding.
			 * The bucket leading-zero count may be less than actual,
			 * so we need more meaningful bits.
			 */
			meaningful_bits = 32 - bucket_lz - trailing;
			meaningful_val = (xor_val >> trailing) & ((1ULL << meaningful_bits) - 1);

			/* Emit: 1-bit flag */
			bitwriter_put(&bw, 1, 1);

			/* Emit: leading-zero bucket index (3 bits) */
			bitwriter_put(&bw, (uint64) bucket_idx, CHIMP_LZ_BITS_F32);

			/*
			 * Emit: 5-bit meaningful-bits length (1..32, stored as 0..31)
			 */
			bitwriter_put(&bw, (uint64)(meaningful_bits - 1), 5);

			/* Emit: the meaningful bits themselves */
			bitwriter_put(&bw, (uint64) meaningful_val, meaningful_bits);
		}

		prev_bits = cur_bits;
	}

	packed_bytes = bitwriter_bytes_written(&bw);
	hdr->chimp_packed_bytes = packed_bytes;

	return hdr_size + packed_bytes;
}

/*
 * chimp_decode
 *		Decode Chimp-compressed float4 values.
 */
void
chimp_decode(const char *src, int src_size,
			 Datum *datums, bool *isnulls, int num_elements)
{
	ChimpBlockHeader hdr;
	BitReader	br;
	uint32		prev_bits;
	int			val_idx = 0;
	int			decoded = 0;
	int			nonnull_target;

	Assert(src_size >= (int) sizeof(ChimpBlockHeader));

	memcpy(&hdr, src, sizeof(ChimpBlockHeader));
	Assert(hdr.chimp_value_width == 4);

	nonnull_target = hdr.chimp_num_values;

	bitreader_init(&br, (const unsigned char *) src + sizeof(ChimpBlockHeader),
				   hdr.chimp_packed_bytes);

	prev_bits = hdr.chimp_first_value_lo;

	/* Distribute decoded values across the non-null positions */
	for (int i = 0; i < num_elements && decoded < nonnull_target; i++)
	{
		if (isnulls[i])
		{
			datums[i] = Float4GetDatum(0.0);
			continue;
		}

		if (val_idx == 0)
		{
			/* First value is stored verbatim in the header */
			datums[i] = Float4GetDatum(bits_to_float4(prev_bits));
			val_idx++;
			decoded++;
			continue;
		}

		/* Read flag bit */
		{
			uint64		flag = bitreader_get(&br, 1);

			if (flag == 0)
			{
				/* XOR == 0: same as previous */
				datums[i] = Float4GetDatum(bits_to_float4(prev_bits));
			}
			else
			{
				int			bucket_idx;
				int			bucket_lz;
				int			meaningful_bits;
				uint32		meaningful_val;
				uint32		xor_val;
				int			trailing;
				uint32		cur_bits;

				bucket_idx = (int) bitreader_get(&br, CHIMP_LZ_BITS_F32);
				bucket_lz = chimp_lz_buckets_f32[bucket_idx];

				meaningful_bits = (int) bitreader_get(&br, 5) + 1;

				meaningful_val = (uint32) bitreader_get(&br, meaningful_bits);

				trailing = 32 - bucket_lz - meaningful_bits;
				xor_val = meaningful_val << trailing;
				cur_bits = prev_bits ^ xor_val;

				datums[i] = Float4GetDatum(bits_to_float4(cur_bits));
				prev_bits = cur_bits;
			}
		}

		val_idx++;
		decoded++;
	}

	/* Fill remaining NULL slots if any */
	for (int i = 0; i < num_elements; i++)
	{
		if (isnulls[i])
			datums[i] = Float4GetDatum(0.0);
	}
}


/* ----------
 * Float8 (64-bit) encoding/decoding
 * ----------
 */

static inline uint64
float8_to_bits(double d)
{
	uint64		bits;

	memcpy(&bits, &d, sizeof(uint64));
	return bits;
}

static inline double
bits_to_float8(uint64 bits)
{
	double		d;

	memcpy(&d, &bits, sizeof(double));
	return d;
}

/*
 * chimp_encode_float8
 *		Encode float8 values using Chimp XOR compression.
 */
int
chimp_encode_float8(Datum *datums, bool *isnulls,
					int num_elements, char *dst, int dst_capacity)
{
	ChimpBlockHeader *hdr;
	BitWriter	bw;
	int			num_nonnull = 0;
	uint64		prev_bits;
	int			hdr_size = sizeof(ChimpBlockHeader);
	int			packed_bytes;
	bool		first = true;

	for (int i = 0; i < num_elements; i++)
		if (!isnulls[i])
			num_nonnull++;

	if (num_nonnull < 2)
		return 0;

	if (dst_capacity < hdr_size + 8)
		return 0;

	hdr = (ChimpBlockHeader *) dst;
	hdr->chimp_value_width = 8;
	hdr->chimp_leading_bits = CHIMP_LZ_BITS_F64;
	hdr->chimp_num_values = num_nonnull;

	bitwriter_init(&bw, (unsigned char *) dst + hdr_size,
				   dst_capacity - hdr_size);

	prev_bits = 0;

	for (int i = 0; i < num_elements; i++)
	{
		uint64		cur_bits;
		uint64		xor_val;

		if (isnulls[i])
			continue;

		cur_bits = float8_to_bits(DatumGetFloat8(datums[i]));

		if (first)
		{
			hdr->chimp_first_value_lo = (uint32)(cur_bits & 0xFFFFFFFF);
			hdr->chimp_first_value_hi = (uint32)(cur_bits >> 32);
			prev_bits = cur_bits;
			first = false;
			continue;
		}

		xor_val = cur_bits ^ prev_bits;

		if (xor_val == 0)
		{
			bitwriter_put(&bw, 0, 1);
		}
		else
		{
			int			leading = clz64(xor_val);
			int			trailing = ctz64(xor_val);
			int			meaningful_bits;
			int			bucket_idx;
			int			bucket_lz;
			uint64		meaningful_val;

			bucket_idx = chimp_lz_to_bucket(leading,
											chimp_lz_buckets_f64,
											CHIMP_LZ_NBUCKETS_F64);
			bucket_lz = chimp_lz_buckets_f64[bucket_idx];

			meaningful_bits = 64 - bucket_lz - trailing;
			meaningful_val = (xor_val >> trailing) & ((meaningful_bits == 64) ?
													  UINT64_MAX :
													  ((1ULL << meaningful_bits) - 1));

			/* 1-bit flag */
			bitwriter_put(&bw, 1, 1);

			/* Leading-zero bucket (4 bits) */
			bitwriter_put(&bw, (uint64) bucket_idx, CHIMP_LZ_BITS_F64);

			/* Meaningful-bits length: 6 bits (1..64, stored as 0..63) */
			bitwriter_put(&bw, (uint64)(meaningful_bits - 1), 6);

			/* The meaningful bits */
			bitwriter_put(&bw, meaningful_val, meaningful_bits);
		}

		prev_bits = cur_bits;
	}

	packed_bytes = bitwriter_bytes_written(&bw);
	hdr->chimp_packed_bytes = packed_bytes;

	return hdr_size + packed_bytes;
}

/*
 * chimp_decode_float8
 *		Decode Chimp-compressed float8 values.
 */
void
chimp_decode_float8(const char *src, int src_size,
					Datum *datums, bool *isnulls, int num_elements)
{
	ChimpBlockHeader hdr;
	BitReader	br;
	uint64		prev_bits;
	int			val_idx = 0;
	int			decoded = 0;
	int			nonnull_target;

	Assert(src_size >= (int) sizeof(ChimpBlockHeader));

	memcpy(&hdr, src, sizeof(ChimpBlockHeader));
	Assert(hdr.chimp_value_width == 8);

	nonnull_target = hdr.chimp_num_values;

	bitreader_init(&br, (const unsigned char *) src + sizeof(ChimpBlockHeader),
				   hdr.chimp_packed_bytes);

	prev_bits = ((uint64) hdr.chimp_first_value_hi << 32) |
		(uint64) hdr.chimp_first_value_lo;

	for (int i = 0; i < num_elements && decoded < nonnull_target; i++)
	{
		if (isnulls[i])
		{
			datums[i] = Float8GetDatum(0.0);
			continue;
		}

		if (val_idx == 0)
		{
			datums[i] = Float8GetDatum(bits_to_float8(prev_bits));
			val_idx++;
			decoded++;
			continue;
		}

		{
			uint64		flag = bitreader_get(&br, 1);

			if (flag == 0)
			{
				datums[i] = Float8GetDatum(bits_to_float8(prev_bits));
			}
			else
			{
				int			bucket_idx;
				int			bucket_lz;
				int			meaningful_bits;
				uint64		meaningful_val;
				uint64		xor_val;
				int			trailing;
				uint64		cur_bits;

				bucket_idx = (int) bitreader_get(&br, CHIMP_LZ_BITS_F64);
				bucket_lz = chimp_lz_buckets_f64[bucket_idx];

				meaningful_bits = (int) bitreader_get(&br, 6) + 1;

				meaningful_val = bitreader_get(&br, meaningful_bits);

				trailing = 64 - bucket_lz - meaningful_bits;
				xor_val = meaningful_val << trailing;
				cur_bits = prev_bits ^ xor_val;

				datums[i] = Float8GetDatum(bits_to_float8(cur_bits));
				prev_bits = cur_bits;
			}
		}

		val_idx++;
		decoded++;
	}

	for (int i = 0; i < num_elements; i++)
	{
		if (isnulls[i])
			datums[i] = Float8GetDatum(0.0);
	}
}


/* ----------
 * Detection / sizing helper
 * ----------
 */

/*
 * chimp_should_encode
 *		Decide whether Chimp encoding is beneficial.
 *
 * Estimates the compressed size analytically by computing XOR values
 * and counting the bits that would be needed, without actually writing
 * to a buffer.  This avoids the overhead of a full trial encode
 * (palloc, memset, bit-stream writes, pfree).
 */
bool
chimp_should_encode(Form_pg_attribute att,
					Datum *datums, bool *isnulls,
					int num_elements, int raw_datasz,
					int *chimp_datasz_p)
{
	int			num_nonnull = 0;
	int			total_bits = 0;
	int			encoded_size;
	bool		is_float8;
	const uint8 *lz_buckets;
	int			lz_nbuckets;
	int			lz_bits;
	int			mbits_width;

	/* Chimp only applies to float4 and float8 */
	if (att->atttypid != FLOAT4OID && att->atttypid != FLOAT8OID)
		return false;

	is_float8 = (att->atttypid == FLOAT8OID);
	lz_buckets = is_float8 ? chimp_lz_buckets_f64 : chimp_lz_buckets_f32;
	lz_nbuckets = is_float8 ? CHIMP_LZ_NBUCKETS_F64 : CHIMP_LZ_NBUCKETS_F32;
	lz_bits = is_float8 ? CHIMP_LZ_BITS_F64 : CHIMP_LZ_BITS_F32;
	mbits_width = is_float8 ? 6 : 5;	/* bits to encode meaningful-bits length */

	/* Need at least 2 non-null values */
	for (int i = 0; i < num_elements; i++)
		if (!isnulls[i])
			num_nonnull++;

	if (num_nonnull < 2)
		return false;

	/*
	 * Estimate compressed size by walking the XOR chain.
	 * For each pair, compute the XOR and count the bits that the
	 * encoder would emit, without actually writing them.
	 */
	if (is_float8)
	{
		uint64		prev_bits = 0;
		bool		first = true;

		for (int i = 0; i < num_elements; i++)
		{
			uint64		cur_bits;
			uint64		xor_val;

			if (isnulls[i])
				continue;

			cur_bits = float8_to_bits(DatumGetFloat8(datums[i]));

			if (first)
			{
				prev_bits = cur_bits;
				first = false;
				continue;
			}

			xor_val = cur_bits ^ prev_bits;

			if (xor_val == 0)
			{
				total_bits += 1;	/* single 0-bit */
			}
			else
			{
				int		leading = clz64(xor_val);
				int		trailing = ctz64(xor_val);
				int		bucket_idx = chimp_lz_to_bucket(leading, lz_buckets,
														lz_nbuckets);
				int		bucket_lz = lz_buckets[bucket_idx];
				int		meaningful_bits = 64 - bucket_lz - trailing;

				/* 1-bit flag + bucket index + mbits length + meaningful bits */
				total_bits += 1 + lz_bits + mbits_width + meaningful_bits;
			}

			prev_bits = cur_bits;
		}
	}
	else
	{
		uint32		prev_bits = 0;
		bool		first = true;

		for (int i = 0; i < num_elements; i++)
		{
			uint32		cur_bits;
			uint32		xor_val;

			if (isnulls[i])
				continue;

			cur_bits = float4_to_bits(DatumGetFloat4(datums[i]));

			if (first)
			{
				prev_bits = cur_bits;
				first = false;
				continue;
			}

			xor_val = cur_bits ^ prev_bits;

			if (xor_val == 0)
			{
				total_bits += 1;
			}
			else
			{
				int		leading = clz32(xor_val);
				int		trailing = ctz32(xor_val);
				int		bucket_idx = chimp_lz_to_bucket(leading, lz_buckets,
														lz_nbuckets);
				int		bucket_lz = lz_buckets[bucket_idx];
				int		meaningful_bits = 32 - bucket_lz - trailing;

				total_bits += 1 + lz_bits + mbits_width + meaningful_bits;
			}

			prev_bits = cur_bits;
		}
	}

	encoded_size = (int) sizeof(ChimpBlockHeader) + (total_bits + 7) / 8;

	if (encoded_size <= 0)
		return false;

	/* Only use Chimp if we save at least 25% compared to raw */
	if (encoded_size >= raw_datasz * 3 / 4)
		return false;

	*chimp_datasz_p = encoded_size;
	return true;
}
