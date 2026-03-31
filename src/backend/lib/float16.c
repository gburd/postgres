/*
 * float16.c
 *		Bfloat16 scalar quantization for vector compression.
 *
 * Implements bfloat16 (brain floating-point) conversion: float32 to/from
 * a 16-bit format that keeps the IEEE 754 sign + exponent + top 7 mantissa
 * bits.  This gives 50% size reduction while preserving the full float32
 * dynamic range.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/lib/float16.c
 */
#include "postgres.h"

#include <math.h>
#include <string.h>

#include "lib/float16.h"

/*
 * float32_to_bfloat16 -- convert float32 to bfloat16
 *
 * Truncates the lower 16 bits of the float32 mantissa.  This is a simple
 * right-shift of the IEEE 754 bit pattern.  NaN and Inf are preserved.
 */
bfloat16
float32_to_bfloat16(float value)
{
	uint32		bits;

	memcpy(&bits, &value, sizeof(uint32));

	/* Simply take the upper 16 bits (truncation) */
	return (bfloat16) (bits >> 16);
}

/*
 * bfloat16_to_float32 -- convert bfloat16 back to float32
 *
 * Pads the lower 16 mantissa bits with zeros.
 */
float
bfloat16_to_float32(bfloat16 bf)
{
	uint32		bits;
	float		result;

	bits = ((uint32) bf) << 16;
	memcpy(&result, &bits, sizeof(float));

	return result;
}

/*
 * quantize_float_array -- batch convert float32[] to bfloat16[]
 *
 * Converts num_values floats from src into dst.  Returns num_values.
 */
int
quantize_float_array(const float *src, int num_values, bfloat16 *dst)
{
	for (int i = 0; i < num_values; i++)
		dst[i] = float32_to_bfloat16(src[i]);

	return num_values;
}

/*
 * dequantize_float_array -- batch convert bfloat16[] to float32[]
 *
 * Converts num_values bfloat16s from src into dst.  Returns num_values.
 */
int
dequantize_float_array(const bfloat16 *src, int num_values, float *dst)
{
	for (int i = 0; i < num_values; i++)
		dst[i] = bfloat16_to_float32(src[i]);

	return num_values;
}

/*
 * float_array_should_quantize -- check if an array is a good quantization candidate
 *
 * Returns false if:
 *   - fewer than 4 values (not worth the overhead)
 *   - all values are zero
 *   - any value is NaN or Inf (quantization would preserve these but they
 *     indicate non-embedding data)
 */
bool
float_array_should_quantize(const float *values, int num_values)
{
	bool		all_zero = true;

	if (num_values < 4)
		return false;

	for (int i = 0; i < num_values; i++)
	{
		if (isnan(values[i]) || isinf(values[i]))
			return false;

		if (values[i] != 0.0f)
			all_zero = false;
	}

	return !all_zero;
}
