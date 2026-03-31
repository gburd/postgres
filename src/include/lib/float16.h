/*
 * float16.h
 *		Bfloat16 scalar quantization for vector compression.
 *
 * Bfloat16 (brain floating-point) truncates float32 to 16 bits by keeping
 * the sign bit, 8-bit exponent, and top 7 mantissa bits.  This preserves
 * the full float32 range while halving storage, at the cost of reduced
 * precision (roughly 2-3 decimal digits vs 6-7 for float32).
 *
 * For embedding vectors (typical ML float32 outputs), the precision loss
 * is negligible for similarity searches.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/include/lib/float16.h
 */
#ifndef FLOAT16_H
#define FLOAT16_H

#include "postgres.h"

/*
 * Bfloat16 is stored as uint16.  The layout matches the upper 16 bits
 * of IEEE 754 float32:
 *   bit 15:    sign
 *   bits 14-7: exponent (8 bits, same as float32)
 *   bits 6-0:  mantissa (top 7 of 23 float32 mantissa bits)
 */
typedef uint16 bfloat16;

/* Convert a single float32 to bfloat16 (truncation, no rounding) */
extern bfloat16 float32_to_bfloat16(float value);

/* Convert a single bfloat16 back to float32 */
extern float bfloat16_to_float32(bfloat16 bf);

/*
 * Batch-convert an array of float32 values to bfloat16.
 * Returns the number of bfloat16 values written (== num_values).
 */
extern int quantize_float_array(const float *src, int num_values,
								bfloat16 *dst);

/*
 * Batch-convert an array of bfloat16 values back to float32.
 * Returns the number of float32 values written (== num_values).
 */
extern int dequantize_float_array(const bfloat16 *src, int num_values,
								  float *dst);

/*
 * Check whether a float array is a good candidate for bfloat16 quantization.
 * Returns true if the values are in a range where bfloat16 truncation
 * preserves meaningful precision (i.e., not all zeros, not all NaN/Inf).
 */
extern bool float_array_should_quantize(const float *values, int num_values);

#endif							/* FLOAT16_H */
