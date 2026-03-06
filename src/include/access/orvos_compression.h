/*
 * orvos_compression.h
 *		internal declarations for Orvos compression
 *
 * Copyright (c) 2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/include/access/orvos_compression.h
 */
#ifndef ORVOS_COMPRESSION_H
#define ORVOS_COMPRESSION_H

extern int	ov_try_compress(const char *src, char *dst, int srcSize, int dstCapacity);
extern void ov_decompress(const char *src, char *dst, int compressedSize, int uncompressedSize);

#endif							/* ORVOS_COMPRESSION_H */
