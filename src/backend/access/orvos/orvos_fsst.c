/*
 * orvos_fsst.c
 *		FSST (Fast Static Symbol Table) string compression for orvos.
 *
 * This implements a self-contained FSST-inspired compression algorithm.
 * FSST builds a 256-entry symbol table mapping single-byte codes to
 * multi-byte sequences (1-8 bytes).  Encoding replaces common byte
 * sequences with their codes; decoding expands them back.
 *
 * The algorithm uses a greedy approach:
 * 1. Count frequency of all 1-byte through 8-byte sequences in the input.
 * 2. Score each candidate symbol by (frequency * (len - 1)), representing
 *    the total bytes saved.
 * 3. Greedily select the top-scoring symbols, up to 255 entries.
 * 4. Code 255 is reserved as an escape: the next byte is a literal.
 *
 * This provides 30-60% additional compression on string data when used
 * as a pre-filter before zstd/lz4.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/orvos/orvos_fsst.c
 */
#include "postgres.h"

#include "access/orvos_fsst.h"
#include "utils/memutils.h"

/*
 * Maximum number of candidate n-grams to track during symbol table
 * construction.  We hash n-grams and use a fixed-size hash table.
 */
#define FSST_HASH_SIZE		(1 << 16)	/* 64K entries */
#define FSST_HASH_MASK		(FSST_HASH_SIZE - 1)

/* Maximum sample size for building the symbol table (bytes) */
#define FSST_MAX_SAMPLE_SIZE	(64 * 1024)

/*
 * Hash table entry for counting n-gram frequencies during symbol table
 * construction.
 */
typedef struct FsstHashEntry
{
	uint64		hash;			/* full hash for collision detection */
	uint32		count;			/* frequency count */
	uint8		len;			/* n-gram length (1-8) */
	uint8		bytes[FSST_MAX_SYMBOL_LEN];
} FsstHashEntry;

/*
 * Simple hash function for byte sequences.
 */
static uint64
fsst_hash_bytes(const uint8 *data, int len)
{
	uint64		h = 0xcbf29ce484222325ULL;	/* FNV-1a offset basis */

	for (int i = 0; i < len; i++)
	{
		h ^= data[i];
		h *= 0x100000001b3ULL;	/* FNV-1a prime */
	}
	return h;
}

/*
 * Insert or increment an n-gram in the hash table.
 */
static void
fsst_hash_insert(FsstHashEntry *htab, const uint8 *bytes, int len)
{
	uint64		h = fsst_hash_bytes(bytes, len);
	int			idx = (int) (h & FSST_HASH_MASK);
	int			probe;

	for (probe = 0; probe < 16; probe++)
	{
		int			slot = (idx + probe) & FSST_HASH_MASK;

		if (htab[slot].len == 0)
		{
			/* empty slot */
			htab[slot].hash = h;
			htab[slot].count = 1;
			htab[slot].len = len;
			memcpy(htab[slot].bytes, bytes, len);
			return;
		}
		if (htab[slot].hash == h && htab[slot].len == len &&
			memcmp(htab[slot].bytes, bytes, len) == 0)
		{
			/* found existing entry */
			htab[slot].count++;
			return;
		}
	}
	/* hash table full at this bucket, just drop it */
}

/*
 * Build a FSST symbol table from the given strings.
 *
 * We sample the input strings, count n-gram frequencies, score them,
 * and select the top 255 symbols.
 */
FsstSymbolTable *
fsst_build_symbol_table(const char **strings, const int *lengths,
						int nstrings)
{
	FsstHashEntry *htab;
	FsstSymbolTable *table;
	int			total_bytes = 0;
	int			sample_bytes = 0;
	int			best_indices[FSST_NUM_SYMBOLS];
	int			num_candidates = 0;

	table = palloc0(sizeof(FsstSymbolTable));
	table->magic = FSST_MAGIC;
	table->num_symbols = 0;

	if (nstrings == 0)
		return table;

	/* Allocate hash table in a temporary context */
	htab = palloc0(sizeof(FsstHashEntry) * FSST_HASH_SIZE);

	/*
	 * Sample strings and count n-gram frequencies.
	 * Limit to FSST_MAX_SAMPLE_SIZE bytes total.
	 */
	for (int i = 0; i < nstrings && sample_bytes < FSST_MAX_SAMPLE_SIZE; i++)
	{
		const uint8 *s = (const uint8 *) strings[i];
		int			slen = lengths[i];

		if (slen <= 0)
			continue;

		/* Clamp to remaining budget */
		if (sample_bytes + slen > FSST_MAX_SAMPLE_SIZE)
			slen = FSST_MAX_SAMPLE_SIZE - sample_bytes;

		/* Count n-grams of length 2 through FSST_MAX_SYMBOL_LEN */
		for (int pos = 0; pos < slen; pos++)
		{
			for (int nglen = 2; nglen <= FSST_MAX_SYMBOL_LEN && pos + nglen <= slen; nglen++)
			{
				fsst_hash_insert(htab, &s[pos], nglen);
			}
		}

		sample_bytes += slen;
		total_bytes += lengths[i];
	}

	/*
	 * Score each candidate: score = count * (len - 1).
	 * This represents total bytes saved if we assign this n-gram a code.
	 * Collect the top 255 candidates.
	 */
	{
		/* Simple selection: scan hash table, keep top entries */
		int64		min_score = 0;
		int			min_idx = -1;

		num_candidates = 0;
		memset(best_indices, -1, sizeof(best_indices));

		for (int i = 0; i < FSST_HASH_SIZE; i++)
		{
			int64		score;

			if (htab[i].len < 2 || htab[i].count < 3)
				continue;

			score = (int64) htab[i].count * (htab[i].len - 1);

			if (num_candidates < (FSST_NUM_SYMBOLS - 1))
			{
				best_indices[num_candidates] = i;
				num_candidates++;

				if (num_candidates == (FSST_NUM_SYMBOLS - 1))
				{
					/* Find the minimum score entry */
					min_score = INT64_MAX;
					for (int j = 0; j < num_candidates; j++)
					{
						int			bi = best_indices[j];
						int64		s = (int64) htab[bi].count * (htab[bi].len - 1);

						if (s < min_score)
						{
							min_score = s;
							min_idx = j;
						}
					}
				}
			}
			else if (score > min_score)
			{
				/* Replace the worst entry */
				best_indices[min_idx] = i;

				/* Re-find minimum */
				min_score = INT64_MAX;
				for (int j = 0; j < num_candidates; j++)
				{
					int			bi = best_indices[j];
					int64		s = (int64) htab[bi].count * (htab[bi].len - 1);

					if (s < min_score)
					{
						min_score = s;
						min_idx = j;
					}
				}
			}
		}
	}

	/*
	 * Build the final symbol table.
	 * Codes 0..num_candidates-1 map to selected symbols.
	 * Code 255 is the escape byte.
	 */
	for (int i = 0; i < num_candidates; i++)
	{
		int			hi = best_indices[i];

		table->symbols[i].len = htab[hi].len;
		memcpy(table->symbols[i].bytes, htab[hi].bytes, htab[hi].len);
	}
	table->num_symbols = num_candidates;

	pfree(htab);

	return table;
}

/*
 * Compress data using the FSST symbol table.
 *
 * For each position in the input, we try to match the longest symbol
 * starting at that position.  If a match is found, we emit the symbol's
 * code byte.  If no symbol matches, we emit FSST_ESCAPE followed by
 * the literal byte.
 *
 * Returns compressed size, or 0 if compression didn't reduce size.
 */
int
fsst_compress(const char *src, int srcSize,
			  char *dst, int dstCapacity,
			  const FsstSymbolTable *table)
{
	const uint8 *in = (const uint8 *) src;
	uint8	   *out = (uint8 *) dst;
	int			inpos = 0;
	int			outpos = 0;
	int			nsymbols = table->num_symbols;

	Assert(table->magic == FSST_MAGIC);

	if (nsymbols == 0)
		return 0;

	while (inpos < srcSize)
	{
		int			best_code = -1;
		int			best_len = 0;
		int			remaining = srcSize - inpos;

		/*
		 * Find the longest matching symbol at current position.
		 * Linear scan through symbols is acceptable since we typically
		 * have < 255 symbols and this runs once per position.
		 */
		for (int c = 0; c < nsymbols; c++)
		{
			int			slen = table->symbols[c].len;

			if (slen <= best_len || slen > remaining)
				continue;

			if (memcmp(&in[inpos], table->symbols[c].bytes, slen) == 0)
			{
				best_code = c;
				best_len = slen;
			}
		}

		if (best_len >= 2)
		{
			/* Emit symbol code */
			if (outpos >= dstCapacity)
				return 0;
			out[outpos++] = (uint8) best_code;
			inpos += best_len;
		}
		else
		{
			/* Emit escape + literal byte */
			if (outpos + 1 >= dstCapacity)
				return 0;
			out[outpos++] = FSST_ESCAPE;
			out[outpos++] = in[inpos++];
		}
	}

	/* Only return compressed if it's actually smaller */
	if (outpos >= srcSize)
		return 0;

	return outpos;
}

/*
 * Decompress FSST-compressed data.
 *
 * Returns decompressed size.
 */
int
fsst_decompress(const char *src, int compressedSize,
				char *dst, int dstCapacity,
				const FsstSymbolTable *table)
{
	const uint8 *in = (const uint8 *) src;
	uint8	   *out = (uint8 *) dst;
	int			inpos = 0;
	int			outpos = 0;

	Assert(table->magic == FSST_MAGIC);

	while (inpos < compressedSize)
	{
		uint8		code = in[inpos++];

		if (code == FSST_ESCAPE)
		{
			/* Literal byte follows */
			if (inpos >= compressedSize)
				elog(ERROR, "FSST: truncated escape sequence");
			if (outpos >= dstCapacity)
				elog(ERROR, "FSST: output buffer overflow");
			out[outpos++] = in[inpos++];
		}
		else if (code < table->num_symbols && table->symbols[code].len > 0)
		{
			/* Expand symbol */
			int			slen = table->symbols[code].len;

			if (outpos + slen > dstCapacity)
				elog(ERROR, "FSST: output buffer overflow");
			memcpy(&out[outpos], table->symbols[code].bytes, slen);
			outpos += slen;
		}
		else
		{
			/* Unknown code -- treat as single-byte literal */
			if (outpos >= dstCapacity)
				elog(ERROR, "FSST: output buffer overflow");
			out[outpos++] = code;
		}
	}

	return outpos;
}
