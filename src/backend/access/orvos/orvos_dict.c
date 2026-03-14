/*
 * orvos_dict.c
 *		Dictionary encoding for low-cardinality columns in Orvos tables
 *
 * Dictionary encoding replaces repeated values with small integer indices
 * into a table of distinct values. This is highly effective for columns
 * with low cardinality (few distinct values relative to row count), such
 * as status fields, country codes, boolean-like text columns, etc.
 *
 * The encoding stores a dictionary (list of distinct values) followed by
 * an array of uint16 indices, one per element. For a column with N rows
 * and D distinct values, this uses roughly D * avg_value_size + N * 2
 * bytes, compared to N * avg_value_size without encoding.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/orvos/orvos_dict.c
 */
#include "postgres.h"

#include "access/orvos_dict.h"
#include "access/orvos_internal.h"
#include "utils/datum.h"
#include "common/hashfn.h"
#include "utils/memutils.h"

/*
 * Internal hash entry used during encoding. We use a simplistic approach:
 * hash on the raw bytes of the datum value.
 */
typedef struct DictBuildEntry
{
	uint32		hash;			/* hash of the value bytes */
	uint16		index;			/* dictionary index */
	int			size;			/* size of the value in bytes */
	char	   *value;			/* pointer to the value bytes */
	struct DictBuildEntry *next; /* chain for collision resolution */
} DictBuildEntry;

#define DICT_HASH_SIZE 256

typedef struct DictBuildState
{
	DictBuildEntry *buckets[DICT_HASH_SIZE];
	int			num_entries;
	int			total_data_size;

	/* Ordered list of entries for output */
	DictBuildEntry **entries;
	int			entries_allocated;
} DictBuildState;

/*
 * Get the raw bytes and size of a datum value for hashing/comparison.
 */
static void
get_datum_bytes(Form_pg_attribute att, Datum datum,
				const char **bytes, int *size)
{
	if (att->attlen > 0)
	{
		if (att->attbyval)
		{
			*bytes = (const char *) &datum;
			*size = att->attlen;
		}
		else
		{
			*bytes = (const char *) DatumGetPointer(datum);
			*size = att->attlen;
		}
	}
	else if (att->attlen == -1)
	{
		struct varlena *vl = (struct varlena *) DatumGetPointer(datum);

		if (VARATT_IS_EXTERNAL(vl) && VARTAG_EXTERNAL(vl) == VARTAG_ORVOS)
		{
			/* orvos toast pointer - use the raw bytes */
			*bytes = (const char *) vl;
			*size = (int) sizeof(varatt_ov_toastptr);
		}
		else
		{
			*bytes = VARDATA_ANY(vl);
			*size = (int) VARSIZE_ANY_EXHDR(vl);
		}
	}
	else
	{
		Assert(att->attlen == -2);
		*bytes = (const char *) DatumGetPointer(datum);
		*size = (int) strlen(*bytes);
	}
}

/*
 * Simple hash function for datum bytes.
 */
static uint32
hash_datum_bytes(const char *bytes, int size)
{
	return hash_bytes((const unsigned char *) bytes, size);
}

/*
 * Look up or insert a value in the build state.
 * Returns the dictionary index, or -1 if the dictionary is full.
 */
static int
dict_build_lookup_or_insert(DictBuildState *state,
							const char *bytes, int size,
							uint32 hash_val)
{
	int			bucket = hash_val % DICT_HASH_SIZE;
	DictBuildEntry *entry;

	/* Search existing entries */
	for (entry = state->buckets[bucket]; entry != NULL; entry = entry->next)
	{
		if (entry->hash == hash_val &&
			entry->size == size &&
			memcmp(entry->value, bytes, size) == 0)
		{
			return entry->index;
		}
	}

	/* Not found - insert new entry */
	if (state->num_entries >= OV_DICT_MAX_ENTRIES)
		return -1;

	if (state->total_data_size + size > OV_DICT_MAX_TOTAL_SIZE)
		return -1;

	/* Grow entries array if needed */
	if (state->num_entries >= state->entries_allocated)
	{
		int			new_alloc = state->entries_allocated * 2;

		if (new_alloc < 64)
			new_alloc = 64;

		state->entries = repalloc(state->entries,
								 new_alloc * sizeof(DictBuildEntry *));
		state->entries_allocated = new_alloc;
	}

	entry = palloc(sizeof(DictBuildEntry));
	entry->hash = hash_val;
	entry->index = (uint16) state->num_entries;
	entry->size = size;
	entry->value = palloc(size);
	memcpy(entry->value, bytes, size);
	entry->next = state->buckets[bucket];
	state->buckets[bucket] = entry;

	state->entries[state->num_entries] = entry;
	state->num_entries++;
	state->total_data_size += size;

	return entry->index;
}

/*
 * Check whether dictionary encoding would be beneficial for a set of datums.
 *
 * Returns true if the number of distinct values is low relative to
 * the total number of items, and the estimated encoded size would be
 * smaller than the raw data.
 */
bool
ov_dict_should_encode(Form_pg_attribute att,
					  Datum *datums, bool *isnulls,
					  int nitems)
{
	DictBuildState state;
	int			i;
	int			raw_data_size = 0;
	int			dict_data_size;
	int			encoded_indices_size;

	/* Need at least a few items to be worth it */
	if (nitems < 16)
		return false;

	/* For fixed-width byval types smaller than 2 bytes, not worth it */
	if (att->attbyval && att->attlen <= 2)
		return false;

	memset(&state, 0, sizeof(state));
	state.entries = palloc(64 * sizeof(DictBuildEntry *));
	state.entries_allocated = 64;

	for (i = 0; i < nitems; i++)
	{
		const char *bytes;
		int			size;
		uint32		hash_val;
		int			idx;

		if (isnulls[i])
			continue;

		get_datum_bytes(att, datums[i], &bytes, &size);
		raw_data_size += size;

		hash_val = hash_datum_bytes(bytes, size);
		idx = dict_build_lookup_or_insert(&state, bytes, size, hash_val);

		if (idx < 0)
		{
			/* Too many distinct values, bail out */
			pfree(state.entries);
			return false;
		}
	}

	/* Check cardinality threshold */
	if (nitems > 0 &&
		(double) state.num_entries / (double) nitems >= OV_DICT_CARDINALITY_THRESHOLD &&
		state.num_entries > 4)
	{
		pfree(state.entries);
		return false;
	}

	/* Check if encoding would actually save space */
	dict_data_size = sizeof(OVDictHeader) +
		state.num_entries * sizeof(uint32) +
		state.total_data_size;
	encoded_indices_size = nitems * sizeof(uint16);

	if (dict_data_size + encoded_indices_size >= raw_data_size)
	{
		pfree(state.entries);
		return false;
	}

	/* Clean up */
	for (i = 0; i < DICT_HASH_SIZE; i++)
	{
		DictBuildEntry *entry = state.buckets[i];

		while (entry != NULL)
		{
			DictBuildEntry *next = entry->next;

			pfree(entry->value);
			pfree(entry);
			entry = next;
		}
	}
	pfree(state.entries);

	return true;
}

/*
 * Encode an array of datums using dictionary encoding.
 *
 * Returns a palloc'd buffer containing:
 *   [OVDictHeader] [offsets: uint32 * num_entries] [values data] [indices: uint16 * nitems]
 *
 * Sets *encoded_size to the total size of the buffer.
 */
char *
ov_dict_encode(Form_pg_attribute att,
			   Datum *datums, bool *isnulls,
			   int nitems, int *encoded_size)
{
	DictBuildState state;
	uint16	   *indices;
	int			i;
	OVDictHeader *hdr;
	uint32	   *offsets;
	char	   *values_data;
	char	   *result;
	int			result_size;
	char	   *p;
	uint32		cur_offset;
	bool		fixed_size = true;
	int			first_size = -1;

	memset(&state, 0, sizeof(state));
	state.entries = palloc(64 * sizeof(DictBuildEntry *));
	state.entries_allocated = 64;

	/* First pass: build dictionary and collect indices */
	indices = palloc(nitems * sizeof(uint16));

	for (i = 0; i < nitems; i++)
	{
		const char *bytes;
		int			size;
		uint32		hash_val;
		int			idx;

		if (isnulls[i])
		{
			indices[i] = OV_DICT_NULL_INDEX;
			continue;
		}

		get_datum_bytes(att, datums[i], &bytes, &size);
		hash_val = hash_datum_bytes(bytes, size);
		idx = dict_build_lookup_or_insert(&state, bytes, size, hash_val);

		Assert(idx >= 0);		/* caller should have checked with
								 * ov_dict_should_encode */
		indices[i] = (uint16) idx;

		/* Track if all entries are the same size */
		if (first_size < 0)
			first_size = size;
		else if (size != first_size)
			fixed_size = false;
	}

	/* Compute result size */
	result_size = sizeof(OVDictHeader);
	result_size += state.num_entries * sizeof(uint32);	/* offsets */
	result_size += state.total_data_size;	/* values */
	result_size += nitems * sizeof(uint16); /* indices */

	result = palloc(result_size);
	p = result;

	/* Write header */
	hdr = (OVDictHeader *) p;
	hdr->num_entries = (uint16) state.num_entries;
	hdr->entry_size = (uint16) ((fixed_size && first_size >= 0) ? first_size : 0);
	hdr->total_data_size = state.total_data_size;
	p += sizeof(OVDictHeader);

	/* Write offsets */
	offsets = (uint32 *) p;
	cur_offset = 0;
	for (i = 0; i < state.num_entries; i++)
	{
		offsets[i] = cur_offset;
		cur_offset += state.entries[i]->size;
	}
	p += state.num_entries * sizeof(uint32);

	/* Write values data */
	values_data = p;
	for (i = 0; i < state.num_entries; i++)
	{
		memcpy(values_data + offsets[i],
			   state.entries[i]->value,
			   state.entries[i]->size);
	}
	p += state.total_data_size;

	/* Write indices */
	memcpy(p, indices, nitems * sizeof(uint16));
	p += nitems * sizeof(uint16);

	Assert(p - result == result_size);

	*encoded_size = result_size;

	/* Clean up */
	for (i = 0; i < DICT_HASH_SIZE; i++)
	{
		DictBuildEntry *entry = state.buckets[i];

		while (entry != NULL)
		{
			DictBuildEntry *next = entry->next;

			pfree(entry->value);
			pfree(entry);
			entry = next;
		}
	}
	pfree(state.entries);
	pfree(indices);

	return result;
}

/*
 * Decode dictionary-encoded data back into an array of Datums.
 *
 * Reads from src, which contains [OVDictHeader][offsets][values][indices].
 * Populates datums[] and isnulls[] with the decoded values.
 *
 * buf/buf_size: working buffer for reconstructing varlena values.
 * For fixed-length pass-by-ref or varlena types, decoded values point
 * into this buffer.
 *
 * Returns the number of bytes consumed from src.
 */
int
ov_dict_decode(Form_pg_attribute att,
			   const char *src, int src_size,
			   Datum *datums, bool *isnulls,
			   int nitems,
			   char *buf, int buf_size)
{
	const OVDictHeader *hdr;
	const uint32 *offsets;
	const char *values_data;
	const uint16 *indices;
	const char *p = src;
	int			i;
	char	   *bufp = buf;

	/* Read header */
	hdr = (const OVDictHeader *) p;
	p += sizeof(OVDictHeader);

	/* Read offsets */
	offsets = (const uint32 *) p;
	p += hdr->num_entries * sizeof(uint32);

	/* Read values data */
	values_data = p;
	p += hdr->total_data_size;

	/* Read indices */
	indices = (const uint16 *) p;
	p += nitems * sizeof(uint16);

	/* Decode each element */
	for (i = 0; i < nitems; i++)
	{
		uint16		idx = indices[i];

		if (idx == OV_DICT_NULL_INDEX)
		{
			isnulls[i] = true;
			datums[i] = (Datum) 0;
			continue;
		}

		isnulls[i] = false;
		Assert(idx < hdr->num_entries);

		if (att->attlen > 0 && att->attbyval)
		{
			/* Pass-by-value fixed length: reconstruct the Datum */
			const char *val = values_data + offsets[idx];
			Datum		d = 0;

			memcpy(&d, val, att->attlen);
			datums[i] = d;
		}
		else if (att->attlen > 0)
		{
			/* Pass-by-reference fixed length */
			const char *val = values_data + offsets[idx];

			memcpy(bufp, val, att->attlen);
			datums[i] = PointerGetDatum(bufp);
			bufp += att->attlen;
		}
		else if (att->attlen == -1)
		{
			/* Varlena: reconstruct with a proper varlena header */
			const char *val = values_data + offsets[idx];
			int			val_size;

			if (idx + 1 < hdr->num_entries)
				val_size = (int) (offsets[idx + 1] - offsets[idx]);
			else
				val_size = (int) (hdr->total_data_size - offsets[idx]);

			if (att->attstorage != 'p' && val_size + 1 <= 127)
			{
				/* Use short varlena header (1 byte) */
				SET_VARSIZE_1B(bufp, 1 + val_size);
				memcpy(bufp + 1, val, val_size);
				datums[i] = PointerGetDatum(bufp);
				bufp += 1 + val_size;
			}
			else
			{
				/* Use standard 4-byte varlena header */
				bufp = (char *) att_align_nominal(bufp, 'i');
				SET_VARSIZE(bufp, VARHDRSZ + val_size);
				memcpy(VARDATA(bufp), val, val_size);
				datums[i] = PointerGetDatum(bufp);
				bufp += VARHDRSZ + val_size;
			}
		}
		else
		{
			/* cstring (attlen == -2) */
			const char *val = values_data + offsets[idx];
			int			val_size;

			if (idx + 1 < hdr->num_entries)
				val_size = (int) (offsets[idx + 1] - offsets[idx]);
			else
				val_size = (int) (hdr->total_data_size - offsets[idx]);

			memcpy(bufp, val, val_size);
			bufp[val_size] = '\0';
			datums[i] = PointerGetDatum(bufp);
			bufp += val_size + 1;
		}
	}

	return (int) (p - src);
}

/*
 * Compute the encoded size of dictionary data without actually encoding.
 * Returns -1 if dictionary encoding is not applicable.
 */
int
ov_dict_encoded_size(Form_pg_attribute att,
					 Datum *datums, bool *isnulls,
					 int nitems)
{
	DictBuildState state;
	int			i;
	int			result;

	memset(&state, 0, sizeof(state));
	state.entries = palloc(64 * sizeof(DictBuildEntry *));
	state.entries_allocated = 64;

	for (i = 0; i < nitems; i++)
	{
		const char *bytes;
		int			size;
		uint32		hash_val;
		int			idx;

		if (isnulls[i])
			continue;

		get_datum_bytes(att, datums[i], &bytes, &size);
		hash_val = hash_datum_bytes(bytes, size);
		idx = dict_build_lookup_or_insert(&state, bytes, size, hash_val);

		if (idx < 0)
		{
			pfree(state.entries);
			return -1;
		}
	}

	result = sizeof(OVDictHeader) +
		state.num_entries * sizeof(uint32) +
		state.total_data_size +
		nitems * sizeof(uint16);

	/* Clean up */
	for (i = 0; i < DICT_HASH_SIZE; i++)
	{
		DictBuildEntry *entry = state.buckets[i];

		while (entry != NULL)
		{
			DictBuildEntry *next = entry->next;

			pfree(entry->value);
			pfree(entry);
			entry = next;
		}
	}
	pfree(state.entries);

	return result;
}
