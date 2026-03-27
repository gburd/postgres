/*-------------------------------------------------------------------------
 *
 * external_clob.c
 *	  Text-specific operations for the external CLOB data type
 *
 * This module provides SQL-callable functions that operate on CLOB
 * values with text semantics: character length, substring extraction,
 * concatenation, and encoding validation.  The underlying storage is
 * handled by the BLOB infrastructure in blob.c; this file adds the
 * text-aware layer on top.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/external_clob.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xact.h"
#include "fmgr.h"
#include "mb/pg_wchar.h"
#include "utils/blob.h"
#include "utils/builtins.h"
#include "varatt.h"

/* SQL-callable function declarations */
PG_FUNCTION_INFO_V1(clob_length);
PG_FUNCTION_INFO_V1(clob_octet_length);
PG_FUNCTION_INFO_V1(clob_substring);
PG_FUNCTION_INFO_V1(clob_concat);
PG_FUNCTION_INFO_V1(clob_like);
PG_FUNCTION_INFO_V1(clob_encoding);

/*
 * clob_length - Return the character length of a CLOB
 *
 * This reads the CLOB content and counts characters according to
 * the current server encoding.
 */
Datum
clob_length(PG_FUNCTION_ARGS)
{
	ExternalBlobRef *ref = (ExternalBlobRef *) PG_GETARG_POINTER(0);
	void	   *data;
	Size		byte_len;
	int			char_len;

	data = ExternalBlobRead(ref, &byte_len);

	char_len = pg_mbstrlen_with_len((const char *) data, byte_len);

	pfree(data);

	PG_RETURN_INT32(char_len);
}

/*
 * clob_octet_length - Return the byte length of a CLOB
 */
Datum
clob_octet_length(PG_FUNCTION_ARGS)
{
	ExternalBlobRef *ref = (ExternalBlobRef *) PG_GETARG_POINTER(0);

	PG_RETURN_INT64((int64) ref->size);
}

/*
 * clob_substring - Extract a substring from a CLOB
 *
 * Arguments: clob, start_position (1-based), length (in characters)
 * Returns: text
 */
Datum
clob_substring(PG_FUNCTION_ARGS)
{
	ExternalBlobRef *ref = (ExternalBlobRef *) PG_GETARG_POINTER(0);
	int32		start = PG_GETARG_INT32(1);
	int32		count = PG_GETARG_INT32(2);
	void	   *data;
	Size		byte_len;
	const char *p;
	const char *end;
	int			char_pos;
	const char *substr_start;
	int			substr_bytes;
	text	   *result;

	if (count < 0)
		ereport(ERROR,
				(errcode(ERRCODE_SUBSTRING_ERROR),
				 errmsg("negative substring length not allowed")));

	data = ExternalBlobRead(ref, &byte_len);
	p = (const char *) data;
	end = p + byte_len;

	/* Advance to start position (1-based) */
	if (start < 1)
		start = 1;

	for (char_pos = 1; char_pos < start && p < end; char_pos++)
		p += pg_mblen(p);

	substr_start = p;

	/* Count 'count' characters forward */
	for (char_pos = 0; char_pos < count && p < end; char_pos++)
		p += pg_mblen(p);

	substr_bytes = p - substr_start;

	result = (text *) palloc(substr_bytes + VARHDRSZ);
	SET_VARSIZE(result, substr_bytes + VARHDRSZ);
	memcpy(VARDATA(result), substr_start, substr_bytes);

	pfree(data);

	PG_RETURN_TEXT_P(result);
}

/*
 * clob_concat - Concatenate two CLOBs
 *
 * Returns a new CLOB containing the concatenation of both inputs.
 */
Datum
clob_concat(PG_FUNCTION_ARGS)
{
	ExternalBlobRef *ref1 = (ExternalBlobRef *) PG_GETARG_POINTER(0);
	ExternalBlobRef *ref2 = (ExternalBlobRef *) PG_GETARG_POINTER(1);
	void	   *data1;
	void	   *data2;
	Size		size1;
	Size		size2;
	void	   *combined;
	ExternalBlobRef *result;
	UndoRecPtr	undo_ptr;

	undo_ptr = GetCurrentTransactionUndoRecPtr();

	data1 = ExternalBlobRead(ref1, &size1);
	data2 = ExternalBlobRead(ref2, &size2);

	combined = palloc(size1 + size2);
	memcpy(combined, data1, size1);
	memcpy((char *) combined + size1, data2, size2);

	pfree(data1);
	pfree(data2);

	result = ExternalBlobCreate(combined, size1 + size2, true, undo_ptr);

	pfree(combined);

	PG_RETURN_POINTER(result);
}

/*
 * clob_like - Pattern match a CLOB against a LIKE pattern
 *
 * Reads the CLOB content, converts to text, and delegates to the
 * standard textlike function.
 */
Datum
clob_like(PG_FUNCTION_ARGS)
{
	ExternalBlobRef *ref = (ExternalBlobRef *) PG_GETARG_POINTER(0);
	text	   *pattern = PG_GETARG_TEXT_PP(1);
	void	   *data;
	Size		size;
	text	   *clob_text;
	Datum		result;

	data = ExternalBlobRead(ref, &size);

	clob_text = (text *) palloc(size + VARHDRSZ);
	SET_VARSIZE(clob_text, size + VARHDRSZ);
	memcpy(VARDATA(clob_text), data, size);
	pfree(data);

	result = DirectFunctionCall2(textlike,
								 PointerGetDatum(clob_text),
								 PointerGetDatum(pattern));
	pfree(clob_text);

	PG_RETURN_DATUM(result);
}

/*
 * clob_encoding - Return the encoding name for CLOB content
 *
 * CLOBs are always stored in the server encoding.  This function
 * returns the encoding name for informational purposes.
 */
Datum
clob_encoding(PG_FUNCTION_ARGS)
{
	/* CLOBs use the server encoding */
	const char *encoding_name = GetDatabaseEncodingName();

	PG_RETURN_TEXT_P(cstring_to_text(encoding_name));
}
