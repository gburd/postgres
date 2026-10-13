/*-------------------------------------------------------------------------
 *
 * prop_jsonapi.c
 *	  Property-based tests for src/common/jsonapi.c incremental JSON parser.
 *
 * The JSON parser has two front ends over one grammar: a one-shot recursive
 * descent parser (pg_parse_json) and an incremental, chunk-at-a-time parser
 * (pg_parse_json_incremental) used by COPY, the JSON datum input path, and
 * libpq.  They are supposed to accept and reject exactly the same inputs.
 * The incremental lexer carries a "partial token" across chunk boundaries, so
 * a token split across a chunk edge (a number cut as "4" then "-", a string
 * cut mid-escape, "tr" then "ue") is where cross-chunk reassembly bugs live.
 * Drawing a json-ish restricted alphabet and tiny chunk sizes aims straight
 * at those boundaries.
 *
 * Properties:
 *	- chunking_agrees_with_oneshot: one-shot and incremental parses of the
 *	  same bytes reach the same final outcome (both succeed, or both fail).
 *	- oneshot_no_crash: one-shot parsing of arbitrary bytes always returns a
 *	  JsonParseErrorType and never crashes.
 *	- incremental_chunk_size_invariant: two incremental parses of the same
 *	  bytes with different chunk sizes reach the same final outcome.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_jsonapi.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "jsonapi"
#include "pg_prop.h"

#include "common/jsonapi.h"
#include "lib/stringinfo.h"
#include "mb/pg_wchar.h"

/*
 * A json-ish alphabet: structural punctuation, digits, number syntax, and the
 * letters of true/false/null, plus a little whitespace.  Most draws from this
 * set are structurally interesting (partial numbers, half-finished keywords,
 * unbalanced brackets) rather than random noise that fails at the first byte.
 */
#define JSON_ALPHABET "{}[]\":,0123456789-+.eEtruefalsn \t"

/*
 * Format a possibly-non-printable, possibly-NUL-containing buffer into an
 * escaped, NUL-terminated C string for diagnostics.  Non-printable bytes
 * become \xNN.  The result is malloc'd; caller frees.
 */
static char *
escape_for_note(const unsigned char *buf, size_t len)
{
	char	   *out;
	size_t		i;
	size_t		o;

	/* worst case every byte expands to "\xNN" (4 chars), plus terminator */
	out = pg_malloc(len * 4 + 1);
	o = 0;
	for (i = 0; i < len; i++)
	{
		unsigned char c = buf[i];

		if (c >= 0x20 && c < 0x7f && c != '\\')
			out[o++] = (char) c;
		else
		{
			snprintf(out + o, 5, "\\x%02x", c);
			o += 4;
		}
	}
	out[o] = '\0';
	return out;
}

/*
 * Parse the whole buffer at once with the recursive-descent parser.  Uses a
 * heap-allocated lex (NULL first argument) so freeJsonLexContext() frees the
 * struct too.  Always drives the parser by explicit length, never strlen, so
 * interior NUL bytes in the input are passed through faithfully.
 */
static JsonParseErrorType
parse_oneshot(const char *buf, size_t len)
{
	JsonLexContext *lex;
	JsonParseErrorType result;

	lex = makeJsonLexContextCstringLen(NULL, buf, len, PG_UTF8, false);
	result = pg_parse_json(lex, &nullSemAction);
	freeJsonLexContext(lex);

	return result;
}

/*
 * Parse the whole buffer with the incremental parser, feeding it in slices of
 * at most chunk_size bytes.  is_last is set only on the final slice (including
 * the empty-input case, where a single is_last slice of length zero is fed).
 *
 * A non-final chunk must return JSON_INCOMPLETE; anything else from a non-final
 * chunk is already a definitive result (success is impossible mid-stream, and
 * any error is final), so we return it immediately.  The final chunk yields the
 * authoritative outcome.  We feed successive slices of the original buffer
 * directly, mirroring test_json_parser_incremental.c but without the staging
 * StringInfo (the parser only ever reads the length we pass).
 */
static JsonParseErrorType
parse_incremental(const char *buf, size_t len, int64 chunk_size)
{
	JsonLexContext *lex;
	JsonParseErrorType result;
	size_t		offset;

	lex = makeJsonLexContextIncremental(NULL, PG_UTF8, false);

	offset = 0;
	for (;;)
	{
		size_t		remaining = len - offset;
		size_t		this_chunk;
		bool		is_last;

		this_chunk = (remaining < (size_t) chunk_size) ? remaining : (size_t) chunk_size;
		is_last = (offset + this_chunk >= len);

		result = pg_parse_json_incremental(lex, &nullSemAction,
										   buf + offset, this_chunk,
										   is_last);

		if (is_last)
			break;

		/*
		 * A non-final chunk that is not merely incomplete has produced a
		 * definitive answer (an error that will not change by feeding more
		 * input).  Report it as the outcome.
		 */
		if (result != JSON_INCOMPLETE)
			break;

		offset += this_chunk;
	}

	freeJsonLexContext(lex);

	return result;
}

/*
 * The key property: the one-shot and incremental parsers must agree on whether
 * a given byte sequence is valid JSON.  We only compare success-vs-failure,
 * not the specific error code (the two front ends legitimately report
 * different error codes for the same bad input).
 */
PG_PROP(chunking_agrees_with_oneshot)
{
	size_t		len;
	char	   *s = pg_prop_draw_from(tc, JSON_ALPHABET, 0, 60, &len);
	int64		chunk_size = pg_prop_draw_int(tc, 1, 8);
	JsonParseErrorType one;
	JsonParseErrorType inc;

	one = parse_oneshot(s, len);
	inc = parse_incremental(s, len, chunk_size);

	if ((one == JSON_SUCCESS) != (inc == JSON_SUCCESS))
	{
		char	   *shown = escape_for_note((const unsigned char *) s, len);

		pg_prop_fail(tc,
					 "oneshot/incremental disagree: oneshot=%d incremental=%d "
					 "(chunk_size=%lld input=\"%s\")",
					 (int) one, (int) inc, (long long) chunk_size, shown);
	}

	pg_free(s);
}

/*
 * Robustness: the one-shot parser must return some JsonParseErrorType for any
 * byte sequence at all, never crash.  Because properties run in-process, a
 * crash would abort the whole run and surface as a hard failure -- which is
 * exactly the signal we want for a no-crash property; we do not fork here.  If
 * the call returns, the parser survived and the property passes.
 */
PG_PROP(oneshot_no_crash)
{
	size_t		len;
	unsigned char *buf = pg_prop_draw_bytes(tc, 0, 256, &len);

	/* parse by explicit length; interior NULs are fine for the parser */
	(void) parse_oneshot((const char *) buf, len);

	pg_free(buf);
}

/*
 * Chunk-boundary invariant: parsing the same bytes incrementally with two
 * different small chunk sizes must reach the same final outcome.  This
 * isolates cross-chunk reassembly bugs from any one-shot/incremental
 * difference -- both sides here use the incremental front end.
 */
PG_PROP(incremental_chunk_size_invariant)
{
	size_t		len;
	char	   *s = pg_prop_draw_from(tc, JSON_ALPHABET, 0, 60, &len);
	int64		size_a = pg_prop_draw_int(tc, 1, 8);
	int64		size_b = pg_prop_draw_int(tc, 1, 8);
	JsonParseErrorType res_a;
	JsonParseErrorType res_b;

	res_a = parse_incremental(s, len, size_a);
	res_b = parse_incremental(s, len, size_b);

	if ((res_a == JSON_SUCCESS) != (res_b == JSON_SUCCESS))
	{
		char	   *shown = escape_for_note((const unsigned char *) s, len);

		pg_prop_fail(tc,
					 "chunk sizes %lld and %lld disagree: %d vs %d (input=\"%s\")",
					 (long long) size_a, (long long) size_b,
					 (int) res_a, (int) res_b, shown);
	}

	pg_free(s);
}
