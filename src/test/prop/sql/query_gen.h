/*-------------------------------------------------------------------------
 *
 * query_gen.h
 *	  Shared SQL query generator for the query-level property areas.
 *
 * The scalar and type areas (prop_numeric.c and friends) generate one value
 * and check one function.  The query areas in this subdirectory instead
 * generate a whole relation and a whole SQL query over it, then check a
 * property of the *result set* -- the kind of property that catches planner
 * and executor wrong-results bugs rather than type-I/O bugs.
 *
 * This header is the generator those areas share: it builds a random table
 * and a random boolean predicate over its columns, both as SQL text, so each
 * area can wrap them in whatever query form its oracle needs (a ternary
 * partition for TLP, an optimisable/unoptimisable pair for NoREC, the same
 * query under flipped planner GUCs for the metamorphic area).
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/sql/query_gen.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef QUERY_GEN_H
#define QUERY_GEN_H

#include "pg_prop.h"
#include "lib/stringinfo.h"

/*
 * The generated table.  A fixed, legible shape: a few typed columns whose
 * domains are small so a counterexample stays readable and a case stays fast,
 * but wide enough that AND/OR/NOT predicates over them exercise real
 * qualification, join, and NULL-handling paths.
 *
 * Columns:
 *	 id   int    PRIMARY KEY (1..n, dense) -- gives an index to (not) use
 *	 a    int    small domain, ~15% NULL   -- the main predicate column
 *	 b    int    small domain, ~15% NULL
 *	 c    text   short [a-c] string or NULL
 *	 f    float8 small domain or NULL
 */
#define QG_TABLE "qg_t"

/* Number of rows the generated table holds this case (0 .. QG_MAX_ROWS). */
#define QG_MAX_ROWS 40

/*
 * Build QG_TABLE (dropping any prior one) and populate it with a random set
 * of 0..QG_MAX_ROWS rows.  Returns the row count via *nrows.  Fails the test
 * case (does not return) on any SQL error, since the generator itself must
 * never produce invalid DDL/DML.
 */
extern void qg_build_table(pg_prop_tc *tc, pg_prop_conn *conn, int *nrows);

/*
 * Append a random boolean predicate over QG_TABLE's columns to buf.  The
 * predicate is always well-typed and parseable, mixes AND/OR/NOT, comparisons
 * (= <> < <= > >=), IS [NOT] NULL, IN-lists and BETWEEN, and references the
 * numeric, text and float columns.  Depth is bounded so it always terminates.
 *
 * The predicate is three-valued (may evaluate to NULL per row): that is the
 * point, because the TLP and NoREC oracles are precisely about correct
 * three-valued-logic handling in the qualification machinery.
 */
extern void qg_draw_predicate(pg_prop_tc *tc, StringInfo buf);

/*
 * Draw a random scalar expression over QG_TABLE's columns (an integer-typed
 * expression: a column, a constant, or an arithmetic combination).  Used where
 * an area needs an ORDER BY / GROUP BY / projection target rather than a
 * boolean.  Always well-typed and non-erroring (division is guarded).
 */
extern void qg_draw_int_expr(pg_prop_tc *tc, StringInfo buf);

#endif							/* QUERY_GEN_H */
