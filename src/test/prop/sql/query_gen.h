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
 * This header is the generator those areas share.  It builds a random table
 * and a random boolean predicate over its columns, both as SQL text, so each
 * area can wrap them in whatever query form its oracle needs (a ternary
 * partition for TLP, an optimisable/unoptimisable pair for NoREC, the same
 * query under steered plans for the plan-advice area).
 *
 * The table's shape is a schema (qg_schema) rather than a hard-coded column
 * list.  Core registers a fixed set of typed columns; an extension can add
 * columns of its own types through the "query_gen.column" extension point, and
 * predicate leaves over them through "query_gen.predicate" (see pg_prop.h,
 * Extension points).  Because the oracles only ever read the generated SQL
 * text, an extension that teaches the generator about its type immediately
 * gets TLP, NoREC, cardinality, PQS and plan-advice coverage for that type
 * with no new oracle code.
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

/* The generated table's name. */
#define QG_TABLE "qg_t"

/* Number of rows the generated table holds this case (0 .. QG_MAX_ROWS). */
#define QG_MAX_ROWS 40

/* Upper bound on columns (core + extension). */
#define QG_MAX_COLS 16

/*
 * A column's value generator: append one SQL literal (or NULL) for this
 * column to buf, drawing from tc.  Must always emit a well-typed, parseable
 * literal for the column's declared SQL type.
 */
typedef void (*qg_value_fn) (pg_prop_tc *tc, StringInfo buf);

typedef struct qg_column
{
	const char *name;			/* SQL identifier */
	const char *sqltype;		/* SQL type text, e.g. "int", "citext" */
	qg_value_fn draw;			/* emits one literal or NULL */
	bool		is_text_like;	/* comparable with text operators/order */
	bool		is_numeric_like;	/* usable in arithmetic / numeric compare */
} qg_column;

typedef struct qg_schema
{
	qg_column	cols[QG_MAX_COLS];
	int			ncols;
} qg_schema;

/*
 * Add a column to the schema.  Called by core to seed the standard columns and
 * by "query_gen.column" hook contributors to add extension columns.  Silently
 * ignores additions past QG_MAX_COLS (the generator stays bounded).
 */
extern void qg_add_column(qg_schema *s, const char *name, const char *sqltype,
						  qg_value_fn draw, bool is_text_like,
						  bool is_numeric_like);

/*
 * Context handed to a "query_gen.predicate" hook contributor via the void
 * *ctx argument: the schema in play and the buffer to append a leaf to.  A
 * contributor appends exactly one boolean leaf predicate referencing one of
 * the extension's own columns (which it finds by name in schema).
 *
 * The "query_gen.setup" extension point is also published by qg_build_table:
 * its ctx is the live pg_prop_conn *, and a contributor runs idempotent DDL
 * (typically CREATE EXTENSION IF NOT EXISTS) so the types its columns use
 * exist before the table is created.  It runs once per generated table.
 */
typedef struct qg_pred_ctx
{
	const qg_schema *schema;
	StringInfo	buf;
} qg_pred_ctx;

/*
 * Build QG_TABLE (dropping any prior one) and populate it with a random set
 * of 0..QG_MAX_ROWS rows according to the current schema (core columns plus
 * any contributed by "query_gen.column" hooks).  Returns the row count via
 * *nrows.  Fails the test case (does not return) on any SQL error, since the
 * generator itself must never produce invalid DDL/DML.
 */
extern void qg_build_table(pg_prop_tc *tc, pg_prop_conn *conn, int *nrows);

/*
 * Append a random boolean predicate over QG_TABLE's columns to buf.  The
 * predicate is always well-typed and parseable, mixes AND/OR/NOT, comparisons,
 * IS [NOT] NULL, IN-lists and BETWEEN, and references the generated columns.
 * It may be three-valued (NULL per row) -- that is the point, since the TLP
 * and NoREC oracles are about correct three-valued-logic handling.  Extension
 * predicate leaves (from "query_gen.predicate" hooks) are mixed in when any
 * are registered.
 */
extern void qg_draw_predicate(pg_prop_tc *tc, StringInfo buf);

/*
 * Draw a random integer-typed scalar expression over QG_TABLE's columns (a
 * column, a constant, or an arithmetic combination).  Used where an area needs
 * an ORDER BY / GROUP BY / projection target rather than a boolean.  Always
 * well-typed and non-erroring (division is guarded).
 */
extern void qg_draw_int_expr(pg_prop_tc *tc, StringInfo buf);

/*
 * The schema currently in effect (built once per process from core columns +
 * "query_gen.column" hooks).  Exposed so an area or a hook can inspect the
 * columns -- for example PQS needs the column names and types to rectify a
 * predicate against a pivot row.
 */
extern const qg_schema *qg_current_schema(pg_prop_tc *tc);

#endif							/* QUERY_GEN_H */
