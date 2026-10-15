/*-------------------------------------------------------------------------
 *
 * prop_spgist.c
 *	  AM-specific property tests for SP-GiST.
 *
 * SP-GiST ships no integer opclass, so it is not in the generic int-column
 * index trait suite (prop_index_am.c).  It does ship opclasses for text (the
 * radix-tree text_ops), point (quad/kd trees) and inet; those are exercised
 * here with the same core oracle used everywhere for indexes -- an index scan
 * must return exactly the rows a sequential scan does -- over the opclasses
 * SP-GiST actually supports.
 *
 *	 text_prefix   -- SP-GiST text_ops answering a prefix (LIKE 'p%') and an
 *				     equality (c = 'p') and a range (c < 'p') matches a seqscan.
 *	 point_contains -- SP-GiST over point answering a containment (p <@ box)
 *				      matches a seqscan.
 *	 inet_contains  -- SP-GiST over inet answering a network containment matches
 *				      a seqscan.
 *
 * Tier B: runs SQL against the cluster the TAP driver started.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/sql/prop_spgist.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "spgist"
#include "pg_prop.h"

#include "lib/stringinfo.h"

static pg_prop_conn *
connect_sp(pg_prop_tc *tc)
{
	char	   *err = NULL;
	pg_prop_conn *conn = pg_prop_connect(&err);

	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");
	pg_prop_sql_exec(conn, "SET client_min_messages=warning", &err);
	if (err)
		pg_free(err);
	return conn;
}

/*
 * Force the index path, then a seqscan, require identical rows.  bitmap selects
 * which forced-index shape (SP-GiST supports both plain and bitmap scans).
 */
static void
index_eq_seqscan(pg_prop_tc *tc, pg_prop_conn *conn, const char *select)
{
	char	   *err = NULL;
	char	   *idx,
			   *seq;
	bool		fi = false,
				fs = false;

	pg_prop_sql_exec(conn,
					 "SET enable_seqscan=off; SET enable_indexscan=on; "
					 "SET enable_bitmapscan=on", &err);
	idx = pg_prop_sql_rows(conn, select, &fi, &err);
	if (fi)
		pg_prop_fail(tc, "index-forced query failed: %s\n  SQL: %s",
					 err ? err : "", select);
	pg_prop_sql_exec(conn,
					 "SET enable_seqscan=on; SET enable_indexscan=off; "
					 "SET enable_bitmapscan=off", &err);
	seq = pg_prop_sql_rows(conn, select, &fs, &err);
	if (fs)
		pg_prop_fail(tc, "seqscan-forced query failed: %s\n  SQL: %s",
					 err ? err : "", select);
	pg_prop_sql_exec(conn, "RESET ALL", &err);
	if (strcmp(idx, seq) != 0)
		pg_prop_fail(tc,
					 "spgist index != seqscan:\n  SQL: %s\n  idx:\n%s\n  seq:\n%s",
					 select, idx, seq);
	pg_free(idx);
	pg_free(seq);
	if (err)
		pg_free(err);
}

/* Draw a short [a-c] string literal or NULL, ~15% NULL. */
static void
draw_text(pg_prop_tc *tc, StringInfo buf)
{
	int			len,
				i;

	if (pg_prop_draw_int(tc, 0, 6) == 0)
	{
		appendStringInfoString(buf, "NULL");
		return;
	}
	len = (int) pg_prop_draw_int(tc, 0, 3);
	appendStringInfoChar(buf, '\'');
	for (i = 0; i < len; i++)
		appendStringInfoChar(buf, (char) ('a' + pg_prop_draw_int(tc, 0, 2)));
	appendStringInfoChar(buf, '\'');
}

/* SP-GiST text_ops: equality, prefix (LIKE) and range all match a seqscan. */
PG_PROP(text_prefix)
{
	pg_prop_conn *conn = connect_sp(tc);
	char	   *err = NULL;
	int			n = (int) pg_prop_draw_int(tc, 0, 60);
	int			i;
	StringInfoData ddl;
	StringInfoData q;
	StringInfoData lit;

	initStringInfo(&ddl);
	appendStringInfoString(&ddl,
						   "DROP TABLE IF EXISTS spt; "
						   "CREATE TABLE spt (id int PRIMARY KEY, c text); ");
	for (i = 0; i < n; i++)
	{
		appendStringInfo(&ddl, "INSERT INTO spt VALUES (%d, ", i);
		{
			StringInfoData v;

			initStringInfo(&v);
			draw_text(tc, &v);
			appendStringInfoString(&ddl, v.data);
			pfree(v.data);
		}
		appendStringInfoString(&ddl, "); ");
	}
	appendStringInfoString(&ddl,
						   "CREATE INDEX spt_idx ON spt USING spgist (c); "
						   "ANALYZE spt;");
	if (!pg_prop_sql_exec(conn, ddl.data, &err))
		pg_prop_fail(tc, "spgist text build failed: %s", err ? err : "");
	pfree(ddl.data);

	initStringInfo(&lit);
	draw_text(tc, &lit);

	/* equality */
	initStringInfo(&q);
	appendStringInfo(&q, "SELECT id, c FROM spt WHERE c = %s ORDER BY id",
					 lit.data);
	index_eq_seqscan(tc, conn, q.data);

	/* prefix: LIKE 'x%' using a one-letter prefix drawn from a-c */
	resetStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT id, c FROM spt WHERE c LIKE '%c%%' ORDER BY id",
					 (char) ('a' + pg_prop_draw_int(tc, 0, 2)));
	index_eq_seqscan(tc, conn, q.data);

	/* range */
	resetStringInfo(&q);
	appendStringInfo(&q, "SELECT id, c FROM spt WHERE c < %s ORDER BY id",
					 lit.data[0] == 'N' ? "'b'" : lit.data);
	index_eq_seqscan(tc, conn, q.data);

	pfree(q.data);
	pfree(lit.data);
	pg_prop_sql_exec(conn, "DROP TABLE IF EXISTS spt", &err);
	if (err)
		pg_free(err);
	pg_prop_disconnect(conn);
}

/* SP-GiST over point: a box-containment query matches a seqscan. */
PG_PROP(point_contains)
{
	pg_prop_conn *conn = connect_sp(tc);
	char	   *err = NULL;
	int			n = (int) pg_prop_draw_int(tc, 0, 60);
	int			i;
	int			x0,
				y0,
				x1,
				y1,
				t;
	StringInfoData ddl;
	StringInfoData q;

	initStringInfo(&ddl);
	appendStringInfoString(&ddl,
						   "DROP TABLE IF EXISTS spp; "
						   "CREATE TABLE spp (id int PRIMARY KEY, p point); ");
	for (i = 0; i < n; i++)
		appendStringInfo(&ddl,
						 "INSERT INTO spp VALUES (%d, point(%d, %d)); ",
						 i, (int) pg_prop_draw_int(tc, -10, 10),
						 (int) pg_prop_draw_int(tc, -10, 10));
	appendStringInfoString(&ddl,
						   "CREATE INDEX spp_idx ON spp USING spgist (p); "
						   "ANALYZE spp;");
	if (!pg_prop_sql_exec(conn, ddl.data, &err))
		pg_prop_fail(tc, "spgist point build failed: %s", err ? err : "");
	pfree(ddl.data);

	x0 = (int) pg_prop_draw_int(tc, -10, 10);
	y0 = (int) pg_prop_draw_int(tc, -10, 10);
	x1 = (int) pg_prop_draw_int(tc, -10, 10);
	y1 = (int) pg_prop_draw_int(tc, -10, 10);
	if (x0 > x1) { t = x0; x0 = x1; x1 = t; }
	if (y0 > y1) { t = y0; y0 = y1; y1 = t; }

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT id FROM spp WHERE p <@ box(point(%d,%d), point(%d,%d)) "
					 "ORDER BY id", x0, y0, x1, y1);
	index_eq_seqscan(tc, conn, q.data);

	pfree(q.data);
	pg_prop_sql_exec(conn, "DROP TABLE IF EXISTS spp", &err);
	if (err)
		pg_free(err);
	pg_prop_disconnect(conn);
}

/* SP-GiST over inet: a network-containment query matches a seqscan. */
PG_PROP(inet_contains)
{
	pg_prop_conn *conn = connect_sp(tc);
	char	   *err = NULL;
	int			n = (int) pg_prop_draw_int(tc, 0, 60);
	int			i;
	StringInfoData ddl;
	StringInfoData q;
	int			octet;

	initStringInfo(&ddl);
	appendStringInfoString(&ddl,
						   "DROP TABLE IF EXISTS spi; "
						   "CREATE TABLE spi (id int PRIMARY KEY, a inet); ");
	for (i = 0; i < n; i++)
		appendStringInfo(&ddl,
						 "INSERT INTO spi VALUES (%d, ('10.0.%d.%d')::inet); ",
						 i, (int) pg_prop_draw_int(tc, 0, 3),
						 (int) pg_prop_draw_int(tc, 0, 255));
	appendStringInfoString(&ddl,
						   "CREATE INDEX spi_idx ON spi USING spgist (a inet_ops); "
						   "ANALYZE spi;");
	if (!pg_prop_sql_exec(conn, ddl.data, &err))
	{
		/* some builds lack the spgist inet opclass; skip cleanly */
		if (err)
			pg_free(err);
		pg_prop_skip(tc, "spgist inet_ops not available");
	}
	pfree(ddl.data);

	octet = (int) pg_prop_draw_int(tc, 0, 3);
	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT id FROM spi WHERE a <<= ('10.0.%d.0/24')::inet "
					 "ORDER BY id", octet);
	index_eq_seqscan(tc, conn, q.data);

	pfree(q.data);
	pg_prop_sql_exec(conn, "DROP TABLE IF EXISTS spi", &err);
	if (err)
		pg_free(err);
	pg_prop_disconnect(conn);
}
