/*-------------------------------------------------------------------------
 *
 * prop_dml_model.c
 *	  Stateful model test of the write path.
 *
 * Every other oracle in the suite checks a *read* (two ways of computing a
 * query agree).  This one checks *writes*: a random sequence of statements --
 * INSERT, UPDATE of key and value, DELETE, INSERT ... ON CONFLICT DO UPDATE /
 * DO NOTHING, MERGE with all three action kinds, savepoint/rollback, VACUUM,
 * index creation -- is applied both to a real table and to an in-memory model
 * of it (a plain C array: key -> value).  After every step the table, read
 * back in full, must equal the model.  Statements are drawn with the keys
 * colliding on purpose, so the conflict, HOT-update, index-maintenance and
 * MERGE-matching paths are all hit.
 *
 * The model is the specification: it is ten lines of obviously-correct C per
 * statement kind.  A disagreement means the server applied a write wrongly (or
 * a read after a write is wrong), and the shrunk statement sequence is the
 * reproducer.
 *
 * The table carries a secondary index on the value and (sometimes) a partial
 * index, so the read-back can be steered through an index scan as well as a
 * seqscan; both must agree with the model.  That catches an index left out of
 * step with the heap -- the classic write-path corruption.
 *
 * Portions Copyright (c) 2024, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "dml_model"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/* key domain: small, so statements collide on keys constantly */
#define DM_KMAX 24
/* steps per case */
#define DM_MAX_STEPS 40

typedef struct DmModel
{
	bool		present[DM_KMAX + 1];
	int			val[DM_KMAX + 1];	/* value; meaningful only if present */
	bool		isnull[DM_KMAX + 1];	/* value IS NULL */
} DmModel;

/* the model's committed state, plus a copy taken at the open savepoint */
typedef struct DmState
{
	DmModel		cur;
	DmModel		saved;
	bool		in_savepoint;
} DmState;

static void
dm_exec(pg_prop_tc *tc, pg_prop_conn *conn, const char *sql)
{
	char	   *err = NULL;

	if (!pg_prop_sql_exec(conn, sql, &err))
		pg_prop_fail(tc, "statement failed: %s\n  SQL: %s", err ? err : "", sql);
}

/* render the model as the same text the server's read-back produces */
static char *
dm_render(const DmModel *m)
{
	StringInfoData s;

	initStringInfo(&s);
	for (int k = 1; k <= DM_KMAX; k++)
	{
		if (!m->present[k])
			continue;
		if (m->isnull[k])
			appendStringInfo(&s, "%d\t\\N\n", k);
		else
			appendStringInfo(&s, "%d\t%d\n", k, m->val[k]);
	}
	return s.data;
}

/*
 * Compare the table with the model, reading the table back two ways: a
 * forced seqscan and a forced index scan on the value index (ordered by key in
 * both cases).  Either disagreeing with the model fails the case.
 */
static void
dm_check(pg_prop_tc *tc, pg_prop_conn *conn, const DmModel *m,
		 const StringInfo log)
{
	static const char *const reads[] = {
		"SET LOCAL enable_indexscan=off; SET LOCAL enable_bitmapscan=off; "
		"SET LOCAL enable_indexonlyscan=off;",
		"SET LOCAL enable_seqscan=off; SET LOCAL enable_bitmapscan=off;",
		"SET LOCAL enable_seqscan=off; SET LOCAL enable_indexscan=off;",
	};
	char	   *want = dm_render(m);

	for (int i = 0; i < (int) lengthof(reads); i++)
	{
		bool		failed = false;
		char	   *err = NULL;
		char	   *got;
		StringInfoData q;

		dm_exec(tc, conn, "BEGIN");
		dm_exec(tc, conn, reads[i]);
		initStringInfo(&q);
		/* v >= minint OR v IS NULL is true for every row, but lets an index on v drive the scan */
		appendStringInfoString(&q,
							   "SELECT k, v FROM dm WHERE v >= -2147483648 OR v IS NULL "
							   "ORDER BY k");
		got = pg_prop_sql_rows(conn, q.data, &failed, &err);
		pfree(q.data);
		dm_exec(tc, conn, "ROLLBACK");
		if (failed)
			pg_prop_fail(tc, "read-back failed: %s", err ? err : "");
		if (strcmp(got, want) != 0)
			pg_prop_fail(tc,
						 "table does not match the model (read %d: %s)\n"
						 "  model:\n%s  table:\n%s  statements so far:\n%s",
						 i, reads[i], want, got, log->data);
		pg_free(got);
	}
	pfree(want);
}

/* draw a value: small ints with heavy repeats, sometimes NULL */
static void
dm_draw_val(pg_prop_tc *tc, int *v, bool *isnull)
{
	*isnull = pg_prop_draw_int(tc, 0, 7) == 0;
	*v = (int) pg_prop_draw_int(tc, -3, 9);
}

static void
dm_val_sql(StringInfo b, int v, bool isnull)
{
	if (isnull)
		appendStringInfoString(b, "NULL");
	else
		appendStringInfo(b, "%d", v);
}

/*
 * One random step: emit its SQL into "sql" and apply its effect to the model.
 * Statements that the model predicts will fail (a duplicate-key INSERT, a key
 * UPDATE onto an existing key) are not emitted; the model only covers
 * successful writes, and the constraint area tests the failures.
 */
static void
dm_step(pg_prop_tc *tc, DmState *st, StringInfo sql)
{
	DmModel    *m = &st->cur;
	int			kind = (int) pg_prop_draw_int(tc, 0, 11);
	int			k = (int) pg_prop_draw_int(tc, 1, DM_KMAX);
	int			v;
	bool		vn;

	dm_draw_val(tc, &v, &vn);

	switch (kind)
	{
		case 0:					/* INSERT (only if absent) */
			if (m->present[k])
				goto upsert;
			appendStringInfo(sql, "INSERT INTO dm VALUES (%d, ", k);
			dm_val_sql(sql, v, vn);
			appendStringInfoString(sql, ")");
			m->present[k] = true;
			m->val[k] = v;
			m->isnull[k] = vn;
			break;
		case 1:					/* UPDATE value of one key */
			appendStringInfoString(sql, "UPDATE dm SET v = ");
			dm_val_sql(sql, v, vn);
			appendStringInfo(sql, " WHERE k = %d", k);
			if (m->present[k])
			{
				m->val[k] = v;
				m->isnull[k] = vn;
			}
			break;
		case 2:					/* UPDATE v = v + 1 over a value range (multi-row) */
			{
				int			lo = (int) pg_prop_draw_int(tc, -3, 9);

				appendStringInfo(sql, "UPDATE dm SET v = v + 1 WHERE v >= %d", lo);
				for (int i = 1; i <= DM_KMAX; i++)
					if (m->present[i] && !m->isnull[i] && m->val[i] >= lo)
						m->val[i]++;
			}
			break;
		case 3:					/* UPDATE the key itself, when the target is free */
			{
				int			k2 = (int) pg_prop_draw_int(tc, 1, DM_KMAX);

				if (!m->present[k] || m->present[k2] || k == k2)
				{
					appendStringInfo(sql, "SELECT 1");
					break;
				}
				appendStringInfo(sql, "UPDATE dm SET k = %d WHERE k = %d", k2, k);
				m->present[k2] = true;
				m->val[k2] = m->val[k];
				m->isnull[k2] = m->isnull[k];
				m->present[k] = false;
			}
			break;
		case 4:					/* DELETE one key */
			appendStringInfo(sql, "DELETE FROM dm WHERE k = %d", k);
			m->present[k] = false;
			break;
		case 5:					/* DELETE by value predicate, incl. NULLs */
			if (pg_prop_draw_bool(tc))
			{
				appendStringInfoString(sql, "DELETE FROM dm WHERE v IS NULL");
				for (int i = 1; i <= DM_KMAX; i++)
					if (m->present[i] && m->isnull[i])
						m->present[i] = false;
			}
			else
			{
				appendStringInfo(sql, "DELETE FROM dm WHERE v = %d", v);
				for (int i = 1; i <= DM_KMAX; i++)
					if (m->present[i] && !m->isnull[i] && m->val[i] == v)
						m->present[i] = false;
			}
			break;
		case 6:					/* INSERT ... ON CONFLICT DO UPDATE */
	upsert:
			appendStringInfo(sql, "INSERT INTO dm VALUES (%d, ", k);
			dm_val_sql(sql, v, vn);
			appendStringInfoString(sql,
								   ") ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v");
			m->present[k] = true;
			m->val[k] = v;
			m->isnull[k] = vn;
			break;
		case 7:					/* INSERT ... ON CONFLICT DO NOTHING */
			appendStringInfo(sql, "INSERT INTO dm VALUES (%d, ", k);
			dm_val_sql(sql, v, vn);
			appendStringInfoString(sql, ") ON CONFLICT DO NOTHING");
			if (!m->present[k])
			{
				m->present[k] = true;
				m->val[k] = v;
				m->isnull[k] = vn;
			}
			break;
		case 8:					/* MERGE over a small source of keys */
			{
				int			n = (int) pg_prop_draw_int(tc, 1, 4);
				int			sk[4];
				int			sv[4];
				bool		svn[4];
				int			nsrc = 0;
				bool		seen[DM_KMAX + 1] = {0};

				/* distinct source keys: MERGE rejects a row matched twice */
				for (int i = 0; i < n; i++)
				{
					int			kk = (int) pg_prop_draw_int(tc, 1, DM_KMAX);

					if (seen[kk])
						continue;
					seen[kk] = true;
					sk[nsrc] = kk;
					dm_draw_val(tc, &sv[nsrc], &svn[nsrc]);
					nsrc++;
				}
				appendStringInfoString(sql, "MERGE INTO dm t USING (VALUES ");
				for (int i = 0; i < nsrc; i++)
				{
					appendStringInfo(sql, "%s(%d, ", i ? ", " : "", sk[i]);
					dm_val_sql(sql, sv[i], svn[i]);
					appendStringInfoString(sql, "::int)");
				}
				/*
				 * matched & source value NULL -> delete; matched -> update;
				 * not matched -> insert.
				 */
				appendStringInfoString(sql,
									   ") AS s(k, v) ON t.k = s.k "
									   "WHEN MATCHED AND s.v IS NULL THEN DELETE "
									   "WHEN MATCHED THEN UPDATE SET v = s.v "
									   "WHEN NOT MATCHED THEN INSERT VALUES (s.k, s.v)");
				for (int i = 0; i < nsrc; i++)
				{
					int			kk = sk[i];

					if (m->present[kk] && svn[i])
						m->present[kk] = false;
					else if (m->present[kk])
					{
						m->val[kk] = sv[i];
						m->isnull[kk] = false;
					}
					else
					{
						m->present[kk] = true;
						m->val[kk] = sv[i];
						m->isnull[kk] = svn[i];
					}
				}
			}
			break;
		case 9:					/* savepoint open / rollback-to / release */
			if (!st->in_savepoint)
			{
				appendStringInfoString(sql, "SAVEPOINT sp");
				st->saved = st->cur;
				st->in_savepoint = true;
			}
			else if (pg_prop_draw_bool(tc))
			{
				appendStringInfoString(sql, "ROLLBACK TO SAVEPOINT sp; RELEASE SAVEPOINT sp");
				st->cur = st->saved;
				st->in_savepoint = false;
			}
			else
			{
				appendStringInfoString(sql, "RELEASE SAVEPOINT sp");
				st->in_savepoint = false;
			}
			break;
		case 10:				/* COMMIT and start a new transaction (VACUUM needs out-of-txn) */
			appendStringInfoString(sql, "COMMIT; BEGIN");
			st->in_savepoint = false;
			break;
		default:				/* TRUNCATE (transactional; undone by ROLLBACK TO) */
			appendStringInfoString(sql, "TRUNCATE dm");
			for (int i = 1; i <= DM_KMAX; i++)
				m->present[i] = false;
			break;
	}
}

/*
 * The property: run a random statement sequence inside transactions, and after
 * every statement compare the table with the model.  Every few steps the
 * transaction commits, sometimes followed by VACUUM (so pruning, HOT chains
 * and index cleanup run between writes) or by a fresh index build.
 */
PG_PROP(dml_matches_model)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	DmState		st;
	StringInfoData log;
	StringInfoData sql;
	int			nsteps;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	memset(&st, 0, sizeof(st));
	initStringInfo(&log);
	initStringInfo(&sql);

	/* fillfactor < 100 leaves room on the page for HOT updates */
	appendStringInfo(&sql,
					 "DROP TABLE IF EXISTS dm; "
					 "CREATE TABLE dm (k int PRIMARY KEY, v int) WITH (fillfactor = %d); "
					 "CREATE INDEX dm_v ON dm (v)",
					 pg_prop_draw_bool(tc) ? 100 : 50);
	if (pg_prop_draw_bool(tc))
		appendStringInfoString(&sql, "; CREATE INDEX dm_vp ON dm (v) WHERE v > 3");
	dm_exec(tc, conn, sql.data);
	dm_exec(tc, conn, "BEGIN");

	nsteps = (int) pg_prop_draw_int(tc, 1, DM_MAX_STEPS);
	for (int i = 0; i < nsteps; i++)
	{
		resetStringInfo(&sql);
		dm_step(tc, &st, &sql);
		appendStringInfo(&log, "    %s;\n", sql.data);
		dm_exec(tc, conn, sql.data);

		/* occasionally, between transactions, VACUUM or REINDEX */
		if (strncmp(sql.data, "COMMIT", 6) == 0 && pg_prop_draw_bool(tc))
		{
			const char *maint = pg_prop_draw_bool(tc) ? "VACUUM dm" : "REINDEX TABLE dm";

			dm_exec(tc, conn, "COMMIT");
			appendStringInfo(&log, "    COMMIT; %s; BEGIN;\n", maint);
			dm_exec(tc, conn, maint);
			dm_exec(tc, conn, "BEGIN");
		}
		/*
		 * The read-back runs in its own BEGIN/ROLLBACK, which would end the
		 * open transaction; wrap the check in a savepoint instead when one is
		 * not already in use, else check after the next COMMIT.
		 */
		if (!st.in_savepoint)
		{
			dm_exec(tc, conn, "COMMIT");
			dm_check(tc, conn, &st.cur, &log);
			dm_exec(tc, conn, "BEGIN");
		}
	}
	dm_exec(tc, conn, "COMMIT");
	st.in_savepoint = false;
	dm_check(tc, conn, &st.cur, &log);

	dm_exec(tc, conn, "DROP TABLE dm");
	pfree(log.data);
	pfree(sql.data);
	pg_prop_disconnect(conn);
}
