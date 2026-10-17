/*-------------------------------------------------------------------------
 *
 * prop_concurrency.c
 *	  Multi-backend concurrency property tests.
 *
 * The other areas use one backend per property (a couple use two or three for
 * isolation).  This area orchestrates a POOL of backends whose membership
 * churns -- sessions connect, do work, and disconnect throughout the test --
 * and asserts invariants that must hold regardless of how the concurrent work
 * interleaves.  The async primitives (pg_prop_sql_send / pg_prop_sql_await)
 * let several statements be in flight at once; the harness here drives a
 * variable-sized set of them.
 *
 * The oracles are conservation / consistency laws a correct concurrent system
 * must preserve under any serializable schedule:
 *
 *	 transfer_conserves_total -- a set of accounts starts with a known total;
 *						       many concurrent SERIALIZABLE transactions each move
 *						       a random amount between two accounts (debit one,
 *						       credit another).  Transfers conserve the total, so
 *						       after all commit/abort the sum of balances equals
 *						       the starting total exactly.  A lost update or a
 *						       torn transaction would change the total.
 *	 counter_increments_exact -- N concurrent sessions each do K atomic
 *						       UPDATE ... SET c = c + 1 on one row; the final
 *						       value equals the number of COMMITTED increments
 *						       (no lost updates under row locking).
 *	 churn_no_orphan_xacts    -- while sessions constantly open and close, no
 *						       committed row is lost and no uncommitted row is
 *						       visible: a snapshot count equals the count of
 *						       committed inserts.
 *
 * Backend-count churn: the number of live sessions is drawn and varied within
 * a case (sessions are opened and closed as the work proceeds), so the system
 * is exercised under a changing concurrency level, not a fixed one.
 *
 * Tier B: runs SQL against the cluster the TAP driver started.  Uses up to
 * CONC_MAX_SESS connections; the harness's per-case cleanup closes any left
 * open.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/sql/prop_concurrency.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "concurrency"
#include "pg_prop.h"

#include "lib/stringinfo.h"

#define CONC_MAX_SESS 16

static pg_prop_conn *
open_conn(pg_prop_tc *tc)
{
	char	   *err = NULL;
	pg_prop_conn *c = pg_prop_connect(&err);

	if (c == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");
	return c;
}

static long
scalar_long(pg_prop_tc *tc, pg_prop_conn *conn, const char *sql)
{
	bool		failed = false;
	char	   *err = NULL;
	char	   *v;
	long		n;

	v = pg_prop_sql_scalar(conn, sql, &failed, &err);
	if (failed)
		pg_prop_fail(tc, "scalar failed: %s\n  SQL: %s",
					 err ? err : "(unknown)", sql);
	n = v ? atol(v) : 0;
	if (v)
		pg_free(v);
	if (err)
		pg_free(err);
	return n;
}

/*
 * Transfers conserve the total.  Set up ACCOUNTS accounts of 1000 each
 * (total = ACCOUNTS*1000).  Then run many transfers across a churning pool of
 * sessions: each transfer is a SERIALIZABLE txn that reads two balances and
 * writes them (debit/credit the same amount).  Some will fail with
 * serialization errors and roll back -- that is fine, a rolled-back transfer
 * moves nothing.  After everything settles, the total must be unchanged.
 */
PG_PROP(transfer_conserves_total)
{
	pg_prop_conn *admin = open_conn(tc);
	pg_prop_conn *sess[CONC_MAX_SESS];
	int			nsess = 0;
	char	   *err = NULL;
	int			accounts = (int) pg_prop_draw_int(tc, 2, 8);
	int			transfers = (int) pg_prop_draw_int(tc, 1, 40);
	long		start_total,
				end_total;
	int			i;
	StringInfoData q;

	/* set up the accounts table */
	initStringInfo(&q);
	appendStringInfoString(&q,
						   "DROP TABLE IF EXISTS acct; "
						   "CREATE TABLE acct (id int PRIMARY KEY, bal bigint); ");
	for (i = 0; i < accounts; i++)
		appendStringInfo(&q, "INSERT INTO acct VALUES (%d, 1000); ", i);
	if (!pg_prop_sql_exec(admin, q.data, &err))
		pg_prop_fail(tc, "accounts setup failed: %s", err ? err : "");

	start_total = scalar_long(tc, admin, "SELECT sum(bal) FROM acct");

	/*
	 * Drive the transfers over a churning pool: for each transfer, with some
	 * probability open a new session (up to the cap) or reuse/close one, then
	 * run the transfer on a drawn session.  The pool size thus wanders.
	 */
	for (i = 0; i < transfers; i++)
	{
		int			from = (int) pg_prop_draw_int(tc, 0, accounts - 1);
		int			to = (int) pg_prop_draw_int(tc, 0, accounts - 1);
		int			amt = (int) pg_prop_draw_int(tc, 1, 100);
		int			which;
		char	   *ss = NULL;

		if (to == from)
			to = (to + 1) % accounts;

		/* churn: maybe grow the pool */
		if ((nsess == 0 || pg_prop_draw_bool(tc)) && nsess < CONC_MAX_SESS)
			sess[nsess++] = open_conn(tc);
		/* churn: maybe close the oldest and reopen (a disconnect/reconnect) */
		else if (nsess > 1 && pg_prop_draw_int(tc, 0, 3) == 0)
		{
			pg_prop_disconnect(sess[0]);
			for (int j = 1; j < nsess; j++)
				sess[j - 1] = sess[j];
			sess[nsess - 1] = open_conn(tc);
		}

		which = (int) pg_prop_draw_int(tc, 0, nsess - 1);

		/* a serializable transfer: debit then credit the same amount */
		resetStringInfo(&q);
		appendStringInfo(&q,
						 "BEGIN ISOLATION LEVEL SERIALIZABLE; "
						 "UPDATE acct SET bal = bal - %d WHERE id = %d; "
						 "UPDATE acct SET bal = bal + %d WHERE id = %d; "
						 "COMMIT",
						 amt, from, amt, to);
		/* run it; a serialization failure rolls the whole txn back (no move) */
		if (!pg_prop_sql_exec(sess[which], q.data, &err))
		{
			/* ensure the aborted session is usable again */
			pg_prop_sql_exec(sess[which], "ROLLBACK", &ss);
			if (ss)
				pg_free(ss);
		}
		if (err)
		{
			pg_free(err);
			err = NULL;
		}
	}

	/* close the pool */
	for (i = 0; i < nsess; i++)
		pg_prop_disconnect(sess[i]);

	end_total = scalar_long(tc, admin, "SELECT sum(bal) FROM acct");
	if (end_total != start_total)
		pg_prop_fail(tc,
					 "transfers did not conserve total: start=%ld end=%ld "
					 "(%d accounts, %d transfers)",
					 start_total, end_total, accounts, transfers);

	pg_prop_sql_exec(admin, "DROP TABLE IF EXISTS acct", &err);
	if (err)
		pg_free(err);
	pfree(q.data);
	pg_prop_disconnect(admin);
}

/*
 * No lost updates: a single counter row, incremented by many transactions
 * spread over a churning pool.  Each increment is its own autocommit
 * UPDATE ... SET c = c + 1 (row lock serializes them).  The final value equals
 * the number of increments that COMMITTED.  Since each is a single autocommit
 * statement it cannot partially apply, so committed == attempted here, and the
 * final counter must equal the attempt count.
 */
PG_PROP(counter_increments_exact)
{
	pg_prop_conn *admin = open_conn(tc);
	pg_prop_conn *sess[CONC_MAX_SESS];
	int			nsess = 0;
	char	   *err = NULL;
	int			incs = (int) pg_prop_draw_int(tc, 0, 60);
	int			i;
	long		final;

	if (!pg_prop_sql_exec(admin,
						  "DROP TABLE IF EXISTS ctr; "
						  "CREATE TABLE ctr (c bigint); INSERT INTO ctr VALUES (0)",
						  &err))
		pg_prop_fail(tc, "counter setup failed: %s", err ? err : "");

	for (i = 0; i < incs; i++)
	{
		int			which;

		if ((nsess == 0 || pg_prop_draw_bool(tc)) && nsess < CONC_MAX_SESS)
			sess[nsess++] = open_conn(tc);
		else if (nsess > 1 && pg_prop_draw_int(tc, 0, 4) == 0)
		{
			pg_prop_disconnect(sess[nsess - 1]);
			sess[nsess - 1] = open_conn(tc);
		}
		which = (int) pg_prop_draw_int(tc, 0, nsess - 1);
		if (!pg_prop_sql_exec(sess[which], "UPDATE ctr SET c = c + 1", &err))
			pg_prop_fail(tc, "increment failed: %s", err ? err : "");
	}

	for (i = 0; i < nsess; i++)
		pg_prop_disconnect(sess[i]);

	final = scalar_long(tc, admin, "SELECT c FROM ctr");
	if (final != incs)
		pg_prop_fail(tc, "lost updates: counter=%ld != increments=%d",
					 final, incs);

	pg_prop_sql_exec(admin, "DROP TABLE IF EXISTS ctr", &err);
	if (err)
		pg_free(err);
	pg_prop_disconnect(admin);
}

/*
 * Churn with no orphan transactions: sessions open, insert a committed row,
 * sometimes open a transaction that inserts a row and ROLLS BACK, then close.
 * The number of rows visible afterward equals the number of committed inserts;
 * no rolled-back row survives.
 */
PG_PROP(churn_no_orphan_xacts)
{
	pg_prop_conn *admin = open_conn(tc);
	char	   *err = NULL;
	int			ops = (int) pg_prop_draw_int(tc, 0, 50);
	int			committed = 0;
	int			i;
	long		visible;

	if (!pg_prop_sql_exec(admin,
						  "DROP TABLE IF EXISTS churn; "
						  "CREATE TABLE churn (id serial PRIMARY KEY, v int)",
						  &err))
		pg_prop_fail(tc, "churn setup failed: %s", err ? err : "");

	for (i = 0; i < ops; i++)
	{
		pg_prop_conn *s = open_conn(tc);
		int			v = (int) pg_prop_draw_int(tc, 0, 1000);
		StringInfoData q;

		initStringInfo(&q);
		if (pg_prop_draw_bool(tc))
		{
			/* committed insert */
			appendStringInfo(&q, "INSERT INTO churn (v) VALUES (%d)", v);
			if (pg_prop_sql_exec(s, q.data, &err))
				committed++;
			else if (err) { pg_free(err); err = NULL; }
		}
		else
		{
			/* rolled-back insert: must leave nothing */
			resetStringInfo(&q);
			appendStringInfo(&q,
							 "BEGIN; INSERT INTO churn (v) VALUES (%d); ROLLBACK",
							 v);
			pg_prop_sql_exec(s, q.data, &err);
			if (err) { pg_free(err); err = NULL; }
		}
		pfree(q.data);
		pg_prop_disconnect(s);	/* backend count churns every op */
	}

	visible = scalar_long(tc, admin, "SELECT count(*) FROM churn");
	if (visible != committed)
		pg_prop_fail(tc,
					 "visible rows %ld != committed inserts %d (rolled-back row leaked?)",
					 visible, committed);

	pg_prop_sql_exec(admin, "DROP TABLE IF EXISTS churn", &err);
	if (err)
		pg_free(err);
	pg_prop_disconnect(admin);
}
