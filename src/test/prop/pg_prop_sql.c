/*-------------------------------------------------------------------------
 *
 * pg_prop_sql.c
 *	  libpq oracle helpers for Tier-B property tests.
 *
 * Backend code (planner, executor, type I/O) cannot be linked into a frontend
 * binary, so properties that exercise it drive SQL against a live cluster and
 * compare the result to an oracle.  These helpers wrap just enough of libpq
 * for that: connect, run a scalar/rowset/DDL query, render the result as a
 * string the property can compare.  See pg_prop.h for the contract.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/pg_prop_sql.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include <time.h>
#include <unistd.h>

#include "lib/stringinfo.h"
#include "libpq-fe.h"
#include "pg_prop.h"

struct pg_prop_conn
{
	PGconn	   *conn;
	struct pg_prop_conn *next;	/* link in the live-connection registry */
};

/*
 * Live connections, so the driver can close any a failing/assumed-away case
 * leaked by longjmp'ing over its pg_prop_disconnect().  One run is single-
 * threaded, so a plain list needs no locking.
 */
static pg_prop_conn *live_conns = NULL;

pg_prop_conn *
pg_prop_connect(char **err)
{
	pg_prop_conn *c;
	PGconn	   *conn;

	/*
	 * Empty connection string: everything comes from the environment
	 * (PGHOST, PGPORT, PGDATABASE), which the TAP driver points at the test
	 * node.  dbname defaults to "postgres" if PGDATABASE is unset.
	 */
	const char *keywords[] = {"dbname", NULL};
	const char *values[] = {getenv("PGDATABASE") ? getenv("PGDATABASE") : "postgres", NULL};

	conn = PQconnectdbParams(keywords, values, true);
	if (PQstatus(conn) != CONNECTION_OK)
	{
		if (err)
			*err = pg_strdup(PQerrorMessage(conn));
		PQfinish(conn);
		return NULL;
	}
	c = pg_malloc(sizeof(pg_prop_conn));
	c->conn = conn;
	c->next = live_conns;
	live_conns = c;

	/* Quieten DROP ... IF EXISTS notices and similar chatter. */
	PQclear(PQexec(conn, "SET client_min_messages = warning"));

	return c;
}

/* Unlink c from the live list, if present. */
static void
unlink_conn(pg_prop_conn *c);

/*
 * Crash tripwire.  A backend that dies (assertion failure, segfault) takes
 * its connection with it, and the property would otherwise see only a
 * confusing SQL failure, which the engine then "shrinks" by trying other
 * inputs until the case stops reproducing -- by which time the postmaster is
 * in crash recovery and every following area fails to connect.  So every
 * statement helper calls this after PQexec: if the connection is now bad,
 * the statement that killed it is appended to $PG_PROP_CRASH_LOG (default
 * pg_prop_crashes.sql in the working directory), stamped with the area
 * and time, and the run aborts at once with a distinct exit code.  The
 * crashing statement is the reproducer; it is captured before shrinking can
 * lose it, and nothing downstream runs against a recovering server.
 */
static void
check_backend_alive(pg_prop_conn *conn, const char *sql)
{
	FILE	   *f;
	const char *path;
	time_t		now;

	if (PQstatus(conn->conn) == CONNECTION_OK)
		return;

	path = getenv("PG_PROP_CRASH_LOG");
	if (path == NULL || path[0] == '\0')
		path = "pg_prop_crashes.sql";
	now = time(NULL);
	f = fopen(path, "a");
	if (f != NULL)
	{
		fprintf(f, "-- backend died: area=%s pid=%d at %s"
				"-- server said: %s\n%s;\n\n",
				pg_prop_current_area ? pg_prop_current_area : "?",
				(int) getpid(), ctime(&now),
				PQerrorMessage(conn->conn), sql);
		fclose(f);
	}
	fprintf(stderr,
			"pg_prop: BACKEND CRASH -- the server connection died while running:\n"
			"%s\n(statement saved to %s; aborting so no further area runs "
			"against a recovering server)\n", sql, path);
	fflush(stdout);
	exit(3);
}

/* Unlink c from the live list, if present. */
static void
unlink_conn(pg_prop_conn *c)
{
	pg_prop_conn **pp = &live_conns;

	while (*pp != NULL)
	{
		if (*pp == c)
		{
			*pp = c->next;
			return;
		}
		pp = &(*pp)->next;
	}
}

void
pg_prop_disconnect(pg_prop_conn *conn)
{
	if (conn == NULL)
		return;
	unlink_conn(conn);
	PQfinish(conn->conn);
	pg_free(conn);
}

void
pg_prop_sql_cleanup(void)
{
	while (live_conns != NULL)
	{
		pg_prop_conn *c = live_conns;

		live_conns = c->next;
		PQfinish(c->conn);
		pg_free(c);
	}
}

char *
pg_prop_sql_scalar(pg_prop_conn *conn, const char *sql, bool *failed, char **errmsg)
{
	PGresult   *res = PQexec(conn->conn, sql);
	ExecStatusType st;
	char	   *out;

	check_backend_alive(conn, sql);
	st = PQresultStatus(res);

	if (failed)
		*failed = false;
	if (st != PGRES_TUPLES_OK)
	{
		if (failed)
			*failed = true;
		if (errmsg)
			*errmsg = pg_strdup(PQresultErrorMessage(res));
		PQclear(res);
		return NULL;
	}
	if (PQntuples(res) == 0 || PQnfields(res) == 0 || PQgetisnull(res, 0, 0))
	{
		PQclear(res);
		return NULL;			/* genuine NULL / empty, *failed stays false */
	}
	out = pg_strdup(PQgetvalue(res, 0, 0));
	PQclear(res);
	return out;
}

char *
pg_prop_sql_rows(pg_prop_conn *conn, const char *sql, bool *failed, char **errmsg)
{
	PGresult   *res = PQexec(conn->conn, sql);
	ExecStatusType st;
	StringInfoData s;
	int			nrows,
				ncols;

	check_backend_alive(conn, sql);
	st = PQresultStatus(res);

	if (failed)
		*failed = false;
	if (st != PGRES_TUPLES_OK)
	{
		if (failed)
			*failed = true;
		if (errmsg)
			*errmsg = pg_strdup(PQresultErrorMessage(res));
		PQclear(res);
		return NULL;
	}

	initStringInfo(&s);
	nrows = PQntuples(res);
	ncols = PQnfields(res);
	for (int r = 0; r < nrows; r++)
	{
		for (int col = 0; col < ncols; col++)
		{
			if (col > 0)
				appendStringInfoChar(&s, '\t');
			if (PQgetisnull(res, r, col))
				appendStringInfoString(&s, "\\N");
			else
				appendStringInfoString(&s, PQgetvalue(res, r, col));
		}
		appendStringInfoChar(&s, '\n');
	}
	PQclear(res);
	return s.data;				/* caller frees */
}

/*
 * Shared body for the two rowhash variants.  Digests the whole result
 * server-side: md5 of the newline-joined per-row text.  Returning a 32-char
 * hash instead of the full result keeps client memory O(1) regardless of
 * result size (the deep generators can produce results that overflow the
 * 1 GB StringInfo cap when rendered client-side; see finding 027).  When
 * "ordered" is false the per-row text is sorted first, so the digest is of
 * the row multiset rather than the row sequence.  An empty result hashes
 * md5('') so two empty results compare equal.
 */
static char *
rowhash_body(pg_prop_conn *conn, const char *sql, bool ordered,
			 bool *failed, char **errmsg)
{
	StringInfoData q;
	PGresult   *res;
	ExecStatusType st;
	char	   *out;

	if (failed)
		*failed = false;

	initStringInfo(&q);
	if (ordered)
		appendStringInfo(&q,
						 "SELECT md5(coalesce(string_agg(q.r, E'\\n'), '')) "
						 "FROM (SELECT (sub)::text AS r FROM (%s) sub) q",
						 sql);
	else
		appendStringInfo(&q,
						 "SELECT md5(coalesce(string_agg(q.r, E'\\n' ORDER BY q.r), '')) "
						 "FROM (SELECT (sub)::text AS r FROM (%s) sub) q",
						 sql);
	res = PQexec(conn->conn, q.data);
	check_backend_alive(conn, sql);
	pfree(q.data);
	st = PQresultStatus(res);
	if (st != PGRES_TUPLES_OK)
	{
		if (failed)
			*failed = true;
		if (errmsg)
			*errmsg = pg_strdup(PQresultErrorMessage(res));
		PQclear(res);
		return NULL;
	}
	if (PQntuples(res) == 0 || PQgetisnull(res, 0, 0))
	{
		PQclear(res);
		return pg_strdup("");	/* no rows -> empty digest */
	}
	out = pg_strdup(PQgetvalue(res, 0, 0));
	PQclear(res);
	return out;
}

char *
pg_prop_sql_rowhash(pg_prop_conn *conn, const char *sql, bool *failed,
					char **errmsg)
{
	return rowhash_body(conn, sql, true, failed, errmsg);
}

char *
pg_prop_sql_rowhash_unordered(pg_prop_conn *conn, const char *sql,
							  bool *failed, char **errmsg)
{
	return rowhash_body(conn, sql, false, failed, errmsg);
}

bool
pg_prop_sql_exec(pg_prop_conn *conn, const char *sql, char **errmsg)
{
	PGresult   *res = PQexec(conn->conn, sql);
	ExecStatusType st;

	check_backend_alive(conn, sql);
	st = PQresultStatus(res);

	if (st != PGRES_COMMAND_OK && st != PGRES_TUPLES_OK)
	{
		if (errmsg)
			*errmsg = pg_strdup(PQresultErrorMessage(res));
		PQclear(res);
		return false;
	}
	PQclear(res);
	return true;
}

/*
 * Load an extension (contrib module or procedural language) into the session,
 * returning true if it is now available.  Checks pg_available_extensions first
 * so a missing module is reported as "unavailable" (false) rather than a hard
 * SQL error; if present, runs CREATE EXTENSION IF NOT EXISTS.  Used by areas
 * that test an extension so they can pg_prop_skip() cleanly when it is not
 * installed on the server under test.
 */
bool
pg_prop_require_extension(pg_prop_conn *conn, const char *extname)
{
	PGresult   *res;
	char		q[256];
	bool		avail;

	/* is it installable here at all? */
	snprintf(q, sizeof(q),
			 "SELECT 1 FROM pg_available_extensions WHERE name = '%s'",
			 extname);
	res = PQexec(conn->conn, q);
	avail = (PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) == 1);
	PQclear(res);
	if (!avail)
		return false;

	snprintf(q, sizeof(q), "CREATE EXTENSION IF NOT EXISTS \"%s\"", extname);
	res = PQexec(conn->conn, q);
	avail = (PQresultStatus(res) == PGRES_COMMAND_OK ||
			 PQresultStatus(res) == PGRES_TUPLES_OK);
	PQclear(res);
	return avail;
}

/*
 * Asynchronous statement support, for properties that need two sessions in
 * flight at once (e.g. provoking a deadlock, where both must be blocked before
 * either can resolve).  pg_prop_sql_send dispatches a statement without waiting
 * for its result; pg_prop_sql_await then blocks for the result and reports
 * whether it succeeded, returning the SQLSTATE (a malloc'd 5-char code, or NULL)
 * via *sqlstate so the caller can distinguish, e.g., a deadlock (40P01) from a
 * serialization failure (40001).
 */
bool
pg_prop_sql_send(pg_prop_conn *conn, const char *sql)
{
	return PQsendQuery(conn->conn, sql) == 1;
}

bool
pg_prop_sql_await(pg_prop_conn *conn, char **sqlstate, char **errmsg)
{
	PGresult   *res;
	bool		ok = true;

	if (sqlstate)
		*sqlstate = NULL;
	if (errmsg)
		*errmsg = NULL;

	/* drain all results for the dispatched command(s) */
	while ((res = PQgetResult(conn->conn)) != NULL)
	{
		ExecStatusType st = PQresultStatus(res);

		if (st != PGRES_COMMAND_OK && st != PGRES_TUPLES_OK &&
			st != PGRES_EMPTY_QUERY)
		{
			ok = false;
			if (sqlstate && *sqlstate == NULL)
			{
				char	   *s = PQresultErrorField(res, PG_DIAG_SQLSTATE);

				if (s)
					*sqlstate = pg_strdup(s);
			}
			if (errmsg && *errmsg == NULL)
				*errmsg = pg_strdup(PQresultErrorMessage(res));
		}
		PQclear(res);
	}
	return ok;
}
