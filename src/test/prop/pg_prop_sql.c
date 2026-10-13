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
	ExecStatusType st = PQresultStatus(res);
	char	   *out;

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
	ExecStatusType st = PQresultStatus(res);
	StringInfoData s;
	int			nrows,
				ncols;

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

bool
pg_prop_sql_exec(pg_prop_conn *conn, const char *sql, char **errmsg)
{
	PGresult   *res = PQexec(conn->conn, sql);
	ExecStatusType st = PQresultStatus(res);

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
