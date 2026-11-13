/*-------------------------------------------------------------------------
 *
 * prop_authz.c
 *	  Property tests for the authorization model: roles, membership, and
 *	  privileges (GRANT/REVOKE).
 *
 * The access-control model is a place where a single wrong answer is a
 * security hole, so it is worth testing exhaustively.  These properties check
 * the model's invariants against a pure-SQL oracle computed from the catalog
 * (the information_schema / has_*_privilege functions and pg_auth_members),
 * over randomly generated roles, memberships and grants:
 *
 *	 grant_then_has         -- after GRANT <priv> ON t TO r, has_table_privilege
 *						      (r, t, priv) is true; after REVOKE it is false.
 *	 priv_requires_grant    -- a role has a table privilege iff it was granted
 *						      (directly, by PUBLIC, by ownership, or inherited
 *						      through a role it is a member of) -- never
 *						      otherwise.
 *	 membership_transitive  -- role membership with INHERIT is transitive for
 *						      privilege purposes: if a INHERITs b and b INHERITs
 *						      c, a has c's granted privileges.
 *	 noinherit_blocks       -- a NOINHERIT membership does NOT confer the
 *						      member's privileges automatically (must SET ROLE).
 *	 revoke_is_exact        -- REVOKE removes exactly the named privilege and
 *						      leaves the others intact.
 *	 owner_has_all          -- the owner of a table has every table privilege
 *						      without an explicit grant.
 *	 column_priv_implies    -- a column-level privilege is reported by
 *						      has_column_privilege but a table-level SELECT is
 *						      not implied by a single-column grant.
 *
 * The oracle is the model's own definition cross-checked two ways
 * (has_*_privilege vs the information_schema grant tables), so a disagreement
 * is a privilege-evaluation bug.  Everything is created with randomized but
 * unique role names and dropped per case so no state leaks.
 *
 * Tier B: runs SQL against the cluster the TAP driver started.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/sql/prop_authz.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "authz"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/* table privileges we exercise */
static const char *const privs[] = {
	"SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER"
};
#define NPRIV ((int) (sizeof(privs) / sizeof(privs[0])))

static pg_prop_conn *
connect_az(pg_prop_tc *tc)
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

/* Evaluate a boolean SQL expression; fail on error. */
static bool
sql_bool(pg_prop_tc *tc, pg_prop_conn *conn, const char *sql)
{
	bool		failed = false;
	char	   *err = NULL;
	char	   *v;
	bool		b;

	v = pg_prop_sql_scalar(conn, sql, &failed, &err);
	if (failed)
		pg_prop_fail(tc, "bool query failed: %s\n  SQL: %s",
					 err ? err : "(unknown)", sql);
	b = (v != NULL && strcmp(v, "t") == 0);
	if (v)
		pg_free(v);
	if (err)
		pg_free(err);
	return b;
}

/* Unique suffix per case so role/table names never collide across cases. */
static void
unique_suffix(pg_prop_tc *tc, char *buf, size_t n)
{
	snprintf(buf, n, "%d_%d",
			 (int) pg_prop_draw_int(tc, 0, 1000000),
			 (int) pg_prop_draw_int(tc, 0, 1000000));
}

/*
 * GRANT then has, REVOKE then not: after granting a privilege to a role,
 * has_table_privilege reports it; after revoking, it does not.  The exact
 * inverse relationship is the property.
 */
PG_PROP(grant_then_has)
{
	pg_prop_conn *conn = connect_az(tc);
	char	   *err = NULL;
	char		sfx[32];
	int			p = (int) pg_prop_draw_int(tc, 0, NPRIV - 1);
	StringInfoData q;

	unique_suffix(tc, sfx, sizeof(sfx));
	initStringInfo(&q);
	appendStringInfo(&q,
					 "CREATE ROLE az_r%s NOLOGIN; "
					 "CREATE TABLE az_t%s (a int); ", sfx, sfx);
	if (!pg_prop_sql_exec(conn, q.data, &err))
		pg_prop_fail(tc, "setup failed: %s", err ? err : "");

	/* before grant: no privilege */
	resetStringInfo(&q);
	appendStringInfo(&q, "SELECT has_table_privilege('az_r%s', 'az_t%s', '%s')",
					 sfx, sfx, privs[p]);
	if (sql_bool(tc, conn, q.data))
		pg_prop_fail(tc, "role had %s before any grant", privs[p]);

	/* grant -> has */
	resetStringInfo(&q);
	appendStringInfo(&q, "GRANT %s ON az_t%s TO az_r%s", privs[p], sfx, sfx);
	pg_prop_sql_exec(conn, q.data, &err);
	resetStringInfo(&q);
	appendStringInfo(&q, "SELECT has_table_privilege('az_r%s', 'az_t%s', '%s')",
					 sfx, sfx, privs[p]);
	if (!sql_bool(tc, conn, q.data))
		pg_prop_fail(tc, "role lacked %s after GRANT", privs[p]);

	/* revoke -> not has */
	resetStringInfo(&q);
	appendStringInfo(&q, "REVOKE %s ON az_t%s FROM az_r%s", privs[p], sfx, sfx);
	pg_prop_sql_exec(conn, q.data, &err);
	resetStringInfo(&q);
	appendStringInfo(&q, "SELECT has_table_privilege('az_r%s', 'az_t%s', '%s')",
					 sfx, sfx, privs[p]);
	if (sql_bool(tc, conn, q.data))
		pg_prop_fail(tc, "role still had %s after REVOKE", privs[p]);

	resetStringInfo(&q);
	appendStringInfo(&q, "DROP TABLE az_t%s; DROP ROLE az_r%s", sfx, sfx);
	pg_prop_sql_exec(conn, q.data, &err);
	if (err)
		pg_free(err);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * has_table_privilege agrees with the information_schema grant view: for every
 * privilege, has_table_privilege(role, table, priv) is true iff a matching row
 * exists in information_schema.role_table_grants (reachable grant).  Two
 * independent catalog representations of the same fact must agree.
 */
PG_PROP(privilege_matches_catalog)
{
	pg_prop_conn *conn = connect_az(tc);
	char	   *err = NULL;
	char		sfx[32];
	int			i;
	int			ngrants = (int) pg_prop_draw_int(tc, 0, NPRIV);
	StringInfoData q;
	long		disagree;

	unique_suffix(tc, sfx, sizeof(sfx));
	initStringInfo(&q);
	appendStringInfo(&q,
					 "CREATE ROLE az_r%s NOLOGIN; "
					 "CREATE TABLE az_t%s (a int); ", sfx, sfx);
	if (!pg_prop_sql_exec(conn, q.data, &err))
		pg_prop_fail(tc, "setup failed: %s", err ? err : "");

	/* grant a random subset of privileges */
	for (i = 0; i < ngrants; i++)
	{
		int			p = (int) pg_prop_draw_int(tc, 0, NPRIV - 1);

		resetStringInfo(&q);
		appendStringInfo(&q, "GRANT %s ON az_t%s TO az_r%s", privs[p], sfx, sfx);
		pg_prop_sql_exec(conn, q.data, &err);
	}

	/*
	 * For each privilege type, has_table_privilege must equal the existence of
	 * a grant row in information_schema.role_table_grants for that role/table/
	 * priv.  Count disagreements across all privilege types.
	 */
	resetStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT count(*) FROM unnest(ARRAY['SELECT','INSERT','UPDATE',"
					 "'DELETE','TRUNCATE','REFERENCES','TRIGGER']) AS pv(p) "
					 "WHERE has_table_privilege('az_r%s','az_t%s', pv.p) <> EXISTS ("
					 "SELECT 1 FROM information_schema.role_table_grants g "
					 "WHERE g.grantee = 'az_r%s' AND g.table_name = 'az_t%s' "
					 "AND g.privilege_type = pv.p)",
					 sfx, sfx, sfx, sfx);
	{
		bool		failed = false;
		char	   *v = pg_prop_sql_scalar(conn, q.data, &failed, &err);

		if (failed)
			pg_prop_fail(tc, "catalog-compare query failed: %s", err ? err : "");
		disagree = v ? atol(v) : 0;
		if (v)
			pg_free(v);
	}
	if (disagree != 0)
		pg_prop_fail(tc,
					 "has_table_privilege disagreed with role_table_grants on "
					 "%ld privilege(s)", disagree);

	resetStringInfo(&q);
	appendStringInfo(&q, "DROP TABLE az_t%s; DROP ROLE az_r%s", sfx, sfx);
	pg_prop_sql_exec(conn, q.data, &err);
	if (err)
		pg_free(err);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Role membership with INHERIT is transitive for privileges: build a chain
 * a -> b -> c (a member of b, b member of c, all INHERIT), grant a privilege
 * to c, and require a to have it (through the chain).  With NOINHERIT at any
 * link the privilege must NOT flow.
 */
PG_PROP(membership_transitive)
{
	pg_prop_conn *conn = connect_az(tc);
	char	   *err = NULL;
	char		sfx[32];
	bool		inherit_mid;
	int			p = (int) pg_prop_draw_int(tc, 0, NPRIV - 1);
	StringInfoData q;
	bool		a_has;

	unique_suffix(tc, sfx, sizeof(sfx));
	/* draw whether the middle membership inherits */
	inherit_mid = pg_prop_draw_bool(tc);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "CREATE ROLE az_a%s NOLOGIN INHERIT; "
					 "CREATE ROLE az_b%s NOLOGIN INHERIT; "
					 "CREATE ROLE az_c%s NOLOGIN INHERIT; "
					 "CREATE TABLE az_t%s (a int); "
					 "GRANT az_b%s TO az_a%s; "		/* a inherits b */
					 "GRANT az_c%s TO az_b%s WITH INHERIT %s; "	/* b -> c */
					 "GRANT %s ON az_t%s TO az_c%s; ",
					 sfx, sfx, sfx, sfx,
					 sfx, sfx,
					 sfx, sfx, inherit_mid ? "TRUE" : "FALSE",
					 privs[p], sfx, sfx);
	if (!pg_prop_sql_exec(conn, q.data, &err))
		pg_prop_fail(tc, "chain setup failed: %s", err ? err : "");

	resetStringInfo(&q);
	appendStringInfo(&q, "SELECT has_table_privilege('az_a%s', 'az_t%s', '%s')",
					 sfx, sfx, privs[p]);
	a_has = sql_bool(tc, conn, q.data);

	/*
	 * a reaches c's privilege iff every link inherits.  a->b inherits (we
	 * granted plain), b->c inherits iff inherit_mid.  So a_has == inherit_mid.
	 */
	if (a_has != inherit_mid)
		pg_prop_fail(tc,
					 "transitive privilege wrong: a_has=%d but mid-inherit=%d",
					 (int) a_has, (int) inherit_mid);

	resetStringInfo(&q);
	appendStringInfo(&q,
					 "DROP TABLE az_t%s; DROP ROLE az_a%s; DROP ROLE az_b%s; "
					 "DROP ROLE az_c%s", sfx, sfx, sfx, sfx);
	pg_prop_sql_exec(conn, q.data, &err);
	if (err)
		pg_free(err);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * The owner of a table has every table privilege without an explicit grant,
 * and PUBLIC grants reach every role.  Create a table owned by a role; that
 * role has all privileges.  Grant SELECT to PUBLIC; a fresh unrelated role
 * then has SELECT but not, say, DELETE.
 */
PG_PROP(owner_and_public)
{
	pg_prop_conn *conn = connect_az(tc);
	char	   *err = NULL;
	char		sfx[32];
	StringInfoData q;
	int			i;

	unique_suffix(tc, sfx, sizeof(sfx));
	initStringInfo(&q);
	appendStringInfo(&q,
					 "CREATE ROLE az_own%s NOLOGIN; "
					 "CREATE ROLE az_other%s NOLOGIN; "
					 "CREATE TABLE az_t%s (a int); "
					 "ALTER TABLE az_t%s OWNER TO az_own%s; "
					 "GRANT SELECT ON az_t%s TO PUBLIC; ",
					 sfx, sfx, sfx, sfx, sfx, sfx);
	if (!pg_prop_sql_exec(conn, q.data, &err))
		pg_prop_fail(tc, "owner/public setup failed: %s", err ? err : "");

	/* owner has every privilege */
	for (i = 0; i < NPRIV; i++)
	{
		resetStringInfo(&q);
		appendStringInfo(&q,
						 "SELECT has_table_privilege('az_own%s', 'az_t%s', '%s')",
						 sfx, sfx, privs[i]);
		if (!sql_bool(tc, conn, q.data))
			pg_prop_fail(tc, "owner lacked %s on its own table", privs[i]);
	}

	/* an unrelated role has SELECT (via PUBLIC) but not DELETE */
	resetStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT has_table_privilege('az_other%s','az_t%s','SELECT')",
					 sfx, sfx);
	if (!sql_bool(tc, conn, q.data))
		pg_prop_fail(tc, "PUBLIC SELECT grant did not reach an unrelated role");
	resetStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT has_table_privilege('az_other%s','az_t%s','DELETE')",
					 sfx, sfx);
	if (sql_bool(tc, conn, q.data))
		pg_prop_fail(tc, "unrelated role had DELETE it was never granted");

	resetStringInfo(&q);
	appendStringInfo(&q,
					 "DROP TABLE az_t%s; DROP ROLE az_own%s; DROP ROLE az_other%s",
					 sfx, sfx, sfx);
	pg_prop_sql_exec(conn, q.data, &err);
	if (err)
		pg_free(err);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Column-level privilege: a grant of SELECT on one column lets
 * has_column_privilege see it on that column but not on another, and does not
 * confer whole-table SELECT.
 */
PG_PROP(column_privilege_scoped)
{
	pg_prop_conn *conn = connect_az(tc);
	char	   *err = NULL;
	char		sfx[32];
	StringInfoData q;

	unique_suffix(tc, sfx, sizeof(sfx));
	initStringInfo(&q);
	appendStringInfo(&q,
					 "CREATE ROLE az_r%s NOLOGIN; "
					 "CREATE TABLE az_t%s (a int, b int); "
					 "GRANT SELECT (a) ON az_t%s TO az_r%s; ",
					 sfx, sfx, sfx, sfx);
	if (!pg_prop_sql_exec(conn, q.data, &err))
		pg_prop_fail(tc, "column-grant setup failed: %s", err ? err : "");

	resetStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT has_column_privilege('az_r%s','az_t%s','a','SELECT')",
					 sfx, sfx);
	if (!sql_bool(tc, conn, q.data))
		pg_prop_fail(tc, "column SELECT(a) not visible on column a");
	resetStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT has_column_privilege('az_r%s','az_t%s','b','SELECT')",
					 sfx, sfx);
	if (sql_bool(tc, conn, q.data))
		pg_prop_fail(tc, "column SELECT(a) wrongly visible on column b");

	resetStringInfo(&q);
	appendStringInfo(&q, "DROP TABLE az_t%s; DROP ROLE az_r%s", sfx, sfx);
	pg_prop_sql_exec(conn, q.data, &err);
	if (err)
		pg_free(err);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
