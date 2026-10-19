/*-------------------------------------------------------------------------
 *
 * pg_prop.h
 *	  Property-based testing harness for PostgreSQL, built on libhegel.
 *
 * This is the single entry point for every property-based test binary under
 * src/test/prop.  It wraps the libhegel C ABI (hegel.h) in a small, plain-C
 * API so individual test files never touch the engine handles directly, and
 * so a test file reads like an ordinary C unit test.
 *
 * A property is a function:
 *
 *		static void prop_my_thing(pg_prop_tc *tc, void *arg)
 *		{
 *			int64 n = pg_prop_draw_int(tc, INT64_MIN, INT64_MAX);
 *			if (!my_invariant(n))
 *				pg_prop_fail(tc, "invariant violated for %lld",
 *							 (long long) n);
 *		}
 *
 * registered with PG_PROP() and run from the generated main().  The engine
 * generates many inputs, and on failure shrinks to a minimal counterexample
 * and prints a base64 blob that replays it exactly.
 *
 * Why a C API and not hegel-cpp: PostgreSQL is a C project maintained by C
 * programmers.  libhegel ships a stable C ABI; we bind to it directly.  The
 * only C++-ism anywhere in this tree is that the engine's own shared library
 * is built in Rust; nothing we write is C++.
 *
 * The isolate() facility (pg_prop_run_isolated) forks a child to run code
 * that may crash, abort() on a failed Assert, or exit() on a fatal error --
 * all common in frontend PostgreSQL code -- and turns any of those into an
 * ordinary property failure the engine can shrink.  Without it, a crash in
 * the code under test would take the whole test process down and lose the
 * counterexample.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/pg_prop.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_PROP_H
#define PG_PROP_H

#include "postgres_fe.h"

/*
 * Opaque per-test-case handle.  One is handed to each property invocation;
 * it must not be stored past the end of that invocation.
 */
typedef struct pg_prop_tc pg_prop_tc;

/* A property function: draws inputs from tc, checks, calls pg_prop_fail. */
typedef void (*pg_prop_fn) (pg_prop_tc *tc, void *arg);

/*
 * Register a property.  Use the PG_PROP() macro rather than calling this
 * directly.  "area" groups properties that share a target (and a binary
 * subcommand); "name" selects one on the command line and names it in the
 * example database as "area/name"; "fn" is the property body.
 */
extern void pg_prop_register(const char *area, const char *name,
							 pg_prop_fn fn, void *arg);

/*
 * Each prop_<area>.c defines PG_PROP_AREA to its area name before including
 * this header:
 *
 *		#define PG_PROP_AREA "path"
 *		#include "pg_prop.h"
 *
 * PG_PROP(ident) then defines and registers a property body under that area.
 * Run it as:  pg_prop <area> [options] [name ...]
 *
 * The harness translation unit (pg_prop.c) includes this header without
 * defining PG_PROP_AREA, which is fine -- it never uses PG_PROP().  Only a
 * use of PG_PROP() without an area is an error, enforced below.
 */
#ifndef PG_PROP_AREA
#define PG_PROP_AREA pg_prop_area_not_defined
#endif

#define PG_PROP(ident) \
	static void ident(pg_prop_tc *tc, void *arg); \
	static void pg_prop_reg_##ident(void) pg_attribute_constructor; \
	static void pg_prop_reg_##ident(void) \
	{ \
		pg_prop_register(PG_PROP_AREA, CppAsString(ident), ident, NULL); \
	} \
	static void ident(pg_prop_tc *tc, void *arg)

/*
 * Extension points (hooks).
 *
 * Two ways an extension can participate in this suite:
 *
 *	 (a) Add a whole new area.  Compile a prop_<ext>.c into the pg_prop binary
 *		 (see src/test/prop/README, "Extensions") that defines PG_PROP_AREA to
 *		 the extension's name and uses PG_PROP() exactly as a core area does.
 *		 Nothing else is needed: the constructor registers it and it appears as
 *		 its own subcommand.  CREATE EXTENSION in the area's connect helper.
 *
 *	 (b) Augment an existing area.  A core area may publish a named extension
 *		 point and call pg_prop_run_hooks() at it; an extension registers a
 *		 contributor with PG_PROP_HOOK(point).  The contributor receives the
 *		 point name and a void *ctx whose concrete type is documented by the
 *		 point.  For example, the SQL query generator (sql/query_gen.h)
 *		 publishes "query_gen.column" and "query_gen.predicate" so an
 *		 extension type can add columns and predicate leaves to the generated
 *		 tables and queries, after which every query-level oracle (TLP, NoREC,
 *		 cardinality, PQS, plan-advice) covers that type with no further work.
 *
 * A hook contributor:
 *
 *		static void my_ext_cols(pg_prop_tc *tc, const char *point, void *ctx)
 *		{
 *			qg_schema *s = ctx;		// concrete type for this point
 *			qg_add_column(s, "ck", "citext");
 *		}
 *		PG_PROP_HOOK("query_gen.column", my_ext_cols);
 */
typedef void (*pg_prop_hook_fn) (pg_prop_tc *tc, const char *point, void *ctx);

extern void pg_prop_register_hook(const char *point, pg_prop_hook_fn fn);
extern int	pg_prop_hooks_count(const char *point);
extern void pg_prop_run_hooks(pg_prop_tc *tc, const char *point, void *ctx);
extern void pg_prop_run_hook_n(pg_prop_tc *tc, const char *point, int n,
							   void *ctx);

/*
 * Register a hook contributor before main().  "point" is the extension-point
 * name; "fn" runs when the publishing area reaches that point.  Place these at
 * file scope in an extension's prop_<ext>.c.
 */
#define PG_PROP_HOOK(point, fn) \
	static void pg_prop_hookreg_##fn(void) pg_attribute_constructor; \
	static void pg_prop_hookreg_##fn(void) \
	{ \
		pg_prop_register_hook((point), (fn)); \
	} \
	struct pg_prop_hook_force_semicolon_##fn
#ifndef pg_attribute_constructor
#define pg_attribute_constructor __attribute__((constructor))
#endif

/*
 * Draw functions.  Each pulls one value from the engine's choice stream for
 * this test case.  Bounds are inclusive.  The engine deliberately favours
 * boundary values (min, max, 0, -1, 1), so pass the widest bounds the code
 * under test is supposed to accept -- narrowing them hides exactly the bugs
 * this tool exists to find.
 */
extern int64 pg_prop_draw_int(pg_prop_tc *tc, int64 min, int64 max);
extern double pg_prop_draw_float(pg_prop_tc *tc, double min, double max);
extern bool pg_prop_draw_bool(pg_prop_tc *tc);

/*
 * Draw a byte buffer of length in [min_size, max_size].  The result is
 * malloc'd and owned by the caller; free it.  *len receives the length.  The
 * buffer is NUL-terminated for convenience (one extra byte past *len) but may
 * contain interior NULs, so always use *len for the real content.
 */
extern unsigned char *pg_prop_draw_bytes(pg_prop_tc *tc, size_t min_size,
										  size_t max_size, size_t *len);

/*
 * Draw a UTF-8 string of length in [min_size, max_size] code points, drawn
 * from the full assigned Unicode range (not ASCII-only).  Caller frees.  *len
 * receives the byte length.  NUL-terminated; may contain interior NULs.
 */
extern char *pg_prop_draw_utf8(pg_prop_tc *tc, size_t min_size,
							   size_t max_size, size_t *len);

/*
 * Draw a string restricted to the given alphabet (a NUL-terminated C string
 * of allowed ASCII characters).  Useful when the code under test is only
 * meaningful on a restricted charset (identifiers, numeric text, ...).
 * Caller frees.
 */
extern char *pg_prop_draw_from(pg_prop_tc *tc, const char *alphabet,
							   size_t min_size, size_t max_size, size_t *len);

/*
 * Reject the current test case: its generated inputs do not satisfy a
 * precondition, so it should be discarded rather than counted.  Use sparingly
 * -- a high rejection rate trips a health check.  Prefer constructing valid
 * inputs directly over drawing-then-rejecting.
 */
extern void pg_prop_assume(pg_prop_tc *tc, bool condition);

/*
 * Skip the whole property: its precondition cannot be met in this environment
 * (a required extension or procedural language is not installed, a platform
 * feature is absent).  Unlike pg_prop_assume -- which discards one generated
 * case -- this abandons the entire property and reports it as skipped, with
 * "reason" shown as the TAP SKIP directive.  Call it at the top of the body,
 * before drawing inputs.  Does not return.
 */
extern pg_noreturn void pg_prop_skip(pg_prop_tc *tc, const char *fmt,...) pg_attribute_printf(2, 3);

/*
 * Attach a diagnostic note to the current case.  It is printed only when this
 * case is the final shrunk counterexample, so use it to record context the
 * drawn values alone don't show (e.g. an intermediate the property computed).
 */
extern void pg_prop_note(pg_prop_tc *tc, const char *fmt,...) pg_attribute_printf(2, 3);

/*
 * Fail the current test case.  This is how a property reports that its
 * invariant did not hold.  The message is the human-readable description of
 * the failure; it is printed with the counterexample.  Does not return.
 */
extern pg_noreturn void pg_prop_fail(pg_prop_tc *tc, const char *fmt,...) pg_attribute_printf(2, 3);

/*
 * Run "body" in a forked child.  Returns 0 if the child exited 0, otherwise a
 * non-zero code describing how it died.  *detail (if non-NULL) receives a
 * malloc'd human-readable description ("exited N", "signal N (SIGSEGV)", ...).
 *
 * Use this to wrap any call into code that may crash, abort() on a failed
 * assertion, or exit() -- so the failure becomes a value you can test and
 * pg_prop_fail on, with the drawn inputs still in hand to shrink.  Draw your
 * inputs in the parent (before calling this) so the engine sees them; the
 * child only runs the code under test.
 */
typedef void (*pg_prop_isolated_fn) (void *arg);
extern int	pg_prop_run_isolated(pg_prop_isolated_fn body, void *arg,
								 char **detail);

/*
 * SQL oracle support (Tier B).
 *
 * Backend code -- the planner, executor, type I/O -- cannot be linked into a
 * frontend binary, so properties that exercise it instead drive SQL against a
 * live cluster and compare the answer to an oracle (a reference query, or the
 * same query with an optimisation disabled).  The TAP driver t/002_sql_props.pl
 * starts one node and exports PGHOST/PGPORT; these helpers connect to it.
 *
 * A SQL area opens one connection in its property body (connections are cheap
 * against a local socket and keeping them per-case keeps state from leaking
 * between cases).  The handle is an opaque PGconn-wrapper so pg_prop.h need
 * not include libpq-fe.h.
 */
typedef struct pg_prop_conn pg_prop_conn;

/*
 * Ensure an extension (contrib module or procedural language) is available for
 * the session, loading it with CREATE EXTENSION IF NOT EXISTS.  Returns true
 * if it is now present.  If the extension is not installed on this server,
 * returns false; the caller then typically pg_prop_skip()s.  "conn" is an open
 * connection.  Example:
 *
 *		if (!pg_prop_require_extension(conn, "pageinspect"))
 *			pg_prop_skip(tc, "pageinspect not installed");
 */
extern bool pg_prop_require_extension(pg_prop_conn *conn, const char *extname);

/*
 * Connect to the test cluster named by PGHOST/PGPORT/PGDATABASE in the
 * environment.  Returns NULL and sets *err (malloc'd) on failure; the caller
 * typically pg_prop_fail()s on NULL.  Close with pg_prop_disconnect.
 */
extern pg_prop_conn *pg_prop_connect(char **err);
extern void pg_prop_disconnect(pg_prop_conn *conn);

/*
 * Run "sql" and return its single-row, single-column result as a malloc'd
 * string (caller frees), or NULL for SQL NULL.  On a SQL error, *errmsg (if
 * non-NULL) receives a malloc'd message and the function returns NULL with
 * *failed set true; callers distinguish a genuine NULL result from an error
 * via *failed.  Use pg_prop_sql_scalar for the common one-value oracle.
 */
extern char *pg_prop_sql_scalar(pg_prop_conn *conn, const char *sql,
								bool *failed, char **errmsg);

/*
 * Run "sql" expecting a result set, and return the whole thing as one newline-
 * joined, row-major string (every value rendered, NULLs as "\\N"), suitable for
 * comparing two queries that should return identical rows in identical order.
 * Caller frees.  *failed / *errmsg as above.  The caller is responsible for
 * ordering (add ORDER BY) when it wants order-insensitive comparison use a
 * sorted projection.
 */
extern char *pg_prop_sql_rows(pg_prop_conn *conn, const char *sql,
							  bool *failed, char **errmsg);

/*
 * Like pg_prop_sql_rows, but return an md5 digest of the whole result
 * (computed server-side) instead of the full rendered text.  Use this for
 * oracles over large/deep results: the client holds only a 32-char hash, so a
 * result that would overflow the 1 GB StringInfo cap when rendered
 * client-side (the deep generators can produce such results) still compares in
 * O(1) client memory.  Two queries that return the same rows in the same order
 * return the same digest; the caller supplies the ORDER BY that fixes the
 * order.  Caller frees; *failed / *errmsg as above.
 */
extern char *pg_prop_sql_rowhash(pg_prop_conn *conn, const char *sql,
								 bool *failed, char **errmsg);

/*
 * Like pg_prop_sql_rowhash but order-insensitive: the per-row text is sorted
 * server-side before hashing, so two queries that return the same *multiset*
 * of rows compare equal regardless of row order.  Use this for any oracle
 * that compares results across plans or query rewrites: SQL never guarantees
 * row order without ORDER BY, and even with ORDER BY a tie among equal sort
 * keys (equal numeric scales, a nondeterministic collation, a FULL/RIGHT join
 * that null-extends every key) leaves the order unspecified.  Comparing
 * multisets removes that whole class of false positive (false_positives.md
 * 2.1-2.2) and lets the generator emit shapes whose output has no total order.
 * Counts and duplicates are preserved (it is a multiset, not a set).
 */
extern char *pg_prop_sql_rowhash_unordered(pg_prop_conn *conn, const char *sql,
										   bool *failed, char **errmsg);

/*
 * Run "sql" for side effects (DDL/DML), returning true on success.  On error
 * returns false and sets *errmsg (malloc'd).
 */
extern bool pg_prop_sql_exec(pg_prop_conn *conn, const char *sql, char **errmsg);

/* "area/property" currently running (NULL outside a run); for diagnostics. */
extern const char *pg_prop_current_area;

/*
 * Asynchronous statement support, for properties that need two sessions in
 * flight at once (e.g. provoking a deadlock, where both sessions must be
 * blocked before either can resolve).  pg_prop_sql_send dispatches "sql"
 * without waiting (returns false if it could not be sent); pg_prop_sql_await
 * then blocks for the result, returning true on success and, on error, the
 * SQLSTATE via *sqlstate (malloc'd 5-char code, or NULL) so the caller can
 * tell a deadlock (40P01) from a serialization failure (40001) or a lock
 * timeout (55P03).  Use pg_prop_sql_send on both connections, then await each.
 */
extern bool pg_prop_sql_send(pg_prop_conn *conn, const char *sql);
extern bool pg_prop_sql_await(pg_prop_conn *conn, char **sqlstate, char **errmsg);

/*
 * Close any connections still open at the end of a test case.  A property that
 * fails or is assumed away unwinds by longjmp, jumping over its own
 * pg_prop_disconnect(); the driver calls this after every case so a leaked
 * connection cannot accumulate across the thousands of cases in a run (which
 * would otherwise exhaust the server's connection slots).  Defined in
 * pg_prop_sql.c; a no-op if no SQL area ever connected.
 */
extern void pg_prop_sql_cleanup(void);

#endif							/* PG_PROP_H */
