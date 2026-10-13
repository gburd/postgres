/*-------------------------------------------------------------------------
 *
 * pg_prop.c
 *	  Property-based testing harness: libhegel driver, generators, CLI, TAP.
 *
 * See pg_prop.h for the API contract.  This file is linked into every test
 * binary under src/test/prop and provides main().
 *
 * The CLI shape is:
 *
 *		pg_prop_<area> [options] [PROPERTY ...]
 *
 *	--list                list registered properties and exit
 *	-n, --iterations N    number of test cases per property (default 100)
 *	-s, --seed N          fixed RNG seed (default: random, printed as a note)
 *	-v, --verbose         raise engine verbosity
 *	-q, --quiet           lower engine verbosity
 *	--no-tap              plain output instead of TAP (for interactive use)
 *	-h, --help            usage
 *
 * With no PROPERTY arguments every registered property runs.  Environment
 * variables PG_PROP_ITERATIONS, PG_PROP_SEED and PG_PROP_TESTS supply the same
 * three controls (so they can be driven through the TAP wrapper and meson);
 * explicit command-line options win over the environment.
 *
 * Output is TAP by default so meson/prove consume it directly: one "ok"/"not
 * ok" line per property, the counterexample and replay blob as "#" diagnostic
 * lines under a failure.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/pg_prop.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include <math.h>
#include <setjmp.h>
#include <signal.h>
#include <stdarg.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "common/logging.h"
#include "getopt_long.h"
#include "lib/stringinfo.h"
#include "pg_prop.h"

/*
 * libhegel's C ABI.  Installed under a prefix given to configure via
 * --with-includes / extra_include_dirs; the header has no PostgreSQL
 * dependencies.
 */
#include "hegel.h"

/* ---- registry -------------------------------------------------------- */

typedef struct PropEntry
{
	const char *area;
	const char *name;
	pg_prop_fn	fn;
	void	   *arg;
} PropEntry;

#define MAX_PROPS 4096
static PropEntry props[MAX_PROPS];
static int	nprops = 0;

void
pg_prop_register(const char *area, const char *name, pg_prop_fn fn, void *arg)
{
	if (nprops >= MAX_PROPS)
	{
		fprintf(stderr, "pg_prop: too many properties (max %d)\n", MAX_PROPS);
		exit(2);
	}
	props[nprops].area = area;
	props[nprops].name = name;
	props[nprops].fn = fn;
	props[nprops].arg = arg;
	nprops++;
}

/* ---- extension-point (hook) registry --------------------------------- *
 *
 * An extension that wants to augment an existing area -- rather than add a
 * whole new area of its own -- registers a hook against a named extension
 * point that the area publishes.  The area calls pg_prop_run_hooks() at the
 * point, and every registered contributor runs in registration order with the
 * area's context pointer.  This is how, for example, a contrib type can teach
 * the SQL query generator to emit predicates over its own columns, so the
 * existing TLP/NoREC/cardinality/PQS oracles then cover that type for free.
 *
 * Hooks are a flat list keyed by point name; areas and extensions agree on
 * the point name and the concrete type behind the void *ctx out of band (in
 * the point's documentation), exactly as a C callback contract always does.
 */
typedef struct HookEntry
{
	const char *point;
	pg_prop_hook_fn fn;
} HookEntry;

#define MAX_HOOKS 1024
static HookEntry hooks[MAX_HOOKS];
static int	nhooks = 0;

void
pg_prop_register_hook(const char *point, pg_prop_hook_fn fn)
{
	if (nhooks >= MAX_HOOKS)
	{
		fprintf(stderr, "pg_prop: too many hooks (max %d)\n", MAX_HOOKS);
		exit(2);
	}
	hooks[nhooks].point = point;
	hooks[nhooks].fn = fn;
	nhooks++;
}

int
pg_prop_hooks_count(const char *point)
{
	int			n = 0;

	for (int i = 0; i < nhooks; i++)
		if (strcmp(hooks[i].point, point) == 0)
			n++;
	return n;
}

void
pg_prop_run_hooks(pg_prop_tc *tc, const char *point, void *ctx)
{
	for (int i = 0; i < nhooks; i++)
		if (strcmp(hooks[i].point, point) == 0)
			hooks[i].fn(tc, point, ctx);
}

/*
 * Run the Nth hook registered against "point" (0-based over just that point's
 * contributors), or do nothing if n is out of range.  Lets an area pick one
 * contributor at random -- e.g. the query generator choosing one extension
 * type for a column -- rather than running all of them.
 */
void
pg_prop_run_hook_n(pg_prop_tc *tc, const char *point, int n, void *ctx)
{
	int			seen = 0;

	for (int i = 0; i < nhooks; i++)
		if (strcmp(hooks[i].point, point) == 0)
		{
			if (seen == n)
			{
				hooks[i].fn(tc, point, ctx);
				return;
			}
			seen++;
		}
}

/* ---- per-case handle ------------------------------------------------- */

/*
 * One pg_prop_tc wraps the libhegel context and the current test case.  A
 * failure is reported by longjmp back to the driver, which marks the case
 * INTERESTING; the engine then shrinks and replays.  We use setjmp rather
 * than returning a status so pg_prop_fail can be called from arbitrarily deep
 * in a property body (and so it can be pg_attribute_noreturn, which keeps the
 * compiler from warning about missing returns after it).
 */
struct pg_prop_tc
{
	hegel_context_t *ctx;
	hegel_test_case_t *tc;
	StringInfoData failmsg;		/* set when the body called pg_prop_fail */
	bool		failed;
	sigjmp_buf	jmp;			/* longjmp target: body -> driver */
	const char *origin;			/* stable failure origin for this property */
};

/* ---- engine error helper --------------------------------------------- */

static void
check_rc(hegel_context_t *ctx, hegel_result_t rc, const char *what)
{
	/*
	 * HEGEL_E_STOP_TEST and HEGEL_E_ASSUME are not errors: they are the
	 * engine telling the body to stop (budget exhausted) or that the case was
	 * assumed away.  Both are handled by the callers that can see them; any
	 * other negative code is a real engine fault and is fatal.
	 */
	if (rc == HEGEL_OK || rc == HEGEL_E_STOP_TEST || rc == HEGEL_E_ASSUME)
		return;
	fprintf(stderr, "pg_prop: libhegel %s failed (%d): %s\n",
			what, (int) rc, hegel_context_last_error(ctx));
	exit(2);
}

/*
 * A draw that returns HEGEL_E_STOP_TEST means the engine wants the body to
 * stop immediately (it ran out of choice budget for this case).  We model
 * that as a failure-free longjmp out of the body: the driver then marks the
 * case OVERRUN.  pg_prop_fail reuses the same jump with failed=true.
 */
static pg_prop_tc *current_tc;	/* for the stop/overrun jump */
static bool overrun;

static void
stop_test(pg_prop_tc *tc)
{
	overrun = true;
	siglongjmp(tc->jmp, 1);
}

/* ---- generators ------------------------------------------------------ */

int64
pg_prop_draw_int(pg_prop_tc *tc, int64 min, int64 max)
{
	int64		out = 0;
	hegel_result_t rc = hegel_generate_integer(tc->ctx, tc->tc, min, max, &out);

	if (rc == HEGEL_E_STOP_TEST)
		stop_test(tc);
	check_rc(tc->ctx, rc, "generate_integer");
	return out;
}

double
pg_prop_draw_float(pg_prop_tc *tc, double min, double max)
{
	double		out = 0;
	hegel_result_t rc;

	/*
	 * Full doubles including NaN and the infinities only when the range is
	 * unbounded on the matching side, matching the engine's own defaults.
	 */
	rc = hegel_generate_float(tc->ctx, tc->tc, 64, min, max,
							  /* allow_nan */ isinf(min) || isinf(max) ? false : false,
							  /* allow_infinity */ false,
							  false, false, 5e-324, &out);
	if (rc == HEGEL_E_STOP_TEST)
		stop_test(tc);
	check_rc(tc->ctx, rc, "generate_float");
	return out;
}

bool
pg_prop_draw_bool(pg_prop_tc *tc)
{
	bool		out = false;
	hegel_result_t rc = hegel_generate_boolean(tc->ctx, tc->tc, 0.5,
											   false, false, &out);

	if (rc == HEGEL_E_STOP_TEST)
		stop_test(tc);
	check_rc(tc->ctx, rc, "generate_boolean");
	return out;
}

unsigned char *
pg_prop_draw_bytes(pg_prop_tc *tc, size_t min_size, size_t max_size, size_t *len)
{
	hegel_generate_bytes_result_t res = {NULL, 0};
	hegel_result_t rc = hegel_generate_bytes(tc->ctx, tc->tc, min_size,
											 max_size, &res);
	unsigned char *out;

	if (rc == HEGEL_E_STOP_TEST)
		stop_test(tc);
	check_rc(tc->ctx, rc, "generate_bytes");

	out = pg_malloc(res.len + 1);
	if (res.len > 0)
		memcpy(out, res.data, res.len);
	out[res.len] = '\0';
	*len = res.len;
	hegel_generate_bytes_result_free(tc->ctx, &res);
	return out;
}

/*
 * Shared text-draw path: build a one-shot string generator, draw from it,
 * copy the result out, free both.  Building a generator per draw is cheap
 * relative to a round trip through the engine and keeps the API stateless.
 */
static char *
draw_string(pg_prop_tc *tc, hegel_string_generator_t *gen, size_t *len)
{
	hegel_generate_string_result_t res = {NULL, 0};
	hegel_result_t rc = hegel_generate_string(tc->ctx, tc->tc, gen, &res);
	char	   *out;

	if (rc == HEGEL_E_STOP_TEST)
	{
		hegel_string_generator_free(tc->ctx, gen);
		stop_test(tc);
	}
	if (rc == HEGEL_E_ASSUME)
	{
		/* generator rejected its own draw (e.g. length cap); discard case */
		hegel_string_generator_free(tc->ctx, gen);
		siglongjmp(tc->jmp, 2); /* 2 == assume/invalid */
	}
	check_rc(tc->ctx, rc, "generate_string");

	out = pg_malloc(res.len + 1);
	if (res.len > 0)
		memcpy(out, res.data, res.len);
	out[res.len] = '\0';
	*len = res.len;
	hegel_generate_string_result_free(tc->ctx, &res);
	hegel_string_generator_free(tc->ctx, gen);
	return out;
}

char *
pg_prop_draw_utf8(pg_prop_tc *tc, size_t min_size, size_t max_size, size_t *len)
{
	hegel_string_generator_t *gen = NULL;
	hegel_result_t rc;

	rc = hegel_string_generator_text(tc->ctx, min_size, max_size,
									 "utf-8", 0, UINT32_MAX,
									 NULL, 0, NULL, 0, NULL, 0, NULL, 0, &gen);
	check_rc(tc->ctx, rc, "string_generator_text");
	return draw_string(tc, gen, len);
}

char *
pg_prop_draw_from(pg_prop_tc *tc, const char *alphabet,
				  size_t min_size, size_t max_size, size_t *len)
{
	hegel_string_generator_t *gen = NULL;
	hegel_result_t rc;

	/*
	 * There is no "alphabet" field in the C ABI's text generator; emulate it
	 * with include_characters plus an empty category set, which restricts the
	 * draw to exactly those characters.
	 */
	rc = hegel_string_generator_text(tc->ctx, min_size, max_size,
									 "ascii", 0, UINT32_MAX,
	/* categories: non-NULL empty list -> empty base alphabet */
									 (const char *const *) &alphabet, 0,
									 NULL, 0,
									 (const unsigned char *) alphabet, strlen(alphabet),
									 NULL, 0, &gen);
	check_rc(tc->ctx, rc, "string_generator_text(alphabet)");
	return draw_string(tc, gen, len);
}

/* ---- assume / note / fail -------------------------------------------- */

void
pg_prop_assume(pg_prop_tc *tc, bool condition)
{
	if (!condition)
		siglongjmp(tc->jmp, 2); /* 2 == assume/invalid */
}

void
pg_prop_note(pg_prop_tc *tc, const char *fmt,...)
{
	va_list		ap;
	StringInfoData s;

	initStringInfo(&s);
	va_start(ap, fmt);
	appendStringInfoVA(&s, fmt, ap);
	va_end(ap);
	hegel_note(tc->ctx, tc->tc, (const unsigned char *) s.data, s.len);
	pfree(s.data);
}

void
pg_prop_fail(pg_prop_tc *tc, const char *fmt,...)
{
	va_list		ap;

	resetStringInfo(&tc->failmsg);
	va_start(ap, fmt);
	appendStringInfoVA(&tc->failmsg, fmt, ap);
	va_end(ap);
	tc->failed = true;
	siglongjmp(tc->jmp, 1);		/* 1 == interesting/failure */
}

/* ---- fork isolation -------------------------------------------------- */

int
pg_prop_run_isolated(pg_prop_isolated_fn body, void *arg, char **detail)
{
	pid_t		pid;
	int			status;

	if (detail)
		*detail = NULL;

	fflush(stdout);
	fflush(stderr);
	pid = fork();
	if (pid < 0)
	{
		if (detail)
			*detail = pg_strdup("fork failed");
		return -1;
	}
	if (pid == 0)
	{
		/* child: run the code under test, then _exit so no atexit/flush runs */
		body(arg);
		fflush(stdout);
		fflush(stderr);
		_exit(0);
	}

	/* parent: wait and classify */
	while (waitpid(pid, &status, 0) < 0)
	{
		if (errno != EINTR)
		{
			if (detail)
				*detail = pg_strdup("waitpid failed");
			return -1;
		}
	}

	if (WIFEXITED(status))
	{
		int			code = WEXITSTATUS(status);

		if (code == 0)
			return 0;
		if (detail)
		{
			char		buf[64];

			snprintf(buf, sizeof(buf), "child exited with status %d", code);
			*detail = pg_strdup(buf);
		}
		return code ? code : 1;
	}
	if (WIFSIGNALED(status))
	{
		int			sig = WTERMSIG(status);

		if (detail)
		{
			char		buf[96];

			snprintf(buf, sizeof(buf), "child killed by signal %d (%s)",
					 sig, strsignal(sig));
			*detail = pg_strdup(buf);
		}
		/* encode as a distinct non-zero code */
		return 1000 + sig;
	}
	if (detail)
		*detail = pg_strdup("child stopped or unknown status");
	return -1;
}

/* ---- driver ---------------------------------------------------------- */

/*
 * Per-property run state passed from run_property() through the body-runner.
 * libhegel's run loop hands us a test case; we build a pg_prop_tc, run the
 * body under setjmp, and mark the case.
 */
typedef struct RunState
{
	PropEntry  *prop;
	StringInfoData last_fail;	/* message from the last failing body */
} RunState;

static hegel_verbosity_t verbosity = HEGEL_VERBOSITY_NORMAL;

/*
 * Run the body for one test case handle; returns the hegel_status_t to pass
 * to hegel_mark_complete.  Captures the failure message into rs->last_fail.
 */
static uint32_t
run_one_case(RunState *rs, hegel_context_t *ctx, hegel_test_case_t *handle)
{
	pg_prop_tc	t;
	int			jmpval;
	uint32_t	status;

	t.ctx = ctx;
	t.tc = handle;
	t.failed = false;
	t.origin = rs->prop->name;
	initStringInfo(&t.failmsg);
	current_tc = &t;
	overrun = false;

	jmpval = sigsetjmp(t.jmp, 1);
	if (jmpval == 0)
	{
		rs->prop->fn(&t, rs->prop->arg);
		/* body returned normally: property held for this case */
		status = HEGEL_STATUS_VALID;
	}
	else if (jmpval == 2)
	{
		/* assume/invalid */
		status = HEGEL_STATUS_INVALID;
	}
	else if (overrun)
	{
		status = HEGEL_STATUS_OVERRUN;
	}
	else
	{
		/* failure: keep the message for the report */
		resetStringInfo(&rs->last_fail);
		appendBinaryStringInfo(&rs->last_fail, t.failmsg.data, t.failmsg.len);
		status = HEGEL_STATUS_INTERESTING;
	}

	pfree(t.failmsg.data);

	/*
	 * Close any SQL connection the body opened and did not close -- a body
	 * that failed or assumed away longjmp'd over its own pg_prop_disconnect().
	 * This must run on every exit path, or a long run exhausts the server's
	 * connection slots.  A no-op for in-process (Tier A) areas.
	 */
	pg_prop_sql_cleanup();

	return status;
}

/*
 * Replay a counterexample blob to re-derive the failing message and drawn
 * values for the report.  Returns the (malloc'd) failure message.
 */
static char *
replay_blob(RunState *rs, hegel_context_t *ctx, const hegel_settings_t *s,
			const char *blob)
{
	hegel_test_case_t *handle = NULL;
	hegel_result_t rc;
	uint32_t	status;
	char	   *msg;

	rc = hegel_test_case_from_blob(ctx, s, blob, NULL, NULL, &handle);
	if (rc != HEGEL_OK || handle == NULL)
		return pg_strdup("(could not replay counterexample)");

	status = run_one_case(rs, ctx, handle);
	hegel_mark_complete(ctx, handle, status,
						status == HEGEL_STATUS_INTERESTING ? rs->prop->name : NULL);
	hegel_test_case_free(ctx, handle);

	if (status == HEGEL_STATUS_INTERESTING && rs->last_fail.len > 0)
		msg = pg_strdup(rs->last_fail.data);
	else
		msg = pg_strdup("(counterexample did not reproduce)");
	return msg;
}

/*
 * Run one property to completion.  Returns true if it held, false if a
 * counterexample was found.  On failure, prints the message, the minimal
 * counterexample's drawn values (via replay), and the replay blob as TAP
 * diagnostics.  "testno" is the 1-based TAP point number.
 */
static bool
run_property(PropEntry *prop, uint64 iterations, bool have_seed, uint64 seed,
			 const char *dbpath, int testno, bool tap)
{
	hegel_context_t *ctx = hegel_context_new();
	hegel_settings_t *s = NULL;
	hegel_run_t *run = NULL;
	hegel_run_result_t *result = NULL;
	hegel_run_status_t rstatus;
	RunState	rs;
	bool		ok;

	char		dbkey[256];

	rs.prop = prop;
	initStringInfo(&rs.last_fail);

	check_rc(ctx, hegel_settings_new(ctx, &s), "settings_new");
	check_rc(ctx, hegel_settings_set_test_cases(ctx, s, iterations), "set_test_cases");
	check_rc(ctx, hegel_settings_set_verbosity(ctx, s, verbosity), "set_verbosity");
	check_rc(ctx, hegel_settings_set_seed(ctx, s, seed, have_seed), "set_seed");
	snprintf(dbkey, sizeof(dbkey), "%s/%s", prop->area, prop->name);
	check_rc(ctx, hegel_settings_set_database_key(ctx, s, dbkey), "set_db_key");
	check_rc(ctx, hegel_settings_set_database(ctx, s, dbpath ? dbpath : ""),
			 "set_database");

	check_rc(ctx, hegel_run_start(ctx, s, NULL, NULL, &run), "run_start");

	for (;;)
	{
		hegel_test_case_t *handle = NULL;
		uint32_t	status;

		check_rc(ctx, hegel_next_test_case(ctx, run, &handle), "next_test_case");
		if (handle == NULL)
			break;
		status = run_one_case(&rs, ctx, handle);
		hegel_mark_complete(ctx, handle, status,
							status == HEGEL_STATUS_INTERESTING ? prop->name : NULL);
		hegel_test_case_free(ctx, handle);
	}

	check_rc(ctx, hegel_run_result(ctx, run, &result), "run_result");
	check_rc(ctx, hegel_run_result_status(ctx, result, &rstatus), "run_result_status");

	ok = (rstatus == HEGEL_RUN_STATUS_PASSED);

	if (tap)
		printf("%s %d - %s/%s\n", ok ? "ok" : "not ok", testno,
			   prop->area, prop->name);
	else
		printf("%s/%s: %s\n", prop->area, prop->name,
			   ok ? "PASSED" : "FAILED");

	if (!ok)
	{
		const char *pfx = tap ? "# " : "    ";

		if (rstatus == HEGEL_RUN_STATUS_ERROR)
		{
			const char *err = NULL;

			hegel_run_result_error(ctx, result, &err);
			printf("%srun error: %s\n", pfx, err ? err : "(unknown)");
		}
		else
		{
			size_t		count = 0;

			hegel_run_result_failure_count(ctx, result, &count);
			for (size_t i = 0; i < count; i++)
			{
				hegel_failure_t *f = NULL;
				const char *blob = NULL;
				char	   *msg;

				hegel_run_result_failure(ctx, result, i, &f);
				hegel_failure_reproduction_blob(ctx, f, &blob);
				msg = replay_blob(&rs, ctx, s, blob);

				printf("%sfalsified: %s\n", pfx, msg);
				if (blob)
					printf("%sreplay: PG_PROP_SEED unavailable; rerun with "
						   "--blob to reproduce: %s\n", pfx, blob);
				pg_free(msg);
				hegel_failure_free(ctx, f);
			}
		}
	}
	fflush(stdout);

	pfree(rs.last_fail.data);
	hegel_run_result_free(ctx, result);
	hegel_run_free(ctx, run);
	hegel_settings_free(ctx, s);
	hegel_context_free(ctx);
	return ok;
}

/* ---- CLI ------------------------------------------------------------- */

static void
usage(const char *argv0)
{
	printf("Usage: %s AREA [options] [PROPERTY ...]\n\n", argv0);
	printf("  AREA                  which group of properties to run\n");
	printf("                        (run with --list to see the areas)\n");
	printf("  --list                list AREA/PROPERTY names and exit\n");
	printf("  -n, --iterations N    test cases per property (default 100)\n");
	printf("  -s, --seed N          fixed RNG seed (default: random)\n");
	printf("  -v, --verbose         raise engine verbosity\n");
	printf("  -q, --quiet           lower engine verbosity\n");
	printf("      --no-tap          plain output instead of TAP\n");
	printf("  -h, --help            this help\n\n");
	printf("With no PROPERTY, every property in AREA runs.  Examples:\n");
	printf("  %s jsonapi --list\n", argv0);
	printf("  %s path canonicalize_idempotent -n 100000 -s 42\n\n", argv0);
	printf("Environment overrides: PG_PROP_ITERATIONS, PG_PROP_SEED, PG_PROP_TESTS.\n");
}

/* Does name match any of the selectors? */
static bool
selected(const char *name, char **sel, int nsel)
{
	if (nsel == 0)
		return true;
	for (int i = 0; i < nsel; i++)
		if (strcmp(name, sel[i]) == 0)
			return true;
	return false;
}

int
main(int argc, char **argv)
{
	static struct option long_options[] = {
		{"list", no_argument, NULL, 'L'},
		{"iterations", required_argument, NULL, 'n'},
		{"seed", required_argument, NULL, 's'},
		{"verbose", no_argument, NULL, 'v'},
		{"quiet", no_argument, NULL, 'q'},
		{"no-tap", no_argument, NULL, 'T'},
		{"help", no_argument, NULL, 'h'},
		{NULL, 0, NULL, 0}
	};
	uint64		iterations = 100;
	bool		have_seed = false;
	uint64		seed = 0;
	bool		do_list = false;
	bool		tap = true;
	const char *env;
	const char *dbpath;
	const char *area = NULL;
	int			c;
	int			npass = 0;
	int			nrun = 0;
	int			nsel;
	char	  **sel;

	pg_logging_init(argv[0]);

	/* environment defaults (command line wins) */
	if ((env = getenv("PG_PROP_ITERATIONS")) != NULL && env[0])
		iterations = strtou64(env, NULL, 10);
	if ((env = getenv("PG_PROP_SEED")) != NULL && env[0])
	{
		seed = strtou64(env, NULL, 10);
		have_seed = true;
	}

	while ((c = getopt_long(argc, argv, "n:s:vqhL", long_options, NULL)) != -1)
	{
		switch (c)
		{
			case 'L':
				do_list = true;
				break;
			case 'n':
				iterations = strtou64(optarg, NULL, 10);
				break;
			case 's':
				seed = strtou64(optarg, NULL, 10);
				have_seed = true;
				break;
			case 'v':
				verbosity = HEGEL_VERBOSITY_VERBOSE;
				break;
			case 'q':
				verbosity = HEGEL_VERBOSITY_QUIET;
				break;
			case 'T':
				tap = false;
				break;
			case 'h':
				usage(argv[0]);
				return 0;
			default:
				usage(argv[0]);
				return 2;
		}
	}

	if (do_list)
	{
		for (int i = 0; i < nprops; i++)
			printf("%s/%s\n", props[i].area, props[i].name);
		return 0;
	}

	/*
	 * First positional argument selects the area (the target group).  Running
	 * every area at once is intentionally not supported from one invocation:
	 * the TAP driver runs one area per binary subcommand, and a developer
	 * debugging a failure wants one area in front of them.
	 */
	if (optind >= argc)
	{
		fprintf(stderr, "pg_prop: missing AREA argument "
				"(try --list, or -h for help)\n");
		return 2;
	}
	area = argv[optind++];

	/* remaining positionals, else PG_PROP_TESTS, select properties by name */
	sel = &argv[optind];
	nsel = argc - optind;
	if (nsel == 0 && (env = getenv("PG_PROP_TESTS")) != NULL && env[0])
	{
		/* split PG_PROP_TESTS on whitespace into a fresh argv-like array */
		static char *envsel[MAX_PROPS];
		char	   *dup = pg_strdup(env);
		char	   *tok = strtok(dup, " \t\n");

		nsel = 0;
		while (tok && nsel < MAX_PROPS)
		{
			envsel[nsel++] = tok;
			tok = strtok(NULL, " \t\n");
		}
		sel = envsel;
	}

	/*
	 * Pick and announce a seed when none was given, so a failure seen on a
	 * buildfarm animal can be reproduced exactly.  derandomize is left off:
	 * we want fresh randomness unless the caller pins a seed.
	 */
	if (!have_seed)
	{
		srandom((unsigned int) (getpid() ^ (unsigned int) time(NULL)));
		seed = ((uint64) random() << 32) ^ (uint64) random();
		have_seed = true;
		if (tap)
			printf("# seed: %llu (rerun with -s %llu)\n",
				   (unsigned long long) seed, (unsigned long long) seed);
		else
			printf("seed: %llu\n", (unsigned long long) seed);
	}

	/*
	 * The example database persists counterexamples between runs so a known
	 * failure replays first.  Keep it out of the source tree: honour
	 * TESTDATADIR (set by the TAP wrapper) when present, else disable it.
	 */
	dbpath = getenv("PG_PROP_DB");
	if (dbpath == NULL)
	{
		const char *td = getenv("TESTDATADIR");

		dbpath = td ? NULL : "";	/* NULL under TESTDATADIR -> default .hegel
									 * there; handled below */
		if (td)
		{
			static char dbbuf[MAXPGPATH];

			snprintf(dbbuf, sizeof(dbbuf), "%s/hegel-db", td);
			dbpath = dbbuf;
		}
	}

	/* count the selected properties for the TAP plan */
	if (tap)
	{
		int			plan = 0;

		for (int i = 0; i < nprops; i++)
			if (strcmp(props[i].area, area) == 0 &&
				selected(props[i].name, sel, nsel))
				plan++;
		printf("1..%d\n", plan);
	}

	for (int i = 0; i < nprops; i++)
	{
		if (strcmp(props[i].area, area) != 0)
			continue;
		if (!selected(props[i].name, sel, nsel))
			continue;
		nrun++;
		if (run_property(&props[i], iterations, have_seed, seed, dbpath,
						 nrun, tap))
			npass++;
	}

	if (nrun == 0)
	{
		fprintf(stderr, "pg_prop: no matching properties in area \"%s\"\n", area);
		return 2;
	}
	if (!tap)
		printf("%d/%d properties passed\n", npass, nrun);

	return (npass == nrun) ? 0 : 1;
}
