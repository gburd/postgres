# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/bin/pgbench/t/001_pgbench_with_server.pl.

Exercises pgbench against a live server: initialization (client- and
server-side data generation, partitions, tablespaces, foreign keys), builtin
and custom scripts, deterministic seeded expressions/permute checks, extended
query parameter logging, \\gset/\\aset, pipelines, a large table of expression
and meta-command errors, log sampling, retry on serialization/deadlock errors,
and --exit-on-abort / --continue-on-error.
"""

# The script constants below embed pgbench scripts verbatim from the Perl
# original; some are single long literals (e.g. 256 repeated arguments) that
# cannot be wrapped without changing their meaning.
# pylint: disable=line-too-long

import os
import re

_RE_EMPTY = r"^$"


def _check_data_state(node, kind):
    """Assert the initialized pgbench tables' filler/history state (per kind)."""
    assert (
        node.safe_psql(
            "SELECT count(*) AS null_count FROM pgbench_accounts "
            "WHERE filler IS NULL LIMIT 10;"
        )
        == "0"
    ), "{}: filler column of pgbench_accounts has no NULL data".format(kind)
    assert (
        node.safe_psql(
            "SELECT count(*) AS null_count FROM pgbench_branches "
            "WHERE filler IS NULL;"
        )
        == "1"
    ), "{}: filler column of pgbench_branches has only NULL data".format(kind)
    assert (
        node.safe_psql(
            "SELECT count(*) AS null_count FROM pgbench_tellers WHERE filler IS NULL;"
        )
        == "10"
    ), "{}: filler column of pgbench_tellers has only NULL data".format(kind)
    assert (
        node.safe_psql("SELECT count(*) AS data_count FROM pgbench_history;") == "0"
    ), "{}: pgbench_history has no data".format(kind)


def _check_pgbench_logs(node, prefix, nb, minc, maxc, pattern):
    """Validate per-thread pgbench log files (count, naming, line format)."""
    bdir = node.basedir
    logs = [
        str(bdir / e)
        for e in os.listdir(bdir)
        if re.match(r"^{}\..*$".format(re.escape(prefix)), e)
    ]
    assert len(logs) == nb, "number of log files"
    name_re = re.compile(r"/{}\.\d+(\.\d+)?$".format(re.escape(prefix)))
    assert sum(1 for log in logs if name_re.search(log)) == nb, "file name format"
    rx = re.compile(pattern)
    for log in sorted(logs):
        with open(log, encoding="utf-8") as fh:
            contents = fh.read().split("\n")
        if contents and contents[-1] == "":
            contents.pop()
        clen = len(contents)
        assert clen >= minc, "transaction count for {} ({}) above min".format(log, clen)
        assert clen <= maxc, "transaction count for {} ({}) below max".format(log, clen)
        assert (
            sum(1 for line in contents if rx.search(line)) == clen
        ), "transaction format for {}".format(prefix)


def _test_init_and_basic(node, ts_name):
    """Connection errors, initialization steps and builtin scripts."""
    labels = ",".join("'l{}'".format(i) for i in range(1, 1001))
    node.pgbench(
        "--no-vacuum --client=5 --protocol=prepared --transactions=25",
        0,
        [r"processed: 125/125"],
        [_RE_EMPTY],
        "concurrent OID generation",
        {
            "001_pgbench_concurrent_insert": (
                "CREATE TYPE pg_temp.e AS ENUM ({}); "
                "DROP TYPE pg_temp.e;".format(labels)
            )
        },
    )

    # TODO upstream (PROC_IN_VACUUM scan breakage): tolerate failure.
    node.safe_psql("CREATE TABLE ddl_target ()")
    try:
        node.pgbench(
            "--no-vacuum --client=5 --protocol=prepared --transactions=50",
            0,
            [r"processed: 250/250"],
            [_RE_EMPTY],
            "concurrent GRANT/VACUUM",
            {
                "001_pgbench_grant@9": (
                    "DO $$\nBEGIN\n"
                    "    PERFORM pg_advisory_xact_lock(42);\n"
                    "    FOR i IN 1 .. 10 LOOP\n"
                    "        GRANT SELECT ON ddl_target TO PUBLIC;\n"
                    "        REVOKE SELECT ON ddl_target FROM PUBLIC;\n"
                    "    END LOOP;\nEND\n$$;\n"
                ),
                "001_pgbench_vacuum_ddl_target@1": "VACUUM ddl_target;",
            },
        )
    except AssertionError:
        pass

    node.pgbench(
        "no-such-database",
        1,
        [_RE_EMPTY],
        [
            r"connection to server .* failed",
            r'FATAL:  database "no-such-database" does not exist',
        ],
        "no such database",
    )
    node.pgbench(
        "-S -t 1",
        1,
        [],
        [r"Perhaps you need to do initialization"],
        "run without init",
    )
    node.pgbench(
        "-i",
        0,
        [_RE_EMPTY],
        [
            r"creating tables",
            r"vacuuming",
            r"creating primary keys",
            r"done in \d+\.\d\d s ",
        ],
        "pgbench scale 1 initialization",
    )
    _check_data_state(node, "client-side")

    node.pgbench(
        "--initialize --init-steps=dtpvg --scale=1 --unlogged-tables "
        "--fillfactor=98 --foreign-keys --quiet --tablespace={ts} "
        "--index-tablespace={ts} --partitions=2 "
        "--partition-method=hash".format(ts=ts_name),
        0,
        [r"(?i)^$"],
        [
            r"dropping old tables",
            r"creating tables",
            r"creating 2 partitions",
            r"vacuuming",
            r"creating primary keys",
            r"creating foreign keys",
            r"(?!vacuuming)",
            r"done in \d+\.\d\d s ",
        ],
        "pgbench scale 1 initialization",
    )
    node.pgbench(
        "--initialize --init-steps=dtpvGvv --no-vacuum --foreign-keys "
        "--unlogged-tables --partitions=3",
        0,
        [_RE_EMPTY],
        [
            r"dropping old tables",
            r"creating tables",
            r"creating 3 partitions",
            r"creating primary keys",
            r"generating data \(server-side\)",
            r"creating foreign keys",
            r"(?!vacuuming)",
            r"done in \d+\.\d\d s ",
        ],
        "pgbench --init-steps",
    )
    _check_data_state(node, "server-side")


def _test_builtin_scripts(node):
    """Run the TPC-B / simple-update / select-only builtin scripts."""
    node.pgbench(
        "--transactions=5 -Dfoo=bla --client=2 --protocol=simple --builtin=t"
        " --connect -n -v -n",
        0,
        [
            r"builtin: TPC-B",
            r"clients: 2\b",
            r"processed: 10/10",
            r"mode: simple",
            r"maximum number of tries: 1",
        ],
        [_RE_EMPTY],
        "pgbench tpcb-like",
    )
    node.pgbench(
        "--transactions=20 --client=5 -M extended --builtin=si -C --no-vacuum -s 1",
        0,
        [
            r"builtin: simple update",
            r"clients: 5\b",
            r"threads: 1\b",
            r"processed: 100/100",
            r"mode: extended",
        ],
        [r"scale option ignored"],
        "pgbench simple update",
    )
    node.pgbench(
        "-t 100 -c 7 -M prepared -b se --debug",
        0,
        [
            r"builtin: select only",
            r"clients: 7\b",
            r"threads: 1\b",
            r"processed: 700/700",
            r"mode: prepared",
        ],
        [r"vacuum", r"client 0", r"client 1", r"sending", r"receiving", r"executing"],
        "pgbench select only",
    )


def _detect_nthreads(pg_bin):
    """Return 2 if pgbench supports threads on this platform, else 1."""
    result = pg_bin.result(["pgbench", "--jobs", "2", "--bad-option"])
    if "threads are not supported on this platform" in result.stderr:
        return 1
    return 2


def _test_custom_scripts(node, nthreads):
    """Custom scripts with weights and a few simple/extended variants."""
    node.pgbench(
        "-t 100 -c 1 -j {} -M prepared -n".format(nthreads),
        0,
        [
            r"type: multiple scripts",
            r"mode: prepared",
            r"script 1: .*/001_pgbench_custom_script_1",
            r"weight: 2",
            r"script 2: .*/001_pgbench_custom_script_2",
            r"weight: 1",
            r"processed: 100/100",
        ],
        [_RE_EMPTY],
        "pgbench custom scripts",
        {
            "001_pgbench_custom_script_1@1": (
                "-- select only\n"
                "\\set aid random(1, :scale * 100000)\n"
                "SELECT abalance::INTEGER AS balance\n"
                "  FROM pgbench_accounts\n"
                "  WHERE aid=:aid;\n"
            ),
            "001_pgbench_custom_script_2@2": (
                "-- special variables\n"
                "BEGIN;\n"
                "\\set foo 1\n"
                "-- cast are needed for typing under -M prepared\n"
                "SELECT :foo::INT + :scale::INT * :client_id::INT AS bla;\n"
                "COMMIT;\n"
            ),
        },
    )
    for client, mode, num in (("1", "simple", "3"), ("2", "extended", "4")):
        total = "10/10" if client == "1" else "20/20"
        node.pgbench(
            "-n -t 10 -c {} -M {}".format(client, mode),
            0,
            [
                r"type: .*/001_pgbench_custom_script_{}".format(num),
                r"processed: {}".format(total),
                r"mode: {}".format(mode),
            ],
            [_RE_EMPTY],
            "pgbench custom script",
            {
                "001_pgbench_custom_script_{}".format(num): (
                    "-- select only variant\n"
                    "\\set aid random(1, :scale * 100000)\n"
                    "BEGIN;\n"
                    "SELECT abalance::INTEGER AS balance\n"
                    "  FROM pgbench_accounts\n"
                    "  WHERE aid=:aid;\n"
                    "COMMIT;\n"
                )
            },
        )


def _test_param_logging(node):
    """Server-side logging of query parameters under several GUC settings."""
    long_sel = (
        "select $$'Valame Dios!' dijo Sancho; 'no le dije yo a vuestra merced "
        "que mirase bien lo que hacia?'$$ as long \\gset\n"
    )
    invalid_json_script = (
        "select '{ invalid ' as value \\gset\n"
        + long_sel
        + "select column1::jsonb from (values (:value), (:long)) as q;\n"
    )
    div_zero_script = (
        "select '1' as one \\gset\n"
        "SELECT 1 / (random() / 2)::int, :one::int, :two::int;\n"
    )

    node.append_conf(
        "log_min_duration_statement = 0\n"
        "log_parameter_max_length = 0\n"
        "log_parameter_max_length_on_error = 0"
    )
    node.reload()
    node.pgbench(
        "-n -t1 -c1 -M prepared",
        2,
        [],
        [
            r"ERROR:  invalid input syntax for type json",
            r"(?!unnamed portal with parameters)",
        ],
        "server parameter logging",
        {"001_param_1": invalid_json_script},
    )
    log = node.log_content()
    assert not re.search(
        r"DETAIL:  Parameters: \$1 = '\{ invalid ',", log
    ), "no parameters logged"

    node.append_conf(
        "log_parameter_max_length = -1\nlog_parameter_max_length_on_error = 64"
    )
    node.reload()
    node.pgbench(
        "-n -t1 -c1 -M prepared",
        2,
        [],
        [
            r"ERROR:  division by zero",
            r"CONTEXT:  unnamed portal with parameters: \$1 = '1', \$2 = NULL",
        ],
        "server parameter logging",
        {"001_param_2": div_zero_script},
    )
    node.pgbench(
        "-n -t1 -c1 -M prepared",
        2,
        [],
        [
            r"ERROR:  invalid input syntax for type json",
            r"CONTEXT:  JSON data, line 1: \{ invalid\.\.\.[\r\n]+unnamed portal "
            r"with parameters: \$1 = '\{ invalid ', \$2 = '''Valame Dios!'' dijo "
            r"Sancho; ''no le dije yo a vuestra merced que \.\.\.'",
        ],
        "server parameter logging",
        {"001_param_3": invalid_json_script},
    )
    log = node.log_content()
    assert re.search(
        r"DETAIL:  Parameters: \$1 = '\{ invalid ', \$2 = '''Valame Dios!'' "
        r"dijo Sancho; ''no le dije yo a vuestra merced que mirase bien lo que "
        r"hacia\?'''",
        log,
    ), "parameter report does not truncate"

    node.append_conf(
        "log_min_duration_statement = -1\n"
        "log_parameter_max_length = 7\n"
        "log_parameter_max_length_on_error = -1"
    )
    node.reload()
    node.pgbench(
        "-n -t1 -c1 -M prepared",
        2,
        [],
        [
            r"ERROR:  division by zero",
            r"CONTEXT:  unnamed portal with parameters: \$1 = '1', \$2 = NULL",
        ],
        "server parameter logging",
        {"001_param_4": div_zero_script},
    )
    node.append_conf("log_min_duration_statement = 0")
    node.reload()
    node.pgbench(
        "-n -t1 -c1 -M prepared",
        2,
        [],
        [
            r"ERROR:  invalid input syntax for type json",
            r"CONTEXT:  JSON data, line 1: \{ invalid\.\.\.[\r\n]+unnamed portal "
            r"with parameters: \$1 = '\{ invalid ', \$2 = '''Valame Dios!'' dijo "
            r"Sancho; ''no le dije yo a vuestra merced que mirase bien lo que "
            r"hacia\?'",
        ],
        "server parameter logging",
        {"001_param_5": invalid_json_script},
    )
    log = node.log_content()
    assert re.search(
        r"DETAIL:  Parameters: \$1 = '\{ inval\.\.\.', \$2 = '''Valame\.\.\.'", log
    ), "parameter report truncates"

    node.pgbench(
        "-n -t1 -c1 -M prepared",
        2,
        [],
        [
            r'ERROR:  invalid input syntax for type smallint: "1a"',
            r"CONTEXT:  unnamed portal parameter \$2 = '1a'",
        ],
        "server parameter logging",
        {
            "001_param_6": (
                "select 42 as value1, '1a' as value2 \\gset\n"
                "select :value1::smallint, :value2::smallint;\n"
            )
        },
    )
    node.append_conf(
        "log_min_duration_statement = -1\n"
        "log_parameter_max_length_on_error = 0\n"
        "log_parameter_max_length = -1"
    )
    node.reload()


def _test_seeded_random_determinism(node):
    """A seeded run produces identical random values across two invocations."""
    node.safe_psql(
        "CREATE UNLOGGED TABLE seeded_random(seed INT8 NOT NULL, "
        "rand TEXT NOT NULL, val INTEGER NOT NULL);"
    )
    seed = 123456789
    for i in (1, 2):
        node.pgbench(
            "--random-seed={} -t 1".format(seed),
            0,
            [r"processed: 1/1"],
            [r"setting random seed to {}\b".format(seed)],
            "random seeded with {}".format(seed),
            {
                "001_pgbench_random_seed_{}".format(i): (
                    "-- test random functions\n"
                    "\\set ur random(1000, 1999)\n"
                    "\\set er random_exponential(2000, 2999, 2.0)\n"
                    "\\set gr random_gaussian(3000, 3999, 3.0)\n"
                    "\\set zr random_zipfian(4000, 4999, 1.5)\n"
                    "INSERT INTO seeded_random(seed, rand, val) VALUES\n"
                    "  (:random_seed, 'uniform', :ur),\n"
                    "  (:random_seed, 'exponential', :er),\n"
                    "  (:random_seed, 'gaussian', :gr),\n"
                    "  (:random_seed, 'zipfian', :zr);\n"
                )
            },
        )
    result = node.psql_capture(
        "SELECT seed, rand, val, COUNT(*) FROM seeded_random "
        "GROUP BY seed, rand, val"
    )
    assert result.rc == 0, "psql seeded_random count ok"
    assert result.stderr == "", "psql seeded_random count stderr is empty"
    for kind, lead in (
        ("uniform", "1"),
        ("exponential", "2"),
        ("gaussian", "3"),
        ("zipfian", "4"),
    ):
        assert re.search(
            r"\b{}\|{}\|{}\d\d\d\|2".format(seed, kind, lead), result.stdout
        ), "psql seeded_random count {}".format(kind)
    node.safe_psql("DROP TABLE seeded_random;")


_NESTED_IF_SCRIPT = "\n\t\t\t\\if false\n\t\t\t\tSELECT 1 / 0;\n\t\t\t\t\\if true\n\t\t\t\t\tSELECT 1 / 0;\n\t\t\t\t\\elif true\n\t\t\t\t\tSELECT 1 / 0;\n\t\t\t\t\\else\n\t\t\t\t\tSELECT 1 / 0;\n\t\t\t\t\\endif\n\t\t\t\tSELECT 1 / 0;\n\t\t\t\\elif false\n\t\t\t\t\\if true\n\t\t\t\t\tSELECT 1 / 0;\n\t\t\t\t\\elif true\n\t\t\t\t\tSELECT 1 / 0;\n\t\t\t\t\\else\n\t\t\t\t\tSELECT 1 / 0;\n\t\t\t\t\\endif\n\t\t\t\\else\n\t\t\t\t\\if false\n\t\t\t\t\tSELECT 1 / 0;\n\t\t\t\t\\elif false\n\t\t\t\t\tSELECT 1 / 0;\n\t\t\t\t\\else\n\t\t\t\t\tSELECT 'correct';\n\t\t\t\t\\endif\n\t\t\t\\endif\n\t\t\t\\if true\n\t\t\t\tSELECT 'correct';\n\t\t\t\\else\n\t\t\t\t\\if true\n\t\t\t\t\tSELECT 1 / 0;\n\t\t\t\t\\elif true\n\t\t\t\t\tSELECT 1 / 0;\n\t\t\t\t\\else\n\t\t\t\t\tSELECT 1 / 0;\n\t\t\t\t\\endif\n\t\t\t\\endif\n\t\t"
_BACKSLASH_SCRIPT = "-- run set\n\\set zero 0\n\\set one 1.0\n-- sleep\n\\sleep :one ms\n\\sleep 100 us\n\\sleep 0 s\n\\sleep :zero\n-- setshell and continuation\n\\setshell another_one\\\n  echo \\\n    :one\n\\set n debug(:another_one)\n-- shell\n\\shell echo shell-echo-output\n"
_GSET_SCRIPT = "-- test gset\n-- no columns\nSELECT \\gset\n-- one value\nSELECT 0 AS i0 \\gset\n\\set i debug(:i0)\n-- two values\nSELECT 1 AS i1, 2 AS i2 \\gset\n\\set i debug(:i1)\n\\set i debug(:i2)\n-- with prefix\nSELECT 3 AS i3 \\gset x_\n\\set i debug(:x_i3)\n-- overwrite existing variable\nSELECT 0 AS i4, 4 AS i4 \\gset\n\\set i debug(:i4)\n-- work on the last SQL command under \\;\n\\; \\; SELECT 0 AS i5 \\; SELECT 5 AS i5 \\; \\; \\gset\n\\set i debug(:i5)\n"
_ASET_SCRIPT = "\n-- test aset, which applies to a combined query\n\\; SELECT 6 AS i6 \\; SELECT 7 AS i7 \\; \\aset\n-- unless it returns more than one row, last is kept\nSELECT 8 AS i6 UNION SELECT 9 ORDER BY 1 DESC \\aset\n\\set i debug(:i6)\n\\set i debug(:i7)\n"
_SERIALIZATION_SCRIPT = "\n-- What's happening:\n-- The first client starts the transaction with the isolation level Repeatable\n-- Read:\n--\n-- BEGIN;\n-- UPDATE xy SET y = ... WHERE x = 1;\n--\n-- The second client starts a similar transaction with the same isolation level:\n--\n-- BEGIN;\n-- UPDATE xy SET y = ... WHERE x = 1;\n-- <waiting for the first client>\n--\n-- The first client commits its transaction, and the second client gets a\n-- serialization error.\n\n\\set delta random(-5000, 5000)\n\n-- The second client will stop here\nSELECT pg_advisory_lock(0);\n\n-- Start transaction with concurrent update\nBEGIN;\nUPDATE xy SET y = y + :delta WHERE x = 1 AND pg_advisory_lock(1) IS NOT NULL;\n\n-- Wait for the second client\nDO $$\nDECLARE\n  exists boolean;\n  waiters integer;\nBEGIN\n  -- The second client always comes in second, and the number of rows in the\n  -- table first_client_table reflect this. Here the first client inserts a row,\n  -- so the second client will see a non-empty table when repeating the\n  -- transaction after the serialization error.\n  SELECT EXISTS (SELECT * FROM first_client_table) INTO STRICT exists;\n  IF NOT exists THEN\n\t-- Let the second client begin\n\tPERFORM pg_advisory_unlock(0);\n\t-- And wait until the second client tries to get the same lock\n\tLOOP\n\t  SELECT COUNT(*) INTO STRICT waiters FROM pg_locks WHERE\n\t  locktype = 'advisory' AND objsubid = 1 AND\n\t  ((classid::bigint << 32) | objid::bigint = 1::bigint) AND NOT granted;\n\t  IF waiters = 1 THEN\n\t\tINSERT INTO first_client_table VALUES (1);\n\n\t\t-- Exit loop\n\t\tEXIT;\n\t  END IF;\n\tEND LOOP;\n  END IF;\nEND$$;\n\nCOMMIT;\nSELECT pg_advisory_unlock_all();\n"
_DEADLOCK_SCRIPT = "\n-- What's happening:\n-- The first client gets the lock 2.\n-- The second client gets the lock 3 and tries to get the lock 2.\n-- The first client tries to get the lock 3 and one of them gets a deadlock\n-- error.\n--\n-- A client that does not get a deadlock error must hold a lock at the\n-- transaction start. Thus in the end it releases all of its locks before the\n-- client with the deadlock error starts a retry (we do not want any errors\n-- again).\n\n-- Since the client with the deadlock error has not released the blocking locks,\n-- let's do this here.\nSELECT pg_advisory_unlock_all();\n\n-- The second client and the client with the deadlock error stop here\nSELECT pg_advisory_lock(0);\nSELECT pg_advisory_lock(1);\n\n-- The second client and the client with the deadlock error always come after\n-- the first and the number of rows in the table first_client_table reflects\n-- this. Here the first client inserts a row, so in the future the table is\n-- always non-empty.\nDO $$\nDECLARE\n  exists boolean;\nBEGIN\n  SELECT EXISTS (SELECT * FROM first_client_table) INTO STRICT exists;\n  IF exists THEN\n\t-- We are the second client or the client with the deadlock error\n\n\t-- The first client will take care by itself of this lock (see below)\n\tPERFORM pg_advisory_unlock(0);\n\n\tPERFORM pg_advisory_lock(3);\n\n\t-- The second client can get a deadlock here\n\tPERFORM pg_advisory_lock(2);\n  ELSE\n\t-- We are the first client\n\n\t-- This code should not be used in a new transaction after an error\n\tINSERT INTO first_client_table VALUES (1);\n\n\tPERFORM pg_advisory_lock(2);\n  END IF;\nEND$$;\n\nDO $$\nDECLARE\n  num_rows integer;\n  waiters integer;\nBEGIN\n  -- Check if we are the first client\n  SELECT COUNT(*) FROM first_client_table INTO STRICT num_rows;\n  IF num_rows = 1 THEN\n\t-- This code should not be used in a new transaction after an error\n\tINSERT INTO first_client_table VALUES (2);\n\n\t-- Let the second client begin\n\tPERFORM pg_advisory_unlock(0);\n\tPERFORM pg_advisory_unlock(1);\n\n\t-- Make sure the second client is ready for deadlock\n\tLOOP\n\t  SELECT COUNT(*) INTO STRICT waiters FROM pg_locks WHERE\n\t  locktype = 'advisory' AND\n\t  objsubid = 1 AND\n\t  ((classid::bigint << 32) | objid::bigint = 2::bigint) AND\n\t  NOT granted;\n\n\t  IF waiters = 1 THEN\n\t    -- Exit loop\n\t\tEXIT;\n\t  END IF;\n\tEND LOOP;\n\n\tPERFORM pg_advisory_lock(0);\n    -- And the second client took care by itself of the lock 1\n  END IF;\nEND$$;\n\n-- The first client can get a deadlock here\nSELECT pg_advisory_lock(3);\n\nSELECT pg_advisory_unlock_all();\n"
_PIPELINE_SYNC_SCRIPT = "\n-- test startpipeline\n\\startpipeline\nselect 1;\n\\syncpipeline\n\\syncpipeline\nselect 2;\n\\syncpipeline\nselect 3;\n\\endpipeline\n"


_EXPRESSIONS_SCRIPT = """\
-- integer functions
\\set i1 debug(random(10, 19))
\\set i2 debug(random_exponential(100, 199, 10.0))
\\set i3 debug(random_gaussian(1000, 1999, 10.0))
\\set i4 debug(abs(-4))
\\set i5 debug(greatest(5, 4, 3, 2))
\\set i6 debug(11 + least(-5, -4, -3, -2))
\\set i7 debug(int(7.3))
-- integer arithmetic and bit-wise operators
\\set i8 debug(17 / (4|1) + ( 4 + (7 >> 2)))
\\set i9 debug(- (3 * 4 - (-(~ 1) + -(~ 0))) / -1 + 3 % -1)
\\set ia debug(10 + (0 + 0 * 0 - 0 / 1))
\\set ib debug(:ia + :scale)
\\set ic debug(64 % (((2 + 1 * 2 + (1 # 2) | 4 * (2 & 11)) - (1 << 2)) + 2))
-- double functions and operators
\\set d1 debug(sqrt(+1.5 * 2.0) * abs(-0.8E1))
\\set d2 debug(double(1 + 1) * (-75.0 / :foo))
\\set pi debug(pi() * 4.9)
\\set d4 debug(greatest(4, 2, -1.17) * 4.0 * Ln(Exp(1.0)))
\\set d5 debug(least(-5.18, .0E0, 1.0/0) * -3.3)
-- reset variables
\\set i1 0
\\set d1 false
-- yet another integer function
\\set id debug(random_zipfian(1, 9, 1.3))
--- pow and power
\\set poweri debug(pow(-3,3))
\\set powerd debug(pow(2.0,10))
\\set poweriz debug(pow(0,0))
\\set powerdz debug(pow(0.0,0.0))
\\set powernegi debug(pow(-2,-3))
\\set powernegd debug(pow(-2.0,-3.0))
\\set powernegd2 debug(power(-5.0,-5.0))
\\set powerov debug(pow(9223372036854775807, 2))
\\set powerov2 debug(pow(10,30))
-- comparisons and logical operations
\\set c0 debug(1.0 = 0.0 and 1.0 != 0.0)
\\set c1 debug(0 = 1 Or 1.0 = 1)
\\set c4 debug(case when 0 < 1 then 32 else 0 end)
\\set c5 debug(case when true then 33 else 0 end)
\\set c6 debug(case when false THEN -1 when 1 = 1 then 13 + 19 + 2.0 end )
\\set c7 debug(case when (1 > 0) and (1 >= 0) and (0 < 1) and (0 <= 1) and (0 != 1) and (0 = 0) and (0 <> 1) then 35 else 0 end)
\\set c8 debug(CASE \\
                WHEN (1.0 > 0.0) AND (1.0 >= 0.0) AND (0.0 < 1.0) AND (0.0 <= 1.0) AND \\
                     (0.0 != 1.0) AND (0.0 = 0.0) AND (0.0 <> 1.0) AND (0.0 = 0.0) \\
                  THEN 36 \\
                  ELSE 0 \\
              END)
\\set c9 debug(CASE WHEN NOT FALSE THEN 3 * 12.3333334 END)
\\set ca debug(case when false then 0 when 1-1 <> 0 then 1 else 38 end)
\\set cb debug(10 + mod(13 * 7 + 12, 13) - mod(-19 * 11 - 17, 19))
\\set cc debug(NOT (0 > 1) AND (1 <= 1) AND NOT (0 >= 1) AND (0 < 1) AND \\
    NOT (false and true) AND (false OR TRUE) AND (NOT :f) AND (NOT FALSE) AND \\
    NOT (NOT TRUE))
-- NULL value and associated operators
\\set n0 debug(NULL + NULL * exp(NULL))
\\set n1 debug(:n0)
\\set n2 debug(NOT (:n0 IS NOT NULL OR :d1 IS NULL))
\\set n3 debug(:n0 IS NULL AND :d1 IS NOT NULL AND :d1 NOTNULL)
\\set n4 debug(:n0 ISNULL AND NOT :n0 IS TRUE AND :n0 IS NOT FALSE)
\\set n5 debug(CASE WHEN :n IS NULL THEN 46 ELSE NULL END)
-- use a variables of all types
\\set n6 debug(:n IS NULL AND NOT :f AND :t)
-- conditional truth
\\set cs debug(CASE WHEN 1 THEN TRUE END AND CASE WHEN 1.0 THEN TRUE END AND CASE WHEN :n THEN NULL ELSE TRUE END)
-- hash functions
\\set h0 debug(hash(10, 5432))
\\set h1 debug(:h0 = hash_murmur2(10, 5432))
\\set h3 debug(hash_fnv1a(10, 5432))
\\set h4 debug(hash(10))
\\set h5 debug(hash(10) = hash(10, :default_seed))
-- lazy evaluation
\\set zy 0
\\set yz debug(case when :zy = 0 then -1 else (1 / :zy) end)
\\set yz debug(case when :zy = 0 or (1 / :zy) < 0 then -1 else (1 / :zy) end)
\\set yz debug(case when :zy > 0 and (1 / :zy) < 0 then (1 / :zy) else 1 end)
-- substitute variables of all possible types
\\set v0 NULL
\\set v1 TRUE
\\set v2 5432
\\set v3 -54.21E-2
SELECT :v0, :v1, :v2, :v3;
-- if tests
\\set nope 0
\\if 1 > 0
\\set id debug(65)
\\elif 0
\\set nope 1
\\else
\\set nope 1
\\endif
\\if 1 < 0
\\set nope 1
\\elif 1 > 0
\\set ie debug(74)
\\else
\\set nope 1
\\endif
\\if 1 < 0
\\set nope 1
\\elif 1 < 0
\\set nope 1
\\else
\\set if debug(83)
\\endif
\\if 1 = 1
\\set ig debug(86)
\\elif 0
\\set nope 1
\\endif
\\if 1 = 0
\\set nope 1
\\elif 1 <> 0
\\set ih debug(93)
\\endif
-- must be zero if false branches where skipped
\\set nope debug(:nope)
-- check automatic variables
\\set sc debug(:scale)
\\set ci debug(:client_id)
\\set rs debug(:random_seed)
-- minint constant parsing
\\set min debug(-9223372036854775808)
\\set max debug(-(:min + 1))
-- parametric pseudorandom permutation function
\\set t debug(permute(0, 2) + permute(1, 2) = 1)
\\set t debug(permute(0, 3) + permute(1, 3) + permute(2, 3) = 3)
\\set t debug(permute(0, 4) + permute(1, 4) + permute(2, 4) + permute(3, 4) = 6)
\\set t debug(permute(0, 5) + permute(1, 5) + permute(2, 5) + permute(3, 5) + permute(4, 5) = 10)
\\set t debug(permute(0, 16) + permute(1, 16) + permute(2, 16) + permute(3, 16) + \\
             permute(4, 16) + permute(5, 16) + permute(6, 16) + permute(7, 16) + \\
             permute(8, 16) + permute(9, 16) + permute(10, 16) + permute(11, 16) + \\
             permute(12, 16) + permute(13, 16) + permute(14, 16) + permute(15, 16) = 120)
-- random sanity checks
\\set size random(2, 1000)
\\set v random(0, :size - 1)
\\set p permute(:v, :size)
\\set t debug(0 <= :p and :p < :size and :p = permute(:v + :size, :size) and :p <> permute(:v + 1, :size))
-- actual values
\\set t debug(permute(:v, 1) = 0)
\\set t debug(permute(0, 2, 5431) = 0 and permute(1, 2, 5431) = 1 and \\
             permute(0, 2, 5433) = 1 and permute(1, 2, 5433) = 0)
-- check permute's portability across architectures
\\set size debug(:max - 10)
\\set t debug(permute(:size-1, :size, 5432) = 520382784483822430 and \\
             permute(:size-2, :size, 5432) = 1143715004660802862 and \\
             permute(:size-3, :size, 5432) = 447293596416496998 and \\
             permute(:size-4, :size, 5432) = 916527772266572956 and \\
             permute(:size-5, :size, 5432) = 2763809008686028849 and \\
             permute(:size-6, :size, 5432) = 8648551549198294572 and \\
             permute(:size-7, :size, 5432) = 4542876852200565125)"""


_EXPRESSIONS_EXPECTED = [
    r"setting random seed to 5432\b",
    r"command=1.: int 17\b",
    r"command=2.: int 104\b",
    r"command=3.: int 1498\b",
    r"command=4.: int 4\b",
    r"command=5.: int 5\b",
    r"command=6.: int 6\b",
    r"command=7.: int 7\b",
    r"command=8.: int 8\b",
    r"command=9.: int 9\b",
    r"command=10.: int 10\b",
    r"command=11.: int 11\b",
    r"command=12.: int 12\b",
    r"command=15.: double 15\b",
    r"command=16.: double 16\b",
    r"command=17.: double 17\b",
    r"command=20.: int 3\b",
    r"command=21.: double -27\b",
    r"command=22.: double 1024\b",
    r"command=23.: double 1\b",
    r"command=24.: double 1\b",
    r"command=25.: double -0.125\b",
    r"command=26.: double -0.125\b",
    r"command=27.: double -0.00032\b",
    r"command=28.: double 8.50705917302346e\+0?37\b",
    r"command=29.: double 1e\+0?30\b",
    r"command=30.: boolean false\b",
    r"command=31.: boolean true\b",
    r"command=32.: int 32\b",
    r"command=33.: int 33\b",
    r"command=34.: double 34\b",
    r"command=35.: int 35\b",
    r"command=36.: int 36\b",
    r"command=37.: double 37\b",
    r"command=38.: int 38\b",
    r"command=39.: int 39\b",
    r"command=40.: boolean true\b",
    r"command=41.: null\b",
    r"command=42.: null\b",
    r"command=43.: boolean true\b",
    r"command=44.: boolean true\b",
    r"command=45.: boolean true\b",
    r"command=46.: int 46\b",
    r"command=47.: boolean true\b",
    r"command=48.: boolean true\b",
    r"command=49.: int -5817877081768721676\b",
    r"command=50.: boolean true\b",
    r"command=51.: int -7793829335365542153\b",
    r"command=52.: int -?\d+\b",
    r"command=53.: boolean true\b",
    r"command=65.: int 65\b",
    r"command=74.: int 74\b",
    r"command=83.: int 83\b",
    r"command=86.: int 86\b",
    r"command=93.: int 93\b",
    r"command=95.: int 0\b",
    r"command=96.: int 1\b",
    r"command=97.: int 0\b",
    r"command=98.: int 5432\b",
    r"command=99.: int -9223372036854775808\b",
    r"command=100.: int 9223372036854775807\b",
    r"command=101.: boolean true\b",
    r"command=102.: boolean true\b",
    r"command=103.: boolean true\b",
    r"command=104.: boolean true\b",
    r"command=105.: boolean true\b",
    r"command=109.: boolean true\b",
    r"command=110.: boolean true\b",
    r"command=111.: boolean true\b",
    r"command=113.: boolean true\b",
]


_ERRORS = [
    (
        "sql syntax error",
        2,
        [r"ERROR:  syntax error", r"prepared statement .* does not exist"],
        "-- SQL syntax error\n    SELECT 1 + ;\n",
    ),
    (
        "sql too many args",
        1,
        [r"statement has too many arguments.*\b255\b"],
        "-- MAX_ARGS=256 for prepared\n\\set i 0\nSELECT LEAST(:i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i, :i)",
    ),
    (
        "shell bad command",
        2,
        [r"\(shell\) .* meta-command failed"],
        "\\shell no-such-command",
    ),
    (
        "shell undefined variable",
        2,
        [r'undefined variable ":nosuchvariable"'],
        "-- undefined variable in shell\n\\shell echo ::foo :nosuchvariable\n",
    ),
    (
        "shell missing command",
        1,
        [r"missing command "],
        "\\shell",
    ),
    (
        "shell too many args",
        1,
        [r'too many arguments in command "shell"'],
        "-- 256 arguments to \\shell\n\\shell echo arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg arg",
    ),
    (
        "set syntax error",
        1,
        [r'syntax error in command "set"'],
        "\\set i 1 +",
    ),
    (
        "set no such function",
        1,
        [r"unexpected function name"],
        "\\set i noSuchFunction()",
    ),
    (
        "set invalid variable name",
        2,
        [r"invalid variable name"],
        "\\set . 1",
    ),
    (
        "set division by zero",
        2,
        [r"division by zero"],
        "\\set i 1/0",
    ),
    (
        "set undefined variable",
        2,
        [r'undefined variable "nosuchvariable"'],
        "\\set i :nosuchvariable",
    ),
    (
        "set unexpected char",
        1,
        [r"unexpected character .;."],
        "\\set i ;",
    ),
    (
        "set too many args",
        2,
        [r"too many function arguments"],
        "\\set i least(0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16)",
    ),
    (
        "set empty random range",
        2,
        [r"empty range given to random"],
        "\\set i random(5,3)",
    ),
    (
        "set random range too large",
        2,
        [r"random range is too large"],
        "\\set i random(:minint, :maxint)",
    ),
    (
        "set gaussian param too small",
        2,
        [r"gaussian param.* at least 2"],
        "\\set i random_gaussian(0, 10, 1.0)",
    ),
    (
        "set exponential param greater 0",
        2,
        [r"exponential parameter must be greater "],
        "\\set i random_exponential(0, 10, 0.0)",
    ),
    (
        "set zipfian param to 1",
        2,
        [r"zipfian parameter must be in range \[1\.001, 1000\]"],
        "\\set i random_zipfian(0, 10, 1)",
    ),
    (
        "set zipfian param too large",
        2,
        [r"zipfian parameter must be in range \[1\.001, 1000\]"],
        "\\set i random_zipfian(0, 10, 1000000)",
    ),
    (
        "set non numeric value",
        2,
        [r'malformed variable "foo" value: "bla"'],
        "\\set i :foo + 1",
    ),
    (
        "set no expression",
        1,
        [r"syntax error"],
        "\\set i",
    ),
    (
        "set missing argument",
        1,
        [r"missing argument"],
        "\\set",
    ),
    (
        "set not a bool",
        2,
        [r"cannot coerce double to boolean"],
        "\\set b NOT 0.0",
    ),
    (
        "set not an int",
        2,
        [r"cannot coerce boolean to int"],
        "\\set i TRUE + 2",
    ),
    (
        "set not a double",
        2,
        [r"cannot coerce boolean to double"],
        "\\set d ln(TRUE)",
    ),
    (
        "set case error",
        1,
        [r'syntax error in command "set"'],
        "\\set i CASE TRUE THEN 1 ELSE 0 END",
    ),
    (
        "set random error",
        2,
        [r"cannot coerce boolean to int"],
        "\\set b random(FALSE, TRUE)",
    ),
    (
        "set number of args mismatch",
        1,
        [r"unexpected number of arguments"],
        "\\set d ln(1.0, 2.0))",
    ),
    (
        "set at least one arg",
        1,
        [r"at least one argument expected"],
        "\\set i greatest())",
    ),
    (
        "set double to int overflow",
        2,
        [r"double to int overflow for 100"],
        "\\set i int(1E32)",
    ),
    (
        "set bigint add overflow",
        2,
        [r"int add out"],
        "\\set i (1<<62) + (1<<62)",
    ),
    (
        "set bigint sub overflow",
        2,
        [r"int sub out"],
        "\\set i 0 - (1<<62) - (1<<62) - (1<<62)",
    ),
    (
        "set bigint mul overflow",
        2,
        [r"int mul out"],
        "\\set i 2 * (1<<62)",
    ),
    (
        "set bigint div out of range",
        2,
        [r"bigint div out of range"],
        "\\set i :minint / -1",
    ),
    (
        "setshell not an int",
        2,
        [r"command must return an integer"],
        "\\setshell i echo -n one",
    ),
    (
        "setshell missing arg",
        1,
        [r"missing argument "],
        "\\setshell var",
    ),
    (
        "setshell no such command",
        2,
        [r"could not read result "],
        "\\setshell var no-such-command",
    ),
    (
        "sleep undefined variable",
        2,
        [r"sleep: undefined variable"],
        "\\sleep :nosuchvariable",
    ),
    (
        "sleep too many args",
        1,
        [r"too many arguments"],
        "\\sleep too many args",
    ),
    (
        "sleep missing arg",
        1,
        [r"missing argument", r"\\sleep"],
        "\\sleep",
    ),
    (
        "sleep unknown unit",
        1,
        [r"unrecognized time unit"],
        "\\sleep 1 week",
    ),
    (
        "misc invalid backslash command",
        1,
        [r'invalid command .* "nosuchcommand"'],
        "\\nosuchcommand",
    ),
    (
        "misc empty script",
        1,
        [r"empty command list for script"],
        "",
    ),
    (
        "bad boolean",
        2,
        [r"malformed variable.*trueXXX"],
        "\\set b :badtrue or true",
    ),
    (
        "invalid permute size",
        2,
        [r"permute size parameter must be greater than zero"],
        "\\set i permute(0, 0)",
    ),
    (
        "gset no row",
        2,
        [r"expected one row, got 0\b"],
        "SELECT WHERE FALSE \\gset",
    ),
    (
        "gset alone",
        1,
        [r"gset must follow an SQL command"],
        "\\gset",
    ),
    (
        "gset no SQL",
        1,
        [r"gset must follow an SQL command"],
        "\\set i +1\n\\gset",
    ),
    (
        "gset too many arguments",
        1,
        [r"too many arguments"],
        "SELECT 1 \\gset a b",
    ),
    (
        "gset after gset",
        1,
        [r"gset must follow an SQL command"],
        "SELECT 1 AS i \\gset\n\\gset",
    ),
    (
        "gset non SELECT",
        2,
        [r"expected one row, got 0"],
        "DROP TABLE IF EXISTS no_such_table \\gset",
    ),
    (
        "gset bad default name",
        2,
        [r"error storing into variable \?column\?"],
        "SELECT 1 \\gset",
    ),
    (
        "gset bad name",
        2,
        [r"error storing into variable bad name!"],
        'SELECT 1 AS "bad name!" \\gset',
    ),
]


def _test_expressions(node):
    """Deterministic seeded expression and permute checks."""
    node.pgbench(
        "--random-seed=5432 -t 1 -Dfoo=-10.1 -Dbla=false -Di=+3 -Dn=null "
        "-Dt=t -Df=of -Dd=1.0",
        0,
        [r"type: .*/001_pgbench_expressions", r"processed: 1/1"],
        _EXPRESSIONS_EXPECTED,
        "pgbench expressions",
        {"001_pgbench_expressions": _EXPRESSIONS_SCRIPT},
    )


def _test_nested_ifs(node):
    """Nested \\if/\\elif/\\else constructs select the right branch."""
    node.pgbench(
        "--no-vacuum --client=1 --exit-on-abort --transactions=1",
        0,
        [r"actually processed"],
        [_RE_EMPTY],
        "nested ifs",
        {"pgbench_nested_if": _NESTED_IF_SCRIPT},
    )


def _test_backslash_commands(node):
    """\\set, \\sleep, \\setshell continuation, \\shell."""
    node.pgbench(
        "-t 1",
        0,
        [
            r"type: .*/001_pgbench_backslash_commands",
            r"processed: 1/1",
            r"shell-echo-output",
        ],
        [r"command=8.: int 1\b"],
        "pgbench backslash commands",
        {"001_pgbench_backslash_commands": _BACKSLASH_SCRIPT},
    )


def _test_gset_aset(node):
    """\\gset and \\aset behavior, including their error cases."""
    node.pgbench(
        "-t 1",
        0,
        [r"type: .*/001_pgbench_gset", r"processed: 1/1"],
        [
            r"command=3.: int 0\b",
            r"command=5.: int 1\b",
            r"command=6.: int 2\b",
            r"command=8.: int 3\b",
            r"command=10.: int 4\b",
            r"command=12.: int 5\b",
        ],
        "pgbench gset command",
        {"001_pgbench_gset": _GSET_SCRIPT},
    )
    node.pgbench(
        "-t 1",
        2,
        [r"type: .*/001_pgbench_gset_two_rows", r"processed: 0/1"],
        [r"expected one row, got 2\b"],
        "pgbench gset command with two rows",
        {
            "001_pgbench_gset_two_rows": (
                "\nSELECT 5432 AS fail UNION SELECT 5433 ORDER BY 1 \\gset\n"
            )
        },
    )
    node.pgbench(
        "-t 1",
        0,
        [r"type: .*/001_pgbench_aset", r"processed: 1/1"],
        [r"command=3.: int 8\b", r"command=4.: int 7\b"],
        "pgbench aset command",
        {"001_pgbench_aset": _ASET_SCRIPT},
    )
    node.pgbench(
        "-t 1",
        2,
        [r"type: .*/001_pgbench_aset_empty", r"processed: 0/1"],
        [r"undefined variable \"i8\"", r"evaluation of meta-command failed\b"],
        "pgbench aset command with empty result",
        {
            "001_pgbench_aset_empty": (
                "\n-- empty result\n\\; SELECT 5432 AS i8 WHERE FALSE \\; \\aset\n"
                "\\set i debug(:i8)\n"
            )
        },
    )


def _test_pipelines(node):
    """\\startpipeline / \\syncpipeline / \\endpipeline and their errors."""
    select_ten = "select 1;\n" * 10
    node.pgbench(
        "-t 1 -n -M extended",
        0,
        [r"type: .*/001_pgbench_pipeline", r"actually processed: 1/1"],
        [],
        "working \\startpipeline",
        {
            "001_pgbench_pipeline": "\n-- test startpipeline\n\\startpipeline\n"
            + select_ten
            + "\n\\endpipeline\n"
        },
    )
    node.pgbench(
        "-t 1 -n -M extended",
        0,
        [r"type: .*/001_pgbench_pipeline_sync", r"actually processed: 1/1"],
        [],
        "working \\startpipeline with \\syncpipeline",
        {"001_pgbench_pipeline_sync": _PIPELINE_SYNC_SCRIPT},
    )
    node.pgbench(
        "-t 1 -n -M prepared",
        0,
        [r"type: .*/001_pgbench_pipeline_prep", r"actually processed: 1/1"],
        [],
        "working \\startpipeline",
        {
            "001_pgbench_pipeline_prep": "\n-- test startpipeline\n\\startpipeline\n"
            "\\endpipeline\n\\startpipeline\n" + select_ten + "\n\\endpipeline\n"
        },
    )
    _test_pipeline_errors(node)
    _test_pipeline_implicit_xact(node)
    node.pgbench(
        "-c4 -t 10 -n -M prepared",
        0,
        [
            r"type: .*/001_pgbench_pipeline_serializable",
            r"actually processed: (\d+)/\1",
        ],
        [],
        "working \\startpipeline with serializable",
        {
            "001_pgbench_pipeline_serializable": (
                "\n-- test startpipeline with serializable\n\\startpipeline\n"
                "BEGIN ISOLATION LEVEL SERIALIZABLE;\n"
                + select_ten
                + "END;\n\\endpipeline\n"
            )
        },
    )


def _test_pipeline_errors(node):
    """The simple pipeline misuse errors (twice/no-start/gset/unclosed)."""
    cases = [
        (
            "001_pgbench_pipeline_2",
            r"already in pipeline mode",
            "error: call \\startpipeline twice",
            "\n-- startpipeline twice\n\\startpipeline\n\\startpipeline\n",
            "-t 1",
        ),
        (
            "001_pgbench_pipeline_3",
            r"not in pipeline mode",
            "error: \\endpipeline with no start",
            "\n-- pipeline not started\n\\endpipeline\n",
            "-t 1",
        ),
        (
            "001_pgbench_pipeline_4",
            r"gset is not allowed in pipeline mode",
            "error: \\gset not allowed in pipeline mode",
            "\n\\startpipeline\nselect 1 \\gset f\n\\endpipeline\n",
            "-t 1",
        ),
        (
            "001_pgbench_pipeline_5",
            r"end of script reached with pipeline open",
            "error: call \\startpipeline without \\endpipeline in a single "
            "transaction",
            "\n-- startpipeline only with single transaction\n\\startpipeline\n",
            "-t 1",
        ),
        (
            "001_pgbench_pipeline_6",
            r"end of script reached with pipeline open",
            "error: call \\startpipeline without \\endpipeline",
            "\n-- startpipeline only\n\\startpipeline\n",
            "-t 2",
        ),
        (
            "001_pgbench_pipeline_7",
            r"end of script reached with pipeline open",
            "error: call \\startpipeline and \\syncpipeline without \\endpipeline",
            "\n-- startpipeline with \\syncpipeline only\n\\startpipeline\n"
            "\\syncpipeline\n",
            "-t 2",
        ),
    ]
    for name, err, msg, script, topt in cases:
        node.pgbench(
            "{} -n -M extended".format(topt),
            2,
            [],
            [err],
            msg,
            {name: script},
        )


def _test_pipeline_implicit_xact(node):
    """SET LOCAL / REINDEX / VACUUM / subtrans / LOCK in a pipeline xact."""
    cases = [
        (
            "001_pgbench_pipeline_set_local_1",
            0,
            [r"WARNING:  SET LOCAL can only be used in transaction blocks"],
            "SET LOCAL outside implicit transaction block of pipeline",
            "\n\\startpipeline\nSET LOCAL statement_timeout='1h';\n\\endpipeline\n",
        ),
        (
            "001_pgbench_pipeline_set_local_2",
            0,
            [_RE_EMPTY],
            "SET LOCAL inside implicit transaction block of pipeline",
            "\n\\startpipeline\nSELECT 1;\nSET LOCAL statement_timeout='1h';\n"
            "\\endpipeline\n",
        ),
        (
            "001_pgbench_pipeline_set_local_3",
            0,
            [r"WARNING:  SET LOCAL can only be used in transaction blocks"],
            "SET LOCAL and \\syncpipeline",
            "\n\\startpipeline\nSELECT 1;\n\\syncpipeline\n"
            "SET LOCAL statement_timeout='1h';\n\\endpipeline\n",
        ),
        (
            "001_pgbench_pipeline_reindex_1",
            0,
            [],
            "REINDEX CONCURRENTLY outside implicit transaction block of pipeline",
            "\n\\startpipeline\nREINDEX TABLE CONCURRENTLY pgbench_accounts;\n"
            "SELECT 1;\n\\endpipeline\n",
        ),
        (
            "001_pgbench_pipeline_reindex_2",
            2,
            [],
            "error: REINDEX CONCURRENTLY inside implicit transaction block of "
            "pipeline",
            "\n\\startpipeline\nSELECT 1;\n"
            "REINDEX TABLE CONCURRENTLY pgbench_accounts;\n\\endpipeline\n",
        ),
        (
            "001_pgbench_pipeline_vacuum_1",
            0,
            [],
            "VACUUM outside implicit transaction block of pipeline",
            "\n\\startpipeline\nVACUUM pgbench_accounts;\n\\endpipeline\n",
        ),
        (
            "001_pgbench_pipeline_vacuum_2",
            2,
            [],
            "error: VACUUM inside implicit transaction block of pipeline",
            "\n\\startpipeline\nSELECT 1;\nVACUUM pgbench_accounts;\n"
            "\\endpipeline\n",
        ),
        (
            "001_pgbench_pipeline_subtrans",
            2,
            [],
            "error: subtransactions not allowed in pipeline",
            "\n\\startpipeline\nSAVEPOINT a;\nSELECT 1;\nROLLBACK TO SAVEPOINT a;\n"
            "SELECT 2;\n\\endpipeline\n",
        ),
        (
            "001_pgbench_pipeline_lock_1",
            2,
            [],
            "error: LOCK TABLE outside implicit transaction block of pipeline",
            "\n\\startpipeline\nLOCK pgbench_accounts;\nSELECT 1;\n\\endpipeline\n",
        ),
        (
            "001_pgbench_pipeline_lock_2",
            0,
            [],
            "LOCK TABLE inside implicit transaction block of pipeline",
            "\n\\startpipeline\nSELECT 1;\nLOCK pgbench_accounts;\n\\endpipeline\n",
        ),
    ]
    for name, status, err, msg, script in cases:
        node.pgbench("-t 1 -n -M extended", status, [], err, msg, {name: script})


def _test_errors_table(node):
    """The large table of expression and meta-command errors."""
    base = (
        "-n -t 1 -Dfoo=bla -Dnull=null -Dtrue=true -Done=1 -Dzero=0.0 "
        "-Dbadtrue=trueXXX -Dmaxint=9223372036854775807 "
        "-Dminint=-9223372036854775808 -M prepared"
    )
    for name, status, err, script in _ERRORS:
        assert status != 0, 'invalid expected status for test "{}"'.format(name)
        fname = "001_pgbench_error_" + name.replace(" ", "_")
        out = [_RE_EMPTY] if status == 1 else [r"processed: 0/1"]
        node.pgbench(
            base, status, out, err, "pgbench script error: " + name, {fname: script}
        )


def _test_throttling(node):
    """--rate / --latency-limit throttling, including late throttling."""
    node.pgbench(
        "-t 100 -S --rate=100000 --latency-limit=1000000 -c 2 -n -r",
        0,
        [r"processed: 200/200", r"builtin: select only"],
        [_RE_EMPTY],
        "pgbench throttling",
    )
    node.pgbench(
        "-t 10 --rate=100000 --latency-limit=1 -n -r",
        0,
        [
            r"processed: [01]/10",
            r"type: .*/001_pgbench_sleep",
            r"above the 1.0 ms latency limit: [01]/",
        ],
        [_RE_EMPTY],
        "pgbench late throttling",
        {"001_pgbench_sleep": "\\sleep 2ms"},
    )


def _test_logs(node):
    """--log sampling and per-thread log file format."""
    bdir = node.basedir
    node.pgbench(
        "-n -S -t 50 -c 2 --log --sampling-rate=0.5",
        0,
        [r"select only", r"processed: 100/100"],
        [_RE_EMPTY],
        "pgbench logs",
        None,
        "--log-prefix={}/001_pgbench_log_2".format(bdir),
    )
    _check_pgbench_logs(
        node, "001_pgbench_log_2", 1, 8, 92, r"^[01] \d{1,2} \d+ \d \d+ \d+$"
    )
    node.pgbench(
        "-n -b select-only -t 10 -l",
        0,
        [r"select only", r"processed: 10/10"],
        [_RE_EMPTY],
        "pgbench logs contents",
        None,
        "--log-prefix={}/001_pgbench_log_3".format(bdir),
    )
    _check_pgbench_logs(
        node, "001_pgbench_log_3", 1, 10, 10, r"^0 \d{1,2} \d+ \d \d+ \d+$"
    )
    node.pgbench(
        "--no-vacuum",
        2,
        [r"processed: 1/10"],
        [
            r"client 0 aborted: end of script reached without completing the "
            r"last transaction"
        ],
        "incomplete transaction block",
        {"001_pgbench_incomplete_transaction_block": "BEGIN;SELECT 1;"},
    )


def _test_retry(node):
    """Serialization and deadlock errors with --max-tries retry."""
    node.safe_psql(
        "CREATE UNLOGGED TABLE first_client_table (value integer); "
        "CREATE UNLOGGED TABLE xy (x integer, y integer); "
        "INSERT INTO xy VALUES (1, 2);"
    )
    serial_err = (
        r"(?s)(client (0|1) sending UPDATE xy SET y = y \+ -?\d+\b).*"
        r"client \2 got an error in command 3 \(SQL\) of script 0; "
        r"ERROR:  could not serialize access due to concurrent update\b.*"
        r"\1"
    )
    old_opts = os.environ.get("PGOPTIONS")
    os.environ["PGOPTIONS"] = "-c default_transaction_isolation=repeatable\\ read"
    try:
        node.pgbench(
            "-n -c 2 -t 1 --debug --verbose-errors --max-tries 2",
            0,
            [
                r"processed: 2/2\b",
                r"number of transactions retried: 1\b",
                r"total number of retries: 1\b",
            ],
            [serial_err],
            "concurrent update with retrying",
            {"001_pgbench_serialization": _SERIALIZATION_SCRIPT},
        )
    finally:
        _restore_pgoptions(old_opts)
    node.safe_psql("DELETE FROM first_client_table;")

    deadlock_err = (
        r"client (0|1) got an error in command (3|5) \(SQL\) of script 0; "
        r"ERROR:  deadlock detected\b"
    )
    os.environ["PGOPTIONS"] = "-c default_transaction_isolation=read\\ committed"
    try:
        node.pgbench(
            "-n -c 2 -t 1 --max-tries 2 --verbose-errors",
            0,
            [
                r"processed: 2/2\b",
                r"number of transactions retried: 1\b",
                r"total number of retries: 1\b",
            ],
            [deadlock_err],
            "deadlock with retrying",
            {"001_pgbench_deadlock": _DEADLOCK_SCRIPT},
        )
    finally:
        _restore_pgoptions(old_opts)
    node.safe_psql("DROP TABLE first_client_table, xy;")


def _restore_pgoptions(old_opts):
    """Restore (or clear) the PGOPTIONS environment variable."""
    if old_opts is None:
        os.environ.pop("PGOPTIONS", None)
    else:
        os.environ["PGOPTIONS"] = old_opts


def _test_exit_on_abort_and_copy(node):
    """--exit-on-abort aborts the run; COPY in a script is rejected."""
    node.safe_psql("CREATE TABLE counter(i int); INSERT INTO counter VALUES (0);")
    node.pgbench(
        "-t 10 -c 2 -j 2 --exit-on-abort",
        2,
        [],
        [r"division by zero", r"Run was aborted due to an error in thread"],
        "test --exit-on-abort",
        {
            "001_exit_on_abort": (
                "\nupdate counter set i = i+1 returning i \\gset\n"
                "\\if :i = 5\n\\set y 1/0\n\\endif\n"
            )
        },
    )
    node.pgbench(
        "-t 10",
        2,
        [],
        [r"COPY is not supported in pgbench, aborting"],
        "Test copy in script",
        {"001_copy": " COPY pgbench_accounts FROM stdin "},
    )
    node.safe_psql("DROP TABLE counter;")


def _test_continue_on_error(node):
    """--continue-on-error keeps running past per-transaction failures."""
    node.safe_psql("CREATE TABLE unique_table(i int unique);")
    node.pgbench(
        "-n -t 10 --continue-on-error --failures-detailed",
        0,
        [r"processed: 1/10\b", r"other failures: 9\b"],
        [],
        "test --continue-on-error",
        {"001_continue_on_error": "\n\t\tINSERT INTO unique_table VALUES(0);\n\t\t"},
    )
    node.safe_psql("DROP TABLE unique_table;")


def test_001_pgbench_with_server(create_pg, pg_bin):
    """pgbench end-to-end behavior against a live server."""
    node = create_pg("main", start=False, extra=["--locale", "C"])
    node.start()
    ts_dir = node.basedir / "regress_pgbench_tap_1_ts_dir"
    ts_dir.mkdir()
    node.safe_psql(
        "CREATE TABLESPACE regress_pgbench_tap_1_ts LOCATION '{}';".format(ts_dir)
    )

    _test_init_and_basic(node, "regress_pgbench_tap_1_ts")
    _test_builtin_scripts(node)
    nthreads = _detect_nthreads(pg_bin)
    _test_custom_scripts(node, nthreads)
    _test_param_logging(node)
    _test_expressions(node)
    _test_nested_ifs(node)
    _test_seeded_random_determinism(node)
    _test_backslash_commands(node)
    _test_gset_aset(node)
    _test_pipelines(node)
    _test_errors_table(node)
    _test_throttling(node)
    _test_logs(node)
    _test_retry(node)
    _test_exit_on_abort_and_copy(node)
    _test_continue_on_error(node)

    node.safe_psql("DROP TABLESPACE regress_pgbench_tap_1_ts")
    node.stop()
