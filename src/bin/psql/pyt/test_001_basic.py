# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/bin/psql/t/001_basic.pl."""

import re

from pypg import append_to_file, slurp_file

_INSERT = "INSERT INTO tab_psql_single VALUES"


def _psql_like(node, sql, expected_stdout, name, on_error_stop=True):
    result = node.psql_capture(sql, on_error_stop=on_error_stop)
    assert result.rc == 0, "{}: exit code 0".format(name)
    assert result.stderr == "", "{}: no stderr".format(name)
    assert re.search(expected_stdout, result.stdout), "{}: matches".format(name)


def _psql_fails_like(node, sql, expected_stderr, name, replication=None):
    result = node.psql_capture(sql, replication=replication)
    assert result.rc != 0, "{}: exit code not 0".format(name)
    assert re.search(expected_stderr, result.stderr), "{}: matches".format(name)


def test_basic(pg_bin, create_pg, tmp_path):
    """psql meta-commands, timing, encoding, notify, crash, switches, pipelines."""
    pg_bin.program_help_ok("psql")
    pg_bin.program_version_ok("psql")
    pg_bin.program_options_handling_ok("psql")

    for arg in ("commands", "variables"):
        result = pg_bin.result(["psql", "--help=" + arg])
        assert result.rc == 0, "psql --help={} exit code 0".format(arg)
        assert result.stdout != "", "psql --help={} goes to stdout".format(arg)
        assert result.stderr == "", "psql --help={} nothing to stderr".format(arg)

    node = create_pg("main", extra=["--locale=C", "--encoding=UTF8"], start=False)
    node.append_conf(
        "wal_level = 'logical'\nmax_replication_slots = 4\nmax_wal_senders = 4"
    )
    node.start()

    _meta(node)
    _crash_and_errverbose(node)
    _switches(node, tmp_path)
    _copy_default(node, tmp_path)
    _watch(node)
    _g_pipe(node, tmp_path)
    _pipelines(node)


def _meta(node):
    _psql_like(node, "\\copyright", r"Copyright", "\\copyright")
    _psql_like(node, "\\help", r"ALTER", "\\help without arguments")
    _psql_like(node, "\\help SELECT", r"SELECT", "\\help with argument")

    _psql_fails_like(
        node,
        "START_REPLICATION 0/0",
        r"unexpected PQresultStatus: 8$",
        "handling of unexpected PQresultStatus",
        replication="database",
    )

    _psql_like(
        node,
        "\\timing on\nSELECT 1",
        r"(?m)^1$\n^Time: \d+[.,]\d\d\d ms",
        "\\timing with successful query",
    )

    result = node.psql_capture("\\timing on\nSELECT error")
    assert result.rc != 0, "\\timing with query error: query failed"
    assert re.search(
        r"(?m)^Time: \d+[.,]\d\d\d ms", result.stdout
    ), "\\timing with query error: timing output appears"
    assert not re.search(
        r"(?m)^Time: 0[.,]000 ms", result.stdout
    ), "\\timing with query error: timing was updated"

    _psql_like(
        node,
        "\\echo :ENCODING\nset client_encoding = LATIN1;\n\\echo :ENCODING",
        r"(?m)^UTF8$\n^LATIN1$",
        "ENCODING variable is set and updated",
    )

    _psql_like(
        node,
        "LISTEN foo;\nNOTIFY foo;",
        r'^Asynchronous notification "foo" received from server process '
        r"with PID \d+\.$",
        "notification",
    )
    _psql_like(
        node,
        "LISTEN foo;\nNOTIFY foo, 'bar';",
        r'^Asynchronous notification "foo" with payload "bar" received from '
        r"server process with PID \d+\.$",
        "notification with payload",
    )


def _crash_and_errverbose(node):
    result = node.psql_capture(
        "SELECT 'before' AS running;\n"
        "SELECT pg_terminate_backend(pg_backend_pid());\n"
        "SELECT 'AFTER' AS not_running;\n"
    )
    assert result.rc == 2, "server crash: psql exit code"
    assert re.search(r"before", result.stdout), "server crash: output before crash"
    assert not re.search(r"AFTER", result.stdout), "server crash: no output after crash"
    assert re.search(
        r"psql:<stdin>:2: FATAL:  terminating connection due to administrator "
        r"command\n"
        r"psql:<stdin>:2: server closed the connection unexpectedly\n"
        r"\tThis probably means the server terminated abnormally\n"
        r"\tbefore or while processing the request\.\n"
        r"psql:<stdin>:2: error: connection to server was lost",
        result.stderr,
    ), "server crash: error message"

    _psql_like(
        node,
        "SELECT 1;\n\\errverbose",
        r"^1\nThere is no previous error\.$",
        "\\errverbose with no previous error",
    )

    errverbose = (
        r"(?m)\A^psql:<stdin>:{0}: ERROR:  .*$\n"
        r"^LINE 1: SELECT error{1}$\n"
        r"^ *^.*$\n"
        r"^psql:<stdin>:{2}: error: ERROR:  [0-9A-Z]{{5}}: .*$\n"
        r"^LINE 1: SELECT error{1}$\n"
        r"^ *^.*$\n"
        r"^LOCATION: .*$"
    )
    assert re.search(
        errverbose.format(1, ";", 2),
        node.psql_capture("SELECT error;\n\\errverbose", on_error_stop=False).stderr,
    ), "\\errverbose after normal query with error"
    assert re.search(
        errverbose.format(2, ";", 3),
        node.psql_capture(
            "\\set FETCH_COUNT 1\nSELECT error;\n\\errverbose", on_error_stop=False
        ).stderr,
    ), "\\errverbose after FETCH_COUNT query with error"
    assert re.search(
        errverbose.format(1, "", 2),
        node.psql_capture(
            "SELECT error\\gdesc\n\\errverbose", on_error_stop=False
        ).stderr,
    ), "\\errverbose after \\gdesc with error"


def _switches(node, tmp_path):
    node.safe_psql("CREATE TABLE tab_psql_single (a int);")
    nonexistent = str(tmp_path / "nonexistent")
    copy_cmd = "\\copy tab_psql_single FROM '{}'".format(nonexistent)
    base = ["psql", "--no-psqlrc", "--single-transaction"]
    stop = ["--set", "ON_ERROR_STOP=1"]

    def count():
        return node.safe_psql("SELECT count(*) FROM tab_psql_single")

    node.command_ok(
        base + stop + ["-c", _INSERT + " (1)", "-c", _INSERT + " (2)"],
        "ON_ERROR_STOP, --single-transaction and multiple -c switches",
    )
    assert count() == "2", "--single-transaction commits with ON_ERROR_STOP, -c"

    node.command_fails(
        base + stop + ["-c", _INSERT + " (3)", "-c", copy_cmd],
        "ON_ERROR_STOP, --single-transaction and multiple -c switches, error",
    )
    assert count() == "2", "client-side error rolls back with ON_ERROR_STOP, -c"

    insert_file = str(tmp_path / "tab_insert.sql")
    copy_file = str(tmp_path / "tab_copy.sql")
    append_to_file(insert_file, _INSERT + " (4);")
    append_to_file(copy_file, copy_cmd + ";")

    node.command_ok(
        base + stop + ["-f", insert_file, "-f", insert_file],
        "ON_ERROR_STOP, --single-transaction and multiple -f switches",
    )
    assert count() == "4", "--single-transaction commits with ON_ERROR_STOP, -f"

    node.command_fails(
        base + stop + ["-f", insert_file, "-f", copy_file],
        "ON_ERROR_STOP, --single-transaction and multiple -f switches, error",
    )
    assert count() == "4", "client-side error rolls back with ON_ERROR_STOP, -f"

    node.command_fails(
        base + ["-f", insert_file, "-f", insert_file, "-c", copy_cmd],
        "no ON_ERROR_STOP, --single-transaction and multiple -f/-c switches",
    )
    assert count() == "6", "client-side error commits, no ON_ERROR_STOP, -f/-c"

    node.command_ok(
        base + ["-f", insert_file, "-f", insert_file, "-f", copy_file],
        "no ON_ERROR_STOP, --single-transaction and multiple -f switches",
    )
    assert count() == "8", "client-side error commits, no ON_ERROR_STOP, -f"

    node.command_ok(
        base + ["-c", _INSERT + " (5)", "-f", copy_file, "-c", _INSERT + " (6)"],
        "no ON_ERROR_STOP, --single-transaction and multiple -c switches",
    )
    assert count() == "10", "client-side error commits, no ON_ERROR_STOP, -c"


def _copy_default(node, tmp_path):
    node.safe_psql(
        "CREATE TABLE copy_default ("
        "id integer PRIMARY KEY, "
        "text_value text NOT NULL DEFAULT 'test', "
        "ts_value timestamp without time zone NOT NULL DEFAULT '2022-07-05')"
    )
    csv = str(tmp_path / "copy_default.csv")
    append_to_file(csv, "1,value,2022-07-04\n")
    append_to_file(csv, "2,placeholder,2022-07-03\n")
    append_to_file(csv, "3,placeholder,placeholder\n")
    _psql_like(
        node,
        "\\copy copy_default from {} with (format 'csv', default 'placeholder');\n"
        "SELECT * FROM copy_default".format(csv),
        "1\\|value\\|2022-07-04 00:00:00\n"
        "2|test|2022-07-03 00:00:00\n"
        "3|test|2022-07-05 00:00:00",
        "\\copy from with DEFAULT",
    )


def _watch(node):
    _psql_like(node, "SELECT 1 \\watch c=3 i=0.01", r"1\n1\n1", "\\watch 3x i=0.01")
    _psql_like(node, "SELECT 1 \\watch c=3 i=0.0001", r"1\n1\n1", "\\watch 3x i=0.0001")
    _psql_like(
        node, "\\set WATCH_INTERVAL 0\nSELECT 1 \\watch c=3", r"1\n1\n1", "\\watch i=0"
    )
    _psql_fails_like(
        node, "SELECT 3 \\watch m=x", r"incorrect minimum row count", "\\watch bad m"
    )
    _psql_fails_like(
        node,
        "SELECT 3 \\watch m=1 min_rows=2",
        r"minimum row count specified more than once",
        "\\watch m twice",
    )
    _psql_like(
        node,
        "with x as (\n"
        "  select now()-backend_start AS howlong\n"
        "  from pg_stat_activity\n"
        "  where pid = pg_backend_pid()\n"
        ") select 123 from x where howlong < '2 seconds' \\watch i=0.5 m=2",
        r"^123$",
        "\\watch, 2 minimum rows",
    )
    for spec, msg in (
        ("-10", r'incorrect interval value "-10"'),
        ("10ab", r'incorrect interval value "10ab"'),
        ("10e400", r'incorrect interval value "10e400"'),
        ("1 1", r"interval value is specified more than once"),
        ("c=1 c=1", r"iteration count is specified more than once"),
    ):
        _psql_fails_like(node, "SELECT 1 \\watch " + spec, msg, "\\watch " + spec)

    _psql_like(
        node,
        "\\echo :WATCH_INTERVAL\n\\set WATCH_INTERVAL 10\n\\echo :WATCH_INTERVAL\n"
        "\\unset WATCH_INTERVAL\n\\echo :WATCH_INTERVAL",
        r"(?m)^2$\n^10$\n^2$",
        "WATCH_INTERVAL variable is set and updated",
    )
    _psql_fails_like(
        node, "\\set WATCH_INTERVAL 1e500", r"is out of range", "WATCH_INTERVAL range"
    )
    _psql_like(node, "\\echo :WATCH_INTERVAL", r"(?m)^2$", "WATCH_INTERVAL not altered")


def _g_pipe(node, tmp_path):
    g_file = str(tmp_path / "g_file_1.out")
    pipe = "cat >{}".format(g_file)

    _psql_like(node, "SELECT 'one' \\g | {}".format(pipe), r"", "one command \\g")
    assert re.search(r"one", slurp_file(g_file))

    _psql_like(
        node,
        "SELECT 'two' \\; SELECT 'three' \\g | {}".format(pipe),
        r"",
        "two commands \\g",
    )
    assert re.search(r"two.*three", slurp_file(g_file), re.S)

    _psql_like(
        node,
        "\\set SHOW_ALL_RESULTS 0\nSELECT 'four' \\; SELECT 'five' \\g | {}".format(
            pipe
        ),
        r"",
        "two commands \\g with only last result",
    )
    c3 = slurp_file(g_file)
    assert re.search(r"five", c3)
    assert not re.search(r"four", c3)

    _psql_like(
        node,
        "copy (values ('foo'),('bar')) to stdout \\g | {}".format(pipe),
        r"",
        "copy output passed to \\g pipe",
    )
    assert re.search(r"foo.*bar", slurp_file(g_file), re.S)


def _pipelines(node):
    aborts = r"COPY in a pipeline is not supported, aborting connection"
    node.safe_psql("CREATE TABLE psql_pipeline()")
    log_location = node.current_log_position()

    _psql_fails_like(
        node,
        "\\startpipeline\nCOPY psql_pipeline FROM STDIN;\nSELECT 'val1';\n"
        "\\syncpipeline\n\\endpipeline",
        aborts,
        "COPY FROM in pipeline: fails",
    )
    node.wait_for_log(
        r"FATAL: .*terminating connection because protocol synchronization was lost",
        log_location,
    )

    _psql_fails_like(
        node,
        "\\startpipeline\nCOPY psql_pipeline TO STDOUT;\nSELECT 'val1';\n\\endpipeline",
        aborts,
        "COPY TO in pipeline: fails",
    )
    _psql_fails_like(
        node,
        "\\startpipeline\n\\copy psql_pipeline from stdin;\nSELECT 'val1';\n"
        "\\syncpipeline\n\\endpipeline",
        aborts,
        "\\copy from in pipeline: fails",
    )
    _psql_fails_like(
        node,
        "\\startpipeline\n\\copy psql_pipeline to stdout;\n\\syncpipeline\n"
        "\\endpipeline",
        aborts,
        "\\copy to in pipeline: fails",
    )

    _psql_fails_like(
        node,
        "\\restrict test\n\\! should_fail",
        r"backslash commands are restricted; only \\unrestrict is allowed",
        "meta-command in restrict mode fails",
    )
