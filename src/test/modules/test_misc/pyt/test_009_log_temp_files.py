# Copyright (c) 2024-2026, PostgreSQL Global Development Group

# pylint: disable=line-too-long
"""Port of src/test/modules/test_misc/t/009_log_temp_files.pl.

With log_temp_files=0 and tiny work_mem, a sort that spills to a temporary file
logs the temp-file removal attributed to the right statement across many
protocol/portal shapes: unnamed/named portals, extended-protocol bind without a
query, pipelined queries, simple queries, cursors (and WITH HOLD), and
prepare/execute. Bind-without-query cases log the temp file but no STATEMENT line.
"""


def _temp_then_statement(stmt):
    return r"(?s)LOG:\s+temporary file: path.*\n.* STATEMENT:\s+" + stmt


def test_009_log_temp_files(create_pg):
    """Temp-file logging is attributed to the right statement across portals."""
    node = create_pg("primary", start=False)
    node.append_conf(
        "\nwork_mem = 64kB\nlog_temp_files = 0\ndebug_parallel_query = off\n"
        "log_error_verbosity = default\n"
    )
    node.start()
    node.safe_psql(
        "CREATE UNLOGGED TABLE foo(a int);\n"
        "INSERT INTO foo(a) SELECT * FROM generate_series(1, 5000);"
    )
    off = node.current_log_position()
    node.safe_psql(
        "BEGIN;\nSELECT a FROM foo ORDER BY a OFFSET $1 \\bind 4990 \\g\n"
        "SELECT 'unnamed portal';\nEND;"
    )
    assert node.log_matches(
        _temp_then_statement(r"SELECT 'unnamed portal'"), off
    ), "unnamed portal"
    off = node.current_log_position()
    node.safe_psql("SELECT a FROM foo ORDER BY a OFFSET $1 \\bind 4991 \\g\n")
    assert node.log_matches(
        r"(?s)LOG:\s+temporary file:", off
    ), "bind and implicit transaction, temporary file removed"
    assert not node.log_matches(
        r"(?s)STATEMENT:", off
    ), "bind and implicit transaction, no statement logged"
    node.safe_psql(
        "BEGIN;\nSELECT a FROM foo ORDER BY a OFFSET $1 \\parse stmt\n"
        "\\bind_named stmt 4999 \\g\nSELECT 'named portal';\nEND;"
    )
    assert node.log_matches(
        _temp_then_statement(r"SELECT 'named portal'"), off
    ), "named portal"
    off = node.current_log_position()
    node.safe_psql(
        "\\startpipeline\n"
        "SELECT a FROM foo ORDER BY a OFFSET $1 \\bind 4992 \\sendpipeline\n"
        "SELECT 'pipelined query';\n\\endpipeline\n"
    )
    assert node.log_matches(
        _temp_then_statement(r"SELECT 'pipelined query'"), off
    ), "pipelined query"
    off = node.current_log_position()
    node.safe_psql(
        "SELECT a, a, a FROM foo ORDER BY a OFFSET $1 \\parse p1\n"
        "\\bind_named p1 4993 \\g\n"
    )
    assert node.log_matches(
        r"(?s)LOG:\s+temporary file:", off
    ), "parse and bind, temporary file removed"
    assert not node.log_matches(
        r"(?s)STATEMENT:", off
    ), "bind and bind, no statement logged"
    off = node.current_log_position()
    node.safe_psql("BEGIN;\nSELECT a FROM foo ORDER BY a OFFSET 4994;\nEND;")
    assert node.log_matches(
        _temp_then_statement(r"SELECT a FROM foo ORDER BY a OFFSET 4994;"), off
    ), "simple query"
    _cursor_and_prepare(node)


def _cursor_and_prepare(node):
    off = node.current_log_position()
    node.safe_psql(
        "BEGIN;\nDECLARE mycur CURSOR FOR SELECT a FROM foo ORDER BY a OFFSET 4995;\n"
        "FETCH 10 FROM mycur;\nSELECT 1;\nCLOSE mycur;\nEND;"
    )
    assert node.log_matches(_temp_then_statement(r"CLOSE mycur;"), off), "cursor"
    off = node.current_log_position()
    node.safe_psql(
        "BEGIN;\nDECLARE holdcur CURSOR WITH HOLD FOR SELECT a FROM foo ORDER BY a "
        "OFFSET 4996;\nFETCH 10 FROM holdcur;\nCOMMIT;\nCLOSE holdcur;"
    )
    assert node.log_matches(_temp_then_statement(r"COMMIT;"), off), "cursor WITH HOLD"
    off = node.current_log_position()
    node.safe_psql(
        "BEGIN;\nPREPARE p1 AS SELECT a FROM foo ORDER BY a OFFSET 4997;\n"
        "EXECUTE p1;\nDEALLOCATE p1;\nEND;"
    )
    assert node.log_matches(
        _temp_then_statement(r"EXECUTE p1;"), off
    ), "prepare/execute"
    node.stop("fast")
