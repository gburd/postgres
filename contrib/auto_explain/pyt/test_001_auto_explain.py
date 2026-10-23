# Copyright (c) 2021-2026, PostgreSQL Global Development Group

# pylint: disable=line-too-long
"""Port of contrib/auto_explain/t/001_auto_explain.pl.

auto_explain logs plans for statements: query text, parameters (with truncation
and disabling), seq/index scan nodes, query identifier (compute_query_id),
JSON format, extension options, non-superuser SET-permission behavior, and
pg_get_loaded_modules() reporting. Each query is run with per-query GUCs via
PGOPTIONS and the freshly appended log slice is examined.
"""

import re

import pypg


def _query_log(node, sql, params=None, user=None):
    extra_env = {}
    if params:
        extra_env["PGOPTIONS"] = " ".join(
            "-c {}={}".format(k, v) for k, v in params.items()
        )
    if user:
        extra_env["PGUSER"] = user
    offset = node.current_log_position()
    node.safe_psql(sql, extra_env=extra_env or None)
    return pypg.slurp_file(node.log, offset)


def _like(log, pattern, msg, flags=0):
    assert re.search(pattern, log, flags), "{}: {!r} not found".format(msg, pattern)


def _unlike(log, pattern, msg):
    assert not re.search(pattern, log), "{}: {!r} unexpectedly found".format(
        msg, pattern
    )


def test_001_auto_explain(create_pg):
    """auto_explain plan logging across formats, parameters, and permissions."""
    node = create_pg("main", auth_extra=["--create-role", "regress_user1"], start=False)
    node.append_conf("session_preload_libraries = 'pg_overexplain,auto_explain'")
    node.append_conf("auto_explain.log_min_duration = 0")
    node.append_conf("auto_explain.log_analyze = on")
    node.start()
    log = _query_log(node, "SELECT * FROM pg_class;")
    _like(log, r"Query Text: SELECT \* FROM pg_class;", "query text logged, text mode")
    _unlike(
        log, r"Query Parameters:", "no query parameters logged when none, text mode"
    )
    _like(log, r"Seq Scan on pg_class", "sequential scan logged, text mode")
    log = _query_log(
        node,
        "PREPARE get_proc(name) AS SELECT * FROM pg_proc WHERE proname = $1; "
        "EXECUTE get_proc('int4pl');",
    )
    _like(
        log,
        r"Query Text: PREPARE get_proc\(name\) AS SELECT \* FROM pg_proc WHERE proname = \$1;",
        "prepared query text logged, text mode",
    )
    _like(
        log, r"Query Parameters: \$1 = 'int4pl'", "query parameters logged, text mode"
    )
    _like(
        log,
        r"Index Scan using pg_proc_proname_args_nsp_index on pg_proc",
        "index scan logged, text mode",
    )
    log = _query_log(
        node,
        "PREPARE get_type(name) AS SELECT * FROM pg_type WHERE typname = $1; "
        "EXECUTE get_type('float8');",
        {"auto_explain.log_parameter_max_length": 3},
    )
    _like(
        log,
        r"Query Text: PREPARE get_type\(name\) AS SELECT \* FROM pg_type WHERE typname = \$1;",
        "prepared query text logged, text mode",
    )
    _like(
        log,
        r"Query Parameters: \$1 = 'flo\.\.\.'",
        "query parameters truncated, text mode",
    )
    log = _query_log(
        node,
        "PREPARE get_type(name) AS SELECT * FROM pg_type WHERE typname = $1; "
        "EXECUTE get_type('float8');",
        {"auto_explain.log_parameter_max_length": 0},
    )
    _like(
        log,
        r"Query Text: PREPARE get_type\(name\) AS SELECT \* FROM pg_type WHERE typname = \$1;",
        "prepared query text logged, text mode",
    )
    _unlike(
        log,
        r"Query Parameters:",
        "query parameters not logged when disabled, text mode",
    )
    log = _query_log(
        node,
        "SELECT * FROM pg_class;",
        {"auto_explain.log_verbose": "on", "compute_query_id": "on"},
    )
    _like(
        log,
        r"Query Identifier:",
        "query identifier logged with compute_query_id=on, text mode",
    )
    log = _query_log(
        node,
        "SELECT * FROM pg_class;",
        {"auto_explain.log_verbose": "on", "compute_query_id": "regress"},
    )
    _unlike(
        log,
        r"Query Identifier:",
        "query identifier not logged with compute_query_id=regress, text mode",
    )
    log = _query_log(
        node, "SELECT * FROM pg_class;", {"auto_explain.log_format": "json"}
    )
    _like(
        log, r'"Query Text": "SELECT \* FROM pg_class;"', "query text logged, json mode"
    )
    _unlike(
        log, r'"Query Parameters":', "query parameters not logged when none, json mode"
    )
    _like(
        log,
        r'"Node Type": "Seq Scan"[^}]*"Relation Name": "pg_class"',
        "sequential scan logged, json mode",
        re.DOTALL,
    )
    log = _query_log(
        node,
        "PREPARE get_class(name) AS SELECT * FROM pg_class WHERE relname = $1; "
        "EXECUTE get_class('pg_class');",
        {"auto_explain.log_format": "json"},
    )
    _like(
        log,
        r'"Query Text": "PREPARE get_class\(name\) AS SELECT \* FROM pg_class WHERE relname = \$1;"',
        "prepared query text logged, json mode",
    )
    _like(
        log,
        r'"Node Type": "Index Scan"[^}]*"Index Name": "pg_class_relname_nsp_index"',
        "index scan logged, json mode",
        re.DOTALL,
    )
    log = _query_log(node, "SELECT 1;", {"auto_explain.log_extension_options": "debug"})
    _like(log, r"Parallel Safe:", "extension option produces per-node output")
    _like(log, r"Command Type: select", "extension option produces per-plan output")
    node.safe_psql(
        "CREATE USER regress_user1;\n"
        "GRANT SET ON PARAMETER auto_explain.log_format TO regress_user1;"
    )
    log = _query_log(
        node,
        "SELECT * FROM pg_database;",
        {"auto_explain.log_format": "json"},
        user="regress_user1",
    )
    _like(
        log,
        r'"Query Text": "SELECT \* FROM pg_database;"',
        "query text logged, json mode selected by non-superuser",
    )
    log = _query_log(
        node,
        "SELECT * FROM pg_database;",
        {"auto_explain.log_level": "log"},
        user="regress_user1",
    )
    _like(
        log,
        r'WARNING: ( 42501:)? permission denied to set parameter "auto_explain\.log_level"',
        "permission failure logged",
    )
    node.safe_psql(
        "REVOKE SET ON PARAMETER auto_explain.log_format FROM regress_user1;\n"
        "DROP USER regress_user1;"
    )
    res = node.safe_psql(
        "SELECT module_name,\n"
        "       version = current_setting('server_version') as version_ok,\n"
        "       regexp_replace(file_name, '\\..*', '') as file_name_stripped\n"
        "FROM pg_get_loaded_modules()\n"
        "WHERE module_name = 'auto_explain';"
    )
    _like(res, r"^auto_explain\|t\|auto_explain$", "pg_get_loaded_modules() ok")
