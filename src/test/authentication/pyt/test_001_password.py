# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/test/authentication/t/001_password.pl.

Set of tests for authentication and pg_hba.conf, exercising the password
methods Plain, MD5-encrypted and SCRAM-encrypted, plus the require_auth
connection option, the log_connections GUC, SYSTEM_USER (including with parallel
workers), password expiration warnings, channel binding without SSL, .pgpass
processing, regular-expression matching for user/database names in pg_hba.conf,
and role membership policies (+role, samerole, samegroup). Requires
Unix-domain sockets.
"""

import os
import re
import sys
import time

import pytest


def _reset_pg_hba(node, database, role, hba_method):
    """Delete pg_hba.conf and write a single 'local <db> <role> <method>' rule.

    The rule deliberately uses a continuation line (backslash-newline) to
    exercise that parser path, mirroring the Perl helper, then reloads.
    """
    (node.datadir / "pg_hba.conf").unlink(missing_ok=True)
    node.append_conf(
        "local {} {}\\\n {}".format(database, role, hba_method),
        filename="pg_hba.conf",
    )
    node.reload()


def _test_conn(
    node,
    connstr,
    method,
    expected_res,
    *,
    log_like=None,
    log_unlike=None,
    expected_stderr=None,
):
    """Connect with connstr; assert success (0) or failure (else) and log match.

    For failures only the status code (and optional log match) is checked, as in
    the Perl test_conn helper; expected_stderr applies to the success case.
    """
    status_string = "success" if expected_res == 0 else "failed"
    testname = "authentication {} for method {}, connstr {}".format(
        status_string, method, connstr
    )
    if expected_res == 0:
        node.connect_ok(
            connstr,
            testname,
            expected_stderr=expected_stderr,
            log_like=log_like,
            log_unlike=log_unlike,
        )
    else:
        node.connect_fails(connstr, testname, log_like=log_like, log_unlike=log_unlike)


def _set_pgpassword(value):
    if value is None:
        os.environ.pop("PGPASSWORD", None)
    else:
        os.environ["PGPASSWORD"] = value


def _setup_roles(node):
    """Create roles for the password_expiration_warning_threshold tests."""
    current_year = time.localtime().tm_year
    expire_year = current_year - 1
    node.safe_psql(
        "CREATE ROLE expired LOGIN VALID UNTIL '{}-01-01' PASSWORD 'pass'".format(
            expire_year
        )
    )
    expire_year = current_year + 2
    node.safe_psql(
        "CREATE ROLE expiration_warnings LOGIN VALID UNTIL '{}-01-01' "
        "PASSWORD 'pass'".format(expire_year)
    )
    expire_year = current_year + 5
    node.safe_psql(
        "CREATE ROLE no_warnings LOGIN VALID UNTIL '{}-01-01' "
        "PASSWORD 'pass'".format(expire_year)
    )


def _test_log_connections(node):
    """Test behavior of the log_connections GUC."""
    node.safe_psql("CREATE DATABASE test_log_connections")

    log_connections = node.safe_psql(
        "SHOW log_connections;", dbname="test_log_connections"
    )
    assert log_connections == "on", "check log connections has expected value 'on'"

    node.connect_ok(
        "test_log_connections",
        "log_connections 'on' works as expected for backwards compatibility",
        log_like=[
            r"connection received",
            r"connection authenticated",
            r"connection authorized: user=\S+ database=test_log_connections",
        ],
        log_unlike=[r"connection ready"],
    )

    node.safe_psql(
        "ALTER SYSTEM SET log_connections = "
        "receipt,authorization,setup_durations;\n"
        "SELECT pg_reload_conf();",
        dbname="test_log_connections",
    )
    node.connect_ok(
        "test_log_connections",
        "log_connections with subset of specified options logs only those aspects",
        log_like=[
            r"connection received",
            r"connection authorized: user=\S+ database=test_log_connections",
            r"connection ready",
        ],
        log_unlike=[r"connection authenticated"],
    )

    node.safe_psql(
        "ALTER SYSTEM SET log_connections = 'all'; SELECT pg_reload_conf();",
        dbname="test_log_connections",
    )
    node.connect_ok(
        "test_log_connections",
        "log_connections 'all' logs all available connection aspects",
        log_like=[
            r"connection received",
            r"connection authenticated",
            r"connection authorized: user=\S+ database=test_log_connections",
            r"connection ready",
        ],
    )


def _create_password_roles(node, md5_works):
    """Create roles with SCRAM/MD5 passwords and tables for SYSTEM_USER tests."""
    assert (
        node.psql_capture(
            "SET password_encryption='scram-sha-256'; "
            "CREATE ROLE scram_role LOGIN PASSWORD 'pass';"
        ).rc
        == 0
    ), "created user with SCRAM password"
    expected_md5_rc = 0 if md5_works else 3
    assert (
        node.psql_capture(
            "SET password_encryption='md5'; "
            "CREATE ROLE md5_role LOGIN PASSWORD 'pass';"
        ).rc
        == expected_md5_rc
    ), "created user with md5 password"

    node.safe_psql(
        "CREATE TABLE sysuser_data (n) AS SELECT NULL FROM generate_series(1, 10);"
        " GRANT ALL ON sysuser_data TO scram_role;"
    )
    os.environ["PGPASSWORD"] = "pass"

    # A role that contains a comma to stress the parsing.
    node.safe_psql(
        "SET password_encryption='scram-sha-256'; "
        "CREATE ROLE \"scram,role\" LOGIN PASSWORD 'pass';"
    )

    # A role with a non-default iteration count.
    node.safe_psql(
        "SET password_encryption='scram-sha-256';\n"
        " SET scram_iterations=1024;\n"
        " CREATE ROLE scram_role_iter LOGIN PASSWORD 'pass';\n"
        " RESET scram_iterations;"
    )
    res = node.safe_psql(
        "SELECT substr(rolpassword,1,19) FROM pg_authid "
        "WHERE rolname = 'scram_role_iter'"
    )
    assert res == "SCRAM-SHA-256$1024:", "scram_iterations in server side ROLE"


def _test_password_command(node):
    """Clientside \\password uses scram_iterations when computing SCRAM secrets.

    Mirrors the IO::Pty-gated block in the Perl original: an interactive psql
    session sets scram_iterations and runs \\password, then the stored secret is
    checked to confirm the client-side iteration count was used.

    pypg's BackgroundPsql drives psql over pipes, not a PTY. psql's \\password
    prompts via simple_prompt_extended, which opens /dev/tty; when no
    controlling terminal is available (the meson test harness and this sandbox)
    it falls back to reading from stdin. The prompts carry no trailing newline,
    so the line-buffered reader cannot match them with query_until the way
    IPC::Run's byte-level pump does; the password lines are instead fed in
    sequence (psql consumes them in order from stdin), reproducing the same
    scenario and assertion faithfully.
    """
    session = node.background_psql("postgres")
    try:
        session.set_query_timer_restart()
        session.query("SET password_encryption='scram-sha-256';")
        session.query("SET scram_iterations=42;")
        session.send("\\password scram_role_iter\npass\npass\n")
        time.sleep(1.0)
    finally:
        session.quit()

    res = node.safe_psql(
        "SELECT substr(rolpassword,1,17) FROM pg_authid "
        "WHERE rolname = 'scram_role_iter'"
    )
    assert res == "SCRAM-SHA-256$42:", "scram_iterations in psql \\password command"


def _test_trust(node, md5_works):
    """For "trust" method, all users can connect; check SYSTEM_USER and require_auth."""
    _reset_pg_hba(node, "all", "all", "trust")
    _test_conn(
        node,
        "user=scram_role",
        "trust",
        0,
        log_like=[r'connection authenticated: user="scram_role" method=trust'],
    )
    if md5_works:
        _test_conn(
            node,
            "user=md5_role",
            "trust",
            0,
            log_like=[r'connection authenticated: user="md5_role" method=trust'],
        )

    # SYSTEM_USER is null when not authenticated.
    res = node.safe_psql("SELECT SYSTEM_USER IS NULL;")
    assert res == "t", "users with trust authentication use SYSTEM_USER = NULL"

    # SYSTEM_USER with parallel workers when not authenticated.
    res = node.safe_psql(
        "SET min_parallel_table_scan_size TO 0;\n"
        "SET parallel_setup_cost TO 0;\n"
        "SET parallel_tuple_cost TO 0;\n"
        "SET max_parallel_workers_per_gather TO 2;\n"
        "SELECT bool_and(SYSTEM_USER IS NOT DISTINCT FROM n) FROM sysuser_data;",
        connstr="user=scram_role",
    )
    assert (
        res == "t"
    ), "users with trust authentication use SYSTEM_USER = NULL in parallel workers"

    _test_require_auth_trust(node)


def _test_require_auth_trust(node):
    """require_auth interactions with trust authentication."""
    node.connect_ok("user=scram_role require_auth=", "empty require_auth succeeds")

    fail_methods = {
        "gss": "gss",
        "sspi": "sspi",
        "password": "password",
        "md5": "md5",
        "scram-sha-256": "scram-sha-256",
        "password,scram-sha-256": "password,scram-sha-256",
    }
    names = {
        "gss": "GSS authentication required, fails with trust auth",
        "sspi": "SSPI authentication required, fails with trust auth",
        "password": "password authentication required, fails with trust auth",
        "md5": "MD5 authentication required, fails with trust auth",
        "scram-sha-256": "SCRAM authentication required, fails with trust auth",
        "password,scram-sha-256": (
            "password and SCRAM authentication required, fails with trust auth"
        ),
    }
    for method, value in fail_methods.items():
        node.connect_fails(
            "user=scram_role require_auth={}".format(value),
            names[method],
            expected_stderr=r'authentication method requirement "{}" failed: '
            r"server did not complete authentication".format(re.escape(value)),
        )

    for value, name in [
        ("!gss", "GSS authentication can be forbidden, succeeds with trust auth"),
        ("!sspi", "SSPI authentication can be forbidden, succeeds with trust auth"),
        (
            "!password",
            "password authentication can be forbidden, succeeds with trust auth",
        ),
        ("!md5", "md5 authentication can be forbidden, succeeds with trust auth"),
        (
            "!scram-sha-256",
            "SCRAM authentication can be forbidden, succeeds with trust auth",
        ),
        (
            "!password,!scram-sha-256",
            "multiple authentication types forbidden, succeeds with trust auth",
        ),
    ]:
        node.connect_ok("user=scram_role require_auth={}".format(value), name)

    node.connect_ok(
        "user=scram_role require_auth=none",
        "all authentication types forbidden, succeeds with trust auth",
    )
    node.connect_fails(
        "user=scram_role require_auth=!none",
        "any authentication types required, fails with trust auth",
        expected_stderr=r"server did not complete authentication",
    )

    _test_require_auth_invalid(node)


def _test_require_auth_invalid(node):
    """require_auth values that are syntactically invalid."""
    node.connect_fails(
        "user=scram_role require_auth=scram-sha-256,!md5",
        "negative require_auth methods cannot be mixed with positive ones",
        expected_stderr=r'negative require_auth method "!md5" cannot be mixed '
        r"with non-negative methods",
    )
    node.connect_fails(
        "user=scram_role require_auth=!password,!none,scram-sha-256",
        "positive require_auth methods cannot be mixed with negative one",
        expected_stderr=r'require_auth method "scram-sha-256" cannot be mixed '
        r"with negative methods",
    )
    dup_cases = [
        (
            "password,md5,password",
            "require_auth methods cannot include duplicates, positive case",
            r'require_auth method "password" is specified more than once',
        ),
        (
            "!password,!md5,!password",
            "require_auth methods cannot be duplicated, negative case",
            r'require_auth method "!password" is specified more than once',
        ),
        (
            "none,md5,none",
            "require_auth methods cannot be duplicated, none case",
            r'require_auth method "none" is specified more than once',
        ),
        (
            "!none,!md5,!none",
            "require_auth methods cannot be duplicated, !none case",
            r'require_auth method "!none" is specified more than once',
        ),
        (
            "scram-sha-256,scram-sha-256",
            "require_auth methods cannot be duplicated, scram-sha-256 case",
            r'require_auth method "scram-sha-256" is specified more than once',
        ),
        (
            "!scram-sha-256,!scram-sha-256",
            "require_auth methods cannot be duplicated, !scram-sha-256 case",
            r'require_auth method "!scram-sha-256" is specified more than once',
        ),
    ]
    for value, name, stderr in dup_cases:
        node.connect_fails(
            "user=scram_role require_auth={}".format(value),
            name,
            expected_stderr=stderr,
        )
    node.connect_fails(
        "user=scram_role require_auth=none,abcdefg",
        "unknown require_auth methods are rejected",
        expected_stderr=r'invalid require_auth value: "abcdefg"',
    )


def _test_password_method(node, md5_works):
    """For plain "password" method, all users can connect; require_auth checks."""
    _reset_pg_hba(node, "all", "all", "password")
    _test_conn(
        node,
        "user=scram_role",
        "password",
        0,
        log_like=[r'connection authenticated: identity="scram_role" method=password'],
    )
    if md5_works:
        _test_conn(
            node,
            "user=md5_role",
            "password",
            0,
            log_like=[r'connection authenticated: identity="md5_role" method=password'],
        )

    node.connect_ok(
        "user=scram_role require_auth=password",
        "password authentication required, works with password auth",
    )
    node.connect_ok(
        "user=scram_role require_auth=!none",
        "any authentication required, works with password auth",
    )
    node.connect_ok(
        "user=scram_role require_auth=scram-sha-256,password,md5",
        "multiple authentication types required, works with password auth",
    )

    node.connect_fails(
        "user=scram_role require_auth=md5",
        "md5 authentication required, fails with password auth",
        expected_stderr=r'authentication method requirement "md5" failed: '
        r"server requested a cleartext password",
    )
    node.connect_fails(
        "user=scram_role require_auth=scram-sha-256",
        "SCRAM authentication required, fails with password auth",
        expected_stderr=r'authentication method requirement "scram-sha-256" '
        r"failed: server requested a cleartext password",
    )
    node.connect_fails(
        "user=scram_role require_auth=none",
        "all authentication forbidden, fails with password auth",
        expected_stderr=r'authentication method requirement "none" failed: '
        r"server requested a cleartext password",
    )
    node.connect_fails(
        "user=scram_role require_auth=!password",
        "password authentication forbidden, fails with password auth",
        expected_stderr=r"server requested a cleartext password",
    )
    node.connect_fails(
        "user=scram_role require_auth=!password,!md5,!scram-sha-256",
        "multiple authentication types forbidden, fails with password auth",
        expected_stderr=r' method requirement "!password,!md5,!scram-sha-256" '
        r"failed: server requested a cleartext password",
    )


def _test_scram_method(node):
    """For "scram-sha-256" method: scram_role connects, md5_role fails."""
    _reset_pg_hba(node, "all", "all", "scram-sha-256")
    _test_conn(
        node,
        "user=scram_role",
        "scram-sha-256",
        0,
        log_like=[
            r'connection authenticated: identity="scram_role" method=scram-sha-256'
        ],
    )
    _test_conn(
        node,
        "user=scram_role_iter",
        "scram-sha-256",
        0,
        log_like=[
            r'connection authenticated: identity="scram_role_iter" '
            r"method=scram-sha-256"
        ],
    )
    _test_conn(
        node,
        "user=md5_role",
        "scram-sha-256",
        2,
        log_unlike=[r"connection authenticated:"],
    )

    node.connect_ok(
        "user=scram_role require_auth=scram-sha-256",
        "SCRAM authentication required, works with SCRAM auth",
    )
    node.connect_ok(
        "user=scram_role require_auth=!none",
        "any authentication required, works with SCRAM auth",
    )
    node.connect_ok(
        "user=scram_role require_auth=password,scram-sha-256,md5",
        "multiple authentication types required, works with SCRAM auth",
    )

    node.connect_fails(
        "user=scram_role require_auth=password",
        "password authentication required, fails with SCRAM auth",
        expected_stderr=r'authentication method requirement "password" failed: '
        r"server requested SASL authentication",
    )
    node.connect_fails(
        "user=scram_role require_auth=md5",
        "md5 authentication required, fails with SCRAM auth",
        expected_stderr=r'authentication method requirement "md5" failed: '
        r"server requested SASL authentication",
    )
    node.connect_fails(
        "user=scram_role require_auth=none",
        "all authentication forbidden, fails with SCRAM auth",
        expected_stderr=r'authentication method requirement "none" failed: '
        r"server requested SASL authentication",
    )
    node.connect_fails(
        "user=scram_role require_auth=!scram-sha-256",
        "SCRAM authentication forbidden, fails with SCRAM auth",
        expected_stderr=r"server requested SCRAM-SHA-256 authentication",
    )
    node.connect_fails(
        "user=scram_role require_auth=!password,!md5,!scram-sha-256",
        "multiple authentication types forbidden, fails with SCRAM auth",
        expected_stderr=r"server requested SCRAM-SHA-256 authentication",
    )

    # Bad passwords are rejected.
    os.environ["PGPASSWORD"] = "badpass"
    _test_conn(
        node,
        "user=scram_role",
        "scram-sha-256",
        2,
        log_unlike=[r"connection authenticated:"],
    )
    os.environ["PGPASSWORD"] = "pass"


def _test_md5_method(node, md5_works):
    """For "md5" method: all users connect (SCRAM used for SCRAM secrets)."""
    _reset_pg_hba(node, "all", "all", "md5")
    _test_conn(
        node,
        "user=scram_role",
        "md5",
        0,
        log_like=[r'connection authenticated: identity="scram_role" method=md5'],
    )
    if md5_works:
        _test_conn(
            node,
            "user=md5_role",
            "md5",
            0,
            expected_stderr=r"authenticated with an MD5-encrypted password",
            log_like=[r'connection authenticated: identity="md5_role" method=md5'],
        )

    node.connect_ok(
        "user=scram_role require_auth=scram-sha-256",
        "SCRAM authentication required, works with SCRAM auth",
    )
    node.connect_ok(
        "user=scram_role require_auth=!none",
        "any authentication required, works with SCRAM auth",
    )
    node.connect_ok(
        "user=scram_role require_auth=md5,scram-sha-256,password",
        "multiple authentication types required, works with SCRAM auth",
    )

    node.connect_fails(
        "user=scram_role require_auth=password",
        "password authentication required, fails with SCRAM auth",
        expected_stderr=r'authentication method requirement "password" failed: '
        r"server requested SASL authentication",
    )
    node.connect_fails(
        "user=scram_role require_auth=md5",
        "MD5 authentication required, fails with SCRAM auth",
        expected_stderr=r'authentication method requirement "md5" failed: '
        r"server requested SASL authentication",
    )
    node.connect_fails(
        "user=scram_role require_auth=none",
        "all authentication types forbidden, fails with SCRAM auth",
        expected_stderr=r'authentication method requirement "none" failed: '
        r"server requested SASL authentication",
    )
    node.connect_fails(
        "user=scram_role require_auth=!scram-sha-256",
        "password authentication forbidden, fails with SCRAM auth",
        expected_stderr=r'authentication method requirement "!scram-sha-256" '
        r"failed: server requested SCRAM-SHA-256 authentication",
    )
    node.connect_fails(
        "user=scram_role require_auth=!password,!md5,!scram-sha-256",
        "multiple authentication types forbidden, fails with SCRAM auth",
        expected_stderr=r"authentication method requirement "
        r'"!password,!md5,!scram-sha-256" failed: server requested '
        r"SCRAM-SHA-256 authentication",
    )


def _test_password_expiration(node):
    """Test password_expiration_warning_threshold behaviour."""
    node.connect_fails(
        "user=expired dbname=postgres",
        "connection fails due to expired password",
        expected_stderr=r'password authentication failed for user "expired"',
    )
    node.connect_ok(
        "user=expiration_warnings dbname=postgres",
        "connection succeeds with password expiration warning",
        expected_stderr=r"role password will expire soon",
    )
    node.connect_ok(
        "user=no_warnings dbname=postgres",
        "connection succeeds with no password expiration warning",
    )


def _test_system_user_parallel(node):
    """SYSTEM_USER != NULL with parallel workers under md5."""
    node.safe_psql(
        "TRUNCATE sysuser_data;\n"
        "INSERT INTO sysuser_data SELECT 'md5:scram_role' "
        "FROM generate_series(1, 10);",
        connstr="user=scram_role",
    )
    res = node.safe_psql(
        "SET min_parallel_table_scan_size TO 0;\n"
        "SET parallel_setup_cost TO 0;\n"
        "SET parallel_tuple_cost TO 0;\n"
        "SET max_parallel_workers_per_gather TO 2;\n"
        "SELECT bool_and(SYSTEM_USER IS NOT DISTINCT FROM n) FROM sysuser_data;",
        connstr="user=scram_role",
    )
    assert res == "t", (
        "users with md5 authentication use SYSTEM_USER = md5:role "
        "in parallel workers"
    )


def _test_channel_binding(node):
    """Channel binding without SSL can't work, for password and SCRAM methods."""
    _reset_pg_hba(node, "all", "all", "password")
    os.environ["PGCHANNELBINDING"] = "require"
    _test_conn(node, "user=scram_role", "scram-sha-256", 2)
    _reset_pg_hba(node, "all", "all", "scram-sha-256")
    os.environ["PGCHANNELBINDING"] = "require"
    _test_conn(node, "user=scram_role", "scram-sha-256", 2)


def _test_pgpass(node, tmp_path):
    """Test .pgpass processing using a temporary file."""
    pgpassfile = str(tmp_path / "pgpass")
    _set_pgpassword(None)
    os.environ.pop("PGCHANNELBINDING", None)
    os.environ["PGPASSFILE"] = pgpassfile

    _unlink(pgpassfile)
    long_comment = (
        "This very long comment is just here to exercise handling "
        "of long lines in the file. "
    )
    with open(pgpassfile, "w", encoding="utf-8") as fh:
        fh.write(
            "\n# {}\n*:*:postgres:scram_role:pass:this is not part of the "
            "password.\n".format(long_comment * 5)
        )
    os.chmod(pgpassfile, 0o600)

    _reset_pg_hba(node, "all", "all", "password")
    _test_conn(node, "user=scram_role", "password from pgpass", 0)
    _test_conn(node, "user=md5_role", "password from pgpass", 2)

    with open(pgpassfile, "a", encoding="utf-8") as fh:
        fh.write("\n*:*:*:scram_role:p\\ass\n*:*:*:scram,role:p\\ass\n")

    _test_conn(node, "user=scram_role", "password from pgpass", 0)

    _test_regex_hba(node)

    _unlink(pgpassfile)
    os.environ.pop("PGPASSFILE", None)


def _test_regex_hba(node):
    """Regular-expression matching for user/database names in pg_hba.conf."""
    # User regexp; the third regexp matches.
    _reset_pg_hba(node, "all", "/^.*nomatch.*$, baduser, /^scr.*$", "password")
    _test_conn(
        node,
        "user=scram_role",
        "password, matching regexp for username",
        0,
        log_like=[r'connection authenticated: identity="scram_role" method=password'],
    )
    # The third regexp no longer matches.
    _reset_pg_hba(node, "all", "/^.*nomatch.*$, baduser, /^sc_r.*$", "password")
    _test_conn(
        node,
        "user=scram_role",
        "password, non matching regexp for username",
        2,
        log_unlike=[r"connection authenticated:"],
    )
    # A comma in the regular expression; double quotes are mandatory.
    _reset_pg_hba(node, "all", '"/^.*m,.*e$"', "password")
    _test_conn(
        node,
        "user=scram,role",
        "password, matching regexp for username",
        0,
        log_like=[r'connection authenticated: identity="scram,role" method=password'],
    )
    # dbname regexp; the third regexp matches.
    _reset_pg_hba(node, "/^.*nomatch.*$, baddb, /^regex_t.*b$", "all", "password")
    _test_conn(
        node,
        "user=scram_role dbname=regex_testdb",
        "password, matching regexp for dbname",
        0,
        log_like=[r'connection authenticated: identity="scram_role" method=password'],
    )
    # The third regexp no longer matches.
    _reset_pg_hba(node, "/^.*nomatch.*$, baddb, /^regex_t.*ba$", "all", "password")
    _test_conn(
        node,
        "user=scram_role dbname=regex_testdb",
        "password, non matching regexp for dbname",
        2,
        log_unlike=[r"connection authenticated:"],
    )


def _test_role_membership(node):
    """Authentication tests with specific HBA policies on roles."""
    _reset_pg_hba(node, "all", "all", "trust")
    node.safe_psql("CREATE DATABASE regress_regression_group;")
    node.safe_psql(
        "CREATE ROLE regress_regression_group LOGIN PASSWORD 'pass';\n"
        "CREATE ROLE regress_member LOGIN SUPERUSER IN ROLE "
        "regress_regression_group PASSWORD 'pass';\n"
        "CREATE ROLE regress_not_member LOGIN SUPERUSER PASSWORD 'pass';"
    )
    os.environ["PGPASSWORD"] = "pass"

    auth_re = r'connection authenticated: identity="{}" method=scram-sha-256'

    # Exact matching, no members allowed.
    _reset_pg_hba(node, "all", "regress_regression_group", "scram-sha-256")
    _membership_triple(node, auth_re, member_ok=False, not_member_ok=False)

    # '+' membership: all members are allowed.
    _reset_pg_hba(node, "all", "+regress_regression_group", "scram-sha-256")
    _membership_triple(node, auth_re, member_ok=True, not_member_ok=False)

    # samerole respects membership. The Perl test sets PGDATABASE to select the
    # connection database; pypg's connection environment always pins
    # PGDATABASE=postgres, so the database is carried in the connstr instead
    # (semantically identical for these connection-outcome checks).
    _reset_pg_hba(node, "samerole", "all", "scram-sha-256")
    _membership_triple(
        node,
        auth_re,
        member_ok=True,
        not_member_ok=False,
        dbname="regress_regression_group",
    )

    # samegroup respects membership.
    _reset_pg_hba(node, "samegroup", "all", "scram-sha-256")
    _membership_triple(
        node,
        auth_re,
        member_ok=True,
        not_member_ok=False,
        dbname="regress_regression_group",
    )


def _membership_triple(node, auth_re, *, member_ok, not_member_ok, dbname=None):
    """Run the group/member/not-member connection triple for a membership policy.

    dbname, when given, is appended to each connstr so the connection targets a
    specific database (used by the samerole/samegroup policies).
    """
    suffix = " dbname={}".format(dbname) if dbname else ""
    _test_conn(
        node,
        "user=regress_regression_group" + suffix,
        "scram-sha-256",
        0,
        log_like=[auth_re.format("regress_regression_group")],
    )
    if member_ok:
        _test_conn(
            node,
            "user=regress_member" + suffix,
            "scram-sha-256",
            0,
            log_like=[auth_re.format("regress_member")],
        )
    else:
        _test_conn(
            node,
            "user=regress_member" + suffix,
            "scram-sha-256",
            2,
            log_unlike=[auth_re.format("regress_member")],
        )
    if not_member_ok:
        _test_conn(
            node,
            "user=regress_not_member" + suffix,
            "scram-sha-256",
            0,
            log_like=[auth_re.format("regress_not_member")],
        )
    else:
        _test_conn(
            node,
            "user=regress_not_member" + suffix,
            "scram-sha-256",
            2,
            log_unlike=[auth_re.format("regress_not_member")],
        )


def _unlink(path):
    try:
        os.unlink(path)
    except FileNotFoundError:
        pass


@pytest.mark.skipif(sys.platform == "win32", reason="needs Unix-domain sockets")
def test_001_password(create_pg, tmp_path):
    """Password authentication, require_auth, log_connections, pgpass and HBA."""
    node = create_pg("primary", start=False)
    node.append_conf("log_connections = on\n")
    # Needed to allow connect_fails to inspect the postmaster log.
    node.append_conf("log_min_messages = debug2")
    node.append_conf("password_expiration_warning_threshold = '1100d'")
    node.start()

    _setup_roles(node)
    _test_log_connections(node)

    # md5 could fail in FIPS mode.
    md5_works = node.psql_capture("select md5('')").rc == 0

    _create_password_roles(node, md5_works)
    _test_password_command(node)

    # Database used by the regular-expression dbname tests.
    node.safe_psql("CREATE database regex_testdb;")

    _test_trust(node, md5_works)
    _test_password_method(node, md5_works)
    _test_scram_method(node)
    _test_md5_method(node, md5_works)
    _test_password_expiration(node)
    _test_system_user_parallel(node)
    _test_channel_binding(node)
    _test_pgpass(node, tmp_path)
    _test_role_membership(node)
