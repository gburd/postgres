# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/test/authentication/t/003_peer.pl.

Tests peer authentication and the user name map. Peer auth maps the connecting
OS user to a database role, optionally through pg_ident.conf maps that may use
exact names, the "all" keyword, regular expressions (with \\1 subexpression
replacement), and group membership ("+role"). The test is skipped if the
platform does not support peer authentication, and requires Unix-domain sockets.

The OS/system user is discovered from the server itself via SYSTEM_USER (which
reads "peer:username" under peer auth), matching the Perl original rather than
guessing from the process environment.
"""

import re
import sys

import pytest


def _reset_pg_hba(node, hba_method):
    """Delete pg_hba.conf, write a single 'local all all <method>' line, reload."""
    (node.datadir / "pg_hba.conf").unlink(missing_ok=True)
    node.append_conf("local all all {}".format(hba_method), filename="pg_hba.conf")
    node.reload()


def _reset_pg_ident(node, map_name, system_user, pg_user):
    """Delete pg_ident.conf, write a single map line, reload."""
    (node.datadir / "pg_ident.conf").unlink(missing_ok=True)
    node.append_conf(
        "{} {} {}".format(map_name, system_user, pg_user), filename="pg_ident.conf"
    )
    node.reload()


def _test_role(node, role, method, expected_res, test_details, *, log_like=None):
    """Connect as role and assert success (expected_res 0) or failure (else).

    For a failure only the status code is checked (no error-message match), as in
    the Perl test_role helper. log_like patterns are asserted against the server
    log emitted during the attempt.
    """
    connstr = "user={}".format(role)
    status_string = "success" if expected_res == 0 else "failed"
    testname = "authentication {} for method {}, role {} {}".format(
        status_string, method, role, test_details
    )
    if expected_res == 0:
        node.connect_ok(connstr, testname, log_like=log_like)
    else:
        node.connect_fails(connstr, testname, log_like=log_like)


def _create_roles(node):
    """Create the roles and group used by the user name map tests."""
    node.safe_psql("CREATE ROLE testmapuser LOGIN")
    node.safe_psql("CREATE ROLE testmapgroup NOLOGIN")
    node.safe_psql("GRANT testmapgroup TO testmapuser")
    # Note the double backslash in the role name.
    node.safe_psql(r'CREATE ROLE "testmapgroupliteral\1" LOGIN')
    node.safe_psql(r'GRANT "testmapgroupliteral\1" TO testmapuser')


def _run_map_tests(node, system_user):
    """Run the user-name-map matching scenarios for the given system user."""
    auth_ok = [
        re.compile(
            r'connection authenticated: identity="{}" method=peer'.format(
                re.escape(system_user)
            )
        )
    ]

    # With a user name map.
    _reset_pg_ident(node, "mypeermap", system_user, "testmapuser")
    _reset_pg_hba(node, "peer map=mypeermap")
    _test_role(node, "testmapuser", "peer", 0, "with user name map", log_like=auth_ok)

    # With the "all" keyword.
    _reset_pg_ident(node, "mypeermap", system_user, "all")
    _test_role(
        node,
        "testmapuser",
        "peer",
        0,
        'with keyword "all" as database user in user name map',
        log_like=auth_ok,
    )

    # With the "all" keyword, but quoted (no effect here).
    _reset_pg_ident(node, "mypeermap", system_user, '"all"')
    _test_role(
        node,
        "testmapuser",
        "peer",
        2,
        'with quoted keyword "all" as database user in user name map',
        log_like=[r'no match in usermap "mypeermap" for user "testmapuser"'],
    )

    # Regexp of the database user matches.
    _reset_pg_ident(node, "mypeermap", system_user, r"/^testm.*$")
    _test_role(
        node,
        "testmapuser",
        "peer",
        0,
        "with regexp of database user in user name map",
        log_like=auth_ok,
    )

    # Regexp of the database user does not match.
    _reset_pg_ident(node, "mypeermap", system_user, r"/^doesnotmatch.*$")
    _test_role(
        node,
        "testmapuser",
        "peer",
        2,
        "with bad regexp of database user in user name map",
        log_like=[r'no match in usermap "mypeermap" for user "testmapuser"'],
    )

    _run_system_user_regex_tests(node, system_user, auth_ok)
    _run_backref_tests(node, system_user, auth_ok)
    _run_group_tests(node, system_user, auth_ok)


def _run_system_user_regex_tests(node, system_user, auth_ok):
    """Map tests that use a regular expression for the system user."""
    # Last 3 chars of the system user (or the whole name if <= 3 chars).
    regex_test_string = system_user[-3:]

    # System user regular expression matches.
    _reset_pg_ident(
        node, "mypeermap", r"/^.*{}$".format(regex_test_string), "testmapuser"
    )
    _test_role(
        node,
        "testmapuser",
        "peer",
        0,
        "with regexp of system user in user name map",
        log_like=auth_ok,
    )

    # Both regular expressions match.
    _reset_pg_ident(
        node, "mypeermap", r"/^.*{}$".format(regex_test_string), r"/^testm.*$"
    )
    _test_role(
        node,
        "testmapuser",
        "peer",
        0,
        "with regexps for both system and database user in user name map",
        log_like=auth_ok,
    )

    # Regexp matches and database role is the "all" keyword.
    _reset_pg_ident(node, "mypeermap", r"/^.*{}$".format(regex_test_string), "all")
    _test_role(
        node,
        "testmapuser",
        "peer",
        0,
        'with regexp of system user and keyword "all" in user name map',
        log_like=auth_ok,
    )


def _run_backref_tests(node, system_user, auth_ok):
    """Map tests exercising \\1 subexpression replacement and its errors."""
    regex_test_string = system_user[-3:]
    mapped_name = "test{0}map{0}user".format(regex_test_string)
    node.safe_psql("CREATE ROLE {} LOGIN".format(mapped_name))

    # Regexp matches and \1 is replaced in the subexpression.
    _reset_pg_ident(
        node, "mypeermap", r"/^.*({})$".format(regex_test_string), r"test\1map\1user"
    )
    _test_role(
        node,
        mapped_name,
        "peer",
        0,
        r"with regular expression in user name map with \1 replaced",
        log_like=auth_ok,
    )

    # Regexp matches and \1 is replaced, even if quoted.
    _reset_pg_ident(
        node,
        "mypeermap",
        r"/^.*({})$".format(regex_test_string),
        r'"test\1map\1user"',
    )
    _test_role(
        node,
        mapped_name,
        "peer",
        0,
        r"with regular expression in user name map with quoted \1 replaced",
        log_like=auth_ok,
    )

    # The regexp has no subexpression, but the database user contains \1.
    _reset_pg_ident(node, "mypeermap", r"/^{}$".format(system_user), r"\1testmapuser")
    _test_role(
        node,
        "testmapuser",
        "peer",
        2,
        r"with regular expression in user name map with \1 not replaced",
        log_like=[
            r'regular expression "\^{}\$" has no subexpressions as requested '
            r'by backreference in "\\1testmapuser"'.format(re.escape(system_user))
        ],
    )

    # Regexp of the system user does not match (doubled system user).
    bad_regex_test_string = system_user + system_user
    _reset_pg_ident(
        node, "mypeermap", r"/^.*{}$".format(bad_regex_test_string), "testmapuser"
    )
    _test_role(
        node,
        "testmapuser",
        "peer",
        2,
        "with regexp of system user in user name map",
        log_like=[r'no match in usermap "mypeermap" for user "testmapuser"'],
    )


def _run_group_tests(node, system_user, auth_ok):
    """Map tests exercising group ("+role") membership matching."""
    regex_test_string = system_user[-3:]

    # Group role match for the database user.
    _reset_pg_ident(node, "mypeermap", system_user, "+testmapgroup")
    _test_role(
        node, "testmapuser", "peer", 0, "plain user with group", log_like=auth_ok
    )
    _test_role(
        node,
        "testmapgroup",
        "peer",
        2,
        "group user with group",
        log_like=[r'role "testmapgroup" is not permitted to log in'],
    )

    # Quotes on the group match nullify its effect.
    _reset_pg_ident(node, "mypeermap", system_user, '"+testmapgroup"')
    _test_role(
        node,
        "testmapuser",
        "peer",
        2,
        "plain user with quoted group name",
        log_like=[r'no match in usermap "mypeermap" for user "testmapuser"'],
    )

    # Regexp for the system user, with a group membership check.
    _reset_pg_ident(
        node, "mypeermap", r"/^.*{}$".format(regex_test_string), "+testmapgroup"
    )
    _test_role(
        node,
        "testmapuser",
        "peer",
        0,
        "regexp of system user as group member",
        log_like=auth_ok,
    )
    _test_role(
        node,
        "testmapgroup",
        "peer",
        2,
        "regexp of system user as non-member of group",
        log_like=[r'role "testmapgroup" is not permitted to log in'],
    )

    # Membership checks and regexes use literal \1 instead of replacing it.
    _reset_pg_ident(
        node,
        "mypeermap",
        r"/^.*{}(.*)$".format(regex_test_string),
        r"+testmapgroupliteral\1",
    )
    _test_role(
        node,
        "testmapuser",
        "peer",
        0,
        r"membership check with literal \1",
        log_like=auth_ok,
    )

    # Same with a quoted regular expression for the database user; no \1 repl.
    _reset_pg_ident(
        node,
        "mypeermap",
        r"/^.*{}(.*)$".format(regex_test_string),
        r'"/^testmapgroupliteral\\1$"',
    )
    _test_role(
        node,
        r"testmapgroupliteral\\1",
        "peer",
        0,
        r"regexp of database user with literal \1",
        log_like=auth_ok,
    )


@pytest.mark.skipif(sys.platform == "win32", reason="needs Unix-domain sockets")
def test_003_peer(create_pg):
    """Peer authentication and user name map matching scenarios."""
    node = create_pg("node", start=False)
    node.append_conf("log_connections = authentication\n")
    # Needed to allow connect_fails to inspect the postmaster log.
    node.append_conf("log_min_messages = debug2")
    node.start()

    # Set pg_hba.conf with the peer authentication.
    _reset_pg_hba(node, "peer")

    # Check if peer authentication is supported on this platform.
    log_offset = node.current_log_position()
    node.psql_capture("", on_error_stop=False)
    if node.log_matches(
        r"peer authentication is not supported on this platform", log_offset
    ):
        pytest.skip("peer authentication is not supported on this platform")

    _create_roles(node)

    # Extract the system user for the user name map.
    system_user = node.safe_psql("select (string_to_array(SYSTEM_USER, ':'))[2]")

    # While on it, check the status of huge pages: either on or off, never
    # unknown.
    huge_pages_status = node.safe_psql("SHOW huge_pages_status;")
    assert huge_pages_status != "unknown", "check huge_pages_status"

    # Without the user name map: failure as the database role does not map to an
    # authorized system user.
    _test_role(
        node,
        "testmapuser",
        "peer",
        2,
        "without user name map",
        log_like=[r'Peer authentication failed for user "testmapuser"'],
    )

    _run_map_tests(node, system_user)
