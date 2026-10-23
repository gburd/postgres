# Copyright (c) 2023-2026, PostgreSQL Global Development Group

"""Port of src/test/postmaster/t/002_connection_limits.pl.

Connection-slot reservation tiers: with max_connections=6,
reserved_connections=2, superuser_reserved_connections=1, regular users are
refused once the unreserved slots fill (reserved for pg_use_reserved_connections
roles), then reserved-privilege users are refused (slots reserved for
superusers), then superusers hit "too many clients already". Finally dead-end
raw connections are accepted up to the listen backlog.
"""

import struct
import sys

import pytest


def _bg_as(node, user):
    return node.background_psql(
        "postgres", on_error_stop=True, extra_params=["--username", user]
    )


def _connect_fails_wait(node, connstr, test_name, expected_stderr):
    log_location = node.current_log_position()
    node.connect_fails(connstr, test_name, expected_stderr=expected_stderr)
    node.wait_for_log(
        r"DEBUG:  (00000: )?client backend.*exited with exit code 1", log_location
    )


@pytest.mark.skipif(sys.platform == "win32", reason="requires raw_connect()")
def test_002_connection_limits(create_pg):
    """Reserved-connection tiers refuse the right roles as slots fill."""
    node = create_pg(
        "primary",
        auth_extra=[
            "--create-role",
            "regress_regular,regress_reserved,regress_superuser",
        ],
        start=False,
    )
    for line in (
        "max_connections = 6",
        "reserved_connections = 2",
        "superuser_reserved_connections = 1",
        "log_connections = 'receipt,authentication,authorization'",
        "log_min_messages=debug2",
    ):
        node.append_conf(line)
    node.start()
    node.safe_psql(
        "CREATE USER regress_regular LOGIN;\nCREATE USER regress_reserved LOGIN;\n"
        "GRANT pg_use_reserved_connections TO regress_reserved;\n"
        "CREATE USER regress_superuser LOGIN SUPERUSER;\n"
    )
    node.restart()
    sessions = []
    sessions.append(_bg_as(node, "regress_regular"))
    sessions.append(_bg_as(node, "regress_regular"))
    sessions.append(_bg_as(node, "regress_regular"))
    _connect_fails_wait(
        node,
        "dbname=postgres user=regress_regular",
        "regular connections limit",
        r"FATAL:  remaining connection slots are reserved for roles with "
        r'privileges of the "pg_use_reserved_connections" role',
    )
    sessions.append(_bg_as(node, "regress_reserved"))
    sessions.append(_bg_as(node, "regress_reserved"))
    _connect_fails_wait(
        node,
        "dbname=postgres user=regress_reserved",
        "reserved_connections limit",
        r"FATAL:  remaining connection slots are reserved for roles with the "
        r"SUPERUSER attribute",
    )
    sessions.append(_bg_as(node, "regress_superuser"))
    _connect_fails_wait(
        node,
        "dbname=postgres user=regress_superuser",
        "superuser_reserved_connections limit",
        r"FATAL:  sorry, too many clients already",
    )
    raw_connections = []
    if node.raw_connect_works():
        negotiate_ssl = struct.pack(">IHH", 8, 1234, 5679)
        for i in range(21):
            sock = node.raw_connect()
            sock.send(negotiate_ssl)
            assert sock.recv(1) == b"N", "dead-end connection {}".format(i)
            raw_connections.append(sock)
    for session in sessions:
        session.quit()
    for sock in raw_connections:
        sock.close()
