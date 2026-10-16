# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/test/authentication/t/002_saslprep.pl.

SCRAM authentication applies SASLprep normalization to passwords: equivalent
Unicode forms of a stored password authenticate successfully, while
non-equivalent ones fail. Roles are created with various passwords and logged in
with byte sequences that should normalize to the stored value (success) or not
(failure).
"""

import os
import sys

import pytest


def _reset_pg_hba(node, method):
    (node.datadir / "pg_hba.conf").unlink(missing_ok=True)
    node.append_conf("local all all {}".format(method), filename="pg_hba.conf")
    node.reload()


def _test_login(node, role, password, expected_ok):
    connstr = "user={}".format(role)
    status = "success" if expected_ok else "failed"
    name = "authentication {} for role {} with password {!r}".format(
        status, role, password
    )
    os.environ["PGPASSWORD"] = password
    if expected_ok:
        node.connect_ok(connstr, name)
    else:
        node.connect_fails(connstr, name)


@pytest.mark.skipif(sys.platform == "win32", reason="needs Unix-domain sockets")
def test_002_saslprep(create_pg):
    """SASLprep-equivalent passwords authenticate; non-equivalent ones fail."""
    node = create_pg("primary", extra=["--locale=C", "--encoding=UTF8"], start=False)
    node.start()
    node.safe_psql(
        "SET password_encryption='scram-sha-256';\nSET client_encoding='utf8';\n"
        "CREATE ROLE saslpreptest1_role LOGIN PASSWORD 'IX';\n"
        "CREATE ROLE saslpreptest4a_role LOGIN PASSWORD 'a';\n"
        "CREATE ROLE saslpreptest4b_role LOGIN PASSWORD E'\\xc2\\xaa';\n"
        "CREATE ROLE saslpreptest6_role LOGIN PASSWORD E'foo\\x07bar';\n"
        "CREATE ROLE saslpreptest7_role LOGIN PASSWORD E'foo\\u0627\\u0031bar';"
    )
    _reset_pg_hba(node, "scram-sha-256")
    # Passwords are raw byte strings decoded as latin-1 so each \xNN is one byte
    # on the wire (libpq reads PGPASSWORD as bytes), matching the Perl literals.
    cases = [
        ("saslpreptest1_role", b"I\xc2\xadX", True),
        ("saslpreptest1_role", b"\xe2\x85\xa8", True),
        ("saslpreptest1_role", b"ix", False),
        ("saslpreptest4a_role", b"a", True),
        ("saslpreptest4a_role", b"\xc2\xaa", True),
        ("saslpreptest4b_role", b"a", True),
        ("saslpreptest4b_role", b"\xc2\xaa", True),
        ("saslpreptest6_role", b"foo\x07bar", True),
        ("saslpreptest6_role", b"foobar", False),
        ("saslpreptest7_role", b"foo\xd8\xa71bar", True),
        ("saslpreptest7_role", b"foo1\xd8\xa7bar", False),
        ("saslpreptest7_role", b"foobar", False),
    ]
    for role, password, expected_ok in cases:
        # Decode with surrogateescape so os.fsencode round-trips the exact bytes
        # into the subprocess environment (latin-1 would be re-encoded as UTF-8).
        _test_login(
            node, role, password.decode("utf-8", "surrogateescape"), expected_ok
        )
