# Copyright (c) 2023-2026, PostgreSQL Global Development Group

"""Port of src/test/authentication/t/006_login_trigger.pl.

A login event trigger fires on every connection: it records the SESSION_USER in
a table and raises a NOTICE ("You are welcome") for allowed users, or an
EXCEPTION for a disallowed one. The test exercises creating/enabling the trigger,
connecting as another role, verifying the recorded logins, and cleanup, checking
each command's exit code, stdout, and stderr.
"""

import re
import sys

import pytest


def _psql_command(
    node,
    sql,
    expected_ret,
    test_name,
    *,
    connstr=None,
    log_like=None,
    log_unlike=None,
    log_exact=None,
    err_like=None,
    err_unlike=None,
    err_exact=None,
):
    res = node.psql_capture(sql, connstr=connstr, on_error_stop=False)
    assert res.rc == expected_ret, "{}: exit code {}".format(test_name, expected_ret)
    out, err = res.stdout, res.stderr
    for rx in log_like or []:
        assert re.search(rx, out), "{}: log matches".format(test_name)
    for rx in log_unlike or []:
        assert not re.search(rx, out), "{}: log unmatches".format(test_name)
    if log_exact is not None:
        assert out == log_exact, "{}: log equals".format(test_name)
    for rx in err_like or []:
        assert re.search(rx, err), "{}: err matches".format(test_name)
    for rx in err_unlike or []:
        assert not re.search(rx, err), "{}: err unmatches".format(test_name)
    if err_exact is not None:
        assert err == err_exact, "{}: err equals".format(test_name)


@pytest.mark.skipif(sys.platform == "win32", reason="needs Unix-domain sockets")
def test_006_login_trigger(create_pg):
    """A login event trigger records logins and gates them by SESSION_USER."""
    node = create_pg("main", extra=["--locale=C", "--encoding=UTF8"], start=False)
    node.append_conf(
        "\nwal_level = 'logical'\nmax_replication_slots = 4\nmax_wal_senders = 4\n"
    )
    node.start()
    _psql_command(
        node,
        "CREATE ROLE regress_alice WITH LOGIN;\n"
        "CREATE ROLE regress_mallory WITH LOGIN;\n"
        "CREATE TABLE user_logins(id serial, who text);\n"
        "GRANT SELECT ON user_logins TO public;\n",
        0,
        "create tmp objects",
        log_exact="",
        err_exact="",
    )
    _psql_command(
        node,
        "CREATE FUNCTION on_login_proc() RETURNS event_trigger AS $$\nBEGIN\n"
        "  INSERT INTO user_logins (who) VALUES (SESSION_USER);\n"
        "  IF SESSION_USER = 'regress_mallory' THEN\n"
        "    RAISE EXCEPTION 'Hello %! You are NOT welcome here!', SESSION_USER;\n"
        "  END IF;\n"
        "  RAISE NOTICE 'Hello %! You are welcome!', SESSION_USER;\nEND;\n"
        "$$ LANGUAGE plpgsql SECURITY DEFINER;\n",
        0,
        "create trigger function",
        log_exact="",
        err_exact="",
    )
    _psql_command(
        node,
        "CREATE EVENT TRIGGER on_login_trigger ON login EXECUTE PROCEDURE "
        "on_login_proc();",
        0,
        "create event trigger",
        log_exact="",
        err_exact="",
    )
    _psql_command(
        node,
        "ALTER EVENT TRIGGER on_login_trigger ENABLE ALWAYS;",
        0,
        "alter event trigger",
        log_exact="",
        err_like=[r"You are welcome"],
    )
    _psql_command(
        node,
        "SELECT COUNT(*) FROM user_logins;",
        0,
        "select count",
        log_exact="2",
        err_like=[r"You are welcome"],
    )
    _psql_command(
        node,
        "SELECT 1;",
        0,
        "try regress_alice",
        connstr="user=regress_alice",
        log_exact="1",
        err_like=[r"You are welcome"],
        err_unlike=[r"You are NOT welcome"],
    )
    _psql_command(
        node,
        "SELECT * FROM user_logins;",
        0,
        "select *",
        log_like=[r"3\|regress_alice"],
        log_unlike=[r"regress_mallory"],
        err_like=[r"You are welcome"],
    )
    _psql_command(
        node,
        "SELECT COUNT(*) FROM user_logins;",
        0,
        "select count",
        log_exact="5",
        err_like=[r"You are welcome"],
    )
    _psql_command(
        node,
        "DROP EVENT TRIGGER on_login_trigger;",
        0,
        "drop event trigger",
        log_exact="",
        err_like=[r"You are welcome"],
    )
    _psql_command(
        node,
        "DROP TABLE user_logins;\nDROP FUNCTION on_login_proc;\n"
        "DROP ROLE regress_mallory;\nDROP ROLE regress_alice;\n",
        0,
        "cleanup",
        log_exact="",
        err_exact="",
    )
