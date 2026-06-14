# Copyright (c) 2024-2026, PostgreSQL Global Development Group

# pylint: disable=line-too-long
"""Port of contrib/dblink/t/001_auth_scram.pl.

dblink SCRAM passthrough: a dblink_fdw server with use_scram_passthrough reuses
the client's SCRAM keys to authenticate to the remote (same or different
cluster) without a stored password; disabling passthrough, overriding
require_auth, or supplying scram_client_key/scram_server_key options all fail
as expected, and loopback trust/password configurations are rejected.
"""

import os
import re

_USER = "user01"
_DB0 = "db0"
_DB1 = "db1"
_DB2 = "db2"


def _u_connstr(node, db):
    return node.connstr(db) + " user=" + _USER


def _setup_table(node, db, tbl):
    node.safe_psql(
        "CREATE TABLE {} AS SELECT g as a, g + 1 as b "
        "FROM generate_series(1,10) g(g)".format(tbl),
        dbname=db,
    )
    node.safe_psql("GRANT USAGE ON SCHEMA public TO {}".format(_USER), dbname=db)
    node.safe_psql("GRANT SELECT ON {} TO {}".format(tbl, _USER), dbname=db)


def _setup_fdw_server(node, db, fdw, fdw_node, dbname, require_auth=None):
    extra = ", require_auth 'none'" if require_auth == "none" else ""
    node.safe_psql(
        "CREATE SERVER {} FOREIGN DATA WRAPPER dblink_fdw options ("
        "host '{}', port '{}', dbname '{}', use_scram_passthrough 'true'{})".format(
            fdw, fdw_node.host, fdw_node.port, dbname, extra
        ),
        dbname=db,
    )
    node.safe_psql(
        "GRANT USAGE ON FOREIGN SERVER {} TO {};".format(fdw, _USER), dbname=db
    )
    node.safe_psql("GRANT ALL ON SCHEMA public TO {}".format(_USER), dbname=db)


def _setup_user_mapping(node, db, fdw):
    node.safe_psql(
        "CREATE USER MAPPING FOR {user} SERVER {fdw} OPTIONS "
        "(user '{user}');".format(user=_USER, fdw=fdw),
        dbname=db,
    )


def _test_fdw_auth(node, db, tbl, fdw, testname):
    res = node.psql_capture(
        "SELECT count(1) FROM dblink('{fdw}', 'SELECT * FROM {tbl}') AS "
        "{tbl}(a int, b int)".format(fdw=fdw, tbl=tbl),
        connstr=_u_connstr(node, db),
    )
    assert res.stdout == "10", testname


def test_001_auth_scram(create_pg):  # pylint: disable=too-many-statements
    """dblink SCRAM passthrough succeeds and misconfigurations fail."""
    node1 = create_pg("node1")
    node2 = create_pg("node2")
    fdw_server, fdw_server2 = "db1_fdw", "db2_fdw"
    fdw_server3 = "db1_fdw_override"
    fdw_invalid_server, fdw_invalid_server2 = "db2_fdw_invalid", "db2_fdw_invalid2"
    node1.safe_psql("CREATE USER {} WITH password 'pass'".format(_USER))
    node2.safe_psql("CREATE USER {} WITH password 'pass'".format(_USER))
    os.environ["PGPASSWORD"] = "pass"
    try:
        node1.safe_psql("CREATE DATABASE {}".format(_DB0))
        node1.safe_psql("CREATE DATABASE {}".format(_DB1))
        node2.safe_psql("CREATE DATABASE {}".format(_DB2))
        _setup_table(node1, _DB1, "t")
        _setup_table(node2, _DB2, "t2")
        node1.safe_psql("CREATE EXTENSION IF NOT EXISTS dblink", dbname=_DB0)
        _setup_fdw_server(node1, _DB0, fdw_server, node1, _DB1)
        _setup_fdw_server(node1, _DB0, fdw_server2, node2, _DB2)
        _setup_fdw_server(
            node1, _DB0, fdw_invalid_server, node2, _DB2, require_auth="none"
        )
        _setup_fdw_server(node1, _DB0, fdw_invalid_server2, node2, _DB2)
        _setup_fdw_server(node1, _DB0, fdw_server3, node1, _DB1)
        for fdw in (fdw_server, fdw_server2, fdw_invalid_server, fdw_server3):
            _setup_user_mapping(node1, _DB0, fdw)
        rolpassword = node1.safe_psql(
            "SELECT rolpassword FROM pg_authid WHERE rolname = '{}';".format(_USER)
        )
        node2.safe_psql("ALTER ROLE {} PASSWORD '{}'".format(_USER, rolpassword))
        os.unlink("{}/pg_hba.conf".format(node1.datadir))
        os.unlink("{}/pg_hba.conf".format(node2.datadir))
        node1.append_conf(
            "\nlocal   db0             all                                     scram-sha-256\n"
            "local   db1             all                                     scram-sha-256\n",
            "pg_hba.conf",
        )
        node2.append_conf(
            "\nlocal   db2             all                                     scram-sha-256\n",
            "pg_hba.conf",
        )
        node1.restart()
        node2.restart()
        _test_scram_keys_not_overwritten(node1, _DB0, fdw_invalid_server2)
        _test_fdw_auth(
            node1,
            _DB0,
            "t",
            fdw_server,
            "SCRAM auth on the same database cluster must succeed",
        )
        _test_fdw_auth(
            node1,
            _DB0,
            "t2",
            fdw_server2,
            "SCRAM auth on a different database cluster must succeed",
        )
        _test_invalid_overwritten_require_auth(node1, fdw_invalid_server)
        _test_disabled_passthrough(node1, fdw_server3)
        _test_loopback_rejections(node1, node2, fdw_server, fdw_server2)
    finally:
        os.environ.pop("PGPASSWORD", None)


def _test_scram_keys_not_overwritten(node, db, fdw):
    for opt in ("scram_client_key", "scram_server_key"):
        res = node.psql_capture(
            "CREATE USER MAPPING FOR {user} SERVER {fdw} OPTIONS "
            "(user '{user}', {opt} 'key');".format(user=_USER, fdw=fdw, opt=opt),
            connstr=_u_connstr(node, db),
        )
        assert res.exit_code == 3, "user mapping creation fails when using {}".format(
            opt
        )
        assert re.search(r'ERROR:  invalid option "{}"'.format(opt), res.stderr)


def _test_invalid_overwritten_require_auth(node1, fdw):
    res = node1.psql_capture(
        "select * from dblink('{}', 'select * from t') as t(a int, b int)".format(fdw),
        connstr=_u_connstr(node1, _DB0),
    )
    assert res.exit_code == 3, "loopback trust fails when overwriting require_auth"
    assert re.search(
        r"password or GSSAPI delegated credentials required", res.stderr
    ), "expected error when connecting to a fdw overwriting the require_auth"


def _test_disabled_passthrough(node1, fdw):
    connstr = _u_connstr(node1, _DB0)
    node1.psql_capture(
        "ALTER USER MAPPING FOR {} SERVER {} OPTIONS(add use_scram_passthrough "
        "'false')".format(_USER, fdw),
        connstr=connstr,
    )
    res = node1.psql_capture(
        "select * from dblink('{}', 'select * from t') as t(a int, b int)".format(fdw),
        connstr=connstr,
    )
    assert res.exit_code == 3, "SCRAM passthrough disabled on user mapping should fail"
    assert re.search(
        r"password", res.stderr, re.IGNORECASE
    ), "expected password-related error when scram passthrough disabled"


def _test_loopback_rejections(node1, node2, fdw_server, fdw_server2):
    os.unlink("{}/pg_hba.conf".format(node1.datadir))
    os.unlink("{}/pg_hba.conf".format(node2.datadir))
    node1.append_conf(
        "\nlocal   db0             all                                     scram-sha-256\n"
        "local   db1             all                                     trust\n",
        "pg_hba.conf",
    )
    node2.append_conf(
        "\nlocal   all             all                                     password\n",
        "pg_hba.conf",
    )
    node1.restart()
    node2.restart()
    res = node1.psql_capture(
        "SELECT * FROM dblink('{}', 'SELECT * FROM t') AS t(a int, b int)".format(
            fdw_server
        ),
        connstr=_u_connstr(node1, _DB0),
    )
    assert res.exit_code == 3, "loopback trust fails on the same cluster"
    assert re.search(
        r'failed: authentication method requirement "scram-sha-256" failed: '
        r"server did not complete authentication",
        res.stderr,
    ), "expected error from loopback trust (same cluster)"
    res = node1.psql_capture(
        "SELECT * FROM dblink('{}', 'SELECT * FROM t2') AS t2(a int, b int)".format(
            fdw_server2
        ),
        connstr=_u_connstr(node1, _DB0),
    )
    assert res.exit_code == 3, "loopback password fails on a different cluster"
    assert re.search(
        r'authentication method requirement "scram-sha-256" failed: '
        r"server requested a cleartext password",
        res.stderr,
    ), "expected error from loopback password (different cluster)"
