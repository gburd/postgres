# Copyright (c) 2024-2026, PostgreSQL Global Development Group

"""Port of contrib/postgres_fdw/t/001_auth_scram.pl.

postgres_fdw SCRAM credential passthrough: with use_scram_passthrough on the
foreign server, a SCRAM-authenticated user's credentials are forwarded so the
FDW connection authenticates without a stored password -- on the same cluster
and across clusters. Disabling passthrough on the user mapping makes the FDW
query fail with a password error, and loopback trust/password HBA entries are
rejected because the connection requires scram-sha-256.
"""

import os
import re
import sys

import pytest

USER = "user01"
HOSTADDR = "127.0.0.1"


def _setup_table(node, db, tbl):
    node.safe_psql(
        "CREATE TABLE {} AS SELECT g, g + 1 FROM generate_series(1,10) g(g)".format(
            tbl
        ),
        dbname=db,
    )
    node.safe_psql("GRANT USAGE ON SCHEMA public TO {}".format(USER), dbname=db)
    node.safe_psql("GRANT SELECT ON {} TO {}".format(tbl, USER), dbname=db)


def _setup_fdw_server(node, db, fdw, fdw_node, dbname):
    node.safe_psql(
        "CREATE SERVER {} FOREIGN DATA WRAPPER postgres_fdw options (host '{}', "
        "port '{}', dbname '{}', use_scram_passthrough 'true')".format(
            fdw, fdw_node.host, fdw_node.port, dbname
        ),
        dbname=db,
    )


def _setup_user_mapping(node, db, fdw):
    node.safe_psql(
        "CREATE USER MAPPING FOR {u} SERVER {f} OPTIONS (user '{u}');".format(
            u=USER, f=fdw
        ),
        dbname=db,
    )
    node.safe_psql(
        "GRANT USAGE ON FOREIGN SERVER {} TO {}".format(fdw, USER), dbname=db
    )
    node.safe_psql("GRANT ALL ON SCHEMA public TO {}".format(USER), dbname=db)


def _setup_pghba(node):
    (node.datadir / "pg_hba.conf").unlink(missing_ok=True)
    node.append_conf(
        "local   all             all                                     "
        "scram-sha-256\n"
        "host    all             all             {}/32            "
        "scram-sha-256\n".format(HOSTADDR),
        filename="pg_hba.conf",
    )
    node.restart()


def _test_auth(node, db, tbl, test_name):
    connstr = node.connstr(db) + " user={}".format(USER)
    assert (
        node.safe_psql(
            "SELECT count(1) FROM {}".format(tbl), dbname=db, connstr=connstr
        )
        == "10"
    ), test_name


def _test_fdw_auth(node, db, tbl, fdw, test_name):
    connstr = node.connstr(db) + " user={}".format(USER)
    node.safe_psql(
        "IMPORT FOREIGN SCHEMA public LIMIT TO ({}) FROM SERVER {} INTO "
        "public;".format(tbl, fdw),
        dbname=db,
        connstr=connstr,
    )
    _test_auth(node, db, tbl, test_name)


@pytest.mark.skipif(sys.platform == "win32", reason="needs Unix-domain sockets")
def test_001_auth_scram(create_pg):
    """postgres_fdw forwards SCRAM credentials; passthrough-off and trust fail."""
    db0, db1, db2 = "db0", "db1", "db2"
    fdw1, fdw2, fdw3 = "db1_fdw", "db2_fdw", "db1_fdw_override"
    node1 = create_pg("node1")
    node2 = create_pg("node2")
    node1.safe_psql("CREATE USER {} WITH password 'pass'".format(USER))
    node2.safe_psql("CREATE USER {} WITH password 'pass'".format(USER))
    os.environ["PGPASSWORD"] = "pass"
    node1.safe_psql("CREATE DATABASE {}".format(db0))
    node1.safe_psql("CREATE DATABASE {}".format(db1))
    node2.safe_psql("CREATE DATABASE {}".format(db2))
    _setup_table(node1, db1, "t")
    _setup_table(node2, db2, "t2")
    node1.safe_psql("CREATE EXTENSION IF NOT EXISTS postgres_fdw", dbname=db0)
    _setup_fdw_server(node1, db0, fdw1, node1, db1)
    _setup_fdw_server(node1, db0, fdw2, node2, db2)
    _setup_fdw_server(node1, db0, fdw3, node1, db1)
    for fdw in (fdw1, fdw2, fdw3):
        _setup_user_mapping(node1, db0, fdw)
    rolpassword = node1.safe_psql(
        "SELECT rolpassword FROM pg_authid WHERE rolname = '{}';".format(USER)
    )
    node2.safe_psql("ALTER ROLE {} PASSWORD '{}'".format(USER, rolpassword))
    _setup_pghba(node1)
    _setup_pghba(node2)
    _test_fdw_auth(
        node1, db0, "t", fdw1, "SCRAM auth on the same database cluster must succeed"
    )
    _test_fdw_auth(
        node1,
        db0,
        "t2",
        fdw2,
        "SCRAM auth on a different database cluster must succeed",
    )
    _test_auth(
        node2, db2, "t2", "SCRAM auth directly on foreign server should still succeed"
    )
    _passthrough_off(node1, db0, fdw3)
    _loopback_rejections(node1, node2, db0)


def _passthrough_off(node1, db0, fdw3):
    connstr = node1.connstr(db0) + " user={}".format(USER)
    node1.safe_psql(
        "ALTER USER MAPPING FOR {} SERVER {} OPTIONS(add use_scram_passthrough "
        "'false')".format(USER, fdw3),
        dbname=db0,
        connstr=connstr,
    )
    node1.safe_psql(
        "CREATE FOREIGN TABLE override_t (g int, col2 int) SERVER {} OPTIONS "
        "(table_name 't');".format(fdw3),
        dbname=db0,
        connstr=connstr,
    )
    node1.safe_psql(
        "GRANT SELECT ON override_t TO {};".format(USER), dbname=db0, connstr=connstr
    )
    res = node1.psql_capture(
        "SELECT count(1) FROM override_t", dbname=db0, connstr=connstr
    )
    assert res.rc == 3, "SCRAM passthrough disabled on user mapping should fail"
    assert re.search(
        r"password", res.stderr, re.I
    ), "expected password-related error when scram passthrough disabled"


def _loopback_rejections(node1, node2, db0):
    (node1.datadir / "pg_hba.conf").unlink(missing_ok=True)
    (node2.datadir / "pg_hba.conf").unlink(missing_ok=True)
    node1.append_conf(
        "local   db0             all                                     "
        "scram-sha-256\n"
        "local   db1             all                                     trust\n",
        filename="pg_hba.conf",
    )
    node2.append_conf(
        "local   all             all                                     password\n",
        filename="pg_hba.conf",
    )
    node1.restart()
    node2.restart()
    connstr = node1.connstr(db0) + " user={}".format(USER)
    res = node1.psql_capture("select count(1) from t", dbname=db0, connstr=connstr)
    assert res.rc == 3, "loopback trust fails on the same cluster"
    assert re.search(
        r'failed: authentication method requirement "scram-sha-256"', res.stderr
    ), "expected error from loopback trust (same cluster)"
    res = node1.psql_capture("select count(1) from t2", dbname=db0, connstr=connstr)
    assert res.rc == 3, "loopback password fails on a different cluster"
    assert re.search(
        r'failed: authentication method requirement "scram-sha-256"', res.stderr
    ), "expected error from loopback password (different cluster)"
