# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/test/modules/test_misc/t/002_tablespace.pl.

CREATE/DROP TABLESPACE and moving tables between tablespaces, covering
absolute-path tablespaces and in-place tablespaces (allow_in_place_tablespaces),
including the expected failures (clobbering an existing tablespace, dropping a
non-empty tablespace).
"""

import os


def _ok(node, sql, msg):
    assert node.psql_capture(sql).rc == 0, msg


def _fail(node, sql, msg):
    assert node.psql_capture(sql).rc != 0, msg


def test_002_tablespace(create_pg):
    """Tablespace create/drop and table moves across abs and in-place dirs."""
    node = create_pg("main")
    ts1 = os.path.join(node.basedir, "ts1")
    ts2 = os.path.join(node.basedir, "ts2")
    os.mkdir(ts1)
    os.mkdir(ts2)
    _ok(
        node,
        "CREATE TABLESPACE regress_ts1 LOCATION '{}'".format(ts1),
        "create tablespace with absolute path",
    )
    _fail(
        node,
        "CREATE TABLESPACE regress_ts1 LOCATION '{}'".format(ts1),
        "clobber tablespace with absolute path",
    )
    _ok(
        node,
        "CREATE TABLE t () TABLESPACE regress_ts1",
        "create table in tablespace with absolute path",
    )
    _fail(node, "DROP TABLESPACE regress_ts1", "drop non-empty tablespace fails")
    _ok(node, "DROP TABLE t", "drop table in tablespace with absolute path")
    _ok(node, "DROP TABLESPACE regress_ts1", "drop tablespace with absolute path")
    _ok(
        node,
        "CREATE TABLESPACE regress_ts1 LOCATION '{}'".format(ts1),
        "create tablespace 1 with absolute path",
    )
    _ok(
        node,
        "CREATE TABLESPACE regress_ts2 LOCATION '{}'".format(ts2),
        "create tablespace 2 with absolute path",
    )
    _ok(
        node,
        "SET allow_in_place_tablespaces=on; CREATE TABLESPACE regress_ts3 "
        "LOCATION ''",
        "create tablespace 3 with in-place directory",
    )
    _ok(
        node,
        "SET allow_in_place_tablespaces=on; CREATE TABLESPACE regress_ts4 "
        "LOCATION ''",
        "create tablespace 4 with in-place directory",
    )
    _ok(node, "CREATE TABLE t () TABLESPACE regress_ts1", "create table in ts1")
    _ok(node, "ALTER TABLE t SET tablespace regress_ts2", "move table abs->abs")
    _ok(node, "ALTER TABLE t SET tablespace regress_ts3", "move table abs->in-place")
    _ok(
        node,
        "ALTER TABLE t SET tablespace regress_ts4",
        "move table in-place->in-place",
    )
    _ok(node, "ALTER TABLE t SET tablespace regress_ts1", "move table in-place->abs")
    _ok(node, "DROP TABLE t", "drop table in ts1")
    for i in (1, 2, 3, 4):
        _ok(
            node,
            "DROP TABLESPACE regress_ts{}".format(i),
            "drop tablespace {}".format(i),
        )
    node.stop()
