# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/test/recovery/t/029_stats_restart.pl.

Cumulative statistics survive a clean restart (the stats file is written at
shutdown and reloaded) but are discarded after an immediate (crash) shutdown or
when the stats file is corrupted/truncated. Also checks that pg_stat_reset_shared
for checkpointer and wal resets the right counters and bumps stats_reset, and
that those resets persist across restarts.
"""

import os
import shutil
import tempfile

CONNECT_DB = "postgres"
TEST_DB = "test"


def _have_stats(node, kind, dboid, objid):
    return node.safe_psql(
        "SELECT pg_stat_have_stats('{}', {}, {})".format(kind, dboid, objid)
    )


def _trigger_funcrel_stat(node):
    node.safe_psql(
        "SELECT * FROM tab_stats_crash_discard_test1;\n"
        "SELECT func_stats_crash_discard1();\nSELECT pg_stat_force_next_flush();",
        dbname=TEST_DB,
    )


def _checkpoint_stats(node):
    return {
        "count": int(
            node.safe_psql("SELECT num_timed + num_requested FROM pg_stat_checkpointer")
        ),
        "reset": node.safe_psql("SELECT stats_reset FROM pg_stat_checkpointer"),
    }


def _wal_stats(node):
    return {
        "records": int(node.safe_psql("SELECT wal_records FROM pg_stat_wal")),
        "bytes": int(node.safe_psql("SELECT wal_bytes FROM pg_stat_wal")),
        "reset": node.safe_psql("SELECT stats_reset FROM pg_stat_wal"),
    }


def _io_stats(node, context, obj, backend_type):
    where = "context = '{}' AND object = '{}' AND backend_type = '{}'".format(
        context, obj, backend_type
    )
    writes = node.safe_psql("SELECT writes FROM pg_stat_io WHERE " + where)
    reads = node.safe_psql("SELECT reads FROM pg_stat_io WHERE " + where)
    return {
        "writes": int(writes) if writes else 0,
        "reads": int(reads) if reads else 0,
    }


def test_029_stats_restart(create_pg):
    """Stats persist across clean restart, vanish on crash/corruption, reset OK."""
    node = create_pg("primary", allows_streaming=True, start=False)
    node.append_conf("track_functions = 'all'")
    node.start()
    standalone = _io_stats(node, "init", "wal", "standalone backend")
    startup = _io_stats(node, "normal", "wal", "startup")
    assert standalone["writes"] > 0, "increased standalone backend IO writes"
    assert startup["reads"] > 0, "increased startup IO reads"
    node.safe_psql("CREATE DATABASE {}".format(TEST_DB))
    node.safe_psql(
        "CREATE TABLE tab_stats_crash_discard_test1 AS "
        "SELECT generate_series(1,100) AS a",
        dbname=TEST_DB,
    )
    node.safe_psql(
        "CREATE FUNCTION func_stats_crash_discard1() RETURNS VOID AS "
        "'select 2;' LANGUAGE SQL IMMUTABLE",
        dbname=TEST_DB,
    )
    dboid = node.safe_psql(
        "SELECT oid FROM pg_database WHERE datname = '{}'".format(TEST_DB),
        dbname=TEST_DB,
    )
    funcoid = node.safe_psql(
        "SELECT 'func_stats_crash_discard1()'::regprocedure::oid", dbname=TEST_DB
    )
    tableoid = node.safe_psql(
        "SELECT 'tab_stats_crash_discard_test1'::regclass::oid", dbname=TEST_DB
    )
    _trigger_funcrel_stat(node)
    for kind, objid in (
        ("database", "0"),
        ("function", funcoid),
        ("relation", tableoid),
    ):
        assert (
            _have_stats(node, kind, dboid, objid) == "t"
        ), "initial: {} stats exist".format(kind)
    _stats_file_cycles(node, dboid, funcoid, tableoid)
    _reset_cycles(node)


def _expect_stats(node, dboid, funcoid, tableoid, present, sect):
    for kind, objid in (
        ("database", "0"),
        ("function", funcoid),
        ("relation", tableoid),
    ):
        assert (
            _have_stats(node, kind, dboid, objid) == present
        ), "{}: {} stats {}".format(sect, kind, "exist" if present == "t" else "absent")


def _stats_file_cycles(node, dboid, funcoid, tableoid):
    node.stop()
    statsfile = tempfile.mktemp(prefix="discard_stats1_")
    og_stats = node.datadir / "pg_stat" / "pgstat.stat"
    assert og_stats.is_file(), "origin stats file must exist"
    shutil.copy(og_stats, statsfile)
    node.start()
    _expect_stats(node, dboid, funcoid, tableoid, "t", "copy")
    node.stop("immediate")
    assert not og_stats.exists(), "no stats file after immediate shutdown"
    shutil.copy(statsfile, og_stats)
    node.start()
    _expect_stats(node, dboid, funcoid, tableoid, "f", "post immediate")
    os.unlink(statsfile)
    _trigger_funcrel_stat(node)
    _expect_stats(node, dboid, funcoid, tableoid, "t", "post immediate, new")
    node.stop()
    with open(og_stats, "w", encoding="utf-8") as fh:
        fh.write("ZZZZZZZZZZZZZ")
    node.start()
    _expect_stats(node, dboid, funcoid, tableoid, "f", "invalid_overwrite")
    _trigger_funcrel_stat(node)
    node.stop()
    with open(og_stats, "a", encoding="utf-8") as fh:
        fh.write("XYZ")
    node.start()
    _expect_stats(node, dboid, funcoid, tableoid, "f", "invalid_append")


def _reset_cycles(node):
    node.safe_psql("CHECKPOINT; CHECKPOINT;")
    ckpt0 = _checkpoint_stats(node)
    wal0 = _wal_stats(node)
    node.restart()
    ckpt1 = _checkpoint_stats(node)
    wal1 = _wal_stats(node)
    assert ckpt0["count"] < ckpt1["count"], "post restart: increased checkpoint count"
    assert wal0["records"] < wal1["records"], "post restart: increased wal records"
    assert wal0["bytes"] < wal1["bytes"], "post restart: increased wal bytes"
    assert (
        ckpt0["reset"] == ckpt1["reset"]
    ), "post restart: checkpoint stats_reset equal"
    assert wal0["reset"] == wal1["reset"], "post restart: wal stats_reset equal"
    node.safe_psql("SELECT pg_stat_reset_shared('checkpointer')")
    ckpt2 = _checkpoint_stats(node)
    wal2 = _wal_stats(node)
    assert ckpt1["count"] > ckpt2["count"], "post ckpt reset: checkpoint count smaller"
    assert ckpt0["reset"] < ckpt2["reset"], "post ckpt reset: stats_reset newer"
    assert wal1["records"] <= wal2["records"], "post ckpt reset: wal records unaffected"
    assert wal0["reset"] == wal2["reset"], "post ckpt reset: wal stats_reset equal"
    node.restart()
    ckpt3 = _checkpoint_stats(node)
    wal3 = _wal_stats(node)
    assert ckpt3["count"] < ckpt1["count"], "post ckpt reset & restart: still reset"
    assert (
        ckpt3["reset"] == ckpt2["reset"]
    ), "post ckpt reset & restart: stats_reset same"
    assert (
        wal2["records"] < wal3["records"]
    ), "post ckpt reset & restart: increased wal records"
    assert (
        wal2["bytes"] < wal3["bytes"]
    ), "post ckpt reset & restart: increased wal bytes"
    assert (
        wal0["reset"] == wal3["reset"]
    ), "post ckpt reset & restart: wal stats_reset equal"
    node.safe_psql("SELECT pg_stat_reset_shared('wal')")
    wal4 = _wal_stats(node)
    assert wal4["records"] < wal3["records"], "post wal reset: smaller record count"
    assert wal4["bytes"] < wal3["bytes"], "post wal reset: smaller bytes"
    assert wal4["reset"] > wal3["reset"], "post wal reset: newer stats_reset"
    node.restart()
    wal5 = _wal_stats(node)
    assert (
        wal5["records"] < wal3["records"]
    ), "post wal reset & restart: smaller record count"
    assert wal4["bytes"] < wal3["bytes"], "post wal reset & restart: smaller bytes"
    assert wal4["reset"] > wal3["reset"], "post wal reset & restart: newer stats_reset"
    node.stop("immediate")
    node.start()
    wal6 = _wal_stats(node)
    assert (
        wal5["reset"] < wal6["reset"]
    ), "post immediate restart: reset timestamp is new"
    node.stop()
