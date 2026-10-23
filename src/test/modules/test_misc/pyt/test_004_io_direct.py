# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/test/modules/test_misc/t/004_io_direct.pl.

Exercises debug_io_direct: with tiny shared_buffers forcing real I/O, data is
written and read back through shared buffers and local (temp-table) buffers, and
survives an immediate-stop crash plus recovery. On non-macOS/Windows it first
checks that the filesystem under tmp_check actually supports opening a file with
O_DIRECT, skipping if not (matching the Perl pre-flight).
"""

import os
import sys

import pytest


def test_004_io_direct(create_pg, tmp_check):
    """debug_io_direct round-trips data through shared/local buffers and recovery."""
    if sys.platform not in ("darwin", "win32"):
        # Perl's Fcntl knows if this system has O_DIRECT in <fcntl.h>.
        if hasattr(os, "O_DIRECT"):
            # Can we open a file in O_DIRECT mode in the file system where
            # tmp_check lives?
            path = tmp_check / "test_o_direct_file"
            try:
                fd = os.open(str(path), os.O_RDWR | os.O_DIRECT | os.O_CREAT)
            except OSError as exc:
                pytest.skip(
                    "pre-flight test if we can open a file with O_DIRECT "
                    "failed: {}".format(exc)
                )
            os.close(fd)
        else:
            pytest.skip("no O_DIRECT")
    node = create_pg("main", start=False)
    node.append_conf(
        "\n"
        "debug_io_direct = 'data,wal,wal_init'\n"
        "shared_buffers = '256kB' # tiny to force I/O\n"
        "wal_level = replica # minimal runs out of shared_buffers when set so tiny\n"
    )
    node.start()
    node.safe_psql("create table t1 as select 1 as i from generate_series(1, 10000)")
    node.safe_psql("create table t2count (i int)")
    node.safe_psql(
        "\n"
        "begin;\n"
        "create temporary table t2 as select 1 as i from generate_series(1, 10000);\n"
        "update t2 set i = i;\n"
        "insert into t2count select count(*) from t2;\n"
        "commit;\n"
    )
    node.safe_psql("update t1 set i = i")
    assert node.safe_psql("select count(*) from t1") == "10000", "read back from shared"
    assert node.safe_psql("select * from t2count") == "10000", "read back from local"
    node.stop("immediate")
    node.start()
    assert (
        node.safe_psql("select count(*) from t1") == "10000"
    ), "read back from shared after crash recovery"
    node.stop()
