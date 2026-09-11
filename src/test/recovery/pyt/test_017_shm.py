# Copyright (c) 2017-2026, PostgreSQL Global Development Group

"""Port of src/test/recovery/t/017_shm.pl.

Exercises PostgreSQL's System V shared-memory startup interlock. A foreign shm
segment created with the same key the server derives (the data directory inode)
must not stop the server (it recycles its own key); after a kill9 with a live
backend still holding the shm, a fresh start (and single-user mode) must refuse
with "pre-existing shared memory block" until the orphaned backend is gone.
"""

import os
import sys
import time

import pytest

import pypg
from pypg.sysv_shm import SysVSharedMemory


def _poll_start(node):
    """Start node, retrying (cf. the Perl poll_start helper)."""
    for _ in range(10 * pypg.test_timeout_default()):
        if node.start(fail_ok=True):
            return
        node.stop("fast")
        time.sleep(0.1)
    node.start()


@pytest.mark.skipif(sys.platform == "win32", reason="SysV shm unsupported")
def test_017_shm(create_pg, pg_bin):
    """A live backend's shared memory blocks restart until that backend exits."""
    gnat = create_pg("gnat", start=False)
    gnat_inode = os.stat(gnat.datadir).st_ino
    conflict = SysVSharedMemory.create(gnat_inode)
    gnat.start()
    gnat.restart()  # keeps the same shmem key
    gnat.kill9()
    _poll_start(gnat)  # recycles its former shm key
    if conflict:
        conflict.remove()
    gnat.kill9()
    _poll_start(gnat)
    gnat.stop()
    conflict = SysVSharedMemory.create(gnat_inode)
    gnat.start()
    gnat.stop()
    if conflict:
        conflict.remove()
    gnat.start()
    _live_backend_blocks_restart(gnat, pg_bin)
    gnat.stop()


def _live_backend_blocks_restart(gnat, pg_bin):
    regress_shlib = os.environ["REGRESS_SHLIB"]
    gnat.safe_psql(
        "CREATE FUNCTION wait_pid(int)\n   RETURNS void\n   AS '{}'\n"
        "   LANGUAGE C STRICT;".format(regress_shlib)
    )
    slow_query = "SELECT wait_pid(pg_backend_pid())"
    slow = gnat.background_psql("postgres", on_error_stop=False)
    slow.send(slow_query + ";\n")
    # The background psql stores the statement with its trailing semicolon in
    # pg_stat_activity (the Perl client uses --command, which strips it).
    stored_query = slow_query + ";"
    assert gnat.poll_query_until(
        "SELECT 1 FROM pg_stat_activity WHERE query = '{}'".format(stored_query), "1"
    ), "slow query started"
    slow_pid = gnat.safe_psql(
        "SELECT pid FROM pg_stat_activity WHERE query = '{}'".format(stored_query)
    )
    gnat.kill9()
    (gnat.datadir / "postmaster.pid").unlink(missing_ok=True)
    gnat.rotate_logfile()
    pre_existing = r"pre-existing shared memory block"
    for _ in range(10 * pypg.test_timeout_default()):
        if gnat.start(fail_ok=True) or gnat.log_matches(pre_existing):
            break
        time.sleep(0.1)
    assert gnat.log_matches(pre_existing), "detected live backend via shared memory"
    pg_bin.command_fails_like(
        ["postgres", "--single", "-D", str(gnat.datadir), "template1"],
        pre_existing,
        "single-user mode detected live backend via shared memory",
    )
    gnat.pg_ctl("kill", "QUIT", slow_pid)
    slow.quit()
    _poll_start(gnat)
