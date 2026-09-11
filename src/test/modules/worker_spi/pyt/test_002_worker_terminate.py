# Copyright (c) 2024-2026, PostgreSQL Global Development Group

"""Port of src/test/modules/worker_spi/t/002_worker_terminate.pl.

A non-interruptible worker_spi background worker connected to a database blocks
CREATE DATABASE ... WITH TEMPLATE of that database; an interruptible worker is
instead terminated by administrator commands that need exclusive access
(CREATE DATABASE WITH TEMPLATE, ALTER DATABASE RENAME/SET TABLESPACE, DROP
DATABASE), each logging the termination and the worker's exit. Requires an
injection-points build.
"""

import os
import re
import tempfile

import pytest


def _launch_bgworker(node, database, testcase, interruptible):
    pid = node.safe_psql(
        "SELECT worker_spi_launch({}, '{}'::regdatabase, 0, '{{}}', {});".format(
            testcase, database, interruptible
        )
    )
    assert node.poll_query_until(
        "SELECT wait_event FROM pg_stat_activity WHERE pid = {};".format(pid),
        "WorkerSpiMain",
    ), "dynamic bgworker {} launched".format(testcase)
    return pid


def _run_interruptible(node, command, test_name, pid):
    offset = node.current_log_position()
    node.safe_psql(command)
    node.wait_for_log(
        r'terminating background worker "worker_spi dynamic" due to '
        r"administrator command",
        offset,
    )
    node.wait_for_log(
        r'LOG: .*background worker "worker_spi dynamic" \(PID {}\) exited with '
        r"exit code".format(pid),
        offset,
    )
    assert (
        node.safe_psql(
            "SELECT count(*) = 0 FROM pg_stat_activity WHERE pid = {};".format(pid)
        )
        == "t"
    ), "dynamic bgworker stopped for {}".format(test_name)


def test_002_worker_terminate(create_pg):
    """worker_spi bgworkers block or are terminated by exclusive DB commands."""
    if os.environ.get("enable_injection_points") != "yes":
        pytest.skip("Injection points not supported by this build")
    node = create_pg("mynode", start=False)
    node.append_conf(
        "\nautovacuum = off\ndebug_parallel_query = off\nlog_min_messages = debug1\n"
        "worker_spi.naptime = 600\n"
    )
    node.start()
    if not node.check_extension("injection_points"):
        pytest.skip("Extension injection_points not installed")
    node.safe_psql("CREATE EXTENSION worker_spi;")
    _launch_bgworker(node, "postgres", 0, "false")
    node.safe_psql("CREATE EXTENSION injection_points;")
    node.safe_psql("SELECT injection_points_attach('procarray-reduce-count', 'error');")
    res = node.psql_capture("CREATE DATABASE testdb WITH TEMPLATE postgres")
    assert re.search(
        r'source database "postgres" is being accessed by other users', res.stderr
    ), "background worker blocked the database creation"
    assert (
        node.safe_psql(
            "SELECT count(1) FROM pg_stat_activity WHERE backend_type = "
            "'worker_spi dynamic';"
        )
        == "1"
    ), "background worker still running after CREATE DATABASE WITH TEMPLATE"
    node.safe_psql(
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE "
        "backend_type = 'worker_spi dynamic';"
    )
    node.safe_psql("SELECT injection_points_detach('procarray-reduce-count');")
    pid = _launch_bgworker(node, "postgres", 1, "true")
    _run_interruptible(
        node,
        "CREATE DATABASE testdb WITH TEMPLATE postgres",
        "CREATE DATABASE WITH TEMPLATE",
        pid,
    )
    pid = _launch_bgworker(node, "testdb", 2, "true")
    _run_interruptible(
        node, "ALTER DATABASE testdb RENAME TO renameddb", "ALTER DATABASE RENAME", pid
    )
    tablespace = tempfile.mkdtemp(prefix="ts_")
    node.safe_psql("CREATE TABLESPACE test_tablespace LOCATION '{}'".format(tablespace))
    pid = _launch_bgworker(node, "renameddb", 3, "true")
    _run_interruptible(
        node,
        "ALTER DATABASE renameddb SET TABLESPACE test_tablespace",
        "ALTER DATABASE SET TABLESPACE",
        pid,
    )
    pid = _launch_bgworker(node, "renameddb", 4, "true")
    _run_interruptible(node, "DROP DATABASE renameddb", "DROP DATABASE", pid)
