# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/test/recovery/t/013_crash_restart.pl.

Restarts of postgres due to crashes of a subprocess. Two background psql
sessions are used: one whose backend is killed (triggering crash-restart) and
one long-running monitor that detects when the crash happened.
"""

# stderr patterns indicating the killed backend's connection went away.
_KILLME_SIGQUIT = (
    r"WARNING:  terminating connection because of unexpected SIGQUIT signal"
    r"|server closed the connection unexpectedly"
    r"|connection to server was lost|could not send data to server"
)
_KILLME_SIGKILL = (
    r"server closed the connection unexpectedly"
    r"|connection to server was lost|could not send data to server"
)
_MONITOR_DIED = (
    r"WARNING:  terminating connection because of crash of another server process"
    r"|server closed the connection unexpectedly"
    r"|connection to server was lost|could not send data to server"
)
_DIGIT_LINE = r"[0-9]+[\r\n]"


def _monitor_connect(monitor):
    monitor.query_until(
        r"psql-connected", "SELECT $$psql-connected$$;\nSELECT pg_sleep(3600);\n"
    )


def _reconnect(node):
    # poll until a fresh psql with empty input produces empty output: the
    # server is accepting connections again.
    assert node.poll_query_until("", expected=""), "reconnected after crash"


def _crash_cycle(pg_bin, node, killme, monitor, signal, killme_rx, insert_sql):
    """Acquire the backend pid, kill it with signal, and detect the restart."""
    pid = killme.query_until(_DIGIT_LINE, "SELECT pg_backend_pid();\n").strip()
    killme.query_until(
        r"in-progress-before-sig" + ("quit" if signal == "QUIT" else "kill"), insert_sql
    )
    _monitor_connect(monitor)

    pg_bin.command_ok(
        ["pg_ctl", "kill", signal, pid], "killed process with SIG{}".format(signal)
    )

    killme.wait_for_stderr(killme_rx, "SELECT 1;\n")
    killme.finish()
    monitor.wait_for_stderr(_MONITOR_DIED)
    monitor.finish()
    _reconnect(node)


def test_crash_restart(pg_bin, create_pg):
    """A crashing backend triggers crash-restart; committed rows survive."""
    node = create_pg("primary", allows_streaming=True, start=False)
    node.append_conf(
        "shared_preload_libraries = 'pg_stat_statements'\n"
        "pg_stat_statements.max = 50000\n"
        "compute_query_id = 'regress'"
    )
    node.start()

    node.safe_psql(
        "ALTER SYSTEM SET restart_after_crash = 1;\n"
        "ALTER SYSTEM SET log_connections = receipt;\n"
        "SELECT pg_reload_conf();"
    )
    stats_reset = node.safe_psql(
        "CREATE EXTENSION pg_stat_statements;\n"
        "SELECT stats_reset FROM pg_stat_statements_info;"
    )

    killme = node.background_psql()
    monitor = node.background_psql()

    # SIGQUIT: the backend exits after emitting an error.
    killme.query_until(
        _DIGIT_LINE,
        "CREATE TABLE alive(status text);\n"
        "INSERT INTO alive VALUES($$committed-before-sigquit$$);\n"
        "SELECT pg_backend_pid();\n",
    )
    killme.clear()
    _crash_cycle(
        pg_bin,
        node,
        killme,
        monitor,
        "QUIT",
        _KILLME_SIGQUIT,
        "BEGIN;\n"
        "INSERT INTO alive VALUES($$in-progress-before-sigquit$$) RETURNING status;\n",
    )

    # Restart the psql sessions now that the crash cycle finished.
    killme.restart()
    monitor.restart()

    stats_reset_after = node.safe_psql(
        "SELECT stats_reset FROM pg_stat_statements_info"
    )
    assert stats_reset != stats_reset_after, "pg_stat_statements was reset by restart"

    # SIGKILL: the backend exits without being able to emit an error.
    _crash_cycle(
        pg_bin,
        node,
        killme,
        monitor,
        "KILL",
        _KILLME_SIGKILL,
        "INSERT INTO alive VALUES($$committed-before-sigkill$$) RETURNING status;\n"
        "BEGIN;\n"
        "INSERT INTO alive VALUES($$in-progress-before-sigkill$$) RETURNING status;\n",
    )

    assert node.safe_psql("SELECT * FROM alive") == (
        "committed-before-sigquit\ncommitted-before-sigkill"
    ), "data survived"
    assert (
        node.safe_psql(
            "INSERT INTO alive VALUES($$before-orderly-restart$$) RETURNING status"
        )
        == "before-orderly-restart"
    ), "can still write after crash restart"

    assert node.poll_query_until(
        "SELECT count(*) = 1 FROM pg_stat_activity "
        "WHERE backend_type = 'logical replication launcher'"
    ), "logical replication launcher restarted after crash"

    # An orderly restart still works.
    node.restart()
    assert node.safe_psql("SELECT * FROM alive") == (
        "committed-before-sigquit\ncommitted-before-sigkill\nbefore-orderly-restart"
    ), "data survived"
    assert (
        node.safe_psql(
            "INSERT INTO alive VALUES($$after-orderly-restart$$) RETURNING status"
        )
        == "after-orderly-restart"
    ), "can still write after orderly restart"

    node.stop()
