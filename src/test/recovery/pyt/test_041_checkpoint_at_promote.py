# Copyright (c) 2024-2026, PostgreSQL Global Development Group

"""Port of src/test/recovery/t/041_checkpoint_at_promote.pl.

A restartpoint in progress on a standby (paused via an injection point) must
complete after the standby is promoted, and the freshly-promoted primary must
survive a backend SIGKILL through crash recovery (restart_after_crash) and
accept new connections. Requires injection points.
"""

import os

import pytest


def test_041_checkpoint_at_promote(create_pg):
    """An in-progress restartpoint completes post-promote; crash recovery works."""
    if os.environ.get("enable_injection_points") != "yes":
        pytest.skip("Injection points not supported by this build")
    primary = create_pg("master", allows_streaming=True, start=False)
    primary.append_conf("\nlog_checkpoints = on\nrestart_after_crash = on\n")
    primary.start()
    if not primary.check_extension("injection_points"):
        pytest.skip("Extension injection_points not installed")
    backup_name = "my_backup"
    primary.backup(backup_name)
    standby = create_pg(
        "standby1", from_backup=(primary, backup_name), has_streaming=True, start=False
    )
    standby.start()
    primary.safe_psql("checkpoint")
    primary.safe_psql("CREATE TABLE prim_tab (a int);")
    primary.safe_psql("CREATE EXTENSION injection_points;")
    primary.wait_for_replay_catchup(standby)
    standby.safe_psql("SELECT injection_points_attach('create-restart-point', 'wait');")
    logstart = standby.current_log_position()
    psql_session = standby.background_psql("postgres", on_error_stop=False)
    psql_session.query_until(
        r"starting_checkpoint", "\n   \\echo starting_checkpoint\n   CHECKPOINT;\n"
    )
    primary.safe_psql("INSERT INTO prim_tab VALUES (1);")
    primary.safe_psql("SELECT pg_switch_wal();")
    primary.wait_for_replay_catchup(standby)
    standby.wait_for_event("checkpointer", "create-restart-point")
    assert standby.log_contains(
        "restartpoint starting: fast wait", logstart
    ), "restartpoint has started"
    primary.stop()
    standby.promote()
    logstart = standby.current_log_position()
    standby.safe_psql("SELECT injection_points_wakeup('create-restart-point');")
    assert standby.wait_for_log(
        r"restartpoint complete", logstart
    ), "restart point has completed"
    killme = standby.background_psql("postgres")
    pid = killme.query("SELECT pg_backend_pid();").strip()
    standby.signal_backend(int(pid), "KILL")
    killme.wait_for_stderr(
        r"server closed the connection unexpectedly|connection to server was lost"
        r"|could not send data to server",
        "SELECT 1;\n",
    )
    killme.quit()
    assert standby.poll_query_until("", ""), "server back up after crash recovery"
    res = standby.psql_capture("select 1")
    assert res.rc == 0, "psql connect success"
    assert res.stdout == "1", "psql select 1"
