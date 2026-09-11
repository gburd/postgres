# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""
Port of src/bin/pg_ctl/t/002_status.pl.

Checks pg_ctl status exit codes for a nonexistent data directory, a stopped
server, and a running server.
"""


def test_status(pg_bin, create_pg, tmp_path):
    """pg_ctl status reports the documented exit codes."""
    pg_bin.command_exit_is(
        ["pg_ctl", "status", "--pgdata", tmp_path / "nonexistent"],
        4,
        "pg_ctl status with nonexistent directory",
    )

    node = create_pg("main", start=False)

    pg_bin.command_exit_is(
        ["pg_ctl", "status", "--pgdata", node.datadir],
        3,
        "pg_ctl status with server not running",
    )

    node.start()
    pg_bin.command_exit_is(
        ["pg_ctl", "status", "--pgdata", node.datadir],
        0,
        "pg_ctl status with server running",
    )

    node.stop()
