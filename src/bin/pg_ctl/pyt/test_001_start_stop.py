# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""
Port of src/bin/pg_ctl/t/001_start_stop.pl.

Drives pg_ctl directly (initdb/start/stop/restart) against a hand-configured
data directory and checks default and group-access file permissions.
"""

import os
import platform

import pytest

import pypg

windows_os = platform.system() == "Windows"
use_unix_sockets = not windows_os


def test_start_stop(pg_bin, tmp_path, sockdir):
    """pg_ctl start/stop/restart and the resulting file permissions."""
    pg_regress = os.environ.get("PG_REGRESS")
    if not pg_regress:
        pytest.skip("PG_REGRESS environment variable is not set")

    pg_bin.program_help_ok("pg_ctl")
    pg_bin.program_version_ok("pg_ctl")
    pg_bin.program_options_handling_ok("pg_ctl")

    pg_bin.command_exit_is(
        ["pg_ctl", "start", "--pgdata", tmp_path / "nonexistent"],
        1,
        "pg_ctl start with nonexistent directory",
    )

    data = tmp_path / "data"
    pg_bin.command_ok(
        ["pg_ctl", "initdb", "--pgdata", data, "--options", "--no-sync"],
        "pg_ctl initdb",
    )
    pg_bin.command_ok([pg_regress, "--config-auth", data], "configure authentication")

    node_port = pypg.get_free_port()
    with open(data / "postgresql.conf", "a", encoding="utf-8") as conf:
        conf.write("fsync = off\n")
        conf.write("port = {}\n".format(node_port))
        temp_config = os.environ.get("TEMP_CONFIG")
        if temp_config:
            conf.write(pypg.slurp_file(temp_config))
        if use_unix_sockets:
            conf.write("listen_addresses = ''\n")
            conf.write("unix_socket_directories = '{}'\n".format(sockdir))
        else:
            conf.write("listen_addresses = '127.0.0.1'\n")

    log_path = tmp_path / "001_start_stop_server.log"
    pg_bin.command_like(
        ["pg_ctl", "start", "--pgdata", data, "--log", log_path],
        r"(?s)done.*server started",
        "pg_ctl start",
    )

    pg_bin.command_fails(
        ["pg_ctl", "start", "--pgdata", data], "second pg_ctl start fails"
    )
    pg_bin.command_ok(["pg_ctl", "stop", "--pgdata", data], "pg_ctl stop")
    pg_bin.command_fails(
        ["pg_ctl", "stop", "--pgdata", data], "second pg_ctl stop fails"
    )

    # Log file for default permission test.
    log_file_name = data / "perm-test-600.log"
    pg_bin.command_ok(
        ["pg_ctl", "restart", "--pgdata", data, "--log", log_file_name],
        "pg_ctl restart with server not running",
    )

    # Permissions on log file should be default.
    if not windows_os:
        assert log_file_name.is_file()
        assert pypg.check_mode_recursive(data, 0o700, 0o600)

    # Log file for group access test.
    log_file_name = data / "perm-test-640.log"
    if not windows_os:
        # Stop, then change the data dir mode so the log file will be created
        # with group read privileges on the next start.
        pg_bin.command_ok(["pg_ctl", "stop", "--pgdata", data])
        pypg.chmod_recursive(data, 0o750, 0o640)

        pg_bin.command_ok(
            ["pg_ctl", "start", "--pgdata", data, "--log", log_file_name],
            "start server to check group permissions",
        )

        assert log_file_name.is_file()
        assert pypg.check_mode_recursive(data, 0o750, 0o640)

    pg_bin.command_ok(
        ["pg_ctl", "restart", "--pgdata", data, "--log", log_file_name],
        "pg_ctl restart with server running",
    )

    pg_bin.command_ok(["pg_ctl", "stop", "--pgdata", data])
