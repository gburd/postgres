# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/bin/pg_test_fsync/t/001_basic.pl."""


def test_pg_test_fsync(pg_bin):
    """pg_test_fsync option validation."""
    pg_bin.program_help_ok("pg_test_fsync")
    pg_bin.program_version_ok("pg_test_fsync")
    pg_bin.program_options_handling_ok("pg_test_fsync")

    pg_bin.command_fails_like(
        ["pg_test_fsync", "--secs-per-test", "a"],
        r"pg_test_fsync: error: invalid argument for option --secs-per-test",
        "pg_test_fsync: invalid argument for option --secs-per-test",
    )
    pg_bin.command_fails_like(
        ["pg_test_fsync", "--secs-per-test", "0"],
        r"pg_test_fsync: error: --secs-per-test must be in range 1\.\.4294967295",
        "pg_test_fsync: --secs-per-test must be in range",
    )
