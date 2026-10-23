# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/bin/pg_config/t/001_pg_config.pl."""


def test_pg_config(pg_bin):
    """pg_config option handling and output shape."""
    pg_bin.program_help_ok("pg_config")
    pg_bin.program_version_ok("pg_config")
    pg_bin.program_options_handling_ok("pg_config")
    pg_bin.command_like(
        ["pg_config", "--bindir"], r"bin", "pg_config single option"
    )  # XXX might be wrong
    pg_bin.command_like(
        ["pg_config", "--bindir", "--libdir"],
        r"bin.*\n.*lib",
        "pg_config two options",
    )
    pg_bin.command_like(
        ["pg_config", "--libdir", "--bindir"],
        r"lib.*\n.*bin",
        "pg_config two options different order",
    )
    pg_bin.command_like(
        ["pg_config"],
        r".*\n.*\n.*",
        "pg_config without options prints many lines",
    )
