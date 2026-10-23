# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/bin/pg_walsummary/t/001_basic.pl."""


def test_pg_walsummary_basic(pg_bin):
    """pg_walsummary option handling and required-input check."""
    pg_bin.program_help_ok("pg_walsummary")
    pg_bin.program_version_ok("pg_walsummary")
    pg_bin.program_options_handling_ok("pg_walsummary")

    pg_bin.command_fails_like(
        ["pg_walsummary"],
        r"no input files specified",
        "input files must be specified",
    )
