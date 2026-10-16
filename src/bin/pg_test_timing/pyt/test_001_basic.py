# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/bin/pg_test_timing/t/001_basic.pl."""

import re


def test_pg_test_timing(pg_bin):
    """pg_test_timing option validation and a basic run."""
    pg_bin.program_help_ok("pg_test_timing")
    pg_bin.program_version_ok("pg_test_timing")
    pg_bin.program_options_handling_ok("pg_test_timing")

    pg_bin.command_fails_like(
        ["pg_test_timing", "--duration", "a"],
        r"pg_test_timing: invalid argument for option --duration",
        "pg_test_timing: invalid argument for option --duration",
    )
    pg_bin.command_fails_like(
        ["pg_test_timing", "--duration", "0"],
        r"pg_test_timing: --duration must be in range 1\.\.4294967295",
        "pg_test_timing: --duration must be in range",
    )
    pg_bin.command_fails_like(
        ["pg_test_timing", "--cutoff", "101"],
        r"pg_test_timing: --cutoff must be in range 0\.\.100",
        "pg_test_timing: --cutoff must be in range",
    )

    # We can't check for specific output, but a simple run should produce
    # the expected headers.
    result = pg_bin.result(["pg_test_timing", "--duration", "1"])
    assert result.rc == 0, "pg_test_timing: exit code 0"
    assert result.stderr == "", "pg_test_timing: no stderr"
    assert re.search(
        r"(?s)Testing timing overhead for 1 second\..*"
        r"Histogram of timing durations:.*"
        r"Observed timing durations up to 99\.9900%:",
        result.stdout,
    ), "pg_test_timing: stdout passes sanity check"
