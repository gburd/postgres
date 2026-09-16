# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/bin/scripts/t/080_pg_isready.pl."""

import os


def test_pg_isready(pg_bin, create_pg):
    """pg_isready fails with no server and succeeds once the server is up."""
    pg_bin.program_help_ok("pg_isready")
    pg_bin.program_version_ok("pg_isready")
    pg_bin.program_options_handling_ok("pg_isready")

    node = create_pg("main", start=False)

    node.command_fails(["pg_isready"], "fails with no server running")

    node.start()

    timeout_default = os.environ.get("PG_TEST_TIMEOUT_DEFAULT", "180")
    node.command_ok(
        ["pg_isready", "--timeout", timeout_default],
        "succeeds with server running",
    )
