# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/bin/pg_checksums/t/001_basic.pl."""


def test_pg_checksums_basic(pg_bin):
    """pg_checksums option handling."""
    pg_bin.program_help_ok("pg_checksums")
    pg_bin.program_version_ok("pg_checksums")
    pg_bin.program_options_handling_ok("pg_checksums")
