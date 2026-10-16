# Copyright (c) 2022-2026, PostgreSQL Global Development Group

# pylint: disable=line-too-long,too-many-statements
"""Port of src/test/postmaster/t/001_basic.pl.

postgres (postmaster) --help / --version / invalid-option handling.
Generated from the Perl original via .agent/gen_golden.py.
"""


def test_001_basic(pg_bin):
    """postgres (postmaster) --help / --version / invalid-option handling.."""
    pg_bin.program_help_ok("postgres")
    pg_bin.program_version_ok("postgres")
    pg_bin.program_options_handling_ok("postgres")
