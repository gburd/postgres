# Copyright (c) 2022-2026, PostgreSQL Global Development Group

# pylint: disable=line-too-long,too-many-statements
"""Port of contrib/vacuumlo/t/001_basic.pl.

vacuumlo --help / --version / invalid-option handling.
Generated from the Perl original via .agent/gen_golden.py.
"""


def test_001_basic(pg_bin):
    """vacuumlo --help / --version / invalid-option handling.."""
    pg_bin.program_help_ok("vacuumlo")
    pg_bin.program_version_ok("vacuumlo")
    pg_bin.program_options_handling_ok("vacuumlo")
