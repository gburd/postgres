# Copyright (c) 2022-2026, PostgreSQL Global Development Group

# pylint: disable=line-too-long,too-many-statements
"""Port of src/bin/pg_combinebackup/t/001_basic.pl.

pg_combinebackup argument validation: missing input directories and missing output directory each fail with the documented message.
Generated from the Perl original via .agent/gen_golden.py.
"""


def test_001_basic(pg_bin, tmp_path):
    """pg_combinebackup argument validation."""
    tempdir = tmp_path
    pg_bin.program_help_ok("pg_combinebackup")
    pg_bin.program_version_ok("pg_combinebackup")
    pg_bin.program_options_handling_ok("pg_combinebackup")
    pg_bin.command_fails_like(
        ["pg_combinebackup"],
        r"""no input directories specified""",
        "input directories must be specified",
    )
    pg_bin.command_fails_like(
        ["pg_combinebackup", str(tempdir)],
        r"""no output directory specified""",
        "output directory must be specified",
    )
