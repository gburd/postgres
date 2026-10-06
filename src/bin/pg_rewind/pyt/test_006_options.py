# Copyright (c) 2022-2026, PostgreSQL Global Development Group

# pylint: disable=line-too-long,too-many-statements
"""Port of src/bin/pg_rewind/t/006_options.pl.

pg_rewind command-line option validation: too many arguments, no source, both remote and local sources, and --write-recovery-conf without a local source each fail.
Generated from the Perl original via .agent/gen_golden.py.
"""


def test_006_options(pg_bin, tmp_path):
    """pg_rewind command-line option validation."""
    pg_bin.program_help_ok("pg_rewind")
    pg_bin.program_version_ok("pg_rewind")
    pg_bin.program_options_handling_ok("pg_rewind")
    primary_pgdata = tmp_path / "primary"
    standby_pgdata = tmp_path / "standby"
    pg_bin.command_fails(
        [
            "pg_rewind",
            "--debug",
            "--target-pgdata",
            str(primary_pgdata),
            "--source-pgdata",
            str(standby_pgdata),
            "extra_arg1",
        ],
        "too many arguments",
    )
    pg_bin.command_fails(
        ["pg_rewind", "--target-pgdata", str(primary_pgdata)], "no source specified"
    )
    pg_bin.command_fails(
        [
            "pg_rewind",
            "--debug",
            "--target-pgdata",
            str(primary_pgdata),
            "--source-pgdata",
            str(standby_pgdata),
            "--source-server",
            "incorrect_source",
        ],
        "both remote and local sources specified",
    )
    pg_bin.command_fails(
        [
            "pg_rewind",
            "--debug",
            "--target-pgdata",
            str(primary_pgdata),
            "--source-pgdata",
            str(standby_pgdata),
            "--write-recovery-conf",
        ],
        "no local source with --write-recovery-conf",
    )
