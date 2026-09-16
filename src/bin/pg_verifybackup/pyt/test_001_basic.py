# Copyright (c) 2022-2026, PostgreSQL Global Development Group

# pylint: disable=line-too-long,too-many-statements
"""Port of src/bin/pg_verifybackup/t/001_basic.pl.

pg_verifybackup argument validation: missing/invalid target directory, missing
backup_manifest, too many arguments, and the --manifest-path option pointing at
a nonexistent manifest. Generated from the Perl original via .agent/gen_golden.py.
"""


def test_001_basic(pg_bin, tmp_path):
    """pg_verifybackup argument and manifest-path validation."""
    tempdir = tmp_path
    pg_bin.program_help_ok("pg_verifybackup")
    pg_bin.program_version_ok("pg_verifybackup")
    pg_bin.program_options_handling_ok("pg_verifybackup")
    pg_bin.command_fails_like(
        ["pg_verifybackup"],
        r"""no backup directory specified""",
        "target directory must be specified",
    )
    pg_bin.command_fails_like(
        ["pg_verifybackup", str(tempdir)],
        r'''could not open file.*\/backup_manifest\"''',
        "pg_verifybackup requires a manifest",
    )
    pg_bin.command_fails_like(
        ["pg_verifybackup", str(tempdir), str(tempdir)],
        r"""too many command-line arguments""",
        "multiple target directories not allowed",
    )
    (tmp_path / "backup_manifest").write_text("", encoding="utf-8")
    pg_bin.command_fails_like(
        [
            "pg_verifybackup",
            "--manifest-path",
            str(tempdir) + "/not_the_manifest",
            str(tempdir),
        ],
        r'''could not open file.*\/not_the_manifest\"''',
        "pg_verifybackup respects -m flag",
    )
