# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/bin/pg_archivecleanup/t/010_pg_archivecleanup.pl."""

import re

# Each entry is (name, present): whether the file should still exist after
# pg_archivecleanup runs.
_WALFILES_VERBOSE = [
    ("00000001000000370000000D", False),
    ("00000001000000370000000E", True),
]
_WALFILES_WITH_GZ = [
    ("00000001000000370000000C.gz", False),
    ("00000001000000370000000D", False),
    ("00000001000000370000000D.backup", True),
    ("00000001000000370000000E", True),
    ("00000001000000370000000F.partial", True),
    ("unrelated_file", True),
]
_WALFILES_CLEAN_BACKUP_HISTORY = [
    ("00000001000000370000000D", False),
    ("00000001000000370000000D.00000028.backup", False),
    ("00000001000000370000000E", True),
    ("00000001000000370000000F.partial", True),
    ("unrelated_file", True),
]


def _create_files(tempdir, walfiles):
    for name, _present in walfiles:
        (tempdir / name).write_text("CONTENT", encoding="utf-8")


def _remove_files(tempdir, walfiles):
    for name, _present in walfiles:
        (tempdir / name).unlink(missing_ok=True)


def _run_check(pg_bin, tempdir, testdata, oldest_kept, test_name, *options):
    _create_files(tempdir, testdata)

    pg_bin.command_ok(
        ["pg_archivecleanup", *options, str(tempdir), oldest_kept],
        "{}: runs".format(test_name),
    )

    for name, present in testdata:
        exists = (tempdir / name).is_file()
        if present:
            assert exists, "{}:{} was not cleaned up".format(test_name, name)
        else:
            assert not exists, "{}:{} was cleaned up".format(test_name, name)

    _remove_files(tempdir, testdata)


def test_pg_archivecleanup(pg_bin, tmp_path):
    """pg_archivecleanup argument handling, dry run, and cleanup scenarios."""
    pg_bin.program_help_ok("pg_archivecleanup")
    pg_bin.program_version_ok("pg_archivecleanup")
    pg_bin.program_options_handling_ok("pg_archivecleanup")

    pg_bin.command_fails_like(
        ["pg_archivecleanup"],
        r"must specify archive location",
        "fails if archive location is not specified",
    )
    pg_bin.command_fails_like(
        ["pg_archivecleanup", str(tmp_path)],
        r"must specify oldest kept WAL file",
        "fails if oldest kept WAL file name is not specified",
    )
    pg_bin.command_fails_like(
        ["pg_archivecleanup", "notexist", "foo"],
        r"archive location .* does not exist",
        "fails if archive location does not exist",
    )
    pg_bin.command_fails_like(
        ["pg_archivecleanup", str(tmp_path), "foo", "bar"],
        r"too many command-line arguments",
        "fails with too many command-line arguments",
    )
    pg_bin.command_fails_like(
        ["pg_archivecleanup", str(tmp_path), "foo"],
        r"invalid file name argument",
        "fails with invalid restart file name",
    )

    # Dry run: no files are physically removed, but logs show what would be.
    _create_files(tmp_path, _WALFILES_VERBOSE)
    result = pg_bin.result(
        [
            "pg_archivecleanup",
            "--debug",
            "--dry-run",
            str(tmp_path),
            "00000001000000370000000E",
        ]
    )
    assert result.rc == 0, "pg_archivecleanup dry run: exit code 0"

    for name, present in _WALFILES_VERBOSE:
        pattern = r"{}.*would be removed".format(name)
        if present:
            assert not re.search(
                pattern, result.stderr
            ), "pg_archivecleanup dry run for {}: matches".format(name)
        else:
            assert re.search(
                pattern, result.stderr
            ), "pg_archivecleanup dry run for {}: matches".format(name)
    for name, _present in _WALFILES_VERBOSE:
        assert (tmp_path / name).is_file(), "{} not removed".format(name)
    _remove_files(tmp_path, _WALFILES_VERBOSE)

    _run_check(
        pg_bin,
        tmp_path,
        _WALFILES_WITH_GZ,
        "00000001000000370000000E",
        "pg_archivecleanup",
        "-x.gz",
    )
    _run_check(
        pg_bin,
        tmp_path,
        _WALFILES_WITH_GZ,
        "00000001000000370000000E.partial",
        "pg_archivecleanup with .partial file",
        "-x.gz",
    )
    _run_check(
        pg_bin,
        tmp_path,
        _WALFILES_WITH_GZ,
        "00000001000000370000000E.00000020.backup",
        "pg_archivecleanup with .backup file",
        "-x.gz",
    )
    _run_check(
        pg_bin,
        tmp_path,
        _WALFILES_CLEAN_BACKUP_HISTORY,
        "00000001000000370000000E",
        "pg_archivecleanup with --clean-backup-history",
        "-b",
    )
