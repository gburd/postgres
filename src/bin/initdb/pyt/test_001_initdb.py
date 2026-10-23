# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/bin/initdb/t/001_initdb.pl.

To test successful data directory creation with an additional feature, first
try to elaborate the "successful creation" test instead of adding a test:
successful initdb consumes much time and I/O.
"""

import os
import platform
import re

import pypg

windows_os = platform.system() == "Windows"


def test_initdb(pg_bin, tmp_path, monkeypatch):
    """initdb argument handling, successful creation, providers, checksums."""
    xlogdir = tmp_path / "pgxlog"
    datadir = tmp_path / "data"
    supports_syncfs = pg_bin.check_pg_config("#define HAVE_SYNCFS 1")

    pg_bin.program_help_ok("initdb")
    pg_bin.program_version_ok("initdb")
    pg_bin.program_options_handling_ok("initdb")

    pg_bin.command_fails(
        ["initdb", "--sync-only", tmp_path / "nonexistent"],
        "sync missing data directory",
    )

    xlogdir.mkdir()
    (xlogdir / "lost+found").mkdir()
    pg_bin.command_fails(
        ["initdb", "--waldir", xlogdir, datadir], "existing nonempty xlog directory"
    )
    (xlogdir / "lost+found").rmdir()
    pg_bin.command_fails(
        ["initdb", "--waldir", "pgxlog", datadir], "relative xlog directory not allowed"
    )

    pg_bin.command_fails(
        ["initdb", "--username", "pg_test", datadir],
        'role names cannot begin with "pg_"',
    )

    datadir.mkdir()

    # Run one successful test without a TZ setting to exercise initdb's time
    # zone setting code.
    monkeypatch.delenv("TZ", raising=False)
    pg_bin.command_ok(
        [
            "initdb",
            "--no-sync",
            "--text-search-config",
            "german",
            "--set",
            "default_text_search_config=german",
            "--waldir",
            xlogdir,
            datadir,
        ],
        "successful creation",
    )
    if not windows_os:
        assert pypg.check_mode_recursive(
            datadir, 0o700, 0o600
        ), "check PGDATA permissions"

    # Control file should report data checksums enabled by default.
    pg_bin.command_like(
        ["pg_controldata", datadir],
        r"Data page checksum version:.*1",
        "checksums are enabled in control file",
    )

    pg_bin.command_ok(["initdb", "--sync-only", datadir], "sync only")
    pg_bin.command_ok(
        ["initdb", "--sync-only", "--no-sync-data-files", datadir],
        "--no-sync-data-files",
    )
    pg_bin.command_fails(["initdb", datadir], "existing data directory")

    if supports_syncfs:
        pg_bin.command_ok(
            ["initdb", "--sync-only", datadir, "--sync-method", "syncfs"],
            "sync method syncfs",
        )
    else:
        pg_bin.command_fails(
            ["initdb", "--sync-only", datadir, "--sync-method", "syncfs"],
            "sync method syncfs",
        )

    if not windows_os:
        datadir_group = tmp_path / "data_group"
        pg_bin.command_ok(
            ["initdb", "--allow-group-access", datadir_group],
            "successful creation with group access",
        )
        assert pypg.check_mode_recursive(
            datadir_group, 0o750, 0o640
        ), "check PGDATA permissions"

    if os.environ.get("with_icu") == "yes":
        _test_icu_provider(pg_bin, tmp_path)
    else:
        pg_bin.command_fails(
            ["initdb", "--no-sync", "--locale-provider", "icu", tmp_path / "data2"],
            "locale provider ICU fails since no ICU support",
        )

    _test_builtin_provider(pg_bin, tmp_path)
    _test_set_and_checksums(pg_bin, tmp_path)


def _test_icu_provider(pg_bin, tmp_path):
    pg_bin.command_fails_like(
        ["initdb", "--no-sync", "--locale-provider", "icu", tmp_path / "data2"],
        r"initdb: error: locale must be specified if provider is icu",
        "locale provider ICU requires --icu-locale",
    )
    pg_bin.command_ok(
        [
            "initdb",
            "--no-sync",
            "--locale-provider",
            "icu",
            "--icu-locale",
            "en",
            tmp_path / "data3",
        ],
        "option --icu-locale",
    )
    pg_bin.command_like(
        [
            "initdb",
            "--no-sync",
            "--auth",
            "trust",
            "--locale-provider",
            "icu",
            "--locale",
            "und",
            "--lc-collate",
            "C",
            "--lc-ctype",
            "C",
            "--lc-messages",
            "C",
            "--lc-numeric",
            "C",
            "--lc-monetary",
            "C",
            "--lc-time",
            "C",
            tmp_path / "data4",
        ],
        r"(?ms)^\s+default collation:\s+und\n",
        "options --locale-provider=icu --locale=und --lc-*=C",
    )
    for icu_locale, pattern, name in (
        (
            "@colNumeric=lower",
            r"could not open collator for locale",
            "fails for invalid ICU locale",
        ),
        (
            "nonsense-nowhere",
            r'error: locale "nonsense-nowhere" has unknown language "nonsense"',
            "fails for nonsense language",
        ),
        (
            "@colNumeric=lower",
            r'could not open collator for locale "und-u-kn-lower": '
            r"U_ILLEGAL_ARGUMENT_ERROR",
            "fails for invalid collation argument",
        ),
    ):
        pg_bin.command_fails_like(
            [
                "initdb",
                "--no-sync",
                "--locale-provider",
                "icu",
                "--icu-locale",
                icu_locale,
                tmp_path / "dataX",
            ],
            pattern,
            name,
        )
    pg_bin.command_fails_like(
        [
            "initdb",
            "--no-sync",
            "--locale-provider",
            "icu",
            "--encoding",
            "SQL_ASCII",
            "--icu-locale",
            "en",
            tmp_path / "dataX",
        ],
        r"error: encoding mismatch",
        "fails for encoding not supported by ICU",
    )


def _test_builtin_provider(pg_bin, tmp_path):
    pg_bin.command_fails(
        ["initdb", "--no-sync", "--locale-provider", "builtin", tmp_path / "data6"],
        "locale provider builtin fails without --locale",
    )
    pg_bin.command_ok(
        [
            "initdb",
            "--no-sync",
            "--locale-provider",
            "builtin",
            "--locale",
            "C",
            tmp_path / "data7",
        ],
        "locale provider builtin with --locale",
    )
    pg_bin.command_ok(
        [
            "initdb",
            "--no-sync",
            "--locale-provider",
            "builtin",
            "--encoding",
            "UTF-8",
            "--lc-collate",
            "C",
            "--lc-ctype",
            "C",
            "--builtin-locale",
            "C.UTF-8",
            tmp_path / "data8",
        ],
        "locale provider builtin with --encoding=UTF-8 --builtin-locale=C.UTF-8",
    )
    pg_bin.command_fails(
        [
            "initdb",
            "--no-sync",
            "--locale-provider",
            "builtin",
            "--encoding",
            "SQL_ASCII",
            "--lc-collate",
            "C",
            "--lc-ctype",
            "C",
            "--builtin-locale",
            "C.UTF-8",
            tmp_path / "data9",
        ],
        "locale provider builtin with --builtin-locale=C.UTF-8 fails for SQL_ASCII",
    )
    pg_bin.command_ok(
        [
            "initdb",
            "--no-sync",
            "--locale-provider",
            "builtin",
            "--lc-ctype",
            "C",
            "--locale",
            "C",
            tmp_path / "data10",
        ],
        "locale provider builtin with --lc-ctype",
    )
    for args, name in (
        (["--icu-locale", "en"], "fails for locale provider builtin with ICU locale"),
        (["--icu-rules", '""'], "fails for locale provider builtin with ICU rules"),
    ):
        pg_bin.command_fails(
            [
                "initdb",
                "--no-sync",
                "--locale-provider",
                "builtin",
                *args,
                tmp_path / "dataX",
            ],
            name,
        )
    pg_bin.command_fails(
        ["initdb", "--no-sync", "--locale-provider", "xyz", tmp_path / "dataX"],
        "fails for invalid locale provider",
    )
    pg_bin.command_fails(
        [
            "initdb",
            "--no-sync",
            "--locale-provider",
            "libc",
            "--icu-locale",
            "en",
            tmp_path / "dataX",
        ],
        "fails for invalid option combination",
    )


def _test_set_and_checksums(pg_bin, tmp_path):
    pg_bin.command_fails(
        ["initdb", "--no-sync", "--set", "foo=bar", tmp_path / "dataX"],
        "fails for invalid --set option",
    )

    # Multiple --set parameters are added case-insensitively.
    pg_bin.command_ok(
        [
            "initdb",
            "--no-sync",
            "--set",
            "work_mem=128",
            "--set",
            "Work_Mem=256",
            "--set",
            "WORK_MEM=512",
            tmp_path / "dataY",
        ],
        "multiple --set options with different case",
    )
    conf = pypg.slurp_file(tmp_path / "dataY" / "postgresql.conf")
    assert not re.search(r"(?m)^WORK_MEM = ", conf), "WORK_MEM should not be configured"
    assert not re.search(r"(?m)^Work_Mem = ", conf), "Work_Mem should not be configured"
    assert re.search(r"(?m)^work_mem = 512", conf), "work_mem should be in config"

    # --no-data-checksums flag.
    datadir_nochecksums = tmp_path / "data_no_checksums"
    pg_bin.command_ok(
        ["initdb", "--no-data-checksums", datadir_nochecksums],
        "successful creation without data checksums",
    )
    pg_bin.command_like(
        ["pg_controldata", datadir_nochecksums],
        r"Data page checksum version:.*0",
        "checksums are disabled in control file",
    )
    pg_bin.command_fails(
        ["pg_checksums", "--pgdata", datadir_nochecksums],
        "pg_checksums fails with data checksum disabled",
    )
