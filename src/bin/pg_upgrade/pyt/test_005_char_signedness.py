# Copyright (c) 2024-2026, PostgreSQL Global Development Group

"""Port of src/bin/pg_upgrade/t/005_char_signedness.pl.

pg_upgrade propagates the default char data signedness from the old cluster.
After flipping the old cluster to 'unsigned' via pg_resetwal, pg_upgrade rejects
--set-char-signedness (it cannot be used upgrading from v18+) and, on a plain
run, the upgraded new cluster inherits the old cluster's unsigned signedness.
"""

import os


def test_005_char_signedness(create_pg, pg_bin, tmp_check, monkeypatch):
    """Char signedness is carried from the old cluster through pg_upgrade."""
    mode = os.environ.get("PG_TEST_PG_UPGRADE_MODE") or "--copy"
    old = create_pg("old", start=False)
    new = create_pg("new", start=False)
    pg_bin.command_like(
        ["pg_controldata", old.datadir],
        r"Default char data signedness:\s+signed",
        "default char signedness of old cluster is signed in control file",
    )
    pg_bin.command_like(
        ["pg_controldata", new.datadir],
        r"Default char data signedness:\s+signed",
        "default char signedness of new cluster is signed in control file",
    )
    pg_bin.command_ok(
        ["pg_resetwal", "--char-signedness", "unsigned", "--force", old.datadir],
        "set old cluster's default char signedness to unsigned",
    )
    pg_bin.command_like(
        ["pg_controldata", old.datadir],
        r"Default char data signedness:\s+unsigned",
        "updated default char signedness is unsigned in control file",
    )
    monkeypatch.chdir(tmp_check)
    pg_bin.command_checks_all(
        [
            "pg_upgrade",
            "--no-sync",
            "--old-datadir",
            old.datadir,
            "--new-datadir",
            new.datadir,
            "--old-bindir",
            old.config_data("--bindir"),
            "--new-bindir",
            new.config_data("--bindir"),
            "--socketdir",
            new.host,
            "--old-port",
            str(old.port),
            "--new-port",
            str(new.port),
            "--set-char-signedness",
            "signed",
            mode,
        ],
        1,
        [r"option --set-char-signedness cannot be used"],
        [],
        "--set-char-signedness option cannot be used for upgrading from v18 or later",
    )
    pg_bin.command_ok(
        [
            "pg_upgrade",
            "--no-sync",
            "--old-datadir",
            old.datadir,
            "--new-datadir",
            new.datadir,
            "--old-bindir",
            old.config_data("--bindir"),
            "--new-bindir",
            new.config_data("--bindir"),
            "--socketdir",
            new.host,
            "--old-port",
            str(old.port),
            "--new-port",
            str(new.port),
            mode,
        ],
        "run of pg_upgrade",
    )
    pg_bin.command_like(
        ["pg_controldata", new.datadir],
        r"Default char data signedness:\s+unsigned",
        "the default char signedness is updated during pg_upgrade",
    )
