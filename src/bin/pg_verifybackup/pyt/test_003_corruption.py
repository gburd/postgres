# Copyright (c) 2020-2026, PostgreSQL Global Development Group

# pylint: disable=line-too-long
"""Port of src/bin/pg_verifybackup/t/003_corruption.pl.

pg_verifybackup detects each way a base backup can be corrupted: extra files
(in the data dir and in a tablespace), missing files/tablespaces, appended or
truncated files, a replaced file (checksum mismatch), a wrong system identifier,
a bad manifest checksum, and unreadable files/directories. Each scenario takes a
fresh tablespace-mapped backup, verifies it intact, mutilates it, and checks
pg_verifybackup fails with the matching message -- for both directory-format and
(where applicable) tar-format backups.
"""

import os
import shutil
import subprocess
import tempfile

import pypg


def _create_extra_file(backup_path, relative_path):
    with open(os.path.join(backup_path, relative_path), "w", encoding="utf-8") as fh:
        fh.write("This is an extra file.\n")


def _only_entry(path):
    return [e for e in pypg.slurp_dir(path) if e not in (".", "..")][0]


def _mutilate_extra_file(backup_path):
    _create_extra_file(backup_path, "extra_file")


def _mutilate_extra_tablespace_file(backup_path):
    tsoid = _only_entry(os.path.join(backup_path, "pg_tblspc"))
    catvdir = _only_entry(os.path.join(backup_path, "pg_tblspc", tsoid))
    tsdboid = _only_entry(os.path.join(backup_path, "pg_tblspc", tsoid, catvdir))
    _create_extra_file(
        backup_path, "pg_tblspc/{}/{}/{}/extra_ts_file".format(tsoid, catvdir, tsdboid)
    )


def _mutilate_missing_file(backup_path):
    os.unlink(os.path.join(backup_path, "pg_xact", "0000"))


def _mutilate_missing_tablespace(backup_path):
    tsoid = _only_entry(os.path.join(backup_path, "pg_tblspc"))
    os.unlink(os.path.join(backup_path, "pg_tblspc", tsoid))


def _mutilate_append_to_file(backup_path):
    pypg.append_to_file(os.path.join(backup_path, "global", "pg_control"), "x")


def _mutilate_truncate_file(backup_path):
    with open(os.path.join(backup_path, "pg_hba.conf"), "w", encoding="utf-8"):
        pass


def _mutilate_replace_file(backup_path):
    pathname = os.path.join(backup_path, "PG_VERSION")
    contents = pypg.slurp_file(pathname)
    with open(pathname, "w", encoding="utf-8") as fh:
        fh.write("q" * len(contents))


def _mutilate_bad_manifest(backup_path):
    pypg.append_to_file(os.path.join(backup_path, "backup_manifest"), "\n")


def _mutilate_open_file_fails(backup_path):
    os.chmod(os.path.join(backup_path, "PG_VERSION"), 0)


def _mutilate_open_directory_fails(backup_path):
    os.chmod(os.path.join(backup_path, "pg_subtrans"), 0)


def _cleanup_open_directory_fails(backup_path):
    os.chmod(os.path.join(backup_path, "pg_subtrans"), 0o700)


def _mutilate_search_directory_fails(backup_path):
    os.chmod(os.path.join(backup_path, "base"), 0o400)


def _cleanup_search_directory_fails(backup_path):
    os.chmod(os.path.join(backup_path, "base"), 0o700)


def _make_system_identifier_mutilator(create_pg):
    def mutilate(backup_path):
        node = create_pg("node", force_initdb=True, allows_streaming=True)
        node.backup("backup2")
        shutil.move(
            os.path.join(str(node.backup_dir), "backup2", "backup_manifest"),
            os.path.join(backup_path, "backup_manifest"),
        )
        node.teardown_node(fail_ok=True)

    return mutilate


def _scenarios(create_pg):
    return [
        (
            "extra_file",
            _mutilate_extra_file,
            None,
            r'extra_file.*present (on disk|in archive "[^"]+") but not in the manifest',
            False,
        ),
        (
            "extra_tablespace_file",
            _mutilate_extra_tablespace_file,
            None,
            r'extra_ts_file.*present (on disk|in archive "[^"]+") but not in the manifest',
            False,
        ),
        (
            "missing_file",
            _mutilate_missing_file,
            None,
            r'pg_xact/0000.*present in the manifest but not (on disk|in archive "[^"]+")',
            False,
        ),
        (
            "missing_tablespace",
            _mutilate_missing_tablespace,
            None,
            r'pg_tblspc.*present in the manifest but not (on disk|in archive "[^"]+")',
            False,
        ),
        (
            "append_to_file",
            _mutilate_append_to_file,
            None,
            r'has size \d+ (on disk|in archive "[^"]+") but size \d+ in the manifest',
            False,
        ),
        (
            "truncate_file",
            _mutilate_truncate_file,
            None,
            r'has size 0 (on disk|in archive "[^"]+") but size \d+ in the manifest',
            False,
        ),
        (
            "replace_file",
            _mutilate_replace_file,
            None,
            r"checksum mismatch for file",
            False,
        ),
        (
            "system_identifier",
            _make_system_identifier_mutilator(create_pg),
            None,
            r"manifest system identifier is .*, but control file has",
            False,
        ),
        (
            "bad_manifest",
            _mutilate_bad_manifest,
            None,
            r"manifest checksum mismatch",
            False,
        ),
        (
            "open_file_fails",
            _mutilate_open_file_fails,
            None,
            r"could not open file",
            True,
        ),
        (
            "open_directory_fails",
            _mutilate_open_directory_fails,
            _cleanup_open_directory_fails,
            r"could not open directory",
            True,
        ),
        (
            "search_directory_fails",
            _mutilate_search_directory_fails,
            _cleanup_search_directory_fails,
            r"could not stat file or directory",
            True,
        ),
    ]


def _tar_check(primary, name, backup_path, fails_like, tar, tar_flags):
    tar_backup_path = os.path.join(str(primary.backup_dir), "tar_" + name)
    os.mkdir(tar_backup_path)
    tblspc = os.path.join(backup_path, "pg_tblspc")
    for tsoid in [e for e in pypg.slurp_dir(tblspc) if e not in (".", "..")]:
        tspath = os.path.join(tblspc, tsoid)
        subprocess.run(
            [
                tar,
                *tar_flags,
                "-cf",
                os.path.join(tar_backup_path, tsoid + ".tar"),
                ".",
            ],
            cwd=tspath,
            check=True,
        )
        shutil.rmtree(tspath)
    subprocess.run(
        [tar, *tar_flags, "-cf", os.path.join(tar_backup_path, "pg_wal.tar"), "."],
        cwd=os.path.join(backup_path, "pg_wal"),
        check=True,
    )
    shutil.rmtree(os.path.join(backup_path, "pg_wal"))
    shutil.move(
        os.path.join(backup_path, "backup_manifest"),
        os.path.join(tar_backup_path, "backup_manifest"),
    )
    subprocess.run(
        [tar, *tar_flags, "-cf", os.path.join(tar_backup_path, "base.tar"), "."],
        cwd=backup_path,
        check=True,
    )
    primary.command_fails_like(
        ["pg_verifybackup", tar_backup_path],
        fails_like,
        "corrupt backup fails verification: " + name,
    )
    shutil.rmtree(tar_backup_path)


def _tar_portability_options(tar):
    """Return portability flags for tar (mirrors Utils::tar_portability_options).

    Prefer GNU/BSD ustar with owner/group 0; fall back to OpenBSD '-F ustar';
    otherwise no flags.
    """
    if not tar:
        return []
    devnull = os.devnull
    gnu = subprocess.run(
        [tar, "--format=ustar", "--owner=0", "--group=0", "-cf", devnull, devnull],
        stderr=subprocess.DEVNULL,
        check=False,
    )
    if gnu.returncode == 0:
        return ["--format=ustar", "--owner=0", "--group=0"]
    obsd = subprocess.run(
        [tar, "-F", "ustar", "-cf", devnull, devnull],
        stderr=subprocess.DEVNULL,
        check=False,
    )
    if obsd.returncode == 0:
        return ["-F", "ustar"]
    return []


def test_003_corruption(create_pg):
    """pg_verifybackup detects each kind of backup corruption."""
    tar = os.environ.get("TAR")
    tar_flags = _tar_portability_options(tar)
    primary = create_pg("primary", allows_streaming=True)
    source_ts_path = tempfile.mkdtemp(prefix="ts_")
    primary.safe_psql(
        "CREATE TABLE x1 (a int);\nINSERT INTO x1 VALUES (111);\n"
        "CREATE TABLESPACE ts1 LOCATION '{}';\n"
        "CREATE TABLE x2 (a int) TABLESPACE ts1;\n"
        "INSERT INTO x1 VALUES (222);".format(source_ts_path)
    )
    for name, mutilate, cleanup, fails_like, needs_perms in _scenarios(create_pg):
        if needs_perms and os.name == "nt":
            continue
        backup_path = os.path.join(str(primary.backup_dir), name)
        backup_ts_path = tempfile.mkdtemp(prefix="ts_")
        primary.command_ok(
            [
                "pg_basebackup",
                "--pgdata",
                backup_path,
                "--no-sync",
                "--checkpoint",
                "fast",
                "--tablespace-mapping",
                "{}={}".format(source_ts_path, backup_ts_path),
            ],
            "base backup ok",
        )
        primary.command_ok(["pg_verifybackup", backup_path], "intact backup verified")
        mutilate(backup_path)
        primary.command_fails_like(
            ["pg_verifybackup", backup_path],
            fails_like,
            "corrupt backup fails verification: " + name,
        )
        if cleanup:
            cleanup(backup_path)
        if not needs_perms and tar:
            _tar_check(primary, name, backup_path, fails_like, tar, tar_flags)
        shutil.rmtree(backup_path, ignore_errors=True)
