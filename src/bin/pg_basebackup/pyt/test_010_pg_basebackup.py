# Copyright (c) 2021-2026, PostgreSQL Global Development Group

# pylint: disable=line-too-long,too-many-lines
"""Port of src/bin/pg_basebackup/t/010_pg_basebackup.pl.

The broad pg_basebackup test: option sanity, client- and server-side
compression-spec failures, exclusion of non-copied files/forks/temp relations,
permission checks, separate WAL directory, tar format, tablespace mapping
(plain and tar restore), symlinks, recovery-conf generation, WAL fetch/stream
modes, backup targets, replication slots, checksum-mismatch reporting, gzip
compression, a background-stream-process termination test, an in-place
tablespace backup, and the different-system-identifier incremental failure.
"""

import glob
import os
import re
import shutil
import subprocess
import tempfile
import threading
import time

import pypg

# Options shared by nearly all pg_basebackup invocations, mirroring the Perl
# @pg_basebackup_defs (keep test times reasonable).
_DEFS = ["pg_basebackup", "--no-sync", "-cfast"]

# Files that should never be copied into a backup.
_DONOTCOPY_FILES = [
    "backup_label",
    "tablespace_map",
    "postgresql.auto.conf.tmp",
    "current_logfiles.tmp",
    "global/pg_internal.init.123",
]

_TEMP_RELATION_FILES = ["t999_999", "t9999_999.1", "t999_9999_vm", "t99999_99999_vm.1"]

_COMPRESSION_FAILURE_TESTS = [
    (
        "extrasquishy",
        'unrecognized compression algorithm: "extrasquishy"',
        "failure on invalid compression algorithm",
    ),
    (
        "gzip:",
        "invalid compression specification: found empty string where a compression option was expected",
        "failure on empty compression options list",
    ),
    (
        "gzip:thunk",
        'invalid compression specification: unrecognized compression option: "thunk"',
        "failure on unknown compression option",
    ),
    (
        "gzip:level",
        'invalid compression specification: compression option "level" requires a value',
        "failure on missing compression level",
    ),
    (
        "gzip:level=",
        'invalid compression specification: value for compression option "level" must be an integer',
        "failure on empty compression level",
    ),
    (
        "gzip:level=high",
        'invalid compression specification: value for compression option "level" must be an integer',
        "failure on non-numeric compression level",
    ),
    (
        "gzip:level=236",
        'invalid compression specification: compression algorithm "gzip" expects a compression level between 1 and 9',
        "failure on out-of-range compression level",
    ),
    (
        "gzip:level=9,",
        "invalid compression specification: found empty string where a compression option was expected",
        "failure on extra, empty compression option",
    ),
    (
        "gzip:workers=3",
        'invalid compression specification: compression algorithm "gzip" does not accept a worker count',
        "failure on worker count for gzip",
    ),
    (
        "gzip:long",
        'invalid compression specification: compression algorithm "gzip" does not support long-distance mode',
        "failure on long mode for gzip",
    ),
]

_SUPERLONGNAME = "superlongname_" + ("x" * 100)


class _BgCommand:
    """A background command whose stderr is captured for pump_until.

    The Python analogue of the ``IPC::Run::start`` handle the Perl test keeps:
    stderr is read by a thread so the test can poll the accumulated stderr for
    a pattern (mirroring ``pump_until``), then ``finish`` waits for exit.
    """

    def __init__(self, cmd, env, timeout):
        self._timeout = timeout
        self._lock = threading.Lock()
        self._stderr = ""
        # pylint: disable=consider-using-with  # long-lived; closed in finish()
        self._proc = subprocess.Popen(
            cmd,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            env=env,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
        )
        self._thread = threading.Thread(target=self._reader, daemon=True)
        self._thread.start()

    def _reader(self):
        assert self._proc.stderr is not None
        for line in iter(self._proc.stderr.readline, ""):
            with self._lock:
                self._stderr += line

    def pump_until(self, pattern):
        """Poll captured stderr until pattern matches; return True, else False."""
        regex = re.compile(pattern, re.DOTALL)
        deadline = time.monotonic() + self._timeout
        while True:
            with self._lock:
                if regex.search(self._stderr):
                    return True
            exited = self._proc.poll() is not None
            with self._lock:
                if exited and regex.search(self._stderr):
                    return True
            if exited or time.monotonic() > deadline:
                return False
            time.sleep(0.05)

    def finish(self):
        """Wait for the process to exit and join the reader thread."""
        self._proc.wait()
        self._thread.join()


def _badchars_file(tempdir):
    """Write a file with a non-UTF8 name (some Windows code pages reject it).

    Mirrors the Perl test, which writes into $tempdir/pgdata (a scratch dir),
    not the server's data directory.
    """
    name = os.fsencode("{}/pgdata/".format(tempdir)) + b"FOO\xe0\xe0\xe0BAR"
    with open(name, "ab") as fh:
        fh.write(b"test backup of file with non-UTF8 name\n")


def _option_and_wal_config_failures(node, tempdir, pgdata):
    """Option sanity, missing WAL config, and the no-clean directory behaviors."""
    node.command_fails(
        ["pg_basebackup"], "pg_basebackup needs target directory specified"
    )
    node.command_fails_like(
        [
            "pg_basebackup",
            "--pgdata",
            "{}/backup".format(tempdir),
            "--compress",
            "none:1",
        ],
        r'compression algorithm "none" does not accept a compression level',
        'failure if method "none" specified with compression level',
    )
    node.command_fails_like(
        [
            "pg_basebackup",
            "--pgdata",
            "{}/backup".format(tempdir),
            "--compress",
            "none+",
        ],
        r'unrecognized compression algorithm: "none\+"',
        "failure on incorrect separator to define compression level",
    )
    _badchars_file(tempdir)
    node.append_conf(
        "\n# Allow replication (set up by the test)\n", filename="pg_hba.conf"
    )
    node.reload()
    node.command_fails(
        _DEFS + ["--pgdata", "{}/backup".format(tempdir)],
        "pg_basebackup fails because of WAL configuration",
    )
    assert not os.path.isdir(
        "{}/backup".format(tempdir)
    ), "backup directory was cleaned up"
    # A non-empty backup directory makes the next run fail but leaves it behind.
    os.mkdir("{}/backup".format(tempdir))
    pypg.append_to_file("{}/backup/dir-not-empty.txt".format(tempdir), "Some data")
    node.command_fails(
        _DEFS + ["--pgdata", "{}/backup".format(tempdir), "-n"],
        "failing run with no-clean option",
    )
    assert os.path.isdir(
        "{}/backup".format(tempdir)
    ), "backup directory was created and left behind"
    shutil.rmtree("{}/backup".format(tempdir))
    pypg.append_to_file(
        pgdata / "postgresql.conf",
        "max_replication_slots = 10\nmax_wal_senders = 10\nwal_level = replica\n",
    )
    node.restart()


def _compression_failure_tests(node, tempdir):
    """Client- and server-side invalid compression specs both fail (ZLIB only)."""
    if not pypg.check_pg_config(r"#define HAVE_LIBZ 1"):
        return
    client_fails = "pg_basebackup: error: "
    server_fails = "pg_basebackup: error: could not initiate base backup: ERROR:  "
    for spec, message, desc in _COMPRESSION_FAILURE_TESTS:
        node.command_fails_like(
            [
                "pg_basebackup",
                "--pgdata",
                "{}/backup".format(tempdir),
                "--compress",
                spec,
            ],
            re.escape(client_fails + message),
            "client " + desc,
        )
        node.command_fails_like(
            [
                "pg_basebackup",
                "--pgdata",
                "{}/backup".format(tempdir),
                "--compress",
                "server-" + spec,
            ],
            re.escape(server_fails + message),
            "server " + desc,
        )


def _write_donotcopy_files(pgdata):
    """Write files that should not be copied, plus a non-darwin .DS_Store."""
    for filename in _DONOTCOPY_FILES:
        with open(pgdata / filename, "ab") as fh:
            fh.write(b"DONOTCOPY")
    with open(pgdata / ".DS_Store", "ab") as fh:
        fh.write(b"DONOTCOPY")


def _setup_relations_for_exclusion(node, pgdata):
    """Create unlogged + temp-looking relation files to verify exclusion."""
    # Connect to create global/pg_internal.init (else the not-copied check is a
    # false positive).
    node.safe_psql("SELECT 1;")
    node.safe_psql("CREATE UNLOGGED TABLE base_unlogged (id int)")
    base_unlogged_path = node.safe_psql("select pg_relation_filepath('base_unlogged')")
    assert os.path.isfile(
        "{}/{}_init".format(pgdata, base_unlogged_path)
    ), "unlogged init fork in base"
    assert os.path.isfile(
        "{}/{}".format(pgdata, base_unlogged_path)
    ), "unlogged main fork in base"
    postgres_oid = node.safe_psql(
        "select oid from pg_database where datname = 'postgres'"
    )
    for filename in _TEMP_RELATION_FILES:
        pypg.append_to_file(
            "{}/base/{}/{}".format(pgdata, postgres_oid, filename), "TEMP_RELATION"
        )
    return base_unlogged_path, postgres_oid


def _run_first_backup_and_check_exclusions(
    node, tempdir, pgdata, base_unlogged_path, postgres_oid
):
    """Run the first backup and verify all the exclusion/permission rules."""
    node.command_ok(
        _DEFS + ["--pgdata", "{}/backup".format(tempdir), "--wal-method", "none"],
        "pg_basebackup runs",
    )
    backup = "{}/backup".format(tempdir)
    assert os.path.isfile("{}/PG_VERSION".format(backup)), "backup was created"
    assert os.path.isfile(
        "{}/backup_manifest".format(backup)
    ), "backup manifest included"
    assert pypg.check_mode_recursive(
        backup, 0o700, 0o600
    ), "check backup dir permissions"
    assert sorted(pypg.slurp_dir("{}/pg_wal/".format(backup))) == sorted(
        ["archive_status", "summaries"]
    ), "no WAL files copied"
    for dirname in [
        "pg_dynshmem",
        "pg_notify",
        "pg_replslot",
        "pg_serial",
        "pg_snapshots",
        "pg_stat_tmp",
        "pg_subtrans",
    ]:
        assert (
            sorted(pypg.slurp_dir("{}/{}/".format(backup, dirname))) == []
        ), "contents of {}/ not copied".format(dirname)
    for filename in [
        "postgresql.auto.conf.tmp",
        "postmaster.opts",
        "postmaster.pid",
        "tablespace_map",
        "current_logfiles.tmp",
        "global/pg_internal.init",
        "global/pg_internal.init.123",
    ]:
        assert not os.path.isfile(
            "{}/{}".format(backup, filename)
        ), "{} not copied".format(filename)
    assert not os.path.isfile("{}/.DS_Store".format(backup)), ".DS_Store not copied"
    assert os.path.isfile(
        "{}/{}_init".format(backup, base_unlogged_path)
    ), "unlogged init fork in backup"
    assert not os.path.isfile(
        "{}/{}".format(backup, base_unlogged_path)
    ), "unlogged main fork not in backup"
    for filename in _TEMP_RELATION_FILES:
        assert not os.path.isfile(
            "{}/base/{}/{}".format(backup, postgres_oid, filename)
        ), "base/{}/{} not copied".format(postgres_oid, filename)
    assert (
        pypg.slurp_file("{}/backup_label".format(backup)) != "DONOTCOPY"
    ), "existing backup_label not copied"
    shutil.rmtree(backup)
    # Delete the bogus backup_label so it does not interfere with startup.
    os.unlink(pgdata / "backup_label")


def _waldir_and_tar_format(node, tempdir):
    """Separate xlog dir, tar format, and tablespace-mapping format failures."""
    node.command_ok(
        _DEFS
        + [
            "--pgdata",
            "{}/backup2".format(tempdir),
            "--no-manifest",
            "--waldir",
            "{}/xlog2".format(tempdir),
        ],
        "separate xlog directory",
    )
    assert os.path.isfile("{}/backup2/PG_VERSION".format(tempdir)), "backup was created"
    assert not os.path.isfile(
        "{}/backup2/backup_manifest".format(tempdir)
    ), "manifest was suppressed"
    assert os.path.isdir("{}/xlog2/".format(tempdir)), "xlog directory was created"
    shutil.rmtree("{}/backup2".format(tempdir))
    shutil.rmtree("{}/xlog2".format(tempdir))
    node.command_ok(
        _DEFS + ["--pgdata", "{}/tarbackup".format(tempdir), "--format", "tar"],
        "tar format",
    )
    assert os.path.isfile(
        "{}/tarbackup/base.tar".format(tempdir)
    ), "backup tar was created"
    shutil.rmtree("{}/tarbackup".format(tempdir))
    _tablespace_mapping_format_failures(node, tempdir)


def _tablespace_mapping_format_failures(node, tempdir):
    """All the invalid --tablespace-mapping format errors."""
    cases = [
        ("=/foo", r"invalid tablespace mapping format", "empty old directory"),
        ("/foo=", r"invalid tablespace mapping format", "empty new directory"),
        ("/foo=/bar=/baz", r'multiple "=" signs in tablespace mapping', "multiple ="),
        (
            "foo=/bar",
            r"old directory is not an absolute path in tablespace mapping",
            "old directory not absolute",
        ),
        (
            "/foo=bar",
            r"new directory is not an absolute path in tablespace mapping",
            "new directory not absolute",
        ),
        ("foo", r"invalid tablespace mapping format", "invalid format"),
    ]
    for mapping, pattern, desc in cases:
        node.command_fails_like(
            _DEFS
            + [
                "--pgdata",
                "{}/backup_foo".format(tempdir),
                "--format",
                "plain",
                "--tablespace-mapping",
                mapping,
            ],
            pattern,
            "--tablespace-mapping with {} fails".format(desc),
        )


def _long_name_tar(node, tempdir, pgdata):
    """Tar format cannot store filenames longer than 100 bytes."""
    superlongpath = pgdata / _SUPERLONGNAME
    with open(superlongpath, "w", encoding="utf-8"):
        pass
    node.command_fails(
        _DEFS + ["--pgdata", "{}/tarbackup_l1".format(tempdir), "--format", "tar"],
        "pg_basebackup tar with long name fails",
    )
    os.unlink(superlongpath)


def _setup_symlinks(node, pgdata, tempdir):
    """Move pg_replslot out of pgdata under a symlink; return sys-temp paths."""
    node.stop()
    os.umask(0o027)
    pypg.chmod_recursive(str(pgdata), 0o750, 0o640)
    sys_tempdir = tempfile.mkdtemp(prefix="pgbb_")
    # pg_replslot should be empty; recreate it under sys_tempdir before
    # symlinking to avoid moving things across drives.
    os.rmdir(pgdata / "pg_replslot")
    os.mkdir("{}/pg_replslot".format(sys_tempdir))
    os.symlink("{}/pg_replslot".format(sys_tempdir), pgdata / "pg_replslot")
    node.start()
    real_sys_tempdir = "{}/tempdir".format(sys_tempdir)
    os.symlink(tempdir, real_sys_tempdir)
    return sys_tempdir, real_sys_tempdir


def _tablespace_tar_backup(node, create_pg, tempdir, real_sys_tempdir):
    """Tar-format backup of a tablespace, restored into a replica via tar."""
    os.mkdir("{}/tblspc1".format(tempdir))
    real_ts_dir = "{}/tblspc1".format(real_sys_tempdir)
    node.safe_psql("CREATE TABLESPACE tblspc1 LOCATION '{}';".format(real_ts_dir))
    node.safe_psql(
        "CREATE TABLE test1 (a int) TABLESPACE tblspc1;INSERT INTO test1 VALUES (1234);"
    )
    node.backup("tarbackup2", backup_options=["--format", "tar"])
    node.safe_psql("TRUNCATE TABLE test1;")
    backupdir = "{}/tarbackup2".format(node.backup_dir)
    assert os.path.isfile("{}/base.tar".format(backupdir)), "backup tar was created"
    assert os.path.isfile("{}/pg_wal.tar".format(backupdir)), "WAL tar was created"
    tblspc_tars = glob.glob("{}/[0-9]*.tar".format(backupdir))
    assert len(tblspc_tars) == 1, "one tablespace tar was created"
    tar = os.environ.get("TAR")
    if not tar:
        return
    match = re.search(r"/([0-9]*)\.tar$", tblspc_tars[0])
    assert match is not None
    tblspcoid = match.group(1)
    real_rep_ts_dir = "{}/tblspc1replica".format(real_sys_tempdir)
    node2 = create_pg(
        "replica",
        from_backup=(node, "tarbackup2"),
        tar_program=tar,
        tablespace_map={tblspcoid: real_rep_ts_dir},
        start=False,
    )
    node2.start()
    assert (
        node2.safe_psql("SELECT * FROM test1") == "1234"
    ), "tablespace data restored from tar-format backup"
    node2.stop()


def _tablespace_unlogged_temp_setup(node, pgdata, real_sys_tempdir, postgres_oid):
    """Create unlogged + temp-looking relation files inside the tablespace."""
    node.safe_psql(
        "CREATE UNLOGGED TABLE tblspc1_unlogged (id int) TABLESPACE tblspc1;"
    )
    tblspc1_unlogged_path = node.safe_psql(
        "select pg_relation_filepath('tblspc1_unlogged')"
    )
    assert os.path.isfile(
        "{}/{}_init".format(pgdata, tblspc1_unlogged_path)
    ), "unlogged init fork in tablespace"
    assert os.path.isfile(
        "{}/{}".format(pgdata, tblspc1_unlogged_path)
    ), "unlogged main fork in tablespace"
    test1_path = node.safe_psql("select pg_relation_filepath('test1')")
    tbl_spc1_id = os.path.basename(os.path.dirname(os.path.dirname(test1_path)))
    for filename in ["t888_888", "t888888_888888_vm.1"]:
        pypg.append_to_file(
            "{}/tblspc1/{}/{}/{}".format(
                real_sys_tempdir, tbl_spc1_id, postgres_oid, filename
            ),
            "TEMP_RELATION",
        )
    return tblspc1_unlogged_path, tbl_spc1_id


def _tablespace_plain_backup(node, tempdir, pgdata, real_ts_dir):
    """Plain backup fails without mapping, then succeeds and relocates."""
    node.command_fails(
        _DEFS + ["--pgdata", "{}/backup1".format(tempdir), "--format", "plain"],
        "plain format with tablespaces fails without tablespace mapping",
    )
    node.command_ok(
        _DEFS
        + [
            "--pgdata",
            "{}/backup1".format(tempdir),
            "--format",
            "plain",
            "--tablespace-mapping",
            "{}={}/tbackup/tblspc1".format(real_ts_dir, tempdir),
        ],
        "plain format with tablespaces succeeds with tablespace mapping",
    )
    assert os.path.isdir(
        "{}/tbackup/tblspc1".format(tempdir)
    ), "tablespace was relocated"
    _check_tablespace_symlink(pgdata, tempdir)
    assert pypg.check_mode_recursive(
        "{}/backup1".format(tempdir), 0o750, 0o640
    ), "check backup dir permissions"


def _check_tablespace_symlink(pgdata, tempdir):
    """The relocated tablespace symlink under pg_tblspc points to the new dir."""
    found = False
    for entry in os.listdir(pgdata / "pg_tblspc"):
        link = "{}/backup1/pg_tblspc/{}".format(tempdir, entry)
        if os.path.islink(link) and os.readlink(link) == "{}/tbackup/tblspc1".format(
            tempdir
        ):
            found = True
    assert found, "tablespace symlink was updated"


def _tablespace_exclusion_checks(
    node, tempdir, postgres_oid, real_sys_tempdir, tblspc1_unlogged_path, tbl_spc1_id
):
    """Unlogged/temp forks excluded from the relocated tablespace backup."""
    match = re.search(r"[^/]*/[^/]*/[^/]*$", tblspc1_unlogged_path)
    assert match is not None
    backup_path = match.group(0)
    assert os.path.isfile(
        "{}/tbackup/tblspc1/{}_init".format(tempdir, backup_path)
    ), "unlogged init fork in tablespace backup"
    assert not os.path.isfile(
        "{}/tbackup/tblspc1/{}".format(tempdir, backup_path)
    ), "unlogged main fork not in tablespace backup"
    for filename in ["t888_888", "t888888_888888_vm.1"]:
        assert not os.path.isfile(
            "{}/tbackup/tblspc1/{}/{}/{}".format(
                tempdir, tbl_spc1_id, postgres_oid, filename
            )
        ), "[tblspc1]/{}/{} not copied".format(postgres_oid, filename)
        # Remove temp relation files or tablespace drop will fail.
        os.unlink(
            "{}/tblspc1/{}/{}/{}".format(
                real_sys_tempdir, tbl_spc1_id, postgres_oid, filename
            )
        )
    assert os.path.isdir(
        "{}/backup1/pg_replslot".format(tempdir)
    ), "pg_replslot symlink copied as directory"
    shutil.rmtree("{}/backup1".format(tempdir))


def _tablespace_equals_and_longname(node, tempdir, real_sys_tempdir):
    """Tablespace whose path contains '=' and a very long symlink target."""
    os.mkdir("{}/tbl=spc2".format(tempdir))
    real_ts_dir = "{}/tbl=spc2".format(real_sys_tempdir)
    node.safe_psql("DROP TABLE test1;")
    node.safe_psql("DROP TABLE tblspc1_unlogged;")
    node.safe_psql("DROP TABLESPACE tblspc1;")
    node.safe_psql("CREATE TABLESPACE tblspc2 LOCATION '{}';".format(real_ts_dir))
    escaped = real_ts_dir.replace("=", "\\=")
    node.command_ok(
        _DEFS
        + [
            "--pgdata",
            "{}/backup3".format(tempdir),
            "--format",
            "plain",
            "--tablespace-mapping",
            "{}={}/tbackup/tbl\\=spc2".format(escaped, tempdir),
        ],
        "mapping tablespace with = sign in path",
    )
    assert os.path.isdir(
        "{}/tbackup/tbl=spc2".format(tempdir)
    ), "tablespace with = sign was relocated"
    node.safe_psql("DROP TABLESPACE tblspc2;")
    shutil.rmtree("{}/backup3".format(tempdir))
    os.mkdir("{}/{}".format(tempdir, _SUPERLONGNAME))
    real_ts_dir = "{}/{}".format(real_sys_tempdir, _SUPERLONGNAME)
    node.safe_psql("CREATE TABLESPACE tblspc3 LOCATION '{}';".format(real_ts_dir))
    node.command_ok(
        _DEFS + ["--pgdata", "{}/tarbackup_l3".format(tempdir), "--format", "tar"],
        "pg_basebackup tar with long symlink target",
    )
    node.safe_psql("DROP TABLESPACE tblspc3;")
    shutil.rmtree("{}/tarbackup_l3".format(tempdir))


def _recovery_conf_and_xlog_modes(node, tempdir):
    """--write-recovery-conf, default/fetch/stream/tar WAL modes, --no-slot."""
    node.command_ok(
        _DEFS + ["--pgdata", "{}/backupR".format(tempdir), "--write-recovery-conf"],
        "pg_basebackup --write-recovery-conf runs",
    )
    assert os.path.isfile(
        "{}/backupR/postgresql.auto.conf".format(tempdir)
    ), "postgresql.auto.conf exists"
    assert os.path.isfile(
        "{}/backupR/standby.signal".format(tempdir)
    ), "standby.signal was created"
    recovery_conf = pypg.slurp_file("{}/backupR/postgresql.auto.conf".format(tempdir))
    shutil.rmtree("{}/backupR".format(tempdir))
    assert re.search(
        r"(?m)^primary_conninfo = '.*port={}.*'\n".format(node.port), recovery_conf
    ), "postgresql.auto.conf sets primary_conninfo"
    for sub, flags, msg in [
        ("backupxd", [], "pg_basebackup runs in default xlog mode"),
        (
            "backupxf",
            ["--wal-method", "fetch"],
            "pg_basebackup --wal-method fetch runs",
        ),
        (
            "backupxs",
            ["--wal-method", "stream"],
            "pg_basebackup --wal-method stream runs",
        ),
    ]:
        node.command_ok(_DEFS + ["--pgdata", "{}/{}".format(tempdir, sub)] + flags, msg)
        assert any(
            re.match(r"^[0-9A-F]{24}$", f)
            for f in pypg.slurp_dir("{}/{}/pg_wal".format(tempdir, sub))
        ), "WAL files copied"
        shutil.rmtree("{}/{}".format(tempdir, sub))
    node.command_ok(
        _DEFS
        + [
            "--pgdata",
            "{}/backupxst".format(tempdir),
            "--wal-method",
            "stream",
            "--format",
            "tar",
        ],
        "pg_basebackup --wal-method stream runs in tar mode",
    )
    assert os.path.isfile(
        "{}/backupxst/pg_wal.tar".format(tempdir)
    ), "tar file was created"
    shutil.rmtree("{}/backupxst".format(tempdir))
    node.command_ok(
        _DEFS
        + [
            "--pgdata",
            "{}/backupnoslot".format(tempdir),
            "--wal-method",
            "stream",
            "--no-slot",
        ],
        "pg_basebackup --wal-method stream runs with --no-slot",
    )
    shutil.rmtree("{}/backupnoslot".format(tempdir))
    node.command_ok(
        _DEFS + ["--pgdata", "{}/backupxf".format(tempdir), "--wal-method", "fetch"],
        "pg_basebackup --wal-method fetch runs",
    )


def _backup_target_tests(node, tempdir):
    """--target validation and the blackhole/server targets."""
    node.command_fails_like(
        _DEFS + ["--target", "blackhole"],
        r"WAL cannot be streamed when a backup target is specified",
        "backup target requires --wal-method",
    )
    node.command_fails_like(
        _DEFS + ["--target", "blackhole", "--wal-method", "stream"],
        r"WAL cannot be streamed when a backup target is specified",
        "backup target requires --wal-method other than --wal-method stream",
    )
    node.command_fails_like(
        _DEFS + ["--target", "bogus", "--wal-method", "none"],
        r"unrecognized target",
        "backup target unrecognized",
    )
    node.command_fails_like(
        _DEFS
        + [
            "--target",
            "blackhole",
            "--wal-method",
            "none",
            "--pgdata",
            "{}/blackhole".format(tempdir),
        ],
        r"cannot specify both output directory and backup target",
        "backup target and output directory",
    )
    node.command_fails_like(
        _DEFS + ["--target", "blackhole", "--wal-method", "none", "--format", "tar"],
        r"cannot specify both format and backup target",
        "backup target and format",
    )
    node.command_ok(
        _DEFS + ["--target", "blackhole", "--wal-method", "none"],
        "backup target blackhole",
    )
    node.command_ok(
        _DEFS
        + [
            "--target",
            "server:{}/backuponserver".format(tempdir),
            "--wal-method",
            "none",
        ],
        "backup target server",
    )
    assert os.path.isfile(
        "{}/backuponserver/base.tar".format(tempdir)
    ), "backup tar was created"
    shutil.rmtree("{}/backuponserver".format(tempdir))
    node.command_ok(
        ["createuser", "--replication", "--role=pg_write_server_files", "backupuser"],
        "create backup user",
    )
    node.command_ok(
        _DEFS
        + [
            "--username",
            "backupuser",
            "--target",
            "server:{}/backuponserver".format(tempdir),
            "--wal-method",
            "none",
        ],
        "backup target server",
    )
    assert os.path.isfile(
        "{}/backuponserver/base.tar".format(tempdir)
    ), "backup tar was created as non-superuser"
    shutil.rmtree("{}/backuponserver".format(tempdir))


def _slot_tests(node, tempdir):
    """Slot-related failures, slot creation, restart_lsn advancement."""
    node.command_fails_like(
        _DEFS
        + [
            "--pgdata",
            "{}/backupxs_sl_fail".format(tempdir),
            "--wal-method",
            "stream",
            "--slot",
            "slot0",
        ],
        r'replication slot "slot0" does not exist',
        "pg_basebackup fails with nonexistent replication slot",
    )
    node.command_fails_like(
        _DEFS + ["--pgdata", "{}/backupxs_slot".format(tempdir), "--create-slot"],
        r"--create-slot needs a slot to be specified using --slot",
        "pg_basebackup --create-slot fails without slot name",
    )
    node.command_fails_like(
        _DEFS
        + [
            "--pgdata",
            "{}/backupxs_slot".format(tempdir),
            "--create-slot",
            "--slot",
            "slot0",
            "--no-slot",
        ],
        r"--no-slot cannot be used with slot name",
        "pg_basebackup fails with --create-slot --slot --no-slot",
    )
    node.command_fails_like(
        _DEFS + ["--target", "blackhole", "--pgdata", "{}/blackhole".format(tempdir)],
        r"cannot specify both output directory and backup target",
        "backup target and output directory",
    )
    node.command_ok(
        _DEFS + ["--pgdata", "{}/backuptr/co".format(tempdir), "--wal-method", "none"],
        "pg_basebackup --wal-method fetch runs",
    )
    node.command_ok(
        _DEFS
        + [
            "--pgdata",
            "{}/backupxs_slot".format(tempdir),
            "--create-slot",
            "--slot",
            "slot0",
        ],
        "pg_basebackup --create-slot runs",
    )
    shutil.rmtree("{}/backupxs_slot".format(tempdir))
    assert (
        node.safe_psql(
            "SELECT slot_name FROM pg_replication_slots WHERE slot_name = 'slot0'"
        )
        == "slot0"
    ), "replication slot was created"
    assert (
        node.safe_psql(
            "SELECT restart_lsn FROM pg_replication_slots WHERE slot_name = 'slot0'"
        )
        != ""
    ), "restart LSN of new slot is not null"
    node.command_fails_like(
        _DEFS
        + [
            "--pgdata",
            "{}/backupxs_slot1".format(tempdir),
            "--create-slot",
            "--slot",
            "slot0",
        ],
        r'replication slot "slot0" already exists',
        "pg_basebackup fails with --create-slot --slot and a previously existing slot",
    )
    _slot1_tests(node, tempdir)


def _slot1_tests(node, tempdir):
    """slot1 (physical, no reserve) advances restart_lsn during stream."""
    node.safe_psql("SELECT * FROM pg_create_physical_replication_slot('slot1')")
    lsn = node.safe_psql(
        "SELECT restart_lsn FROM pg_replication_slots WHERE slot_name = 'slot1'"
    )
    assert lsn == "", "restart LSN of new slot is null"
    node.command_fails(
        _DEFS
        + [
            "--pgdata",
            "{}/fail".format(tempdir),
            "--slot",
            "slot1",
            "--wal-method",
            "none",
        ],
        "pg_basebackup with replication slot fails without WAL streaming",
    )
    node.command_ok(
        _DEFS
        + [
            "--pgdata",
            "{}/backupxs_sl".format(tempdir),
            "--wal-method",
            "stream",
            "--slot",
            "slot1",
        ],
        "pg_basebackup --wal-method stream with replication slot runs",
    )
    lsn = node.safe_psql(
        "SELECT restart_lsn FROM pg_replication_slots WHERE slot_name = 'slot1'"
    )
    assert re.match(r"^0/[0-9A-Z]{7,8}$", lsn), "restart LSN of slot has advanced"
    shutil.rmtree("{}/backupxs_sl".format(tempdir))
    node.command_ok(
        _DEFS
        + [
            "--pgdata",
            "{}/backupxs_sl_R".format(tempdir),
            "--wal-method",
            "stream",
            "--slot",
            "slot1",
            "--write-recovery-conf",
        ],
        "pg_basebackup with replication slot and --write-recovery-conf runs",
    )
    assert re.search(
        r"(?m)^primary_slot_name = 'slot1'\n",
        pypg.slurp_file("{}/backupxs_sl_R/postgresql.auto.conf".format(tempdir)),
    ), "recovery conf file sets primary_slot_name"
    assert node.safe_psql("SHOW data_checksums;") == "on", "checksums are enabled"
    shutil.rmtree("{}/backupxs_sl_R".format(tempdir))


def _dbname_recovery_conf(node, tempdir):
    """--dbname is written into the generated recovery conf."""
    node.command_ok(
        _DEFS
        + [
            "--pgdata",
            "{}/backup_dbname_R".format(tempdir),
            "--wal-method",
            "stream",
            "--dbname",
            "dbname=db1",
            "--write-recovery-conf",
        ],
        "pg_basebackup with dbname and --write-recovery-conf runs",
    )
    assert re.search(
        r"(?m)dbname=db1",
        pypg.slurp_file("{}/backup_dbname_R/postgresql.auto.conf".format(tempdir)),
    ), "recovery conf file sets dbname"
    shutil.rmtree("{}/backup_dbname_R".format(tempdir))


def _checksum_corruption_tests(node, tempdir):
    """Checksum-mismatch reporting: 1, capped at 5, total count, and -k bypass."""
    file_corrupt1 = node.safe_psql(
        "CREATE TABLE corrupt1 AS SELECT a FROM generate_series(1,10000) AS a; "
        "ALTER TABLE corrupt1 SET (autovacuum_enabled=false); "
        "SELECT pg_relation_filepath('corrupt1')"
    )
    file_corrupt2 = node.safe_psql(
        "CREATE TABLE corrupt2 AS SELECT b FROM generate_series(1,2) AS b; "
        "ALTER TABLE corrupt2 SET (autovacuum_enabled=false); "
        "SELECT pg_relation_filepath('corrupt2')"
    )
    block_size = int(node.safe_psql("SHOW block_size;"))
    node.stop()
    node.corrupt_page_checksum(file_corrupt1, 0)
    node.start()
    node.command_checks_all(
        _DEFS + ["--pgdata", "{}/backup_corrupt".format(tempdir)],
        1,
        [r"^$"],
        [r"(?s)^WARNING.*checksum verification failed"],
        "pg_basebackup reports checksum mismatch",
    )
    shutil.rmtree("{}/backup_corrupt".format(tempdir))
    node.stop()
    for i in range(1, 6):
        node.corrupt_page_checksum(file_corrupt1, i * block_size)
    node.start()
    node.command_checks_all(
        _DEFS + ["--pgdata", "{}/backup_corrupt2".format(tempdir)],
        1,
        [r"^$"],
        [r"(?s)^WARNING.*further.*failures.*will.not.be.reported"],
        "pg_basebackup does not report more than 5 checksum mismatches",
    )
    shutil.rmtree("{}/backup_corrupt2".format(tempdir))
    node.stop()
    node.corrupt_page_checksum(file_corrupt2, 0)
    node.start()
    node.command_checks_all(
        _DEFS + ["--pgdata", "{}/backup_corrupt3".format(tempdir)],
        1,
        [r"^$"],
        [r"(?s)^WARNING.*7 total checksum verification failures"],
        "pg_basebackup correctly report the total number of checksum mismatches",
    )
    shutil.rmtree("{}/backup_corrupt3".format(tempdir))
    node.command_ok(
        _DEFS
        + ["--pgdata", "{}/backup_corrupt4".format(tempdir), "--no-verify-checksums"],
        "pg_basebackup with -k does not report checksum mismatch",
    )
    shutil.rmtree("{}/backup_corrupt4".format(tempdir))
    node.safe_psql("DROP TABLE corrupt1;")
    node.safe_psql("DROP TABLE corrupt2;")


def _compression_methods(node, tempdir):
    """ZLIB compression: --compress, --gzip, gzip:1, file naming, integrity."""
    if not pypg.check_pg_config(r"#define HAVE_LIBZ 1"):
        return
    node.command_ok(
        _DEFS
        + [
            "--pgdata",
            "{}/backup_gzip".format(tempdir),
            "--compress",
            "1",
            "--format",
            "t",
        ],
        "pg_basebackup with --compress",
    )
    node.command_ok(
        _DEFS
        + ["--pgdata", "{}/backup_gzip2".format(tempdir), "--gzip", "--format", "t"],
        "pg_basebackup with --gzip",
    )
    node.command_ok(
        _DEFS
        + [
            "--pgdata",
            "{}/backup_gzip3".format(tempdir),
            "--compress",
            "gzip:1",
            "--format",
            "t",
        ],
        "pg_basebackup with --compress=gzip:1",
    )
    zlib_files = glob.glob("{}/backup_gzip/*.tar.gz".format(tempdir))
    assert (
        len(zlib_files) == 2
    ), "two files created with --compress=NUM (base.tar.gz and pg_wal.tar.gz)"
    zlib_files2 = glob.glob("{}/backup_gzip2/*.tar.gz".format(tempdir))
    assert (
        len(zlib_files2) == 2
    ), "two files created with --gzip (base.tar.gz and pg_wal.tar.gz)"
    zlib_files3 = glob.glob("{}/backup_gzip3/*.tar.gz".format(tempdir))
    assert (
        len(zlib_files3) == 2
    ), "two files created with --compress=gzip:NUM (base.tar.gz and pg_wal.tar.gz)"
    gzip = os.environ.get("GZIP_PROGRAM")
    if gzip:
        result = node.bin.run_command(
            [gzip, "--test"] + zlib_files + zlib_files2 + zlib_files3
        )
        assert result.rc == 0, "gzip verified the integrity of compressed data"
    shutil.rmtree("{}/backup_gzip".format(tempdir))
    shutil.rmtree("{}/backup_gzip2".format(tempdir))
    shutil.rmtree("{}/backup_gzip3".format(tempdir))


def _sigchld_test(node, tempdir):
    """A killed background stream process makes pg_basebackup exit with an error."""
    node.safe_psql("CREATE TABLE t AS SELECT a FROM generate_series(1,10000) AS a;")
    timeout = pypg.test_timeout_default()
    # pg_basebackup uses PGAPPNAME as its fallback application_name, which the
    # walsender then reports in pg_stat_activity (mirrors the Perl harness
    # setting PGAPPNAME to the test file's basename).
    appname = "test_010_pg_basebackup.py"
    cmd = [
        str(node.bin_dir / "pg_basebackup"),
        "--no-sync",
        "-cfast",
        "--wal-method=stream",
        "--pgdata",
        "{}/sigchld".format(tempdir),
        "--max-rate",
        "32",
        "--dbname",
        node.connstr("postgres"),
    ]
    env = dict(node.connenv)
    env["PGAPPNAME"] = appname
    bg = _BgCommand(cmd, env, timeout)
    try:
        assert node.poll_query_until(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE "
            "application_name = '{}' AND wait_event = 'WalSenderMain' "
            "AND backend_type = 'walsender' AND query ~ 'START_REPLICATION'".format(
                appname
            )
        ), "Walsender killed"
        assert bg.pump_until(
            r"background process terminated unexpectedly"
        ), "background process exit message"
    finally:
        bg.finish()


def _in_place_tablespace_backup(node):
    """Back up a cluster containing an in-place tablespace."""
    node.safe_psql(
        "SET allow_in_place_tablespaces = on; CREATE TABLESPACE tblspc2 LOCATION '';"
    )
    node.safe_psql(
        "CREATE TABLE test2 (a int) TABLESPACE tblspc2;INSERT INTO test2 VALUES (1234);"
    )
    tblspc_oid = node.safe_psql(
        "SELECT oid FROM pg_tablespace WHERE spcname = 'tblspc2';"
    )
    node.backup("backup3")
    node.safe_psql("DROP TABLE test2;")
    node.safe_psql("DROP TABLESPACE tblspc2;")
    backupdir = "{}/backup3".format(node.backup_dir)
    dst_tblspc = glob.glob("{}/pg_tblspc/{}/PG_*".format(backupdir, tblspc_oid))
    assert len(dst_tblspc) == 1, "tblspc directory copied"
    return backupdir


def _different_sysid_manifest(node, create_pg, tempdir, backupdir):
    """Incremental backup against a manifest from a different cluster fails."""
    node2 = create_pg(
        "node2",
        force_initdb=True,
        has_archiving=True,
        allows_streaming=True,
        start=False,
    )
    node2.append_conf("summarize_wal = on")
    node2.start()
    node2.command_fails_like(
        _DEFS
        + [
            "--pgdata",
            "{}/diff_sysid".format(tempdir),
            "--incremental",
            "{}/backup_manifest".format(backupdir),
        ],
        r"system identifier in backup manifest is .*, but database system identifier is",
        "pg_basebackup fails with different database system manifest",
    )


def test_010_pg_basebackup(create_pg, pg_bin, tmp_path):
    """End-to-end pg_basebackup coverage mirroring 010_pg_basebackup.pl."""
    pg_bin.program_help_ok("pg_basebackup")
    pg_bin.program_version_ok("pg_basebackup")
    pg_bin.program_options_handling_ok("pg_basebackup")
    os.umask(0o077)
    tempdir = str(tmp_path / "tempdir")
    os.mkdir(tempdir)
    os.mkdir("{}/pgdata".format(tempdir))
    node = create_pg(
        "main",
        extra=["--data-checksums"],
        auth_extra=["--create-role", "backupuser"],
        start=False,
    )
    # Mirror Cluster->init without allows_streaming: a non-streaming primary is
    # configured with minimal WAL so the first pg_basebackup attempt fails for
    # WAL-configuration reasons (the test enables replication later).
    node.append_conf("wal_level = minimal\nmax_wal_senders = 0\n")
    node.start()
    pgdata = node.datadir

    _option_and_wal_config_failures(node, tempdir, pgdata)
    _compression_failure_tests(node, tempdir)
    _write_donotcopy_files(pgdata)
    base_unlogged_path, postgres_oid = _setup_relations_for_exclusion(node, pgdata)
    _run_first_backup_and_check_exclusions(
        node, tempdir, pgdata, base_unlogged_path, postgres_oid
    )
    _waldir_and_tar_format(node, tempdir)
    _long_name_tar(node, tempdir, pgdata)

    _sys_tempdir, real_sys_tempdir = _setup_symlinks(node, pgdata, tempdir)
    _tablespace_tar_backup(node, create_pg, tempdir, real_sys_tempdir)
    real_ts_dir = "{}/tblspc1".format(real_sys_tempdir)
    tblspc1_unlogged_path, tbl_spc1_id = _tablespace_unlogged_temp_setup(
        node, pgdata, real_sys_tempdir, postgres_oid
    )
    _tablespace_plain_backup(node, tempdir, pgdata, real_ts_dir)
    _tablespace_exclusion_checks(
        node,
        tempdir,
        postgres_oid,
        real_sys_tempdir,
        tblspc1_unlogged_path,
        tbl_spc1_id,
    )
    _tablespace_equals_and_longname(node, tempdir, real_sys_tempdir)

    _recovery_conf_and_xlog_modes(node, tempdir)
    _backup_target_tests(node, tempdir)
    _slot_tests(node, tempdir)
    _dbname_recovery_conf(node, tempdir)
    _checksum_corruption_tests(node, tempdir)
    _compression_methods(node, tempdir)
    _sigchld_test(node, tempdir)
    backupdir = _in_place_tablespace_backup(node)
    _different_sysid_manifest(node, create_pg, tempdir, backupdir)
