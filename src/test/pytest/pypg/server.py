# Copyright (c) 2025, PostgreSQL Global Development Group

import contextlib
import os
import pathlib
import platform
import re
import shlex
import shutil
import socket
import stat
import subprocess
import tempfile
import time
from collections import namedtuple
from typing import Callable, Dict, Optional, Tuple

from ._env import test_timeout_default
from .command import PgBin, ProgramResult
from .bgpsql import BackgroundPsql
from .errors import PgServerError, PgSqlError
from .sqlresult import SqlResult
from .interactive import InteractivePsql
from .util import append_to_file, eprint, run, slurp_file
from libpq import PGconn, connect as libpq_connect, ExecStatus


class FileBackup(contextlib.AbstractContextManager):
    """
    A context manager which backs up a file's contents, restoring them on exit.
    """

    def __init__(self, file: pathlib.Path):
        super().__init__()

        self._file = file
        self._backup: Optional[pathlib.Path] = None

    def __enter__(self):
        with tempfile.NamedTemporaryFile(
            prefix=self._file.name, dir=self._file.parent, delete=False
        ) as f:
            self._backup = pathlib.Path(f.name)

        shutil.copyfile(self._file, self._backup)

        return self

    def __exit__(self, *exc):
        # Swap the backup and the original file, so that the modified contents
        # can still be inspected in case of failure.
        assert self._backup is not None  # set by __enter__
        tmp = self._backup.parent / (self._backup.name + ".tmp")

        shutil.copyfile(self._file, tmp)
        shutil.copyfile(self._backup, self._file)
        shutil.move(tmp, self._backup)


class HBA(FileBackup):
    """
    Backs up a server's HBA configuration and provides means for temporarily
    editing it.
    """

    def __init__(self, datadir: pathlib.Path):
        super().__init__(datadir / "pg_hba.conf")

    def prepend(self, *lines):
        """
        Temporarily prepends lines to the server's pg_hba.conf.

        As sugar for aligning HBA columns in the tests, each line can be either
        a string or a list of strings. List elements will be joined by single
        spaces before they are written to file.
        """
        with open(self._file, "r", encoding="utf-8") as f:
            prior_data = f.read()

        with open(self._file, "w", encoding="utf-8") as f:
            for line in lines:
                if isinstance(line, list):
                    print(*line, file=f)
                else:
                    print(line, file=f)

            f.write(prior_data)


class Config(FileBackup):
    """
    Backs up a server's postgresql.conf and provides means for temporarily
    editing it.
    """

    def __init__(self, datadir: pathlib.Path):
        super().__init__(datadir / "postgresql.conf")

    def set(self, **gucs):
        """
        Temporarily appends GUC settings to the server's postgresql.conf.
        """

        with open(self._file, "a", encoding="utf-8") as f:
            print(file=f)

            for n, v in gucs.items():
                v = str(v)

                # Quote and escape the value for postgresql.conf single-quoted
                # strings. This is doing the reversee of DeescapeQuotedString.
                v = v.replace("\\", "\\\\")
                v = v.replace("'", "''")
                v = v.replace("\n", "\\n")
                v = v.replace("\r", "\\r")
                v = v.replace("\t", "\\t")
                v = v.replace("\b", "\\b")
                v = v.replace("\f", "\\f")
                v = "'{}'".format(v)

                print(n, "=", v, file=f)


Backup = namedtuple("Backup", "conf, hba")

WINDOWS_OS = platform.system() == "Windows"

# psql, with the verbose-error settings the framework uses, prefixes the SQLSTATE
# line as e.g. "psql:...: ERROR:  ..." plus optional DETAIL/HINT lines.
_PSQL_PRIMARY_RE = re.compile(
    r"^(?:psql:[^:]*:\d+: )?(?:ERROR|FATAL|PANIC):\s+(.*)$", re.M
)
_PSQL_FIELD_RES = {
    "detail": re.compile(r"^DETAIL:\s+(.*)$", re.M),
    "hint": re.compile(r"^HINT:\s+(.*)$", re.M),
    "context": re.compile(r"^CONTEXT:\s+(.*)$", re.M),
}


def _parse_psql_diagnostics(stderr) -> Dict[str, str]:
    """Extract the diagnostic fields psql prints into PgSqlError kwargs.

    psql's text protocol does not surface the SQLSTATE, so sqlstate stays None;
    the primary message and any DETAIL/HINT/CONTEXT lines are recovered so the
    error object is still introspectable.
    """
    fields: Dict[str, str] = {}
    match = _PSQL_PRIMARY_RE.search(stderr or "")
    if match:
        fields["primary"] = match.group(1).strip()
    for name, regex in _PSQL_FIELD_RES.items():
        found = regex.search(stderr or "")
        if found:
            fields[name] = found.group(1).strip()
    return fields


def _psql_error_message(query, stderr):
    """Build a PgSqlError message from a failed psql invocation."""
    text = (stderr or "").strip()
    return "SQL failed: {}\nquery was: {}".format(text, query)


class PostgresServer:
    """
    Represents a running PostgreSQL server instance with management utilities.
    Provides methods for configuration, user/database creation, and server control.
    """

    def __init__(
        self,
        name,
        bindir,
        datadir,
        sockdir,
        libpq_handle,
        *,
        hostaddr: Optional[str] = None,
        port: Optional[int] = None,
        allows_streaming: bool = False,
        from_backup: Optional[Tuple["PostgresServer", str]] = None,
        has_streaming: bool = False,
        has_restoring: bool = False,
        standby: bool = True,
        has_archiving: bool = False,
        extra: Optional[list] = None,
        combine_with_prior: Optional[list] = None,
        combine_mode: Optional[str] = None,
        auth_extra: Optional[list] = None,
        no_data_checksums: bool = False,
        force_initdb: bool = False,
        tablespace_map: Optional[dict] = None,
        tar_program: Optional[str] = None,
    ):
        """
        Initialize a PostgreSQL server instance. Call start() to actually
        start the server.

        Args:
            name: The name of this server instance (for logging purposes)
            bindir: Path to PostgreSQL bin directory
            datadir: Path to data directory for this server
            sockdir: Path to directory for Unix sockets
            libpq_handle: ctypes handle to libpq
            hostaddr: If provided, use this specific address (e.g., "127.0.0.2")
            port: If provided, use this port instead of finding a free one,
                is currently only allowed if hostaddr is also provided
            allows_streaming: Configure the server as a streaming-replication
                primary (wal_level, max_wal_senders, etc.), mirroring
                PostgreSQL::Test::Cluster->init(allows_streaming => 1).
            from_backup: (source_server, backup_name) to initialize the data
                directory from a base backup instead of running initdb,
                mirroring init_from_backup().
            has_streaming: When initializing from a backup, configure this
                server as a streaming standby of the backup's source server.
        """

        if hostaddr is None and port is not None:
            raise NotImplementedError("port was provided without hostaddr")
        if has_streaming and from_backup is None:
            raise ValueError("has_streaming requires from_backup")
        if has_restoring and from_backup is None:
            raise ValueError("has_restoring requires from_backup")

        self.name = name
        self.datadir = datadir
        self.sockdir = sockdir
        self.libpq_handle = libpq_handle
        self._remaining_timeout_fn: Optional[Callable[[], float]] = None
        self._bindir = bindir
        self._pg_ctl = bindir / "pg_ctl"
        self.log = datadir / "postgresql.log"
        self._log_start_pos = 0
        self._logfile_generation = 0
        self.pid: Optional[int] = None
        self._backup_root = pathlib.Path(datadir).parent / (str(name) + "_backups")

        # ExitStack for cleanup callbacks
        self._cleanup_stack = contextlib.ExitStack()

        # Determine whether to use Unix sockets
        use_unix_sockets = platform.system() != "Windows" and hostaddr is None

        # Initialize the data directory: from a base backup, an initdb template
        # (much faster), or a fresh initdb.
        if no_data_checksums:
            extra = (extra or []) + ["--no-data-checksums"]
        self._init_datadir(
            from_backup,
            extra,
            combine_with_prior,
            combine_mode,
            force_initdb,
            tablespace_map,
            tar_program,
        )
        if from_backup is None and auth_extra:
            self._config_auth(auth_extra)

        # Figure out a port to listen on. Attempt to reserve both IPv4 and IPv6
        # addresses in one go.
        #
        # Note: socket.has_dualstack_ipv6/create_server are only in Python 3.8+.
        if hostaddr is not None:
            # Explicit address provided
            addrs: list[str] = [hostaddr]
            temp_sock = socket.socket()
            if port is None:
                temp_sock.bind((hostaddr, 0))
                _, port = temp_sock.getsockname()

        elif hasattr(socket, "has_dualstack_ipv6") and socket.has_dualstack_ipv6():
            addr = ("::1", 0)
            temp_sock = socket.create_server(
                addr, family=socket.AF_INET6, dualstack_ipv6=True
            )

            hostaddr, port, _, _ = temp_sock.getsockname()
            assert hostaddr is not None
            addrs = [hostaddr, "127.0.0.1"]

        else:
            addr = ("127.0.0.1", 0)

            temp_sock = socket.socket()
            temp_sock.bind(addr)

            hostaddr, port = temp_sock.getsockname()
            assert hostaddr is not None
            addrs = [hostaddr]

        # Store the computed values
        self.hostaddr = hostaddr
        self.port = port
        # Including the host to use for connections - either the socket
        # directory or TCP address
        if use_unix_sockets:
            self.host = str(sockdir)
        else:
            self.host = hostaddr

        self._write_base_config(
            use_unix_sockets, addrs, port, allows_streaming, from_backup
        )

        if has_archiving:
            self._enable_archiving()

        # Between closing of the socket, s, and server start, we're racing
        # against anything that wants to open up ephemeral ports, so try not to
        # put any new work here.

        temp_sock.close()

        # Initializing from a backup: optionally turn this into a streaming
        # standby of the backup's source server, and/or a restoring standby
        # that fetches WAL from the source's archive.
        if has_streaming:
            assert from_backup is not None  # guaranteed by the check above
            self._enable_streaming(from_backup[0])
        if has_restoring:
            assert from_backup is not None  # guaranteed by the check above
            self._enable_restoring(from_backup[0], standby)

    def _write_base_config(
        self, use_unix_sockets, addrs, port, allows_streaming, from_backup=None
    ):
        """Append the test server's base configuration to postgresql.conf.

        For a node initialized from a backup only the connection-identity
        settings (socket/listen/port) are written: the policy settings
        (logging, fsync, restart_after_crash, ...) are inherited from the backup
        and may have been intentionally overridden on the source, mirroring
        PostgreSQL::Test::Cluster->init_from_backup (which rewrites only port and
        listen_addresses/unix_socket_directories).
        """
        with open(self.datadir / "postgresql.conf", "a", encoding="utf-8") as f:
            print(file=f)
            if use_unix_sockets:
                print(
                    "unix_socket_directories = '{}'".format(self.sockdir.as_posix()),
                    file=f,
                )
            else:
                # Disable Unix sockets when using TCP to avoid lock conflicts
                print("unix_socket_directories = ''", file=f)
            print("listen_addresses = '{}'".format(",".join(addrs)), file=f)
            print("port =", port, file=f)
            if from_backup is not None:
                return
            print("log_connections = all", file=f)
            print("fsync = off", file=f)
            print("datestyle = 'ISO'", file=f)
            print("timezone = 'UTC'", file=f)

            # Logging settings mirroring PostgreSQL::Test::Cluster->init, so
            # that statement-log assertions (issues_sql_like) and replication
            # behave the same as in the Perl suite.
            print("log_statement = all", file=f)
            print("log_replication_commands = on", file=f)
            print("log_line_prefix = '%m %b[%p] %q%a '", file=f)
            print("restart_after_crash = off", file=f)
            print("wal_retrieve_retry_interval = '500ms'", file=f)

            if allows_streaming:
                wal_level = "logical" if allows_streaming == "logical" else "replica"
                print("wal_level = {}".format(wal_level), file=f)
                print("max_wal_senders = 10", file=f)
                print("max_replication_slots = 10", file=f)
                print("wal_log_hints = on", file=f)
                print("hot_standby = on", file=f)
                print("max_wal_size = 128MB", file=f)

    def _init_datadir(
        self,
        from_backup,
        extra=None,
        combine_with_prior=None,
        combine_mode=None,
        force_initdb=False,
        tablespace_map=None,
        tar_program=None,
    ):
        """Populate the data directory from a backup, a template, or initdb.

        When extra initdb options are given, a fresh initdb is always run
        (the cached template may be incompatible), mirroring the force_initdb
        behavior of PostgreSQL::Test::Cluster->init.
        """
        if from_backup is not None:
            source, backup_name = from_backup
            if combine_with_prior:
                # Reconstruct a full data directory from a chain of prior
                # (full/incremental) backups plus this one, via
                # pg_combinebackup (mirrors init_from_backup combine_with_prior).
                inputs = [
                    str(source.backup_path(prior)) for prior in combine_with_prior
                ]
                inputs.append(str(source.backup_path(backup_name)))
                extra_combine = [combine_mode] if combine_mode else []
                ts_args = [
                    "-T{}={}".format(old, new)
                    for old, new in (tablespace_map or {}).items()
                ]
                run(
                    self._bindir / "pg_combinebackup",
                    *inputs,
                    *ts_args,
                    *extra_combine,
                    "-o",
                    self.datadir,
                )
            elif tar_program:
                self._restore_tar_backup(
                    source.backup_path(backup_name), tar_program, tablespace_map
                )
            elif tablespace_map:
                self._copy_backup_with_tablespaces(
                    source.backup_path(backup_name), tablespace_map
                )
            else:
                shutil.copytree(source.backup_path(backup_name), self.datadir)
            # A backup carries the source's postmaster.pid/standby state; remove
            # anything that would confuse a fresh start.
            for leftover in ("postmaster.pid", "standby.signal", "recovery.signal"):
                (self.datadir / leftover).unlink(missing_ok=True)
            return

        initdb_template = os.environ.get("INITDB_TEMPLATE")
        if (
            initdb_template
            and os.path.isdir(initdb_template)
            and not extra
            and not force_initdb
        ):
            shutil.copytree(initdb_template, self.datadir)
        else:
            # Match Cluster.pm and the initdb template: trust auth for local
            # connections (the template-copy path above is already trust).
            run(
                self._bindir / "initdb",
                "--no-sync",
                "--auth",
                "trust",
                "--pgdata",
                self.datadir,
                *(extra or []),
            )

    def _copy_backup_with_tablespaces(self, backup_path, tablespace_map):
        """Copy a base backup, relocating mapped tablespaces and writing
        tablespace_map (mirrors Cluster->init_from_backup's plain-copy path).

        tablespace_map maps a tablespace OID (the pg_tblspc/<oid> entry) to the
        new directory the tablespace should live in. Mapped tablespace links are
        skipped during the main copy, copied to their new homes, and recorded in
        the data directory's tablespace_map file.
        """
        backup_path = pathlib.Path(backup_path)
        seen_tsoids = []

        def _ignore(directory, names):
            ignored = []
            rel = pathlib.Path(directory).relative_to(backup_path)
            if str(rel) == "pg_tblspc":
                for name in names:
                    if name in tablespace_map:
                        seen_tsoids.append(name)
                        ignored.append(name)
            return ignored

        shutil.copytree(backup_path, self.datadir, ignore=_ignore)
        if not seen_tsoids:
            return
        with open(self.datadir / "tablespace_map", "w", encoding="utf-8") as tsmap:
            for tsoid in seen_tsoids:
                olddir = backup_path / "pg_tblspc" / tsoid
                newdir = tablespace_map[tsoid]
                shutil.copytree(olddir, newdir)
                tsmap.write("{} {}\n".format(tsoid, newdir))

    def _restore_tar_backup(self, backup_path, tar_program, tablespace_map):
        """Restore a tar-format base backup into the data directory.

        Mirrors PostgreSQL::Test::Cluster->init_from_backup's tar_program path:
        extract base.tar into the data dir and pg_wal.tar into pg_wal, then
        extract each numbered tablespace tar into its mapped directory and
        record it in the data directory's tablespace_map file. tablespace_map
        maps a tablespace OID (the tar's base name) to the directory it should
        be restored into.
        """
        backup_path = pathlib.Path(backup_path)
        tablespace_map = tablespace_map or {}
        self.datadir.mkdir(parents=True)
        run(tar_program, "xf", backup_path / "base.tar", "-C", self.datadir)
        run(
            tar_program,
            "xf",
            backup_path / "pg_wal.tar",
            "-C",
            self.datadir / "pg_wal",
        )
        tstars = sorted(
            name for name in os.listdir(backup_path) if re.match(r"^\d+\.tar", name)
        )
        with open(self.datadir / "tablespace_map", "w", encoding="utf-8") as tsmap:
            for tstar in tstars:
                tsoid = re.sub(r"\.tar$", "", tstar)
                if tsoid not in tablespace_map:
                    raise PgServerError("no tablespace mapping for {}".format(tstar))
                newdir = tablespace_map[tsoid]
                os.mkdir(newdir)
                run(tar_program, "xf", backup_path / tstar, "-C", newdir)
                escaped_newdir = str(newdir).replace("\\", "\\\\")
                tsmap.write("{} {}\n".format(tsoid, escaped_newdir))

    def _config_auth(self, auth_extra):
        """Run pg_regress --config-auth on the data dir (mirrors init auth_extra).

        Sets up authentication (e.g. extra ident-mapped roles) so the test's OS
        user can connect as those roles. Requires PG_REGRESS in the environment.
        """
        pg_regress = os.environ.get("PG_REGRESS")
        if not pg_regress:
            return
        run(
            pg_regress,
            "--config-auth",
            self.datadir,
            *(str(opt) for opt in auth_extra),
        )

    def start(self, fail_ok=False):
        """Start the server using pg_ctl. Returns True on success; with
        fail_ok, returns False instead of raising if pg_ctl reports failure."""
        # Set cluster_name at startup (not in postgresql.conf) so it is not
        # copied to standbys via backup, mirroring Cluster->start. walreceiver
        # uses cluster_name as its application_name in pg_stat_replication.
        try:
            self.pg_ctl("--options", "--cluster-name={}".format(self.name), "start")
        except subprocess.CalledProcessError as exc:
            if fail_ok:
                return False
            # pg_ctl's own output rarely says why startup failed; include the
            # server log, which holds the actual startup error.
            raise PgServerError(
                'pg_ctl start failed for node "{}":\n--- {} ---\n{}'.format(
                    self.name, self.log, self._log_text()
                )
            ) from exc
        # Read the PID file to get the postmaster PID
        with open(os.path.join(self.datadir, "postmaster.pid"), encoding="utf-8") as f:
            self.pid = int(f.readline().strip())
        return True

    def _log_text(self):
        """Return the whole server log as text, normalizing CRLF/CR to LF.

        Log offsets are character positions in this normalized text (see
        current_log_position), not raw byte counts: on Windows the log uses CRLF
        line endings, and a byte offset would overshoot the folded text and skip
        past the lines being checked.

        Co-authored-by: Andrew Dunstan <andrew@dunslane.net>
        """
        if not self.log.exists():
            return ""
        with open(self.log, encoding="utf-8", errors="replace") as fh:
            text = fh.read()
        return text.replace("\r\n", "\n").replace("\r", "\n")

    def current_log_position(self):
        """Get the current end position of the log, as a character offset.

        Character length of the CRLF-normalized log text (not the raw byte
        size), so it slices log text consistently on Windows.
        """
        return len(self._log_text())

    def is_alive(self):
        """Return True if the server answers pg_isready (mirrors Cluster->is_alive)."""
        result = self.bin.run_command(
            ["pg_isready", "--host", str(self.host), "--port", str(self.port)]
        )
        return result.rc == 0

    def advance_wal(self, num):
        """Advance the WAL by num segments (cf. Cluster->advance_wal).

        Each iteration emits a logical message and forces a WAL switch, which
        flushes WAL and moves to a fresh segment.
        """
        for _ in range(num):
            self.safe_psql(
                "SELECT pg_logical_emit_message(false, '', 'foo');\n"
                "SELECT pg_switch_wal();"
            )

    def emit_wal(self, size):
        """Emit a logical message of size bytes; return its end LSN as an int.

        Mirrors PostgreSQL::Test::Cluster->emit_wal.
        """
        return int(
            self.safe_psql(
                "SELECT pg_logical_emit_message(true, '', repeat('a', {})) "
                "- '0/0'".format(size)
            )
        )

    def _get_insert_lsn(self):
        """Current WAL insert LSN as an int offset from 0/0."""
        return int(self.safe_psql("SELECT pg_current_wal_insert_lsn() - '0/0'"))

    def advance_wal_out_of_record_splitting_zone(self, wal_block_size):
        """Emit WAL until the insert LSN is clear of the page-boundary zone.

        Mirrors Cluster->advance_wal_out_of_record_splitting_zone: keeps the
        insert pointer at least a quarter-page away from the end of the current
        WAL page so a following record will not be split across pages.
        """
        page_threshold = wal_block_size // 4
        end_lsn = self._get_insert_lsn()
        page_offset = end_lsn % wal_block_size
        while page_offset >= wal_block_size - page_threshold:
            self.emit_wal(page_threshold)
            end_lsn = self._get_insert_lsn()
            page_offset = end_lsn % wal_block_size
        return end_lsn

    def advance_wal_to_record_splitting_zone(self, wal_block_size):
        """Emit WAL until the insert LSN is near a page boundary.

        Mirrors Cluster->advance_wal_to_record_splitting_zone: positions the
        insert pointer within a record-header's width of the page end, so a
        following record header straddles the page boundary.
        """
        record_header_size = 24
        end_lsn = self._get_insert_lsn()
        page_offset = end_lsn % wal_block_size
        while page_offset <= wal_block_size - 512:
            self.emit_wal(wal_block_size - page_offset - 256)
            end_lsn = self._get_insert_lsn()
            page_offset = end_lsn % wal_block_size
        message_size = wal_block_size - 80
        while page_offset <= wal_block_size - record_header_size:
            self.emit_wal(message_size)
            end_lsn = self._get_insert_lsn()
            old_offset = page_offset
            page_offset = end_lsn % wal_block_size
            delta = page_offset - old_offset
            if delta > 8:
                message_size -= 8
            elif delta <= 0:
                message_size += 8
        return end_lsn

    def write_wal(self, tli, lsn, segment_size, data):
        """Overwrite bytes at lsn in the WAL segment file; return its path.

        Mirrors Cluster->write_wal: locates the segment containing lsn, seeks to
        the in-segment offset, and writes data there (raw bytes).
        """
        segment = lsn // segment_size
        offset = lsn % segment_size
        path = self.datadir / "pg_wal" / "{:08X}{:08X}{:08X}".format(tli, 0, segment)
        with open(path, "r+b") as fh:
            fh.seek(offset)
            fh.write(data)
        return str(path)

    def dump_info(self):
        """Print basic node info for debugging (cf. Cluster->dump_info)."""
        eprint(
            "# Node {!r}: host={} port={} datadir={}".format(
                self.name, self.host, self.port, self.datadir
            )
        )

    def slot(self, slot_name):
        """Return a dict of this slot's pg_replication_slots fields.

        Mirrors PostgreSQL::Test::Cluster->slot: an unknown slot yields
        empty-string values for every column.
        """
        columns = [
            "plugin",
            "slot_type",
            "datoid",
            "database",
            "active",
            "active_pid",
            "xmin",
            "catalog_xmin",
            "restart_lsn",
        ]
        row = self.safe_psql(
            "SELECT {} FROM pg_catalog.pg_replication_slots "
            "WHERE slot_name = '{}'".format(", ".join(columns), slot_name)
        )
        values = row.split("|") if row != "" else [""] * len(columns)
        return dict(zip(columns, values))

    def pg_recvlogical_upto(
        self, dbname, slot_name, endpos, timeout_secs, options=None
    ):
        """Stream a logical slot up to endpos and return pg_recvlogical's stdout.

        Mirrors PostgreSQL::Test::Cluster->pg_recvlogical_upto (scalar context):
        runs pg_recvlogical --no-loop --start to the given end LSN, applying any
        plugin options (a name->value dict), raising on a non-zero exit.
        """
        cmd = [
            str(self._bindir / "pg_recvlogical"),
            "--slot",
            slot_name,
            "--dbname",
            self.connstr(dbname),
            "--endpos",
            str(endpos),
            "--file",
            "-",
            "--no-loop",
            "--start",
        ]
        for key, value in (options or {}).items():
            if "=" in key:
                raise ValueError("= not permitted in replication option name")
            cmd += ["--option", "{}={}".format(key, value)]
        proc = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            encoding="utf-8",
            errors="replace",
            env=self._connenv(),
            timeout=timeout_secs,
            check=False,
        )
        if proc.returncode != 0:
            raise PgServerError(
                "pg_recvlogical exited with {}, stdout {!r} stderr {!r}".format(
                    proc.returncode, proc.stdout, proc.stderr
                )
            )
        return proc.stdout

    def wait_for_slot_catchup(self, slot_name, mode="restart", target_lsn=None):
        """Wait until slot_name's <mode>_lsn passes target_lsn.

        Mirrors Cluster->wait_for_slot_catchup. mode is 'restart' or
        'confirmed_flush'.
        """
        if mode not in ("restart", "confirmed_flush"):
            raise ValueError("valid modes are restart, confirmed_flush")
        if target_lsn is None:
            raise ValueError("target lsn must be specified")
        assert self.poll_query_until(
            "SELECT '{}' <= {}_lsn FROM pg_catalog.pg_replication_slots "
            "WHERE slot_name = '{}';".format(target_lsn, mode, slot_name)
        ), "timed out waiting for catchup"

    def validate_slot_inactive_since(self, slot_name, reference_time):
        """Return slot_name's inactive_since after sanity-checking it.

        Mirrors Cluster->validate_slot_inactive_since: the captured
        inactive_since must be later than the epoch and than reference_time.
        """
        inactive_since = self.safe_psql(
            "SELECT inactive_since FROM pg_replication_slots\n"
            "    WHERE slot_name = '{}' AND inactive_since IS NOT NULL;".format(
                slot_name
            )
        )
        assert (
            self.safe_psql(
                "SELECT '{since}'::timestamptz > to_timestamp(0) AND\n"
                "    '{since}'::timestamptz > '{ref}'::timestamptz;".format(
                    since=inactive_since, ref=reference_time
                )
            )
            == "t"
        ), "last inactive time for slot {} is valid".format(slot_name)
        return inactive_since

    def log_standby_snapshot(self, standby, slot_name):
        """Emit an xl_running_xacts record the standby's logical slot waits for.

        Mirrors Cluster->log_standby_snapshot: wait until the standby slot's
        restart_lsn is determined, then call pg_log_standby_snapshot() on self
        (the primary) so the standby can advance the slot.
        """
        assert standby.poll_query_until(
            "SELECT restart_lsn IS NOT NULL\n"
            "FROM pg_catalog.pg_replication_slots WHERE slot_name = '{}'".format(
                slot_name
            )
        ), "timed out waiting for logical slot to calculate its restart_lsn"
        self.safe_psql("SELECT pg_log_standby_snapshot()")

    def create_logical_slot_on_standby(self, primary, slot_name, dbname):
        """Create a logical slot on this standby, coordinated with primary.

        Mirrors Cluster->create_logical_slot_on_standby: starts pg_recvlogical
        --create-slot in the background, has primary log a standby snapshot so
        the slot can compute its restart_lsn, then verifies the slot is logical.
        """
        proc = subprocess.Popen(  # pylint: disable=consider-using-with
            [
                str(self._bindir / "pg_recvlogical"),
                "--dbname",
                self.connstr(dbname),
                "--plugin",
                "test_decoding",
                "--slot",
                slot_name,
                "--create-slot",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=self._connenv(),
        )
        primary.log_standby_snapshot(self, slot_name)
        proc.wait()
        assert (
            self.slot(slot_name)["slot_type"] == "logical"
        ), "{} on standby created".format(slot_name)

    def reset_log_position(self):
        """Mark current log position as start for log_content()."""
        self._log_start_pos = self.current_log_position()

    @contextlib.contextmanager
    def start_new_test(self, remaining_timeout):
        """
        Prepare server for a new test.

        Sets timeout, resets log position, and enters a cleanup subcontext.
        """
        self.set_timeout(remaining_timeout)
        self.reset_log_position()
        with self.subcontext():
            yield self

    def psql(self, *args):
        """Run psql with the given arguments."""
        self._run(os.path.join(self._bindir, "psql"), "-w", *args)

    def psql_capture(
        self,
        query,
        dbname="postgres",
        on_error_stop=True,
        replication=None,
        extra_params=None,
        connstr=None,
        timeout=None,
    ):
        """
        Run psql with query piped on stdin and return ProgramResult(rc, stdout,
        stderr) without raising. Mirrors PostgreSQL::Test::Cluster->psql in list
        context: --no-psqlrc --no-align --tuples-only --quiet, ON_ERROR_STOP by
        default (a SQL error then yields exit code 3), with an optional
        replication connection. extra_params are appended to the psql command
        line (e.g. ['--username', 'someuser']). A connstr overrides the --dbname
        target (libpq merges it with PGHOST/PGPORT from the environment). Use it
        to assert on psql's own stdout/stderr/exit code.
        """
        if connstr is None:
            connstr = self.dbname_connstr(dbname)
        if replication is not None:
            connstr += " replication={}".format(replication)
        cmd = [
            str(self._bindir / "psql"),
            "--no-psqlrc",
            "--no-align",
            "--tuples-only",
            "--quiet",
            "--dbname",
            connstr,
            "--file",
            "-",
        ]
        if on_error_stop:
            cmd += ["--set", "ON_ERROR_STOP=1"]
        if extra_params:
            cmd += [str(p) for p in extra_params]
        proc = subprocess.run(
            cmd,
            input=query,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            encoding="utf-8",
            errors="replace",
            env=self._connenv(),
            check=False,
            timeout=timeout,
        )
        # Match Cluster->psql, which chomps a single trailing newline off each.
        stdout = proc.stdout[:-1] if proc.stdout.endswith("\n") else proc.stdout
        stderr = proc.stderr[:-1] if proc.stderr.endswith("\n") else proc.stderr
        return ProgramResult(proc.returncode, stdout, stderr)

    def safe_psql(
        self, query, dbname="postgres", timeout=None, extra_env=None, connstr=None
    ):
        """
        Execute query via psql and return its trimmed stdout, raising on error.
        Mirrors PostgreSQL::Test::Cluster->safe_psql: the SQL is piped to psql
        (so multiple statements run separately, e.g. CREATE DATABASE works), in
        tuples-only unaligned mode with ON_ERROR_STOP. An optional timeout (in
        seconds) bounds the psql invocation; extra_env adds/overrides connection
        environment variables (e.g. PGOPTIONS, PGUSER). A connstr overrides the
        --dbname target (merged with PGHOST/PGPORT from the environment), used
        by the SSL tests to pick a specific cert/host combination.

        Prefer :meth:`sql`, which returns a typed :class:`SqlResult` rather than
        a bare string; safe_psql is kept for the per-statement string contract.
        """
        stdout = self._psql_text(
            query, dbname=dbname, timeout=timeout, extra_env=extra_env, connstr=connstr
        )
        return stdout.rstrip("\n")

    def _psql_text(
        self, query, *, dbname="postgres", timeout=None, extra_env=None, connstr=None
    ):
        """Run query through psql (tuples-only, unaligned, ON_ERROR_STOP) and
        return its raw stdout, raising PgSqlError on a nonzero exit. Shared by
        safe_psql and sql.
        """
        if connstr is None:
            connstr = self.dbname_connstr(dbname)
        cmd = [
            str(self._bindir / "psql"),
            "--no-psqlrc",
            "--no-align",
            "--tuples-only",
            "--quiet",
            "--set",
            "ON_ERROR_STOP=1",
            "--dbname",
            connstr,
        ]
        proc = subprocess.run(
            cmd,
            input=query,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            encoding="utf-8",
            errors="replace",
            env=self._connenv(**(extra_env or {})),
            check=False,
            timeout=timeout,
        )
        if proc.returncode != 0:
            diags = _parse_psql_diagnostics(proc.stderr)
            raise PgSqlError(
                _psql_error_message(query, proc.stderr),
                primary=diags.get("primary"),
                detail=diags.get("detail"),
                hint=diags.get("hint"),
                context=diags.get("context"),
            )
        return proc.stdout

    def sql(
        self,
        query,
        *,
        dbname="postgres",
        timeout=None,
        extra_env=None,
        connstr=None,
        channel="psql",
    ) -> SqlResult:
        """Run query and return a typed :class:`SqlResult`, raising on error.

        This is the framework's primary SQL entry point. The default ``psql``
        channel pipes the SQL to the psql client, so each statement runs in its
        own implicit transaction (CREATE DATABASE and other
        non-transaction-block statements work) -- the same semantics as Perl's
        safe_psql. Pass ``channel="libpq"`` to run the query in-process over a
        fresh libpq connection (one connection, useful for protocol-level tests
        or a single transaction); errors raise the same PgSqlError either way.

        Use the result's accessors to say what shape you expect:
        ``.scalar()``, ``.row()``, ``.column()``, or ``.rows``.
        """
        if channel == "libpq":
            rows: list = []
            with self.connect(dbname=dbname) as conn:
                result = conn.exec(query)
                status = result.status()
                if status == ExecStatus.PGRES_TUPLES_OK:
                    rows = result.fetch_all()
                elif status != ExecStatus.PGRES_COMMAND_OK:
                    result.raise_error()
            return SqlResult([tuple(str(c) for c in row) for row in rows])
        if channel != "psql":
            raise ValueError(
                "channel must be 'psql' or 'libpq', got {!r}".format(channel)
            )
        stdout = self._psql_text(
            query, dbname=dbname, timeout=timeout, extra_env=extra_env, connstr=connstr
        )
        return SqlResult.from_psql(stdout)

    def check_extension(self, extname):
        """Return True if extname is available (in pg_available_extensions).

        Mirrors PostgreSQL::Test::Cluster->check_extension.
        """
        return (
            self.safe_psql(
                "SELECT count(*) > 0 FROM pg_available_extensions "
                "WHERE name = '{}'".format(extname)
            )
            == "t"
        )

    def config_data(self, *args):
        """Run pg_config from this cluster's install and return its output.

        Mirrors PostgreSQL::Test::Cluster->config_data: with a single option
        like '--bindir' the matching value is returned (trailing newline
        stripped); with no arguments the full pg_config output is returned.
        """
        cmd = [str(self._bindir / "pg_config")] + [str(a) for a in args]
        proc = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            encoding="utf-8",
            errors="replace",
            check=True,
        )
        return proc.stdout.rstrip("\n")

    def _check_log_patterns(self, test_name, offset, log_like, log_unlike):
        """Assert the server log (from offset onward) matches log_like patterns
        and matches none of the log_unlike patterns.

        Mirrors the log_like/log_unlike handling of
        PostgreSQL::Test::Cluster->connect_ok/connect_fails: each pattern is a
        regex applied to the log text emitted since `offset`. Because the
        backend writes its log asynchronously, this polls (up to the test
        timeout) until the log_like patterns appear before asserting.
        """
        if not log_like and not log_unlike:
            return
        deadline = (
            self._remaining_timeout_fn()
            if self._remaining_timeout_fn is not None
            else test_timeout_default()
        )
        end = time.monotonic() + deadline
        log = ""
        while True:
            log = slurp_file(self.log, offset) if self.log.exists() else ""
            if all(re.search(p, log) for p in (log_like or [])):
                break
            if time.monotonic() >= end:
                break
            time.sleep(0.1)
        for pattern in log_like or []:
            assert re.search(pattern, log), "{}: log matches {!r}\nlog:\n{}".format(
                test_name, pattern, log
            )
        for pattern in log_unlike or []:
            assert not re.search(
                pattern, log
            ), "{}: log unexpectedly matches {!r}\nlog:\n{}".format(
                test_name, pattern, log
            )

    def raw_connect(self):
        """Open a raw socket to the server's listening endpoint.

        Mirrors PostgreSQL::Test::Cluster->raw_connect: a connected stream socket
        to the Unix-domain socket (or TCP host:port) with no protocol
        negotiation. The caller drives the wire protocol and closes it.
        """
        if platform.system() != "Windows" and not str(self.host).startswith(
            ("127.", "::1")
        ):
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            sock.connect("{}/.s.PGSQL.{}".format(self.host, self.port))
        else:
            sock = socket.create_connection((str(self.host), self.port))
        return sock

    def raw_connect_works(self):
        """Return True if raw_connect() works on this platform.

        Mirrors PostgreSQL::Test::Cluster->raw_connect_works.
        """
        try:
            self.raw_connect().close()
        except OSError:
            return False
        return True

    def connect_ok(
        self,
        connstr,
        test_name,
        sql=None,
        expected_stdout=None,
        expected_stderr=None,
        log_like=None,
        log_unlike=None,
    ):
        """Assert that a connection with connstr succeeds.

        Mirrors PostgreSQL::Test::Cluster->connect_ok: psql connects with the
        given connstr (merged with PGHOST/PGPORT from the environment) and -w
        (never prompt for a password), runs sql (default a trivial SELECT), and
        must exit 0. Optionally the stdout must match expected_stdout; stderr
        must match expected_stderr if given, else be empty. log_like/log_unlike
        are lists of regexes that must (respectively must not) match the server
        log emitted during the connection attempt.
        """
        if sql is None:
            sql = "SELECT $$connected with {}$$".format(connstr)
        offset = self.current_log_position()
        result = self.psql_capture(
            sql, connstr=connstr, extra_params=["-w"], on_error_stop=False
        )
        assert result.rc == 0, "{}: exit {}\n{}".format(
            test_name, result.rc, result.stderr
        )
        if expected_stdout is not None:
            assert re.search(
                expected_stdout, result.stdout
            ), "{}: stdout matches {!r}, got {!r}".format(
                test_name, expected_stdout, result.stdout
            )
        if expected_stderr is not None:
            assert re.search(
                expected_stderr, result.stderr
            ), "{}: stderr matches {!r}, got {!r}".format(
                test_name, expected_stderr, result.stderr
            )
        else:
            assert result.stderr == "", "{}: no stderr, got {!r}".format(
                test_name, result.stderr
            )
        self._check_log_patterns(test_name, offset, log_like, log_unlike)

    def connect_fails(
        self, connstr, test_name, expected_stderr=None, log_like=None, log_unlike=None
    ):
        """Assert that a connection with connstr fails.

        Mirrors PostgreSQL::Test::Cluster->connect_fails: psql connects with the
        given connstr and -w but no SQL, and must exit non-zero. Optionally the
        stderr must match expected_stderr. log_like/log_unlike are lists of
        regexes that must (respectively must not) match the server log emitted
        during the connection attempt.
        """
        offset = self.current_log_position()
        result = self.psql_capture(
            "", connstr=connstr, extra_params=["-w"], on_error_stop=False
        )
        assert result.rc != 0, "{}: expected non-zero exit\n{}".format(
            test_name, result.stdout
        )
        if expected_stderr is not None:
            assert re.search(
                expected_stderr, result.stderr
            ), "{}: stderr matches {!r}, got {!r}".format(
                test_name, expected_stderr, result.stderr
            )
        self._check_log_patterns(test_name, offset, log_like, log_unlike)

    def wait_for_event(self, backend_type, wait_event_name):
        """Poll until a backend of backend_type is waiting on wait_event_name.
        Mirrors PostgreSQL::Test::Cluster->wait_for_event.
        """
        ok = self.poll_query_until(
            "SELECT count(*) > 0 FROM pg_stat_activity "
            "WHERE backend_type = '{}' AND wait_event = '{}'".format(
                backend_type, wait_event_name
            )
        )
        if not ok:
            raise AssertionError(
                "timed out waiting for event {!r} on backend {!r}".format(
                    wait_event_name, backend_type
                )
            )

    def poll_query_until(self, query, expected="t", dbname="postgres"):
        """
        Run query via psql repeatedly until its trimmed output equals expected
        (default "t") with empty stderr, or the timeout elapses. Returns True
        on success, False on timeout. Mirrors
        PostgreSQL::Test::Cluster->poll_query_until.
        """
        cmd = [
            str(self._bindir / "psql"),
            "--no-psqlrc",
            "--no-align",
            "--tuples-only",
            "--dbname",
            self.dbname_connstr(dbname),
        ]
        max_attempts = 10 * test_timeout_default()
        stdout = stderr = ""
        for _ in range(max_attempts):
            proc = subprocess.run(
                cmd,
                input=query,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                encoding="utf-8",
                errors="replace",
                env=self._connenv(),
                check=False,
            )
            stdout = proc.stdout.strip()
            stderr = proc.stderr.strip()
            if stdout == expected and stderr == "":
                return True
            time.sleep(0.1)

        eprint(
            "poll_query_until timed out:\nquery: {}\nexpected: {}\n"
            "last stdout: {}\nlast stderr: {}".format(query, expected, stdout, stderr)
        )
        return False

    def connstr(self, dbname=None):
        """Return a libpq connection string for this server.

        No inner quoting is applied (matching how the Perl suite embeds a
        connstr in primary_conninfo='...' / CONNECTION '...'), so the result
        can be nested inside a single-quoted string without escaping. Helpers
        that hand the dbname to psql (safe_psql/psql_capture/poll_query_until)
        escape it themselves via dbname_connstr.
        """
        parts = ["host={}".format(self.host), "port={}".format(self.port)]
        if dbname:
            parts.append("dbname={}".format(dbname))
        return " ".join(parts)

    def dbname_connstr(self, dbname):
        """Return a standalone connstr targeting dbname, with the name escaped.

        Mirrors PostgreSQL::Test::Cluster->connstr(dbname): the database name is
        single-quoted with backslashes and single quotes escaped, so a name
        containing spaces, quotes, or backslashes forms a valid connection
        string when passed as one argument (psql --dbname, or a client tool's
        connection-string option such as pg_createsubscriber --publisher-server).
        Unlike connstr(), the result must not be nested inside another
        single-quoted string.
        """
        escaped = dbname.replace("\\", "\\\\").replace("'", "\\'")
        return "host={} port={} dbname='{}'".format(self.host, self.port, escaped)

    def append_conf(self, text, filename="postgresql.conf"):
        """Append text (plus a trailing newline) to a file in the data dir.

        Mirrors PostgreSQL::Test::Cluster->append_conf.
        """
        append_to_file(self.datadir / filename, text + "\n")

    def adjust_conf(
        self, setting, value, filename="postgresql.conf", skip_equals=False
    ):
        """Rewrite a config file, replacing or removing a setting in place.

        Mirrors PostgreSQL::Test::Cluster->adjust_conf: every line that sets
        `setting` is dropped; if `value` is not None a single new line setting
        it is written in its place (other lines preserved). The file mode is
        reset to match the data dir's group accessibility.
        """
        conffile = self.datadir / filename
        eq = "" if skip_equals else "= "
        result = []
        for line in slurp_file(conffile).split("\n"):
            if not re.match(r"^{}\W".format(re.escape(setting)), line):
                if line != "":
                    result.append(line + "\n")
            elif value is not None:
                result.append("{} {}{}\n".format(setting, eq, value))
        with open(conffile, "w", encoding="utf-8") as fh:
            fh.write("".join(result))
        os.chmod(conffile, self._signal_file_mode())

    def checksum_enable_offline(self):
        """Enable data checksums on the stopped cluster (pg_checksums -e)."""
        result = self.bin.run_command(["pg_checksums", "-D", str(self.datadir), "-e"])
        assert result.rc == 0, "pg_checksums -e failed: {}".format(result.stderr)

    def checksum_disable_offline(self):
        """Disable data checksums on the stopped cluster (pg_checksums -d)."""
        result = self.bin.run_command(["pg_checksums", "-D", str(self.datadir), "-d"])
        assert result.rc == 0, "pg_checksums -d failed: {}".format(result.stderr)

    def backup_path(self, backup_name):
        """Return the path where backup_name is (or would be) stored."""
        return self._backup_root / backup_name

    @property
    def backup_dir(self):
        """The directory holding this server's backups (cf. Cluster->backup_dir).

        Created on demand so server-side backup targets (pg_basebackup --target
        server:DIR/...) can write into it immediately, as in the Perl suite.
        """
        self._backup_root.mkdir(parents=True, exist_ok=True)
        return self._backup_root

    @property
    def basedir(self):
        """The directory that contains this server's data dir (cf. Cluster)."""
        return pathlib.Path(self.datadir).parent

    @property
    def archive_dir(self):
        """The WAL archive directory for this server (cf. Cluster)."""
        return self.basedir / "archives"

    @staticmethod
    def _file_copy_command(src, dst):
        """A shell command that copies file src to dst.

        src/dst may embed the archive/restore %p/%f placeholders. On Windows
        use cmd's ``copy`` with backslash paths; elsewhere ``cp``.

        Co-authored-by: Andrew Dunstan <andrew@dunslane.net>
        """
        if WINDOWS_OS:
            return 'copy "{}" "{}"'.format(
                str(src).replace("/", "\\"), str(dst).replace("/", "\\")
            )
        return 'cp "{}" "{}"'.format(src, dst)

    def _enable_archiving(self):
        """Create the archive directory and turn on WAL archiving."""
        self.archive_dir.mkdir(parents=True, exist_ok=True)
        copy_command = self._file_copy_command("%p", "{}/%f".format(self.archive_dir))
        self.append_conf(
            "archive_mode = on\narchive_command = '{}'".format(copy_command)
        )

    def enable_archiving(self):
        """Enable WAL archiving on this (stopped) server.

        Mirrors PostgreSQL::Test::Cluster->enable_archiving.
        """
        self._enable_archiving()

    def corrupt_page_checksum(self, file, page_offset):
        """
        Flip the pd_checksum field of the page at page_offset in a relation
        file (relative to the data dir), mirroring
        PostgreSQL::Test::Cluster->corrupt_page_checksum.
        """
        path = self.datadir / file
        with open(path, "r+b") as fh:
            fh.seek(page_offset)
            header = bytearray(fh.read(24))
            # pd_checksum is a 2-byte field at offset 8 in PageHeaderData.
            header[8] ^= 0xFF
            header[9] ^= 0xFF
            fh.seek(page_offset)
            fh.write(header)

    def backup(self, backup_name, backup_options=None):
        """
        Take a base backup of this running server with pg_basebackup. Mirrors
        PostgreSQL::Test::Cluster->backup. backup_options are extra
        pg_basebackup arguments (e.g. --incremental). Returns the backup path.
        """
        path = self.backup_path(backup_name)
        self._backup_root.mkdir(parents=True, exist_ok=True)
        run(
            self._bindir / "pg_basebackup",
            "--no-sync",
            "--pgdata",
            path,
            "--host",
            self.host,
            "--port",
            str(self.port),
            "--checkpoint",
            "fast",
            *(str(opt) for opt in (backup_options or [])),
            env=self._connenv(),
        )
        return path

    def backup_fs_cold(self, backup_name):
        """
        Filesystem-level cold backup of a stopped server, excluding log/ and
        postmaster.pid (mirrors PostgreSQL::Test::Cluster->backup_fs_cold).
        """
        path = self.backup_path(backup_name)
        self._backup_root.mkdir(parents=True, exist_ok=True)
        shutil.copytree(
            self.datadir,
            path,
            ignore=shutil.ignore_patterns("log", "postmaster.pid"),
        )
        return path

    def reload(self):
        """Reload server configuration via pg_ctl reload."""
        self.pg_ctl("reload")

    def background_psql(
        self,
        dbname="postgres",
        on_error_stop=True,
        replication=None,
        extra_params=None,
        timeout=None,
        wait=True,
        tuples_only=True,
        quiet=True,
    ) -> BackgroundPsql:
        """
        Start an interactive psql session in the background, mirroring
        PostgreSQL::Test::Cluster->background_psql. Close it with .quit().

        tuples_only/quiet default True (the Perl -XAtq form). Set both False to
        see command tags and row-count footers (e.g. 'UPDATE 1', '(1 row)') in
        the session output, as some visibility tests match on those.
        """
        connstr = self.dbname_connstr(dbname)
        if replication is not None:
            connstr += " replication={}".format(replication)
        cmd = [
            str(self._bindir / "psql"),
            "--no-psqlrc",
            "--no-align",
        ]
        if tuples_only:
            cmd.append("--tuples-only")
        if quiet:
            cmd.append("--quiet")
        cmd += [
            "--dbname",
            connstr,
            "--file",
            "-",
        ]
        if on_error_stop:
            cmd += ["--set", "ON_ERROR_STOP=1"]
        if extra_params:
            cmd += extra_params
        return BackgroundPsql(cmd, self._connenv(), timeout=timeout, wait=wait)

    def interactive_psql(self, dbname="postgres", history_file=None, extra_params=None):
        """Start a PTY-backed interactive psql session.

        Mirrors PostgreSQL::Test::Cluster->interactive_psql: psql runs on a
        pseudo-terminal so it believes it is interactive (readline/libedit
        enabled for tab-completion and line-editing tests). PSQL_HISTORY/INPUTRC
        are redirected and TERM/LS_COLORS unset for deterministic output.
        """
        env = self._connenv()
        env["PSQL_HISTORY"] = history_file or "/dev/null"
        env["INPUTRC"] = "/dev/null"
        env.pop("TERM", None)
        env.pop("LS_COLORS", None)
        cmd = [
            str(self._bindir / "psql"),
            "--no-psqlrc",
            "--no-align",
            "--tuples-only",
            "--dbname",
            self.dbname_connstr(dbname),
        ]
        if extra_params:
            cmd += [str(p) for p in extra_params]
        return InteractivePsql(cmd, env)

    def _enable_streaming(self, source):
        """Configure this server as a streaming standby of source."""
        self.append_conf("primary_conninfo='{}'".format(source.connstr()))
        (self.datadir / "standby.signal").touch()

    def enable_streaming(self, source):
        """Configure this (stopped) server as a streaming standby of source.

        Mirrors PostgreSQL::Test::Cluster->enable_streaming; used to re-stream a
        demoted former primary after a failover role swap.
        """
        self._enable_streaming(source)

    def enable_restoring(self, source, standby=True):
        """Configure this (stopped) server to restore WAL from source's archive.

        Mirrors PostgreSQL::Test::Cluster->enable_restoring.
        """
        self._enable_restoring(source, standby)

    def _enable_restoring(self, source, standby=True):
        """Configure this server to restore WAL from source's archive.

        With standby=True a standby.signal is placed (standby mode); otherwise a
        recovery.signal is placed (recovery mode), mirroring init_from_backup's
        has_restoring/standby parameters.
        """
        restore_command = self._file_copy_command(
            "{}/%f".format(source.archive_dir), "%p"
        )
        self.append_conf("restore_command = '{}'".format(restore_command))
        signal = "standby.signal" if standby else "recovery.signal"
        sig = self.datadir / signal
        sig.touch()
        sig.chmod(self._signal_file_mode())

    def logrotate(self):
        """Request a log rotation via pg_ctl logrotate."""
        self.pg_ctl("logrotate")

    def promote(self):
        """Promote a standby via pg_ctl promote."""
        self.pg_ctl("promote")

    def teardown_node(self, fail_ok=False):
        """Stop the node (mirrors Cluster->teardown_node)."""
        self.stop("immediate" if fail_ok else "fast")

    def clean_node(self):
        """Stop the node and remove its data directory (cf. Cluster->clean_node).

        Frees the data dir so a node of the same name can be re-created from a
        fresh backup, as some streaming tests do.
        """
        self.stop()
        if self.datadir.exists():
            shutil.rmtree(self.datadir)

    def restart(self, mode="fast", fail_ok=False, log_like=None, log_unlike=None):
        """Restart the server via pg_ctl restart and refresh the postmaster PID.

        Mirrors PostgreSQL::Test::Cluster->restart. With fail_ok=True a failed
        restart returns False (1 in Perl maps to True here for success) instead
        of raising, and log_like/log_unlike (lists of regexes) are asserted
        against the log emitted during the restart attempt. Returns True on a
        successful restart, False on failure (only when fail_ok).
        """
        offset = self.current_log_position()
        try:
            self.pg_ctl("restart", "--mode", mode)
        except subprocess.CalledProcessError as exc:
            if not fail_ok:
                raise PgServerError(
                    'restart failed for node "{}"'.format(self.name)
                ) from exc
            self._check_log_patterns("restart", offset, log_like, log_unlike)
            return False
        with open(os.path.join(self.datadir, "postmaster.pid"), encoding="utf-8") as f:
            self.pid = int(f.readline().strip())
        self._check_log_patterns("restart", offset, log_like, log_unlike)
        return True

    def _signal_file_mode(self):
        """0o640 if the data dir is group-accessible (initdb -g), else 0o600."""
        dir_mode = stat.S_IMODE(self.datadir.stat().st_mode)
        return 0o640 if dir_mode & stat.S_IRGRP else 0o600

    def set_standby_mode(self):
        """Place a standby.signal file (cf. Cluster->set_standby_mode)."""
        sig = self.datadir / "standby.signal"
        sig.touch()
        sig.chmod(self._signal_file_mode())

    def set_recovery_mode(self):
        """Place a recovery.signal file (cf. Cluster->set_recovery_mode)."""
        sig = self.datadir / "recovery.signal"
        sig.touch()
        sig.chmod(self._signal_file_mode())

    def rotate_logfile(self):
        """Switch to a fresh server log file, used on the next (re)start.

        Mirrors PostgreSQL::Test::Cluster->rotate_logfile.
        """
        self._logfile_generation += 1
        self.log = self.datadir / "postgresql_{}.log".format(self._logfile_generation)
        return self.log

    def lsn(self, mode):
        """Return a WAL LSN of the given kind, or None if empty (cf. Cluster->lsn).

        mode is one of insert, flush, write, receive, replay.
        """
        modes = {
            "insert": "pg_current_wal_insert_lsn()",
            "flush": "pg_current_wal_flush_lsn()",
            "write": "pg_current_wal_lsn()",
            "receive": "pg_last_wal_receive_lsn()",
            "replay": "pg_last_wal_replay_lsn()",
        }
        if mode not in modes:
            raise ValueError("unknown mode for lsn: {!r}".format(mode))
        result = self.safe_psql("SELECT {}".format(modes[mode]))
        return result or None

    def wait_for_replay_catchup(self, standby, node=None):
        """Wait until standby has replayed up to this node's flush LSN.

        Mirrors PostgreSQL::Test::Cluster->wait_for_replay_catchup.
        """
        source = node if node is not None else self
        self.wait_for_catchup(standby, "replay", source.lsn("flush"))

    def wait_for_catchup(self, standby, mode="replay", target_lsn=None):
        """
        Wait until a standby has caught up to target_lsn (default: this node's
        current write/replay LSN), by polling pg_stat_replication. Mirrors
        PostgreSQL::Test::Cluster->wait_for_catchup (the polling fallback path).

        standby may be a PostgresServer or an application_name string.
        """
        valid_modes = ("sent", "write", "flush", "replay")
        if mode not in valid_modes:
            raise ValueError("unknown mode {!r} for wait_for_catchup".format(mode))

        standby_name = standby.name if isinstance(standby, PostgresServer) else standby

        if target_lsn is None:
            if self.safe_psql("SELECT pg_is_in_recovery()") == "t":
                target_lsn = self.lsn("replay")
            else:
                target_lsn = self.lsn("write")

        # Match the connection whose application_name is standby_name. Standbys
        # with a tool-generated primary_conninfo (pg_rewind / pg_basebackup
        # --write-recovery-conf) connect without setting application_name and so
        # report 'walreceiver'; fall back to that, but only when no connection
        # with the requested name exists. Otherwise an unrelated 'walreceiver'
        # connection (e.g. a physical standby alongside a named logical
        # subscriber) would also match, the per-row query would return more than
        # one row, and poll_query_until's single-"t" comparison never succeeds.
        query = (
            "SELECT '{lsn}' <= {mode}_lsn AND state = 'streaming'"
            " FROM pg_catalog.pg_stat_replication"
            " WHERE application_name = '{name}'"
            "    OR (application_name = 'walreceiver'"
            "        AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_replication"
            "                       WHERE application_name = '{name}'))"
        ).format(lsn=target_lsn, mode=mode, name=standby_name)

        if not self.poll_query_until(query):
            details = self.safe_psql("SELECT * FROM pg_catalog.pg_stat_replication")
            raise AssertionError(
                "timed out waiting for catchup\n"
                "pg_stat_replication:\n{}".format(details)
            )

    def wait_for_subscription_sync(
        self, publisher=None, subname=None, dbname="postgres"
    ):
        """
        Wait for all of this subscriber's tables to finish initial sync, then
        (if publisher/subname given) wait for the publisher to catch up.
        Mirrors PostgreSQL::Test::Cluster->wait_for_subscription_sync.
        """
        query = (
            "SELECT count(1) = 0 FROM pg_subscription_rel "
            "WHERE srsubstate NOT IN ('r', 's');"
        )
        if not self.poll_query_until(query, dbname=dbname):
            details = self.safe_psql("SELECT * FROM pg_subscription_rel", dbname=dbname)
            raise AssertionError(
                "timed out waiting for subscriber to synchronize data\n"
                "pg_subscription_rel:\n{}".format(details)
            )

        if publisher is not None:
            if subname is None:
                raise ValueError("subscription name must be specified")
            publisher.wait_for_catchup(subname)

    def pg_ctl(self, *args):
        """Run pg_ctl with the given arguments."""
        self._run(self._pg_ctl, "--pgdata", self.datadir, "--log", self.log, *args)

    def _connenv(self, **extra):
        """Return an environment dict with this server's PG* connection vars."""
        subenv = dict(os.environ)
        subenv.update(
            {
                "PGHOST": str(self.host),
                "PGPORT": str(self.port),
                "PGDATABASE": "postgres",
                "PGDATA": str(self.datadir),
            }
        )
        subenv.update(extra)
        return subenv

    @property
    def bin_dir(self):
        """This server's bin directory (cf. Cluster install bindir)."""
        return self._bindir

    @property
    def connenv(self):
        """An environment dict with this server's PG* connection vars.

        Public view of the connection environment, for spawning server binaries
        (e.g. postgres --single) directly via subprocess.
        """
        return self._connenv()

    @property
    def bin(self) -> PgBin:
        """A PgBin bound to this server's bindir and connection environment.

        Use it for node-scoped command assertions, e.g. node.bin.command_ok().
        """
        return PgBin(
            self._bindir,
            extra_env={
                "PGHOST": str(self.host),
                "PGPORT": str(self.port),
                "PGDATABASE": "postgres",
                "PGDATA": str(self.datadir),
            },
        )

    def command_ok(self, cmd, msg=None):
        """command_ok against this server's connection. See PgBin.command_ok."""
        return self.bin.command_ok(cmd, msg)

    def command_fails(self, cmd, msg=None):
        """command_fails against this server's connection."""
        return self.bin.command_fails(cmd, msg)

    def command_like(self, cmd, pattern, msg=None):
        """command_like against this server's connection."""
        return self.bin.command_like(cmd, pattern, msg)

    def command_fails_like(self, cmd, pattern, msg=None):
        """command_fails_like against this server's connection."""
        return self.bin.command_fails_like(cmd, pattern, msg)

    def command_checks_all(self, cmd, exit_code, stdout_res, stderr_res, msg=None):
        """command_checks_all against this server's connection."""
        return self.bin.command_checks_all(cmd, exit_code, stdout_res, stderr_res, msg)

    def pgbench(  # pylint: disable=keyword-arg-before-vararg
        self, opts, exit_code, stdout_res, stderr_res, msg, files=None, *args
    ):
        """Run pgbench against this server and check its output.

        Mirrors PostgreSQL::Test::Cluster->pgbench: opts is a string of pgbench
        options, files maps a script name to SQL run via -f, and args are
        appended. A script name may carry a trailing ``@<weight>`` which is kept
        in the --file argument (pgbench reads it as the script weight) but
        stripped from the on-disk filename. Files are written to this node's
        basedir in sorted order for determinism, and the command is verified
        with command_checks_all (expected exit_code, stdout_res/stderr_res).
        """
        cmd = ["pgbench"] + shlex.split(opts)
        script_files = files or {}
        for name in sorted(script_files):
            file_arg = self.basedir / name
            cmd += ["-f", str(file_arg)]
            # Strip a trailing @<weight> to get the real on-disk filename.
            on_disk = re.sub(r"@\d+$", "", str(file_arg))
            assert not os.path.exists(on_disk), "{} must not already exist".format(
                on_disk
            )
            with open(on_disk, "w", encoding="utf-8") as fh:
                fh.write(script_files[name])
        cmd += list(args)
        return self.bin.command_checks_all(cmd, exit_code, stdout_res, stderr_res, msg)

    def issues_sql_like(self, cmd, pattern, msg=None):
        """
        Run cmd against this server (expecting exit 0), then assert the server
        log gained a line matching pattern. Mirrors
        PostgreSQL::Test::Cluster->issues_sql_like (relies on log_statement=all).
        """
        offset = self.current_log_position()
        self.command_ok(cmd, msg)
        log = slurp_file(self.log, offset)
        assert re.search(
            pattern, log
        ), "{}: pattern {!r} not found in server log\nlog:\n{}".format(
            msg, pattern, log
        )

    def issues_sql_unlike(self, cmd, pattern, msg=None):
        """
        Run cmd against this server (expecting exit 0), then assert the server
        log did NOT gain a line matching pattern. Mirrors
        PostgreSQL::Test::Cluster->issues_sql_unlike.
        """
        offset = self.current_log_position()
        self.command_ok(cmd, msg)
        log = slurp_file(self.log, offset)
        assert not re.search(
            pattern, log
        ), "{}: pattern {!r} unexpectedly found in server log\nlog:\n{}".format(
            msg, pattern, log
        )

    def _run(self, cmd, *args, addenv: Optional[dict] = None):
        """Run a command with PG* environment variables set."""
        subenv = self._connenv(**(addenv or {}))
        run(cmd, *args, env=subenv)

    def create_users(self, *userkeys: str):
        """Create test users and register them for cleanup."""
        usermap = {}
        for u in userkeys:
            name = u + "user"
            usermap[u] = name
            self.psql("-c", "CREATE USER " + name)
            self._cleanup_stack.callback(self.psql, "-c", "DROP USER " + name)
        return usermap

    def create_dbs(self, *dbkeys: str):
        """Create test databases and register them for cleanup."""
        dbmap = {}
        for d in dbkeys:
            name = d + "db"
            dbmap[d] = name
            self.psql("-c", "CREATE DATABASE " + name)
            self._cleanup_stack.callback(self.psql, "-c", "DROP DATABASE " + name)
        return dbmap

    @contextlib.contextmanager
    def reloading(self):
        """
        Provides a context manager for making configuration changes.

        If the context suite finishes successfully, the configuration will
        be reloaded via pg_ctl. On teardown, the configuration changes will
        be unwound, and the server will be signaled to reload again.

        The context target contains the following attributes which can be
        used to configure the server:
        - .conf: modifies postgresql.conf
        - .hba: modifies pg_hba.conf

        For example:

            with pg_server_session.reloading() as s:
                s.conf.set(log_connections="on")
                s.hba.prepend("local all all trust")
        """
        # Push a reload onto the stack before making any other
        # unwindable changes. That way the order of operations will be
        #
        #  # test
        #   - config change 1
        #   - config change 2
        #   - reload
        #  # teardown
        #   - undo config change 2
        #   - undo config change 1
        #   - reload
        #
        self._cleanup_stack.callback(self.pg_ctl, "reload")
        yield self._backup_configuration()

        # Now actually reload
        self.pg_ctl("reload")

    @contextlib.contextmanager
    def restarting(self):
        """Like .reloading(), but with a full server restart."""
        self._cleanup_stack.callback(self.pg_ctl, "restart")
        yield self._backup_configuration()
        self.pg_ctl("restart")

    def _backup_configuration(self):
        # Wrap the existing HBA and configuration with FileBackups.
        return Backup(
            hba=self._cleanup_stack.enter_context(HBA(self.datadir)),
            conf=self._cleanup_stack.enter_context(Config(self.datadir)),
        )

    @contextlib.contextmanager
    def subcontext(self):
        """
        Create a new cleanup context for per-test isolation.

        Temporarily replaces the cleanup stack so that any cleanup callbacks
        registered within this context will be cleaned up when the context exits.
        """
        old_stack = self._cleanup_stack
        self._cleanup_stack = contextlib.ExitStack()
        try:
            self._cleanup_stack.__enter__()  # pylint: disable=unnecessary-dunder-call
            yield self
        finally:
            self._cleanup_stack.__exit__(None, None, None)
            self._cleanup_stack = old_stack

    def stop(self, mode="fast"):
        """
        Stop the PostgreSQL server instance.

        Ignores failures if the server is already stopped.
        """
        try:
            self.pg_ctl("stop", "--mode", mode)
        except subprocess.CalledProcessError:
            # Server may have already been stopped
            pass

    def signal_backend(self, pid, signame):
        """Send signal signame (e.g. "QUIT", "KILL", "TERM") to process pid.

        Uses ``pg_ctl kill``, which delivers the signal through the server's own
        mechanism and so works on every platform (Windows has no Unix signals).
        Not for SIGSTOP/SIGCONT, which pg_ctl kill cannot send; those remain
        Unix-only via os.kill in the few tests that need them.

        Co-authored-by: Andrew Dunstan <andrew@dunslane.net>
        """
        self._run(os.path.join(self._bindir, "pg_ctl"), "kill", signame, str(pid))

    def kill9(self):
        """Hard-kill the postmaster (cf. PostgreSQL::Test::Cluster->kill9).

        Reads the postmaster PID from postmaster.pid and signals it via
        ``pg_ctl kill KILL`` (portable to Windows); a no-op if the file is
        absent (server already gone).

        Co-authored-by: Andrew Dunstan <andrew@dunslane.net>
        """
        pidfile = os.path.join(self.datadir, "postmaster.pid")
        try:
            with open(pidfile, encoding="utf-8") as fh:
                pid = int(fh.readline().strip())
        except (OSError, ValueError):
            return
        try:
            self.signal_backend(pid, "KILL")
        except subprocess.CalledProcessError:
            pass

    def log_content(self) -> str:
        """Return log content from the current context's start position."""
        if not self.log.exists():
            return ""
        with open(self.log, encoding="utf-8", errors="replace") as f:
            f.seek(self._log_start_pos)
            return f.read()

    def log_matches(self, pattern, offset=0) -> bool:
        """Return True if the server log matches pattern from offset onward.

        Boolean counterpart to PostgreSQL::Test::Cluster->log_contains (the
        context-manager log_contains() on this class checks during a block).
        offset is a character position from current_log_position().
        """
        return re.search(pattern, self._log_text()[offset:]) is not None

    def wait_for_log(self, pattern, offset=0):
        """
        Poll the server log until pattern matches from offset onward, returning
        the new end offset. Mirrors PostgreSQL::Test::Cluster->wait_for_log.
        offset is a character position (see current_log_position).
        """
        max_attempts = 10 * test_timeout_default()
        for _ in range(max_attempts):
            text = self._log_text()
            if re.search(pattern, text[offset:]):
                return len(text)
            time.sleep(0.1)
        raise AssertionError("timed out waiting for log to match: {!r}".format(pattern))

    def log_check(self, test_name, offset, log_like=None, log_unlike=None):
        """Assert the server log (from offset onward) matches the given patterns.

        Mirrors PostgreSQL::Test::Cluster->log_check: log_like is a list of
        regexes that must all match the log text emitted since offset, and
        log_unlike a list of regexes that must none of them match. Because the
        backend flushes its log asynchronously, this polls (up to the test
        timeout) for the log_like patterns before asserting, so callers should
        first wait_for_log() on the event that guarantees the lines are present.
        """
        self._check_log_patterns(test_name, offset, log_like, log_unlike)

    @contextlib.contextmanager
    def log_contains(self, pattern, times=None):
        """
        Context manager that checks if the log matches pattern during the block.

        Args:
            pattern: The regex pattern to search for.
            times: If None, any number of matches is accepted.
                   If a number, exactly that many matches are required.
        """
        start_pos = self.current_log_position()
        yield
        with open(self.log, encoding="utf-8", errors="replace") as f:
            f.seek(start_pos)
            content = f.read()
        if times is None:
            assert re.search(pattern, content), f"Pattern {pattern!r} not found in log"
        else:
            match_count = len(re.findall(pattern, content))
            assert (
                match_count == times
            ), f"Expected {times} matches of {pattern!r}, found {match_count}"

    def cleanup(self):
        """Run all registered cleanup callbacks."""
        self._cleanup_stack.close()

    def set_timeout(self, remaining_timeout_fn: Callable[[], float]) -> None:
        """
        Set the timeout function for connections.
        This is typically called by pg fixture for each test.
        """
        self._remaining_timeout_fn = remaining_timeout_fn

    def connect(self, **opts) -> PGconn:
        """
        Creates a connection to this PostgreSQL server instance.

        Args:
            **opts: Additional connection options (can override defaults)

        Returns:
            PGconn: Connected database connection

        Example:
            conn = pg.connect()
            conn = pg.connect(dbname='mydb')
        """
        if self._remaining_timeout_fn is None:
            raise RuntimeError(
                "Timeout function not set. Use set_timeout() or pg fixture."
            )

        defaults = {
            "host": self.host,
            "port": self.port,
            "dbname": "postgres",
        }
        defaults.update(opts)

        return libpq_connect(
            self.libpq_handle,
            self._cleanup_stack,
            self._remaining_timeout_fn,
            **defaults,
        )
