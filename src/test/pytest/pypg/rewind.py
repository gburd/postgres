# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Helper for the pg_rewind test suite, mirroring src/bin/pg_rewind/t/RewindTest.pm.

A RewindTest instance owns a primary and a standby and drives the standard
pg_rewind scenario: set up a primary (with a minimal-privilege rewind_user),
start it, create a streaming standby, optionally promote the standby so the
primary diverges, then rewind the old primary from the standby and restart it.

Only the 'local' and 'remote' source modes are implemented (the 'archive' mode
additionally needs enable_restoring/RecursiveCopy and is not yet ported).
"""

import shutil


GRANT_REWIND_USER = """
CREATE ROLE rewind_user LOGIN;
GRANT EXECUTE ON function pg_catalog.pg_ls_dir(text, boolean, boolean)
  TO rewind_user;
GRANT EXECUTE ON function pg_catalog.pg_stat_file(text, boolean)
  TO rewind_user;
GRANT EXECUTE ON function pg_catalog.pg_read_binary_file(text)
  TO rewind_user;
GRANT EXECUTE ON function pg_catalog.pg_read_binary_file(text, bigint, bigint, boolean)
  TO rewind_user;"""


class RewindTest:
    """Stateful driver for a pg_rewind primary/standby scenario."""

    def __init__(self, create_pg, pg_bin, tmp_path):
        self._create_pg = create_pg
        self._pg_bin = pg_bin
        self._tmp_path = tmp_path
        self.primary = None
        self.standby = None
        self._group_access = False

    def setup_cluster(self, extra_name=None, extra=None):
        """Initialize the primary (checksums, streaming, rewind_user auth)."""
        name = "primary" + ("_" + extra_name if extra_name else "")
        self._group_access = bool(extra and "-g" in extra)
        self.primary = self._create_pg(
            name,
            allows_streaming=True,
            auth_extra=["--create-role", "rewind_user"],
            extra=extra,
            start=False,
        )
        self.primary.append_conf(
            "wal_keep_size = 320MB\nallow_in_place_tablespaces = on\n"
        )

    def start_primary(self):
        """Start the primary and create the minimal-privilege rewind_user."""
        assert self.primary is not None
        self.primary.start()
        self.primary.safe_psql(GRANT_REWIND_USER)

    def create_standby(self, extra_name=None):
        """Back up the primary and bring up a streaming standby from it."""
        assert self.primary is not None
        name = "standby" + ("_" + extra_name if extra_name else "")
        self.primary.backup("my_backup")
        self.standby = self._create_pg(
            name, from_backup=(self.primary, "my_backup"), start=False
        )
        self.standby.append_conf(
            "primary_conninfo='{}'\n".format(self.primary.connstr())
        )
        self.standby.set_standby_mode()
        self.standby.start()

    def promote_standby(self):
        """Wait for the standby to catch up, then promote it (primary diverges)."""
        assert self.primary is not None and self.standby is not None
        self.primary.wait_for_catchup(self.standby, "write")
        self.standby.promote()

    def run_pg_rewind(self, test_mode):
        """Rewind the old primary from the standby in 'local', 'remote' or
        'archive' mode."""
        assert self.primary is not None and self.standby is not None
        primary_pgdata = self.primary.datadir
        standby_pgdata = self.standby.datadir
        standby_connstr = self.standby.connstr("postgres") + " user=rewind_user"
        conf_tmp = self._tmp_path / "primary-postgresql.conf.tmp"

        if test_mode == "archive":
            # WAL files are moved to the archive; stop gracefully so a clean
            # restart is still possible (--no-ensure-shutdown is used below).
            self.primary.stop()
        else:
            # The primary must finish recovery once; pg_rewind ensures that.
            self.primary.stop("immediate")

        # Keep a copy of postgresql.conf; pg_rewind overwrites it.
        shutil.copy(primary_pgdata / "postgresql.conf", conf_tmp)

        if test_mode == "local":
            self.standby.stop()
            self._pg_bin.command_ok(
                [
                    "pg_rewind",
                    "--debug",
                    "--source-pgdata",
                    str(standby_pgdata),
                    "--target-pgdata",
                    str(primary_pgdata),
                    "--no-sync",
                    "--config-file",
                    str(conf_tmp),
                ],
                "pg_rewind local",
            )
        elif test_mode == "remote":
            self._pg_bin.command_ok(
                [
                    "pg_rewind",
                    "--debug",
                    "--source-server",
                    standby_connstr,
                    "--target-pgdata",
                    str(primary_pgdata),
                    "--no-sync",
                    "--write-recovery-conf",
                    "--config-file",
                    str(conf_tmp),
                ],
                "pg_rewind remote",
            )
            auto = (primary_pgdata / "postgresql.auto.conf").read_text(encoding="utf-8")
            assert "dbname=postgres" in auto, "recovery conf file sets dbname"
            assert (
                primary_pgdata / "standby.signal"
            ).exists(), "standby.signal created after pg_rewind"
            self.standby.safe_psql("ALTER ROLE rewind_user WITH REPLICATION;")
        elif test_mode == "archive":
            # Source is a local pgdata; WAL is supplied from the target's
            # archive via restore_command (--restore-target-wal). Move all WAL
            # segments from the (gracefully stopped) old primary to its archive.
            archive_dir = self.primary.archive_dir
            wal_dir = primary_pgdata / "pg_wal"
            if archive_dir.exists():
                shutil.rmtree(archive_dir)
            shutil.copytree(wal_dir, archive_dir)
            shutil.rmtree(wal_dir)
            wal_dir.mkdir()
            archive_dir.chmod(0o700)
            wal_dir.chmod(0o700)
            # Add restore_command to the target cluster (restore from itself).
            self.primary._enable_restoring(  # pylint: disable=protected-access
                self.primary, standby=False
            )
            self.standby.stop()
            self._pg_bin.command_ok(
                [
                    "pg_rewind",
                    "--debug",
                    "--source-pgdata",
                    str(standby_pgdata),
                    "--target-pgdata",
                    str(primary_pgdata),
                    "--no-sync",
                    "--no-ensure-shutdown",
                    "--restore-target-wal",
                    "--config-file",
                    str(primary_pgdata / "postgresql.conf"),
                ],
                "pg_rewind archive",
            )
        else:
            raise ValueError("unsupported pg_rewind test mode: {}".format(test_mode))

        # Restore the saved postgresql.conf.
        shutil.move(str(conf_tmp), str(primary_pgdata / "postgresql.conf"))
        (primary_pgdata / "postgresql.conf").chmod(
            0o640 if self._group_access else 0o600
        )

        # Reconnect the rewound primary to the promoted standby (non-remote).
        if test_mode != "remote":
            self.primary.append_conf(
                "primary_conninfo='port={}'\n".format(self.standby.port)
            )
            self.primary.set_standby_mode()

        self.primary.start()

    def primary_psql(self, cmd, dbname="postgres"):
        """Run cmd on the primary (dies on error), like RewindTest::primary_psql."""
        assert self.primary is not None
        self.primary.safe_psql(cmd, dbname=dbname)

    def standby_psql(self, cmd, dbname="postgres"):
        """Run cmd on the standby (dies on error), like RewindTest::standby_psql."""
        assert self.standby is not None
        self.standby.safe_psql(cmd, dbname=dbname)

    def check_query(self, query, expected_stdout, test_name):
        """Assert that query against the primary returns expected_stdout."""
        assert self.primary is not None
        result = self.primary.safe_psql(query)
        assert result == expected_stdout, "{}: query result matches".format(test_name)

    def clean_rewind_test(self):
        """Stop both servers if they are still running."""
        for node in (self.primary, self.standby):
            if node is not None:
                try:
                    node.stop()
                except Exception:  # pylint: disable=broad-exception-caught
                    pass
