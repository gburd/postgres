# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/test/recovery/t/024_archive_recovery.pl.

Archive recovery of WAL generated with wal_level=minimal must fail.
"""

import re
import time

from pypg import slurp_file

_REPLICA_CONFIG = (
    "wal_level = replica\n"
    "archive_mode = on\n"
    "max_wal_senders = 10\n"
    "hot_standby = off"
)


def _wait_postmaster_gone(node):
    pidfile = node.datadir / "postmaster.pid"
    for _ in range(10 * 180):
        if not pidfile.exists():
            return
        time.sleep(0.1)


def test_archive_recovery(create_pg, pg_bin):
    """Recovery (and standby start) FATALs on a wal_level=minimal record."""
    node = create_pg("orig", has_archiving=True, allows_streaming=True, start=False)
    node.append_conf(_REPLICA_CONFIG)
    node.start()

    backup_name = "my_backup"
    node.backup(backup_name)

    # Generate WAL with wal_level=minimal (archiving off, so not archived yet).
    node.append_conf("wal_level = minimal\narchive_mode = off\nmax_wal_senders = 0")
    node.restart()

    # Switch back to replica/archiving so the wal_level-change record gets
    # archived.
    node.append_conf(_REPLICA_CONFIG)
    node.restart()

    walfile = node.safe_psql("SELECT pg_walfile_name(pg_current_wal_lsn());")
    node.safe_psql("SELECT pg_switch_wal()")
    assert node.poll_query_until(
        "SELECT '{}' <= last_archived_wal FROM pg_stat_archiver;".format(walfile)
    ), "WAL segment archived"
    node.stop()

    def check_recovery(node_name, node_text, standby_setting):
        recovery_node = create_pg(
            node_name,
            from_backup=(node, backup_name),
            has_restoring=True,
            standby=standby_setting,
            start=False,
        )
        # pg_ctl directly (not start) because recovery is expected to fail.
        pg_bin.result(
            [
                "pg_ctl",
                "--pgdata",
                str(recovery_node.datadir),
                "--log",
                str(recovery_node.log),
                "start",
            ]
        )
        _wait_postmaster_gone(recovery_node)
        assert re.search(
            r'FATAL: .* WAL was generated with "wal_level=minimal", '
            r"cannot continue recovering",
            slurp_file(recovery_node.log),
        ), "{} ends with an error on wal_level=minimal WAL".format(node_text)

    check_recovery("archive_recovery", "archive recovery", False)
    check_recovery("standby", "standby", True)
