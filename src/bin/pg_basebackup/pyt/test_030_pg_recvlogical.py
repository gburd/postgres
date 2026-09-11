# Copyright (c) 2017-2026, PostgreSQL Global Development Group

"""Port of src/bin/pg_basebackup/t/030_pg_recvlogical.pl.

pg_recvlogical option/usage handling and behavior: required slot/database/
action, slot create/start/drop, two-phase and failover slot creation, and a
reconnection test where the streaming consumer is terminated server-side and
resumes, ultimately writing both committed INSERTs to its output file.
"""

import os
import platform
import signal
import stat

import pypg


def _cs(node):
    return node.connstr("postgres")


def test_030_pg_recvlogical(create_pg, pg_bin):
    """pg_recvlogical usage paths, slot lifecycle, two-phase/failover, reconnect."""
    pg_bin.program_help_ok("pg_recvlogical")
    pg_bin.program_version_ok("pg_recvlogical")
    pg_bin.program_options_handling_ok("pg_recvlogical")
    node = create_pg("main", allows_streaming=True, has_archiving=True, start=False)
    node.append_conf(
        "\nwal_level = 'logical'\nmax_replication_slots = 4\nmax_wal_senders = 4\n"
        "log_min_messages = 'debug1'\nlog_error_verbosity = verbose\n"
        "max_prepared_transactions = 10\n"
    )
    node.start()
    node.command_fails(["pg_recvlogical"], "pg_recvlogical needs a slot name")
    node.command_fails(
        ["pg_recvlogical", "--slot", "test"], "pg_recvlogical needs a database"
    )
    node.command_fails(
        ["pg_recvlogical", "--slot", "test", "--dbname", "postgres"],
        "pg_recvlogical needs an action",
    )
    node.command_fails(
        ["pg_recvlogical", "--slot", "test", "--dbname", _cs(node), "--start"],
        "no destination file",
    )
    node.command_ok(
        ["pg_recvlogical", "--slot", "test", "--dbname", _cs(node), "--create-slot"],
        "slot created",
    )
    assert node.slot("test")["restart_lsn"] != "", "restart lsn is defined for new slot"
    node.psql_capture("CREATE TABLE test_table(x integer)")
    node.psql_capture(
        "INSERT INTO test_table(x) SELECT y FROM generate_series(1, 10) a(y);"
    )
    nextlsn = node.safe_psql("SELECT pg_current_wal_insert_lsn()")
    node.command_ok(
        [
            "pg_recvlogical",
            "--slot",
            "test",
            "--dbname",
            _cs(node),
            "--start",
            "--endpos",
            nextlsn,
            "--no-loop",
            "--file",
            "-",
        ],
        "replayed a transaction",
    )
    node.command_ok(
        ["pg_recvlogical", "--slot", "test", "--dbname", _cs(node), "--drop-slot"],
        "slot dropped",
    )
    node.command_ok(
        [
            "pg_recvlogical",
            "--slot",
            "test",
            "--dbname",
            _cs(node),
            "--create-slot",
            "--two-phase",
        ],
        "slot with two-phase created",
    )
    assert node.slot("test")["restart_lsn"] != "", "restart lsn is defined for new slot"
    node.safe_psql(
        "BEGIN; INSERT INTO test_table values (11); PREPARE TRANSACTION 'test'"
    )
    node.safe_psql("COMMIT PREPARED 'test'")
    nextlsn = node.safe_psql("SELECT pg_current_wal_insert_lsn()")
    node.command_fails(
        [
            "pg_recvlogical",
            "--slot",
            "test",
            "--dbname",
            _cs(node),
            "--start",
            "--endpos",
            nextlsn,
            "--enable-two-phase",
            "--no-loop",
            "--file",
            "-",
        ],
        "incorrect usage",
    )
    node.command_ok(
        [
            "pg_recvlogical",
            "--slot",
            "test",
            "--dbname",
            _cs(node),
            "--start",
            "--endpos",
            nextlsn,
            "--no-loop",
            "--file",
            "-",
        ],
        "replayed a two-phase transaction",
    )
    node.command_ok(
        ["pg_recvlogical", "--slot", "test", "--drop-slot"],
        "drop could work without dbname",
    )
    node.command_ok(
        [
            "pg_recvlogical",
            "--slot",
            "test",
            "--dbname",
            _cs(node),
            "--create-slot",
            "--enable-failover",
        ],
        "slot with failover created",
    )
    assert (
        node.safe_psql(
            "SELECT failover FROM pg_catalog.pg_replication_slots "
            "WHERE slot_name = 'test'"
        )
        == "t"
    ), "failover is enabled for the new slot"
    _reconnect_test(node)


def _reconnect_test(node):
    """A terminated streaming consumer reconnects and writes both INSERTs."""
    outfile = "{}/reconnect.out".format(node.basedir)
    node.command_ok(
        [
            "pg_recvlogical",
            "--slot",
            "reconnect_test",
            "--dbname",
            _cs(node),
            "--create-slot",
        ],
        "slot created for reconnection test",
    )
    node.safe_psql("INSERT INTO test_table VALUES (1)")
    cmd = [
        "pg_recvlogical",
        "--slot",
        "reconnect_test",
        "--dbname",
        _cs(node),
        "--start",
        "--file",
        outfile,
        "--fsync-interval",
        "1",
        "--status-interval",
        "100",
        "--verbose",
    ]
    recv = node.bin.popen(cmd)
    try:
        first_ins = pypg.wait_for_file(outfile, r"INSERT")
        backend_pid = node.safe_psql(
            "SELECT active_pid FROM pg_replication_slots "
            "WHERE slot_name = 'reconnect_test'"
        )
        node.safe_psql("SELECT pg_terminate_backend({})".format(backend_pid))
        assert node.poll_query_until(
            "SELECT active_pid IS NOT NULL AND active_pid != {} "
            "FROM pg_replication_slots WHERE slot_name = 'reconnect_test'".format(
                backend_pid
            )
        ), "Timed out while waiting for pg_recvlogical to reconnect"
        node.safe_psql("INSERT INTO test_table VALUES (2)")
        pypg.wait_for_file(outfile, r"INSERT", first_ins)
    finally:
        recv.send_signal(signal.SIGTERM)
        recv.wait()
    outfiledata = pypg.slurp_file(outfile)
    count = outfiledata.count("INSERT")
    assert count == 2, "pg_recvlogical has received and written two INSERTs"

    # pg_recvlogical derives output-file permissions from the source cluster.
    # Unix-style permissions are not supported on Windows (cf. the Perl SKIP).
    if platform.system() != "Windows":
        # The cluster was initialized without group access, so the output file
        # should be 0600.
        mode = stat.S_IMODE(os.stat(outfile).st_mode)
        assert mode == 0o600, (
            "pg_recvlogical output file has no group permissions (0600), "
            "got {:04o}".format(mode)
        )

        # Enable group access on the source cluster and restart so
        # pg_recvlogical observes the updated source-cluster permissions.
        node.stop()
        pypg.chmod_recursive(node.datadir, 0o750, 0o640)
        node.start()

        group_outfile = "{}/group_access.out".format(node.basedir)
        group_cmd = [
            "pg_recvlogical",
            "--slot",
            "reconnect_test",
            "--dbname",
            _cs(node),
            "--start",
            "--file",
            group_outfile,
            "--fsync-interval",
            "1",
        ]
        group_recv = node.bin.popen(group_cmd)
        try:
            node.safe_psql("INSERT INTO test_table VALUES (3)")
            pypg.wait_for_file(group_outfile, r"INSERT")
        finally:
            group_recv.send_signal(signal.SIGTERM)
            group_recv.wait()

        # With group access on the source cluster, the output file is 0640.
        mode = stat.S_IMODE(os.stat(group_outfile).st_mode)
        assert mode == 0o640, (
            "pg_recvlogical output file respects group permissions (0640), "
            "got {:04o}".format(mode)
        )

    node.command_ok(
        [
            "pg_recvlogical",
            "--slot",
            "reconnect_test",
            "--dbname",
            _cs(node),
            "--drop-slot",
        ],
        "reconnect_test slot dropped",
    )
