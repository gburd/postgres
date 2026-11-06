# Copyright (c) 2021-2026, PostgreSQL Global Development Group

"""Port of src/test/recovery/t/023_pitr_prepared_xact.pl.

Point-in-time recovery (PITR) with prepared transactions.
"""


def test_pitr_prepared_xact(create_pg):
    """A PITR target just after PREPARE leaves the 2PC xact to be committed."""
    primary = create_pg(
        "primary", has_archiving=True, allows_streaming=True, start=False
    )
    primary.append_conf("max_prepared_transactions = 10")
    primary.start()

    backup_name = "my_backup"
    primary.backup(backup_name)

    # Target a restore point just after PREPARE TRANSACTION, so the promoted
    # node still needs an explicit COMMIT PREPARED.
    node_pitr = create_pg(
        "node_pitr",
        from_backup=(primary, backup_name),
        standby=False,
        has_restoring=True,
        start=False,
    )
    node_pitr.append_conf(
        "recovery_target_name = 'rp'\nrecovery_target_action = 'promote'"
    )

    primary.psql_capture(
        "CREATE TABLE foo(i int);\n"
        "BEGIN;\n"
        "INSERT INTO foo VALUES(1);\n"
        "PREPARE TRANSACTION 'fooinsert';\n"
        "SELECT pg_create_restore_point('rp');\n"
        "INSERT INTO foo VALUES(2);\n"
    )

    walfile = primary.safe_psql("SELECT pg_walfile_name(pg_current_wal_lsn());")
    primary.safe_psql("SELECT pg_switch_wal()")
    assert primary.poll_query_until(
        "SELECT '{}' <= last_archived_wal FROM pg_stat_archiver;".format(walfile)
    ), "WAL segment archived"

    node_pitr.start()
    assert node_pitr.poll_query_until(
        "SELECT pg_is_in_recovery() = 'f';"
    ), "PITR node exited recovery"

    # Only the prepared-transaction row should be present; the INSERT after the
    # restore point is past the recovery target.
    node_pitr.psql_capture("COMMIT PREPARED 'fooinsert';")
    assert (
        node_pitr.safe_psql("SELECT * FROM foo;") == "1"
    ), "check table contents after COMMIT PREPARED"

    # New data on the post-promotion timeline must survive an immediate restart.
    node_pitr.psql_capture("INSERT INTO foo VALUES(3);\nCHECKPOINT;\n")
    node_pitr.stop("immediate")
    node_pitr.start()
