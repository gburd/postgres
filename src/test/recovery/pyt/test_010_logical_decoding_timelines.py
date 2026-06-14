# Copyright (c) 2017-2026, PostgreSQL Global Development Group

"""Port of src/test/recovery/t/010_logical_decoding_timelines.pl.

Logical replication slots follow timeline changes across a filesystem-level
base backup and a standby promotion: a slot created before the backup is usable
on the promoted replica (decoding data written before, after, and post-failover)
while a slot created after the backup never reaches the replica; a dropped
database's slot is removed on the standby; and the physical slot's xmin /
catalog_xmin are tracked. Output is cross-checked via pg_recvlogical.
"""

import re

import pypg

_EXPECTED = (
    "BEGIN\n"
    "table public.decoding: INSERT: blah[text]:'beforebb'\n"
    "COMMIT\n"
    "BEGIN\n"
    "table public.decoding: INSERT: blah[text]:'afterbb'\n"
    "COMMIT\n"
    "BEGIN\n"
    "table public.decoding: INSERT: blah[text]:'after failover'\n"
    "COMMIT"
)


def test_010_logical_decoding_timelines(create_pg):
    """Logical slots follow timelines across fs backup and standby promotion."""
    primary = create_pg(
        "primary", allows_streaming=True, has_archiving=True, start=False
    )
    primary.append_conf(
        "\nwal_level = 'logical'\nmax_replication_slots = 3\nmax_wal_senders = 2\n"
        "log_min_messages = 'debug2'\nhot_standby_feedback = on\n"
        "wal_receiver_status_interval = 1\n"
    )
    primary.dump_info()
    primary.start()
    primary.safe_psql(
        "SELECT pg_create_logical_replication_slot('before_basebackup', "
        "'test_decoding');"
    )
    primary.safe_psql("CREATE TABLE decoding(blah text);")
    primary.safe_psql("INSERT INTO decoding(blah) VALUES ('beforebb');")
    primary.safe_psql("CREATE DATABASE dropme;")
    primary.safe_psql(
        "SELECT pg_create_logical_replication_slot('dropme_slot', 'test_decoding');",
        dbname="dropme",
    )
    primary.safe_psql("CHECKPOINT;")
    backup_name = "b1"
    primary.stop()
    primary.backup_fs_cold(backup_name)
    primary.start()
    primary.safe_psql("SELECT pg_create_physical_replication_slot('phys_slot');")
    replica = create_pg(
        "replica",
        from_backup=(primary, backup_name),
        has_streaming=True,
        has_restoring=True,
        start=False,
    )
    replica.append_conf("primary_slot_name = 'phys_slot'")
    replica.start()
    assert (
        primary.psql_capture("DROP DATABASE dropme").rc == 0
    ), "dropped DB with logical slot OK on primary"
    primary.wait_for_catchup(replica)
    assert (
        replica.safe_psql("SELECT 1 FROM pg_database WHERE datname = 'dropme'") == ""
    ), "dropped DB dropme on standby"
    assert (
        replica.slot("dropme_slot")["plugin"] == ""
    ), "logical slot was actually dropped on standby"
    primary.safe_psql(
        "SELECT pg_create_logical_replication_slot('after_basebackup', "
        "'test_decoding');"
    )
    primary.safe_psql("INSERT INTO decoding(blah) VALUES ('afterbb');")
    primary.safe_psql("CHECKPOINT;")
    assert (
        replica.safe_psql(
            "SELECT slot_name FROM pg_replication_slots ORDER BY slot_name"
        )
        == "before_basebackup"
    ), "Expected to find only slot before_basebackup on replica"
    assert primary.poll_query_until(
        "SELECT catalog_xmin IS NOT NULL FROM pg_replication_slots "
        "WHERE slot_name = 'phys_slot'"
    ), "slot's catalog_xmin never became set"
    phys_slot = primary.slot("phys_slot")
    assert phys_slot["xmin"] != "", "xmin assigned on physical slot of primary"
    assert (
        phys_slot["catalog_xmin"] != ""
    ), "catalog_xmin assigned on physical slot of primary"
    assert int(phys_slot["xmin"]) >= int(
        phys_slot["catalog_xmin"]
    ), "xmin on physical slot must not be lower than catalog_xmin"
    primary.safe_psql("CHECKPOINT")
    primary.wait_for_catchup(replica, "write")
    primary.stop("immediate")
    replica.promote()
    replica.safe_psql("INSERT INTO decoding(blah) VALUES ('after failover');")
    res = replica.psql_capture(
        "SELECT data FROM pg_logical_slot_peek_changes('after_basebackup', NULL, "
        "NULL, 'include-xids', '0', 'skip-empty-xacts', '1');"
    )
    assert res.rc == 3, "replaying from after_basebackup slot fails"
    assert re.search(
        r'replication slot "after_basebackup" does not exist', res.stderr
    ), "after_basebackup slot missing"
    res = replica.psql_capture(
        "SELECT data FROM pg_logical_slot_peek_changes('before_basebackup', NULL, "
        "NULL, 'include-xids', '0', 'skip-empty-xacts', '1');",
        timeout=pypg.test_timeout_default(),
    )
    assert res.rc == 0, "replay from slot before_basebackup succeeds"
    assert res.stdout == _EXPECTED, "decoded expected data from slot before_basebackup"
    assert res.stderr == "", "replay from slot before_basebackup produces no stderr"
    endpos = replica.safe_psql(
        "SELECT lsn FROM pg_logical_slot_peek_changes('before_basebackup', NULL, "
        "NULL) ORDER BY lsn DESC LIMIT 1;"
    )
    stdout = replica.pg_recvlogical_upto(
        "postgres",
        "before_basebackup",
        endpos,
        pypg.test_timeout_default(),
        options={"include-xids": "0", "skip-empty-xacts": "1"},
    )
    assert (
        stdout.rstrip("\n") == _EXPECTED
    ), "got same output from walsender via pg_recvlogical on before_basebackup"
    replica.teardown_node()
