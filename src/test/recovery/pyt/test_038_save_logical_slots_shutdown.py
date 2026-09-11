# Copyright (c) 2023-2026, PostgreSQL Global Development Group

"""Port of src/test/recovery/t/038_save_logical_slots_shutdown.pl.

Logical replication slots are always flushed to disk during a shutdown
checkpoint: the slot's confirmed_flush LSN must equal the latest checkpoint
location after a restart.
"""

import re

from pypg import slurp_file

_STREAMING = (
    r"Streaming transactions committing after ([A-F0-9]+/[A-F0-9]+), "
    r"reading WAL from ([A-F0-9]+/[A-F0-9]+)\."
)


def _advance_wal(node, num):
    # pg_switch_wal() forces a WAL flush, making the non-transactional
    # pg_logical_emit_message() safe to use.
    for _ in range(num):
        node.safe_psql(
            "SELECT pg_logical_emit_message(false, '', 'foo');\n"
            "SELECT pg_switch_wal();"
        )


def _latest_checkpoint(pg_bin, node):
    result = pg_bin.result(["pg_controldata", str(node.datadir)])
    match = re.search(
        r"^Latest checkpoint location:\s*(.*)$", result.stdout, re.MULTILINE
    )
    assert match, "Latest checkpoint location not found in control file"
    return match.group(1).strip()


def test_save_logical_slots_shutdown(pg_bin, create_pg):
    """A logical slot's confirmed_flush LSN matches the shutdown checkpoint."""
    publisher = create_pg("pub", allows_streaming="logical", start=False)
    # Avoid stray checkpoints so the latest checkpoint location stays put.
    publisher.append_conf("checkpoint_timeout = 1h\nautovacuum = off")
    publisher.start()
    subscriber = create_pg("sub")

    publisher.safe_psql("CREATE TABLE test_tbl (id int)")
    subscriber.safe_psql("CREATE TABLE test_tbl (id int)")

    # Advance the WAL segment so the shutdown checkpoint record (from the
    # restart below) doesn't land on a new page, which would desync
    # confirmed_flush_lsn from the checkpoint location.
    _advance_wal(publisher, 1)
    publisher.safe_psql("INSERT INTO test_tbl VALUES (generate_series(1, 5));")

    connstr = publisher.connstr() + " dbname=postgres"
    publisher.safe_psql("CREATE PUBLICATION pub FOR ALL TABLES")
    subscriber.safe_psql(
        "CREATE SUBSCRIPTION sub CONNECTION '{}' PUBLICATION pub".format(connstr)
    )
    subscriber.wait_for_subscription_sync(publisher, "sub")
    assert (
        subscriber.safe_psql("SELECT count(*) FROM test_tbl") == "5"
    ), "check initial copy was done"

    offset = publisher.current_log_position()
    # Restart to ensure the slot is flushed if required.
    publisher.restart()

    publisher.wait_for_log(_STREAMING, offset)
    match = re.search(_STREAMING, slurp_file(publisher.log, offset))
    assert match, "could not get confirmed_flush_lsn"
    confirmed_flush = match.group(1)

    assert (
        _latest_checkpoint(pg_bin, publisher) == confirmed_flush
    ), "slot's confirmed_flush LSN equals the latest checkpoint location"
