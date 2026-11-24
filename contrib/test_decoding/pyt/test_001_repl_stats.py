# Copyright (c) 2022-2026, PostgreSQL Global Development Group

"""Port of contrib/test_decoding/t/001_repl_stats.pl.

Replication-slot statistics in pg_stat_replication_slots: stats persist across a
restart, survive a slot whose on-disk file was removed (when it no longer fits
under max_replication_slots), and a logical decoding worker that restarts does
not crash. The pgstat file must exist after a clean shutdown.
"""

import os
import shutil


def _test_slot_stats(node, expected, msg):
    """Assert the per-slot total_txns/total_bytes summary matches expected."""
    result = node.safe_psql(
        """
		SELECT slot_name, total_txns > 0 AS total_txn,
			   total_bytes > 0 AS total_bytes
			   FROM pg_stat_replication_slots
			   ORDER BY slot_name"""
    )
    assert result == expected, msg


def test_001_repl_stats(create_pg):
    """Replication-slot statistics persist and recover correctly."""
    node = create_pg("test", allows_streaming="logical", start=False)
    node.append_conf("synchronous_commit = on")
    node.start()
    node.safe_psql("CREATE TABLE test_repl_stat(col1 int)")
    node.safe_psql(
        """
	SELECT pg_create_logical_replication_slot('regression_slot1', 'test_decoding');
	SELECT pg_create_logical_replication_slot('regression_slot2', 'test_decoding');
	SELECT pg_create_logical_replication_slot('regression_slot3', 'test_decoding');
	SELECT pg_create_logical_replication_slot('regression_slot4', 'test_decoding');
"""
    )
    node.safe_psql("INSERT INTO test_repl_stat values(generate_series(1, 5));")
    node.safe_psql(
        """
	SELECT data FROM pg_logical_slot_get_changes('regression_slot1', NULL,
	NULL, 'include-xids', '0', 'skip-empty-xacts', '1');
	SELECT data FROM pg_logical_slot_get_changes('regression_slot2', NULL,
	NULL, 'include-xids', '0', 'skip-empty-xacts', '1');
	SELECT data FROM pg_logical_slot_get_changes('regression_slot3', NULL,
	NULL, 'include-xids', '0', 'skip-empty-xacts', '1');
	SELECT data FROM pg_logical_slot_get_changes('regression_slot4', NULL,
	NULL, 'include-xids', '0', 'skip-empty-xacts', '1');
"""
    )
    assert node.poll_query_until(
        """
	SELECT count(slot_name) >= 4 FROM pg_stat_replication_slots
	WHERE slot_name ~ 'regression_slot'
	AND total_txns > 0 AND total_bytes > 0;
"""
    ), "Timed out while waiting for statistics to be updated"
    node.safe_psql("SELECT pg_drop_replication_slot('regression_slot4')")
    node.stop()
    node.start()
    _test_slot_stats(
        node,
        "regression_slot1|t|t\nregression_slot2|t|t\nregression_slot3|t|t",
        "check replication statistics are updated",
    )
    node.stop()
    datadir = node.datadir
    slot3_replslotdir = "{}/pg_replslot/regression_slot3".format(datadir)
    shutil.rmtree(slot3_replslotdir)
    node.append_conf("max_replication_slots = 2")
    node.start()
    _test_slot_stats(
        node,
        "regression_slot1|t|t\nregression_slot2|t|t",
        "check replication statistics after removing the slot file",
    )
    node.safe_psql("DROP TABLE test_repl_stat")
    node.safe_psql("SELECT pg_drop_replication_slot('regression_slot1')")
    node.safe_psql("SELECT pg_drop_replication_slot('regression_slot2')")
    node.stop()
    node.start()
    slot_name_restart = "regression_slot5"
    node.safe_psql(
        "SELECT pg_create_logical_replication_slot('{}', 'test_decoding');".format(
            slot_name_restart
        )
    )
    bpgsql = node.background_psql("postgres", on_error_stop=True)
    bpgsql.query_safe(
        "SELECT pg_logical_slot_peek_binary_changes('{}', NULL, NULL)".format(
            slot_name_restart
        )
    )
    node.safe_psql("SELECT pg_drop_replication_slot('{}')".format(slot_name_restart))
    node.safe_psql(
        "SELECT pg_create_logical_replication_slot('{}', 'test_decoding');".format(
            slot_name_restart
        )
    )
    bpgsql.query_safe(
        "SELECT pg_logical_slot_peek_binary_changes('{}', NULL, NULL)".format(
            slot_name_restart
        )
    )
    node.safe_psql("SELECT pg_drop_replication_slot('{}')".format(slot_name_restart))
    node.stop()
    node.bin.command_like(
        ["pg_controldata", node.datadir],
        r"Database cluster state:\s+shut down\n",
        "node shut down ok",
    )
    stats_file = "{}/pg_stat/pgstat.stat".format(datadir)
    assert os.path.isfile(stats_file), "stats file must exist after shutdown"
    bpgsql.quit()
