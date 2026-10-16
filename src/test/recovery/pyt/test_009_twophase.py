# Copyright (c) 2017-2026, PostgreSQL Global Development Group

"""Port of src/test/recovery/t/009_twophase.pl.

Verifies prepared (two-phase) transactions survive restarts, immediate
teardowns, and primary/standby role swaps. Two synchronous-replication nodes
(london and paris) trade the primary role repeatedly; at each step a 2PC
transaction is prepared, then committed/rolled back after a restart, teardown,
or promotion, and the standby's shared-memory 2PC state and MVCC visibility are
checked. At the end the full t_009_tbl contents are validated on both nodes.
"""


class _Roles:
    """Tracks which node is currently primary/standby (they swap)."""

    def __init__(self, primary, standby):
        self.primary = primary
        self.standby = standby

    def swap(self):
        self.primary, self.standby = self.standby, self.primary

    @property
    def name(self):
        return self.primary.name


def _configure_and_reload(node, parameter):
    node.append_conf("\n{}\n".format(parameter))
    out = node.psql_capture("SELECT pg_reload_conf()")
    assert out.stdout.strip() == "t", "reload node {} with {}".format(
        node.name, parameter
    )


def _issue(node, body):
    """Run a multi-statement block with on_error_stop off; return rc."""
    return node.psql_capture(body, on_error_stop=False).exit_code


def test_009_twophase(create_pg):
    """Prepared transactions survive restart/teardown/promotion role swaps."""
    london = create_pg("london", allows_streaming=True, start=False)
    london.append_conf("\nmax_prepared_transactions = 10\nlog_checkpoints = true\n")
    london.start()
    london.backup("london_backup")
    paris = create_pg(
        "paris", from_backup=(london, "london_backup"), has_streaming=True, start=False
    )
    paris.append_conf("\nsubtransaction_buffers = 32\n")
    paris.start()
    _configure_and_reload(london, "synchronous_standby_names = 'paris'")
    _configure_and_reload(paris, "synchronous_standby_names = 'london'")
    roles = _Roles(london, paris)
    _restart_teardown_phase(roles)
    _standby_cleanup_phase(roles)
    _promotion_phase(roles, london, paris)
    _final_checks(roles)


def _prep(name, lo, hi, nm):
    return (
        "BEGIN;\nINSERT INTO t_009_tbl VALUES ({lo}, 'issued to {nm}');\n"
        "SAVEPOINT s1;\n"
        "INSERT INTO t_009_tbl VALUES ({hi}, 'issued to {nm}');\n"
        "PREPARE TRANSACTION '{name}';".format(lo=lo, hi=hi, nm=nm, name=name)
    )


def _restart_teardown_phase(roles):
    p = roles.primary
    nm = roles.name
    p.psql_capture("CREATE TABLE t_009_tbl (id int, msg text)")
    _issue(p, _prep("xact_009_1", 1, 2, nm) + "\n" + _prep("xact_009_2", 3, 4, nm))
    p.stop()
    p.start()
    assert _issue(p, "COMMIT PREPARED 'xact_009_1'") == 0, "commit after restart"
    assert _issue(p, "ROLLBACK PREPARED 'xact_009_2'") == 0, "rollback after restart"
    _issue(
        p,
        "CHECKPOINT;\n"
        + _prep("xact_009_3", 5, 6, nm)
        + "\n"
        + _prep("xact_009_4", 7, 8, nm),
    )
    p.teardown_node()
    p.start()
    assert _issue(p, "COMMIT PREPARED 'xact_009_3'") == 0, "commit after teardown"
    assert _issue(p, "ROLLBACK PREPARED 'xact_009_4'") == 0, "rollback after teardown"
    _issue(
        p,
        "CHECKPOINT;\n"
        + _prep("xact_009_5", 9, 10, nm)
        + "\nCOMMIT PREPARED 'xact_009_5';\n"
        + _prep("xact_009_5", 11, 12, nm),
    )
    p.teardown_node()
    p.start()
    assert _issue(p, "COMMIT PREPARED 'xact_009_5'") == 0, "same GID replay"
    _issue(p, _prep("xact_009_6", 13, 14, nm) + "\nCOMMIT PREPARED 'xact_009_6';")
    p.teardown_node()
    p.start()
    assert _issue(p, _prep("xact_009_7", 15, 16, nm)) == 0, "2PC shmem cleanup"
    p.psql_capture("COMMIT PREPARED 'xact_009_7'")


def _standby_cleanup_phase(roles):
    p, s = roles.primary, roles.standby
    nm = roles.name
    _issue(p, _prep("xact_009_8", 17, 18, nm) + "\nCOMMIT PREPARED 'xact_009_8';")
    assert (
        s.psql_capture("SELECT count(*) FROM pg_prepared_xacts").stdout.strip() == "0"
    ), "standby shmem cleanup without checkpoint"
    _issue(p, _prep("xact_009_9", 19, 20, nm))
    s.psql_capture("CHECKPOINT")
    p.psql_capture("COMMIT PREPARED 'xact_009_9'")
    assert (
        s.psql_capture("SELECT count(*) FROM pg_prepared_xacts").stdout.strip() == "0"
    ), "standby shmem cleanup after checkpoint"


def _promotion_phase(roles, london, paris):
    p = roles.primary
    nm = roles.name
    _issue(p, _prep("xact_009_10", 21, 22, nm))
    p.stop()
    roles.standby.promote()
    roles.swap()  # paris primary, london standby
    assert (
        _issue(
            roles.primary, "SET synchronous_commit = off; COMMIT PREPARED 'xact_009_10'"
        )
        == 0
    ), "restore prepared xact on promoted standby"
    roles.standby.enable_streaming(roles.primary)
    roles.standby.start()
    nm = roles.name
    _issue(roles.primary, _prep("xact_009_11", 23, 24, nm))
    roles.primary.stop()
    roles.standby.restart()
    roles.standby.promote()
    roles.swap()  # london primary, paris standby
    assert (
        roles.primary.psql_capture(
            "SELECT count(*) FROM pg_prepared_xacts"
        ).stdout.strip()
        == "1"
    ), "restore prepared xacts from files with primary down"
    roles.standby.enable_streaming(roles.primary)
    roles.standby.start()
    roles.primary.psql_capture("COMMIT PREPARED 'xact_009_11'")
    nm = roles.name
    _issue(roles.primary, _prep("xact_009_12", 25, 26, nm))
    roles.primary.stop()
    roles.standby.teardown_node()
    roles.standby.start()
    roles.standby.promote()
    roles.swap()  # paris primary, london standby
    assert (
        roles.primary.psql_capture(
            "SELECT count(*) FROM pg_prepared_xacts"
        ).stdout.strip()
        == "1"
    ), "restore prepared xacts from records with primary down"
    roles.standby.enable_streaming(roles.primary)
    roles.standby.start()
    roles.primary.psql_capture("COMMIT PREPARED 'xact_009_12'")
    _standby_mvcc(roles)


def _standby_mvcc(roles):
    p, s = roles.primary, roles.standby
    nm = roles.name
    p.psql_capture(
        "SET synchronous_commit='remote_apply';\n"
        "CREATE TABLE t_009_tbl_standby_mvcc (id int, msg text);\nBEGIN;\n"
        "INSERT INTO t_009_tbl_standby_mvcc VALUES (1, 'issued to {n}');\n"
        "SAVEPOINT s1;\n"
        "INSERT INTO t_009_tbl_standby_mvcc VALUES (2, 'issued to {n}');\n"
        "PREPARE TRANSACTION 'xact_009_standby_mvcc';".format(n=nm)
    )
    p.stop()
    s.restart()
    sess = s.background_psql("postgres", on_error_stop=True)
    sess.query_safe("BEGIN ISOLATION LEVEL REPEATABLE READ")
    assert (
        sess.query_safe("SELECT count(*) FROM t_009_tbl_standby_mvcc").strip() == "0"
    ), "prepared xact not visible in standby before commit"
    p.start()
    p.psql_capture(
        "SET synchronous_commit='remote_apply';\n"
        "COMMIT PREPARED 'xact_009_standby_mvcc';"
    )
    assert (
        sess.query_safe("SELECT count(*) FROM t_009_tbl_standby_mvcc").strip() == "0"
    ), "committed prepared xact not visible to old snapshot"
    sess.query_safe("COMMIT")
    assert (
        sess.query_safe("SELECT count(*) FROM t_009_tbl_standby_mvcc").strip() == "2"
    ), "committed prepared xact visible to new snapshot"
    sess.quit()
    _ddl_phase(roles)


def _ddl_phase(roles):
    p, s = roles.primary, roles.standby
    nm = roles.name
    p.psql_capture(
        "BEGIN;\nCREATE TABLE t_009_tbl2 (id int, msg text);\nSAVEPOINT s1;\n"
        "INSERT INTO t_009_tbl2 VALUES (27, 'issued to {n}');\n"
        "PREPARE TRANSACTION 'xact_009_13';\nCHECKPOINT;\n"
        "COMMIT PREPARED 'xact_009_13';".format(n=nm)
    )
    lsn = p.safe_psql("SELECT pg_current_wal_lsn()")
    assert s.poll_query_until(
        "SELECT '{}'::pg_lsn <= pg_last_wal_replay_lsn()".format(lsn)
    ), "Timed out while waiting for standby to catch up"
    assert (
        s.psql_capture("SELECT count(*) FROM t_009_tbl2").stdout.strip() == "1"
    ), "replay prepared xact with DDL"
    _issue(
        p,
        "BEGIN;\nCREATE TABLE t_009_tbl3 (id int, msg text);\nSAVEPOINT s1;\n"
        "INSERT INTO t_009_tbl3 VALUES (28, 'issued to {n}');\n"
        "PREPARE TRANSACTION 'xact_009_14';\n".format(n=nm)
        + _ddl_prep("xact_009_15", "t_009_tbl4", 29, nm),
    )
    p.teardown_node()
    p.start()
    assert _issue(p, "COMMIT PREPARED 'xact_009_14'") == 0, "commit DDL after teardown"
    assert (
        _issue(p, "ROLLBACK PREPARED 'xact_009_15'") == 0
    ), "rollback DDL after teardown"
    _issue(
        p,
        _ddl_prep("xact_009_16", "t_009_tbl5", 30, nm)
        + _ddl_prep("xact_009_17", "t_009_tbl6", 31, nm),
    )
    p.stop()
    p.start()
    assert _issue(p, "COMMIT PREPARED 'xact_009_16'") == 0, "commit DDL after restart"
    assert (
        _issue(p, "ROLLBACK PREPARED 'xact_009_17'") == 0
    ), "rollback DDL after restart"


def _ddl_prep(name, table, val, nm):
    return (
        "BEGIN;\nCREATE TABLE {t} (id int, msg text);\nSAVEPOINT s1;\n"
        "INSERT INTO {t} VALUES ({v}, 'issued to {n}');\n"
        "PREPARE TRANSACTION '{name}';\n".format(t=table, v=val, n=nm, name=name)
    )


_EXPECTED_TBL = (
    "1|issued to london\n2|issued to london\n5|issued to london\n"
    "6|issued to london\n9|issued to london\n10|issued to london\n"
    "11|issued to london\n12|issued to london\n13|issued to london\n"
    "14|issued to london\n15|issued to london\n16|issued to london\n"
    "17|issued to london\n18|issued to london\n19|issued to london\n"
    "20|issued to london\n21|issued to london\n22|issued to london\n"
    "23|issued to paris\n24|issued to paris\n25|issued to london\n"
    "26|issued to london"
)


def _final_checks(roles):
    p, s = roles.primary, roles.standby
    assert (
        p.psql_capture("SELECT count(*) FROM pg_prepared_xacts").stdout.strip() == "0"
    ), "no uncommitted prepared xacts on primary"
    assert (
        p.psql_capture("SELECT * FROM t_009_tbl ORDER BY id").stdout.strip()
        == _EXPECTED_TBL
    ), "expected t_009_tbl data on primary"
    assert (
        p.psql_capture("SELECT * FROM t_009_tbl2").stdout.strip()
        == "27|issued to paris"
    ), "expected t_009_tbl2 data on primary"
    assert (
        s.psql_capture("SELECT count(*) FROM pg_prepared_xacts").stdout.strip() == "0"
    ), "no uncommitted prepared xacts on standby"
    assert (
        s.psql_capture("SELECT * FROM t_009_tbl ORDER BY id").stdout.strip()
        == _EXPECTED_TBL
    ), "expected t_009_tbl data on standby"
