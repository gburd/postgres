# Copyright (c) 2024-2026, PostgreSQL Global Development Group

"""Port of src/test/recovery/t/049_wait_for_lsn.pl.

Checks waiting for an LSN using the WAIT FOR command. Tests the standby wait
modes (standby_replay/standby_write/standby_flush) on a standby and primary_flush
mode on a primary: sessions block until replay/write/flush reaches the target
LSN and unblock exactly then, error on invalid targets or when called in a
transaction with a held snapshot / inside a function/procedure/DO block, time
out as specified, surface the right wait events in pg_stat_activity, behave
correctly across recovery pause/resume and promotion, and survive a cascade
upstream's timeline switch.
"""


def _stop_walreceiver(node):
    """Stop node's walreceiver by clearing primary_conninfo.

    Returns the saved (quoted) primary_conninfo so resume_walreceiver() can
    restore it, mirroring the Perl stop_walreceiver helper. Freezes the
    walreceiver-tracked positions (writtenUpto, flushedUpto) for fencepost
    tests.
    """
    saved_primary_conninfo = node.safe_psql(
        "SELECT pg_catalog.quote_literal(setting)\n"
        "FROM pg_settings\n"
        "WHERE name = 'primary_conninfo';"
    )
    node.safe_psql("ALTER SYSTEM SET primary_conninfo = '';\nSELECT pg_reload_conf();")
    assert node.poll_query_until(
        "SELECT NOT EXISTS (SELECT * FROM pg_stat_wal_receiver);"
    )
    return saved_primary_conninfo


def _resume_walreceiver(node, saved_primary_conninfo):
    """Restart node's walreceiver by restoring the saved primary_conninfo.

    Must be paired with a prior _stop_walreceiver() call (mirrors the Perl
    resume_walreceiver helper).
    """
    node.safe_psql(
        "ALTER SYSTEM SET primary_conninfo = {};\n"
        "SELECT pg_reload_conf();".format(saved_primary_conninfo)
    )
    assert node.poll_query_until("SELECT EXISTS (SELECT * FROM pg_stat_wal_receiver);")


def _check_wait_for_lsn_fencepost(node, mode, current_lsn, label):
    """Verify the wait predicate target <= currentLSN at the boundary.

    Given current_lsn (the frozen position for mode), checks that target ==
    current and target == current - 1 succeed and target == current + 1 times
    out. Returns (lsn_minus, lsn_plus). Mirrors the Perl helper.
    """
    lsn_minus = node.safe_psql("SELECT ('{}'::pg_lsn - 1)::text".format(current_lsn))
    lsn_plus = node.safe_psql("SELECT ('{}'::pg_lsn + 1)::text".format(current_lsn))

    cases = [
        (current_lsn, "success", "target == current succeeds", "5s"),
        (lsn_minus, "success", "target == current - 1 succeeds", "5s"),
        (lsn_plus, "timeout", "target == current + 1 times out", "500ms"),
    ]
    for target_lsn, expected, desc, timeout in cases:
        output = node.safe_psql(
            "WAIT FOR LSN '{target}'\n"
            "\tWITH (MODE '{mode}', timeout '{timeout}', no_throw);".format(
                target=target_lsn, mode=mode, timeout=timeout
            )
        )
        assert output == expected, "{}: {}".format(label, desc)

    return lsn_minus, lsn_plus


def _launch_wait(node, script):
    """Start a background psql, run script up to its '\\echo start', return it.

    The script must echo 'start' before its blocking WAIT FOR, so query_until
    returns while the WAIT FOR keeps running in the session (mirrors the Perl
    background_psql + query_until(qr/start/) pattern).
    """
    session = node.background_psql("postgres")
    session.query_until(r"start", script)
    return session


def _expect_blocked(node, count, wait_event_pattern):
    """Poll until exactly count backends are blocked on the wait event(s)."""
    if wait_event_pattern.endswith("%"):
        predicate = "wait_event LIKE '{}'".format(wait_event_pattern)
    else:
        predicate = "wait_event = '{}'".format(wait_event_pattern)
    assert node.poll_query_until(
        "SELECT count(*) = {} FROM pg_stat_activity WHERE {}".format(count, predicate)
    )


def test_049_wait_for_lsn(create_pg):
    """WAIT FOR LSN across all modes, validation, pause/resume, and promotion."""
    primary = create_pg("primary", allows_streaming=True)
    primary.safe_psql("CREATE TABLE wait_test AS SELECT generate_series(1,10) AS a")
    backup_name = "my_backup"
    primary.backup(backup_name)

    standby = create_pg(
        "standby", from_backup=(primary, backup_name), has_streaming=True, start=False
    )
    standby.append_conf("recovery_min_apply_delay = '1s'")
    standby.start()

    lsn2, lsn3 = _basic_modes(primary, standby)
    _timeout_and_subxact(primary, standby, lsn2, lsn3)
    _mode_and_syntax_validation(primary, standby, lsn3)
    _multi_replay_waiters(primary, standby)
    _multi_write_waiters(primary, standby)
    _multi_flush_waiters(primary, standby)
    _mixed_mode_waiters(primary, standby)
    _multi_primary_flush_waiters(primary)
    _promotion_terminates_waits(primary, standby)

    _archive_only_standby(create_pg)
    _fresh_shmem_walreceiver(create_pg)
    _cascade_timeline_switch(create_pg)


def _basic_modes(primary, standby):
    """Sections 1-3: basic WAIT FOR in each mode reaches the target LSN."""
    # 1. WAIT FOR works for replay.
    primary.safe_psql("INSERT INTO wait_test VALUES (generate_series(11, 20))")
    lsn1 = primary.safe_psql("SELECT pg_current_wal_insert_lsn()")
    output = standby.safe_psql(
        "WAIT FOR LSN '{lsn}' WITH (timeout '1d');\n"
        "SELECT pg_lsn_cmp(pg_last_wal_replay_lsn(), '{lsn}'::pg_lsn);".format(lsn=lsn1)
    )
    assert (
        int(output.split("\n")[-1]) >= 0
    ), "standby reached the same LSN as primary after WAIT FOR"

    # 2. New data is visible after WAIT FOR.
    primary.safe_psql("INSERT INTO wait_test VALUES (generate_series(21, 30))")
    lsn2 = primary.safe_psql("SELECT pg_current_wal_insert_lsn()")
    output = standby.safe_psql(
        "WAIT FOR LSN '{}';\nSELECT count(*) FROM wait_test;".format(lsn2)
    )
    assert output.split("\n")[-1] == "30", "standby reached the same LSN as primary"

    # 3. WAIT FOR with standby_write, standby_flush, and primary_flush modes.
    primary.safe_psql("INSERT INTO wait_test VALUES (generate_series(31, 40))")
    lsn_write = primary.safe_psql("SELECT pg_current_wal_insert_lsn()")
    output = standby.safe_psql(
        "WAIT FOR LSN '{lsn}' WITH (MODE 'standby_write', timeout '1d');\n"
        "SELECT pg_lsn_cmp((SELECT written_lsn FROM pg_stat_wal_receiver), "
        "'{lsn}'::pg_lsn);".format(lsn=lsn_write)
    )
    assert int(output.split("\n")[-1]) >= 0, (
        "standby wrote WAL up to target LSN after WAIT FOR with MODE " "'standby_write'"
    )

    primary.safe_psql("INSERT INTO wait_test VALUES (generate_series(41, 50))")
    lsn_flush = primary.safe_psql("SELECT pg_current_wal_insert_lsn()")
    output = standby.safe_psql(
        "WAIT FOR LSN '{lsn}' WITH (MODE 'standby_flush', timeout '1d');\n"
        "SELECT pg_lsn_cmp(pg_last_wal_receive_lsn(), '{lsn}'::pg_lsn);".format(
            lsn=lsn_flush
        )
    )
    assert int(output.split("\n")[-1]) >= 0, (
        "standby flushed WAL up to target LSN after WAIT FOR with MODE "
        "'standby_flush'"
    )

    primary.safe_psql("INSERT INTO wait_test VALUES (generate_series(51, 60))")
    lsn_primary_flush = primary.safe_psql("SELECT pg_current_wal_insert_lsn()")
    output = primary.safe_psql(
        "WAIT FOR LSN '{lsn}' WITH (MODE 'primary_flush', timeout '1d');\n"
        "SELECT pg_lsn_cmp(pg_current_wal_flush_lsn(), '{lsn}'::pg_lsn);".format(
            lsn=lsn_primary_flush
        )
    )
    assert (
        int(output.split("\n")[-1]) >= 0
    ), "primary flushed WAL up to target LSN after WAIT FOR with MODE 'primary_flush'"

    lsn3 = primary.safe_psql("SELECT pg_current_wal_insert_lsn() + 10000000000")
    return lsn2, lsn3


def _timeout_and_subxact(primary, standby, lsn2, lsn3):
    """Section 4 + 4a: timeout statuses and subtransaction cleanup."""
    standby.safe_psql("WAIT FOR LSN '{}' WITH (timeout '10ms');".format(lsn2))
    res = standby.psql_capture(
        "WAIT FOR LSN '{}' WITH (timeout '1000ms');".format(lsn3)
    )
    assert (
        "timed out while waiting for target LSN" in res.stderr
    ), "get timeout on waiting for unreachable LSN"

    output = standby.safe_psql(
        "WAIT FOR LSN '{}' WITH (timeout '0.1s', no_throw);".format(lsn2)
    )
    assert (
        output == "success"
    ), "WAIT FOR returns correct status after successful waiting"
    output = standby.safe_psql(
        "WAIT FOR LSN '{}' WITH (timeout '10ms', no_throw);".format(lsn3)
    )
    assert output == "timeout", "WAIT FOR returns correct status after timeout"

    _subxact_cleanup(primary)


def _subxact_cleanup(primary):
    """Section 4a: aborting a subtransaction during WAIT FOR cleans up state."""
    subxact_lsn = primary.safe_psql("SELECT pg_current_wal_insert_lsn() + 10000000000")
    appname = "wait_for_lsn_subxact_cleanup"
    session = primary.background_psql("postgres", on_error_stop=False)
    session.query_until(
        r"start",
        "SET application_name = '{appname}';\n"
        "BEGIN;\n"
        "SAVEPOINT wait_cleanup;\n"
        "\\echo start\n"
        "WAIT FOR LSN '{lsn}' WITH (MODE 'primary_flush');\n"
        "ROLLBACK TO wait_cleanup;\n"
        "WAIT FOR LSN '{lsn}'\n"
        "\tWITH (MODE 'primary_flush', timeout '10ms', no_throw);\n"
        "COMMIT;\n".format(appname=appname, lsn=subxact_lsn),
    )
    assert primary.poll_query_until(
        "SELECT count(*) = 1 FROM pg_stat_activity\n"
        "WHERE application_name = '{}'\n"
        "  AND wait_event = 'WaitForWalFlush'".format(appname)
    ), "WAIT FOR LSN did not enter the primary_flush wait path"
    subxact_cancelled = primary.safe_psql(
        "SELECT pg_cancel_backend(pid) FROM pg_stat_activity\n"
        "WHERE application_name = '{}'\n"
        "  AND wait_event = 'WaitForWalFlush'".format(appname)
    )
    assert subxact_cancelled == "t", "canceled WAIT FOR LSN in subtransaction"
    session.quit()
    stdout = session.stdout.rstrip("\n")
    assert (
        "canceling statement due to user request" in session.stderr
    ), "query cancel interrupted WAIT FOR LSN in subtransaction"
    assert stdout == "timeout", "second WAIT FOR LSN timed out after savepoint rollback"
    assert (
        "server closed the connection unexpectedly" not in session.stderr
    ), "WAIT FOR LSN after savepoint rollback did not disconnect"


def _mode_and_syntax_validation(primary, standby, lsn3):
    """Section 5 + 6: mode validation and parameter/syntax error cases."""
    res = primary.psql_capture(
        "WAIT FOR LSN '{}' WITH (MODE 'standby_flush');".format(lsn3)
    )
    assert (
        "recovery is not in progress" in res.stderr
    ), "get an error when running standby_flush on the primary"

    res = standby.psql_capture(
        "WAIT FOR LSN '{}' WITH (MODE 'primary_flush');".format(lsn3)
    )
    assert (
        "recovery is in progress" in res.stderr
    ), "get an error when running primary_flush on the standby"

    res = standby.psql_capture(
        "BEGIN ISOLATION LEVEL REPEATABLE READ; SELECT 1; "
        "WAIT FOR LSN '{}';".format(lsn3)
    )
    assert (
        "WAIT FOR must be called without an active or registered snapshot" in res.stderr
    ), (
        "get an error when running in a transaction with an isolation level "
        "higher than REPEATABLE READ"
    )

    _wrap_validation(primary, standby, lsn3)
    _param_validation(primary, standby)


def _wrap_validation(primary, standby, lsn3):
    """Section 5 (cont.): WAIT FOR errors inside function/procedure/DO block."""
    primary.safe_psql(
        "CREATE FUNCTION pg_wal_replay_wait_wrap(target_lsn pg_lsn) "
        "RETURNS void AS $$\n"
        "  BEGIN\n"
        "    EXECUTE format('WAIT FOR LSN %L;', target_lsn);\n"
        "  END\n"
        "$$\n"
        "LANGUAGE plpgsql;\n"
        "\n"
        "CREATE PROCEDURE pg_wal_replay_wait_proc(target_lsn pg_lsn) AS $$\n"
        "  BEGIN\n"
        "    EXECUTE format('WAIT FOR LSN %L;', target_lsn);\n"
        "  END\n"
        "$$\n"
        "LANGUAGE plpgsql;\n"
    )
    primary.wait_for_catchup(standby)

    res = standby.psql_capture("SELECT pg_wal_replay_wait_wrap('{}');".format(lsn3))
    assert (
        "WAIT FOR can only be executed as a top-level statement" in res.stderr
    ), "get an error when running within a function"

    res = standby.psql_capture("CALL pg_wal_replay_wait_proc('{}');".format(lsn3))
    assert (
        "WAIT FOR can only be executed as a top-level statement" in res.stderr
    ), "get an error when running within a procedure"

    res = standby.psql_capture(
        "DO $$ BEGIN EXECUTE format('WAIT FOR LSN %L;', '{}'); END $$;".format(lsn3)
    )
    assert (
        "WAIT FOR can only be executed as a top-level statement" in res.stderr
    ), "get an error when running within a DO block"


def _param_validation(primary, standby):
    """Section 6: parameter and syntax validation error cases on standby."""
    test_lsn = primary.safe_psql("SELECT pg_current_wal_insert_lsn()")
    lsn2 = test_lsn
    lsn3 = primary.safe_psql("SELECT pg_current_wal_insert_lsn() + 10000000000")

    checks = [
        (
            "WAIT FOR LSN '{}' WITH (timeout '-1000ms');".format(test_lsn),
            "timeout cannot be negative",
            "get error for negative timeout",
        ),
        (
            "WAIT FOR LSN '{}' WITH (unknown_param 'value');".format(test_lsn),
            'option "unknown_param" not recognized',
            "get error for unknown parameter",
        ),
        (
            "WAIT FOR LSN '{}' WITH (timeout '1000', timeout '2000');".format(test_lsn),
            "conflicting or redundant options",
            "get error for duplicate TIMEOUT parameter",
        ),
        (
            "WAIT FOR LSN '{}' WITH (no_throw, no_throw);".format(test_lsn),
            "conflicting or redundant options",
            "get error for duplicate NO_THROW parameter",
        ),
        (
            "WAIT FOR LSN '{}' (timeout '100ms');".format(test_lsn),
            "syntax error",
            "get syntax error when options specified without WITH keyword",
        ),
        (
            "WAIT FOR TIMEOUT 1000;",
            "syntax error",
            "get syntax error for missing LSN",
        ),
        (
            "WAIT FOR LSN 'invalid_lsn';",
            "invalid input syntax for type pg_lsn",
            "get error for invalid LSN format",
        ),
        (
            "WAIT FOR LSN '{}' WITH (timeout 'invalid');".format(test_lsn),
            "invalid timeout value",
            "get error for invalid timeout format",
        ),
        (
            "WAIT FOR LSN '{}' WITH (invalid_option 'value');".format(test_lsn),
            'option "invalid_option" not recognized',
            "get error for invalid WITH clause option",
        ),
        (
            "WAIT FOR LSN '{}' WITH (MODE 'invalid');".format(test_lsn),
            'unrecognized value for WAIT option "mode": "invalid"',
            "get error for invalid MODE value",
        ),
        (
            "WAIT FOR LSN '{}' WITH (MODE 'standby_replay', "
            "MODE 'standby_write');".format(test_lsn),
            "conflicting or redundant options",
            "get error for duplicate MODE parameter",
        ),
    ]
    for sql, pattern, msg in checks:
        res = standby.psql_capture(sql)
        assert pattern in res.stderr, "{}: stderr was {!r}".format(msg, res.stderr)

    output = standby.safe_psql(
        "WAIT FOR LSN '{}' WITH (timeout '0.1s', no_throw);".format(lsn2)
    )
    assert output == "success", "WAIT FOR WITH clause syntax works correctly"
    output = standby.safe_psql(
        "WAIT FOR LSN '{}' WITH (timeout 100, no_throw);".format(lsn3)
    )
    assert output == "timeout", "WAIT FOR WITH clause returns correct timeout status"


def _create_logging_functions(primary):
    """Create the log_count / log_wait_done helper functions on the primary."""
    primary.safe_psql(
        "CREATE FUNCTION log_count(i int) RETURNS void AS $$\n"
        "  DECLARE\n"
        "    count int;\n"
        "  BEGIN\n"
        "    SELECT count(*) FROM wait_test INTO count;\n"
        "    IF count >= 31 + i THEN\n"
        "      RAISE LOG 'count %', i;\n"
        "    END IF;\n"
        "  END\n"
        "$$\n"
        "LANGUAGE plpgsql;\n"
        "\n"
        "CREATE FUNCTION log_wait_done(prefix text, i int) RETURNS void AS $$\n"
        "  BEGIN\n"
        "    RAISE LOG '% %', prefix, i;\n"
        "  END\n"
        "$$\n"
        "LANGUAGE plpgsql;\n"
    )


def _multi_replay_waiters(primary, standby):
    """Section 7a: multiple standby_replay waiters report consistent data."""
    _create_logging_functions(primary)
    standby.safe_psql("SELECT pg_wal_replay_pause();")

    sessions = []
    for i in range(5):
        primary.safe_psql("INSERT INTO wait_test VALUES ({});".format(i))
        lsn = primary.safe_psql("SELECT pg_current_wal_insert_lsn()")
        sessions.append(
            _launch_wait(
                standby,
                "\\echo start\n"
                "WAIT FOR LSN '{lsn}';\n"
                "SELECT log_count({i});\n".format(lsn=lsn, i=i),
            )
        )

    log_offset = standby.current_log_position()
    standby.safe_psql("SELECT pg_wal_replay_resume();")
    for i in range(5):
        standby.wait_for_log("count {}".format(i), log_offset)
        sessions[i].quit()


def _multi_write_waiters(primary, standby):
    """Section 7b: multiple standby_write waiters unblock when WAL is written."""
    saved = _stop_walreceiver(standby)

    write_lsns = []
    for i in range(5):
        primary.safe_psql("INSERT INTO wait_test VALUES (100 + {});".format(i))
        write_lsns.append(primary.safe_psql("SELECT pg_current_wal_insert_lsn()"))

    sessions = []
    for i in range(5):
        sessions.append(
            _launch_wait(
                standby,
                "\\echo start\n"
                "WAIT FOR LSN '{lsn}' WITH (MODE 'standby_write', timeout '1d');\n"
                "SELECT log_wait_done('write_done', {i});\n".format(
                    lsn=write_lsns[i], i=i
                ),
            )
        )

    _expect_blocked(standby, 5, "WaitForWalWrite")

    write_log_offset = standby.current_log_position()
    _resume_walreceiver(standby, saved)

    for i in range(5):
        standby.wait_for_log("write_done {}".format(i), write_log_offset)
        sessions[i].quit()

    output = standby.safe_psql(
        "SELECT pg_lsn_cmp((SELECT written_lsn FROM pg_stat_wal_receiver), "
        "'{}'::pg_lsn);".format(write_lsns[4])
    )
    assert (
        int(output) >= 0
    ), "multiple standby_write waiters: standby wrote WAL up to target LSN"


def _multi_flush_waiters(primary, standby):
    """Section 7c: multiple standby_flush waiters unblock when WAL is flushed."""
    saved = _stop_walreceiver(standby)

    flush_lsns = []
    for i in range(5):
        primary.safe_psql("INSERT INTO wait_test VALUES (200 + {});".format(i))
        flush_lsns.append(primary.safe_psql("SELECT pg_current_wal_insert_lsn()"))

    sessions = []
    for i in range(5):
        sessions.append(
            _launch_wait(
                standby,
                "\\echo start\n"
                "WAIT FOR LSN '{lsn}' WITH (MODE 'standby_flush', timeout '1d');\n"
                "SELECT log_wait_done('flush_done', {i});\n".format(
                    lsn=flush_lsns[i], i=i
                ),
            )
        )

    _expect_blocked(standby, 5, "WaitForWalFlush")

    flush_log_offset = standby.current_log_position()
    _resume_walreceiver(standby, saved)

    for i in range(5):
        standby.wait_for_log("flush_done {}".format(i), flush_log_offset)
        sessions[i].quit()

    output = standby.safe_psql(
        "SELECT pg_lsn_cmp(pg_last_wal_receive_lsn(), '{}'::pg_lsn);".format(
            flush_lsns[4]
        )
    )
    assert (
        int(output) >= 0
    ), "multiple standby_flush waiters: standby flushed WAL up to target LSN"


def _mixed_mode_waiters(primary, standby):
    """Section 7d: mixed standby-mode waiters unblock on resume + reconnect."""
    saved = _stop_walreceiver(standby)
    standby.safe_psql("SELECT pg_wal_replay_pause();")

    primary.safe_psql("INSERT INTO wait_test VALUES (generate_series(301, 310));")
    target_lsn = primary.safe_psql("SELECT pg_current_wal_insert_lsn()")

    modes = ["standby_replay", "standby_write", "standby_flush"]
    sessions = []
    for i in range(6):
        sessions.append(
            _launch_wait(
                standby,
                "\\echo start\n"
                "WAIT FOR LSN '{lsn}' WITH (MODE '{mode}', timeout '1d');\n"
                "SELECT log_wait_done('mixed_done', {i});\n".format(
                    lsn=target_lsn, mode=modes[i % 3], i=i
                ),
            )
        )

    _expect_blocked(standby, 6, "WaitForWal%")

    mixed_log_offset = standby.current_log_position()
    standby.safe_psql("SELECT pg_wal_replay_resume();")
    assert standby.poll_query_until("SELECT NOT pg_is_wal_replay_paused();")

    _resume_walreceiver(standby, saved)

    for i in range(6):
        standby.wait_for_log("mixed_done {}".format(i), mixed_log_offset)
        sessions[i].quit()

    output = standby.safe_psql(
        "SELECT pg_lsn_cmp((SELECT written_lsn FROM pg_stat_wal_receiver), "
        "'{lsn}'::pg_lsn) >= 0 AND\n"
        "       pg_lsn_cmp(pg_last_wal_receive_lsn(), '{lsn}'::pg_lsn) >= 0 AND\n"
        "       pg_lsn_cmp(pg_last_wal_replay_lsn(), '{lsn}'::pg_lsn) >= 0;".format(
            lsn=target_lsn
        )
    )
    assert (
        output == "t"
    ), "mixed mode waiters: all modes completed and reached target LSN"


def _multi_primary_flush_waiters(primary):
    """Section 7e: multiple primary_flush waiters on the primary complete."""
    primary_flush_lsns = []
    for i in range(5):
        primary.safe_psql("INSERT INTO wait_test VALUES (400 + {});".format(i))
        primary_flush_lsns.append(
            primary.safe_psql("SELECT pg_current_wal_insert_lsn()")
        )

    log_offset = primary.current_log_position()

    sessions = []
    for i in range(5):
        sessions.append(
            _launch_wait(
                primary,
                "\\echo start\n"
                "WAIT FOR LSN '{lsn}' WITH (MODE 'primary_flush', timeout '1d');\n"
                "SELECT log_wait_done('primary_flush_done', {i});\n".format(
                    lsn=primary_flush_lsns[i], i=i
                ),
            )
        )

    for i in range(5):
        primary.wait_for_log("primary_flush_done {}".format(i), log_offset)
        sessions[i].quit()

    output = primary.safe_psql(
        "SELECT pg_lsn_cmp(pg_current_wal_flush_lsn(), '{}'::pg_lsn);".format(
            primary_flush_lsns[4]
        )
    )
    assert (
        int(output) >= 0
    ), "multiple primary_flush waiters: primary flushed WAL up to target LSN"


def _promotion_terminates_waits(primary, standby):
    """Section 8: standby promotion terminates all standby wait modes."""
    lsn4 = primary.safe_psql("SELECT pg_current_wal_insert_lsn() + 10000000000")
    lsn5 = primary.safe_psql("SELECT pg_current_wal_insert_lsn()")

    modes = ["standby_replay", "standby_write", "standby_flush"]
    sessions = []
    for i in range(3):
        sessions.append(
            _launch_wait(
                standby,
                "\\echo start\n"
                "WAIT FOR LSN '{lsn}' WITH (MODE '{mode}');\n".format(
                    lsn=lsn4, mode=modes[i]
                ),
            )
        )

    primary.safe_psql("SELECT pg_switch_wal();")
    primary.wait_for_catchup(standby)

    log_offset = standby.current_log_position()
    standby.promote()

    standby.wait_for_log(r"Recovery ended before target LSN.*was written", log_offset)
    standby.wait_for_log(r"Recovery ended before target LSN.*was flushed", log_offset)
    standby.wait_for_log(r"Recovery ended before target LSN.*was replayed", log_offset)

    standby.safe_psql("WAIT FOR LSN '{}';".format(lsn5))

    output = standby.safe_psql(
        "WAIT FOR LSN '{}' WITH (timeout '10ms', no_throw);".format(lsn4)
    )
    assert (
        output == "not in recovery"
    ), "WAIT FOR returns correct status after standby promotion"

    standby.stop()
    primary.stop()

    for session in sessions:
        session.quit()


def _archive_only_standby(create_pg):
    """Section 9: standby_write/standby_flush on an archive-only standby."""
    arc_primary = create_pg("arc_primary", has_archiving=True, allows_streaming=True)
    arc_primary.safe_psql("CREATE TABLE arc_test AS SELECT generate_series(1,10) AS a")
    arc_backup_name = "arc_backup"
    arc_primary.backup(arc_backup_name)

    arc_primary.safe_psql("INSERT INTO arc_test VALUES (generate_series(11, 20))")
    arc_target_lsn = arc_primary.safe_psql("SELECT pg_current_wal_insert_lsn()")

    arc_segment = arc_primary.safe_psql("SELECT pg_walfile_name(pg_current_wal_lsn())")
    arc_primary.safe_psql("SELECT pg_switch_wal()")
    assert arc_primary.poll_query_until(
        "SELECT last_archived_wal >= '{}' FROM pg_stat_archiver".format(arc_segment)
    ), "Timed out waiting for WAL archiving on arc_primary"

    arc_standby = create_pg(
        "arc_standby",
        from_backup=(arc_primary, arc_backup_name),
        has_restoring=True,
    )

    assert arc_standby.poll_query_until(
        "SELECT pg_wal_lsn_diff(pg_last_wal_replay_lsn(), '{}') >= 0".format(
            arc_target_lsn
        )
    ), "Timed out waiting for archive replay on arc_standby"

    output = arc_standby.safe_psql("SELECT count(*) FROM pg_stat_wal_receiver")
    assert output == "0", "arc_standby has no walreceiver"

    # 9a. Getter fallback: succeed immediately when already replayed.
    for mode in ("standby_write", "standby_flush"):
        output = arc_standby.safe_psql(
            "WAIT FOR LSN '{lsn}'\n"
            "\tWITH (MODE '{mode}', timeout '3s', no_throw);".format(
                lsn=arc_target_lsn, mode=mode
            )
        )
        assert (
            output == "success"
        ), "{} succeeds on archive-only standby (getter fallback)".format(mode)

    _archive_replay_waker(arc_primary, arc_standby)

    arc_standby.stop()
    arc_primary.stop()


def _archive_replay_waker(arc_primary, arc_standby):
    """Section 9b: sleeping standby_write/flush waiters woken by replay."""
    arc_standby.safe_psql("SELECT pg_wal_replay_pause()")

    arc_primary.safe_psql("INSERT INTO arc_test VALUES (generate_series(21, 30))")
    arc_target_lsn2 = arc_primary.safe_psql("SELECT pg_current_wal_insert_lsn()")

    arc_segment2 = arc_primary.safe_psql("SELECT pg_walfile_name(pg_current_wal_lsn())")
    arc_primary.safe_psql("SELECT pg_switch_wal()")
    assert arc_primary.poll_query_until(
        "SELECT last_archived_wal >= '{}' FROM pg_stat_archiver".format(arc_segment2)
    ), "Timed out waiting for WAL archiving on arc_primary (round 2)"

    write_session = _launch_wait(
        arc_standby,
        "\\echo start\n"
        "WAIT FOR LSN '{lsn}'\n"
        "\tWITH (MODE 'standby_write', timeout '1d', no_throw);\n".format(
            lsn=arc_target_lsn2
        ),
    )
    flush_session = _launch_wait(
        arc_standby,
        "\\echo start\n"
        "WAIT FOR LSN '{lsn}'\n"
        "\tWITH (MODE 'standby_flush', timeout '1d', no_throw);\n".format(
            lsn=arc_target_lsn2
        ),
    )

    _expect_blocked(arc_standby, 2, "WaitForWal%")

    arc_standby.safe_psql("SELECT pg_wal_replay_resume()")

    write_session.quit()
    flush_session.quit()
    assert (
        write_session.stdout.rstrip("\n") == "success"
    ), "standby_write waiter woken by replay on archive-only standby"
    assert (
        flush_session.stdout.rstrip("\n") == "success"
    ), "standby_flush waiter woken by replay on archive-only standby"


def _fresh_shmem_walreceiver(create_pg):
    """Sections 10-11: fresh-shmem walreceiver startup and fencepost checks."""
    rcv_primary = create_pg("rcv_primary", allows_streaming=True, start=False)
    rcv_primary.append_conf("autovacuum = off")
    rcv_primary.start()
    rcv_primary.safe_psql("CREATE TABLE rcv_test AS SELECT generate_series(1,10) AS a")

    rcv_backup = "rcv_backup"
    rcv_primary.backup(rcv_backup)

    rcv_standby = create_pg(
        "rcv_standby", from_backup=(rcv_primary, rcv_backup), has_streaming=True
    )

    rcv_primary.safe_psql("INSERT INTO rcv_test VALUES (generate_series(11, 100))")
    rcv_primary.safe_psql("SELECT pg_switch_wal()")
    rcv_primary.safe_psql("INSERT INTO rcv_test VALUES (generate_series(101, 110))")
    rcv_primary.wait_for_catchup(rcv_standby)

    rcv_standby.stop()
    rcv_primary.stop()
    rcv_standby.start()

    assert rcv_standby.poll_query_until(
        "SELECT pg_last_wal_receive_lsn() IS NOT NULL;"
    ), "walreceiver initial value did not become visible"

    rcv_standby.safe_psql("SELECT pg_wal_replay_pause()")
    assert rcv_standby.poll_query_until(
        "SELECT pg_get_wal_replay_pause_state() = 'paused'"
    ), "Timed out waiting for rcv_standby replay to pause"

    _fresh_shmem_checks(rcv_standby)

    rcv_standby.safe_psql("SELECT pg_wal_replay_resume()")
    rcv_primary.start()
    rcv_primary.safe_psql("INSERT INTO rcv_test VALUES (generate_series(111, 120))")
    rcv_primary.wait_for_catchup(rcv_standby)

    _fencepost_checks(rcv_primary, rcv_standby)

    rcv_standby.stop()
    rcv_primary.stop()


def _fresh_shmem_checks(rcv_standby):
    """Section 10: verify seeded walreceiver flush position and replay floor."""
    rcv_receive = rcv_standby.safe_psql("SELECT pg_last_wal_receive_lsn()")
    rcv_replay = rcv_standby.safe_psql("SELECT pg_last_wal_replay_lsn()")
    rcv_gap = rcv_standby.safe_psql(
        "SELECT pg_wal_lsn_diff('{replay}'::pg_lsn, '{recv}'::pg_lsn) > 0".format(
            replay=rcv_replay, recv=rcv_receive
        )
    )
    assert rcv_gap == "t", "replay sits ahead of initial walreceiver flush position"

    rcv_receive_offset = rcv_standby.safe_psql(
        "SELECT mod(pg_wal_lsn_diff('{recv}'::pg_lsn, '0/0'::pg_lsn),\n"
        "            setting::numeric)::int\n"
        "   FROM pg_settings\n"
        "  WHERE name = 'wal_segment_size'".format(recv=rcv_receive)
    )
    assert (
        rcv_receive_offset == "0"
    ), "initial walreceiver flush position is segment-aligned"

    for rcv_mode in ("standby_write", "standby_flush"):
        output = rcv_standby.safe_psql(
            "WAIT FOR LSN '{lsn}'\n"
            "\tWITH (MODE '{mode}', timeout '5s', no_throw);".format(
                lsn=rcv_replay, mode=rcv_mode
            )
        )
        assert (
            output == "success"
        ), "{} succeeds for already-replayed LSN after standby restart".format(rcv_mode)


def _fencepost_checks(rcv_primary, rcv_standby):
    """Section 11: off-by-one boundary checks for the wait predicate."""
    saved = _stop_walreceiver(rcv_standby)
    rcv_standby.safe_psql("SELECT pg_wal_replay_pause()")
    assert rcv_standby.poll_query_until(
        "SELECT pg_get_wal_replay_pause_state() = 'paused'"
    ), "Timed out waiting for rcv_standby replay to pause"

    # 11a. standby_replay exact fencepost.
    replay_lsn = rcv_standby.safe_psql("SELECT pg_last_wal_replay_lsn()")
    _, replay_lsn_plus = _check_wait_for_lsn_fencepost(
        rcv_standby, "standby_replay", replay_lsn, "standby_replay"
    )

    # 11b. standby_flush exact fencepost.
    flush_lsn = rcv_standby.safe_psql("SELECT pg_last_wal_receive_lsn()")
    flush_covers_replay = rcv_standby.safe_psql(
        "SELECT pg_wal_lsn_diff('{flush}'::pg_lsn, '{replay}'::pg_lsn) >= 0".format(
            flush=flush_lsn, replay=replay_lsn
        )
    )
    assert (
        flush_covers_replay == "t"
    ), "standby_flush boundary is not masked by replay floor"

    _check_wait_for_lsn_fencepost(
        rcv_standby, "standby_flush", flush_lsn, "standby_flush"
    )

    # 11c. A sleeping waiter at current + 1 wakes once replay advances past it.
    rcv_primary.safe_psql("INSERT INTO rcv_test VALUES (generate_series(200, 210))")

    boundary_session = _launch_wait(
        rcv_standby,
        "\\echo start\n"
        "WAIT FOR LSN '{lsn}'\n"
        "\tWITH (MODE 'standby_replay', timeout '1d', no_throw);\n".format(
            lsn=replay_lsn_plus
        ),
    )
    assert rcv_standby.poll_query_until(
        "SELECT count(*) > 0 FROM pg_stat_activity "
        "WHERE wait_event = 'WaitForWalReplay'"
    ), "Boundary waiter did not sleep"

    rcv_standby.safe_psql("SELECT pg_wal_replay_resume()")
    _resume_walreceiver(rcv_standby, saved)
    boundary_session.quit()
    assert (
        boundary_session.stdout.rstrip("\n") == "success"
    ), "standby_replay: waiter at current + 1 wakes when replay advances"


def _cascade_timeline_switch(create_pg):
    """Section 12: a WAIT FOR waiter survives a cascade upstream's promotion."""
    tl_primary = create_pg("tl_primary", allows_streaming=True, start=False)
    tl_primary.append_conf("autovacuum = off")
    tl_primary.start()
    tl_primary.safe_psql("CREATE TABLE tl_test AS SELECT generate_series(1, 10) AS a")

    tl_backup = "tl_backup"
    tl_primary.backup(tl_backup)

    tl_standby1 = create_pg(
        "tl_standby1", from_backup=(tl_primary, tl_backup), has_streaming=True
    )

    tl_backup2 = "tl_backup2"
    tl_standby1.backup(tl_backup2)

    tl_standby2 = create_pg(
        "tl_standby2", from_backup=(tl_standby1, tl_backup2), has_streaming=True
    )

    tl_primary.safe_psql("INSERT INTO tl_test VALUES (generate_series(11, 20))")
    tl_primary.wait_for_catchup(tl_standby1)
    tl_standby1.wait_for_catchup(tl_standby2)

    tl_target = tl_primary.safe_psql(
        "SELECT (pg_current_wal_insert_lsn() + 65536)::text"
    )

    tl_standby2.safe_psql("SELECT pg_wal_replay_pause()")
    assert tl_standby2.poll_query_until(
        "SELECT pg_get_wal_replay_pause_state() = 'paused'"
    ), "Timed out waiting for tl_standby2 replay to pause"

    tl_session = _launch_wait(
        tl_standby2,
        "\\echo start\n"
        "WAIT FOR LSN '{lsn}'\n"
        "\tWITH (MODE 'standby_replay', timeout '1d', no_throw);\n".format(
            lsn=tl_target
        ),
    )
    assert tl_standby2.poll_query_until(
        "SELECT count(*) > 0 FROM pg_stat_activity "
        "WHERE wait_event = 'WaitForWalReplay'"
    ), "Cascade waiter did not sleep before promotion"

    tl_standby1.promote()
    tl_standby1.safe_psql("INSERT INTO tl_test VALUES (generate_series(21, 1020))")
    tl_standby1.safe_psql("SELECT pg_switch_wal()")

    tl_standby2.safe_psql("SELECT pg_wal_replay_resume()")

    assert tl_standby2.poll_query_until(
        "SELECT received_tli > 1 FROM pg_stat_wal_receiver"
    ), "tl_standby2 did not follow upstream timeline switch"

    tl_session.quit()
    assert tl_session.stdout.rstrip("\n") == "success", (
        "WAIT FOR LSN survives upstream promotion and timeline switch on "
        "cascade standby"
    )

    tl_standby2.stop()
    tl_standby1.stop()
    tl_primary.stop()
