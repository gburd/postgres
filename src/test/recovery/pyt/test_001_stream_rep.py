# Copyright (c) 2017-2026, PostgreSQL Global Development Group

"""Port of src/test/recovery/t/001_stream_rep.pl.

Streaming replication end to end: a primary with two cascading standbys, content
and sequence streaming, read-only enforcement on standbys, libpq
target_session_attrs routing across a multi-host connstr, SHOW/READ_REPLICATION_SLOT
over replication connections, physical-slot xmin tracking under
hot_standby_feedback, physical slot advance persisting across restart (and WAL
recycling), and BASE_BACKUP interlock plus cancellation.
"""

import re


def _setup_cluster(create_pg):
    primary = create_pg(
        "primary",
        allows_streaming=True,
        auth_extra=["--create-role", "repl_role"],
        start=False,
    )
    primary.start()
    primary.backup("my_backup")
    standby1 = create_pg(
        "standby_1", from_backup=(primary, "my_backup"), has_streaming=True, start=False
    )
    standby1.start()
    standby1.backup("my_backup")
    primary.stop()
    standby1.backup("my_backup_2")
    primary.start()
    standby2 = create_pg(
        "standby_2",
        from_backup=(standby1, "my_backup"),
        has_streaming=True,
        start=False,
    )
    standby2.start()
    return primary, standby1, standby2


def test_001_stream_rep(create_pg):
    """Streaming replication, routing, slot xmins, and BASE_BACKUP behavior."""
    primary, standby1, standby2 = _setup_cluster(create_pg)
    primary.safe_psql("SELECT pg_stat_reset_shared('io')")
    primary.safe_psql("CREATE TABLE tab_int AS SELECT generate_series(1,1002) AS a")
    primary.safe_psql(
        "CREATE TABLE user_logins(id serial, who text);\n"
        "CREATE FUNCTION on_login_proc() RETURNS EVENT_TRIGGER AS $$\nBEGIN\n"
        "  IF NOT pg_is_in_recovery() THEN\n"
        "    INSERT INTO user_logins (who) VALUES (session_user);\n  END IF;\n"
        "  IF session_user = 'regress_hacker' THEN\n"
        "    RAISE EXCEPTION 'You are not welcome!';\n  END IF;\nEND;\n"
        "$$ LANGUAGE plpgsql SECURITY DEFINER;\n"
        "CREATE EVENT TRIGGER on_login_trigger ON login "
        "EXECUTE FUNCTION on_login_proc();\n"
        "ALTER EVENT TRIGGER on_login_trigger ENABLE ALWAYS;"
    )
    primary.wait_for_replay_catchup(standby1)
    standby1.wait_for_replay_catchup(standby2, primary)
    assert standby1.safe_psql("SELECT count(*) FROM tab_int") == "1002"
    assert standby2.safe_psql("SELECT count(*) FROM tab_int") == "1002"
    assert (
        standby1.safe_psql(
            "SELECT count(*) FROM pg_stat_recovery WHERE promote_triggered IS NOT NULL"
        )
        == "1"
    ), "check recovery state on standby 1"
    _check_sequences(primary, standby1, standby2)
    _target_session_attrs(primary, standby1, standby2)
    _show_and_read_slot(primary)
    _slot_xmins(primary, standby1, standby2)
    _physical_slot_advance(primary, standby1, standby2)
    _base_backup_interlock(primary)


def _check_sequences(primary, standby1, standby2):
    primary.safe_psql("CREATE SEQUENCE seq1; SELECT nextval('seq1')")
    primary.wait_for_replay_catchup(standby1)
    standby1.wait_for_replay_catchup(standby2, primary)
    assert standby1.safe_psql("SELECT * FROM seq1") == "33|0|t"
    assert standby2.safe_psql("SELECT * FROM seq1") == "33|0|t"
    primary.safe_psql("CREATE UNLOGGED SEQUENCE ulseq; SELECT nextval('ulseq')")
    primary.wait_for_replay_catchup(standby1)
    assert (
        standby1.safe_psql("SELECT pg_sequence_last_value('ulseq'::regclass) IS NULL")
        == "t"
    ), "pg_sequence_last_value() on unlogged sequence on standby 1"
    assert (
        standby1.psql_capture("INSERT INTO tab_int VALUES (1)").rc == 3
    ), "read-only queries on standby 1"
    assert (
        standby2.psql_capture("INSERT INTO tab_int VALUES (1)").rc == 3
    ), "read-only queries on standby 2"


def _tsa(node1, node2, target, mode, status):
    connstr = "host={},{} port={},{} target_session_attrs={}".format(
        node1.host, node2.host, node1.port, node2.port, mode
    )
    res = node1.psql_capture(
        "",
        connstr=connstr,
        extra_params=["--command", "SHOW port;"],
        on_error_stop=False,
    )
    if status == 0:
        assert res.rc == 0 and res.stdout.strip() == str(
            target.port
        ), 'connect with mode "{}" and {},{} listed'.format(
            mode, node1.name, node2.name
        )
    else:
        assert (
            res.rc == status and target is None
        ), 'fail to connect with mode "{}"'.format(mode)


def _target_session_attrs(primary, standby1, standby2):
    _tsa(primary, standby1, primary, "read-write", 0)
    _tsa(standby1, primary, primary, "read-write", 0)
    _tsa(primary, standby1, primary, "any", 0)
    _tsa(standby1, primary, standby1, "any", 0)
    _tsa(primary, standby1, primary, "primary", 0)
    _tsa(standby1, primary, primary, "primary", 0)
    _tsa(primary, standby1, standby1, "read-only", 0)
    _tsa(standby1, primary, standby1, "read-only", 0)
    _tsa(primary, primary, primary, "prefer-standby", 0)
    _tsa(primary, standby1, standby1, "prefer-standby", 0)
    _tsa(standby1, primary, standby1, "prefer-standby", 0)
    _tsa(primary, standby1, standby1, "standby", 0)
    _tsa(standby1, primary, standby1, "standby", 0)
    _tsa(standby1, standby2, None, "read-write", 2)
    _tsa(standby1, standby2, None, "primary", 2)
    _tsa(primary, primary, None, "read-only", 2)
    _tsa(primary, primary, None, "standby", 2)


def _show_and_read_slot(primary):
    primary.psql_capture(
        "CREATE ROLE repl_role REPLICATION LOGIN;\n"
        "GRANT pg_read_all_settings TO repl_role;"
    )
    common = "host={} port={} user=repl_role".format(primary.host, primary.port)
    rep = common + " replication=1"
    db = common + " replication=database dbname=postgres"
    for connstr, label in ((rep, "physical"), (db, "logical")):
        for sql in ("SHOW ALL;", "SHOW work_mem;", "SHOW primary_conninfo;"):
            assert (
                primary.psql_capture(sql, connstr=connstr).rc == 0
            ), "{} over {} replication".format(sql, label)
    slotname = "test_read_replication_slot_physical"
    res = primary.psql_capture(
        "READ_REPLICATION_SLOT non_existent_slot;", connstr=rep, on_error_stop=False
    )
    assert res.rc == 0, "READ_REPLICATION_SLOT exit code 0 on success"
    assert re.search(
        r"^\|\|$", res.stdout.strip(), re.M
    ), "READ_REPLICATION_SLOT returns NULL values if slot does not exist"
    primary.psql_capture(
        "CREATE_REPLICATION_SLOT {} PHYSICAL RESERVE_WAL;".format(slotname), connstr=rep
    )
    res = primary.psql_capture(
        "READ_REPLICATION_SLOT {};".format(slotname), connstr=rep
    )
    assert res.rc == 0, "READ_REPLICATION_SLOT success with existing slot"
    assert re.search(
        r"^physical\|[^|]*\|1$", res.stdout.strip(), re.M
    ), "READ_REPLICATION_SLOT returns tuple with slot information"
    primary.psql_capture("DROP_REPLICATION_SLOT {};".format(slotname), connstr=rep)


def _get_slot_xmins(node, slotname, check_expr):
    assert node.poll_query_until(
        "SELECT {}\nFROM pg_catalog.pg_replication_slots\n"
        "WHERE slot_name = '{}';".format(check_expr, slotname)
    ), "Timed out waiting for slot xmins to advance"
    info = node.slot(slotname)
    return info["xmin"], info["catalog_xmin"]


def _slot_xmins(primary, standby1, standby2):
    assert primary.poll_query_until(
        "SELECT sum(reads) > 0 FROM pg_catalog.pg_stat_io\n"
        "WHERE backend_type = 'walsender' AND object = 'wal'"
    ), "Timed out waiting for the walsender to update its IO statistics"
    primary.append_conf("max_replication_slots = 4")
    primary.restart()
    assert (
        primary.psql_capture(
            "SELECT pg_create_physical_replication_slot('standby_1');"
        ).rc
        == 0
    ), "physical slot created on primary"
    standby1.append_conf("primary_slot_name = standby_1")
    standby1.append_conf("wal_receiver_status_interval = 1")
    standby1.append_conf("max_replication_slots = 4")
    standby1.restart()
    assert (
        standby1.psql_capture(
            "SELECT pg_create_physical_replication_slot('standby_2');"
        ).rc
        == 0
    ), "physical slot created on intermediate replica"
    standby2.append_conf("primary_slot_name = standby_2")
    standby2.append_conf("wal_receiver_status_interval = 1")
    standby2.reload()
    xmin, cat = _get_slot_xmins(
        primary, "standby_1", "xmin IS NULL AND catalog_xmin IS NULL"
    )
    assert xmin == "" and cat == "", "non-cascaded slot null with no hs_feedback"
    xmin, cat = _get_slot_xmins(
        standby1, "standby_2", "xmin IS NULL AND catalog_xmin IS NULL"
    )
    assert xmin == "" and cat == "", "cascaded slot null with no hs_feedback"
    primary.safe_psql("CREATE TABLE replayed(val integer);")
    _hs_feedback_xmin(primary, standby1, standby2)


def _replay_check(primary, standby1, standby2):
    newval = primary.safe_psql(
        "INSERT INTO replayed(val) SELECT coalesce(max(val),0) + 1 AS newval "
        "FROM replayed RETURNING val"
    )
    primary.wait_for_replay_catchup(standby1)
    standby1.wait_for_replay_catchup(standby2, primary)
    assert standby1.safe_psql("SELECT 1 FROM replayed WHERE val = {}".format(newval))
    assert standby2.safe_psql("SELECT 1 FROM replayed WHERE val = {}".format(newval))
    return newval


def _hs_feedback_xmin(primary, standby1, standby2):
    _replay_check(primary, standby1, standby2)
    for node in (standby1, standby2):
        assert (
            node.safe_psql(
                "SELECT evtname FROM pg_event_trigger WHERE evtevent = 'login'"
            )
            == "on_login_trigger"
        ), "Name of login trigger"
    for node in (standby1, standby2):
        node.safe_psql("ALTER SYSTEM SET hot_standby_feedback = on;")
        node.reload()
    _replay_check(primary, standby1, standby2)
    xmin, cat = _get_slot_xmins(
        primary, "standby_1", "xmin IS NOT NULL AND catalog_xmin IS NULL"
    )
    assert xmin != "" and cat == "", "non-cascaded slot non-null with hs feedback"
    xmin1, cat1 = _get_slot_xmins(
        standby1, "standby_2", "xmin IS NOT NULL AND catalog_xmin IS NULL"
    )
    assert xmin1 != "" and cat1 == "", "cascaded slot non-null with hs feedback"
    primary.safe_psql(
        "do $$\nbegin\n  for i in 10000..11000 loop\n    begin\n"
        "      insert into tab_int values (i);\n    exception\n"
        "      when division_by_zero then null;\n    end;\n  end loop;\nend$$;"
    )
    primary.safe_psql("VACUUM;")
    primary.safe_psql("CHECKPOINT;")
    xmin2, cat2 = _get_slot_xmins(primary, "standby_1", "xmin <> '{}'".format(xmin))
    assert xmin2 != xmin and cat2 == "", "non-cascaded slot xmin changed"
    xmin2, cat2 = _get_slot_xmins(standby1, "standby_2", "xmin <> '{}'".format(xmin1))
    assert xmin2 != xmin1 and cat2 == "", "cascaded slot xmin changed"
    for node in (standby1, standby2):
        node.safe_psql("ALTER SYSTEM SET hot_standby_feedback = off;")
        node.reload()
    _replay_check(primary, standby1, standby2)
    xmin, cat = _get_slot_xmins(
        primary, "standby_1", "xmin IS NULL AND catalog_xmin IS NULL"
    )
    assert xmin == "" and cat == "", "non-cascaded slot null with hs feedback reset"
    xmin, cat = _get_slot_xmins(
        standby1, "standby_2", "xmin IS NULL AND catalog_xmin IS NULL"
    )
    assert xmin == "" and cat == "", "cascaded slot null with hs feedback reset"


def _physical_slot_advance(primary, standby1, standby2):
    standby2.append_conf("primary_slot_name = ''")
    standby2.enable_streaming(primary)
    standby2.reload()
    assert (
        standby1.safe_psql(
            "SELECT sum(writes) > 0 FROM pg_stat_io WHERE backend_type = "
            "'walreceiver' AND object = 'wal'"
        )
        == "t"
    ), "WAL receiver generates statistics for WAL writes"
    standby1.stop()
    newval = primary.safe_psql(
        "INSERT INTO replayed(val) SELECT coalesce(max(val),0) + 1 AS newval "
        "FROM replayed RETURNING val"
    )
    primary.wait_for_catchup(standby2)
    assert (
        standby2.safe_psql("SELECT 1 FROM replayed WHERE val = {}".format(newval))
        == "1"
    ), "standby_2 replayed primary value {}".format(newval)
    primary.safe_psql(
        "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots;"
    )
    phys_slot = "phys_slot"
    primary.safe_psql(
        "SELECT pg_create_physical_replication_slot('{}', true);".format(phys_slot)
    )
    segment_removed = primary.safe_psql("SELECT pg_walfile_name(pg_current_wal_lsn())")
    primary.advance_wal(1)
    current_lsn = primary.safe_psql("SELECT pg_current_wal_lsn();")
    assert (
        primary.psql_capture(
            "SELECT pg_replication_slot_advance('{}', '{}'::pg_lsn);".format(
                phys_slot, current_lsn
            )
        ).rc
        == 0
    ), "slot advancing with physical slot"
    pre = primary.safe_psql(
        "SELECT restart_lsn from pg_replication_slots WHERE slot_name = "
        "'{}';".format(phys_slot)
    )
    primary.restart()
    post = primary.safe_psql(
        "SELECT restart_lsn from pg_replication_slots WHERE slot_name = "
        "'{}';".format(phys_slot)
    )
    assert pre == post, "physical slot advance persists across restarts"
    assert not (
        primary.datadir / "pg_wal" / segment_removed
    ).is_file(), "WAL segment {} recycled after physical slot advancing".format(
        segment_removed
    )


def _base_backup_interlock(primary):
    connstr = primary.connstr("postgres") + " replication=database"
    primary.command_fails_like(
        [
            "psql",
            "--no-psqlrc",
            "--command",
            "SELECT pg_backup_start('backup', true)",
            "--command",
            "BASE_BACKUP",
            "--dbname",
            connstr,
        ],
        r"a backup is already in progress in this session",
        "BASE_BACKUP cannot run in session already running backup",
    )
    sess = primary.background_psql(
        "postgres", on_error_stop=False, replication="database"
    )
    sess.send(
        "BASE_BACKUP (CHECKPOINT 'fast', MAX_RATE 32);\nSELECT pg_backup_stop();\n"
    )
    assert primary.poll_query_until(
        "SELECT pg_cancel_backend(a.pid) FROM pg_stat_activity a, "
        "pg_stat_progress_basebackup b WHERE a.pid = b.pid AND "
        "a.query ~ 'BASE_BACKUP' AND b.phase = 'streaming database files';",
        "t",
    ), "WAL sender sending base backup killed"
    sess.wait_for_stderr(r"backup is not in progress")
    sess.quit()
