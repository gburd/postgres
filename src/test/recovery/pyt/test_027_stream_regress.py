# Copyright (c) 2021-2026, PostgreSQL Global Development Group

# pylint: disable=line-too-long
"""Port of src/test/recovery/t/027_stream_regress.pl.

Runs the core regression suite against a streaming primary, replicates to a
standby, and verifies the two stay logically identical: full pg_dumpall outputs
match, pg_catalog dumps match after quiescing, and pg_stat_statements on the
primary recorded the expected statement categories.
"""

import os

import pypg


def test_027_stream_regress(create_pg, pg_bin):
    """Regression suite replicates faithfully; primary/standby dumps match."""
    primary = create_pg("primary", allows_streaming=True, start=False)
    primary.adjust_conf("max_connections", "25")
    primary.append_conf("max_prepared_transactions = 10")
    primary.append_conf(
        "shared_preload_libraries = 'pg_stat_statements'\n"
        "pg_stat_statements.max = 50000\ncompute_query_id = 'regress'\n"
    )
    primary.append_conf("synchronize_seqscans = off")
    if "wal_consistency_checking" in os.environ.get("PG_TEST_EXTRA", "").split():
        primary.append_conf("wal_consistency_checking = all")
    primary.start()
    assert (
        primary.psql_capture(
            "SELECT pg_create_physical_replication_slot('standby_1');"
        ).exit_code
        == 0
    ), "physical slot created on primary"
    backup_name = "my_backup"
    primary.backup(backup_name)
    standby = create_pg(
        "standby_1", from_backup=(primary, backup_name), has_streaming=True, start=False
    )
    standby.append_conf("primary_slot_name = standby_1")
    standby.append_conf("max_standby_streaming_delay = 600s")
    standby.start()
    dlpath = os.path.dirname(os.environ["REGRESS_SHLIB"])
    outputdir = str(_tmp_check())
    regress_in = os.path.join(os.path.dirname(__file__), "..", "..", "regress")
    extra_opts = os.environ.get("EXTRA_REGRESS_OPTS", "")
    cmd = (
        [os.environ["PG_REGRESS"]]
        + extra_opts.split()
        + [
            "--dlpath=" + dlpath,
            "--bindir=",
            "--host=" + str(primary.host),
            "--port=" + str(primary.port),
            "--schedule=" + os.path.join(regress_in, "parallel_schedule"),
            "--max-concurrent-tests=20",
            "--inputdir=" + regress_in,
            "--outputdir=" + outputdir,
        ]
    )
    pg_bin.command_ok(cmd, "regression tests pass")
    assert primary.is_alive(), "primary alive after regression test run"
    assert standby.is_alive(), "standby alive after regression test run"
    primary.psql_capture(
        "select setval(seqrelid, nextval(seqrelid)) from pg_sequence",
        dbname="regression",
    )
    primary.wait_for_replay_catchup(standby)
    pg_bin.command_ok(
        [
            "pg_dumpall",
            "--file",
            outputdir + "/primary.dump",
            "--no-sync",
            "--no-statistics",
            "--restrict-key",
            "test",
            "--host",
            str(primary.host),
            "--port",
            str(primary.port),
            "--no-unlogged-table-data",
        ],
        "dump primary server",
    )
    pg_bin.command_ok(
        [
            "pg_dumpall",
            "--file",
            outputdir + "/standby.dump",
            "--no-sync",
            "--no-statistics",
            "--restrict-key",
            "test",
            "--host",
            str(standby.host),
            "--port",
            str(standby.port),
        ],
        "dump standby server",
    )
    pypg.compare_files(
        outputdir + "/primary.dump",
        outputdir + "/standby.dump",
        "compare primary and standby dumps",
    )
    primary.append_conf("autovacuum = off")
    primary.restart()
    primary.wait_for_replay_catchup(standby)
    pg_bin.command_ok(
        [
            "pg_dump",
            "--schema",
            "pg_catalog",
            "--file",
            outputdir + "/catalogs_primary.dump",
            "--no-sync",
            "--restrict-key",
            "test",
            "--host",
            str(primary.host),
            "--port",
            str(primary.port),
            "--no-unlogged-table-data",
            "regression",
        ],
        "dump catalogs of primary server",
    )
    pg_bin.command_ok(
        [
            "pg_dump",
            "--schema",
            "pg_catalog",
            "--file",
            outputdir + "/catalogs_standby.dump",
            "--no-sync",
            "--restrict-key",
            "test",
            "--host",
            str(standby.host),
            "--port",
            str(standby.port),
            "regression",
        ],
        "dump catalogs of standby server",
    )
    pypg.compare_files(
        outputdir + "/catalogs_primary.dump",
        outputdir + "/catalogs_standby.dump",
        "compare primary and standby catalog dumps",
    )
    primary.safe_psql("CREATE EXTENSION pg_stat_statements")
    result = primary.safe_psql(
        "WITH select_stats AS\n"
        "  (SELECT upper(substr(query, 1, 6)) AS select_query\n"
        "     FROM pg_stat_statements\n"
        "     WHERE upper(substr(query, 1, 6)) IN ('SELECT', 'UPDATE',\n"
        "                                          'INSERT', 'DELETE',\n"
        "                                          'CREATE'))\n"
        "  SELECT select_query, count(select_query) > 1 AS some_rows\n"
        "    FROM select_stats\n"
        "    GROUP BY select_query ORDER BY select_query;"
    )
    assert (
        result == "CREATE|t\nDELETE|t\nINSERT|t\nSELECT|t\nUPDATE|t"
    ), "check contents of pg_stat_statements on regression database"
    standby.stop()
    primary.stop()


def _tmp_check():
    import tempfile  # pylint: disable=import-outside-toplevel

    return tempfile.mkdtemp(prefix="streamregress_")
