# Copyright (c) 2022-2026, PostgreSQL Global Development Group

# pylint: disable=line-too-long,too-many-statements
"""Port of src/test/modules/worker_spi/t/001_worker_spi.pl.

worker_spi dynamic and preloaded background workers: workers launch via worker_spi_launch and as shared_preload_libraries entries, create their schema/table, perform their periodic work, and respect database/role arguments.
Generated from the Perl original via .agent/gen_golden.py.
"""


def test_001_worker_spi(create_pg):
    """worker_spi dynamic and preloaded background workers."""
    node = create_pg("mynode", start=False)
    node.start()
    node.safe_psql("CREATE EXTENSION worker_spi;")
    result = node.safe_psql("SELECT worker_spi_launch(4) IS NOT NULL;")
    assert result == "t", "dynamic bgworker launched"
    node.poll_query_until(
        "SELECT count(*) > 0 FROM information_schema.tables\n\t    WHERE table_schema = 'schema4' AND table_name = 'counted';"
    )
    node.safe_psql("INSERT INTO schema4.counted VALUES ('total', 0), ('delta', 1);")
    node.reload()
    node.poll_query_until(
        "SELECT count(*) FROM schema4.counted WHERE type = 'delta';", expected="0"
    )
    result = node.safe_psql("SELECT * FROM schema4.counted;")
    assert result == "total|1", "dynamic bgworker correctly consumed tuple data"
    result = node.poll_query_until(
        "SELECT wait_event FROM pg_stat_activity WHERE backend_type ~ 'worker_spi';",
        expected="WorkerSpiMain",
    )
    assert result, 'dynamic bgworker has reported "WorkerSpiMain" as wait event'
    result = node.safe_psql(
        "SELECT count(*) > 0 from pg_wait_events where type = 'Extension' and name = 'WorkerSpiMain';"
    )
    assert result == "t", '"WorkerSpiMain" is reported in pg_wait_events'
    node.safe_psql("CREATE DATABASE mydb;")
    node.safe_psql("CREATE ROLE myrole SUPERUSER LOGIN;")
    node.safe_psql("CREATE EXTENSION worker_spi;", dbname="mydb")
    node.append_conf(
        "\nshared_preload_libraries = 'worker_spi'\nworker_spi.database = 'mydb'\nworker_spi.total_workers = 3\nmax_worker_processes = 32\n"
    )
    node.restart()
    assert node.poll_query_until(
        "SELECT datname, count(datname), wait_event FROM pg_stat_activity\n            WHERE backend_type = 'worker_spi' GROUP BY datname, wait_event;",
        expected="mydb|3|WorkerSpiMain",
    ), "poll_query_until"
    myrole_id = node.safe_psql(
        "SELECT oid FROM pg_roles where rolname = 'myrole';", dbname="mydb"
    )
    mydb_id = node.safe_psql(
        "SELECT oid FROM pg_database where datname = 'mydb';", dbname="mydb"
    )
    postgresdb_id = node.safe_psql(
        "SELECT oid FROM pg_database where datname = 'postgres';", dbname="mydb"
    )
    worker1_pid = node.safe_psql(
        "SELECT worker_spi_launch(10, " + str(mydb_id) + ", " + str(myrole_id) + ");",
        dbname="mydb",
    )
    worker2_pid = node.safe_psql(
        "SELECT worker_spi_launch(11, "
        + str(postgresdb_id)
        + ", "
        + str(myrole_id)
        + ");",
        dbname="mydb",
    )
    assert node.poll_query_until(
        "SELECT datname, usename, wait_event FROM pg_stat_activity\n            WHERE backend_type = 'worker_spi dynamic' AND\n            pid IN ("
        + str(worker1_pid)
        + ", "
        + str(worker2_pid)
        + ") ORDER BY datname;",
        expected="mydb|myrole|WorkerSpiMain\npostgres|myrole|WorkerSpiMain",
    ), "poll_query_until"
    node.safe_psql("CREATE DATABASE noconndb ALLOW_CONNECTIONS false;")
    noconndb_id = node.safe_psql(
        "SELECT oid FROM pg_database where datname = 'noconndb';", dbname="mydb"
    )
    log_offset = node.current_log_position()
    node.psql_capture(
        "SELECT worker_spi_launch(12, "
        + str(noconndb_id)
        + ", "
        + str(myrole_id)
        + ");"
    )
    node.wait_for_log(
        r"""database "noconndb" is not currently accepting connections""", log_offset
    )
    worker4_pid = node.safe_psql(
        "SELECT worker_spi_launch(12, "
        + str(noconndb_id)
        + ", "
        + str(myrole_id)
        + ", '{\"ALLOWCONN\"}');"
    )
    assert node.poll_query_until(
        "SELECT datname, usename, wait_event FROM pg_stat_activity\n            WHERE backend_type = 'worker_spi dynamic' AND\n            pid IN ("
        + str(worker4_pid)
        + ") ORDER BY datname;",
        expected="noconndb|myrole|WorkerSpiMain",
    ), "poll_query_until"
    node.safe_psql(
        "CREATE ROLE nologrole WITH NOLOGIN;\n  GRANT CREATE ON DATABASE mydb TO nologrole;"
    )
    nologrole_id = node.safe_psql(
        "SELECT oid FROM pg_roles where rolname = 'nologrole';", dbname="mydb"
    )
    log_offset = node.current_log_position()
    node.psql_capture(
        "SELECT worker_spi_launch(13, " + str(mydb_id) + ", " + str(nologrole_id) + ");"
    )
    node.wait_for_log(r"""role "nologrole" is not permitted to log in""", log_offset)
    log_offset = node.current_log_position()
    worker5_pid = node.safe_psql(
        "SELECT worker_spi_launch(13, "
        + str(mydb_id)
        + ", "
        + str(nologrole_id)
        + ", '{\"ROLELOGINCHECK\"}');",
        dbname="mydb",
    )
    assert node.poll_query_until(
        "SELECT datname, usename, wait_event FROM pg_stat_activity\n            WHERE backend_type = 'worker_spi dynamic' AND\n            pid = "
        + str(worker5_pid)
        + ";",
        expected="mydb|nologrole|WorkerSpiMain",
    ), "poll_query_until"
