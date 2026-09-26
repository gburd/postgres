# Copyright (c) 2024-2026, PostgreSQL Global Development Group

# pylint: disable=line-too-long
"""Port of src/test/modules/test_autovacuum/t/001_parallel_autovacuum.pl.

Parallel autovacuum: a table configured with autovacuum_parallel_workers runs
its index vacuum phase with the expected number of parallel workers, and a
cost-parameter change made while a parallel autovacuum is paused (via an
injection point) is propagated to the already-launched parallel workers.
Requires an injection-points build.
"""

import os

import pytest


def _prepare_for_next_test(node, test_number):
    node.safe_psql(
        "ALTER TABLE test_autovac SET (autovacuum_enabled = false);\n"
        "UPDATE test_autovac SET col_1 = {};".format(test_number)
    )


def test_001_parallel_autovacuum(create_pg):
    """Parallel autovacuum launches workers and propagates cost-param changes."""
    if os.environ.get("enable_injection_points") != "yes":
        pytest.skip("Injection points not supported by this build")
    node = create_pg("main", start=False)
    node.append_conf(
        "\nautovacuum_max_workers = 1\nautovacuum_worker_slots = 1\n"
        "autovacuum_max_parallel_workers = 2\nmax_worker_processes = 10\n"
        "max_parallel_workers = 10\nlog_min_messages = debug2\n"
        "autovacuum_naptime = '1s'\nmin_parallel_index_scan_size = 0\n"
        "log_autovacuum_min_duration = -1\n"
    )
    node.start()
    if not node.check_extension("injection_points"):
        pytest.skip("Extension injection_points not installed")
    node.safe_psql("CREATE EXTENSION injection_points;")
    indexes_num = 3
    initial_rows_num = 10_000
    autovacuum_parallel_workers = 2
    node.safe_psql(
        "CREATE TABLE test_autovac (\n"
        "    id SERIAL PRIMARY KEY,\n"
        "    col_1 INTEGER,  col_2 INTEGER,  col_3 INTEGER,  col_4 INTEGER\n"
        ") WITH (autovacuum_parallel_workers = {},\n"
        "        log_autovacuum_min_duration = 0);\n"
        "INSERT INTO test_autovac\n"
        "SELECT g AS col1, g + 1 AS col2, g + 2 AS col3, g + 3 AS col4\n"
        "FROM generate_series(1, {}) AS g;".format(
            autovacuum_parallel_workers, initial_rows_num
        )
    )
    node.safe_psql(
        "DO $$\n"
        "DECLARE\n"
        "    i INTEGER;\n"
        "BEGIN\n"
        "    FOR i IN 1..{} LOOP\n"
        "        EXECUTE format('CREATE INDEX idx_col_%s ON test_autovac "
        "(col_%s);', i, i);\n"
        "    END LOOP;\n"
        "END $$;".format(indexes_num)
    )
    _prepare_for_next_test(node, 1)
    log_offset = node.current_log_position()
    node.safe_psql("ALTER TABLE test_autovac SET (autovacuum_enabled = true);")
    node.wait_for_log(
        r"parallel workers: index vacuum: 2 planned, 2 launched in total",
        log_offset,
    )
    _prepare_for_next_test(node, 2)
    log_offset = node.current_log_position()
    node.safe_psql(
        "SELECT injection_points_attach('autovacuum-start-parallel-vacuum', "
        "'wait');\n"
        "ALTER TABLE test_autovac SET (autovacuum_parallel_workers = 1, "
        "autovacuum_enabled = true);"
    )
    node.wait_for_event("autovacuum worker", "autovacuum-start-parallel-vacuum")
    node.safe_psql(
        "ALTER SYSTEM SET autovacuum_vacuum_cost_limit = 500;\n"
        "ALTER SYSTEM SET autovacuum_vacuum_cost_delay = 5;\n"
        "ALTER SYSTEM SET vacuum_cost_page_miss = 10;\n"
        "ALTER SYSTEM SET vacuum_cost_page_dirty = 10;\n"
        "ALTER SYSTEM SET vacuum_cost_page_hit = 10;\n"
        "SELECT pg_reload_conf();"
    )
    node.safe_psql(
        "SELECT injection_points_wakeup('autovacuum-start-parallel-vacuum');"
    )
    node.wait_for_log(
        r"parallel autovacuum worker updated cost params: cost_limit=500, "
        r"cost_delay=5, cost_page_miss=10, cost_page_dirty=10, cost_page_hit=10",
        log_offset,
    )
    node.safe_psql(
        "SELECT injection_points_detach('autovacuum-start-parallel-vacuum');"
    )
    node.stop()
