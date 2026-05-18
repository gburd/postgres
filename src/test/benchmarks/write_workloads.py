"""
Write workload patterns for RECNO vs HEAP benchmarking.

These workloads exercise INSERT, UPDATE, DELETE, ROLLBACK, VACUUM,
and TOAST/overflow paths -- the areas where RECNO's in-place update
and UNDO-based rollback should show the most difference from heap.

Usage:
    from write_workloads import WRITE_WORKLOADS, run_write_workload

Each workload is a dict with:
    name        - human-readable label
    setup_sql   - SQL to create and populate tables (heap + recno)
    workload_sql- SQL to run the actual workload (parameterized with {am})
    measure_sql - SQL to collect metrics after the workload
    cleanup_sql - SQL to drop tables
    description - what this tests and why
"""

import time
import logging

logger = logging.getLogger(__name__)

# Default row counts -- tuned for ~5 min total across all workloads
DEFAULT_ROWS = 500_000
LARGE_ROWS = 1_000_000
SMALL_ROWS = 10_000


def _make_workload(name, description, setup, workload, measure, cleanup,
                   row_count=DEFAULT_ROWS):
    return {
        "name": name,
        "description": description,
        "row_count": row_count,
        "setup_sql": setup,
        "workload_sql": workload,
        "measure_sql": measure,
        "cleanup_sql": cleanup,
    }


WRITE_WORKLOADS = [

    # ----------------------------------------------------------------
    # 1. BULK INSERT -- large batch via INSERT...SELECT
    # ----------------------------------------------------------------
    _make_workload(
        name="bulk_insert",
        description=(
            "Insert N rows via INSERT...SELECT generate_series. "
            "Tests raw insertion throughput and page allocation."
        ),
        row_count=LARGE_ROWS,
        setup="DROP TABLE IF EXISTS bench_{am}; "
              "CREATE TABLE bench_{am} (id bigint, val int, data text) USING {am};",
        workload=(
            "INSERT INTO bench_{am} (id, val, data) "
            "SELECT g, g % 1000, repeat('x', 60) "
            "FROM generate_series(1, {rows}) g;"
        ),
        measure=(
            "SELECT pg_relation_size('bench_{am}') AS rel_size, "
            "       pg_total_relation_size('bench_{am}') AS total_size, "
            "       (SELECT count(*) FROM bench_{am}) AS row_count;"
        ),
        cleanup="DROP TABLE IF EXISTS bench_{am};",
    ),

    # ----------------------------------------------------------------
    # 2. INDIVIDUAL INSERT -- one row at a time (PL/pgSQL loop)
    # ----------------------------------------------------------------
    _make_workload(
        name="individual_insert",
        description=(
            "Insert rows one at a time in a PL/pgSQL loop. "
            "Tests per-row overhead (WAL, UNDO, buffer management)."
        ),
        row_count=SMALL_ROWS,
        setup="DROP TABLE IF EXISTS bench_{am}; "
              "CREATE TABLE bench_{am} (id bigint, val int, data text) USING {am};",
        workload=(
            "DO $$ BEGIN "
            "FOR i IN 1..{rows} LOOP "
            "  INSERT INTO bench_{am} VALUES (i, i % 100, 'row_' || i); "
            "END LOOP; END $$;"
        ),
        measure=(
            "SELECT pg_relation_size('bench_{am}') AS rel_size, "
            "       (SELECT count(*) FROM bench_{am}) AS row_count;"
        ),
        cleanup="DROP TABLE IF EXISTS bench_{am};",
    ),

    # ----------------------------------------------------------------
    # 3. IN-PLACE UPDATE -- repeated full-table update (RECNO strength)
    # ----------------------------------------------------------------
    _make_workload(
        name="in_place_update",
        description=(
            "10 rounds of UPDATE all rows SET counter = counter + 1. "
            "RECNO does in-place updates; heap creates dead tuples. "
            "This is RECNO's primary advantage."
        ),
        row_count=DEFAULT_ROWS,
        setup=(
            "DROP TABLE IF EXISTS bench_{am}; "
            "CREATE TABLE bench_{am} (id bigint PRIMARY KEY, counter int DEFAULT 0) USING {am}; "
            "INSERT INTO bench_{am} (id) SELECT g FROM generate_series(1, {rows}) g;"
        ),
        workload=(
            "UPDATE bench_{am} SET counter = counter + 1; "
            "UPDATE bench_{am} SET counter = counter + 1; "
            "UPDATE bench_{am} SET counter = counter + 1; "
            "UPDATE bench_{am} SET counter = counter + 1; "
            "UPDATE bench_{am} SET counter = counter + 1; "
            "UPDATE bench_{am} SET counter = counter + 1; "
            "UPDATE bench_{am} SET counter = counter + 1; "
            "UPDATE bench_{am} SET counter = counter + 1; "
            "UPDATE bench_{am} SET counter = counter + 1; "
            "UPDATE bench_{am} SET counter = counter + 1; "
        ),
        measure=(
            "SELECT pg_relation_size('bench_{am}') AS rel_size, "
            "       pg_total_relation_size('bench_{am}') AS total_size, "
            "       n_dead_tup, n_live_tup "
            "FROM pg_stat_user_tables WHERE relname = 'bench_{am}';"
        ),
        cleanup="DROP TABLE IF EXISTS bench_{am};",
    ),

    # ----------------------------------------------------------------
    # 4. TARGETED UPDATE -- update 10% of rows per round
    # ----------------------------------------------------------------
    _make_workload(
        name="targeted_update",
        description=(
            "5 rounds updating 10% of rows (WHERE id %% 10 = 0). "
            "Tests partial update with index."
        ),
        row_count=DEFAULT_ROWS,
        setup=(
            "DROP TABLE IF EXISTS bench_{am}; "
            "CREATE TABLE bench_{am} (id bigint PRIMARY KEY, status text DEFAULT 'active') USING {am}; "
            "INSERT INTO bench_{am} (id) SELECT g FROM generate_series(1, {rows}) g;"
        ),
        workload=(
            "UPDATE bench_{am} SET status = 'round1' WHERE id %% 10 = 0; "
            "UPDATE bench_{am} SET status = 'round2' WHERE id %% 10 = 0; "
            "UPDATE bench_{am} SET status = 'round3' WHERE id %% 10 = 0; "
            "UPDATE bench_{am} SET status = 'round4' WHERE id %% 10 = 0; "
            "UPDATE bench_{am} SET status = 'round5' WHERE id %% 10 = 0; "
        ),
        measure=(
            "SELECT pg_relation_size('bench_{am}') AS rel_size, "
            "       n_dead_tup, n_live_tup "
            "FROM pg_stat_user_tables WHERE relname = 'bench_{am}';"
        ),
        cleanup="DROP TABLE IF EXISTS bench_{am};",
    ),

    # ----------------------------------------------------------------
    # 5. DELETE HALF -- delete 50% of rows, measure dead tuples
    # ----------------------------------------------------------------
    _make_workload(
        name="delete_half",
        description=(
            "Delete 50% of rows and measure dead tuple count. "
            "Heap accumulates dead tuples; RECNO uses UNDO tombstones."
        ),
        row_count=DEFAULT_ROWS,
        setup=(
            "DROP TABLE IF EXISTS bench_{am}; "
            "CREATE TABLE bench_{am} (id bigint, data text) USING {am}; "
            "INSERT INTO bench_{am} SELECT g, repeat('d', 80) FROM generate_series(1, {rows}) g;"
        ),
        workload="DELETE FROM bench_{am} WHERE id %% 2 = 0;",
        measure=(
            "SELECT pg_relation_size('bench_{am}') AS rel_size, "
            "       n_dead_tup, n_live_tup, "
            "       (SELECT count(*) FROM bench_{am}) AS remaining "
            "FROM pg_stat_user_tables WHERE relname = 'bench_{am}';"
        ),
        cleanup="DROP TABLE IF EXISTS bench_{am};",
    ),

    # ----------------------------------------------------------------
    # 6. VACUUM AFTER DELETE -- full delete + VACUUM cycle
    # ----------------------------------------------------------------
    _make_workload(
        name="vacuum_cycle",
        description=(
            "Delete 50% of rows then VACUUM. Measures VACUUM overhead. "
            "RECNO should have less VACUUM work (UNDO handles rollback)."
        ),
        row_count=DEFAULT_ROWS,
        setup=(
            "DROP TABLE IF EXISTS bench_{am}; "
            "CREATE TABLE bench_{am} (id bigint, data text) USING {am}; "
            "INSERT INTO bench_{am} SELECT g, repeat('v', 80) FROM generate_series(1, {rows}) g;"
        ),
        workload=(
            "DELETE FROM bench_{am} WHERE id %% 2 = 0; "
            "VACUUM bench_{am};"
        ),
        measure=(
            "SELECT pg_relation_size('bench_{am}') AS rel_size_after_vacuum, "
            "       n_dead_tup, n_live_tup "
            "FROM pg_stat_user_tables WHERE relname = 'bench_{am}';"
        ),
        cleanup="DROP TABLE IF EXISTS bench_{am};",
    ),

    # ----------------------------------------------------------------
    # 7. ROLLBACK SMALL -- rollback a small transaction
    # ----------------------------------------------------------------
    _make_workload(
        name="rollback_small",
        description=(
            "BEGIN; INSERT 1000 rows; ROLLBACK. "
            "Tests UNDO-based rollback cost for small transactions."
        ),
        row_count=1000,
        setup=(
            "DROP TABLE IF EXISTS bench_{am}; "
            "CREATE TABLE bench_{am} (id bigint, data text) USING {am}; "
            "INSERT INTO bench_{am} SELECT g, 'base' FROM generate_series(1, 100) g;"
        ),
        workload=(
            "BEGIN; "
            "INSERT INTO bench_{am} SELECT g, repeat('r', 80) "
            "FROM generate_series(1000, 1999) g; "
            "ROLLBACK;"
        ),
        measure=(
            "SELECT (SELECT count(*) FROM bench_{am}) AS row_count, "
            "       pg_relation_size('bench_{am}') AS rel_size;"
        ),
        cleanup="DROP TABLE IF EXISTS bench_{am};",
    ),

    # ----------------------------------------------------------------
    # 8. ROLLBACK LARGE -- rollback a large transaction
    # ----------------------------------------------------------------
    _make_workload(
        name="rollback_large",
        description=(
            "BEGIN; INSERT 500K rows; ROLLBACK. "
            "Tests ATM instant abort for large UNDO. "
            "RECNO with ATM should be nearly instant."
        ),
        row_count=DEFAULT_ROWS,
        setup=(
            "DROP TABLE IF EXISTS bench_{am}; "
            "CREATE TABLE bench_{am} (id bigint, data text) USING {am};"
        ),
        workload=(
            "BEGIN; "
            "INSERT INTO bench_{am} SELECT g, repeat('R', 80) "
            "FROM generate_series(1, {rows}) g; "
            "ROLLBACK;"
        ),
        measure=(
            "SELECT (SELECT count(*) FROM bench_{am}) AS row_count, "
            "       pg_relation_size('bench_{am}') AS rel_size;"
        ),
        cleanup="DROP TABLE IF EXISTS bench_{am};",
    ),

    # ----------------------------------------------------------------
    # 9. UPDATE BLOAT -- measure storage growth over repeated updates
    # ----------------------------------------------------------------
    _make_workload(
        name="update_bloat",
        description=(
            "20 rounds of full-table UPDATE, measuring relation size "
            "after each round. Heap bloats; RECNO stays compact."
        ),
        row_count=100_000,
        setup=(
            "DROP TABLE IF EXISTS bench_{am}; "
            "CREATE TABLE bench_{am} (id bigint, counter int DEFAULT 0, "
            "  pad text DEFAULT repeat('b', 40)) USING {am}; "
            "INSERT INTO bench_{am} (id) SELECT g FROM generate_series(1, {rows}) g;"
        ),
        workload=(
            "UPDATE bench_{am} SET counter = counter + 1; "
            "UPDATE bench_{am} SET counter = counter + 1; "
            "UPDATE bench_{am} SET counter = counter + 1; "
            "UPDATE bench_{am} SET counter = counter + 1; "
            "UPDATE bench_{am} SET counter = counter + 1; "
            "UPDATE bench_{am} SET counter = counter + 1; "
            "UPDATE bench_{am} SET counter = counter + 1; "
            "UPDATE bench_{am} SET counter = counter + 1; "
            "UPDATE bench_{am} SET counter = counter + 1; "
            "UPDATE bench_{am} SET counter = counter + 1; "
            "UPDATE bench_{am} SET counter = counter + 1; "
            "UPDATE bench_{am} SET counter = counter + 1; "
            "UPDATE bench_{am} SET counter = counter + 1; "
            "UPDATE bench_{am} SET counter = counter + 1; "
            "UPDATE bench_{am} SET counter = counter + 1; "
            "UPDATE bench_{am} SET counter = counter + 1; "
            "UPDATE bench_{am} SET counter = counter + 1; "
            "UPDATE bench_{am} SET counter = counter + 1; "
            "UPDATE bench_{am} SET counter = counter + 1; "
            "UPDATE bench_{am} SET counter = counter + 1; "
        ),
        measure=(
            "SELECT pg_relation_size('bench_{am}') AS rel_size, "
            "       pg_total_relation_size('bench_{am}') AS total_size, "
            "       n_dead_tup, n_live_tup "
            "FROM pg_stat_user_tables WHERE relname = 'bench_{am}';"
        ),
        cleanup="DROP TABLE IF EXISTS bench_{am};",
    ),

    # ----------------------------------------------------------------
    # 10. TOAST vs OVERFLOW -- large column storage comparison
    # ----------------------------------------------------------------
    _make_workload(
        name="toast_overflow",
        description=(
            "Insert rows with large text columns (1KB to 100KB). "
            "Heap uses TOAST; RECNO uses overflow records. "
            "Measures storage efficiency and retrieval speed."
        ),
        row_count=10_000,
        setup=(
            "DROP TABLE IF EXISTS bench_{am}; "
            "CREATE TABLE bench_{am} (id bigint, small_text text, "
            "  large_text text) USING {am};"
        ),
        workload=(
            "INSERT INTO bench_{am} "
            "SELECT g, 'small_' || g, repeat(chr(65 + (g %% 26)), 1000 + (g %% 99000)) "
            "FROM generate_series(1, {rows}) g;"
        ),
        measure=(
            "SELECT pg_relation_size('bench_{am}') AS main_size, "
            "       pg_total_relation_size('bench_{am}') AS total_size, "
            "       (SELECT count(*) FROM bench_{am}) AS row_count, "
            "       (SELECT avg(length(large_text)) FROM bench_{am}) AS avg_col_len;"
        ),
        cleanup="DROP TABLE IF EXISTS bench_{am};",
    ),

    # ----------------------------------------------------------------
    # 11. TOAST/OVERFLOW UPDATE -- update large columns
    # ----------------------------------------------------------------
    _make_workload(
        name="toast_overflow_update",
        description=(
            "Update large text columns in-place. RECNO should avoid "
            "rewriting the entire TOAST chain for small changes."
        ),
        row_count=5_000,
        setup=(
            "DROP TABLE IF EXISTS bench_{am}; "
            "CREATE TABLE bench_{am} (id bigint PRIMARY KEY, "
            "  status text DEFAULT 'active', "
            "  large_data text) USING {am}; "
            "INSERT INTO bench_{am} "
            "SELECT g, 'active', repeat('D', 5000) "
            "FROM generate_series(1, {rows}) g;"
        ),
        workload=(
            "UPDATE bench_{am} SET status = 'updated_1'; "
            "UPDATE bench_{am} SET status = 'updated_2'; "
            "UPDATE bench_{am} SET status = 'updated_3'; "
        ),
        measure=(
            "SELECT pg_relation_size('bench_{am}') AS main_size, "
            "       pg_total_relation_size('bench_{am}') AS total_size, "
            "       n_dead_tup "
            "FROM pg_stat_user_tables WHERE relname = 'bench_{am}';"
        ),
        cleanup="DROP TABLE IF EXISTS bench_{am};",
    ),

    # ----------------------------------------------------------------
    # 12. SEQUENTIAL SCAN AFTER UPDATES -- read perf post-bloat
    # ----------------------------------------------------------------
    _make_workload(
        name="scan_after_updates",
        description=(
            "After 10 rounds of updates, measure sequential scan speed. "
            "Heap must skip dead tuples; RECNO has cleaner pages."
        ),
        row_count=DEFAULT_ROWS,
        setup=(
            "DROP TABLE IF EXISTS bench_{am}; "
            "CREATE TABLE bench_{am} (id bigint, counter int DEFAULT 0) USING {am}; "
            "INSERT INTO bench_{am} (id) SELECT g FROM generate_series(1, {rows}) g; "
            "UPDATE bench_{am} SET counter = counter + 1; "
            "UPDATE bench_{am} SET counter = counter + 1; "
            "UPDATE bench_{am} SET counter = counter + 1; "
            "UPDATE bench_{am} SET counter = counter + 1; "
            "UPDATE bench_{am} SET counter = counter + 1; "
            "UPDATE bench_{am} SET counter = counter + 1; "
            "UPDATE bench_{am} SET counter = counter + 1; "
            "UPDATE bench_{am} SET counter = counter + 1; "
            "UPDATE bench_{am} SET counter = counter + 1; "
            "UPDATE bench_{am} SET counter = counter + 1; "
        ),
        workload="SELECT count(*), sum(counter) FROM bench_{am};",
        measure=(
            "SELECT pg_relation_size('bench_{am}') AS rel_size, "
            "       n_dead_tup "
            "FROM pg_stat_user_tables WHERE relname = 'bench_{am}';"
        ),
        cleanup="DROP TABLE IF EXISTS bench_{am};",
    ),
]


async def run_write_workload(conn, workload, am, iterations=3):
    """Run a single write workload and return timing + metrics.

    Args:
        conn: asyncpg connection
        workload: dict from WRITE_WORKLOADS
        am: 'heap' or 'recno'
        iterations: number of times to repeat for averaging

    Returns:
        dict with timing_ms (list), metrics (from measure_sql)
    """
    results = {
        "name": workload["name"],
        "am": am,
        "row_count": workload["row_count"],
        "timings_ms": [],
        "metrics": None,
    }

    rows = workload["row_count"]

    for iteration in range(iterations):
        # Setup
        setup = workload["setup_sql"].format(am=am, rows=rows)
        for stmt in setup.split(";"):
            stmt = stmt.strip()
            if stmt:
                await conn.execute(stmt)

        # Reset stats
        await conn.execute(
            "SELECT pg_stat_reset_single_table_counters("
            f"'bench_{am}'::regclass)"
        )

        # Run workload with timing
        wl = workload["workload_sql"].format(am=am, rows=rows)
        start = time.perf_counter()
        for stmt in wl.split(";"):
            stmt = stmt.strip()
            if stmt:
                await conn.execute(stmt)
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        results["timings_ms"].append(elapsed_ms)

        # Collect metrics (only on last iteration)
        if iteration == iterations - 1:
            measure = workload["measure_sql"].format(am=am, rows=rows)
            row = await conn.fetchrow(measure)
            if row:
                results["metrics"] = dict(row)

        # Cleanup between iterations (except last)
        if iteration < iterations - 1:
            cleanup = workload["cleanup_sql"].format(am=am)
            for stmt in cleanup.split(";"):
                stmt = stmt.strip()
                if stmt:
                    await conn.execute(stmt)

    # Final cleanup
    cleanup = workload["cleanup_sql"].format(am=am)
    for stmt in cleanup.split(";"):
        stmt = stmt.strip()
        if stmt:
            await conn.execute(stmt)

    return results


def format_write_results(heap_results, recno_results):
    """Format a comparison table from paired heap/recno results."""
    import statistics

    lines = []
    lines.append(f"\n{'Workload':<25} {'Heap (ms)':<15} {'RECNO (ms)':<15} {'Speedup':<10} {'Notes'}")
    lines.append("-" * 85)

    for h, r in zip(heap_results, recno_results):
        h_med = statistics.median(h["timings_ms"])
        r_med = statistics.median(r["timings_ms"])
        speedup = h_med / r_med if r_med > 0 else float('inf')

        notes = ""
        if h.get("metrics") and r.get("metrics"):
            hm = h["metrics"]
            rm = r["metrics"]
            if "rel_size" in hm and "rel_size" in rm:
                h_sz = hm.get("rel_size") or hm.get("main_size", 0)
                r_sz = rm.get("rel_size") or rm.get("main_size", 0)
                if h_sz and r_sz:
                    ratio = h_sz / r_sz if r_sz > 0 else 0
                    notes = f"size ratio: {ratio:.1f}x"
            if "n_dead_tup" in hm:
                notes += f" heap_dead={hm['n_dead_tup']}"

        lines.append(
            f"{h['name']:<25} {h_med:>12.1f}   {r_med:>12.1f}   {speedup:>7.2f}x  {notes}"
        )

    return "\n".join(lines)
