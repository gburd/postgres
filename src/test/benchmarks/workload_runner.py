"""
Workload runner: executes query patterns against HEAP and Noxu tables,
collecting timing and EXPLAIN ANALYZE data.  Also supports multi-strategy
buffer-pool workloads.
"""

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from .config import BufferStrategy, ColumnType, PoolConfig, QueryPattern, TableSchema
from .database import DatabaseManager

logger = logging.getLogger(__name__)


@dataclass
class QueryResult:
    """Result of a single query execution."""
    query_pattern: str
    table_name: str
    storage_method: str  # "heap" or "noxu"
    query_sql: str
    elapsed_seconds: float
    row_count: int = 0
    explain_plan: Optional[Dict[str, Any]] = None


@dataclass
class WorkloadResult:
    """Aggregated results for a complete workload run."""
    schema_name: str
    row_count: int
    distribution: str
    storage_method: str
    results: List[QueryResult] = field(default_factory=list)

    def add(self, result: QueryResult):
        self.results.append(result)


class WorkloadRunner:
    """Generates and executes query workloads against benchmark tables."""

    def __init__(
        self,
        db: DatabaseManager,
        warmup_iterations: int = 2,
        measure_iterations: int = 5,
    ):
        self.db = db
        self.warmup_iterations = warmup_iterations
        self.measure_iterations = measure_iterations

    # ------------------------------------------------------------------
    # Query generators per pattern
    # ------------------------------------------------------------------

    def _full_scan_query(self, table_name: str, schema: TableSchema) -> str:
        return f"SELECT * FROM {table_name}"

    def _column_projection_query(self, table_name: str, schema: TableSchema) -> str:
        # Select first 2 non-id columns (or all if < 2)
        cols = [c[0] for c in schema.columns if c[0] != "id"][:2]
        if not cols:
            cols = [schema.columns[0][0]]
        return f"SELECT {', '.join(cols)} FROM {table_name}"

    def _filtered_scan_query(self, table_name: str, schema: TableSchema) -> str:
        # Find a suitable filter column
        for col_name, col_type in schema.columns:
            if col_type == ColumnType.INT and col_name != "id":
                return f"SELECT * FROM {table_name} WHERE {col_name} > 0"
            if col_type == ColumnType.BOOLEAN:
                return f"SELECT * FROM {table_name} WHERE {col_name} = TRUE"
        # Fallback: filter on id
        return f"SELECT * FROM {table_name} WHERE id > 0 AND id <= 1000"

    def _aggregation_query(self, table_name: str, schema: TableSchema) -> str:
        agg_exprs = []
        for col_name, col_type in schema.columns:
            if col_type in (ColumnType.INT, ColumnType.BIGINT, ColumnType.FLOAT, ColumnType.NUMERIC):
                agg_exprs.append(f"SUM({col_name})")
                agg_exprs.append(f"AVG({col_name})")
                if len(agg_exprs) >= 6:
                    break
        if not agg_exprs:
            agg_exprs = ["COUNT(*)"]
        return f"SELECT COUNT(*), {', '.join(agg_exprs)} FROM {table_name}"

    def _group_by_query(self, table_name: str, schema: TableSchema) -> str:
        # Find a good GROUP BY column (low-ish cardinality integer or boolean)
        group_col = None
        agg_col = None
        for col_name, col_type in schema.columns:
            if col_name == "id":
                continue
            if col_type in (ColumnType.INT, ColumnType.BOOLEAN) and group_col is None:
                group_col = col_name
            if col_type in (ColumnType.FLOAT, ColumnType.NUMERIC, ColumnType.INT, ColumnType.BIGINT) and agg_col is None:
                agg_col = col_name

        if group_col is None:
            group_col = schema.columns[0][0]
        if agg_col is None:
            agg_col = "id"

        return (
            f"SELECT {group_col}, COUNT(*), SUM({agg_col}), AVG({agg_col}) "
            f"FROM {table_name} GROUP BY {group_col}"
        )

    def _index_scan_query(self, table_name: str, schema: TableSchema) -> str:
        return f"SELECT * FROM {table_name} WHERE id = 42"

    def _repeated_hotset_query(self, table_name: str, schema: TableSchema) -> str:
        """Access hot 5% of rows repeatedly, full scan, re-access hot set."""
        # Uses a PL/pgSQL block to simulate multi-phase access pattern
        return (
            f"DO $$ DECLARE r record; BEGIN "
            f"FOR r IN SELECT * FROM {table_name} WHERE id <= "
            f"(SELECT max(id)*0.05 FROM {table_name}) LOOP END LOOP; "
            f"FOR r IN SELECT * FROM {table_name} LOOP END LOOP; "
            f"FOR r IN SELECT * FROM {table_name} WHERE id <= "
            f"(SELECT max(id)*0.05 FROM {table_name}) LOOP END LOOP; "
            f"END $$"
        )

    def _working_set_shift_query(self, table_name: str, schema: TableSchema) -> str:
        """Access rows 1-1000, then 500-1500, then 1000-2000 (overlapping shifts)."""
        return (
            f"DO $$ DECLARE r record; BEGIN "
            f"FOR r IN SELECT * FROM {table_name} WHERE id BETWEEN 1 AND 1000 LOOP END LOOP; "
            f"FOR r IN SELECT * FROM {table_name} WHERE id BETWEEN 500 AND 1500 LOOP END LOOP; "
            f"FOR r IN SELECT * FROM {table_name} WHERE id BETWEEN 1000 AND 2000 LOOP END LOOP; "
            f"END $$"
        )

    def _zipfian_access_query(self, table_name: str, schema: TableSchema) -> str:
        """Power-law row access: most accesses hit a few hot rows."""
        # Simulate Zipfian by accessing low IDs much more frequently
        return (
            f"DO $$ DECLARE r record; i int; target int; BEGIN "
            f"FOR i IN 1..2000 LOOP "
            f"target := greatest(1, floor(pow(random(), 2) * "
            f"(SELECT max(id) FROM {table_name}))::int); "
            f"SELECT * INTO r FROM {table_name} WHERE id = target; "
            f"END LOOP; END $$"
        )

    def _cyclic_loop_query(self, table_name: str, schema: TableSchema) -> str:
        """Access pages in a repeating cycle where working set > pool.
        LIRS excels here because it tracks IRR and keeps short-gap pages.
        """
        return (
            f"DO $$ DECLARE r record; cycle int; BEGIN "
            f"cycle := greatest(200, (SELECT count(*) FROM {table_name}) / 2); "
            f"FOR pass IN 1..3 LOOP "
            f"FOR r IN SELECT * FROM {table_name} WHERE id <= cycle ORDER BY id LOOP END LOOP; "
            f"END LOOP; END $$"
        )

    def _capacity_pressure_query(self, table_name: str, schema: TableSchema) -> str:
        """Random access in a working set larger than the pool."""
        return (
            f"DO $$ DECLARE r record; i int; target int; mx int; BEGIN "
            f"mx := (SELECT max(id) FROM {table_name}); "
            f"FOR i IN 1..2000 LOOP "
            f"target := 1 + floor(random() * mx)::int; "
            f"SELECT * INTO r FROM {table_name} WHERE id = target; "
            f"END LOOP; END $$"
        )

    def _sequential_then_random_query(self, table_name: str, schema: TableSchema) -> str:
        """Phase 1: full sequential scan. Phase 2: random point lookups on a subset.
        Tests algorithm recovery after scan pollution.
        """
        return (
            f"DO $$ DECLARE r record; i int; target int; mx int; BEGIN "
            f"FOR r IN SELECT * FROM {table_name} LOOP END LOOP; "
            f"mx := greatest(1, (SELECT max(id) FROM {table_name}) / 4); "
            f"FOR i IN 1..1000 LOOP "
            f"target := 1 + floor(random() * mx)::int; "
            f"SELECT * INTO r FROM {table_name} WHERE id = target; "
            f"END LOOP; END $$"
        )

    def _correlated_range_query(self, table_name: str, schema: TableSchema) -> str:
        """Repeated range scans on overlapping but shifting ranges.
        Tests ghost list effectiveness and adaptation speed.
        """
        return (
            f"DO $$ DECLARE r record; BEGIN "
            f"FOR r IN SELECT * FROM {table_name} WHERE id BETWEEN 1 AND 500 LOOP END LOOP; "
            f"FOR r IN SELECT * FROM {table_name} WHERE id BETWEEN 250 AND 750 LOOP END LOOP; "
            f"FOR r IN SELECT * FROM {table_name} WHERE id BETWEEN 500 AND 1000 LOOP END LOOP; "
            f"FOR r IN SELECT * FROM {table_name} WHERE id BETWEEN 750 AND 1250 LOOP END LOOP; "
            f"FOR r IN SELECT * FROM {table_name} WHERE id BETWEEN 1 AND 500 LOOP END LOOP; "
            f"END $$"
        )

    def _write_heavy_update_query(self, table_name: str, schema: TableSchema) -> str:
        """80% UPDATE, 20% SELECT on the same rows.
        Tests dirty buffer management and trickle writer interaction.
        """
        # Find a suitable text column or fall back to id
        update_col = None
        for col_name, col_type in schema.columns:
            if col_type == ColumnType.TEXT and col_name != "id":
                update_col = col_name
                break
        if update_col is None:
            for col_name, col_type in schema.columns:
                if col_type in (ColumnType.INT, ColumnType.BIGINT) and col_name != "id":
                    update_col = col_name
                    break
        if update_col is None:
            update_col = "id"

        return (
            f"DO $$ DECLARE r record; i int; target int; mx int; BEGIN "
            f"mx := greatest(100, (SELECT max(id) FROM {table_name}) / 10); "
            f"FOR i IN 1..1000 LOOP "
            f"target := 1 + floor(random() * mx)::int; "
            f"IF random() < 0.8 THEN "
            f"UPDATE {table_name} SET {update_col} = {update_col} WHERE id = target; "
            f"ELSE "
            f"SELECT * INTO r FROM {table_name} WHERE id = target; "
            f"END IF; "
            f"END LOOP; END $$"
        )

    def _concurrent_mixed_query(self, table_name: str, schema: TableSchema) -> str:
        """Simulate mixed workload: alternating seq scan and point lookups.
        Real concurrency requires multiple connections, so this approximates
        the pattern within a single session.
        """
        return (
            f"DO $$ DECLARE r record; i int; target int; mx int; BEGIN "
            f"mx := (SELECT max(id) FROM {table_name}); "
            f"FOR r IN SELECT * FROM {table_name} WHERE id <= mx/4 LOOP END LOOP; "
            f"FOR i IN 1..500 LOOP "
            f"target := 1 + floor(random() * mx)::int; "
            f"SELECT * INTO r FROM {table_name} WHERE id = target; "
            f"END LOOP; "
            f"FOR r IN SELECT * FROM {table_name} WHERE id > mx/4 AND id <= mx/2 LOOP END LOOP; "
            f"FOR i IN 1..500 LOOP "
            f"target := 1 + floor(random() * mx)::int; "
            f"SELECT * INTO r FROM {table_name} WHERE id = target; "
            f"END LOOP; "
            f"END $$"
        )

    def _working_set_80_20_query(self, table_name: str, schema: TableSchema) -> str:
        """80% of accesses target 20% of data (standard OLTP pattern).
        Tests how well the algorithm identifies and retains the hot partition.
        """
        return (
            f"DO $$ DECLARE r record; i int; target int; mx int; hot_max int; BEGIN "
            f"mx := (SELECT max(id) FROM {table_name}); "
            f"hot_max := greatest(1, mx / 5); "
            f"FOR i IN 1..2000 LOOP "
            f"IF random() < 0.8 THEN "
            f"target := 1 + floor(random() * hot_max)::int; "
            f"ELSE "
            f"target := hot_max + 1 + floor(random() * (mx - hot_max))::int; "
            f"END IF; "
            f"SELECT * INTO r FROM {table_name} WHERE id = target; "
            f"END LOOP; END $$"
        )

    def _temporal_locality_query(self, table_name: str, schema: TableSchema) -> str:
        """Multiple phases targeting different key ranges, then revisiting earlier ranges.
        Tests ghost list / frequency tracking: algorithms that remember evicted pages
        (ARC, CAR, LIRS) should recapture earlier ranges faster on revisit.
        """
        return (
            f"DO $$ DECLARE r record; i int; target int; mx int; "
            f"phase_size int; BEGIN "
            f"mx := (SELECT max(id) FROM {table_name}); "
            f"phase_size := greatest(100, mx / 5); "
            f"/* Phase 1: access range [1, phase_size] */ "
            f"FOR i IN 1..500 LOOP "
            f"target := 1 + floor(random() * phase_size)::int; "
            f"SELECT * INTO r FROM {table_name} WHERE id = target; "
            f"END LOOP; "
            f"/* Phase 2: access range [phase_size+1, 2*phase_size] */ "
            f"FOR i IN 1..500 LOOP "
            f"target := phase_size + 1 + floor(random() * phase_size)::int; "
            f"SELECT * INTO r FROM {table_name} WHERE id = target; "
            f"END LOOP; "
            f"/* Phase 3: access range [2*phase_size+1, 3*phase_size] */ "
            f"FOR i IN 1..500 LOOP "
            f"target := 2 * phase_size + 1 + floor(random() * phase_size)::int; "
            f"SELECT * INTO r FROM {table_name} WHERE id = target; "
            f"END LOOP; "
            f"/* Phase 4: revisit range [1, phase_size] */ "
            f"FOR i IN 1..500 LOOP "
            f"target := 1 + floor(random() * phase_size)::int; "
            f"SELECT * INTO r FROM {table_name} WHERE id = target; "
            f"END LOOP; "
            f"/* Phase 5: revisit range [phase_size+1, 2*phase_size] */ "
            f"FOR i IN 1..500 LOOP "
            f"target := phase_size + 1 + floor(random() * phase_size)::int; "
            f"SELECT * INTO r FROM {table_name} WHERE id = target; "
            f"END LOOP; "
            f"END $$"
        )

    def _sequential_scan_burst_query(self, table_name: str, schema: TableSchema) -> str:
        """Periodic full table scans interspersed with point lookups on hot rows.
        Tests scan resistance: clock-sweep and LRU can be poisoned by the scan,
        evicting hot data. ARC/CAR/LIRS should protect frequently-accessed pages.
        """
        return (
            f"DO $$ DECLARE r record; i int; target int; mx int; BEGIN "
            f"mx := (SELECT max(id) FROM {table_name}); "
            f"/* Warm up hot set */ "
            f"FOR i IN 1..200 LOOP "
            f"target := 1 + floor(random() * greatest(1, mx / 10))::int; "
            f"SELECT * INTO r FROM {table_name} WHERE id = target; "
            f"END LOOP; "
            f"/* Scan burst 1 */ "
            f"FOR r IN SELECT * FROM {table_name} LOOP END LOOP; "
            f"/* Point lookups on hot set */ "
            f"FOR i IN 1..200 LOOP "
            f"target := 1 + floor(random() * greatest(1, mx / 10))::int; "
            f"SELECT * INTO r FROM {table_name} WHERE id = target; "
            f"END LOOP; "
            f"/* Scan burst 2 */ "
            f"FOR r IN SELECT * FROM {table_name} LOOP END LOOP; "
            f"/* Point lookups on hot set */ "
            f"FOR i IN 1..200 LOOP "
            f"target := 1 + floor(random() * greatest(1, mx / 10))::int; "
            f"SELECT * INTO r FROM {table_name} WHERE id = target; "
            f"END LOOP; "
            f"END $$"
        )

    def _mixed_oltp_scan_query(self, table_name: str, schema: TableSchema) -> str:
        """Concurrent OLTP + sequential scan (simulates analytics + transactions).
        Alternates between transactional point operations and analytical scans
        within the same session, stressing both hot-set retention and scan handling.
        """
        update_col = None
        for col_name, col_type in schema.columns:
            if col_type == ColumnType.TEXT and col_name != "id":
                update_col = col_name
                break
        if update_col is None:
            for col_name, col_type in schema.columns:
                if col_type in (ColumnType.INT, ColumnType.BIGINT) and col_name != "id":
                    update_col = col_name
                    break
        if update_col is None:
            update_col = "id"

        return (
            f"DO $$ DECLARE r record; i int; target int; mx int; cnt bigint; BEGIN "
            f"mx := (SELECT max(id) FROM {table_name}); "
            f"/* OLTP phase: point reads and updates */ "
            f"FOR i IN 1..300 LOOP "
            f"target := 1 + floor(random() * greatest(1, mx / 5))::int; "
            f"IF random() < 0.7 THEN "
            f"SELECT * INTO r FROM {table_name} WHERE id = target; "
            f"ELSE "
            f"UPDATE {table_name} SET {update_col} = {update_col} WHERE id = target; "
            f"END IF; "
            f"END LOOP; "
            f"/* Analytics scan */ "
            f"SELECT count(*) INTO cnt FROM {table_name}; "
            f"/* More OLTP */ "
            f"FOR i IN 1..300 LOOP "
            f"target := 1 + floor(random() * greatest(1, mx / 5))::int; "
            f"SELECT * INTO r FROM {table_name} WHERE id = target; "
            f"END LOOP; "
            f"/* Another analytics scan */ "
            f"SELECT count(*) INTO cnt FROM {table_name} WHERE id > mx / 2; "
            f"/* Final OLTP burst */ "
            f"FOR i IN 1..300 LOOP "
            f"target := 1 + floor(random() * greatest(1, mx / 5))::int; "
            f"SELECT * INTO r FROM {table_name} WHERE id = target; "
            f"END LOOP; "
            f"END $$"
        )

    def _get_query(
        self, pattern: QueryPattern, table_name: str, schema: TableSchema
    ) -> str:
        generators = {
            QueryPattern.FULL_SCAN: self._full_scan_query,
            QueryPattern.COLUMN_PROJECTION: self._column_projection_query,
            QueryPattern.FILTERED_SCAN: self._filtered_scan_query,
            QueryPattern.AGGREGATION: self._aggregation_query,
            QueryPattern.GROUP_BY: self._group_by_query,
            QueryPattern.INDEX_SCAN: self._index_scan_query,
            QueryPattern.REPEATED_HOTSET: self._repeated_hotset_query,
            QueryPattern.WORKING_SET_SHIFT: self._working_set_shift_query,
            QueryPattern.ZIPFIAN_ACCESS: self._zipfian_access_query,
            QueryPattern.CYCLIC_LOOP: self._cyclic_loop_query,
            QueryPattern.CAPACITY_PRESSURE: self._capacity_pressure_query,
            QueryPattern.SEQUENTIAL_THEN_RANDOM: self._sequential_then_random_query,
            QueryPattern.CORRELATED_RANGE: self._correlated_range_query,
            QueryPattern.WRITE_HEAVY_UPDATE: self._write_heavy_update_query,
            QueryPattern.CONCURRENT_MIXED: self._concurrent_mixed_query,
            QueryPattern.WORKING_SET_80_20: self._working_set_80_20_query,
            QueryPattern.TEMPORAL_LOCALITY: self._temporal_locality_query,
            QueryPattern.SEQUENTIAL_SCAN_BURST: self._sequential_scan_burst_query,
            QueryPattern.MIXED_OLTP_SCAN: self._mixed_oltp_scan_query,
        }
        gen = generators.get(pattern)
        if gen is None:
            raise ValueError(f"Unknown query pattern: {pattern}")
        return gen(table_name, schema)

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

    async def _run_single(
        self,
        query: str,
        pattern: QueryPattern,
        table_name: str,
        storage_method: str,
        collect_explain: bool = True,
    ) -> QueryResult:
        """Run a single query, returning timing and optional EXPLAIN data."""
        # Warm up
        for _ in range(self.warmup_iterations):
            await self.db.fetch(query)

        # Measure
        timings = []
        row_count = 0
        for _ in range(self.measure_iterations):
            rows, elapsed = await self.db.fetch_timed(query)
            timings.append(elapsed)
            row_count = len(rows)

        median_time = sorted(timings)[len(timings) // 2]

        # Collect EXPLAIN ANALYZE on one run
        explain_plan = None
        if collect_explain:
            try:
                explain_plan = await self.db.explain_analyze(query)
            except Exception as e:
                logger.warning("EXPLAIN ANALYZE failed for %s: %s", table_name, e)

        return QueryResult(
            query_pattern=pattern.value,
            table_name=table_name,
            storage_method=storage_method,
            query_sql=query,
            elapsed_seconds=median_time,
            row_count=row_count,
            explain_plan=explain_plan,
        )

    async def run_workload(
        self,
        schema: TableSchema,
        heap_table: str,
        noxu_table: str,
        row_count: int,
        distribution: str,
        patterns: Optional[List[QueryPattern]] = None,
        collect_explain: bool = True,
    ) -> tuple:
        """Run a full workload against both HEAP and Noxu tables.

        Returns (heap_workload_result, noxu_workload_result).
        """
        if patterns is None:
            patterns = list(QueryPattern)

        heap_result = WorkloadResult(
            schema_name=schema.name,
            row_count=row_count,
            distribution=distribution,
            storage_method="heap",
        )
        noxu_result = WorkloadResult(
            schema_name=schema.name,
            row_count=row_count,
            distribution=distribution,
            storage_method="noxu",
        )

        for pattern in patterns:
            logger.info(
                "Running %s on %s/%s (rows=%d, dist=%s)",
                pattern.value,
                heap_table,
                noxu_table,
                row_count,
                distribution,
            )

            # HEAP
            heap_query = self._get_query(pattern, heap_table, schema)
            heap_qr = await self._run_single(
                heap_query, pattern, heap_table, "heap", collect_explain
            )
            heap_result.add(heap_qr)

            # Noxu
            noxu_query = self._get_query(pattern, noxu_table, schema)
            noxu_qr = await self._run_single(
                noxu_query, pattern, noxu_table, "noxu", collect_explain
            )
            noxu_result.add(noxu_qr)

            speedup = (
                heap_qr.elapsed_seconds / noxu_qr.elapsed_seconds
                if noxu_qr.elapsed_seconds > 0
                else float("inf")
            )
            logger.info(
                "  %s: heap=%.4fs noxu=%.4fs speedup=%.2fx",
                pattern.value,
                heap_qr.elapsed_seconds,
                noxu_qr.elapsed_seconds,
                speedup,
            )

        return heap_result, noxu_result

    async def run_custom_query(
        self,
        query: str,
        table_name: str,
        storage_method: str,
        label: str = "custom",
        collect_explain: bool = True,
    ) -> QueryResult:
        """Run an arbitrary query with benchmarking instrumentation."""
        return await self._run_single(
            query,
            QueryPattern.FULL_SCAN,  # placeholder
            table_name,
            storage_method,
            collect_explain,
        )

    # ------------------------------------------------------------------
    # Buffer pool strategy comparison
    # ------------------------------------------------------------------

    async def _snapshot_pool_stats(
        self, pool_config: PoolConfig
    ) -> Dict[str, Any]:
        """Snapshot pg_stat_bufferpool and pg_stat_arc for delta computation."""
        snap: Dict[str, Any] = {}
        if pool_config.strategy == BufferStrategy.CLOCK:
            return snap

        pool_name = pool_config.pool_name
        try:
            row = await self.db.fetchrow(
                "SELECT * FROM pg_stat_bufferpool WHERE name = $1",
                pool_name,
            )
            if row:
                snap["bufferpool"] = dict(row)
        except Exception:
            pass

        if pool_config.strategy == BufferStrategy.ARC:
            try:
                row = await self.db.fetchrow(
                    "SELECT * FROM pg_stat_arc WHERE name = $1",
                    pool_name,
                )
                if row:
                    snap["arc"] = dict(row)
            except Exception:
                pass

        if pool_config.strategy == BufferStrategy.CAR:
            try:
                row = await self.db.fetchrow(
                    "SELECT * FROM pg_stat_car WHERE name = $1",
                    pool_name,
                )
                if row:
                    snap["car"] = dict(row)
            except Exception:
                pass

        if pool_config.strategy == BufferStrategy.LIRS:
            try:
                row = await self.db.fetchrow(
                    "SELECT * FROM pg_stat_lirs WHERE name = $1",
                    pool_name,
                )
                if row:
                    snap["lirs"] = dict(row)
            except Exception:
                pass

        if pool_config.strategy == BufferStrategy.LRU:
            try:
                row = await self.db.fetchrow(
                    "SELECT * FROM pg_stat_lru WHERE name = $1",
                    pool_name,
                )
                if row:
                    snap["lru"] = dict(row)
            except Exception:
                pass

        if pool_config.strategy == BufferStrategy.OSIC:
            try:
                row = await self.db.fetchrow(
                    "SELECT * FROM pg_stat_osic WHERE pool_name = $1",
                    pool_name,
                )
                if row:
                    snap["osic"] = dict(row)
            except Exception:
                pass

        return snap

    @staticmethod
    def _compute_pool_delta(
        before: Dict[str, Any], after: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Compute deltas between two pool stat snapshots."""
        delta: Dict[str, Any] = {}

        bp_before = before.get("bufferpool", {})
        bp_after = after.get("bufferpool", {})
        if bp_before and bp_after:
            bp_delta = {}
            for key in ("reads", "hits", "evictions"):
                b = bp_before.get(key, 0) or 0
                a = bp_after.get(key, 0) or 0
                bp_delta[key] = a - b
            total = bp_delta.get("hits", 0) + bp_delta.get("reads", 0)
            bp_delta["hit_ratio"] = (
                bp_delta["hits"] / total if total > 0 else 0.0
            )
            delta["bufferpool"] = bp_delta

        # ARC and CAR use the same stat keys
        for algo_key in ("arc", "car"):
            algo_before = before.get(algo_key, {})
            algo_after = after.get(algo_key, {})
            if algo_before and algo_after:
                algo_delta = {}
                for key in (
                    "lookups", "t1_hits", "t2_hits", "b1_hits", "b2_hits",
                    "misses", "t1_evictions", "t2_evictions",
                    "t1_size", "t2_size", "b1_size", "b2_size",
                    "target_t1_size",
                ):
                    b = algo_before.get(key, 0) or 0
                    a = algo_after.get(key, 0) or 0
                    algo_delta[key] = a - b
                # For size fields, use the absolute 'after' value
                for key in ("t1_size", "t2_size", "b1_size", "b2_size",
                            "target_t1_size"):
                    algo_delta[key] = algo_after.get(key, 0) or 0
                delta[algo_key] = algo_delta

        # LIRS uses different stat keys
        lirs_before = before.get("lirs", {})
        lirs_after = after.get("lirs", {})
        if lirs_before and lirs_after:
            lirs_delta = {}
            for key in (
                "lookups", "lir_hits", "hir_hits", "ghost_hits",
                "misses", "lir_demotions", "hir_promotions",
                "evictions", "stack_prunes",
            ):
                b = lirs_before.get(key, 0) or 0
                a = lirs_after.get(key, 0) or 0
                lirs_delta[key] = a - b
            # For size fields, use the absolute 'after' value
            for key in ("lir_size", "hir_size", "ghost_size",
                        "lir_capacity", "stack_size", "q_size"):
                lirs_delta[key] = lirs_after.get(key, 0) or 0
            delta["lirs"] = lirs_delta

        # LRU uses simple stat keys
        lru_before = before.get("lru", {})
        lru_after = after.get("lru", {})
        if lru_before and lru_after:
            lru_delta = {}
            for key in ("hits", "misses", "evictions"):
                b = lru_before.get(key, 0) or 0
                a = lru_after.get(key, 0) or 0
                lru_delta[key] = a - b
            # For size fields, use the absolute 'after' value
            for key in ("list_size",):
                lru_delta[key] = lru_after.get(key, 0) or 0
            delta["lru"] = lru_delta

        # OSIC uses simple stat keys
        osic_before = before.get("osic", {})
        osic_after = after.get("osic", {})
        if osic_before and osic_after:
            osic_delta = {}
            for key in ("hits", "misses", "evictions", "cooling_sweeps"):
                b = osic_before.get(key, 0) or 0
                a = osic_after.get(key, 0) or 0
                osic_delta[key] = a - b
            # For size fields, use the absolute 'after' value
            for key in ("hot_count", "cool_count"):
                osic_delta[key] = osic_after.get(key, 0) or 0
            delta["osic"] = osic_delta

        return delta

    async def run_strategy_workload(
        self,
        schema: TableSchema,
        pool_configs: List[PoolConfig],
        row_count: int,
        distribution: str,
        patterns: Optional[List[QueryPattern]] = None,
        collect_explain: bool = False,
    ) -> Dict[BufferStrategy, "StrategyWorkloadResult"]:
        """Run the same query workload against tables in different buffer pools.

        For each strategy: create table in pool, load data, run queries,
        snapshot pool stats before/after.

        Returns {strategy: StrategyWorkloadResult}.
        """
        from .data_generator import DataGenerator
        from .schema_builder import SchemaBuilder

        if patterns is None:
            patterns = list(QueryPattern)

        builder = SchemaBuilder(self.db)
        gen = DataGenerator(seed=42)
        results: Dict[BufferStrategy, StrategyWorkloadResult] = {}

        for pc in pool_configs:
            strategy = pc.strategy
            suffix = f"_{strategy.value}"
            table_name = f"{schema.name}{suffix}"

            logger.info(
                "Running strategy workload: %s (pool=%s, rows=%d, dist=%s)",
                strategy.value, pc.pool_name or "default", row_count, distribution,
            )

            # Create pool + table
            await builder.create_buffer_pool(pc)
            await builder.create_table_in_pool(schema, pc, suffix)
            await builder.create_indexes(schema, table_name)

            # Load data
            from .config import DataDistribution
            insert_sql = gen.generate_server_side_insert(
                schema, row_count,
                DataDistribution(distribution),
                table_suffix=suffix,
            )
            await builder.load_data(table_name, insert_sql)

            # Snapshot before
            before = await self._snapshot_pool_stats(pc)

            # Run queries
            wr = WorkloadResult(
                schema_name=schema.name,
                row_count=row_count,
                distribution=distribution,
                storage_method=strategy.value,
            )

            for pattern in patterns:
                query = self._get_query(pattern, table_name, schema)
                qr = await self._run_single(
                    query, pattern, table_name, strategy.value, collect_explain
                )
                wr.add(qr)
                logger.info(
                    "  %s on %s: %.4fs",
                    pattern.value, strategy.value, qr.elapsed_seconds,
                )

            # Snapshot after
            after = await self._snapshot_pool_stats(pc)
            delta = self._compute_pool_delta(before, after)

            results[strategy] = StrategyWorkloadResult(
                strategy=strategy,
                pool_config=pc,
                workload=wr,
                pool_stats_before=before,
                pool_stats_after=after,
                pool_stats_delta=delta,
            )

            # Cleanup table (but leave pool for metrics collection)
            await self.db.drop_table(table_name)
            await builder.drop_buffer_pool(pc)

        return results


@dataclass
class StrategyWorkloadResult:
    """Results for a single buffer replacement strategy."""
    strategy: BufferStrategy
    pool_config: PoolConfig
    workload: WorkloadResult
    pool_stats_before: Dict[str, Any] = field(default_factory=dict)
    pool_stats_after: Dict[str, Any] = field(default_factory=dict)
    pool_stats_delta: Dict[str, Any] = field(default_factory=dict)
