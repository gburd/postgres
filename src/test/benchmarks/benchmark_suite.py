"""
Main orchestrator: coordinates data generation, schema creation, workload
execution, metrics collection, analysis, and visualization for the full
benchmark matrix.

Supports two modes:
- NOXU mode: HEAP vs Noxu columnar comparison (original)
- BUFPOOL mode: buffer replacement algorithm comparison (clock-sweep vs ARC)
"""

import asyncio
import logging
import os
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from .config import (
    ALL_SCHEMAS,
    BenchmarkConfig,
    BenchmarkMode,
    BufferStrategy,
    DataDistribution,
    PoolConfig,
    QueryPattern,
    TableSchema,
)
from .data_generator import DataGenerator
from .database import DatabaseManager
from .metrics_collector import BenchmarkMetrics, MetricsCollector
from .result_analyzer import AnalysisReport, ResultAnalyzer
from .schema_builder import SchemaBuilder
from .adaptation_sampler import AdaptationSampler
from .visualizer import Visualizer
from .workload_runner import StrategyWorkloadResult, WorkloadResult, WorkloadRunner

logger = logging.getLogger(__name__)


class BenchmarkSuite:
    """Orchestrates the full Noxu benchmark suite."""

    def __init__(self, config: Optional[BenchmarkConfig] = None):
        self.config = config or BenchmarkConfig()
        self.db = DatabaseManager(self.config.connection)
        self.schema_builder = SchemaBuilder(self.db)
        self.data_generator = DataGenerator(seed=self.config.seed)
        self.workload_runner = WorkloadRunner(
            self.db,
            warmup_iterations=self.config.warmup_iterations,
            measure_iterations=self.config.measure_iterations,
        )
        self.metrics_collector = MetricsCollector(self.db)
        self.analyzer = ResultAnalyzer()

        # Collected results
        self._workload_pairs: List[Tuple[WorkloadResult, WorkloadResult]] = []
        self._metrics_list: List[BenchmarkMetrics] = []

    async def setup(self):
        """Initialize database connections and verify Noxu availability."""
        logger.info("Initializing benchmark suite...")
        await self.db.initialize()

        # Check Noxu
        if not await self.db.check_noxu_available():
            raise RuntimeError(
                "Noxu table AM not found. Ensure PostgreSQL is built with Noxu support."
            )
        logger.info("Noxu table AM is available")

        # Try to enable pg_stat_statements
        if self.config.enable_pg_stat_statements:
            ok = await self.db.ensure_extension("pg_stat_statements")
            if not ok:
                logger.warning(
                    "pg_stat_statements not available; some metrics will be missing"
                )
                self.config.enable_pg_stat_statements = False

    async def teardown(self):
        """Close database connections."""
        await self.db.close()

    async def run_single_benchmark(
        self,
        schema: TableSchema,
        row_count: int,
        distribution: DataDistribution,
    ) -> Tuple[WorkloadResult, WorkloadResult, BenchmarkMetrics]:
        """Run a complete benchmark for one (schema, row_count, distribution) combination."""
        dist_name = distribution.value
        logger.info(
            "=== Benchmark: %s, %d rows, %s distribution ===",
            schema.name,
            row_count,
            dist_name,
        )

        # 1. Create tables
        tables = await self.schema_builder.setup_benchmark_tables(schema)
        heap_table = tables["heap_table"]
        noxu_table = tables["noxu_table"]

        # 2. Generate and load data
        insert_sql_heap = self.data_generator.generate_server_side_insert(
            schema, row_count, distribution, table_suffix="_heap"
        )
        insert_sql_noxu = self.data_generator.generate_server_side_insert(
            schema, row_count, distribution, table_suffix="_noxu"
        )

        logger.info("Loading %d rows into %s...", row_count, heap_table)
        t0 = time.perf_counter()
        await self.schema_builder.load_data(heap_table, insert_sql_heap)
        heap_load_time = time.perf_counter() - t0
        logger.info("HEAP load: %.2fs", heap_load_time)

        logger.info("Loading %d rows into %s...", row_count, noxu_table)
        t0 = time.perf_counter()
        await self.schema_builder.load_data(noxu_table, insert_sql_noxu)
        noxu_load_time = time.perf_counter() - t0
        logger.info("Noxu load: %.2fs", noxu_load_time)

        # 3. Reset stats
        if self.config.enable_pg_stat_statements:
            await self.db.reset_pg_stat_statements()

        # 4. Run workloads
        heap_wr, noxu_wr = await self.workload_runner.run_workload(
            schema=schema,
            heap_table=heap_table,
            noxu_table=noxu_table,
            row_count=row_count,
            distribution=dist_name,
            patterns=self.config.query_patterns,
        )

        # 5. Collect metrics
        metrics = await self.metrics_collector.collect_all(
            heap_table=heap_table,
            noxu_table=noxu_table,
            schema_name=schema.name,
            row_count=row_count,
            distribution=dist_name,
        )

        # 6. Cleanup tables
        await self.schema_builder.cleanup(schema)

        return heap_wr, noxu_wr, metrics

    async def run_full_suite(self) -> AnalysisReport:
        """Run the complete benchmark matrix and return an analysis report."""
        start_time = time.perf_counter()
        self._workload_pairs = []
        self._metrics_list = []

        total_combos = (
            len(self.config.schemas)
            * len(self.config.get_row_counts())
            * len(self.config.distributions)
        )
        combo_idx = 0

        for schema in self.config.schemas:
            for row_count in self.config.get_row_counts():
                for dist in self.config.distributions:
                    combo_idx += 1
                    logger.info(
                        "--- Combination %d/%d ---", combo_idx, total_combos
                    )
                    try:
                        heap_wr, noxu_wr, metrics = await self.run_single_benchmark(
                            schema, row_count, dist
                        )
                        self._workload_pairs.append((heap_wr, noxu_wr))
                        self._metrics_list.append(metrics)
                    except Exception as e:
                        logger.error(
                            "Benchmark failed for %s/%d/%s: %s",
                            schema.name,
                            row_count,
                            dist.value,
                            e,
                        )

        elapsed = time.perf_counter() - start_time
        logger.info("Full suite completed in %.1fs", elapsed)

        # Analyze
        report = self.analyzer.build_report(self._workload_pairs, self._metrics_list)
        return report

    def generate_output(self, report: AnalysisReport) -> str:
        """Generate CSV files, charts, and HTML dashboard.

        Returns the path to the output directory.
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_dir = os.path.join(self.config.output_dir, f"run_{timestamp}")
        viz = Visualizer(output_dir)

        csv_path = viz.export_csv(report)
        logger.info("CSV results: %s", csv_path)

        dashboard_path = viz.generate_dashboard(report)
        logger.info("Dashboard: %s", dashboard_path)

        return output_dir


class BufferPoolBenchmarkSuite:
    """Orchestrates buffer replacement algorithm comparison benchmarks."""

    def __init__(self, config: Optional[BenchmarkConfig] = None):
        self.config = config or BenchmarkConfig()
        self.db = DatabaseManager(self.config.connection)
        self.schema_builder = SchemaBuilder(self.db)
        self.data_generator = DataGenerator(seed=self.config.seed)
        self.workload_runner = WorkloadRunner(
            self.db,
            warmup_iterations=self.config.warmup_iterations,
            measure_iterations=self.config.measure_iterations,
        )
        self.metrics_collector = MetricsCollector(self.db)
        self._ext_avail: dict = {}

        # Collected results per (schema, row_count, distribution)
        self._strategy_results: List[
            Dict[BufferStrategy, StrategyWorkloadResult]
        ] = []

    async def setup(self):
        """Initialize database connections and verify extensions."""
        logger.info("Initializing buffer pool benchmark suite...")
        await self.db.initialize()
        self._ext_avail = await self.db.setup_bufferpool_extensions()

        if not self._ext_avail.get("pg_stat_bufferpool"):
            logger.warning(
                "pg_stat_bufferpool view not found; pool-level stats "
                "will be unavailable"
            )

    async def teardown(self):
        await self.db.close()

    def _build_pool_configs(self) -> List[PoolConfig]:
        """Build PoolConfig list from configured strategies."""
        return [
            PoolConfig.for_strategy(s, self.config.pool_size)
            for s in self.config.strategies
        ]

    async def run_single_benchmark(
        self,
        schema: TableSchema,
        row_count: int,
        distribution: DataDistribution,
    ) -> Dict[BufferStrategy, StrategyWorkloadResult]:
        """Run strategy comparison for one (schema, row_count, distribution)."""
        dist_name = distribution.value
        logger.info(
            "=== Buffer Pool Benchmark: %s, %d rows, %s ===",
            schema.name, row_count, dist_name,
        )

        pool_configs = self._build_pool_configs()

        # Reset pg_stat_statements if available
        if self._ext_avail.get("pg_stat_statements"):
            await self.db.reset_pg_stat_statements()

        results = await self.workload_runner.run_strategy_workload(
            schema=schema,
            pool_configs=pool_configs,
            row_count=row_count,
            distribution=dist_name,
            patterns=self.config.query_patterns,
        )

        return results

    async def run_full_suite(self) -> dict:
        """Run the complete strategy comparison matrix.

        Returns a dict with:
          - 'strategy_results': list of per-combination strategy result dicts
          - 'summary': high-level summary statistics
        """
        start_time = time.perf_counter()
        self._strategy_results = []

        total_combos = (
            len(self.config.schemas)
            * len(self.config.get_row_counts())
            * len(self.config.distributions)
        )
        combo_idx = 0

        for schema in self.config.schemas:
            for row_count in self.config.get_row_counts():
                for dist in self.config.distributions:
                    combo_idx += 1
                    logger.info(
                        "--- Combination %d/%d ---", combo_idx, total_combos
                    )
                    try:
                        results = await self.run_single_benchmark(
                            schema, row_count, dist
                        )
                        self._strategy_results.append(results)
                    except Exception as e:
                        logger.error(
                            "Buffer pool benchmark failed for %s/%d/%s: %s",
                            schema.name, row_count, dist.value, e,
                        )

        elapsed = time.perf_counter() - start_time
        logger.info("Buffer pool suite completed in %.1fs", elapsed)

        summary = self._build_summary()
        return {"strategy_results": self._strategy_results, "summary": summary}

    def _build_summary(self) -> Dict:
        """Build a summary of strategy comparison results."""
        import statistics as stats

        summary: Dict = {"strategies": [], "per_strategy_timings": {}}
        if not self._strategy_results:
            return summary

        # Collect all strategies that were tested
        all_strategies = set()
        for sr in self._strategy_results:
            all_strategies.update(sr.keys())
        summary["strategies"] = [s.value for s in sorted(all_strategies, key=lambda s: s.value)]

        # Per-strategy average timings across all patterns/combos
        for strategy in all_strategies:
            timings = []
            hit_ratios = []
            for sr in self._strategy_results:
                swr = sr.get(strategy)
                if not swr:
                    continue
                for qr in swr.workload.results:
                    timings.append(qr.elapsed_seconds)
                bp_delta = swr.pool_stats_delta.get("bufferpool", {})
                if bp_delta.get("hit_ratio") is not None:
                    hit_ratios.append(bp_delta["hit_ratio"])

            s_info: Dict = {}
            if timings:
                s_info["avg_time_s"] = stats.mean(timings)
                s_info["median_time_s"] = stats.median(timings)
            if hit_ratios:
                s_info["avg_hit_ratio"] = stats.mean(hit_ratios)
            summary["per_strategy_timings"][strategy.value] = s_info

        # Strategy speedups (relative to clock baseline)
        default_timings = summary["per_strategy_timings"].get(
            BufferStrategy.CLOCK.value, {}
        )
        if default_timings.get("median_time_s"):
            baseline = default_timings["median_time_s"]
            for s_val, s_info in summary["per_strategy_timings"].items():
                if s_info.get("median_time_s") and s_info["median_time_s"] > 0:
                    s_info["speedup_vs_default"] = (
                        baseline / s_info["median_time_s"]
                    )

        return summary

    def generate_output(self, suite_results: dict) -> str:
        """Generate CSV and dashboard for strategy comparison results."""
        import csv

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_dir = os.path.join(self.config.output_dir, f"bufpool_{timestamp}")
        os.makedirs(output_dir, exist_ok=True)

        # CSV: per-query timing comparison
        csv_path = os.path.join(output_dir, "strategy_timings.csv")
        with open(csv_path, "w", newline="") as f:
            writer = csv.writer(f)
            header = [
                "schema", "row_count", "distribution", "query_pattern",
            ]
            # Add columns per strategy
            strategies = suite_results["summary"].get("strategies", [])
            for s in strategies:
                header.extend([f"{s}_median_s", f"{s}_hit_ratio"])
            writer.writerow(header)

            for sr in suite_results["strategy_results"]:
                # Get metadata from first strategy's workload
                first_swr = next(iter(sr.values()))
                wr = first_swr.workload
                for qr in wr.results:
                    row = [
                        wr.schema_name, wr.row_count, wr.distribution,
                        qr.query_pattern,
                    ]
                    for s_val in strategies:
                        s_key = BufferStrategy(s_val)
                        swr = sr.get(s_key)
                        if swr:
                            # Find matching pattern result
                            matching = [
                                r for r in swr.workload.results
                                if r.query_pattern == qr.query_pattern
                            ]
                            if matching:
                                row.append(f"{matching[0].elapsed_seconds:.6f}")
                            else:
                                row.append("")
                            bp = swr.pool_stats_delta.get("bufferpool", {})
                            row.append(
                                f"{bp.get('hit_ratio', 0):.4f}"
                                if bp.get("hit_ratio") is not None else ""
                            )
                        else:
                            row.extend(["", ""])
                    writer.writerow(row)

        # CSV: pool stats
        pool_csv = os.path.join(output_dir, "pool_stats.csv")
        with open(pool_csv, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                "schema", "row_count", "distribution", "strategy",
                "reads", "hits", "evictions", "hit_ratio",
                "algo_t1_size", "algo_t2_size", "algo_target_t1",
                "algo_lookups", "algo_misses",
                "algo_t1_evictions", "algo_t2_evictions",
            ])
            for sr in suite_results["strategy_results"]:
                for strategy, swr in sr.items():
                    wr = swr.workload
                    bp = swr.pool_stats_delta.get("bufferpool", {})
                    # Pick whichever algo delta is present
                    algo = (swr.pool_stats_delta.get("arc") or
                            swr.pool_stats_delta.get("car") or {})
                    lirs = swr.pool_stats_delta.get("lirs", {})
                    lru = swr.pool_stats_delta.get("lru", {})
                    osic = swr.pool_stats_delta.get("osic", {})
                    # For LIRS, map to common column names
                    if lirs and not algo:
                        algo = {
                            "t1_size": lirs.get("lir_size", ""),
                            "t2_size": lirs.get("hir_size", ""),
                            "target_t1_size": lirs.get("lir_capacity", ""),
                            "lookups": lirs.get("lookups", ""),
                            "misses": lirs.get("misses", ""),
                            "t1_evictions": lirs.get("evictions", ""),
                            "t2_evictions": lirs.get("stack_prunes", ""),
                        }
                    # For LRU, map to common column names
                    if lru and not algo:
                        algo = {
                            "t1_size": lru.get("list_size", ""),
                            "t2_size": "",
                            "target_t1_size": "",
                            "lookups": "",
                            "misses": lru.get("misses", ""),
                            "t1_evictions": lru.get("evictions", ""),
                            "t2_evictions": "",
                        }
                    # For OSIC, map to common column names
                    if osic and not algo:
                        algo = {
                            "t1_size": osic.get("hot_count", ""),
                            "t2_size": osic.get("cool_count", ""),
                            "target_t1_size": "",
                            "lookups": "",
                            "misses": osic.get("misses", ""),
                            "t1_evictions": osic.get("evictions", ""),
                            "t2_evictions": osic.get("cooling_sweeps", ""),
                        }
                    writer.writerow([
                        wr.schema_name, wr.row_count, wr.distribution,
                        strategy.value,
                        bp.get("reads", ""),
                        bp.get("hits", ""),
                        bp.get("evictions", ""),
                        f"{bp.get('hit_ratio', 0):.4f}" if bp else "",
                        algo.get("t1_size", ""),
                        algo.get("t2_size", ""),
                        algo.get("target_t1_size", ""),
                        algo.get("lookups", ""),
                        algo.get("misses", ""),
                        algo.get("t1_evictions", ""),
                        algo.get("t2_evictions", ""),
                    ])

        # Generate adaptation chart if time-series data was collected
        viz = Visualizer(output_dir)
        adaptation_series = suite_results.get("adaptation_series", [])
        if adaptation_series:
            viz.generate_adaptation_chart(adaptation_series)

        # Generate charts via visualizer
        viz.generate_strategy_dashboard(suite_results)

        logger.info("Buffer pool benchmark results: %s", output_dir)
        return output_dir


async def run_benchmark(config: Optional[BenchmarkConfig] = None) -> AnalysisReport:
    """Convenience entry point: run the full suite and generate output."""
    cfg = config or BenchmarkConfig()

    if cfg.mode == BenchmarkMode.BUFPOOL:
        suite = BufferPoolBenchmarkSuite(cfg)
        try:
            await suite.setup()
            results = await suite.run_full_suite()
            output_dir = suite.generate_output(results)
            logger.info("Results written to: %s", output_dir)
            # Return a minimal AnalysisReport with summary for CLI display
            report = AnalysisReport()
            report.summary = results.get("summary", {})
            return report
        finally:
            await suite.teardown()
    else:
        suite = BenchmarkSuite(cfg)
        try:
            await suite.setup()
            report = await suite.run_full_suite()
            output_dir = suite.generate_output(report)
            logger.info("Results written to: %s", output_dir)
            return report
        finally:
            await suite.teardown()
