"""TPROC-C benchmark runner: orchestrates schema, data, pgbench execution, and reporting."""

import logging
import os
import subprocess
import time
from datetime import datetime
from typing import List, Optional

from .tprocc_config import AccessMethod, TproccConfig
from .tprocc_data import populate_all, vacuum_tables
from .tprocc_report import (
    ComparisonResult,
    RunResult,
    compute_comparisons,
    generate_summary,
    parse_pgbench_log,
    parse_pgbench_output,
    write_csv,
    write_json_report,
)
from .tprocc_schema import create_tables, run_sql
from .tprocc_scripts import generate_scripts

logger = logging.getLogger(__name__)


class TproccBenchmark:
    """End-to-end TPROC-C benchmark orchestrator."""

    def __init__(self, config: TproccConfig):
        self.config = config
        self.results: List[RunResult] = []
        self.comparisons: List[ComparisonResult] = []
        self._run_dir: Optional[str] = None

    @property
    def run_dir(self) -> str:
        if self._run_dir is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            self._run_dir = os.path.join(self.config.output_dir, f"tprocc_{timestamp}")
            os.makedirs(self._run_dir, exist_ok=True)
        return self._run_dir

    def setup(self) -> None:
        """Create tables, populate data, and flush all dirty pages.

        Issues an explicit CHECKPOINT after all population is complete to
        ensure background I/O from data loading doesn't overlap with the
        measurement phase.  On large datasets (W=10+), the automatic
        checkpoint from population can take 5-10 minutes and severely
        degrade benchmark results if it runs concurrently.
        """
        if self.config.skip_init:
            logger.info("Skipping initialization (--skip-init)")
            return

        for am in self.config.access_methods:
            create_tables(self.config, am)
            populate_all(self.config, am)
            vacuum_tables(self.config, am)

        # Force checkpoint to flush all dirty pages from population.
        # This prevents checkpoint I/O from overlapping with measurement.
        logger.info("Forcing CHECKPOINT to flush population data...")
        run_sql("CHECKPOINT;", self.config)
        logger.info("CHECKPOINT complete — ready for benchmarking")

    def _build_pgbench_cmd(self, am: AccessMethod, clients: int,
                           script_paths: dict) -> List[str]:
        """Build the pgbench command line."""
        conn = self.config.connection
        cmd = [self.config.pgbench_bin]

        # Connection
        if conn.host:
            cmd += ["-h", conn.host]
        if conn.port:
            cmd += ["-p", str(conn.port)]
        if conn.user:
            cmd += ["-U", conn.user]

        # Execution parameters
        cmd += [
            "-c", str(clients),
            "-j", str(min(clients, os.cpu_count() or 4)),
            "-T", str(self.config.total_duration),
            "-P", "5",                # progress every 5s
            "--no-vacuum",            # we vacuum explicitly
            "-r",                     # report per-statement latencies
            "--failures-detailed",    # report serialization/deadlock failures
            "--max-tries", "2",       # retry once on serialization failure
        ]

        # Transaction scripts with weights
        mix = self.config.txn_mix
        from .tprocc_config import TxnType
        weight_map = {
            TxnType.NEW_ORDER: "neworder",
            TxnType.PAYMENT: "payment",
            TxnType.ORDER_STATUS: "orderstatus",
            TxnType.DELIVERY: "delivery",
            TxnType.STOCK_LEVEL: "stocklevel",
        }
        for txn_type, txn_name in weight_map.items():
            weight = mix.get(txn_type, 0)
            if weight > 0 and txn_name in script_paths:
                cmd += ["-f", f"{script_paths[txn_name]}@{weight}"]

        # Logging for latency percentiles
        log_dir = os.path.join(self.run_dir, "logs")
        os.makedirs(log_dir, exist_ok=True)
        log_prefix = os.path.join(log_dir, f"pgbench_{am.value}_c{clients}")
        cmd += ["--log", "--log-prefix", log_prefix]

        # Database name (positional)
        cmd.append(conn.database)

        return cmd

    def _run_pgbench(self, am: AccessMethod, clients: int, rep: int,
                     script_paths: dict) -> RunResult:
        """Execute pgbench and parse results."""
        cmd = self._build_pgbench_cmd(am, clients, script_paths)

        logger.info("  Running: am=%s clients=%d rep=%d duration=%ds",
                    am.value, clients, rep, self.config.total_duration)
        logger.debug("  Command: %s", " ".join(cmd))

        env = None
        if self.config.connection.password:
            env = dict(os.environ, PGPASSWORD=self.config.connection.password)

        t0 = time.time()
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            env=env,
            timeout=self.config.total_duration + 120,  # generous timeout
        )
        elapsed = time.time() - t0

        if result.returncode != 0:
            logger.error("pgbench failed (rc=%d): %s", result.returncode, result.stderr[:500])
            # Save failed output for debugging
            fail_path = os.path.join(self.run_dir, "raw",
                                     f"FAILED_{am.value}_c{clients}_r{rep}.txt")
            os.makedirs(os.path.dirname(fail_path), exist_ok=True)
            with open(fail_path, "w") as f:
                f.write(f"COMMAND: {' '.join(cmd)}\n\nSTDOUT:\n{result.stdout}\n\nSTDERR:\n{result.stderr}")
            raise RuntimeError(f"pgbench failed for {am.value} c={clients}: {result.stderr[:200]}")

        # Save raw output
        raw_dir = os.path.join(self.run_dir, "raw")
        os.makedirs(raw_dir, exist_ok=True)
        raw_path = os.path.join(raw_dir, f"{am.value}_c{clients}_r{rep}.txt")
        with open(raw_path, "w") as f:
            f.write(result.stdout)
            if result.stderr:
                f.write("\n--- STDERR ---\n")
                f.write(result.stderr)

        # Parse main output (TPS is computed excluding warmup by pgbench -T)
        # But we need to handle warmup ourselves since pgbench -T includes warmup
        # Actually we pass total_duration = duration + warmup, and pgbench reports
        # overall TPS. We'll rely on pgbench's own "excluding connections establishing"
        # line which is the full-run TPS. For warmup exclusion, we parse the --log file.
        run_result = parse_pgbench_output(
            result.stdout, am.value, clients, rep,
            self.config.duration, self.config.warmup,
        )

        # Parse log file for percentiles
        log_dir = os.path.join(self.run_dir, "logs")
        # pgbench creates log files like: prefix.client_id
        # or with newer versions: prefix.client_id.thread_id
        log_files = [
            os.path.join(log_dir, f)
            for f in os.listdir(log_dir)
            if f.startswith(f"pgbench_{am.value}_c{clients}")
        ]

        all_latencies = []
        for lf in log_files:
            percentiles = parse_pgbench_log(lf)
            if percentiles["p50"] > 0:
                all_latencies.append(percentiles)

        if all_latencies:
            # Average across log files
            run_result.lat_p50_ms = sum(p["p50"] for p in all_latencies) / len(all_latencies)
            run_result.lat_p95_ms = sum(p["p95"] for p in all_latencies) / len(all_latencies)
            run_result.lat_p99_ms = sum(p["p99"] for p in all_latencies) / len(all_latencies)

        logger.info("    TPS=%.1f NOPM=%.0f lat_avg=%.2fms P95=%.2fms P99=%.2fms (%.1fs)",
                    run_result.tps, run_result.nopm, run_result.lat_avg_ms,
                    run_result.lat_p95_ms, run_result.lat_p99_ms, elapsed)

        return run_result

    def _prewarm_tables(self, am: AccessMethod) -> None:
        """Prewarm tables into shared_buffers using pg_prewarm if available."""
        from .tprocc_schema import get_table_name
        tables = [
            "warehouse", "district", "customer", "item", "stock",
            "orders", "new_order", "order_line",
        ]
        sql_parts = []
        for t in tables:
            tbl = get_table_name(t, am)
            sql_parts.append(f"SELECT pg_prewarm('{tbl}') AS prewarm_{t}")
        sql = ";\n".join(sql_parts) + ";\n"
        try:
            run_sql(sql, self.config)
            logger.info("  Tables prewarmed for %s", am.value)
        except RuntimeError:
            logger.debug("pg_prewarm not available, skipping")

    def run(self) -> None:
        """Execute the full benchmark matrix."""
        logger.info("=" * 60)
        logger.info("TPROC-C Benchmark: HEAP vs RECNO")
        logger.info("=" * 60)
        logger.info("Warehouses: %d", self.config.warehouses)
        logger.info("Duration: %ds + %ds warmup", self.config.duration, self.config.warmup)
        logger.info("Reps: %d", self.config.reps)
        logger.info("Clients: %s", self.config.clients)
        logger.info("Output: %s", self.run_dir)
        logger.info("=" * 60)

        # Generate scripts
        script_dir = os.path.join(self.run_dir, "scripts")
        am_scripts = {}
        for am in self.config.access_methods:
            am_scripts[am] = generate_scripts(self.config, am, script_dir)

        self.results = []

        for clients in self.config.clients:
            logger.info("")
            logger.info("=== Client count: %d ===", clients)

            for rep in range(1, self.config.reps + 1):
                if self.config.reps > 1:
                    logger.info("--- Repetition %d/%d ---", rep, self.config.reps)

                for am in self.config.access_methods:
                    self._prewarm_tables(am)
                    try:
                        result = self._run_pgbench(am, clients, rep, am_scripts[am])
                        self.results.append(result)
                    except (RuntimeError, subprocess.TimeoutExpired) as e:
                        logger.error("Run failed: %s", e)

        logger.info("")
        logger.info("All runs complete. %d results collected.", len(self.results))

    def report(self) -> str:
        """Generate analysis and write output files."""
        if not self.results:
            logger.warning("No results to report")
            return ""

        # Compute comparisons
        self.comparisons = compute_comparisons(self.results)

        # Write CSV
        csv_path = os.path.join(self.run_dir, "tprocc_results.csv")
        write_csv(self.results, csv_path)

        # Write JSON
        json_path = os.path.join(self.run_dir, "report.json")
        write_json_report(self.results, self.comparisons, self.config, json_path)

        # Generate summary text
        summary = generate_summary(self.results, self.comparisons, self.config)

        # Write summary
        summary_path = os.path.join(self.run_dir, "summary.txt")
        with open(summary_path, "w") as f:
            f.write(summary)
        logger.info("Summary written: %s", summary_path)

        # Print to console
        print(summary)

        return summary

    def run_full(self) -> str:
        """Convenience: setup + run + report."""
        self.setup()
        self.run()
        return self.report()
