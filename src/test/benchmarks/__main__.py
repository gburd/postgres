"""
CLI entry point for the Noxu benchmark suite.

Usage:
    python -m src.test.benchmarks [OPTIONS]

    # Or from within the benchmarks directory:
    python -m benchmarks [OPTIONS]

Examples:
    # Quick run with defaults (read-pattern benchmarks)
    python -m src.test.benchmarks

    # TPROC-C benchmark: HEAP vs RECNO
    python -m src.test.benchmarks --workload tprocc

    # TPROC-C quick validation
    python -m src.test.benchmarks --workload tprocc --quick

    # TPROC-C with specific parameters
    python -m src.test.benchmarks --workload tprocc --warehouses 10 --duration 120 --clients 1,2,4,8,16,32

    # Custom database and output
    python -m src.test.benchmarks --database mydb --output-dir /tmp/bench

    # Full matrix (all row counts including 10M)
    python -m src.test.benchmarks --full-matrix

    # Specific schema and row count
    python -m src.test.benchmarks --schema medium --rows 100000

    # Verbose output
    python -m src.test.benchmarks -v
"""

import argparse
import asyncio
import logging
import sys

from .config import (
    ALL_SCHEMAS,
    BenchmarkConfig,
    ConnectionConfig,
    DataDistribution,
    MEDIUM_SCHEMA,
    NARROW_SCHEMA,
    QueryPattern,
    WIDE_SCHEMA,
)
from .benchmark_suite import run_benchmark


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Noxu Performance Benchmark Suite",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    # Workload selection
    parser.add_argument(
        "--workload",
        choices=["read", "tprocc"],
        default="read",
        help="Workload type: 'read' (default, read-pattern benchmarks) or 'tprocc' (TPROC-C OLTP)",
    )

    # Connection
    parser.add_argument("--host", default=None, help="PostgreSQL host")
    parser.add_argument("--port", type=int, default=None, help="PostgreSQL port")
    parser.add_argument("--database", "-d", default=None, help="Database name")
    parser.add_argument("--user", "-U", default=None, help="Database user")

    # TPROC-C specific options
    parser.add_argument(
        "--warehouses", type=int, default=10,
        help="[tprocc] Number of warehouses (default: 10)",
    )
    parser.add_argument(
        "--duration", type=int, default=120,
        help="[tprocc] Seconds per measurement run (default: 120)",
    )
    parser.add_argument(
        "--clients", type=str, default="1,2,4,8,16,32",
        help="[tprocc] Comma-separated client counts (default: 1,2,4,8,16,32)",
    )
    parser.add_argument(
        "--reps", type=int, default=1,
        help="[tprocc] Repetitions per config (default: 1)",
    )
    parser.add_argument(
        "--skip-init", action="store_true",
        help="[tprocc] Skip table creation/population",
    )
    parser.add_argument(
        "--heap-only", action="store_true",
        help="[tprocc] Only benchmark HEAP tables",
    )
    parser.add_argument(
        "--recno-only", action="store_true",
        help="[tprocc] Only benchmark RECNO tables",
    )
    parser.add_argument(
        "--quick", action="store_true",
        help="[tprocc] Quick mode: W=2, D=30, R=1 for fast validation",
    )
    parser.add_argument(
        "--pgbench-bin", default="pgbench",
        help="[tprocc] Path to pgbench binary (default: pgbench in PATH)",
    )
    parser.add_argument(
        "--psql-bin", default="psql",
        help="[tprocc] Path to psql binary (default: psql in PATH)",
    )

    # Read-workload specific options
    parser.add_argument(
        "--schema",
        choices=["narrow", "medium", "wide", "all"],
        default="all",
        help="[read] Table schema to test (default: all)",
    )
    parser.add_argument(
        "--rows",
        type=int,
        nargs="+",
        default=None,
        help="[read] Row counts to test (default: 1000 10000 100000)",
    )
    parser.add_argument(
        "--distribution",
        choices=["random", "clustered", "low_cardinality", "high_null", "all"],
        default="all",
        help="[read] Data distribution (default: all)",
    )
    parser.add_argument(
        "--pattern",
        choices=[p.value for p in QueryPattern] + ["all"],
        default="all",
        help="[read] Query pattern to test (default: all)",
    )
    parser.add_argument(
        "--full-matrix",
        action="store_true",
        help="[read] Run full matrix including 10M rows",
    )

    # Execution (shared / read-workload)
    parser.add_argument(
        "--warmup", type=int, default=None,
        help="Warmup: iterations for read workload (default: 2), seconds for tprocc (default: 10)",
    )
    parser.add_argument(
        "--iterations", type=int, default=5, help="[read] Measurement iterations (default: 5)"
    )
    parser.add_argument("--seed", type=int, default=42, help="RNG seed (default: 42)")

    # Output
    parser.add_argument(
        "--output-dir", "-o", default=None, help="Output directory"
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Verbose logging"
    )

    return parser.parse_args()


def build_read_config(args: argparse.Namespace) -> BenchmarkConfig:
    conn = ConnectionConfig()
    if args.host:
        conn.host = args.host
    if args.port:
        conn.port = args.port
    if args.database:
        conn.database = args.database
    if args.user:
        conn.user = args.user

    schema_map = {
        "narrow": [NARROW_SCHEMA],
        "medium": [MEDIUM_SCHEMA],
        "wide": [WIDE_SCHEMA],
        "all": list(ALL_SCHEMAS),
    }
    schemas = schema_map[args.schema]

    if args.distribution == "all":
        distributions = list(DataDistribution)
    else:
        distributions = [DataDistribution(args.distribution)]

    if args.pattern == "all":
        patterns = list(QueryPattern)
    else:
        patterns = [QueryPattern(args.pattern)]

    warmup = args.warmup if args.warmup is not None else 2

    config = BenchmarkConfig(
        connection=conn,
        schemas=schemas,
        distributions=distributions,
        query_patterns=patterns,
        warmup_iterations=warmup,
        measure_iterations=args.iterations,
        seed=args.seed,
        output_dir=args.output_dir or "benchmark_results",
        full_matrix=args.full_matrix,
        verbose=args.verbose,
    )

    if args.rows:
        config.row_counts = args.rows

    return config


def run_tprocc(args: argparse.Namespace) -> None:
    """Run the TPROC-C benchmark."""
    from .tprocc import TproccBenchmark
    from .tprocc.tprocc_config import TproccConfig

    conn = ConnectionConfig()
    if args.host:
        conn.host = args.host
    if args.port:
        conn.port = args.port
    if args.database:
        conn.database = args.database
    if args.user:
        conn.user = args.user

    clients = [int(c.strip()) for c in args.clients.split(",")]
    warmup = args.warmup if args.warmup is not None else 10

    # Quick mode overrides
    warehouses = args.warehouses
    duration = args.duration
    reps = args.reps
    if args.quick:
        warehouses = 2
        duration = 30
        reps = 1
        warmup = 5

    config = TproccConfig(
        connection=conn,
        warehouses=warehouses,
        duration=duration,
        warmup=warmup,
        reps=reps,
        clients=clients,
        skip_init=args.skip_init,
        heap_only=args.heap_only,
        recno_only=args.recno_only,
        output_dir=args.output_dir or "results",
        verbose=args.verbose,
        psql_bin=args.psql_bin,
        pgbench_bin=args.pgbench_bin,
    )

    bench = TproccBenchmark(config)
    try:
        bench.run_full()
    except KeyboardInterrupt:
        print("\nBenchmark interrupted.")
        sys.exit(1)
    except Exception as e:
        logging.error("TPROC-C benchmark failed: %s", e, exc_info=True)
        sys.exit(1)


def run_read_workload(args: argparse.Namespace) -> None:
    """Run the read-pattern benchmark."""
    config = build_read_config(args)

    print("=" * 60)
    print("  Noxu Performance Benchmark Suite")
    print("=" * 60)
    print(f"  Database : {config.connection.database}")
    print(f"  Schemas  : {[s.name for s in config.schemas]}")
    print(f"  Row counts: {config.get_row_counts()}")
    print(f"  Distributions: {[d.value for d in config.distributions]}")
    print(f"  Patterns : {[p.value for p in config.query_patterns]}")
    print(f"  Iterations: {config.measure_iterations} (warmup: {config.warmup_iterations})")
    print(f"  Output   : {config.output_dir}")
    print("=" * 60)
    print()

    try:
        report = asyncio.run(run_benchmark(config))
    except KeyboardInterrupt:
        print("\nBenchmark interrupted.")
        sys.exit(1)
    except Exception as e:
        logging.error("Benchmark failed: %s", e, exc_info=True)
        sys.exit(1)

    # Print summary
    s = report.summary
    print()
    print("=" * 60)
    print("  RESULTS SUMMARY")
    print("=" * 60)
    if s.get("median_speedup"):
        print(f"  Median query speedup:      {s['median_speedup']:.2f}x")
        print(f"  Best speedup:              {s['max_speedup']:.2f}x")
        print(f"  Worst speedup:             {s['min_speedup']:.2f}x")
    if s.get("avg_compression_ratio"):
        print(f"  Avg compression ratio:     {s['avg_compression_ratio']:.2f}x")
        print(f"  Avg space savings:         {s.get('avg_space_savings_pct', 0):.1f}%")
    if s.get("per_pattern_avg_speedup"):
        print()
        print("  Per-pattern average speedup:")
        for pattern, speedup in sorted(s["per_pattern_avg_speedup"].items()):
            indicator = ">>>" if speedup > 1.0 else "   "
            print(f"    {indicator} {pattern:25s} {speedup:.2f}x")
    if s.get("best_noxu_scenario"):
        best = s["best_noxu_scenario"]
        print()
        print(
            f"  Best Noxu scenario: {best['pattern']} on {best['schema']} "
            f"({best['distribution']}) = {best['speedup']:.2f}x"
        )
    if s.get("worst_noxu_scenario"):
        worst = s["worst_noxu_scenario"]
        print(
            f"  Worst Noxu scenario: {worst['pattern']} on {worst['schema']} "
            f"({worst['distribution']}) = {worst['speedup']:.2f}x"
        )
    print("=" * 60)


def main():
    args = parse_args()

    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    if args.workload == "tprocc":
        run_tprocc(args)
    else:
        run_read_workload(args)


if __name__ == "__main__":
    main()
