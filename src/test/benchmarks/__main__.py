"""
CLI entry point for the benchmark suite.

Supports two modes:
- noxu:   HEAP vs Noxu columnar comparison (original)
- bufpool: buffer replacement algorithm comparison

Usage:
    python -m src.test.benchmarks [OPTIONS]

Examples:
    # Quick run with defaults (buffer pool mode)
    python -m src.test.benchmarks

    # Buffer pool mode, ARC only
    python -m src.test.benchmarks --mode bufpool --strategy arc

    # All strategies, 32MB pools
    python -m src.test.benchmarks --mode bufpool --strategy all --pool-size 33554432

    # Noxu mode (original comparison)
    python -m src.test.benchmarks --mode noxu

    # Custom database and output
    python -m src.test.benchmarks --database mydb --output-dir /tmp/bench

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
    BenchmarkMode,
    BufferStrategy,
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
        description="PostgreSQL Benchmark Suite (Noxu / Buffer Pool)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    # Connection
    parser.add_argument("--host", default=None, help="PostgreSQL host")
    parser.add_argument("--port", type=int, default=None, help="PostgreSQL port")
    parser.add_argument("--database", "-d", default=None, help="Database name")
    parser.add_argument("--user", "-U", default=None, help="Database user")

    # Mode selection
    parser.add_argument(
        "--mode",
        choices=["noxu", "bufpool"],
        default="bufpool",
        help="Benchmark mode: noxu (HEAP vs Noxu) or bufpool (buffer strategy comparison, default)",
    )
    parser.add_argument(
        "--strategy",
        choices=["clock", "arc", "car", "lirs", "lru", "osic", "all"],
        default="all",
        help="Buffer strategies to test (bufpool mode only, default: all)",
    )
    parser.add_argument(
        "--pool-size",
        default="33554432",
        help="Buffer pool size in bytes (bufpool mode only, default: 33554432 = 32MB)",
    )

    # Test matrix
    parser.add_argument(
        "--schema",
        choices=["narrow", "medium", "wide", "all"],
        default="all",
        help="Table schema to test (default: all)",
    )
    parser.add_argument(
        "--rows",
        type=int,
        nargs="+",
        default=None,
        help="Row counts to test (default: 1000 10000 100000)",
    )
    parser.add_argument(
        "--distribution",
        choices=["random", "clustered", "low_cardinality", "high_null", "all"],
        default="all",
        help="Data distribution (default: all)",
    )
    parser.add_argument(
        "--pattern",
        choices=[p.value for p in QueryPattern] + ["all"],
        default="all",
        help="Query pattern to test (default: all)",
    )
    parser.add_argument(
        "--full-matrix",
        action="store_true",
        help="Run full matrix including 10M rows",
    )

    # Execution
    parser.add_argument(
        "--warmup", type=int, default=2, help="Warmup iterations (default: 2)"
    )
    parser.add_argument(
        "--iterations", type=int, default=5, help="Measurement iterations (default: 5)"
    )
    parser.add_argument("--seed", type=int, default=42, help="RNG seed (default: 42)")

    # Output
    parser.add_argument(
        "--output-dir", "-o", default="benchmark_results", help="Output directory"
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Verbose logging"
    )

    return parser.parse_args()


def build_config(args: argparse.Namespace) -> BenchmarkConfig:
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

    # Determine mode and strategies
    mode = BenchmarkMode(args.mode)
    if args.strategy == "all":
        strategies = list(BufferStrategy)
    else:
        strategies = [BufferStrategy(args.strategy)]

    config = BenchmarkConfig(
        connection=conn,
        schemas=schemas,
        distributions=distributions,
        query_patterns=patterns,
        warmup_iterations=args.warmup,
        measure_iterations=args.iterations,
        seed=args.seed,
        output_dir=args.output_dir,
        full_matrix=args.full_matrix,
        verbose=args.verbose,
        mode=mode,
        strategies=strategies,
        pool_size=args.pool_size,
    )

    if args.rows:
        config.row_counts = args.rows

    return config


def _print_noxu_summary(s: dict):
    """Print summary for Noxu comparison mode."""
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


def _print_bufpool_summary(s: dict):
    """Print summary for buffer pool comparison mode."""
    per_strategy = s.get("per_strategy_timings", {})
    if not per_strategy:
        print("  No results collected.")
        return

    print()
    print(f"  {'Strategy':<20} {'Avg (s)':<12} {'Median (s)':<12} {'Hit Ratio':<12} {'Speedup':<10}")
    print(f"  {'-' * 66}")
    for strategy_name, info in sorted(per_strategy.items()):
        avg_t = info.get("avg_time_s", 0)
        med_t = info.get("median_time_s", 0)
        hit_r = info.get("avg_hit_ratio")
        speedup = info.get("speedup_vs_default")
        hr_str = f"{hit_r:.2%}" if hit_r is not None else "N/A"
        sp_str = f"{speedup:.2f}x" if speedup is not None else "-"
        print(f"  {strategy_name:<20} {avg_t:<12.4f} {med_t:<12.4f} {hr_str:<12} {sp_str:<10}")


def main():
    args = parse_args()

    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    config = build_config(args)

    mode_label = (
        "Buffer Pool Strategy Comparison"
        if config.mode == BenchmarkMode.BUFPOOL
        else "Noxu Performance Benchmark"
    )

    print("=" * 60)
    print(f"  {mode_label}")
    print("=" * 60)
    print(f"  Database : {config.connection.database}")
    print(f"  Mode     : {config.mode.value}")
    if config.mode == BenchmarkMode.BUFPOOL:
        print(f"  Strategies: {[s.value for s in config.strategies]}")
        print(f"  Pool size : {config.pool_size} bytes")
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

    if config.mode == BenchmarkMode.BUFPOOL:
        _print_bufpool_summary(s)
    else:
        _print_noxu_summary(s)

    print("=" * 60)


if __name__ == "__main__":
    main()
