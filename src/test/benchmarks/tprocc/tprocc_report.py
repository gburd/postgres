"""TPROC-C result analysis: NOPM calculation, latency percentiles, comparison reports."""

import csv
import json
import logging
import os
import re
import statistics
from dataclasses import dataclass, field
from typing import Dict, List

from .tprocc_config import TproccConfig

logger = logging.getLogger(__name__)


@dataclass
class RunResult:
    """Results from a single pgbench run."""
    am: str                     # "heap" or "recno"
    clients: int
    rep: int
    tps: float                  # total transactions per second (excl. warmup)
    nopm: float                 # new-order per minute
    lat_avg_ms: float           # average latency in ms
    lat_p50_ms: float = 0.0
    lat_p95_ms: float = 0.0
    lat_p99_ms: float = 0.0
    # Per-script breakdown (script_name -> tps)
    per_script_tps: Dict[str, float] = field(default_factory=dict)
    failures: int = 0           # serialization/deadlock failures
    rollbacks: int = 0          # intentional 1% rollbacks (New-Order)
    duration: int = 0
    warmup: int = 0
    raw_output: str = ""


@dataclass
class ComparisonResult:
    """Comparison between HEAP and RECNO at a given concurrency level."""
    clients: int
    heap_tps: float
    recno_tps: float
    heap_nopm: float
    recno_nopm: float
    ratio_tps: float            # recno/heap
    ratio_nopm: float           # recno/heap
    heap_lat_p95: float
    recno_lat_p95: float
    heap_lat_p99: float
    recno_lat_p99: float


def parse_pgbench_output(output: str, am: str, clients: int, rep: int,
                         duration: int, warmup: int) -> RunResult:
    """Parse pgbench stdout to extract TPS, latency, and failure counts."""
    tps = 0.0
    lat_avg = 0.0
    failures = 0

    # Look for the summary line like:
    # tps = 1234.567890 (without initial connection time)
    # or: tps = 1234.567890 (excluding connections establishing)
    for line in output.split("\n"):
        m = re.search(r"tps\s*=\s*([\d.]+)\s*\((?:without initial|excluding)", line)
        if m:
            tps = float(m.group(1))
        # number of failed transactions: 12 (0.023%)
        m = re.search(r"number of failed transactions:\s*(\d+)", line)
        if m:
            failures = int(m.group(1))
        # latency average = 1.234 ms
        m = re.search(r"latency average\s*=\s*([\d.]+)\s*ms", line)
        if m:
            lat_avg = float(m.group(1))

    # NOPM: New-Order is 45% of transactions, convert TPS to per-minute
    nopm = tps * 0.45 * 60.0

    return RunResult(
        am=am,
        clients=clients,
        rep=rep,
        tps=tps,
        nopm=nopm,
        lat_avg_ms=lat_avg,
        failures=failures,
        duration=duration,
        warmup=warmup,
        raw_output=output,
    )


def parse_pgbench_log(log_path: str) -> Dict[str, float]:
    """Parse pgbench --log file to compute latency percentiles.

    pgbench log format: client_id transaction_no time usec script_no time_epoch time_us [schedule_lag]
    Returns dict with p50, p95, p99 in milliseconds.
    """
    latencies = []
    try:
        with open(log_path) as f:
            for line in f:
                parts = line.strip().split()
                if len(parts) >= 4:
                    try:
                        usec = int(parts[2])
                        latencies.append(usec / 1000.0)  # convert to ms
                    except (ValueError, IndexError):
                        continue
    except FileNotFoundError:
        logger.warning("Log file not found: %s", log_path)
        return {"p50": 0, "p95": 0, "p99": 0}

    if not latencies:
        return {"p50": 0, "p95": 0, "p99": 0}

    latencies.sort()
    n = len(latencies)
    return {
        "p50": latencies[int(n * 0.50)],
        "p95": latencies[int(n * 0.95)],
        "p99": latencies[int(n * 0.99)],
    }


def compute_comparisons(results: List[RunResult]) -> List[ComparisonResult]:
    """Compute HEAP vs RECNO comparisons per concurrency level."""
    # Group by clients
    by_clients: Dict[int, Dict[str, List[RunResult]]] = {}
    for r in results:
        by_clients.setdefault(r.clients, {}).setdefault(r.am, []).append(r)

    comparisons = []
    for clients in sorted(by_clients.keys()):
        groups = by_clients[clients]
        heap_runs = groups.get("heap", [])
        recno_runs = groups.get("recno", [])

        if not heap_runs or not recno_runs:
            continue

        # Use median TPS across repetitions
        heap_tps = statistics.median(r.tps for r in heap_runs)
        recno_tps = statistics.median(r.tps for r in recno_runs)
        heap_nopm = statistics.median(r.nopm for r in heap_runs)
        recno_nopm = statistics.median(r.nopm for r in recno_runs)
        heap_p95 = statistics.median(r.lat_p95_ms for r in heap_runs)
        recno_p95 = statistics.median(r.lat_p95_ms for r in recno_runs)
        heap_p99 = statistics.median(r.lat_p99_ms for r in heap_runs)
        recno_p99 = statistics.median(r.lat_p99_ms for r in recno_runs)

        ratio_tps = recno_tps / heap_tps if heap_tps > 0 else 0
        ratio_nopm = recno_nopm / heap_nopm if heap_nopm > 0 else 0

        comparisons.append(ComparisonResult(
            clients=clients,
            heap_tps=heap_tps,
            recno_tps=recno_tps,
            heap_nopm=heap_nopm,
            recno_nopm=recno_nopm,
            ratio_tps=ratio_tps,
            ratio_nopm=ratio_nopm,
            heap_lat_p95=heap_p95,
            recno_lat_p95=recno_p95,
            heap_lat_p99=heap_p99,
            recno_lat_p99=recno_p99,
        ))

    return comparisons


def write_csv(results: List[RunResult], output_path: str) -> None:
    """Write results to CSV."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "am", "clients", "rep", "tps", "nopm",
            "lat_avg_ms", "lat_p50_ms", "lat_p95_ms", "lat_p99_ms",
        ])
        for r in results:
            writer.writerow([
                r.am, r.clients, r.rep, f"{r.tps:.2f}", f"{r.nopm:.1f}",
                f"{r.lat_avg_ms:.3f}", f"{r.lat_p50_ms:.3f}",
                f"{r.lat_p95_ms:.3f}", f"{r.lat_p99_ms:.3f}",
            ])
    logger.info("CSV written: %s", output_path)


def write_json_report(results: List[RunResult], comparisons: List[ComparisonResult],
                      config: TproccConfig, output_path: str) -> None:
    """Write machine-readable JSON report."""
    report = {
        "config": {
            "warehouses": config.warehouses,
            "duration": config.duration,
            "warmup": config.warmup,
            "reps": config.reps,
            "clients": config.clients,
        },
        "results": [
            {
                "am": r.am, "clients": r.clients, "rep": r.rep,
                "tps": r.tps, "nopm": r.nopm,
                "lat_avg_ms": r.lat_avg_ms, "lat_p50_ms": r.lat_p50_ms,
                "lat_p95_ms": r.lat_p95_ms, "lat_p99_ms": r.lat_p99_ms,
            }
            for r in results
        ],
        "comparisons": [
            {
                "clients": c.clients,
                "heap_tps": c.heap_tps, "recno_tps": c.recno_tps,
                "heap_nopm": c.heap_nopm, "recno_nopm": c.recno_nopm,
                "ratio_tps": c.ratio_tps, "ratio_nopm": c.ratio_nopm,
                "heap_lat_p95": c.heap_lat_p95, "recno_lat_p95": c.recno_lat_p95,
                "heap_lat_p99": c.heap_lat_p99, "recno_lat_p99": c.recno_lat_p99,
            }
            for c in comparisons
        ],
    }
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(report, f, indent=2)
    logger.info("JSON report written: %s", output_path)


def generate_summary(results: List[RunResult], comparisons: List[ComparisonResult],
                     config: TproccConfig) -> str:
    """Generate human-readable summary text."""
    lines = []
    lines.append("=" * 70)
    lines.append("  TPROC-C Benchmark Results: HEAP vs RECNO")
    lines.append("=" * 70)
    lines.append(f"  Warehouses: {config.warehouses}")
    lines.append(f"  Duration: {config.duration}s + {config.warmup}s warmup")
    lines.append(f"  Repetitions: {config.reps}")
    lines.append("")

    # Main comparison table
    lines.append("  {:>8s}  {:>10s}  {:>10s}  {:>8s}  {:>10s}  {:>10s}  {:>8s}".format(
        "Clients", "HEAP TPS", "RECNO TPS", "Ratio", "HEAP NOPM", "RECNO NOPM", "Ratio"))
    lines.append("  " + "-" * 68)

    for c in comparisons:
        lines.append("  {:>8d}  {:>10.1f}  {:>10.1f}  {:>7.1f}%  {:>10.0f}  {:>10.0f}  {:>7.1f}%".format(
            c.clients, c.heap_tps, c.recno_tps, c.ratio_tps * 100,
            c.heap_nopm, c.recno_nopm, c.ratio_nopm * 100))

    lines.append("")

    # Latency comparison
    lines.append("  Latency Percentiles (ms):")
    lines.append("  {:>8s}  {:>10s}  {:>10s}  {:>10s}  {:>10s}".format(
        "Clients", "HEAP P95", "RECNO P95", "HEAP P99", "RECNO P99"))
    lines.append("  " + "-" * 52)
    for c in comparisons:
        lines.append("  {:>8d}  {:>10.2f}  {:>10.2f}  {:>10.2f}  {:>10.2f}".format(
            c.clients, c.heap_lat_p95, c.recno_lat_p95, c.heap_lat_p99, c.recno_lat_p99))

    lines.append("")

    # Scaling analysis
    if len(comparisons) >= 2:
        first = comparisons[0]
        last = comparisons[-1]
        heap_scale = last.heap_tps / first.heap_tps if first.heap_tps > 0 else 0
        recno_scale = last.recno_tps / first.recno_tps if first.recno_tps > 0 else 0
        lines.append(f"  Scaling (c={first.clients} -> c={last.clients}):")
        lines.append(f"    HEAP:  {heap_scale:.2f}x")
        lines.append(f"    RECNO: {recno_scale:.2f}x")
        lines.append("")

    # Per-AM summary across all client counts
    heap_results = [r for r in results if r.am == "heap"]
    recno_results = [r for r in results if r.am == "recno"]
    if heap_results and recno_results:
        peak_heap = max(r.tps for r in heap_results)
        peak_recno = max(r.tps for r in recno_results)
        lines.append(f"  Peak TPS:  HEAP={peak_heap:.1f}  RECNO={peak_recno:.1f}  ratio={peak_recno/peak_heap*100:.1f}%")
        peak_heap_nopm = max(r.nopm for r in heap_results)
        peak_recno_nopm = max(r.nopm for r in recno_results)
        lines.append(f"  Peak NOPM: HEAP={peak_heap_nopm:.0f}  RECNO={peak_recno_nopm:.0f}  ratio={peak_recno_nopm/peak_heap_nopm*100:.1f}%")

    lines.append("")
    lines.append("=" * 70)
    return "\n".join(lines)
