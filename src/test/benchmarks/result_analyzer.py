"""
Statistical analysis of benchmark results: mean, median, p95, p99,
standard deviation, speedup ratios, and confidence intervals.
"""

import math
import statistics
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from .workload_runner import QueryResult, WorkloadResult
from .metrics_collector import BenchmarkMetrics, StorageMetrics


@dataclass
class TimingSummary:
    """Statistical summary of timing measurements."""
    values: List[float]
    mean: float = 0.0
    median: float = 0.0
    stdev: float = 0.0
    p95: float = 0.0
    p99: float = 0.0
    min_val: float = 0.0
    max_val: float = 0.0

    def __post_init__(self):
        if self.values:
            self.mean = statistics.mean(self.values)
            self.median = statistics.median(self.values)
            self.stdev = statistics.stdev(self.values) if len(self.values) > 1 else 0.0
            self.min_val = min(self.values)
            self.max_val = max(self.values)
            self.p95 = self._percentile(95)
            self.p99 = self._percentile(99)

    def _percentile(self, p: float) -> float:
        if not self.values:
            return 0.0
        sorted_vals = sorted(self.values)
        k = (len(sorted_vals) - 1) * (p / 100.0)
        f = math.floor(k)
        c = math.ceil(k)
        if f == c:
            return sorted_vals[int(k)]
        return sorted_vals[f] * (c - k) + sorted_vals[c] * (k - f)


@dataclass
class ComparisonResult:
    """Comparison between HEAP and Orvos for a single query pattern."""
    query_pattern: str
    schema_name: str
    row_count: int
    distribution: str
    heap_timing: TimingSummary
    orvos_timing: TimingSummary
    speedup: float = 0.0  # > 1.0 means orvos is faster
    heap_rows: int = 0
    orvos_rows: int = 0

    def __post_init__(self):
        if self.orvos_timing.median > 0:
            self.speedup = self.heap_timing.median / self.orvos_timing.median
        elif self.heap_timing.median > 0:
            self.speedup = float("inf")


@dataclass
class StorageComparison:
    """Storage size comparison between HEAP and Orvos."""
    schema_name: str
    row_count: int
    distribution: str
    heap_table_bytes: int = 0
    heap_index_bytes: int = 0
    heap_total_bytes: int = 0
    orvos_table_bytes: int = 0
    orvos_index_bytes: int = 0
    orvos_total_bytes: int = 0
    compression_ratio: float = 1.0

    @property
    def space_savings_pct(self) -> float:
        if self.heap_total_bytes == 0:
            return 0.0
        return (1.0 - self.orvos_total_bytes / self.heap_total_bytes) * 100


@dataclass
class AnalysisReport:
    """Complete analysis report for a benchmark suite run."""
    comparisons: List[ComparisonResult] = field(default_factory=list)
    storage_comparisons: List[StorageComparison] = field(default_factory=list)
    per_column_compression: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    summary: Dict[str, Any] = field(default_factory=dict)


class ResultAnalyzer:
    """Analyzes raw benchmark results into statistical summaries."""

    def analyze_workload_pair(
        self,
        heap_result: WorkloadResult,
        orvos_result: WorkloadResult,
    ) -> List[ComparisonResult]:
        """Compare HEAP and Orvos workload results per query pattern."""
        comparisons = []

        # Group results by query pattern
        heap_by_pattern: Dict[str, List[QueryResult]] = {}
        for qr in heap_result.results:
            heap_by_pattern.setdefault(qr.query_pattern, []).append(qr)

        orvos_by_pattern: Dict[str, List[QueryResult]] = {}
        for qr in orvos_result.results:
            orvos_by_pattern.setdefault(qr.query_pattern, []).append(qr)

        all_patterns = set(heap_by_pattern.keys()) | set(orvos_by_pattern.keys())
        for pattern in sorted(all_patterns):
            heap_timings = [qr.elapsed_seconds for qr in heap_by_pattern.get(pattern, [])]
            orvos_timings = [
                qr.elapsed_seconds for qr in orvos_by_pattern.get(pattern, [])
            ]

            heap_rows = 0
            orvos_rows = 0
            if heap_by_pattern.get(pattern):
                heap_rows = heap_by_pattern[pattern][-1].row_count
            if orvos_by_pattern.get(pattern):
                orvos_rows = orvos_by_pattern[pattern][-1].row_count

            comp = ComparisonResult(
                query_pattern=pattern,
                schema_name=heap_result.schema_name,
                row_count=heap_result.row_count,
                distribution=heap_result.distribution,
                heap_timing=TimingSummary(heap_timings or [0.0]),
                orvos_timing=TimingSummary(orvos_timings or [0.0]),
                heap_rows=heap_rows,
                orvos_rows=orvos_rows,
            )
            comparisons.append(comp)

        return comparisons

    def analyze_storage(
        self, metrics: BenchmarkMetrics
    ) -> StorageComparison:
        """Create storage comparison from benchmark metrics."""
        sc = StorageComparison(
            schema_name=metrics.schema_name,
            row_count=metrics.row_count,
            distribution=metrics.distribution,
        )
        if metrics.heap_storage:
            sc.heap_table_bytes = metrics.heap_storage.table_size_bytes
            sc.heap_index_bytes = metrics.heap_storage.index_size_bytes
            sc.heap_total_bytes = metrics.heap_storage.total_size_bytes
        if metrics.orvos_storage:
            sc.orvos_table_bytes = metrics.orvos_storage.table_size_bytes
            sc.orvos_index_bytes = metrics.orvos_storage.index_size_bytes
            sc.orvos_total_bytes = metrics.orvos_storage.total_size_bytes
        sc.compression_ratio = metrics.compression_ratio
        return sc

    def analyze_compression_per_column(
        self, metrics: BenchmarkMetrics
    ) -> Dict[str, Dict[str, Any]]:
        """Analyze per-column compression characteristics."""
        result = {}
        heap_stats = metrics.compression_stats.get("heap", {})
        orvos_stats = metrics.compression_stats.get("orvos", {})

        all_cols = set(heap_stats.keys()) | set(orvos_stats.keys())
        for col in sorted(all_cols):
            h = heap_stats.get(col, {})
            o = orvos_stats.get(col, {})
            col_analysis = {
                "column_type": h.get("column_type", o.get("column_type", "unknown")),
                "heap_avg_width": h.get("avg_width", 0),
                "orvos_avg_width": o.get("avg_width", 0),
                "heap_n_distinct": h.get("n_distinct", 0),
                "orvos_n_distinct": o.get("n_distinct", 0),
                "heap_null_fraction": h.get("null_fraction", 0),
                "orvos_null_fraction": o.get("null_fraction", 0),
            }
            # Width reduction ratio
            if h.get("avg_width", 0) > 0 and o.get("avg_width", 0) > 0:
                col_analysis["width_ratio"] = h["avg_width"] / o["avg_width"]
            result[col] = col_analysis
        return result

    def build_report(
        self,
        workload_pairs: List[tuple],  # [(heap_result, orvos_result), ...]
        metrics_list: List[BenchmarkMetrics],
    ) -> AnalysisReport:
        """Build a complete analysis report from all collected data."""
        report = AnalysisReport()

        for heap_wr, orvos_wr in workload_pairs:
            comps = self.analyze_workload_pair(heap_wr, orvos_wr)
            report.comparisons.extend(comps)

        for metrics in metrics_list:
            sc = self.analyze_storage(metrics)
            report.storage_comparisons.append(sc)
            col_comp = self.analyze_compression_per_column(metrics)
            key = f"{metrics.schema_name}_{metrics.row_count}_{metrics.distribution}"
            report.per_column_compression[key] = col_comp

        # Build summary
        report.summary = self._build_summary(report)
        return report

    def _build_summary(self, report: AnalysisReport) -> Dict[str, Any]:
        """Generate high-level summary statistics."""
        summary: Dict[str, Any] = {}

        if report.comparisons:
            speedups = [c.speedup for c in report.comparisons if c.speedup != float("inf")]
            if speedups:
                summary["avg_speedup"] = statistics.mean(speedups)
                summary["median_speedup"] = statistics.median(speedups)
                summary["max_speedup"] = max(speedups)
                summary["min_speedup"] = min(speedups)

            # Per-pattern averages
            pattern_speedups: Dict[str, List[float]] = {}
            for c in report.comparisons:
                if c.speedup != float("inf"):
                    pattern_speedups.setdefault(c.query_pattern, []).append(c.speedup)
            summary["per_pattern_avg_speedup"] = {
                p: statistics.mean(v) for p, v in pattern_speedups.items()
            }

        if report.storage_comparisons:
            ratios = [
                sc.compression_ratio
                for sc in report.storage_comparisons
                if sc.compression_ratio > 0
            ]
            if ratios:
                summary["avg_compression_ratio"] = statistics.mean(ratios)
                summary["max_compression_ratio"] = max(ratios)
                summary["min_compression_ratio"] = min(ratios)

            savings = [sc.space_savings_pct for sc in report.storage_comparisons]
            if savings:
                summary["avg_space_savings_pct"] = statistics.mean(savings)

        # Identify best/worst scenarios for Orvos
        if report.comparisons:
            best = max(report.comparisons, key=lambda c: c.speedup if c.speedup != float("inf") else 0)
            worst = min(report.comparisons, key=lambda c: c.speedup)
            summary["best_orvos_scenario"] = {
                "pattern": best.query_pattern,
                "schema": best.schema_name,
                "distribution": best.distribution,
                "speedup": best.speedup,
            }
            summary["worst_orvos_scenario"] = {
                "pattern": worst.query_pattern,
                "schema": worst.schema_name,
                "distribution": worst.distribution,
                "speedup": worst.speedup,
            }

        return summary
