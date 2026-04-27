"""
Visualization: generates matplotlib charts and an HTML dashboard
from benchmark analysis results.

Supports both Noxu comparison dashboards and buffer pool strategy
comparison dashboards.
"""

import html
import json
import logging
import os
from typing import Any, Dict, List, Optional

from .result_analyzer import AnalysisReport, ComparisonResult, StorageComparison

logger = logging.getLogger(__name__)

# Try importing matplotlib; gracefully degrade if missing
try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.ticker as ticker
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False
    logger.info("matplotlib not available; chart generation will be skipped")


def _human_bytes(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024  # type: ignore
    return f"{n:.1f} PB"


class Visualizer:
    """Generates charts and HTML dashboard from benchmark results."""

    def __init__(self, output_dir: str):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)

    # ------------------------------------------------------------------
    # Chart generation (requires matplotlib)
    # ------------------------------------------------------------------

    def _save_fig(self, fig, name: str) -> str:
        path = os.path.join(self.output_dir, name)
        fig.savefig(path, dpi=120, bbox_inches="tight")
        plt.close(fig)
        logger.info("Saved chart: %s", path)
        return name

    def generate_speedup_chart(
        self, comparisons: List[ComparisonResult]
    ) -> Optional[str]:
        """Bar chart of speedup ratios by query pattern."""
        if not HAS_MATPLOTLIB or not comparisons:
            return None

        patterns = sorted(set(c.query_pattern for c in comparisons))
        # Average speedup per pattern across all schemas/distributions
        avg_speedups = []
        for p in patterns:
            vals = [c.speedup for c in comparisons if c.query_pattern == p and c.speedup != float("inf")]
            avg_speedups.append(sum(vals) / len(vals) if vals else 1.0)

        fig, ax = plt.subplots(figsize=(10, 6))
        colors = ["#2ecc71" if s > 1.0 else "#e74c3c" for s in avg_speedups]
        bars = ax.barh(patterns, avg_speedups, color=colors)
        ax.axvline(x=1.0, color="black", linestyle="--", linewidth=0.8, label="HEAP baseline")
        ax.set_xlabel("Speedup (Noxu / HEAP)")
        ax.set_title("Query Performance: Noxu vs HEAP")

        for bar, val in zip(bars, avg_speedups):
            ax.text(
                bar.get_width() + 0.05,
                bar.get_y() + bar.get_height() / 2,
                f"{val:.2f}x",
                va="center",
                fontsize=9,
            )

        ax.legend()
        fig.tight_layout()
        return self._save_fig(fig, "speedup_by_pattern.png")

    def generate_storage_chart(
        self, storage_comps: List[StorageComparison]
    ) -> Optional[str]:
        """Grouped bar chart comparing HEAP and Noxu storage sizes."""
        if not HAS_MATPLOTLIB or not storage_comps:
            return None

        labels = [
            f"{sc.schema_name}\n{sc.row_count:,} rows\n{sc.distribution}"
            for sc in storage_comps
        ]
        heap_sizes = [sc.heap_total_bytes / (1024 * 1024) for sc in storage_comps]
        noxu_sizes = [sc.noxu_total_bytes / (1024 * 1024) for sc in storage_comps]

        fig, ax = plt.subplots(figsize=(max(8, len(labels) * 2), 6))
        x = range(len(labels))
        width = 0.35
        ax.bar([i - width / 2 for i in x], heap_sizes, width, label="HEAP", color="#3498db")
        ax.bar([i + width / 2 for i in x], noxu_sizes, width, label="Noxu", color="#2ecc71")

        ax.set_ylabel("Total Size (MB)")
        ax.set_title("Storage Comparison: HEAP vs Noxu")
        ax.set_xticks(list(x))
        ax.set_xticklabels(labels, fontsize=8)
        ax.legend()

        # Annotate compression ratio
        for i, sc in enumerate(storage_comps):
            ax.text(
                i, max(heap_sizes[i], noxu_sizes[i]) + 0.5,
                f"{sc.compression_ratio:.1f}x",
                ha="center", fontsize=9, fontweight="bold",
            )

        fig.tight_layout()
        return self._save_fig(fig, "storage_comparison.png")

    def generate_latency_heatmap(
        self, comparisons: List[ComparisonResult]
    ) -> Optional[str]:
        """Heatmap of median latencies across schemas and query patterns."""
        if not HAS_MATPLOTLIB or not comparisons:
            return None

        schemas = sorted(set(c.schema_name for c in comparisons))
        patterns = sorted(set(c.query_pattern for c in comparisons))

        data = []
        for schema in schemas:
            row = []
            for pattern in patterns:
                vals = [
                    c.speedup
                    for c in comparisons
                    if c.schema_name == schema and c.query_pattern == pattern
                    and c.speedup != float("inf")
                ]
                row.append(sum(vals) / len(vals) if vals else 1.0)
            data.append(row)

        fig, ax = plt.subplots(figsize=(max(8, len(patterns) * 1.5), max(4, len(schemas) * 1.5)))
        im = ax.imshow(data, cmap="RdYlGn", aspect="auto", vmin=0.5, vmax=3.0)
        ax.set_xticks(range(len(patterns)))
        ax.set_xticklabels(patterns, rotation=45, ha="right", fontsize=8)
        ax.set_yticks(range(len(schemas)))
        ax.set_yticklabels(schemas, fontsize=9)
        ax.set_title("Speedup Heatmap (green = Noxu faster)")

        for i in range(len(schemas)):
            for j in range(len(patterns)):
                ax.text(j, i, f"{data[i][j]:.2f}x", ha="center", va="center", fontsize=8)

        fig.colorbar(im, ax=ax, label="Speedup (Noxu/HEAP)")
        fig.tight_layout()
        return self._save_fig(fig, "speedup_heatmap.png")

    def generate_compression_chart(
        self, report: AnalysisReport
    ) -> Optional[str]:
        """Bar chart of per-column compression width ratios."""
        if not HAS_MATPLOTLIB or not report.per_column_compression:
            return None

        # Take the first config's per-column data
        first_key = next(iter(report.per_column_compression))
        col_data = report.per_column_compression[first_key]

        cols = sorted(col_data.keys())
        heap_widths = [col_data[c].get("heap_avg_width", 0) for c in cols]
        noxu_widths = [col_data[c].get("noxu_avg_width", 0) for c in cols]

        fig, ax = plt.subplots(figsize=(max(8, len(cols)), 6))
        x = range(len(cols))
        width = 0.35
        ax.bar([i - width / 2 for i in x], heap_widths, width, label="HEAP avg_width", color="#3498db")
        ax.bar([i + width / 2 for i in x], noxu_widths, width, label="Noxu avg_width", color="#2ecc71")

        ax.set_ylabel("Average Width (bytes)")
        ax.set_title(f"Per-Column Average Width: {first_key}")
        ax.set_xticks(list(x))
        ax.set_xticklabels(cols, rotation=45, ha="right", fontsize=8)
        ax.legend()
        fig.tight_layout()
        return self._save_fig(fig, "column_compression.png")

    # ------------------------------------------------------------------
    # CSV export
    # ------------------------------------------------------------------

    def export_csv(self, report: AnalysisReport) -> str:
        """Export benchmark results to CSV files. Returns path to main CSV."""
        import csv

        # Query timing comparisons
        timing_path = os.path.join(self.output_dir, "timing_results.csv")
        with open(timing_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                "schema", "row_count", "distribution", "query_pattern",
                "heap_median_s", "noxu_median_s", "speedup",
                "heap_p95_s", "noxu_p95_s",
                "heap_mean_s", "noxu_mean_s",
            ])
            for c in report.comparisons:
                writer.writerow([
                    c.schema_name, c.row_count, c.distribution, c.query_pattern,
                    f"{c.heap_timing.median:.6f}",
                    f"{c.noxu_timing.median:.6f}",
                    f"{c.speedup:.4f}",
                    f"{c.heap_timing.p95:.6f}",
                    f"{c.noxu_timing.p95:.6f}",
                    f"{c.heap_timing.mean:.6f}",
                    f"{c.noxu_timing.mean:.6f}",
                ])

        # Storage comparisons
        storage_path = os.path.join(self.output_dir, "storage_results.csv")
        with open(storage_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                "schema", "row_count", "distribution",
                "heap_table_bytes", "heap_index_bytes", "heap_total_bytes",
                "noxu_table_bytes", "noxu_index_bytes", "noxu_total_bytes",
                "compression_ratio", "space_savings_pct",
            ])
            for sc in report.storage_comparisons:
                writer.writerow([
                    sc.schema_name, sc.row_count, sc.distribution,
                    sc.heap_table_bytes, sc.heap_index_bytes, sc.heap_total_bytes,
                    sc.noxu_table_bytes, sc.noxu_index_bytes, sc.noxu_total_bytes,
                    f"{sc.compression_ratio:.4f}",
                    f"{sc.space_savings_pct:.2f}",
                ])

        # Per-column compression
        col_path = os.path.join(self.output_dir, "column_compression.csv")
        with open(col_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                "config", "column", "type",
                "heap_avg_width", "noxu_avg_width", "width_ratio",
                "heap_n_distinct", "noxu_n_distinct",
            ])
            for config_key, cols in report.per_column_compression.items():
                for col_name, stats in cols.items():
                    writer.writerow([
                        config_key, col_name,
                        stats.get("column_type", ""),
                        stats.get("heap_avg_width", ""),
                        stats.get("noxu_avg_width", ""),
                        f"{stats.get('width_ratio', 0):.4f}" if stats.get("width_ratio") else "",
                        stats.get("heap_n_distinct", ""),
                        stats.get("noxu_n_distinct", ""),
                    ])

        logger.info("CSV files written to %s", self.output_dir)
        return timing_path

    # ------------------------------------------------------------------
    # HTML dashboard
    # ------------------------------------------------------------------

    def generate_recommendations(self, report: AnalysisReport) -> list:
        """Generate optimization recommendations based on benchmark results."""
        recs = []
        summary = report.summary

        # Recommendation 1: Column projection performance
        per_pattern = summary.get("per_pattern_avg_speedup", {})
        proj_speedup = per_pattern.get("column_projection", 1.0)
        if proj_speedup < 1.2:
            recs.append({
                "priority": "HIGH",
                "area": "Column Projection",
                "finding": f"Column projection speedup is only {proj_speedup:.2f}x over HEAP.",
                "recommendation": (
                    "Investigate column-skip efficiency. Noxu should show large "
                    "gains for narrow projections on wide tables. Check that "
                    "non-projected columns are truly not read from disk."
                ),
            })
        elif proj_speedup > 2.0:
            recs.append({
                "priority": "INFO",
                "area": "Column Projection",
                "finding": f"Column projection shows strong {proj_speedup:.2f}x speedup.",
                "recommendation": "This is a key Noxu advantage. Highlight in documentation.",
            })

        # Recommendation 2: Aggregation performance
        agg_speedup = per_pattern.get("aggregation", 1.0)
        if agg_speedup < 1.0:
            recs.append({
                "priority": "HIGH",
                "area": "Aggregation",
                "finding": f"Aggregation is {agg_speedup:.2f}x vs HEAP (slower).",
                "recommendation": (
                    "Columnar storage should excel at aggregations. Check for "
                    "unnecessary tuple reconstruction and decompression overhead "
                    "in the aggregation path."
                ),
            })

        # Recommendation 3: Compression ratio
        avg_comp = summary.get("avg_compression_ratio", 1.0)
        if avg_comp < 1.5:
            recs.append({
                "priority": "MEDIUM",
                "area": "Compression",
                "finding": f"Average compression ratio is only {avg_comp:.2f}x.",
                "recommendation": (
                    "Consider implementing additional compression strategies: "
                    "dictionary encoding for low-cardinality text, RLE for "
                    "clustered data, and delta encoding for sorted integers."
                ),
            })

        # Recommendation 4: Full scan overhead
        full_scan_speedup = per_pattern.get("full_scan", 1.0)
        if full_scan_speedup < 0.8:
            recs.append({
                "priority": "MEDIUM",
                "area": "Full Table Scan",
                "finding": f"Full scan is {full_scan_speedup:.2f}x vs HEAP (regression).",
                "recommendation": (
                    "Full scans that read all columns should be close to HEAP "
                    "performance. The overhead suggests tuple reconstruction cost "
                    "is significant. Consider optimizing the column-to-tuple "
                    "assembly path."
                ),
            })

        # Recommendation 5: Index scan performance
        idx_speedup = per_pattern.get("index_scan", 1.0)
        if idx_speedup < 0.9:
            recs.append({
                "priority": "MEDIUM",
                "area": "Index Scan",
                "finding": f"Index scan is {idx_speedup:.2f}x vs HEAP (regression).",
                "recommendation": (
                    "Point lookups via index should not regress. Check that "
                    "TID-to-column-page mapping is efficient and does not "
                    "require scanning through column pages sequentially."
                ),
            })

        # Recommendation 6: Storage efficiency per data type
        for config_key, col_data in report.per_column_compression.items():
            for col_name, stats in col_data.items():
                ratio = stats.get("width_ratio", 0)
                col_type = stats.get("column_type", "")
                if ratio > 0 and ratio < 1.0:
                    recs.append({
                        "priority": "LOW",
                        "area": f"Column Storage ({col_name})",
                        "finding": (
                            f"Column '{col_name}' ({col_type}) has width ratio "
                            f"{ratio:.2f} (Noxu wider than HEAP)."
                        ),
                        "recommendation": (
                            f"Investigate per-column overhead for {col_type} type. "
                            "The columnar format should not be wider than HEAP."
                        ),
                    })
            break  # Only check first configuration

        # If no issues found, add a positive recommendation
        if not recs:
            recs.append({
                "priority": "INFO",
                "area": "Overall",
                "finding": "Benchmark results look good across all patterns.",
                "recommendation": (
                    "Continue with larger dataset sizes to identify scaling behavior."
                ),
            })

        return recs

    def generate_dashboard(self, report: AnalysisReport) -> str:
        """Generate a self-contained HTML dashboard. Returns path to HTML file."""
        charts = {}
        if HAS_MATPLOTLIB:
            charts["speedup"] = self.generate_speedup_chart(report.comparisons)
            charts["storage"] = self.generate_storage_chart(report.storage_comparisons)
            charts["heatmap"] = self.generate_latency_heatmap(report.comparisons)
            charts["compression"] = self.generate_compression_chart(report)

        recommendations = self.generate_recommendations(report)
        html_content = self._render_html(report, charts, recommendations)
        path = os.path.join(self.output_dir, "dashboard.html")
        with open(path, "w") as f:
            f.write(html_content)
        logger.info("Dashboard written to %s", path)
        return path

    def _render_html(
        self, report: AnalysisReport, charts: Dict[str, Optional[str]],
        recommendations: Optional[list] = None,
    ) -> str:
        summary = report.summary

        # Build timing table
        timing_rows = ""
        for c in report.comparisons:
            color = "#2ecc71" if c.speedup > 1.0 else "#e74c3c"
            timing_rows += f"""
            <tr>
                <td>{html.escape(c.schema_name)}</td>
                <td>{c.row_count:,}</td>
                <td>{html.escape(c.distribution)}</td>
                <td>{html.escape(c.query_pattern)}</td>
                <td>{c.heap_timing.median * 1000:.2f}</td>
                <td>{c.noxu_timing.median * 1000:.2f}</td>
                <td style="color: {color}; font-weight: bold;">{c.speedup:.2f}x</td>
            </tr>"""

        # Build storage table
        storage_rows = ""
        for sc in report.storage_comparisons:
            storage_rows += f"""
            <tr>
                <td>{html.escape(sc.schema_name)}</td>
                <td>{sc.row_count:,}</td>
                <td>{html.escape(sc.distribution)}</td>
                <td>{_human_bytes(sc.heap_total_bytes)}</td>
                <td>{_human_bytes(sc.noxu_total_bytes)}</td>
                <td style="font-weight: bold;">{sc.compression_ratio:.2f}x</td>
                <td>{sc.space_savings_pct:.1f}%</td>
            </tr>"""

        # Chart image tags
        def img_tag(name: Optional[str]) -> str:
            if name:
                return f'<img src="{html.escape(name)}" style="max-width:100%;margin:10px 0;">'
            return '<p style="color:#999;">Chart not available (matplotlib not installed)</p>'

        summary_json = html.escape(json.dumps(summary, indent=2, default=str))

        # Build recommendations HTML
        rec_rows = ""
        if recommendations:
            priority_colors = {
                "HIGH": "#e74c3c",
                "MEDIUM": "#f39c12",
                "LOW": "#3498db",
                "INFO": "#2ecc71",
            }
            for rec in recommendations:
                color = priority_colors.get(rec["priority"], "#999")
                rec_rows += f"""
            <tr>
                <td style="color: {color}; font-weight: bold;">{html.escape(rec['priority'])}</td>
                <td>{html.escape(rec['area'])}</td>
                <td>{html.escape(rec['finding'])}</td>
                <td>{html.escape(rec['recommendation'])}</td>
            </tr>"""

        return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Noxu Benchmark Dashboard</title>
<style>
  body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
         max-width: 1200px; margin: 0 auto; padding: 20px; background: #f5f5f5; }}
  h1 {{ color: #2c3e50; border-bottom: 3px solid #3498db; padding-bottom: 10px; }}
  h2 {{ color: #34495e; margin-top: 30px; }}
  .card {{ background: white; border-radius: 8px; padding: 20px; margin: 15px 0;
           box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
  .summary-grid {{ display: grid; grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
                   gap: 15px; }}
  .metric {{ text-align: center; padding: 15px; background: #ecf0f1; border-radius: 6px; }}
  .metric .value {{ font-size: 2em; font-weight: bold; color: #2c3e50; }}
  .metric .label {{ font-size: 0.85em; color: #7f8c8d; margin-top: 5px; }}
  table {{ border-collapse: collapse; width: 100%; }}
  th, td {{ border: 1px solid #ddd; padding: 8px 12px; text-align: right; }}
  th {{ background: #3498db; color: white; }}
  tr:nth-child(even) {{ background: #f9f9f9; }}
  tr:hover {{ background: #eef; }}
  td:first-child, td:nth-child(3), td:nth-child(4) {{ text-align: left; }}
  pre {{ background: #2c3e50; color: #ecf0f1; padding: 15px; border-radius: 6px;
         overflow-x: auto; font-size: 0.85em; }}
  .charts {{ display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }}
  @media (max-width: 800px) {{ .charts {{ grid-template-columns: 1fr; }} }}
</style>
</head>
<body>
<h1>Noxu Benchmark Dashboard</h1>

<div class="card">
<h2>Summary</h2>
<div class="summary-grid">
  <div class="metric">
    <div class="value">{summary.get('median_speedup', 0):.2f}x</div>
    <div class="label">Median Query Speedup</div>
  </div>
  <div class="metric">
    <div class="value">{summary.get('max_speedup', 0):.2f}x</div>
    <div class="label">Best Speedup</div>
  </div>
  <div class="metric">
    <div class="value">{summary.get('avg_compression_ratio', 0):.2f}x</div>
    <div class="label">Avg Compression Ratio</div>
  </div>
  <div class="metric">
    <div class="value">{summary.get('avg_space_savings_pct', 0):.1f}%</div>
    <div class="label">Avg Space Savings</div>
  </div>
</div>
</div>

<div class="card">
<h2>Charts</h2>
<div class="charts">
  <div>{img_tag(charts.get("speedup"))}</div>
  <div>{img_tag(charts.get("storage"))}</div>
  <div>{img_tag(charts.get("heatmap"))}</div>
  <div>{img_tag(charts.get("compression"))}</div>
</div>
</div>

<div class="card">
<h2>Query Timing Comparison</h2>
<table>
<thead>
<tr>
  <th>Schema</th><th>Rows</th><th>Distribution</th><th>Pattern</th>
  <th>HEAP (ms)</th><th>Noxu (ms)</th><th>Speedup</th>
</tr>
</thead>
<tbody>
{timing_rows}
</tbody>
</table>
</div>

<div class="card">
<h2>Storage Comparison</h2>
<table>
<thead>
<tr>
  <th>Schema</th><th>Rows</th><th>Distribution</th>
  <th>HEAP Total</th><th>Noxu Total</th><th>Compression</th><th>Savings</th>
</tr>
</thead>
<tbody>
{storage_rows}
</tbody>
</table>
</div>

<div class="card">
<h2>Optimization Recommendations</h2>
<table>
<thead>
<tr>
  <th style="width:80px;">Priority</th><th style="width:150px;">Area</th>
  <th>Finding</th><th>Recommendation</th>
</tr>
</thead>
<tbody>
{rec_rows}
</tbody>
</table>
</div>

<div class="card">
<h2>Raw Summary Data</h2>
<pre>{summary_json}</pre>
</div>

<footer style="text-align:center;color:#999;margin-top:30px;padding:20px;">
  Generated by Noxu Benchmark Suite
</footer>
</body>
</html>"""

    # ------------------------------------------------------------------
    # Adaptation time-series charts
    # ------------------------------------------------------------------

    def generate_adaptation_chart(
        self, series_list: list,
    ) -> Optional[str]:
        """Line chart of target_t1_size, t1_size, t2_size over time.

        Args:
            series_list: list of AdaptationTimeSeries objects
        """
        if not HAS_MATPLOTLIB or not series_list:
            return None

        fig, ax = plt.subplots(figsize=(12, 6))
        colors = {
            "target_t1_size": "#e74c3c",
            "t1_size": "#3498db",
            "t2_size": "#2ecc71",
        }
        line_styles = {
            "target_t1_size": "--",
            "t1_size": "-",
            "t2_size": "-",
        }

        for series in series_list:
            for metric_name in ("target_t1_size", "t1_size", "t2_size"):
                timestamps, values = series.get_metric_series(metric_name)
                if not timestamps:
                    continue
                label = f"{series.pool_name} {metric_name}"
                ax.plot(
                    timestamps, values,
                    label=label,
                    color=colors.get(metric_name, "#999"),
                    linestyle=line_styles.get(metric_name, "-"),
                    linewidth=1.5,
                    alpha=0.8,
                )

        ax.set_xlabel("Time (seconds)")
        ax.set_ylabel("Buffer Count")
        ax.set_title("Cache Adaptation Over Time")
        ax.legend(fontsize=8, loc="upper right")
        ax.grid(True, alpha=0.3)
        fig.tight_layout()
        return self._save_fig(fig, "adaptation_timeseries.png")

    # ------------------------------------------------------------------
    # Eviction breakdown charts
    # ------------------------------------------------------------------

    def generate_eviction_chart(
        self, suite_results: Dict[str, Any],
    ) -> Optional[str]:
        """Stacked bar chart: T1 evictions vs T2 evictions per strategy."""
        if not HAS_MATPLOTLIB:
            return None

        strategy_results = suite_results.get("strategy_results", [])
        if not strategy_results:
            return None

        from collections import defaultdict
        evictions: Dict[str, Dict[str, int]] = defaultdict(
            lambda: {"t1_evictions": 0, "t2_evictions": 0}
        )

        for sr in strategy_results:
            for strategy, swr in sr.items():
                s_name = strategy.value
                for algo_key in ("arc", "car"):
                    algo_delta = swr.pool_stats_delta.get(algo_key, {})
                    if algo_delta:
                        evictions[s_name]["t1_evictions"] += (
                            algo_delta.get("t1_evictions", 0) or 0
                        )
                        evictions[s_name]["t2_evictions"] += (
                            algo_delta.get("t2_evictions", 0) or 0
                        )

        # Filter to strategies with eviction data
        strategies = [s for s in sorted(evictions.keys())
                      if evictions[s]["t1_evictions"] + evictions[s]["t2_evictions"] > 0]

        if not strategies:
            return None

        t1_vals = [evictions[s]["t1_evictions"] for s in strategies]
        t2_vals = [evictions[s]["t2_evictions"] for s in strategies]

        fig, ax = plt.subplots(figsize=(8, 5))
        x = range(len(strategies))
        ax.bar(x, t1_vals, label="T1 evictions", color="#3498db")
        ax.bar(x, t2_vals, bottom=t1_vals, label="T2 evictions", color="#e74c3c")

        ax.set_ylabel("Eviction Count")
        ax.set_title("Eviction Breakdown by Strategy")
        ax.set_xticks(list(x))
        ax.set_xticklabels(strategies, fontsize=9)
        ax.legend()
        fig.tight_layout()
        return self._save_fig(fig, "eviction_breakdown.png")

    # ------------------------------------------------------------------
    # Buffer pool strategy comparison charts and dashboard
    # ------------------------------------------------------------------

    def generate_hit_ratio_chart(
        self, suite_results: Dict[str, Any]
    ) -> Optional[str]:
        """Bar chart of cache hit ratio per strategy."""
        if not HAS_MATPLOTLIB:
            return None

        summary = suite_results.get("summary", {})
        per_strategy = summary.get("per_strategy_timings", {})
        if not per_strategy:
            return None

        strategies = sorted(per_strategy.keys())
        hit_ratios = [
            per_strategy[s].get("avg_hit_ratio", 0) or 0
            for s in strategies
        ]

        fig, ax = plt.subplots(figsize=(8, 5))
        colors = ["#3498db", "#2ecc71", "#e74c3c", "#f39c12", "#9b59b6",
                  "#1abc9c", "#e67e22", "#34495e"][:len(strategies)]
        bars = ax.bar(strategies, hit_ratios, color=colors)
        ax.set_ylabel("Cache Hit Ratio")
        ax.set_title("Cache Hit Ratio by Buffer Strategy")
        ax.set_ylim(0, 1.0)
        ax.yaxis.set_major_formatter(ticker.PercentFormatter(1.0))

        for bar, val in zip(bars, hit_ratios):
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                bar.get_height() + 0.02,
                f"{val:.1%}",
                ha="center", fontsize=10, fontweight="bold",
            )

        fig.tight_layout()
        return self._save_fig(fig, "hit_ratio_comparison.png")

    def generate_strategy_timing_chart(
        self, suite_results: Dict[str, Any]
    ) -> Optional[str]:
        """Grouped bar chart of median query time per strategy per pattern."""
        if not HAS_MATPLOTLIB:
            return None

        strategy_results = suite_results.get("strategy_results", [])
        if not strategy_results:
            return None

        # Collect per-pattern, per-strategy timings
        from collections import defaultdict
        pattern_timings: Dict[str, Dict[str, List[float]]] = defaultdict(
            lambda: defaultdict(list)
        )

        for sr in strategy_results:
            for strategy, swr in sr.items():
                for qr in swr.workload.results:
                    pattern_timings[qr.query_pattern][strategy.value].append(
                        qr.elapsed_seconds * 1000  # convert to ms
                    )

        if not pattern_timings:
            return None

        import statistics

        patterns = sorted(pattern_timings.keys())
        strategies = sorted(
            {s for pt in pattern_timings.values() for s in pt.keys()}
        )

        fig, ax = plt.subplots(figsize=(max(10, len(patterns) * 2), 6))
        width = 0.8 / len(strategies)
        x = list(range(len(patterns)))
        colors = ["#3498db", "#2ecc71", "#e74c3c", "#f39c12", "#9b59b6",
                  "#1abc9c", "#e67e22", "#34495e"][:len(strategies)]

        for i, strategy in enumerate(strategies):
            offsets = [xi - 0.4 + width * (i + 0.5) for xi in x]
            medians = []
            for p in patterns:
                vals = pattern_timings[p].get(strategy, [0])
                medians.append(statistics.median(vals))
            ax.bar(offsets, medians, width, label=strategy, color=colors[i])

        ax.set_ylabel("Median Time (ms)")
        ax.set_title("Query Timing by Strategy and Pattern")
        ax.set_xticks(x)
        ax.set_xticklabels(patterns, rotation=45, ha="right", fontsize=8)
        ax.legend()
        fig.tight_layout()
        return self._save_fig(fig, "strategy_timing.png")

    def generate_strategy_speedup_heatmap(
        self, suite_results: Dict[str, Any]
    ) -> Optional[str]:
        """Heatmap of (ARC time / default clock time) per workload."""
        if not HAS_MATPLOTLIB:
            return None

        strategy_results = suite_results.get("strategy_results", [])
        if not strategy_results:
            return None

        from collections import defaultdict
        import statistics

        from .config import BufferStrategy

        # Collect median timings per strategy per pattern
        pattern_strategy_times: Dict[str, Dict[str, List[float]]] = defaultdict(
            lambda: defaultdict(list)
        )
        for sr in strategy_results:
            for strategy, swr in sr.items():
                for qr in swr.workload.results:
                    pattern_strategy_times[qr.query_pattern][strategy.value].append(
                        qr.elapsed_seconds
                    )

        patterns = sorted(pattern_strategy_times.keys())
        strategies = sorted(
            {s for pt in pattern_strategy_times.values() for s in pt.keys()}
        )
        baseline_key = BufferStrategy.CLOCK.value

        if baseline_key not in strategies or len(strategies) < 2:
            return None

        non_baseline = [s for s in strategies if s != baseline_key]

        data = []
        for s in non_baseline:
            row = []
            for p in patterns:
                base_vals = pattern_strategy_times[p].get(baseline_key, [1])
                s_vals = pattern_strategy_times[p].get(s, [1])
                base_med = statistics.median(base_vals) if base_vals else 1
                s_med = statistics.median(s_vals) if s_vals else 1
                # Speedup: > 1 means this strategy is faster than baseline
                ratio = base_med / s_med if s_med > 0 else 1.0
                row.append(ratio)
            data.append(row)

        fig, ax = plt.subplots(
            figsize=(max(8, len(patterns) * 1.5), max(3, len(non_baseline) * 1.5))
        )
        im = ax.imshow(data, cmap="RdYlGn", aspect="auto", vmin=0.5, vmax=2.0)
        ax.set_xticks(range(len(patterns)))
        ax.set_xticklabels(patterns, rotation=45, ha="right", fontsize=8)
        ax.set_yticks(range(len(non_baseline)))
        ax.set_yticklabels(non_baseline, fontsize=9)
        ax.set_title(f"Speedup vs {baseline_key} (green = faster)")

        for i in range(len(non_baseline)):
            for j in range(len(patterns)):
                ax.text(j, i, f"{data[i][j]:.2f}x", ha="center", va="center", fontsize=8)

        fig.colorbar(im, ax=ax, label=f"Speedup vs {baseline_key}")
        fig.tight_layout()
        return self._save_fig(fig, "strategy_speedup_heatmap.png")

    def generate_strategy_dashboard(
        self, suite_results: Dict[str, Any]
    ) -> str:
        """Generate an HTML dashboard for buffer pool strategy comparison."""
        charts = {}
        if HAS_MATPLOTLIB:
            charts["hit_ratio"] = self.generate_hit_ratio_chart(suite_results)
            charts["timing"] = self.generate_strategy_timing_chart(suite_results)
            charts["heatmap"] = self.generate_strategy_speedup_heatmap(suite_results)
            charts["eviction"] = self.generate_eviction_chart(suite_results)

        summary = suite_results.get("summary", {})
        per_strategy = summary.get("per_strategy_timings", {})

        # Build strategy summary rows
        strategy_rows = ""
        for s_name in sorted(per_strategy.keys()):
            info = per_strategy[s_name]
            avg_t = info.get("avg_time_s", 0)
            med_t = info.get("median_time_s", 0)
            hit_r = info.get("avg_hit_ratio")
            speedup = info.get("speedup_vs_default")
            hr_str = f"{hit_r:.2%}" if hit_r is not None else "N/A"
            sp_str = f"{speedup:.2f}x" if speedup is not None else "-"
            color = "#2ecc71" if speedup and speedup > 1.0 else "#333"
            strategy_rows += f"""
            <tr>
                <td style="font-weight:bold;">{html.escape(s_name)}</td>
                <td>{avg_t:.4f}</td>
                <td>{med_t:.4f}</td>
                <td>{hr_str}</td>
                <td style="color:{color};font-weight:bold;">{sp_str}</td>
            </tr>"""

        # Per-combination detail rows
        detail_rows = ""
        for sr in suite_results.get("strategy_results", []):
            for strategy, swr in sr.items():
                wr = swr.workload
                bp = swr.pool_stats_delta.get("bufferpool", {})
                arc = swr.pool_stats_delta.get("arc", {})
                for qr in wr.results:
                    hr = bp.get("hit_ratio")
                    hr_str = f"{hr:.2%}" if hr is not None else "N/A"
                    detail_rows += f"""
            <tr>
                <td>{html.escape(wr.schema_name)}</td>
                <td>{wr.row_count:,}</td>
                <td>{html.escape(wr.distribution)}</td>
                <td>{html.escape(strategy.value)}</td>
                <td>{html.escape(qr.query_pattern)}</td>
                <td>{qr.elapsed_seconds * 1000:.2f}</td>
                <td>{hr_str}</td>
            </tr>"""

        def img_tag(name: Optional[str]) -> str:
            if name:
                return f'<img src="{html.escape(name)}" style="max-width:100%;margin:10px 0;">'
            return '<p style="color:#999;">Chart not available</p>'

        summary_json = html.escape(json.dumps(summary, indent=2, default=str))

        html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Buffer Pool Strategy Comparison</title>
<style>
  body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
         max-width: 1200px; margin: 0 auto; padding: 20px; background: #f5f5f5; }}
  h1 {{ color: #2c3e50; border-bottom: 3px solid #e74c3c; padding-bottom: 10px; }}
  h2 {{ color: #34495e; margin-top: 30px; }}
  .card {{ background: white; border-radius: 8px; padding: 20px; margin: 15px 0;
           box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
  .summary-grid {{ display: grid; grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
                   gap: 15px; }}
  .metric {{ text-align: center; padding: 15px; background: #ecf0f1; border-radius: 6px; }}
  .metric .value {{ font-size: 2em; font-weight: bold; color: #2c3e50; }}
  .metric .label {{ font-size: 0.85em; color: #7f8c8d; margin-top: 5px; }}
  table {{ border-collapse: collapse; width: 100%; }}
  th, td {{ border: 1px solid #ddd; padding: 8px 12px; text-align: right; }}
  th {{ background: #e74c3c; color: white; }}
  tr:nth-child(even) {{ background: #f9f9f9; }}
  tr:hover {{ background: #eef; }}
  td:first-child {{ text-align: left; }}
  pre {{ background: #2c3e50; color: #ecf0f1; padding: 15px; border-radius: 6px;
         overflow-x: auto; font-size: 0.85em; }}
  .charts {{ display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }}
  @media (max-width: 800px) {{ .charts {{ grid-template-columns: 1fr; }} }}
</style>
</head>
<body>
<h1>Buffer Pool Strategy Comparison</h1>

<div class="card">
<h2>Strategy Summary</h2>
<table>
<thead>
<tr>
  <th style="text-align:left;">Strategy</th><th>Avg Time (s)</th>
  <th>Median Time (s)</th><th>Hit Ratio</th><th>Speedup vs Default</th>
</tr>
</thead>
<tbody>
{strategy_rows}
</tbody>
</table>
</div>

<div class="card">
<h2>Charts</h2>
<div class="charts">
  <div>{img_tag(charts.get("hit_ratio"))}</div>
  <div>{img_tag(charts.get("timing"))}</div>
  <div>{img_tag(charts.get("heatmap"))}</div>
  <div>{img_tag(charts.get("eviction"))}</div>
</div>
</div>

<div class="card">
<h2>Detailed Results</h2>
<table>
<thead>
<tr>
  <th style="text-align:left;">Schema</th><th>Rows</th><th>Distribution</th>
  <th>Strategy</th><th>Pattern</th><th>Time (ms)</th><th>Hit Ratio</th>
</tr>
</thead>
<tbody>
{detail_rows}
</tbody>
</table>
</div>

<div class="card">
<h2>Raw Summary</h2>
<pre>{summary_json}</pre>
</div>

<footer style="text-align:center;color:#999;margin-top:30px;padding:20px;">
  Generated by Buffer Pool Benchmark Suite
</footer>
</body>
</html>"""

        path = os.path.join(self.output_dir, "dashboard.html")
        with open(path, "w") as f:
            f.write(html_content)
        logger.info("Strategy dashboard written to %s", path)
        return path
