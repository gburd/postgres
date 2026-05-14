#!/usr/bin/env python3
#
# plot_results.py - Plot RECNO vs HEAP benchmark results from CSV files.
#
# Usage:
#   python3 plot_results.py [results_dir]
#
# Reads CSV files produced by the benchmark .pl scripts and generates
# comparison charts as PNG files in the same results directory.
#
# Dependencies: matplotlib, pandas (pip install matplotlib pandas)

import os
import sys
import glob

try:
    import pandas as pd
    import matplotlib
    matplotlib.use('Agg')  # Non-interactive backend
    import matplotlib.pyplot as plt
    import matplotlib.ticker as ticker
except ImportError as e:
    print(f"Missing dependency: {e}")
    print("Install with: pip install matplotlib pandas")
    sys.exit(1)


def find_results_dir():
    """Find the results directory."""
    if len(sys.argv) > 1:
        return sys.argv[1]
    script_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(script_dir, 'results')


def load_csv(results_dir, name):
    """Load a benchmark CSV file, return DataFrame or None."""
    path = os.path.join(results_dir, name)
    if not os.path.exists(path):
        print(f"  Skipping {name}: not found")
        return None
    try:
        df = pd.read_csv(path)
        if df.empty:
            print(f"  Skipping {name}: empty")
            return None
        return df
    except Exception as e:
        print(f"  Error reading {name}: {e}")
        return None


def plot_bulk_insert(results_dir):
    """Plot bulk insert throughput and storage size comparison."""
    df = load_csv(results_dir, 'bulk_insert.csv')
    if df is None:
        return

    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    fig.suptitle('Bulk Insert: RECNO vs HEAP', fontsize=14, fontweight='bold')

    # Throughput comparison
    ax = axes[0]
    throughput = df[df['metric'] == 'throughput'].copy()
    if not throughput.empty:
        throughput['value'] = pd.to_numeric(throughput['value'])
        for am in ['heap', 'recno']:
            subset = throughput[throughput['access_method'] == am]
            if not subset.empty:
                bars = ax.bar(
                    [f"{int(r['rows']):,}" for _, r in subset.iterrows()],
                    subset['value'],
                    label=am.upper(),
                    alpha=0.8,
                    width=0.35,
                    align='edge' if am == 'heap' else 'center',
                )
        ax.set_xlabel('Row Count')
        ax.set_ylabel('Throughput (rows/sec)')
        ax.set_title('Insert Throughput')
        ax.legend()
        ax.yaxis.set_major_formatter(ticker.FuncFormatter(
            lambda x, p: f'{x:,.0f}'))

    # Storage size comparison
    ax = axes[1]
    sizes = df[(df['metric'] == 'table_size') &
               (df['access_method'].isin(['heap', 'recno']))].copy()
    if not sizes.empty:
        sizes['value_mb'] = pd.to_numeric(sizes['value']) / (1024 * 1024)
        row_counts = sorted(sizes['rows'].unique())
        x = range(len(row_counts))
        width = 0.35
        for i, am in enumerate(['heap', 'recno']):
            subset = sizes[sizes['access_method'] == am].sort_values('rows')
            if not subset.empty:
                ax.bar(
                    [xi + (i * width) for xi in x],
                    subset['value_mb'].values,
                    width,
                    label=am.upper(),
                    alpha=0.8,
                )
        ax.set_xlabel('Row Count')
        ax.set_ylabel('Table Size (MB)')
        ax.set_title('Storage Size')
        ax.set_xticks([xi + width / 2 for xi in x])
        ax.set_xticklabels([f'{int(rc):,}' for rc in row_counts])
        ax.legend()

    plt.tight_layout()
    outpath = os.path.join(results_dir, 'bulk_insert.png')
    plt.savefig(outpath, dpi=150)
    plt.close()
    print(f"  Saved: {outpath}")


def plot_update_workload(results_dir):
    """Plot update workload: bloat over time and TPS comparison."""
    df = load_csv(results_dir, 'update_workload.csv')
    if df is None:
        return

    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    fig.suptitle('Update Workload: RECNO vs HEAP', fontsize=14,
                 fontweight='bold')

    # Storage bloat over update rounds
    ax = axes[0]
    for am in ['heap', 'recno']:
        rounds = df[(df['access_method'] == am) &
                     (df['phase'].str.startswith('round_')) &
                     (df['metric'] == 'table_size')].copy()
        if not rounds.empty:
            rounds['round'] = rounds['phase'].str.extract(r'round_(\d+)').astype(int)
            rounds['value_mb'] = pd.to_numeric(rounds['value']) / (1024 * 1024)
            rounds = rounds.sort_values('round')
            ax.plot(rounds['round'], rounds['value_mb'],
                    marker='o', label=am.upper(), linewidth=2)

    # Add baseline
    for am in ['heap', 'recno']:
        baseline = df[(df['access_method'] == am) &
                       (df['phase'] == 'baseline') &
                       (df['metric'] == 'table_size')]
        if not baseline.empty:
            val = pd.to_numeric(baseline['value'].iloc[0]) / (1024 * 1024)
            ax.axhline(y=val, linestyle='--', alpha=0.5,
                       label=f'{am.upper()} baseline')

    ax.set_xlabel('Update Round')
    ax.set_ylabel('Table Size (MB)')
    ax.set_title('Storage Bloat Over Update Rounds')
    ax.legend()

    # TPS comparison
    ax = axes[1]
    tps_data = df[(df['metric'] == 'tps') &
                   (df['access_method'].isin(['heap', 'recno']))].copy()
    if not tps_data.empty:
        tps_data['value'] = pd.to_numeric(tps_data['value'])
        phases = tps_data['phase'].unique()
        x = range(len(phases))
        width = 0.35
        for i, am in enumerate(['heap', 'recno']):
            subset = tps_data[tps_data['access_method'] == am]
            vals = []
            for phase in phases:
                row = subset[subset['phase'] == phase]
                vals.append(row['value'].iloc[0] if not row.empty else 0)
            ax.bar([xi + (i * width) for xi in x], vals, width,
                   label=am.upper(), alpha=0.8)
        ax.set_xlabel('Test Phase')
        ax.set_ylabel('Transactions per Second')
        ax.set_title('Update TPS Comparison')
        ax.set_xticks([xi + width / 2 for xi in x])
        ax.set_xticklabels(phases, rotation=30, ha='right', fontsize=8)
        ax.legend()
        ax.yaxis.set_major_formatter(ticker.FuncFormatter(
            lambda x, p: f'{x:,.0f}'))

    plt.tight_layout()
    outpath = os.path.join(results_dir, 'update_workload.png')
    plt.savefig(outpath, dpi=150)
    plt.close()
    print(f"  Saved: {outpath}")


def plot_sequential_scan(results_dir):
    """Plot sequential scan performance comparison."""
    df = load_csv(results_dir, 'sequential_scan.csv')
    if df is None:
        return

    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    fig.suptitle('Sequential Scan: RECNO vs HEAP', fontsize=14,
                 fontweight='bold')

    # Scan time by test type
    ax = axes[0]
    time_data = df[(df['metric'] == 'avg_time_sec') &
                    (df['access_method'].isin(['heap', 'recno']))].copy()
    if not time_data.empty:
        time_data['value'] = pd.to_numeric(time_data['value']) * 1000  # to ms
        tests = time_data['test'].unique()
        x = range(len(tests))
        width = 0.35
        for i, am in enumerate(['heap', 'recno']):
            subset = time_data[time_data['access_method'] == am]
            vals = []
            for test in tests:
                row = subset[subset['test'] == test]
                vals.append(row['value'].iloc[0] if not row.empty else 0)
            ax.bar([xi + (i * width) for xi in x], vals, width,
                   label=am.upper(), alpha=0.8)
        ax.set_xlabel('Scan Type')
        ax.set_ylabel('Average Time (ms)')
        ax.set_title('Scan Time by Test')
        ax.set_xticks([xi + width / 2 for xi in x])
        ax.set_xticklabels(tests, rotation=45, ha='right', fontsize=7)
        ax.legend()

    # I/O throughput
    ax = axes[1]
    io_data = df[(df['metric'] == 'io_throughput') &
                  (df['access_method'].isin(['heap', 'recno']))].copy()
    if not io_data.empty:
        io_data['value'] = pd.to_numeric(io_data['value'])
        for am in ['heap', 'recno']:
            subset = io_data[io_data['access_method'] == am]
            if not subset.empty:
                ax.bar(am.upper(), subset['value'].iloc[0], alpha=0.8)
        ax.set_ylabel('Throughput (MB/s)')
        ax.set_title('I/O Throughput (COUNT(*))')
    else:
        ax.text(0.5, 0.5, 'No I/O throughput data', ha='center',
                va='center', transform=ax.transAxes)

    plt.tight_layout()
    outpath = os.path.join(results_dir, 'sequential_scan.png')
    plt.savefig(outpath, dpi=150)
    plt.close()
    print(f"  Saved: {outpath}")


def plot_index_scan(results_dir):
    """Plot index scan latency comparison."""
    df = load_csv(results_dir, 'index_scan.csv')
    if df is None:
        return

    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    fig.suptitle('Index Scan: RECNO vs HEAP', fontsize=14,
                 fontweight='bold')

    # Point query latency distribution
    ax = axes[0]
    for am in ['heap', 'recno']:
        pk = df[(df['access_method'] == am) &
                 (df['test'] == 'pk_point') &
                 (df['metric'].isin(['avg_ms', 'p50_ms', 'p95_ms', 'p99_ms']))].copy()
        if not pk.empty:
            pk['value'] = pd.to_numeric(pk['value'])
            percentiles = ['avg_ms', 'p50_ms', 'p95_ms', 'p99_ms']
            vals = []
            for p in percentiles:
                row = pk[pk['metric'] == p]
                vals.append(row['value'].iloc[0] if not row.empty else 0)
            x = range(len(percentiles))
            width = 0.35
            offset = 0 if am == 'heap' else width
            ax.bar([xi + offset for xi in x], vals, width,
                   label=am.upper(), alpha=0.8)
    ax.set_xlabel('Percentile')
    ax.set_ylabel('Latency (ms)')
    ax.set_title('PK Point Query Latency')
    ax.set_xticks([xi + 0.175 for xi in range(4)])
    ax.set_xticklabels(['avg', 'p50', 'p95', 'p99'])
    ax.legend()

    # Range scan scaling
    ax = axes[1]
    range_tests = ['range_10', 'range_100', 'range_1000', 'range_10000']
    for am in ['heap', 'recno']:
        avgs = []
        labels = []
        for test in range_tests:
            row = df[(df['access_method'] == am) &
                      (df['test'] == test) &
                      (df['metric'] == 'avg_ms')]
            if not row.empty:
                avgs.append(pd.to_numeric(row['value'].iloc[0]))
                labels.append(test.replace('range_', ''))
        if avgs:
            ax.plot(labels, avgs, marker='o', label=am.upper(), linewidth=2)
    ax.set_xlabel('Range Width (rows)')
    ax.set_ylabel('Average Latency (ms)')
    ax.set_title('Range Scan Scaling')
    ax.legend()

    plt.tight_layout()
    outpath = os.path.join(results_dir, 'index_scan.png')
    plt.savefig(outpath, dpi=150)
    plt.close()
    print(f"  Saved: {outpath}")


def plot_summary(results_dir):
    """Create a combined summary comparison chart."""
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle('RECNO vs HEAP Performance Summary', fontsize=16,
                 fontweight='bold')

    # Panel 1: Bulk insert throughput
    ax = axes[0][0]
    df = load_csv(results_dir, 'bulk_insert.csv')
    if df is not None:
        throughput = df[df['metric'] == 'throughput'].copy()
        if not throughput.empty:
            throughput['value'] = pd.to_numeric(throughput['value'])
            for am in ['heap', 'recno']:
                subset = throughput[throughput['access_method'] == am]
                if not subset.empty:
                    row_labels = [f"{int(r['rows']):,}" for _, r in subset.iterrows()]
                    ax.barh(row_labels, subset['value'],
                            label=am.upper(), alpha=0.8, height=0.3)
            ax.set_xlabel('Rows/sec')
            ax.set_title('Bulk Insert Throughput')
            ax.legend()

    # Panel 2: Update TPS
    ax = axes[0][1]
    df = load_csv(results_dir, 'update_workload.csv')
    if df is not None:
        tps = df[(df['metric'] == 'tps') &
                  (df['access_method'].isin(['heap', 'recno']))].copy()
        if not tps.empty:
            tps['value'] = pd.to_numeric(tps['value'])
            phases = tps['phase'].unique()
            x = range(len(phases))
            width = 0.35
            for i, am in enumerate(['heap', 'recno']):
                subset = tps[tps['access_method'] == am]
                vals = [subset[subset['phase'] == p]['value'].iloc[0]
                        if not subset[subset['phase'] == p].empty else 0
                        for p in phases]
                ax.bar([xi + i * width for xi in x], vals, width,
                       label=am.upper(), alpha=0.8)
            ax.set_xticks([xi + width / 2 for xi in x])
            ax.set_xticklabels(phases, rotation=30, ha='right', fontsize=7)
            ax.set_ylabel('TPS')
            ax.set_title('Update TPS')
            ax.legend()

    # Panel 3: Sequential scan times
    ax = axes[1][0]
    df = load_csv(results_dir, 'sequential_scan.csv')
    if df is not None:
        times = df[(df['metric'] == 'avg_time_sec') &
                    (df['access_method'].isin(['heap', 'recno']))].copy()
        if not times.empty:
            times['value_ms'] = pd.to_numeric(times['value']) * 1000
            tests = times['test'].unique()[:6]  # Limit to 6 tests
            x = range(len(tests))
            width = 0.35
            for i, am in enumerate(['heap', 'recno']):
                subset = times[times['access_method'] == am]
                vals = [subset[subset['test'] == t]['value_ms'].iloc[0]
                        if not subset[subset['test'] == t].empty else 0
                        for t in tests]
                ax.bar([xi + i * width for xi in x], vals, width,
                       label=am.upper(), alpha=0.8)
            ax.set_xticks([xi + width / 2 for xi in x])
            ax.set_xticklabels(tests, rotation=45, ha='right', fontsize=7)
            ax.set_ylabel('Time (ms)')
            ax.set_title('Sequential Scan Time')
            ax.legend()

    # Panel 4: Index scan latency
    ax = axes[1][1]
    df = load_csv(results_dir, 'index_scan.csv')
    if df is not None:
        pk = df[(df['test'] == 'pk_point') &
                 (df['metric'] == 'avg_ms') &
                 (df['access_method'].isin(['heap', 'recno']))].copy()
        if not pk.empty:
            pk['value'] = pd.to_numeric(pk['value'])
            ax.bar(['HEAP', 'RECNO'],
                   [pk[pk['access_method'] == 'heap']['value'].iloc[0]
                    if not pk[pk['access_method'] == 'heap'].empty else 0,
                    pk[pk['access_method'] == 'recno']['value'].iloc[0]
                    if not pk[pk['access_method'] == 'recno'].empty else 0],
                   alpha=0.8, color=['#1f77b4', '#ff7f0e'])
            ax.set_ylabel('Latency (ms)')
            ax.set_title('PK Point Query Avg Latency')

    plt.tight_layout()
    outpath = os.path.join(results_dir, 'summary.png')
    plt.savefig(outpath, dpi=150)
    plt.close()
    print(f"  Saved: {outpath}")


def main():
    results_dir = find_results_dir()

    if not os.path.isdir(results_dir):
        print(f"Results directory not found: {results_dir}")
        print("Run the benchmark scripts first to generate CSV data.")
        sys.exit(1)

    csvs = glob.glob(os.path.join(results_dir, '*.csv'))
    if not csvs:
        print(f"No CSV files found in {results_dir}")
        sys.exit(1)

    print("=" * 60)
    print("Plotting RECNO vs HEAP Benchmark Results")
    print(f"Results dir: {results_dir}")
    print("=" * 60)

    print("\nPlotting bulk insert results...")
    plot_bulk_insert(results_dir)

    print("\nPlotting update workload results...")
    plot_update_workload(results_dir)

    print("\nPlotting sequential scan results...")
    plot_sequential_scan(results_dir)

    print("\nPlotting index scan results...")
    plot_index_scan(results_dir)

    print("\nPlotting summary chart...")
    plot_summary(results_dir)

    print("\n" + "=" * 60)
    print("All plots generated.")
    print("=" * 60)


if __name__ == '__main__':
    main()
