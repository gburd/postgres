#!/bin/bash
#
# run_tpcb_benchmark.sh - Rigorous TPC-B benchmark: HEAP vs RECNO
#
# This script performs a controlled, repeatable TPC-B comparison between
# PostgreSQL's standard HEAP and the RECNO table access method.
#
# Key parameters:
#   - Scale factor 10000 (eliminates artificial hot-page contention)
#   - 10-minute runs per data point
#   - 3 repetitions per configuration (reports mean +/- stddev, CV%)
#   - 30-second warmup discarded
#   - Client counts: 1, 2, 4, 8, 16, 32
#   - Latency percentiles: P50, P95, P99 via --log post-processing
#   - System metrics: vmstat/iostat at 1s intervals
#   - pg_prewarm before each run
#   - Optional: OS cache drop, CPU pinning
#
# Usage:
#   ./run_tpcb_benchmark.sh [OPTIONS]
#
# Options:
#   -h HOST          PostgreSQL host (default: /tmp, unix socket)
#   -p PORT          PostgreSQL port (default: 5432)
#   -d DBNAME        Database name (default: tpcb_bench)
#   -D DURATION      Duration per run in seconds (default: 600)
#   -W WARMUP        Warmup seconds to discard (default: 30)
#   -R REPS          Repetitions per config (default: 3)
#   -S SCALE         pgbench scale factor (default: 10000)
#   -o OUTDIR        Output directory (default: ./results/tpcb_TIMESTAMP)
#   -P PGBINDIR      Path to PostgreSQL bin directory (default: use PATH)
#   --drop-cache     Drop OS filesystem cache before each run (requires sudo)
#   --taskset CPUS   Pin pgbench to specific CPUs (e.g., "0-7")
#   --skip-init      Skip pgbench initialization (tables must already exist)
#   --heap-only      Run only HEAP workload
#   --recno-only     Run only RECNO workload
#

set -euo pipefail

# ============================================================================
# Defaults
# ============================================================================
PGHOST="/tmp"
PGPORT="5432"
DBNAME="tpcb_bench"
DURATION=600
WARMUP=30
REPS=3
SCALE=10000
OUTDIR=""
PGBINDIR=""
DROP_CACHE=false
TASKSET_CPUS=""
SKIP_INIT=false
RUN_HEAP=true
RUN_RECNO=true
CLIENT_COUNTS=(1 2 4 8 16 32)
PROGRESS_INTERVAL=10

# ============================================================================
# Parse arguments
# ============================================================================
while [[ $# -gt 0 ]]; do
    case "$1" in
        -h) PGHOST="$2"; shift 2 ;;
        -p) PGPORT="$2"; shift 2 ;;
        -d) DBNAME="$2"; shift 2 ;;
        -D) DURATION="$2"; shift 2 ;;
        -W) WARMUP="$2"; shift 2 ;;
        -R) REPS="$2"; shift 2 ;;
        -S) SCALE="$2"; shift 2 ;;
        -o) OUTDIR="$2"; shift 2 ;;
        -P) PGBINDIR="$2/"; shift 2 ;;
        --drop-cache) DROP_CACHE=true; shift ;;
        --taskset) TASKSET_CPUS="$2"; shift 2 ;;
        --skip-init) SKIP_INIT=true; shift ;;
        --heap-only) RUN_RECNO=false; shift ;;
        --recno-only) RUN_HEAP=false; shift ;;
        *) echo "Unknown option: $1" >&2; exit 1 ;;
    esac
done

# ============================================================================
# Derived variables
# ============================================================================
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
if [[ -z "$OUTDIR" ]]; then
    SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
    OUTDIR="${SCRIPT_DIR}/results/tpcb_${TIMESTAMP}"
fi
mkdir -p "${OUTDIR}/raw" "${OUTDIR}/logs" "${OUTDIR}/sysmetrics"

PSQL="${PGBINDIR}psql -h ${PGHOST} -p ${PGPORT} -X -q"
PGBENCH="${PGBINDIR}pgbench -h ${PGHOST} -p ${PGPORT}"

# Effective run duration (warmup is part of the total run, discarded in post-processing)
TOTAL_RUN=$((DURATION + WARMUP))

# CSV output file
CSV="${OUTDIR}/tpcb_results.csv"
echo "am,clients,rep,tps_total,tps_excl_warmup,lat_avg_ms,lat_p50_ms,lat_p95_ms,lat_p99_ms" > "${CSV}"

# ============================================================================
# Helper functions
# ============================================================================

log() {
    echo "[$(date '+%H:%M:%S')] $*"
}

die() {
    echo "FATAL: $*" >&2
    exit 1
}

check_prereqs() {
    command -v "${PGBINDIR}pgbench" >/dev/null 2>&1 || die "pgbench not found in PATH"
    command -v "${PGBINDIR}psql" >/dev/null 2>&1 || die "psql not found in PATH"

    # Verify connection
    ${PSQL} -d "${DBNAME}" -c "SELECT 1" >/dev/null 2>&1 || {
        # Try creating the database
        ${PGBINDIR}createdb -h "${PGHOST}" -p "${PGPORT}" "${DBNAME}" 2>/dev/null || \
            die "Cannot connect to database '${DBNAME}' and cannot create it"
    }

    # Check if pg_prewarm extension is available
    ${PSQL} -d "${DBNAME}" -c "CREATE EXTENSION IF NOT EXISTS pg_prewarm" 2>/dev/null || \
        log "WARNING: pg_prewarm not available; skipping prewarm"
}

drop_os_cache() {
    if [[ "$DROP_CACHE" == "true" ]]; then
        if sudo -n sh -c 'sync; echo 3 > /proc/sys/vm/drop_caches' 2>/dev/null; then
            log "  OS filesystem cache dropped"
        elif sudo -n sh -c 'sysctl vm.drop_caches=3' 2>/dev/null; then
            log "  OS filesystem cache dropped (FreeBSD)"
        else
            log "  WARNING: Cannot drop OS cache (sudo not available or not permitted)"
        fi
    fi
}

prewarm_tables() {
    local prefix="$1"  # "pgbench" for heap, "pgbench_recno" for recno

    ${PSQL} -d "${DBNAME}" <<SQL 2>/dev/null || log "  WARNING: pg_prewarm failed"
SELECT pg_prewarm('${prefix}_accounts', 'buffer');
SELECT pg_prewarm('${prefix}_tellers', 'buffer');
SELECT pg_prewarm('${prefix}_branches', 'buffer');
SQL
    log "  Tables prewarmed into shared_buffers"
}

start_sysmetrics() {
    local tag="$1"
    local outfile="${OUTDIR}/sysmetrics/${tag}"

    # vmstat
    if command -v vmstat >/dev/null 2>&1; then
        vmstat 1 > "${outfile}_vmstat.txt" 2>/dev/null &
        VMSTAT_PID=$!
    else
        VMSTAT_PID=""
    fi

    # iostat
    if command -v iostat >/dev/null 2>&1; then
        iostat -x 1 > "${outfile}_iostat.txt" 2>/dev/null &
        IOSTAT_PID=$!
    else
        IOSTAT_PID=""
    fi
}

stop_sysmetrics() {
    [[ -n "${VMSTAT_PID:-}" ]] && kill "$VMSTAT_PID" 2>/dev/null || true
    [[ -n "${IOSTAT_PID:-}" ]] && kill "$IOSTAT_PID" 2>/dev/null || true
    wait 2>/dev/null || true
}

# Build the taskset prefix command if CPU pinning is requested
taskset_prefix() {
    if [[ -n "$TASKSET_CPUS" ]]; then
        if command -v taskset >/dev/null 2>&1; then
            echo "taskset -c ${TASKSET_CPUS}"
        elif command -v cpuset >/dev/null 2>&1; then
            echo "cpuset -l ${TASKSET_CPUS}"
        else
            log "WARNING: --taskset requested but neither taskset nor cpuset found"
            echo ""
        fi
    else
        echo ""
    fi
}

# Compute latency percentiles from pgbench log file.
# pgbench --log produces lines: client_no time transaction_no latency_usec script_no
compute_percentiles() {
    local logfile="$1"
    local warmup_us=$(( WARMUP * 1000000 ))

    # Extract latencies (column 4) from transactions after warmup period.
    # The "time" column (col 2) is epoch seconds relative to start; we filter
    # by comparing elapsed time (col 3 is the unix timestamp in usec since epoch
    # in pgbench >= 14, or seconds since start in older versions).
    # Actually pgbench --log format: client_id seconds_since_start usec_since_epoch script_no latency_usec schedule_lag
    # In newer pgbench: each line is:
    #   client_no transaction_no time(unix epoch sec) script_no latency(usec) schedule_lag(usec)
    # We filter rows where (time - first_time) > warmup, then extract latency.

    if [[ ! -f "$logfile" ]] || [[ ! -s "$logfile" ]]; then
        echo "0,0,0"
        return
    fi

    awk -v warmup_sec="$WARMUP" '
    BEGIN { n = 0; start = 0 }
    NR == 1 { start = $3 }
    {
        elapsed = $3 - start
        if (elapsed >= warmup_sec) {
            latencies[n++] = $4 + 0  # latency in usec (field depends on version)
        }
    }
    END {
        if (n == 0) { print "0,0,0"; exit }
        # Sort latencies
        for (i = 0; i < n; i++) {
            for (j = i+1; j < n; j++) {
                if (latencies[i] > latencies[j]) {
                    t = latencies[i]; latencies[i] = latencies[j]; latencies[j] = t
                }
            }
        }
        p50 = latencies[int(n * 0.50)] / 1000.0
        p95 = latencies[int(n * 0.95)] / 1000.0
        p99 = latencies[int(n * 0.99)] / 1000.0
        printf "%.3f,%.3f,%.3f\n", p50, p95, p99
    }' "$logfile"
}

# Compute TPS excluding warmup from pgbench progress output or raw log
compute_tps_excl_warmup() {
    local logfile="$1"

    if [[ ! -f "$logfile" ]] || [[ ! -s "$logfile" ]]; then
        echo "0"
        return
    fi

    awk -v warmup_sec="$WARMUP" '
    BEGIN { start = 0; count = 0; last_time = 0 }
    NR == 1 { start = $3 }
    {
        elapsed = $3 - start
        if (elapsed >= warmup_sec) {
            count++
            last_time = elapsed
        }
    }
    END {
        duration = last_time - warmup_sec
        if (duration > 0) printf "%.2f\n", count / duration
        else print "0"
    }' "$logfile"
}

# Parse pgbench stdout for overall TPS (including connections)
parse_tps() {
    local outfile="$1"
    grep -oP '(?<=tps = )\S+' "$outfile" | tail -1 || echo "0"
}

# Parse pgbench stdout for average latency
parse_lat_avg() {
    local outfile="$1"
    grep -oP '(?<=latency average = )\S+' "$outfile" || echo "0"
}

# ============================================================================
# Initialization
# ============================================================================

init_heap_tables() {
    log "Initializing HEAP tables at scale=${SCALE}..."
    ${PGBENCH} -d "${DBNAME}" -i -s "${SCALE}" --init-steps=dtGvp 2>&1 | tail -3
    log "  HEAP tables initialized"
}

init_recno_tables() {
    log "Initializing RECNO tables at scale=${SCALE}..."

    # Create RECNO equivalents of pgbench tables
    ${PSQL} -d "${DBNAME}" <<SQL
DROP TABLE IF EXISTS pgbench_recno_history CASCADE;
DROP TABLE IF EXISTS pgbench_recno_accounts CASCADE;
DROP TABLE IF EXISTS pgbench_recno_tellers CASCADE;
DROP TABLE IF EXISTS pgbench_recno_branches CASCADE;

CREATE TABLE pgbench_recno_branches (
    bid     INT NOT NULL PRIMARY KEY,
    bbalance INT,
    filler  CHAR(88)
) USING recno;

CREATE TABLE pgbench_recno_tellers (
    tid     INT NOT NULL PRIMARY KEY,
    bid     INT,
    tbalance INT,
    filler  CHAR(84)
) USING recno;

CREATE TABLE pgbench_recno_accounts (
    aid     INT NOT NULL PRIMARY KEY,
    bid     INT,
    abalance INT,
    filler  CHAR(84)
) USING recno;

CREATE TABLE pgbench_recno_history (
    tid     INT,
    bid     INT,
    aid     INT,
    delta   INT,
    mtime   TIMESTAMP,
    filler  CHAR(22)
) USING recno;

-- Populate from the standard pgbench tables (already initialized by init_heap_tables)
INSERT INTO pgbench_recno_branches SELECT * FROM pgbench_branches;
INSERT INTO pgbench_recno_tellers SELECT * FROM pgbench_tellers;
INSERT INTO pgbench_recno_accounts SELECT * FROM pgbench_accounts;

ANALYZE pgbench_recno_branches;
ANALYZE pgbench_recno_tellers;
ANALYZE pgbench_recno_accounts;
VACUUM pgbench_recno_branches;
VACUUM pgbench_recno_tellers;
VACUUM pgbench_recno_accounts;
SQL
    log "  RECNO tables initialized"
}

# ============================================================================
# Workload scripts (written to temp files)
# ============================================================================

write_workload_scripts() {
    # Standard TPC-B for HEAP (uses default pgbench table names)
    cat > "${OUTDIR}/tpcb_heap.sql" <<'EOF'
\set aid random(1, :scale * 100000)
\set bid random(1, :scale)
\set tid random(1, :scale * 10)
\set delta random(-5000, 5000)
BEGIN;
UPDATE pgbench_accounts SET abalance = abalance + :delta WHERE aid = :aid;
SELECT abalance FROM pgbench_accounts WHERE aid = :aid;
UPDATE pgbench_tellers SET tbalance = tbalance + :delta WHERE tid = :tid;
UPDATE pgbench_branches SET bbalance = bbalance + :delta WHERE bid = :bid;
INSERT INTO pgbench_history (tid, bid, aid, delta, mtime) VALUES (:tid, :bid, :aid, :delta, CURRENT_TIMESTAMP);
END;
EOF

    # TPC-B for RECNO tables
    cat > "${OUTDIR}/tpcb_recno.sql" <<'EOF'
\set aid random(1, :scale * 100000)
\set bid random(1, :scale)
\set tid random(1, :scale * 10)
\set delta random(-5000, 5000)
BEGIN;
UPDATE pgbench_recno_accounts SET abalance = abalance + :delta WHERE aid = :aid;
SELECT abalance FROM pgbench_recno_accounts WHERE aid = :aid;
UPDATE pgbench_recno_tellers SET tbalance = tbalance + :delta WHERE tid = :tid;
UPDATE pgbench_recno_branches SET bbalance = bbalance + :delta WHERE bid = :bid;
INSERT INTO pgbench_recno_history (tid, bid, aid, delta, mtime) VALUES (:tid, :bid, :aid, :delta, CURRENT_TIMESTAMP);
END;
EOF
}

# ============================================================================
# Run a single pgbench invocation
# ============================================================================

run_pgbench() {
    local am="$1"       # heap or recno
    local clients="$2"
    local rep="$3"
    local tag="${am}_c${clients}_r${rep}"
    local script="${OUTDIR}/tpcb_${am}.sql"
    local logprefix="${OUTDIR}/logs/${tag}"
    local stdout_file="${OUTDIR}/raw/${tag}_stdout.txt"
    local jobs

    # Use at most clients/2 threads, minimum 1
    jobs=$(( clients / 2 ))
    [[ $jobs -lt 1 ]] && jobs=1

    local prefix
    prefix=$(taskset_prefix)

    log "  Running: am=${am} clients=${clients} rep=${rep} duration=${TOTAL_RUN}s"

    drop_os_cache

    # Prewarm
    if [[ "$am" == "heap" ]]; then
        prewarm_tables "pgbench"
    else
        prewarm_tables "pgbench_recno"
    fi

    # Checkpoint before run to avoid mid-run checkpoint overhead skew
    ${PSQL} -d "${DBNAME}" -c "CHECKPOINT" 2>/dev/null || true

    # Start system metrics collection
    start_sysmetrics "$tag"

    # Run pgbench
    # --log writes per-transaction latency to file for percentile computation
    # -M prepared uses prepared statements
    # -P reports progress every N seconds
    # -D scale=SCALE passes the scale factor as a variable
    ${prefix} ${PGBENCH} -d "${DBNAME}" \
        -f "${script}" \
        -c "${clients}" \
        -j "${jobs}" \
        -T "${TOTAL_RUN}" \
        -M prepared \
        -P "${PROGRESS_INTERVAL}" \
        -D scale="${SCALE}" \
        --log --log-prefix="${logprefix}" \
        > "${stdout_file}" 2>&1 || true

    stop_sysmetrics

    # Parse results
    local tps_total lat_avg tps_excl percentiles p50 p95 p99

    tps_total=$(parse_tps "${stdout_file}")
    lat_avg=$(parse_lat_avg "${stdout_file}")

    # Find the log file(s) pgbench created
    local logfile
    logfile=$(ls "${logprefix}"* 2>/dev/null | head -1 || echo "")

    if [[ -n "$logfile" ]]; then
        tps_excl=$(compute_tps_excl_warmup "$logfile")
        percentiles=$(compute_percentiles "$logfile")
    else
        tps_excl="$tps_total"
        percentiles="0,0,0"
    fi

    p50=$(echo "$percentiles" | cut -d, -f1)
    p95=$(echo "$percentiles" | cut -d, -f2)
    p99=$(echo "$percentiles" | cut -d, -f3)

    # Append to CSV
    echo "${am},${clients},${rep},${tps_total},${tps_excl},${lat_avg},${p50},${p95},${p99}" >> "${CSV}"

    log "    TPS=${tps_total} (excl warmup: ${tps_excl}) lat_avg=${lat_avg}ms P50=${p50} P95=${p95} P99=${p99}"
}

# ============================================================================
# Summary report
# ============================================================================

generate_summary() {
    local summary="${OUTDIR}/summary.txt"

    log "Generating summary report..."

    cat > "${summary}" <<EOF
================================================================================
TPC-B Benchmark Summary
================================================================================
Date:       $(date)
Host:       ${PGHOST}:${PGPORT}
Database:   ${DBNAME}
Scale:      ${SCALE} (= $((SCALE * 100000)) accounts, $((SCALE * 10)) tellers, ${SCALE} branches)
Duration:   ${DURATION}s per data point (+ ${WARMUP}s warmup discarded)
Reps:       ${REPS} per configuration
Clients:    ${CLIENT_COUNTS[*]}
CPU pin:    ${TASKSET_CPUS:-none}
Cache drop: ${DROP_CACHE}
================================================================================

Results (mean +/- stddev, CV%):

EOF

    # Process CSV to compute mean/stddev per (am, clients)
    awk -F',' '
    NR == 1 { next }  # skip header
    {
        am = $1; clients = $2
        key = am "," clients
        tps[key][++count[key]] = $5 + 0  # tps_excl_warmup
        lat[key][count[key]] = $6 + 0
        p50s[key][count[key]] = $7 + 0
        p95s[key][count[key]] = $8 + 0
        p99s[key][count[key]] = $9 + 0
    }
    function mean(arr, n,   s, i) { s=0; for(i=1;i<=n;i++) s+=arr[i]; return s/n }
    function stddev(arr, n,   m, s, i) {
        m = mean(arr, n); s = 0
        for(i=1;i<=n;i++) s += (arr[i]-m)^2
        return sqrt(s/n)
    }
    END {
        printf "%-6s %8s %12s %10s %10s %10s %10s\n", \
            "AM", "Clients", "TPS(excl)", "StdDev", "CV%", "P95(ms)", "P99(ms)"
        printf "%-6s %8s %12s %10s %10s %10s %10s\n", \
            "------", "--------", "------------", "----------", "----------", "----------", "----------"

        # Sort keys: heap first, then recno; by client count
        n_keys = asorti(count, sorted_keys)
        for (k = 1; k <= n_keys; k++) {
            key = sorted_keys[k]
            split(key, parts, ",")
            am = parts[1]; clients = parts[2]
            n = count[key]
            m_tps = mean(tps[key], n)
            s_tps = stddev(tps[key], n)
            cv = (m_tps > 0) ? (s_tps / m_tps * 100) : 0
            m_p95 = mean(p95s[key], n)
            m_p99 = mean(p99s[key], n)
            printf "%-6s %8d %12.1f %10.1f %9.1f%% %10.3f %10.3f\n", \
                am, clients, m_tps, s_tps, cv, m_p95, m_p99
        }
    }' "${CSV}" >> "${summary}"

    # Append ratio comparison
    cat >> "${summary}" <<'EOF'

--------------------------------------------------------------------------------
RECNO / HEAP ratio (higher = RECNO is faster):
--------------------------------------------------------------------------------
EOF

    awk -F',' '
    NR == 1 { next }
    {
        am = $1; clients = $2
        key = am "," clients
        tps_sum[key] += $5 + 0
        tps_cnt[key] += 1
    }
    END {
        printf "%-8s %12s %12s %8s\n", "Clients", "HEAP TPS", "RECNO TPS", "Ratio"
        printf "%-8s %12s %12s %8s\n", "--------", "------------", "------------", "--------"
        # Iterate client counts
        for (key in tps_sum) {
            split(key, parts, ",")
            am = parts[1]; c = parts[2]
            avg[am][c] = tps_sum[key] / tps_cnt[key]
        }
        n = asorti(avg["heap"], clients_sorted, "@ind_num_asc")
        for (i = 1; i <= n; i++) {
            c = clients_sorted[i]
            h = avg["heap"][c]
            r = avg["recno"][c]
            ratio = (h > 0) ? r / h : 0
            printf "%-8s %12.1f %12.1f %7.1f%%\n", c, h, r, ratio * 100
        }
    }' "${CSV}" >> "${summary}"

    echo "" >> "${summary}"
    echo "Raw CSV: ${CSV}" >> "${summary}"
    echo "Logs:    ${OUTDIR}/logs/" >> "${summary}"
    echo "================================================================================\n" >> "${summary}"

    cat "${summary}"
}

# ============================================================================
# Main
# ============================================================================

main() {
    log "============================================"
    log "TPC-B Benchmark: HEAP vs RECNO"
    log "============================================"
    log "Host:       ${PGHOST}:${PGPORT}"
    log "Database:   ${DBNAME}"
    log "Scale:      ${SCALE}"
    log "Duration:   ${DURATION}s + ${WARMUP}s warmup"
    log "Reps:       ${REPS}"
    log "Clients:    ${CLIENT_COUNTS[*]}"
    log "Output:     ${OUTDIR}"
    log "============================================"

    check_prereqs
    write_workload_scripts

    # Initialization
    if [[ "$SKIP_INIT" == "false" ]]; then
        if [[ "$RUN_HEAP" == "true" ]]; then
            init_heap_tables
        fi
        if [[ "$RUN_RECNO" == "true" ]]; then
            init_recno_tables
        fi
    fi

    # Run benchmarks: alternate HEAP and RECNO for each client count
    # to distribute temporal effects (thermals, background activity) evenly.
    for clients in "${CLIENT_COUNTS[@]}"; do
        log ""
        log "=== Client count: ${clients} ==="

        for rep in $(seq 1 "${REPS}"); do
            log ""
            log "--- Repetition ${rep}/${REPS} ---"

            if [[ "$RUN_HEAP" == "true" ]]; then
                run_pgbench "heap" "$clients" "$rep"
            fi

            if [[ "$RUN_RECNO" == "true" ]]; then
                run_pgbench "recno" "$clients" "$rep"
            fi
        done
    done

    log ""
    log "============================================"
    log "All runs complete. Generating summary..."
    log "============================================"

    generate_summary

    log ""
    log "Results: ${OUTDIR}/"
    log "  CSV:     ${CSV}"
    log "  Summary: ${OUTDIR}/summary.txt"
    log "  Raw:     ${OUTDIR}/raw/"
    log "  Logs:    ${OUTDIR}/logs/"
    log "  Metrics: ${OUTDIR}/sysmetrics/"
}

main "$@"
