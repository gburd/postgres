#!/usr/bin/env bash
#
# overnight_bench.sh - Overnight heap vs recno benchmark runner
#
# Runs the benchmark suite multiple times with different configurations,
# handles crashes gracefully, and produces a comprehensive results file.
#
set -u

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="/scratch/recno"
PG_BIN="${REPO_DIR}/build/tmp_install/scratch/recno/install/bin"
PG_LIB="${REPO_DIR}/build/tmp_install/scratch/recno/install/lib"
RESULTS_DIR="/scratch/recno/benchmark_results"
LOGFILE="${RESULTS_DIR}/overnight_$(date +%Y%m%d_%H%M%S).log"

export LD_LIBRARY_PATH="${PG_LIB}"
export DYLD_LIBRARY_PATH="${PG_LIB}"
export PATH="${PG_BIN}:${PATH}"

mkdir -p "${RESULTS_DIR}"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "${LOGFILE}"
}

run_benchmark() {
    local scale="$1"
    local duration="$2"
    local max_clients="$3"
    local run_label="$4"

    log "=========================================="
    log "RUN: ${run_label}"
    log "  Scale=${scale}, Duration=${duration}s, MaxClients=${max_clients}"
    log "=========================================="

    local pgdata_base="/tmp/pgbench_overnight_$$"
    local result_file="${RESULTS_DIR}/result_${run_label}_$(date +%Y%m%d_%H%M%S).txt"

    for am in heap recno; do
        local pgdata="${pgdata_base}/${am}"
        local port
        if [ "$am" = "heap" ]; then port=54320; else port=54321; fi

        log "  --- ${am} AM (port ${port}) ---"

        # Clean any leftover state
        "${PG_BIN}/pg_ctl" -D "${pgdata}" stop -m immediate 2>/dev/null || true
        rm -rf "${pgdata}"

        # Init
        log "  initdb..."
        "${PG_BIN}/initdb" -D "${pgdata}" --no-locale -E UTF8 >"${RESULTS_DIR}/initdb_${am}.log" 2>&1
        if [ $? -ne 0 ]; then
            log "  ERROR: initdb failed for ${am}"
            cat "${RESULTS_DIR}/initdb_${am}.log" >> "${LOGFILE}"
            continue
        fi

        # Configure
        cat >> "${pgdata}/postgresql.conf" <<PGCONF
default_table_access_method = '${am}'
port = ${port}
unix_socket_directories = '/tmp'
shared_buffers = '256MB'
work_mem = '16MB'
maintenance_work_mem = '128MB'
max_connections = 100
fsync = off
synchronous_commit = off
wal_level = minimal
max_wal_senders = 0
checkpoint_timeout = '30min'
max_wal_size = '4GB'
log_min_messages = warning
PGCONF

        # For recno runs, increase logging to capture sLog/UNDO diagnostics
        if [ "$am" = "recno" ]; then
            cat >> "${pgdata}/postgresql.conf" <<RECNO_CONF
log_min_messages = log
RECNO_CONF
        fi

        # Start
        log "  Starting server..."
        "${PG_BIN}/pg_ctl" -D "${pgdata}" -l "${pgdata}/pg.log" start -w -t 30
        if [ $? -ne 0 ]; then
            log "  ERROR: server start failed for ${am}"
            tail -20 "${pgdata}/pg.log" >> "${LOGFILE}"
            continue
        fi

        # Init pgbench
        log "  pgbench init (scale=${scale})..."
        "${PG_BIN}/pgbench" -h /tmp -p "${port}" -i -s "${scale}" postgres >"${RESULTS_DIR}/pgbench_init_${am}.log" 2>&1
        if [ $? -ne 0 ]; then
            log "  ERROR: pgbench init failed for ${am}"
            cat "${RESULTS_DIR}/pgbench_init_${am}.log" >> "${LOGFILE}"
            "${PG_BIN}/pg_ctl" -D "${pgdata}" stop -m immediate 2>/dev/null
            continue
        fi

        # Run benchmarks at various client counts
        local clients=1
        while [ "${clients}" -le "${max_clients}" ]; do
            log "  pgbench: ${am}, ${clients} clients, ${duration}s..."

            local bench_out
            bench_out=$("${PG_BIN}/pgbench" -h /tmp -p "${port}" \
                -c "${clients}" -j "${clients}" -T "${duration}" postgres 2>&1)
            local bench_exit=$?

            if [ ${bench_exit} -eq 0 ]; then
                local tps
                tps=$(echo "${bench_out}" | grep "tps.*without" | awk '{print $3}')
                local lat
                lat=$(echo "${bench_out}" | grep "latency average" | awk '{print $4}')
                log "  RESULT: ${am} c=${clients} tps=${tps} lat=${lat}ms"
                echo "${run_label},${am},${clients},${tps},${lat}" >> "${result_file}"
            else
                log "  CRASH/ERROR: ${am} c=${clients} exit=${bench_exit}"
                echo "${run_label},${am},${clients},FAIL,FAIL" >> "${result_file}"

                # Check if server is still alive
                "${PG_BIN}/pg_ctl" -D "${pgdata}" status >/dev/null 2>&1
                if [ $? -ne 0 ]; then
                    log "  Server crashed at ${clients} clients. Restarting..."
                    tail -5 "${pgdata}/pg.log" >> "${LOGFILE}"

                    # Restart for remaining tests
                    "${PG_BIN}/pg_ctl" -D "${pgdata}" -l "${pgdata}/pg.log" start -w -t 30 2>/dev/null
                    if [ $? -ne 0 ]; then
                        log "  Server failed to restart. Skipping remaining client counts."
                        break
                    fi
                    # Reinitialize pgbench after crash recovery
                    "${PG_BIN}/pgbench" -h /tmp -p "${port}" -i -s "${scale}" postgres >/dev/null 2>&1
                fi
            fi

            clients=$((clients * 2))
        done

        # Preserve per-run pg.log before stopping server
        if [ -f "${pgdata}/pg.log" ]; then
            cp "${pgdata}/pg.log" "${RESULTS_DIR}/pglog_${am}_${run_label}.log"
            log "  Preserved pg.log -> pglog_${am}_${run_label}.log"
        fi

        # Stop server
        "${PG_BIN}/pg_ctl" -D "${pgdata}" stop -m fast 2>/dev/null
        rm -rf "${pgdata}"
        log "  ${am} AM complete."
    done

    # Print summary table from result file
    if [ -f "${result_file}" ]; then
        log ""
        log "  ┌─────────────────────────────────────────────────────────────┐"
        log "  │  ${run_label} Results (Scale=${scale}, Duration=${duration}s)"
        log "  ├─────────┬────────────┬─────────────┬────────────┬──────────┤"
        log "  │ Clients │  Heap TPS  │  Recno TPS  │ Heap Lat   │ Recno Lat│"
        log "  ├─────────┼────────────┼─────────────┼────────────┼──────────┤"

        local clients=1
        while [ "${clients}" -le "${max_clients}" ]; do
            local htps hlat rtps rlat
            htps=$(grep "^${run_label},heap,${clients}," "${result_file}" | tail -1 | cut -d, -f4)
            hlat=$(grep "^${run_label},heap,${clients}," "${result_file}" | tail -1 | cut -d, -f5)
            rtps=$(grep "^${run_label},recno,${clients}," "${result_file}" | tail -1 | cut -d, -f4)
            rlat=$(grep "^${run_label},recno,${clients}," "${result_file}" | tail -1 | cut -d, -f5)

            htps=${htps:-N/A}; hlat=${hlat:-N/A}; rtps=${rtps:-N/A}; rlat=${rlat:-N/A}
            printf "  │ %7d │ %10s │ %11s │ %10s │ %8s │\n" \
                "${clients}" "${htps}" "${rtps}" "${hlat}" "${rlat}" | tee -a "${LOGFILE}"

            clients=$((clients * 2))
        done
        log "  └─────────┴────────────┴─────────────┴────────────┴──────────┘"
        log ""
    fi
}

# ============================================================
# Main overnight benchmark plan
# ============================================================

log "============================================"
log "OVERNIGHT BENCHMARK SUITE"
log "Host: $(hostname)"
log "Date: $(date)"
log "PG_BIN: ${PG_BIN}"
log "Results: ${RESULTS_DIR}"
log "============================================"

# Verify binaries work
"${PG_BIN}/postgres" --version | tee -a "${LOGFILE}"

# Run 1: Small scale, quick warmup
run_benchmark 1 30 4 "warmup_s1"

# Run 2: Scale 10, 60-second runs, up to 4 clients (safe range)
run_benchmark 10 60 4 "scale10_60s"

# Run 3: Scale 10, 120-second runs for more stable numbers
run_benchmark 10 120 4 "scale10_120s"

# Run 4: Scale 50, 60-second runs
run_benchmark 50 60 4 "scale50_60s"

# Run 5: Scale 100, 60-second runs
run_benchmark 100 60 4 "scale100_60s"

# Run 6: Try higher client counts (may crash for recno)
run_benchmark 10 60 16 "scale10_highclients"

# Run 7: Long run at scale 10 with 2 clients (stress test)
run_benchmark 10 300 2 "scale10_stress_2c"

# Run 8: Long run at scale 10 with 4 clients (stress test)
run_benchmark 10 300 4 "scale10_stress_4c"

log "============================================"
log "OVERNIGHT BENCHMARKS COMPLETE"
log "Date: $(date)"
log "Results in: ${RESULTS_DIR}"
log "============================================"

# Final summary: aggregate all CSV results
log ""
log "=== ALL RESULTS ==="
for f in "${RESULTS_DIR}"/result_*.txt; do
    [ -f "$f" ] && cat "$f" | tee -a "${LOGFILE}"
done
