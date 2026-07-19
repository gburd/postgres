#!/usr/bin/env bash
# mtpg_remote_bench.sh -- external-load-driver steady-state A/B benchmark.
#
# Runs ON the SUT (system under test).  Expects a SEPARATE load-driver host
# (LOADGEN) reachable over a low-latency private network, with the SAME PG
# client binaries (for pgbench).  The driver runs pgbench over TCP so the
# client CPU never steals from the server -- the point is clean, apples-to-
# apples, steady-state numbers under constant heavy load.
#
# Compares, per (workload x clients x carriers) cell:
#   - process (multithreaded=off, fork model)
#   - threaded (multithreaded=on, pooled carriers)
# capturing median TPS, p50/p95/p99/p99.9 latency (from pgbench --log on the
# driver), SUT PSS memory, and SUT CPU utilization.
#
# Goal is BEAT stock, not just match: steady state, meaningful duration,
# constant heavy load, external driver, warmup discarded.
#
# Env (override as needed):
#   PGBIN        server+client bin dir on the SUT (has postgres/initdb/psql)
#   LOADGEN      ssh target of the load-driver host (user@ip)
#   LOADGEN_KEY  ssh key for LOADGEN (optional; else default agent/key)
#   SUT_IP       private IP the driver connects to (SUT's private addr)
#   PGBENCH      pgbench path on the DRIVER (default: from PATH)
#   DURATION     measured seconds per cell (default 120 -- steady state)
#   WARMUP       warmup seconds discarded (default 30)
#   SCALE        pgbench scale (default 1000).  At scale 1000 pgbench_branches
#                has 1000 rows, so the tpcb 'UPDATE pgbench_branches' hotspot is
#                not the bottleneck up to a few hundred clients.  Do NOT lower it
#                for high client counts: at small scale (e.g. 100) tpcb becomes
#                pgbench_branches-row-contention-bound and reports a misleading,
#                contention-limited number (identical for both lanes, so the A/B
#                is still fair, but it is not a throughput ceiling).
#   SHBUF        shared_buffers (default 8GB -- keep dataset resident)
#   WORKLOADS    space list: tpcb select update  (default "select update").
#                'select' = read-only prepared (no contention).  'update' =
#                hot-row UPDATE on pgbench_accounts (wide key space -> a CLEAN
#                write throughput test, not branch-contention-bound).  'tpcb' is
#                the standard mix INCLUDING the pgbench_branches hotspot; include
#                it explicitly if you want the mixed number, but prefer 'update'
#                for the write A/B ceiling.
#   CLIENTS      space list (default "16 32 64 128")
#   CARRIERS     space list for threaded (default: auto = "" -> use server default; plus a sweep)
#   OUT          output dir (default /mnt/nvme/work/rbench)
set -uo pipefail

PGBIN="${PGBIN:-/mnt/nvme/work/pg/inst/usr/local/pgsql/bin}"
LOADGEN="${LOADGEN:?set LOADGEN=user@private-ip of the load driver}"
LOADGEN_KEY="${LOADGEN_KEY:-}"
SUT_IP="${SUT_IP:?set SUT_IP=private IP the driver dials}"
PGBENCH_REMOTE="${PGBENCH:-pgbench}"
DURATION="${DURATION:-120}"
WARMUP="${WARMUP:-30}"
SCALE="${SCALE:-1000}"
SHBUF="${SHBUF:-8GB}"
WORKLOADS="${WORKLOADS:-select update}"
CLIENTS="${CLIENTS:-16 32 64 128}"
CARRIERS="${CARRIERS:-auto}"      # "auto" = server default; or a space list to sweep
DURABILITY="${DURABILITY:-off}"  # off = fsync/sync_commit/fpw OFF (scheduler-isolation); on = all ON (storage-realistic)
case "$DURABILITY" in on|ON|1|true) DUR_FSYNC=on; DUR_SYNC=on; DUR_FPW=on;; *) DUR_FSYNC=off; DUR_SYNC=off; DUR_FPW=off;; esac
OUT="${OUT:-/mnt/nvme/work/rbench}"
PORT="${PORT:-5439}"

export LD_LIBRARY_PATH="$(dirname "$(find "$(dirname "$PGBIN")" -name 'libpq.so.5' 2>/dev/null | head -1)"):${LD_LIBRARY_PATH:-}"
SSHL=(ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=15)
[ -n "$LOADGEN_KEY" ] && SSHL+=(-i "$LOADGEN_KEY")

mkdir -p "$OUT"
RES="$OUT/results.tsv"
echo -e "mode\tworkload\tclients\tcarriers\ttps\tp50_ms\tp95_ms\tp99_ms\tp999_ms\tsut_pss_mb\tsut_cpu_pct" > "$RES"

sudo prlimit --pid $$ --stack=67108864:67108864 2>/dev/null || true
sudo sysctl -w kernel.perf_event_paranoid=1 kernel.kptr_restrict=0 >/dev/null 2>&1 || true

pctl() { # $1=logfile-glob  -> prints "p50 p95 p99 p999" in ms from pgbench --log (field 3 = latency us)
  awk '{print $3}' $1 2>/dev/null | sort -n | awk '
    function q(p,  i){ i=int(p*NR); if(i<1)i=1; if(i>NR)i=NR; return v[i]/1000.0 }
    { v[NR]=$1 }
    END {
      if (NR==0) { print "NA NA NA NA"; exit }
      printf "%.4f %.4f %.4f %.4f\n", q(0.50), q(0.95), q(0.99), q(0.999)
    }'
}

sut_pss_mb() { # sum PSS across all postgres procs/threads (one proc in threaded, many in process)
  local total=0 p
  for p in $(pgrep -x postgres 2>/dev/null); do
    local v; v=$(awk '/^Pss:/{s+=$2} END{print s+0}' /proc/$p/smaps_rollup 2>/dev/null)
    total=$((total + ${v:-0}))
  done
  echo $((total / 1024))
}

start_server() { # $1=mode(process|threaded) $2=carriers-or-empty
  local mode="${1:-process}" carriers="${2:-}" D="$OUT/data_${1:-process}"
  find "$D" -mindepth 1 -delete 2>/dev/null; rmdir "$D" 2>/dev/null
  "$PGBIN/initdb" -D "$D" -U postgres -E UTF8 >"$OUT/initdb_$mode.log" 2>&1 || { echo "INITDB_FAIL $mode"; return 1; }
  cat >> "$D/postgresql.conf" <<CONF
port = $PORT
listen_addresses = '*'
shared_buffers = $SHBUF
max_connections = 400
max_wal_size = 16GB
checkpoint_timeout = 30min
io_method = sync
fsync = $DUR_FSYNC
synchronous_commit = $DUR_SYNC
full_page_writes = $DUR_FPW
CONF
  # trust the driver's private subnet
  echo "host all all 0.0.0.0/0 trust" >> "$D/pg_hba.conf"
  if [ "$mode" = threaded ]; then
    echo "multithreaded = on" >> "$D/postgresql.conf"
    [ "$carriers" != "auto" ] && [ -n "$carriers" ] && echo "pooled_protocol_carriers = $carriers" >> "$D/postgresql.conf"
  fi
  "$PGBIN/postgres" -D "$D" >"$OUT/pg_$mode.log" 2>&1 &
  echo $! > "$OUT/pg_$mode.pid"
  local i
  for i in $(seq 1 40); do "$PGBIN/psql" -h 127.0.0.1 -p $PORT -U postgres -tAc "select 1" postgres >/dev/null 2>&1 && return 0; sleep 1; done
  echo "START_FAIL $mode"; tail -15 "$OUT/pg_$mode.log"; return 1
}
stop_server() { local m="$1"; kill "$(cat "$OUT/pg_$m.pid" 2>/dev/null)" 2>/dev/null; sleep 3; kill -9 "$(cat "$OUT/pg_$m.pid" 2>/dev/null)" 2>/dev/null; }

drive() { # $1=clients $2=script-args...  -> prints one line: "tps p50 p95 p99 p999" (latencies ms)
  local c="$1"; shift
  local rlog="/tmp/rbench_drv"
  # Run pgbench ON THE DRIVER; compute percentiles ON THE DRIVER (never cat the
  # multi-million-row per-txn log back over ssh -- that OOMs/stalls at high tps).
  # Warmup (discarded), then measured run with --log; awk the log locally into
  # p50/p95/p99/p999 and echo only tps + the four percentiles.
  "${SSHL[@]}" "$LOADGEN" "
    $PGBENCH_REMOTE -h $SUT_IP -p $PORT -U postgres -n -T $WARMUP -c $c -j $c $* postgres >/dev/null 2>&1
    rm -f ${rlog}.* 2>/dev/null
    out=\$($PGBENCH_REMOTE -h $SUT_IP -p $PORT -U postgres -n -T $DURATION -c $c -j $c --log --log-prefix=${rlog} $* postgres 2>&1)
    tps=\$(echo \"\$out\" | grep -iE 'tps' | grep -oE '[0-9]+\.[0-9]+' | head -1)
    read p50 p95 p99 p999 < <(sort -n -k3,3 ${rlog}.* 2>/dev/null | awk 'function q(p,  i){i=int(p*NR);if(i<1)i=1;if(i>NR)i=NR;return v[i]/1000.0} {v[NR]=\$3} END{if(NR==0){print \"NA NA NA NA\";exit} printf \"%.4f %.4f %.4f %.4f\n\",q(0.50),q(0.95),q(0.99),q(0.999)}')
    rm -f ${rlog}.* 2>/dev/null
    echo \"\${tps:-NA} \$p50 \$p95 \$p99 \$p999\"
  "
}

pgbench_args() { case "$1" in
  tpcb)   echo "--builtin tpcb-like" ;;
  select) echo "--builtin select-only -M prepared" ;;
  update) echo "-f /tmp/hotrow.sql" ;;   # driver must have this; we scp it
esac }

# ship a hot-row update script to the driver (for the 'update' workload)
"${SSHL[@]}" "$LOADGEN" "printf 'UPDATE pgbench_accounts SET abalance=abalance+1 WHERE aid=(random()*100000*%d)::int+1;\n' $SCALE > /tmp/hotrow.sql" 2>/dev/null || true

carrier_list() { local w="${1:-}"; if [ "$w" = process ]; then echo "-"; else echo $CARRIERS; fi; }

for workload in $WORKLOADS; do
  args="$(pgbench_args "$workload")"
  # --- process lane (baseline) ---
  start_server process "" || exit 1
  "$PGBIN/pgbench" -h 127.0.0.1 -p $PORT -U postgres -i -s $SCALE postgres >"$OUT/init_${workload}_proc.log" 2>&1
  for c in $CLIENTS; do
    ( while :; do sut_pss_mb >>"$OUT/pss_p.$c" ; sleep 5; done ) & PSSMON=$!
    mpstat 1 $((DURATION)) 2>/dev/null | awk '/all/{print 100-$NF}' >"$OUT/cpu_p.$c" &
    read tps p50 p95 p99 p999 <<<"$(drive "$c" $args)"
    kill $PSSMON 2>/dev/null
    pss=$(sort -n "$OUT/pss_p.$c" 2>/dev/null | tail -1); cpu=$(sort -n "$OUT/cpu_p.$c" 2>/dev/null | tail -1)
    echo -e "process\t$workload\t$c\t-\t${tps:-NA}\t${p50:-NA}\t${p95:-NA}\t${p99:-NA}\t${p999:-NA}\t${pss:-NA}\t${cpu:-NA}" | tee -a "$RES"
  done
  stop_server process
  # --- threaded lanes (carrier sweep) ---
  for carriers in $CARRIERS; do
    start_server threaded "$carriers" || exit 1
    "$PGBIN/pgbench" -h 127.0.0.1 -p $PORT -U postgres -i -s $SCALE postgres >"$OUT/init_${workload}_thr.log" 2>&1
    eff=$("$PGBIN/psql" -h 127.0.0.1 -p $PORT -U postgres -tAc 'show pooled_protocol_carriers' postgres 2>/dev/null | tr -d ' ')
    for c in $CLIENTS; do
      ( while :; do sut_pss_mb >>"$OUT/pss_t.$carriers.$c" ; sleep 5; done ) & PSSMON=$!
      mpstat 1 $((DURATION)) 2>/dev/null | awk '/all/{print 100-$NF}' >"$OUT/cpu_t.$carriers.$c" &
      read tps p50 p95 p99 p999 <<<"$(drive "$c" $args)"
      kill $PSSMON 2>/dev/null
      pss=$(sort -n "$OUT/pss_t.$carriers.$c" 2>/dev/null | tail -1); cpu=$(sort -n "$OUT/cpu_t.$carriers.$c" 2>/dev/null | tail -1)
      echo -e "threaded(c=$eff)\t$workload\t$c\t$eff\t${tps:-NA}\t${p50:-NA}\t${p95:-NA}\t${p99:-NA}\t${p999:-NA}\t${pss:-NA}\t${cpu:-NA}" | tee -a "$RES"
    done
    stop_server threaded
  done
done

echo "=== RESULTS ($RES) ==="
column -t "$RES"
echo "RBENCH_DONE"
