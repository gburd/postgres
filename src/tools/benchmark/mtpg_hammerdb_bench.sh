#!/usr/bin/env bash
# mtpg_hammerdb_bench.sh -- HammerDB TPROC-C / TPROC-H A/B via an external driver.
#
# Runs ON the SUT.  HammerDB lives on the LOADGEN and drives the SUT over the
# private network (client load never steals SUT CPU).  Builds the TPROC schema
# ONCE, then runs BOTH the process (fork) and threaded (pooled carrier) lanes
# against the SAME data dir -- restarting postgres between lanes with/without
# multithreaded=on -- so the comparison is apples-to-apples on identical data.
#
# TPROC-C = TPC-C-like OLTP (mixed r/w stored procedures): the realistic OLTP
#   workload for the fork-vs-threaded comparison.  Metric: NOPM (New Orders/min).
# TPROC-H = TPC-H-like analytics (22 complex queries): long carrier-occupancy.
#
# Env:
#   PGBIN     server bin dir on the SUT
#   LOADGEN   ssh target of the driver (user@private-ip)
#   LOADGEN_KEY  ssh key on the SUT for the driver
#   SUT_IP    private IP the driver dials
#   HAMMER    HammerDB dir on the driver (default /mnt/work/hammer)
#   BENCH     tproc-c | tproc-h  (default tproc-c)
#   WAREHOUSES  TPROC-C warehouse count / TPROC-H scale (default 100)
#   VU_LIST   virtual-user counts to sweep (default "16 32 64")
#   CARRIERS  threaded carrier settings to sweep (default "auto 32")
#   RAMPUP    minutes rampup (default 2)     DURATION  minutes measured (default 5)
#   SHBUF     shared_buffers (default 8GB)
#   OUT       output dir (default /mnt/work/hbench)
set -uo pipefail
PGBIN="${PGBIN:-/mnt/work/work/pg/inst/usr/local/pgsql/bin}"
LOADGEN="${LOADGEN:?user@driver-private-ip}"
LOADGEN_KEY="${LOADGEN_KEY:-/home/ec2-user/.ssh/xtc-p17.pem}"
SUT_IP="${SUT_IP:?private IP}"
HAMMER="${HAMMER:-/mnt/work/hammer}"
BENCH="${BENCH:-tproc-c}"
WAREHOUSES="${WAREHOUSES:-100}"
VU_LIST="${VU_LIST:-16 32 64}"
CARRIERS="${CARRIERS:-auto 32}"
RAMPUP="${RAMPUP:-2}"
DURATION="${DURATION:-5}"
SHBUF="${SHBUF:-8GB}"
OUT="${OUT:-/mnt/work/hbench}"
PORT="${PORT:-5439}"
export LD_LIBRARY_PATH="$(dirname "$(find "$(dirname "$PGBIN")" -name 'libpq.so.5' 2>/dev/null | head -1)"):${LD_LIBRARY_PATH:-}"
DATA="$OUT/data"
SSHL=(ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=15 -i "$LOADGEN_KEY")
mkdir -p "$OUT"
RES="$OUT/hresults.tsv"
[ -f "$RES" ] || echo -e "bench\tmode\tcarriers\tvu\tnopm\ttpm\tsut_pss_mb\tsut_cpu_pct" > "$RES"
sudo prlimit --pid $$ --stack=67108864:67108864 2>/dev/null || true
sudo sysctl -w kernel.perf_event_paranoid=1 kernel.kptr_restrict=0 >/dev/null 2>&1 || true

start_pg() { # $1=mode(process|threaded) $2=carriers-or-empty
  local mode="${1:-process}" carriers="${2:-}"
  cat > "$DATA/postgresql.conf.mode" <<CONF
port = $PORT
listen_addresses = '*'
shared_buffers = $SHBUF
max_connections = 512
max_wal_size = 32GB
checkpoint_timeout = 30min
checkpoint_completion_target = 0.9
wal_buffers = 64MB
fsync = off
synchronous_commit = off
full_page_writes = off
CONF
  [ "$mode" = threaded ] && { echo "multithreaded = on" >> "$DATA/postgresql.conf.mode"; [ "$carriers" != auto ] && [ -n "$carriers" ] && echo "pooled_protocol_carriers = $carriers" >> "$DATA/postgresql.conf.mode"; }
  # rewrite the include line
  grep -v "include 'postgresql.conf.mode'" "$DATA/postgresql.conf" > "$DATA/postgresql.conf.base" 2>/dev/null || cp "$DATA/postgresql.conf" "$DATA/postgresql.conf.base"
  cp "$DATA/postgresql.conf.base" "$DATA/postgresql.conf"
  echo "include 'postgresql.conf.mode'" >> "$DATA/postgresql.conf"
  "$PGBIN/postgres" -D "$DATA" >"$OUT/pg_$mode.log" 2>&1 &
  echo $! > "$OUT/pg.pid"
  local i; for i in $(seq 1 120); do "$PGBIN/psql" -h 127.0.0.1 -p $PORT -U postgres -tAc "select 1" postgres >/dev/null 2>&1 && break; sleep 1; done
  if [ "$mode" = threaded ]; then
    # A local select-1 can succeed before the pooled carrier scheduler is up
    # (and while the server is still finishing startup/recovery), during which
    # window TCP connections fork-fail under multithreaded=on.  Wait for the
    # carriers to actually be up before letting the driver connect.
    for i in $(seq 1 120); do grep -q "carrier scheduler thread up" "$OUT/pg_$mode.log" 2>/dev/null && break; sleep 1; done
    grep -q "carrier scheduler thread up" "$OUT/pg_$mode.log" 2>/dev/null || { echo "CARRIERS_NOT_UP $mode"; tail -15 "$OUT/pg_$mode.log"; return 1; }
    # settle: no more fork-fails should occur once carriers are up
    sleep 3
  fi
  "$PGBIN/psql" -h 127.0.0.1 -p $PORT -U postgres -tAc "select 1" postgres >/dev/null 2>&1 && return 0
  echo "PG_START_FAIL $mode"; tail -15 "$OUT/pg_$mode.log"; return 1
}
stop_pg() {
  # Drain dirty buffers with an explicit CHECKPOINT first (a heavy write run
  # leaves ~80% of shared_buffers dirty; the implicit shutdown checkpoint can
  # exceed pg_ctl's timeout, causing kill -9 -> crash recovery on the next lane
  # -> a fork-fail doom loop).  Then a clean fast stop with a generous timeout.
  "$PGBIN/psql" -h 127.0.0.1 -p $PORT -U postgres -c "CHECKPOINT" postgres >/dev/null 2>&1
  "$PGBIN/pg_ctl" -D "$DATA" -m fast -w -t 600 stop >/dev/null 2>&1
  local pid; pid=$(cat "$OUT/pg.pid" 2>/dev/null)
  local i; for i in $(seq 1 60); do kill -0 "$pid" 2>/dev/null || break; sleep 1; done
  kill -0 "$pid" 2>/dev/null && { kill -9 "$pid" 2>/dev/null; sleep 2; }
  pkill -9 -f "postgres -D $DATA" 2>/dev/null
  sleep 1
  rm -f "$DATA/postmaster.pid" 2>/dev/null
}

sut_pss_mb() { local t=0 p v; for p in $(pgrep -x postgres 2>/dev/null); do v=$(awk '/^Pss:/{s+=$2} END{print s+0}' /proc/$p/smaps_rollup 2>/dev/null); t=$((t+${v:-0})); done; echo $((t/1024)); }

# --- one-time: initdb + build the TPROC schema (in process mode, once) ---
if [ ! -f "$OUT/.schema_built" ]; then
  find "$DATA" -mindepth 1 -delete 2>/dev/null; rmdir "$DATA" 2>/dev/null
  "$PGBIN/initdb" -D "$DATA" -U postgres -E UTF8 >"$OUT/initdb.log" 2>&1
  echo "host all all 0.0.0.0/0 trust" >> "$DATA/pg_hba.conf"
  start_pg process "" || exit 1
  "$PGBIN/psql" -h 127.0.0.1 -p $PORT -U postgres -c "create role tpcc superuser login password 'tpcc';" postgres >/dev/null 2>&1 || true
  # write the HammerDB build script to the driver
  local_bd="$OUT/build.tcl"
  if [ "$BENCH" = tproc-c ]; then
    cat > "$local_bd" <<TCL
dbset db pg
dbset bm TPROC-C
diset connection pg_host $SUT_IP
diset connection pg_port $PORT
diset tpcc pg_superuser postgres
diset tpcc pg_superuserpass postgres
diset tpcc pg_user tpcc
diset tpcc pg_pass tpcc
diset tpcc pg_dbase tpcc
diset tpcc pg_count_ware $WAREHOUSES
diset tpcc pg_num_vu 8
diset tpcc pg_storedprocs true
buildschema
waittocomplete
quit
TCL
  else
    cat > "$local_bd" <<TCL
dbset db pg
dbset bm TPROC-H
diset connection pg_host $SUT_IP
diset connection pg_port $PORT
diset tpch pg_tpch_superuser postgres
diset tpch pg_tpch_superuserpass postgres
diset tpch pg_tpch_user tpcc
diset tpch pg_tpch_pass tpcc
diset tpch pg_tpch_dbase tpch
diset tpch pg_scale_fact $WAREHOUSES
diset tpch pg_num_tpch_threads 8
buildschema
waittocomplete
quit
TCL
  fi
  "${SSHL[@]}" "$LOADGEN" "mkdir -p /tmp/hdb" 2>/dev/null
  scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i "$LOADGEN_KEY" "$local_bd" "$LOADGEN:/tmp/hdb/build.tcl" >/dev/null 2>&1
  echo "=== building $BENCH schema ($WAREHOUSES) via HammerDB on the driver ..."
  "${SSHL[@]}" "$LOADGEN" "cd $HAMMER && ./hammerdbcli auto /tmp/hdb/build.tcl 2>&1 | tail -5"
  stop_pg
  touch "$OUT/.schema_built"
fi

# --- run one lane: $1=mode $2=carriers $3=vu -> prints NOPM TPM ---
run_lane() {
  local mode="$1" carriers="$2" vu="$3" dbn=tpcc
  [ "$BENCH" = tproc-h ] && dbn=tpch
  local rt="$OUT/run_${mode}_${carriers}_${vu}.tcl"
  if [ "$BENCH" = tproc-c ]; then
    cat > "$rt" <<TCL
dbset db pg
dbset bm TPROC-C
diset connection pg_host $SUT_IP
diset connection pg_port $PORT
diset tpcc pg_superuser postgres
diset tpcc pg_superuserpass postgres
diset tpcc pg_user tpcc
diset tpcc pg_pass tpcc
diset tpcc pg_dbase tpcc
diset tpcc pg_storedprocs true
diset tpcc pg_driver timed
diset tpcc pg_rampup $RAMPUP
diset tpcc pg_duration $DURATION
diset tpcc pg_timeprofile false
vuset logtotemp 1
loadscript
vuset vu $vu
vucreate
vurun
runtimer [expr {($RAMPUP + $DURATION) * 60 + 300}]
vudestroy
quit
TCL
  else
    cat > "$rt" <<TCL
dbset db pg
dbset bm TPROC-H
diset connection pg_host $SUT_IP
diset connection pg_port $PORT
diset tpch pg_tpch_user tpcc
diset tpch pg_tpch_pass tpcc
diset tpch pg_tpch_dbase tpch
diset tpch pg_total_querysets 1
loadscript
vuset vu $vu
vucreate
vurun
runtimer 3600
vudestroy
quit
TCL
  fi
  scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i "$LOADGEN_KEY" "$rt" "$LOADGEN:/tmp/hdb/run.tcl" >/dev/null 2>&1
  "${SSHL[@]}" "$LOADGEN" "cd $HAMMER && ./hammerdbcli auto /tmp/hdb/run.tcl 2>&1" | tee "$OUT/hdbout_${mode}_${carriers}_${vu}.log" \
    | grep -iE 'NOPM|TPM|System achieved|Query Set|completed in'
}

emit() { # mode carriers vu ; parses the last hdb log
  local mode="${1:-?}" carriers="${2:-?}" vu="${3:-?}" log="$OUT/hdbout_${1:-x}_${2:-x}_${3:-x}.log"
  local nopm tpm
  nopm=$(grep -oiE '([0-9]+) NOPM' "$log" 2>/dev/null | grep -oE '[0-9]+' | tail -1)
  tpm=$(grep -oiE '([0-9]+) (PostgreSQL )?TPM' "$log" 2>/dev/null | grep -oE '[0-9]+' | tail -1)
  local pss=$(cat "$OUT/pss.$mode.$carriers.$vu" 2>/dev/null | sort -n | tail -1)
  local cpu=$(cat "$OUT/cpu.$mode.$carriers.$vu" 2>/dev/null | sort -n | tail -1)
  echo -e "$BENCH\t$mode\t$carriers\t$vu\t${nopm:-NA}\t${tpm:-NA}\t${pss:-NA}\t${cpu:-NA}" | tee -a "$RES"
}

for vu in $VU_LIST; do
  # process lane
  start_pg process "" || exit 1
  ( while :; do sut_pss_mb >>"$OUT/pss.process.-.$vu"; sleep 10; done ) & M=$!
  mpstat 10 $(( (RAMPUP+DURATION)*6 )) 2>/dev/null | awk '/all/{print 100-$NF}' >"$OUT/cpu.process.-.$vu" &
  run_lane process "-" "$vu"
  kill $M 2>/dev/null; stop_pg
  emit process "-" "$vu"
  # threaded lanes
  for carriers in $CARRIERS; do
    start_pg threaded "$carriers" || exit 1
    eff=$("$PGBIN/psql" -h 127.0.0.1 -p $PORT -U postgres -tAc 'show pooled_protocol_carriers' postgres 2>/dev/null | tr -d ' ')
    ( while :; do sut_pss_mb >>"$OUT/pss.threaded.$eff.$vu"; sleep 10; done ) & M=$!
    mpstat 10 $(( (RAMPUP+DURATION)*6 )) 2>/dev/null | awk '/all/{print 100-$NF}' >"$OUT/cpu.threaded.$eff.$vu" &
    run_lane threaded "$eff" "$vu"
    kill $M 2>/dev/null; stop_pg
    emit threaded "$eff" "$vu"
  done
done
echo "=== HAMMERDB RESULTS ($RES) ==="
column -t "$RES"
echo "HBENCH_DONE"
