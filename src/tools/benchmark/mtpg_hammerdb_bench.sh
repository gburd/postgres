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
#   DURABILITY  off (default) = fsync/synchronous_commit/full_page_writes OFF
#               (scheduler-isolation profile); on = all three ON (storage-
#               realistic profile).  The ONLY other diff between lanes stays
#               multithreaded + carriers; DURABILITY is identical across lanes.
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
DURABILITY="${DURABILITY:-off}"
OUT="${OUT:-/mnt/work/hbench}"
case "$DURABILITY" in on|ON|1|true) DUR_FSYNC=on; DUR_SYNC=on; DUR_FPW=on;; *) DUR_FSYNC=off; DUR_SYNC=off; DUR_FPW=off;; esac
PORT="${PORT:-5439}"
export LD_LIBRARY_PATH="$(dirname "$(find "$(dirname "$PGBIN")" -name 'libpq.so.5' 2>/dev/null | head -1)"):${LD_LIBRARY_PATH:-}"
DATA="$OUT/data"
SSHL=(ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=15 -i "$LOADGEN_KEY")
mkdir -p "$OUT"
RES="$OUT/hresults.tsv"
[ -f "$RES" ] || echo -e "bench\tmode\tcarriers\tvu\tnopm\ttpm\tsut_pss_mb\tsut_cpu_pct" > "$RES"
sudo prlimit --pid $$ --stack=67108864:67108864 2>/dev/null || true
sudo prlimit --pid $$ --nofile=1048576:1048576 2>/dev/null || true  # threaded mode multiplexes many sessions onto ONE process fd table; the default per-process nofile (e.g. 65535) is exhausted by high carrier/VU counts, so raise it before launching either lane (identical for both -- fork mode just never approaches the limit).
sudo sysctl -w kernel.perf_event_paranoid=1 kernel.kptr_restrict=0 >/dev/null 2>&1 || true

start_pg() { # $1=mode(process|threaded) $2=carriers-or-empty
  local mode="${1:-process}" carriers="${2:-}"
  # A prior lane's postmaster must be fully gone before we reuse the data dir.
  # A stale postmaster.pid (from a hung/killed stop) makes the next start fail
  # with "lock file already exists" and, if the old server is still alive,
  # cascades every subsequent cell into NA -- the exact 2026-08-04 failure.
  local oldpid; oldpid=$(head -1 "$DATA/postmaster.pid" 2>/dev/null)
  if [ -n "$oldpid" ] && kill -0 "$oldpid" 2>/dev/null; then
    echo "STALE_POSTMASTER $oldpid still alive before start_pg $mode -- killing"
    kill -9 "$oldpid" 2>/dev/null; sleep 3
  fi
  pkill -9 -f "postgres -D $DATA" 2>/dev/null; sleep 1
  rm -f "$DATA/postmaster.pid" 2>/dev/null
  cat > "$DATA/postgresql.conf.mode" <<CONF
port = $PORT
listen_addresses = '*'
shared_buffers = $SHBUF
max_connections = 1024
max_wal_size = 64GB
checkpoint_timeout = 30min
checkpoint_completion_target = 0.9
wal_buffers = 64MB
io_method = sync
huge_pages = on
fsync = $DUR_FSYNC
synchronous_commit = $DUR_SYNC
full_page_writes = $DUR_FPW
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
    # A local select-1 can succeed before the server has finished startup/
    # recovery, during which window TCP connections may transiently fork-fail
    # under multithreaded=on.  The real gate is: the driver can connect over
    # TCP repeatedly and reliably (the pooled carrier path is engaged lazily on
    # connect; this build does not always print a scheduler-up banner, so probe
    # the connection path directly instead of grepping a log string).  Require
    # the readiness log first, then require several consecutive remote-style TCP
    # connects (127.0.0.1 forces the TCP path, like the driver) to succeed.
    for i in $(seq 1 180); do grep -q "ready to accept connections" "$OUT/pg_$mode.log" 2>/dev/null && break; sleep 1; done
    grep -q "ready to accept connections" "$OUT/pg_$mode.log" 2>/dev/null || { echo "THREADED_NOT_READY $mode"; tail -20 "$OUT/pg_$mode.log"; return 1; }
    local streak=0 j
    for j in $(seq 1 120); do
      if "$PGBIN/psql" -h 127.0.0.1 -p $PORT -U postgres -tAc "select 1" postgres >/dev/null 2>&1; then
        streak=$((streak+1)); [ "$streak" -ge 5 ] && break
      else
        streak=0
      fi
      sleep 1
    done
    [ "$streak" -ge 5 ] || { echo "THREADED_TCP_UNSTABLE $mode"; tail -20 "$OUT/pg_$mode.log"; return 1; }
    sleep 2
    # HARD-ASSERT the pooled carrier scheduler is actually in effect.  If
    # pooled_protocol_carriers resolves to 0 (unpooled), multithreaded=on runs
    # THREAD-PER-SESSION, which fork()s a backend per connection -> ENOSYS ->
    # every client fails.  A pre-flight probe passing is NOT enough; assert it
    # on the ACTUAL benchmark server, at start, and abort loudly if wrong.
    local eff_c pool_rows
    eff_c=$("$PGBIN/psql" -h 127.0.0.1 -p $PORT -U postgres -tAc 'show pooled_protocol_carriers' postgres 2>/dev/null | tr -d ' ')
    pool_rows=$("$PGBIN/psql" -h 127.0.0.1 -p $PORT -U postgres -tAc 'select count(*) from pg_stat_xtc_carriers' postgres 2>/dev/null | tr -d ' ')
    if [ -z "$eff_c" ] || [ "$eff_c" = 0 ] || [ -z "$pool_rows" ] || [ "$pool_rows" = 0 ]; then
      echo "POOLED_NOT_IN_EFFECT mode=$mode requested_carriers=$carriers show=$eff_c carrier_rows=$pool_rows -- refusing to run an unpooled 'threaded' lane"
      tail -20 "$OUT/pg_$mode.log"; return 1
    fi
    echo "POOLED_OK carriers=$eff_c carrier_rows=$pool_rows"
  fi
  "$PGBIN/psql" -h 127.0.0.1 -p $PORT -U postgres -tAc "select 1" postgres >/dev/null 2>&1 && {
    # huge_pages=on forces failure if pages are unavailable, so reaching here
    # proves both lanes mapped huge pages.  Record the snapshot per lane.
    grep -E 'HugePages_(Total|Free|Rsvd)' /proc/meminfo > "$OUT/hugepages_${mode}_${carriers:-none}.txt" 2>/dev/null
    grep -qi 'HugePages' "$OUT/pg_$mode.log" && grep -i 'HugePages\|huge page' "$OUT/pg_$mode.log" >> "$OUT/hugepages_${mode}_${carriers:-none}.txt"
    return 0
  }
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
  # If -m fast did not bring the postmaster down (observed under multithreaded=on
  # when the postmaster is spinning -- e.g. fork-ENOSYS), escalate to immediate
  # rather than leaving a live server holding the lock file for the next lane.
  if kill -0 "$pid" 2>/dev/null; then
    echo "FAST_STOP_HUNG pid=$pid -- escalating to immediate"
    "$PGBIN/pg_ctl" -D "$DATA" -m immediate -w -t 120 stop >/dev/null 2>&1
    for i in $(seq 1 30); do kill -0 "$pid" 2>/dev/null || break; sleep 1; done
  fi
  kill -0 "$pid" 2>/dev/null && { kill -9 "$pid" 2>/dev/null; sleep 2; }
  pkill -9 -f "postgres -D $DATA" 2>/dev/null
  sleep 1
  rm -f "$DATA/postmaster.pid" 2>/dev/null
  # Confirm the port is free before the caller starts the next lane.
  for i in $(seq 1 30); do "$PGBIN/psql" -h 127.0.0.1 -p $PORT -U postgres -tAc 'select 1' postgres >/dev/null 2>&1 || break; sleep 1; done
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
