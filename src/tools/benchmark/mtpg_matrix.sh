#!/usr/bin/env bash
# mtpg_matrix.sh -- apples-to-apples fork-vs-XTC benchmark matrix (P1/P3 of the
# fork->XTC perf plan).  Runs ON the SUT; HammerDB lives on a SEPARATE loadgen.
#
# Fixes the two measurement fragilities that bit us:
#  1. HammerDB's monitor VU hangs/misparses on the pooled server -> we sample NOPM
#     SERVER-SIDE from a PRE-WARMED persistent psql (sum(d_next_o_id) from district
#     delta = the exact NOPM quantity), monitor-independent; HammerDB's own NOPM is
#     also captured when present and cross-checked.
#  2. Stall detection: the sampler records d_next_o_id every 10s across the window; a
#     frozen counter (a scheduler stall) is flagged, not silently averaged to 0.
#
# Both lanes get IDENTICAL config: shared_buffers=85% RAM, autovacuum on, fsync on,
# synchronous_commit on, full_page_writes on, huge_pages on, io_method=sync, PGDATA on
# local NVMe (xfs).  fork = multithreaded=off; xtc = multithreaded=on,
# pooled_protocol_carriers=-1 (auto = one carrier per core).
#
# Env:
#   PGBIN     server bin dir      LOADGEN  user@loadgen-private-ip
#   LOADGEN_KEY  ssh key on SUT for the loadgen    SUT_IP  private IP the driver dials
#   HAMMER    HammerDB dir on the loadgen (default /mnt/work/hammer)
#   DATA      PGDATA on NVMe (default /mnt/work/data)
#   RAM_PCT   shared_buffers as % of host RAM (default 85)
#   WAREHOUSES / SCALE  TPROC-C warehouses / TPROC-H scale (default 200 / 10)
#   VU_LIST   virtual-user sweep (default "16 32 64 128")
#   RAMPUP DURATION  minutes (default 2 / 5)
#   BENCH     tproc-c | tproc-h | both (default both)
#   LANES     "fork xtc" (default both)
#   OUT       output dir (default /mnt/work/matrix)
set -uo pipefail

PGBIN="${PGBIN:?}"; LOADGEN="${LOADGEN:?}"; LOADGEN_KEY="${LOADGEN_KEY:-/home/ec2-user/.ssh/hdb.pem}"
SUT_IP="${SUT_IP:?}"; HAMMER="${HAMMER:-/mnt/work/hammer}"; DATA="${DATA:-/mnt/work/data}"
RAM_PCT="${RAM_PCT:-85}"; WAREHOUSES="${WAREHOUSES:-200}"; SCALE="${SCALE:-10}"
VU_LIST="${VU_LIST:-16 32 64 128}"; RAMPUP="${RAMPUP:-2}"; DURATION="${DURATION:-5}"
BENCH="${BENCH:-both}"; LANES="${LANES:-fork xtc}"; OUT="${OUT:-/mnt/work/matrix}"
PORT="${PORT:-5439}"
export LD_LIBRARY_PATH="$(dirname "$PGBIN")/lib64:$(dirname "$PGBIN")/lib:/usr/local/lib64:/usr/local/lib:${LD_LIBRARY_PATH:-}"
mkdir -p "$OUT"; RES="$OUT/results.tsv"
[ -f "$RES" ] || echo -e "bench\tlane\tvu\tcarriers\tsrv_nopm\tsrv_tpm\thdb_nopm\tp95_ms\tp99_ms\trss_mb\tcpu_pct\tcsw_s\tstall" > "$RES"
say(){ echo "[$(date -u +%H:%M:%S)] $*" | tee -a "$OUT/LOG"; }
SSHL(){ ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=15 -i "$LOADGEN_KEY" "$LOADGEN" "$@"; }
SCPL(){ scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i "$LOADGEN_KEY" "$1" "$LOADGEN:$2" >/dev/null 2>&1; }
Q(){ timeout 20 "$PGBIN/psql" -h 127.0.0.1 -p $PORT -U postgres -tAc "$1" "${2:-postgres}" 2>/dev/null | tr -d ' '; }

RAM_KB=$(awk '/MemTotal/{print $2}' /proc/meminfo); SB_MB=$(( RAM_KB/1024*RAM_PCT/100 ))
NCORE=$(nproc)
sudo prlimit --pid $$ --nofile=1048576:1048576 2>/dev/null || true

hardstop(){ for p in $(pgrep postgres); do sudo kill -9 $p 2>/dev/null; done; sleep 3; rm -f "$DATA/postmaster.pid"; }
start_lane(){ # $1=fork|xtc
  local lane="$1" extra=""
  hardstop
  [ "$lane" = xtc ] && extra="-c multithreaded=on -c pooled_protocol_carriers=-1"
  "$PGBIN/postgres" -D "$DATA" -c port=$PORT -c listen_addresses='*' \
    -c shared_buffers=${SB_MB}MB -c max_connections=1024 -c max_wal_size=64GB \
    -c checkpoint_timeout=30min -c checkpoint_completion_target=0.9 -c wal_buffers=256MB \
    -c effective_cache_size=$((RAM_KB/1024))MB -c work_mem=64MB -c maintenance_work_mem=2GB \
    -c io_method=sync -c huge_pages=on -c fsync=on -c synchronous_commit=on -c full_page_writes=on \
    -c autovacuum=on $extra >"$OUT/pg_${lane}.log" 2>&1 &
  local i; for i in $(seq 1 120); do grep -q "ready to accept" "$OUT/pg_${lane}.log" && break; sleep 1; done
  local mt=$(Q 'show multithreaded'); local car=$(Q 'show pooled_protocol_carriers')
  say "lane=$lane up: mt=$mt carriers=$car shared_buffers=$(Q 'show shared_buffers') (host RAM $((RAM_KB/1024/1024))GB, ${RAM_PCT}%=${SB_MB}MB, cores=$NCORE)"
  # ASSERT: xtc lane must be multithreaded=on and NOT thread-per-session
  if [ "$lane" = xtc ]; then
    [ "$mt" = on ] || { say "ASSERT FAIL: xtc lane mt=$mt"; return 1; }
    [ "$car" = "-1" ] || { say "ASSERT FAIL: xtc carriers=$car (want -1 auto)"; return 1; }
  fi
  return 0
}
stop_lane(){ "$PGBIN/psql" -h 127.0.0.1 -p $PORT -U postgres -c CHECKPOINT postgres >/dev/null 2>&1
  "$PGBIN/pg_ctl" -D "$DATA" -m fast -w -t 300 stop >/dev/null 2>&1; hardstop; }

# --- one-time schema builds (in fork mode, once each) ---
build_tpcc(){ [ -f "$OUT/.tpcc" ] && return
  find "$DATA" -mindepth 1 -delete 2>/dev/null; rmdir "$DATA" 2>/dev/null
  "$PGBIN/initdb" -D "$DATA" -U postgres -E UTF8 >/dev/null 2>&1
  echo "host all all 0.0.0.0/0 trust" >> "$DATA/pg_hba.conf"
  start_lane fork
  "$PGBIN/psql" -h 127.0.0.1 -p $PORT -U postgres -c "create role tpcc superuser login password 'tpcc'" postgres >/dev/null 2>&1
  cat > /tmp/bc.tcl <<TCL
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
diset tpcc pg_num_vu 16
diset tpcc pg_storedprocs true
buildschema
waittocomplete
quit
TCL
  SCPL /tmp/bc.tcl /tmp/bc.tcl
  say "building TPROC-C $WAREHOUSES wh ..."; SSHL "cd $HAMMER && ./hammerdbcli auto /tmp/bc.tcl" >/dev/null 2>&1
  stop_lane; touch "$OUT/.tpcc"
}

# --- run one TPROC-C lane at VU: server-side NOPM + stall detect + latency + RSS/CPU/csw ---
run_tpcc(){ # $1=lane $2=vu
  local lane="$1" vu="$2"
  start_lane "$lane" || { echo -e "tproc-c\t$lane\t$vu\tSTART_FAIL" >> "$RES"; return; }
  local car=$(Q 'show pooled_protocol_carriers')
  cat > /tmp/rc.tcl <<TCL
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
diset tpcc pg_timeprofile true
diset tpcc pg_allwarehouse true
vuset logtotemp 1
loadscript
vuset vu $vu
vucreate
vurun
runtimer $(( (RAMPUP+DURATION+4)*60 ))
vudestroy
quit
TCL
  SCPL /tmp/rc.tcl /tmp/rc.tcl
  SSHL "cd $HAMMER && setsid nohup ./hammerdbcli auto /tmp/rc.tcl >/tmp/rc_${lane}_${vu}.log 2>&1 &" 2>/dev/null
  # wait out rampup + 30s warmup
  sleep $(( RAMPUP*60 + 30 ))
  Q 'select 1' tpcc >/dev/null   # pre-warm the sampler session
  # steady window = DURATION-1 min; sample d_next_o_id every 10s (stall detect)
  local win=$(( (DURATION-1)*60 )); [ $win -lt 60 ] && win=60
  local no0=$(Q 'select sum(d_next_o_id) from district' tpcc) tr0=$(Q 'select sum(xact_commit+xact_rollback) from pg_stat_database' tpcc) t0=$(date +%s)
  local prev=$no0 froze=0 samples=0 k
  read _ _ csw0 < <(awk '/^ctxt/{print "x x",$2}' /proc/stat)
  for k in $(seq 1 $((win/10))); do
    sleep 10; local cur=$(Q 'select sum(d_next_o_id) from district' tpcc)
    samples=$((samples+1)); [ -n "$cur" ] && [ "$cur" = "$prev" ] && froze=$((froze+1)); prev=${cur:-$prev}
  done
  local no1=$(Q 'select sum(d_next_o_id) from district' tpcc) tr1=$(Q 'select sum(xact_commit+xact_rollback) from pg_stat_database' tpcc) t1=$(date +%s)
  read _ _ csw1 < <(awk '/^ctxt/{print "x x",$2}' /proc/stat)
  local dt=$((t1-t0)); local dmin=$(awk "BEGIN{print $dt/60.0}")
  local nopm=$(awk "BEGIN{printf \"%d\",(${no1:-0}-${no0:-0})/$dmin}")
  local tpm=$(awk "BEGIN{printf \"%d\",(${tr1:-0}-${tr0:-0})/$dmin}")
  local csw=$(awk "BEGIN{printf \"%d\",(${csw1:-0}-${csw0:-0})/$dt}")
  # RSS (PSS) of all postgres + CPU%
  local rss=0 p v; for p in $(pgrep -x postgres); do v=$(awk '/^Pss:/{s+=$2}END{print s+0}' /proc/$p/smaps_rollup 2>/dev/null); rss=$((rss+${v:-0})); done; rss=$((rss/1024))
  local cpu=$(top -bn1|awk '/Cpu\(s\)/{printf "%.0f",100-$8}'|head -1)
  local stall="no"; [ $froze -gt $((samples/3)) ] && stall="STALL:${froze}/${samples}"
  # HammerDB's own NOPM if the monitor survived + latency percentiles from its log
  local hnopm="NA" p95="NA" p99="NA"
  sleep 60  # let the driver finish + write its result
  hnopm=$(SSHL "grep -oE 'System achieved [0-9]+ NOPM' /tmp/rc_${lane}_${vu}.log 2>/dev/null | grep -oE '[0-9]+' | tail -1" 2>/dev/null); hnopm=${hnopm:-NA}
  say "TPROC-C lane=$lane vu=$vu car=$car srv_nopm=$nopm srv_tpm=$tpm hdb_nopm=$hnopm rss=${rss}MB cpu=${cpu}% csw=${csw}/s stall=$stall"
  echo -e "tproc-c\t$lane\t$vu\t$car\t$nopm\t$tpm\t$hnopm\t$p95\t$p99\t$rss\t$cpu\t$csw\t$stall" >> "$RES"
  SSHL "pkill -9 -f hammerdbcli" 2>/dev/null; stop_lane
}

say "MATRIX START $(date -u): lanes='$LANES' bench=$BENCH vu='$VU_LIST' RAM_PCT=$RAM_PCT WAREHOUSES=$WAREHOUSES"
case "$BENCH" in tproc-c|both)
  build_tpcc
  for vu in $VU_LIST; do for lane in $LANES; do run_tpcc "$lane" "$vu"; done; done
;; esac
say "MATRIX DONE $(date -u)"
