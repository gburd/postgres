#!/bin/bash
# HYPOTHESIS: with pooled carriers=N, only N sessions get service; sessions N+1..C are STARVED.
# Test: vary clients against a KNOWN carrier count and count how many clients make progress.
set -u
PGB=/mnt/work/inst/usr/local/pgsql/bin
export LD_LIBRARY_PATH=/mnt/work/inst/usr/local/pgsql/lib64:/usr/local/lib64
export PGHOST=127.0.0.1 PGUSER=postgres PGDATABASE=postgres
BASE=/mnt/nvme; RESU=/mnt/work/out; SB=$(( $(free -m|awk '/Mem:/{print $2}')*40/100 ))MB
: > $RESU/F.txt
printf "%-5s %-8s %-9s %-8s %-12s %-8s %s\n" lane clients car_eff working starved tps note >> $RESU/F.txt
cell () {
  local clients="$1"; local car="$2"; local lane="$3"; local n="$4"
  local port=$((7980+n)); local D=$BASE/f_$n; local LOGD=/mnt/nvme/fl$n
  sudo pkill -9 -f "postgres -D /mnt/nvme" 2>/dev/null; sudo fuser -k ${port}/tcp >/dev/null 2>&1; sleep 2
  for d in "$D" "$LOGD"; do find "$d" -mindepth 1 -delete 2>/dev/null; rmdir "$d" 2>/dev/null; done
  mkdir -p "$LOGD"; $PGB/initdb -D $D -U postgres >/dev/null 2>&1
  echo "host all all 127.0.0.1/32 trust" >> $D/pg_hba.conf
  local MT="-c multithreaded=on -c pooled_protocol_carriers=$car"
  [ "$lane" = fork ] && MT="-c multithreaded=off"
  $PGB/postgres -D $D -c port=$port -c listen_addresses=127.0.0.1 -c shared_buffers=$SB -c huge_pages=off \
    $MT -c fsync=on -c autovacuum=on -c max_connections=300 >$RESU/f_${n}.log 2>&1 &
  for i in $(seq 1 50); do grep -q "ready to accept" $RESU/f_${n}.log 2>/dev/null && break; sleep 1; done
  timeout 180 $PGB/pgbench -p $port -i -s 20 >/dev/null 2>&1
  local eff=$($PGB/psql -p $port -tAc "SELECT count(*) FROM pg_stat_xtc_carriers" 2>/dev/null)
  cd "$LOGD"
  local out=$(timeout 90 $PGB/pgbench -p $port -n -S -T 20 -c $clients -j $clients --log postgres 2>&1)
  local tps=$(echo "$out"|grep -oE "tps = [0-9.]+"|head -1|sed 's/tps = //')
  # a client is STARVED if it completed <= 2 txns in 20s (healthy clients do ~10^5)
  local counts=$(cat ${LOGD}/pgbench_log.* 2>/dev/null | awk '{c[$1]++} END{w=0;s=0; for(k in c){ if(c[k]<=2) s++; else w++ } print w" "s}')
  local w=$(echo $counts|cut -d' ' -f1); local s=$(echo $counts|cut -d' ' -f2)
  local note=""
  [ "${s:-0}" -gt 0 ] && note="STARVED=$s (car_eff=${eff:-0})"
  printf "%-5s %-8s %-9s %-8s %-12s %-8s %s\n" "$lane" "$clients" "${eff:-0}" "${w:-?}" "${s:-?}" "${tps:-FAIL}" "$note" >> $RESU/F.txt
  cd /; sudo pkill -9 -f "pgbench -p $port" 2>/dev/null
  $PGB/pg_ctl -D $D -m immediate -w stop >/dev/null 2>&1; sleep 1
}
i=0
# xtc: does 'working' track carrier count exactly?
for c in 4 8 12 16 24 32; do i=$((i+1)); cell $c -1 xtc $i; done
# explicit small pool: 4 carriers, 16 clients -> if hypothesis holds, 4 work / 12 starve
i=$((i+1)); cell 16 4 xtc $i
# fork control at the same counts
for c in 16 32; do i=$((i+1)); cell $c 0 fork $i; done
echo "F_COMPLETE" >> $RESU/F.txt
