#!/bin/bash
set -u
PGB=/mnt/work/inst/usr/local/pgsql/bin
export LD_LIBRARY_PATH=/mnt/work/inst/usr/local/pgsql/lib64:/usr/local/lib64
export PGHOST=127.0.0.1 PGUSER=postgres PGDATABASE=postgres
RESU=/mnt/work/out; : > $RESU/J.txt
printf "%-6s %-4s %-9s %-8s %-9s %s\n" clients -j car_eff working starved tps >> $RESU/J.txt
cell () {
  local clients="$1"; local jobs="$2"; local car="$3"; local n="$4"
  local port=$((7860+n)); local D=/mnt/nvme/j_$n; local LOGD=/mnt/nvme/jl$n
  sudo pkill -9 -f "postgres -D /mnt/nvme" 2>/dev/null; sudo fuser -k ${port}/tcp >/dev/null 2>&1; sleep 2
  for d in "$D" "$LOGD"; do find "$d" -mindepth 1 -delete 2>/dev/null; rmdir "$d" 2>/dev/null; done
  mkdir -p "$LOGD"; $PGB/initdb -D $D -U postgres >/dev/null 2>&1
  echo "host all all 127.0.0.1/32 trust" >> $D/pg_hba.conf
  $PGB/postgres -D $D -c port=$port -c listen_addresses=127.0.0.1 -c shared_buffers=1GB -c huge_pages=off \
    -c multithreaded=on -c pooled_protocol_carriers=$car -c max_connections=200 >$RESU/j_${n}.log 2>&1 &
  for i in $(seq 1 50); do grep -q "ready to accept" $RESU/j_${n}.log 2>/dev/null && break; sleep 1; done
  timeout 180 $PGB/pgbench -p $port -i -s 20 >/dev/null 2>&1
  local eff=$($PGB/psql -p $port -tAc "SELECT count(*) FROM pg_stat_xtc_carriers" 2>/dev/null)
  cd "$LOGD"
  local out=$(timeout 60 $PGB/pgbench -p $port -n -S -T 15 -c $clients -j $jobs --log postgres 2>&1)
  local tps=$(echo "$out"|grep -oE "tps = [0-9.]+"|head -1|sed 's/tps = //')
  local cs=$(cat ${LOGD}/pgbench_log.* 2>/dev/null | awk '{c[$1]++} END{w=0;s=0;for(k in c){if(c[k]<=2)s++;else w++} print w" "s}')
  printf "%-6s %-4s %-9s %-8s %-9s %s\n" "$clients" "$jobs" "${eff:-0}" "$(echo $cs|cut -d' ' -f1)" "$(echo $cs|cut -d' ' -f2)" "${tps:-FAIL}" >> $RESU/J.txt
  cd /; sudo pkill -9 -f "pgbench -p $port" 2>/dev/null; $PGB/pg_ctl -D $D -m immediate -w stop >/dev/null 2>&1; sleep 1
}
# same 16 clients, vary -j: if starvation tracks -j rather than carriers, it is a pgbench-thread artifact
i=0
i=$((i+1)); cell 16 1  8 $i
i=$((i+1)); cell 16 4  8 $i
i=$((i+1)); cell 16 8  8 $i
i=$((i+1)); cell 16 16 8 $i
echo "J_COMPLETE" >> $RESU/J.txt
