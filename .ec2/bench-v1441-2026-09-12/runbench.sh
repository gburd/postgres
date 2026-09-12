#!/bin/bash
# Fork vs fiber, write-heavy (TPC-B) + read (-S). Fresh server per run, median of 3.
# libxtc v1.44.1 (lost-wake fixed). Committed clean HEAD binary.
set -u
PGB=/mnt/nvme/instbench/usr/local/pgsql/bin
export LD_LIBRARY_PATH=/mnt/nvme/instbench/usr/local/pgsql/lib64:/usr/local/lib64
export PGHOST=127.0.0.1 PGUSER=postgres PGDATABASE=postgres
RESU=/mnt/nvme/out; : > $RESU/BENCH.txt
SBM=$(( $(free -m|awk '/Mem:/{print $2}') * 85 / 100 ))MB   # 85% RAM per spec
SCALE=50; CL=32; J=8; WARM=15; DUR=60
printf "%-6s %-9s %-4s %-3s run tps\n" lane workload cl "" >> $RESU/BENCH.txt

cell () {
  local lane="$1" wl="$2" run="$3"
  local port=$((7600 + RANDOM % 400))
  local D=/mnt/nvme/b_${lane}_${wl}_${run}
  local LOGD=/mnt/nvme/blog_${lane}_${wl}_${run}
  local MT="-c multithreaded=on -c pooled_protocol_carriers=0"
  [ "$lane" = fork ] && MT="-c multithreaded=off"
  sudo pkill -9 -f "postgres -D /mnt/nvme/b_${lane}_${wl}_${run} " 2>/dev/null
  sudo fuser -k ${port}/tcp >/dev/null 2>&1; sleep 2
  for d in "$D" "$LOGD"; do find "$d" -mindepth 1 -delete 2>/dev/null; rmdir "$d" 2>/dev/null; done
  mkdir -p "$LOGD"
  $PGB/initdb -D $D -U postgres >/dev/null 2>&1
  echo "host all all 127.0.0.1/32 trust" >> $D/pg_hba.conf
  $PGB/postgres -D $D -c port=$port -c listen_addresses=127.0.0.1 -c shared_buffers=$SBM \
    -c huge_pages=off $MT -c fsync=on -c synchronous_commit=on -c full_page_writes=on \
    -c autovacuum=on -c max_connections=300 -c max_wal_size=16GB >$RESU/srv_${lane}_${wl}_${run}.log 2>&1 &
  local SRV=$!
  for i in $(seq 1 60); do grep -q "ready to accept" $RESU/srv_${lane}_${wl}_${run}.log 2>/dev/null && break; sleep 1; done
  if ! grep -q "ready to accept" $RESU/srv_${lane}_${wl}_${run}.log 2>/dev/null; then
    printf "%-6s %-9s c=%-2s %s %s\n" "$lane" "$wl" "$CL" "$run" "SRV_NOSTART" >> $RESU/BENCH.txt; return; fi
  if ! timeout 200 $PGB/pgbench -p $port -i -s $SCALE >$LOGD/init.log 2>&1; then
    printf "%-6s %-9s c=%-2s %s %s\n" "$lane" "$wl" "$CL" "$run" "INIT_FAIL:$(grep -iE 'closed the connection|error' $LOGD/init.log|head -1|tr -d '\t'|cut -c1-40)" >> $RESU/BENCH.txt
    sudo pkill -9 -f "postgres -D /mnt/nvme/b_${lane}_${wl}_${run} " 2>/dev/null; return; fi
  local args="-M prepared"
  [ "$wl" = read ] && args="-S -M prepared"
  cd "$LOGD"
  local out=$(timeout 200 $PGB/pgbench -p $port -n $args -T $DUR -c $CL -j $J --log postgres 2>&1)
  cd /
  local tps=$(echo "$out"|grep -oE "tps = [0-9.]+"|head -1|sed 's/tps = //')
  if [ -n "$tps" ]; then
    # per-client fairness: clients that did <=2 txns (starved)
    local starved=$(cat ${LOGD}/pgbench_log.* 2>/dev/null|awk '{c[$1]++}END{s=0;for(k in c)if(c[k]<=2)s++;print s+0}')
    printf "%-6s %-9s c=%-2s %s PASS tps=%s starved=%s\n" "$lane" "$wl" "$CL" "$run" "$tps" "$starved" >> $RESU/BENCH.txt
  else
    local alive=$(pgrep -f "postgres -D /mnt/nvme/b_${lane}_${wl}_${run} "|head -1)
    if [ -n "$alive" ]; then st="HANG(alive)"; else st="CRASH(gone)"; fi
    # external SIGTERM check
    grep -qiE "fast shutdown request|smart shutdown" $RESU/srv_${lane}_${wl}_${run}.log && st="VOID(ext-shutdown)"
    printf "%-6s %-9s c=%-2s %s %s %s\n" "$lane" "$wl" "$CL" "$run" "$st" "$(echo "$out"|grep -iE 'error'|head -1|tr -d '\t'|cut -c1-40)" >> $RESU/BENCH.txt
  fi
  sudo pkill -9 -f "postgres -D /mnt/nvme/b_${lane}_${wl}_${run} " 2>/dev/null
  $PGB/pg_ctl -D $D -m immediate -w stop >/dev/null 2>&1; sleep 1
}
for wl in write read; do
  for lane in fork xtc; do
    for run in 1 2 3; do cell "$lane" "$wl" "$run"; done
  done
done
echo "BENCH_COMPLETE" >> $RESU/BENCH.txt
