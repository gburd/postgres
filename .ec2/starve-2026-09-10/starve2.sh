#!/bin/bash
# Correct measurement: per-transaction --log, then compute MAX from the log (as P1 did).
# Question: does ONE read-only session stall for ~the whole run at 2 sessions/carrier?
set -u
PGB=/mnt/work/inst/usr/local/pgsql/bin
export LD_LIBRARY_PATH=/mnt/work/inst/usr/local/pgsql/lib64:/usr/local/lib64
export PGHOST=127.0.0.1 PGUSER=postgres PGDATABASE=postgres
BASE=/mnt/nvme; RESU=/mnt/work/out; SB=$(( $(free -m|awk '/Mem:/{print $2}')*40/100 ))MB
: > $RESU/S2.txt
cell () {
  local clients="$1"; local car="$2"; local lane="$3"; local n="$4"
  local port=$((7940+n)); local D=$BASE/s2_$n; local LOGD=/mnt/nvme/pblog$n
  sudo pkill -9 -f "postgres -D /mnt/nvme" 2>/dev/null; sudo fuser -k ${port}/tcp >/dev/null 2>&1; sleep 2
  for d in "$D" "$LOGD"; do find "$d" -mindepth 1 -delete 2>/dev/null; rmdir "$d" 2>/dev/null; done
  mkdir -p "$LOGD"
  $PGB/initdb -D $D -U postgres >/dev/null 2>&1
  echo "host all all 127.0.0.1/32 trust" >> $D/pg_hba.conf
  local MT="-c multithreaded=on -c pooled_protocol_carriers=$car"
  [ "$lane" = fork ] && MT="-c multithreaded=off"
  PG_XTC_TAIL=1 $PGB/postgres -D $D -c port=$port -c listen_addresses=127.0.0.1 -c shared_buffers=$SB \
    -c huge_pages=off $MT -c fsync=on -c synchronous_commit=on -c autovacuum=on \
    -c max_connections=200 >$RESU/s2_${n}.log 2>&1 &
  local SRV=$!
  for i in $(seq 1 50); do grep -q "ready to accept" $RESU/s2_${n}.log 2>/dev/null && break; sleep 1; done
  timeout 180 $PGB/pgbench -p $port -i -s 20 >/dev/null 2>&1
  local eff=$($PGB/psql -p $port -tAc "SELECT count(*) FROM pg_stat_xtc_carriers" 2>/dev/null)
  cd "$LOGD"
  local out=$(timeout 90 $PGB/pgbench -p $port -n -S -T 30 -c $clients -j $clients --log postgres 2>&1)
  local tps=$(echo "$out"|grep -oE "tps = [0-9.]+"|head -1|sed 's/tps = //')
  # MAX + p99 from the per-txn logs (field 3 = latency us)
  local stats=$(cat ${LOGD}/pgbench_log.* 2>/dev/null | awk '{print $3}' | sort -n | awk '
    {a[NR]=$1} END{if(NR==0){print "0 0 0"; exit}
      printf "%.3f %.3f %d\n", a[int(NR*0.99)]/1000.0, a[NR]/1000.0, NR}')
  local p99=$(echo $stats|cut -d' ' -f1); local mx=$(echo $stats|cut -d' ' -f2); local cnt=$(echo $stats|cut -d' ' -f3)
  printf "%-5s c=%-3s car_eff=%-3s tps=%-12s p99=%-9s MAX_ms=%-11s txns=%s\n" \
     "$lane" "$clients" "${eff:-0}" "${tps:-FAIL}" "$p99" "$mx" "$cnt" >> $RESU/S2.txt
  if [ -n "$mx" ] && awk "BEGIN{exit !($mx > 5000)}"; then
    echo "   *** STALL: one txn took ${mx}ms of a 30000ms run ***" >> $RESU/S2.txt
    sudo timeout 60 gdb -p $SRV -batch -ex "source /tmp/xtc-gdb.py" -ex "xtc-tail-dump /tmp/s2_$n.xtcl" >/tmp/s2_${n}_gdb.txt 2>&1
    python3 /mnt/work/xtc/tools/xtc-tail.py /tmp/s2_$n.xtcl > /tmp/s2_${n}_tail.txt 2>&1
    echo "   tail: $(grep -oE 'SCHED [A-Z_]+' /tmp/s2_${n}_tail.txt|sort|uniq -c|tr '\n' ' ')" >> $RESU/S2.txt
  fi
  cd /; sudo pkill -9 -f "pgbench -p $port" 2>/dev/null
  $PGB/pg_ctl -D $D -m immediate -w stop >/dev/null 2>&1; sleep 1
}
i=0
for rep in 1 2 3; do
  i=$((i+1)); cell 16 -1 xtc  $i
  i=$((i+1)); cell 16 0  fork $i     # fork comparison at the same client count
done
echo "S2_COMPLETE" >> $RESU/S2.txt
