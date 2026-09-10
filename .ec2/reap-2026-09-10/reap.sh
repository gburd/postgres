#!/bin/bash
# Their ask: run xtc-tail.py --strands on a hang capture and report which bucket the strands land in.
#   no REAP        => kernel never posted it / we never looked  -> submission / ring / the wait
#   REAP, no WAKE  => reaper consumed it and dropped it         -> reap-to-dispatch handoff
set -u
PGB=/mnt/work/inst/usr/local/pgsql/bin
export LD_LIBRARY_PATH=/mnt/work/inst/usr/local/pgsql/lib64:/usr/local/lib64
export PGHOST=127.0.0.1 PGUSER=postgres PGDATABASE=postgres
BASE=/mnt/nvme; RESU=/mnt/work/out; SB=$(( $(free -m|awk '/Mem:/{print $2}')*40/100 ))MB
: > $RESU/RP.txt
mkdir -p /mnt/work/traces
HANGS=0
for n in $(seq 1 10); do
  [ $HANGS -ge 2 ] && break
  port=$((7840+n)); D=$BASE/rp$n
  sudo pkill -9 -f "postgres -D /mnt/nvme" 2>/dev/null; sudo fuser -k ${port}/tcp >/dev/null 2>&1; sleep 2
  find $D -mindepth 1 -delete 2>/dev/null; rmdir $D 2>/dev/null
  $PGB/initdb -D $D -U postgres >/dev/null 2>&1
  echo "host all all 127.0.0.1/32 trust" >> $D/pg_hba.conf
  PG_XTC_TAIL=1 $PGB/postgres -D $D -c port=$port -c listen_addresses=127.0.0.1 -c shared_buffers=$SB \
    -c huge_pages=off -c multithreaded=on -c pooled_protocol_carriers=0 -c fsync=on \
    -c synchronous_commit=on -c full_page_writes=on -c autovacuum=on -c max_connections=200 \
    >$RESU/rp${n}_srv.log 2>&1 &
  SRV=$!
  for i in $(seq 1 50); do grep -q "ready to accept" $RESU/rp${n}_srv.log 2>/dev/null && break; sleep 1; done
  timeout 150 $PGB/pgbench -p $port -i -s 50 >/dev/null 2>&1
  out=$(timeout 60 $PGB/pgbench -p $port -c 32 -j 8 -T 8 2>&1)
  if echo "$out" | grep -q "tps = "; then echo "run $n: PASS $(echo "$out"|grep -oE 'tps = [0-9.]+'|head -1)" >> $RESU/RP.txt; else
    HANGS=$((HANGS+1)); T=/mnt/work/traces/hang${HANGS}.xtcl
    echo "=== HANG #$HANGS (run $n) ===" >> $RESU/RP.txt
    sleep 12
    sudo timeout 90 gdb -p $SRV -batch -ex "source /tmp/xtc-gdb.py" \
      -ex "xtc-tail-dropped" -ex "xtc-tail-dump $T" -ex "xtc-rings" >/mnt/work/traces/hang${HANGS}_gdb.txt 2>&1
    sudo chown ec2-user "$T" 2>/dev/null
    echo "--- xtc-tail-dropped ---" >> $RESU/RP.txt
    grep -iE "emitted=|dropped=|WARNING" /mnt/work/traces/hang${HANGS}_gdb.txt | head -3 | sed 's/^/  /' >> $RESU/RP.txt
    echo "--- THE ANSWER: xtc-tail.py --strands ---" >> $RESU/RP.txt
    python3 /mnt/work/xtc/tools/xtc-tail.py "$T" --strands >> $RESU/RP.txt 2>&1
    echo "--- REAP detail split (tagged vs 0) ---" >> $RESU/RP.txt
    python3 /mnt/work/xtc/tools/xtc-tail.py "$T" 2>/dev/null | grep -c "SCHED REAP" | sed 's/^/  total REAP: /' >> $RESU/RP.txt
    python3 /mnt/work/xtc/tools/xtc-tail.py "$T" 2>/dev/null | grep "SCHED REAP" | grep -cE "detail=0x0\b|detail=0\b" | sed 's/^/  REAP detail=0 (consumed, not handed on): /' >> $RESU/RP.txt
    sudo pkill -9 -f "pgbench -p $port" 2>/dev/null; $PGB/pg_ctl -D $D -m immediate -w stop >/dev/null 2>&1
  fi
  sudo pkill -9 -f "pgbench -p $port" 2>/dev/null; $PGB/pg_ctl -D $D -m immediate -w stop >/dev/null 2>&1; sleep 1
done
echo "RP_COMPLETE hangs=$HANGS" >> $RESU/RP.txt
