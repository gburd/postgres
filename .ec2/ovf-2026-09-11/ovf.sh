#!/bin/bash
# Re-measure the ring with the ovf column: is unreaped=0 real, or masked by CQ overflow?
set -u
PGB=/mnt/work/inst/usr/local/pgsql/bin
export LD_LIBRARY_PATH=/mnt/work/inst/usr/local/pgsql/lib64:/usr/local/lib64
export PGHOST=127.0.0.1 PGUSER=postgres PGDATABASE=postgres
BASE=/mnt/nvme; RESU=/mnt/work/out; SB=$(( $(free -m|awk '/Mem:/{print $2}')*40/100 ))MB
: > $RESU/OV.txt
mkdir -p /mnt/work/traces
H=0
for n in $(seq 1 10); do
  [ $H -ge 2 ] && break
  port=$((7890+n)); D=$BASE/ov$n
  sudo pkill -9 -f "postgres -D /mnt/nvme" 2>/dev/null; sudo fuser -k ${port}/tcp >/dev/null 2>&1; sleep 2
  find $D -mindepth 1 -delete 2>/dev/null; rmdir $D 2>/dev/null
  $PGB/initdb -D $D -U postgres >/dev/null 2>&1
  echo "host all all 127.0.0.1/32 trust" >> $D/pg_hba.conf
  PG_XTC_TAIL=1 $PGB/postgres -D $D -c port=$port -c listen_addresses=127.0.0.1 -c shared_buffers=$SB \
    -c huge_pages=off -c multithreaded=on -c pooled_protocol_carriers=0 -c fsync=on \
    -c synchronous_commit=on -c full_page_writes=on -c autovacuum=on -c max_connections=200 \
    >$RESU/ov${n}_srv.log 2>&1 &
  SRV=$!
  for i in $(seq 1 50); do grep -q "ready to accept" $RESU/ov${n}_srv.log 2>/dev/null && break; sleep 1; done
  timeout 150 $PGB/pgbench -p $port -i -s 50 >/dev/null 2>&1
  out=$(timeout 60 $PGB/pgbench -p $port -c 32 -j 8 -T 8 2>&1)
  if echo "$out" | grep -q "tps = "; then echo "run $n: PASS" >> $RESU/OV.txt; else
    H=$((H+1)); echo "=== HANG #$H (run $n) ===" >> $RESU/OV.txt
    sleep 2
    for s in 1 2 3; do
      echo "--- xtc-rings sample $s ---" >> $RESU/OV.txt
      sudo timeout 45 gdb -p $SRV -batch -ex "source /tmp/xtc-gdb.py" -ex "xtc-rings" 2>/dev/null \
        | awk 'NR<=2 || ($0 ~ /0x/)' | head -36 >> $RESU/OV.txt
      sleep 1
    done
    echo "--- ANY ring with ovf=1 across the samples? ---" >> $RESU/OV.txt
    sudo timeout 45 gdb -p $SRV -batch -ex "source /tmp/xtc-gdb.py" -ex "xtc-rings" 2>/dev/null > /tmp/ov_r$n.txt
    awk '$0 ~ /0x/ {print}' /tmp/ov_r$n.txt | awk '{print $(NF-1), $NF}' | sort | uniq -c | sed 's/^/    /' >> $RESU/OV.txt
    sudo timeout 60 gdb -p $SRV -batch -ex "source /tmp/xtc-gdb.py" -ex "xtc-tail-dump /mnt/work/traces/ov$n.xtcl" >/dev/null 2>&1
    sudo chown ec2-user /mnt/work/traces/ov$n.xtcl 2>/dev/null
    sudo pkill -9 -f "pgbench -p $port" 2>/dev/null; $PGB/pg_ctl -D $D -m immediate -w stop >/dev/null 2>&1
  fi
  sudo pkill -9 -f "pgbench -p $port" 2>/dev/null; $PGB/pg_ctl -D $D -m immediate -w stop >/dev/null 2>&1; sleep 1
done
echo "OV_COMPLETE hangs=$H" >> $RESU/OV.txt
