#!/bin/bash
set -u
PGB=/mnt/work/inst/usr/local/pgsql/bin
export LD_LIBRARY_PATH=/mnt/work/inst/usr/local/pgsql/lib64:/usr/local/lib64
export PGHOST=127.0.0.1 PGUSER=postgres PGDATABASE=postgres
BASE=/mnt/nvme; RESU=/mnt/work/out; SB=$(( $(free -m|awk '/Mem:/{print $2}')*40/100 ))MB
: > $RESU/V.txt; mkdir -p /mnt/work/traces
H=0
for n in $(seq 1 10); do
  [ $H -ge 2 ] && break
  port=$((7960+n)); D=$BASE/v$n
  sudo pkill -9 -f "postgres -D /mnt/nvme" 2>/dev/null; sudo fuser -k ${port}/tcp >/dev/null 2>&1; sleep 2
  find $D -mindepth 1 -delete 2>/dev/null; rmdir $D 2>/dev/null
  $PGB/initdb -D $D -U postgres >/dev/null 2>&1
  echo "host all all 127.0.0.1/32 trust" >> $D/pg_hba.conf
  PG_XTC_TAIL=1 $PGB/postgres -D $D -c port=$port -c listen_addresses=127.0.0.1 -c shared_buffers=$SB \
    -c huge_pages=off -c multithreaded=on -c pooled_protocol_carriers=0 -c fsync=on \
    -c synchronous_commit=on -c full_page_writes=on -c autovacuum=on -c max_connections=200 \
    >$RESU/v${n}_srv.log 2>&1 &
  SRV=$!
  for i in $(seq 1 50); do grep -q "ready to accept" $RESU/v${n}_srv.log 2>/dev/null && break; sleep 1; done
  timeout 150 $PGB/pgbench -p $port -i -s 50 >/dev/null 2>&1
  out=$(timeout 60 $PGB/pgbench -p $port -c 32 -j 8 -T 8 2>&1)
  if echo "$out"|grep -q "tps = "; then echo "run $n: PASS $(echo "$out"|grep -oE 'tps = [0-9.]+'|head -1)" >> $RESU/V.txt; else
    H=$((H+1)); echo "=== HANG #$H (run $n) ===" >> $RESU/V.txt
    sleep 2
    sudo timeout 90 gdb -p $SRV -batch -ex "source /tmp/xtc-gdb.py" \
      -ex "xtc-tail-dropped" -ex "xtc-tail-dump /mnt/work/traces/v$n.xtcl" \
      -ex "xtc-rings" -ex "xtc-cqes" >/mnt/work/traces/v${n}_gdb.txt 2>&1
    sudo chown ec2-user /mnt/work/traces/v$n.xtcl 2>/dev/null
    grep -iE "emitted=|dropped=" /mnt/work/traces/v${n}_gdb.txt | head -2 | sed 's/^/  /' >> $RESU/V.txt
    echo "  --- rings with unreaped>0 (loop id ring_fd unreaped ovf) ---" >> $RESU/V.txt
    awk 'NF>=8 && $1 ~ /^0x/ && $6+0>0 {print "    loop="$2, "fd="$4, "unreaped="$6, "ovf="$7}' /mnt/work/traces/v${n}_gdb.txt | head -6 >> $RESU/V.txt
    echo "  --- xtc-cqes: the DECISIVE join (user_data -> task) ---" >> $RESU/V.txt
    sed -n '/xtc-cqes/,$p' /mnt/work/traces/v${n}_gdb.txt | grep -E "user_data=|loop .*ring_fd=" | head -14 | sed 's/^/    /' >> $RESU/V.txt
    python3 /mnt/work/xtc/tools/xtc-tail.py /mnt/work/traces/v$n.xtcl --strands 2>&1 | head -12 | sed 's/^/  /' >> $RESU/V.txt
    sudo pkill -9 -f "pgbench -p $port" 2>/dev/null; $PGB/pg_ctl -D $D -m immediate -w stop >/dev/null 2>&1
  fi
  sudo pkill -9 -f "pgbench -p $port" 2>/dev/null; $PGB/pg_ctl -D $D -m immediate -w stop >/dev/null 2>&1; sleep 1
done
echo "V_COMPLETE hangs=$H" >> $RESU/V.txt
