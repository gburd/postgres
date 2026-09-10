#!/bin/bash
# Independent of pgbench: 12 plain psql sessions vs 4 carriers.
# If working==carriers, only 4 should answer; the rest hang. Also capture WHERE they wait.
set -u
PGB=/mnt/work/inst/usr/local/pgsql/bin
export LD_LIBRARY_PATH=/mnt/work/inst/usr/local/pgsql/lib64:/usr/local/lib64
export PGHOST=127.0.0.1 PGUSER=postgres PGDATABASE=postgres
RESU=/mnt/work/out; D=/mnt/nvme/iv; port=7777
: > $RESU/IV.txt
sudo pkill -9 -f "postgres -D /mnt/nvme" 2>/dev/null; sudo fuser -k ${port}/tcp >/dev/null 2>&1; sleep 2
find $D -mindepth 1 -delete 2>/dev/null; rmdir $D 2>/dev/null
$PGB/initdb -D $D -U postgres >/dev/null 2>&1
echo "host all all 127.0.0.1/32 trust" >> $D/pg_hba.conf
$PGB/postgres -D $D -c port=$port -c listen_addresses=127.0.0.1 -c shared_buffers=1GB -c huge_pages=off \
  -c multithreaded=on -c pooled_protocol_carriers=4 -c max_connections=100 >$RESU/iv_srv.log 2>&1 &
SRV=$!
for i in $(seq 1 50); do grep -q "ready to accept" $RESU/iv_srv.log 2>/dev/null && break; sleep 1; done
echo "carriers_eff=$($PGB/psql -p $port -tAc 'SELECT count(*) FROM pg_stat_xtc_carriers')" >> $RESU/IV.txt
# 12 independent psql clients, each does a trivial SELECT then sleeps; write a marker on success
for k in $(seq 1 12); do
  ( timeout 25 $PGB/psql -p $port -tAc "SELECT $k, pg_sleep(0.2), 'ok$k'" >/tmp/iv_$k.out 2>&1; echo $? > /tmp/iv_$k.rc ) &
done
sleep 8
echo "--- while they run: what does the SERVER see? ---" >> $RESU/IV.txt
$PGB/psql -p $port -tAc "SELECT count(*) AS total_backends FROM pg_stat_activity WHERE backend_type='client backend'" >> $RESU/IV.txt 2>&1
$PGB/psql -p $port -tAc "SELECT coalesce(wait_event_type,'(running)')||' / '||coalesce(wait_event,'-')||' : '||count(*) FROM pg_stat_activity WHERE backend_type='client backend' GROUP BY 1 ORDER BY 1" >> $RESU/IV.txt 2>&1
echo "--- state breakdown ---" >> $RESU/IV.txt
$PGB/psql -p $port -tAc "SELECT state||' : '||count(*) FROM pg_stat_activity WHERE backend_type='client backend' GROUP BY 1" >> $RESU/IV.txt 2>&1
wait
echo "" >> $RESU/IV.txt
echo "--- RESULTS: how many of the 12 psql clients completed? ---" >> $RESU/IV.txt
ok=0; fail=0
for k in $(seq 1 12); do
  if grep -q "ok$k" /tmp/iv_$k.out 2>/dev/null; then ok=$((ok+1)); else fail=$((fail+1)); fi
done
echo "COMPLETED=$ok  HUNG/FAILED=$fail  (carriers=4)" >> $RESU/IV.txt
echo "--- a failed client's output ---" >> $RESU/IV.txt
for k in $(seq 1 12); do grep -q "ok$k" /tmp/iv_$k.out 2>/dev/null || { echo "client $k: rc=$(cat /tmp/iv_$k.rc 2>/dev/null) out=$(head -c 120 /tmp/iv_$k.out 2>/dev/null)" >> $RESU/IV.txt; break; }; done
$PGB/pg_ctl -D $D -m immediate -w stop >/dev/null 2>&1
echo "IV_COMPLETE" >> $RESU/IV.txt
