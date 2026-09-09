#!/bin/bash
# v1.43.0 decisive test: does XTC_TAIL_WAKE show a dispatch for the stranded fiber?
#   WAKE present + no RUN  => wake delivered, task never scheduled (scheduler/run-queue bug)
#   NO WAKE at all         => completion never dispatched to the task (dispatch/poll bug)
# Plus xtc_tail_dropped() so absence is interpretable, and idle-only LOOP_POLL for liveness.
set -u
PGB=/mnt/work/inst/usr/local/pgsql/bin
export LD_LIBRARY_PATH=/mnt/work/inst/usr/local/pgsql/lib64:/usr/local/lib64
export PGHOST=127.0.0.1 PGUSER=postgres PGDATABASE=postgres
BASE=/mnt/nvme; RESU=/mnt/work/out
SB=$(( $(free -m|awk '/Mem:/{print $2}') * 40 / 100 ))MB
: > $RESU/W.txt
HANGS=0
for n in $(seq 1 10); do
  [ $HANGS -ge 2 ] && break
  port=$((7800+n)); D=$BASE/w$n
  sudo pkill -9 -f "postgres -D /mnt/nvme" 2>/dev/null; sudo fuser -k ${port}/tcp >/dev/null 2>&1; sleep 2
  find $D -mindepth 1 -delete 2>/dev/null; rmdir $D 2>/dev/null
  $PGB/initdb -D $D -U postgres >/dev/null 2>&1
  echo "host all all 127.0.0.1/32 trust" >> $D/pg_hba.conf
  PG_XTC_TAIL=1 $PGB/postgres -D $D -c port=$port -c listen_addresses=127.0.0.1 -c shared_buffers=$SB -c huge_pages=off \
    -c multithreaded=on -c pooled_protocol_carriers=0 -c fsync=on -c synchronous_commit=on \
    -c full_page_writes=on -c autovacuum=on -c max_connections=200 >$RESU/w${n}_srv.log 2>&1 &
  SRVPID=$!
  for i in $(seq 1 50); do grep -q "ready to accept" $RESU/w${n}_srv.log 2>/dev/null && break; sleep 1; done
  timeout 120 $PGB/pgbench -p $port -i -s 50 >/dev/null 2>&1
  out=$(timeout 60 $PGB/pgbench -p $port -c 32 -j 8 -T 8 2>&1)
  if echo "$out" | grep -q "tps = "; then
    echo "run $n: PASS ($(echo "$out"|grep -oE 'tps = [0-9.]+'|head -1))" >> $RESU/W.txt
  else
    HANGS=$((HANGS+1))
    echo "=== HANG #$HANGS (run $n) ===" >> $RESU/W.txt
    sleep 12   # keep recording through the freeze
    sudo timeout 60 gdb -p $SRVPID -batch -ex "source /tmp/xtc-gdb.py" \
        -ex "xtc-tail-dump /tmp/w$n.xtcl" -ex "xtc-rings" -ex "xtc-stranded" >/tmp/w${n}_gdb.txt 2>&1
    python3 /mnt/work/xtc/tools/xtc-tail.py /tmp/w$n.xtcl > /tmp/w${n}_tail.txt 2>&1
    echo "  tail events=$(wc -l </tmp/w${n}_tail.txt)" >> $RESU/W.txt
    echo "  kinds: $(grep -oE 'SCHED [A-Z_]+' /tmp/w${n}_tail.txt | sort | uniq -c | tr '\n' ' ')" >> $RESU/W.txt
    echo "  DROPPED (v1.43.0 -- if >0, absence is NOT evidence): $(grep -iE 'dropped' /tmp/w${n}_gdb.txt | head -2 | tr '\n' ' ')" >> $RESU/W.txt
    python3 - "$n" <<'PY' >> $RESU/W.txt 2>&1
import re,sys,collections
n=sys.argv[1]
lines=open("/tmp/w%s_tail.txt"%n).read().splitlines()
park=collections.Counter();run=collections.Counter();aio=collections.Counter()
wake=collections.Counter();lastpark={}
for i,ln in enumerate(lines):
    m=re.search(r"pid=(\d+)\.(\d+)\.(\d+)",ln)
    if not m: continue
    p="%s.%s.%s"%m.groups()
    if "SCHED PARK" in ln:
        park[p]+=1; lastpark[p]=i
        if "detail=" in ln: aio[p]+=1
    elif "SCHED RUN"  in ln: run[p]+=1
    elif "SCHED WAKE" in ln: wake[p]+=1
c=[(p,aio[p],park[p]-run[p],lastpark[p]) for p in park if park[p]>run[p] and aio[p]>0]
print("  STRANDED (deficit>0, aio_park>0):", [(p,"aio=%d"%a,"def=%d"%d) for p,a,d,_ in c] or "none")
if c:
    p,a,d,L=c[0]; loop=p.split(".")[0]
    print("  stranded=%s loop=%s  total WAKE events for it=%d" % (p,loop,wake[p]))
    after=lines[L+1:]
    w_after=[ln for ln in after if "SCHED WAKE" in ln and "pid=%s "%p in ln+" "]
    print("  WAKE events for %s AFTER its final unmatched PARK: %d" % (p,len(w_after)))
    for ln in w_after[:3]: print("     ",ln.strip())
    print("  --- its last 6 events ---")
    own=[ln for ln in lines if re.search(r"pid=%s\b"%re.escape(p),ln)]
    for ln in own[-6:]: print("     ",ln.strip())
    ip=[ln for ln in after if "LOOP_POLL" in ln and re.search(r"pid=%s\."%loop,ln)]
    print("  idle LOOP_POLL on loop %s after the park: %d (idle-only in 1.43.0)" % (loop,len(ip)))
    print()
    if len(w_after)>0:
        print("  VERDICT: WAKE was DISPATCHED but the task never RAN")
        print("        => scheduler/run-queue bug, NOT a missed completion")
    else:
        print("  VERDICT: NO WAKE dispatched to the stranded task")
        print("        => the completion never reached it: dispatch/poll side")
PY
    sudo pkill -9 -f "pgbench -p $port" 2>/dev/null; $PGB/pg_ctl -D $D -m immediate -w stop >/dev/null 2>&1
  fi
  sudo pkill -9 -f "pgbench -p $port" 2>/dev/null; $PGB/pg_ctl -D $D -m immediate -w stop >/dev/null 2>&1; sleep 1
done
echo "W_COMPLETE hangs=$HANGS" >> $RESU/W.txt
