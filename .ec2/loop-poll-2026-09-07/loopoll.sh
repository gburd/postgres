#!/bin/bash
# libxtc v1.42.0: XTC_TAIL_LOOP_POLL + DUMP LATER (their two-part ask).
# Question: after the stranded fiber's unmatched aio PARK, does its loop keep
# emitting LOOP_POLL (=> lost wake, loop alive) or go silent while others poll
# (=> dead/blocked worker)?
set -u
PGB=/mnt/work/inst/usr/local/pgsql/bin
export LD_LIBRARY_PATH=/mnt/work/inst/usr/local/pgsql/lib64:/usr/local/lib64
export PGHOST=127.0.0.1 PGUSER=postgres PGDATABASE=postgres
BASE=/mnt/nvme; RESU=/mnt/work/out
SB=$(( $(free -m|awk '/Mem:/{print $2}') * 40 / 100 ))MB
: > $RESU/LP.txt
for n in $(seq 1 8); do
  port=$((7600+n)); D=$BASE/lp$n
  sudo pkill -9 -f "postgres -D /mnt/nvme" 2>/dev/null; sudo fuser -k ${port}/tcp >/dev/null 2>&1; sleep 2
  find $D -mindepth 1 -delete 2>/dev/null; rmdir $D 2>/dev/null
  $PGB/initdb -D $D -U postgres >/dev/null 2>&1
  echo "host all all 127.0.0.1/32 trust" >> $D/pg_hba.conf
  PG_XTC_TAIL=1 $PGB/postgres -D $D -c port=$port -c listen_addresses=127.0.0.1 -c shared_buffers=$SB -c huge_pages=off \
    -c multithreaded=on -c pooled_protocol_carriers=0 -c fsync=on -c synchronous_commit=on \
    -c full_page_writes=on -c autovacuum=on -c max_connections=200 >$RESU/lp${n}_srv.log 2>&1 &
  SRVPID=$!
  for i in $(seq 1 50); do grep -q "ready to accept" $RESU/lp${n}_srv.log 2>/dev/null && break; sleep 1; done
  timeout 120 $PGB/pgbench -p $port -i -s 50 >/dev/null 2>&1
  out=$(timeout 60 $PGB/pgbench -p $port -c 32 -j 8 -T 8 2>&1)
  if echo "$out" | grep -q "tps = "; then echo "run $n: PASS" >> $RESU/LP.txt; else
    echo "=== run $n HANG (pid=$SRVPID) ===" >> $RESU/LP.txt
    # THEIR ASK: let the freeze sit ~10s so recording continues THROUGH it
    echo "  freeze detected; sleeping 12s with the tail still recording..." >> $RESU/LP.txt
    sleep 12
    sudo timeout 45 gdb -p $SRVPID -batch -ex "source /tmp/xtc-gdb.py" -ex "xtc-tail-dump /tmp/lp.xtcl" >/dev/null 2>&1
    python3 /mnt/work/xtc/tools/xtc-tail.py /tmp/lp.xtcl > /tmp/lp_tail.txt 2>&1
    echo "  tail events=$(wc -l </tmp/lp_tail.txt)" >> $RESU/LP.txt
    echo "  kind histogram:" >> $RESU/LP.txt
    grep -oE "SCHED (PARK|RUN|SPAWN|EXIT|LOOP_POLL)|MSG +[A-Z_]+" /tmp/lp_tail.txt | sort | uniq -c | sed 's/^/    /' >> $RESU/LP.txt
    python3 - <<'PY' >> $RESU/LP.txt 2>&1
import re, collections
lines=open('/tmp/lp_tail.txt').read().splitlines()
park=collections.Counter(); run=collections.Counter(); aio=collections.Counter()
idx_last_park={}
for i,ln in enumerate(lines):
    m=re.search(r'pid=(\d+)\.(\d+)\.(\d+)', ln)
    if not m: continue
    pid=f"{m.group(1)}.{m.group(2)}.{m.group(3)}"
    if 'SCHED PARK' in ln:
        park[pid]+=1; idx_last_park[pid]=i
        if 'detail=' in ln: aio[pid]+=1
    elif 'SCHED RUN' in ln: run[pid]+=1
cands=[(p,park[p],run[p],aio[p]) for p in park if park[p]>run[p] and aio[p]>0]
print("  STRANDED CANDIDATES (deficit>0 and aio_park>0):")
for p,k,r,a in cands: print(f"    {p}: aio_parks={a} park={k} run={r} deficit={k-r} last_park_line={idx_last_park[p]}")
if not cands:
    print("    (none this run)")
else:
    p=cands[0][0]; loop=p.split('.')[0]; L=idx_last_park[p]
    after=lines[L+1:]
    print(f"  === THE ANSWER: events after {p}'s final PARK (line {L} of {len(lines)}) = {len(after)} ===")
    lp=collections.Counter()
    for ln in after:
        if 'LOOP_POLL' in ln:
            m=re.search(r'pid=(\d+)', ln)
            if m: lp[m.group(1)]+=1
    print(f"    LOOP_POLL events after, for the STRANDED fiber's loop {loop}: {lp.get(loop,0)}")
    tot=sum(lp.values()); print(f"    LOOP_POLL events after, all loops: {tot} across {len(lp)} loops")
    top=sorted(lp.items(), key=lambda x:-x[1])[:8]
    print(f"    busiest loops after: {top}")
    if lp.get(loop,0) > 0:
        print("    => VERDICT: loop kept POLLING after the park -> the fiber was SKIPPED (lost wake)")
    elif tot > 0:
        print("    => VERDICT: other loops polled but loop %s did NOT -> loop %s STOPPED (dead/blocked worker)" % (loop,loop))
    else:
        print("    => VERDICT: NO loop polled after the park -> whole runtime quiesced (inconclusive/global)")
PY
    sudo pkill -9 -f "pgbench -p $port" 2>/dev/null; $PGB/pg_ctl -D $D -m immediate -w stop >/dev/null 2>&1
    break
  fi
  sudo pkill -9 -f "pgbench -p $port" 2>/dev/null; $PGB/pg_ctl -D $D -m immediate -w stop >/dev/null 2>&1; sleep 1
done
echo "LP_COMPLETE" >> $RESU/LP.txt
