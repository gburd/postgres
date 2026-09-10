#!/bin/bash
# THE DECISIVE TEST (libxtc c67cf33): join PARK_TASK -> WAKE by task pointer.
#   no WAKE for T                  => completion never reached dispatch (reap/handoff)
#   WAKE for T but no later RUN     => dispatch ran, loss is downstream (waker CAS/enqueue)
# Also: xtc-tail-dropped, so any ABSENCE claim is falsifiable.
set -u
PGB=/mnt/work/inst/usr/local/pgsql/bin
export LD_LIBRARY_PATH=/mnt/work/inst/usr/local/pgsql/lib64:/usr/local/lib64
export PGHOST=127.0.0.1 PGUSER=postgres PGDATABASE=postgres
BASE=/mnt/nvme; RESU=/mnt/work/out; SB=$(( $(free -m|awk '/Mem:/{print $2}')*40/100 ))MB
: > $RESU/JN.txt
HANGS=0
for n in $(seq 1 10); do
  [ $HANGS -ge 2 ] && break
  port=$((7820+n)); D=$BASE/jn$n
  sudo pkill -9 -f "postgres -D /mnt/nvme" 2>/dev/null; sudo fuser -k ${port}/tcp >/dev/null 2>&1; sleep 2
  find $D -mindepth 1 -delete 2>/dev/null; rmdir $D 2>/dev/null
  $PGB/initdb -D $D -U postgres >/dev/null 2>&1
  echo "host all all 127.0.0.1/32 trust" >> $D/pg_hba.conf
  PG_XTC_TAIL=1 $PGB/postgres -D $D -c port=$port -c listen_addresses=127.0.0.1 -c shared_buffers=$SB \
    -c huge_pages=off -c multithreaded=on -c pooled_protocol_carriers=0 -c fsync=on \
    -c synchronous_commit=on -c full_page_writes=on -c autovacuum=on -c max_connections=200 \
    >$RESU/jn${n}_srv.log 2>&1 &
  SRV=$!
  for i in $(seq 1 50); do grep -q "ready to accept" $RESU/jn${n}_srv.log 2>/dev/null && break; sleep 1; done
  timeout 150 $PGB/pgbench -p $port -i -s 50 >/dev/null 2>&1
  out=$(timeout 60 $PGB/pgbench -p $port -c 32 -j 8 -T 8 2>&1)
  if echo "$out" | grep -q "tps = "; then echo "run $n: PASS $(echo "$out"|grep -oE 'tps = [0-9.]+'|head -1)" >> $RESU/JN.txt; else
    HANGS=$((HANGS+1)); echo "=== HANG #$HANGS (run $n) ===" >> $RESU/JN.txt
    sleep 12
    sudo timeout 90 gdb -p $SRV -batch -ex "source /tmp/xtc-gdb.py" \
      -ex "xtc-tail-dropped" -ex "xtc-tail-dump /tmp/jn$n.xtcl" -ex "xtc-rings" >/tmp/jn${n}_gdb.txt 2>&1
    echo "--- xtc-tail-dropped (falsifiability gate) ---" >> $RESU/JN.txt
    grep -iE "emitted=|dropped|WARNING|absence" /tmp/jn${n}_gdb.txt | head -4 | sed 's/^/  /' >> $RESU/JN.txt
    python3 /mnt/work/xtc/tools/xtc-tail.py /tmp/jn$n.xtcl > /tmp/jn${n}_tail.txt 2>&1
    echo "--- kinds ---" >> $RESU/JN.txt
    grep -oE "SCHED [A-Z_]+" /tmp/jn${n}_tail.txt | sort | uniq -c | sed 's/^/  /' >> $RESU/JN.txt
    python3 - "$n" <<'PY' >> $RESU/JN.txt 2>&1
import re,sys,collections
n=sys.argv[1]; lines=open("/tmp/jn%s_tail.txt"%n).read().splitlines()
def pid(l):
    m=re.search(r"pid=(\d+)\.(\d+)\.(\d+)",l); return "%s.%s.%s"%m.groups() if m else None
def det(l):
    m=re.search(r"(?:task|detail|fd/op)=(0x[0-9a-f]+|\d+)",l)
    if not m: return None
    v=m.group(1); return int(v,16) if v.startswith("0x") else int(v)
park=collections.Counter(); run=collections.Counter(); lastpark={}; task_of={}
wake_at=collections.defaultdict(list)
pending_task={}
for i,l in enumerate(lines):
    p=pid(l)
    if "PARK_TASK" in l and p:
        pending_task[p]=det(l)
    elif "SCHED PARK" in l and p:
        park[p]+=1; lastpark[p]=i
        if p in pending_task: task_of[(p,i)]=pending_task[p]
    elif "SCHED RUN" in l and p:
        run[p]+=1
    elif "SCHED WAKE" in l:
        t=det(l)
        if t is not None: wake_at[t].append(i)
strands=[(p,park[p],run[p],lastpark[p]) for p in park if park[p]>run[p]]
real=[(p,k,r,i) for p,k,r,i in strands if (p,i) in task_of]
print("  pids with PARK>RUN: %d ; of those, %d have a joinable PARK_TASK" % (len(strands),len(real)))
print()
no_wake=0; wake_no_run=0
print("  %-10s %-18s %-8s %s" % ("pid","task","WAKEs_after","verdict"))
for p,k,r,i in sorted(real, key=lambda x:-x[3])[:12]:
    T=task_of[(p,i)]
    after=[j for j in wake_at.get(T,[]) if j>i]
    if after:
        # did the pid RUN after that wake?
        ran=any("SCHED RUN" in lines[j2] and pid(lines[j2])==p for j2 in range(after[0],len(lines)))
        v="WAKE then %s" % ("RAN(ok)" if ran else "NO RUN => waker CAS/enqueue")
        wake_no_run += (0 if ran else 1)
    else:
        v="NO WAKE => never reached dispatch"; no_wake+=1
    print("  %-10s %-18s %-8d %s" % (p,hex(T),len(after),v))
print()
print("  TALLY: no-WAKE=%d   WAKE-but-no-RUN=%d" % (no_wake,wake_no_run))
if no_wake and not wake_no_run: print("  VERDICT: completion never reached dispatch -> reap / event-array / handoff")
elif wake_no_run and not no_wake: print("  VERDICT: dispatch ran, loss downstream -> xtc_waker_wake CAS / enqueue")
elif no_wake and wake_no_run: print("  VERDICT: BOTH shapes present -> likely two distinct defects")
PY
    sudo pkill -9 -f "pgbench -p $port" 2>/dev/null; $PGB/pg_ctl -D $D -m immediate -w stop >/dev/null 2>&1
  fi
  sudo pkill -9 -f "pgbench -p $port" 2>/dev/null; $PGB/pg_ctl -D $D -m immediate -w stop >/dev/null 2>&1; sleep 1
done
echo "JN_COMPLETE hangs=$HANGS" >> $RESU/JN.txt
