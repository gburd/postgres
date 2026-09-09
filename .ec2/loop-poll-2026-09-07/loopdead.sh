#!/bin/bash
# Confirm branch 2 across MULTIPLE hangs + capture the stranded loop's worker wait state.
set -u
PGB=/mnt/work/inst/usr/local/pgsql/bin
export LD_LIBRARY_PATH=/mnt/work/inst/usr/local/pgsql/lib64:/usr/local/lib64
export PGHOST=127.0.0.1 PGUSER=postgres PGDATABASE=postgres
BASE=/mnt/nvme; RESU=/mnt/work/out
SB=$(( $(free -m|awk '/Mem:/{print $2}') * 40 / 100 ))MB
: > $RESU/LD.txt
HANGS=0
for n in $(seq 1 10); do
  [ $HANGS -ge 3 ] && break
  port=$((7700+n)); D=$BASE/ld$n
  sudo pkill -9 -f "postgres -D /mnt/nvme" 2>/dev/null; sudo fuser -k ${port}/tcp >/dev/null 2>&1; sleep 2
  find $D -mindepth 1 -delete 2>/dev/null; rmdir $D 2>/dev/null
  $PGB/initdb -D $D -U postgres >/dev/null 2>&1
  echo "host all all 127.0.0.1/32 trust" >> $D/pg_hba.conf
  PG_XTC_TAIL=1 $PGB/postgres -D $D -c port=$port -c listen_addresses=127.0.0.1 -c shared_buffers=$SB -c huge_pages=off \
    -c multithreaded=on -c pooled_protocol_carriers=0 -c fsync=on -c synchronous_commit=on \
    -c full_page_writes=on -c autovacuum=on -c max_connections=200 >$RESU/ld${n}_srv.log 2>&1 &
  SRVPID=$!
  for i in $(seq 1 50); do grep -q "ready to accept" $RESU/ld${n}_srv.log 2>/dev/null && break; sleep 1; done
  timeout 120 $PGB/pgbench -p $port -i -s 50 >/dev/null 2>&1
  out=$(timeout 60 $PGB/pgbench -p $port -c 32 -j 8 -T 8 2>&1)
  if echo "$out" | grep -q "tps = "; then echo "run $n: PASS" >> $RESU/LD.txt; else
    HANGS=$((HANGS+1))
    echo "=== HANG #$HANGS (run $n) ===" >> $RESU/LD.txt
    sleep 12                      # keep recording THROUGH the freeze (their ask)
    sudo timeout 60 gdb -p $SRVPID -batch -ex "source /tmp/xtc-gdb.py" \
        -ex "xtc-tail-dump /tmp/ld$n.xtcl" -ex "xtc-rings" >/tmp/ld${n}_gdb.txt 2>&1
    python3 /mnt/work/xtc/tools/xtc-tail.py /tmp/ld$n.xtcl > /tmp/ld${n}_tail.txt 2>&1
    STRLOOP=$(python3 - "$n" <<'PY'
import re,sys,collections
lines=open("/tmp/ld%s_tail.txt"%sys.argv[1]).read().splitlines()
park=collections.Counter();run=collections.Counter();aio=collections.Counter();lp={}
for i,ln in enumerate(lines):
    m=re.search(r"pid=(\d+)\.(\d+)\.(\d+)",ln)
    if not m: continue
    p="%s.%s.%s"%m.groups()
    if "SCHED PARK" in ln:
        park[p]+=1; lp[p]=i
        if "detail=" in ln: aio[p]+=1
    elif "SCHED RUN" in ln: run[p]+=1
c=[(p,aio[p],park[p]-run[p],lp[p]) for p in park if park[p]>run[p] and aio[p]>0]
if c: print("%s %d %d %d"%(c[0][0],c[0][1],c[0][2],c[0][3]))
else: print("NONE 0 0 0")
PY
)
    set -- $STRLOOP; SP=$1; SA=$2; SD=$3; SL=$4
    echo "  stranded fiber=$SP aio_parks=$SA deficit=$SD" >> $RESU/LD.txt
    if [ "$SP" != NONE ]; then
      LOOP=${SP%%.*}
      python3 - "$n" "$LOOP" "$SL" <<'PY' >> $RESU/LD.txt
import re,sys,collections
n,loop,L=sys.argv[1],sys.argv[2],int(sys.argv[3])
lines=open("/tmp/ld%s_tail.txt"%n).read().splitlines()
pts=int(re.match(r"\s*(\d+)",lines[L]).group(1))
polls=collections.defaultdict(list)
for ln in lines:
    if "?8" in ln or "LOOP_POLL" in ln:
        m=re.search(r"pid=(\d+)\.",ln); t=re.match(r"\s*(\d+)",ln)
        if m and t: polls[m.group(1)].append(int(t.group(1)))
mine=polls.get(loop,[])
after={l:len([t for t in v if t>pts+10**9]) for l,v in polls.items()}
alive=[(l,c) for l,c in after.items() if c>0]
print("  stranded loop=%s: %d polls total, last %+.3f ms vs park, polls >1s after = %d"
      % (loop, len(mine), ((max(mine)-pts)/1e6 if mine else 0), after.get(loop,0)))
print("  OTHER loops still polling >1s after the park: %d loops, %d polls (max %+.1f s)"
      % (len(alive), sum(c for _,c in alive), (max(max(v) for v in polls.values())-pts)/1e9))
print("  VERDICT: %s" % ("LOOP STOPPED (branch 2: worker blocked in wait_cqe)" if after.get(loop,0)==0 and len(alive)>0 else ("LOST WAKE (branch 1: loop alive, task skipped)" if after.get(loop,0)>0 else "inconclusive")))
PY
      echo "  --- rings: unreaped for any ring (3 of them) ---" >> $RESU/LD.txt
      grep -iE "unreaped|ring" /tmp/ld${n}_gdb.txt | head -4 | sed 's/^/    /' >> $RESU/LD.txt
    fi
    sudo pkill -9 -f "pgbench -p $port" 2>/dev/null; $PGB/pg_ctl -D $D -m immediate -w stop >/dev/null 2>&1
  fi
  sudo pkill -9 -f "pgbench -p $port" 2>/dev/null; $PGB/pg_ctl -D $D -m immediate -w stop >/dev/null 2>&1; sleep 1
done
echo "LD_COMPLETE hangs=$HANGS" >> $RESU/LD.txt
