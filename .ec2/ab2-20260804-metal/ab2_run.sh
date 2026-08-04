#!/usr/bin/env bash
# Self-contained metal A/B: stock(fork) vs mt(pooled). Runs ON the box, detached.
# HammerDB local (single box -- the part that worked last time). All the fixed
# start/stop/pooled-assert logic is inlined here so there is no driver box to leak.
set -uo pipefail
LXTC_REV=563329f9487739ce33709b5fc210ba89bde03b87   # v1.32.0
STAGE=/mnt/work
PGSRC=$STAGE/pgsrc
PGBIN=$STAGE/inst/usr/local/pgsql/bin
DATA=$STAGE/data
OUT=$STAGE/out
PORT=5439
SHBUF=32GB
WAREHOUSES=100
RAMPUP=2
DURATION=5
CARRIERS=192
VU_LIST="192 384"
HAMMER=/opt/HammerDB-5.0
mkdir -p "$OUT"
# HammerDB's Pgtcl dlopens libpq.so.5 at runtime; our PG installs it under
# inst/.../pgsql/lib, which is NOT on the default loader path -> "cannot open
# libpq.so.5" and every VU fails.  Put our libpq dir on LD_LIBRARY_PATH for
# every hammerdbcli invocation.
PGROOT=$(dirname "$PGBIN")
export LD_LIBRARY_PATH="$PGROOT/lib64:$PGROOT/lib:/usr/local/lib:${LD_LIBRARY_PATH:-}"
log(){ echo "[$(date +%H:%M:%S)] $*" | tee -a "$OUT/run.log"; }
say(){ echo "$*" >> "$OUT/STATUS"; }

# ---------------- phase gate so a poll can see where we are ----------------
say "START $(date)"

# ---------------- OS: RAID-0 the instance NVMe, tune, hugepages ------------
log "OS tune"
mapfile -t NVMES < <(lsblk -dpno NAME,MODEL | awk '/Instance Storage|EC2 NVMe Instance/{print $1}')
if [ "${#NVMES[@]}" -ge 2 ] && [ ! -d "$STAGE/.mounted" ]; then
  sudo mdadm --create /dev/md0 --level=0 --raid-devices=${#NVMES[@]} "${NVMES[@]}" --run >/dev/null 2>&1
  sudo mkfs.xfs -f /dev/md0 >/dev/null 2>&1
  sudo mkdir -p $STAGE && sudo mount /dev/md0 $STAGE && sudo chown -R ec2-user:ec2-user $STAGE
  mkdir -p "$OUT"; touch "$STAGE/.mounted"
fi
mkdir -p "$OUT"
# hugepages for 32GB shared_buffers + overhead: ~17000 * 2MB
sudo sysctl -w vm.nr_hugepages=18000 >/dev/null 2>&1
echo never | sudo tee /sys/kernel/mm/transparent_hugepage/enabled >/dev/null 2>&1
for c in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do echo performance | sudo tee $c >/dev/null 2>&1; done
sudo sysctl -w kernel.perf_event_paranoid=1 kernel.kptr_restrict=0 >/dev/null 2>&1
sudo prlimit --pid $$ --stack=67108864:67108864 2>/dev/null || true
say "OS_TUNED nvme=${#NVMES[@]} steal=$(awk '/^cpu /{print $9}' /proc/stat)"

# ---------------- build libxtc (autotools) --------------------------------
if [ -f /usr/local/lib/libxtc.so.1.32.0 ] && [ -x "$PGBIN/postgres" ]; then
  log "skip build -- libxtc + PG already present"
  say "BUILD_SKIP libxtc+PG already built"
else
log "build libxtc $LXTC_REV"
cd $STAGE
tar xzf /tmp/libxtc.tar.gz -C $STAGE 2>/dev/null
cd $STAGE/libxtc
( cd dist && autoreconf -i >/dev/null 2>&1 )
mkdir -p build_unix && cd build_unix
../dist/configure --with-io-backend=uring --enable-shared >$OUT/lxtc_conf.log 2>&1
make -j"$(nproc)" >$OUT/lxtc_make.log 2>&1
sudo make install >$OUT/lxtc_install.log 2>&1
echo /usr/local/lib | sudo tee /etc/ld.so.conf.d/usrlocal.conf >/dev/null
sudo ldconfig
LXTC_OK=$(ldconfig -p | grep -c 'libxtc.so.1 ')
say "LIBXTC installed p=$LXTC_OK ver=$(ls /usr/local/lib/libxtc.so.1.* 2>/dev/null)"
if [ "$LXTC_OK" -lt 1 ]; then say "LIBXTC_BUILD_FAIL -- see lxtc_*.log"; log "libxtc build failed"; exit 1; fi

# ---------------- build PG (meson, release) -------------------------------
log "build PG"
cd $STAGE
tar xzf /tmp/pgxtc.tar.gz -C $PGSRC --strip-components=0 2>/dev/null || { mkdir -p $PGSRC && tar xzf /tmp/pgxtc.tar.gz -C $PGSRC; }
cd $PGSRC
export PKG_CONFIG_PATH=/usr/local/lib/pkgconfig
meson setup build --prefix=$STAGE/inst/usr/local/pgsql \
  -Dxtc=enabled -Dliburing=enabled -Dcassert=false -Dbuildtype=release \
  >$OUT/pg_setup.log 2>&1
ninja -C build >$OUT/pg_build.log 2>&1
ninja -C build install >$OUT/pg_install.log 2>&1
ldd "$PGBIN/postgres" | grep xtc > $OUT/ldd_xtc.txt
say "PG built xtc_link=$(cat $OUT/ldd_xtc.txt)"
if [ ! -x "$PGBIN/postgres" ]; then say "PG_BUILD_FAIL -- see pg_setup.log/pg_build.log"; log "PG build failed"; exit 1; fi
if ! grep -q xtc $OUT/ldd_xtc.txt; then say "PG_XTC_LINK_FAIL -- postgres not linked to libxtc"; exit 1; fi
fi

# ---------------- the fixed start/stop/assert helpers ---------------------
write_conf(){ # $1 mode $2 carriers
  cat > "$DATA/postgresql.auto.conf" <<CONF
port = $PORT
listen_addresses = '*'
shared_buffers = $SHBUF
max_connections = 1024
max_wal_size = 64GB
checkpoint_timeout = 30min
checkpoint_completion_target = 0.9
wal_buffers = 64MB
huge_pages = on
fsync = off
synchronous_commit = off
full_page_writes = off
CONF
  # Each lane on its native-best IO path: fork uses sync (io_method=worker needs
  # forked IO subprocesses, unavailable under multithreaded=on), mt uses xtc --
  # the fiber-native method (parks the fiber on the xtc loop, does not block the
  # carrier).  Overridable via IOM_THREADED for an mt-sync-vs-mt-xtc A/B.
  if [ "$1" = threaded ]; then
    echo "io_method = ${IOM_THREADED:-xtc}" >> "$DATA/postgresql.auto.conf"
    echo "multithreaded = on" >> "$DATA/postgresql.auto.conf"
    echo "pooled_protocol_carriers = $2" >> "$DATA/postgresql.auto.conf"
  else
    echo "io_method = sync" >> "$DATA/postgresql.auto.conf"
  fi
}
start_pg(){ # $1 mode $2 carriers
  local mode="$1" carriers="${2:-}"
  local oldpid; oldpid=$(head -1 "$DATA/postmaster.pid" 2>/dev/null)
  [ -n "$oldpid" ] && kill -0 "$oldpid" 2>/dev/null && { kill -9 "$oldpid" 2>/dev/null; sleep 3; }
  pkill -9 -f "postgres -D $DATA" 2>/dev/null; sleep 1; rm -f "$DATA/postmaster.pid"
  write_conf "$mode" "$carriers"
  "$PGBIN/postgres" -D "$DATA" >"$OUT/pg_${mode}.log" 2>&1 &
  echo $! > "$OUT/pg.pid"
  local i; for i in $(seq 1 180); do grep -q "ready to accept connections" "$OUT/pg_${mode}.log" 2>/dev/null && break; sleep 1; done
  grep -q "ready to accept connections" "$OUT/pg_${mode}.log" || { echo "NOT_READY $mode"; tail -20 "$OUT/pg_${mode}.log"; return 1; }
  if [ "$mode" = threaded ]; then
    local streak=0 j
    for j in $(seq 1 120); do
      if "$PGBIN/psql" -h 127.0.0.1 -p $PORT -U postgres -tAc 'select 1' postgres >/dev/null 2>&1; then streak=$((streak+1)); [ $streak -ge 5 ] && break; else streak=0; fi; sleep 1
    done
    [ $streak -ge 5 ] || { echo "TCP_UNSTABLE"; tail -20 "$OUT/pg_${mode}.log"; return 1; }
    sleep 2
    local eff rows
    eff=$("$PGBIN/psql" -h 127.0.0.1 -p $PORT -U postgres -tAc 'show pooled_protocol_carriers' postgres 2>/dev/null | tr -d ' ')
    rows=$("$PGBIN/psql" -h 127.0.0.1 -p $PORT -U postgres -tAc 'select count(*) from pg_stat_xtc_carriers' postgres 2>/dev/null | tr -d ' ')
    if [ -z "$eff" ] || [ "$eff" = 0 ] || [ -z "$rows" ] || [ "$rows" = 0 ]; then
      echo "POOLED_NOT_IN_EFFECT show=$eff rows=$rows"; tail -20 "$OUT/pg_${mode}.log"; return 1
    fi
    say "POOLED_OK carriers=$eff rows=$rows"
  fi
  "$PGBIN/psql" -h 127.0.0.1 -p $PORT -U postgres -tAc 'select 1' postgres >/dev/null 2>&1
}
stop_pg(){
  "$PGBIN/psql" -h 127.0.0.1 -p $PORT -U postgres -c CHECKPOINT postgres >/dev/null 2>&1
  "$PGBIN/pg_ctl" -D "$DATA" -m fast -w -t 600 stop >/dev/null 2>&1
  local pid; pid=$(cat "$OUT/pg.pid" 2>/dev/null); local i
  for i in $(seq 1 60); do kill -0 "$pid" 2>/dev/null || break; sleep 1; done
  if kill -0 "$pid" 2>/dev/null; then
    say "FAST_STOP_HUNG pid=$pid -> immediate"
    "$PGBIN/pg_ctl" -D "$DATA" -m immediate -w -t 120 stop >/dev/null 2>&1
    for i in $(seq 1 30); do kill -0 "$pid" 2>/dev/null || break; sleep 1; done
  fi
  kill -0 "$pid" 2>/dev/null && { kill -9 "$pid" 2>/dev/null; sleep 2; }
  pkill -9 -f "postgres -D $DATA" 2>/dev/null; sleep 1; rm -f "$DATA/postmaster.pid"
  for i in $(seq 1 30); do "$PGBIN/psql" -h 127.0.0.1 -p $PORT -U postgres -tAc 'select 1' postgres >/dev/null 2>&1 || break; sleep 1; done
}

# ---------------- initdb + load schema (once, process mode) ---------------
log "initdb + load"
find "$DATA" -mindepth 1 -delete 2>/dev/null; rmdir "$DATA" 2>/dev/null
"$PGBIN/initdb" -D "$DATA" -U postgres -E UTF8 >$OUT/initdb.log 2>&1
echo "host all all 127.0.0.1/32 trust" >> "$DATA/pg_hba.conf"
start_pg process "" || { say "SCHEMA_START_FAIL"; exit 1; }
"$PGBIN/psql" -h 127.0.0.1 -p $PORT -U postgres -c "create role tpcc superuser login password 'tpcc'" postgres >/dev/null 2>&1
cat > $OUT/build.tcl <<TCL
dbset db pg
dbset bm TPROC-C
diset connection pg_host 127.0.0.1
diset connection pg_port $PORT
diset tpcc pg_superuser postgres
diset tpcc pg_superuserpass postgres
diset tpcc pg_user tpcc
diset tpcc pg_pass tpcc
diset tpcc pg_dbase tpcc
diset tpcc pg_count_ware $WAREHOUSES
diset tpcc pg_num_vu 16
diset tpcc pg_storedprocs true
buildschema
waittocomplete
quit
TCL
( cd $HAMMER && ./hammerdbcli auto $OUT/build.tcl > $OUT/hdb_build.log 2>&1 )
# gate: buildschema must have created the tpcc db
if ! "$PGBIN/psql" -h 127.0.0.1 -p $PORT -U postgres -tAc "select 1 from pg_database where datname='tpcc'" postgres 2>/dev/null | grep -q 1; then
  say "SCHEMA_BUILD_FAIL -- no tpcc db; see hdb_build.log"; log "schema build failed"; stop_pg; exit 1
fi
WH=$("$PGBIN/psql" -h 127.0.0.1 -p $PORT -U postgres -tAc 'select count(*) from warehouse' tpcc 2>/dev/null | tr -d ' ')
if [ -z "$WH" ] || [ "$WH" -lt "$WAREHOUSES" ]; then
  say "SCHEMA_INCOMPLETE warehouses=$WH expected=$WAREHOUSES"; log "schema incomplete"; stop_pg; exit 1
fi
"$PGBIN/psql" -h 127.0.0.1 -p $PORT -U postgres -c "VACUUM ANALYZE" tpcc >/dev/null 2>&1
"$PGBIN/psql" -h 127.0.0.1 -p $PORT -U postgres -c CHECKPOINT postgres >/dev/null 2>&1
say "SCHEMA built tpcc=$("$PGBIN/psql" -h 127.0.0.1 -p $PORT -U postgres -tAc "select pg_size_pretty(pg_database_size('tpcc'))" postgres 2>/dev/null)"
stop_pg

# ---------------- the A/B matrix ------------------------------------------
RES=$OUT/hresults.tsv
echo -e "mode\tcarriers\tvu\trep\tnopm\ttpm\tsrv_tpm" > $RES
run_cell(){ # $1 mode $2 carriers $3 vu $4 rep
  local mode="$1" c="$2" vu="$3" rep="$4"
  local tag="${mode}_c${c}_vu${vu}_r${rep}"
  cat > $OUT/run_$tag.tcl <<TCL
dbset db pg
dbset bm TPROC-C
diset connection pg_host 127.0.0.1
diset connection pg_port $PORT
diset tpcc pg_superuser postgres
diset tpcc pg_superuserpass postgres
diset tpcc pg_user tpcc
diset tpcc pg_pass tpcc
diset tpcc pg_dbase tpcc
diset tpcc pg_storedprocs true
diset tpcc pg_driver timed
diset tpcc pg_rampup $RAMPUP
diset tpcc pg_duration $DURATION
diset tpcc pg_allwarehouse true
vuset vu $vu
vuset logtotemp 1
loadscript
vucreate
vurun
runtimer 900
vudestroy
quit
TCL
  timeout 1500 bash -c "cd $HAMMER && ./hammerdbcli auto $OUT/run_$tag.tcl" > $OUT/hdb_$tag.log 2>&1 &
  local hpid=$!
  # Monitor-independent throughput: HammerDB's monitor VU (Vuser 1) can fail on
  # the pooled path at the rampup->timing transition (server clean, workers all
  # SUCCESS, but no TEST RESULT line -> NOPM=NA).  So sample committed txns from
  # pg_stat_database ourselves over the measured window -- same definition both
  # lanes, needs no HammerDB monitor.  Wait past rampup, snapshot, wait the
  # duration, snapshot again.
  local xq="select sum(xact_commit) from pg_stat_database where datname='tpcc'"
  sleep $(( RAMPUP*60 + 20 ))
  local x0 t0 x1 t1
  x0=$("$PGBIN/psql" -h 127.0.0.1 -p $PORT -U postgres -tAc "$xq" postgres 2>/dev/null | tr -d ' '); t0=$(date +%s)
  sleep $(( DURATION*60 ))
  x1=$("$PGBIN/psql" -h 127.0.0.1 -p $PORT -U postgres -tAc "$xq" postgres 2>/dev/null | tr -d ' '); t1=$(date +%s)
  wait $hpid 2>/dev/null
  local tpm_srv=NA
  if [ -n "$x0" ] && [ -n "$x1" ] && [ "$t1" -gt "$t0" ]; then
    tpm_srv=$(awk -v a=$x0 -v b=$x1 -v s=$t0 -v e=$t1 'BEGIN{printf "%d", (b-a)*60/(e-s)}')
  fi
  local nopm tpm
  nopm=$(grep -oE 'System achieved [0-9]+ NOPM' $OUT/hdb_$tag.log | grep -oE '[0-9]+' | tail -1)
  tpm=$(grep -oE 'NOPM from [0-9]+ PostgreSQL TPM' $OUT/hdb_$tag.log | grep -oE '[0-9]+' | tail -1)
  echo -e "${mode}\t${c}\t${vu}\t${rep}\t${nopm:-NA}\t${tpm:-NA}\t${tpm_srv}" | tee -a $RES
  say "CELL $tag nopm=${nopm:-NA} srv_tpm=${tpm_srv}"
}

for vu in $VU_LIST; do
  for rep in 1 2; do
    start_pg process "" && run_cell process 0 $vu $rep; stop_pg
    start_pg threaded $CARRIERS && run_cell threaded $CARRIERS $vu $rep || say "THREADED_CELL_SKIP vu=$vu rep=$rep"; stop_pg
  done
done

# ---------------- thread count + a select@384 perf snapshot ---------------
log "thread-count + perf on mt select@384"
start_pg threaded $CARRIERS
PM=$(cat $OUT/pg.pid)
say "THREADS postmaster=$(ls /proc/$PM/task 2>/dev/null | wc -l) iouwrk=$(ps -eLf | grep -c iou-wrk)"
# quick pgbench select@384 with perf for update_sg_lb_stats + LWLock
"$PGBIN/pgbench" -h 127.0.0.1 -p $PORT -U postgres -i -s 300 postgres >$OUT/pgb_init.log 2>&1
( sudo perf record -F 199 -a -g -o $OUT/perf_sel384.data -- sleep 40 ) &
"$PGBIN/pgbench" -h 127.0.0.1 -p $PORT -U postgres -S -c 384 -j 32 -T 45 postgres >$OUT/pgb_sel384.log 2>&1
wait
sudo perf report -i $OUT/perf_sel384.data --stdio 2>/dev/null | head -60 > $OUT/perf_sel384_top.txt
say "PGB_SEL384 $(grep -E 'tps =' $OUT/pgb_sel384.log | tail -1)"
say "SGLB $(grep -m1 update_sg_lb_stats $OUT/perf_sel384_top.txt)"
say "LWLOCK $(grep -m3 -iE 'LWLock' $OUT/perf_sel384_top.txt | tr '\n' ' ')"
stop_pg

echo "=== RESULTS ===" | tee -a $OUT/run.log
column -t $RES | tee -a $OUT/run.log
say "DONE $(date)"
