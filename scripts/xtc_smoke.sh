#!/usr/bin/env bash
# xtc-carrier smoke tests -- run on a DISK-BACKED host (meh/nuc/EC2), never on
# tmpfs.  Exercises the loop pool: concurrency, LISTEN/NOTIFY cross-fiber
# wakeup, ereport(ERROR) unwind in a fiber, and clean shutdown.
#
# Usage: xtc_smoke.sh <install-prefix> [scratch-parent-dir]
#   install-prefix = dir containing bin/ (e.g. .../tmp_install/usr/local/pgsql)
#   scratch-parent = disk-backed dir for PGDATA (default: ~/xtc-scratch)
set -u

PREFIX="${1:?install prefix required}"
SCRATCH="${2:-$HOME/xtc-scratch}"
mkdir -p "$SCRATCH"
D=$(mktemp -d "$SCRATCH/xtcXXXXXX")
export PGDATA="$D/pgdata"
export LC_ALL=C LC_CTYPE=C LANG=C
export LD_LIBRARY_PATH="$PREFIX/lib:${LD_LIBRARY_PATH:-}"
export PATH="$PREFIX/bin:$PATH"

fail=0
note() { echo "== $*"; }
ok()   { echo "PASS: $*"; }
bad()  { echo "FAIL: $*"; fail=1; }

initdb -D "$PGDATA" -U postgres --no-locale -E UTF8 >"$D/initdb.log" 2>&1 \
  || { echo "initdb failed"; cat "$D/initdb.log"; exit 1; }
{
  echo "listen_addresses = ''"
  echo "unix_socket_directories = '$D'"
  echo "autovacuum = off"
  echo "logging_collector = off"
} >> "$PGDATA/postgresql.conf"

pg_ctl -D "$PGDATA" -l "$D/pm.log" -o "-c multithreaded=on" -w start \
  || { echo "start failed"; cat "$D/pm.log"; exit 1; }

PSQL="psql -X -h $D -U postgres -d postgres -tA"

# 1. basic round-trip through the xtc carrier (this also triggers the lazy
#    carrier/pool start, so the pool-size check below has a log line to read)
note "select 1"
[ "$($PSQL -c 'select 1')" = "1" ] && ok "select 1" || bad "select 1"

# 1b. carrier pool is multi-loop and sized to the core count.  The whole point
#     of the xtc carrier is a pool matching how it will be used; a regression
#     to a single loop would hide concurrency/wakeup bugs (and shrink DST
#     coverage).  Checked after the first query so the carrier has started.
note "carrier pool sized to cores"
ncpu=$(nproc 2>/dev/null || getconf _NPROCESSORS_ONLN 2>/dev/null || echo 1)
loops=$(grep -aoE "scheduler thread up \(([0-9]+) loops" "$D/pm.log" | grep -oE "[0-9]+" | head -1)
if [ -n "$loops" ] && [ "$loops" = "$ncpu" ]; then
  ok "pool = $loops loops (== $ncpu cores)"
elif [ -n "$loops" ] && [ "$loops" -gt 1 ]; then
  ok "pool = $loops loops (multi-loop; cores=$ncpu)"
else
  bad "pool not multi-loop (loops='$loops', cores=$ncpu) -- regressed to single loop?"
fi

# 2. N concurrent backends, each holds the socket, then a wedge check
note "6 concurrent backends"
for n in 1 2 3 4 5 6; do
  ( $PSQL -c "select pg_sleep(0.4); select $n" >/dev/null 2>&1 ) &
done
wait
r=$(timeout 10 $PSQL -c 'select 42' 2>/dev/null)
[ "$r" = "42" ] && ok "loop not wedged after concurrency" || bad "loop wedged"

# 2b. Concurrent GUC SET/RESET across fibers must not wedge a carrier loop.
#     Each SET/RESET takes the process-wide threaded-GUC critical section
#     (ThreadedGUCLock); a bug in its lock/unlock accounting -- e.g. an
#     unbalanced RESUME_INTERRUPTS on the multithreaded-flag straddle, or a
#     truly loop-blocking hold across a fiber yield -- wedges the loop under
#     contention.  Regression gate for the #5 GUC-lock hazard.
note "concurrent GUC SET/RESET (no wedge)"
for n in 1 2 3 4 5 6 7 8; do
  ( timeout 20 $PSQL -c "DO \$\$ BEGIN FOR k IN 1..300 LOOP PERFORM set_config('work_mem',(4096+k)::text||'kB',false); RESET work_mem; END LOOP; END \$\$; SELECT $n" >/dev/null 2>&1 ) &
done
wait
r=$(timeout 10 $PSQL -c 'select 43' 2>/dev/null)
[ "$r" = "43" ] && ok "loop not wedged after concurrent SET/RESET" || bad "loop wedged by concurrent SET/RESET"

# 3. LISTEN/NOTIFY cross-fiber wakeup: a parked LISTENer must wake when a
#    different backend (a different fiber/loop) NOTIFYs.
note "LISTEN/NOTIFY cross-fiber"
LWORK="$D/listen.out"
( $PSQL -c "LISTEN xtc_chan;" \
        -c "SELECT pg_sleep(2);" >"$LWORK" 2>&1 ) &
lpid=$!
sleep 0.6
$PSQL -c "NOTIFY xtc_chan, 'hello';" >/dev/null 2>&1
wait $lpid
if grep -qi "Asynchronous notification .*xtc_chan" "$LWORK"; then
  ok "LISTENer received cross-fiber NOTIFY"
else
  # psql -tA may not print the notice header; retry with explicit check via
  # a polling LISTEN using a second query after notify.
  echo "  (listen.out:)"; sed 's/^/    /' "$LWORK" | head
  bad "LISTENer did not report NOTIFY"
fi

# 4. ereport(ERROR) inside a fiber: a SQL error must unwind cleanly and the
#    session must remain usable afterward (fiber survives sigsetjmp unwind).
note "ereport(ERROR) unwind in a fiber"
r=$($PSQL -c "SELECT 1/0;" -c "SELECT 'recovered';" 2>&1)
if echo "$r" | grep -q "division by zero" && echo "$r" | grep -q "recovered"; then
  ok "error unwound and session recovered"
else
  echo "  (got:)"; echo "$r" | sed 's/^/    /' | head
  bad "error unwind / recovery"
fi

# 5. clean fast shutdown after backends have run
note "fast stop"
if timeout 25 pg_ctl -D "$PGDATA" -m fast -w -t 20 stop >/dev/null 2>&1; then
  ok "fast stop clean"
else
  bad "fast stop hung"
  pg_ctl -D "$PGDATA" -m immediate stop >/dev/null 2>&1
fi

sp=$(grep -ac "spawned backend fiber" "$D/pm.log" 2>/dev/null || echo 0)
ex=$(grep -ac "backend fiber exiting" "$D/pm.log" 2>/dev/null || echo 0)
# Persistent worker fibers (autovacuum/logrep launcher, fiber-eligible since
# b2367b59c90) are spawned once and stay alive, so spawned >= exited by the
# number of live workers.  The leak signal is the reverse -- an exit with no
# spawn (lost bookkeeping) -- so require exited <= spawned, spawned != 0, and
# that the surplus is covered by the still-live persistent worker fibers.
live=$(grep -ac "background worker launched as xtc fiber" "$D/pm.log" 2>/dev/null || echo 0)
note "fiber accounting: spawned=$sp exited=$ex (persistent workers=$live)"
if [ "$sp" != "0" ] && [ "$ex" -le "$sp" ] && [ "$((sp - ex))" -le "$live" ]; then
  ok "exited fibers accounted for (surplus $((sp - ex)) <= $live live workers)"
else
  bad "spawn/exit mismatch (spawned=$sp exited=$ex workers=$live)"
fi

# 6. io_method=xtc: backend data-file IO through xtc_aio on fibers (item #6).
#    Restart with a tiny shared_buffers + io_method=xtc so a table scan misses
#    cache and issues real AIO reads through xtc_aio_pread.
note "io_method=xtc data-file reads"
{
  echo "io_method = xtc"
  echo "shared_buffers = 1MB"
} >> "$PGDATA/postgresql.conf"
if pg_ctl -D "$PGDATA" -l "$D/pm2.log" -o "-c multithreaded=on" -w start >/dev/null 2>&1; then
  [ "$($PSQL -c 'SHOW io_method')" = "xtc" ] && ok "io_method=xtc active" || bad "io_method not xtc"
  $PSQL -c "CREATE TABLE aiot(id int primary key, payload text)" >/dev/null 2>&1
  $PSQL -c "INSERT INTO aiot SELECT g, repeat('x',200) FROM generate_series(1,200000) g" >/dev/null 2>&1
  r=$($PSQL -c "SELECT count(*), sum(length(payload)) FROM aiot" 2>/dev/null)
  [ "$r" = "200000|40000000" ] && ok "xtc_aio scan correct ($r)" || bad "xtc_aio scan wrong ($r)"
  # multi-iovec integrity (item #6 step 2): a wide-row table forces combined
  # multi-buffer readv; a per-row md5 check catches any iovec misassembly.
  $PSQL -c "CREATE TABLE aiov(id int primary key, h text)" >/dev/null 2>&1
  $PSQL -c "INSERT INTO aiov SELECT g, md5(g::text) FROM generate_series(1,200000) g" >/dev/null 2>&1
  bad_rows=$($PSQL -c "SELECT count(*) FROM aiov WHERE h <> md5(id::text)" 2>/dev/null)
  [ "$bad_rows" = "0" ] && ok "xtc_aio multi-iovec no misassembly" || bad "xtc_aio iovec misassembly ($bad_rows rows)"
  if grep -aiqE "PANIC|wrong state|corrupt|invalid page" "$D/pm2.log" 2>/dev/null; then
    bad "xtc_aio log has crash/corruption signature"
  else
    ok "xtc_aio no crash/corruption signature"
  fi
  timeout 25 pg_ctl -D "$PGDATA" -m fast -w -t 20 stop >/dev/null 2>&1 || pg_ctl -D "$PGDATA" -m immediate stop >/dev/null 2>&1
else
  bad "io_method=xtc server failed to start"
fi

echo "=== carrier line ==="; grep -a "carrier scheduler thread up" "$D/pm.log" | head -1

# 7. autovacuum churn + fast stop.  Regression gate for the autovac
#    worker-start-timeout cancel/reap race (#5 widening): when a worker fiber
#    is launched but never scheduled (a cross-thread wake to an idle carrier
#    loop can be missed in the current libxtc), the launcher's start-timeout
#    cancels it and the orphaned pooled-logical PMChild must still be reaped,
#    or PM_WAIT_BACKENDS wedges at fast stop.  Driven with a hot autovacuum
#    (naptime=1, threshold=1) so a cancel is very likely, then a fast stop that
#    MUST complete.  Valid in both process-mode and thread-carrier autovac too
#    (both must fast-stop cleanly under churn), so it is unconditional; it is
#    the specific gate for the fiber-eligible autovac path.  Uses its own
#    cluster so its autovac settings do not perturb the checks above.
note "autovacuum churn + fast stop"
AVD=$(mktemp -d "$SCRATCH/xtcavXXXXXX")
if initdb -D "$AVD/pgdata" -U postgres --no-locale -E UTF8 >"$AVD/initdb.log" 2>&1; then
  {
    echo "listen_addresses = ''"
    echo "unix_socket_directories = '$AVD'"
    echo "logging_collector = off"
    echo "autovacuum = on"
    echo "autovacuum_naptime = 1"
    echo "autovacuum_vacuum_threshold = 1"
    echo "autovacuum_vacuum_cost_delay = 0"
  } >> "$AVD/pgdata/postgresql.conf"
  if pg_ctl -D "$AVD/pgdata" -l "$AVD/pm.log" -o "-c multithreaded=on" -w start >/dev/null 2>&1; then
    AVPSQL="psql -X -h $AVD -U postgres -d postgres -tA"
    $AVPSQL -c "CREATE TABLE av(id int); INSERT INTO av SELECT g FROM generate_series(1,5000) g;" >/dev/null 2>&1
    for r in 1 2 3 4; do
      $AVPSQL -c "UPDATE av SET id=id+1; DELETE FROM av WHERE id%3=0; INSERT INTO av SELECT g FROM generate_series(1,3000) g;" >/dev/null 2>&1
      sleep 1.5
    done
    sleep 2
    if timeout 25 pg_ctl -D "$AVD/pgdata" -m fast -w -t 20 stop >/dev/null 2>&1; then
      cx=$(grep -ac "took too long to start" "$AVD/pm.log" 2>/dev/null || echo 0)
      ok "autovac churn fast stop clean (worker-start cancels seen: $cx)"
    else
      bad "autovac churn fast stop HUNG (worker-start-timeout cancel/reap race?)"
      pg_ctl -D "$AVD/pgdata" -m immediate stop >/dev/null 2>&1
      kill -9 "$(head -1 "$AVD/pgdata/postmaster.pid" 2>/dev/null)" 2>/dev/null
    fi
  else
    bad "autovac churn server failed to start"
  fi
else
  bad "autovac churn initdb failed"
fi

echo "SCRATCH=$D"
exit $fail
