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
# Persistent worker/singleton fibers (autovacuum/logrep launcher bgworkers,
# fiber-eligible since b2367b59c90; plus the long-lived aux singleton
# walwriter, fiber-eligible per the #5 Tier A widening) are spawned once and
# stay alive, so spawned >= exited by the number of live ones.  Their
# best-effort "backend fiber exiting" raw-STDERR write can also be lost at the
# very end of shutdown (the postmaster closes the log right after "database
# system is shut down").  The leak signal is the reverse -- an exit with no
# spawn (lost bookkeeping) -- so require exited <= spawned, spawned != 0, and
# that the surplus is covered by the still-live persistent fibers (bgworkers +
# the walwriter singleton launched as a fiber).
live=$(grep -acE "(background worker|walwriter) launched as xtc fiber" "$D/pm.log" 2>/dev/null || echo 0)
note "fiber accounting: spawned=$sp exited=$ex (persistent fibers=$live)"
if [ "$sp" != "0" ] && [ "$ex" -le "$sp" ] && [ "$((sp - ex))" -le "$live" ]; then
  ok "exited fibers accounted for (surplus $((sp - ex)) <= $live live persistent fibers)"
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

# 8. walwriter runs as an xtc fiber and shuts down clean.  #5 Tier A widening:
#    the WAL writer is fiber-eligible.  It is a long-lived singleton, so the
#    gates are (a) it actually runs as a fiber (pg_stat_activity shows one
#    walwriter, the pm.log has "walwriter launched as xtc fiber", and the
#    postmaster has ZERO child OS processes -- everything is a thread/fiber in
#    its address space), (b) it survives real WAL write load, and (c) it wakes
#    from its parked WaitLatch on the shutdown interrupt so fast stop completes
#    even after the loops have gone fully idle (its commit-time procLatch fd
#    wake, xlog.c, is what makes an idle-loop wake reliable here).  Uses its
#    own cluster so its config does not perturb the checks above.
note "walwriter runs as a fiber and shuts down clean"
WWD=$(mktemp -d "$SCRATCH/xtcwwXXXXXX")
if initdb -D "$WWD/pgdata" -U postgres --no-locale -E UTF8 >"$WWD/initdb.log" 2>&1; then
  {
    echo "listen_addresses = ''"
    echo "unix_socket_directories = '$WWD'"
    echo "logging_collector = off"
    echo "autovacuum = off"
  } >> "$WWD/pgdata/postgresql.conf"
  if pg_ctl -D "$WWD/pgdata" -l "$WWD/pm.log" -o "-c multithreaded=on" -w start >/dev/null 2>&1; then
    WWPSQL="psql -X -h $WWD -U postgres -d postgres -tA"
    wwcnt=$($WWPSQL -c "SELECT count(*) FROM pg_stat_activity WHERE backend_type='walwriter'" 2>/dev/null)
    wwfiber=$(grep -ac "xtc: walwriter launched as xtc fiber" "$WWD/pm.log" 2>/dev/null || echo 0)
    wwpm=$(head -1 "$WWD/pgdata/postmaster.pid" 2>/dev/null)
    wwchild=$(ps --no-headers --ppid "$wwpm" 2>/dev/null | wc -l)
    if [ "$wwcnt" = "1" ] && [ "$wwfiber" -ge 1 ] && [ "$wwchild" = "0" ]; then
      ok "walwriter runs as a fiber (activity=$wwcnt, fiber-launch=$wwfiber, pm child procs=$wwchild)"
    else
      bad "walwriter not a fiber (activity=$wwcnt fiber-launch=$wwfiber childprocs=$wwchild)"
    fi
    # real WAL write load
    $WWPSQL -c "CREATE TABLE ww(id int, p text)" >/dev/null 2>&1
    $WWPSQL -c "INSERT INTO ww SELECT g, repeat('w',100) FROM generate_series(1,50000) g" >/dev/null 2>&1
    wwrows=$($WWPSQL -c "SELECT count(*) FROM ww" 2>/dev/null)
    [ "$wwrows" = "50000" ] && ok "walwriter fiber survived WAL write load (rows=$wwrows)" || bad "walwriter WAL load ($wwrows)"
    if timeout 30 pg_ctl -D "$WWD/pgdata" -m fast -w -t 25 stop >/dev/null 2>&1; then
      wwcore=$(find "$WWD" -maxdepth 3 -name 'core*' 2>/dev/null | wc -l)
      wwshut=$(grep -ac "database system is shut down" "$WWD/pm.log" 2>/dev/null || echo 0)
      if [ "$wwcore" = "0" ] && [ "$wwshut" -ge 1 ]; then
        ok "walwriter fiber fast stop clean (cores=$wwcore)"
      else
        bad "walwriter fiber fast stop dirty (cores=$wwcore shutmsg=$wwshut)"
      fi
    else
      bad "walwriter fiber fast stop HUNG"
      pg_ctl -D "$WWD/pgdata" -m immediate stop >/dev/null 2>&1
      kill -9 "$(head -1 "$WWD/pgdata/postmaster.pid" 2>/dev/null)" 2>/dev/null
    fi
  else
    bad "walwriter fiber server failed to start"
  fi
else
  bad "walwriter fiber initdb failed"
fi

# 9. walsummarizer runs as an xtc fiber, advances, and shuts down clean.  #5
#    Tier A widening: the WAL summarizer is fiber-eligible.  Gates: (a) it runs
#    as a fiber ("walsummarizer launched as xtc fiber" in pm.log, postmaster
#    has ZERO child OS processes), (b) it advances -- pending_lsn tracks the
#    flush LSN and summarized_lsn advances across a checkpoint, proving it
#    re-polls WAL on each WaitLatch-timeout wake with no fd-based wake needed --
#    and (c) it shuts down clean under BOTH fast and immediate stop.  Immediate
#    stop is the gate for the PROC_DIE fix: a threaded summarizer has no OS
#    crash-exit handler, so SIGQUIT arrives as a PROC_DIE interrupt that
#    ProcessWalSummarizerInterrupts must honor or PM_WAIT_* wedges.  Own cluster
#    with wal_level=replica + summarize_wal=on.
note "walsummarizer runs as a fiber, advances, and shuts down clean"
WSD=$(mktemp -d "$SCRATCH/xtcwsXXXXXX")
if initdb -D "$WSD/pgdata" -U postgres --no-locale -E UTF8 >"$WSD/initdb.log" 2>&1; then
  {
    echo "listen_addresses = ''"
    echo "unix_socket_directories = '$WSD'"
    echo "logging_collector = off"
    echo "autovacuum = off"
    echo "wal_level = replica"
    echo "summarize_wal = on"
  } >> "$WSD/pgdata/postgresql.conf"
  if pg_ctl -D "$WSD/pgdata" -l "$WSD/pm.log" -o "-c multithreaded=on" -w start >/dev/null 2>&1; then
    WSPSQL="psql -X -h $WSD -U postgres -d postgres -tA"
    $WSPSQL -c "CREATE TABLE ws(id int, p text)" >/dev/null 2>&1
    for r in 1 2 3 4 5; do
      $WSPSQL -c "INSERT INTO ws SELECT g, repeat('s',80) FROM generate_series(1,100000) g; SELECT pg_switch_wal();" >/dev/null 2>&1
    done
    $WSPSQL -c "CHECKPOINT" >/dev/null 2>&1
    sleep 3
    wsfiber=$(grep -ac "xtc: walsummarizer launched as xtc fiber" "$WSD/pm.log" 2>/dev/null || echo 0)
    wspm=$(head -1 "$WSD/pgdata/postmaster.pid" 2>/dev/null)
    wschild=$(ps --no-headers --ppid "$wspm" 2>/dev/null | wc -l)
    if [ "$wsfiber" -ge 1 ] && [ "$wschild" = "0" ]; then
      ok "walsummarizer runs as a fiber (fiber-launch=$wsfiber, pm child procs=$wschild)"
    else
      bad "walsummarizer not a fiber (fiber-launch=$wsfiber childprocs=$wschild)"
    fi
    wssum=$($WSPSQL -c "SELECT summarized_lsn FROM pg_get_wal_summarizer_state()" 2>/dev/null)
    wsflush=$($WSPSQL -c "SELECT pg_current_wal_flush_lsn()" 2>/dev/null)
    wspend=$($WSPSQL -c "SELECT pending_lsn FROM pg_get_wal_summarizer_state()" 2>/dev/null)
    wsfiles=$(ls "$WSD/pgdata/pg_wal/summaries/" 2>/dev/null | wc -l)
    if [ "$wsfiles" -ge 1 ] && [ "$wspend" = "$wsflush" ] && [ "$wssum" != "0/017DCAF0" ]; then
      ok "walsummarizer advanced (summarized=$wssum pending=$wspend flush=$wsflush files=$wsfiles)"
    else
      bad "walsummarizer did not advance (summarized=$wssum pending=$wspend flush=$wsflush files=$wsfiles)"
    fi
    if timeout 30 pg_ctl -D "$WSD/pgdata" -m fast -w -t 25 stop >/dev/null 2>&1; then
      ok "walsummarizer fiber fast stop clean"
    else
      bad "walsummarizer fiber fast stop HUNG"
      pg_ctl -D "$WSD/pgdata" -m immediate stop >/dev/null 2>&1
      kill -9 "$(head -1 "$WSD/pgdata/postmaster.pid" 2>/dev/null)" 2>/dev/null
    fi
    if pg_ctl -D "$WSD/pgdata" -l "$WSD/pm2.log" -o "-c multithreaded=on" -w start >/dev/null 2>&1; then
      $WSPSQL -c "INSERT INTO ws SELECT g,'x' FROM generate_series(1,50000) g; SELECT pg_switch_wal(); CHECKPOINT" >/dev/null 2>&1
      sleep 8
      if timeout 30 pg_ctl -D "$WSD/pgdata" -m immediate -w -t 25 stop >/dev/null 2>&1; then
        ok "walsummarizer fiber immediate stop clean (PROC_DIE honored)"
      else
        bad "walsummarizer fiber immediate stop HUNG (PROC_DIE not honored?)"
        kill -9 "$(head -1 "$WSD/pgdata/postmaster.pid" 2>/dev/null)" 2>/dev/null
      fi
    else
      bad "walsummarizer fiber restart for immediate-stop check failed"
    fi
  else
    bad "walsummarizer fiber server failed to start"
  fi
else
  bad "walsummarizer fiber initdb failed"
fi

# 10. archiver runs as an xtc fiber, archives WAL, and shuts down clean.  #5
#     Tier C widening: the WAL archiver is fiber-eligible (file-level work, not
#     on the io_method=worker cross-thread completion path).  Gates: (a) it runs
#     as a fiber ("archiver launched as xtc fiber" in pm.log, postmaster has
#     ZERO child OS processes), (b) it actually archives
#     (pg_stat_archiver.archived_count > 0 and files appear in the archive dir --
#     proving the latch wake and archive_command run on the fiber), and (c) it
#     shuts down clean under BOTH fast and immediate stop.  The archiver's
#     two-step shutdown (SIGTERM=drain-not-die, SIGUSR2=one final cycle then
#     exit) is wired in thread_child_signal_interrupt.  Own cluster with
#     archive_mode=on + a cp archive_command.
note "archiver runs in-process (thread carrier), archives WAL, and shuts down clean"
ARD=$(mktemp -d "$SCRATCH/xtcarXXXXXX")
if initdb -D "$ARD/pgdata" -U postgres --no-locale -E UTF8 >"$ARD/initdb.log" 2>&1; then
  mkdir -p "$ARD/wal_archive"
  {
    echo "listen_addresses = ''"
    echo "unix_socket_directories = '$ARD'"
    echo "logging_collector = off"
    echo "autovacuum = off"
    echo "wal_level = replica"
    echo "archive_mode = on"
    echo "archive_command = 'cp %p $ARD/wal_archive/%f'"
  } >> "$ARD/pgdata/postgresql.conf"
  if pg_ctl -D "$ARD/pgdata" -l "$ARD/pm.log" -o "-c multithreaded=on" -w start >/dev/null 2>&1; then
    ARPSQL="psql -X -h $ARD -U postgres -d postgres -tA"
    $ARPSQL -c "CREATE TABLE ar(id int)" >/dev/null 2>&1
    for r in 1 2 3 4; do
      $ARPSQL -c "INSERT INTO ar SELECT generate_series(1,20000); SELECT pg_switch_wal();" >/dev/null 2>&1
    done
    sleep 3
    arfiber=$(grep -acE "xtc: archiver launched as xtc fiber|starting archiver thread carrier" "$ARD/pm.log" 2>/dev/null || echo 0)
    arpm=$(head -1 "$ARD/pgdata/postmaster.pid" 2>/dev/null)
    archild=$(ps --no-headers --ppid "$arpm" 2>/dev/null | grep -ac archiver)
    if [ "$arfiber" -ge 1 ] && [ "$archild" = "0" ]; then
      ok "archiver runs in-process (thread carrier), 0 archiver forks"
    else
      bad "archiver not in-process (launch=$arfiber archiver-forks=$archild)"
    fi
    arcount=$($ARPSQL -c "SELECT archived_count FROM pg_stat_archiver" 2>/dev/null)
    arfiles=$(ls "$ARD/wal_archive/" 2>/dev/null | grep -cvE '\.done$|backup')
    if [ "${arcount:-0}" -ge 1 ] && [ "$arfiles" -ge 1 ]; then
      ok "archiver archived WAL (archived_count=$arcount files=$arfiles)"
    else
      bad "archiver did not archive (archived_count=$arcount files=$arfiles)"
    fi
    if timeout 30 pg_ctl -D "$ARD/pgdata" -m fast -w -t 25 stop >/dev/null 2>&1; then
      arcore=$(ls "$ARD/pgdata"/core* 2>/dev/null | wc -l)
      if [ "$arcore" = "0" ]; then
        ok "archiver fiber fast stop clean (cores=$arcore)"
      else
        bad "archiver fiber fast stop dirty (cores=$arcore)"
      fi
    else
      bad "archiver fiber fast stop HUNG"
      pg_ctl -D "$ARD/pgdata" -m immediate stop >/dev/null 2>&1
      kill -9 "$(head -1 "$ARD/pgdata/postmaster.pid" 2>/dev/null)" 2>/dev/null
    fi
    if pg_ctl -D "$ARD/pgdata" -l "$ARD/pm2.log" -o "-c multithreaded=on" -w start >/dev/null 2>&1; then
      $ARPSQL -c "INSERT INTO ar SELECT generate_series(1,5000); SELECT pg_switch_wal();" >/dev/null 2>&1
      sleep 2
      if timeout 30 pg_ctl -D "$ARD/pgdata" -m immediate -w -t 25 stop >/dev/null 2>&1; then
        arcore2=$(ls "$ARD/pgdata"/core* 2>/dev/null | wc -l)
        [ "$arcore2" = "0" ] && ok "archiver fiber immediate stop clean (cores=$arcore2)" || bad "archiver fiber immediate stop dirty (cores=$arcore2)"
      else
        bad "archiver fiber immediate stop HUNG"
        kill -9 "$(head -1 "$ARD/pgdata/postmaster.pid" 2>/dev/null)" 2>/dev/null
      fi
    else
      bad "archiver fiber restart for immediate-stop check failed"
    fi
  else
    bad "archiver fiber server failed to start"
  fi
else
  bad "archiver fiber initdb failed"
fi

echo "SCRATCH=$D"
exit $fail
