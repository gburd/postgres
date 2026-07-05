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

PSQL="psql -h $D -U postgres -d postgres -tA"

# 1. basic round-trip through the xtc carrier
note "select 1"
[ "$($PSQL -c 'select 1')" = "1" ] && ok "select 1" || bad "select 1"

# 2. N concurrent backends, each holds the socket, then a wedge check
note "6 concurrent backends"
for n in 1 2 3 4 5 6; do
  ( $PSQL -c "select pg_sleep(0.4); select $n" >/dev/null 2>&1 ) &
done
wait
r=$(timeout 10 $PSQL -c 'select 42' 2>/dev/null)
[ "$r" = "42" ] && ok "loop not wedged after concurrency" || bad "loop wedged"

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
note "fiber accounting: spawned=$sp exited=$ex"
[ "$sp" = "$ex" ] && [ "$sp" != "0" ] && ok "spawned == exited" || bad "spawn/exit mismatch"

echo "=== carrier line ==="; grep -a "carrier scheduler thread up" "$D/pm.log" | head -1
echo "SCRATCH=$D"
exit $fail
