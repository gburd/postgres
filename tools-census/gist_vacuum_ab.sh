#!/bin/bash
# gist_vacuum_ab.sh <install-prefix> <port> <outfile> [rows] [rounds]
#
# End-to-end test of GiST VACUUM's two page sets (empty leaves, internal
# pages) held as IntegerSet (default) or sbm (GIST_PAGESET=sbm).  Each round
# rebuilds the same table and GiST index, deletes a pattern of rows so many
# leaf pages empty, then VACUUMs once under each mode, alternating which goes
# first.  Records the VACUUM wall time and the server's GIST_PAGESET log
# line (set-operation time, set sizes, bytes).
set -euo pipefail
PREFIX=$1
PORT=$2
OUT=$3
ROWS=${4:-2000000}
ROUNDS=${5:-5}
DATA=${DATA:-$(dirname "$OUT")/pgdata-gist-$PORT}
export PATH=$PREFIX/bin:$PATH PGPORT=$PORT PGHOST=/tmp
if [ ! -d "$DATA" ]; then
	initdb -D "$DATA" --no-sync -A trust >/dev/null
	cat >>"$DATA/postgresql.conf" <<EOF
port = $PORT
unix_socket_directories = '/tmp'
listen_addresses = ''
autovacuum = off
shared_buffers = 2GB
maintenance_work_mem = 1GB
fsync = off
synchronous_commit = off
full_page_writes = off
max_wal_size = 20GB
log_min_messages = log
EOF
fi
q() { psql -X -q -At -d postgres -v ON_ERROR_STOP=1 "$@"; }

run_mode() {
	local mode=$1 round=$2 t0 t1 ms line
	if [ "$mode" = sbm ]; then
		GIST_PAGESET=sbm ${PIN:-} pg_ctl -D "$DATA" -l "$DATA/log" -w start >/dev/null
	else
		${PIN:-} pg_ctl -D "$DATA" -l "$DATA/log" -w start >/dev/null
	fi
	# Same starting state for both modes: rebuild the deleted index state.
	q -c "drop table if exists g" \
		-c "create table g (p point) with (autovacuum_enabled = off)" \
		-c "insert into g select point(i % 3001, i / 3001) from generate_series(1, $ROWS) i" \
		-c "create index g_gist on g using gist (p)" \
		-c "delete from g where (p[1]::int % 50) < 49" \
		-c "checkpoint"
	t0=$(date +%s%N)
	q -c "vacuum (index_cleanup on, truncate off) g"
	t1=$(date +%s%N)
	ms=$(( (t1 - t0) / 1000000 ))
	line=$(grep 'GIST_PAGESET' "$DATA/log" | tail -1 | sed 's/.*GIST_PAGESET //')
	echo "$round,$mode,$ms,$line" >>"$OUT"
	pg_ctl -D "$DATA" -m fast -w stop >/dev/null
}

echo "round,mode,vacuum_ms,detail" >"$OUT"
for r in $(seq 1 "$ROUNDS"); do
	if [ $((r % 2)) -eq 1 ]; then
		run_mode intset "$r"
		run_mode sbm "$r"
	else
		run_mode sbm "$r"
		run_mode intset "$r"
	fi
done
