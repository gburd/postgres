#!/bin/bash
# ops_bench.sh <install-prefix> <port> <outfile>
#
# Runs test_sbm_ops_bench over the shapes used to compare the sbm port
# before and after sparsemap 5.9, plus test_sbm_bms_shapes and
# test_sbm_intset_bench, writing CSV to <outfile>.  The server is started
# and stopped here; the data directory is created on first use.
set -euo pipefail
PREFIX=$1
PORT=$2
OUT=$3
DATA=${DATA:-$(dirname "$OUT")/pgdata-$PORT}
export PATH=$PREFIX/bin:$PATH PGPORT=$PORT PGHOST=/tmp
if [ ! -d "$DATA" ]; then
	initdb -D "$DATA" --no-sync -A trust >/dev/null
	printf "port = %s\nunix_socket_directories = '/tmp'\nlisten_addresses = ''\nautovacuum = off\n" \
		"$PORT" >>"$DATA/postgresql.conf"
fi
${PIN:-} pg_ctl -D "$DATA" -l "$DATA/log" -w start >/dev/null
trap 'pg_ctl -D "$DATA" -m fast -w stop >/dev/null' EXIT
psql -X -q -d postgres -c 'drop extension if exists test_sbm' \
	-c 'create extension test_sbm'
q() { psql -X -d postgres -P pager=off -A -F, -c "$1"; }

{
	echo "# ops"
	q "select p.label, b.* from (values
	     ('uniform 1e-2 n=1e5', 100000::bigint, 10000000::bigint, 0),
	     ('uniform 1e-3 n=1e5', 100000, 100000000, 0),
	     ('runs64 1e-2 n=1e5', 100000, 10000000, 1),
	     ('clustered n=1e5', 100000, 100000000, 2),
	     ('uniform 1e-2 n=1e3', 1000, 100000, 0),
	     ('dense n=1e3 in 1024', 1000, 1024, 0)) p(label, n, u, pat),
	   lateral test_sbm_ops_bench(p.n, p.u, p.pat, 4000, 3) b"
	echo "# shapes"
	q "select * from test_sbm_bms_shapes(10000, 20000, 7)"
	echo "# intset"
	q "select p.label, b.* from (values
	     ('random 1% of 10M', 100000::bigint, 10000000::bigint, 0, 1::bigint),
	     ('random 50% of 200k', 100000, 200000, 0, 1),
	     ('runs64 5% of 2M', 100000, 2000000, 1, 64)) p(label, nm, nb, pat, rl),
	   lateral test_sbm_intset_bench(p.nm, p.nb, p.pat, p.rl, 20000, 3) b"
} >"$OUT"
