#!/bin/bash
# census_run.sh <install-prefix> <outdir>
#
# Runs each workload against a fresh cluster with BMS_CENSUS_DIR pointing at
# <outdir>/<workload>, so every backend dumps its Bitmapset census there on
# exit.  Workloads: core regression suite, partitioning at 1000 and 10000
# partitions, a wide table, a many-way join, and pgbench.
set -euo pipefail
PREFIX=$1
OUT=$2
SRC=${SRC:-/scratch/postgres-bits/wt-census}
BUILD=${BUILD:-/scratch/postgres-bits/build-census}
PORT=${PORT:-54377}
DATA=$OUT/pgdata
# Installed pg_regress; distros differ on lib vs lib64/postgresql.
PGREGRESS=$(find "$PREFIX" -path '*/pgxs/src/test/regress/pg_regress' -type f | head -1)
export PATH=$PREFIX/bin:$PATH
export PGPORT=$PORT PGHOST=/tmp PGDATABASE=postgres

mkdir -p "$OUT"
if [ ! -d "$DATA" ]; then
	initdb -D "$DATA" --no-sync -A trust >/dev/null
	cat >>"$DATA/postgresql.conf" <<EOF
port = $PORT
unix_socket_directories = '/tmp'
max_connections = 50
shared_buffers = 1GB
max_locks_per_transaction = 1024
fsync = off
synchronous_commit = off
full_page_writes = off
autovacuum = off
listen_addresses = ''
EOF
fi

start() {
	mkdir -p "$OUT/$1"
	BMS_CENSUS_DIR="$OUT/$1" pg_ctl -D "$DATA" -l "$OUT/$1.log" -w start >/dev/null
}
stop() { pg_ctl -D "$DATA" -m fast -w stop >/dev/null; }
q() { psql -X -q -v ON_ERROR_STOP=1 "$@"; }

# Set up the schema with the census off (the setup is not the workload).
pg_ctl -D "$DATA" -l "$OUT/setup.log" -w start >/dev/null
q -c "drop database if exists w"
q -c "create database w"
q -c "drop database if exists regression"
q -c "create database regression"
q -d w <<'SQL'
create table lp1k (a int, b int, c text) partition by list (a);
select format('create table lp1k_%s partition of lp1k for values in (%s)', x, x)
  from generate_series(0, 999) x \gexec
create table lp10k (a int, b int) partition by list (a);
select format('create table lp10k_%s partition of lp10k for values in (%s)', x, x)
  from generate_series(0, 9999) x \gexec
create index on lp10k (a);
create table hp1 (a int, b int) partition by hash (a);
create table hp2 (a int, b int) partition by hash (a);
select format('create table hp1_%s partition of hp1 for values with (modulus 1000, remainder %s)', x, x),
       format('create table hp2_%s partition of hp2 for values with (modulus 1000, remainder %s)', x, x)
  from generate_series(0, 999) x \gexec
insert into lp1k select x % 1000, x, 'v' || x from generate_series(1, 100000) x;
insert into lp10k select x % 10000, x from generate_series(1, 100000) x;
insert into hp1 select x, x from generate_series(1, 100000) x;
insert into hp2 select x, x from generate_series(1, 100000) x;
select format('create table wide (%s)',
       string_agg(format('c%s int', x), ', ')) from generate_series(1, 1500) x \gexec
insert into wide (c1, c750, c1500) select x, x, x from generate_series(1, 1000) x;
select format('create table j%s (id int primary key, nxt int, v int)', x) from generate_series(1, 14) x \gexec
select format('insert into j%s select g, g, g from generate_series(1, 1000) g', x) from generate_series(1, 14) x \gexec
analyze;
SQL
stop

# 1. Core regression suite against the running server.
start regress
"$PGREGRESS" --use-existing --bindir="$PREFIX/bin" \
	--inputdir="$SRC/src/test/regress" --dlpath="$BUILD/src/test/regress" \
	--schedule="$SRC/src/test/regress/parallel_schedule" \
	--outputdir="$OUT/regress-out" --max-connections=8 >"$OUT/regress-run.log" 2>&1 || true
stop

# 2. Partitioning: plan-time pruning, runtime pruning (generic plans), full
#    scans, partitionwise join and aggregate, and DML.
start partition
q -d w <<'SQL'
set plan_cache_mode = force_generic_plan;
prepare p1(int) as select * from lp10k where a = $1;
select count(*) from (select (select count(*) from lp10k where a = g) from generate_series(1, 50) g) s;
execute p1(17); execute p1(5000); execute p1(9999);
execute p1(17); execute p1(5000); execute p1(9999);
prepare p2(int) as select * from lp1k where a = $1;
execute p2(1); execute p2(500); execute p2(999);
reset plan_cache_mode;
explain (costs off) select * from lp10k where a in (1, 5000, 9999);
explain (costs off) select * from lp1k order by a limit 10;
explain (costs off) select count(*) from lp10k;
set enable_partitionwise_join = on;
set enable_partitionwise_aggregate = on;
explain (costs off) select * from hp1 join hp2 using (a) where hp1.b < 100;
select count(*) from hp1 join hp2 using (a) where hp1.b < 1000;
select a, count(*) from hp1 group by a order by 2 desc limit 1;
update lp1k set b = b + 1 where a = 7;
update lp10k set b = b + 1 where a = 7777;
delete from lp1k where a = 3 and b < 0;
SQL
stop

# 3. Wide table: column sets reach 1500 attributes.
start wide
q -d w <<'SQL'
select count(*) from wide where c1500 > 10;
update wide set c750 = c750 + 1, c1499 = 1 where c1 < 100;
select c1, c750, c1500 from wide where c1 = 5;
insert into wide (c1, c1500) values (1, 1);
SQL
stop

# 4. Many-way join planning (exhaustive search up to the collapse limits).
start join
q -d w <<'SQL'
set join_collapse_limit = 14; set from_collapse_limit = 14; set geqo_threshold = 20;
explain (costs off) select * from j1, j2, j3, j4, j5, j6, j7, j8, j9, j10, j11, j12
 where j1.nxt = j2.id and j2.nxt = j3.id and j3.nxt = j4.id and j4.nxt = j5.id
   and j5.nxt = j6.id and j6.nxt = j7.id and j7.nxt = j8.id and j8.nxt = j9.id
   and j9.nxt = j10.id and j10.nxt = j11.id and j11.nxt = j12.id;
explain (costs off) select * from j1 join j2 on j1.v = j2.v join j3 on j1.v = j3.v
 join j4 on j1.v = j4.v join j5 on j1.v = j5.v join j6 on j1.v = j6.v
 join j7 on j1.v = j7.v join j8 on j1.v = j8.v join j9 on j1.v = j9.v;
SQL
stop

# 5. OLTP: pgbench select-only and tpcb-like.
start pgbench
pgbench -i -s 5 -q w >/dev/null 2>&1
pgbench -n -S -c 4 -j 4 -t 2000 w >"$OUT/pgbench-S.log" 2>&1
pgbench -n -c 4 -j 4 -t 1000 w >"$OUT/pgbench-tpcb.log" 2>&1
stop

echo "census done: $OUT"
