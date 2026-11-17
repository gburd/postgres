#!/bin/bash
# ec2_run.sh: build both trees on a fresh Fedora host and run every
# measurement in the census branch.  Writes results to ~/results.
#
# Build A is the census build (bms_census.c compiled in, bms_* wrapped);
# build B is a plain release build.  Timing runs use B only.  Each timing
# function alternates Bitmapset/sbm (or intset/sbm) order per repetition and
# reports the minimum, and the whole timing set is run three times so the
# spread across runs is visible.
set -euo pipefail
BRANCH=${BRANCH:-wip/bms-census}
REPO=${REPO:-https://github.com/gburd/postgres.git}
W=$HOME/w
R=$HOME/results
mkdir -p "$W" "$R"

sudo dnf -y -q install git gcc meson ninja-build flex bison perl \
	perl-FindBin perl-lib perl-File-Compare python3 libicu-devel \
	readline-devel zlib-devel binutils numactl >/dev/null

[ -d "$W/src" ] || git clone -q --depth 1 -b "$BRANCH" "$REPO" "$W/src"
cd "$W/src"
git fetch -q --depth 1 origin "$BRANCH" && git reset -q --hard FETCH_HEAD
git log --oneline -1 >"$R/commit.txt"

common=(-Dssl=none -Dicu=disabled -Dlibcurl=disabled -Dreadline=disabled
	-Dzlib=disabled -Dsystemd=disabled -Dcassert=false)
WRAP=$(cat tools-census/wrapflags)
[ -d "$W/build-census" ] || meson setup "$W/build-census" --buildtype=debugoptimized "${common[@]}" \
	--prefix="$W/inst-census" \
	-Dc_args='-fno-optimize-sibling-calls -DBMS_CENSUS' \
	-Dc_link_args="$WRAP" >/dev/null
[ -d "$W/build-rel" ] || meson setup "$W/build-rel" --buildtype=release "${common[@]}" \
	--prefix="$W/inst-rel" >/dev/null
for b in census rel; do
	ninja -C "$W/build-$b" install install-test-files >"$R/build-$b.log" 2>&1
	grep -cE 'warning:' "$R/build-$b.log" >"$R/build-$b.warnings" || true
done

{
	echo "cpu: $(lscpu | sed -n 's/^Model name: *//p')"
	echo "kernel: $(uname -r)"
	echo "gcc: $(gcc --version | head -1)"
	echo "governor: $(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null || echo n/a)"
	echo "no_turbo: $(cat /sys/devices/system/cpu/intel_pstate/no_turbo 2>/dev/null || echo n/a)"
	echo "thp: $(cat /sys/kernel/mm/transparent_hugepage/enabled)"
	nproc
} >"$R/env.txt"

# 1. Census (allocation counts; machine-independent).
C=$W/census-$(date +%s)
SRC=$W/src BUILD=$W/build-census tools-census/census_run.sh \
	"$W/inst-census" "$C" >"$R/census-run.log" 2>&1
PGBIN=$W/inst-census/bin/postgres python3 tools-census/census_sum.py \
	"$C/regress" "$C/partition" "$C/join" "$C/pgbench" "$C/wide" \
	>"$R/census-summary.txt"

# 2. Timing on the release build, pinned to one core of node 0.
D=$W/pgdata-rel-$(date +%s)
export PATH=$W/inst-rel/bin:$PATH PGPORT=54390 PGHOST=/tmp
initdb -D "$D" --no-sync -A trust >/dev/null
printf "port = 54390\nunix_socket_directories = '/tmp'\nlisten_addresses = ''\nautovacuum = off\nshared_buffers = 256MB\n" >>"$D/postgresql.conf"
numactl --physcpubind=2 --membind=0 pg_ctl -D "$D" -l "$R/pg-rel.log" -w start >/dev/null
psql -X -q -d postgres -c 'create extension test_sbm'
for run in 1 2 3; do
	psql -X -d postgres -P pager=off -A -F, -c "
	  select $run as run, shape, op, bms_ns, sbm_ns, ratio, bms_bytes, sbm_bytes
	  from test_sbm_bms_shapes(10000, 20000, 9)" >"$R/shapes-$run.csv"
	psql -X -d postgres -P pager=off -A -F, -c "
	  select $run as run, p.label, b.*
	  from (values
	   ('random 1% of 10M', 100000::bigint, 10000000::bigint, 0, 1::bigint),
	   ('random 10% of 1M', 100000, 1000000, 0, 1),
	   ('random 50% of 200k', 100000, 200000, 0, 1),
	   ('runs64 5% of 2M', 100000, 2000000, 1, 64),
	   ('runs8 1% of 10M', 100000, 10000000, 1, 8)) p(label, nm, nb, pat, rl),
	   lateral test_sbm_intset_bench(p.nm, p.nb, p.pat, p.rl, 200000, 5) b" \
		>"$R/intset-$run.csv"
	# 1M members: the plain sbm probe is O(chunks), so fewer probes.
	psql -X -d postgres -P pager=off -A -F, -c "
	  select $run as run, p.label, b.*
	  from (values
	   ('random 1% of 100M', 1000000::bigint, 100000000::bigint, 0, 1::bigint),
	   ('runs64 5% of 20M', 1000000, 20000000, 1, 64)) p(label, nm, nb, pat, rl),
	   lateral test_sbm_intset_bench(p.nm, p.nb, p.pat, p.rl, 20000, 3) b" \
		>"$R/intset-big-$run.csv"
done
pg_ctl -D "$D" -m fast -w stop >/dev/null
echo finished >"$R/DONE"
