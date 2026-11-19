#!/bin/bash
# ec2_run59.sh: same-host before/after of the sbm 5.9 port, plus the
# Bitmapset-substitution re-test, on a fresh Fedora host.  Results land in
# ~/results59.
#
#   base: wip/bms-census-58bench (sbm at sparsemap 5.8.5)
#   new:  wip/bms-census-59    (sbm at sparsemap 5.9.0)
#
# Each tree gets a release build.  Timing runs pin the server to one core and
# alternate base/new across three rounds; every timing function inside reports
# the minimum of its own repetitions.
set -euo pipefail
REPO=${REPO:-https://github.com/gburd/postgres.git}
W=$HOME/w59
R=$HOME/results59
mkdir -p "$W" "$R"

sudo dnf -y -q install git gcc meson ninja-build flex bison perl \
	perl-FindBin perl-lib perl-File-Compare python3 libicu-devel \
	readline-devel zlib-devel binutils numactl >/dev/null

common=(-Dssl=none -Dicu=disabled -Dlibcurl=disabled -Dreadline=disabled
	-Dzlib=disabled -Dsystemd=disabled -Dcassert=false)
for side in base new; do
	branch=wip/bms-census-58bench
	[ "$side" = new ] && branch=wip/bms-census-59
	if [ ! -d "$W/src-$side" ]; then
		git clone -q --depth 1 -b "$branch" "$REPO" "$W/src-$side"
	fi
	git -C "$W/src-$side" log --oneline -1 >"$R/commit-$side.txt"
	[ -d "$W/build-$side" ] || meson setup "$W/build-$side" "$W/src-$side" \
		--buildtype=release "${common[@]}" --prefix="$W/inst-$side" >/dev/null
	ninja -C "$W/build-$side" install install-test-files \
		>"$R/build-$side.log" 2>&1
	grep -cE 'warning:' "$R/build-$side.log" >"$R/build-$side.warnings" || true
done

{
	echo "cpu: $(lscpu | sed -n 's/^Model name: *//p')"
	echo "kernel: $(uname -r)"
	echo "gcc: $(gcc --version | head -1)"
	echo "thp: $(cat /sys/kernel/mm/transparent_hugepage/enabled)"
	nproc
} >"$R/env.txt"

# Timing: ops/shapes/intset microbenchmarks, alternating base and new.
port_of() { if [ "$1" = base ]; then echo 54401; else echo 54402; fi; }
for round in 1 2 3; do
	if [ $((round % 2)) -eq 1 ]; then order="base new"; else order="new base"; fi
	for side in $order; do
		PIN="numactl --physcpubind=2 --membind=0" DATA="$W/pgdata-ops-$side" \
			"$W/src-new/tools-census/ops_bench.sh" "$W/inst-$side" \
			"$(port_of "$side")" "$R/ops-$side-$round.csv"
	done
done

# End to end: GiST VACUUM page sets, IntegerSet versus sbm, on the new tree.
PIN="numactl --physcpubind=2 --membind=0" DATA="$W/pgdata-gist" \
	"$W/src-new/tools-census/gist_vacuum_ab.sh" "$W/inst-new" 54403 \
	"$R/gist.csv" 4000000 6

echo finished >"$R/DONE"
