#!/usr/bin/env bash
#
# mtpg_profile.sh -- locate the threaded/pooled per-command overhead.
#
# Phase 18's premise is that the ~35% threaded-vs-process gap is "per-command
# scheduling + current-work (TLS bridge) indirection layered on top of libxtc"
# (plan_docs/MULTITHREADED_PLAN.md).  That is a HYPOTHESIS; this profiles it so
# the first fusion adoption targets the real hotspot instead of a guess.
#
# For each lane (process, threaded-pooled) it runs a CPU-bound prepared-SELECT
# pgbench workload under `perf record` and emits a `perf report` symbol summary.
# Compare the two: symbols that dominate the pooled lane but not the process lane
# ARE the per-command threaded overhead (expect TLS-bridge accessors, the
# protocol/scheduler step loop, and any duplicated locking if the hypothesis
# holds).
#
# Usage:
#   mtpg_profile.sh --install=DIR [--clients=N] [--duration=SEC] [--out=DIR]
#     --install=DIR   tmp_install of the build to profile (same build, both lanes)
#
# Run on a bare-metal-ish box (EC2 m6id.8xlarge); needs `perf` + kernel
# perf_event_paranoid <= 1.  ponytail: plain perf record + report, no custom
# profiler; the only logic is lane setup, which mirrors the A/B harness.
set -euo pipefail

install="" clients=16 duration=30 out="/tmp/mtpg_profile_$(date +%Y%m%d_%H%M%S)"
for a in "$@"; do
	case "$a" in
		--install=*)  install="${a#*=}" ;;
		--clients=*)  clients="${a#*=}" ;;
		--duration=*) duration="${a#*=}" ;;
		--out=*)      out="${a#*=}" ;;
		*) echo "unknown arg: $a" >&2; exit 2 ;;
	esac
done
[[ -n "$install" ]] || { echo "usage: mtpg_profile.sh --install=DIR [--clients=N] [--duration=SEC] [--out=DIR]" >&2; exit 2; }

pgroot="$install/usr/local/pgsql"
[[ -x "$pgroot/bin/postgres" ]] || pgroot="$install"
[[ -x "$pgroot/bin/postgres" ]] || { echo "no postgres under $install" >&2; exit 2; }
export PATH="$pgroot/bin:$PATH"
export LD_LIBRARY_PATH="$pgroot/lib:$pgroot/lib64:${LD_LIBRARY_PATH:-}"
mkdir -p "$out"

command -v perf >/dev/null || { echo "perf not installed" >&2; exit 2; }
if [[ "$(cat /proc/sys/kernel/perf_event_paranoid 2>/dev/null || echo 3)" -gt 1 ]]; then
	echo "note: set kernel.perf_event_paranoid<=1 (sudo sysctl -w kernel.perf_event_paranoid=1)" >&2
fi

run_lane() {
	local lane="$1" mt="$2"
	local d="$out/$lane"
	local pgdata="$d/pgdata"
	rm -f "$out/.stamp" 2>/dev/null || true
	mkdir -p "$d"
	initdb -D "$pgdata" -U postgres --locale=C >/dev/null 2>&1
	{
		echo "unix_socket_directories = '$d'"
		echo "listen_addresses = ''"
		echo "shared_buffers = 1GB"
		echo "max_connections = 200"
		echo "fsync = off"                     # profiling CPU path, not IO
		echo "multithreaded = $mt"
	} >> "$pgdata/postgresql.conf"
	postgres -D "$pgdata" > "$d/pm.log" 2>&1 &
	local pmpid=$!
	local up=0 i
	for i in $(seq 1 30); do
		psql -X -h "$d" -U postgres -tAc "SELECT 1" >/dev/null 2>&1 && { up=1; break; }
		kill -0 "$pmpid" 2>/dev/null || break
		sleep 1
	done
	if [[ "$up" != 1 ]]; then
		echo "[$lane] server failed to start" >&2
		tail -5 "$d/pm.log" >&2
		kill -9 "$pmpid" 2>/dev/null || true
		return 1
	fi
	pgbench -i -s 20 -h "$d" -U postgres postgres >/dev/null 2>&1
	# CPU-bound prepared SELECT; profile the whole server tree during it.
	perf record -F 999 -g -o "$d/perf.data" -p "$pmpid" -- \
		pgbench -n -M prepared -S -c "$clients" -j "$clients" -T "$duration" \
			-h "$d" -U postgres postgres > "$d/pgbench.out" 2>&1 || true
	grep -E 'tps|latency' "$d/pgbench.out" | sed "s/^/[$lane] /"
	perf report -i "$d/perf.data" --stdio --sort=overhead,symbol 2>/dev/null \
		| grep -vE '^#|^\s*$' | head -40 > "$d/perf.symbols.txt" || true
	echo "[$lane] top symbols -> $d/perf.symbols.txt"
	kill -TERM "$pmpid" 2>/dev/null || true; sleep 2; kill -9 "$pmpid" 2>/dev/null || true
	ipcrm --all=shm 2>/dev/null || true
}

echo "== profiling: install=$install clients=$clients duration=${duration}s out=$out =="
run_lane process off
run_lane threaded_pooled on

echo
echo "== compare the two symbol lists to find per-command threaded overhead: =="
echo "   diff <(head -25 $out/process/perf.symbols.txt) <(head -25 $out/threaded_pooled/perf.symbols.txt)"
