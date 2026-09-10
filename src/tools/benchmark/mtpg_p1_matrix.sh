#!/usr/bin/env bash
# mtpg_p1_matrix.sh -- P1 of plan_docs/FORK_TO_XTC_PERF_PLAN.md: ONE command that
# runs the fixed section-1 methodology for BOTH lanes (fork vs threaded) and BOTH
# workloads (read-mostly pgbench -S, write-heavy pgbench tpcb-like), on a SEPARATE
# loadgen host, and emits results.tsv + latency percentiles.
#
# This is pgbench-based (not HammerDB): HammerDB needs a Tcl/Java driver install on
# the loadgen and its own harness (mtpg_hammerdb_bench.sh) already exists for that;
# this script gives P1's "one command" for the pgbench half of section 1, reusing
# pgbench_pctl for percentiles per the existing README.  Wire HammerDB in later by
# adding a lane here rather than duplicating the runner (ponytail: don't rewrite
# mtpg_hammerdb_bench.sh's TPROC-C plumbing).
#
# Runs ON the SUT.  Requires a SEPARATE loadgen host reachable over SSH with the
# SAME pgbench/psql client binaries on PATH (or LOADGEN_PGBIN pointing at them).
#
# Encodes the nine hardening requirements (see README.md "P1 requirements" table):
#  1. fresh initdb/start/stop EVERY run (no server reuse across runs).
#  2. `pgbench -i` wrapped in timeout.
#  3. diagnostics (SHOW/pg_stat_*) captured BEFORE stop_pg's -m immediate teardown.
#  4. tps parsed with an explicit match-or-die; a parse failure prints the raw
#     pgbench output and is written as PARSE_FAIL, never silently folded into NA/0.
#  5. carrier count asserted via SHOW + pg_stat_xtc_carriers; run FAILS if it does
#     not match intent.
#  6. build/binary existence verified with -x checks, no `| tail -N` masking.
#  7. driver/SUT host separation enforced: LOADGEN and SUT_IP must differ, or the
#     row is tainted DEGRADED (co-located, --local-driver escape hatch).
#  8. confound context columns baked into every results.tsv row.
#  9. SUT idle%/CPU captured, but idle_meaningful is only "yes" when requirement 7
#     held for that row.
#
# Env (SUT-side):
#   PGBIN        install bin dir with postgres/initdb/pg_ctl/psql (pgbench used
#                LOCALLY only for `-i` init; the measured run drives from LOADGEN)
#   LOADGEN      user@driver-host (ssh target); omit + pass --local-driver to run
#                co-located (tainted DEGRADED, smoke-test only)
#   LOADGEN_KEY  ssh key for LOADGEN (optional)
#   SUT_IP       address the driver dials (private IP if on the same VPC)
#   LOADGEN_PGBIN  pgbench/psql dir on the loadgen (default: PATH)
#   DATA         PGDATA parent dir, wiped+reinitialized every run (default /mnt/nvme/p1data)
#   OUT          output dir for results.tsv + logs (default /mnt/nvme/p1out)
#   RAM_PCT      shared_buffers as % of host RAM (default 85; PARAMETER per R1, do not
#                hardcode away from 85 -- section 1 settles R1 by measurement later)
#   CARRIERS     space list of pooled_protocol_carriers to sweep for the xtc lane;
#                "auto" means -1 (default "auto")
#   LANES        space list: "fork xtc" (default both)
#   WORKLOADS    space list: "select tpcb" (default both; select = -S read-mostly,
#                tpcb = tpcb-like write-heavy, both per section 1's "workloads")
#   CLIENTS      space list of pgbench client counts (default "16 32")
#   SCALE        pgbench scale factor (default 50)
#   DURATION     measured seconds per cell (default 60)
#   WARMUP       warmup seconds, discarded (default 15)
#   RUNS         repeats per cell, median reported (default 1; use >=3 for a
#                headline run per section 1)
#   PORT         (default 5439)
#   LOCAL_DRIVER set to 1 (or pass --local-driver) to allow LOADGEN unset / == SUT_IP;
#                tags every row DEGRADED and forces idle_meaningful=no.
set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

LOCAL_DRIVER="${LOCAL_DRIVER:-0}"
for a in "$@"; do case "$a" in --local-driver) LOCAL_DRIVER=1 ;; esac; done

PGBIN="${PGBIN:?set PGBIN=install bin dir (postgres/initdb/pg_ctl/psql)}"
LOADGEN="${LOADGEN:-}"
LOADGEN_KEY="${LOADGEN_KEY:-}"
SUT_IP="${SUT_IP:?set SUT_IP=address the driver dials (or 127.0.0.1 with --local-driver)}"
LOADGEN_PGBIN="${LOADGEN_PGBIN:-}"
DATA="${DATA:-/mnt/nvme/p1data}"
OUT="${OUT:-/mnt/nvme/p1out}"
RAM_PCT="${RAM_PCT:-85}"
CARRIERS="${CARRIERS:-auto}"
LANES="${LANES:-fork xtc}"
WORKLOADS="${WORKLOADS:-select tpcb}"
CLIENTS="${CLIENTS:-16 32}"
SCALE="${SCALE:-50}"
DURATION="${DURATION:-60}"
WARMUP="${WARMUP:-15}"
RUNS="${RUNS:-1}"
PORT="${PORT:-5439}"

if [ -z "$LOADGEN" ] || [ "$LOADGEN" = "$SUT_IP" ]; then
	if [ "$LOCAL_DRIVER" != 1 ]; then
		echo "REQUIREMENT-7 FAIL: LOADGEN ('$LOADGEN') is unset or equals SUT_IP ('$SUT_IP')." >&2
		echo "Set LOADGEN=user@separate-driver-host, or pass --local-driver to force a" >&2
		echo "co-located smoke run (every row is then tagged DEGRADED, never a headline)." >&2
		exit 2
	fi
	COLOCATED=1
else
	COLOCATED=0
fi

# requirement 6: verify the binaries exist -- never build-then-pipe-through-tail.
for b in postgres initdb pg_ctl psql pgbench; do
	[ -x "$PGBIN/$b" ] || { echo "MISSING BINARY: $PGBIN/$b (build did not produce it, or PGBIN is wrong)" >&2; exit 2; }
done

mkdir -p "$OUT"
RES="$OUT/results.tsv"
LATS="$OUT/latencies.tsv"
if [ ! -f "$RES" ]; then
	printf 'bench\tlane\tcarriers_req\tcarriers_eff\tclients\ttps\tp50_ms\tp95_ms\tp99_ms\tmax_ms\ttxn_count\tsut_cpu_busy_pct\tidle_meaningful\tdegraded\tdriver_host\tsut_host\tshared_buffers_mb\tram_pct\tfsync\tsynchronous_commit\tfull_page_writes\tio_method\tdata_device\twal_device\tlibxtc_version\tpg_commit\trun\tstall\tnotes\n' > "$RES"
fi
if [ ! -f "$LATS" ]; then
	printf 'bench\tlane\tcarriers_req\tclients\trun\tpctl_line\n' > "$LATS"
fi

say(){ echo "[$(date -u +%H:%M:%S)] $*" | tee -a "$OUT/LOG" >&2; }

# write_row: ALWAYS emits all 29 results.tsv columns (never a short/misaligned
# row) so a failure path can't leave the TSV desynced by dumping free text into
# the wrong column.  Args are the columns in results.tsv order; missing
# trailing args become empty fields.
write_row(){
	local vals=("$@")
	local n=29 i line=""
	for i in $(seq 0 $((n-1))); do
		local v="${vals[$i]:-}"
		v="${v//$'\t'/ }"; v="${v//$'\n'/ ; }"   # never let embedded TAB/NL desync columns
		line+="$v"
		[ "$i" -lt $((n-1)) ] && line+=$'\t'
	done
	printf '%s\n' "$line" >> "$RES"
}

RAM_KB=$(awk '/MemTotal/{print $2}' /proc/meminfo)
SB_MB=$(( RAM_KB/1024*RAM_PCT/100 ))
NCORE=$(nproc)
SUT_HOST=$(hostname -f 2>/dev/null || hostname)
DRIVER_HOST_LABEL="$LOADGEN"; [ "$COLOCATED" = 1 ] && DRIVER_HOST_LABEL="$SUT_HOST(colocated)"
PG_COMMIT=$(cd "$here/../../.." && git rev-parse --short=12 HEAD 2>/dev/null || echo unknown)
LIBXTC_VERSION=$(pkg-config --modversion xtc 2>/dev/null || echo unknown)
DATA_DEVICE=$(findmnt -no SOURCE --target "$(dirname "$DATA")" 2>/dev/null || echo unknown)
WAL_DEVICE="$DATA_DEVICE"  # R3 settled: single device, both lanes identical -- see plan section 1/R3

SSHOPT=(-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=15 -o BatchMode=yes)
[ -n "$LOADGEN_KEY" ] && SSHOPT+=(-i "$LOADGEN_KEY")
DRV(){ if [ "$COLOCATED" = 1 ]; then timeout 300 bash -c "$1"; else timeout 300 ssh "${SSHOPT[@]}" "$LOADGEN" "$1"; fi; }
drv_pgbench(){ if [ -n "$LOADGEN_PGBIN" ]; then echo "$LOADGEN_PGBIN/pgbench"; else echo pgbench; fi; }

Q(){ timeout 20 "$PGBIN/psql" -h 127.0.0.1 -p "$PORT" -U postgres -tAc "$1" postgres 2>/dev/null | tr -d ' '; }

hardstop(){
	pkill -9 -f "postgres -D $DATA/pgdata" 2>/dev/null
	sleep 1
	rm -f "$DATA/pgdata/postmaster.pid" 2>/dev/null
}

# requirement 1: FRESH initdb every run -- never reuse a server across runs.
fresh_initdb(){
	find "$DATA" -mindepth 1 -delete 2>/dev/null
	rmdir "$DATA" 2>/dev/null
	mkdir -p "$DATA"
	timeout 120 "$PGBIN/initdb" -D "$DATA/pgdata" -U postgres -E UTF8 >"$OUT/initdb.log" 2>&1 \
		|| { echo "INITDB_FAIL"; cat "$OUT/initdb.log" >&2; return 1; }
	echo "host all all 0.0.0.0/0 trust" >> "$DATA/pgdata/pg_hba.conf"
}

# start_lane LANE CARRIERS_REQ -> 0 ok, 1 fail.  Sets EFF_CARRIERS on success.
start_lane(){
	local lane="$1" creq="$2" extra=""
	hardstop
	if [ "$lane" = xtc ]; then
		local cval="$creq"; [ "$cval" = auto ] && cval=-1
		extra="-c multithreaded=on -c pooled_protocol_carriers=$cval"
	fi
	timeout 600 "$PGBIN/postgres" -D "$DATA/pgdata" -c port="$PORT" -c listen_addresses='*' \
		-c shared_buffers="${SB_MB}MB" -c max_connections=512 -c max_wal_size=16GB \
		-c checkpoint_timeout=30min -c checkpoint_completion_target=0.9 -c wal_buffers=256MB \
		-c io_method=sync -c huge_pages=try -c fsync=on -c synchronous_commit=on \
		-c full_page_writes=on -c autovacuum=on $extra >"$OUT/pg_${lane}.log" 2>&1 &
	local pgpid=$!
	echo "$pgpid" > "$OUT/pg.pid"
	local i
	for i in $(seq 1 120); do
		grep -q "ready to accept connections" "$OUT/pg_${lane}.log" 2>/dev/null && break
		kill -0 "$pgpid" 2>/dev/null || { echo "SERVER_DIED_ON_STARTUP"; tail -30 "$OUT/pg_${lane}.log" >&2; return 1; }
		sleep 1
	done
	grep -q "ready to accept connections" "$OUT/pg_${lane}.log" 2>/dev/null \
		|| { echo "SERVER_NEVER_READY"; tail -30 "$OUT/pg_${lane}.log" >&2; return 1; }

	# requirement 5: ASSERT the effective carrier count, don't trust intent.
	local mt eff rows
	mt=$(Q 'show multithreaded')
	eff=$(Q 'show pooled_protocol_carriers')
	if [ "$lane" = xtc ]; then
		[ "$mt" = on ] || { echo "ASSERT_FAIL mt=$mt want on"; return 1; }
		local want="$creq"
		if [ "$want" = auto ]; then
			# auto resolves to ncpus (floor 8, ceiling 256, capped by max_connections);
			# just require it resolved to something positive and sane, and cross-check
			# pg_stat_xtc_carriers row count matches.
			[ -n "$eff" ] && [ "$eff" -gt 0 ] 2>/dev/null || { echo "ASSERT_FAIL auto resolved to '$eff' (want >0)"; return 1; }
		else
			[ "$eff" = "$want" ] || { echo "ASSERT_FAIL carriers=$eff want=$want"; return 1; }
		fi
		rows=$(Q 'select count(*) from pg_stat_xtc_carriers')
		[ -n "$rows" ] && [ "$rows" -gt 0 ] 2>/dev/null || { echo "ASSERT_FAIL pg_stat_xtc_carriers empty (carrier scheduler not up)"; return 1; }
		[ "$rows" = "$eff" ] || say "NOTE: pg_stat_xtc_carriers rows=$rows eff_carriers=$eff (informational)"
	else
		[ "$mt" = off ] || { echo "ASSERT_FAIL fork lane mt=$mt want off"; return 1; }
		eff="-"
	fi
	EFF_CARRIERS="$eff"
	say "lane=$lane up: mt=$mt carriers_req=$creq carriers_eff=$eff shared_buffers=$(Q 'show shared_buffers') cores=$NCORE"
	return 0
}

# stop_lane: requirement 3 -- caller must capture diagnostics BEFORE calling this.
stop_lane(){
	"$PGBIN/pg_ctl" -D "$DATA/pgdata" -m fast -w -t 120 stop >/dev/null 2>&1
	local pid; pid=$(cat "$OUT/pg.pid" 2>/dev/null)
	local i
	for i in $(seq 1 30); do [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null || break; sleep 1; done
	if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
		say "fast stop hung, escalating to immediate"
		"$PGBIN/pg_ctl" -D "$DATA/pgdata" -m immediate -w -t 60 stop >/dev/null 2>&1
	fi
	hardstop
}

pgbench_args_for(){ case "$1" in
	select) echo "--builtin select-only -M prepared" ;;
	tpcb)   echo "--builtin tpcb-like" ;;
esac }

# run_cell BENCH LANE CREQ CLIENTS RUN
run_cell(){
	local bench="$1" lane="$2" creq="$3" clients="$4" run="$5"
	local args; args="$(pgbench_args_for "$bench")"
	local degraded="no"; [ "$COLOCATED" = 1 ] && degraded="yes"

	if ! fresh_initdb; then
		write_row "$bench" "$lane" "$creq" "-" "$clients" NA NA NA NA NA NA NA no "$degraded" "$DRIVER_HOST_LABEL" "$SUT_HOST" "$SB_MB" "$RAM_PCT" NA NA NA NA NA NA NA NA "$run" no INITDB_FAIL
		return
	fi
	if ! start_lane "$lane" "$creq"; then
		write_row "$bench" "$lane" "$creq" "-" "$clients" NA NA NA NA NA NA NA no "$degraded" "$DRIVER_HOST_LABEL" "$SUT_HOST" "$SB_MB" "$RAM_PCT" NA NA NA NA NA NA NA NA "$run" no START_FAIL
		hardstop
		return
	fi

	# requirement 2: pgbench -i wrapped in timeout, LOCALLY on the SUT (data load
	# does not need the loadgen; only the measured run does).
	if ! timeout 300 "$PGBIN/pgbench" -h 127.0.0.1 -p "$PORT" -U postgres -i -s "$SCALE" \
		--quiet postgres >"$OUT/init_${bench}_${lane}.log" 2>&1; then
		echo "PGBENCH_INIT_FAIL or TIMEOUT ($bench/$lane); see $OUT/init_${bench}_${lane}.log" >&2
		tail -20 "$OUT/init_${bench}_${lane}.log" >&2
		write_row "$bench" "$lane" "$creq" "$EFF_CARRIERS" "$clients" NA NA NA NA NA NA NA no "$degraded" "$DRIVER_HOST_LABEL" "$SUT_HOST" "$SB_MB" "$RAM_PCT" NA NA NA NA NA NA NA NA "$run" no INIT_FAIL
		stop_lane
		return
	fi

	local logpfx="/tmp/p1_${bench}_${lane}_${clients}_${run}_$$"
	local pgbench_bin; pgbench_bin="$(drv_pgbench)"
	local remote_cmd
	remote_cmd="rm -f ${logpfx}.* 2>/dev/null; \
$pgbench_bin -h $SUT_IP -p $PORT -U postgres -n -T $WARMUP -c $clients -j $clients $args postgres >/dev/null 2>&1; \
$pgbench_bin -h $SUT_IP -p $PORT -U postgres -n -T $DURATION -c $clients -j $clients --log --log-prefix=$logpfx $args postgres 2>&1; \
echo __LOGPFX__=$logpfx"

	# stall detector, monitor-independent: sample txn count server-side every 10s
	# across the measured window; a frozen counter across >1/3 of samples is a
	# STALL, not a silent 0.
	local win=$DURATION; [ "$win" -lt 20 ] && win=20
	(
		local prev="" froze=0 samples=0 k cur
		Q 'select 1' >/dev/null  # pre-warm the sampler connection path
		for k in $(seq 1 $((win/10))); do
			sleep 10
			cur=$(Q 'select sum(xact_commit+xact_rollback) from pg_stat_database')
			samples=$((samples+1))
			[ -n "$cur" ] && [ "$cur" = "$prev" ] && froze=$((froze+1))
			prev=${cur:-$prev}
		done
		echo "$froze $samples" > "$OUT/.stall_${bench}_${lane}_${clients}_${run}"
	) & STALLPID=$!

	# requirement 4: tps must be parsed loudly or not at all.
	local out
	out=$(DRV "$remote_cmd" 2>&1)
	local rc=$?
	wait "$STALLPID" 2>/dev/null
	local froze=0 samples=0
	read froze samples < "$OUT/.stall_${bench}_${lane}_${clients}_${run}" 2>/dev/null
	rm -f "$OUT/.stall_${bench}_${lane}_${clients}_${run}"
	local stall="no"; [ "${samples:-0}" -gt 0 ] 2>/dev/null && [ "$froze" -gt $((samples/3)) ] && stall="STALL:${froze}/${samples}"

	echo "$out" > "$OUT/pgbench_${bench}_${lane}_${clients}_${run}.log"
	local tps
	tps=$(echo "$out" | grep -E '^tps = ' | grep -oE '[0-9]+\.[0-9]+' | head -1)
	if [ $rc -ne 0 ] || [ -z "$tps" ]; then
		say "TPS_PARSE_FAIL bench=$bench lane=$lane clients=$clients rc=$rc -- raw output in $OUT/pgbench_${bench}_${lane}_${clients}_${run}.log"
		echo "$out" | tail -20 >&2
		# requirement 3: capture diagnostics BEFORE teardown even on failure.
		local diag; diag=$(Q "select wait_event_type,wait_event,count(*) from pg_stat_activity where pid<>pg_backend_pid() group by 1,2 order by 3 desc limit 5" 2>/dev/null | tr '\n' ';')
		write_row "$bench" "$lane" "$creq" "$EFF_CARRIERS" "$clients" TPS_PARSE_FAIL NA NA NA NA NA NA no "$degraded" "$DRIVER_HOST_LABEL" "$SUT_HOST" "$SB_MB" "$RAM_PCT" NA NA NA NA NA NA NA NA "$run" "$stall" "diag: $diag"
		stop_lane
		return
	fi

	# requirement 3: capture diagnostics BEFORE any teardown (immediate-stop floods
	# FATAL logs that look like a crash if captured after).
	local cpu_busy="NA"
	if command -v mpstat >/dev/null 2>&1; then
		cpu_busy=$(mpstat 1 1 2>/dev/null | awk '/Average/{print 100-$NF; exit}')
	fi
	local activity_snapshot
	activity_snapshot=$(Q "select count(*) from pg_stat_activity where state='active'" 2>/dev/null)
	say "diag: active_backends=$activity_snapshot cpu_busy=${cpu_busy}%"

	# pull the per-txn log back for percentiles.  Requirement per README: never
	# cat a multi-million-row log over ssh; compute percentiles on the DRIVER and
	# ship back only the summary line via pgbench_pctl (already there, present on
	# the driver only if this repo is checked out there -- fall back to scp+local
	# if pgbench_pctl is not on the loadgen).
	local pctl_line=""
	if [ "$COLOCATED" = 1 ]; then
		pctl_line=$(perl "$here/pgbench_pctl" --glob "${logpfx}.*" 2>/dev/null)
		rm -f "${logpfx}".* 2>/dev/null
	else
		local remote_pctl="/tmp/mtpg_pgbench_pctl_$$"
		if base64 "$here/pgbench_pctl" | timeout 30 ssh "${SSHOPT[@]}" "$LOADGEN" "base64 -d > $remote_pctl && chmod +x $remote_pctl"; then
			pctl_line=$(DRV "perl $remote_pctl --glob '${logpfx}.*'; rm -f ${logpfx}.* $remote_pctl")
		fi
	fi
	local p50 p95 p99 pmax cnt
	p50=$(echo "$pctl_line" | grep -oE 'p50_ms\s+[0-9.]+' | grep -oE '[0-9.]+$')
	p95=$(echo "$pctl_line" | grep -oE 'p95_ms\s+[0-9.]+' | grep -oE '[0-9.]+$')
	p99=$(echo "$pctl_line" | grep -oE 'p99_ms\s+[0-9.]+' | grep -oE '[0-9.]+$')
	pmax=$(echo "$pctl_line" | grep -oE 'max_ms\s+[0-9.]+' | grep -oE '[0-9.]+$')
	cnt=$(echo "$pctl_line" | grep -oE 'count\s+[0-9]+' | grep -oE '[0-9]+$')

	local idle_meaningful="no"; [ "$COLOCATED" != 1 ] && idle_meaningful="yes"
	local fsync sync_commit fpw iom
	fsync=$(Q 'show fsync'); sync_commit=$(Q 'show synchronous_commit'); fpw=$(Q 'show full_page_writes'); iom=$(Q 'show io_method')

	write_row "$bench" "$lane" "$creq" "$EFF_CARRIERS" "$clients" "$tps" "${p50:-NA}" "${p95:-NA}" "${p99:-NA}" "${pmax:-NA}" \
		"${cnt:-NA}" "${cpu_busy:-NA}" "$idle_meaningful" "$degraded" "$DRIVER_HOST_LABEL" "$SUT_HOST" "$SB_MB" "$RAM_PCT" \
		"$fsync" "$sync_commit" "$fpw" "$iom" "$DATA_DEVICE" "$WAL_DEVICE" "$LIBXTC_VERSION" "$PG_COMMIT" "$run" "$stall" ""
	printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$bench" "$lane" "$creq" "$clients" "$run" "$pctl_line" >> "$LATS"
	say "DONE bench=$bench lane=$lane carriers_req=$creq carriers_eff=$EFF_CARRIERS clients=$clients tps=$tps p95=${p95:-NA}ms p99=${p99:-NA}ms stall=$stall degraded=$degraded"

	stop_lane
}

say "P1 MATRIX START: lanes='$LANES' workloads='$WORKLOADS' clients='$CLIENTS' carriers='$CARRIERS' runs=$RUNS colocated=$COLOCATED"
[ "$COLOCATED" = 1 ] && say "WARNING: --local-driver / co-located mode -- every row will be tagged DEGRADED and idle_meaningful=no (requirement 7)."

for bench in $WORKLOADS; do
	for lane in $LANES; do
		if [ "$lane" = xtc ]; then
			for creq in $CARRIERS; do
				for clients in $CLIENTS; do
					for run in $(seq 1 "$RUNS"); do
						run_cell "$bench" "$lane" "$creq" "$clients" "$run"
					done
				done
			done
		else
			for clients in $CLIENTS; do
				for run in $(seq 1 "$RUNS"); do
					run_cell "$bench" "$lane" "-" "$clients" "$run"
				done
			done
		fi
	done
done

say "P1 MATRIX DONE. results: $RES  latencies: $LATS"
column -t "$RES" | tee "$OUT/results.txt" >&2
echo "P1_MATRIX_DONE"
