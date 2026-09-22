#!/usr/bin/env bash
# P1 pgbench matrix. Run ON the SUT; measured traffic comes from LOADGEN.
# fork = this branch with multithreaded=off, NOT stock PostgreSQL.
# Primary lanes: stock (explicit upstream merge-base install), fork, fiber.
# xtc retains the old stackless-pool interface for diagnostic use only.
# See README.md for methodology, prerequisites, artifact layout and limitations.
#
# Existing env: PGBIN, LOADGEN, LOADGEN_KEY, SUT_IP, LOADGEN_PGBIN, DATA, OUT,
# RAM_PCT, CARRIERS (legacy xtc lane), LANES, WORKLOADS, CLIENTS, SCALE,
# DURATION, WARMUP, RUNS, PORT, PG_COMMIT, LOCAL_DRIVER / --local-driver.
# Added: STOCK_PGBIN, STOCK_COMMIT (required for stock), FIBER_LOOPS (default:
# min(online CPUs,256)), TIMEOUT_GRACE (seconds beyond each pgbench duration).
set -uo pipefail
export LC_ALL=C
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOCAL_DRIVER="${LOCAL_DRIVER:-0}"
for a in "$@"; do
	case "$a" in --local-driver) LOCAL_DRIVER=1;; *) echo "Unknown argument: $a" >&2; exit 2;; esac
done
PGBIN="${PGBIN:?set PGBIN=branch install bin directory}"
LOADGEN="${LOADGEN:-}"
LOADGEN_KEY="${LOADGEN_KEY:-}"
SUT_IP="${SUT_IP:?set SUT_IP=address the driver dials}"
LOADGEN_PGBIN="${LOADGEN_PGBIN:-}"
DATA="${DATA:-/mnt/nvme/p1data}"
OUT="${OUT:-/mnt/nvme/p1out}"
RAM_PCT="${RAM_PCT:-85}"
CARRIERS="${CARRIERS:-auto}"
LANES="${LANES:-fork fiber}"
WORKLOADS="${WORKLOADS:-select tpcb}"
CLIENTS="${CLIENTS:-16 32}"
SCALE="${SCALE:-50}"
DURATION="${DURATION:-60}"
WARMUP="${WARMUP:-15}"
RUNS="${RUNS:-1}"
PORT="${PORT:-5439}"
TIMEOUT_GRACE="${TIMEOUT_GRACE:-60}"
NCORE=$(getconf _NPROCESSORS_ONLN)
FIBER_LOOPS="${FIBER_LOOPS:-$((NCORE < 256 ? NCORE : 256))}"
STOCK_PGBIN="${STOCK_PGBIN:-}"
STOCK_COMMIT="${STOCK_COMMIT:-}"
for n in "$RAM_PCT" "$SCALE" "$DURATION" "$RUNS" "$PORT" "$TIMEOUT_GRACE" "$FIBER_LOOPS" $CLIENTS; do
	[[ "$n" =~ ^[1-9][0-9]*$ ]] || { echo "Invalid positive integer: $n" >&2; exit 2; }
done
[[ "$WARMUP" =~ ^(0|[1-9][0-9]*)$ ]] || { echo 'Invalid WARMUP' >&2; exit 2; }
# A single executor loop has no pg_stat_xtc_carriers rows, so cannot be
# verified by this harness. Refuse it rather than claiming a verified count.
if ((RAM_PCT > 95 || PORT > 65535 || FIBER_LOOPS < 2 || FIBER_LOOPS > 1024)); then
	echo 'Require RAM_PCT<=95, PORT<=65535, and FIBER_LOOPS=2..1024' >&2; exit 2
fi
for lane in $LANES; do
	case "$lane" in
		fork|fiber|xtc) ;;
		stock) if [ -z "$STOCK_PGBIN" ] || [ -z "$STOCK_COMMIT" ]; then echo 'stock requires STOCK_PGBIN and STOCK_COMMIT' >&2; exit 2; fi;;
		*) echo "Unknown lane: $lane" >&2; exit 2;;
	esac
done
for bench in $WORKLOADS; do
	case "$bench" in select|tpcb) ;; *) echo "Unknown workload: $bench" >&2; exit 2;; esac
done
for c in $CARRIERS; do
	[[ "$c" = auto || "$c" =~ ^(0|[1-9][0-9]*)$ ]] || { echo "Invalid CARRIERS: $c" >&2; exit 2; }
done
SSHOPT=(-o BatchMode=yes -o ConnectTimeout=15)
[ -n "$LOADGEN_KEY" ] && SSHOPT+=(-i "$LOADGEN_KEY")

# Strip SSH users before comparison. Also compare kernel boot identities over
# SSH: DNS aliases, public/private addresses and SSH Host aliases are not proof
# of separate machines. Failure to prove separation is fatal, not a green row.
COLOCATED=0
if [ "$LOCAL_DRIVER" = 1 ]; then
	COLOCATED=1
else
	if [ -z "$LOADGEN" ] || [ "${LOADGEN##*@}" = "${SUT_IP##*@}" ]; then
		echo 'REQUIREMENT-7 FAIL: unset or same-address LOADGEN; use a separate driver' >&2; exit 2
	fi
	sut_id=$(cat /proc/sys/kernel/random/boot_id) || exit 2
	driver_id=$(timeout -k 5 30 ssh "${SSHOPT[@]}" "$LOADGEN" 'cat /proc/sys/kernel/random/boot_id')
	rc=$?
	if [ "$rc" != 0 ] || [ -z "$driver_id" ] || [ "$driver_id" = "$sut_id" ]; then
		echo "REQUIREMENT-7 FAIL: driver identity missing, same SUT, or SSH failed rc=$rc" >&2; exit 2
	fi
fi
for lane in $LANES; do
	bin="$PGBIN"; [ "$lane" = stock ] && bin="$STOCK_PGBIN"
	for b in postgres initdb pg_ctl psql pgbench; do
		[ -x "$bin/$b" ] || { echo "MISSING BINARY: $bin/$b" >&2; exit 2; }
	done
done
# Never wipe output or another cell's artifacts when reusing OUT. DATA is
# destructive scratch space, and must not contain OUT (or vice versa).
DATA=$(realpath -m "$DATA") || exit 2
OUT=$(realpath -m "$OUT") || exit 2
if [[ "$DATA" = / || "$OUT/" = "$DATA/"* || "$DATA/" = "$OUT/"* ]]; then
	echo 'DATA must be dedicated scratch space, disjoint from OUT' >&2; exit 2
fi
if [ -f "$DATA/pgdata/postmaster.pid" ]; then
	echo "Refusing to wipe DATA with postmaster.pid: $DATA; stop/inspect it manually" >&2; exit 2
fi
mkdir -p "$OUT" || exit 2
RES="$OUT/results.tsv"; LATS="$OUT/latencies.tsv"
if [ ! -f "$RES" ]; then
	printf 'bench\tlane\tcarriers_req\tcarriers_eff\tclients\ttps\tp50_ms\tp95_ms\tp99_ms\tmax_ms\ttxn_count\tsut_cpu_busy_pct\tidle_meaningful\tdegraded\tdriver_host\tsut_host\tshared_buffers_mb\tram_pct\tfsync\tsynchronous_commit\tfull_page_writes\tio_method\tdata_device\twal_device\tlibxtc_version\tpg_commit\trun\tstall\tnotes\n' > "$RES"
fi
[ -f "$LATS" ] || printf 'bench\tlane\tcarriers_req\tclients\trun\tpctl_line\n' > "$LATS"
say(){ echo "[$(date -u +%H:%M:%S)] $*" | tee -a "$OUT/LOG" >&2; }
write_row(){
	local vals=("$@") i v line=""
	for ((i=0; i<29; i++)); do
		v="${vals[$i]:-}"; v="${v//$'\t'/ }"; v="${v//$'\n'/ ; }"
		line+="$v"; [ "$i" -eq 28 ] || line+=$'\t'
	done
	printf '%s\n' "$line" >> "$RES" || { echo "RESULT_WRITE_FAIL: $RES" >&2; exit 2; }
}
RAM_KB=$(awk '/MemTotal/{print $2}' /proc/meminfo)
SB_MB=$((RAM_KB*RAM_PCT/1024/100))
SUT_HOST=$(hostname -f 2>/dev/null || hostname)
DRIVER_HOST_LABEL="$LOADGEN"; [ "$COLOCATED" = 1 ] && DRIVER_HOST_LABEL="$SUT_HOST(colocated)"
PG_COMMIT="${PG_COMMIT:-$(git -C "$here" rev-parse HEAD 2>/dev/null || echo unknown)}"
LIBXTC_VERSION=$(pkg-config --modversion xtc 2>/dev/null || echo unknown)
DATA_DEVICE=$(findmnt -no SOURCE --target "$(dirname "$DATA")" 2>/dev/null || echo unknown)
WAL_DEVICE="$DATA_DEVICE"

# Bound commands on BOTH ends of SSH. Killing only ssh can leave pgbench alive.
# Separate warmup/measurement invocations preserve stage and exit status.
DRV(){
	local seconds="${2:-300}" quoted
	if [ "$COLOCATED" = 1 ]; then
		timeout -k 5 "$seconds" bash -c "$1"
	else
		printf -v quoted '%q' "$1"
		timeout -k 5 "$((seconds+20))" ssh "${SSHOPT[@]}" "$LOADGEN" "timeout -k 5 $seconds bash -c $quoted"
	fi
}
# Monitor connections commit in template1, NEVER in the benchmark's postgres
# database. Query only postgres counters; do not sum the observer's commits.
Q(){ timeout -k 5 20 "$PGBIN/psql" -X -v ON_ERROR_STOP=1 -h 127.0.0.1 -p "$PORT" -U postgres -tAc "$1" template1; }
PGPID=""; CPUPID=""; STALLPID=""; DRIVERPID=""
stop_lane(){
	[ -n "$PGPID" ] || return 0
	local rc
	timeout -k 5 130 "$PGBIN/pg_ctl" -D "$DATA/pgdata" -m fast -w -t 120 stop >> "$OUT/stop.log" 2>&1
	rc=$?; echo "fast_stop_rc=$rc" >> "$OUT/stop.log"
	if [ "$rc" != 0 ]; then
		timeout -k 5 70 "$PGBIN/pg_ctl" -D "$DATA/pgdata" -m immediate -w -t 60 stop >> "$OUT/stop.log" 2>&1
		rc=$?; echo "immediate_stop_rc=$rc" >> "$OUT/stop.log"
	fi
	# Never pkill by a path regex or delete live data after failed shutdown.
	# Leave the server/data for manual inspection and abort the matrix.
	if [ "$rc" != 0 ]; then
		say "STOP_FAIL rc=$rc pid=$PGPID; preserving DATA=$DATA; manual cleanup required"
		PGPID=""
		return "$rc"
	fi
	# pg_ctl can observe pidfile removal before the postmaster exits. Bash
	# reaps exited children asynchronously; only wait after the child is gone,
	# so a stuck exit callback cannot wedge this harness indefinitely.
	local i
	for ((i=0; i<30; i++)); do
		kill -0 "$PGPID" 2>/dev/null || break
		sleep 1
	done
	if kill -0 "$PGPID" 2>/dev/null; then
		echo "child_exit_timeout pid=$PGPID rc=124" >> "$OUT/stop.log"
		say "STOP_FAIL rc=124 pid=$PGPID; preserving DATA=$DATA; manual cleanup required"
		PGPID=""
		return 124
	fi
	wait "$PGPID" 2>/dev/null; rc=$?
	echo "child_exit_rc=$rc" >> "$OUT/stop.log"
	PGPID=""
	return "$rc"
}
# Called by the EXIT trap, including exits from within run_cell.
# shellcheck disable=SC2317
cleanup(){
	local p
	for p in "$CPUPID" "$STALLPID" "$DRIVERPID"; do
		[ -z "$p" ] || kill "$p" 2>/dev/null || :
	done
	stop_lane
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
fresh_initdb(){
	mkdir -p "$DATA" && find "$DATA" -mindepth 1 -delete || return 1
	timeout -k 5 120 "$PGBIN/initdb" -D "$DATA/pgdata" -U postgres -E UTF8 || return $?
	echo 'host all all 0.0.0.0/0 trust' >> "$DATA/pgdata/pg_hba.conf"
}
start_lane(){
	local lane="$1" creq="$2" mt eff rows want i
	local extra=()
	case "$lane" in
		fork) extra=(-c multithreaded=off);;
		fiber) extra=(-c multithreaded=on -c pooled_protocol_carriers=0);;
		xtc) extra=(-c multithreaded=on -c "pooled_protocol_carriers=${creq/auto/-1}");;
	esac
	local cmd=("$PGBIN/postgres" -D "$DATA/pgdata" -c port="$PORT" -c listen_addresses='*'
		-c shared_buffers="${SB_MB}MB" -c max_connections=512 -c max_wal_size=16GB
		-c checkpoint_timeout=30min -c checkpoint_completion_target=0.9 -c wal_buffers=256MB
		-c io_method=sync -c huge_pages=try -c fsync=on -c synchronous_commit=on
		-c full_page_writes=on -c autovacuum=on "${extra[@]}")
	printf '%q ' "${cmd[@]}" > "$OUT/server-command.txt"; echo >> "$OUT/server-command.txt"
	# No fixed server lifetime: the bounded stages and EXIT trap own teardown.
	PG_XTC_CARRIER_LOOPS="$FIBER_LOOPS" "${cmd[@]}" > "$OUT/server.log" 2>&1 & PGPID=$!
	for ((i=0; i<120; i++)); do
		grep -q 'ready to accept connections' "$OUT/server.log" && break
		kill -0 "$PGPID" 2>/dev/null || { echo SERVER_DIED_ON_STARTUP; return 1; }
		sleep 1
	done
	grep -q 'ready to accept connections' "$OUT/server.log" || { echo SERVER_NEVER_READY; return 1; }
	EFF_CARRIERS=-
	[ "$lane" = stock ] && return 0 # upstream does not have branch-only GUCs
	mt=$(Q 'show multithreaded') || return 1
	if [ "$lane" = fork ]; then
		[ "$mt" = off ] || { echo "ASSERT_FAIL mt=$mt want=off"; return 1; }
		return 0
	fi
	[ "$mt" = on ] || { echo "ASSERT_FAIL mt=$mt want=on"; return 1; }
	eff=$(Q 'show pooled_protocol_carriers') || return 1
	want="$creq"
	[ "$lane" = fiber ] && want=0
	if [ "$want" = auto ]; then
		want=$NCORE; [ "$want" -ge 8 ] || want=8; [ "$want" -le 256 ] || want=256
	fi
	[ "$eff" = "$want" ] || { echo "ASSERT_FAIL carriers=$eff want=$want"; return 1; }
	# This view counts executor loops, NOT stackless pooled-protocol carriers.
	# Pin its independent loop budget so comparing unlike pools cannot reject a
	# valid server (nor excuse an actual executor mismatch as informational).
	rows=$(Q 'select count(*) from pg_stat_xtc_carriers') || return 1
	[ "$rows" = "$FIBER_LOOPS" ] || { echo "ASSERT_FAIL executor rows=$rows want=$FIBER_LOOPS"; return 1; }
	EFF_CARRIERS="$eff"; [ "$lane" != fiber ] || EFF_CARRIERS="$rows"
	echo "mt=$mt pooled_protocol_carriers=$eff executor_loops=$rows"
}
FAILED=0
# Uses run_cell's locals to keep all failure rows aligned and retain exact stage.
fail_cell(){
	local reason="$1"
	FAILED=1
	say "$reason; artifacts=$OUT"
	if [ -n "$PGPID" ]; then
		Q "select wait_event_type,wait_event,count(*) from pg_stat_activity where pid<>pg_backend_pid() group by 1,2" > "$OUT/failure-diagnostics.log" 2>&1 || :
	fi
	local stop_rc=0 tps_status=NA
	stop_lane || stop_rc=$?
	[ "$stop_rc" = 0 ] || reason+="; STOP_FAIL rc=$stop_rc"
	[ "$reason" != TPS_PARSE_FAIL ] || tps_status=TPS_PARSE_FAIL
	write_row "$bench" "$lane" "$creq" "$EFF_CARRIERS" "$clients" "$tps_status" NA NA NA NA NA NA no "$degraded" "$DRIVER_HOST_LABEL" "$SUT_HOST" "$SB_MB" "$RAM_PCT" NA NA NA NA "$DATA_DEVICE" "$WAL_DEVICE" "$LIBXTC_VERSION" "$PG_COMMIT" "$run" "$stall" "$reason; artifacts=$OUT"
	[ "$stop_rc" = 0 ] || exit 1
}
run_cell(){
	local bench="$1" lane="$2" creq="$3" clients="$4" run="$5"
	local OUT PGBIN="$PGBIN" PG_COMMIT="$PG_COMMIT" LIBXTC_VERSION="$LIBXTC_VERSION"
	OUT=$(mktemp -d "${RES%/*}/${bench}_${lane}_${creq}_${clients}_${run}.XXXXXX") || exit 2
	if [ "$lane" = stock ]; then PGBIN="$STOCK_PGBIN"; PG_COMMIT="$STOCK_COMMIT"; LIBXTC_VERSION=none; fi
	local degraded=no stall=UNKNOWN EFF_CARRIERS=- rc
	[ "$COLOCATED" = 1 ] && degraded=yes
	printf 'PGBIN=%s\nPG_COMMIT=%s\nLOADGEN=%s\nSUT_IP=%s\nWARMUP=%s\nDURATION=%s\nTIMEOUT_GRACE=%s\nFIBER_LOOPS=%s\nPG_XTC_FORCE_LOOP=%s\n' \
		"$PGBIN" "$PG_COMMIT" "$DRIVER_HOST_LABEL" "$SUT_IP" "$WARMUP" "$DURATION" "$TIMEOUT_GRACE" "$FIBER_LOOPS" "${PG_XTC_FORCE_LOOP:-}" > "$OUT/context.txt"
	printf 'sut_boot_id=%s\ndriver_boot_id=%s\nSCALE=%s\nPORT=%s\n' "${sut_id:-local}" "${driver_id:-local}" "$SCALE" "$PORT" >> "$OUT/context.txt"
	"$PGBIN/postgres" --version >> "$OUT/context.txt" 2>&1; rc=$?
	if [ "$rc" != 0 ]; then fail_cell "BINARY_VERSION_FAIL rc=$rc"; return; fi
	fresh_initdb > "$OUT/initdb.log" 2>&1; rc=$?
	if [ "$rc" != 0 ]; then fail_cell "INITDB_FAIL rc=$rc"; return; fi
	cp "$DATA/pgdata/postgresql.conf" "$DATA/pgdata/pg_hba.conf" "$OUT/" 2> "$OUT/config-copy.log"; rc=$?
	if [ "$rc" != 0 ]; then fail_cell "CONFIG_COPY_FAIL rc=$rc"; return; fi
	start_lane "$lane" "$creq" > "$OUT/start.log" 2>&1; rc=$?
	if [ "$rc" != 0 ]; then fail_cell "START_FAIL rc=$rc $(< "$OUT/start.log")"; return; fi
	Q 'show all' > "$OUT/settings.log" 2>&1; rc=$?
	if [ "$rc" != 0 ]; then fail_cell "SETTINGS_FAIL rc=$rc"; return; fi
	if ! awk -F '|' '$1=="fsync" || $1=="synchronous_commit" || $1=="full_page_writes" || $1=="io_method" {n++} END {exit n!=4}' "$OUT/settings.log"; then
		fail_cell SETTINGS_PARSE_FAIL; return
	fi
	timeout -k 5 300 "$PGBIN/pgbench" -h 127.0.0.1 -p "$PORT" -U postgres -i -s "$SCALE" --quiet postgres > "$OUT/init.log" 2>&1; rc=$?
	if [ "$rc" != 0 ]; then
		local why=PGBENCH_INIT_FAIL; [ "$rc" != 124 ] || why=PGBENCH_INIT_TIMEOUT
		fail_cell "$why rc=$rc"; return
	fi
	local pgbench_bin="${LOADGEN_PGBIN:+$LOADGEN_PGBIN/}pgbench" args cmd logpfx
	printf -v cmd '%q --version' "$pgbench_bin"
	DRV "$cmd" 30 > "$OUT/driver-version.log" 2>&1; rc=$?
	if [ "$rc" != 0 ]; then fail_cell "DRIVER_VERSION_FAIL rc=$rc"; return; fi
	local builtin=(--builtin tpcb-like)
	[ "$bench" != select ] || builtin=(--builtin select-only -M prepared)
	printf -v args '%q ' "$pgbench_bin" -h "$SUT_IP" -p "$PORT" -U postgres -n -c "$clients" -j "$clients" "${builtin[@]}"
	if [ "$WARMUP" -gt 0 ]; then
		cmd="$args -T $WARMUP postgres"; echo "$cmd" > "$OUT/warmup-command.txt"
		DRV "$cmd" "$((WARMUP+TIMEOUT_GRACE))" > "$OUT/warmup.log" 2>&1; rc=$?
		if [ "$rc" != 0 ]; then
			local why=WARMUP_FAIL; [ "$rc" != 124 ] || why=WARMUP_TIMEOUT
			fail_cell "$why rc=$rc"; return
		fi
	fi
	logpfx="/tmp/p1_${OUT##*/}_$$"
	cmd="$args -T $DURATION --log --log-prefix=$logpfx postgres"
	echo "$cmd" > "$OUT/measurement-command.txt"
	printf 'driver=%s\nlog_prefix=%s\n' "$DRIVER_HOST_LABEL" "$logpfx" > "$OUT/driver-logs.txt"
	# Baseline and samples exclude observer commits. Failed samples stay visible,
	# and yield UNKNOWN, never a fabricated no-stall result.
	local sql="select xact_commit+xact_rollback from pg_stat_database where datname='postgres'"
	Q "$sql" > "$OUT/progress.log" 2> "$OUT/progress-errors.log" || echo ERROR >> "$OUT/progress.log"
	if command -v mpstat >/dev/null 2>&1; then
		mpstat 1 > "$OUT/mpstat.log" 2>&1 & CPUPID=$!
	fi
	DRV "$cmd" "$((DURATION+TIMEOUT_GRACE))" > "$OUT/pgbench.log" 2>&1 & DRIVERPID=$!
	(
		while kill -0 "$DRIVERPID" 2>/dev/null; do
			sleep 10
			kill -0 "$DRIVERPID" 2>/dev/null || break
			Q "$sql" >> "$OUT/progress.log" 2>> "$OUT/progress-errors.log" || echo ERROR >> "$OUT/progress.log"
		done
	) & STALLPID=$!
	wait "$DRIVERPID"; rc=$?; DRIVERPID=""
	[ -z "$CPUPID" ] || { kill "$CPUPID" 2>/dev/null || :; wait "$CPUPID" 2>/dev/null || :; CPUPID=""; }
	local sampler_rc
	wait "$STALLPID"; sampler_rc=$?; STALLPID=""
	echo "sampler_exit_rc=$sampler_rc" >> "$OUT/progress-errors.log"
	stall=$(awk '/^[0-9]+$/ {if (n++) {samples++; if ($1==prev) froze++} prev=$1; next}
		{bad=1} END {if (bad || samples<2) print "UNKNOWN"; else if (froze>samples/3) printf "STALL:%d/%d\n",froze,samples; else print "no"}' "$OUT/progress.log")
	[ "$sampler_rc" = 0 ] || stall=UNKNOWN
	# Retain raw transaction logs on success AND failure. Remote originals are
	# deliberately left in place if transfer fails. Percentiles reuse pgbench_pctl.
	mkdir "$OUT/transactions"
	local fetch_rc=0
	if [ "$COLOCATED" = 1 ]; then
		cp "${logpfx}".* "$OUT/transactions/" 2> "$OUT/fetch.log" || fetch_rc=$?
		[ "$fetch_rc" != 0 ] || rm -f "${logpfx}".*
	else
		DRV "cd /tmp && tar -czf - ${logpfx##*/}.*" > "$OUT/transactions.tar.gz" 2> "$OUT/fetch.log" &&
			tar -xzf "$OUT/transactions.tar.gz" -C "$OUT/transactions" 2>> "$OUT/fetch.log" || fetch_rc=$?
	fi
	if [ "$rc" != 0 ]; then
		local why=PGBENCH_FAIL; [ "$rc" != 124 ] || why=PGBENCH_TIMEOUT
		fail_cell "$why rc=$rc fetch_rc=$fetch_rc"; return
	fi
	if [ "$sampler_rc" != 0 ]; then fail_cell "PROGRESS_SAMPLER_FAIL rc=$sampler_rc fetch_rc=$fetch_rc"; return; fi
	local tps
	tps=$(awk '/^tps = [0-9]+([.][0-9]+)?([[:space:]]|$)/ {print $3; exit}' "$OUT/pgbench.log")
	if [ -z "$tps" ]; then fail_cell TPS_PARSE_FAIL; return; fi
	if [ "$fetch_rc" != 0 ]; then fail_cell "LOG_FETCH_FAIL rc=$fetch_rc"; return; fi
	if grep -q ERROR "$OUT/progress.log"; then fail_cell PROGRESS_SAMPLE_FAIL; return; fi
	perl "$here/pgbench_pctl" --glob "$OUT/transactions/*" > "$OUT/percentiles.log" 2>&1; rc=$?
	if [ "$rc" != 0 ]; then fail_cell "PCTL_FAIL rc=$rc"; return; fi
	local cnt p50 p95 p99 pmax _unused
	IFS=$'\t' read -r _unused _unused cnt _unused _unused _unused p50 _unused p95 _unused p99 _unused pmax < <(grep '^TSV' "$OUT/percentiles.log")
	# pgbench can report TPS despite failed/buffered log writes. Full logging
	# must contain one valid latency per successful transaction, independently
	# of TPS. Accept known summary labels, never guess a count from throughput.
	local reported
	reported=$(awk -F ': ' '/^number of transactions ((actually |successfully )?processed|completed): [0-9]+$/ {n++; count=$2}
		END {if(n==1) print count}' "$OUT/pgbench.log")
	if [ -z "$reported" ]; then fail_cell TXN_COUNT_PARSE_FAIL; return; fi
	if [ "$cnt" != "$reported" ]; then
		fail_cell "TXN_COUNT_MISMATCH reported=$reported retained=$cnt"; return
	fi
	local cpu_busy=NA idle_meaningful=no
	if [ -f "$OUT/mpstat.log" ]; then
		cpu_busy=$(awk '$0 ~ /all/ && $NF ~ /^[0-9.]+$/ {sum+=100-$NF;n++} END {if(n) printf "%.2f",sum/n; else print "NA"}' "$OUT/mpstat.log")
	fi
	[ "$COLOCATED" = 1 ] || [ "$cpu_busy" = NA ] || idle_meaningful=yes
	local fsync sync_commit fpw iom
	# Read durability context from the retained pre-workload SHOW ALL rather
	# than silently accepting failed post-workload queries as empty settings.
	fsync=$(awk -F '|' '$1=="fsync" {print $2}' "$OUT/settings.log")
	sync_commit=$(awk -F '|' '$1=="synchronous_commit" {print $2}' "$OUT/settings.log")
	fpw=$(awk -F '|' '$1=="full_page_writes" {print $2}' "$OUT/settings.log")
	iom=$(awk -F '|' '$1=="io_method" {print $2}' "$OUT/settings.log")
	Q 'select * from pg_stat_activity' > "$OUT/activity.log" 2>&1; rc=$?
	if [ "$rc" != 0 ]; then fail_cell "DIAGNOSTICS_FAIL rc=$rc"; return; fi
	stop_lane; rc=$?
	if [ "$rc" != 0 ]; then fail_cell "STOP_FAIL rc=$rc"; exit 1; fi
	write_row "$bench" "$lane" "$creq" "$EFF_CARRIERS" "$clients" "$tps" "$p50" "$p95" "$p99" "$pmax" "$cnt" "$cpu_busy" "$idle_meaningful" "$degraded" "$DRIVER_HOST_LABEL" "$SUT_HOST" "$SB_MB" "$RAM_PCT" "$fsync" "$sync_commit" "$fpw" "$iom" "$DATA_DEVICE" "$WAL_DEVICE" "$LIBXTC_VERSION" "$PG_COMMIT" "$run" "$stall" "artifacts=$OUT"
	printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$bench" "$lane" "$creq" "$clients" "$run" "$(grep '^TSV' "$OUT/percentiles.log" | tr '\t' ' ')" >> "$LATS" || exit 2
	say "DONE $bench/$lane clients=$clients run=$run stall=$stall degraded=$degraded"
}
say "P1 START lanes='$LANES' driver=$DRIVER_HOST_LABEL colocated=$COLOCATED"
# Alternate lanes within each repeat rather than running all fork then all fiber.
for bench in $WORKLOADS; do
	for clients in $CLIENTS; do
		for run in $(seq 1 "$RUNS"); do
			for lane in $LANES; do
				case "$lane" in
					xtc) for creq in $CARRIERS; do run_cell "$bench" "$lane" "$creq" "$clients" "$run"; done;;
					fiber) run_cell "$bench" "$lane" "$FIBER_LOOPS" "$clients" "$run";;
					*) run_cell "$bench" "$lane" - "$clients" "$run";;
				esac
			done
		done
	done
done
say "P1 DONE failures=$FAILED results=$RES"
if command -v column >/dev/null 2>&1; then column -t "$RES" > "$OUT/results.txt"; fi
[ "$FAILED" = 0 ] && echo P1_MATRIX_DONE
exit "$FAILED"
