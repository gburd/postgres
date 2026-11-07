# PostgreSQL Development Aliases

# ============================================================
# Build helpers shared by every variant.
# ============================================================
pg_clean_for_compiler() {
	local current_compiler="$(basename $CC)"
	local build_dir="${1:-$PG_BUILD_DIR}"

	if [ -f "$build_dir/compile_commands.json" ]; then
		local last_compiler=$(grep -o '/[^/]*/bin/[gc]cc\|/[^/]*/bin/clang' "$build_dir/compile_commands.json" | head -1 | xargs basename 2>/dev/null || echo "unknown")

		if [ "$last_compiler" != "$current_compiler" ] && [ "$last_compiler" != "unknown" ]; then
			echo "Detected compiler change from $last_compiler to $current_compiler"
			echo "Cleaning build directory..."
			trash "$build_dir" 2>/dev/null || rm -rf "$build_dir"
			mkdir -p "$build_dir"
		fi
	fi

	mkdir -p "$build_dir"
	echo "$current_compiler" >"$build_dir/.compiler_used"
}

# ============================================================
# Core PostgreSQL commands (default/debug build)
# ============================================================
alias pg-setup='
  if [ -z "$PERL_CORE_DIR" ]; then
    echo "Error: Could not find perl CORE directory" >&2
    return 1
  fi

  pg_clean_for_compiler "$PG_BUILD_DIR"

  echo "=== PostgreSQL Build Configuration ==="
  echo "Compiler: $CC"
  echo "LLVM: $(llvm-config --version 2>/dev/null || echo disabled)"
  echo "Source: $PG_SOURCE_DIR"
  echo "Build: $PG_BUILD_DIR"
  echo "Install: $PG_INSTALL_DIR"
  echo "======================================"

  env CFLAGS="-I$PERL_CORE_DIR $CFLAGS" \
      LDFLAGS="-L$PERL_CORE_DIR -lperl $LDFLAGS" \
  meson setup $MESON_EXTRA_SETUP \
    --reconfigure \
    -Doptimization=g \
    -Ddebug=true \
    -Db_sanitize=none \
    -Db_lundef=false \
    -Dlz4=enabled \
    -Dzstd=enabled \
    -Dllvm=disabled \
    -Dplperl=enabled \
    -Dplpython=enabled \
    -Dpltcl=enabled \
    -Dlibxml=enabled \
    -Duuid=e2fs \
    -Dlibxslt=enabled \
    -Dssl=openssl \
    -Dldap=disabled \
    -Dcassert=true \
    -Dtap_tests=enabled \
    -Dinjection_points=true \
    -Ddocs_pdf=enabled \
    -Ddocs_html_style=website \
    --prefix="$PG_INSTALL_DIR" \
    "$PG_BUILD_DIR" \
    "$PG_SOURCE_DIR"'

alias pg-compdb='compdb -p build/ list > compile_commands.json'
alias pg-build='meson compile -C "$PG_BUILD_DIR"'
alias pg-install='meson install -C "$PG_BUILD_DIR"'
alias pg-test='meson test -q --print-errorlogs -C "$PG_BUILD_DIR"'

# Clean commands
alias pg-clean='ninja -C "$PG_BUILD_DIR" clean'
alias pg-full-clean='trash "$PG_BUILD_DIR" "$PG_INSTALL_DIR" 2>/dev/null || rm -rf "$PG_BUILD_DIR" "$PG_INSTALL_DIR"; echo "Build and install directories cleaned"'

# Database management
alias pg-init='trash "$PG_DATA_DIR" 2>/dev/null || rm -rf "$PG_DATA_DIR"; "$PG_INSTALL_DIR/bin/initdb" --debug --no-clean "$PG_DATA_DIR"'

alias pg-start='ulimit -c unlimited && "$PG_INSTALL_DIR/bin/postgres" -D "$PG_DATA_DIR" -k "$PG_DATA_DIR"'

alias pg-stop='pkill -f "postgres.*-D.*$PG_DATA_DIR" || true'
alias pg-restart='pg-stop && sleep 2 && pg-start'
alias pg-status='pgrep -f "postgres.*-D.*$PG_DATA_DIR" && echo "PostgreSQL is running" || echo "PostgreSQL is not running"'

# Client connections
alias pg-psql='"$PG_INSTALL_DIR/bin/psql" -h "$PG_DATA_DIR" postgres'
alias pg-createdb='"$PG_INSTALL_DIR/bin/createdb" -h "$PG_DATA_DIR"'
alias pg-dropdb='"$PG_INSTALL_DIR/bin/dropdb" -h "$PG_DATA_DIR"'

# ============================================================
# Debugger attachments
# ============================================================
alias pg-debug-gdb='gdb -x "$GDBINIT" -x .gdbinit "$PG_INSTALL_DIR/bin/postgres"'
alias pg-debug-lldb='lldb "$PG_INSTALL_DIR/bin/postgres"'
alias pg-debug='
  if command -v gdb >/dev/null 2>&1; then
    pg-debug-gdb
  elif command -v lldb >/dev/null 2>&1; then
    pg-debug-lldb
  else
    echo "No debugger available (gdb or lldb required)"
  fi'

alias pg-attach-gdb='
  PG_PID=$(pgrep -f "postgres.*-D.*$PG_DATA_DIR" | head -1)
  if [ -n "$PG_PID" ]; then
    echo "Attaching GDB to PostgreSQL process $PG_PID"
    gdb -x "$GDBINIT" -x .gdbinit -p "$PG_PID"
  else
    echo "No PostgreSQL process found"
  fi'

alias pg-attach-lldb='
  PG_PID=$(pgrep -f "postgres.*-D.*$PG_DATA_DIR" | head -1)
  if [ -n "$PG_PID" ]; then
    echo "Attaching LLDB to PostgreSQL process $PG_PID"
    lldb -p "$PG_PID"
  else
    echo "No PostgreSQL process found"
  fi'

alias pg-attach='
  if command -v gdb >/dev/null 2>&1; then
    pg-attach-gdb
  elif command -v lldb >/dev/null 2>&1; then
    pg-attach-lldb
  else
    echo "No debugger available (gdb or lldb required)"
  fi'

# ============================================================
# Valgrind-instrumented build and tests
#
# The valgrind build lives in a separate directory so the normal
# build stays warm.  Runs use a wrapper dir that shadows `postgres`
# with a valgrind wrapper -- pg_regress finds it via PATH.
# ============================================================
pg-build-valgrind() {
	local bdir="$PG_BUILD_DIR_VALGRIND"
	if [ -z "$PERL_CORE_DIR" ]; then
		echo "Error: PERL_CORE_DIR is not set" >&2
		return 1
	fi

	pg_clean_for_compiler "$bdir"

	echo "=== Configuring Valgrind build in $bdir ==="
	env CFLAGS="-Og -ggdb3 -fno-omit-frame-pointer -DUSE_VALGRIND -I$PERL_CORE_DIR $CFLAGS" \
		LDFLAGS="-L$PERL_CORE_DIR -lperl $LDFLAGS" \
		meson setup --reconfigure \
		-Doptimization=g \
		-Ddebug=true \
		-Dcassert=true \
		-Dtap_tests=enabled \
		-Dinjection_points=true \
		-Dllvm=disabled \
		-Dplperl=enabled -Dplpython=enabled -Dpltcl=enabled \
		-Dlz4=enabled -Dzstd=enabled \
		-Dlibxml=enabled -Dlibxslt=enabled -Dssl=openssl -Duuid=e2fs \
		-Dldap=disabled \
		--prefix="$PG_INSTALL_DIR-valgrind" \
		"$bdir" "$PG_SOURCE_DIR" || return 1

	meson compile -C "$bdir"
}

# Drop a wrapper directory that shadows the real binaries; `postgres`
# exec's into valgrind, everything else is a symlink.  Writes to the
# supplied wrap dir and echoes its path.
_pg_make_valgrind_wrapper() {
	local bindir="$1"
	local wrapdir="$2"

	mkdir -p "$wrapdir"
	cat >"$wrapdir/postgres" <<EOF
#!/usr/bin/env bash
exec valgrind \\
	--tool=memcheck \\
	--leak-check=no \\
	--track-origins=yes \\
	--read-var-info=yes \\
	--num-callers=30 \\
	--trace-children=yes \\
	--error-limit=no \\
	--gen-suppressions=all \\
	--suppressions=$PG_SOURCE_DIR/src/tools/valgrind.supp \\
	--time-stamp=yes \\
	--log-file=$PG_BENCH_DIR/valgrind-%p.log \\
	$bindir/postgres "\$@"
EOF
	chmod +x "$wrapdir/postgres"

	# Symlink the rest so pg_regress finds them via the wrapper PATH.
	for bin in initdb psql pg_ctl createdb dropdb pg_dump pg_restore \
		pg_isready pg_basebackup pg_recvlogical pg_waldump createuser dropuser \
		pg_upgrade pg_resetwal pg_archivecleanup pg_rewind pg_verifybackup; do
		if [ -x "$bindir/$bin" ]; then
			ln -sf "$bindir/$bin" "$wrapdir/$bin"
		fi
	done
}

pg-valgrind-regress() {
	local bdir="$PG_BUILD_DIR_VALGRIND"
	if [ ! -x "$bdir/src/backend/postgres" ]; then
		echo "Valgrind build not found; run 'pg-build-valgrind' first." >&2
		return 1
	fi

	local tmpbin="$bdir/tmp_install$PG_INSTALL_DIR-valgrind/bin"
	if [ ! -x "$tmpbin/postgres" ]; then
		echo "Populating tmp_install..."
		meson test -C "$bdir" tmp_install install_test_files initdb_cache >/dev/null || return 1
	fi

	local wrap
	wrap=$(mktemp -d /tmp/pg-vg-wrap-XXXXXX)
	_pg_make_valgrind_wrapper "$tmpbin" "$wrap"

	mkdir -p "$PG_BENCH_DIR"
	echo "Valgrind logs: $PG_BENCH_DIR/valgrind-*.log"
	echo "Wrapper dir:   $wrap (will be removed on exit)"
	echo "Expect the regress suite to take 15-45 minutes under valgrind."

	local rc=0
	(cd "$bdir" && PATH="$wrap:$PATH" meson test -t 60 --print-errorlogs regress/regress) || rc=$?

	trash "$wrap" 2>/dev/null || rm -rf "$wrap"
	return "$rc"
}

pg-valgrind-test() {
	local bdir="$PG_BUILD_DIR_VALGRIND"
	if [ ! -x "$bdir/src/backend/postgres" ]; then
		echo "Valgrind build not found; run 'pg-build-valgrind' first." >&2
		return 1
	fi

	echo "This runs the FULL postgres test suite under valgrind."
	echo "Expect many hours, and tens of GB of valgrind log output."
	echo "Logs: $PG_BENCH_DIR/valgrind-*.log"
	local yn
	read -r -p "Continue? [y/N] " yn
	case "$yn" in
		y | Y | yes) ;;
		*) echo "Aborted."; return 0 ;;
	esac

	local tmpbin="$bdir/tmp_install$PG_INSTALL_DIR-valgrind/bin"
	if [ ! -x "$tmpbin/postgres" ]; then
		echo "Populating tmp_install..."
		meson test -C "$bdir" tmp_install install_test_files initdb_cache >/dev/null || return 1
	fi

	local wrap
	wrap=$(mktemp -d /tmp/pg-vg-wrap-XXXXXX)
	_pg_make_valgrind_wrapper "$tmpbin" "$wrap"
	mkdir -p "$PG_BENCH_DIR"

	local rc=0
	(cd "$bdir" && PATH="$wrap:$PATH" meson test -t 60 --print-errorlogs) || rc=$?

	trash "$wrap" 2>/dev/null || rm -rf "$wrap"
	return "$rc"
}

# ============================================================
# AddressSanitizer / UndefinedBehaviorSanitizer build and tests
# ============================================================
pg-build-asan() {
	local bdir="$PG_BUILD_DIR_ASAN"
	if [ -z "$PERL_CORE_DIR" ]; then
		echo "Error: PERL_CORE_DIR is not set" >&2
		return 1
	fi

	pg_clean_for_compiler "$bdir"

	echo "=== Configuring ASan+UBSan build in $bdir ==="
	env CFLAGS="-Og -ggdb3 -fno-omit-frame-pointer -fsanitize=address,undefined -fno-sanitize-recover=all -I$PERL_CORE_DIR $CFLAGS" \
		LDFLAGS="-fsanitize=address,undefined -L$PERL_CORE_DIR -lperl $LDFLAGS" \
		meson setup --reconfigure \
		-Doptimization=g \
		-Ddebug=true \
		-Dcassert=true \
		-Dtap_tests=enabled \
		-Dinjection_points=true \
		-Dllvm=disabled \
		-Dplperl=enabled -Dplpython=enabled -Dpltcl=enabled \
		-Dlz4=enabled -Dzstd=enabled \
		-Dlibxml=enabled -Dlibxslt=enabled -Dssl=openssl -Duuid=e2fs \
		-Dldap=disabled \
		--prefix="$PG_INSTALL_DIR-asan" \
		"$bdir" "$PG_SOURCE_DIR" || return 1

	meson compile -C "$bdir"
}

pg-asan-regress() {
	local bdir="$PG_BUILD_DIR_ASAN"
	if [ ! -x "$bdir/src/backend/postgres" ]; then
		echo "ASan build not found; run 'pg-build-asan' first." >&2
		return 1
	fi

	# halt_on_error=0 lets regress continue past the first diagnostic so
	# the whole suite runs; abort_on_error=1 makes each hit fail the test.
	ASAN_OPTIONS="halt_on_error=0:abort_on_error=1:detect_leaks=0:print_summary=1:print_stacktrace=1" \
		UBSAN_OPTIONS="halt_on_error=1:abort_on_error=1:print_stacktrace=1:print_summary=1" \
		meson test -t 5 --print-errorlogs -C "$bdir" regress/regress
}

# ============================================================
# rr (deterministic record-and-replay)
# Requires kernel.perf_event_paranoid <= 1.  rr is the single most
# effective tool for postgres bugs that reproduce intermittently.
# ============================================================
pg-rr-check() {
	if ! command -v rr >/dev/null; then
		echo "rr is not installed (expected in the dev shell)." >&2
		return 1
	fi
	local paranoid
	paranoid=$(cat /proc/sys/kernel/perf_event_paranoid 2>/dev/null || echo 99)
	if [ "$paranoid" -gt 1 ]; then
		echo "rr requires kernel.perf_event_paranoid <= 1; currently $paranoid"
		echo "To enable (root needed):"
		echo "  echo 1 | sudo tee /proc/sys/kernel/perf_event_paranoid"
		return 1
	fi
	echo "rr ready (perf_event_paranoid=$paranoid)"
}

pg-rr-record() {
	pg-rr-check >/dev/null || {
		pg-rr-check
		return 1
	}
	ulimit -c unlimited
	rr record -- "$PG_INSTALL_DIR/bin/postgres" -D "$PG_DATA_DIR" -k "$PG_DATA_DIR"
}

pg-rr-replay() {
	rr replay "$@"
}

# ============================================================
# perf wrappers (parallel to the flame-graph helper)
# ============================================================
pg-perf-record() {
	local pid
	pid=$(pgrep -f "postgres.*-D.*$PG_DATA_DIR" | head -1)
	if [ -z "$pid" ]; then
		echo "No postgres running under $PG_DATA_DIR" >&2
		return 1
	fi
	mkdir -p "$PG_BENCH_DIR"
	local out="$PG_BENCH_DIR/perf-$(date +%Y%m%d_%H%M%S).data"
	echo "Recording to $out (Ctrl-C to stop)"
	perf record -F 997 --call-graph dwarf -p "$pid" -o "$out" "$@"
	echo "Saved: $out"
}

pg-perf-report() {
	local data
	data=$(ls -t "$PG_BENCH_DIR"/perf-*.data 2>/dev/null | head -1)
	if [ -z "$data" ]; then
		echo "No perf data in $PG_BENCH_DIR" >&2
		return 1
	fi
	echo "Reading $data"
	perf report -i "$data" "$@"
}

pg-perf-annotate() {
	local data
	data=$(ls -t "$PG_BENCH_DIR"/perf-*.data 2>/dev/null | head -1)
	if [ -z "$data" ]; then
		echo "No perf data in $PG_BENCH_DIR" >&2
		return 1
	fi
	perf annotate -i "$data" "$@"
}

# ============================================================
# Single regression test / group runner.
# Runs pg_regress directly against the existing build so you skip the
# full meson-driven suite wrapper.  Usage: pg-test-one boolean [name ...]
# ============================================================
pg-test-one() {
	if [ $# -eq 0 ]; then
		echo "usage: pg-test-one TESTNAME [TESTNAME ...]"
		echo "example: pg-test-one boolean"
		return 2
	fi
	local bdir="${PG_BUILD_DIR_ONE:-$PG_BUILD_DIR}"
	local tmpbin="$bdir/tmp_install$PG_INSTALL_DIR/bin"
	if [ ! -x "$tmpbin/postgres" ]; then
		echo "Populating tmp_install..."
		meson test -C "$bdir" tmp_install install_test_files initdb_cache >/dev/null || return 1
	fi
	local outdir
	outdir=$(mktemp -d /tmp/pg-test-one-XXXXXX)
	echo "Test output: $outdir"
	"$bdir/src/test/regress/pg_regress" \
		--bindir="$tmpbin" \
		--inputdir="$PG_SOURCE_DIR/src/test/regress" \
		--expecteddir="$PG_SOURCE_DIR/src/test/regress" \
		--dlpath="$bdir/src/test/regress" \
		--outputdir="$outdir" \
		--temp-instance="$outdir/tmp" \
		--port=40099 \
		"$@"
}

# Full flame graph / benchmark aliases
alias pg-flame='pg-flame-generate'
alias pg-flame-30='pg-flame-generate 30'
alias pg-flame-60='pg-flame-generate 60'
alias pg-flame-120='pg-flame-generate 120'

pg-flame-custom() {
	local duration=${1:-30}
	local output_dir=${2:-$PG_FLAME_DIR}
	echo "Generating flame graph for ${duration}s, output to: $output_dir"
	pg-flame-generate "$duration" "$output_dir"
}

alias pg-bench='pg-bench-run'
alias pg-bench-quick='pg-bench-run 5 1 100 1 30 select-only'
alias pg-bench-standard='pg-bench-run 10 2 1000 10 60 tpcb-like'
alias pg-bench-heavy='pg-bench-run 50 4 5000 100 300 tpcb-like'
alias pg-bench-readonly='pg-bench-run 20 4 2000 50 120 select-only'

pg-bench-custom() {
	local clients=${1:-10}
	local threads=${2:-2}
	local transactions=${3:-1000}
	local scale=${4:-10}
	local duration=${5:-60}
	local test_type=${6:-tpcb-like}

	echo "Running custom benchmark:"
	echo "  Clients: $clients, Threads: $threads"
	echo "  Transactions: $transactions, Scale: $scale"
	echo "  Duration: ${duration}s, Type: $test_type"

	pg-bench-run "$clients" "$threads" "$transactions" "$scale" "$duration" "$test_type"
}

pg-bench-flame() {
	local duration=${1:-60}
	local clients=${2:-10}
	local scale=${3:-10}

	echo "Running benchmark with flame graph generation"
	echo "Duration: ${duration}s, Clients: $clients, Scale: $scale"

	pg-bench-run "$clients" 2 1000 "$scale" "$duration" tpcb-like &
	local bench_pid=$!

	sleep 5

	local flame_duration=$((duration - 10))
	if [ $flame_duration -gt 10 ]; then
		pg-flame-generate "$flame_duration" &
		local flame_pid=$!
	fi

	wait $bench_pid
	if [ -n "${flame_pid:-}" ]; then
		wait $flame_pid
	fi

	echo "Benchmark and flame graph generation completed"
}

# Live monitoring
alias pg-perf='perf top -p $(pgrep -f "postgres.*-D.*$PG_DATA_DIR" | head -1)'
alias pg-htop='htop -p $(pgrep -f "postgres.*-D.*$PG_DATA_DIR" | tr "\n" "," | sed "s/,$//")'

pg-stats() {
	local duration=${1:-30}
	echo "Collecting system stats for ${duration}s..."

	iostat -x 1 "$duration" >"$PG_BENCH_DIR/iostat_$(date +%Y%m%d_%H%M%S).log" &
	vmstat 1 "$duration" >"$PG_BENCH_DIR/vmstat_$(date +%Y%m%d_%H%M%S).log" &

	wait
	echo "System stats saved to $PG_BENCH_DIR"
}

# ============================================================
# Code quality helpers
# ============================================================
pg-format() {
	local since=${1:-HEAD}

	if [ ! -f "$PG_SOURCE_DIR/src/tools/pgindent/pgindent" ]; then
		echo "Error: pgindent not found at $PG_SOURCE_DIR/src/tools/pgindent/pgindent"
	else

		modified_files=$(git diff --name-only "${since}" | grep -E "\.c$|\.h$")

		if [ -z "$modified_files" ]; then
			echo "No modified .c or .h files found"
		else

			echo "Formatting modified files with pgindent:"
			for file in $modified_files; do
				if [ -f "$file" ]; then
					echo "  Formatting: $file"
					"$PG_SOURCE_DIR/src/tools/pgindent/pgindent" "$file"
				else
					echo "  Warning: File not found: $file"
				fi
			done

			echo "Checking files for whitespace:"
			git diff --check "${since}"
		fi
	fi
}

pg-tidy() {
	local since=${1:-HEAD}
	local files
	files=$(git diff --name-only "$since" | grep -E "\.(c|h)$")
	if [ -z "$files" ]; then
		echo "No modified .c or .h files."
		return 0
	fi
	for f in $files; do
		[ -f "$f" ] || continue
		echo "clang-tidy: $f"
		clang-tidy -p "$PG_BUILD_DIR" "$f" 2>&1 | head -50
	done
}

pg-spell() {
	local since=${1:-HEAD}
	local files=$(git diff --name-only "$since" | grep -E '\.(c|h|sgml|md)$')
	if [ -z "$files" ]; then
		echo "No .c/.h/.sgml/.md files changed since $since"
		return 0
	fi
	for f in $files; do
		[ -f "$f" ] || continue
		case "$f" in
			*.c | *.h)
				grep -nE '^\s*(/\*|\*|//)' "$f" | codespell --stdin-single-line - 2>/dev/null \
					&& echo "  $f: ok" || true
				;;
			*.sgml | *.md)
				codespell "$f" || true
				;;
		esac
	done
}

# ============================================================
# Core dump one-shots (one-time, requires root).  kernel.core_pattern
# is a system-wide sysctl -- we don't touch it on every shell entry.
# ============================================================
pg-cores-status() {
	echo "ulimit -c:            $(ulimit -c)"
	echo "kernel.core_pattern: $(cat /proc/sys/kernel/core_pattern 2>/dev/null || echo unreadable)"
	echo "cwd:                 $(pwd)"
}

pg-enable-cores() {
	ulimit -c unlimited
	if ! [ -w /proc/sys/kernel/core_pattern ]; then
		echo "Setting kernel.core_pattern (requires sudo)..."
		echo "core.%p" | sudo tee /proc/sys/kernel/core_pattern >/dev/null || {
			echo "Failed to write /proc/sys/kernel/core_pattern" >&2
			return 1
		}
	else
		echo "core.%p" >/proc/sys/kernel/core_pattern
	fi
	pg-cores-status
}

pg-disable-cores() {
	ulimit -c 0
	if ! [ -w /proc/sys/kernel/core_pattern ]; then
		echo "Restoring kernel.core_pattern to 'core' (requires sudo)..."
		echo "core" | sudo tee /proc/sys/kernel/core_pattern >/dev/null || {
			echo "Failed to restore /proc/sys/kernel/core_pattern" >&2
			return 1
		}
	else
		echo "core" >/proc/sys/kernel/core_pattern
	fi
	pg-cores-status
}

# ============================================================
# Logs and results
# ============================================================
alias pg-log='tail -f "$PG_DATA_DIR/log/postgresql-$(date +%Y-%m-%d).log" 2>/dev/null || echo "No log file found"'
alias pg-log-errors='grep -i error "$PG_DATA_DIR/log/"*.log 2>/dev/null || echo "No error logs found"'

alias pg-build-log='cat "$PG_BUILD_DIR/meson-logs/meson-log.txt"'
alias pg-build-errors='grep -i error "$PG_BUILD_DIR/meson-logs/meson-log.txt" 2>/dev/null || echo "No build errors found"'

alias pg-bench-results='ls -la "$PG_BENCH_DIR" && echo "Latest results:" && tail -20 "$PG_BENCH_DIR"/results_*.txt 2>/dev/null | tail -20'
alias pg-flame-results='ls -la "$PG_FLAME_DIR" && echo "Open flame graphs with: firefox $PG_FLAME_DIR/*.svg"'

pg-clean-results() {
	local days=${1:-7}
	echo "Cleaning benchmark and flame graph results older than $days days..."
	find "$PG_BENCH_DIR" -type f -mtime +$days -delete 2>/dev/null || true
	find "$PG_FLAME_DIR" -type f -mtime +$days -delete 2>/dev/null || true
	echo "Cleanup completed"
}

# ============================================================
# Info
# ============================================================
alias pg-info='
  echo "=== PostgreSQL Development Environment ==="
  echo "Source:          $PG_SOURCE_DIR"
  echo "Build (default): $PG_BUILD_DIR"
  echo "Build (valgrind):$PG_BUILD_DIR_VALGRIND"
  echo "Build (asan):    $PG_BUILD_DIR_ASAN"
  echo "Install:         $PG_INSTALL_DIR"
  echo "Data:            $PG_DATA_DIR"
  echo "Benchmarks:      $PG_BENCH_DIR"
  echo "Flame graphs:    $PG_FLAME_DIR"
  echo "Compiler:        $CC"
  echo ""
  echo "Available commands:"
  echo "  Setup/build:   pg-setup, pg-build, pg-install"
  echo "  Database:      pg-init, pg-start, pg-stop, pg-psql"
  echo "  Tests:         pg-test, pg-test-one NAME"
  echo "  Valgrind:      pg-build-valgrind, pg-valgrind-regress, pg-valgrind-test"
  echo "  ASan/UBSan:    pg-build-asan, pg-asan-regress"
  echo "  Debug:         pg-debug, pg-attach"
  echo "  Record/replay: pg-rr-check, pg-rr-record, pg-rr-replay"
  echo "  Perf:          pg-perf-record, pg-perf-report, pg-perf-annotate, pg-perf"
  echo "  Flame graphs:  pg-flame, pg-flame-30, pg-flame-60, pg-flame-custom"
  echo "  Benchmarks:    pg-bench-quick, pg-bench-standard, pg-bench-heavy"
  echo "  Combined:      pg-bench-flame"
  echo "  Results:       pg-bench-results, pg-flame-results"
  echo "  Logs:          pg-log, pg-build-log"
  echo "  Clean:         pg-clean, pg-full-clean, pg-clean-results"
  echo "  Code quality:  pg-format, pg-tidy, pg-spell"
  echo "  Cores:         pg-enable-cores, pg-disable-cores, pg-cores-status"
  echo "=========================================="'

echo "PostgreSQL aliases loaded. Run 'pg-info' for available commands."
