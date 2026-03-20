# PostgreSQL Development Aliases

# Build system management
pg_clean_for_compiler() {
        local current_compiler="$(basename $CC)"
        local build_dir="$PG_BUILD_DIR"

        if [ -f "$build_dir/compile_commands.json" ]; then
                local last_compiler=$(grep -o '/[^/]*/bin/[gc]cc\|/[^/]*/bin/clang' "$build_dir/compile_commands.json" | head -1 | xargs basename 2>/dev/null || echo "unknown")

                if [ "$last_compiler" != "$current_compiler" ] && [ "$last_compiler" != "unknown" ]; then
                        echo "Detected compiler change from $last_compiler to $current_compiler"
                        echo "Cleaning build directory..."
                        rm -rf "$build_dir"
                        mkdir -p "$build_dir"
                fi
        fi

        mkdir -p "$build_dir"
        echo "$current_compiler" >"$build_dir/.compiler_used"
}

# Core PostgreSQL commands
alias pg-setup='
  if [ -z "$PERL_CORE_DIR" ]; then
    echo "Error: Could not find perl CORE directory" >&2
    return 1
  fi

  pg_clean_for_compiler

  echo "=== PostgreSQL Build Configuration ==="
  echo "Compiler: $CC"
  echo "LLVM: $(llvm-config --version 2>/dev/null || echo 'disabled')"
  echo "Source: $PG_SOURCE_DIR"
  echo "Build: $PG_BUILD_DIR"
  echo "Install: $PG_INSTALL_DIR"
  echo "======================================"
  # --fatal-meson-warnings
  # --buildtype=debugoptimized \
  env CFLAGS="-I$PERL_CORE_DIR $CFLAGS" \
      LDFLAGS="-L$PERL_CORE_DIR -lperl $LDFLAGS" \
  meson setup $MESON_EXTRA_SETUP \
    --reconfigure \
    -Ddebug=true \
    -Doptimization=0 \
    -Db_coverage=false \
    -Db_lundef=false \
    -Dcassert=true \
    -Ddocs_html_style=website \
    -Ddocs_pdf=enabled \
    -Dicu=enabled \
    -Dinjection_points=true \
    -Dldap=enabled \
    -Dlibcurl=enabled \
    -Dlibxml=enabled \
    -Dlibxslt=enabled \
    -Dllvm=auto \
    -Dlz4=enabled \
    -Dnls=enabled \
    -Dplperl=enabled \
    -Dplpython=enabled \
    -Dpltcl=enabled \
    -Dreadline=enabled \
    -Dssl=openssl \
    -Dtap_tests=enabled \
    -Duuid=e2fs \
    -Dzstd=enabled \
    --prefix="$PG_INSTALL_DIR" \
    "$PG_BUILD_DIR" \
    "$PG_SOURCE_DIR"'

alias pg-compdb='compdb -p build/ list > compile_commands.json'
alias pg-build='meson compile -C "$PG_BUILD_DIR"'
alias pg-install='meson install -C "$PG_BUILD_DIR"'
alias pg-test='meson test -q --print-errorlogs -C "$PG_BUILD_DIR"'

# Clean commands
alias pg-clean='ninja -C "$PG_BUILD_DIR" clean'
alias pg-full-clean='rm -rf "$PG_BUILD_DIR" "$PG_INSTALL_DIR" && echo "Build and install directories cleaned"'

# Database management
alias pg-init='rm -rf "$PG_DATA_DIR" && "$PG_INSTALL_DIR/bin/initdb" --debug --no-clean "$PG_DATA_DIR"'
alias pg-start='"$PG_INSTALL_DIR/bin/postgres" -D "$PG_DATA_DIR" -k "$PG_DATA_DIR"'
alias pg-stop='pkill -f "postgres.*-D.*$PG_DATA_DIR" || true'
alias pg-restart='pg-stop && sleep 2 && pg-start'
alias pg-status='pgrep -f "postgres.*-D.*$PG_DATA_DIR" && echo "PostgreSQL is running" || echo "PostgreSQL is not running"'

# Client connections
alias pg-psql='"$PG_INSTALL_DIR/bin/psql" -h "$PG_DATA_DIR" postgres'
alias pg-createdb='"$PG_INSTALL_DIR/bin/createdb" -h "$PG_DATA_DIR"'
alias pg-dropdb='"$PG_INSTALL_DIR/bin/dropdb" -h "$PG_DATA_DIR"'

# Debugging
alias pg-debug-gdb='gdb -x "$GDBINIT" "$PG_INSTALL_DIR/bin/postgres"'
alias pg-debug-lldb='lldb "$PG_INSTALL_DIR/bin/postgres"'
alias pg-debug='
  if command -v gdb >/dev/null 2>&1; then
    pg-debug-gdb
  elif command -v lldb >/dev/null 2>&1; then
    pg-debug-lldb
  else
    echo "No debugger available (gdb or lldb required)"
  fi'

# Attach to running process
alias pg-attach-gdb='
  PG_PID=$(pgrep -f "postgres.*-D.*$PG_DATA_DIR" | head -1)
  if [ -n "$PG_PID" ]; then
    echo "Attaching GDB to PostgreSQL process $PG_PID"
    gdb -x "$GDBINIT" -p "$PG_PID"
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

# Performance profiling and analysis
alias pg-valgrind='valgrind --tool=memcheck --leak-check=full --show-leak-kinds=all "$PG_INSTALL_DIR/bin/postgres" -D "$PG_DATA_DIR"'
alias pg-strace='strace -f -o /tmp/postgres.strace "$PG_INSTALL_DIR/bin/postgres" -D "$PG_DATA_DIR"'

# Flame graph generation
alias pg-flame='pg-flame-generate'
alias pg-flame-30='pg-flame-generate 30'
alias pg-flame-60='pg-flame-generate 60'
alias pg-flame-120='pg-flame-generate 120'

# Custom flame graph with specific duration and output
pg-flame-custom() {
        local duration=${1:-30}
        local output_dir=${2:-$PG_FLAME_DIR}
        echo "Generating flame graph for ${duration}s, output to: $output_dir"
        pg-flame-generate "$duration" "$output_dir"
}

# Benchmarking with pgbench
alias pg-bench='pg-bench-run'
alias pg-bench-quick='pg-bench-run 5 1 100 1 30 select-only'
alias pg-bench-standard='pg-bench-run 10 2 1000 10 60 tpcb-like'
alias pg-bench-heavy='pg-bench-run 50 4 5000 100 300 tpcb-like'
alias pg-bench-readonly='pg-bench-run 20 4 2000 50 120 select-only'

# Custom benchmark function
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

# Benchmark with flame graph
pg-bench-flame() {
        local duration=${1:-60}
        local clients=${2:-10}
        local scale=${3:-10}

        echo "Running benchmark with flame graph generation"
        echo "Duration: ${duration}s, Clients: $clients, Scale: $scale"

        # Start benchmark in background
        pg-bench-run "$clients" 2 1000 "$scale" "$duration" tpcb-like &
        local bench_pid=$!

        # Wait a bit for benchmark to start
        sleep 5

        # Generate flame graph for most of the benchmark duration
        local flame_duration=$((duration - 10))
        if [ $flame_duration -gt 10 ]; then
                pg-flame-generate "$flame_duration" &
                local flame_pid=$!
        fi

        # Wait for benchmark to complete
        wait $bench_pid

        # Wait for flame graph if it was started
        if [ -n "${flame_pid:-}" ]; then
                wait $flame_pid
        fi

        echo "Benchmark and flame graph generation completed"
}

# Performance monitoring
alias pg-perf='perf top -p $(pgrep -f "postgres.*-D.*$PG_DATA_DIR" | head -1)'
alias pg-htop='htop -p $(pgrep -f "postgres.*-D.*$PG_DATA_DIR" | tr "\n" "," | sed "s/,$//")'

# System performance stats during PostgreSQL operation
pg-stats() {
        local duration=${1:-30}
        echo "Collecting system stats for ${duration}s..."

        iostat -x 1 "$duration" >"$PG_BENCH_DIR/iostat_$(date +%Y%m%d_%H%M%S).log" &
        vmstat 1 "$duration" >"$PG_BENCH_DIR/vmstat_$(date +%Y%m%d_%H%M%S).log" &

        wait
        echo "System stats saved to $PG_BENCH_DIR"
}

# Development helpers
pg-format() {
        local since=${1:-HEAD}

        if [ ! -f "$PG_SOURCE_DIR/src/tools/pgindent/pgindent" ]; then
                echo "Error: pgindent not found at $PG_SOURCE_DIR/src/tools/pgindent/pgindent"
        else

                modified_files=$(git diff --diff-filter=M --name-only "${since}" | grep -E "\.c$|\.h$")

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

                        echo "Checking files for non-ASCII characters:"
                        for file in $modified_files; do
                                if [ -f "$file" ]; then
                                        grep --with-filename --line-number -P '[^\x00-\x7F]' "$file"
                                else
                                        echo "  Warning: File not found: $file"
                                fi
                        done
                fi
        fi
}

alias pg-tidy='find "$PG_SOURCE_DIR" -name "*.c" | head -10 | xargs clang-tidy'

# Log management
alias pg-log='tail -f "$PG_DATA_DIR/log/postgresql-$(date +%Y-%m-%d).log" 2>/dev/null || echo "No log file found"'
alias pg-log-errors='grep -i error "$PG_DATA_DIR/log/"*.log 2>/dev/null || echo "No error logs found"'

# Build logs
alias pg-build-log='cat "$PG_BUILD_DIR/meson-logs/meson-log.txt"'
alias pg-build-errors='grep -i error "$PG_BUILD_DIR/meson-logs/meson-log.txt" 2>/dev/null || echo "No build errors found"'

# Results viewing
alias pg-bench-results='ls -la "$PG_BENCH_DIR" && echo "Latest results:" && tail -20 "$PG_BENCH_DIR"/results_*.txt 2>/dev/null | tail -20'
alias pg-flame-results='ls -la "$PG_FLAME_DIR" && echo "Open flame graphs with: firefox $PG_FLAME_DIR/*.svg"'

# Clean up old results
pg-clean-results() {
        local days=${1:-7}
        echo "Cleaning benchmark and flame graph results older than $days days..."
        find "$PG_BENCH_DIR" -type f -mtime +$days -delete 2>/dev/null || true
        find "$PG_FLAME_DIR" -type f -mtime +$days -delete 2>/dev/null || true
        echo "Cleanup completed"
}

# Information
# Test failure analysis and debugging
alias pg-retest='
  local testlog="$PG_BUILD_DIR/meson-logs/testlog.txt"

  if [ ! -f "$testlog" ]; then
    echo "No test log found at $testlog"
    echo "Run pg-test first to generate test results"
    return 1
  fi

  echo "Finding failed tests..."
  local failed_tests=$(grep "^FAIL" "$testlog" | awk "{print \$2}" | sort -u)

  if [ -z "$failed_tests" ]; then
    echo "No failed tests found!"
    return 0
  fi

  local count=$(echo "$failed_tests" | wc -l)
  echo "Found $count failed test(s). Re-running one at a time..."
  echo ""

  for test in $failed_tests; do
    echo "========================================"
    echo "Running: $test"
    echo "========================================"
    meson test -C "$PG_BUILD_DIR" "$test" --print-errorlogs
    echo ""
  done
'

pg_meld_test() {
        local test_name="$1"
        local testrun_dir="$PG_BUILD_DIR/testrun"

        # Function to find expected and actual output files for a test
        find_test_files() {
                local tname="$1"
                local expected=""
                local actual=""

                # Try to find in testrun directory structure
                # Pattern: testrun/<suite>/<test>/results/*.out vs src/test/<suite>/expected/*.out
                for suite_dir in "$testrun_dir"/*; do
                        if [ -d "$suite_dir" ]; then
                                local suite=$(basename "$suite_dir")
                                local test_dir="$suite_dir/$tname"

                                if [ -d "$test_dir/results" ]; then
                                        local result_file=$(find "$test_dir/results" -name "*.out" -o -name "*.diff" | head -1)

                                        if [ -n "$result_file" ]; then
                                                # Found actual output, now find expected
                                                local base_name=$(basename "$result_file" .out)
                                                base_name=$(basename "$base_name" .diff)

                                                # Look for expected file
                                                if [ -f "$PG_SOURCE_DIR/src/test/$suite/expected/${base_name}.out" ]; then
                                                        expected="$PG_SOURCE_DIR/src/test/$suite/expected/${base_name}.out"
                                                        actual="$result_file"
                                                        break
                                                fi
                                        fi
                                fi
                        fi
                done

                if [ -n "$expected" ] && [ -n "$actual" ]; then
                        echo "$expected|$actual"
                        return 0
                fi
                return 1
        }

        if [ -n "$test_name" ]; then
                # Single test specified
                local files=$(find_test_files "$test_name")

                if [ -z "$files" ]; then
                        echo "Could not find test output files for: $test_name"
                        return 1
                fi

                local expected=$(echo "$files" | cut -d"|" -f1)
                local actual=$(echo "$files" | cut -d"|" -f2)

                echo "Opening meld for test: $test_name"
                echo "Expected: $expected"
                echo "Actual: $actual"
                nohup meld "$expected" "$actual" >/dev/null 2>&1 &
        else
                # No test specified - find all failed tests
                local testlog="$PG_BUILD_DIR/meson-logs/testlog.txt"

                if [ ! -f "$testlog" ]; then
                        echo "No test log found. Run pg-test first."
                        return 1
                fi

                local failed_tests=$(grep "^FAIL" "$testlog" | awk "{print \$2}" | sort -u)

                if [ -z "$failed_tests" ]; then
                        echo "No failed tests found!"
                        return 0
                fi

                echo "Opening meld for all failed tests..."
                local opened=0

                for test in $failed_tests; do
                        local files=$(find_test_files "$test")

                        if [ -n "$files" ]; then
                                local expected=$(echo "$files" | cut -d"|" -f1)
                                local actual=$(echo "$files" | cut -d"|" -f2)

                                echo "  $test: $expected vs $actual"
                                nohup meld "$expected" "$actual" >/dev/null 2>&1 &
                                opened=$((opened + 1))
                                sleep 0.5 # Small delay to avoid overwhelming the system
                        fi
                done

                if [ $opened -eq 0 ]; then
                        echo "Could not find output files for any failed tests"
                        return 1
                fi

                echo "Opened $opened meld session(s)"
        fi
}

alias pg-meld="pg_meld_test"

alias pg-info='
  echo "=== PostgreSQL Development Environment ==="
  echo "Source: $PG_SOURCE_DIR"
  echo "Build: $PG_BUILD_DIR"
  echo "Install: $PG_INSTALL_DIR"
  echo "Data: $PG_DATA_DIR"
  echo "Benchmarks: $PG_BENCH_DIR"
  echo "Flame graphs: $PG_FLAME_DIR"
  echo "Compiler: $CC"
  echo ""
  echo "Available commands:"
  echo "  Setup: pg-setup, pg-build, pg-install"
  echo "  Testing: pg-test, pg-retest, pg-meld"
  echo "  Database: pg-init, pg-start, pg-stop, pg-psql"
  echo "  Debug: pg-debug, pg-attach, pg-valgrind"
  echo "  Performance: pg-flame, pg-bench, pg-perf"
  echo "  Benchmarks: pg-bench-quick, pg-bench-standard, pg-bench-heavy"
  echo "  Flame graphs: pg-flame-30, pg-flame-60, pg-flame-custom"
  echo "  Combined: pg-bench-flame"
  echo "  Results: pg-bench-results, pg-flame-results"
  echo "  Logs: pg-log, pg-build-log"
  echo "  Clean: pg-clean, pg-full-clean, pg-clean-results"
  echo "  Code quality: pg-format, pg-tidy"
  echo "=========================================="'

echo "PostgreSQL aliases loaded. Run 'pg-info' for available commands."
