#!/usr/bin/env bash
# CF-5556 PostgreSQL Performance Test Suite
# Tests patches that identify modified indexed attributes before acquiring exclusive locks
# Enhanced with concurrent read/write pressure analysis

set -u

# ============================================================================
# CONFIGURATION
# ============================================================================

readonly TEST_DURATION="${TEST_DURATION:-60}"
readonly CLIENTS="${CLIENTS:-8}"
readonly JOBS="${JOBS:-4}"
readonly WORKDIR="${WORKDIR:-$(pwd)}"
readonly EXT_DIR="${EXT_DIR:-${WORKDIR}/..}"
readonly RESULTS_BASE="/tmp/cf5556-perf-results"
readonly RESULTS_DIR="${RESULTS_BASE}/$(date +%Y%m%d_%H%M%S)"

SETUP_EXTENSIONS=0
TEST_EXTENSIONS=0

# ============================================================================
# ARGUMENT PARSING
# ============================================================================

while [ $# -gt 0 ]; do
    case "$1" in
        --setup-extensions) SETUP_EXTENSIONS=1; shift ;;
        --test-extensions)  TEST_EXTENSIONS=1; shift ;;
        --help|-h)
            cat <<'HELP'
Usage: ./perf-cf5556.sh [OPTIONS]

Options:
  --setup-extensions    Clone, build and install all extensions
  --test-extensions     Include extension-based tests
  --help                Show this help message

Environment Variables:
  TEST_DURATION         Duration of each test in seconds (default: 60)
  CLIENTS               Number of pgbench clients (default: 8)
  JOBS                  Number of pgbench jobs (default: 4)
  WORKDIR               Working directory (default: current directory)
  EXT_DIR               Directory containing extension sources
HELP
            exit 0
            ;;
        *) echo "Unknown argument: $1. Use --help for usage."; exit 1 ;;
    esac
done

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

log_info() { echo "  $*"; }
log_error() { echo "  ✗ $*" >&2; }
log_success() { echo "  ✓ $*"; }
log_warn() { echo "  ⚠ $*"; }
log_section() { echo ""; echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"; echo "  $*"; echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"; }
log_header() { echo ""; echo "╔════════════════════════════════════════════════════════════════════╗"; echo "║  $*"; echo "╚════════════════════════════════════════════════════════════════════╝"; }

detect_gmake() {
    local cmd
    for cmd in gmake make; do
        if command -v "$cmd" >/dev/null 2>&1 && "$cmd" --version 2>/dev/null | grep -q "GNU Make"; then
            echo "$cmd"
            return 0
        fi
    done
    return 1
}

prompt_yn() {
    local message=$1 default=${2:-n}
    [ ! -t 0 ] && { [ "$default" = "y" ] && return 0 || return 1; }
    local prompt; [ "$default" = "y" ] && prompt="[Y/n]" || prompt="[y/N]"
    printf "    %s %s: " "$message" "$prompt"
    read -r answer < /dev/tty || answer=""
    case "$(echo "$answer" | tr '[:upper:]' '[:lower:]')" in
        y|yes) return 0 ;; n|no) return 1 ;;
        "") [ "$default" = "y" ] && return 0 || return 1 ;;
        *) return 1 ;;
    esac
}

# ============================================================================
# CLEANUP & SAFETY
# ============================================================================

check_postgres_not_running() {
    log_info "Checking for running PostgreSQL instances..."
    if pgrep -x postgres > /dev/null 2>&1; then
        log_error "PostgreSQL is already running. Stop it first."
        pgrep -a postgres || true
        exit 1
    fi
    log_success "No other PostgreSQL instances running"
}

cleanup() {
    local rc=$?
    echo ""
    log_info "Cleaning up..."
    pkill -9 postgres > /dev/null 2>&1 || true
    sleep 1
    rm -rf /tmp/pgdata_* 2>/dev/null || true
    log_success "Cleanup complete"
    exit $rc
}

trap cleanup EXIT INT TERM

# ============================================================================
# EXTENSION MANAGEMENT
# ============================================================================

declare -A EXT_URLS=(
    [pg_stat_kcache]="https://github.com/powa-team/pg_stat_kcache.git"
    [pg_jobmon]="https://github.com/omniti-labs/pg_jobmon.git"
    [pg_partman]="https://github.com/pgpartman/pg_partman.git"
    [pgvector]="https://github.com/pgvector/pgvector.git"
    [documentdb]="https://github.com/documentdb/documentdb.git"
)

declare -A EXT_AVAILABLE=()
declare -A EXT_BUILT=()

check_and_clone_extensions() {
    log_info "Checking extension sources..."
    echo ""

    for ext in pg_stat_kcache pg_jobmon pg_partman pgvector documentdb; do
        local url="${EXT_URLS[$ext]}" dir="$EXT_DIR/$ext"

        if [ -d "$dir" ]; then
            log_success "$ext found at $dir"
            EXT_AVAILABLE[$ext]=1
        else
            log_warn "$ext not found at $dir"
            if prompt_yn "Clone $ext from $url?"; then
                mkdir -p "$EXT_DIR"
                local clone_log="$RESULTS_DIR/${ext}_clone.log"
                printf "    Cloning... "
                if git clone --depth 1 "$url" "$dir" > "$clone_log" 2>&1; then
                    echo "✓"
                    EXT_AVAILABLE[$ext]=1
                else
                    echo "✗"
                    tail -3 "$clone_log" 2>/dev/null | sed 's/^/      /'
                    EXT_AVAILABLE[$ext]=0
                fi
            else
                log_info "Skipping $ext"
                EXT_AVAILABLE[$ext]=0
            fi
        fi
    done
    echo ""
}

build_extension() {
    local ext=$1 install_dir=$2 log=$3

    [ "${EXT_AVAILABLE[$ext]:-0}" -ne 1 ] && { echo "skipped"; return 0; }

    : > "$log"

    case "$ext" in
        pg_stat_kcache|pg_jobmon|pg_partman|pgvector)
            (
                export PATH="$install_dir/bin:$PATH"
                cd "$EXT_DIR/$ext" 2>/dev/null || return 1
                "$GMAKE" clean >> "$log" 2>&1 || true
                "$GMAKE" USE_PGXS=1 PG_CONFIG="$install_dir/bin/pg_config" >> "$log" 2>&1 || return 1
                "$GMAKE" USE_PGXS=1 PG_CONFIG="$install_dir/bin/pg_config" install >> "$log" 2>&1 || return 1
            ) || return 1
            ;;
        documentdb)
            (
                export PATH="$install_dir/bin:$PATH"
                cd "$EXT_DIR/$ext" 2>/dev/null || return 1
                local built=0
                for subdir in pg_documentdb_core pg_documentdb; do
                    if [ -d "$subdir" ] && [ -f "$subdir/Makefile" ]; then
                        (
                            cd "$subdir"
                            "$GMAKE" clean >> "$log" 2>&1 || true
                            "$GMAKE" USE_PGXS=1 PG_CONFIG="$install_dir/bin/pg_config" >> "$log" 2>&1 || true
                            "$GMAKE" USE_PGXS=1 PG_CONFIG="$install_dir/bin/pg_config" install >> "$log" 2>&1 || true
                        )
                        built=1
                    fi
                done
                [ $built -eq 0 ] && return 1
            ) || return 1
            ;;
    esac

    return 0
}

build_all_extensions() {
    local version=$1 install_dir=$2
    log_info "Building extensions..."

    for ext in pg_stat_kcache pg_jobmon pg_partman pgvector documentdb; do
        EXT_BUILT[$ext]=0
        local log="$RESULTS_DIR/${version}_${ext}_build.log"
        printf "    %-20s" "$ext..."
        if build_extension "$ext" "$install_dir" "$log"; then
            echo "✓"
            EXT_BUILT[$ext]=1
        else
            echo "✗"
        fi
    done
    echo ""
}

# ============================================================================
# DATABASE SETUP
# ============================================================================

psql_exec() {
    local install_dir=$1 db=$2 log=$3
    "$install_dir/bin/psql" -X -A -t -q -d "$db" >> "$log" 2>&1
}

create_license_table() {
    local install_dir=$1 db=$2 log=$3

    log_info "Creating driver_license table (100k rows, 5 BTREE indexes)..."

    psql_exec "$install_dir" "$db" "$log" <<'SQL'
DROP TABLE IF EXISTS driver_license CASCADE;
CREATE TABLE driver_license (
    id       UUID NOT NULL DEFAULT gen_random_uuid(),
    seq_id   INT  NOT NULL,
    license_data JSONB NOT NULL,
    PRIMARY KEY (id)
);
CREATE INDEX idx_lic_seq       ON driver_license USING btree (seq_id);
CREATE INDEX idx_lic_last_name ON driver_license USING btree ((license_data->>'last_name'));
CREATE INDEX idx_lic_first_sdx ON driver_license USING btree (soundex(license_data->>'first_name'));
CREATE INDEX idx_lic_last_sdx  ON driver_license USING btree (soundex(license_data->>'last_name'));
CREATE UNIQUE INDEX idx_lic_ssn ON driver_license USING btree ((license_data->>'ssn'));
SQL

    psql_exec "$install_dir" "$db" "$log" <<'SQL'
INSERT INTO driver_license (seq_id, license_data)
SELECT i,
    jsonb_build_object(
        'first_name', (ARRAY['John','Jane','Bob','Alice','Charlie'])[floor(random()*5)+1],
        'last_name',  (ARRAY['Smith','Johnson','Williams','Brown','Jones'])[floor(random()*5)+1],
        'ssn',        lpad(i::text, 9, '0'),
        'license_id', 'LIC' || lpad(i::text, 12, '0'),
        'birth_date', (now() - ((15 + random()*55) * interval '1 year'))::date::text,
        'address',    '123 Main St',
        'height',     (60 + random()*12)::int,
        'weight',     (100 + random()*200)::int,
        'eye_color',  (ARRAY['Brown','Blue','Green','Hazel'])[floor(random()*4)+1],
        'hair_color', (ARRAY['Brown','Black','Blonde','Red','Gray'])[floor(random()*5)+1],
        'expires_date',(now() + random() * interval '10 years')::date::text,
        'issued_date', (now() - random() * interval '5 years')::date::text
    )
FROM generate_series(1, 100000) AS i;
VACUUM ANALYZE driver_license;
SQL

    local cnt=$("$install_dir/bin/psql" -X -t -A -d "$db" -c "SELECT count(*) FROM driver_license;" 2>/dev/null || echo "0")
    if [ "${cnt:-0}" -gt 0 ]; then
        log_success "driver_license ready ($cnt rows)"
        return 0
    else
        log_error "driver_license has no data"
        return 1
    fi
}

create_jsonb_table() {
    local install_dir=$1 db=$2 log=$3

    log_info "Creating t_jsonb table (10k rows, 3 BTREE expression indexes)..."

    psql_exec "$install_dir" "$db" "$log" <<'SQL'
DROP TABLE IF EXISTS t_jsonb CASCADE;
CREATE TABLE t_jsonb (id INT PRIMARY KEY, data JSONB NOT NULL);
CREATE INDEX idx_jsonb_cat    ON t_jsonb USING btree ((data->>'category'));
CREATE INDEX idx_jsonb_region ON t_jsonb USING btree ((data->>'region'));
CREATE INDEX idx_jsonb_score  ON t_jsonb USING btree (((data->>'score')::int));
INSERT INTO t_jsonb
SELECT i, jsonb_build_object(
    'category', (ARRAY['A','B','C','D'])[floor(random()*4)+1],
    'region',   (ARRAY['us-east','us-west','eu-west','ap-south'])[floor(random()*4)+1],
    'score',    floor(random()*1000)::int,
    'value',    0
) FROM generate_series(1, 10000) i;
VACUUM ANALYZE t_jsonb;
SQL
    log_success "t_jsonb ready (10k rows)"
}

create_gin_table() {
    local install_dir=$1 db=$2 log=$3

    log_info "Creating t_gin table (10k rows, GIN index — control)..."

    psql_exec "$install_dir" "$db" "$log" <<'SQL'
DROP TABLE IF EXISTS t_gin CASCADE;
CREATE TABLE t_gin (id INT PRIMARY KEY, data JSONB NOT NULL);
CREATE INDEX idx_gin_tags ON t_gin USING GIN ((data->'tags'));
INSERT INTO t_gin
SELECT i, jsonb_build_object('tags', jsonb_build_array('postgres','sql'), 'counter', 0)
FROM generate_series(1, 10000) i;
VACUUM ANALYZE t_gin;
SQL
    log_success "t_gin ready (10k rows, GIN — control)"
}

# ============================================================================
# SERVER MANAGEMENT
# ============================================================================

write_pg_config() {
    local datadir=$1 spl=$2
    cat > "$datadir/postgresql.conf" <<PGCONF
autovacuum = off
max_connections = 100
shared_buffers = 256MB
work_mem = 16MB
maintenance_work_mem = 128MB
wal_level = minimal
max_wal_senders = 0
fsync = off
synchronous_commit = off
full_page_writes = off
checkpoint_completion_target = 0.9
PGCONF
    [ -n "$spl" ] && echo "shared_preload_libraries = '$spl'" >> "$datadir/postgresql.conf"
}

start_server() {
    local install_dir=$1 datadir=$2 spl=$3 server_log=$4

    write_pg_config "$datadir" "$spl"

    if ! "$install_dir/bin/pg_ctl" -D "$datadir" -l "$server_log" start > /dev/null 2>&1; then
        log_error "Server failed to start. Check $server_log"
        cat "$server_log" | head -20 | sed 's/^/    /'
        return 1
    fi

    sleep 2

    if ! "$install_dir/bin/psql" -X -d postgres -c "SELECT 1;" > /dev/null 2>&1; then
        log_error "Server started but not responding. Check $server_log"
        cat "$server_log" | head -20 | sed 's/^/    /'
        return 1
    fi

    log_success "Server started"
    [ -n "$spl" ] && log_info "shared_preload_libraries: $spl"
    return 0
}

# ============================================================================
# TEST EXECUTION
# ============================================================================

extract_tps() {
    local file=$1
    grep "^tps = " "$file" 2>/dev/null | tail -1 | awk '{print $3}' || echo "0"
}

extract_latency() {
    local file=$1
    grep "^latency average" "$file" 2>/dev/null | awk '{print $4}' || echo "0"
}

run_test() {
    local version=$1 install_dir=$2 table=$3 workload=$4 log=$5
    local db="${table}_db"

    "$install_dir/bin/psql" -X -d "$db" -c "VACUUM ANALYZE;" >> "$log" 2>&1 || true

    local output_file="$RESULTS_DIR/${version}_${table}_${workload}.txt"

    # Run pgbench with explicit output
    if [ "$workload" = "tpcb-like" ] || [ "$workload" = "simple-update" ]; then
        "$install_dir/bin/pgbench" -n -c "$CLIENTS" -j "$JOBS" -T "$TEST_DURATION" \
            "$db" > "$output_file" 2>&1 || true
    else
        "$install_dir/bin/pgbench" -n -c "$CLIENTS" -j "$JOBS" -T "$TEST_DURATION" \
            -f "$RESULTS_DIR/${table}_${workload}.sql" "$db" > "$output_file" 2>&1 || true
    fi

    # Extract TPS and latency
    local tps=$(extract_tps "$output_file")
    local lat=$(extract_latency "$output_file")

    printf "    %-40s TPS: %-10s Lat: %-8s\n" "${table}_${workload}" "$tps" "${lat}ms"

    # Store for later analysis
    echo "$version|$table|$workload|$tps|$lat" >> "$RESULTS_DIR/results.txt"
}

run_concurrent_test() {
    local version=$1 install_dir=$2 table=$3 write_clients=$4 read_clients=$5 log=$6
    local db="${table}_db"
    local total_clients=$((write_clients + read_clients))

    "$install_dir/bin/psql" -X -d "$db" -c "VACUUM ANALYZE;" >> "$log" 2>&1 || true

    local write_output="$RESULTS_DIR/${version}_${table}_concurrent_${write_clients}w_${read_clients}r_write.txt"
    local read_output="$RESULTS_DIR/${version}_${table}_concurrent_${write_clients}w_${read_clients}r_read.txt"

    log_info "Running concurrent test: $write_clients writers + $read_clients readers..."

    # Start write workload in background
    "$install_dir/bin/pgbench" -n -c "$write_clients" -j "$JOBS" -T "$TEST_DURATION" \
        -f "$RESULTS_DIR/${table}_write_concurrent.sql" "$db" > "$write_output" 2>&1 &
    local write_pid=$!

    # Give writers a moment to start
    sleep 1

    # Run read workload concurrently
    "$install_dir/bin/pgbench" -n -c "$read_clients" -j "$JOBS" -T "$((TEST_DURATION - 2))" \
        -f "$RESULTS_DIR/${table}_read_concurrent.sql" "$db" > "$read_output" 2>&1 &
    local read_pid=$!

    # Wait for both to complete
    wait $write_pid 2>/dev/null || true
    wait $read_pid 2>/dev/null || true

    # Extract metrics
    local write_tps=$(extract_tps "$write_output")
    local write_lat=$(extract_latency "$write_output")
    local read_tps=$(extract_tps "$read_output")
    local read_lat=$(extract_latency "$read_output")

    printf "    %-40s Write: %-8s TPS  Read: %-8s TPS\n" "${table}_${write_clients}w_${read_clients}r" "$write_tps" "$read_tps"
    printf "    %-40s Write: %-8s ms   Read: %-8s ms\n" "" "$write_lat" "$read_lat"

    # Store for later analysis
    echo "$version|$table|concurrent_${write_clients}w_${read_clients}r_write|$write_tps|$write_lat" >> "$RESULTS_DIR/results.txt"
    echo "$version|$table|concurrent_${write_clients}w_${read_clients}r_read|$read_tps|$read_lat" >> "$RESULTS_DIR/results.txt"
}

# ============================================================================
# MAIN EXECUTION
# ============================================================================

mkdir -p "$RESULTS_DIR"
cd "$WORKDIR"

check_postgres_not_running

if ! GMAKE=$(detect_gmake); then
    log_error "GNU Make not found"
    exit 1
fi

log_header "CF-5556 PERFORMANCE TEST SUITE"

log_info "Configuration:"
log_info "  Test duration     : ${TEST_DURATION}s"
log_info "  Clients / Jobs    : $CLIENTS / $JOBS"
log_info "  Results directory : $RESULTS_DIR"
log_info "  Setup extensions  : $([ "$SETUP_EXTENSIONS" -eq 1 ] && echo "YES" || echo "NO")"
log_info "  Test extensions   : $([ "$TEST_EXTENSIONS" -eq 1 ] && echo "YES" || echo "NO")"
echo ""

# ============================================================================
# EXTENSION SETUP
# ============================================================================

if [ "$SETUP_EXTENSIONS" -eq 1 ]; then
    log_section "EXTENSION SETUP"
    check_and_clone_extensions
fi

# ============================================================================
# GIT SETUP PHASE
# ============================================================================

BASELINE_COMMIT=$(git rev-parse --short=11 origin/master)
PATCHES=($(ls -1 v*-*.patch 2>/dev/null | sort))
[ ${#PATCHES[@]} -eq 0 ] && { log_error "No v*-*.patch files found"; exit 1; }

git stash -q 2>/dev/null || true
git branch -D cf-5556-test-* 2>/dev/null || true
git checkout -q origin/master

log_section "BUILDING AND TESTING"
log_info "Baseline: $BASELINE_COMMIT (origin/master)"
log_info "Patches:  ${#PATCHES[@]} patch(es) to test cumulatively"
echo ""

pkill -9 postgres > /dev/null 2>&1 || true
sleep 2
rm -rf /tmp/pgdata_* 2>/dev/null || true

: > "$RESULTS_DIR/results.txt"

# ============================================================================
# BUILD AND TEST PHASE
# ============================================================================

test_version() {
    local version=$1 commit=$2
    local commit_short; commit_short=$(git rev-parse --short=11 "$commit")
    local build_dir="$WORKDIR/build-${commit_short}"
    local install_dir="$WORKDIR/install-${commit_short}"
    local datadir="/tmp/pgdata_${version}"
    local server_log="$RESULTS_DIR/${version}_server.log"
    local setup_log="$RESULTS_DIR/${version}_setup.log"

    log_section "VERSION: $version ($commit_short)"

    git checkout -q "$commit"

    # Build PostgreSQL
    if [ -d "$install_dir" ] && [ -f "$install_dir/bin/postgres" ]; then
        log_success "Using cached build"
    else
        log_info "Building PostgreSQL..."
        local blog="$RESULTS_DIR/${version}_build.log"
        rm -rf "$build_dir"
        if ! meson setup --buildtype=release -Dreadline=disabled \
            --prefix="$install_dir" "$build_dir" "$WORKDIR" > "$blog" 2>&1; then
            log_error "Meson setup failed. Check $blog"
            tail -10 "$blog" | sed 's/^/    /'
            exit 1
        fi
        if ! meson compile -C "$build_dir" >> "$blog" 2>&1; then
            log_error "Meson compile failed. Check $blog"
            tail -10 "$blog" | sed 's/^/    /'
            exit 1
        fi
        if ! meson install -C "$build_dir" >> "$blog" 2>&1; then
            log_error "Meson install failed. Check $blog"
            tail -10 "$blog" | sed 's/^/    /'
            exit 1
        fi
        log_success "PostgreSQL built"
    fi

    # Build extensions if requested
    if [ "$SETUP_EXTENSIONS" -eq 1 ]; then
        build_all_extensions "$version" "$install_dir"
    fi

    # Start server
    log_info "Starting server..."
    pkill -9 postgres > /dev/null 2>&1 || true
    sleep 1
    rm -rf "$datadir"
    "$install_dir/bin/initdb" -D "$datadir" > /dev/null 2>&1

    if ! start_server "$install_dir" "$datadir" "pg_stat_statements" "$server_log"; then
        exit 1
    fi

    # Create test databases and tables
    log_info "Setting up test databases..."
    : > "$setup_log"

    "$install_dir/bin/createdb" "license_db" 2>/dev/null || true
    psql_exec "$install_dir" "license_db" "$setup_log" <<'SQL'
CREATE EXTENSION IF NOT EXISTS fuzzystrmatch CASCADE;
CREATE EXTENSION IF NOT EXISTS pg_stat_statements CASCADE;
SQL

    if ! create_license_table "$install_dir" "license_db" "$setup_log"; then
        log_error "Failed to create license table"
        exit 1
    fi

    "$install_dir/bin/createdb" "jsonb_db" 2>/dev/null || true
    psql_exec "$install_dir" "jsonb_db" "$setup_log" <<'SQL'
CREATE EXTENSION IF NOT EXISTS pg_stat_statements CASCADE;
SQL
    create_jsonb_table "$install_dir" "jsonb_db" "$setup_log"

    "$install_dir/bin/createdb" "gin_db" 2>/dev/null || true
    psql_exec "$install_dir" "gin_db" "$setup_log" <<'SQL'
CREATE EXTENSION IF NOT EXISTS pg_stat_statements CASCADE;
SQL
    create_gin_table "$install_dir" "gin_db" "$setup_log"

    # Setup pgbench database
    "$install_dir/bin/createdb" "pgbench_db" 2>/dev/null || true
    "$install_dir/bin/pgbench" -i -s 10 "pgbench_db" >> "$setup_log" 2>&1

    echo ""
    log_info "Running isolated tests (${TEST_DURATION}s each)..."
    echo ""

    # Generate custom test SQL scripts
    cat > "$RESULTS_DIR/license_write_single.sql" <<'SQL'
\set row_id random(1, 100000)
UPDATE driver_license SET license_data = jsonb_set(license_data, '{address}', to_jsonb('Updated'::text)) WHERE seq_id = :row_id;
SQL

    cat > "$RESULTS_DIR/jsonb_write_single.sql" <<'SQL'
\set row_id random(1, 10000)
UPDATE t_jsonb SET data = jsonb_set(data, '{value}', to_jsonb(floor(random()*1e6)::int)) WHERE id = :row_id;
SQL

    cat > "$RESULTS_DIR/jsonb_write_batch.sql" <<'SQL'
\set base_id random(1, 9950)
UPDATE t_jsonb SET data = jsonb_set(data, '{value}', to_jsonb(floor(random()*1e6)::int)) WHERE id >= :base_id AND id < :base_id + 50;
SQL

    cat > "$RESULTS_DIR/gin_write_single.sql" <<'SQL'
\set row_id random(1, 10000)
UPDATE t_gin SET data = jsonb_set(data, '{counter}', to_jsonb(floor(random()*1e6)::int)) WHERE id = :row_id;
SQL

    # Run isolated tests
    run_test "$version" "$install_dir" "license" "write_single" "$setup_log"
    run_test "$version" "$install_dir" "jsonb" "write_single" "$setup_log"
    run_test "$version" "$install_dir" "jsonb" "write_batch" "$setup_log"
    run_test "$version" "$install_dir" "gin" "write_single" "$setup_log"
    run_test "$version" "$install_dir" "pgbench" "tpcb-like" "$setup_log"
    run_test "$version" "$install_dir" "pgbench" "simple-update" "$setup_log"

    echo ""
    log_info "Running concurrent read/write tests..."
    echo ""

    # Generate concurrent test SQL scripts
    cat > "$RESULTS_DIR/jsonb_write_concurrent.sql" <<'SQL'
\set row_id random(1, 10000)
UPDATE t_jsonb SET data = jsonb_set(data, '{value}', to_jsonb(floor(random()*1e6)::int)) WHERE id = :row_id;
SQL

    cat > "$RESULTS_DIR/jsonb_read_concurrent.sql" <<'SQL'
\set row_id random(1, 10000)
SELECT * FROM t_jsonb WHERE id = :row_id;
SQL

    cat > "$RESULTS_DIR/license_write_concurrent.sql" <<'SQL'
\set row_id random(1, 100000)
UPDATE driver_license SET license_data = jsonb_set(license_data, '{address}', to_jsonb('Updated'::text)) WHERE seq_id = :row_id;
SQL

    cat > "$RESULTS_DIR/license_read_concurrent.sql" <<'SQL'
\set row_id random(1, 100000)
SELECT * FROM driver_license WHERE seq_id = :row_id;
SQL

    # Run concurrent tests with varying write pressure
    run_concurrent_test "$version" "$install_dir" "jsonb" 2 6 "$setup_log"
    run_concurrent_test "$version" "$install_dir" "jsonb" 4 4 "$setup_log"
    run_concurrent_test "$version" "$install_dir" "jsonb" 6 2 "$setup_log"
    run_concurrent_test "$version" "$install_dir" "license" 2 6 "$setup_log"
    run_concurrent_test "$version" "$install_dir" "license" 4 4 "$setup_log"
    run_concurrent_test "$version" "$install_dir" "license" 6 2 "$setup_log"

    echo ""

    # Cleanup
    log_info "Stopping server..."
    "$install_dir/bin/pg_ctl" -D "$datadir" stop > /dev/null 2>&1 || true
    pkill -9 postgres > /dev/null 2>&1 || true
    log_success "Server stopped"
    echo ""
}

# Test baseline
test_version "baseline" "$BASELINE_COMMIT"

# Test all patches applied together (cumulative)
git checkout -q origin/master
git checkout -q -b "cf-5556-test-all-patches"

log_info "Applying all ${#PATCHES[@]} patches cumulatively..."
for patch_file in "${PATCHES[@]}"; do
    log_info "  Applying $patch_file..."
    if ! git am "$patch_file" > /dev/null 2>&1; then
        log_error "Failed to apply $patch_file"
        git am --abort 2>/dev/null || true
        exit 1
    fi
done

PATCHED_COMMIT=$(git rev-parse --short=11 HEAD)
test_version "patched" "$PATCHED_COMMIT"

# ============================================================================
# REPORTING
# ============================================================================

log_header "RESULTS SUMMARY"

if [ -f "$RESULTS_DIR/results.txt" ]; then
    echo ""
    echo "═══════════════════════════════════════════════════════════════════════════════════"
    echo "ISOLATED WORKLOAD COMPARISON (Patched vs Baseline)"
    echo "═══════════════════════════════════════════════════════════════════════════════════"
    printf "%-25s %-25s %12s %12s %10s\n" "Table" "Workload" "Baseline TPS" "Patched TPS" "Δ%"
    echo "───────────────────────────────────────────────────────────────────────────────────"

    # Parse results and compute deltas
    declare -A baseline_tps
    declare -A patched_tps

    while IFS='|' read -r version table workload tps lat; do
        if [[ "$workload" != concurrent* ]]; then
            key="${table}|${workload}"
            if [ "$version" = "baseline" ]; then
                baseline_tps["$key"]="$tps"
            else
                patched_tps["$key"]="$tps"
            fi
        fi
    done < "$RESULTS_DIR/results.txt"

    for key in "${!baseline_tps[@]}"; do
        btps="${baseline_tps[$key]}"
        ptps="${patched_tps[$key]:-0}"

        if [ -n "$btps" ] && [ -n "$ptps" ] && [ "$btps" != "0" ] && [ "$ptps" != "0" ]; then
            delta=$(awk "BEGIN {printf \"%.1f\", ($ptps - $btps) / $btps * 100}")
            table=$(echo "$key" | cut -d'|' -f1)
            workload=$(echo "$key" | cut -d'|' -f2)
            printf "%-25s %-25s %12.1f %12.1f %+9.1f%%\n" "$table" "$workload" "$btps" "$ptps" "$delta"
        fi
    done | sort

    echo "───────────────────────────────────────────────────────────────────────────────────"
    echo ""
    echo "═══════════════════════════════════════════════════════════════════════════════════"
    echo "CONCURRENT WORKLOAD ANALYSIS (Write Pressure Impact on Reads)"
    echo "═══════════════════════════════════════════════════════════════════════════════════"
    printf "%-25s %-20s %12s %12s %12s %12s\n" "Table" "Write:Read" "Base Write" "Patch Write" "Base Read" "Patch Read"
    echo "───────────────────────────────────────────────────────────────────────────────────"

    declare -A conc_results

    while IFS='|' read -r version table workload tps lat; do
        if [[ "$workload" == concurrent* ]]; then
            conc_results["${version}|${table}|${workload}"]="$tps"
        fi
    done < "$RESULTS_DIR/results.txt"

    for ratio in "2w_6r" "4w_4r" "6w_2r"; do
        for table in "jsonb" "license"; do
            bw="${conc_results[baseline|${table}|concurrent_${ratio}_write]:-0}"
            pw="${conc_results[patched|${table}|concurrent_${ratio}_write]:-0}"
            br="${conc_results[baseline|${table}|concurrent_${ratio}_read]:-0}"
            pr="${conc_results[patched|${table}|concurrent_${ratio}_read]:-0}"

            if [ -n "$bw" ] && [ -n "$pw" ] && [ -n "$br" ] && [ -n "$pr" ] && \
               [ "$bw" != "0" ] && [ "$pw" != "0" ] && [ "$br" != "0" ] && [ "$pr" != "0" ]; then
                printf "%-25s %-20s %12.1f %12.1f %12.1f %12.1f\n" "$table" "$ratio" "$bw" "$pw" "$br" "$pr"
            fi
        done
    done

    echo "───────────────────────────────────────────────────────────────────────────────────"
    echo ""
    echo "Output files:"
    echo "  $RESULTS_DIR/results.txt (raw results)"
    echo "  $RESULTS_DIR/*_server.log (server startup/error logs)"
    echo "  $RESULTS_DIR/*_setup.log (database setup logs)"
    echo "  $RESULTS_DIR/*_build.log (build logs)"
    echo "  $RESULTS_DIR/*_*.txt (pgbench output)"
    echo "  $RESULTS_DIR/*_*.sql (test queries)"
    echo ""
fi

git checkout -q - 2>/dev/null || true
