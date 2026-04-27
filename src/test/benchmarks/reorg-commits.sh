#!/usr/bin/env bash
#
# reorg-commits.sh -- Reorganize the arc branch commits into a clean series.
#
# This script restructures the buffer pool commits from their development
# order into a logical merge-ready sequence:
#
#   1. "Add support for multiple buffer pools, strategies, and relation-pool binding"
#      (core infrastructure, RECYCLE, trickle writer, oversubscription, OVERFLOW, JAM)
#   2. "Add the Least-Recently Used (LRU) buffer pool strategy"
#   3. "Add the OSIC buffer pool strategy from LeanStore"
#   4. "Add the Adaptive Replacement Cache (ARC) buffer pool strategy"
#   5. "Add the Clock with Adaptive Replacement (CAR) buffer pool strategy"
#   6. "Add the LIRS-2 buffer pool strategy"
#   7. "[NOT FOR MERGE] Benchmarking for BUFFER POOLs"
#
# Safety:
#   - Creates a backup branch (arc-pre-reorg) before any changes
#   - Creates the new history on a temporary branch (arc-reorg)
#   - Tests compilation at each commit
#   - Does NOT force-push; you must do that manually
#
# Usage:
#   ./reorg-commits.sh [--dry-run]
#
# After running successfully:
#   git checkout arc
#   git reset --hard arc-reorg
#   git push --force-with-lease origin arc
#
# To revert:
#   git checkout arc
#   git reset --hard arc-pre-reorg

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
cd "$REPO_ROOT"

DRY_RUN=0
if [[ "${1:-}" == "--dry-run" ]]; then
    DRY_RUN=1
    echo "[DRY RUN] Would perform the following operations:"
    echo ""
fi

# Base commit: last non-buffer-pool commit (dev setup v27)
BASE_COMMIT="a490352273d"

# Current buffer pool commits (development order, oldest first)
# Updated after rebase onto origin/master (2026-04-25)
C_INFRA="5112c5f92cf"      # Multi-buffer-pool infrastructure with DSM-backed dynamic pools
C_FIXES="cbf7fb5612f"      # Bug fixes, integration tests, and hardening
C_ARC_FIX="b3c81a02458"    # Fix ARC VACUUM crash, add hardening, + benchmark suite
C_EXTRACT="99ffc657be5"    # Extract ARC to contrib, add CAR
C_BENCHSTRESS="7f89d64675a" # Cache-stress workloads, adaptation sampling, eviction charts
C_TESTS="5f3d3c7dd7d"      # Algorithm-specific behavioral tests for ARC and CAR
C_STRATS="a93804b1526"     # LRU, OSIC, LIRS strategies, trickle iterators, runtime swap
C_OVERFLOW="4077a99dbdd"   # overflow_buffer_pool reloption
C_AMCALLBACK="4f9a9b97787" # AM overflow pool callback and TOAST inheritance
C_CASCADE="63e23e96230"    # CASCADE overflow_buffer_pool to TOAST
C_JAM="6856fa17b34"        # JAM buffer pool strategy
C_TRICKLE="ddb3290b89a"    # Trickle writer GUCs and writeback capacity
C_RECYCLE="333ceed83b1"    # RECYCLE pool with one-chance clock sweep
C_ROUTE="06866a1caba"      # Route BufferAccessStrategy through RECYCLE
C_OVERSUB="a4b5b6d7cfb"    # Oversubscription tracking and nudging
C_BENCHPAT="4d7b69c055e"   # Cache-eviction benchmark workload patterns
C_SIGSEGV="eea90d4de17"    # Fix trickle writer SIGSEGV on pool destruction
C_HAMMERDB="f802e294108"   # HammerDB integration, profiling, extended benchmarks
C_REMAINDER="1e8a4bc0fba"  # REMAINDER pool for auto-sized unclaimed buffer space
C_DIRECTIO="889bb2ff5b8"   # Per-pool bp_use_direct_io flag
C_ALTER="c1230a71b46"      # ALTER SET SIZE/options, unified pool model, hash partitions

echo "=== Buffer Pool Commit Reorganization ==="
echo ""
echo "Base commit: $BASE_COMMIT"
echo "Current tip: $(git rev-parse --short HEAD)"
echo ""

if [[ $DRY_RUN -eq 1 ]]; then
    echo "Target commit structure:"
    echo ""
    echo "  1. Core infrastructure (squash of 15 commits):"
    echo "     - $C_INFRA Multi-buffer-pool infrastructure"
    echo "     - $C_FIXES Bug fixes and hardening"
    echo "     - $C_ARC_FIX (core parts: freelist.c, bufmgr.c, bufpool.c, bufpool.h)"
    echo "     - $C_STRATS (core parts: trickle iterators, runtime algorithm swap)"
    echo "     - $C_OVERFLOW overflow_buffer_pool reloption"
    echo "     - $C_AMCALLBACK AM overflow pool callback"
    echo "     - $C_CASCADE CASCADE overflow_buffer_pool"
    echo "     - $C_JAM JAM buffer pool strategy"
    echo "     - $C_TRICKLE Trickle writer GUCs"
    echo "     - $C_RECYCLE RECYCLE pool"
    echo "     - $C_ROUTE BufferAccessStrategy routing"
    echo "     - $C_OVERSUB Oversubscription tracking"
    echo "     - $C_SIGSEGV Fix trickle writer SIGSEGV"
    echo "     - $C_REMAINDER REMAINDER pool"
    echo "     - $C_DIRECTIO Per-pool direct_io flag"
    echo "     - $C_ALTER ALTER SET SIZE/options, unified pool model, hash partitions"
    echo ""
    echo "  2. LRU (from $C_STRATS): contrib/pg_bp_lru/*"
    echo "  3. OSIC (from $C_STRATS): contrib/pg_bp_osic/*"
    echo "  4. ARC (from $C_EXTRACT + $C_ARC_FIX + $C_TESTS): contrib/pg_bp_arc/*"
    echo "  5. CAR (from $C_EXTRACT + $C_TESTS): contrib/pg_bp_car/*"
    echo "  6. LIRS (from $C_STRATS): contrib/pg_bp_lirs/*"
    echo "  7. Benchmarks (from $C_ARC_FIX + $C_BENCHSTRESS + $C_BENCHPAT + $C_HAMMERDB)"
    echo ""
    echo "Run without --dry-run to execute."
    exit 0
fi

# Safety: backup
echo "Creating backup branch: arc-pre-reorg"
git branch -f arc-pre-reorg HEAD

# Create temporary working branch
echo "Creating temporary branch: arc-reorg from $BASE_COMMIT"
git checkout -B arc-reorg "$BASE_COMMIT"

build_test() {
    local label="$1"
    echo "  Building ($label)..."
    if ! meson compile -C build >/dev/null 2>&1; then
        echo "  ERROR: Build failed after $label"
        echo "  Restore with: git checkout arc && git reset --hard arc-pre-reorg"
        exit 1
    fi
    echo "  Build OK"
}

#######################################################################
# Commit 1: Core infrastructure
# Squash all core buffer pool changes into one commit
#######################################################################
echo ""
echo "=== Commit 1: Core infrastructure ==="

# Apply commits in order, squashing
for commit in \
    "$C_INFRA" "$C_FIXES" "$C_ARC_FIX" "$C_STRATS" \
    "$C_OVERFLOW" "$C_AMCALLBACK" "$C_CASCADE" "$C_JAM" \
    "$C_TRICKLE" "$C_RECYCLE" "$C_ROUTE" "$C_OVERSUB" "$C_SIGSEGV" \
    "$C_REMAINDER" "$C_DIRECTIO" "$C_ALTER"; do

    short_msg=$(git log --oneline -1 "$commit")
    echo "  Cherry-picking: $short_msg"

    # For C_ARC_FIX: exclude benchmark files (they go in commit 7)
    if [[ "$commit" == "$C_ARC_FIX" ]]; then
        git cherry-pick --no-commit "$commit" 2>/dev/null || git checkout --theirs . 2>/dev/null || true
        # Unstage benchmark files -- they'll be in commit 7
        git reset HEAD -- src/test/benchmarks/ >/dev/null 2>&1 || true
        git checkout -- src/test/benchmarks/ 2>/dev/null || true
        continue
    fi

    # For C_STRATS: exclude individual algorithm contrib dirs (they go in commits 2-6)
    # Keep core changes (trickle iterators, runtime algorithm swap, contrib/meson.build)
    if [[ "$commit" == "$C_STRATS" ]]; then
        git cherry-pick --no-commit "$commit" 2>/dev/null || git checkout --theirs . 2>/dev/null || true
        # Unstage individual algorithm extensions (keep contrib/meson.build)
        git reset HEAD -- contrib/pg_bp_lru/ contrib/pg_bp_osic/ contrib/pg_bp_lirs/ >/dev/null 2>&1 || true
        git checkout -- contrib/pg_bp_lru/ contrib/pg_bp_osic/ contrib/pg_bp_lirs/ 2>/dev/null || true
        continue
    fi

    # For C_EXTRACT: only take core changes (freelist.c, system_views, guc), not ARC/CAR contrib
    if [[ "$commit" == "$C_EXTRACT" ]]; then
        git cherry-pick --no-commit "$commit" 2>/dev/null || git checkout --theirs . 2>/dev/null || true
        # Unstage ARC and CAR contrib dirs
        git reset HEAD -- contrib/pg_bp_arc/ contrib/pg_bp_car/ >/dev/null 2>&1 || true
        git checkout -- contrib/pg_bp_arc/ contrib/pg_bp_car/ 2>/dev/null || true
        continue
    fi

    git cherry-pick --no-commit "$commit" 2>/dev/null || {
        echo "    Resolving conflicts..."
        # Accept theirs for all conflicts (we're building incrementally)
        git checkout --theirs . 2>/dev/null || true
        git add -u 2>/dev/null || true
    }
done

# Ensure benchmark files are not included
git reset HEAD -- src/test/benchmarks/ >/dev/null 2>&1 || true

git commit -m "$(cat <<'EOF'
Add support for multiple buffer pools, strategies, and relation-pool binding

Introduce a multi-buffer-pool architecture for PostgreSQL that allows
multiple buffer replacement strategies to coexist. Key features:

- CREATE/ALTER/DROP BUFFER POOL DDL with pluggable handler interface
- DSM-backed dynamic pools with per-pool trickle writers
- Well-known pools: DEFAULT (clock-sweep) and RECYCLE (replacing ring buffers)
- RECYCLE pool with three modes: bulk_read, bulk_write, vacuum
- Trickle writer iteration API for strategy-specific write-back ordering
- Runtime algorithm swap via buffer_pool_algorithm GUC
- Oversubscription tracking with trickle writer nudging
- Relation-pool binding via buffer_pool reloption on tables and indexes
- overflow_buffer_pool reloption for TOAST/overflow data routing
- AM callback for default overflow pool selection
- CASCADE of overflow pool changes to TOAST tables
- JAM buffer pool strategy optimized for TOAST access patterns
- BufferAccessStrategy routing through RECYCLE pool
- pg_stat_bufferpool system view for pool-level statistics
EOF
)"

build_test "commit 1: core infrastructure"

#######################################################################
# Commit 2: LRU
#######################################################################
echo ""
echo "=== Commit 2: LRU buffer pool strategy ==="

# Extract LRU files from C_STRATS
git cherry-pick --no-commit "$C_STRATS" 2>/dev/null || {
    git checkout --theirs . 2>/dev/null || true
    git add -A 2>/dev/null || true
}
# Keep only LRU contrib dir + meson.build update
git reset HEAD 2>/dev/null
git add contrib/pg_bp_lru/ contrib/meson.build 2>/dev/null || true
git checkout -- . 2>/dev/null || true

git commit -m "$(cat <<'EOF'
Add the Least-Recently Used (LRU) buffer pool strategy

Classic doubly-linked LRU list providing the simplest possible buffer
replacement algorithm. Serves as a baseline for benchmarking more
sophisticated strategies.

Operations are O(1): on_hit moves to MRU end, get_victim takes from
LRU end. No ghost lists or adaptation -- purely recency-based.

Vulnerable to sequential scan pollution but useful as a reference
implementation and for workloads with strong temporal locality.

Install: CREATE EXTENSION pg_bp_lru;
Usage:   CREATE BUFFER POOL mypool SIZE '64MB' HANDLER lru_pool_handler;
EOF
)"

build_test "commit 2: LRU"

#######################################################################
# Commit 3: OSIC
#######################################################################
echo ""
echo "=== Commit 3: OSIC buffer pool strategy ==="

git cherry-pick --no-commit "$C_STRATS" 2>/dev/null || {
    git checkout --theirs . 2>/dev/null || true
    git add -A 2>/dev/null || true
}
git reset HEAD 2>/dev/null
git add contrib/pg_bp_osic/ contrib/meson.build 2>/dev/null || true
git checkout -- . 2>/dev/null || true

git commit -m "$(cat <<'EOF'
Add the OSIC buffer pool strategy from LeanStore

Implements the One-Shot Item Cache (OSIC) algorithm based on the
LeanStore paper. Key advantage: lock-free hot path -- on_hit only
clears an atomic cool flag, avoiding spinlock contention.

Two-stage eviction: hot -> cool -> evicted. Partitioned clock hands
reduce contention across cores. Inflight counter prevents over-eviction
during I/O storms.

Well-suited for NUMA systems and high-concurrency OLTP where spinlock
contention on the buffer replacement algorithm is a bottleneck.

Install: CREATE EXTENSION pg_bp_osic;
Usage:   CREATE BUFFER POOL mypool SIZE '64MB' HANDLER osic_pool_handler;
EOF
)"

build_test "commit 3: OSIC"

#######################################################################
# Commit 4: ARC
#######################################################################
echo ""
echo "=== Commit 4: ARC buffer pool strategy ==="

# Apply ARC extraction commit (only ARC parts)
git cherry-pick --no-commit "$C_EXTRACT" 2>/dev/null || {
    git checkout --theirs . 2>/dev/null || true
    git add -A 2>/dev/null || true
}
git reset HEAD 2>/dev/null
git add contrib/pg_bp_arc/ contrib/meson.build 2>/dev/null || true
git checkout -- . 2>/dev/null || true

# Apply ARC-specific parts of the fix commit
git cherry-pick --no-commit "$C_ARC_FIX" 2>/dev/null || {
    git checkout --theirs . 2>/dev/null || true
    git add -A 2>/dev/null || true
}
git reset HEAD 2>/dev/null
git add contrib/pg_bp_arc/ 2>/dev/null || true
git checkout -- . 2>/dev/null || true

# Apply ARC test improvements
git cherry-pick --no-commit "$C_TESTS" 2>/dev/null || {
    git checkout --theirs . 2>/dev/null || true
    git add -A 2>/dev/null || true
}
git reset HEAD 2>/dev/null
git add contrib/pg_bp_arc/ 2>/dev/null || true
git checkout -- . 2>/dev/null || true

# Apply ARC changes from C_STRATS (updated pg_bp_arc.c for trickle iterators)
git cherry-pick --no-commit "$C_STRATS" 2>/dev/null || {
    git checkout --theirs . 2>/dev/null || true
    git add -A 2>/dev/null || true
}
git reset HEAD 2>/dev/null
git add contrib/pg_bp_arc/ 2>/dev/null || true
git checkout -- . 2>/dev/null || true

git commit --allow-empty -m "$(cat <<'EOF'
Add the Adaptive Replacement Cache (ARC) buffer pool strategy

Implements the ARC algorithm which adapts cache behavior to workload
patterns by maintaining four lists: T1 (recency), T2 (frequency),
B1 (T1 ghost entries), B2 (T2 ghost entries). Ghost list hits drive
adaptive tuning of the T1/T2 partition point.

The IBM patent on ARC expired in 2024, making this implementation
freely usable.

Includes algorithm-specific behavioral tests verifying ghost list
adaptation and frequency promotion under workload shifts.

Install: CREATE EXTENSION pg_bp_arc;
Usage:   CREATE BUFFER POOL mypool SIZE '64MB' HANDLER arc_pool_handler;
Monitor: SELECT * FROM pg_stat_arc;
EOF
)"

build_test "commit 4: ARC"

#######################################################################
# Commit 5: CAR
#######################################################################
echo ""
echo "=== Commit 5: CAR buffer pool strategy ==="

git cherry-pick --no-commit "$C_EXTRACT" 2>/dev/null || {
    git checkout --theirs . 2>/dev/null || true
    git add -A 2>/dev/null || true
}
git reset HEAD 2>/dev/null
git add contrib/pg_bp_car/ contrib/meson.build 2>/dev/null || true
git checkout -- . 2>/dev/null || true

git cherry-pick --no-commit "$C_TESTS" 2>/dev/null || {
    git checkout --theirs . 2>/dev/null || true
    git add -A 2>/dev/null || true
}
git reset HEAD 2>/dev/null
git add contrib/pg_bp_car/ 2>/dev/null || true
git checkout -- . 2>/dev/null || true

git cherry-pick --no-commit "$C_STRATS" 2>/dev/null || {
    git checkout --theirs . 2>/dev/null || true
    git add -A 2>/dev/null || true
}
git reset HEAD 2>/dev/null
git add contrib/pg_bp_car/ 2>/dev/null || true
git checkout -- . 2>/dev/null || true

git commit --allow-empty -m "$(cat <<'EOF'
Add the Clock with Adaptive Replacement (CAR) buffer pool strategy

Implements CAR, a clock-based variant of ARC that replaces LRU list
operations with clock hand sweeps. Maintains the same four-list
adaptive structure (T1/T2/B1/B2) but uses reference bits instead of
list position, reducing per-access overhead.

Particularly effective for concurrent workloads where the clock-based
approach avoids the list-manipulation contention of pure ARC.

Install: CREATE EXTENSION pg_bp_car;
Usage:   CREATE BUFFER POOL mypool SIZE '64MB' HANDLER car_pool_handler;
Monitor: SELECT * FROM pg_stat_car;
EOF
)"

build_test "commit 5: CAR"

#######################################################################
# Commit 6: LIRS
#######################################################################
echo ""
echo "=== Commit 6: LIRS-2 buffer pool strategy ==="

git cherry-pick --no-commit "$C_STRATS" 2>/dev/null || {
    git checkout --theirs . 2>/dev/null || true
    git add -A 2>/dev/null || true
}
git reset HEAD 2>/dev/null
git add contrib/pg_bp_lirs/ contrib/meson.build 2>/dev/null || true
git checkout -- . 2>/dev/null || true

git commit --allow-empty -m "$(cat <<'EOF'
Add the LIRS-2 buffer pool strategy

Implements the Low Inter-reference Recency Set (LIRS) algorithm which
uses inter-reference recency (IRR) -- the number of distinct pages
accessed between two consecutive accesses to the same page -- as the
primary replacement metric.

Maintains LIR (Low IRR, hot) and HIR (High IRR, cold) sets with a
ghost queue for tracking evicted pages. Pages with low IRR stay
resident; pages with high IRR are eviction candidates.

Particularly resistant to sequential scan pollution and effective for
workloads with cyclic access patterns larger than the cache.

Install: CREATE EXTENSION pg_bp_lirs;
Usage:   CREATE BUFFER POOL mypool SIZE '64MB' HANDLER lirs_pool_handler;
Monitor: SELECT * FROM pg_stat_lirs;
EOF
)"

build_test "commit 6: LIRS"

#######################################################################
# Commit 7: Benchmarks (NOT FOR MERGE)
#######################################################################
echo ""
echo "=== Commit 7: Benchmarks ==="

# Apply all benchmark-related changes
C_REORG="d507422024e"     # Commit reorganization script (original)

for commit in "$C_ARC_FIX" "$C_BENCHSTRESS" "$C_BENCHPAT" "$C_HAMMERDB" "$C_REORG"; do
    short_msg=$(git log --oneline -1 "$commit")
    echo "  Cherry-picking benchmark parts: $short_msg"

    git cherry-pick --no-commit "$commit" 2>/dev/null || {
        git checkout --theirs . 2>/dev/null || true
        git add -u 2>/dev/null || true
    }
    git reset HEAD 2>/dev/null

    # Only stage benchmark files
    git add src/test/benchmarks/ 2>/dev/null || true
    git checkout -- . 2>/dev/null || true
done

git commit --allow-empty -m "$(cat <<'EOF'
[NOT FOR MERGE] Benchmarking for BUFFER POOLs

Comprehensive benchmarking infrastructure for comparing buffer pool
replacement algorithms:

Python benchmark suite (src/test/benchmarks/):
- 6 algorithms: clock, arc, car, lirs, lru, osic
- 19 query patterns targeting specific cache behaviors
- Configurable test matrix: schemas x row counts x distributions
- Adaptation sampling with time-series visualization
- HTML dashboard and CSV export

HammerDB integration (src/test/benchmarks/hammerdb/):
- TPC-C (OLTP) and TPC-H (analytical) workloads
- Per-algorithm orchestration with pool stats collection
- Configurable warehouse count, duration, and virtual users

Profiling scripts (src/test/benchmarks/profiling/):
- DTrace (.d) and bpftrace (.bt) scripts for:
  - Spinlock hold times in buffer pool code
  - List traversal lengths during victim selection
  - Ghost list operation rates (ARC/CAR/LIRS adaptation)
  - Buffer hit rate time-series sampling
- Linux perf stat/record with flamegraph generation

Extended benchmark runner (run_extended.sh):
- Full matrix: 6 algorithms x 3 pool sizes x 3 row counts x 19 patterns
- HammerDB TPC-C/H per algorithm with profiling
- Quick mode for development iteration
EOF
)"

echo ""
echo "=== Reorganization complete ==="
echo ""
echo "New commit history on arc-reorg:"
git log --oneline "$BASE_COMMIT"..HEAD
echo ""
echo "To apply:"
echo "  git checkout arc"
echo "  git reset --hard arc-reorg"
echo "  git push --force-with-lease origin arc"
echo ""
echo "To revert:"
echo "  git checkout arc"
echo "  git reset --hard arc-pre-reorg"
