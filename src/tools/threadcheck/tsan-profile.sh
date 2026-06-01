#!/usr/bin/env bash
#
# tsan-profile.sh - configure, build, and exercise PostgreSQL under
# ThreadSanitizer (F1.4 of docs/threading/F1_CLASSIFICATION_HARNESS.md).
#
# This is the NON-GATING TSan profile: it builds with -Db_sanitize=thread and
# runs the core regression suite in threaded mode, collecting any data-race
# reports.  PostgreSQL is still multi-process today (the threaded runtime is
# dormant while multithreaded=off), so this is wired as a nightly to track
# burn-down, not a hard gate -- it becomes gating once threaded mode is clean.
#
# The suppressions file (tsan_suppressions.txt, next to this script) is the
# TSan analogue of the threadcheck/srclint ratcheting baselines: drive it to
# empty as subsystems convert.
#
# Usage:
#   src/tools/threadcheck/tsan-profile.sh [BUILDDIR]
#
# Environment:
#   CC, CXX        compiler to use (must support -fsanitize=thread; clang or
#                  recent gcc).  Defaults to the meson auto-detected compiler.
#   MESON_EXTRA    extra meson setup args (e.g. -Dllvm=disabled).
#
# Exit status is informational only (the report is the artifact); a non-zero
# regression-suite result is surfaced but does not, by itself, mean a TSan
# failure.  Inspect $BUILDDIR/tsan.log for the race reports.

set -u

HERE=$(cd "$(dirname "$0")" && pwd)
SRCROOT=$(cd "$HERE/../../.." && pwd)
BUILDDIR=${1:-"$SRCROOT/build-tsan"}
SUPP="$HERE/tsan_suppressions.txt"

# halt_on_error=0 keeps the suite running so we collect ALL reports in one
# pass; history_size widens TSan's stack history; the suppressions file hides
# the justified known patterns.
export TSAN_OPTIONS="halt_on_error=0:history_size=4:second_deadlock_stack=1:suppressions=$SUPP:log_path=$BUILDDIR/tsan.log"

echo "tsan-profile: source   = $SRCROOT"
echo "tsan-profile: build    = $BUILDDIR"
echo "tsan-profile: suppress = $SUPP"

if [ ! -d "$BUILDDIR" ]; then
	# shellcheck disable=SC2086
	meson setup "$BUILDDIR" "$SRCROOT" \
		-Db_sanitize=thread \
		-Dcassert=true \
		-Dbuildtype=debugoptimized \
		${MESON_EXTRA:-} || { echo "tsan-profile: meson setup failed"; exit 2; }
else
	echo "tsan-profile: reusing existing $BUILDDIR"
fi

ninja -C "$BUILDDIR" || { echo "tsan-profile: build failed"; exit 2; }

echo "tsan-profile: running core regression suite under TSan ..."
# The race reports go to $BUILDDIR/tsan.log.<pid> via log_path; the suite's
# own pass/fail is reported but is not the gate.
meson test -C "$BUILDDIR" --suite setup --suite regress --print-errorlogs
suite_rc=$?

shopt -s nullglob
logs=("$BUILDDIR"/tsan.log*)
if [ ${#logs[@]} -gt 0 ]; then
	races=$(grep -c "WARNING: ThreadSanitizer" "${logs[@]}" 2>/dev/null | \
		awk -F: '{s+=$NF} END {print s+0}')
	echo "tsan-profile: regression suite rc=$suite_rc; TSan reports=$races"
	echo "tsan-profile: see ${logs[*]}"
else
	echo "tsan-profile: regression suite rc=$suite_rc; no tsan.log produced"
	echo "tsan-profile: (a clean run, or the build is not actually TSan-instrumented)"
fi

# Non-gating: always exit 0 so a nightly does not fail the pipeline on
# pre-existing races.  Flip this to 'exit $((races > 0))' to make it gating.
exit 0
