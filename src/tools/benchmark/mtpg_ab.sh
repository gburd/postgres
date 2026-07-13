#!/usr/bin/env bash
#
# mtpg_ab.sh -- Phase 18 A/B perf gate.
#
# Every Phase 18 libxtc-fusion adoption must be neutral-or-better on
# check-threaded-pooled perf against the version it replaces (see
# plan_docs/MULTITHREADED_PLAN.md).  This script standardizes that measurement:
# build the branch twice (baseline commit, then candidate commit), point the
# pgbench matrix at both installs, run the threaded + pooled lanes, and report
# the per-workload median tps delta with a pass/fail on a configurable
# regression threshold.
#
# It wraps src/tools/benchmark/mtpg_pgbench_matrix.pl, which already supports
# two install trees (--vanilla-install/--branch-install) and lane selection;
# here "vanilla" lane = the BASELINE build, "branch" lanes = the CANDIDATE.
#
# Usage:
#   mtpg_ab.sh --baseline=DIR --candidate=DIR [--runs=N] [--threshold=PCT] [matrix opts...]
#
#   --baseline=DIR    tmp_install of the baseline build (before the adoption)
#   --candidate=DIR   tmp_install of the candidate build (with the adoption)
#   --runs=N          pgbench runs per datapoint, median reported (default 5)
#   --threshold=PCT   max allowed median regression, percent (default 2.0)
#
# Any extra args pass through to mtpg_pgbench_matrix.pl (e.g. --clients,
# --pool-sizes, --workloads, --duration).  Intended for a bare-metal-ish box
# (EC2 m6id.8xlarge); on the dev host the numbers are too noisy to gate on.
#
# ponytail: wraps the existing matrix tool rather than reimplementing the
# harness; the pass/fail delta math is the only new logic and it has a self-test
# (mtpg_ab_selftest).  Upgrade path: if per-workload weighting or CI wiring is
# needed, add it here, not in the matrix tool.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
matrix="$here/mtpg_pgbench_matrix.pl"

baseline="" candidate="" runs=5 threshold=2.0
passthru=()
for arg in "$@"; do
	case "$arg" in
		--baseline=*)  baseline="${arg#*=}" ;;
		--candidate=*) candidate="${arg#*=}" ;;
		--runs=*)      runs="${arg#*=}" ;;
		--threshold=*) threshold="${arg#*=}" ;;
		--selftest)    exec "$here/mtpg_ab_selftest" ;;
		*)             passthru+=("$arg") ;;
	esac
done

if [[ -z "$baseline" || -z "$candidate" ]]; then
	echo "usage: mtpg_ab.sh --baseline=DIR --candidate=DIR [--runs=N] [--threshold=PCT] [matrix opts...]" >&2
	exit 2
fi
for d in "$baseline" "$candidate"; do
	[[ -x "$d/usr/local/pgsql/bin/postgres" || -x "$d/bin/postgres" ]] \
		|| { echo "no postgres under install tree: $d" >&2; exit 2; }
done

out="/tmp/mtpg_ab_$(date +%Y%m%d_%H%M%S)"
echo "== A/B: baseline=$baseline candidate=$candidate runs=$runs threshold=${threshold}% =="

# vanilla lane = baseline; branch_pool = candidate under the pooled protocol.
perl "$matrix" \
	--vanilla-install="$baseline" \
	--branch-install="$candidate" \
	--client-install="$baseline" \
	--runs="$runs" \
	--lanes=vanilla,branch_pool \
	--out-dir="$out" \
	"${passthru[@]}"

# The matrix writes a TSV; delta + gate is computed by the self-tested helper.
"$here/mtpg_ab_gate" --results="$out" --threshold="$threshold"
