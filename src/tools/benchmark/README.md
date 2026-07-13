# Phase 18 A/B perf gate

Phase 18 (libxtc deduplication/fusion) requires every adoption to be
**neutral-or-better** on `check-threaded-pooled` perf against the version it
replaces (see `plan_docs/MULTITHREADED_PLAN.md`).  This directory provides the
standardized measurement so each adoption is gated identically.

## Tools

- `mtpg_pgbench_matrix.pl` -- the underlying pgbench matrix runner (lanes,
  workloads, pool sizes, warmup/duration, median-over-runs, optional resource
  sampling).  Point it at two install trees with `--vanilla-install` /
  `--branch-install` (or set `MTPG_VANILLA_INSTALL`); it has no hardcoded paths.
- `mtpg_ab.sh` -- the Phase 18 A/B wrapper: given a baseline build and a
  candidate build, runs the baseline + pooled-candidate lanes and gates on a
  regression threshold.
- `mtpg_ab_gate` -- parses a run's `tps.tsv` (median tps per lane/workload),
  computes per-workload deltas, and exits non-zero if any workload regresses
  beyond the threshold.  `--selftest` runs assert-based unit tests of the delta
  math (also reachable via `mtpg_ab_selftest`).

## Usage

Build the branch twice (baseline commit, candidate commit) into separate install
trees, then:

    src/tools/benchmark/mtpg_ab.sh \
        --baseline=/path/to/baseline/tmp_install \
        --candidate=/path/to/candidate/tmp_install \
        --runs=5 --threshold=2.0 \
        --clients=32 --pool-sizes=8,24,48 --duration=60

Exit 0 = neutral-or-better on every workload (adoption may land); exit 1 = a
workload regressed beyond `--threshold` percent.

## Where to run

On a bare-metal-ish box (EC2 m6id.8xlarge or similar); the dev host's numbers are
too noisy to gate on.  Keep the two builds identical except for the adoption
under test, and prefer several runs (`--runs`) so the reported median is stable.
