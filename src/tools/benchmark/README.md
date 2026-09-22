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
- `pgbench_pctl` -- latency percentiles (p50/p95/p99) from a pgbench `--log`
  file.  pgbench itself reports only AVERAGE latency, but the project goal
  requires p95/p99 parity with the fork model, so run pgbench with `--log` and
  post-process the per-transaction log:

      pgbench -n -M prepared -S -c16 -j16 -T30 --log --log-prefix=/tmp/pgb ...
      src/tools/benchmark/pgbench_pctl --glob '/tmp/pgb.*'
      # -> count=... avg=..ms p50=..ms p95=..ms p99=..ms max=..ms

  `--selftest` runs assert-based unit tests of the percentile math.  Report the
  candidate's p50/p95/p99 next to the baseline's when landing a perf change --
  TPS parity is necessary but not sufficient; tail latency (p95/p99) must not
  regress either.

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

## Finding the target first: `mtpg_profile.sh`

Before picking a Phase 18 adoption, PROVE where the threaded/pooled per-command
overhead is (the plan's ~35% gap is a hypothesis, not a measurement).

    src/tools/benchmark/mtpg_profile.sh --install=/path/tmp_install --clients=16 --duration=30

It profiles the same build in a `process` lane and a `threaded_pooled` lane under
`perf record` (CPU-bound prepared SELECT) and writes a top-symbol list per lane.
Symbols that dominate the pooled lane but not the process lane are the
per-command threaded overhead -- target those, then A/B the fix with the gate
above.  Needs `perf` and `kernel.perf_event_paranoid <= 1`.

## P1 external-driver matrix: `mtpg_p1_matrix.sh`

The primary comparison is **upstream merge-base stock, branch fork, branch
fiber**. `fork` is this branch with `multithreaded=off`; it is NOT stock.
The default lanes are `fork fiber`. Optional `stock` requires `STOCK_PGBIN`
and `STOCK_COMMIT`: supply an independently built upstream merge-base install,
not an arbitrary upstream tip or another branch build. This harness neither
builds binaries nor verifies their source provenance.

    LOADGEN=admin@<driver-ip> SUT_IP=<sut-ip> \
      PGBIN=/path/to/branch/bin PG_COMMIT=<branch-revision> \
      STOCK_PGBIN=/path/to/upstream-merge-base/bin STOCK_COMMIT=<merge-base> \
      LANES='stock fork fiber' FIBER_LOOPS=16 WORKLOADS='select tpcb' \
      CLIENTS='16 32 64' SCALE=100 DURATION=300 WARMUP=120 RUNS=3 \
      OUT=/mnt/nvme/p1out DATA=/mnt/nvme/p1data \
      bash src/tools/benchmark/mtpg_p1_matrix.sh

Run ON the SUT as a non-root database owner, using dedicated destructive
`DATA` scratch space and a disjoint `OUT`. Do not run concurrent invocations
against the same DATA, OUT or port. An existing `postmaster.pid` is refused,
even if stale: inspect and stop that server yourself before removing it.
A fresh initdb/server is used per cell. Failed shutdown aborts the matrix,
retaining the server/data for manual cleanup; it never kills by a path regex.
The server has no fixed lifetime; the bounded stages and exit trap own shutdown.
Network access is trust-authenticated: use isolated benchmark hosts and restrict
port access at the firewall. This is not a production deployment tool.

Prerequisites: Linux, Bash, GNU timeout/coreutils, Perl, tar, SSH and optionally
sysstat/mpstat. Preinstall/verify SSH host keys; `LOADGEN_KEY` remains supported.
The driver needs Bash, timeout, tar and pgbench (`LOADGEN_PGBIN`, default PATH).
Use the SAME measured pgbench binary for every lane. Server and driver version
strings are retained, but equal version strings do not prove identical builds.
`PG_COMMIT` defaults to the harness checkout revision, not the binary revision;
override it when those differ. `pkg-config`'s libxtc version is advisory, not
proof of the server's loaded library/backend. Verify these separately.

Compatibility and defaults:

- Existing variables and the 29-column `results.tsv` layout remain supported.
  Per-row `notes` now identifies a unique artifact directory; old flat log
  filenames are replaced by these directories to prevent overwrite.
- `LANES='fork xtc' CARRIERS='auto 4 0'` remains available for diagnostics.
  `xtc` is the legacy stackless-pool lane, not the primary performance target.
  Its `carriers_req/eff` describe the pooled-protocol GUC (0 means fiber).
- `fiber` sets `pooled_protocol_carriers=0`; its carrier columns describe
  executor loops. `FIBER_LOOPS` defaults to min(online CPUs,256), and explicitly
  sets `PG_XTC_CARRIER_LOOPS` in threaded lanes, including legacy `xtc`.
  Both the pooled GUC and independent executor row count are asserted. These
  are different pools: the carrier view does not measure stackless pool size.
  Single-loop executors have no view rows and are deliberately unsupported;
  specify `FIBER_LOOPS=2..1024` (also on single-core smoke hosts).
- Defaults: WORKLOADS='select tpcb', CLIENTS='16 32', SCALE=50, RAM_PCT=85,
  DURATION=60, WARMUP=15, RUNS=1, PORT=5439, TIMEOUT_GRACE=60.
  WARMUP=0 skips warmup. Warmup and measurement have separate duration+grace
  deadlines, enforced locally and on the driver, with a 5-second kill grace.
  Initialization retains its 300-second deadline. Use RUNS>=3 for comparisons;
  rows remain individual runs, not medians. Lanes alternate within each repeat.

Separation fails closed: unset LOADGEN, equal addresses after stripping SSH
users, equal Linux boot IDs (including aliases), or failed identity queries
exit 2. `--local-driver` / LOCAL_DRIVER=1 unconditionally runs locally, marks
all rows `degraded=yes`, and forces `idle_meaningful=no`. Boot ID is an
operational check between trusted hosts, not hardware attestation; containers,
VMs, routing and a misdirected SUT_IP still require operator verification.

Warmup and measured stdout/stderr, commands, raw transaction logs, configs,
SHOW ALL, version/context records, CPU samples and progress samples are retained
in unique per-cell directories on success and failure. Remote logs are fetched
as a tar archive, extracted locally, and summarized by the existing
`pgbench_pctl`; remote originals are intentionally NOT deleted. The saved
`driver-logs.txt` gives their prefix for recovery/cleanup. Budget disk space
on both hosts, verify collected files before deleting remote originals, and
retain OUT with the report. Percentiles still sort all samples in memory.

`mpstat` runs concurrently with measurement (including client startup and
failure/timeout time), not afterward. CPU is NA when no samples exist; idle is
meaningful only with samples and a separate driver. A sampler connects to
**template1**, querying only the workload database **postgres** every 10 seconds:
its own commits cannot manufacture workload progress. Frozen counters across
more than one third of intervals yield `STALL:n/m`; fewer than two intervals
yield `UNKNOWN`. Sampler query errors fail the cell. This aggregate heuristic
is NOT per-client fairness validation; other activity/autovacuum in postgres
can still mask a stall. Inspect retained per-client transaction logs separately.

Exit 0 means commands and artifact processing succeeded, NOT a performance or
fairness gate: inspect stall/degraded/idle fields. Exit 1 means a cell failed;
exit 2 means preflight/artifact-output setup failed. Failures preserve stage and
return code in notes (for example WARMUP_FAIL rc=7, PGBENCH_TIMEOUT rc=124,
LOG_FETCH_FAIL), rather than classifying every nonzero exit as a TPS parse
failure. GNU timeout's kill escalation can return 137 rather than 124; that
exact code is retained without claiming whether the cause was timeout or an
external kill. `TPS_PARSE_FAIL` remains in the TPS column for parse-only errors.
Diagnostics are captured before teardown; shutdown errors are in stop.log.
Interrupted invocations retain existing files but may lack a completed row;
remote commands remain subject to their own deadlines. Disk exhaustion cannot
guarantee artifact retention. Redirect the harness's console output to a file
outside DATA as well, particularly for preflight failures.

Local checks (mock external commands only; no PostgreSQL/SSH/AWS validation):

    src/tools/benchmark/mtpg_p1_selftest
    src/tools/benchmark/mtpg_ab_selftest
    bash -n src/tools/benchmark/mtpg_p1_matrix.sh src/tools/benchmark/mtpg_p1_selftest
    shellcheck src/tools/benchmark/mtpg_p1_matrix.sh src/tools/benchmark/mtpg_p1_selftest

## Steady-state, external-driver A/B: `mtpg_remote_bench.sh` + `mtpg_ec2_ab_provision.sh`

The gate above (necessary-minimum viability) proves neutral-or-better.  The
PROJECT GOAL is stronger: BEAT the fork model on multiple dimensions (TPS, tail
latency, memory) in **apples-to-apples, steady-state, constant-heavy-load** runs
of meaningful duration.  Two host-noise problems make the in-box matrix
inadequate for that claim: (1) pgbench on the SUT steals client CPU from the
server, and (2) short runs measure warmup/transients, not steady state.

`mtpg_remote_bench.sh` fixes both.  It runs ON the SUT but drives pgbench from a
SEPARATE load-driver host over the private network, discards a warmup window,
and measures for a long steady-state duration -- capturing, per
`(workload x clients x carriers)` cell for BOTH process and threaded lanes:
median TPS, p50/p95/p99/p99.9 latency (from the driver's pgbench `--log`), SUT
PSS memory (fair cross-model: sums PSS over all postgres procs/threads), and SUT
CPU utilization.

`mtpg_ec2_ab_provision.sh` stands up the two-instance cluster (SUT + LOADGEN) in
one subnet and a cluster **placement group** for low-latency, low-jitter private
networking, and prints the exact build + run + teardown commands.

    # 1) provision (SUT + LOADGEN in a cluster placement group)
    SG=sg-... SUBNET=subnet-... KEY=xtc-p17 \
      bash src/tools/benchmark/mtpg_ec2_ab_provision.sh
    # 2) build PG on the SUT (release + frame pointers), install to inst/
    # 3) run the external-driver matrix on the SUT:
    LOADGEN=ec2-user@<loadgen-private-ip> SUT_IP=<sut-private-ip> \
      PGBENCH=/mnt/nvme/work/pg/inst/usr/local/pgsql/bin/pgbench \
      CARRIERS='auto 16 32 64' CLIENTS='16 32 64 128' \
      WORKLOADS='tpcb select update' DURATION=120 WARMUP=30 SCALE=100 \
      bash src/tools/benchmark/mtpg_remote_bench.sh
    # 4) TERMINATE both instances when done (they cost money).

Interpreting: `CARRIERS='auto ...'` includes the shipped auto-default (one
carrier per core) plus an explicit sweep so we see whether the default is
neutral-or-better vs. hand-tuned, and where threaded BEATS process (idle-heavy /
high-connection / bursty patterns are where the fork model pays a per-process
tax the pooled model does not).  The LOADGEN host needs the same `pgbench` binary
and `libpq` reachable on its PATH/`LD_LIBRARY_PATH`.


## P1 requirement audit of the pre-existing scripts (2026-09-10)

plan_docs/FORK_TO_XTC_PERF_PLAN.md P1 lists nine hardening requirements, each a
real bug that produced a false result in an earlier session.  Auditing the
scripts that predate `mtpg_p1_matrix.sh` against that list, so the gap is on
record rather than rediscovered:

| # | requirement | mtpg_matrix.sh | mtpg_hammerdb_bench.sh | mtpg_remote_bench.sh |
|---|---|---|---|---|
| 1 | fresh server per run | VIOLATES: one server per (lane,vu) cell, not per run; the schema is built once and every VU/lane cell restarts postgres but reuses the same data dir for the whole matrix | OK-ish: `start_pg`/`stop_pg` restart postgres per (mode,carriers,vu) cell and detect+kill a stale postmaster.pid, but the data dir (and any accumulated bloat/state) persists across the whole VU sweep | VIOLATES: `start_server`/`stop_server` re-`initdb` per (workload,carriers) cell but reuse the SAME running server across the whole `CLIENTS` sweep within that cell |
| 2 | `pgbench -i` under `timeout` | N/A directly -- schema build goes through HammerDB's `buildschema`, not pgbench | N/A directly -- same, HammerDB `buildschema` | VIOLATES: `"$PGBIN/pgbench" -i -s $SCALE` calls are unguarded, no `timeout` wrapper |
| 3 | diagnostics captured before teardown | PARTIAL: samples `pg_stat_database`/context-switches/RSS during the run and before `stop_lane`, but the HammerDB-side NOPM grep happens via an ssh call issued right before `stop_lane` with only an informal ordering, not an explicit pre-stop snapshot | OK: `stop_pg` issues an explicit `CHECKPOINT` before `pg_ctl -m fast`, specifically to avoid the immediate-stop FATAL flood, but there is no separate diagnostic (wait-event) snapshot step | VIOLATES: no diagnostic capture step at all; `stop_server` is a bare `kill`/`kill -9` with no checkpoint and no pre-stop snapshot |
| 4 | loud tps-parse failure | N/A (HammerDB NOPM, not pgbench tps) | N/A (HammerDB NOPM) | VIOLATES: `drive()` greps for `tps` and falls back to a bare `${tps:-NA}` with no distinction between "no transactions completed" and "the grep pattern broke" |
| 5 | assert effective carrier count | OK: `start_lane` asserts `mt=on` and `carriers=-1` before proceeding, returns failure otherwise | OK: `start_pg` hard-asserts `pooled_protocol_carriers` and `pg_stat_xtc_carriers` row count are both nonzero, aborting the lane if the pool is not actually up | VIOLATES: reads `show pooled_protocol_carriers` into `eff` purely for the results-row label; never compares it against what was requested, never checks `pg_stat_xtc_carriers` |
| 6 | no `| tail -N` masking a failed build | N/A -- none of the three build PostgreSQL; they consume an already-built `PGBIN` | N/A | N/A |
| 7 | separate driver enforced | VIOLATES: `LOADGEN` and `SUT_IP` are required env vars but never checked against each other; a co-located run is not prevented and no output is ever tainted | VIOLATES: same pattern -- required but unchecked, no tainting | VIOLATES: same pattern -- required but unchecked, no tainting |
| 8 | confound context in every row | PARTIAL: `results.tsv` carries carriers/rss/cpu/csw/stall but no driver/SUT host column and no fsync/synchronous_commit/io_method/device columns | VIOLATES: `hresults.tsv` has only bench/mode/carriers/vu/nopm/tpm/pss/cpu -- no host, no durability flags, no device, even though `DURABILITY` is a script parameter it is not recorded per row | VIOLATES: `results.tsv` has tps/latency/pss/cpu but no host, no durability flags, no shared_buffers, no device columns |
| 9 | idle%/CPU gated on requirement 7 | VIOLATES: reports `cpu` unconditionally; there is no requirement-7 tracking to gate on | VIOLATES: same | VIOLATES: same |

Net: the closest of the three to the P1 bar was `mtpg_hammerdb_bench.sh`
(requirement 5 solid, requirement 3 partly covered by the explicit
checkpoint-before-stop), but none of the three enforce requirement 7 -- the one
that actually confounded a real study (the 2026-08-27 write-heavy RCA) -- and
none carry the full confound context per row.  `mtpg_p1_matrix.sh` was written
fresh to close all nine at once rather than patch three scripts with divergent
per-cell/per-run granularity; the HammerDB scripts remain the right tool once a
Tcl/Java driver install is available on the loadgen, and can be wired in as an
additional lane in `mtpg_p1_matrix.sh` later rather than reimplemented.
