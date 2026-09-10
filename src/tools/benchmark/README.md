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

## P1 (fork-vs-XTC apples-to-apples matrix): `mtpg_p1_matrix.sh`

plan_docs/FORK_TO_XTC_PERF_PLAN.md item P1: ONE command that runs the fixed
section-1 methodology for BOTH lanes (fork, xtc) and BOTH workloads (pgbench
`-S` read-mostly, `tpcb-like` write-heavy) over a client x carrier grid, on a
SEPARATE loadgen host, and emits `results.tsv` + a companion `latencies.tsv`
(percentiles via `pgbench_pctl`).

    LOADGEN=ec2-user@<loadgen-private-ip> SUT_IP=<sut-private-ip> \
      PGBIN=/path/to/inst/usr/local/pgsql/bin \
      LANES='fork xtc' CARRIERS='auto' WORKLOADS='select tpcb' \
      CLIENTS='16 32 64' SCALE=100 DURATION=120 WARMUP=30 RUNS=3 \
      OUT=/mnt/nvme/p1out DATA=/mnt/nvme/p1data \
      bash src/tools/benchmark/mtpg_p1_matrix.sh

Every run gets a FRESH `initdb` + start + stop (requirement 1 below) --
expensive but the only way to avoid the half-dead-postmaster cascade that once
fabricated a 7/8 "hang rate" that was really 4/6 (some "hangs" were a reused
server stuck fork-failing after a prior timeout-kill).

It hard-fails at startup unless `LOADGEN` is a host distinct from `SUT_IP`
(requirement 7); pass `--local-driver` to force a co-located smoke run, which
tags every emitted row `degraded=yes` and `idle_meaningful=no` so a co-located
number can never later be mistaken for a headline (this is exactly the class
of error `.ec2/writeheavy-rootcause-profile-2026-08-27.md` made and had to be
retroactively caveated).

Each `results.tsv` row carries the full confound context on its own
(requirement 8): driver host, SUT host, carriers requested/effective, clients,
shared_buffers (+ % of RAM), fsync/synchronous_commit/full_page_writes,
io_method, data device, WAL device, libxtc version (`pkg-config --modversion
xtc`), PG commit -- so methodology is auditable from the TSV alone.

`pooled_protocol_carriers` is asserted, not assumed (requirement 5): the xtc
lane checks `SHOW pooled_protocol_carriers` resolved to the requested value (or
something sane for `auto`) AND that `pg_stat_xtc_carriers` has rows, failing
the run loudly (`ASSERT_FAIL ...`) rather than silently benchmarking an
unpooled thread-per-session server.

A failed tps parse is `TPS_PARSE_FAIL` in the `tps` column with the raw
pgbench output kept on disk and pg_stat_activity wait-event diagnostics
captured into the row's `notes` column (requirement 4 + 3) -- never a silent
`NA` that could be misread as "ran, produced nothing."  Diagnostics are always
gathered before any server stop, so a `-m immediate` FATAL-flood at teardown
can never be captured and mistaken for a crash.

Stall detection is monitor-independent: a background sampler polls
`sum(xact_commit+xact_rollback) from pg_stat_database` every 10s across the
measured window and flags a frozen counter (>1/3 of samples unchanged) as
`STALL:n/m` in the `stall` column.

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
