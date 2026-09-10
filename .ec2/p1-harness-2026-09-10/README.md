# P1 harness validation (2026-09-10, bene account, us-east-2)

Validates plan_docs/FORK_TO_XTC_PERF_PLAN.md item P1: `src/tools/benchmark/mtpg_p1_matrix.sh`,
the one-command fork-vs-XTC matrix (both lanes, both workloads, section-1 methodology).
This is a HARNESS validation run, not a headline number: small instances, short durations,
low scale.  A production headline run needs a bigger box, longer duration, more runs
(`RUNS>=3`), and section-1's full client/carrier sweep.

## Instances (both `c6id.2xlarge`, terminated after this run -- see below)

- SUT:     `i-0145a09f52fe77011`, private `172.31.30.124`, key/tag `xtc-p1b-20260910-035300`
- LOADGEN: `i-0dcb100a4f8607506`, private `172.31.18.93`,  key/tag `xtc-p1b-20260910-035300`
- Region us-east-2, subnet `subnet-0bfe4b750cdc33c45` (us-east-2a); SG allowing SSH from
  my IP + intra-SG all-traffic.
- libxtc pinned to v1.43.0 (build recipe adapted from
  `.ec2/loop-poll-2026-09-07/xtcpg_build.sh`, NOT `buildA.sh`).
- PG built from this branch at commit `090687ff2735956b776766e974340fb69dd642e9`
  (extracted from a `git archive` tarball on both hosts -- no `.git`, hence the
  harness's `PG_COMMIT=` override, added in that same commit).
- Root volumes 150GB gp3 (per the task's "an earlier run filled 80GB" warning).
- `pooled_protocol_carriers=-1` (auto) used for the xtc lane -- resolved to 8 (one
  carrier per core on this 8-vCPU box), asserted via `SHOW` + `pg_stat_xtc_carriers`
  by the harness itself (requirement 5).

## Run 1: co-located smoke -- proves requirement 7 fires (`results_colo_tainted.tsv`)

`LOADGEN` unset, `--local-driver` passed (forces it through).  Both rows: fork tps
181,823, xtc tps 176,468, and:

    degraded         = yes
    idle_meaningful   = no
    driver_host       = ip-172-31-30-124...(colocated)

This demonstrates requirement 7's tainting: a co-located driver number is emitted
(so the harness stays usable for a fast local smoke check) but is UNMISTAKABLY
marked so it can never later pass for a headline number -- the exact failure mode
that confounded `.ec2/writeheavy-rootcause-profile-2026-08-27.md`.

Earlier in this same session, running the SAME co-located command as `root` (via
`sudo bash`) produced a clean `INITDB_FAIL` row instead of a crash or a silent bad
number -- `initdb: error: cannot be run as root` was surfaced loudly in `notes` per
requirement 3 (diagnostics before any teardown) and requirement 4 (loud failure, not
a silently-parsed NA).  Included as `LOG_colo.txt`.

## Run 2: real two-host validation -- requirement 7 does NOT fire (`results_2host.tsv`)

`LOADGEN=ec2-user@172.31.18.93` (a separate EC2 instance), `SUT_IP=172.31.30.124`
(the SUT's own private IP).  Both lanes (`fork`, `xtc`), both workloads (`select`
read-mostly, `tpcb` write-heavy), clients `8 16`, scale 20, 30s duration + 10s warmup,
1 run per cell (8 cells total).  All 8 cells completed; every row shows:

    degraded         = no
    idle_meaningful   = yes
    driver_host       = ec2-user@172.31.18.93          (distinct from sut_host)
    sut_host          = ip-172-31-30-124...

No `ASSERT_FAIL`, no `TPS_PARSE_FAIL`, no `INIT_FAIL`, no `STALL`.  Every row carries
the full confound context per requirement 8: `carriers_eff=8` for every xtc row,
`shared_buffers_mb=13346` (85% of the 15GB host RAM), `fsync=on synchronous_commit=on
full_page_writes=on io_method=sync`, `data_device=wal_device=/dev/nvme1n1` (R3:
single device, both lanes identical), `libxtc_version=1.43.0`,
`pg_commit=090687ff2735956b776766e974340fb69dd642e9`.

Numbers (8 vCPU box, small scale, short duration -- illustrative only, NOT a
headline; see caveats below):

| bench  | lane | clients | tps    | p95_ms | p99_ms | max_ms   |
|--------|------|---------|--------|--------|--------|----------|
| select | fork | 8       | 54,856 | 0.161  | 0.182  | 16.1     |
| select | fork | 16      | 95,931 | 0.202  | 0.224  | 2.7      |
| select | xtc  | 8       | 54,394 | 0.165  | 0.194  | 5.6      |
| select | xtc  | 16      | 54,704 | 0.161  | 0.181  | 29,979.2 |
| tpcb   | fork | 8       | 6,095  | 1.604  | 2.638  | 15.1     |
| tpcb   | fork | 16      | 9,921  | 2.077  | 2.527  | 26.4     |
| tpcb   | xtc  | 8       | 6,444  | 1.463  | 1.686  | 16.6     |
| tpcb   | xtc  | 16      | 6,319  | 1.494  | 1.725  | 29,982.4 |

Notable, and exactly the kind of signal this harness is supposed to surface
faithfully: BOTH xtc c=16 cells (select and tpcb) show a ~30s outlier max latency
(p99 stays healthy: 0.181ms / 1.725ms) while fork's worst outlier stays under 27ms.
This is consistent with the pooled-scheduler stall behavior already documented
across the plan docs (B1/B2, the cross-loop wake gap) -- the harness's per-row
`max_ms` caught a real straggler transaction without needing the stall detector to
fire (the STALL column looks at `pg_stat_database` commit-count freezing over 10s
windows within a 30s run; a single straggler among ~1.6M transactions does not
freeze the aggregate counter, so `stall=no` is correct -- the outlier shows up in
the latency tail exactly where it should).  Not a claim about B1/B2 status; just
evidence the harness reports what actually happened rather than smoothing it away.

## What this validates

- Fresh initdb/start/stop per run works (requirement 1): 10 total server
  start/stops across the two runs (2 colo cells + 8 two-host cells), no
  reused-server carryover.
- `pgbench -i` timeout wrapping did not need to fire (no init hung), but the
  INITDB_FAIL path was exercised for real (the root-user case) and produced a
  well-formed, loud row.
- Diagnostics-before-teardown (requirement 3): every completed cell's log shows
  the `diag: active_backends=... cpu_busy=...%` line BEFORE `stop_lane` runs.
- Loud tps-parse failure (requirement 4): exercised earlier in local smoke testing
  (see the main commit series) with a genuine `pgbench: error` and a missing-binary
  `command not found`; both became `TPS_PARSE_FAIL` rows, never silent NA.
- Carrier-count assertion (requirement 5): every xtc row's `carriers_eff=8` was
  actually asserted via `SHOW pooled_protocol_carriers` + `pg_stat_xtc_carriers`
  row count before the run proceeded.
- Binary-exists check (requirement 6): exercised earlier in local smoke testing
  with a missing `PGBIN`; produced a loud `MISSING BINARY` exit 2, no masked build
  failure.
- Driver/SUT separation (requirement 7): both directions demonstrated -- co-located
  taints (Run 1), separate does not (Run 2).
- Confound context (requirement 8): present in full on every row of both runs.
- Gated idle% (requirement 9): `idle_meaningful=no` throughout Run 1,
  `idle_meaningful=yes` throughout Run 2.

## What this does NOT validate (explicitly out of scope for this harness pass)

- NOT a headline throughput number: 8 vCPU / 15GB box, 20-30s durations, RUNS=1,
  only two client points.  Section-1's real methodology wants a bare-metal-ish box,
  >=2min rampup + >=5min steady window, 3 repeats/median, and the full VU sweep.
- Does NOT exercise HammerDB (TPROC-C/TPROC-H) -- this harness is pgbench-based;
  HammerDB integration is flagged as a follow-up lane in the README rather than
  reimplemented (the existing `mtpg_hammerdb_bench.sh` plumbing is reused, not
  duplicated).
- Does NOT prove or disprove B1/B2 (the pooled-scheduler wake/feeding gaps) -- the
  30s outlier above is consistent with those known issues but this was not a
  root-cause investigation and drew no conclusion beyond "the harness reported it
  faithfully."
- `RAM_PCT=85` used the DEFAULT (per the task: leave it a parameter, do not
  hardcode away from 85); R1 (85% vs 25%) was not re-litigated here.
- huge_pages was requested as `try` (not hard `on`) because the small validation
  instances were not pre-provisioned with hugepages; a real headline run should
  set `huge_pages=on` with hugepages sized to `shared_buffers` beforehand, per
  section 1.

## Teardown

Both instances terminated, security group deleted, key pair deleted, local `.pem`
shredded, 5-region sweep for `key-name=xtc-p1b-*` performed.  See the harness task
final report for the terminate/verify transcript.


--------------------------------------------------------------------------------
## Reviewer note (2026-09-10): two findings in this validation data worth escalating

The harness was the deliverable, but its first real two-host run produced two results that are
themselves evidence. Recording them here so they are not lost as "harness noise".

### 1. xtc does not scale from c=8 to c=16; fork nearly doubles

```
select  fork  c=8   54,855 tps      select  xtc(eff=8)  c=8   54,394 tps
select  fork  c=16  95,931 tps      select  xtc(eff=8)  c=16  54,704 tps   <-- FLAT
tpcb    fork  c=8    6,095 tps      tpcb   xtc(eff=8)   c=8    6,444 tps
tpcb    fork  c=16   9,921 tps      tpcb   xtc(eff=8)   c=16   6,319 tps   <-- FLAT/DOWN
```

At c=8 xtc MATCHES fork on select (0.99x) and BEATS it on tpcb (1.06x). At c=16 fork scales
1.75x/1.63x while xtc stays flat -- ending at 0.57x and 0.64x. Note `carriers_eff=8` on an
8-vCPU c6id.2xlarge: at c=16 there are 2 sessions per carrier, which is exactly the
multiplexing regime. Flat-not-degrading is the signature of a concurrency ceiling (a serialization
point or a scheduler that will not overlap 2 sessions per carrier), not of overload.

This is measured on a small box with a 30 s cell, so it is a POINTER, not a headline. But it
matches the "carrier under-utilization / scheduler feeding" hypothesis in
FORK_TO_XTC_PERF_PLAN.md P4 -- and unlike the 2026-08-27 RCA that hypothesis came from, this run
has a SEPARATE loadgen, so it is not confounded by driver CPU contention. That makes it the first
clean evidence for the P4 lever.

### 2. A ~30 s max latency in BOTH xtc c=16 cells -- on a 30 s run

```
select  xtc  c=16  max_ms = 29,979.164     (99.9 % of the entire 30 s run)
tpcb    xtc  c=16  max_ms = 29,982.415     (99.9 %)
fork, both workloads, both client counts: max_ms <= 26.4
```

p50/p95/p99 stay healthy (0.181 ms / 1.725 ms at p99), so this is ONE session starved for
essentially the whole run while its peers ran normally. That is a starvation/lost-wake shape, not
a latency tail.

**Crucially, `select` is pgbench `-S`: read-only, no WAL, no commit, no fsync.** So this is NOT
the `xtc_aio_fdatasync` lost-wake we have open with libxtc, and NOT the checkpointer/fiber
buffer-lock deadlock in THREADED_TEST_BASELINE_2026-09.md (this ran the pooled lane and completed).
It is a third starvation shape, reproducible in both workloads, appearing exactly when
sessions > carriers.

**Caveat, stated plainly:** n=1 per cell, 30 s, 8 vCPU, and I have not re-run it. Two cells
agreeing is suggestive, not conclusive. The falsifiable next step is cheap: re-run the c=16 xtc
cells N times with `PG_XTC_TAIL=1` and see whether a fiber shows a PARK with no matching RUN while
its loop keeps polling -- the same instrument that localized the fdatasync bug. If it reproduces,
it is a better repro than the fsync hang (read-only, no I/O, 30 s, deterministic-ish) and should
become the primary lost-wake test case.
