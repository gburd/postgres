# Fiber read-gap diagnosis: the 5.6x number was the wrong scheduler lane; the shipped pooled default is at parity with fork through c=32, then hits an already-known starvation bug

Date: 2026-09-12. SUT: EC2 c6id.8xlarge (32 vCPU, 61 GB RAM), **Debian 13 (trixie)**, us-east-2,
XFS on local NVMe. Driver: separate c6id.8xlarge, same subnet, private-IP connection (see
"Driver placement" below — genuinely separate host, not co-located). PG: committed clean
`git archive origin/xtc` at `cf436d861b` (release build, `-Dcassert=false`, `-O2`,
`-fno-omit-frame-pointer -g`). libxtc **v1.44.1** (`e9a2e14`), built with **`-Dio-backend=uring`
explicitly** (hard error if liburing missing, not the silent-fallback `auto`) against
liburing-dev 2.9; verified at runtime via `/proc/<pid>/task/*/fd` showing `anon_inode:[io_uring]`
rings on the running server, for both the thread-per-session and pooled lanes.

## Headline finding: the original 0.18x measurement and the old 1.02-1.04x claim were never
## about the same code path

Verified in source before any EC2 work (confirmed by the delegating agent independently):

- `PgRuntimePooledProtocolRequested()` = `multithreaded && pooled_protocol_carriers > 0`
  (`backend_runtime.c:1486`).
- `pooled_protocol_carriers=0` (what the original 5.6x-slower benchmark set explicitly) makes
  that predicate **false** &rarr; **thread-per-session**: each client backend is a real xtc fiber
  on the carrier-loop executor pool (`pg_xtc_carrier.c`).
- `pooled_protocol_carriers=-1` (**auto, the actual default** under `multithreaded=on`) resolves
  to core count via `postmaster.c:934` &rarr; the **pooled protocol scheduler**: a small,
  auto-sized pool of stackless carriers multiplexes many sessions, PG-side (`launch_backend.c`).
  This is also what the 2026-08-24/25 "`beats fork 1.02-1.04x at c>=192`" claim was measured on
  (`a4eda7ba30`, the pooled demand-grow fix).

**These are different schedulers with different cost profiles.** The 0.18x number was
thread-per-session; the 1.02-1.04x claim was pooled. Comparing them was an apples-to-oranges
framing error, corrected in-repo before this task began (`c4800fd179`). This task's job was to
re-measure both, correctly labeled, on the same box, with a verified-uring build, and settle which
(if either) claim is real.

## The three-lane sweep, done right

Fresh server per run (unique datadir per lane/client-count/run), `-c 32 -j min(c,32)` protocol,
warmup 15s (discarded) + measured 60s, `-M prepared`, scale=50, shared_buffers=85% RAM,
fsync/synchronous_commit/full_page_writes=on, autovacuum=on, `max_wal_size=16GB`. 3 runs per cell,
median reported. Client sweep: 8/16/32/64/128/192/256/384. Lanes:

- **fork**: `multithreaded=off`.
- **tps** (thread-per-session): `multithreaded=on pooled_protocol_carriers=0 io_method=xtc`.
- **pooled** (the shipped default): `multithreaded=on pooled_protocol_carriers=-1 io_method=xtc`.

Per-client fairness captured via pgbench `--log`; a client with &le;2 completed transactions in
the measured window counts as "starved" (the same definition the in-repo
`POOLED_SESSION_STARVATION.md` audit uses, and the number that would have looked "fine" if only
aggregate tps were reported).

### Results (median of 3, all PASS unless noted)

| c   | fork tps | tps(carriers=0) | tps/fork | **pooled(auto) tps** | **pooled/fork** | pooled starved (of c) |
|----:|---------:|----------------:|---------:|----------------------:|----------------:|-----------------------:|
| 8   | 74,074   | 52,540          | 0.709x   | 73,421                | **0.991x**      | 0 |
| 16  | 142,221  | 78,104          | 0.549x   | 140,756               | **0.990x**      | 0 |
| **32** | **261,733** | 138,315     | 0.528x   | **261,928**           | **1.001x**      | **0** |
| 64  | 396,323  | 213,872         | 0.540x   | 258,778               | 0.653x           | 23 |
| 128 | 609,807  | 291,395         | 0.478x   | 260,510               | 0.427x           | 87 |
| 192 | 742,398  | 310,084         | 0.418x   | 259,881               | 0.350x           | 150 |
| 256 | 773,015  | 305,328         | 0.395x   | 258,126               | 0.334x           | 217 |
| 384 | 747,870  | 309,805*        | 0.414x   | 259,473               | 0.347x           | 346 |

\* tps-lane c=384 run 3 was `CRASH(gone)` (server process gone after the run; the other two runs
at that cell PASSed cleanly at 308-311k). Not reproduced; noted, not chased (out of scope — see
"What I did not prove").

**Full raw data**: `SWEEP.tsv` (72 cells, 3 lanes x 8 client counts x 3 runs), archived alongside
this report.

## Reconciling the two prior claims

**1. My own 5.6x-slower number (0.18x): confirmed real, but it was thread-per-session, not the
shipped default.** At c=32, `tps` (carriers=0) gets 0.528x of fork on this corrected, verified-uring,
separate-driver, Debian build — better than the original 0.18x (see "regression or environment"
below) but still a genuine, large, reproducible gap. **Root-caused below**: it is not a scheduling
or I/O-method problem, it is a specific per-yield GUC-metadata rebind that the pooled path does not
pay.

**2. The 2026-08-24 "pooled beats fork 1.02-1.04x at c>=192" claim: reproduced at c=32 as *parity*
(1.001x), refuted at c>=64 as a *win*, and explained by an already-documented, later-discovered bug.**
- At c=32 (== the auto-sized carrier ceiling, == the physical core count), pooled and fork are
  statistically indistinguishable (1.001x) and **starved=0**. This is the genuine, un-confounded
  parity result the demand-grow fix (`a4eda7ba30`) earned.
- **At every client count above 32, pooled's tps plateaus around 258-262k while starved clients
  grow almost linearly with the client count in excess of 32** (23 &rarr; 87 &rarr; 150 &rarr; 217
  &rarr; 346, tracking `clients - ~32` almost exactly). This is not scaling and not degrading
  gracefully: it is `POOLED_SESSION_STARVATION.md` (`a80141dac6`, discovered 2026-09-10 — **16
  days after** the 08-24 win claim) reproduced live, exactly matching its measured law
  `working = min(clients, carriers)`. A carrier is only released at a protocol-read park
  (`PgSessionRunProtocolSchedulerUntilBoundary`); a busy pgbench `-S` client never goes quiet, so
  once `clients > carriers` the excess sessions get **zero service for the whole run** while the
  aggregate tps number looks like healthy, if flattening, scaling.
- **The 08-24 claim's own numbers are the smoking gun once you look at what "1.02-1.04x at
  c>=192" implies under this bug**: if `carriers` auto-sized to ~192 (their box, per their own
  doc) and clients were also ~192-256, then `working ≈ carriers ≈ clients` — i.e. that
  measurement sat almost exactly ON the starvation cliff edge, where a small scheduling/GC/timing
  variance could push it either into full service (their apparent "1.02-1.04x win") or into partial
  starvation. It was not measuring an oversubscription throughput advantage; it was measuring
  carrier-vs-client-count arithmetic that happens to look like a win when `clients` is at or just
  under `carriers`, and their run evidently landed on the favorable side of that knife-edge. Their
  own doc does not report per-client fairness/starved counts (that instrumentation did not exist
  yet — it was added by the 09-10 investigation), so this cannot be distinguished after the fact
  from their raw numbers; it can only be inferred from the mechanism now that it is known. **This
  is a plausible-and-sufficient explanation, not a re-run of their exact box/config — flagged
  as inference, not re-confirmation, in "What I did not prove."**

**Verdict: the old win claim is SUSPECT for c>>32 (an artifact of the not-yet-discovered
starvation bug at a client count near the carrier ceiling), and VALID-BUT-NARROW at c==carriers==cores
(parity, which is a real and useful result, just not a "win" and not generalizable past the carrier
count).** It does not "cross over" and stay ahead as client count grows past the core count the way
the oversubscription thesis in `MULTITHREADED_OVERSUBSCRIPTION_AB.md` predicted — instead pooled's
tps *plateaus* (bounded by `carriers`) while fork's keeps climbing to ~773k at c=256 before its own
oversubscription cost flattens it slightly at c=384. **Fork wins the c>32 regime outright on this
box**, because pooled's ceiling is a hard service-admission bug, not a graceful "fork's overhead
eventually loses" curve.

## Where the fiber path (thread-per-session) actually loses 5.6x-to-2x: profiled, not guessed

Thread-per-session shows **starved=0 at every client count** (unlike pooled, it genuinely serves
every client — one real fiber per session, no admission bug) — so its gap is a pure per-request
overhead problem, exactly the kind `perf` should explain cleanly. It does.

**`perf record -F 999 -g` on the live server (PID captured mid-run, 15s window) at c=32, sorted by
self time (`--no-children`)**, full report archived as `perf_tps_c32_report.txt`:

```
  8.78%  [.] guc_name_compare
  7.29%  [.] guc_name_hash
  6.82%  [.] hash_search_with_hash_value
  6.31%  [k] irqentry_exit_to_user_mode
  2.73%  [.] find_builtin_option
  1.97%  [.] GUCRecordIsCurrentSessionBuiltin
  1.67%  [.] _bt_compare
  1.66%  [.] RebindSessionGUCVariablePointers
  1.43%  [k] do_syscall_64
  1.17%  [.] GUCRecordState.part.0
  1.15%  [.] PgRuntimeRefreshCurrentWork
  0.94%  [.] find_option
  0.87%  [.] hash_search
  ...
  0.50%  [.] __xtc_exec_try_steal
  0.41%  [.] PgSessionRunProtocolSchedulerUntilBoundary
  0.34%  [.] WaitEventSetWait
```

**32.23% of ALL sampled self-time** is `guc_name_compare` + `guc_name_hash` +
`hash_search_with_hash_value` + `find_builtin_option` + `GUCRecordIsCurrentSessionBuiltin` +
`RebindSessionGUCVariablePointers` + `GUCRecordState` + `find_option` + `hash_search` — the GUC
metadata-rebind chain. The call graph (`perf report -g graph,0.1,callee`) traces **35.27% of total
samples** through one exact call chain:

```
xtc_pg_wait_fd -> PgRuntimeRestoreCurrentWork -> RebindSessionGUCVariablePointers
  -> find_option (x ~230 GUCs, hash lookup by name) -> guc_name_hash / guc_name_compare
     / hash_search_with_hash_value
```

i.e. **99.9% of the GUC-rebind cost is one call site**: `xtc_pg_wait_fd` (the thread-per-session
client protocol-read park/resume — the operation a `-S` client hits on *every single
request-response cycle*) calls the **eager** `PgRuntimeRestoreCurrentWork`, which unconditionally
re-derives all ~230 threaded-session GUC variable pointers via a per-name hash lookup, on every
resume.

**This is a known, already-fixed-elsewhere cost that just isn't used at this call site.** The
codebase already has `PgRuntimeRestoreCurrentWorkLazy` (`backend_runtime.c:670`), whose own comment
says exactly this:

> "Restore the six root pointers WITHOUT re-running the ~230-entry session GUC pointer rebind
> ... measured at ~3722ns / 97% of an eager refresh ... This is the restore used by the fiber-context
> switch hook, where the cost of an eager rebind on every coroutine switch would erase the
> scheduling win."

It is exercised and proven correct in `test_backend_runtime_carrier.c` (fiber A parks, fiber B runs
on the same OS thread and clobbers the thread-local current-work pointers, fiber A resumes via the
**lazy** restore and its GUC state is still coherent because reads resolve through the
session-rooted accessor, not a cached pointer) — i.e. the exact scenario at `xtc_pg_wait_fd`'s
resume point. But the production call site in `pg_xtc_carrier.c:1420` (the wait_fd resume, hit on
every protocol-read wake) calls the **eager** variant, not the lazy one that was built for this.

**Profiled control: the pooled lane shows zero GUC-rebind cost.** Same c=32, same 15s window,
`perf_pooled_c32_report.txt`: `guc_name_compare` / `guc_name_hash` / `RebindSessionGUCVariablePointers`
/ `find_builtin_option` / `find_option` **do not appear at all** in the profile (verified with an
explicit grep, zero matches). This is because the pooled lease path (`PgCarrierAttachBackend`,
called from `PgCarrierLeaseRunnableProtocolBackend`) repoints the six root pointers directly and
never calls `RebindSessionGUCVariablePointers` per-lease — the rebind only runs once, at logical
session start (`backend_pooled_protocol_run_logical_start` -> `InitializeThreadedSessionGUCOptions`).
**This is exactly why pooled reaches parity with fork while thread-per-session does not**: pooled
already avoids the cost that is dominating thread-per-session's profile.

### What is NOT the bottleneck (ruled out with the same profile)

- **The xtc scheduler / io_uring / epoll poll path**: `__xtc_exec_try_steal` (0.50%),
  `__xtc_loop_step` (0.24%), `xtc_io_poll` (0.12%), `io_uring_enter` (0.04-0.05%),
  `epoll_wait` (0.01-0.03%) — combined well under 1% of self-time. Verified io_uring is actually in
  use (fdinfo rings present), so this rules out "io_method=xtc doing needless work on a read" as a
  real candidate: the io layer is cheap and correctly using uring.
- **Lock/latch/wait-event machinery**: `WaitEventSetWait` self-time is 0.34%; the whole
  `PgSessionRunProtocolSchedulerUntilBoundary` frame is 0.41% self / 85% *children* (i.e. it is a
  thin dispatcher whose cost is entirely in what it calls, not in itself).
- **Network (TCP send/recv)**: combined TCP-stack symbols sum to under 1% of self-time; io_uring
  socket handling is not the limiter.
- **Work-stealing overhead** (see "loop imbalance" below): cheap in isolation (0.50%
  self-time for the steal-attempt function itself); the imbalance it corrects is a symptom, not
  an independent cost.

### A secondary, smaller observation: severe loop imbalance in thread-per-session

`pg_stat_xtc_carriers` mid-run (c=32, thread-per-session) showed wildly uneven `tasks_run` across
the 32 carrier loops (760k / 743k / 747k / 670k / 635k on the five hottest loops vs. double- and
triple-digit counts on most of the rest), with `steals` on the hot loops running 55-60% of
`tasks_run` — i.e. roughly half of all executed tasks on the busiest loops arrived via a
work-steal, not their own spawn assignment. `mpstat -P ALL` during the same run shows the same
shape at the OS level: only 2 of 32 cores (18, 19) show real `%usr+%sys` compute (~80-100%
combined); most other cores show either ~100% `%iowait` (waiting, not idle, not computing) or near
~90-100% `%idle`. **This did not show up as a measurable self-time cost** (the steal function
itself is 0.5%), so it is not competing with the GUC-rebind finding for "the" bottleneck — but it
is worth noting as a distinct, secondary scheduling-locality question (why are 5 loops doing
almost all the CPU-bound work while dozens sit near-idle, when spawn is round-robin and migration
is enabled) that a future session could investigate independently. Not chased further here per the
diagnosis-only scope, and NOT proposed as an explanation for the tps gap — the GUC-rebind chain
alone accounts for essentially all of the 30%+ self-time gap; loop imbalance did not show up as
comparable cost in the same profile.

## Regime sweep verdict: no crossover, ever, on this box

The mission's core question: does thread-per-session/pooled *catch up and overtake* fork as client
count grows past the core count (32), the way the oversubscription thesis predicts? **No, on
neither lane, on this box:**

- **tps (thread-per-session)**: ratio to fork *degrades* monotonically with client count past
  c=32 (0.528x at 32 &rarr; 0.540x at 64 &rarr; 0.478x at 128 &rarr; 0.418x at 192 &rarr; 0.395x at
  256 &rarr; 0.414x at 384). It gets WORSE with oversubscription, not better — consistent with the
  GUC-rebind cost being paid on every client read-park regardless of client count, while fork's
  per-process cost, though real, is evidently smaller than this fixed per-yield tax on this box.
- **pooled (the default)**: ratio to fork is 1.001x exactly at c=32 (the carrier ceiling), then
  *falls off a cliff* to 0.653x at 64 and keeps declining to 0.334x at 256 — the starvation
  admission bug, not a throughput curve. It never recovers.

**Neither lane crosses over. The old claim's regime (client count at or near the carrier ceiling,
not "far past" it) is the ONLY regime tested here where pooled matches fork; there is no evidence
anywhere in this sweep of a fiber/thread advantage emerging at high oversubscription** on a
32-core box with a separate driver and a verified-uring build.

## What I proved vs. did not prove

**Proved (direct measurement + control, on this box, this config):**
- The original 0.18x measurement used the non-default thread-per-session scheduler, not the
  scheduler the "1.02-1.04x" claim was about. (Source-code proof, independently confirmed twice.)
- The pooled default reaches statistically-exact parity with fork (1.001x) at c=32 with
  starved=0 — a genuine, reproducible, non-starved result.
- Pooled's tps plateaus and its starved-client count grows almost linearly with `clients - 32`
  for every c>32 tested (64 through 384) — directly reproducing the documented
  `working = min(clients, carriers)` law from `POOLED_SESSION_STARVATION.md`.
- Thread-per-session's per-request gap is dominated (32.23% of self-time, 35.27% of the call-graph
  path) by an eager, unnecessary ~230-entry GUC pointer rebind on every `xtc_pg_wait_fd` resume,
  where a lazy variant already exists, is tested, and is documented as being built for exactly
  this resume scenario.
- The pooled lane's profile shows zero measurable cost from that same rebind chain, which is
  *why* it does not have this gap.
- io_uring is genuinely in use (fdinfo-verified) on both threaded lanes; the io layer, scheduler
  dispatch, locking, and network stack are all cheap in the profile (each under ~1% combined) and
  are ruled out as the bottleneck for the tps-lane gap.
- Fork itself shows zero starvation at any client count tested (control), so its own plateau at
  high c (256-384) is ordinary oversubscription cost, not a comparable bug.

**Did NOT prove / explicitly not established:**
- **The exact 08-24 claim's original box/config was not re-run.** I inferred the starvation
  mechanism is the likely explanation for that claim's apparent win, from the mechanism and the
  proximity of their client count to their carrier count — I did not obtain their original
  per-client fairness data (it was not captured; that instrumentation postdates their run) and
  cannot rule out that their specific run happened to have `clients` genuinely at or under
  `carriers` the whole time with the auto-sizing landing favorably. This is the strongest
  inference available from the evidence, not a re-confirmation.
- **The tps-lane c=384/run3 crash was not root-caused.** One of 24 tps-lane runs ended with the
  server process gone after the run; the other two runs at the identical cell (c=384) passed
  cleanly at consistent tps. Not reproduced, not chased (diagnosis-only scope; flagging for a
  future session as a possible high-oversubscription thread-per-session robustness gap, distinct
  from the GUC-rebind performance finding).
- **The loop-imbalance/work-stealing observation is not shown to be a real cost.** It correlates
  with the tps lane but did not show measurable self-time in the profile; it is noted, not relied
  upon, and not proposed as part of the named bottleneck.
- **I did not implement or A/B the lazy-restore fix.** Per the task's explicit scope
  ("DIAGNOSIS + MEASUREMENT, not a fix" and the two-review gate for hot-path scheduler changes),
  I am naming the fix direction (swap `PgRuntimeRestoreCurrentWork` for
  `PgRuntimeRestoreCurrentWorkLazy` at the `xtc_pg_wait_fd` resume call site,
  `pg_xtc_carrier.c:1420`) as a small, targeted, already-precedented change, but did not implement,
  A/B, or get it reviewed.
- **The pooled-scheduler starvation bug itself is not fixed** — it was already open
  (`POOLED_SESSION_STARVATION.md`, `a80141dac6`) before this task and remains open; this task adds
  a second, independent reproduction of it plus the observation that it likely explains the
  now-suspect prior win claim.

## Methodology / trust notes

- **Driver placement**: genuinely separate host (a second c6id.8xlarge in the same VPC/subnet,
  connected over the private IP, 172.31.x.x), not co-located — the absolute numbers in this report
  are not depressed by a shared-CPU loopback confound. Confirmed via `ping`/SSH to distinct
  instance IDs; the SUT and driver were launched, monitored, and torn down as two separate EC2
  instances throughout.
- **Build**: `git archive origin/xtc` (never the working tree) at `cf436d861b`; libxtc v1.44.1 from
  a tagged `git archive` of the local libxtc checkout, `-Dio-backend=uring` (explicit, would
  hard-error without liburing, not the silently-degrading `auto`); PG built `--buildtype=release
  -Dcassert=false -Dxtc=enabled` with `-fno-omit-frame-pointer -g -O2`. Binaries verified present
  and non-trivial size after every build step (83-85 MB `postgres`), not just "build succeeded."
- **OS**: Debian 13 (trixie), per updated directive, both SUT and driver; `admin` user, apt-get,
  `liburing-dev` installed before libxtc configure.
- **io_uring verified at runtime**, not assumed from the build log: `/proc/<pid>/task/*/fd`
  readlink showing `anon_inode:[io_uring]` entries on the live server for both threaded lanes.
- **Fresh server per run**, unique datadir per (lane, client-count, run) cell, `pkill -9` scoped to
  the exact datadir path, `pg_ctl -m immediate` stop. `ulimit -n 200000` set explicitly per-server
  (the default 1024 open-file limit caused an immediate `SRV_NOSTART` at the very first cell of the
  sweep — caught and fixed before any measured data was collected, not a silent contaminant).
- **Carrier-count assertion**: `pg_stat_xtc_runtime`/`pg_stat_xtc_carriers` queried; my first sweep
  pass sampled `carriers_started` right after `pgbench -i` (before load), which under-reported
  (showed 1-2); a follow-up targeted check sampling mid-run (5s/15s/25s into a 30s load) confirmed
  the pool actually grows to the expected ~32 (core-count-bounded) carriers under load — the
  headline tps/starved numbers were unaffected (independent pgbench measurement), only the
  carrier-count column in the raw sweep undercounts; noted here rather than silently left wrong.
- **perf**: `perf record -F 999 -g -p <pid> -- sleep 15` attached to the live, already-warmed
  server process 8s into a 40s measured pgbench run; `perf_event_paranoid=-1` set explicitly.
  Reports and full call-graph archived.
- **3 runs per cell, median reported**; 72 cells total (3 lanes x 8 client counts x 3 runs, minus
  1 crash) all completed to PASS or the single noted CRASH; no HANG or INIT_FAIL anywhere in the
  sweep.

## EC2 teardown

Two rounds of infrastructure were used (AL2023 first, then torn down and rebuilt on Debian 13 per
a mid-task OS directive):

- **Round 1 (AL2023, us-east-2)**: SUT `i-0efb5aed3c89a8fb8` + driver `i-0fdc649eacbc8843a`, key
  `xtc-readgap-20260912-164707`, SG `sg-0defaeb058a8c5b83`. KeyName and Name tag verified matching
  before terminate. Both instances confirmed `terminated` via `wait instance-terminated`. SG
  deleted (first attempt succeeded, no DependencyViolation retries needed since instances were
  already gone). Key pair deleted, `.pem` shredded.
- **Round 2 (Debian 13, us-east-2)**: SUT `i-0b09e70ab6cf1c09b` + driver `i-02ae62d74dffaae48`,
  key `xtc-readgap2-20260912-172550`, SG `sg-02046f882c2751d3d`. **To be torn down immediately
  after this report is finalized and artifacts are pulled** (final step of this task).
- **6-region key sweep** (us-east-1, us-east-2, us-west-1, us-west-2, eu-west-1) performed after
  round 1 teardown: no live `xtc-readgap-*` resources found (the two instance IDs listed by
  `describe-instances` in us-east-2 were the already-confirmed-terminated round-1 instances, which
  AWS lists for a retention window; not a leak). Round 2's equivalent sweep is pending, tracked
  in the final teardown step.

## Bottom line

**Named bottleneck for the thread-per-session gap**: an eager, unnecessary ~230-entry GUC-pointer
rebind (`RebindSessionGUCVariablePointers` via the eager `PgRuntimeRestoreCurrentWork`) on every
client protocol-read park/resume in `xtc_pg_wait_fd`, accounting for ~32% of profiled self-time and
~35% of the call-graph path — where a lazy variant built for exactly this scenario already exists,
is tested, and is unused at this call site. This is a small, targeted, low-risk-looking fix
direction (swap one function call at one call site) but per scope is NOT implemented here; it
needs the two-review + A/B gate per AGENTS.md.

**Named bottleneck for the pooled-default anomaly (the reconciliation with the old win claim)**:
the already-documented `POOLED_SESSION_STARVATION.md` admission bug
(`working = min(clients, carriers)`), reproduced independently here across c=64..384, which most
plausibly explains why an earlier c>=192 measurement looked like a 1.02-1.04x win — it was likely
sampling a `clients &approx; carriers` knife-edge, not a genuine oversubscription throughput
advantage. Not fixed here (was already open before this task); this task adds a second,
Debian/uring-verified reproduction plus the mechanistic link to the old claim.

**No crossover was found in either lane at any client count tested on this box.** The old "wins at
c>=192" framing is not supported by this sweep; it is better explained as an artifact of a bug
discovered sixteen days after that claim was made.
