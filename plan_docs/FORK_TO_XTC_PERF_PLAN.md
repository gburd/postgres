# Plan: demonstrate PG/XTC (pooled threaded) matching-then-beating stock fork PG

Status: DRAFT for review — do not start work until approved.
Author: coordinator, 2026-08-28.
Goal (from the user): migrate PostgreSQL from the fork model to the XTC pooled-thread
model and DEMONSTRATE the performance AND predictability wins, apples-to-apples, on a
realistic production-shaped configuration.

--------------------------------------------------------------------------------
## 0. The north star, restated precisely

On an IDENTICAL host with IDENTICAL resources, PG/XTC must match then beat stock fork PG
on TPS/NOPM with comparable or better p95/p99 latency, on the workloads that matter:
- Read-mostly OLTP (pgbench -S / TPROC-C read-heavy): ALREADY beats fork 1.02-1.04x at
  high concurrency. Keep it.
- CPU-bound compute: ALREADY 1.53x fork. Keep it.
- WRITE-heavy durable OLTP (HammerDB TPROC-C, the realistic OLTP case): the open gap.
- Analytics (TPROC-H): unmeasured on threaded; likely fine (long queries, low
  scheduler churn) but must be shown.

Predictability wins to demonstrate alongside throughput: lower + tighter p99, lower and
BOUNDED RSS (one process vs N forks), lower context-switch rate, graceful behavior at
oversubscription (2x cores).

--------------------------------------------------------------------------------
## 1. The apples-to-apples benchmark methodology (fixed for ALL runs)

Both lanes (stock fork = multithreaded=off; PG/XTC = multithreaded=on,
pooled_protocol_carriers=-1 auto=cores) get the SAME config, SAME data, SAME driver:

- Host: bare-metal-ish NVMe box. Start c6id/c7id metal-ish (32-192 vCPU); the flagship
  demo run on a large metal box (matches the historical 192-core -S win environment).
- Storage: PGDATA on LOCAL NVMe, formatted XFS (not tmpfs, not EBS). WAL on the same
  NVMe (or a second NVMe — decide once; keep identical across lanes).
- shared_buffers = 85% of host RAM (the user's spec; e.g. 52GB on a 61GB box, 160GB on
  192GB). huge_pages=on sized to cover it. NOTE: 85% is aggressive vs the usual 25%
  guidance — it maximizes cache hits and is the same for both lanes, so it is fair; but
  leave headroom for work_mem*max_connections + carrier stacks (see risk R3).
- autovacuum = ON (default). fsync = ON. synchronous_commit = ON. full_page_writes = ON.
  (Realistic durability — this is where the current gap lives; do not hide it with
  fsync=off.)
- io_method = sync (threaded cannot fork IO workers; io=xtc is a later A/B).
- max_connections sized to the VU sweep + headroom.
- Driver: HammerDB on ONE OR MORE SEPARATE EC2 hosts over the private network (client
  CPU never steals SUT CPU). For very high VU counts, multiple loadgens.
- VU sweep spanning 1x to 2x oversubscription of cores (e.g. on 32 cores: 16/32/64/128;
  on 192 cores: 96/192/384) to exercise the oversubscription thesis.
- Metrics per lane per point: NOPM + TPM, p50/p95/p99 latency, SUT RSS (PSS), CPU%,
  context-switch rate (vmstat cs), and (threaded) carrier count + idle%.
- Duration: >=2min rampup + >=5min steady measured window; 3 repeats, report median.

Measurement integrity (learned this session):
- HammerDB's monitor VU HANGS on the pooled server today (the cross-loop wake miss), so
  its NOPM is unreliable for threaded. Until the wake fix lands, measure NOPM
  independently: sample `sum(d_next_o_id) from district` deltas over the steady window
  from a PRE-WARMED persistent connection (the exact NOPM quantity, monitor-independent).
  After the wake fix, cross-check against HammerDB's own monitor NOPM.
- Always confirm the threaded lane's effective carrier count (show
  pooled_protocol_carriers; pg_stat_xtc_runtime carriers_started) and that it is NOT
  thread-per-session (0) or under-provisioned.

--------------------------------------------------------------------------------
## 2. Blockers to clear (ordered; each is a gate)

### B1 [CRITICAL, external] libxtc cross-loop idle-loop wake miss
The pooled scheduler periodically stalls under write load: carriers sleep in
libxtc's xtc_io_poll while a runnable PG session waits, hanging sessions and
collapsing throughput to ~0. Report filed: /tmp/libxtc-cross-loop-wake-miss-report-
2026-08-28.md.
- Track A (PG-side, we can try NOW, may be the whole fix): when we mark a parked
  session runnable / signal_ready_work cross-thread, ALSO call xtc_io_wakeup() on the
  target carrier loop(s) (xtc_exec_loop(g_xtc_exec, i)->io). This is option B in the
  report; if the contract is "producer must nudge", this closes it without waiting for
  libxtc. A/B-gate it; it must be lost-wake-free (nudge AFTER the enqueue, and the
  carrier must re-check after wake).
- Track B (libxtc): if Track A doesn't fully close it, libxtc extends the
  re-arm-before-drain guarantee to consumer fds / provides a guaranteed lost-wake-free
  loop kick. We already landed a PG-side partial fix (d693f4d4af).
- GATE: the pg_stat_database-under-write-load repro no longer hangs; HammerDB monitor VU
  succeeds on the threaded lane.

### B2 [HIGH] Write-path scheduler feeding / carrier utilization
Even without stalls, write-heavy pooled left ~26-28% of cores idle under load
(2026-08-27 profile): the scheduler doesn't keep enough sessions concurrently runnable
to fill all carriers the way fork's N processes overlap client RTT + I/O wait.
- Investigate after B1 (B1 stalls contaminate any utilization measurement).
- Candidates: (a) protocol-read pipelining/prefetch so a carrier has >1 runnable
  session; (b) spread leased sessions across loops (today loop 0 gets the bulk;
  steals=0 because pooled carriers are pinned migratable=0) — either round-robin the
  session onto the least-busy loop or make pooled sessions migratable so idle loops
  steal; (c) the F2-layer pooled-queue lock/notify contention flagged in the CPU-bound
  case (PopRunnable/notify/condvar) — dedup onto xtc primitives.
- GATE: threaded write-heavy carrier idle% under load ~ fork's; NOPM within striking
  distance of fork, then >= fork.

### B3 [MED] Explicit-carriers>cores wedge + thread-per-session c>=64 wedge
Two known hangs (documented): pooled_protocol_carriers set explicitly above core count
wedges; thread-per-session (=0) wedges at c>=64 write load. Auto-sizing avoids B3a, but
both should be fixed or fenced so a mis-set knob can't hang. Likely subsumed by B1.

### B4 [LOW] Measurement harness robustness
The HammerDB monitor dependency + the verbose-log NOPM count both bit us. Bake the
pre-warmed-persistent-sampler NOPM method into the harness (mtpg_hammerdb_bench.sh) so
threaded NOPM is always captured monitor-independently. Add a per-lane assertion that
carrier count == expected and no stall (district counter advanced every N seconds).

--------------------------------------------------------------------------------
## 3. Work plan (phased, each phase A/B-gated + two-review for hot-path scheduler code)

P1. Repro harness hardening (B4). Bake monitor-independent NOPM + stall detection +
    carrier-count assertion into the benchmark harness. Deliverable: one command that
    runs the fixed methodology (section 1) for both lanes and both workloads and emits a
    results.tsv + latency percentiles. ~0.5-1 session.

P2. B1 Track A: PG-side xtc_io_wakeup nudge on MarkRunnable/signal_ready_work.
    Implement, two-review, A/B on the write-load repro + the pg_stat_database hang test.
    If it closes B1 -> huge unblock. If not -> escalate Track B to libxtc with the
    narrowed evidence. ~1 session (+ libxtc round-trip if Track B).

P3. Re-measure the FULL matrix (section 1) once B1 is closed: pgbench -S (confirm the
    1.02-1.04x still holds), CPU-bound (confirm 1.53x), HammerDB TPROC-C (the target),
    TPROC-H (new). Establish the honest current standing on the realistic config
    (85% RAM, autovac/fsync on, NVMe/XFS, separate loadgen). ~1 session.

P4. B2 write-path feeding: profile (flamegraph + carrier idle% + wait events) the
    de-stalled write lane; name the top limiter; fix the RIGHT thing (loop-spreading OR
    read-pipelining OR queue-lock dedup) per the profile, A/B neutral-or-better on
    read-S/CPU. Iterate until write-heavy TPROC-C >= fork. ~2-4 sessions (the real work).

P5. Predictability story: capture p99 latency, RSS, ctx-switch, and the 2x-
    oversubscription lane for both models; produce the "threaded is faster AND more
    predictable" evidence set. ~1 session.

P6. TPROC-H (analytics) + a mixed workload; confirm no regression; document. ~1 session.

--------------------------------------------------------------------------------
## 4. Risks / open questions to settle at review

R1. 85% shared_buffers is much higher than the usual 25% rule. It is fair (identical
    both lanes) and maximizes the in-cache OLTP case, but with fsync=on the bottleneck
    shifts to WAL/checkpoint I/O, not buffer cache — so 85% may not change the write
    result much and could starve work_mem/carrier stacks. Confirm we want 85% for the
    headline, and/or also run the standard 25% for a "recommended config" comparison.
R2. SETTLED 2026-09-07 (measured against libxtc v1.42.0 sources, not estimated).
    Fiber stack = XTC_PG_FIBER_STACK = 8 MiB (pg_xtc_carrier.c:122), matching the
    tree's pthread stack; xtc allocates it as anonymous mmap + one PROT_NONE guard
    page (coro_fctx.c:337-367), so it is LAZILY FAULTED -- the 8 MiB is virtual
    reserve, not RSS.

    Per-session budget is therefore a non-issue at the sizes we care about:
      host  61GB, 85% SB -> 9.1GB non-buffer; 256 sessions = 2.0GB VIRTUAL (21.9%),
                            but only ~0.25GB RSS if each stack faults ~1MB (2.7%).
      host 192GB, 85% SB -> 28.8GB non-buffer; 384 sessions = 3.0GB virtual (10.4%),
                            ~0.38GB RSS at 1MB faulted (1.3%).

    THE REAL RISK IS NOT PER-SESSION, IT IS THE STACK POOL. coro_common.h:44
    XTC_STACK_POOL_MAX = 64 and the pool is XTC_THREAD_LOCAL, i.e. up to 64 retained
    8 MiB stacks PER CARRIER THREAD. Recycled stacks are NOT madvise'd, so whatever
    each stack faulted at its high-water mark stays resident:
       32 carriers x 64 x 8MiB  = 16GB   worst case retained
       96 carriers x 64 x 8MiB  = 48GB
      192 carriers x 64 x 8MiB  = 96GB
    16GB EXCEEDS the entire 9.1GB non-buffer budget of a 61GB host at 85% SB. The
    worst case needs deep-recursion sessions (parser/planner) to fault most of a
    stack, so the expected case is far smaller -- but it is a real ceiling and it
    scales with carrier count, which is exactly what Option A raises.

    MITIGATION AVAILABLE, CURRENTLY UNUSED: libxtc lever S1,
    xtc_stack_reclaim_enable(keep_bytes) (coro_common.h:88), madvise(MADV_DONTNEED)s
    the unused tail of a parking fiber's stack; OFF by default and PG never calls it.
    ACTION for the big run: instrument RSS vs pooled-stack count, and if retained
    stack RSS is material, either call xtc_stack_reclaim_enable at carrier start
    (A/B it -- refaulting on resume costs page faults on a hot path) or ask libxtc
    to make XTC_STACK_POOL_MAX tunable. Do NOT pre-emptively enable S1: measure
    first, per the neutral-or-better rule.
R3. SETTLED 2026-09-07: SINGLE NVMe, both lanes identical.
    Rationale is evidence-based, not convenience: the write-heavy root-cause profile
    (.ec2/writeheavy-rootcause-profile-2026-08-27.md) found the gap is NOT WAL or I/O
    contention -- "NO WAL-insert spinlock (insertpos_lck)", no perform_spin_delay, no
    LWLock, no XLogFlush in the top 30, fsync explicitly cleared. A separate WAL device
    isolates a commit path that the profile says is not the limiter, so it would add
    cost and a config difference without buying evidence.
    NOTE (2026-09-07): that RCA ran the driver ON the SUT, so its idle-core headline is
    confounded (see the caveat banner on the doc). The NEGATIVE result relied on here is
    robust to that confound -- a co-located driver cannot remove server-side lock
    contention from a profile -- so R3's basis stands, but P3 must re-confirm under
    section-1 methodology.
    INVARIANT that would reverse this: if a P3/P4 profile shows the WAL device
    saturated (device util near 100%, or IO/WalSync/IO/WalWrite dominating the wait
    profile) in BOTH lanes, then the single device is masking the commit path and P4
    must re-run with a dedicated WAL NVMe before drawing a write-heavy conclusion.
R4. SETTLED 2026-09-07 by user directive, and the evidence says it is achievable.
    THE BAR: PG/XTC must OUTRIGHT WIN on ALL performance tests versus stock fork PG by
    a significant margin, with particular weight on STABILITY UNDER LOAD AT SCALE.
    Parity on write-heavy is therefore NOT an acceptable exit for P4.

    This is demanding but not unreasoned. The concern behind R4 was that write-heavy
    durable OLTP is fork's structural best case (independent per-backend commit) and so
    threaded might only reach parity. The write-heavy root-cause profile refutes that
    framing for our current gap: no WAL-insert spinlock contention, no LWLock/XLogFlush
    in the top 30, fsync cleared as culprit -- i.e. the gap is not fork's commit
    independence. A self-inflicted gap is fixable; a structural advantage would not be.
    So the bar stands.
    CALIBRATION (2026-09-07): the specific claim "~26-28% of cores idle => scheduler
    feeding is the limiter" comes from a run with the DRIVER CO-LOCATED on the SUT,
    where pgbench competed for the same 32 vCPUs; co-located load can manufacture an
    idle-core signature. That claim is downgraded to a hypothesis. P3 re-establishes the
    profile with a separate loadgen BEFORE P4 picks its lever, so P4 fixes the limiter
    the clean profile names rather than the one the confounded profile suggested.
    HONEST CAVEAT retained: if, after the feeding gap is closed, write-heavy lands at
    parity rather than a win, that result gets reported as-is with the profile that
    explains it. The bar drives the work; it does not license flattering the numbers.

R5. libxtc dependency: B1 Track B (if needed) is an external round-trip. P4 can proceed
    on read/CPU wins in parallel, but the write-heavy headline waits on B1.

--------------------------------------------------------------------------------
## 5. What I recommend we commit to at review

1. Adopt section-1 methodology as the fixed apples-to-apples protocol.
2. Do P1 + P2 (harness + the PG-side wake nudge) first — cheap, and P2 may unblock
   everything.
3. Settle R1 (85% vs 25%) and R4 (write-win vs aggregate-win bar) before P3/P4.
4. Keep every scheduler change under the two-review gate + neutral-or-better-on-
   read/CPU rule; process mode stays byte-for-byte.


--------------------------------------------------------------------------------
## VERIFIED RESULT 2026-09-12 (Debian 13, io_uring-verified, separate driver)

The authoritative read `-S` three-lane sweep (fiber-read-gap-diagnosis-2026-09-12.md):

| c   | fork    | pooled(default) | pooled/fork | pooled starved |
|-----|---------|-----------------|-------------|----------------|
| 16  | 142,221 | 140,756         | 0.990x      | 0 |
| 32  | 261,733 | 261,928         | **1.001x**  | 0 |
| 64  | 396,323 | 258,778         | 0.653x      | 23 |
| 128 | 609,807 | 260,510         | 0.427x      | 87 |
| 192 | 742,398 | 259,881         | 0.350x      | 150 |
| 256 | 773,015 | 258,126         | 0.334x      | 217 |
| 384 | 747,870 | 259,473         | 0.347x      | 346 |

**Findings, all evidence-based:**
- The shipped pooled default reaches **exact parity with fork at c=32** (== cores == auto carrier
  ceiling), starved=0.  Genuine, un-confounded.
- **Above c=32 pooled PLATEAUS at ~260k** (bounded by carrier count) while fork climbs to 773k.
  The plateau IS the POOLED_SESSION_STARVATION bug (working = min(clients, carriers)), reproduced
  independently, starved clients growing linearly with clients-32.
- The old "pooled beats fork 1.02-1.04x at c>=192" claim is **REFUTED as a win** and re-explained:
  it was measured before the starvation bug was discovered (16 days earlier) and reported only
  aggregate tps, which hides the starvation.  At c>=64 fork wins outright on read.
- **thread-per-session (carriers=0) never reaches fork** (0.53x -> 0.39x, degrading with
  oversubscription).  My earlier 0.18x was real for that lane but it is not the shipped path.
- perf named the thread-per-session bottleneck: 32% self-time in
  xtc_pg_wait_fd -> PgRuntimeRestoreCurrentWork(eager) -> RebindSessionGUCVariablePointers
  (~230-entry GUC rehash) on EVERY protocol-read resume.  A LAZY variant
  (PgRuntimeRestoreCurrentWorkLazy) already exists, tested, unused at this site.  The pooled lane
  shows zero cost from this chain.  Scheduler/io_uring/network/lock all <1% -- ruled out.

**Bottom line vs the north star:** at the config that ships (pooled default), fiber is at PARITY
with fork up to the core count and LOSES beyond it (to the starvation bug).  It does NOT "beat fork
by a significant margin" on read anywhere.  To even approach the north star: (1) fix pooled
starvation so carriers>cores sessions make progress (candidate WIP stashed); (2) the GUC-rebind
lazy fix would help thread-per-session but that is not the shipped path; (3) write-heavy is
separately wedged (3 bugs, keystone fix in flight).  Parity-at-core-count is real and worth
recording, but it is parity, not a win.
