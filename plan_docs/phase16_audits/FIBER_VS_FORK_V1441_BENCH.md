> **FRAMING CORRECTION 2026-09-12 (later).** This benchmark set `pooled_protocol_carriers=0`,
> which is the THREAD-PER-SESSION path, NOT the pooled scheduler.  Verified in source:
> `PgRuntimePooledProtocolRequested() = multithreaded && pooled_protocol_carriers > 0`
> (backend_runtime.c:1486), so carriers=0 is false -> thread-per-session; carriers=-1 (auto,
> the DEFAULT) resolves to core-count > 0 -> the pooled scheduler (postmaster.c:934).
> The prior 'fiber beats fork 1.02-1.04x at c>=192' claim (a4eda7ba30, an ancestor of HEAD)
> and the SHIPPED default are the POOLED scheduler.  So this run measured the non-default
> path and compared its 0.18x to a claim about the default path -- an apples-to-oranges
> framing error of mine.  AGENTS.md notes thread-per-session 'is not the final normal-mode
> target'.  A three-lane sweep (fork / thread-per-session carriers=0 / pooled default auto)
> across c=8..384 is in flight to establish whether the POOLED default beats fork and where,
> and whether thread-per-session is simply categorically slower.  The write WEDGE is likewise
> being re-checked on both configs -- it may be thread-per-session-specific.  Treat the
> numbers below as a thread-per-session measurement, not a verdict on the shipped path.

# Fiber-vs-fork benchmark on libxtc v1.44.1: the honest result — fiber is NOT yet competitive

Date: 2026-09-12. SUT: EC2 c6id.8xlarge (32 vCPU, 61 GB), us-east-1, XFS on local NVMe.
PG: committed clean HEAD `ba0c145367` (release build, cassert OFF, from a `git archive` — not the
working tree). libxtc **v1.44.1** (`e9a2e14`, the cross-loop aio lost-wake fix).
Config (both lanes identical): shared_buffers = 85 % RAM, fsync=on, synchronous_commit=on,
full_page_writes=on, autovacuum=on, max_wal_size=16GB. pgbench scale=50, `-c 32 -j 8 -M prepared`,
60 s measured + 15 s warmup, **fresh server per run, 3 runs per cell**. Driver co-located (single
box) — read the caveat.

--------------------------------------------------------------------------------
## Results

| workload | lane | run 1 | run 2 | run 3 | median | vs fork |
|---|---|---|---|---|---|---|
| **write** (TPC-B) | fork | 49,383 | 48,536 | 51,169 | **49,383** | 1.00x |
| **write** (TPC-B) | xtc (fiber) | **WEDGED** | **WEDGED** | 3,052 | — | **not competitive** |
| **read** (-S) | fork | 471,682 | 474,251 | 473,830 | **473,830** | 1.00x |
| **read** (-S) | xtc (fiber) | 56,035 | 85,862 | 84,757 | **84,757** | **0.18x (5.6x slower)** |

`starved=0` on every completing run (per-client fairness is fine — the pooled-starvation bug does
not affect the fiber path, as expected).

## What this says, plainly

**The fiber path is not ready and does not beat fork on either workload.** This is the honest
answer to "is all this work paying off in performance": **not yet, and not close, at c=32 on this
config.**

- **Write-heavy: 2 of 3 runs WEDGED.** Server came up, ran ~30 s, then stopped making progress;
  my 200 s timeout killed pgbench and the harness teardown SIGKILLed the server (the
  "FATAL: terminating connection due to administrator command" + "SIGKILL to recalcitrant children"
  is *my teardown*, not a product crash). The one completing run did 3,052 tps — **6 %** of fork.
  So the lost-wake fix cleared the *init*-time fdatasync strand (init completed cleanly all 3
  times, the pmchild crash is gone), but a **different stall still wedges the write path under
  concurrency**. v1.44.1 fixed one hang; it did not make write-heavy healthy.
- **Read-only: completes reliably but is 5.6x SLOWER than fork** (0.18x). Clean 3/3, no starvation.

## This CONTRADICTS earlier session claims, and I am flagging that rather than smoothing it

Prior notes in this repo claim fiber read `-S` "beats fork 1.02-1.04x at c>=192" and CPU-bound
"1.53x". **This run shows read at 0.18x.** Those cannot both be right on the same workload. Possible
reasons, none yet established:
- Those earlier wins may have been at much higher client counts (c>=192, 2x oversubscription) where
  fork's per-process overhead dominates, while this is c=32 where fork is at its best. Different
  regime.
- Or they were on a different config / a co-located-driver artifact / a different libxtc.
- Or this build has a regression the earlier ones did not.
I did **not** re-run the c>=192 oversubscription lane that produced the original claim, so I cannot
say which. What I can say: **at the standard c=32 apples-to-apples config, fiber loses badly on
both workloads today.**

## Caveats on this measurement (so it is not over-read)
- **Co-located driver.** pgbench ran on the SUT (single box), which I have repeatedly flagged as a
  confound — it can depress both lanes and does not depress them equally. Fork's absolute numbers
  (49k write / 474k read) are plausible for this box, but the *ratio* is not airtight. A separate
  loadgen is needed for a headline. That said, a 5.6x read gap and a wedging write path are far too
  large to be explained by driver co-location alone.
- c=32 only. No oversubscription sweep, no HammerDB, no p99/RSS predictability capture.
- The write wedge needs the xtc_tail instruments (PARK_TASK/REAP/SUBMIT/POLL_FULL, xtc-cqes) to
  characterize — it is a *different* stall from the fdatasync lost-wake v1.44.1 fixed, since init
  (which also fsyncs) now survives.

## Bottom line
v1.44.1 removed the specific cross-loop aio lost-wake that blocked the fiber path from *starting* a
write workload. It did **not** deliver a competitive fiber path: write-heavy still wedges under
concurrency, and read is 5.6x off fork at c=32. The north-star "beat fork by a significant margin"
is **not met** by this measurement, and the earlier read/CPU win claims need re-establishing at the
client counts and config that produced them before they are trusted. The next real work is
characterizing the c=32 write wedge (with the now-excellent xtc_tail tooling) and the read-path
5.6x gap — both are performance/scheduler problems, not the libxtc lost-wake, which is fixed.

Artifacts: full BENCH.txt, per-lane server logs, pgbench per-txn logs retained.
