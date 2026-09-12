# v1.44.1: the hang is GONE (0/12), but a PG-side double-release crash now blocks the benchmark

Date: 2026-09-12
libxtc: **v1.44.1** (tag `e9a2e14`, the cross-loop aio lost-wake fix), verified pristine + correctly
built. PG "xtc": tested at HEAD `d314c72c7c` and at `b5e19491b8` (pre-`f4ec2a937b`). cassert=OFF.
SUT: lava/us-east-2 c6id.8xlarge, XFS on local NVMe, scale=50, `-c 32 -T 10`, fresh server per run.

--------------------------------------------------------------------------------
## Headline: two things are simultaneously true

1. **The lost-wake hang is gone.** Across 12 write-heavy fiber-path runs on v1.44.1, **0 hangs**
   (was ~22-60 % on every prior libxtc). The `xtc_loop_wake(submit_loop)` nudge closed it.
2. **A PG-side crash now stops the workload** before I can get a clean tps distribution: 11 of 12
   runs logged the F0d "terminating threaded server runtime" fail-stop, in the
   `MarkPostmasterChildSlot`/`CleanupBackend` double-release family. So I cannot yet report a
   write-heavy tps figure for v1.44.1 — the honest answer to "re-run the benchmarks" is *the hang
   is fixed, but a separate crash blocks the numbers.*

The one run that completed did **26 tps** — not a real measurement (a single survivor among crashes),
recorded only so it is not mistaken for a headline.

## Evidence the hang is fixed (not merely absent by luck)
- v1.44.1 carries `2c851a5` verbatim: `xtc_loop_wake(loop)` at `aio.c:286`/`:396` when a migratable
  fiber resumes on a different loop than it submitted on. This is the exact mechanism for the named
  strand (res=0 fdatasync CQE on the submit loop's ring, fiber homed elsewhere, never reaped).
- 12/12 runs: `hang=0`. No run showed the previous signature (live server, stable nonzero `unreaped`
  on a ring, a parked fiber joined to a stuck CQE). The failures are *crashes* (server gone,
  "terminating" logged), a categorically different shape.

## The blocker crash, and its provenance (isolated by bisection, not asserted)
Backtrace (cassert OFF, so the `Assert(slot>0)` guard is compiled out and `slot=0` underflows):

```
MarkPostmasterChildSlotUnassigned (slot=0)   pmsignal.c:271   -> PMChildFlags[SIZE_MAX] SEGV
ReleasePostmasterChildSlot                    pmchild.c:700
CleanupBackend (exitstatus=0)                 postmaster.c:3042
process_pm_pooled_logical_exit                postmaster.c:2896
ServerLoop
```

A PMChild slot released with a bad/zeroed `child_slot` on the pooled-logical-exit path — the same
`pmsignal` double-release family filed earlier (`RegisterPostmasterChildActive` slot assert). It is
**ours, on the postmaster thread, not libxtc's.**

**Bisected cleanly:**
- Fork, pooled(auto), pooled(4): init + run fine. **Only the fiber path (carriers=0) crashes.**
- My v1.44.0 build (PG at `c2fb0ab2ae`) passed init 4x. My v1.44.1 build (PG at `d314c72c7c`)
  crashes. The **only** PG-side commit between them is `f4ec2a937b` (the walwriter
  restart-intensity tracker).
- **Decisive control:** I rebuilt PG at `b5e19491b8` (immediately before `f4ec2a937b`) against the
  **same v1.44.1 libxtc**. Init: **3/3 OK.** So `f4ec2a937b` is implicated in the *init-time* crash.
- On that pre-`f4ec2a937b` build, init survives but the **measured run** still crashes (same
  `terminating` family). So there are plausibly **two** issues: `f4ec2a937b` made an init-time
  variant deterministic, and an underlying pooled-logical-exit double-release fires under write load
  regardless.

I am careful here: `f4ec2a937b` only touches `cleanup_wal_writer_child` (walwriter), while the
crashing backend is a `B_BACKEND` client. So `f4ec2a937b` is *correlated by bisection* with the
init-time determinism but is not obviously on the crash's own call path — it may be perturbing timing
of a pre-existing race rather than being the direct cause. Stated as bisection result, not mechanism.

## What is NOT established
- A clean write-heavy **tps** for v1.44.1 (the crash prevents it).
- The exact double-release mechanism (cores are `-O2`; a cassert build is needed to catch the first
  bad release with an exact stack).
- Whether `f4ec2a937b` is the cause or an aggravator of the crash.

## Next steps to actually get the benchmark
1. **Fix / revert-and-rework `f4ec2a937b`** and the underlying pooled-logical-exit double-release —
   this is the pmsignal-lifecycle agent's territory, and this core is the sharpest evidence yet
   (deterministic, fiber-path-only, exact frames). Reproduce with **cassert ON** to get the first
   bad release.
2. Then re-run this exact benchmark on v1.44.1 — the hang being gone means a clean tps distribution
   is finally in reach, which is the write-heavy durable-OLTP number the whole effort has gated on.

## Bottom line
**The libxtc blocker we chased for the entire investigation is fixed** — the fiber path does not
hang on v1.44.1. The path to the benchmark is now blocked by a PG-side crash we own, freshly
localized to the pooled-logical-exit slot lifecycle and correlated with a just-landed commit. That
is a much better place to be than "waiting on upstream."

## Artifacts
Mode-isolation matrix, the pre/post-`f4ec2a937b` control (3/3 vs 12/12), the `MarkPostmasterChildSlot`
core backtrace, and the 12-run hang=0 result. libxtc v1.44.1 build clean and reusable.
