# v1.44.1: the lost-wake fix is verified present, but a PG-side crash on the fiber path blocks the benchmark

Date: 2026-09-12
libxtc: **v1.44.1** (tag `e9a2e14`), verified pristine. PG "xtc" `d314c72c7c`, cassert=OFF.
SUT: lava/us-east-2 c6id.8xlarge, XFS on local NVMe.

--------------------------------------------------------------------------------
## The libxtc fix is real and correctly built

`2c851a5` is exactly the fix for the cross-loop aio lost wake we named: `xtc_loop_wake(submit_loop)`
when a migratable fiber resumes on a different loop than it submitted on, at both aio emit sites
(`aio.c:286`, `:396`), guarded to the migration case. The comment cites our finding verbatim. The
built library reports `1.44.1`. **We could not get to measuring whether it closes the hang, because
a separate, PG-side crash on the fiber path stops the workload before the measured run.**

## The blocker: a null/underflow deref on the pooled-logical-exit path — OURS, not libxtc's

`pgbench -i` dies **deterministically** at `alter table pgbench_tellers add primary key`, and it is
mode-specific:

```
FORK           : INIT OK
POOLED(car=-1) : INIT OK
POOLED(car=4)  : INIT OK
FIBER (car=0)  : INIT_FAIL  -- server gone
```

**Only the fiber-per-session path.** systemd-coredump caught it (`SIGSEGV`, core present). The
faulting thread is the **postmaster thread**, not a carrier:

```
#3  MarkPostmasterChildSlotUnassigned (slot=0)          pmsignal.c:271
#4  ReleasePostmasterChildSlot (pmchild=0x20d34de0)     pmchild.c:700
#5  CleanupBackend (bp=0x20d34de0, exitstatus=0)        postmaster.c:3042
#6  process_pm_pooled_logical_exit ()                   postmaster.c:2896
#7  ServerLoop ()                                       postmaster.c:1975
```

`pmsignal.c:271` is `PMChildFlags[slot]` after `slot--`. With cassert OFF (confirmed:
`USE_ASSERT_CHECKING` undefined in the built `pg_config.h`), the guard `Assert(slot > 0 && slot <=
num_child_flags)` is compiled out, so **`slot=0` passes through, `slot--` underflows to `SIZE_MAX`,
and `PMChildFlags[SIZE_MAX]` segfaults.** `num_child_flags` is a healthy 486, so this is not
uninitialised shared memory — it is a bad slot value.

The interesting inconsistency: at frame 4 `pmchild->child_slot` reads `1` (valid), while the value
that reached frame 3 is `0`. Either the field is being decremented/cleared between the two frames
(a double-release: the slot was already returned and re-zeroed, then released again), or one of the
reads is a stale optimized-out register in a `-O2` build. I could not disambiguate from the core
alone (optimized frames), and I am flagging that boundary rather than asserting a mechanism.

## Why this is almost certainly the pre-existing pmsignal double-release, now deterministic

This is the same family as the cassert crash filed earlier
(`RegisterPostmasterChildActive` slot assert, `.ec2/fusion-staging-validation-2026-08-31.md`) and
the pmsignal teardown work: a PMChild slot released twice on the pooled-logical-exit path. What is
new is that it is now **deterministic on the fiber path** and fires under an ordinary
`ADD PRIMARY KEY` (an index build, which spawns/joins a session), not only under crash teardown.
`CleanupBackend` with `exitstatus=0` — a clean exit — reaching a slot that is already 0 is the
double-release signature.

## What I did and did not establish

* **Established:** v1.44.1 carries the aio lost-wake fix, correctly built. Fork and both pooled
  modes initialise fine. The fiber path crashes deterministically at `ADD PRIMARY KEY`, on the
  postmaster thread, in `MarkPostmasterChildSlotUnassigned(slot=0)` via
  `process_pm_pooled_logical_exit` → `CleanupBackend` → `ReleasePostmasterChildSlot`.
* **Not established:** whether v1.44.1 fixes the fsync lost-wake hang. **The crash prevents the
  benchmark from running at all**, so I have no hang-rate or tps measurement on v1.44.1. That is the
  honest state: the thing you asked me to re-benchmark is blocked by a *different*, PG-side bug.
* **Not established:** the exact double-release mechanism (core is `-O2`; frame-4 vs frame-3 slot
  disagreement is unresolved).

## Why I did not fix it in-line

It is PG-side and squarely in the pmsignal / PMChild-lifecycle territory that the pmsignal sub-agent
already owns (and whose earlier fix — the `ExitPostmaster` fail-stop / `RegisterPostmasterChildActive`
race — is stashed). A `ReleasePostmasterChildSlot` double-release on `process_pm_pooled_logical_exit`
is a lifecycle-critical change needing the focused lifecycle/teardown test targets and two reviews,
not an ad-hoc patch mid-benchmark. It should be handed to a fresh agent with this core in hand.

## Next step to unblock the benchmark

1. Reproduce with **cassert ON** — the `Assert(slot > 0)` will fire at the *first* bad release with
   a clean stack, turning the `-O2` ambiguity into an exact answer, and will likely catch the
   double-release one frame earlier (at the point `child_slot` is first zeroed).
2. Instrument `ReleasePostmasterChildSlot` / `AssignPostmasterChildSlot` to log slot transitions for
   a pooled-logical backend across `ADD PRIMARY KEY`, since it is deterministic.
3. Fix the double-release, then re-run this exact v1.44.1 benchmark — the lost-wake question is
   answerable the moment the fiber path survives init.

## Artifacts
Core at `coredumpctl` pid 48026 (637.9K, `SIGSEGV present`) and `/tmp/core48026` on the box;
mode-isolation matrix and full backtrace saved. The libxtc v1.44.1 build itself is clean and
re-usable.
