# RETRACTED 2026-09-16: this RCA was WRONG -- the diagnostic mislabelled a healthy park

**This document's conclusion is retracted.** libxtc v1.47.0 (commit bbe49a4) found that `park_reason`
never named the MAILBOX park: the branch meant to catch it tested `task->park_requested`, which the coro
substrate CONSUMES when converting the yield into PENDING, so it was always 0 by the time the task
reached PARKED -- dead code since it was written. A fiber healthily blocked in `xtc_recv(..., -1)`
therefore reported a bare, sourceless `PARKED`, which is ALSO what a lost wake looks like. The evidence
below was manufactured by that mislabel.

They could not reproduce a lost wake (20,000 cross-thread sends drained 6/6; all 17,615 deliveries that
found `waker_armed == 0` landed on a RUNNING receiver) and added a POSIX gate pinning it. Verified here
on v1.47.0: the same supervisor now reads `PARKED(mailbox)` with `mbox=0`, and `xtc-stranded` reports
`park=mailbox 8` with **0 suspects** (was 1). Our `/tmp` bug report is withdrawn.

**What survives:** the wedge itself is real and reproduces on a genuinely clean server at v1.47.0
(write 1354 -> 1763 -> 394 -> 0.0 forever), with libxtc confirmed healthy. The wait census at the hang is
`BufferExclusive` 30 / `transactionid` 16 / `tuple` 11 / `WALInsert` 6, so it is a PostgreSQL-side
buffer-content-lock problem -- which means my EARLIER buffer-lock findings
(`WEDGE_RCA_FINAL_HELD_BUFFER_LOCK_2026-09-14.md`) were closer to the truth than this document, and the
"reframing" this document did to them was itself wrong.

**Lesson, recorded so it is not repeated:** a "sourceless park" is a statement about the TOOL's
knowledge, not about the runtime. It must never be read as a lost wake without a second independent
signal. My four controls here were individually sound but all downstream of one mislabelled field --
they could not distinguish a healthy mailbox park from a dropped wake, so they agreed with each other
while being collectively uninformative. This is the fourth time in this investigation I attributed the
wedge to the wrong layer.

Original (incorrect) analysis retained below.

--------------------------------------------------------------------------------

# WEDGE ROOT CAUSE FOUND: the per-loop supervisor's mailbox wake is lost (libxtc bug, reported)

Date: 2026-09-15. Found with a DEBUG libxtc build + `xtc-procs` / `xtc-proc` / `xtc-mailbox` /
`xtc-tail-dropped`. This is the real cause of the write-path wedge and it **supersedes all three of my
earlier attributions** in this series (lost LWLock wakeup, held buffer content lock, backend leak).

## What it is
Our per-loop supervisor fiber (`xtc_carrier_supervisor_proc`) blocks in `xtc_recv(&msg,&len,-1)`. Under
load it stops waking even though messages keep arriving:

```
proc        pid      mbox peak save state           lnk/mon
0x12718b0   1.1.1     36   36    0  PARKED          0/24     <-- supervisor: 36 queued, NO park source
0x7fff...   1.2.1      0    0    0  PARKED(fd 1185) 0/0      <-- every normal fiber shows its source
```

Frozen, not slow -- three samples 4 s apart are bit-identical, `recv_total` stuck at 64 while depth grew
23 -> 29 -> 36. The queued messages are `size=24` (our spawn request) with `from=0.0.0`
(`XTC_PID_NONE`), i.e. delivered by **cross-thread `xtc_send` from the postmaster thread**, which has no
loop of its own.

## Why it wedges the whole server
Spawn requests are deliberately routed *through* the supervisor so `xtc_proc_spawn_monitor()` is atomic
and a fast-crashing child cannot die unobserved. So if the supervisor stops draining:
- that loop can never create another backend fiber -> new sessions on it are never served;
- sessions already holding row/buffer locks can no longer be joined by the sessions that would let them
  finish, so waiters pile up (`Lock/tuple` 53, `Lock/extend` 22, `Lock/transactionid` 5 in one capture);
- the cluster looks like a lock pile-up, which is exactly why I misdiagnosed it three times.

## Controls (all clean -- this is not an observability artifact)
- `xtc-tail-dropped`: *"ring did not wrap: absence claims are meaningful."*
- `kill_pending=0`, `crit_depth=0`, `recovery armed=1 fired=0` -- not masked, killed, or in a critical
  section.
- `drop_total=0`, `depth=36` of `cap=4096`, senders got `XTC_OK` -- nothing dropped, no backpressure.
- Loop 1 is alive: its other fibers park normally on fds and the loop thread sits in `epoll_wait`.
- `wm lvl=0 fired=0` -- no watermark interaction.

## Reported, not worked around
Filed `/tmp/libxtc-cross-thread-send-does-not-wake-parked-recv-2026-09-15.md`. It smells like the same
family as the v1.44.1 cross-loop aio lost-wake (`xtc_loop_wake` on the submit loop when the resume
happens elsewhere): here the sender is on a thread with **no loop**, so whatever nudges the target loop
may be skipped or may race the park. We also asked for (a) a debug-build assert when a send succeeds into
a `PARKED`-with-no-source receiver whose depth goes 0 -> 1, and (b) an explicit statement in the
`xtc_send` docs about whether send wakes a parked receiver -- the current doc block covers queueing and
backpressure thoroughly but never says so, and we assumed it.

Per the standing directive we are NOT building a PG-side workaround (e.g. a polling `xtc_recv` timeout,
or having the postmaster spawn directly and lose the atomic spawn+monitor guarantee). Both would paper
over a runtime contract failure and cost us the crash-observability property we deliberately bought.

## How this reframes the earlier findings in this directory
- `WEDGE_RCA_FINAL_HELD_BUFFER_LOCK_2026-09-14.md`: the held buffer lock and the empty
  `pg_blocking_pids()` were REAL observations but downstream effects -- the holder could not finish
  because the loop that would run its peers was spawn-starved.
- `LOST_LWLOCK_WAKEUP_PROVEN_2026-09-14.md`: the waiters really were armed with `eventfd-count: 0`; no
  release ever happened because the releasing side was starved, not because the LWLock wake was lost.
  The "proof" was sound about *what* was true and wrong about *why*.
- `FIBER_BACKEND_LEAK_IS_THE_REAL_WEDGE.md`: backends did accumulate -- they were the sessions that
  could never be spawned/reaped on the affected loop.
- `WEDGE_IS_FIBER_SPECIFIC_NOT_CONTENTION_2026-09-15.md`: still correct and now explained -- fork
  sustains 3,060 tps on the same workload because it has no supervisor mailbox in the spawn path.

The three wait-path fixes committed in `41a97e3d94` remain valid on their own merits (they are real
defects and lifted write tps 198 -> 499), they just were never going to fix this.

## Next
Blocked on the libxtc fix for the wedge itself. Meanwhile the useful adjacent work is the
`quickdie()`-equivalent resource tracking the owner asked for: register every abandonable resource
(LWLocks, buffer content locks/pins, WAL insertion slots) with `xtc_scope`/`xtc_proc_at_exit` so a fiber
can be force-killed without leaving shared state mid-mutation. That is the prerequisite for per-fiber
kill (see IMMEDIATE_SHUTDOWN_CANNOT_KILL_WEDGED_FIBER.md) and is independent of this bug.
