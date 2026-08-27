# libxtc: missed cross-loop wake to an idle io_uring loop (parked-fd readiness)

Date: 2026-08-27.  libxtc v1.38.0 (rev f1a50cc9 / commit 44498a7).
Reporter: PostgreSQL threaded-runtime (pooled protocol scheduler) integration.
Evidence: gdb backtraces + a deterministic reproduction, below.

## Symptom

Under the pooled-carrier protocol scheduler, a low-frequency IDLE client session
(HammerDB's TPROC-C monitor VU, which runs one `select sum(xact_commit+xact_rollback)
from pg_stat_database` every few seconds) HANGS INDEFINITELY under concurrent write
load, while high-frequency worker sessions run fine.  The query executes server-side
in <1ms (logged), but its result/next-step never resumes: the owning session is parked
and no carrier ever picks it up.

## gdb evidence (8 pooled carriers, 8 write workers hammering)

With the monitor's session stuck (psql blocked 600+ s on the query), all 8 carrier
threads are idle INSIDE libxtc's own loop, NOT in the PG-level wait:

    8x  backend_pooled_protocol_carrier_entry   (PG carrier loop)
    7x  xtc_io_poll  <- from /usr/local/lib64/libxtc.so.1   (idle io_uring wait)
    2x  epoll_wait

i.e. the carriers have returned to libxtc's `xtc_io_poll` / loop step and are blocked
there.  The parked session's socket becomes readable (its next query arrives), but the
readiness does not wake any carrier's idle io_uring loop -> the session is never
resumed.

This matches the hazard our own code comment already flagged
(launch_backend.c:244): "a cross-thread wake to an idle io_uring loop can be missed in
the current implementation."

## What we ruled out (so it points at the loop-wake path, not our queue)

- The query itself, cursors, async single-row: all work in isolation.
- 16 read workers + the monitor query: works (no hang).
- 8 stored-proc WRITE workers + the monitor query in ONE process: works.
- We fixed a PG-SIDE lost-wakeup (a carrier sleeping on our queue-only wait while a
  parked fd went readable) -- that reduced but did NOT eliminate the hang, because the
  residual stall is at the libxtc loop level (carriers in xtc_io_poll), below our
  queue.

## The ask

When a carrier's io_uring loop is idle in xtc_io_poll and another thread makes a fd
that loop is responsible for (or should poll) become readable -- OR posts cross-thread
work targeting that loop -- the idle loop must be woken (eventfd/self-pipe nudge in the
io_uring SQ, or an IORING_OP that the waker can trigger).  Today that wake is missed
for a low-frequency/idle consumer, stranding it.

Concretely: does xtc_io / the loop expose a "wake this loop's io_uring wait from another
thread" primitive that is guaranteed not to be lost if the target is between
"decided to sleep" and "entered io_uring_enter"?  If there is one, our integration may
be mis-using it; if not, this is the missing primitive.  Either way a
lost-wake-free cross-loop nudge closes this.

## Reproduction (PostgreSQL side, for context)

multithreaded=on, pooled_protocol_carriers=8; build a small TPROC-C schema; run 8
neword write workers; from a separate connection issue
`select sum(xact_commit+xact_rollback) from pg_stat_database` repeatedly -- it
intermittently hangs 600+ s.  gdb -p <postmaster> "thread apply all bt" shows the
carriers in xtc_io_poll as above.

Impact: HammerDB cannot measure threaded TPROC-C NOPM (its monitor VU hangs -> FINISHED
FAILED).  We worked around measurement by counting committed new-orders server-side
(threaded = 43k NOPM = 0.44x fork at VU=64), but the hang is a real
idle-session-liveness bug that would also strand real monitoring tools (Datadog,
pg_stat pollers) against a busy pooled server.
