# libxtc report: xtc_proc_wait_fd with a FINITE timeout does not deliver the timeout wake for a migratable fiber under load (WL_TIMEOUT never fires) -- the 6th cross-loop surface

Date: 2026-09-01
libxtc: v1.40.6 (rev 3edc2e9). All five prior cross-loop fixes confirmed in (fd registry
defer, reap-before-steal, atomic task->state CAS, timer arm on running loop, cancel-before-
re-arm in xtc_proc_sleep/xtc_recv).
Consumer: PostgreSQL "xtc" branch, sessions-as-fibers path (pooled_protocol_carriers=0).
Classification: appears to be a genuine libxtc bug on the xtc_proc_wait_fd(TIMEOUT) resume
path -- the fd-park sibling of the xtc_proc_sleep/xtc_recv timer-park you fixed in v1.40.6.

--------------------------------------------------------------------------------
## TL;DR
A backend FIBER blocked in xtc_proc_wait_fd(fd, interest, timeout_ns=1000ms, &revents)
under 64-client write load intermittently NEVER receives its timeout wake: WL_TIMEOUT never
fires, the park does not return at the deadline, and if no other thread happens to SetLatch
it (write the watched fd), the fiber parks FOREVER.  We proved this by instrumenting the
exact WaitLatch call and logging its return: in a healthy window every wake is a
latch/fd-readiness wake (rc has WL_LATCH_SET, never WL_TIMEOUT); in the hang window the
WaitLatch returns 0 times in 4 seconds despite a 1000ms timeout -- the timed park is stuck.

This is the root of the write-path stall we have chased across v1.40.1..v1.40.6.  It is
NOT the deadlock detector, NOT setitimer, NOT the PG hot-cell race, and NOT the
xtc_aio_fdatasync completion (all ruled out with instrumentation).  It is the fd-park
timeout resume.

--------------------------------------------------------------------------------
## Instrumented proof (PG-side, but the missing wake is libxtc's)
PG's lock wait (ProcSleep) parks via WaitLatch(MyLatch, WL_LATCH_SET|WL_EXIT_ON_PM_DEATH,
wait_timeout, ...) -> WaitEventSetWait -> WaitEventSetWaitBlock ->
  xtc_pg_wait_fd(epoll_fd, WL_SOCKET_READABLE, timeout_ms)
    -> rc = xtc_proc_wait_fd(fd, interest, timeout_ns, &revents);   // pg_xtc_carrier.c:1370
       if (timed out) out |= WL_TIMEOUT;
We set wait_timeout = deadlock_timeout (1000ms) so the fiber MUST wake within 1s even if no
SetLatch arrives, then logged every WaitLatch return:

  healthy window:  "ProcSleep WOKE fiber=1 wait_timeout=1000 rc=1 (WL_TIMEOUT=0 WL_LATCH=1)"
                   -- wakes come from SetLatch/fd-readiness; WL_TIMEOUT is NEVER set.
  hang window:     WOKE-events delta = 0 over 4 seconds
                   -- the WaitLatch(1000ms) does not return AT ALL; no timeout wake, no
                      latch wake; the fiber is stuck inside xtc_proc_wait_fd.

Server-side commits at the hang: delta 0 over the window (frozen).  All carrier loops idle
in xtc_io_poll; the WALWriteLock holder fiber is off every OS thread (your v1.40.1..6
signature).  When the workload does NOT hit this window it runs fine (600-1900 commits/s),
so it is intermittent and migration-timing dependent -- exactly the shape of the five
cross-loop bugs you already fixed.

## Why we conclude it is the xtc_proc_wait_fd TIMEOUT path specifically
- WL_TIMEOUT is never observed to fire on a fiber, even when we explicitly pass a 1000ms
  timeout to WaitLatch (which passes timeout_ns=1e9 to xtc_proc_wait_fd).  Every successful
  wake is WL_LATCH_SET (someone wrote the fd).  So the TIMEOUT arm of xtc_proc_wait_fd
  either never arms or its fire is lost for a migratable fiber.
- In the hang, no SetLatch is coming (the peer that would write the fd is itself parked
  behind the stuck holder), so the ONLY thing that could break the wait is the timeout --
  and it never fires -> permanent park.
- v1.40.6 fixed the cancel-before-re-arm for xtc_proc_sleep and the xtc_recv timeout loop.
  xtc_proc_wait_fd is the third timed-park primitive and takes the same timeout_ns; it looks
  like it has the same class of stale-timer-across-migration / lost-timeout-wake problem
  that the other two just got fixed for.

## Reproduction
1. PG "xtc" HEAD on libxtc v1.40.6, multithreaded=on, pooled_protocol_carriers=0
   (sessions-as-fibers), fsync=on, PGDATA on NVMe, 32-vCPU.
2. pgbench -i -s 50 ; pgbench -c 64 -j 16 -T 40.  Intermittently (roughly 1 in 3-8 runs)
   commits freeze to 0 within ~15-40s and never recover; a fiber holds WALWriteLock off
   every thread, all loops idle in xtc_io_poll.
3. The distinguishing evidence vs a normal slow window: a fiber parked in
   xtc_proc_wait_fd with a finite timeout does not return at the deadline (WL_TIMEOUT never
   fires; the WaitLatch does not return at all over multi-second windows).
We can build the PG-free harness you suggested for #1-#5: N migratable fibers each doing
xtc_proc_wait_fd(pipe_fd, READABLE, timeout=1000ms) in a loop under eager rebalance + steal
churn, asserting each returns within ~timeout even when no writer touches the pipe; we
expect it to reproduce the lost timeout wake deterministically.  Say the word and we'll
send it.

## What we ruled out (with instrumentation, this session)
- Deadlock detector: fires correctly (394x) in healthy runs; the machinery works.
- setitimer/SIGALRM: PostgresMain/backend_startup correctly gate fiber backends to
  InitializeLogicalTimeouts (no setitimer); not the cause.
- PG hot current-work cell race: fixed (mode-state thread-local); collapse unchanged.
- xtc_aio_fdatasync completion: sound (you verified; our progressing runs confirm
  WALWriteLock releases fine after the flush).
The one remaining, instrumented, un-explained fact is: xtc_proc_wait_fd's finite timeout
does not wake a migratable fiber under load.

## PG-side mitigation we landed (does not fix the root)
We bounded ProcSleep's fiber lock-wait by deadlock_timeout and run CheckDeadLock on any
bounded wake -- but since the timeout wake itself does not fire, the mitigation is inert
until this libxtc fd-park timeout is fixed.  Once xtc_proc_wait_fd delivers its timeout
reliably, that guard turns any residual bucket glitch into a bounded (slow) wait rather than
a hang.

## When we come back
If v1.40.7 (or whatever carries the fix) still hangs, we will capture a fresh trace showing
whether WL_TIMEOUT now fires on a fiber.  Thank you again -- your five same-day fixes each
closed a real instance of this family; this looks like the sixth on the fd-park timeout.
