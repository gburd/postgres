# Fiber park instrumentation: what it proved, and what it did NOT

Date: 2026-09-16. Added `PG_XTC_PARK_TRACE=1` instrumentation to the fiber branch of
`WaitEventSetWaitBlock` to find why a fiber parked on a ready epoll fd is never resumed.

## PROVEN: the wedge is a hot re-park spin, not a lost wake
The stuck fiber is **waking and harvesting an event every time**, then re-parking. Trace of the last
parks of the fiber whose client socket held unread data (`fd 1554`, `Recv-Q=76`, watched by `efd=1589`
with `events 0x19`):

```
PARKTRACE pid=6.7.1 efd=1589 timeout=8303 wl=0x2 rc=1 nev=3 harvest
PARKTRACE pid=6.7.1 efd=1589 timeout=8300 wl=0x2 rc=1 nev=3 harvest
PARKTRACE pid=6.7.1 efd=1589 timeout=8296 wl=0x2 rc=1 nev=3 harvest
...  1410 parks total, rc=1 (one event harvested) on 1396 of them
```
`timeout` decays 9999 -> 8250 over 1410 iterations, i.e. **~1.75 s of wall time for 1410 round trips**
-- a spin, not a hang. So `xtc_pg_wait_fd` is doing its job and libxtc is delivering wakes; the fiber
harvests a ready event, the decode loop returns `returned_events == 0`, and the outer
`while (returned_events == 0)` re-parks immediately.

That kills the "lost fd wake" framing outright: the wake is delivered ~800x/second and *discarded*.

## The dominant shape (439,705 traced parks, whole server)
```
422,925  pos=0  pgev=0x2  epev=0x1
 16,578  pos=0  pgev=0x1  epev=0x1
    140  pos=1  pgev=0x1  epev=0x1
     42  pos=0  pgev=0x2  epev=0x11
```
`pos=0` is the client socket. `pgev` is `WaitEvent.events`, `epev` is what epoll returned.
**96% of all parks are `pgev=0x2` (`WL_SOCKET_WRITEABLE`) with `epev=0x1` (`EPOLLIN`)** -- the caller is
waiting for *writable* and epoll is reporting *readable*. The decode at waiteventset.c:1716-1728 only
sets `WL_SOCKET_READABLE` if the caller asked for it, so that combination yields nothing and spins.

A `WL_SOCKET_WRITEABLE` registration maps to `EPOLLOUT|EPOLLERR|EPOLLHUP` (0x1c) and **cannot** return
`EPOLLIN`, so on its face this is a mirror/kernel disagreement.

## NOT PROVEN: that a mirror/kernel desync causes it
I added a desync detector (compare `e0->events` against the epoll bits actually returned, flag any bit
epoll returned that the mirror did not request) and it fired **zero times** in a full run -- but **that
run did not wedge** (it degraded to 95-160 tps and kept progressing). So the detector never observed the
wedged state, and my desync hypothesis is **unconfirmed, not disproven**. I am recording that distinction
rather than claiming a root cause I have not demonstrated; overclaiming is how the previous six
misattributions happened.

Supporting-but-not-conclusive observation: every client socket registration visible in
`/proc/<pid>/fdinfo/*` reads `events: 19` (`EPOLLIN|EPOLLERR|EPOLLHUP`) and **`EPOLLOUT` (0x4) never
appears anywhere**, across 29 registrations. If PG ever genuinely registered a writable-only wait, the
kernel does not show it. That is consistent with the desync theory but could also mean the sampled sets
simply were not in a writable wait at sample time.

## Why `ModifyWaitEvent`'s fast path is the prime suspect
```c
/* waiteventset.c:882 */
if (events == event->events && ... )
    return;                      /* skips epoll_ctl(EPOLL_CTL_MOD) entirely */
```
This is sound **only if `event->events` faithfully mirrors the kernel registration**. It is an upstream
optimisation for exactly this pattern ("ModifyWaitEvent is frequently used to switch from waiting for
reads to waiting on writes"). On the fiber path the FeBe wait set is per-connection and **persists across
parks** (postgres.c:6370 documents this), and position 3 is lazily populated with the *current* PGPROC's
`interrupt_wake_fd` -- so a set can outlive the fd generation its kernel registrations were made for. Any
path that writes `event->events` without a matching `epoll_ctl` makes the fast path silently wrong and
produces precisely the observed spin.

## Next step (concrete, and the instrumentation is committed to support it)
The detector must run *in a wedged process*. Two ways:
1. Widen the reproduction window: the wedge appeared at 45 s and 90 s in earlier runs but not in the
   detector run, so run the detector build repeatedly / longer until it wedges (it is env-gated and
   costs nothing when off).
2. Better: assert the invariant at the *write* site rather than the read site -- in `ModifyWaitEvent`,
   before taking the fast-path return, verify the kernel registration for that fd with
   `epoll_ctl(EPOLL_CTL_MOD)` unconditionally under `USE_ASSERT_CHECKING`, or read it back. If the fast
   path is the bug, an unconditional `epoll_ctl` on the fiber path is also the *fix* -- it costs one
   syscall per wait-mask switch and removes the correctness dependency on the mirror.

I have deliberately not applied that as a fix yet, because I cannot yet show it changes the wedge, and a
"fix" validated only by "the symptom did not recur in one run" is what produced several of the earlier
false conclusions here.

## Instrumentation left in place
`PG_XTC_PARK_TRACE=1` (off by default, `USE_XTC_CARRIER` only): `PARKTRACE` per park (timeout, wl, rc),
`PARKTRACE2` when an event is harvested (pos, PG mask, epoll bits), and `PARKDESYNC` when epoll returns a
bit the mirror did not request. `meson regress` passes with it compiled in.
