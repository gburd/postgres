# fd-leak part 2: eventfd leak under HammerDB's simultaneous-fail storm (2026-08-05)

## What the socket-close fix (966ba480fe4) did and did NOT resolve
FIXED: the dup'd client-SOCKET leak on the threaded error-recovery release paths.
Validated: pgbench -C -c 64 -T 60 -> fds FLAT (2320->3115), 0 SSL errors.

STILL LEAKS: HammerDB @384 VUs (all connect simultaneously) -> tps cell NA again,
264/384 fail "SSL exchange", postmaster fd count back to 8,532.  fd-type sample:
398 of 400 are anon_inode:[eventfd].  So the residual leak is EVENTFDs, not
sockets -- a DIFFERENT leak than the one just fixed.

## Why pgbench -C passed but HammerDB fails
pgbench -C reconnects SEQUENTIALLY per client (<=64 connects in flight at once).
HammerDB opens all 384 VUs SIMULTANEOUSLY -> 384 concurrent sessions failing at
startup at once, each leaking an eventfd; HammerDB retries -> thousands leak ->
fd exhaustion -> "SSL exchange".  The socket-close fix handles the sequential
case; the eventfd leak only bites at high simultaneous-failure count.

## Which eventfd (localized, not yet fixed)
NOT sem_wake_fd/interrupt_wake_fd (proc.c:333/338 -- those are static, created
once per PGPROC slot in InitProcGlobal's TotalProcs loop; ~2*TotalProcs of them
= the ~2320 tps BASELINE fd count, not a leak).
NOT the pooled carrier self-pipe (per-carrier, 16 static) nor pooled_protocol_
wake_fd (1 static).
The leak is the PER-BACKEND wait-event-set signal eventfd (PgCarrier
wait_event_signal_fd / the WaitEventSet self-pipe/eventfd created when a backend
fiber sets up its latch/wait-event support -- InitializeLatchWaitSet /
InitializeWaitEventSupport).  In thread-per-session each session is a fiber with
its own wait-event-set -> its own signal eventfd.  When a fiber FAILS during
startup (before the normal teardown that closes it), that eventfd leaks.  384
simultaneous startup-failures -> 384 leaked eventfds/wave.

## The fix (next, scoped)
Audit the backend-fiber startup FAILURE path (the sigsetjmp/longjmp error exit,
and any FATAL during BackendInitialize/ProcessStartupPacket on a tps fiber) and
ensure the wait-event-set signal eventfd (and any per-session self-pipe/eventfd)
is closed on EVERY exit, not just the clean one.  Likely the fix is in the
backend-fiber exit path (backend_thread_exit / the tps equivalent of
backend_pooled_protocol_exit_logical) to close the wait-event-set's fds, OR in
FreeWaitEventSet being reached on the failure path.  Mirror the socket-close fix:
make the exit/release path close-if-open, idempotent.
INSTRUMENT FIRST: on a c7i box, tps server, fire 384 SIMULTANEOUS connects that
FAIL (or a HammerDB-style burst), and watch the eventfd count -- confirm it
climbs, land the close, confirm it stays flat (like the socket fix's pgbench -C
plateau).

## Fork baseline (valid, consistent across ab5/6/7/8)
@384: srv_tpm ~2.40-2.45M, ctx-switch ~411-414k/s, RSS ~58MB.

## Status
Socket-leak fix LANDED + validated (bounded under pgbench -C).  Eventfd leak
LOCALIZED (per-backend wait-event-set signal fd, leaked on tps fiber startup
failure) -- the next scoped fix, instrument-first.  The oversubscription A/B
stays blocked on it (HammerDB's simultaneous-VU open triggers it).  Blocker
chain: ... accept-serialization (FIXED) -> socket leak (FIXED) -> eventfd leak
on simultaneous startup-failure (localized, next).  All EC2 torn down + verified.
