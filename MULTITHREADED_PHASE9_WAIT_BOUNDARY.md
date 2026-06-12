# Phase 9 Wait/Wakeup Boundary Notes

Phase 9 is complete for the thread-per-session prerequisite. Blocking waits
that can hide a regular backend from cancellation, termination, or timeout
delivery now cross a visible wait boundary and logical interrupt delivery can
wake the target backend's recorded latch.

## First Slice

The first slice introduces `PgSuspend()` in the backend runtime and routes
`WaitEventSetWait()` through it. This preserves existing process-mode blocking
behavior while recording the current logical backend's wait state before the
blocking wait body runs.

The new runtime state is:

- `PgWaitSpec`: wait kind, wait event info, wake event mask, and timeout;
- `PgBackendWaitState`: the current wait spec plus an atomic `waiting` flag;
- `PgSuspend()`: publishes the wait spec, sets `waiting`, invokes the existing
  wait implementation callback, and clears the wait state on normal return or
  `ERROR`.

For this slice only `PG_WAIT_KIND_EVENT_SET` exists, backed by
`WaitEventSetWait()`. Higher-level waits such as `WaitLatch()` and
`WaitLatchOrSocket()` already use `WaitEventSetWait()`, so they cross the same
logical suspend boundary without changing their public API.

The `waiting` flag is atomic and is published after the wait spec is copied.
Future scheduler or interrupt-routing code should treat the atomic flag as the
authoritative indicator that the spec is live.

## Target Backend Wake Slice

The second slice makes logical interrupt wakeups target a backend object rather
than assuming the target is always `CurrentPgBackend`.

The runtime now records an interrupt latch on `PgBackend`:

- process mode initializes it from `MyLatch`;
- latch switches between local and shared process latches refresh it;
- `PgBackendRaiseInterrupt()` sets the target backend's mailbox bit and calls
  `SetLatch()` on that backend's recorded interrupt latch;
- `CHECK_FOR_INTERRUPTS()` now treats either legacy `InterruptPending` or a
  non-empty current-backend mailbox as pending work.

This preserves the fast path for process-mode signal handlers by still setting
`InterruptPending` when the target backend is the current backend. For a future
threaded runtime, a different carrier can set mailbox bits and wake the target
backend's latch without depending on Unix process signals or the sender's
thread-local interrupt flags.

The `test_backend_runtime` module now includes a focused C-level regression
test that creates a fake non-current backend, gives it a local latch, raises a
logical cancel interrupt against it, and verifies both that the target mailbox
is visible when that backend becomes current and that the target latch was set.

## Wait Family Inventory

The Phase 9 inventory checked the major long-wait families that can hide a
backend from cancellation or termination:

- frontend command reads and output flushes use `secure_read()` and
  `secure_write()` in `src/backend/libpq/be-secure.c`; both wait through
  `WaitEventSetWait(FeBeWaitSet, ...)` with `WAIT_EVENT_CLIENT_READ` or
  `WAIT_EVENT_CLIENT_WRITE`;
- connection liveness probes use `pq_check_connection()`, which polls
  `FeBeWaitSet` through `WaitEventSetWait()`;
- latch waits and socket/latch waits use `WaitLatch()` or
  `WaitLatchOrSocket()`, which route through `WaitEventSetWait()`;
- heavyweight lock waits use `ProcSleep()`, which blocks through
  `WaitLatch()`;
- condition-variable waits use `ConditionVariableTimedSleep()`, which blocks
  through `WaitLatch()`;
- shared memory queue waits use `WaitLatch()`;
- walsender waits use `WaitEventSetWait(FeBeWaitSet, ...)`;
- async append waits use `WaitEventSetWait()`.

Those paths now publish `PgBackendWaitState` on the current `PgBackend` before
entering the blocking wait body. The current session and execution are
reachable through the same `PgBackend`, which is sufficient for Phase 9's
thread-per-session target. Scheduler-aware phases may later copy richer
session, execution, connection, and file-descriptor details into `PgWaitSpec`
when a wait must suspend a logical task instead of blocking the current
carrier.

The remaining direct `pg_usleep()` users found in this pass are bounded polling
or deliberate short-delay sites such as vacuum cost delay, standby recovery
polling, short procarray/dropdb polling, spinlock backoff, and file-descriptor
retry loops. They do not need new Phase 9 routing before thread-per-session
launch because they do not represent unbounded hidden waits. They remain
candidates for explicit scheduler yield points in Phases 13-15.

Frontend command reads and output flushes do not need a separate connection
wait kind before Phase 10. `wait_event_info` already distinguishes client read
and write waits, `CurrentPgConnection` identifies the connection in the
thread-per-session runtime, and the Phase 10 carrier is allowed to block.

## Validation

- focused object rebuilds for `waiteventset.o` and `backend_runtime.o`;
- full incremental `gmake -j8`;
- full `gmake -j8 DESTDIR="$PWD/tmp_install" install`, followed by the
  standard macOS `install_name_tool` patching for temp-installed frontend
  binaries;
- `git diff --check`;
- global-lifetime scanner baseline check, still leaving only the documented
  `tsrank.c` typedef artifact;
- direct isolation `timeouts` regression test;
- live temp-cluster blocked-backend smoke: one `pg_sleep()` backend was woken
  by `pg_cancel_backend()`, another was woken by `pg_terminate_backend()`, and
  the server accepted a subsequent query.
- after the target-wake slice changed the exported `PgBackend` layout, a clean
  backend rebuild regenerated backend-side headers and rebuilt `src/backend`
  from scratch to avoid stale object offsets;
- `gmake -C src/test/modules/test_backend_runtime check` passed outside the
  managed sandbox, covering temp-install `initdb` and the target-latch wake
  regression;
- direct isolation `timeouts` passed against the fresh temp install;
- live temp-cluster cancel/terminate smoke passed again after the target-wake
  slice.
- live Phase 9 behavior smoke passed for transaction timeout during
  `pg_sleep()`, idle-in-transaction timeout, idle-session timeout,
  `LISTEN`/`NOTIFY` delivery to an idle backend, config reload while another
  backend was waiting, blocked-backend cancellation after reload, and
  client-disconnect detection with `client_connection_check_interval`;
- core process-mode regression passed with `gmake -C src/test/regress check`
  after the target-wake slice.
