# Phase 9 Wait/Wakeup Boundary Notes

Phase 9 is in progress. The goal is to make blocking waits visible and
targetable before regular backends can run as threads.

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

## Validation So Far

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

## Remaining Phase 9 Work

- inventory and route other long wait families through the explicit boundary
  when they do not already use `WaitEventSetWait()`;
- validate idle timeout, transaction timeout, cancellation, termination,
  config reload, LISTEN/NOTIFY, and disconnect/FATAL behavior;
- decide whether frontend command reads and output flushes need a more
  connection-specific suspend kind before Phase 10.
