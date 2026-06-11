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

## Remaining Phase 9 Work

- inventory and route other long wait families through the explicit boundary
  when they do not already use `WaitEventSetWait()`;
- expose target-backend wake hooks that do not depend on process-directed Unix
  signals;
- validate idle timeout, transaction timeout, cancellation, termination,
  config reload, LISTEN/NOTIFY, and disconnect/FATAL behavior;
- decide whether frontend command reads and output flushes need a more
  connection-specific suspend kind before Phase 10.
