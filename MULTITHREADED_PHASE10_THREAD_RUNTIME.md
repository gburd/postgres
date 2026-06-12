# Phase 10 Thread-Per-Session Runtime Notes

Phase 10 is in progress. The goal is to run regular client backends as OS
threads inside one server runtime while preserving the process launch path.

## Launch Selection Scaffold

The first slice adds the explicit launch selector without starting backend
threads yet:

- `multithreaded` is a hidden `PGC_POSTMASTER` developer GUC, defaulting off;
- `PgRuntimeGetBackendLaunchModel()` returns `PG_BACKEND_LAUNCH_THREAD` only
  when `multithreaded` is enabled for a regular `B_BACKEND` launch;
- `postmaster_child_launch()` now consults the runtime selector before the fork
  path;
- the threaded branch currently logs a clear "not implemented yet" message and
  rejects the connection instead of silently forking.

Dead-end backends, auxiliary workers, background workers, and other
server-owned worker families remain process launches. WAL senders still begin
life as `B_BACKEND`; they need an explicit policy once the regular backend
thread launcher exists.

The next slice should add a backend thread portability layer and a real thread
launcher that can run `BackendMain()` with carrier-local runtime pointers.

## Thread Portability Slice

The second slice adds the minimal backend port wrapper for native carrier
threads:

- `PgThread` stores the native thread handle;
- `pg_thread_create()` creates a named carrier thread from a
  `void (*)(void *)` routine;
- `pg_thread_join()` and `pg_thread_detach()` cover the lifecycle operations
  needed by thread-per-session launch and teardown;
- `pg_thread_set_name()` sets a native thread name where this checkout can
  compile the platform support.

This wrapper deliberately does not expose mutexes, condition variables, or a
thread pool. Those belong in later runtime/scheduler layers once backend
threads can actually run.

The Windows implementation is a best-effort `_beginthreadex()` wrapper and has
not been validated in this macOS checkout.

## Explicit Backend Startup Mode Slice

The third slice starts separating backend startup from process inheritance:

- `BackendMain()` remains the existing postmaster-child process entrypoint;
- `BackendMainWithStartupData()` now takes explicit startup data, an explicit
  client socket, and a `BackendStartupMode`;
- process mode uses `BACKEND_STARTUP_PROCESS`, preserving the historical
  `SIGTERM`, `SIGALRM`, startup-packet timeout, and `_exit()` behavior before
  shared memory is touched;
- `BACKEND_STARTUP_THREAD` is named but still fails explicitly because startup
  timeout and termination still need to be routed through logical backend
  exit before it can safely run inside the postmaster address space.

This is intentionally not the thread launcher yet. It creates the call shape
the launcher needs and makes the remaining process-only startup semantics
visible.

## Validation

- `gmake -C src/backend/postmaster launch_backend.o` passed;
- `gmake -C src/backend/utils/init backend_runtime.o globals.o` passed;
- `gmake -C src/backend/utils/misc guc_tables.o` passed after regenerating
  `guc_tables.inc.c`;
- `gmake -C src/backend/tcop backend_startup.o` passed after the explicit
  startup-mode slice;
- full `gmake -C src/backend -j8` passed;
- `gmake -C src/test/modules/test_backend_runtime check` passed, including the
  `pg_thread_create()`/`pg_thread_join()` smoke;
- temp install smoke with the default `multithreaded=off` showed `off` and ran
  `SELECT 1`;
- temp install smoke with `multithreaded=on` rejected a client connection with
  `Function not implemented` and logged the explicit threaded-launch stub
  message.
