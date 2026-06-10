# Multithreaded PostgreSQL Agent Guide

This repository is an experimental branch for making PostgreSQL capable of
running backend sessions in a multithreaded runtime. The branch is allowed to
be ambitious and is not currently optimized for upstream patch shape.

Implementation is now underway. Keep the plan and architecture notes current as
the code evolves.

## Project Docs

- [MULTITHREADED_ARCHITECTURE.md](MULTITHREADED_ARCHITECTURE.md) describes the
  desired end-state architecture.
- [MULTITHREADED_PLAN.md](MULTITHREADED_PLAN.md) describes the staged
  implementation plan, validation strategy, and risk register.
- [MULTITHREADED_PHASE5_INTERRUPTS.md](MULTITHREADED_PHASE5_INTERRUPTS.md)
  records the logical interrupt boundary and recovery-conflict fixture
  decision.
- [MULTITHREADED_PHASE6_EXIT.md](MULTITHREADED_PHASE6_EXIT.md) records the
  current backend lifecycle/exit boundary and deferred thread-runtime proof.
- [MULTITHREADED_PHASE7_EXTENSIONS.md](MULTITHREADED_PHASE7_EXTENSIONS.md)
  records the extension backend-model gate and PL/pgSQL audit result.
- [refs/REFERENCES.md](refs/REFERENCES.md) lists external references.
- [refs/pgconf-2025-multithreading-transcript.md](refs/pgconf-2025-multithreading-transcript.md)
  is the local transcript of the PgConf.dev 2025 talk that motivates this work.

## Working Assumptions

- Use Heikki Linnakangas's multithreading branch as reference material, not as
  a base to merge wholesale.
- Preserve multiprocess PostgreSQL as a supported backend model.
- The first native threading target should be thread-per-session. The longer
  term target is an explicit scheduler that can map sessions/executions to a
  pool of carriers.
- Thread-per-session for regular client backends is not the final normal-mode
  target. Normal threaded server mode should eventually run in-tree
  server-owned workers, including autovacuum and auxiliary worker families, as
  threaded runtime-owned workers rather than forked subprocesses.
- Single-user mode, bootstrap mode, frontend command-line utilities,
  postmaster/control-plane process lifetime, and crash-escalation paths are
  deliberate process-lifetime exceptions.
- Do not overfit the design to WASM. Keep the main-loop and wait-boundary
  abstractions clean enough that a future host-driven runtime can use them.
- Existing third-party C extensions may be process-backend-only. That is an
  acceptable compatibility break for threaded mode.
- Existing third-party background workers may remain process-only or be
  rejected in threaded mode unless explicit worker-runtime metadata opts them
  in.
- In-tree modules and important bundled languages, especially PL/pgSQL, should
  have a plausible path to work in threaded mode.

## Source Orientation

Important current files:

- `src/backend/tcop/postgres.c`: `PostgresMain()`, the top-level backend loop,
  error recovery, command read, command dispatch, and `ProcessInterrupts()`.
- `src/include/access/session.h` and `src/backend/access/common/session.c`:
  existing `Session` abstraction for session-scoped DSM/DSA state. Treat this
  as a seed for the broader session object unless there is a strong reason not
  to.
- `src/include/miscadmin.h`: widely visible process/session globals and the
  interrupt macros.
- `src/backend/storage/ipc/procsignal.c`: process-signal-style backend
  communication.
- `src/backend/storage/ipc/latch.c` and `src/backend/storage/ipc/waiteventset.c`:
  wait/wakeup infrastructure.
- `src/backend/postmaster/launch_backend.c` and
  `src/backend/postmaster/postmaster.c`: backend launch and supervision.
- `src/backend/postmaster/autovacuum.c`,
  `src/backend/postmaster/auxprocess.c`,
  `src/backend/postmaster/bgworker.c`, and the individual auxiliary worker
  files under `src/backend/postmaster/`: worker launch, supervision, and
  server-owned worker lifecycles.
- `src/backend/replication/walreceiver.c`,
  `src/backend/replication/logical/launcher.c`,
  `src/backend/replication/logical/worker.c`, and
  `src/backend/storage/aio/method_worker.c`: replication and AIO worker
  lifecycles that must eventually use the threaded worker runtime.
- `src/include/fmgr.h` and `src/backend/utils/fmgr/dfmgr.c`: extension module
  ABI checks.
- `src/pl/plpgsql`: PL/pgSQL implementation.

## Development Rules For This Branch

- Keep documentation and code commits coherent. Prefer one conceptual change
  per commit.
- Before editing core code, read the surrounding implementation and current
  comments. PostgreSQL has many invariants that are documented only locally.
- Keep process-mode behavior working after each implementation phase.
- Use static annotations and tools to classify globals before moving large
  amounts of state.
- Do not attempt thread launch until the thread-safety floor is in place:
  backend-local globals must not be shared plain process globals, backend exit
  must not terminate the whole runtime, and timeout/interrupt delivery must be
  per logical backend.
- Prefer introducing compatibility wrappers around current globals before
  changing all call sites.
- Avoid broad mechanical churn unless it unlocks a specific migration step.
- Do not remove process isolation paths merely because threaded mode exists.

## Terminology

- Runtime: one server runtime inside an address space. In process mode, each
  backend process has its own private address space plus shared memory. In
  threaded mode, many backends share one address space.
- Carrier: the physical execution vehicle, such as an OS process, OS thread,
  or future host scheduler worker.
- Backend: a logical PostgreSQL backend identity visible to cancellation,
  statistics, lock ownership, and monitoring.
- Session: SQL session state for a client or pooled logical session.
- Execution: active transaction/query/portal work currently consuming backend
  resources.
- Connection: frontend/backend protocol transport. It is usually a socket in
  native PostgreSQL, but should not be architecturally identical to a session.
