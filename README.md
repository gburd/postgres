Multithreaded PostgreSQL Experimental Branch
============================================

This repository is an experimental PostgreSQL branch exploring how PostgreSQL
can run backend sessions in a native multithreaded runtime while preserving the
existing multiprocess backend model.

The aim is not to bolt `pthread_create()` onto PostgreSQL and call it done. The
branch is making backend, session, connection, execution, interrupt, lifecycle,
and wait state explicit so that PostgreSQL can support:

- the existing process-per-backend model;
- a native thread-per-session runtime for regular client backends;
- threaded in-tree worker families where ownership is understood;
- a protocol-boundary scheduler that can park idle frontend input and run
  logical sessions on a bounded pool of protocol carrier threads.

The current implementation starts from PostgreSQL `REL_19_BETA1`. Phase 14
closed the first protocol-boundary scheduler foundation: only top-level
frontend protocol input may detach from its carrier, while deep waits remain
carrier-pinned. The active direction is Phase 15: making the real bounded
protocol carrier pool reliable and measurable without regressing process mode
or thread-per-session mode.

Important constraints:

- process-mode PostgreSQL must continue to work;
- arbitrary third-party C extensions are not assumed to be thread-safe;
- thread-per-session is a milestone, not the final scheduler design;
- pooled protocol-carrier scheduling is intentionally limited to frontend input
  parks at this stage;
- correctness and lifecycle ownership come before broad performance claims.

Current status:

- Phase 14 protocol-boundary scheduler foundation is in place.
- Phase 15 is active, focused on the real protocol carrier pool, wakeup
  reliability, and honest throughput/memory measurements.
- Process mode remains supported.
- Thread-per-session mode runs regular client backends and normal SQL paths.
- Pooled protocol-carrier mode can park idle frontend input and resume sessions
  on a bounded carrier pool while preserving session state.
- Core backend/session/connection/execution/carrier state has explicit runtime
  ownership sufficient for startup, command execution, PL/pgSQL, core GUC
  behavior, logical interrupts, cancellation, termination, teardown, reconnect,
  worker handoff, and the current in-tree worker runtime scope.
- The branch is still experimental. It is not a production-ready PostgreSQL
  server and does not claim contrib-wide threaded extension support.
- Phase 16 owns broader extension hardening, bundled procedural languages
  beyond PL/pgSQL, and the full custom/extension GUC matrix.

Current validation baseline:

- `make check`
- `make check-threaded`
- `make check-threaded-smoke`
- `make check-threaded-150`
- `make check-threaded-200`
- `make check-threaded-world-core`

These validation targets were green in the local WSL development tree after
the latest full Phase 15 benchmark run on June 22, 2026. `check-world` is not a
current green target for this branch. Re-run the focused set above before
claiming a new release-quality checkpoint, because this branch intentionally
keeps changing runtime ownership boundaries.

Performance guidance:

Performance work is currently measured against vanilla PostgreSQL 19 beta 1
using vanilla `pgbench` as the client. The reusable local runner is
`src/tools/benchmark/mtpg_pgbench_matrix.pl`; it compares vanilla process mode,
this branch in process mode, this branch in thread-per-session mode, and
configurable pooled protocol carrier lanes such as `branch_pool_64`,
`branch_pool_128`, and `branch_pool_512`. The Phase 15 scenario wrapper is
`src/tools/benchmark/mtpg_phase15_benchmark_suite.pl`; its `pinned_hot` profile
checks hot-path parity, while its pooled profiles measure mostly-idle, real-ish
idle/wake, stateful session, reconnect-heavy, and large-connection-population
shapes.

The latest full local suite completed all 12 profiles with zero failed
transactions across 113 TPS result rows. Detailed results and workload
definitions are in [benchmark results](MULTITHREADED_BENCHMARKS.md). The short
headline is:

| Signal | Current result |
| --- | --- |
| Hot tiny-query path | Branch process is 0.907x to 0.934x vanilla; pinned threads are 0.797x to 0.952x vanilla depending on workload. |
| 200 mostly-idle clients, 100 ms wake cycle | Pooled carriers are near process/threaded throughput once the pool is at least 64 carriers; pool 32 shows an `app_mixed` outlier. |
| 1000 mostly-idle clients, `SELECT 1; \sleep 1000 ms; SELECT 1;` | `branch_pool_128` reaches 978 TPS versus 997 TPS for pinned threads, while using 122 server threads instead of 1008. |
| 1000 mostly-idle memory profile | Pooled lanes use about 537 KB to 561 KB PSS per client versus 961 KB for pinned threads, 1062 KB for branch process, and 1212 KB for vanilla. |
| Connection churn | Pooled mode is not yet competitive with process or pinned threads; this remains a known optimization target. |

Treat these numbers as development guidance, not a portability or production
benchmark. The important current signal is that pooled carriers now demonstrate
the intended memory and thread-count advantage for large quiet connection
populations, while hot-path speed and connection churn still need work.

Background and inspiration:

- [PostgreSQL wiki: Multithreading](https://wiki.postgresql.org/wiki/Multithreading)
- [PostgreSQL wiki: Signals](https://wiki.postgresql.org/wiki/Signals)
- [PGConf.dev 2025 Developer Unconference notes](https://wiki.postgresql.org/wiki/PGConf.dev_2025_Developer_Unconference)
- [Investigating Multithreaded PostgreSQL](https://www.youtube.com/watch?v=7BvLaRkaijc),
  Thomas Munro's PGConf.dev 2025 talk, attended by the branch author and a
  significant inspiration for this work.

Useful project documents:

- [Architecture](MULTITHREADED_ARCHITECTURE.md): target object model and
  north-star design.
- [Implementation plan](MULTITHREADED_PLAN.md): staged roadmap, validation
  gates, and phase boundaries.
- [Phase 13 plan](MULTITHREADED_PHASE13_PLAN.md): historical scheduler-aware
  wait boundary work.
- [Benchmark results](MULTITHREADED_BENCHMARKS.md): latest Phase 15 benchmark
  suite, workload definitions, TPS tables, and memory-footprint results.
- [Threading review](MULTITHREADED_THREADING_REVIEW.md): review of the branch
  direction, risks, and historical correctness blockers.
- [Agent guide](AGENTS.md): local development rules, validation defaults, and
  current working assumptions for this branch.

Original PostgreSQL README
==========================

PostgreSQL Database Management System
=====================================

This directory contains the source code distribution of the PostgreSQL
database management system.

PostgreSQL is an advanced object-relational database management system
that supports an extended subset of the SQL standard, including
transactions, foreign keys, subqueries, triggers, user-defined types
and functions.  This distribution also contains C language bindings.

Copyright and license information can be found in the file COPYRIGHT.

General documentation about this version of PostgreSQL can be found at
<https://www.postgresql.org/docs/devel/>.  In particular, information
about building PostgreSQL from the source code can be found at
<https://www.postgresql.org/docs/devel/installation.html>.

The latest version of this software, and related software, may be
obtained at <https://www.postgresql.org/download/>.  For more information
look at our web site located at <https://www.postgresql.org/>.
