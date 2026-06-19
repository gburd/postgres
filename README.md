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
- a later scheduler that can run logical sessions or executions on a smaller
  pool of physical carriers.

The current implementation starts from PostgreSQL `REL_19_BETA1`. Phase 12 has
closed the scoped core thread-per-session state-migration gate. The active
direction is Phase 13: making waits scheduler-aware while keeping process mode
and thread-per-session fallback behavior healthy.

Important constraints:

- process-mode PostgreSQL must continue to work;
- arbitrary third-party C extensions are not assumed to be thread-safe;
- thread-per-session is a milestone, not the final scheduler design;
- pooled scheduling comes after explicit wait boundaries exist;
- correctness and lifecycle ownership come before broad performance claims.

Current status:

- Phase 12 / Gate E2-Core is closed for the scoped core runtime.
- Phase 13 is active, focused on scheduler-aware wait boundaries.
- Process mode remains supported.
- Thread-per-session mode runs regular client backends and normal SQL paths.
- Core backend/session/connection/execution/carrier state has explicit runtime
  ownership sufficient for startup, command execution, PL/pgSQL, core GUC
  behavior, logical interrupts, cancellation, termination, teardown, reconnect,
  worker handoff, and the current in-tree worker runtime scope.
- The branch is still experimental. It is not a production-ready PostgreSQL
  server, does not yet provide pooled carrier scheduling, and does not claim
  contrib-wide threaded extension support.
- Phase 16 owns broader extension hardening, bundled procedural languages
  beyond PL/pgSQL, and the full custom/extension GUC matrix.

Current validation baseline:

- `gmake check`
- `gmake check-threaded`
- `gmake check-threaded-workers`
- `gmake check-threaded-world-core`
- `gmake check-runtime-lifecycles`
- `gmake check-global-lifetimes`
- `git diff --check`

These Gate E2-Core validation targets are currently green in the local WSL
development tree as of June 19, 2026. Re-run the full set before claiming a new
release-quality checkpoint, because this branch intentionally keeps changing
runtime ownership boundaries.

Performance guidance:

Performance work is currently measured against vanilla PostgreSQL 19 beta 1
using vanilla `pgbench` as the client. The reusable local runner is
`src/tools/benchmark/mtpg_pgbench_matrix.pl`; it compares vanilla process mode,
this branch in process mode, this branch in thread-per-session mode, and
configurable pooled protocol carrier lanes such as `branch_pool_4`,
`branch_pool_8`, and `branch_pool_16`. Use `--restart-per-workload` when a
profile needs to isolate one workload per fresh postmaster, for example while
debugging pooled-mode lifecycle failures between sequential client batches.

The most recent five-workload local profile for tiny read-only `pgbench`
workloads showed the branch around parity with vanilla on this machine:

| Workload | Branch process / vanilla | Branch threaded / vanilla |
| --- | ---: | ---: |
| `builtin_select_simple` | 0.976 | 1.014 |
| `builtin_select_prepared` | 0.983 | 0.980 |
| `select1_prepared` | 1.003 | 1.029 |
| `bench_one_prepared` | 1.018 | 1.006 |
| `kv_read_prepared` | 0.973 | 0.991 |

Treat these numbers as development guidance, not a portability or production
benchmark. The important current signal is that the scoped thread-per-session
runtime is close enough to vanilla on these small read-only workloads to move
the next optimization effort into Phase 13 wait-boundary work. Future
performance work should continue to compare all branch lanes, because some
remaining overhead is branch-wide rather than threaded-only.

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
- [Phase 13 plan](MULTITHREADED_PHASE13_PLAN.md): current scheduler-aware wait
  boundary work.
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
