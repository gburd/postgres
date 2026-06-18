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
