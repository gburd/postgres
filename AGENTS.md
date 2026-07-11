# Multithreaded PostgreSQL Agent Guide

This repository is an experimental branch for making PostgreSQL capable of
running backend sessions in a multithreaded runtime. Keep process-mode
PostgreSQL working while advancing the threaded runtime.

## Mandatory Reading

- `plan_docs/MULTITHREADED_PLAN.md`: active staged plan, validation strategy,
  gates, and risk register.
- `plan_docs/MULTITHREADED_ARCHITECTURE.md`: desired end-state architecture.
- `plan_docs/MULTITHREADED_AGENT_REFERENCE.md`: source orientation, build/test
  notes, platform friction, and terminology.
- `MULTITHREADED_RUNTIME_LIFECYCLE.tsv`: checked runtime-root lifecycle
  manifest.
- `MULTITHREADED_RUNTIME_OWNERS.tsv`: checked legacy-symbol owner map.

Conditional references:

- `plan_docs/MULTITHREADED_PHASE12_STATE.md`: archival Phase 12 implementation
  ledger and Gate E2-Core evidence. Read it when investigating Phase 12
  regressions or updating closeout evidence.
- `plan_docs/MULTITHREADED_AGENT_PHASE12_GUIDE.md`: detailed Phase 12 workflow
  rules. Read it before reopening Phase 12 state/lifecycle migration work.
- `plan_docs/MULTITHREADED_THREADING_REVIEW.md`: historical Gate E2 blocker
  rationale.
- `refs/REFERENCES.md` and
  `refs/pgconf-2025-multithreading-transcript.md`: motivating background
  material.

## Current Direction

Phases 12-15 are closed (core thread-per-session runtime, wait-boundary
scheduler awareness, the real pooled protocol scheduler).  Pooled protocol mode
is now the DEFAULT under multithreaded=on (auto-sized carrier pool; explicit 0
keeps thread-per-session).  Phase 16 (bundled extensions/PLs/GUC matrix) and
Phase 17/18 are the active surface.

The north star has been sharpened (2026-07-11): the goal is to FUSE the runtime
with libxtc deeply enough that the threaded branch matches and then beats the
fork model, not merely to keep libxtc as a swappable substrate.  Session-5 perf
put threaded at ~65 % of process; closing that gap means adopting libxtc's OTP
behaviours (xtc_svr, xtc_orc, xtc_pool, xtc_fsm, crash-aware registry, xtc_pg,
xtc_xproc, xtc_credit) as first-class runtime building blocks and deduping
Latch/LWLock/CV/AIO onto xtc primitives -- one behaviour at a time, each
A/B-measured and kept only if neutral-or-better on check-threaded-pooled.  See
the "North star" section in plan_docs/MULTITHREADED_PLAN.md and the promoted
Phase 18.  This is aggressive fusion, NOT a blind cutover: process mode stays
supported and the process-lifetime exceptions stand.

Do not reopen broad Phase 12 migration, refactor, or documentation churn unless
runtime evidence proves a core blocker. Valid evidence includes threaded TAP
failures, retained-root warnings, lifecycle/global-lifetime checker failures,
crashes, hangs, or process-mode regressions.

Phase 16 / Gate E2-Extensions owns contrib-wide threaded support, bundled
procedural languages beyond PL/pgSQL, and the full custom/extension GUC
matrix. Use "defer with invariant" for intentional exclusions: explain why the
scope is safe now, which guard would catch a wrong assumption, and which later
phase/gate owns completion.

## Development Rules

- Keep documentation and code commits coherent. Prefer one conceptual change
  per commit.
- After each commit, push the current branch immediately unless explicitly told
  not to.
- Before editing core code, read the surrounding implementation and comments.
  PostgreSQL has many invariants documented only locally.
- Keep process mode green.
- Use static annotations and tools to classify globals before moving state.
- Prefer larger coherent batches when ownership and validation surface match.
  Avoid one-variable commits unless the variable sits on a fragile lifecycle
  path.
- Keep `src/backend/utils/init/backend_runtime.c` focused on root runtime
  construction, current-pointer installation, process/thread symmetry, and
  top-level adoption/reset orchestration. Put owner-specific accessors and
  simple lifecycle helpers in owner-adjacent subsystem files.
- Add backend-runtime tests to the split object-family test files under
  `src/test/modules/test_backend_runtime`, not back into the old monolith.
- Before adding handwritten lifecycle lists, use the checked bucket `.def`
  files and `PG_RUNTIME_DEFINE_*` helpers where they fit. Semantic cleanup
  stays handwritten near the owning subsystem.
- When local build/test friction repeats, update
  `plan_docs/MULTITHREADED_AGENT_REFERENCE.md` rather than growing this file.

## Validation Defaults

- For doc-only changes, run `git diff --check`.
- For ordinary code changes, keep `gmake check` and `gmake check-threaded`
  green.
- For runtime-root, lifecycle, GUC, teardown, worker, or wait-boundary changes,
  also run the focused target that matches the touched surface and the relevant
  guardrails:
  `gmake check-threaded-workers`, `gmake check-threaded-world-core`,
  `gmake check-runtime-lifecycles`, and/or
  `gmake check-global-lifetimes`.

## Working Assumptions

- Track plain upstream postgres/master as the branch base (switched from
  heikki/master 2026-07-11, once they had converged).  Cherry-pick or
  incorporate Heikki Linnakangas's multithreading work as needed during future
  upstream rebases rather than tracking his branch as the base.
- Preserve multiprocess PostgreSQL as a supported backend model.
- The first native threading target was thread-per-session; the default is now
  the pooled protocol scheduler.  The longer-term target is an explicit
  scheduler that maps sessions/executions to carriers, built on and fused with
  libxtc (adopt its OTP behaviours; dedup PG primitives onto xtc where it is a
  measured net win) -- aiming to beat the fork model, not just match it.
- Thread-per-session for regular client backends is not the final normal-mode
  target. Normal threaded server mode should eventually run in-tree
  server-owned workers as threaded runtime-owned workers rather than forked
  subprocesses.
- Single-user mode, bootstrap mode, frontend command-line utilities,
  postmaster/control-plane process lifetime, and crash-escalation paths are
  deliberate process-lifetime exceptions.  Threaded genuine crashes fail-stop
  the whole process (fast, no core-dump hang -- RLIMIT_CORE dropped at carrier
  start, PG_XTC_ALLOW_CORE=1 to override); restart is external-supervision
  territory (systemd), with a future in-tree xtc_xproc watchdog as the path to
  in-tree restart.
- Do not overfit the design to WASM. Keep the main-loop and wait-boundary
  abstractions clean enough for a future host-driven runtime.
- Existing third-party C extensions may be process-backend-only. Existing
  third-party background workers may remain process-only or be rejected in
  threaded mode unless explicit worker-runtime metadata opts them in.
- In-tree modules and important bundled languages, especially PL/pgSQL, should
  have a plausible path to work in threaded mode.

See .agent-steering-domains.md for domain-specific steering (local).
