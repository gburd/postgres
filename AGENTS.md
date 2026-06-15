# Multithreaded PostgreSQL Agent Guide

This repository is an experimental branch for making PostgreSQL capable of
running backend sessions in a multithreaded runtime. The branch is allowed to
be ambitious and is not currently optimized for upstream patch shape.

Implementation is underway. Keep the plan and architecture notes current as the
code evolves, and keep process-mode PostgreSQL working while adding threaded
runtime support.

## Mandatory Reading

- `MULTITHREADED_PLAN.md`: staged implementation plan, validation strategy,
  gates, and risk register.
- `MULTITHREADED_ARCHITECTURE.md`: desired end-state architecture.
- `MULTITHREADED_PHASE12_STATE.md`: current Phase 12 state-migration log and
  required lifecycle/preflight notes.
- `MULTITHREADED_THREADING_REVIEW.md`: Gate E2 blocker rationale.
- `MULTITHREADED_AGENT_PHASE12_GUIDE.md`: detailed Phase 12/Gate E2 workflow
  rules. Read before substantive Phase 12 implementation.
- `MULTITHREADED_AGENT_REFERENCE.md`: source orientation, local build/test
  notes, platform friction, and terminology.
- `MULTITHREADED_RUNTIME_LIFECYCLE.tsv`: checked runtime-root lifecycle
  manifest.
- `MULTITHREADED_RUNTIME_OWNERS.tsv`: checked legacy-symbol owner map.
- `refs/REFERENCES.md` and
  `refs/pgconf-2025-multithreading-transcript.md`: motivating reference
  material.

## Current Phase 12 Rule

Default Gate E2-Core ordering: lifecycle ergonomics/refactor first, then
threaded teardown, PMChild/thread synchronization, systematic GUC adoption,
startup-serialization narrowing/removal, and remaining object migration.

Gate E2-Core is the Phase 12 exit gate for the core threaded runtime. It is not
the bundled-extension completion gate. Phase 16 / Gate E2-Extensions owns
contrib-wide threaded support, bundled procedural languages beyond PL/pgSQL,
and the full custom/extension GUC matrix.

Milestone W is the short-path target before Gate E2-Core: a working core
threaded runtime. It requires threaded startup, normal SQL, PL/pgSQL,
process-only extension/background-worker rejection, core GUC semantics, clean
disconnect/abandoned/FATAL/terminate/reconnect teardown, no retained
`TopMemoryContext` warning in threaded TAP, and passing lifecycle/global scans.
It does not require contrib-wide threaded regression, bundled languages beyond
PL/pgSQL, every platform/test shim removal, or the full custom/extension GUC
matrix.

Use evidence-driven fixes. Runtime assertions, retained-root warnings,
lifecycle checks, raw lifetime scans, and threaded TAP failures should drive
the next migration. Treat `gmake check-global-lifetimes` as a guardrail and
triage input, not a standalone TODO list.

Use "defer with invariant" for any Phase 12 item intentionally left outside
Milestone W: name why it is safe for the working core runtime, name the runtime
assertion/log guard/lifecycle check/TAP failure that would catch the assumption
if wrong, and name the later phase or gate that owns completion.

## Lifecycle Preflight

Before every substantial Phase 12/Gate E2 implementation batch, record a
lifecycle preflight in `MULTITHREADED_PHASE12_STATE.md` before editing code:

```text
Lifecycle/preflight note:

- target:
- touched roots/buckets:
- owner source files:
- legacy symbols/accessors:
- repeated lifecycle operations:
- checked primitive decision:
- validation impact:
```

If the batch would repeat init/adopt/reset/destroy helper shapes, object-owned
context allocation, delete-and-null cleanup, list/hash cleanup, fallback
copy/adopt/reset, owner-map bookkeeping, or checker exceptions, add or reuse
checked lifecycle machinery first. Acceptable primitives include named
`PG_RUNTIME_*` actions, `PG_RUNTIME_DEFINE_*` helpers, bucket `.def` rows,
declarative owner/source tables, and `check_runtime_lifecycles.pl` rules.

Do not respond to lifecycle friction by slicing the same repeated manual work
smaller. Add the checked macro/table/checker primitive that lets a larger batch
move safely, then migrate state through it. Keep exceptional ownership,
ordering, and subsystem cleanup handwritten and owner-adjacent.

Do not retry wholesale thread-exit `TopMemoryContext` deletion as a narrow
cleanup. A prior probe caused follow-on backend failures, which points at
remaining process-global or insufficiently migrated catalog/cache pointers.
Before attempting root context reclamation again, identify the retained owners,
add any missing checked lifecycle primitive, migrate the coherent state group,
and rerun the threaded runtime TAP.

## Development Rules

- Keep documentation and code commits coherent. Prefer one conceptual change
  per commit.
- After each commit, push the current branch immediately unless explicitly told
  not to.
- Before editing core code, read the surrounding implementation and comments.
  PostgreSQL has many invariants documented only locally.
- Use static annotations and tools to classify globals before moving large
  amounts of state.
- Prefer larger coherent Phase 12 batches when ownership and validation surface
  match. Avoid one-variable commits unless the variable sits on a fragile
  lifecycle path.
- Keep `src/backend/utils/init/backend_runtime.c` focused on root runtime
  construction, current-pointer installation, process/thread symmetry, and
  top-level adoption/reset orchestration. Put owner-specific accessors and
  simple lifecycle helpers in fork-owned adjacent subsystem files.
- Add backend-runtime tests to the split object-family test files under
  `src/test/modules/test_backend_runtime`, not back into the old monolith.
- Before adding more handwritten lifecycle lists, use the checked bucket `.def`
  files and `PG_RUNTIME_DEFINE_*` helpers where they fit. Semantic cleanup
  stays handwritten near the owning subsystem.
- When local build/test friction repeats, update
  `MULTITHREADED_AGENT_REFERENCE.md` rather than growing this entry file.

## Working Assumptions

- Use Heikki Linnakangas's multithreading branch as reference material, not as
  a base to merge wholesale.
- Preserve multiprocess PostgreSQL as a supported backend model.
- The first native threading target is thread-per-session. The longer-term
  target is an explicit scheduler that can map sessions/executions to carriers.
- Thread-per-session for regular client backends is not the final normal-mode
  target. Normal threaded server mode should eventually run in-tree
  server-owned workers as threaded runtime-owned workers rather than forked
  subprocesses.
- Single-user mode, bootstrap mode, frontend command-line utilities,
  postmaster/control-plane process lifetime, and crash-escalation paths are
  deliberate process-lifetime exceptions.
- Do not overfit the design to WASM. Keep the main-loop and wait-boundary
  abstractions clean enough for a future host-driven runtime.
- Existing third-party C extensions may be process-backend-only. Existing
  third-party background workers may remain process-only or be rejected in
  threaded mode unless explicit worker-runtime metadata opts them in.
- In-tree modules and important bundled languages, especially PL/pgSQL, should
  have a plausible path to work in threaded mode.
