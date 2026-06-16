# Phase 12 Overnight Stretch Goal

Archived status: this was the execution plan for the Phase 12 stretch/closeout
session. Keep it as provenance for the refactor, `check-threaded-world-core`,
and Gate E2-Core closeout decisions. `MULTITHREADED_PLAN.md` now contains the
concise active closeout summary, and Phase 13 is the current forward plan.

This is an ambitious execution plan for pushing the branch from the current
Milestone W-style core regression success toward a credible Gate E2-Core
closeout. It is intentionally larger than a single implementation slice, but
each stage must land in coherent, validated commits.

Current baseline before starting this stretch:

- `gmake check` passes all 245 core regression tests in process mode.
- `gmake check-threaded` passes all 245 core regression tests.
- `gmake check-threaded-workers` passes all 245 core regression tests with
  `io_method = worker` and `summarize_wal = on`.
- Prepared transactions are enabled in threaded pg_regress.
- Dynamic parallel workers are admitted in threaded mode.
- The `select_parallel_0.out` leader-only threaded expectation is removed.
- The temporary threaded GUC mutex has been narrowed for simple session-owned
  built-in `SET` and `SHOW` paths, but remains for ambiguous startup, hook,
  custom, extension, and process-global-backed paths.

## Stretch Goal

Make Phase 12 measurably closer to complete by doing four linked things:

1. Refactor the branch-added massive runtime bridge and backend-runtime test
   files so ownership is clear and future Gate E2 work is tractable.
2. Add and pass a broader threaded validation target based on a limited
   `check-world`/near-world scope, without pulling Phase 16 extension
   completeness into Phase 12.
3. Move into the next Gate E2-Core blocker after the refactor and new tests,
   using runtime evidence to choose between teardown, PMChild/thread
   synchronization, GUC adoption, startup serialization, or retained memory
   ownership.
4. Push as far as practical toward Phase 12 completion while keeping the new
   and existing validation targets green.

## Non-Negotiable Rules

- Push after every commit.
- Do not redefine success around the easiest passing subset. The stretch goal
  remains Phase 12 / Gate E2-Core progress.
- Keep process-mode PostgreSQL green. `gmake check` is the control group.
- Before substantial Phase 12 code changes, add a lifecycle/preflight note to
  `MULTITHREADED_PHASE12_STATE.md`.
- If lifecycle boilerplate repeats, add or reuse checked lifecycle helpers,
  bucket `.def` rows, or checker rules before moving more state.
- Prefer owner-adjacent runtime bridge files over growing
  `src/backend/utils/init/backend_runtime.c`.
- Do not chase contrib-wide threaded support, bundled procedural languages
  beyond PL/pgSQL, or the full custom/extension GUC matrix. Those remain
  Phase 16 / Gate E2-Extensions unless a core Gate E2 invariant proves
  otherwise.
- Use "defer with invariant" for every intentionally excluded suite or
  feature: say why it is safe, what guard/test would catch a wrong assumption,
  and which later phase/gate owns it.

## Stage 1: Refactor Runtime Bridge And Test Monoliths

Target: reduce the branch-added monoliths without changing behavior.

Primary files to inspect:

- `src/backend/utils/init/backend_runtime.c`
- `src/include/utils/backend_runtime.h`
- `src/backend/utils/init/backend_runtime_teardown.c`
- `src/backend/utils/misc/backend_runtime_guc.c`
- `src/test/modules/test_backend_runtime/test_backend_runtime.c`

Match the branch's existing pattern:

- Put owner-specific runtime bridge code in the owning subsystem directory as
  `backend_runtime_<subsystem>.c`.
- Keep root construction, current-pointer installation, process/thread
  symmetry, and top-level lifecycle orchestration in
  `src/backend/utils/init/backend_runtime.c`.
- Keep public compatibility accessor declarations in
  `src/include/utils/backend_runtime.h`.
- Use `src/backend/utils/init/backend_runtime_internal.h` for backend-private
  current-bucket helpers needed by owner-adjacent runtime bridge files.
- Add new object files to the owning subsystem `Makefile`.
- If a new owner-adjacent file owns lifecycle/owner-map state, add it to
  `src/tools/runtime_lifecycle/check_runtime_lifecycles.pl` source coverage
  and update `MULTITHREADED_RUNTIME_OWNERS.tsv`.
- Keep semantic cleanup and ordering-sensitive teardown owner-adjacent.

Suggested refactor order:

1. Move pure accessor blocks out of `backend_runtime.c` first. These should be
   functions that simply return a pointer into `CurrentPgBackend`,
   `CurrentPgSession`, `CurrentPgConnection`, `CurrentPgExecution`, or
   fallback-selected bucket state.
2. Move clearly owned helper blocks next, but only after a preflight confirms
   existing checked lifecycle primitives cover the init/adopt/reset pattern.
3. Split `test_backend_runtime.c` into object-family test files under
   `src/test/modules/test_backend_runtime`, preserving the same extension and
   regression surface.
4. Avoid broad include churn. Each new file should include only `postgres.h`,
   local subsystem headers it actually needs, and the runtime internal header.

Good first split candidates:

- xact/transaction execution accessors into an access/transam-owned runtime
  bridge file;
- memory-context execution accessors into a utils/mmgr-owned runtime bridge
  file;
- resource-owner execution accessors into a utils/resowner-owned runtime
  bridge file;
- SPI/executor execution accessors into an executor-owned runtime bridge file;
- regex accessors into the regex subsystem;
- remaining session generic accessors into smaller owner-adjacent files rather
  than further growing `backend_runtime_session.c`.

Validation for each refactor commit:

- touched-object build for moved files and `backend_runtime.o`;
- `gmake check-runtime-lifecycles`;
- `gmake check-global-lifetimes`;
- focused `test_backend_runtime` control if test files or runtime bridge
  ownership changed;
- `git diff --check`.

After several refactor commits, run:

- `gmake check`;
- `gmake check-threaded`;
- `gmake check-threaded-workers`.

## Stage 2: Add A Broader Threaded World-Core Target

Target: create a repeatable broad threaded validation target beyond the 245
core regression tests, while keeping Phase 16 work explicitly out of scope.

Working name:

```sh
gmake check-threaded-world-core
```

The exact target shape should be discovered by running near-world commands and
classifying failures, but the intended stable target should include:

- full core regression in threaded mode with worker settings enabled, or a
  dependency on the existing `check-threaded-workers` target;
- PL/pgSQL threaded coverage;
- `src/test/modules/test_backend_runtime` process-mode control and direct
  threaded TAP;
- focused GUC, reconnect, terminate, FATAL, abandoned-client, cancellation,
  worker handoff, and cleanup coverage;
- isolation tests where they exercise core Gate E2 behavior and are practical
  under threaded temp configuration;
- lifecycle/global scans.

Discovery commands to try, sequentially rather than in parallel with other
`tmp_install`-recreating checks:

```sh
gmake check-world TEMP_CONFIG=$PWD/src/test/regress/threaded_workers.conf
```

If full `check-world` is too noisy on this platform, run subtrees directly
with `TEMP_CONFIG=$PWD/src/test/regress/threaded_workers.conf`, starting with:

- `src/test/regress`
- `src/pl/plpgsql`
- `src/test/isolation`
- `src/test/modules/test_backend_runtime`
- selected `src/bin` and `src/test` TAP suites whose failures are not just
  platform dynamic-library issues.

Required classification for every excluded failure:

- `include`: belongs in `check-threaded-world-core` now.
- `fix-core`: blocks Gate E2-Core and needs a runtime fix.
- `defer-phase16`: extension/language/custom-GUC completeness.
- `defer-platform`: local macOS/tooling issue, with a rerun recipe if useful.
- `defer-gate-f`: scheduler/pooled-carrier behavior.

Each defer entry must name the invariant or guard that keeps it safe.

Before calling this stage done:

- remove the remaining core expected-output shim if practical:
  `src/test/regress/expected/guc_0.out`;
- document the new target in `MULTITHREADED_AGENT_REFERENCE.md`;
- record the target and exclusions in `MULTITHREADED_PHASE12_STATE.md`;
- validate the new target passes from a clean enough temp install.

## Stage 3: Move To The Next Gate E2-Core Blocker

After the refactor and world-core target are in place, choose the next blocker
from runtime evidence rather than static suspicion.

Gate E2-Core blocker order remains:

1. threaded teardown and retained memory/resource cleanup;
2. PMChild/thread synchronization and signal/reap ownership;
3. systematic core GUC adoption/rebind/reset/default behavior;
4. startup serialization narrowing/removal;
5. remaining object migration required by runtime evidence, retained-root
   warnings, lifecycle checks, or TAP failures.

Selection rule:

- If the new world-core target or threaded TAP finds a crash, leak warning,
  retained `TopMemoryContext` accounting warning, teardown corruption, or
  reconnect-after-failure problem, fix that first.
- If failures are mostly test harness or broad extension completeness, defer
  with invariant and move to the highest remaining Gate E2 blocker.
- If lifecycle boilerplate slows the selected blocker, add the missing checked
  primitive first.

Minimum validation for each blocker commit:

- touched-object builds;
- focused runtime/TAP test proving the failure is fixed;
- `gmake check-runtime-lifecycles`;
- `gmake check-global-lifetimes`;
- `git diff --check`.

Run the full suite set after a coherent blocker batch:

- `gmake check`;
- `gmake check-threaded`;
- `gmake check-threaded-workers`;
- `gmake check-threaded-world-core` if it exists by then.

## Stage 4: Phase 12 Closeout Attempt

Only attempt to mark Phase 12 / Gate E2-Core done after an explicit audit.

Evidence required:

- `gmake check-runtime-lifecycles` passes.
- `gmake check-global-lifetimes` passes.
- full process-mode `gmake check` passes.
- full `gmake check-threaded` passes.
- full `gmake check-threaded-workers` passes.
- the new `check-threaded-world-core` target passes.
- direct threaded runtime TAP passes and its log guard has no crash,
  corruption, or retained `TopMemoryContext` accounting warnings.
- process-mode backend-runtime regression passes.
- PL/pgSQL threaded coverage passes.
- focused GUC, teardown, cancellation, termination, reconnect, abandoned
  client, FATAL, and worker handoff smokes pass or have explicit
  defer-with-invariant entries.
- process-only extensions and background workers are still rejected in
  threaded mode with clear errors.

Do not close Phase 12 if:

- carrier `TopMemoryContext` cleanup is still known to corrupt later carriers;
- PMChild/thread reaping has unsynchronized pointer ownership;
- normal post-bootstrap SQL execution still depends on a broad startup/GUC
  serialization gate without a precise invariant;
- process-mode `gmake check` regresses;
- the new threaded world-core target only passes by hiding core behavior behind
  expected-output shims.

## Suggested Commit Boundaries

1. Refactor one runtime bridge owner family.
2. Refactor another runtime bridge owner family.
3. Split backend-runtime test monolith.
4. Add `check-threaded-world-core` plumbing and documentation.
5. Fix the first new core threaded failure exposed by the target.
6. Add defer-with-invariant documentation for excluded non-core suites.
7. Address the next Gate E2 blocker selected by runtime evidence.
8. Final closeout/audit commit if the evidence supports it.

Each commit must be pushed before continuing.

## Copy-Paste Goal Prompt

```text
We are in /Users/samwillis/Code/multithreaded-postgres on branch multithreaded.

Use MULTITHREADED_PHASE12_STRETCH_GOAL.md as the plan for this session. Also
read AGENTS.md, MULTITHREADED_PLAN.md, MULTITHREADED_PHASE12_STATE.md,
MULTITHREADED_THREADING_REVIEW.md, MULTITHREADED_AGENT_PHASE12_GUIDE.md, and
MULTITHREADED_AGENT_REFERENCE.md before coding.

Stretch goal:

1. Refactor the branch-added massive runtime bridge files and the
   test_backend_runtime test monolith to match the existing owner-adjacent
   backend_runtime_<subsystem>.c pattern. Keep backend_runtime.c focused on
   root runtime construction, current-pointer installation, process/thread
   symmetry, and top-level lifecycle orchestration.
2. Add a broader threaded validation target based on a limited
   check-world/near-world scope, tentatively gmake check-threaded-world-core.
   It should go beyond the 245 core regression tests while keeping Phase 16
   extension/language/custom-GUC completeness out of Phase 12. Classify every
   excluded suite with defer-with-invariant.
3. Move to the next Gate E2-Core blocker using runtime evidence from the new
   and existing tests: teardown/retained memory, PMChild/thread
   synchronization, systematic core GUC adoption, startup serialization, or
   remaining object migration.
4. Push as far as practical toward Phase 12 / Gate E2-Core completion while
   ensuring the existing and newly added tests remain green.

Current required baselines to preserve:

- gmake check
- gmake check-threaded
- gmake check-threaded-workers
- gmake check-runtime-lifecycles
- gmake check-global-lifetimes
- git diff --check

Working rules:

- Push after every commit.
- Before substantial Phase 12 code changes, add a lifecycle/preflight note to
  MULTITHREADED_PHASE12_STATE.md.
- If lifecycle boilerplate repeats, add or reuse checked lifecycle helpers,
  bucket .def rows, or checker rules first.
- Keep process mode green.
- Do not chase contrib-wide threaded support, bundled procedural languages
  beyond PL/pgSQL, or the full custom/extension GUC matrix unless runtime
  evidence proves a Gate E2-Core dependency.
- Use defer-with-invariant for anything intentionally outside this stretch:
  why safe, what guard catches it if wrong, and which later phase/gate owns it.

Start by inspecting git status, confirming the current baseline, inventorying
branch-added backend_runtime_*.c files and the test_backend_runtime monolith,
then begin the first mechanical refactor slice.
```
