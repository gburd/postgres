# Multithreaded Agent Phase 12 Workflow Reference

This file holds the detailed Phase 12/Gate E2 working rules split out of
`AGENTS.md`. Read it before substantive Phase 12 implementation work,
especially lifecycle, teardown, GUC, PMChild, startup-gate, or state-migration
slices.

## Extracted Active Notes

This repository is an experimental branch for making PostgreSQL capable of
running backend sessions in a multithreaded runtime. The branch is allowed to
be ambitious and is not currently optimized for upstream patch shape.

Implementation is now underway. Keep the plan and architecture notes current as
the code evolves.

Current Phase 12/Gate E2 default: move state in larger coherent batches, but
do not let lifecycle boilerplate grow by hand. Before every substantial Gate
E2 slice, record a lifecycle preflight in `MULTITHREADED_PHASE12_STATE.md`
that names the touched root/bucket rows, legacy state owners, repeated
lifecycle operations, and the checked primitive being reused or added.

If the slice would repeat init/adopt/reset/destroy helper shapes, object-owned
context allocation, delete-and-null cleanup, list/hash cleanup, fallback
copy/adopt/reset, owner-map bookkeeping, or checker exceptions, add or reuse
the checked lifecycle machinery first. Acceptable primitives include named
`PG_RUNTIME_*` actions, `PG_RUNTIME_DEFINE_*` helpers, bucket `.def` rows,
declarative owner/source tables, and `check_runtime_lifecycles.pl` rules.
Only continue with handwritten helpers when the preflight explains the
operations have different ordering, ownership, or subsystem-specific
semantics.

Treat lifecycle friction as implementation work, not documentation debt. If a
larger Phase 12 batch feels slow because lifecycle bookkeeping is repetitive,
the next coding task is the missing macro/table/checker primitive; then move
the larger batch through that checked path. This applies especially before
the remaining Gate E2 blockers: threaded teardown, PMChild/thread
synchronization, startup-gate narrowing/removal, systematic GUC adoption, and
remaining object migration.

When deciding how to make faster progress, first ask whether lifecycle
bookkeeping can be made macroable or table-driven. Prefer adding a small
checked lifecycle primitive that lets a larger batch move safely over splitting
the same repeated init/adopt/reset/destroy work into smaller handwritten
patches.

If repetition is discovered after a Phase 12 slice has already started, pause
the state movement and add the missing checked helper/action/table/checker
support before continuing. Do not finish a boilerplate-heavy migration by hand
and leave the lifecycle simplification as later cleanup; the simplification is
part of the same Gate E2 implementation work.

Quick lifecycle-ergonomics checklist for each substantial Phase 12 batch:

- Can two or more fields use the same init/adopt/reset/destroy shape?
- Can a `PG_RUNTIME_*` action or `PG_RUNTIME_DEFINE_*` helper remove repeated
  boilerplate?
- Can a bucket `.def` row or owner/source manifest make the update
  mechanically checkable?
- Can `check_runtime_lifecycles.pl` enforce the rule so future agents cannot
  forget it?
- Would a small macro, X-macro/table entry, declarative manifest rule, or
  checker extension let the next batch move more state safely in one go?

If the answer to any of these is yes, implement that small lifecycle primitive
first, then use it for the larger migration in the same slice.

Standing instruction for resumed Phase 12 work: do not wait for another user
prompt before applying this rule. When lifecycle bookkeeping is the limiting
factor, inspect `src/backend/utils/init/backend_runtime_lifecycle.h`, the
runtime bucket `.def` files, `MULTITHREADED_RUNTIME_LIFECYCLE.tsv`,
`MULTITHREADED_RUNTIME_OWNERS.tsv`, and
`src/tools/runtime_lifecycle/check_runtime_lifecycles.pl` first. If a small
macro, X-macro/table row, owner-map rule, or checker extension would let the
next migration move a larger coherent group safely, make that lifecycle
primitive the first code change in the slice and then migrate state through it.

Before starting the next substantive Gate E2 coding slice, explicitly record
the answer in `MULTITHREADED_PHASE12_STATE.md` under that slice's preflight.
Do not start by moving another set of globals unless the preflight names the
checked lifecycle machinery being reused or the new helper/checker/table rule
that will be added first.

Do this proactively. When lifecycle mechanics are the drag on a Phase 12/Gate
E2 batch, the right next step is usually a checked macro, X-macro/table row, or
checker rule, not a smaller manual batch. Commit the helper and the state moved
through it as one coherent slice unless the helper is useful and complete on
its own.

Do not retry wholesale thread-exit `TopMemoryContext` deletion as a narrow
cleanup. A Phase 12 probe that deleted the thread execution top context after
bucket reset caused follow-on backend failures (`unsupported byval length: 0`
and `could not find tuple for opclass 112`), which points at remaining
process-global or insufficiently migrated catalog/cache pointers. Before
attempting root context reclamation again, first use the lifecycle preflight
rule: identify the still-retained cache/state owners, add any missing checked
lifecycle macro/action/table/checker primitive, migrate the larger coherent
state group, then rerun the threaded runtime TAP.

## Active Phase 12 Gate E2-Core Rule

- Default ordering for the next substantial Gate E2-Core work: lifecycle
  ergonomics/refactor first, then the remaining teardown, PMChild/thread
  synchronization, systematic GUC adoption, startup-serialization narrowing,
  and large state-migration batches. Treat lifecycle friction as implementation
  work, not documentation debt.
- Gate E2-Core is the Phase 12 exit gate for the core threaded runtime. It is
  not the bundled-extension completion gate. Do not continue sweeping contrib
  or hard extensions unless a teardown probe, raw lifetime scan, retained
  `TopMemoryContext` warning, or threaded TAP failure shows they directly block
  core backend/session/connection/execution cleanup.
- Phase 16 / Gate E2-Extensions owns contrib-wide threaded support, bundled
  procedural languages beyond PL/pgSQL, and the full custom/extension GUC
  matrix. PL/pgSQL and `test_backend_runtime_threaded` remain in Phase 12 as
  the proof points for safe in-tree module loading, GUC prefix reservation,
  and safe rejection of process-only modules.
- Milestone W is the short-path target before Gate E2-Core: a working core
  threaded runtime. It requires threaded startup, normal SQL, PL/pgSQL,
  process-only extension/background-worker rejection, core GUC semantics,
  clean disconnect/abandoned/FATAL/terminate/reconnect teardown, no retained
  `TopMemoryContext` warning in threaded TAP, and passing lifecycle/global
  scans. It does not require contrib-wide threaded regression, bundled
  languages beyond PL/pgSQL, every platform/test shim removal, or the full
  custom/extension GUC matrix.
- Use evidence-driven fixes for remaining Phase 12 work. Runtime assertions,
  retained-root warnings, lifecycle checks, raw lifetime scans, and threaded
  TAP failures should drive the next migration. Do not migrate every suspected
  owner proactively just because it appears in a broad static search.
- Treat `gmake check-global-lifetimes` as a guardrail and triage input, not a
  standalone TODO list. Classified legacy owners may remain for Milestone W
  when they do not block core threaded startup, teardown, GUC behavior,
  PL/pgSQL, or scheduler-readiness evidence.
- Use "defer with invariant" for any Phase 12 item intentionally left outside
  Milestone W: name why it is safe for the working core runtime, name the
  runtime assertion/log guard/lifecycle check/TAP failure that would catch the
  assumption if wrong, and name the later phase or gate that owns completion.
- Before the next repetitive Phase 12/Gate E2-Core lifecycle batch, do a short
  lifecycle-ergonomics preflight. If the batch would add two or more similar
  init/adopt/reset/destroy helpers, first add or extend a checked lifecycle
  primitive: a `PG_RUNTIME_*` bucket action, `PG_RUNTIME_DEFINE_*` helper,
  bucket `.def` rule, or `check_runtime_lifecycles.pl` validation.
- Treat that preflight as an implementation step, not a note to self. The
  expected answer is either "the existing checked macro/table/checker path is
  sufficient" with the exact mechanism named, or "extend the lifecycle
  framework first" with the new macro, action, `.def` pattern, or checker rule
  landed before the state migration.
- When Phase 12 progress feels slow because lifecycle bookkeeping is
  repetitive, do not just split the migration into smaller manual batches.
  First consider whether a macro, X-macro/bucket table, owner-map rule, or
  checker extension would let a larger batch move safely. If it would, add that
  checked primitive and use it in the same implementation slice.
- Record the preflight decision in
  [MULTITHREADED_PHASE12_STATE.md](MULTITHREADED_PHASE12_STATE.md) before
  editing code. The note must either name the existing bucket rows/macros/
  checker rules being reused, or name the new primitive added first.
- Keep exceptional ownership, ordering, and subsystem cleanup handwritten and
  owner-adjacent. Use macros/tables only for repeated lifecycle mechanics so
  large batches move faster without weakening the manifest gate.
- Apply that rule before each remaining Gate E2-Core blocker, not only before raw
  global migration. If teardown, PMChild synchronization, startup-gate cleanup,
  or owner-map hardening would require repeated bookkeeping, first land the
  small checked macro/table/checker primitive and then do the larger slice
  through that path.
- Do not leave this as an abstract guideline. The next time a Phase 12 batch
  repeats object-owned allocation-context setup, delete-and-null cleanup,
  list/hash cleanup, fallback copy-then-reset, or reset-through-initializer
  code, add the missing checked lifecycle primitive first. The likely next
  useful primitive is an allocation-context helper/table rule that covers
  create-on-demand context ownership plus checked close-time deletion.
- Use `PG_RUNTIME_DELETE_MEMORY_CONTEXT_AND_RESET(context, init_expr)` when a
  bucket reset only deletes an owned memory context and then restores
  constructor defaults. Add new checked `PG_RUNTIME_*` actions the same way
  when another repeated teardown/reset shape appears.
- Use `PgRuntimeGetOwnedMemoryContextWithSizes(context, name, ...)` for
  repeated create-on-demand object-owned memory-context setup where allocation
  sizes matter, and `PgRuntimeGetOwnedMemoryContext(context, name)` for the
  common small-context case. Do not open-code another `if NULL,
  AllocSetContextCreate(TopMemoryContext, ...)` helper branch without first
  explaining why the shared helper does not fit.
- Operational reminder for future agents: when planning a larger Phase 12
  batch, start by deciding whether lifecycle macros, bucket `.def` rows,
  declarative tables, or checker rules would make the batch simpler. If yes,
  land that lifecycle-framework improvement before moving the globals or
  teardown code. The intended speed-up is larger object-migration batches with
  less handwritten lifecycle bookkeeping, not smaller manual slices.
- When a batch feels slow because lifecycle setup, reset, destroy, or manifest
  updates are repetitive, do not respond by slicing the work smaller. Add or
  reuse the checked macro/action/table/checker path that lets the larger batch
  move safely.
- Before picking the next group of globals, explicitly look for lifecycle
  friction first. If the next group would require repeated manual lifecycle
  code, the next implementation task is the lifecycle helper itself: add a
  checked `PG_RUNTIME_*` action, `PG_RUNTIME_DEFINE_*` macro, declarative
  bucket-table rule, owner-map metadata, or checker validation, then migrate
  the larger group through that mechanism.
- On resumption, do this helper-first check before choosing the next concrete
  migration target. Quickly inspect the existing lifecycle helper surface
  (`backend_runtime_lifecycle.h`, bucket `.def` files, the owner map, and
  `check_runtime_lifecycles.pl`). If two or more remaining owners would use
  the same create/adopt/reset/destroy shape, bias toward landing the checked
  macro/table/checker primitive first rather than adding another manual helper
  pair.
- The current concrete lifecycle-simplification candidate is owned memory
  context state: a bucket field that is created on demand, may be copied from
  the early fallback bucket, and is deleted/nullified during close-state reset.
  If another Phase 12 batch adds multiple owned context parents or repeated
  context-delete reset code, first add a checked action/table/helper for that
  shape and then move the larger batch through it.

## Phase 12 Lifecycle Preflight Checklist

Before starting any substantial Phase 12/Gate E2 implementation batch, answer
these questions in `MULTITHREADED_PHASE12_STATE.md`:

- Which runtime root, bucket rows, legacy globals, and owner source files are
  being touched?
- Which lifecycle operations repeat: init, early adoption, fallback reset,
  close-time reset, destructor calls, memory-context deletion, list/hash
  cleanup, pointer clearing, or owner-map updates?
- Does an existing checked primitive cover the repetition? Name the exact
  `PG_RUNTIME_*` action, `PG_RUNTIME_DEFINE_*` helper, bucket `.def` rule,
  manifest/checker rule, or owner-map validation.
- If no existing primitive covers the repetition, add the missing checked
  primitive first, then use it for the larger migration batch.
- If the cleanup is semantic or ordering-sensitive, keep that part handwritten
  in the owner file, but still use checked primitives for any surrounding
  clerical lifecycle mechanics.

Do not begin a large state migration with a vague "no new primitive needed"
claim. The preflight note must explain why existing checked lifecycle
infrastructure is enough, or it must point at the lifecycle-framework commit
that made it enough.

Use this preflight note shape in `MULTITHREADED_PHASE12_STATE.md` before the
code changes:

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

The `checked primitive decision` entry must name the existing macro/action/
table/checker path being reused, or name the new checked primitive that will be
landed before moving state. If the work is deliberately handwritten because it
is semantic or ordering-sensitive, say that explicitly and still identify any
surrounding clerical lifecycle mechanics covered by existing primitives.

When resuming Phase 12 after an interruption, compaction, or branch review,
run this checklist before choosing the next globals or teardown target. The
default next step is not "move another symbol"; it is to decide whether the
next coherent batch needs a small lifecycle macro/action/table/checker
improvement first, land that improvement if needed, and then move the larger
batch through the checked path.

Before the next non-trivial Gate E2 migration, actively look for a
macroable/checker-enforceable lifecycle pattern rather than waiting until the
batch becomes painful. The first useful candidates to consider are repeated
object-owned memory-context creation/reset, repeated delete-and-null teardown,
parallel init/adopt/reset helper bodies, and owner-map/source-list
bookkeeping. If any candidate has two or more call sites in the planned batch,
make the helper/checker improvement part of that same implementation slice.

## Development Rules For This Branch

- Keep documentation and code commits coherent. Prefer one conceptual change
  per commit.
- After each commit, push the current branch immediately unless the user has
  explicitly asked not to push.
- Before editing core code, read the surrounding implementation and current
  comments. PostgreSQL has many invariants that are documented only locally.
- Keep process-mode behavior working after each implementation phase.
- Use static annotations and tools to classify globals before moving large
  amounts of state.
- For Phase 12 state migration, prefer larger coherent batches when the state
  has the same owner and validation surface. Avoid one-variable commits unless
  the variable sits on a fragile lifecycle path where a narrow proof is needed.
- Current Phase 12/Gate E2 workflow: before starting the next repetitive
  state-migration or teardown batch, first decide whether lifecycle mechanics
  can be simplified. If the batch would add two or more similar
  init/adopt/reset/destroy helpers, add the checked macro/action/table/checker
  primitive first, then move the larger batch through that path. Good
  candidates include `PG_RUNTIME_DEFINE_*` helpers, named `PG_RUNTIME_*`
  bucket actions, bucket `.def` row patterns, and
  `check_runtime_lifecycles.pl` validation. Record the preflight result in
  `MULTITHREADED_PHASE12_STATE.md`.
- Keep `src/backend/utils/init/backend_runtime.c` focused on root runtime
  construction, current-pointer installation, process/thread symmetry, and
  top-level adoption/reset orchestration. New domain-specific accessors and
  simple lifecycle helpers should live in fork-owned adjacent subsystem files,
  with `check-runtime-lifecycles` taught to scan those files.
- Before continuing with additional Gate E2 state migration or starting Phase
  13 scheduler-aware wait work, complete the documented maintainability
  refactor: split owner-specific runtime bridge code out of
  `backend_runtime.c` where practical, make every manifest-referenced split
  source part of the default lifecycle checker input, and split
  `src/test/modules/test_backend_runtime` into smaller
  object/lifetime-focused test sources while preserving the same extension,
  SQL, expected output, and TAP surface.
- The first Gate E2 maintainability split is in place. Keep adding
  owner-specific runtime accessors to adjacent `backend_runtime_*.c` files and
  backend-runtime tests to the object-family test files instead of rebuilding
  the old monoliths.
- Before pushing deeper into Gate E2 teardown, add a small lifecycle
  framework: root-object bucket definition files, X-macros, or an equivalent
  checked manifest for `PgBackend`, `PgSession`, `PgConnection`, and
  `PgExecution`. Use it as the single source of truth for constructor,
  early-adoption, and reset/destroy call lists. Keep semantic cleanup
  functions handwritten and owner-adjacent; generate only repetitive coverage
  and call-list mechanics. Extend `check_runtime_lifecycles.pl` to validate
  the bucket definitions against `MULTITHREADED_RUNTIME_LIFECYCLE.tsv` and to
  reject unintentional process/thread lifecycle asymmetry.
- Treat that lifecycle framework as the next Gate E2 implementation slice, not
  optional polish. Prefer checked `.def` bucket files included from the
  top-level runtime constructors/adoption/reset orchestration before adding
  more handwritten init/adopt/reset lists.
- Make the lifecycle framework reduce manual work. Add small macros,
  templates, or declarative rule columns for routine copied-scalar,
  zero-reset, whole-bucket copy/adopt, and destructor-call cases, so future
  agents can move larger batches without maintaining several call lists by
  hand. Keep exceptional ordering and semantic cleanup handwritten near the
  owning subsystem.
- Before each large Phase 12 migration or Gate E2 teardown batch, do a
  lifecycle-ergonomics preflight and record the result in
  `MULTITHREADED_PHASE12_STATE.md`. If the batch would add repetitive
  init/adopt/reset/destroy boilerplate, first add the small checked lifecycle
  action, `PG_RUNTIME_DEFINE_*` helper, bucket `.def` rule, or checker
  validation that makes the batch declarative. If the existing checked
  mechanism is sufficient, say which bucket rows/macros/checker rules are
  being reused before editing code.
- The first lifecycle framework slice uses
  `src/backend/utils/init/backend_runtime_*_buckets.def`. The checker validates
  one bucket-definition row for every `PgCarrier`, `PgBackend`, `PgSession`,
  `PgConnection`, and `PgExecution` field. Carrier, backend, connection, and
  execution constructor orchestration includes those rows directly. Backend,
  connection, and execution closed-reset orchestration also includes the rows;
  carrier has no closed-backend reset path yet because carrier lifetime is
  outside closed logical backend reset. Session constructor/adoption includes
  the rows. Session closed reset uses the separate ordered
  `backend_runtime_session_reset_buckets.def` because its teardown order is
  intentionally different from early-adoption order; keep semantic cleanup in
  handwritten helper functions and add ordered reset rows for new non-noop
  session reset buckets.
- For routine lifecycle helper functions, use the
  `PG_RUNTIME_DEFINE_*` helpers in
  `src/backend/utils/init/backend_runtime_internal.h` where they fit. The
  lifecycle checker recognizes those macro-defined functions, so use them for
  ordinary zero-init, whole-bucket early adoption, initialized-bucket adoption,
  and initialized-bucket adoption with a distinct early-reset function. Do not
  hide exceptional destructor ordering, pointer rebasing, list-head repair, or
  ownership assertions behind these macros.
- When a Phase 12 migration starts adding repeated lifecycle boilerplate,
  improve the checked lifecycle framework before continuing. Prefer adding a
  helper macro, `.def` bucket row, or declarative lifecycle rule over
  maintaining another handwritten constructor/adoption/reset list. This should
  make larger coherent migrations easier while keeping ownership and teardown
  semantics explicit.
- If lifecycle bookkeeping is slowing progress, treat that friction as a
  design signal. Batch related buckets by root object or subsystem, add the
  missing helper macro/table rule/checker validation first, and then migrate the
  batch through that mechanism instead of making several narrow one-off edits.
- Before taking another large Phase 12 migration batch, explicitly consider
  whether the lifecycle work can be simplified first. If the batch would add
  repetitive init/adopt/reset/destroy code, add or extend checked helper macros,
  `.def` rows, or declarative lifecycle rules before moving the state. The goal
  is faster large-batch migration with the same manifest-checked discipline,
  not more manual bookkeeping.
- Treat this as a required lifecycle-ergonomics checkpoint, not a preference:
  before coding a boilerplate-heavy Phase 12 batch, decide whether the existing
  `PG_RUNTIME_DEFINE_*` macros, bucket `.def` files, and checker rules are
  enough. If not, extend that framework first and record the chosen pattern in
  `MULTITHREADED_PHASE12_STATE.md` so future agents follow the same path.
- Practical rule: if a planned Phase 12 or Gate E2 slice would write the same
  init/adopt/reset/destroy shape twice, stop and make that shape declarative
  first. Treat checked lifecycle macros/actions as the default acceleration
  tool for large batches: prefer a checked `PG_RUNTIME_*` action,
  `PG_RUNTIME_DEFINE_*` helper, bucket `.def` row pattern, or checker rule over
  another bespoke helper pair. The next likely candidates are object-owned
  allocation contexts, delete-and-null memory contexts, list/hash cleanup, and
  copy/adopt-then-reset fallback buckets. For reset-through-initializer
  buckets, use the checked `PG_RUNTIME_RESET_THROUGH_INITIALIZER(init_expr)`
  action instead of raw initializer calls in reset columns.
- Keep improving lifecycle ergonomics when the pattern becomes repetitive. Good
  candidates are checked action names in the bucket `.def` files for common
  cases such as zero-init, zero-reset, copy/adopt, copy/adopt-with-reinit,
  reset-through-initializer, and memory-context/list/hash destruction; small
  `PG_RUNTIME_DEFINE_*` wrappers for those actions; and checker rules that
  reject unclassified `(void) 0` lifecycle cells on buckets with pointers,
  lists, memory contexts, or owned resources. Add these framework improvements
  before migrating another large batch that would otherwise duplicate the same
  helper code by hand.
- Use this concrete preflight before a large Phase 12 migration:
  identify the target root object and bucket rows, list the repeated lifecycle
  operations the batch would need, decide whether the existing macros/`.def`
  rows/checker rules cover them, add a reusable checked helper first if they do
  not, then migrate the batch through that mechanism. Record the decision in
  the Phase 12 state log even when the existing framework is sufficient.
- This lifecycle preflight is mandatory before the next code batch that moves
  object-owned globals or adds reset/destroy behavior. The preflight note must
  say one of: "existing lifecycle mechanism is sufficient" with the specific
  bucket rows/macros named, or "framework extended first" with the new macro,
  `.def` rule, or checker validation named. Do not start by writing another
  handwritten helper list if a small checked macro/table rule would cover the
  repeated pattern.
- When adding another runtime root object or moving more fields into an
  existing root, extend the checked lifecycle framework first if the existing
  macros and `.def` rows do not make the lifecycle obvious. The default should
  be one manifest row plus one checked bucket-definition row per field, with
  helper macros covering routine init/adopt/reset cases and handwritten code
  reserved for real ownership semantics.
- The next time a Phase 12 batch would add several similar lifecycle helpers,
  implement a checked lifecycle action vocabulary before moving the state. The
  intended direction is a small set of named actions in the bucket `.def` rows
  for zero-init, zero-reset, scalar copy/adopt, whole-bucket copy/adopt,
  copy/adopt-with-reinit, reset-through-initializer, and explicit
  owner-adjacent destroy calls. Teach `check_runtime_lifecycles.pl` to verify
  those actions against `MULTITHREADED_RUNTIME_LIFECYCLE.tsv` and reject
  unexplained no-op lifecycle cells for buckets that own pointers, lists,
  memory contexts, sockets, or other close-time resources.
- The first checked lifecycle action is `PG_RUNTIME_NOOP`. Use it in
  `backend_runtime_*_buckets.def` instead of bare `(void) 0`; the lifecycle
  checker rejects anonymous no-op cells and unknown `PG_RUNTIME_*` action
  names. Extend this vocabulary before adding another family of repetitive
  lifecycle helper bodies.
- `PG_RUNTIME_DELETE_MEMORY_CONTEXT(context)` is the checked action for the
  routine memory-context delete-and-null pattern. Use it in bucket `.def` rows
  and local reset helpers when the context has no extra semantic teardown.
  Leave ordered cleanup, conditional ownership, and companion pointer/list/hash
  reset handwritten near the owning subsystem.
- The next lifecycle-framework simplification should cover the patterns now
  recurring in Gate E2: object-owned allocation contexts, delete-and-null
  memory-context teardown, free/reset list heads, clear-pointer-slot reset,
  copy/adopt-then-reset-fallback, and reset-through-initializer. If a planned
  Phase 12 batch needs two or more parallel helper bodies for these patterns,
  add the checked `PG_RUNTIME_*` action, `PG_RUNTIME_DEFINE_*` helper, bucket
  `.def` rule, and checker validation before moving the state.
- Treat the object-owned allocation-context pattern as the first concrete
  lifecycle-ergonomics target. The next time a batch repeats create-on-demand
  context accessors plus delete-and-null reset helpers, add a reusable checked
  primitive for that pattern before moving more state through one-off helpers.
- If the lifecycle process itself feels slow or repetitive, stop and improve
  the checked lifecycle vocabulary before continuing the migration. The
  preferred fix is a small named action, helper macro, table row, or checker
  rule that makes the next batch easier and keeps the manifest as the source
  of truth; do not paper over the friction with another manual helper list.
- Current Phase 12 standing instruction: before the next boilerplate-heavy
  migration batch, explicitly decide whether lifecycle helper macros,
  checked action names, or declarative bucket rules would make the batch
  simpler. If yes, land that lifecycle-framework improvement first, then move
  the globals through the checked path.
- Lifecycle macro decision rule: if a planned Phase 12/Gate E2 batch would
  add two or more structurally similar lifecycle helpers or repeat a known
  lifecycle pattern across multiple buckets, the default next step is to add a
  reusable checked `PG_RUNTIME_*` action, `PG_RUNTIME_DEFINE_*` macro, bucket
  `.def` rule, or checker validation first. Only skip that framework step when
  the repetition is accidental or the cleanup order/ownership semantics are
  genuinely different, and record that decision in
  `MULTITHREADED_PHASE12_STATE.md`.
- Treat lifecycle-helper repetition as implementation work, not documentation
  debt. If a Phase 12/Gate E2 slice would add two or more similar
  init/adopt/reset/destroy helpers, first add or extend the checked
  lifecycle mechanism: a `PG_RUNTIME_DEFINE_*` helper, named `PG_RUNTIME_*`
  bucket action, `.def` row pattern, or `check_runtime_lifecycles.pl`
  validation. Only continue with handwritten helpers when the cleanup has
  real ordering or ownership semantics that need owner-adjacent code.
- Near-term lifecycle ergonomics TODO: when the next Phase 12 batch repeats an
  object-owned allocation-context, delete-and-null, list/hash reset, pointer
  clear, copy/adopt-then-reset, or reset-through-initializer pattern, stop and
  add the checked action/macro/checker support first. The intended deliverable
  is a named `PG_RUNTIME_*` action or `PG_RUNTIME_DEFINE_*` helper that lets
  future batches update the manifest and bucket `.def` row instead of copying
  another helper body.
- Do this lifecycle-ergonomics preflight before any further large Gate E2-Core
  teardown or state-migration batch, including PMChild/thread-backend cleanup
  work if it starts adding repeated reset/destroy glue. The expected outcome is
  either a short state-log note naming the existing checked mechanism being
  reused, or a documentation/code slice that adds the missing macro, named
  action, `.def` row, or checker rule before the behavior change.
- Session GUC direct-variable rebinding in `src/backend/utils/misc/guc.c`
  is table-driven by `threaded_session_guc_rebinds[]`. Add new migrated
  built-in direct-pointer GUCs to that table instead of extending
  `RebindSessionGUCVariablePointers()` with handwritten `find_option()` blocks.
  `ValidateSessionGUCVariableRebinds()` and
  `test_session_guc_rebind_table_matches_registry()` verify that the table
  matches the live GUC registry. In Phase 12, keep custom/extension GUC
  testing to the minimal thread-compatible test module and PL/pgSQL/runtime
  metadata paths needed for Gate E2-Core; broader custom/extension GUC
  semantics are Phase 16 / Gate E2-Extensions work.
- Do not attempt thread launch until the thread-safety floor is in place:
  backend-local globals must not be shared plain process globals, backend exit
  must not terminate the whole runtime, and timeout/interrupt delivery must be
  per logical backend.
- Before leaving Phase 12 or starting scheduler-aware wait work, run
  `gmake check-global-lifetimes` as part of Gate E2. A new mutable global must
  be annotated with an explicit `PG_GLOBAL_*` owner or deliberately accepted in
  `src/tools/global_lifetime/global_lifetime_baseline.tsv`.
- Raw `PG_THREAD_LOCAL PG_GLOBAL_BACKEND`, `PG_GLOBAL_SESSION`,
  `PG_GLOBAL_CONNECTION`, and `PG_GLOBAL_EXECUTION` declarations should now be
  confined to `src/backend/utils/init/backend_runtime.c` early-fallback
  storage. Raw `PG_GLOBAL_CARRIER` declarations should be limited to the
  runtime current pointers and narrowly documented process-context flags such
  as `IsUnderPostmaster`; wait-event self-pipe/signalfd state,
  `stack_base_ptr`, backend-thread launch state, and threaded GUC mutex depth
  live in `PgCarrier`. If a new in-tree module needs cached shared registry
  data, prefer `PG_GLOBAL_RUNTIME`; if it needs backend/session/execution or
  carrier state, add an explicit runtime-object bucket instead.
- Windows-only Phase 12 edits made from this macOS checkout must be marked as
  best-effort until a Windows build validates them. The current
  `pgwin32_noblock` bridge is covered by shared connection-object tests here,
  but `src/backend/port/win32/socket.c` still needs Windows compile coverage.
- Before leaving Phase 12, perform the Gate E2 object-lifecycle audit. Every
  carrier/backend/session/connection/execution state bucket needs a documented
  initializer, early-adoption behavior or proof that early adoption is
  impossible, reset/destroy behavior, owner/lifetime, and copy/adoption rule
  for pointer, list, memory-context, socket, hash-table, and opaque-pointer
  fields. Keep `MULTITHREADED_RUNTIME_LIFECYCLE.tsv` synchronized with
  `src/include/utils/backend_runtime.h`, and run
  `gmake check-runtime-lifecycles` after adding, renaming, or removing
  `PgCarrier`, `PgBackend`, `PgSession`, `PgConnection`, or `PgExecution`
  fields. Manual process/thread init/adopt asymmetries must be centralized or
  explicitly justified. The checker also verifies the manifest's runtime
  lifecycle function references, owner-map references, and the required
  process/thread constructor and top-level adoption calls; update the checker
  deliberately if the object construction shape changes.
- When closing a lifecycle row for a memory-context or compatibility bridge,
  make the reset order explicit in code and docs. If cleanup only clears
  pointer slots while existing transaction/main-loop cleanup still owns the
  pointed-to contexts, say that in `MULTITHREADED_RUNTIME_LIFECYCLE.tsv`
  rather than implying the broader `TopMemoryContext` split is solved.
- Backend early fallback adoption is centralized in
  `PgBackendAdoptEarlyState()`. Do not add a backend bucket adoption helper to
  only the process or thread install path; add it to that shared helper or
  document why the asymmetry is intentional. Pointer/list-bearing buckets need
  an explicit copy/adopt rule, and copied empty list heads must be asserted and
  reinitialized in the destination object rather than preserving fallback
  self-pointers.
- Session, connection, and execution early fallback adoption are centralized in
  `PgSessionAdoptEarlyState()`, `PgConnectionAdoptEarlyState()`, and
  `PgExecutionAdoptEarlyState()`. Add newly migrated buckets to those helpers
  rather than directly to
  `InitializePgProcessRuntime()` or `InstallPgThreadBackendRuntimeState()`.
  Threaded connection adoption must preserve the live `Port` supplied during
  `InitializePgThreadBackendRuntimeState()` by passing it as
  `PgConnectionAdoptEarlyState()`'s `preserved_port`.
- Phase 12 miscellaneous execution scratch state now lives under
  `PgExecution`: `PgExecutionAnalyzeState.array_extra_data`,
  `PgExecutionRegexState`, `PgExecutionValgrindState`, and
  `PgExecutionSnapBuildState`. These buckets use whole-bucket copy/adopt plus
  zero reset; their pointer fields are borrowed or opaque and do not own lists,
  memory contexts, hash tables, sockets, or heap allocations. After changing
  these runtime structs or accessors, use the installed-header/layout clean
  rebuild path before trusting TAP or extension results.
- Catalog transaction/execution scratch state now lives under
  `PgExecutionCatalogState`: uncommitted enum type/value hash pointers,
  REINDEX suppression state, and pending storage delete/sync state. The
  catalog files keep their historic local variable names as macros over
  runtime accessors. If a local struct field has the same name as one of those
  macros, rename the field; this was required for `SerializedReindexState`.
  The actual hash/list storage is still owned by existing transaction cleanup
  paths such as enum, reindex, and smgr end-of-transaction cleanup.
- Catalog cache execution scratch state now lives under
  `PgExecutionCatalogCacheState`: catcache's create-in-progress stack pointer,
  relcache's `RelationBuildDesc()` in-progress list pointer/length/capacity,
  relcache's EOXact relation OID list/length/overflow flag, and relcache's
  EOXact tupledesc array pointer/index/capacity. The runtime object owns the
  slots and inline OID array; pointed-to catcache stack entries, relcache
  in-progress storage, and tupledesc arrays remain owned by existing stack,
  `CacheMemoryContext`, and relcache EOXact cleanup paths. After changing this
  bridge, rebuild `backend_runtime.o`, `catcache.o`, `relcache.o`, and
  `test_backend_runtime.o`, then run `gmake check-runtime-lifecycles` and
  `gmake check-global-lifetimes`.
- LISTEN/NOTIFY transaction scratch state now lives under
  `PgExecutionAsyncState`: pending LISTEN/UNLISTEN actions, pending NOTIFY
  lists, pending listen hash state, queue head snapshots used by
  `SignalBackends()`, and its preallocated workspace arrays. `async.c` keeps
  the historic local names as macros over runtime accessors. The pending lists
  remain owned by transaction memory contexts and async transaction cleanup;
  the signal workspace arrays are still allocated under `TopMemoryContext`
  until the broader backend destructor model is closed.
- Session teardown now explicitly drops prepared statements, destroys the
  prepared-query hash, frees leftover `ON COMMIT` actions, and destroys any
  remaining async local-channel hash. Keep that cleanup after `shmem_exit()`
  and `on_proc_exit` callbacks; async shared listener cleanup still belongs to
  the existing proc-exit callback path.
- Text-search parser, dictionary, and configuration caches now live under
  `PgSessionTextSearchState` with the `default_text_search_config` value and
  OID cache. The reset path owns parser/config hash destruction, dictionary
  private memory-context deletion, config map-array frees, and last-used
  pointer clearing. After changing this bridge, rebuild `backend_runtime.o`,
  `ts_cache.o`, and `test_backend_runtime.o`, then run
  `gmake check-runtime-lifecycles` and `gmake check-global-lifetimes`.
- ACL role-membership caches now live under `PgSessionUserIdentityState`.
  `acl.c` keeps `cached_role`, `cached_roles`, and `cached_db_hash` as
  file-local macros over the current session object. The copied membership
  lists are allocated in `TopMemoryContext` by existing ACL code and freed by
  `PgSessionResetClosedState()`. After changing this bridge, rebuild `acl.o`,
  `backend_runtime.o`, and `test_backend_runtime.o`, then run
  `gmake check-runtime-lifecycles` and `gmake check-global-lifetimes`.
- The fmgr external C-function lookup hash now lives under
  `PgSessionFunctionManagerState`. `fmgr.c` keeps `CFuncHash` as a file-local
  macro over `PgCurrentCFuncHashRef()`. `PgSessionResetClosedState()` destroys
  the hash; dynamic library handles and `Pg_finfo_record` metadata remain
  runtime/dynamic-loader owned. After changing this bridge, rebuild
  `fmgr.o`, `backend_runtime.o`, and `test_backend_runtime.o`, then run
  `gmake check-runtime-lifecycles` and `gmake check-global-lifetimes`.
- Syscache, relcache, and relsync invalidation callback registries now live
  under `PgSessionInvalidationCallbackState`. `inval.c` keeps the historic
  registry names as file-local macros over `PgCurrentInvalidationCallbackState()`.
  `PgSessionResetClosedState()` clears callback registrations after dependent
  session caches have been destroyed. After changing this bridge, rebuild
  `inval.o`, `backend_runtime.o`, and `test_backend_runtime.o`, then run
  `gmake check-runtime-lifecycles` and `gmake check-global-lifetimes`.
- Catalog lookup cache roots for attribute options, relfilenumber mapping,
  tablespace options, event triggers, ruleutils SPI plans, and the ICU
  converter now live under `PgSessionCatalogLookupState`. The bucket owns the
  root slots and reset closes/destroys the roots that can be safely reclaimed.
  Active backend teardown now deletes the session-owned `CacheMemoryContext`
  after dependent roots have been cleared, switching to `TopMemoryContext`
  first if that cache context is current. Full carrier `TopMemoryContext`
  reclamation remains a separate Gate E2 ownership audit. After changing this
  bridge, rebuild `backend_runtime.o`, `attoptcache.o`,
  `relfilenumbermap.o`, `spccache.o`, `evtcache.o`, `ruleutils.o`,
  `pg_locale_icu.o`, and `test_backend_runtime.o`, then run
  `gmake check-runtime-lifecycles` and `gmake check-global-lifetimes`.
- PL/pgSQL's custom-GUC, compile, namespace, plugin, simple-expression, and
  cast-cache session state now lives behind an opaque private pointer in
  `PgSessionExtensionModuleState`. PL/pgSQL registers a session reset callback
  so closed-session reset can release its private roots before
  `dynamic_library_context` is deleted. After changing this bridge, rebuild
  `backend_runtime.o`, PL/pgSQL objects, and `test_backend_runtime.o`; clean
  and reinstall PL/pgSQL into `tmp_install` before threaded TAP.
- Transaction cleanup slots now live under
  `PgExecutionTransactionCleanupState`: large-object descriptor cleanup slots,
  the transaction temporary-file cleanup flag, the pgstat subtransaction stack,
  and RI fast-path batch-cache state. The runtime object owns these slots and
  scalar flags, but the pointed-to storage remains owned by existing
  large-object, temporary-file, pgstat, and RI transaction/subtransaction
  cleanup paths. Add future execution cleanup buckets through
  `PgExecutionAdoptEarlyState()` and update
  `MULTITHREADED_RUNTIME_LIFECYCLE.tsv`; after changing this bridge, run
  touched-object builds plus `gmake check-runtime-lifecycles` and
  `gmake check-global-lifetimes` before trusting TAP.
- Execution error and replication/apply scratch state now lives under
  `PgExecution`: `PgExecutionErrorState` owns the `elog.c` error-data stack,
  recursion depth, saved timestamp cache, and formatted log-time buffer;
  `PgExecutionReplicationScratchState` owns the event-trigger query-state
  pointer, replication-origin transaction state, logical apply error-context
  stack, apply message context, and logical streaming context. The moved
  pointer slots are borrowed from existing error, event-trigger, and logical
  apply cleanup paths; replication-origin state is copied scalar state. After
  changing this bridge, rebuild touched logical replication, event-trigger, and
  error-reporting objects, then run `gmake check-runtime-lifecycles` and
  `gmake check-global-lifetimes` before trusting TAP.
- `AuxProcessResourceOwner` is now routed through `PgBackend` via
  `PgCurrentAuxProcessResourceOwnerRef()` and the `AuxProcessResourceOwner`
  lvalue macro. After changing `src/include/utils/resowner.h` or this backend
  runtime bridge, clean and rebuild backend objects before trusting link or TAP
  results; stale objects can still reference the removed
  `_AuxProcessResourceOwner` symbol.
- `MyProc` is now routed through `PgBackend` via `PgCurrentMyProcRef()` and
  the `MyProc` lvalue macro. After changing `src/include/storage/proc.h` or
  this backend runtime bridge, clean and rebuild backend objects and any
  extension modules under test before trusting link or TAP results; stale
  objects can still reference the removed `_MyProc` symbol. At minimum, clean
  and reinstall PL/pgSQL and `src/test/modules/test_backend_runtime` before
  rerunning their tests after a `MyProc` bridge change.
- `MyProcNumber` and `ParallelLeaderProcNumber` are now routed through
  `PgBackend` via `PgCurrentMyProcNumberRef()`,
  `PgCurrentParallelLeaderProcNumberRef()`, and the existing lvalue names in
  `src/include/storage/procnumber.h`. After changing that header or this
  backend runtime bridge, clean and rebuild backend objects and any extension
  modules under test before trusting link or TAP results; stale objects can
  still reference the removed `_MyProcNumber` or
  `_ParallelLeaderProcNumber` symbols, or miss the new accessor symbols. At
  minimum, clean and reinstall PL/pgSQL and
  `src/test/modules/test_backend_runtime` before testing.
- `MyBEEntry` is now routed through `PgBackend` via
  `PgCurrentMyBEEntryRef()` and the existing lvalue name in
  `src/include/utils/backend_status.h`. After changing that header or this
  backend runtime bridge, clean and rebuild backend objects and any extension
  modules under test before trusting link or TAP results; stale objects can
  still reference the removed `_MyBEEntry` symbol, or miss the new accessor
  symbol. At minimum, clean and reinstall
  `src/test/modules/test_backend_runtime` before testing.
- `MyBgworkerEntry` is now routed through `PgBackend` via
  `PgCurrentMyBgworkerEntryRef()` and the lvalue macro in
  `src/include/postmaster/bgworker.h`. After changing that header or this
  backend runtime bridge, clean and rebuild backend objects and any extension
  modules under test before trusting link or TAP results; stale objects can
  still reference the removed `_MyBgworkerEntry` symbol, or miss the new
  accessor symbol. At minimum, clean and reinstall
  `src/test/modules/test_backend_runtime`, `src/test/modules/worker_spi`,
  `src/test/modules/test_shm_mq`, and any worker modules under test.
- `ConfigReloadPending`, `ShutdownRequestPending`, `WakeupStopPending`,
  `AutoVacLauncherPending`, and `CheckpointerShutdownXLOGPending` are now
  fields in `PgBackendPendingInterruptState`, exposed through compatibility
  macros in `src/include/miscadmin.h`; their old exported TLS symbols were
  removed from `src/backend/postmaster/interrupt.c` and
  `src/backend/postmaster/checkpointer.c`. After changing this bridge, clean
  and rebuild backend objects and extension modules that include
  `postmaster/interrupt.h` or `miscadmin.h`; stale modules can still reference
  removed `_ConfigReloadPending`, `_ShutdownRequestPending`,
  `_WakeupStopPending`, `_AutoVacLauncherPending`, or
  `_CheckpointerShutdownXLOGPending` symbols. At minimum, clean and reinstall
  PL/pgSQL, `src/test/modules/test_backend_runtime`, worker modules, and
  contrib modules under test before validating.
- nbtree, GIN, GiST, and SP-GiST WAL redo `opCtx` memory contexts now live in
  `PgBackendXLogState` through source-local compatibility macros. After
  changing the XLog state bridge or these redo files, run touched-object
  builds for the affected AM redo objects and `backend_runtime.o`, then clean
  rebuild/install before trusting runtime tests.
- Allocation-set freelists and the memory-context logging reentrancy guard now
  live in `PgBackendMemoryManagerState`. After changing this bridge or
  `src/backend/utils/mmgr/aset.c`/`mcxt.c`, run touched-object builds for
  `backend_runtime.o`, `aset.o`, `mcxt.o`, and `test_backend_runtime.o`, then
  use a clean backend rebuild/install before runtime validation. The
  global-lifetime scan drops by one for this slice because two raw globals are
  replaced by one early-backend fallback bucket.
- Wait-event reporting storage now lives in `PgBackendWaitState`, and
  `my_wait_event_info` is a compatibility macro over the current backend wait
  state except in the standalone `S_LOCK_TEST` build. The shared-invalidation
  local transaction ID counter now lives in `PgBackendIPCState`. After changing
  this bridge or `src/include/utils/wait_event.h`, run touched-object builds
  for `backend_runtime.o`, `wait_event.o`, `sinvaladt.o`, and
  `test_backend_runtime.o`, then clean/rebuild backend and `src/common` before
  full runtime validation; stale `src/common` server objects can still
  reference removed wait-event symbols.
- `DoingCommandRead` now lives in `PgSessionLoopState`, while tcop command
  option/timing state lives in `PgBackendCommandState` and elog formatted
  start-time/line-number/PID cache state lives in `PgBackendLogState`. After
  changing this bridge, run touched-object builds for `backend_runtime.o`,
  `postgres.o`, `elog.o`, and `test_backend_runtime.o`, then use the normal
  backend plus `src/common` clean rebuild path before runtime validation.
- `pgStatLocal` now lives in `PgBackendPgStatPendingState.local`; the
  `pgStatLocal` identifier is a compatibility macro over
  `PgCurrentPgStatLocalState()`. This currently makes `backend_runtime.h`
  include `pgstat_internal.h` so the pgstat-local object stays embedded
  instead of being lazily allocated. After changing this bridge, run
  touched-object builds for `backend_runtime.o`, `pgstat.o`, representative
  `src/backend/utils/activity` objects, and `test_backend_runtime.o`, then use
  the normal backend plus `src/common` clean rebuild path before runtime
  validation. Include `gmake check-global-lifetimes`, contrib build, PL/pgSQL
  rebuild/install, `test_backend_runtime check`, and direct threaded TAP.
- Computed-goto expression interpreter lookup state now lives in
  `PgBackendExprInterpState`; `dispatch_table` and `reverse_dispatch_table`
  in `execExprInterp.c` are compatibility macros over the current backend.
  The reverse table stores integer opcodes in a fixed
  `PG_BACKEND_EXPR_INTERP_MAX_OPS` array and `execExprInterp.c` asserts that
  `EEOP_LAST` fits. After changing this bridge, run touched-object builds for
  `execExprInterp.o`, `backend_runtime.o`, and `test_backend_runtime.o`, then
  use the normal backend plus `src/common` clean rebuild path before runtime
  validation. Include `gmake check-global-lifetimes`, contrib build, PL/pgSQL
  rebuild/install, `test_backend_runtime check`, and direct threaded TAP.
- `proc_exit_inprogress` and `shmem_exit_inprogress` are now fields in
  `PgBackendExitState`, exposed through compatibility macros in
  `src/include/storage/ipc.h`; the old exported TLS definitions were removed
  from `src/backend/storage/ipc/ipc.c`. After changing this bridge, clean and
  rebuild backend objects and extension modules that include `storage/ipc.h`;
  stale modules can still reference the removed `_proc_exit_inprogress` or
  `_shmem_exit_inprogress` symbols, or miss the
  `PgCurrentBackendExitStateRef()` accessor.
- `PendingBgWriterStats`, `PendingCheckpointerStats`,
  `PendingIOStats`, `pending_SLRUStats`, `PendingLockStats`,
  `PendingBackendStats`, `pgStatXactCommit`, `pgStatXactRollback`,
  `pgStatBlockReadTime`, `pgStatBlockWriteTime`, `pgStatActiveTime`,
  `pgStatTransactionIdleTime`, `total_func_time`, `prevWalUsage`,
  `prevBackendWalUsage`, `pgstat_report_fixed`, `pgStatForceNextFlush`,
  `force_stats_snapshot_clear`, `pgstat_is_initialized`,
  `pgstat_is_shutdown`, `pgStatPendingContext`, `pgStatPending`, and the
  related `have_*stats`/`backend_has_iostats` booleans are now fields in
  `PgBackendPgStatPendingState`, exposed through compatibility macros in
  `src/include/pgstat.h` and private macros/accessors in
  `src/backend/utils/activity/pgstat.c` and
  `src/include/utils/pgstat_internal.h`; the old exported/static TLS
  definitions were removed from pgstat implementation files.
  `PGSTAT_SLRU_NUM_ELEMENTS` is public only to size the runtime SLRU
  pending-state array and is asserted against `slru_names[]` in
  `src/include/utils/pgstat_internal.h`. The pending-entry list bridge assumes
  no early pgstat pending entries exist before backend-runtime adoption; copied
  non-empty `dlist_head` values would still point at the old list head, so the
  adoption path asserts that invariant and reinitializes the adopted head.
  After changing this bridge, clean and rebuild backend objects and extension
  modules that include `pgstat.h`; stale objects can still reference removed
  pgstat symbols or miss the new accessor symbols. At minimum, clean and
  reinstall PL/pgSQL,
  `src/test/modules/test_backend_runtime`, and contrib/test modules under
  pgstat coverage before validating.
- `pgBufferUsage`, `save_pgBufferUsage`, `pgWalUsage`, and
  `save_pgWalUsage` are now fields in `PgBackendInstrumentationState`,
  exposed through compatibility macros in `src/include/executor/instrument.h`;
  the old exported/static TLS definitions were removed from
  `src/backend/executor/instrument.c`. After changing this bridge, clean and
  rebuild backend objects and extension modules that include `instrument.h`;
  stale objects can still reference removed `_pgBufferUsage` or
  `_pgWalUsage` symbols, or miss the new accessor symbols. At minimum, clean
  and reinstall PL/pgSQL, `src/test/modules/test_backend_runtime`, and contrib
  before validating.
- Pending file-sync state (`pendingOps`, `pendingUnlinks`,
  `pendingOpsCxt`, `sync_cycle_ctr`, `checkpoint_cycle_ctr`, and
  `sync_in_progress`), storage-manager relation state (`SMgrRelationHash` and
  `unpinned_relns`), magnetic-disk storage-manager context (`MdCxt`), and
  file-descriptor/VFD state (`VfdCache`, `SizeVfdCache`, `nfile`,
  `temporary_files_allowed`, `numAllocatedDescs`, `maxAllocatedDescs`,
  `allocatedDescs`, and `numExternalFDs`) are now fields in
  `PgBackendStorageState`, exposed through private compatibility macros in
  `src/backend/storage/sync/sync.c`, `src/backend/storage/smgr/smgr.c`,
  `src/backend/storage/smgr/md.c`, and `src/backend/storage/file/fd.c`.
  The smgr adoption path asserts that no early smgr relation hash/list exists
  before backend-runtime adoption; copied non-empty `dlist_head` values would
  still point at the old list head. Threaded backend startup can reserve file
  descriptors before installing the backend runtime, so
  `InstallPgThreadBackendRuntimeState()` must adopt early storage state into
  the thread-backed `PgBackend`; losing that fallback fd state can make the
  threaded TAP postmaster exit immediately after launching worker threads.
  Closed-backend reset now routes the `storage` bucket through
  `PgBackendResetStorageClosedState()`: fd.c reclaims its private VFD and
  AllocateDesc arrays, while the owner-adjacent runtime file bridge destroys
  pending-sync/smgr hash and list state, deletes pending-sync and md memory
  contexts, and reinitializes the bucket. Keep normal temp-file/resource-owner
  semantics in the existing transaction/proc-exit paths; the runtime reset is
  the retained-object cleanup pass after those callbacks.
  After changing this bridge, clean and rebuild backend objects because
  `PgBackend` layout and installed runtime headers changed; at minimum rebuild
  and reinstall PL/pgSQL, `src/test/modules/test_backend_runtime`, and contrib
  before validating.
- Deadlock detector workspace state (`visitedProcs`, `nVisitedProcs`,
  `topoProcs`, `beforeConstraints`, `afterConstraints`, `waitOrders`,
  `nWaitOrders`, `waitOrderProcs`, `curConstraints`, `nCurConstraints`,
  `maxCurConstraints`, `possibleConstraints`, `nPossibleConstraints`,
  `maxPossibleConstraints`, `deadlockDetails`, `nDeadlockDetails`, and
  `blocking_autovacuum_proc`) is now owned by `PgBackendLockState`, exposed
  through private compatibility macros in `src/backend/storage/lmgr/deadlock.c`.
  `PgBackendLockState` intentionally uses opaque pointer fields so the private
  `deadlock.c` `EDGE`, `WAIT_ORDER`, and `DEADLOCK_INFO` types stay local to
  that source file. After changing this bridge, clean and rebuild backend
  objects because `PgBackend` layout and installed runtime headers changed; at
  minimum rebuild and reinstall PL/pgSQL, `src/test/modules/test_backend_runtime`,
  and contrib before validating.
- Local-buffer state (`NLocBuffer`, `LocalBufferDescriptors`,
  `LocalBufferBlockPointers`, `LocalRefCount`, `nextFreeLocalBufId`,
  `LocalBufHash`, `NLocalPinnedBuffers`, and the `GetLocalBufferStorage()`
  allocation cursor/context fields) is now owned by `PgBackendBufferState`.
  Exported local-buffer names are compatibility macros in `storage/bufmgr.h`
  and `storage/buf_internals.h`; private names remain compatibility macros in
  `src/backend/storage/buffer/localbuf.c`. Shared-buffer pin/writeback state
  (`BackendWritebackContext`, `PinCountWaitBuf`, the private refcount
  array/hash state, and `MaxProportionalPins`) is also owned by
  `PgBackendBufferState`; `BackendWritebackContext` remains object-like at call
  sites through a `storage/buf_internals.h` macro. After changing this bridge,
  clean and rebuild backend objects because `PgBackend` layout and installed
  buffer/runtime headers changed; at minimum rebuild and reinstall PL/pgSQL,
  `src/test/modules/test_backend_runtime`, and contrib before validating.
- IPC/sinval backend state (`MyProcSignalSlot`, `SharedInvalidMessageCounter`,
  `catchupInterruptPending`, and the recursive
  `ReceiveSharedInvalidMessages()` buffer/cursor state) is now owned by
  `PgBackendIPCState`. `procsignal.c` keeps `ProcSignalSlot` private through a
  file-local compatibility macro; `sinval.h` keeps the exported counter/flag
  names as compatibility macros. After changing this bridge, clean and rebuild
  backend objects because `PgBackend` layout and installed storage/runtime
  headers changed; at minimum rebuild and reinstall PL/pgSQL,
  `src/test/modules/test_backend_runtime`, and contrib before validating.
- Lock-manager backend-local state now also lives in `PgBackendLockState`:
  fast-path local-use counters, relation-extension lock ownership,
  `LockMethodLocalHash`, strong-lock progress, awaited-lock/owner state,
  `got_deadlock_timeout`, condition-variable sleep target state, and
  speculative insertion token state. `lock.c`, `proc.c`,
  `condition_variable.c`, and `lmgr.c` keep local compatibility macros. After
  changing this bridge, clean and rebuild backend objects because
  `PgBackend` layout and installed runtime headers changed; at minimum rebuild
  and reinstall PL/pgSQL, `src/test/modules/test_backend_runtime`, and contrib
  before validating.
- Always-built LWLock backend-local state now also lives in
  `PgBackendLockState`: `num_held_lwlocks`, the fixed `held_lwlocks` array,
  and `LocalNumUserDefinedTranches` are backed by runtime accessors while
  `lwlock.c` keeps the existing local source names. Optional `LWLOCK_STATS`
  debug state also lives in this bucket: the stats hash pointer, dummy stats
  entry, stats memory context pointer, and exit-registration flag are routed
  through backend-runtime accessors. Normal builds in this checkout do not
  compile the debug-only stats block, so pair static lifetime scan coverage
  with the backend-runtime accessor test unless using an `LWLOCK_STATS` build.
  After changing this bridge, clean and rebuild backend objects because
  `PgBackend` layout and installed runtime headers changed; at minimum rebuild
  and reinstall PL/pgSQL, `src/test/modules/test_backend_runtime`, and contrib
  before validating.
- Predicate-lock backend-local state now also lives in `PgBackendLockState`:
  `LocalPredicateLockHash`, `MySerializableXact`, `MyXactDidWrite`, and
  `SavedSerializableXact` are backed by runtime accessors while `predicate.c`
  keeps the existing local source names. Keep private `SERIALIZABLEXACT`
  layout out of `backend_runtime.h`; store those pointers as opaque `void *`
  fields and cast them in `predicate.c`. After changing this bridge, clean and
  rebuild backend objects because `PgBackend` layout and installed runtime
  headers changed; at minimum rebuild and reinstall PL/pgSQL,
  `src/test/modules/test_backend_runtime`, and contrib before validating.
  Direct threaded TAP should be run; a clean rerun is acceptable if the first
  run hits the known transient macOS child-count/shutdown race after SQL
  assertions finish.
- Transaction/access-manager backend-local state now lives in
  `PgBackendTransactionState`: transaction-status cache fields, two-phase
  locked-GXACT and exit-registration fields, the private `TwoPhaseGetGXact()`
  lookup cache, SLRU error-report fields, and multixact cache/debug-string
  state. This bridge deliberately includes function-local statics that do not
  appear in the raw `PG_THREAD_LOCAL` scan. The multixact list head must be
  initialized through the runtime state initializer, and early adoption asserts
  that any initialized early list is empty before copying. After changing this
  bridge, clean and rebuild backend objects because `PgBackend` layout and
  installed runtime headers changed; at minimum rebuild and reinstall
  PL/pgSQL, `src/test/modules/test_backend_runtime`, and contrib before
  validating.
- ProcArray backend-local visibility/cache state now also lives in
  `PgBackendTransactionState`: the `TransactionIdIsInProgress()` negative
  cache, `GlobalVisState` horizon caches, the
  `ComputeXidHorizonsResultLastXmin` throttle, and `XIDCACHE_DEBUG` counters.
  `GlobalVisState` is defined in `utils/backend_runtime.h` so the runtime can
  store it by value while existing snapshot/heapam headers keep using forward
  declarations. After changing this bridge, clean and rebuild backend objects
  because `PgBackend` layout and installed runtime headers changed; at minimum
  rebuild and reinstall PL/pgSQL, `src/test/modules/test_backend_runtime`, and
  contrib before validating.
- Snapshot-manager and combo-CID transaction visibility state now lives in
  `PgExecution`: `PgExecutionSnapshotState` owns `snapmgr.c` snapshot pointers,
  reusable `SnapshotData`, `TransactionXmin`, `RecentXmin`,
  `FirstSnapshotSet`, active/registered snapshot tracking, historic tuple-CID
  state, and exported-snapshot tracking; `PgExecutionComboCidState` owns the
  combo-CID hash, array pointer, and counters. `snapmgr.c` keeps its
  `ActiveSnapshotElt` type and registered-snapshot heap comparator private;
  the runtime bucket stores the heap and `snapmgr.c` lazily initializes the
  comparator. After changing this bridge, clean and rebuild backend objects
  because `PgExecution` layout and installed runtime/snapshot headers changed;
  at minimum rebuild and reinstall PL/pgSQL, `src/test/modules/test_backend_runtime`,
  and contrib before validating.
- WAL record-construction workspace now lives in `PgExecution`:
  `PgExecutionXLogInsertState` owns the `xloginsert.c` registered-buffer
  workspace, main-data `XLogRecData` chain state, insert flags, header
  record/scratch storage, registered-data array state, in-progress flag, and
  memory context. `registered_buffer` remains private to `xloginsert.c` behind
  an opaque runtime pointer. Early adoption asserts that no WAL insert is in
  progress and retargets the `mainrdata_last` self-pointer sentinel when early
  `InitXLogInsert()` has run before process/thread runtime installation. The
  hidden `XLogGetFakeLSN()` function-local statics are still a follow-up
  because they need a separate session/execution lifetime decision. After
  changing this bridge, clean and rebuild backend objects because
  `PgExecution` layout and installed runtime headers changed; at minimum
  rebuild and reinstall `src/test/modules/test_backend_runtime`, PL/pgSQL, and
  contrib before validating.
- Simple exported transaction execution state now lives in `PgExecution`:
  `PgExecutionXactState` owns `XactIsoLevel`, `XactReadOnly`,
  `XactDeferrable`, `xact_is_sampled`, `CheckXidAlive`, `bsysscan`, and
  `MyXactFlags`, plus the top full XID, parallel-current-XID count/borrowed
  pointer, inline unreported-XID array, subtransaction and command ID counters,
  transaction timestamps, prepare GID, force-sync flag, and transaction abort
  context pointer. `xact.h` keeps the public exported names as lvalue macros
  but must not include `backend_runtime.h`; it only declares accessor
  prototypes because `backend_runtime.h` already includes `xact.h`. The
  private transaction-state stack and transaction callback lists in `xact.c`
  remain a follow-up requiring a broader lifecycle split. After changing this
  bridge, clean and rebuild backend objects because `PgExecution` layout and
  installed runtime/xact headers changed; at minimum rebuild and reinstall
  `src/test/modules/test_backend_runtime`, PL/pgSQL, and contrib before
  validating. If `xact.c` defines compatibility macros for private names,
  rename local struct fields such as serialized transaction-state fields so
  macro expansion does not rewrite `tstate->field` references.
- GUC/error-report scratch state now lives in `PgExecution`:
  `PgExecutionGUCErrorState` owns the GUC check-hook error code and
  message/detail/hint strings, `pre_format_elog_string()` errno/domain state,
  and config-file scanner line/fatal-jump state. `guc.h` keeps public
  `GUC_check_errmsg_string`, `GUC_check_errdetail_string`, and
  `GUC_check_errhint_string` as lvalue macros. `guc.c`, `elog.c`, and
  `guc-file.l` keep private names through file-local compatibility macros.
  After changing this bridge, clean and rebuild backend objects because
  `PgExecution` layout and installed `guc.h` changed; stale backend objects
  can fail to link against removed `_GUC_check_*` symbols and stale PL/pgSQL
  can fail during `initdb` while loading removed `_GUC_check_*` symbols. At
  minimum rebuild and reinstall `src/test/modules/test_backend_runtime`,
  PL/pgSQL, and contrib before validating.
- Backend activity snapshot state now lives in `PgBackendActivityState`:
  `localBackendStatusTable`, `localNumBackends`, and
  `backendStatusSnapContext` are backed by runtime accessors while
  `backend_status.c` keeps the existing local source names. Pgstat
  shared-entry reference-cache state (`pgStatEntryRefHash`,
  `pgStatSharedRefAge`, `pgStatSharedRefContext`, and
  `pgStatEntryRefHashContext`) now lives in `PgBackendPgStatPendingState`
  behind private pgstat accessors and `pgstat_shmem.c` compatibility macros.
  The private simplehash type stays local to `pgstat_shmem.c` through an
  opaque runtime pointer. `pgStatLocal` remains standalone backend-local TLS
  for a later dedicated pgstat-local slice because its type depends on
  internal pgstat snapshot structures. After changing this bridge, clean and
  rebuild backend objects because `PgBackend` layout and installed runtime
  headers changed; at minimum rebuild and reinstall PL/pgSQL,
  `src/test/modules/test_backend_runtime`, and contrib before validating.
- Backend utility/support state now lives in `PgBackendUtilityState`:
  dynahash active sequential-scan tracking, the superuser one-entry cache,
  the resource-owner release callback pointer, and optional `RESOWNER_STATS`
  counters are backed by runtime accessors while `dynahash.c`,
  `superuser.c`, and `resowner.c` keep local source names. The private
  `ResourceReleaseCallbackItem` type stays local to `resowner.c`; the runtime
  stores the callback head as an opaque pointer and `resowner.c` casts it
  through a file-local typed helper. After changing this bridge, clean and
  rebuild backend objects because `PgBackend` layout and installed runtime
  headers changed; at minimum rebuild and reinstall PL/pgSQL,
  `src/test/modules/test_backend_runtime`, and contrib before validating.
- Utility cache/scratch state now also lives in `PgBackendUtilityState`:
  date/time token caches, degree-trig cached constants, date/time and numeric
  format-picture caches, the optional libxml allocation context, and the
  missing-attribute datum cache are backed by runtime accessors while
  `datetime.c`, `float.c`, `formatting.c`, `xml.c`, and `heaptuple.c` keep
  local source names. Private cache entry types stay private to their owning
  files through opaque runtime pointer arrays and local casts. After changing
  this bridge, clean and rebuild backend objects because `PgBackend` layout
  and installed runtime headers changed; at minimum rebuild and reinstall
  PL/pgSQL, `src/test/modules/test_backend_runtime`, and contrib before
  validating.
- Timezone-abbreviation session state now lives in `PgSessionDateTimeState`:
  `datetime.c` keeps local names for the active `TimeZoneAbbrevTable` pointer
  and recent abbreviation lookup cache through runtime accessors. The table
  pointer is borrowed from GUC extra storage; the inline cache is copied with
  the session bucket and reset by `InstallTimeZoneAbbrevs()` and
  `ClearTimeZoneAbbrevCache()`. After changing this bridge, rebuild
  `backend_runtime.o`, `datetime.o`, and `test_backend_runtime.o`, then run
  `gmake check-runtime-lifecycles` and `gmake check-global-lifetimes`.
- Logical replication session-cache roots now live in
  `PgSessionLogicalReplicationState`: `origin.c`, `relation.c`, `syncutils.c`,
  and `pgoutput.c` keep local names through runtime accessors. Relation-map
  contexts own their hashes and entries; `pgoutput` owns its relation sync
  hash; the replication-origin slot is a borrowed shared-memory pointer whose
  refcount is still released by `replorigin_session_reset()`/exit cleanup.
  After changing this bridge, rebuild `backend_runtime.o`, `origin.o`,
  `relation.o`, `syncutils.o`, `pgoutput.o`, and
  `test_backend_runtime.o`, then run the lifecycle/global scans and the
  `test_backend_runtime` regression.
- Utility command/cache state in `PgBackendUtilityState` now also covers
  async notify pending and exit-registration flags, the extension sibling cache
  head, the injection-point callback cache, and the legacy sampling reservoir
  state. `notifyInterruptPending` remains an exported source-compatible macro
  in `commands/async.h`; `ExtensionSiblingCache` stays private to
  `extension.c`; injection-point coverage requires an
  `--enable-injection-points` build for runtime tests. After changing this
  bridge, clean and rebuild backend objects because `PgBackend` layout and
  installed runtime headers changed; at minimum rebuild and reinstall
  PL/pgSQL, `src/test/modules/test_backend_runtime`, and contrib before
  validating.
- Parallel worker and pqmq backend-local state now lives in
  `PgBackendParallelState`: `ParallelWorkerNumber`,
  `ParallelMessagePending`, `InitializingParallelWorker`, private parallel
  context tracking, and shared-memory message queue redirection state are
  backed by runtime accessors while `parallel.c` and `pqmq.c` keep local
  source names. Private `FixedParallelState` and `shm_mq_handle` types remain
  opaque outside their owning files. The early fallback parallel state must
  keep the legacy `ParallelWorkerNumber = -1` sentinel as a static
  initializer; bootstrap reaches `IsParallelWorker()` before full backend
  runtime adoption, and a zero fallback makes `initdb` believe it is in a
  parallel worker. After changing this bridge, clean and rebuild backend
  objects because `PgBackend` layout and installed runtime headers changed; at
  minimum rebuild and reinstall PL/pgSQL, `src/test/modules/test_backend_runtime`,
  and contrib before validating.
- DSM/latch IPC backend-local state now also lives in `PgBackendIPCState`:
  `dsm_init_done`, `dsm_registry_dsa`, `dsm_registry_table`, `LatchWaitSet`,
  and `LocalLatchData` are backed by runtime accessors while `dsm.c`,
  `dsm_registry.c`, `latch.c`, and `miscinit.c` keep local compatibility
  names. Threaded backend startup initializes `MyLatch` and `LatchWaitSet`
  before installing the backend runtime object, so early IPC adoption must
  retarget adopted `backend->core.latch` and `backend->interrupt_latch`
  pointers from the early fallback latch to the backend-owned latch. If this
  is missed, direct threaded TAP fails during startup with
  `cannot wait on a latch owned by another process`. After changing this
  bridge, clean and rebuild backend objects because `PgBackend` layout and
  installed runtime headers changed; at minimum rebuild and reinstall
  PL/pgSQL, `src/test/modules/test_backend_runtime`, and contrib before
  validating.
- Timeout scheduler backend-local state now lives in `PgBackendTimeoutState`:
  registered timeout parameters, the active timeout queue, alarm/signal
  pending flags, firing-target pointers, and signal-vs-logical delivery mode
  are backed by runtime accessors while `timeout.c` keeps local compatibility
  names. `PgTimeoutParams` is defined in `utils/timeout.h` so `PgBackend` can
  own the fixed timeout arrays directly. After changing this bridge, clean and
  rebuild backend objects because `PgBackend` layout and installed runtime
  headers changed; at minimum rebuild and reinstall PL/pgSQL,
  `src/test/modules/test_backend_runtime`, and contrib before validating.
  Direct threaded TAP exercises logical timeout delivery and should be run.
- WAL sender backend-local state now lives in `PgBackendWalSenderState`.
  Public WAL sender flags and `MyWalSnd` are compatibility macros over
  `PgCurrentWalSenderState()`, while `walsender.c` uses private macros for the
  streaming cursor, timeline state, reply buffers, logical decoding context,
  replication command context, and lag tracker. Keep the local sent pointer
  named distinctly from `WalSnd.sentPtr` to avoid macro expansion inside
  shared-memory struct field references. After changing this bridge, clean and
  rebuild backend objects because `PgBackend` layout and installed runtime
  headers changed; at minimum rebuild and reinstall PL/pgSQL,
  `src/test/modules/test_backend_runtime`, and contrib before validating.
  Direct threaded TAP should be run.
- Replication receiver and slot backend-local state now lives in
  `PgBackendReplicationState`. `MyReplicationSlot` is a compatibility macro
  over `PgCurrentReplicationState()`, while `syncrep.c` and `walreceiver.c`
  keep local compatibility names for sync-rep wait mode and WAL receiver
  connection/file/logstream/wakeup/reply state. The runtime initializer sets
  non-zero sentinels: `sync_rep_wait_mode = SYNC_REP_NO_WAIT`,
  `walreceiver_recv_file = -1`, and
  `walreceiver_primary_has_standby_xmin = true`. Fake-backend tests that
  inspect untouched replication state must initialize those fields explicitly
  because raw `MemSet()` does not model `PgBackendInitializeReplicationState()`.
  After changing this bridge, clean and rebuild backend objects because
  `PgBackend` layout and installed runtime headers changed; at minimum rebuild
  and reinstall PL/pgSQL, `src/test/modules/test_backend_runtime`, and contrib
  before validating. Direct threaded TAP should be run.
- Logical replication worker backend-local state now lives in
  `PgBackendLogicalReplicationState`. Public logical replication headers keep
  the old names for `ApplyContext`, `MyParallelShared`,
  `ParallelApplyMessagePending`, `LogRepWorkerWalRcvConn`, `MySubscription`,
  `MyLogicalRepWorker`, `in_remote_transaction`, `InitializingApplyWorker`,
  `table_states_not_ready`, `SlotSyncShutdownPending`, and `XLogLogicalInfo`
  as compatibility macros over `PgCurrentLogicalReplicationState()`.
  Source-private launcher, apply-worker, parallel-apply, table-sync,
  sequence-sync, logical-info, and slot-sync fields use local macros in their
  owning files. The runtime initializer sets non-zero sentinels for
  `remote_final_lsn`, `stream_xid`, `skip_xact_finish_lsn`, and
  `last_flushpos`. Fake-backend tests that inspect untouched logical
  replication state must initialize those fields explicitly because raw
  `MemSet()` does not model `PgBackendInitializeLogicalReplicationState()`.
  The deeper logical replication internals `lsn_mapping`,
  `apply_error_callback_arg`, `subxact_data`, and slot-sync `sleep_ms` are
  also now stored in `PgBackendLogicalReplicationState`; the runtime
  initializer sets their non-zero sentinels, including `remote_attnum = -1`,
  invalid transaction/LSN values, invalid `subxact_last`, and
  `PG_BACKEND_SLOTSYNC_INITIAL_SLEEP_MS`. Do not include
  `logicalrelation.h` or `logicalproto.h` from `backend_runtime.h`; keep
  private logical-replication layouts opaque there by using `struct
  LogicalRepRelMapEntry *` and `int` storage for the relation pointer and
  message type. After changing this bridge, clean and rebuild backend objects
  because `PgBackend` layout and installed runtime headers changed; at minimum
  rebuild and reinstall PL/pgSQL, `src/test/modules/test_backend_runtime`, and
  contrib before validating. Direct threaded TAP should be run.
- Treat `PMChild.thread_backend` as private PMChild-owned publication state.
  Postmaster code should use PMChild helper APIs for threaded backend
  interrupt, wakeup, and thread-exit publication rather than dereferencing or
  clearing the raw pointer outside PMChild.
- Treat `PMChild.signal_pid` as live carrier-visible routing/logging state.
  Thread exit publication must capture the exited logical backend id in the
  PMChild exit payload before clearing `signal_pid`, so the postmaster can log
  the exited backend without advertising a dead thread as signalable.
- Thread-backed PMChild signal-id reads and thread-exit payload reads must use
  PMChild helper APIs. They are protected by the same PMChild mutex as
  `thread_backend` publication and clearing. Thread-carrier payload resets in
  `PostmasterChildSetProcess()`, `PostmasterChildSetThread()`, and
  `ReleasePostmasterChildSlot()` also belong under that mutex, so slot release
  and reuse cannot race with signal-id, interrupt, wakeup, or exit-payload
  readers.
- Use `PostmasterChildDetachThreadBackend()` when a thread carrier needs to
  stop advertising its live logical-backend pointer before final exit
  publication. It preserves the exited logical id for reaping/logging while
  preventing later signal routing from targeting a backend committed to
  teardown.
- `test_pmchild_thread_backend_publication_race()` in
  `src/test/modules/test_backend_runtime` is the focused C-level stress for
  the PMChild helper contract. Run the full `test_backend_runtime` regression
  after changing PMChild thread publication, detach, signal-id, interrupt,
  wakeup, or exit-payload behavior.
- For thread-backed PMChild reaping, successful `pg_thread_join()` is the
  boundary before child cleanup and slot release. If join fails, leave the
  PMChild active and re-publish the claimed thread-exit report for retry; do
  not release or reuse a slot whose native carrier was not joined.
- Threaded backend exit captures the live carrier `TopMemoryContext` pointer in
  `PgBackend.exit_state` before cleanup clears runtime slots. After
  `PgBackendExitCleanup()` runs closed connection/session/backend/execution
  reset, `backend_thread_finish()` deletes that retained root and publishes
  zero retained bytes through PMChild exit accounting. If a future thread exit
  reports nonzero retained `TopMemoryContext` bytes, the postmaster logs a
  warning and the threaded TAP log guard treats it as a Gate E2 teardown
  regression. Keep that accounting path until an equally strong replacement
  exists.
- Forked process-mode children must detach inherited runtime current pointers
  and reset copied backend-local runtime objects before touching
  runtime-backed process-local globals. `fork_process()` calls
  `PgRuntimeResetAfterFork()` before reseeding `MyProcPid`; without that,
  child workers can inherit the postmaster's current backend/latch or copied
  DSM mapping lists and fail startup with messages such as `cannot wait on a
  latch owned by another process` or spin while walking inherited DSM list
  links.
- `test_backend_runtime_emit_fatal()` in
  `test_backend_runtime_threaded` is the focused threaded backend `FATAL`
  fixture. Run it through
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` with the
  local TAP `PERL5LIB` paths documented below, so the check covers the
  expected `FATAL`, verifies the backend id leaves `pg_stat_activity`, and
  confirms the server remains usable.
- `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` contains
  the broader mixed teardown stress for Gate E2. It starts concurrent
  backend-local `FATAL`, `pg_terminate_backend()`, and abandoned-client
  sessions, then verifies logical backend ids leave `pg_stat_activity`,
  advisory locks are released, and the server remains usable. Keep this
  fixture current when changing PMChild exit publication, thread join/retry,
  backend teardown, or session resource cleanup.
- Threaded regular backend launch duplicates the accepted client socket into
  `BackendThreadStart.client_sock`. `pq_init()` marks that launch-time socket
  copy invalid only after `Port` owns the descriptor and `socket_close()` is
  registered. `backend_thread_finish()` is the backstop for closing a still
  valid copied socket if startup fails before that handoff.
- Backend libpq connection teardown is now part of the Gate E2 resource model:
  `socket_close()` frees the frontend/backend `WaitEventSet`, the dynamically
  sized send buffer, and the `PortContext` that owns `Port` plus most startup
  packet/remote-host/authentication strings and SSL/GSS connection identity
  structures before closing the accepted socket. Keep the threaded TAP teardown
  matrix current when changing backend libpq socket I/O or `Port` ownership
  state, because normal disconnect, abandoned clients, `FATAL`, and
  administrator termination all exercise this callback.
- `PgConnectionResetClosedState()` is the retained-object cleanup companion to
  `socket_close()`. `socket_close()` remains responsible for freeing the
  palloc-backed send buffer and `WaitEventSet`; the runtime helper scrubs the
  retained `PgConnection` socket/protocol/startup/security buckets, deletes
  any connection-owned warning context left by startup/authentication, and
  frees the malloc-backed GSS buffers. `StoreConnectionWarning()` delegates to
  the object-explicit `StoreConnectionWarningForConnection()`, which copies
  warning text into `PgConnection.startup.connection_warning_context`; do not
  reintroduce `TopMemoryContext` allocation for connection warning list cells
  or strings. Keep `test_connection_reset_closed_state()` and
  `test_connection_warning_state_is_connection_local()` current when changing
  connection teardown ownership.
- `PgSessionResetClosedState()` is the first retained-session cleanup
  companion. It deletes `PgSession.dynamic_library_context`, which owns the
  `dynamic_library_inits` list cells used for per-session dynamic-library
  `_PG_init()` replay, and clears the list pointer after `on_proc_exit`
  callbacks have run. Keep `test_session_reset_closed_state()` current when
  changing extension module replay or session teardown ownership.
- Thread-backed auxiliary workers receive postmaster `SIGQUIT`, `SIGKILL`,
  and `SIGABRT` as logical `PG_BACKEND_INTERRUPT_PROC_DIE` mailbox events, not
  as process signal handlers that can `_exit()`. Any custom auxiliary
  interrupt loop that calls `PgCurrentBackendApplyInterrupts()` must explicitly
  handle `ProcDiePending`, or immediate shutdown can leave thread carriers
  waiting for SIGKILL escalation.
- There is no threaded startup serialization gate. Do not reintroduce a broad
  `backend_thread_entry()` gate: it can block normal client startup behind
  long-running worker initialization or a worker path that has not reached
  `ThreadedBackendStartupComplete()`. Any future startup serialization must
  name the shared-state dependency, use the narrowest possible critical
  section, and include a stress test that proves the gate releases.
- Thread-backed startup/exit publication must tolerate a missing postmaster
  latch during startup-era handoff. Startup carriers can finish before
  `ServerLoop()` has configured `postmaster_pmsignal_latch`; PMChild
  publication records the atomic state even with a NULL latch, and the
  postmaster drains thread startup/exit state before each blocking wait.
- Process-model background workers are still rejected in threaded mode.
  Thread-compatible dynamic background workers publish their shared bgworker
  started state only after the worker reaches
  `ThreadedBackendStartupComplete()`, so dynamic waiters cannot terminate the
  worker while `InitProcess()`, `BaseInit()`, or background-worker function
  lookup are still running. Background writer/checkpointer/WAL writer bypass
  was validated as a worker-specific narrowing because their common auxiliary
  startup does not run database/session bootstrap before entering the worker
  loop. The autovacuum launcher bypass is validated against the no-database
  launcher loop; autovacuum worker bypass is validated against a real
  database-connected autovacuum worker launch and table vacuum smoke. Startup
  process, archiver, WAL receiver, and WAL summarizer bypasses are validated
  separately because they use the same common auxiliary startup, publish
  wakeup/progress state through shared memory, and keep per-loop work state
  backend-local. WAL receiver's gate bypass covers
  `AuxiliaryProcessMainCommon()`; the later `libpqwalreceiver` load and
  streaming loop are validated by a threaded physical-replication smoke.
  Startup process bypass is validated by threaded normal-startup and
  crash-recovery smokes. Slot sync worker bypass is validated by a threaded
  physical standby smoke that synchronizes a failover logical slot from a
  primary and verifies standby catalog usability. Keep any future startup-gate
  reintroduction narrowly tied to a named shared-state dependency and covered
  by concurrent catalog-startup stress.
- Prefer introducing compatibility wrappers around current globals before
  changing all call sites.
- Be careful moving GUC backing variables behind dynamic lvalue macros. The
  generated GUC table stores direct pointers for many variables during
  `InitializeGUCVariablePointers()`. Variables written only by assign hooks,
  such as parsed `DateStyle`/`DateOrder`, can be moved independently, but
  direct-pointer GUCs need a GUC-table pointer rebind/adoption mechanism.
  Threaded startup now records the direct backing-variable pointers after
  `InitializeGUCVariablePointers()`, runs
  `RebindSessionGUCVariablePointers()`, and initializes every built-in GUC
  record whose backing pointer changed. Keep extending
  `RebindSessionGUCVariablePointers()` when moving more direct-pointer GUC
  backing variables under runtime/session/execution objects. Only the small
  TLS dummy startup compatibility list in
  `InitializeThreadedSessionCompatibilityGUCOptions()` should remain
  hand-curated until those dummy GUCs get explicit session accessors. When
  common GUC names become macros, local struct fields with the same names must
  be renamed because macro expansion also hits `object->field` expressions;
  this was observed for the local GIN build-state `work_mem` field and the
  `TableSpaceOpts` `seq_page_cost`/`random_page_cost` fields.
- Some string GUCs can still be unset after runtime installation because the
  generated GUC table may already point at early fallback accessors before the
  "changed pointer" pass runs. `InstallPgThreadBackendRuntimeState()` therefore
  calls `InitializeThreadedSessionRequiredGUCOptions()` after
  `PgSetCurrentSession()` and after installing `CurrentPgExecution`; the latter
  is required because GUC check hooks allocate through the current execution's
  memory context state. That pass now initializes any built-in string GUC whose
  backing pointer is owned by the installed `PgSession` and still has NULL
  string storage, so future session-owned string GUCs do not need to be added
  to a growing whitelist. `client_encoding` remains the only post-install
  compatibility exception because its authoritative state is the session
  encoding object rather than a direct `char *` field in `PgSession`.
- The central GUC registry is now `PgSession` state, not an independent
  process/thread-global bucket. `PgSessionGUCState` owns `GUCMemoryContext`,
  the copied GUC records, the GUC hash table, non-default/stack/report lists,
  reporting state, and `GUCNestLevel`. Any fake `PgSession` used by tests that
  call `SetConfigOption()`, `GetConfigOption()`, or `RebindSessionGUCVariablePointers()`
  needs a real per-session GUC table from `InitializeThreadedSessionGUCOptions()`;
  otherwise `guc_hashtab` will be NULL or a test sentinel and `find_option()`
  can crash. `test_backend_runtime` centralizes this in
  `test_copy_current_user_identity()`.
- Early GUC owner adoption must run before copying GUC-backed string buckets
  such as datetime, text search, and connection GUC state. The copied strings
  are owned by the transferred `GUCMemoryContext`; resetting the detached
  early datetime/text-search/connection string buckets leaves them
  uninitialized with NULL string pointers so partial runtime installation does
  not allocate new fallback-owned strings or free non-owned fallback defaults.
  Do not move `PgSessionAdoptEarlyGUCState()` later in
  `PgSessionAdoptEarlyState()`.
- Threaded GUC setup, mutation, and display currently use a temporary
  process-wide GUC critical section in `guc.c`. For Gate E2-Core, narrow or
  justify it for core PostgreSQL behavior: postmaster/runtime defaults,
  database/role settings, startup options, direct-pointer built-in variables,
  built-in assign hooks, and reset/default semantics. Full custom/extension GUC
  hook coverage is Phase 16 / Gate E2-Extensions work, except for the minimal
  thread-compatible test module and PL/pgSQL/runtime metadata paths needed to
  prove safe loading, safe rejection, and core teardown.
  The reentrancy depth for this bridge lives in `PgCarrier`, not standalone
  TLS; tests that swap fake carriers and touch `PgCurrentThreadedGUCMutexDepthRef()`
  must preserve and restore `CurrentPgCarrier`.
- Threaded `read_nondefault_variables()` skips `PGC_POSTMASTER` and
  `PGC_INTERNAL` records. Thread carriers share the postmaster address space,
  so process-global postmaster/internal GUCs are already present and must not
  be replayed through a session `GUCMemoryContext`.
- Runtime-global GUC metadata must not allocate from a session
  `GUCMemoryContext`. `reserved_class_prefix` is process/runtime metadata used
  by extension module initialization such as PL/pgSQL's GUC prefix
  reservation, so `MarkGUCPrefixReserved()` uses the runtime-owned extension
  module memory context and the temporary threaded GUC lock.
- Portal manager session state now lives in `PgSessionPortalManagerState`.
  `portalmem.c` keeps `TopPortalContext`, `PortalHashTable`, and the unnamed
  portal counter as local macros over runtime accessors. The lifecycle rule is
  destructive at session close: `PgSessionResetClosedState()` deletes
  `TopPortalContext`, which owns portal structs, portal contexts, hold
  contexts, and the portal hash table, then clears the counter.
- Regex session cache state now lives in `PgSessionRegexState`. `regexp.c`
  keeps the compiled-regexp context, fixed cached-entry array, cached-entry
  count, and ctype cache list behind runtime accessors. Session reset deletes
  the compiled-regexp cache context, clears the inline array/count, and frees
  the ctype cache list.
- Syscache and catcache session roots now live in
  `PgSessionCatalogLookupState`. `syscache.c` keeps `SysCache[]`, the
  initialization flag, and relation/supporting-relation OID arrays behind
  runtime accessors; `catcache.c` keeps `CacheHdr` behind a runtime accessor.
  Relcache root hashes, critical-cache flags, and the relcache invalidation
  counter also live in this bucket; `relcache.h` keeps the historical critical
  flag names as accessor macros for `relcache.c`, `catcache.c`, and
  `postinit.c`. Typcache root hashes, the domain list, in-progress stack
  pointer/counters, record-cache array/counters, and tupledesc ID counter also
  live in this bucket; `typcache.c` keeps the historical local names as
  accessor macros. Session reset clears those roots and scalars, while cache
  entry memory is reclaimed with the session-owned `CacheMemoryContext` after
  dependent buckets reset. Do not move `funccache.c`'s hash root without
  adding a real iterator/destructor for copied tuple descriptors and
  language-specific cached-function state.
- After changing the relcache critical-cache flags from exported TLS variables
  to `relcache.h` accessor macros, stale objects may still reference the old
  linker symbols even when GNU make thinks they are up to date. If the backend
  link fails with unresolved `_criticalRelcachesBuilt` or
  `_criticalSharedRelcachesBuilt`, remove and rebuild at least
  `src/backend/utils/cache/catcache.o`, `src/backend/utils/init/postinit.o`,
  and `src/backend/commands/seclabel.o`, then rerun `gmake -j8`.
- Do not shallow-copy live `dlist_head` or `dclist_head` values when moving
  fallback state into a real runtime object. Use the runtime list-head move
  helpers so moved list nodes' back-links point at the destination head. This
  currently matters for the GUC non-default list and RI valid-entry dclist.
- Threaded backend cleanup deletes each exiting carrier's retained
  `TopMemoryContext` in `backend_thread_finish()`. Do not free AllocSet context
  freelists during threaded `PgBackendResetClosedState()` because the retained
  root-context deletion owns that memory-context teardown; thread-mode reset
  clears the memory-manager freelist bucket, while process-mode reset still
  calls `AllocSetFreeContextFreelists()`.
- Background writer, WAL writer, checkpointer, and WAL summarizer work
  contexts are owned through `PgBackend.maintenance_worker`, not local-only
  variables. Preserve that ownership when changing those loops: the worker
  still resets its context after recoverable errors, and
  `PgBackendResetMaintenanceWorkerClosedState()` deletes the retained context
  on closed-backend reset.
- Threaded startup serialization has been removed rather than kept as a no-op
  helper. A broad `backend_thread_entry()` gate can block normal threaded
  startup behind worker paths that have not reached
  `ThreadedBackendStartupComplete()`. Any future startup gate must name the
  exact shared-state dependency, use a narrow critical section, and include a
  release/stress test.
- Backend timeout state now has a closed-backend reset:
  `PgBackendResetTimeoutClosedState()` lives beside timeout semantics in
  `src/backend/utils/misc/timeout.c`, and `check-runtime-lifecycles` scans
  that file. This reset is for retained closed logical backends and clears
  active timeout arrays, handler registrations, signal flags, and stale
  `PgBackend`/`PgExecution` target pointers. Do not substitute
  `disable_all_timeouts()` for closed-backend teardown; that API is for live
  backends and preserves timeout registrations.
- Gate E2 backend closed-state reset now covers the parallel, buffer, IPC,
  transaction, recovery, and repack buckets through
  `backend_runtime_backend_buckets.def`. Buffer reset has an important
  process-mode caveat: at late `proc_exit`, buffer callbacks have already
  checked semantic cleanup and some private-refcount hash storage may live in
  contexts that are no longer safe to destroy. In that path,
  `PgBackendResetBufferClosedState()` reinitializes constructor defaults and
  lets process exit reclaim storage. Only non-exit retained-backend reset
  frees the local-buffer/private-refcount allocations directly.
- Custom extension GUCs in threaded sessions rely on per-session `_PG_init()`
  invocation for already-loaded dynamic libraries. `dfmgr.c` records loaded
  module init state in `PgSession.dynamic_library_inits`, with list storage
  allocated under `PgSession.dynamic_library_context`; when a second threaded
  session reuses a process-loaded module, `_PG_init()` must run again so that
  session's GUC table receives the custom GUC definitions. A focused custom-GUC
  smoke should use `LOAD 'test_backend_runtime_threaded'` plus `SHOW`, so it
  validates module/GUC behavior without depending on catalog writes.
- Threaded catalog-writing DDL previously crashed in `XLogInsert()` during
  `CREATE TABLE` because the derived `wal_consistency_checking` bool array was
  NULL in the installed `PgSession`. Keep the threaded
  `CREATE TABLE`/`INSERT`/`DROP TABLE` smoke in
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` when
  changing required GUC bootstrap or WAL GUC state.
- The same threaded runtime TAP fixture now covers database, role, and startup
  GUC adoption: `ALTER DATABASE postgres SET work_mem`, `ALTER ROLE ... SET
  statement_timeout`, `ALTER ROLE ... SET default_statistics_target`, and a
  startup-packet `options='-c lock_timeout=8s'` connection. Keep that matrix
  current when changing threaded GUC replay/adoption paths.
- Threaded backend startup must replay postmaster nondefault GUC state after
  `InitializeThreadedSessionGUCOptions()` and before
  `InstallPgThreadBackendRuntimeState()`. That ordering lets
  `read_nondefault_variables()` write configured built-in defaults into early
  fallback session/runtime GUC buckets, which runtime installation then adopts
  into the thread's `PgSession`/runtime objects. Moving runtime installation
  earlier can crash because some adoption paths allocate GUC strings before
  `GUCMemoryContext` exists.
  The replay depends on the postmaster write side too: non-`EXEC_BACKEND`
  postmasters must call `write_nondefault_variables()` when `multithreaded` is
  enabled, both after initial config load and after SIGHUP reloads. Without
  `global/config_exec_params`, threaded clients silently fall back to boot
  defaults such as `work_mem = 4MB`.
- Avoid broad mechanical churn unless it unlocks a specific migration step.
- Do not remove process isolation paths merely because threaded mode exists.
