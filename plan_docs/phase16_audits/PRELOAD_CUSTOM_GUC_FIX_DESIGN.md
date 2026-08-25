# shared_preload_libraries custom GUCs invisible under multithreaded=on — fix design

Read-only investigation. No source files were modified. Line numbers are from
the working tree at investigation time (branch `xtc`).

Companion analysis: `.ec2/preload-custom-guc-threaded-gap.md`.
Companion (per-session VALUE correctness, already designed): `CUSTOM_GUC_FIX_DESIGN.md`.

---

## 1. Confirmed root cause (with file:line)

### 1a. Two storage classes for GUC descriptors

- **Built-in GUCs** use the immutable process-global `ConfigureNames[]` table
  plus a **per-session** overlay array `guc_variable_states[]`:
  - `build_guc_variables()` sets `guc_variables = ConfigureNames;` and allocates
    `guc_variable_states` in the per-session `GUCMemoryContext`
    (`guc.c:1793`, `:1798`).
  - A built-in record is recognized purely by **address-range membership** in
    `guc_variables == ConfigureNames`: `GUCRecordIsCurrentSessionBuiltin()`
    (`guc.c:257-275`), and its mutable state is `&guc_variable_states[index]`
    (`GUCRecordState`, `guc.c:326-341`).
  - The shared name→index lookup table `guc_builtin_hashtab` is a
    `PG_GLOBAL_RUNTIME`, built once in `TopMemoryContext`
    (`ensure_builtin_guc_name_index`, `guc.c:2204-2245`;
    declared `guc.c:834`). Immutable after build → safely shared by all sessions.

- **Custom GUCs and placeholders** live in a **per-session** hash table
  `guc_hashtab`:
  - `#define guc_hashtab (*PgCurrentGUCHashTableRef())` (`guc.c:100`).
  - `PgCurrentGUCHashTableRef()` resolves to
    `PgCurrentSessionGUCState()->hash_table` (`backend_runtime_guc.c:441-444`) —
    per-session (or the process-global early-session fallback when
    `CurrentPgSession == NULL`).
  - Comment stating the split: "Custom GUCs and placeholders remain per-session
    in guc_hashtab" (`guc.c:825-828`).
  - The table is created lazily by `ensure_guc_custom_hashtab()` in the
    per-session `GUCMemoryContext` (`guc.c:2478-2496`).

### 1b. A preloaded module registers into the POSTMASTER's guc_hashtab, once

- The postmaster runs `InitializeGUCOptions()` (`postmaster.c:632`) →
  `build_guc_variables()` (`guc.c:1771`) **before** preload.
- The postmaster then runs `process_shared_preload_libraries()` **once**
  (`postmaster.c:1097`). At that point `CurrentPgSession == NULL`, so
  `guc_hashtab` resolves to the process-global **early-session fallback**
  `PgSessionGUCState` (`backend_runtime_guc.c:395-402, 441-444`).
- Each module's `_PG_init` calls `DefineCustom*Variable`
  (`guc.c:6793` ff.) → `define_custom_variable()` (`guc.c:6575`) →
  `add_guc_variable()` (`guc.c:1861`) → `ensure_guc_custom_hashtab()` writes the
  descriptor into **that early-fallback `guc_hashtab`** (`guc.c:1873-1886`).
- `process_shared_preload_libraries_in_progress` gates it: only PGC_POSTMASTER
  customs are allowed to be created outside preload (`init_custom_variable`,
  `guc.c:6519-6529`). This runs in the postmaster only.

### 1c. Each threaded session builds a FRESH hashtab, without the customs

- A pooled/threaded session enters bring-up via
  `InitializeThreadedSessionGUCOptions()` (called from `launch_backend.c:2055`
  and `:2282`, and `postinit.c:1035`).
- That function calls `build_guc_variables()` (`guc.c:2748`) — which populates
  **built-ins only** (it iterates `ConfigureNames[]`, `guc.c:1793-1796`) — then
  `RebindSessionGUCVariablePointers()` and
  `InitializeThreadedSessionReboundGUCOptions()` (`guc.c:2750-2751`).
- `InitializeThreadedSessionReboundGUCOptions()` iterates only
  `num_guc_variables` — the built-in array — never the custom hashtab
  (`guc.c:2843-2857`).
- The session's `guc_hashtab` starts NULL and is only populated by
  *this session's own* `DefineCustom*`/placeholder creation. The postmaster's
  preload registrations never reach it, because `_PG_init` is **not** re-run per
  threaded session.
- Confirming the symptom precisely: when a preload custom GUC was also SET in
  `postgresql.conf`, `read_nondefault_variables()` (`guc.c:7435`) looks it up
  with `find_option(varname, /*create_placeholders=*/true, ...)`
  (`guc.c:7468`). With no real descriptor present, that creates a **placeholder**
  (`GUC_CUSTOM_PLACEHOLDER`), so `SHOW pg_stat_statements.track` still errors
  "unrecognized configuration parameter" (a placeholder is `GUC_NO_SHOW_ALL` and
  is not the real definition). If it was *not* set in the config, nothing is
  created at all. Either way the real descriptor is missing.

### 1d. Why the fork model works

A forked backend does **no** re-registration. `fork()` copies the postmaster's
entire address space copy-on-write, so the child inherits:
- the postmaster's `guc_hashtab` **with the preload custom descriptors already in
  it**, and
- the extension's C globals and hooks already initialized by `_PG_init`.

The forked child never re-runs `_PG_init` for shared_preload_libraries; it just
inherits the finished state. (EXEC_BACKEND / Windows re-exec is the exception:
it re-runs `process_shared_preload_libraries()` in the child —
`launch_backend.c:3345` — precisely to rebuild that inherited state after exec.)

**Root cause, one line:** custom-GUC descriptors are per-session state that the
process model gets "for free" via fork inheritance, but the threaded model
rebuilds per session from `ConfigureNames[]` only — and `_PG_init` runs once in
the postmaster, so the descriptors are never re-created in the carrier session.

---

## 2. Fork-vs-thread missing-state enumeration

What a **forked** backend has that a **threaded** session is missing, per
preload custom GUC:

| State | Forked backend | Threaded session | Needed by session? |
|---|---|---|---|
| `guc_hashtab` entry (name→`config_generic *`) | inherited | **MISSING** | Yes — `find_option` returns NULL → "unrecognized" (`guc.c:2075-2085`) |
| `config_generic` **descriptor** (name, context, vartype, flags, boot_val, min/max, enum options, check/assign/show hooks) | inherited | **MISSING** | Yes — every SHOW/SET/pg_settings path needs it |
| `config_generic_state` **`->state`** (live `variable` ptr, `reset_val`, status, source, scontext, srole, stack, cold) | inherited (one copy, private to the child) | **MISSING** | Yes — this is per-session mutable state; see §3 |
| Extension C globals / `_PG_init` side effects (hooks, shmem hooks) | inherited | present in the **shared carrier address space** (hooks set once in postmaster fire for all sessions) | already OK — hooks fire (confirmed in the analysis doc) |
| The per-session VALUE cell the GUC writes through (`valueAddr`) | the extension's own global, private per process | see below | Yes — must be per-session; owned by `CUSTOM_GUC_FIX_DESIGN.md` |

Critical structural detail for the fix (from `guc_tables.h:311-355` and the
`GUC_*` macros at `guc.c:499-543`):

- For a **custom** record, `struct config_generic` is allocated by
  `init_custom_variable()` with a *separate* heap `->state`
  (`guc.c:6551-6557`). ALL mutable runtime fields route through
  `GUC_STATE(record)` → `GUCRecordState(record)` → `record->state` for customs:
  `GUC_VARIABLE_*`, `GUC_RESET_*`, `GUC_STATUS`, `GUC_SOURCE`, `GUC_STACK`, etc.
  (`guc.c:504-543`).
- Therefore the descriptor's *own* body (name, hooks, boot_val, min/max, enum
  table) is effectively immutable after registration, but **`->state` is
  per-session mutable**. A single shared descriptor with a single shared
  `->state` would let two sessions race on `variable`, `reset_val`, `status`,
  `source`, and `stack`. So:
  - the **descriptor** can be shared (immutable), but
  - each session still needs its **own `config_generic_state`**, and
  - the **`valueAddr`** (the cell the extension reads and guc.c writes) must be
    per-session — already the subject of `CUSTOM_GUC_FIX_DESIGN.md` (the
    `PgSessionEnsureExtensionPrivateState` pattern).

**Answer to the explicit question** ("does the DefineCustom* value storage /
per-session value cell need anything?"): yes. Seeding only the descriptor is not
enough for correctness. The session needs (i) the descriptor, (ii) a fresh
per-session `config_generic_state`, and (iii) the `valueAddr` pointed at
per-session backing. (i) is the gap this doc fixes; (ii) is created naturally
when we insert into the session hashtab (see §3 code shape); (iii) is the
already-designed extension conversion — it must land per extension **before**
that extension is marked affine, exactly as `CUSTOM_GUC_FIX_DESIGN.md` §(d)
sequences it.

---

## 3. Fix options, analysis, recommendation

### Option (a) — shared immutable "preload custom-GUC registry", seed per session

The postmaster, at the end of preload, snapshots the descriptors it registered
into a `PG_GLOBAL_RUNTIME` registry (analogous to `guc_builtin_hashtab`). Each
threaded session's GUC bring-up seeds its per-session `guc_hashtab` from that
registry, allocating a **fresh per-session `config_generic_state`** for each and
running `InitializeOneGUCOption`/`...ResetMetadata` against per-session storage.

- **Correctness of the direct C-global read:** the descriptor's `valueAddr` in
  the registry is whatever the extension passed at `DefineCustom*` time. When the
  extension is converted per `CUSTOM_GUC_FIX_DESIGN.md`, `valueAddr` resolves to
  the *current session's* cell (via `PgSessionEnsureExtensionPrivateState`), so
  the session's per-session `config_generic_state.variable` and the extension's
  `if (global)` read hit the same per-session cell. Seeding must set each
  session's `state->variable` from the extension's per-session-resolving address
  — i.e., copy the registry descriptor but let `InitializeOneGUCOption` /
  rebound-init recompute `variable` for the current session, exactly as
  `InitializeThreadedSessionReboundGUCOptions` already does for built-ins
  (`guc.c:2843-2857`), using `PgCurrentOrEarlySessionOwnsPointer` to decide
  whether to write the boot value or only reset-metadata.
- **Thread-safety / happens-before:** trivial. The postmaster populates the
  registry single-threaded, entirely, at `postmaster.c:1097` — **before any
  carrier session exists** (carriers spawn on connection, long after preload).
  Readers are sessions; the registry is immutable after preload. This is the
  same discipline `guc_builtin_hashtab` already relies on. No lock needed on the
  read path (the existing `ThreadedGUCLock()` around bring-up already serializes
  session hashtab construction anyway — `guc.c:2743-2758`).
- **Memory ownership:** the registry lives in `TopMemoryContext` (like
  `guc_builtin_hashtab`, `guc.c:2222`), never freed. Descriptor bodies are
  `guc_strdup`/`guc_malloc`'d already (`init_custom_variable`), owned by the
  postmaster's `GUCMemoryContext`, which also lives for process lifetime. The
  per-session `config_generic_state` copies are allocated in each session's
  `GUCMemoryContext` and die with the session.
- **Interaction with `CUSTOM_GUC_FIX_DESIGN.md`:** clean and orthogonal. That
  doc makes `valueAddr` per-session; this doc makes the descriptor reach the
  session. Both are required; neither subsumes the other. Order:
  extension-conversion (that doc) gates the affine marking; this core fix is a
  prerequisite for *validating* any preloaded affine extension.

### Option (b) — re-run custom-GUC registration per session

Re-invoke the DefineCustom* portion of each preloaded module per session.

- **Re-entrancy hazard (disqualifying):** `_PG_init` does far more than
  `DefineCustom*` — it installs hooks (`shmem_request_hook`,
  `ExecutorStart_hook`, planner hooks), requests shmem, allocates process-global
  state. Those must run **once per process**, not per session. There is no clean
  seam to run "only the DefineCustom* calls" without the module's cooperation.
  Rejected.

### Option (c) — make preload custom hashtab entries themselves shared

Put the actual `config_generic` records into a shared table and have all
sessions point at them, with per-session value storage.

- **Correctness problem:** as shown in §2, a custom record's mutable runtime
  state (`status`, `source`, `reset_val`, `stack`, and `variable`) all live in
  `record->state` via `GUCRecordState` (`guc.c:326-341`, macros `guc.c:504-543`).
  A single shared `->state` is a data race across sessions. To make (c) correct
  you must give each session its own `->state` anyway — at which point (c)
  collapses into (a): share the immutable descriptor, keep `->state`
  per-session. Also `GUCRecordIsCurrentSessionBuiltin` identifies built-ins by
  address range in `guc_variables`; a shared custom record is outside that range,
  so it correctly falls to `record->state` — meaning the per-session `->state`
  routing already exists and (a) reuses it directly.

### Recommendation: **Option (a)**, structured as a shared descriptor registry +
per-session `config_generic_state`, reusing the existing per-session `->state`
routing and the existing rebound-init discipline.

Rationale: it mirrors the proven `guc_builtin_hashtab` sharing model, needs no
per-module cooperation (unlike b), and is the only variant that is
race-free by construction for the mutable `->state` (unlike naive c). It composes
cleanly with the already-designed per-session `valueAddr` conversion.

### Concrete code shape

New shared registry (guc.c, near `guc_builtin_hashtab`, `guc.c:834`):

```c
/* Descriptors for custom GUCs registered during shared_preload_libraries.
 * Populated once in the postmaster after preload; immutable thereafter;
 * shared by all carrier sessions.  Lives in TopMemoryContext. */
static PG_GLOBAL_RUNTIME struct config_generic **preload_custom_gucs = NULL;
static PG_GLOBAL_RUNTIME int num_preload_custom_gucs = 0;
```

Snapshot hook, called once at the end of preload (add a call in
`process_shared_preload_libraries()` in `miscinit.c:1896-1904`, guarded by
`multithreaded`, right after `process_shared_preload_libraries_done = true`):

```c
/* guc.c */
void
SnapshotPreloadCustomGUCs(void)
{
    HASH_SEQ_STATUS status;
    GUCHashEntry   *hentry;
    int             n = 0, i = 0;

    if (guc_hashtab == NULL)          /* nothing custom registered */
        return;
    /* count non-placeholder custom entries */
    hash_seq_init(&status, guc_hashtab);
    while ((hentry = hash_seq_search(&status)) != NULL)
        if ((hentry->gucvar->flags & GUC_CUSTOM_PLACEHOLDER) == 0)
            n++;
    if (n == 0)
        return;
    preload_custom_gucs =
        MemoryContextAlloc(TopMemoryContext, n * sizeof(struct config_generic *));
    hash_seq_init(&status, guc_hashtab);
    while ((hentry = hash_seq_search(&status)) != NULL)
        if ((hentry->gucvar->flags & GUC_CUSTOM_PLACEHOLDER) == 0)
            preload_custom_gucs[i++] = hentry->gucvar;   /* descriptor pointer */
    num_preload_custom_gucs = n;
}
```

Seed point — new step inside `InitializeThreadedSessionGUCOptions()`
(`guc.c:2730`), after `build_guc_variables()` and the rebound-init pass
(`guc.c:2748-2751`):

```c
/* guc.c, called from InitializeThreadedSessionGUCOptions() */
static void
SeedSessionPreloadCustomGUCs(void)
{
    for (int i = 0; i < num_preload_custom_gucs; i++)
    {
        struct config_generic *tmpl = preload_custom_gucs[i];
        struct config_generic *var;
        config_generic_state  *state;

        if (find_option(tmpl->name, false, true, DEBUG5) != NULL)
            continue;                 /* already present (e.g. placeholder path) */

        /* Fresh per-session descriptor + state in this session's GUCMemoryContext. */
        var   = guc_malloc(ERROR, sizeof(*var));
        *var  = *tmpl;                /* copy immutable descriptor body incl. hooks,
                                       * boot_val, min/max, enum table, *variable ptr */
        state = guc_malloc(ERROR, sizeof(*state));
        memset(state, 0, sizeof(*state));
        var->state = state;

        /* Point the value cell at per-session backing and set boot/reset,
         * exactly like the built-in rebound-init split. */
        if (!PgCurrentOrEarlySessionOwnsPointer(GUCOptionVariablePointer(var)))
            InitializeOneGUCOptionResetMetadata(var);   /* not yet owned: reset meta only */
        else
            InitializeOneGUCOption(var);                /* per-session cell: boot it */

        add_guc_variable(var, ERROR);  /* into this session's guc_hashtab */
    }
}
```

Note the `*var = *tmpl` copy carries the descriptor's `variable` pointer as
registered. For a converted extension that pointer is a per-session-resolving
address (`&macro_over_PgSessionEnsureExtensionPrivateState`), so
`PgCurrentOrEarlySessionOwnsPointer` is true → the session boots its own cell.
For an as-yet-unconverted custom GUC it is an extension global → not owned →
reset-metadata only (no clobber). That matches, byte-for-byte, the built-in
handling in `InitializeThreadedSessionReboundGUCOptions` (`guc.c:2843-2857`),
so no new policy is introduced.

Functions to add/change, summary:
- **add** `SnapshotPreloadCustomGUCs()` (guc.c) + one call in
  `process_shared_preload_libraries()` (miscinit.c:1896).
- **add** `SeedSessionPreloadCustomGUCs()` (guc.c) + one call in
  `InitializeThreadedSessionGUCOptions()` (guc.c:2730, after line 2751).
- **add** two `PG_GLOBAL_RUNTIME` registry statics (guc.c:~834).
- **no change** to `add_guc_variable`, `define_custom_variable`,
  `build_guc_variables`, or the `DefineCustom*` entry points — they already do
  the right thing; we only *replay descriptors* into the session hashtab.

---

## 4. Placeholders

Placeholders (`GUC_CUSTOM_PLACEHOLDER`, created by `add_placeholder_variable`,
`guc.c:1993`) are the mechanism for a custom GUC name **SET before its defining
module has registered it**. They hold a stringized pending value and are later
replaced by the real descriptor in `define_custom_variable()` (`guc.c:6653-6660`).

Interaction with the fix:

- **The registry stores only fully-defined preload customs, NOT placeholders.**
  `SnapshotPreloadCustomGUCs()` filters on
  `(flags & GUC_CUSTOM_PLACEHOLDER) == 0`. A placeholder in the postmaster's
  hashtab means the module was preloaded and got its real definition (which
  *replaced* the placeholder during preload) — so by end-of-preload the real
  descriptor is what remains and gets snapshotted. Any placeholder *still
  present* at end-of-preload is for a class that no preloaded module defined; it
  is per-session by nature and must not be shared (its pending value came from
  config replay, which each session reconstructs itself).
- **Per-session placeholder creation still works unchanged.** A session that
  SETs an unknown `class.name` still creates its own placeholder via
  `find_option(..., create_placeholders=true, ...)`. The seed step runs *before*
  `read_nondefault_variables()` config replay in the carrier path
  (`launch_backend.c:2055` then `:2056`), so the real descriptor is present
  first and replay assigns into it instead of manufacturing a placeholder — this
  is exactly what fixes the "SHOW returns unrecognized" symptom for a
  config-SET preload custom GUC.

Ordering requirement: **seed before replay.** In `launch_backend.c` the sequence
is `InitializeThreadedSessionGUCOptions()` (`:2055`) then
`read_nondefault_variables()` (`:2056`). Since the seed lives *inside*
`InitializeThreadedSessionGUCOptions()`, ordering is satisfied automatically.

---

## 5. Scope: session_preload_libraries / local_preload_libraries

These are **not** affected and need no fix — the gap is specifically
`shared_preload_libraries`.

- `session_preload_libraries` and `local_preload_libraries` load via
  `process_session_preload_libraries()` (`postinit.c:1367`), which runs **inside
  the session** during `InitPostgres`, after GUC bring-up.
- Their `_PG_init` therefore runs in the session's own context, so their
  `DefineCustom*` calls register into the **session's own `guc_hashtab`** (the
  same per-session table `find_option` reads). Their custom GUCs are visible to
  that session with no seeding needed.
- This mirrors the already-validated behavior for `postgres_fdw` (loads on first
  use, `_PG_init` in-session → GUCs work under mt=on) and `auto_explain` via
  `LOAD` (analysis doc "Scope / not-yet-affected"). Confirmed: the gap is
  exclusively the postmaster-time, once-only `shared_preload_libraries` path.

Recommend a one-line regression assertion in the test plan (§6) that a
`session_preload_libraries` custom GUC is visible, to lock in this scope
boundary.

---

## 6. Test plan

Target: a **preloaded** `pg_stat_statements` under `multithreaded=on`, SHOW +
SET the custom GUC from two sessions. (Prerequisite: `pg_stat_statements` must
already be per-session-value-converted per `CUSTOM_GUC_FIX_DESIGN.md` and marked
affine — the analysis doc records it as converted but HELD on the affine marker
pending this fix. This fix unblocks that marker.)

Setup:
- `shared_preload_libraries = 'pg_stat_statements'`, `compute_query_id = on`,
  `multithreaded = on`, pooled protocol with a **1-carrier pool** so two sessions
  share one address space (the `check-threaded-pooled` harness).

Steps:
1. **SHOW visibility (the core regression).** Session A:
   `SHOW pg_stat_statements.track;` → must return `top` (not
   "unrecognized configuration parameter"). This is the exact A/B failure from
   the analysis doc; with the fix it must match `multithreaded=off`.
2. **Two-session SET isolation.** Force both sessions onto the one carrier:
   - A: `SET pg_stat_statements.track = 'all';`
   - B: `SET pg_stat_statements.track = 'none';`
   - Interleave (round-trip / `pg_sleep(0)` to yield the fiber at a wait
     boundary), then:
     - A: `SHOW pg_stat_statements.track;` → `all`
     - B: `SHOW pg_stat_statements.track;` → `none`
   This exercises both the descriptor seed (this fix) and the per-session value
   cell (`CUSTOM_GUC_FIX_DESIGN.md`). If either is wrong the reads bleed.
3. **A/B parity.** Same `SHOW`/`SET` transcript under `multithreaded=off` must
   produce identical results — the fork path is the oracle.
4. **Placeholder / config-SET path.** Set `pg_stat_statements.track = 'top'` in
   `postgresql.conf` (a preload custom GUC set in config), start mt=on, and
   `SHOW pg_stat_statements.track` in a fresh session → `top` (proves seed runs
   before `read_nondefault_variables` replay, §4).
5. **Scope guard (session-preload negative-space).** With
   `session_preload_libraries='auto_explain'` (no shared preload), confirm
   `SHOW auto_explain.log_analyze` works in-session under mt=on — locks in §5.
6. **RESET / xact coverage.** In A: `BEGIN; SET pg_stat_statements.track='all';
   ROLLBACK;` then A reads its prior value and B is unaffected — exercises the
   unguarded `AtEOXact_GUC`/`ResetAllOptions` paths on per-session `->state`
   (cross-ref `CUSTOM_GUC_FIX_DESIGN.md` §e step 5).

Where the assertions live: extend the existing pg_stat_statements TAP/regress
under a preload+mt=on config, plus reuse the `test_backend_runtime_threaded`
isolation harness (`src/test/modules/test_backend_runtime`, split files per
AGENTS.md) for the forced-single-carrier interleave.

Validation targets: `gmake check-threaded-pooled`, `gmake check-threaded`,
`gmake check-runtime-lifecycles` (the new `TopMemoryContext` registry is a
process-lifetime global — must pass the global-lifetime checker as a deliberate
process-lifetime exception, like `guc_builtin_hashtab`), and `gmake check`
(process mode must stay green — the snapshot/seed are `multithreaded`-guarded and
inert under process mode).

---

## Appendix — key file:line index

- Per-session custom hashtab: `guc.c:100`, `:825-828`,
  `backend_runtime_guc.c:441-444`.
- Shared built-in index (the model to copy): `guc.c:834`, `:2204-2245`,
  `:2249-2266`.
- `build_guc_variables` (built-ins only): `guc.c:1771-1804`.
- `add_guc_variable` / `ensure_guc_custom_hashtab`: `guc.c:1861-1886`,
  `:2478-2496`.
- `define_custom_variable` / `init_custom_variable` / `DefineCustom*`:
  `guc.c:6575`, `:6510`, `:6793` ff.
- Custom record `->state` routing (why (c) races): `GUCRecordState`
  `guc.c:326-341`, `GUCRecordIsCurrentSessionBuiltin` `guc.c:257-275`, macros
  `guc.c:499-543`, struct `guc_tables.h:311-355`, state struct `guc_tables.h:186-195`.
- Postmaster preload once: `postmaster.c:632` (GUC init), `:1097` (preload).
- Threaded session bring-up (seed point): `InitializeThreadedSessionGUCOptions`
  `guc.c:2730-2758`; rebound-init `guc.c:2843-2857`; carrier call
  `launch_backend.c:2055-2056`, `postinit.c:1035`.
- Config replay creates placeholders: `read_nondefault_variables` `guc.c:7435`,
  `find_option(..., create_placeholders=true)` `guc.c:7468`.
- EXEC_BACKEND re-runs preload (contrast): `launch_backend.c:3345`.
- Session/local preload (out of scope, in-session): `postinit.c:1367`.
- Per-session VALUE cell pattern: `PgSessionEnsureExtensionPrivateState`
  `backend_runtime_session.c:2525-2555`; see `CUSTOM_GUC_FIX_DESIGN.md`.
