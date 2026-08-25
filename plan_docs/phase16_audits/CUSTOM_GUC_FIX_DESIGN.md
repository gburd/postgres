# Custom-GUC per-session correctness — fix design

Read-only investigation. No source files were modified.

All line numbers are from `src/backend/utils/misc/guc.c` at HEAD
(`b0f9952b4e`) unless another file is named.

---

## (a) Confirmed bug + exact write sites

### Storage model

A custom GUC's live value pointer is `record->state->variable.<type>`:

- `GUC_VARIABLE_BOOL(record)` = `GUC_STATE(record)->variable.boolvar` (guc.c:537-541).
- `GUC_STATE(record)` = `GUCRecordState(record)` (guc.c:499). For a **custom**
  GUC `GUCRecordIsCurrentSessionBuiltin()` is false (the record was
  `guc_malloc`'d in `init_custom_variable`, not inside the per-session
  `guc_variables` array), so `GUCRecordState` returns `record->state`
  (guc.c:307-308) — a per-session heap struct.

But `variable.boolvar` itself is set at registration to the caller's address:

- `DefineCustomBoolVariable(..., bool *valueAddr, ...)` →
  `GUC_VARIABLE_BOOL(var) = valueAddr;` (guc.c:6809). Same for Int (6835),
  Real (6863), String (6889), Enum (6913).

So the per-session record and its `state` are per-session, **but
`state->variable.boolvar` points at the single extension-owned C global**
passed as `valueAddr`. Every session's record for `custom.foo` stores the same
`valueAddr`. Two sessions writing through their own records write the same
word.

### Trace: `SET custom.foo = true` in a threaded session (BOOL)

`set_config_option_ext` → `set_config_with_handle` → `do_assign` (the big
`switch` on `record->vartype`). The BOOL `changeVal` block:

```
guc.c:5340   if (!makeDefault) push_old_value(record, action);
guc.c:5344   if (conf->assign_hook) conf->assign_hook(newval, newextra);
guc.c:5346   *GUC_VARIABLE_BOOL(record) = newval;      // UNGUARDED
```

`*GUC_VARIABLE_BOOL(record)` dereferences the shared `valueAddr`. No
`PgCurrentOrEarlySessionOwnsPointer` guard. Session A `SET custom.foo=true`
and session B `SET custom.foo=false` on the same carrier write the same
`bool`; the reader `if (auto_explain_log_analyze)` in whichever session runs
next sees the other session's value.

### Every unguarded write site, by type

do_assign (interactive SET / RESET-to-value):
- BOOL  `*GUC_VARIABLE_BOOL(record)  = newval;` — guc.c:5346
- INT   `*GUC_VARIABLE_INT(record)   = newval;` — guc.c:5442
- REAL  `*GUC_VARIABLE_REAL(record)  = newval;` — guc.c:5538
- ENUM  `*GUC_VARIABLE_ENUM(record)  = newval;` — guc.c:5820

`ResetAllOptions` (RESET ALL):
- BOOL guc.c:3561, INT 3573, REAL 3585, STRING (via `set_string_field`) 3597,
  ENUM 3610 — all unconditional.

`AtEOXact_GUC` stack-restore on xact end/abort:
- BOOL guc.c:3942, INT 3960, REAL 3978, STRING (via `set_string_field`) ~3995,
  ENUM 4024 — all unconditional (**including STRING here**).

`InitializeOneGUCOption` boot-value write (runs from
`define_custom_variable` → `InitializeOneGUCOption`, guc.c:6603/6620):
- BOOL/INT/REAL/STRING/ENUM `*GUC_VARIABLE_x(gconf) = GUC_RESET_x(gconf) = newval;`
  — guc.c:3063 / 3079 / 3095 / 3118 / 3130 — all unconditional.

**Confirmed:** BOOL/INT/REAL/ENUM (and STRING on the RESET-ALL, AtEOXact, and
InitializeOneGUCOption paths) write the shared `valueAddr` with no ownership
guard.

---

## (b) Is STRING's guard correct, or merely non-corrupting? And how does a
direct C-global read see the right per-session value?

### What the STRING guard actually is

`PgCurrentOrEarlySessionOwnsPointer(ptr)` (backend_runtime_session.c:2089-2107)
returns true **iff `ptr` lands inside the current `PgSession` struct** (or the
early-session fallback). It is an address-range test on `[CurrentPgSession,
CurrentPgSession + sizeof(PgSession))`.

The three STRING call sites:

1. `InitializeThreadedSessionReboundGUCOptions` (guc.c:2842-2858): for **every**
   vartype, if `!OwnsPointer(variable)` it calls
   `InitializeOneGUCOptionResetMetadata` (no live write) instead of
   `InitializeOneGUCOption`. This is type-agnostic init-time protection.
2. `InitializeOneGUCOption`'s STRING arm (guc.c:3207): if `!OwnsPointer` it
   snapshots the *existing* global into `GUC_RESET_STRING` and skips the boot
   write.
3. `do_assign` STRING (guc.c:5596-5598):
   `assign_variable = !GUCThreadedBackendReplayActive(is_reload) || OwnsPointer(...)`.
   `GUCThreadedBackendReplayActive` (guc.c:580-587) is **only** true when
   `is_reload` (SIGHUP/config replay). On an ordinary interactive `SET`,
   `is_reload` is false, so `assign_variable` is **true** and STRING writes the
   pointer **unconditionally**, exactly like BOOL/INT/REAL/ENUM.

**Definitive answer:** The STRING "guard" is **not** a general per-session SET
guard. It protects two things only — (i) building a copied GUC table without
clobbering process-global backing during session bring-up, and (ii) config
**replay**. For an ordinary user `SET`, STRING on a genuinely
extension-owned-global custom GUC would corrupt exactly like the numeric types.
The guard is neither "correct per-session" nor even "reliably non-corrupting"
for interactive SET; it is orthogonal (it gates *replay/bring-up*, not *SET*).

If the guard *did* gate interactive SET, it would only be non-corrupting, not
correct: a non-owning session's SET would be silently dropped, leaving that
session unable to change its own value. So a bare guard is the wrong fix for
any type.

### How a direct C-global read sees the right per-session value

Not via a guard and not via the builtin rebind registry. The pattern this fork
already uses — and the reason STRING is actually safe for the bundled
extensions — is: **make the extension's "global" resolve to per-session
storage, then hand the address of that per-session cell to `DefineCustom*` as
`valueAddr`.** Both the extension's read and guc.c's write then hit the same
per-session cell, and `OwnsPointer` becomes true because the cell lives inside
`PgSession`.

Concretely (auto_explain, guc.c-adjacent contrib):

```c
/* contrib/auto_explain/auto_explain.c:106-136 */
static AutoExplainSessionState *auto_explain_session_state(void) {
    return PgSessionEnsureExtensionPrivateState(AUTO_EXPLAIN_SESSION_STATE_KEY,
                                                sizeof(AutoExplainSessionState), NULL);
}
/* auto_explain.c:138-143 — the "global" is a macro over per-session storage */
#define auto_explain_log_analyze (auto_explain_session_state()->log_analyze_value)
```

`DefineCustomBoolVariable(..., &auto_explain_log_analyze, ...)` (auto_explain.c:266)
takes the address of the *current session's* cell. `if (auto_explain_log_analyze)`
(auto_explain.c:453) reads the same cell. `pg_stat_statements` uses the identical
pattern (`#define pgss_track (pgss_session_state()->track)`,
pg_stat_statements.c:390; `&pgss_track`, pg_stat_statements.c:512). The test
module documents the rule explicitly and warns that plain `__thread`/TLS is
WRONG because a fiber can run on any carrier
(test_backend_runtime_threaded.c:70-78; backing ref
backend_runtime_session.c:3156-3159).

**So STRING is "correct" only when the extension has already been converted to
per-session backing.** It is not the guard that makes it correct; it is the
per-session `valueAddr`. The guard just keeps bring-up/replay from touching a
not-yet-owned pointer. The numeric types are missing nothing that STRING has for
the *SET* path — both are safe once and only once `valueAddr` is per-session.

---

## (c) Recommended fix for BOOL/INT/REAL/ENUM

**Recommendation: option (b), per-session backing storage — for the extension,
not guc.c. Do NOT add ownership guards to the numeric do_assign paths, and do
NOT extend the builtin rebind registry to custom GUCs.**

Reasoning:

- A guard in guc.c (option a) is wrong for the reason in (b): it either
  corrupts on the unguarded paths you didn't guard, or (if it gated SET) it
  silently drops a non-owning session's own SET. It also cannot make the
  extension's direct `if (global)` read see the right value — the read
  bypasses guc.c entirely.
- The builtin rebind registry (`RebindSessionGUCVariablePointer`, guc.c:2888;
  `threaded_accessor` in guc_parameters.dat) repoints `valueAddr` to
  per-session storage on session switch. That is the builtin equivalent of the
  same idea, but it is driven by a static in-tree table keyed by GUC name; an
  out-of-tree extension cannot add rows to it, and it needs the backing field
  to already exist in `PgSession`. It is the right mechanism for **builtins**
  and unnecessary indirection for extensions, which can point `valueAddr`
  straight at their own per-session cell at `DefineCustom*` time.
- Per-session backing (option b) is the mechanism already in the tree for
  exactly this problem, already proven on auto_explain, pg_stat_statements,
  plperl, and the test module. It makes the read and the write agree with zero
  guc.c changes.

### Concrete code shape (per bundled/affine extension)

For each session-scoped custom GUC (PGC_USERSET / PGC_SUSET — **not**
PGC_POSTMASTER runtime-globals like `pg_stat_statements.max`, which are set once
and legitimately process-wide):

```c
/* 1. per-session state struct + accessor via the extension-module slot */
typedef struct FooSessionState { bool initialized; bool bar_value; int baz_value; } FooSessionState;
static FooSessionState *foo_session_state(void) {
    FooSessionState *s = PgSessionEnsureExtensionPrivateState(FOO_SESSION_KEY,
                                                              sizeof(*s), NULL);
    if (!s->initialized) { /* boot defaults */ s->initialized = true; }
    return s;
}
/* 2. the "global" becomes a macro over per-session storage */
#define foo_bar (foo_session_state()->bar_value)
#define foo_baz (foo_session_state()->baz_value)

/* 3. registration hands guc.c the per-session cell address */
DefineCustomBoolVariable("foo.bar", ..., &foo_bar, ...);   /* &(...) is legal: macro is an lvalue */
DefineCustomIntVariable ("foo.baz", ..., &foo_baz, ...);
```

Because the cell lives inside `PgSession`, `PgCurrentOrEarlySessionOwnsPointer`
returns true, so the existing STRING guard sites and the
`InitializeThreadedSessionReboundGUCOptions` init split all do the right thing
automatically — and the unguarded numeric writes are now writing a per-session
cell, so they are correct with **no guc.c change at all**.

guc.c is left alone. The fix lives entirely in each extension's `_PG_init`
storage decisions plus one `PgSession.extension_modules` field per value. This
is the pattern the plan already records as landed for auto_explain
(`MULTITHREADED_PLAN.md:646`) and pg_plan_advice/pg_stash_advice
(`:653`).

**One guc.c caveat worth a follow-up (not required for correctness):** the
AtEOXact STRING restore (guc.c ~3995) and RESET-ALL paths are unguarded for all
types. Once every custom GUC in a carrier is per-session-backed these are
correct (they write per-session cells). They would only bite a custom GUC that
is *still* extension-global — which the reachability gate (d) already forbids
from loading. So no extra guard is needed; the invariant is "a custom GUC that
loads under threaded has per-session `valueAddr`," enforced by (d).

---

## (d) Reachability / prerequisite conclusion

The extension backend-model gate is fail-closed:

- `PG_MODULE_MAGIC` / `PG_MODULE_MAGIC_EXT` default `backend_model` to
  `PG_BACKEND_MODEL_PROCESS = 0` (fmgr.h:487, 541-558; comment at 557-559
  states the default is deliberately process-only).
- Under a threaded runtime `internal_load_library` →
  `check_module_backend_model` (dfmgr.c:320-334, 592-607) refuses to keep a
  module whose declared model is weaker than
  `PgRuntimeGetExtensionBackendModel()`, throwing
  "library is not supported in the threaded backend runtime" and `dlclose`-ing
  it (dfmgr.c:643-651). The module never enters the carrier address space.

Bundled extensions today:
- `auto_explain` — `PG_MODULE_MAGIC_EXT(.name, .version)` with **no**
  `PG_MODULE_MAGIC_BACKEND_MODEL_*` (auto_explain.c:31-34) → PROCESS.
- `pg_stat_statements` — same, no model field (pg_stat_statements.c:75) →
  PROCESS.

So both are currently PROCESS and are refused under `multithreaded=on`. **The
custom-GUC corruption is latent**: no session can load these into a carrier
today, so no two sessions share their `valueAddr` in one address space yet.

**Conclusion — the fix IS a prerequisite for Phase-16 Tier-1 affine markings.**
The moment an extension's `PG_MODULE_MAGIC` is bumped to
`POOLED_PROTOCOL_AFFINE` (or thread-per-session), the gate lets it load into a
shared carrier and every session's record starts sharing the one `valueAddr`.
The per-session-backing conversion (c) must land **before** the affine marking,
per extension. auto_explain has already had its storage converted
(MULTITHREADED_PLAN.md:646) but has **not** yet been marked affine — which is
exactly the correct ordering: convert first, mark second. Marking an
unconverted extension affine would ship the corruption.

Ordering rule for each Tier-1 extension:
1. Convert every session-scoped custom GUC to per-session backing (c).
2. Verify with the (e) isolation test.
3. Only then bump the `PG_MODULE_MAGIC` backend model.

---

## (e) Test plan — two sessions on one carrier, isolation

Reuse the existing `test_backend_runtime_threaded` module, which already backs
its custom GUC per-session (test_backend_runtime_threaded.c:76-78,
DefineCustomStringVariable at :195). Add a numeric GUC to prove the BOOL/INT
path, since STRING alone would not have caught the numeric bug.

Setup (isolation-style, forcing both sessions onto one carrier):
- Build threaded; run under a 1-carrier pool
  (`gmake check-threaded-pooled` harness, carriers=1) so two sessions are
  guaranteed to time-share the same OS thread / address space.
- Mark the test module affine for the duration of the test (it is in-tree, so
  it can carry the affine `PG_MODULE_MAGIC` without shipping a third-party
  regression).

Steps:
1. Session A: `LOAD 'test_backend_runtime_threaded';`
   `SET test_backend_runtime_threaded.custom_guc = 'A';`
   `SET test_backend_runtime_threaded.custom_int = 111;`  (add a per-session INT
   GUC mirroring the STRING one)
2. Session B (same carrier): `LOAD ...;`
   `SET ... .custom_guc = 'B';`  `SET ... .custom_int = 222;`
3. Interleave reads by yielding between the sets (the harness already forces
   fiber switches at wait boundaries; a `pg_sleep(0)` or a round-trip per
   statement suffices):
   - A: `SELECT test_backend_runtime_custom_guc_value();` → must be `'A'`.
   - B: `SELECT test_backend_runtime_custom_guc_value();` → must be `'B'`.
   - A: `SHOW test_backend_runtime_threaded.custom_int;` → `111`.
   - B: `SHOW test_backend_runtime_threaded.custom_int;` → `222`.
4. Negative control (regression guard): temporarily register the INT GUC with a
   plain file-scope `static int` `valueAddr` instead of the per-session macro;
   the test must FAIL (cross-session bleed: A reads 222 or B reads 111). This
   proves the test actually exercises the shared-`valueAddr` hazard. Revert the
   control before commit.
5. RESET / xact-abort coverage: in A, `BEGIN; SET ... custom_int = 999;
   ROLLBACK;` then confirm A reads `111` and B still reads `222` — exercises the
   unguarded `AtEOXact_GUC` restore (guc.c:3960) and `ResetAllOptions`
   (guc.c:3573) on per-session storage.

Assertions live as an isolation spec (`src/test/modules/test_backend_runtime`,
split test files per AGENTS.md) plus the existing
`test_backend_runtime_custom_guc_value()` SQL function
(test_backend_runtime_threaded.c:446-451). Validation targets:
`gmake check-threaded-pooled` and `gmake check-threaded-workers`.

Expected result with the fix: full isolation (step 3 passes, step 4 control
fails as designed). Without per-session backing (step 4 as the real code):
values bleed across sessions.
