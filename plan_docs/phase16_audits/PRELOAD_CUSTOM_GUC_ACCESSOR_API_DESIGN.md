# Preload custom-GUC per-session accessor API — corrected fix design

Read-only investigation. No source files were modified. Line numbers are from
the working tree at design time (branch `xtc`).

Supersedes the seed-only design in `PRELOAD_CUSTOM_GUC_FIX_DESIGN.md`, whose
`## Adversarial review` correctly found that seeding a descriptor alone leaves a
frozen postmaster-heap `valueAddr` and makes `SHOW`/`SET`/`pg_settings` silently
wrong (cross-session race on one shared cell). This doc owns the missing half:
the **per-session value-pointer rebind**, driven by an extension-supplied
accessor, mirroring the built-in `RebindSessionGUCVariablePointer` mechanism.

Companions:
- `PRELOAD_CUSTOM_GUC_FIX_DESIGN.md` — descriptor-registry seed (§3 option a).
  Retained for the registry/seed shape; its ownership-branch claim is corrected
  here.
- `CUSTOM_GUC_FIX_DESIGN.md` — per-session `valueAddr` via
  `PgSessionEnsureExtensionPrivateState`. That makes the *extension's macro read*
  per-session. It does NOT re-point the *descriptor* the postmaster froze; that
  is this doc.
- `.ec2/preload-custom-guc-threaded-gap.md` — measured A/B symptom.

---

## 0. One-paragraph statement of the corrected fix

The built-in threaded GUCs re-derive their per-session value cell on session
bring-up: `RebindSessionGUCVariablePointer` (`guc.c:2887-2913`) calls a
registered accessor (`rebind->accessor.bool_ref()` etc.) that returns the
CURRENT session's cell, driven by the `threaded_accessor` column in
`guc_parameters.dat` (`gen_guc_tables.pl:252-258`). Custom GUCs have no such
accessor: `DefineCustom*` captures a raw `valueAddr` once
(`guc.c:6809/6835/6863/6889/6913`), which at postmaster-preload time
(`CurrentPgSession == NULL`) is a frozen `TopMemoryContext` early-session heap
block. The fix adds a small **extension-facing accessor registration** for
custom GUCs, stores that accessor on the shared preload registry, and the
per-session seed rebinds `GUC_VARIABLE_<T>(var) = accessor()` — exactly the
built-in code path, reused verbatim. Custom GUCs that register no accessor are
**fail-closed**: refused under threaded (their state is process-global by
construction, so sharing one cell across sessions is a data race).

---

## 1. Extension-facing API

### 1.1 Decision: option (b), a companion registration call — `RegisterCustomGUCSessionAccessor`

Read the three candidate shapes against the existing types:

- The accessor union already exists and is exactly the right shape:
  `ThreadedSessionGUCVariableAccessor` (`guc_tables.h:32-39`) is a `union` of
  `bool *(*bool_ref)(void)`, `int *(*int_ref)(void)`, `double *(*real_ref)(void)`,
  `char **(*string_ref)(void)`, `int *(*enum_ref)(void)`. This is precisely
  "a function returning the current session's typed cell". Reuse it — do not
  invent a parallel type.

- The built-in registry entry `ThreadedSessionGUCRebind` (`guc_tables.h:41-46`)
  is `{ name, vartype, accessor }`. The custom registry entry is the same triple
  plus a descriptor pointer.

**Rejected (a) — `DefineCustom*Threaded` variants.** Adds five new 12-arg public
functions duplicating five existing ones, forces every caller to switch entry
point, and doubles the ABI surface. The accessor is a single extra pointer; a
whole variant family for one pointer is over-engineering.

**Rejected (c) — extend the generated `threaded_accessor`/`guc_parameters.dat`
machinery to customs.** That table is compile-time, in-tree, keyed by GUC name,
emitted by `gen_guc_tables.pl` into `PG_GLOBAL_IMMUTABLE ThreadedSessionGUCRebinds[]`
(`gen_guc_tables.pl:247-264`). An out-of-tree extension cannot add rows to a
generated in-tree table. Non-starter for the general case; only usable for
in-tree customs, which is a strictly smaller set than we must support.

**Chosen (b): one new public call the extension makes right after its
`DefineCustom*`.** Smallest surface: no new Define variants, no ABI churn to the
five existing entry points, works for out-of-tree modules, and the accessor
reuses the existing union. The registration looks up the just-defined descriptor
by name and staples the accessor onto it.

### 1.2 Signatures (additions only)

Public header `src/include/utils/guc.h` (near the `DefineCustom*` block,
`guc.h:502-560`):

```c
/*
 * Register a per-session value accessor for a custom GUC previously created
 * with DefineCustom<Type>Variable().  Under a threaded runtime the accessor is
 * called on each session's GUC bring-up to rebind the descriptor's live value
 * pointer at the CURRENT session's storage cell, exactly as built-in threaded
 * GUCs are rebound (see RebindSessionGUCVariablePointer).  The accessor must
 * return the address of the same per-session cell the extension itself reads
 * (typically via PgSessionEnsureExtensionPrivateState).  No-op under process
 * mode.  Must be called during shared_preload_libraries processing, from the
 * same _PG_init that defined the GUC.
 */
extern void RegisterCustomGUCSessionAccessor(const char *name,
											 bool *(*accessor) (void));   /* PGC_BOOL   */
extern void RegisterCustomGUCSessionAccessorInt(const char *name,
											 int *(*accessor) (void));    /* PGC_INT    */
extern void RegisterCustomGUCSessionAccessorReal(const char *name,
											 double *(*accessor) (void)); /* PGC_REAL   */
extern void RegisterCustomGUCSessionAccessorString(const char *name,
											 char **(*accessor) (void));  /* PGC_STRING */
extern void RegisterCustomGUCSessionAccessorEnum(const char *name,
											 int *(*accessor) (void));    /* PGC_ENUM   */
```

Five typed entry points (not one `void*`) so the compiler type-checks the
accessor return against the GUC's declared type, matching the union arms in
`guc_tables.h:34-38`. Internally they funnel to one static worker keyed on
`vartype`:

```c
/* guc.c, near define_custom_variable (guc.c:6575) */
static void
register_custom_guc_session_accessor(const char *name,
									 enum config_type vartype,
									 ThreadedSessionGUCVariableAccessor accessor)
{
	struct config_generic *var;

	/* Only meaningful under a threaded runtime; harmless no-op otherwise. */
	if (!IsMultithreaded())            /* see §1.4 gating */
		return;

	if (!process_shared_preload_libraries_in_progress)
		elog(FATAL,
			 "RegisterCustomGUCSessionAccessor must be called during "
			 "shared_preload_libraries processing");

	var = find_option(name, false, false, ERROR);   /* real descriptor, no placeholder */
	if (var->vartype != vartype)
		elog(ERROR, "custom GUC \"%s\" accessor type mismatch", name);
	if (var->group != CUSTOM_OPTIONS)                /* reject built-ins */
		elog(ERROR, "\"%s\" is not a custom GUC", name);

	CustomGUCAttachSessionAccessor(var, accessor);   /* §2 */
}
```

The five public wrappers are one line each, e.g.:

```c
void
RegisterCustomGUCSessionAccessor(const char *name, bool *(*accessor)(void))
{
	ThreadedSessionGUCVariableAccessor a = { .bool_ref = accessor };
	register_custom_guc_session_accessor(name, PGC_BOOL, a);
}
```

### 1.3 Why not fold the accessor into `DefineCustom*` as a defaulted arg

C has no defaulted args; folding it in is option (a) by another name (breaks the
five signatures / ABI). The separate call keeps `DefineCustom*` byte-compatible
for every existing extension and makes accessor registration an explicit,
greppable opt-in — which is exactly what the fail-closed policy (§6) keys on.

### 1.4 Gating

`RegisterCustomGUCSessionAccessor*` is a no-op when the runtime is not threaded
(`IsMultithreaded()` false) so an extension can call it unconditionally in its
`_PG_init` and stay correct in process mode. Under process mode the frozen
`valueAddr` is inherited by fork and is correct; no rebind is needed or wanted.

---

## 2. Where the accessor is stored

The accessor must survive `postmaster _PG_init` → shared registry → per-session
seed. Two structs are in play:

- `struct config_generic` (`guc_tables.h:311-359`) — the descriptor. Its body is
  effectively immutable after registration.
- `struct config_generic_state` (`guc_tables.h:186-195`) — per-session mutable
  state (`variable`, `reset_val`, `status`, `source`, `scontext`, `srole`,
  `cold`). For customs this is a separate `guc_malloc`'d block
  (`init_custom_variable`, `guc.c:6551-6557`).

The accessor is **immutable, per-GUC, shared across sessions** — same lifetime
class as the descriptor body, NOT per-session. So it belongs on
`config_generic`, not on `config_generic_state`.

Add one field to `struct config_generic` (`guc_tables.h`, in the
"constant fields" region near `state`, `guc_tables.h:319-323`):

```c
	/*
	 * Per-session value accessor for a threaded custom GUC.  NULL for built-ins
	 * (they use ThreadedSessionGUCRebinds[]) and for custom GUCs that did not
	 * register one.  Set once during preload; immutable thereafter.
	 */
	ThreadedSessionGUCVariableAccessor session_accessor;   /* union of *_ref */
	bool		has_session_accessor;
```

`init_custom_variable` (`guc.c:6551-6570`) already `memset(gen, 0, ...)`, so
`has_session_accessor` defaults false with no code change.

Attach helper (guc.c):

```c
static void
CustomGUCAttachSessionAccessor(struct config_generic *var,
							   ThreadedSessionGUCVariableAccessor accessor)
{
	var->session_accessor = accessor;
	var->has_session_accessor = true;
}
```

Because the descriptor is copied whole into the shared preload registry
(`preload_custom_gucs[]`, `PRELOAD_CUSTOM_GUC_FIX_DESIGN.md` §3), the accessor
travels with it automatically. The per-session seed reads
`tmpl->session_accessor` — no separate parallel array.

---

## 3. Core seed + rebind

Two core pieces, both `multithreaded`-guarded and inert in process mode.

### 3.1 `SnapshotPreloadCustomGUCs()` — postmaster, once

Unchanged in structure from `PRELOAD_CUSTOM_GUC_FIX_DESIGN.md` §3: at the end of
`process_shared_preload_libraries()` (`miscinit.c:1896-1904`, right after
`process_shared_preload_libraries_done = true`, guarded by `IsMultithreaded()`),
snapshot every non-placeholder custom descriptor pointer into a
`PG_GLOBAL_RUNTIME struct config_generic **preload_custom_gucs` in
`TopMemoryContext`. The descriptor now carries `session_accessor` /
`has_session_accessor` (§2), so nothing extra to capture.

Add here the fail-closed check (§6): any snapshotted custom GUC that is
session-scoped (`PGC_USERSET`/`PGC_SUSET`) and has `!has_session_accessor` is
either FATAL at preload (strict) or flagged so its owning module is refused at
load (see §6 for which). Runtime-scoped customs (`PGC_POSTMASTER`/`PGC_SIGHUP`)
need no accessor (§4.2) and are exempt.

### 3.2 `SeedPreloadCustomGUCs()` — per session, inside `InitializeThreadedSessionGUCOptions`

Call site: inside `InitializeThreadedSessionGUCOptions()` (`guc.c:2730-2762`),
after `build_guc_variables()` and the existing built-in rebind pass
(`guc.c:2748-2751`):

```c
		build_guc_variables();
		RebindSessionGUCVariablePointers();
		InitializeThreadedSessionReboundGUCOptions();
		SeedPreloadCustomGUCs();                     /* NEW: after built-ins */
		InitializeThreadedSessionCompatibilityGUCOptions();
```

Ordering matters: it must run before `read_nondefault_variables()` config replay
(`launch_backend.c:2055` then `:2056`) so the real descriptor is present before
replay would otherwise manufacture a placeholder — that ordering is satisfied
because the seed lives inside `InitializeThreadedSessionGUCOptions`.

```c
/* guc.c */
static void
SeedPreloadCustomGUCs(void)
{
	for (int i = 0; i < num_preload_custom_gucs; i++)
	{
		struct config_generic *tmpl = preload_custom_gucs[i];
		struct config_generic *var;
		config_generic_state  *state;

		if (find_option(tmpl->name, false, true, DEBUG5) != NULL)
			continue;                       /* already present this session */

		/* Fresh per-session descriptor + state in this session's GUCMemoryContext. */
		var  = guc_malloc(ERROR, sizeof(*var));
		*var = *tmpl;                        /* immutable body incl. hooks,
											  * boot_val, min/max, enum table,
											  * session_accessor */
		state = guc_malloc(ERROR, sizeof(*state));
		memset(state, 0, sizeof(*state));
		var->state = state;

		/*
		 * THE MISSING HALF (this doc).  Re-derive the per-session value cell
		 * from the extension's accessor, exactly like the built-in path in
		 * RebindSessionGUCVariablePointer (guc.c:2887-2913).  Do this BEFORE
		 * InitializeOneGUCOption so the ownership guard sees the per-session
		 * cell and boots THAT cell (not the frozen postmaster cell).
		 */
		if (var->has_session_accessor)
		{
			switch (var->vartype)
			{
				case PGC_BOOL:
					GUC_VARIABLE_BOOL(var)   = var->session_accessor.bool_ref();
					break;
				case PGC_INT:
					GUC_VARIABLE_INT(var)    = var->session_accessor.int_ref();
					break;
				case PGC_REAL:
					GUC_VARIABLE_REAL(var)   = var->session_accessor.real_ref();
					break;
				case PGC_STRING:
					GUC_VARIABLE_STRING(var) = var->session_accessor.string_ref();
					break;
				case PGC_ENUM:
					GUC_VARIABLE_ENUM(var)   = var->session_accessor.enum_ref();
					break;
			}
		}

		/*
		 * Now the standard init split.  For a rebound session-scoped custom,
		 * GUCOptionVariablePointer(var) is now the per-session cell (inside
		 * PgSession or its extension-module private-state list), so
		 * PgCurrentOrEarlySessionOwnsPointer() is TRUE and we boot that cell.
		 * For a runtime-scoped custom with no accessor, the frozen runtime
		 * pointer is not session-owned, so we only re-init reset metadata and
		 * never clobber the process-wide value (correct: see §4.2).
		 */
		if (!PgCurrentOrEarlySessionOwnsPointer(GUCOptionVariablePointer(var)))
			InitializeOneGUCOptionResetMetadata(var);   /* guc.c:3148 */
		else
			InitializeOneGUCOption(var);                /* guc.c:3038 */

		add_guc_variable(var, ERROR);                    /* guc.c:1861 */
	}
}
```

Why this is now correct where the old design was not: the adversarial review
proved `*var = *tmpl` carries a frozen `TopMemoryContext` early-session pointer,
so `PgCurrentOrEarlySessionOwnsPointer` returns FALSE and the old code took the
reset-metadata-only arm, leaving `variable` pointing at the postmaster cell that
every session's `SHOW`/`SET` then raced. Here the accessor call
`var->session_accessor.bool_ref()` returns
`&CurrentPgSession->...extension private state cell` (whatever the extension's
`PgSessionEnsureExtensionPrivateState` resolves to for THIS session), so the
guard flips TRUE and the session boots and reads/writes its own cell. This is
the identical happens-before and identical mechanism as the built-in rebind
(`guc.c:2895-2911`); no new policy.

### 3.3 `PgCurrentOrEarlySessionOwnsPointer` and extension private state

Caveat worth noting for the implementer: `PgCurrentOrEarlySessionOwnsPointer`
(`backend_runtime_session.c:2089-2107`) is an address-range test against the
`PgSession` struct (and the early fallback). A cell reached through
`CurrentPgSession->extension_modules` `List` is a separate heap block, NOT inside
the `PgSession` struct — so the guard could still be FALSE for a validly rebound
extension cell. Two resolutions, implementer picks one and records it:

- **(preferred) Boot unconditionally after a successful accessor rebind.** If
  `has_session_accessor` is true, the accessor by contract returned the current
  session's cell; call `InitializeOneGUCOption(var)` directly (skip the
  ownership test). The ownership test remains for the no-accessor runtime-scoped
  case. This is the cleanest and matches the built-in intent (built-ins rebind
  then unconditionally treat the cell as session-owned).
- (alt) Extend `PgCurrentOrEarlySessionOwnsPointer` to also accept addresses
  owned by the current session's extension-module private-state list. Larger
  blast radius (touches a hot predicate used by numeric SET guards); avoid
  unless the boot-unconditionally path proves insufficient.

Recommend the preferred path: in `SeedPreloadCustomGUCs`, if
`var->has_session_accessor`, call `InitializeOneGUCOption(var)` directly;
otherwise fall to the ownership split above.

---

## 4. What the extension must change — pg_stat_statements

pg_stat_statements has 5 custom GUCs (`pg_stat_statements.c:496-556`). The
storage split is already in place (`CUSTOM_GUC_FIX_DESIGN.md` landed the
per-session macros):

| GUC | context | macro backing (`pg_stat_statements.c`) | scope | needs accessor? |
|---|---|---|---|---|
| `pg_stat_statements.max` | PGC_POSTMASTER | `pgss_runtime_state()->max` (`:386`) | runtime/process | **NO** (§4.2) |
| `pg_stat_statements.save` | PGC_SIGHUP | `pgss_runtime_state()->save` (`:387`) | runtime/process | **NO** (§4.2) |
| `pg_stat_statements.track` | PGC_SUSET | `pgss_session_state()->track` (`:390`) | session | **YES** |
| `pg_stat_statements.track_utility` | PGC_SUSET | `pgss_session_state()->track_utility` (`:391`) | session | **YES** |
| `pg_stat_statements.track_planning` | PGC_SUSET | `pgss_session_state()->track_planning` (`:392`) | session | **YES** |

### 4.1 The three session-scoped GUCs — register accessors

The accessor must return the address of the SAME per-session cell the extension
macro reads. `pgss_track` = `pgss_session_state()->track` (an `int` enum). So:

```c
/* pg_stat_statements.c — near the pgss_session_state() definition (:330) */
static int  *pgss_track_ref(void)          { return &pgss_session_state()->track; }
static bool *pgss_track_utility_ref(void)  { return &pgss_session_state()->track_utility; }
static bool *pgss_track_planning_ref(void) { return &pgss_session_state()->track_planning; }
```

Then, immediately after each `DefineCustom*` in `_PG_init`
(`pg_stat_statements.c:509-556`):

```c
	DefineCustomEnumVariable("pg_stat_statements.track", ...,
							 &pgss_track, ... );
	RegisterCustomGUCSessionAccessorEnum("pg_stat_statements.track",
										 pgss_track_ref);

	DefineCustomBoolVariable("pg_stat_statements.track_utility", ...,
							 &pgss_track_utility, ... );
	RegisterCustomGUCSessionAccessor("pg_stat_statements.track_utility",
									 pgss_track_utility_ref);

	DefineCustomBoolVariable("pg_stat_statements.track_planning", ...,
							 &pgss_track_planning, ... );
	RegisterCustomGUCSessionAccessor("pg_stat_statements.track_planning",
									 pgss_track_planning_ref);
```

The `&pgss_track` passed to `DefineCustom*` at preload is still the frozen
early-session cell — that is fine, the accessor overrides it per session. (One
could pass `NULL` valueAddr instead, but the five entry points are
`pg_attribute_nonnull(1, 4)` — `guc.h:511` — so keep passing the macro address;
it is only used until the first per-session rebind and never in a client
session.)

Note `pgss_track` is an ENUM stored as `int` — use
`RegisterCustomGUCSessionAccessorEnum` (returns `int*`), matching the union arm
`enum_ref` (`guc_tables.h:38`) and the ENUM rebind arm (`guc.c:2895-2911`).

### 4.2 The two runtime-scoped GUCs — NO accessor, frozen pointer is correct

`max` (PGC_POSTMASTER) and `save` (PGC_SIGHUP) back onto
`pgss_runtime_state()` = `PgRuntimeEnsureExtensionPrivateState(...)`
(`pg_stat_statements.c:311-320`), i.e. **per-carrier/process-wide** state, not
per-session. That is semantically correct: `max` is fixed at postmaster start
and shared by the whole instance; `save` is a SIGHUP-wide setting. They are set
once (POSTMASTER) or process-globally (SIGHUP) and read by all sessions on the
carrier — a single shared cell is the CORRECT model, not a bug.

Confirm the mechanism: for `max`/`save` the seed finds `!has_session_accessor`,
so `SeedPreloadCustomGUCs` takes the ownership-split path. The frozen pointer is
the runtime cell; `PgCurrentOrEarlySessionOwnsPointer` is FALSE (it is not inside
`PgSession`), so `InitializeOneGUCOptionResetMetadata` runs — reset/pg_settings
metadata is valid, and the live value is left as the postmaster-established
runtime value, shared read-only-ish by all sessions. A PGC_POSTMASTER GUC cannot
be `SET` by a session anyway, and PGC_SIGHUP is process-wide by definition, so no
cross-session `SET` race exists. **So: runtime-scoped customs deliberately skip
the accessor; the frozen process-wide pointer is the intended behavior.**

Caveat for the implementer: the one shared runtime cell is now shared across all
sessions on a carrier and, if multiple carriers exist, each carrier has its own
`PgRuntimeEnsureExtensionPrivateState` — matches process-per-carrier semantics.
`max` sizing the shared hash is a carrier-lifetime value; that is consistent with
"runtime = per address space".

---

## 5. Placeholders, RESET/AtEOXact, pg_settings/SHOW correctness

### 5.1 SHOW / SET / pg_settings correctness with the rebound pointer

After the rebind, the descriptor's `variable` IS the current session's cell:

- `SHOW pg_stat_statements.track` → `ShowGUCOptionInternal` →
  `config_enum_lookup_by_value(record, *GUC_VARIABLE_ENUM(record))`
  (`guc.c:7251-7252`) — reads the session cell. Correct.
- `SET pg_stat_statements.track='all'` → `do_assign` ENUM arm
  `*GUC_VARIABLE_ENUM(record) = newval;` (`guc.c:5820`) — writes the session
  cell. Correct and per-session isolated; no shared-word race.
- `pg_settings` iterates the session's `guc_hashtab` (now containing the seeded
  descriptor) and reads through the same `GUC_VARIABLE_<T>` — session-correct.

### 5.2 No divergence between SHOW/SET and the extension's own macro read

The extension reads `pgss_track` = `pgss_session_state()->track`; the accessor
returns `&pgss_session_state()->track`. Same cell, same session. `SET` via
guc.c writes `*GUC_VARIABLE_ENUM(record)` = that same address. So a `SET` is
immediately visible to `pgss_enabled()` (`pg_stat_statements.c:394-397`) and to
a subsequent `SHOW`. **Confirmed: no divergence.** (This is why the accessor must
return the exact same cell the macro dereferences — the API contract in §1.2.)

### 5.3 Placeholders

Unchanged from `PRELOAD_CUSTOM_GUC_FIX_DESIGN.md` §4: the registry snapshots only
fully-defined customs (`(flags & GUC_CUSTOM_PLACEHOLDER) == 0`). Because the seed
runs before `read_nondefault_variables()` replay, a config-SET preload custom GUC
finds the real (rebound) descriptor and assigns into the session cell instead of
manufacturing a placeholder — this is what fixes the "unrecognized parameter"
symptom for the config-SET case. Per-session placeholder creation for genuinely
unknown classes still works untouched.

### 5.4 RESET / RESET ALL / AtEOXact_GUC

These write through `GUC_VARIABLE_<T>(record)` / `GUC_RESET_<T>(record)`:
`ResetAllOptions` (`guc.c:3561-3610`), `AtEOXact_GUC` stack restore
(`guc.c:3942-4024`). With the rebind, `variable` is the per-session cell and
`reset_val` was set by `InitializeOneGUCOption(var)` in the seed (§3.2), so
RESET restores the session's boot value into the session's cell and rollback of a
`SET` restores the session's prior value into the session's cell. All
per-session, no cross-session effect. (The `CUSTOM_GUC_FIX_DESIGN.md` §a note
that these paths are unguarded is now moot for a rebound custom: the pointer they
write is session-owned by construction.)

Test coverage for this is Test-plan step 6 (§7).

---

## 6. Backward compatibility / fail-closed policy

An unconverted or out-of-tree custom GUC that does NOT call
`RegisterCustomGUCSessionAccessor*` has `has_session_accessor == false`. Two
sub-cases:

- **Session-scoped (`PGC_USERSET`/`PGC_SUSET`) with no accessor: REFUSE
  (fail-closed).** Its `valueAddr` is a process-global cell; seeding it without
  a rebind is exactly the silent cross-session `SET` race the adversarial review
  condemned. It must NOT be silently seeded. Enforce at the extension load gate,
  not per-GUC-late: the module that defines such a GUC must declare a threaded
  backend model, and the check happens where that model is validated.
  - The existing gate `check_module_backend_model`
    (`dfmgr.c:592-607`, refusal + `dlclose` at `:643-651`) already refuses a
    module whose declared `backend_model` is weaker than the runtime's. A module
    that defines session-scoped custom GUCs but registers no accessor for them
    MUST NOT be marked threaded-affine — that is a module-author invariant, and
    the design's enforcement is: **`SnapshotPreloadCustomGUCs` (§3.1) FATALs at
    preload if a threaded-affine module left a session-scoped custom GUC without
    an accessor.** Fail-closed at postmaster start, loud, before any session.
  - Rationale for FATAL-at-preload over refuse-at-load: preload already ran the
    module's `_PG_init` in the postmaster; by the time we snapshot we know both
    the GUC's context and whether an accessor was attached. A missing accessor on
    a session-scoped custom in a module the operator explicitly marked affine is
    a packaging bug — fail the postmaster start with a clear message
    (`"custom GUC \"%s\" is session-scoped but registered no per-session
    accessor; the defining module cannot run threaded"`), same spirit as
    `init_custom_variable`'s FATAL for post-startup PGC_POSTMASTER
    (`guc.c:6527-6529`).

- **Runtime-scoped (`PGC_POSTMASTER`/`PGC_SIGHUP`) with no accessor: ALLOW.**
  Process-wide by design (§4.2). No accessor needed; frozen pointer correct.

- **Process-only modules (default `PG_BACKEND_MODEL_PROCESS`):** already refused
  wholesale by `check_module_backend_model` under threaded
  (`dfmgr.c:320-334`, `:592-651`). They never reach the carrier address space, so
  their unconverted custom GUCs never seed. Phase 19 routes such sessions to a
  forked process-fallback backend. No change here.

**Net policy, one line:** a custom GUC that loads under threaded is either
runtime-scoped (frozen pointer OK) or session-scoped-with-registered-accessor
(rebound per session); anything else fails-closed at preload. No path silently
seeds a session-scoped custom GUC onto a shared cell.

---

## 7. Phased implementation + test plan

### Phase A — core API + rebind (no extension change yet)
1. Add `session_accessor` / `has_session_accessor` to `struct config_generic`
   (`guc_tables.h:319-323`); zero-init is free via `init_custom_variable`
   memset.
2. Add `register_custom_guc_session_accessor` + 5 public wrappers +
   `CustomGUCAttachSessionAccessor` (`guc.c` near `define_custom_variable`);
   declare the 5 in `guc.h:502` block.
3. Add `SnapshotPreloadCustomGUCs` (with §6 fail-closed check) + registry
   statics (`guc.c`); call once in `process_shared_preload_libraries`
   (`miscinit.c:1903`, `IsMultithreaded()`-guarded).
4. Add `SeedPreloadCustomGUCs` with the accessor rebind (§3.2); call in
   `InitializeThreadedSessionGUCOptions` (`guc.c:2751`).
   Validate: `gmake check` (process mode inert), `gmake check-threaded`
   (no custom preload yet → registry empty → no behavior change),
   `gmake check-runtime-lifecycles` (new `TopMemoryContext` registry is a
   deliberate process-lifetime global, like `guc_builtin_hashtab`).

### Phase B — convert pg_stat_statements + mark affine
5. Add the 3 `*_ref` accessors and 3 `RegisterCustomGUCSessionAccessor*` calls
   (§4.1). Leave `max`/`save` alone (§4.2).
6. Bump `pg_stat_statements` `PG_MODULE_MAGIC` to the affine backend model
   (unblocks the marker held in `.ec2/preload-custom-guc-threaded-gap.md`
   "Consequence for Phase 16 markers").
   Validate: the test plan below on `check-threaded-pooled`.

### Phase C — regression + scope guards
7. Reuse `test_backend_runtime` split files (per AGENTS.md); add a
   preload+mt=on config.

Test plan (target: preloaded `pg_stat_statements`, `multithreaded=on`, pooled
protocol, **1-carrier pool** so two sessions time-share one address space —
`check-threaded-pooled`):

1. **SHOW visibility (core regression).** Session A: `SHOW pg_stat_statements.track;`
   → `top`, not "unrecognized configuration parameter". Matches mt=off oracle.
2. **Two-session SET isolation (catches the frozen-pointer bug).** Both sessions
   on the one carrier: A `SET pg_stat_statements.track='all';`, B
   `SET ...='none';`, yield at a wait boundary (`pg_sleep(0)` / round-trip), then
   A `SHOW ...` → `all`, B `SHOW ...` → `none`. Also assert `pgss_enabled()`
   behavior differs per session (query tracked in A, not in B) — proves SHOW/SET
   and the extension macro read the SAME cell (§5.2). **This step FAILS against
   the old seed-only design; it is the exact assertion that gates the rebind.**
3. **A/B parity.** Same transcript under `multithreaded=off` → identical. Fork is
   the oracle.
4. **Config-SET / placeholder path.** `pg_stat_statements.track='top'` in
   `postgresql.conf`, mt=on, fresh session `SHOW ...` → `top` (proves seed before
   replay, §5.3).
5. **Runtime-scoped correctness.** `SHOW pg_stat_statements.max` → the configured
   value in every session; `SET pg_stat_statements.track_planning` in A does not
   perturb `max`. Confirms §4.2 (no accessor, shared runtime cell, no race).
6. **RESET / xact-abort.** A: `BEGIN; SET pg_stat_statements.track='all';
   ROLLBACK;` then A reads its prior value, B unaffected — exercises §5.4
   (per-session RESET/AtEOXact on the rebound cell).
7. **Fail-closed guard.** In `test_backend_runtime`, define a session-scoped
   custom GUC WITHOUT registering an accessor, mark the module affine, preload
   under mt=on → postmaster start FATALs with the §6 message. (Negative test;
   revert the deliberate omission after asserting the failure.)
8. **Scope guard (session-preload negative space).** `session_preload_libraries`
   custom GUC (e.g. `auto_explain` via that path) visible in-session under mt=on
   with no seeding — locks the boundary from `PRELOAD_CUSTOM_GUC_FIX_DESIGN.md`
   §5.

Validation targets: `gmake check-threaded-pooled`, `gmake check-threaded`,
`gmake check-runtime-lifecycles`, `gmake check-global-lifetimes` (registry is a
process-lifetime exception), `gmake check` (process mode green).

---

## 8. Ownership split between the three docs (resolves the review's item 3)

- `CUSTOM_GUC_FIX_DESIGN.md`: extension per-session VALUE storage (the macro read
  is per-session). Necessary, not sufficient.
- `PRELOAD_CUSTOM_GUC_FIX_DESIGN.md`: shared descriptor REGISTRY + per-session
  seed (the descriptor reaches the session). Necessary, not sufficient.
- **This doc**: the per-session value-pointer REBIND via an extension-registered
  accessor (SHOW/SET/pg_settings hit the session cell). This doc owns the
  accessor registration and its storage, because it owns the registry field the
  accessor rides on and the seed that invokes it. All three are required; none
  subsumes another.

---

## Appendix — key file:line index

- Accessor union + rebind entry struct: `guc_tables.h:32-46`.
- Built-in rebind (the model reused): `RebindSessionGUCVariablePointer`
  `guc.c:2887-2913`; `RebindSessionGUCVariablePointers` `guc.c:2916-2923`;
  `ValidateSessionGUCVariableRebinds` `guc.c:2926-2972`.
- `threaded_accessor` machinery (in-tree only, why option c rejected):
  `guc_parameters.dat` column via `gen_guc_tables.pl:64, 236-264`.
- `DefineCustom*` capture of frozen valueAddr:
  `guc.c:6809/6835/6863/6889/6913`; signatures `guc.h:502-560`
  (`pg_attribute_nonnull(1, 4)`).
- `init_custom_variable` (memset, separate `->state`): `guc.c:6510-6570`.
- `define_custom_variable`: `guc.c:6575`.
- `config_generic` struct (add accessor field): `guc_tables.h:311-359`;
  `config_generic_state`: `guc_tables.h:186-195`.
- Seed insertion point: `InitializeThreadedSessionGUCOptions` `guc.c:2730-2762`;
  built-in rebound-init `InitializeThreadedSessionReboundGUCOptions`
  `guc.c:2843-2858`.
- `find_option`/`add_guc_variable`/`InitializeOneGUCOption`/
  `InitializeOneGUCOptionResetMetadata`/`GUCOptionVariablePointer`:
  `guc.c:2064, 1861, 3038, 3148, 2824`.
- Ownership predicate: `PgCurrentOrEarlySessionOwnsPointer`
  `backend_runtime_session.c:2089-2107`.
- SHOW/SET write sites: `guc.c:5346/5442/5538/5820`, `ShowGUCOptionInternal`
  enum `guc.c:7251-7252`.
- RESET/AtEOXact: `ResetAllOptions` `guc.c:3561-3610`, `AtEOXact_GUC`
  `guc.c:3942-4024`.
- Preload once + registry snapshot point: `process_shared_preload_libraries`
  `miscinit.c:1896-1904`; postmaster preload call `postmaster.c:1097`.
- Load gate (fail-closed for process-only / unconverted affine):
  `check_module_backend_model` `dfmgr.c:320-334, 592-607, 643-651`.
- pg_stat_statements: session state `pg_stat_statements.c:311-352`; macros
  `:386-392`; `DefineCustom*` `:496-556`; `pgss_enabled` `:394-397`.
