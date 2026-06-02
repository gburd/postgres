# F3 - The GUC Accessor API

**Status:** implemented on branch `xtc`. The accessor layer is generated
and wired into the build; the tree-wide read sweep is applied and is
idempotent (the `cocci` dry-run gate passes). The accessors are identity
wrappers today, so the change is behavior-preserving; the `multithreaded`
build option / runtime GUC stays dormant and OFF by default.
**Lives:** `~/ws/postgres/xtc` (the fork worktree).
**Depends on:** F3 accessor layer generator (Perl, `Catalog::ParseData`)
and [Coccinelle](https://coccinelle.gitlabpages.inria.fr/website/)
(`spatch`, for the sweep). See also
[COCCINELLE_CONVENTION.md](COCCINELLE_CONVENTION.md).

## What and why

The GUC subsystem stores each of its ~440 settings in a process-global
that callers read directly:

```c
if (work_mem > limit) ...
```

In a threaded server those values become per-session, so the storage has
to move behind an accessor. F3 introduces a typed *read* accessor layer
and rewrites every direct read in the backend to go through it:

```c
if (GetGUCInt(GUC_work_mem) > limit) ...
```

Today every accessor is a thin identity wrapper (`GetGUC<Type>(x)` is
`(x)`, `GUC_<var>` is `(<var>)`), so this is a no-op at runtime. Its value
is establishing the *single choke point* through which a future threaded
build can serve per-session values without re-touching ~1,800 call sites.

## Implementation map

| Piece | Where | Run via |
|---|---|---|
| Accessor header generator | `src/backend/utils/misc/gen_guc_accessors.pl` | meson custom_target `guc_accessors`; Makefile |
| Generated header | `build/src/include/utils/guc_accessors.h` (`#include`d from `postgres.h`) | build |
| Sweep generator | `src/backend/utils/misc/gen_guc_accessors_cocci.pl` | by hand after `.dat` changes |
| Generated sweep | `src/tools/cocci/guc_accessors.cocci` | `python3 src/tools/cocci/run-cocci.py` |
| Source of truth | `src/backend/utils/misc/guc_parameters.dat` | — |

Both generators read `guc_parameters.dat` via `Catalog::ParseData`, so the
accessor set and the sweep's variable set stay in lockstep. Each entry's
`type` selects the accessor and `variable` is the C backing global (which
differs from the GUC `name`).

## The accessor layer

`guc_accessors.h` defines five typed identity macros and one token per
backing global:

```c
#define GetGUCBool(x)   (x)
#define GetGUCInt(x)    (x)
#define GetGUCReal(x)   (x)
#define GetGUCString(x) (x)
#define GetGUCEnum(x)   (x)        /* distinct from Int: enum GUCs */
#define GUC_work_mem    (work_mem)
/* ... one GUC_<var> per backing global ... */
```

A distinct `GetGUCEnum()` (rather than reusing `GetGUCInt()`) keeps enum
GUCs typed for a later non-identity implementation.

## The sweep

`guc_accessors.cocci` is a generated, re-runnable Coccinelle semantic
patch. Design highlights (the full rationale and the spatch gotchas it
encodes are in the generator's header comment):

- **One read rule for all variables.** A single `@rd@` rule matches every
  backing global via one big name alternation; a `@script:python@` rule
  picks the accessor for each match; the `GUC_<var>` token is built with
  `fresh identifier`. Matching all variables in one pass (instead of one
  rule per variable) keeps spatch's recomputed match positions consistent
  and the patch fast (constant rule count rather than ~1,800 rules).
- **Reads only.** Coccinelle matches identifiers in expression position,
  so declarations, `extern`s and struct fields (`st->work_mem`) are left
  alone automatically. The expression-position non-reads that must be
  preserved are excluded:
  - lvalue writes (`work_mem = e`, `+=`, `++`, ...) via `@lval@`;
  - `offsetof(T, work_mem)` member designators via `@ofs@`;
  - address-of (`&work_mem`) via a disjunction branch in the transform
    rule. A bare `&g` is not a parseable standalone SmPL rule body, so it
    cannot be a position-exclusion rule of its own; written as
    `( &g | -g +acc(GUC_g) )` it parses and leaves every `&g` (call args,
    initialisers like `&Trace_locks`) intact. Handling address-of here is
    what makes the patch **idempotent**.
- All rules are constrained to the GUC name set; leaving them as
  unconstrained `identifier g;` makes spatch test the disjunction at every
  identifier in every file and is pathologically slow.

## Scope and exclusions

The accessor layer is **backend-only**: `guc_accessors.h` is reachable
only through `postgres.h`. The sweep therefore runs over `src/backend`
plus the handful of backend-only headers in `src/include`, and **excludes**:

- `src/common` and `src/bin` — they compile with `-DFRONTEND`, never see
  `guc_accessors.h`, and contain locals/options coincidentally named like
  GUCs (`block_size`, `quote_all_identifiers`, ...);
- the GUC storage-owning files `utils/misc/{guc.c, guc_tables.c,
  guc_funcs.c}` (and generated `guc_tables.inc.c`) — they write the
  globals through `&variable` pointers in the GUC table and must keep
  reading them raw.

`run-cocci.py` enforces this through its `PATCH_SCOPE` map (narrower
target dirs + a file-exclude list), so the dry-run normal-form gate passes
against the swept tree.

## Hand fixups

A few sites cannot be rewritten mechanically and were fixed by hand, then
re-swept for their ordinary reads:

- `multixact.c`, `procarray.c`, `lock.c`: a GUC name used inside a
  `#define` body (`NumMemberSlots`, `PROCARRAY_MAXPROCS`, `NLOCKENTS`)
  makes spatch abort with "try to delete an expanded token"; the macro
  definitions were rewritten by hand to call the accessor.

## Running / regenerating

```
# dry-run gate (exit 1 if the sweep would still change the tree)
ninja -C <build> cocci
python3 src/tools/cocci/run-cocci.py guc_accessors.cocci

# re-apply after an origin/master merge introduces new reads
python3 src/tools/cocci/run-cocci.py --apply guc_accessors.cocci

# regenerate the patch after guc_parameters.dat changes
perl src/backend/utils/misc/gen_guc_accessors_cocci.pl \
     src/backend/utils/misc/guc_parameters.dat \
     src/tools/cocci/guc_accessors.cocci
```

Verified: build-green; the full smoke suite passes, including fast-stop
with a live walsender; the `cocci` dry-run gate is clean (the sweep is
idempotent across all swept files).
