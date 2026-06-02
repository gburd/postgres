# Coccinelle convention — mechanical transforms as semantic patches

**Status:** adopted on branch `xtc` going forward.  Tooling lives in
`src/tools/cocci/`; run via the meson `cocci` target.
**Depends on:** [Coccinelle](https://coccinelle.gitlabpages.inria.fr/website/)
(`spatch`).  Present in this environment at `spatch` 1.3.0.

## The rule

The threaded conversion contains two very different kinds of edit:

- **Mechanical** — high-volume, pattern-driven, semantics-preserving
  sweeps: call-site rewrites, signature changes, annotation seeding,
  `&global` → accessor conversions.  These are tedious by hand, easy to
  get subtly inconsistent, and *recur* every time we merge from
  `origin/master` (new call sites appear).
- **Structural / creative** — the load-bearing refactors where the design
  *is* the work (e.g. the Latch → Interrupt re-derivation).  These need
  human judgement per site.

**Convention:** ship *mechanical* transforms as re-runnable `.cocci`
semantic patches in `src/tools/cocci/`.  Keep *structural* edits
hand-derived.  A checked-in semantic patch is self-documenting,
re-runnable as the tree drifts, and turns an `origin/master` merge into
"re-run the patch" instead of "re-resolve the conflicts by hand."

**Do NOT retrofit `.cocci` for transforms that have already landed**
(e.g. Steps 1–7 of the interrupt re-derivation).  The convention is for
upcoming phases only.

## Layout

```
src/tools/cocci/
  run-cocci.py        driver: list / dry-run / --apply the patches
  guc_accessors.cocci tree-wide GUC read -> GetGUC<Type>(GUC_<var>) sweep
                      (GENERATED; see F3_GUC_ACCESSORS.md)
```

## Running

```
# dry-run every patch (exit 1 if any would change the tree); needs spatch
ninja -C <build> cocci
# or directly:
python3 src/tools/cocci/run-cocci.py            # dry-run, prints diffs
python3 src/tools/cocci/run-cocci.py --list      # list patches
python3 src/tools/cocci/run-cocci.py --apply     # write changes in place
```

By default the driver runs each `*.cocci` over
`src/{backend,include,common,bin}` (one `spatch --dir` per directory —
spatch accepts only one `--dir`), skipping vendored/generated trees.
Dry-run uses `spatch --show-diff`; `--apply` uses `--in-place`.

A patch may override that scope via the `PATCH_SCOPE` map in
`run-cocci.py` (keyed by basename): a narrower set of target directories
plus a list of file suffixes to exclude.  This is used by
`guc_accessors.cocci`, which is backend-only and must skip the GUC
storage-owning files (see below).  For scoped patches the driver passes an
explicit file list (so individual files can be excluded — `--dir` cannot
do that) and omits `--include-headers`.

A patch named `*_poc.cocci` is treated as *illustrative*: its matches
are reported but never fail the gate.  Normal-form patches — ones that
should already be applied — fail the dry-run gate (exit 1) if they would
still change the tree.

## The first applied sweep: GUC read accessors (F3)

The single most-cited mechanical sweep in the conversion is the GUC one:
the GUC system stores each of its ~440 settings in a process-global that
callers read directly (`work_mem`).  Under threaded mode that storage
moves behind a read accessor (`GetGUC<Type>(GUC_<var>)`), so every direct
read becomes an accessor call.

This sweep is now **applied tree-wide** as `guc_accessors.cocci`, a
generated, re-runnable, idempotent semantic patch.  It is the worked
example of the convention: a mechanical transform shipped as a checked-in
patch rather than a one-shot hand edit, so re-running it after an
`origin/master` merge re-converts any newly-appeared reads.  See
[F3_GUC_ACCESSORS.md](F3_GUC_ACCESSORS.md) for the generator, the SmPL
design (single read rule + script-selected accessor, address-of handled
in a disjunction to stay idempotent), the backend-only scope, and the
storage-file exclusions.

(The earlier `guc_accessor_poc.cocci` proof-of-concept — which rewrote
just `log_duration`/`log_min_duration_statement` against a not-yet-landed
API — has been removed now that the real generated sweep exists.)
