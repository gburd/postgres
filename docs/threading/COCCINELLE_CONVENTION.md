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
  run-cocci.py            driver: list / dry-run / --apply the patches
  guc_accessor_poc.cocci  proof-of-concept: GUC &global -> GetGUC<Type>()
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

The driver runs each `*.cocci` over `src/{backend,include,common,bin}`
(one `spatch --dir` per directory — spatch accepts only one `--dir`),
skipping vendored/generated trees.  Dry-run uses `spatch --show-diff`;
`--apply` uses `--in-place`.

A patch named `*_poc.cocci` is treated as *illustrative*: its matches
are reported but never fail the gate (so the `cocci` target stays green
while shipping a not-yet-applicable proof-of-concept).  Normal-form
patches — ones that should already be applied — fail the dry-run gate
(exit 1) if they would still change the tree.

## The proof-of-concept

`guc_accessor_poc.cocci` demonstrates the single most-cited mechanical
sweep in the conversion: the GUC system stores each of its ~350 settings
in a process-global that callers read directly (`a_global`).  Under
threaded mode that storage moves behind a function-call API
(`GetGUC<Type>()`/`SetGUC<Type>()`; xtc: `xtc_cfg`), so every direct read
becomes an accessor call.  The patch matches reads of two real GUC
globals — `log_duration` (bool) and `log_min_duration_statement` (int) —
and rewrites them to `GetGUCBool(...)` / `GetGUCInt(...)`, leaving the
storage definition and the GUC machinery's own writes alone.

It is **illustrative, not applied**: the `GetGUC*` accessor API is an F3
deliverable that does not yet exist on this branch, so `run-cocci.py`
runs it in dry-run only.  When the F3 accessor layer lands, extend the
identifier lists (or generate them from `guc_tables.c`) and apply
tree-wide.  Verified: the dry-run produces the expected unified diff
across `src/backend/tcop/postgres.c`.
