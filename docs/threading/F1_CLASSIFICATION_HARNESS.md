# F1 - The Classification Harness

**Status:** plan. The prerequisite to all per-global conversion work.
**Lives:** `~/ws/postgres/xtc` (the fork worktree).
**Depends on:** clang/LLVM (already a PG optional dep for JIT).

## Why this is the foundation of the foundation

The threaded conversion is dominated by reclassifying process-global
state. Hard counts from a clean `master` (`src/backend`, 903 `.c`
files):

- ~517 file-scope mutable `static`/extern globals.
- 153 function-scope `static`, 104 with initializers.
- 827 `PGDLLIMPORT` exported globals across 170 headers.
- ~350 GUC entries, each bound to `&a_global`.

You cannot convert this by hand and keep it converted. Every new
patch to PG adds globals. F1 makes the machine:

1. **classify** every mutable global into a known lifetime, and
2. **hard-fail CI** when an unclassified or wrongly-classified global
   appears.

Without F1, threaded mode rots the day after it lands. With F1,
"is this global safe under threads?" has a mechanical answer on every
commit.

## What already exists (Heikki's branch) and what we reuse

Heikki's `heikki-threading` branch already built most of the machine.
F1 is mostly *productionising* it, not inventing it.

### Forward-port outcome (Phase 0)

Heikki's branch is ~11 months behind our master (tip `cf248542072`,
2025-05-09; merge-base `b28c59a6cd0`; 53 commits his side / 2746 master
side). A full rebase is rejected: the bottom third of his series is one
load-bearing "Replace Latches with Interrupts" refactor, and master kept
evolving `latch.c`/`WaitEventSet` independently (incl. "Split
WaitEventSet functions to separate source file") -- the worst merge
surface, on the layer we least want verbatim. Strategy is *selective
forward-port*: cherry-pick the self-contained tooling, re-derive the
structural core against current master using his diff as reference.

- **Transplanted clean** (cherry-picked `-x` onto branch `xtc`):
  - `f6d7d42c81f Add pgguclifetimes` -> `a51cde5fd63`
  - `67ef58ead71 Add pg_static_vars tool.` -> `f32fda073f7`
  Both are essentially net-new `src/tools/pgguclifetimes/` files; only
  `src/meson.build` (+1) and `.gitignore` (+1) shared, auto-merged.
- **Deferred to re-derivation** (NOT cherry-picked -- unmet dependency
  on the annotation layer, which does not exist on master):
  - `27fe5c0a65c Add multithreaded GUC` -- conflicts in `globals.c`,
    `guc_tables.c`, `miscadmin.h` because it assumes the
    `postmaster_guc`/`session_local`/`session_guc`/`pg_global` macros
    and the `guc_tables.inc.c` mechanism are already present. They are
    not on master. Re-derive in F3 alongside the macro decision.
  - `347bf07e484 Add PG_MODULE_MAGIC_REENTRANT` -- mutates the
    `Pg_abi_values` ABI struct (`backendmodel`) + `dfmgr.c` load path.
    Belongs with the F7 extension-classification ABI, re-derived there.

The annotation macros themselves (`session_local` etc. in
`postgres_ext.h`) and the ~1187+133 applied annotations live in
Heikki's `e72653aedb8`/`3b43bbca2e0`/macro commits; those are
re-derived in F3, not cherry-picked, since they ride on the same
structural layer.

We adopt the tooling wholesale; the rest is reference design.

- `src/tools/pgguclifetimes/pgguclifetimes.c` -- a libclang tool that
  walks every global and checks it carries an `annotate(...)`
  attribute naming its lifetime. Builds against `-lclang`.
- `src/tools/pgguclifetimes/pg_static_vars.cpp` -- a C++/LibTooling
  companion that finds function-scope statics.
- Annotation macro (`postgres_ext.h`):
  `#define session_local __thread __attribute__((annotate("session_local")))`.
  Under `multithreaded=off`, `session_local` expands to nothing.
- Lifetime taxonomy already defined in the tool:
  `dynamic_singleton`, `global`, `internal_guc`, `postmaster_guc`,
  `session_guc`, `session_local`, `sighup_guc`, `static_singleton`,
  `suset_guc`, `userset_guc`.
- A `t/001_pgguclifetimes.pl` TAP test with `tests/success` (fully
  annotated) and `tests/failure` (an un-annotated global) fixtures.
- Already applied: 1187 `session_local` + 133 `postmaster_guc`
  annotations across the tree.

## The lifetime taxonomy (the classification rule)

Each mutable global must be exactly one of these. The annotation is
the author's declaration; the tool verifies presence and the lints
verify consistency.

| Lifetime | Meaning | Threaded disposition |
|---|---|---|
| `session_local` | per-backend/session mutable state | TLS (`__thread`); lives in the `Session` struct (F4) |
| `postmaster_guc` | set once at postmaster start, then read-only | plain global, read-only after startup |
| `sighup_guc` | changes only on config reload | RCU-published or guarded read |
| `userset_guc` / `suset_guc` / `session_guc` | GUCs with per-session scope | move to `xtc_cfg` function-call API; per-session overlay |
| `internal_guc` | GUC machinery internal | as the GUC system dictates |
| `static_singleton` | initialised once, immutable thereafter | plain global, no guard needed |
| `dynamic_singleton` | one shared instance, mutated under a lock | needs an explicit lock; flagged for review |
| `global` | genuinely process-wide shared mutable | the dangerous bucket; each one is a design decision |

The point of the taxonomy is that **`session_local` is NOT the
default answer.** Blindly making everything `__thread` is wrong: it
silently duplicates state that was meant to be shared (breaking
correctness) and bloats per-thread footprint. The tool forces a
conscious choice per global.

## Deliverables

### F1.1 - Build the tools in both build systems, in CI
- Port the `pgguclifetimes` Makefile + meson build forward onto
  current `master` (it currently lives on Heikki's older base).
- Gate on `--with-llvm` (reuse PG's existing LLVM detection).
- Produce a compilation database (`meson` emits
  `compile_commands.json` natively; for autoconf use `bear` or
  `compiledb`).

### F1.2 - A `make threadcheck` target
- Runs `pgguclifetimes` + `pg_static_vars` over the whole backend
  compilation database.
- Exit non-zero on: any unannotated mutable global, any
  function-scope static without an explicit lifetime, any global
  whose annotation contradicts its usage (best-effort).
- Emits a categorised report (counts per lifetime, list of
  offenders with `file:line`).

### F1.3 - Source lints (cheap, fast, pre-clang gate)
Port xtc's `dist/s_globals` / `dist/s_signals` style grep-lints as
fast pre-checks that run without LLVM:
- `s_globals`: forbid a *new* bare `static T x;` (mutable, file or
  function scope) outside an allowlist, unless annotated.
- `s_signals`: forbid new raw `signal()` / `sigaction()` outside the
  port layer.
- `s_libc`: forbid `setlocale`, `strerror`, `getenv`/`setenv`,
  `strtok`, `rand`, `localtime`, `getopt` outside their thread-safe
  wrappers.
These run on every PR in seconds and catch 90% of regressions before
the heavier clang pass.

### F1.4 - TSan build profile
- `--enable-sanitize=thread` (meson `-Db_sanitize=thread`) build that
  runs the core regression suite in threaded mode.
- Starts as a non-gating nightly; becomes gating once threaded mode
  passes clean (mirrors the PG workplan "TSan in CI from CF1").

### F1.5 - The baseline + ratchet
- Record the current offender list as a baseline allowlist
  (`src/tools/threadcheck/baseline.txt`).
- CI fails if the offender set *grows*. Each converted subsystem
  removes lines from the baseline. The baseline only ever shrinks.
- This lets us land F1 immediately without first converting all
  ~670 existing globals -- it just stops the bleeding and tracks
  burn-down.

### F1.6 - Vendor xtc as the amalgamation + wire the build
F1 ships in the same Phase 0 commit that first brings xtc into the
tree, so the build wiring lands here. **xtc is vendored as the
single-file amalgamation, not a separately built `libxtc.a` linked via
`--with-xtc`:**

**Status (Phase 0, done):** submodule added at `contrib/libxtc`,
pinned to `6bc9107` (= `v0.4.0-36`; current `origin/main` tip). The R1
`at_exit`/`mctx`/`down_decode` helpers the F5 spike depends on landed at
`2eba22b` (`v0.4.0-7`); this newer pin additionally carries the R1
async-signal-safety + Windows-SEH hardening (commit `22c277a`), the
ready R2 `xtc_net_send_frame/recv_frame`, R4 `xtc_sup_add_child`, A8
`xtc_svr_call_abortable`, and the cooperative yield watchdog -- all
verified additive (the POSIX `xtc_proc_recovery_arm()` macro is
unchanged, so the F5 spike needs no source change).
Generator script `src/tools/gen_xtc_amalgamation.sh` regenerates
`xtc.h`+`xtc.c` from the submodule into a build-local `xtc-amalg/`
(gitignored). Verified: generates (44 .c, 30 public + 34 stub headers)
and compiles clean, both release and `-DDEBUG -DXTC_RELATIVE_LOC`, and
links + runs a smoke test (`-pthread -ldl -lm`).

- Generate `xtc.h` + `xtc.c` with `dist/mkamalgamation.py`
  (SQLite-style single-file), wrapped by
  `src/tools/gen_xtc_amalgamation.sh` (resolves the submodule, warns on
  pin drift, writes to a build-local outdir). Working consumer pattern:
  `xtc/examples/06_sqlxtc/` Makefile `amalg` target.
- Carry the upstream sources as a git submodule at `contrib/libxtc`
  and regenerate the amalgamation from it, building with
  `-DDEBUG -DXTC_RELATIVE_LOC=contrib/libxtc` so `#line` directives
  remap diagnostics/backtraces back to the original xtc sources in
  debug builds. Release builds use the flat amalgamation.
- **GOTCHA (verified):** the amalgamation emits internal
  `#define _GNU_SOURCE`, but they land *after* the first system-header
  include (`xtc.h` -> `stdint.h` -> `features.h`), so they are
  ineffective on glibc -- `pthread_setname_np`, `sched_getcpu`,
  `struct ucred` fail to declare. Consumers MUST pass `-D_GNU_SOURCE`
  on the command line (matches upstream `examples/06_sqlxtc`
  `AMALG_CFLAGS`). Report to xtc-dev: move the defines ahead of the
  first include, or document the required flag.
- Forwarding stubs: the amalgamation emits per-header stubs
  (`xtc_proc.h`, `xtc_loop.h`, ...) that all `#include "xtc.h"`, so PG
  glue code includes the familiar `xtc_*.h` names with a single
  `-Ixtc-amalg/include`.
- Build inputs: compile `xtc.c` once into one object with
  `-std=c11 -D_GNU_SOURCE -Ixtc-amalg/include`; the amalgamation
  auto-selects the epoll backend and needs only `-pthread -ldl -lm`
  (no liburing, no separate library). Add the `xtc.c` object to both
  the autoconf and meson backend link, gated so `multithreaded=off`
  builds still link cleanly (the runtime is present but dormant).
- `threadcheck` (F1.2) and the lints (F1.3) must EXCLUDE the vendored
  `xtc.c`/`xtc.h` and `contrib/libxtc/**` from the PG-globals baseline
  -- xtc has its own thread-safety discipline; we lint PG core only.

## Rollout

1. Land F1.1-F1.3 + F1.6 (amalgamation vendoring + build wiring) with
   `threadcheck` non-gating + the frozen baseline. Zero behaviour
   change; `multithreaded` stays off.
2. Flip the source lints (F1.3) to gating (cheap, low false-positive).
3. Flip `threadcheck` (F1.2) to gating against the ratcheting
   baseline.
4. Bring TSan (F1.4) online as nightly, then gating once green.

## Exit criteria

- `make threadcheck` runs in CI on every PR.
- The global baseline is frozen and can only shrink.
- A PR that adds an unclassified global, a raw signal, or a
  thread-unsafe libc call fails CI with a precise `file:line`.
- A burn-down dashboard shows globals-remaining per subsystem.

## Risk notes

- **libclang version drift**: the tool is sensitive to clang AST
  changes. Pin a clang version in CI; the tool already degrades
  gracefully (`required: false` in meson).
- **Compilation-database coverage**: globals in files not in the
  compdb are invisible. The meson build gives full coverage for
  free; the autoconf path needs `bear`. Treat missing-from-compdb
  as an error, not a pass.
- **Macros that synthesise globals** (e.g. `PG_FUNCTION_INFO_V1`,
  fmgr stubs) need an allowlist so they are not false positives.
- **Amalgamation drift**: the vendored `xtc.c`/`xtc.h` are generated
  artifacts. Pin the `contrib/libxtc` submodule to a known-good xtc
  commit and regenerate the amalgamation as a scripted, reviewable
  step (never hand-edit the single file). The `XTC_RELATIVE_LOC`
  `#line` remap keeps debug backtraces pointing at the real sources.
  (Upstream xtc gates its amalgamation build on every commit, so a
  pinned commit is known-good as a single-file consumer.)
