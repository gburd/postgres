# Fiber-ctx-hook elimination design (Option 3 investigation)

Status: DESIGN + INVESTIGATION. No functional code landed this pass. Pin stays
at libxtc v1.24.0 (e944d00). HEAD ~56ceadfd383.

## TL;DR verdict

**The fiber-ctx hook CAN be eliminated, and it should be.** The hook's only
runtime job is to repoint the thread-local bridge cells to the resuming fiber's
6 roots. Two independent facts make it removable:

1. **The manual wait-boundary seams already do that repoint** on every
   cooperative yield a backend fiber has (`xtc_pg_wait_fd`,
   `method_xtc.c`, `fd.c` all call `PgRuntimeSaveCurrentWork` /
   `PgRuntimeRestoreCurrentWork` around the park). The hook is only *uniquely*
   needed for **involuntary SIGVTALRM-preemption switches** — and those don't
   need a root repoint while pinned (resume-on-same-thread) and are the
   migration case once unpinned.

2. **The 6 roots are fiber-owned** (Phase A finding, independently confirmed):
   they travel with the fiber's stack across a steal, so the *source of truth*
   already migrates. Only the thread-local *cache* needs to reflect the running
   fiber.

The make-or-break was the per-hot-read cost of resolving roots on-demand from
libxtc's running-proc identity. Measured: **`xtc_self()` = ~1.76 ns vs the
current bridge fast path = ~0.35 ns (a ~5x per-read tax)**. That kills
**Option 2** (self-validate every hot read). It does **not** kill the design,
because the seams already carry the repoint — we don't need per-read
validation at all.

**Recommended path (decisive):**

- **Eliminate the ctx hook.** Keep the manual seams as the sole repoint
  mechanism for the pinned world (they already are the belt-and-suspenders and
  the hook was explicitly "no-op-equivalent while pinned"). Removing the hook
  removes the `__xtc_fiber_ctx_save/restore` extern dependency entirely →
  **v1.25.0 links cleanly** → `xtc_proc_opts_t.migratable` is in hand.
- For the **unpin** step (Phase D), the involuntary-preemption switch that the
  hook used to cover needs *a* supported per-resume repoint. The clean,
  supported mechanism is a **small libxtc API: a public per-proc `void*`
  userdata** the consumer sets once at spawn and reads O(1) from any context.
  PG hangs its `PgThreadBackendRuntimeState`/snapshot off it and re-derives the
  bridge lazily on the first hot read after a resume, driven by a cheap
  per-thread "current-proc changed" check — **not** a chained switch hook.
  Verify-before-requesting done below: **no public per-proc storage exists in
  v1.25**; request drafted at `/tmp/libxtc-per-proc-userdata-request.md`.

Fallback if libxtc declines: static-link libxtc so the internal
`__xtc_fiber_ctx_*` symbols stay visible. **Not recommended** — it re-couples us
to symbols libxtc explicitly declared internal (`local: *` in `libxtc.map`,
`xtc_proc.h:602`); a future refactor removes them and we break again.

---

## 1. The problem, restated from source

### 1.1 What the hook does

`src/backend/postmaster/pg_xtc_carrier.c`:

- Declares the internal globals (lines 597-598):
  ```c
  extern void *(*__xtc_fiber_ctx_save) (void);
  extern void (*__xtc_fiber_ctx_restore) (void *);
  ```
- Reads + **assigns** them to chain PG's save/restore (lines 831-847,
  `xtc_pg_install_fiber_ctx_hook`).
- `xtc_pg_fiber_ctx_save` (line ~708): snapshots the 6 roots into
  `fc->snap` (a per-fiber blob on the fiber's own stack), returns the *real
  proc* (never the blob — hard safety requirement under the global-hook-reset
  race).
- `xtc_pg_fiber_ctx_restore` (line ~772): chains the proc-layer restore, then
  `PgRuntimeRestoreCurrentWorkLazy(&fc->snap)` **iff** the thread-local blob's
  recorded `proc_ctx == ctx` (positive identity of the resuming fiber).

The restore is **lazy**: it swaps the 6 root pointers and lets the ~130 derived
hot-field caches + session-rooted GUC state re-derive on first touch via the
existing owner-token mechanism (no per-switch GUC rebind; eager was ~3722 ns,
measured previously).

### 1.2 Why v1.25.0 breaks it

`libxtc.map` after commit 23242a7 (verified in `/home/gburd/ws/xtc`,
`git show HEAD:dist/libxtc.map`):

```
global:
    xtc_*;
    __xtc_proc_recovery_slot;
    __xtc_recovery_prep;
    __xtc_recovery_ctx;
    __xtc_recovery_result;
local:
    *;               <- hides __xtc_fiber_ctx_save / __xtc_fiber_ctx_restore
```

PG links libxtc **shared** (`meson.build:1140 xtc = dependency('xtc')` →
`libxtc.so.1`, confirmed by `ldd build/src/backend/postgres | grep xtc` →
`libxtc.so.1 => .../xtc-1.24.0/lib/libxtc.so.1`). Under v1.25.0 the two
`__xtc_fiber_ctx_*` symbols are `local` (unexported) → our `postgres` will not
resolve them.

### 1.3 The design signal

`xtc_proc.h:602` (verified in v1.25):
```
/* The internal current-proc context save/restore across a yield
 * (__xtc_proc_ctx_save / __xtc_proc_ctx_restore) is library-internal
 * (the __ prefix) ... */
```
libxtc's stance is explicit: consumers must not reach into the fiber-ctx hook.
The migratable feature preserves `__current_proc` across migration *internally*.
Reaching for the internal switch hook fights that.

---

## 2. The bridge, from source — how the 6 roots resolve today

`src/include/utils/backend_runtime_current.h`:

- The bridge is `PG_THREAD_LOCAL PgRuntimeCurrentBridge PgRuntimeCurrentBridgeState`
  (line 148). Its first members are the 6 roots:
  ```c
  typedef struct PgRuntimeCurrentBridge {
      PgRuntime  *runtime;
      PgCarrier  *carrier;
      PgBackend  *backend;
      PgSession  *session;
      PgConnection *connection;
      PgExecution *execution;
      /* ... hot cells / mirrors / fields (owner-token cached) ... */
  } PgRuntimeCurrentBridge;
  ```
- `CurrentPgRuntime` etc. are **raw reads** of those cells (lines ~403-411):
  ```c
  #define CurrentPgRuntime (*PgCurrentRuntimeHotRefMaybeRef())
  // PgCurrentRuntimeHotRefMaybeRef() { return &PgRuntimeCurrentBridgeState.runtime; }
  ```
  No owner check on the roots themselves — they are the source the caches key
  off of.
- The ~130 derived hot fields carry an **owner token** and self-invalidate
  (lines 263-275, `variable##MaybeRef`):
  ```c
  slot = bridge->variable;
  if (likely(slot != NULL && bridge->variable##Owner == (const void *) bridge->owner))
      return slot;              /* fast path: ~0.35 ns (measured) */
  return fallback();            /* stale token -> re-derive from the roots */
  ```
  **Key macro subtlety** (source-verified): `owner` here is the *macro
  parameter* naming which of the 6 roots owns that field (e.g. `backend`,
  `session`, `execution`, from `backend_runtime_hot_fields.def`). So
  `bridge->owner` expands to `bridge->backend` / `bridge->session` / etc. —
  **the owner token IS the actual root pointer**. When the roots swap, every
  derived field's token mismatches and re-derives lazily. This is exactly why
  `PgRuntimeRestoreCurrentWorkLazy` (which only swaps the 6 roots, no GUC
  rebind) is correct: swapping the root invalidates all its dependents for free.

So a stale bridge is **already safe**: it re-derives. The only requirement is
that the 6 root pointers reflect the running fiber.

---

## 3. Where the roots come from at resume (the crux)

`PgRuntimeCurrentBridgeState` is thread-local. On carrier OS thread T it holds
whatever fiber last set it on T. If fiber F migrates T1→T2, T2's bridge reflects
some *other* fiber. F must see ITS roots. Today two mechanisms set the roots for
the resuming fiber:

### 3.1 Manual wait-boundary seams (the ones that actually matter, source-verified)

- `waiteventset.c:1483` → `xtc_pg_wait_fd()` (`pg_xtc_carrier.c:1223`):
  ```c
  PgRuntimeSaveCurrentWork(&snap);
  rc = xtc_proc_wait_fd(fd, interest, timeout_ns, &revents);   /* PARK */
  ... PgRuntimeRestoreCurrentWork(&snap);   /* on resume, before touching PG state */
  ```
  (also the no-fd sleep branch, lines 1252-1254.)
- `method_xtc.c:131/150/154` (AIO wait) — save/restore around the park.
- `fd.c:543-545 / 602-604` (fsync/fdatasync) — save/restore around the park.

**Every cooperative yield a backend fiber has is one of these seams**, and each
already saves before the park and restores on resume. These seams run
*unconditionally* (unlike the global hook, which proc.c transiently reverts to
its own on any concurrent spawn — that race is documented at
`pg_xtc_carrier.c:698-705` and is precisely why the seams remain the
"belt-and-suspenders correctness guarantee").

### 3.2 The ctx hook

Covers ANY switch, *including involuntary SIGVTALRM preemption* — the one class
of switch the seams cannot cover (it happens at an arbitrary instruction, not at
a seam). The hook is documented as **"no-op-equivalent while pinned"**
(`pg_xtc_carrier.c:576-587`): a pinned fiber resumes on the same loop with the
same session, so the roots restored equal the roots that were current.

**Conclusion:** while pinned, the ctx hook is dead weight (seams do the real
work). Its value is entirely prospective — the migrating + involuntarily-
preempted case. That is Phase D territory, gated on `migratable` landing.

---

## 4. Cost: the make-or-break number

Microbench `/tmp/xtc_self_cost/bench.c`, `cc -O2`, linked against the actual
1.24.0 shared lib (matches PG's link), single-thread dev host, 5e8 iters,
3 runs, stable:

| Path | ns/read |
|------|--------:|
| bridge fast path (TLS root read + owner-token pointer compare) | **~0.35** |
| `xtc_self()` (cross-.so call, reads TLS `__current_proc`, returns pid) | **~1.76** |
| `xtc_self()` + `xtc_pid_eq` (Option-2 self-validate) | **~1.76** |

`xtc_self()` is a real exported function (`proc.c:1250`, `return __current_proc
!= NULL ? __current_proc->pid : XTC_PID_NONE;`) — not inlined across the .so, so
it costs a call + a TLS load + an 8-byte struct return.

**Interpretation vs the earlier eager-vs-lazy doc** (plan-doc "Fiber-ctx-hook
AUDIT + COST": roots-only 0.8 ns/switch, eager-full 3832 ns/switch, lazy
5 ns/switch; eager ≈ 3.8 cores at ~1M switches/s → erases the win; lazy ≈ 0.4%):

- **Option 2 (self-validate every hot read against `xtc_self()`)**: adds
  ~1.4 ns to *every* hot read of the ~130 derived fields (work_mem, MyProc,
  CurrentMemoryContext, ...). These are read at instruction frequency, not
  switch frequency — orders of magnitude more often than the ~1M switches/s.
  A ~5x tax on the hottest reads in the backend is **catastrophically** more
  expensive than the lazy hook. **DISQUALIFIED.** The elimination must be
  ≤ lazy-hook cost; per-read validation is ~1000x over budget on the hot path.

- **The recommended path adds ZERO per-read cost**: it keeps the bridge fast
  path exactly as-is (~0.35 ns). The repoint happens at the seam (already there)
  and, for the unpin case, at *most once per resume* via a cheap per-thread
  "current-proc changed?" stamp compared once (not per hot read). At ~1M
  resumes/s a single `xtc_self()`-class check per resume is ~1.76 ns × 1e6 =
  ~0.18% of one core — same order as the lazy hook, and only armed when
  migration is live.

---

## 5. The three options, judged

### Option 1 — Hang PG's roots off the proc, resolve `CurrentPg*` via the proc

*Would* be the cleanest ("embrace the model"): `CurrentPgRuntime` resolves from
`xtc_self()`→proc→per-proc storage instead of a thread-local cell.

**Blocked by cost + missing API:**
- Resolving *every* `CurrentPg*` via `xtc_self()` is the ~1.76 ns tax (§4). The
  roots are read almost as often as the derived fields. Too slow to be the
  primary accessor.
- **No public per-proc storage exists in v1.25** (verified §6). Even if the cost
  were acceptable, there is nowhere to hang `PgThreadBackendRuntimeState` off
  the proc via a supported API.

So Option 1 in its pure form is out. Its *good idea* — per-proc storage that
survives migration — survives as the libxtc request feeding the recommended
path's lazy stamp.

### Option 2 — Self-validating cache (owner keyed on current proc)

Extend the owner-token so every read compares against `xtc_self()`/proc and
re-derives on mismatch. **DISQUALIFIED by §4**: ~5x per-hot-read tax.

### Option 3 (RECOMMENDED) — Eliminate the hook; seams cover pinned; lazy per-resume stamp covers unpin

- **Now (pinned world):** delete the ctx hook and its `__xtc_fiber_ctx_*`
  dependency. The manual seams already repoint on every cooperative yield;
  involuntary-preemption switches need no repoint while pinned (same thread,
  same roots). This is a pure removal — the hook is documented no-op-equivalent
  while pinned, and the seams are the standing correctness guarantee.
  → **v1.25.0 links cleanly; `migratable` is available.**
- **Unpin (Phase D):** the involuntary-preemption-across-migration switch is the
  only case the seams miss. Cover it with a **lazy, generation-checked repoint**
  driven by a per-thread "current-proc changed" stamp, refreshed by the FIRST
  hot access after a resume — not a chained switch hook. The stamp source and
  the roots-for-this-proc lookup both want **public per-proc userdata** (the
  libxtc request). Cost: one cheap check per resume, only when migration is
  live; zero per-read tax.

---

## 6. Verify-before-requesting: no public per-proc mechanism in v1.25

Searched `/home/gburd/ws/xtc/src/inc` (v1.25.0, tag 913f3c49 / commit 8fe0155),
public installed headers only (`xtc_*.h`, `xtc.h`; excluding `*_int.h`):

- **Per-proc userdata / setspecific / getspecific / proc-key: NONE.** The only
  `user_data` in `xtc_proc.h` is the `xtc_match_fn` selective-receive callback
  arg (line 63) and `xtc_pg_members` walk cookie — not per-proc storage. The
  only other `userdata` is `xtc_tls.h passphrase_userdata` (TLS config,
  unrelated).
- **Public switch/resume hook: NONE.** `__xtc_proc_ctx_save/restore` and
  `__xtc_fiber_ctx_*` are `__`-prefixed, doc'd library-internal
  (`xtc_proc.h:602`), and now `local:` in the map.
- **Public "current proc opaque pointer": NONE.** The only public "which proc am
  I" is `xtc_self()` → `xtc_pid_t` (`xtc_proc.h:178`, impl `proc.c:1250`). It
  reads the thread-local `__current_proc` (`proc.c:515`) which libxtc's own
  internal proc-ctx hook keeps tracking the running fiber across migration
  (`__xtc_proc_ctx_save/restore`, `proc.c:1267-1276`). `xtc_pid_t` is 8 bytes
  (loop_id/local_id/gen), comparable via `xtc_pid_eq` (3 int compares).
- **Public no-steal primitive: EXISTS** (`xtc_proc_critical_enter/leave`,
  `xtc_proc.h:453-454`) — for the Phase B affine-section guards, not for root
  resolution. `xtc_proc_at_exit` (line 465) is public and already used.

**Confirmed:** the interim elimination needs **no** new API (seams cover
pinned). The Phase-D unpin path is cleanest with a **new public per-proc
`void*` userdata**; request drafted at
`/tmp/libxtc-per-proc-userdata-request.md`.

---

## 7. Migration path to v1.25.0

### 7.1 Interim (this is the unblock — pinned, process byte-for-byte)

1. **Remove the ctx hook** from `pg_xtc_carrier.c`:
   - delete the two externs (597-598), `xtc_pg_fiber_ctx_save/restore`, the
     install/clear/is_current helpers, the `g_xtc_prev_ctx_*` / blob
     machinery, and the `XtcPgFiberCtx` blob in `xtc_carrier_proc`.
   - keep `xtc_pg_affine_section_reset()` at fiber entry/exit (Phase B, driven
     by `xtc_proc_at_exit`, independent of the ctx hook).
   - keep the manual seams (`xtc_pg_wait_fd`, `method_xtc.c`, `fd.c`) — they are
     now the sole repoint and were always the standing guarantee.
   - `PgRuntimeSaveCurrentWork/RestoreCurrentWork[Lazy]` and
     `PgCurrentWorkSnapshot` stay (used by the seams and Phase D).
2. **Bump the pin** to v1.25.0 (`flake.nix:14` rev e944d00 → 913f3c49; refresh
   `flake.lock`). Fresh `nix develop` shell (RUNPATH hygiene: the plan-doc's
   1.8.0-leak lesson — verify `ldd build/src/backend/postgres | grep xtc` →
   xtc-1.25.0 before trusting any runtime validation).
3. **Wire `migratable`** into `xtc_proc_opts_t` at the spawn site
   (`pg_xtc_carrier.c:491` `xtc_proc_spawn_monitor`), defaulted **off**
   (`.migratable = 0` = pinned = byte-identical) — this is Phase C, ABI-neutral.
   Do NOT flip to 1 (that is Phase D, and needs §7.2).

Validation gate (per AGENTS.md): 0 warnings; `ldd` → 1.25.0; process
`gmake check` 245/245; `gmake check-threaded` + `check-threaded-world-core`;
`test_backend_runtime` 0 Fail; two-reviewer. This is correctness-neutral: the
hook was no-op-equivalent while pinned, so removing it changes nothing observable
while `migratable=0`.

### 7.2 Phase D (unpin — separate gated change, needs the per-proc API OR the fallback)

- Land the libxtc per-proc-userdata API (request in `/tmp`).
- PG: at `xtc_carrier_proc` entry, `xtc_proc_set_userdata(&fc->snap)` (or a
  pointer to the fiber's runtime_state). On any switch that could have migrated,
  the first hot access re-derives the bridge if a cheap per-thread stamp shows
  "current proc changed" — read once via the per-proc userdata / `xtc_self()`,
  compared, then the seams'/stamp's lazy restore does the root swap. Zero
  per-read tax.
- Flip `.migratable = 1`; add the Phase B no-steal guards live
  (`xtc_proc_critical_enter/leave` around the OpenSSL ERR span, the sigprocmask
  windows). Re-run A/B benchmarks (user gates).

### 7.3 Fallback if libxtc declines the per-proc API

Static-link libxtc (`libxtc.a` exists in the build dirs; the version-script
`local:` only hides from the `.so`, so a static link still sees
`__xtc_fiber_ctx_*`). **Not recommended:** re-couples us to symbols libxtc
declared internal; a future libxtc refactor removes them and re-breaks us; and
it fights an explicit ABI decision (`xtc_proc.h:602`). Only take this if Phase D
is urgent and the API is rejected.

**Recommendation ordering:** (1) do §7.1 now — it unblocks v1.25.0 + migratable
with a pure removal and no new API; (2) send the per-proc-userdata request for
§7.2; (3) hold static-link as the last resort.

---

## Appendix — evidence index

- Hook: `pg_xtc_carrier.c` 597-598 (externs), 831-847 (install/assign),
  708-735 (save), 772-815 (restore), 576-587 & 698-705 (no-op-while-pinned +
  global-reset race), 1223-1273 (`xtc_pg_wait_fd` seam save/restore).
- Manual seams: `waiteventset.c:1483`; `method_xtc.c:131/150/154`;
  `fd.c:543-545/602-604`.
- Bridge: `backend_runtime_current.h` 116-146 (struct), 148 (`PG_THREAD_LOCAL`),
  231-287 (`MaybeRef` fast path + owner-token = root pointer), 403-411
  (`CurrentPg*` = raw root reads).
- Root install/lazy restore: `backend_runtime.c` 300-360 (hot cell/field load),
  566-643 (`PgRuntimeSetCurrentWork` / `RestoreCurrentWork[Lazy]`).
- Link mode: `meson.build:1140` (`dependency('xtc')`); `ldd` → `libxtc.so.1`.
- libxtc v1.25: `libxtc.map` (23242a7, `local: *`); `xtc_proc.h:96` (migratable),
  178 (`xtc_self`), 602 (internal-ctx doc), 453-454 (critical_enter/leave),
  465 (`at_exit`); `proc.c:515` (`__current_proc` TLS), 1250 (`xtc_self`),
  1267-1276 (proc-ctx save/restore keeps `__current_proc` across migration).
- Cost bench: `/tmp/xtc_self_cost/bench.c` (0.35 ns fast path vs 1.76 ns
  `xtc_self()`), linked against xtc-1.24.0 shared lib.
