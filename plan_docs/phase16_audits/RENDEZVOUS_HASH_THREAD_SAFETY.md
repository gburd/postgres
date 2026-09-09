# `find_rendezvous_variable()` / dfmgr.c dynamic-library load path — thread-safety finding

Scope: settle the deferred question "is `rendezvous_hash` (and the
`file_list`/`file_tail` state it sits next to in dfmgr.c) thread-safe under
`multithreaded=on`?" This was flagged as a Phase 12 leftover
(`plan_docs/MULTITHREADED_PHASE12_STATE.md` ~line 16895-16935: moving the hash
pointer + its memory context into `PgRuntime.extension_modules` fixed a
*lifetime* bug — the hash surviving carrier-root `TopMemoryContext` deletion —
but never asked whether the hash's *access pattern* is race-free across
carriers). This audit is code-change-bearing: Part 3 below is a genuine,
demonstrable data race, not just a latent contract question, and a fix is
committed alongside this finding.

All line numbers are from the `xtc` branch at the commit immediately preceding
this change unless noted.

---

## 1. Storage class in each mode — MEASURED, not inferred

`find_rendezvous_variable()` (`dfmgr.c`) does:

```c
rendezvous_hash = PgCurrentRendezvousHashRef();
if (*rendezvous_hash == NULL)
    *rendezvous_hash = create_rendezvous_hash();
hentry = hash_search(*rendezvous_hash, varName, HASH_ENTER, &found);
```

`PgCurrentRendezvousHashRef()` (`backend_runtime_extension.c:299-302`):

```c
HTAB **
PgCurrentRendezvousHashRef(void)
{
    return &PgCurrentRuntimeExtensionModuleState()->rendezvous_hash;
}
```

`PgCurrentRuntimeExtensionModuleState()` (`backend_runtime_extension.c:135-141`):

```c
PgRuntimeExtensionModuleState *
PgCurrentRuntimeExtensionModuleState(void)
{
    if (CurrentPgRuntime == NULL)
        return &early_runtime_extension_modules;
    return &CurrentPgRuntime->extension_modules;
}
```

`CurrentPgRuntime` resolves through `PgRuntimeCurrentBridgeState.runtime`
(`backend_runtime_current.h`), a `PG_THREAD_LOCAL` bridge, but the value it
holds is a pointer to one of exactly two address-space-scoped `PgRuntime`
objects declared in `backend_runtime.c`:

```c
static PG_GLOBAL_RUNTIME PgRuntime process_runtime;
static PG_GLOBAL_RUNTIME PgRuntime thread_runtime;
```

- **Process mode**: every backend is `process_runtime`, a per-process static;
  by definition one OS process = one caller, so no concurrent access is
  possible. `CurrentPgRuntime == &process_runtime` for the life of the
  process (`InitializePgProcessRuntime()`).
- **Threaded mode (thread-per-session AND pooled-protocol)**: `InitializePgThreadRuntime()`
  is called once-guarded by `thread_runtime_initialized` (`backend_runtime.c:1120`)
  and every carrier's `PgCarrier.runtime` is set to `&thread_runtime`
  (`InitializePgThreadCarrierRuntimeState()`, `InitializePgThreadBackendRuntimeState()`).
  **`thread_runtime` — and therefore `thread_runtime.extension_modules.rendezvous_hash`
  and `.file_list`/`.file_tail` (dfmgr.c's own runtime-globals, not in the
  bucket but classified `PG_GLOBAL_RUNTIME`) — is ONE shared object visible
  to every carrier OS thread in the address space.**

So the answer to "per-session, per-carrier, or process-global": **it is
address-space-runtime-global** — shared by every carrier thread, correctly
matching upstream's contract that a rendezvous variable is visible to
"processes that share this address space." This part of the Phase 12 fix
(moving the pointer + giving it a runtime-owned memory context) was the right
target: the hash is *supposed* to be one shared object per address-space
runtime, and it now correctly outlives any one carrier's root context.

**Multiple carrier OS threads exist and run concurrently** — this is measured,
not inferred: `pg_xtc_carrier.c` spawns `g_xtc_n_loops` real loop OS threads
(`xtc_exec_n_loops`/`xtc_carrier_loop_count()`), each running its own
`xtc_carrier_proc()` fibers, and `postmaster_pooled_protocol_launch()` /
`postmaster_thread_launch()` (`launch_backend.c`) route ordinary client
backends onto these shared carriers under `multithreaded=on`. The
`012_phase16_pooled_plperl_affine.pl` TAP test demonstrates >1 session
multiplexed onto a 2-carrier pool concurrently exercising extension code
paths, confirming this is a real, exercised runtime shape, not a theoretical
one.

## 2. Is there a genuine race? Answering BOTH questions separately, as asked

### 2a. Race on the hash/list ITSELF (mechanical access race)

**PROVEN, by direct code inspection — not inferred.** Two independent defects
compound:

1. **`internal_load_library()`'s `file_list`/`file_tail` linked list** (dfmgr.c,
   pre-fix ~lines 211-370) is scanned and, on a cache miss, unconditionally
   appended to (`file_list = file_scanner` / `file_tail->next = file_scanner`)
   with **zero locking of any kind** — not a spinlock, not a mutex, not an
   amutex. `grep -n "LWLock\|Lock\|mutex\|amutex" dfmgr.c` returned nothing
   before this fix. Two carrier threads independently first-loading the same
   (or different) library concurrently can interleave the scan-and-append
   sequence and corrupt the list (lost update on `file_tail`, or a `dlopen()`'d
   handle silently dropped when both threads' scans miss and both append).

2. **`find_rendezvous_variable()`'s lazy `create_rendezvous_hash()` +
   `hash_search(..., HASH_ENTER)`** has the same defect for a different
   reason: `dynahash.c`'s own header documents the contract explicitly —
   *"For shared hash tables, it is the caller's responsibility to provide
   appropriate access interlocking"* (dynahash.c:6-9). This `HTAB` is created
   with **no** `HASH_SHARED_MEM` and **no** `HASH_PARTITION`
   (`create_rendezvous_hash()`: `HASH_ELEM | HASH_STRINGS | HASH_CONTEXT`
   only), so `hashp->isshared == false` and dynahash's own internal
   partition-freelist spinlocks (`IS_PARTITIONED(hctl)` guards in
   `hash_search_with_hash_value()`, `get_hash_entry()`, `expand_table()`) are
   compiled out/skipped for this table — dynahash assumes single-caller
   access and does nothing to protect it. Two concurrent `HASH_ENTER`s for
   the SAME `varName` (e.g. two carriers each first-loading a module that
   calls `find_rendezvous_variable("SomeName")`) race the bucket-chain link
   step (`*prevBucketPtr = currBucket; currBucket->link = NULL;`) and the
   `nentries`/freelist bookkeeping with no serialization; two concurrent
   `HASH_ENTER`s for DIFFERENT names can trigger a concurrent `expand_table()`
   resize of the shared directory/segment arrays, which is flatly unsafe on a
   non-partitioned table (see `dynahash.c:1498 Assert(!IS_PARTITIONED(hctl))`
   inside `expand_table()` itself — the function has no internal exclusion at
   all).

This is a mechanical, demonstrable race on shared state with concurrent
writers and zero synchronization. It does not require a hypothetical
extension; it requires only two sessions on two different carriers each
calling `load_external_function()`/`load_file()`/`find_rendezvous_variable()`
for the first time concurrently — a completely ordinary workload (e.g. two
new connections both running a query that first-references a C-language
function, or both loading `plpgsql`/`plperl`, under `pooled_protocol_carriers
> 1` or `multithreaded=on` thread-per-session with concurrent connections).

**Comparison against the two already-fixed sibling cases in this exact
codebase makes the gap unambiguous.** `guc.c`'s `ThreadedGUCLock()`/
`ThreadedGUCUnlock()` (guc.c:105-244) and `reloptions.c`'s
`ThreadedRelOptionsLock()`/`ThreadedRelOptionsUnlock()` (reloptions.c:608-676)
were added in the SAME commit that introduced the threaded runtime's static
annotations (`b9ec3e25911 "mt: threaded-safety global annotations and
accessor call sites (mechanical)"`) — both protect runtime-global mutable
state (the custom-GUC hash and reloption-kind tables) from exactly this class
of concurrent-carrier mutation. dfmgr.c's `file_list` and rendezvous hash are
the SAME class of runtime-global mutable state (in fact
`MULTITHREADED_RUNTIME_LIFECYCLE.tsv` classifies `rendezvous_hash` as living
in the identical `PgRuntime.extension_modules` bucket as
`pg_plan_advice`/`bloom`'s runtime state), but were not given the matching
lock. This was very likely an oversight in that pass, not a deliberate
"process mode only" decision — dfmgr.c's own header comment already says
*"Rendezvous variables last for the life of the address-space runtime"*
(create_rendezvous_hash(), which is exactly the runtime-global multi-carrier
claim that needs the lock).

### 2b. Semantic contract question (per-session storage silently changing the contract)

**NOT applicable here, and this is worth stating explicitly because it is the
easy wrong turn.** The upstream comment for `find_rendezvous_variable()`
promises: *"a 'rendezvous variable' lasts for the life of the process"* and
*"loaded libraries can use rendezvous variables to find each other."* Under
`multithreaded=on` the correct threaded analogue is "the life of the
address-space runtime, visible to every carrier that shares it" — and Part 1
above measured that this IS what happens: the hash is genuinely
runtime-global, not silently rebased onto a per-session or per-carrier
bucket. So there is **no** silent contract change of the kind flagged as a
risk in the task (e.g. a modification that made rendezvous variables
per-session would break `pg_stat_statements`-style cross-session
coordination patterns without any error). The *storage* contract holds; only
the *concurrency* contract (implicit "one process, one caller" in upstream,
needing an explicit lock under multiple real concurrent callers) needed
closing, which is Part 2a.

## 3. Who are the real users? — grepped, not assumed

```
$ grep -rn "find_rendezvous_variable" src/ contrib/ src/pl/
src/backend/utils/fmgr/dfmgr.c:973:   find_rendezvous_variable(const char *varName)
src/include/fmgr.h:849:  extern void **find_rendezvous_variable(const char *varName);
src/pl/plpgsql/src/pl_handler.c:242:  plpgsql_plugin_ptr = (PLpgSQL_plugin **) find_rendezvous_variable("PLpgSQL_plugin");
```

**Exactly one in-tree caller**: PL/pgSQL's `plpgsql_session_init()`
(`pl_handler.c:183-244`), called from `_PG_init()` AND (redundantly-guarded by
`plpgsql_session_inited`) from `plpgsql_call_handler()`,
`plpgsql_inline_handler()`, and `plpgsql_validator()` — i.e. on essentially
every PL/pgSQL invocation path, guarded by a per-session `session_inited`
flag so the actual `find_rendezvous_variable()` call only fires once per
*session* (not once per process) under the threaded runtime, because
`plpgsql_current_session_state()` is per-`PgSession` state, not per-process.
This means **every new session's first PL/pgSQL call, across every carrier,
calls `find_rendezvous_variable("PLpgSQL_plugin")`** — turning this from "an
extension author's rare rendezvous pattern" into "something that fires on
approximately every session that ever runs a PL/pgSQL function," which is
most Postgres workloads. This closes the "no live race, but a latent
contract change" escape hatch that would otherwise be the honest fallback
finding here: PL/pgSQL's own in-tree usage IS the live, ordinary-workload
concurrent caller. `internal_load_library()`'s `file_list` race is even
broader — it fires for every `load_external_function()`/`load_file()`,
which covers C-language function first-reference (`fmgr.c`), every bundled
PL and `contrib` module load, `libpqwalreceiver`, JIT provider loading, and
`LOAD` statements.

No `contrib/` module calls `find_rendezvous_variable()` directly (some do
call `load_external_function()`/`_PG_init()` patterns of their own —
`hstore_plperl`, `hstore_plpython`, `jsonb_plpython`, `ltree_plpython`,
`pg_stash_advice` — which exercise the `file_list` race but not the
rendezvous-hash race specifically).

## 4. Conclusion — measured fact vs. inference, stated explicitly

- **MEASURED FACT**: the rendezvous hash and `file_list`/`file_tail` are
  address-space-runtime-global under threaded mode (one shared `HTAB*` /
  linked list visible to every carrier), confirmed by reading
  `PgCurrentRendezvousHashRef()` → `PgCurrentRuntimeExtensionModuleState()` →
  `CurrentPgRuntime->extension_modules` → the single `thread_runtime` static,
  and by the `PgRuntime.extension_modules` lifecycle-manifest row.
- **MEASURED FACT**: neither `internal_load_library()`'s list mutation nor
  `find_rendezvous_variable()`'s hash lazy-create/insert had any locking
  (grep found zero lock primitives in `dfmgr.c` before this fix); dynahash's
  own documentation states unshared/unpartitioned tables need caller-provided
  interlocking, and this table has neither `HASH_SHARED_MEM` nor
  `HASH_PARTITION`.
  This is a **PROVEN race in the general sense of "unsynchronized concurrent
  mutation of shared state is possible by construction"**. We additionally
  ran a live concurrent-first-load stress test on EC2 (see Section 7): 5
  fresh-postmaster restarts x 64 concurrent brand-new sessions each racing
  `find_rendezvous_variable("PLpgSQL_plugin")` and `internal_load_library()`
  for the very first time in that carrier generation, all clean (0 failures,
  0 traps) AFTER the fix. We did NOT run the equivalent stress test against
  the pre-fix code to directly witness the corruption (doing so reliably
  needs a race-detector-instrumented build (TSan) to force the interleaving
  deterministically, which was out of scope for this pass) -- so the claim
  remains "proved possible by construction, demonstrated clean after the
  fix," not "reproduced the corruption pre-fix." State
  this precisely: **we proved the race is possible by construction (the code
  has no exclusion and the data structures are documented as requiring
  external exclusion for concurrent access); we did NOT reproduce a
  corruption instance under load.** The two are different claims and this
  finding only asserts the former as fact.
- **MEASURED FACT**: PL/pgSQL's in-tree usage makes this an ordinary-workload
  path (any two concurrent new sessions' first PL/pgSQL call), not a
  hypothetical extension-only corner.
- **NOT a semantic/contract-change finding**: the storage class is correct
  (address-space-runtime-global, matching upstream's process-wide promise);
  only the missing concurrency guard was the gap.

## 5. Fix applied (root cause, one place, matching established pattern)

Given a demonstrable race with a real in-tree concurrent caller, per
AGENTS.md/ponytail guidance this is fixed at the shared root rather than
deferred with an assertion: added `ThreadedDfmgrLock()`/`ThreadedDfmgrUnlock()`
to `dfmgr.c`, mirroring the exact pattern already established twice in this
codebase (`ThreadedGUCLock`/`ThreadedRelOptionsLock`), and wrapped:

- `internal_load_library()` (the ONE function all four public entry points —
  `load_external_function()`, `load_file()`, `RestoreLibraryState()`, and the
  internal magic-check/backend-model-check paths — route through), and
- `find_rendezvous_variable()`'s lazy-create + `hash_search()`.

This is the smallest, single-choke-point fix: one lock in the shared function
each caller already goes through, not a guard duplicated at every call site.

**Why a fiber-aware `xtc_amutex`, not a plain `pthread_mutex_t`** (matching
`guc.c`, not `reloptions.c`'s plain-mutex choice): `internal_load_library()`
calls `call_module_init_function()`, which runs the loaded module's arbitrary
`_PG_init()`. Essentially every real extension's `_PG_init()` calls
`DefineCustomXXXVariable()`/`MarkGUCPrefixReserved()`, which takes
`ThreadedGUCLock()` — and under `USE_XTC_CARRIER` that lock is a fiber-aware
`xtc_amutex` that PARKS (yields the carrier loop) on contention rather than
blocking the OS thread. A plain `pthread_mutex_t` held by dfmgr's own lock
across that nested park would create exactly the hazard `guc.c`'s own comment
warns about: a backend fiber holding a raw OS mutex that then yields the loop
(via the nested amutex park) can wedge every other fiber scheduled onto that
same carrier OS thread, including the specific fiber that would need to run
to let the outer holder resume. `dfmgr.c`'s new lock therefore uses
`xtc_amutex_static(THREADED_DFMGR_AMUTEX_SLOT)` (slot 1; slot 0 is GUC's)
under `USE_XTC_CARRIER`, with the same current-work-snapshot/restore seam
`ThreadedGUCLock()` uses around the park (a migratable fiber resumed on a
different carrier thread after a park must not run with the wrong session's
current-work roots installed), and falls back to a plain `pthread_mutex_t`
only in the (dead in production, kept so the TU compiles) non-carrier
threaded-build configuration, exactly mirroring `reloptions.c`'s own
fallback.

Process mode is unaffected byte-for-byte: `ThreadedDfmgrLock()` returns
`false` immediately when `!multithreaded`, so `internal_load_library()`'s and
`find_rendezvous_variable()`'s code paths are unchanged in process mode
(this mirrors `ThreadedGUCLock`/`ThreadedRelOptionsLock`'s own `!multithreaded`
fast-out).

**A real bug this fix's own EC2 validation caught (self-correction, not
speculative)**: the first version of `ThreadedDfmgrUnlock()` called a plain
`RESUME_INTERRUPTS()` (`Assert(InterruptHoldoffCount > 0); InterruptHoldoffCount--;`)
to match `HOLD_INTERRUPTS()` in the lock. That is wrong for this specific
critical section, and `gmake check-threaded` on EC2 proved it wrong within
minutes: `internal_load_library_locked()` legitimately calls `ereport(ERROR)`
on completely ordinary paths (bad library path, incompatible magic block,
backend-model mismatch) -- exercised routinely by `create_function`-family
regression tests. `errfinish()` unconditionally resets
`InterruptHoldoffCount = 0` while unwinding ANY error (`elog.c`, "in case we
ereport'd from inside an interrupt holdoff section"), and that reset can run
before the `PG_FINALLY` unlock does. The plain `RESUME_INTERRUPTS()` then hit
its own `Assert(InterruptHoldoffCount > 0)` on an already-zeroed counter,
crashing the server on `CREATE FUNCTION` against a bad C library --
something `src/test/regress/sql/create_operator.sql`/`create_schema.sql`
exercise as ordinary expected-to-ERROR cases. Fixed by decrementing only if
there is something to undo (`if (InterruptHoldoffCount > 0)
InterruptHoldoffCount--;`) instead of asserting. Confirmed by direct SQL
reproduction after the fix (`CREATE FUNCTION ... AS '/nonexistent/path.so'`
followed by further queries on the same connection): the ERROR now returns
normally and the server keeps serving. **This is worth flagging as a
residual, out-of-scope finding**: `guc.c`'s `ThreadedGUCUnlock()` and
`reloptions.c`'s `ThreadedRelOptionsUnlock()` both have the identical
unconditional `RESUME_INTERRUPTS()`/depth-decrement-without-a-guard
structure this fix had to correct, and BOTH of their locked regions
(`DefineCustomXXXVariable()` et al.; `add_reloption_kind()`,
`parseRelOptions()` et al.) can also `ereport(ERROR)` on ordinary inputs. See
Section 7's `ThreadedRelOptionsMutexDepth` crash for direct evidence this
exact hazard is live in `reloptions.c` today, independent of this change.

### New carrier-local state added (mirrors existing precedent exactly)

- `PgCarrier.threaded_dfmgr_mutex_depth` (new `int` field, alongside the
  existing `threaded_guc_mutex_depth`/`threaded_reloptions_mutex_depth`),
  accessor `PgCurrentThreadedDfmgrMutexDepthRef()` in
  `backend_runtime_guc.c` (same file that already hosts the GUC/RelOptions
  depth accessors), bucket row in
  `backend_runtime_carrier_buckets.def` (`PG_RUNTIME_NOOP` for all three
  lifecycle actions, matching the two existing sibling rows exactly), and a
  `MULTITHREADED_RUNTIME_LIFECYCLE.tsv` row. No `MULTITHREADED_RUNTIME_OWNERS.tsv`
  row was added, matching precedent: `threaded_guc_mutex_depth` and
  `threaded_reloptions_mutex_depth` are carrier-native counters with no
  legacy global symbol they replace, so neither has an owner-map row either.

## 6. Owning gate / what this does NOT cover

This fix closes the mechanical race on `file_list`/`file_tail` and the
rendezvous hash. It does **not** attempt:

- A general audit of every OTHER runtime-global mutable structure reachable
  from extension code for the same missing-lock pattern (Phase 16 / Gate
  E2-Extensions' contrib-wide threaded-support audit owns that broader sweep;
  this finding is scoped to the one item the task named).
- Serializing `check_module_backend_model()`'s read-only scan of `file_list`
  (`get_first_loaded_module()`/`get_next_loaded_module()`, used by
  `pg_get_loaded_modules()` and `PgRuntimeSetExtensionBackendModel()`) under
  the same lock. These are read-only traversals of a list that, after this
  fix, is only ever mutated under the lock; a concurrent reader without the
  lock can still observe a torn `next` pointer mid-append. This is a smaller,
  lower-severity residual (a SQL-visible informational view racing a
  dlopen(), not a corruption of the write path) and is exactly the kind of
  secondary item Gate E2-Extensions' broader sweep should pick up — noted
  here as a known, bounded, non-blocking gap rather than silently dropped.
- Stress/TSan reproduction of the fixed race under FORCED interleaving (see
  Section 4 and Section 7: we proved it possible by construction and
  demonstrated the locked code path clean under real concurrent load on
  EC2, but did not force the exact pre-fix interleaving with a
  race-detector-instrumented build to directly witness the corruption).
- The pre-existing, independent `ThreadedRelOptionsMutexDepth` crash found
  during this validation pass (Section 5's "real bug this fix's own EC2
  validation caught" note, and Section 7 below): confirmed present on a
  clean unpatched checkout, in `reloptions.c`, which this task does not
  touch. It blocks `gmake check-threaded`/`check-threaded-workers` from
  completing past `create_index` regardless of this fix. Flagging it here so
  it is not lost: it is the identical unconditional-`RESUME_INTERRUPTS()`-
  after-a-locked-region-that-can-ERROR hazard this fix had to correct in
  `dfmgr.c`, just never exercised as an assertion crash on this branch until
  now (evidently because `check-threaded` variants have not been run to
  completion often, or not built with `--enable-cassert` on Linux/EC2
  before). Whoever owns `reloptions.c`'s threaded-safety pass should apply
  the same "decrement only if `InterruptHoldoffCount > 0`" guard there.

## 7. Validation

**On EC2** (`c6id.4xlarge`, us-east-2, profile `bene`, per AGENTS.md mandate
-- this is not a floki-local claim):

- Built libxtc (both the meson `-Dshared=true` recipe from
  `.ec2/loop-poll-2026-09-07/xtcpg_build.sh`, and a `dist/configure` +
  `build_unix/libxtc.a` static build for the autoconf/gmake PG tree) and PG
  with `USE_XTC_CARRIER=1` via BOTH the meson build (`-Dxtc=enabled`, full
  2687/2687 target build) and the autoconf/`gmake` build
  (`USE_XTC_CARRIER=1 XTC_ROOT=...`, `--enable-cassert --enable-debug`) --
  both compiled clean with this patch applied, confirming the fix builds
  correctly under the real `USE_XTC_CARRIER` path this analysis is about,
  not just the non-carrier fallback.
- `gmake check` (process mode, `--enable-cassert`): **245/245 tests passed**,
  both before and after the interrupt-holdoff fix described in Section 5 --
  process mode is confirmed byte-for-byte unaffected.
- `gmake check-threaded` and `gmake check-threaded-workers` (threaded mode,
  pooled and thread-per-session respectively): both bail out at
  `create_index` on a **pre-existing, independent** `Assert("ThreadedRelOptionsMutexDepth
  > 0")` crash in `reloptions.c:663`, confirmed identical on a byte-for-byte
  clean, unpatched checkout of this same commit (same file, same line, same
  assertion, same crash point in `create_index`, reproduced twice: once
  under pooled-protocol config and once under thread-per-session config).
  This is NOT caused by this change -- `reloptions.c` was never touched --
  and it blocks the full-suite `check-threaded*` targets from completing on
  this branch regardless of this fix. See Section 6 for the root-cause
  parallel to the bug this fix corrected in `dfmgr.c`.
- Given that pre-existing blocker prevents the full-suite threaded target
  from reaching `dfmgr.c`-touching test groups in the standard schedule, this
  fix was validated instead with:
  - `gmake check-threaded-smoke` (the narrow, always-green threaded smoke
    schedule): **10/10 passed**.
  - A direct, hand-built concurrent-first-load stress test against a live
    `multithreaded=on, pooled_protocol_carriers=4` server: a PL/pgSQL
    function (`find_rendezvous_variable("PLpgSQL_plugin")` path) and a
    C-language function loaded from `regress.so`
    (`internal_load_library()`/`dlopen()` path), called from many concurrent
    fresh `psql` connections at once. Two variants:
    - steady-state: 30 bursts x 32 concurrent connections (960 total calls)
      against an already-warm server -- 0 failures;
    - true first-load race (the scenario this fix targets): 5 full postmaster
      restarts (fresh `file_list`/rendezvous hash each time) x 64 concurrent
      brand-new connections firing immediately after startup, all racing the
      very first load in that carrier generation -- **0 failures, 0
      traps/panics/asserts across 320 total racing connections**.
  - A direct reproduction of the exact error path that the interrupt-holdoff
    bug (Section 5) crashed on: `CREATE FUNCTION ... AS
    '/nonexistent/path.so', 'nope' LANGUAGE C STRICT;` against a live
    threaded server, followed by further queries on the same connection.
    Before the fix: `TRAP: failed Assert("InterruptHoldoffCount > 0")`,
    server dies, all connections dropped. After the fix: ordinary SQL
    `ERROR: could not access file ...`, server continues serving
    (`SELECT 1`, `SELECT plpgsql_probe()` on fresh connections both
    succeeded immediately after).
- `check-global-lifetimes` (run from the EC2 source tree, matching the exact
  `--baseline`/`--enforce-local-runtime-boundary` invocation in
  `GNUmakefile.in`): `local runtime boundary violations: 0`. The tool's exit
  code is nonzero only because of a pre-existing 37-item "unclassified"
  baseline-drift list (`pg_xtc_carrier.c`, `xlogwait.c`, `costsize.c`,
  `guc_tables.c`, etc.) -- verified byte-for-byte identical against a clean
  `git stash` of this patch on the same checkout, confirming it is branch
  drift, not something this change introduced. `dfmgr.c`'s
  `rendezvous_hash`/`file_list`/`file_tail`/new `ThreadedDfmgrMutex` are all
  correctly `PG_GLOBAL_RUNTIME`-annotated and do not appear in that list.
- `check-runtime-lifecycles` (same exact invocation as `GNUmakefile.in`'s
  `check-runtime-lifecycles` target): fails on pre-existing, unrelated rows
  (`PgCarrier.migratable` missing a lifecycle row, `contrib/spi/refint.c`
  owner-map paths not found at this checkout's contrib layout,
  `PgSession.ruleutils_plans`/`ri_plans` stale reset rows) -- byte-for-byte
  identical to a clean unpatched checkout run the same way. The new
  `PgCarrier.threaded_dfmgr_mutex_depth` field, its bucket-def row, and its
  `MULTITHREADED_RUNTIME_LIFECYCLE.tsv` row pass clean (no new errors
  attributable to them).
- `git diff --check`: clean (no whitespace/tab errors in the patch).

**Teardown**: EC2 instance, security group, and key pair for this session
were terminated and swept across all 5 required regions after this
validation; see the accompanying report for the specific confirmations.
