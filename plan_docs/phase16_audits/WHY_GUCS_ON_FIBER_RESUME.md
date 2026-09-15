# Why are GUCs (and 134 hot fields) restored on every fiber resume? Mostly they should NOT be.

Date: 2026-09-14. Answering the question raised while profiling the write-path wedge, where the
hottest spinner was `PgRuntimeInstallHotFieldPointers()` reached from `xtc_pg_wait_fd`'s resume.

## The short answer
Two different things happen on a fiber resume, and only one of them is defensible:

| what | why it was added | is it needed per-resume? |
|---|---|---|
| restore the **six root pointers** (runtime/carrier/backend/session/connection/execution) | the bridge is `PG_THREAD_LOCAL`, so after a park the thread may be running a *different* fiber's roots | **YES** -- load-bearing |
| eagerly reinstall **134 derived "hot field" slots** + hot cells/buckets/mirrors | speed up subsequent reads | **NO for hot fields/mirrors** -- their readers already self-invalidate |
| rebind **231 GUC `variable` pointers** (`RebindSessionGUCVariablePointers`) | `build_guc_variables()` captured raw C addresses; storage for those GUCs moved into `PgSession` | **NO per-resume** -- it is a per-session-storage-change concern |

## Why the root restore is genuinely required
`PgRuntimeCurrentBridgeState` is `PG_THREAD_LOCAL` (`backend_runtime_current.h:148`), i.e. per carrier
OS thread, not per fiber. While fiber A is parked, the loop runs fibers B/C on that same thread and
overwrites the bridge. When A resumes -- possibly on a *different* loop after a work-steal -- the
bridge points at whoever ran last. So A must repoint the roots before touching any PG state. The
snapshot is taken on A's own stack, so it rides with A across a steal. That part is correct and must
stay.

## Why the 134 hot-field reinstalls are redundant
The generated reader already validates the owner token
(`backend_runtime_current.h:263-278`):

```c
slot = bridge->variable;
if (likely(slot != NULL && bridge->variable##Owner == (const void *) bridge->owner))
    return slot;
PG_RUNTIME_BRIDGE_COUNT_FALLBACK(hot_field);
return fallback();          /* re-derives */
```

and the installer's own comment states the contract: *"Derived slots carry an owner token. Inline hot
paths must compare the token with the current owner before using the slot."*
(`backend_runtime.c:545-548`).

So each hot field is **self-invalidating**: once the roots are repointed, every stale slot fails its
owner check on first touch and re-derives itself. Eagerly rewriting all 134 slots + owner tokens on
every park/wake is work whose only benefit is avoiding a *first-touch* fallback per field -- and a
resumed fiber typically touches a handful of them, not 134. `PG_RUNTIME_HOT_MIRROR` has the same
owner-validated shape and is likewise redundant.

**Important exception -- do NOT blanket-remove:** `PG_RUNTIME_HOT_CELL` and `PG_RUNTIME_HOT_BUCKET`
readers check only `!= NULL`, with **no owner comparison**
(`backend_runtime_current.h:231-243` and `289-301`). A stale non-NULL cell/bucket left by another
fiber would be silently used -- a cross-session state bug. Those two installs must keep running on a
root switch until their readers are owner-validated too.

## Why the GUC rebind is the wrong shape for a resume
`RebindSessionGUCVariablePointer()` does, per GUC:

```c
gconf = find_option(rebind->name, false, false, PANIC);   /* hash lookup by NAME */
GUC_VARIABLE_INT(gconf) = rebind->accessor.int_ref();     /* repoint to session storage */
```

231 of these (`threaded_accessor` entries in `guc_parameters.dat`), each a `find_option()` hash
lookup + `guc_name_compare`. That is what `perf` caught burning a core:
`RebindSessionGUCVariablePointers -> RebindSessionGUCVariablePointer -> find_option -> hash_search ->
guc_name_compare`.

It is per-resume for no good reason:
- The GUC **state array is already per-session**: `guc_variable_states` is
  `(*PgCurrentGUCVariableStatesRef())`, and for builtins `GUCRecordState()` returns
  `&guc_variable_states[index]` (`guc.c:318-333`). Reads therefore already resolve through the current
  session root -- swapping the root is sufficient.
- The rebind's own doc says it exists because *"a later logical session switch must update any records
  whose backing storage moved from TLS globals into PgSession"* -- i.e. it is about the **storage
  location changing**, which happens at session setup
  (`InitializeThreadedSessionGUCOptions`, `PgCarrierAttachBackend`), not every time a fiber wakes from
  an fd park.
- `PgRuntimeRestoreCurrentWorkLazy()` already documents exactly this and skips the rebind, calling the
  eager version's cost "~3722ns / 97% of an eager refresh" and noting the one-time physical rebind
  "still happens where the backing storage location actually changes ... not on every yield."

So the eager rebind on the wait/resume path was simply the wrong variant being called. I already
switched the two fiber wait/resume sites (`xtc_pg_wait_fd` resume and the no-fd sleep resume) to the
lazy variant in `41a97e3d94`; that alone took write throughput from **198 -> 499 tps** at c=64.

## What remains to do (concrete)
1. **Split `PgRuntimeRefreshCurrentWork()`** so a *root switch* (the resume case) does only what is
   load-bearing: install hot **cells** and **buckets** (not owner-validated) and repoint the roots;
   skip hot **fields** and **mirrors** (owner-validated, self-healing) and skip the GUC rebind.
   Keep the current full-refresh for genuine session-storage changes.
2. **Measure it** with the existing `PgRuntimeBridgeFallbackStats` counters (`hot_field`, `hot_mirror`,
   `hot_cell`, `hot_bucket`): the trade is "134 eager writes per resume" vs "N first-touch fallbacks
   per resume". If `hot_field` fallbacks per resume are small (expected: single digits), the lazy path
   wins outright. Those counters already exist, so this is measurable rather than a guess.
3. **Then consider owner-validating the cell/bucket readers**, which would let the resume path skip
   those installs too and make the whole refresh unnecessary on a root switch.
4. Re-check the remaining eager `PgRuntimeRestoreCurrentWork()` call sites (`method_xtc.c`, `fd.c`,
   `dfmgr.c`, `guc.c:184`) -- they are lower-frequency (per-IO / per-dfmgr / per-SET) so they were
   deliberately left eager, but items 1-2 may make the distinction moot.

## Caveat on scope
This is a **performance** defect, not the cause of the write-path wedge. The wedge is a buffer content
lock that is acquired and never released (see `WEDGE_RCA_FINAL_HELD_BUFFER_LOCK_2026-09-14.md`);
fixing the refresh cost made the symptom less dramatic (198 -> 499 tps) but cannot fix a missing
release. Both need doing, independently.
