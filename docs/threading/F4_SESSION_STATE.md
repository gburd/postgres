# F4 - Per-session state consolidation

**Status:** in progress on branch `xtc`. The bulk `session_local` annotation
pass has landed (commit `023b0686810`); F4 now consolidates the annotated
per-session globals into named per-module structs, one module at a time. The
`multithreaded` build option / runtime GUC stays dormant and OFF by default;
each step is behavior-preserving.
**Lives:** `~/ws/postgres/xtc` (the fork worktree).
**Depends on:** F1 classification harness (`session_local` taxonomy), the F4
annotation pass.

## What and why

The annotation pass tagged ~705 globals across ~231 files `session_local`.
`session_local` expands to `__thread`, so in a threaded server each backend
thread already gets its own copy. That is sufficient for correctness, but a
flat sea of hundreds of independent thread-local variables has costs:

- **Lifecycle is implicit.** There is no single place to allocate, reset, or
  tear down a session's state as a unit.
- **TLS overhead.** Each `__thread` object consumes a TLS slot and adds
  per-access indirection; hundreds of them bloat the per-thread image.
- **No session handle.** A backend cannot hold or hand off a session's state
  as one object (useful for diagnostics and for any future model where a
  thread services more than one logical session over its lifetime).

F4 groups *related* `session_local` globals into a named struct with a single
`session_local` instance, and rewrites the use sites to reach through it. This
is the same consolidation pattern PostgreSQL already uses upstream — see
`8f1e2dfe033` *"Consolidate replication origin session globals into a single
struct"*, which collapses `replorigin_session_origin*` into one
`ReplOriginXactState replorigin_xact_state`.

## Strategy: per-module structs, incremental

A single monolithic `MySession` mega-struct would require touching all ~231
files in one unreviewable commit and would conflict badly with upstream. We
instead proceed module by module:

1. Identify a cluster of `session_local` globals that form one logical unit
   (typically the file-local statics of a single subsystem).
2. Define a `struct` for them; replace the individual globals with one
   `session_local` instance of that struct.
3. Rewrite the use sites (`foo` → `state.foo`).
4. Verify: build green (default **and** `-Dmultithreaded=true`), smoke green
   (incl. fast-stop with a live walsender), and the threadcheck / srclint
   gates report `0 new`.
5. Commit the module as one self-contained unit.

Each consolidated struct reduces the `session_local` object count by its
member count minus one. A top-level `MySession` aggregate that *holds* the
per-module sub-structs may be introduced later once enough modules are
consolidated; it is not required for any individual step and is deliberately
deferred.

### Naming

The names `Session` / `CurrentSession` are already taken by the parallel-query
DSM session (`src/include/access/session.h`), which is unrelated to F4. Any
F4-introduced aggregate must use a non-colliding name (e.g. `MySession`).
Per-module structs are named after their subsystem (e.g. `XLogInsertState`).

## Pilot: xloginsert.c WAL-insert state

The first consolidation is the WAL record-assembly state in
`src/backend/access/transam/xloginsert.c`. It is an ideal pilot: the 14
`session_local` variables are all file-local `static`s (no externs, no
cross-file references), and they already form one logical unit — the
in-progress WAL record being built by the `XLogRegister*` / `XLogInsert` API.

The variables (`registered_buffers`, `max_registered_buffers`,
`max_registered_block_id`, `mainrdata_head`, `mainrdata_last`, `mainrdata_len`,
`curinsert_flags`, `hdr_rdt`, `hdr_scratch`, `rdatas`, `num_rdatas`,
`max_rdatas`, `begininsert_called`, `xloginsert_cxt`) collapse into one
`XLogInsertState` struct with a single file-local `session_local` instance.
This drops 14 thread-local objects to 1 and gives the WAL-insert machinery an
explicit reset point (it already centralizes reset in `XLogResetInsertion()`).

## Modules consolidated

Each row is one commit. All preserve behavior and pass every gate below.

| # | Module | Struct / instance | Vars | Notes |
|---|---|---|---|---|
| 1 (pilot) | `access/transam/xloginsert.c` | `XLogInsertState` / `xlog_insert_state` | 14 | All file-local statics; in-progress WAL record. |
| 2 | `storage/file/fd.c` | `FdState` / `fd_state` | 14 | All file-local statics: VFD cache, temp-file bookkeeping, allocated/external descriptor tracking, temp-tablespace array. `temporary_files_allowed` member stays `#ifdef USE_ASSERT_CHECKING` to keep non-assert layout identical. The `FileIsValid` / `FileIsNotOpen` macros (defined before the struct) are rewritten to `fd_state.<field>` along with all other use sites. |
| 3 | `utils/cache/typcache.c` | `TypeCacheState` / `typecache_state` | 10 | All file-local statics: main type-cache hashtable, domain-type list, in-progress-domain stack, registered-record-type hashtable/array, and the tupledesc id counter. `RelIdToTypeIdCacheHash` is deliberately left out — the annotation pass kept it non-`session_local`, so pulling it in would silently change its threading class. |

## Verification gates (every step)

| Gate | Command |
|---|---|
| Default build | `ninja -C build` |
| Multithreaded build | `ninja -C build-mt` (`-Dmultithreaded=true`) |
| Smoke (incl. live-walsender fast-stop) | `bash /tmp/smoke.sh` |
| Thread classification | `threadcheck.py . build-tc` → `0 new` |
| Source lints | `srclint.py .` → `0 new` |
