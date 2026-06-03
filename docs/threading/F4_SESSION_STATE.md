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
| 4 | `utils/cache/inval.c` | `InvalState` / `inval_state` | 7 | All file-local statics: the two invalidation-message arrays, the transaction-invalidation info pointer, and the syscache/relcache callback registries (list + count + links). The two callback arrays were declared via inline anonymous `struct SYSCACHECALLBACK`/`RELCACHECALLBACK` tags; these are lifted to named typedefs (`SyscacheCallback`, `RelcacheCallback`) so they can be struct members. Deliberately excluded (not `session_local`): `inplaceInvalInfo`, the `RELSYNCCALLBACK` registry, and the `debug_discard_caches` GUC. |
| 5 | `catalog/namespace.c` | `NamespaceState` / `namespace_state` | 14 | All file-local statics: the active and base search-path state (`activeSearchPath`, `activeCreationNamespace`, `activeTempCreationPending`, `activePathGeneration`, `baseSearchPath`, `baseCreationNamespace`, `baseTempCreationPending`, `namespaceUser`, `baseSearchPathValid`), the search-path cache validity/context (`searchPathCacheValid`, `SearchPathCacheContext`), and the per-session temp-namespace OIDs (`myTempNamespace`, `myTempToastNamespace`, `myTempNamespaceSubID`). |
| 6 | `storage/ipc/procarray.c` | `ProcArrayState` / `proc_array_state` | 16 | All file-local statics, zero externs: the `TransactionIdIsInProgress` cache (`cachedXidIsNotInProgress`), `standbySnapshotPendingXmin`, the four `GlobalVisState` horizons (`GlobalVisSharedRels`/`GlobalVisCatalogRels`/`GlobalVisDataRels`/`GlobalVisTempRels`), `ComputeXidHorizonsResultLastXmin`, and the nine `xc_*` XID-cache counters. The counters and their struct members stay wrapped in `#ifdef XIDCACHE_DEBUG`; the `xc_*_inc()` macros and `DisplayXidCache` are rewritten to `proc_array_state.<field>`. The `pg_global` vars in the same decl region (`allProcs`, `KnownAssignedXids`, `KnownAssignedXidsValid`, `latestObservedXid`) are deliberately left out — they are shared, not session-local. |
| 7 | `libpq/be-secure-gssapi.c` | `GssState` / `gss_state` | 10 | All file-local statics, zero externs: the GSS send/recv/result buffer cursors (`PqGSSSendBuffer`/`PqGSSSendLength`/`PqGSSSendNext`/`PqGSSSendConsumed`, `PqGSSRecvBuffer`/`PqGSSRecvLength`, `PqGSSResultBuffer`/`PqGSSResultLength`/`PqGSSResultNext`) and `PqGSSMaxPktSize`. The identically-named symbols in the frontend `interfaces/libpq/fe-secure-gssapi.c` are an independent `#define PqGSSSendBuffer (conn->gss_SendBuffer)` family — no shared linkage. The whole file is GSSAPI-only (`ENABLE_GSS`); since this toolchain has no krb5, compile was verified with `cc -fsyntax-only -DENABLE_GSS -DHAVE_GSSAPI_H` against a minimal gssapi stub plus the project includes (and `-Wdeclaration-after-statement`). srclint (text-based) covers the file and is `0 new`; threadcheck's compile DB does not include it. |
| 8 | `utils/mb/mbutils.c` | `MbState` / `mb_state` | 9 | All file-local statics, zero externs: the conversion-proc list (`ConvProcList`), the active to-server/to-client/UTF-8-to-server conversion `FmgrInfo` pointers (`ToServerConvProc`/`ToClientConvProc`/`Utf8ToServerConvProc`), the currently-selected client/database/message encodings (`ClientEncoding`/`DatabaseEncoding`/`MessageEncoding`), and the startup-deferral flags (`backend_startup_complete`, `pending_client_encoding`). The `ConvProcInfo` struct typedef above the block is a type, not a var, and is left in place. The `\bDatabaseEncoding\b` etc. word-boundary rewrite correctly skips the `Get*`/`Set*`/`Initialize*`/`Prepare*` function names. (The `DatabaseEncoding` mentions in `utils/adt/ascii.c` and `bin/pg_dump/pg_dump_sort.c` are unrelated prose comments.) |
| 9 | `utils/cache/ts_cache.c` | `TsCacheState` / `ts_cache_state` | 7 | All file-local statics, zero externs: the three text-search cache hashtables and their last-used-entry pointers (`TSParserCacheHash`/`lastUsedParser`, `TSDictionaryCacheHash`/`lastUsedDictionary`, `TSConfigCacheHash`/`lastUsedConfig`) and the current-config OID cache (`TSCurrentConfigCache`). The adjacent GUC `TSCurrentConfig` (`session_guc`, the `default_text_search_config` setting) is deliberately left out — folding it in would change its threading class. Word boundaries keep `\bTSCurrentConfig\b` (the GUC) distinct from `\bTSCurrentConfigCache\b` (the folded var). |
| 10 | `utils/misc/timeout.c` | `TimeoutState` / `timeout_state` | 7 | All file-local statics, zero externs: the `all_timeouts[MAX_TIMEOUTS]` reason array and its `all_timeouts_initialized` flag, the active-timeout queue (`num_active_timeouts`, `active_timeouts[MAX_TIMEOUTS]`), and the timer-signal-handler state (`alarm_enabled`, `signal_pending`, `signal_due_at`). The `volatile` / `volatile sig_atomic_t` qualifiers on the signal-handler-touched members are preserved on the struct members. `session_local` keeps them thread-local, so the per-thread timer handler still sees its own copies. The `disable_alarm()` / `enable_alarm()` macros (defined just after the struct) are rewritten to `timeout_state.alarm_enabled`; the `timeout_params` typedef above is a type, not a var, and is left in place. |

| 11 | `utils/cache/relmapper.c` | `RelMapState` / `relmap_state` | 6 | All file-local statics, zero externs: the shared and local active/pending relation-map files (`shared_map`, `local_map`, `active_shared_updates`, `active_local_updates`, `pending_shared_updates`, `pending_local_updates`). The `SerializedActiveRelMaps` typedef (used for parallel-worker serialization) has members named identically to two of the folded statics; its field accesses (`relmaps->active_shared_updates`, `relmaps->active_local_updates`) are NOT folded — a post-substitution revert of `->relmap_state.` / `.relmap_state.` restores struct-field accesses while leaving the `relmap_state.` global accesses intact. |

| 12 | `utils/adt/formatting.c` | `FormatState` / `format_state` | 6 | All file-local statics, zero externs: the two formatting-picture caches — the date/time cache (`DCHCache[DCH_CACHE_ENTRIES]`, `n_DCHCache`, `DCHCounter`) and the number cache (`NUMCache[NUM_CACHE_ENTRIES]`, `n_NUMCache`, `NUMCounter`). The `DCHCacheEntry` / `NUMCacheEntry` element typedefs above the decls are types not vars and are left in place; the `\bDCHCache\b` / `\bNUMCache\b` word boundaries skip the `*CacheEntry` type names. |

| 13 | `utils/cache/syscache.c` | `SysCacheState` / `sys_cache_state` | 6 | All file-local statics, zero externs: the system-cache pointer array (`SysCache[SysCacheSize]`), the `CacheInitialized` flag, and the two sorted relation-OID arrays with their sizes (`SysCacheRelationOid`/`SysCacheRelationOidSize`, `SysCacheSupportingRelOid`/`SysCacheSupportingRelOidSize`). `SysCacheSize` is the enum array-dimension constant (not a var) and is left untouched; the many `\bSysCache\b`-prefixed function/enum names (`SearchSysCache`, `ReleaseSysCache`, `SysCacheGetAttr`, `SysCacheIdentifier`, `SysCacheSize`, …) are skipped by the word boundary. |

| 14 | `storage/ipc/waiteventset.c` | `WaitEventState` / `wait_event_state` | 7 | All file-local statics, zero externs, but heavily platform-`#if`-gated. The seven members live in mutually-exclusive guards, each preserved on the struct member and the initializer: `waiting` (`#ifndef WIN32`, kept `volatile sig_atomic_t` — the SIGURG handler reads it); `MyInterruptEvent` / `LocalInterruptEvent` (`#ifdef WIN32`, `HANDLE`); `signal_fd` (`#ifdef WAIT_USE_SIGNALFD`); `selfpipe_readfd` / `selfpipe_writefd` / `selfpipe_owner_pid` (`#ifdef WAIT_USE_SELF_PIPE`). `waiting` is an extremely common English word in this file's prose, so the comment-FP revert pass removed 19 false positives, leaving exactly the 10 real code uses. Smoke's interrupt-driven paths (pg_cancel_backend, NOTIFY, live-walsender fast-stop) exercise this machinery. |

| 15 | `storage/buffer/localbuf.c` (+ `bufmgr.h`, `buf_internals.h`, `bufmgr.c`, `hash.c`) | `LocalBufferState` / `local_buffer_state` | 7 | **First extern-bearing module — establishes the cross-file pattern.** Four of the seven vars were exported globals (`NLocBuffer`, `LocalBufferDescriptors`, `LocalBufferBlockPointers`, `LocalRefCount`) referenced from inline functions in public headers (`GetLocalBufferDescriptor` in `buf_internals.h`; `BufferIsValid` / `BufferGetBlock` in `bufmgr.h`) plus `bufmgr.c` (9 uses) and `hash.c` (1 use, `NLocBuffer`); the other three were file-local statics. **Pattern:** the `LocalBufferState` struct typedef + the single `extern PGDLLIMPORT session_local LocalBufferState local_buffer_state;` live in `bufmgr.h`, using forward declarations `struct BufferDesc;` / `struct HTAB;` for the pointer members so the struct needs no extra header includes; `buf_internals.h` (which includes `bufmgr.h`) gets the complete `BufferDesc` for its dereferencing accessor. All four separate externs were removed from the two headers and every use site (headers, `localbuf.c`, `bufmgr.c`, `hash.c`) rewritten to `local_buffer_state.<field>`. The `LocalBufHdrGetBlock` macro is rewritten too. |
| 16 | `replication/walreceiver.c` | `WalReceiverState` / `wal_receiver_state` | 7 | **Split approach** — only the seven *private* file-local statics were folded (`wrconn`, `recvFile`, `recvFileTLI`, `recvSegNo`, the anonymous `LogstreamResult` struct, the `wakeup[NUM_WALRCV_WAKEUPS]` array, `reply_message`). The exported `WalReceiverFunctions` plugin dispatch table (used by ~17 `walrcv_*` macros in `walreceiver.h` and set in `libpqwalreceiver.c`) was **left standalone** as its own `session_local` global rather than exposing private state through a public header. The anonymous `LogstreamResult` struct was kept as a nested `struct { XLogRecPtr Write; XLogRecPtr Flush; }` member; `wakeup[]` stayed an array member (with 7 prose-comment false positives — "wakeup times/reason" — reverted). The 3 `sighup_guc` GUCs (`wal_receiver_status_interval`, `wal_receiver_timeout`, `hot_standby_feedback`) and the `WalRcvWakeupReason` enum/`NUM_WALRCV_WAKEUPS` macro were left in place. Single-file, no header change. |
| 17 | `utils/time/snapmgr.c` | `SnapshotState` / `snapshot_state` | 12 | **Split approach** — folded the twelve *private* file-local statics (`CurrentSnapshotData`, `SecondarySnapshotData`, `CatalogSnapshotData`, `CurrentSnapshot`, `SecondarySnapshot`, `CatalogSnapshot`, `HistoricSnapshot`, `tuplecid_data`, `ActiveSnapshot`, `RegisteredSnapshots`, `FirstXactSnapshot`, `exportedSnapshots`) into one file-local `static session_local SnapshotState snapshot_state`. The six *exported* globals — `SnapshotSelfData` / `SnapshotAnyData` / `SnapshotToastData` (with their `SnapshotSelf` / `SnapshotAny` macros), `TransactionXmin`, `RecentXmin`, and `FirstSnapshotSet` — were **left standalone** because they have `extern` decls in `utils/snapmgr.h` and are read from many other `.c` files. The struct is placed after the `ActiveSnapshotElt` / `ExportedSnapshot` typedefs and the `xmin_cmp` forward declaration so its members (`ActiveSnapshotElt *`, `pairingheap RegisteredSnapshots = {&xmin_cmp,...}`) resolve. Cross-file matches for the private `static` names (`CatalogSnapshot`, `ActiveSnapshot`, `FirstXactSnapshot`, `tuplecid_data`) were all prose comments or a same-named function parameter (`heapam.h`), not real references. Single-file, no header change; 18 prose-comment false positives reverted. |
| 18 | `replication/logical/worker.c` | `ApplyWorkerState` / `apply_worker_state` | 11 | **Split approach** — folded the eleven *private* file-local statics (`apply_error_callback_arg`, `LogicalStreamingContext`, `MySubscriptionValid`, `on_commit_wakeup_workers_subids`, `remote_final_lsn`, `in_streamed_transaction`, `stream_xid`, `parallel_stream_nchanges`, `skip_xact_finish_lsn`, `stream_fd`, `subxact_data`) into one file-local `static session_local ApplyWorkerState apply_worker_state`. The seven *exported* globals — `apply_error_context_stack`, `ApplyMessageContext`, `ApplyContext`, `LogRepWorkerWalRcvConn`, `MySubscription`, `in_remote_transaction`, `InitializingApplyWorker` — were **left standalone** because they are declared in `replication/worker_internal.h` and read from the other logical-replication `.c` files (`applyparallelworker.c`, `tablesync.c`, `launcher.c`, etc.). The struct is placed after the `SubXactInfo` / `ApplySubXactData` typedefs so the `subxact_data` member resolves; the `is_skipping_changes()` macro (defined above the struct) was hand-rewritten to `apply_worker_state.skip_xact_finish_lsn`. The `apply_error_callback_arg` multi-line designated initializer became a nested designator in the instance init. The plain `static last_flushpos` (not `session_local`) was left alone. Single-file, no header change; 5 prose-comment false positives reverted. |

## Verification gates (every step)

| Gate | Command |
|---|---|
| Default build | `ninja -C build` |
| Multithreaded build | `ninja -C build-mt` (`-Dmultithreaded=true`) |
| Smoke (incl. live-walsender fast-stop) | `bash /tmp/smoke.sh` |
| Thread classification | `threadcheck.py . build-tc` → `0 new` |
| Source lints | `srclint.py .` → `0 new` |
