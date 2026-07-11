# Heikki heikki/threading cherry-pick review (Option B)

Reviewed the 55 commits on heikki/threading not in xtc (0 patch-equivalent).
For each idea that overlaps our tree, adopt / align / skip, with rationale.
Adoptions land as deliberate commits AFTER Sam's layers and BEFORE Greg's
carrier layers in the reshaped series (per the plan), once approved.

## ADOPT (his is better / more maintainable / will-land-upstream)

1. PG_MODULE_MAGIC_REENTRANT model (d344ce32e5c) -- ALIGN, don't replace.
   His: int backendmodel bitmask in Pg_abi_values (PROCESS_BACKEND=1<<0,
   THREAD_BACKEND=1<<1); check: IsMultiThreaded && !(model & THREAD_BACKEND) ->
   reject.  Simpler, binary, and is the upstream-track ABI field.
   Ours: 6-level ordinal enum (PROCESS<THREAD_PER_SESSION<POOLED_SCHEDULER<
   POOLED_PROTOCOL_AFFINE<POOLED_PROTOCOL_MIGRATABLE<TASK_REENTRANT); more
   expressive (we NEED affine-vs-migratable for the pooled scheduler).
   DECISION: keep our finer enum for the pooled distinctions we require, but put
   it in / interoperate with his .backendmodel ABI field and accept his
   THREAD_BACKEND/PROCESS_BACKEND bits, so a module marked EITHER way loads
   correctly and we converge with what upstream accepts.  (Idea adopted:
   his ABI placement + binary base; our enum layered as the finer contract.)

2. pg_static_vars tool (feac5b59175, Stas Kelvich) -- ADOPT as an auditor.
   A clang-based tool (src/tools/pgguclifetimes/pg_static_vars.cpp) that finds
   mutable static/global vars.  Complements (does not replace) our
   src/tools/global_lifetime scanner; run it to catch un-annotated globals our
   scanner misses.  Low-risk additive tooling.

3. Add SessionResourceOwner to avoid fd leaks (41b25a91b6e) -- EVALUATE/ADOPT.
   A per-session ResourceOwner catching fd leaks at session end.  We fixed
   several fd/descriptor leaks ad hoc; his systematic SessionResourceOwner is
   cleaner.  Port if it doesn't collide with our session teardown.

## ALIGN LATER (his direction is where upstream is going; big refactor)

4. Replace Latches with Interrupts (1f2c31f562f, 130 files, +3658/-3568) +
   "use pipes for wakeups in latches" + Rename interrupt.c -> ipc/signal_
   handlers.c + Move CHECK_FOR_INTERRUPTS to ipc/interrupt.h.
   This is the community's interrupts-not-latches refactor (also referenced in
   Munro's pg_threads.h thread).  Our carrier wait path is built on Latch/
   WaitEventSet.  DO NOT adopt now (huge, and our wait seam works), but track
   it: when it lands upstream, our xtc_pg_wait_fd seam must move onto the
   Interrupt API.  Note in the plan as a forward-alignment item.

5. Make SendPostmasterSignal() work with threads (31ff21d20c7),
   Add trivial DSM implementation for multi-threaded (8c9acb3d4e0).
   We solved the same problems our own way (xtc_proc_wake for cross-thread
   wakes; process-mode DSM).  Compare implementations; adopt his if cleaner,
   else keep ours.  Not urgent.

## SKIP (we do it differently and ours is fine / more advanced)

6. Add multithreaded GUC (f417d1de9bb), Launch thread if multithreaded GUC set
   (3115722bbdc), Annotate all global variables (95ab320e69e), Mark
   in-function statics session_local, etc.
   We already have the multithreaded GUC and a more advanced global-relocation
   approach (per-subsystem backend_runtime_* + PgCurrent...Ref accessors + the
   pooled protocol scheduler, which his branch does not have).  His global
   annotations are the same job done differently; adopting them wholesale would
   mean re-homing onto his foundation (rejected in the Option-B decision).
   Keep ours.

7. "Enable threading by default to simplify tests" (ee244beae32) -- SKIP.
   We deliberately keep process mode the default and gate threaded behind the
   GUC; pooled is the default only *within* multithreaded=on.  Diverges from our
   validation strategy (process mode must stay green as the default).

## FORWARD ALIGNMENT (community substrate, from the pg_threads.h thread)

8. Sit the carrier layer ON port/pg_threads.h (Munro, Heikki-reviewed
   0001-0007 "ready to commit") instead of raw pg_thread_create/pthread, and
   express the carrier pool in terms of the emerging pg_thrd_pool (Bryan Green).
   Converge our reentrancy marks with PG_MODULE_MAGIC_REENTRANT (item 1).
   This is the credible-upstream story: libxtc as the scheduler/fiber layer
   ABOVE the community pg_threads.h/pg_thrd_pool substrate.  Plan item, not now.
