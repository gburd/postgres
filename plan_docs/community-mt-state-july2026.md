# Community MT state (pgsql-hackers, July 2026) — inputs to our Option-B reshape

Captured 2026-07-11 via the agora MCP.  These shape what we align to / cherry-pick.

## Thomas Munro — "pg_threads.h take II" (thread 836012, 2026-07-06)

A portable C11 `<threads.h>`-style thread API in `src/port/pg_threads.h`, 14
patches.  Provides: `pg_thrd_create/join/current`, `pg_mtx_*`, `pg_cnd_*`,
`pg_barrier_*`, `thread_local`, and a narrow `pg_thrd_atexit` for thread-exit
cleanup of thread_local resources (replaces the old tss_t/pthread_key_t stuff).
Unix pthreads + Windows harmonised; kills fork()-mode + TerminateThread() in
frontend tools.  Mentions a future reusable `pg_thrd_pool`.

- **Heikki reviewed 0001-0007 and says "ready to be committed"** (minor nits:
  errno mapping, drop the barrier helper, Assert mtx type).  So pg_threads.h is
  on a near-term path INTO upstream.
- Coordinating with Jelte (interrupts/latches refactor) and Bryan (thread pool).

## Bryan Green — "pg_dump: use threads for parallel workers on all platforms"
(thread 785142, 2026-07-02)

One threaded model everywhere, coordinated by an in-process work queue (mutex +
2 condvars) instead of the fork/socket-emulation split.  Explicitly headed
toward a **reusable pg_thrd_pool** (Munro will fit his fork-ectomy to it).
Names the crash-isolation trade-off (threads give up per-worker address-space
isolation) — same trade we make, and the same fail-stop reasoning.

## heikki/threading branch (rewritten ~2026-06-25, 55 commits ahead of our
fork point; 0 patch-equivalent in xtc)

The community-track MT foundation, distinct from heikki/master:
- `Add multithreaded GUC`, `Launch thread if multithreaded GUC is set`
- `Add PG_MODULE_MAGIC_REENTRANT` + `Mark plperl/regress as reentrant`
  (his module backend-model marker; cf our PG_MODULE_MAGIC_BACKEND_MODEL_*)
- `Annotate all global variables`, `Mark in-function static variables as
  session_local`, `Add pg_static_vars tool`, `Add pgguclifetimes`
  (his global-relocation approach; cf our per-subsystem backend_runtime_* +
  PgCurrent...Ref accessors)
- `Replace Latches with Interrupts`, `use pipes for wakeups in latches`,
  `Make SendPostmasterSignal() work with threads`, `Add SessionResourceOwner`,
  `Add trivial DSM implementation for multi-threaded`

## Implications for the Option-B reshape

1. Base stays plain upstream/master (+ CI + dev-v36); we do NOT re-home onto
   heikki/threading (its foundation diverges from ours and would be weeks of
   porting).  We curate OUR validated tree into a small reviewable series.
2. Cherry-pick the IDEAS from heikki/threading that are better/more
   maintainable than ours (candidates: PG_MODULE_MAGIC_REENTRANT as a cleaner
   module model than our backend-model ordinal enum; pg_static_vars /
   pgguclifetimes tooling to audit global relocation; the interrupts-not-latches
   direction).  Adopt as ideas, adapted to our tree, layered after Sam / before
   Greg's carrier work.
3. Forward alignment (note, not now): express the carrier layer's thread + pool
   needs in terms the community is standardizing on — pg_threads.h for the raw
   thread primitive and pg_thrd_pool for the pool — so the libxtc carrier can
   sit ON pg_threads.h rather than raw pthread_create, and our reentrancy marks
   can converge with PG_MODULE_MAGIC_REENTRANT.  This is the credible-upstream
   story: libxtc as the scheduler/fiber layer ABOVE the community's
   pg_threads.h/pg_thrd_pool substrate.
