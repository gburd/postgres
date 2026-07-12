# Does Munro's pg_threads.h overlap our work? — analysis (2026-07-11)

Compared Munro's v1 patch 0002 (src/include/port/pg_threads.h, 628 lines) against
our tree.

## Short answer: YES, it directly overlaps ONE piece — our thread-portability
## shim — and it is COMPLEMENTARY (not competing) with everything else we built.

pg_threads.h is a portable C11-<threads.h>-style primitive layer:
  pg_thrd_create/join/current/equal/exit, pg_mtx_*, pg_cnd_*, pg_rwlock_*,
  pg_barrier_*, pg_once_*, thread_local, pg_thrd_atexit (thread-exit cleanup).
Pure Unix-pthreads/Windows harmonization.  NOTHING about scheduling, fibers,
sessions, carriers, cooperative yield, or backend runtime.

## The direct overlap (small, and in our favor to converge)

- OUR src/include/port/pg_thread.h + pg_pthread.h: a hand-rolled PgThread /
  pg_thread_create/join/detach/set_name/exit shim over pthread/Windows.
  This is EXACTLY what pg_threads.h replaces, and pg_threads.h is the more
  complete/idiomatic version (C11 naming, rwlock, barrier, once, atexit).
  => When pg_threads.h lands upstream, retire our pg_thread.h/pg_pthread.h and
     use pg_thrd_* underneath: pg_thread_create -> pg_thrd_create.
     Call sites: launch_backend.c:883/1007 (backend + carrier thread spawn),
     pg_xtc_carrier.c:645 (scheduler thread).  ~3 spawn sites, mechanical.

- OUR raw pthread_mutex_t / pthread_cond_t in the threaded plumbing (8 files:
  pmchild.c, launch_backend.c, pg_xtc_carrier.c, guc.c, ps_status.c,
  pg_locale.c, reloptions.c, backend_runtime_backend.c).
  => Move these to pg_mtx_t / pg_cnd_t for portability (Windows) and to stop
     duplicating what the community standardizes on.  Mechanical.

## What does NOT overlap (our genuine, distinct contribution)

pg_threads.h stops at "how do I portably make a thread + a mutex."  Everything
that makes THIS branch novel sits ABOVE that line and has no counterpart in
Munro's patch:

- the libxtc carrier layer (fibers on an N-loop scheduler pool);
- cooperative wait/yield at protocol boundaries (xtc_pg_wait_fd, the pooled
  protocol scheduler, carrier-pinned deep waits);
- io_method=xtc (per-fiber async IO);
- the backend-runtime session/execution state relocation;
- the backend-model / affine reentrancy gate;
- crash fail-stop.

libxtc is itself a thread+coroutine runtime that today calls pthread_create
directly; it does NOT need pg_threads.h.  The overlap is only in PostgreSQL's
OWN thread-spawn/mutex shim, not in libxtc.

## Conclusion / forward alignment

pg_threads.h is the community's PORTABILITY SUBSTRATE and it is complementary:
the credible upstream story is "libxtc (scheduler/fiber layer) + our
backend-runtime, sitting ABOVE the community's pg_threads.h thread primitive and
(coming) pg_thrd_pool."  Concretely, once pg_threads.h is committed:

1. Retire src/include/port/pg_thread.h + pg_pthread.h; route pg_thread_create /
   join / set_name / exit onto pg_thrd_* (3 spawn sites).
2. Convert our 8 raw pthread_mutex/cond files to pg_mtx_t / pg_cnd_t.
3. Consider using pg_thrd_atexit for carrier/backend thread-exit cleanup
   (replaces any ad-hoc TLS teardown).
4. Have libxtc's own pthread_create optionally route through pg_threads.h too
   (nice-to-have; not required — libxtc is a separate lib).

None of this is blocking; it is a portability + upstream-alignment cleanup to do
when pg_threads.h merges (Heikki has already reviewed 0001-0007 ready-to-commit).
