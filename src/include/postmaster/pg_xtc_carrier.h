/*-------------------------------------------------------------------------
 * pg_xtc_carrier.h -- interface to the xtc carrier (OUT-OF-TREE spike).
 *	See pg_xtc_carrier.c.  Gated behind USE_XTC_CARRIER.
 *-------------------------------------------------------------------------
 */
#ifndef PG_XTC_CARRIER_H
#define PG_XTC_CARRIER_H

#ifdef USE_XTC_CARRIER

/* The tree's backend thread entry has this shape (void (*)(void *)). */
typedef void (*xtc_carrier_entry_fn) (void *arg);

/*
 * launch_backend.c owns the private BackendThreadStart layout; this accessor
 * recovers a fiber's own PgCarrier root (runtime_state.carrier) from the
 * opaque BackendThreadStart * the fiber received as its entry arg.  The
 * carrier is fiber-owned, so the returned pointer rides with the fiber across
 * a work-stealing steal -- the carrier layer reads it via xtc_proc_userdata()
 * for an O(1), migration-safe self-lookup that does not depend on the (maybe
 * stale after a steal) thread-local current-work bridge.
 */
struct PgCarrier;
extern struct PgCarrier *xtc_pg_backend_thread_start_carrier(void *thread_start);
/* True on the carrier thread while a backend fiber is running. */
extern __thread bool xtc_in_backend_fiber;

/* Start the single-loop xtc scheduler thread (idempotent). */
extern int	xtc_pg_carrier_start(void);

/* Spawn `entry(entry_arg)` as an xtc fiber on the carrier loop. */
extern int	xtc_pg_launch_backend_fiber(xtc_carrier_entry_fn entry,
										void *entry_arg);

/* waiteventset.c seam: yield the fiber until fd is ready / timeout. */
extern int	xtc_pg_wait_fd(int fd, int interest_pg, long timeout_ms);

/*
 * Backend exit seam.  When a backend runs as an xtc fiber, its proc_exit
 * teardown must NOT end in pg_thread_exit() (pthread_exit on the carrier
 * thread would kill the whole scheduler, not just this fiber).  Instead the
 * fiber returns control to the xtc loop via xtc_exit_self(), releasing its
 * proc slot so the Nth backend behaves exactly like the 1st.  Does not
 * return.  Only valid while xtc_in_backend_fiber is true.
 */
pg_noreturn extern void xtc_pg_backend_fiber_exit(int code);

/*
 * #7 Stage 1b escalation poll.  Returns true, once, if a per-loop supervisor
 * has observed a GENUINE backend-fiber crash (a fiber that faulted before
 * reaching its clean, already-published exit).  The postmaster polls this in
 * ServerLoop and drives the same crash policy a crashed thread carrier would
 * (ExitPostmaster under multithreaded mode).  Benign xtc_exit_self teardown
 * faults (post-clean-exit) never set the flag, so this cannot false-fire.
 */
extern bool xtc_pg_consume_genuine_crash(void);

/*
 * True iff a running backend fiber may be migrated across carriers (stolen).
 *
 * Fibers are PINNED today, so this is always false and callers can treat a
 * true result as "a future unpin has landed".  It exists so no-migrate
 * invariants (e.g. the ssl_sni server-side-SNI gate in be-secure-openssl.c,
 * which relies on TLS-bearing fibers not migrating while libxtc's ClientHello
 * context-swap #29 is deferred) can be written as active tripwires now and
 * stay correct when the gated unpin flips this to a real per-fiber query.
 * Only valid to consult while xtc_in_backend_fiber is true; false otherwise.
 */
extern bool xtc_pg_backend_fiber_is_migratable(void);

/*
 * Diagnostic: total tasks work-stolen across all carrier loops since startup.
 * Nonzero proves migratable fibers actually rebalanced across loops; 0 in
 * single-loop mode.  Used by the forced-migration stress test.
 */
extern uint64 xtc_pg_carrier_total_steals(void);

/*
 * No-steal affine-section tripwire (Phase B).
 *
 * Some short spans of backend code hold OS-thread-affine state that would be
 * wrong if the fiber resumed on a different carrier: a raw spinlock hold, the
 * OpenSSL per-thread error queue span (ERR_clear_error .. ERR_get_error), the
 * sigprocmask signal-mask windows, and per-call static scratch buffers.  The
 * audit (plan_docs/MULTITHREADED_FIBER_WORKER_DESIGN.md section 4) established
 * that NONE of these spans contains a cooperative yield point, so a fiber can
 * never PARK inside one and thus can never be stolen inside one (a running
 * fiber is never moved mid-instruction; only a parked-then-woken task migrates
 * off the deque).  They are therefore safe-by-construction while pinned AND
 * after the future unpin.
 *
 * XtcPgNoStealEnter/Leave bracket such a span and bump a per-fiber affine
 * depth; the fiber park choke points (xtc_pg_wait_fd and the fiber-ctx save
 * hook) assert the depth is zero, so if a future change ever introduces a
 * yield inside a bracketed affine span the assertion fires the instant it
 * becomes reachable -- the ssl_sni-invariant tripwire pattern, extended to the
 * park boundary.  While fibers are pinned (and in a non-assert or process
 * build) the assertion is dead; the whole mechanism compiles to nothing
 * outside USE_XTC_CARRIER + assertions, so process mode and non-fiber threaded
 * mode are byte-for-byte unchanged.
 *
 * The depth counter and the park-boundary CHECK are BOTH compiled only in
 * assert builds (USE_ASSERT_CHECKING): the whole mechanism is a tripwire, not
 * runtime enforcement, so a release build -- carrier or not -- pays nothing and
 * is byte-for-byte unchanged.
 */
#ifdef USE_ASSERT_CHECKING
extern void xtc_pg_affine_section_enter(void);
extern void xtc_pg_affine_section_leave(void);
extern int	xtc_pg_affine_section_depth(void);
extern void xtc_pg_affine_section_reset(void);
extern void xtc_pg_verify_current_work_is_self(void);
struct PgCurrentWorkSnapshot;
extern void xtc_pg_verify_snapshot_is_self(const struct PgCurrentWorkSnapshot *snap);

#define XtcPgNoStealEnter() xtc_pg_affine_section_enter()
#define XtcPgNoStealLeave() xtc_pg_affine_section_leave()
#define XtcPgVerifyCurrentWorkIsSelf() xtc_pg_verify_current_work_is_self()
#define XtcPgVerifySnapshotIsSelf(snap) xtc_pg_verify_snapshot_is_self(snap)
#else
#define XtcPgNoStealEnter() ((void) 0)
#define XtcPgNoStealLeave() ((void) 0)
#define XtcPgVerifyCurrentWorkIsSelf() ((void) 0)
#define XtcPgVerifySnapshotIsSelf(snap) ((void) (snap))
#endif

#else							/* !USE_XTC_CARRIER */

/*
 * Process build and non-carrier threaded build: the no-steal tripwire is
 * inert.  Defining the brackets as no-ops here lets affine sites in shared
 * code (be-secure-openssl.c, postgres.c) carry the annotation unconditionally
 * without an #ifdef at every call site, and keeps those TUs byte-for-byte
 * unchanged when the carrier is not compiled in.
 */
#define XtcPgNoStealEnter() ((void) 0)
#define XtcPgNoStealLeave() ((void) 0)
#define XtcPgVerifyCurrentWorkIsSelf() ((void) 0)
#define XtcPgVerifySnapshotIsSelf(snap) ((void) (snap))

#endif							/* USE_XTC_CARRIER */
#endif							/* PG_XTC_CARRIER_H */
