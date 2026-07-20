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

#endif							/* USE_XTC_CARRIER */
#endif							/* PG_XTC_CARRIER_H */
