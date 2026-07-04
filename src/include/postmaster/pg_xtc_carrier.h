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

#endif							/* USE_XTC_CARRIER */
#endif							/* PG_XTC_CARRIER_H */
