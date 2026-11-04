/*-------------------------------------------------------------------------
 * pg_xtc_xproc_demo.h -- interface to the xtc_xproc cross-fork watchdog
 *	DEMONSTRATION.  See pg_xtc_xproc_demo.c.  Gated behind USE_XTC_CARRIER
 *	AND the developer GUC xtc_xproc_watchdog_demo (default off).
 *
 *	This is a DEMO of how one WOULD build an in-tree restart-after-crash
 *	watchdog on libxtc's xtc_xproc facility.  It is NOT wired into the
 *	PostgreSQL crash-escalation policy, which stays fail-stop.
 *-------------------------------------------------------------------------
 */
#ifndef PG_XTC_XPROC_DEMO_H
#define PG_XTC_XPROC_DEMO_H

#ifdef USE_XTC_CARRIER

/*
 * Developer GUC xtc_xproc_watchdog_demo (declared in miscadmin.h, defined in
 * globals.c like the other xtc_* developer knobs).  When off (the default)
 * the demo never runs and there is zero production impact.
 */
extern void xtc_xproc_watchdog_demo_kick(void);

#endif							/* USE_XTC_CARRIER */
#endif							/* PG_XTC_XPROC_DEMO_H */
