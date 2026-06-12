/*-------------------------------------------------------------------------
 *
 * interrupt.h
 *	  Interrupt handling routines.
 *
 * Responses to interrupts are fairly varied and many types of backends
 * have their own implementations, but we provide a few generic things
 * here to facilitate code reuse.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/include/postmaster/interrupt.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef INTERRUPT_H
#define INTERRUPT_H

#include <signal.h>

#include "utils/global_lifetime.h"

extern PGDLLIMPORT PG_THREAD_LOCAL PG_GLOBAL_BACKEND volatile sig_atomic_t ConfigReloadPending;
extern PGDLLIMPORT PG_THREAD_LOCAL PG_GLOBAL_BACKEND volatile sig_atomic_t ShutdownRequestPending;
extern PGDLLIMPORT PG_THREAD_LOCAL PG_GLOBAL_BACKEND volatile sig_atomic_t WakeupStopPending;

extern void ProcessMainLoopInterrupts(void);
extern void SignalHandlerForConfigReload(SIGNAL_ARGS);
extern void SignalHandlerForCrashExit(SIGNAL_ARGS);
extern void SignalHandlerForShutdownRequest(SIGNAL_ARGS);

#endif
