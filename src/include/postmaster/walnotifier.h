/*-------------------------------------------------------------------------
 *
 * walnotifier.h
 *	  Exports from postmaster/walnotifier.c.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 *
 * src/include/postmaster/walnotifier.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef WALNOTIFIER_H
#define WALNOTIFIER_H

pg_noreturn extern void WalNotifierMain(const void *startup_data, size_t startup_data_len);

#endif							/* WALNOTIFIER_H */
