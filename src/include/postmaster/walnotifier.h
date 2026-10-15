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

/*
 * MAX_NOTIFIER_SHARDS (the clamp for the notifier_shards GUC, and the size of
 * the per-shard state array and the postmaster's notifier child array) is
 * defined in storage/proc.h next to MAX_IO_WORKERS, because NUM_AUXILIARY_PROCS
 * must reserve a PGPROC slot for each possible notifier.
 */
#include "storage/proc.h"

/* GUC: number of WAL notifier shards / processes (PGC_POSTMASTER). */
extern PGDLLIMPORT int notifier_shards;

pg_noreturn extern void WalNotifierMain(const void *startup_data, size_t startup_data_len);

#endif							/* WALNOTIFIER_H */
