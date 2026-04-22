/*-------------------------------------------------------------------------
 *
 * recno_multixact.c
 *	  MultiXact support for RECNO access method (REMOVED)
 *
 * MultiXact support has been replaced by sLog-based concurrent tuple
 * locking (see recno_slog.c).  This file is intentionally empty.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/recno/recno_multixact.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
