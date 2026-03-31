/*
 * noxu_simple8b.c
 *		Simple-8b encoding wrapper for noxu
 *
 * This file previously contained a copy of the Simple-8b encoding/decoding
 * code from src/backend/lib/integerset.c.  The common algorithm has been
 * extracted to src/backend/lib/simple8b.c, and this file now simply
 * re-exports those functions via the noxu_simple8b.h header.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/noxu/noxu_simple8b.c
 */
#include "postgres.h"

#include "access/noxu_simple8b.h"

/*
 * All Simple-8b functions are now provided by src/backend/lib/simple8b.c
 * and declared in lib/simple8b.h.  The noxu_simple8b.h header includes
 * lib/simple8b.h, so callers get the shared implementations transparently.
 */
