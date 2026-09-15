/*-------------------------------------------------------------------------
 *
 * pg_fts_sm.h
 *		pg_fts's namespaced view of the vendored sparsemap library.
 *
 * Always include this instead of vendor/sm.h directly.  It defines
 * SPARSEMAP_PREFIX so every sparsemap public symbol is renamed to
 * __pg_bm25_<name> (e.g. sm_add -> __pg_bm25_sm_add).  This prevents dynamic
 * linker collisions if another extension in the same backend also links its
 * own copy of sparsemap.  The vendored sm.c defines the same prefix, so the
 * definitions and these declarations resolve to the same namespaced symbols.
 *
 * IDENTIFICATION
 *	  contrib/pg_fts/pg_fts_sm.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_FTS_SM_H
#define PG_FTS_SM_H

#ifndef SPARSEMAP_PREFIX
#define SPARSEMAP_PREFIX __pg_bm25_
#endif

/* expose the sm_t layout so callers can stack-allocate maps (sm_init/sm_open) */
#define SM_EXPOSE_STRUCT

#include "vendor/sm.h"

#endif							/* PG_FTS_SM_H */
