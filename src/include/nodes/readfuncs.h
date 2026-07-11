/*-------------------------------------------------------------------------
 *
 * readfuncs.h
 *	  header file for read.c and readfuncs.c. These functions are internal
 *	  to the stringToNode interface and should not be used by anyone else.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/nodes/readfuncs.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef READFUNCS_H
#define READFUNCS_H

#include "nodes/nodes.h"

#ifdef DEBUG_NODE_TESTS_ENABLED
/*
 * xtc-carrier: alias restore_location_fields to a stable inline that calls the
 * real extern accessor (backend_runtime_nodes.c).  This must survive a LATER
 * include of backend_runtime.h re-defining PgCurrentNodeRestoreLocationFieldsRef
 * as the generated .def macro (which expands to ->restore_location_fields and
 * would recurse).  Capturing the extern in an inline pins the resolution here.
 */
#undef PgCurrentNodeRestoreLocationFieldsRef
extern bool *PgCurrentNodeRestoreLocationFieldsRef(void);
static inline bool *
pg_readfuncs_restore_loc_ref(void)
{
	return PgCurrentNodeRestoreLocationFieldsRef();
}
#undef restore_location_fields
#define restore_location_fields (*pg_readfuncs_restore_loc_ref())
#endif

/*
 * prototypes for functions in read.c (the lisp token parser)
 */
extern const char *pg_strtok(int *length);
extern char *debackslash(const char *token, int length);
extern void *nodeRead(const char *token, int tok_len);

/*
 * prototypes for functions in readfuncs.c
 */
extern Node *parseNodeString(void);

#endif							/* READFUNCS_H */
