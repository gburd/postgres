/*
 * src/pl/plpython/plpy_subxactobject.h
 */

#ifndef PLPY_SUBXACTOBJECT
#define PLPY_SUBXACTOBJECT

#include "nodes/pg_list.h"
#include "plpython.h"
#include "utils/backend_runtime.h"
#include "utils/resowner.h"

/*
 * A list of nested explicit subtransactions.  Option C (threaded affine): this
 * is per-session state (aliased over the backend_runtime accessor); each session
 * gets its own NIL-initialized stack head, so concurrent sessions interleaving
 * on a carrier do not share it.  A plain per-session indirection in process mode.
 */
#define explicit_subtransactions \
	(*PgCurrentPLpythonExplicitSubxactsRef())


typedef struct PLySubtransactionObject
{
	PyObject_HEAD
	bool		started;
	bool		exited;
} PLySubtransactionObject;

/* explicit subtransaction data */
typedef struct PLySubtransactionData
{
	MemoryContext oldcontext;
	ResourceOwner oldowner;
} PLySubtransactionData;

extern void PLy_subtransaction_init_type(void);
extern PyObject *PLy_subtransaction_new(PyObject *self, PyObject *unused);

#endif							/* PLPY_SUBXACTOBJECT */
