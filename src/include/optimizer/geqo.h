/*-------------------------------------------------------------------------
 *
 * geqo.h
 *	  prototypes for various files in optimizer/geqo
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/optimizer/geqo.h
 *
 *-------------------------------------------------------------------------
 */

/*
 * contributed by:
 * =*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=
 * *  Martin Utesch				 * Institute of Automatic Control	   *
 * =							 = University of Mining and Technology =
 * *  utesch@aut.tu-freiberg.de  * Freiberg, Germany				   *
 * =*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=
 */

#ifndef GEQO_H
#define GEQO_H

#include "common/pg_prng.h"
#include "nodes/pathnodes.h"
#include "optimizer/extendplan.h"
#include "optimizer/geqo_gene.h"
#include "utils/backend_runtime_current.h"
#include "utils/global_lifetime.h"


/* GEQO debug flag */
/*
 * #define GEQO_DEBUG
 */

/* choose one recombination mechanism here */
/*
 * #define ERX
 * #define PMX
 * #define CX
 * #define PX
 * #define OX1
 * #define OX2
 */
#define ERX


/*
 * Configuration options
 *
 * If you change these, update backend/utils/misc/postgresql.conf.sample
 */
/* 1 .. 10, knob for adjustment of defaults */
#ifndef PgCurrentGeqoEffortRef
extern int *PgCurrentGeqoEffortRef(void);
#endif

#define DEFAULT_GEQO_EFFORT 5
#define MIN_GEQO_EFFORT 1
#define MAX_GEQO_EFFORT 10

/* 2 .. inf, or 0 to use default */
#ifndef PgCurrentGeqoPoolSizeRef
extern int *PgCurrentGeqoPoolSizeRef(void);
#endif

/* 1 .. inf, or 0 to use default */
#ifndef PgCurrentGeqoGenerationsRef
extern int *PgCurrentGeqoGenerationsRef(void);
#endif

#ifndef PgCurrentGeqoSelectionBiasRef
extern double *PgCurrentGeqoSelectionBiasRef(void);
#endif

#ifndef PgCurrentGeqoPlannerExtensionIdRef
extern int *PgCurrentGeqoPlannerExtensionIdRef(void);
#endif

#define DEFAULT_GEQO_SELECTION_BIAS 2.0
#define MIN_GEQO_SELECTION_BIAS 1.5
#define MAX_GEQO_SELECTION_BIAS 2.0

/* 0 .. 1 */
#ifndef PgCurrentGeqoSeedRef
extern double *PgCurrentGeqoSeedRef(void);
#endif

#define Geqo_effort \
	(*PG_RUNTIME_CURRENT_HOT_FIELD_REF(PgCurrentGeqoEffortHotRef, \
									   CurrentPgSession, \
									   PgCurrentGeqoEffortRef))
#define Geqo_pool_size \
	(*PG_RUNTIME_CURRENT_HOT_FIELD_REF(PgCurrentGeqoPoolSizeHotRef, \
									   CurrentPgSession, \
									   PgCurrentGeqoPoolSizeRef))
#define Geqo_generations \
	(*PG_RUNTIME_CURRENT_HOT_FIELD_REF(PgCurrentGeqoGenerationsHotRef, \
									   CurrentPgSession, \
									   PgCurrentGeqoGenerationsRef))
#define Geqo_selection_bias \
	(*PG_RUNTIME_CURRENT_HOT_FIELD_REF(PgCurrentGeqoSelectionBiasHotRef, \
									   CurrentPgSession, \
									   PgCurrentGeqoSelectionBiasRef))
#define Geqo_planner_extension_id \
	(*PG_RUNTIME_CURRENT_HOT_FIELD_REF(PgCurrentGeqoPlannerExtensionIdHotRef, \
									   CurrentPgSession, \
									   PgCurrentGeqoPlannerExtensionIdRef))
#define Geqo_seed \
	(*PG_RUNTIME_CURRENT_HOT_FIELD_REF(PgCurrentGeqoSeedHotRef, \
									   CurrentPgSession, \
									   PgCurrentGeqoSeedRef))


/*
 * Private state for a GEQO run --- accessible via GetGeqoPrivateData
 */
typedef struct
{
	List	   *initial_rels;	/* the base relations we are joining */
	pg_prng_state random_state; /* PRNG state */
} GeqoPrivateData;

static inline GeqoPrivateData *
GetGeqoPrivateData(PlannerInfo *root)
{
	/* headers must be C++-compliant, so the cast is required here */
	return (GeqoPrivateData *)
		GetPlannerInfoExtensionState(root, Geqo_planner_extension_id);
}

/* routines in geqo_main.c */
extern RelOptInfo *geqo(PlannerInfo *root,
						int number_of_rels, List *initial_rels);

/* routines in geqo_eval.c */
extern Cost geqo_eval(PlannerInfo *root, Gene *tour, int num_gene);
extern RelOptInfo *gimme_tree(PlannerInfo *root, Gene *tour, int num_gene);

#endif							/* GEQO_H */
