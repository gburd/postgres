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
extern PGDLLIMPORT PG_THREAD_LOCAL PG_GLOBAL_SESSION int Geqo_effort;

#define DEFAULT_GEQO_EFFORT 5
#define MIN_GEQO_EFFORT 1
#define MAX_GEQO_EFFORT 10

/* 2 .. inf, or 0 to use default */
extern PGDLLIMPORT PG_THREAD_LOCAL PG_GLOBAL_SESSION int Geqo_pool_size;

/* 1 .. inf, or 0 to use default */
extern PGDLLIMPORT PG_THREAD_LOCAL PG_GLOBAL_SESSION int Geqo_generations;

extern PGDLLIMPORT PG_THREAD_LOCAL PG_GLOBAL_SESSION double Geqo_selection_bias;

extern PGDLLIMPORT int Geqo_planner_extension_id;

#define DEFAULT_GEQO_SELECTION_BIAS 2.0
#define MIN_GEQO_SELECTION_BIAS 1.5
#define MAX_GEQO_SELECTION_BIAS 2.0

/* 0 .. 1 */
extern PGDLLIMPORT PG_THREAD_LOCAL PG_GLOBAL_SESSION double Geqo_seed;


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
