/*-------------------------------------------------------------------------
 *
 * pg_matview_ivm.h
 *	  definition of the "incrementally maintained materialized view" system
 *	  catalog (pg_matview_ivm)
 *
 * One row per materialized view created WITH (incremental_view_maintenance),
 * holding the maintenance program compiled from its query at creation.
 *
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_matview_ivm.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_MATVIEW_IVM_H
#define PG_MATVIEW_IVM_H

#include "catalog/genbki.h"
#include "catalog/pg_matview_ivm_d.h"	/* IWYU pragma: export */

/* ----------------
 *		pg_matview_ivm definition.  cpp turns this into
 *		typedef struct FormData_pg_matview_ivm
 * ----------------
 */
BEGIN_CATALOG_STRUCT

CATALOG(pg_matview_ivm,9853,MatViewIvmRelationId)
{
	/* the materialized view */
	Oid			ivmrelid BKI_LOOKUP(pg_class);

	/* maintenance class, IVM_CLASS_xxx below */
	int16		ivmclass;

	/* base relations the view reads, sorted */
	oidvector	ivmbaserels BKI_LOOKUP(pg_class) BKI_FORCE_NOT_NULL;

#ifdef CATALOG_VARLEN			/* variable-length fields start here */
	/* the compiled maintenance program, an IvmProgram */
	pg_node_tree ivmprogram BKI_FORCE_NOT_NULL;
#endif
} FormData_pg_matview_ivm;

END_CATALOG_STRUCT

/* ----------------
 *		Form_pg_matview_ivm corresponds to a pointer to a tuple with
 *		the format of pg_matview_ivm relation.
 * ----------------
 */
typedef FormData_pg_matview_ivm *Form_pg_matview_ivm;

DECLARE_TOAST(pg_matview_ivm, 9855, 9856);

DECLARE_UNIQUE_INDEX_PKEY(pg_matview_ivm_relid_index, 9854, MatViewIvmRelidIndexId, pg_matview_ivm, btree(ivmrelid oid_ops));

MAKE_SYSCACHE(MATVIEWIVMRELID, pg_matview_ivm_relid_index, 8);

#ifdef EXPOSE_TO_CLIENT_CODE

/*
 * Maintenance classes.  A class 1 view's maintenance reads only the change
 * and the view's own rows; class 2 also reads base tables (joins, and
 * aggregates that are recomputed per group).
 */
#define IVM_CLASS_SELF_MAINTAINABLE		1
#define IVM_CLASS_READS_BASE			2

#endif							/* EXPOSE_TO_CLIENT_CODE */

#endif							/* PG_MATVIEW_IVM_H */
