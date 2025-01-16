/*-------------------------------------------------------------------------
 *
 * pg_toast_rel.h
 *	  toasters and TOAST relations system catalog (pg_toast_rel)
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_toast_rel.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_TOAST_REL_H
#define PG_TOAST_REL_H

#include "catalog/genbki.h"
#include "catalog/pg_toast_rel_d.h"
#include "utils/relcache.h"

/* ----------------
 *		pg_toast_rel definition.  cpp turns this into
 *		typedef struct FormData_pg_toast_rel
 * ----------------
 */
CATALOG(pg_toast_rel,9881,ToastrelRelationId)
{
	Oid			oid;			   /* oid */
   Oid			toasteroid;		/* oid */
   Oid			relid;		   /* oid */
   Oid			toastentid;		/* oid */
   int16			attnum;		   /* oid */
   int16       version;
   NameData	   relname;		   /* original table name */
   NameData	   toastentname;	/* toast storage entity name */
   char		   flag;	         /* Cleanup flag */
	char		   toastoptions;	/* Toast options */
} FormData_pg_toast_rel;

/* ----------------
 *		Form_pg_toast_rel corresponds to a pointer to a tuple with
 *		the format of pg_toast_rel relation.
 * ----------------
 */
typedef FormData_pg_toast_rel *Form_pg_toast_rel;

DECLARE_UNIQUE_INDEX_PKEY(pg_toast_rel_oid_index, 9882, ToastrelOidIndexId, pg_toast_rel, btree(oid oid_ops));
DECLARE_UNIQUE_INDEX(pg_toast_rel_name_index, 9883, ToastrelKeyIndexId, pg_toast_rel, btree(toasteroid oid_ops, relid oid_ops, version int2_ops, attnum int2_ops));
DECLARE_INDEX(pg_toast_rel_rel_index, 9884, ToastrelRelIndexId, pg_toast_rel, btree(relid oid_ops, attnum int2_ops));
DECLARE_INDEX(pg_toast_rel_tsr_index, 9885, ToastrelTsrIndexId, pg_toast_rel, btree(toasteroid oid_ops));

MAKE_SYSCACHE(TOASTRELKEY, pg_toast_rel_name_index, 16);
MAKE_SYSCACHE(TOASTRELOID, pg_toast_rel_oid_index, 16);

#endif							/* PG_TOAST_REL_H */
