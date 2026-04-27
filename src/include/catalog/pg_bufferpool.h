/*-------------------------------------------------------------------------
 *
 * pg_bufferpool.h
 *	  definition of the "buffer pool" system catalog (pg_bufferpool)
 *
 * Each row represents a named buffer pool with its own eviction algorithm.
 * The default pool ("default") uses clock-sweep and is always present.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_bufferpool.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_BUFFERPOOL_H
#define PG_BUFFERPOOL_H

#include "catalog/genbki.h"
#include "catalog/pg_bufferpool_d.h"	/* IWYU pragma: export */

/* ----------------
 *		pg_bufferpool definition.  cpp turns this into
 *		typedef struct FormData_pg_bufferpool
 * ----------------
 */
BEGIN_CATALOG_STRUCT

CATALOG(pg_bufferpool,8400,BufferPoolRelationId)
{
	Oid			oid;			/* oid */

	/* buffer pool name (unique) */
	NameData	bpname;

	/* handler function returning BufferPoolRoutine* */
	regproc		bphandler BKI_LOOKUP(pg_proc);

	/* pool size in bytes (0 means "all of shared_buffers" for default) */
	int64		bpsize;

} FormData_pg_bufferpool;

END_CATALOG_STRUCT

/* ----------------
 *		Form_pg_bufferpool corresponds to a pointer to a tuple with
 *		the format of pg_bufferpool relation.
 * ----------------
 */
typedef FormData_pg_bufferpool *Form_pg_bufferpool;

DECLARE_UNIQUE_INDEX(pg_bufferpool_bpname_index, 8401, BufferPoolNameIndexId, pg_bufferpool, btree(bpname name_ops));
DECLARE_UNIQUE_INDEX_PKEY(pg_bufferpool_oid_index, 8402, BufferPoolOidIndexId, pg_bufferpool, btree(oid oid_ops));

MAKE_SYSCACHE(BUFFERPOOLNAME, pg_bufferpool_bpname_index, 4);
MAKE_SYSCACHE(BUFFERPOOLOID, pg_bufferpool_oid_index, 4);

#endif							/* PG_BUFFERPOOL_H */
