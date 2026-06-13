/*-------------------------------------------------------------------------
 *
 * binary_upgrade.h
 *	  variables used for binary upgrades
 *
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/binary_upgrade.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef BINARY_UPGRADE_H
#define BINARY_UPGRADE_H

#include "common/relpath.h"
#include "utils/global_lifetime.h"

extern Oid *PgCurrentBinaryUpgradeNextPgTablespaceOidRef(void);
extern Oid *PgCurrentBinaryUpgradeNextPgTypeOidRef(void);
extern Oid *PgCurrentBinaryUpgradeNextArrayPgTypeOidRef(void);
extern Oid *PgCurrentBinaryUpgradeNextMrngPgTypeOidRef(void);
extern Oid *PgCurrentBinaryUpgradeNextMrngArrayPgTypeOidRef(void);
extern Oid *PgCurrentBinaryUpgradeNextHeapPgClassOidRef(void);
extern RelFileNumber *PgCurrentBinaryUpgradeNextHeapPgClassRelfilenumberRef(void);
extern Oid *PgCurrentBinaryUpgradeNextIndexPgClassOidRef(void);
extern RelFileNumber *PgCurrentBinaryUpgradeNextIndexPgClassRelfilenumberRef(void);
extern Oid *PgCurrentBinaryUpgradeNextToastPgClassOidRef(void);
extern RelFileNumber *PgCurrentBinaryUpgradeNextToastPgClassRelfilenumberRef(void);
extern Oid *PgCurrentBinaryUpgradeNextPgEnumOidRef(void);
extern Oid *PgCurrentBinaryUpgradeNextPgAuthidOidRef(void);
extern bool *PgCurrentBinaryUpgradeRecordInitPrivsRef(void);

#define binary_upgrade_next_pg_tablespace_oid \
	(*PgCurrentBinaryUpgradeNextPgTablespaceOidRef())
#define binary_upgrade_next_pg_type_oid \
	(*PgCurrentBinaryUpgradeNextPgTypeOidRef())
#define binary_upgrade_next_array_pg_type_oid \
	(*PgCurrentBinaryUpgradeNextArrayPgTypeOidRef())
#define binary_upgrade_next_mrng_pg_type_oid \
	(*PgCurrentBinaryUpgradeNextMrngPgTypeOidRef())
#define binary_upgrade_next_mrng_array_pg_type_oid \
	(*PgCurrentBinaryUpgradeNextMrngArrayPgTypeOidRef())

#define binary_upgrade_next_heap_pg_class_oid \
	(*PgCurrentBinaryUpgradeNextHeapPgClassOidRef())
#define binary_upgrade_next_heap_pg_class_relfilenumber \
	(*PgCurrentBinaryUpgradeNextHeapPgClassRelfilenumberRef())
#define binary_upgrade_next_index_pg_class_oid \
	(*PgCurrentBinaryUpgradeNextIndexPgClassOidRef())
#define binary_upgrade_next_index_pg_class_relfilenumber \
	(*PgCurrentBinaryUpgradeNextIndexPgClassRelfilenumberRef())
#define binary_upgrade_next_toast_pg_class_oid \
	(*PgCurrentBinaryUpgradeNextToastPgClassOidRef())
#define binary_upgrade_next_toast_pg_class_relfilenumber \
	(*PgCurrentBinaryUpgradeNextToastPgClassRelfilenumberRef())

#define binary_upgrade_next_pg_enum_oid \
	(*PgCurrentBinaryUpgradeNextPgEnumOidRef())
#define binary_upgrade_next_pg_authid_oid \
	(*PgCurrentBinaryUpgradeNextPgAuthidOidRef())

#define binary_upgrade_record_init_privs \
	(*PgCurrentBinaryUpgradeRecordInitPrivsRef())

#endif							/* BINARY_UPGRADE_H */
