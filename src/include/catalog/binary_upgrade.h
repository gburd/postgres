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

#ifndef PgCurrentBinaryUpgradeNextPgTablespaceOidRef
extern Oid *PgCurrentBinaryUpgradeNextPgTablespaceOidRef(void);
#endif
#ifndef PgCurrentBinaryUpgradeNextPgTypeOidRef
extern Oid *PgCurrentBinaryUpgradeNextPgTypeOidRef(void);
#endif
#ifndef PgCurrentBinaryUpgradeNextArrayPgTypeOidRef
extern Oid *PgCurrentBinaryUpgradeNextArrayPgTypeOidRef(void);
#endif
#ifndef PgCurrentBinaryUpgradeNextMrngPgTypeOidRef
extern Oid *PgCurrentBinaryUpgradeNextMrngPgTypeOidRef(void);
#endif
#ifndef PgCurrentBinaryUpgradeNextMrngArrayPgTypeOidRef
extern Oid *PgCurrentBinaryUpgradeNextMrngArrayPgTypeOidRef(void);
#endif
#ifndef PgCurrentBinaryUpgradeNextHeapPgClassOidRef
extern Oid *PgCurrentBinaryUpgradeNextHeapPgClassOidRef(void);
#endif
#ifndef PgCurrentBinaryUpgradeNextHeapPgClassRelfilenumberRef
extern RelFileNumber *PgCurrentBinaryUpgradeNextHeapPgClassRelfilenumberRef(void);
#endif
#ifndef PgCurrentBinaryUpgradeNextIndexPgClassOidRef
extern Oid *PgCurrentBinaryUpgradeNextIndexPgClassOidRef(void);
#endif
#ifndef PgCurrentBinaryUpgradeNextIndexPgClassRelfilenumberRef
extern RelFileNumber *PgCurrentBinaryUpgradeNextIndexPgClassRelfilenumberRef(void);
#endif
#ifndef PgCurrentBinaryUpgradeNextToastPgClassOidRef
extern Oid *PgCurrentBinaryUpgradeNextToastPgClassOidRef(void);
#endif
#ifndef PgCurrentBinaryUpgradeNextToastPgClassRelfilenumberRef
extern RelFileNumber *PgCurrentBinaryUpgradeNextToastPgClassRelfilenumberRef(void);
#endif
#ifndef PgCurrentBinaryUpgradeNextPgEnumOidRef
extern Oid *PgCurrentBinaryUpgradeNextPgEnumOidRef(void);
#endif
#ifndef PgCurrentBinaryUpgradeNextPgAuthidOidRef
extern Oid *PgCurrentBinaryUpgradeNextPgAuthidOidRef(void);
#endif
#ifndef PgCurrentBinaryUpgradeRecordInitPrivsRef
extern bool *PgCurrentBinaryUpgradeRecordInitPrivsRef(void);
#endif

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
