/*-------------------------------------------------------------------------
 *
 * user.h
 *	  Commands for manipulating roles (formerly called users).
 *
 *
 * src/include/commands/user.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef USER_H
#define USER_H

#include "catalog/objectaddress.h"
#include "libpq/crypt.h"
#include "nodes/parsenodes.h"
#include "parser/parse_node.h"
#include "utils/backend_runtime.h"
#include "utils/guc.h"

/* GUCs */
#ifndef PgCurrentPasswordEncryptionRef
extern int *PgCurrentPasswordEncryptionRef(void);
#endif
#ifndef PgCurrentCreateRoleSelfGrantRef
extern char **PgCurrentCreateRoleSelfGrantRef(void);
#endif

#define Password_encryption (*PgCurrentPasswordEncryptionRef())
#define createrole_self_grant (*PgCurrentCreateRoleSelfGrantRef())

/* Hook to check passwords in CreateRole() and AlterRole() */
typedef void (*check_password_hook_type) (const char *username, const char *shadow_pass, PasswordType password_type, Datum validuntil_time, bool validuntil_null);

extern PGDLLIMPORT PG_GLOBAL_RUNTIME check_password_hook_type check_password_hook;

extern Oid	CreateRole(ParseState *pstate, CreateRoleStmt *stmt);
extern Oid	AlterRole(ParseState *pstate, AlterRoleStmt *stmt);
extern Oid	AlterRoleSet(AlterRoleSetStmt *stmt);
extern void DropRole(DropRoleStmt *stmt);
extern void GrantRole(ParseState *pstate, GrantRoleStmt *stmt);
extern ObjectAddress RenameRole(const char *oldname, const char *newname);
extern void DropOwnedObjects(DropOwnedStmt *stmt);
extern void ReassignOwnedObjects(ReassignOwnedStmt *stmt);
extern List *roleSpecsToIds(List *memberNames);

extern bool check_createrole_self_grant(char **newval, void **extra,
										GucSource source);
extern void assign_createrole_self_grant(const char *newval, void *extra);

#endif							/* USER_H */
