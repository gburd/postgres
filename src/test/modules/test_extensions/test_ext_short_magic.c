/*
 * test_ext_short_magic.c
 *
 * Extension module with old-layout magic metadata.
 *
 * Portions Copyright (c) 2026, PostgreSQL Global Development Group
 */
#include "postgres.h"

#include "fmgr.h"

/*
 * Mimic a module built before Pg_magic_struct grew backend_model metadata.
 * The loader must reject this as an ABI mismatch without reading past the
 * shorter static object.
 */
typedef struct ShortPgMagicStruct
{
	int			len;
	Pg_abi_values abi_fields;
	const char *name;
	const char *version;
} ShortPgMagicStruct;

StaticAssertDecl(sizeof(ShortPgMagicStruct) ==
				 offsetof(Pg_magic_struct, backend_model),
				 "short magic block must end before backend_model");

extern PGDLLEXPORT const Pg_magic_struct *PG_MAGIC_FUNCTION_NAME(void);
const Pg_magic_struct *
PG_MAGIC_FUNCTION_NAME(void)
{
	static const ShortPgMagicStruct Pg_magic_data = {
		.len = sizeof(ShortPgMagicStruct),
		.abi_fields = PG_MODULE_ABI_DATA,
		.name = "test_ext_short_magic",
		.version = PG_VERSION
	};

	return (const Pg_magic_struct *) &Pg_magic_data;
}

extern int	no_such_variable;
