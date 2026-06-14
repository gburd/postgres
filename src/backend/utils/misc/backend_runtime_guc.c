/*-------------------------------------------------------------------------
 *
 * backend_runtime_guc.c
 *	  Runtime bridge accessors for session-owned GUC compatibility state.
 *
 * These accessors keep GUC backing variables mapped onto the current
 * PgSession while leaving runtime construction and top-level lifecycle
 * orchestration in utils/init/backend_runtime.c.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/backend/utils/misc/backend_runtime_guc.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "utils/backend_runtime.h"
#include "../init/backend_runtime_internal.h"

int *
PgCurrentSslRenegotiationLimitRef(void)
{
	return &PgCurrentSessionConnectionGUCState()->ssl_renegotiation_limit_value;
}

char **
PgCurrentDateStyleStringRef(void)
{
	return &PgCurrentSessionDateTimeState()->datestyle_string_value;
}

char **
PgCurrentTimeZoneAbbreviationsStringRef(void)
{
	return &PgCurrentSessionDateTimeState()->timezone_abbreviations_string_value;
}

bool *
PgCurrentDefaultWithOidsRef(void)
{
	return &PgCurrentSessionGeneralGUCState()->default_with_oids_value;
}

bool *
PgCurrentStandardConformingStringsRef(void)
{
	return &PgCurrentSessionGeneralGUCState()->standard_conforming_strings_value;
}

double *
PgCurrentPhonyRandomSeedRef(void)
{
	return &PgCurrentSessionGeneralGUCState()->phony_random_seed_value;
}

char **
PgCurrentSessionAuthorizationStringRef(void)
{
	return &PgCurrentSessionGeneralGUCState()->session_authorization_string_value;
}

char **
PgCurrentClientEncodingStringRef(void)
{
	return &PgCurrentSessionEncodingState()->client_encoding_string_value;
}

char **
PgCurrentServerEncodingStringRef(void)
{
	return &PgCurrentSessionEncodingState()->server_encoding_string_value;
}
