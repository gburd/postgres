/* -------------------------------------------------------------------------
 *
 * auth_delay.c
 *
 * Copyright (c) 2010-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		contrib/auth_delay/auth_delay.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include <limits.h>

#include "libpq/auth.h"
#include "utils/backend_runtime.h"
#include "utils/guc.h"
#include "utils/global_lifetime.h"

PG_MODULE_MAGIC_EXT(
					.name = "auth_delay",
					.version = PG_VERSION,
					PG_MODULE_MAGIC_BACKEND_MODEL_THREAD_PER_SESSION
);

#define AUTH_DELAY_SESSION_STATE_KEY "auth_delay.session"

typedef struct AuthDelaySessionState
{
	int			milliseconds;
} AuthDelaySessionState;

static AuthDelaySessionState *
auth_delay_session_state(void)
{
	return (AuthDelaySessionState *)
		PgSessionEnsureExtensionPrivateState(AUTH_DELAY_SESSION_STATE_KEY,
											 sizeof(AuthDelaySessionState),
											 NULL);
}

/* GUC Variables */
#define auth_delay_milliseconds (auth_delay_session_state()->milliseconds)

/* Original Hook */
static PG_GLOBAL_RUNTIME ClientAuthentication_hook_type original_client_auth_hook = NULL;
static PG_GLOBAL_RUNTIME bool auth_delay_hook_installed = false;

/*
 * Check authentication
 */
static void
auth_delay_checks(Port *port, int status)
{
	/*
	 * Any other plugins which use ClientAuthentication_hook.
	 */
	if (original_client_auth_hook)
		original_client_auth_hook(port, status);

	/*
	 * Inject a short delay if authentication failed.
	 */
	if (status != STATUS_OK)
	{
		pg_usleep(1000L * auth_delay_milliseconds);
	}
}

/*
 * Module Load Callback
 */
void
_PG_init(void)
{
	/* Define custom GUC variables */
	DefineCustomIntVariable("auth_delay.milliseconds",
							"Milliseconds to delay before reporting authentication failure",
							NULL,
							&auth_delay_milliseconds,
							0,
							0, INT_MAX / 1000,
							PGC_SIGHUP,
							GUC_UNIT_MS,
							NULL,
							NULL,
							NULL);

	MarkGUCPrefixReserved("auth_delay");

	/* Install Hooks */
	if (!auth_delay_hook_installed)
	{
		original_client_auth_hook = ClientAuthentication_hook;
		ClientAuthentication_hook = auth_delay_checks;
		auth_delay_hook_installed = true;
	}
}
