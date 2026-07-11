/*-------------------------------------------------------------------------
 *
 * tcopprot.h
 *	  prototypes for postgres.c.
 *
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/tcop/tcopprot.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef TCOPPROT_H
#define TCOPPROT_H

#include "nodes/params.h"
#include "nodes/plannodes.h"
#include "storage/procsignal.h"
#include "utils/backend_runtime.h"
#include "utils/guc.h"
#include "utils/queryenvironment.h"

typedef struct ExplainState ExplainState;	/* defined in explain_state.h */

/*
 * Connection output state remains source-compatible, but storage belongs to
 * PgConnection so a logical connection can move between carriers.
 */
static inline CommandDest *
PgCurrentWhereToSendOutputRefFast(void)
{
	PgConnection *connection = CurrentPgConnection;

	if (likely(connection != NULL))
		return &connection->output.where_to_send_output;
	return PgCurrentWhereToSendOutputRef();
}

#define whereToSendOutput (*PgCurrentWhereToSendOutputRefFast())
#ifndef PgCurrentPostAuthDelayRef
extern int *PgCurrentPostAuthDelayRef(void);
#endif
#define PostAuthDelay (*PgCurrentPostAuthDelayRef())
static inline int *
PgCurrentClientConnectionCheckIntervalRefFast(void)
{
	PgConnection *connection = CurrentPgConnection;

	if (likely(connection != NULL))
		return &connection->output.client_connection_check_interval;
	return PgCurrentClientConnectionCheckIntervalRef();
}

#define client_connection_check_interval (*PgCurrentClientConnectionCheckIntervalRefFast())

/*
 * Compatibility lvalue for the historical execution-local debug query string.
 * Storage belongs to the current PgExecution object.
 */
#define debug_query_string \
	(*PG_RUNTIME_CURRENT_HOT_FIELD_REF(PgCurrentDebugQueryStringHotRef, \
									   CurrentPgExecution, \
									   PgCurrentDebugQueryStringRef))

/* GUC-configurable parameters */

typedef enum
{
	LOGSTMT_NONE,				/* log no statements */
	LOGSTMT_DDL,				/* log data definition statements */
	LOGSTMT_MOD,				/* log modification statements, plus DDL */
	LOGSTMT_ALL,				/* log all statements */
} LogStmtLevel;

#ifndef PgCurrentLogDisconnectionsRef
extern bool *PgCurrentLogDisconnectionsRef(void);
#endif
#ifndef PgCurrentLogStatementRef
extern int *PgCurrentLogStatementRef(void);
#endif
#define Log_disconnections (*PgCurrentLogDisconnectionsRef())
#define log_statement (*PgCurrentLogStatementRef())

/* Flags for restrict_nonsystem_relation_kind value */
#define RESTRICT_RELKIND_VIEW			0x01
#define RESTRICT_RELKIND_FOREIGN_TABLE	0x02

#ifndef PgCurrentRestrictNonsystemRelationKindRef
extern int *PgCurrentRestrictNonsystemRelationKindRef(void);
#endif
#define restrict_nonsystem_relation_kind (*PgCurrentRestrictNonsystemRelationKindRef())

extern List *pg_parse_query(const char *query_string);
extern List *pg_rewrite_query(Query *query);
extern List *pg_analyze_and_rewrite_fixedparams(RawStmt *parsetree,
												const char *query_string,
												const Oid *paramTypes, int numParams,
												QueryEnvironment *queryEnv);
extern List *pg_analyze_and_rewrite_varparams(RawStmt *parsetree,
											  const char *query_string,
											  Oid **paramTypes,
											  int *numParams,
											  QueryEnvironment *queryEnv);
extern List *pg_analyze_and_rewrite_withcb(RawStmt *parsetree,
										   const char *query_string,
										   ParserSetupHook parserSetup,
										   void *parserSetupArg,
										   QueryEnvironment *queryEnv);
extern PlannedStmt *pg_plan_query(Query *querytree, const char *query_string,
								  int cursorOptions,
								  ParamListInfo boundParams,
								  ExplainState *es);
extern List *pg_plan_queries(List *querytrees, const char *query_string,
							 int cursorOptions,
							 ParamListInfo boundParams);

extern void die(SIGNAL_ARGS);
pg_noreturn extern void quickdie(SIGNAL_ARGS);
extern void StatementCancelHandler(SIGNAL_ARGS);
pg_noreturn extern void FloatExceptionHandler(SIGNAL_ARGS);
extern void HandleRecoveryConflictInterrupt(void);
extern void ProcessClientReadInterrupt(bool blocked);
extern void ProcessClientWriteInterrupt(bool blocked);
extern void PgSessionServiceProtocolReadWake(PgSession *session);

extern void process_postgres_switches(int argc, char *argv[],
									  GucContext ctx, const char **dbname);
pg_noreturn extern void PostgresSingleUserMain(int argc, char *argv[],
											   const char *username);
extern PgSession *PostgresBootstrapSession(const char *dbname,
										   const char *username);
extern bool PgBackendPollProtocolReadPark(PgBackend *backend,
										  uint32 *wake_events);
extern int	PgRuntimeProtocolSchedulerPollParkedReads(PgRuntime *runtime,
													 PgBackend **scratch,
													 int max_backends);
extern PgStepResult PgSessionRunProtocolSchedulerUntilBoundary(PgSession *session);
pg_noreturn extern void PostgresRunSession(PgSession *session);
pg_noreturn extern void PostgresMain(const char *dbname,
									 const char *username);
extern void ResetUsage(void);
extern void ShowUsage(const char *title);
extern int	check_log_duration(char *msec_str, bool was_logged);
extern void set_debug_options(int debug_flag,
							  GucContext context, GucSource source);
extern bool set_plan_disabling_options(const char *arg,
									   GucContext context, GucSource source);
extern const char *get_stats_option_name(const char *arg);

#endif							/* TCOPPROT_H */
