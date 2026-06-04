/*-------------------------------------------------------------------------
 *
 * mysession.h
 *	  Top-level per-session state aggregate (the "MySession" struct).
 *
 * Background
 * ----------
 * The threading re-derivation classifies every backend global as either
 * shared across the whole server (pg_global) or private to one logical
 * session (session_local, i.e. __thread under -Dmultithreaded=true).  F4
 * has been collapsing clusters of related session_local globals into named
 * per-subsystem structs (XxxState), each with a single session_local
 * instance.  See docs/threading/F4_SESSION_STATE.md.
 *
 * MySession is the top-level aggregate those per-subsystem structs are
 * intended to roll up into.  The end goal of the threading model is for a
 * thread that serves a logical session to reach all of that session's
 * mutable state through a single anchor, so that the per-session TLS
 * footprint is one object rather than hundreds.  This header introduces
 * that anchor.
 *
 * Migration is incremental and reviewable: subsystems move into MySession
 * one at a time (one commit each), exactly as the per-module XxxState
 * consolidation proceeded.  Until a subsystem is migrated it keeps its own
 * file-local session_local instance; nothing here forces a flag day.
 *
 * The names Session / CurrentSession are already taken by the parallel-query
 * DSM session (access/session.h), which is unrelated; hence MySession.
 *
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/mysession.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef MYSESSION_H
#define MYSESSION_H

#include <signal.h>				/* for sig_atomic_t */

#include "executor/instrument.h"
#include "portability/instr_time.h"
#include "utils/hsearch.h"
#include "utils/palloc.h"

/*
 * Forward declarations for subsystems whose private types are not (and need
 * not be) visible here: MySession only stores a pointer to them, so the full
 * definition stays in the owning module's own header / .c file.
 */
struct catcacheheader;			/* utils/catcache.h: CatCacheHeader */
struct PgStat_SubXactStatus;	/* utils/pgstat_internal.h */
struct ArrayAnalyzeExtraData;	/* utils/adt/array_typanalyze.c */
struct ResourceReleaseCallbackItem;	/* utils/resowner/resowner.c */
struct List;					/* nodes/pg_list.h */
struct pg_locale_struct;		/* utils/pg_locale.h: pg_locale_t */
struct EventTriggerQueryState;	/* commands/event_trigger.c */
struct ConditionVariable;		/* storage/condition_variable.h */
struct StringInfoData;			/* lib/stringinfo.h: StringInfo */
struct ProcSignalSlot;			/* storage/ipc/procsignal.c */
struct FixedParallelState;		/* access/transam/parallel.c */
struct ReplicationState;		/* replication/logical/origin.c */
struct HTAB;					/* utils/hsearch.h */
struct PendingRelDelete;		/* catalog/storage.c */
struct RelationData;			/* utils/rel.h: Relation */
struct SeqTableData;			/* commands/sequence.c */

/*
 * Saved instrumentation counters captured across a nested executor run
 * (executor/instrument.c).  Embedded by value in MySession; BufferUsage and
 * WalUsage are already visible via executor/instrument.h above.
 */
typedef struct InstrumentState
{
	BufferUsage save_pgBufferUsage;
	WalUsage	save_pgWalUsage;
} InstrumentState;

/*
 * Saved error context for SlruReportIOError (access/transam/slru.c).  Embedded
 * by value in MySession; self-contained, no external type dependencies.
 */
typedef enum
{
	SLRU_OPEN_FAILED,
	SLRU_SEEK_FAILED,
	SLRU_READ_FAILED,
	SLRU_WRITE_FAILED,
	SLRU_FSYNC_FAILED,
	SLRU_CLOSE_FAILED,
} SlruErrorCause;

typedef struct SlruState
{
	SlruErrorCause slru_errcause;
	int			slru_errno;
} SlruState;

/*
 * Pending relation create/delete/sync state (catalog/storage.c).  The element
 * types are file-local to storage.c, so the members are kept as pointers to
 * forward-declared structs.
 */
typedef struct StorageState
{
	struct PendingRelDelete *pendingDeletes;	/* head of linked list */
	struct HTAB *pendingSyncHash;
} StorageState;

/*
 * Registered extensible-node and custom-scan method tables (nodes/extensible.c).
 */
typedef struct ExtensibleState
{
	struct HTAB *extensible_node_methods;
	struct HTAB *custom_scan_methods;
} ExtensibleState;

/*
 * Logical-replication relation/partition map caches
 * (replication/logical/relation.c).
 */
typedef struct LogicalRepRelMapState
{
	MemoryContext LogicalRepRelMapContext;
	struct HTAB *LogicalRepRelMap;
	MemoryContext LogicalRepPartMapContext;
	struct HTAB *LogicalRepPartMap;
} LogicalRepRelMapState;

/*
 * Shared pg_largeobject heap and index Relation references
 * (storage/large_object/inv_api.c).
 */
typedef struct InvApiState
{
	struct RelationData *lo_heap_r;
	struct RelationData *lo_index_r;
} InvApiState;

/*
 * Per-backend sequence value cache and last-used sequence pointer
 * (commands/sequence.c).
 */
typedef struct SequenceState
{
	struct HTAB *seqhashtab;	/* hash table for SeqTable items */
	struct SeqTableData *last_used_seq;	/* last sequence used by nextval() */
} SequenceState;

/*
 * Per-transaction hash tables tracking enum types/values created or added
 * in the current (sub)transaction (catalog/pg_enum.c).
 */
typedef struct PgEnumState
{
	struct HTAB *uncommitted_enum_types;
	struct HTAB *uncommitted_enum_values;
} PgEnumState;

/*
 * Pending fsync/unlink request tracking for the checkpointer / standalone
 * backend (storage/sync/sync.c).  The CycleCtr counters are declared as
 * uint16 here (the file-local CycleCtr typedef stays in sync.c).
 */
typedef struct SyncState
{
	struct HTAB *pendingOps;
	struct List *pendingUnlinks;
	MemoryContext pendingOpsCxt; /* context for the above */

	uint16		sync_cycle_ctr;
	uint16		checkpoint_cycle_ctr;
} SyncState;

/*
 * Per-session state aggregate.
 *
 * Members are added here as subsystems migrate in.  Plain-typed members
 * (no private-type dependency) live directly in this struct; subsystems
 * with richer private state are added as sub-structs whose typedefs become
 * visible here at migration time.
 */
typedef struct MySession
{
	/*
	 * Trigger nesting depth (commands/trigger.c).  Incremented around each
	 * trigger invocation; exposed to SQL via pg_trigger_depth().
	 */
	int			trigger_depth;

	/*
	 * Total time charged to functions so far in this backend
	 * (utils/activity/pgstat_function.c).  Used to separate "self" and
	 * "other" time charges.  Initializes to zero.
	 */
	instr_time	total_func_time;

	/*
	 * WAL usage counters saved from pgWalUsage at the previous
	 * pgstat_report_wal() (utils/activity/pgstat_wal.c).  Subtracted from the
	 * current counters to compute WAL usage between reports.
	 */
	WalUsage	prev_wal_usage;

	/*
	 * Is a deadlock check pending? (storage/lmgr/proc.c)  Set by the deadlock
	 * timeout signal handler, consumed by the lock-wait loop.
	 */
	volatile sig_atomic_t got_deadlock_timeout;

	/*
	 * Memory context holding all MdfdVec objects for the md.c storage
	 * manager (storage/smgr/md.c).  Created in mdinit().
	 */
	MemoryContext md_cxt;

	/*
	 * Cache of resolved C-language function info, keyed by pg_proc OID
	 * (utils/fmgr/fmgr.c).  Lazily created; NULL until first use.
	 */
	HTAB	   *cfunc_hash;

	/*
	 * Cache of per-tablespace options, keyed by tablespace OID
	 * (utils/cache/spccache.c).  Lazily created; NULL until first use.
	 */
	HTAB	   *tablespace_cache_hash;

	/*
	 * Cache of per-attribute options, keyed by (attrelid, attnum)
	 * (utils/cache/attoptcache.c).  Lazily created; NULL until first use.
	 */
	HTAB	   *attopt_cache_hash;

	/*
	 * Catalog cache management header listing all the catalog caches
	 * (utils/cache/catcache.c).  Lazily created; NULL until first use.
	 */
	struct catcacheheader *catcache_hdr;

	/*
	 * Head of the per-(sub)transaction cumulative-stats status stack
	 * (utils/activity/pgstat_xact.c).  NULL when no transaction is active.
	 */
	struct PgStat_SubXactStatus *pgstat_xact_stack;

	/*
	 * Element-type comparison/hash lookup data shared between the array
	 * ANALYZE callbacks (utils/adt/array_typanalyze.c).  Valid only for the
	 * duration of one compute_array_stats() run.
	 */
	struct ArrayAnalyzeExtraData *array_analyze_extra;

	/*
	 * Head of the list of add-on resource-release callbacks
	 * (utils/resowner/resowner.c).  NULL when none are registered.
	 */
	struct ResourceReleaseCallbackItem *resource_release_callbacks;

	/*
	 * Cache of "missing" attribute default values, keyed by (len, value)
	 * (access/common/heaptuple.c).  Lazily created; NULL until first use.
	 */
	HTAB	   *missing_cache;

	/*
	 * Memory context holding the per-transaction local MultiXactId member
	 * cache (access/transam/multixact.c).  Created lazily as a child of
	 * TopTransactionContext; NULL when the cache is empty.
	 */
	MemoryContext multixact_cache_cxt;

	/*
	 * Bumped whenever recovery-prefetch GUCs change so that each
	 * XLogPrefetcher reconfigures itself on next use
	 * (access/transam/xlogprefetcher.c).
	 */
	int			xlog_prefetch_reconfigure_count;

	/*
	 * Hash table of WAL-referenced pages that were found missing during
	 * recovery (access/transam/xlogutils.c).  Lazily created; NULL until
	 * first use.
	 */
	HTAB	   *invalid_page_tab;

	/*
	 * Nesting depth of concurrent materialized-view incremental maintenance
	 * (commands/matview.c).  Nonzero while a REFRESH ... CONCURRENTLY is
	 * running.
	 */
	int			matview_maintenance_depth;

	/*
	 * Hash table of this backend's prepared statements, keyed by statement
	 * name (commands/prepare.c).  Plans are not shared between backends.
	 * Lazily created; NULL until first use.
	 */
	HTAB	   *prepared_queries;

	/*
	 * Cache of btree operator proof lookups for predicate testing
	 * (optimizer/util/predtest.c).  Lazily created; NULL until first use.
	 */
	HTAB	   *oprproof_cache_hash;

	/*
	 * Operator lookup cache hashtable (parser/parse_oper.c).  Maps operator
	 * name + arg types to resolved operator OID.  Lazily created; NULL until
	 * first use.
	 */
	HTAB	   *opr_cache_hash;

	/*
	 * Per-archive-cycle scratch memory context (postmaster/pgarch.c).  Reset
	 * after each WAL file is archived.  NULL until the archiver initializes.
	 */
	MemoryContext archive_context;

	/*
	 * Per-backend counter for generating speculative insertion tokens
	 * (storage/lmgr/lmgr.c).  May wrap around; only used for the short window
	 * between a speculative insert and its constraint check.
	 */
	uint32		speculative_insertion_token;

	/*
	 * Extra shared-memory bytes requested by loadable modules via
	 * RequestAddinShmemSpace (storage/ipc/ipci.c), summed during shmem sizing.
	 */
	Size		total_addin_request;

	/*
	 * Current parse position for the pg_strtok node-string reader
	 * (nodes/read.c).  Points into the string being read; NULL when idle.
	 */
	const char *pg_strtok_ptr;

	/*
	 * List of pending ON COMMIT actions for temporary tables
	 * (commands/tablecmds.c).  Each element is an OnCommitItem.
	 */
	struct List *on_commits;

	/*
	 * Locale in effect for the regexp engine's ctype probes
	 * (regex/regc_pg_locale.c).  Set at the start of regexp compile/execute.
	 */
	struct pg_locale_struct *pg_regex_locale;

	/*
	 * Top of the stack of active event-trigger execution states
	 * (commands/event_trigger.c).  NULL when no event trigger is running.
	 */
	struct EventTriggerQueryState *currentEventTriggerState;

	/*
	 * Condition variable this backend is currently prepared to sleep on
	 * (storage/lmgr/condition_variable.c).  NULL when not in a CV sleep.
	 */
	struct ConditionVariable *cv_sleep_target;

	/*
	 * Next local (virtual) transaction id to hand out for this backend
	 * (storage/ipc/sinvaladt.c).
	 */
	LocalTransactionId nextLocalTransactionId;

	/*
	 * Has this backend initialized the dynamic shared memory system yet?
	 * (storage/ipc/dsm.c).
	 */
	bool		dsm_init_done;

	/*
	 * Map remembering which relation schemas pgoutput has already sent
	 * (replication/pgoutput/pgoutput.c).  Lazily created; NULL until first use.
	 */
	HTAB	   *RelationSyncCache;

	/*
	 * Reassembly buffer for COPY data received during logical-replication
	 * table sync (replication/logical/tablesync.c).  NULL until first use.
	 */
	struct StringInfoData *copybuf;

	/*
	 * This backend's slot in the shared ProcSignal array
	 * (storage/ipc/procsignal.c).  NULL until ProcSignalInit runs.
	 */
	struct ProcSignalSlot *MyProcSignalSlot;

	/*
	 * Pointer to this parallel worker's fixed parallel state
	 * (access/transam/parallel.c); NULL in the leader / non-parallel backends.
	 */
	struct FixedParallelState *MyFixedParallelState;

	/*
	 * Replication-origin state slot this session has acquired
	 * (replication/logical/origin.c).  NULL when no origin is set up.
	 */
	struct ReplicationState *session_replication_state;

	/*
	 * Registered base-backup target types (backup/basebackup_target.c); NIL
	 * until the predefined types are loaded on first use.
	 */
	struct List *BaseBackupTargetTypeList;

	/*
	 * Registered security-label providers (commands/seclabel.c); NIL until a
	 * provider registers itself.
	 */
	struct List *label_provider_list;

	/*
	 * Saved buffer/WAL usage counters across a nested executor run
	 * (executor/instrument.c).
	 */
	InstrumentState instrument_state;

	/*
	 * Saved error context for SlruReportIOError (access/transam/slru.c).
	 */
	SlruState	slru_state;

	/*
	 * Pending relation create/delete/sync state (catalog/storage.c).
	 */
	StorageState storage_state;

	/*
	 * Pending fsync/unlink request tracking (storage/sync/sync.c).
	 */
	SyncState	sync_state;

	/*
	 * Registered extensible-node / custom-scan method tables
	 * (nodes/extensible.c).
	 */
	ExtensibleState extensible_state;

	/*
	 * Logical-replication relation/partition map caches
	 * (replication/logical/relation.c).
	 */
	LogicalRepRelMapState logicalrep_relmap_state;

	/*
	 * Shared pg_largeobject heap and index Relation references
	 * (storage/large_object/inv_api.c).
	 */
	InvApiState inv_api_state;

	/*
	 * Per-backend sequence value cache and last-used sequence pointer
	 * (commands/sequence.c).
	 */
	SequenceState sequence_state;

	/*
	 * Per-transaction hash tables tracking enum types/values created or added
	 * in the current (sub)transaction (catalog/pg_enum.c).
	 */
	PgEnumState pg_enum_state;
} MySession;

/*
 * The single per-session instance.  Defined in
 * src/backend/utils/init/globals.c alongside the other top-level session
 * globals (MyProcPid, etc.).
 */
extern PGDLLIMPORT_TLS session_local MySession MySessionData;

#endif							/* MYSESSION_H */
