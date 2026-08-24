/*-------------------------------------------------------------------------
 *
 * backend_runtime.h
 *	  Runtime/backend/session scaffolding for backend execution.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/include/utils/backend_runtime.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef BACKEND_RUNTIME_H
#define BACKEND_RUNTIME_H

#include <signal.h>
#include <sys/resource.h>
#include <sys/time.h>

struct pollfd;

#include "access/session.h"
#include "access/skey.h"
#include "access/tupdesc.h"
#include "access/transam.h"
#include "access/xlogdefs.h"
#include "access/xlog_internal.h"
#include "common/pg_prng.h"
#include "common/relpath.h"
#include "executor/instrument.h"
#include "fmgr.h"
#include "jit/jit.h"
#include "jit/llvmjit_runtime.h"
#include "lib/ilist.h"
#include "lib/stringinfo.h"
#include "libpq/hba.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "nodes/pg_list.h"
#include "pgtime.h"
#include "port/pg_crc32c.h"
#include "port/atomics.h"
#include "replication/origin.h"
#include "storage/buf.h"
#include "storage/checksum.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/procnumber.h"
#include "storage/relfilelocator.h"
#include "storage/spin.h"
#include "tcop/dest.h"
#include "utils/backend_id.h"
#include "utils/backend_status.h"
#include "utils/datetime.h"
#include "utils/elog.h"
#include "utils/global_lifetime.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/inval.h"
#include "utils/palloc.h"
#include "utils/pgstat_internal.h"
#include "utils/sampling.h"
#include "utils/snapshot.h"
#include "utils/timeout.h"

typedef struct PgRuntime PgRuntime;
typedef struct PgCarrier PgCarrier;
typedef struct PgBackend PgBackend;
typedef struct PgBackendStatus PgBackendStatus;
typedef struct PgWaitCompletion PgWaitCompletion;
typedef struct BackgroundWorker BackgroundWorker;
typedef struct IncrementalBackupInfo IncrementalBackupInfo;
typedef struct LagTracker LagTracker;
typedef struct LogicalDecodingContext LogicalDecodingContext;
typedef struct PgSession PgSession;
typedef struct PgConnection PgConnection;
typedef struct PgExecution PgExecution;
typedef struct PQcommMethods PQcommMethods;
typedef struct WaitEventSet WaitEventSet;
typedef struct WritebackContext WritebackContext;
typedef struct BufferDesc BufferDesc;
typedef struct WalSnd WalSnd;
typedef struct WalReceiverConn WalReceiverConn;
typedef struct ReplicationSlot ReplicationSlot;
typedef struct LogicalRepWorker LogicalRepWorker;
typedef struct ParallelApplyWorkerInfo ParallelApplyWorkerInfo;
typedef struct ParallelApplyWorkerShared ParallelApplyWorkerShared;
typedef struct Subscription Subscription;
typedef struct BufFile BufFile;
typedef struct LargeObjectDesc LargeObjectDesc;
typedef struct EventTriggerQueryState EventTriggerQueryState;
typedef struct catcache CatCache;
typedef struct catcacheheader CatCacheHeader;
typedef struct CatCInProgress CatCInProgress;
typedef struct inprogressent InProgressEnt;
typedef struct TypeCacheEntry TypeCacheEntry;
typedef struct RecordCacheArrayEntry RecordCacheArrayEntry;
typedef struct TSParserCacheEntry TSParserCacheEntry;
typedef struct TSDictionaryCacheEntry TSDictionaryCacheEntry;
typedef struct TSConfigCacheEntry TSConfigCacheEntry;
typedef struct TransactionStateData TransactionStateData;
typedef struct CachedPlanSource CachedPlanSource;
typedef struct XactCallbackItem XactCallbackItem;
typedef struct SubXactCallbackItem SubXactCallbackItem;
typedef struct dsa_area dsa_area;
typedef struct dshash_table dshash_table;
typedef struct XLogReaderState XLogReaderState;
typedef struct PortalData *Portal;
typedef struct SPITupleTable SPITupleTable;
typedef struct _SPI_connection _SPI_connection;
struct _SPI_plan;
struct LogicalRepRelMapEntry;
struct SeqTableData;
struct pg_ctype_cache;
struct InvalidationInfo;
struct TransInvalidationInfo;
struct RelationData;
struct PendingRelDelete;
struct StateFileChunk;
struct avl_dbase;
struct WorkerInfoData;
struct DecodingWorker;
struct ExtensionSiblingCache;
struct PgAioBackend;
struct PgAioUringContext;
struct AllocSetContext;
struct ClientSocket;
struct AutoPrewarmSharedState;
struct pgsa_shared_state;
typedef struct dsm_segment dsm_segment;
typedef void (*PgBackendExitContinuation) (int code);
typedef int (*PgSuspendCallback) (void *callback_arg);

typedef enum PgRuntimeKind
{
	PG_RUNTIME_PROCESS,
	PG_RUNTIME_THREAD_PER_SESSION,
	PG_RUNTIME_POOLED_PROTOCOL
} PgRuntimeKind;

typedef enum PgCarrierKind
{
	PG_CARRIER_PROCESS,
	PG_CARRIER_THREAD
} PgCarrierKind;

typedef enum PgBackendLaunchModel
{
	PG_BACKEND_LAUNCH_PROCESS,
	PG_BACKEND_LAUNCH_THREAD
} PgBackendLaunchModel;

struct GlobalVisState
{
	/* XIDs >= are considered running by some backend */
	FullTransactionId definitely_needed;

	/* XIDs < are not considered to be running by any backend */
	FullTransactionId maybe_needed;
};

/*
 * Budget for one invocation of PgSessionStep().  A positive value yields after
 * that many frontend protocol messages.  A zero or negative value is
 * unbounded, which lets process and thread-per-session carriers amortize the
 * bottom error boundary across the same long-running loop shape that vanilla
 * PostgresMain() uses.  Later nonblocking schedulers can use small positive
 * budgets to preserve fairness.
 */
typedef struct PgStepBudget
{
	int			max_messages;
	bool		protocol_park_enabled;
	bool		return_logical_exits;
} PgStepBudget;

typedef enum PgStepResult
{
	PG_STEP_CONTINUE,
	PG_STEP_PARK_PROTOCOL_READ,
	PG_STEP_ERROR_RECOVERED,
	PG_STEP_DONE,
	PG_STEP_FATAL_EXIT
} PgStepResult;

/*
 * Protocol park and logical-exit results are opt-in through PgStepBudget until
 * scheduler dispatch grows beyond process/thread-per-session compatibility.
 */

typedef enum PgProtocolByteResult
{
	PG_PROTOCOL_BYTE_NONE,
	PG_PROTOCOL_BYTE_AVAILABLE,
	PG_PROTOCOL_BYTE_EOF
} PgProtocolByteResult;

typedef struct PgProtocolByteProbe
{
	unsigned char type;
	uint32		transport_wait_events;
	bool		transport_buffered_input;
	uint64		transport_generation;
} PgProtocolByteProbe;

typedef enum PgProtocolParkState
{
	PG_PROTOCOL_PARK_NONE,
	PG_PROTOCOL_PARK_PREPARED,
	PG_PROTOCOL_PARK_COMMITTED
} PgProtocolParkState;

typedef enum PgProtocolParkWakeReason
{
	PG_PROTOCOL_PARK_WAKE_NONE = 0,
	PG_PROTOCOL_PARK_WAKE_BUFFERED_INPUT = (1 << 0),
	PG_PROTOCOL_PARK_WAKE_TRANSPORT = (1 << 1),
	PG_PROTOCOL_PARK_WAKE_CLOSED = (1 << 2),
	PG_PROTOCOL_PARK_WAKE_LOGICAL = (1 << 3),
	PG_PROTOCOL_PARK_WAKE_POSTMASTER = (1 << 4),
	PG_PROTOCOL_PARK_WAKE_STALE_TRANSPORT = (1 << 5),
	PG_PROTOCOL_PARK_WAKE_TIMEOUT = (1 << 6),
	PG_PROTOCOL_PARK_WAKE_STALE_TIMEOUT = (1 << 7),
	PG_PROTOCOL_PARK_WAKE_NOTIFY = (1 << 8),
	PG_PROTOCOL_PARK_WAKE_HIBERNATE = (1 << 9)
} PgProtocolParkWakeReason;

typedef struct PgProtocolParkSpec
{
	PgBackend  *backend;
	PgSession  *session;
	PgConnection *connection;
	pgsocket	socket;
	uint32		transport_wait_events;
	bool		transport_buffered_input;
	uint64		transport_generation;
	uint64		timeout_generation;
	bool		timeout_wake_at_valid;
	TimestampTz timeout_wake_at;
	uint32		wait_event_info;
	uint64		generation;
} PgProtocolParkSpec;

typedef enum PgProtocolSchedulerQueueState
{
	PG_PROTOCOL_SCHEDULER_QUEUE_NONE,
	PG_PROTOCOL_SCHEDULER_QUEUE_PARKED_PROTOCOL_READ,
	PG_PROTOCOL_SCHEDULER_QUEUE_POLLING,
	PG_PROTOCOL_SCHEDULER_QUEUE_RUNNABLE,
	PG_PROTOCOL_SCHEDULER_QUEUE_LEASED
} PgProtocolSchedulerQueueState;

typedef struct PgProtocolSchedulerState
{
	slock_t		lock;
	dlist_head	runnable_queue;
	dlist_head	parked_protocol_queue;
	uint32		runnable_count;
	uint32		parked_protocol_count;
	uint64		runnable_enqueue_count;
	uint64		parked_protocol_enqueue_count;
	uint32		carrier_limit;
	uint32		registered_carrier_count;
	uint32		idle_carrier_count;
	uint32		active_carrier_count;
	uint64		carrier_register_count;
	uint64		carrier_reject_count;
	uint64		carrier_lease_count;
	uint64		carrier_release_count;
	uint64		same_carrier_resume_count;
	uint64		migrated_resume_count;
} PgProtocolSchedulerState;

typedef struct PgProtocolParkSnapshot
{
	PgProtocolParkState state;
	PgProtocolSchedulerQueueState scheduler_queue_state;
	uint64		generation;
	uint32		wake_reasons;
	uint32		wake_events;
	uint64		wake_generation;
	uint32		last_wake_reasons;
	uint32		last_wake_events;
	uint64		last_wake_generation;
	uint64		notify_wake_generation;
	uint64		deferred_notify_generation;
	uint64		deferred_notify_park_generation;
	uint32		deferred_notify_reasons;
	uint32		scheduler_runnable_count;
	uint32		scheduler_parked_protocol_count;
	uint64		scheduler_runnable_enqueue_count;
	uint64		scheduler_parked_protocol_enqueue_count;
	bool		carrier_attached;
	bool		session_present;
	bool		connection_present;
	bool		execution_present;
	uint32		scheduler_carrier_limit;
	uint64		scheduler_same_carrier_resume_count;
	uint64		scheduler_migrated_resume_count;
	uint32		scheduler_registered_carrier_count;
	uint32		scheduler_idle_carrier_count;
	uint32		scheduler_active_carrier_count;
	bool		last_park_duration_valid;
	long		last_park_duration_ms;
} PgProtocolParkSnapshot;

typedef struct PgBackendProtocolParkState
{
	PgProtocolParkState state;
	PgProtocolParkSpec spec;
	dlist_node	scheduler_node;
	PgProtocolSchedulerQueueState scheduler_queue_state;
	uint64		next_generation;
	uint32		wake_reasons;
	uint32		wake_events;
	uint64		wake_generation;
	uint32		last_wake_reasons;
	uint32		last_wake_events;
	uint64		last_wake_generation;
	uint64		notify_wake_generation;
	uint64		deferred_notify_generation;
	uint64		deferred_notify_park_generation;
	uint32		deferred_notify_reasons;
	TimestampTz committed_at;
	bool		last_park_duration_valid;
	long		last_park_duration_ms;
	bool		hibernated;
	PgCarrier  *parked_carrier;
} PgBackendProtocolParkState;

/*
 * Logical interrupts target a backend object first.  In process mode these are
 * bridged back to the historical volatile globals serviced by
 * CHECK_FOR_INTERRUPTS(); later backend models can route these bits without
 * depending on Unix signals as the in-process representation.
 */
typedef enum PgBackendInterruptType
{
	PG_BACKEND_INTERRUPT_QUERY_CANCEL,
	PG_BACKEND_INTERRUPT_PROC_DIE,
	PG_BACKEND_INTERRUPT_CLIENT_CONNECTION_CHECK,
	PG_BACKEND_INTERRUPT_IDLE_IN_TRANSACTION_SESSION_TIMEOUT,
	PG_BACKEND_INTERRUPT_TRANSACTION_TIMEOUT,
	PG_BACKEND_INTERRUPT_IDLE_SESSION_TIMEOUT,
	PG_BACKEND_INTERRUPT_IDLE_STATS_UPDATE_TIMEOUT,
	PG_BACKEND_INTERRUPT_PROC_SIGNAL_BARRIER,
	PG_BACKEND_INTERRUPT_LOG_MEMORY_CONTEXT,
	PG_BACKEND_INTERRUPT_RECOVERY_CONFLICT,
	PG_BACKEND_INTERRUPT_CONFIG_RELOAD,
	PG_BACKEND_INTERRUPT_SHUTDOWN_REQUEST,
	PG_BACKEND_INTERRUPT_CATCHUP,
	PG_BACKEND_INTERRUPT_NOTIFY,
	PG_BACKEND_INTERRUPT_PARALLEL_MESSAGE,
	PG_BACKEND_INTERRUPT_PARALLEL_APPLY_MESSAGE,
	PG_BACKEND_INTERRUPT_SLOT_SYNC_MESSAGE,
	PG_BACKEND_INTERRUPT_REPACK_MESSAGE,
	PG_BACKEND_INTERRUPT_WAKEUP_STOP,
	PG_BACKEND_INTERRUPT_AUTOVAC_LAUNCHER,
	PG_BACKEND_INTERRUPT_CHECKPOINTER_SHUTDOWN_XLOG,
	PG_BACKEND_INTERRUPT_LOG_ROTATE,
	PG_BACKEND_INTERRUPT_STARTUP_PROMOTE,
	PG_BACKEND_INTERRUPT_COUNT
} PgBackendInterruptType;

typedef uint32 PgBackendInterruptMask;

#define PG_BACKEND_INTERRUPT_MASK(interrupt_type) \
	(((PgBackendInterruptMask) 1) << (interrupt_type))

typedef struct PgBackendInterruptMailbox
{
	pg_atomic_uint32 pending_mask;
	pg_atomic_uint32 notify_generation;
	volatile int proc_die_sender_pid;
	volatile int proc_die_sender_uid;
} PgBackendInterruptMailbox;

typedef enum PgWaitKind
{
	PG_WAIT_KIND_NONE,
	PG_WAIT_KIND_EVENT_SET,
	PG_WAIT_KIND_SEMAPHORE
} PgWaitKind;

typedef struct PgWaitSpec
{
	PgWaitKind	kind;
	uint32		wait_event_info;
	uint32		wake_events;
	pgsocket	socket;
	long		timeout;
} PgWaitSpec;

/*
 * Phase 13 wait-completion record.
 *
 * Logical events still flow through backend interrupts.  Wait readiness is
 * recorded here and wakes the owning backend's latch in the thread-per-session
 * fallback.  Phase 14/15 protocol scheduling must not treat this record as a
 * carrier-release continuation for deep waits.
 */
typedef enum PgWaitCompletionState
{
	PG_WAIT_COMPLETION_INACTIVE = 0,
	PG_WAIT_COMPLETION_WAITING,
	PG_WAIT_COMPLETION_READY,
	PG_WAIT_COMPLETION_CANCELLED
} PgWaitCompletionState;

typedef enum PgWaitCompletionInterrupt
{
	PG_WAIT_COMPLETION_INTERRUPT_CANCEL = (1 << 0),
	PG_WAIT_COMPLETION_INTERRUPT_TERMINATE = (1 << 1)
} PgWaitCompletionInterrupt;

struct PgWaitCompletion
{
	PgWaitSpec	spec;
	PgBackend  *backend;
	PgSession  *session;
	PgExecution *execution;
	pg_atomic_uint32 state;
	pg_atomic_uint32 ready_events;
	pg_atomic_uint32 interrupt_events;
};

typedef struct PgBackendWaitState
{
	PgWaitSpec	spec;
	PgWaitCompletion completion;
	uint32		local_wait_event_info;
	uint32	   *wait_event_info_ptr;
	pg_atomic_uint32 waiting;
} PgBackendWaitState;

typedef struct PgBackendCoreState
{
	bool		exit_on_any_error;
	int			proc_pid;
	pg_time_t	start_time;
	TimestampTz start_timestamp;
	struct Latch *latch;
	int			pm_child_slot;
	char		output_file_name[MAXPGPATH];
	ProcessingMode mode;
	bool		ignore_system_indexes;
	pg_prng_state global_prng_state;
} PgBackendCoreState;

typedef struct PgBackendCommandState
{
	const char *user_d_option;
	struct rusage save_rusage;
	struct timeval save_timeval;
} PgBackendCommandState;

#define PG_BACKEND_FORMATTED_TS_LEN 128

typedef struct PgBackendLogState
{
	char		formatted_start_time[PG_BACKEND_FORMATTED_TS_LEN];
	long		line_number;
	int			line_pid;
} PgBackendLogState;

typedef struct PgBackendExprEvalOpLookup
{
	const void *opcode;
	int			op;
} PgBackendExprEvalOpLookup;

typedef struct PgBackendExprInterpState
{
	const void **dispatch_table;
	PgBackendExprEvalOpLookup *reverse_dispatch_table;
} PgBackendExprInterpState;

typedef struct PgBackendTimeoutState
{
	PgTimeoutParams all_timeouts[MAX_TIMEOUTS];
	bool		all_timeouts_initialized;
	volatile int num_active_timeouts;
	PgTimeoutParams *volatile active_timeouts[MAX_TIMEOUTS];
	volatile sig_atomic_t alarm_enabled;
	volatile sig_atomic_t signal_pending;
	volatile TimestampTz signal_due_at;
	PgBackend  *firing_timeout_target;
	PgExecution *firing_timeout_execution;
	bool		signal_delivery;
	uint64		generation;
} PgBackendTimeoutState;

typedef struct PgBackendWalSenderState
{
	WalSnd	   *my_wal_snd;
	bool		is_walsender;
	bool		is_cascading_walsender;
	bool		is_db_walsender;
	bool		wake_requested;
	XLogReaderState *xlogreader;
	IncrementalBackupInfo *uploaded_manifest;
	MemoryContext uploaded_manifest_mcxt;
	TimeLineID	send_time_line;
	TimeLineID	send_time_line_next_tli;
	bool		send_time_line_is_historic;
	XLogRecPtr	send_time_line_valid_upto;
	XLogRecPtr	sent_ptr;
	StringInfoData output_message;
	StringInfoData reply_message;
	StringInfoData tmpbuf;
	TimestampTz last_processing;
	TimestampTz last_reply_timestamp;
	bool		waiting_for_ping_response;
	TimestampTz shutdown_request_timestamp;
	bool		shutdown_stream_done_queued;
	bool		streaming_done_sending;
	bool		streaming_done_receiving;
	bool		caught_up;
	volatile sig_atomic_t got_sigusr2;
	volatile sig_atomic_t got_stopping;
	volatile sig_atomic_t replication_active;
	LogicalDecodingContext *logical_decoding_ctx;
	MemoryContext replication_cmd_context;
	LagTracker *lag_tracker;
} PgBackendWalSenderState;

#define PG_BACKEND_WALRCV_NUM_WAKEUPS 4

typedef struct PgBackendWalReceiverLogstreamResult
{
	XLogRecPtr	Write;
	XLogRecPtr	Flush;
} PgBackendWalReceiverLogstreamResult;

typedef struct PgBackendReplicationState
{
	ReplicationSlot *my_replication_slot;
	int			sync_rep_wait_mode;
	WalReceiverConn *walreceiver_conn;
	int			walreceiver_recv_file;
	TimeLineID	walreceiver_recv_file_tli;
	XLogSegNo	walreceiver_recv_seg_no;
	PgBackendWalReceiverLogstreamResult walreceiver_logstream_result;
	TimestampTz walreceiver_wakeup[PG_BACKEND_WALRCV_NUM_WAKEUPS];
	StringInfoData walreceiver_reply_message;
	bool		walreceiver_primary_has_standby_xmin;
} PgBackendReplicationState;

#define PG_BACKEND_SLOTSYNC_INITIAL_SLEEP_MS 200

typedef struct SubXactInfo
{
	TransactionId xid;
	int			fileno;
	pgoff_t		offset;
} SubXactInfo;

typedef struct ApplySubXactData
{
	uint32		nsubxacts;
	uint32		nsubxacts_max;
	TransactionId subxact_last;
	SubXactInfo *subxacts;
} ApplySubXactData;

typedef struct ApplyErrorCallbackArg
{
	int			command;
	struct LogicalRepRelMapEntry *rel;
	int			remote_attnum;
	TransactionId remote_xid;
	XLogRecPtr	finish_lsn;
	char	   *origin_name;
} ApplyErrorCallbackArg;

typedef struct PgBackendLogicalReplicationState
{
	dlist_head	lsn_mapping;
	ApplyErrorCallbackArg apply_error_callback_arg;
	ApplySubXactData subxact_data;
	MemoryContext apply_context;
	ParallelApplyWorkerShared *my_parallel_shared;
	volatile sig_atomic_t parallel_apply_message_pending;
	WalReceiverConn *logrep_worker_walrcv_conn;
	Subscription *my_subscription;
	bool		my_subscription_valid;
	LogicalRepWorker *my_logical_rep_worker;
	List	   *on_commit_wakeup_workers_subids;
	bool		in_remote_transaction;
	XLogRecPtr	remote_final_lsn;
	bool		in_streamed_transaction;
	TransactionId stream_xid;
	uint32		parallel_stream_nchanges;
	bool		initializing_apply_worker;
	XLogRecPtr	skip_xact_finish_lsn;
	BufFile    *stream_fd;
	XLogRecPtr	last_flushpos;
	List	   *table_states_not_ready;
	StringInfo	copybuf;
	List	   *seqinfos;
	bool		xlog_logical_info;
	bool		xlog_logical_info_update_pending;
	bool		slotsync_syncing_slots;
	char	   *slotsync_observed_primary_conninfo;
	char	   *slotsync_observed_primary_slotname;
	bool		slotsync_observed_sync_replication_slots;
	bool		slotsync_observed_hot_standby_feedback;
	volatile sig_atomic_t slotsync_shutdown_pending;
	long		slotsync_sleep_ms;
	dsa_area   *launcher_last_start_times_dsa;
	dshash_table *launcher_last_start_times;
	bool		launcher_on_commit_wakeup;
	HTAB	   *parallel_apply_txn_hash;
	List	   *parallel_apply_worker_pool;
	ParallelApplyWorkerInfo *stream_apply_worker;
	List	   *parallel_apply_subxactlist;
	MemoryContext parallel_apply_message_context;
} PgBackendLogicalReplicationState;

typedef struct PgBackendXLogWriteResult
{
	XLogRecPtr	Write;
	XLogRecPtr	Flush;
} PgBackendXLogWriteResult;

typedef struct PgBackendXLogState
{
	bool		local_recovery_in_progress;
	int			local_xlog_insert_allowed;
	XLogRecPtr	proc_last_rec_ptr;
	XLogRecPtr	xact_last_rec_end;
	XLogRecPtr	xact_last_commit_end;
	XLogRecPtr	redo_rec_ptr;
	bool		do_page_writes;
	PgBackendXLogWriteResult logwrt_result;
	int			open_log_file;
	XLogSegNo	open_log_seg_no;
	TimeLineID	open_log_tli;
	XLogRecPtr	local_min_recovery_point;
	TimeLineID	local_min_recovery_point_tli;
	bool		update_min_recovery_point;
	ChecksumStateType local_data_checksum_state;
	int			my_lock_no;
	bool		holding_all_locks;
	MemoryContext wal_debug_context;
	MemoryContext btree_xlog_op_context;
	MemoryContext gin_xlog_op_context;
	MemoryContext gist_xlog_op_context;
	MemoryContext spgist_xlog_op_context;
} PgBackendXLogState;

#define PG_BACKEND_STANDBY_INITIAL_WAIT_US 1000

typedef struct PgBackendRecoveryState
{
	volatile sig_atomic_t startup_got_sighup;
	volatile sig_atomic_t startup_shutdown_requested;
	volatile sig_atomic_t startup_promote_signaled;
	volatile sig_atomic_t startup_in_restore_command;
	TimestampTz startup_progress_phase_start_time;
	volatile sig_atomic_t startup_progress_timer_expired;
	bool		local_hot_standby_active;
	bool		local_promote_is_triggered;
	HTAB	   *recovery_lock_hash;
	HTAB	   *recovery_lock_xid_hash;
	volatile sig_atomic_t got_standby_deadlock_timeout;
	volatile sig_atomic_t got_standby_delay_timeout;
	volatile sig_atomic_t got_standby_lock_timeout;
	int			standby_wait_us;
} PgBackendRecoveryState;

typedef struct PgBackendMaintenanceWorkerState
{
	char	   *arch_module_errdetail_string;
	time_t		pgarch_last_sigterm_time;
	const struct ArchiveModuleCallbacks *archive_callbacks;
	struct ArchiveModuleState *archive_module_state;
	MemoryContext archive_context;
	char	   *loaded_archive_library;
	struct arch_files_state *pgarch_files;
	MemoryContext bgwriter_context;
	MemoryContext walwriter_context;
	MemoryContext checkpointer_context;
	MemoryContext walsummarizer_context;
	volatile sig_atomic_t pgarch_ready_to_stop;
	bool		ckpt_active;
	pg_time_t	ckpt_start_time;
	XLogRecPtr	ckpt_start_recptr;
	double		ckpt_cached_elapsed;
	pg_time_t	last_checkpoint_time;
	pg_time_t	last_xlog_switch_time;
	TimestampTz bgwriter_last_snapshot_ts;
	XLogRecPtr	bgwriter_last_snapshot_lsn;
	long		walsummarizer_sleep_quanta;
	long		walsummarizer_pages_read_since_last_sleep;
	XLogRecPtr	walsummarizer_redo_pointer_at_last_summary_removal;
	volatile sig_atomic_t datachecksum_abort_requested;
	volatile sig_atomic_t datachecksum_launcher_running;
	int			datachecksum_operation;
	uint64		datachecksum_worker_invocation;
} PgBackendMaintenanceWorkerState;

typedef struct PgBackendAutovacuumState
{
	double		av_storage_param_cost_delay;
	int			av_storage_param_cost_limit;
	volatile sig_atomic_t got_sigusr2;
	TransactionId recent_xid;
	MultiXactId recent_multi;
	int			default_freeze_min_age;
	int			default_freeze_table_age;
	int			default_multixact_freeze_min_age;
	int			default_multixact_freeze_table_age;
	MemoryContext autovac_mem_cxt;
	dlist_head	database_list;
	MemoryContext database_list_cxt;
	struct avl_dbase *avl_dbase_array;
	struct WorkerInfoData *my_worker_info;
} PgBackendAutovacuumState;

typedef struct PgBackendRepackState
{
	struct DecodingWorker *decoding_worker;
	volatile sig_atomic_t message_pending;
	bool		am_repack_worker;
	XLogSegNo	current_segment;
	dsm_segment *worker_dsm_segment;
	RelFileLocator repacked_rel_locator;
	RelFileLocator repacked_rel_toast_locator;
	MemoryContext message_context;
} PgBackendRepackState;

typedef struct PgBackendAioState
{
	struct PgAioBackend *my_backend;
	int			my_io_worker_id;
	struct PgAioUringContext *my_uring_context;
} PgBackendAioState;

typedef struct PgBackendExtensionModuleState
{
	List	   *private_states;
} PgBackendExtensionModuleState;

typedef struct PgBackendPgStatPendingColdState
{
	PgStat_BgWriterStats pending_bgwriter;
	PgStat_CheckpointerStats pending_checkpointer;
	PgStat_SLRUStats slru_stats[PGSTAT_SLRU_NUM_ELEMENTS];
	PgStat_PendingLock lock_stats;
} PgBackendPgStatPendingColdState;

typedef struct PgBackendPgStatPendingState
{
	PgStat_LocalState *local;
	MemoryContext fixed_snapshot_context;
	void	   *entry_ref_hash;
	int			shared_ref_age;
	MemoryContext shared_ref_context;
	MemoryContext entry_ref_hash_context;
	PgStat_PendingIO io_stats;
	bool		io_stats_pending;
	bool		slru_stats_pending;
	bool		lock_stats_pending;
	PgStat_BackendPending backend_stats;
	bool		backend_io_stats_pending;
	bool		backend_lock_stats_pending;
	PgBackendPgStatPendingColdState *cold;
	MemoryContext pending_context;
	dlist_head	pending;
	bool		report_fixed;
	bool		force_next_flush;
	bool		force_snapshot_clear;
	bool		is_initialized;
	bool		is_shutdown;
	int			xact_commit;
	int			xact_rollback;
	PgStat_Counter block_read_time;
	PgStat_Counter block_write_time;
	PgStat_Counter active_time;
	PgStat_Counter transaction_idle_time;
	instr_time	func_total_time;
	WalUsage	wal_prev_usage;
	WalUsage	backend_wal_prev_usage;
} PgBackendPgStatPendingState;

typedef struct PgBackendActivityState
{
	LocalPgBackendStatus *backend_status_table;
	int			num_backends;
	MemoryContext backend_status_context;
} PgBackendActivityState;

typedef struct PgBackendAllocSetFreeList
{
	int			num_free;
	struct AllocSetContext *first_free;
} PgBackendAllocSetFreeList;

#define PG_BACKEND_ALLOCSET_NUM_FREELISTS 2

typedef struct PgBackendMemoryManagerState
{
	PgBackendAllocSetFreeList context_freelists[PG_BACKEND_ALLOCSET_NUM_FREELISTS];
	bool		log_memory_context_in_progress;
} PgBackendMemoryManagerState;

#define PG_BACKEND_MAX_SEQ_SCANS 100
#define PG_BACKEND_MAX_DATE_FIELDS 25
#define PG_BACKEND_FORMAT_CACHE_ENTRIES 20

typedef struct PgBackendUtilityState
{
	volatile sig_atomic_t notify_interrupt_pending;
	bool		async_unlisten_exit_registered;
	dshash_table *async_global_channel_table;
	struct dsa_area *async_global_channel_dsa;
	struct ExtensionSiblingCache *extension_sibling_list;
	HTAB	   *injection_point_cache;
	MemoryContext utility_cache_context;
	ReservoirStateData sampling_old_reservoir;
	bool		sampling_old_reservoir_initialized;
	HTAB	   *seq_scan_tables[PG_BACKEND_MAX_SEQ_SCANS];
	int			seq_scan_levels[PG_BACKEND_MAX_SEQ_SCANS];
	int			num_seq_scans;
	Oid			superuser_last_roleid;
	bool		superuser_last_roleid_is_super;
	bool		superuser_roleid_callback_registered;
	void	   *resource_release_callbacks;
#ifdef RESOWNER_STATS
	int			resource_owner_array_lookups;
	int			resource_owner_hash_lookups;
#endif
	const void *date_cache[PG_BACKEND_MAX_DATE_FIELDS];
	const void *delta_cache[PG_BACKEND_MAX_DATE_FIELDS];
	bool		degree_consts_set;
	float8		degree_sin_30;
	float8		degree_one_minus_cos_60;
	float8		degree_asin_0_5;
	float8		degree_acos_0_5;
	float8		degree_atan_1_0;
	float8		degree_tan_45;
	float8		degree_cot_45;
	void	   *dch_cache[PG_BACKEND_FORMAT_CACHE_ENTRIES];
	int			n_dch_cache;
	int			dch_counter;
	void	   *num_cache[PG_BACKEND_FORMAT_CACHE_ENTRIES];
	int			n_num_cache;
	int			num_counter;
	MemoryContext format_cache_context;
	MemoryContext libxml_context;
	HTAB	   *missing_attr_cache;
} PgBackendUtilityState;

typedef struct PgBackendParallelState
{
	int			worker_number;
	volatile sig_atomic_t message_pending;
	bool		initializing_worker;
	void	   *fixed_parallel_state;
	dlist_head	context_list;
	bool		context_list_initialized;
	pid_t		leader_pid;
	void	   *pq_mq_handle;
	bool		pq_mq_busy;
	pid_t		pq_mq_parallel_leader_pid;
	ProcNumber	pq_mq_parallel_leader_proc_number;
	MemoryContext message_context;
} PgBackendParallelState;

typedef struct PgBackendInstrumentationState
{
	BufferUsage buffer_usage;
	BufferUsage saved_buffer_usage;
	WalUsage	wal_usage;
	WalUsage	saved_wal_usage;
} PgBackendInstrumentationState;

typedef struct PgBackendBufferState
{
	BufferDesc *pin_count_wait_buf;
	int			nlocbuffer;
	void	   *local_buffer_descriptors;
	void	   *local_buffer_block_pointers;
	int32	   *local_ref_count;
	int			next_free_local_buf_id;
	HTAB	   *local_buf_hash;
	int			n_local_pinned_buffers;
	char	   *local_buffer_cur_block;
	int			local_buffer_next_buf_in_block;
	int			local_buffer_num_bufs_in_block;
	int			local_buffer_total_bufs_allocated;
	MemoryContext local_buffer_context;
	MemoryContext buffer_context;
	WritebackContext *backend_writeback_context;
	void	   *private_ref_count_array_keys;
	void	   *private_ref_count_array;
	void	   *private_ref_count_hash;
	int32		private_ref_count_overflowed;
	uint32		private_ref_count_clock;
	int			reserved_ref_count_slot;
	int			private_ref_count_entry_last;
	uint32		max_proportional_pins;
	bool		private_ref_count_released_while_idle;
} PgBackendBufferState;

typedef struct PgBackendStorageState
{
	void	   *vfd_cache;
	Size		size_vfd_cache;
	int			nfile;
	bool		temporary_files_allowed;
	int			num_allocated_descs;
	int			max_allocated_descs;
	void	   *allocated_descs;
	int			num_external_fds;
	HTAB	   *sync_pending_ops;
	List	   *sync_pending_unlinks;
	MemoryContext sync_pending_ops_context;
	uint16		sync_cycle_counter;
	uint16		sync_checkpoint_cycle_counter;
	bool		sync_in_progress;
	HTAB	   *smgr_relation_hash;
	dlist_head	smgr_unpinned_relations;
	MemoryContext md_context;
} PgBackendStorageState;

#define PG_BACKEND_MAX_INLINE_LWLOCKS 16
#define PG_BACKEND_MAX_SIMUL_LWLOCKS 200

typedef struct PgBackendLWLockHandle
{
	LWLock	   *lock;
	LWLockMode	mode;
} PgBackendLWLockHandle;

typedef struct PgBackendLWLockStatsKey
{
	int			tranche;
	void	   *instance;
} PgBackendLWLockStatsKey;

typedef struct PgBackendLWLockStats
{
	PgBackendLWLockStatsKey key;
	int			sh_acquire_count;
	int			ex_acquire_count;
	int			block_count;
	int			dequeue_self_count;
	int			spin_delay_count;
} PgBackendLWLockStats;

typedef struct PgBackendLockState
{
	PgBackendLWLockHandle *held_lwlocks_array;
	PgBackendLWLockHandle held_lwlocks_inline[PG_BACKEND_MAX_INLINE_LWLOCKS];
	int			held_lwlocks_capacity;
	int			num_held_lwlocks;
	int			local_num_user_defined_lwlock_tranches;
	HTAB	   *lwlock_stats_htab;
	PgBackendLWLockStats lwlock_stats_dummy;
	MemoryContext lwlock_stats_context;
	bool		lwlock_stats_exit_registered;
	void	   *fast_path_local_use_counts;
	bool		fast_path_local_use_counts_owned;
	bool		relation_extension_lock_held;
	HTAB	   *lock_method_local_hash;
	void	   *strong_lock_in_progress;
	void	   *awaited_lock;
	void	   *awaited_owner;
	volatile sig_atomic_t deadlock_timeout_pending;
	void	   *condition_variable_sleep_target;
	uint32		speculative_insertion_token;
	void	   *deadlock_visited_procs;
	int			deadlock_n_visited_procs;
	void	   *deadlock_topo_procs;
	void	   *deadlock_before_constraints;
	void	   *deadlock_after_constraints;
	void	   *deadlock_wait_orders;
	int			deadlock_n_wait_orders;
	void	   *deadlock_wait_order_procs;
	void	   *deadlock_cur_constraints;
	int			deadlock_n_cur_constraints;
	int			deadlock_max_cur_constraints;
	void	   *deadlock_possible_constraints;
	int			deadlock_n_possible_constraints;
	int			deadlock_max_possible_constraints;
	void	   *deadlock_details;
	int			deadlock_n_details;
	void	   *blocking_autovacuum_proc;
	bool		deadlock_workspace_owned;
	HTAB	   *local_predicate_lock_hash;
	void	   *my_serializable_xact;
	bool		my_xact_did_write;
	void	   *saved_serializable_xact;
} PgBackendLockState;

typedef struct PgBackendIPCState
{
	void	   *proc_signal_slot;
	uint64		shared_invalid_message_counter;
	volatile sig_atomic_t catchup_interrupt_pending;
	void	   *shared_invalidation_messages;
	volatile int shared_invalidation_next_msg;
	volatile int shared_invalidation_num_msgs;
	bool		dsm_init_done;
	void	   *dsm_registry_dsa;
	void	   *dsm_registry_table;
	LocalTransactionId next_local_transaction_id;
	WaitEventSet *latch_wait_set;
	Latch		local_latch_data;
} PgBackendIPCState;

typedef struct PgBackendTransactionState
{
	TransactionId cached_fetch_xid;
	int			cached_fetch_xid_status;
	XLogRecPtr	cached_commit_lsn;
	void	   *two_phase_locked_gxact;
	bool		two_phase_exit_registered;
	FullTransactionId two_phase_cached_fxid;
	void	   *two_phase_cached_gxact;
	int			slru_error_cause;
	int			slru_errno_value;
	dclist_head multixact_cache;
	bool		multixact_cache_initialized;
	MemoryContext multixact_context;
	char	   *multixact_debug_string;
	TransactionId procarray_cached_xid_not_in_progress;
	struct GlobalVisState global_vis_shared_rels;
	struct GlobalVisState global_vis_catalog_rels;
	struct GlobalVisState global_vis_data_rels;
	struct GlobalVisState global_vis_temp_rels;
	TransactionId compute_xid_horizons_result_last_xmin;
	long		xidcache_by_recent_xmin;
	long		xidcache_by_known_xact;
	long		xidcache_by_my_xact;
	long		xidcache_by_latest_xid;
	long		xidcache_by_main_xid;
	long		xidcache_by_child_xid;
	long		xidcache_by_known_assigned;
	long		xidcache_no_overflow;
	long		xidcache_slow_answer;
} PgBackendTransactionState;

#define PG_EXECUTION_ERRORDATA_STACK_SIZE 5
#define PG_EXECUTION_RELCACHE_MAX_EOXACT_LIST 32
#define PG_EXECUTION_RELMAPPER_MAX_MAPPINGS 64

typedef struct PgExecutionDebugState
{
	const char *debug_query_string;
} PgExecutionDebugState;

typedef struct PgExecutionErrorState
{
	struct ErrorContextCallback *context_stack;
	sigjmp_buf *exception_stack;
	ErrorData	errordata[PG_EXECUTION_ERRORDATA_STACK_SIZE];
	int			errordata_stack_depth;
	int			recursion_depth;
	struct timeval saved_timeval;
	bool		saved_timeval_set;
	char		formatted_log_time[PG_BACKEND_FORMATTED_TS_LEN];
} PgExecutionErrorState;

typedef struct PgExecutionMemoryContextState
{
	MemoryContext top_context;
	MemoryContext current_context;
	MemoryContext error_context;
	MemoryContext message_context;
	MemoryContext top_transaction_context;
	MemoryContext cur_transaction_context;
	MemoryContext portal_context;
} PgExecutionMemoryContextState;

typedef struct PgExecutionResourceOwnerState
{
	struct ResourceOwnerData *current_owner;
	struct ResourceOwnerData *cur_transaction_owner;
	struct ResourceOwnerData *top_transaction_owner;
	MemoryContext resource_owner_context;
} PgExecutionResourceOwnerState;

typedef struct PgExecutionSPIState
{
	uint64		processed;
	SPITupleTable *tuptable;
	int			result;
	_SPI_connection *stack;
	_SPI_connection *current;
	int			stack_depth;
	int			connected;
} PgExecutionSPIState;

typedef struct PgExecutionPortalState
{
	Portal		active;
} PgExecutionPortalState;

typedef struct PgExecutionVacuumState
{
	bool		in_vacuum;
	int			cost_balance;
	bool		cost_active;
	pg_atomic_uint32 *shared_cost_balance;
	pg_atomic_uint32 *active_nworkers;
	int			cost_balance_local;
	bool		failsafe_active;
	int64		parallel_worker_delay_ns;
	void	   *parallel_shared_cost_params;
	uint32		parallel_shared_params_generation_local;
} PgExecutionVacuumState;

typedef struct PgExecutionNodeIOState
{
	bool		write_location_fields;
	const char *strtok_ptr;
	bool		restore_location_fields;
} PgExecutionNodeIOState;

typedef struct PgExecutionBaseBackupState
{
	bool		backup_started_in_recovery;
	long long int total_checksum_failures;
	bool		noverify_checksums;
} PgExecutionBaseBackupState;

typedef struct PgExecutionAnalyzeState
{
	MemoryContext context;
	BufferAccessStrategy strategy;
	void	   *array_extra_data;
} PgExecutionAnalyzeState;

typedef void (*PgExecutionDebugHandler) (const char *message);

typedef struct PgExecutionExtensionState
{
	bool		creating;
	Oid			current_object;
	List	   *private_states;
} PgExecutionExtensionState;

typedef struct PgExecutionMatViewState
{
	int			maintenance_depth;
} PgExecutionMatViewState;

typedef struct PgExecutionSnapshotState
{
	SnapshotData current_snapshot_data;
	SnapshotData secondary_snapshot_data;
	SnapshotData catalog_snapshot_data;
	Snapshot	current_snapshot;
	Snapshot	secondary_snapshot;
	Snapshot	catalog_snapshot;
	Snapshot	historic_snapshot;
	TransactionId transaction_xmin;
	TransactionId recent_xmin;
	HTAB	   *tuplecid_data;
	void	   *active_snapshot;
	pairingheap registered_snapshots;
	bool		first_snapshot_set;
	Snapshot	first_xact_snapshot;
	List	   *exported_snapshots;
} PgExecutionSnapshotState;

typedef struct PgExecutionComboCidState
{
	HTAB	   *hash;
	void	   *cids;
	int			used;
	int			size;
} PgExecutionComboCidState;

typedef struct PgExecutionXLogInsertState
{
	void	   *registered_buffers;
	int			max_registered_buffers;
	int			max_registered_block_id;
	XLogRecData *mainrdata_head;
	XLogRecData *mainrdata_last;
	uint64		mainrdata_len;
	uint8		curinsert_flags;
	XLogRecData hdr_rdt;
	char	   *hdr_scratch;
	XLogRecData *rdatas;
	int			num_rdatas;
	int			max_rdatas;
	bool		begininsert_called;
	MemoryContext context;
	/*
	 * One-entry cache of the WAL buffer page last accessed by GetXLogBuffer().
	 * Upstream keeps these as function-local statics, which are per-process
	 * (hence per-backend) in process mode.  Under the threaded runtime a plain
	 * static local is shared across every backend fiber, so the cache must live
	 * in per-backend execution state instead.
	 */
	uint64		get_xlog_buffer_cached_page;
	char	   *get_xlog_buffer_cached_pos;
} PgExecutionXLogInsertState;

#define PG_EXECUTION_UNREPORTED_XIDS_CAPACITY 64

typedef struct PgExecutionXactState
{
	int			iso_level;
	bool		read_only;
	bool		deferrable;
	bool		is_sampled;
	TransactionId check_xid_alive;
	bool		bsysscan_value;
	int			flags;
	FullTransactionId top_full_transaction_id;
	int			n_parallel_current_xids;
	TransactionId *parallel_current_xids;
	int			n_unreported_xids;
	TransactionId unreported_xids[PG_EXECUTION_UNREPORTED_XIDS_CAPACITY];
	SubTransactionId current_sub_transaction_id;
	CommandId	current_command_id;
	bool		current_command_id_used;
	TimestampTz xact_start_timestamp;
	TimestampTz stmt_start_timestamp;
	TimestampTz xact_stop_timestamp;
	char	   *prepare_gid;
	bool		force_sync_commit;
	MemoryContext transaction_abort_context;
	TransactionStateData *top_transaction_state_data;
	TransactionStateData *current_transaction_state;
} PgExecutionXactState;

typedef struct PgExecutionTransactionCleanupState
{
	LargeObjectDesc **lo_cookies;
	int			lo_cookies_size;
	bool		lo_cleanup_needed;
	MemoryContext lo_context;
	bool		have_xact_temporary_files;
	PgStat_SubXactStatus *pgstat_xact_stack;
	HTAB	   *ri_fastpath_cache;
	bool		ri_fastpath_callback_registered;
	bool		ri_fastpath_flushing;
} PgExecutionTransactionCleanupState;

typedef struct PgExecutionReplicationScratchState
{
	EventTriggerQueryState *event_trigger_query_state;
	MemoryContext event_trigger_context;
	ReplOriginXactState replorigin_xact;
	ErrorContextCallback *apply_error_context_stack;
	MemoryContext apply_message_context;
	MemoryContext logical_streaming_context;
} PgExecutionReplicationScratchState;

typedef struct PgExecutionGUCErrorState
{
	int			check_errcode_value;
	char	   *check_errmsg_string;
	char	   *check_errdetail_string;
	char	   *check_errhint_string;
	int			format_errnumber;
	const char *format_domain;
	unsigned int config_file_lineno;
	const char *flex_fatal_errmsg;
	sigjmp_buf *flex_fatal_jmp;
} PgExecutionGUCErrorState;

typedef struct PgExecutionAsyncQueuePosition
{
	int64		page;
	int			offset;
} PgExecutionAsyncQueuePosition;

struct ActionList;
struct NotificationList;

typedef struct PgExecutionAsyncState
{
	struct ActionList *pending_actions;
	HTAB	   *pending_listen_actions;
	struct NotificationList *pending_notifies;
	PgExecutionAsyncQueuePosition queue_head_before_write;
	PgExecutionAsyncQueuePosition queue_head_after_write;
	MemoryContext signal_context;
	int32	   *signal_pids;
	ProcNumber *signal_procnos;
	bool		try_advance_tail;
} PgExecutionAsyncState;

typedef struct PgExecutionCatalogState
{
	HTAB	   *uncommitted_enum_types;
	HTAB	   *uncommitted_enum_values;
	Oid			currently_reindexed_heap;
	Oid			currently_reindexed_index;
	List	   *pending_reindexed_indexes;
	int			reindexing_nest_level;
	struct PendingRelDelete *pending_rel_deletes;
	HTAB	   *pending_sync_hash;
} PgExecutionCatalogState;

typedef struct PgExecutionCatalogCacheState
{
	CatCInProgress *catcache_in_progress_stack;
	InProgressEnt *relcache_in_progress_list;
	int			relcache_in_progress_list_len;
	int			relcache_in_progress_list_maxlen;
	Oid			relcache_eoxact_list[PG_EXECUTION_RELCACHE_MAX_EOXACT_LIST];
	int			relcache_eoxact_list_len;
	bool		relcache_eoxact_list_overflowed;
	TupleDesc  *relcache_eoxact_tupledesc_array;
	int			relcache_next_eoxact_tupledesc_num;
	int			relcache_eoxact_tupledesc_array_len;
} PgExecutionCatalogCacheState;

typedef struct PgExecutionRelMapping
{
	Oid			mapoid;
	RelFileNumber mapfilenumber;
} PgExecutionRelMapping;

typedef struct PgExecutionRelMapFile
{
	int32		magic;
	int32		num_mappings;
	PgExecutionRelMapping mappings[PG_EXECUTION_RELMAPPER_MAX_MAPPINGS];
	pg_crc32c	crc;
} PgExecutionRelMapFile;

typedef struct PgExecutionRelMapState
{
	PgExecutionRelMapFile active_shared_updates;
	PgExecutionRelMapFile active_local_updates;
	PgExecutionRelMapFile pending_shared_updates;
	PgExecutionRelMapFile pending_local_updates;
} PgExecutionRelMapState;

typedef struct PgExecutionInvalMessageArray
{
	void	   *msgs;
	int			maxmsgs;
} PgExecutionInvalMessageArray;

typedef struct PgExecutionInvalidationState
{
	PgExecutionInvalMessageArray message_arrays[2];
	struct TransInvalidationInfo *trans_info;
	struct InvalidationInfo *inplace_info;
} PgExecutionInvalidationState;

typedef struct PgExecutionTwoPhaseRecordState
{
	struct StateFileChunk *head;
	struct StateFileChunk *tail;
	uint32		num_chunks;
	uint32		bytes_free;
	uint32		total_len;
} PgExecutionTwoPhaseRecordState;

typedef struct PgExecutionTriggerState
{
	int			depth;
	void	   *after_triggers_data;
	MemoryContext after_triggers_context;
} PgExecutionTriggerState;

typedef struct PgExecutionRegexState
{
	void	   *regex_locale;
} PgExecutionRegexState;

typedef struct PgExecutionValgrindState
{
	unsigned int old_error_count;
} PgExecutionValgrindState;

typedef struct PgExecutionSnapBuildState
{
	struct ResourceOwnerData *saved_resource_owner_during_export;
	bool		export_in_progress;
} PgExecutionSnapBuildState;

typedef struct PgSessionDatabaseState
{
	Oid			database_id;
	Oid			database_tablespace;
	bool		database_has_login_event_triggers;

	/*
	 * Path relative to DataDir of this database's primary directory, ie its
	 * directory in the default tablespace.
	 */
	char	   *database_path;
	MemoryContext database_path_context;
	bool		database_path_owned;
} PgSessionDatabaseState;

typedef struct PgSessionTablespaceState
{
	bool		initialized;
	char	   *default_tablespace_name;
	char	   *temp_tablespaces_names;
	bool		allow_in_place_tablespaces_value;
	Oid			binary_upgrade_next_pg_tablespace_oid_value;
} PgSessionTablespaceState;

typedef struct PgSessionBinaryUpgradeState
{
	bool		initialized;
	Oid			binary_upgrade_next_pg_type_oid_value;
	Oid			binary_upgrade_next_array_pg_type_oid_value;
	Oid			binary_upgrade_next_mrng_pg_type_oid_value;
	Oid			binary_upgrade_next_mrng_array_pg_type_oid_value;
	Oid			binary_upgrade_next_heap_pg_class_oid_value;
	RelFileNumber binary_upgrade_next_heap_pg_class_relfilenumber_value;
	Oid			binary_upgrade_next_index_pg_class_oid_value;
	RelFileNumber binary_upgrade_next_index_pg_class_relfilenumber_value;
	Oid			binary_upgrade_next_toast_pg_class_oid_value;
	RelFileNumber binary_upgrade_next_toast_pg_class_relfilenumber_value;
	Oid			binary_upgrade_next_pg_enum_oid_value;
	Oid			binary_upgrade_next_pg_authid_oid_value;
	bool		binary_upgrade_record_init_privs_value;
} PgSessionBinaryUpgradeState;

typedef struct PgSessionTzAbbrevCache
{
	char		abbrev[TOKMAXLEN + 1];
	char		ftype;
	int			offset;
	pg_tz	   *tz;
} PgSessionTzAbbrevCache;

typedef struct PgSessionDateTimeState
{
	bool		initialized;
	int			date_style;
	int			date_order;
	int			interval_style;
	char	   *datestyle_string_value;
	char	   *timezone_string_value;
	char	   *log_timezone_string_value;
	char	   *timezone_abbreviations_string_value;
	pg_tz	   *session_timezone_value;
	pg_tz	   *log_timezone_value;
	TimeZoneAbbrevTable *timezone_abbrev_table;
	PgSessionTzAbbrevCache timezone_abbrev_cache[PG_BACKEND_MAX_DATE_FIELDS];
	TimestampTz current_time_cache_ts;
	pg_tz	   *current_time_cache_timezone;
	struct pg_tm current_time_cache_tm;
	fsec_t		current_time_cache_fsec;
	int			current_time_cache_tz;
} PgSessionDateTimeState;

typedef struct PgSessionTextSearchState
{
	bool		initialized;
	char	   *current_config_value;
	Oid			current_config_cache;
	HTAB	   *parser_cache_hash;
	TSParserCacheEntry *last_used_parser;
	HTAB	   *dictionary_cache_hash;
	TSDictionaryCacheEntry *last_used_dictionary;
	HTAB	   *config_cache_hash;
	TSConfigCacheEntry *last_used_config;
} PgSessionTextSearchState;

typedef struct PgSessionConnectionGUCState
{
	bool		initialized;
	char	   *application_name_value;
	int			ssl_renegotiation_limit_value;
	int			tcp_keepalives_idle_value;
	int			tcp_keepalives_interval_value;
	int			tcp_keepalives_count_value;
	int			tcp_user_timeout_value;
	bool		log_disconnections_value;
	int			log_statement_value;
	int			post_auth_delay_seconds;
	char	   *restrict_nonsystem_relation_kind_string_value;
	int			restrict_nonsystem_relation_kind_value;
} PgSessionConnectionGUCState;

typedef struct PgSessionParserState
{
	bool		initialized;
	bool		transform_null_equals_value;
	int			backslash_quote_value;
	HTAB	   *operator_lookup_cache;
} PgSessionParserState;

typedef struct PgSessionVacuumState
{
	bool		initialized;
	int			vacuum_buffer_usage_limit_kb;
	int			vacuum_cost_page_hit_value;
	int			vacuum_cost_page_miss_value;
	int			vacuum_cost_page_dirty_value;
	int			vacuum_cost_limit_value;
	double		vacuum_cost_delay_ms;
	int			default_statistics_target_value;
	int			vacuum_freeze_min_age_value;
	int			vacuum_freeze_table_age_value;
	int			vacuum_multixact_freeze_min_age_value;
	int			vacuum_multixact_freeze_table_age_value;
	int			vacuum_failsafe_age_value;
	int			vacuum_multixact_failsafe_age_value;
	bool		track_cost_delay_timing_value;
	bool		vacuum_truncate_value;
	double		vacuum_max_eager_freeze_failure_rate_value;
	double		local_vacuum_cost_delay_ms;
	int			local_vacuum_cost_limit_value;
} PgSessionVacuumState;

typedef struct PgSessionBufferIOState
{
	bool		initialized;
	bool		zero_damaged_pages_value;
	bool		track_io_timing_value;
	int			effective_io_concurrency_value;
	int			maintenance_io_concurrency_value;
	int			io_combine_limit_value;
	int			io_combine_limit_guc_value;
	int			backend_flush_after_value;
} PgSessionBufferIOState;

typedef struct PgSessionXactDefaultState
{
	bool		initialized;
	int			default_xact_iso_level;
	bool		default_xact_read_only;
	bool		default_xact_deferrable;
	int			synchronous_commit_value;
} PgSessionXactDefaultState;

typedef struct PgSessionLockWaitState
{
	bool		initialized;
	int			deadlock_timeout_ms;
	int			statement_timeout_ms;
	int			lock_timeout_ms;
	int			idle_in_transaction_session_timeout_ms;
	int			transaction_timeout_ms;
	int			idle_session_timeout_ms;
	bool		log_lock_waits_value;
	bool		log_lock_failures_value;
	int			trace_lock_oidmin_value;
	bool		trace_locks_value;
	bool		trace_userlocks_value;
	int			trace_lock_table_value;
	bool		debug_deadlocks_value;
	bool		trace_lwlocks_value;
} PgSessionLockWaitState;

typedef struct PgSessionLoggingState
{
	bool		initialized;
	bool		debug_print_plan_value;
	bool		debug_print_parse_value;
	bool		debug_print_raw_parse_value;
	bool		debug_print_rewritten_value;
	bool		debug_pretty_print_value;
#ifdef DEBUG_NODE_TESTS_ENABLED
	bool		debug_copy_parse_plan_trees_value;
	bool		debug_write_read_parse_plan_trees_value;
	bool		debug_raw_expression_coverage_test_value;
#endif
	bool		log_parser_stats_value;
	bool		log_planner_stats_value;
	bool		log_executor_stats_value;
	bool		log_statement_stats_value;
	bool		log_btree_build_stats_value;
	char	   *event_source_value;
	bool		log_duration_value;
	int			log_error_verbosity_value;
	int			log_parameter_max_length_value;
	int			log_parameter_max_length_on_error_value;
	int			log_min_error_statement_value;
	int			log_min_messages_values[BACKEND_NUM_TYPES];
	char	   *log_min_messages_string_value;
	int			client_min_messages_value;
	int			log_min_duration_sample_value;
	int			log_min_duration_statement_value;
	int			log_temp_files_value;
	double		log_statement_sample_rate_value;
	double		log_xact_sample_rate_value;
	char	   *backtrace_functions_value;
	char	   *backtrace_function_list_value;
} PgSessionLoggingState;

typedef struct PgSessionMiscGUCState
{
	bool		initialized;
	bool		allow_system_table_mods_value;
	int			max_stack_depth_kb;
	ssize_t		max_stack_depth_bytes;
	char	   *session_preload_libraries_value;
	char	   *local_preload_libraries_value;
	char	   *dynamic_library_path_value;
	char	   *extension_control_path_value;
	bool		update_process_title_value;
} PgSessionMiscGUCState;

typedef struct PgSessionGUCState
{
	bool		initialized;
	MemoryContext memory_context;
	struct config_generic *variables;
	struct config_generic_state *variable_states;
	int			num_variables;
	HTAB	   *hash_table;
	dlist_head	nondef_list;
	slist_head	stack_list;
	slist_head	report_list;
	bool		reporting_enabled;
	int			nest_level;
} PgSessionGUCState;

typedef struct PgSessionPgStatState
{
	bool		initialized;
	bool		track_counts;
	int			track_functions;
	int			fetch_consistency;
	bool		track_activities;
	SessionEndType session_end_cause;
	PgStat_Counter last_session_report_time;
} PgSessionPgStatState;

typedef struct PgSessionQueryIdState
{
	bool		initialized;
	int			compute_query_id_value;
	bool		query_id_enabled_value;
} PgSessionQueryIdState;

typedef struct PgSessionStorageGUCState
{
	bool		initialized;
	bool		ignore_checksum_failure_value;
	int			file_copy_method_value;
} PgSessionStorageGUCState;

typedef struct PgSessionUserGUCState
{
	bool		initialized;
	int			password_encryption_value;
	char	   *createrole_self_grant_value;
	bool		createrole_self_grant_enabled;
	unsigned	createrole_self_grant_options_specified;
	bool		createrole_self_grant_options_admin;
	bool		createrole_self_grant_options_inherit;
	bool		createrole_self_grant_options_set;
} PgSessionUserGUCState;

typedef struct PgSessionUserIdentityState
{
	bool		initialized;
	Oid			authenticated_user_id;
	Oid			session_user_id;
	Oid			outer_user_id;
	Oid			current_user_id;
	const char *system_user;
	MemoryContext system_user_context;
	bool		system_user_owned;
	bool		session_user_is_superuser;
	int			security_restriction_context;
	bool		set_role_is_active;
	Oid			cached_role[3];
	List	   *cached_roles[3];
	uint32		cached_db_hash;
} PgSessionUserIdentityState;

typedef struct PgSessionCommandGUCState
{
	bool		initialized;
	int			session_replication_role_value;
	bool		event_triggers_value;
	bool		trace_notify_value;
} PgSessionCommandGUCState;

typedef struct PgSessionReplicationGUCState
{
	bool		initialized;
	int			wal_sender_timeout_ms;
	int			wal_sender_shutdown_timeout_ms;
	bool		log_replication_commands_value;
	int			wal_receiver_timeout_ms;
	int			logical_decoding_work_mem_kb;
	int			debug_logical_replication_streaming_value;
} PgSessionReplicationGUCState;

typedef struct PgSessionLogicalReplicationState
{
	struct ReplicationState *session_replication_state;
	MemoryContext logical_rep_relmap_context;
	HTAB	   *logical_rep_relmap;
	MemoryContext logical_rep_partmap_context;
	HTAB	   *logical_rep_partmap;
	bool		pgoutput_publications_valid;
	HTAB	   *pgoutput_relation_sync_cache;
	int			syncing_relations_state;
} PgSessionLogicalReplicationState;

typedef struct PgSessionGeneralGUCState
{
	bool		initialized;
	bool		allow_alter_system_value;
	bool		row_security_value;
	bool		check_function_bodies_value;
	bool		current_role_is_superuser_value;
	bool		default_with_oids_value;
	bool		standard_conforming_strings_value;
	double		phony_random_seed_value;
	int			temp_file_limit_kb;
	int			num_temp_buffers_blocks;
	char	   *role_string_value;
	char	   *session_authorization_string_value;
	bool		lo_compat_privileges_value;
	int			extra_float_digits_value;
	bool		array_nulls_value;
	int			bytea_output_value;
	int			xmlbinary_value;
	int			xmloption_value;
	bool		quote_all_identifiers_value;
	int			plan_cache_mode_value;
	int			gin_fuzzy_search_limit_value;
	int			gin_pending_list_limit_value;
} PgSessionGeneralGUCState;

typedef struct PgSessionAccessWalGUCState
{
	bool		initialized;
	char	   *default_table_access_method_value;
	bool		synchronize_seqscans_value;
	int			default_toast_compression_value;
	int			wal_compression_value;
	bool		wal_init_zero_value;
	bool		wal_recycle_value;
	char	   *wal_consistency_checking_string_value;
	bool	   *wal_consistency_checking_value;
	int			commit_delay_us;
	int			commit_siblings_value;
	bool		track_wal_io_timing_value;
	int			wal_skip_threshold_kb;
#ifdef WAL_DEBUG
	bool		xlog_debug_value;
#endif
#ifdef TRACE_SYNCSCAN
	bool		trace_syncscan_value;
#endif
} PgSessionAccessWalGUCState;

typedef struct PgSessionJitGUCState
{
	bool		initialized;
	bool		jit_enabled_value;
	char	   *jit_provider_value;
	bool		jit_debugging_support_value;
	bool		jit_dump_bitcode_value;
	bool		jit_expressions_value;
	bool		jit_profiling_support_value;
	bool		jit_tuple_deforming_value;
	double		jit_above_cost_value;
	double		jit_inline_above_cost_value;
	double		jit_optimize_above_cost_value;
} PgSessionJitGUCState;

typedef struct PgSessionJitProviderState
{
	JitProviderCallbacks provider;
	bool		provider_successfully_loaded;
	bool		provider_failed_loading;
} PgSessionJitProviderState;

typedef struct PgSessionSortGUCState
{
	bool		initialized;
	bool		trace_sort_value;
#ifdef DEBUG_BOUNDED_SORT
	bool		optimize_bounded_sort_value;
#endif
} PgSessionSortGUCState;

typedef struct PgSessionQueryMemoryState
{
	bool		initialized;
	int			work_mem_kb;
	double		hash_mem_multiplier_value;
	int			maintenance_work_mem_kb;
	int			max_parallel_maintenance_workers_value;
} PgSessionQueryMemoryState;

typedef struct PgSessionPlannerCostState
{
	bool		initialized;
	double		seq_page_cost_value;
	double		random_page_cost_value;
	double		cpu_tuple_cost_value;
	double		cpu_index_tuple_cost_value;
	double		cpu_operator_cost_value;
	double		parallel_tuple_cost_value;
	double		parallel_setup_cost_value;
	double		recursive_worktable_factor_value;
	int			effective_cache_size_pages;
	double		disable_cost_value;
	int			max_parallel_workers_per_gather_value;
	int			debug_parallel_query_value;
	bool		parallel_leader_participation_value;
} PgSessionPlannerCostState;

typedef struct PgSessionPlannerMethodState
{
	bool		initialized;
	bool		enable_seqscan_value;
	bool		enable_indexscan_value;
	bool		enable_indexonlyscan_value;
	bool		enable_bitmapscan_value;
	bool		enable_tidscan_value;
	bool		enable_sort_value;
	bool		enable_incremental_sort_value;
	bool		enable_hashagg_value;
	bool		enable_nestloop_value;
	bool		enable_material_value;
	bool		enable_memoize_value;
	bool		enable_mergejoin_value;
	bool		enable_hashjoin_value;
	bool		enable_gathermerge_value;
	bool		enable_partitionwise_join_value;
	bool		enable_partitionwise_aggregate_value;
	bool		enable_parallel_append_value;
	bool		enable_parallel_hash_value;
	bool		enable_partition_pruning_value;
	bool		enable_presorted_aggregate_value;
	bool		enable_async_append_value;
	bool		enable_distinct_reordering_value;
	bool		enable_geqo_value;
	bool		enable_eager_aggregate_value;
	bool		enable_group_by_reordering_value;
	bool		enable_self_join_elimination_value;
	double		cursor_tuple_fraction_value;
	int			constraint_exclusion_value;
	int			geqo_threshold_value;
	int			Geqo_effort_value;
	int			Geqo_pool_size_value;
	int			Geqo_generations_value;
	double		Geqo_selection_bias_value;
	double		Geqo_seed_value;
	int			Geqo_planner_extension_id_value;
	double		min_eager_agg_group_size_value;
	int			min_parallel_table_scan_size_blocks;
	int			min_parallel_index_scan_size_blocks;
	int			from_collapse_limit_value;
	int			join_collapse_limit_value;
} PgSessionPlannerMethodState;

typedef struct PgSessionFunctionManagerState
{
	MemoryContext function_manager_context;
	HTAB	   *c_func_hash;
	HTAB	   *cached_function_hash;
} PgSessionFunctionManagerState;

typedef void (*PgSessionResetCallback) (void *arg);
typedef void (*PgExtensionPrivateStateCleanup) (void *state);
typedef PgExtensionPrivateStateCleanup PgSessionExtensionPrivateStateCleanup;

typedef struct PgSessionResetCallbackItem
{
	PgSessionResetCallback callback;
	void	   *arg;
} PgSessionResetCallbackItem;

typedef struct PgExtensionPrivateState
{
	const char *key;
	void	   *state;
	PgExtensionPrivateStateCleanup cleanup;
} PgExtensionPrivateState;

typedef PgExtensionPrivateState PgBackendExtensionPrivateState;
typedef PgExtensionPrivateState PgExecutionExtensionPrivateState;
typedef PgExtensionPrivateState PgRuntimeExtensionPrivateState;
typedef PgExtensionPrivateState PgSessionExtensionPrivateState;

typedef struct PgSessionExtensionModuleState
{
	void	   *plpgsql_state;
	void	   *plpython_procedure_cache;
	MemoryContext plpython_memory_context;
	bool		plpython_reset_registered;
	MemoryContext plperl_memory_context;
	bool		plperl_inited;
	void	   *plperl_interp_hash;
	void	   *plperl_proc_hash;
	void	   *plperl_active_interp;
	void	   *plperl_held_interp;
	bool		plperl_use_strict;
	char	   *plperl_on_init;
	char	   *plperl_on_plperl_init;
	char	   *plperl_on_plperlu_init;
	bool		plperl_ending;
	void	   *plperl_current_call_data;
	bool		plperl_reset_registered;
	MemoryContext pltcl_memory_context;
	char	   *pltcl_start_proc;
	char	   *pltclu_start_proc;
	void	   *pltcl_hold_interp;
	void	   *pltcl_interp_hash;
	void	   *pltcl_proc_hash;
	void	   *pltcl_current_call_state;
	bool		pltcl_reset_registered;
	MemoryContext plsample_memory_context;
	char	   *test_backend_runtime_custom_guc;
	int			test_backend_runtime_custom_guc_init_counter;
	List	   *private_states;
	List	   *reset_callbacks;
} PgSessionExtensionModuleState;

typedef struct PgSessionCatalogLookupState
{
	MemoryContext cache_memory_context;
	CatCache   *sys_cache[SysCacheSize];
	bool		sys_cache_initialized;
	Oid			sys_cache_relation_oid[SysCacheSize];
	int			sys_cache_relation_oid_size;
	Oid			sys_cache_supporting_rel_oid[SysCacheSize * 2];
	int			sys_cache_supporting_rel_oid_size;
	CatCacheHeader *cat_cache_header;
	HTAB	   *relcache_relation_id_cache;
	bool		relcache_critical_built;
	bool		relcache_critical_shared_built;
	long		relcache_invals_received;
	TupleDesc	relcache_pg_class_descriptor;
	TupleDesc	relcache_pg_index_descriptor;
	HTAB	   *relcache_opclass_cache;
	HTAB	   *typcache_type_cache_hash;
	HTAB	   *typcache_relid_to_typeid_hash;
	TypeCacheEntry *typcache_first_domain_type_entry;
	Oid		   *typcache_in_progress_list;
	int			typcache_in_progress_list_len;
	int			typcache_in_progress_list_maxlen;
	HTAB	   *typcache_record_cache_hash;
	RecordCacheArrayEntry *typcache_record_cache_array;
	int32		typcache_record_cache_array_len;
	int32		typcache_next_record_typmod;
	uint64		typcache_tupledesc_id_counter;
	HTAB	   *attopt_cache_hash;
	HTAB	   *relfilenumber_map_hash;
	ScanKeyData relfilenumber_skey[2];
	HTAB	   *tablespace_cache_hash;
	HTAB	   *event_trigger_cache;
	MemoryContext event_trigger_cache_context;
	int			event_trigger_cache_state;
	struct _SPI_plan *ruleutils_rule_by_oid_plan;
	struct _SPI_plan *ruleutils_view_rule_plan;
} PgSessionCatalogLookupState;

#define PG_SESSION_MAX_SYSCACHE_CALLBACKS 64
#define PG_SESSION_MAX_RELCACHE_CALLBACKS 10
#define PG_SESSION_MAX_RELSYNC_CALLBACKS 10

typedef struct PgSessionSyscacheCallback
{
	int16		id;
	int16		link;
	SyscacheCallbackFunction function;
	Datum		arg;
} PgSessionSyscacheCallback;

typedef struct PgSessionRelcacheCallback
{
	RelcacheCallbackFunction function;
	Datum		arg;
} PgSessionRelcacheCallback;

typedef struct PgSessionRelSyncCallback
{
	RelSyncCallbackFunction function;
	Datum		arg;
} PgSessionRelSyncCallback;

typedef struct PgSessionInvalidationCallbackState
{
	PgSessionSyscacheCallback syscache_callback_list[PG_SESSION_MAX_SYSCACHE_CALLBACKS];
	int16		syscache_callback_links[SysCacheSize];
	int			syscache_callback_count;
	PgSessionRelcacheCallback relcache_callback_list[PG_SESSION_MAX_RELCACHE_CALLBACKS];
	int			relcache_callback_count;
	PgSessionRelSyncCallback relsync_callback_list[PG_SESSION_MAX_RELSYNC_CALLBACKS];
	int			relsync_callback_count;
} PgSessionInvalidationCallbackState;

typedef struct PgSessionRIGlobalsState
{
	HTAB	   *constraint_cache;
	HTAB	   *query_cache;
	HTAB	   *compare_cache;
	dclist_head constraint_cache_valid_list;
	bool		fastpath_xact_callback_registered;
	bool		debug_discard_caches_initialized;
	int			debug_discard_caches_value;
} PgSessionRIGlobalsState;

typedef struct PgSessionRelMapState
{
	PgExecutionRelMapFile shared_map;
	PgExecutionRelMapFile local_map;
} PgSessionRelMapState;

typedef struct PgSessionPreparedStatementState
{
	HTAB	   *prepared_queries;
} PgSessionPreparedStatementState;

typedef struct PgSessionOnCommitState
{
	List	   *on_commits;
} PgSessionOnCommitState;

typedef struct PgSessionSequenceState
{
	HTAB	   *seqhashtab;
	struct SeqTableData *last_used_seq;
} PgSessionSequenceState;

typedef struct PgSessionXactCallbackState
{
	XactCallbackItem *xact_callbacks;
	SubXactCallbackItem *subxact_callbacks;
	MemoryContext xact_callback_context;
} PgSessionXactCallbackState;

typedef struct PgSessionBackupState
{
	struct BackupState *backup_state;
	StringInfo	tablespace_map;
	MemoryContext backup_context;
	uint8		session_backup_state;
} PgSessionBackupState;

#define PG_SESSION_MAX_CACHED_REGEX 32

typedef struct PgSessionRegexCachedEntry
{
	MemoryContext cre_context;
	char	   *cre_pat;
	int			cre_pat_len;
	int			cre_flags;
	Oid			cre_collation;
	regex_t		cre_re;
} PgSessionRegexCachedEntry;

typedef struct PgSessionRegexState
{
	MemoryContext regexp_cache_context;
	int			num_cached_res;
	PgSessionRegexCachedEntry *cached_res;
	struct pg_ctype_cache *ctype_cache_list;
} PgSessionRegexState;

typedef struct PgSessionPortalManagerState
{
	MemoryContext top_portal_context;
	HTAB	   *portal_hash_table;
	unsigned int unnamed_portal_count;
} PgSessionPortalManagerState;

typedef struct PgSessionLargeObjectState
{
	struct RelationData *heap_relation;
	struct RelationData *index_relation;
} PgSessionLargeObjectState;

typedef struct PgSessionAsyncState
{
	HTAB	   *local_channel_table;
	bool		registered_listener;
} PgSessionAsyncState;

typedef struct PgSessionEncodingState
{
	List	   *conv_proc_list;
	MemoryContext encoding_cache_context;
	FmgrInfo   *to_server_conv_proc;
	FmgrInfo   *to_client_conv_proc;
	FmgrInfo   *utf8_to_server_conv_proc;
	char	   *client_encoding_string_value;
	char	   *server_encoding_string_value;
	const pg_enc2name *client_encoding;
	const pg_enc2name *database_encoding;
	const pg_enc2name *message_encoding;
	bool		backend_startup_complete;
	int			pending_client_encoding;
} PgSessionEncodingState;

typedef struct PgSessionTempFileState
{
	bool		initialized;
	uint64		temporary_files_size;
	long		temp_file_counter;
	Oid		   *temp_table_spaces;
	int			num_temp_table_spaces;
	int			next_temp_table_space;
} PgSessionTempFileState;

typedef struct PgSessionRandomState
{
	bool		initialized;
	pg_prng_state prng_state;
	bool		prng_seed_set;
} PgSessionRandomState;

typedef struct PgSessionOptimizerState
{
	const char **planner_extension_names;
	int			planner_extension_names_assigned;
	int			planner_extension_names_allocated;
	HTAB	   *opr_proof_cache_hash;
} PgSessionOptimizerState;

typedef struct PgSessionPlanCacheState
{
	bool		initialized;
	dlist_head	saved_plan_list;
	dlist_head	cached_expression_list;
} PgSessionPlanCacheState;

typedef struct PgSessionNamespaceState
{
	bool		initialized;
	List	   *active_search_path;
	Oid			active_creation_namespace;
	bool		active_temp_creation_pending;
	uint64		active_path_generation;
	List	   *base_search_path;
	Oid			base_creation_namespace;
	bool		base_temp_creation_pending;
	Oid			namespace_user;
	bool		base_search_path_valid;
	bool		search_path_cache_valid;
	MemoryContext search_path_context;
	MemoryContext search_path_cache_context;
	Oid			my_temp_namespace;
	Oid			my_temp_toast_namespace;
	SubTransactionId my_temp_namespace_subid;
	char	   *namespace_search_path_value;
	void	   *search_path_cache;
	void	   *last_search_path_cache_entry;
} PgSessionNamespaceState;

typedef struct PgSessionLocaleState
{
	bool		initialized;
	char	   *locale_messages_value;
	char	   *locale_monetary_value;
	char	   *locale_numeric_value;
	char	   *locale_time_value;
	int			icu_validation_level_value;
	char	   *localized_abbrev_days_values[7 + 1];
	char	   *localized_full_days_values[7 + 1];
	char	   *localized_abbrev_months_values[12 + 1];
	char	   *localized_full_months_values[12 + 1];
	MemoryContext locale_time_context;
	void	   *default_locale;
	bool		locale_conv_valid;
	bool		locale_time_valid;
	MemoryContext locale_conv_context;
	void	   *current_locale_conv;
	bool		current_locale_conv_allocated;
	MemoryContext collation_cache_context;
	void	   *collation_cache;
	Oid			last_collation_cache_oid;
	void	   *last_collation_cache_locale;
	void	   *icu_converter;
} PgSessionLocaleState;

typedef struct PgRuntimeServerGUCState
{
	bool		initialized;
	char	   *cluster_name_value;
	char	   *config_file_name;
	char	   *hba_file_name;
	char	   *ident_file_name;
	char	   *hosts_file_name;
	char	   *external_pid_file_value;
} PgRuntimeServerGUCState;

typedef struct PgRuntimeExtensionModuleState
{
	MemoryContext memory_context;
	HTAB	   *rendezvous_hash;
	List	   *private_states;
} PgRuntimeExtensionModuleState;

#define PG_CONNECTION_SEND_BUFFER_SIZE 8192
#define PG_CONNECTION_RECV_BUFFER_SIZE 8192
#define PG_CONNECTION_CANCEL_KEY_LENGTH 32

typedef struct PgConnectionIdentityState
{
	struct Port *port;
	MemoryContext port_context;
	uint8		cancel_key[PG_CONNECTION_CANCEL_KEY_LENGTH];
	int			cancel_key_length;
} PgConnectionIdentityState;

typedef struct PgConnectionSocketIOState
{
	char	   *send_buffer;
	char	   *recv_buffer;
	MemoryContext socket_io_context;
	int			send_buffer_size;
	size_t		send_pointer;
	size_t		send_start;
	int			recv_pointer;
	int			recv_length;
	bool		comm_busy;
	bool		comm_reading_msg;
	int			win32_noblock;
	uint64		transport_generation;
} PgConnectionSocketIOState;

typedef struct PgConnectionProtocolState
{
	const PQcommMethods *comm_methods;
	WaitEventSet *fe_be_wait_set;
	uint32		frontend_protocol;
} PgConnectionProtocolState;

typedef struct PgConnectionOutputState
{
	CommandDest where_to_send_output;
	int			client_connection_check_interval;
} PgConnectionOutputState;

/*
 * A collection of timings of various stages of connection establishment and
 * setup for client backends and WAL senders.
 *
 * Used to emit the setup_durations log message for the log_connections GUC.
 */
typedef struct ConnectionTiming
{
	/*
	 * The time at which the client socket is created and the time at which
	 * the connection is fully set up and first ready for query. Together
	 * these represent the total connection establishment and setup time.
	 */
	TimestampTz socket_create;
	TimestampTz ready_for_use;

	/* Time at which process creation was initiated */
	TimestampTz fork_start;

	/* Time at which process creation was completed */
	TimestampTz fork_end;

	/* Time at which authentication started */
	TimestampTz auth_start;

	/* Time at which authentication was finished */
	TimestampTz auth_end;
} ConnectionTiming;

typedef struct PgConnectionInterruptState
{
	volatile sig_atomic_t check_client_connection_pending;
	volatile sig_atomic_t client_connection_lost;
} PgConnectionInterruptState;

typedef struct PgConnectionStartupState
{
	bool		client_auth_in_progress;
	struct ClientSocket *client_socket;
	ConnectionTiming timing;
	bool		connection_warnings_emitted;
	MemoryContext connection_warning_context;
	List	   *connection_warning_messages;
	List	   *connection_warning_details;
	List	   *connection_warning_filters;
} PgConnectionStartupState;

typedef struct PgConnectionClientConnectionInfoState
{
	const char *authn_id;
	UserAuth	auth_method;
} PgConnectionClientConnectionInfoState;

typedef struct PgConnectionSecurityState
{
	bool		ssl_loaded_verify_locations;
	char	   *gss_send_buffer;
	int			gss_send_length;
	int			gss_send_next;
	int			gss_send_consumed;
	char	   *gss_recv_buffer;
	int			gss_recv_length;
	char	   *gss_result_buffer;
	int			gss_result_length;
	int			gss_result_next;
	uint32		gss_max_packet_size;
	const char *pam_password;
	struct Port *pam_port;
	bool		pam_no_password;
} PgConnectionSecurityState;

/*
 * Main-loop state owned by PgSession. Some of this state used to be volatile
 * locals in PostgresMain(); now that the state lives in PgSession storage,
 * updates survive the top-level longjmp used for backend error recovery
 * without forcing volatile access in the hot loop.
 */
typedef struct PgSessionLoopState
{
	bool		send_ready_for_query;
	bool		idle_in_transaction_timeout_enabled;
	bool		idle_session_timeout_enabled;
	bool		doing_extended_query_message;
	bool		ignore_till_sync;
	bool		step_error_boundary_active;
	bool		doing_command_read;
	bool		transaction_started;
} PgSessionLoopState;

typedef struct PgSessionTcopState
{
	CachedPlanSource *unnamed_stmt_psrc;
	bool		echo_query;
	bool		use_semi_newline_newline;
	MemoryContext row_description_context;
	StringInfoData row_description_buf;
} PgSessionTcopState;

struct PgRuntime
{
	PgRuntimeKind kind;
	PgCarrier  *current_carrier;
	PgBackendModel extension_backend_model;

	/*
	 * Optional continuation used after PgBackendExitCleanup().  Process mode
	 * leaves this NULL and falls through to exit().  A threaded runtime must
	 * install a handler that removes the logical backend from its scheduler
	 * without returning to the cleaned-up backend stack.
	 */
	PgBackendExitContinuation exit_backend;

	PgProtocolSchedulerState protocol_scheduler;
	PgRuntimeServerGUCState server_guc;
	PgRuntimeExtensionModuleState extension_modules;
};

struct PgCarrier
{
	PgCarrierKind kind;
	PgRuntime  *runtime;
	PgBackend  *current_backend;
	PgSession  *current_session;
	PgExecution *current_execution;
	PgExecution *scheduler_execution;
	void	   *backend_thread_start;
	bool		is_under_postmaster;
	volatile sig_atomic_t wait_event_waiting;
	int			wait_event_signal_fd;
	int			wait_event_selfpipe_readfd;
	int			wait_event_selfpipe_writefd;
	int			wait_event_selfpipe_owner_pid;
	char	   *stack_base_ptr;
	int			threaded_guc_mutex_depth;
	int			threaded_reloptions_mutex_depth;
	bool		protocol_scheduler_registered;
	bool		protocol_scheduler_idle;

	/*
	 * xtc-carrier: whether the backend fiber that owns this carrier may be
	 * work-stolen across carrier loops (xtc_proc_opts_t.migratable).  The
	 * carrier struct is fiber-owned (it lives in the fiber's per-backend
	 * BackendThreadStart, not per-loop), so this flag rides with the fiber
	 * across a steal and can be read O(1) from any context via the fiber's
	 * xtc_proc_userdata() -- see xtc_pg_backend_fiber_is_migratable().  Always
	 * false in process mode and for pinned fibers (the default); set only at
	 * spawn for the client-backend fibers the gated unpin makes migratable.
	 */
	bool		migratable;
};

struct PgBackend
{
	PgBackendId id;
	PgRuntime  *runtime;
	PgCarrier  *carrier;
	PgSession  *session;
	PgConnection *connection;
	PgExecution *execution;
	PgBackendInterruptMailbox interrupts;
	struct Latch *interrupt_latch;
	PgBackendExitState exit_state;
	PgBackendCoreState core;
	PgBackendCommandState command;
	PgBackendLogState log_state;
	PgBackendExprInterpState expr_interp;
	PgBackendTimeoutState timeout;
	PgBackendWalSenderState walsender;
	PgBackendReplicationState replication;
	PgBackendLogicalReplicationState logical_replication;
	PgBackendXLogState xlog;
	PgBackendRecoveryState recovery;
	PgBackendMaintenanceWorkerState maintenance_worker;
	PgBackendAutovacuumState autovacuum;
	PgBackendRepackState repack;
	PgBackendAioState aio;
	PgBackendExtensionModuleState extension_modules;
	PgBackendPgStatPendingState pgstat_pending;
	PgBackendActivityState activity;
	PgBackendMemoryManagerState memory_manager;
	PgBackendUtilityState utility;
	PgBackendParallelState parallel;
	PgBackendInstrumentationState instrumentation;
	PgBackendBufferState buffers;
	PgBackendStorageState storage;
	PgBackendLockState locks;
	PgBackendIPCState ipc;
	PgBackendTransactionState transaction;
	PgBackendPendingInterruptState pending_interrupts;
	PgBackendInterruptHoldoffState interrupt_holdoffs;
	PgBackendWaitState wait_state;
	PgBackendProtocolParkState protocol_park;
	struct PGPROC *my_proc;
	ProcNumber	my_proc_number;
	ProcNumber	parallel_leader_proc_number;
	PgBackendStatus *my_beentry;
	BackgroundWorker *my_bgworker_entry;
	struct ResourceOwnerData *aux_process_resource_owner;

	/* Backend-local dynamic shared memory mappings and detach callbacks. */
	dlist_head	dsm_segment_list;

	BackendType backend_type;
};

struct PgSession
{
	PgBackend  *backend;
	PgConnection *connection;
	PgExecution *execution;
	Session    *legacy_session;
	MemoryContext legacy_session_context;
	PgSessionLoopState loop_state;
	PgSessionTcopState tcop;
	PgSessionDatabaseState database;
	PgSessionTablespaceState tablespace;
	PgSessionBinaryUpgradeState binary_upgrade;
	PgSessionDateTimeState datetime;
	PgSessionParserState parser;
	PgSessionVacuumState vacuum;
	PgSessionBufferIOState buffer_io;
	PgSessionXactDefaultState xact_defaults;
	PgSessionLockWaitState lock_wait;
	PgSessionLoggingState logging;
	PgSessionMiscGUCState misc_guc;
	PgSessionGUCState guc;
	PgSessionPgStatState pgstat;
	PgSessionQueryIdState query_id;
	PgSessionStorageGUCState storage_guc;
	PgSessionUserGUCState user_guc;
	PgSessionUserIdentityState user_identity;
	PgSessionCommandGUCState command_guc;
	PgSessionReplicationGUCState replication_guc;
	PgSessionLogicalReplicationState logical_replication;
	PgSessionGeneralGUCState general_guc;
	PgSessionAccessWalGUCState access_wal_guc;
	PgSessionJitGUCState jit_guc;
	PgSessionJitProviderState jit_provider_state;
	PgSessionLLVMJitState llvm_jit;
	PgSessionSortGUCState sort_guc;
	PgSessionTextSearchState text_search;
	PgSessionConnectionGUCState connection_guc;
	PgSessionQueryMemoryState query_memory;
	PgSessionPlannerCostState planner_cost;
	PgSessionPlannerMethodState planner_method;
	PgSessionFunctionManagerState function_manager;
	PgSessionExtensionModuleState extension_modules;
	PgSessionCatalogLookupState catalog_lookup;
	PgSessionInvalidationCallbackState invalidation_callbacks;
	PgSessionRIGlobalsState ri_globals;
	PgSessionRelMapState relmap;
	PgSessionPreparedStatementState prepared_statement;
	PgSessionOnCommitState on_commit;
	PgSessionSequenceState sequence;
	PgSessionXactCallbackState xact_callbacks;
	PgSessionBackupState backup;
	PgSessionRegexState regex;
	PgSessionPortalManagerState portal_manager;
	PgSessionLargeObjectState large_object;
	PgSessionAsyncState async;
	PgSessionEncodingState encoding;
	PgSessionTempFileState temp_file;
	PgSessionRandomState random;
	PgSessionOptimizerState optimizer;
	PgSessionPlanCacheState plan_cache;
	PgSessionNamespaceState namespace_state;
	PgSessionLocaleState locale;
	MemoryContext dynamic_library_context;
	List	   *dynamic_library_inits;
};

struct PgConnection
{
	PgBackend  *backend;
	PgSession  *session;
	PgConnectionIdentityState identity;
	PgConnectionSocketIOState socket_io;
	PgConnectionProtocolState protocol;
	PgConnectionOutputState output;
	PgConnectionInterruptState interrupts;
	PgConnectionStartupState startup;
	PgConnectionClientConnectionInfoState client_connection_info;
	MemoryContext client_connection_info_context;
	bool		client_connection_info_authn_id_owned;
	PgConnectionSecurityState security;
};

struct PgExecution
{
	PgBackend  *backend;
	PgSession  *session;
	PgCarrier  *carrier;
	PgExecutionDebugState debug;
	PgExecutionErrorState error;
	PgExecutionMemoryContextState memory_contexts;
	PgExecutionResourceOwnerState resource_owners;
	PgExecutionSPIState spi;
	PgExecutionPortalState portal;
	PgExecutionVacuumState vacuum;
	PgExecutionNodeIOState node_io;
	PgExecutionBaseBackupState basebackup;
	PgExecutionAnalyzeState analyze;
	PgExecutionExtensionState extension;
	PgExecutionMatViewState matview;
	PgExecutionSnapshotState snapshot;
	PgExecutionComboCidState combo_cid;
	PgExecutionXLogInsertState xloginsert;
	PgExecutionXactState xact;
	PgExecutionTransactionCleanupState transaction_cleanup;
	PgExecutionReplicationScratchState replication_scratch;
	PgExecutionGUCErrorState guc_error;
	PgExecutionAsyncState async;
	PgExecutionCatalogState catalog;
	PgExecutionCatalogCacheState catalog_cache;
	PgExecutionRelMapState relmap;
	PgExecutionInvalidationState invalidation;
	PgExecutionTwoPhaseRecordState two_phase_records;
	PgExecutionTriggerState trigger;
	PgExecutionRegexState regex;
	PgExecutionValgrindState valgrind;
	PgExecutionSnapBuildState snapbuild;
};

typedef struct PgThreadBackendLogicalState
{
	PgBackend	backend;
	PgSession	session;
	PgConnection connection;
	PgExecution execution;
} PgThreadBackendLogicalState;

typedef struct PgThreadBackendRuntimeState
{
	PgCarrier	carrier;
	PgThreadBackendLogicalState logical;
} PgThreadBackendRuntimeState;

extern void PgRuntimeResetAfterFork(void);
extern void PgRuntimeResetThreadForNewBackend(void);

extern bool *PgCurrentIsUnderPostmasterRef(void);
extern bool *PgCurrentDoingCommandReadRef(void);
extern MemoryContext *PgTopMemoryContextRef(void);
extern MemoryContext *PgCurrentMemoryContextRef(void);
extern void PgSetCurrentMemoryContextObject(MemoryContext context);
extern MemoryContext *PgErrorContextRef(void);
extern MemoryContext *PgMessageContextRef(void);
extern MemoryContext *PgTopTransactionContextRef(void);
extern MemoryContext *PgCurTransactionContextRef(void);
extern MemoryContext *PgPortalContextRef(void);
extern uint64 *PgCurrentSPIProcessedRef(void);
extern SPITupleTable **PgCurrentSPITuptableRef(void);
extern int *PgCurrentSPIResultRef(void);
extern _SPI_connection **PgCurrentSPIStackRef(void);
extern _SPI_connection **PgCurrentSPICurrentRef(void);
extern int *PgCurrentSPIStackDepthRef(void);
extern int *PgCurrentSPIConnectedRef(void);
extern Portal *PgCurrentActivePortalRef(void);
extern CommandDest *PgCurrentWhereToSendOutputRef(void);
extern int *PgCurrentClientConnectionCheckIntervalRef(void);
extern ConnectionTiming *PgCurrentConnectionTimingRef(void);
extern bool *PgCurrentConnectionWarningsEmittedRef(void);
extern List **PgCurrentConnectionWarningMessagesRef(void);
extern List **PgCurrentConnectionWarningDetailsRef(void);
extern List **PgCurrentConnectionWarningFiltersRef(void);
extern bool *PgCurrentVacuumInProgressRef(void);
extern int *PgCurrentVacuumCostBalanceRef(void);
extern bool *PgCurrentVacuumCostActiveRef(void);
extern pg_atomic_uint32 **PgCurrentVacuumSharedCostBalanceRef(void);
extern pg_atomic_uint32 **PgCurrentVacuumActiveNWorkersRef(void);
extern int *PgCurrentVacuumCostBalanceLocalRef(void);
extern bool *PgCurrentVacuumFailsafeActiveRef(void);
extern int64 *PgCurrentParallelVacuumWorkerDelayNsRef(void);
extern void **PgCurrentParallelVacuumSharedCostParamsRef(void);
extern uint32 *PgCurrentParallelVacuumSharedParamsGenerationLocalRef(void);
extern bool *PgCurrentNodeWriteLocationFieldsRef(void);
extern const char **PgCurrentNodeReadStrtokPtrRef(void);
extern bool *PgCurrentNodeRestoreLocationFieldsRef(void);
extern bool *PgCurrentBaseBackupStartedInRecoveryRef(void);
extern long long int *PgCurrentBaseBackupTotalChecksumFailuresRef(void);
extern bool *PgCurrentBaseBackupNoVerifyChecksumsRef(void);
extern MemoryContext *PgCurrentAnalyzeContextRef(void);
extern BufferAccessStrategy *PgCurrentAnalyzeStrategyRef(void);
extern void **PgCurrentArrayAnalyzeExtraDataRef(void);
extern bool *PgCurrentCreatingExtensionRef(void);
extern Oid *PgCurrentExtensionObjectRef(void);
extern int *PgCurrentMatViewMaintenanceDepthRef(void);
extern int *PgCurrentDeadlockTimeoutRef(void);
extern int *PgCurrentStatementTimeoutRef(void);
extern int *PgCurrentLockTimeoutRef(void);
extern int *PgCurrentIdleInTransactionSessionTimeoutRef(void);
extern int *PgCurrentTransactionTimeoutRef(void);
extern int *PgCurrentIdleSessionTimeoutRef(void);
extern bool *PgCurrentLogLockWaitsRef(void);
extern bool *PgCurrentLogLockFailuresRef(void);
extern int *PgCurrentTraceLockOidMinRef(void);
extern bool *PgCurrentTraceLocksRef(void);
extern bool *PgCurrentTraceUserlocksRef(void);
extern int *PgCurrentTraceLockTableRef(void);
extern bool *PgCurrentDebugDeadlocksRef(void);
extern bool *PgCurrentTraceLwlocksRef(void);
extern bool *PgCurrentDebugPrintPlanRef(void);
extern bool *PgCurrentDebugPrintParseRef(void);
extern bool *PgCurrentDebugPrintRawParseRef(void);
extern bool *PgCurrentDebugPrintRewrittenRef(void);
extern bool *PgCurrentDebugPrettyPrintRef(void);
#ifdef DEBUG_NODE_TESTS_ENABLED
extern bool *PgCurrentDebugCopyParsePlanTreesRef(void);
extern bool *PgCurrentDebugWriteReadParsePlanTreesRef(void);
extern bool *PgCurrentDebugRawExpressionCoverageTestRef(void);
#endif
extern bool *PgCurrentLogParserStatsRef(void);
extern bool *PgCurrentLogPlannerStatsRef(void);
extern bool *PgCurrentLogExecutorStatsRef(void);
extern bool *PgCurrentLogStatementStatsRef(void);
extern bool *PgCurrentLogBtreeBuildStatsRef(void);
extern char **PgCurrentEventSourceRef(void);
extern bool *PgCurrentLogDurationRef(void);
extern int *PgCurrentLogErrorVerbosityRef(void);
extern int *PgCurrentLogParameterMaxLengthRef(void);
extern int *PgCurrentLogParameterMaxLengthOnErrorRef(void);
extern int *PgCurrentLogMinErrorStatementRef(void);
extern int *PgCurrentLogMinMessagesArrayRef(void);
extern char **PgCurrentLogMinMessagesStringRef(void);
extern int *PgCurrentClientMinMessagesRef(void);
extern int *PgCurrentLogMinDurationSampleRef(void);
extern int *PgCurrentLogMinDurationStatementRef(void);
extern int *PgCurrentLogTempFilesRef(void);
extern double *PgCurrentLogStatementSampleRateRef(void);
extern double *PgCurrentLogXactSampleRateRef(void);
extern char **PgCurrentBacktraceFunctionsRef(void);
extern char **PgCurrentBacktraceFunctionListRef(void);
extern bool *PgCurrentAllowSystemTableModsRef(void);
extern int *PgCurrentMaxStackDepthRef(void);
extern ssize_t *PgCurrentMaxStackDepthBytesRef(void);
extern char **PgCurrentSessionPreloadLibrariesRef(void);
extern char **PgCurrentLocalPreloadLibrariesRef(void);
extern char **PgCurrentDynamicLibraryPathRef(void);
extern char **PgCurrentExtensionControlPathRef(void);
extern bool *PgCurrentUpdateProcessTitleRef(void);
extern MemoryContext *PgCurrentGUCMemoryContextRef(void);
/*
 * Non-allocating peek at the current GUC memory context cell.  Unlike
 * PgCurrentGUCMemoryContextRef(), this never lazily creates the early-fallback
 * context, so it is safe in invariant checks (e.g. build_guc_variables()'s
 * "not yet built" Assert) that must observe the cell without populating it.
 */
extern MemoryContext PgCurrentGUCMemoryContextPeek(void);
extern struct config_generic **PgCurrentGUCVariablesRef(void);
extern struct config_generic_state **PgCurrentGUCVariableStatesRef(void);
extern int *PgCurrentNumGUCVariablesRef(void);
extern HTAB **PgCurrentGUCHashTableRef(void);
extern dlist_head *PgCurrentGUCNondefListRef(void);
extern slist_head *PgCurrentGUCStackListRef(void);
extern slist_head *PgCurrentGUCReportListRef(void);
extern bool *PgCurrentGUCReportingEnabledRef(void);
extern int *PgCurrentGUCNestLevelRef(void);
extern int *PgCurrentThreadedGUCMutexDepthRef(void);
extern int *PgCurrentThreadedRelOptionsMutexDepthRef(void);
extern void **PgCurrentBackendThreadStartRef(void);
extern volatile sig_atomic_t *PgCurrentWaitEventWaitingRef(void);
extern int *PgCurrentWaitEventSignalFdRef(void);
extern int *PgCurrentWaitEventSelfPipeReadFdRef(void);
extern int *PgCurrentWaitEventSelfPipeWriteFdRef(void);
extern int *PgCurrentWaitEventSelfPipeOwnerPidRef(void);
extern char **PgCurrentStackBasePtrRef(void);
extern bool *PgCurrentPgStatTrackCountsRef(void);
extern int *PgCurrentPgStatTrackFunctionsRef(void);
extern int *PgCurrentPgStatFetchConsistencyRef(void);
extern bool *PgCurrentPgStatTrackActivitiesRef(void);
extern SessionEndType *PgCurrentPgStatSessionEndCauseRef(void);
extern PgStat_Counter *PgCurrentPgStatLastSessionReportTimeRef(void);
extern LocalPgBackendStatus **PgCurrentLocalBackendStatusTableRef(void);
extern int *PgCurrentLocalNumBackendsRef(void);
extern MemoryContext *PgCurrentBackendStatusSnapContextRef(void);
extern void PgBackendResetActivityClosedState(PgBackendActivityState *activity);
extern PgBackendAllocSetFreeList *PgCurrentAllocSetContextFreeLists(void);
extern void AllocSetFreeContextFreelists(PgBackendAllocSetFreeList *freelists,
										 int nfreelists);
extern bool *PgCurrentLogMemoryContextInProgressRef(void);
extern HTAB **PgCurrentSeqScanTables(void);
extern int *PgCurrentSeqScanLevels(void);
extern int *PgCurrentNumSeqScansRef(void);
extern volatile sig_atomic_t *PgCurrentNotifyInterruptPendingRef(void);
extern bool *PgCurrentAsyncUnlistenExitRegisteredRef(void);
extern dshash_table **PgCurrentAsyncGlobalChannelTableRef(void);
extern struct dsa_area **PgCurrentAsyncGlobalChannelDSARef(void);
extern struct ExtensionSiblingCache **PgCurrentExtensionSiblingListRef(void);
extern HTAB **PgCurrentInjectionPointCacheRef(void);
extern MemoryContext PgCurrentUtilityCacheMemoryContext(void);
extern ReservoirStateData *PgCurrentSamplingOldReservoirRef(void);
extern bool *PgCurrentSamplingOldReservoirInitializedRef(void);
extern Oid *PgCurrentSuperuserLastRoleIdRef(void);
extern bool *PgCurrentSuperuserLastRoleIdIsSuperRef(void);
extern bool *PgCurrentSuperuserRoleIdCallbackRegisteredRef(void);
extern void **PgCurrentResourceReleaseCallbacksRef(void);
#ifdef RESOWNER_STATS
extern int *PgCurrentResourceOwnerArrayLookupsRef(void);
extern int *PgCurrentResourceOwnerHashLookupsRef(void);
#endif
extern const void **PgCurrentDateTokenCache(void);
extern const void **PgCurrentDeltaTokenCache(void);
extern bool *PgCurrentDegreeConstsSetRef(void);
extern float8 *PgCurrentDegreeSin30Ref(void);
extern float8 *PgCurrentDegreeOneMinusCos60Ref(void);
extern float8 *PgCurrentDegreeAsin05Ref(void);
extern float8 *PgCurrentDegreeAcos05Ref(void);
extern float8 *PgCurrentDegreeAtan10Ref(void);
extern float8 *PgCurrentDegreeTan45Ref(void);
extern float8 *PgCurrentDegreeCot45Ref(void);
extern void **PgCurrentDCHCache(void);
extern int *PgCurrentNumDCHCacheRef(void);
extern int *PgCurrentDCHCounterRef(void);
extern void **PgCurrentNUMCache(void);
extern int *PgCurrentNumNUMCacheRef(void);
extern int *PgCurrentNUMCounterRef(void);
extern MemoryContext PgCurrentFormatCacheMemoryContext(void);
extern MemoryContext *PgCurrentLibxmlContextRef(void);
extern HTAB **PgCurrentMissingAttrCacheRef(void);
extern int *PgCurrentParallelWorkerNumberRef(void);
extern volatile sig_atomic_t *PgCurrentParallelMessagePendingRef(void);
extern bool *PgCurrentInitializingParallelWorkerRef(void);
extern void **PgCurrentFixedParallelStateRef(void);
extern dlist_head *PgCurrentParallelContextListRef(void);
extern bool *PgCurrentParallelContextListInitializedRef(void);
extern pid_t *PgCurrentParallelLeaderPidRef(void);
extern MemoryContext *PgCurrentParallelMessageContextRef(void);
extern void **PgCurrentPqMqHandleRef(void);
extern bool *PgCurrentPqMqBusyRef(void);
extern pid_t *PgCurrentPqMqParallelLeaderPidRef(void);
extern ProcNumber *PgCurrentPqMqParallelLeaderProcNumberRef(void);
extern int *PgCurrentComputeQueryIdRef(void);
extern bool *PgCurrentQueryIdEnabledRef(void);
extern bool *PgCurrentIgnoreChecksumFailureRef(void);
extern int *PgCurrentFileCopyMethodRef(void);
extern int *PgCurrentPasswordEncryptionRef(void);
extern char **PgCurrentCreateRoleSelfGrantRef(void);
extern bool *PgCurrentCreateRoleSelfGrantEnabledRef(void);
extern unsigned *PgCurrentCreateRoleSelfGrantOptionsSpecifiedRef(void);
extern bool *PgCurrentCreateRoleSelfGrantOptionsAdminRef(void);
extern bool *PgCurrentCreateRoleSelfGrantOptionsInheritRef(void);
extern bool *PgCurrentCreateRoleSelfGrantOptionsSetRef(void);
extern int *PgCurrentSessionReplicationRoleRef(void);
extern bool *PgCurrentEventTriggersRef(void);
extern bool *PgCurrentTraceNotifyRef(void);
extern int *PgCurrentWalSenderTimeoutRef(void);
extern int *PgCurrentWalSenderShutdownTimeoutRef(void);
extern bool *PgCurrentLogReplicationCommandsRef(void);
extern int *PgCurrentWalReceiverTimeoutRef(void);
extern int *PgCurrentLogicalDecodingWorkMemRef(void);
extern int *PgCurrentDebugLogicalReplicationStreamingRef(void);
extern struct ReplicationState **PgCurrentReplicationOriginSessionStateRef(void);
extern MemoryContext *PgCurrentLogicalRepRelMapContextRef(void);
extern HTAB **PgCurrentLogicalRepRelMapRef(void);
extern MemoryContext *PgCurrentLogicalRepPartMapContextRef(void);
extern HTAB **PgCurrentLogicalRepPartMapRef(void);
extern bool *PgCurrentPgOutputPublicationsValidRef(void);
extern HTAB **PgCurrentPgOutputRelationSyncCacheRef(void);
extern int *PgCurrentLogicalRepSyncingRelationsStateRef(void);
extern bool *PgCurrentAllowAlterSystemRef(void);
extern bool *PgCurrentRowSecurityRef(void);
extern bool *PgCurrentCheckFunctionBodiesRef(void);
extern bool *PgCurrentCurrentRoleIsSuperuserRef(void);
extern bool *PgCurrentDefaultWithOidsRef(void);
extern bool *PgCurrentStandardConformingStringsRef(void);
extern double *PgCurrentPhonyRandomSeedRef(void);
extern int *PgCurrentTempFileLimitRef(void);
extern int *PgCurrentNumTempBuffersRef(void);
extern char **PgCurrentRoleStringRef(void);
extern char **PgCurrentSessionAuthorizationStringRef(void);
extern bool *PgCurrentLoCompatPrivilegesRef(void);
extern int *PgCurrentExtraFloatDigitsRef(void);
extern bool *PgCurrentArrayNullsRef(void);
extern int *PgCurrentByteaOutputRef(void);
extern int *PgCurrentXmlBinaryRef(void);
extern int *PgCurrentXmlOptionRef(void);
extern bool *PgCurrentQuoteAllIdentifiersRef(void);
extern int *PgCurrentPlanCacheModeRef(void);
extern int *PgCurrentGinFuzzySearchLimitRef(void);
extern int *PgCurrentGinPendingListLimitRef(void);
extern char **PgCurrentDefaultTableAccessMethodRef(void);
extern bool *PgCurrentSynchronizeSeqscansRef(void);
extern int *PgCurrentDefaultToastCompressionRef(void);
extern int *PgCurrentWalCompressionRef(void);
extern bool *PgCurrentWalInitZeroRef(void);
extern bool *PgCurrentWalRecycleRef(void);
extern char **PgCurrentWalConsistencyCheckingStringRef(void);
extern bool **PgCurrentWalConsistencyCheckingRef(void);
extern int *PgCurrentCommitDelayRef(void);
extern int *PgCurrentCommitSiblingsRef(void);
extern bool *PgCurrentTrackWalIoTimingRef(void);
extern int *PgCurrentWalSkipThresholdRef(void);
#ifdef WAL_DEBUG
extern bool *PgCurrentXLogDebugRef(void);
#endif
#ifdef TRACE_SYNCSCAN
extern bool *PgCurrentTraceSyncscanRef(void);
#endif
extern bool *PgCurrentJitEnabledRef(void);
extern char **PgCurrentJitProviderRef(void);
extern bool *PgCurrentJitDebuggingSupportRef(void);
extern bool *PgCurrentJitDumpBitcodeRef(void);
extern bool *PgCurrentJitExpressionsRef(void);
extern bool *PgCurrentJitProfilingSupportRef(void);
extern bool *PgCurrentJitTupleDeformingRef(void);
extern double *PgCurrentJitAboveCostRef(void);
extern double *PgCurrentJitInlineAboveCostRef(void);
extern double *PgCurrentJitOptimizeAboveCostRef(void);
extern JitProviderCallbacks *PgCurrentJitProviderCallbacksRef(void);
extern bool *PgCurrentJitProviderSuccessfullyLoadedRef(void);
extern bool *PgCurrentJitProviderFailedLoadingRef(void);
extern bool *PgCurrentTraceSortRef(void);
#ifdef DEBUG_BOUNDED_SORT
extern bool *PgCurrentOptimizeBoundedSortRef(void);
#endif
extern char **PgCurrentTSCurrentConfigRef(void);
extern Oid *PgCurrentTSCurrentConfigCacheRef(void);
extern HTAB **PgCurrentTSParserCacheHashRef(void);
extern TSParserCacheEntry **PgCurrentTSLastUsedParserRef(void);
extern HTAB **PgCurrentTSDictionaryCacheHashRef(void);
extern TSDictionaryCacheEntry **PgCurrentTSLastUsedDictionaryRef(void);
extern HTAB **PgCurrentTSConfigCacheHashRef(void);
extern TSConfigCacheEntry **PgCurrentTSLastUsedConfigRef(void);
extern char **PgCurrentDefaultTablespaceRef(void);
extern char **PgCurrentTempTablespacesRef(void);
extern bool *PgCurrentAllowInPlaceTablespacesRef(void);
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
extern char **PgCurrentTimeZoneStringRef(void);
extern char **PgCurrentLogTimeZoneStringRef(void);
extern char **PgCurrentDateStyleStringRef(void);
extern char **PgCurrentTimeZoneAbbreviationsStringRef(void);
extern PgSessionDateTimeState *PgCurrentSessionDateTimeState(void);
extern pg_tz **PgCurrentSessionTimeZoneRef(void);
extern pg_tz **PgCurrentLogTimeZoneRef(void);
extern TimeZoneAbbrevTable **PgCurrentTimeZoneAbbrevTableRef(void);
extern PgSessionTzAbbrevCache *PgCurrentTimeZoneAbbrevCache(void);
extern char **PgCurrentClusterNameRef(void);
extern char **PgCurrentConfigFileNameRef(void);
extern char **PgCurrentHbaFileNameRef(void);
extern char **PgCurrentIdentFileNameRef(void);
extern char **PgCurrentHostsFileNameRef(void);
extern char **PgCurrentExternalPidFileRef(void);
extern char **PgCurrentApplicationNameRef(void);
extern int *PgCurrentSslRenegotiationLimitRef(void);
extern int *PgCurrentTcpKeepalivesIdleRef(void);
extern int *PgCurrentTcpKeepalivesIntervalRef(void);
extern int *PgCurrentTcpKeepalivesCountRef(void);
extern int *PgCurrentTcpUserTimeoutRef(void);
extern bool *PgCurrentLogDisconnectionsRef(void);
extern int *PgCurrentLogStatementRef(void);
extern int *PgCurrentPostAuthDelayRef(void);
extern char **PgCurrentRestrictNonsystemRelationKindStringRef(void);
extern int *PgCurrentRestrictNonsystemRelationKindRef(void);
extern CachedPlanSource **PgCurrentUnnamedStmtPsrcRef(void);
extern bool *PgCurrentEchoQueryRef(void);
extern bool *PgCurrentUseSemiNewlineNewlineRef(void);
extern MemoryContext *PgCurrentRowDescriptionContextRef(void);
extern StringInfoData *PgCurrentRowDescriptionBufRef(void);
extern MemoryContext *PgCurrentFunctionManagerMemoryContextRef(void);
extern MemoryContext PgCurrentFunctionManagerMemoryContext(void);
extern HTAB **PgCurrentCFuncHashRef(void);
extern HTAB **PgCurrentCachedFunctionHashRef(void);
extern CatCache **PgCurrentSysCacheArray(void);
extern bool *PgCurrentSysCacheInitializedRef(void);
extern Oid *PgCurrentSysCacheRelationOidArray(void);
extern int *PgCurrentSysCacheRelationOidSizeRef(void);
extern Oid *PgCurrentSysCacheSupportingRelOidArray(void);
extern int *PgCurrentSysCacheSupportingRelOidSizeRef(void);
extern CatCacheHeader **PgCurrentCatCacheHeaderRef(void);
extern HTAB **PgCurrentRelationIdCacheRef(void);
extern bool *PgCurrentCriticalRelcachesBuiltRef(void);
extern bool *PgCurrentCriticalSharedRelcachesBuiltRef(void);
extern long *PgCurrentRelcacheInvalsReceivedRef(void);
extern TupleDesc *PgCurrentPgClassDescriptorRef(void);
extern TupleDesc *PgCurrentPgIndexDescriptorRef(void);
extern HTAB **PgCurrentOpClassCacheRef(void);
extern HTAB **PgCurrentTypeCacheHashRef(void);
extern HTAB **PgCurrentRelIdToTypeIdCacheHashRef(void);
extern TypeCacheEntry **PgCurrentFirstDomainTypeEntryRef(void);
extern Oid **PgCurrentTypCacheInProgressListRef(void);
extern int *PgCurrentTypCacheInProgressListLenRef(void);
extern int *PgCurrentTypCacheInProgressListMaxLenRef(void);
extern HTAB **PgCurrentRecordCacheHashRef(void);
extern RecordCacheArrayEntry **PgCurrentRecordCacheArrayRef(void);
extern int32 *PgCurrentRecordCacheArrayLenRef(void);
extern int32 *PgCurrentNextRecordTypmodRef(void);
extern uint64 *PgCurrentTupleDescIdCounterRef(void);
extern HTAB **PgCurrentAttoptCacheHashRef(void);
extern HTAB **PgCurrentRelfilenumberMapHashRef(void);
extern ScanKeyData *PgCurrentRelfilenumberScanKeyArray(void);
extern HTAB **PgCurrentTableSpaceCacheHashRef(void);
extern HTAB **PgCurrentEventTriggerCacheRef(void);
extern MemoryContext *PgCurrentEventTriggerCacheContextRef(void);
extern int *PgCurrentEventTriggerCacheStateRef(void);
extern struct _SPI_plan **PgCurrentRuleutilsRuleByOidPlanRef(void);
extern struct _SPI_plan **PgCurrentRuleutilsViewRulePlanRef(void);
extern PgSessionInvalidationCallbackState *PgCurrentInvalidationCallbackState(void);
extern HTAB **PgCurrentRIConstraintCacheRef(void);
extern HTAB **PgCurrentRIQueryCacheRef(void);
extern HTAB **PgCurrentRICompareCacheRef(void);
extern dclist_head *PgCurrentRIConstraintCacheValidListRef(void);
extern bool *PgCurrentRIFastPathXactCallbackRegisteredRef(void);
extern int *PgCurrentDebugDiscardCachesRef(void);
extern PgExecutionRelMapFile *PgCurrentRelMapSharedMapRef(void);
extern PgExecutionRelMapFile *PgCurrentRelMapLocalMapRef(void);
extern HTAB **PgCurrentPreparedQueriesRef(void);
extern List **PgCurrentOnCommitActionsRef(void);
extern HTAB **PgCurrentSequenceHashTableRef(void);
extern struct SeqTableData **PgCurrentLastUsedSequenceRef(void);
extern XactCallbackItem **PgCurrentXactCallbacksRef(void);
extern SubXactCallbackItem **PgCurrentSubXactCallbacksRef(void);
extern MemoryContext PgCurrentXactCallbackMemoryContext(void);
extern struct BackupState **PgCurrentBackupStateRef(void);
extern StringInfo *PgCurrentTablespaceMapRef(void);
extern MemoryContext *PgCurrentBackupContextRef(void);
extern uint8 *PgCurrentSessionBackupStateRef(void);
extern HTAB **PgCurrentOperatorLookupCacheRef(void);
extern struct pg_ctype_cache **PgCurrentRegexCtypeCacheListRef(void);
extern MemoryContext *PgCurrentRegexpCacheMemoryContextRef(void);
extern int *PgCurrentRegexpNumCachedResRef(void);
extern PgSessionRegexCachedEntry *PgCurrentRegexpCachedResArray(void);
extern MemoryContext *PgCurrentTopPortalContextRef(void);
extern HTAB **PgCurrentPortalHashTableRef(void);
extern unsigned int *PgCurrentUnnamedPortalCountRef(void);
extern struct RelationData **PgCurrentLargeObjectHeapRelationRef(void);
extern struct RelationData **PgCurrentLargeObjectIndexRelationRef(void);
extern HTAB **PgCurrentAsyncLocalChannelTableRef(void);
extern bool *PgCurrentAsyncRegisteredListenerRef(void);
extern List **PgCurrentEncodingConvProcListRef(void);
extern MemoryContext PgCurrentEncodingCacheMemoryContext(void);
extern FmgrInfo **PgCurrentToServerConvProcRef(void);
extern FmgrInfo **PgCurrentToClientConvProcRef(void);
extern FmgrInfo **PgCurrentUtf8ToServerConvProcRef(void);
extern char **PgCurrentClientEncodingStringRef(void);
extern char **PgCurrentServerEncodingStringRef(void);
extern const pg_enc2name **PgCurrentClientEncodingRef(void);
extern const pg_enc2name **PgCurrentDatabaseEncodingRef(void);
extern const pg_enc2name **PgCurrentMessageEncodingRef(void);
extern bool *PgCurrentEncodingStartupCompleteRef(void);
extern int *PgCurrentPendingClientEncodingRef(void);
extern uint64 *PgCurrentTemporaryFilesSizeRef(void);
extern long *PgCurrentTempFileCounterRef(void);
extern Oid **PgCurrentTempTableSpaceOidsRef(void);
extern int *PgCurrentNumTempTableSpacesRef(void);
extern int *PgCurrentNextTempTableSpaceRef(void);
extern pg_prng_state *PgCurrentPseudoRandomStateRef(void);
extern bool *PgCurrentPseudoRandomSeedSetRef(void);
extern const char ***PgCurrentPlannerExtensionNameArrayRef(void);
extern int *PgCurrentPlannerExtensionNamesAssignedRef(void);
extern int *PgCurrentPlannerExtensionNamesAllocatedRef(void);
extern HTAB **PgCurrentOprProofCacheHashRef(void);
extern dlist_head *PgCurrentSavedPlanListRef(void);
extern dlist_head *PgCurrentCachedExpressionListRef(void);
extern PgSessionNamespaceState *PgCurrentNamespaceState(void);
extern char **PgCurrentNamespaceSearchPathRef(void);
extern PgSessionLocaleState *PgCurrentLocaleState(void);
extern void PgCloseIcuConverter(void *converter);
extern void **PgCurrentIcuConverterRef(void);
extern char **PgCurrentLocaleMessagesRef(void);
extern char **PgCurrentLocaleMonetaryRef(void);
extern char **PgCurrentLocaleNumericRef(void);
extern char **PgCurrentLocaleTimeRef(void);
extern int *PgCurrentIcuValidationLevelRef(void);
extern PgSessionUserIdentityState *PgCurrentUserIdentityState(void);
extern MemoryContext *PgCurrentSystemUserContextRef(void);
extern MemoryContext *PgCurrentDatabasePathContextRef(void);
extern bool *PgCurrentDatabasePathOwnedRef(void);
extern int *PgCurrentNLocBufferRef(void);
extern void **PgCurrentLocalBufferDescriptorsRef(void);
extern void **PgCurrentLocalBufferBlockPointersRef(void);
extern int32 **PgCurrentLocalRefCountRef(void);
extern int *PgCurrentNextFreeLocalBufIdRef(void);
extern HTAB **PgCurrentLocalBufHashRef(void);
extern int *PgCurrentNLocalPinnedBuffersRef(void);
extern char **PgCurrentLocalBufferCurBlockRef(void);
extern int *PgCurrentLocalBufferNextBufInBlockRef(void);
extern int *PgCurrentLocalBufferNumBufsInBlockRef(void);
extern int *PgCurrentLocalBufferTotalBufsAllocatedRef(void);
extern MemoryContext *PgCurrentLocalBufferContextRef(void);
extern BufferDesc **PgCurrentPinCountWaitBufRef(void);
extern WritebackContext *PgCurrentBackendWritebackContextRef(void);
extern void **PgCurrentPrivateRefCountArrayKeysRef(void);
extern void **PgCurrentPrivateRefCountArrayRef(void);
extern void **PgCurrentPrivateRefCountHashRef(void);
extern int32 *PgCurrentPrivateRefCountOverflowedRef(void);
extern uint32 *PgCurrentPrivateRefCountClockRef(void);
extern int *PgCurrentReservedRefCountSlotRef(void);
extern int *PgCurrentPrivateRefCountEntryLastRef(void);
extern uint32 *PgCurrentMaxProportionalPinsRef(void);
extern void **PgCurrentProcSignalSlotRef(void);
extern uint64 *PgCurrentSharedInvalidMessageCounterRef(void);
extern volatile sig_atomic_t *PgCurrentCatchupInterruptPendingRef(void);
extern void **PgCurrentSharedInvalidationMessagesRef(void);
extern volatile int *PgCurrentSharedInvalidationNextMsgRef(void);
extern volatile int *PgCurrentSharedInvalidationNumMsgsRef(void);
extern bool *PgCurrentDsmInitDoneRef(void);
extern void **PgCurrentDsmRegistryDsaRef(void);
extern void **PgCurrentDsmRegistryTableRef(void);
extern LocalTransactionId *PgCurrentNextLocalTransactionIdRef(void);
extern WaitEventSet **PgCurrentLatchWaitSetRef(void);
extern Latch *PgCurrentLocalLatchData(void);
extern uint32 **PgCurrentMyWaitEventInfoRef(void);
extern uint32 *PgCurrentLocalWaitEventInfoRef(void);
extern const char **PgCurrentUserDOptionRef(void);
extern struct rusage *PgCurrentUsageSaveRusageRef(void);
extern struct timeval *PgCurrentUsageSaveTimevalRef(void);
extern char *PgCurrentFormattedStartTimeBuffer(void);
extern long *PgCurrentLogLineNumberRef(void);
extern int *PgCurrentLogLinePidRef(void);
extern PgBackendExprInterpState *PgCurrentExprInterpState(void);
extern HTAB **PgCurrentLWLockStatsHashRef(void);
extern PgBackendLWLockStats *PgCurrentLWLockStatsDummy(void);
extern MemoryContext *PgCurrentLWLockStatsContextRef(void);
extern bool *PgCurrentLWLockStatsExitRegisteredRef(void);
extern PgBackendTimeoutState *PgCurrentTimeoutState(void);
extern PgBackendWalSenderState *PgCurrentWalSenderState(void);
extern PgBackendReplicationState *PgCurrentReplicationState(void);
extern PgBackendLogicalReplicationState *PgCurrentLogicalReplicationState(void);
extern PgBackendXLogState *PgCurrentXLogState(void);
extern PgBackendRecoveryState *PgCurrentRecoveryState(void);
extern PgBackendMaintenanceWorkerState *PgCurrentMaintenanceWorkerState(void);
extern PgBackendAutovacuumState *PgCurrentAutovacuumState(void);
extern PgBackendRepackState *PgCurrentRepackState(void);
extern volatile sig_atomic_t *PgCurrentRepackMessagePendingRef(void);
extern PgBackendAioState *PgCurrentAioState(void);
extern struct PgAioBackend **PgCurrentAioBackendRef(void);
extern PgBackendExtensionModuleState *PgCurrentBackendExtensionModuleState(void);
extern void *PgBackendGetExtensionPrivateState(const char *key);
extern void *PgBackendEnsureExtensionPrivateState(const char *key, Size size,
												 PgExtensionPrivateStateCleanup cleanup);
extern char **PgCurrentBasicArchiveDirectoryRef(void);
extern TransactionId *PgCurrentCachedFetchXidRef(void);
extern int *PgCurrentCachedFetchXidStatusRef(void);
extern XLogRecPtr *PgCurrentCachedCommitLSNRef(void);
extern void **PgCurrentTwoPhaseLockedGxactRef(void);
extern bool *PgCurrentTwoPhaseExitRegisteredRef(void);
extern FullTransactionId *PgCurrentTwoPhaseCachedFxidRef(void);
extern void **PgCurrentTwoPhaseCachedGxactRef(void);
extern int *PgCurrentSlruErrorCauseRef(void);
extern int *PgCurrentSlruErrnoRef(void);
extern HTAB **PgCurrentUncommittedEnumTypesRef(void);
extern HTAB **PgCurrentUncommittedEnumValuesRef(void);
extern Oid *PgCurrentReindexedHeapRef(void);
extern Oid *PgCurrentReindexedIndexRef(void);
extern List **PgCurrentPendingReindexedIndexesRef(void);
extern int *PgCurrentReindexingNestLevelRef(void);
extern struct PendingRelDelete **PgCurrentPendingRelDeletesRef(void);
extern HTAB **PgCurrentPendingSyncHashRef(void);
extern CatCInProgress **PgCurrentCatCacheInProgressStackRef(void);
extern InProgressEnt **PgCurrentRelcacheInProgressListRef(void);
extern int *PgCurrentRelcacheInProgressListLenRef(void);
extern int *PgCurrentRelcacheInProgressListMaxLenRef(void);
extern Oid *PgCurrentRelcacheEOXactList(void);
extern int *PgCurrentRelcacheEOXactListLenRef(void);
extern bool *PgCurrentRelcacheEOXactListOverflowedRef(void);
extern TupleDesc **PgCurrentRelcacheEOXactTupleDescArrayRef(void);
extern int *PgCurrentRelcacheNextEOXactTupleDescNumRef(void);
extern int *PgCurrentRelcacheEOXactTupleDescArrayLenRef(void);
extern PgExecutionRelMapFile *PgCurrentRelMapActiveSharedUpdatesRef(void);
extern PgExecutionRelMapFile *PgCurrentRelMapActiveLocalUpdatesRef(void);
extern PgExecutionRelMapFile *PgCurrentRelMapPendingSharedUpdatesRef(void);
extern PgExecutionRelMapFile *PgCurrentRelMapPendingLocalUpdatesRef(void);
extern PgExecutionInvalMessageArray *PgCurrentInvalMessageArrays(void);
extern struct TransInvalidationInfo **PgCurrentTransInvalInfoRef(void);
extern struct InvalidationInfo **PgCurrentInplaceInvalInfoRef(void);
extern PgExecutionTwoPhaseRecordState *PgCurrentTwoPhaseRecordStateRef(void);
extern int *PgCurrentTriggerDepthRef(void);
extern void **PgCurrentAfterTriggersDataRef(void);
extern MemoryContext PgCurrentAfterTriggersMemoryContext(void);
extern MemoryContext *PgCurrentAfterTriggersMemoryContextRef(void);
extern dclist_head *PgCurrentMultiXactCacheRef(void);
extern bool *PgCurrentMultiXactCacheInitializedRef(void);
extern MemoryContext *PgCurrentMultiXactContextRef(void);
extern char **PgCurrentMultiXactDebugStringRef(void);
extern TransactionId *PgCurrentProcArrayCachedXidNotInProgressRef(void);
extern struct GlobalVisState *PgCurrentGlobalVisSharedRelsRef(void);
extern struct GlobalVisState *PgCurrentGlobalVisCatalogRelsRef(void);
extern struct GlobalVisState *PgCurrentGlobalVisDataRelsRef(void);
extern struct GlobalVisState *PgCurrentGlobalVisTempRelsRef(void);
extern TransactionId *PgCurrentComputeXidHorizonsResultLastXminRef(void);
extern long *PgCurrentXidCacheByRecentXminRef(void);
extern long *PgCurrentXidCacheByKnownXactRef(void);
extern long *PgCurrentXidCacheByMyXactRef(void);
extern long *PgCurrentXidCacheByLatestXidRef(void);
extern long *PgCurrentXidCacheByMainXidRef(void);
extern long *PgCurrentXidCacheByChildXidRef(void);
extern long *PgCurrentXidCacheByKnownAssignedRef(void);
extern long *PgCurrentXidCacheNoOverflowRef(void);
extern long *PgCurrentXidCacheSlowAnswerRef(void);
extern void **PgCurrentVfdCacheRef(void);
extern Size *PgCurrentSizeVfdCacheRef(void);
extern int *PgCurrentNFileRef(void);
extern bool *PgCurrentTemporaryFilesAllowedRef(void);
extern int *PgCurrentNumAllocatedDescsRef(void);
extern int *PgCurrentMaxAllocatedDescsRef(void);
extern void **PgCurrentAllocatedDescsRef(void);
extern int *PgCurrentNumExternalFDsRef(void);
extern HTAB **PgCurrentSyncPendingOpsRef(void);
extern List **PgCurrentSyncPendingUnlinksRef(void);
extern MemoryContext *PgCurrentSyncPendingOpsContextRef(void);
extern uint16 *PgCurrentSyncCycleCounterRef(void);
extern uint16 *PgCurrentSyncCheckpointCycleCounterRef(void);
extern bool *PgCurrentSyncInProgressRef(void);
extern HTAB **PgCurrentSMgrRelationHashRef(void);
extern dlist_head *PgCurrentSMgrUnpinnedRelationsRef(void);
extern MemoryContext *PgCurrentMdContextRef(void);
extern void **PgCurrentFastPathLocalUseCountsRef(void);
extern bool *PgCurrentFastPathLocalUseCountsOwnedRef(void);
extern PgBackendLWLockHandle *PgCurrentHeldLWLocks(void);
extern int *PgCurrentNumHeldLWLocksRef(void);
extern int *PgCurrentLocalNumUserDefinedLWLockTranchesRef(void);
extern bool *PgCurrentRelationExtensionLockHeldRef(void);
extern HTAB **PgCurrentLockMethodLocalHashRef(void);
extern void **PgCurrentStrongLockInProgressRef(void);
extern void **PgCurrentAwaitedLockRef(void);
extern void **PgCurrentAwaitedOwnerRef(void);
extern volatile sig_atomic_t *PgCurrentDeadlockTimeoutPendingRef(void);
extern void **PgCurrentConditionVariableSleepTargetRef(void);
extern uint32 *PgCurrentSpeculativeInsertionTokenRef(void);
extern void **PgCurrentDeadlockVisitedProcsRef(void);
extern int *PgCurrentDeadlockNVisitedProcsRef(void);
extern void **PgCurrentDeadlockTopoProcsRef(void);
extern void **PgCurrentDeadlockBeforeConstraintsRef(void);
extern void **PgCurrentDeadlockAfterConstraintsRef(void);
extern void **PgCurrentDeadlockWaitOrdersRef(void);
extern int *PgCurrentDeadlockNWaitOrdersRef(void);
extern void **PgCurrentDeadlockWaitOrderProcsRef(void);
extern void **PgCurrentDeadlockCurConstraintsRef(void);
extern int *PgCurrentDeadlockNCurConstraintsRef(void);
extern int *PgCurrentDeadlockMaxCurConstraintsRef(void);
extern void **PgCurrentDeadlockPossibleConstraintsRef(void);
extern int *PgCurrentDeadlockNPossibleConstraintsRef(void);
extern int *PgCurrentDeadlockMaxPossibleConstraintsRef(void);
extern void **PgCurrentDeadlockDetailsRef(void);
extern int *PgCurrentDeadlockNDetailsRef(void);
extern bool *PgCurrentDeadlockWorkspaceOwnedRef(void);
extern void **PgCurrentBlockingAutovacuumProcRef(void);
extern HTAB **PgCurrentLocalPredicateLockHashRef(void);
extern void **PgCurrentMySerializableXactRef(void);
extern bool *PgCurrentMyXactDidWriteRef(void);
extern void **PgCurrentSavedSerializableXactRef(void);

extern void InitializePgProcessRuntime(void);
extern void InitializePgThreadRuntime(PgBackendExitContinuation exit_backend);
extern void InitializePgThreadCarrierRuntimeState(PgCarrier *carrier);
extern void InitializePgThreadBackendLogicalState(PgThreadBackendLogicalState *logical,
												 PgCarrier *carrier,
												 BackendType backend_type,
												 struct Port *port,
												 struct Latch *interrupt_latch);
extern void InitializePgThreadBackendRuntimeState(PgThreadBackendRuntimeState *state,
												 BackendType backend_type,
												 struct Port *port,
												 struct Latch *interrupt_latch);
extern void PreInstallPgThreadBackendRuntimeState(PgThreadBackendRuntimeState *state);
extern void InstallPgThreadBackendRuntimeState(PgThreadBackendRuntimeState *state);
extern void InitializePgThreadBackendRuntime(PgThreadBackendRuntimeState *state,
											 BackendType backend_type,
											 struct Port *port,
											 struct Latch *interrupt_latch);
extern void PgSetCurrentRuntime(PgRuntime *runtime);
extern void PgSetCurrentCarrier(PgCarrier *carrier);
extern void PgSetCurrentBackend(PgBackend *backend);
extern void PgSetCurrentSession(PgSession *session);
extern void PgSetCurrentConnection(PgConnection *connection);
extern void PgSetCurrentExecution(PgExecution *execution);
extern void PgRuntimeSetCurrentWork(PgRuntime *runtime, PgCarrier *carrier,
									PgBackend *backend, PgSession *session,
									PgConnection *connection,
									PgExecution *execution,
									bool rebind_session_gucs);

/*
 * Snapshot of a thread's current-work pointers, used by the xtc carrier to
 * make current work fiber-local across a cooperative yield.  See
 * PgRuntimeSaveCurrentWork / PgRuntimeRestoreCurrentWork.
 */
typedef struct PgCurrentWorkSnapshot
{
	PgRuntime  *runtime;
	PgCarrier  *carrier;
	PgBackend  *backend;
	PgSession  *session;
	PgConnection *connection;
	PgExecution *execution;
} PgCurrentWorkSnapshot;

extern void PgRuntimeSaveCurrentWork(PgCurrentWorkSnapshot *snap);
extern void PgRuntimeRestoreCurrentWork(const PgCurrentWorkSnapshot *snap);
extern void PgRuntimeRestoreCurrentWorkLazy(const PgCurrentWorkSnapshot *snap);
extern void PgCarrierAttachBackend(PgCarrier *carrier, PgBackend *backend,
								   PgSession *session,
								   PgConnection *connection,
								   PgExecution *execution);
extern void PgCarrierDetachBackend(PgCarrier *carrier, PgBackend *backend);
extern void PgRuntimeReportBridgeFallbackStats(void);
extern bool PgCurrentSessionOwnsPointer(const void *ptr);
extern bool PgCurrentOrEarlySessionOwnsPointer(const void *ptr);
extern void PgBackendResetClosedState(PgBackend *backend);
extern MemoryContext PgSessionGetDynamicLibraryMemoryContext(PgSession *session);
extern List **PgCurrentSessionDynamicLibraryInitsRef(void);
extern PgRuntimeExtensionModuleState *PgCurrentRuntimeExtensionModuleState(void);
extern MemoryContext PgCurrentRuntimeExtensionModuleMemoryContext(void);
extern void *PgRuntimeGetExtensionPrivateState(const char *key);
extern void *PgRuntimeEnsureExtensionPrivateState(const char *key, Size size,
												 PgExtensionPrivateStateCleanup cleanup);
extern MemoryContext *PgCurrentPgPlanAdviceContextRef(void);
extern List **PgCurrentPgPlanAdviceAdvisorHookListRef(void);
extern MemoryContext *PgCurrentBloomContextRef(void);
extern HTAB **PgCurrentRendezvousHashRef(void);
extern void **PgCurrentPLpgSQLSessionStateRef(void);
extern PgSessionExtensionModuleState *PgCurrentSessionExtensionModuleState(void);
extern void *PgSessionGetExtensionPrivateState(const char *key);
extern void *PgSessionEnsureExtensionPrivateState(const char *key, Size size,
												 PgSessionExtensionPrivateStateCleanup cleanup);
extern void **PgCurrentPLpythonProcedureCacheRef(void);
extern MemoryContext *PgCurrentPLpythonMemoryContextRef(void);
extern bool *PgCurrentPLpythonResetRegisteredRef(void);
extern MemoryContext *PgCurrentPLperlMemoryContextRef(void);
extern bool *PgCurrentPLperlInitedRef(void);
extern void **PgCurrentPLperlInterpHashRef(void);
extern void **PgCurrentPLperlProcHashRef(void);
extern void **PgCurrentPLperlActiveInterpRef(void);
extern void **PgCurrentPLperlHeldInterpRef(void);
extern bool *PgCurrentPLperlUseStrictRef(void);
extern char **PgCurrentPLperlOnInitRef(void);
extern char **PgCurrentTestBackendRuntimeCustomGucRef(void);
extern int *PgCurrentTestBackendRuntimeCustomGucInitCounterRef(void);
extern char **PgCurrentPLperlOnPLperlInitRef(void);
extern char **PgCurrentPLperlOnPLperluInitRef(void);
extern bool *PgCurrentPLperlEndingRef(void);
extern void **PgCurrentPLperlCurrentCallDataRef(void);
extern bool *PgCurrentPLperlResetRegisteredRef(void);
extern MemoryContext *PgCurrentPLTclMemoryContextRef(void);
extern char **PgCurrentPLTclStartProcRef(void);
extern char **PgCurrentPLTclUStartProcRef(void);
extern void **PgCurrentPLTclHoldInterpRef(void);
extern void **PgCurrentPLTclInterpHashRef(void);
extern void **PgCurrentPLTclProcHashRef(void);
extern void **PgCurrentPLTclCurrentCallStateRef(void);
extern bool *PgCurrentPLTclResetRegisteredRef(void);
extern MemoryContext *PgCurrentPLsampleMemoryContextRef(void);
extern PgExecutionExtensionState *PgCurrentExecutionExtensionState(void);
extern void PgExecutionInitializeExtensionState(PgExecutionExtensionState *extension);
extern void *PgExecutionGetExtensionPrivateState(const char *key);
extern void *PgExecutionEnsureExtensionPrivateState(const char *key, Size size,
												   PgExtensionPrivateStateCleanup cleanup);
extern PgExecutionDebugHandler *PgCurrentPgcryptoDebugHandlerRef(void);
extern void PgSessionRegisterResetCallback(PgSessionResetCallback callback,
										   void *arg);
extern void PgSessionResetClosedState(PgSession *session);
extern void PgExecutionResetClosedState(PgExecution *execution);
extern MemoryContext PgCurrentResourceOwnerMemoryContext(void);
extern Session *PgSessionGetLegacySession(PgSession *session);
extern Session *PgCurrentLegacySession(void);
extern Session **PgCurrentLegacySessionRef(void);
extern struct Port **PgConnectionProcPortRef(PgConnection *connection);
extern struct Port **PgCurrentProcPortRef(void);
extern MemoryContext *PgConnectionPortContextRef(PgConnection *connection);
extern MemoryContext *PgCurrentPortContextRef(void);
extern uint8 *PgConnectionCancelKey(PgConnection *connection);
extern uint8 *PgCurrentCancelKey(void);
extern int *PgConnectionCancelKeyLengthRef(PgConnection *connection);
extern int *PgCurrentCancelKeyLengthRef(void);
extern const char **PgExecutionDebugQueryStringRef(PgExecution *execution);
extern const char **PgCurrentDebugQueryStringRef(void);
extern SnapshotData *PgCurrentSnapshotDataRef(void);
extern SnapshotData *PgCurrentSecondarySnapshotDataRef(void);
extern SnapshotData *PgCurrentCatalogSnapshotDataRef(void);
extern Snapshot *PgCurrentSnapshotRef(void);
extern Snapshot *PgCurrentSecondarySnapshotRef(void);
extern Snapshot *PgCurrentCatalogSnapshotRef(void);
extern Snapshot *PgCurrentHistoricSnapshotRef(void);
extern TransactionId *PgCurrentTransactionXminRef(void);
extern TransactionId *PgCurrentRecentXminRef(void);
extern HTAB **PgCurrentTupleCidDataRef(void);
extern void **PgCurrentActiveSnapshotRef(void);
extern pairingheap *PgCurrentRegisteredSnapshotsRef(void);
extern bool *PgCurrentFirstSnapshotSetRef(void);
extern Snapshot *PgCurrentFirstXactSnapshotRef(void);
extern List **PgCurrentExportedSnapshotsRef(void);
extern HTAB **PgCurrentComboCidHashRef(void);
extern void **PgCurrentComboCidsRef(void);
extern int *PgCurrentUsedComboCidsRef(void);
extern int *PgCurrentSizeComboCidsRef(void);
extern void **PgCurrentXLogInsertRegisteredBuffersRef(void);
extern int *PgCurrentXLogInsertMaxRegisteredBuffersRef(void);
extern int *PgCurrentXLogInsertMaxRegisteredBlockIdRef(void);
extern XLogRecData **PgCurrentXLogInsertMainRDataHeadRef(void);
extern XLogRecData **PgCurrentXLogInsertMainRDataLastRef(void);
extern uint64 *PgCurrentXLogInsertMainRDataLenRef(void);
extern uint8 *PgCurrentXLogInsertFlagsRef(void);
extern XLogRecData *PgCurrentXLogInsertHeaderRecordDataRef(void);
extern char **PgCurrentXLogInsertHeaderScratchRef(void);
extern XLogRecData **PgCurrentXLogInsertRDatasRef(void);
extern int *PgCurrentXLogInsertNumRDatasRef(void);
extern int *PgCurrentXLogInsertMaxRDatasRef(void);
extern bool *PgCurrentXLogInsertBeginCalledRef(void);
extern MemoryContext *PgCurrentXLogInsertContextRef(void);
extern uint64 *PgCurrentGetXLogBufferCachedPageRef(void);
extern char **PgCurrentGetXLogBufferCachedPosRef(void);
extern int *PgCurrentXactIsoLevelRef(void);
extern bool *PgCurrentXactReadOnlyRef(void);
extern bool *PgCurrentXactDeferrableRef(void);
extern bool *PgCurrentXactIsSampledRef(void);
extern TransactionId *PgCurrentCheckXidAliveRef(void);
extern bool *PgCurrentBSysScanRef(void);
extern int *PgCurrentMyXactFlagsRef(void);
extern FullTransactionId *PgCurrentXactTopFullTransactionIdRef(void);
extern int *PgCurrentNParallelCurrentXidsRef(void);
extern TransactionId **PgCurrentParallelCurrentXidsRef(void);
extern int *PgCurrentNUnreportedXidsRef(void);
extern TransactionId *PgCurrentUnreportedXids(void);
extern SubTransactionId *PgCurrentSubTransactionIdCounterRef(void);
extern CommandId *PgCurrentCommandIdCounterRef(void);
extern bool *PgCurrentCommandIdUsedRef(void);
extern TimestampTz *PgCurrentXactStartTimestampRef(void);
extern TimestampTz *PgCurrentStmtStartTimestampRef(void);
extern TimestampTz *PgCurrentXactStopTimestampRef(void);
extern char **PgCurrentPrepareGIDRef(void);
extern bool *PgCurrentForceSyncCommitRef(void);
extern MemoryContext *PgCurrentTransactionAbortContextRef(void);
extern TransactionStateData **PgCurrentTopTransactionStateDataRef(void);
extern TransactionStateData **PgCurrentTransactionStateRef(void);
extern LargeObjectDesc ***PgCurrentLargeObjectCookiesRef(void);
extern int *PgCurrentLargeObjectCookiesSizeRef(void);
extern bool *PgCurrentLargeObjectCleanupNeededRef(void);
extern MemoryContext *PgCurrentLargeObjectContextRef(void);
extern bool *PgCurrentHaveXactTemporaryFilesRef(void);
extern PgStat_SubXactStatus **PgCurrentPgStatXactStackRef(void);
extern HTAB **PgCurrentRIFastPathCacheRef(void);
extern bool *PgCurrentRIFastPathCallbackRegisteredRef(void);
extern bool *PgCurrentRIFastPathFlushingRef(void);
extern EventTriggerQueryState **PgCurrentEventTriggerQueryStateRef(void);
extern MemoryContext PgCurrentEventTriggerMemoryContext(void);
extern MemoryContext *PgCurrentEventTriggerMemoryContextRef(void);
extern ReplOriginXactState *PgCurrentReplOriginXactStateRef(void);
extern ErrorContextCallback **PgCurrentApplyErrorContextStackRef(void);
extern MemoryContext *PgCurrentApplyMessageContextRef(void);
extern MemoryContext *PgCurrentLogicalStreamingContextRef(void);
extern int *PgCurrentGUCCheckErrcodeValueRef(void);
extern char **PgCurrentGUCCheckErrmsgStringRef(void);
extern char **PgCurrentGUCCheckErrdetailStringRef(void);
extern char **PgCurrentGUCCheckErrhintStringRef(void);
extern int *PgCurrentFormatErrnumberRef(void);
extern const char **PgCurrentFormatDomainRef(void);
extern unsigned int *PgCurrentConfigFileLinenoRef(void);
extern const char **PgCurrentGUCFlexFatalErrmsgRef(void);
extern sigjmp_buf **PgCurrentGUCFlexFatalJmpRef(void);
extern struct ActionList **PgCurrentPendingActionsRef(void);
extern HTAB **PgCurrentPendingListenActionsRef(void);
extern struct NotificationList **PgCurrentPendingNotifiesRef(void);
extern PgExecutionAsyncQueuePosition *PgCurrentQueueHeadBeforeWriteRef(void);
extern PgExecutionAsyncQueuePosition *PgCurrentQueueHeadAfterWriteRef(void);
extern MemoryContext PgCurrentAsyncSignalWorkspaceContext(void);
extern int32 **PgCurrentSignalPidsRef(void);
extern ProcNumber **PgCurrentSignalProcnosRef(void);
extern bool *PgCurrentTryAdvanceTailRef(void);
extern void **PgCurrentRegexLocaleRef(void);
extern unsigned int *PgCurrentValgrindOldErrorCountRef(void);
extern struct ResourceOwnerData **PgCurrentSnapBuildSavedResourceOwnerDuringExportRef(void);
extern bool *PgCurrentSnapBuildExportInProgressRef(void);
extern PgConnectionSocketIOState *PgConnectionSocketIORef(PgConnection *connection);
extern PgConnectionSocketIOState *PgCurrentConnectionSocketIORef(void);
extern MemoryContext *PgConnectionSocketIOContextRef(PgConnection *connection);
extern MemoryContext *PgCurrentConnectionSocketIOContextRef(void);
extern int *PgCurrentPgwin32NoBlockRef(void);
extern const PQcommMethods **PgConnectionPqCommMethodsRef(PgConnection *connection);
extern const PQcommMethods **PgCurrentPqCommMethodsRef(void);
extern WaitEventSet **PgConnectionFeBeWaitSetRef(PgConnection *connection);
extern WaitEventSet **PgCurrentFeBeWaitSetRef(void);
extern uint32 *PgConnectionFrontendProtocolRef(PgConnection *connection);
extern uint32 *PgCurrentFrontendProtocolRef(void);
extern volatile sig_atomic_t *PgConnectionCheckClientConnectionPendingRef(PgConnection *connection);
extern volatile sig_atomic_t *PgCurrentCheckClientConnectionPendingRef(void);
extern volatile sig_atomic_t *PgConnectionClientConnectionLostRef(PgConnection *connection);
extern volatile sig_atomic_t *PgCurrentClientConnectionLostRef(void);
extern bool *PgConnectionClientAuthInProgressRef(PgConnection *connection);
extern bool *PgCurrentClientAuthInProgressRef(void);
extern struct ClientSocket **PgConnectionClientSocketRef(PgConnection *connection);
extern struct ClientSocket **PgCurrentClientSocketRef(void);
extern void *PgConnectionClientConnectionInfoRef(PgConnection *connection);
extern void *PgCurrentClientConnectionInfoRef(void);
extern MemoryContext *PgConnectionClientConnectionInfoContextRef(PgConnection *
																connection);
extern MemoryContext *PgCurrentClientConnectionInfoContextRef(void);
extern bool *PgCurrentClientConnectionInfoAuthnIdOwnedRef(void);
extern PgConnectionSecurityState *PgConnectionSecurityStateRef(PgConnection *connection);
extern PgConnectionSecurityState *PgCurrentConnectionSecurityStateRef(void);
extern bool PgRuntimeKindIsThreadBacked(PgRuntimeKind kind);
extern bool PgRuntimeIsThreadBacked(PgRuntime *runtime);
extern bool PgRuntimeKindIsPooledProtocol(PgRuntimeKind kind);
extern bool PgRuntimeIsPooledProtocol(PgRuntime *runtime);
extern bool PgRuntimePooledProtocolRequested(void);
extern int	PgRuntimePooledProtocolCarrierLimit(void);
extern uint32 PgRuntimePooledProtocolIdleCarrierCount(void);
extern uint32 PgRuntimePooledProtocolRunnableCount(void);
extern PgBackendLaunchModel PgRuntimeGetBackendLaunchModel(BackendType backend_type);
extern bool PgRuntimeShouldThreadBackend(BackendType backend_type);
extern PgBackendModel PgRuntimeGetExtensionBackendModel(void);
extern void PgRuntimeSetExtensionBackendModel(PgBackendModel backend_model);
extern MemoryContext PgBackendBufferAllocationContext(void);
#define PgRuntimeGetOwnedMemoryContextWithSizes(context, name, ...) \
	((*(context) != NULL) ? *(context) : \
	 (*(context) = AllocSetContextCreate(TopMemoryContext, (name), \
										 __VA_ARGS__)))
#define PgRuntimeGetOwnedMemoryContext(context, name) \
	PgRuntimeGetOwnedMemoryContextWithSizes((context), (name), \
											ALLOCSET_SMALL_SIZES)
extern void PgRuntimeDeleteOwnedMemoryContext(MemoryContext *context);
extern void PgBackendInitializeInterrupts(PgBackend *backend);
extern void PgBackendAdoptEarlyState(PgBackend *backend);
extern void PgSessionAdoptEarlyState(PgSession *session);
extern void PgConnectionAdoptEarlyState(PgConnection *connection,
										 struct Port *preserved_port);
extern void PgConnectionResetClosedState(PgConnection *connection);
extern void PgExecutionAdoptEarlyState(PgExecution *execution);
extern void PgBackendSetInterruptLatch(PgBackend *backend,
										struct Latch *interrupt_latch);
extern PgBackendId PgBackendGetId(PgBackend *backend);
extern PgBackendId PgCurrentBackendId(void);
extern int	PgBackendGetSignalPid(PgBackend *backend);
extern int	PgCurrentBackendSignalPid(void);
extern bool PgBackendUsesProcessSignals(PgBackend *backend);
extern void PgBackendWakeup(PgBackend *backend);
extern void PgBackendUnregisterThreadedBackend(PgBackend *backend);
extern bool PgBackendSendInterruptById(PgBackendId backend_id,
									  PgBackendInterruptType interrupt_type,
									  int sender_pid, int sender_uid);
extern uint64 PgBackendNotifyInterruptGeneration(PgBackend *backend);
extern void PgRuntimeInitializeProtocolScheduler(PgProtocolSchedulerState *scheduler);
extern bool PgRuntimeProtocolSchedulerParkBackend(PgRuntime *runtime,
												  PgBackend *backend);
extern bool PgRuntimeProtocolSchedulerMarkRunnable(PgRuntime *runtime,
												   PgBackend *backend);
extern bool PgRuntimeProtocolSchedulerLeaseBackend(PgRuntime *runtime,
												   PgBackend *backend);
extern PgBackend *PgRuntimeProtocolSchedulerLeaseParkedBackend(PgRuntime *runtime);
extern bool PgRuntimeProtocolSchedulerReparkBackend(PgRuntime *runtime,
													PgBackend *backend);
extern bool PgRuntimeProtocolSchedulerReparkBackendIfPolling(PgRuntime *runtime,
															 PgBackend *backend);
extern PgBackend *PgRuntimeProtocolSchedulerPopRunnable(PgRuntime *runtime);
extern int	PgRuntimeProtocolSchedulerCollectParked(PgRuntime *runtime,
													PgBackend **backends,
													int max_backends);
extern int	PgRuntimeProtocolSchedulerWaitParkedReads(PgRuntime *runtime,
													 PgBackend **scratch,
													 struct pollfd *poll_scratch,
													 int max_backends,
													 int wake_fd,
													 long timeout_ms);
extern bool PgRuntimeProtocolSchedulerRegisterCarrier(PgRuntime *runtime,
													  PgCarrier *carrier);
extern bool PgRuntimeProtocolSchedulerUnregisterCarrier(PgRuntime *runtime,
														PgCarrier *carrier);
extern PgBackend *PgCarrierLeaseRunnableProtocolBackend(PgCarrier *carrier);
extern bool PgRuntimeProtocolSchedulerRemoveBackend(PgRuntime *runtime,
													PgBackend *backend);
extern bool PgBackendSnapshotProtocolParkById(PgBackendId backend_id,
											  PgProtocolParkSnapshot *snapshot);
/*
 * Logical backend interrupts are for backend events such as cancel, die,
 * notify, and proc-signal-derived work. Wait readiness should remain with
 * latches, condition variables, wait event sets, or Phase 13 wait-completion
 * records until the scheduler owns that wait family.
 */
extern void SendInterrupt(PgBackend *backend,
						  PgBackendInterruptType interrupt_type);
extern void RaiseInterrupt(PgBackendInterruptType interrupt_type);
extern void PgBackendRaiseInterrupt(PgBackend *backend,
									 PgBackendInterruptType interrupt_type);
extern void PgBackendRaiseProcDieInterrupt(PgBackend *backend, int sender_pid,
										   int sender_uid);
extern void PgCurrentBackendRaiseInterrupt(PgBackendInterruptType interrupt_type);
extern void PgCurrentBackendRaiseProcDieInterrupt(int sender_pid,
												 int sender_uid);
extern PgBackendInterruptMask PgBackendConsumeInterrupts(PgBackend *backend);
extern void PgBackendConsumeProcDieSender(PgBackend *backend, int *sender_pid,
										  int *sender_uid);
extern bool PgCurrentBackendHasPendingInterrupts(void);
extern void PgCurrentBackendApplyInterrupts(void);
/*
 * Generic wait-completion publication is diagnostic-only.  Production builds
 * leave PG_RUNTIME_ENABLE_WAIT_COMPLETION_PUBLICATION undefined, making these
 * APIs no-op/false except for local wait-state initialization.
 */
extern bool PgSetWaitCompletionPublication(bool enabled);
extern PgWaitCompletion *PgBackendCurrentWaitCompletion(PgBackend *backend);
extern bool PgBackendSnapshotWaitCompletionById(PgBackendId backend_id,
												PgWaitCompletion *snapshot,
												uint32 *waiting);
extern void PgBackendMarkWaitCompletionInterrupt(PgBackend *backend,
												 PgWaitCompletionInterrupt interrupt);
extern bool PgBackendWakeWaitCompletion(PgBackend *backend,
										uint32 ready_events);
extern bool PgBackendWakeWaitCompletionById(PgBackendId backend_id,
											uint32 ready_events);
extern bool PgBackendShouldPublishWaitCompletion(PgBackend *backend);
extern bool PgBackendLogicalTimeoutNextWake(PgBackend *backend,
											TimestampTz *wake_at,
											uint64 *generation);
extern bool PgBackendPrepareProtocolReadPark(PgBackend *backend,
											 PgProtocolParkSpec *spec);
extern void PgCarrierCommitProtocolReadPark(PgCarrier *carrier,
											PgBackend *backend);
extern bool PgBackendMarkProtocolReadParkWake(PgBackend *backend,
											  uint64 generation,
											  uint32 wake_reasons,
											  uint32 wake_events);
extern bool PgBackendMarkProtocolReadParkDeferredNotify(PgBackend *backend,
														uint64 notify_generation,
														uint32 wake_reasons);
extern void PgBackendClearProtocolReadParkDeferredNotify(PgBackend *backend);
extern bool PgBackendProtocolReadParkTimeoutGenerationValid(PgBackend *backend,
															uint64 generation);
extern void PgBackendResumeProtocolReadPark(PgBackend *backend);
extern int	PgSuspend(const PgWaitSpec *wait_spec,
					  PgSuspendCallback callback, void *callback_arg);
extern PgStepResult PgSessionStep(PgSession *session, PgStepBudget budget);
pg_noreturn extern void PgSessionRun(PgSession *session);

#define PG_RUNTIME_FAST_BUCKET_ACCESSOR(variable, fallback) \
	__extension__ \
	({ \
		typeof(variable) pg_runtime_bucket = (variable); \
 \
		if (unlikely(pg_runtime_bucket == NULL)) \
		{ \
			PG_RUNTIME_BRIDGE_COUNT_FALLBACK(fast_bucket); \
			pg_runtime_bucket = fallback(); \
		} \
		pg_runtime_bucket; \
	})

#define PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(variable, fallback) \
	__extension__ \
	({ \
		typeof(variable) pg_runtime_bucket = (variable); \
 \
		if (unlikely(pg_runtime_bucket == NULL || \
					 !pg_runtime_bucket->initialized)) \
		{ \
			PG_RUNTIME_BRIDGE_COUNT_FALLBACK(fast_initialized_bucket); \
			pg_runtime_bucket = fallback(); \
		} \
		pg_runtime_bucket; \
	})

#define PG_RUNTIME_FAST_BUCKET_ACCESSOR_INITIALIZED_BY(variable, fallback, initialized_member) \
	__extension__ \
	({ \
		typeof(variable) pg_runtime_bucket = (variable); \
 \
		if (unlikely(pg_runtime_bucket == NULL || \
					 !pg_runtime_bucket->initialized_member)) \
		{ \
			PG_RUNTIME_BRIDGE_COUNT_FALLBACK(fast_initialized_bucket); \
			pg_runtime_bucket = fallback(); \
		} \
		pg_runtime_bucket; \
	})

#define PG_RUNTIME_CURRENT_FIELD_REF(variable, fallback, member) \
	__extension__ \
	({ \
		typeof(variable) pg_runtime_bucket = (variable); \
 \
		unlikely(pg_runtime_bucket == NULL) ? \
		(PG_RUNTIME_BRIDGE_COUNT_FALLBACK_EXPR(fast_bucket), (fallback)()) : \
		&pg_runtime_bucket->member; \
	})

#define PG_RUNTIME_CURRENT_INITIALIZED_BY_FIELD_REF(variable, fallback, initialized_member, member) \
	__extension__ \
	({ \
		typeof(variable) pg_runtime_bucket = (variable); \
 \
		unlikely(pg_runtime_bucket == NULL || \
				 !pg_runtime_bucket->initialized_member) ? \
		(PG_RUNTIME_BRIDGE_COUNT_FALLBACK_EXPR(fast_initialized_bucket), \
		 (fallback)()) : \
		&pg_runtime_bucket->member; \
	})

#define PG_RUNTIME_CURRENT_FIELD_REF_PASTE(variable, fallback, head, tail) \
	__extension__ \
	({ \
		typeof(variable) pg_runtime_bucket = (variable); \
 \
		unlikely(pg_runtime_bucket == NULL) ? \
		(PG_RUNTIME_BRIDGE_COUNT_FALLBACK_EXPR(fast_bucket), (fallback)()) : \
		&pg_runtime_bucket->head ## tail; \
	})

#define PG_RUNTIME_CURRENT_INITIALIZED_FIELD_REF(variable, fallback, member) \
	__extension__ \
	({ \
		typeof(variable) pg_runtime_bucket = (variable); \
 \
		unlikely(pg_runtime_bucket == NULL || \
				 !pg_runtime_bucket->initialized) ? \
		(PG_RUNTIME_BRIDGE_COUNT_FALLBACK_EXPR(fast_initialized_bucket), \
		 (fallback)()) : \
		&pg_runtime_bucket->member; \
	})

#define PG_RUNTIME_CURRENT_INITIALIZED_FIELD_REF_PASTE(variable, fallback, head, tail) \
	__extension__ \
	({ \
		typeof(variable) pg_runtime_bucket = (variable); \
 \
		unlikely(pg_runtime_bucket == NULL || \
				 !pg_runtime_bucket->initialized) ? \
		(PG_RUNTIME_BRIDGE_COUNT_FALLBACK_EXPR(fast_initialized_bucket), \
		 (fallback)()) : \
		&pg_runtime_bucket->head ## tail; \
	})

#define PG_RUNTIME_CURRENT_FIELD_PTR(variable, fallback, member) \
	__extension__ \
	({ \
		typeof(variable) pg_runtime_bucket = (variable); \
 \
		unlikely(pg_runtime_bucket == NULL) ? \
		(PG_RUNTIME_BRIDGE_COUNT_FALLBACK_EXPR(fast_bucket), (fallback)()) : \
		pg_runtime_bucket->member; \
	})

#define PG_RUNTIME_CURRENT_FIELD_PTR_PASTE(variable, fallback, head, tail) \
	__extension__ \
	({ \
		typeof(variable) pg_runtime_bucket = (variable); \
 \
		unlikely(pg_runtime_bucket == NULL) ? \
		(PG_RUNTIME_BRIDGE_COUNT_FALLBACK_EXPR(fast_bucket), (fallback)()) : \
		pg_runtime_bucket->head ## tail; \
	})

#define PG_RUNTIME_CURRENT_INITIALIZED_FIELD_PTR(variable, fallback, member) \
	__extension__ \
	({ \
		typeof(variable) pg_runtime_bucket = (variable); \
 \
		unlikely(pg_runtime_bucket == NULL || \
				 !pg_runtime_bucket->initialized) ? \
		(PG_RUNTIME_BRIDGE_COUNT_FALLBACK_EXPR(fast_initialized_bucket), \
		 (fallback)()) : \
		pg_runtime_bucket->member; \
	})

#define PG_RUNTIME_CURRENT_ROOT_FIELD_REF(root, fallback, member) \
	__extension__ \
	({ \
		typeof(PgRuntimeCurrentBridgeState.root) pg_runtime_owner = \
			PgRuntimeCurrentBridgeState.root; \
 \
		unlikely(pg_runtime_owner == NULL) ? \
		(PG_RUNTIME_BRIDGE_COUNT_FALLBACK_EXPR(fast_bucket), (fallback)()) : \
		&pg_runtime_owner->member; \
	})

#define PG_RUNTIME_CURRENT_ROOT_FIELD_PTR(root, fallback, member) \
	__extension__ \
	({ \
		typeof(PgRuntimeCurrentBridgeState.root) pg_runtime_owner = \
			PgRuntimeCurrentBridgeState.root; \
 \
		unlikely(pg_runtime_owner == NULL) ? \
		(PG_RUNTIME_BRIDGE_COUNT_FALLBACK_EXPR(fast_bucket), (fallback)()) : \
		pg_runtime_owner->member; \
	})

#define PG_RUNTIME_CURRENT_CARRIER_FIELD_REF(fallback, member) \
	__extension__ \
	({ \
		PgCarrier  *pg_runtime_carrier = PgRuntimeCurrentBridgeState.carrier; \
 \
		unlikely(pg_runtime_carrier == NULL) ? \
		(PG_RUNTIME_BRIDGE_COUNT_FALLBACK_EXPR(carrier), (fallback)()) : \
		&pg_runtime_carrier->member; \
	})

#ifndef BACKEND_RUNTIME_NO_INLINE_BUCKET_ACCESSORS
#define PgCurrentBackendThreadStartRef() \
	PG_RUNTIME_CURRENT_CARRIER_FIELD_REF(PgCurrentBackendThreadStartRef, \
										 backend_thread_start)
#define PgCurrentIsUnderPostmasterRef() \
	PG_RUNTIME_CURRENT_CARRIER_FIELD_REF(PgCurrentIsUnderPostmasterRef, \
										 is_under_postmaster)
#define PgCurrentThreadedGUCMutexDepthRef() \
	PG_RUNTIME_CURRENT_CARRIER_FIELD_REF(PgCurrentThreadedGUCMutexDepthRef, \
										 threaded_guc_mutex_depth)
#define PgCurrentThreadedRelOptionsMutexDepthRef() \
	PG_RUNTIME_CURRENT_CARRIER_FIELD_REF(PgCurrentThreadedRelOptionsMutexDepthRef, \
										 threaded_reloptions_mutex_depth)
#define PgCurrentWaitEventWaitingRef() \
	PG_RUNTIME_CURRENT_CARRIER_FIELD_REF(PgCurrentWaitEventWaitingRef, \
										 wait_event_waiting)
#define PgCurrentWaitEventSignalFdRef() \
	PG_RUNTIME_CURRENT_CARRIER_FIELD_REF(PgCurrentWaitEventSignalFdRef, \
										 wait_event_signal_fd)
#define PgCurrentWaitEventSelfPipeReadFdRef() \
	PG_RUNTIME_CURRENT_CARRIER_FIELD_REF(PgCurrentWaitEventSelfPipeReadFdRef, \
										 wait_event_selfpipe_readfd)
#define PgCurrentWaitEventSelfPipeWriteFdRef() \
	PG_RUNTIME_CURRENT_CARRIER_FIELD_REF(PgCurrentWaitEventSelfPipeWriteFdRef, \
										 wait_event_selfpipe_writefd)
#define PgCurrentWaitEventSelfPipeOwnerPidRef() \
	PG_RUNTIME_CURRENT_CARRIER_FIELD_REF(PgCurrentWaitEventSelfPipeOwnerPidRef, \
										 wait_event_selfpipe_owner_pid)
#define PgCurrentStackBasePtrRef() \
	PG_RUNTIME_CURRENT_CARRIER_FIELD_REF(PgCurrentStackBasePtrRef, \
										 stack_base_ptr)
#include "utils/backend_runtime_current_state_accessor_prototypes.def"
#include "utils/backend_runtime_hot_bucket_accessors.def"
#include "utils/backend_runtime_current_field_accessor_prototypes.def"
#include "utils/backend_runtime_current_field_accessors.def"
#include "utils/backend_runtime_current_state_field_accessor_prototypes.def"
#include "utils/backend_runtime_current_state_field_accessors.def"
#endif

#endif							/* BACKEND_RUNTIME_H */
