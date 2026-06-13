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

#include "access/session.h"
#include "access/transam.h"
#include "access/xlogdefs.h"
#include "common/pg_prng.h"
#include "common/relpath.h"
#include "executor/instrument.h"
#include "fmgr.h"
#include "lib/ilist.h"
#include "lib/stringinfo.h"
#include "libpq/hba.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "nodes/pg_list.h"
#include "pgtime.h"
#include "pgstat.h"
#include "port/atomics.h"
#include "storage/buf.h"
#include "storage/checksum.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/procnumber.h"
#include "storage/relfilelocator.h"
#include "tcop/dest.h"
#include "utils/backend_id.h"
#include "utils/backend_status.h"
#include "utils/global_lifetime.h"
#include "utils/hsearch.h"
#include "utils/palloc.h"
#include "utils/sampling.h"
#include "utils/timeout.h"

typedef struct PgRuntime PgRuntime;
typedef struct PgCarrier PgCarrier;
typedef struct PgBackend PgBackend;
typedef struct PgBackendStatus PgBackendStatus;
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
typedef struct dsa_area dsa_area;
typedef struct dshash_table dshash_table;
typedef struct XLogReaderState XLogReaderState;
typedef struct PortalData *Portal;
typedef struct SPITupleTable SPITupleTable;
typedef struct _SPI_connection _SPI_connection;
struct LogicalRepRelMapEntry;
struct SeqTableData;
struct pg_ctype_cache;
struct RelationData;
struct avl_dbase;
struct WorkerInfoData;
struct DecodingWorker;
struct ExtensionSiblingCache;
struct PgAioBackend;
struct PgAioUringContext;
struct AllocSetContext;
struct ClientSocket;
typedef struct dsm_segment dsm_segment;
typedef void (*PgBackendExitContinuation) (int code);
typedef int (*PgSuspendCallback) (void *callback_arg);

typedef enum PgRuntimeKind
{
	PG_RUNTIME_PROCESS,
	PG_RUNTIME_THREAD_PER_SESSION
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
 * Budget for one invocation of PgSessionStep().  The process-mode runner uses
 * a single-message budget today; later schedulers can extend this contract
 * without changing the caller shape.
 */
typedef struct PgStepBudget
{
	int			max_messages;
} PgStepBudget;

typedef enum PgStepResult
{
	PG_STEP_CONTINUE,
	PG_STEP_ERROR_RECOVERED
} PgStepResult;

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
	volatile int proc_die_sender_pid;
	volatile int proc_die_sender_uid;
} PgBackendInterruptMailbox;

typedef enum PgWaitKind
{
	PG_WAIT_KIND_NONE,
	PG_WAIT_KIND_EVENT_SET
} PgWaitKind;

typedef struct PgWaitSpec
{
	PgWaitKind	kind;
	uint32		wait_event_info;
	uint32		wake_events;
	long		timeout;
} PgWaitSpec;

typedef struct PgBackendWaitState
{
	PgWaitSpec	spec;
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
} PgBackendRepackState;

typedef struct PgBackendAioState
{
	struct PgAioBackend *my_backend;
	int			my_io_worker_id;
	struct PgAioUringContext *my_uring_context;
} PgBackendAioState;

typedef struct PgBackendPgStatPendingState
{
	void	   *entry_ref_hash;
	int			shared_ref_age;
	MemoryContext shared_ref_context;
	MemoryContext entry_ref_hash_context;
	PgStat_BgWriterStats pending_bgwriter;
	PgStat_CheckpointerStats pending_checkpointer;
	PgStat_PendingIO io_stats;
	bool		io_stats_pending;
	PgStat_SLRUStats slru_stats[PGSTAT_SLRU_NUM_ELEMENTS];
	bool		slru_stats_pending;
	PgStat_PendingLock lock_stats;
	bool		lock_stats_pending;
	PgStat_BackendPending backend_stats;
	bool		backend_io_stats_pending;
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
	struct ExtensionSiblingCache *extension_sibling_list;
	HTAB	   *injection_point_cache;
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
	WritebackContext *backend_writeback_context;
	void	   *private_ref_count_array_keys;
	void	   *private_ref_count_array;
	void	   *private_ref_count_hash;
	int32		private_ref_count_overflowed;
	uint32		private_ref_count_clock;
	int			reserved_ref_count_slot;
	int			private_ref_count_entry_last;
	uint32		max_proportional_pins;
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

#define PG_BACKEND_MAX_SIMUL_LWLOCKS 200

typedef struct PgBackendLWLockHandle
{
	LWLock	   *lock;
	LWLockMode	mode;
} PgBackendLWLockHandle;

typedef struct PgBackendLockState
{
	PgBackendLWLockHandle held_lwlocks[PG_BACKEND_MAX_SIMUL_LWLOCKS];
	int			num_held_lwlocks;
	int			local_num_user_defined_lwlock_tranches;
	void	   *fast_path_local_use_counts;
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

typedef struct PgExecutionDebugState
{
	const char *debug_query_string;
} PgExecutionDebugState;

typedef struct PgExecutionErrorState
{
	struct ErrorContextCallback *context_stack;
	sigjmp_buf *exception_stack;
} PgExecutionErrorState;

typedef struct PgExecutionMemoryContextState
{
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
} PgExecutionAnalyzeState;

typedef struct PgExecutionExtensionState
{
	bool		creating;
	Oid			current_object;
} PgExecutionExtensionState;

typedef struct PgExecutionMatViewState
{
	int			maintenance_depth;
} PgExecutionMatViewState;

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

typedef struct PgSessionDateTimeState
{
	bool		initialized;
	int			date_style;
	int			date_order;
	int			interval_style;
	char	   *timezone_string_value;
	char	   *log_timezone_string_value;
	pg_tz	   *session_timezone_value;
	pg_tz	   *log_timezone_value;
} PgSessionDateTimeState;

typedef struct PgSessionTextSearchState
{
	bool		initialized;
	char	   *current_config_value;
	Oid			current_config_cache;
} PgSessionTextSearchState;

typedef struct PgSessionConnectionGUCState
{
	bool		initialized;
	char	   *application_name_value;
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
} PgSessionMiscGUCState;

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
	bool		session_user_is_superuser;
	int			security_restriction_context;
	bool		set_role_is_active;
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

typedef struct PgSessionGeneralGUCState
{
	bool		initialized;
	bool		allow_alter_system_value;
	bool		row_security_value;
	bool		check_function_bodies_value;
	bool		current_role_is_superuser_value;
	int			temp_file_limit_kb;
	int			num_temp_buffers_blocks;
	char	   *role_string_value;
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

typedef struct PgSessionRegexState
{
	struct pg_ctype_cache *ctype_cache_list;
} PgSessionRegexState;

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
	FmgrInfo   *to_server_conv_proc;
	FmgrInfo   *to_client_conv_proc;
	FmgrInfo   *utf8_to_server_conv_proc;
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
	void	   *default_locale;
	bool		locale_conv_valid;
	bool		locale_time_valid;
	void	   *current_locale_conv;
	bool		current_locale_conv_allocated;
	MemoryContext collation_cache_context;
	void	   *collation_cache;
	Oid			last_collation_cache_oid;
	void	   *last_collation_cache_locale;
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

#define PG_CONNECTION_SEND_BUFFER_SIZE 8192
#define PG_CONNECTION_RECV_BUFFER_SIZE 8192
#define PG_CONNECTION_CANCEL_KEY_LENGTH 32

typedef struct PgConnectionIdentityState
{
	struct Port *port;
	uint8		cancel_key[PG_CONNECTION_CANCEL_KEY_LENGTH];
	int			cancel_key_length;
} PgConnectionIdentityState;

typedef struct PgConnectionSocketIOState
{
	char	   *send_buffer;
	int			send_buffer_size;
	size_t		send_pointer;
	size_t		send_start;
	char		recv_buffer[PG_CONNECTION_RECV_BUFFER_SIZE];
	int			recv_pointer;
	int			recv_length;
	bool		comm_busy;
	bool		comm_reading_msg;
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
 * locals in PostgresMain(); keep the loop flags volatile because they must
 * survive the top-level longjmp used for backend error recovery.
 */
typedef struct PgSessionLoopState
{
	volatile bool send_ready_for_query;
	volatile bool idle_in_transaction_timeout_enabled;
	volatile bool idle_session_timeout_enabled;
	volatile bool doing_extended_query_message;
	volatile bool ignore_till_sync;
	volatile bool step_error_boundary_active;
	bool		transaction_started;
} PgSessionLoopState;

struct PgRuntime
{
	PgRuntimeKind kind;
	PgCarrier  *current_carrier;
	PgBackendModel extension_backend_model;
	PgRuntimeServerGUCState server_guc;

	/*
	 * Optional continuation used after PgBackendExitCleanup().  Process mode
	 * leaves this NULL and falls through to exit().  A threaded runtime must
	 * install a handler that removes the logical backend from its scheduler
	 * without returning to the cleaned-up backend stack.
	 */
	PgBackendExitContinuation exit_backend;
};

struct PgCarrier
{
	PgCarrierKind kind;
	PgRuntime  *runtime;
	PgBackend  *current_backend;
	PgSession  *current_session;
	PgExecution *current_execution;
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
	PgSessionLoopState loop_state;
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
	PgSessionPgStatState pgstat;
	PgSessionQueryIdState query_id;
	PgSessionStorageGUCState storage_guc;
	PgSessionUserGUCState user_guc;
	PgSessionUserIdentityState user_identity;
	PgSessionCommandGUCState command_guc;
	PgSessionReplicationGUCState replication_guc;
	PgSessionGeneralGUCState general_guc;
	PgSessionAccessWalGUCState access_wal_guc;
	PgSessionJitGUCState jit_guc;
	PgSessionSortGUCState sort_guc;
	PgSessionTextSearchState text_search;
	PgSessionConnectionGUCState connection_guc;
	PgSessionQueryMemoryState query_memory;
	PgSessionPlannerCostState planner_cost;
	PgSessionPlannerMethodState planner_method;
	PgSessionPreparedStatementState prepared_statement;
	PgSessionOnCommitState on_commit;
	PgSessionSequenceState sequence;
	PgSessionRegexState regex;
	PgSessionLargeObjectState large_object;
	PgSessionAsyncState async;
	PgSessionEncodingState encoding;
	PgSessionTempFileState temp_file;
	PgSessionRandomState random;
	PgSessionOptimizerState optimizer;
	PgSessionPlanCacheState plan_cache;
	PgSessionNamespaceState namespace_state;
	PgSessionLocaleState locale;
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
};

typedef struct PgThreadBackendRuntimeState
{
	PgCarrier	carrier;
	PgBackend	backend;
	PgSession	session;
	PgConnection connection;
	PgExecution execution;
} PgThreadBackendRuntimeState;

extern PGDLLIMPORT PG_GLOBAL_RUNTIME PgRuntime *CurrentPgRuntime;
extern PGDLLIMPORT PG_THREAD_LOCAL PG_GLOBAL_CARRIER PgCarrier *CurrentPgCarrier;
extern PGDLLIMPORT PG_THREAD_LOCAL PG_GLOBAL_CARRIER PgBackend *CurrentPgBackend;
extern PGDLLIMPORT PG_THREAD_LOCAL PG_GLOBAL_CARRIER PgSession *CurrentPgSession;
extern PGDLLIMPORT PG_THREAD_LOCAL PG_GLOBAL_CARRIER PgConnection *CurrentPgConnection;
extern PGDLLIMPORT PG_THREAD_LOCAL PG_GLOBAL_CARRIER PgExecution *CurrentPgExecution;

extern MemoryContext *PgCurrentMemoryContextRef(void);
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
extern bool *PgCurrentPgStatTrackCountsRef(void);
extern int *PgCurrentPgStatTrackFunctionsRef(void);
extern int *PgCurrentPgStatFetchConsistencyRef(void);
extern bool *PgCurrentPgStatTrackActivitiesRef(void);
extern SessionEndType *PgCurrentPgStatSessionEndCauseRef(void);
extern PgStat_Counter *PgCurrentPgStatLastSessionReportTimeRef(void);
extern LocalPgBackendStatus **PgCurrentLocalBackendStatusTableRef(void);
extern int *PgCurrentLocalNumBackendsRef(void);
extern MemoryContext *PgCurrentBackendStatusSnapContextRef(void);
extern PgBackendAllocSetFreeList *PgCurrentAllocSetContextFreeLists(void);
extern bool *PgCurrentLogMemoryContextInProgressRef(void);
extern HTAB **PgCurrentSeqScanTables(void);
extern int *PgCurrentSeqScanLevels(void);
extern int *PgCurrentNumSeqScansRef(void);
extern volatile sig_atomic_t *PgCurrentNotifyInterruptPendingRef(void);
extern bool *PgCurrentAsyncUnlistenExitRegisteredRef(void);
extern struct ExtensionSiblingCache **PgCurrentExtensionSiblingListRef(void);
extern HTAB **PgCurrentInjectionPointCacheRef(void);
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
extern MemoryContext *PgCurrentLibxmlContextRef(void);
extern HTAB **PgCurrentMissingAttrCacheRef(void);
extern int *PgCurrentParallelWorkerNumberRef(void);
extern volatile sig_atomic_t *PgCurrentParallelMessagePendingRef(void);
extern bool *PgCurrentInitializingParallelWorkerRef(void);
extern void **PgCurrentFixedParallelStateRef(void);
extern dlist_head *PgCurrentParallelContextListRef(void);
extern bool *PgCurrentParallelContextListInitializedRef(void);
extern pid_t *PgCurrentParallelLeaderPidRef(void);
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
extern bool *PgCurrentAllowAlterSystemRef(void);
extern bool *PgCurrentRowSecurityRef(void);
extern bool *PgCurrentCheckFunctionBodiesRef(void);
extern bool *PgCurrentCurrentRoleIsSuperuserRef(void);
extern int *PgCurrentTempFileLimitRef(void);
extern int *PgCurrentNumTempBuffersRef(void);
extern char **PgCurrentRoleStringRef(void);
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
extern bool *PgCurrentTraceSortRef(void);
#ifdef DEBUG_BOUNDED_SORT
extern bool *PgCurrentOptimizeBoundedSortRef(void);
#endif
extern char **PgCurrentTSCurrentConfigRef(void);
extern Oid *PgCurrentTSCurrentConfigCacheRef(void);
extern char **PgCurrentTimeZoneStringRef(void);
extern char **PgCurrentLogTimeZoneStringRef(void);
extern pg_tz **PgCurrentSessionTimeZoneRef(void);
extern pg_tz **PgCurrentLogTimeZoneRef(void);
extern char **PgCurrentApplicationNameRef(void);
extern int *PgCurrentTcpKeepalivesIdleRef(void);
extern int *PgCurrentTcpKeepalivesIntervalRef(void);
extern int *PgCurrentTcpKeepalivesCountRef(void);
extern int *PgCurrentTcpUserTimeoutRef(void);
extern bool *PgCurrentLogDisconnectionsRef(void);
extern int *PgCurrentLogStatementRef(void);
extern int *PgCurrentPostAuthDelayRef(void);
extern char **PgCurrentRestrictNonsystemRelationKindStringRef(void);
extern int *PgCurrentRestrictNonsystemRelationKindRef(void);
extern HTAB **PgCurrentPreparedQueriesRef(void);
extern List **PgCurrentOnCommitActionsRef(void);
extern HTAB **PgCurrentSequenceHashTableRef(void);
extern struct SeqTableData **PgCurrentLastUsedSequenceRef(void);
extern HTAB **PgCurrentOperatorLookupCacheRef(void);
extern struct pg_ctype_cache **PgCurrentRegexCtypeCacheListRef(void);
extern struct RelationData **PgCurrentLargeObjectHeapRelationRef(void);
extern struct RelationData **PgCurrentLargeObjectIndexRelationRef(void);
extern HTAB **PgCurrentAsyncLocalChannelTableRef(void);
extern bool *PgCurrentAsyncRegisteredListenerRef(void);
extern List **PgCurrentEncodingConvProcListRef(void);
extern FmgrInfo **PgCurrentToServerConvProcRef(void);
extern FmgrInfo **PgCurrentToClientConvProcRef(void);
extern FmgrInfo **PgCurrentUtf8ToServerConvProcRef(void);
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
extern char **PgCurrentLocaleMessagesRef(void);
extern char **PgCurrentLocaleMonetaryRef(void);
extern char **PgCurrentLocaleNumericRef(void);
extern char **PgCurrentLocaleTimeRef(void);
extern int *PgCurrentIcuValidationLevelRef(void);
extern PgSessionUserIdentityState *PgCurrentUserIdentityState(void);
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
extern WaitEventSet **PgCurrentLatchWaitSetRef(void);
extern Latch *PgCurrentLocalLatchData(void);
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
extern TransactionId *PgCurrentCachedFetchXidRef(void);
extern int *PgCurrentCachedFetchXidStatusRef(void);
extern XLogRecPtr *PgCurrentCachedCommitLSNRef(void);
extern void **PgCurrentTwoPhaseLockedGxactRef(void);
extern bool *PgCurrentTwoPhaseExitRegisteredRef(void);
extern FullTransactionId *PgCurrentTwoPhaseCachedFxidRef(void);
extern void **PgCurrentTwoPhaseCachedGxactRef(void);
extern int *PgCurrentSlruErrorCauseRef(void);
extern int *PgCurrentSlruErrnoRef(void);
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
extern void **PgCurrentBlockingAutovacuumProcRef(void);
extern HTAB **PgCurrentLocalPredicateLockHashRef(void);
extern void **PgCurrentMySerializableXactRef(void);
extern bool *PgCurrentMyXactDidWriteRef(void);
extern void **PgCurrentSavedSerializableXactRef(void);

extern void InitializePgProcessRuntime(void);
extern void InitializePgThreadRuntime(PgBackendExitContinuation exit_backend);
extern void InitializePgThreadBackendRuntimeState(PgThreadBackendRuntimeState *state,
												 BackendType backend_type,
												 struct Port *port,
												 struct Latch *interrupt_latch);
extern void InstallPgThreadBackendRuntimeState(PgThreadBackendRuntimeState *state);
extern void InitializePgThreadBackendRuntime(PgThreadBackendRuntimeState *state,
											 BackendType backend_type,
											 struct Port *port,
											 struct Latch *interrupt_latch);
extern void PgSetCurrentSession(PgSession *session);
extern bool PgCurrentSessionOwnsPointer(const void *ptr);
extern Session *PgSessionGetLegacySession(PgSession *session);
extern void PgSessionSetLegacySession(PgSession *session,
									   Session *legacy_session);
extern Session *PgCurrentLegacySession(void);
extern struct Port **PgConnectionProcPortRef(PgConnection *connection);
extern struct Port **PgCurrentProcPortRef(void);
extern uint8 *PgConnectionCancelKey(PgConnection *connection);
extern uint8 *PgCurrentCancelKey(void);
extern int *PgConnectionCancelKeyLengthRef(PgConnection *connection);
extern int *PgCurrentCancelKeyLengthRef(void);
extern const char **PgExecutionDebugQueryStringRef(PgExecution *execution);
extern const char **PgCurrentDebugQueryStringRef(void);
extern PgConnectionSocketIOState *PgConnectionSocketIORef(PgConnection *connection);
extern PgConnectionSocketIOState *PgCurrentConnectionSocketIORef(void);
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
extern PgConnectionSecurityState *PgConnectionSecurityStateRef(PgConnection *connection);
extern PgConnectionSecurityState *PgCurrentConnectionSecurityStateRef(void);
extern PgBackendLaunchModel PgRuntimeGetBackendLaunchModel(BackendType backend_type);
extern bool PgRuntimeShouldThreadBackend(BackendType backend_type);
extern PgBackendModel PgRuntimeGetExtensionBackendModel(void);
extern void PgRuntimeSetExtensionBackendModel(PgBackendModel backend_model);
extern void PgBackendInitializeInterrupts(PgBackend *backend);
extern void PgBackendSetInterruptLatch(PgBackend *backend,
										struct Latch *interrupt_latch);
extern PgBackendId PgBackendGetId(PgBackend *backend);
extern PgBackendId PgCurrentBackendId(void);
extern int	PgBackendGetSignalPid(PgBackend *backend);
extern int	PgCurrentBackendSignalPid(void);
extern bool PgBackendUsesProcessSignals(PgBackend *backend);
extern void PgBackendWakeup(PgBackend *backend);
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
extern int	PgSuspend(const PgWaitSpec *wait_spec,
					  PgSuspendCallback callback, void *callback_arg);
extern PgStepResult PgSessionStep(PgSession *session, PgStepBudget budget);
pg_noreturn extern void PgSessionRun(PgSession *session);

#endif							/* BACKEND_RUNTIME_H */
