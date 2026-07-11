/*-------------------------------------------------------------------------
 *
 * pmchild.c
 *	  Functions for keeping track of postmaster child processes.
 *
 * Postmaster keeps track of all child processes so that when a process exits,
 * it knows what kind of a process it was and can clean up accordingly.  Every
 * child process is allocated a PMChild struct from a fixed pool of structs.
 * The size of the pool is determined by various settings that configure how
 * many worker processes and backend connections are allowed, i.e.
 * autovacuum_worker_slots, max_worker_processes, max_wal_senders, and
 * max_connections.
 *
 * Dead-end backends are handled slightly differently.  There is no limit
 * on the number of dead-end backends, and they do not need unique IDs, so
 * their PMChild structs are allocated dynamically, not from a pool.
 *
 * The structures and functions in this file are private to the postmaster
 * process.  But note that there is an array in shared memory, managed by
 * pmsignal.c, that mirrors this.
 *
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/postmaster/pmchild.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "miscadmin.h"
#include "postmaster/autovacuum.h"
#include "postmaster/postmaster.h"
#include "replication/walsender.h"
#include "storage/latch.h"
#include "storage/pmsignal.h"
#include "storage/proc.h"
#include "utils/backend_runtime.h"

#ifndef WIN32
#include "port/pg_pthread.h"
#endif

/*
 * Freelists for different kinds of child processes.  We maintain separate
 * pools for each, so that for example launching a lot of regular backends
 * cannot prevent autovacuum or an aux process from launching.
 */
typedef struct PMChildPool
{
	int			size;			/* number of PMChild slots reserved for this
								 * kind of processes */
	int			first_slotno;	/* first slot belonging to this pool */
	dlist_head	freelist;		/* currently unused PMChild entries */
} PMChildPool;

static PG_GLOBAL_RUNTIME PMChildPool pmchild_pools[BACKEND_NUM_TYPES];
PG_GLOBAL_RUNTIME NON_EXEC_STATIC int num_pmchild_slots = 0;

#ifndef WIN32
static PG_GLOBAL_RUNTIME pthread_mutex_t PMChildLogicalBackendMutex = PTHREAD_MUTEX_INITIALIZER;
#endif

static void PMChildLogicalBackendLock(void);
static void PMChildLogicalBackendUnlock(void);
static void PMChildResetLogicalPublicationState(PMChild *pmchild,
												pid_t logical_signal_pid);
static void PostmasterChildPublishExit(PMChild *pmchild, int exitstatus,
									   Size top_memory_allocated,
									   Size top_memory_reclaimed,
									   Latch *postmaster_latch);
static bool PostmasterChildHasExited(PMChild *pmchild, int *exitstatus,
									 Size *top_memory_allocated,
									 Size *top_memory_reclaimed,
									 pid_t *signal_pid);
static void PostmasterChildWakePostmaster(Latch *postmaster_latch);

/*
 * Thread-backed PMChild ownership contract:
 *
 * - ActiveChildList membership, slot assignment/release, carrier_kind, bkend
 *   type, bgworker metadata, and native-thread join are owned by the
 *   postmaster main thread.
 * - logical_backend, logical_signal_pid, and thread-exit payload fields are
 *   the cross-thread publication surface between a logical backend and the
 *   postmaster.  They must be read or written only by the helper APIs in this
 *   file while holding PMChildLogicalBackendMutex.
 * - thread_startup_complete and thread_exited are publication flags.  The
 *   publishing side writes payload first, issues a memory barrier, then sets
 *   the flag and wakes the postmaster.
 */

/*
 * List of active child processes.  This includes dead-end children.
 */
PG_GLOBAL_RUNTIME dlist_head ActiveChildList;

/*
 * Dummy pointer to persuade Valgrind that we've not leaked the array of
 * PMChild structs.  Make it global to ensure the compiler doesn't
 * optimize it away.
 */
#ifdef USE_VALGRIND
extern PG_GLOBAL_RUNTIME PMChild *pmchild_array;
PG_GLOBAL_RUNTIME PMChild *pmchild_array;
#endif

static void
PMChildLogicalBackendLock(void)
{
#ifndef WIN32
	int			rc;

	rc = pthread_mutex_lock(&PMChildLogicalBackendMutex);
	if (rc != 0)
	{
		errno = rc;
		elog(FATAL, "could not lock PMChild logical-backend state: %m");
	}
#endif
}

static void
PMChildLogicalBackendUnlock(void)
{
#ifndef WIN32
	int			rc;

	rc = pthread_mutex_unlock(&PMChildLogicalBackendMutex);
	if (rc != 0)
	{
		errno = rc;
		elog(FATAL, "could not unlock PMChild logical-backend state: %m");
	}
#endif
}

static void
PMChildResetLogicalPublicationState(PMChild *pmchild, pid_t logical_signal_pid)
{
	PMChildLogicalBackendLock();
	pmchild->logical_signal_pid = logical_signal_pid;
	pmchild->logical_backend = NULL;
	pmchild->thread_exitstatus = 0;
	pmchild->thread_exit_logical_signal_pid = 0;
	pmchild->thread_exit_top_memory_allocated = 0;
	pmchild->thread_exit_top_memory_reclaimed = 0;
	PMChildLogicalBackendUnlock();

	pg_atomic_write_u32(&pmchild->thread_startup_complete, 0);
	pg_atomic_write_u32(&pmchild->thread_exited, 0);
}


/*
 * MaxLivePostmasterChildren
 *
 * This reports the number of postmaster child processes that can be active.
 * It includes all children except for dead-end children.  This allows the
 * array in shared memory (PMChildFlags) to have a fixed maximum size.
 */
int
MaxLivePostmasterChildren(void)
{
	if (num_pmchild_slots == 0)
		elog(ERROR, "PM child array not initialized yet");
	return num_pmchild_slots;
}

/*
 * Initialize at postmaster startup
 *
 * Note: This is not called on crash restart.  We rely on PMChild entries to
 * remain valid through the restart process.  This is important because the
 * syslogger survives through the crash restart process, so we must not
 * invalidate its PMChild slot.
 */
void
InitPostmasterChildSlots(void)
{
	int			slotno;
	PMChild    *slots;

	/*
	 * We allow more connections here than we can have backends because some
	 * might still be authenticating; they might fail auth, or some existing
	 * backend might exit before the auth cycle is completed.  The exact
	 * MaxConnections limit is enforced when a new backend tries to join the
	 * PGPROC array.
	 *
	 * WAL senders start out as regular backends, so they share the same pool.
	 */
	pmchild_pools[B_BACKEND].size = 2 * (MaxConnections + max_wal_senders);

	pmchild_pools[B_AUTOVAC_WORKER].size = autovacuum_worker_slots;
	pmchild_pools[B_BG_WORKER].size = max_worker_processes;
	pmchild_pools[B_IO_WORKER].size = MAX_IO_WORKERS;

	/*
	 * There can be only one of each of these running at a time.  They each
	 * get their own pool of just one entry.
	 */
	pmchild_pools[B_AUTOVAC_LAUNCHER].size = 1;
	pmchild_pools[B_SLOTSYNC_WORKER].size = 1;
	pmchild_pools[B_ARCHIVER].size = 1;
	pmchild_pools[B_BG_WRITER].size = 1;
	pmchild_pools[B_CHECKPOINTER].size = 1;
	pmchild_pools[B_STARTUP].size = 1;
	pmchild_pools[B_WAL_RECEIVER].size = 1;
	pmchild_pools[B_WAL_SUMMARIZER].size = 1;
	pmchild_pools[B_WAL_WRITER].size = 1;
	pmchild_pools[B_LOGGER].size = 1;

	/* The rest of the pmchild_pools are left at zero size */

	/* Count the total number of slots */
	num_pmchild_slots = 0;
	for (int i = 0; i < BACKEND_NUM_TYPES; i++)
		num_pmchild_slots += pmchild_pools[i].size;

	/* Allocate enough slots, and make sure Valgrind doesn't complain */
	slots = palloc_array(PMChild, num_pmchild_slots);
#ifdef USE_VALGRIND
	pmchild_array = slots;
#endif

	/* Initialize them */
	slotno = 0;
	for (int btype = 0; btype < BACKEND_NUM_TYPES; btype++)
	{
		pmchild_pools[btype].first_slotno = slotno + 1;
		dlist_init(&pmchild_pools[btype].freelist);

		for (int j = 0; j < pmchild_pools[btype].size; j++)
		{
			slots[slotno].carrier_kind = PM_CHILD_CARRIER_PROCESS;
			slots[slotno].pid = 0;
			slots[slotno].logical_signal_pid = 0;
			slots[slotno].logical_backend = NULL;
			slots[slotno].thread_exitstatus = 0;
			slots[slotno].thread_exit_logical_signal_pid = 0;
			slots[slotno].thread_exit_top_memory_allocated = 0;
			slots[slotno].thread_exit_top_memory_reclaimed = 0;
			pg_atomic_init_u32(&slots[slotno].thread_startup_complete, 0);
			pg_atomic_init_u32(&slots[slotno].thread_exited, 0);
			slots[slotno].child_slot = slotno + 1;
			slots[slotno].bkend_type = B_INVALID;
			slots[slotno].rw = NULL;
			slots[slotno].bgworker_notify = false;
			dlist_push_tail(&pmchild_pools[btype].freelist, &slots[slotno].elem);
			slotno++;
		}
	}
	Assert(slotno == num_pmchild_slots);

	/* Initialize other structures */
	dlist_init(&ActiveChildList);
}

/*
 * Allocate a PMChild entry for a postmaster child process of given type.
 *
 * The entry is taken from the right pool for the type.
 *
 * pmchild->child_slot in the returned struct is unique among all active child
 * processes.
 */
PMChild *
AssignPostmasterChildSlot(BackendType btype)
{
	dlist_head *freelist;
	PMChild    *pmchild;

	if (pmchild_pools[btype].size == 0)
		elog(ERROR, "cannot allocate a PMChild slot for backend type %d", btype);

	freelist = &pmchild_pools[btype].freelist;
	if (dlist_is_empty(freelist))
		return NULL;

	pmchild = dlist_container(PMChild, elem, dlist_pop_head_node(freelist));
	pmchild->carrier_kind = PM_CHILD_CARRIER_PROCESS;
	pmchild->pid = 0;
	PMChildResetLogicalPublicationState(pmchild, 0);
	pmchild->bkend_type = btype;
	pmchild->rw = NULL;
	pmchild->bgworker_notify = true;
	pmchild->carrier_orphan_start = NULL;

	/*
	 * pmchild->child_slot for each entry was initialized when the array of
	 * slots was allocated.  Sanity check it.
	 */
	if (!(pmchild->child_slot >= pmchild_pools[btype].first_slotno &&
		  pmchild->child_slot < pmchild_pools[btype].first_slotno + pmchild_pools[btype].size))
	{
		elog(ERROR, "pmchild freelist for backend type %d is corrupt",
			 pmchild->bkend_type);
	}

	dlist_push_head(&ActiveChildList, &pmchild->elem);

	/* Update the status in the shared memory array */
	MarkPostmasterChildSlotAssigned(pmchild->child_slot);

	elog(DEBUG2, "assigned pm child slot %d for %s",
		 pmchild->child_slot, PostmasterChildName(btype));

	return pmchild;
}

/*
 * Allocate a PMChild struct for a dead-end backend.  Dead-end children are
 * not assigned a child_slot number.  The struct is palloc'd; returns NULL if
 * out of memory.
 */
PMChild *
AllocDeadEndChild(void)
{
	PMChild    *pmchild;

	elog(DEBUG2, "allocating dead-end child");

	pmchild = (PMChild *) palloc_extended(sizeof(PMChild), MCXT_ALLOC_NO_OOM);
	if (pmchild)
	{
		pmchild->carrier_kind = PM_CHILD_CARRIER_PROCESS;
		pmchild->pid = 0;
		pmchild->logical_signal_pid = 0;
		pmchild->logical_backend = NULL;
		pmchild->thread_exitstatus = 0;
		pmchild->thread_exit_logical_signal_pid = 0;
		pmchild->thread_exit_top_memory_allocated = 0;
		pmchild->thread_exit_top_memory_reclaimed = 0;
		pg_atomic_init_u32(&pmchild->thread_startup_complete, 0);
		pg_atomic_init_u32(&pmchild->thread_exited, 0);
		pmchild->child_slot = 0;
		pmchild->bkend_type = B_DEAD_END_BACKEND;
		pmchild->rw = NULL;
		pmchild->bgworker_notify = false;

		dlist_push_head(&ActiveChildList, &pmchild->elem);
	}

	return pmchild;
}

bool
PostmasterChildIsProcess(const PMChild *pmchild)
{
	return pmchild->carrier_kind == PM_CHILD_CARRIER_PROCESS;
}

bool
PostmasterChildIsThread(const PMChild *pmchild)
{
	return pmchild->carrier_kind == PM_CHILD_CARRIER_THREAD;
}

bool
PostmasterChildIsPooledLogical(const PMChild *pmchild)
{
	return pmchild->carrier_kind == PM_CHILD_CARRIER_POOLED_LOGICAL;
}

bool
PostmasterChildHasLogicalBackendPublication(const PMChild *pmchild)
{
	return PostmasterChildIsThread(pmchild) ||
		PostmasterChildIsPooledLogical(pmchild);
}

pid_t
PostmasterChildSignalPid(const PMChild *pmchild)
{
	pid_t		signal_pid;

	Assert(pmchild != NULL);

	if (!PostmasterChildHasLogicalBackendPublication(pmchild))
		return pmchild->pid;

	PMChildLogicalBackendLock();
	signal_pid = pmchild->logical_signal_pid;
	PMChildLogicalBackendUnlock();
	return signal_pid;
}

void
PostmasterChildSetProcess(PMChild *pmchild, pid_t pid)
{
	Assert(pid > 0);

	pmchild->carrier_kind = PM_CHILD_CARRIER_PROCESS;
	pmchild->pid = pid;
	PMChildResetLogicalPublicationState(pmchild, pid);
}

void
PostmasterChildSetThread(PMChild *pmchild, const PgThread *thread)
{
	Assert(thread != NULL);

	pmchild->carrier_kind = PM_CHILD_CARRIER_THREAD;
	pmchild->pid = 0;
	pmchild->thread = *thread;
	PMChildResetLogicalPublicationState(pmchild, 0);
}

void
PostmasterChildSetPooledLogical(PMChild *pmchild)
{
	pmchild->carrier_kind = PM_CHILD_CARRIER_POOLED_LOGICAL;
	pmchild->pid = 0;
	PMChildResetLogicalPublicationState(pmchild, 0);
}

void
PostmasterChildPublishLogicalBackend(PMChild *pmchild, struct PgBackend *backend)
{
	Assert(PostmasterChildHasLogicalBackendPublication(pmchild));

	PMChildLogicalBackendLock();
	pmchild->logical_backend = backend;
	if (backend != NULL)
		pmchild->logical_signal_pid = PgBackendGetSignalPid(backend);
	else
		pmchild->logical_signal_pid = 0;
	PMChildLogicalBackendUnlock();
}

void
PostmasterChildUnpublishLogicalBackend(PMChild *pmchild)
{
	Assert(PostmasterChildHasLogicalBackendPublication(pmchild));

	PMChildLogicalBackendLock();
	if (PostmasterChildIsThread(pmchild))
		pmchild->thread_exit_logical_signal_pid = pmchild->logical_signal_pid;
	pmchild->logical_backend = NULL;
	pmchild->logical_signal_pid = 0;
	PMChildLogicalBackendUnlock();
}

bool
PostmasterChildRaiseThreadInterrupt(PMChild *pmchild,
									int interrupt)
{
	bool		raised = false;

	Assert(PostmasterChildHasLogicalBackendPublication(pmchild));

	PMChildLogicalBackendLock();
	if (pmchild->logical_backend != NULL)
	{
		SendInterrupt(pmchild->logical_backend, interrupt);
		raised = true;
	}
	PMChildLogicalBackendUnlock();

	return raised;
}

bool
PostmasterChildWakeThreadBackend(PMChild *pmchild)
{
	bool		woke = false;

	Assert(PostmasterChildHasLogicalBackendPublication(pmchild));

	PMChildLogicalBackendLock();
	if (pmchild->logical_backend != NULL)
	{
		PgBackendWakeup(pmchild->logical_backend);
		woke = true;
	}
	PMChildLogicalBackendUnlock();

	return woke;
}

static void
PostmasterChildWakePostmaster(Latch *postmaster_latch)
{
	if (postmaster_latch != NULL)
		SetLatch(postmaster_latch);
	else
		PostmasterSignalPMSignal();
}

void
PostmasterChildPublishLogicalStartupComplete(PMChild *pmchild,
											 Latch *postmaster_latch)
{
	Assert(PostmasterChildHasLogicalBackendPublication(pmchild));

	pg_memory_barrier();
	pg_atomic_write_u32(&pmchild->thread_startup_complete, 1);
	PostmasterChildWakePostmaster(postmaster_latch);
}

void
PostmasterChildPublishThreadStartupComplete(PMChild *pmchild,
											Latch *postmaster_latch)
{
	Assert(PostmasterChildIsThread(pmchild));

	PostmasterChildPublishLogicalStartupComplete(pmchild, postmaster_latch);
}

bool
PostmasterChildHasStartupComplete(PMChild *pmchild)
{
	if (!PostmasterChildHasLogicalBackendPublication(pmchild))
		return false;

	return pg_atomic_exchange_u32(&pmchild->thread_startup_complete, 0) != 0;
}

static void
PostmasterChildPublishExit(PMChild *pmchild, int exitstatus,
						   Size top_memory_allocated,
						   Size top_memory_reclaimed,
						   Latch *postmaster_latch)
{
	Assert(PostmasterChildHasLogicalBackendPublication(pmchild));

	/*
	 * Logical exit publication owns the handoff from the exiting backend to
	 * the postmaster.  Clear the volatile logical-backend pointer under the
	 * same lock used by signal/wakeup delivery before making the exited flag
	 * visible, so later postmaster signal routing cannot race with teardown.
	 */
	PMChildLogicalBackendLock();
	if (pmchild->logical_backend != NULL || pmchild->logical_signal_pid != 0)
	{
		pmchild->thread_exit_logical_signal_pid =
			pmchild->logical_signal_pid;
		pmchild->logical_backend = NULL;
		pmchild->logical_signal_pid = 0;
	}
	pmchild->thread_exitstatus = exitstatus;
	pmchild->thread_exit_top_memory_allocated = top_memory_allocated;
	pmchild->thread_exit_top_memory_reclaimed = top_memory_reclaimed;
	PMChildLogicalBackendUnlock();

	/*
	 * Publish the exit status before waking the postmaster.  The postmaster
	 * owns PMChild list mutation and slot release.
	 */
	pg_memory_barrier();
	pg_atomic_write_u32(&pmchild->thread_exited, 1);
	PostmasterChildWakePostmaster(postmaster_latch);
}

void
PostmasterChildPublishPooledLogicalExit(PMChild *pmchild, int exitstatus,
										Size top_memory_allocated,
										Size top_memory_reclaimed,
										Latch *postmaster_latch)
{
	Assert(PostmasterChildIsPooledLogical(pmchild));

	PostmasterChildPublishExit(pmchild, exitstatus, top_memory_allocated,
							   top_memory_reclaimed, postmaster_latch);
}

void
PostmasterChildPublishThreadExit(PMChild *pmchild, int exitstatus,
								 Size top_memory_allocated,
								 Size top_memory_reclaimed,
								 Latch *postmaster_latch)
{
	Assert(PostmasterChildIsThread(pmchild));

	PostmasterChildPublishExit(pmchild, exitstatus, top_memory_allocated,
							   top_memory_reclaimed, postmaster_latch);
}

static bool
PostmasterChildHasExited(PMChild *pmchild, int *exitstatus,
						 Size *top_memory_allocated,
						 Size *top_memory_reclaimed,
						 pid_t *signal_pid)
{
	if (pg_atomic_exchange_u32(&pmchild->thread_exited, 0) == 0)
		return false;

	PMChildLogicalBackendLock();
	*exitstatus = pmchild->thread_exitstatus;
	if (top_memory_allocated != NULL)
		*top_memory_allocated = pmchild->thread_exit_top_memory_allocated;
	if (top_memory_reclaimed != NULL)
		*top_memory_reclaimed = pmchild->thread_exit_top_memory_reclaimed;
	if (signal_pid != NULL)
		*signal_pid = pmchild->thread_exit_logical_signal_pid;
	PMChildLogicalBackendUnlock();

	return true;
}

bool
PostmasterChildHasExitedPooledLogical(PMChild *pmchild, int *exitstatus,
									  Size *top_memory_allocated,
									  Size *top_memory_reclaimed,
									  pid_t *signal_pid)
{
	if (!PostmasterChildIsPooledLogical(pmchild))
		return false;

	return PostmasterChildHasExited(pmchild, exitstatus, top_memory_allocated,
									top_memory_reclaimed, signal_pid);
}

bool
PostmasterChildHasExitedThread(PMChild *pmchild, int *exitstatus,
							   Size *top_memory_allocated,
							   Size *top_memory_reclaimed,
							   pid_t *signal_pid)
{
	if (!PostmasterChildIsThread(pmchild))
		return false;

	return PostmasterChildHasExited(pmchild, exitstatus, top_memory_allocated,
									top_memory_reclaimed, signal_pid);
}

void
PostmasterChildRetryThreadExit(PMChild *pmchild)
{
	Assert(PostmasterChildIsThread(pmchild));

	/*
	 * The postmaster claims a thread-exit report before joining the native
	 * carrier.  If that join fails, keep the PMChild active and make the exit
	 * report visible again so a later postmaster loop can retry instead of
	 * releasing a slot whose carrier was not joined.
	 */
	pg_memory_barrier();
	pg_atomic_write_u32(&pmchild->thread_exited, 1);
}

int
PostmasterChildJoinThread(PMChild *pmchild)
{
	Assert(PostmasterChildIsThread(pmchild));

	/*
	 * The native thread handle is postmaster-owned: SetThread stores it before
	 * the carrier can publish startup or exit, and slot release happens only
	 * after a successful join.  Keep the join behind the PMChild API boundary
	 * so callers do not grow direct access to thread-carrier fields.
	 */
	return pg_thread_join(&pmchild->thread);
}

/*
 * Release a PMChild slot, after the child process has exited.
 *
 * Returns true if the child detached cleanly from shared memory, false
 * otherwise (see MarkPostmasterChildSlotUnassigned).
 */
bool
ReleasePostmasterChildSlot(PMChild *pmchild)
{
	dlist_delete(&pmchild->elem);
	pmchild->pid = 0;

	PMChildResetLogicalPublicationState(pmchild, 0);
	if (pmchild->bkend_type == B_DEAD_END_BACKEND)
	{
		elog(DEBUG2, "releasing dead-end backend");
		pfree(pmchild);
		return true;
	}
	else
	{
		PMChildPool *pool;

		elog(DEBUG2, "releasing pm child slot %d", pmchild->child_slot);

		/* WAL senders start out as regular backends, and share the pool */
		if (pmchild->bkend_type == B_WAL_SENDER)
			pool = &pmchild_pools[B_BACKEND];
		else
			pool = &pmchild_pools[pmchild->bkend_type];

		/* sanity check that we return the entry to the right pool */
		if (!(pmchild->child_slot >= pool->first_slotno &&
			  pmchild->child_slot < pool->first_slotno + pool->size))
		{
			elog(ERROR, "pmchild freelist for backend type %d is corrupt",
				 pmchild->bkend_type);
		}

		dlist_push_head(&pool->freelist, &pmchild->elem);
		return MarkPostmasterChildSlotUnassigned(pmchild->child_slot);
	}
}

/*
 * Find the PMChild entry of a running child process by PID.
 */
PMChild *
FindPostmasterChildByPid(int pid)
{
	dlist_iter	iter;

	dlist_foreach(iter, &ActiveChildList)
	{
		PMChild    *bp = dlist_container(PMChild, elem, iter.cur);

		if (PostmasterChildIsProcess(bp) && bp->pid == pid)
			return bp;
	}
	return NULL;
}
