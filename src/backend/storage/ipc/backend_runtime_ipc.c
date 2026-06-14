/*-------------------------------------------------------------------------
 *
 * backend_runtime_ipc.c
 *	  Runtime bridge accessors for backend-local IPC state.
 *
 * These accessors keep IPC, sinval, DSM, and latch compatibility globals
 * mapped onto the current PgBackend while leaving runtime construction and
 * early fallback ownership in utils/init/backend_runtime.c.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/backend/storage/ipc/backend_runtime_ipc.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "storage/latch.h"
#include "storage/waiteventset.h"
#include "../../utils/init/backend_runtime_internal.h"

void **
PgCurrentProcSignalSlotRef(void)
{
	return &PgCurrentBackendIPCState()->proc_signal_slot;
}

uint64 *
PgCurrentSharedInvalidMessageCounterRef(void)
{
	return &PgCurrentBackendIPCState()->shared_invalid_message_counter;
}

volatile sig_atomic_t *
PgCurrentCatchupInterruptPendingRef(void)
{
	return &PgCurrentBackendIPCState()->catchup_interrupt_pending;
}

void **
PgCurrentSharedInvalidationMessagesRef(void)
{
	return &PgCurrentBackendIPCState()->shared_invalidation_messages;
}

volatile int *
PgCurrentSharedInvalidationNextMsgRef(void)
{
	return &PgCurrentBackendIPCState()->shared_invalidation_next_msg;
}

volatile int *
PgCurrentSharedInvalidationNumMsgsRef(void)
{
	return &PgCurrentBackendIPCState()->shared_invalidation_num_msgs;
}

bool *
PgCurrentDsmInitDoneRef(void)
{
	return &PgCurrentBackendIPCState()->dsm_init_done;
}

void **
PgCurrentDsmRegistryDsaRef(void)
{
	return &PgCurrentBackendIPCState()->dsm_registry_dsa;
}

void **
PgCurrentDsmRegistryTableRef(void)
{
	return &PgCurrentBackendIPCState()->dsm_registry_table;
}

LocalTransactionId *
PgCurrentNextLocalTransactionIdRef(void)
{
	return &PgCurrentBackendIPCState()->next_local_transaction_id;
}

WaitEventSet **
PgCurrentLatchWaitSetRef(void)
{
	return &PgCurrentBackendIPCState()->latch_wait_set;
}

Latch *
PgCurrentLocalLatchData(void)
{
	return &PgCurrentBackendIPCState()->local_latch_data;
}

volatile sig_atomic_t *
PgCurrentWaitEventWaitingRef(void)
{
	return &PgCurrentCarrierState()->wait_event_waiting;
}

int *
PgCurrentWaitEventSignalFdRef(void)
{
	return &PgCurrentCarrierState()->wait_event_signal_fd;
}

int *
PgCurrentWaitEventSelfPipeReadFdRef(void)
{
	return &PgCurrentCarrierState()->wait_event_selfpipe_readfd;
}

int *
PgCurrentWaitEventSelfPipeWriteFdRef(void)
{
	return &PgCurrentCarrierState()->wait_event_selfpipe_writefd;
}

int *
PgCurrentWaitEventSelfPipeOwnerPidRef(void)
{
	return &PgCurrentCarrierState()->wait_event_selfpipe_owner_pid;
}
