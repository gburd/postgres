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
#define BACKEND_RUNTIME_NO_INLINE_BUCKET_ACCESSORS
#include "postgres.h"

#include "storage/latch.h"
#include "storage/waiteventset.h"
#include "../../utils/init/backend_runtime_internal.h"

static inline PgCarrier *
PgCurrentCarrierStateFast(void)
{
	PgCarrier  *carrier;

	carrier = PgRuntimeCurrentBridgeState.carrier;
	if (likely(carrier != NULL))
		return carrier;

	PG_RUNTIME_BRIDGE_COUNT_FALLBACK(carrier);
	return PgCurrentCarrierState();
}

void **
PgCurrentProcSignalSlotRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendIPCRuntimeState, PgCurrentBackendIPCState)->proc_signal_slot;
}

uint64 *
PgCurrentSharedInvalidMessageCounterRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendIPCRuntimeState, PgCurrentBackendIPCState)->shared_invalid_message_counter;
}

volatile sig_atomic_t *
PgCurrentCatchupInterruptPendingRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendIPCRuntimeState, PgCurrentBackendIPCState)->catchup_interrupt_pending;
}

void **
PgCurrentSharedInvalidationMessagesRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendIPCRuntimeState, PgCurrentBackendIPCState)->shared_invalidation_messages;
}

volatile int *
PgCurrentSharedInvalidationNextMsgRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendIPCRuntimeState, PgCurrentBackendIPCState)->shared_invalidation_next_msg;
}

volatile int *
PgCurrentSharedInvalidationNumMsgsRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendIPCRuntimeState, PgCurrentBackendIPCState)->shared_invalidation_num_msgs;
}

bool *
PgCurrentDsmInitDoneRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendIPCRuntimeState, PgCurrentBackendIPCState)->dsm_init_done;
}

void **
PgCurrentDsmRegistryDsaRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendIPCRuntimeState, PgCurrentBackendIPCState)->dsm_registry_dsa;
}

void **
PgCurrentDsmRegistryTableRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendIPCRuntimeState, PgCurrentBackendIPCState)->dsm_registry_table;
}

LocalTransactionId *
PgCurrentNextLocalTransactionIdRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendIPCRuntimeState, PgCurrentBackendIPCState)->next_local_transaction_id;
}

WaitEventSet **
PgCurrentLatchWaitSetRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendIPCRuntimeState, PgCurrentBackendIPCState)->latch_wait_set;
}

Latch *
PgCurrentLocalLatchData(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendIPCRuntimeState, PgCurrentBackendIPCState)->local_latch_data;
}

uint32 **
PgCurrentMyWaitEventInfoRef(void)
{
	PgBackendWaitState *wait_state = CurrentPgBackendWaitRuntimeState;

	if (unlikely(wait_state == NULL || wait_state->wait_event_info_ptr == NULL))
	{
		PG_RUNTIME_BRIDGE_COUNT_FALLBACK(fast_initialized_bucket);
		wait_state = PgCurrentBackendWaitState();
	}

	return &wait_state->wait_event_info_ptr;
}

uint32 *
PgCurrentLocalWaitEventInfoRef(void)
{
	PgBackendWaitState *wait_state = CurrentPgBackendWaitRuntimeState;

	if (unlikely(wait_state == NULL || wait_state->wait_event_info_ptr == NULL))
	{
		PG_RUNTIME_BRIDGE_COUNT_FALLBACK(fast_initialized_bucket);
		wait_state = PgCurrentBackendWaitState();
	}

	return &wait_state->local_wait_event_info;
}

volatile sig_atomic_t *
PgCurrentWaitEventWaitingRef(void)
{
	return &PgCurrentCarrierStateFast()->wait_event_waiting;
}

int *
PgCurrentWaitEventSignalFdRef(void)
{
	return &PgCurrentCarrierStateFast()->wait_event_signal_fd;
}

int *
PgCurrentWaitEventSelfPipeReadFdRef(void)
{
	return &PgCurrentCarrierStateFast()->wait_event_selfpipe_readfd;
}

int *
PgCurrentWaitEventSelfPipeWriteFdRef(void)
{
	return &PgCurrentCarrierStateFast()->wait_event_selfpipe_writefd;
}

int *
PgCurrentWaitEventSelfPipeOwnerPidRef(void)
{
	return &PgCurrentCarrierStateFast()->wait_event_selfpipe_owner_pid;
}
