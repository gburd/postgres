/*-------------------------------------------------------------------------
 *
 * backend_runtime_buffer.c
 *	  Runtime bridge accessors for buffer state.
 *
 * These accessors keep buffer-manager compatibility globals mapped onto the
 * current runtime objects while leaving runtime construction and early
 * fallback ownership in utils/init/backend_runtime.c.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/backend/storage/buffer/backend_runtime_buffer.c
 *
 *-------------------------------------------------------------------------
 */
#define BACKEND_RUNTIME_NO_INLINE_BUCKET_ACCESSORS
#include "postgres.h"

#include "storage/buf_internals.h"
#include "utils/memutils.h"
#include "../../utils/init/backend_runtime_internal.h"

static inline PgBackendBufferState *
PgCurrentBackendBufferStateFast(void)
{
	if (likely(CurrentPgBackendBufferRuntimeState != NULL))
		return CurrentPgBackendBufferRuntimeState;

	return PgCurrentBackendBufferState();
}

bool *
PgCurrentZeroDamagedPagesRef(void)
{
	return &PgCurrentSessionBufferIOState()->zero_damaged_pages_value;
}

bool *
PgCurrentTrackIOTimingRef(void)
{
	return &PgCurrentSessionBufferIOState()->track_io_timing_value;
}

int *
PgCurrentEffectiveIOConcurrencyRef(void)
{
	return &PgCurrentSessionBufferIOState()->effective_io_concurrency_value;
}

int *
PgCurrentMaintenanceIOConcurrencyRef(void)
{
	return &PgCurrentSessionBufferIOState()->maintenance_io_concurrency_value;
}

int *
PgCurrentIOCombineLimitRef(void)
{
	return &PgCurrentSessionBufferIOState()->io_combine_limit_value;
}

int *
PgCurrentIOCombineLimitGUCRef(void)
{
	return &PgCurrentSessionBufferIOState()->io_combine_limit_guc_value;
}

int *
PgCurrentBackendFlushAfterRef(void)
{
	return &PgCurrentSessionBufferIOState()->backend_flush_after_value;
}

int *
PgCurrentNLocBufferRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendBufferRuntimeState, PgCurrentBackendBufferState)->nlocbuffer;
}

void **
PgCurrentLocalBufferDescriptorsRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendBufferRuntimeState, PgCurrentBackendBufferState)->local_buffer_descriptors;
}

void **
PgCurrentLocalBufferBlockPointersRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendBufferRuntimeState, PgCurrentBackendBufferState)->local_buffer_block_pointers;
}

int32 **
PgCurrentLocalRefCountRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendBufferRuntimeState, PgCurrentBackendBufferState)->local_ref_count;
}

int *
PgCurrentNextFreeLocalBufIdRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendBufferRuntimeState, PgCurrentBackendBufferState)->next_free_local_buf_id;
}

HTAB **
PgCurrentLocalBufHashRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendBufferRuntimeState, PgCurrentBackendBufferState)->local_buf_hash;
}

int *
PgCurrentNLocalPinnedBuffersRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendBufferRuntimeState, PgCurrentBackendBufferState)->n_local_pinned_buffers;
}

char **
PgCurrentLocalBufferCurBlockRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendBufferRuntimeState, PgCurrentBackendBufferState)->local_buffer_cur_block;
}

int *
PgCurrentLocalBufferNextBufInBlockRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendBufferRuntimeState, PgCurrentBackendBufferState)->local_buffer_next_buf_in_block;
}

int *
PgCurrentLocalBufferNumBufsInBlockRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendBufferRuntimeState, PgCurrentBackendBufferState)->local_buffer_num_bufs_in_block;
}

int *
PgCurrentLocalBufferTotalBufsAllocatedRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendBufferRuntimeState, PgCurrentBackendBufferState)->local_buffer_total_bufs_allocated;
}

MemoryContext *
PgCurrentLocalBufferContextRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendBufferRuntimeState, PgCurrentBackendBufferState)->local_buffer_context;
}

BufferDesc **
PgCurrentPinCountWaitBufRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendBufferRuntimeState, PgCurrentBackendBufferState)->pin_count_wait_buf;
}

WritebackContext *
PgCurrentBackendWritebackContextRef(void)
{
	PgBackendBufferState *buffers = PgCurrentBackendBufferState();

	if (buffers->backend_writeback_context == NULL)
		buffers->backend_writeback_context =
			MemoryContextAllocZero(PgBackendBufferAllocationContext(),
								   sizeof(WritebackContext));

	return buffers->backend_writeback_context;
}

void **
PgCurrentPrivateRefCountArrayKeysRef(void)
{
	return &PgCurrentBackendBufferStateFast()->private_ref_count_array_keys;
}

void **
PgCurrentPrivateRefCountArrayRef(void)
{
	return &PgCurrentBackendBufferStateFast()->private_ref_count_array;
}

void **
PgCurrentPrivateRefCountHashRef(void)
{
	return &PgCurrentBackendBufferStateFast()->private_ref_count_hash;
}

int32 *
PgCurrentPrivateRefCountOverflowedRef(void)
{
	return &PgCurrentBackendBufferStateFast()->private_ref_count_overflowed;
}

uint32 *
PgCurrentPrivateRefCountClockRef(void)
{
	return &PgCurrentBackendBufferStateFast()->private_ref_count_clock;
}

int *
PgCurrentReservedRefCountSlotRef(void)
{
	return &PgCurrentBackendBufferStateFast()->reserved_ref_count_slot;
}

int *
PgCurrentPrivateRefCountEntryLastRef(void)
{
	return &PgCurrentBackendBufferStateFast()->private_ref_count_entry_last;
}

uint32 *
PgCurrentMaxProportionalPinsRef(void)
{
	return &PgCurrentBackendBufferStateFast()->max_proportional_pins;
}
