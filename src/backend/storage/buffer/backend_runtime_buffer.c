/*-------------------------------------------------------------------------
 *
 * backend_runtime_buffer.c
 *	  Runtime bridge accessors for backend-local buffer state.
 *
 * These accessors keep buffer-manager compatibility globals mapped onto the
 * current PgBackend while leaving runtime construction and early fallback
 * ownership in utils/init/backend_runtime.c.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/backend/storage/buffer/backend_runtime_buffer.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "storage/buf_internals.h"
#include "utils/memutils.h"
#include "../../utils/init/backend_runtime_internal.h"

int *
PgCurrentNLocBufferRef(void)
{
	return &PgCurrentBackendBufferState()->nlocbuffer;
}

void **
PgCurrentLocalBufferDescriptorsRef(void)
{
	return &PgCurrentBackendBufferState()->local_buffer_descriptors;
}

void **
PgCurrentLocalBufferBlockPointersRef(void)
{
	return &PgCurrentBackendBufferState()->local_buffer_block_pointers;
}

int32 **
PgCurrentLocalRefCountRef(void)
{
	return &PgCurrentBackendBufferState()->local_ref_count;
}

int *
PgCurrentNextFreeLocalBufIdRef(void)
{
	return &PgCurrentBackendBufferState()->next_free_local_buf_id;
}

HTAB **
PgCurrentLocalBufHashRef(void)
{
	return &PgCurrentBackendBufferState()->local_buf_hash;
}

int *
PgCurrentNLocalPinnedBuffersRef(void)
{
	return &PgCurrentBackendBufferState()->n_local_pinned_buffers;
}

char **
PgCurrentLocalBufferCurBlockRef(void)
{
	return &PgCurrentBackendBufferState()->local_buffer_cur_block;
}

int *
PgCurrentLocalBufferNextBufInBlockRef(void)
{
	return &PgCurrentBackendBufferState()->local_buffer_next_buf_in_block;
}

int *
PgCurrentLocalBufferNumBufsInBlockRef(void)
{
	return &PgCurrentBackendBufferState()->local_buffer_num_bufs_in_block;
}

int *
PgCurrentLocalBufferTotalBufsAllocatedRef(void)
{
	return &PgCurrentBackendBufferState()->local_buffer_total_bufs_allocated;
}

MemoryContext *
PgCurrentLocalBufferContextRef(void)
{
	return &PgCurrentBackendBufferState()->local_buffer_context;
}

BufferDesc **
PgCurrentPinCountWaitBufRef(void)
{
	return &PgCurrentBackendBufferState()->pin_count_wait_buf;
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
	return &PgCurrentBackendBufferState()->private_ref_count_array_keys;
}

void **
PgCurrentPrivateRefCountArrayRef(void)
{
	return &PgCurrentBackendBufferState()->private_ref_count_array;
}

void **
PgCurrentPrivateRefCountHashRef(void)
{
	return &PgCurrentBackendBufferState()->private_ref_count_hash;
}

int32 *
PgCurrentPrivateRefCountOverflowedRef(void)
{
	return &PgCurrentBackendBufferState()->private_ref_count_overflowed;
}

uint32 *
PgCurrentPrivateRefCountClockRef(void)
{
	return &PgCurrentBackendBufferState()->private_ref_count_clock;
}

int *
PgCurrentReservedRefCountSlotRef(void)
{
	return &PgCurrentBackendBufferState()->reserved_ref_count_slot;
}

int *
PgCurrentPrivateRefCountEntryLastRef(void)
{
	return &PgCurrentBackendBufferState()->private_ref_count_entry_last;
}

uint32 *
PgCurrentMaxProportionalPinsRef(void)
{
	return &PgCurrentBackendBufferState()->max_proportional_pins;
}
