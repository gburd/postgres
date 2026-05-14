/*-------------------------------------------------------------------------
 *
 * recno_hlc.c
 *	  RECNO Hybrid Logical Clock (HLC) implementation
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/recno/recno_hlc.c
 *
 * NOTES
 *	  Implements Hybrid Logical Clocks (Kulkarni et al., 2014) for RECNO's
 *	  time-based MVCC.  An HLC timestamp packs a 48-bit physical component
 *	  (milliseconds since epoch) and a 16-bit logical counter into a single
 *	  uint64.  The physical component stays close to wall-clock time while
 *	  the logical counter preserves causal ordering when events happen
 *	  within the same millisecond or when clocks jump backwards.
 *
 *	  The dual-mode MVCC wrappers at the bottom of this file bridge the
 *	  HLC and legacy timestamp code paths.  When recno_use_hlc is true,
 *	  RecnoGetCommitHLC() generates an HLC timestamp; when false, it
 *	  wraps RecnoGetCommitTimestamp() in an identity cast.  Callers that
 *	  store and compare uint64 commit timestamps need no structural
 *	  changes because HLCTimestamp is a typedef for uint64.
 *
 *	  DVV (Dotted Version Vector) support has been removed.  HLC is now
 *	  the sole clock mechanism.  Concurrent tuple locking is handled by
 *	  the sLog (recno_slog.c).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/recno.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/guc.h"
#include "utils/timestamp.h"


/* ----------------------------------------------------------------
 *					HLC Implementation Constants
 *
 * Primary constants and macros live in recno.h.
 * Only implementation-specific helpers are defined here.
 * ----------------------------------------------------------------
 */

/* HLC_MAX_LOGICAL is defined in recno.h (0xFFFF) */
#undef HLC_MAX_LOGICAL
#define HLC_MAX_LOGICAL			HLC_LOGICAL_MASK	/* 65535 */

/*
 * Maximum physical time in milliseconds that fits in 48 bits.
 * ~8,925 years from epoch -- sufficient for any reasonable use.
 */
#define HLC_MAX_PHYSICAL		((UINT64CONST(1) << HLC_PHYSICAL_BITS) - 1)

/* ----------------------------------------------------------------
 *					Shared Memory Structures
 * ----------------------------------------------------------------
 */

/*
 * Global HLC state in shared memory.
 *
 * global_hlc is updated via compare-and-swap (CAS) in HLCNow(), eliminating
 * the previous LWLock bottleneck that serialized all DML timestamp generation.
 * Diagnostic counters are updated with atomic operations (best-effort).
 */
typedef struct RecnoHLCShmemData
{
	pg_atomic_uint64 global_hlc;	/* Most recent HLC issued (CAS-updated) */
	uint16		node_id;		/* This node's replica ID */

	/* Clock drift diagnostics (lockless, best-effort) */
	pg_atomic_uint64 max_drift_ms;	/* Largest observed HLC-wall drift */
	pg_atomic_uint64 total_backward_jumps;	/* Wall clock went backward */
	pg_atomic_uint64 total_overflow_events; /* Logical counter saturated */
	uint64		max_offset_ms;	/* Configured max allowed drift (read-only) */
}			RecnoHLCShmemData;

static RecnoHLCShmemData * RecnoHLCShmem = NULL;

/* ----------------------------------------------------------------
 *					GUC Variables
 * ----------------------------------------------------------------
 */

/* Node/replica ID for this server (0 = single-node default) */
int			recno_node_id = 0;

/* Maximum expected clock offset in milliseconds (for uncertainty intervals) */
int			recno_max_clock_offset_ms = 250;

/* Whether to use HLC (true) or legacy plain timestamps (false) */
bool		recno_use_hlc = true;

/* Whether replicas should wait when encountering uncertainty windows */
bool		recno_uncertainty_wait = true;

/* ----------------------------------------------------------------
 *					Physical Time Helper
 * ----------------------------------------------------------------
 */

/*
 * Get current wall-clock time in milliseconds since PostgreSQL epoch.
 *
 * PostgreSQL's GetCurrentTimestamp() returns microseconds as TimestampTz.
 * We convert to milliseconds for the 48-bit HLC physical component.
 */
static uint64
RecnoGetPhysicalTimeMs(void)
{
	TimestampTz now = GetCurrentTimestamp();
	uint64		ms;

	/*
	 * TimestampTz is int64 microseconds from PG epoch (2000-01-01). Convert
	 * to milliseconds, clamping to 48-bit range.
	 */
	ms = (uint64) now / 1000;

	if (ms > HLC_MAX_PHYSICAL)
		ms = HLC_MAX_PHYSICAL;

	return ms;
}

/* ----------------------------------------------------------------
 *					Shared Memory Init/Size
 * ----------------------------------------------------------------
 */

Size
RecnoHLCShmemSize(void)
{
	return MAXALIGN(sizeof(RecnoHLCShmemData));
}

void
RecnoHLCShmemInit(void)
{
	bool		found;

	RecnoHLCShmem = (RecnoHLCShmemData *)
		ShmemInitStruct("RECNO HLC Data",
						RecnoHLCShmemSize(),
						&found);

	if (!found)
	{
		uint64		initial_physical;

		/* Set initial HLC from wall clock */
		initial_physical = RecnoGetPhysicalTimeMs();
		pg_atomic_init_u64(&RecnoHLCShmem->global_hlc,
						   HLC_MAKE(initial_physical, 0));

		/* Configure node ID from GUC */
		RecnoHLCShmem->node_id = (uint16) (recno_node_id & 0x0FFF);

		/* Initialize clock drift diagnostics */
		pg_atomic_init_u64(&RecnoHLCShmem->max_drift_ms, 0);
		pg_atomic_init_u64(&RecnoHLCShmem->total_backward_jumps, 0);
		pg_atomic_init_u64(&RecnoHLCShmem->total_overflow_events, 0);
		RecnoHLCShmem->max_offset_ms = (uint64) recno_max_clock_offset_ms;
	}
}

/*
 * Subsystem callback wrappers for PG_SHMEM_SUBSYSTEM infrastructure
 */
static void
RecnoHLCShmemRequest(void *arg)
{
	ShmemRequestStruct(.name = "RECNO HLC Data",
					   .size = RecnoHLCShmemSize(),
					   .ptr = (void **) &RecnoHLCShmem);
}

static void
RecnoHLCShmemInit_cb(void *arg)
{
	uint64		initial_physical;

	/* RecnoHLCShmem is already set by ShmemRequestStruct .ptr mechanism */
	Assert(RecnoHLCShmem != NULL);

	/* Set initial HLC from wall clock */
	initial_physical = RecnoGetPhysicalTimeMs();
	pg_atomic_init_u64(&RecnoHLCShmem->global_hlc,
					   HLC_MAKE(initial_physical, 0));

	/* Configure node ID from GUC */
	RecnoHLCShmem->node_id = (uint16) (recno_node_id & 0x0FFF);

	/* Initialize clock drift diagnostics */
	pg_atomic_init_u64(&RecnoHLCShmem->max_drift_ms, 0);
	pg_atomic_init_u64(&RecnoHLCShmem->total_backward_jumps, 0);
	pg_atomic_init_u64(&RecnoHLCShmem->total_overflow_events, 0);
	RecnoHLCShmem->max_offset_ms = (uint64) recno_max_clock_offset_ms;
}

const ShmemCallbacks RecnoHLCShmemCallbacks = {
	.request_fn = RecnoHLCShmemRequest,
	.init_fn = RecnoHLCShmemInit_cb,
};

/* ----------------------------------------------------------------
 *					HLC Core Operations
 * ----------------------------------------------------------------
 */

/*
 * HLCNow -- generate a new HLC timestamp.
 *
 * Implements the HLC "send/local" algorithm from Kulkarni et al.:
 *
 *   pt = physical_time()
 *   l.pt = max(l.pt, pt)
 *   if l.pt == old l.pt:
 *       l.lc += 1
 *   else:
 *       l.lc = 0
 *   return (l.pt, l.lc)
 *
 * When msg_hlc != 0, this also incorporates a received message's
 * HLC (the "receive" variant):
 *
 *   pt = physical_time()
 *   l.pt = max(l.pt, msg.pt, pt)
 *   if l.pt == old l.pt == msg.pt:
 *       l.lc = max(l.lc, msg.lc) + 1
 *   else if l.pt == old l.pt:
 *       l.lc += 1
 *   else if l.pt == msg.pt:
 *       l.lc = msg.lc + 1
 *   else:
 *       l.lc = 0
 *   return (l.pt, l.lc)
 */
HLCTimestamp
HLCNow(HLCTimestamp msg_hlc)
{
	uint64		pt;
	uint64		old_hlc;
	uint64		old_pt;
	uint64		old_lc;
	uint64		new_pt;
	uint64		new_lc;
	uint64		new_hlc;
	bool		had_overflow = false;

	if (RecnoHLCShmem == NULL)
		elog(ERROR, "RECNO HLC not initialized");

	pt = RecnoGetPhysicalTimeMs();

	/*
	 * Lock-free CAS loop.  Each iteration reads the current global HLC,
	 * computes the next value, and atomically swaps it in.  CAS failure means
	 * another backend advanced the clock concurrently — we simply retry
	 * with the updated value.  In the common case (different millisecond),
	 * CAS succeeds on the first attempt.  Under same-ms contention,
	 * convergence takes 1-3 retries because each retry sees the latest
	 * counter.
	 */
	for (;;)
	{
		old_hlc = pg_atomic_read_u64(&RecnoHLCShmem->global_hlc);
		old_pt = HLC_GET_PHYSICAL(old_hlc);
		old_lc = HLC_GET_LOGICAL(old_hlc);

		if (msg_hlc != 0)
		{
			uint64		msg_pt = HLC_GET_PHYSICAL(msg_hlc);
			uint64		msg_lc = HLC_GET_LOGICAL(msg_hlc);

			/* Receive variant */
			new_pt = Max(Max(old_pt, msg_pt), pt);

			if (new_pt == old_pt && new_pt == msg_pt)
				new_lc = Max(old_lc, msg_lc) + 1;
			else if (new_pt == old_pt)
				new_lc = old_lc + 1;
			else if (new_pt == msg_pt)
				new_lc = msg_lc + 1;
			else
				new_lc = 0;
		}
		else
		{
			/* Local/send variant */
			new_pt = Max(old_pt, pt);

			if (new_pt == old_pt)
				new_lc = old_lc + 1;
			else
				new_lc = 0;
		}

		/*
		 * Handle logical counter overflow.  Extremely unlikely (65535 events
		 * in the same millisecond), but we must be safe.
		 */
		if (new_lc > HLC_MAX_LOGICAL)
		{
			new_pt += 1;
			new_lc = 0;
			had_overflow = true;
		}

		new_hlc = HLC_MAKE(new_pt, new_lc);

		/* Attempt atomic swap; retry if another backend intervened */
		if (pg_atomic_compare_exchange_u64(&RecnoHLCShmem->global_hlc,
										   &old_hlc, new_hlc))
			break;

		/* CAS failed — old_hlc now holds the current value. Retry. */
		had_overflow = false;
	}

	/* Update diagnostic counters after successful CAS (best-effort) */
	if (had_overflow)
		pg_atomic_fetch_add_u64(&RecnoHLCShmem->total_overflow_events, 1);

	/*
	 * Clock drift diagnostics.
	 *
	 * Track how far the HLC physical component has drifted from the wall
	 * clock.  A forward drift (new_pt > pt) means events are being generated
	 * faster than real time advances, or a message HLC pushed us forward.  A
	 * backward jump (pt < old_pt) means the wall clock was adjusted backward
	 * (NTP step, VM migration).
	 */
	if (pt < old_pt)
		pg_atomic_fetch_add_u64(&RecnoHLCShmem->total_backward_jumps, 1);

	if (new_pt > pt)
	{
		uint64		drift = new_pt - pt;
		uint64		cur_max;

		/* Update max_drift_ms with lockless CAS loop */
		cur_max = pg_atomic_read_u64(&RecnoHLCShmem->max_drift_ms);
		while (drift > cur_max)
		{
			if (pg_atomic_compare_exchange_u64(&RecnoHLCShmem->max_drift_ms,
											   &cur_max, drift))
				break;
			/* cur_max updated by failed CAS; re-check */
		}

		if (drift > RecnoHLCShmem->max_offset_ms)
		{
			ereport(WARNING,
					(errmsg("HLC drift exceeds max_offset: "
							"hlc_physical=" UINT64_FORMAT
							", wall_clock=" UINT64_FORMAT
							", drift=" UINT64_FORMAT " ms",
							new_pt, pt, drift),
					 errhint("Check NTP synchronization or increase "
							 "recno_max_clock_offset_ms.")));
		}
	}

	return new_hlc;
}

/*
 * HLCCompare -- compare two HLC timestamps.
 *
 * Returns negative if a < b, zero if a == b, positive if a > b.
 * Since HLC is packed as (physical << 16 | logical), simple uint64
 * comparison gives the correct total order.
 */
int
HLCCompare(HLCTimestamp a, HLCTimestamp b)
{
	if (a < b)
		return -1;
	if (a > b)
		return 1;
	return 0;
}

/*
 * HLCGetPhysical -- extract the physical component (milliseconds).
 */
uint64
HLCGetPhysical(HLCTimestamp hlc)
{
	return HLC_GET_PHYSICAL(hlc);
}

/*
 * HLCGetLogical -- extract the logical counter.
 */
uint16
HLCGetLogical(HLCTimestamp hlc)
{
	return (uint16) HLC_GET_LOGICAL(hlc);
}

/*
 * HLCMake -- construct an HLC timestamp from components.
 */
HLCTimestamp
HLCMake(uint64 physical_ms, uint16 logical)
{
	if (physical_ms > HLC_MAX_PHYSICAL)
		physical_ms = HLC_MAX_PHYSICAL;

	return HLC_MAKE(physical_ms, logical);
}

/*
 * HLCToTimestampTz -- convert HLC physical component to TimestampTz.
 *
 * Useful for displaying HLC as a human-readable timestamp.
 * The logical counter is lost in this conversion.
 */
TimestampTz
HLCToTimestampTz(HLCTimestamp hlc)
{
	uint64		physical_ms = HLC_GET_PHYSICAL(hlc);

	/* Convert milliseconds back to microseconds (TimestampTz) */
	return (TimestampTz) (physical_ms * 1000);
}

/*
 * HLCFromTimestampTz -- create an HLC from a TimestampTz.
 *
 * Sets logical counter to 0.  Useful for constructing snapshot HLCs.
 */
HLCTimestamp
HLCFromTimestampTz(TimestampTz ts)
{
	uint64		ms = (uint64) ts / 1000;

	if (ms > HLC_MAX_PHYSICAL)
		ms = HLC_MAX_PHYSICAL;

	return HLC_MAKE(ms, 0);
}

/*
 * HLCGetGlobal -- read the current global HLC without advancing it.
 *
 * Useful for reading the current state (e.g., for statistics).
 */
HLCTimestamp
HLCGetGlobal(void)
{
	if (RecnoHLCShmem == NULL)
		return 0;

	return pg_atomic_read_u64(&RecnoHLCShmem->global_hlc);
}

/*
 * HLCToString -- format an HLC timestamp for debugging/logging.
 *
 * Returns a palloc'd string in the format "physical_ms:logical"
 * (e.g., "826185600042:17").  The caller is responsible for pfree'ing
 * the result, or it will be freed when the current memory context
 * is reset.
 *
 * For InvalidHLCTimestamp (0), returns "0:0".
 */
char *
HLCToString(HLCTimestamp hlc)
{
	uint64		physical = HLC_GET_PHYSICAL(hlc);
	uint16		logical = (uint16) HLC_GET_LOGICAL(hlc);

	return psprintf(UINT64_FORMAT ":%u", physical, (unsigned int) logical);
}

/*
 * HLCGetDriftStats -- read clock drift diagnostic counters.
 *
 * Returns a snapshot of the drift statistics accumulated since
 * server startup.  All output parameters are optional (NULL-safe).
 */
void
HLCGetDriftStats(uint64 *max_drift_ms,
				 uint64 *total_backward_jumps,
				 uint64 *total_overflow_events)
{
	if (RecnoHLCShmem == NULL)
	{
		if (max_drift_ms)
			*max_drift_ms = 0;
		if (total_backward_jumps)
			*total_backward_jumps = 0;
		if (total_overflow_events)
			*total_overflow_events = 0;
		return;
	}

	if (max_drift_ms)
		*max_drift_ms = pg_atomic_read_u64(&RecnoHLCShmem->max_drift_ms);
	if (total_backward_jumps)
		*total_backward_jumps = pg_atomic_read_u64(&RecnoHLCShmem->total_backward_jumps);
	if (total_overflow_events)
		*total_overflow_events = pg_atomic_read_u64(&RecnoHLCShmem->total_overflow_events);
}

/* ----------------------------------------------------------------
 *					Uncertainty Interval
 * ----------------------------------------------------------------
 */

/*
 * HLCGetUncertaintyInterval -- compute the uncertainty interval for
 * an HLC timestamp.
 *
 * The interval is [hlc - offset, hlc + offset] where offset is
 * the maximum expected clock skew (recno_max_clock_offset_ms).
 *
 * Used in distributed scenarios where a reader must account for
 * the possibility that a write's real-time ordering differs from
 * its HLC ordering by up to max_clock_offset.
 */
void
HLCGetUncertaintyInterval(HLCTimestamp hlc,
						  HLCTimestamp *lower,
						  HLCTimestamp *upper)
{
	uint64		physical = HLC_GET_PHYSICAL(hlc);
	uint64		offset = (uint64) recno_max_clock_offset_ms;

	/* Lower bound: subtract offset, clamped to 0 */
	if (physical > offset)
		*lower = HLC_MAKE(physical - offset, 0);
	else
		*lower = HLC_MAKE(0, 0);

	/* Upper bound: add offset, clamped to max */
	if (physical + offset <= HLC_MAX_PHYSICAL)
		*upper = HLC_MAKE(physical + offset, HLC_MAX_LOGICAL);
	else
		*upper = HLC_MAKE(HLC_MAX_PHYSICAL, HLC_MAX_LOGICAL);
}

/*
 * HLCInUncertaintyWindow -- check if a timestamp falls within the
 * uncertainty window of a commit HLC.
 *
 * Returns true if reader_hlc is within [commit_hlc, commit_hlc + offset].
 * This is the one-sided check used by CockroachDB: a reader whose
 * timestamp is in the "future" part of the uncertainty interval must
 * either wait or push its timestamp beyond the window.
 */
bool
HLCInUncertaintyWindow(HLCTimestamp reader_hlc, HLCTimestamp commit_hlc)
{
	uint64		reader_phys = HLC_GET_PHYSICAL(reader_hlc);
	uint64		commit_phys = HLC_GET_PHYSICAL(commit_hlc);
	uint64		offset = (uint64) recno_max_clock_offset_ms;

	/* Reader is before the commit: no uncertainty */
	if (reader_phys < commit_phys)
		return false;

	/* Reader is within [commit, commit + offset]: uncertainty */
	if (reader_phys <= commit_phys + offset)
		return true;

	/* Reader is well past the commit: no uncertainty */
	return false;
}

/* ----------------------------------------------------------------
 *					GUC Assign Hooks
 * ----------------------------------------------------------------
 */

void
assign_recno_node_id(int newval, void *extra)
{
	if (RecnoHLCShmem != NULL)
	{
		/* node_id is rarely written and not in the hot path; plain store */
		RecnoHLCShmem->node_id = (uint16) (newval & 0x0FFF);
		pg_write_barrier();
	}
}

void
assign_recno_max_clock_offset(int newval, void *extra)
{
	/* No shared state to update; GUC value is read directly */
}
