# RECNO Clock-Bound Integration Design

## Overview

This document describes the integration of AWS clock-bound daemon with PostgreSQL RECNO for safe timestamp-based MVCC and logical replication across distributed nodes.

## Architecture

### Components

1. **HLC (Hybrid Logical Clock)** - Already implemented in `recno_hlc.c`
   - 48-bit physical time (milliseconds) + 16-bit logical counter
   - Provides causally consistent timestamps
   - Masks backward clock jumps

2. **Clock-Bound Daemon Integration** - New in `recno_clock.c`
   - Reads error bounds from `/dev/shm/clockbound`
   - Provides earliest/latest time bounds
   - Falls back to HLC +/- max_offset when unavailable

3. **Uncertainty Intervals** - Added to MVCC
   - Tracks uncertainty window for each transaction
   - Triggers transaction restarts when needed
   - Prevents read anomalies from clock skew

## Data Structures

### RecnoTimestampBound
```c
typedef struct RecnoTimestampBound
{
    HLCTimestamp hlc;           /* Hybrid logical clock timestamp */
    int64        earliest_us;   /* Earliest possible time (microseconds) */
    int64        latest_us;     /* Latest possible time (microseconds) */
    uint64       error_bound_ms; /* Error bound in milliseconds */
    bool         bounds_valid;   /* True if from clock-bound daemon */
} RecnoTimestampBound;
```

### ClockBoundData (from daemon)
```c
typedef struct ClockBoundData
{
    struct timespec earliest;   /* Earliest possible time */
    struct timespec latest;     /* Latest possible time */
    uint64      error_bound_ns; /* Error bound in nanoseconds */
    uint32      segment_id;     /* Daemon segment ID */
    uint32      flags;          /* Status flags */
} ClockBoundData;
```

## Key Functions

### RecnoGetTimestampBounds()
- Primary interface for getting bounded timestamps
- Reads from clock-bound daemon if available
- Falls back to HLC +/- max_offset
- Returns RecnoTimestampBound with error bounds

### RecnoTupleVisibleWithUncertainty()
- Enhanced visibility check with uncertainty handling
- Detects when tuple is in uncertainty window
- Triggers transaction restart if needed
- Prevents read anomalies from clock skew

### RecnoWaitForClockBound()
- Used by replicas during logical replication
- Waits until uncertainty is resolved
- Ensures causally consistent reads
- Prevents replication anomalies

## Configuration Parameters

### recno_enable_clock_bound (bool)
- Default: true
- Enables clock-bound daemon integration
- Falls back to HLC-only when false or unavailable

### recno_fatal_on_clock_drift (bool)
- Default: true
- Shut down node if drift exceeds 80% of max offset
- Prevents consistency violations
- Trade-off: consistency over availability

### recno_clock_check_interval_ms (int)
- Default: 1000 (1 second)
- How often to check clock health
- Lower values detect drift sooner

### recno_max_clock_offset_ms (int)
- Default: 250
- Maximum expected clock skew between nodes
- Used for uncertainty intervals
- Smaller = fewer restarts, larger = more tolerance

## Transaction Restart Protocol

When a transaction encounters data in its uncertainty window:

1. Transaction reads tuple with timestamp T
2. Check if T is in uncertainty window [snapshot, snapshot + max_offset]
3. If yes: restart transaction with new timestamp T + 1
4. Uncertainty window shrinks on each restart
5. Eventually succeeds (guaranteed progress)

## Logical Replication Integration

### WAL Record Enhancement
```c
typedef struct xl_recno_hlc_info
{
    uint64  commit_hlc;         /* Commit HLC timestamp */
    uint64  commit_dvv;         /* Commit DVV dot */
    uint64  uncertainty_lower;  /* Lower bound of uncertainty */
    uint64  uncertainty_upper;  /* Upper bound of uncertainty */
} xl_recno_hlc_info;
```

### Replica Apply Logic
1. Read WAL record with HLC and bounds
2. Advance local HLC to at least commit_hlc
3. Check if uncertainty resolved
4. If not, wait until local time > uncertainty_upper
5. Apply change safely

## Clock Skew Handling

### Detection
- Monitor error bounds from clock-bound
- Track maximum observed error
- Warning at 50% of max_offset
- Fatal at 80% of max_offset

### Self-Shutdown
- Node terminates if drift exceeds threshold
- Prevents silent data corruption
- Forces operator intervention
- Must fix NTP before restart

## Fallback Modes

### Clock-Bound Available
- Error bounds: 1-10ms (EC2 with Amazon Time Sync)
- Tight uncertainty windows
- Fewer transaction restarts
- Better performance

### Clock-Bound Unavailable
- Error bounds: 250ms (configured max_offset)
- Larger uncertainty windows
- More transaction restarts
- Still correct, lower performance

## Testing Strategy

### Unit Tests
1. Mock clock-bound daemon
2. Simulate various error bounds
3. Test fallback to HLC-only
4. Verify transaction restart logic

### Integration Tests
1. Multi-node setup with clock skew
2. Logical replication with drift
3. Transaction conflicts in uncertainty
4. Node shutdown on excessive drift

### Failure Scenarios
1. Clock-bound daemon crashes
2. NTP synchronization lost
3. Clock jumps forward/backward
4. Network partition with drift

## Performance Impact

### Expected Overhead
- Clock-bound read: < 1μs (shared memory)
- Uncertainty check: ~10ns per visibility check
- Transaction restart: 0-1% of transactions
- Replication wait: 0-10ms typical

### Optimization Opportunities
1. Cache clock-bound data for 1ms
2. Batch uncertainty checks
3. Predictive restart avoidance
4. Adaptive max_offset tuning

## Comparison to Other Systems

### vs CockroachDB
- Similar: HLC + uncertainty intervals
- Similar: Transaction restarts not waits
- Different: We support clock-bound for tighter bounds
- Different: Graceful fallback to HLC-only

### vs Spanner
- Different: No atomic clocks required
- Different: Restarts instead of commit waits
- Similar: Bounded uncertainty
- Similar: Causally consistent

## Future Enhancements

### Phase 2: Background Monitoring
- Continuous clock health checks
- NTP sync verification
- Drift trend analysis
- Predictive warnings

### Phase 3: Advanced Features
- Per-transaction max_offset override
- Adaptive uncertainty windows
- Clock synchronization hints
- Cross-datacenter optimization

## References

1. Kulkarni et al., "Logical Physical Clocks", 2014
2. CockroachDB Clock Management
3. AWS Clock-Bound Documentation
4. Google Spanner TrueTime

## Status

- Phase 1: Core clock-bound reading - **COMPLETE**
- Phase 2: Background monitoring - TODO
- Phase 3: Advanced features - FUTURE