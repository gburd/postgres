#!/usr/sbin/dtrace -s
/*
 * spinlock_hold_times.d -- Measure spinlock hold times in buffer pool code.
 *
 * Traces SpinLockAcquire/SpinLockRelease pairs in key buffer pool functions
 * to measure lock contention across different replacement algorithms.
 *
 * Key locks measured:
 *   - buffer_strategy_lock (clock-sweep freelist.c)
 *   - arc_lock (ARC's four-list lock)
 *   - buf_mapping_lock (buffer hash table partitions)
 *   - buffer header spinlocks (per-buffer state transitions)
 *
 * Usage:
 *   dtrace -s spinlock_hold_times.d -p <postgres_pid>
 *   dtrace -s spinlock_hold_times.d -c 'postgres -D /path/to/data'
 *
 * Output: histogram of hold times in nanoseconds, per lock category
 */

#pragma D option quiet
#pragma D option defaultargs

dtrace:::BEGIN
{
    printf("Tracing spinlock hold times in buffer pool code...\n");
    printf("Hit Ctrl-C to stop and print results.\n\n");
    start_time = timestamp;
}

/*
 * Track SpinLockAcquire entry -- record timestamp per thread.
 * We key on the lock address to distinguish different locks.
 */
pid$target::SpinLockAcquire:entry
{
    self->spin_start = timestamp;
    self->spin_addr = arg0;
}

pid$target::SpinLockRelease:entry
/self->spin_start/
{
    this->hold_ns = timestamp - self->spin_start;

    @hold_time["all_spinlocks"] = quantize(this->hold_ns);
    @hold_count["all_spinlocks"] = count();
    @hold_total["all_spinlocks"] = sum(this->hold_ns);

    /* Per-address tracking for top contended locks */
    @per_lock_count[self->spin_addr] = count();
    @per_lock_total[self->spin_addr] = sum(this->hold_ns);

    self->spin_start = 0;
    self->spin_addr = 0;
}

/*
 * Track LWLockAcquire for buffer mapping locks and algorithm swap locks.
 */
pid$target::LWLockAcquire:entry
{
    self->lwlock_start = timestamp;
    self->lwlock_id = arg0;
}

pid$target::LWLockRelease:entry
/self->lwlock_start/
{
    this->hold_ns = timestamp - self->lwlock_start;

    @lwlock_hold["all_lwlocks"] = quantize(this->hold_ns);
    @lwlock_count["all_lwlocks"] = count();

    self->lwlock_start = 0;
    self->lwlock_id = 0;
}

/*
 * Track StrategyGetBuffer calls (clock sweep victim selection).
 */
pid$target::StrategyGetBuffer:entry
{
    self->strategy_start = timestamp;
}

pid$target::StrategyGetBuffer:return
/self->strategy_start/
{
    this->elapsed_ns = timestamp - self->strategy_start;
    @strategy_time["StrategyGetBuffer"] = quantize(this->elapsed_ns);
    @strategy_count["StrategyGetBuffer"] = count();
    self->strategy_start = 0;
}

/*
 * Track *GetVictim calls (pluggable algorithm victim selection).
 * These are called via function pointers so we trace by name pattern.
 */
pid$target::*get_victim*:entry
{
    self->victim_start = timestamp;
    self->victim_func = probefunc;
}

pid$target::*get_victim*:return
/self->victim_start/
{
    this->elapsed_ns = timestamp - self->victim_start;
    @victim_time[self->victim_func] = quantize(this->elapsed_ns);
    @victim_count[self->victim_func] = count();
    self->victim_start = 0;
}

dtrace:::END
{
    this->elapsed_s = (timestamp - start_time) / 1000000000;
    printf("\n=== Spinlock Hold Times (elapsed: %d seconds) ===\n\n", this->elapsed_s);

    printf("--- All SpinLock Hold Times (ns) ---\n");
    printa(@hold_time);

    printf("\n--- SpinLock Acquisition Counts ---\n");
    printa("  %-40s %@d\n", @hold_count);

    printf("\n--- SpinLock Total Hold Time (ns) ---\n");
    printa("  %-40s %@d\n", @hold_total);

    printf("\n--- Top Contended Lock Addresses (by count) ---\n");
    trunc(@per_lock_count, 10);
    printa("  lock=0x%p  count=%@d\n", @per_lock_count);

    printf("\n--- Top Contended Lock Addresses (by total hold) ---\n");
    trunc(@per_lock_total, 10);
    printa("  lock=0x%p  total_ns=%@d\n", @per_lock_total);

    printf("\n--- LWLock Hold Times (ns) ---\n");
    printa(@lwlock_hold);

    printf("\n--- LWLock Acquisition Counts ---\n");
    printa("  %-40s %@d\n", @lwlock_count);

    printf("\n--- StrategyGetBuffer Latency (ns) ---\n");
    printa(@strategy_time);
    printa("  %-40s calls=%@d\n", @strategy_count);

    printf("\n--- Victim Selection Latency (ns) ---\n");
    printa(@victim_time);
    printa("  %-40s calls=%@d\n", @victim_count);
}
