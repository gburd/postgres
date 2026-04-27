#!/usr/sbin/dtrace -s
/*
 * list_traversal.d -- Count list traversal lengths in victim selection.
 *
 * Measures how many buffers each algorithm inspects before finding a victim.
 * Higher traversal = more contention and wasted CPU. Key metric for comparing
 * clock-sweep vs ARC vs CAR vs LIRS vs LRU vs OSIC.
 *
 * Usage:
 *   dtrace -s list_traversal.d -p <postgres_pid>
 */

#pragma D option quiet
#pragma D option defaultargs

dtrace:::BEGIN
{
    printf("Tracing list traversal lengths in victim selection...\n");
    printf("Hit Ctrl-C to stop.\n\n");
}

/*
 * Clock-sweep: count buffers examined per StrategyGetBuffer call.
 * The clock hand advances once per examined buffer.
 */
pid$target::StrategyGetBuffer:entry
{
    self->clock_examined = 0;
    self->in_clock = 1;
}

/* BufferDescriptorGetBuffer is called for each candidate in the scan */
pid$target::GetBufferDescriptor:entry
/self->in_clock/
{
    self->clock_examined++;
}

pid$target::StrategyGetBuffer:return
/self->in_clock/
{
    @clock_traversal = quantize(self->clock_examined);
    @clock_total_examined = sum(self->clock_examined);
    @clock_calls = count();
    self->in_clock = 0;
    self->clock_examined = 0;
}

/*
 * Generic victim selection: count via the get_victim function pointer.
 * Most algorithms do internal traversal -- we measure the total time
 * as a proxy for traversal length.
 */
pid$target::*get_victim*:entry
{
    self->gv_start = timestamp;
}

pid$target::*get_victim*:return
/self->gv_start/
{
    this->elapsed = timestamp - self->gv_start;
    @victim_latency[probefunc] = quantize(this->elapsed);
    @victim_calls[probefunc] = count();
    self->gv_start = 0;
}

/*
 * ARC-specific: track ghost list operations.
 * Ghost list scans happen during replacement decisions.
 */
pid$target::*arc*ghost*:entry,
pid$target::*arc*adapt*:entry
{
    self->arc_op_start = timestamp;
}

pid$target::*arc*ghost*:return,
pid$target::*arc*adapt*:return
/self->arc_op_start/
{
    this->elapsed = timestamp - self->arc_op_start;
    @arc_ops[probefunc] = quantize(this->elapsed);
    @arc_op_count[probefunc] = count();
    self->arc_op_start = 0;
}

dtrace:::END
{
    printf("\n=== List Traversal Analysis ===\n\n");

    printf("--- Clock-Sweep: Buffers Examined Per Victim ---\n");
    printa(@clock_traversal);
    printf("\n  Total calls: ");
    printa("%@d\n", @clock_calls);
    printf("  Total buffers examined: ");
    printa("%@d\n", @clock_total_examined);

    printf("\n--- Victim Selection Latency by Algorithm (ns) ---\n");
    printa(@victim_latency);
    printf("\n--- Victim Selection Call Counts ---\n");
    printa("  %-40s %@d\n", @victim_calls);

    printf("\n--- ARC Ghost/Adaptation Operations (ns) ---\n");
    printa(@arc_ops);
    printf("\n--- ARC Operation Counts ---\n");
    printa("  %-40s %@d\n", @arc_op_count);
}
