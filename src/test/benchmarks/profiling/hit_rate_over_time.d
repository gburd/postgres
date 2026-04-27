#!/usr/sbin/dtrace -s
/*
 * hit_rate_over_time.d -- Sample buffer pool hit/miss rates over time.
 *
 * Produces a time-series of hit ratio for adaptation curve analysis.
 * Shows how quickly each algorithm converges on an optimal hit rate
 * as workload patterns change.
 *
 * Usage:
 *   dtrace -s hit_rate_over_time.d -p <postgres_pid>
 */

#pragma D option quiet
#pragma D option defaultargs

dtrace:::BEGIN
{
    printf("Sampling buffer hit rates every second...\n");
    printf("%-20s  %10s  %10s  %8s\n",
           "TIMESTAMP", "HITS", "MISSES", "HIT_PCT");
    hits = 0;
    misses = 0;
    total_hits = 0;
    total_misses = 0;
}

/*
 * Buffer hit -- ReadBuffer finds the page in shared buffers.
 * BufTableLookup returning a valid buffer ID = hit.
 */
pid$target::ReadBuffer_common:entry
{
    self->in_readbuf = 1;
}

pid$target::BufTableLookup:return
/self->in_readbuf && arg1 >= 0/
{
    hits++;
    total_hits++;
}

pid$target::BufTableLookup:return
/self->in_readbuf && arg1 < 0/
{
    misses++;
    total_misses++;
}

pid$target::ReadBuffer_common:return
/self->in_readbuf/
{
    self->in_readbuf = 0;
}

/*
 * Alternative: track via shared buffer reads vs hits directly.
 * These fire on the BufferAlloc path.
 */
pid$target::BufferAlloc:return
{
    /* arg1 = foundInBuffer (boolean) */
    self->buf_found = arg1;
}

/*
 * Per-second output
 */
tick-1s
{
    this->total = hits + misses;
    this->pct = this->total > 0 ? (hits * 100) / this->total : 0;

    printf("%-20Y  %10d  %10d  %7d%%\n",
           walltimestamp, hits, misses, this->pct);

    hits = 0;
    misses = 0;
}

dtrace:::END
{
    this->grand_total = total_hits + total_misses;
    this->grand_pct = this->grand_total > 0 ?
                      (total_hits * 100) / this->grand_total : 0;

    printf("\n=== Summary ===\n");
    printf("Total hits:   %d\n", total_hits);
    printf("Total misses: %d\n", total_misses);
    printf("Overall hit rate: %d%%\n", this->grand_pct);
}
