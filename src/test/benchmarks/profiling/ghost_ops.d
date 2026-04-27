#!/usr/sbin/dtrace -s
/*
 * ghost_ops.d -- Ghost list operations per second for ARC, CAR, LIRS.
 *
 * Ghost lists (B1/B2 in ARC/CAR, Q in LIRS) track metadata about recently
 * evicted pages. High ghost hit rates indicate good adaptation; high ghost
 * miss rates indicate rapid working set changes.
 *
 * Usage:
 *   dtrace -s ghost_ops.d -p <postgres_pid>
 */

#pragma D option quiet
#pragma D option defaultargs
#pragma D option aggrate=1s

dtrace:::BEGIN
{
    printf("Tracing ghost list operations... Hit Ctrl-C to stop.\n");
    printf("%-20s  %8s  %8s  %8s  %8s  %8s\n",
           "TIMESTAMP", "G_INSERT", "G_HIT", "G_EVICT", "ADAPT+", "ADAPT-");
    seconds = 0;
}

/*
 * Ghost list insert -- page evicted from T1/T2 moves to B1/B2
 * Look for functions that add to ghost lists.
 */
pid$target::*ghost*insert*:entry,
pid$target::*ghost*add*:entry,
pid$target::*b1*add*:entry,
pid$target::*b2*add*:entry
{
    @ghost_inserts = count();
}

/*
 * Ghost list hit -- found page in ghost list, triggers adaptation
 */
pid$target::*ghost*hit*:entry,
pid$target::*ghost*found*:entry,
pid$target::*b1*hit*:entry,
pid$target::*b2*hit*:entry
{
    @ghost_hits = count();
}

/*
 * Ghost list eviction -- oldest ghost entry removed
 */
pid$target::*ghost*evict*:entry,
pid$target::*ghost*remove*:entry,
pid$target::*ghost*trim*:entry
{
    @ghost_evicts = count();
}

/*
 * Adaptation: target_t1_size increased (B1 hit => favor recency)
 */
pid$target::*adapt*increase*:entry,
pid$target::*arc*adapt*:entry
{
    @adapt_increase = count();
}

/*
 * Adaptation: target_t1_size decreased (B2 hit => favor frequency)
 */
pid$target::*adapt*decrease*:entry
{
    @adapt_decrease = count();
}

/*
 * Per-second tick -- print rates
 */
tick-1s
{
    seconds++;
    normalize(@ghost_inserts, 1);
    normalize(@ghost_hits, 1);
    normalize(@ghost_evicts, 1);
    normalize(@adapt_increase, 1);
    normalize(@adapt_decrease, 1);

    printf("%-20Y  ", walltimestamp);
    printa("%@8d  ", @ghost_inserts);
    printa("%@8d  ", @ghost_hits);
    printa("%@8d  ", @ghost_evicts);
    printa("%@8d  ", @adapt_increase);
    printa("%@8d\n", @adapt_decrease);

    clear(@ghost_inserts);
    clear(@ghost_hits);
    clear(@ghost_evicts);
    clear(@adapt_increase);
    clear(@adapt_decrease);
}

dtrace:::END
{
    printf("\nTraced for %d seconds.\n", seconds);
}
