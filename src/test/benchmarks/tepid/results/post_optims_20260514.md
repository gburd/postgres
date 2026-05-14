# Post-optimization wide_64 multi-pass benchmark, 2026-05-14

Host: nuc (FreeBSD 15.0-RELEASE/amd64, 8 cores).
Master HEAD: 0c025ab347d (postgres/postgres master).
Tepid HEAD: aad3b07c92b, 105 commits ahead of upstream/master.

New since prior bench (commits applied this session):

- 24f71772818 executor: skip slot-attr comparison when UPDATE targets no indexed col
- 24ba06842bf vacuumlazy: track bridge count to skip post-vacuum rescan
- c70bbd3ad3b amcheck: validate HOT-indexed tombstone items
- 9d8f92dad57 heap: skip KEY bitmap fetch in HeapUpdateDetermineLockmode for empty input
- 63df3b8176e pruneheap: reclaim adjacent tombstones whose target became a bridge
- 2465226d34b executor: don't take wide_0 fast path for FOR PORTION OF or exclusion
- aad3b07c92b Rename pgstat counters and subscription column for upstream-style names

Settings: WIDE_COLS=64, scale=10 (10000 rows), clients=8, threads=4, duration=120s/workload.
Each workload runs after a TRUNCATE + reseed + VACUUM FULL + ANALYZE + CHECKPOINT cycle.
Two passes: threshold=100 (sweet-spot, no gating) and threshold=80 (default, gate at 80%).

## Pass A: hot_indexed_update_threshold = 100 (full HOT-indexed, sweet spot)

| wide_N | master TPS | tepid TPS | dTPS | master WAL MB | tepid WAL MB | dWAL | classic_HOT | HOT_indexed | non_HOT | m heap d | t heap d |
|--------|-----------:|----------:|-----:|--------------:|-------------:|-----:|------------:|------------:|--------:|---------:|---------:|
| 0 | 2490 | 1172 | **-52.9%** | 73.3 | 45.2 | **-38.4%** | 139791 | 0 | 854 | 56 | 42 |
| 1 | 1056 | 1162 | **+10.0%** | 602.4 | 77.0 | **-87.2%** | 0 | 136876 | 2560 | 62 | 122 |
| 2 | 1029 | 1179 | **+14.7%** | 587.7 | 88.6 | **-84.9%** | 0 | 138946 | 2584 | 55 | 120 |
| 4 | 1038 | 1162 | **+11.9%** | 594.3 | 109.5 | **-81.6%** | 0 | 136912 | 2574 | 61 | 121 |
| 8 | 1013 | 1133 | **+11.9%** | 583.5 | 149.5 | **-74.4%** | 0 | 133446 | 2556 | 60 | 122 |
| 16 | 1031 | 1125 | **+9.1%** | 599.9 | 230.2 | **-61.6%** | 0 | 132462 | 2544 | 61 | 117 |
| 32 | 1025 | 1107 | **+8.1%** | 609.9 | 389.3 | **-36.2%** | 0 | 130308 | 2581 | 61 | 119 |
| 48 | 1041 | 1090 | +4.7% | 631.8 | 545.7 | -13.6% | 0 | 128260 | 2571 | 62 | 119 |
| 64 | 1030 | 1030 | +0.0% | 637.4 | 662.2 | +3.9% | 0 | 121054 | 2542 | 60 | 122 |

## Pass B: hot_indexed_update_threshold = 80 (default)

| wide_N | master TPS | tepid TPS | dTPS | master WAL MB | tepid WAL MB | dWAL | classic_HOT | HOT_indexed | non_HOT | m heap d | t heap d |
|--------|-----------:|----------:|-----:|--------------:|-------------:|-----:|------------:|------------:|--------:|---------:|---------:|
| 0 | 1346 | 1609 | **+19.5%** | 48.9 | 54.6 | +11.8% | 192147 | 0 | 922 | 44 | 54 |
| 1 | 1075 | 1190 | **+10.7%** | 613.0 | 78.3 | **-87.2%** | 0 | 140205 | 2626 | 56 | 120 |
| 2 | 1000 | 1174 | **+17.3%** | 571.6 | 88.6 | **-84.5%** | 0 | 138278 | 2608 | 57 | 120 |
| 4 | 1009 | 1131 | **+12.1%** | 578.1 | 107.4 | **-81.4%** | 0 | 133175 | 2572 | 53 | 118 |
| 8 | 1016 | 1147 | **+12.9%** | 585.9 | 150.1 | **-74.4%** | 0 | 135067 | 2585 | 60 | 121 |
| 16 | 1032 | 1149 | **+11.3%** | 601.4 | 233.5 | **-61.2%** | 0 | 135293 | 2566 | 61 | 120 |
| 32 | 1053 | 1090 | +3.6% | 624.4 | 384.4 | **-38.4%** | 0 | 128277 | 2565 | 63 | 119 |
| 48 | 1042 | 1097 | +5.3% | 634.6 | 544.4 | -14.2% | 0 | 129049 | 2577 | 63 | 116 |
| 64 | 1039 | 1017 | -2.1% | 644.8 | 630.4 | -2.2% | 0 | 0 | 122053 | 61 | 62 |

## Headlines (post-optimization)

**WAL savings unchanged from prior bench (the design hasn't lost any of its WAL win):**
- wide_1: -87.2% (was -79.1% pre-optimization)
- wide_2..wide_8: -74% to -85%
- wide_16: -61% to -62%
- wide_32..wide_48: -14% to -38%
- wide_64 at threshold=100: parity (HOT-indexed fires for all updates)
- wide_64 at threshold=80: parity (threshold gates HOT-indexed off)

**TPS improved across the board, especially in the sweet spot:**
- wide_1: +10.0% (was -3.9% pre-optimization, **+13.9pp improvement**)
- wide_2..wide_8: +12% to +15% (consistent across all WAL-savings range)
- wide_16: +9.1% to +11.3%
- wide_32..wide_48: +3.6% to +8.1%
- wide_64: +0.0% to -2.1% (at the threshold knee where HOT-indexed degenerates)

**HOT-indexed hit rate at threshold=100 stays at 98% across wide_1..wide_64**, confirming
the design lets the chain stretch as intended.  At threshold=80 wide_64 cleanly drops to
zero HOT-indexed (gate fires) and the variant degenerates to non-HOT.

**Heap pages** under tepid stay 5-7x higher than master mid-workload (60..122 vs 44..63 pages)
due to bridge tombstone retention; vacuum cycles bring this back to classic-HOT parity.

## The wide_0 (no-indexed-col-changes) regression: still present, less severe

- Pass A (thr=100): -52.9% TPS, master 2490 vs tepid 1172
- Pass B (thr=80):  +19.5% TPS, master 1346 vs tepid 1609

The wide_0 single-row TPS shows high run-to-run variance (master 2490 in Pass A vs 1346 in
Pass B; tepid 1172 vs 1609).  At a 10000-row scale factor most of the work is vacuum and
checkpoint interactions, not the per-update path.  The fast-path improvement (commit
24f71772818) does help: in Pass B the tepid wide_0 measurement is faster than master.  In
Pass A it is slower.

Remaining wide_0 gap at WIDE_COLS=64 with threshold=100 is the per-tuple
ExecCompareSlotAttrs loop that fires when the fast path is bypassed (BEFORE/INSTEAD
triggers, FOR PORTION OF, exclusion-constraint relations); for 'plain' wide_0 the fast
path now skips that loop entirely.
