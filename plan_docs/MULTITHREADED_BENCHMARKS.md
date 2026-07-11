Multithreaded PostgreSQL Benchmarks
===================================

This document records the latest full Phase 15 benchmark run for the
multithreaded PostgreSQL branch. It is meant to be a stable comparison point
for future protocol-carrier pool work, not a production benchmark claim.

Run metadata
------------

| Field | Value |
| --- | --- |
| Date | June 22, 2026 |
| Branch | `phase15-real-carrier-pool` |
| Commit | `5782dccf5c` |
| Result directory | `/home/sam/codex-work/mtpg-bench-results/full_phase15_fixed_20260622_192912` |
| Suite log | `/home/sam/codex-work/mtpg-bench-results/full_phase15_fixed_20260622_192912.log` |
| Suite index | `/home/sam/codex-work/mtpg-bench-results/full_phase15_fixed_20260622_192912/index.md` |
| Runner | `src/tools/benchmark/mtpg_phase15_benchmark_suite.pl --profiles=all` |
| Matrix runner | `src/tools/benchmark/mtpg_pgbench_matrix.pl` |
| Branch install | `/home/sam/codex-work/mtpg-current/tmp_install` |
| Vanilla install | `/home/sam/codex-work/vanilla-pg19/tmp_install` |
| Client install | `/home/sam/codex-work/vanilla-pg19/tmp_install` |
| Result status | 12 profiles completed, 113 TPS rows, all `failed_transactions = 0` |

Post-benchmark validation was rerun after this benchmark pass:

| Target | Result |
| --- | --- |
| `make check` | PASS |
| `make check-threaded` | PASS |
| `make check-threaded-smoke` | PASS |
| `make check-threaded-150` | PASS |
| `make check-threaded-200` | PASS |
| `make check-threaded-world-core` | PASS |

`check-world` is not a current green target for this branch and was not part of
this validation baseline.

Lane definitions
----------------

| Lane | Meaning |
| --- | --- |
| `vanilla` | PostgreSQL 19 beta 1 built from the vanilla source tree in this workspace. |
| `branch_process` | This branch with normal process-per-backend execution. |
| `branch_threaded` | This branch with `multithreaded = on` and `pooled_protocol_carriers = 0`, giving one carrier thread per session. |
| `branch_pool_N` | This branch with `multithreaded = on` and `pooled_protocol_carriers = N`, giving a bounded pool of protocol carrier threads. |

The pooled mode in this phase only detaches at top-level frontend protocol
input. Deep waits remain carrier-pinned.

Profile definitions
-------------------

| Profile | Clients | Pgbench threads | Runs | Duration | Pool sizes | Purpose |
| --- | ---: | ---: | ---: | ---: | --- | --- |
| `pinned_hot` | 32 | 8 | 3 | 20s | none | Hot-path parity check for vanilla, branch process, and pinned threads. |
| `pool_realish_100ms` | 200 | 32 | 3 | 20s | 32, 64, 128, 192 | Mostly-idle clients with indexed reads, writes, WAL, range reads, and 100 ms client think time. |
| `pool_realish_1000ms` | 200 | 32 | 3 | 25s | 16, 32, 64, 128 | Long-idle clients with table/index work and protocol-read parks. |
| `pool_stateful_1000ms` | 100 | 16 | 2 | 30s | 16, 32, 64 | Stateful temp-table diagnostic, verifying session state survives parking and wakeup. |
| `pool_scale_1000_realish` | 1000 | 64 | 2 | 20s | 64, 128, 256, 512 | Large indexed-read idle population comparing vanilla, pinned threads, and bounded pools. |
| `connection_churn_realish` | 64 | 16 | 3 | 20s | 32, 64, 128 | One database-touching transaction per connection. |
| `pool_idle_100ms` | 200 | 32 | 3 | 15s | 32, 64, 128, 192 | Mostly-idle `SELECT 1` wake cycle at 100 ms. |
| `pool_idle_1000ms` | 200 | 32 | 3 | 20s | 16, 32, 64, 128 | Long-idle `SELECT 1` wake cycle at 1000 ms. |
| `pool_burst_10ms` | 200 | 32 | 3 | 15s | 64, 128, 192 | Short-idle diagnostic for parking overhead and bursty wakeups. |
| `pool_scale_1000_idle` | 1000 | 64 | 3 | 30s | 64, 128, 256, 512 | Large mostly-idle connection population, pinned threads versus pools. |
| `connection_memory_idle` | 1000 | 64 | 2 | 20s | 64, 128, 256, 512 | Large idle connection memory profile with memory detail and protocol-park memory logging. |
| `connection_churn` | 64 | 16 | 3 | 15s | 32, 64, 128 | One tiny transaction per connection. |

Workload definitions
--------------------

All non-builtin SQL workloads use prepared mode. Extra setup creates
`bench_one`, `bench_kv`, and `bench_client_state`, initializes normal
`pgbench_accounts`, vacuums the tables, and checkpoints before measurement.

| Workload | What it does |
| --- | --- |
| `builtin_select_prepared` | Pgbench built-in select-only workload in prepared mode. |
| `select1_prepared` | `SELECT 1;` |
| `bench_one_prepared` | Reads one fixed row from `bench_one`. |
| `kv_read_prepared` | Random point read from `bench_kv`. |
| `select1_sleep_wake_10ms_prepared` | `SELECT 1;`, client-side `\sleep 10 ms`, then `SELECT 1;`. |
| `select1_sleep_wake_100ms_prepared` | `SELECT 1;`, client-side `\sleep 100 ms`, then `SELECT 1;`. |
| `select1_sleep_wake_1000ms_prepared` | `SELECT 1;`, client-side `\sleep 1000 ms`, then `SELECT 1;`. |
| `select1_connect_prepared` | `SELECT 1;` with pgbench reconnecting for each transaction. |
| `kv_read_sleep_wake_100ms_prepared` | Zipfian point read from `bench_kv`, 100 ms client sleep, then another point read of the same key. |
| `kv_read_sleep_wake_1000ms_prepared` | Zipfian point read from `bench_kv`, 1000 ms client sleep, then another point read of the same key. |
| `app_txn_sleep_wake_100ms_prepared` | Account point read, 100 ms client sleep, transaction updating `pgbench_accounts` and `bench_client_state`, commit, then account point read. |
| `app_txn_sleep_wake_1000ms_prepared` | Same as `app_txn_sleep_wake_100ms_prepared`, but with 1000 ms client sleep. |
| `app_mixed_sleep_wake_100ms_prepared` | Zipfian `bench_kv` point read, 50 ms client sleep, small state update, 50 ms client sleep, then range aggregate over nearby `bench_kv` rows. |
| `stateful_temp_sleep_wake_1000ms_prepared` | Sets `application_name`, creates or reuses a session temp table, updates it, sleeps for 1000 ms, then reads it back. |
| `app_txn_connect_prepared` | Reconnect-heavy app transaction: point read and update in `pgbench_accounts`, update of `bench_client_state`, then commit. |

Headline results
----------------

| Signal | Result |
| --- | --- |
| Hot tiny-query path | Branch process is 0.907x to 0.934x vanilla; pinned threads are 0.797x to 0.952x vanilla depending on workload. |
| 200 mostly-idle clients, 100 ms `SELECT 1` wake cycle | `branch_pool_64` and larger are within about 0.4% of vanilla/process/threaded throughput. |
| 200 real-ish clients, 100 ms wake cycle | Pool sizes 64 and larger are close to process/threaded on `kv_read` and `app_txn`; pool 32 has a large `app_mixed` outlier. |
| 1000 mostly-idle clients | `branch_pool_128` reaches 978 TPS versus 997 TPS for pinned threads while using 122 server threads instead of 1008. |
| 1000 mostly-idle memory profile | Pooled lanes use about 537 KB to 561 KB PSS per client versus 961 KB for pinned threads, 1062 KB for branch process, and 1212 KB for vanilla. |
| 1000 real-ish idle clients | `branch_pool_512` reaches 934 TPS, about 0.941x vanilla and 0.948x pinned threads, while using 123 server threads instead of about 1000. |
| Connection churn | Pooled mode is still slower than process and pinned threads. This is not the current design win and remains an optimization target. |

Full TPS results
----------------

The suite records median TPS and latency across each profile's measured runs.

| Profile | Lane | Workload | TPS | Latency ms | Failed txns |
| --- | --- | --- | ---: | ---: | ---: |
| `connection_churn` | `vanilla` | `select1_connect_prepared` | 3655.681716 | 17.507 | 0 |
| `connection_churn` | `branch_process` | `select1_connect_prepared` | 3266.792957 | 19.591 | 0 |
| `connection_churn` | `branch_threaded` | `select1_connect_prepared` | 1894.912628 | 33.775 | 0 |
| `connection_churn` | `branch_pool_32` | `select1_connect_prepared` | 1583.679950 | 40.412 | 0 |
| `connection_churn` | `branch_pool_64` | `select1_connect_prepared` | 2077.182975 | 30.811 | 0 |
| `connection_churn` | `branch_pool_128` | `select1_connect_prepared` | 2092.927361 | 30.579 | 0 |
| `connection_churn_realish` | `vanilla` | `app_txn_connect_prepared` | 2246.298257 | 28.491 | 0 |
| `connection_churn_realish` | `branch_process` | `app_txn_connect_prepared` | 2089.208378 | 30.634 | 0 |
| `connection_churn_realish` | `branch_threaded` | `app_txn_connect_prepared` | 1430.441259 | 44.741 | 0 |
| `connection_churn_realish` | `branch_pool_32` | `app_txn_connect_prepared` | 740.364616 | 86.444 | 0 |
| `connection_churn_realish` | `branch_pool_64` | `app_txn_connect_prepared` | 1143.404354 | 55.973 | 0 |
| `connection_churn_realish` | `branch_pool_128` | `app_txn_connect_prepared` | 1171.831855 | 54.615 | 0 |
| `connection_memory_idle` | `vanilla` | `select1_sleep_wake_1000ms_prepared` | 994.491644 | 1005.539 | 0 |
| `connection_memory_idle` | `branch_process` | `select1_sleep_wake_1000ms_prepared` | 993.473204 | 1006.5695 | 0 |
| `connection_memory_idle` | `branch_threaded` | `select1_sleep_wake_1000ms_prepared` | 990.4881155 | 1009.617 | 0 |
| `connection_memory_idle` | `branch_pool_64` | `select1_sleep_wake_1000ms_prepared` | 942.9938925 | 1060.457 | 0 |
| `connection_memory_idle` | `branch_pool_128` | `select1_sleep_wake_1000ms_prepared` | 957.74689 | 1044.178 | 0 |
| `connection_memory_idle` | `branch_pool_256` | `select1_sleep_wake_1000ms_prepared` | 955.7442005 | 1046.415 | 0 |
| `connection_memory_idle` | `branch_pool_512` | `select1_sleep_wake_1000ms_prepared` | 957.3513345 | 1044.6315 | 0 |
| `pinned_hot` | `vanilla` | `builtin_select_prepared` | 241311.871145 | 0.133 | 0 |
| `pinned_hot` | `vanilla` | `select1_prepared` | 353507.430340 | 0.091 | 0 |
| `pinned_hot` | `vanilla` | `bench_one_prepared` | 294319.405768 | 0.109 | 0 |
| `pinned_hot` | `vanilla` | `kv_read_prepared` | 253972.629926 | 0.126 | 0 |
| `pinned_hot` | `branch_process` | `builtin_select_prepared` | 224819.472158 | 0.142 | 0 |
| `pinned_hot` | `branch_process` | `select1_prepared` | 320657.287165 | 0.100 | 0 |
| `pinned_hot` | `branch_process` | `bench_one_prepared` | 274826.913089 | 0.116 | 0 |
| `pinned_hot` | `branch_process` | `kv_read_prepared` | 233939.343979 | 0.137 | 0 |
| `pinned_hot` | `branch_threaded` | `builtin_select_prepared` | 199645.561364 | 0.160 | 0 |
| `pinned_hot` | `branch_threaded` | `select1_prepared` | 336603.136083 | 0.095 | 0 |
| `pinned_hot` | `branch_threaded` | `bench_one_prepared` | 234590.418958 | 0.136 | 0 |
| `pinned_hot` | `branch_threaded` | `kv_read_prepared` | 207120.210193 | 0.154 | 0 |
| `pool_burst_10ms` | `branch_threaded` | `select1_sleep_wake_10ms_prepared` | 19480.822335 | 10.267 | 0 |
| `pool_burst_10ms` | `branch_pool_64` | `select1_sleep_wake_10ms_prepared` | 6091.409324 | 32.833 | 0 |
| `pool_burst_10ms` | `branch_pool_128` | `select1_sleep_wake_10ms_prepared` | 6193.077420 | 32.294 | 0 |
| `pool_burst_10ms` | `branch_pool_192` | `select1_sleep_wake_10ms_prepared` | 6089.903329 | 32.841 | 0 |
| `pool_idle_1000ms` | `vanilla` | `select1_sleep_wake_1000ms_prepared` | 199.708605 | 1001.459 | 0 |
| `pool_idle_1000ms` | `branch_process` | `select1_sleep_wake_1000ms_prepared` | 199.675328 | 1001.626 | 0 |
| `pool_idle_1000ms` | `branch_threaded` | `select1_sleep_wake_1000ms_prepared` | 199.677361 | 1001.616 | 0 |
| `pool_idle_1000ms` | `branch_pool_16` | `select1_sleep_wake_1000ms_prepared` | 196.035015 | 1020.226 | 0 |
| `pool_idle_1000ms` | `branch_pool_32` | `select1_sleep_wake_1000ms_prepared` | 198.081600 | 1009.685 | 0 |
| `pool_idle_1000ms` | `branch_pool_64` | `select1_sleep_wake_1000ms_prepared` | 199.015609 | 1004.946 | 0 |
| `pool_idle_1000ms` | `branch_pool_128` | `select1_sleep_wake_1000ms_prepared` | 198.998807 | 1005.031 | 0 |
| `pool_idle_100ms` | `vanilla` | `select1_sleep_wake_100ms_prepared` | 1984.743624 | 100.769 | 0 |
| `pool_idle_100ms` | `branch_process` | `select1_sleep_wake_100ms_prepared` | 1982.803938 | 100.867 | 0 |
| `pool_idle_100ms` | `branch_threaded` | `select1_sleep_wake_100ms_prepared` | 1984.259460 | 100.793 | 0 |
| `pool_idle_100ms` | `branch_pool_32` | `select1_sleep_wake_100ms_prepared` | 1958.555654 | 102.116 | 0 |
| `pool_idle_100ms` | `branch_pool_64` | `select1_sleep_wake_100ms_prepared` | 1977.650544 | 101.130 | 0 |
| `pool_idle_100ms` | `branch_pool_128` | `select1_sleep_wake_100ms_prepared` | 1977.482091 | 101.139 | 0 |
| `pool_idle_100ms` | `branch_pool_192` | `select1_sleep_wake_100ms_prepared` | 1977.653319 | 101.130 | 0 |
| `pool_realish_1000ms` | `vanilla` | `kv_read_sleep_wake_1000ms_prepared` | 199.691190 | 1001.546 | 0 |
| `pool_realish_1000ms` | `vanilla` | `app_txn_sleep_wake_1000ms_prepared` | 196.891415 | 1015.788 | 0 |
| `pool_realish_1000ms` | `branch_process` | `kv_read_sleep_wake_1000ms_prepared` | 199.644896 | 1001.779 | 0 |
| `pool_realish_1000ms` | `branch_process` | `app_txn_sleep_wake_1000ms_prepared` | 196.817926 | 1016.168 | 0 |
| `pool_realish_1000ms` | `branch_threaded` | `kv_read_sleep_wake_1000ms_prepared` | 199.549728 | 1002.256 | 0 |
| `pool_realish_1000ms` | `branch_threaded` | `app_txn_sleep_wake_1000ms_prepared` | 196.439757 | 1018.124 | 0 |
| `pool_realish_1000ms` | `branch_pool_16` | `kv_read_sleep_wake_1000ms_prepared` | 196.507125 | 1017.775 | 0 |
| `pool_realish_1000ms` | `branch_pool_16` | `app_txn_sleep_wake_1000ms_prepared` | 190.342710 | 1050.736 | 0 |
| `pool_realish_1000ms` | `branch_pool_32` | `kv_read_sleep_wake_1000ms_prepared` | 198.142666 | 1009.374 | 0 |
| `pool_realish_1000ms` | `branch_pool_32` | `app_txn_sleep_wake_1000ms_prepared` | 194.989199 | 1025.698 | 0 |
| `pool_realish_1000ms` | `branch_pool_64` | `kv_read_sleep_wake_1000ms_prepared` | 198.774729 | 1006.164 | 0 |
| `pool_realish_1000ms` | `branch_pool_64` | `app_txn_sleep_wake_1000ms_prepared` | 196.318810 | 1018.751 | 0 |
| `pool_realish_1000ms` | `branch_pool_128` | `kv_read_sleep_wake_1000ms_prepared` | 198.794423 | 1006.064 | 0 |
| `pool_realish_1000ms` | `branch_pool_128` | `app_txn_sleep_wake_1000ms_prepared` | 196.191630 | 1019.411 | 0 |
| `pool_realish_100ms` | `vanilla` | `kv_read_sleep_wake_100ms_prepared` | 1983.744317 | 100.819 | 0 |
| `pool_realish_100ms` | `vanilla` | `app_txn_sleep_wake_100ms_prepared` | 1816.717162 | 110.089 | 0 |
| `pool_realish_100ms` | `vanilla` | `app_mixed_sleep_wake_100ms_prepared` | 1836.323210 | 108.913 | 0 |
| `pool_realish_100ms` | `branch_process` | `kv_read_sleep_wake_100ms_prepared` | 1979.825082 | 101.019 | 0 |
| `pool_realish_100ms` | `branch_process` | `app_txn_sleep_wake_100ms_prepared` | 1814.238489 | 110.239 | 0 |
| `pool_realish_100ms` | `branch_process` | `app_mixed_sleep_wake_100ms_prepared` | 1834.903801 | 108.998 | 0 |
| `pool_realish_100ms` | `branch_threaded` | `kv_read_sleep_wake_100ms_prepared` | 1978.517220 | 101.086 | 0 |
| `pool_realish_100ms` | `branch_threaded` | `app_txn_sleep_wake_100ms_prepared` | 1819.091188 | 109.945 | 0 |
| `pool_realish_100ms` | `branch_threaded` | `app_mixed_sleep_wake_100ms_prepared` | 1841.325534 | 108.617 | 0 |
| `pool_realish_100ms` | `branch_pool_32` | `kv_read_sleep_wake_100ms_prepared` | 1963.124318 | 101.878 | 0 |
| `pool_realish_100ms` | `branch_pool_32` | `app_txn_sleep_wake_100ms_prepared` | 1761.253905 | 113.555 | 0 |
| `pool_realish_100ms` | `branch_pool_32` | `app_mixed_sleep_wake_100ms_prepared` | 1219.956758 | 163.940 | 0 |
| `pool_realish_100ms` | `branch_pool_64` | `kv_read_sleep_wake_100ms_prepared` | 1974.099514 | 101.312 | 0 |
| `pool_realish_100ms` | `branch_pool_64` | `app_txn_sleep_wake_100ms_prepared` | 1861.570141 | 107.436 | 0 |
| `pool_realish_100ms` | `branch_pool_64` | `app_mixed_sleep_wake_100ms_prepared` | 1829.443023 | 109.323 | 0 |
| `pool_realish_100ms` | `branch_pool_128` | `kv_read_sleep_wake_100ms_prepared` | 1972.988486 | 101.369 | 0 |
| `pool_realish_100ms` | `branch_pool_128` | `app_txn_sleep_wake_100ms_prepared` | 1862.348660 | 107.391 | 0 |
| `pool_realish_100ms` | `branch_pool_128` | `app_mixed_sleep_wake_100ms_prepared` | 1821.892538 | 109.776 | 0 |
| `pool_realish_100ms` | `branch_pool_192` | `kv_read_sleep_wake_100ms_prepared` | 1974.387583 | 101.297 | 0 |
| `pool_realish_100ms` | `branch_pool_192` | `app_txn_sleep_wake_100ms_prepared` | 1854.851747 | 107.825 | 0 |
| `pool_realish_100ms` | `branch_pool_192` | `app_mixed_sleep_wake_100ms_prepared` | 1829.088663 | 109.344 | 0 |
| `pool_scale_1000_idle` | `branch_threaded` | `select1_sleep_wake_1000ms_prepared` | 996.639398 | 1003.372 | 0 |
| `pool_scale_1000_idle` | `branch_pool_64` | `select1_sleep_wake_1000ms_prepared` | 961.133350 | 1040.438 | 0 |
| `pool_scale_1000_idle` | `branch_pool_128` | `select1_sleep_wake_1000ms_prepared` | 978.117700 | 1022.372 | 0 |
| `pool_scale_1000_idle` | `branch_pool_256` | `select1_sleep_wake_1000ms_prepared` | 977.409270 | 1023.113 | 0 |
| `pool_scale_1000_idle` | `branch_pool_512` | `select1_sleep_wake_1000ms_prepared` | 978.006237 | 1022.488 | 0 |
| `pool_scale_1000_realish` | `vanilla` | `kv_read_sleep_wake_1000ms_prepared` | 992.918038 | 1007.1325 | 0 |
| `pool_scale_1000_realish` | `branch_threaded` | `kv_read_sleep_wake_1000ms_prepared` | 985.155781 | 1015.069 | 0 |
| `pool_scale_1000_realish` | `branch_pool_64` | `kv_read_sleep_wake_1000ms_prepared` | 932.1103975 | 1072.839 | 0 |
| `pool_scale_1000_realish` | `branch_pool_128` | `kv_read_sleep_wake_1000ms_prepared` | 929.219954 | 1076.195 | 0 |
| `pool_scale_1000_realish` | `branch_pool_256` | `kv_read_sleep_wake_1000ms_prepared` | 931.696427 | 1073.313 | 0 |
| `pool_scale_1000_realish` | `branch_pool_512` | `kv_read_sleep_wake_1000ms_prepared` | 934.017647 | 1070.6495 | 0 |
| `pool_stateful_1000ms` | `vanilla` | `stateful_temp_sleep_wake_1000ms_prepared` | 99.7167815 | 1002.84 | 0 |
| `pool_stateful_1000ms` | `branch_process` | `stateful_temp_sleep_wake_1000ms_prepared` | 99.693457 | 1003.075 | 0 |
| `pool_stateful_1000ms` | `branch_threaded` | `stateful_temp_sleep_wake_1000ms_prepared` | 99.7014335 | 1002.9945 | 0 |
| `pool_stateful_1000ms` | `branch_pool_16` | `stateful_temp_sleep_wake_1000ms_prepared` | 98.21898 | 1018.1335 | 0 |
| `pool_stateful_1000ms` | `branch_pool_32` | `stateful_temp_sleep_wake_1000ms_prepared` | 98.9102855 | 1011.018 | 0 |
| `pool_stateful_1000ms` | `branch_pool_64` | `stateful_temp_sleep_wake_1000ms_prepared` | 99.0306455 | 1009.789 | 0 |

Memory footprint results
------------------------

The table below records the comparable columns from each profile's
`memory_footprint.tsv`. The raw output directory contains the full RSS, PSS,
shared, private, sampled process/thread, map, and protocol-park attribution
files.

`Pooled idle PSS KB` and `Pooled carrier PSS KB` are fitted estimates derived
from the pool-size sweep for that profile. Negative fitted carrier values in
the simple connection-churn profile indicate that this profile is too transient
for the pooled linear fit to be meaningful.

| Profile | Workload | Lane | Clients | Max proc | Max threads | PSS/client KB | Private/client KB | Pooled idle PSS KB | Pooled carrier PSS KB |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `connection_churn` | `select1_connect_prepared` | `vanilla` | 64 | 58 | 39 | 372.45 | 357.56 | 1672.20 | -1144.91 |
| `connection_churn` | `select1_connect_prepared` | `branch_process` | 64 | 61 | 44 | 355.92 | 347.25 | 1672.20 | -1144.91 |
| `connection_churn` | `select1_connect_prepared` | `branch_threaded` | 64 | 1 | 66 | 523.39 | 521.81 | 1672.20 | -1144.91 |
| `connection_churn` | `select1_connect_prepared` | `branch_pool_32` | 64 | 1 | 41 | 1136.20 | 1133.12 | 1672.20 | -1144.91 |
| `connection_churn` | `select1_connect_prepared` | `branch_pool_64` | 64 | 1 | 66 | 670.64 | 669.19 | 1672.20 | -1144.91 |
| `connection_churn` | `select1_connect_prepared` | `branch_pool_128` | 64 | 1 | 66 | 687.38 | 685.62 | 1672.20 | -1144.91 |
| `connection_churn_realish` | `app_txn_connect_prepared` | `vanilla` | 64 | 73 | 54 | 980.78 | 808.38 | 1085.38 | 238.75 |
| `connection_churn_realish` | `app_txn_connect_prepared` | `branch_process` | 64 | 71 | 53 | 914.17 | 765.31 | 1085.38 | 238.75 |
| `connection_churn_realish` | `app_txn_connect_prepared` | `branch_threaded` | 64 | 1 | 72 | 1030.47 | 1026.38 | 1085.38 | 238.75 |
| `connection_churn_realish` | `app_txn_connect_prepared` | `branch_pool_32` | 64 | 1 | 41 | 1192.88 | 1189.81 | 1085.38 | 238.75 |
| `connection_churn_realish` | `app_txn_connect_prepared` | `branch_pool_64` | 64 | 1 | 73 | 1332.86 | 1330.00 | 1085.38 | 238.75 |
| `connection_churn_realish` | `app_txn_connect_prepared` | `branch_pool_128` | 64 | 1 | 84 | 1349.66 | 1346.56 | 1085.38 | 238.75 |
| `connection_memory_idle` | `select1_sleep_wake_1000ms_prepared` | `vanilla` | 1000 | 1008 | 1008 | 1211.77 | 1197.78 | 505.89 | 483.09 |
| `connection_memory_idle` | `select1_sleep_wake_1000ms_prepared` | `branch_process` | 1000 | 1009 | 1008 | 1061.96 | 1042.69 | 505.89 | 483.09 |
| `connection_memory_idle` | `select1_sleep_wake_1000ms_prepared` | `branch_threaded` | 1000 | 1 | 1008 | 961.12 | 957.84 | 505.89 | 483.09 |
| `connection_memory_idle` | `select1_sleep_wake_1000ms_prepared` | `branch_pool_64` | 1000 | 1 | 72 | 536.70 | 533.41 | 505.89 | 483.09 |
| `connection_memory_idle` | `select1_sleep_wake_1000ms_prepared` | `branch_pool_128` | 1000 | 1 | 121 | 560.90 | 557.63 | 505.89 | 483.09 |
| `connection_memory_idle` | `select1_sleep_wake_1000ms_prepared` | `branch_pool_256` | 1000 | 1 | 122 | 559.85 | 556.56 | 505.89 | 483.09 |
| `connection_memory_idle` | `select1_sleep_wake_1000ms_prepared` | `branch_pool_512` | 1000 | 1 | 116 | 558.87 | 555.61 | 505.89 | 483.09 |
| `pool_burst_10ms` | `select1_sleep_wake_10ms_prepared` | `branch_threaded` | 200 | 1 | 208 | 636.28 | 633.00 | 137.22 | 1518.00 |
| `pool_burst_10ms` | `select1_sleep_wake_10ms_prepared` | `branch_pool_64` | 200 | 1 | 71 | 599.88 | 596.88 | 137.22 | 1518.00 |
| `pool_burst_10ms` | `select1_sleep_wake_10ms_prepared` | `branch_pool_128` | 200 | 1 | 72 | 607.80 | 604.90 | 137.22 | 1518.00 |
| `pool_burst_10ms` | `select1_sleep_wake_10ms_prepared` | `branch_pool_192` | 200 | 1 | 71 | 600.54 | 597.16 | 137.22 | 1518.00 |
| `pool_idle_1000ms` | `select1_sleep_wake_1000ms_prepared` | `vanilla` | 200 | 208 | 208 | 1219.39 | 1182.90 | 545.99 | 228.51 |
| `pool_idle_1000ms` | `select1_sleep_wake_1000ms_prepared` | `branch_process` | 200 | 208 | 208 | 1084.05 | 1045.56 | 545.99 | 228.51 |
| `pool_idle_1000ms` | `select1_sleep_wake_1000ms_prepared` | `branch_threaded` | 200 | 1 | 208 | 656.30 | 652.90 | 545.99 | 228.51 |
| `pool_idle_1000ms` | `select1_sleep_wake_1000ms_prepared` | `branch_pool_16` | 200 | 1 | 24 | 559.65 | 556.30 | 545.99 | 228.51 |
| `pool_idle_1000ms` | `select1_sleep_wake_1000ms_prepared` | `branch_pool_32` | 200 | 1 | 40 | 583.74 | 580.34 | 545.99 | 228.51 |
| `pool_idle_1000ms` | `select1_sleep_wake_1000ms_prepared` | `branch_pool_64` | 200 | 1 | 73 | 615.32 | 611.98 | 545.99 | 228.51 |
| `pool_idle_1000ms` | `select1_sleep_wake_1000ms_prepared` | `branch_pool_128` | 200 | 1 | 72 | 619.50 | 616.10 | 545.99 | 228.51 |
| `pool_idle_100ms` | `select1_sleep_wake_100ms_prepared` | `vanilla` | 200 | 208 | 208 | 1208.63 | 1174.34 | 548.11 | 227.46 |
| `pool_idle_100ms` | `select1_sleep_wake_100ms_prepared` | `branch_process` | 200 | 208 | 208 | 1083.64 | 1044.70 | 548.11 | 227.46 |
| `pool_idle_100ms` | `select1_sleep_wake_100ms_prepared` | `branch_threaded` | 200 | 1 | 208 | 636.88 | 633.62 | 548.11 | 227.46 |
| `pool_idle_100ms` | `select1_sleep_wake_100ms_prepared` | `branch_pool_32` | 200 | 1 | 40 | 582.23 | 578.90 | 548.11 | 227.46 |
| `pool_idle_100ms` | `select1_sleep_wake_100ms_prepared` | `branch_pool_64` | 200 | 1 | 72 | 622.63 | 619.28 | 548.11 | 227.46 |
| `pool_idle_100ms` | `select1_sleep_wake_100ms_prepared` | `branch_pool_128` | 200 | 1 | 72 | 613.18 | 609.92 | 548.11 | 227.46 |
| `pool_idle_100ms` | `select1_sleep_wake_100ms_prepared` | `branch_pool_192` | 200 | 1 | 71 | 620.04 | 616.72 | 548.11 | 227.46 |
| `pool_realish_1000ms` | `kv_read_sleep_wake_1000ms_prepared` | `vanilla` | 200 | 209 | 209 | 1395.29 | 1329.40 | 792.73 | 102.22 |
| `pool_realish_1000ms` | `kv_read_sleep_wake_1000ms_prepared` | `branch_process` | 200 | 208 | 208 | 1276.58 | 1188.22 | 792.73 | 102.22 |
| `pool_realish_1000ms` | `kv_read_sleep_wake_1000ms_prepared` | `branch_threaded` | 200 | 1 | 208 | 818.50 | 815.24 | 792.73 | 102.22 |
| `pool_realish_1000ms` | `kv_read_sleep_wake_1000ms_prepared` | `branch_pool_16` | 200 | 1 | 26 | 806.20 | 802.72 | 792.73 | 102.22 |
| `pool_realish_1000ms` | `kv_read_sleep_wake_1000ms_prepared` | `branch_pool_32` | 200 | 1 | 41 | 799.52 | 796.26 | 792.73 | 102.22 |
| `pool_realish_1000ms` | `kv_read_sleep_wake_1000ms_prepared` | `branch_pool_64` | 200 | 1 | 72 | 835.33 | 831.98 | 792.73 | 102.22 |
| `pool_realish_1000ms` | `kv_read_sleep_wake_1000ms_prepared` | `branch_pool_128` | 200 | 1 | 73 | 817.25 | 813.90 | 792.73 | 102.22 |
| `pool_realish_1000ms` | `app_txn_sleep_wake_1000ms_prepared` | `vanilla` | 200 | 208 | 208 | 1552.86 | 1391.98 | 1017.53 | 188.68 |
| `pool_realish_1000ms` | `app_txn_sleep_wake_1000ms_prepared` | `branch_process` | 200 | 208 | 208 | 1425.51 | 1273.62 | 1017.53 | 188.68 |
| `pool_realish_1000ms` | `app_txn_sleep_wake_1000ms_prepared` | `branch_threaded` | 200 | 1 | 208 | 1018.28 | 1014.88 | 1017.53 | 188.68 |
| `pool_realish_1000ms` | `app_txn_sleep_wake_1000ms_prepared` | `branch_pool_16` | 200 | 1 | 24 | 1027.40 | 1024.00 | 1017.53 | 188.68 |
| `pool_realish_1000ms` | `app_txn_sleep_wake_1000ms_prepared` | `branch_pool_32` | 200 | 1 | 41 | 1050.64 | 1047.24 | 1017.53 | 188.68 |
| `pool_realish_1000ms` | `app_txn_sleep_wake_1000ms_prepared` | `branch_pool_64` | 200 | 1 | 72 | 1071.93 | 1068.54 | 1017.53 | 188.68 |
| `pool_realish_1000ms` | `app_txn_sleep_wake_1000ms_prepared` | `branch_pool_128` | 200 | 1 | 73 | 1079.59 | 1076.52 | 1017.53 | 188.68 |
| `pool_realish_100ms` | `kv_read_sleep_wake_100ms_prepared` | `vanilla` | 200 | 209 | 209 | 1400.52 | 1325.94 | 782.38 | 105.65 |
| `pool_realish_100ms` | `kv_read_sleep_wake_100ms_prepared` | `branch_process` | 200 | 208 | 208 | 1273.08 | 1178.00 | 782.38 | 105.65 |
| `pool_realish_100ms` | `kv_read_sleep_wake_100ms_prepared` | `branch_threaded` | 200 | 1 | 210 | 890.77 | 887.42 | 782.38 | 105.65 |
| `pool_realish_100ms` | `kv_read_sleep_wake_100ms_prepared` | `branch_pool_32` | 200 | 1 | 41 | 798.13 | 794.76 | 782.38 | 105.65 |
| `pool_realish_100ms` | `kv_read_sleep_wake_100ms_prepared` | `branch_pool_64` | 200 | 1 | 72 | 815.08 | 811.64 | 782.38 | 105.65 |
| `pool_realish_100ms` | `kv_read_sleep_wake_100ms_prepared` | `branch_pool_128` | 200 | 1 | 72 | 818.27 | 814.92 | 782.38 | 105.65 |
| `pool_realish_100ms` | `kv_read_sleep_wake_100ms_prepared` | `branch_pool_192` | 200 | 1 | 74 | 812.67 | 809.30 | 782.38 | 105.65 |
| `pool_realish_100ms` | `app_txn_sleep_wake_100ms_prepared` | `vanilla` | 200 | 208 | 208 | 1538.73 | 1389.70 | 1015.25 | 174.63 |
| `pool_realish_100ms` | `app_txn_sleep_wake_100ms_prepared` | `branch_process` | 200 | 208 | 208 | 1431.88 | 1278.08 | 1015.25 | 174.63 |
| `pool_realish_100ms` | `app_txn_sleep_wake_100ms_prepared` | `branch_threaded` | 200 | 1 | 209 | 1109.66 | 1106.26 | 1015.25 | 174.63 |
| `pool_realish_100ms` | `app_txn_sleep_wake_100ms_prepared` | `branch_pool_32` | 200 | 1 | 40 | 1040.60 | 1037.20 | 1015.25 | 174.63 |
| `pool_realish_100ms` | `app_txn_sleep_wake_100ms_prepared` | `branch_pool_64` | 200 | 1 | 73 | 1075.37 | 1071.94 | 1015.25 | 174.63 |
| `pool_realish_100ms` | `app_txn_sleep_wake_100ms_prepared` | `branch_pool_128` | 200 | 1 | 72 | 1068.24 | 1064.82 | 1015.25 | 174.63 |
| `pool_realish_100ms` | `app_txn_sleep_wake_100ms_prepared` | `branch_pool_192` | 200 | 1 | 74 | 1066.24 | 1062.82 | 1015.25 | 174.63 |
| `pool_realish_100ms` | `app_mixed_sleep_wake_100ms_prepared` | `vanilla` | 200 | 208 | 208 | 1609.98 | 1484.60 | 1139.34 | 190.54 |
| `pool_realish_100ms` | `app_mixed_sleep_wake_100ms_prepared` | `branch_process` | 200 | 208 | 208 | 1508.88 | 1368.62 | 1139.34 | 190.54 |
| `pool_realish_100ms` | `app_mixed_sleep_wake_100ms_prepared` | `branch_threaded` | 200 | 1 | 209 | 1240.80 | 1237.42 | 1139.34 | 190.54 |
| `pool_realish_100ms` | `app_mixed_sleep_wake_100ms_prepared` | `branch_pool_32` | 200 | 1 | 40 | 1166.68 | 1163.34 | 1139.34 | 190.54 |
| `pool_realish_100ms` | `app_mixed_sleep_wake_100ms_prepared` | `branch_pool_64` | 200 | 1 | 72 | 1211.26 | 1208.02 | 1139.34 | 190.54 |
| `pool_realish_100ms` | `app_mixed_sleep_wake_100ms_prepared` | `branch_pool_128` | 200 | 1 | 73 | 1189.99 | 1186.42 | 1139.34 | 190.54 |
| `pool_realish_100ms` | `app_mixed_sleep_wake_100ms_prepared` | `branch_pool_192` | 200 | 1 | 73 | 1195.19 | 1191.88 | 1139.34 | 190.54 |
| `pool_scale_1000_idle` | `select1_sleep_wake_1000ms_prepared` | `branch_threaded` | 1000 | 1 | 1008 | 703.64 | 700.41 | 502.89 | 217.08 |
| `pool_scale_1000_idle` | `select1_sleep_wake_1000ms_prepared` | `branch_pool_64` | 1000 | 1 | 72 | 516.35 | 513.22 | 502.89 | 217.08 |
| `pool_scale_1000_idle` | `select1_sleep_wake_1000ms_prepared` | `branch_pool_128` | 1000 | 1 | 122 | 527.21 | 524.01 | 502.89 | 217.08 |
| `pool_scale_1000_idle` | `select1_sleep_wake_1000ms_prepared` | `branch_pool_256` | 1000 | 1 | 124 | 527.87 | 524.67 | 502.89 | 217.08 |
| `pool_scale_1000_idle` | `select1_sleep_wake_1000ms_prepared` | `branch_pool_512` | 1000 | 1 | 124 | 527.40 | 524.20 | 502.89 | 217.08 |
| `pool_scale_1000_realish` | `kv_read_sleep_wake_1000ms_prepared` | `vanilla` | 1000 | 1008 | 1008 | 1404.81 | 1371.92 | 734.01 | 168.07 |
| `pool_scale_1000_realish` | `kv_read_sleep_wake_1000ms_prepared` | `branch_threaded` | 1000 | 1 | 1009 | 920.65 | 917.44 | 734.01 | 168.07 |
| `pool_scale_1000_realish` | `kv_read_sleep_wake_1000ms_prepared` | `branch_pool_64` | 1000 | 1 | 73 | 744.47 | 741.26 | 734.01 | 168.07 |
| `pool_scale_1000_realish` | `kv_read_sleep_wake_1000ms_prepared` | `branch_pool_128` | 1000 | 1 | 121 | 752.24 | 749.02 | 734.01 | 168.07 |
| `pool_scale_1000_realish` | `kv_read_sleep_wake_1000ms_prepared` | `branch_pool_256` | 1000 | 1 | 122 | 752.06 | 748.83 | 734.01 | 168.07 |
| `pool_scale_1000_realish` | `kv_read_sleep_wake_1000ms_prepared` | `branch_pool_512` | 1000 | 1 | 123 | 753.82 | 750.60 | 734.01 | 168.07 |
| `pool_stateful_1000ms` | `stateful_temp_sleep_wake_1000ms_prepared` | `vanilla` | 100 | 109 | 109 | 1741.35 | 1549.32 | 1402.14 | 306.28 |
| `pool_stateful_1000ms` | `stateful_temp_sleep_wake_1000ms_prepared` | `branch_process` | 100 | 108 | 108 | 1688.97 | 1412.00 | 1402.14 | 306.28 |
| `pool_stateful_1000ms` | `stateful_temp_sleep_wake_1000ms_prepared` | `branch_threaded` | 100 | 1 | 108 | 1774.22 | 1768.88 | 1402.14 | 306.28 |
| `pool_stateful_1000ms` | `stateful_temp_sleep_wake_1000ms_prepared` | `branch_pool_16` | 100 | 1 | 25 | 1456.37 | 1451.04 | 1402.14 | 306.28 |
| `pool_stateful_1000ms` | `stateful_temp_sleep_wake_1000ms_prepared` | `branch_pool_32` | 100 | 1 | 41 | 1498.39 | 1493.24 | 1402.14 | 306.28 |
| `pool_stateful_1000ms` | `stateful_temp_sleep_wake_1000ms_prepared` | `branch_pool_64` | 100 | 1 | 54 | 1545.69 | 1540.36 | 1402.14 | 306.28 |

Raw artifact guide
------------------

Each profile directory contains:

| File | Contents |
| --- | --- |
| `tps.tsv` | Median TPS and latency by lane/workload. |
| `samples.tsv` | Per-run TPS and latency samples. |
| `ratios.tsv` | Ratios against vanilla, or the first selected lane when vanilla is absent. |
| `server_resources.tsv` | Max sampled server process/thread counts and aggregate memory by workload. |
| `server_resource_samples.tsv` | Raw sampled process-tree memory observations. |
| `server_resource_baselines.tsv` | Idle server resource samples before workload clients. |
| `resource_efficiency.tsv` | Derived TPS/thread and memory/client metrics. |
| `memory_footprint.tsv` | Baseline-adjusted memory footprint estimates. |
| `server_process_rollups.tsv` | Per-process `smaps_rollup` rows when memory detail is enabled. |
| `server_memory_map_summary.tsv` | One detailed `smaps` category snapshot per run when memory detail is enabled. |
| `server_memory_map_path_summary.tsv` | Detailed `smaps` totals by category and mapped path. |
| `server_thread_stacks.tsv` | Per-thread stack visibility for detailed snapshots. |
| `protocol_park_memory.tsv` | Parsed per-park memory-context attribution rows when protocol park logging is enabled. |
| `protocol_park_guc_memory.tsv` | Parsed per-park GUC memory attribution rows when protocol park logging is enabled. |
| `protocol_park_context_memory.tsv` | Bounded per-backend memory-context tree rows emitted at committed protocol-read parks. |
| `protocol_park_*_summary.tsv` | Median per-park memory summaries by lane/workload. |

Current interpretation
----------------------

Pooled protocol carriers now show the intended shape for large quiet connection
populations: much lower server thread counts and materially lower per-client
PSS than pinned threads, process mode, or vanilla. The strongest current proof
point is the 1000-client idle memory profile, where pooled lanes stay around
943 TPS to 958 TPS and about 537 KB to 561 KB PSS per client, compared with
990 TPS and 961 KB PSS per client for pinned threads.

The branch is not yet back to the earlier hot-path speed position. The hot
tiny-query profile shows branch process and pinned threads behind vanilla, and
the connection-churn profiles show pooled mode behind all non-pooled lanes.
Those remain the next performance targets.
