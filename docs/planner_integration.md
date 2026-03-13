# Orvos Planner Integration

## Overview

PostgreSQL's query planner estimates I/O and CPU costs for each possible query
plan, then picks the cheapest. The default cost model assumes heap storage:
every sequential scan reads every page regardless of which columns the query
actually needs. For a columnar engine like Orvos this is wrong -- a query that
touches 2 of 10 columns should read far less data than one that touches all 10.

The planner hooks in `orvos_planner.c` solve this by:

1. Detecting which columns each query accesses.
2. Reducing the estimated page count proportionally.
3. Accounting for fixed overhead (TID tree, metadata) that persists even when
   few columns are read.

Without these hooks the optimizer over-estimates Orvos scan costs, which can
cause it to choose nested-loop or index plans when a columnar sequential scan
would be cheaper.

## Architecture

### Hook chain

```
_PG_init()
  -> orvos_planner_init()
       saves prev_get_relation_info_hook
       installs orvos_get_relation_info
```

When PostgreSQL plans a query it calls `get_relation_info()` for every base
relation. Our hook intercepts that call:

```
get_relation_info_hook(root, relid, inhparent, rel)
  -> chain to previous hook (if any)
  -> open relation, check is_orvos_relation()
  -> if Orvos: create_orvos_rel_stats()
       pull_varattnos() from target list
       pull_varattnos() from base restriction quals
       compute column_selectivity = accessed / total
       store OrvosRelStats in rel->fdw_private
```

### Size estimation callback

`orvosam_relation_estimate_size()` in `orvos_handler.c` is the Table AM
callback that the planner calls to get page and tuple estimates. After
computing the standard heap-style estimate it applies a columnar adjustment:

```
orvos_calculate_cost_factors(column_selectivity, compression_ratio,
                             &io_factor, &cpu_factor)

adjusted_pages = ceil(curpages * io_factor)
```

Currently the callback uses conservative defaults (60% column selectivity,
2.5x compression) because the stats lookup (`orvos_get_relation_stats`) is
not yet wired up. Once the hash-table bridge between the hook and the callback
is implemented, the planner will use per-query column access patterns.

## Cost Model

### I/O reduction formula

```
io_factor = 0.2 + 0.8 * column_selectivity
```

The 0.2 term represents fixed overhead: even when reading a single column,
Orvos must traverse the TID tree and read page headers. The 0.8 term scales
linearly with the fraction of columns accessed.

Examples:

| Columns accessed | Selectivity | io_factor | I/O reduction |
|-----------------|-------------|-----------|---------------|
| 1 of 10         | 0.10        | 0.28      | 72%           |
| 2 of 10         | 0.20        | 0.36      | 64%           |
| 5 of 10         | 0.50        | 0.60      | 40%           |
| 8 of 10         | 0.80        | 1.00      | 0% (cutoff)   |
| 10 of 10        | 1.00        | 1.00      | 0%            |

When column selectivity >= 0.8, no reduction is applied. At that point the
overhead of columnar access offsets any savings from skipping columns.

### CPU decompression overhead

Compressed columns add CPU cost during decompression. The constant
`ORVOS_DECOMPRESSION_CPU_FACTOR` (0.3) represents zstd decompression overhead
as a fraction of per-tuple CPU cost. This is not yet applied in the size
estimation callback but is available for future integration.

### Default constants

| Constant                          | Value | Meaning                                 |
|----------------------------------|-------|-----------------------------------------|
| `ORVOS_DEFAULT_COMPRESSION_RATIO` | 2.5   | Conservative average across column types |
| `ORVOS_DECOMPRESSION_CPU_FACTOR`  | 0.3   | CPU overhead fraction for decompression  |
| `ORVOS_MIN_COLUMN_SELECTIVITY`    | 0.8   | Cutoff above which no I/O reduction applies |

## Queries That Benefit

The planner integration provides the most benefit when:

- **Wide tables, few columns accessed**: `SELECT col1, col2 FROM wide_table`
  on a 20-column table gets a significant I/O cost reduction.
- **Analytical aggregations**: `SELECT avg(price) FROM sales` reads one column
  instead of the full row.
- **Filtered scans on narrow projections**: `SELECT name FROM users WHERE id < 100`
  touches 2 columns out of many.

Queries that access most or all columns (`SELECT *`) see no benefit. The
cutoff at 80% selectivity ensures the planner does not underestimate costs
for wide reads.

## Current Limitations

1. **Stats lookup not wired up**: `orvos_get_relation_stats()` returns NULL.
   The hook stores stats in `rel->fdw_private`, but the size estimation
   callback cannot access them because it receives only a `Relation`, not a
   `RelOptInfo`. A per-planning-cycle hash table (Oid -> OrvosRelStats) is
   needed to bridge this gap.

2. **Conservative defaults**: Without the stats bridge, every Orvos table gets
   the same 60% column selectivity estimate regardless of the actual query.

3. **No per-column compression stats**: The compression ratio is a fixed
   constant. ANALYZE enhancements (in progress) will collect actual per-column
   ratios and store them for the planner to use.

4. **CPU factor unused**: The decompression CPU cost is calculated but not
   applied in the current size estimation callback.

5. **No join-aware optimization**: The hook only processes base relations.
   Join-level column pruning is handled by PostgreSQL's existing projection
   pushdown, which works correctly but does not get Orvos-specific cost
   adjustments.

## Source Files

| File | Purpose |
|------|---------|
| `src/backend/access/orvos/orvos_planner.c` | Hook implementation and cost model |
| `src/include/access/orvos_planner.h` | Constants, OrvosRelStats struct, public API |
| `src/backend/access/orvos/orvos_handler.c` | Size estimation callback, module init |
