# Incremental maintenance of materialized views, in core

Status: plan for review, revision 3.  Nothing is implemented.  Branch
`ivmdf` on `gburd/postgres`.  Line references are to `origin/master` at
`84b53981825`.

Revision 2 followed an independent review of revision 1 against the
source.  It changed read-time maintenance from writing shared view rows
to a private overlay, defined the class of a view by its delta rule
rather than by its table count, restricted per-key locking to joins that
share one key, and added sections on invalidation, privileges and
failure.  Section 13 lists what changed and why.

Revision 3 records three decisions: the overlay gets its own executor
node, `IvmOverlay` (section 5.5); a maintenance error aborts the writer,
with no stale-instead option (section 8); creating an incremental view
requires `MAINTAIN` or ownership on every base table, not `TRIGGER`
(section 2).

This document replaces the libdtfl + UNDO design that used to be in this
file and in `~/ws/libdtfl/dev/notes/pg-incremental-views.md`.

## 1. Goal and ground rules

An incremental materialized view, read by any statement, equals its
defining query evaluated over the base tables as that statement's
snapshot sees them, including the reading transaction's own uncommitted
changes.

Ground rules (fixed):

- A core feature.  No extension, no `shared_preload_libraries`, no
  background worker.
- The defining query is analysed once, at `CREATE MATERIALIZED VIEW`.
  Maintenance works from stored parse trees and never parses SQL text.
- No triggers, no reading WAL, no logical decoding.  The transaction that
  changes a base table maintains the view, at a read of the view or at
  commit, whichever comes first.
- No new storage.  The view and its state live in relations of whatever
  table access method the view was created with, normally heap.  Nothing
  depends on UNDO, FLUX or any other non-core AM.  WAL carries only
  ordinary heap and index records.
- Maintenance locks are held for as short a time as correctness allows.
- Start with views over one table with invertible aggregates.  Design so
  every query shape a materialized view accepts today can follow.

## 2. Syntax, catalog, privileges

```sql
CREATE MATERIALIZED VIEW v WITH (incremental_view_maintenance) AS SELECT ...;
```

The reloption name matches Nagata's v40 patch (commitfest 4337), which
also chose a reloption over new syntax.  In the first version it can't
be changed after CREATE.

New catalog `pg_matview_ivm`, one row per incremental view:

| column | contents |
|---|---|
| `ivmrelid` | the view |
| `ivmclass` | 1 or 2, section 6 |
| `ivmprogram` | `pg_node_tree`: the compiled maintenance program |
| `ivmbaserels` | `oid[]`: base relations, sorted |
| `ivmkeyidx` | the view's key index |
| `ivmstateversion` | format version of the stored aggregate states (section 3) |
| `ivmstale` | true when the view must be refreshed before use (section 8) |

`pg_depend` records the view's dependency on each base relation, on
every function, operator, view and policy its program uses (via
`recordDependencyOnExpr`, `dependency.c:1572`), and on each owned index.

`pg_class` gets `relhasivm` on every base relation of an incremental
view, in the style of `relhastriggers`.  The relcache keeps the list of
dependent views.  Capture costs nothing on tables without the flag.

**Privileges.**  An incremental view makes its query run inside other
users' transactions, and a failing maintenance aborts their commit
(section 8), so whoever creates one controls which writes to the base
tables can fail.  Creating one therefore requires, on every base table,
`SELECT` and `MAINTAIN` (or ownership, which implies it), checked as
`CREATE INDEX` on another's table is (`indexcmds.c:3214`).  `MAINTAIN`
already permits the `ShareLock` CREATE takes (`lockcmds.c:308`) and is
what the `pg_maintain` role grants.  If the compiler must create an
index on a base table (section 3, owned indexes), the creator must own
that table, as for `CREATE INDEX` (`utility.c:1482`,
`RangeVarCallbackOwnsRelation`); otherwise CREATE fails and names the
index the table owner can create first.  The `TRIGGER` privilege plays
no part: nothing here is a trigger.

## 3. CREATE: compiling the view

`ExecCreateTableAs` stores the analysed, unrewritten Query for the
view's `_RETURN` rule (`parser/analyze.c:3237`).  The compiler takes that Query
through rewrite (views inlined, row security applied as the view owner)
and produces the program.  It rejects anything it can't maintain with an
error naming the construct.

**The program** is a tree of operators with their delta rules, in the
vocabulary of differential dataflow: scan with pushed-down quals,
project, inner equi-join, aggregate, union all, distinct, antijoin.
Each operator records the base-table columns it reads, its join keys and
the index each probe uses, and how each aggregate is maintained.

**Class.**  A view is class 1 if its delta rule reads nothing but the
captured change and the view's own rows: one base relation appearing
once, no join, no subquery over a base relation, no recompute path.
Everything else is class 2, including self-joins and views whose
aggregates may be recomputed from the base table (section 5.3): min and
max, and sum or avg over numeric, whose inverse can give up.  Section 6
explains why the distinction is the delta rule's inputs, not the number
of tables.

**Aggregates.**  An aggregate is maintained incrementally if:

1. it has an inverse transition function (`aggminvtransfn`);
2. its moving-aggregate state can be stored: `aggmtranstype` is an SQL
   type, or is `internal` with `aggmtransfn = aggtransfn` and an
   `aggserialfn`/`aggdeserialfn` pair;
3. its inverse doesn't depend on removal order (below).

In the current catalog 40 aggregates have an inverse and 37 meet
condition 2: `count(*)` and `count(x)`; `sum` and `avg` over `int2`,
`int4`, `int8`, `numeric` and `interval`; `sum(money)`; and the
`var_*`/`stddev_*` family over integers and `numeric`.  `bool_and`,
`bool_or` and `every` have inverses, but their moving state is
`internal` with no serialisation function.

Condition 3 exists because window aggregation removes inputs in the
order it added them, and `CREATE AGGREGATE` lets an inverse rely on that
(`create_aggregate.sgml:730-733`).  Maintenance removes arbitrary
inputs.  The built-in inverses are order-independent, except that
`sum(money)` can overflow at a different point depending on order.  In
the first version only an allowlist of built-ins counts as invertible;
`sum(money)` is not on it.  A `pg_aggregate` flag for user aggregates can
follow.

Every other aggregate (`min`, `max`, `sum(float4/float8)`, the
`bool_*`/`bit_*` families, `string_agg`, `array_agg`, the JSON
aggregates) is maintained by recomputing the affected groups from the
base table, through a parameterised query compiled at CREATE.  Ordered-set
aggregates, `DISTINCT` or `FILTER` inside an aggregate, and grouping sets
are rejected in the first version.

**Aggregate state storage.**  The view relation carries internal
columns: a row count per group, and per aggregate its transition state
and its count of non-NULL inputs.  The state is needed because a final
value can't be reversed; for example `sum(numeric)` tracks how many
inputs have the maximum scale (`numeric.c:4773`).

For `internal` states the stored form is the `aggserialfn` output.  That
is a wire format, not an on-disk one, and it changes: commit
`d699687b329` (2025-08) changed the serialised form of the `int128`-based
states.  So `ivmstateversion` records the server version that wrote the
states, and pg_upgrade marks every view with a different version as
stale (section 8), to be refreshed after upgrade.  Patch 1 checks
`d699687b329`'s change against this rule as its test case.

**Internal columns** carry a new `pg_attribute.attisinternal` flag.
They are excluded from:

- `*` expansion, the way dropped columns are (`parse_relation.c:3181`);
- whole-row references and the view's composite type;
- `COPY v TO`, which would otherwise read them (`copyto.c:837`);
- INSERT and COPY column lists.

Nothing but the compiler can name them.  pg_dump dumps the view as its
query; binary upgrade reproduces the column layout exactly.

**Owned indexes.**  The compiler creates, as dependencies of the view:

- a unique index on the view's key, `NULLS NOT DISTINCT`, so maintenance
  finds a group with one probe and NULL keys need no special case.  The
  key's types must have a btree opclass, and must be bitwise-equal when
  equal: the opclass has `BTEQUALIMAGE_PROC` returning true
  (`nbtree.h:720`), which excludes `numeric`, `float4`/`float8`,
  `interval` and text under a nondeterministic collation.  Other keys
  are rejected in the first version;
- for each probe a delta rule makes (group-key index on the base table
  for recompute, join-key index for class 2), an index if none exists.

**Locks at CREATE.**  `ShareRowExclusiveLock` on every base table until
commit, plus a relcache invalidation for each.  It blocks writers, so no
writer can commit a change before capture exists for it, and it is
self-exclusive, so two CREATEs on one table do not race on its
`pg_class` row (heap's in-place-update check requires one of these two
modes, `heapam.c:4445`).  The view is then populated from a fresh
snapshot taken after the lock (`GetLatestSnapshot()`), not the
statement's snapshot, which predates the lock wait: a writer that
committed during the wait must be in the view.  A CONCURRENTLY form can
come later.

**Rejected at CREATE**, all versions: mutable functions (the rule for
index expressions), user-defined aggregates (CREATE AGGREGATE marks the
aggregate immutable whatever its support functions are), `TABLESAMPLE`,
`FOR UPDATE`, foreign tables or partitions, system catalogs (their
writes bypass the executor), temporary objects (already rejected,
`parser/analyze.c:3205`), system columns, virtual generated columns
(the planner expands them, so the stored program would name a column
with no stored value).  In the first version also: unlogged base tables
(they come back empty after a crash and are unreadable on a standby, so
the view would silently disagree), base tables with inheritance
children (their changes would not be captured), and base tables with
row-level security enabled (the program does not carry policies).

**Kept true after CREATE.**  Each rejection above that a later DDL
could undo is also refused while the view exists, or a dump of the
database would fail to restore the view: `SET UNLOGGED`, `ENABLE ROW
LEVEL SECURITY`, gaining an inheritance child (`INHERITS`, `INHERIT`),
becoming a partition (`ATTACH PARTITION`), and `SET EXPRESSION` on a
stored generated column (which rewrites the table outside the executor).

## 4. Capture

`ExecInitModifyTable`, and the COPY and logical-apply equivalents, build
an `IVMCaptureState` for a result relation that has dependent views.
Capture is called at its own sites, next to the `ExecAR*Triggers` calls
but not inside their conditions: `copyfrom.c:571-589` calls
`ExecARInsertTriggers` only when the table has indexes or row triggers,
so a hook nested there would miss COPY into a bare table.  Sites: 7 in
`nodeModifyTable.c`, 4 in `copyfrom.c`, 3 in `execReplication.c`,
covering INSERT, UPDATE, DELETE, MERGE, `ON CONFLICT DO UPDATE`,
cross-partition UPDATE, COPY FROM and logical replication apply.

Because capture isn't a trigger, `DISABLE TRIGGER ALL` and
`session_replication_role = replica` don't skip it, so views on a
subscriber stay current, and none of the transition-table limits apply.

Per row capture records the old image with weight −1, the new image
with +1, or both, projected to the columns any dependent view reads.

- **Skipping UPDATEs.**  A statement whose updated columns can't touch a
  read column skips capture.  The column set is
  `ExecGetAllUpdatedCols` (`execUtils.c:1443`), which includes generated
  columns.  A BEFORE ROW trigger can change any column, so with one
  present the decision is made per row by comparing the projected old
  and new images.
- **Partitions.**  Rows are converted to the root's row type, as
  transition capture does.
- **Toast.**  Toasted values are kept as pointers.  Their toast rows
  can't be pruned while the capturing transaction runs.  In-transaction
  rewrites that invalidate them (`ALTER TABLE ... SET ACCESS METHOD`,
  rewrite-causing `ALTER TYPE`) first apply pending changes.
- **Frozen inserts.**  `COPY FREEZE` writes tuples that every snapshot
  sees regardless of command id (`heapam_visibility.c:1021`), which
  breaks the "before" state of section 5.2.  A frozen insert into a base
  table marks the view for recompute.

Rows go to backend-local tuplestores, one per (base relation,
subtransaction), spilling to temp files past `work_mem`, never
WAL-logged.  Each captured row also records the command id that wrote
it.  `ROLLBACK TO SAVEPOINT` discards the subtransaction's stores and
`RELEASE` appends them to the parent's, as `AfterTriggerEndSubXact` does
with its event lists (`trigger.c:5478`).

**Recompute instead of capture** for changes that bypass the executor:
TRUNCATE (which also drops the table's pending changes),
`ATTACH`/`DETACH PARTITION`, `INHERIT`/`NO INHERIT`, frozen inserts,
and non-concurrent `REFRESH` of a base matview (which swaps its heap).
`REFRESH ... CONCURRENTLY` of a base matview applies its diff with
ordinary DML and is captured.  A recompute is the same work as REFRESH
of the view, done at the view's next maintenance.

## 5. Maintenance

### 5.1 Two passes

Maintenance has a read-time pass and a commit-time pass.

**Read-time pass: private.**  When a statement that may read the view is
about to start and this transaction has changes the view hasn't seen,
the backend computes the view's delta and keeps it as a private overlay:
a backend-local tuplestore of (view row, weight) netted per key.  Scans
of the view by this backend return the shared rows merged with the
overlay.  Nothing shared is written and no lock is taken.  Other
sessions see only the committed view, which is correct, since they can't
see this transaction's base changes either.

The overlay is computed against the reading statement's own snapshot,
so the view agrees with the base tables that statement sees.  It is
discarded at the end of the transaction, on `ROLLBACK TO SAVEPOINT`
back to its creation, and whenever its snapshot no longer matches the
reading statement's (READ COMMITTED takes a new one per statement); it
is then recomputed from the full set of pending changes, against the new
snapshot.  An overlay therefore never needs correcting for other
transactions' commits: it is always computed against the snapshot that
reads it.

Scans of the view read the overlay through a new executor node,
`IvmOverlay`, described in section 5.5.  Plans built before the overlay
existed are invalidated by a backend-local relcache invalidation of the
view when its first overlay is created in the transaction, and again
when the transaction ends.

**Commit-time pass: shared.**  At commit, for each view with pending
changes, the delta is computed from the full transaction's changes
against a fresh snapshot (section 6), applied to the shared view rows,
and the overlay is discarded.  In `CommitTransaction` the pre-commit
loop (`xact.c:2319-2326`) becomes:

```c
for (;;)
{
    AfterTriggerFireDeferred();
    if (PreCommit_Portals(false))
        continue;
    if (!PreCommit_MaintainMatViews())
        break;
}
```

It runs after deferred triggers (which can still change base tables),
while plans can still execute, before `XACT_EVENT_PRE_COMMIT`, and
before `PreCommit_CheckForSerializationFailure` (`xact.c:2396`).
Materialized views can't have triggers (`trigger.c:310-315`), so the
pass queues no trigger events.  `PrepareTransaction` gets the same loop
(`xact.c:2604-2611`).

**Views over views.**  Views are processed in dependency order.  At
commit, a write to view V1 is captured like any write, so V2 over V1
picks it up later in the pass.  At read time, V1's overlay is V2's
captured change for V1.

**Security.**  Both passes run as the view owner, with
`SECURITY_RESTRICTED_OPERATION` and the restricted `search_path` REFRESH
uses (`matview.c:192-194`).  The commit pass writes through the
`OpenMatViewIncrementalMaintenance` gate (`matview.c:960`, checked at
`execMain.c:1122`).

### 5.2 Where the read-time check runs

The check runs wherever a statement gets the snapshot it will read with.
Revision 1 listed three sites; the review found more.  The complete list
for patch 4, each to be covered by a test:

| Site | Covers |
|---|---|
| `PortalStart`, `pquery.c:584` | `PORTAL_ONE_SELECT` |
| `PortalRunMulti`, `pquery.c:1343` | DML with `SELECT` from the view, `RETURNING`, modifying CTEs |
| `PortalRunUtility`, `pquery.c:1234` | EXPLAIN ANALYZE, `COPY v TO`, CTAS, DECLARE, EXECUTE, REFRESH of a matview that reads v |
| `_SPI_execute_plan`, `spi.c:2611`, and its per-statement `UpdateActiveSnapshotCommandId`, `spi.c:2666` | SPI, PL languages |
| `SPI_cursor_open_internal`, `spi.c:1749-1754` | SPI cursors |
| `functions.c:1688` and `1692` | SQL functions, including later statements in the body |
| `EnsurePortalSnapshotExists`, `pquery.c:1892` | procedures after COMMIT |
| `pl_exec.c:6315` | PL/pgSQL simple expressions calling a stable function |

"May read the view": the plan's `relationOids` contains it
(`setrefs.c:607`; views over views appear too), or its `invalItems`
names a user function (`setrefs.c:3667`, which records only non-built-in
functions).  When the transaction has no pending changes for any view
the check is one test of a backend-local counter.

Where the snapshot is supplied by a caller (`SPI_execute_snapshot`,
read-only SPI using `GetActiveSnapshot()`), the overlay is computed
against that snapshot; nothing needs to move.

**In-statement reads are not served in the first version.**  A
read-time pass during a statement that is itself writing a base table
(a BEFORE ROW trigger or a volatile function that reads the view) would
see captured rows from the running command, whose command id is at or
above the statement's.  Revision 1's "before" snapshot then
double-counts (the review's counterexample: two R rows inserted by one
statement, each firing a trigger that inserts into S and reads V).  In
the first version, a read of an incremental view while a command that
writes one of its base tables is active raises an error.  Patch 10
revisits it.

### 5.3 The delta queries

Each delta rule becomes a Query tree in which the captured change is an
`RTE_NAMEDTUPLESTORE`.  The planner costs those by row count
(`costsize.c:6386`): a three-row change plans as index probes and a
ten-million-row load as hash joins.  Plans are cached per backend with
`CreateCachedPlanForQuery` (`plancache.c:274`), which exists for Query
trees with no source text, and replanned when the change size moves by
an order of magnitude.  For a change that is a large fraction of the
table, recomputing the view and diffing it in is another plan the cost
model can pick.

Named-tuplestore scans are not parallel safe (`allpaths.c:783`), so
delta queries run serially.

**"Before" and "after" a transaction's changes.**  All reads in one
pass use one snapshot.  The "after" state is that snapshot at the
current command id.  The "before" state is a copy with `curcid =
FirstCommandId`.  Heap visibility hides the transaction's own inserts
and still shows its own deletes and pre-update versions when
`cmin`/`cmax >= curcid` (`heapam_visibility.c:1062-1067`; the multixact
and combo-cid paths behave the same).  A scan reads the "before" state
when its range-table entry carries a new flag; that flag is the one
executor change outside capture and the overlay node.  It is passed to
the table AM as the scan's snapshot, so any AM whose MVCC visibility
honours `curcid` works.  Since both passes always start from the full
set of the transaction's changes, `FirstCommandId` is always the right
"before" point; there is no partial-application state to track.

**Joins.**  For changes to several tables, terms are ordered so cross
terms count once:

```
ΔV = Σᵢ  R₁ᵃ ⋯ Rᵢ₋₁ᵃ ⋈ ΔRᵢ ⋈ Rᵢ₊₁ᵇ ⋯ Rₙᵇ        (a = after, b = before)
```

the rule differential dataflow's delta joins use.  It fixes the
single-statement two-source double count pg_reflex still has open.

### 5.4 Applying the change (commit pass)

- **Grouped aggregates.**  Sort the change by group key.  Per group:
  find the row through the key index, or insert it speculatively as
  `ON CONFLICT` does; apply forward or inverse transitions per input
  row; delete the row when its count reaches zero, otherwise finalise
  the visible columns and update it.  The key doesn't change, so on heap
  this is a HOT update unless a user indexed an aggregate column.
  Sorting fixes the lock order.
- **Scalar aggregates** (no GROUP BY).  The view always has exactly one
  row, even over an empty table (`count(*)` is 0, `sum` is NULL), so the
  row is created by CREATE and never deleted.
- **State handling.**  Stored states are copied out of the buffer
  (`datumCopy`, or deserialised) before a transition runs, because
  transition functions modify by-reference state in place when they see
  an aggregate context (`numeric.c:6507`, in `int4_avg_accum`).
- **NULL inputs.**  A strict forward or inverse function is skipped for
  a NULL input, as window aggregation does (`nodeWindowAgg.c:505`); the
  aggregate's non-NULL count tells when to reset its state instead of
  calling the inverse on the last input (`nodeWindowAgg.c:542`).
- **Inverse that gives up.**  An inverse may return NULL to mean "can't
  reverse this input" (`xaggr.sgml:220`); `numeric_accum_inv` does so for
  the maximum-scale case (`numeric.c:5432`).  The group is then
  recomputed.
- **Call context.**  Support functions are called with a context that
  `AggCheckCallContext` recognises (`nodeAgg.c:4635`), since some refuse
  to run without one (`numeric.c:4795`).  Whether that is a new
  `AGG_CONTEXT_MAINTAIN` or `AGG_CONTEXT_WINDOW` (`fmgr.h:832`) is
  decided in patch 3.
- **Views without aggregates** (patch 8).  Duplicate rows are stored as
  duplicate rows, as REFRESH produces, so reads need no change.  Deletes
  remove `|weight|` matching rows.  The key index is not unique for such
  views; when the output columns have no usable key (section 3), the
  view is rejected in that version.

### 5.5 The overlay node

A scan of an incremental view that has an overlay in this transaction
must return the shared rows with the overlay applied: rows whose key
the overlay changes are replaced or removed, and new keys are added.
The planner can't express that with existing nodes without knowing
the overlay's contents, which change between statements, so it gets a
node of its own.

**Plan shape.**  When `set_rel_pathlist` sees an incremental view with
an overlay in this backend (a relcache flag, backend-local), it wraps
every path for the view in an `IvmOverlay` path:

```
IvmOverlay (view = v, overlay = backend slot n)
  -> any scan of v (seq, index, bitmap, index-only)
```

The child is planned normally, so the view keeps using its indexes,
including index-only scans.  `IvmOverlay` drops child rows whose key
the overlay touches, then appends the overlay's replacement rows that
satisfy the scan's quals.  The output has no order guarantee; if the
child's path was ordered, the planner adds a Sort above or picks an
unordered child, by normal costing.

**Execution.**

1. At `ExecInitIvmOverlay`, look up the overlay for the view in the
   transaction's state (it was built before the statement's snapshot
   was taken, section 5.2).  It holds, per affected key: the key, the
   replacement row or a delete marker.  It is a hash table keyed by the
   view's key (`datum_image_hash`, matching the bitwise-equal keys
   section 3 requires), backed by a tuplestore for the rows.
2. Child rows: for each row from the child, probe the hash table by key
   and skip the row if present.  The child must return the key columns,
   so the planner adds them to its target list when the query doesn't
   read them.
3. After the child is exhausted, emit each replacement row that
   satisfies the scan's quals and projection.  The node evaluates those
   quals itself (copied from the child's `qual` and index quals at plan
   time), since the replacement rows never pass through the child.
4. `ExecReScanIvmOverlay` rescans the child and restarts the
   replacement pass; parameterised rescans (inner side of a nested loop)
   re-evaluate the quals with the new parameters.

**What the overlay row is.**  For aggregate views, the overlay holds
the group's final row as it would be after this transaction (computed
from the shared row's internal state plus the delta, then finalised),
so the node only replaces whole rows; it does no aggregate arithmetic
at scan time.  For views with duplicate rows (patch 8), the overlay
holds, per key, the full multiset of rows for that key after the
transaction, and the child's rows with that key are all skipped.

**Restrictions in the first version.**

- Not parallel safe: the overlay is backend-local.  A view with an
  overlay is scanned without parallelism in that transaction.
- `TABLESAMPLE` and `FOR UPDATE`/`FOR SHARE` on an incremental view
  with an overlay are rejected.  Row locking on a matview is already
  rejected (`execMain.c:1247`).
- `ctid` and other system columns are not available for overlay rows;
  reading a system column of an incremental view with an overlay raises
  an error.
- `EXPLAIN` shows the node and the overlay's key count.

The node touches the planner (`allpaths.c`, `createplan.c`), the
executor (`execProcnode.c` dispatch, a new `nodeIvmOverlay.c`), and
`explain.c`, in the same places `NamedTuplestoreScan` does
(`execProcnode.c:274`, `createplan.c:422`, `explain.c:1208`).

## 6. Concurrency and isolation

Only the commit pass writes shared rows, so only it takes locks.

### 6.1 Class 1

No view-level lock.  The commit pass's delta depends only on the
transaction's own changes, so it is correct against any state of the
view; applying it commutes with other transactions' deltas.  Writers to
different groups run in parallel; writers to one group serialise on its
row lock, like counter rows.

The apply loop is insert-or-lock:

1. probe the key index with a dirty snapshot; if no row, insert
   speculatively, and on conflict go back to 1;
2. lock the latest version with `TUPLE_LOCK_FLAG_FIND_LAST_VERSION`, as
   `nodeModifyTable.c:1752` and `nodeLockRows.c:183` do, under
   `GetLatestSnapshot()`, at every isolation level (the transaction
   reads nothing from the view afterwards, so the check
   `ExecCheckTupleVisible` makes at `nodeModifyTable.c:381` doesn't
   apply);
3. on `TM_Deleted` (another writer emptied the group and committed), go
   back to 1 and recreate it from the empty state.

### 6.2 Class 2

**The race.**  V = R ⋈ S.  T₁ changes S, T₂ changes R, concurrently.
T₁'s commit pass computes `R ⋈ ΔS₁`, T₂'s computes `ΔR₂ ⋈ S`.  The cross
term `ΔR₂ ⋈ ΔS₁` is applied once if and only if the second committer's
snapshot sees the first committer's commit.  The same holds for a class 2
view over one table (a self-join, or a recompute that reads other rows
of the group), which is why class follows the delta rule.

**The lock.**  The commit pass takes an exclusive lock, then its
snapshot, applies, and keeps the lock until its commit is visible to new
snapshots (`ProcArrayEndTransaction`, `xact.c:2446`); locks are released
right after (`xact.c:2495`).  The window can't end earlier: the commit
record flush and the synchronous-replication wait (`xact.c:1609`) come
before visibility, and a maintainer that took its snapshot during them
would miss the cross term.

Read-time passes take no lock (section 5.1), so a writer never holds a
view lock across its own statements, its idle time, or anyone else's.
It holds it across its own commit only.

**What is locked.**

- One-key joins (every base relation joins on the same key, as in a star
  join through a common column): an exclusive lock on
  `(view, hash of key)` for each key in the transaction's delta, in hash
  order, with a new lock tag type.  Writers on different keys don't
  wait.  Above a threshold of distinct keys the pass locks the whole
  view instead, to stay within `max_locks_per_transaction`.
- Every other class 2 view: one exclusive lock on the view.  Per-key
  locks are unsound when two deltas meet through a third table on
  different keys: in `R ⋈ₐ S ⋈_b T`, T₁ changing R at a=1 and T₂ changing
  T at b=2 lock different keys, and the term `ΔR ⋈ S ⋈ ΔT` is lost for
  any S row with (a=1, b=2).

**Lock order and deadlock.**  Locks are taken by view in dependency
order, then by key hash.  The commit pass waits on view row locks while
holding view or key locks; the only other holders of view row locks are
other commit passes, which took their view and key locks in the same
order, so no cycle forms through maintenance alone.

### 6.3 Isolation levels

- **Commit pass, every level.**  Applies to the latest row versions
  under a fresh snapshot and never raises `40001` on its own account.
  Under SERIALIZABLE, the view writes conflict with other transactions'
  predicate locks on the view as usual.  The base-table reads use an
  MVCC snapshot, so they take predicate locks too
  (`predicate.c:502`); whether reading at a later snapshot than the
  transaction's under SSI admits an anomaly is open question 1.
- **Read-time pass, every level.**  The overlay uses the reading
  statement's snapshot and touches no shared row, so it raises nothing.
  At REPEATABLE READ the view the transaction sees is as of its
  snapshot plus its own changes, matching its base-table reads.

### 6.4 REFRESH of an incremental view

REFRESH takes `AccessExclusiveLock` (or `ExclusiveLock` for
CONCURRENTLY, `matview.c:127`) on the view, and must take its snapshot
after the lock: today the snapshot is pushed before the lock wait, so a
commit pass that commits during the wait is overwritten.  Under
REPEATABLE READ the transaction snapshot predates the lock, so REFRESH
of an incremental view in a REPEATABLE READ or SERIALIZABLE transaction
is refused, as pg_ivm does for `refresh_immv`.  REFRESH also clears
`ivmstale` and drops this transaction's pending changes for the view.

## 7. Hidden reads (later)

A read of the view can be invisible to the plan of the outer statement:
built-ins that run SQL text (`query_to_xml`, `xml.c:3072`;
`query_to_xmlschema`, `xml.c:3132`; `ts_stat`, `tsvector_op.c:2590`;
`ts_rewrite`, `tsquery_rewrite.c:307`), and PL functions that build SQL.
All of them go through SPI, so the inner statement hits a section 5.2
site and gets the overlay.

The open case is read-only SPI, which reuses the caller's snapshot
(`spi.c:1749-1750`; `query_to_xml_internal` calls `SPI_execute(query,
true, 0)`).  With an overlay this is fine, since the overlay is computed
against whatever snapshot the read uses.  It needs tests, and it is
patch 0: tests for reads through `query_to_xml`, a stable SQL function,
PL/pgSQL `EXECUTE`, and a cursor opened before the change.  Reads by
other sessions (`dblink`, `postgres_fdw` loopback) never see this
transaction's changes and need nothing.

## 8. Invalidation and failure

**What invalidates the program.**  The program embeds analysed
expressions, so anything that changes their meaning makes the stored
aggregate state inconsistent, not just stale.  The `pg_depend` entries
(section 2) make these DDL refuse without CASCADE, or, where PostgreSQL
allows replacing in place, mark the view stale:

| Change | Effect |
|---|---|
| `DROP` of a dependency | refused without CASCADE |
| `CREATE OR REPLACE FUNCTION` of a used function | `ivmstale`; recompile at REFRESH |
| `CREATE OR REPLACE VIEW` of an inlined view | `ivmstale` |
| `CREATE/ALTER/DROP POLICY` on a base table, `ENABLE/DISABLE ROW LEVEL SECURITY` | `ivmstale` |
| `ALTER COLUMN SET EXPRESSION` on a read generated column | `ivmstale` |
| `ALTER COLUMN TYPE` on a read column | refused (`tablecmds.c:15631`, as for views) |
| new or dropped index on a base table | cached delta plans replanned (relcache invalidation) |
| pg_upgrade to a different state version | `ivmstale` (section 3) |

A stale view raises an error on read and is no longer maintained until
`REFRESH MATERIALIZED VIEW` recompiles and repopulates it.  That is the
same contract as an unpopulated materialized view
(`execUtils.c:784`).  Its base tables' writes no longer fail because of
it.

**When maintenance fails.**  A commit pass that errors (division by
zero in the view query, overflow, a unique-key violation, a lock
timeout) aborts the writing transaction, base changes included, as a
deferred foreign key does.  There is no option to mark the view stale
instead: a view that is wrong is worse than a write that fails.  This is
why creating one needs `MAINTAIN` or ownership on every base table
(section 2): the view's creator chooses which writes can fail.  A
read-time pass that errors aborts the reading statement, as any error in
it would.

## 9. Patch series

| # | Patch | Done when |
|---|---|---|
| 0 | tests for hidden reads through SPI, section 7 | read-only SPI, SQL function, `EXECUTE`, cursor cases specified |
| 1 | reloption, `pg_matview_ivm`, `relhasivm`, `attisinternal`, privilege checks, compiler for single-table aggregates, CREATE locks, dependencies, `ivmstale` | accept/reject regression tests for every construct; views created and REFRESHed, not maintained |
| 2 | capture: all sites, tuplestores, subtransactions, column filter, recompute flags | captured images tested for every DML path, COPY into a bare table, BEFORE ROW triggers, generated columns |
| 3 | class 1 commit pass: invertible aggregates, scalar aggregates, apply loop, state copy, call context | oracle (section 10) passes at all isolation levels |
| 4 | read-time pass: overlay construction, `IvmOverlay` planner path and executor node, every site in 5.2, in-statement read error | own-write visibility tests at every site; EXPLAIN shows the node; index, index-only, bitmap and seq scans of a view with an overlay agree with the oracle |
| 5 | recompute: min/max, non-invertible aggregates, inverse gives up, TRUNCATE, partition DDL, frozen inserts | oracle with these shapes |
| 6 | class 2: inner joins and self-joins, before-state flag, multi-table delta ordering, view lock, owned join-key indexes | isolation spec for the cross-term race in every interleaving |
| 7 | per-key locks for one-key joins | same spec, plus throughput with disjoint keys; a spec showing the three-table case still takes the view lock |
| 8 | DISTINCT, duplicates, UNION/INTERSECT/EXCEPT | |
| 9 | outer joins, EXISTS / NOT EXISTS | |
| 10 | HAVING, `ORDER BY ... LIMIT`, window functions, `WITH RECURSIVE`, in-statement reads | |

Patches 1-3 are the first usable slice: incremental `GROUP BY` views
over one table, maintained at commit.  Patch 4 adds read-your-writes.

**Status (2026-11-12).**  Patches 1 and 2 are done.  Patch 3 is done in
a first form, 3a: every view is maintained by the class 2 protocol, as a
recompute of the groups the transaction touched, under the view lock.
That is correct for every accepted aggregate, including min/max and
numeric, but costs a read of each touched group and serialises writers
to one view.  3b adds incremental state (internal columns, inverse
transition functions) and the lock-free class 1 path for invertible
aggregates.

## 10. Testing

- **Oracle.**  After every statement and every commit: the view
  `EXCEPT ALL` its query, both ways, is empty.  A test helper checks
  every incremental view in the database.
- **Isolation specs** for each class at each isolation level, with
  savepoints, aborts after the commit pass, `TM_Deleted` on a group, the
  cross-term race, and the three-table per-key case.
- **TAP** for crash during the commit pass, hot standby reads,
  pg_upgrade with incremental views (including the stale-on-upgrade
  rule), and logical replication into a subscriber with an incremental
  view.
- **Property tests** with `src/test/prop` (the libhegel harness in
  `~/ws/postgres/hegel`): random schemas, random view definitions from
  the supported grammar, random interleaved transactions, compared with
  REFRESH.  libdtfl's `examples/ivm/ivm_view.c` is a second oracle for
  the delta algebra outside the server.
- **Seeds** from pg_ivm's isolation specs and pg_reflex's
  `untreated_bugs/` (142 reports at 1.12.2), each reduced to a test,
  and the counterexamples of the revision 1 review (section 13).

## 11. Every query shape: state needed

An operator whose output isn't enough to maintain it gets an owned
relation, indexed for the probes its delta rule makes: differential
dataflow's arrangements, stored as tables.

| Shape | Extra state | Maintenance |
|---|---|---|
| invertible aggregates, one table | internal columns | O(change), class 1 |
| min, max, non-invertible | none | recompute group via index, class 2 |
| inner joins | none | probe base indexes, class 2 |
| DISTINCT, set operations | multiplicity column (per branch for set ops) | counting |
| duplicates (no aggregate) | none | delete or insert `|weight|` rows |
| outer joins, EXISTS / NOT EXISTS | none if the other side is indexed | inner join plus antijoin |
| HAVING | owned relation with every group's state, including failing groups | counting, then filter |
| `ORDER BY ... LIMIT n` | owned relation of candidates past the cut | refill from it |
| window functions | none | recompute affected partitions via partition-key index |
| `WITH RECURSIVE` | owned relation of the fixpoint with derivation counts | delete-and-rederive; own design |

## 12. Prior work

- **pg_ivm 1.17** (SRA OSS, extension).  Statement triggers before and
  after each change, `stringToNode` and replanning per statement,
  generated SQL through SPI, `ExclusiveLock` on the view from the first
  statement to commit, `40001` under REPEATABLE READ.  Needs superuser.
- **pg_reflex 1.12.2** (extension, Rust).  Statement triggers with
  transition tables, sqlparser-rs at CREATE and text substitution at run
  time, MERGE to apply, an advisory lock per view to commit, a deferred
  mode.
- **Nagata, commitfest 4337** (core version of pg_ivm).  v40 (2026-08)
  narrows to SPJ views and adopts the reloption; same trigger design.

This design differs in: capture in the executor instead of triggers;
compiled parse trees instead of SQL text; private read-time overlays,
so no shared write or lock before commit; the `curcid` "before" state;
aggregates driven by `pg_aggregate`; and a commit-only lock on a key
or, failing that, the view.

## 13. Changes from revision 1

From an independent review against the source (2026-10-29):

| Revision 1 | Problem | Revision 2 |
|---|---|---|
| read-time pass writes shared view rows; commit pass adds a correction for commits since | correction at the current command id misses an update of a row another transaction inserted between the passes (worked counterexample); read-time row locks last to commit and deadlock with commit-pass key locks | private overlay at read time, shared write only at commit; no correction |
| "before" state at the last maintained command id | double-counts when a read-time pass runs inside a writing statement | always start from `FirstCommandId`; reads during a writing command error in the first version |
| per-key locks for every join view | unsound for `R ⋈ₐ S ⋈_b T` | per-key only for one-key joins, view lock otherwise |
| class by number of base tables | self-joins and recomputes need the lock too | class by delta rule's inputs |
| four read-time sites | `PortalRunMulti`, `PortalRunUtility`, per-statement CCI, `EnsurePortalSnapshotExists`, PL/pgSQL simple expressions missed | complete list in 5.2 |
| `ExecGetUpdatedCols` filter | misses generated columns and BEFORE ROW trigger changes | `ExecGetAllUpdatedCols`, per-row when BEFORE ROW triggers exist |
| hook beside `ExecARInsertTriggers` | COPY into a bare table skipped | own call sites |
| count 0 deletes the row | wrong for scalar aggregates | scalar views keep one row |
| multiplicity column for duplicates | changes what readers see | duplicates stored as rows |
| serialised `internal` states as storage | format changes across versions (`d699687b329`) | versioned, stale on mismatch |
| no privilege model | SELECT would let anyone make others' writes fail | `MAINTAIN` or ownership on base tables (revision 3) |
| COPY FREEZE, base-matview REFRESH, SET EXPRESSION, replaced functions/views/policies unhandled | silent drift or corrupt state | recompute or `ivmstale` (sections 4, 8) |
| citation errors | `trigger.c:1358`, `execReplication.c` flags, `ri_triggers.c` SSI claim, `procarray.c:590`, `heapam_visibility.c:986` | corrected or removed |

## 14. Open questions

1. **SSI and the commit pass's later snapshot.**  Under SERIALIZABLE the
   commit pass reads base tables with a snapshot later than the
   transaction's and writes the view.  Find a dangerous-structure
   example or an argument that the view's predicate locks cover it.
2. **Overlay node cost.**  `IvmOverlay` (section 5.5) probes a hash
   table per child row.  For a selective index scan of a large view with
   a small overlay that is cheap; for a sequential scan with a large
   overlay, measure.  Pushing the overlay keys into an index scan as a
   skip set is a later optimisation.
3. **Overlay size.**  A large transaction produces a large overlay at
   every read.  Above a threshold, a read-time recompute against the
   statement snapshot may be cheaper; same cost model as 5.3.
4. **Two-phase commit.**  The commit pass runs before PREPARE, so class 2
   locks are held from PREPARE to COMMIT PREPARED.  Document, or refuse
   PREPARE for transactions holding them.
5. **Plan-cache invalidation** for cached delta plans when statistics
   change, and for owned indexes.
6. **Row security.**  The program is compiled as the view owner, as
   REFRESH plans the query; confirm equivalence including `FORCE ROW
   LEVEL SECURITY`, and that policy changes reach `ivmstale` (section 8).
7. **`AGG_CONTEXT_MAINTAIN` or `AGG_CONTEXT_WINDOW`.**
8. **Commit-pass cost under synchronous replication** for class 2 views:
   writers to one view, or one key, are limited to one per commit
   latency.  Measure.
