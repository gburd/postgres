# UPSERT — a Lime grammar-extension demonstrator

`upsert` is a small `contrib` extension that adds an `UPSERT` statement to
PostgreSQL using the runtime grammar-extension feature (Track B / Lime).  It
adds one keyword (`UPSERT`) and one production to the in-process composed
grammar; at parse time the production is rewritten into the equivalent
`INSERT ... ON CONFLICT ... DO UPDATE` statement.  No SQL is executed by the
extension itself — it only produces a different parse tree, which the normal
planner and executor handle.

## Syntax

```
UPSERT INTO <table> (<col>, ...) VALUES (<expr>, ...) ON (<conflict_col>, ...)
```

* `(<col>, ...)` — the full column list being written.
* `VALUES (<expr>, ...)` — one row of values, positionally matching the columns.
* `ON (<conflict_col>, ...)` — the columns that form the conflict target
  (a primary key or unique constraint).

## Mapping

`UPSERT` is rewritten to:

```
INSERT INTO <table> (<col>, ...)
VALUES (<expr>, ...)
ON CONFLICT (<conflict_col>, ...)
DO UPDATE SET <c> = excluded.<c>   -- for every <c> in the column list
                                   --   that is NOT a conflict column
```

If every written column is a conflict column there is nothing to update, so
the rewrite degrades to `ON CONFLICT (...) DO NOTHING`.

## Example

```sql
CREATE EXTENSION upsert;
CREATE TABLE inventory (sku text PRIMARY KEY, qty int, updated timestamptz);

-- These two statements are equivalent:
UPSERT INTO inventory (sku, qty, updated) VALUES ('A-1', 5, now()) ON (sku);

INSERT INTO inventory (sku, qty, updated) VALUES ('A-1', 5, now())
  ON CONFLICT (sku) DO UPDATE SET qty = excluded.qty, updated = excluded.updated;
```

## How it works

* `_PG_init` (run from `shared_preload_libraries`) registers the `UPSERT`
  keyword and the `upsert_stmt` production with `pg_grammar_ext_register`.
* The base grammar is composed with this fragment in-process (no subprocess,
  no C compiler) at postmaster start.
* The reduce callback runs through the push-parse host-reduce path; it reads
  the table, column list, value list, and conflict columns from the rule's
  RHS values and builds an `InsertStmt` with an `OnConflictClause`.

This is a demonstrator for the Lime grammar-extension mechanism; it is not a
proposal to add `UPSERT` to core PostgreSQL (the SQL-standard spelling is
`INSERT ... ON CONFLICT`, which this extension lowers to).
