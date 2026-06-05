# contrib/quel — QUEL query language as a parser extension

QUEL was the query language of UC Berkeley's Ingres relational DBMS,
developed by Stonebraker and others starting in 1973.  POSTGRES (the
project that became PostgreSQL) inherited a derivative called Postquel,
which PostgreSQL replaced with SQL in 1995 (PostgreSQL 6.0).

This contrib module reintroduces a small but representative subset of
QUEL via the `parser_extension.h` API, demonstrating that Lime's
runtime grammar composition can host an entire alternative query
language alongside SQL in the same backend.

## Why this exists

Beyond the historical curiosity, `contrib/quel` is the migration's
flagship demonstration that **PostgreSQL extensions can extend the SQL
grammar at backend startup with a non-trivial body of grammar**.

Specifically, this is the test case that exercises:

  - **Token vocabulary additions**: 10 new keyword tokens
    (`RETRIEVE`, `REPLACE`, `APPEND`, `DELETE_QUEL`, `RANGE`, `OF`,
    `IS`, `TO`, `INTO_QUEL`, `BY`).
  - **Non-terminal additions**: 6 new non-terminals (`quel_stmt`,
    `quel_retrieve_stmt`, …).  Each carries a `Node *` value that
    downstream parse-analysis would convert into a PG plan tree.
  - **Multi-statement productions**: 13 new rules covering five QUEL
    statement forms (`RETRIEVE`, `REPLACE`, `APPEND`, `DELETE`,
    `RANGE OF … IS …`).
  - **Precedence directives**: 4 `%nonassoc` markers placing the QUEL
    statement keywords at level 1000 (well above SQL's ladder which
    occupies 1–99) so QUEL and SQL operator precedence cannot
    interfere with each other.
  - **Cross-statement integration**: `stmt ::= quel_stmt` glues the
    QUEL extension into the base grammar's start symbol.  No
    modifications to the base SQL grammar.
  - **Reduce-callback dispatch**: every QUEL rule has a `quel_reduce`
    callback that fires via `pg_grammar_ext_dispatch_reduce` from
    the rebuilt parser .so.

## Subset of QUEL implemented

```
range of e is emp
retrieve (e.name, e.salary) where e.dept = "shoe"
retrieve into expensive (e.name, e.salary) where e.salary > 50000
append to emp (name = "alice", salary = 1000, dept = "toy")
replace e (salary = e.salary * 1.1) where e.dept = "shoe"
delete e where e.salary < 1000
```

The grammar accepts these shapes; the reduce callbacks emit `NOTICE`
messages identifying which production fired.  Wiring the reductions
through to PG plan trees is Track B follow-up.

## Track A vs. Track B status

The current `parser_extension.h` implementation runs the rebuild via
a subprocess pipeline (fork + lime + cc + dlopen, cached by SHA256
of the registered grammar fragment).  Under Track A:

  ✅ The rebuild pipeline runs at postmaster startup.
  ✅ The cache key is stable; subsequent boots hit the cache.
  ✅ The rebuilt parser .so is reachable via base_yyparse_fn
     indirection.
  ✅ Reduce callbacks dispatch correctly when invoked through the
     dispatch trampoline.
  ✅ The base SQL grammar parses unchanged.

Under Track B Phase 1 (LIVE):

  ✅ The scanner-keyword hook (pg_grammar_ext_keyword_hook in
     scan.c) recognises QUEL keyword lexemes and emits them as
     the rebuilt parser's token codes.  Real psql input matching
     a registered keyword now reaches the rebuilt parser as
     K_QUEL_* rather than IDENT.

  ❌ **The QUEL grammar itself is incomplete.**  This contrib
     module currently registers BARE keyword rules
     (`quel_retrieve_stmt ::= K_QUEL_RETRIEVE.`) -- the full
     RHS shapes with parens, target lists, WHERE clauses, BY
     sort lists, etc., are deferred to QUEL Phase A grammar
     expansion.  See `.agent/notes/quel-full-implementation-
     plan.md` for the 4-6 week implementation plan.

Real QUEL queries like `retrieve (e.name) where e.salary > 50000`
parse as far as the K_QUEL_RETRIEVE keyword, then hit a syntax
error at the open-paren because the grammar doesn't yet describe
the paren-list target form.  This is a contrib/quel-side gap, not
a parser_extension.h gap.

## Loading

QUEL must be loaded via `shared_preload_libraries` so its
`_PG_init()` runs before the first parse.  `_PG_init()` calls the
parser_extension API, which queues the registration; the rebuild
fires lazily on the first `raw_parser()` call.

```
# postgresql.conf
shared_preload_libraries = 'quel'

# psql
postgres=# CREATE EXTENSION quel;
postgres=# SELECT quel_extension_status();
postgres=# SELECT quel_serialized_lime();
```

## SQL functions exposed

`quel_extension_status() RETURNS text`
  Diagnostic summary of the registration: number of tokens, types,
  rules, precedence directives, and explicit notes on Track-B-only
  features.

`quel_serialized_lime() RETURNS text`
  The .lime grammar fragment QUEL contributed to the rebuild.  Useful
  for inspecting how the extension API serializes registrations and
  for diagnosing interactions with other grammar extensions.

## See also

  - `src/include/parser/parser_extension.h` — the runtime grammar
    extension API.
  - `src/test/modules/grammar_ext_compose/` — the API torture test
    (six smaller extensions in different combinations).
  - `src/test/modules/dummy_grammar_ext/` — the smoke test
    establishing the basic register/dispatch contract.
