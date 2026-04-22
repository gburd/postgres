# Contributing to RECNO

This document describes how to contribute to the RECNO storage access method,
including code style, review process, testing requirements, and common
development patterns.


## Getting Started

### Prerequisites

- A working PostgreSQL build environment (see PostgreSQL Developer FAQ)
- Familiarity with PostgreSQL's table access method (tableam) interface
- Understanding of PostgreSQL buffer management, WAL, and MVCC concepts

### Building

RECNO is built as part of PostgreSQL. After cloning the repository:

```bash
# Meson (preferred)
meson setup build
cd build && ninja
ninja install

# Make (traditional)
./configure
make -j$(nproc)
make install
```

When adding new source files:
1. Add the `.o` target to `src/backend/access/recno/Makefile` OBJS list
2. Add the `.c` file to `src/backend/access/recno/meson.build` backend_sources
3. If adding WAL record types, update `src/include/access/rmgrlist.h`
4. If adding GUC parameters, register them in the appropriate GUC module

### Running Tests

Before submitting any changes, run the full test suite:

```bash
# Regression tests
make installcheck EXTRA_TESTS=recno
make installcheck EXTRA_TESTS=recno_performance
make installcheck EXTRA_TESTS=recno_undo_redo

# Or via psql
psql -d testdb -f src/test/regress/sql/recno.sql
psql -d testdb -f src/backend/access/recno/test_recno_locks.sql
```

See TESTING.md for detailed instructions on running and writing tests.


## Code Style

RECNO follows PostgreSQL coding conventions.

### Formatting

- Use tabs for indentation (not spaces)
- Run `pgindent` on all modified files before submitting
- Maximum line length: 79 characters for code, longer for comments if needed
- Opening braces on the same line as the control structure
- Function definitions: return type on its own line, function name at column 1

```c
/*
 * FunctionName
 *
 *     Brief description of what the function does.
 */
static ReturnType
FunctionName(Type arg1, Type arg2)
{
    /* variable declarations first */
    int     local_var;
    Buffer  buffer;

    /* then executable code */
    ...
}
```

### Naming Conventions

- Functions: `RecnoVerbNoun()` (e.g., `RecnoFormTuple`, `RecnoLockPage`)
- Internal/static functions: `recno_verb_noun()` (e.g., `recno_scan_getnextslot`)
- Constants: `RECNO_UPPER_CASE` (e.g., `RECNO_TUPLE_DELETED`)
- Type names: `RecnoCamelCase` (e.g., `RecnoTupleHeader`, `RecnoPageOpaque`)
- GUC variables: `recno_lower_case` (e.g., `recno_use_hlc`, `recno_node_id`)

### Comments

- Every exported function must have a block comment describing purpose,
  parameters, and return value
- Use `/* ... */` style, not `//`
- Explain "why" not "what" -- the code shows what happens, comments explain
  the reasoning
- Mark design decisions and trade-offs explicitly
- Use `TODO` for known improvements, `FIXME` for known bugs, `XXX` for
  questionable code that works but should be reviewed

### Error Handling

- Use `ereport(ERROR, ...)` for recoverable errors
- Use `elog(PANIC, ...)` only inside critical sections (between
  `START_CRIT_SECTION` and `END_CRIT_SECTION`)
- Never call `ereport(ERROR)` inside a critical section
- Use `elog(DEBUG1, ...)` for development diagnostics (not WARNING)
- Clean up resources (buffer pins, locks) before raising errors


## Review Process

### Before Submitting

1. Run `pgindent` on all modified files
2. Run the full regression test suite and verify all tests pass
3. If your change modifies page format or WAL records, test crash recovery
4. If your change affects MVCC, test with concurrent sessions
5. Verify the change compiles without warnings (`-Wall -Werror`)
6. Update documentation (README, IMPLEMENTATION.md, or other docs) as needed

### What Reviewers Look For

- **Correctness**: Does the code handle all cases, including edge cases?
- **WAL safety**: Are all page modifications properly WAL-logged?
- **Concurrency safety**: Are buffer locks and tuple locks used correctly?
- **Lock ordering**: Does the code follow the lower-block-first convention?
- **Critical sections**: Are page modifications wrapped in critical sections?
- **Memory management**: Are allocations in the right memory context?
- **Error handling**: Are resources cleaned up on error paths?
- **Test coverage**: Are there tests for the new functionality?
- **Performance**: Does the change avoid unnecessary I/O or lock contention?


## Testing Requirements

Every change must include appropriate test coverage.

### For Bug Fixes

- Add a regression test that demonstrates the bug and verifies the fix
- If the bug involves crash recovery, add a TAP test or document manual
  recovery testing

### For New Features

- Add regression tests covering normal operation
- Add tests for error paths and edge cases
- If the feature modifies pages, test crash recovery
- If the feature affects concurrency, add isolation tests
- Update TESTING.md to document the new test coverage

### For Performance Changes

- Include benchmark results comparing before and after
- Use `pgbench` with custom scripts or `EXPLAIN ANALYZE` output
- Document the workload characteristics where the improvement applies


## Common Development Patterns

### Modifying a Page

Every page modification follows this pattern:

```c
Buffer buffer = ReadBuffer(rel, blockno);
LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

Page page = BufferGetPage(buffer);

START_CRIT_SECTION();

/* Make changes to the page */
...

MarkBufferDirty(buffer);

/* WAL log the change */
if (RelationNeedsWAL(rel))
{
    XLogRecPtr recptr = RecnoXLog...(rel, buffer, ...);
    PageSetLSN(page, recptr);
}

END_CRIT_SECTION();

UnlockReleaseBuffer(buffer);
```

Key rules:
- Always acquire the buffer lock before reading page contents
- Always mark the buffer dirty before WAL logging
- Always set the page LSN to the WAL record's LSN
- Never call `ereport(ERROR)` inside the critical section

### Adding a WAL Record Type

1. Define the opcode in `recno_xlog.h` (next available 0xN0 value)
2. Define the record structure in `recno_xlog.h` (both `#ifndef FRONTEND`
   and `#else` versions if the structure uses PostgreSQL-specific types)
3. Implement the logging function in `recno_xlog.c`:
   ```c
   XLogRecPtr
   RecnoXLogMyOp(Relation rel, Buffer buffer, ...)
   {
       xl_recno_myop xlrec;
       /* fill xlrec fields */

       XLogBeginInsert();
       XLogRegisterData(&xlrec, sizeof(xlrec));
       XLogRegisterBuffer(0, buffer, REGBUF_STANDARD);
       /* optionally register additional data */

       return XLogInsert(RM_RECNO_ID, XLOG_RECNO_MYOP);
   }
   ```
4. Add the REDO case in `recno_redo()` in `recno_xlog.c`
5. Add the description in `recno_desc()` and identification in `recno_identify()`
6. Test crash recovery by inserting data, performing the operation, crashing
   the server (`pg_ctl stop -m immediate`), restarting, and verifying state

### Working with Overflow Pages

When storing data in overflow chains:
- Always WAL-log overflow page writes (`RecnoXLogOverflowWrite`)
- Always link pages via `RecnoLinkOverflowPages` before releasing the buffer
- Check for runaway chains (max 1024 pages per chain)
- Clean up overflow chains when the referencing tuple is deleted

### Working with HLC/DVV

- Use `HLCNow()` to get the current HLC timestamp
- Use `DVVGetNext()` to generate a new DVV dot for causal tracking
- Use `HLCInUncertaintyWindow()` to check if a read may see inconsistent data
- Use `RecnoFillHLCInfo()` to populate WAL records with HLC uncertainty data
- Always check `recno_use_hlc` before accessing HLC state


## Architecture Overview

For detailed code walkthrough and algorithm descriptions, see:
- IMPLEMENTATION.md -- Complete code walkthrough
- DESIGN -- Architecture and design rationale
- LOCKING_AND_UNDO_REDO.md -- Locking and WAL design
- RECNO_HLC_DVV_DESIGN.md -- HLC/DVV design details
- RECNO_PLANNER_INTEGRATION.md -- Planner cost model


## Known Areas Needing Work

The following areas are known to need improvement and are good places to
start contributing (see also DESIGN section 11.3):

### Priority 1 (Correctness)
- Connect real LZ4/ZSTD libraries (current implementations are stubs)
- Implement VACUUM to reclaim dead tuple space
- Fix in-place update free space check (always-true condition)

### Priority 2 (Functionality)
- Implement parallel scan support
- Implement speculative insertion (INSERT ... ON CONFLICT)
- Implement full serializable isolation with cycle detection
- Optimize multi_insert for true batch page operations

### Priority 3 (Performance)
- Compress overflow page data
- Add page-level visibility optimization using pd_commit_ts
- Cache FSM state per-relation
- Implement prefetch for overflow page chain reads
- Change development elog(WARNING) messages to elog(DEBUG1)

### Priority 4 (Features)
- Add pg_stat_recno views for monitoring
- Implement online defragmentation (background worker)
- Persist compression dictionaries across restarts
- Implement distributed timestamp coordination
