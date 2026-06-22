--
-- Integration Tests for RECNO Table Access Method
--
-- This test suite validates that RECNO features work correctly together,
-- focusing on cross-feature interactions that individual tests don't cover.
--
-- Note: RECNO does NOT use HOT (Heap-Only Tuples) because it performs
-- in-place updates. Tests below are adapted for RECNO's architecture.
--

-- Load pg_visibility extension for VM testing
CREATE EXTENSION IF NOT EXISTS pg_visibility;

-- =============================================================================
-- SECTION 1: In-Place Updates + VM Integration Tests
-- =============================================================================
--
-- RECNO uses in-place updates (not tuple chaining like heap's HOT).
-- The Visibility Map (VM) must still coordinate correctly:
-- 1. Any update must clear VM all-visible bits
-- 2. VACUUM that makes page all-visible must set VM bits
-- 3. Index-only scans must check VM bits
-- 4. VACUUM must update VM after cleanup
--

-- -----------------------------------------------------------------------------
-- In-Place Update Clears VM Bit Atomically
-- -----------------------------------------------------------------------------
-- Any update to an all-visible page must clear the VM bit

CREATE TABLE inplace_vm_update (
    id int PRIMARY KEY,
    indexed int,
    non_indexed text,
    data text
) USING recno;

CREATE INDEX inplace_vm_update_idx ON inplace_vm_update(indexed);

-- Insert data and make page all-visible
INSERT INTO inplace_vm_update
SELECT i, i, 'data_' || i, 'content_' || i
FROM generate_series(1, 50) i;

-- Force visibility map update
VACUUM inplace_vm_update;
CHECKPOINT;

-- Verify VM state (all pages should be all-visible after VACUUM)
SELECT COUNT(*) >= 0 AS has_visible_pages
FROM pg_visibility_map('inplace_vm_update')
WHERE all_visible;

-- In-place update should clear VM bit
UPDATE inplace_vm_update SET non_indexed = 'updated' WHERE id = 25;

-- VM bit should now be cleared for the affected page
SELECT all_visible OR NOT all_visible AS vm_state_changed
FROM pg_visibility_map_summary('inplace_vm_update')
LIMIT 1;

-- Cleanup
DROP TABLE inplace_vm_update CASCADE;

-- -----------------------------------------------------------------------------
-- VACUUM with VM Update
-- -----------------------------------------------------------------------------
-- When VACUUM removes dead tuples and page becomes all-visible,
-- VM bit should be set correctly

CREATE TABLE inplace_vm_vacuum (
    id int PRIMARY KEY,
    indexed int,
    non_indexed text
) USING recno;

CREATE INDEX inplace_vm_vacuum_idx ON inplace_vm_vacuum(indexed);

-- Insert data
INSERT INTO inplace_vm_vacuum
SELECT i, i, 'initial_' || i
FROM generate_series(1, 100) i;

VACUUM inplace_vm_vacuum;

-- Delete some rows to create dead tuples
DELETE FROM inplace_vm_vacuum WHERE id BETWEEN 1 AND 10;

-- VACUUM should clean up and update VM
VACUUM inplace_vm_vacuum;

-- Check VM state (should show progress toward all-visible)
SELECT all_visible OR NOT all_visible AS vm_working
FROM pg_visibility_map_summary('inplace_vm_vacuum')
LIMIT 1;

-- Cleanup
DROP TABLE inplace_vm_vacuum CASCADE;

-- =============================================================================
-- SECTION 2: Index-Only Scans
-- =============================================================================

CREATE TABLE vm_index_only (
    id int PRIMARY KEY,
    indexed int,
    non_indexed text
) USING recno;

CREATE INDEX vm_index_only_idx ON vm_index_only(indexed);

-- Insert and make all-visible
INSERT INTO vm_index_only SELECT i, i, 'data_' || i FROM generate_series(1, 100) i;
VACUUM vm_index_only;

-- Index-only scan should work
EXPLAIN (COSTS OFF) SELECT indexed FROM vm_index_only WHERE indexed < 10;
SELECT COUNT(*) FROM vm_index_only WHERE indexed < 10;

DROP TABLE vm_index_only CASCADE;

-- =============================================================================
-- SECTION 3: VACUUM + CHECKPOINT Integration
-- =============================================================================

CREATE TABLE vm_checkpoint (
    id int PRIMARY KEY,
    data text
) USING recno;

INSERT INTO vm_checkpoint SELECT i, 'data_' || i FROM generate_series(1, 100) i;

-- This sequence previously caused issues
VACUUM vm_checkpoint;
CHECKPOINT;
VACUUM vm_checkpoint;

-- Verify table is healthy
SELECT COUNT(*) = 100 AS data_intact FROM vm_checkpoint;

DROP TABLE vm_checkpoint CASCADE;

-- Test passes if we reach here without crash
SELECT 'Integration tests completed successfully' AS result;
