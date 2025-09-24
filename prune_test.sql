\set aid random(1, 100000 * :scale)
\set note_suffix random(1, 1000)
\set large_data_size random(50, 200)

-- Single transaction with multiple updates to same row to create dead tuples and page pressure
BEGIN;

-- Update the same row multiple times with increasingly large data to create dead tuples
UPDATE pgbench_accounts
SET last_updated = now(),
    update_count = update_count + 1,
    notes = 'v1_' || :note_suffix || '_' || repeat('data_chunk_', :large_data_size)
WHERE aid = :aid;

UPDATE pgbench_accounts
SET last_updated = now(),
    update_count = update_count + 2,
    notes = 'v2_' || :note_suffix || '_' || repeat('more_data_chunk_', :large_data_size + 10)
WHERE aid = :aid;

UPDATE pgbench_accounts
SET last_updated = now(),
    update_count = update_count + 3,
    notes = 'v3_' || :note_suffix || '_' || repeat('final_data_chunk_', :large_data_size + 20)
WHERE aid = :aid;

-- Also update a few nearby rows to create more page pressure
UPDATE pgbench_accounts
SET last_updated = now(),
    update_count = update_count + 1,
    notes = 'neighbor_' || :note_suffix || '_' || repeat('neighbor_data_', :large_data_size)
WHERE aid BETWEEN :aid + 1 AND :aid + 3;

COMMIT;
