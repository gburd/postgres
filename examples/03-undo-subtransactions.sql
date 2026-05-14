-- ============================================================================
-- Example 3: Subtransactions (SAVEPOINTs) with UNDO
-- ============================================================================

CREATE TABLE account_ledger (
    account_id  int,
    amount      numeric(10,2),
    posted_at   timestamptz DEFAULT now()
) USING recno;

BEGIN;

-- Parent transaction: Initial credit
INSERT INTO account_ledger VALUES (1001, 1000.00);

SAVEPOINT sp1;

-- Subtransaction 1: Debit attempt
INSERT INTO account_ledger VALUES (1001, -500.00);

SAVEPOINT sp2;

-- Subtransaction 2: Another debit
INSERT INTO account_ledger VALUES (1001, -300.00);

-- Check balance
SELECT SUM(amount) FROM account_ledger WHERE account_id = 1001;
-- Shows: 200.00

-- Rollback to sp2 (undo the -300.00)
ROLLBACK TO sp2;

-- Check balance after rollback
SELECT SUM(amount) FROM account_ledger WHERE account_id = 1001;
-- Shows: 500.00

-- Rollback to sp1 (undo the -500.00)
ROLLBACK TO sp1;

-- Check balance after full rollback to sp1
SELECT SUM(amount) FROM account_ledger WHERE account_id = 1001;
-- Shows: 1000.00 (only initial credit remains)

-- Commit parent transaction
COMMIT;
