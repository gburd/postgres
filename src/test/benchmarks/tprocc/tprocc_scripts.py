"""Generate pgbench transaction SQL scripts for TPROC-C workload.

Each transaction type gets a separate .sql file, one set per access method.
pgbench runs them with @weight to match the TPROC-C mix.
"""

import logging
import os

from .tprocc_config import AccessMethod, TproccConfig
from .tprocc_schema import get_table_name

logger = logging.getLogger(__name__)


def _new_order_script(am: AccessMethod, config: TproccConfig) -> str:
    """New-Order transaction (45% of mix).

    Per TPC-C spec: 1% of transactions roll back (simulating invalid item).
    This exercises the UNDO rollback path which is critical for RECNO testing.
    Uses \\gset to capture d_next_o_id from UPDATE RETURNING.
    Uses \\if for conditional ROLLBACK (pgbench >= PG11).

    Simplifications vs full TPC-C spec:
    - Fixed 10 order lines (spec says 5-15 random)
    - Single warehouse only (no remote warehouse items)
    - Customer lookup by c_id (spec has 60% by-name with cursor)
    """
    w = config.warehouses
    warehouse = get_table_name("warehouse", am)
    district = get_table_name("district", am)
    customer = get_table_name("customer", am)
    orders = get_table_name("orders", am)
    new_order = get_table_name("new_order", am)
    order_line = get_table_name("order_line", am)
    item = get_table_name("item", am)
    stock = get_table_name("stock", am)

    # Build order line block (repeated 10 times)
    ol_lines = []
    for i in range(1, 11):
        ol_lines.append(f"""-- Order line {i}
SELECT i_price, i_name, i_data FROM {item} WHERE i_id = :ol_i_id_{i};
UPDATE {stock} SET s_quantity = CASE WHEN s_quantity > 10 THEN s_quantity - :ol_qty ELSE s_quantity + 91 - :ol_qty END, s_ytd = s_ytd + :ol_qty, s_order_cnt = s_order_cnt + 1 WHERE s_i_id = :ol_i_id_{i} AND s_w_id = :w_id;
INSERT INTO {order_line} (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_delivery_d, ol_quantity, ol_amount, ol_dist_info)
VALUES (:o_id, :d_id, :w_id, {i}, :ol_i_id_{i}, :w_id, '1970-01-01', :ol_qty, 0, 'aaaaaaaaaaaaaaaaaaaaaaaa')
ON CONFLICT (ol_o_id, ol_d_id, ol_w_id, ol_number) DO NOTHING;""")

    ol_block = "\n".join(ol_lines)

    return f"""\\set w_id random(1, {w})
\\set d_id random(1, 10)
\\set c_id random(1, 3000)
\\set ol_i_id_1 random(1, 100000)
\\set ol_i_id_2 random(1, 100000)
\\set ol_i_id_3 random(1, 100000)
\\set ol_i_id_4 random(1, 100000)
\\set ol_i_id_5 random(1, 100000)
\\set ol_i_id_6 random(1, 100000)
\\set ol_i_id_7 random(1, 100000)
\\set ol_i_id_8 random(1, 100000)
\\set ol_i_id_9 random(1, 100000)
\\set ol_i_id_10 random(1, 100000)
\\set ol_qty random(1, 10)
\\set rollback_pct random(1, 100)
BEGIN;
-- Get warehouse tax
SELECT w_tax FROM {warehouse} WHERE w_id = :w_id;
-- Get district info and increment next_o_id; capture via \\gset
UPDATE {district} SET d_next_o_id = d_next_o_id + 1 WHERE d_id = :d_id AND d_w_id = :w_id RETURNING d_next_o_id - 1 AS o_id, d_tax;
\\gset
-- Get customer discount
SELECT c_discount, c_last, c_credit FROM {customer} WHERE c_id = :c_id AND c_d_id = :d_id AND c_w_id = :w_id;
-- Insert order (ON CONFLICT handles rare EPQ retry race at high concurrency)
INSERT INTO {orders} (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local)
VALUES (:o_id, :d_id, :w_id, :c_id, now(), 0, 10, 1)
ON CONFLICT (o_id, o_d_id, o_w_id) DO NOTHING;
-- Insert new_order
INSERT INTO {new_order} (no_o_id, no_d_id, no_w_id)
VALUES (:o_id, :d_id, :w_id)
ON CONFLICT (no_o_id, no_d_id, no_w_id) DO NOTHING;
{ol_block}
-- 1% rollback: simulates invalid item detection per TPC-C spec.
-- After all work is done, roll back — exercises full UNDO chain reversal.
\\if :rollback_pct = 1
ROLLBACK;
\\else
COMMIT;
\\endif
"""


def _payment_script(am: AccessMethod, config: TproccConfig) -> str:
    """Payment transaction (43% of mix).

    Customer looked up by c_id (pgbench can't do cursor-based middle-row lookup by name).
    """
    w = config.warehouses
    warehouse = get_table_name("warehouse", am)
    district = get_table_name("district", am)
    customer = get_table_name("customer", am)
    history = get_table_name("history", am)

    return f"""\\set w_id random(1, {w})
\\set d_id random(1, 10)
\\set c_id random(1, 3000)
\\set h_amount random(1, 5000)
BEGIN;
-- Update warehouse YTD
UPDATE {warehouse} SET w_ytd = w_ytd + :h_amount WHERE w_id = :w_id;
-- Update district YTD
UPDATE {district} SET d_ytd = d_ytd + :h_amount WHERE d_id = :d_id AND d_w_id = :w_id;
-- Update customer balance and counters
UPDATE {customer} SET
    c_balance = c_balance - :h_amount,
    c_ytd_payment = c_ytd_payment + :h_amount,
    c_payment_cnt = c_payment_cnt + 1
WHERE c_id = :c_id AND c_d_id = :d_id AND c_w_id = :w_id;
-- Insert history record
INSERT INTO {history} (h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date, h_amount, h_data)
VALUES (:c_id, :d_id, :w_id, :d_id, :w_id, now(), :h_amount, 'payment_data_here_pad');
COMMIT;
"""


def _order_status_script(am: AccessMethod, config: TproccConfig) -> str:
    """Order-Status transaction (4% of mix). Read-only."""
    w = config.warehouses
    customer = get_table_name("customer", am)
    orders = get_table_name("orders", am)
    order_line = get_table_name("order_line", am)

    return f"""\\set w_id random(1, {w})
\\set d_id random(1, 10)
\\set c_id random(1, 3000)
BEGIN;
-- Get customer info
SELECT c_balance, c_first, c_middle, c_last FROM {customer} WHERE c_id = :c_id AND c_d_id = :d_id AND c_w_id = :w_id;
-- Get latest order
SELECT o_id, o_entry_d, o_carrier_id FROM {orders} WHERE o_w_id = :w_id AND o_d_id = :d_id AND o_c_id = :c_id ORDER BY o_id DESC LIMIT 1;
-- Get order lines for that order (use subquery since pgbench can't store results)
SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d
FROM {order_line}
WHERE ol_w_id = :w_id AND ol_d_id = :d_id
  AND ol_o_id = (SELECT max(o_id) FROM {orders} WHERE o_w_id = :w_id AND o_d_id = :d_id AND o_c_id = :c_id);
COMMIT;
"""


def _delivery_script(am: AccessMethod, config: TproccConfig) -> str:
    """Delivery transaction (4% of mix).

    Processes 1 district per call (TPC-C spec says 10, but pgbench can't loop).
    Uses SKIP LOCKED to avoid blocking on contested new_order rows.
    Captures deleted order via RETURNING + \\gset for consistent follow-up ops.
    """
    w = config.warehouses
    new_order = get_table_name("new_order", am)
    orders = get_table_name("orders", am)
    order_line = get_table_name("order_line", am)
    customer = get_table_name("customer", am)

    return f"""\\set w_id random(1, {w})
\\set d_id random(1, 10)
\\set carrier_id random(1, 10)
BEGIN;
-- Find and delete oldest undelivered order in this district.
-- SKIP LOCKED avoids blocking when multiple delivery txns target same district.
DELETE FROM {new_order}
WHERE ctid = (
    SELECT ctid FROM {new_order}
    WHERE no_w_id = :w_id AND no_d_id = :d_id
    ORDER BY no_o_id LIMIT 1
    FOR UPDATE SKIP LOCKED
)
RETURNING no_o_id AS del_o_id;
\\gset
-- Update the order's carrier
UPDATE {orders} SET o_carrier_id = :carrier_id
WHERE o_w_id = :w_id AND o_d_id = :d_id AND o_id = :del_o_id;
-- Update order_line delivery dates
UPDATE {order_line} SET ol_delivery_d = now()
WHERE ol_w_id = :w_id AND ol_d_id = :d_id AND ol_o_id = :del_o_id;
-- Update customer balance with sum of that order's line amounts
UPDATE {customer} SET
    c_balance = c_balance + COALESCE((
        SELECT SUM(ol_amount) FROM {order_line}
        WHERE ol_w_id = :w_id AND ol_d_id = :d_id AND ol_o_id = :del_o_id
    ), 0),
    c_delivery_cnt = c_delivery_cnt + 1
WHERE c_w_id = :w_id AND c_d_id = :d_id
  AND c_id = (SELECT o_c_id FROM {orders}
              WHERE o_w_id = :w_id AND o_d_id = :d_id AND o_id = :del_o_id);
COMMIT;
"""


def _stock_level_script(am: AccessMethod, config: TproccConfig) -> str:
    """Stock-Level transaction (4% of mix). Read-only.

    Counts distinct items below threshold in last 20 orders for a district.
    """
    w = config.warehouses
    district = get_table_name("district", am)
    order_line = get_table_name("order_line", am)
    stock = get_table_name("stock", am)

    return f"""\\set w_id random(1, {w})
\\set d_id random(1, 10)
\\set threshold random(10, 20)
BEGIN;
SELECT COUNT(DISTINCT s_i_id)
FROM {stock}
JOIN {order_line} ON ol_i_id = s_i_id AND ol_w_id = s_w_id
WHERE s_w_id = :w_id
  AND s_quantity < :threshold
  AND ol_w_id = :w_id
  AND ol_d_id = :d_id
  AND ol_o_id >= (SELECT d_next_o_id - 20 FROM {district} WHERE d_id = :d_id AND d_w_id = :w_id)
  AND ol_o_id < (SELECT d_next_o_id FROM {district} WHERE d_id = :d_id AND d_w_id = :w_id);
COMMIT;
"""


# Map transaction types to their script generators
_SCRIPT_GENERATORS = {
    "neworder": _new_order_script,
    "payment": _payment_script,
    "orderstatus": _order_status_script,
    "delivery": _delivery_script,
    "stocklevel": _stock_level_script,
}


def generate_scripts(config: TproccConfig, am: AccessMethod, script_dir: str) -> dict:
    """Generate all pgbench SQL scripts for the given access method.

    Returns dict mapping txn_name -> file_path.
    """
    os.makedirs(script_dir, exist_ok=True)
    paths = {}

    for txn_name, generator in _SCRIPT_GENERATORS.items():
        filename = f"{txn_name}_{am.value}.sql"
        filepath = os.path.join(script_dir, filename)
        content = generator(am, config)

        with open(filepath, "w") as f:
            f.write(content)

        paths[txn_name] = filepath
        logger.debug("Generated script: %s", filepath)

    logger.info("Generated %d pgbench scripts for %s in %s", len(paths), am.value, script_dir)
    return paths
