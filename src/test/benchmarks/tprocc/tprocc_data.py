"""TPROC-C data population via SQL generate_series and INSERT...SELECT."""

import logging
import time

from .tprocc_config import AccessMethod, TproccConfig
from .tprocc_schema import get_table_name, run_sql

logger = logging.getLogger(__name__)


def _random_str(length: int) -> str:
    """SQL expression for a random string of given length.

    md5() produces 32 hex chars. For lengths > 32, concatenate multiple calls.
    """
    if length <= 32:
        return f"substr(md5(random()::text), 1, {length})"
    # Chain multiple md5 calls
    reps = (length + 31) // 32
    parts = " || ".join(f"md5(random()::text)" for _ in range(reps))
    return f"substr({parts}, 1, {length})"


def _random_zip() -> str:
    """SQL expression for a random 9-char zip (4 digits + 11111)."""
    return "lpad((random()*9999)::int::text, 4, '0') || '11111'"


def _random_phone() -> str:
    """SQL expression for a 16-char phone number."""
    return "lpad((random()*9999999999999999)::bigint::text, 16, '0')"


def populate_item(config: TproccConfig, am: AccessMethod) -> None:
    """Populate the item table (100,000 rows, same for all warehouses)."""
    tbl = get_table_name("item", am)
    logger.info("Populating %s (100,000 rows)...", tbl)
    t0 = time.time()

    sql = f"""
INSERT INTO {tbl} (i_id, i_im_id, i_name, i_price, i_data)
SELECT
    gs AS i_id,
    (random() * 10000)::int AS i_im_id,
    {_random_str(14)} AS i_name,
    (100 + random() * 9900)::int AS i_price,
    CASE WHEN random() < 0.10
        THEN substr({_random_str(24)} || 'ORIGINAL' || {_random_str(18)}, 1, 50)
        ELSE {_random_str(50)}
    END AS i_data
FROM generate_series(1, 100000) gs;
"""
    run_sql(sql, config)
    logger.info("  %s populated in %.1fs", tbl, time.time() - t0)


def populate_warehouse(config: TproccConfig, am: AccessMethod) -> None:
    """Populate the warehouse table."""
    tbl = get_table_name("warehouse", am)
    w = config.warehouses
    logger.info("Populating %s (%d rows)...", tbl, w)
    t0 = time.time()

    sql = f"""
INSERT INTO {tbl} (w_id, w_name, w_street_1, w_street_2, w_city, w_state, w_zip, w_tax, w_ytd)
SELECT
    gs AS w_id,
    {_random_str(10)} AS w_name,
    {_random_str(20)} AS w_street_1,
    {_random_str(20)} AS w_street_2,
    {_random_str(20)} AS w_city,
    {_random_str(2)} AS w_state,
    {_random_zip()} AS w_zip,
    (random() * 2000)::int AS w_tax,
    30000000 AS w_ytd
FROM generate_series(1, {w}) gs;
"""
    run_sql(sql, config)
    logger.info("  %s populated in %.1fs", tbl, time.time() - t0)


def populate_district(config: TproccConfig, am: AccessMethod) -> None:
    """Populate the district table (10 per warehouse)."""
    tbl = get_table_name("district", am)
    w = config.warehouses
    total = w * 10
    logger.info("Populating %s (%d rows)...", tbl, total)
    t0 = time.time()

    sql = f"""
INSERT INTO {tbl} (d_id, d_w_id, d_name, d_street_1, d_street_2, d_city, d_state, d_zip, d_tax, d_ytd, d_next_o_id)
SELECT
    ((gs - 1) % 10) + 1 AS d_id,
    ((gs - 1) / 10) + 1 AS d_w_id,
    {_random_str(10)} AS d_name,
    {_random_str(20)} AS d_street_1,
    {_random_str(20)} AS d_street_2,
    {_random_str(20)} AS d_city,
    {_random_str(2)} AS d_state,
    {_random_zip()} AS d_zip,
    (random() * 2000)::int AS d_tax,
    3000000 AS d_ytd,
    3001 AS d_next_o_id
FROM generate_series(1, {total}) gs;
"""
    run_sql(sql, config)
    logger.info("  %s populated in %.1fs", tbl, time.time() - t0)


def populate_customer(config: TproccConfig, am: AccessMethod) -> None:
    """Populate the customer table (3000 per district = 30,000 per warehouse)."""
    tbl = get_table_name("customer", am)
    w = config.warehouses
    total = w * 10 * 3000
    logger.info("Populating %s (%d rows)...", tbl, total)
    t0 = time.time()

    sql = f"""
INSERT INTO {tbl} (
    c_id, c_d_id, c_w_id, c_first, c_middle, c_last,
    c_street_1, c_street_2, c_city, c_state, c_zip, c_phone,
    c_since, c_credit, c_credit_lim, c_discount, c_balance,
    c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_data
)
SELECT
    ((gs - 1) % 3000) + 1 AS c_id,
    (((gs - 1) / 3000) % 10) + 1 AS c_d_id,
    ((gs - 1) / 30000) + 1 AS c_w_id,
    {_random_str(16)} AS c_first,
    'OE' AS c_middle,
    'LASTNAME' || lpad(((gs - 1) % 1000)::text, 4, '0') AS c_last,
    {_random_str(20)} AS c_street_1,
    {_random_str(20)} AS c_street_2,
    {_random_str(20)} AS c_city,
    {_random_str(2)} AS c_state,
    {_random_zip()} AS c_zip,
    {_random_phone()} AS c_phone,
    now() AS c_since,
    CASE WHEN random() < 0.10 THEN 'BC' ELSE 'GC' END AS c_credit,
    5000000 AS c_credit_lim,
    (random() * 5000)::int AS c_discount,
    -1000 AS c_balance,
    1000 AS c_ytd_payment,
    1 AS c_payment_cnt,
    0 AS c_delivery_cnt,
    {_random_str(200)} AS c_data
FROM generate_series(1, {total}) gs;
"""
    run_sql(sql, config)
    logger.info("  %s populated in %.1fs", tbl, time.time() - t0)


def populate_history(config: TproccConfig, am: AccessMethod) -> None:
    """Populate history table (1 per customer initially)."""
    tbl = get_table_name("history", am)
    cust_tbl = get_table_name("customer", am)
    w = config.warehouses
    total = w * 10 * 3000
    logger.info("Populating %s (%d rows)...", tbl, total)
    t0 = time.time()

    sql = f"""
INSERT INTO {tbl} (h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date, h_amount, h_data)
SELECT
    c_id, c_d_id, c_w_id, c_d_id, c_w_id,
    now(), 1000, {_random_str(24)}
FROM {cust_tbl};
"""
    run_sql(sql, config)
    logger.info("  %s populated in %.1fs", tbl, time.time() - t0)


def populate_orders(config: TproccConfig, am: AccessMethod) -> None:
    """Populate orders, new_order, and order_line tables."""
    orders_tbl = get_table_name("orders", am)
    no_tbl = get_table_name("new_order", am)
    ol_tbl = get_table_name("order_line", am)
    w = config.warehouses
    total_orders = w * 10 * 3000
    total_ol = total_orders * 10  # fixed 10 lines per order for simplicity
    total_no = w * 10 * 900

    logger.info("Populating %s (%d rows)...", orders_tbl, total_orders)
    t0 = time.time()

    # Orders: 3000 per district, random customer permutation approximated
    sql = f"""
INSERT INTO {orders_tbl} (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local)
SELECT
    ((gs - 1) % 3000) + 1 AS o_id,
    (((gs - 1) / 3000) % 10) + 1 AS o_d_id,
    ((gs - 1) / 30000) + 1 AS o_w_id,
    ((gs - 1) % 3000) + 1 AS o_c_id,
    now() - ((3000 - ((gs - 1) % 3000)) || ' seconds')::interval AS o_entry_d,
    CASE WHEN ((gs - 1) % 3000) < 2100 THEN (random() * 9 + 1)::int ELSE 0 END AS o_carrier_id,
    10 AS o_ol_cnt,
    1 AS o_all_local
FROM generate_series(1, {total_orders}) gs;
"""
    run_sql(sql, config)
    logger.info("  %s populated in %.1fs", orders_tbl, time.time() - t0)

    # New-Order: last 900 orders per district
    logger.info("Populating %s (%d rows)...", no_tbl, total_no)
    t0 = time.time()
    sql = f"""
INSERT INTO {no_tbl} (no_o_id, no_d_id, no_w_id)
SELECT o_id, o_d_id, o_w_id
FROM {orders_tbl}
WHERE o_id > 2100;
"""
    run_sql(sql, config)
    logger.info("  %s populated in %.1fs", no_tbl, time.time() - t0)

    # Order-Line: 10 lines per order
    logger.info("Populating %s (%d rows)...", ol_tbl, total_ol)
    t0 = time.time()
    sql = f"""
INSERT INTO {ol_tbl} (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_delivery_d, ol_quantity, ol_amount, ol_dist_info)
SELECT
    o_id AS ol_o_id,
    o_d_id AS ol_d_id,
    o_w_id AS ol_w_id,
    ln AS ol_number,
    (random() * 99999 + 1)::int AS ol_i_id,
    o_w_id AS ol_supply_w_id,
    CASE WHEN o_carrier_id > 0 THEN o_entry_d ELSE '1970-01-01'::timestamp END AS ol_delivery_d,
    5 AS ol_quantity,
    CASE WHEN o_carrier_id > 0 THEN 0 ELSE (random() * 999900 + 1)::int END AS ol_amount,
    {_random_str(24)} AS ol_dist_info
FROM {orders_tbl}, generate_series(1, 10) ln;
"""
    run_sql(sql, config)
    logger.info("  %s populated in %.1fs", ol_tbl, time.time() - t0)


def populate_stock(config: TproccConfig, am: AccessMethod) -> None:
    """Populate the stock table (100,000 per warehouse)."""
    tbl = get_table_name("stock", am)
    w = config.warehouses
    total = w * 100000
    logger.info("Populating %s (%d rows)...", tbl, total)
    t0 = time.time()

    sql = f"""
INSERT INTO {tbl} (
    s_i_id, s_w_id, s_quantity,
    s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05,
    s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10,
    s_ytd, s_order_cnt, s_remote_cnt, s_data
)
SELECT
    ((gs - 1) % 100000) + 1 AS s_i_id,
    ((gs - 1) / 100000) + 1 AS s_w_id,
    (random() * 90 + 10)::int AS s_quantity,
    {_random_str(24)} AS s_dist_01,
    {_random_str(24)} AS s_dist_02,
    {_random_str(24)} AS s_dist_03,
    {_random_str(24)} AS s_dist_04,
    {_random_str(24)} AS s_dist_05,
    {_random_str(24)} AS s_dist_06,
    {_random_str(24)} AS s_dist_07,
    {_random_str(24)} AS s_dist_08,
    {_random_str(24)} AS s_dist_09,
    {_random_str(24)} AS s_dist_10,
    0 AS s_ytd,
    0 AS s_order_cnt,
    0 AS s_remote_cnt,
    CASE WHEN random() < 0.10
        THEN substr({_random_str(24)} || 'ORIGINAL' || {_random_str(18)}, 1, 50)
        ELSE {_random_str(50)}
    END AS s_data
FROM generate_series(1, {total}) gs;
"""
    run_sql(sql, config)
    logger.info("  %s populated in %.1fs", tbl, time.time() - t0)


def populate_all(config: TproccConfig, am: AccessMethod) -> None:
    """Populate all TPROC-C tables for the given access method."""
    logger.info("=== Populating TPROC-C data for %s (W=%d) ===", am.value, config.warehouses)
    t0 = time.time()

    populate_item(config, am)
    populate_warehouse(config, am)
    populate_district(config, am)
    populate_customer(config, am)
    populate_history(config, am)
    populate_orders(config, am)
    populate_stock(config, am)

    elapsed = time.time() - t0
    logger.info("=== %s population complete in %.1fs ===", am.value, elapsed)


def vacuum_tables(config: TproccConfig, am: AccessMethod) -> None:
    """VACUUM ANALYZE all TPROC-C tables."""
    logger.info("Running VACUUM ANALYZE on %s tables...", am.value)
    tables = [
        "warehouse", "district", "customer", "history",
        "orders", "new_order", "order_line", "item", "stock",
    ]
    stmts = [f"VACUUM ANALYZE {get_table_name(t, am)}" for t in tables]
    sql = ";\n".join(stmts) + ";\n"
    run_sql(sql, config)
