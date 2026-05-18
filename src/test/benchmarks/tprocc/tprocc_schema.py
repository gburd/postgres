"""TPROC-C 9-table schema DDL for HEAP and RECNO variants."""

import logging
import subprocess

from .tprocc_config import AccessMethod, TproccConfig

logger = logging.getLogger(__name__)


def _table_suffix(am: AccessMethod) -> str:
    return "" if am == AccessMethod.HEAP else "_recno"


def _using_clause(am: AccessMethod) -> str:
    return "" if am == AccessMethod.HEAP else " USING recno"


def get_table_name(base: str, am: AccessMethod) -> str:
    return f"tprocc_{base}{_table_suffix(am)}"


def generate_create_ddl(am: AccessMethod) -> str:
    """Generate CREATE TABLE statements for all 9 TPROC-C tables."""
    sfx = _table_suffix(am)
    using = _using_clause(am)

    stmts = []

    # Warehouse
    # NOTE: monetary amounts stored as bigint cents, tax rates as integer
    # basis points (1/10000). This avoids PostgreSQL's variable-length numeric
    # type which causes "tuple does not fit on page" with RECNO in-place updates
    # when accumulated values grow in byte-width.
    stmts.append(f"""
CREATE TABLE IF NOT EXISTS tprocc_warehouse{sfx} (
    w_id        integer NOT NULL,
    w_name      char(10),
    w_street_1  char(20),
    w_street_2  char(20),
    w_city      char(20),
    w_state     char(2),
    w_zip       char(9),
    w_tax       integer,
    w_ytd       bigint
){using}""")

    # District
    stmts.append(f"""
CREATE TABLE IF NOT EXISTS tprocc_district{sfx} (
    d_id        integer NOT NULL,
    d_w_id      integer NOT NULL,
    d_name      char(10),
    d_street_1  char(20),
    d_street_2  char(20),
    d_city      char(20),
    d_state     char(2),
    d_zip       char(9),
    d_tax       integer,
    d_ytd       bigint,
    d_next_o_id integer
){using}""")

    # Customer
    stmts.append(f"""
CREATE TABLE IF NOT EXISTS tprocc_customer{sfx} (
    c_id            integer NOT NULL,
    c_d_id          integer NOT NULL,
    c_w_id          integer NOT NULL,
    c_first         char(16),
    c_middle        char(2),
    c_last          char(16),
    c_street_1      char(20),
    c_street_2      char(20),
    c_city          char(20),
    c_state         char(2),
    c_zip           char(9),
    c_phone         char(16),
    c_since         timestamp,
    c_credit        char(2),
    c_credit_lim    bigint,
    c_discount      integer,
    c_balance       bigint,
    c_ytd_payment   bigint,
    c_payment_cnt   integer,
    c_delivery_cnt  integer,
    c_data          char(200)
){using}""")

    # History (append-only, no primary key in TPROC-C spec)
    stmts.append(f"""
CREATE TABLE IF NOT EXISTS tprocc_history{sfx} (
    h_c_id      integer,
    h_c_d_id    integer,
    h_c_w_id    integer,
    h_d_id      integer,
    h_w_id      integer,
    h_date      timestamp,
    h_amount    bigint,
    h_data      char(24)
){using}""")

    # Orders
    stmts.append(f"""
CREATE TABLE IF NOT EXISTS tprocc_orders{sfx} (
    o_id            integer NOT NULL,
    o_d_id          integer NOT NULL,
    o_w_id          integer NOT NULL,
    o_c_id          integer NOT NULL DEFAULT 0,
    o_entry_d       timestamp NOT NULL DEFAULT '1970-01-01',
    o_carrier_id    integer NOT NULL DEFAULT 0,
    o_ol_cnt        integer NOT NULL DEFAULT 0,
    o_all_local     integer NOT NULL DEFAULT 0
){using}""")

    # New-Order
    stmts.append(f"""
CREATE TABLE IF NOT EXISTS tprocc_new_order{sfx} (
    no_o_id     integer NOT NULL,
    no_d_id     integer NOT NULL,
    no_w_id     integer NOT NULL
){using}""")

    # Order-Line
    stmts.append(f"""
CREATE TABLE IF NOT EXISTS tprocc_order_line{sfx} (
    ol_o_id         integer NOT NULL,
    ol_d_id         integer NOT NULL,
    ol_w_id         integer NOT NULL,
    ol_number       integer NOT NULL,
    ol_i_id         integer NOT NULL DEFAULT 0,
    ol_supply_w_id  integer NOT NULL DEFAULT 0,
    ol_delivery_d   timestamp NOT NULL DEFAULT '1970-01-01',
    ol_quantity     integer NOT NULL DEFAULT 0,
    ol_amount       bigint NOT NULL DEFAULT 0,
    ol_dist_info    char(24) NOT NULL DEFAULT ''
){using}""")

    # Item (static, shared across warehouses)
    stmts.append(f"""
CREATE TABLE IF NOT EXISTS tprocc_item{sfx} (
    i_id        integer NOT NULL,
    i_im_id     integer,
    i_name      char(24),
    i_price     integer,
    i_data      char(50)
){using}""")

    # Stock
    stmts.append(f"""
CREATE TABLE IF NOT EXISTS tprocc_stock{sfx} (
    s_i_id      integer NOT NULL,
    s_w_id      integer NOT NULL,
    s_quantity  integer,
    s_dist_01   char(24),
    s_dist_02   char(24),
    s_dist_03   char(24),
    s_dist_04   char(24),
    s_dist_05   char(24),
    s_dist_06   char(24),
    s_dist_07   char(24),
    s_dist_08   char(24),
    s_dist_09   char(24),
    s_dist_10   char(24),
    s_ytd       integer,
    s_order_cnt integer,
    s_remote_cnt integer,
    s_data      char(50)
){using}""")

    return ";\n".join(stmts) + ";\n"


def generate_index_ddl(am: AccessMethod) -> str:
    """Generate primary key and secondary indexes."""
    sfx = _table_suffix(am)
    stmts = []

    stmts.append(f"ALTER TABLE tprocc_warehouse{sfx} ADD PRIMARY KEY (w_id)")
    stmts.append(f"ALTER TABLE tprocc_district{sfx} ADD PRIMARY KEY (d_id, d_w_id)")
    stmts.append(f"ALTER TABLE tprocc_customer{sfx} ADD PRIMARY KEY (c_id, c_d_id, c_w_id)")
    stmts.append(f"ALTER TABLE tprocc_orders{sfx} ADD PRIMARY KEY (o_id, o_d_id, o_w_id)")
    stmts.append(f"ALTER TABLE tprocc_new_order{sfx} ADD PRIMARY KEY (no_o_id, no_d_id, no_w_id)")
    stmts.append(f"ALTER TABLE tprocc_order_line{sfx} ADD PRIMARY KEY (ol_o_id, ol_d_id, ol_w_id, ol_number)")
    stmts.append(f"ALTER TABLE tprocc_item{sfx} ADD PRIMARY KEY (i_id)")
    stmts.append(f"ALTER TABLE tprocc_stock{sfx} ADD PRIMARY KEY (s_i_id, s_w_id)")

    # Secondary indexes for common lookups
    stmts.append(f"CREATE INDEX idx_tprocc_customer_name{sfx} ON tprocc_customer{sfx} (c_w_id, c_d_id, c_last, c_first)")
    stmts.append(f"CREATE INDEX idx_tprocc_orders_cust{sfx} ON tprocc_orders{sfx} (o_w_id, o_d_id, o_c_id, o_id)")

    return ";\n".join(stmts) + ";\n"


def generate_drop_ddl(am: AccessMethod) -> str:
    """Generate DROP TABLE statements."""
    sfx = _table_suffix(am)
    tables = [
        "order_line", "new_order", "orders", "history",
        "customer", "stock", "item", "district", "warehouse",
    ]
    stmts = [f"DROP TABLE IF EXISTS tprocc_{t}{sfx} CASCADE" for t in tables]
    return ";\n".join(stmts) + ";\n"


def run_sql(sql: str, config: TproccConfig, on_error_stop: bool = True) -> None:
    """Execute SQL via psql.

    Args:
        sql: SQL to execute.
        config: Benchmark configuration (connection details).
        on_error_stop: If True, psql aborts on first SQL error (default).
            Set to False for DDL where DROP IF NOT EXISTS emits harmless NOTICEs.
    """
    conn = config.connection
    cmd = [config.psql_bin, "-X", "-q"]
    if on_error_stop:
        cmd += ["-v", "ON_ERROR_STOP=1"]
    if conn.host:
        cmd += ["-h", conn.host]
    if conn.port:
        cmd += ["-p", str(conn.port)]
    if conn.user:
        cmd += ["-U", conn.user]
    cmd += ["-d", conn.database]

    env = None
    if conn.password:
        import os
        env = dict(os.environ, PGPASSWORD=conn.password)

    result = subprocess.run(
        cmd,
        input=sql,
        capture_output=True,
        text=True,
        env=env,
    )
    if result.returncode != 0:
        logger.error("psql failed (rc=%d): %s", result.returncode, result.stderr[:500])
        raise RuntimeError(f"psql error: {result.stderr[:500]}")
    if result.stderr:
        # Log NOTICE messages but don't fail
        for line in result.stderr.strip().split("\n"):
            if "NOTICE" in line or "notice" in line:
                logger.debug(line)
            else:
                logger.warning("psql stderr: %s", line)


def create_tables(config: TproccConfig, am: AccessMethod) -> None:
    """Create TPROC-C tables for the given access method."""
    logger.info("Creating TPROC-C tables for %s...", am.value)
    # DROP may emit NOTICEs about non-existent tables — don't abort on those
    run_sql(generate_drop_ddl(am), config, on_error_stop=False)
    run_sql(generate_create_ddl(am), config)
    logger.info("Creating indexes for %s...", am.value)
    run_sql(generate_index_ddl(am), config)


def drop_tables(config: TproccConfig, am: AccessMethod) -> None:
    """Drop TPROC-C tables for the given access method."""
    logger.info("Dropping TPROC-C tables for %s...", am.value)
    run_sql(generate_drop_ddl(am), config)
