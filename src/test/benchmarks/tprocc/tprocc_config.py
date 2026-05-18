"""TPROC-C specific configuration: warehouses, transaction mix, defaults."""

from dataclasses import dataclass, field
from enum import Enum
from typing import List

from ..config import ConnectionConfig


class AccessMethod(Enum):
    HEAP = "heap"
    RECNO = "recno"


class TxnType(Enum):
    NEW_ORDER = "neworder"
    PAYMENT = "payment"
    ORDER_STATUS = "orderstatus"
    DELIVERY = "delivery"
    STOCK_LEVEL = "stocklevel"


# Standard TPROC-C transaction mix (weights must sum to 100)
DEFAULT_TXN_MIX = {
    TxnType.NEW_ORDER: 45,
    TxnType.PAYMENT: 43,
    TxnType.ORDER_STATUS: 4,
    TxnType.DELIVERY: 4,
    TxnType.STOCK_LEVEL: 4,
}


@dataclass
class TproccConfig:
    """Configuration for a TPROC-C benchmark run."""
    connection: ConnectionConfig = field(default_factory=ConnectionConfig)
    warehouses: int = 10
    duration: int = 120          # seconds per measurement run
    warmup: int = 10             # seconds to discard at start
    reps: int = 1                # repetitions per (am, clients) combo
    clients: List[int] = field(default_factory=lambda: [1, 2, 4, 8, 16, 32])
    txn_mix: dict = field(default_factory=lambda: dict(DEFAULT_TXN_MIX))
    skip_init: bool = False
    heap_only: bool = False
    recno_only: bool = False
    output_dir: str = "results"
    verbose: bool = False
    # Binary paths (default: find in PATH)
    psql_bin: str = "psql"
    pgbench_bin: str = "pgbench"

    @property
    def access_methods(self) -> List[AccessMethod]:
        if self.heap_only:
            return [AccessMethod.HEAP]
        if self.recno_only:
            return [AccessMethod.RECNO]
        return [AccessMethod.HEAP, AccessMethod.RECNO]

    @property
    def total_duration(self) -> int:
        """Total pgbench duration including warmup."""
        return self.duration + self.warmup


# Row counts per warehouse (TPROC-C spec)
ROWS_PER_WAREHOUSE = {
    "warehouse": 1,
    "district": 10,
    "customer": 30_000,
    "history": 30_000,
    "orders": 30_000,
    "new_order": 900,
    "order_line": 300_000,
    "item": 100_000,       # fixed, not per-warehouse
    "stock": 100_000,
}
