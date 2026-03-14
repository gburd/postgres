"""
Benchmark configuration: connection pooling, test parameters, and matrix definitions.
"""

import os
from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional


class TableWidth(Enum):
    NARROW = "narrow"      # 3-4 columns
    MEDIUM = "medium"      # 8-10 columns
    WIDE = "wide"          # 20+ columns


class DataDistribution(Enum):
    RANDOM = "random"
    CLUSTERED = "clustered"
    LOW_CARDINALITY = "low_cardinality"


class QueryPattern(Enum):
    FULL_SCAN = "full_scan"
    COLUMN_PROJECTION = "column_projection"
    FILTERED_SCAN = "filtered_scan"
    AGGREGATION = "aggregation"
    GROUP_BY = "group_by"
    INDEX_SCAN = "index_scan"


class ColumnType(Enum):
    INT = "integer"
    BIGINT = "bigint"
    TEXT = "text"
    BOOLEAN = "boolean"
    UUID = "uuid"
    TIMESTAMP = "timestamp"
    FLOAT = "double precision"
    NUMERIC = "numeric(12,2)"


ROW_COUNTS = [1_000, 10_000, 100_000, 1_000_000, 10_000_000]

# Smaller default for quick runs
DEFAULT_ROW_COUNTS = [1_000, 10_000, 100_000]


@dataclass
class ConnectionConfig:
    host: str = "localhost"
    port: int = 5432
    database: str = "benchmark_db"
    user: str = ""
    password: str = ""
    min_pool_size: int = 2
    max_pool_size: int = 10
    statement_cache_size: int = 100

    def __post_init__(self):
        self.host = os.environ.get("PGHOST", self.host)
        self.port = int(os.environ.get("PGPORT", str(self.port)))
        self.database = os.environ.get("PGDATABASE", self.database)
        self.user = os.environ.get("PGUSER", self.user) or os.environ.get("USER", "")
        self.password = os.environ.get("PGPASSWORD", self.password)

    @property
    def dsn(self) -> str:
        parts = [f"host={self.host}", f"port={self.port}", f"dbname={self.database}"]
        if self.user:
            parts.append(f"user={self.user}")
        if self.password:
            parts.append(f"password={self.password}")
        return " ".join(parts)


@dataclass
class TableSchema:
    """Defines a table schema for benchmarking."""
    name: str
    width: TableWidth
    columns: List[tuple]  # (col_name, ColumnType)
    index_columns: List[str] = field(default_factory=list)

    @property
    def column_names(self) -> List[str]:
        return [c[0] for c in self.columns]

    @property
    def column_types(self) -> List[ColumnType]:
        return [c[1] for c in self.columns]


# Pre-defined table schemas for the test matrix
NARROW_SCHEMA = TableSchema(
    name="bench_narrow",
    width=TableWidth.NARROW,
    columns=[
        ("id", ColumnType.BIGINT),
        ("val_int", ColumnType.INT),
        ("val_text", ColumnType.TEXT),
        ("flag", ColumnType.BOOLEAN),
    ],
    index_columns=["id"],
)

MEDIUM_SCHEMA = TableSchema(
    name="bench_medium",
    width=TableWidth.MEDIUM,
    columns=[
        ("id", ColumnType.BIGINT),
        ("category", ColumnType.INT),
        ("amount", ColumnType.NUMERIC),
        ("description", ColumnType.TEXT),
        ("is_active", ColumnType.BOOLEAN),
        ("created_at", ColumnType.TIMESTAMP),
        ("ref_uuid", ColumnType.UUID),
        ("score", ColumnType.FLOAT),
        ("status_code", ColumnType.INT),
        ("notes", ColumnType.TEXT),
    ],
    index_columns=["id", "category"],
)

WIDE_SCHEMA = TableSchema(
    name="bench_wide",
    width=TableWidth.WIDE,
    columns=[
        ("id", ColumnType.BIGINT),
        ("col_int_1", ColumnType.INT),
        ("col_int_2", ColumnType.INT),
        ("col_int_3", ColumnType.INT),
        ("col_bigint_1", ColumnType.BIGINT),
        ("col_bigint_2", ColumnType.BIGINT),
        ("col_text_1", ColumnType.TEXT),
        ("col_text_2", ColumnType.TEXT),
        ("col_text_3", ColumnType.TEXT),
        ("col_text_4", ColumnType.TEXT),
        ("col_bool_1", ColumnType.BOOLEAN),
        ("col_bool_2", ColumnType.BOOLEAN),
        ("col_bool_3", ColumnType.BOOLEAN),
        ("col_float_1", ColumnType.FLOAT),
        ("col_float_2", ColumnType.FLOAT),
        ("col_numeric_1", ColumnType.NUMERIC),
        ("col_numeric_2", ColumnType.NUMERIC),
        ("col_uuid_1", ColumnType.UUID),
        ("col_uuid_2", ColumnType.UUID),
        ("col_ts_1", ColumnType.TIMESTAMP),
        ("col_ts_2", ColumnType.TIMESTAMP),
    ],
    index_columns=["id", "col_int_1", "col_text_1"],
)

ALL_SCHEMAS = [NARROW_SCHEMA, MEDIUM_SCHEMA, WIDE_SCHEMA]


@dataclass
class BenchmarkConfig:
    """Top-level benchmark configuration."""
    connection: ConnectionConfig = field(default_factory=ConnectionConfig)
    schemas: List[TableSchema] = field(default_factory=lambda: list(ALL_SCHEMAS))
    row_counts: List[int] = field(default_factory=lambda: list(DEFAULT_ROW_COUNTS))
    distributions: List[DataDistribution] = field(
        default_factory=lambda: [
            DataDistribution.RANDOM,
            DataDistribution.CLUSTERED,
            DataDistribution.LOW_CARDINALITY,
        ]
    )
    query_patterns: List[QueryPattern] = field(
        default_factory=lambda: list(QueryPattern)
    )
    warmup_iterations: int = 2
    measure_iterations: int = 5
    seed: int = 42
    output_dir: str = "benchmark_results"
    enable_pg_stat_statements: bool = True
    enable_compression_stats: bool = True
    verbose: bool = False
    # Run the full matrix or a reduced subset
    full_matrix: bool = False

    def get_row_counts(self) -> List[int]:
        if self.full_matrix:
            return ROW_COUNTS
        return self.row_counts
