"""
Schema builder: creates matching HEAP and Orvos tables for A/B comparison.
"""

import logging
from typing import List, Optional

from .config import ColumnType, TableSchema
from .database import DatabaseManager

logger = logging.getLogger(__name__)


class SchemaBuilder:
    """Creates and manages benchmark table schemas for both HEAP and Orvos."""

    def __init__(self, db: DatabaseManager):
        self.db = db

    @staticmethod
    def _col_type_sql(col_type: ColumnType) -> str:
        return col_type.value

    def _create_table_ddl(
        self,
        schema: TableSchema,
        suffix: str,
        access_method: Optional[str] = None,
    ) -> str:
        """Generate CREATE TABLE DDL."""
        table_name = f"{schema.name}{suffix}"
        col_defs = []
        for col_name, col_type in schema.columns:
            type_sql = self._col_type_sql(col_type)
            if col_name == "id":
                col_defs.append(f"  {col_name} {type_sql} NOT NULL")
            else:
                col_defs.append(f"  {col_name} {type_sql}")

        ddl = f"CREATE TABLE {table_name} (\n"
        ddl += ",\n".join(col_defs)
        ddl += "\n)"
        if access_method:
            ddl += f" USING {access_method}"
        return ddl

    async def create_pair(
        self,
        schema: TableSchema,
        drop_existing: bool = True,
    ) -> tuple:
        """Create a HEAP and an Orvos table from the same schema.

        Returns (heap_table_name, orvos_table_name).
        """
        heap_name = f"{schema.name}_heap"
        orvos_name = f"{schema.name}_orvos"

        if drop_existing:
            await self.db.drop_table(heap_name)
            await self.db.drop_table(orvos_name)

        heap_ddl = self._create_table_ddl(schema, "_heap")
        orvos_ddl = self._create_table_ddl(schema, "_orvos", access_method="orvos")

        logger.info("Creating HEAP table: %s", heap_name)
        await self.db.execute(heap_ddl)

        logger.info("Creating Orvos table: %s", orvos_name)
        await self.db.execute(orvos_ddl)

        return heap_name, orvos_name

    async def create_indexes(
        self,
        schema: TableSchema,
        table_name: str,
    ) -> List[str]:
        """Create indexes on the specified columns. Returns index names."""
        created = []
        for col in schema.index_columns:
            idx_name = f"idx_{table_name}_{col}"
            ddl = f"CREATE INDEX {idx_name} ON {table_name} ({col})"
            logger.info("Creating index: %s", idx_name)
            await self.db.execute(ddl)
            created.append(idx_name)
        return created

    async def setup_benchmark_tables(
        self,
        schema: TableSchema,
        drop_existing: bool = True,
    ) -> dict:
        """Full setup: create table pair and indexes.

        Returns a dict with table names and index names.
        """
        heap_name, orvos_name = await self.create_pair(schema, drop_existing)

        heap_indexes = await self.create_indexes(schema, heap_name)
        orvos_indexes = await self.create_indexes(schema, orvos_name)

        return {
            "heap_table": heap_name,
            "orvos_table": orvos_name,
            "heap_indexes": heap_indexes,
            "orvos_indexes": orvos_indexes,
        }

    async def load_data(
        self,
        table_name: str,
        insert_sql: str,
        analyze: bool = True,
    ):
        """Execute an INSERT statement and optionally ANALYZE."""
        logger.info("Loading data into %s ...", table_name)
        await self.db.execute(insert_sql, timeout=600.0)
        if analyze:
            logger.info("Running VACUUM ANALYZE on %s ...", table_name)
            await self.db.vacuum_analyze(table_name)

    async def cleanup(self, schema: TableSchema):
        """Drop the HEAP and Orvos tables for a schema."""
        await self.db.drop_table(f"{schema.name}_heap")
        await self.db.drop_table(f"{schema.name}_orvos")
