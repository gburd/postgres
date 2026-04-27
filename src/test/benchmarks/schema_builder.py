"""
Schema builder: creates matching HEAP and Noxu tables for A/B comparison,
and optionally creates buffer-pool-specific tables for strategy comparison.
"""

import logging
from typing import Dict, List, Optional

from .config import BufferStrategy, ColumnType, PoolConfig, TableSchema
from .database import DatabaseManager

logger = logging.getLogger(__name__)


class SchemaBuilder:
    """Creates and manages benchmark table schemas for both HEAP and Noxu."""

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
        """Create a HEAP and an Noxu table from the same schema.

        Returns (heap_table_name, noxu_table_name).
        """
        heap_name = f"{schema.name}_heap"
        noxu_name = f"{schema.name}_noxu"

        if drop_existing:
            await self.db.drop_table(heap_name)
            await self.db.drop_table(noxu_name)

        heap_ddl = self._create_table_ddl(schema, "_heap")
        noxu_ddl = self._create_table_ddl(schema, "_noxu", access_method="noxu")

        logger.info("Creating HEAP table: %s", heap_name)
        await self.db.execute(heap_ddl)

        logger.info("Creating Noxu table: %s", noxu_name)
        await self.db.execute(noxu_ddl)

        return heap_name, noxu_name

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
        heap_name, noxu_name = await self.create_pair(schema, drop_existing)

        heap_indexes = await self.create_indexes(schema, heap_name)
        noxu_indexes = await self.create_indexes(schema, noxu_name)

        return {
            "heap_table": heap_name,
            "noxu_table": noxu_name,
            "heap_indexes": heap_indexes,
            "noxu_indexes": noxu_indexes,
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
        """Drop the HEAP and Noxu tables for a schema."""
        await self.db.drop_table(f"{schema.name}_heap")
        await self.db.drop_table(f"{schema.name}_noxu")

    # ------------------------------------------------------------------
    # Buffer pool strategy comparison methods
    # ------------------------------------------------------------------

    async def create_buffer_pool(self, pool_config: PoolConfig):
        """Create a dynamic buffer pool."""
        if pool_config.strategy == BufferStrategy.CLOCK:
            return  # default pool already exists
        ddl = (
            f"CREATE BUFFER POOL {pool_config.pool_name} "
            f"HANDLER {pool_config.handler} "
            f"SIZE '{pool_config.pool_size}'"
        )
        logger.info("Creating buffer pool: %s", pool_config.pool_name)
        try:
            await self.db.execute(ddl)
        except Exception as e:
            if "already exists" in str(e):
                logger.info("Buffer pool %s already exists, reusing", pool_config.pool_name)
            else:
                raise

    async def drop_buffer_pool(self, pool_config: PoolConfig):
        """Drop a dynamic buffer pool if it exists.

        NOTE: Skipping actual DROP due to trickle writer SIGSEGV bug.
        Pools are cleaned up on server restart.
        """
        if pool_config.strategy == BufferStrategy.CLOCK:
            return
        logger.info("Skipping DROP BUFFER POOL %s (trickle writer crash workaround)", pool_config.pool_name)

    async def create_table_in_pool(
        self,
        schema: TableSchema,
        pool_config: PoolConfig,
        suffix: str,
        drop_existing: bool = True,
    ) -> str:
        """Create a HEAP table in a specific buffer pool.

        Returns the table name.
        """
        table_name = f"{schema.name}{suffix}"

        if drop_existing:
            await self.db.drop_table(table_name)

        ddl = self._create_table_ddl(schema, suffix)
        if pool_config.strategy != BufferStrategy.CLOCK:
            ddl += f" WITH (buffer_pool = '{pool_config.pool_name}')"

        logger.info("Creating table %s in pool %s",
                     table_name,
                     pool_config.pool_name or "default")
        await self.db.execute(ddl)
        return table_name

    async def create_strategy_set(
        self,
        schema: TableSchema,
        pool_configs: List[PoolConfig],
        drop_existing: bool = True,
    ) -> Dict[BufferStrategy, str]:
        """Create one table per buffer strategy, each in its own pool.

        Returns {strategy: table_name}.
        """
        result = {}
        for pc in pool_configs:
            suffix = f"_{pc.strategy.value}"
            table_name = await self.create_table_in_pool(
                schema, pc, suffix, drop_existing
            )
            await self.create_indexes(schema, table_name)
            result[pc.strategy] = table_name
        return result

    async def cleanup_strategy_set(
        self,
        schema: TableSchema,
        pool_configs: List[PoolConfig],
    ):
        """Drop tables and pools for a strategy set."""
        for pc in pool_configs:
            suffix = f"_{pc.strategy.value}"
            await self.db.drop_table(f"{schema.name}{suffix}")
            await self.drop_buffer_pool(pc)
