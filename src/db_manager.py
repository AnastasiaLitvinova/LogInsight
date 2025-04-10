from dataclasses import dataclass
import logging
import asyncpg
from asyncpg.pool import Pool
from contextlib import asynccontextmanager
from typing import Dict, List, Tuple


@dataclass(frozen=True)
class DBConfig:
    """Database connection configuration"""
    host: str
    port: str
    dbname: str
    user: str
    password: str


class DatabaseManager:
    """Database connection pool manager with async support"""
    _pool: Pool = None  # type: ignore

    def __init__(self, config: DBConfig):
        self.config = config
        self.logger = logging.getLogger("file_logger")

    async def initialize(self):
        """Initialize connection pool"""
        self._pool = await asyncpg.create_pool(
            min_size=2,
            max_size=10,
            host=self.config.host,
            port=self.config.port,
            database=self.config.dbname,
            user=self.config.user,
            password=self.config.password
        )
        self.logger.info("Initialized database connection pool")

    @asynccontextmanager
    async def get_connection(self):
        """Get async connection from pool"""
        if not self._pool:
            await self.initialize()

        conn = await self._pool.acquire()
        try:
            yield conn
        finally:
            await self._pool.release(conn)

    async def create_table(self, table_name: str, columns: Dict[str, str]) -> None:
        """Create table with specified schema"""
        columns_sql = ', '.join(
            [f'{k} {v}' for k, v in columns.items()]
        )

        async with self.get_connection() as conn:
            await conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    {columns_sql}
                )
                """
            )
            self.logger.info(f"Created table {table_name}")

    async def bulk_insert(self, table_name: str, columns: List[str], rows: List[Tuple]) -> int:
        """Async bulk insert using COPY command"""
        if not rows:
            return 0

        async with self.get_connection() as conn:
            result = await conn.copy_records_to_table(
                table_name,
                records=rows,
                columns=columns
            )
            self.logger.info(f"Inserted {len(rows)} records into {table_name}")
            return len(rows)
