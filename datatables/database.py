from typing import Type
from sqlalchemy import Select, func, select
from sqlalchemy.ext.asyncio import AsyncSession
from .schema import DataTablesRequest
from .utils import global_filter, column_filter, order_column
from .contract import DatabaseBackend


class SQLAlchemyBackend(DatabaseBackend):  # Specific database backend
    db_session: AsyncSession = None

    def __init__(self, db_session):
        super().__init__(db_session)

    async def get_total_records(self, model: Type) -> int:
        stmt = select(func.count()).select_from(model)
        result = await self.db_session.execute(stmt)
        return result.scalar_one()

    async def get_filtered_records(self, stmt: Select) -> int:
        count_stmt = select(func.count()).select_from(stmt.subquery())
        result = await self.db_session.execute(count_stmt)
        return result.scalar_one()

    async def execute_query(self, stmt: Select):
        async with self.db_session as session:  # ensures connection is returned
            result = await session.execute(stmt)
            return result.scalars().all()

    async def get_filtered_query(self, stmt: Select, request_data: DataTablesRequest):
        search_value = (
            request_data.search.value.strip() if request_data.search.value else ""
        )
        stmt = global_filter(search_value, stmt, request_data.columns, self.model)

        stmt = column_filter(stmt, request_data.columns, self.model)

        return stmt

    async def apply_ordering(
        self, stmt: Select, request_data: DataTablesRequest
    ) -> Select:
        return order_column(self.model, stmt, request_data)
