# fastapi_datatables/database.py
from typing import Any, Type

from sqlalchemy import Select, func, select


class DatabaseBackend:
    def __init__(self, db_session: Any):
        self.db_session = db_session  # Could be SQLAlchemy, Databases, etc.

    def get_total_records(self, model) -> int:
        """Count the total number of records (no filters)"""
        raise NotImplementedError

    def get_filtered_records(self, query) -> int:
        """Count filtered number of records (with search filters)"""
        raise NotImplementedError

    def execute_query(self, query):
        """Executes the final, constructed query"""
        raise NotImplementedError


class SQLAlchemyBackend(DatabaseBackend):  # Specific database backend
    def __init__(self, db_session):
        super().__init__(db_session)
        # Additional SQLAlchemy-specific initialization could go here

    async def get_total_records(self, model: Type) -> int:
        stmt = select(func.count()).select_from(model)
        result = await self.db_session.execute(stmt)
        return result.scalar_one()

    async def get_filtered_records(self, stmt: Select) -> int:
        count_stmt = select(func.count()).select_from(stmt.subquery())
        result = await self.db_session.execute(count_stmt)
        return result.scalar_one()

    async def execute_query(self, stmt: Select):
        result = await self.db_session.execute(stmt)
        return result.scalars().all()
