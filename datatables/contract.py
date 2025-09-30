from typing import Type
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import Select
from sqlalchemy.orm import DeclarativeBase


class DatabaseBackend:
    model: Type = None

    def __init__(self, db_session: AsyncSession):
        self.db_session = db_session  # Could be SQLAlchemy, Databases, etc.

    def get_total_records(self, model: DeclarativeBase) -> int:
        """Count the total number of records (no filters)"""
        raise NotImplementedError

    def get_filtered_records(self, query: Select) -> int:
        """Count filtered number of records (with search filters)"""
        raise NotImplementedError

    def execute_query(self, query: Select):
        """Executes the final, constructed query"""
        raise NotImplementedError
