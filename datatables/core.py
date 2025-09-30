from typing import Optional, Type
from sqlalchemy import Select, select
from sqlalchemy.ext.asyncio import AsyncSession
from .database import DatabaseBackend, SQLAlchemyBackend  # Add
from .schema import DataTablesRequest


class DataTables:
    def __init__(
        self,
        db_session: AsyncSession,
        model: Type,
        base_statment: Select = None,
        db_backend: Optional[DatabaseBackend] = None,
    ):
        """
        Initializes the DataTables processor.

        Args:
            db_session:  SQLAlchemy Session (or an adaptable object for other ORMs)
            model: The SQLAlchemy model representing the data.
        """
        self.model = model
        self.base_statment = base_statment
        if db_backend is None:
            self.db_backend = SQLAlchemyBackend(db_session)
            self.db_backend.model = model
        else:
            self.db_backend = db_backend

    async def get_query(self):
        if self.base_statment is None:
            return select(self.model)
        else:
            return self.base_statment

    async def process(self, request_data: DataTablesRequest):
        """
        Processes the DataTables request and returns the response.
        """

        # -- Base SELECT Statement --
        stmt = await self.get_query()

        # -- Total Records (Unfiltered) --
        records_total = await self.db_backend.get_total_records(stmt)

        # -- Apply Search Filter (Global Search) --
        stmt = await self.db_backend.get_filtered_query(stmt, request_data)

        # -- Count After Filtering --
        records_filtered = await self.db_backend.get_filtered_records(stmt)

        # -- Apply Ordering --
        stmt = await self.db_backend.apply_ordering(stmt, request_data)

        # -- Apply Pagination --
        stmt = stmt.offset(request_data.start).limit(request_data.length)

        # -- Execute Final Query --
        data = await self.db_backend.execute_query(stmt)

        return {
            "draw": request_data.draw,
            "recordsTotal": records_total,
            "recordsFiltered": records_filtered,
            "data": data,
        }
