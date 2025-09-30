from typing import Optional, Type

from sqlalchemy import Select, String, Text, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased
from sqlalchemy.orm.attributes import InstrumentedAttribute

from .database import DatabaseBackend, SQLAlchemyBackend  # Add
from .exceptions import ConfigurationError, InvalidColumnError
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
        else:
            self.db_backend = db_backend

    async def get_query(self):
        if self.base_statment is None:
            return select(self.model)
        else:
            return self.base_statment

    async def get_filtered_query(self, stmt: Select, request_data: DataTablesRequest):
        search_value = request_data.search.get("value", "").strip() if request_data.search else ""
        joins = {}
        aliased_models = {}

        if search_value:
            search_conditions = []
            for col in request_data.columns:
                if col.get("searchable"):
                    column_path = col["name"]
                    try:
                        current_model = self.model
                        current_path = []
                        current_attr = None

                        parts = column_path.split(".")
                        for i, part in enumerate(parts):
                            current_path.append(part)
                            path_str = ".".join(current_path)

                            if i < len(parts) - 1:  # it's a relation
                                if path_str not in aliased_models:
                                    relation_attr: InstrumentedAttribute = getattr(current_model, part)
                                    related_model = relation_attr.property.mapper.class_
                                    aliased_model = aliased(related_model)
                                    aliased_models[path_str] = aliased_model
                                    joins[path_str] = (relation_attr, aliased_model)
                                current_model = aliased_models[path_str]
                            else:  # it's the final column
                                column_attr = getattr(current_model, part)
                                current_attr = column_attr

                        if current_attr is not None:
                            try:
                                column_type = current_attr.type
                                if isinstance(column_type, (String, Text)):
                                    search_conditions.append(current_attr.ilike(f"%{search_value}%"))
                            except AttributeError:
                                # In case of a hybrid_property or unsupported type, skip
                                pass

                    except AttributeError:
                        raise InvalidColumnError(f"Invalid column path: {col['name']}")

            # Apply joins
            for key, (relation_attr, aliased_model) in joins.items():
                stmt = stmt.join(aliased_model, relation_attr)

            if search_conditions:
                stmt = stmt.where(or_(*search_conditions))

        return stmt

    async def apply_ordering(self, stmt: Select, request_data: DataTablesRequest) -> Select:
        if not request_data.order:
            return stmt

        for order in request_data.order:
            col_index = order["column"]
            # print(f"Ordering by column: {col_name}")

            col_name = request_data.columns[int(col_index)]["name"]
            direction = order["dir"]

            try:
                current_model = self.model
                current_path = []
                aliased_models = {}
                joins_for_order = {}
                parts = col_name.split(".")

                for i, part in enumerate(parts):
                    current_path.append(part)
                    path_str = ".".join(current_path)

                    if i < len(parts) - 1:  # relation part
                        if path_str not in aliased_models:
                            relation_attr = getattr(current_model, part)
                            related_model = relation_attr.property.mapper.class_
                            aliased_model = aliased(related_model)
                            aliased_models[path_str] = aliased_model
                            joins_for_order[path_str] = (relation_attr, aliased_model)
                        current_model = aliased_models[path_str]
                    else:  # final column part
                        order_column = getattr(current_model, part)

                # Apply necessary joins
                for _, (relation_attr, aliased_model) in joins_for_order.items():
                    stmt = stmt.join(aliased_model, relation_attr)

                # Apply ordering
                if direction == "asc":
                    stmt = stmt.order_by(order_column.asc())
                elif direction == "desc":
                    stmt = stmt.order_by(order_column.desc())
                else:
                    raise ConfigurationError("Order direction must be 'asc' or 'desc'")

            except AttributeError:
                raise InvalidColumnError(f"Invalid column for ordering: {col_name}")

        return stmt

    async def process(self, request_data: DataTablesRequest):
        """
        Processes the DataTables request and returns the response.
        """

        # -- Base SELECT Statement --
        stmt = await self.get_query()

        # -- Total Records (Unfiltered) --
        records_total = await self.db_backend.get_total_records(stmt)

        # -- Apply Search Filter (Global Search) --
        stmt = await self.get_filtered_query(stmt, request_data)

        # -- Count After Filtering --
        records_filtered = await self.db_backend.get_filtered_records(stmt)

        # -- Apply Ordering --
        stmt = await self.apply_ordering(stmt, request_data)

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
