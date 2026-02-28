from sqlalchemy import Select, and_, or_
from sqlalchemy.orm import aliased

from .exceptions import ConfigurationError
from .exceptions import InvalidColumnError
from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy import String, Text
from .schema import DataTablesRequest, DataTablesColumn


def global_filter(
    search_value: str,
    stmt: Select,
    columns: list[DataTablesColumn],
    model,
) -> Select:
    joins = {}
    aliased_models = {}
    if search_value:
        search_conditions = []
        for col in columns:
            if col and col.searchable:
                column_path = col.name
                try:
                    current_model = model
                    current_path = []
                    current_attr = None

                    parts = column_path.split(".")
                    for i, part in enumerate(parts):
                        current_path.append(part)
                        path_str = ".".join(current_path)

                        if i < len(parts) - 1:  # it's a relation
                            if path_str not in aliased_models:
                                relation_attr: InstrumentedAttribute = getattr(
                                    current_model, part
                                )
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
                                search_conditions.append(
                                    current_attr.ilike(f"%{search_value}%")
                                )
                        except AttributeError:
                            # In case of a hybrid_property or unsupported type, skip
                            pass

                except AttributeError:
                    raise InvalidColumnError(f"Invalid column path: {col['field']}")

        # Apply joins
        for key, (relation_attr, aliased_model) in joins.items():
            stmt = stmt.join(aliased_model, relation_attr)

        if search_conditions:
            stmt = stmt.where(or_(*search_conditions))
    return stmt


def column_filter(
    stmt: Select,
    columns: list[DataTablesColumn],
    model,
) -> Select:
    joins = {}
    aliased_models = {}
    column_conditions = []

    for col in columns:
        if not col.searchable:
            continue
        field = col.name
        value = col.search.value.strip() if col.search.value else ""
        if not value:
            continue

        try:
            current_model = model
            current_path = []
            current_attr = None

            parts = field.split(".")
            for i, part in enumerate(parts):
                current_path.append(part)
                path_str = ".".join(current_path)

                if i < len(parts) - 1:  # is relation
                    if path_str not in aliased_models:
                        relation_attr = getattr(current_model, part)
                        related_model = relation_attr.property.mapper.class_
                        aliased_model = aliased(related_model)
                        aliased_models[path_str] = aliased_model
                        joins[path_str] = (relation_attr, aliased_model)
                    current_model = aliased_models[path_str]
                else:  # final column
                    current_attr: Select = getattr(current_model, part)

            if current_attr is not None:
                condition = current_attr.ilike(f"%{value}%")
                if condition is not None:
                    column_conditions.append(condition)

        except AttributeError:
            raise InvalidColumnError(f"Invalid column path: {field}")

    for key, (relation_attr, aliased_model) in joins.items():
        stmt = stmt.join(aliased_model, relation_attr)

    if column_conditions:
        stmt = stmt.where(and_(*column_conditions))

    return stmt


def order_column(model: type, stmt: Select, request_data: DataTablesRequest) -> Select:
    if not request_data.order:
        return stmt
    for order in request_data.order:
        col_index = order.column
        col = request_data.columns[int(col_index)]
        if not col.orderable:
            continue
        col_name = col.name
        if not col_name:
            continue
        direction = order.dir

        try:
            current_model = model
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
