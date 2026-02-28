from sqlalchemy import Select, and_, or_, cast, Date, DateTime, TIMESTAMP
from sqlalchemy.orm import aliased

from .exceptions import ConfigurationError
from .exceptions import InvalidColumnError
from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy import String, Text, Integer, not_
from .schema import DataTablesRequest, DataTablesColumn
from .enum import MatchMode


def build_condition(column_attr, match_mode: MatchMode, value: str):
    """
    Build a SQLAlchemy filter condition based on column type and match mode.
    Handles String/Text, Integer, and DateTime/Date/TIMESTAMP columns.
    """
    try:
        column_type = column_attr.type
    except AttributeError:
        # hybrid_property or unsupported type
        return None

    if isinstance(column_type, (String, Text)):
        if match_mode == MatchMode.CONTAINS:
            return column_attr.ilike(f"%{value}%")
        elif match_mode == MatchMode.EQUALS:
            return column_attr == value
        elif match_mode == MatchMode.STARTS_WITH:
            return column_attr.ilike(f"{value}%")
        elif match_mode == MatchMode.ENDS_WITH:
            return column_attr.ilike(f"%{value}")
        elif match_mode == MatchMode.NOT_CONTAINS:
            return not_(column_attr.ilike(f"%{value}%"))
        elif match_mode == MatchMode.NOT_EQUALS:
            return column_attr != value

    elif isinstance(column_type, Integer):
        try:
            int_value = int(value)
            if match_mode == MatchMode.EQUALS:
                return column_attr == int_value
            elif match_mode == MatchMode.NOT_EQUALS:
                return column_attr != int_value
        except ValueError:
            pass  # Ignore invalid integers

    elif isinstance(column_type, (DateTime, Date, TIMESTAMP)):
        db_date_only = cast(column_attr, Date)
        col_as_str = cast(db_date_only, String)
        clean_value = value.split("T")[0]

        if match_mode == MatchMode.CONTAINS:
            return col_as_str.ilike(f"%{clean_value}%")
        elif match_mode == MatchMode.EQUALS:
            return col_as_str == clean_value
        elif match_mode == MatchMode.STARTS_WITH:
            return col_as_str.ilike(f"{clean_value}%")
        elif match_mode == MatchMode.ENDS_WITH:
            return col_as_str.ilike(f"%{clean_value}")
        elif match_mode == MatchMode.NOT_CONTAINS:
            return not_(col_as_str.ilike(f"%{clean_value}%"))
        elif match_mode == MatchMode.NOT_EQUALS:
            return col_as_str != clean_value

    return None


def resolve_column(model, column_path: str, joins: dict, aliased_models: dict):
    """
    Resolve a dotted column path (e.g. 'user.name') to a SQLAlchemy column attribute.
    Handles relationship traversal with aliased joins.
    Returns (column_attr, current_model) or (None, None) on failure.
    """
    current_model = model
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
            current_attr = getattr(current_model, part)

    return current_attr


def apply_joins(stmt: Select, joins: dict) -> Select:
    """Apply all accumulated joins to the statement."""
    for key, (relation_attr, aliased_model) in joins.items():
        stmt = stmt.join(aliased_model, relation_attr)
    return stmt


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
                    current_attr = resolve_column(
                        model, column_path, joins, aliased_models
                    )

                    if current_attr is not None:
                        condition = build_condition(
                            current_attr, MatchMode.CONTAINS, search_value
                        )
                        if condition is not None:
                            search_conditions.append(condition)

                except AttributeError:
                    raise InvalidColumnError(
                        f"Invalid column path: {column_path}"
                    )

        stmt = apply_joins(stmt, joins)

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
            current_attr = resolve_column(model, field, joins, aliased_models)

            if current_attr is not None:
                condition = build_condition(
                    current_attr, MatchMode.CONTAINS, value
                )
                if condition is not None:
                    column_conditions.append(condition)

        except AttributeError:
            raise InvalidColumnError(f"Invalid column path: {field}")

    stmt = apply_joins(stmt, joins)

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
            joins = {}
            aliased_models = {}
            order_col = resolve_column(model, col_name, joins, aliased_models)

            stmt = apply_joins(stmt, joins)

            # Apply ordering
            if direction == "asc":
                stmt = stmt.order_by(order_col.asc())
            elif direction == "desc":
                stmt = stmt.order_by(order_col.desc())
            else:
                raise ConfigurationError("Order direction must be 'asc' or 'desc'")

        except AttributeError:
            raise InvalidColumnError(f"Invalid column for ordering: {col_name}")

    return stmt
