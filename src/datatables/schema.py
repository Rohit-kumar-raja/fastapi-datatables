# fastapi_datatables/models.py
from typing import Any, Dict, List, Optional, Generic, TypeVar
from pydantic import BaseModel

T = TypeVar("T")


# --- DataTables Request Models ---


class DataTablesSearch(BaseModel):
    value: str = ""
    regex: bool = False


class DataTablesColumn(BaseModel):
    data: str = ""
    name: str
    searchable: bool = True
    orderable: bool = True
    search: DataTablesSearch = DataTablesSearch()


class DataTablesOrder(BaseModel):
    column: int
    dir: str  # 'asc' or 'desc'


class DataTablesRequest(BaseModel):
    draw: int
    start: int = 0
    length: int = 10
    search: DataTablesSearch = DataTablesSearch()
    order: List[DataTablesOrder] = []
    columns: List[DataTablesColumn] = []
    extra: Optional[Dict[str, Any]] = None


# --- DataTables Response Model ---


class DataTablesResponse(BaseModel, Generic[T]):
    draw: int
    recordsTotal: int
    recordsFiltered: int
    data: Optional[T]
    error: Optional[str] = None
