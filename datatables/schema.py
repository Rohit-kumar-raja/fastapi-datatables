# fastapi_datatables/models.py
from typing import Any, Dict, List, Optional

from pydantic import BaseModel
from typing import Generic, TypeVar, Optional

T = TypeVar("T")

class DataTablesRequest(BaseModel):
    draw: Optional[int] = 1  # Provide a default value
    start: Optional[int] = 0
    length: Optional[int] = 10
    search: Optional[Dict[str, str]] = {}
    order: Optional[List[Dict[str, Any]]] = []
    columns: Optional[List[Dict[str, Any]]] = []
    extra: Optional[Dict[str, Any]] = {}


class DataTablesResponse(BaseModel,Generic[T]):
    draw: int
    recordsTotal: int
    recordsFiltered: int
    data: Optional[T]
    error: Optional[str] = None
