# fastapi_datatables/__init__.py
from .core import DataTables
from .database import DatabaseBackend, SQLAlchemyBackend  # Expose the classes.
from .schema import DataTablesRequest, DataTablesResponse
from .exceptions import ConfigurationError, DataTablesError, InvalidColumnError
from .enum import MatchMode
from .utils import build_condition

__version__ = "0.1.0"  # Initial version

__all__ = [  # This is for from package import *. Good for avoiding accident overwriting.
    "DataTables",
    "DatabaseBackend",
    "SQLAlchemyBackend",
    "DataTablesRequest",
    "DataTablesResponse",
    "DataTablesError",
    "ConfigurationError",
    "InvalidColumnError",
    "MatchMode",
    "build_condition",
]
