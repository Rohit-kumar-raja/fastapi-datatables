# fastapi_datatables/exceptions.py


class DataTablesError(Exception):
    """Base class for DataTables errors."""

    pass


class ConfigurationError(DataTablesError):
    """Raised for configuration problems."""

    pass


class InvalidColumnError(DataTablesError):
    """Raised when operations on invalid column."""

    pass
