from enum import Enum


class MatchMode(str, Enum):
    CONTAINS = "contains"
    EQUALS = "equals"
    STARTS_WITH = "startsWith"
    ENDS_WITH = "endsWith"
    NOT_CONTAINS = "notContains"
    NOT_EQUALS = "notEquals"
