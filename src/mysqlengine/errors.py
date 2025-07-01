from typing import Literal
from sqlcycli import errors as sqlerrors


# . Base Exceptions ---------------------------------------------------------------------------------
class MysqlEngineError(sqlerrors.MySQLError):
    """MysqlEngine error."""


class MysqlEngineKeyError(MysqlEngineError, KeyError):
    """MysqlEngine key error."""


class MysqlEngineArgumentError(MysqlEngineError, ValueError):
    """Invalid argument error."""


class MysqlEngineCriticalError(AssertionError):
    """MysqlEngine critical error."""


class MysqlEngineWarning(sqlerrors.Warning, MysqlEngineError):
    """MysqlEngine warning."""


# . Engine Exceptions -------------------------------------------------------------------------------
class EngineError(MysqlEngineError):
    """Engine error."""


# . Database Exceptions -----------------------------------------------------------------------------
class DatabaseError(MysqlEngineError):
    """Database error."""


class DatabaseDefinitionError(DatabaseError, MysqlEngineArgumentError):
    """Database definition error."""


class DatabaseMetadataError(DatabaseError, MysqlEngineArgumentError):
    """Database metadata error."""


class DatabaseArgumentError(DatabaseError, MysqlEngineArgumentError):
    """Invalid database argument error."""


class DatabaseNotExistsError(DatabaseError, MysqlEngineKeyError):
    """Database not exists error."""


class DatabaseCriticalError(DatabaseError, MysqlEngineCriticalError):
    """Database critical error."""


class DatabaseWarning(MysqlEngineWarning, DatabaseError):
    """Database warning."""


# . Table Exceptions --------------------------------------------------------------------------------
class TableError(DatabaseError):
    """Table error."""


class TableDefinitionError(TableError, DatabaseDefinitionError):
    """Table definition error."""


class TableMetadataError(TableError, DatabaseMetadataError):
    """Table metadata error."""


class TableArgumentError(TableError, DatabaseArgumentError):
    """Invalid table argument error."""


class TableNotExistsError(TableError, MysqlEngineKeyError):
    """Table not exists error."""


class TableCriticalError(TableError, MysqlEngineCriticalError):
    """Table critical error."""


class TableWarning(DatabaseWarning):
    """Table warning."""


# . Column Exceptions -------------------------------------------------------------------------------
class ColumnError(TableError):
    """Column error."""


class ColumnDefinitionError(ColumnError, TableDefinitionError):
    """Column definition error."""


class ColumnMetadataError(ColumnError, TableMetadataError):
    """Column metadata error."""


class ColumnArgumentError(ColumnError, TableArgumentError):
    """Invalid column argument error."""


class ColumnNotExistsError(ColumnError, MysqlEngineKeyError):
    """Column not exists error."""


class ColumnCriticalError(ColumnError, MysqlEngineCriticalError):
    """Column critical error."""


class ColumnWarning(ColumnError, TableWarning):
    """Column warning."""


# . Constraint Exceptions ---------------------------------------------------------------------------
class ConstraintError(TableError):
    """Constraint error."""


class ConstraintDefinitionError(ConstraintError, TableDefinitionError):
    """Constraint definition error."""


class ConstraintMetadataError(ConstraintError, TableMetadataError):
    """Constraint metadata error."""


class ConstraintArgumentError(ConstraintError, TableArgumentError):
    """Invalid constraint argument error."""


class ConstraintNotExistsError(ConstraintError, MysqlEngineKeyError):
    """Constraint not exists error."""


class ConstraintCriticalError(ConstraintError, MysqlEngineCriticalError):
    """Constraint critical error."""


class ConstraintWarning(ConstraintError, TableWarning):
    """Constraint warning."""


# . Index Exceptions --------------------------------------------------------------------------------
class IndexError(TableError):
    """Index error."""


class IndexDefinitionError(IndexError, TableDefinitionError):
    """Index definition error."""


class IndexMetadataError(IndexError, TableMetadataError):
    """Index metadata error."""


class IndexArgumentError(IndexError, TableArgumentError):
    """Invalid index argument error."""


class IndexNotExistsError(IndexError, MysqlEngineKeyError):
    """Index not exists error."""


class IndexCriticalError(IndexError, MysqlEngineCriticalError):
    """Index critical error."""


class IndexWarning(IndexError, TableWarning):
    """Index warning."""


# . Partition Exceptions ----------------------------------------------------------------------------
class PartitionError(TableError):
    """Partition error."""


class PartitionDefinitionError(PartitionError, TableDefinitionError):
    """Partition definition error."""


class PartitionMetadataError(PartitionError, TableMetadataError):
    """Partition metadata error."""


class PartitionArgumentError(PartitionError, TableArgumentError):
    """Invalid partition argument error."""


class PartitionNotExistsError(PartitionError, MysqlEngineKeyError):
    """Partition not exists error."""


class PartitionCriticalError(PartitionError, MysqlEngineCriticalError):
    """Partition critical error."""


class PartitionWarning(PartitionError, TableWarning):
    """Partition warning."""


# . DML Exceptions ----------------------------------------------------------------------------------
class DMLError(MysqlEngineError):
    """DML error."""


class DMLClauseError(DMLError, MysqlEngineArgumentError):
    """Invalid DML clause order error."""


class DMLArgumentError(DMLClauseError):
    """Invalid DML argument error."""


class DMLCriticalError(DMLError, MysqlEngineCriticalError):
    """DML critical error."""


class DMLWarning(MysqlEngineWarning, DMLError):
    """DML warning."""


# . Map Exceptions ----------------------------------------------------------------------------------
# Raise errors
_ERROR_MAP: dict[str, dict[str, Exception]] = {
    "DATABASE": {
        "DEFINITION": DatabaseDefinitionError,
        "METADATA": DatabaseMetadataError,
        "ARGUMENT": DatabaseArgumentError,
        "NOT_EXISTS": DatabaseNotExistsError,
        "CRITICAL": DatabaseCriticalError,
        "WARNING": DatabaseWarning,
    },
    "TABLE": {
        "DEFINITION": TableDefinitionError,
        "METADATA": TableMetadataError,
        "ARGUMENT": TableArgumentError,
        "NOT_EXISTS": TableNotExistsError,
        "CRITICAL": TableCriticalError,
        "WARNING": TableWarning,
    },
    "COLUMN": {
        "DEFINITION": ColumnDefinitionError,
        "METADATA": ColumnMetadataError,
        "ARGUMENT": ColumnArgumentError,
        "NOT_EXISTS": ColumnNotExistsError,
        "CRITICAL": ColumnCriticalError,
        "WARNING": ColumnWarning,
    },
    "CONSTRAINT": {
        "DEFINITION": ConstraintDefinitionError,
        "METADATA": ConstraintMetadataError,
        "ARGUMENT": ConstraintArgumentError,
        "NOT_EXISTS": ConstraintNotExistsError,
        "CRITICAL": ConstraintCriticalError,
        "WARNING": ConstraintWarning,
    },
    "INDEX": {
        "DEFINITION": IndexDefinitionError,
        "METADATA": IndexMetadataError,
        "ARGUMENT": IndexArgumentError,
        "NOT_EXISTS": IndexNotExistsError,
        "CRITICAL": IndexCriticalError,
        "WARNING": IndexWarning,
    },
    "PARTITION": {
        "DEFINITION": PartitionDefinitionError,
        "METADATA": PartitionMetadataError,
        "ARGUMENT": PartitionArgumentError,
        "NOT_EXISTS": PartitionNotExistsError,
        "CRITICAL": PartitionCriticalError,
        "WARNING": PartitionWarning,
    },
}


def map_sql_element_exc(
    el_cate: Literal[
        "DATABASE",
        "TABLE",
        "COLUMN",
        "CONSTRAINT",
        "INDEX",
        "PARTITION",
    ],
    error_type: Literal[
        "DEFINITION",
        "METADATA",
        "ARGUMENT",
        "NOT_EXISTS",
        "CRITICAL",
        "WARNING",
    ],
) -> type:
    """Map the element error.

    :param el_cate: The element category.
    :param error_type: The error type.
    """
    return _ERROR_MAP[el_cate][error_type]
