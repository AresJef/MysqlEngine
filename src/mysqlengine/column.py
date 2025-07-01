# cython: language_level=3
# cython: wraparound=False
# cython: boundscheck=False
from __future__ import annotations

# Cython imports
import cython
from cython.cimports.cpython.tuple import PyTuple_Size as tuple_len  # type: ignore
from cython.cimports.cpython.set import PySet_Size as set_len  # type: ignore
from cython.cimports.cpython.set import PySet_Contains as set_contains  # type: ignore
from cython.cimports.cpython.unicode import PyUnicode_Split as str_split  # type: ignore
from cython.cimports.cpython.unicode import PyUnicode_GET_LENGTH as str_len  # type: ignore
from cython.cimports.cpython.unicode import PyUnicode_Substring as str_substr  # type: ignore
from cython.cimports.cpython.unicode import PyUnicode_Contains as str_contains  # type: ignore
from cython.cimports.cpython.unicode import PyUnicode_Tailmatch as str_tailmatch  # type: ignore
from cython.cimports.sqlcycli.charset import Charset  # type: ignore
from cython.cimports.mysqlengine.element import Element, Elements, Logs, Metadata, Query  # type: ignore
from cython.cimports.mysqlengine import utils  # type: ignore

# Python imports
import datetime
from decimal import Decimal
from asyncio import gather as _aio_gather
from cytimes import Pydt
from sqlcycli.aio.pool import Pool
from sqlcycli.charset import Charset
from sqlcycli import errors as sqlerrors, DictCursor
from sqlcycli.aio import DictCursor as AioDictCursor
from mysqlengine.element import Element, Elements, Logs, Metadata, Query
from mysqlengine import utils


__all__ = [
    "Definition",
    "Define",
    "Column",
    "GeneratedColumn",
    "Columns",
    "ColumnMetadata",
]


# Definition -------------------------------------------------------------------------------------------------
"""Incomplete Data Types:

String (Character and Binary) Data Types
Character (Textual) Types:
	•	SET(‘val1’,‘val2’,…): A string object that can have zero or more values from a defined list

Bit Value Type:
	•	BIT[(M)]

JSON Data Type
	•	JSON: Stores JSON documents in a format optimized for quick read access

Spatial (GIS) Data Types
(Requires the MySQL Spatial Extensions. These store geometric data.)
	•	GEOMETRY: A generic type for a geometry value
	•	POINT: A geometry that represents a single point
	•	LINESTRING: A geometry that represents a series of points
	•	POLYGON: A geometry that represents a polygon
	•	MULTIPOINT: A geometry that represents multiple points
	•	MULTILINESTRING: A geometry that represents multiple linestrings
	•	MULTIPOLYGON: A geometry that represents multiple polygons
	•	GEOMETRYCOLLECTION: A geometry that is a collection of multiple geometry types
"""


@cython.cclass
class Definition(Element):
    """The base class for column definitions in a database table.

    ## Notice
    - please do `NOT` instantiate this class directly. Instead,
      use one of the subclasses from the `Define` module to define
      a column's data type.
    """

    # . definition
    _data_type: str
    _python_type: type
    _null: cython.bint
    _default: object
    _primary_key: cython.bint
    _unique_key: cython.bint
    _indexed: cython.bint
    _comment: str
    _visible: cython.bint
    # . integer column
    _unsigned: cython.bint
    _auto_increment: cython.bint
    # . floating/fixed point column
    _default_precision: cython.int
    _precision: cython.int
    _default_scale: cython.int
    _scale: cython.int
    # . temporal column
    _default_fsp: cython.int
    _fsp: cython.int
    _auto_init: cython.bint
    _auto_update: cython.bint
    # . string column
    _default_length: cython.longlong
    _length: cython.longlong
    # . enum column
    _elements: tuple[str]

    def __init__(
        self,
        data_type: str,
        python_type: type,
        null: cython.bint = False,
        default: object | None = None,
        comment: str = None,
        visible: cython.bint = True,
    ):
        """The definition of a column in the table.

        ## Notice
        - please do `NOT` instantiate this class directly. Instead,
          use one of the subclasses from the `Define` module to define
          a column's data type.

        :param data_type `<'str'>`: The string representation of the MySQL data type (e.g., 'TINYINT', 'VARCHAR', 'DATETIME').
        :param python_type `<'type'>`: The corresponding Python type of the column (e.g., `float`, `str`, `datetime`).
        :param null `<'bool'>`: Whether the column can contain NULL values. Defaults to `False`.
        :param default `<'Any/None'>`: The DEFAULT value assigned to the column. Defaults to `None`.
        :param comment `<'str/None'>`: The COMMENT of the column. Defaults to `None`.
        :param visible `<'bool'>`: The visibility of the column. Defaults to `True`.
            An invisible column hidden to queries, but can be accessed if explicitly referenced.
        """
        super().__init__("COLUMN", data_type)
        # . definition
        self._data_type = data_type
        self._python_type = python_type
        self._default = default
        self._primary_key = False
        self._unique_key = False
        self._indexed = False
        self._null = null
        self._comment = self._validate_comment(comment)
        self._visible = visible
        # . integer column
        self._unsigned = False
        self._auto_increment = False
        # . floating/fixed point column
        self._precision = -1
        self._scale = -1
        # . temporal column
        self._default_fsp = -1
        self._fsp = -1
        self._auto_init = False
        self._auto_update = False
        # . string column
        self._default_length = -1
        self._length = -1
        # . enum column
        self._elements = None

    # Property -----------------------------------------------------------------------------
    @property
    def data_type(self) -> str:
        """The MySQL data type (e.g., "TINYINT", "VARCHAR", "DATETIME") `<'str'>`."""
        return self._data_type

    @property
    def python_type(self) -> type:
        """The corresponding Python type of the column (e.g., `float`, `str`, `datetime`) `<'type'>`."""
        return self._python_type

    @property
    def null(self) -> bool:
        """Whether the column can contain NULL values `<'bool'>`."""
        return self._null

    @property
    def default(self) -> object | None:
        """The DEFAULT value assigned to the column `<'Any/None'>`."""
        return self._default

    @property
    def primary_key(self) -> bool:
        """Whether the column is (part of) the PRIMARY KEY `<'bool'>`."""
        return self._primary_key

    @property
    def unique_key(self) -> bool:
        """Whether the column is (part of) a UNIQUE KEY `<'bool'>`."""
        return self._unique_key

    @property
    def indexed(self) -> bool:
        """Whether the column is (part of) an INDEX `<'bool'>`."""
        return self._indexed

    @property
    def comment(self) -> str | None:
        """The COMMENT of the column `<'str/None'>`."""
        return self._comment

    @property
    def visible(self) -> bool:
        """Whether the column is visible to the queries `<'bool'>`."""
        return self._visible

    # . integer
    @property
    def unsigned(self) -> bool:
        """Whether the column is constrainted to UNSIGNED values `<'bool'>`.

        ## Notice
        - Only applicable to integer columns, other data type always return `False`.
        """
        return self._unsigned

    @property
    def auto_increment(self) -> bool:
        """Whether the column does AUTO_INCREMENT `<'bool'>`.

        ## Notice
        - Only applicable to integer columns, other data type always return `False`.
        """
        return self._auto_increment

    # . floating/fixed point
    @property
    def precision(self) -> int | None:
        """The precision of the column `<'int/None'>`.

        ## Notice
        - Only applicable to floating/fixed point columns, other data type always return `None`.
        """
        return None if self._precision == -1 else self._precision

    @property
    def scale(self) -> int | None:
        """The scale of the column `<'int/None'>`.

        ## Notice
        - Only applicable to fixed point columns, other data type always return `None`.
        """
        return None if self._scale == -1 else self._scale

    # . temporal
    @property
    def fsp(self) -> int | None:
        """The fractional seconds precision `<'int/None'>`.

        ## Notice
        - Only applicable to 'DATETIME', 'TIMESTAMP' and 'TIME' columns,
          other data type always return `None`.
        """
        _fsp: cython.int = self._get_fsp()
        return None if _fsp == -1 else _fsp

    @property
    def auto_init(self) -> bool:
        """Whether the column implements DEFAULT CURRENT_TIMESTAMP `<'bool'>`.

        ## Notice
        - Only applicable to 'DATETIME', 'TIMESTAMP' columns,
          other data type always return `False`.
        """
        return self._auto_init

    @property
    def auto_update(self) -> bool:
        """Whether the column implements ON UPDATE CURRENT_TIMESTAMP `<'bool'>`.

        ## Notice
        - Only applicable to 'DATETIME', 'TIMESTAMP' columns,
          other data type always return `False`.
        """
        return self._auto_update

    # . string
    @property
    def length(self) -> int | None:
        """The length of the string column `<'int/None'>`.

        ## Notice
        - Only applicable to string columns (e.g., 'CHAR', 'BINARY'),
          other data type always return `None`.
        """
        _length = self._get_length()
        return None if _length == -1 else _length

    @property
    def elements(self) -> tuple[str] | None:
        """The elements of the column `<'tuple[str]'>`.

        ## Notice
        - Only applicable to 'ENUM' and 'SET' columns,
          other data type always return `None`.
        """
        return self._elements

    # Generate SQL -------------------------------------------------------------------------
    @cython.ccall
    def _gen_definition_sql(self) -> str:
        """(internal) Generate the definition SQL of the column `<'str'>`."""
        self._raise_not_implemented_error("_gen_definition_sql")

    @cython.ccall
    def _gen_data_type_sql(self) -> str:
        """(internal) Generate the DATA TYPE of the column `<'str'>`."""
        self._raise_not_implemented_error("_gen_data_type_sql")

    # Metadata -----------------------------------------------------------------------------
    @cython.ccall
    def _sync_from_metadata(self, meta: ColumnMetadata, logs: Logs = None) -> Logs:
        """(internal) Synchronize local configs with the remote column metadata `<'Logs'>`."""
        if logs is None:
            logs = Logs()

        # Validate
        if self._data_type != meta._data_type:
            logs.log_sync_failed_mismatch(
                self, "column data type", self._data_type, meta._data_type
            )
            return logs._skip()  # exit

        # Base configs
        if self._null != meta._null:
            logs.log_config_bool(self, "null", self._null, meta._null)
            self._null = meta._null
        if self._primary_key != meta._primary_key:
            logs.log_config_bool(
                self, "primary_key", self._primary_key, meta._primary_key
            )
            self._primary_key = meta._primary_key
        if self._unique_key != meta._unique_key:
            logs.log_config_bool(self, "unique", self._unique_key, meta._unique_key)
            self._unique_key = meta._unique_key
        if self._indexed != meta._indexed:
            logs.log_config_bool(self, "indexed", self._indexed, meta._indexed)
            self._indexed = meta._indexed
        if self._comment != meta._comment:
            logs.log_config_obj(self, "comment", self._comment, meta._comment)
            self._comment = meta._comment
        if self._visible != meta._visible:
            logs.log_config_bool(self, "visible", self._visible, meta._visible)
            self._visible = meta._visible
        default = self._validate_default(meta._default)
        if self._default != default:
            logs.log_config_obj(self, "default", self._default, default)
            self._default = default

        # Return logs
        return logs

    @cython.ccall
    @cython.exceptval(-1, check=False)
    def _diff_from_metadata(self, meta: ColumnMetadata) -> cython.int:
        """(internal) Check if the definition configurations are different 
        from the remote server metadata `<'int'>`.

        :returns `<'int'>`:
        - `0`: Definition configurations are identical.
        - `1`: Definition configurations differ in more than visibility.
        - `2`: Only the visibility differs.
        """
        # Different
        if self._data_type != meta._data_type:
            return 1
        if self._null != meta._null:
            return 1
        if self._comment != meta._comment:
            return 1
        if self._default != self._validate_default(meta._default):
            return 1
        if self._visible != meta._visible:
            return 2
        # Same
        return 0

    @cython.ccall
    @cython.exceptval(-2, check=False)
    def _read_metadata_precision(self, value: object) -> cython.longlong:
        """(internal) Read metadata value such as 'precision', 'scale' or 'length' `<'int'>`.

        :returns `<'int'>`:
        - `-1`: if value is None.
        - The precision integer value otherwise.
        """
        if value is None or (isinstance(value, str) and str_len(value) == 0):
            return -1
        if isinstance(value, int):
            return value
        try:
            return int(value)
        except Exception as err:
            self._raise_metadata_error(
                "matadata precision value is invalid: %s %r." % (type(value), value),
                err,
            )

    # Setter -------------------------------------------------------------------------------
    @cython.ccall
    @cython.exceptval(-1, check=False)
    def setup(self, col: Column) -> cython.bint:
        """Setup the definition.

        :param col `<'Column'>`: The column of the definition.
        """
        self._name = col._name
        self._tb_name = col._tb_name
        self._db_name = col._db_name
        self._tb_qualified_name = col._tb_qualified_name
        if self._charset is None and col._charset is not None:
            self._charset = col._charset
        self._pool = col._pool
        return True

    # Assure Ready -------------------------------------------------------------------------
    @cython.ccall
    @cython.exceptval(-1, check=False)
    def _assure_ready(self) -> cython.bint:
        """(internal) Assure the definition is ready."""
        if not self._el_ready:
            self._assure_tb_name_ready()
            Element._assure_ready(self)
        return True

    # Validate -----------------------------------------------------------------------------
    @cython.ccall
    def _validate_default(self, default: object) -> object:
        """(internal) Validate the default value `<'Any/None'>`."""
        if default is None:
            return None
        if isinstance(default, str) and str_len(default) == 0:
            return None
        return default

    # Internal -----------------------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(-2, check=False)
    def _get_fsp(self) -> cython.int:
        """(internal) Get the fractional seconds precision `<'int'>`."""
        return self._fsp if self._fsp != -1 else self._default_fsp

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(-2, check=False)
    def _get_length(self) -> cython.longlong:
        """(internal) Get the maximum length the string `<'int'>`."""
        return self._length if self._length != -1 else self._default_length

    # Copy ---------------------------------------------------------------------------------
    @cython.ccall
    def copy(self) -> Definition:
        """Make a copy of the definition `<'Definition'>`."""
        self._raise_not_implemented_error("copy")

    # Special methods ----------------------------------------------------------------------
    @cython.ccall
    @cython.exceptval(-2, check=False)
    def _sp_equal(self, o: object) -> cython.int:
        """(special) Check if the element equals to the passed-in object `<'int'>.

        :returns `<'int'>`
        - `1` means equal.
        - `0` means not equal.
        - `-1` means NotImplemented.
        """
        if not isinstance(o, Definition):
            return -1
        if not self.__class__ is o.__class__:
            return 0
        _o: Definition = o
        if (
            # . definition
            self._data_type == _o._data_type
            and self._python_type is _o._python_type
            and self._null == _o._null
            and self._default == _o._default
            and self._comment == _o._comment
            and self._visible == _o._visible
            # . integer column
            and self._unsigned == _o._unsigned
            and self._auto_increment == _o._auto_increment
            # . floating/fixed point column
            and self._default_precision == _o._default_precision
            and self._precision == _o._precision
            and self._default_scale == _o._default_scale
            and self._scale == _o._scale
            # . temporal column
            and self._default_fsp == _o._default_fsp
            and self._fsp == _o._fsp
            and self._auto_init == _o._auto_init
            and self._auto_update == _o._auto_update
            # . string column
            and self._default_length == _o._default_length
            and self._length == _o._length
            # . enum column
            and self._elements == _o._elements
        ):
            return 1
        else:
            return 0

    @cython.cfunc
    @cython.inline(True)
    def _gen_repr(self, reprs: list[str]) -> str:
        """(internal) Generate the representation of the definition `<'str'>`."""
        return "<%s (\n\t%s\n)>" % (self._data_type, ",\n\t".join(reprs))

    def __str__(self) -> str:
        return self._data_type


# . numeric
@cython.cclass
class NumericType(Definition):
    """The base class for numeric columns definition in a database table."""

    def __init__(
        self,
        data_type: str,
        python_type: type,
        null: bool = False,
        default: object | None = None,
        comment: str | None = None,
        visible: bool = True,
    ):
        """The base class for numeric columns definition in a database table.

        :param data_type `<'str'>`: The string representation of the MySQL data type.
        :param python_type `<'type'>`: The corresponding Python type of the column.
        :param null `<'bool'>`: Whether the column can contain NULL values. Defaults to `False`.
        :param default `<'Any/None'>`: The DEFAULT value assigned to the column. Defaults to `None`.
        :param comment `<'str/None'>`: The COMMENT of the column. Defaults to `None`.
        :param visible `<'bool'>`: The visibility of the column. Defaults to `True`.
            An invisible column hidden to queries, but can be accessed if explicitly referenced.
        """
        super().__init__(
            data_type,
            python_type,
            null,
            default,
            comment,
            visible,
        )


@cython.cclass
class IntegerType(NumericType):
    """The base class for integer columns definition in a database table."""

    def __init__(
        self,
        data_type: str,
        unsigned: cython.bint = False,
        null: bool = False,
        default: int | None = None,
        auto_increment: cython.bint = False,
        comment: str | None = None,
        visible: bool = True,
    ):
        """The base class for integer columns definition in a database table.

        :param data_type `<'str'>`: The string representation of the MySQL data type.
        :param unsigned `<'bool'>`: Whether the column is constrainted to UNSIGNED values. Defaults to `False`.
        :param null `<'bool'>`: Whether the column can contain NULL values. Defaults to `False`.
        :param default `<'int/None'>`: The DEFAULT value assigned to the column. Defaults to `None`.
        :param auto_increment `<'bool'>`: Whether to enable AUTO_INCREMENT. Defaults to `False`.
        :param comment `<'str/None'>`: The COMMENT of the column. Defaults to `None`.
        :param visible `<'bool'>`: The visibility of the column. Defaults to `True`.
            An invisible column hidden to queries, but can be accessed if explicitly referenced.
        """
        super().__init__(
            data_type,
            int,
            null,
            default,
            comment,
            visible,
        )
        self._auto_increment = auto_increment
        if self._auto_increment:
            self._unsigned = True
            self._default = None
        else:
            self._unsigned = unsigned
            if self._default is not None:
                self._default = self._validate_default(self._default)

    # Property -----------------------------------------------------------------------------
    @property
    def default(self) -> int | None:
        """The DEFAULT value assigned to the column `<'int/None'>`."""
        return self._default

    # Generate SQL -------------------------------------------------------------------------
    @cython.ccall
    def _gen_definition_sql(self) -> str:
        """(internal) Generate the definition SQL of the column `<'str'>`."""
        sql: str = self._gen_data_type_sql()
        if not self._null:
            sql += " NOT NULL"
        if self._auto_increment:
            sql += " AUTO_INCREMENT"
        elif self._default is not None:
            sql += self._format_sql(" DEFAULT %s", self._default)
        if self._comment is not None:
            sql += self._format_sql(" COMMENT %s", self._comment)
        if not self._visible:
            sql += " INVISIBLE"
        return sql

    @cython.ccall
    def _gen_data_type_sql(self) -> str:
        """(internal) Generate the DATA TYPE of the column `<'str'>`."""
        sql: str = self._data_type
        if self._unsigned:
            sql += " UNSIGNED"
        return sql

    # Metadata -----------------------------------------------------------------------------
    @cython.ccall
    def _sync_from_metadata(self, meta: ColumnMetadata, logs: Logs = None) -> Logs:
        """(internal) Synchronize local configs with the remote column metadata `<'Logs'>`."""
        # Base configs
        logs = Definition._sync_from_metadata(self, meta, logs)
        if logs._skip_flag:
            return logs  # exit

        # Integer configs
        if self._unsigned != meta._unsigned:
            logs.log_config_bool(self, "unsigned", self._unsigned, meta._unsigned)
            self._unsigned = meta._unsigned
        if self._auto_increment != meta._auto_increment:
            logs.log_config_bool(
                self, "auto_increment", self._auto_increment, meta._auto_increment
            )
            self._auto_increment = meta._auto_increment

        # Return logs
        return logs

    @cython.ccall
    @cython.exceptval(-1, check=False)
    def _diff_from_metadata(self, meta: ColumnMetadata) -> cython.int:
        """(internal) Check if the definition configurations are different 
        from the remote server metadata `<'int'>`.

        :returns `<'int'>`:
        - `0`: Definition configurations are identical.
        - `1`: Definition configurations differ in more than visibility.
        - `2`: Only the visibility differs.
        """
        # Different
        diff = Definition._diff_from_metadata(self, meta)
        if diff == 1:
            return 1
        if self._unsigned != meta._unsigned:
            return 1
        if self._auto_increment != meta._auto_increment:
            return 1
        # Same or Toggle visibility
        return diff

    # Validate -----------------------------------------------------------------------------
    @cython.ccall
    def _validate_default(self, default: object) -> object:
        """(internal) Validate the default value `<'int/None'>`."""
        if Definition._validate_default(self, default) is None:
            return None
        if isinstance(default, int):
            return default
        try:
            return int(default)
        except Exception as err:
            self._raise_definition_error(
                "DEFINITION 'default' value must be <'int'> type, "
                "instead got %s %r." % (type(default), default),
                err,
            )

    # Copy ---------------------------------------------------------------------------------
    @cython.ccall
    def copy(self) -> Definition:
        """Make a copy of the definition `<'Definition'>`."""
        return self.__class__(
            self._unsigned,
            self._null,
            self._default,
            self._auto_increment,
            self._comment,
            self._visible,
        )

    # Special methods ----------------------------------------------------------------------
    def __repr__(self) -> str:
        return self._gen_repr(
            [
                "unsigned=%s" % self._unsigned,
                "null=%s" % self._null,
                "default=" + repr(self._default),
                "auto_increment=%s" % self._auto_increment,
                "comment=%r" % self._comment,
                "visible=%s" % self._visible,
            ]
        )


@cython.cclass
class FloatingPointType(NumericType):
    """The base class for floating-point columns definition in a database table."""

    def __init__(
        self,
        data_type: str,
        null: bool = False,
        default: float | None = None,
        comment: str | None = None,
        visible: bool = True,
    ):
        """The base class for floating-point columns definition in a database table.

        :param data_type `<'str'>`: The string representation of the MySQL data type.
        :param null `<'bool'>`: Whether the column can contain NULL values. Defaults to `False`.
        :param default `<'float/None'>`: The DEFAULT value assigned to the column. Defaults to `None`.
        :param comment `<'str/None'>`: The COMMENT of the column. Defaults to `None`.
        :param visible `<'bool'>`: The visibility of the column. Defaults to `True`.
            An invisible column hidden to queries, but can be accessed if explicitly referenced.
        """
        super().__init__(
            data_type,
            float,
            null,
            default,
            comment,
            visible,
        )
        if self._default is not None:
            self._default = self._validate_default(self._default)

    # Property -----------------------------------------------------------------------------
    @property
    def default(self) -> float | None:
        """The DEFAULT value assigned to the column `<'float/None'>`."""
        return self._default

    # Generate SQL -------------------------------------------------------------------------
    @cython.ccall
    def _gen_definition_sql(self) -> str:
        """(internal) Generate the definition SQL of the column `<'str'>`."""
        sql: str = self._gen_data_type_sql()
        if not self._null:
            sql += " NOT NULL"
        if self._default is not None:
            sql += self._format_sql(" DEFAULT %s", self._default)
        if self._comment is not None:
            sql += self._format_sql(" COMMENT %s", self._comment)
        if not self._visible:
            sql += " INVISIBLE"
        return sql

    @cython.ccall
    def _gen_data_type_sql(self) -> str:
        """(internal) Generate the DATA TYPE of the column `<'str'>`."""
        return self._data_type

    # Validate -----------------------------------------------------------------------------
    @cython.ccall
    def _validate_default(self, default: object) -> object:
        """(internal) Validate the default value `<'float/None'>`."""
        if Definition._validate_default(self, default) is None:
            return None
        if isinstance(default, float):
            return default
        try:
            return float(default)
        except Exception as err:
            self._raise_definition_error(
                "DEFINITION 'default' value must be <'float'> type, "
                "instead got %s %r." % (type(default), default),
                err,
            )

    # Copy ---------------------------------------------------------------------------------
    @cython.ccall
    def copy(self) -> Definition:
        """Make a copy of the definition `<'Definition'>`."""
        return self.__class__(
            self._null,
            self._default,
            self._comment,
            self._visible,
        )

    # Special methods ----------------------------------------------------------------------
    def __repr__(self) -> str:
        return self._gen_repr(
            [
                "null=%s" % self._null,
                "default=" + repr(self._default),
                "comment=%r" % self._comment,
                "visible=%s" % self._visible,
            ]
        )


@cython.cclass
class FixedPointType(NumericType):
    """The base class for fixed-point columns definition in a database table."""

    def __init__(
        self,
        data_type: str,
        default_precision: cython.int,
        default_scale: cython.int,
        precision: int | None = None,
        scale: int | None = None,
        null: bool = False,
        default: Decimal | None = None,
        comment: str | None = None,
        visible: bool = True,
    ):
        """The base class for fixed-point columns definition in a database table.

        :param data_type `<'str'>`: The string representation of the MySQL data type.
        :param default_precision `<'int'>`: The default fixed-point precision.
        :param default_scale `<'int'>`: The default fixed-point scale.
        :param precision `<'int/None'>`: The fixed-point precision. Defaults to `None`.
        :param scale `<'int/None'>`: The fixed-point scale. Defaults to `None`.
        :param null `<'bool'>`: Whether the column can contain NULL values. Defaults to `False`.
        :param default `<'Decimal/None'>`: The DEFAULT value assigned to the column. Defaults to `None`.
        :param comment `<'str/None'>`: The COMMENT of the column. Defaults to `None`.
        :param visible `<'bool'>`: The visibility of the column. Defaults to `True`.
            An invisible column hidden to queries, but can be accessed if explicitly referenced.
        """
        super().__init__(
            data_type,
            Decimal,
            null,
            default,
            comment,
            visible,
        )
        self._default_precision = min(max(1, default_precision), 65)
        self._precision = self._validate_precision(precision)
        self._default_scale = min(max(0, default_scale), self._default_precision, 30)
        self._scale = self._validate_scale(scale)
        if self._default is not None:
            self._default = self._validate_default(self._default)
            self._default = self._round_to_scale(self._default)

    # Property -----------------------------------------------------------------------------
    @property
    def default(self) -> Decimal | None:
        """The DEFAULT value assigned to the column `<'Decimal/None'>`."""
        return self._default

    # Generate SQL -------------------------------------------------------------------------
    @cython.ccall
    def _gen_definition_sql(self) -> str:
        """(internal) Generate the definition SQL of the column `<'str'>`."""
        sql: str = self._gen_data_type_sql()
        if not self._null:
            sql += " NOT NULL"
        if self._default is not None:
            sql += self._format_sql(" DEFAULT %s", self._default)
        if self._comment is not None:
            sql += self._format_sql(" COMMENT %s", self._comment)
        if not self._visible:
            sql += " INVISIBLE"
        return sql

    @cython.ccall
    def _gen_data_type_sql(self) -> str:
        """(internal) Generate the DATA TYPE of the column `<'str'>`."""
        return "%s(%d,%d)" % (self._data_type, self._precision, self._scale)

    # Metadata -----------------------------------------------------------------------------
    @cython.ccall
    def _sync_from_metadata(self, meta: ColumnMetadata, logs: Logs = None) -> Logs:
        """(internal) Synchronize local configs with the remote column metadata `<'Logs'>`."""
        # Base configs
        logs = Definition._sync_from_metadata(self, meta, logs)
        if logs._skip_flag:
            return logs  # exit

        # Fixed-point configs
        precision = self._read_metadata_precision(meta._numeric_precision)
        if self._precision != precision:
            logs.log_config_int(self, "precision", self._precision, precision)
            self._precision = precision
        scale = self._read_metadata_precision(meta._numeric_scale)
        if self._scale != scale:
            logs.log_config_int(self, "scale", self._scale, scale)
            self._scale = scale

        # Return logs
        return logs

    @cython.ccall
    @cython.exceptval(-1, check=False)
    def _diff_from_metadata(self, meta: ColumnMetadata) -> cython.int:
        """(internal) Check if the definition configurations are different 
        from the remote server metadata `<'int'>`.

        :returns `<'int'>`:
        - `0`: Definition configurations are identical.
        - `1`: Definition configurations differ in more than visibility.
        - `2`: Only the visibility differs.
        """
        # Different
        diff = Definition._diff_from_metadata(self, meta)
        if diff == 1:
            return 1
        if self._precision != self._read_metadata_precision(meta._numeric_precision):
            return 1
        if self._scale != self._read_metadata_precision(meta._numeric_scale):
            return 1
        # Same or Toggle visibility
        return diff

    # Validate -----------------------------------------------------------------------------
    @cython.ccall
    @cython.exceptval(-2, check=False)
    def _validate_precision(self, precision: object) -> cython.int:
        """(internal) Validate fixed-point precision `<'int'>`."""
        if precision is None:
            return self._default_precision
        if not isinstance(precision, int):
            self._raise_definition_error(
                "DEFINITION 'precision' must be <'int'> type, "
                "instead got %s %r." % (type(precision), precision)
            )
        _prec: cython.longlong = precision
        if not 1 <= _prec <= 65:
            self._raise_definition_error(
                "DEFINITION 'precision' must be an integer between 1 and 65, "
                "instead got '%d'." % _prec
            )
        return _prec

    @cython.ccall
    @cython.exceptval(-2, check=False)
    def _validate_scale(self, scale: object) -> cython.int:
        """(internal) Validate fixed-point scale `<'int'>`."""
        if scale is None:
            return self._default_scale
        if not isinstance(scale, int):
            self._raise_definition_error(
                "DEFINITION 'scale' must be <'int'> type, "
                "instead got %s %r." % (type(scale), scale)
            )
        _scale: cython.longlong = scale
        if not 0 <= _scale <= min(self._precision, 30):
            self._raise_definition_error(
                "DEFINITION 'scale' must be an integer between 0 and %d, "
                "instead got '%d'." % (min(self._precision, 30), _scale)
            )
        return _scale

    @cython.ccall
    def _validate_default(self, default: object) -> object:
        """(internal) Validate the default value `<'Decimal/None'>`."""
        if Definition._validate_default(self, default) is None:
            return None
        if not isinstance(default, Decimal):
            try:
                default = Decimal(str(default))
            except Exception as err:
                self._raise_definition_error(
                    "DEFINITION 'default' value must be <'Decimal'> type, "
                    "instead got %s %r." % (type(default), default),
                    err,
                )
        if not default.is_finite():
            self._raise_definition_error(
                "DEFINITION 'default' value must be finite, "
                "instead got %r." % default,
            )
        return default

    # Utils --------------------------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    def _round_to_scale(self, dec: Decimal) -> object:
        """(internal) Round the decimal value to scale `<'Decimal'>`."""
        if dec is None:
            return None
        if not isinstance(dec, Decimal):
            self._raise_definition_error(
                "DEFINITION 'default' value must be <'Decimal'> type, "
                "instead got %s %r." % (type(dec), dec)
            )
        return dec.quantize(Decimal("1e-%d" % self._scale))

    # Copy ---------------------------------------------------------------------------------
    @cython.ccall
    def copy(self) -> Definition:
        """Make a copy of the definition `<'Definition'>`."""
        return self.__class__(
            self._precision,
            self._scale,
            self._null,
            self._default,
            self._comment,
            self._visible,
        )

    # Special methods ----------------------------------------------------------------------
    def __repr__(self) -> str:
        return self._gen_repr(
            [
                "precision=%s" % self._precision,
                "scale=%s" % self._scale,
                "null=%s" % self._null,
                "default=" + repr(self._default),
                "comment=%r" % self._comment,
                "visible=%s" % self._visible,
            ]
        )


# . temporal
@cython.cclass
class TemporalType(Definition):
    """The base class for temporal columns definition in a database table."""

    def __init__(
        self,
        data_type: str,
        python_type: type,
        default_fsp: cython.int,
        fsp: int | None = None,
        null: bool = False,
        default: object | None = None,
        comment: str | None = None,
        visible: bool = True,
    ):
        """The base class for temporal columns definition in a database table.

        :param data_type `<'str'>`: The string representation of the MySQL data type.
        :param python_type `<'type'>`: The corresponding Python type for the column.
        :param default_fsp `<'int'>`: The default fractional seconds precision.
            The column supports fractional precision, only when `default_fsp>=0`.
        :param fsp `<'int/None'>`: The fractional seconds precision (0-6). Defaults to `None`
        :param null `<'bool'>`: Whether the column can contain NULL values. Defaults to `False`.
        :param default `<'Any/None'>`: The DEFAULT value assigned to the column. Defaults to `None`.
        :param comment `<'str/None'>`: The COMMENT of the column. Defaults to `None`.
        :param visible `<'bool'>`: The visibility of the column. Defaults to `True`.
            An invisible column hidden to queries, but can be accessed if explicitly referenced.
        """
        super().__init__(
            data_type,
            python_type,
            null,
            default,
            comment,
            visible,
        )
        self._default_fsp = min(max(default_fsp, -1), 6)
        self._fsp = self._validate_fsp(fsp)

    # Metadata -----------------------------------------------------------------------------
    @cython.ccall
    def _sync_from_metadata(self, meta: ColumnMetadata, logs: Logs = None) -> Logs:
        """(internal) Synchronize local configs with the remote column metadata `<'Logs'>`."""
        # Base configs
        logs = Definition._sync_from_metadata(self, meta, logs)
        if logs._skip_flag:
            return logs  # exit

        # Temporal configs
        if self._default_fsp != -1:
            fsp = self._read_metadata_precision(meta._datetime_precision)
            if self._get_fsp() != fsp:
                logs.log_config_int(self, "fsp", self._get_fsp(), fsp)
                self._fsp = fsp

        # Return Logs
        return logs

    @cython.ccall
    @cython.exceptval(-1, check=False)
    def _diff_from_metadata(self, meta: ColumnMetadata) -> cython.int:
        """(internal) Check if the definition configurations are different 
        from the remote server metadata `<'int'>`.

        :returns `<'int'>`:
        - `0`: Definition configurations are identical.
        - `1`: Definition configurations differ in more than visibility.
        - `2`: Only the visibility differs.
        """
        # Different
        diff = Definition._diff_from_metadata(self, meta)
        if diff == 1:
            return 1
        # fmt: off
        if (
            self._default_fsp != -1
            and self._get_fsp() != self._read_metadata_precision(meta._datetime_precision)
        ):
            return 1
        # fmt: on
        # Same or Toggle visibility
        return diff

    # Validate -----------------------------------------------------------------------------
    @cython.ccall
    @cython.exceptval(-2, check=False)
    def _validate_fsp(self, fsp: object) -> cython.int:
        """(internal) Validate fractional seconds precision `<'int'>`."""
        if fsp is None:
            return -1
        if not isinstance(fsp, int):
            self._raise_definition_error(
                "DEFINITION 'fsp' must be <'int'> type, "
                "instead got %s %r." % (type(fsp), fsp)
            )
        _fsp: cython.longlong = fsp
        if _fsp == -1:
            return -1
        if not 0 <= _fsp <= 6:
            self._raise_definition_error(
                "DEFINITION 'fsp' must be an integer between 0 and 6, "
                "instead got '%d'." % _fsp
            )
        return _fsp


@cython.cclass
class DateAndTimeType(TemporalType):
    """The base class for date & time columns definition in a database table."""

    def __init__(
        self,
        data_type: str,
        python_type: type,
        default_fsp: int,
        fsp: int | None = None,
        null: bool = False,
        auto_init: cython.bint = False,
        auto_update: cython.bint = False,
        default: object | None = None,
        comment: str | None = None,
        visible: bool = True,
    ):
        """The base class for datetime columns definition in a database table.

        :param data_type `<'str'>`: The string representation of the MySQL data type.
        :param python_type `<'type'>`: The corresponding Python type for the column.
        :param default_fsp `<'int'>`: The default fractional seconds precision.
            The column supports fractional precision, only when `default_fsp>=0`.
        :param fsp `<'int/None'>`: The fractional seconds precision (0-6). Defaults to `None`
        :param null `<'bool'>`: Whether the column can contain NULL values. Defaults to `False`.
        :param auto_init `<'bool'>`: Whether the column implements DEFAULT CURRENT_TIMESTAMP. Defaults to `False`.
        :param auto_update `<'bool'>`: Whether the column implements ON UPDATE CURRENT_TIMESTAMP. Defaults to `False`.
        :param default `<'Any/None'>`: The DEFAULT value assigned to the column. Defaults to `None`.
        :param comment `<'str/None'>`: The COMMENT of the column. Defaults to `None`.
        :param visible `<'bool'>`: The visibility of the column. Defaults to `True`.
            An invisible column hidden to queries, but can be accessed if explicitly referenced.
        """
        super().__init__(
            data_type,
            python_type,
            default_fsp,
            fsp,
            null,
            default,
            comment,
            visible,
        )
        self._auto_init = auto_init
        self._auto_update = auto_update
        if self._auto_init:
            self._default = None
        elif self._default is not None:
            self._default = self._validate_default(default)

    # Generate SQL -------------------------------------------------------------------------
    @cython.ccall
    def _gen_definition_sql(self) -> str:
        """(internal) Generate the definition SQL of the column `<'str'>`."""
        sql: str = self._gen_data_type_sql()
        if not self._null:
            sql += " NOT NULL"
        if self._auto_init:
            if self._fsp == -1:
                sql += " DEFAULT CURRENT_TIMESTAMP"
            else:
                sql += " DEFAULT CURRENT_TIMESTAMP(%d)" % self._fsp
        elif self._default is not None:
            sql += self._format_sql(" DEFAULT %s", self._default)
        if self._auto_update:
            if self._fsp == -1:
                sql += " ON UPDATE CURRENT_TIMESTAMP"
            else:
                sql += " ON UPDATE CURRENT_TIMESTAMP(%d)" % self._fsp
        if self._comment is not None:
            sql += self._format_sql(" COMMENT %s", self._comment)
        if not self._visible:
            sql += " INVISIBLE"
        return sql

    @cython.ccall
    def _gen_data_type_sql(self) -> str:
        """(internal) Generate the DATA TYPE of the column `<'str'>`."""
        if self._fsp == -1:
            return self._data_type
        return "%s(%d)" % (self._data_type, self._fsp)

    # Metadata -----------------------------------------------------------------------------
    @cython.ccall
    def _sync_from_metadata(self, meta: ColumnMetadata, logs: Logs = None) -> Logs:
        """(internal) Synchronize local configs with the remote column metadata `<'Logs'>`."""
        # Base configs
        logs = TemporalType._sync_from_metadata(self, meta, logs)
        if logs._skip_flag:
            return logs  # exit

        # DateTime configs
        if self._auto_init != meta._auto_init:
            logs.log_config_bool(self, "auto_init", self._auto_init, meta._auto_init)
            self._auto_init = meta._auto_init
        if self._auto_update != meta._auto_update:
            logs.log_config_bool(
                self, "auto_update", self._auto_update, meta._auto_update
            )
            self._auto_update = meta._auto_update

        # Return logs
        return logs

    @cython.ccall
    @cython.exceptval(-1, check=False)
    def _diff_from_metadata(self, meta: ColumnMetadata) -> cython.int:
        """(internal) Check if the definition configurations are different 
        from the remote server metadata `<'int'>`.

        :returns `<'int'>`:
        - `0`: Definition configurations are identical.
        - `1`: Definition configurations differ in more than visibility.
        - `2`: Only the visibility differs.
        """
        # Different
        diff = TemporalType._diff_from_metadata(self, meta)
        if diff == 1:
            return 1
        if self._auto_init != meta._auto_init:
            return 1
        if self._auto_update != meta._auto_update:
            return 1
        # Same or Toggle visibility
        return diff

    # Validate -----------------------------------------------------------------------------
    @cython.ccall
    def _validate_default(self, default: object) -> object:
        """(internal) Validate the default value `<'datetime.datetime/None'>`."""
        if Definition._validate_default(self, default) is None:
            return None
        if self._auto_init and default == "CURRENT_TIMESTAMP":
            return None
        try:
            return Pydt.parse(default)
        except Exception as err:
            self._raise_definition_error(
                "DEFINITION 'default' value must be <'date/datetime'> type, "
                "instead got %s %r." % (type(default), default),
                err,
            )


@cython.cclass
class DateType(DateAndTimeType):
    """The base class for date columns definition in a database table."""

    def __init__(
        self,
        data_type: str,
        null: bool = False,
        default: datetime.date | None = None,
        comment: str | None = None,
        visible: bool = True,
    ):
        """The base class for date columns definition in a database table.

        :param data_type `<'str'>`: The string representation of the MySQL data type.
        :param null `<'bool'>`: Whether the column can contain NULL values. Defaults to `False`.
        :param default `<'datetime.date/None'>`: The DEFAULT value assigned to the column. Defaults to `None`.
        :param comment `<'str/None'>`: The COMMENT of the column. Defaults to `None`.
        :param visible `<'bool'>`: The visibility of the column. Defaults to `True`.
            An invisible column hidden to queries, but can be accessed if explicitly referenced.
        """
        super().__init__(
            data_type,
            datetime.date,
            -1,  # Does not support fractional seconds
            None,
            null,
            False,
            False,
            default,
            comment,
            visible,
        )

    # Property -----------------------------------------------------------------------------
    @property
    def default(self) -> datetime.date | None:
        """The DEFAULT value assigned to the column `<'date/None'>`."""
        return self._default

    # Validate -----------------------------------------------------------------------------
    @cython.ccall
    def _validate_default(self, default: object) -> object:
        """(internal) Validate the default value `<'datetime.date/None'>`."""
        default: Pydt = DateAndTimeType._validate_default(self, default)
        return None if default is None else default.date()

    # Copy ---------------------------------------------------------------------------------
    @cython.ccall
    def copy(self) -> Definition:
        """Make a copy of the definition `<'Definition'>`."""
        return self.__class__(
            self._null,
            self._default,
            self._comment,
            self._visible,
        )

    # Special methods ----------------------------------------------------------------------
    def __repr__(self) -> str:
        return self._gen_repr(
            [
                "null=%s" % self._null,
                "default=" + repr(self._default),
                "comment=%r" % self._comment,
                "visible=%s" % self._visible,
            ]
        )


@cython.cclass
class DateTimeType(DateAndTimeType):
    """The base class for datetime columns definition in a database table."""

    def __init__(
        self,
        data_type: str,
        fsp: int | None = None,
        null: bool = False,
        auto_init: bool = False,
        auto_update: bool = False,
        default: datetime.datetime | None = None,
        comment: str | None = None,
        visible: bool = True,
    ):
        """The base class for datetime columns definition in a database table.

        :param data_type `<'str'>`: The string representation of the MySQL data type.
        :param fsp `<'int/None'>`: The fractional seconds precision (0-6). Defaults to `None`.
        :param null `<'bool'>`: Whether the column can contain NULL values. Defaults to `False`.
        :param auto_init `<'bool'>`: Whether the column implements DEFAULT CURRENT_TIMESTAMP. Defaults to `False`.
        :param auto_update `<'bool'>`: Whether the column implements ON UPDATE CURRENT_TIMESTAMP. Defaults to `False`.
        :param default `<'datetime.datetime/None'>`: The DEFAULT value assigned to the column. Defaults to `None`.
        :param comment `<'str/None'>`: The COMMENT of the column. Defaults to `None`.
        :param visible `<'bool'>`: The visibility of the column. Defaults to `True`.
            An invisible column hidden to queries, but can be accessed if explicitly referenced.
        """
        super().__init__(
            data_type,
            datetime.datetime,
            0,  # Default fsp is 0
            fsp,
            null,
            auto_init,
            auto_update,
            default,
            comment,
            visible,
        )
        if self._default is not None:
            self._default = self._adjust_to_fsp(self._default)

    # Property -----------------------------------------------------------------------------
    @property
    def default(self) -> datetime.datetime | None:
        """The DEFAULT value assigned to the column `<'datetime/None'>`."""
        return self._default

    # Utils --------------------------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    def _adjust_to_fsp(self, dt: Pydt) -> object:
        """(internal) Adjust the datetime to defined fsp `<'Pydt'>`."""
        if dt is None:
            return None
        if not isinstance(dt, Pydt):
            self._raise_definition_error(
                "DEFINITION 'default' value must be <'Pydt'> type, "
                "instead got %s %r." % (type(dt), dt)
            )
        return dt.fsp(0) if self._fsp <= 0 else dt.fsp(self._fsp)

    # Copy ---------------------------------------------------------------------------------
    @cython.ccall
    def copy(self) -> Definition:
        """Make a copy of the definition `<'Definition'>`."""
        return self.__class__(
            self._fsp,
            self._null,
            self._auto_init,
            self._auto_update,
            self._default,
            self._comment,
            self._visible,
        )

    # Special methods ----------------------------------------------------------------------
    def __repr__(self) -> str:
        return self._gen_repr(
            [
                "fsp=%s" % self._get_fsp(),
                "null=%s" % self._null,
                "auto_init=%s" % self._auto_init,
                "auto_update=%s" % self._auto_update,
                "default=" + repr(self._default),
                "comment=%r" % self._comment,
                "visible=%s" % self._visible,
            ]
        )


@cython.cclass
class TimeType(TemporalType):
    """The base class for time columns definition in a database table."""

    def __init__(
        self,
        data_type: str,
        fsp: int | None = None,
        null: bool = False,
        default: datetime.time | None = None,
        comment: str | None = None,
        visible: bool = True,
    ):
        """The base class for time columns definition in a database table.

        :param data_type `<'str'>`: The string representation of the MySQL data type.
        :param fsp `<'int/None'>`: The fractional seconds precision (0-6). Defaults to `None`
        :param null `<'bool'>`: Whether the column can contain NULL values. Defaults to `False`.
        :param default `<'datetime.time/None'>`: The DEFAULT value assigned to the column. Defaults to `None`.
        :param comment `<'str/None'>`: The COMMENT of the column. Defaults to `None`.
        :param visible `<'bool'>`: The visibility of the column. Defaults to `True`.
            An invisible column hidden to queries, but can be accessed if explicitly referenced.
        """
        super().__init__(
            data_type,
            datetime.time,
            0,  # default fsp is 0
            fsp,
            null,
            default,
            comment,
            visible,
        )
        if self._default is not None:
            self._default = self._validate_default(self._default)
            self._default = self._adjust_to_fsp(self._default)

    # Property -----------------------------------------------------------------------------
    @property
    def default(self) -> datetime.time | None:
        """The DEFAULT value assigned to the column `<'time/None'>`."""
        return self._default

    # Generate SQL -------------------------------------------------------------------------
    @cython.ccall
    def _gen_definition_sql(self) -> str:
        """(internal) Generate the definition SQL of the column `<'str'>`."""
        sql: str = self._gen_data_type_sql()
        if not self._null:
            sql += " NOT NULL"
        if self._default is not None:
            sql += self._format_sql(" DEFAULT %s", self._default)
        if self._comment is not None:
            sql += self._format_sql(" COMMENT %s", self._comment)
        if not self._visible:
            sql += " INVISIBLE"
        return sql

    @cython.ccall
    def _gen_data_type_sql(self) -> str:
        """(internal) Generate the DATA TYPE of the column `<'str'>`."""
        if self._fsp == -1:
            return self._data_type
        return "%s(%d)" % (self._data_type, self._fsp)

    # Validate -----------------------------------------------------------------------------
    @cython.ccall
    def _validate_default(self, default: object) -> object:
        """(internal) Validate time default value `<'datetime.time/None'>`."""
        if Definition._validate_default(self, default) is None:
            return None
        if isinstance(default, datetime.time):
            return default
        try:
            return Pydt.parse(default, utils.BASE_DATE).time()
        except Exception as err:
            self._raise_definition_error(
                "DEFINITION 'default' value must be <'time'> type, "
                "instead got %s %r." % (type(default), default),
                err,
            )

    # Utils --------------------------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    def _adjust_to_fsp(self, time: datetime.time) -> object:
        """(internal) Adjust the time to defined fsp `<'datetime.time'>`."""
        if time is None:
            return None
        if not isinstance(time, datetime.time):
            self._raise_definition_error(
                "DEFINITION 'default' value must be <'datetime.time'> type, "
                "instead got %s %r." % (type(time), time)
            )
        dt = Pydt.combine(utils.BASE_DATE, time)
        return dt.fsp(0).time() if self._fsp <= 0 else dt.fsp(self._fsp).time()

    # Copy ---------------------------------------------------------------------------------
    @cython.ccall
    def copy(self) -> Definition:
        """Make a copy of the definition `<'Definition'>`."""
        # TIME
        return self.__class__(
            self._fsp,
            self._null,
            self._default,
            self._comment,
            self._visible,
        )

    # Special methods ----------------------------------------------------------------------
    def __repr__(self) -> str:
        return self._gen_repr(
            [
                "fsp=%s" % self._get_fsp(),
                "null=%s" % self._null,
                "default=" + repr(self._default),
                "comment=%r" % self._comment,
                "visible=%s" % self._visible,
            ]
        )


@cython.cclass
class YearType(TemporalType):
    """The base class for year columns definition in a database table."""

    def __init__(
        self,
        data_type: str,
        null: bool = False,
        default: int | datetime.date | datetime.datetime | None = None,
        comment: str | None = None,
        visible: bool = True,
    ):
        """The base class for year columns definition in a database table.

        :param data_type `<'str'>`: The string representation of the MySQL data type.
        :param null `<'bool'>`: Whether the column can contain NULL values. Defaults to `False`.
        :param default `<'int/datetime.date/datetime.datetime/None'>`: The DEFAULT value assigned to the column. Defaults to `None`.
        :param comment `<'str/None'>`: The COMMENT of the column. Defaults to `None`.
        :param visible `<'bool'>`: The visibility of the column. Defaults to `True`.
            An invisible column hidden to queries, but can be accessed if explicitly referenced.
        """
        super().__init__(
            data_type,
            int,
            -1,  # Does not support fractional seconds
            None,
            null,
            default,
            comment,
            visible,
        )
        if self._default is not None:
            self._default = self._validate_default(self._default)

    # Property -----------------------------------------------------------------------------
    @property
    def default(self) -> int | None:
        """The DEFAULT value assigned to the column `<'int'>`."""
        return self._default

    # Generate SQL -------------------------------------------------------------------------
    @cython.ccall
    def _gen_definition_sql(self) -> str:
        """(internal) Generate the definition SQL of the column `<'str'>`."""
        sql: str = self._gen_data_type_sql()
        if not self._null:
            sql += " NOT NULL"
        if self._default is not None:
            sql += self._format_sql(" DEFAULT %s", self._default)
        if self._comment is not None:
            sql += self._format_sql(" COMMENT %s", self._comment)
        if not self._visible:
            sql += " INVISIBLE"
        return sql

    @cython.ccall
    def _gen_data_type_sql(self) -> str:
        """(internal) Generate the DATA TYPE of the column `<'str'>`."""
        return self._data_type

    # Validate -----------------------------------------------------------------------------
    @cython.ccall
    def _validate_default(self, default: object) -> object:
        """(internal) Validate the default value `<'int/None'>`."""
        if Definition._validate_default(self, default) is None:
            return None
        if isinstance(default, int):
            pass
        elif isinstance(default, datetime.date):
            default = default.year
        elif isinstance(default, datetime.datetime):
            default = default.year
        else:
            try:
                default = int(default)
            except Exception as err:
                self._raise_definition_error(
                    "DEFINITION 'default' value must be <'int'> type, "
                    "instead got %s %r." % (type(default), default),
                    err,
                )
        _year: cython.longlong = default
        if _year < 0:
            self._raise_definition_error(
                "DEFINITION 'default' value must be must be a positive integer, "
                "instead got '%d'." % _year,
            )
        elif _year <= 69:
            _year += 2000
        elif _year <= 99:
            _year += 1900
        elif not 1901 <= _year <= 2155:
            self._raise_definition_error(
                "DEFINITION 'default' value '%d' is not valid, "
                "must be between 0-99 or 1901-2155." % _year,
            )
        return _year

    # Copy ---------------------------------------------------------------------------------
    @cython.ccall
    def copy(self) -> Definition:
        """Make a copy of the definition `<'Definition'>`."""
        # YEAR
        return self.__class__(
            self._null,
            self._default,
            self._comment,
            self._visible,
        )

    # Special methods ----------------------------------------------------------------------
    def __repr__(self) -> str:
        return self._gen_repr(
            [
                "null=%s" % self._null,
                "default=" + repr(self._default),
                "comment=%r" % self._comment,
                "visible=%s" % self._visible,
            ]
        )


# . string
@cython.cclass
class StringType(Definition):
    """The base class for string columns definition in a database table."""

    def __init__(
        self,
        data_type: str,
        python_type: type,
        default_length: cython.longlong,
        length: int | None = None,
        null: bool = False,
        default: object | None = None,
        comment: str | None = None,
        visible: bool = True,
    ):
        """The base class for string columns definition in a database table.

        :param data_type `<'str'>`: The string representation of the MySQL data type.
        :param python_type `<'type'>`: The corresponding Python type for the column.
        :param default_length `<'int'>`: The default length of the string column.
            The column supports variable length, only when `default_length>=0`.
            Otherwise, the column only supports fixed length.
        :param length `<'int/None'>`: The maximum length of the string column. Defaults to `None`.
        :param null `<'bool'>`: Whether the column can contain NULL values. Defaults to `False`.
        :param default `<'Any/None'>`: The DEFAULT value assigned to the column. Defaults to `None`.
        :param comment `<'str/None'>`: The COMMENT of the column. Defaults to `None`.
        :param visible `<'bool'>`: The visibility of the column. Defaults to `True`.
            An invisible column hidden to queries, but can be accessed if explicitly referenced.
        """
        super().__init__(
            data_type,
            python_type,
            null,
            default,
            comment,
            visible,
        )
        self._default_length = max(default_length, -1)
        self._length = self._validate_length(length)

    # Metadata -----------------------------------------------------------------------------
    @cython.ccall
    def _sync_from_metadata(self, meta: ColumnMetadata, logs: Logs = None) -> Logs:
        """(internal) Synchronize local configs with the remote column metadata `<'Logs'>`."""
        # Base configs
        logs = Definition._sync_from_metadata(self, meta, logs)
        if logs._skip_flag:
            return logs  # exit

        # String configs
        if self._default_length != -1:
            length = self._read_metadata_precision(meta._character_maximum_length)
            if self._get_length() != length:
                logs.log_config_int(self, "length", self._length, length)
                self._length = length

        # Return logs
        return logs

    @cython.ccall
    @cython.exceptval(-1, check=False)
    def _diff_from_metadata(self, meta: ColumnMetadata) -> cython.int:
        """(internal) Check if the definition configurations are different 
        from the remote server metadata `<'int'>`.

        :returns `<'int'>`:
        - `0`: Definition configurations are identical.
        - `1`: Definition configurations differ in more than visibility.
        - `2`: Only the visibility differs.
        """
        # Different
        diff = Definition._diff_from_metadata(self, meta)
        if diff == 1:
            return 1
        # fmt: off
        if (
            self._default_length != -1 
            and self._get_length() != self._read_metadata_precision(meta._character_maximum_length)
        ):
            return 1
        # fmt: on
        # Same or Toggle visibility
        return diff

    # Validate -----------------------------------------------------------------------------
    @cython.ccall
    @cython.exceptval(-2, check=False)
    def _validate_length(self, length: object) -> cython.longlong:
        """(internal) Validate string max length `<'int'>`."""
        if length is None:
            return -1
        if not isinstance(length, int):
            try:
                length = int(length)
            except Exception as err:
                self._raise_definition_error(
                    "DEFINITION 'length' must be <'int'> type, "
                    "instead got %s %r." % (type(length), length),
                    err,
                )
        _length: cython.longlong = length
        if _length == -1:
            return -1
        if not _length >= 0:
            self._raise_definition_error(
                "DEFINITION 'length' must be a positive integer, "
                "instead got '%d'." % _length,
            )
        return _length


@cython.cclass
class ChStringType(StringType):
    """The base class for character string columns definition in a database table."""

    def __init__(
        self,
        data_type: str,
        default_length: int,
        length: int | None = None,
        null: bool = False,
        default: str | None = None,
        charset: object | None = None,
        collate: object | None = None,
        comment: str | None = None,
        visible: bool = True,
    ):
        """The base class for character string columns definition in a database table.

        :param data_type `<'str'>`: The string representation of the MySQL data type.
        :param default_length `<'int'>`: The default length of the string column.
            The column supports variable length, only when `default_length>=0`.
            Otherwise, the column only supports fixed length.
        :param length `<'int/None'>`: The maximum length of the string column. Defaults to `None`.
        :param null `<'bool'>`: Whether the column can contain NULL values. Defaults to `False`.
        :param default `<'str/None'>`: The DEFAULT value assigned to the column. Defaults to `None`.
        :param charset `<'str/Charset/None'>`: The CHARACTER SET of the column. Defaults to `None`.
        :param collate `<'str/None'>`: The COLLATION of the column. Defaults to `None`.
        :param comment `<'str/None'>`: The COMMENT of the column. Defaults to `None`.
        :param visible `<'bool'>`: The visibility of the column. Defaults to `True`.
            An invisible column hidden to queries, but can be accessed if explicitly referenced.
        """
        super().__init__(
            data_type,
            str,
            default_length,
            length,
            null,
            default,
            comment,
            visible,
        )
        self._charset = self._validate_charset(charset, collate)
        if self._default is not None:
            self._default = self._validate_default(self._default)

    # Property -----------------------------------------------------------------------------
    @property
    def default(self) -> str | None:
        """The DEFAULT value assigned to the column `<'str/None'>`."""
        return self._default

    # Generate SQL -------------------------------------------------------------------------
    @cython.ccall
    def _gen_definition_sql(self) -> str:
        """(internal) Generate the definition SQL of the column `<'str'>`."""
        sql: str = self._gen_data_type_sql()
        if not self._null:
            sql += " NOT NULL"
        if self._default is not None:
            sql += self._format_sql(" DEFAULT %s", self._default)
        if self._charset is not None:
            sql += " COLLATE %s" % self._charset._collation
        if self._comment is not None:
            sql += self._format_sql(" COMMENT %s", self._comment)
        if not self._visible:
            sql += " INVISIBLE"
        return sql

    @cython.ccall
    def _gen_data_type_sql(self) -> str:
        """(internal) Generate the DATA TYPE of the column `<'str'>`."""
        if self._length == -1:
            return self._data_type
        return "%s(%d)" % (self._data_type, self._length)

    # Metadata -----------------------------------------------------------------------------
    @cython.ccall
    def _sync_from_metadata(self, meta: ColumnMetadata, logs: Logs = None) -> Logs:
        """(internal) Synchronize local configs with the remote column metadata `<'Logs'>`."""
        # Base configs
        logs = StringType._sync_from_metadata(self, meta, logs)
        if logs._skip_flag:
            return logs  # exit

        # Character string configs
        if self._charset is not meta._charset:
            logs.log_charset(self, self._charset, meta._charset)
            self._charset = meta._charset

        # Return logs
        return logs

    @cython.ccall
    @cython.exceptval(-1, check=False)
    def _diff_from_metadata(self, meta: ColumnMetadata) -> cython.int:
        """(internal) Check if the definition configurations are different 
        from the remote server metadata `<'int'>`.

        :returns `<'int'>`:
        - `0`: Definition configurations are identical.
        - `1`: Definition configurations differ in more than visibility.
        - `2`: Only the visibility differs.
        """
        # Different
        diff = StringType._diff_from_metadata(self, meta)
        if diff == 1:
            return 1
        if self._charset is not meta._charset:
            return 1
        # Same or Toggle visibility
        return diff

    # Validate -----------------------------------------------------------------------------
    @cython.ccall
    def _validate_default(self, default: object) -> object:
        """(internal) Validate the default value `<'str/None'>`."""
        if default is None:
            return None
        if isinstance(default, str):
            return default
        try:
            return str(default)
        except Exception as err:
            self._raise_definition_error(
                "DEFINITION 'default' value must be <'str'> type, "
                "instead got %s %r." % (type(default), default),
                err,
            )


@cython.cclass
class CharType(ChStringType):
    """The base class for char & varchar columns definition in a database table."""

    # Copy ---------------------------------------------------------------------------------
    @cython.ccall
    def copy(self) -> Definition:
        """Make a copy of the definition `<'Definition'>`."""
        return self.__class__(
            self._length,
            self._null,
            self._default,
            self._charset,
            None,
            self._comment,
            self._visible,
        )

    # Special methods ----------------------------------------------------------------------
    def __repr__(self) -> str:
        if self._charset is None:
            reprs = [
                "length=%s" % self._get_length(),
                "null=%s" % self._null,
                "default=" + repr(self._default),
                "charset=None",
                "collate=None",
                "comment=%r" % self._comment,
                "visible=%s" % self._visible,
            ]
        else:
            reprs = [
                "length=%s" % self._get_length(),
                "null=%s" % self._null,
                "default=" + repr(self._default),
                "charset='%s'" % self._charset._name,
                "collate='%s'" % self._charset._collation,
                "comment=%r" % self._comment,
                "visible=%s" % self._visible,
            ]
        return self._gen_repr(reprs)


@cython.cclass
class TextType(ChStringType):
    """The base class for text columns definition in a database table."""

    def __init__(
        self,
        data_type: str,
        null: bool = False,
        charset: object | None = None,
        collate: object | None = None,
        comment: str | None = None,
        visible: bool = True,
    ):
        """The base class for text columns definition in a database table.

        :param data_type `<'str'>`: The string representation of the MySQL data type.
        :param null `<'bool'>`: Whether the column can contain NULL values. Defaults to `False`.
        :param charset `<'str/Charset/None'>`: The CHARACTER SET of the column. Defaults to `None`.
        :param collate `<'str/None'>`: The COLLATION of the column. Defaults to `None`.
        :param comment `<'str/None'>`: The COMMENT of the column. Defaults to `None`.
        :param visible `<'bool'>`: The visibility of the column. Defaults to `True`.
            An invisible column hidden to queries, but can be accessed if explicitly referenced.
        """
        super().__init__(
            data_type,
            -1,  # -1 means fixed string length
            None,
            null,
            None,
            charset,
            collate,
            comment,
            visible,
        )

    # Validate -----------------------------------------------------------------------------
    @cython.ccall
    def _validate_default(self, default: object) -> object:
        """(internal) Validate the default value `<'str/None'>`."""
        if default is None:
            return None
        self._raise_operational_error(
            1101,
            "cannot have a default value, instead got %s %r."
            % (type(default), default),
        )

    # Copy ---------------------------------------------------------------------------------
    @cython.ccall
    def copy(self) -> Definition:
        """Make a copy of the definition `<'Definition'>`."""
        return self.__class__(
            self._null,
            self._charset,
            None,
            self._comment,
            self._visible,
        )

    # Special methods ----------------------------------------------------------------------
    def __repr__(self) -> str:
        if self._charset is None:
            reprs = [
                "null=%s" % self._null,
                "charset=None",
                "collate=None",
                "comment=%r" % self._comment,
                "visible=%s" % self._visible,
            ]
        else:
            reprs = [
                "null=%s" % self._null,
                "charset='%s'" % self._charset._name,
                "collate='%s'" % self._charset._collation,
                "comment=%r" % self._comment,
                "visible=%s" % self._visible,
            ]
        return self._gen_repr(reprs)


@cython.cclass
class EnumType(ChStringType):
    """The base class for enumeration columns definition in a database table."""

    def __init__(
        self,
        data_type: str,
        elements: tuple[str],
        null: bool = False,
        default: str | None = None,
        charset: object | None = None,
        collate: object | None = None,
        comment: str | None = None,
        visible: bool = True,
    ):
        """The base class for enumeration columns definition in a database table.

        :param data_type `<'str'>`: The string representation of the MySQL data type.
        :param elements `<'tuple[str]'>`: The enumeration elements of the column.
        :param null `<'bool'>`: Whether the column can contain NULL values. Defaults to `False`.
        :param default `<'str/None'>`: The DEFAULT value assigned to the column. Defaults to `None`.
        :param charset `<'str/Charset/None'>`: The CHARACTER SET of the column. Defaults to `None`.
        :param collate `<'str/None'>`: The COLLATION of the column. Defaults to `None`.
        :param comment `<'str/None'>`: The COMMENT of the column. Defaults to `None`.
        :param visible `<'bool'>`: The visibility of the column. Defaults to `True`.
            An invisible column hidden to queries, but can be accessed if explicitly referenced.
        """
        self._elements = None
        super().__init__(
            data_type,
            -1,  # -1 fixed string length
            None,  # No length for ENUM
            null,
            default,
            charset,
            collate,
            comment,
            visible,
        )
        self._elements = self._validate_elements(elements)
        if self._default is not None:
            self._default = self._validate_default(self._default)

    # Generate SQL -------------------------------------------------------------------------
    @cython.ccall
    def _gen_definition_sql(self) -> str:
        """(internal) Generate the definition SQL of the column `<'str'>`."""
        sql: str = self._gen_data_type_sql()
        if not self._null:
            sql += " NOT NULL"
        if self._default is not None:
            sql += self._format_sql(" DEFAULT %s", self._default)
        if self._charset is not None:
            sql += " COLLATE %s" % self._charset._collation
        if self._comment is not None:
            sql += self._format_sql(" COMMENT %s", self._comment)
        if not self._visible:
            sql += " INVISIBLE"
        return sql

    @cython.ccall
    def _gen_data_type_sql(self) -> str:
        """(internal) Generate the DATA TYPE of the column `<'str'>`."""
        return self._data_type + self._escape_args(self._elements, False)

    # Metadata -----------------------------------------------------------------------------
    @cython.ccall
    def _sync_from_metadata(self, meta: ColumnMetadata, logs: Logs = None) -> Logs:
        """(internal) Synchronize local configs with the remote column metadata `<'Logs'>`."""
        # Base configs
        logs = ChStringType._sync_from_metadata(self, meta, logs)
        if logs._skip_flag:
            return logs  # exit

        # Enum configs
        elements = self._read_metadata_enum_elements(meta._column_type)
        if self._elements != elements:
            logs.log_config_obj(self, "elements", self._elements, elements)
            self._elements = elements

        # Return logs
        return logs

    @cython.ccall
    @cython.exceptval(-1, check=False)
    def _diff_from_metadata(self, meta: ColumnMetadata) -> cython.int:
        """(internal) Check if the definition configurations are different 
        from the remote server metadata `<'int'>`.

        :returns `<'int'>`:
        - `0`: Definition configurations are identical.
        - `1`: Definition configurations differ in more than visibility.
        - `2`: Only the visibility differs.
        """
        # Different
        diff = ChStringType._diff_from_metadata(self, meta)
        if diff == 1:
            return 1
        if self._elements != self._read_metadata_enum_elements(meta._column_type):
            return 1
        # Same or Toggle visibility
        return diff

    @cython.ccall
    def _read_metadata_enum_elements(self, value: object) -> tuple:
        """(internal) Read metadata ENUM elements from 'COLUMN_TYPE' `<'str'>`."""
        if not isinstance(value, str):
            self._raise_metadata_error(
                "metadata 'COLUMN_TYPE' must <'str'> type, "
                "instead got %s %r." % (type(value), value)
            )
        col_type: str = value
        size: cython.Py_ssize_t = str_len(col_type)
        if size < 8:
            self._raise_metadata_error(
                "metadata 'COLUMN_TYPE' is invalid '%s'." % col_type
            )
        if not str_tailmatch(col_type, "ENUM('", 0, size, -1):
            self._raise_metadata_error(
                "metadata 'COLUMN_TYPE' must startwith 'ENUM(', "
                "instead got '%s'." % col_type
            )
        col_type = str_substr(col_type, 6, size - 2)
        return tuple(str_split(col_type, "','", -1))

    # Validate -----------------------------------------------------------------------------
    @cython.ccall
    def _validate_elements(self, elements: tuple) -> tuple[str]:
        """(internal) Validate enumeration elements `<'tuple[str]'>`."""
        res: list = []
        seen = set()
        count: cython.uint = 0
        for el in elements:
            if isinstance(el, str):
                if str_len(el) == 0:
                    self._raise_definition_error(
                        "DEFINITION element cannot be an empty string."
                    )
                if set_contains(seen, el):
                    self._raise_definition_error(
                        "DEFINITION elements must be unique, "
                        "instead got duplicate '%s'." % el
                    )
                seen.add(el)
                res.append(el)
                count += 1

            elif isinstance(el, (tuple, list)):
                for e in el:
                    if not isinstance(e, str):
                        self._raise_definition_error(
                            "DEFINITION element must be <'str'> type, "
                            "instead got %s %r." % (type(e), e)
                        )
                    if str_len(e) == 0:
                        self._raise_definition_error(
                            "DEFINITION element cannot be an empty string."
                        )
                    if set_contains(seen, e):
                        self._raise_definition_error(
                            "DEFINITION elements must be unique, "
                            "instead got duplicate '%s'." % e
                        )
                    seen.add(e)
                    res.append(e)
                    count += 1

            else:
                self._raise_definition_error(
                    "DEFINITION element must be <'str'> type, "
                    "instead got %s %r." % (type(el), el)
                )

        # Return
        if count == 0:
            self._raise_definition_error("DEFINITION must have at least one element.")
        return tuple(res)

    @cython.ccall
    def _validate_default(self, default: object) -> object:
        """(internal) Validate the default value `<'str/None'>`."""
        if self._elements is None or default is None:
            return default
        if default not in self._elements:
            self._raise_definition_error(
                "DEFINITION 'default' value must be one of the elements %s, "
                "instead got %s %r." % (self._elements, type(default), default),
            )
        return default

    # Copy ---------------------------------------------------------------------------------
    @cython.ccall
    def copy(self) -> Definition:
        """Make a copy of the definition `<'Definition'>`."""
        # ENUM
        return self.__class__(
            *self._elements,
            null=self._null,
            default=self._default,
            charset=self._charset,
            comment=self._comment,
            visible=self._visible,
        )

    # Special methods ----------------------------------------------------------------------
    def __repr__(self) -> str:
        if self._charset is None:
            reprs = [
                "elements=%s" % str(self._elements),
                "null=%s" % self._null,
                "default=" + repr(self._default),
                "charset=None",
                "collate=None",
                "comment=%r" % self._comment,
                "visible=%s" % self._visible,
            ]
        else:
            reprs = [
                "elements=%s" % str(self._elements),
                "null=%s" % self._null,
                "default=" + repr(self._default),
                "charset='%s'" % self._charset._name,
                "collate='%s'" % self._charset._collation,
                "comment=%r" % self._comment,
                "visible=%s" % self._visible,
            ]
        return self._gen_repr(reprs)


@cython.cclass
class BiStringType(StringType):
    """The base class for binary string columns definition in a database table."""

    def __init__(
        self,
        data_type: str,
        default_length: int,
        length: int | None = None,
        null: bool = False,
        default: bytes | None = None,
        comment: str | None = None,
        visible: bool = True,
    ):
        """The base class for binary string columns definition in a database table.

        :param data_type `<'str'>`: The string representation of the MySQL data type.
        :param default_length `<'int'>`: The default length of the string column.
            The column supports variable length, only when `default_length>=0`.
            Otherwise, the column only supports fixed length.
        :param length `<'int/None'>`: The maximum length of the string column. Defaults to `None`.
        :param null `<'bool'>`: Whether the column can contain NULL values. Defaults to `False`.
        :param default `<'bytes/None'>`: The DEFAULT value assigned to the column. Defaults to `None`.
        :param comment `<'str/None'>`: The COMMENT of the column. Defaults to `None`.
        :param visible `<'bool'>`: The visibility of the column. Defaults to `True`.
            An invisible column hidden to queries, but can be accessed if explicitly referenced.
        """
        super().__init__(
            data_type,
            bytes,
            default_length,
            length,
            null,
            default,
            comment,
            visible,
        )
        if self._default is not None:
            self._default = self._validate_default(self._default)

    # Property -----------------------------------------------------------------------------
    @property
    def default(self) -> bytes | None:
        """The DEFAULT value assigned to the column `<'bytes/None'>`."""
        return self._default

    # Generate SQL -------------------------------------------------------------------------
    @cython.ccall
    def _gen_definition_sql(self) -> str:
        """(internal) Generate the definition SQL of the column `<'str'>`."""
        sql: str = self._gen_data_type_sql()
        if not self._null:
            sql += " NOT NULL"
        if self._default is not None:
            sql += self._format_sql(" DEFAULT %s", self._default)
        if self._comment is not None:
            sql += self._format_sql(" COMMENT %s", self._comment)
        if not self._visible:
            sql += " INVISIBLE"
        return sql

    @cython.ccall
    def _gen_data_type_sql(self) -> str:
        """(internal) Generate the DATA TYPE of the column `<'str'>`."""
        if self._length == -1:
            return self._data_type
        return "%s(%d)" % (self._data_type, self._length)

    # Validate -----------------------------------------------------------------------------
    @cython.ccall
    def _validate_default(self, default: object) -> object:
        """(internal) Validate the default value `<'str/None'>`."""
        if default is None:
            return None
        if isinstance(default, bytes):
            return default
        if isinstance(default, str):
            size: cython.Py_ssize_t = str_len(default)
            if str_tailmatch(default, "0x", 0, size, -1):
                try:
                    return bytes.fromhex(default[2:size])
                except Exception:
                    pass
            self._raise_definition_error(
                "DEFINITION 'default' value '%s' is invalid." % default,
            )
        self._raise_definition_error(
            "DEFINITION 'default' value must be <'bytes'> type, "
            "instead got %s %r." % (type(default), default),
        )


@cython.cclass
class BinaryType(BiStringType):
    """The base class for binary & varbinary columns definition in a database table."""

    # Copy ---------------------------------------------------------------------------------
    @cython.ccall
    def copy(self) -> Definition:
        """Make a copy of the definition `<'Definition'>`."""
        return self.__class__(
            self._length,
            self._null,
            self._default,
            self._comment,
            self._visible,
        )

    # Special methods ----------------------------------------------------------------------
    def __repr__(self) -> str:
        return self._gen_repr(
            [
                "length=%s" % self._get_length(),
                "null=%s" % self._null,
                "default=" + repr(self._default),
                "comment=%r" % self._comment,
                "visible=%s" % self._visible,
            ]
        )


@cython.cclass
class BlobType(BiStringType):
    """The base class for blob columns definition in a database table."""

    def __init__(
        self,
        data_type: str,
        null: bool = False,
        comment: str | None = None,
        visible: bool = True,
    ):
        """The base class for blob columns definition in a database table.

        :param data_type `<'str'>`: The string representation of the MySQL data type.
        :param null `<'bool'>`: Whether the column can contain NULL values. Defaults to `False`.
        :param comment `<'str/None'>`: The COMMENT of the column. Defaults to `None`.
        :param visible `<'bool'>`: The visibility of the column. Defaults to `True`.
            An invisible column hidden to queries, but can be accessed if explicitly referenced.
        """
        super().__init__(
            data_type,
            -1,  # -1 means fixed length
            None,
            null,
            None,
            comment,
            visible,
        )

    # Validate -----------------------------------------------------------------------------
    @cython.ccall
    def _validate_default(self, default: object) -> object:
        """(internal) Validate the default value `<'str/None'>`."""
        if default is None:
            return None
        self._raise_operational_error(
            1101,
            "cannot have a default value, instead got %s %r."
            % (type(default), default),
        )

    # Copy ---------------------------------------------------------------------------------
    @cython.ccall
    def copy(self) -> Definition:
        """Make a copy of the definition `<'Definition'>`."""
        return self.__class__(
            self._null,
            self._comment,
            self._visible,
        )

    # Special methods ----------------------------------------------------------------------
    def __repr__(self) -> str:
        return self._gen_repr(
            [
                "null=%s" % self._null,
                "comment=%r" % self._comment,
                "visible=%s" % self._visible,
            ]
        )


class Define:
    """A collection of column definitions for use in database table.

    This class provides a collection of column definitions for use in
    database table. Each definition is a class attribute that can be
    used to define the column's data type, constraints, and other
    attributes used when creating or managing database tables.
    """

    # Numeric
    class TINYINT(IntegerType):
        """Represents a `TINYINT` column definition in a database table.

        ## Integer Value Range:
        - SIGNED: -128 to 127
        - UNSIGNED: 0 to 255
        """

        def __init__(
            self,
            unsigned: bool = False,
            null: bool = False,
            default: int | None = None,
            auto_increment: bool = False,
            comment: str | None = None,
            visible: bool = True,
        ):
            """The `TINYINT` column definition in a database table.

            ## Integer Value Range:
            - SIGNED: -128 to 127
            - UNSIGNED: 0 to 255

            :param unsigned `<'bool'>`: Whether the column is constrainted to UNSIGNED values. Defaults to `False`.
            :param null `<'bool'>`: Whether the column can contain NULL values. Defaults to `False`.
            :param default `<'int/None'>`: The DEFAULT value assigned to the column. Defaults to `None`.
            :param auto_increment `<'bool'>`: Whether to enable AUTO_INCREMENT. Defaults to `False`.
            :param comment `<'str/None'>`: The COMMENT of the column. Defaults to `None`.
            :param visible `<'bool'>`: The visibility of the column. Defaults to `True`.
                An invisible column hidden to queries, but can be accessed if explicitly referenced.
            """
            super().__init__(
                "TINYINT",
                unsigned,
                null,
                default,
                auto_increment,
                comment,
                visible,
            )

    class SMALLINT(IntegerType):
        """Represents a `SMALLINT` column definition in a database table.

        ## Integer Value Range:
        - SIGNED: -32,768 to 32,767
        - UNSIGNED: 0 to 65,535
        """

        def __init__(
            self,
            unsigned: bool = False,
            null: bool = False,
            default: int | None = None,
            auto_increment: bool = False,
            comment: str | None = None,
            visible: bool = True,
        ):
            """The `SMALLINT` column definition in a database table.

            ## Integer Value Range:
            - SIGNED: -32,768 to 32,767
            - UNSIGNED: 0 to 65,535

            :param unsigned `<'bool'>`: Whether the column is constrainted to UNSIGNED values. Defaults to `False`.
            :param null `<'bool'>`: Whether the column can contain NULL values. Defaults to `False`.
            :param default `<'int/None'>`: The DEFAULT value assigned to the column. Defaults to `None`.
            :param auto_increment `<'bool'>`: Whether to enable AUTO_INCREMENT. Defaults to `False`.
            :param comment `<'str/None'>`: The COMMENT of the column. Defaults to `None`.
            :param visible `<'bool'>`: The visibility of the column. Defaults to `True`.
                An invisible column hidden to queries, but can be accessed if explicitly referenced.
            """
            super().__init__(
                "SMALLINT",
                unsigned,
                null,
                default,
                auto_increment,
                comment,
                visible,
            )

    class MEDIUMINT(IntegerType):
        """Represents a `MEDIUMINT` column definition in a database table.

        ## Integer Value Range:
        - SIGNED: -8,388,608 to 8,388,607
        - UNSIGNED: 0 to 16,777,215
        """

        def __init__(
            self,
            unsigned: bool = False,
            null: bool = False,
            default: int | None = None,
            auto_increment: bool = False,
            comment: str | None = None,
            visible: bool = True,
        ):
            """The `MEDIUMINT` column definition in a database table.

            ## Integer Value Range:
            - SIGNED: -8,388,608 to 8,388,607
            - UNSIGNED: 0 to 16,777,215

            :param unsigned `<'bool'>`: Whether the column is constrainted to UNSIGNED values. Defaults to `False`.
            :param null `<'bool'>`: Whether the column can contain NULL values. Defaults to `False`.
            :param default `<'int/None'>`: The DEFAULT value assigned to the column. Defaults to `None`.
            :param auto_increment `<'bool'>`: Whether to enable AUTO_INCREMENT. Defaults to `False`.
            :param comment `<'str/None'>`: The COMMENT of the column. Defaults to `None`.
            :param visible `<'bool'>`: The visibility of the column. Defaults to `True`.
                An invisible column hidden to queries, but can be accessed if explicitly referenced.
            """
            super().__init__(
                "MEDIUMINT",
                unsigned,
                null,
                default,
                auto_increment,
                comment,
                visible,
            )

    class INT(IntegerType):
        """Represents an `INT` column definition in a database table.

        ## Integer Value Range:
        - SIGNED: -2,147,483,648 to 2,147,483,647
        - UNSIGNED: 0 to 4,294,967,295
        """

        def __init__(
            self,
            unsigned: bool = False,
            null: bool = False,
            default: int | None = None,
            auto_increment: bool = False,
            comment: str | None = None,
            visible: bool = True,
        ):
            """The `INT` column definition in a database table.

            ## Integer Value Range:
            - SIGNED: -2,147,483,648 to 2,147,483,647
            - UNSIGNED: 0 to 4,294,967,295

            :param unsigned `<'bool'>`: Whether the column is constrainted to UNSIGNED values. Defaults to `False`.
            :param null `<'bool'>`: Whether the column can contain NULL values. Defaults to `False`.
            :param default `<'int/None'>`: The DEFAULT value assigned to the column. Defaults to `None`.
            :param auto_increment `<'bool'>`: Whether to enable AUTO_INCREMENT. Defaults to `False`.
            :param comment `<'str/None'>`: The COMMENT of the column. Defaults to `None`.
            :param visible `<'bool'>`: The visibility of the column. Defaults to `True`.
                An invisible column hidden to queries, but can be accessed if explicitly referenced.
            """
            super().__init__(
                "INT",
                unsigned,
                null,
                default,
                auto_increment,
                comment,
                visible,
            )

    class BIGINT(IntegerType):
        """Represents a `BIGINT` column definition in a database table.

        ## Integer Value Range:
        - SIGNED: -9,223,372,036,854,775,808 to 9,223,372,036,854,775,807
        - UNSIGNED: 0 to 18,446,744,073,709,551,615
        """

        def __init__(
            self,
            unsigned: bool = False,
            null: bool = False,
            default: int | None = None,
            auto_increment: bool = False,
            comment: str | None = None,
            visible: bool = True,
        ):
            """The `BIGINT` column definition in a database table.

            ## Integer Value Range:
            - SIGNED: -9,223,372,036,854,775,808 to 9,223,372,036,854,775,807
            - UNSIGNED: 0 to 18,446,744,073,709,551,615

            :param unsigned `<'bool'>`: Whether the column is constrainted to UNSIGNED values. Defaults to `False`.
            :param null `<'bool'>`: Whether the column can contain NULL values. Defaults to `False`.
            :param default `<'int/None'>`: The DEFAULT value assigned to the column. Defaults to `None`.
            :param auto_increment `<'bool'>`: Whether to enable AUTO_INCREMENT. Defaults to `False`.
            :param comment `<'str/None'>`: The COMMENT of the column. Defaults to `None`.
            :param visible `<'bool'>`: The visibility of the column. Defaults to `True`.
                An invisible column hidden to queries, but can be accessed if explicitly referenced.
            """
            super().__init__(
                "BIGINT",
                unsigned,
                null,
                default,
                auto_increment,
                comment,
                visible,
            )

    class FLOAT(FloatingPointType):
        """Represents a `FLOAT` (4-byte single-precision) column definition in a database table.

        ## Floating-Point Precision:
        - Accurate to approximately 7 decimal places.
        - The actual range might be slightly different depends on hardware or operating system.
        """

        def __init__(
            self,
            null: bool = False,
            default: float | None = None,
            comment: str | None = None,
            visible: bool = True,
        ):
            """The `FLOAT` (4-byte single-precision) column definition in a database table.

            ## Floating-Point Precision:
            - Accurate to approximately 7 decimal places.
            - The actual range might be slightly different depends on hardware or operating system.

            :param null `<'bool'>`: Whether the column can contain NULL values. Defaults to `False`.
            :param default `<'float/None'>`: The DEFAULT value assigned to the column. Defaults to `None`.
            :param comment `<'str/None'>`: The COMMENT of the column. Defaults to `None`.
            :param visible `<'bool'>`: The visibility of the column. Defaults to `True`.
                An invisible column hidden to queries, but can be accessed if explicitly referenced.
            """
            super().__init__(
                "FLOAT",
                null,
                default,
                comment,
                visible,
            )

    class DOUBLE(FloatingPointType):
        """Represents a `DOUBLE` (8-byte double-precision) column definition in a database table.

        ## Floating-Point Precision:
        - Accurate to approximately 15 decimal places.
        - The actual range might be slightly different depends on hardware or operating system.
        """

        def __init__(
            self,
            null: bool = False,
            default: float | None = None,
            comment: str | None = None,
            visible: bool = True,
        ):
            """The `DOUBLE` (8-byte double-precision) column definition in a database table.

            ## Floating-Point Precision:
            - Accurate to approximately 15 decimal places.
            - The actual range might be slightly different depends on hardware or operating system.

            :param null `<'bool'>`: Whether the column can contain NULL values. Defaults to `False`.
            :param default `<'float/None'>`: The DEFAULT value assigned to the column. Defaults to `None`.
            :param comment `<'str/None'>`: The COMMENT of the column. Defaults to `None`.
            :param visible `<'bool'>`: The visibility of the column. Defaults to `True`.
                An invisible column hidden to queries, but can be accessed if explicitly referenced.
            """
            super().__init__(
                "DOUBLE",
                null,
                default,
                comment,
                visible,
            )

    class DECIMAL(FixedPointType):
        """Represents a `DECIMAL` column definition in a database table.

        ## Fixed-Point Precision:
        - Maximum of 65 digits
        - Up to 30 digits after the decimal point.
        """

        def __init__(
            self,
            precision: int | None = None,
            scale: int | None = None,
            null: bool = False,
            default: Decimal | None = None,
            comment: str | None = None,
            visible: bool = True,
        ):
            """The `DECIMAL` column definition in a database table.

            ## Fixed-Point Precision:
            - Maximum of 65 digits
            - Up to 30 digits after the decimal point.

            :param precision `<'int/None'>`: The fixed-point precision (total digits: 1-65). Defaults to `None (10)`.
            :param scale `<'int/None'>`: The fixed-point scale (number of digits after the decimal point: 0-30). Defaults to `None (0)`.
            :param null `<'bool'>`: Whether the column can contain NULL values. Defaults to `False`.
            :param default `<'Decimal/None'>`: The DEFAULT value assigned to the column. Defaults to `None`.
            :param comment `<'str/None'>`: The COMMENT of the column. Defaults to `None`.
            :param visible `<'bool'>`: The visibility of the column. Defaults to `True`.
                An invisible column hidden to queries, but can be accessed if explicitly referenced.
            """
            super().__init__(
                "DECIMAL",
                10,  # default precision
                0,  # default scale
                precision,
                scale,
                null,
                default,
                comment,
                visible,
            )

    # Temporal
    class DATE(DateType):
        """Represents a `DATE` column definition in a database table.

        ## Date Value Range:
        - '1000-01-01' to '9999-12-31'
        """

        def __init__(
            self,
            null: bool = False,
            default: datetime.date | None = None,
            comment: str | None = None,
            visible: bool = True,
        ):
            """The `DATE` column definition in a database table.

            ## Date Value Range:
            - '1000-01-01' to '9999-12-31'

            :param null `<'bool'>`: Whether the column can contain NULL values. Defaults to `False`.
            :param default `<'datetime.date/None'>`: The DEFAULT value assigned to the column. Defaults to `None`.
            :param comment `<'str/None'>`: The COMMENT of the column. Defaults to `None`.
            :param visible `<'bool'>`: The visibility of the column. Defaults to `True`.
                An invisible column hidden to queries, but can be accessed if explicitly referenced.
            """
            super().__init__(
                "DATE",
                null,
                default,
                comment,
                visible,
            )

    class DATETIME(DateTimeType):
        """Represents a `DATETIME` column definition in a database table.

        ## Datetime Value Range:
        - '1000-01-01 00:00:00.000000' to '9999-12-31 23:59:59.499999'
        """

        def __init__(
            self,
            fsp: int | None = None,
            null: bool = False,
            auto_init: bool = False,
            auto_update: bool = False,
            default: datetime.datetime | None = None,
            comment: str | None = None,
            visible: bool = True,
        ):
            """The `DATETIME` column definition in a database table.

            ## Datetime Value Range:
            - '1000-01-01 00:00:00.000000' to '9999-12-31 23:59:59.499999'

            :param fsp `<'int/None'>`: The fractional seconds precision (0-6). Defaults to `None`.
            :param null `<'bool'>`: Whether the column can contain NULL values. Defaults to `False`.
            :param auto_init `<'bool'>`: Whether the column implements DEFAULT CURRENT_TIMESTAMP. Defaults to `False`.
            :param auto_update `<'bool'>`: Whether the column implements ON UPDATE CURRENT_TIMESTAMP. Defaults to `False`.
            :param default `<'datetime.datetime/None'>`: The DEFAULT value assigned to the column. Defaults to `None`.
            :param comment `<'str/None'>`: The COMMENT of the column. Defaults to `None`.
            :param visible `<'bool'>`: The visibility of the column. Defaults to `True`.
                An invisible column hidden to queries, but can be accessed if explicitly referenced.
            """
            super().__init__(
                "DATETIME",
                fsp,
                null,
                auto_init,
                auto_update,
                default,
                comment,
                visible,
            )

    class TIMESTAMP(DateTimeType):
        """Represents a `TIMESTAMP` column definition in a database table.

        ## Timestamp Value Range:
        - '1970-01-01 00:00:01.000000' to '2038-01-19 03:14:07.499999'
        """

        def __init__(
            self,
            fsp: int | None = None,
            null: bool = False,
            auto_init: bool = False,
            auto_update: bool = False,
            default: datetime.datetime | None = None,
            comment: str | None = None,
            visible: bool = True,
        ):
            """The `TIMESTAMP` column definition in a database table.

            ## Timestamp Value Range:
            - '1970-01-01 00:00:01.000000' to '2038-01-19 03:14:07.499999'

            :param fsp `<'int/None'>`: The fractional seconds precision (0-6). Defaults to `None`.
            :param null `<'bool'>`: Whether the column can contain NULL values. Defaults to `False`.
            :param auto_init `<'bool'>`: Whether the column implements DEFAULT CURRENT_TIMESTAMP. Defaults to `False`.
            :param auto_update `<'bool'>`: Whether the column implements ON UPDATE CURRENT_TIMESTAMP. Defaults to `False`.
            :param default `<'datetime.datetime/None'>`: The DEFAULT value assigned to the column. Defaults to `None`.
            :param comment `<'str/None'>`: The COMMENT of the column. Defaults to `None`.
            :param visible `<'bool'>`: The visibility of the column. Defaults to `True`.
                An invisible column hidden to queries, but can be accessed if explicitly referenced.
            """
            super().__init__(
                "TIMESTAMP",
                fsp,
                null,
                auto_init,
                auto_update,
                default,
                comment,
                visible,
            )

    class TIME(TimeType):
        """Represents a `TIME` column definition in a database table.

        ## Time Value Range:
        - '-838:59:59.000000' to '838:59:59.000000'
        """

        def __init__(
            self,
            fsp: int | None = None,
            null: bool = False,
            default: datetime.time | None = None,
            comment: str | None = None,
            visible: bool = True,
        ):
            """The `TIME` column definition in a database table.

            ## Time Value Range:
            - '-838:59:59.000000' to '838:59:59.000000'

            :param fsp `<'int/None'>`: The fractional seconds precision (0-6). Defaults to `None`.
            :param null `<'bool'>`: Whether the column can contain NULL values. Defaults to `False`.
            :param default `<'datetime.time/None'>`: The DEFAULT value assigned to the column. Defaults to `None`.
            :param comment `<'str/None'>`: The COMMENT of the column. Defaults to `None`.
            :param visible `<'bool'>`: The visibility of the column. Defaults to `True`.
                An invisible column hidden to queries, but can be accessed if explicitly referenced.
            """
            super().__init__(
                "TIME",
                fsp,
                null,
                default,
                comment,
                visible,
            )

    class YEAR(YearType):
        """Represents a `YEAR` column definition in a database table.

        ## Year Value Range:
        - 0 to 69 (interpreted as 2000 to 2069)
        - 70 to 99 (interpreted as 1970 to 1999)
        - 1901 to 2155
        """

        def __init__(
            self,
            null: bool = False,
            default: int | datetime.date | datetime.datetime | None = None,
            comment: str | None = None,
            visible: bool = True,
        ):
            """The `YEAR` column definition in a database table.

            ## Year Value Range:
            - 0 to 69 (interpreted as 2000 to 2069)
            - 70 to 99 (interpreted as 1970 to 1999)
            - 1901 to 2155

            :param null `<'bool'>`: Whether the column can contain NULL values. Defaults to `False`.
            :param default `<'int/datetime.date/datetime.datetime/None'>`: The DEFAULT value assigned to the column. Defaults to `None`.
            :param comment `<'str/None'>`: The COMMENT of the column. Defaults to `None`.
            :param visible `<'bool'>`: The visibility of the column. Defaults to `True`.
                An invisible column hidden to queries, but can be accessed if explicitly referenced.
            """
            super().__init__(
                "YEAR",
                null,
                default,
                comment,
                visible,
            )

    # Character String
    class CHAR(CharType):
        """Represents a `CHAR` column definition in a database table.

        ## Character Length Range:
        - 0 to 255 characters
        """

        def __init__(
            self,
            length: int | None = None,
            null: bool = False,
            default: str | None = None,
            charset: object | None = None,
            collate: object | None = None,
            comment: str | None = None,
            visible: bool = True,
        ):
            """The `CHAR` column definition in a database table.

            ## Character Length Range:
            - 0 to 255 characters

            :param length `<'int/None'>`: The length of the string column (0-255). Defaults to `None (1)`.
            :param null `<'bool'>`: Whether the column can contain NULL values. Defaults to `False`.
            :param default `<'str/None'>`: The DEFAULT value assigned to the column. Defaults to `None`.
            :param charset `<'str/Charset/None'>`: The CHARACTER SET of the column. Defaults to `None`.
            :param collate `<'str/None'>`: The COLLATION of the column. Defaults to `None`.
            :param comment `<'str/None'>`: The COMMENT of the column. Defaults to `None`.
            :param visible `<'bool'>`: The visibility of the column. Defaults to `True`.
                An invisible column hidden to queries, but can be accessed if explicitly referenced.
            """
            super().__init__(
                "CHAR",
                1,
                length,
                null,
                default,
                charset,
                collate,
                comment,
                visible,
            )

    class VARCHAR(CharType):
        """Represents a `VARCHAR` column definition in a database table.

        ## Character Length Range:
        - 0 to 65,535 characters
        """

        def __init__(
            self,
            length: int,
            null: bool = False,
            default: str | None = None,
            charset: object | None = None,
            collate: object | None = None,
            comment: str | None = None,
            visible: bool = True,
        ):
            """The `VARCHAR` column definition in a database table.

            ## Character Length Range:
            - 0 to 65,535 characters

            :param length `<'int/None'>`: The length of the string column (0-65,535).
            :param null `<'bool'>`: Whether the column can contain NULL values. Defaults to `False`.
            :param default `<'str/None'>`: The DEFAULT value assigned to the column. Defaults to `None`.
            :param charset `<'str/Charset/None'>`: The CHARACTER SET of the column. Defaults to `None`.
            :param collate `<'str/None'>`: The COLLATION of the column. Defaults to `None`.
            :param comment `<'str/None'>`: The COMMENT of the column. Defaults to `None`.
            :param visible `<'bool'>`: The visibility of the column. Defaults to `True`.
                An invisible column hidden to queries, but can be accessed if explicitly referenced.
            """
            super().__init__(
                "VARCHAR",
                1_000_000,  # any value larger than 65535
                length,
                null,
                default,
                charset,
                collate,
                comment,
                visible,
            )

    class TINYTEXT(TextType):
        """Represents a `TINYTEXT` column definition in a database table.

        ## Character Length Range:
        - 0 to 255 characters
        """

        def __init__(
            self,
            null: bool = False,
            charset: object | None = None,
            collate: object | None = None,
            comment: str | None = None,
            visible: bool = True,
        ):
            """The `TINYTEXT` column definition in a database table.

            ## Character Length Range:
            - 0 to 255 characters

            :param null `<'bool'>`: Whether the column can contain NULL values. Defaults to `False`.
            :param charset `<'str/Charset/None'>`: The CHARACTER SET of the column. Defaults to `None`.
            :param collate `<'str/None'>`: The COLLATION of the column. Defaults to `None`.
            :param comment `<'str/None'>`: The COMMENT of the column. Defaults to `None`.
            :param visible `<'bool'>`: The visibility of the column. Defaults to `True`.
                An invisible column hidden to queries, but can be accessed if explicitly referenced.
            """
            super().__init__(
                "TINYTEXT",
                null,
                charset,
                collate,
                comment,
                visible,
            )

    class TEXT(TextType):
        """Represents a `TEXT` column definition in a database table.

        ## Character Length Range:
        - 0 to 65,535 characters
        """

        def __init__(
            self,
            null: bool = False,
            charset: object | None = None,
            collate: object | None = None,
            comment: str | None = None,
            visible: bool = True,
        ):
            """The `TEXT` column definition in a database table.

            ## Character Length Range:
            - 0 to 65,535 characters

            :param null `<'bool'>`: Whether the column can contain NULL values. Defaults to `False`.
            :param charset `<'str/Charset/None'>`: The CHARACTER SET of the column. Defaults to `None`.
            :param collate `<'str/None'>`: The COLLATION of the column. Defaults to `None`.
            :param comment `<'str/None'>`: The COMMENT of the column. Defaults to `None`.
            :param visible `<'bool'>`: The visibility of the column. Defaults to `True`.
                An invisible column hidden to queries, but can be accessed if explicitly referenced.
            """
            super().__init__(
                "TEXT",
                null,
                charset,
                collate,
                comment,
                visible,
            )

    class MEDIUMTEXT(TextType):
        """Represents a `MEDIUMTEXT` column definition in a database table.

        ## Character Length Range:
        - 0 to 16,777,215 characters
        """

        def __init__(
            self,
            null: bool = False,
            charset: object | None = None,
            collate: object | None = None,
            comment: str | None = None,
            visible: bool = True,
        ):
            """The `MEDIUMTEXT` column definition in a database table.

            ## Character Length Range:
            - 0 to 16,777,215 characters

            :param null `<'bool'>`: Whether the column can contain NULL values. Defaults to `False`.
            :param charset `<'str/Charset/None'>`: The CHARACTER SET of the column. Defaults to `None`.
            :param collate `<'str/None'>`: The COLLATION of the column. Defaults to `None`.
            :param comment `<'str/None'>`: The COMMENT of the column. Defaults to `None`.
            :param visible `<'bool'>`: The visibility of the column. Defaults to `True`.
                An invisible column hidden to queries, but can be accessed if explicitly referenced.
            """
            super().__init__(
                "MEDIUMTEXT",
                null,
                charset,
                collate,
                comment,
                visible,
            )

    class LONGTEXT(TextType):
        """Represents a `LONGTEXT` column definition in a database table.

        ## Character Length Range:
        - 0 to 4,294,967,295 characters
        """

        def __init__(
            self,
            null: bool = False,
            charset: object | None = None,
            collate: object | None = None,
            comment: str | None = None,
            visible: bool = True,
        ):
            """The `LONGTEXT` column definition in a database table.

            ## Character Length Range:
            - 0 to 4,294,967,295 characters

            :param null `<'bool'>`: Whether the column can contain NULL values. Defaults to `False`.
            :param charset `<'str/Charset/None'>`: The CHARACTER SET of the column. Defaults to `None`.
            :param collate `<'str/None'>`: The COLLATION of the column. Defaults to `None`.
            :param comment `<'str/None'>`: The COMMENT of the column. Defaults to `None`.
            :param visible `<'bool'>`: The visibility of the column. Defaults to `True`.
                An invisible column hidden to queries, but can be accessed if explicitly referenced.
            """
            super().__init__(
                "LONGTEXT",
                null,
                charset,
                collate,
                comment,
                visible,
            )

    class ENUM(EnumType):
        """Represents an `ENUM` column definition in a database table.

        ## Enumeration Elements:
        - Maximum of 65,535 distinct elements
        """

        def __init__(
            self,
            *elements: str,
            null: bool = False,
            default: str | None = None,
            charset: object | None = None,
            collate: object | None = None,
            comment: str | None = None,
            visible: bool = True,
        ):
            """The `ENUM` column definition in a database table.

            ## Enumeration Elements:
            - Maximum of 65,535 distinct elements

            :param elements `<'*str'>`: The elements of the enumeration.
            :param null `<'bool'>`: Whether the column can contain NULL values. Defaults to `False`.
            :param default `<'str/None'>`: The DEFAULT value assigned to the column. Defaults to `None`.
            :param charset `<'str/Charset/None'>`: The CHARACTER SET of the column. Defaults to `None`.
            :param collate `<'str/None'>`: The COLLATION of the column. Defaults to `None`.
            :param comment `<'str/None'>`: The COMMENT of the column. Defaults to `None`.
            :param visible `<'bool'>`: The visibility of the column. Defaults to `True`.
                An invisible column hidden to queries, but can be accessed if explicitly referenced.
            """
            super().__init__(
                "ENUM",
                elements,
                null,
                default,
                charset,
                collate,
                comment,
                visible,
            )

    # Binary String
    class BINARY(BinaryType):
        """Represents a `BINARY` column definition in a database table.

        ## Binary Length Range:
        - 0 to 255 bytes
        """

        def __init__(
            self,
            length: int | None = None,
            null: bool = False,
            default: bytes | None = None,
            comment: str | None = None,
            visible: bool = True,
        ):
            """The `BINARY` column definition in a database table.

            ## Binary Length Range:
            - 0 to 255 bytes

            :param length `<'int/None'>`: The length of the binary column (0-255). Defaults to `None (1)`.
            :param null `<'bool'>`: Whether the column can contain NULL values. Defaults to `False`.
            :param default `<'bytes/None'>`: The DEFAULT value assigned to the column. Defaults to `None`.
            :param comment `<'str/None'>`: The COMMENT of the column. Defaults to `None`.
            :param visible `<'bool'>`: The visibility of the column. Defaults to `True`.
                An invisible column hidden to queries, but can be accessed if explicitly referenced.
            """
            super().__init__(
                "BINARY",
                1,
                length,
                null,
                default,
                comment,
                visible,
            )

    class VARBINARY(BinaryType):
        """Represents a `VARBINARY` column definition in a database table.

        ## Binary Length Range:
        - 0 to 65,535 bytes
        """

        def __init__(
            self,
            length: int,
            null: bool = False,
            default: bytes | None = None,
            comment: str | None = None,
            visible: bool = True,
        ):
            """The `VARBINARY` column definition in a database table.

            ## Binary Length Range:
            - 0 to 65,535 bytes

            :param length `<'int/None'>`: The length of the binary column (0-255). Defaults to `None (1)`.
            :param null `<'bool'>`: Whether the column can contain NULL values. Defaults to `False`.
            :param default `<'bytes/None'>`: The DEFAULT value assigned to the column. Defaults to `None`.
            :param comment `<'str/None'>`: The COMMENT of the column. Defaults to `None`.
            :param visible `<'bool'>`: The visibility of the column. Defaults to `True`.
                An invisible column hidden to queries, but can be accessed if explicitly referenced.
            """
            super().__init__(
                "VARBINARY",
                1_000_000,  # any value larger than 65535
                length,
                null,
                default,
                comment,
                visible,
            )

    class TINYBLOB(BlobType):
        """Represents a `TINYBLOB` column definition in a database table.

        ## Binary Length Range:
        - 0 to 255 bytes
        """

        def __init__(
            self,
            null: bool = False,
            comment: str | None = None,
            visible: bool = True,
        ):
            """The `TINYBLOB` column definition in a database table.

            ## Binary Length Range:
            - 0 to 255 bytes

            :param null `<'bool'>`: Whether the column can contain NULL values. Defaults to `False`.
            :param comment `<'str/None'>`: The COMMENT of the column. Defaults to `None`.
            :param visible `<'bool'>`: The visibility of the column. Defaults to `True`.
                An invisible column hidden to queries, but can be accessed if explicitly referenced.
            """
            super().__init__(
                "TINYBLOB",
                null,
                comment,
                visible,
            )

    class BLOB(BlobType):
        """Represents a `BLOB` column definition in a database table.

        ## Binary Length Range:
        - 0 to 65,535 bytes
        """

        def __init__(
            self,
            null: bool = False,
            comment: str | None = None,
            visible: bool = True,
        ):
            """The `BLOB` column definition in a database table.

            ## Binary Length Range:
            - 0 to 65,535 bytes

            :param null `<'bool'>`: Whether the column can contain NULL values. Defaults to `False`.
            :param comment `<'str/None'>`: The COMMENT of the column. Defaults to `None`.
            :param visible `<'bool'>`: The visibility of the column. Defaults to `True`.
                An invisible column hidden to queries, but can be accessed if explicitly referenced.
            """
            super().__init__(
                "BLOB",
                null,
                comment,
                visible,
            )

    class MEDIUMBLOB(BlobType):
        """Represents a `MEDIUMBLOB` column definition in a database table.

        ## Binary Length Range:
        - 0 to 16,777,215 bytes
        """

        def __init__(
            self,
            null: bool = False,
            comment: str | None = None,
            visible: bool = True,
        ):
            """The `MEDIUMBLOB` column definition in a database table.

            ## Binary Length Range:
            - 0 to 16,777,215 bytes

            :param null `<'bool'>`: Whether the column can contain NULL values. Defaults to `False`.
            :param comment `<'str/None'>`: The COMMENT of the column. Defaults to `None`.
            :param visible `<'bool'>`: The visibility of the column. Defaults to `True`.
                An invisible column hidden to queries, but can be accessed if explicitly referenced.
            """
            super().__init__(
                "MEDIUMBLOB",
                null,
                comment,
                visible,
            )

    class LONGBLOB(BlobType):
        """Represents a `LONGBLOB` column definition in a database table.

        ## Binary Length Range:
        - 0 to 4,294,967,295 bytes
        """

        def __init__(
            self,
            null: bool = False,
            comment: str | None = None,
            visible: bool = True,
        ):
            """The `LONGBLOB` column definition in a database table.

            ## Binary Length Range:
            - 0 to 4,294,967,295 bytes

            :param null `<'bool'>`: Whether the column can contain NULL values. Defaults to `False`.
            :param comment `<'str/None'>`: The COMMENT of the column. Defaults to `None`.
            :param visible `<'bool'>`: The visibility of the column. Defaults to `True`.
                An invisible column hidden to queries, but can be accessed if explicitly referenced.
            """
            super().__init__(
                "LONGBLOB",
                null,
                comment,
                visible,
            )


# Column -----------------------------------------------------------------------------------------------------
@cython.cclass
class Column(Element):
    """Represents a column in a database table."""

    # Common
    _definition: Definition
    _primary_key: cython.bint
    _unique_key: cython.bint
    _indexed: cython.bint
    # Generated column
    _expression: str
    _virtual: cython.int

    def __init__(self, definition: object):
        """The column in a database table.

        :param definition `<'Definition'>`: The definition of the column.
        """
        super().__init__("COLUMN", "COLUMN")
        if not isinstance(definition, Definition):
            self._raise_definition_error(
                "definition expects an instance of <'Definition'> (e.g., 'Define.INT()), "
                "instead got %s %r." % (type(definition), definition)
            )
        self._set_definition(definition)
        self._expression = None
        self._virtual = -1

    # Property -------------------------------------------------------------------------------------------
    @property
    def name(self) -> str:
        """The name of the column `<'str'>`."""
        self._assure_ready()
        return self._name

    @property
    def db_name(self) -> str:
        """The database name of the column `<'str'>`."""
        self._assure_ready()
        return self._db_name

    @property
    def tb_name(self) -> str:
        """The table name of the column `<'str'>`."""
        self._assure_ready()
        return self._tb_name

    @property
    def tb_qualified_name(self) -> str:
        """The qualified table name '{db_name}.{tb_name}' `<'str'>`."""
        self._assure_ready()
        return self._tb_qualified_name

    @property
    def definition(self) -> Definition:
        """The definition of the column `<'Definition'>`."""
        return self._definition

    @property
    def primary_key(self) -> bool:
        """Whether the column is (part of) the PRIMARY KEY `<'bool'>`."""
        return self._primary_key

    @property
    def unique_key(self) -> bool:
        """Whether the column is (part of) a UNIQUE KEY `<'bool'>`."""
        return self._unique_key

    @property
    def indexed(self) -> bool:
        """Whether the column is (part of) an INDEX `<'bool'>`."""
        return self._indexed

    @property
    def position(self) -> int:
        """The ordinal position of the column in the table `<'int'>`."""
        self._assure_ready()
        return self._el_position

    # Sync ---------------------------------------------------------------------------------
    @cython.ccall
    def Initialize(self, force: cython.bint = False) -> Logs:
        """[sync] Initialize the column `<'Logs'>`.

        :param force `<'bool'>`: Whether to force the initialization even
            the column has already been initialized. Defaults to `False`.

        ## Explanation (difference from add)
        - Add (insert) the column to the table if not exists.
        """
        logs: Logs = Logs()
        # Skip
        if self._initialized and not force:
            return logs
        # Initialize: column
        if not self.Exists():
            logs.extend(self.Add(self._el_position))
        else:
            logs.extend(self.SyncFromRemote())
        # Finished
        self._set_initialized(True)
        return logs

    @cython.ccall
    def Add(self, position: cython.int = 0) -> Logs:
        """[sync] Add (insert) the column to the table `<'Logs'>`.

        :param position `<'int'>`: The desired ordinal position for the column. Defaults to `0`.
            If `position >= 1`, the column is inserted at that postition. Otherwise
            (including `0`), the column is appended to the end of the table.

        :raises `<'OperationalError'>`: If a column with the same name already exists.
        """
        # Execute alteration
        cols = self.ShowColumnNames()
        sql: str = self._gen_add_sql(position, cols)
        with self._pool.acquire() as conn:
            with conn.transaction() as cur:
                cur.execute(sql)
        logs = Logs().log_sql(self, sql)
        logs.log_element_creation(self, False)
        # Sync from remote
        return logs.extend(self.SyncFromRemote())

    @cython.ccall
    @cython.exceptval(-1, check=False)
    def Exists(self) -> cython.bint:
        """[sync] Check if the column exists in the table `<'bool'>`."""
        sql: str = self._gen_exists_sql()
        with self._pool.acquire() as conn:
            with conn.transaction() as cur:
                cur.execute(sql)
                res = cur.fetchone()
        if res is None:
            self._set_initialized(False)
            return False
        return True

    @cython.ccall
    def Drop(self) -> Logs:
        """[sync] Drop the column from the table `<'Logs'>`.

        :raises `<'OperationalError'>`: If the column does not exist.
        """
        sql: str = self._gen_drop_sql()
        with self._pool.acquire() as conn:
            with conn.transaction() as cur:
                cur.execute(sql)
        self._set_initialized(False)
        logs = Logs().log_sql(self, sql)
        return logs.log_element_deletion(self, False)

    def Modify(
        self,
        definition: Definition = None,
        position: cython.int = 0,
    ) -> Logs:
        """[sync] Modify the column `<'Logs'>`.

        :param definition `<'Definition/None'>`: New definition of the column. Defaults to `None`.
            If `None`, retains the current definition.
        :param position `<'int'>`: New ordinal position of the column. Defaults to `0`.
            If `position >= 1`, moves the column to that position. Otherwise,
            leaves the column in its current position.
        """
        return self._Modify(definition, None, position)

    @cython.ccall
    def _Modify(
        self,
        definition: Definition | None,
        expression: object | None,
        position: cython.int,
    ) -> Logs:
        """(internal) [sync] Modify the column `<'Logs'>`.

        :param definition `<'Definition/None'>`: New definition of the column. Defaults to `None`.
            If `None`, retains the current definition.
        :param expression `<'str/SQLFunction/None'>`: New expression of the generated column. Defaults to `None`.
            If `None`, retains the current expression. Only applicable to <'GeneratedColumn'>.
        :param position `<'int'>`: New ordinal position of the column. Defaults to `0`.
            If `position >= 1`, moves the column to that position. Otherwise,
            leaves the column in its current position.
        """
        # Generate modify sql
        meta = self.ShowMetadata()
        cols = self.ShowColumnNames()
        query = self._gen_modify_query(meta, definition, expression, position, cols)
        # Execute modification
        if query.executable():
            with self._pool.acquire() as conn:
                with conn.transaction() as cur:
                    query.execute(cur)
            # . refresh metadata
            meta = self.ShowMetadata()
        # Sync from remote
        self._set_definition(query._definition)
        return self._sync_from_metadata(meta, query._logs)

    @cython.ccall
    def SetVisible(self, visible: cython.bint) -> Logs:
        """[sync] Toggles visibility of the column `<'Logs'>`.

        :param visible `<'bool'>`: The visibility of the column.
            An invisible column hidden to queries, but can be accessed if explicitly referenced.
        """
        # Execute alteration
        sql: str = self._gen_set_visible_sql(visible)
        with self._pool.acquire() as conn:
            with conn.transaction() as cur:
                cur.execute(sql)
        logs = Logs().log_sql(self, sql)
        # Sync from remote
        return logs.extend(self.SyncFromRemote())

    @cython.ccall
    def SetDefault(self, default: object | None) -> Logs:
        """[sync] Set or remove the default value of the column `<'Logs'>`.

        :param default `<'Any/None'>`: New DEFAULT value of the column.
            To remove the existing default value, set default to `None`
        """
        # Execute alteration
        sql: str = self._gen_set_default_sql(default)
        with self._pool.acquire() as conn:
            with conn.transaction() as cur:
                cur.execute(sql)
        logs = Logs().log_sql(self, sql)
        # Sync from remote
        return logs.extend(self.SyncFromRemote())

    @cython.ccall
    def ShowMetadata(self) -> ColumnMetadata:
        """[sync] Show the column metadata from the remote server `<'ColumnMetadata'>`.

        :raises `<'OperationalError'>`: If the column does not exist.
        """
        sql: str = self._gen_show_metadata_sql()
        with self._pool.acquire() as conn:
            conn.set_decode_json(False)
            with conn.transaction(DictCursor) as cur:
                cur.execute(sql)
                res = cur.fetchone()
        if res is None:
            self._raise_operational_error(1072, "does not exist")
        return ColumnMetadata(res)

    @cython.ccall
    def ShowColumnNames(self) -> tuple[str]:
        """[sync] Show all the column names of the table
        (sorted by ordinal position) `<'tuple[str]'>`.
        """
        sql: str = self._gen_show_column_names_sql()
        with self._pool.acquire() as conn:
            with conn.transaction() as cur:
                cur.execute(sql)
                res = cur.fetchall()
        return self._flatten_rows(res)

    @cython.ccall
    def SyncFromRemote(self) -> Logs:
        """[sync] Synchronize the local column configs with the remote server `<'Logs'>`.

        ## Explanation
        - This method does `NOT` modify the remote server column,
          but only changes the local column configurations to match
          the remote server metadata.
        """
        try:
            meta = self.ShowMetadata()
        except sqlerrors.OperationalError as err:
            if err.args[0] == 1072:
                return Logs().log_sync_failed_not_exist(self)
            raise err
        return self._sync_from_metadata(meta)

    @cython.ccall
    def SyncToRemote(self) -> Logs:
        """[sync] Synchronize the remote server column with local configs `<'Logs'>`.

        ## Explanation
        - This method compares the local column configurations with the
          remote server metadata and issues the necessary ALTER TABLE
          statements so that the remote one matches the local settings.
        """
        # Check existence
        if not self.Exists():
            return self.Add(self._el_position)
        # Sync to remote
        return self._Modify(self._definition, self._expression, self._el_position)

    # Async --------------------------------------------------------------------------------
    async def aioInitialize(self, force: cython.bint = False) -> Logs:
        """[async] Initialize the column `<'Logs'>`.

        :param force `<'bool'>`: Whether to force the initialization even
            the column has already been initialized. Defaults to `False`.

        ## Explanation (difference from add)
        - Add (insert) the column to the table if not exists.
        """
        logs: Logs = Logs()
        # Skip
        if self._initialized and not force:
            return logs
        # Initialize: column
        if not await self.aioExists():
            logs.extend(await self.aioAdd(self._el_position))
        else:
            logs.extend(await self.aioSyncFromRemote())
        # Finished
        self._set_initialized(True)
        return logs

    async def aioAdd(self, position: cython.int = 0) -> Logs:
        """[async] Add (insert) the column to the table `<'Logs'>`.

        :param position `<'int'>`: The desired ordinal position for the column. Defaults to `0`.
            If `position >= 1`, the column is inserted at that postition. Otherwise
            (including `0`), the column is appended to the end of the table.

        :raises `<'OperationalError'>`: If a column with the same name already exists.
        """
        # Execute alteration
        cols = await self.aioShowColumnNames()
        sql: str = self._gen_add_sql(position, cols)
        async with self._pool.acquire() as conn:
            async with conn.transaction() as cur:
                await cur.execute(sql)
        logs = Logs().log_sql(self, sql)
        logs.log_element_creation(self, False)
        # Sync from remote
        return logs.extend(await self.aioSyncFromRemote())

    async def aioExists(self) -> bool:
        """[async] Check if the column exists in the table `<'bool'>`."""
        sql: str = self._gen_exists_sql()
        async with self._pool.acquire() as conn:
            async with conn.transaction() as cur:
                await cur.execute(sql)
                res = await cur.fetchone()
        if res is None:
            self._set_initialized(False)
            return False
        return True

    async def aioDrop(self) -> Logs:
        """[async] Drop the column from the table `<'Logs'>`.

        :raises `<'OperationalError'>`: If the column does not exist.
        """
        sql: str = self._gen_drop_sql()
        async with self._pool.acquire() as conn:
            async with conn.transaction() as cur:
                await cur.execute(sql)
        self._set_initialized(False)
        logs = Logs().log_sql(self, sql)
        return logs.log_element_deletion(self, False)

    async def aioModify(
        self,
        definition: Definition = None,
        position: cython.int = 0,
    ) -> Logs:
        """[async] Modify the column `<'Logs'>`.

        :param definition `<'Definition/None'>`: New definition of the column. Defaults to `None`.
            If `None`, retains the current definition.
        :param position `<'int'>`: New ordinal position of the column. Defaults to `0`.
            If `position >= 1`, moves the column to that position. Otherwise,
            leaves the column in its current position.
        """
        return await self._aioModify(definition, None, position)

    async def _aioModify(
        self,
        definition: Definition | None,
        expression: object | None,
        position: cython.int,
    ) -> Logs:
        """(internal) [async] Modify the column `<'Logs'>`.

        :param definition `<'Definition/None'>`: New definition of the column. Defaults to `None`.
            If `None`, retains the current definition.
        :param expression `<'str/SQLFunction/None'>`: New expression of the generated column. Defaults to `None`.
            If `None`, retains the current expression. Only applicable to <'GeneratedColumn'>.
        :param position `<'int'>`: New ordinal position of the column. Defaults to `0`.
            If `position >= 1`, moves the column to that position. Otherwise,
            leaves the column in its current position.
        """
        # Generate modify sql
        meta, cols = await _aio_gather(
            self.aioShowMetadata(), self.aioShowColumnNames()
        )
        query = self._gen_modify_query(meta, definition, expression, position, cols)
        # Execute modification
        if query.executable():
            async with self._pool.acquire() as conn:
                async with conn.transaction() as cur:
                    await query.aio_execute(cur)
            # . refresh metadata
            meta = await self.aioShowMetadata()
        # Sync from remote
        self._set_definition(query._definition)
        return self._sync_from_metadata(meta, query._logs)

    async def aioSetVisible(self, visible: cython.bint) -> Logs:
        """[async] Toggles visibility of the column `<'Logs'>`.

        :param visible `<'bool'>`: The visibility of the column.
            An invisible column hidden to queries, but can be accessed if explicitly referenced.
        """
        # Execute alteration
        sql: str = self._gen_set_visible_sql(visible)
        async with self._pool.acquire() as conn:
            async with conn.transaction() as cur:
                await cur.execute(sql)
        logs = Logs().log_sql(self, sql)
        # Sync from remote
        return logs.extend(await self.aioSyncFromRemote())

    async def aioSetDefault(self, default: object | None) -> Logs:
        """[async] Set or remove the default value of the column `<'Logs'>`.

        :param default `<'Any/None'>`: New DEFAULT value of the column.
            To remove the existing default value, set default to `None`
        """
        # Execute alteration
        sql: str = self._gen_set_default_sql(default)
        async with self._pool.acquire() as conn:
            async with conn.transaction() as cur:
                await cur.execute(sql)
        logs = Logs().log_sql(self, sql)
        # Sync from remote
        return logs.extend(await self.aioSyncFromRemote())

    async def aioShowMetadata(self) -> ColumnMetadata:
        """[async] Show the column metadata from the remote server `<'ColumnMetadata'>`.

        :raises `<'OperationalError'>`: If the column does not exist.
        """
        sql: str = self._gen_show_metadata_sql()
        async with self._pool.acquire() as conn:
            conn.set_decode_json(False)
            async with conn.transaction(AioDictCursor) as cur:
                await cur.execute(sql)
                res = await cur.fetchone()
        if res is None:
            self._raise_operational_error(1072, "does not exist")
        return ColumnMetadata(res)

    async def aioShowColumnNames(self) -> tuple[str]:
        """[async] Show all the column names of the table
        (sorted by ordinal position) `<'tuple[str]'>`.
        """
        sql: str = self._gen_show_column_names_sql()
        async with self._pool.acquire() as conn:
            async with conn.transaction() as cur:
                await cur.execute(sql)
                res = await cur.fetchall()
        return self._flatten_rows(res)

    async def aioSyncFromRemote(self) -> Logs:
        """[async] Synchronize the local column configs with the remote server `<'Logs'>`.

        ## Explanation
        - This method does `NOT` modify the remote server column,
          but only changes the local column configurations to match
          the remote server metadata.
        """
        try:
            meta = await self.aioShowMetadata()
        except sqlerrors.OperationalError as err:
            if err.args[0] == 1072:
                return Logs().log_sync_failed_not_exist(self)
            raise err
        return self._sync_from_metadata(meta)

    async def aioSyncToRemote(self) -> Logs:
        """[async] Synchronize the remote server column with local configs `<'Logs'>`.

        ## Explanation
        - This method compares the local column configurations with the
          remote server metadata and issues the necessary ALTER TABLE
          statements so that the remote one matches the local settings.
        """
        # Check existence
        if not await self.aioExists():
            return await self.aioAdd(self._el_position)
        # Sync to remote
        return await self._aioModify(
            self._definition, self._expression, self._el_position
        )

    # Generate SQL -------------------------------------------------------------------------
    @cython.ccall
    def _gen_definition_sql(self) -> str:
        """(internal) Generate the definition SQL of the column `<'str'>`."""
        self._assure_ready()
        return "%s %s" % (self._name, self._definition._gen_definition_sql())

    @cython.ccall
    def _gen_add_sql(self, position: cython.int, columns: tuple[str]) -> str:
        """(internal) Generate SQL to add (insert) the column `<'str'>`.
        
        :param position `<'int'>`: The desired ordinal position for the column.
        :param columns `<'tuple[str]'>`: All the column names (by ordinal position) of the table before modification.
        """
        self._assure_ready()
        # Add to first
        if position == 1:
            return "ALTER TABLE %s ADD COLUMN %s FIRST;" % (
                self._tb_qualified_name,
                self._gen_definition_sql(),
            )
        # Add to end
        elif position <= 0 or position > tuple_len(columns):
            return "ALTER TABLE %s ADD COLUMN %s;" % (
                self._tb_qualified_name,
                self._gen_definition_sql(),
            )
        # Add to position
        else:
            return "ALTER TABLE %s ADD COLUMN %s AFTER %s;" % (
                self._tb_qualified_name,
                self._gen_definition_sql(),
                columns[position - 2],
            )

    @cython.ccall
    def _gen_exists_sql(self) -> str:
        """(internal) Generate SQL to check if the column exists in the table `<'str'>`."""
        self._assure_ready()
        return (
            "SELECT 1 "
            "FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_NAME = '%s' "
            "AND TABLE_SCHEMA = '%s' "
            "AND COLUMN_NAME = '%s' "
            "LIMIT 1;" % (self._tb_name, self._db_name, self._name)
        )

    @cython.ccall
    def _gen_drop_sql(self) -> str:
        """(internal) Generate SQL to drop the column `<'str'>`."""
        self._assure_ready()
        return "ALTER TABLE %s DROP COLUMN %s;" % (self._tb_qualified_name, self._name)

    @cython.ccall
    def _gen_modify_query(
        self,
        meta: ColumnMetadata,
        definition: Definition,
        expression: object | None,
        position: cython.int,
        columns: tuple[str],
    ) -> ColumnQuery:
        """(internal) Generate query to modify the column `<'ColumnQuery'>`.

        :param meta `<'ColumnMetadata'>`: The remote server column metadata.
        :param definition `<'Definition/None'>`: New definition of the column. `None` means do not change.
        :param expression `<'str/SQLFunction/None'>`: New expression of the generated column. `None` means do not change.
            Only applicable to <'GeneratedColumn'>.
        :param position `<'int'>`: New ordinal position of the column in the table. `0` means do not change.
        :param columns `<'tuple[str]'>`: All the column names (by ordinal position) of the table before modification.
        """
        self._assure_ready()
        query = ColumnQuery()

        # New column
        col: Column = self._construct(definition, expression, meta._virtual)
        col._set_position(position if position > 0 else self._el_position)
        col.set_name(self._name)
        col.setup(self._tb_name, self._db_name, self._charset, None, self._pool)
        # Compare differences
        diff = col._diff_from_metadata(meta)
        # . no differences
        if diff == 0:
            return query  # exit
        # . modify definition
        elif diff == 1:
            # . update definition
            query._definition = col._definition
            # . compose sql
            position = col._el_position
            if position == meta._position:
                sql: str = "ALTER TABLE %s MODIFY COLUMN %s;" % (
                    self._tb_qualified_name,
                    col._gen_definition_sql(),
                )
            else:
                sql: str = "ALTER TABLE %s MODIFY COLUMN %s" % (
                    self._tb_qualified_name,
                    col._gen_definition_sql(),
                )
                size: cython.Py_ssize_t
                # . move to first
                if position == 1:
                    sql += " FIRST;"
                # . move to end
                elif position > (size := tuple_len(columns)) or position <= 0:
                    if meta._position != size:
                        sql += " AFTER %s;" % columns[size - 1]
                    else:
                        sql += ";"
                # . move forward
                elif position < meta._position:
                    sql += " AFTER %s;" % columns[position - 2]
                # . move backward
                else:
                    sql += " AFTER %s;" % columns[position - 1]
            query.set_sql(self, sql)
        # . toggle visibility
        else:
            query.set_sql(self, self._gen_set_visible_sql(col._definition._visible))
        return query

    @cython.ccall
    def _gen_set_visible_sql(self, visible: cython.bint) -> str:
        """(internal) Generate SQL to set the column visibility `<'str'>`."""
        self._assure_ready()
        if visible:
            return "ALTER TABLE %s ALTER COLUMN %s SET VISIBLE;" % (
                self._tb_qualified_name,
                self._name,
            )
        else:
            return "ALTER TABLE %s ALTER COLUMN %s SET INVISIBLE;" % (
                self._tb_qualified_name,
                self._name,
            )

    @cython.ccall
    def _gen_set_default_sql(self, default: object | None) -> str:
        """(internal) Generate SQL to set/drop the column default `<'str'>`."""
        self._assure_ready()
        if default is None:
            return "ALTER TABLE %s ALTER COLUMN %s DROP DEFAULT;" % (
                self._tb_qualified_name,
                self._name,
            )
        else:
            return "ALTER TABLE %s ALTER COLUMN %s SET DEFAULT %s;" % (
                self._tb_qualified_name,
                self._name,
                self._escape_args(self._definition._validate_default(default), False),
            )

    @cython.ccall
    def _gen_show_metadata_sql(self) -> str:
        """(internal) Generate SQL to show column metadata `<'str'>`."""
        self._assure_ready()
        tb_name: str = self._tb_name
        db_name: str = self._db_name
        return (
            "SELECT "
            # . columns [t1]
            "t1.TABLE_CATALOG AS CATALOG_NAME, "
            "t1.TABLE_SCHEMA AS SCHEMA_NAME, "
            "t1.TABLE_NAME AS TABLE_NAME, "
            "t1.COLUMN_NAME AS COLUMN_NAME, "
            "UPPER(t1.COLUMN_TYPE) AS COLUMN_TYPE, "
            "UPPER(t1.COLUMN_KEY) AS COLUMN_KEY, "
            "t1.ORDINAL_POSITION AS ORDINAL_POSITION, "
            "t1.COLUMN_DEFAULT AS COLUMN_DEFAULT, "
            "t1.IS_NULLABLE AS IS_NULLABLE, "
            "UPPER(t1.DATA_TYPE) AS DATA_TYPE, "
            "t1.CHARACTER_MAXIMUM_LENGTH AS CHARACTER_MAXIMUM_LENGTH, "
            "t1.CHARACTER_OCTET_LENGTH AS CHARACTER_OCTET_LENGTH, "
            "t1.NUMERIC_PRECISION AS NUMERIC_PRECISION, "
            "t1.NUMERIC_SCALE AS NUMERIC_SCALE, "
            "t1.DATETIME_PRECISION AS DATETIME_PRECISION, "
            "t1.CHARACTER_SET_NAME AS CHARACTER_SET_NAME, "
            "t1.COLLATION_NAME AS COLLATION_NAME, "
            "UPPER(t1.EXTRA) AS EXTRA, "
            "t1.PRIVILEGES AS PRIVILEGES, "
            "t1.COLUMN_COMMENT AS COLUMN_COMMENT, "
            "t1.GENERATION_EXPRESSION AS GENERATION_EXPRESSION, "
            "t1.SRS_ID AS SRS_ID, "
            # . columns [t2]
            "t2.ENGINE_ATTRIBUTE AS ENGINE_ATTRIBUTE, "
            "t2.SECONDARY_ENGINE_ATTRIBUTE AS SECONDARY_ENGINE_ATTRIBUTE, "
            # . columns [t3]
            "t3.INDEX_NAME AS COLUMN_INDEX_NAME, "
            "t3.NON_UNIQUE AS COLUMN_INDEX_NON_UNIQUE, "
            "t3.SEQ_IN_INDEX AS COLUMN_INDEX_SEQ, "
            # . columns [t4]
            "t4.COLUMN_INDEX_LENGTH AS COLUMN_INDEX_LENGTH "
            # . information_schema.columns
            "FROM INFORMATION_SCHEMA.COLUMNS AS t1 "
            # . information_schema.columns_estensions
            "LEFT JOIN INFORMATION_SCHEMA.COLUMNS_EXTENSIONS AS t2 "
            "ON t1.TABLE_NAME = t2.TABLE_NAME "
            "AND t1.TABLE_SCHEMA = t2.TABLE_SCHEMA "
            "AND t1.COLUMN_NAME = t2.COLUMN_NAME "
            # . information_schema.statistics
            "LEFT JOIN INFORMATION_SCHEMA.STATISTICS AS t3 "
            "ON t1.TABLE_NAME = t3.TABLE_NAME "
            "AND t1.TABLE_SCHEMA = t3.TABLE_SCHEMA "
            "AND t1.COLUMN_NAME = t3.COLUMN_NAME "
            "AND t3.INDEX_SCHEMA = t3.TABLE_SCHEMA "
            # . information_schema.statistics [subquery]
            "LEFT JOIN ("
            "SELECT INDEX_NAME, COUNT(*) AS COLUMN_INDEX_LENGTH "
            "FROM INFORMATION_SCHEMA.STATISTICS "
            "WHERE TABLE_NAME = '%s' "
            "AND TABLE_SCHEMA = '%s' "
            "AND TABLE_SCHEMA = INDEX_SCHEMA "
            "GROUP BY INDEX_NAME) AS t4 "
            "ON t3.INDEX_NAME = t4.INDEX_NAME "
            # . conditions
            "WHERE t1.TABLE_NAME = '%s' "
            "AND t1.TABLE_SCHEMA = '%s' "
            "AND t1.COLUMN_NAME = '%s';"
            % (tb_name, db_name, tb_name, db_name, self._name)
        )

    @cython.ccall
    def _gen_show_column_names_sql(self) -> str:
        """(internal) Generate SQL to select all column names
        of the table sorted by ordinal position `<'str'>`.
        """
        self._assure_ready()
        return (
            "SELECT COLUMN_NAME AS i "
            "FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_NAME = '%s' "
            "AND TABLE_SCHEMA = '%s' "
            "ORDER BY ORDINAL_POSITION ASC;" % (self._tb_name, self._db_name)
        )

    # Metadata -----------------------------------------------------------------------------
    @cython.ccall
    def _sync_from_metadata(self, meta: ColumnMetadata, logs: Logs = None) -> Logs:
        """(internal) Synchronize local configs with the remote column metadata `<'Logs'>`."""
        self._assure_ready()
        if logs is None:
            logs = Logs()

        # Validate
        if self._el_type != meta._el_type:
            logs.log_sync_failed_mismatch(
                self, "column type", self._el_type, meta._el_type
            )
            return logs._skip()  # exit
        if self._name != meta._column_name:
            logs.log_sync_failed_mismatch(
                self, "column name", self._name, meta._column_name
            )
            return logs._skip()  # exit
        if self._tb_name != meta._tb_name:
            logs.log_sync_failed_mismatch(
                self, "table name", self._tb_name, meta._tb_name
            )
            return logs._skip()  # exit
        if self._db_name != meta._db_name:
            logs.log_sync_failed_mismatch(
                self, "database name", self._db_name, meta._db_name
            )
            return logs._skip()  # exit

        # Definition
        logs = self._definition._sync_from_metadata(meta, logs)
        if logs._skip_flag:
            return logs  # exit
        self._set_definition(self._definition)

        # Column position
        if self._el_position != meta._position:
            logs.log_config_int(self, "position", self._el_position, meta._position)
            self._set_position(meta._position)

        # Return logs
        return logs

    @cython.ccall
    @cython.exceptval(-1, check=False)
    def _diff_from_metadata(self, meta: ColumnMetadata) -> cython.int:
        """(internal) Check if the column configurations are different
        from the remote server metadata `<'int'>`.

        :returns `<'int'>`:
        - `0`: Column configurations are identical.
        - `1`: Column configurations differ in more than visibility.
        - `2`: Only the visibility differs.
        """
        # Different
        diff = self._definition._diff_from_metadata(meta)
        if diff == 1:
            return 1
        if self._el_type != meta._el_type:
            return 1
        if self._el_position != meta._position:
            return 1
        # Same or Toggle visibility
        return diff

    # Setter -------------------------------------------------------------------------------
    @cython.ccall
    @cython.exceptval(-1, check=False)
    def setup(
        self,
        tb_name: str,
        db_name: str,
        charset: str | Charset,
        collate: str | None,
        pool: Pool,
    ) -> cython.bint:
        """Setup the column.

        :param tb_name `<'str'>`: The table name of the column.
        :param db_name `<'str'>`: The database name of the column.
        :param charset `<'str/Charset'>`: The charset of the column.
        :param collate `<'str/None'>`: The collation of the table.
        :param pool `<'Pool'>`: The pool of the column.
        """
        self._set_tb_name(tb_name)
        self._set_db_name(db_name)
        self._set_charset(charset, collate)
        self._set_pool(pool)
        self._definition.setup(self)
        return self._assure_ready()

    @cython.ccall
    @cython.exceptval(-1, check=False)
    def set_name(self, name: object) -> cython.bint:
        """Set the name of the Column."""
        if Element.set_name(self, name):
            self._name = self._validate_column_name(name)
        return True

    @cython.ccall
    @cython.exceptval(-1, check=False)
    def _set_definition(self, definition: Definition) -> cython.bint:
        """(internal) Set the definition of the Column `<'bool'>."""
        # Invalid
        if definition is None:
            return False  # exit
        # Set definition
        if definition is not self._definition:
            self._definition = definition.copy()
            self._definition.setup(self)
        # Sync settings
        if definition._charset is None:
            self._definition._charset = self._charset
        else:
            self._charset = definition._charset
        self._primary_key = definition._primary_key
        self._unique_key = definition._unique_key
        self._indexed = definition._indexed
        return True

    # Assure Ready -------------------------------------------------------------------------
    @cython.ccall
    @cython.exceptval(-1, check=False)
    def _assure_ready(self) -> cython.bint:
        """(internal) Assure the column is ready."""
        if not self._el_ready:
            self._assure_name_ready()
            self._assure_tb_name_ready()
            self._assure_position_ready()
            self._definition._assure_ready()
            Element._assure_ready(self)
        return True

    # Copy ---------------------------------------------------------------------------------
    @cython.ccall
    def copy(self) -> Column:
        """Make a copy of the column `<'Column'>`."""
        col: Column = self._construct(
            self._definition,
            self._expression,
            self._virtual,
        )
        col.set_name(self._name)
        return col

    @cython.ccall
    def _construct(
        self,
        definition: Definition,
        expression: object | None,
        virtual: bool | None,
    ) -> Column:
        """(internal) Construct a new column instance `<'Column'>`.

        ## Notice
        - Value `None` means to use the corresponding settings of this column.

        :param definition `<'Definition/None'>`: The definition of the column.
        :param expression `<'str/SQLFunction/None'>`: The expression of the generated column.
            Only applicable to <'GeneratedColumn'>.
        :param virtual `<'bool/None'>`: The generated column is virtual if `True`, else stored physically.
            Only applicable to <'GeneratedColumn'>.
        """
        return Column(self._definition if definition is None else definition)

    # Special Methods ----------------------------------------------------------------------
    def __repr__(self) -> str:
        return "<%s (%s)>" % (self.__class__.__name__, self._gen_definition_sql())

    def __str__(self) -> str:
        self._assure_ready()
        return self._name


@cython.cclass
class GeneratedColumn(Column):
    """Represents a generated column in a database table."""

    def __init__(
        self,
        definition: object,
        expression: object,
        virtual: cython.bint = True,
    ):
        """The column in a database table.

        :param definition `<'Definition'>`: The definition of the column.
        :param expression `<'str/SQLFunction'>`: The expression of the generated column.
        :param virtual `<'bool'>`: The generated column is virtual if `True`, else stored physically. Defaults to `True`.
        """
        super().__init__(definition)
        self._set_el_type("GENERATED COLUMN")
        self._expression = self._validate_expression(expression)
        self._virtual = virtual

    # Property -----------------------------------------------------------------------------
    @property
    def expression(self) -> str:
        """The expression of the generated column `<'str'>`."""
        return self._expression

    @property
    def virtual(self) -> bool:
        """Whether the generated column is virtual or stored physically `<'bool'>`."""
        return True if self._virtual == 1 else False

    # Sync ---------------------------------------------------------------------------------
    def Modify(
        self,
        definition: Definition = None,
        expression: object | None = None,
        position: cython.int = 0,
    ) -> Logs:
        """[sync] Modify the generated column `<'Logs'>`.

        :param definition `<'Definition/None'>`: New definition of the column. Defaults to `None`.
            If `None`, retains the current definition.
        :param expression `<'str/SQLFunction/None'>`: New expression of the generated column. Defaults to `None`.
            If `None`, retains the current expression.
        :param position `<'int'>`: New ordinal position of the generated column. Defaults to `0`.
            If `position >= 1`, moves the column to that position. Otherwise,
            leaves the column in its current position.
        """
        return self._Modify(definition, expression, position)

    @cython.ccall
    def SetDefault(self, default: object | None) -> Logs:
        """[sync] Set or remove the default value of the column `<'Logs'>`.

        :param default `<'Any/None'>`: New DEFAULT value of the column.

        ## Notice
        - Generated columns does not support default values. Calling
          this method raises `OperationalError`.
        """
        self._raise_operational_error(
            1221,
            "cannot have a default value, instead got %s %r."
            % (type(default), default),
        )

    # Async --------------------------------------------------------------------------------
    async def aioModify(
        self,
        definition: Definition = None,
        expression: object | None = None,
        position: cython.int = 0,
    ) -> Logs:
        """[async] Modify the generated column `<'Logs'>`.

        :param definition `<'Definition/None'>`: New definition of the column. Defaults to `None`.
            If `None`, retains the current definition.
        :param expression `<'str/SQLFunction/None'>`: New expression of the generated column. Defaults to `None`.
            If `None`, retains the current expression.
        :param position `<'int'>`: New ordinal position of the generated column. Defaults to `0`.
            If `position >= 1`, moves the column to that position. Otherwise,
            leaves the column in its current position.
        """
        return await self._aioModify(definition, expression, position)

    async def aioSetDefault(self, default: object | None) -> Logs:
        """[async] Set or remove the default value of the column `<'Logs'>`.

        :param default `<'Any/None'>`: New DEFAULT value of the column.

        ## Notice
        - Generated columns does not support default values. Calling
          this method raises `OperationalError`.
        """
        self._raise_operational_error(
            1221,
            "cannot have a default value, instead got %s %r."
            % (type(default), default),
        )

    # Generate SQL -------------------------------------------------------------------------
    @cython.ccall
    def _gen_definition_sql(self) -> str:
        """(internal) Generate the definition SQL of the column `<'str'>`."""
        self._assure_ready()
        sql: str = "%s %s GENERATED ALWAYS AS (%s)" % (
            self._name,
            self._definition._gen_data_type_sql(),
            self._expression,
        )
        if self._virtual == 0:
            sql += " STORED"
        if not self._definition._null:
            sql += " NOT NULL"
        if self._definition._comment is not None:
            sql += self._format_sql(" COMMENT %s", self._definition._comment)
        if not self._definition._visible:
            sql += " INVISIBLE"
        return sql

    # Metadata -----------------------------------------------------------------------------
    @cython.ccall
    def _sync_from_metadata(self, meta: ColumnMetadata, logs: Logs = None) -> Logs:
        """(internal) Synchronize local configs with the remote column metadata `<'Logs'>`."""
        logs = Column._sync_from_metadata(self, meta, logs)
        if logs._skip_flag:
            return logs

        # Generated expression
        if self._expression != meta._expression:
            logs.log_config_obj(self, "expression", self._expression, meta._expression)
            self._expression = meta._expression

        # Generated virtual
        if self._virtual != meta._virtual:
            logs.log_config_bool(self, "virtual", self._virtual, meta._virtual)
            self._virtual = meta._virtual

        # Return logs
        return logs

    @cython.ccall
    @cython.exceptval(-1, check=False)
    def _diff_from_metadata(self, meta: ColumnMetadata) -> cython.int:
        """(internal) Check if the column configurations are different
        from the remote server metadata `<'int'>`.

        :returns `<'int'>`:
        - `0`: Column configurations are identical.
        - `1`: Column configurations differ in more than visibility.
        - `2`: Only the visibility differs.
        """
        # Different
        diff = Column._diff_from_metadata(self, meta)
        if diff == 1:
            return 1
        if self._expression != meta._expression:
            return 1
        if self._virtual != meta._virtual:
            return 1
        # Same or Toggle visibility
        return diff

    # Setter -------------------------------------------------------------------------------
    @cython.ccall
    @cython.exceptval(-1, check=False)
    def _set_definition(self, definition: Definition) -> cython.bint:
        """(internal) Set the definition of the Generated Column."""
        Column._set_definition(self, definition)
        # Adjustement for generated column
        self._definition._default = None
        self._definition._auto_increment = False
        self._definition._auto_init = False
        self._definition._auto_update = False
        return True

    # Copy ---------------------------------------------------------------------------------
    @cython.ccall
    def _construct(
        self,
        definition: Definition,
        expression: object | None,
        virtual: bool | None,
    ) -> Column:
        """(internal) Construct a new column instance `<'Column'>`.

        ## Notice
        - Value `None` means to use the corresponding settings of this column.

        :param definition `<'Definition/None'>`: The definition of the column.
        :param expression `<'str/SQLFunction/None'>`: The expression of the generated column.
            Only applicable to <'GeneratedColumn'>.
        :param virtual `<'bool/None'>`: The generated column is virtual if `True`, else stored physically.
            Only applicable to <'GeneratedColumn'>.
        """
        return GeneratedColumn(
            # fmt: off
            self._definition if definition is None else definition,
            self._expression if expression is None else self._validate_expression(expression),
            self._virtual if virtual is None else bool(virtual),
            # fmt: on
        )


# Columns ----------------------------------------------------------------------------------------------------
@cython.cclass
class Columns(Elements):
    """Represents a collection of columns in a table.

    Works as a dictionary where keys are the column names
    and values the column instances.
    """

    def __init__(self, *columns: Column):
        """The collection of columns in a table.

        Works as a dictionary where keys are the column names
        and values the column instances.

        :param columns `<'*Column'>`: The columns in a table.
        """
        super().__init__("COLUMN", "COLUMNS", Column, *columns)

    # Collection ---------------------------------------------------------------------------
    @cython.ccall
    def _search_type(self, types: object, exact: cython.bint) -> Columns:
        """(internal) Find the columns in the collection by MySQL type `<'str'>` or Python class `<'type'>` `<'Columns'>`.

        :param type(s) `<'str/type/Column/Definition/tuple/list'>`: One or more MySQL types,
            Python types, Column Definition or Column instances to search for.

        The string representation of mysql type(s) or column/definition type(s) to search for.
        :param exact `<'bool'`>: Perform exact matches if `True`, else partial matches. Defaults to `True`.
            - **`True`**: only elements whose type exactly matches the provided values are returned.
            - **`False`**: returns elements whose MySQL type name contains any of the provided strings,
              or whose Python type is a subclass of any provided class.
        """
        # Validate
        if self._size == 0 or types is None:
            return self.__class__()  # exit
        el_types: set = self._extract_element_types(types, "'search_type'")
        if set_len(el_types) == 0:
            return self.__class__()  # exit

        # Search Type
        el: Column
        res: list = []
        for el in self._el_dict.values():
            if exact:
                if (
                    set_contains(el_types, el._definition._data_type)
                    or set_contains(el_types, el._el_type)
                    or set_contains(el_types, type(el._definition))
                    or set_contains(el_types, type(el))
                    or set_contains(el_types, el._definition._python_type)
                ):
                    res.append(el)
            else:
                for _type in el_types:
                    if isinstance(_type, str):
                        # fmt: off
                        if (
                            str_contains(el._definition._data_type, _type)
                            or str_contains(el._el_type, _type) 
                        ):
                            res.append(el)
                            break
                        # fmt: on
                    elif isinstance(el._definition, _type) or isinstance(el, _type):
                        res.append(el)
                        break
                    elif el._definition._python_type is _type:
                        res.append(el)
                        break
        return Columns(*res)

    # Generate SQL -------------------------------------------------------------------------
    @cython.ccall
    def _gen_definition_sql(self, indent: cython.int = 0) -> str:
        """(internal) Generate the definition SQL of the collection `<'str'>`.

        :param indent `<'int'>`: The indentation of the definition SQL. Defaults to `0`.
        """
        # Empty collection
        if self._size == 0:
            return ""

        # Generate SQL
        col: Column
        sqls = [col._gen_definition_sql() for col in self._sorted_elements()]
        # . without indent
        nxt: str = ",\n"
        if indent == 0:
            return nxt.join(sqls)
        # With indent
        if indent > 0:
            pad: str = "\t" * indent
            nxt += pad
            return pad + nxt.join(sqls)
        # Invalid Indent
        self._raise_argument_error(
            "definition SQL statement 'indent' must be a positive integer, "
            "instead got %d." % indent
        )

    # Setter -------------------------------------------------------------------------------
    @cython.ccall
    @cython.exceptval(-1, check=False)
    def setup(
        self,
        tb_name: str,
        db_name: str,
        charset: str | Charset,
        collate: str | None,
        pool: Pool,
    ) -> cython.bint:
        """Setup the column collection.

        :param tb_name `<'str'>`: The table name of the column collection.
        :param db_name `<'str'>`: The database name of the column collection.
        :param charset `<'str/Charset'>`: The charset of the column collection.
        :param collate `<'str/None'>`: The collation of the column collection.
        :param pool `<'Pool'>`: The pool of the column collection.
        """
        # Collection
        self._set_tb_name(tb_name)
        self._set_db_name(db_name)
        self._set_charset(charset, collate)
        self._set_pool(pool)
        # Elements
        tb_name = self._tb_name
        db_name = self._db_name
        charset = self._charset
        pool = self._pool
        el: Column
        for el in self._el_dict.values():
            if not el._el_ready:
                el.setup(tb_name, db_name, charset, None, pool)
        return self._assure_ready()

    # Assure Ready -------------------------------------------------------------------------
    @cython.ccall
    @cython.exceptval(-1, check=False)
    def _assure_ready(self) -> cython.bint:
        """(internal) Assure the column collection is ready."""
        if not self._el_ready:
            self._assure_tb_name_ready()
            Elements._assure_ready(self)
        return True

    # Copy ---------------------------------------------------------------------------------
    @cython.ccall
    def copy(self) -> Columns:
        """Make a copy of the column collection `<'Columns'>`."""
        el: Column
        return Columns(*[el.copy() for el in self._el_dict.values()])


# Metadata ---------------------------------------------------------------------------------------------------
@cython.cclass
class ColumnMetadata(Metadata):
    """Represents the metadata from the remote server of a column."""

    # Base data
    _db_name: str
    _tb_name: str
    _column_name: str
    _column_type: str
    _position: cython.int
    _default: str
    _null: cython.bint
    _data_type: str
    _character_maximum_length: object
    _numeric_precision: object
    _numeric_scale: object
    _datetime_precision: object
    _charset: Charset
    _extra: str
    _comment: str
    _expression: str
    _virtual: cython.int
    # Additional data
    _el_type: str
    _visible: cython.bint
    # . integer
    _auto_increment: cython.bint
    _unsigned: cython.bint
    # . datetime
    _auto_init: cython.bint
    _auto_update: cython.bint
    # . index
    _primary_key: cython.bint
    _unique_key: cython.bint
    _indexed: cython.bint
    _column_index_name: str
    _column_index_seq: cython.int
    _column_index_length: cython.int

    def __init__(self, meta: dict):
        """The metadata from the remote server of a column.

        :param meta `<'dict'>`: A dictionary containing the following column metadata:
        ```python
        - "CATALOG_NAME"
        - "SCHEMA_NAME"
        - "TABLE_NAME"
        - "COLUMN_NAME"
        - "COLUMN_TYPE"
        - "COLUMN_KEY"
        - "ORDINAL_POSITION"
        - "COLUMN_DEFAULT"
        - "IS_NULLABLE"
        - "DATA_TYPE"
        - "CHARACTER_MAXIMUM_LENGTH"
        - "CHARACTER_OCTET_LENGTH"
        - "NUMERIC_PRECISION"
        - "NUMERIC_SCALE"
        - "DATETIME_PRECISION"
        - "CHARACTER_SET_NAME"
        - "COLLATION_NAME"
        - "EXTRA"
        - "PRIVILEGES"
        - "COLUMN_COMMENT"
        - "GENERATION_EXPRESSION"
        - "SRS_ID"
        - "ENGINE_ATTRIBUTE"
        - "SECONDARY_ENGINE_ATTRIBUTE"
        - "COLUMN_INDEX_NAME"
        - "COLUMN_INDEX_NON_UNIQUE"
        - "COLUMN_INDEX_SEQ"
        - "COLUMN_INDEX_LENGTH"
        ```
        """
        super().__init__("COLUMN", meta, 28)
        try:
            # Base data
            self._db_name = meta["SCHEMA_NAME"]
            self._tb_name = meta["TABLE_NAME"]
            self._column_name = meta["COLUMN_NAME"]
            self._column_type = meta["COLUMN_TYPE"]
            self._position = meta["ORDINAL_POSITION"]
            self._default = meta["COLUMN_DEFAULT"]
            self._null = utils.validate_null(self._meta["IS_NULLABLE"])
            self._data_type = meta["DATA_TYPE"]
            self._character_maximum_length = meta["CHARACTER_MAXIMUM_LENGTH"]
            self._numeric_precision = meta["NUMERIC_PRECISION"]
            self._numeric_scale = meta["NUMERIC_SCALE"]
            self._datetime_precision = meta["DATETIME_PRECISION"]
            self._charset = utils.validate_charset(
                meta["CHARACTER_SET_NAME"], meta["COLLATION_NAME"]
            )
            self._extra = meta["EXTRA"]
            self._comment = utils.validate_comment(meta["COLUMN_COMMENT"])
            self._expression = utils.validate_expression(meta["GENERATION_EXPRESSION"])
            if self._expression is None:
                self._el_type = "COLUMN"
                self._virtual = -1
            else:
                self._el_type = "GENERATED COLUMN"
                if utils.validate_str_contains(self._extra, "VIRTUAL GENERATED"):
                    self._virtual = 1
                else:
                    self._virtual = 0
            # Additional data
            self._visible = not utils.validate_str_contains(self._extra, "INVISIBLE")
            # . integer
            self._auto_increment = utils.validate_str_contains(
                self._extra, "AUTO_INCREMENT"
            )
            self._unsigned = utils.validate_str_contains(self._column_type, "UNSIGNED")
            # . datetime
            self._auto_init = utils.validate_str_contains(
                self._extra, "DEFAULT_GENERATED"
            )
            self._auto_update = utils.validate_str_contains(
                self._extra, "ON UPDATE CURRENT_TIMESTAMP"
            )
            # . index
            self._column_index_name = meta["COLUMN_INDEX_NAME"]
            if self._column_index_name is None:
                self._unique_key = False
                self._primary_key = False
                self._indexed = False
                self._column_index_seq = -1
                self._column_index_length = -1
            else:
                self._unique_key = meta["COLUMN_INDEX_NON_UNIQUE"] == 0
                if self._unique_key:
                    self._primary_key = self._column_index_name == "PRIMARY"
                else:
                    self._primary_key = False
                self._indexed = True
                self._column_index_seq = meta["COLUMN_INDEX_SEQ"]
                self._column_index_length = meta["COLUMN_INDEX_LENGTH"]
        except Exception as err:
            self._raise_invalid_metadata_error(meta, err)

    # Property -----------------------------------------------------------------------------
    @property
    def catelog_name(self) -> str:
        """The catalog name of the column `<'str'>`."""
        return self._meta["CATALOG_NAME"]

    @property
    def db_name(self) -> str:
        """The schema name of the column `<'str'>`."""
        return self._db_name

    @property
    def tb_name(self) -> str:
        """The table name of the column `<'str'>`."""
        return self._tb_name

    @property
    def column_name(self) -> str:
        """The name of the column `<'str'>`."""
        return self._column_name

    @property
    def column_type(self) -> str:
        """The type of the column `<'str'>`."""
        return self._column_type

    @property
    def column_key(self) -> str | None:
        """Indicates whether the column is a primary key,
        unique key, or multiple key `<'str/None'>`.

        ## Explanation:
        - `'PRI'`: The column is (part of) the primary key of the table.
        - `'UNI'`: The column is (part of) a unique key of the table.
        - `'MUL'`: The column is (part of) a index of the table.
        - `''`: The column is not (part of) any kind of key of the table.
        """
        return self._meta["COLUMN_KEY"]

    @property
    def position(self) -> int:
        """The ordinal position of the column in the table `<'int'>`."""
        return self._position

    @property
    def default(self) -> str | None:
        """The default value of the column `<'str/None'>`."""
        return self._default

    @property
    def null(self) -> bool:
        """Whether the column can contain NULL values (nullable) `<'bool'>`."""
        return self._null

    @property
    def data_type(self) -> str:
        """The data type of the column `<'str'>`."""
        return self._data_type

    @property
    def character_maximum_length(self) -> int | None:
        """The maximum length of the character column `<'int/None'>`.

        ## Notice
        - Only relevant for character-based columns.
        """
        return self._character_maximum_length

    @property
    def character_octet_length(self) -> int | None:
        """The maximum length of the character column in bytes `<'int/None'>`.

        ## Notice
        - Only relevant for character-based columns.
        """
        return self._meta["CHARACTER_OCTET_LENGTH"]

    @property
    def numeric_precision(self) -> int | None:
        """The precision (maximum number of digits)
        for numeric data types `<'int/None'>`.

        ## Notice
        - Only relevant for numeric-based columns.
        """
        return self._numeric_precision

    @property
    def numeric_scale(self) -> int | None:
        """The scale (maximum number of digits to the right
        of the decimal point) for numeric data types `<'int/None'>`.

        ## Notice
        - Only relevant for numeric-based columns.
        """
        return self._numeric_scale

    @property
    def datetime_precision(self) -> int | None:
        """The fractional seconds precision for `TIME`, `DATETIME`,
        and `TIMESTAMP` columns `<'int/None'>`.

        ## Notice
        - Only relevant for datetime-based columns.
        """
        return self._datetime_precision

    @property
    def charset(self) -> Charset | None:
        """The character set of the column `<'Charset/None'>`.

        ## Notice
        - Only relevant for character-based columns.
        """
        return self._charset

    @property
    def extra(self) -> str:
        """Additional information about the column. An empty string
        means there are no extra attributes. `<'str'>`.
        """
        return self._extra

    @property
    def privileges(self) -> str:
        """The privileges of the column `<'str'>`."""
        return self._meta["PRIVILEGES"]

    @property
    def comment(self) -> str:
        """The COMMENT of the column `<'str'>`."""
        return self._comment

    @property
    def expression(self) -> str | None:
        """The formula or expression if the column is generated `<'str/None'>`.

        ## Notice
        - Only relevant for generated columns.
        """
        return self._expression

    @property
    def virtual(self) -> bool | None:
        """Whether the generated column is virtual `<'bool'>`.

        ## Notice
        - Only relevant for generated columns.
        """
        if self._virtual == -1:
            return None
        elif self._virtual == 1:
            return True
        else:
            return False

    @property
    def srs_id(self) -> int | None:
        """The spatial reference system identifier `<'int/None'>`."""
        return self._meta["SRS_ID"]

    @property
    def engine_attribute(self) -> str | None:
        """The JSON field introduced in later MySQL versions that can
        store custom engine-specific attributes `<'str/None'>`.
        """
        return self._meta["ENGINE_ATTRIBUTE"]

    @property
    def secondary_engine_attribute(self) -> str | None:
        """The secondary JSON field introduced in later MySQL versions
        that can store custom engine-specific attributes `<'str/None'>`.
        """
        return self._meta["SECONDARY_ENGINE_ATTRIBUTE"]

    @property
    def visible(self) -> bool:
        """Whether the column is visible to queries `<'bool'>`."""
        return self._visible

    # . integer
    @property
    def auto_increment(self) -> bool:
        """Whether the column is auto-incremented `<'bool'>`.

        ## Notice
        - Only relevant for integer-based columns.
        """
        return self._auto_increment

    @property
    def unsigned(self) -> bool:
        """Whether the column is unsigned `<'bool'>`.

        ## Notice
        - Only relevant for numeric-based columns.
        """
        return self._unsigned

    # . datetime
    @property
    def auto_init(self) -> bool:
        """Whether the column implements DEFAULT CURRENT_TIMESTAMP `<'bool'>`.

        ## Notice
        - Only relevant for datetime-based columns.
        """
        return self._auto_init

    @property
    def auto_update(self) -> bool:
        """Whether the column implements ON UPDATE CURRENT_TIMESTAMP `<'bool'>`.

        ## Notice
        - Only relevant for datetime-based columns.
        """
        return self._auto_update

    # . index
    @property
    def primary_key(self) -> bool:
        """Whether the column is (part of) the PRIMARY KEY `<'bool'>`."""
        return self._primary_key

    @property
    def unique_key(self) -> bool:
        """Whether the column is (part of) a UNIQUE KEY `<'bool'>`."""
        return self._unique_key

    @property
    def indexed(self) -> bool:
        """Whether the column is (part of) an INDEX `<'bool'>`."""
        return self._indexed

    @property
    def column_index_name(self) -> str | None:
        """The name of the index that the column is part of `<'str/None'>`.

        Returns `None` if the column is not part of any index.
        """
        return self._column_index_name

    @property
    def column_index_seq(self) -> int | None:
        """The sequence of the column in the index `<'int/None'>`.

        Returns `None` if the column is not part of any index.
        """
        return None if self._column_index_seq == -1 else self._column_index_seq

    @property
    def column_index_length(self) -> int | None:
        """The number of columns in the index that column is part of `<'int/None'>`.

        Returns `None` if the column is not part of any index.
        """
        return None if self._column_index_length == -1 else self._column_index_length


# Query ------------------------------------------------------------------------------------------------------
@cython.cclass
class ColumnQuery(Query):
    """Represents the column query to be executed."""

    _definition: Definition

    def __init__(self):
        """The column query to be executed."""
        super().__init__()
        self._definition = None
