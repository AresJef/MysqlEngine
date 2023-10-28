# cython: language_level=3
# cython: wraparound=False
# cython: boundscheck=False

# Cython imports
import cython
from cython.cimports import numpy as np  # type: ignore
from cython.cimports.cpython import datetime  # type: ignore
from cython.cimports.cpython.int import PyInt_Check as is_int  # type: ignore
from cython.cimports.cpython.float import PyFloat_Check as is_float  # type: ignore
from cython.cimports.cpython.bytes import PyBytes_Check as is_bytes  # type: ignore
from cython.cimports.cpython.bytes import PyBytes_GET_SIZE as bytes_len  # type: ignore
from cython.cimports.cpython.string import PyString_Check as is_str  # type: ignore
from cython.cimports.cpython.unicode import PyUnicode_GET_LENGTH as str_len  # type: ignore
from cython.cimports.cpython.bytearray import PyByteArray_Check as is_bytearray  # type: ignore
from cython.cimports.cytimes.pddt import pddt  # type: ignore
from cython.cimports.cytimes.pydt import pydt  # type: ignore
from cython.cimports.cytimes import cymath, cydatetime as cydt  # type: ignore
from cython.cimports.mysqlengine import errors, transcode, utils  # type: ignore

np.import_array()
datetime.import_datetime()

# Python imports
from typing import Any, Union
from decimal import Decimal
import datetime, numpy as np
from pandas import Series, Timestamp
from cytimes.pddt import pddt
from cytimes.pydt import pydt
from cytimes import cymath, cydatetime as cydt
from mysqlengine import errors, transcode, utils


__all__ = ["DataType", "MysqlTypes"]

# Constant ====================================================================================
DEFAULT_DATETIME: datetime.datetime = cydt.gen_dt(1970, 1, 1, 0, 0, 0, 0, None, 0)
INTEGER_UNSIGNED_MIN: cython.longlong = 0
TINYINT_SIGNED_MIN: cython.longlong = -128
TINYINT_SIGNED_MAX: cython.longlong = 127
TINYINT_UNSIGNED_MAX: cython.longlong = 255
SMALLINT_SIGNED_MIN: cython.longlong = -32768
SMALLINT_SIGNED_MAX: cython.longlong = 32767
SMALLINT_UNSIGNED_MAX: cython.longlong = 65535
MEDIUMINT_SIGNED_MIN: cython.longlong = -8388608
MEDIUMINT_SIGNED_MAX: cython.longlong = 8388607
MEDIUMINT_UNSIGNED_MAX: cython.longlong = 16777215
INT_SIGNED_MIN: cython.longlong = -2147483648
INT_SIGNED_MAX: cython.longlong = 2147483647
INT_UNSIGNED_MAX: cython.longlong = 4294967295
BIGINT_SIGNED_MIN: cython.longlong = -9223372036854775807
BIGINT_SIGNED_MAX: cython.longlong = 9223372036854775807
BIGINT_UNSIGNED_MAX: cython.longlong = 9223372036854775807
TINYTEXT_MAX: cython.longlong = 255
TEXT_MAX: cython.longlong = 65535
MEDIUMTEXT_MAX: cython.longlong = 16777215
LONGTEXT_MAX: cython.longlong = 4294967295


# Data type ===================================================================================
@cython.cclass
class DataType:
    """The base class for all MySQL data types."""

    _mysql: str
    _python: type
    _primary_key: cython.bint
    _tabletime: cython.bint
    _auto_increment: cython.bint
    _null: cython.bint
    _default: object
    _syntax: str

    def __init__(
        self,
        dtype_mysql: str,
        dtype_python: type,
        primary_key: cython.bint = False,
        auto_increment: cython.bint = False,
        null: cython.bint = False,
        default: object = None,
        tabletime: cython.bint = False,
    ) -> None:
        """Base class for all MySQL data types.

        :param dtype_mysql: `<str>` The str representation of the MySQL data type. e.g. `'TINYINT'`, `'VARCHAR'`, `'DATETIME'`, etc.
        :param dtype_python: `<str>` The corresponding Python data type. e.g. `int`, `float`, `str`, `datetime`, etc.
        :param primary_key: `<bool>` Whether is the primary key. Defaults to `False`.
        :param auto_increment: `<bool>` Whether is auto incremented. Defaults to `False`.
        :param null: `<bool>` Whether is nullable. Defaults to `False`.
        :param default: `<Any/None>` The default value of the column. Defaults to `None`.
        :param tabletime: `<bool>` Whether determines `TimeTable`'s tabletime. Defaults to `False`.
            - This is not an actual MySQL settings, and only affects built-in query
              methods within `Database` and `Table` classes.
            - All `TimeTable` require to have one column set to `tabletime=True`.
            - If `True`, this columns will be used to determine the sub-timetable
              when utilizing engine built-in query methods. Meanwhile, `null` will
              be set to `False` automatically.
            - If `False`, this column will be treated as a normal time column.
        """
        # data type
        self._mysql = dtype_mysql
        self._python = dtype_python
        # metadata
        self._primary_key = primary_key
        if self.primary_key:
            self._auto_increment = True
        else:
            self._auto_increment = auto_increment
        if self._auto_increment:
            self._null = False
        else:
            self._null = null
        self._default = default
        self._tabletime = tabletime
        # syntax
        self._syntax = None
        # setup
        self.__setup()

    @property
    def mysql(self) -> str:
        """The str representation of the MySQL data type `<str>`.
        e.g. `TINYINT`, `VARCHAR`, `DATETIME`, etc."""
        return self._mysql

    @property
    def python(self) -> type:
        """The matching Python native data type `<type>`.
        e.g. `int`, `float`, `str`, `datetime`, etc."""
        return self._python

    @property
    def primary_key(self) -> bool:
        "Whether is the primary key `<bool>`."
        return self._primary_key

    @property
    def auto_increment(self) -> bool:
        "Whether is auto incremented `<bool>`."
        return self._auto_increment

    @property
    def null(self) -> bool:
        "Whether is nullable `<bool>`."
        return self._null

    @property
    def default(self) -> Union[Any, None]:
        """The `DEFAULT` value of the column `<Any/None>`.
        Return `None` if default value not specified.
        """
        return self._default

    @property
    def tabletime(self) -> bool:
        """Whether determines `TimeTable`'s tabletime `<bool>`.
        Only applicable for DATE/DATETIME/TIMESTAMP data types.
        """
        return self._tabletime

    @property
    def syntax(self) -> str:
        "The `SQL` syntax of the data type `<str>`."
        return self._syntax

    @property
    def _null_syntax(self) -> str:
        "The `SQL` syntax of the `NULL` constraint `<str>`."
        return "NULL" if self._null else "NOT NULL"

    @property
    def _default_syntax(self) -> str:
        "The `SQL` syntax of the `DEFAULT` constraint `<str>`."
        if self._default is not None:
            return "DEFAULT " + transcode.encode_item(self._default, True)
        else:
            return ""

    # Setup ---------------------------------------------------------------
    def __setup(self) -> None:
        self._validation()
        self._construct_syntax()

    def _validation(self) -> None:
        raise NotImplementedError(
            f"<{self.__class__.__name__}.validation> Must be implemented in subclass."
        )

    def _construct_syntax(self) -> None:
        self._syntax = utils._str_squeeze_spaces(
            " ".join(
                [
                    self._mysql,
                    self._null_syntax,
                    self._default_syntax,
                ]
            ),
            True,
        )

    # Validater ----------------------------------------------------------
    def item_validator(self, v: Any) -> Any:
        "Validate item for the Column `<Any>`."
        return v

    def series_validator(self, s: Series) -> Series:
        "Validate Series for the Column `<Series>`."
        return s

    # Utils --------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    def convert_str(self, val: object) -> str:
        "(cfunc) Convert value to `<str>`."
        return val if is_str(val) else str(val)

    @cython.cfunc
    @cython.inline(True)
    def convert_int(self, val: object) -> cython.longlong:
        "(cfunc) Convert value to `<int>`."
        return val if is_int(val) else int(val)

    @cython.cfunc
    @cython.inline(True)
    def convert_float(self, val: object) -> cython.double:
        "(cfunc) Convert value to `<float>`."
        return val if is_float(val) else float(str(val))

    @cython.cfunc
    @cython.inline(True)
    def convert_bytes(self, val: object) -> bytes:
        "(cfunc) Convert value to `<bytes>`."
        if is_bytes(val):
            return val
        elif is_bytearray(val):
            return bytes(val)
        else:
            raise ValueError(f"{repr(val)} {type(val)} is not of type `<bytes>`.")

    # Special Methods ------------------------------------------------------
    def __repr__(self) -> str:
        return "<%s (mysql='%s', python=%s, syntax='%s')>" % (
            self.__class__.__name__,
            self._mysql,
            self._python.__name__,
            self._syntax,
        )

    def __str__(self) -> str:
        return self._syntax

    def __hash__(self) -> int:
        return hash(self._syntax)

    def __eq__(self, __o: object) -> bool:
        return self._syntax == __o.syntax if isinstance(__o, type(self)) else False

    def __bool__(self) -> bool:
        return True

    def __del__(self):
        self._python = None
        self._default = None


@cython.cclass
class _Integer(DataType):
    """The base class for all MySQL `INTEGER` data types."""

    _signed: cython.bint
    _max: cython.longlong
    _min: cython.longlong

    def __init__(
        self,
        dtype_mysql: str,
        min: cython.longlong,
        max: cython.longlong,
        primary_key: cython.bint = False,
        auto_increment: cython.bint = False,
        signed: cython.bint = False,
        null: cython.bint = False,
        default: Union[int, None] = None,
    ) -> None:
        """Base class for all MySQL `INTEGER` data types.

        :param dtype_mysql: `<str>` The str representation of the MySQL data type. e.g. `'TINYINT'`, `'BIGINT'`, etc.
        :param min: `<int>` The minimum integer value of the column (if applicable).
        :param max: `<int>` The maximum integer value of the column (if applicable).
        :param primary_key: `<bool>` Whether is the primary key. Defaults to `False`.
        :param auto_increment: `<bool>` Whether is auto incremented. Defaults to `False`.
        :param signed: `<bool>` Whether is signed (if applicable). Defaults to `False`.
        :param null: `<bool>` Whether is nullable. Defaults to `False`.
        :param default: `<int/None>` The default value of the column. Defaults to `None`.
        """
        self._signed = signed
        self._max = max
        self._min = min
        super().__init__(
            dtype_mysql,
            int,
            primary_key=primary_key,
            auto_increment=auto_increment,
            null=null,
            default=default,
            tabletime=False,
        )

    @property
    def default(self) -> Union[int, None]:
        """The `DEFAULT` value of the column `<int>`.
        Return `None` if default value not specified.
        """
        return self._default

    @property
    def signed(self) -> bool:
        "Whether is signed `<bool>`."
        return self._signed

    @property
    def min(self) -> int:
        "Minimum integer value for the column `<int>`."
        return self._min

    @property
    def max(self) -> int:
        "Maximum integer value for the column `<int>`."
        return self._max

    @property
    def _signed_syntax(self) -> str:
        "The `SQL` syntax of the `SIGNED` constraint `<str>`."
        return "SIGNED" if self._signed else "UNSIGNED"

    # Setup ---------------------------------------------------------------
    def _validation(self) -> None:
        # Validate default
        if self._default is not None:
            try:
                self._default = self.convert_int(self._default)
            except Exception as err:
                raise errors.DataTypeMetadataError(
                    "<{}.metadata> Failed to covnert default value to `<int>`: {} {}".format(
                        self.__class__.__name__,
                        repr(self._default),
                        type(self._default),
                    )
                ) from err
            if not self._min <= self._default <= self._max:
                raise errors.DataTypeMetadataError(
                    "<{}.metadata> Default value must between {} and {}, instead of: {}".format(
                        self.__class__.__name__,
                        self._min,
                        self._max,
                        self._default,
                    )
                )

    def _construct_syntax(self) -> None:
        if self._primary_key:
            self._syntax = (
                "%s UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY" % self._mysql
            )
        elif self._auto_increment:
            self._syntax = "%s UNSIGNED NOT NULL AUTO_INCREMENT" % self._mysql
        else:
            self._syntax = utils._str_squeeze_spaces(
                " ".join(
                    [
                        self._mysql,
                        self._signed_syntax,
                        self._null_syntax,
                        self._default_syntax,
                    ]
                ),
                True,
            )

    # Validater ----------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    def _item_validator(self, v: object) -> cython.longlong:
        "(cfunc) Validate item for the Column `<int>`."
        try:
            val: cython.longlong = self.convert_int(v)
        except Exception as err:
            raise errors.QueryDataValidationError(
                f"`{self._mysql}` Can't validate `item` value: {repr(v)} {type(v)}"
            ) from err
        if not self._min <= val <= self._max:
            raise errors.QueryDataValidationError(
                "`{}` Value {} is requried to fall between {} and {}.".format(
                    self._mysql, val, self._min, self._max
                )
            )
        return val

    def item_validator(self, v: Any) -> int:
        "Validate item for the Column `<int>`."
        return self._item_validator(v)

    @cython.cfunc
    @cython.inline(True)
    def _series_validator(self, s: Series) -> object:
        "(cfunc) Validate Series for the Column `<Series[int]>`."
        kind: str = s.dtype.kind
        arr: np.ndarray = s.values
        if kind == "i":
            if (np.max(arr) > self._max) or (np.min(arr) < self._min):
                raise errors.QueryDataValidationError(
                    "`{}` Value is requried to fall between {} and {}.".format(
                        self._mysql, self._min, self._max
                    )
                )
            return s
        else:
            try:
                arr = arr.astype(np.int64)
            except Exception as err:
                raise errors.QueryDataValidationError(
                    f"`{self._mysql}` Can't validate `Series` value: {err}"
                )
            if (np.max(arr) > self._max) or (np.min(arr) < self._min):
                raise errors.QueryDataValidationError(
                    "`{}` Value is requried to fall between {} and {}.".format(
                        self._mysql, self._min, self._max
                    )
                )
            return Series(arr, s.index, name=s.name)

    def series_validator(self, s: Series) -> Series[int]:
        "Validate Series for the Column `<Series[int]>`."
        return self._series_validator(s)


@cython.cclass
class _FloatingPoint(DataType):
    """The base class for all MySQL `FLOATING-POINT` data type."""

    _precision: cython.int
    _max_preceision: cython.int

    def __init__(
        self,
        dtype_mysql: str,
        precision: Union[int, None],
        max_precision: cython.int,
        null: cython.bint = False,
        default: Union[float, None] = None,
    ) -> None:
        """Base class for all MySQL `FLOATING-POINT` data type.

        :param dtype_mysql: `<str>` The str representation of the MySQL data type. e.g. `'FLOAT'`, `'DOUBLE'`, etc.
        :param precision: `<int/None>` The precision of the floating-point column.
            - A precision from 0 to 23 (and `None`) results in a four-byte single-precision FLOAT column.
            - A precision from 24 to 53 results in an eight-byte double-precision DOUBLE column.

        :param max_precision: `<int>` The maximum precision of the float-point.
        :param null: `<bool>` Whether is nullable. Defaults to `False`.
        :param default: `<float/None>` The default value of the column. Defaults to `None`.
        """
        self._precision = precision if is_int(precision) else -1
        self._max_preceision = max_precision
        super().__init__(
            dtype_mysql,
            float,
            primary_key=False,
            auto_increment=False,
            null=null,
            default=default,
            tabletime=False,
        )

    @property
    def default(self) -> Union[float, None]:
        """The `DEFAULT` value of the column `<float>`.
        Return `None` if default value not specified.
        """
        return self._default

    @property
    def precision(self) -> int:
        "The precision of the FLOAT column `<float>`."
        return self._precision

    # Setup ---------------------------------------------------------------
    def _validation(self) -> None:
        # Validate max precision
        if self._max_preceision != 23 and self._max_preceision != 53:
            raise errors.DataTypeMetadataError(
                "<{}.metadata> Max precision must be either 23 or 53 for MySQL "
                "'FLOATING-POINT' DATATYPE.".format(self.__class__.__name__)
            )

        # Validate precision
        if self._precision != -1 and not 0 <= self._precision <= self._max_preceision:
            raise errors.DataTypeMetadataError(
                "<{}.metadata> Precision must between 0 and {} for MySQL '{}' DATATYPE.".format(
                    self.__class__.__name__, self._max_preceision, self._mysql
                )
            )

        # Validate default
        if self._default is not None:
            try:
                self._default = self.convert_float(self._default)
            except Exception as err:
                raise errors.DataTypeMetadataError(
                    "<{}.metadata> Failed to covnert default value to `<float>`: {} {}".format(
                        self.__class__.__name__,
                        repr(self._default),
                        type(self._default),
                    )
                ) from err
            if not cymath.is_finite(self._default):
                raise errors.DataTypeMetadataError(
                    "<{}.metadata> Default value must be finite, instead of: {}".format(
                        self.__class__.__name__, self._default
                    )
                )

    def _construct_syntax(self) -> None:
        self._syntax = utils._str_squeeze_spaces(
            " ".join(
                [
                    "%s(%s)" % (self._mysql, self._precision)
                    if self._precision != -1
                    else self._mysql,
                    self._null_syntax,
                    self._default_syntax,
                ]
            ),
            True,
        )

    # Validator -----------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    def _item_validator(self, v: object) -> cython.double:
        "(cfunc) Validate item for the Column `<float>`."
        try:
            val: cython.double = self.convert_float(v)
        except Exception as err:
            raise errors.QueryDataValidationError(
                f"`{self._mysql}` Can't validate `item` value: {repr(v)} {type(v)}"
            ) from err
        if not cymath.is_finite(val):
            raise errors.QueryDataValidationError(
                "`{}` Value '{}' cannot be infinite".format(self._mysql, val)
            )
        return val

    def item_validator(self, v: Any) -> float:
        "Validate item for the Column `<float>`."
        return self._item_validator(v)

    @cython.cfunc
    @cython.inline(True)
    def _series_validator(self, s: Series) -> object:
        "(cfunc) Validate Series for the Column `<Series[float]>`."
        kind: str = s.dtype.kind
        if kind == "f":
            return s
        try:
            return s.astype(np.float64)
        except Exception as err:
            raise errors.QueryDataValidationError(
                f"`{self._mysql}` Can't validate `Series` value: {err}"
            )

    def series_validator(self, s: Series) -> Series[float]:
        "Validate Series for the Column `<Series[float]>`."
        return self._series_validator(s)


@cython.cclass
class _FixedPoint(DataType):
    """The base class for all MySQL `FIXED-POINT` data type."""

    _precision: cython.int
    _scale: cython.int
    _factor: cython.longlong
    _min: Decimal
    _min_float: cython.double
    _max: Decimal
    _max_float: cython.double

    def __init__(
        self,
        dtype_mysql: str,
        precision: cython.int,
        scale: cython.int,
        null: cython.bint = False,
        default: Union[str, int, float, Decimal, None] = None,
    ) -> None:
        """Base class for all MySQL `FIXED-POINT` data type.

        :param dtype_mysql: `<str>` The str representation of the MySQL data type. e.g. `'DECIMAL'`, etc.
        :param precision: `<int>` The precision of the decimal, represents the total number of significant digits.
        :param scale: `<int>` The scale of the decimal, represents the number of digits after the decimal point.
        :param null: `<bool>` Whether is nullable. Defaults to `False`.
        :param default: `<str/int/float/Decimal/None>` The default value of the column. Defaults to `None`.
        """
        self._precision = precision
        self._scale = scale
        self._factor: int = 10
        super().__init__(
            dtype_mysql,
            Decimal,
            primary_key=False,
            auto_increment=False,
            null=null,
            default=default,
            tabletime=False,
        )

    @property
    def default(self) -> Union[Decimal, None]:
        """The `DEFAULT` value of the column `<Decimal>`.
        Return `None` if default value not specified.
        """
        return self._default

    @property
    def default_float(self) -> Union[float, None]:
        """The `DEFAULT` value of the column, access in `<float>`.
        Return `None` if default value not specified.
        """
        return None if self._default is None else float(self._default)

    @property
    def precision(self) -> int:
        """The precision of the decimal, represents the
        total number of significant digits `<int>`."""
        return self._precision

    @property
    def scale(self) -> int:
        """The scale of the decimal, represents the number
        of digits after the decimal point `<int>`."""
        return self._scale

    @property
    def min(self) -> Decimal:
        "The minimum value of the column `<Decimal>`."
        return self._min

    @property
    def min_float(self) -> float:
        "The minimum value of the column, access in `<float>`."
        return self._min_float

    @property
    def max(self) -> Decimal:
        "The maximum value of the column `<Decimal>`."
        return self._max

    @property
    def max_float(self) -> float:
        "The maximum value of the column, access in `<float>`."
        return self._max_float

    # Setup ---------------------------------------------------------------
    def _validation(self):
        # Validate precision
        if not 1 <= self._precision <= 65:
            raise errors.DataTypeMetadataError(
                "<{}.metadata> Precision must between 1 and 65 for MySQL '{}' "
                "DATATYPE.".format(self.__class__.__name__, self._mysql)
            )
        if not 0 <= self._scale <= 30 or self._scale > self._precision:
            raise errors.DataTypeMetadataError(
                "<{}.metadata> Scaler must between 0 and {} for MySQL '{}' DATATYPE.".format(
                    self.__class__.__name__, min(30, self._precision), self._mysql
                )
            )

        # Construct min & max
        limit: str = f"{'9'*(self._precision-self._scale)}.{'9'*self._scale}"
        self._min = Decimal("-" + limit)
        self._min_float = float(self._min)
        self._max = Decimal(limit)
        self._max_float = float(self._max)

        # Calculate rounding factor
        self._factor = int(10**self._scale)

        # Validate default
        if self._default is not None:
            if not isinstance(self._default, Decimal):
                try:
                    self._default = Decimal(str(self._default))
                except Exception as err:
                    raise errors.DataTypeMetadataError(
                        "<{}.metadata> Failed to convert default value to `<Decimal>`: {} {}".format(
                            self.__class__.__name__,
                            repr(self._default),
                            type(self._default),
                        )
                    ) from err
            if not self._default.is_finite():
                raise errors.DataTypeMetadataError(
                    "<{}.metadata> Default value must be finite, instead of: {} {}".format(
                        self.__class__.__name__,
                        repr(self._default),
                        type(self._default),
                    )
                )
            self._default = round(self._default, self._scale)
            if not self._min <= self._default <= self._max:
                raise errors.DataTypeMetadataError(
                    "<{}.metadata> Default value must between {} and {}, instead of: {}".format(
                        self.__class__.__name__,
                        self._min,
                        self._max,
                        self._default,
                    )
                )

    def _construct_syntax(self) -> None:
        self._syntax = utils._str_squeeze_spaces(
            " ".join(
                [
                    "%s(%s, %s)" % (self._mysql, self._precision, self._scale),
                    self._null_syntax,
                    self._default_syntax,
                ]
            ),
            True,
        )

    # Validator -----------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    def _item_validator(self, v: object) -> cython.double:
        "(cfunc) Validate item for the Column `<float>`."
        try:
            val: cython.double = utils._round_half_away_factor(
                self.convert_float(v), self._factor
            )
        except Exception as err:
            raise errors.QueryDataValidationError(
                f"`{self._mysql}` Can't validate `item` value: {repr(v)} {type(v)}"
            ) from err
        if not self._min_float <= val <= self._max_float:
            raise errors.QueryDataValidationError(
                "`{}` Value {} is requried to fall between "
                "{} and {}.".format(self._mysql, val, self._min, self._max)
            )
        return val

    def item_validator(self, v: Any) -> float:
        "Validate item for the Column `<float>`."
        return self._item_validator(v)

    @cython.cfunc
    @cython.inline(True)
    def _series_validator(self, s: Series) -> object:
        "(cfunc) Validate Series for the Column `<Series[float]>`."
        kind: str = s.dtype.kind
        arr: np.ndarray = s.values
        if kind != "f":
            try:
                arr = arr.astype(np.float64)
            except Exception as err:
                raise errors.QueryDataValidationError(
                    f"`{self._mysql}` Can't validate `Series` value: {err}"
                )
        adj = np.where(arr >= 0, 0.5, -0.5)
        arr = (arr * self._factor + adj).astype(np.int64) / self._factor
        if (np.max(arr) > self._max_float) or (np.min(arr) < self._min_float):
            raise errors.QueryDataValidationError(
                "`{}` Value is requried to fall between {} and {}.".format(
                    self._mysql, self._min, self._max
                )
            )
        return Series(arr, s.index, name=s.name)

    def series_validator(self, s: Series) -> Series[float]:
        "Validate Series for the Column `<Series[float]>`."
        return self._series_validator(s)


@cython.cclass
class _Date(DataType):
    """The base class for all MySQL `DATE` data type."""

    _format: str
    _default_str: str

    def __init__(
        self,
        dtype_mysql: str,
        null: cython.bint = False,
        default: Union[str, datetime.date, datetime.datetime, None] = None,
        tabletime: cython.bint = False,
    ) -> None:
        """Base class for all MySQL `DATE` data type.

        :param dtype_mysql: `<str>` The str representation of the MySQL data type. e.g. `'DATE'`, etc.
        :param null: `<bool>` Whether is nullable. Defaults to `False`.
        :param default: `<str/date/datetime/None>` The default value of the column. Defaults to `None`.
        :param tabletime: `<bool>` Whether determines `TimeTable`'s tabletime. Defaults to `False`.
            - This is not an actual MySQL settings, and only affects built-in query
              methods within `Database` and `Table` classes.
            - All `TimeTable` require to have one column set to `tabletime=True`.
            - If `True`, this columns will be used to determine the sub-timetable
              when utilizing engine built-in query methods. Meanwhile, `null` will
              be set to `False` automatically.
            - If `False`, this column will be treated as a normal time column.
        """
        self._format = "%Y-%m-%d"
        self._default_str = None
        super().__init__(
            dtype_mysql,
            datetime.date,
            primary_key=False,
            auto_increment=False,
            null=null,
            default=self._pre_validation(default),
            tabletime=tabletime,
        )

    @property
    def default(self) -> Union[datetime.date, None]:
        """The `DEFAULT` value of the column `<datetime.date>`.
        Return `None` if default value not specified.
        """
        return self._default

    @property
    def default_str(self) -> Union[str, None]:
        """The `DEFAULT` value of the column, access in `<str>`.
        Return `None` if default value not specified.
        """
        return self._default_str

    @property
    def format(self) -> str:
        "The correspinding date format `<str>`. e.g. `'%Y-%m-%d'`."
        return self._format

    # Setup ---------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    def _pre_validation(self, default: object) -> datetime.date:
        if default is None:
            return None
        try:
            return pydt(default, default=DEFAULT_DATETIME, ignoretz=True)._date()
        except Exception as err:
            raise errors.DataTypeMetadataError(
                "<{}.metadata> Invalid default value {} for MySQL 'DATE' "
                "DATATYPE.".format(self.__class__.__name__, repr(default))
            ) from err

    def _validation(self) -> None:
        # Generate default str
        if self._default is not None:
            self._default_str = self._default.strftime(self._format)

        # Adjust nullable
        if self._tabletime:
            self._null = False

    # Validator -----------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    def _item_validator(self, v: object) -> object:
        "(cfunc) Validate item for the Column `<datetime.date>`."
        if datetime.PyDate_CheckExact(v):
            return v
        try:
            return pydt(v, default=self._default, ignoretz=True)._date()
        except Exception as err:
            raise errors.QueryDataValidationError(
                f"`{self._mysql}` Can't validate `item` value: {repr(v)} {type(v)}"
            ) from err

    def item_validator(self, v: Any) -> datetime.date:
        "Validate item for the Column `<datetime.date>`."
        return self._item_validator(v)

    @cython.cfunc
    @cython.inline(True)
    def _series_validator(self, s: Series) -> object:
        "(cfunc) Validate Series for the Column `<Series[datetime.date]>`."
        kind: str = s.dtype.kind
        if kind == "M":
            return s.dt.date
        try:
            return pddt(s).date
        except Exception:
            return s.apply(self._item_validator)

    def series_validator(self, s: Series) -> Series[datetime.date]:
        "Validate Series for the Column `<Series[datetime.date]>`."
        return self._series_validator(s)


@cython.cclass
class _Datetime(DataType):
    """The base class for all MySQL `DATETIME` data type."""

    _format: str
    _fraction: cython.int
    _auto_init: cython.bint
    _auto_update: cython.bint
    _default_str: str

    def __init__(
        self,
        dtype_mysql: str,
        fraction: cython.int,
        auto_init: cython.bint = False,
        auto_update: cython.bint = False,
        null: cython.bint = False,
        default: Union[str, datetime.date, datetime.datetime, None] = None,
        tabletime: cython.bint = False,
    ) -> None:
        """Base class for all MySQL `DATETIME` data type.

        :param dtype_mysql: `<str>` The str representation of the MySQL data type. e.g. `'DATETIME'`, etc.
        :param fraction: `<int>` Define the fractional seconds precision.
        :param auto_init: `<bool>` Whether enable auto initiation with current timestamp. Defaults to `False`.
        :param auto_update: `<bool>` Whether enable auto update with current timestamp. Defaults to `False`.
        :param null: `<bool>` Whether is nullable. Defaults to `False`.
        :param default: `<str/date/datetime/None>` The default value of the column. Defaults to `None`.
        :param tabletime: `<bool>` Whether determines `TimeTable`'s tabletime. Defaults to `False`.
            - This is not an actual MySQL settings, and only affects built-in query
              methods within `Database` and `Table` classes.
            - All `TimeTable` require to have one column set to `tabletime=True`.
            - If `True`, this columns will be used to determine the sub-timetable
              when utilizing engine built-in query methods. Meanwhile, `null` will
              be set to `False` automatically.
            - If `False`, this column will be treated as a normal time column.
        """
        self._format = "%Y-%m-%d %H:%M:%S"
        self._fraction = fraction
        self._auto_init = auto_init
        self._auto_update = auto_update
        self._default_str = None
        super().__init__(
            dtype_mysql,
            datetime.datetime,
            primary_key=False,
            auto_increment=False,
            null=null,
            default=self._pre_validation(default),
            tabletime=tabletime,
        )

    @property
    def default(self) -> Union[datetime.datetime, None]:
        """The `DEFAULT` value of the column `<datetime.datetime>`.
        Return `None` if default value not specified.
        """
        return self._default

    @property
    def default_str(self) -> Union[str, None]:
        """The `DEFAULT` value of the column, access in `<str>`.
        Return `None` if default value not specified.
        """
        return self._default_str

    @property
    def format(self) -> str:
        "The correspinding datetime format `<str>`. e.g. `'%Y-%m-%d %H:%M:%S'`."
        return self._format

    @property
    def fraction(self) -> int:
        "The fractional seconds precision `<int>`."
        return self._fraction

    @property
    def auto_init(self) -> bool:
        "Whether enable auto initiation with current timestamp."
        return self._auto_init

    @property
    def auto_update(self) -> bool:
        "Whether enable auto update with current timestamp."
        return self._auto_update

    @property
    def _auto_init_syntax(self) -> str:
        "The `SQL` syntax of the `DEFAULT CURRENT_TIMESTAMP` constraint `<str>`."
        return "DEFAULT CURRENT_TIMESTAMP" if self._auto_init else ""

    @property
    def _auto_update_syntax(self) -> str:
        "The `SQL` syntax of the `ON UPDATE CURRENT_TIMESTAMP` constraint `<str>`."
        return "ON UPDATE CURRENT_TIMESTAMP" if self._auto_update else ""

    @property
    def _default_syntax(self) -> str:
        "The `SQL` syntax of the `DEFAULT` constraint `<str>`."
        if self._auto_init:
            return self._auto_init_syntax
        elif self._default is not None:
            return "DEFAULT %s" % transcode.encode_item(self.default_str, True)
        else:
            return ""

    # Setup ---------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    def _pre_validation(self, default: object) -> datetime.datetime:
        if default is None:
            return None
        try:
            return pydt(default, default=DEFAULT_DATETIME, ignoretz=True)._dt
        except Exception as err:
            raise errors.DataTypeMetadataError(
                "<{}.metadata> Invalid default value {} for MySQL 'DATETIME' "
                "DATATYPE.".format(self.__class__.__name__, repr(default))
            ) from err

    def _validation(self) -> None:
        # Validate fractional seconds precision
        if not 0 <= self._fraction <= 6:
            raise errors.DataTypeMetadataError(
                "<{}.metadata> Fractional seconds precision must between 0 and 6 for "
                "MySQL '{}' DATATYPE.".format(self.__class__.__name__, self._mysql)
            )

        # Adjust format
        if self._fraction > 0:
            self._format = self._format + ".%f"

        # Generate default str
        if self._default is not None:
            self._default_str = self._default.strftime(self._format)
            if self._fraction == 0:
                self._default = self._default.replace(microsecond=0)

        # Adjust nullable
        if self._tabletime:
            self._null = False

    def _construct_syntax(self) -> None:
        self._syntax = utils._str_squeeze_spaces(
            " ".join(
                [
                    self._mysql
                    if self._fraction == 0
                    else "%s(%d)" % (self._mysql, self._fraction),
                    self._null_syntax,
                    self._default_syntax,
                    self._auto_update_syntax,
                ]
            ),
            True,
        )

    # Validator -----------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    def _item_validator(self, v: object) -> object:
        "(cfunc) Validate item for the Column `<datetime.datetime>`."
        if datetime.PyDateTime_Check(v):
            if datetime.PyDateTime_CheckExact(v):
                return v
            else:
                return cydt.dt_fr_dt(v)
        try:
            return pydt(v, default=self._default, ignoretz=True)._dt
        except Exception:
            raise errors.QueryDataValidationError(
                f"`{self._mysql}` Can't validate `item` value: {repr(v)} {type(v)}"
            )

    def item_validator(self, v: Any) -> datetime.datetime:
        "Validate item for the Column `<datetime.datetime>`."
        return self._item_validator(v)

    @cython.cfunc
    @cython.inline(True)
    def _series_validator(self, s: Series) -> object:
        "(cfunc) Validate Series for the Column `<Series[Timestamp]>`."
        kind: str = s.dtype.kind
        if kind == "M":
            return s
        try:
            return pddt(s).dt
        except Exception:
            return s.apply(self._item_validator)

    def series_validator(self, s: Series) -> Series[Timestamp]:
        "Validate Series for the Column `<Series[Timestamp]>`."
        return self._series_validator(s)


@cython.cclass
class _Timestamp(DataType):
    """The base class for all MySQL `TIMESTAMP` data type."""

    _format: str
    _fraction: cython.int
    _auto_init: cython.bint
    _auto_update: cython.bint

    def __init__(
        self,
        dtype_mysql: str,
        fraction: cython.int,
        auto_init: cython.bint = True,
        auto_update: cython.bint = False,
        null: cython.bint = False,
        tabletime: cython.bint = False,
    ) -> None:
        """Base class for all MySQL `TIMESTAMP` data type.

        :param dtype_mysql: `<str>` The str representation of the MySQL data type. e.g. `'TIMESTAMP'`, etc.
        :param fraction: `<int>` Define the fractional seconds precision.
        :param auto_init: `<bool>` Whether enable auto initiation with current timestamp. Defaults to `True`.
        :param auto_update: `<bool>` Whether enable auto update with current timestamp. Defaults to `False`.
        :param null: `<bool>` Whether is nullable. Defaults to `False`.
        :param tabletime: `<bool>` Whether determines `TimeTable`'s tabletime. Defaults to `False`.
            - This is not an actual MySQL settings, and only affects built-in query
              methods within `Database` and `Table` classes.
            - All `TimeTable` require to have one column set to `tabletime=True`.
            - If `True`, this columns will be used to determine the sub-timetable
              when utilizing engine built-in query methods. Meanwhile, `null` will
              be set to `False` automatically.
            - If `False`, this column will be treated as a normal time column.
        """
        self._format = "%Y-%m-%d %H:%M:%S"
        self._fraction = fraction
        self._auto_init = auto_init
        self._auto_update = auto_update
        super().__init__(
            dtype_mysql,
            datetime.datetime,
            primary_key=False,
            auto_increment=False,
            null=null,
            default=None,
            tabletime=tabletime,
        )

    @property
    def format(self) -> str:
        "The correspinding timestamp format `<str>`. e.g. `'%Y-%m-%d %H:%M:%S'`."
        return self._format

    @property
    def fraction(self) -> int:
        "The fractional seconds precision `<int>`."
        return self._fraction

    @property
    def auto_init(self) -> bool:
        "Whether enable auto initiation with current timestamp."
        return self._auto_init

    @property
    def auto_update(self) -> bool:
        "Whether enable auto update with current timestamp."
        return self._auto_update

    @property
    def _auto_init_syntax(self) -> str:
        "The `SQL` syntax of the `DEFAULT CURRENT_TIMESTAMP` constraint `<str>`."
        return "DEFAULT CURRENT_TIMESTAMP" if self._auto_init else ""

    @property
    def _auto_update_syntax(self) -> str:
        "The `SQL` syntax of the `ON UPDATE CURRENT_TIMESTAMP` constraint `<str>`."
        return "ON UPDATE CURRENT_TIMESTAMP" if self._auto_update else ""

    @property
    def _default_syntax(self) -> str:
        "The `SQL` syntax of the `DEFAULT` constraint `<str>`."
        if self._auto_init:
            return self._auto_init_syntax
        else:
            return ""

    # Setup ---------------------------------------------------------------
    def _validation(self) -> None:
        # Validate microseconds
        if not 0 <= self._fraction <= 6:
            raise errors.DataTypeMetadataError(
                "<{}.metadata> Fractional seconds precision must between 0 and 6 for "
                "MySQL '{}' DATATYPE.".format(self.__class__.__name__, self._mysql)
            )

        # Adjust format
        if self._fraction > 0:
            self._format = self._format + ".%f"

        # Adjust nullable
        if self._tabletime:
            self._null = False

    def _construct_syntax(self) -> None:
        self._syntax = utils._str_squeeze_spaces(
            " ".join(
                [
                    self._mysql
                    if self._fraction == 0
                    else "%s(%s)" % (self._mysql, self._fraction),
                    self._null_syntax,
                    self._default_syntax,
                    self._auto_update_syntax,
                ]
            ),
            True,
        )

    # Validator -----------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    def _item_validator(self, v: object) -> object:
        "(cfunc) Validate item for the Column `<datetime.datetime>`."
        if datetime.PyDateTime_Check(v):
            if datetime.PyDateTime_CheckExact(v):
                return v
            else:
                return cydt.dt_fr_dt(v)
        try:
            return pydt(v, default=DEFAULT_DATETIME, ignoretz=True)._dt
        except Exception:
            raise errors.QueryDataValidationError(
                f"`{self._mysql}` Can't validate `item` value: {repr(v)} {type(v)}"
            )

    def item_validator(self, v: Any) -> datetime.datetime:
        "Validate item for the Column `<datetime.datetime>`."
        return self._item_validator(v)

    @cython.cfunc
    @cython.inline(True)
    def _series_validator(self, s: Series) -> object:
        "(cfunc) Validate Series for the Column `<Series[Timestamp]>`."
        kind: str = s.dtype.kind
        if kind == "M":
            return s
        try:
            return pddt(s).dt
        except Exception:
            return s.apply(self._item_validator)

    def series_validator(self, s: Series) -> Series[Timestamp]:
        "Validate Series for the Column `<Series[Timestamp]>`."
        return self._series_validator(s)


@cython.cclass
class _Time(DataType):
    """The base class for all MySQL `TIME` data type."""

    _format: str
    _fraction: cython.int
    _default_str: str

    def __init__(
        self,
        dtype_mysql: str,
        fraction: cython.int = 0,
        null: cython.bint = False,
        default: Union[str, datetime.time, datetime.datetime, None] = None,
    ) -> None:
        """Base class for all MySQL `TIME` data type.

        :param dtype_mysql: `<str>` The str representation of the MySQL data type. e.g. `'TIME'`, etc.
        :param fraction: `<int>` Define the fractional seconds precision.
        :param null: `<bool>` Whether is nullable. Defaults to `False`.
        :param default: `<str/time/datetime/None>` The default value of the column. Defaults to `None`.
        """
        self._format = "%H:%M:%S"
        self._fraction = fraction
        self._default_str = None
        super().__init__(
            dtype_mysql,
            datetime.time,
            primary_key=False,
            auto_increment=False,
            null=null,
            default=self._pre_validation(default),
            tabletime=False,
        )

    @property
    def default(self) -> Union[datetime.time, None]:
        """The `DEFAULT` value of the column `<datetime.time>`.
        Return `None` if default value not specified.
        """
        return self._default

    @property
    def default_str(self) -> Union[str, None]:
        """The `DEFAULT` value of the column, access in `<str>`.
        Return `None` if default value not specified.
        """
        return self._default_str

    @property
    def format(self) -> str:
        "The correspinding time format `<str>`. e.g. `'%H:%M:%S'`."
        return self._format

    @property
    def fraction(self) -> int:
        "The fractional seconds precision `<int>`."
        return self._fraction

    # Setup ---------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    def _pre_validation(self, default: object) -> datetime.time:
        if default is None:
            return None
        try:
            return pydt(default, ignoretz=True)._time()
        except Exception as err:
            raise errors.DataTypeMetadataError(
                "<{}.metadata> Invalid default value {} for MySQL 'TIME' "
                "DATATYPE.".format(self.__class__.__name__, repr(default))
            ) from err

    def _validation(self) -> None:
        # Validate microseconds
        if not 0 <= self._fraction <= 6:
            raise errors.DataTypeMetadataError(
                "<{}.metadata> Fractional seconds precision must between 0 and 6 for "
                "MySQL '{}' DATATYPE.".format(self.__class__.__name__, self._mysql)
            )

        # Adjust format
        if self._fraction > 0:
            self._format = self._format + ".%f"

        # Generate default str
        if self._default is not None:
            self._default_str = self._default.strftime(self._format)
            if self._fraction == 0:
                self._default = self._default.replace(microsecond=0)

    def _construct_syntax(self) -> None:
        self._syntax = utils._str_squeeze_spaces(
            " ".join(
                [
                    self._mysql
                    if self._fraction == 0
                    else "%s(%d)" % (self._mysql, self._fraction),
                    self._null_syntax,
                    self._default_syntax,
                ]
            ),
            True,
        )

    # Validator -----------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    def _item_validator(self, v: object) -> object:
        "(cfunc) Validate item for the Column `<datetime.time>`."
        if datetime.PyDelta_Check(v):
            if datetime.PyDelta_CheckExact(v):
                return v
            else:
                return cydt.delta_fr_delta(v)
        if datetime.PyTime_CheckExact(v):
            return v
        try:
            return pydt(v, ignoretz=True)._time()
        except Exception:
            raise errors.QueryDataValidationError(
                f"`{self._mysql}` Can't validate `item` value: {repr(v)} {type(v)}"
            )

    def item_validator(self, v: Any) -> datetime.time:
        "Validate item for the Column `<datetime.time>`."
        return self._item_validator(v)

    @cython.cfunc
    @cython.inline(True)
    def _series_validator(self, s: Series) -> object:
        "(cfunc) Validate Series for the Column `<Series[datetime.time]>`."
        kind: str = s.dtype.kind
        if kind == "m":
            return s
        elif kind == "M":
            return s.dt.time
        else:
            return s.apply(self._item_validator)

    def series_validator(self, s: Series) -> Series[datetime.time]:
        "Validate Series for the Column `<Series[datetime.time]>`."
        return self._series_validator(s)


@cython.cclass
class _Year(DataType):
    """The base class for all MySQL `YEAR` data type."""

    _min: cython.int
    _max: cython.int

    def __init__(
        self,
        dtype_mysql: str,
        null: cython.bint = False,
        default: Union[int, None] = None,
    ) -> None:
        """Base class for all MySQL `YEAR` data type.

        :param dtype_mysql: `<str>` The str representation of the MySQL data type. e.g. `'YEAR'`, etc.
        :param null: `<bool>` Whether is nullable. Defaults to `False`.
        :param default: `<int/None>` The default value of the column. Defaults to `None`.
        """
        self._min = 1901
        self._max = 2155
        super().__init__(
            dtype_mysql,
            int,
            primary_key=False,
            auto_increment=False,
            null=null,
            default=default,
            tabletime=False,
        )

    @property
    def default(self) -> Union[int, None]:
        """The `DEFAULT` value of the column `<int>`.
        Return `None` if default value not specified.
        """
        return self._default

    @property
    def min(self) -> int:
        "The minimum acceptable value for year `<int>`."
        return self._min

    @property
    def max(self) -> int:
        "The maximum acceptable value for year `<int>`."
        return self._max

    # Setup ---------------------------------------------------------------
    def _validation(self) -> None:
        # Validate default
        if self._default is not None:
            try:
                self._default = self.convert_int(self._default)
            except Exception as err:
                raise errors.DataTypeMetadataError(
                    "<{}.metadata> Failed to covnert default value to `<int>`: {} {}".format(
                        self.__class__.__name__,
                        repr(self._default),
                        type(self._default),
                    )
                ) from err
            if not self._min <= self._default <= self._max:
                raise errors.DataTypeMetadataError(
                    "<{}.metadata> Default value must between {} and {}, instead of: {}".format(
                        self.__class__.__name__,
                        self._min,
                        self._max,
                        self._default,
                    )
                )

    # Validator -----------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    def _item_validator(self, v: object) -> cython.int:
        "(cfunc) Validate item for the Column `<int>`."
        try:
            val: cython.longlong = self.convert_int(v)
        except Exception:
            raise errors.QueryDataValidationError(
                f"`{self._mysql}` Can't validate `item` value: {repr(v)} {type(v)}"
            )
        if not self._min <= val <= self._max:
            raise errors.QueryDataValidationError(
                "`{}` Value {} is requried to fall between {} and {}.".format(
                    self._mysql, val, self._min, self._max
                )
            )
        return val

    def item_validator(self, v: Any) -> int:
        "Validate item for the Column `<int>`."
        return self._item_validator(v)

    @cython.cfunc
    @cython.inline(True)
    def _series_validator(self, s: Series) -> object:
        "(cfunc) Validate Series for the Column `<Series[int]>`."
        kind: str = s.dtype.kind
        arr: np.ndarray
        if kind == "i":
            arr = s.values
            if (np.max(arr) > self._max) or (np.min(arr) < self._min):
                raise errors.QueryDataValidationError(
                    "`{}` Value is requried to fall between {} and {}.".format(
                        self._mysql, self._min, self._max
                    )
                )
            return s
        elif kind == "M":
            arr = s.dt.year.values
        else:
            try:
                arr = s.values.astype(np.int64)
            except Exception as err:
                raise errors.QueryDataValidationError(
                    f"`{self._mysql}` Can't validate `Series` value: {err}"
                )
            if (np.max(arr) > self._max) or (np.min(arr) < self._min):
                raise errors.QueryDataValidationError(
                    "`{}` Value is requried to fall between {} and {}.".format(
                        self._mysql, self._min, self._max
                    )
                )
        return Series(arr, s.index, name=s.name)

    def series_validator(self, s: Series) -> Series[int]:
        "Validate Series for the Column `<Series[int]>`."
        return self._series_validator(s)


@cython.cclass
class _String(DataType):
    """The base class for all MySQL `STRING` data types."""

    _length: cython.longlong
    _max_length: cython.longlong
    _limit_length: cython.longlong

    def __init__(
        self,
        dtype_mysql: str,
        length: Union[int, None],
        max_length: cython.longlong,
        null: cython.bint = False,
        default: Union[str, None] = None,
    ) -> None:
        """Base class for all MySQL `STRING` data types.

        :param dtype_mysql: `<str>` The str representation of the MySQL data type. e.g. `'VARCHAR'`, `'LONGTEXT'`, etc.
        :param length: `<int/None>` Define the maximum length of the column (if applicable).
        :param max_length: `<int>` The maximum acceptable length of the STRING TYPE.
        :param null: `<bool>` Whether is nullable. Defaults to `False`.
        :param default: `<str/None>` The default value of the column. Defaults to `None`.
        """
        self._length = length if is_int(length) else -1
        self._max_length = max_length
        super().__init__(
            dtype_mysql,
            str,
            primary_key=False,
            auto_increment=False,
            null=null,
            default=default,
            tabletime=False,
        )

    @property
    def default(self) -> str:
        """The `DEFAULT` value of the column `<str>`.
        Return `None` if default value not specified.
        """
        return self._default

    @property
    def length(self) -> int:
        "The maximum defined length of the column `<int>`."
        return self._limit_length

    @property
    def max_length(self) -> int:
        "The maximum length of the MySQL STRING data type `<int>`."
        return self._max_length

    # Setup ---------------------------------------------------------------
    def _validation(self) -> None:
        # Validate max length
        if self._max_length < 1:
            raise errors.DataTypeMetadataError(
                "<{}.metadata> Max length must be greater than 0 for MySQL '{}' "
                "DATATYPE.".format(self.__class__.__name__, self._mysql)
            )

        # Validate length
        if self._length != -1:
            if self._length < 1:
                raise errors.DataTypeMetadataError(
                    "<{}.metadata> Length must be greater than 0 for MySQL '{}' "
                    "DATATYPE.".format(self.__class__.__name__, self._mysql)
                )
            if self._length > self._max_length:
                raise errors.DataTypeMetadataError(
                    "<{}.metadata> Length must be <= {} for MySQL '{}' DATATYPE.".format(
                        self.__class__.__name__, self._max_length, self._mysql
                    )
                )

        # Set limit length
        if self._length == -1:
            self._limit_length = self._max_length
        else:
            self._limit_length = self._length

        # Validate default
        if self._default is not None:
            try:
                self._default = self.convert_str(self._default)
            except Exception as err:
                raise errors.DataTypeMetadataError(
                    "<{}.metadata> Failed to covnert default value to `<str>`: {} {}".format(
                        self.__class__.__name__,
                        repr(self._default),
                        type(self._default),
                    )
                ) from err
            if str_len(self._default) > self._limit_length:
                raise errors.DataTypeMetadataError(
                    "<{}.metadata> Default {} length must <= {} for MySQL '{}' DATATYPE.".format(
                        self.__class__.__name__,
                        repr(self._default),
                        self._limit_length,
                        self._mysql,
                    )
                )

    def _construct_syntax(self) -> None:
        self._syntax = utils._str_squeeze_spaces(
            " ".join(
                [
                    self._mysql
                    if self._length == -1
                    else "%s(%s)" % (self._mysql, self._length),
                    self._null_syntax,
                    self._default_syntax,
                ]
            ),
            True,
        )

    # Validater -----------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    def _item_validator(self, v: object) -> str:
        "(cfunc) Validate item for the Column `<str>`."
        try:
            val: str = self.convert_str(v)
        except Exception as err:
            raise errors.QueryDataValidationError(
                f"`{self._mysql}` Can't validate `item` value: {repr(v)} {type(v)}"
            ) from err
        if str_len(val) > self._limit_length:
            raise errors.QueryDataValidationError(
                "`{}` Value '{}' length is required to be <= {}.".format(
                    self._mysql, val, self._limit_length
                )
            )
        return val

    def item_validator(self, v: Any) -> str:
        "Validate item for the Column `<str>`."
        return self._item_validator(v)

    @cython.cfunc
    @cython.inline(True)
    def _series_validator(self, s: Series) -> object:
        "(cfunc) Validate Series for the Column `<Series[str]>`."
        try:
            s = s.astype(str)
        except Exception as err:
            raise errors.QueryDataValidationError(
                f"`{self._mysql}` Can't validate `Series` value: {err}"
            ) from err
        strlen: np.ndarray = np.vectorize(len)(s.values)
        if np.max(strlen) > self._limit_length:
            raise errors.QueryDataValidationError(
                "`{}` Value length is required to be <= {}.".format(
                    self._mysql, self._limit_length
                )
            )
        return s

    def series_validator(self, s: Series) -> Series[str]:
        "Validate Series for the Column `<Series[str]>`."
        return self._series_validator(s)


@cython.cclass
class _Enum(DataType):
    """The base class for all MySQL `ENUM` data type."""

    _enums: tuple
    _enums_set: set

    def __init__(
        self,
        dtype_mysql: str,
        enums: tuple[str],
        null: cython.bint = False,
        default: Union[str, None] = None,
    ) -> None:
        """Base class for all MySQL `ENUM` data type.

        :param dtype_mysql: `<str>` The str representation of the MySQL data type. e.g. `'ENUM'`, etc.
        :param enums: `<tuple[str]>` The enumerations (require at least one).
        :param null: `<bool>` Whether is nullable. Defaults to `False`.
        :param default: `<str/None>` The default value of the column. Defaults to `None`.
        """
        self._enums = enums
        self._enums_set = set(enums)
        super().__init__(
            dtype_mysql,
            str,
            primary_key=False,
            auto_increment=False,
            null=null,
            default=default,
            tabletime=False,
        )
        self._validation()

    @property
    def default(self) -> str:
        """The `DEFAULT` value of the column `<str>`.
        Return `None` if default value not specified.
        """
        return self._default

    @property
    def enums(self) -> tuple[str]:
        "The enumeration values `<tuple>`."
        return self._enums

    @property
    def enums_set(self) -> set[str]:
        "The enumeration values, access in `<set>`."
        return self._enums_set

    # Setup ---------------------------------------------------------------
    def _validation(self) -> None:
        # Validate enums
        if not self._enums:
            raise errors.DataTypeMetadataError(
                "<{}.metadata> Must provide at least one enum value for MySQL "
                "'{}' DATATYPE.".format(self.__class__.__name__, self._mysql)
            )
        if not all([is_str(i) for i in self._enums]):
            raise errors.DataTypeMetadataError(
                "<{}.metadata> All enum values must be type of `<str>` for "
                "MySQL '{}' DATATYPE.".format(self.__class__.__name__, self._mysql)
            )

        # Validate default
        if self._default is not None:
            try:
                self._default = self.convert_str(self._default)
            except Exception as err:
                raise errors.DataTypeMetadataError(
                    "<{}.metadata> Failed to covnert default value to `<str>`: {} {}".format(
                        self.__class__.__name__,
                        repr(self._default),
                        type(self._default),
                    )
                ) from err
            if self._default not in self._enums_set:
                raise errors.DataTypeMetadataError(
                    "<{}.metadata> Default value must be one of the enumerations "
                    "for MySQL '{}' DATATYPE. Current default '{}' is not a subset "
                    "of the enumeration [{}].".format(
                        self.__class__.__name__,
                        self._mysql,
                        self._default,
                        ", ".join(map(repr, self._enums)),
                    )
                )

    def _construct_syntax(self) -> None:
        self._syntax = utils._str_squeeze_spaces(
            " ".join(
                [
                    self._mysql + transcode.encode_item(self._enums, True),
                    self._null_syntax,
                    self._default_syntax,
                ]
            ),
            True,
        )

    # Validater -----------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    def _item_validator(self, v: object) -> str:
        "(cfunc) Validate item for the Column `<str>`."
        try:
            val: str = self.convert_str(v)
        except Exception:
            raise errors.QueryDataValidationError(
                f"`{self._mysql}` Can't validate `item` value: {repr(v)} {type(v)}"
            )
        if val not in self._enums_set:
            raise errors.QueryDataValidationError(
                "`{}` Value '{}' is invalid, must be a subset of [{}].".format(
                    self._mysql, val, ", ".join(map(repr, self._enums))
                )
            )
        return val

    def item_validator(self, v: Any) -> str:
        "Validate item for the Column `<str>`."
        return self._item_validator(v)

    @cython.cfunc
    @cython.inline(True)
    def _series_validator(self, s: Series) -> object:
        "(cfunc) Validate Series for the Column `<Series[str]>`."
        try:
            s = s.astype(str)
        except Exception as err:
            raise errors.QueryDataValidationError(
                f"`{self._mysql}` Can't validate `Series` value: {err}"
            )
        if not s.isin(self._enums_set).all():
            raise errors.QueryDataValidationError(
                "`{}` Values are invalid, must to be a subset of [{}]".format(
                    self._mysql, ", ".join(map(repr, self._enums))
                )
            )
        return s

    def series_validator(self, s: Series) -> Series[str]:
        "Validate Series for the Column `<Series[str]>`."
        return self._series_validator(s)


@cython.cclass
class _Binary(DataType):
    """The base class for all MySQL `BINARY` data types."""

    _length: cython.longlong
    _max_length: cython.longlong
    _limit_length: cython.longlong

    def __init__(
        self,
        dtype_mysql: str,
        length: Union[int, None],
        max_length: cython.longlong,
        null: cython.bint = False,
        default: Union[bytes, None] = None,
    ) -> None:
        """Base class for all MySQL `BINARY` data types.

        :param dtype_mysql: `<str>` The str representation of the MySQL data type. e.g. `'VARBINARY'`, `'LONGBLOB'`, etc.
        :param length: `<int/None>` Define the maximum length of the column (if applicable).
        :param max_length: `<int>` The maximum acceptable length of the BINARY TYPE.
        :param null: `<bool>` Whether is nullable. Defaults to `False`.
        :param default: `<bytes/None>` The default value of the column. Defaults to `None`.
        """
        self._length = length if is_int(length) else -1
        self._max_length = max_length
        super().__init__(
            dtype_mysql,
            bytes,
            primary_key=False,
            auto_increment=False,
            null=null,
            default=default,
            tabletime=False,
        )

    @property
    def default(self) -> bytes:
        """The `DEFAULT` value of the column `<bytes>`.
        Return `None` if default value not specified.
        """
        return self._default

    @property
    def length(self) -> int:
        "The maximum defined length of the column `<int>`."
        return self._limit_length

    @property
    def max_length(self) -> int:
        "The maximum length of the MySQL BINARY data type `<int>`."
        return self._max_length

    # Setup ---------------------------------------------------------------
    def _validation(self) -> None:
        # Validate max length
        if self._max_length < 1:
            raise errors.DataTypeMetadataError(
                "<{}.metadata> Max length must be greater than 0 for MySQL '{}' "
                "DATATYPE.".format(self.__class__.__name__, self._mysql)
            )

        # Validate length
        if self._length != -1:
            if self._length < 1:
                raise errors.DataTypeMetadataError(
                    "<{}.metadata> Length must be greater than 0 for MySQL '{}' "
                    "DATATYPE.".format(self.__class__.__name__, self._mysql)
                )
            if self._length > self._max_length:
                raise errors.DataTypeMetadataError(
                    "<{}.metadata> Length must be <= {} for MySQL '{}' DATATYPE.".format(
                        self.__class__.__name__, self._max_length, self._mysql
                    )
                )

        # Set limit length
        if self._length == -1:
            self._limit_length = self._max_length
        else:
            self._limit_length = self._length

        # Validate default
        if self._default is not None:
            try:
                self._default = self.convert_bytes(self._default)
            except Exception as err:
                raise errors.DataTypeMetadataError(
                    "<{}.metadata> Failed to covnert default value to `<bytes>`: {} {}".format(
                        self.__class__.__name__,
                        repr(self._default),
                        type(self._default),
                    )
                ) from err
            if bytes_len(self._default) > self._limit_length:
                raise errors.DataTypeMetadataError(
                    "<{}.metadata> Default {} length must <= {} for MySQL '{}' DATATYPE.".format(
                        self.__class__.__name__,
                        repr(self._default),
                        self._limit_length,
                        self._mysql,
                    )
                )

    def _construct_syntax(self) -> None:
        self._syntax = utils._str_squeeze_spaces(
            " ".join(
                [
                    self._mysql
                    if self._length == -1
                    else "%s(%s)" % (self._mysql, self._length),
                    self._null_syntax,
                    self._default_syntax,
                ]
            ),
            True,
        )

    # Validater -----------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    def _item_validator(self, v: object) -> bytes:
        "(cfunc) Validate item for the Column `<bytes>`."
        try:
            val: bytes = self.convert_bytes(v)
        except Exception as err:
            raise errors.QueryDataValidationError(
                f"`{self._mysql}` Can't validate `item` value: {repr(v)} {type(v)}"
            ) from err
        if bytes_len(val) > self._limit_length:
            raise errors.QueryDataValidationError(
                "`{}` Value {} length is required to be <= {}.".format(
                    self._mysql, repr(val), self._limit_length
                )
            )
        return val

    def item_validator(self, v: Any) -> bytes:
        "Validate item for the Column `<bytes>`."
        return self._item_validator(v)

    @cython.cfunc
    @cython.inline(True)
    def _series_validator(self, s: Series) -> object:
        "(cfunc) Validate Series for the Column `<Series[bytes]>`."
        try:
            s = s.apply(self.convert_bytes)
        except Exception as err:
            raise errors.QueryDataValidationError(
                f"`{self._mysql}` Can't validate `Series` value: {err}"
            ) from err
        byteslen: np.ndarray = np.vectorize(len)(s.values)
        if np.max(byteslen) > self._limit_length:
            raise errors.QueryDataValidationError(
                "`{}` Value length is required to be <= {}.".format(
                    self._mysql, self._limit_length
                )
            )
        return s

    def series_validator(self, s: Series) -> Series[bytes]:
        "Validate Series for the Column `<Series[bytes]>`."
        return self._series_validator(s)


# Data Types ==================================================================================
class MysqlTypes:
    """Collection of all MySQL data types supported by this package."""

    ### Numeric
    class TINYINT(_Integer):
        """The `TINYINT` MySQL data type (signed: -128 to 127, unsigned: 0 to 255)."""

        def __init__(
            self,
            primary_key: bool = False,
            auto_increment: bool = False,
            signed: bool = False,
            null: bool = False,
            default: Union[int, None] = 0,
        ) -> None:
            """The `TINYINT` MySQL data type (signed: -128 to 127, unsigned: 0 to 255).

            :param primary_key: `<bool>` Whether is the primary key. Defaults to `False`.
            :param auto_increment: `<bool>` Whether is auto incremented. Defaults to `False`.
            :param signed: `<bool>` Whether is signed (if applicable). Defaults to `False`.
            :param null: `<bool>` Whether is nullable. Defaults to `False`.
            :param default: `<int/None>` The default value of the column. Defaults to `0`.
            """
            super().__init__(
                "TINYINT",
                TINYINT_SIGNED_MIN if signed else INTEGER_UNSIGNED_MIN,
                TINYINT_SIGNED_MAX if signed else TINYINT_UNSIGNED_MAX,
                primary_key=primary_key,
                auto_increment=auto_increment,
                signed=signed,
                null=null,
                default=default,
            )

    class SMALLINT(_Integer):
        """The `SMALLINT` MySQL data type (signed: -32768 to 32767, unsigned: 0 to 65535)."""

        def __init__(
            self,
            primary_key: bool = False,
            auto_increment: bool = False,
            signed: bool = False,
            null: bool = False,
            default: Union[int, None] = 0,
        ) -> None:
            """The `SMALLINT` MySQL data type (signed: -32768 to 32767, unsigned: 0 to 65535).

            :param primary_key: `<bool>` Whether is the primary key. Defaults to `False`.
            :param auto_increment: `<bool>` Whether is auto incremented. Defaults to `False`.
            :param signed: `<bool>` Whether is signed (if applicable). Defaults to `False`.
            :param null: `<bool>` Whether is nullable. Defaults to `False`.
            :param default: `<int/None>` The default value of the column. Defaults to `0`.
            """
            super().__init__(
                "SMALLINT",
                SMALLINT_SIGNED_MIN if signed else INTEGER_UNSIGNED_MIN,
                SMALLINT_SIGNED_MAX if signed else SMALLINT_UNSIGNED_MAX,
                primary_key=primary_key,
                auto_increment=auto_increment,
                signed=signed,
                null=null,
                default=default,
            )

    class MEDIUMINT(_Integer):
        """The `MEDIUMINT` MySQL data type.
        (signed: -8388608 to 8388607, unsigned: 0 to 16777215).
        """

        def __init__(
            self,
            primary_key: bool = False,
            auto_increment: bool = False,
            signed: bool = False,
            null: bool = False,
            default: Union[int, None] = 0,
        ) -> None:
            """The `MEDIUMINT` MySQL data type.
            (signed: -8388608 to 8388607, unsigned: 0 to 16777215).

            :param primary_key: `<bool>` Whether is the primary key. Defaults to `False`.
            :param auto_increment: `<bool>` Whether is auto incremented. Defaults to `False`.
            :param signed: `<bool>` Whether is signed (if applicable). Defaults to `False`.
            :param null: `<bool>` Whether is nullable. Defaults to `False`.
            :param default: `<int/None>` The default value of the column. Defaults to `0`.
            """
            super().__init__(
                "MEDIUMINT",
                MEDIUMINT_SIGNED_MIN if signed else INTEGER_UNSIGNED_MIN,
                MEDIUMINT_SIGNED_MAX if signed else MEDIUMINT_UNSIGNED_MAX,
                primary_key=primary_key,
                auto_increment=auto_increment,
                signed=signed,
                null=null,
                default=default,
            )

    class INT(_Integer):
        """The `INT` MySQL data type.
        (signed: -2147483648 to 2147483647, unsigned: 0 to 4294967295).
        """

        def __init__(
            self,
            primary_key: bool = False,
            auto_increment: bool = False,
            signed: bool = False,
            null: bool = False,
            default: Union[int, None] = 0,
        ) -> None:
            """The `INT` MySQL data type.
            (signed: -2147483648 to 2147483647, unsigned: 0 to 4294967295).

            :param primary_key: `<bool>` Whether is the primary key. Defaults to `False`.
            :param auto_increment: `<bool>` Whether is auto incremented. Defaults to `False`.
            :param signed: `<bool>` Whether is signed (if applicable). Defaults to `False`.
            :param null: `<bool>` Whether is nullable. Defaults to `False`.
            :param default: `<int/None>` The default value of the column. Defaults to `0`.
            """
            super().__init__(
                "INT",
                INT_SIGNED_MIN if signed else INTEGER_UNSIGNED_MIN,
                INT_SIGNED_MAX if signed else INT_UNSIGNED_MAX,
                primary_key=primary_key,
                auto_increment=auto_increment,
                signed=signed,
                null=null,
                default=default,
            )

    class BIGINT(_Integer):
        """The `BIGINT` MySQL data type.
        (
            signed: -9223372036854775808 to 9223372036854775807,
            unsigned: 0 to 18446744073709551615
        ).
        """

        def __init__(
            self,
            primary_key: bool = False,
            auto_increment: bool = False,
            signed: bool = False,
            null: bool = False,
            default: Union[int, None] = 0,
        ) -> None:
            """The `BIGINT` MySQL data type.
            (
                signed: -9223372036854775808 to 9223372036854775807,
                unsigned: 0 to 18446744073709551615
            )

            :param primary_key: `<bool>` Whether is the primary key. Defaults to `False`.
            :param auto_increment: `<bool>` Whether is auto incremented. Defaults to `False`.
            :param signed: `<bool>` Whether is signed (if applicable). Defaults to `False`.
            :param null: `<bool>` Whether is nullable. Defaults to `False`.
            :param default: `<int/None>` The default value of the column. Defaults to `0`.
            """
            super().__init__(
                "BIGINT",
                BIGINT_SIGNED_MIN if signed else INTEGER_UNSIGNED_MIN,
                BIGINT_SIGNED_MAX if signed else BIGINT_UNSIGNED_MAX,
                primary_key=primary_key,
                auto_increment=auto_increment,
                signed=signed,
                null=null,
                default=default,
            )

    class FLOAT(_FloatingPoint):
        """The `FLOAT` MySQL data type."""

        def __init__(
            self,
            precision: Union[int, None] = None,
            null: bool = False,
            default: Union[float, None] = 0.0,
        ) -> None:
            """The `FLOAT` MySQL data type.

            :param precision: `<int/None>` The precision of the column. Accepts value from 0 to 53. Defaults to `None`.
                - A precision from 0 to 23 (and `None`) results in a four-byte single-precision FLOAT column.
                - A precision from 24 to 53 results in an eight-byte double-precision `DOUBLE` column.

            :param null: `<bool>` Whether is nullable. Defaults to `False`.
            :param default: `<float/None>` The default value of the column. Defaults to `0.0`.
            """
            super().__init__("FLOAT", precision, 53, null=null, default=default)

    class DOUBLE(_FloatingPoint):
        """The `DOUBLE` MySQL data type."""

        def __init__(
            self,
            null: bool = False,
            default: Union[float, None] = 0.0,
        ) -> None:
            """The `DOUBLE` MySQL data type.

            :param null: `<bool>` Whether is nullable. Defaults to `False`.
            :param default: `<float/None>` The default value of the column. Defaults to `0.0`.
            """
            super().__init__("DOUBLE", None, 53, null=null, default=default)

    class DECIMAL(_FixedPoint):
        """The `DECIMAL` MySQL data type."""

        def __init__(
            self,
            precision: int = 10,
            scale: int = 2,
            null: bool = False,
            default: Union[str, int, float, Decimal, None] = "0.0",
        ) -> None:
            """The `DECIMAL` MySQL data type.

            :param precision: `<int>` The precision of the decimal, represents the total number of significant digits. Defaults to `10`.
            :param scale: `<int>` The scale of the decimal, represents the number of digits after the decimal point. Defaults to `2`.
            :param null: `<bool>` Whether is nullable. Defaults to `False`.
            :param default: `<str/int/float/Decimal/None>` The default value of the column. Defaults to `'0.0'`.
            """
            super().__init__("DECIMAL", precision, scale, null=null, default=default)

    ### Date & Time
    class DATE(_Date):
        """The `DATE` MySQL data type."""

        def __init__(
            self,
            null: bool = False,
            default: Union[str, datetime.date, datetime.datetime, None] = "1970-01-01",
            tabletime: bool = False,
        ) -> None:
            """The `DATE` MySQL data type.

            :param null: `<bool>` Whether is nullable. Defaults to `False`.
            :param default: `<str/date/datetime/None>` The default value of the column. Defaults to `'1970-01-01'`.
            :param tabletime: `<bool>` Whether determines `TimeTable`'s tabletime. Defaults to `False`.
                - This is not an actual MySQL settings, and only affects built-in query
                  methods within `Database` and `Table` classes.
                - All `TimeTable` require to have one column set to `tabletime=True`.
                - If `True`, this columns will be used to determine the sub-timetable
                  when utilizing engine built-in query methods. Meanwhile, `null` will
                  be set to `False` automatically.
                - If `False`, this column will be treated as a normal time column.
            """
            super().__init__("DATE", null=null, default=default, tabletime=tabletime)

    class DATETIME(_Datetime):
        """The `DATETIME` MySQL data type."""

        def __init__(
            self,
            fraction: int = 0,
            auto_init: bool = False,
            auto_update: bool = False,
            null: bool = False,
            default: Union[str, datetime.date, datetime.datetime, None] = "1970-01-01",
            tabletime: bool = False,
        ) -> None:
            """The `DATETIME` MySQL data type.

            :param fraction: `<int>` Define the fractional seconds precision. Defaults to `0`.
            :param auto_init: `<bool>` Whether enable auto initiation with current timestamp. Defaults to `False`.
            :param auto_update: `<bool>` Whether enable auto update with current timestamp. Defaults to `False`.
            :param null: `<bool>` Whether is nullable. Defaults to `False`.
            :param default: `<str/date/datetime/None>` The default value of the column. Defaults to `'1970-01-01'`.
            :param tabletime: `<bool>` Whether determines `TimeTable`'s tabletime. Defaults to `False`.
                - This is not an actual MySQL settings, and only affects built-in query
                  methods within `Database` and `Table` classes.
                - All `TimeTable` require to have one column set to `tabletime=True`.
                - If `True`, this columns will be used to determine the sub-timetable
                  when utilizing engine built-in query methods. Meanwhile, `null` will
                  be set to `False` automatically.
                - If `False`, this column will be treated as a normal time column.
            """
            super().__init__(
                "DATETIME",
                fraction,
                auto_init=auto_init,
                auto_update=auto_update,
                null=null,
                default=default,
                tabletime=tabletime,
            )

    class TIMESTAMP(_Timestamp):
        """The `TIMESTAMP` MySQL data type."""

        def __init__(
            self,
            fraction: int = 0,
            auto_init: bool = True,
            auto_update: bool = False,
            null: bool = False,
            tabletime: bool = False,
        ) -> None:
            """The `TIMESTAMP` MySQL data type.

            :param fraction: `<int>` Define the fractional seconds precision. Defaults to `0`.
            :param auto_init: `<bool>` Whether enable auto initiation with current timestamp. Defaults to `True`.
            :param auto_update: `<bool>` Whether enable auto update with current timestamp. Defaults to `False`.
            :param null: `<bool>` Whether is nullable. Defaults to `False`.
            :param tabletime: `<bool>` Whether determines `TimeTable`'s tabletime. Defaults to `False`.
                - This is not an actual MySQL settings, and only affects built-in query
                  methods within `Database` and `Table` classes.
                - All `TimeTable` require to have one column set to `tabletime=True`.
                - If `True`, this columns will be used to determine the sub-timetable
                  when utilizing engine built-in query methods. Meanwhile, `null` will
                  be set to `False` automatically.
                - If `False`, this column will be treated as a normal time column.
            """
            super().__init__(
                "TIMESTAMP",
                fraction,
                auto_init=auto_init,
                auto_update=auto_update,
                null=null,
                tabletime=tabletime,
            )

    class TIME(_Time):
        """The `TIME` MySQL data type."""

        def __init__(
            self,
            fraction: int = 0,
            null: bool = False,
            default: Union[str, datetime.time, datetime.datetime, None] = "00:00:00",
        ) -> None:
            """The `TIME` MySQL data type.

            :param fraction: `<int>` Define the fractional seconds precision. Defaults to `0`.
            :param null: `<bool>` Whether is nullable. Defaults to `False`.
            :param default: `<str/time/datetime/None>` The default value of the column. Defaults to `'00:00:00'`.
            """
            super().__init__("TIME", fraction, null=null, default=default)

    class YEAR(_Year):
        """The `YEAR` MySQL data type."""

        def __init__(
            self,
            null: bool = False,
            default: Union[int, None] = 1901,
        ) -> None:
            """The `YEAR` MySQL data type (value: 1901 to 2155).

            :param null: `<bool>` Whether is nullable. Defaults to `False`.
            :param default: `<int/None>` The default value of the column. Defaults to `1901`.
            """
            super().__init__("YEAR", null=null, default=default)

    ### String Types
    class CHAR(_String):
        """The `CHAR` MySQL data type (length: 0 to 255)."""

        def __init__(
            self,
            length: int = 255,
            null: bool = False,
            default: Union[str, None] = "",
        ) -> None:
            """The `CHAR` MySQL data type (length: 0 to 255).

            :param length: `<int>` Define the maximum length of the column. Defaults to `255`.
            :param null: `<bool>` Whether is nullable. Defaults to `False`.
            :param default: `<str/None>` The default value of the column. Defaults to `''`.
            """
            if length <= 0:
                raise errors.DataTypeMetadataError(
                    "<CHAR.metadata> `length` must be greater "
                    "than 0 for MySQL 'CHAR' DATATYPE."
                )
            super().__init__("CHAR", length, TINYTEXT_MAX, null=null, default=default)

    class VARCHAR(_String):
        """The `VARCHAR` MySQL data type (length: 0 to 65535)."""

        def __init__(
            self,
            length: int = 255,
            null: bool = False,
            default: Union[str, None] = "",
        ) -> None:
            """The `VARCHAR` MySQL data type (length: 0 to 65535).

            :param length: `<int>` Define the maximum length of the column. Defaults to `255`.
            :param null: `<bool>` Whether is nullable. Defaults to `False`.
            :param default: `<str/None>` The default value of the column. Defaults to `''`.
            """
            if length <= 0:
                raise errors.DataTypeMetadataError(
                    "<VARCHAR.metadata> `length` must be greater "
                    "than 0 for MySQL 'VARCHAR' DATATYPE."
                )
            super().__init__("VARCHAR", length, TEXT_MAX, null=null, default=default)

    class TINYTEXT(_String):
        """The `TINYTEXT` MySQL data type (length: 255)."""

        def __init__(self, null: bool = False) -> None:
            """The `TINYTEXT` MySQL data type (length: 255).

            :param null: `<bool>` Whether is nullable. Defaults to `False`.
            """
            super().__init__("TINYTEXT", None, TINYTEXT_MAX, null=null, default=None)

    class TEXT(_String):
        """The `TEXT` MySQL data type (length: 65535)."""

        def __init__(self, null: bool = False) -> None:
            """The `TEXT` MySQL data type (length: 65535).

            :param null: `<bool>` Whether is nullable. Defaults to `False`.
            """
            super().__init__("TEXT", None, TEXT_MAX, null=null, default=None)

    class MEDIUMTEXT(_String):
        """The `MEDIUMTEXT` MySQL data type (length: 16777215)."""

        def __init__(self, null: bool = False) -> None:
            """The `MEDIUMTEXT` MySQL data type (length: 16777215).

            :param null: `<bool>` Whether is nullable. Defaults to `False`.
            """
            super().__init__(
                "MEDIUMTEXT", None, MEDIUMTEXT_MAX, null=null, default=None
            )

    class LONGTEXT(_String):
        """The `LONGTEXT` MySQL data type (length: 4294967295)."""

        def __init__(self, null: bool = False) -> None:
            """The `LONGTEXT` MySQL data type (length: 4294967295).

            :param null: `<bool>` Whether is nullable. Defaults to `False`.
            """
            super().__init__("LONGTEXT", None, LONGTEXT_MAX, null=null, default=None)

    class ENUM(_Enum):
        """The `ENUM` MySQL data type."""

        def __init__(
            self,
            *enums: str,
            null: bool = False,
            default: Union[str, None] = None,
        ) -> None:
            """The `ENUM` MySQL data type.

            :param enums: `*str` The enumerations (require at least one).
            :param null: `<bool>` Whether is nullable. Defaults to `False`.
            :param default: `<str/None>` The default value of the column. Defaults to `None`.
            """
            super().__init__("ENUM", enums, null=null, default=default)

    ### Binary Types
    class BINARY(_Binary):
        """The `BINARY` MySQL data type (length: 0 to 255)."""

        def __init__(
            self,
            length: int = 255,
            null: bool = False,
            default: Union[bytes, None] = b"",
        ) -> None:
            """The `BINARY` MySQL data type (length: 0 to 255).

            :param length: `<int>` Define the maximum length of the column. Defaults to `255`.
            :param null: `<bool>` Whether is nullable. Defaults to `False`.
            :param default: `<bytes/None>` The default value of the column. Defaults to `b''`.
            """
            if length <= 0:
                raise errors.DataTypeMetadataError(
                    "<BINARY.metadata> `length` must be greater "
                    "than 0 for MySQL 'BINARY' DATATYPE."
                )
            super().__init__("BINARY", length, TINYTEXT_MAX, null=null, default=default)

    class VARBINARY(_Binary):
        """The `VARBINARY` MySQL data type (length: 0 to 65535)."""

        def __init__(
            self,
            length: int = 255,
            null: bool = False,
            default: Union[bytes, None] = b"",
        ) -> None:
            """The `VARBINARY` MySQL data type (length: 0 to 65535).

            :param length: `<int>` Define the maximum length of the column. Defaults to `255`.
            :param null: `<bool>` Whether is nullable. Defaults to `False`.
            :param default: `<bytes/None>` The default value of the column. Defaults to `b''`.
            """
            if length <= 0:
                raise errors.DataTypeMetadataError(
                    "<VARBINARY.metadata> `length` must be greater "
                    "than 0 for MySQL 'VARBINARY' DATATYPE."
                )
            super().__init__("VARBINARY", length, TEXT_MAX, null=null, default=default)

    class TINYBLOB(_Binary):
        """The `TINYBLOB` MySQL data type (length: 255)."""

        def __init__(self, null: bool = False) -> None:
            """The `TINYBLOB` MySQL data type (length: 255).

            :param null: `<bool>` Whether is nullable. Defaults to `False`.
            """
            super().__init__("TINYBLOB", None, TINYTEXT_MAX, null=null, default=None)

    class BLOB(_Binary):
        """The `BLOB` MySQL data type (length: 65535)."""

        def __init__(self, null: bool = False) -> None:
            """The `BLOB` MySQL data type (length: 65535).

            :param null: `<bool>` Whether is nullable. Defaults to `False`.
            """
            super().__init__("BLOB", None, TEXT_MAX, null=null, default=None)

    class MEDIUMBLOB(_Binary):
        """The `MEDIUMBLOB` MySQL data type (length: 16777215)."""

        def __init__(self, null: bool = False) -> None:
            """The `MEDIUMBLOB` MySQL data type (length: 16777215).

            :param null: `<bool>` Whether is nullable. Defaults to `False`.
            """
            super().__init__(
                "MEDIUMBLOB", None, MEDIUMTEXT_MAX, null=null, default=None
            )

    class LONGBLOB(_Binary):
        """The `LONGBLOB` MySQL data type (length: 4294967295)."""

        def __init__(self, null: bool = False) -> None:
            """The `LONGBLOB` MySQL data type (length: 4294967295).

            :param null: `<bool>` Whether is nullable. Defaults to `False`.
            """
            super().__init__("LONGBLOB", None, LONGTEXT_MAX, null=null, default=None)
