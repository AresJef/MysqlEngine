# cython: language_level=3
# cython: wraparound=False
# cython: boundscheck=False

# Cython imports
import cython
from cython.cimports.cpython.dict import PyDict_GetItem as dict_get  # type: ignore
from cython.cimports.cpython.dict import PyDict_Items as dict_items  # type: ignore
from cython.cimports.cpython.list import PyList_Append as list_append  # type: ignore
from cython.cimports.cpython.set import PySet_Contains as set_contains  # type: ignore
from cython.cimports.cpython.unicode import PyUnicode_Contains as str_contains  # type: ignore
from cython.cimports.mysqlengine.dtype import DataType  # type: ignore
from cython.cimports.mysqlengine import errors, settings, utils  # type: ignore

# Python imports
from typing import Any, Union, Callable, Iterator
from mysqlengine import errors, settings, utils
from mysqlengine.dtype import DataType, MysqlTypes

__all__ = ["Column", "DummyColumn", "Columns", "TableColumns"]


# Column =====================================================================================
@cython.cclass
class Column:
    """Represents a Column of the `Table`."""

    _dtype: DataType
    _name: str
    _name_lower: str
    _mysql: str
    _python: type
    _primary_key: cython.bint
    _auto_increment: cython.bint
    _null: cython.bint
    _default: object
    _tabletime: cython.bint
    _syntax: str

    def __init__(self, name: str, dtype: DataType) -> None:
        """Column of a `Table`.

        :param name: `<str>` Column name. Must be unique for the hosting table.
        :param dtype: `<DataType>` The MySQL data type of the Column.
            The `DataType` class is used to define the column MetaData. All `DataTypes`
            can be accessed through `mysqlengine.MysqlTypes`.

        ### Example:
        >>> # Create columns
            from mysqlengine import Column, Mysql_Types
            id = Column("id", MysqlTypes.BIGINT(primary_key=True))
            name = Column("name", MysqlTypes.VARCHAR(255))
            ...
        """
        self._dtype = dtype
        self._name = name
        self.__setup()

    # Properties ----------------------------------------------------------
    @property
    def name(self) -> str:
        "The name of the column `<str>`."
        return self._name

    @property
    def dtype(self) -> DataType:
        "The MySQL data type of the column `<DataType>`."
        return self._dtype

    @property
    def mysql(self) -> str:
        """The str representation of columns' MySQL data type `<str>`.
        e.g. `TINYINT`, `VARCHAR`, `DATETIME`, etc."""
        return self._mysql

    @property
    def python(self) -> type:
        """The matching Python native data type `<type>`.
        e.g. `int`, `float`, `str`, `datetime`, etc."""
        return self._python

    @property
    def primary_key(self) -> bool:
        "Whether column is the primary key of the table `<bool>`."
        return self._primary_key

    @property
    def auto_increment(self) -> bool:
        "Whether column is auto incremented `<bool>`."
        return self._auto_increment

    @property
    def null(self) -> bool:
        "Whether column is nullable `<bool>`."
        return self._null

    @property
    def default(self) -> Union[Any, None]:
        """The `DEFAULT` value of the column `<Any/None>`.
        Return `None` if default value not specified.
        """
        return self._default

    @property
    def tabletime(self) -> bool:
        "Whether column determine `TimeTable`'s tabletime `<bool>`."
        return self._tabletime

    @property
    def syntax(self) -> str:
        "The `SQL` syntax of the column `<str>`."
        return self._syntax

    # Setup ---------------------------------------------------------------
    def __setup(self) -> None:
        # Validate column name
        _name = utils._str_clean_name(self._name)
        if not _name:
            raise errors.ColumnMetadataError(
                "<{}.metadata> Invalid Column name: {}".format(
                    self.__class__.__name__, repr(self._name)
                )
            )
        if _name.lower() in settings.PROHIBIT_NAMES:
            raise errors.ColumnMetadataError(
                "<{}.metadata> Column name '{}' is prohibited, "
                "please choose another one.".format(self.__class__.__name__, _name)
            )
        if len(_name) > settings.MAX_NAME_LENGTH:
            raise errors.ColumnMetadataError(
                "<{}.metadata> Column name '{}' is too long, "
                "must be less than {} characters.".format(
                    self.__class__.__name__, _name, settings.MAX_NAME_LENGTH
                )
            )
        self._name = _name
        self._name_lower = _name.lower()

        # Validate column data type
        if not isinstance(self._dtype, DataType):
            raise errors.ColumnMetadataError(
                "<{}.metadata> Column data type must be an instance of "
                "<'mysqlenging.DataType'>, instead of: {} {}".format(
                    self.__class__.__name__, type(self._dtype), repr(self._dtype)
                )
            )

        # Essential attributes
        self._mysql = self._dtype._mysql
        self._python = self._dtype._python
        self._primary_key = self._dtype._primary_key
        self._auto_increment = self._dtype._auto_increment
        self._null = self._dtype._null
        self._default = self._dtype._default
        self._tabletime = self._dtype._tabletime

        # Construct column syntax
        self._syntax = utils._str_squeeze_spaces(
            self._name + " " + self._dtype._syntax, True
        )

    # Special Methods ------------------------------------------------------
    def __repr__(self) -> str:
        return "<%s (name=%s, syntax=%s)>" % (
            self.__class__.__name__,
            repr(self._name),
            repr(self._syntax),
        )

    def __str__(self) -> str:
        return self._syntax

    def __hash__(self) -> int:
        return hash(self._syntax)

    def __eq__(self, __o: object) -> bool:
        return self._syntax == __o.syntax if isinstance(__o, Column) else False

    def __bool__(self) -> bool:
        return True

    def __del__(self):
        self._dtype = None
        self._python = None
        self._default = None


@cython.cclass
class DummyColumn(Column):
    """Represents `None` for a Column."""

    _err_msg: str

    def __init__(self) -> None:
        """Represents `None` for a Column."""
        super().__init__("None", MysqlTypes.BIGINT())
        self._err_msg: str = "<class 'DummyColumn'> Represents `None` for a Column.."
        self._dtype = None
        self._name = None
        self._mysql = None
        self._python = None
        self._primary_key = False
        self._auto_increment = False
        self._null = False
        self._default = None
        self._tabletime = False
        self._syntax = ""

    # Properties ----------------------------------------------------------
    @property
    def dtype(self) -> str:
        "The `DataType` of the column."
        raise errors.ColumnError(self._err_msg)

    @property
    def mysql(self) -> str:
        """The str representation of columns' MySQL data type `<str>`.
        e.g. `TINYINT`, `VARCHAR`, `DATETIME`, etc."""
        raise errors.ColumnError(self._err_msg)

    @property
    def python(self) -> type:
        """The matching Python native data type `<type>`.
        e.g. `int`, `float`, `str`, `datetime`, etc."""
        raise errors.ColumnError(self._err_msg)

    @property
    def default(self) -> Any:
        "The preset `DEFAULT` value of the column."
        raise errors.ColumnError(self._err_msg)

    @property
    def syntax(self) -> str:
        "The `SQL` syntax of the column."
        raise errors.ColumnError(self._err_msg)

    # Special Methods -----------------------------------------------------
    def __hash__(self) -> int:
        raise errors.ColumnError(self._err_msg)

    def __eq__(self, __o: object) -> bool:
        return False

    def __bool__(self) -> bool:
        return False


# Columns ====================================================================================
@cython.cclass
class Columns:
    """Represents a collection of Columns.

    Works as an immutable dictionary with static typings, where
    keys are the column names and values are the `Column` instances.
    Meanwhile, it provides search methods to find columns.
    """

    _dict: dict[str, Column]
    _names: tuple[str]
    _names_set: set[str]
    _instances: tuple[Column]
    _items: tuple[tuple[str, Column]]
    _length: cython.int

    def __init__(self, *columns: Column) -> None:
        """A collection of Columns.

        Works as an immutable dictionary with static typings, where
        keys are the column names and values are the `Column` instances.
        Meanwhile, it provides search methods to find columns.

        :param columns: `<Column>(s)` Column(s) to add to the collection.
        """
        self.__setup(columns)

    # Properties -----------------------------------------------------------
    @property
    def names(self) -> tuple[str]:
        "The names of the columns `<tuple[str]>`."
        return self._names

    @property
    def names_set(self) -> set[str]:
        "The names of the columns, access in `<set[str]>`."
        return self._names_set

    @property
    def instances(self) -> tuple[Column]:
        "The column instances `<tuple[Column]>`."
        return self._instances

    # Setup ---------------------------------------------------------------
    def __setup(self, columns: tuple[Column]) -> None:
        # Validate columns
        _seen: set = set()
        _names: list = []
        _instances: list = []
        _dic: dict = {}
        for col in columns:
            # . data type
            if not isinstance(col, Column):
                raise errors.ColumnError(
                    "<{}> Only accepts instance of <'mysqlenging.Column'>, instead of: "
                    "{} {}".format(self.__class__.__name__, type(col), repr(col))
                )
            if isinstance(col, DummyColumn):
                raise errors.ColumnError(
                    "<{}> `<DummyColumn>` Represents `None` for a column, "
                    "which is prohibited to add to Columns collection.".format(
                        self.__class__.__name__
                    )
                )
            # . duplicates
            if col in _seen:
                raise errors.ColumnError(
                    "<{}> Duplicated column: {}.".format(
                        self.__class__.__name__, repr(col)
                    )
                )
            if col.name in _names:
                raise errors.ColumnError(
                    "<{}> Duplicated column name: '{}'.".format(
                        self.__class__.__name__, repr(col.name)
                    )
                )
            _seen.add(col)
            _names.append(col.name)
            _instances.append(col)
            _dic[col.name] = col

        # Setup columns
        self._dict = _dic
        self._names = tuple(_names)
        self._names_set = set(_names)
        self._instances = tuple(_instances)
        self._items = tuple(dict_items(_dic))
        self._length = len(_names)
        del _seen

    # Search --------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def _search_by_name(self, name: str, exact: cython.bint) -> list:
        """(cfunc) Search columns by name.

        :param name: `<str>` The name or partial name of the desired column(s).
        :param exact: `<bool>` Whether to perform exact match.
            - If `True`, return only the column with the exact provided 'name'.
            - If `False`, return all columns that contain the '(partial) name'.

        :return `<list[Column]>`: Matched column(s).
        """
        res: list = []
        col: Column
        if exact:
            for col in self._instances:
                if col._name == name:
                    list_append(res, col)
        else:
            name = name.lower()
            for col in self._instances:
                if str_contains(col._name_lower, name):
                    list_append(res, col)
        return res

    def search_by_name(self, name: str, exact: bool = False) -> list[Column]:
        """Search columns by name.

        :param name: `<str>` The name or partial name of the desired column(s).
        :param exact: `<bool>` Whether to perform exact match. Defaults to `False`.
            - If `True`, return only the column with the exact provided 'name'.
            - If `False`, return all columns that contain the '(partial) name'.

        :return `<list[Column]>`: Matched column(s).
        """
        return self._search_by_name(name, exact)

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def _search_by_mysql_dtype(self, dtype: str, exact: cython.bint) -> list:
        """(cfunc) Search columns by MySQL data type.

        :param dtype: `<str>` String representation of MySQL data type. e.g. `'BIGINT'`, `'VARCHAR'`, etc.
        :param exact: `<bool>` Whether to perfrom exact match.
            - If `True`, return only columns with the exact provided 'dtype'.
            - If `False`, return all columns with data type containing the provided
              'dtype' string. For example, a 'dtype' of 'INT' will match 'BIGINT'
              if exact_match is `False`.

        :return `<list[Column]>`: Matched column(s).
        """
        res: list = []
        col: Column
        if exact:
            for col in self._instances:
                if col._mysql == dtype:
                    list_append(res, col)
        else:
            dtype = dtype.upper()
            for col in self._instances:
                if str_contains(col._mysql, dtype):
                    list_append(res, col)
        return res

    def search_by_mysql_dtype(self, dtype: str, exact: bool = False) -> list[Column]:
        """Search columns by MySQL data type.

        :param dtype: `<str>` String representation of MySQL data type. e.g. `'BIGINT'`, `'VARCHAR'`, etc.
        :param exact: `<bool>` Whether to perfrom exact match. Defaults to `False`.
            - If `True`, return only columns with the exact provided 'dtype'.
            - If `False`, return all columns with data type containing the provided
              'dtype' string. For example, a 'dtype' of 'INT' will match 'BIGINT'
              if exact_match is `False`.

        :return `<list[Column]>`: Matched column(s).
        """
        return self._search_by_mysql_dtype(dtype, exact)

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def _search_by_python_dtype(self, dtypes: tuple) -> list:
        """(cfunc) Search columns by Python data type.

        :param dtypes: `<tuple[type]>` Python data types (not `<str>`). e.g. `int`, `Decimal`, etc.
        :return `<list[Column]>`: All columns that match any of the provided Python data types.
        """
        type_set: set = set(dtypes)
        res: list = []
        col: Column
        for col in self._instances:
            if set_contains(type_set, col._python):
                list_append(res, col)
        return res

    def search_by_python_dtype(self, *dtypes: type) -> list[Column]:
        """Search columns by Python data type.

        :param dtypes: `<type>` Python data types (not `<str>`). e.g. `int`, `Decimal`, etc.
        :return `<list[Column]>`: All columns that match any of the provided Python data types.
        """
        return self._search_by_python_dtype(dtypes)

    # Filter --------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def _issubset(self, columns: tuple) -> cython.bint:
        "(cfunc) Whether Columns collection is a subset of the given columns `<bool>`."
        # Empty columns
        if self._length == 0 or not columns:
            return False

        # Get column names
        names: set = get_columns_names(columns)  # type: ignore

        # Subset comparison
        return True if self._names_set <= names else False

    def issubset(self, *columns: Union[str, Column]) -> bool:
        "Whether Columns collection is a subset of the given columns `<bool>`."
        return self._issubset(columns)

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def _filter(self, columns: tuple) -> list:
        """(cfunc) Filter & sort the given columns by the Columns collection.
        Returns the column names that exist in the collection `<list[str]>`.
        """
        # Empty columns
        if self._length == 0 or not columns:
            return []

        # Get column names
        names: set = get_columns_names(columns)  # type: ignore

        # Filter & sort
        res: list = []
        for name in self._names:
            if set_contains(names, name):
                list_append(res, name)
        return res

    def filter(self, *columns: Union[str, Column]) -> list[str]:
        """Filter & sort the given columns by the Columns collection.
        Returns the column names that exist in the collection `<list[str]>`.
        """
        return self._filter(columns)

    # Accessors ------------------------------------------------------------
    def keys(self) -> tuple[str]:
        "Access the names of the columns `<tuple[str]>`."
        return self._names

    def values(self) -> tuple[Column]:
        "Access the column instances `<tuple[Column]>`."
        return self._instances

    def items(self) -> tuple[tuple[str, Column]]:
        "Access the column names and instances `<tuple[tuple[str, Column]]>`."
        return self._items

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def _get(self, col: object, default: object) -> object:
        "(cfunc) Get column `<Column/Any>`. Return `default` if column does not exist."
        res = dict_get(self._dict, get_column_name(col))  # type: ignore
        if res == cython.NULL:
            return default
        else:
            return cython.cast(object, res)

    def get(self, col: Union[str, Column], default: Any = None) -> Union[Column, Any]:
        "Get column `<Column>`. Return `default` if column does not exist."
        return self._get(col, default)

    # Special Methods ------------------------------------------------------
    def __repr__(self) -> str:
        return "<%s %s>" % (self.__class__.__name__, self._dict.__repr__())

    def __str__(self) -> str:
        return self._dict.__str__()

    def __hash__(self) -> int:
        return self.__repr__().__hash__()

    def __eq__(self, __o: object) -> bool:
        return str(self) == str(__o) if isinstance(__o, Columns) else False

    def __bool__(self) -> bool:
        return self._length > 0

    def __len__(self) -> int:
        return self._length

    def __iter__(self) -> Iterator[Column]:
        return self._instances.__iter__()

    @cython.cfunc
    @cython.inline(True)
    def _getitem(self, col: object) -> Column:
        """(cfunc) Get column `<Column>`. Equivalent to `Columns[col]`.
        Raises `ColumnNotExistError` if column does not exist.
        """
        res = dict_get(self._dict, get_column_name(col))  # type: ignore
        if res == cython.NULL:
            raise errors.ColumnNotExistError(
                "<%s> Does not contain column: %s"
                % (self.__class__.__name__, repr(col))
            )
        return cython.cast(object, res)

    def __getitem__(self, col: object) -> Column:
        return self._getitem(col)

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def _contains(self, col: object) -> cython.bint:
        "(cfunc) Whether contains the column `<bool>`. Equivalent to `col in Columns`."
        return set_contains(self._names_set, get_column_name(col))  # type: ignore

    def __contains__(self, col: object) -> bool:
        return self._contains(col)

    def __del__(self):
        self._dict = None
        self._names = None
        self._names_set = None
        self._instances = None
        self._items = None


@cython.cclass
class TableColumns(Columns):
    """Represents a collection of Columns of for `<Table>` class (specific).

    Works as an immutable dictionary with static typings, where
    keys are the column names and values are the `Column` instances.
    Meanwhile provides distinctive features compared to the `Columns` class:
    - 1. Column validation based on MySQL Table rules.
    - 2. Direct access to the table's `PRIMARY KEY` column `<Column>`.
    - 3. Direct access to the column with `tabletime=True` (if applicable) `<Column>`.
    - 4. Access to column validators `<dict>`.
    - 5. Collection of all auto-incremented columns `<Columns>`.
    - 6. Collection of all non-auto incremented columns `<Columns>`.
    """

    _primary_key: Column
    _tabletime: Column
    _auto_increments: Columns
    _non_auto_increments: Columns
    _item_validators: dict[str, Callable]
    _series_validators: dict[str, Callable]
    _syntax: str

    def __init__(self, *columns: Column) -> None:
        """A collection of Columns of for `<Table>` class (specific).

        Works as an immutable dictionary with static typings, where
        keys are the column names and values are the `Column` instances.
        Meanwhile provides distinctive features compared to the `Columns` class:
        - 1. Column validation based on MySQL Table rules.
        - 2. Direct access to the table's `PRIMARY KEY` column `<Column>`.
        - 3. Direct access to the column with `tabletime=True` (if applicable) `<Column>`.
        - 4. Access to column validators `<dict>`.
        - 5. Collection of all auto-incremented columns `<Columns>`.
        - 6. Collection of all non-auto incremented columns `<Columns>`.

        :param columns: `<Column>(s)` Column(s) to add to the Table.
        """
        super().__init__(*columns)
        self.__setup()

    # Properties -----------------------------------------------------------
    @property
    def primary_key(self) -> Column:
        "The `PRIMARY KEY` column of the Table `<Column>`."
        return self._primary_key

    @property
    def tabletime(self) -> Union[Column, DummyColumn]:
        """The column with `tabletime=True` `<Column>`.
        If column does not exist, return `<DummyColumn>`.
        """
        return self._tabletime

    @property
    def auto_increments(self) -> Columns:
        "The collection of all auto-incremented columns `<Columns>`."
        return self._auto_increments

    @property
    def non_auto_increments(self) -> Columns:
        "The collection of all non-auto incremented columns `<Columns>`."
        return self._non_auto_increments

    @property
    def item_validators(self) -> dict[str, Callable]:
        "The validators of the columns value (item) `<dict[str, Callable]>`."
        return self._item_validators

    @property
    def series_validators(self) -> dict[str, Callable]:
        "The validators of the columns value (Series) `<dict[str, Callable]>`."
        return self._series_validators

    @property
    def syntax(self) -> str:
        "The `SQL` syntax of all the columns `<str>`."
        return self._syntax

    # Setup ----------------------------------------------------------------
    def __setup(self) -> None:
        # Check for columns limits
        if self._length == 0:
            raise errors.TableMetadataError(
                "<{}.metadata> Table must have at least one "
                "column.".format(self.__class__.__name__)
            )
        if self._length > settings.MAX_COLUMNS:
            raise errors.TableMetadataError(
                "<{}.metadata> Table has too many columns, must be less "
                "than {}.".format(self.__class__.__name__, settings.MAX_COLUMNS)
            )

        # Validate columns
        _primary_key: list = []
        _tabletime: list = []
        _auto_increments: list = []
        _non_auto_increments: list = []
        _item_validators: dict = {}
        _series_validators: dict = {}
        _syntax: list = []
        for col in self._instances:
            # . primary key
            if col.primary_key:
                list_append(_primary_key, col)
            # . tabletime
            if col.tabletime:
                list_append(_tabletime, col)
            # . auto increment
            if col.auto_increment:
                list_append(_auto_increments, col)
            else:
                list_append(_non_auto_increments, col)
            # . validators
            _item_validators[col.name] = col.dtype.item_validator
            _series_validators[col.name] = col.dtype.series_validator
            # . syntax
            list_append(_syntax, col.syntax)

        # Validate primary key
        if len(_primary_key) != 1:
            raise errors.TableMetadataError(
                "<{}.metadata> Table must have one `PRIMARY KEY` "
                "column.".format(self.__class__.__name__)
            )
        self._primary_key = _primary_key[0]

        # Validate tabletime
        if len(_tabletime) > 1:
            raise errors.TableMetadataError(
                "<{}.metadata> Table can only have one column with "
                "`tabletime=True`.".format(self.__class__.__name__)
            )
        elif len(_tabletime) == 1:
            self._tabletime = _tabletime[0]
        else:
            self._tabletime = DummyColumn()

        # Setup sub-columns
        self._auto_increments = Columns(*_auto_increments)
        self._non_auto_increments = Columns(*_non_auto_increments)

        # Setup column validators
        self._item_validators = _item_validators
        self._series_validators = _series_validators

        # Construct syntax
        self._syntax = utils._str_squeeze_spaces(",\n".join(_syntax), True)

    # Syntax ---------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def _gen_syntax(self, columns: tuple) -> str:
        """(cfunc) Generate the SQL COLUMN syntax for the specified columns.

        This method will create the (partial) SQL COLUMN syntax based on
        the provided columns. If no columns are given, all columns will
        be included in the syntax

        :param columns: `<tuple[str/Column]>` The columns for the syntax.
            - If provided, only columns that are specified by the given
              columns will be included in the syntax.
            - If not provided, all columns will be included in the syntax.

        :return: `<str>` The (partial) SQL COLUMN syntax.
        """
        # Full syntax if not specified
        if not columns:
            return self._syntax

        # Get columns names
        names: set = get_columns_names(columns)  # type: ignore

        # Filter & sort
        res: list = []
        col: Column
        for col in self._instances:
            if set_contains(names, col._name):
                list_append(res, col._syntax)
        return utils._str_squeeze_spaces(",\n".join(res), False)

    def gen_syntax(self, *columns: Union[str, Column]) -> str:
        """Generate the SQL COLUMN syntax for the specified columns.

        This method will create the (partial) SQL COLUMN syntax based on
        the provided columns. If no columns are given, all columns will
        be included in the syntax

        :param columns: `<str/Column>` The columns for the syntax.
            - If provided, only columns that are specified by the given
              columns will be included in the syntax.
            - If not provided, all columns will be included in the syntax.

        :return: `<str>` The (partial) SQL COLUMN syntax.
        """
        return self._gen_syntax(columns)

    # Validate -------------------------------------------------------------
    def validate(self, column: Column) -> Column:
        """Validate column for `<Table>` class. `<Column>`.
        Used for adding columns to the table (internal use only).
        """
        # Check for column limits
        if self._length + 1 > settings.MAX_COLUMNS:
            raise errors.TableMetadataError(
                "<{}.metadata> Table has too many columns, "
                "must be less than {}.".format(
                    self.__class__.__name__, settings.MAX_COLUMNS
                )
            )

        # Validate data type
        if not isinstance(column, Column):
            raise errors.TableMetadataError(
                "<{}.metadata> Table column must be instance of "
                "<'mysqlenging.Column'>, instead of: {} {}".format(
                    self.__class__.__name__, type(column), repr(column)
                )
            )
        if isinstance(column, DummyColumn):
            raise errors.TableMetadataError(
                "<{}.metadata> Table column cannot be instance of "
                "<'DummyColumn'>, which represents `None` for a column.".format(
                    self.__class__.__name__
                )
            )

        # Check for duplicates
        if column in self._instances:
            raise errors.TableMetadataError(
                "<{}.metadata> Duplicated column: {}.".format(
                    self.__class__.__name__, repr(column)
                )
            )
        if column.name in self._names_set:
            raise errors.TableMetadataError(
                "<{}.metadata> Duplicated column name: '{}'.".format(
                    self.__class__.__name__, repr(column.name)
                )
            )

        # Validate primary key
        if column.primary_key:
            raise errors.TableMetadataError(
                "<{}.metadata> Table already has a `PRIMARY KEY` "
                "column: {}.".format(self.__class__.__name__, repr(self._primary_key))
            )

        # Validate tabletime
        if column.tabletime and self._tabletime:
            raise errors.TableMetadataError(
                "<{}.metadata> Table already has a column with `tabletime=True`: "
                "{}.".format(self.__class__.__name__, repr(self._tabletime))
            )

        # Return column
        return column
