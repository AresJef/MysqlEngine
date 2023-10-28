# cython: language_level=3
# cython: wraparound=False
# cython: boundscheck=False

# Cython imports
import cython
from cython.cimports.cpython.set import PySet_Contains as set_contains  # type: ignore
from cython.cimports.cpython.dict import PyDict_GetItem as dict_get  # type: ignore
from cython.cimports.cpython.dict import PyDict_Items as dict_items  # type: ignore
from cython.cimports.cpython.list import PyList_Append as list_append  # type: ignore
from cython.cimports.cpython.unicode import PyUnicode_Contains as str_contains  # type: ignore
from cython.cimports.mysqlengine.column import Column, Columns  # type: ignore
from cython.cimports.mysqlengine.column import get_columns_names  # type: ignore
from cython.cimports.mysqlengine import errors, settings, utils  # type: ignore

# Python imports
from typing import Any, Union, Iterator
from mysqlengine import errors, settings, utils
from mysqlengine.column import Column, Columns, MysqlTypes

__all__ = ["Index", "DummyIndex", "Indexes", "UniqueIndexes", "TableIndexes"]


# Index ======================================================================================
@cython.cclass
class Index:
    """Represents an Index of the `Table`."""

    _name: str
    _name_lower: str
    _unique: cython.bint
    _primary_unique: cython.bint
    _columns: Columns
    _syntax: str

    def __init__(
        self,
        *columns: Column,
        name: Union[str, None] = None,
        unique: bool = False,
        primary_unique: bool = False,
    ) -> None:
        """Index of a `Table`.

        :param columns: `<Column>(s)` Columns that compose the index.
        :param name: `<str/None>` The name of the index. Defaults to `None`.
            - If `None`, an index name will be automatically generated based on the
              index type and given columns, formated as the following camelCase:
              `'<index_type><column_name><column_name>...'`.
            - `index_type`: `'uix'` for UNIQUE index, else `'idx'`.
            - `column_name`: Column names' spacing will be removed and then titled.
               For example: first_name -> Firstname.

        :param unique: `<bool>` Whether is an `UNIQUE` index. Defaults to `False`.
        :param primary_unique: `<bool>` Whether is the primary `UNIQUE` index in the Table. Defaults to `False`.
            - This is not an acutal MySQL settings, and only affects built-in
              query methods within `Database` and `Table` classes.
            - If `True`, the index will be used by built-in query methods for
              duplication and uniqueness related operations.
            - Each table can only have one 'primary_unique' index. Also, if the
              Table is designed to have indexes, one must be set as the primary.
              (Error checking will be handle by `Table` class).

        ### Example:
        >>> # Create columns
            from mysqlengine import Column, Mysql_Types
            id = Column("id", MysqlTypes.BIGINT(primaryKey=True))
            name = Column("name", MysqlTypes.VARCHAR(255))
            ...

        >>> # Create index
            from mysqlengine import Index
            idx = Index(id, name, unique=True, primary_unique=True)
        >>> <Index (name='uixIdName', syntax='UNIQUE INDEX uixIdName (id, name)')>
        """
        # Initiate variables
        if not unique and primary_unique:
            raise errors.IndexError(
                "<Index> Cannot set 'primary_unique=True` for a non-`UNIQUE` index."
            )
        self._name = name
        self._unique = unique
        self._primary_unique = primary_unique
        # Setup index columns
        self.__setup(columns)

    # Properties ----------------------------------------------------------
    @property
    def name(self) -> str:
        "The name of the index `<str>`."
        return self._name

    @property
    def unique(self) -> bool:
        "Whether the index is `UNIQUE` `<bool`>."
        return self._unique

    @property
    def primary_unique(self) -> bool:
        "Whether is the `primary` `UNIQUE` index for the table `<bool>`."
        return self._primary_unique

    @property
    def columns(self) -> Columns:
        """Access all the `Column` for the index `<Columns>`.
        Works as an immutable dictionary with static typings, where
        keys are the column names and values are the `Column` objects.
        """
        return self._columns

    @property
    def syntax(self) -> str:
        "The `SQL` syntax of the index `<str>`."
        return self._syntax

    # Setup ---------------------------------------------------------------
    def __setup(self, columns: tuple[Column]) -> None:
        # Validate columns
        if not columns:
            raise errors.IndexMetadataError(
                "<{}.metadata> Index requires at least one "
                "Column.".format(self.__class__.__name__)
            )
        if len(columns) > settings.MAX_INDEX_COLUMNS:
            raise errors.IndexMetadataError(
                "<{}.metadata> Index cannot have more than {} columns.".format(
                    self.__class__.__name__, settings.MAX_INDEX_COLUMNS
                )
            )
        _columns: list[Column] = []
        _column_names: list[str] = []
        for col in columns:
            # . validate column data type
            if not isinstance(col, Column):
                raise errors.IndexMetadataError(
                    "<{}.metadata> Index's Column must by instance of "
                    "`<mysqlengine.Column>`, instead of: {} {}".format(
                        self.__class__.__name__, type(col), repr(col)
                    )
                )
            # . check for duplicates
            if col.name in _column_names:
                raise errors.IndexMetadataError(
                    "<{}.metadata> Duplicated Column: '{}'".format(
                        self.__class__.__name__, col.name
                    )
                )
            _columns.append(col)
            _column_names.append(col.name)

        # Setup columns
        self._columns = Columns(*_columns)

        # Auto generate index name
        if not self._name:
            pfix: str = "uix" if self._unique else "idx"
            name: str = "".join([i.replace("_", "").title() for i in _column_names])
            self._name = utils._str_squeeze_spaces(pfix + name, True)
        self._name_lower = self._name.lower()

        # Validate index name
        if self._name_lower in settings.PROHIBIT_NAMES:
            raise errors.IndexMetadataError(
                "<{}.metadata> Index name '{}' is prohibited. If this name is auto "
                "generated `(name=None)`, consider providing a custom name. Else, "
                "please choose a different name or change index columns.".format(
                    self.__class__.__name__, self._name
                )
            )
        if len(self._name) > settings.MAX_NAME_LENGTH:
            raise errors.IndexMetadataError(
                "<{}.metadata> Index name '{}' is too long, If this name is auto "
                "generated `(name=None)`, consider providing a custom name. Else, "
                "please choose a different name or change index columns.".format(
                    self.__class__.__name__, self._name
                )
            )

        # Construct index syntax
        self._syntax = utils._str_squeeze_spaces(
            "%s INDEX %s (%s)"
            % ("UNIQUE" if self._unique else "", self._name, ", ".join(_column_names)),
            True,
        )

    def set_primary(self, primary: bool = True) -> None:
        """Set Index primary `UNIQUE` state.
        :param primary: `<bool>` Whether is the primary `UNIQUE` index for the table.
        """
        if self._unique:
            self._primary_unique = primary

    # Search --------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def _search_by_name(self, name: str, exact: cython.bint) -> list:
        """(cfunc) Search index's columns by name.

        :param name: `<str>` The name or partial name of the desired column(s).
        :param exact: `<bool>` Whether to perform exact match.
            - If `True`, return only the column with the exact provided 'name'.
            - If `False`, return all columns that contain the '(partial) name'.

        :return `<list[Column]>`: Matched Index column(s).
        """
        return self._columns._search_by_name(name, exact)

    def search_by_name(self, name: str, exact: bool = False) -> list[Column]:
        """Search Index's columns by name.

        :param name: `<str>` The name or partial name of the desired column(s).
        :param exact: `<bool>` Whether to perform exact match. Defaults to `False`.
            - If `True`, return only the column with the exact provided 'name'.
            - If `False`, return all columns that contain the '(partial) name'.

        :return `<list[Column]>`: Matched Index column(s).
        """
        return self._columns._search_by_name(name, exact)

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def _search_by_mysql_dtype(self, dtype: str, exact: cython.bint) -> list:
        """(cfunc) Search Index's columns by MySQL data type.

        :param dtype: `<str>` String representation of MySQL data type. e.g. `'BIGINT'`, `'VARCHAR'`, etc.
        :param exact: `<bool>` Whether to perfrom exact match.
            - If `True`, return only columns with the exact provided 'dtype'.
            - If `False`, return all columns with data type containing the provided
              'dtype' string. For example, a 'dtype' of 'INT' will match 'BIGINT'
              if exact_match is `False`.

        :return `<list[Column]>`: Matched Index column(s).
        """
        return self._columns._search_by_mysql_dtype(dtype, exact)

    def search_by_mysql_dtype(self, dtype: str, exact: bool = False) -> list[Column]:
        """Search Index's columns by MySQL data type.

        :param dtype: `<str>` String representation of MySQL data type. e.g. `'BIGINT'`, `'VARCHAR'`, etc.
        :param exact: `<bool>` Whether to perfrom exact match. Defaults to `False`.
            - If `True`, return only columns with the exact provided 'dtype'.
            - If `False`, return all columns with data type containing the provided
              'dtype' string. For example, a 'dtype' of 'INT' will match 'BIGINT'
              if exact_match is `False`.

        :return `<list[Column]>`: Matched Index column(s).
        """
        return self._columns._search_by_mysql_dtype(dtype, exact)

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def _search_by_python_dtype(self, dtypes: tuple) -> list:
        """(cfunc) Search Index's columns by Python data type.

        :param dtypes: `<tuple[type]>` Python data types (not `<str>`). e.g. `int`, `Decimal`, etc.
        :return `<list[Column]>`: All columns that match any of the provided Python data types.
        """
        return self._columns._search_by_python_dtype(dtypes)

    def search_by_python_dtype(self, *dtypes: type) -> list[Column]:
        """Search Index's columns by Python data type.

        :param dtypes: `<type>` Python data types (not `<str>`). e.g. `int`, `Decimal`, etc.
        :return `<list[Column]>`: All columns that match any of the provided Python data types.
        """
        return self._columns._search_by_python_dtype(dtypes)

    # Filter --------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def _issubset(self, columns: tuple) -> cython.bint:
        "(cfunc) Whether the Index columns is subset of the given columns `<bool>`."
        return self._columns._issubset(columns)

    def issubset(self, *columns: Union[str, Column]) -> bool:
        "Whether the Index columns is subset of the given columns `<bool>`."
        return self._columns._issubset(columns)

    # Accessors ------------------------------------------------------------
    def keys(self) -> tuple[str]:
        "Access Index's column names `<tuple[str]>`."
        return self._columns._names

    def values(self) -> tuple[Column]:
        "Access Index's column instances `<tuple[Column]>`."
        return self._columns._instances

    def items(self) -> tuple[tuple[str, Column]]:
        "Access Index's column names and instances `<tuple[tuple[str, Column]]>`."
        return self._columns._items

    def get(self, col: Union[str, Column], default: Any = None) -> Union[Column, Any]:
        "Get Index's column `<Column>`. Return `default` if column does not exist."
        return self._columns._get(col, default)

    # Special Methods -----------------------------------------------------
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
        return self._syntax == __o.syntax if isinstance(__o, Index) else False

    def __bool__(self) -> bool:
        return True

    def __len__(self) -> int:
        return self._columns._length

    def __iter__(self) -> Iterator[Column]:
        return self._columns.__iter__()

    def __getitem__(self, col: object) -> Column:
        try:
            return self._columns._getitem(col)
        except KeyError as err:
            raise errors.ColumnNotExistError(
                "<%s> '%s' does not contain column: %s"
                % (self.__class__.__name__, self._name, repr(col))
            ) from err

    def __contains__(self, col: object) -> bool:
        return self._columns._contains(col)

    def __del__(self):
        self._columns = None


@cython.cclass
class DummyIndex(Index):
    """Represents `None` for an Index."""

    _err_msg: str

    def __init__(self) -> None:
        """Represents an Index that means `None`."""
        super().__init__(Column("DummyColumn", MysqlTypes.BIGINT()))
        self._err_msg: str = "<DummyIndex> Represents `None` for an Index."
        self._name = None
        self._unique = False
        self._primary_unique = False
        self._columns = None
        self._syntax = ""

    # Properties ----------------------------------------------------------
    @property
    def columns(self) -> Columns:
        """Access all the `Column` for the index `<Columns>`.
        Works as an immutable dictionary with static typings, where
        keys are the column names and values are the `Column` objects.
        """
        raise errors.IndexError(self._err_msg)

    @property
    def syntax(self) -> str:
        "The `SQL` syntax of the index `<str>`."
        raise errors.IndexError(self._err_msg)

    # Setup ---------------------------------------------------------------
    def set_primary(self, primary: bool = True) -> None:
        """Set Index primary `UNIQUE` state.
        :param primary: `<bool>` Whether is the primary `UNIQUE` index for the table.
        """
        raise errors.IndexError(self._err_msg)

    # Utils ---------------------------------------------------------------
    def issubset(self, *columns: Union[str, Column]) -> bool:
        "Whether the Index columns is subset of the given columns `<bool>`."
        raise errors.IndexError(self._err_msg)

    # Accessors ------------------------------------------------------------
    def keys(self) -> tuple[str]:
        "Access Index's column names `<tuple[str]>`."
        raise errors.IndexError(self._err_msg)

    def values(self) -> tuple[Column]:
        "Access Index's column instances `<tuple[Column]>`."
        raise errors.IndexError(self._err_msg)

    def items(self) -> tuple[tuple[str, Column]]:
        "Access Index's column names and instances `<tuple[tuple[str, Column]]>`."
        raise errors.IndexError(self._err_msg)

    def get(self, col: Union[str, Column], default: Any = None) -> Union[Column, Any]:
        "Get Index's column `<Column>`. Return `default` if column does not exist."
        raise errors.IndexError(self._err_msg)

    def search(self, name: str) -> list[Column]:
        "Search Index's columns by (partial) name `<list[Column]>`."
        raise errors.IndexError(self._err_msg)

    # Special Methods -----------------------------------------------------
    def __hash__(self) -> int:
        raise errors.IndexError(self._err_msg)

    def __eq__(self, __o: object) -> bool:
        return False

    def __bool__(self) -> bool:
        return False

    def __len__(self) -> int:
        raise errors.IndexError(self._err_msg)

    def __iter__(self) -> Iterator[Column]:
        raise errors.IndexError(self._err_msg)

    def __getitem__(self, col: object) -> Column:
        raise errors.IndexError(self._err_msg)

    def __contains__(self, col: object) -> bool:
        raise errors.IndexError(self._err_msg)


# Indexes ====================================================================================
@cython.cclass
class Indexes:
    """Represents a collection of Indexes.

    Works as an immutable dictionary with static typings, where
    keys are the index names and values are the `Index` instances.
    Meanwhile, it provides search methods to find indexes.
    """

    _dict: dict[str, Index]
    _names: tuple[str]
    _names_set: set[str]
    _instances: tuple[Index]
    _items: tuple[tuple[str, Index]]
    _length: cython.int

    def __init__(self, *indexes: Index) -> None:
        """A collection of Indexes.

        Works as an immutable dictionary with static typings, where
        keys are the index names and values are the `Index` instances.
        Meanwhile, it provides search methods to find indexes.

        :param indexes: `<Index>(s)` Index(es) to add to the collection.
        """
        self.__setup(indexes)

    # Properties -----------------------------------------------------------
    @property
    def names(self) -> tuple[str]:
        "The names of the indexes `<tuple[str]>`."
        return self._names

    @property
    def names_set(self) -> set[str]:
        "The names of the indexes, access in `<set[str]>`."
        return self._names_set

    @property
    def instances(self) -> tuple[Index]:
        "The index instances `<tuple[Index]>`."
        return self._instances

    # Setup ---------------------------------------------------------------
    def __setup(self, indexes: tuple[Index]) -> None:
        # Validate indexes
        _seen: set = set()
        _names: list = []
        _instances: list = []
        _dic: dict = {}
        for idx in indexes:
            if not isinstance(idx, Index):
                raise errors.ColumnError(
                    "<{}> Only accepts instance of <'mysqlenging.Index'>, instead of: "
                    "{} {}".format(self.__class__.__name__, type(idx), repr(idx))
                )
            if isinstance(idx, DummyIndex):
                raise errors.ColumnError(
                    "<{}> `<DummyIndex>` Represents `None` for an index, "
                    "which is prohibited to add to Indexes collection.".format(
                        self.__class__.__name__
                    )
                )
            if idx in _seen:
                raise errors.ColumnError(
                    "<{}> Duplicated index: {}.".format(
                        self.__class__.__name__, repr(idx)
                    )
                )
            if idx.name in _names:
                raise errors.ColumnError(
                    "<{}> Duplicated index name: '{}'.".format(
                        self.__class__.__name__, idx.name
                    )
                )
            _seen.add(idx)
            _names.append(idx.name)
            _instances.append(idx)
            _dic[idx.name] = idx

        # Setup indexes
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
        """(cfunc) Search indexes by name.

        :param name: `<str>` The name or partial name of the desired index(es).
        :param exact: `<bool>` Whether to perform exact match.
            - If `True`, return only the index with the exact provided 'name'.
            - If `False`, return all indexes that contain the '(partial) name'.

        :return `<list[Index]>`: Matched index(es).
        """
        res: list = []
        idx: Index
        if exact:
            for idx in self._instances:
                if idx._name == name:
                    list_append(res, idx)
        else:
            name = name.lower()
            for idx in self._instances:
                if str_contains(idx._name_lower, name):
                    list_append(res, idx)
        return res

    def search_by_name(self, name: str, exact: bool = False) -> list[Index]:
        """Search indexes by name.

        :param name: `<str>` The name or partial name of the desired index(es).
        :param exact: `<bool>` Whether to perform exact match. Defaults to `False`.
            - If `True`, return only the index with the exact provided 'name'.
            - If `False`, return all indexes that contain the '(partial) name'.

        :return `<list[Index]>`: Matched index(es).
        """
        return self._search_by_name(name, exact)

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def _search_by_columns(self, columns: tuple, match_all: cython.bint) -> list:
        """(cfunc) Search indexes by columns.

        :param columns: `<tuple[str, Column]>` Column names or instances.
        :param match_all: `<bool>` Whether to match all columns.
            - If `True`, return indexes where all its columns are a subset
              of the provided 'columns'.
            - If `False`, return indexes if any of its columns matches any
              of the provided 'columns'.

        :return `<list[Index]>`: Matched index(es).
        """
        # Empty columns
        if not columns:
            return []

        # Get column names
        names: set = get_columns_names(columns)

        # Search columns
        res: list = []
        idx: Index
        if match_all:
            for idx in self._instances:
                if idx._columns._names_set <= names:
                    list_append(res, idx)
        else:
            for idx in self._instances:
                if idx._columns._names_set & names:
                    list_append(res, idx)
        return res

    def search_by_columns(
        self,
        *columns: Union[str, Column],
        match_all: bool = False,
    ) -> list[Index]:
        """Search indexes by columns.

        :param columns: `<tuple[str, Column]>` Column names or instances.
        :param match_all: `<bool>` Whether to match all columns. Defaults to `False`.
            - If `True`, return indexes where all its columns are a subset
              of the provided 'columns'.
            - If `False`, return indexes if any of its columns matches any
              of the provided 'columns'.

        :return `<list[Index]>`: Matched index(es).
        """
        return self._search_by_columns(columns, match_all)

    # Filter --------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def _issubset(self, indexes: tuple) -> cython.bint:
        "(cfunc) Whether Indexes collecion is a subset of the given indexes `<bool>`."
        # Empty indexes
        if self._length == 0 or not indexes:
            return False

        # Get index names
        names: set = get_indexes_names(indexes)  # type: ignore

        # Subset comparison
        return True if self._names_set <= names else False

    def issubset(self, *indexes: Union[str, Index]) -> bool:
        "Whether Indexes collecion is a subset of the given indexes `<bool>`."
        return self._issubset(indexes)

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def _filter(self, indexes: tuple) -> list:
        """(cfunc) Filter & sort the given indexes by the Indexes collection.
        Returns the index names that exist in the collection `<list[str]>`.
        """
        # Empty indexes
        if self._length == 0 or not indexes:
            return []

        # Get column names
        names: set = get_indexes_names(indexes)  # type: ignore

        # Filter & sort
        res: list = []
        for name in self._names:
            if set_contains(names, name):
                list_append(res, name)
        return res

    def filter(self, *indexes: Union[str, Index]) -> list[str]:
        """Filter & sort the given indexes by the Indexes collection.
        Returns the index names that exist in the collection `<list[str]>`.
        """
        return self._filter(indexes)

    # Accessors ------------------------------------------------------------
    def keys(self) -> tuple[str]:
        "Access the names of the indexes `<tuple[str]>`."
        return self._names

    def values(self) -> tuple[Index]:
        "Access the index instances `<tuple[Index]>`."
        return self._instances

    def items(self) -> tuple[tuple[str, Index]]:
        "Access the index names and instances `<tuple[tuple[str, Index]]>`."
        return self._items

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def _get(self, idx: object, default: object) -> object:
        "(cfunc) Get index `<Index/Any>`. Return `default` if index does not exist."
        res = dict_get(self._dict, get_index_name(idx))  # type: ignore
        if res == cython.NULL:
            return default
        else:
            return cython.cast(object, res)

    def get(self, idx: Union[str, Index], default: Any = None) -> Union[Index, Any]:
        "Get index `<Index>`. Return `default` if index does not exist."
        return self._get(idx, default)

    # Special Methods ------------------------------------------------------
    def __repr__(self) -> str:
        return "<%s %s>" % (self.__class__.__name__, self._dict.__repr__())

    def __str__(self) -> str:
        return self._dict.__str__()

    def __hash__(self) -> int:
        return self.__repr__().__hash__()

    def __eq__(self, __o: object) -> bool:
        return str(self) == str(__o) if isinstance(__o, Index) else False

    def __bool__(self) -> bool:
        return self._length > 0

    def __len__(self) -> int:
        return self._length

    def __iter__(self) -> Iterator[Index]:
        return self._instances.__iter__()

    @cython.cfunc
    @cython.inline(True)
    def _getitem(self, idx: object) -> Index:
        """(cfunc) Get index `<Index>`. Equivalent to `Indexes[idx]`.
        Raises `IndexNotExistError` if index does not exist.
        """
        res = dict_get(self._dict, get_index_name(idx))  # type: ignore
        if res == cython.NULL:
            raise errors.IndexNotExistError(
                "<%s> Does not contain index: %s" % (self.__class__.__name__, repr(idx))
            )
        return cython.cast(object, res)

    def __getitem__(self, idx: object) -> Index:
        return self._getitem(idx)

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def _contains(self, idx: object) -> cython.bint:
        "(cfunc) Whether contains the index `<bool>`. Equivalent to `idx in Indexes`."
        return set_contains(self._names_set, get_index_name(idx))  # type: ignore

    def __contains__(self, idx: object) -> bool:
        return self._contains(idx)

    def __del__(self):
        self._dict = None
        self._names = None
        self._names_set = None
        self._instances = None
        self._items = None


@cython.cclass
class UniqueIndexes(Indexes):
    """Represents a collection of `UNIQUE` Indexes.

    Works as an immutable dictionary with static typings, where
    keys are the index names and values are the `Index` instances.
    Meanwhile provides distinctive features compared to the `Indexes` class:
    - 1. Only contains `UNIQUE` Indexes.
    - 2. Direct access to the primary `UNIQUE` index (`primary_unique=True`) `<Index>`.
    """

    _primary: Index

    def __init__(self, *indexes: Index) -> None:
        """A collection of `UNIQUE` Indexes.

        Works as an immutable dictionary with static typings, where
        keys are the index names and values are the `Index` instances.
        Meanwhile provides distinctive features compared to the `Indexes` class:
        - 1. Only contains `UNIQUE` Indexes.
        - 2. Direct access to the primary `UNIQUE` index (`primary_unique=True`) `<Index>`.

        :param indexes: `<Index>(s)` Index(es) to add to the collection.
        """
        super().__init__(*indexes)
        self.__setup()

    # Properties -----------------------------------------------------------
    @property
    def primary(self) -> Index:
        """The index with `primary_unique=True` `<Index>`.
        If primary index does not exist, returns `<DummyIndex>`.
        """
        return self._primary

    # Setup ----------------------------------------------------------------
    def __setup(self) -> None:
        # Empty indexes
        if self._length == 0:
            self._primary = DummyIndex()

        # Validate indexes
        _primary: list = []
        _uniques: list = []
        for idx in self._instances:
            # . unique
            if idx.unique:
                list_append(_uniques, idx)
                # . primary unique
                if idx.primary_unique:
                    list_append(_primary, idx)

        # Validate unique
        if len(_primary) > 1:
            raise errors.TableMetadataError(
                "<{}.metadata> Table can only have one index with "
                "`primary_unique=True`.".format(self.__class__.__name__)
            )
        elif len(_primary) == 0:
            if _uniques:
                raise errors.TableMetadataError(
                    "<{}.metadata> For Table designed to have `UNIQUE` indexes, "
                    "must set one as the primary (`primary_unique=True`).".format(
                        self.__class__.__name__
                    )
                )
            else:
                self._primary = DummyIndex()
        else:
            self._primary = _primary[0]

        # Setup indexes
        uniques = Indexes(*_uniques)
        self._dict = uniques._dict
        self._names = uniques._names
        self._names_set = uniques._names_set
        self._instances = uniques._instances
        self._items = uniques._items
        self._length = uniques._length
        del uniques


@cython.cclass
class TableIndexes(Indexes):
    """Represents a collection of Indexes of for `<Table>` class (specific).

    Works as an immutable dictionary with static typings, where
    keys are the index names and values are the `Index` instances.
    Meanwhile provides distinctive features compared to the `Indexes` class:
    - 1. Index validation based on MySQL Table rules.
    - 2. Collection of all `UNIQUE` indexes `<UniqueIndexes>`.
    """

    _uniques: UniqueIndexes
    _syntax: str

    def __init__(self, *indexes: Index) -> None:
        """A collection of Indexes of for `<Table>` class (specific).

        Works as an immutable dictionary with static typings, where
        keys are the index names and values are the `Index` instances.
        Meanwhile provides distinctive features compared to the `Indexes` class:
        - 1. Index validation based on MySQL Table rules.
        - 2. Collection of all `UNIQUE` indexes `<UniqueIndexes>`.

        :param indexes: `<Index>(s)` Index(es) to add to the Table.
        """
        super().__init__(*indexes)
        self.__setup()

    # Properties -----------------------------------------------------------
    @property
    def uniques(self) -> UniqueIndexes:
        "The collection of all `UNIQUE` indexes `<UniqueIndexes>`."
        return self._uniques

    @property
    def syntax(self) -> str:
        "The `SQL` syntax of all indexes `<str>`."
        return self._syntax

    # Setup ----------------------------------------------------------------
    def __setup(self) -> None:
        # Empty indexes
        if self._length == 0:
            self._uniques = UniqueIndexes()
            self._syntax = ""
            return None  # exit

        # Check for indexes limits
        if self._length > settings.MAX_INDEXES:
            raise errors.TableMetadataError(
                "<{}.metadata> Table has too many indexes, must be less "
                "than {}.".format(self.__class__.__name__, settings.MAX_INDEXES)
            )

        # Setup sub-indexes
        self._uniques = UniqueIndexes(*self._instances)

        # Construct syntax
        self._syntax = utils._str_squeeze_spaces(
            ",\n".join([idx.syntax for idx in self._instances]), True
        )

    # Syntax ---------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def _gen_syntax(self, columns: tuple) -> str:
        """(cfunc) Generate the SQL INDEX syntax for the specified columns.

        This method will create the (partial) SQL INDEX syntax based on
        the provided columns. If no columns are given, all indexes will
        be included in the syntax

        :param columns: `<tuple[str/Column]>` The columns for the syntax.
            - If provided, only indexes that are satisfied by the given
              columns will be included in the syntax.
            - If not provided, all indexes will be included in the syntax.

        :return: `<str>` The (partial) SQL INDEX syntax.
        """
        # Full syntax if not specified
        if not columns:
            return self._syntax

        # Get columns names
        names: set = get_columns_names(columns)

        # Filter & sort
        res: list = []
        idx: Index
        for idx in self._instances:
            if idx._columns._names_set <= names:
                list_append(res, idx._syntax)
        return utils._str_squeeze_spaces(",\n".join(res), False)

    def gen_syntax(self, *columns: Union[str, Column]) -> str:
        """Generate the SQL INDEX syntax for the specified columns.

        This method will create the (partial) SQL INDEX syntax based on
        the provided columns. If no columns are given, all indexes will
        be included in the syntax

        :param columns: `<str/Column>` The columns for the syntax.
            - If provided, only indexes that are satisfied by the given
              columns will be included in the syntax.
            - If not provided, all indexes will be included in the syntax.

        :return: `<str>` The (partial) SQL INDEX syntax.
        """
        return self._gen_syntax(columns)

    # Validate -------------------------------------------------------------
    def validate(self, index: Index) -> Index:
        """Validate index for `<Table>` class. `<Index>`.
        Used for adding indexes to the table (internal use only).
        """
        # Check for indexes limits
        if self._length + 1 > settings.MAX_INDEXES:
            raise errors.TableMetadataError(
                "<{}.metadata> Table has too many indexes, "
                "must be less than {}.".format(
                    self.__class__.__name__, settings.MAX_INDEXES
                )
            )

        # Validate data type
        if not isinstance(index, Index):
            raise errors.TableMetadataError(
                "<{}.metadata> Table index must be instance of "
                "<'mysqlenging.Index'>, instead of: {} {}".format(
                    self.__class__.__name__, type(index), repr(index)
                )
            )
        if isinstance(index, DummyIndex):
            raise errors.TableMetadataError(
                "<{}.metadata> Table index cannot be instance of "
                "<'DummyIndex'>, which represents `None` for an index.".format(
                    self.__class__.__name__
                )
            )

        # Check for duplicates
        if index in self._instances:
            raise errors.TableMetadataError(
                "<{}.metadata> Duplicated index: {}.".format(
                    self.__class__.__name__, repr(index)
                )
            )
        if index.name in self._names_set:
            raise errors.TableMetadataError(
                "<{}.metadata> Duplicated index name: '{}'.".format(
                    self.__class__.__name__, repr(index.name)
                )
            )

        # Validate primary unique
        if index.primary_unique and self._uniques._primary:
            raise errors.TableMetadataError(
                "<{}.metadata> Table already has an index with `primary_unique=True`: "
                "{}.".format(self.__class__.__name__, repr(self._uniques._primary))
            )
        if not index.primary_unique and index.unique and not self._uniques:
            raise errors.TableMetadataError(
                "<{}.metadata> Since Table currently does not have a primery unique "
                "index, this index must set as the primary one (`primary_unique=True`): "
                "{}".format(self.__class__.__name__, repr(index))
            )

        # Return index
        return index
