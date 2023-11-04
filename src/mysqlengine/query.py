# cython: language_level=3
# cython: wraparound=False
# cython: boundscheck=False
from __future__ import annotations

# Cython imports
import cython
from cython.cimports import numpy as np  # type: ignore
from cython.cimports.cpython import datetime  # type: ignore
from cython.cimports.cpython.time import time as unixtime  # type: ignore
from cython.cimports.cpython.set import PySet_Add as set_add  # type: ignore
from cython.cimports.cpython.set import PySet_Pop as set_pop  # type: ignore
from cython.cimports.cpython.set import PySet_GET_SIZE as set_len  # type: ignore
from cython.cimports.cpython.set import PySet_Contains as set_contains  # type: ignore
from cython.cimports.cpython.list import PyList_Check as is_list  # type: ignore
from cython.cimports.cpython.list import PyList_GET_SIZE as list_len  # type: ignore
from cython.cimports.cpython.list import PyList_Append as list_append  # type: ignore
from cython.cimports.cpython.list import PyList_Insert as list_insert  # type: ignore
from cython.cimports.cpython.tuple import PyTuple_Check as is_tuple  # type: ignore
from cython.cimports.cpython.tuple import PyTuple_GET_ITEM as tuple_getitem  # type: ignore
from cython.cimports.cpython.dict import PyDict_Check as is_dict  # type: ignore
from cython.cimports.cpython.dict import PyDict_Size as dict_len  # type: ignore
from cython.cimports.cpython.dict import PyDict_Keys as dict_keys  # type: ignore
from cython.cimports.cpython.dict import PyDict_Items as dict_items  # type: ignore
from cython.cimports.cpython.dict import PyDict_Values as dict_values  # type: ignore
from cython.cimports.cpython.dict import PyDict_SetItem as dict_setitem  # type: ignore
from cython.cimports.cpython.dict import PyDict_DelItem as dict_delitem  # type: ignore
from cython.cimports.cpython.dict import PyDict_Contains as dict_contains  # type: ignore
from cython.cimports.cpython.int import PyInt_Check as is_int  # type: ignore
from cython.cimports.cpython.string import PyString_Check as is_str  # type: ignore
from cython.cimports.cpython.unicode import PyUnicode_Replace as uni_replace  # type: ignore
from cython.cimports.cpython.unicode import PyUnicode_Contains as str_contains  # type: ignore
from cython.cimports.cytimes.pydt import pydt  # type: ignore
from cython.cimports.mysqlengine import errors, settings, utils  # type: ignore
from cython.cimports.mysqlengine.connection import Connection, MULTIROWS_ARGS_DTYPES  # type: ignore
from cython.cimports.mysqlengine.index import Index, access_index_name, access_index_syntax  # type: ignore
from cython.cimports.mysqlengine.column import access_column_mysql, access_column_dtype  # type: ignore
from cython.cimports.mysqlengine.column import Column, access_column_name, access_column_syntax  # type: ignore

np.import_array()
datetime.import_datetime()

# Python imports
import datetime
from uuid import uuid4
from decimal import Decimal
from time import struct_time
from asyncio import gather, Semaphore
from re import compile, Pattern, IGNORECASE
from _collections_abc import dict_values as dict_val, dict_keys as dict_key
from typing import Self, Any, Union, Literal, Callable, TYPE_CHECKING, overload
import numpy as np, pandas as pd
from pandas import DataFrame, Series, concat
from cytimes.pydt import pydt
from mysqlengine.logs import logger
from mysqlengine.index import Index
from mysqlengine.column import Column
from mysqlengine import errors, settings, utils
from mysqlengine.errors import query_exc_handler
from mysqlengine.connection import Connection
from mysqlengine.connection import Cursor, DictCursor, DfCursor
from mysqlengine.connection import SSCursor, SSDictCursor, SSDfCursor

# Type checking
if TYPE_CHECKING:
    from cython.cimports.mysqlengine.database import Database, Table, TimeTable  # type: ignore
    from mysqlengine.database import Database, Table, TimeTable

__all__ = [
    "SelectQuery",
    "InsertQuery",
    "ReplaceQuery",
    "UpdateQuery",
    "DeleteQuery",
    "CreateTempQuery",
    "CompareQuery",
]


# Constants =====================================================================================
# Order of the query clauses
SELECT_CLAUSES: dict[str, int] = {
    "SELECT": 0,
    "JOIN": 1,
    "WHERE": 2,
    "GROUP BY": 3,
    "HAVING": 4,
    "ORDER BY": 5,
    "LIMIT": 6,
    "FOR UPDATE": 7,
    "LOCK IN SHARE MODE": 8,
}
INSERT_CLAUSES: dict[str, int] = {
    "INSERT": 0,
    "REPLACE": 1,
    "VALUES": 2,
    "ON DUPLICATE KEY": 3,
}
UPDATE_CLAUSES: dict[str, int] = {
    "UPDATE": 0,
    "JOIN": 1,
    "SET": 2,
    "WHERE": 3,
    "ORDER BY": 4,
    "LIMIT": 5,
}
DELETE_CLAUSES: dict[str, int] = {
    "DELETE": 0,
    "JOIN": 1,
    "WHERE": 2,
    "ORDER BY": 3,
    "LIMIT": 4,
}
# Clause constrains
TIME_CLAUSE_COLUMN_TYPES: set[str] = {"DATE", "DATETIME", "TIMESTAMP"}
JOIN_CLAUSE_METHODS: set[str] = {"INNER", "LEFT", "RIGHT"}
# Value clause data types
VALUES_CLAUSE_ITEM: set[type] = {
    bool,
    np.bool_,
    int,
    np.int_,
    np.int8,
    np.int16,
    np.int32,
    np.int64,
    np.uint,
    np.uint16,
    np.uint32,
    np.uint64,
    float,
    np.float_,
    np.float16,
    np.float32,
    np.float64,
    Decimal,
    str,
    bytes,
    bytearray,
    datetime.date,
    datetime.datetime,
    pd.Timestamp,
    np.datetime64,
    struct_time,
    datetime.time,
    datetime.timedelta,
    pd.Timedelta,
    np.timedelta64,
    type(None),
}
VALUES_CLAUSE_NEST_SEQU: set[type] = {
    list,
    tuple,
    dict_val,
    np.ndarray,
}
VALUES_CLAUSE_FLAT_SEQU: set[type] = {
    dict_key,
    np.record,
    pd.DatetimeIndex,
    pd.TimedeltaIndex,
    pd.Series,
}
# Regular expressions
ON_DUPLICATE_VALUES_FUNC_RE: Pattern = compile(
    r"\s*(.*?\s*=\s*VALUES\(.*?\))", IGNORECASE
)
# Compare query special columns
UNIQUE_COLUMN: str = "#_$_unique_$_#"
COMPARE_COLUMN: str = "#_$_compare_$_#"
SUBTABLE_COLUMN: str = "#_$_sub-table_$_#"
# Join symbol
JOIN_SYM: str = "@_$_join-sym_$_@"


# Base Clause ===================================================================================
@cython.cclass
class Clause:
    """The base clause of the MySQL query."""

    # Query
    _query: Query
    _db: Database
    _tb: Table
    # Clause
    _name: str
    _index: cython.int
    _alias: str
    # Mode
    _mode: cython.int
    """
    - 1. Normal table
    - 2. TimeTable specified
    - 3. TimeTable placeholder
    - 4. Subquery placeholder
    - 5. Custom table SQL
    """
    _tb_name: str
    _tb_time: datetime.datetime
    _subquery_key: str
    _custom_val: str

    def __init__(
        self,
        query: Query,
        name: str,
        tabletime: Union[str, datetime.date, datetime.datetime, None] = None,
        anchor: cython.bint = False,
        alias: Union[str, None] = None,
    ) -> None:
        """Represents a clause of the MySQL query.

        :param query: `<Query>` The query of the clause.
        :param name: `<str>` The name of the clause. e.g. `'SELECT'`, `'FROM'`, `'WHERE'`.
        :param tabletime: `<str/None>` A specific `tabletime` for the clause. Defaults to `None`.
        :param anchor: `<bool>` Whether to increase query anchor count. Defaults to `False`.
            - If `anchor=True`, the anchor count of the query increases by 1.
            - Anchor count is used to derive the default alias of the cluase, which equals
              to 't' followed by the anchor count (e.g. 't1', 't2', 't3' etc.).

        :param alias: `<str/None>` The alias of the clause. Defaults to `None`.
            - The alias of the clause will be added to the corresponding part of the SQL
              statement using the `'AS <alias>'` syntax.
            - For instance, in a `SELECT` query without specified alias (defualt alias),
              the statement would be constructed as: `'SELECT ... FROM ... AS t1'`. However,
              with a user-defined  alias (for example, `alias='tb'`), the statement would
              be constructed as: `'SELECT ... FROM ... AS tb'`.
            - Although this aliasing behavior might not be necessary for certain SQL
              statements, this module enforces it to ensure all built-in methods can be
              used interchangeably.
        """
        # Query
        self._query = query
        self._db = query._db
        self._tb = query._tb
        # Clause
        self._name = name
        self._query._add_clause(self, anchor)
        if alias:
            self._alias = alias
        elif anchor:
            self._alias = self._query._gen_default_alias()
        else:
            self._alias = None
        self._index = list_len(self._query._clauses) - 1
        # Mode
        self._mode = 0
        self._tb_name = None
        self._tb_time = self.parse_tabletime(tabletime)
        self._subquery_key = None
        self._custom_val = None

    # Properties ---------------------------------------------------------------------------------
    @property
    def name(self) -> str:
        "The name of the clause `<str>`."
        return self._name

    @property
    def index(self) -> str:
        "The index of the clause relative to the query `<int>`."
        return self._index

    @property
    def alias(self) -> str:
        "Alias of the clause `<str>`."
        return self._alias

    @property
    def syntax(self) -> str:
        "The syntax of the clause `<str>`."
        raise NotImplementedError(
            "{}Syntax of the clause is not implemented.".format(self.err_pfix())
        )

    # Settings -----------------------------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    def set_mode(self, obj: object):
        "(cfunc) Set mode of the clause."
        # Main table mode
        if obj is None:
            self.set_to_table_mode()

        # Subquery mode
        elif isinstance(obj, SelectQuery):
            self._subquery_key = self.add_subquery(obj, 1)
            self._mode = 4

        # Custom table mode
        else:
            tb = self._db.tables.get(obj)
            if tb is not None:
                self._tb = tb
                self.set_to_table_mode()
            elif isinstance(obj, str):
                self._custom_val = obj
                self._mode = 5
            else:
                raise errors.QueryValueError(
                    "{}Argument {} is invalid.".format(self.err_pfix(), repr(obj))
                )

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def set_to_table_mode(self):
        "(cfunc) Set clause as table mode."
        # Already set
        if self._mode != 0:
            return None

        # Determine table name
        if self._tb._is_timetable:
            if self._tb_time is None:
                self._mode = 3
                self._tb_name = self._tb._gen_pname()
                dict_setitem(
                    self._query._timetables,
                    self._tb_name,
                    self._tb,
                )
                dict_setitem(
                    self._query._timetable_units,
                    self.access_timetable_unit(self._tb),
                    self._index,
                )
                if not self._query._query_timetables:
                    self._query._query_timetables = True
                if not self._query._select_timetables and dict_contains(
                    SELECT_CLAUSES, self._name
                ):
                    self._query._select_timetables = True
            else:
                self._mode = 2
                self._tb_name = self.parse_timetable_name(self._tb, self._tb_time)
            dict_setitem(
                self._query._timetable_fmts,
                self._tb._columns._tabletime._name,
                self.access_timetable_fmt(self._tb),
            )
        else:
            self._mode = 1
            self._tb_name = self._tb._name
        return self._tb_name

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def set_union_select_clause(self, clause: Clause):
        "(cfunc) Set union select clause."
        list_append(self._query._union_select_clauses, clause)
        if not self._query._force_union_mode:
            self._query._force_union_mode = True
        if not self._query._has_union_clause:
            self._query._has_union_clause = True

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def add_subquery(self, query: SelectQuery, tier: cython.int) -> str:
        """(cfunc) Add a subquery to the clause.

        This method returns a unique placeholder, which should be substituted
        with the corresponding subquery at `construct()/execute()` stage.

        :param query: `<SelectQuery`> The subquery to be set.
        :param tier: `<int>` The embeded tier of the subquery relative to the main query.
        :return: `<str>` The unique placeholder for the subquery.
        """
        phl = "$_SUBQUERY_%s_$" % uuid4()
        query._tier = tier
        query._is_subquery = True
        query._select._explain = False
        query._select._buffer_result = False
        self._query._subqueries[phl] = query
        return phl

    # Utils --------------------------------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def err_pfix(self) -> str:
        "(cfunc) Error prefix `<str>`."
        if self._query._is_subquery:
            return "<%s> [Subquery Select] Clause No.%d `%s`\nError: " % (
                self._tb._fname,
                self._index + 1,
                self._name,
            )
        else:
            return "<%s> [%s] Clause No.%d `%s`\nError: " % (
                self._tb._fname,
                self._query._name,
                self._index + 1,
                self._name,
            )

    @cython.cfunc
    @cython.inline(True)
    def parse_column_names(self, columns: object) -> list:
        "(cfunc) Parse column names `<list[str]>`."
        # `<str>` column
        if is_str(columns):
            return [columns]
        # `<Column>` instance
        elif isinstance(columns, Column):
            return [access_column_name(columns)]
        # Sequence of columns
        else:
            return self.parse_sequ_of_column_names(columns)

    @cython.cfunc
    @cython.inline(True)
    def parse_sequ_of_column_names(self, columns: object) -> list:
        "(cfunc) Parse sequence of column names `<list[str]>`."
        try:
            res: list = []
            for col in columns:
                if is_str(col):
                    list_append(res, col)
                elif isinstance(col, Column):
                    list_append(res, access_column_name(col))
                elif is_list(col) or is_tuple(col):
                    res += self.parse_sequ_of_column_names(col)
                else:
                    raise errors.QueryValueError(
                        "{}Invalid column {}, only accepts column name `<str>` or "
                        "instance of `<Column>`.".format(self.err_pfix(), repr(col))
                    )
        except errors.QueryValueError:
            raise
        except Exception as err:
            raise errors.QueryValueError(
                "{}Invalid 'columns': {}".format(self.err_pfix(), err)
            ) from err
        return res

    @cython.cfunc
    @cython.inline(True)
    def parse_tabletime(self, time: object) -> datetime.datetime:
        "(cfunc) Parse 'tabletime' argument."
        if time is None:
            return None
        try:
            return pydt(time)._dt
        except Exception as err:
            raise errors.QueryValueError(
                "{}Invalid 'tabletime', can't convert to `datetime`"
                ": {}".format(self.err_pfix(), err)
            ) from err

    @cython.cfunc
    @cython.inline(True)
    def parse_timetable_name(self, tb: TimeTable, time: object) -> str:
        "(cfunc) Parse TimeTable name `<str>`."
        return tb._get_name(time)

    @cython.cfunc
    @cython.inline(True)
    def parse_timetable_names(self, tb: TimeTable, times: Series) -> object:
        "(cfunc) Parse TimeTable names `<Series[str]>`."
        return tb._get_names_series(times)

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def access_timetable_unit(self, tb: TimeTable) -> str:
        "(cfunc) Access TimeTable unit."
        return tb._time_unit

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def access_timetable_fmt(self, tb: TimeTable) -> str:
        "(cfunc) Access TimeTable format."
        return tb._time_format

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def prefix_alias(self, column: str) -> str:
        """(cfunc) Prefixes the clause alias to the column name if applicable.

        The alias is prefixed to the column name only if the clause
        has a valid alias and the column name does not contain a dot `"."`.
        If these conditions are not met, the original column name is
        returned without any modifications.
        """
        if self._alias and not str_contains(column, "."):
            return self._alias + "." + column
        else:
            return column

    @cython.cfunc
    @cython.inline(True)
    def escape_item(self, item: object) -> str:
        "(cfunc) Escape item to Literal string `<str>`."
        return self._db._escape_item(item)

    @cython.cfunc
    @cython.inline(True)
    def escape_args(self, args: object) -> object:
        """(cfunc) Escape arguments to literal `<tuple/dict>`.

        - If the given 'args' is type of `<dict>`, returns `<dict>`.
        - All other supported data types returns `<tuple>`.
        """
        return self._db._escape_args(args)

    # Special methods ----------------------------------------------------------------------------
    def __repr__(self) -> str:
        return "<Clause %s>" % self._name

    def __str__(self) -> str:
        return self._name

    def __hash__(self) -> int:
        return hash(self._name)

    def __eq__(self, __o: object) -> bool:
        return self._name == __o


@cython.cclass
class InsertClause(Clause):
    """The base `INSERT` clause of the MySQL query."""

    _columns: list[str]
    _columns_syntax: str
    _ignore: cython.bint

    def __init__(
        self,
        query: Query,
        name: str,
        *columns: Union[str, Column],
        ignore: cython.bint = False,
        tabletime: Union[str, datetime.date, datetime.datetime, None] = None,
    ) -> None:
        """Represent an `INSERT` clause of the MySQL query.

        :param query: `<Query>` The query of the clause.
        :param name: `<str>` The name of the clause. e.g. `'INSERT'`, `'REPLACE'`.
        :param columns: `<str/Column>` The columns targeted by the insert operation, accept both column names `<str>` and instances `<Column>`.
        :param ignore: `<bool>` The `IGNORE` modifier. Defaults to `False`.
            Determines whether to ignore the duplicate key errors.

        :param tabletime: `<str/None>` A specific `tabletime` for the `INSERT` table. Defaults to `None`.
            - This parameter is only applicable when the `INSERT` table corresponds
              to a TimeTable.
            - If `tabletime` is specified, the actual sub-table will derive from this
              parameter. Otherwise, it is required to use `tabletimes()` method to specify
              the sub-tables. For more details, please refer to the `TABLETIMES` clause.
        """
        super().__init__(query, name, tabletime=tabletime, anchor=False, alias=None)
        self._ignore = ignore
        self._construct(columns)
        self.set_mode(None)
        self._query._set_insert(self)

    # Properties ---------------------------------------------------------------------------------
    @property
    def syntax(self) -> str:
        """The `INSERT/REPLACE` syntax `<str>`."""
        return "%s%s INTO %s.%s %s" % (
            # Clause
            self._name,
            # Modifier - IGNORE
            " IGNORE" if self._ignore else "",
            # Table
            self._db._name,
            self._tb_name,
            # Columns
            self._columns_syntax,
        )

    # Construct ----------------------------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    def _construct(self, columns: tuple):
        # Columns not specified
        if not columns:
            # . defaults to table non-auto increment columns
            self._columns = list(self._tb._columns._non_auto_increments._names)

        # Columns provided
        else:
            # . filter out columns not belong to the table
            cols = self.parse_sequ_of_column_names(columns)
            self._columns = self._tb._columns._filter(tuple(cols))

            # . validate tabletime for TimeTable
            if (
                self._tb._is_timetable
                and self._tb._columns._tabletime._name not in self._columns
            ):
                raise errors.QueryValueError(
                    "{}Table '{}' is a TimeTable, it is required for 'columns' argument "
                    "to include the `tabletime` column '{}'. Built-in query methods depends "
                    "on this column to determine which sub-tables the data belongs to.".format(
                        self.err_pfix(),
                        self._tb._fname,
                        self._tb._columns._tabletime._name,
                    )
                )

        # Validate columns
        if not self._columns:
            raise errors.QueryValueError(
                "{}Invalid 'columns': No valid columns "
                "provided.".format(self.err_pfix())
            )

        # Columns syntax
        length: cython.int = list_len(self._columns)
        if length > 3:
            syntax = "(\n\t%s\n)" % ",\n\t".join(self._columns)
        else:
            syntax = "(%s)" % ", ".join(self._columns)
        self._columns_syntax = syntax


@cython.cclass
class ConditionClause(Clause):
    """The base conditional clause of the MySQL query."""

    _conditions: list[str]

    def __init__(
        self,
        query: Query,
        name: str,
        *conditions: str,
        args: object = None,
        ins: Union[dict[str, Union[list, tuple]], None] = None,
        not_ins: Union[dict[str, Union[list, tuple]], None] = None,
        subqueries: Union[dict[str, SelectQuery], None] = None,
        tabletime: Union[str, datetime.date, datetime.datetime, None] = None,
        anchor: cython.bint = False,
        alias: Union[str, None] = None,
    ) -> None:
        """Represents a conditional clause of the MySQL query.

        :param query: `<Query>` The query of the clause.
        :param name: `<str>` The name of the clause. e.g. `'WHERE'`, `'JOIN'`, `'HAVING'`.
        :param conditions: `<str>` Condition expressions: `"id = 1"`, `"name = 'John'"`, `"COUNT(*) > 10"`, etc.
        :param args: `<Any/None>` Arguments for the `'%s'` placeholders of 'conditions'. Defaults to `None`.
        :param ins: `<dict/None>` The `IN` modifier. Defaults to `None`.
            - This parameter must be a dictionary with the column name as the key and
              iterable types such as `list`, `tuple`, `set`, `Series` as the value.
            - For example, `{"id": [1, 2, 3]}` -> `"id IN (1, 2, 3)"`. Duplicates
              are removed automatically.

        :param not_ins: `<dict/None>` The `NOT IN` modifier. Defaults to `None`.
            - Refert to 'ins' argument for more detail.

        :param subqueries: `<dict/None>` The subquery conditions. Defaults to `None`.
            - This parameter must be a dictionary with the column name along with
              the `desired operator` as the key and instances of `SelectQuery`
              as the value.

        :param tabletime: `<str/None>` A specific `tabletime` for the clause. Defaults to `None`.

        :param anchor: `<bool>` Whether to increase query anchor count. Defaults to `False`.
            - If `anchor=True`, the anchor count of the query increases by 1.
            - Anchor count is used to derive the default alias of the cluase, which equals
              to 't' followed by the anchor count (e.g. 't1', 't2', 't3' etc.).

        :param alias: `<str/None>` The alias of the clause. Defaults to `None`.
            - The actual alias of the clause should be implemented in subclasses.
        """
        super().__init__(query, name, tabletime=tabletime, anchor=anchor, alias=alias)
        self._construct(conditions, args, ins, not_ins, subqueries)

    # Properties ---------------------------------------------------------------------------------
    @property
    def syntax(self) -> str:
        "The syntax of the clause `<str>`."
        conditions = self.gen_conditions()
        length: cython.int = list_len(conditions)
        if length > 1:
            return self._name + " " + "\n\tAND ".join(conditions)
        elif length == 1:
            return self._name + " " + conditions[0]
        else:
            return ""

    @cython.cfunc
    @cython.inline(True)
    def gen_conditions(self) -> list:
        "(cfunc) Generate conditions `<list[str]>."
        # Base conditions
        conditions: list = self._conditions.copy()

        # Time conditions
        time_clause = self._query._get_timeclause(self._index + 1)
        if time_clause is not None:
            conditions += time_clause.gen_conditions()

        # Return conditions
        return conditions

    @property
    def conditions(self) -> list[str]:
        "All conditions of the cluase, along with all the trailing `TIME` conditions `<list[str]>`."
        return self.gen_conditions()

    # Construct ----------------------------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    def _construct(
        self,
        conditions: tuple,
        args: object,
        ins: object,
        not_ins: object,
        subqueries: object,
    ):
        # Arguments
        args = tuple() if args is None else self.escape_args(args)

        # Conditions
        if conditions:
            try:
                concat: str = JOIN_SYM.join(conditions)
            except Exception as err:
                raise errors.QueryValueError(
                    "{}Invalid 'conditions': {}".format(self.err_pfix(), err)
                )
            try:
                concat = concat % args
            except Exception as err:
                raise errors.QueryValueError(
                    "{}Invalid 'args': {}".format(self.err_pfix(), err)
                ) from err
            self._conditions = concat.split(JOIN_SYM)
        else:
            self._conditions = []

        # IN modifier
        if is_dict(ins):
            try:
                for col, val in dict_items(ins):
                    if set_contains(MULTIROWS_ARGS_DTYPES, type(val)):
                        sql = str(col) + " IN " + self.escape_item(set(val))
                    else:
                        sql = str(col) + " = " + self.escape_item(val)
                    list_append(self._conditions, sql)
            except Exception as err:
                raise errors.QueryValueError(
                    "{}Invalid 'ins': {}".format(self.err_pfix(), err)
                ) from err

        # NOT IN modifier
        if is_dict(not_ins):
            try:
                for col, val in dict_items(not_ins):
                    if set_contains(MULTIROWS_ARGS_DTYPES, type(val)):
                        sql = str(col) + " NOT IN " + self.escape_item(set(val))
                    else:
                        sql = str(col) + " != " + self.escape_item(val)
                    list_append(self._conditions, sql)
            except Exception as err:
                raise errors.QueryValueError(
                    "{}Invalid 'not_ins': {}".format(self.err_pfix(), err)
                ) from err

        # Subqueries
        if is_dict(subqueries):
            try:
                for col, query in dict_items(subqueries):
                    sql = str(col) + " " + self.add_subquery(query, 2)
                    list_append(self._conditions, sql)
            except Exception as err:
                raise errors.QueryValueError(
                    "{}Invalid 'subqueries': {}".format(self.err_pfix(), err)
                ) from err


@cython.cclass
class JoinClause(ConditionClause):
    """The base `JOIN` clause of the MySQL query."""

    _method: str

    def __init__(
        self,
        query: Query,
        table: Union[str, Table, TimeTable, SelectQuery],
        *ons: str,
        args: object = None,
        ins: Union[dict[str, Union[list, tuple]], None] = None,
        not_ins: Union[dict[str, Union[list, tuple]], None] = None,
        subqueries: Union[dict[str, SelectQuery], None] = None,
        method: Literal["INNER", "LEFT", "RIGHT"] = "INNER",
        tabletime: Union[str, datetime.date, datetime.datetime, None] = None,
        alias: Union[str, None] = None,
    ) -> None:
        """Represents a `JOIN` clause of the MySQL query.

        :param query: `<Query>` The query of the clause.
        :param table: The table to join. This can be specified in two ways:
            - By providing the name `<str>` or instance `<Table/TimeTable>` of the
              table to join. This is equivalent to `"JOIN <table> ..."`.
            - By providing an instance of `SelectQuery` as a subquery. This will
              turn the statement following the `JOIN` clause into a subquery. This
              is equivalent to `"JOIN (SELECT... FROM ... )"`.

        :param ons: `<str>` Condition expressions: `"t1.id = t2.id"`, `"t2.age > 23"`, etc.
            - Each column should manually prefixed with the correct alias `(t1, t2)`.
              For more details about aliasing, please refer to the `alias` parameter.

        :param args: `<Any/None>` Arguments for the `'%s'` placeholders of 'ons'. Defaults to `None`.

        :param ins: `<dict/None>` The `IN` modifier. Defaults to `None`.
            - This parameter must be a dictionary with the column name as the key and
              iterable types such as `list`, `tuple`, `set`, `Series` as the value.
            - For example, `{"t2.id": [1, 2, 3]}` -> `"t2.id IN (1, 2, 3)"`. Duplicates
              are removed automatically.

        :param not_ins: `<dict/None>` The `NOT IN` modifier. Defaults to `None`.
            - Refert to 'ins' argument for more detail.

        :param subqueries: `<dict/None>` The subquery conditions. Defaults to `None`.
            - This parameter must be a dictionary with the column name along with
              the `desired operator` as the key and instances of `SelectQuery`
              as the value.
            - For example: `{"t2.name =": db.user.select("name").where("age > 23")}` ->
              `"t2.name = (SELECT name FROM db.user AS t1 WHERE age > 23)"`.

        :param method: `<str>` The join method. Defaults to `'INNER'`.

        :param tabletime: `<str/None>` A specific `tabletime` for the `JOIN` table. Defaults to `None`.
            - This parameter is only applicable when the argument 'table' corresponds
              to a TimeTable (regardless type of `<str>` or `<TimeTable>` instance).
            - If `tabletime` is specified, the actual sub-table will derive from this
              parameter. Otherwise, it is required to use `tabletimes()` method to specify
              the sub-tables. For more details, please refer to the `TABLETIMES` clause.

        :param alias: `<str/None>` The alias of the `JOIN` clause. Defaults to `None`.
            - The alias of the clause will be added to the corresponding part of the SQL
              statement using the `'AS <alias>'` syntax.
            - For instance, in a `SELECT... FROM ... JOIN ...` query, without specified
              alias (default alias), the statement would be constructed as:
              `'SELECT ... FROM ... AS t1 JOIN ... AS t2'`, where default alias is derived
              from the order of the tables in the query.
            - However, with a user-defined alias (for example, `alias='join_tb'`), the
              statement would be constructed as:
              `'SELECT ... FROM ... AS t1 JOIN ... AS join_tb'`.
        """
        super().__init__(
            query,
            "JOIN",
            *ons,
            args=args,
            ins=ins,
            not_ins=not_ins,
            subqueries=subqueries,
            tabletime=tabletime,
            anchor=True,
            alias=alias,
        )
        # ON conditions
        if not ons:
            raise errors.QueryValueError(
                "{}Invalid 'ons', requires at least one `JOIN ON` "
                "condition.".format(self.err_pfix())
            )
        # Method
        if not set_contains(JOIN_CLAUSE_METHODS, method):
            raise errors.QueryValueError(
                "{}Invalid JOIN method {}, accpets: {}.".format(
                    self.err_pfix(),
                    repr(method),
                    ", ".join(map(repr, JOIN_CLAUSE_METHODS)),
                )
            )
        self._method = method
        # Query
        list_append(self._query._join_clauses, self)
        # Set mode
        try:
            if table is None:
                raise ValueError(repr(table))
            self.set_mode(table)
        except Exception as err:
            raise errors.QueryValueError(
                "{}Invalid 'table': {}".format(self.err_pfix(), err)
            ) from err
        if self._mode == 3:
            dict_setitem(self._query._timetable_joins, method, self._index)

    # Properties ---------------------------------------------------------------------------------
    @property
    def syntax(self) -> str:
        "The `'JOIN...'` syntax `<str>`."
        # ON syntax
        conditions: list = self.gen_conditions()
        length: cython.int = list_len(conditions)
        if length > 1:
            ons = "\n\tON " + "\n\tAND ".join(conditions)
        elif length == 1:
            ons = "\n\tON " + conditions[0]
        else:
            ons = ""

        # Table mode
        if 1 <= self._mode <= 3:
            return "%s JOIN %s.%s AS %s%s" % (
                self._method,
                self._db._name,
                self._tb_name,
                self._alias,
                ons,
            )
        # Subquery mode
        elif self._mode == 4:
            return "%s JOIN %s AS %s%s" % (
                self._method,
                self._subquery_key,
                self._alias,
                ons,
            )
        # Custom mode
        elif self._mode == 5:
            return "%s JOIN %s AS %s%s" % (
                self._method,
                self._custom_val,
                self._alias,
                ons,
            )
        # Invalid mode
        else:
            raise errors.QueryValueError(
                "{}Clause mode not set: {}".format(self.err_pfix(), self._mode)
            )


@cython.cclass
class WhereClause(ConditionClause):
    """The base `WHERE` clause of the MySQL query."""

    def __init__(
        self,
        query: Query,
        *conditions: str,
        args: object = None,
        ins: Union[dict[str, Union[list, tuple]], None] = None,
        not_ins: Union[dict[str, Union[list, tuple]], None] = None,
        subqueries: Union[dict[str, SelectQuery], None] = None,
        alias: Union[str, None] = None,
    ) -> None:
        """Represents a `WHERE` clause of the MySQL query.

        :param query: `<Query>` The query of the clause.
        :param conditions: `<str>` Condition expressions: `"id = 1"`, `"name = 'John'"`, `"COUNT(*) > 10"`, etc.
        :param args: `<Any/None>` Arguments for the `'%s'` placeholders of 'conditions'. Defaults to `None`.
        :param ins: `<dict/None>` The `IN` modifier. Defaults to `None`.
            - This parameter must be a dictionary with the column name as the key and
              iterable types such as `list`, `tuple`, `set`, `Series` as the value.
            - For example, `{"id": [1, 2, 3]}` -> `"id IN (1, 2, 3)"`. Duplicates
              are removed automatically.

        :param not_ins: `<dict/None>` The `NOT IN` modifier. Defaults to `None`.
            - Refert to 'ins' argument for more detail.

        :param subqueries: `<dict/None>` The subquery conditions. Defaults to `None`.
            - This parameter must be a dictionary with the column name along with
              the `desired operator` as the key and instances of `SelectQuery`
              as the value.

        :param alias: `<str/None>` The alias of the clause. Defaults to `None`.
            - The actual alias of the clause should be implemented in subclasses.
        """
        super().__init__(
            query,
            "WHERE",
            *conditions,
            args=args,
            ins=ins,
            not_ins=not_ins,
            subqueries=subqueries,
            tabletime=None,
            anchor=False,
            alias=alias,
        )
        # Query
        list_append(self._query._where_clauses, self)

    # Properties ---------------------------------------------------------------------------------
    @property
    def syntax(self) -> str:
        "The syntax of the clause `<str>`."
        conditions = self.gen_conditions()
        length: cython.int = list_len(conditions)
        if length > 1:
            return self._name + " " + "\n\tAND ".join(conditions)
        elif length == 1:
            return self._name + " " + conditions[0]
        else:
            raise errors.QueryValueError(
                "{}Requires at least one condition.".format(self.err_pfix())
            )


@cython.cclass
class TimeClause(Clause):
    """The base `TIME` clause of the MySQL query.

    `TIME` clause is not a legit SQL clause but a custom design to
    facilitate construct of time range conditions. For instance, `TIME`
    can be used to construct the following syntax:
    `WHERE <time_column> BETWEEN '2020-01-01' AND '2020-01-31'`.
    """

    _syntax: str

    def __init__(
        self,
        query: Query,
        name: str,
        parent: str,
        column: Union[str, Column],
        start: Union[str, datetime.date, datetime.datetime, None] = None,
        end: Union[str, datetime.date, datetime.datetime, None] = None,
        days: Union[int, None] = None,
        unit: Literal["Y", "M", "D", "W", "h", "m", "s", "us"] = "us",
    ) -> None:
        """Represents a `TIME` clause of the MySQL query.

        `TIME` clause is not a legit SQL clause but a custom design to
        facilitate construct of time range conditions. For instance, `TIME`
        can be used to construct the following syntax:
        `WHERE <time_column> BETWEEN '2020-01-01' AND '2020-01-31'`.

        :param query: `<Query>` The query of the clause.
        :param name: `<str>` The name of the clause. e.g. `'WHERE_TIME'`, `'JOIN_TIME'`.
        :param parent: `<str>` The name of the parent clause. e.g. `'WHERE'`, `'JOIN'`.
            - A `TIME` clause can only be used after a parent `CONDITION` clause
            or another `TIME` clause. This argument ensures the `TIME` clause is
            placed at the correct position of the query.
            - For example: the 'parent' for `WHERE_TIME` clause should be `WHERE` clause,
            and the 'parent' for `JOIN_TIME` clause should be `JOIN` clause.

        :param column: `<str/Column>` The name or instance of the column the time condition applies to.
            - Accepts either a `str` or a `Column` instance. This clause only
            supports `DATE`, `DATETIME`, `TIMESTAMP` MySQL column types.
            - Unlike other clauses, this clause automatically prefixes the appropriate
            alias to the column, eliminating the need for manual aliasing.

        :param start, end, days: The time span parameters.
            - If 'start' and 'end' are specified -> constrain between [start, ... end].
            - If 'start' and 'days' are specified -> constrain between [start, ... start + days - 1]
            - If only 'start' is specified -> constrain between [start, ... datetime.now()] or [datetime.now(), ... start]
            - If 'end' and 'days' are specified -> constrain between [end - days + 1, ... end]
            - If only 'end' is specified -> constrain between [datetime.now(), ... end] or [end, ... datetime.now()]
            - If only 'days' is specified -> constrain between [datetime.now() - days + 1, ... datetime.now()]
            - If none of the params are given -> raise `QueryValueError`.

        :param unit: `<str>` Modifies the time span parameters to match the specified unit. Defaults to `'us'`.
            - This parameter adjusts the 'start' and 'end' times after calculating the
              time span to align with the respective unit.
            - For example, if `unit='Y'`, `start` is adjusted to the beginning of the
              year `(XXXX-01-01 00:00:00)` and `end` is adjusted to the end of the year
              `(XXXX-12-31 23:59:59.999999)`. Similar adjustments are made for other
              units, e.g. `month` adjusts to the start and end of the month, etc.
        """
        super().__init__(query, name, tabletime=None, anchor=False, alias=None)
        self._construct(parent, column, start, end, days, unit)

    # Properties ---------------------------------------------------------------------------------
    @property
    def syntax(self) -> str:
        "The syntax of the `TIME` clause `<str>`."
        return "\n\tAND ".join(self.gen_conditions())

    @cython.cfunc
    @cython.inline(True)
    def gen_conditions(self) -> list:
        "(cfunc) Generate conditions `<list[str]>."
        # Base conditions
        conditions: list = [self._syntax]

        # Time conditions
        time_clause = self._query._get_timeclause(self._index + 1)
        if time_clause is not None:
            conditions += time_clause.gen_conditions()

        # Return conditions
        return conditions

    @property
    def conditions(self) -> list[str]:
        "Condition of this `TIME` clause, along with all the trailing `TIME` conditions `<list[str]>`."
        return self.gen_conditions()

    # Construct ----------------------------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    def _construct(
        self,
        parent: str,
        column: Union[str, Column],
        start: object,
        end: object,
        days: object,
        unit: str,
    ):
        # Clause
        last_clause = self._query._get_clause(self._index - 1)
        last_name: str = last_clause._name
        if last_name != parent and last_name != self._name:
            raise errors.QueryValueError(
                "{}Must place right after `{}` or `{}` clause.".format(
                    self.err_pfix(), parent, self._name
                )
            )
        self._alias = last_clause._alias

        # Column
        col: str
        if isinstance(column, Column):
            col = self.prefix_alias(access_column_name(column))
            if set_contains(TIME_CLAUSE_COLUMN_TYPES, access_column_mysql(column)):
                fmt = access_column_dtype(column).format
            else:
                fmt = "%Y-%m-%d %H:%M:%S.%f"
        else:
            try:
                col = self.prefix_alias(column)
            except Exception as err:
                raise errors.QueryValueError(
                    "{}Invalid 'column': {}".format(self.err_pfix(), err)
                ) from err
            fmt = "%Y-%m-%d %H:%M:%S.%f"

        # Calculate start and end
        try:
            start_dt, end_dt = utils._cal_time_span(
                start, end, days if is_int(days) else -1, unit, raise_error=True
            )
        except Exception as err:
            raise errors.QueryValueError(
                "{}Invalid arguments: {}".format(self.err_pfix(), err)
            ) from err
        start: str = start_dt.strftime(fmt)
        end: str = end_dt.strftime(fmt)

        # Syntax
        if start == end:
            self._syntax = "%s = '%s'" % (col, start)
        else:
            self._syntax = "%s BETWEEN '%s' AND '%s'" % (col, start, end)


@cython.cclass
class ValuesClause(Clause):
    """The base `VALUES` clause of the MySQL query."""

    _value_columns: list[str]
    _where_columns: list[str]
    _columns: list[str]
    _tabletime: str
    _groupby_tabletime: cython.bint
    _exclude_tabletime: cython.bint
    _values_dtypes: set[int]
    """
    - 1: DataFrame(s)
    - 2: dict(s)
    - 3: Sequences(s)
    """
    _values_normtable: list[list]
    _values_timetable: dict[str, list[list]]

    def __init__(
        self,
        query: Query,
        *values: object,
        value_columns: Union[list[str | Column], tuple[str | Column]],
        where_columns: Union[list[str | Column], tuple[str | Column]],
        auto_groupby_tabletime: cython.bint = False,
        auto_include_tabletime: cython.bint = False,
        alias: Union[str, None] = None,
    ) -> None:
        """Represents a `VALUES` clause of the MySQL query.

        :param query: `<Query>` The query of the clause.
        :param values: The values of the clause, accepts:
            - `<dict>`: Each represents one row of a table data.
            - `<sequence>`: Data types such as `<list>`, `<tuple>`, `<Series>`, each represents one row of a table data.
            - `<DataFrame>`: Each represents rows of a table data.
            - `<Any>`: Data types such as `<int>`, `<str>`, `<bytes>`, each represents an item of one row of a table data.

        :param value_columns & where_columns: `<list/tuple>` The columns to filter the values.
            - For values consist of `<dict>` or `<DataFrame>`, only keys/columns include
              in given 'columns' will be retained.
            - For other types of values, each data 'row' must match with the given 'columns'.

        :param auto_groupby_tabletime: `<bool>` Whether the auto group values by tabletime. Defaults to `False`.
            - Only applicable for TimeTable query, normal Table query takes no effect.

        :param auto_include_tabletime: `<bool>` Whether to auto include the tabletime column. Defaults to `False`.
            - Only applicable for TimeTable query, normal Table query takes no effect.

        :param alias: `<str/None>` The alias of the clause. Defaults to `None`.
            - Only applicable for `VALUES` clause in `INSERT` table,
              other queries should always be `None`.
        """
        super().__init__(query, "VALUES", tabletime=None, anchor=False, alias=alias)
        self._construct(
            values,
            value_columns,
            where_columns,
            auto_groupby_tabletime,
            auto_include_tabletime,
        )
        self._query._set_values(self)

    # Properties ---------------------------------------------------------------------------------
    @property
    def value_columns(self) -> list[str]:
        "The value columns of the clause `<list[str]>`."
        return self._value_columns

    @property
    def where_columns(self) -> list[str]:
        "The where columns of the clause `<list[str]>`."
        return self._where_columns

    @property
    def values(self) -> Union[list[tuple[str]], dict[str, list[tuple[str]]]]:
        """The value of the clause.
        - Returns `<list[tuple]>` for normal Table.
        - Returns `<dict[str, list[tuple]]>` for TimeTable.
        """
        if self._groupby_tabletime:
            return self._values_timetable
        else:
            return self._values_normtable

    # Construct ----------------------------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    def _construct(
        self,
        values: tuple,
        value_columns: object,
        where_columns: object,
        auto_groupby_tabletime: cython.bint,
        auto_include_tabletime: cython.bint,
    ):
        # Value columns
        if value_columns is not None:
            self._value_columns = self.parse_column_names(value_columns)
        else:
            self._value_columns = []

        # Where columns
        if where_columns is not None:
            self._where_columns = self.parse_column_names(where_columns)
        else:
            self._where_columns = []

        # Columns
        self._columns = self._value_columns + self._where_columns
        if not self._columns:
            raise errors.QueryValueError(
                "{}Invalid 'columns', cannot be empty.".format(self.err_pfix())
            )
        if utils._list_duplicated(self._columns):
            dups = utils._list_duplicates(self._columns)
            raise errors.QueryValueError(
                "{}Invalid 'columns', contains duplicates"
                ": {}".format(self.err_pfix(), ", ".join(map(repr, dups)))
            )

        # TimeTable's tabletime column
        if (
            auto_groupby_tabletime
            and self._tb._is_timetable
            and self._query._query_timetables
        ):
            self._tabletime = self._tb._columns._tabletime._name
            self._groupby_tabletime = True
            if self._tabletime not in self._columns:
                if not auto_include_tabletime:
                    raise errors.QueryValueError(
                        "{}Required `tabletime` column '{}' does not exist in 'columns'. "
                        "Table '{}' is a TimeTable, the `tabletime` column is needed "
                        "to determine which sub-table(s) the values belong to.".format(
                            self.err_pfix(), self._tabletime, self._tb._fname
                        )
                    )
                self._columns.append(self._tabletime)
                self._exclude_tabletime = True
            else:
                self._exclude_tabletime = False
        else:
            self._tabletime = None
            self._groupby_tabletime = False
            self._exclude_tabletime = False

        # Srot Values
        self._values_dtypes = set()
        vals = self._sort_values(values)

        # Parse values
        # . empty
        length: cython.int = set_len(self._values_dtypes)
        if length == 0:
            raise errors.QueryValueError(
                "{}Invalid 'values', cannot be empty.".format(self.err_pfix())
            )
        # . invalid
        if length > 1:
            raise errors.QueryValueError(
                "{}Invalid 'values' data types, don't support mix of `<DataFrame>`, "
                "`<dict>` and `<Sequence>`.".format(self.err_pfix())
            )
        # . dataframe
        dtype: cython.int = set_pop(self._values_dtypes)
        if dtype == 1:
            if self._groupby_tabletime:
                self._values_timetable = self.parse_df_timetable(vals)
                self._values_normtable = None
            else:
                self._values_normtable = self.parse_df_normtable(vals)
                self._values_timetable = None
        # . dictionary
        elif dtype == 2:
            if self._groupby_tabletime:
                self._values_timetable = self.parse_dict_timetable(vals)
                self._values_normtable = None
            else:
                self._values_normtable = self.parse_dict_normtable(vals)
                self._values_timetable = None
        # . sequence
        else:
            if self._groupby_tabletime:
                self._values_timetable = self.parse_sequ_timetable(vals)
                self._values_normtable = None
            else:
                self._values_normtable = self.parse_sequ_normtable(vals)
                self._values_timetable = None

        # Exclude tabletime column
        if self._exclude_tabletime:
            self._columns.remove(self._tabletime)

    @cython.cfunc
    @cython.inline(True)
    def _sort_values(self, values: object) -> list:
        "(cfunc) Sort values data `<list>`."
        res: list = []
        for item in values:
            dtype = type(item)
            if dtype is DataFrame:
                set_add(self._values_dtypes, 1)
                list_append(res, item)
            elif dtype is dict:
                set_add(self._values_dtypes, 2)
                list_append(res, item)
            elif set_contains(VALUES_CLAUSE_ITEM, dtype):
                set_add(self._values_dtypes, 3)
                return [values]
            elif set_contains(VALUES_CLAUSE_FLAT_SEQU, dtype):
                set_add(self._values_dtypes, 3)
                list_append(res, item)
            elif set_contains(VALUES_CLAUSE_NEST_SEQU, dtype):
                res += self._sort_values(item)
            else:
                raise errors.QueryValueError(
                    "{}Invalid 'values' item data type: "
                    "{}".format(self.err_pfix(), dtype)
                )
        return res

    # Data type - dataframe ----------------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    def parse_df_normtable(self, values: list[DataFrame]) -> list:
        df: DataFrame = self.validate_df(values)
        res: list = []
        for row in df.values:
            list_append(res, list(row))
        del df
        return res

    @cython.cfunc
    @cython.inline(True)
    def parse_df_timetable(self, values: list[DataFrame]) -> dict:
        time_column: str = self._tabletime

        # Concatenate & Validate
        df: DataFrame = self.validate_df(values)

        # Tabletime
        try:
            df[SUBTABLE_COLUMN] = self.parse_timetable_names(self._tb, df[time_column])
        except Exception as err:
            raise errors.QueryValueError(
                "{}Invalid `tabletime` column '{}': {}".format(
                    self.err_pfix(), time_column, err
                )
            ) from err

        # Group by tabletime
        if self._exclude_tabletime:
            drop = [SUBTABLE_COLUMN, time_column]
        else:
            drop = [SUBTABLE_COLUMN]
        res: dict = {}
        data: list
        for name, gp in df.groupby(SUBTABLE_COLUMN, as_index=False):
            data = []
            for row in gp.drop(columns=drop).values:
                list_append(data, list(row))
            dict_setitem(res, name, data)
        del df
        return res

    @cython.cfunc
    @cython.inline(True)
    def validate_df(self, values: list[DataFrame]) -> object:
        # Concatenate
        if list_len(values) > 1:
            columns: set = set(values[0].columns)
            for i in values[1:]:
                if set(i.columns) != columns:
                    raise errors.QueryValueError(
                        "{}Invalid 'values', all DataFrames must have "
                        "identical columns.".format(self.err_pfix())
                    )
            try:
                df = concat(values, ignore_index=True)
            except Exception as err:
                raise errors.QueryValueError(
                    "{}Invalid 'values', {}".format(self.err_pfix(), err)
                ) from err
        else:
            df = values[0].copy()

        # Validate columns
        for col in self._columns:
            # . get validator
            try:
                validator = self._tb._columns._series_validators[col]
            except KeyError as err:
                raise errors.QueryValueError(
                    "{}Column '{}' does not belong to table '{}'.".format(
                        self.err_pfix(), col, self._tb._fname
                    )
                ) from err

            # . validate values
            try:
                df[col] = validator(df[col])
            except KeyError as err:
                if self._tabletime == col:
                    raise errors.QueryValueError(
                        "{}Required `tabletime` column '{}' does not exist in 'values'. "
                        "Table '{}' is a TimeTable, the `tabletime` column is needed "
                        "to determine which sub-table(s) the values belong to.".format(
                            self.err_pfix(), col, self._tb._fname
                        )
                    ) from err
                else:
                    raise errors.QueryValueError(
                        "{}Required column '{}' does not exist in 'values'"
                        ": {}.".format(self.err_pfix(), col, err)
                    ) from err
            except Exception as err:
                raise errors.QueryValueError(
                    "{}Invalid values for column '{}'"
                    ": {}".format(self.err_pfix(), col, err)
                ) from err
        return df[self._columns]

    # Data type - dict ---------------------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    def parse_dict_normtable(self, values: list[dict]) -> list:
        res: list = []
        for dic in values:
            data = self.validate_dict(dic)
            list_append(res, dict_values(data))
        return res

    @cython.cfunc
    @cython.inline(True)
    def parse_dict_timetable(self, values: list[dict]) -> dict:
        time_column: str = self._tabletime
        res: dict = {}

        # Drop tabletime
        if self._exclude_tabletime:
            for dic in values:
                data = self.validate_dict(dic)
                try:
                    name = self.parse_timetable_name(self._tb, data[time_column])
                except Exception as err:
                    raise errors.QueryValueError(
                        "{}Invalid column '{}' value as tabletime {}: {}".format(
                            self.err_pfix(), time_column, repr(data[time_column]), err
                        )
                    ) from err
                dict_delitem(data, time_column)
                if dict_contains(res, name):
                    list_append(res[name], dict_values(data))
                else:
                    dict_setitem(res, name, [dict_values(data)])

        # Keep tabletime
        else:
            for dic in values:
                data = self.validate_dict(dic)
                try:
                    name = self.parse_timetable_name(self._tb, data[time_column])
                except Exception as err:
                    raise errors.QueryValueError(
                        "{}Invalid column '{}' value as tabletime {}: {}".format(
                            self.err_pfix(), time_column, repr(data[time_column]), err
                        )
                    ) from err
                if dict_contains(res, name):
                    list_append(res[name], dict_values(data))
                else:
                    dict_setitem(res, name, [dict_values(data)])
        return res

    @cython.cfunc
    @cython.inline(True)
    def validate_dict(self, dic: dict) -> dict:
        res: dict = {}
        for col in self._columns:
            # Get validator
            try:
                validator = self._tb._columns._item_validators[col]
            except KeyError as err:
                raise errors.QueryValueError(
                    "{}Column '{}' does not belong to table '{}'.".format(
                        self.err_pfix(), col, self._tb._fname
                    )
                ) from err

            # Validate value
            try:
                dict_setitem(res, col, validator(dic[col]))
            except KeyError as err:
                if self._tabletime == col:
                    raise errors.QueryValueError(
                        "{}Required `tabletime` column '{}' does not exist in 'values'. "
                        "Table '{}' is a TimeTable, the `tabletime` column is needed "
                        "to determine which sub-table(s) the values belong to.".format(
                            self.err_pfix(), col, self._tb._fname
                        )
                    ) from err
                else:
                    raise errors.QueryValueError(
                        "{}Required column '{}' does not exist in 'values'"
                        ": {}.".format(self.err_pfix(), col, err)
                    ) from err
            except Exception as err:
                raise errors.QueryValueError(
                    "{}Invalid value for column '{}': {}".format(
                        self.err_pfix(), col, err
                    )
                ) from err
        return res

    # Data type - sequence -----------------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    def parse_sequ_normtable(self, values: list[object]) -> list:
        res: list = []
        for item in values:
            data = self.validate_sequ(item)
            list_append(res, dict_values(data))
        return res

    @cython.cfunc
    @cython.inline(True)
    def parse_sequ_timetable(self, values: list[object]) -> dict:
        time_column: str = self._tabletime
        res: dict = {}

        # Drop tabletime
        if self._exclude_tabletime:
            for item in values:
                data = self.validate_sequ(item)
                try:
                    name = self.parse_timetable_name(self._tb, data[time_column])
                except Exception as err:
                    raise errors.QueryValueError(
                        "{}Invalid column '{}' value as tabletime {}: {}".format(
                            self.err_pfix(), time_column, repr(data[time_column]), err
                        )
                    ) from err
                dict_delitem(data, time_column)
                if dict_contains(res, name):
                    list_append(res[name], dict_values(data))
                else:
                    dict_setitem(res, name, [dict_values(data)])

        # Keep tabletime
        else:
            for item in values:
                data = self.validate_sequ(item)
                try:
                    name = self.parse_timetable_name(self._tb, data[time_column])
                except Exception as err:
                    raise errors.QueryValueError(
                        "{}Invalid column '{}' value as tabletime {}: {}".format(
                            self.err_pfix(), time_column, repr(data[time_column]), err
                        )
                    ) from err
                if dict_contains(res, name):
                    list_append(res[name], self.dict_values(data))
                else:
                    dict_setitem(res, name, [dict_values(data)])
        return res

    @cython.cfunc
    @cython.inline(True)
    def validate_sequ(self, seq: object) -> dict:
        # Zip sequence with columns
        try:
            zipped: list = list(zip(self._columns, seq, strict=True))
        except Exception as err:
            raise errors.QueryValueError(
                "{}Invalid 'values', does not match with the 'columns'.\n"
                "<columns len={}> [{}]\n"
                "<values. len={}> [{}]".format(
                    self.err_pfix(),
                    len(self._columns),
                    ", ".join(map(repr, self._columns)),
                    len(seq),
                    ", ".join(map(repr, seq)),
                )
            ) from err

        res: dict = {}
        for col, val in zipped:
            # Get validator
            try:
                validator = self._tb._columns._item_validators[col]
            except KeyError as err:
                raise errors.QueryValueError(
                    "{}Column '{}' does not belong to table '{}'.".format(
                        self.err_pfix(), col, self._tb._fname
                    )
                ) from err

            # Validate value
            try:
                dict_setitem(res, col, validator(val))
            except Exception as err:
                raise errors.QueryValueError(
                    "{}Invalid value for column '{}': {}\n"
                    "This could also happen if the 'values' order does not match with 'columns'.\n"
                    "<columns> [{}]\n"
                    "<values.> [{}]".format(
                        self.err_pfix(),
                        col,
                        err,
                        ", ".join(map(repr, self._columns)),
                        ", ".join(
                            map(repr, seq),
                        ),
                    )
                ) from err
        return res


# Special Clause ================================================================================
@cython.cclass
class TABLETIMES(Clause):
    """The `TABLETIMES` clause of the MySQL query.

    The `TABLETIMES` clause is not a legit SQL clause but a custom design to
    specific the desired sub-tables of a TimeTable query. This clause is only
    applicable when the query involves at least one TimeTable.
    """

    def __init__(
        self,
        query: Query,
        *times: Union[str, datetime.date, datetime.datetime, None],
        start: Union[str, datetime.date, datetime.datetime, None] = None,
        end: Union[str, datetime.date, datetime.datetime, None] = None,
        days: Union[int, None] = None,
    ) -> None:
        super().__init__(query, "TABLETIMES", tabletime=None, anchor=False, alias=None)
        self._construct(times, start, end, days)
        self._query._set_tabletimes(self)

    # Properties ---------------------------------------------------------------------------------
    @property
    def syntax(self) -> str:
        raise NotImplementedError("{}Does not have syntax.".format(self.err_pfix()))

    # Construct ----------------------------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    def _construct(self, times: tuple, start: object, end: object, days: object):
        # Validate timetables
        if not self._query._timetables:
            raise errors.QueryValueError(
                "{}Query does not involve any *unspecified* TimeTables. Please place "
                "`tabletimes()` method after clauses corresponds to a TimeTable, or "
                "remove it from the query.".format(self.err_pfix())
            )
        tb: TimeTable = dict_values(self._query._timetables)[0]

        # Generate through 'times'
        tabletimes: list
        if times:
            tabletimes = []
            for time in times:
                try:
                    list_append(tabletimes, pydt(time)._dt)
                except Exception as err:
                    raise errors.QueryValueError(
                        "{}Invalid 'times' value {}: {}".format(
                            self.err_pfix(), repr(time), err
                        )
                    ) from err

        # Generate through 'start', 'end', 'days'
        else:
            try:
                tabletimes = utils._gen_time_span(
                    start, end, days or -1, tb._time_unit, True
                )
            except Exception as err:
                raise errors.QueryValueError(
                    "{}Invalid time span arguments: {}".format(self.err_pfix(), err)
                ) from err

        # Set tabletimes
        self._query._timetable_times = tabletimes


@cython.cclass
class CUSTOM(Clause):
    """The `CUSTOM` clause of the MySQL query.

    The `CUSTOM` clause is not a legit SQL clause but a custom design
    to execute complex SQL statements when the built-in methods are not
    sufficient to handle the desired query.
    """

    _stmt: str

    def __init__(
        self,
        query: Query,
        stmt: str,
        args: Union[list, tuple, None] = None,
    ) -> None:
        super().__init__(query, "CUSTOM", tabletime=None, anchor=False, alias=None)
        self._construct(stmt, args)
        self._query._set_custom(self)

    # Properties ---------------------------------------------------------------------------------
    @property
    def stmt(self) -> str:
        "The SQL statement of the `CUSTOM` clause `<str>`."
        return self._stmt

    @property
    def syntax(self) -> str:
        "Alias for 'stmt' `<str>."
        return self._stmt

    # Construct ----------------------------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    @cython.wraparound(True)
    def _construct(self, stmt: str, args: object):
        # Validate statement
        stmt = utils._str_dedent(stmt)
        if not stmt:
            raise errors.QueryValueError(
                "{}Invalid 'stmt', cannot be empty.".format(self.err_pfix())
            )

        # Validate arguments
        if args is not None:
            try:
                if set_contains(MULTIROWS_ARGS_DTYPES, type(args)):
                    if is_dict(args):
                        dic: dict = {}
                        for key, val in dict_items(args):
                            dict_setitem(dic, key, self.escape_item(val))
                        args = dic
                    else:
                        items: list = []
                        for item in args:
                            list_append(items, self.escape_item(item))
                        args = tuple(items)
                else:
                    args = self.escape_args(args)
            except Exception as err:
                raise errors.QueryValueError(
                    "{}Invalid 'args': {}".format(self.err_pfix(), err)
                ) from err
        else:
            args = tuple()

        # Set statement
        try:
            stmt = stmt % args
        except Exception as err:
            raise errors.QueryValueError(
                "{}Invalid 'stmt' & 'args': {}".format(self.err_pfix(), err)
            )
        self._stmt = stmt


# Select Clause =================================================================================
@cython.cclass
class SELECT(Clause):
    """The `SELECT... FROM...` clause of the MySQL query."""

    _columns: list[str]
    _columns_syntax: str
    _distinct: cython.bint
    _buffer_result: cython.bint
    _explain: cython.bint

    def __init__(
        self,
        query: Query,
        *columns: Union[str, Column],
        distinct: cython.bint = False,
        buffer_result: cython.bint = False,
        explain: cython.bint = False,
        from_: Union[str, Table, TimeTable, SelectQuery, None] = None,
        tabletime: Union[str, datetime.date, datetime.datetime, None] = None,
        alias: Union[str, None] = None,
    ) -> None:
        super().__init__(query, "SELECT", tabletime=tabletime, anchor=True, alias=alias)
        if distinct:
            self._distinct = True
            self._query._force_union_mode = True
        else:
            self._distinct = False
        self._buffer_result = buffer_result
        self._explain = explain
        self._construct(columns)
        self.set_mode(from_)
        self._query._set_select(self)

    # Properties ---------------------------------------------------------------------------------
    @property
    def syntax(self) -> str:
        "The `'SELECT... FROM...'` syntax `<str>`."
        # Select syntax
        select_syntax: str = "%sSELECT%s%s %s" % (
            # Modifier - EXPLAIN
            "EXPLAIN " if self._explain else "",
            # Modifier - DISTINCT
            " DISTINCT" if self._distinct else "",
            # Modifier - BUFFER RESULT
            " SQL_BUFFER_RESULT" if self._buffer_result else "",
            # Columns
            self._columns_syntax,
        )

        # Table mode
        if 1 <= self._mode <= 3:
            return "%sFROM %s.%s AS %s" % (
                select_syntax,
                self._db._name,
                self._tb_name,
                self._alias,
            )
        # Subquery mode
        elif self._mode == 4:
            return "%sFROM %s AS %s" % (
                select_syntax,
                self._subquery_key,
                self._alias,
            )
        # Custom mode
        elif self._mode == 5:
            return "%sFROM %s AS %s" % (
                select_syntax,
                self._custom_val,
                self._alias,
            )
        # Invalid mode
        else:
            raise errors.QueryValueError(
                "{}Clause mode not set: {}".format(self.err_pfix(), self._mode)
            )

    # TimeTable ----------------------------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def gen_subtable_syntax(self) -> str:
        """(cfunc) Generate the `'SELECT... FROM...'` syntax for sub-tables `<str>`.
        (Only applicable for TimeTable).
        """
        # Select syntax
        select_syntax: str = "SELECT " + self._columns_syntax

        # Table mode
        if 1 <= self._mode <= 3:
            return "%sFROM %s.%s AS %s" % (
                select_syntax,
                self._db._name,
                self._tb_name,
                self._alias,
            )
        # Subquery mode
        elif self._mode == 4:
            return "%sFROM %s AS %s" % (
                select_syntax,
                self._subquery_key,
                self._alias,
            )
        # Custom mode
        elif self._mode == 5:
            return "%sFROM %s AS %s" % (
                select_syntax,
                self._custom_val,
                self._alias,
            )
        # Invalid mode
        else:
            raise errors.QueryValueError(
                "{}Clause mode not set: {}".format(self.err_pfix(), self._mode)
            )

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def gen_union_select(self) -> str:
        """(cfunc) Generate the master `'SELECT... FROM...'` syntax for
        union sub-tables syntax`<str>`. (Only applicable for TimeTable).
        """
        return "%sSELECT%s%s * FROM" % (
            # Modifier - EXPLAIN
            "EXPLAIN " if self._explain else "",
            # Modifier - DISTINCT
            " DISTINCT" if self._distinct else "",
            # Modifier - BUFFER RESULT
            " SQL_BUFFER_RESULT" if self._buffer_result else "",
        )

    # Construct ----------------------------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    def _construct(self, columns: tuple):
        # Columns
        if not columns:
            self._columns = ["*"]
        else:
            self._columns = self.parse_sequ_of_column_names(columns)

        # Columns syntax
        if list_len(self._columns) > 2:
            self._columns_syntax = "\n\t" + ",\n\t".join(self._columns) + "\n"
        else:
            self._columns_syntax = ", ".join(self._columns) + " "


@cython.cclass
class JOIN(JoinClause):
    """The `JOIN` clause of the MySQL query."""

    def __init__(
        self,
        query: Query,
        table: Union[str, Table, TimeTable, SelectQuery],
        *ons: str,
        args: object = None,
        ins: Union[dict[str, Union[list, tuple]], None] = None,
        not_ins: Union[dict[str, Union[list, tuple]], None] = None,
        subqueries: Union[dict[str, SelectQuery], None] = None,
        method: Literal["INNER", "LEFT", "RIGHT"] = "INNER",
        tabletime: Union[str, datetime.date, datetime.datetime, None] = None,
        alias: Union[str, None] = None,
    ) -> None:
        super().__init__(
            query,
            table,
            *ons,
            args=args,
            ins=ins,
            not_ins=not_ins,
            subqueries=subqueries,
            method=method,
            tabletime=tabletime,
            alias=alias,
        )


@cython.cclass
class JOIN_TIME(TimeClause):
    """The `TIME` clause for `JOIN` of the MySQL query.

    `JOIN_TIME` clause is not a legit SQL clause but a custom design to
    facilitate construct of time range conditions. For instance, `JOIN_TIME`
    can be used to construct the following syntax:
    `ON t2.<time_column> BETWEEN '2020-01-01' AND '2020-01-31'`.
    """

    def __init__(
        self,
        query: Query,
        column: Union[str, Column],
        start: Union[str, datetime.date, datetime.datetime, None] = None,
        end: Union[str, datetime.date, datetime.datetime, None] = None,
        days: Union[int, None] = None,
        unit: Literal["Y", "M", "D", "W", "h", "m", "s", "us"] = "us",
    ) -> None:
        super().__init__(
            query,
            "JOIN_TIME",
            "JOIN",
            column,
            start=start,
            end=end,
            days=days,
            unit=unit,
        )


@cython.cclass
class WHERE(WhereClause):
    """The `WHERE` clause of the MySQL query."""

    def __init__(
        self,
        query: Query,
        *conditions: str,
        args: object = None,
        ins: dict[str, list | tuple] | None = None,
        not_ins: dict[str, list | tuple] | None = None,
        subqueries: dict[str, SelectQuery] | None = None,
    ) -> None:
        super().__init__(
            query,
            *conditions,
            args=args,
            ins=ins,
            not_ins=not_ins,
            subqueries=subqueries,
            alias=query._get_select(True)._alias,
        )


@cython.cclass
class WHERE_TIME(TimeClause):
    """The `TIME` clause for `WHERE` of the MySQL query.

    `WHERE_TIME` clause is not a legit SQL clause but a custom design to
    facilitate construct of time range conditions. For instance, `WHERE_TIME`
    can be used to construct the following syntax:
    `WHERE <time_column> BETWEEN '2020-01-01' AND '2020-01-31'`.
    """

    def __init__(
        self,
        query: Query,
        column: Union[str, Column],
        start: Union[str, datetime.date, datetime.datetime, None] = None,
        end: Union[str, datetime.date, datetime.datetime, None] = None,
        days: Union[int, None] = None,
        unit: Literal["Y", "M", "D", "W", "h", "m", "s", "us"] = "us",
    ) -> None:
        super().__init__(
            query,
            "WHERE_TIME",
            "WHERE",
            column,
            start=start,
            end=end,
            days=days,
            unit=unit,
        )


@cython.cclass
class GROUP_BY(Clause):
    """The `GROUP BY` clause of the MySQL query."""

    _syntax: str

    def __init__(self, query: Query, *columns: Union[str, Column]) -> None:
        super().__init__(query, "GROUP BY", tabletime=None, anchor=False, alias=None)
        self._construct(columns)

    # Properties ---------------------------------------------------------------------------------
    @property
    def syntax(self) -> str:
        "The `'GROUP BY ...'` syntax `<str>`."
        return self._syntax

    # Construct ----------------------------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    def _construct(self, columns: tuple):
        # Columns
        if not columns:
            raise errors.QueryValueError(
                "{}Invalid 'columns', cannot be empty.".format(self.err_pfix())
            )
        cols = self.parse_sequ_of_column_names(columns)
        cols = utils.list_drop_duplicates(cols)
        # Syntax
        self._syntax = "GROUP BY %s" % ", ".join(cols)


@cython.cclass
class HAVING(ConditionClause):
    """The `HAVING` clause of the MySQL query."""

    def __init__(
        self,
        query: Query,
        *conditions: str,
        args: object = None,
        ins: Union[dict[str, Union[list, tuple]], None] = None,
        not_ins: Union[dict[str, Union[list, tuple]], None] = None,
        subqueries: Union[dict[str, SelectQuery], None] = None,
    ) -> None:
        super().__init__(
            query,
            "HAVING",
            *conditions,
            args=args,
            ins=ins,
            not_ins=not_ins,
            subqueries=subqueries,
            tabletime=None,
            anchor=False,
            alias=None,
        )


@cython.cclass
class HAVING_TIME(TimeClause):
    """The `TIME` clause for `HAVING` of the MySQL query.

    `HAVING_TIME` clause is not a legit SQL clause but a custom design to
    facilitate construct of time range conditions. For instance, `HAVING_TIME`
    can be used to construct the following syntax:
    `HAVING <time_column> BETWEEN '2020-01-01' AND '2020-01-31'`.
    """

    def __init__(
        self,
        query: Query,
        column: Union[str, Column],
        start: Union[str, datetime.date, datetime.datetime, None] = None,
        end: Union[str, datetime.date, datetime.datetime, None] = None,
        days: Union[int, None] = None,
        unit: Literal["Y", "M", "D", "W", "h", "m", "s", "us"] = "us",
    ) -> None:
        super().__init__(
            query,
            "HAVING_TIME",
            "HAVING",
            column,
            start=start,
            end=end,
            days=days,
            unit=unit,
        )


@cython.cclass
class ORDER_BY(Clause):
    """The `ORDER BY` clause of the MySQL query."""

    _syntax: str

    def __init__(self, query: Query, *orders: str) -> None:
        super().__init__(query, "ORDER BY", tabletime=None, anchor=False, alias=None)
        self._construct(orders)
        # Query
        self.set_union_select_clause(self)

    # Properties ---------------------------------------------------------------------------------
    @property
    def syntax(self) -> str:
        "The `'ORDER BY ...'` syntax `<str>`."
        return self._syntax

    # Construct ----------------------------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    def _construct(self, orders: tuple):
        # Orders
        if not orders:
            raise errors.QueryValueError(
                "{}Invalid 'orders', cannot be empty.".format(self.err_pfix())
            )
        # Syntax
        try:
            syntax: str = ", ".join(orders)
        except Exception as err:
            raise errors.QueryValueError(
                "{}Invalid 'orders': {}".format(self.err_pfix(), err)
            ) from err
        self._syntax = "ORDER BY " + syntax


@cython.cclass
class LIMIT(Clause):
    """The `LIMIT` clause of the MySQL query."""

    _syntax: str

    def __init__(self, query: Query, *limits: int) -> None:
        super().__init__(query, "LIMIT", tabletime=None, anchor=False, alias=None)
        self._construct(limits)
        # Query
        self.set_union_select_clause(self)

    # Properties ---------------------------------------------------------------------------------
    @property
    def syntax(self) -> str:
        "The `'LIMIT ...'` syntax `<str>`."
        return self._syntax

    # Construct ----------------------------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    def _construct(self, limits: tuple):
        # Validate
        res: list[str] = []
        val: int
        for limit in limits:
            if not is_int(limit):
                try:
                    val = int(limit)
                except Exception as err:
                    raise errors.QueryValueError(
                        "{}Invalid 'limits' value {}, cannot convert to an integer"
                        ": {}".format(self.err_pfix(), repr(limit), err)
                    ) from err
            else:
                val = limit
            if val < 0:
                raise errors.QueryValueError(
                    "{}Invalid 'limits' value {}, cannot be negative"
                    ": {}".format(self.err_pfix(), repr(limit), val)
                )
            list_append(res, str(val))

        length: cython.int = list_len(res)
        if length > 2:
            raise errors.QueryValueError(
                "{}Invalid 'limits', only support a maximum of 2 positive integers, instead got"
                ": [{}]".format(self.err_pfix(), ", ".join(map(repr, res)))
            )
        if length == 0:
            raise errors.QueryValueError(
                "{}Invalid 'limits', cannot be empty.".format(self.err_pfix())
            )
        # Syntax
        self._syntax = "LIMIT " + ", ".join(map(str, res))


@cython.cclass
class FOR_UPDATE(Clause):
    """The `FOR UPDATE` clause of the MySQL query."""

    _syntax: str

    def __init__(self, query: Query) -> None:
        super().__init__(query, "FOR UPDATE", tabletime=None, anchor=False, alias=None)
        self._syntax = "FOR UPDATE"
        self._query._set_for_update(self)

    # Properties ---------------------------------------------------------------------------------
    @property
    def syntax(self) -> str:
        "The `'FOR UPDATE'` syntax `<str>`."
        return self._syntax


@cython.cclass
class LOCK_IN_SHARE_MODE(Clause):
    """The `LOCK IN SHARE MODE` clause of the MySQL query."""

    _syntax: str

    def __init__(self, query: Query) -> None:
        super().__init__(
            query,
            "LOCK IN SHARE MODE",
            tabletime=None,
            anchor=False,
            alias=None,
        )
        self._syntax = "LOCK IN SHARE MODE"
        self._query._set_lock_in_share_mode(self)

    # Properties ---------------------------------------------------------------------------------
    @property
    def syntax(self) -> str:
        "The `'LOCK IN SHARE MODE'` syntax `<str>`."
        return self._syntax


# Insert Clause =================================================================================
@cython.cclass
class INSERT(InsertClause):
    """The `INSERT` clause of the MySQL query."""

    def __init__(
        self,
        query: Query,
        *columns: Union[str, Column],
        ignore: cython.bint = False,
        tabletime: Union[str, datetime.date, datetime.datetime, None] = None,
    ) -> None:
        super().__init__(query, "INSERT", *columns, ignore=ignore, tabletime=tabletime)


@cython.cclass
class INSERT_VALUES(ValuesClause):
    """The `VALUES` clause of the MySQL INSERT query."""

    def __init__(self, query: Query, *values: object, alias: str = "val") -> None:
        super().__init__(
            query,
            *values,
            value_columns=query._get_insert(True)._columns,
            where_columns=None,
            auto_groupby_tabletime=True,
            auto_include_tabletime=False,
            alias=alias,
        )

    # Properties ---------------------------------------------------------------------------------
    @property
    def syntax(self) -> str:
        "The `'VALUES ...'` syntax `<str>`."
        # Column syntax
        syntax: str = ", ".join(["%s"] * list_len(self._columns))

        # Value aliaing for MySQL 8.0 and later versions
        if self._db._server.get_version() >= 8:
            if not is_str(self._alias):
                raise errors.QueryValueError(
                    "{}Invalid 'alias', must type of `<str>`, instead of: {} {}".format(
                        self.err_pfix(), type(self._alias), repr(self._alias)
                    )
                )
            return "VALUES (%s) AS %s" % (syntax, self._alias)
        # Normal syntax for MySQL 5.7 and earlier versions
        else:
            return "VALUES (%s)" % syntax


@cython.cclass
class ON_DUPLICATE_KEY(Clause):
    """The `ON DUPLICATE KEY UPDATE` clause of the MySQL INSERT query."""

    _syntax: str

    def __init__(self, query: Query, *updates: str) -> None:
        super().__init__(
            query,
            "ON DUPLICATE KEY",
            tabletime=None,
            anchor=False,
            alias=None,
        )
        self._construct(updates)
        self._query._set_on_dup(self)

    # Properties ---------------------------------------------------------------------------------
    @property
    def syntax(self) -> str:
        "The `'ON DUPLICATE KEY UPDATE ...'` syntax `<str>`."
        return self._syntax

    # Construct ----------------------------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    def _construct(self, updates: tuple):
        # Validate updates
        if not updates:
            raise errors.QueryValueError(
                "{}Invalid 'updates', cannot be empty.".format(self.err_pfix())
            )

        # Syntax
        self._syntax = "ON DUPLICATE KEY UPDATE\n\t" + ",\n\t".join(updates)

        # Version 8.0 and later checker
        if self._db._server.get_version() >= 8:
            val_funcs: list = ON_DUPLICATE_VALUES_FUNC_RE.findall(self._syntax)
            if not val_funcs:
                return  # exit

            confl: list = []
            num: cython.int = 1
            for func in val_funcs:
                if not func:
                    continue
                list_append(confl, "- %s. '%s'" % (num, func))
                num += 1
            raise errors.QueryValueError(
                "{}For MySQL 8.0 and later, the official documentation recommends using "
                "'col = alias.col' over 'col = VALUES(col)' function. To ensure future "
                "compatibility, this package enforce the former syntax for MySQL versions "
                "8.0 and later. By default, the alias for `VALUES` clause is 'val'. All "
                "aliases can be customized through the 'alias' argument of the corresponding "
                "methods. Syntax that needs to be adjusted:\n{}".format(
                    self.err_pfix(), "\n".join(confl)
                )
            )


@cython.cclass
class REPLACE(InsertClause):
    """The `REPLACE` clause of the MySQL query."""

    def __init__(
        self,
        query: Query,
        *columns: Union[str, Column],
        tabletime: Union[str, datetime.date, datetime.datetime, None] = None,
    ) -> None:
        super().__init__(query, "REPLACE", *columns, ignore=False, tabletime=tabletime)


@cython.cclass
class REPLACE_VALUES(ValuesClause):
    """The `VALUES` clause of the MySQL REPLACE query."""

    def __init__(self, query: Query, *values: object) -> None:
        super().__init__(
            query,
            *values,
            value_columns=query._get_insert(True)._columns,
            where_columns=None,
            auto_groupby_tabletime=True,
            auto_include_tabletime=False,
            alias=None,
        )

    # Properties ---------------------------------------------------------------------------------
    @property
    def syntax(self) -> str:
        "The `'VALUES ...'` syntax `<str>`."
        # Column syntax
        syntax: str = ", ".join(["%s"] * list_len(self._columns))

        # Return syntax
        return "VALUES (%s)" % syntax


# Update Clause =================================================================================
@cython.cclass
class UPDATE(Clause):
    """The `UPDATE` clause of the MySQL query."""

    _ignore: cython.bint

    def __init__(
        self,
        query: Query,
        ignore: cython.bint = False,
        tabletime: Union[str, datetime.date, datetime.datetime, None] = None,
        alias: Union[str, None] = None,
    ) -> None:
        super().__init__(query, "UPDATE", tabletime=tabletime, anchor=True, alias=alias)
        self._ignore = ignore
        self.set_mode(None)
        self._query._set_update(self)

    # Properties ---------------------------------------------------------------------------------
    @property
    def syntax(self) -> str:
        "The `'UPDATE ...'` syntax `<str>`."
        return "UPDATE%s %s.%s AS %s" % (
            # Modifier - IGNORE
            " IGNORE" if self._ignore else "",
            # Table
            self._db._name,
            self._tb_name,
            # Alias
            self._alias,
        )


@cython.cclass
class SET(Clause):
    """The `SET` clause of the MySQL UPDATE query."""

    _assignments: list[str]

    def __init__(
        self,
        query: Query,
        *assignments: str,
        args: object = None,
        subqueries: dict[str, SelectQuery] | None = None,
    ) -> None:
        super().__init__(query, "SET", tabletime=None, anchor=False, alias=None)
        self._construct(assignments, args=args, subqueries=subqueries)
        self._query._set_clause_set(self)

    # Properties ---------------------------------------------------------------------------------
    @property
    def syntax(self) -> str:
        # Assignments
        values = self._query._get_update_values(False)
        if values is not None:
            assignments = self._assignments + values._assignments
        else:
            assignments = self._assignments

        # Syntax
        length: cython.int = list_len(assignments)
        if length > 1:
            return "SET %s" % ",\n\t".join(assignments)
        elif length == 1:
            return "SET %s" % assignments[0]
        else:
            raise errors.QueryValueError(
                "{}Requires at least one assignment.".format(self.err_pfix())
            )

    # Construct ----------------------------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    def _construct(self, assignments: tuple, args: object, subqueries: object):
        # Arguments
        args = tuple() if args is None else self.escape_args(args)

        # Assignments
        if assignments:
            try:
                concat: str = JOIN_SYM.join(assignments)
            except Exception as err:
                raise errors.QueryValueError(
                    "{}Invalid 'assignments': {}".format(self.err_pfix(), err)
                )
            try:
                concat = concat % args
            except Exception as err:
                raise errors.QueryValueError(
                    "{}Invalid 'args': {}".format(self.err_pfix(), err)
                ) from err
            self._assignments = concat.split(JOIN_SYM)
        else:
            self._assignments = []

        # Subqueries
        if is_dict(subqueries):
            try:
                for col, query in dict_items(subqueries):
                    sql = str(col) + " = " + self.add_subquery(query, 2)
                    list_append(self._assignments, sql)
            except Exception as err:
                raise errors.QueryValueError(
                    "{}Invalid 'subqueries': {}".format(self.err_pfix(), err)
                ) from err


@cython.cclass
class UPDATE_WHERE(WhereClause):
    """The `WHERE` clause of the MySQL UPDATE query."""

    def __init__(
        self,
        query: Query,
        *conditions: str,
        args: object = None,
        ins: Union[dict[str, Union[list, tuple]], None] = None,
        not_ins: Union[dict[str, Union[list, tuple]], None] = None,
        subqueries: Union[dict[str, SelectQuery], None] = None,
    ) -> None:
        super().__init__(
            query,
            *conditions,
            args=args,
            ins=ins,
            not_ins=not_ins,
            subqueries=subqueries,
            alias=query._get_update(True)._alias,
        )

    # Properties ---------------------------------------------------------------------------------
    @property
    def syntax(self) -> str:
        "The syntax of the clause `<str>`."
        conditions = self.gen_extra_conditions()
        length: cython.int = list_len(conditions)
        if length > 1:
            return self._name + " " + "\n\tAND ".join(conditions)
        elif length == 1:
            return self._name + " " + conditions[0]
        else:
            return ""

    @cython.cfunc
    @cython.inline(True)
    def gen_extra_conditions(self) -> list:
        "(cfunc) Generate conditions with extra from `VALUES` clause <list[str]>."
        # Base conditions
        conditions: list = self.gen_conditions()

        # Values conditions
        values = self._query._get_update_values(False)
        if values is not None:
            conditions += values._conditions

        # Return conditions
        return conditions

    @property
    def conditions(self) -> list[str]:
        """All conditions of the cluase, along with all the trailing
        `TIME` conditions and `VALUES` clause `<list[str]>`."""
        return self.gen_extra_conditions()


@cython.cclass
class UPDATE_VALUES(ValuesClause):
    """The `VALUES` clause of the MySQL UPDATE query.

    The `UPDATE_VALUES` clause is not a legit SQL clause but a custom design to
    facilitate batch updates of different rows on different conditions. For
    instance, `UPDATE_VALUES` can be used to construct the following syntax:

    >>> UPDATE...
        SET <col1> = %s, <col2> = %s, ...
        WHERE <col3> = %s AND <col4> = %s AND ...
    """

    _assignments: list[str]
    _conditions: list[str]

    def __init__(
        self,
        query: Query,
        values: object,
        value_columns: Union[list[str | Column], tuple[str | Column]],
        where_columns: Union[list[str | Column], tuple[str | Column]],
    ) -> None:
        super().__init__(
            query,
            values,
            value_columns=value_columns,
            where_columns=where_columns,
            auto_groupby_tabletime=True,
            auto_include_tabletime=True,
            alias=query._get_update(True)._alias,
        )
        # Validate
        if not self._value_columns:
            raise errors.QueryValueError(
                "{}Must have at least one valid 'value_columns'. This will be used to "
                "to match the columns in 'values' and construct the `SET` assignments "
                "syntax. Otherwise, it is unclear which column should be updated.".format(
                    self.err_pfix()
                )
            )
        if not self._where_columns:
            raise errors.QueryValueError(
                "{}Must have at least one valid 'where_columns'. This will be used to"
                "match the columns in 'values' and construct the `WHERE` condition syntax. "
                "Otherwise, a simple update can achieve through `WHERE` clause without "
                "using the `VALUES` clause.".format(self.err_pfix())
            )
        # Assignments
        res: list = []
        for col in self._value_columns:
            list_append(res, self.prefix_alias(col) + " = %s")
        self._assignments = res
        # Conditions
        res: list = []
        for col in self._where_columns:
            list_append(res, self.prefix_alias(col) + " = %s")
        self._conditions = res

    # Properties ---------------------------------------------------------------------------------
    @property
    def set_syntax(self) -> str:
        "The `'SET ...'` syntax `<str>`."
        return "SET " + ",\n\t".join(self._assignments)

    @property
    def where_syntax(self) -> str:
        "The `'WHERE ...'` syntax `<str>`."
        return "WHERE " + "\n\tAND ".join(self._conditions)


# Delete Clause =================================================================================
@cython.cclass
class DELETE(Clause):
    """The `DELETE` clause of the MySQL query."""

    _table_aliases: tuple[str]
    _ignore: cython.bint

    def __init__(
        self,
        query: Query,
        *table_aliases: str,
        ignore: cython.bint = False,
        tabletime: Union[str, datetime.date, datetime.datetime, None] = None,
        alias: Union[str, None] = None,
    ) -> None:
        super().__init__(
            query,
            "DELETE",
            tabletime=tabletime,
            anchor=True,
            alias=alias,
        )
        self._table_aliases = table_aliases
        self._ignore = ignore
        self.set_mode(None)
        self._query._set_delete(self)

    # Properties ---------------------------------------------------------------------------------
    @property
    def syntax(self) -> str:
        "The `'DELETE ...'` syntax `<str>`."
        # Multi-tables
        if self._query._join_clauses:
            # . get all table aliases
            aliases: list = [self._alias]
            for join in self._query._join_clauses:
                list_append(aliases, join.alias)
            # . generate alias syntax
            if not self._table_aliases:
                alias_syntax = ", ".join(aliases)
            else:
                for alias in self._table_aliases:
                    if alias in aliases:
                        continue
                    _aliases = ", ".join(map(repr, aliases))
                    raise errors.QueryValueError(
                        "{}Invalid table alias {} for multi-table delete operation, "
                        "does not match with any aliases in the query: [{}]".format(
                            self.err_pfix(), repr(alias), _aliases
                        )
                    )
                alias_syntax = ", ".join(self._table_aliases)
            # . multi-table delete syntax
            return "DELETE%s %s FROM %s.%s AS %s" % (
                " IGNORE" if self._ignore else "",
                alias_syntax,
                self._db._name,
                self._tb_name,
                self._alias,
            )
        # Single-table
        else:
            return "DELETE%s FROM %s.%s AS %s" % (
                " IGNORE" if self._ignore else "",
                self._db._name,
                self._tb_name,
                self._alias,
            )


@cython.cclass
class DELETE_JOIN(JoinClause):
    """The `JOIN` clause of the MySQL DELETE query."""

    def __init__(
        self,
        query: Query,
        table: Union[str, Table, TimeTable],
        *ons: str,
        args: object = None,
        ins: Union[dict[str, Union[list, tuple]], None] = None,
        not_ins: Union[dict[str, Union[list, tuple]], None] = None,
        subqueries: Union[dict[str, SelectQuery], None] = None,
        method: Literal["INNER", "LEFT", "RIGHT"] = "INNER",
        tabletime: Union[str, datetime.date, datetime.datetime, None] = None,
        alias: Union[str, None] = None,
    ) -> None:
        super().__init__(
            query,
            table,
            *ons,
            args=args,
            ins=ins,
            not_ins=not_ins,
            subqueries=subqueries,
            method=method,
            tabletime=tabletime,
            alias=alias,
        )
        # Validate
        if self._mode == 4:
            raise errors.QueryValueError(
                "{}Invalid 'table', `DELETE` query does not support joining with "
                "drived tables (subqueries).".format(self.err_pfix())
            )


@cython.cclass
class DELETE_WHERE(WhereClause):
    """The `WHERE` clause of the MySQL DELETE query."""

    def __init__(
        self,
        query: Query,
        *conditions: str,
        args: object = None,
        ins: Union[dict[str, Union[list, tuple]], None] = None,
        not_ins: Union[dict[str, Union[list, tuple]], None] = None,
        subqueries: Union[dict[str, SelectQuery], None] = None,
    ) -> None:
        super().__init__(
            query,
            *conditions,
            args=args,
            ins=ins,
            not_ins=not_ins,
            subqueries=subqueries,
            alias=query._get_delete(True)._alias,
        )

    # Properties ---------------------------------------------------------------------------------
    @property
    def syntax(self) -> str:
        "The syntax of the clause `<str>`."
        conditions = self.gen_extra_conditions()
        length: cython.int = list_len(conditions)
        if length > 1:
            return self._name + " " + "\n\tAND ".join(conditions)
        elif length == 1:
            return self._name + " " + conditions[0]
        else:
            return ""

    @cython.cfunc
    @cython.inline(True)
    def gen_extra_conditions(self) -> list:
        "(cfunc) Generate conditions with extra from `VALUES` clause <list[str]>."
        # Base conditions
        conditions: list = self.gen_conditions()

        # Values conditions
        values = self._query._get_delete_values(False)
        if values is not None:
            conditions += values._conditions

        # Return conditions
        return conditions

    @property
    def conditions(self) -> list[str]:
        """All conditions of the cluase, along with all the trailing
        `TIME` conditions and `VALUES` clause `<list[str]>`."""
        return self.gen_extra_conditions()


@cython.cclass
class DELETE_VALUES(ValuesClause):
    """The `VALUES` clause of the MySQL DELETE query.

    The `DELETE_VALUES` clause is not a legit SQL clause but a custom design to
    facilitate batch deletes of different rows on different conditions. For
    instance, `DELETE_VALUES` can be used to construct the following syntax:

    >>> DELETE...
        WHERE <col1> = %s AND <col1> = %s AND ...
    """

    _conditions: list[str]

    def __init__(
        self,
        query: Query,
        values: object,
        *where_columns: Union[str, Column],
    ) -> None:
        super().__init__(
            query,
            values,
            value_columns=None,
            where_columns=where_columns,
            auto_groupby_tabletime=True,
            auto_include_tabletime=True,
            alias=query._get_delete(True)._alias,
        )
        # Conditions
        res: list = []
        for col in self._where_columns:
            list_append(res, self.prefix_alias(col) + " = %s")
        self._conditions = res

    # Properties ---------------------------------------------------------------------------------
    @property
    def where_syntax(self) -> str:
        "The `'WHERE ...'` syntax `<str>`."
        return "WHERE " + "\n\tAND ".join(self._conditions)


# Create Temp Clause ============================================================================
@cython.cclass
class CREATE_TEMP(Clause):
    """The `CREATE TEMPORARY TABLE` clause of the MySQL query."""

    _charset: str
    _collate: str
    _engine: str
    _columns: list[str]
    _columns_syntax: list[str]
    _indexes: list[str]
    _indexes_syntax: list[str]

    def __init__(
        self,
        query: Query,
        *columns: Union[str, Column],
        indexes: Union[list[str | Index], Literal["auto"], None] = None,
        engine: Literal["MEMORY", "InnoDB", "MyISAM"] = "MEMORY",
        charset: Union[str, None] = None,
        collate: Union[str, None] = None,
    ) -> None:
        super().__init__(query, "CREATE TEMP", tabletime=None, anchor=False, alias=None)
        self._construct(columns, indexes, engine, charset, collate)
        self._tb_name = self._tb._gen_tname()
        self._query._set_create_temp(self)

    # Properties ---------------------------------------------------------------------------------
    @property
    def syntax(self) -> str:
        """The `'CREATE TEMPORARY TABLE ...'` syntax `<str>`."""
        return (
            "CREATE TEMPORARY TABLE %s (\n\t%s\n) CHARACTER SET %s COLLATE %s ENGINE = %s"
            % (
                self._tb_name,
                ",\n\t".join(self._columns_syntax + self._indexes_syntax),
                self._charset,
                self._collate,
                self._engine,
            )
        )

    # Construct ----------------------------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    def _construct(
        self,
        columns: tuple,
        indexes: object,
        engine: object,
        charset: object,
        collate: object,
    ):
        # Valiate engine
        if not set_contains(settings.TEMPORARY_ENGINES, engine):
            raise errors.QueryValueError(
                "{}Invalid 'engine' {}, must be one of: {}".format(
                    self.err_pfix(),
                    repr(engine),
                    ", ".join(map(repr, settings.TEMPORARY_ENGINES)),
                )
            )
        self._engine = engine

        # Validate charset and collate
        if charset is None:
            self._charset = self._tb._charset
        elif is_str(charset):
            self._charset = charset
        else:
            raise errors.QueryValueError(
                "{}Invalid 'charset': {}".format(self.err_pfix(), repr(charset))
            )
        if collate is None:
            self._collate = self._tb._collate
        elif is_str(collate):
            self._collate = collate
        else:
            raise errors.QueryValueError(
                "{}Invalid 'collate': {}".format(self.err_pfix(), repr(collate))
            )
        try:
            charSet = self._tb._validate_charset_collate(self._charset, self._collate)
        except ValueError as err:
            raise errors.QueryValueError(
                "{}Invalid 'charset' and 'collate': {}".format(self.err_pfix(), err)
            ) from err
        self._charset = charSet._name
        self._collate = charSet._collate

        # Construct columns
        self._construct_columns(columns)

        # Construct indexes
        self._construct_indexes(indexes)

    @cython.cfunc
    @cython.inline(True)
    def _construct_columns(self, columns: tuple[str, Column]):
        # Given columns
        names: list
        syntax: list = []
        if columns:
            # Get column names & syntax
            names = []
            for col in columns:
                # . column instance
                if isinstance(col, Column):
                    list_append(names, access_column_name(col))
                    list_append(syntax, access_column_syntax(col))
                # . column <str>
                elif is_str(col):
                    c = self._tb._columns._get(col, None)
                    # . matched column
                    if c is not None:
                        list_append(names, access_column_name(c))
                        list_append(syntax, access_column_syntax(c))
                    # . column syntax
                    else:
                        c0 = col.split(" ")[0]
                        if not self._tb._columns._contains(c0):
                            raise errors.QueryValueError(
                                "{}Invalid column {}, does not belong to table {}.".format(
                                    self.err_pfix(), repr(c), self._tb._fname
                                )
                            )
                        list_append(names, c0)
                        list_append(syntax, col)
                # . invalid
                else:
                    raise errors.QueryValueError(
                        "{}Invalid column {}.".format(self.err_pfix(), repr(col))
                    )

            # Validate duplicates
            if utils._list_duplicated(names):
                dup = utils._list_duplicates(names)
                raise errors.QueryValueError(
                    "{}Invalid 'columns', contains duplicates: {}.".format(
                        self.err_pfix(), ", ".join(map(repr, dup))
                    )
                )

        # Default columns
        else:
            names = list(self._tb._columns._names)
            for col in self._tb._columns._instances:
                list_append(syntax, access_column_syntax(col))

        # Set columns & syntax
        self._columns = names
        self._columns_syntax = syntax

    @cython.cfunc
    @cython.inline(True)
    def _construct_indexes(self, indexes: object):
        # Given indexes
        names: list = []
        syntax: list = []

        # No index
        if indexes is None:
            pass

        # Auto indexes
        elif indexes == "auto":
            indexes = self._tb._indexes._search_by_columns(tuple(self._columns), True)
            for idx in indexes:
                list_append(names, access_index_name(idx))
                list_append(syntax, access_index_syntax(idx))

        # Specified indexes
        else:
            # . adjustment
            if is_str(indexes) or isinstance(indexes, Index):
                indexes = [indexes]
            # get index names & syntax
            try:
                for idx in indexes:
                    # . index instance
                    if isinstance(idx, Index):
                        list_append(names, access_index_name(idx))
                        list_append(syntax, access_index_syntax(idx))
                    # . index <str>
                    elif is_str(idx):
                        i = self._tb._indexes._get(idx, None)
                        # . matched index
                        if i is not None:
                            list_append(names, access_index_name(i))
                            list_append(syntax, access_index_syntax(i))
                        # . index syntax
                        else:
                            list_append(names, idx)
                            list_append(syntax, idx)
                    # . invalid
                    else:
                        raise errors.QueryValueError(
                            "{}Invalid index {}.".format(self.err_pfix(), repr(idx))
                        )
            except errors.QueryValueError:
                raise
            except Exception as err:
                raise errors.QueryValueError(
                    "{}Invalid 'indexes': {}".format(self.err_pfix(), err)
                ) from err

            # Validate duplicates
            if utils._list_duplicated(syntax):
                dup = utils._list_duplicates(syntax)
                raise errors.QueryValueError(
                    "{}Invalid 'indexes', contains duplicates: {}.".format(
                        self.err_pfix(), ", ".join(map(repr, dup))
                    )
                )

        # Set indexes & syntax
        self._indexes = names
        self._indexes_syntax = syntax


@cython.cclass
class CREATE_TEMP_VALUES(ValuesClause):
    """The `VALUES` clause of the MySQL CREATE TEMPORARY TABLE query.

    The `CREATE_TEMP_VALUES` clause is not a legit SQL clause but a custom
    design to facilitate batch inserts of values into the temporary table
    after the table is created.
    """

    def __init__(
        self,
        query: Query,
        values: object,
        *value_columns: Union[str, Column],
    ) -> None:
        temp_columns = query._get_create_temp(True)._columns
        super().__init__(
            query,
            values,
            value_columns=value_columns if value_columns else temp_columns,
            where_columns=None,
            auto_groupby_tabletime=False,
            auto_include_tabletime=False,
            alias=None,
        )
        # Validate columns
        for col in self._columns:
            if col in temp_columns:
                continue
            raise errors.QueryValueError(
                "{}Invalid 'value_columns', column {} does not "
                "belong to the temporary table: [{}]".format(
                    self.err_pfix(), repr(col), ", ".join(map(repr, temp_columns))
                )
            )


# Standard Query ================================================================================
@cython.cclass
class Query:
    """The base MySQL query of the Table."""

    # Query
    _name: str
    _db: Database
    _tb: Table
    _tier: cython.int
    # Clause
    _anchor: cython.int
    _clauses: list[Clause]
    _select_clauses: list[Clause]
    _union_select_clauses: list[Clause]
    """
    For Select query involves TimeTable(s) which `timetable` are specified 
    through `tabletimes()` method, `ORDER BY` and `LIMIT` clauses will 
    force the use of union select mode to avoid SQL syntax error.
    """
    _has_union_clause: cython.bint
    _force_union_mode: cython.bint
    _insert_clauses: list[Clause]
    _update_clauses: list[Clause]
    _delete_clauses: list[Clause]
    _join_clauses: list[JoinClause]
    _where_clauses: list[WhereClause]
    _select: SELECT
    _for_update: FOR_UPDATE
    _lock_in_share_mode: LOCK_IN_SHARE_MODE
    _insert: InsertClause
    _on_dup: ON_DUPLICATE_KEY
    _update: UPDATE
    _set: SET
    _delete: DELETE
    _create_temp: CREATE_TEMP
    _values: ValuesClause
    _tabletimes: TABLETIMES
    _custom: CUSTOM
    # TimeTable
    _query_timetables: cython.bint
    _select_timetables: cython.bint
    _timetables: dict[str, TimeTable]
    _timetable_fmts: dict[str, str]
    _timetable_units: dict[str, int]
    _timetable_joins: dict[str, int]
    _timetable_times: list[datetime.datetime]
    _timetable_repls: list[list[tuple(str, str)]]
    _timetable_processed: cython.bint
    _timetable_invalid: cython.bint
    # Subquery
    _is_subquery: cython.bint
    _subqueries: dict[str, Query]
    _subquery_repls: list[tuple(str, str)]
    _subquery_offset: cython.bint
    _subquery_processed: cython.bint
    _subquery_invalid: cython.bint
    # Statement
    _main_stmt: str
    _main_stmt_mode: cython.int
    """
    - 0: undetermined
    - 1: specifc table statement
    - 2: timetables statement
    - 3: specific table values
    - 4: timetables values
    """
    _select_stmt: str
    _select_stmt_mode: cython.int
    """
    - 0: undetermined
    - 1: specifc table statement
    - 2: timetables statement
    """
    _insert_stmt: str
    # Execute
    _semaphore: Semaphore
    _concurrency: cython.int
    _cursor: type[Cursor]
    _timeout: object
    _warnings: cython.bint
    _retry_on_error: cython.bint
    _retry_times: cython.int
    _min_wait_time: cython.double
    _max_wait_time: cython.double
    _affected_rows: cython.longlong
    # Stats
    _start_time: cython.double

    def __init__(self, name: str, table: Table) -> None:
        """MySQL Query
        :param table: `<Table>` The table that initiates the query.
        """
        # Query
        self._name = name
        self._tb = table
        self._db = table._db
        self._tier = 0
        # Clause
        self._anchor = 0
        self._clauses = []
        self._select_clauses = []
        self._union_select_clauses = []
        self._has_union_clause = False
        self._force_union_mode = False
        self._insert_clauses = []
        self._update_clauses = []
        self._delete_clauses = []
        self._join_clauses = []
        self._where_clauses = []
        self._select = None
        self._for_update = None
        self._lock_in_share_mode = None
        self._insert = None
        self._on_dup = None
        self._update = None
        self._set = None
        self._delete = None
        self._create_temp = None
        self._values = None
        self._tabletimes = None
        self._custom = None
        # TimeTable
        self._query_timetables = False
        self._select_timetables = False
        self._timetables = {}
        self._timetable_fmts = {}
        self._timetable_units = {}
        self._timetable_joins = {}
        self._timetable_times = []
        self._timetable_repls = []
        self._timetable_processed = False
        self._timetable_invalid = False
        # Subquery
        self._is_subquery = False
        self._subqueries = {}
        self._subquery_repls = []
        self._subquery_offset = False
        self._subquery_processed = False
        self._subquery_invalid = False
        # Statement
        self._main_stmt = None
        self._main_stmt_mode = 0
        self._select_stmt = None
        self._select_stmt_mode = 0
        self._insert_stmt = None
        # Execute
        self._semaphore = None
        self._concurrency = 10
        self._cursor = None
        self._timeout = None
        self._warnings = True
        self._retry_on_error = False
        self._retry_times = -1
        self._min_wait_time = 0.2
        self._max_wait_time = 2.0
        self._affected_rows = 0
        # Stats
        self._start_time = unixtime()

    # SQL methods --------------------------------------------------------------------------------
    def join(
        self,
        table: Union[str, Table, TimeTable, SelectQuery],
        *ons: str,
        args: Any = None,
        ins: Union[dict[str, Union[list, tuple]], None] = None,
        not_ins: Union[dict[str, Union[list, tuple]], None] = None,
        subqueries: Union[dict[str, SelectQuery], None] = None,
        method: Literal["INNER", "LEFT", "RIGHT"] = "INNER",
        tabletime: Union[str, datetime.date, datetime.datetime, None] = None,
        alias: Union[str, None] = None,
    ) -> Self:
        """The `JOIN` clause of the MySQL query.

        :param table: The table to join. This can be specified in two ways:
            - By providing the name `<str>` or instance `<Table/TimeTable>` of the
              table to join. This is equivalent to `"JOIN <table> ..."`.
            - By providing an instance of `SelectQuery` as a subquery. This
              will make the statement following the `JOIN` clause into a
              subquery. Equivalent to `"JOIN (SELECT... FROM ... )"`.

        :param ons: `<str>` Condition expressions: `"t1.id = t2.id"`, `"t2.age > 23"`, etc.
            - Each column should be manually prefixed with the correct alias.
              For more details about aliasing, please refer to the `alias`
              parameter.

        :param args: `<Any/None>` Arguments for the `'%s'` placeholders of 'ons'. Defaults to `None`.

        :param ins: `<dict/None>` The `IN` modifier. Defaults to `None`.
            - This parameter must be a dictionary with the column name as the key and
              iterable types such as `list`, `tuple`, `set`, `Series` as the value.
            - For example, `{"t2.id": [1, 2, 3]}` -> `"t2.id IN (1, 2, 3)"`. Duplicates
              are removed automatically.

        :param not_ins: `<dict/None>` The `NOT IN` modifier. Defaults to `None`.
            - Refert to 'ins' argument for more detail.

        :param subqueries: `<dict/None>` The subquery conditions. Defaults to `None`.
            - This parameter must be a dictionary with the column name along with
              the `desired operator` as the key and instances of `SelectQuery`
              as the value.
            - For example: `{"t2.name =": db.user.select("name").where("age > 23")}` ->
              `"t2.name = (SELECT name FROM db.user AS t1 WHERE age > 23)"`.

        :param method: `<str>` The join method. Defaults to `'INNER'`.

        :param tabletime: `<str/None>` A specific `tabletime` for the `JOIN` table. Defaults to `None`.
            - This parameter is only applicable when the argument 'table' corresponds
              to a TimeTable (regardless type of `<str>` or `<TimeTable>` instance).
            - If `tabletime` is specified, the actual sub-table will derive from this
              parameter. Otherwise, it is required to use `tabletimes()` method at to
              specify the sub-tables. For more details, please refer to the `tabltimes()`
              method.

        :param alias: `<str/None>` The alias of the `JOIN` clause. Defaults to `None`.
            - The alias of the clause will be added to the corresponding part of the SQL
              statement using the `'AS <alias>'` syntax.
            - For instance, in a `SELECT... FROM ... JOIN ...` query, without specified
              alias (default alias), the statement would be constructed as:
              `'SELECT ... FROM ... AS t1 JOIN ... AS t2'`, where default alias is derived
              from the order of the tables in the query.
            - However, with a user-defined alias (for example, `alias='join_tb'`), the
              statement would be constructed as:
              `'SELECT ... FROM ... AS t1 JOIN ... AS join_tb'`.

        ### Example (default alias & join table):
        >>> db.user.select(...).join(
                db.user_info, # also accepts "user_info" `<str>`
                "t1.id = t2.user_id", "t2.first_name = %s",
                args="John",
                ins={"t2.age": [23, 24, 25]}),
                method="INNER",
            )
        ### -> Equivalent to:
        >>> SELECT ... FROM db.user AS t1
            INNER JOIN db.user_info AS t2
                ON t1.id = t2.user_id
                AND t2.first_name = 'John'
                AND t2.age IN (23, 24, 25)

        ### Example (user-defined alias & join subquery):
        >>> db.user.select(..., alias="tb").join(
                db.user_info.select(...).where("age > 23"), # SelectQuery instance
                "t1.id = info.user_id", "info.first_name = %s", "info.last_name = %s",
                args=["John", "Wick"],
                ins={"info.age": [23, 24, 25]},
                method="INNER",
                alias="info",
            )
        ### -> Equivalent to:
        >>> SELECT ... FROM db.user AS tb
            INNER JOIN (SELECT ... FROM db.user_info WHERE age > 23) AS info
                ON tb.id = info.user_id
                AND info.first_name = 'John'
                AND info.last_name = 'Wick'
                AND info.age IN (23, 24, 25)
        """
        JOIN(
            self,
            table,
            *ons,
            args=args,
            ins=ins,
            not_ins=not_ins,
            subqueries=subqueries,
            method=method,
            tabletime=tabletime,
            alias=alias,
        )
        return self

    def join_time(
        self,
        column: Union[str, Column],
        start: Union[str, datetime.date, datetime.datetime, None] = None,
        end: Union[str, datetime.date, datetime.datetime, None] = None,
        days: Union[int, None] = None,
        unit: Literal["Y", "M", "D", "W", "h", "m", "s", "us"] = "us",
    ) -> Self:
        """The `TIME` clause trailing `JOIN` of the MySQL query.

        `join_time()` method is not a legit SQL clause but a custom design to facilitate
        construct of time range conditions. For instance, `join_time` can be used to
        construct the following syntax:
        `t2.<time_column> BETWEEN '2020-01-01' AND '2020-01-31'`.

        :param column: `<str/Column>` The name or instance of the column the time condition applies to.
            - Accepts either a `str` or a `Column` instance. This clause only
              supports `DATE`, `DATETIME`, `TIMESTAMP` MySQL column types.
            - Unlike other clauses, this clause automatically prefixes the appropriate
              alias to the column, eliminating the need for manual aliasing.

        :param start, end, days: The time span parameters.
            - If 'start' and 'end' are specified -> constrain between [start, ... end].
            - If 'start' and 'days' are specified -> constrain between [start, ... start + days - 1]
            - If only 'start' is specified -> constrain between [start, ... datetime.now()] or [datetime.now(), ... start]
            - If 'end' and 'days' are specified -> constrain between [end - days + 1, ... end]
            - If only 'end' is specified -> constrain between [datetime.now(), ... end] or [end, ... datetime.now()]
            - If only 'days' is specified -> constrain between [datetime.now() - days + 1, ... datetime.now()]
            - If none of the params are given -> raise `QueryValueError`.

        :param unit: `<str>` Adjust the time span parameters to match the specified unit. Defaults to `'us'`.
            - This parameter adjusts the 'start' and 'end' times after calculating the
              time span to align with the respective unit.
            - For example, if `unit='Y'`, `start` is adjusted to the beginning of the
              year `(XXXX-01-01 00:00:00)` and `end` is adjusted to the end of the year
              `(XXXX-12-31 23:59:59.999999)`. Similar adjustments are made for other
              units, e.g. `month` adjusts to the start and end of the month, etc.
            - Accepts: `'Y'`, `'M'`, `'D'`, `'W'`, `'h'`, `'m'`, `'s'`, `'us'`,
              where `'us'` means no adjustment to make.

        ### Example:
        >>> (
                db.user.select(...)
                .join(...)
                .join_time("create_dt", end="2023-05-15", days=180, unit="day")
                .join_time("update_dt", start="2023-01-01", end="2023-02-15", unit="month")
            )
        ### -> Equivalent to:
        >>> SELECT ... FROM db.user AS t1
            JOIN ... AS t2
                ON ...
                AND t2.create_dt BETWEEN '2022-11-17 00:00:00.000000' AND '2023-05-15 23:59:59.999999'
                # 'end' is adjusted to XXXX-XX-XX 23:59:59.999999 as `unit = 'day'`.
                # and alias 't2' is automatically prefixed to the column.
                AND t2.update_dt BETWEEN '2023-01-01 00:00:00.000000' AND '2023-02-28 23:59:59.999999'
                # 'end' is adjusted to XXXX-XX-28 23:59:59.999999 as `unit = 'month'`.
                # and alias 't2' is automatically prefixed to the column.
        """
        JOIN_TIME(self, column, start=start, end=end, days=days, unit=unit)
        return self

    def where(
        self,
        *conditions: str,
        args: Any = None,
        ins: dict[str, list | tuple] | None = None,
        not_ins: dict[str, list | tuple] | None = None,
        subqueries: dict[str, SelectQuery] | None = None,
    ) -> Self:
        """The `WHERE` clause of the MySQL query.

        :param conditions: `<str>` Condition expressions: `"id = 1"`, `"name = 'John'"`, `"COUNT(*) > 10"`, etc.
        :param args: `<Any/None>` Arguments for the `'%s'` placeholders of 'conditions'. Defaults to `None`.
        :param ins: `<dict/None>` The `IN` modifier. Defaults to `None`.
            - This parameter must be a dictionary with the column name as the key and
              iterable types such as `list`, `tuple`, `set`, `Series` as the value.
            - For example, `{"id": [1, 2, 3]}` -> `"id IN (1, 2, 3)"`. Duplicates
              are removed automatically.

        :param not_ins: `<dict/None>` The `NOT IN` modifier. Defaults to `None`.
            - Refert to 'ins' argument for more detail.

        :param subqueries: `<dict/None>` The subquery conditions. Defaults to `None`.
            - This parameter must be a dictionary with the column name along with
              the `desired operator` as the key and instances of `SelectQuery`
              as the value.

        ### Example:
        >>> db.user.select(...)
            .where(
                "t1.ranking > 3",
                "t1.department = %s",
                args="Sales",
                ins={"t1.id": [1, 2, 3]},
                subqueries={"t1.name IN": db.user_info.select("name").where("age > 23")}
            )
        ### -> Equivalent to:
        >>> SELECT ... FROM db.user AS t1
            WHERE t1.ranking > 3
                AND t1.department = 'Sales'
                AND t1.id IN (1, 2, 3)
                AND t1.name IN (SELECT name FROM db.user_info AS t1 WHERE age > 23)
        """
        WHERE(
            self,
            *conditions,
            args=args,
            ins=ins,
            not_ins=not_ins,
            subqueries=subqueries,
        )
        return self

    def where_time(
        self,
        column: Union[str, Column],
        start: Union[str, datetime.date, datetime.datetime, None] = None,
        end: Union[str, datetime.date, datetime.datetime, None] = None,
        days: Union[int, None] = None,
        unit: Literal["Y", "M", "D", "W", "h", "m", "s", "us"] = "us",
    ) -> Self:
        """The `TIME` clause trailing `WHERE` of the MySQL query.

        `where_time()` method is not a legit SQL clause but a custom design to facilitate
        construct of time range conditions. For instance, `where_time` can be used to
        construct the following syntax:
        `t2.<time_column> BETWEEN '2020-01-01' AND '2020-01-31'`.

        :param column: `<str/Column>` The name or instance of the column the time condition applies to.
            - Accepts either a `str` or a `Column` instance. This clause only
              supports `DATE`, `DATETIME`, `TIMESTAMP` MySQL column types.
            - Unlike other clauses, this clause automatically prefixes the appropriate
              alias to the column, eliminating the need for manual aliasing.

        :param start, end, days: The time span parameters.
            - If 'start' and 'end' are specified -> constrain between [start, ... end].
            - If 'start' and 'days' are specified -> constrain between [start, ... start + days - 1]
            - If only 'start' is specified -> constrain between [start, ... datetime.now()] or [datetime.now(), ... start]
            - If 'end' and 'days' are specified -> constrain between [end - days + 1, ... end]
            - If only 'end' is specified -> constrain between [datetime.now(), ... end] or [end, ... datetime.now()]
            - If only 'days' is specified -> constrain between [datetime.now() - days + 1, ... datetime.now()]
            - If none of the params are given -> raise `QueryValueError`.

        :param unit: `<str>` Adjust the time span parameters to match the specified unit. Defaults to `'us'`.
            - This parameter adjusts the 'start' and 'end' times after calculating the
              time span to align with the respective unit.
            - For example, if `unit='Y'`, `start` is adjusted to the beginning of the
              year `(XXXX-01-01 00:00:00)` and `end` is adjusted to the end of the year
              `(XXXX-12-31 23:59:59.999999)`. Similar adjustments are made for other
              units, e.g. `month` adjusts to the start and end of the month, etc.
            - Accepts: `'Y'`, `'M'`, `'D'`, `'W'`, `'h'`, `'m'`, `'s'`, `'us'`,
              where `'us'` means no adjustment to make.

        ### Example:
        >>> (
                db.user.select(...)
                .where(...)
                .where_time("create_dt", end="2023-05-15", days=180, unit="day")
                .where_time("update_dt", start="2023-01-01", end="2023-02-15", unit="month")
            )
        ### -> Equivalent to:
        >>> SELECT ... FROM db.user AS t1
            WHERE ...
                AND t1.create_dt BETWEEN '2022-11-17 00:00:00.000000' AND '2023-05-15 23:59:59.999999'
                # 'end' is adjusted to XXXX-XX-XX 23:59:59.999999 as `unit = 'day'`.
                # and alias 't1' is automatically prefixed to the column.
                AND t1.update_dt BETWEEN '2023-01-01 00:00:00.000000' AND '2023-02-28 23:59:59.999999'
                # 'end' is adjusted to XXXX-XX-28 23:59:59.999999 as `unit = 'month'`.
                # and alias 't1' is automatically prefixed to the column.
        """
        WHERE_TIME(self, column, start=start, end=end, days=days, unit=unit)
        return self

    def order_by(self, *orders: str) -> Self:
        """The `ORDER BY` clause of the MySQL query.

        :param orders: `<str>` The order expressions: `"id ASC"`, `"RAND()"`, `"LENGTH(name) ASC"`, etc.

        ### Notice:
        When the `order_by()` methods is used in `SELECT...`, `INSERT... SELECT...` or
        `CREATE TEMPORARY TABLE... SELECT...` query and the query involves TimeTable(s)
        which `timetable` are specified through `tabletimes()` method, the `ORDER BY`
        clause will force the use of union select mode to avoid SQL syntax error.
        This behavior also applies to the `limit()` method or when setting
        `select(distinct=True)`. For more details, please refer to the Example
        section below:

        ### Example (normal Table):
        >>> db.user.select(...).order_by("name ASC")
        ### -> Equivalent to:
        >>> SELECT ... FROM db.user AS t1 ORDER BY name ASC

        ### Exmaple (TimeTable with `tabletime()` method):
        >>> # Assume 'user_info' is a TimeTable with time_unit = 'month'.
            db.user_info.select(...)
            .order_by("name ASC")
            .tabletimes(start="2023-01-01", end="2024-01-01")
        ### -> Equivalent to:
        >>> SELECT * FROM (
                SELECT ... FROM db.user_info_2023_01 AS t1
                UNION ALL
                SELECT ... FROM db.user_info_2023_02 AS t1
                UNION ALL
                SELECT ... FROM db.user_info_2023_03 AS t1
                UNION ALL
                ...
            ) AS tb1 ORDER BY name ASC
        """
        ORDER_BY(self, *orders)
        return self

    def limit(self, *limits: int) -> Self:
        """The `LIMIT` clause of the MySQL query.

        :param limits: `<int>` The limit values.
            - If a single integer is provided, it specifies the maximum
              number of rows to return.
            - If two integers are provided, the first one specifies the
              offset of the first row to return, and the second one defines
              the maximum number of rows to return from that offset.

        ### Notice:
        When the `limit()` methods is used in `SELECT...`, `INSERT... SELECT...` or
        `CREATE TEMPORARY TABLE... SELECT...` query and the query involves TimeTable(s)
        which `timetable` are specified through `tabletimes()` method, the `LIMIT`
        clause will force the use of union select mode to avoid SQL syntax error.
        This behavior also applies to the `order_by()` method or when setting
        `select(distinct=True)`. For more details, please refer to the Example
        section below:

        ### Example (normal Table):
        >>> db.user.select(...).limit(10)
        ### -> Equivalent to:
        >>> SELECT ... FROM db.user AS t1 LIMIT 10

        ### Exmaple (TimeTable with `tabletime()` method):
        >>> # Assume 'user_info' is a TimeTable with time_unit = 'month'.
            db.user_info.select(...)
            .limit(10)
            .tabletimes(start="2023-01-01", end="2024-01-01")
        ### -> Equivalent to:
        >>> SELECT * FROM (
                SELECT ... FROM db.user_info_2023_01 AS t1
                UNION ALL
                SELECT ... FROM db.user_info_2023_02 AS t1
                UNION ALL
                SELECT ... FROM db.user_info_2023_03 AS t1
                UNION ALL
                ...
            ) AS tb1 LIMIT 10
        """
        LIMIT(self, *limits)
        return self

    def tabletimes(
        self,
        *times: Union[str, datetime.date, datetime.datetime, None],
        start: Union[str, datetime.date, datetime.datetime, None] = None,
        end: Union[str, datetime.date, datetime.datetime, None] = None,
        days: Union[int, None] = None,
    ) -> Self:
        """The `TABLETIMES` clause of the MySQL query.

        The `tabletimes()` method is not a legit SQL clause but a custom design to
        specific the desired sub-tables for query involves TimeTables. This clause
        is only applicable when the query involves at least one *unspecified*
        TimeTables.

        TimeTable act as a proxy to manage multiple sub-tables on the MySQL server,
        and itself does not corresponds to any specific tables. Therefore, when
        executing a query involves TimeTables, it is required to specify the
        desired sub-tables `tabletime` to construct the final SQL statement.
        There are two supported ways to specify the `tabletime` of TimeTable(s):
            - 1. Use the `'tabletime'` argument (not through this method) of the
                 clause to specify the exact sub-table. This approach allows joining
                 TimeTables with different 'time_unit' and mismatch of tabletimes,
                 but limits to only one sub-table per clause. For example:
                 db.select(..., tabletime="2023-01-01").join(..., tabletime="2023-02-02).
            - 2. Use the `tabletimes()` method to specify a range of sub-tables.
                 This approach allows specifying multiple sub-tables in one query
                 (either execute concurrently or `UNION` into one statement).
                 However, this approach requires all TimeTables to have the same
                 'time_unit' and only supports `INNER` join method on tables
                 at the same 'tabletime'. For example:
                 db.select(...).join(...).tabletimes(start="2023-01-01", end="2024-01-01).
            - 3. Both approaches can be used at the same time, and `timetables()`
                 will only apply to those TimeTables that are not specified by
                 the first approach ('tabletime' argument). For more details,
                 please refer to the Example section below.

        :param times: The specific times of the sub-tables.
            If not provided, will use 'start', 'end', and 'days' arguments to
            determine the time span and the corresponding sub-tables.

        :param start, end, days: The time span parameters.
            - If 'start' and 'end' are specified -> sub-tables between [start, ... end].
            - If 'start' and 'days' are specified -> sub-tables between [start, ... start + days - 1]
            - If only 'start' is specified -> sub-tables between [start, ... datetime.now()] or [datetime.now(), ... start]
            - If 'end' and 'days' are specified -> sub-tables between [end - days + 1, ... end]
            - If only 'end' is specified -> sub-tables between [datetime.now(), ... end] or [end, ... datetime.now()]
            - If only 'days' is specified -> sub-tables between [datetime.now() - days + 1, ... datetime.now()]
            - If none of the params are given -> raise `QueryValueError`.

        ### Example:
        >>> # Assume all of the follwong tables are TimeTables.
            # 'user' & 'user_status' with time_unit = 'month'.
            # 'user_info' with time_unit = 'year'.
            (
                db.user.select("t1.id", "t1.name", "t2.age", "t3.status")
                # 'user' tabletime is unspecified.
                .join(db.user_info, "t1.id == t2.user_id", method="LEFT", tabletime='2023-01-01')
                # 'user_info' tabletime is specified to '2023-01-01' (Approach 1).
                # `LEFT/RIGHT` join is supported and 'user_info' can have different
                # 'time_unit' from tables 'user' & 'user_status'.
                .join(db.user_status, "t1.id == t3.user_id", method="INNER")
                # 'user_status' tabletime is unspecified.
                .tabletimes(start='2023-01-01', end='2023-03-01')
                # `tabletimes()` method will apply to both 'user' & 'user_status'
                # tables (Approach 2). Both TimeTable must have the same 'time_unit'
                # and only `INNER` join is supported.
            )
        ### -> Equivalent to THREE independent queries (concurrent execution mode):
        >>> SELECT t1.id, t1.name, t2.age, t3.status FROM user_202301 AS t1
            LEFT JOIN user_info_2023 AS t2 ON t1.id = t2.user_id
            INNER JOIN user_status_202301 AS t3 ON t1.id = t3.user_id
        >>> SELECT t1.id, t1.name, t2.age, t3.status FROM user_202302 AS t1
            LEFT JOIN user_info_2023 AS t2 ON t1.id = t2.user_id
            INNER JOIN user_status_202302 AS t3 ON t1.id = t3.user_id
        >>> SELECT t1.id, t1.name, t2.age, t3.status FROM user_202303 AS t1
            LEFT JOIN user_info_2023 AS t2 ON t1.id = t2.user_id
            INNER JOIN user_status_202303 AS t3 ON t1.id = t3.user_id
        ### -> Equivalent to ONE union query (`UNION` execution mode):
        >>> SELECT t1.id, t1.name, t2.age, t3.status FROM user_202301 AS t1
            LEFT JOIN user_info_2023 AS t2 ON t1.id = t2.user_id
            INNER JOIN user_status_202301 AS t3 ON t1.id = t3.user_id
            UNION ALL
            SELECT t1.id, t1.name, t2.age, t3.status FROM user_202302 AS t1
            LEFT JOIN user_info_2023 AS t2 ON t1.id = t2.user_id
            INNER JOIN user_status_202302 AS t3 ON t1.id = t3.user_id
            UNION ALL
            SELECT t1.id, t1.name, t2.age, t3.status FROM user_202303 AS t1
            LEFT JOIN user_info_2023 AS t2 ON t1.id = t2.user_id
            INNER JOIN user_status_202303 AS t3 ON t1.id = t3.user_id
        """
        TABLETIMES(self, *times, start=start, end=end, days=days)
        return self

    def custom(
        self,
        stmt: str,
        args: Union[list, tuple, None] = None,
    ) -> Self:
        """The `CUSTOM` clause of the MySQL query.

        The `custom()` method is not a legit SQL clause but a custom design
        to execute complex SQL statements when the built-in methods are not
        sufficient to handle the problem. *Note: When this method is used,
        all other clauses in the query will be ignored.

        :param stmt: `<str>` The custom SQL statement to execute.
            - This should be a complete SQL statement that can directly be
              executed by MySQL.
            - Table names are particularly important when working with TimeTable
              as it acts as a proxy to manage multiple sub-tables and only the actual
              sub-table names are recognized by MySQL. Use `tb.get_name()` or
              `tb.get_fname()` method to generate the corresponding sub-table name.
            - For normal Table, the table instance itself will format to the full
              table name, e.g. `'<db_name>.<tb_name>'`.

        :param args: `<list/tuple>` Arguments for the `'%s'` placeholders in 'stmt'. Defaults to `None`.

        ### Example:
        >>> # Assume 'user' is a normal Table
            # and 'user_info' is a TimeTable.
            db.select().custom(
                f'''
                SELECT t1.id, t1.name. t2.age, t3.status
                FROM {db.user} AS t1
                INNER JOIN {db.user_info.get_fname("2023-06")} AS t2
                    ON t1.id = t2.user_id
                WHERE t1.id IN %s
                    AND t1.create_dt >= %s
                ''',
                # Normal Table 'user' will be formatted to '<db_name>.<tb_name>'
                # TimeTable 'user_info' requires the actual sub-table name.
                args=[[1, 2, 3], datetime.datetime(2023, 1, 1)]
                # [1, 2, 3] `<list/tuple>` will be formatted to '(1, 2, 3)'
                # so the parenthesis for `'WHERE t1.id IN %s'` should be omitted.
            )
        ### -> Equivalent to:
        >>> SELECT t1.id, t1.name. t2.age, t3.status
            FROM db.user AS t1
            INNER JOIN db.user_info_202306 AS t2
                ON t1.id = t2.user_id
            WHERE t1.id IN (1, 2, 3)
                AND t1.create_dt >= '2023-01-01 00:00:00'
        """
        CUSTOM(self, stmt, args=args)
        return self

    # Clause -------------------------------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def _add_clause(self, clause: Clause, anchor: cython.bint):
        "(cfunc) Add a clause to the query."
        # Add clause
        list_append(self._clauses, clause)
        if anchor:
            self._anchor += 1

        # Catagorize clause
        name = clause._name
        if dict_contains(SELECT_CLAUSES, name):
            list_append(self._select_clauses, clause)
        if dict_contains(INSERT_CLAUSES, name):
            list_append(self._insert_clauses, clause)
        if dict_contains(UPDATE_CLAUSES, name):
            list_append(self._update_clauses, clause)
        if dict_contains(DELETE_CLAUSES, name):
            list_append(self._delete_clauses, clause)

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def _get_clause(self, idx: cython.int) -> Clause:
        "(cfunc) Get a clause from the query `<Clause>`."
        if list_len(self._clauses) >= idx + 1:
            return self._clauses[idx]
        else:
            return None

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def _get_timeclause(self, idx: cython.int) -> TimeClause:
        "(cfunc) Get a time clause from the query `<TimeClause>`."
        if list_len(self._clauses) >= idx + 1:
            clause = self._clauses[idx]
            return clause if isinstance(clause, TimeClause) else None
        else:
            return None

    @cython.cfunc
    @cython.inline(True)
    def _set_select(self, clause: SELECT):
        "(cfunc) Set the `SELECT` clause of the query."
        if self._select is not None:
            raise errors.QueryValueError(
                "{}`SELECT` clause already exists.".format(self._err_pfix())
            )
        self._select = clause

    @cython.cfunc
    @cython.inline(True)
    def _get_select(self, raise_error: cython.bint) -> SELECT:
        """(cfunc) Get the `SELECT` clause of the query.
        :param raise_error: `<bool>` Whether to raise `QueryValueError` if the clause does not exists.
        """
        if raise_error and self._select is None:
            raise errors.QueryValueError(
                "{}`SELECT` clause does not exist.".format(self._err_pfix())
            )
        return self._select

    @cython.cfunc
    @cython.inline(True)
    def _set_for_update(self, clause: FOR_UPDATE):
        "(cfunc) Set the `FOR UPDATE` clause of the query."
        if self._for_update is not None:
            raise errors.QueryValueError(
                "{}`FOR UPDATE` clause already exists.".format(self._err_pfix())
            )
        if self._lock_in_share_mode is not None:
            raise errors.QueryValueError(
                "{}`FOR UPDATE` cluase cannot be used along with "
                "`LOCK IN SHARE MODE` clause.".format(self._err_pfix())
            )
        self._for_update = clause

    @cython.cfunc
    @cython.inline(True)
    def _get_for_update(self, raise_error: cython.bint) -> FOR_UPDATE:
        """(cfunc) Get the `FOR UPDATE` clause of the query.
        :param raise_error: `<bool>` Whether to raise `QueryValueError` if the clause does not exists.
        """
        if raise_error and self._for_update is None:
            raise errors.QueryValueError(
                "{}`FOR UPDATE` clause does not exist.".format(self._err_pfix())
            )
        return self._for_update

    @cython.cfunc
    @cython.inline(True)
    def _set_lock_in_share_mode(self, clause: LOCK_IN_SHARE_MODE):
        "(cfunc) Set the `LOCK IN SHARE MODE` clause of the query."
        if self._lock_in_share_mode is not None:
            raise errors.QueryValueError(
                "{}`LOCK IN SHARE MODE` clause already exists.".format(self._err_pfix())
            )
        if self._for_update is not None:
            raise errors.QueryValueError(
                "{}`LOCK IN SHARE MODE` clause cannot be used along "
                "with `FOR UPDATE` clause.".format(self._err_pfix())
            )
        self._lock_in_share_mode = clause

    @cython.cfunc
    @cython.inline(True)
    def _get_lock_in_share_mode(self, raise_error: cython.bint) -> LOCK_IN_SHARE_MODE:
        """(cfunc) Get the `LOCK IN SHARE MODE` clause of the query.
        :param raise_error: `<bool>` Whether to raise `QueryValueError` if the clause does not exists.
        """
        if raise_error and self._lock_in_share_mode is None:
            raise errors.QueryValueError(
                "{}`LOCK IN SHARE MODE` clause does not exist.".format(self._err_pfix())
            )
        return self._lock_in_share_mode

    @cython.cfunc
    @cython.inline(True)
    def _set_insert(self, clause: InsertClause):
        "(cfunc) Set the `INSERT/REPLACE` clause of the query."
        if self._insert is not None:
            raise errors.QueryValueError(
                "{}`INSERT/REPLACE` clause already exists.".format(self._err_pfix())
            )
        self._insert = clause

    @cython.cfunc
    @cython.inline(True)
    def _get_insert(self, raise_error: cython.bint) -> InsertClause:
        """(cfunc) Get the `INSERT/REPLACE` clause of the query.
        :param raise_error: `<bool>` Whether to raise `QueryValueError` if the clause does not exists.
        """
        if raise_error and self._insert is None:
            raise errors.QueryValueError(
                "{}`INSERT` clause does not exist.".format(self._err_pfix())
            )
        return self._insert

    @cython.cfunc
    @cython.inline(True)
    def _set_on_dup(self, clause: ON_DUPLICATE_KEY):
        "(cfunc) Set the `ON DUPLICATE KEY` clause of the query."
        if self._on_dup is not None:
            raise errors.QueryValueError(
                "{}`ON DUPLICATE KEY` clause already exists.".format(self._err_pfix())
            )
        self._on_dup = clause

    @cython.cfunc
    @cython.inline(True)
    def _get_on_dup(self, raise_error: cython.bint) -> ON_DUPLICATE_KEY:
        """(cfunc) Get the `ON DUPLICATE KEY` clause of the query.
        :param raise_error: `<bool>` Whether to raise `QueryValueError` if the clause does not exists.
        """
        if raise_error and self._on_dup is None:
            raise errors.QueryValueError(
                "{}`ON DUPLICATE KEY` clause does not exist.".format(self._err_pfix())
            )
        return self._on_dup

    @cython.cfunc
    @cython.inline(True)
    def _set_update(self, clause: UPDATE):
        "(cfunc) Set the `UPDTE` clause of the query."
        if self._update is not None:
            raise errors.QueryValueError(
                "{}`UPDATE` clause already exists.".format(self._err_pfix())
            )
        self._update = clause

    @cython.cfunc
    @cython.inline(True)
    def _get_update(self, raise_error: cython.bint) -> UPDATE:
        """(cfunc) Get the `UPDTE` clause of the query.
        :param raise_error: `<bool>` Whether to raise `QueryValueError` if the clause does not exists.
        """
        if raise_error and self._update is None:
            raise errors.QueryValueError(
                "{}`UPDATE` clause does not exist.".format(self._err_pfix())
            )
        return self._update

    @cython.cfunc
    @cython.inline(True)
    def _set_clause_set(self, clause: SET):
        "(cfunc) Set the `SET` clause of the query."
        if self._set is not None:
            raise errors.QueryValueError(
                "{}`SET` clause already exists.".format(self._err_pfix())
            )
        self._set = clause

    @cython.cfunc
    @cython.inline(True)
    def _get_clause_set(self, raise_error: cython.bint) -> SET:
        """(cfunc) Get the `SET` clause of the query.
        :param raise_error: `<bool>` Whether to raise `QueryValueError` if the clause does not exists.
        """
        if raise_error and self._set is None:
            raise errors.QueryValueError(
                "{}`SET` clause does not exist.".format(self._err_pfix())
            )
        return self._set

    @cython.cfunc
    @cython.inline(True)
    def _set_delete(self, clause: DELETE):
        "(cfunc) Set the `DELETE` clause of the query."
        if self._delete is not None:
            raise errors.QueryValueError(
                "{}`DELETE` clause already exists.".format(self._err_pfix())
            )
        self._delete = clause

    @cython.cfunc
    @cython.inline(True)
    def _get_delete(self, raise_error: cython.bint) -> DELETE:
        """(cfunc) Get the `DELETE` clause of the query.
        :param raise_error: `<bool>` Whether to raise `QueryValueError` if the clause does not exists.
        """
        if raise_error and self._delete is None:
            raise errors.QueryValueError(
                "{}`DELETE` clause does not exist.".format(self._err_pfix())
            )
        return self._delete

    @cython.cfunc
    @cython.inline(True)
    def _set_create_temp(self, clause: CREATE_TEMP):
        "(cfunc) Set the `CREATE TEMPORARY TABLE` clause of the query."
        if self._create_temp is not None:
            raise errors.QueryValueError(
                "{}`CREATE TEMPORARY TABLE` clause already "
                "exists.".format(self._err_pfix())
            )
        self._create_temp = clause

    @cython.cfunc
    @cython.inline(True)
    def _get_create_temp(self, raise_error: cython.bint) -> CREATE_TEMP:
        """(cfunc) Get the `CREATE TEMPORARY TABLE` clause of the query.
        :param raise_error: `<bool>` Whether to raise `QueryValueError` if the clause does not exists.
        """
        if raise_error and self._create_temp is None:
            raise errors.QueryValueError(
                "{}`CREATE TEMPORARY TABLE` clause does not "
                "exist.".format(self._err_pfix())
            )
        return self._create_temp

    @cython.cfunc
    @cython.inline(True)
    def _set_values(self, clause: ValuesClause):
        "(cfunc) Set the `VALUES` clause of the query."
        if self._values is not None:
            raise errors.QueryValueError(
                "{}`VALUES` clause already exists.".format(self._err_pfix())
            )
        self._values = clause

    @cython.cfunc
    @cython.inline(True)
    def _get_insert_values(self, raise_error: cython.bint) -> INSERT_VALUES:
        """(cfunc) Get the `VALUES` clause of the INSERT query.
        :param raise_error: `<bool>` Whether to raise `QueryValueError` if the clause does not exists.
        """
        if raise_error and self._values is None:
            raise errors.QueryValueError(
                "{}`VALUES` clause does not exist.".format(self._err_pfix())
            )
        return self._values

    @cython.cfunc
    @cython.inline(True)
    def _get_update_values(self, raise_error: cython.bint) -> UPDATE_VALUES:
        """(cfunc) Get the `VALUES` clause of the UPDATE query.
        :param raise_error: `<bool>` Whether to raise `QueryValueError` if the clause does not exists.
        """
        if raise_error and self._values is None:
            raise errors.QueryValueError(
                "{}`VALUES` clause does not exist.".format(self._err_pfix())
            )
        return self._values

    @cython.cfunc
    @cython.inline(True)
    def _get_delete_values(self, raise_error: cython.bint) -> DELETE_VALUES:
        """(cfunc) Get the `VALUES` clause of the DELETE query.
        :param raise_error: `<bool>` Whether to raise `QueryValueError` if the clause does not exists.
        """
        if raise_error and self._values is None:
            raise errors.QueryValueError(
                "{}`VALUES` clause does not exist.".format(self._err_pfix())
            )
        return self._values

    @cython.cfunc
    @cython.inline(True)
    def _get_create_temp_values(self, raise_error: cython.bint) -> CREATE_TEMP_VALUES:
        """(cfunc) Get the `VALUES` clause of the CREATE TEMPORARY TABLE query.
        :param raise_error: `<bool>` Whether to raise `QueryValueError` if the clause does not exists.
        """
        if raise_error and self._values is None:
            raise errors.QueryValueError(
                "{}`VALUES` clause does not exist.".format(self._err_pfix())
            )
        return self._values

    @cython.cfunc
    @cython.inline(True)
    def _set_tabletimes(self, clause: TABLETIMES):
        "(cfunc) Set the `TABLETIMES` clause of the query."
        if self._tabletimes is not None:
            raise errors.QueryValueError(
                "{}`TABLETIMES` clause already exists.".format(self._err_pfix())
            )
        self._tabletimes = clause

    @cython.cfunc
    @cython.inline(True)
    def _get_tabletimes(self, raise_error: cython.bint) -> TABLETIMES:
        """(cfunc) Get the `TABLETIMES` clause of the query.
        :param raise_error: `<bool>` Whether to raise `QueryValueError` if the clause does not exists.
        """
        if raise_error and self._tabletimes is None:
            raise errors.QueryValueError(
                "{}`TABLETIMES` clause does not exist.".format(self._err_pfix())
            )
        return self._tabletimes

    @cython.cfunc
    @cython.inline(True)
    def _set_custom(self, clause: CUSTOM):
        "(cfunc) Set the `CUSTOM` clause of the query."
        if self._custom is not None:
            raise errors.QueryValueError(
                "{}`CUSTOM` clause already exists.".format(self._err_pfix())
            )
        self._custom = clause

    @cython.cfunc
    @cython.inline(True)
    def _get_custom(self, raise_error: cython.bint) -> CUSTOM:
        """(cfunc) Get the `CUSTOM` clause of the query.
        :param raise_error: `<bool>` Whether to raise `QueryValueError` if the clause does not exists.
        """
        if raise_error and self._custom is None:
            raise errors.QueryValueError(
                "{}`CUSTOM` clause does not exist.".format(self._err_pfix())
            )
        return self._custom

    @cython.cfunc
    @cython.inline(True)
    def _get_select_clauses(self, union_mode: cython.bint) -> list:
        """(cfunc) Get `SELECT` query clauses in order `<list[Clause]>`

        :param union_mode: `<bool>` Whether to get select clauses in union mode.
            - If `True`, returns select clauses excluding `HAVING`, `ORDER BY`, `LIMIT`.
            - If `False`, returns all select clauses.
        """
        if union_mode and self._has_union_clause:
            clauses: list = []
            for clause in self._select_clauses:
                if clause not in self._union_select_clauses:
                    list_append(clauses, clause)
            return self._sort_clauses(clauses, SELECT_CLAUSES)
        else:
            return self._sort_clauses(self._select_clauses, SELECT_CLAUSES)

    @cython.cfunc
    @cython.inline(True)
    def _get_union_select_clauses(self) -> list:
        """(cfunc) Get `SELECT` query union clauses in order `<list[Clause]>`

        Expect cluases: `ORDER BY`, `LIMIT`.
        """
        return self._sort_clauses(self._union_select_clauses, SELECT_CLAUSES)

    @cython.cfunc
    @cython.inline(True)
    def _get_insert_clauses(self) -> list:
        "(cfunc) Get `INSERT/REPLACE` query clauses in order `<list[Clause]>`"
        return self._sort_clauses(self._insert_clauses, INSERT_CLAUSES)

    @cython.cfunc
    @cython.inline(True)
    def _get_update_clauses(self) -> list:
        "(cfunc) Get `UPDATE` query clauses in order `<list[Clause]>`"
        return self._sort_clauses(self._update_clauses, UPDATE_CLAUSES)

    @cython.cfunc
    @cython.inline(True)
    def _get_delete_clauses(self) -> list:
        "(cfunc) Get `DELETE` query clauses in order `<list[Clause]>`"
        return self._sort_clauses(self._delete_clauses, DELETE_CLAUSES)

    # Utils --------------------------------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def _err_pfix(self) -> str:
        "(cfunc) Error prefix `<str>`."
        if self._is_subquery:
            return "<%s> [Subquery Select]\nError: " % self._tb._fname
        else:
            return "<%s> [%s]\nError: " % (self._tb._fname, self._name)

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def _gen_default_alias(self) -> str:
        "(cfunc) Generate default alias for the clause based on 'anchor' count `<str>`."
        return "t" + str(self._anchor)

    @cython.cfunc
    @cython.inline(True)
    def _chunk_values(self, values: list, batch_size: cython.int) -> list:
        "(cfunc) Check values into batches"
        return utils._chunk_list(values, batch_size, -1)

    @cython.cfunc
    @cython.inline(True)
    def _sort_clauses(self, clauses: list[Clause], orders: dict[str, int]) -> list:
        "(cfunc) Sort query clauses into the designed orders `<list[Clause]>."
        try:
            clauses.sort(key=lambda x: (orders[x.name], x.index))
            return clauses
        except Exception as err:
            raise errors.QueryValueError(
                "{}Query construction failed, cannot sort clause "
                "orders: {}".format(self._err_pfix(), err)
            ) from err

    @cython.cfunc
    @cython.inline(True)
    def _replace_plhs(self, stmt: str, repls: list[tuple(str, str)]) -> str:
        "(cfunc) Replace placeholders in the statement `<str>`."
        for pair in repls:
            phl = cython.cast(object, tuple_getitem(pair, 0))
            val = cython.cast(object, tuple_getitem(pair, 1))
            stmt = uni_replace(stmt, phl, val, -1)
        return stmt

    @cython.cfunc
    @cython.inline(True)
    def _escape_item(self, item: object) -> str:
        "(cfunc) Escape item to Literal string `<str>`."
        return self._db._escape_item(item)

    @cython.cfunc
    @cython.inline(True)
    def _escape_args(self, args: object) -> object:
        """(cfunc) Escape arguments to literal `<tuple/dict>`.

        - If the given 'args' is type of `<dict>`, returns `<dict>`.
        - All other supported data types returns `<tuple>`.
        """
        return self._db._escape_args(args)

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def _join_clause_syntax(self, clauses: list) -> str:
        syntax: list = []
        for clause in clauses:
            list_append(syntax, clause.syntax)
        return "\n".join(syntax)

    # Process timetable --------------------------------------------------------------------------
    async def _process_timetable(self) -> None:
        "(Internal) Process timetable arguments."
        # Already processed
        if self._timetable_processed:
            return None

        # No timetable involved
        if not self._query_timetables:
            self._timetable_processed = True
            return None

        # Validate timetable times
        if not self._timetable_times:
            raise errors.QueryValueError(
                "{}Query involves TimeTable(s) that have unspecifed "
                "`tabletime`. For more information, please refer to "
                "the `tabletimes()` method.".format(self._err_pfix())
            )

        # Validate timetable units
        if dict_len(self._timetable_units) > 1:
            confl: list = []
            for unit, idx in dict_items(self._timetable_units):
                clause = self._get_clause(idx)
                info = "- no.%d `%s` table '%s' (time_unit='%s')" % (
                    idx + 1,
                    clause._name,
                    clause._tb._name,
                    unit,
                )
                list_append(confl, info)
            raise errors.QueryValueError(
                "{}When using `tabletimes()` method to specify 'tabletime', all unspecified "
                "TimeTables must have the same 'time_unit'. For exmaple, TimeTable "
                "(time_unit='week') cannot only join with TimeTable (time_unit='month').\n"
                "Conflicting TimeTables:\n{}".format(self._err_pfix(), "\n".join(confl))
            )

        # Validate join methods
        if self._timetable_joins and (
            dict_len(self._timetable_joins) > 1
            or not dict_contains(self._timetable_joins, "INNER")
        ):
            confl: list = []
            for method, idx in dict_items(self._timetable_joins):
                if method == "INNER":
                    continue
                clause = self._get_clause(idx)
                info = "- no.%d `%s JOIN` table '%s'" % (
                    idx + 1,
                    method,
                    clause._tb._name,
                )
                list_append(confl, info)
            raise errors.QueryValueError(
                "{}When using `tabletimes()` method to specify 'tabletime', 'INNER' is the "
                "only acceptable join method between TimeTables. Other join methods will "
                "cause unexpected query result (espcially when the sub-tables don't exist).\n"
                "JOIN methods to change:\n{}".format(self._err_pfix(), "\n".join(confl))
            )

        # Validate `LOCK` clause
        if self._for_update is not None or self._lock_in_share_mode is not None:
            raise errors.QueryValueError(
                "{}When using `tabletimes()` method to specify 'tabletime', "
                "`FOR UPDATE` or `LOCK IN SHARE MODE` clauses is not "
                "allowed.".format(self._err_pfix())
            )

        # Process timetable replacements
        self._timetable_repls = await self._process_timetable_repls()
        self._timetable_processed = True

    @query_exc_handler()
    async def _process_timetable_repls(self) -> list[list[tuple[str, str]]]:
        "(Internal) Process (generate) timetable replacements `<list>`."
        # Replacements
        repls: list = []

        # Get timetables
        timetbs: dict[str, TimeTable] = self._timetables.copy()

        # Exclude insert timetable
        insert_clause = self._get_insert(False)
        insert_tb: TimeTable
        if insert_clause is not None:
            insert_tb = timetbs.pop(insert_clause._tb_name, None)
        else:
            insert_tb = None

        # Insert clause corresponds to the only timetable
        if not timetbs and insert_tb is not None:
            # . construct sub-table
            names: list = insert_tb._get_names(
                self._timetable_times, None, None, -1, False, set()
            )

            # . validate
            if not names:
                logger.warning(
                    "{}The `tabletime` of the `INSERT` table is not specified. As a result, "
                    "the query will not be executed. If this is not expected, please examine "
                    "the query arguments.".format(self._err_pfix())
                )
                self._timetable_invalid = True
                return repls

            # . construct placeholders
            phl = insert_clause._tb_name

            # . construct replacements
            for name in names:
                list_append(repls, [(phl, name)])
            return repls

        # At least one timetable coresponds to other clause
        else:
            # . construct sub-table times {datetime: tb_name}
            tasks = [
                tb.get_names(
                    *self._timetable_times, with_dt=True, invert=True, filter=True
                )
                for tb in timetbs.values()
            ]
            time_names: list[dict[datetime.datetime, str]] = await gather(*tasks)

            # . validate
            if not time_names:
                if self._warnings:
                    logger.warning(
                        "{}Based on the query arguments, target sub-tables do not "
                        "exist on the MySQL server. As a result, the query will not "
                        "be executed. If this is not expected, please examine the "
                        "query arguments.".format(self._err_pfix())
                    )
                self._timetable_invalid = True
                return repls
            intersect_times: set = set.intersection(
                *[set(dict_keys(i)) for i in time_names]
            )
            if not intersect_times:
                if self._warnings:
                    logger.warning(
                        "{}Based on the query arguments, target sub-tables do not "
                        "exist on the MySQL server. As a result, the query will not "
                        "be executed. If this is not expected, please examine the "
                        "query arguments.".format(self._err_pfix())
                    )
                self._timetable_invalid = True
                return repls

            # . construct placeholders
            phls: list = dict_keys(timetbs)

            # . add back insert timetable
            time_name: dict
            if insert_tb is not None:
                time_name = {}
                for time in intersect_times:
                    dict_setitem(time_name, time, insert_tb._get_name(time))
                list_append(time_names, time_name)
                list_append(phls, insert_clause._tb_name)

            # . filter timetable times
            time_repls: list[dict] = []
            time_repl: dict
            for time_name, phl in zip(time_names, phls):
                time_repl = {}
                for time, name in dict_items(time_name):
                    if set_contains(intersect_times, time):
                        dict_setitem(time_repl, time, (phl, name))
                list_append(time_repls, time_repl)

            # . construct replacements
            temp: list
            for time in sorted(intersect_times):
                temp = []
                for time_repl in time_repls:
                    list_append(temp, time_repl[time])
                list_append(repls, temp)
            return repls

    # Process subquery ---------------------------------------------------------------------------
    async def _process_subquery(self) -> None:
        "(Internal) Process subquery arguments."
        # Already processed
        if self._subquery_processed:
            return None

        # No subquery involved
        if not self._subqueries:
            self._subquery_processed = True
            return None

        # Construct subquery
        tasks = [
            i._construct_subquery(self._subquery_offset)
            for i in self._subqueries.values()
        ]
        try:
            subqueries: list = await gather(*tasks)
        except errors.QueryValueError:
            raise
        except Exception as err:
            raise errors.QueryValueError(
                "{}Subquery construction failed: {}".format(self._err_pfix(), err)
            ) from err

        # Process subquery replacements
        if all(subqueries):
            repls: list = []
            for phl, val in zip(dict_keys(self._subqueries), subqueries):
                list_append(repls, (phl, val))
            self._subquery_repls = repls
        else:
            self._subquery_repls = []
            self._subquery_invalid = True
        self._subquery_processed = True

    async def _construct_subquery(self, offset: cython.bint) -> str:
        """(Internal) Construct into subquery statement `<str>`.
        (Only applicable for `SelectQuery`).
        """
        stmt: str = await self._construct_select_stmt(True)
        if stmt:
            embed_tier: cython.int = self._tier + 1 if offset else self._tier
            offset1: str = "\t" * (embed_tier)
            offset2: str = "\t" * (embed_tier - 1)
            return "(\n%s%s\n%s)" % (
                offset1,
                uni_replace(stmt, "\n", "\n" + offset1, -1),
                offset2,
            )
        else:
            return ""

    # Construct select statement -----------------------------------------------------------------
    async def _construct_select_stmt(self, union_mode: cython.bint) -> str:
        "(cfunc) Construct the `SELECT` statement `<str>`."
        # Construct statement
        self._select_stmt = ""
        stmt: str
        try:
            # . custom statement
            if self._custom is not None:
                stmt = self._construct_custom_stmt()
                self._select_stmt_mode = 1
            # . select timetable
            elif self._select_timetables:
                await self._process_timetable()
                if self._timetable_invalid:
                    return ""  # exit
                elif union_mode or self._force_union_mode:
                    stmt = self._construct_select_union()
                else:
                    stmt = self._construct_select_indep(2)
            # . select normal table
            else:
                stmt = self._construct_select_indep(1)
        except errors.QueryValueError:
            raise
        except Exception as err:
            raise errors.QueryValueError(
                "{}Query construction failed: {}".format(self._err_pfix(), err)
            ) from err

        # Replace subqueries
        await self._process_subquery()
        if self._subquery_invalid:
            return ""  # exit
        if self._subquery_repls:
            stmt = self._replace_plhs(stmt, self._subquery_repls)

        # Set & return statement
        self._select_stmt = stmt
        return stmt

    @cython.cfunc
    @cython.inline(True)
    def _construct_select_indep(self, mode: cython.int) -> str:
        "(cfunc) Construct the `SELECT` statement in mode 1 `<str>`.)"
        # Set statement mode
        self._select_stmt_mode = mode

        # Construct statement
        clauses = self._get_select_clauses(False)
        return self._join_clause_syntax(clauses)

    @cython.cfunc
    @cython.inline(True)
    def _construct_select_union(self) -> str:
        "(cfunc) Construct the `SELECT` statement in mode 2 `<str>`.)"
        # Set statement mode
        self._select_stmt_mode = 1

        # Construct base statement
        clauses = self._get_select_clauses(True)
        syntax: list = []
        for clause in clauses:
            if clause == "SELECT":
                list_append(syntax, self._select.gen_subtable_syntax())
            else:
                list_append(syntax, clause.syntax)
        stmt: str = "\n".join(syntax)

        # Construct union statement
        stmts: list = []
        for repls in self._timetable_repls:
            list_append(stmts, self._replace_plhs(stmt, repls))
        stmt = "\nUNION ALL\n".join(stmts)

        # Adjust union statement
        if self._force_union_mode:
            # . set subquery offset
            self._subquery_offset = True
            # . get union select
            union_select = self._select.gen_union_select()
            # . construct union clauses
            union_clauses: str
            if self._has_union_clause:
                clauses = self._get_union_select_clauses()
                union_clauses = "\n" + self._join_clause_syntax(clauses)
            else:
                union_clauses = ""
            # . construct union statement
            return "%s (\n\t%s\n) AS %s%s" % (
                union_select,
                uni_replace(stmt, "\n", "\n\t", -1),
                self._select._alias,
                union_clauses,
            )
        else:
            self._subquery_offset = False
            return stmt

    # Construct main statement -------------------------------------------------------------------
    async def _construct_main_stmt(self) -> str:
        """Construct the final query statement `<str>`"""
        raise NotImplementedError(
            "{}Method `construct_main_stmt()` should be "
            "implemented in subclass.".format(self._err_pfix())
        )

    @cython.cfunc
    @cython.inline(True)
    def _construct_custom_stmt(self) -> str:
        "(cfunc) Construct the custom statement `<str>`."
        # Set statement mode
        self._main_stmt_mode = 1
        # Return custom statement
        return self._get_custom(True)._stmt

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def _reset_process_status(self):
        "(cfunc) Reset the process status of the query."
        self._timetable_processed = False
        self._timetable_invalid = False
        self._subquery_processed = False
        self._subquery_invalid = False
        subquery: Clause
        for subquery in dict_values(self._subqueries):
            subquery._reset_process_status()

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def _stmt_semicolon(self, stmt: str) -> str:
        if stmt is None or not stmt:
            return stmt
        if not stmt.endswith(";"):
            return stmt + ";"
        else:
            return stmt

    # Statement ----------------------------------------------------------------------------------
    async def statement(self) -> str:
        """Generate the final query statement `<str>`."""
        stmt: str = await self._construct_main_stmt()
        return self._validate_stmt(stmt)

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def _validate_stmt(self, stmt: str) -> str:
        if stmt is not None and stmt:
            return self._stmt_semicolon(stmt)
        else:
            return "<INVALID QUERY STATEMENT>"

    # Execute ------------------------------------------------------------------------------------
    async def execute(self, *args, **kwargs) -> object:
        """Execute the query."""
        raise NotImplementedError(
            "{}Method `execute()` should be implemented "
            "in subclass.".format(self._err_pfix())
        )

    async def _execute_on_temp_conn(self, *args, **kwargs) -> object:
        """Execute query on temporary connection(s)."""
        raise NotImplementedError(
            "{}Method `exe_on_temp_conn()` should be implemented "
            "in subclass.".format(self._err_pfix())
        )

    async def _execute_on_spec_conn(self, *args, **kwargs) -> object:
        """Execute the query on specified connection."""
        raise NotImplementedError(
            "{}Method `exe_on_temp_conn()` should be implemented "
            "in subclass.".format(self._err_pfix())
        )

    async def _exe_with_retry(
        self,
        stmt: str,
        args: object,
        conn: object,
        resolve_absent_table: bool,
    ) -> int:
        "(Internal use only) Execute with retry on non-critical SQL errors."
        while True:
            try:
                return await query_exc_handler(
                    retry_times=self._retry_times,
                    min_wait_time=self._min_wait_time,
                    max_wait_time=self._max_wait_time,
                )(self._db.execute_query)(
                    stmt,
                    args=args,
                    conn=conn,
                    reusable=True,
                    cursor=self._cursor,
                    timeout=self._timeout,
                    warnings=self._warnings,
                    resolve_absent_table=resolve_absent_table,
                )
            except errors.QueryTableAbsentError:
                pass

    async def _exe_nill_retry(
        self,
        stmt: str,
        args: object,
        conn: object,
        resolve_absent_table: bool,
    ) -> int:
        "(Internal use only) Execute without retry on non-critical SQL errors."
        while True:
            try:
                return await self._db.execute_query(
                    stmt,
                    args=args,
                    conn=conn,
                    reusable=True,
                    cursor=self._cursor,
                    timeout=self._timeout,
                    warnings=self._warnings,
                    resolve_absent_table=resolve_absent_table,
                )
            except errors.QueryTableAbsentError:
                pass

    async def _fch_with_retry(
        self,
        stmt: str,
        args: object,
        conn: object,
        resolve_absent_table: bool,
    ) -> object:
        "(Internal use only) Fetch with retry on non-critical SQL errors."
        while True:
            try:
                return await query_exc_handler(
                    retry_times=self._retry_times,
                    min_wait_time=self._min_wait_time,
                    max_wait_time=self._max_wait_time,
                )(self._db.fetch_query)(
                    stmt,
                    args=args,
                    conn=conn,
                    cursor=self._cursor,
                    timeout=self._timeout,
                    warnings=self._warnings,
                    resolve_absent_table=resolve_absent_table,
                )
            except errors.QueryTableAbsentError:
                pass

    async def _fch_nill_retry(
        self,
        stmt: str,
        args: object,
        conn: object,
        resolve_absent_table: bool,
    ) -> object:
        "(Internal use only) Fetch without retry on non-critical SQL errors."
        while True:
            try:
                return await self._db.fetch_query(
                    stmt,
                    args=args,
                    conn=conn,
                    cursor=self._cursor,
                    timeout=self._timeout,
                    warnings=self._warnings,
                    resolve_absent_table=resolve_absent_table,
                )
            except errors.QueryTableAbsentError:
                pass

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def _setup_semaphore(self):
        """(Internal) Setup semaphore."""
        if self._concurrency <= 0:
            self._semaphore = Semaphore(1)
        else:
            self._semaphore = Semaphore(self._concurrency)

    async def _call_sem(self, func: Callable, *args, **kwargs):
        """(Internal) Execute the query with semaphore."""
        async with self._semaphore:
            return await func(*args, **kwargs)

    # Stats --------------------------------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def _statistics(self, stats: cython.bint, res: object):
        """Print the query execution stats to the console.

        :param stats: `<boo>` Whether to print the query stats.
        :param res: `<Any>` The fetched result. `None` if not a select query.

        ### Stats Information
        - The query information.
        - The executed statement.
        - The execution mode.
        - The affected/selected rows.
        - The execution time.
        """
        if not stats:
            return  # exit

        pad = 100
        breaker = "-" * pad
        end_time: cython.double = unixtime()
        # . information
        print(" Query Stats ".center(pad, "="))
        print("<%s> [%s]" % (self._tb._fname, self._name))
        print(breaker)
        if self._main_stmt:
            # . statement
            print(self._main_stmt)
            if self._insert_stmt:
                print("\n" + self._insert_stmt)
            print(breaker)
            # . execution mode
            if self._main_stmt_mode == 1:
                mode = "1 (Specific Table Statement)"
            elif self._main_stmt_mode == 2:
                mode = (
                    "2 (TimeTable Statement)\n"
                    "* Notice: TimeTable is presented as 'placeholder(s)' in the statement,\n"
                    "  which will be replaced by the following sub-table(s) at query executions:\n"
                    "-> %s" % "\n-> ".join(map(str, self._timetable_repls))
                )
            elif 3 <= self._main_stmt_mode <= 4:
                if self._main_stmt_mode == 3:
                    rows = list_len(self._values.values)
                    msg = "Total values: %d" % rows
                    mode = "3 (Specific Table Values)"
                else:
                    tb_rows: list = []
                    for key, val in dict_items(self._values.values):
                        i = "Sub-table '%s' values: %d" % (key, list_len(val))
                        list_append(tb_rows, i)
                    msg = "\n-> ".join(tb_rows)
                    mode = "4 (TimeTable Values)"
                mode = mode + (
                    "\n* Notice: The values are presented as 'placeholder(s)' in the statement,\n"
                    "  which will be replaced by the given 'values' at query executions:\n"
                    "-> %s" % msg
                )
            else:
                mode = "0 (Undetermined)"
            print("Execution Mode: %s" % mode)
            print(breaker)
            # . affected rows
            if res is None:
                print("Affected Rows.: %d" % self._affected_rows)
            # . selected rows:
            else:
                print("Selected Rows.: %d" % len(res))
                del res
        else:
            print("<INVALID QUERY STATEMENT> Query is not executed.")
        print(breaker)
        # . execution time
        print("Execution Time: %fs" % (end_time - self._start_time))
        print("=" * pad)
        print()

    # Garbage collection -------------------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def _collect_garbage(self):
        # Already collected
        if self._tb is None:
            return None  # exit

        # Query
        self._tb = None
        self._db = None
        # Clause
        self._clauses = None
        self._select_clauses = None
        self._union_select_clauses = None
        self._insert_clauses = None
        self._update_clauses = None
        self._delete_clauses = None
        self._join_clauses = None
        self._where_clauses = None
        self._select = None
        self._for_update = None
        self._lock_in_share_mode = None
        self._insert = None
        self._on_dup = None
        self._update = None
        self._set = None
        self._delete = None
        self._create_temp = None
        self._values = None
        self._tabletimes = None
        self._custom = None
        # TimeTable
        self._timetables = None
        self._timetable_fmts = None
        self._timetable_units = None
        self._timetable_joins = None
        self._timetable_times = None
        self._timetable_repls = None
        # Subquery
        self._subqueries = None
        self._subquery_repls = None
        # Statement
        self._main_stmt = None
        self._select_stmt = None
        self._insert_stmt = None
        # Execute
        self._semaphore = None
        self._cursor = None
        self._timeout = None

    # Special methods ----------------------------------------------------------------------------
    def __repr__(self) -> str:
        return "<Query (name=[%s], table=%s)>" % (self._name, self._tb._fname)

    def __del__(self):
        self._collect_garbage()


@cython.cclass
class SelectShare(Query):
    """`SELECT` query shared methods."""

    # SQL methods --------------------------------------------------------------------------------
    def group_by(self, *columns: Union[str, Column]) -> Self:
        """The `GROUP BY` clause of the MySQL query.

        :param columns: `<str/Column>` The columns to group by, accept both column names `<str>` and instances `<Column>`.

        ### Notice:
        When the query involves TimeTable(s) which `timetable` are specified through `tabletimes()`
        method, the `GROUP BY` operation will only perform at the sub-table level. To perform a
        `GROUP BY` operation of the entire TimeTable, consider using a subquery to select all the
        sub-tables first, and then perform the `GROUP BY` operation at the main `SELECT` query.
        This also applies to `having()`, methods. For more details, please refer to the Example
        sections below:

        ### Example (TimeTable sub-tables level):
        >>> (
                db.user_info.select("age", "COUNT(*) AS count")
                .group_by("age")
                .tabletimes(start="2023-01-01", end="2023-03-01")
            )
        ### -> Equivalent to:
        >>> SELECT age, COUNT(*) AS count FROM user_info_202301 AS t1 GROUP BY age
            UNION ALL
            SELECT age, COUNT(*) AS count FROM user_info_202302 AS t1 GROUP BY age
            UNION ALL
            SELECT age, COUNT(*) AS count FROM user_info_202303 AS t1 GROUP BY age

        ### Example (TimeTable entire 'table'):
        >>> (
                db.user_info.select(
                    "age", "COUNT(*) AS count",
                    from_=(
                        db.user_info.select("age")
                        .tabletimes(start="2023-01-01", end="2023-03-01")
                    )
                ).group_by("age")
            )
        ### -> Equivalent to:
        >>> SELECT age, COUNT(*) AS count FROM (
                SELECT age FROM user_info_202301 AS t1
                UNION ALL
                SELECT age FROM user_info_202302 AS t1
                UNION ALL
                SELECT age FROM user_info_202303 AS t1
            ) AS t1 GROUP BY age
        """
        GROUP_BY(self, *columns)
        return self

    def having(
        self,
        *conditions: str,
        args: Any = None,
        ins: Union[dict[str, Union[list, tuple]], None] = None,
        not_ins: Union[dict[str, Union[list, tuple]], None] = None,
        subqueries: Union[dict[str, SelectQuery], None] = None,
    ) -> Self:
        """The `HAVING` clause of the MySQL query.

        :param conditions: `<str>` Condition expressions: `"id = 1"`, `"name = 'John'"`, `"COUNT(*) > 10"`, etc.
        :param args: `<Any/None>` Arguments for the `'%s'` placeholders of 'conditions'. Defaults to `None`.
        :param ins: `<dict/None>` The `IN` modifier. Defaults to `None`.
            - This parameter must be a dictionary with the column name as the key and
              iterable types such as `list`, `tuple`, `set`, `Series` as the value.
            - For example, `{"id": [1, 2, 3]}` -> `"id IN (1, 2, 3)"`. Duplicates
              are removed automatically.

        :param not_ins: `<dict/None>` The `NOT IN` modifier. Defaults to `None`.
            - Refert to 'ins' argument for more detail.

        :param subqueries: `<dict/None>` The subquery conditions. Defaults to `None`.
            - This parameter must be a dictionary with the column name along with
              the `desired operator` as the key and instances of `SelectQuery`
              as the value.

        ### Notice:
        When the query involves TimeTable(s) which `timetable` are specified through `tabletimes()`
        method, the `HAVING` operation will only perform at the sub-table level. To perform a
        `HAVING` operation of the entire TimeTable, consider using a subquery to select all the
        sub-tables first, and then perform the `HAVING` operation at the main `SELECT` query.
        This also applies to `group_by()`, methods. For more details, please refer to the Example
        sections below:

        ### Example (TimeTable sub-tables level):
        >>> (
                db.user_info.select("age", "COUNT(*) AS count")
                .group_by("age")
                .having("age > 20")
                .tabletimes(start="2023-01-01", end="2023-03-01")
            )
        ### -> Equivalent to:
        >>> SELECT age, COUNT(*) AS count FROM user_info_202301 AS t1 GROUP BY age HAVING age > 20
            UNION ALL
            SELECT age, COUNT(*) AS count FROM user_info_202302 AS t1 GROUP BY age HAVING age > 20
            UNION ALL
            SELECT age, COUNT(*) AS count FROM user_info_202303 AS t1 GROUP BY age HAVING age > 20

        ### Example (TimeTable entire 'table'):
        >>> (
                db.user_info.select(
                    "age", "COUNT(*) AS count",
                    from_=(
                        db.user_info.select("age")
                        .tabletimes(start="2023-01-01", end="2023-03-01")
                    )
                ).group_by("age")
                .having("age > 20")
            )
        ### -> Equivalent to:
        >>> SELECT age, COUNT(*) AS count FROM (
                SELECT age FROM user_info_202301 AS t1
                UNION ALL
                SELECT age FROM user_info_202302 AS t1
                UNION ALL
                SELECT age FROM user_info_202303 AS t1
            ) AS t1 GROUP BY age HAVING age > 20
        """
        HAVING(
            self,
            *conditions,
            args=args,
            ins=ins,
            not_ins=not_ins,
            subqueries=subqueries,
        )
        return self

    def having_time(
        self,
        column: Union[str, Column],
        start: Union[str, datetime.date, datetime.datetime, None] = None,
        end: Union[str, datetime.date, datetime.datetime, None] = None,
        days: Union[int, None] = None,
        unit: Literal["Y", "M", "D", "W", "h", "m", "s", "us"] = "us",
    ) -> Self:
        """The `TIME` clause trailing `HAVING` of the MySQL query.

        `having_time()` method is not a legit SQL clause but a custom design to facilitate
        construct of time range conditions. For instance, `having_time` can be used to
        construct the following syntax:
        `<time_column> BETWEEN '2020-01-01' AND '2020-01-31'`.

        :param column: `<str/Column>` The name or instance of the column the time condition applies to.
            - Accepts either a `str` or a `Column` instance. This clause only
              supports `DATE`, `DATETIME`, `TIMESTAMP` MySQL column types.
            - * Notice: Alias will `NOT` be added to the column name automatically.

        :param start, end, days: The time span parameters.
            - If 'start' and 'end' are specified -> constrain between [start, ... end].
            - If 'start' and 'days' are specified -> constrain between [start, ... start + days - 1]
            - If only 'start' is specified -> constrain between [start, ... datetime.now()] or [datetime.now(), ... start]
            - If 'end' and 'days' are specified -> constrain between [end - days + 1, ... end]
            - If only 'end' is specified -> constrain between [datetime.now(), ... end] or [end, ... datetime.now()]
            - If only 'days' is specified -> constrain between [datetime.now() - days + 1, ... datetime.now()]
            - If none of the params are given -> raise `QueryValueError`.

        :param unit: `<str>` Adjust the time span parameters to match the specified unit. Defaults to `'us'`.
            - This parameter adjusts the 'start' and 'end' times after calculating the
              time span to align with the respective unit.
            - For example, if `unit='Y'`, `start` is adjusted to the beginning of the
              year `(XXXX-01-01 00:00:00)` and `end` is adjusted to the end of the year
              `(XXXX-12-31 23:59:59.999999)`. Similar adjustments are made for other
              units, e.g. `month` adjusts to the start and end of the month, etc.
            - Accepts: `'Y'`, `'M'`, `'D'`, `'W'`, `'h'`, `'m'`, `'s'`, `'us'`,
              where `'us'` means no adjustment to make.

        ### Example:
        >>> (
                db.user.select(...)
                .group_by(...)
                .having(...)
                .having_time("create_dt", end="2023-05-15", days=180, unit="day")
                .having_time("update_dt", start="2023-01-01", end="2023-02-15", unit="month")
            )
        ### -> Equivalent to:
        >>> SELECT ... FROM db.user AS t1
            GROUP BY ...
            HAVING ...
                AND create_dt BETWEEN '2022-11-17 00:00:00.000000' AND '2023-05-15 23:59:59.999999'
                # 'end' is adjusted to XXXX-XX-XX 23:59:59.999999 as `unit = 'day'`.
                AND update_dt BETWEEN '2023-01-01 00:00:00.000000' AND '2023-02-28 23:59:59.999999'
                # 'end' is adjusted to XXXX-XX-28 23:59:59.999999 as `unit = 'month'`.
        """
        HAVING_TIME(self, column, start=start, end=end, days=days, unit=unit)
        return self


@cython.cclass
class SelectQuery(SelectShare):
    """The `SELECT` query of the Table."""

    def __init__(self, table: Table) -> None:
        super().__init__("Select Query", table)

    # SQL methods --------------------------------------------------------------------------------
    def select(
        self,
        *columns: Union[str, Column],
        distinct: cython.bint = False,
        buffer_result: cython.bint = False,
        explain: cython.bint = False,
        from_: Union[str, Table, TimeTable, SelectQuery, None] = None,
        tabletime: Union[str, datetime.date, datetime.datetime, None] = None,
        alias: Union[str, None] = None,
    ) -> Self:
        """The `SELECT... FROM...` clause of the MySQL query.

        :param columns: `<str/Column>` The columns to select, accept both column names `<str>` and instances `<Column>`.
            When a `SELECT` query involves `JOIN`, each column must prefixed
            with the correct alias. For more information about aliasing, see
            the 'alias' parameter.

        :param distinct: `<bool>` The `DISTINCT` modifier. Defaults to `False`.
            - Determines whether to select only distinct values.
            - *Notice: When the `SELECT...` query involes TimeTable(s) which
              `tabletime` are specified through `tabletimes()` method, setting
              `distict=True` will force the use of union select mode. For more
              detail, please refer to the `order_by` or `limit` methods.

        :param buffer_result: `<bool>` The `SQL_BUFFER_RESULT` modifier. Defaults to `False`.
            Determines whether to buffer the result set in MySQL Server memory.
            This can help release table locks earlier for large result sets, but
            will result in higher memory usage.

        :param explain: `<bool>` The `EXPLAIN` modifier. Defaults to `False`.
            Determines whether to return the MySQL optimizer execution plan
            instead of the actual result set. This is helpful for debugging
            and query optimization.

        :param from_: Specify the `FROM` clause. Defaults to `None`.
            - `None`: The `FROM` clause will default to the table that initiated the `SelectQuery`.
            - `<Table/TimeTable>`: The `FROM` clause will be generated based on the given table.
            - `<SelectQuery>`: The `FROM` cluase will be constructed as a subquery based on the
              given `SelectQuery` instance.
            - `<str>`: If the string corresponds to the table name of a table in the database,
              the `FROM` clause will be generated based on that table. Otherwise, the string will
              be treated as a raw sql syntax placed right after the `FROM` keyword.

        :param tabletime: `<str/None>` A specific `tabletime` for the `FORM` table. Defaults to `None`.
            - This parameter is only applicable when the argument 'from_' corresponds
              to a TimeTable (regardless type of `<str>` or `<TimeTable>` instance).
            - If `tabletime` is specified, the actual sub-table will derive from this
              parameter. Otherwise, it is required to `tabletimes()` method to specify
              the sub-tables. For more details, please refer to the `tabletimes()` method.

        :param alias: `<str/None>` The alias of the `SELECT... FROM...` clause. Defaults to `None`.
            - The alias of the clause will be added to the corresponding part of the SQL
              statement using the `'AS <alias>'` syntax.
            - For instance, in a `SELECT... FROM ...` query, without specified
              alias (default alias), the statement would be constructed as:
              `'SELECT ... FROM ... AS t1'`, where default alias is derived
              from the order of the tables in the query.
            - However, with a user-defined alias (for example, `alias='tb'`), the
              statement would be constructed as: `'SELECT ... FROM ... AS tb'`.
            - Although this aliasing behavior might not be necessary for certain SQL
              statements, this module enforces it to ensure all built-in methods can
              be used interchangeably.

        ### Example (select query):
        >>> data = (
                await db.user.select("t1.name", "t2.age")
                .join(db.user_info, "t1.id = t2.id", "t2.age > %s", args=20)
                .where(ins={"t1.name": ["John", "Mary"]})
                .order_by("age DESC", "name ASC")
                .limit(10)
                .execute()
            )
        ### -> Equivalent to:
        >>> SELECT t1.name, t2.age FROM db.user AS t1
            INNER JOIN db.user_info AS t2
                ON t1.id = t2.id AND t2.age > 20
            WHERE t1.name IN ('John', 'Mary')
            ORDER BY age DESC, name ASC
            LIMIT 10

        ### Example (user-defined alias):
        >>> db.user.select("name", "address", "order", alias="user")
        ### -> Equivalent to:
        >>> SELECT name, address, order FROM db.user AS user

        ### Example (from subquery):
        >>> db.user.select("name", from_=db.prime_user.select("name"))
        ### -> Equivalent to:
        >>> SELECT name FROM (SELECT name FROM db.prime_user AS t1) AS t1

        ### Example (from a TimeTable):
        >>> db.price.select("price", "change", tabletime="2021-01-01")
        ### -> Equivalent to:
        >>> SELECT price, change FROM db.price_20210101 AS t1

        ### Example (from temporary table <str>):
        >>> db.user.select("name", "address", "order", from_="db.user_tmp0")
        ### -> Equivalent to:
        >>> SELECT name, address, order FROM db.user_tmp0 AS t1
        """
        SELECT(
            self,
            *columns,
            distinct=distinct,
            buffer_result=buffer_result,
            explain=explain,
            from_=from_,
            tabletime=tabletime,
            alias=alias,
        )
        return self

    def for_update(self) -> Self:
        """The `FOR UPDATE` clause of the MySQL query."""
        FOR_UPDATE(self)
        return self

    def lock_in_share_mode(self) -> Self:
        """The `LOCK IN SHARE MODE` clause of the MySQL query."""
        LOCK_IN_SHARE_MODE(self)
        return self

    # Construct main statement -------------------------------------------------------------------
    async def _construct_main_stmt(self, union_mode: cython.bint) -> str:
        """Construct the final query statement `<str>`"""
        # Construct statement
        stmt: str = await self._construct_select_stmt(union_mode)
        stmt = self._stmt_semicolon(stmt)
        # Set statement mode
        self._main_stmt_mode = self._select_stmt_mode
        # Set & return statement
        self._main_stmt = stmt
        return stmt

    # Statement ----------------------------------------------------------------------------------
    async def statement(self, union_mode: bool = True) -> str:
        """Generate the final query statement `<str>`.

        :param union_mode: `<bool>` Only affects the query involves TimeTable. Defaults to `True`.
            - Affects query involves TimeTables and sub-tables' 'tabletime'
              is specifed through `tabletimes()` methods. For normal Table, the
              statement will always be the same.
            - For more information, please refer to `tabletimes()` method and the
              Example section below.

        :return: `<str>` The final query statement.

        ### Exmaple (union_mode=True)
        >>> # Assume 'user_info' is TimeTable with `time_unit='month'`.
            (
                db.user_info.select("name", "age")
                .tabletimes(start='2023-01-01', end='2023-03-01'))
                .statement(union_mode=True)
            )
        ### -> Returns statement `<str>`:
        >>> '''
            SELECT name, age FROM db.user_info_202301 AS t1
            UNION ALL
            SELECT name, age FROM db.user_info_202302 AS t1
            UNION ALL
            SELECT name, age FROM db.user_info_202303 AS t1
            '''

        ### Example (union_mode=False)
        >>> # Assume 'user_info' is TimeTable with `time_unit='month'`.
            (
                db.user_info.select("name", "age")
                .tabletimes(start='2023-01-01', end='2023-03-01'))
                .statement(union_mode=False)
            )
        ### -> Returns statement `<str>`:
        >>> '''
            SELECT name, age FROM db.$_user_info_0e76621a-2eb5-451e-85c0-5b1e1a5c37e9_$ AS t1
            '''
            # Here the 'db.$_user_info_0e76621a-2eb5-451e-85c0-5b1e1a5c37e9_$' is a placeholder
            # for the actual sub-table name, which will be replaced in the execution stage to
            # create individual statements to execute concurrently.
        """
        stmt = await self._construct_main_stmt(union_mode)
        return self._validate_stmt(stmt)

    # Execute ------------------------------------------------------------------------------------
    @overload
    async def execute(
        self,
        conn: Union[Connection, None] = None,
        concurrency: int = 10,
        cursor: type[DictCursor | SSDictCursor] = DictCursor,
        timeout: Union[int, None] = None,
        warnings: bool = True,
        *,
        retry_on_error: bool = True,
        retry_times: int = -1,
        min_wait_time: float = 0.2,
        max_wait_time: float = 2,
        stats: bool = False,
    ) -> tuple[dict[str, Any]]:
        """Execute the SELECT query.

        :param conn: `<Connection>` The connection to execute the query. Defaults to `None`.
            - If `None`, query will be executed by temporary connections
              acquired from the Server pool.
            - If specified, query will be executed by the given connection.

        :param concurrency: `<int>` The maximum number of concurrent executions. Defaults to `10`.
            - When the query is consists of multiple sub-queries (For example, select
              data from different sub-tables of a TimeTable), this argument determines
              the maximum number of concurrent query executions at the same time.
            - * Notice: This argument is only applicable when `conn=None`. If 'conn'
              is specified, all sub-queries will be executed sequentially by the
              given connection.

        :param cursor: `<type[Cursor]>` The `Cursor` class to use for query execution. Defaults to `DictCursor`.
            - `DictCursor/SSDictCursor`: Fetch result as `<tuple[dict]>`.
            - `DfCursor/SSDfCursor`: Fetch result as `<pandas.DataFrame>`.
            - `Cursor/SSCursor`: Fetch result as `<tuple[tuple]>` (without column names).

        :param timeout: `<int>` Query execution timeout in seconds. Dafaults to `None`.
            - If set to `None` or `0`, `tables.server.query_timeout` will be used
              as the default timeout.
            - `SQLQueryTimeoutError` will be raised when the timeout is reached.

        :param warnings: `<bool>` Whether to issue any SQL related warnings. Defaults to `True`.

        :param retry_on_error: `<bool>` Whether to retry when non-critial SQL error occurs. Defaults to `True`.
            - If `True`, when a non-critical SQL error occurs (such as `connection timeout`,
              `connection lost`, etc.), the query will be retried.
            - If `False`, errors will be raised immediately.

        :param retry_times: `<int>` The maximum number of retries. Defaults to `-1`.
            - * Notice: This argument is only applicable when `retry_on_error=True`.
            - For `retry_times <= 0`, the query retries indefinitely until success.
            - For `retry_times > 0`, the query retries up to the given 'retry_times'.

        :param min_wait_time: `<float>` The minimum wait time in seconds between each retries. Defaults to `0.2`.
            - * Notice: This argument is only applicable when `retry_on_error=True`.

        :param max_wait_time: `<float>` The maximum wait time in seconds between each retries. Defaults to `2`.
            - * Notice: This argument is only applicable when `retry_on_error=True`.

        :param stats: `<bool>` Whether to print the query execution stats to the console. Defaults to `False`.
        :raises: Subclass of `QueryError`.
        :return `<tuple[dict]>`: The fetched result.
        """

    @overload
    async def execute(
        self,
        conn: Union[Connection, None] = None,
        concurrency: int = 10,
        cursor: type[DfCursor | SSDfCursor] = DictCursor,
        timeout: Union[int, None] = None,
        warnings: bool = True,
        *,
        retry_on_error: bool = True,
        retry_times: int = -1,
        min_wait_time: float = 0.2,
        max_wait_time: float = 2,
        stats: bool = False,
    ) -> DataFrame:
        """Execute the SELECT query.

        :param conn: `<Connection>` The connection to execute the query. Defaults to `None`.
            - If `None`, query will be executed by temporary connections
              acquired from the Server pool.
            - If specified, query will be executed by the given connection.

        :param concurrency: `<int>` The maximum number of concurrent executions. Defaults to `10`.
            - When the query is consists of multiple sub-queries (For example, select
              data from different sub-tables of a TimeTable), this argument determines
              the maximum number of concurrent query executions at the same time.
            - * Notice: This argument is only applicable when `conn=None`. If 'conn'
              is specified, all sub-queries will be executed sequentially by the
              given connection.

        :param cursor: `<type[Cursor]>` The `Cursor` class to use for query execution. Defaults to `DictCursor`.
            - `DictCursor/SSDictCursor`: Fetch result as `<tuple[dict]>`.
            - `DfCursor/SSDfCursor`: Fetch result as `<pandas.DataFrame>`.
            - `Cursor/SSCursor`: Fetch result as `<tuple[tuple]>` (without column names).

        :param timeout: `<int>` Query execution timeout in seconds. Dafaults to `None`.
            - If set to `None` or `0`, `tables.server.query_timeout` will be used
              as the default timeout.
            - `SQLQueryTimeoutError` will be raised when the timeout is reached.

        :param warnings: `<bool>` Whether to issue any SQL related warnings. Defaults to `True`.

        :param retry_on_error: `<bool>` Whether to retry when non-critial SQL error occurs. Defaults to `True`.
            - If `True`, when a non-critical SQL error occurs (such as `connection timeout`,
              `connection lost`, etc.), the query will be retried.
            - If `False`, errors will be raised immediately.

        :param retry_times: `<int>` The maximum number of retries. Defaults to `-1`.
            - * Notice: This argument is only applicable when `retry_on_error=True`.
            - For `retry_times <= 0`, the query retries indefinitely until success.
            - For `retry_times > 0`, the query retries up to the given 'retry_times'.

        :param min_wait_time: `<float>` The minimum wait time in seconds between each retries. Defaults to `0.2`.
            - * Notice: This argument is only applicable when `retry_on_error=True`.

        :param max_wait_time: `<float>` The maximum wait time in seconds between each retries. Defaults to `2`.
            - * Notice: This argument is only applicable when `retry_on_error=True`.

        :param stats: `<bool>` Whether to print the query execution stats to the console. Defaults to `False`.
        :raises: Subclass of `QueryError`.
        :return `<DataFrame>`: The fetched result.
        """

    @overload
    async def execute(
        self,
        conn: Union[Connection, None] = None,
        concurrency: int = 10,
        cursor: type[Cursor | SSCursor] = DictCursor,
        timeout: Union[int, None] = None,
        warnings: bool = True,
        *,
        retry_on_error: bool = True,
        retry_times: int = -1,
        min_wait_time: float = 0.2,
        max_wait_time: float = 2,
        stats: bool = False,
    ) -> tuple[tuple[Any]]:
        """Execute the SELECT query.

        :param conn: `<Connection>` The connection to execute the query. Defaults to `None`.
            - If `None`, query will be executed by temporary connections
              acquired from the Server pool.
            - If specified, query will be executed by the given connection.

        :param concurrency: `<int>` The maximum number of concurrent executions. Defaults to `10`.
            - When the query is consists of multiple sub-queries (For example, select
              data from different sub-tables of a TimeTable), this argument determines
              the maximum number of concurrent query executions at the same time.
            - * Notice: This argument is only applicable when `conn=None`. If 'conn'
              is specified, all sub-queries will be executed sequentially by the
              given connection.

        :param cursor: `<type[Cursor]>` The `Cursor` class to use for query execution. Defaults to `DictCursor`.
            - `DictCursor/SSDictCursor`: Fetch result as `<tuple[dict]>`.
            - `DfCursor/SSDfCursor`: Fetch result as `<pandas.DataFrame>`.
            - `Cursor/SSCursor`: Fetch result as `<tuple[tuple]>` (without column names).

        :param timeout: `<int>` Query execution timeout in seconds. Dafaults to `None`.
            - If set to `None` or `0`, `tables.server.query_timeout` will be used
              as the default timeout.
            - `SQLQueryTimeoutError` will be raised when the timeout is reached.

        :param warnings: `<bool>` Whether to issue any SQL related warnings. Defaults to `True`.

        :param retry_on_error: `<bool>` Whether to retry when non-critial SQL error occurs. Defaults to `True`.
            - If `True`, when a non-critical SQL error occurs (such as `connection timeout`,
              `connection lost`, etc.), the query will be retried.
            - If `False`, errors will be raised immediately.

        :param retry_times: `<int>` The maximum number of retries. Defaults to `-1`.
            - * Notice: This argument is only applicable when `retry_on_error=True`.
            - For `retry_times <= 0`, the query retries indefinitely until success.
            - For `retry_times > 0`, the query retries up to the given 'retry_times'.

        :param min_wait_time: `<float>` The minimum wait time in seconds between each retries. Defaults to `0.2`.
            - * Notice: This argument is only applicable when `retry_on_error=True`.

        :param max_wait_time: `<float>` The maximum wait time in seconds between each retries. Defaults to `2`.
            - * Notice: This argument is only applicable when `retry_on_error=True`.

        :param stats: `<bool>` Whether to print the query execution stats to the console. Defaults to `False`.
        :raises: Subclass of `QueryError`.
        :return `<tuple[tuple]>`: The fetched result.
        """

    async def execute(
        self,
        conn: Union[Connection, None] = None,
        concurrency: int = 10,
        cursor: type[
            DictCursor | SSDictCursor | DfCursor | SSDfCursor | Cursor | SSCursor
        ] = DictCursor,
        timeout: Union[int, None] = None,
        warnings: bool = True,
        *,
        retry_on_error: bool = True,
        retry_times: int = -1,
        min_wait_time: float = 0.2,
        max_wait_time: float = 2,
        stats: bool = False,
    ) -> Union[tuple[dict[str, Any] | tuple[Any]], DataFrame]:
        """Execute the SELECT query.

        :param conn: `<Connection>` The connection to execute the query. Defaults to `None`.
            - If `None`, query will be executed by temporary connections
              acquired from the Server pool.
            - If specified, query will be executed by the given connection.

        :param concurrency: `<int>` The maximum number of concurrent executions. Defaults to `10`.
            - When the query is consists of multiple sub-queries (For example, select
              data from different sub-tables of a TimeTable), this argument determines
              the maximum number of concurrent query executions at the same time.
            - * Notice: This argument is only applicable when `conn=None`. If 'conn'
              is specified, all sub-queries will be executed sequentially by the
              given connection.

        :param cursor: `<type[Cursor]>` The `Cursor` class to use for query execution. Defaults to `DictCursor`.
            - `DictCursor/SSDictCursor`: Fetch result as `<tuple[dict]>`.
            - `DfCursor/SSDfCursor`: Fetch result as `<pandas.DataFrame>`.
            - `Cursor/SSCursor`: Fetch result as `<tuple[tuple]>` (without column names).

        :param timeout: `<int>` Query execution timeout in seconds. Dafaults to `None`.
            - If set to `None` or `0`, `tables.server.query_timeout` will be used
              as the default timeout.
            - `SQLQueryTimeoutError` will be raised when the timeout is reached.

        :param warnings: `<bool>` Whether to issue any SQL related warnings. Defaults to `True`.

        :param retry_on_error: `<bool>` Whether to retry when non-critial SQL error occurs. Defaults to `True`.
            - If `True`, when a non-critical SQL error occurs (such as `connection timeout`,
              `connection lost`, etc.), the query will be retried.
            - If `False`, errors will be raised immediately.

        :param retry_times: `<int>` The maximum number of retries. Defaults to `-1`.
            - * Notice: This argument is only applicable when `retry_on_error=True`.
            - For `retry_times <= 0`, the query retries indefinitely until success.
            - For `retry_times > 0`, the query retries up to the given 'retry_times'.

        :param min_wait_time: `<float>` The minimum wait time in seconds between each retries. Defaults to `0.2`.
            - * Notice: This argument is only applicable when `retry_on_error=True`.

        :param max_wait_time: `<float>` The maximum wait time in seconds between each retries. Defaults to `2`.
            - * Notice: This argument is only applicable when `retry_on_error=True`.

        :param stats: `<bool>` Whether to print the query execution stats to the console. Defaults to `False`.
        :raises: Subclass of `QueryError`.
        :return `<tuple[tuple]/tuple[dict]/DataFrame>`: The fetched result (depends on 'cursor' type).
        """
        # Set parameters
        self._concurrency = concurrency
        self._cursor = cursor
        self._timeout = timeout
        self._warnings = warnings
        self._retry_on_error = retry_on_error
        self._retry_times = retry_times
        self._min_wait_time = min_wait_time
        self._max_wait_time = max_wait_time

        res: object = []
        try:
            # Construct statement
            stmt = await self._construct_main_stmt(conn is not None)

            # Invalid query statement
            if not stmt:
                return cursor.empty_result  # exit

            # Execute on specified connection
            elif isinstance(conn, Connection):
                res = await self._execute_on_spec_conn(stmt, conn)

            # Execute on temporary connection(s)
            else:
                res = await self._execute_on_temp_conn(stmt)

            # Return result
            return res

        finally:
            self._statistics(stats, res)
            self._collect_garbage()

    async def _execute_on_temp_conn(self, stmt: str) -> object:
        # Determine execute function
        if self._retry_on_error:
            func = self._fch_with_retry
        else:
            func = self._fch_nill_retry

        # Select - specific table
        if self._main_stmt_mode == 1:
            return await func(stmt, None, None, True)

        # Select - timetables
        elif self._main_stmt_mode == 2:
            self._setup_semaphore()
            tasks = [
                self._call_sem(func, self._replace_plhs(stmt, repls), None, None, True)
                for repls in self._timetable_repls
            ]
            res: list = await gather(*tasks)
            return self._concat_fetch_results(res)

        # Invalid - undetermined
        else:
            raise errors.QueryValueError(
                "{}Invalid query statement mode: {}. Query mode is "
                "undetermined.".format(self._err_pfix(), self._main_stmt_mode)
            )

    async def _execute_on_spec_conn(self, stmt: str, conn: Connection) -> object:
        # Execute without retry
        return await self._fch_nill_retry(stmt, None, conn, True)

    @cython.cfunc
    @cython.inline(True)
    def _concat_fetch_results(self, result: list) -> object:
        # DataFrame cursor
        if self._cursor is DfCursor or self._cursor is SSDfCursor:
            length: int = list_len(result)
            if length > 1:
                return concat(result, ignore_index=True)
            elif length == 1:
                return result[0]
            else:
                return DataFrame()

        # Other cursor
        else:
            res: list = []
            for qur in result:
                res += list(qur)
            return tuple(res)


@cython.cclass
class InsertShare(SelectShare):
    """`INSERT/REPLACE` query shared methods."""

    # SQL methods --------------------------------------------------------------------------------
    def select(
        self,
        from_: Union[str, Table, TimeTable, SelectQuery],
        *columns: Union[str, Column],
        distinct: cython.bint = False,
        tabletime: Union[str, datetime.date, datetime.datetime, None] = None,
        alias: Union[str, None] = None,
    ) -> Self:
        """The `SELECT... FROM...` clause of the MySQL `INSERT/REPLACE` query.

        :param from_: Specify the `FROM` clause.
            - `<Table/TimeTable>`: The `FROM` clause will be generated based on the given table.
            - `<SelectQuery>`: The `FROM` cluase will be constructed as a subquery based on the
              given `SelectQuery` instance.
            - `<str>`: If the string corresponds to the table name of a table in the database,
              the `FROM` clause will be generated based on that table. Otherwise, the string will
              be treated as a raw sql syntax placed right after the `FROM` keyword.

        :param columns: `<str/Column>` The columns to select, accept both column names `<str>` and instances `<Column>`.
            - If not specified, defaults to the columns of the `INSERT/REPLACE` clause.
            - When a `INSERT/REPLACE... SELECT...` query involves `JOIN`, each
              column must prefixed with the correct alias. For more information
              about aliasing, see the 'alias' parameter.

        :param distinct: `<bool>` The `DISTINCT` modifier. Defaults to `False`.
            - Determines whether to select only distinct values.
            - *Notice: When the `SELECT...` query involes TimeTable(s) which
              `tabletime` are specified through `tabletimes()` method, setting
              `distict=True` will force the use of union select mode. For more
              detail, please refer to the `order_by` or `limit` methods.

        :param tabletime: `<str/None>` A specific `tabletime` for the `FORM` table. Defaults to `None`.
            - This parameter is only applicable when the argument 'from_' corresponds
              to a TimeTable (regardless type of `<str>` or `<TimeTable>` instance).
            - If `tabletime` is specified, the actual sub-table will derive from this
              parameter. Otherwise, it is required to use `tabletimes()` method to
              specify the sub-tables. For more details, please refer to the `tabletimes()`
              method.

        :param alias: `<str/None>` The alias of the `INSERT... SELECT... FROM...` clause. Defaults to `None`.
            - The alias of the clause will be added to the corresponding part of the SQL
              statement using the `'AS <alias>'` syntax.
            - For instance, in a `INSERT... SELECT... FROM ...` query, without
              specified alias (default alias), the statement would be constructed
              as: `'INSERT... SELECT ... FROM ... AS t1'`, where default alias is
              derived from the order of the tables in the query.
            - However, with a user-defined alias (for example, `alias='tb'`), the
              statement would be constructed as:
              `'INSERT... SELECT ... FROM ... AS tb'`.

        ### Example:
        >>> db.user.insert("name").select(db.user_info, tabletime="2023-01-01")
        ### -> Equivalent to:
        >>> INSERT INTO db.user (name)
            SELECT name FROM db.user_info_202301 AS t1
        """
        # Pre-validate
        if not isinstance(from_, (str, Table, SelectQuery)):
            raise errors.QueryValueError(
                "<{}> [{}] `SELECT`\nError: "
                "Argument 'from_' must be type of '<str>', '<Table>', or '<SelectQuery>', "
                "instead of: {}".format(self._tb._fname, self._name, type(from_))
            )
        SELECT(
            self,
            *columns if columns else self._get_insert(True)._columns,
            distinct=distinct,
            buffer_result=False,
            explain=False,
            from_=from_,
            tabletime=tabletime,
            alias=alias,
        )
        return self

    # Construct main statement -------------------------------------------------------------------
    async def _construct_main_stmt(self) -> str:
        """Construct the final query statement `<str>`"""
        # Construct statement
        self._main_stmt = ""
        stmt: str
        try:
            # . custom statement
            if self._custom is not None:
                stmt = self._construct_custom_stmt()
            # . insert values
            elif self._values is not None:
                stmt = await self._construct_insert_values()
            # . insert select
            elif self._select is not None:
                stmt = await self._construct_insert_select()
            # . invalid
            else:
                raise errors.QueryValueError(
                    "{}For a complete INSERT/REPLACE query, either 'values()' or 'select()' "
                    "method must be called.".format(self._err_pfix())
                )
        except errors.QueryValueError:
            raise
        except Exception as err:
            raise errors.QueryValueError(
                "{}Query construction failed: {}".format(self._err_pfix(), err)
            ) from err

        # Adjust semicolumn
        stmt = self._stmt_semicolon(stmt)

        # Set & return statement
        self._main_stmt = stmt
        return stmt

    async def _construct_insert_values(self) -> str:
        "Construct the `'INSERT... VALUES...'` statement `<str>`."
        # Validate
        if self._select is not None:
            raise errors.QueryValueError(
                "{}Method 'values()' cannot be used with 'select()' "
                "method in the same INSERT query.".format(self._err_pfix())
            )
        if self._tabletimes is not None:
            raise errors.QueryValueError(
                "{}Method 'values()' cannot be used with 'tabletimes()' method "
                "in the same INSERT query. Sub-tables will be determined by "
                "the given 'values' data.".format(self._err_pfix())
            )

        # Set statement mode
        self._main_stmt_mode = 4 if self._query_timetables else 3

        # Construct statement
        clauses = self._get_insert_clauses()
        return self._join_clause_syntax(clauses)

    async def _construct_insert_select(self) -> str:
        "Construct the `'INSERT... SELECT...'` statement `<str>`."
        # Set statement mode
        if self._query_timetables:
            await self._process_timetable()
            if self._timetable_invalid:
                return ""  # exit
            elif self._insert._mode == 1 or self._insert._mode == 2:
                self._main_stmt_mode = 1
                union_mode = True
            elif dict_len(self._timetables) > 1:
                self._main_stmt_mode = 2
                union_mode = False
            else:
                raise errors.QueryValueError(
                    "{}Query 'INSERT (TimeTable)... SELECT (Specific Table)...' requires "
                    "user to specify the exact sub-table of the TimeTable to insert into. "
                    "Please use `insert(tabletime='YYYY-MM-DD')` argument to specify.".format(
                        self._err_pfix()
                    )
                )
        else:
            self._main_stmt_mode = 1
            union_mode = False

        # Construct select syntax
        select = await self._construct_select_stmt(union_mode)
        if not select:
            return ""  # exit

        # Construct statement
        syntax: list
        if self._on_dup is None:
            syntax = [self._insert.syntax, select]
        else:
            syntax = [self._insert.syntax, select, self._on_dup.syntax]
        return "\n".join(syntax)

    # Execute ------------------------------------------------------------------------------------
    async def execute(
        self,
        conn: Union[Connection, None] = None,
        concurrency: int = 10,
        cursor: type[Cursor | SSCursor] = Cursor,
        timeout: Union[int, None] = None,
        warnings: bool = True,
        *,
        retry_on_error: bool = True,
        retry_times: int = -1,
        min_wait_time: float = 0.2,
        max_wait_time: float = 2,
        stats: bool = False,
    ) -> int:
        """Execute the INSERT/REPLACE query.

        :param conn: `<Connection>` The connection to execute the query. Defaults to `None`.
            - If `None`, query will be executed by temporary connections
              acquired from the Server pool.
            - If specified, query will be executed by the given connection.

        :param concurrency: `<int>` The maximum number of concurrent executions. Defaults to `10`.
            - When the query is consists of multiple sub-queries (For example, insert
              data into different sub-tables of a TimeTable), this argument determines
              the maximum number of concurrent query executions at the same time.
            - * Notice: This argument is only applicable when `conn=None`. If 'conn'
              is specified, all sub-queries will be executed sequentially by the
              given connection.

        :param cursor: `<type[Cursor/SSCursor]>` The `Cursor` class to use for query execution. Defaults to `Cursor`.

        :param timeout: `<int>` Query execution timeout in seconds. Dafaults to `None`.
            - If set to `None` or `0`, `tables.server.query_timeout` will be used
              as the default timeout.
            - `SQLQueryTimeoutError` will be raised when the timeout is reached.

        :param warnings: `<bool>` Whether to issue any SQL related warnings. Defaults to `True`.

        :param retry_on_error: `<bool>` Whether to retry when non-critial SQL error occurs. Defaults to `True`.
            - If `True`, when a non-critical SQL error occurs (such as `connection timeout`,
              `connection lost`, etc.), the query will be retried.
            - If `False`, errors will be raised immediately.

        :param retry_times: `<int>` The maximum number of retries. Defaults to `-1`.
            - * Notice: This argument is only applicable when `retry_on_error=True`.
            - For `retry_times <= 0`, the query retries indefinitely until success.
            - For `retry_times > 0`, the query retries up to the given 'retry_times'.

        :param min_wait_time: `<float>` The minimum wait time in seconds between each retries. Defaults to `0.2`.
            - * Notice: This argument is only applicable when `retry_on_error=True`.

        :param max_wait_time: `<float>` The maximum wait time in seconds between each retries. Defaults to `2`.
            - * Notice: This argument is only applicable when `retry_on_error=True`.

        :param stats: `<bool>` Whether to print the query execution stats to the console. Defaults to `False`.
        :raises: Subclass of `QueryError`.
        :return `<int>`: The number of rows affected by the query.
        """
        # Set parameters
        self._concurrency = concurrency
        self._cursor = cursor
        self._timeout = timeout
        self._warnings = warnings
        self._retry_on_error = retry_on_error
        self._retry_times = retry_times
        self._min_wait_time = min_wait_time
        self._max_wait_time = max_wait_time

        try:
            # Construct statement
            stmt = await self._construct_main_stmt()

            # Invalid query statement
            if not stmt:
                return 0  # exit

            # Execute on specified connection
            elif isinstance(conn, Connection):
                self._affected_rows = await self._execute_on_spec_conn(stmt, conn)

            # Execute on temporary connection(s)
            else:
                self._affected_rows = await self._execute_on_temp_conn(stmt)

            # Return affected rows
            return self._affected_rows

        finally:
            self._statistics(stats, None)
            self._collect_garbage()

    async def _execute_on_temp_conn(self, stmt: str) -> int:
        """Execute query on temporary connection(s)."""
        # Determine execute function
        if self._retry_on_error:
            func = self._exe_with_retry
        else:
            func = self._exe_nill_retry

        # Insert select - specific table
        if self._main_stmt_mode == 1:
            return await func(stmt, None, None, True)

        # Insert select - timetables
        elif self._main_stmt_mode == 2:
            self._setup_semaphore()
            tasks = [
                self._call_sem(func, self._replace_plhs(stmt, repls), None, None, True)
                for repls in self._timetable_repls
            ]
            res = await gather(*tasks)
            return sum(res) if res else 0

        # Insert values - specific table
        elif self._main_stmt_mode == 3:
            return await func(stmt, self._values.values, None, True)

        # Insert values - timetables
        elif self._main_stmt_mode == 4:
            self._setup_semaphore()
            key = self._insert._tb_name
            tasks = [
                self._call_sem(func, uni_replace(stmt, key, tb, -1), args, None, True)
                for tb, args in dict_items(self._values.values)
            ]
            res = await gather(*tasks)
            return sum(res) if res else 0

        # Invalid - undetermined
        else:
            raise errors.QueryValueError(
                "{}Invalid query statement mode: {}. Query mode is "
                "undetermined.".format(self._err_pfix(), self._main_stmt_mode)
            )

    async def _execute_on_spec_conn(self, stmt: str, conn: object) -> int:
        """Execute the query on specified connection."""
        # Determine execute function
        func = self._exe_nill_retry

        # Insert select - specific table
        if self._main_stmt_mode == 1:
            return await func(stmt, None, conn, True)

        # Insert select - timetables
        elif self._main_stmt_mode == 2:
            rows: int = 0
            for repls in self._timetable_repls:
                rows += await func(self._replace_plhs(stmt, repls), None, conn, True)
            return rows

        # Insert values - specific table
        elif self._main_stmt_mode == 3:
            return await func(stmt, self._values.values, conn, True)

        # Insert values - timetables
        elif self._main_stmt_mode == 4:
            key = self._insert._tb_name
            rows: int = 0
            for tb, args in dict_items(self._values.values):
                rows += await func(uni_replace(stmt, key, tb, -1), args, conn, True)
            return rows

        # Invalid - undetermined
        else:
            raise errors.QueryValueError(
                "{}Invalid query statement mode: {}. Query mode is "
                "undetermined.".format(self._err_pfix(), self._main_stmt_mode)
            )


@cython.cclass
class InsertQuery(InsertShare):
    """The `INSERT` query of the Table."""

    def __init__(self, table: Table) -> None:
        super().__init__("Insert Query", table)

    # SQL methods --------------------------------------------------------------------------------
    def insert(
        self,
        *columns: Union[str, Column],
        ignore: cython.bint = False,
        tabletime: Union[str, datetime.date, datetime.datetime, None] = None,
    ) -> Self:
        """The `INSERT` clause of the MySQL query.

        :param columns: `<str/Column>` The columns targeted by the insert operation, accept both column names `<str>` and instances `<Column>`.
        :param ignore: `<bool>` The `IGNORE` modifier. Defaults to `False`.
            Determines whether to ignore the duplicate key errors.

        :param tabletime: `<str/None>` A specific `tabletime` for the `INSERT` table. Defaults to `None`.
            - This parameter is only applicable when the `INSERT` table corresponds
              to a TimeTable.
            - If `tabletime` is specified, the actual sub-table will derive from this
              parameter. Otherwise, it is required to use `tabletimes()` method at to
              specify the sub-tables. For more details, please refer to the `tabletimes()`
              method.

        ### Example (INSERT... VALUES...)
        >>> values = [
                {"id": 1, "name": "John", "age": 20, "status": "active"},
                {"id": 2, "name": "Mary", "age": 25, "status": "inactive"},
                ...
            ]
        >>> (
                await db.user.insert("name", "age")
                .values(values)
                # . the 'columns' arguemnt ('name', "age") of thr `insert()` method
                # . will be used to construct and validate the 'values' data.
                .execute()
            )
        ### -> Equivalent to:
        >>> INSERT INTO db.user (name, age)
            VALUES ('John', 20), ('Mary', 25), ...
            AS val  # (Applicable for MySQL 8.0 and later versions)

        ### Example (INSERT... SELECT...)
        >>> (
                await db.user.insert("name")
                .select(db.user_info, tabletime="2023-01-01")
                .execute()
            )
        ### -> Equivalent to:
        >>> INSERT INTO db.user (name)
            SELECT name FROM db.user_info_202301 AS t1
        """
        INSERT(self, *columns, ignore=ignore, tabletime=tabletime)
        return self

    def values(self, *values: Any, alias: str = "val") -> Self:
        """The `VALUES` clause of the MySQL `INSERT` query.

        :param values: The values to `INSERT`, accepts:
            - `<dict>`: Each represents one row of a table data.
            - `<sequence>`: Data types such as `<list>`, `<tuple>`, `<Series>`, each represents one row of a table data.
            - `<DataFrame>`: Each represents rows of a table data.
            - `<Any>`: Data types such as `<int>`, `<str>`, `<bytes>`, each represents an item of one row of a table data.

        :param alias: The desired alias for the `VALUES` clause. Defaults to `'val'`.
            For MySQL 8.0 and later versions, it's recommended to use the `'col = alias.col`'
            syntax instead of the `VALUES()` function in the `ON DUPLICATE KEY UPDATE` clause.
            This parameter lets users customize the alias for the `VALUES` clause. This
            parameter takes no effect for MySQL 5.7 and earlier versions.

        ### Example:
        >>> values = [
                {"id": 1, "name": "John", "age": 20, "status": "active"},
                {"id": 2, "name": "Mary", "age": 25, "status": "inactive"},
                ...
            ]
        >>> (
                db.user.insert("name", "age").values(values)
                # The 'columns' arguemnt ('name', "age") of thr `insert()`
                # method will be used construct and validate the 'values' data.
            )
        ### -> Equivalent to:
        >>> INSERT INTO db.user (name, age)
            VALUES ('John', 20), ('Mary', 25), ...
            AS val  # (Applicable for MySQL 8.0 and later versions)
        """
        INSERT_VALUES(self, *values, alias=alias)
        return self

    def on_duplicate_key(self, *updates: str) -> Self:
        """The `ON DUPLICATE KEY UPDATE` clause of the MySQL INSERT query.

        :param updates: `<str>` The update expressions: `'col1 = val.col1'` etc.
        """
        ON_DUPLICATE_KEY(self, *updates)
        return self


@cython.cclass
class ReplaceQuery(InsertShare):
    """The `REPLACE` query of the Table."""

    def __init__(self, table: Table) -> None:
        super().__init__("Replace Query", table)

    # SQL methods --------------------------------------------------------------------------------
    def replace(
        self,
        *columns: Union[str, Column],
        tabletime: Union[str, datetime.date, datetime.datetime, None] = None,
    ) -> Self:
        """The `REPLACE` clause of the MySQL query.

        :param columns: `<str/Column>` The columns targeted by the replace operation, accept both column names `<str>` and instances `<Column>`.
        :param tabletime: `<str/None>` A specific `tabletime` for the `REPLACE` table. Defaults to `None`.
            - This parameter is only applicable when the `REPLACE` table corresponds
              to a TimeTable.
            - If `tabletime` is specified, the actual sub-table will derive from this
              parameter. Otherwise, it is required to use `tabletimes()` method to
              specify the sub-tables. For more details, please refer to the `tabletimes()`
              method.

        ### Example (REPLACE... VALUES...)
        >>> values = [
                {"id": 1, "name": "John", "age": 20, "status": "active"},
                {"id": 2, "name": "Mary", "age": 25, "status": "inactive"},
                ...
            ]
        >>> (
                await db.user.replace("name", "age")
                .values(values)
                # The 'columns' arguemnt ('name', "age") of thr `replace()`
                # method will be used construct and validate the 'values' data.
                .execute()
            )
        ### -> Equivalent to:
        >>> REPLACE INTO db.user (name, age)
            VALUES ('John', 20), ('Mary', 25), ...

        ### Example (REPLACE... SELECT...)
        >>> (
                await db.user.replace("name")
                .select(db.user_info, tabletime="2023-01-01")
                .execute()
            )
        ### -> Equivalent to:
        >>> REPLACE INTO db.user (name)
            SELECT name FROM db.user_info_202301 AS t1
        """
        REPLACE(self, *columns, tabletime=tabletime)
        return self

    def values(self, *values: Any) -> Self:
        """The `VALUES` clause of the MySQL `REPLACE` query.

        :param values: The values to `REPLACE`, accepts:
            - `<dict>`: Each represents one row of a table data.
            - `<sequence>`: Data types such as `<list>`, `<tuple>`, `<Series>`, each represents one row of a table data.
            - `<DataFrame>`: Each represents rows of a table data.
            - `<Any>`: Data types such as `<int>`, `<str>`, `<bytes>`, each represents an item of one row of a table data.

        ### Example:
        >>> values = [
                {"id": 1, "name": "John", "age": 20, "status": "active"},
                {"id": 2, "name": "Mary", "age": 25, "status": "inactive"},
                ...
            ]
        >>> (
                db.user.replace("name", "age").values(values)
                # The 'columns' arguemnt ('name', "age") of thr `replace()`
                # method will be used construct and validate the 'values' data.
            )
        ### -> Equivalent to:
        >>> REPLACE INTO db.user (name, age)
            VALUES ('John', 20), ('Mary', 25), ...
        """
        REPLACE_VALUES(self, *values)
        return self


@cython.cclass
class UpdateQuery(Query):
    """The `UPDATE` query of the Table."""

    def __init__(self, table: Table) -> None:
        super().__init__("Update Query", table)

    # SQL methods --------------------------------------------------------------------------------
    def update(
        self,
        ignore: cython.bint = False,
        tabletime: Union[str, datetime.date, datetime.datetime, None] = None,
        alias: Union[str, None] = None,
    ) -> Self:
        """The `UPDATE` clause of the MySQL query.

        :param ignore: `<bool>` The `IGNORE` modifier. Defaults to `False`.
            Determines whether to ignore the duplicate key errors.

        :param tabletime: `<str/None>` A specific `tabletime` for the `UPDATE` table. Defaults to `None`.
            - This parameter is only applicable when the `UPDATE` table corresponds
              to a TimeTable.
            - If `tabletime` is specified, the actual sub-table will derive from this
              parameter. Otherwise, it is required to use `tabletimes()` method to specify
              the sub-tables. For more details, please refer to the `tabletimes()` method.

        :param alias: `<str/None>` The alias of the `UPDATE` clause. Defaults to `None`.
            - The alias of the clause will be added to the corresponding part of the SQL
              statement using the `'AS <alias>'` syntax.
            - For instance, in a `UPDATE... JOIN... SET...` query, without specified
              alias (default alias), the statement would be constructed as:
              `'UPDATE... AS t1 JOIN... AS t2 SET ...'`, where default alias is derived
              from the order of the tables in the query.
            - However, with a user-defined alias (for example, `alias='tb'`), the
              statement would be constructed as: `'UPDATE... AS tb JOIN... AS t2 SET ...'`.

        ### Example (UPDATE... SET...)
        >>> (
                await db.user.update()
                .set("name = %s", "age = %s", args=["John", 20])
                .where("id = 1")
                .execute()
            )
        ### -> Equivalent to:
        >>> UPDATE db.user AS t1
            SET name = 'John', age = 20
            WHERE id = 1

        ### Example (UPDATE... JOIN... SET...)
        >>> (
                await db.user.update()
                .join(db.user_info, "t1.id = t2.user_id", tabletime="2023-01-01")
                .set("t1.name = t2.name", "t1.age = t2.age")
                .where("t1.age > 18")
                .execute()
            )
        ### -> Equivalent to:
        >>> UPDATE db.user AS t1
            INNER JOIN db.user_info_202301 AS t2
                ON t1.id = t2.user_id
            SET t1.name = t2.name, t1.age = t2.age
            WHERE t1.age > 18

        ### Example (UPDATE... with `values()` method)
        >>> values = [
                {"id": 1, "name": "John", "age": 20, "status": "active"},
                {"id": 2, "name": "Mary", "age": 25, "status": "inactive"},
            ]
        >>> (
                await db.user.update()
                .values(
                    values,
                    value_columns=["name", "age"],
                    where_columns=["id"])
                .execute()
            )
        ### -> Equivalent to the following TWO queries:
        >>> UPDATE db.user AS t1
            SET t1.name = 'John', t1.age = 20
            WHERE t1.id = 1
        >>> UPDATE db.user AS t1
            SET t1.name = 'Mary', t1.age = 25
            WHERE t1.id = 2
        """
        UPDATE(self, ignore=ignore, tabletime=tabletime, alias=alias)
        return self

    def set(
        self,
        *assignments: str,
        args: Any = None,
        subqueries: dict[str, SelectQuery] | None = None,
    ) -> Self:
        """The `SET` clause of the MySQL UPDATE query.

        :param assignments: `<str>` The set expressions: `'col1 = 3'`, `'col2 = %s'`, etc.
        :param args: `<Any/None>` Arguments for the `'%s'` placeholders of 'assignments'. Defaults to `None`.
        :param subqueries: `<dict/None>` The subquery assignments. Defaults to `None`.
            - This parameter must be a dictionary with the column name as the
              key and instances of `SelectQuery` as the value.

        ### Example:
        >>> (
                db.user.update()
                .set(
                    "name = John", "age = %s", args=20,
                    subqueries={"status": db.user_info.select("status").where("user_id = 1")}
                )
                .where("id = 1")
            )
        ### -> Equivalent to:
        >>> UPDATE db.user AS t1
            SET
                name = 'John',
                age = 20,
                status = (SELECT status FROM db.user_info AS t1 WHERE user_id = 1)
            WHERE id = 1
        """
        SET(self, *assignments, args=args, subqueries=subqueries)
        return self

    def where(
        self,
        *conditions: str,
        args: Any = None,
        ins: dict[str, list | tuple] | None = None,
        not_ins: dict[str, list | tuple] | None = None,
        subqueries: dict[str, SelectQuery] | None = None,
    ) -> Self:
        """The `WHERE` clause of the MySQL UPDATE query.

        :param conditions: `<str>` Condition expressions: `"id = 1"`, `"name = 'John'"`, `"COUNT(*) > 10"`, etc.
        :param args: `<Any/None>` Arguments for the `'%s'` placeholders of 'conditions'. Defaults to `None`.
        :param ins: `<dict/None>` The `IN` modifier. Defaults to `None`.
            - This parameter must be a dictionary with the column name as the key and
              iterable types such as `list`, `tuple`, `set`, `Series` as the value.
            - For example, `{"id": [1, 2, 3]}` -> `"id IN (1, 2, 3)"`. Duplicates
              are removed automatically.

        :param not_ins: `<dict/None>` The `NOT IN` modifier. Defaults to `None`.
            - Refert to 'ins' argument for more detail.

        :param subqueries: `<dict/None>` The subquery conditions. Defaults to `None`.
            - This parameter must be a dictionary with the column name along with
              the `desired operator` as the key and instances of `SelectQuery`
              as the value.

        ### Example:
        >>> db.user.update(...)
            .set(...)
            .where(
                "t1.ranking > 3",
                "t1.department = %s",
                args="Sales",
                ins={"t1.id": [1, 2, 3]},
                subqueries={"t1.name IN": db.user_info.select("name").where("age > 23")}
            )
        ### -> Equivalent to:
        >>> UPDATE db.user AS t1
            SET ...
            WHERE t1.ranking > 3
                AND t1.department = 'Sales'
                AND t1.id IN (1, 2, 3)
                AND t1.name IN (SELECT name FROM db.user_info AS t1 WHERE age > 23)
        """
        UPDATE_WHERE(
            self,
            *conditions,
            args=args,
            ins=ins,
            not_ins=not_ins,
            subqueries=subqueries,
        )
        return self

    def values(
        self,
        values: object,
        value_columns: Union[list[str | Column], tuple[str | Column]],
        where_columns: Union[list[str | Column], tuple[str | Column]],
    ) -> Self:
        """The `VALUES` clause of the MySQL UPDATE query.

        The `values` method is not a legit SQL clause but a custom design to
        facilitate batch updates of different rows on different conditions. For
        instance, `values()` can be used to construct the following syntax:

        >>> UPDATE...
            SET <col1> = %s, <col2> = %s, ...
            WHERE <col3> = %s AND <col4> = %s AND ...

        :param values: The values to `UPDATE`, accepts:
            - `<dict>`: Represents one row of a table data.
            - `<sequence>`: Data types such as `<list>`, `<tuple>`, each item represents one row of a table data.
            - `<DataFrame>`: Represents rows of a table data.

        :param value_columns: `<list/tuple>` The columns from the 'values' to construct the `SET` assignment syntax.
        :param where_columns: `<list/tuple>` The columns from the 'values' to construct the `WHERE` condition syntax.

        ### Example (Only `values()` method):
        >>> values = [
                {"id": 1, "name": "John", "age": 20, "status": "active"},
                {"id": 2, "name": "Mary", "age": 25, "status": "inactive"},
            ]
        >>> (
                db.user.update()
                .values(
                    values,
                    value_columns=["name", "age"],
                    where_columns=["id"]
                )
            )
        ### -> Equivalent to the following TWO queries:
        >>> UPDATE db.user AS t1
            SET t1.name = 'John', t1.age = 20
            WHERE t1.id = 1
        >>> UPDATE db.user AS t1
            SET t1.name = 'Mary', t1.age = 25
            WHERE t1.id = 2

        ### Example (Mixed with `set()` and `where()` methods):
        >>> values = [
                {"id": 1, "name": "John", "age": 20, "status": "active"},
                {"id": 2, "name": "Mary", "age": 25, "status": "inactive"},
            ]
        >>> (
                db.user.update()
                .set("t1.status = 'inactive'")
                .where("t1.age > 18")
                .values(
                    values,
                    value_columns=["name", "age"],
                    where_columns=["id"]
                )
            )
        ### -> Equivalent to the following TWO queries:
        >>> UPDATE db.user AS t1
            SET t1.status = 'inactive', t1.name = 'John', t1.age = 20
            WHERE t1.age > 18 AND t1.id = 1
        >>> UPDATE db.user AS t1
            SET t1.status = 'inactive', t1.name = 'Mary', t1.age = 25
            WHERE t1.age > 18 AND t1.id = 2
        """
        UPDATE_VALUES(
            self,
            values,
            value_columns=value_columns,
            where_columns=where_columns,
        )
        return self

    # Construct main statement -------------------------------------------------------------------
    async def _construct_main_stmt(self) -> str:
        """Construct the final query statement `<str>`"""
        # Construct statement
        self._main_stmt = ""
        stmt: str
        try:
            # . custom statement
            if self._custom is not None:
                stmt = self._construct_custom_stmt()
            # . update values
            elif self._values is not None:
                stmt = await self._construct_update_values()
            # . update basics
            else:
                stmt = await self._construct_update_basics()
        except errors.QueryValueError:
            raise
        except Exception as err:
            raise errors.QueryValueError(
                "{}Query construction failed: {}".format(self._err_pfix(), err)
            ) from err

        # Replace subqueries
        await self._process_subquery()
        if self._subquery_invalid:
            return ""  # exit
        if self._subquery_repls:
            stmt = self._replace_plhs(stmt, self._subquery_repls)

        # Adjust semicolumn
        stmt = self._stmt_semicolon(stmt)

        # Set & return statement
        self._main_stmt = stmt
        return stmt

    async def _construct_update_values(self) -> str:
        "Construct the `'UPDATE...` statment with `values()` method. `<str>`"
        # Validate
        if self._join_clauses:
            raise errors.QueryValueError(
                "{}Method 'values()' cannot be used with 'join()' "
                "method in the same UPDATE query.".format(self._err_pfix())
            )
        if self._tabletimes is not None:
            raise errors.QueryValueError(
                "{}Method 'values()' cannot be used with 'tabletimes()' method "
                "in the same UPDATE query. Sub-tables will be determined by "
                "the given 'values' data.".format(self._err_pfix())
            )

        # Set statement mode
        self._main_stmt_mode = 4 if self._query_timetables else 3

        # Construct statement
        clauses = self._get_update_clauses()
        syntax: list = []
        for clause in clauses:
            list_append(syntax, clause.syntax)
        values = self._get_update_values(False)
        if self._set is None:
            list_insert(syntax, 1, values.set_syntax)
        if not self._where_clauses:
            list_insert(syntax, 2, values.where_syntax)
        return "\n".join(syntax)

    async def _construct_update_basics(self) -> str:
        "Construct the standard `'UPDATE...` statment. `<str>`"
        # Set statement mode
        if self._query_timetables:
            await self._process_timetable()
            if self._timetable_invalid:
                return ""  # exit
            self._main_stmt_mode = 2
        else:
            self._main_stmt_mode = 1

        # Construct statement
        clauses = self._get_update_clauses()
        return self._join_clause_syntax(clauses)

    # Execute ------------------------------------------------------------------------------------
    async def execute(
        self,
        conn: Union[Connection, None] = None,
        concurrency: int = 10,
        cursor: type[Cursor | SSCursor] = Cursor,
        timeout: Union[int, None] = None,
        warnings: bool = True,
        *,
        retry_on_error: bool = True,
        retry_times: int = -1,
        min_wait_time: float = 0.2,
        max_wait_time: float = 2,
        stats: bool = False,
    ) -> int:
        """Execute the UPDATE query.

        ### Notice
        When UPDATE query involves tables that does not exists (especially for TimeTables),
        instead of raising errors, only warnings will be issued, and the query will be
        considered as successful (0 affected rows for non-existing tables). This behavior
        allows existing tables to perform UPDATE operations as usual while ignoring
        non-existing ones.

        :param conn: `<Connection>` The connection to execute the query. Defaults to `None`.
            - If `None`, query will be executed by temporary connections
              acquired from the Server pool.
            - If specified, query will be executed by the given connection.

        :param concurrency: `<int>` The maximum number of concurrent executions. Defaults to `10`.
            - When the query is consists of multiple sub-queries (For example, update data
              on different sub-tables of a TimeTable or batch updates of data through `values()`
              method), this argument determines the maximum number of concurrent updates to
              be executed at the same time.
            - * Notice: This argument is only applicable when `conn=None`. If 'conn'
              is specified, all queries (if applicable) will be executed sequentially
              by the given connection.

        :param cursor: `<type[Cursor/SSCursor]>` The `Cursor` class to use for query execution. Defaults to `Cursor`.

        :param timeout: `<int>` Query execution timeout in seconds. Dafaults to `None`.
            - If set to `None` or `0`, `tables.server.query_timeout` will be used
              as the default timeout.
            - `SQLQueryTimeoutError` will be raised when the timeout is reached.

        :param warnings: `<bool>` Whether to issue any SQL related warnings. Defaults to `True`.

        :param retry_on_error: `<bool>` Whether to retry when non-critial SQL error occurs. Defaults to `True`.
            - If `True`, when a non-critical SQL error occurs (such as `connection timeout`,
              `connection lost`, etc.), the query will be retried.
            - If `False`, errors will be raised immediately.

        :param retry_times: `<int>` The maximum number of retries. Defaults to `-1`.
            - * Notice: This argument is only applicable when `retry_on_error=True`.
            - For `retry_times <= 0`, the query retries indefinitely until success.
            - For `retry_times > 0`, the query retries up to the given 'retry_times'.

        :param min_wait_time: `<float>` The minimum wait time in seconds between each retries. Defaults to `0.2`.
            - * Notice: This argument is only applicable when `retry_on_error=True`.

        :param max_wait_time: `<float>` The maximum wait time in seconds between each retries. Defaults to `2`.
            - * Notice: This argument is only applicable when `retry_on_error=True`.

        :param stats: `<bool>` Whether to print the query execution stats to the console. Defaults to `False`.
        :raises: Subclass of `QueryError`.
        :return `<int>`: The number of rows affected by the query.
        """
        # Set parameters
        self._concurrency = concurrency
        self._cursor = cursor
        self._timeout = timeout
        self._warnings = warnings
        self._retry_on_error = retry_on_error
        self._retry_times = retry_times
        self._min_wait_time = min_wait_time
        self._max_wait_time = max_wait_time

        try:
            # Construct statement
            stmt = await self._construct_main_stmt()

            # Invalid query statement
            if not stmt:
                return 0  # exit

            # Execute on specified connection
            elif isinstance(conn, Connection):
                self._affected_rows = await self._execute_on_spec_conn(stmt, conn)

            # Execute on temporary connection(s)
            else:
                self._affected_rows = await self._execute_on_temp_conn(stmt)

            # Return affected rows
            return self._affected_rows

        finally:
            self._statistics(stats, None)
            self._collect_garbage()

    async def _execute_on_temp_conn(self, stmt: str) -> int:
        """Execute query on temporary connection(s)."""
        # Determine execute function
        if self._retry_on_error:
            func = self._exe_with_retry
        else:
            func = self._exe_nill_retry

        # Update basics - specific table
        if self._main_stmt_mode == 1:
            return await func(stmt, None, None, False)

        # Update basics - timetables
        elif self._main_stmt_mode == 2:
            self._setup_semaphore()
            tasks = [
                self._call_sem(func, self._replace_plhs(stmt, repls), None, None, False)
                for repls in self._timetable_repls
            ]
            res = await gather(*tasks)
            return sum(res) if res else 0

        # Update values - specific table
        elif self._main_stmt_mode == 3:
            self._setup_semaphore()
            tasks = [
                self._call_sem(func, stmt, arg, None, False)
                for arg in self._values.values
            ]
            res = await gather(*tasks)
            return sum(res) if res else 0

        # Update values - timetables
        elif self._main_stmt_mode == 4:
            self._setup_semaphore()
            key = self._update._tb_name
            tasks = [
                self._call_sem(func, uni_replace(stmt, key, tb, -1), arg, None, False)
                for tb, args in dict_items(self._values.values)
                for arg in args
            ]
            res = await gather(*tasks)
            return sum(res) if res else 0

        # Invalid - undetermined
        else:
            raise errors.QueryValueError(
                "{}Invalid query statement mode: {}. Query mode is "
                "undetermined.".format(self._err_pfix(), self._main_stmt_mode)
            )

    async def _execute_on_spec_conn(self, stmt: str, conn: object) -> int:
        """Execute the query on specified connection."""
        # Determine execute function
        func = self._exe_nill_retry

        # Update basics - specific table
        if self._main_stmt_mode == 1:
            return await func(stmt, None, conn, False)

        # Update basics - timetables
        elif self._main_stmt_mode == 2:
            rows: int = 0
            for repls in self._timetable_repls:
                rows += await func(self._replace_plhs(stmt, repls), None, conn, False)
            return rows

        # Update values - specific table
        elif self._main_stmt_mode == 3:
            values: list = self._values.values
            rows: int = 0
            for arg in values:
                rows += await func(stmt, arg, conn, False)
            return rows

        # Update values - timetables
        elif self._main_stmt_mode == 4:
            key = self._update._tb_name
            rows: int = 0
            for tb, args in dict_items(self._values.values):
                rows += await func(uni_replace(stmt, key, tb, -1), args, conn, False)
            return rows

        # Invalid - undetermined
        else:
            raise errors.QueryValueError(
                "{}Invalid query statement mode: {}. Query mode is "
                "undetermined.".format(self._err_pfix(), self._main_stmt_mode)
            )


@cython.cclass
class DeleteQuery(Query):
    """The `DELETE` query of the Table."""

    def __init__(self, table: Table) -> None:
        super().__init__("Delete Query", table)

    # SQL methods --------------------------------------------------------------------------------
    def delete(
        self,
        *table_aliases: str,
        ignore: cython.bint = False,
        tabletime: Union[str, datetime.date, datetime.datetime, None] = None,
        alias: Union[str, None] = None,
    ) -> Self:
        """The `DELETE` clause of the MySQL query

        :param table_aliases: `<str>` The table aliases of the DELETE operation.
            - Only applicable for multi-table DELETE (when JOIN clause is used).
              Single table DELETE takes no effects.
            - If not specified, the DELETE operation will be performed on all
              tables (main & joined ones).
            - If specified, the DELETE operation will be performed only on the
              given tables.
            - *Notice: this arguments only takes the alias of the tables instead
              of the actual table name. For more information, please refer to
              the 'alias' parameter or the Example section below.

        :param ignore: `<bool>` The `IGNORE` modifier. Defaults to `False`.
            Determines whether to ignore the duplicate key errors.

        :param tabletime: `<str/None>` A specific `tabletime` for the `DELETE` table. Defaults to `None`.
            - This parameter is only applicable when the `DELETE` table corresponds
              to a TimeTable.
            - If `tabletime` is specified, the actual sub-table will derive from this
              parameter. Otherwise, it is required to use `tabletimes()` method to specify
              the sub-tables. For more details, please refer to the `tabletimes()` method.

        :param alias: `<str/None>` The alias of the `DELETE` clause. Defaults to `None`.
            - The alias of the clause will be added to the corresponding part of the SQL
              statement using the `'AS <alias>'` syntax.
            - For instance, in a `DELETE... FROM... WHERE...` query, without specified
              alias (default alias), the statement would be constructed as:
              `'DELETE... FROM... AS t1 WHERE...'`, where default alias is derived
              from the order of the tables in the query.
            - However, with a user-defined alias (for example, `alias='tb'`), the
              statement would be constructed as: `'DELETE... FROM... AS tb WHERE...'`.

        ### Example (DELETE... WHERE... single table):
        >>> await db.user.delete().where("id = 1").execute()
        ### -> Equivalent to:
        >>> DELETE FROM db.user AS t1 WHERE id = 1

        ### Example (DELETE... JOIN... WHERE... multi-table [all tables]):
        >>> (
                await db.user.delete()  # delete from 't1' and 't2'
                .join(db.user_info, "t1.id = t2.user_id", tabletime="2023-01-01")
                .where("t1.age > 18")
                .execute()
            )
        ### -> Equivalent to:
        >>> DELETE t1, t2 FROM db.user AS t1
            INNER JOIN db.user_info_202301 AS t2
                ON t1.id = t2.user_id
            WHERE t1.age > 18

        ### Example (DELETE... JOIN... WHERE... multi-table [specific table(s)]):
        >>> (
                await db.user.delete("t2")  # Only delete from 't2'
                .join(db.user_info, "t1.id = t2.user_id", tabletime="2023-01-01")
                .where("t1.age > 18")
                .execute()
            )
        ### -> Equivalent to:
        >>> DELETE t2 FROM db.user AS t1
            INNER JOIN db.user_info_202301 AS t2
                ON t1.id = t2.user_id
            WHERE t1.age > 18

        ### Example (DELETE... with `values()` method):
        >>> values = [
                {"id": 1, "name": "John", "age": 20, "status": "active"},
                {"id": 2, "name": "Mary", "age": 25, "status": "inactive"},
            ]
        >>> (
                await db.user.delete()
                .values(values, where_columns=["name", "age"])
                .execute()
            )
        ### -> Equivalent to the following TWO queries:
        >>> DELETE FROM db.user AS t1
            WHERE t1.name = 'John' AND t1.age = 20
        >>> DELETE FROM db.user AS t1
            WHERE t1.name = 'Mary' AND t1.age = 25
        """
        DELETE(self, *table_aliases, ignore=ignore, tabletime=tabletime, alias=alias)
        return self

    def join(
        self,
        table: Union[str, Table, TimeTable, SelectQuery],
        *ons: str,
        args: Any = None,
        ins: Union[dict[str, Union[list, tuple]], None] = None,
        not_ins: Union[dict[str, Union[list, tuple]], None] = None,
        subqueries: Union[dict[str, SelectQuery], None] = None,
        method: Literal["INNER", "LEFT", "RIGHT"] = "INNER",
        tabletime: Union[str, datetime.date, datetime.datetime, None] = None,
        alias: Union[str, None] = None,
    ) -> Self:
        """The `JOIN` clause of the MySQL DELETE query.

        :param table: The table to join. This can be specified in two ways:
            - By providing the name `<str>` or instance `<Table/TimeTable>` of the
              table to join. This is equivalent to `"JOIN <table> ..."`.
            - By providing an instance of `SelectQuery` as a subquery. This
              will make the statement following the `JOIN` clause into a
              subquery. Equivalent to `"JOIN (SELECT... FROM ... )"`.

        :param ons: `<str>` Condition expressions: `"t1.id = t2.id"`, `"t2.age > 23"`, etc.
            - Each column should be manually prefixed with the correct alias.
              For more details about aliasing, please refer to the `alias`
              parameter.

        :param args: `<Any/None>` Arguments for the `'%s'` placeholders of 'ons'. Defaults to `None`.

        :param ins: `<dict/None>` The `IN` modifier. Defaults to `None`.
            - This parameter must be a dictionary with the column name as the key and
              iterable types such as `list`, `tuple`, `set`, `Series` as the value.
            - For example, `{"t2.id": [1, 2, 3]}` -> `"t2.id IN (1, 2, 3)"`. Duplicates
              are removed automatically.

        :param not_ins: `<dict/None>` The `NOT IN` modifier. Defaults to `None`.
            - Refert to 'ins' argument for more detail.

        :param subqueries: `<dict/None>` The subquery conditions. Defaults to `None`.
            - This parameter must be a dictionary with the column name along with
              the `desired operator` as the key and instances of `SelectQuery`
              as the value.
            - For example: `{"t2.name =": db.user.select("name").where("age > 23")}` ->
              `"t2.name = (SELECT name FROM db.user AS t1 WHERE age > 23)"`.

        :param method: `<str>` The join method. Defaults to `'INNER'`.

        :param tabletime: `<str/None>` A specific `tabletime` for the `JOIN` table. Defaults to `None`.
            - This parameter is only applicable when the argument 'table' corresponds
              to a TimeTable (regardless type of `<str>` or `<TimeTable>` instance).
            - If `tabletime` is specified, the actual sub-table will derive from this
              parameter. Otherwise, it is required to use `tabletimes()` method to specify
              the sub-tables. For more details, please refer to the `tabltimes()` method.

        :param alias: `<str/None>` The alias of the `JOIN` clause. Defaults to `None`.
            - The alias of the clause will be added to the corresponding part of the SQL
              statement using the `'AS <alias>'` syntax.
            - For instance, in a `DELETE ... JOIN ...` query, without specified
              alias (default alias), the statement would be constructed as:
              `'DELETE t1, t2 ... FROM ... AS t1 JOIN ... AS t2'`, where default
              alias is derived from the order of the tables in the query.
            - However, with a user-defined alias (for example, `alias='join_tb'`), the
              statement would be constructed as:
              `'DELETE t1, join_tb FROM ... AS t1 JOIN ... AS join_tb'`.

        ### Example:
        >>> (
                db.user.delete()
                .join(db.user_info, "t1.id = t2.user_id", tabletime="2023-01-01")
                .where("t1.age > 18")
            )
        ### -> Equivalent to:
        >>> DELETE t1, t2 FROM db.user AS t1
            INNER JOIN db.user_info_202301 AS t2
                ON t1.id = t2.user_id
            WHERE t1.age > 18
        """
        DELETE_JOIN(
            self,
            table,
            *ons,
            args=args,
            ins=ins,
            not_ins=not_ins,
            subqueries=subqueries,
            method=method,
            tabletime=tabletime,
            alias=alias,
        )
        return self

    def where(
        self,
        *conditions: str,
        args: Any = None,
        ins: dict[str, list | tuple] | None = None,
        not_ins: dict[str, list | tuple] | None = None,
        subqueries: dict[str, SelectQuery] | None = None,
    ) -> Self:
        """The `WHERE` clause of the MySQL DELETE query.

        :param conditions: `<str>` Condition expressions: `"id = 1"`, `"name = 'John'"`, `"COUNT(*) > 10"`, etc.
        :param args: `<Any/None>` Arguments for the `'%s'` placeholders of 'conditions'. Defaults to `None`.
        :param ins: `<dict/None>` The `IN` modifier. Defaults to `None`.
            - This parameter must be a dictionary with the column name as the key and
              iterable types such as `list`, `tuple`, `set`, `Series` as the value.
            - For example, `{"id": [1, 2, 3]}` -> `"id IN (1, 2, 3)"`. Duplicates
              are removed automatically.

        :param not_ins: `<dict/None>` The `NOT IN` modifier. Defaults to `None`.
            - Refert to 'ins' argument for more detail.

        :param subqueries: `<dict/None>` The subquery conditions. Defaults to `None`.
            - This parameter must be a dictionary with the column name along with
              the `desired operator` as the key and instances of `SelectQuery`
              as the value.

        ### Example:
        >>> db.user.delete(...)
            .where(
                "t1.ranking > 3",
                "t1.department = %s",
                args="Sales",
                ins={"t1.id": [1, 2, 3]},
                subqueries={"t1.name IN": db.user_info.select("name").where("age > 23")}
            )
        ### -> Equivalent to:
        >>> DELETE FROM db.user AS t1
            WHERE t1.ranking > 3
                AND t1.department = 'Sales'
                AND t1.id IN (1, 2, 3)
                AND t1.name IN (SELECT name FROM db.user_info AS t1 WHERE age > 23)
        """
        DELETE_WHERE(
            self,
            *conditions,
            args=args,
            ins=ins,
            not_ins=not_ins,
            subqueries=subqueries,
        )
        return self

    def values(
        self,
        values: object,
        *where_columns: Union[str, Column],
    ) -> Self:
        """The `VALUES` clause of the MySQL DELETE query.

        The `values()` method is not a legit SQL clause but a custom design to
        facilitate batch deletes of different rows on different conditions. For
        instance, `DELETE_VALUES` can be used to construct the following syntax:

        >>> DELETE...
            WHERE <col1> = %s AND <col1> = %s AND ...

        :param query: `<Query>` The query of the clause.
        :param values: The values to `DELETE`, accepts:
            - `<dict>`: Represents one row of a table data.
            - `<sequence>`: Data types such as `<list>`, `<tuple>`, each item represents one row of a table data.
            - `<DataFrame>`: Represents rows of a table data.

        :param where_columns: `<str/Column>` The columns from the 'values' to construct the `WHERE` condition syntax.

        ### Example:
        >>> values = [
                {"id": 1, "name": "John", "age": 20, "status": "active"},
                {"id": 2, "name": "Mary", "age": 25, "status": "inactive"},
            ]
        >>> (
                db.user.delete()
                .values(values, where_columns=["name", "age"])
            )
        ### -> Equivalent to the following TWO queries:
        >>> DELETE FROM db.user AS t1
            WHERE t1.name = 'John' AND t1.age = 20
        >>> DELETE FROM db.user AS t1
            WHERE t1.name = 'Mary' AND t1.age = 25
        """
        DELETE_VALUES(self, values, *where_columns)
        return self

    # Construct main statement -------------------------------------------------------------------
    async def _construct_main_stmt(self) -> str:
        """Construct the final query statement `<str>`"""
        # Construct statement
        self._main_stmt = ""
        stmt: str
        try:
            # . custom statement
            if self._custom is not None:
                stmt = self._construct_custom_stmt()
            # . delete values
            elif self._values is not None:
                stmt = await self._construct_delete_values()
            # . delete basics
            else:
                stmt = await self._construct_delete_basics()
        except errors.QueryValueError:
            raise
        except Exception as err:
            raise errors.QueryValueError(
                "{}Query construction failed: {}".format(self._err_pfix(), err)
            ) from err

        # Replace subqueries
        await self._process_subquery()
        if self._subquery_invalid:
            return ""  # exit
        if self._subquery_repls:
            stmt = self._replace_plhs(stmt, self._subquery_repls)

        # Adjust semicolumn
        stmt = self._stmt_semicolon(stmt)

        # Set & return statement
        self._main_stmt = stmt
        return stmt

    async def _construct_delete_values(self) -> str:
        "Construct the `'DELETE...` statment with `values()` method. `<str>`"
        # Validate
        if self._join_clauses:
            raise errors.QueryValueError(
                "{}Method 'values()' cannot be used with 'join()' "
                "method in the same DELETE query.".format(self._err_pfix())
            )
        if self._tabletimes is not None:
            raise errors.QueryValueError(
                "{}Method 'values()' cannot be used with 'tabletimes()' method "
                "in the same DELETE query. Sub-tables will be determined by "
                "the given 'values' data.".format(self._err_pfix())
            )

        # Set statement mode
        self._main_stmt_mode = 4 if self._query_timetables else 3

        # Construct statement
        clauses = self._get_delete_clauses()
        syntax: list = []
        for clause in clauses:
            list_append(syntax, clause.syntax)
        values = self._get_delete_values(False)
        if not self._where_clauses:
            list_insert(syntax, 1, values.where_syntax)
        return "\n".join(syntax)

    async def _construct_delete_basics(self) -> str:
        "Construct the standard `'DELETE...` statment. `<str>`"
        # Set statement mode
        if self._query_timetables:
            await self._process_timetable()
            if self._timetable_invalid:
                return ""  # exit
            self._main_stmt_mode = 2
        else:
            self._main_stmt_mode = 1

        # Construct statement
        clauses = self._get_delete_clauses()
        return self._join_clause_syntax(clauses)

    # Execute ------------------------------------------------------------------------------------
    async def execute(
        self,
        conn: Union[Connection, None] = None,
        concurrency: int = 10,
        cursor: type[Cursor | SSCursor] = Cursor,
        timeout: Union[int, None] = None,
        warnings: bool = True,
        *,
        retry_on_error: bool = True,
        retry_times: int = -1,
        min_wait_time: float = 0.2,
        max_wait_time: float = 2,
        stats: bool = False,
    ) -> int:
        """Execute the DELETE query.

        ### Notice
        When DELETE query involves tables that does not exists (especially for TimeTables),
        instead of raising errors, only warnings will be issued, and the query will be
        considered as successful (0 affected rows for non-existing tables). This behavior
        allows existing tables to perform the DELETE operations as usual while ignoring
        non-existing ones.

        :param conn: `<Connection>` The connection to execute the query. Defaults to `None`.
            - If `None`, query will be executed by temporary connections
              acquired from the Server pool.
            - If specified, query will be executed by the given connection.

        :param concurrency: `<int>` The maximum number of concurrent executions. Defaults to `10`.
            - When the query is consists of multiple sub-queries (For example, delete data
              from different sub-tables of a TimeTable or batch delete of data through `values()`
              method), this argument determines the maximum number of concurrent deletes to
              be executed at the same time.
            - * Notice: This argument is only applicable when `conn=None`. If 'conn'
              is specified, all queries (if applicable) will be executed sequentially
              by the given connection.

        :param cursor: `<type[Cursor/SSCursor]>` The `Cursor` class to use for query execution. Defaults to `Cursor`.

        :param timeout: `<int>` Query execution timeout in seconds. Dafaults to `None`.
            - If set to `None` or `0`, `tables.server.query_timeout` will be used
              as the default timeout.
            - `SQLQueryTimeoutError` will be raised when the timeout is reached.

        :param warnings: `<bool>` Whether to issue any SQL related warnings. Defaults to `True`.

        :param retry_on_error: `<bool>` Whether to retry when non-critial SQL error occurs. Defaults to `True`.
            - If `True`, when a non-critical SQL error occurs (such as `connection timeout`,
              `connection lost`, etc.), the query will be retried.
            - If `False`, errors will be raised immediately.

        :param retry_times: `<int>` The maximum number of retries. Defaults to `-1`.
            - * Notice: This argument is only applicable when `retry_on_error=True`.
            - For `retry_times <= 0`, the query retries indefinitely until success.
            - For `retry_times > 0`, the query retries up to the given 'retry_times'.

        :param min_wait_time: `<float>` The minimum wait time in seconds between each retries. Defaults to `0.2`.
            - * Notice: This argument is only applicable when `retry_on_error=True`.

        :param max_wait_time: `<float>` The maximum wait time in seconds between each retries. Defaults to `2`.
            - * Notice: This argument is only applicable when `retry_on_error=True`.

        :param stats: `<bool>` Whether to print the query execution stats to the console. Defaults to `False`.
        :raises: Subclass of `QueryError`.
        :return `<int>`: The number of rows affected by the query.
        """
        # Set parameters
        self._concurrency = concurrency
        self._cursor = cursor
        self._timeout = timeout
        self._warnings = warnings
        self._retry_on_error = retry_on_error
        self._retry_times = retry_times
        self._min_wait_time = min_wait_time
        self._max_wait_time = max_wait_time

        try:
            # Construct statement
            stmt = await self._construct_main_stmt()

            # Invalid query statement
            if not stmt:
                return 0  # exit

            # Execute on specified connection
            elif isinstance(conn, Connection):
                self._affected_rows = await self._execute_on_spec_conn(stmt, conn)

            # Execute on temporary connection(s)
            else:
                self._affected_rows = await self._execute_on_temp_conn(stmt)

            # Return affected rows
            return self._affected_rows

        finally:
            self._statistics(stats, None)
            self._collect_garbage()

    async def _execute_on_temp_conn(self, stmt: str) -> int:
        """Execute query on temporary connection(s)."""
        # Determine execute function
        if self._retry_on_error:
            func = self._exe_with_retry
        else:
            func = self._exe_nill_retry

        # Delete basics - specific table
        if self._main_stmt_mode == 1:
            return await func(stmt, None, None, False)

        # Delete basics - timetables
        elif self._main_stmt_mode == 2:
            self._setup_semaphore()
            tasks = [
                self._call_sem(func, self._replace_plhs(stmt, repls), None, None, False)
                for repls in self._timetable_repls
            ]
            res = await gather(*tasks)
            return sum(res) if res else 0

        # Delete values - specific table
        elif self._main_stmt_mode == 3:
            self._setup_semaphore()
            tasks = [
                self._call_sem(func, stmt, arg, None, False)
                for arg in self._values.values
            ]
            res = await gather(*tasks)
            return sum(res) if res else 0

        # Delete values - timetables
        elif self._main_stmt_mode == 4:
            self._setup_semaphore()
            key = self._delete._tb_name
            tasks = [
                self._call_sem(func, uni_replace(stmt, key, tb, -1), arg, None, False)
                for tb, args in dict_items(self._values.values)
                for arg in args
            ]
            res = await gather(*tasks)
            return sum(res) if res else 0

        # Invalid - undetermined
        else:
            raise errors.QueryValueError(
                "{}Invalid query statement mode: {}. Query mode is "
                "undetermined.".format(self._err_pfix(), self._main_stmt_mode)
            )

    async def _execute_on_spec_conn(self, stmt: str, conn: object) -> int:
        """Execute the query on specified connection."""
        # Determine execute function
        func = self._exe_nill_retry

        # Delete basics - specific table
        if self._main_stmt_mode == 1:
            return await func(stmt, None, conn, False)

        # Delete basics - timetables
        elif self._main_stmt_mode == 2:
            rows: int = 0
            for repls in self._timetable_repls:
                rows += await func(self._replace_plhs(stmt, repls), None, conn, False)
            return rows

        # Delete values - specific table
        elif self._main_stmt_mode == 3:
            values: list = self._values.values
            rows: int = 0
            for arg in values:
                rows += await func(stmt, arg, conn, False)
            return rows

        # Delete values - timetables
        elif self._main_stmt_mode == 4:
            key = self._delete._tb_name
            rows: int = 0
            for tb, args in dict_items(self._values.values):
                rows += await func(uni_replace(stmt, key, tb, -1), args, conn, False)
            return rows

        # Invalid - undetermined
        else:
            raise errors.QueryValueError(
                "{}Invalid query statement mode: {}. Query mode is "
                "undetermined.".format(self._err_pfix(), self._main_stmt_mode)
            )


@cython.cclass
class CreateTempQuery(SelectShare):
    """The `CREATE TEMPORARY TABLE...` query of the Table."""

    def __init__(self, table: Table) -> None:
        super().__init__("Create Temp Query", table)

    # SQL methods --------------------------------------------------------------------------------
    def create_temp(
        self,
        *columns: Union[str, Column],
        indexes: Union[list[str | Index], Literal["auto"], None] = None,
        engine: Literal["MEMORY", "InnoDB", "MyISAM"] = "MEMORY",
        charset: Union[str, None] = None,
        collate: Union[str, None] = None,
    ) -> Self:
        """The `CREATE TEMPORARY TABLE` clause of the MySQL query.

        :param columns: `<str/Column>` The columns of the temporary table, accept both `<str>` and column instances `<Column>`.
            - If columns are not specified, defaults to the 'columns' of the table
              that initiated the 'create_temp()' query.
            - By providing the Column instances or the name of the columns,
              the columns syntax will be generated based on the given columns.
            - However, a custom column syntax `<str>` is also acceptable,
              for example: `"id BIGINT NOT NULL UNSIGNED AUTO_INCREMENT PRIMARY KEY"`.
            - * Notice: Regardless the approach, the column name must belong
              to the table that initiated the query.

        :param indexes: `<list/'auto'/None>` The indexes of the temporary table. Defaults to `None`.
            - `None`: No indexes for the temporary table.
            - `'auto'`: Indexes syntax will be generated automatically based on
              the given columns that belongs to the table.
            - By providing a list/tuple of Index instances or the name of the
              indexes, the index syntax will be generated based on the given indexes.
            - However, a custom index syntax `<str>` is also acceptable,
              for example: `"INDEX idx1 (id, name)"`.

        :param engine: `<str>` The engine of the temporary table. Defaults to `'MEMORY'`.
        :param charset: `<str/None>` The charset of the temporary table. Defaults to `None` (table.charset).
        :param collate: `<str/None>` The collate of the temporary table. Defaults to `None` (table.collate).

        ### Example (CREATE TEMPORARY TABLE... SELECT...):
        >>> async with db.user.transaction() as conn:  # acquire connection
                tmp = ( # . 'tmp' is the name of the created temporary table.
                        await db.user.create_temp("id", "user_name", "age", indexes="auto")
                        # . specify temporary table columns to: "id", "user_name", "age".
                        # . auto generate indexes based on the given columns and table settings.
                        # . use 'MEMORY' engine and 'user' table's charset and collate
                        .select(db.user.select("id", "user_name", "age").where("age > 18")
                        # . select data from 'user' table.
                        .execute(conn)
                        # . provide the connection to `execute()` method.
                    )
                ...  # do something with the temporary table in the transaction
        ### -> Equivalent to:
        >>> CREATE TEMPORARY TABLE db.user_tmp0 (
                id BIGINT NOT NULL UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                user_name VARCHAR(255) NOT NULL,
                age INT NOT NULL,
                UNIQUE INDEX uixUserName (user_name)
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_as_cs ENGINE = MEMORY
            SELECT id, user_name, age FROM db.user AS t1 WHERE age > 18;

        ### Example (CREATE TEMPORARY TABLE... VALUES...):
        >>> values = [
                {"id": 1, "name": "John", "age": 20, "status": "active"},
                {"id": 2, "name": "Mary", "age": 25, "status": "inactive"},
                ...
            ]
        >>> async with db.user.transaction() as conn:  # acquire connection
                tmp = ( # . 'tmp' is the name of the created temporary table.
                    await db.user.create_temp(
                        "id", "user_name", "age",
                        indexes=["UNIQUE INDEX idx1 (name)", "INDEX idx2 (age)"],
                        engine="InnoDB",
                        charset="utf8mb4", collate="utf8mb4_slovak_ci")
                        # . specify temporary table columns to: "id", "user_name", "age".
                        # . specify custom indexes for the temporary table.
                        # . use 'InnoDB' engine and 'utf8mb4' & 'utf8mb4_slovak_ci'.
                        .values(values, "name", "age")
                        # . specify the columns of the 'values' to insert. since 'id' is omitted,
                        # . only 'name' and 'age' will be inserted.
                        # . if columns are not specified, will defaults to 'id', 'user_name'
                        # . and 'age', which are the columns of the temporary table.
                        .execute(conn)
                        # . provide the connection to `execute()` method.
                    )
                ...  # do something with the temporary table in the transaction
        ### -> Equivalent to the following TWO queries:
        >>> CREATE TEMPORARY TABLE db.user_tmp1 (
                id BIGINT NOT NULL UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                user_name VARCHAR(255) NOT NULL,
                age INT NOT NULL,
                UNIQUE INDEX idx1 (name),
                INDEX idx2 (age)
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_slovak_ci ENGINE = InnoDB;
        >>> INSERT INTO db.user_tmp1 (user_name, age)
            VALUES ('John', 20), ('Mary', 25), ...;
        """
        CREATE_TEMP(
            self,
            *columns,
            indexes=indexes,
            engine=engine,
            charset=charset,
            collate=collate,
        )
        return self

    def select(
        self,
        from_: Union[str, Table, TimeTable, SelectQuery],
        *columns: Union[str, Column],
        distinct: cython.bint = False,
        tabletime: Union[str, datetime.date, datetime.datetime, None] = None,
        alias: Union[str, None] = None,
    ) -> Self:
        """The `SELECT... FROM...` clause of the MySQL `CREATE TEMPORARY TABLE` query.

        :param from_: Specify the `FROM` clause.
            - `<Table/TimeTable>`: The `FROM` clause will be generated based on the given table.
            - `<SelectQuery>`: The `FROM` cluase will be constructed as a subquery based on the
              given `SelectQuery` instance.
            - `<str>`: If the string corresponds to the table name of a table in the database,
              the `FROM` clause will be generated based on that table. Otherwise, the string will
              be treated as a raw sql syntax placed right after the `FROM` keyword.

        :param columns: `<str/Column>` The columns to select, accept both column names `<str>` and instances `<Column>`.
            - If not specified, defaults to the 'columns' of the `CREATE_TEMP` clause.
            - When a `CREATE TEMPORARY TABLE... SELECT...` query involves `JOIN`,
              each column must prefixed with the correct alias. For more information
              about aliasing, see the 'alias' parameter.

        :param distinct: `<bool>` The `DISTINCT` modifier. Defaults to `False`.
            - Determines whether to select only distinct values.
            - *Notice: When the `SELECT...` query involes TimeTable(s) which
              `tabletime` are specified through `tabletimes()` method, setting
              `distict=True` will force the use of union select mode. For more
              detail, please refer to the `order_by` or `limit` methods.

        :param tabletime: `<str/None>` A specific `tabletime` for the `FORM` table. Defaults to `None`.
            - This parameter is only applicable when the argument 'from_' corresponds
              to a TimeTable (regardless type of `<str>` or `<TimeTable>` instance).
            - If `tabletime` is specified, the actual sub-table will derive from this
              parameter. Otherwise, it is required to use `tabletimes()` method to
              specify the sub-tables. For more details, please refer to the `tabletimes()`
              method.

        :param alias: `<str/None>` The alias of the `CREATE TEMPORARY TABLE... SELECT... FROM...` clause. Defaults to `None`.
            - The alias of the clause will be added to the corresponding part of
              the SQL statement using the `'AS <alias>'` syntax.
            - For instance, in a `CREATE TEMPORARY TABLE... SELECT... FROM ...`
              query, without specified alias (default alias), the statement would be
              constructed as: `'CREATE TEMPORARY TABLE... SELECT ... FROM ... AS t1'`,
              where default alias is derived from the order of the tables in the query.
            - However, with a user-defined alias (for example, `alias='tb'`), the
              statement would be constructed as:
              `'CREATE TEMPORARY TABLE... SELECT ... FROM ... AS tb'`.

        ### Example (CREATE TEMPORARY TABLE... SELECT...):
        >>> async with db.user.transaction() as conn:  # acquire connection
                tmp = ( # . 'tmp' is the name of the created temporary table.
                        await db.user.create_temp("id", "user_name", "age", indexes="auto")
                        .select(db.user.select("id", "user_name", "age").where("age > 18")
                        # . select data from 'user' table.
                        .execute(conn)
                        # . provide the connection to `execute()` method.
                    )
                ...  # do something with the temporary table in the transaction
        ### -> Equivalent to:
        >>> CREATE TEMPORARY TABLE db.user_tmp0 (
                id BIGINT NOT NULL UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                user_name VARCHAR(255) NOT NULL,
                age INT NOT NULL,
                UNIQUE INDEX uixUserName (user_name)
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_as_cs ENGINE = MEMORY
            SELECT id, user_name, age FROM db.user AS t1 WHERE age > 18;
        """
        # Pre-validate
        if not isinstance(from_, (str, Table, SelectQuery)):
            raise errors.QueryValueError(
                "<{}> [{}] `SELECT`\nError: "
                "Argument 'from_' must be type of '<str>', '<Table>', or '<SelectQuery>', "
                "instead of: {}".format(self._tb._fname, self._name, type(from_))
            )
        SELECT(
            self,
            *columns if columns else self._get_create_temp(True)._columns,
            distinct=distinct,
            buffer_result=False,
            explain=False,
            from_=from_,
            tabletime=tabletime,
            alias=alias,
        )
        return self

    def values(
        self,
        values: object,
        *value_columns: Union[str, Column],
    ) -> Self:
        """The `VALUES` clause of the MySQL CREATE TEMPORARY TABLE query.

        The `CREATE_TEMP_VALUES` clause is not a legit SQL clause but a custom
        design to facilitate batch inserts of values into the temporary table
        after the table is created.

        :param values: The values for the TEMPORARY TABLE, accepts:
            - `<dict>`: Represents one row of a table data.
            - `<sequence>`: Data types such as `<list>`, `<tuple>`, each item represents one row of a table data.
            - `<DataFrame>`: Represents rows of a table data.

        :param value_columns: `<str/Column>` The columns from the 'values' to `'INSERT...'` into the temporary table.
            - If not specified, Defaults to the 'columns' of the `CREATE_TEMP` clause.

        ### Example (CREATE TEMPORARY TABLE... VALUES...):
        >>> values = [
                {"id": 1, "name": "John", "age": 20, "status": "active"},
                {"id": 2, "name": "Mary", "age": 25, "status": "inactive"},
                ...
            ]
        >>> async with db.user.transaction() as conn:  # acquire connection
                tmp = ( # . 'tmp' is the name of the created temporary table.
                    await db.user.create_temp("id", "user_name", "age", indexes="auto")
                        .values(values, "name", "age")
                        # . specify the columns of the 'values' to insert.
                        # . since 'id' is omitted, only 'name' and 'age' will be inserted.
                        # . if columns are not specified, will defaults to 'id', 'user_name', 'age',
                        # . which are the columns of the temporary table.
                        .execute(conn)
                        # . provide the connection to `execute()` method.
                    )
                ...  # do something with the temporary table in the transaction
        ### -> Equivalent to (two queries):
        >>> CREATE TEMPORARY TABLE db.user_tmp1 (
                id BIGINT NOT NULL UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                user_name VARCHAR(255) NOT NULL,
                age INT NOT NULL,
                UNIQUE INDEX uixUserName (user_name)
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_slovak_ci ENGINE = InnoDB;
        >>> INSERT INTO db.user_tmp1 (user_name, age)
            VALUES ('John', 20), ('Mary', 25), ...;
        """
        CREATE_TEMP_VALUES(self, values, *value_columns)
        return self

    # Construct main statement -------------------------------------------------------------------
    async def _construct_main_stmt(self) -> str:
        """Construct the final query statement `<str>`"""
        # Construct statement
        self._main_stmt = ""
        stmt: str
        try:
            # . custom statement
            if self._custom is not None:
                stmt = self._construct_custom_stmt()
            # . create temp values
            elif self._values is not None:
                stmt = await self._construct_create_temp_values()
            # . create temp basics
            else:
                stmt = await self._construct_create_temp_basics()
        except errors.QueryValueError:
            raise
        except Exception as err:
            raise errors.QueryValueError(
                "{}Query construction failed: {}".format(self._err_pfix(), err)
            ) from err

        # Adjust semicolumn
        stmt = self._stmt_semicolon(stmt)

        # Set & return statement
        self._main_stmt = stmt
        return stmt

    async def _construct_create_temp_values(self) -> str:
        "Construct the `'CREATE TEMPORARY TABLE...'` statement with `values()` method `<str>`."
        # Validate
        if self._select is not None:
            raise errors.QueryValueError(
                "{}Method 'values()' cannot be used with 'select()' "
                "method in the same CREATE TEMPORARY TABLE query.".format(
                    self._err_pfix()
                )
            )
        if self._tabletimes is not None:
            raise errors.QueryValueError(
                "{}Method 'values()' cannot be used with 'tabletimes()' method "
                "in the same CREATE TEMPORARY TABLE query. All data will be "
                "inserted into the same temp table regardless `tabletime`.".format(
                    self._err_pfix()
                )
            )

        # Set statement mode
        self._main_stmt_mode = 3

        # Construct insert statement
        columns: list = self._values._value_columns
        length: cython.int = list_len(columns)
        self._insert_stmt = "INSERT INTO %s (\n\t%s\n)\nVALUES (%s)" % (
            self._create_temp._tb_name,
            ",\n\t".join(columns),
            ", ".join(["%s"] * length),
        )

        # Return statement
        return self._create_temp.syntax

    async def _construct_create_temp_basics(self) -> str:
        "Construct the `'CREATE TEMPORARY TABLE... (SELECT...)'` statement `<str>`."
        # Set statement mode
        self._main_stmt_mode = 1

        # Create temp statement
        stmt: str = self._create_temp.syntax

        # Select statement
        if self._select is not None:
            select: str = await self._construct_select_stmt(True)
            if select:
                stmt = stmt + "\n" + select

        # Return statement
        return stmt

    # Execute ------------------------------------------------------------------------------------
    async def execute(
        self,
        conn: Connection,
        cursor: type[Cursor | SSCursor] = Cursor,
        timeout: Union[int, None] = None,
        warnings: bool = True,
        *,
        stats: bool = False,
    ) -> str:
        """Execute the CREATE TEMPORARY TABEL query.

        :param conn: `<Connection>` The connection to create the temporary table.

        :param cursor: `<type[Cursor/SSCursor]>` The `Cursor` class to use for query execution. Defaults to `Cursor`.

        :param timeout: `<int>` Query execution timeout in seconds. Dafaults to `None`.
            - If set to `None` or `0`, `tables.server.query_timeout` will be used
              as the default timeout.
            - `SQLQueryTimeoutError` will be raised when the timeout is reached.

        :param warnings: `<bool>` Whether to issue any SQL related warnings. Defaults to `True`.

        :param stats: `<bool>` Whether to print the query execution stats to the console. Defaults to `False`.
        :raises: Subclass of `QueryError`.
        :return `<str>`: The name of the new created temporary table.
        """
        # Set parameters
        self._cursor = cursor
        self._timeout = timeout
        self._warnings = warnings

        try:
            # Construct statement
            stmt = await self._construct_main_stmt()

            # Create temp table
            rows: int = await self._exe_nill_retry(stmt, None, conn, True)

            # Insert values
            if self._main_stmt_mode == 3:
                for arg in self._values.values:
                    rows += await self._exe_nill_retry(
                        self._insert_stmt, arg, conn, False
                    )

            # Set affected rows
            self._affected_rows = rows

            # Return temp table name
            return self._create_temp._tb_name

        finally:
            self._statistics(stats, None)
            self._collect_garbage()

    async def _exe_nill_retry(
        self,
        stmt: str,
        args: object,
        conn: object,
        resolve_absent_table: bool,
    ) -> int:
        "(Internal use only) Execute without retry on non-critical SQL errors."
        while True:
            try:
                return await self._db.execute_query(
                    stmt,
                    args=args,
                    conn=conn,
                    reusable=False,
                    cursor=self._cursor,
                    timeout=self._timeout,
                    warnings=self._warnings,
                    resolve_absent_table=resolve_absent_table,
                )
            except errors.QueryTableAbsentError:
                pass


# Compare Query =================================================================================
@cython.cclass
class COMPARE_DATA:
    """Represents the table data of the `CompareQuery`."""

    _query: CompareQuery
    _tb: Table
    _name: str
    _processed: cython.bint
    _dtypes: set[int]
    """
    - 1: DataFrame
    - 2: dict
    """
    _columns: list[str]
    _columns_set: set[str]
    _data: DataFrame

    def __init__(self, query: CompareQuery, name: str, *data: object) -> None:
        self._query = query
        self._tb = query._tb
        self._name = name
        self._processed = False
        self._construct(data)

    # Properties ---------------------------------------------------------------------------------
    @property
    def data(self) -> DataFrame:
        "The table data of the `CompareQuery`."
        # Already processed
        if self._processed:
            return self._data

        # Validate operations
        if not self._query._operations_ready:
            raise errors.QueryValueError(
                "{}Method `operations()` has not been called yet.".format(
                    self._err_pfix()
                )
            )

        # Parse tabletime column
        if self._tb._is_timetable:
            time_column = self._tb._columns._tabletime._name
            self._data[SUBTABLE_COLUMN] = self._parse_timetable_names(
                self._tb, self._data[time_column]
            )

        # Create unique columns
        self._data[UNIQUE_COLUMN] = self._tb.concat_df_columns(
            self._data[self._query._ops_unique]
        )

        # Validate uniqueness
        if self._data[UNIQUE_COLUMN].duplicated().any():
            raise errors.QueryValueError(
                "{}Invalid '{} Data', duplicate values exists in unique columns: "
                "{}".format(self._err_pfix(), self._name, self._query._unique_columns)
            )

        # Create compare columns
        if set_contains(self._query._operations, 2) or set_contains(
            self._query._operations, 5
        ):
            self._data[COMPARE_COLUMN] = self._tb.concat_df_columns(
                self._data[self._query._ops_compare]
            )

        # Return data
        self._processed = True
        return self._data

    # Data ---------------------------------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    def _construct(self, data: tuple):
        # Sort data
        self._dtypes = set()
        vals = self._sort_data(data)

        # Parse data
        # . empty
        length: cython.int = set_len(self._dtypes)
        if length == 0:
            raise errors.QueryValueError(
                "{}Invalid '{} Data', cannot be empty.".format(
                    self._err_pfix(), self._name
                )
            )
        # . invalid
        if length > 1:
            raise errors.QueryValueError(
                "{}Invalid '{} Data' types, don't support mix of `<DataFrame>` "
                "and `<dict>`.".format(self._err_pfix(), self._name)
            )
        # . dataframe
        dtype: cython.int = set_pop(self._dtypes)
        if dtype == 1:
            self._data = self._parse_df(vals)
        else:
            self._data = self._parse_dict(vals)

    @cython.cfunc
    @cython.inline(True)
    def _sort_data(self, data: object) -> list:
        "(cfunc) Sort data values `<list>`."
        res: list = []
        for item in data:
            dtype = type(item)
            if dtype is DataFrame:
                set_add(self._dtypes, 1)
                list_append(res, item)
            elif dtype is dict:
                set_add(self._dtypes, 2)
                list_append(res, item)
            elif set_contains(VALUES_CLAUSE_NEST_SEQU, dtype):
                res += self._sort_data(item)
            else:
                raise errors.QueryValueError(
                    "{}Invalid '{} Data' type: {}. Only support `<DataFrame>` "
                    "or `<dict>`.".format(self._err_pfix(), self._name, dtype)
                )
        return res

    @cython.cfunc
    @cython.inline(True)
    def _parse_df(self, data: list[DataFrame]) -> object:
        "(cfunc) Sort data values from list of DataFrames `<list>`."
        # Concatenate
        if list_len(data) > 1:
            columns: set = set(data[0].columns)
            for i in data[1:]:
                if set(i.columns) != columns:
                    raise errors.QueryValueError(
                        "{}Invalid '{} Data', all DataFrames must have identical "
                        "columns.".format(self._err_pfix(), self._name)
                    )
            try:
                df = concat(data, ignore_index=True)
            except Exception as err:
                raise errors.QueryValueError(
                    "{}Invalid '{} Data', {}".format(self._err_pfix(), self._name, err)
                ) from err
        else:
            df = data[0].copy()

        # Validate
        return self._validate_data(df)

    @cython.cfunc
    @cython.inline(True)
    def _parse_dict(self, data: list[dict]) -> object:
        "(cfunc) Sort data values from list of dictionaries `<list>`."
        # Contruct DataFrame
        try:
            df = DataFrame(data)
        except Exception as err:
            raise errors.QueryValueError(
                "{}Invalid '{} Data', {}".format(self._err_pfix(), self._name, err)
            ) from err

        # Validate
        return self._validate_data(df)

    @cython.cfunc
    @cython.inline(True)
    def _validate_data(self, data: DataFrame) -> object:
        "(cfunc) Validate data `<DataFrame>`."
        # Empty check
        if data.empty:
            raise errors.QueryValueError(
                "{}Invalid '{} Data', cannot be empty.".format(
                    self._err_pfix(), self._name
                )
            )

        # Filter columns
        self._columns = self._tb._columns._filter(tuple(data.columns))
        if not self._columns:
            raise errors.QueryValueError(
                "{}Invalid '{} Data', do not match with any columns from Table '{}'.\n"
                "Given columns: {}\n"
                "Table columns: {}".format(
                    self._err_pfix(),
                    self._name,
                    self._tb._fname,
                    tuple(data.columns),
                    self._tb._columns._names,
                )
            )

        # Exclude primary key
        primary_key = self._tb._columns._primary_key._name
        if primary_key in self._columns:
            self._columns.remove(primary_key)
        self._columns_set = set(self._columns)

        # check Tabletime column
        if self._tb._is_timetable and not set_contains(
            self._columns_set, self._tb._columns._tabletime._name
        ):
            raise errors.QueryValueError(
                "{}Required `tabletime` column '{}' does not exist in {} data. "
                "Table '{}' is a TimeTable, the `tabletime` column is needed to "
                "determine which sub-table(s) the data belongs to.".format(
                    self._err_pfix(),
                    self._tb._columns._tabletime._name,
                    self._name,
                    self._tb._fname,
                )
            )

        # Filter column
        data = data[self._columns].copy()

        # Validate column values
        for col in self._columns:
            # . validate values
            try:
                data[col] = self._tb._columns._series_validators[col](data[col])
            except Exception as err:
                raise errors.QueryValueError(
                    "{}Invalid '{} Data' for column '{}': {}".format(
                        self._err_pfix(), self._name, col, err
                    )
                ) from err
        return data

    # Utils --------------------------------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def _err_pfix(self) -> str:
        "(cfunc) Error prefix `<str>`."
        return "<%s> [%s] %s Data\nError: " % (
            self._tb._fname,
            self._query._name,
            self._name,
        )

    @cython.cfunc
    @cython.inline(True)
    def _parse_timetable_names(self, tb: TimeTable, times: Series) -> object:
        "(cfunc) Parse TimeTable names `<Series[str]>`."
        return tb._get_names_series(times)

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def _access_timetable_fmt(self, tb: TimeTable) -> str:
        "(cfunc) Access TimeTable format."
        return tb._time_format

    # Garbage ------------------------------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def _collect_garbage(self):
        # Alread collected
        if self._query is None:
            return None  # exit

        # Collect
        self._query = None
        self._tb = None
        self._dtypes = None
        self._columns = None
        self._columns_set = None
        self._data = None

    # Special methods -----------------------------------------------------------------------------
    def __repr__(self) -> str:
        return "<COMPARE_DATA (name='%s', table='%s')>" % (self._name, self._tb._fname)

    def __del__(self):
        self._collect_garbage()


class COMPARE_RESULTS:
    """The comparision results of the `CompareQuery`.

    Access the results through the following properties:
    - `insert`: The data that only exist in Source Data, but not in Baseline Data.
       The comparison is based on the 'unique_columns'.
    - `update`: The data that exist in both Source Data and Baseline Data, but
       with different values based on the 'compare_columns'.
    - `delete`: The data that only exist in Baseline Data, but not in Source Data.
       The comparison is based on the 'unique_columns'.
    - `common`: The data that exist in both Source Data and Baseline Data, regardless
      the values of the 'compare_columns'.
    - `identical`: The data that exist in both Source Data and Baseline Data, and
      with identical values based on the 'compare_columns'.

    The data type of the results can be changed through `set_dtype()` method.
    """

    def __init__(self, dtype: Literal["df", "dict"] = "df") -> None:
        # Data type
        self.set_dtype(dtype)
        # Results
        self._insert: DataFrame = None
        self._update: DataFrame = None
        self._delete: DataFrame = None
        self._common: DataFrame = None
        self._identical: DataFrame = None

    # Properties ---------------------------------------------------------------------------------
    @property
    def insert(self) -> Union[DataFrame, list[dict]]:
        """The data that only exist in Source Data, but not in Baseline Data.
        The comparison is based on the 'unique_columns'.

        This data will be inserted to the table when calling `execute()`.

        :return `<DataFrame/list[dict]>`: Data type can be changed through `set_dtype()` method.
        """
        return self._convert_dtype(self._insert)

    @property
    def update(self) -> Union[DataFrame, list[dict]]:
        """The data that exist in both Source Data and Baseline Data,
        but with different values based on the 'compare_columns'.

        This data will be used to update the table when calling `execute()`.

        :return `<DataFrame/list[dict]>`: Data type can be changed through `set_dtype()` method.
        """
        return self._convert_dtype(self._update)

    @property
    def delete(self) -> Union[DataFrame, list[dict]]:
        """The data that only exist in Baseline Data, but not in Source Data.
        The comparison is based on the 'unique_columns'.

        This data will be used to delete the table when calling `execute()`.

        :return `<DataFrame/list[dict]>`: Data type can be changed through `set_dtype()` method.
        """
        return self._convert_dtype(self._delete)

    @property
    def common(self) -> Union[DataFrame, list[dict]]:
        """The data that exist in both Source Data and Baseline Data,
        regardless the values of the 'compare_columns'.

        This data does not affect the query execution.

        :return `<DataFrame/list[dict]>`: Data type can be changed through `set_dtype()` method.
        """
        return self._convert_dtype(self._common)

    @property
    def identical(self) -> Union[DataFrame, list[dict]]:
        """The data that exist in both Source Data and Baseline Data, and
        with identical values based on the 'compare_columns'.

        This data does not affect the query execution.

        :return `<DataFrame/list[dict]>`: Data type can be changed through `set_dtype()` method.
        """
        return self._convert_dtype(self._identical)

    # Data type ----------------------------------------------------------------------------------
    def set_dtype(self, dtype: Literal["df", "dict"]) -> Self:
        """Set the data type of the comparison results.

        :param dtype: `<str>` The data type of the comparison results.
            - `'df'`: The comparison results will be returned as `<DataFrame>`.
            - `'dict'`: The comparison results will be returned as `<list[dict]>`.
        """
        if dtype == "dict":
            self._dtype = "dict"
        else:
            self._dtype = "df"

    # Utils --------------------------------------------------------------------------------------
    def _convert_dtype(self, data: DataFrame) -> object:
        "(cfunc) Convert results data type `<DataFrame/list[dict]>`."
        if data is None:
            if self._dtype == "dict":
                return []
            else:
                return DataFrame()
        else:
            if self._dtype == "dict":
                return data.to_dict(orient="records")
            else:
                return data.copy()

    # Garbage ------------------------------------------------------------------------------------
    def _collect_garbage(self):
        self._insert = None
        self._update = None
        self._delete = None
        self._common = None
        self._identical = None

    # Special methods -----------------------------------------------------------------------------
    def __repr__(self) -> str:
        reprs: list = [" Comparison Results ".center(100, "-")]
        data: dict = {
            "Insert Data": self._insert,
            "Update Data": self._update,
            "Delete Data": self._delete,
            "Common Data": self._common,
            "Identical Data": self._identical,
        }
        count: cython.int = 0
        for key, val in dict_items(data):
            if val is None or val.empty:
                continue
            list_append(reprs, "%s:\n%s\n" % (key, val))
            count += 1
        if count == 0:
            list_append(reprs, "* No comparison results.")
        list_append(reprs, "-" * 100)
        return "\n".join(reprs)

    def __bool__(self) -> bool:
        return any(
            i is not None
            for i in [
                self._insert,
                self._update,
                self._delete,
                self._common,
                self._identical,
            ]
        )

    def __del__(self):
        self._collect_garbage()


@cython.cclass
class CompareQuery:
    """The `COMPARE` query of the Table."""

    # Query
    _name: str
    _tb: Table
    _db: Database
    # Data
    _src: COMPARE_DATA
    _bsl: COMPARE_DATA
    _src_common: DataFrame
    _bsl_common: DataFrame
    # Columns
    _unique_columns: list[str]
    _ops_unique: list[str]
    _compare_columns: list[str]
    _ops_compare: list[str]
    # Data
    _insert_data: DataFrame
    _update_data: DataFrame
    _delete_data: DataFrame
    _common_data: DataFrame
    _identical_data: DataFrame
    # Operations
    _operations_ready: cython.bint
    _operations: set[int]
    """
    - 1: find insert data (based on unique columns)
    - 2: find update data (based on compare columns)
    - 3: find delete data (based on unique columns)
    - 4: find common data (based on unique columns)
    - 5: find identical data (based on compare columns)
    """
    _insert_stmt: str
    _insert_values_normtable: list[list]
    _insert_values_timetable: dict[str, list[list]]
    _update_stmt: str
    _update_values_normtable: list[list]
    _update_values_timetable: dict[str, list[list]]
    _delete_stmt: str
    _delete_values_normtable: list[list]
    _delete_values_timetable: dict[str, list[list]]
    _ignore: cython.bint
    _timetable_phl: str
    _drop_columns: list[str]
    # Execute
    _semaphore: Semaphore
    _concurrency: cython.int
    _cursor: type[Cursor]
    _timeout: object
    _warnings: cython.bint
    _retry_on_error: cython.bint
    _retry_times: cython.int
    _min_wait_time: cython.double
    _max_wait_time: cython.double
    _insert_rows: cython.longlong
    _update_rows: cython.longlong
    _delete_rows: cython.longlong
    _affected_rows: cython.longlong
    # Stats
    _start_time: cython.double

    def __init__(self, table: Table) -> None:
        # Query
        self._name = "Compare Query"
        self._tb = table
        self._db = self._tb._db
        # Data
        self._src = None
        self._bsl = None
        self._src_common = None
        self._bsl_common = None
        # Columns
        self._unique_columns = None
        self._ops_unique = None
        self._compare_columns = None
        self._ops_compare = None
        # Data
        self._insert_data = None
        self._update_data = None
        self._delete_data = None
        self._common_data = None
        self._identical_data = None
        # Operations
        self._operations_ready = False
        self._operations = set()
        self._insert_stmt = None
        self._insert_values_normtable = None
        self._insert_values_timetable = None
        self._update_stmt = None
        self._update_values_normtable = None
        self._update_values_timetable = None
        self._delete_stmt = None
        self._delete_values_normtable = None
        self._delete_values_timetable = None
        self._ignore = False
        self._timetable_phl = None
        self._drop_columns = [UNIQUE_COLUMN, COMPARE_COLUMN]
        # Execute
        self._semaphore = None
        self._concurrency = 10
        self._cursor = None
        self._timeout = None
        self._warnings = True
        self._retry_on_error = False
        self._retry_times = -1
        self._min_wait_time = 0.2
        self._max_wait_time = 2.0
        self._insert_rows = 0
        self._update_rows = 0
        self._delete_rows = 0
        self._affected_rows = 0
        # Stats
        self._start_time = unixtime()

    # Query methods ------------------------------------------------------------------------------
    def compare(
        self,
        src_data: Union[DataFrame, list[dict], tuple[dict]],
        bsl_data: Union[DataFrame, list[dict], tuple[dict]],
        unique_columns: Union[list[str | Column], tuple[str | Column]] = None,
        compare_columns: Union[list[str | Column], tuple[str | Column]] = None,
    ) -> Self:
        """The `COMPARE` clause of the query.

        The `CompareQuery` fulfills two purposes:
        - 1. Find out the differences and similarities between the source and
             the baseline data. By calling `results()` method at the end of the
             chain, an instance of `<COMPARE_RESULTS>` will be returned, which
             contains all the comparison information.
        - 2. Modify the table to match the source data. By providing the correct
             baseline data (what the table currently look like) and calling the
             `execute()` method at the end of the chain, the table will be modified
             to match the source data through a combination of `INSERT`, `UPDATE`
             and `DELETE` queries.
        - *. For more information about the data comparison, and how the arguments
             in this method affects the query, please refer to the `operations`
             method, which should always be called after the `compare()` method.

        :param src_data: The source data to compare with the baseline data.
            - This data should be the representation of the final form of
              the table data. In other words, it should be what the table
              look like after the query execution.
            - Data will be cleaned automatically based on the table's column
              schema before comparison and only columns that belongs to the
              table will be kept.
            - Accepts: `<DataFrame>`, `<list[dict]>`, `<tuple[dict]>`.

        :param bsl_data: The baseline data to compare to.
            - This data should be the present state of the table data. In
              other words, this data should be what the table currently
              look like before the query execution.
            - Data will be cleaned automatically based on the table's column
              schema before comparison and only columns that belongs to the
              table will be kept.
            - Accepts: `<DataFrame>`, `<list[dict]>`, `<tuple[dict]>`.

        :param unique_columns: `<list[str/Column]>` The columns to determine the uniqueness of the data. Defaults to `None`.
            - The 'unique_columns' sets a column combination as an unique
              identifier for each row of the data.
            - If not provided (`None`), defaults to the the columns of the `primary`
              UNIQUE INDEX from the table if applicable, else `QueryValueError` will
              be raised.

        :param compare_columns: `<list[str/Column]>` The columns to determine the differences in data values. Defaults to `None`.
            - The 'compare_columns' sets which columns should be used to
              identify the differences in value between the source and the
              baseline data.
            - If not provided (`None`), defaults to all shared columns between
              the source and the baseline data, columns that belongs to the table
              while excluding the 'unique_columns'.

        ### Notice
        Comparision cannot be based on the PARIMARY KEY of the table, and for data
        containing the PRIMARY KEY, the PRIMARY KEY column will be automatically
        excluded at data cleaning process.

        ### Example (find differences and similarities)
        >>> src = DataFrame(...)
            # . the source data (table final form)
        >>> bsl = await db.user.select().where(...).execute()
            # . select the baseline data (table current state)
            results = (
                await db.user.compare(src, bsl,
                    unique_columns=["user_name"],
                    compare_columns=["user_age", "user_status"])
                .operations(
                    find_insert=True,
                    find_update=True,
                    find_delete=True,
                    find_common=True,
                    find_identical=True)
                .results()
            )
            # . returns the comparison results
            print(results)

        ### Example (modify table)
        >>> src = DataFrame(...)
            # . the source data (table final form)
        >>> async with db.transaction() as conn:
                bsl = await db.user.select().where(...).for_update().execute(conn)
                # . select & lock the baseline data (table current state)
                (
                    await db.user.compare(src, bsl,
                        unique_columns=["user_name"],
                        compare_columns=["user_age", "user_status"])
                    .operations(
                        find_insert=True,
                        find_update=True,
                        find_delete=True)
                    .execute(conn)
                )
                # . modify the table to match the source data
        """
        self._validate_comparision(src_data, bsl_data, unique_columns, compare_columns)
        return self

    def operations(
        self,
        find_insert: bool = True,
        find_update: bool = True,
        find_delete: bool = True,
        find_common: bool = False,
        find_identical: bool = False,
    ) -> Self:
        """The `OPERATION` clause of the query. Determines how and what to compare
        between the source and the baseline data.

        :param find_insert: `<bool>` Find the data that only exist in Source Data, but not in Baseline Data. Defaults to `True`.
            - Insert data is determined based on the 'unique_columns'.
            - If `True` and call `execute()` method at the end, unique data in the Source
              Data will be inserted into the table.

        :param find_update: `<bool>` Find the data that exist in both Source Data and Baseline Data, but with different values. Defaults to `True`.
            - Update data is determined based on both the 'unique_columns' and the 'compare_columns'.
            - If `True` and call `execute()` method at the end, the different data in Source
              Data will be used to update the table, where columns in the 'compare_columns'
              determines the SET clause, and columns in the 'unique_columns' determines the
              WHERE clause.

        :param find_delete: `<bool>` Find the data that only exist in Baseline Data, but not in Source Data. Defaults to `True`.
            - Delete data is determined based on the 'unique_columns'.
            - If `True` and call `execute()` method at the end, extra data in the Baseline
              Data will be used to delete the table, where columns in the 'unique_columns'
              determines the WHERE clause.

        :param find_common: `<bool>` Find the data that exist in both Source Data and Baseline Data, regardless the values. Defaults to `False`.
            - Common data is determined based on the 'unique_columns'.
            - This argument does not affect the query execution through `execute()` method.

        :param find_identical: `<bool>` Find the data that exist in both Source Data and Baseline Data, and with identical values. Defaults to `False`.
            - Identical data is determined based on both the 'unique_columns' and the 'compare_columns'.
            - This argument does not affect the query execution through `execute()` method.

        ### Notice
        When calling `results()` method at the end instead of `execute()`, only the
        operations that are set to `True` will be performed and accessible through
        the returned `<COMPARE_RESULTS>` instance.
        """
        # Set operations
        if find_insert:
            set_add(self._operations, 1)
        if find_update:
            set_add(self._operations, 2)
        if find_delete:
            set_add(self._operations, 3)
        if find_common:
            set_add(self._operations, 4)
        if find_identical:
            set_add(self._operations, 5)

        # Validate operations
        if not self._operations:
            raise errors.QueryValueError(
                "{}Invalid 'operations': at least one compare "
                "operations must be `True`.".format(self._err_pfix())
            )
        self._operations_ready = True
        return self

    # Validate -----------------------------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    def _validate_comparision(
        self,
        src_data: object,
        bsl_data: object,
        unique_columns: object,
        compare_columns: object,
    ):
        "(cfunc) Validate comparision arguments."
        # Set data
        self._src = COMPARE_DATA(self, "Source", src_data)
        self._bsl = COMPARE_DATA(self, "Baseline", bsl_data)

        # Common data columns
        common: set = self._src._columns_set & self._bsl._columns_set
        if not common:
            raise errors.QueryValueError(
                "{}Invalid 'src_data' & 'bsl_data', there is "
                "no common columns between two datasets.\n"
                "-> Source..: {}\n"
                "-> Baseline: {}".format(
                    self._err_pfix(),
                    self._src._columns,
                    self._bsl._columns,
                )
            )

        # Unique columns
        columns: list
        if unique_columns is None:
            uqidx = self._tb._indexes._uniques._primary
            if not uqidx:
                raise errors.QueryValueError(
                    "{}For Table '{}', which does not have a 'primary' UNIQUE INDEX, "
                    "it is required to manually specify the 'unique_columns' of the "
                    "data. Otherwise, it is unclear how to perform the comparison.".format(
                        self._err_pfix(), self._tb._fname
                    )
                )
            columns = list(uqidx._columns._names)
        else:
            columns = self._parse_column_names(unique_columns)
            if not columns:
                raise errors.QueryValueError(
                    "{}Invalid 'unique_columns', cannot be "
                    "empty.".format(self._err_pfix())
                )
        for col in columns:
            if not set_contains(common, col):
                raise errors.QueryValueError(
                    "{}Invalid 'unique_columns', all columns must "
                    "exist in both 'src_data' & 'bsl_data'.\n"
                    "-> Except column.: ['{}']\n"
                    "-> Common columns: {}".format(
                        self._err_pfix(),
                        col,
                        self._tb._columns._filter(tuple(common)),
                    )
                )
        self._unique_columns = self._tb._columns._filter(tuple(columns))
        unique_set: set = set(columns)

        # Compare columns
        columns = []
        if compare_columns is None:
            for col in common:
                if not set_contains(unique_set, col):
                    list_append(columns, col)
        else:
            for col in self._parse_column_names(compare_columns):
                if not set_contains(common, col):
                    raise errors.QueryValueError(
                        "{}Invalid 'compare_columns', all columns must "
                        "exist in both 'src_data' & 'bsl_data'.\n"
                        "-> Except column.: ['{}']\n"
                        "-> Common columns: {}".format(
                            self._err_pfix(),
                            col,
                            self._tb._columns._filter(tuple(common)),
                        )
                    )
                if not set_contains(unique_set, col):
                    list_append(columns, col)
        if not columns:
            raise errors.QueryValueError(
                "{}Invalid 'compare_columns', after excluding "
                "'unique_columns', there is no column to compare.\n"
                "-> Unique: {}\n"
                "-> Common: {}".format(
                    self._err_pfix(),
                    self._unique_columns,
                    self._tb._columns._filter(tuple(common)),
                )
            )
        self._compare_columns = self._tb._columns._filter(tuple(columns))

        # Operation columns
        self._ops_unique = self._unique_columns.copy()
        self._ops_compare = self._compare_columns + self._unique_columns
        if self._tb._is_timetable:
            list_append(self._ops_unique, SUBTABLE_COLUMN)
            list_append(self._ops_compare, SUBTABLE_COLUMN)

    # Comparison ---------------------------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    def _comparison(self):
        "(cfunc) Perform data comparison."
        # Validate operations
        if not self._operations_ready:
            raise errors.QueryValueError(
                "{}Method `operations()` has not been called yet.".format(
                    self._err_pfix()
                )
            )

        # Find insert
        if set_contains(self._operations, 1):
            self._insert_data = self._find_insert()
        # Find update
        if set_contains(self._operations, 2):
            self._update_data = self._find_update()
        # Find delete
        if set_contains(self._operations, 3):
            self._delete_data = self._find_delete()
        # Find common
        if set_contains(self._operations, 4):
            self._common_data = self._find_common()
        # Find identical
        if set_contains(self._operations, 5):
            self._identical_data = self._find_identical()

        # Collect garbage
        self._src._collect_garbage()
        self._bsl._collect_garbage()
        self._src_common = None
        self._bsl_common = None

    def _get_src_common(self) -> DataFrame:
        """(Internal) Get the data in Source Data that also exist
        in Baseline Data `<DataFrame>.
        """
        if self._src_common is not None:
            return self._src_common
        else:
            src = self._src.data
            bsl = self._bsl.data
            self._src_common = src[src[UNIQUE_COLUMN].isin(bsl[UNIQUE_COLUMN])].copy()
            return self._src_common

    def _get_bsl_common(self) -> DataFrame:
        """(Internal) Get the data in Baseline Data that also exist
        in Source Data `<DataFrame>.
        """
        if self._bsl_common is not None:
            return self._bsl_common
        else:
            src = self._src.data
            bsl = self._bsl.data
            self._bsl_common = bsl[bsl[UNIQUE_COLUMN].isin(src[UNIQUE_COLUMN])].copy()
            return self._bsl_common

    def _find_insert(self) -> DataFrame:
        """(Internal) Find the data that only exist in Source Data,
        but not in Baseline Data `<DataFrame>`.
        """
        src = self._src.data
        bsl = self._bsl.data
        return (
            src[~src[UNIQUE_COLUMN].isin(bsl[UNIQUE_COLUMN])]
            .copy()
            .drop(columns=self._drop_columns, errors="ignroe")
        )

    def _find_update(self) -> DataFrame:
        """(Internal) Find the data that exist in both Source Data
        and Baseline Data, but with different values `<DataFrame>`.
        """
        src_common = self._get_src_common()
        bsl_common = self._get_bsl_common()
        return (
            src_common[~src_common[COMPARE_COLUMN].isin(bsl_common[COMPARE_COLUMN])]
            .copy()
            .drop(columns=self._drop_columns, errors="ignroe")
        )

    def _find_delete(self) -> DataFrame:
        """(Internal) Find the data that only exist in Baseline Data,
        but not in Source Data `<DataFrame>`.
        """
        src = self._src.data
        bsl = self._bsl.data
        return (
            bsl[~bsl[UNIQUE_COLUMN].isin(src[UNIQUE_COLUMN])]
            .copy()
            .drop(columns=self._drop_columns, errors="ignroe")
        )

    def _find_common(self) -> DataFrame:
        """(Internal) Find the data that exist in both Source Data
        and Baseline Data, regardless the values `<DataFrame>`.
        """
        return (
            self._get_src_common()
            .copy()
            .drop(columns=self._drop_columns, errors="ignroe")
        )

    def _find_identical(self) -> DataFrame:
        """(Internal) Find the data that exist in both Source Data
        and Baseline Data, and with identical values `<DataFrame>`.
        """
        src_common = self._get_src_common()
        bsl_common = self._get_bsl_common()
        return (
            src_common[src_common[COMPARE_COLUMN].isin(bsl_common[COMPARE_COLUMN])]
            .copy()
            .drop(columns=self._drop_columns, errors="ignroe")
        )

    # Construct ----------------------------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def _get_timetable_phl(self) -> str:
        "(cfunc) Get the placeholder for the timetable (once called, the value is immutable)."
        if self._timetable_phl is None:
            self._timetable_phl = self._tb.gen_pname()
        return self._timetable_phl

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def _construct_insert(self):
        # Validate operations
        if self._insert_data is None or self._insert_data.empty:
            self._insert_stmt = None
            return None  # exit

        # TimeTable
        if self._tb._is_timetable:
            # . determine table name
            tb_name: str = self._db._name + "." + self._get_timetable_phl()
            # . construct values
            res_dict: dict = {}
            res_gp: list
            for name, gp in self._insert_data.groupby(SUBTABLE_COLUMN, as_index=False):
                res_gp = []
                for row in gp.drop(columns=SUBTABLE_COLUMN).values:
                    list_append(res_gp, list(row))
                dict_setitem(res_dict, name, res_gp)
            self._insert_values_timetable = res_dict
            # . Insert columns
            columns = list(self._insert_data.columns)
            columns.remove(SUBTABLE_COLUMN)

        # Normal Table
        else:
            # . determine table name
            tb_name: str = self._tb._fname
            # . construct values
            res_list: list = []
            for row in self._insert_data.values:
                list_append(res_list, list(row))
            self._insert_values_normtable = res_list
            # . Insert columns
            columns = list(self._insert_data.columns)

        # Construct statement
        self._insert_stmt = "INSERT%s INTO %s (\n\t%s\n)\nVALUES (%s);" % (
            " IGNROE" if self._ignore else "",
            tb_name,
            ",\n\t".join(columns),
            ", ".join(["%s"] * list_len(columns)),
        )

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def _construct_update(self):
        # Validate operations
        if self._update_data is None or self._update_data.empty:
            self._update_stmt = None
            return None  # exit

        # TimeTable
        self._update_data = self._update_data[self._ops_compare]

        if self._tb._is_timetable:
            # . determine table name
            tb_name: str = self._db._name + "." + self._get_timetable_phl()
            # . construct values
            res_dict: dict = {}
            res_gp: list
            for name, gp in self._update_data.groupby(SUBTABLE_COLUMN, as_index=False):
                res_gp = []
                for row in gp.drop(columns=SUBTABLE_COLUMN).values:
                    list_append(res_gp, list(row))
                dict_setitem(res_dict, name, res_gp)
            self._update_values_timetable = res_dict
        else:
            # . determine table name
            tb_name: str = self._tb._fname
            # . construct values
            res_list: list = []
            for row in self._update_data.values:
                list_append(res_list, list(row))
            self._update_values_normtable = res_list

        # Construct statement
        set_syntax: list = []
        for col in self._compare_columns:
            list_append(set_syntax, col + " = %s")
        where_syntax: list = []
        for col in self._unique_columns:
            list_append(where_syntax, col + " = %s")
        self._update_stmt = "UPDATE%s %s\nSET\n\t%s\nWHERE %s;" % (
            " IGNROE" if self._ignore else "",
            tb_name,
            ",\n\t".join(set_syntax),
            "\n\tAND ".join(where_syntax),
        )

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def _construct_delete(self):
        # Validate operations
        if self._delete_data is None or self._delete_data.empty:
            self._delete_stmt = None
            return None  # exit

        # TimeTable
        self._delete_data = self._delete_data[self._ops_unique]

        if self._tb._is_timetable:
            # . determine table name
            tb_name: str = self._db._name + "." + self._get_timetable_phl()
            # . construct values
            res_dict: dict = {}
            res_gp: list
            for name, gp in self._delete_data.groupby(SUBTABLE_COLUMN, as_index=False):
                res_gp = []
                for row in gp.drop(columns=SUBTABLE_COLUMN).values:
                    list_append(res_gp, list(row))
                dict_setitem(res_dict, name, res_gp)
            self._delete_values_timetable = res_dict

        # Normal Table
        else:
            # . determine table name
            tb_name: str = self._tb._fname
            # . construct values
            res_list: list = []
            for row in self._delete_data.values:
                list_append(res_list, list(row))
            self._delete_values_normtable = res_list

        # Construct statement
        where_syntax: list = []
        for col in self._unique_columns:
            list_append(where_syntax, col + " = %s")
        self._delete_stmt = "DELETE%s FROM %s\nWHERE %s;" % (
            " IGNROE" if self._ignore else "",
            tb_name,
            "\n\tAND ".join(where_syntax),
        )

    # Utils --------------------------------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def _err_pfix(self) -> str:
        "(cfunc) Error prefix `<str>`."
        return "<%s> [%s]\nError: " % (self._tb._fname, self._name)

    @cython.cfunc
    @cython.inline(True)
    def _parse_column_names(self, columns: object) -> list:
        "(cfunc) Parse column names `<list[str]>`."
        # `<str>` column
        if is_str(columns):
            return [columns]
        # `<Column>` instance
        elif isinstance(columns, Column):
            return [access_column_name(columns)]
        # Sequence of columns
        else:
            return self._parse_sequ_of_column_names(columns)

    @cython.cfunc
    @cython.inline(True)
    def _parse_sequ_of_column_names(self, columns: object) -> list:
        "(cfunc) Parse sequence of column names `<list[str]>`."
        try:
            res: list = []
            for col in columns:
                if is_str(col):
                    list_append(res, col)
                elif isinstance(col, Column):
                    list_append(res, access_column_name(col))
                elif is_list(col) or is_tuple(col):
                    res += self._parse_sequ_of_column_names(col)
                else:
                    raise errors.QueryValueError(
                        "{}Invalid column {}, only accepts column name `<str>` or "
                        "instance of `<Column>`.".format(self._err_pfix(), repr(col))
                    )
        except errors.QueryValueError:
            raise
        except Exception as err:
            raise errors.QueryValueError(
                "{}Invalid 'columns': {}".format(self._err_pfix(), err)
            ) from err
        return res

    # Results ------------------------------------------------------------------------------------
    async def results(self, dtype: Literal["df", "dict"] = "df") -> COMPARE_RESULTS:
        """Perform the comparison and return the compare results.

        :param dtype: `<str>` The data type of the compare results. Defaults to `'df'`.
            - `'df'`: Access the compare results as DataFrames.
            - `'dict'`: Access the compare results as list of dicts.

        :returns: `<COMPARE_RESULTS>` The compare results.
        """
        try:
            # Comparison
            self._comparison()
            # Set data type
            results = COMPARE_RESULTS(dtype)
            results._insert = self._insert_data.drop(
                columns=SUBTABLE_COLUMN, errors="ignore"
            )
            results._update = self._update_data.drop(
                columns=SUBTABLE_COLUMN, errors="ignore"
            )
            results._delete = self._delete_data.drop(
                columns=SUBTABLE_COLUMN, errors="ignore"
            )
            results._common = self._common_data.drop(
                columns=SUBTABLE_COLUMN, errors="ignore"
            )
            results._identical = self._identical_data.drop(
                columns=SUBTABLE_COLUMN, errors="ignore"
            )
            # Return results
            return results

        finally:
            self._collect_garbage()

    # Execute ------------------------------------------------------------------------------------
    async def execute(
        self,
        conn: Union[Connection, None] = None,
        concurrency: int = 10,
        ignore: bool = False,
        cursor: type[Cursor | SSCursor] = Cursor,
        timeout: Union[int, None] = None,
        warnings: bool = True,
        *,
        retry_on_error: bool = True,
        retry_times: int = -1,
        min_wait_time: float = 0.2,
        max_wait_time: float = 2,
        stats: bool = False,
    ) -> int:
        """Perform the comparison and execute a combination of `INSERT`,
        `UPDATE` and `DELETE` queries on the table based on the compare
        results.

        :param conn: `<Connection>` The connection to execute the query. Defaults to `None`.
            - If `None`, query will be executed by temporary connections
              acquired from the Server pool.
            - If specified, query will be executed by the given connection.

        :param concurrency: `<int>` The maximum number of concurrent executions. Defaults to `10`.
            - * Notice: This argument is only applicable when `conn=None`. If 'conn'
              is specified, all queries will be executed sequentially by the given
              connection.

        :param ignore: `<bool>` Whether to ignore the duplicate key errors. Defaults to `False`.
            - If `True`, all INSERT, UPDATE and DELETE statements will use the `'IGNROE'`
              keyword, which will ignore the duplicate key errors and only issue warnings.
            - If `False`, duplicate key error will be raised immediately.

        :param cursor: `<type[Cursor/SSCursor]>` The `Cursor` class to use for query execution. Defaults to `Cursor`.

        :param timeout: `<int>` Query execution timeout in seconds. Dafaults to `None`.
            - If set to `None` or `0`, `tables.server.query_timeout` will be used
              as the default timeout.
            - `SQLQueryTimeoutError` will be raised when the timeout is reached.

        :param warnings: `<bool>` Whether to issue any SQL related warnings. Defaults to `True`.

        :param retry_on_error: `<bool>` Whether to retry when non-critial SQL error occurs. Defaults to `True`.
            - If `True`, when a non-critical SQL error occurs (such as `connection timeout`,
              `connection lost`, etc.), the query will be retried.
            - If `False`, errors will be raised immediately.

        :param retry_times: `<int>` The maximum number of retries. Defaults to `-1`.
            - * Notice: This argument is only applicable when `retry_on_error=True`.
            - For `retry_times <= 0`, the query retries indefinitely until success.
            - For `retry_times > 0`, the query retries up to the given 'retry_times'.

        :param min_wait_time: `<float>` The minimum wait time in seconds between each retries. Defaults to `0.2`.
            - * Notice: This argument is only applicable when `retry_on_error=True`.

        :param max_wait_time: `<float>` The maximum wait time in seconds between each retries. Defaults to `2`.
            - * Notice: This argument is only applicable when `retry_on_error=True`.

        :param stats: `<bool>` Whether to print the query execution stats to the console. Defaults to `False`.
        :raises: Subclass of `QueryError`.
        :return `<int>`: The total number of rows affected by the all the quries.
        """
        # Set parameters
        self._concurrency = concurrency
        self._ignore = ignore
        self._cursor = cursor
        self._timeout = timeout
        self._warnings = warnings
        self._retry_on_error = retry_on_error
        self._retry_times = retry_times
        self._min_wait_time = min_wait_time
        self._max_wait_time = max_wait_time

        # Collect garbage
        self._common_data = None
        self._identical_data = None

        try:
            # Comparison
            self._comparison()

            # Construct operations
            self._construct_insert()
            self._construct_update()
            self._construct_delete()

            # Execute on specified connection
            if isinstance(conn, Connection):
                self._affected_rows = await self._execute_on_spec_conn(conn)

            # Execute on temporary connection(s)
            else:
                self._affected_rows = await self._execute_on_temp_conn()

            # Return affected rows
            return self._affected_rows

        finally:
            self._statistics(stats)
            self._collect_garbage()

    async def _execute_on_temp_conn(self) -> int:
        """Execute query on temporary connection(s)."""
        # Determine execute function
        if self._retry_on_error:
            func = self._exe_with_retry
        else:
            func = self._exe_nill_retry

        # Setup tasks
        self._setup_semaphore()
        delete_tasks: list = []
        update_tasks: list = []
        insert_tasks: list = []

        # TimeTable
        if self._tb._is_timetable:
            # . timetable placeholder
            key: str = self._timetable_phl
            # . delete operation
            if self._delete_stmt is not None:
                stmt = self._delete_stmt
                delete_tasks = [
                    self._call_sem(
                        func, uni_replace(stmt, key, tb, -1), arg, None, False
                    )
                    for tb, args in dict_items(self._delete_values_timetable)
                    for arg in args
                ]
            # . update operation
            if self._update_stmt is not None:
                stmt = self._update_stmt
                update_tasks = [
                    self._call_sem(
                        func, uni_replace(stmt, key, tb, -1), arg, None, False
                    )
                    for tb, args in dict_items(self._update_values_timetable)
                    for arg in args
                ]
            # . insert operation
            if self._insert_stmt is not None:
                stmt = self._insert_stmt
                insert_tasks = [
                    self._call_sem(
                        func, uni_replace(stmt, key, tb, -1), args, None, True
                    )
                    for tb, args in dict_items(self._insert_values_timetable)
                ]

        # Normal Table
        else:
            # . delete operation
            if self._delete_stmt is not None:
                stmt = self._delete_stmt
                delete_tasks = [
                    self._call_sem(func, stmt, arg, None, False)
                    for arg in self._delete_values_normtable
                ]
            # . update operation
            if self._update_stmt is not None:
                stmt = self._update_stmt
                update_tasks = [
                    self._call_sem(func, stmt, arg, None, False)
                    for arg in self._update_values_normtable
                ]
            # . insert operation
            if self._insert_stmt is not None:
                stmt = self._insert_stmt
                args = self._insert_values_normtable
                insert_tasks = [self._call_sem(func, stmt, args, None, True)]

        # Execute tasks
        res = await gather(*delete_tasks)
        self._delete_rows = sum(res) if res else 0
        res = await gather(*update_tasks)
        self._update_rows = sum(res) if res else 0
        res = await gather(*insert_tasks)
        self._insert_rows = sum(res) if res else 0

        # Return affected rows
        return self._delete_rows + self._update_rows + self._insert_rows

    async def _execute_on_spec_conn(self, conn: Connection) -> int:
        """Execute the query on specified connection."""
        # Determine execute function
        func = self._exe_nill_retry

        # Setup
        delete_rows: int = 0
        update_rows: int = 0
        insert_rows: int = 0

        # TimeTable
        if self._tb._is_timetable:
            # . timetable placeholder
            key: str = self._timetable_phl
            # . delete operation
            if self._delete_stmt is not None:
                stmt = self._delete_stmt
                for tb, args in dict_items(self._delete_values_timetable):
                    for arg in args:
                        delete_rows += await func(
                            uni_replace(stmt, key, tb, -1), arg, conn, False
                        )
            # . update operation
            if self._update_stmt is not None:
                stmt = self._update_stmt
                for tb, args in dict_items(self._update_values_timetable):
                    for arg in args:
                        update_rows += await func(
                            uni_replace(stmt, key, tb, -1), arg, conn, False
                        )
            # . insert operation
            if self._insert_stmt is not None:
                stmt = self._insert_stmt
                for tb, args in dict_items(self._insert_values_timetable):
                    insert_rows += await func(
                        uni_replace(stmt, key, tb, -1), args, conn, True
                    )

        # Normal Table
        else:
            # . delete operation
            if self._delete_stmt is not None:
                stmt = self._delete_stmt
                for arg in self._delete_values_normtable:
                    delete_rows += await func(stmt, arg, conn, False)
            # . update operation
            if self._update_stmt is not None:
                stmt = self._update_stmt
                for arg in self._update_values_normtable:
                    update_rows += await func(stmt, arg, conn, False)
            # . insert operation
            if self._insert_stmt is not None:
                stmt = self._insert_stmt
                args = self._insert_values_normtable
                insert_rows += await func(stmt, args, conn, True)

        # Record affected rows
        self._delete_rows = delete_rows
        self._update_rows = update_rows
        self._insert_rows = insert_rows

        # Return affected rows
        return self._delete_rows + self._update_rows + self._insert_rows

    async def _exe_with_retry(
        self,
        stmt: str,
        args: object,
        conn: object,
        resolve_absent_table: bool,
    ) -> int:
        "(Internal use only) Execute with retry on non-critical SQL errors."
        while True:
            try:
                return await query_exc_handler(
                    retry_times=self._retry_times,
                    min_wait_time=self._min_wait_time,
                    max_wait_time=self._max_wait_time,
                )(self._db.execute_query)(
                    stmt,
                    args=args,
                    conn=conn,
                    reusable=True,
                    cursor=self._cursor,
                    timeout=self._timeout,
                    warnings=self._warnings,
                    resolve_absent_table=resolve_absent_table,
                )
            except errors.QueryTableAbsentError:
                pass

    async def _exe_nill_retry(
        self,
        stmt: str,
        args: object,
        conn: object,
        resolve_absent_table: bool,
    ) -> int:
        "(Internal use only) Execute without retry on non-critical SQL errors."
        while True:
            try:
                return await self._db.execute_query(
                    stmt,
                    args=args,
                    conn=conn,
                    reusable=True,
                    cursor=self._cursor,
                    timeout=self._timeout,
                    warnings=self._warnings,
                    resolve_absent_table=resolve_absent_table,
                )
            except errors.QueryTableAbsentError:
                pass

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def _setup_semaphore(self):
        """(Internal) Setup semaphore."""
        if self._concurrency <= 0:
            self._semaphore = Semaphore(1)
        else:
            self._semaphore = Semaphore(self._concurrency)

    async def _call_sem(self, func: Callable, *args, **kwargs):
        """(Internal) Execute the query with semaphore."""
        async with self._semaphore:
            return await func(*args, **kwargs)

    # Stats --------------------------------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def _statistics(self, stats: cython.bint):
        """Print the query execution stats to the console.

        :param stats: `<boo>` Whether to print the query stats.

        ### Stats Information
        - The query information.
        - The executed statement.
        - The execution mode.
        - The affected/selected rows.
        - The execution time.
        """
        if not stats:
            return  # exit

        pad = 100
        breaker = "-" * pad
        end_time: cython.double = unixtime()
        # . information
        print(" Query Stats ".center(pad, "="))
        print("<%s> [%s]" % (self._tb._fname, self._name))
        # . get modes
        if not self._tb.is_timetable:
            mode = "3 (Specific Table Values)"
        else:
            mode = "4 (TimeTable Values)"
        mode = mode + (
            "\n* Notice: The values are presented as 'placeholder(s)' in the statement,\n"
            "  which will be replaced by the given 'values' at query executions:\n"
            "-> "
        )
        # . insert operations
        if self._insert_stmt:
            print()
            # . statment
            print(" INSERT OPERATION ".center(pad, "-"))
            print(self._insert_stmt)
            print(breaker)
            # . execution mode
            if not self._tb._is_timetable:
                msg = "Total values: %d" % list_len(self._insert_values_normtable)
            else:
                tb_rows: list = []
                for key, val in dict_items(self._insert_values_timetable):
                    i = "Sub-table '%s' values: %d" % (key, list_len(val))
                    list_append(tb_rows, i)
                msg = "\n-> ".join(tb_rows)
            print("Execution Mode: %s" % mode + msg)
            print(breaker)
            # . affected rows
            print("Insert Rows: %d" % self._insert_rows)
        # . update operations
        if self._update_stmt:
            print()
            # . statment
            print(" UPDATE OPERATION ".center(pad, "-"))
            print(self._update_stmt)
            print(breaker)
            # . execution mode
            if not self._tb._is_timetable:
                msg = "Total values: %d" % list_len(self._update_values_normtable)
            else:
                tb_rows: list = []
                for key, val in dict_items(self._update_values_timetable):
                    i = "Sub-table '%s' values: %d" % (key, list_len(val))
                    list_append(tb_rows, i)
                msg = "\n-> ".join(tb_rows)
            print("Execution Mode: %s" % mode + msg)
            print(breaker)
            # . affected rows
            print("Update Rows: %d" % self._update_rows)
        # . delete operations
        if self._delete_stmt:
            print()
            # . statment
            print(" DELETE OPERATION ".center(pad, "-"))
            print(self._delete_stmt)
            print(breaker)
            # . execution mode
            if not self._tb._is_timetable:
                msg = "Total values: %d" % list_len(self._delete_values_normtable)
            else:
                tb_rows: list = []
                for key, val in dict_items(self._delete_values_timetable):
                    i = "Sub-table '%s' values: %d" % (key, list_len(val))
                    list_append(tb_rows, i)
                msg = "\n-> ".join(tb_rows)
            print("Execution Mode: %s" % mode + msg)
            print(breaker)
            # . affected rows
            print("Delete Rows: %d" % self._delete_rows)
        # . affected rows
        if self._affected_rows > 0:
            print()
        print(breaker)
        print("Affected Rows.: %d" % self._affected_rows)
        print(breaker)
        # . execution time
        print("Execution Time: %fs" % (end_time - self._start_time))
        print("=" * pad)
        print()

    # Garbage ------------------------------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def _collect_garbage(self):
        # Alread collected
        if self._tb is None:
            return None

        # Query
        self._tb = None
        self._db = None
        # Data
        self._src = None
        self._bsl = None
        self._src_common = None
        self._bsl_common = None
        # Columns
        self._unique_columns = None
        self._ops_unique = None
        self._compare_columns = None
        self._ops_compare = None
        # Data
        self._insert_data = None
        self._update_data = None
        self._delete_data = None
        self._common_data = None
        self._identical_data = None
        # Operations
        self._operations = set()
        self._insert_stmt = None
        self._insert_values_normtable = None
        self._insert_values_timetable = None
        self._update_stmt = None
        self._update_values_normtable = None
        self._update_values_timetable = None
        self._delete_stmt = None
        self._delete_values_normtable = None
        self._delete_values_timetable = None
        self._timetable_phl = None
        self._drop_columns = None
        # Execute
        self._semaphore = None
        self._cursor = None
        self._timeout = None
