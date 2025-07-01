# cython: language_level=3
# cython: wraparound=False
# cython: boundscheck=False
from __future__ import annotations

# Cython imports
import cython
from cython.cimports.cpython.list import PyList_Size as list_len  # type: ignore
from cython.cimports.cpython.list import PyList_Append as list_append  # type: ignore
from cython.cimports.cpython.tuple import PyTuple_Size as tuple_len  # type: ignore
from cython.cimports.cpython.dict import PyDict_Size as dict_len  # type: ignore
from cython.cimports.cpython.dict import PyDict_SetItem as dict_setitem  # type: ignore
from cython.cimports.cpython.unicode import PyUnicode_GET_LENGTH as str_len  # type: ignore
from cython.cimports.sqlcycli.sqlfunc import SQLFunction  # type: ignore
from cython.cimports.sqlcycli.transcode import escape as _escape  # type: ignore
from cython.cimports.sqlcycli.utils import format_sql as _format_sql  # type: ignore
from cython.cimports.sqlcycli.aio.pool import Pool, PoolConnection, PoolSyncConnection  # type: ignore
from cython.cimports.mysqlengine.element import Element, Elements  # type: ignore
from cython.cimports.mysqlengine import utils  # type: ignore

# Python imports
from asyncio import gather as _aio_gather
from sqlcycli import errors as sqlerrors
from sqlcycli.sqlfunc import SQLFunction
from sqlcycli.transcode import escape as _escape
from sqlcycli.aio.pool import Pool, PoolConnection, PoolSyncConnection
from mysqlengine.element import Element, Elements
from mysqlengine import utils, errors


__all__ = [
    "SelectDML",
    "InsertDML",
    "ReplaceDML",
    "UpdateDML",
    "DeleteDML",
    "WithDML",
]


# Clause -----------------------------------------------------------------------------------------------------
@cython.cclass
class CLAUSE:
    """Base class for all clauses of a SQL statement."""

    # Index Hints
    _index_hints: list[INDEX_HINTS]
    # Conditions
    _conditions: list[str]
    _in_conditions: dict[str, str | tuple | SelectDML]
    _not_in_conditions: dict[str, str | tuple | SelectDML]
    # Internal
    _hashcode: cython.Py_ssize_t

    def __cinit__(self):
        self._hashcode = -1

    # Compose ------------------------------------------------------------------------------
    @cython.ccall
    def clause(self, pad: str = None) -> str:
        """Compose the clause `<'str'>`.

        :param pad `<'str/None'>`: The indentation padding. Defaults to `None`.
        """
        raise NotImplementedError(
            "<'%s'> 'clause' method is not implemented." % self.__class__.__name__
        )

    # Index Hints --------------------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(-1, check=False)
    def _add_index_hints(self, index_hints: INDEX_HINTS) -> cython.bint:
        """(internal) Add INDEX HINTS (for FROM, JOIN, UPDATE & DELETE clauses).

        :param index_hints `<'INDEX_HINTS'>`: The index hints clause.
        """
        if self._index_hints is None:
            self._index_hints = [index_hints]
        else:
            list_append(self._index_hints, index_hints)
        return True

    @cython.cfunc
    @cython.inline(True)
    def _attach_index_hints(self, clause: str, pad: str = None) -> str:
        """(internal) Attach index hints (if exists) to the clause `<'str'>`.

        :param clause `<'str'>`: The composed clause.
        """
        if self._index_hints is not None and list_len(self._index_hints) > 0:
            i: INDEX_HINTS
            clause += "\n\t" + "\n\t".join([i.clause(pad) for i in self._index_hints])
        return clause

    # Conditions ---------------------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    def _validate_condition(self, cond: str) -> str:
        """(internal) Validate a SQL condition `<'str'>`.

        :param cond `<'str'>`: The condition to validate.

        ## Explanation
        - This method ensures the returned condition string begins with a logical prefix.
        - If 'cond' already starts with `"AND "` or `"OR "`, it is returned unchanged;
          otherwise, `"AND "` is prepended.
        """
        if cond.startswith(("OR ", "AND ")):
            return cond
        return "AND " + cond

    @cython.cfunc
    @cython.inline(True)
    def _prepare_conditions(self, pad: str = None) -> list[str]:
        """(internal) Validate and combine all SQL conditions `<'list[str]'>`.

        :param pad `<'str/None'>`: The indentation padding. Defaults to `None`.

        ## Explanation
        This method validates and combines all SQL conditions, including:
        - Regular conditions (`_conditions`)
        - IN conditions (`_in_conditions`)
        - NOT IN conditions (`_not_in_conditions`)
        """
        cond: str
        res: list[str] = []
        i: cython.Py_ssize_t = 0
        # . conditions
        conds: list = self._conditions
        if conds is not None and list_len(conds) > 0:
            for cond in conds:
                if i != 0:
                    cond = self._validate_condition(cond)
                res.append(cond)
                i += 1

        # . IN conditions
        pad_nxt: str = None
        in_conds: dict = self._in_conditions
        if in_conds is not None and dict_len(in_conds) > 0:
            for cond, values in in_conds.items():
                if i != 0:
                    cond = self._validate_condition(cond)
                if isinstance(values, SelectDML):
                    dml: SelectDML = values
                    if pad_nxt is None:
                        pad_nxt = "\t" if pad is None else pad + "\t"
                    res.append("%s IN %s" % (cond, dml._gen_select_subquery(pad_nxt)))
                elif isinstance(values, tuple):
                    res.append("%s IN (%s)" % (cond, ",".join(values)))
                else:
                    res.append("%s IN (%s)" % (cond, values))
                i += 1

        # . NOT IN conditions
        not_in_conds: dict = self._not_in_conditions
        if not_in_conds is not None and dict_len(not_in_conds) > 0:
            for cond, values in not_in_conds.items():
                if i != 0:
                    cond = self._validate_condition(cond)
                if isinstance(values, SelectDML):
                    dml: SelectDML = values
                    if pad_nxt is None:
                        pad_nxt = "\t" if pad is None else pad + "\t"
                    res.append(
                        "%s NOT IN %s" % (cond, dml._gen_select_subquery(pad_nxt))
                    )
                elif isinstance(values, tuple):
                    res.append("%s NOT IN (%s)" % (cond, ",".join(values)))
                else:
                    res.append("%s NOT IN (%s)" % (cond, values))
                i += 1

        # . return
        return res

    # Special Method -----------------------------------------------------------------------
    def __repr__(self) -> str:
        return "<'%s CLAUSE'>" % self.__class__.__name__

    def __hash__(self) -> int:
        if self._hashcode == -1:
            self._hashcode = id(self)
        return self._hashcode


# Select Clause - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
@cython.cclass
class SELECT(CLAUSE):
    """The SELECT clause of a SQL statement."""

    _expressions: list[str]
    _distinct: cython.bint
    _high_priority: cython.bint
    _straight_join: cython.bint
    _sql_buffer_result: cython.bint

    @cython.ccall
    def setup(
        self,
        expressions: list[str],
        distinct: cython.bint = False,
        high_priority: cython.bint = False,
        straight_join: cython.bint = False,
        sql_buffer_result: cython.bint = False,
    ) -> SELECT:
        """Setup the SELECT clause `<'SELECT'>`."""
        self._expressions = expressions
        self._distinct = distinct
        self._high_priority = high_priority
        self._straight_join = straight_join
        self._sql_buffer_result = sql_buffer_result
        return self

    @cython.ccall
    def clause(self, pad: str = None) -> str:
        """Compose the SELECT clause `<'str'>`.

        :param pad `<'str/None'>`: The indentation padding. Defaults to `None`.
        """
        # Select clause
        if pad is None:
            clause: str = "SELECT"
        else:
            clause: str = pad + "SELECT"
        if self._distinct:
            clause += " DISTINCT"
        if self._high_priority:
            clause += " HIGH_PRIORITY"
        if self._straight_join:
            clause += " STRAIGHT_JOIN"
        if self._sql_buffer_result:
            clause += " SQL_BUFFER_RESULT"

        # One expression
        if list_len(self._expressions) == 1:
            return "%s %s" % (clause, self._expressions[0])

        # Multiple expressions
        nxt: str
        if pad is None:
            nxt = ",\n\t"
            pad = "\n\t"
        else:
            nxt = ",\n\t" + pad
            pad = "\n\t" + pad
        return clause + pad + nxt.join(self._expressions)


@cython.cclass
class FROM(CLAUSE):
    """The FROM clause of a SQL statement."""

    _table_reference: str | SelectDML
    _partition: list[str]
    _alias: str

    @cython.ccall
    def setup(
        self,
        table_reference: str | SelectDML,
        partition: list[str],
        alias: str,
    ) -> FROM:
        """Setup the FROM clause `<'FROM'>`."""
        self._table_reference = table_reference
        self._partition = partition
        self._alias = alias
        return self

    @cython.ccall
    def clause(self, pad: str = None) -> str:
        """Compose the FROM clause `<'str'>`.

        :param pad `<'str/None'>`: The indentation padding. Defaults to `None`.
        """
        # Table reference
        if isinstance(self._table_reference, SelectDML):
            dml: SelectDML = self._table_reference
            table_reference: str = dml._gen_select_subquery(pad)
        else:
            table_reference: str = self._table_reference

        # Partition not specified
        if self._partition is None or list_len(self._partition) == 0:
            if pad is None:
                clause: str = "FROM %s AS %s" % (table_reference, self._alias)
            else:
                clause: str = "%sFROM %s AS %s" % (pad, table_reference, self._alias)
        # Partition specified
        else:
            # fmt: off
            partition: str = ", ".join(self._partition)
            if pad is None:
                clause: str = "FROM %s PARTITION (%s) AS %s" % (
                    table_reference, partition, self._alias,
                )
            else:
                clause: str = "%sFROM %s PARTITION (%s) AS %s" % (
                    pad, table_reference, partition, self._alias,
                )
            # fmt: on

        # Attach index hints
        return self._attach_index_hints(clause, pad)


@cython.cclass
class JOIN(CLAUSE):
    """The JOIN clause of a SQL statement."""

    _join_method: str
    _table_reference: str | SelectDML
    _partition: list[str]
    _using_columns: list[str]
    _alias: str

    @cython.ccall
    def setup(
        self,
        join_method: str,
        table_reference: str | SelectDML,
        partition: list[str],
        on_conditions: list[str],
        using_columns: list[str],
        alias: str,
    ) -> JOIN:
        """Setup the JOIN clause `<'JOIN'>`."""
        self._join_method = join_method
        self._table_reference = table_reference
        self._partition = partition
        self._conditions = on_conditions
        self._using_columns = using_columns
        self._alias = alias
        return self

    @cython.ccall
    def clause(self, pad: str = None) -> str:
        """Compose the JOIN clause `<'str'>`.

        :param pad `<'str/None'>`: The indentation padding. Defaults to `None`.
        """
        # Table reference
        if isinstance(self._table_reference, SelectDML):
            dml: SelectDML = self._table_reference
            table_reference: str = dml._gen_select_subquery(pad)
        else:
            table_reference: str = self._table_reference

        # Partition not specified
        # fmt: off
        if self._partition is None or list_len(self._partition) == 0:
            if pad is None:
                clause: str = "%s %s AS %s" % (
                    self._join_method, table_reference, self._alias,
                )
            else:
                clause: str = "%s%s %s AS %s" % (
                    pad, self._join_method, table_reference, self._alias,
                )
        # Partition specified
        else:
            partition: str = ", ".join(self._partition)
            if pad is None:
                clause: str = "%s %s PARTITION (%s) AS %s" % (
                    self._join_method, table_reference, partition, self._alias,
                )
            else:
                clause: str = "%s%s %s PARTITION (%s) AS %s" % (
                    pad, self._join_method, table_reference, partition, self._alias,
                )
        # fmt: on

        # Attach index hints
        clause = self._attach_index_hints(clause, pad)

        # Conditions
        conds: list = self._prepare_conditions(pad)
        if list_len(conds) > 0:
            if pad is None:
                nxt: str = "\n\t"
                pfx: str = "\n\tON "
            else:
                nxt: str = "\n\t" + pad
                pfx: str = nxt + "ON "
            clause += pfx + nxt.join(conds)
        elif self._using_columns is not None and list_len(self._using_columns) > 0:
            columns: str = ", ".join(self._using_columns)
            if pad is None:
                clause += "\n\tUSING (%s)" % columns
            else:
                clause = "%s\n\t%sUSING (%s)" % (clause, pad, columns)
        return clause


@cython.cclass
class INDEX_HINTS(CLAUSE):
    """The INDEX HINTS clause of a SQL statement."""

    _mode: str
    _indexes: list[str]
    _scope: str

    @cython.ccall
    def setup(
        self,
        mode: str,
        indexes: list[str],
        scope: str,
    ) -> INDEX_HINTS:
        """Setup the INDEX HINTS clause `<'INDEX_HINTS'>`."""
        self._mode = mode
        self._indexes = indexes
        self._scope = scope
        return self

    @cython.ccall
    def clause(self, pad: str = None) -> str:
        """Compose the INDEX HINTS clause `<'str'>`.

        :param pad `<'str/None'>`: The indentation padding. Defaults to `None`.
        """
        indexes: str = ", ".join(self._indexes)
        if self._scope is None:
            if pad is None:
                return "%s (%s)" % (self._mode, indexes)
            else:
                return "%s%s (%s)" % (pad, self._mode, indexes)
        elif pad is None:
            return "%s FOR %s (%s)" % (self._mode, self._scope, indexes)
        else:
            return "%s%s FOR %s (%s)" % (pad, self._mode, self._scope, indexes)


@cython.cclass
class WHERE(CLAUSE):
    """The WHERE clause of a SQL statement."""

    @cython.ccall
    def setup(
        self,
        conditions: list[str],
        in_conditions: dict,
        not_in_conditions: dict,
    ) -> WHERE:
        """Setup the WHERE clause `<'WHERE'>`."""
        self._conditions = conditions
        self._in_conditions = in_conditions
        self._not_in_conditions = not_in_conditions
        return self

    @cython.ccall
    def clause(self, pad: str = None) -> str:
        """Compose the WHERE clause `<'str'>`.

        :param pad `<'str/None'>`: The indentation padding. Defaults to `None`.
        """
        conds: list = self._prepare_conditions(pad)
        if pad is None:
            return "WHERE " + "\n\t".join(conds)
        else:
            return pad + "WHERE " + ("\n\t" + pad).join(conds)


@cython.cclass
class GROUP_BY(CLAUSE):
    """The GROUP BY clause of a SQL statement."""

    _columns: list[str]
    _with_rollup: cython.bint

    @cython.ccall
    def setup(self, columns: list[str], with_rollup: cython.bint) -> GROUP_BY:
        """Setup the GROUP BY clause `<'GROUP_BY'>`."""
        self._columns = columns
        self._with_rollup = with_rollup
        return self

    @cython.ccall
    def clause(self, pad: str = None) -> str:
        """Compose the GROUP BY clause `<'str'>`.

        :param pad `<'str/None'>`: The indentation padding. Defaults to `None`.
        """
        columns: str = ", ".join(self._columns)
        if self._with_rollup:
            if pad is None:
                return "GROUP BY %s WITH ROLLUP" % columns
            else:
                return "%sGROUP BY %s WITH ROLLUP" % (pad, columns)
        elif pad is None:
            return "GROUP BY " + columns
        else:
            return pad + "GROUP BY " + columns


@cython.cclass
class HAVING(CLAUSE):
    """The HAVING clause of a SQL statement."""

    @cython.ccall
    def setup(
        self,
        conditions: list[str],
        in_conditions: dict,
        not_in_conditions: dict,
    ) -> HAVING:
        """Setup the HAVING clause `<'HAVING'>`."""
        self._conditions = conditions
        self._in_conditions = in_conditions
        self._not_in_conditions = not_in_conditions
        return self

    @cython.ccall
    def clause(self, pad: str = None) -> str:
        """Compose the HAVING clause `<'str'>`.

        :param pad `<'str/None'>`: The indentation padding. Defaults to `None`.
        """
        conds: list = self._prepare_conditions(pad)
        if pad is None:
            return "HAVING " + "\n\t".join(conds)
        else:
            return pad + "HAVING " + ("\n\t" + pad).join(conds)


@cython.cclass
class WINDOW(CLAUSE):
    """The WINDOW clause of a SQL statement."""

    _name: str
    _options: list[str]
    _primary: cython.bint

    @cython.ccall
    def setup(self, name: str, options: list[str]) -> WINDOW:
        """Setup the WINDOW clause `<'WINDOW'>`."""
        self._name = name
        self._options = options
        self._primary = True
        return self

    @cython.ccall
    def clause(self, pad: str = None) -> str:
        """Compose the WINDOW clause `<'str'>`.

        :param pad `<'str/None'>`: The indentation padding. Defaults to `None`.
        """
        if self._primary:
            if pad is None:
                clause: str = "WINDOW %s AS " % self._name
            else:
                clause: str = "%sWINDOW %s AS " % (pad, self._name)
        else:
            clause: str = "%s AS " % self._name
        if self._options is None:
            return clause + "()"  # exit

        # Options
        size: cython.Py_ssize_t = list_len(self._options)
        if size == 0:
            return clause + "()"
        elif size == 1:
            return "%s(%s)" % (clause, self._options[0])
        elif pad is None:
            opts: str = "\n\t".join(self._options)
            return "%s(\n\t%s\n)" % (clause, opts)
        else:
            opts: str = ("\n\t" + pad).join(self._options)
            return "%s(\n\t%s%s\n%s)" % (clause, pad, opts, pad)


@cython.cclass
class SET_OPERATION(CLAUSE):
    """The SET OPERATION (UNION, INTERSECT and EXCEPT) clause of a SQL statement."""

    _operation: str
    _subquery: SelectDML
    _all: cython.bint

    @cython.ccall
    def setup(
        self,
        operation: str,
        subquery: SelectDML,
        all: cython.bint,
    ) -> SET_OPERATION:
        """Setup the SET OPERATION (UNION, INTERSECT and EXCEPT) clause `<'SET_OPERATION'>`."""
        self._operation = operation
        self._subquery = subquery
        self._all = all
        return self

    @cython.ccall
    def clause(self, pad: str = None) -> str:
        """Compose the SET OPERATION (UNION, INTERSECT and EXCEPT) clause `<'str'>`.

        :param pad `<'str/None'>`: The indentation padding. Defaults to `None`.
        """
        subquery: str = self._subquery._gen_select_subquery(pad)
        if pad is None:
            if self._all:
                return "%s ALL\n%s" % (self._operation, subquery)
            else:
                return "%s DISTINCT\n%s" % (self._operation, subquery)
        elif self._all:
            return "%s%s ALL\n%s%s" % (pad, self._operation, pad, subquery)
        else:
            return "%s%s DISTINCT\n%s%s" % (pad, self._operation, pad, subquery)


@cython.cclass
class ORDER_BY(CLAUSE):
    """The ORDER BY clause of a SQL statement."""

    _columns: list[str]
    _with_rollup: cython.bint

    @cython.ccall
    def setup(self, columns: list[str], with_rollup: cython.bint) -> ORDER_BY:
        """Setup the ORDER BY clause `<'ORDER_BY'>`."""
        self._columns = columns
        self._with_rollup = with_rollup
        return self

    @cython.ccall
    def clause(self, pad: str = None) -> str:
        """Compose the ORDER BY clause `<'str'>`.

        :param pad `<'str/None'>`: The indentation padding. Defaults to `None`.
        """
        columns: str = ", ".join(self._columns)
        if self._with_rollup:
            if pad is None:
                return "ORDER BY %s WITH ROLLUP" % columns
            else:
                return "%sORDER BY %s WITH ROLLUP" % columns
        elif pad is None:
            return "ORDER BY " + columns
        else:
            return pad + "ORDER BY " + columns


@cython.cclass
class LIMIT(CLAUSE):
    """The LIMIT clause of a SQL statement."""

    _row_count: cython.ulonglong
    _offset: cython.ulonglong

    @cython.ccall
    def setup(
        self,
        row_count: cython.ulonglong,
        offset: cython.ulonglong,
    ) -> LIMIT:
        """Setup the LIMIT clause `<'LIMIT'>`."""
        self._row_count = row_count
        self._offset = offset
        return self

    @cython.ccall
    def clause(self, pad: str = None) -> str:
        """Compose the LIMIT clause `<'str'>`.

        :param pad `<'str/None'>`: The indentation padding. Defaults to `None`.
        """
        if self._offset == 0:
            if pad is None:
                return "LIMIT %d" % self._row_count
            else:
                return "%sLIMIT %d" % (pad, self._row_count)
        elif pad is None:
            return "LIMIT %d, %d" % (self._offset, self._row_count)
        else:
            return "%sLIMIT %d, %d" % (pad, self._offset, self._row_count)


@cython.cclass
class LOCKING_READS(CLAUSE):
    """The LOCKING READS clause of a SQL statement."""

    _mode: str
    _tables: list[str]
    _option: str

    @cython.ccall
    def setup(
        self,
        mode: str,
        tables: list[str],
        option: str,
    ) -> LOCKING_READS:
        """Setup the LOCKING READS clause `<'LOCKING_READS'>`."""
        self._mode = mode
        self._tables = tables
        self._option = option
        return self

    @cython.ccall
    def clause(self, pad: str = None) -> str:
        """Compose the LOCKING READS clause `<'str'>`.

        :param pad `<'str/None'>`: The indentation padding. Defaults to `None`.
        """
        clause: str = self._mode if pad is None else pad + self._mode
        if self._tables is not None and list_len(self._tables) > 0:
            clause += " OF " + ", ".join(self._tables)
        if self._option is not None:
            clause += " " + self._option
        return clause


@cython.cclass
class INTO_VARIABLES(CLAUSE):
    """The INTO (variables) clause of a SQL statement."""

    _variables: list[str]

    @cython.ccall
    def setup(self, variables: list[str]) -> INTO_VARIABLES:
        """Setup the INTO (variables) clause `<'INTO_VARIABLES'>`."""
        self._variables = variables
        return self

    @cython.ccall
    def clause(self, pad: str = None) -> str:
        """Compose the INTO (variables) clause `<'str'>`.

        :param pad `<'str/None'>`: The indentation padding. Defaults to `None`.
        """
        variables: str = ", ".join(self._variables)
        if pad is None:
            return "INTO " + variables
        else:
            return pad + "INTO " + variables


# Insert Clause - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
@cython.cclass
class INSERT(CLAUSE):
    """The INSERT clause of a SQL statement."""

    _table: str
    _partition: list[str]
    _ignore: cython.bint
    _priority: str
    _replace_mode: cython.bint

    @cython.ccall
    def setup(
        self,
        table: str,
        partition: list[str],
        ignore: cython.bint,
        priority: str,
    ) -> INSERT:
        """Setup the INSERT clause `<'INSERT'>`."""
        self._table = table
        self._partition = partition
        self._ignore = ignore
        self._priority = priority
        self._replace_mode = False
        return self

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(-1, check=False)
    def _set_replace_mode(self, replace_mode: cython.bint) -> cython.bint:
        """(internal) Set the replace mode of the INSERT clause.

        :param replace_mode `<'bool'>`: If `True`, the INSERT cluase becomes REPLACE clause.
        """
        self._replace_mode = replace_mode
        return True

    @cython.ccall
    def clause(self, pad: str = None) -> str:
        """Compose the INSERT clause `<'str'>`.

        :param pad `<'str/None'>`: The indentation padding. Defaults to `None`.
        """
        if not self._replace_mode:
            clause: str = "INSERT " if pad is None else pad + "INSERT "
        else:
            clause: str = "REPLACE " if pad is None else pad + "REPLACE "
        if self._priority is not None:
            clause += self._priority + " "
        if self._ignore:
            clause += "IGNORE "
        clause += "INTO " + self._table
        if self._partition is not None and list_len(self._partition) > 0:
            clause += " PARTITION (%s)" % ", ".join(self._partition)
        return clause


@cython.cclass
class INSERT_COLUMNS(CLAUSE):
    """The INSERT COLUMNS clause of a SQL statement."""

    _columns: list[str]

    @cython.ccall
    def setup(self, columns: list[str]) -> INSERT_COLUMNS:
        """Setup the INSERT COLUMNS clause `<'INSERT_COLUMNS'>`."""
        self._columns = columns
        return self

    @cython.ccall
    def clause(self, pad: str = None) -> str:
        """Compose the INSERT COLUMNS clause `<'str'>`.

        :param pad `<'str/None'>`: The indentation padding. Defaults to `None`.
        """
        if self._columns is None or list_len(self._columns) == 0:
            return "\t()" if pad is None else pad + "\t()"

        columns: str = ", ".join(self._columns)
        if pad is None:
            return "\t(%s)" % columns
        else:
            return "%s\t(%s)" % (pad, columns)


@cython.cclass
class INSERT_VALUES(CLAUSE):
    """The INSERT VALUES clause of a SQL statement."""

    _placeholders: cython.int
    _row_alias: ROW_ALIAS

    @cython.ccall
    def setup(self, placeholders: cython.int) -> INSERT_VALUES:
        """Setup the INSERT VALUES clause `<'INSERT_VALUES'>`."""
        self._placeholders = placeholders
        self._row_alias = None
        return self

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(-1, check=False)
    def _add_row_alias(self, row_alias: ROW_ALIAS) -> cython.bint:
        """(internal) Add ROW ALIAS for the VALUES clause.

        :param row_alias `<'ROW_ALIAS'>`: The row alias clause.
        """
        self._row_alias = row_alias
        return True

    @cython.ccall
    def clause(self, pad: str = None) -> str:
        """Compose the INSERT VALUES clause `<'str'>`.

        :param pad `<'str/None'>`: The indentation padding. Defaults to `None`.
        """
        if self._placeholders <= 1:
            phl: str = "%s"
        else:
            phl: str = ",".join(["%s" for _ in range(self._placeholders)])

        # Without Row Alias
        if self._row_alias is None:
            if pad is None:
                return "VALUES (%s)" % phl
            else:
                return "%sVALUES (%s)" % (pad, phl)

        # With Row Alias
        row_alias: str = self._row_alias.clause(None)
        if pad is None:
            return "VALUES (%s) %s" % (phl, row_alias)
        else:
            return "%sVALUES (%s) %s" % (pad, phl, row_alias)


@cython.cclass
class SET(CLAUSE):
    """The SET clause of a SQL statement."""

    _assignments: list[str]
    _row_alias: ROW_ALIAS

    @cython.ccall
    def setup(self, assignments: list[str]) -> SET:
        """Setup the SET clause `<'SET'>`."""
        self._assignments = assignments
        self._row_alias = None
        return self

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(-1, check=False)
    def _add_row_alias(self, row_alias: ROW_ALIAS) -> cython.bint:
        """(internal) Add ROW ALIAS for the SET clause.

        :param row_alias `<'ROW_ALIAS'>`: The row alias clause.
        """
        self._row_alias = row_alias
        return True

    @cython.ccall
    def clause(self, pad: str = None) -> str:
        """Compose the SET clause `<'str'>`.

        :param pad `<'str/None'>`: The indentation padding. Defaults to `None`.
        """
        clause: str = "SET" if pad is None else pad + "SET"
        if list_len(self._assignments) == 1:
            assignments: str = self._assignments[0]
            if self._row_alias is None:
                return "%s %s" % (clause, assignments)
            else:
                alias: str = self._row_alias.clause(None)
                return "%s %s %s" % (clause, assignments, alias)

        alias: str = None if self._row_alias is None else self._row_alias.clause(pad)
        if pad is None:
            nxt = ",\n\t"
            pad = "\n\t"
        else:
            nxt = ",\n\t" + pad
            pad = "\n\t" + pad
        assignments: str = nxt.join(self._assignments)
        if alias is None:
            return clause + pad + assignments
        else:
            return "%s%s%s\n%s" % (clause, pad, assignments, alias)


@cython.cclass
class ROW_ALIAS(CLAUSE):
    """The ROW ALIAS clause of a SQL statement."""

    _row_alias: str
    _col_alias: list[str]

    @cython.ccall
    def setup(
        self,
        row_alias: str,
        col_alias: list[str],
    ) -> ROW_ALIAS:
        """Setup the ROW ALIAS clause `<'ROW_ALIAS'>`."""
        self._row_alias = row_alias
        self._col_alias = col_alias
        return self

    @cython.ccall
    def clause(self, pad: str = None) -> str:
        """Compose the ROW ALIAS clause `<'str'>`.

        :param pad `<'str/None'>`: The indentation padding. Defaults to `None`.
        """
        # Without Column Aliases
        if self._col_alias is None or list_len(self._col_alias) == 0:
            if pad is None:
                return "AS %s" % self._row_alias
            else:
                return "%sAS %s" % (pad, self._row_alias)

        # With Column Aliases
        cols_alias: str = ", ".join(self._col_alias)
        if pad is None:
            return "AS %s (%s)" % (self._row_alias, cols_alias)
        else:
            return "%sAS %s (%s)" % (pad, self._row_alias, cols_alias)


@cython.cclass
class ON_DUPLICATE(CLAUSE):
    """The ON DUPLICATE KEY UPDATE clause of a SQL statement."""

    _assignments: list[str]

    @cython.ccall
    def setup(self, assignments: list[str]) -> ON_DUPLICATE:
        """Setup the ON DUPLICATE KEY UPDATE clause `<'ON_DUPLICATE'>`."""
        self._assignments = assignments
        return self

    @cython.ccall
    def clause(self, pad: str = None) -> str:
        """Compose the ON DUPLICATE KEY UPDATE clause `<'str'>`.

        :param pad `<'str/None'>`: The indentation padding. Defaults to `None`.
        """
        if list_len(self._assignments) == 1:
            assignments: str = self._assignments[0]
            if pad is None:
                return "ON DUPLICATE KEY UPDATE\n\t" + assignments
            else:
                return "%sON DUPLICATE KEY UPDATE\n\t%s%s" % (pad, pad, assignments)
        elif pad is None:
            assignments: str = ",\n\t".join(self._assignments)
            return "ON DUPLICATE KEY UPDATE\n\t" + assignments
        else:
            assignments: str = (",\n\t" + pad).join(self._assignments)
            return "%sON DUPLICATE KEY UPDATE\n\t%s%s" % (pad, pad, assignments)


# Update Clause - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
@cython.cclass
class UPDATE(CLAUSE):
    """The UPDATE clause of a SQL statement."""

    _table_reference: str | SelectDML
    _partition: list[str]
    _ignore: cython.bint
    _low_priority: cython.bint
    _alias: str

    @cython.ccall
    def setup(
        self,
        table_reference: str | SelectDML,
        partition: list[str],
        ignore: cython.bint,
        low_priority: cython.bint,
        alias: str,
    ) -> UPDATE:
        """Setup the UPDATE clause `<'UPDATE'>`."""
        self._table_reference = table_reference
        self._partition = partition
        self._ignore = ignore
        self._low_priority = low_priority
        self._alias = alias
        return self

    @cython.ccall
    def clause(self, pad: str = None) -> str:
        """Compose the UPDATE clause `<'str'>`.

        :param pad `<'str/None'>`: The indentation padding. Defaults to `None`.
        """
        # Update clause
        if self._low_priority:
            if self._ignore:
                clause: str = "UPDATE LOW_PRIORITY IGNORE"
            else:
                clause: str = "UPDATE LOW_PRIORITY"
        elif self._ignore:
            clause: str = "UPDATE IGNORE"
        else:
            clause: str = "UPDATE"

        # Table Reference
        if isinstance(self._table_reference, SelectDML):
            dml: SelectDML = self._table_reference
            table_ref: str = dml._gen_select_subquery(pad)
        else:
            table_ref: str = self._table_reference

        # Without Partition
        if self._partition is None or list_len(self._partition) == 0:
            if pad is None:
                clause: str = "%s %s AS %s" % (clause, table_ref, self._alias)
            else:
                clause: str = "%s%s %s AS %s" % (pad, clause, table_ref, self._alias)
        # With Partition
        else:
            partition: str = ", ".join(self._partition)
            # fmt: off
            if pad is None:
                clause: str = "%s %s PARTITION (%s) AS %s" % (
                    clause, table_ref, partition, self._alias,
                )
            else:
                clause: str = "%s%s %s PARTITION (%s) AS %s" % (
                    pad, clause, table_ref, partition, self._alias,
                )
            # fmt: on

        # Attach index hints
        return self._attach_index_hints(clause, pad)


# Delete Clause - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
@cython.cclass
class DELETE(CLAUSE):
    """The DELETE clause of a SQL statement."""

    _table_reference: str | SelectDML
    _partition: list[str]
    _ignore: cython.bint
    _low_priority: cython.bint
    _quick: cython.bint
    _alias: str
    _multi_tables: list[str]

    @cython.ccall
    def setup(
        self,
        table_reference: str | SelectDML,
        partition: list[str],
        ignore: cython.bint,
        low_priority: cython.bint,
        quick: cython.bint,
        alias: str,
        multi_tables: list[str],
    ) -> DELETE:
        """Setup the DELETE clause `<'DELETE'>`."""
        self._table_reference = table_reference
        self._partition = partition
        self._ignore = ignore
        self._low_priority = low_priority
        self._quick = quick
        self._alias = alias
        self._multi_tables = multi_tables
        return self

    @cython.ccall
    @cython.exceptval(-1, check=False)
    def _has_multi_tables(self) -> cython.bint:
        """(internal) Check if the DELETE clause has
        the 'multi_tables' argument configurated `<'bool'>`.
        """
        if self._multi_tables is None:
            return False
        if list_len(self._multi_tables) == 0:
            return False
        return True

    @cython.ccall
    def clause(self, pad: str = None) -> str:
        """Compose the DELETE clause `<'str'>`.

        :param pad `<'str/None'>`: The indentation padding. Defaults to `None`.
        """
        # Delete Clause
        if self._low_priority:
            if self._quick:
                if self._ignore:
                    clause: str = "DELETE LOW_PRIORITY QUICK IGNORE"
                else:
                    clause: str = "DELETE LOW_PRIORITY"
            elif self._ignore:
                clause: str = "DELETE LOW_PRIORITY IGNORE"
            else:
                clause: str = "DELETE LOW_PRIORITY"
        elif self._quick:
            if self._ignore:
                clause: str = "DELETE QUICK IGNORE"
            else:
                clause: str = "DELETE QUICK"
        elif self._ignore:
            clause: str = "DELETE IGNORE"
        else:
            clause: str = "DELETE"

        # Table Reference
        if isinstance(self._table_reference, SelectDML):
            dml: SelectDML = self._table_reference
            table_ref: str = dml._gen_select_subquery(pad)
        else:
            table_ref: str = self._table_reference

        # Partition
        if self._partition is None or list_len(self._partition) == 0:
            partition: str = None
        else:
            partition: str = ", ".join(self._partition)

        # Single-table
        # fmt: off
        if not self._has_multi_tables():
            if partition is None:
                clause = "%s FROM %s AS %s" % (
                    clause, table_ref, self._alias,
                )
            else:
                clause = "%s FROM %s AS %s PARTITION (%s)" % (
                    clause, table_ref, self._alias, partition,
                )
        # Multi-table
        else:
            table_alias: str = ", ".join(self._multi_tables)
            if partition is None:
                clause = "%s %s FROM %s AS %s" % (
                    clause, table_alias, table_ref, self._alias,
                )
            else:
                clause = "%s %s FROM %s PARTITION (%s) AS %s" % (
                    clause, table_alias, table_ref, partition, self._alias,
                )
            # . attach index hints
            clause = self._attach_index_hints(clause, pad)
        # fmt: on

        # Indentation
        return clause if pad is None else pad + clause


# WITH Clause - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
@cython.cclass
class WITH(CLAUSE):
    """The WITH (Common Table Expression) clause of a SQL statement."""

    _name: str
    _subquery: str | SelectDML
    _columns: list[str]
    _recursive: cython.bint
    _primary: cython.bint

    @cython.ccall
    def setup(
        self,
        name: str,
        subquery: str | SelectDML,
        columns: list[str],
        recursive: cython.bint,
    ) -> WITH:
        """Setup the WITH (Common Table Expression) clause `<'WITH'>`."""
        self._name = name
        self._subquery = subquery
        self._columns = columns
        self._recursive = recursive
        self._primary = True
        return self

    @cython.ccall
    def clause(self, pad: str = None) -> str:
        """Compose the WITH (Common Table Expression) clause `<'str'>`.

        :param pad `<'str/None'>`: The indentation padding. Defaults to `None`.
        """
        # Columns
        if self._columns is not None and list_len(self._columns) > 0:
            columns: str = ", ".join(self._columns)
        else:
            columns: str = None

        # With Clause
        if self._primary:
            if self._recursive:
                if columns is None:
                    clause: str = "WITH RECURSIVE %s AS " % self._name
                else:
                    clause: str = "WITH RECURSIVE %s (%s) AS " % (self._name, columns)
            elif columns is None:
                clause: str = "WITH %s AS " % self._name
            else:
                clause: str = "WITH %s (%s) AS " % (self._name, columns)
        elif columns is None:
            clause: str = "%s AS " % self._name
        else:
            clause: str = "%s (%s) AS " % (self._name, columns)

        # Subquery
        if isinstance(self._subquery, SelectDML):
            dml: SelectDML = self._subquery
            if pad is None:
                return clause + dml._gen_select_subquery(pad)
            else:
                return pad + clause + dml._gen_select_subquery(pad)
        elif pad is None:
            return clause + "(%s)" % self._subquery
        else:
            return pad + clause + "(%s)" % self._subquery


# DML --------------------------------------------------------------------------------------------------------
@cython.cclass
class DML:
    """The base class for the DML (Data Manipulation Language) statement."""

    # settings
    _dml: str
    _db_name: str
    _pool: Pool
    # clauses
    _with_clauses: list[WITH]
    _select_clause: SELECT
    _from_clause: FROM
    _join_clauses: list[JOIN]
    _where_clause: WHERE
    _group_by_clause: GROUP_BY
    _having_clause: HAVING
    _window_clauses: list[WINDOW]
    _set_op_clauses: list[SET_OPERATION]
    _order_by_clause: ORDER_BY
    _limit_clause: LIMIT
    _locking_reads_clauses: list[LOCKING_READS]
    _into_clause: INTO_VARIABLES
    # connection
    _require_conn: cython.bint
    _sync_conn: PoolSyncConnection
    _async_conn: PoolConnection
    # internal
    _clause_id: cython.int
    _tb_id: cython.int
    _multi_table: cython.bint
    _hashcode: cython.Py_ssize_t

    def __init__(self, dml: str, db_name: str, pool: Pool):
        """The base class for the DML (Data Manipulation Language) statement.

        :param dml `<'str'>`: The name of the DML statement (e.g., 'SELECT', 'INSERT', 'UPDATE', 'DELETE').
        :param db_name `<'str'>`: The name of the database.
        :param pool `<'Pool'>`: The connection pool to handle the statement execution.
        """
        # settings
        self._dml = dml
        self._db_name = db_name
        self._pool = pool
        # clauses
        self._with_clauses = None
        self._select_clause = None
        self._from_clause = None
        self._join_clauses = None
        self._where_clause = None
        self._group_by_clause = None
        self._having_clause = None
        self._window_clauses = None
        self._set_op_clauses = None
        self._order_by_clause = None
        self._limit_clause = None
        self._locking_reads_clauses = None
        self._into_clause = None
        # connection
        self._require_conn = False
        self._sync_conn = None
        self._async_conn = None
        # internal
        self._clause_id = utils.DML_CLAUSE.NONE
        self._tb_id = 0
        self._multi_table = False
        self._hashcode = -1

    # Property -----------------------------------------------------------------------------
    @property
    def db_name(self) -> str:
        """The name of the database that issued the DML statement `<'str'>`."""
        return self._db_name

    @property
    def pool(self) -> Pool:
        """The connection pool to handle the statement execution. `<'Pool'>`."""
        return self._pool

    # Connection ---------------------------------------------------------------------------
    @cython.ccall
    @cython.exceptval(-1, check=False)
    def _set_connection(
        self,
        sync_conn: PoolSyncConnection = None,
        async_conn: PoolConnection = None,
    ) -> cython.bint:
        """(internal) Set the connections to execute the DML statement.

        :param sync_conn `<'PoolSyncConnection'>`: The [sync] connection.
        :param async_conn `<'PoolConnection'>`: The [async] connection.
        """
        self._require_conn = True
        self._sync_conn = sync_conn
        self._async_conn = async_conn
        return True

    # SELECT Clause ------------------------------------------------------------------------
    # . select
    @cython.ccall
    def _gen_select_clause(
        self,
        expressions: tuple,
        distinct: cython.bint = False,
        high_priority: cython.bint = False,
        straight_join: cython.bint = False,
        sql_buffer_result: cython.bint = False,
    ) -> SELECT:
        """(internal) Generate the SELECT clause `<'SELECT'>`.

        :param expressions `<'tuple[str/Column]'>`: The (expression of) the column(s) to retrieve.

        :param distinct `<'bool'>`: If `True`, specifies removal of duplicate rows from
            the result set. Defaults to `False`.

        :param high_priority `<'bool'>`: If `True`, gives the SELECT higher priority than
            a statement that updates a table. Defaults to `False`. Should only be used for
            queries that are very fast and must be done at once. A SELECT HIGH_PRIORITY
            query that is issued while the table is locked for reading runs even if there
            is an update statement waiting for the table to be free. This affects only storage
            engines that use only table-level locking (such as MyISAM, MEMORY, and MERGE).

        :param straight_join `<'bool'>`: If `True`, forces the optimizer to join the tables
            in the order in which they are listed in the FROM clause. Defaults to `False`.
            Can be used to speed up a query if the optimizer joins the tables in nonoptimal
            order.

        :param sql_buffer_result `<'bool'>`: If `True`, forces the result to be put into a
            temporary table. Defaults to `False`. This helps MySQL free the table locks early
            and helps in cases where it takes a long time to send the result set to the client.
            This modifier can be used only for top-level SELECT statements, not for subqueries
            or following UNION.
        """
        # Validate
        if tuple_len(expressions) == 0:
            self._raise_argument_error(
                "SELECT clause must specify at least one expression."
            )

        # Clause
        clause = SELECT().setup(
            self._validate_expressions(expressions, "SELECT 'expressions'"),
            distinct,
            high_priority,
            straight_join,
            sql_buffer_result,
        )
        self._clause_id = utils.DML_CLAUSE.SELECT
        return clause

    # . from
    @cython.ccall
    def _gen_from_clause(
        self,
        table: object,
        partition: object = None,
        alias: object = None,
    ) -> FROM:
        """(internal) Generate the FROM clause `<'FROM'>`.

        :param table `<'str/Table/SelectDML'>`: The table (reference) from which to retrieve data.
            Only accepts one table reference. For multiple-table, please use the explicit JOIN clause instead.
        :param partition `<'str/Partition/list/tuple/None'>`: The partition(s) of the table. Defaults to `None`.
        :param alias `<'str/None'>`: The alias of the table (reference). Defaults to `None`.
            When 'alias=None', a default alias is automatically generated based on the count
            of the tables adopting default alias in the same query block (starting from 't0').
        """
        # Validate
        pts: list = None
        if isinstance(table, SelectDML):
            tb: object = table
        else:
            tb: object = self._validate_table(table, "FROM 'table'")
            if partition is not None:
                pts = self._validate_elements(partition, "FROM 'partition'")

        # Clause
        clause = FROM().setup(
            tb, pts, self._validate_table_alias(alias, "FROM 'alias'")
        )
        self._clause_id = utils.DML_CLAUSE.FROM
        return clause

    # . join
    @cython.cfunc
    @cython.inline(True)
    def _gen_join_clause(
        self,
        join_method: str,
        table: object,
        on: tuple,
        using: object = None,
        require_condition: cython.bint = False,
        partition: object = None,
        alias: object = None,
    ) -> JOIN:
        """(internal & cfunc) The base method to generate the JOIN clause `<'JOIN'>`.

        :param join_method `<'str'>`: The join method of the join clause.
            Accepts: `"INNER JOIN"`, `"LEFT JOIN"`, `"RIGHT JOIN"`, `"CROSS JOIN"`,
            `"NATURAL INNER JOIN"`, `"NATURAL LEFT JOIN"`, `"NATURAL RIGHT JOIN"`.
        :param table `<'str/Table/SelectDML'>`: The table (reference) to join.
        :param on `<'tuple[str]'>`: The ON condition(s) specify how to join the table.
        :param using `<'str/Column/list/tuple/None'>`: The USING condition(s) of the common column(s) between the tables. Defaults to `None`.
            When on is specified, using is ignored.
        :param require_condition `<'bool'>`: Whether the join clause requires join condition. Defaults to `False`.
            When 'require_condition=True' and both 'on' and 'using' arguments are not
            specified, an DMLArgumentError is raised.
        :param partition `<'str/Partition/list/tuple/None'>`: The partition(s) of the join table. Defaults to `None`.
        :param alias `<'str/None'>`: The alias of the join table. Defaults to `None`.
            When 'alias=None', a default alias is automatically generated based on the count
            of the tables adopting default alias in the same query block (starting from 't0').
        """
        # Validate
        if join_method is None:
            self._raise_argument_error("JOIN method cannot be 'None'.")
        # . table & partition
        pts: list = None
        if isinstance(table, SelectDML):
            tb: object = table
        else:
            tb: object = self._validate_table(table, "%s 'table'" % join_method)
            if partition is not None:
                pts = self._validate_elements(partition, "%s 'partition'" % join_method)
        # . conditions
        if tuple_len(on) > 0:
            on_conditions: list = self._validate_expressions(
                on, "%s 'on'" % join_method
            )
            using_columns: list = None
        elif using is not None:
            on_conditions: list = None
            using_columns: list = self._validate_elements(
                using, "%s 'using'" % join_method
            )
        elif not require_condition:
            on_conditions: list = None
            using_columns: list = None
        else:
            self._raise_argument_error(
                "%s must specify at least one join condition." % join_method
            )

        # Clause
        # fmt: off
        clause = JOIN().setup(
            join_method, tb, pts, on_conditions, using_columns,
            self._validate_table_alias(alias, "%s 'alias'" % join_method),
        )
        # fmt: on
        self._clause_id = utils.DML_CLAUSE.JOIN
        return clause

    @cython.ccall
    def _gen_inner_join_clause(
        self,
        table: object,
        on: tuple,
        using: object = None,
        partition: object = None,
        alias: object = None,
    ) -> JOIN:
        """(internal) Generate the INNER JOIN clause `<'JOIN'>`.

        ## Parameters
        - Please refer to the '_gen_join_clause()' method for details.
        """
        return self._gen_join_clause(
            "INNER JOIN", table, on, using, False, partition, alias
        )

    @cython.ccall
    def _gen_left_join_clause(
        self,
        table: object,
        on: tuple,
        using: object = None,
        partition: object = None,
        alias: object = None,
    ) -> JOIN:
        """(internal) Generate the LEFT JOIN clause `<'JOIN'>`.

        ## Parameters
        - Please refer to the '_gen_join_clause()' method for details.
        """
        return self._gen_join_clause(
            "LEFT JOIN", table, on, using, True, partition, alias
        )

    @cython.ccall
    def _gen_right_join_clause(
        self,
        table: object,
        on: tuple,
        using: object = None,
        partition: object = None,
        alias: object = None,
    ) -> JOIN:
        """(internal) Generate the RIGHT JOIN clause `<'JOIN'>`.

        ## Parameters
        - Please refer to the '_gen_join_clause()' method for details.
        """
        return self._gen_join_clause(
            "RIGHT JOIN", table, on, using, True, partition, alias
        )

    @cython.ccall
    def _gen_straight_join_clause(
        self,
        table: object,
        on: tuple,
        using: object = None,
        partition: object = None,
        alias: object = None,
    ) -> JOIN:
        """(internal) Generate the STRAIGHT JOIN clause `<'JOIN'>`.

        ## Parameters
        - Please refer to the '_gen_join_clause()' method for details.
        """
        return self._gen_join_clause(
            "STRAIGHT_JOIN", table, on, using, False, partition, alias
        )

    @cython.ccall
    def _gen_cross_join_clause(
        self,
        table: object,
        on: tuple,
        using: object = None,
        partition: object = None,
        alias: object = None,
    ) -> JOIN:
        """(internal) Generate the CROSS JOIN clause `<'JOIN'>`.

        ## Parameters
        - Please refer to the '_gen_join_clause()' method for details.
        """
        return self._gen_join_clause(
            "CROSS JOIN", table, on, using, False, partition, alias
        )

    @cython.ccall
    def _gen_natural_join_clause(
        self,
        table: object,
        join_method: object = "INNER",
        partition: object = None,
        alias: object = None,
    ) -> JOIN:
        """(internal) Generate the NATURAL JOIN clause `<'JOIN'>`.

        :param join_method `<'str'>`: The join method of the natural join clause. Defaults to `"INNER"`.
            Accepts: `"INNER"`, `"LEFT"` and `"RIGHT"`.

        - Please refer to the '_gen_join_clause()' method for other parameter details.
        """
        try:
            join: str = utils.validate_join_method(join_method)
        except Exception as err:
            self._raise_argument_error("NATURAL " + str(err), err)
        if join is None:
            self._raise_argument_error(
                "NATURAL 'join_method' cannot be an empty string."
            )
        return self._gen_join_clause(
            "NATURAL " + join, table, (), None, False, partition, alias
        )

    # . index hints
    @cython.cfunc
    @cython.inline(True)
    def _gen_index_hints_clause(
        self,
        mode: str,
        indexes: tuple,
        scope: object = None,
    ) -> INDEX_HINTS:
        """(internal & cfunc) The base method to generate the INDEX HINTS clause `<'INDEX_HINTS'>`.

        :param mode `<'str'>`: The mode of the INDEX HINTS.
            Accepts: `"USE INDEX"`, `"IGNORE INDEX"` and `"FORCE INDEX"`.
        :param indexes `<'tuple[str/Index]'>`: The index(es) of the INDEX HINTS.
        :param scope `<'str/None'>`: The scope of the index hints (FOR [scope]). Defaults to `None`.
            Accepts: `"JOIN"`, `"ORDER BY"` and `"GROUP BY"`.
            This provides more fine-grained control over optimizer selection of an execution plan
            for various phases of query processing. To affect only the indexes used when MySQL
            decides how to find rows in the table and how to process joins, use FOR JOIN. To
            influence index usage for sorting or grouping rows, use FOR ORDER BY or FOR GROUP BY.
        """
        # Validate
        if mode is None:
            self._raise_critical_error("INDEX HINTS mode cannot be 'None'.")
        try:
            hint_scope: str = utils.validate_index_hints_scope(scope)
        except Exception as err:
            self._raise_argument_error("%s %s" % (mode, err), err)
        if tuple_len(indexes) == 0:
            self._raise_argument_error("%s must specify at least 1 index." % mode)

        # Clause
        return INDEX_HINTS().setup(
            mode, self._validate_elements(indexes, "%s 'indexes'" % mode), hint_scope
        )

    @cython.ccall
    def _gen_use_index_clause(
        self,
        indexes: tuple,
        scope: object = None,
    ) -> INDEX_HINTS:
        """(internal) Generate the USE INDEX clause `<'INDEX_HINTS'>`.

        ## Parameters
        Please refer to the '_gen_index_hints_clause()' method for details.
        """
        return self._gen_index_hints_clause("USE INDEX", indexes, scope)

    @cython.ccall
    def _gen_ignore_index_clause(
        self,
        indexes: tuple,
        scope: object = None,
    ) -> INDEX_HINTS:
        """(internal) Generate the IGNORE INDEX clause `<'INDEX_HINTS'>`.

        ## Parameters
        Please refer to the '_gen_index_hints_clause()' method for details.
        """
        return self._gen_index_hints_clause("IGNORE INDEX", indexes, scope)

    @cython.ccall
    def _gen_force_index_clause(
        self,
        indexes: tuple,
        scope: object = None,
    ) -> INDEX_HINTS:
        """(internal) Generate the FORCE INDEX clause `<'INDEX_HINTS'>`.

        ## Parameters
        Please refer to the '_gen_index_hints_clause()' method for details.
        """
        return self._gen_index_hints_clause("FORCE INDEX", indexes, scope)

    # . where
    @cython.ccall
    def _gen_where_clause(
        self,
        conds: tuple,
        in_conds: dict | None = None,
        not_in_conds: dict | None = None,
    ) -> WHERE:
        """(internal) Generate the WHERE clause `<'WHERE'>`.

        :param conds `<'tuple[str]'>`: The condition(s) that rows must satisfy to be selected.
        :param in_conds `<'dict/None'>`: The IN condition(s). Defaults to `None`.
        :param not_in_conds `<'dict/None'>`: The NOT IN condition(s). Defaults to `None`.

        ## Explanation
        - All conditions are combined with `AND` logic by default.
        - Prepend the condition with "`OR `" to alter the default behavior.
        - The 'in_conds' and 'not_in_conds' arguments must be a dictionary,
          where the keys should be the columns and values the desired ones
          to check against. Values can be any escapable objects or a subquery
          `<'SelectDML'>` instance, but `NOT` strings with placeholders.
        """
        # Validate
        # fmt: off
        conds_count: cython.Py_ssize_t = tuple_len(conds)
        if conds_count == 0:
            conditions: list = None
        else:
            conditions: list = self._validate_expressions(conds, "WHERE 'conds'")
        if in_conds is None:
            in_conds: dict = None
            in_conds_count: cython.Py_ssize_t = 0
        else:
            in_conds: dict = self._validate_in_conditions(in_conds, "WHERE 'in_conds'")
            in_conds_count: cython.Py_ssize_t = dict_len(in_conds)
        if not_in_conds is None:
            not_in_conds: dict = None
            not_in_conds_count: cython.Py_ssize_t = 0
        else:
            not_in_conds: dict = self._validate_in_conditions(not_in_conds, "WHERE 'not_in_conds'")
            not_in_conds_count: cython.Py_ssize_t = dict_len(not_in_conds)
        # fmt: on
        if conds_count + not_in_conds_count + in_conds_count == 0:
            self._raise_argument_error(
                "WHERE clause must specify at least one condition."
            )

        # Clause
        clause = WHERE().setup(conditions, in_conds, not_in_conds)
        self._clause_id = utils.DML_CLAUSE.WHERE
        return clause

    # . group by
    @cython.ccall
    def _gen_group_by_clause(
        self,
        columns: tuple,
        with_rollup: cython.bint = False,
    ) -> GROUP_BY:
        """(internal) Generate the GROUP BY clause `<'GROUP_BY'>`.

        :param columns `<'tuple[str/Column]'>`: The (expression of) column(s) to group by with.
        :param with_rollup `<'bool'>`: Whether to summary output to include extra rows
            that represent higher-level (super-aggregate) grand total.
        """
        # Validate
        if tuple_len(columns) == 0:
            self._raise_argument_error(
                "GROUP BY clause must specify at least one column or expression."
            )

        # Clause
        clause = GROUP_BY().setup(
            self._validate_expressions(columns, "GROUP BY 'columns'"),
            with_rollup,
        )
        self._clause_id = utils.DML_CLAUSE.GROUP_BY
        return clause

    # . having
    @cython.ccall
    def _gen_having_clause(
        self,
        conds: tuple,
        in_conds: dict | None = None,
        not_in_conds: dict | None = None,
    ) -> HAVING:
        """(internal) Generate the HAVING clause `<'HAVING'>`.

        :param conds `<'tuple[str]'>`: The condition(s) that rows must satisfy to be selected.
        :param in_conds `<'dict/None'>`: The IN condition(s). Defaults to `None`.
        :param not_in_conds `<'dict/None'>`: The NOT IN condition(s). Defaults to `None`.

        ## Explanation
        - All conditions are combined with `AND` logic by default.
        - Prepend the condition with "`OR `" to alter the default behavior.
        - The 'in_conds' and 'not_in_conds' arguments must be a dictionary,
          where the keys should be the columns and values the desired ones
          to check against. Values can be any escapable objects or a subquery
          `<'SelectDML'>` instance, but `NOT` strings with placeholders.
        """
        # fmt: off
        conds_count: cython.Py_ssize_t = tuple_len(conds)
        if conds_count == 0:
            conditions: list = None
        else:
            conditions: list = self._validate_expressions(conds, "HAVING 'conds'")
        if in_conds is None:
            in_conds: dict = None
            in_conds_count: cython.Py_ssize_t = 0
        else:
            in_conds: dict = self._validate_in_conditions(in_conds, "HAVING 'in_conds'")
            in_conds_count: cython.Py_ssize_t = dict_len(in_conds)
        if not_in_conds is None:
            not_in_conds: dict = None
            not_in_conds_count: cython.Py_ssize_t = 0
        else:
            not_in_conds: dict = self._validate_in_conditions(not_in_conds, "HAVING 'not_in_conds'")
            not_in_conds_count: cython.Py_ssize_t = dict_len(not_in_conds)
        # fmt: on
        if conds_count + not_in_conds_count + in_conds_count == 0:
            self._raise_argument_error(
                "HAVING clause must specify at least one condition."
            )

        # Clause
        clause = HAVING().setup(conditions, in_conds, not_in_conds)
        self._clause_id = utils.DML_CLAUSE.HAVING
        return clause

    # . window
    @cython.ccall
    def _gen_window_clause(
        self,
        name: object,
        partition_by: object | None = None,
        order_by: object | None = None,
        frame_clause: str | None = None,
    ) -> WINDOW:
        """(internal) Generate the WINDOW clause `<'WINDOW'>`.

        :param name `<'str'>`: The name of the window.

        :param partition_by `<'str/Column/list/tuple/None'>`: Specifies how to divide the query rows into groups. Defaults to `None`.
            The window function result for a given row is based on the rows of the partition
            that contains the row. If 'partition_by=None', there is a single partition
            consisting of all query rows.

        :param order_by `<'str/Column/list/tuple/None'>`: Specifies how to sort rows in each partition. Defaults to `None`.
            Partition rows that are equal according to the ORDER BY clause are considered peers.
            If 'order_by=None', partition rows are unordered, with no processing order implied,
            and all partition rows are peers.

        :param frame_clause `<'str/None'>`: Specifies how to define the frame (subset of the current partition). Defaults to `None`.
            Frames are determined with respect to the current row, which enables a frame to move
            within a partition depending on the location of the current row within its partition.

            Examples: By defining a frame to be all rows from the partition start to the current
            row, you can compute running totals for each row. By defining a frame as extending N
            rows on either side of the current row, you can compute rolling averages.

            For more information, please refer to the MySQL documentation
            [Section 14.20.3, "Window Function Frame Specification"](https://dev.mysql.com/doc/refman/8.0/en/window-function-descriptions.html).
        """
        # Validate
        opts = []
        if partition_by is not None:
            pts_by = self._validate_expressions(partition_by, "WINDOW 'partition_by'")
            if list_len(pts_by) > 0:
                opts.append("PARTITION BY " + ", ".join(pts_by))
        if order_by is not None:
            ord_by = self._validate_expressions(order_by, "WINDOW 'order_by'")
            if list_len(ord_by) > 0:
                opts.append("ORDER BY " + ", ".join(ord_by))
        if frame_clause is not None:
            opts.append(
                self._validate_expression(frame_clause, "WINDOW 'frame_clause'")
            )

        # Clause
        clause = WINDOW().setup(self._validate_element(name, "WINDOW 'name'"), opts)
        self._clause_id = utils.DML_CLAUSE.WINDOW
        return clause

    # . set operations
    @cython.ccall
    def _gen_union_clause(
        self,
        subquery: object,
        all: cython.bint = False,
    ) -> SET_OPERATION:
        """(internal) Generate the UNION clause `<'SET_OPERATION'>`.

        `UNION` (SET OPERATION) combines the result from multiple
        query blocks into a single result set.

        :param subquery `<'SelectDML'>`: The subquery to union with the main query block.
        :param all `<'bool'>`: Keep all the result if `True`, else sort & deduplicate the result set. Defaults to `False`.
        """
        # Clause
        clause = SET_OPERATION().setup(
            "UNION", self._validate_subquery(subquery, "UNION"), all
        )
        self._clause_id = utils.DML_CLAUSE.SET_OPERATION
        return clause

    @cython.ccall
    def _gen_intersect_clause(
        self,
        subquery: object,
        all: cython.bint = False,
    ) -> SET_OPERATION:
        """(internal) Generate the INTERSECT clause `<'SET_OPERATION'>`.

        `INTERSECT` (SET OPERATION) limits the result from multiple
        query blocks to those rows which are common to all.

        :param subquery `<'SelectDML'>`: The subquery to intersect with the main query block.
        :param all `<'bool'>`: Keep all the result if `True`, else sort & deduplicate the result set. Defaults to `False`.
        """
        # Clause
        clause = SET_OPERATION().setup(
            "INTERSECT", self._validate_subquery(subquery, "INTERSECT"), all
        )
        self._clause_id = utils.DML_CLAUSE.SET_OPERATION
        return clause

    @cython.ccall
    def _gen_except_clause(
        self,
        subquery: object,
        all: cython.bint = False,
    ) -> SET_OPERATION:
        """(internal) Generate the EXCEPT clause `<'SET_OPERATION'>`.

        `EXCEPT` (SET OPERATION) limits the result from the first query
        block to those rows which are (also) not found in the second.

        :param subquery `<'SelectDML'>`: The subquery to except with the main query block.
        :param all `<'bool'>`: Keep all the result if `True`, else sort & deduplicate the result set. Defaults to `False`.
        """
        # Clause
        clause = SET_OPERATION().setup(
            "EXCEPT", self._validate_subquery(subquery, "EXCEPT"), all
        )
        self._clause_id = utils.DML_CLAUSE.SET_OPERATION
        return clause

    # . order by
    @cython.ccall
    def _gen_order_by_clause(
        self,
        columns: tuple,
        with_rollup: cython.bint = False,
    ) -> ORDER_BY:
        """(internal) Generate the ORDER BY clause `<'ORDER_BY'>`.

        :param columns `<'tuple[str/Column]'>`: The ordering (expression of) column(s).
            Each element can be a column name or any SQL expression,
            optionally suffixed with 'ASC' or 'DESC'.
        :param with_rollup `<'bool'>`: Whether to append the `WITH ROLLUP` modifier. Defaults to `False`.
            When 'with_rollup=True', any super-aggregate rows produced (e.g. by a preceding
            `GROUP BY … WITH ROLLUP`) are included in this sort; those summary rows participate
            in the same `ORDER BY`, appearing last under `ASC` or first under `DESC`.

            Only supported by MySQL 8.0.12+.
        """
        # Validate
        if tuple_len(columns) == 0:
            self._raise_argument_error(
                "ORDER BY clause must specify at least one expression."
            )

        # Clause
        clause = ORDER_BY().setup(
            self._validate_expressions(columns, "ORDER BY 'columns'"),
            with_rollup,
        )
        self._clause_id = utils.DML_CLAUSE.ORDER_BY
        return clause

    # . limit
    @cython.ccall
    def _gen_limit_clause(self, row_count: object, offset: object = None) -> LIMIT:
        """(internal) Generate the LIMIT clause `<'LIMIT'>`.

        :param row_count `<'int'>`: The number of limited rows to return.
        :param offset `<'int/None'>`: The offset to the first row. Defaults to `None`.
        """
        # Validate
        try:
            rows_int: cython.ulonglong = int(row_count)
        except Exception as err:
            self._raise_argument_error(
                "LIMIT row_count (%s %r) is invalid." % (type(row_count), row_count),
                err,
            )
        if offset is not None:
            try:
                offs_int: cython.ulonglong = int(offset)
            except Exception as err:
                self._raise_argument_error(
                    "LIMIT offset (%s %r) is invalid." % (type(offset), offset), err
                )
        else:
            offs_int: cython.ulonglong = 0

        # Clause
        clause = LIMIT().setup(rows_int, offs_int)
        self._clause_id = utils.DML_CLAUSE.LIMIT
        return clause

    # . locking reads
    @cython.cfunc
    @cython.inline(True)
    def _gen_locking_reads_clause(
        self,
        mode: str,
        tables: tuple,
        option: object = None,
    ) -> LOCKING_READS:
        """(internal & cfunc) The base method to generate the LOCKING READS clause `<'LOCKING_READS'>`.

        :param mode `<'str'>`: The locking reads mode.
            Accepts: `"FOR UPDATE"` and `"FOR SHARE"`.
        :param tables `<'tuple[str/Table]'>`: The specific table(s) to lock.
            If omitted, all tables in the query are locked.
        :param option `<'str/None'>`: The option of the lock. Defaults to `None`. Accepts:
            - `"NOWAIT"`: Never waits to acquire a row lock. The query executes immediately,
              failing with an error if a requested row is locked.
            - `"SKIP LOCKED"`: Never waits to acquire a row lock. The query executes immediately,
              removing locked rows from the result set.
        """
        # Validate
        if mode is None:
            self._raise_critical_error("LOCKING READS mode cannot be 'None'.")
        if tuple_len(tables) == 0:
            tbs: list = None
        else:
            tbs: list = self._validate_tables(tables, "%s 'tables'" % mode)
        try:
            opt: str = utils.validate_locking_reads_option(option)
        except Exception as err:
            self._raise_argument_error("%s %s" % (mode, err), err)

        # Clause
        clause = LOCKING_READS().setup(mode, tbs, opt)
        self._clause_id = utils.DML_CLAUSE.LOCKING_READS
        return clause

    @cython.ccall
    def _gen_for_update_clause(
        self,
        tables: tuple,
        option: object = None,
    ) -> LOCKING_READS:
        """(internal) Generate the FOR UPDATE clause `<'LOCKING_READS'>`.

        :param tables `<'tuple[str/Table]'>`: The specific table(s) to lock FOR UPDATE.
            If omitted, all tables in the query are locked FOR UPDATE.
        :param option `<'str/None'>`: The option of the FOR UPDATE lock. Defaults to `None`. Accepts:
            - `"NOWAIT"`: Never waits to acquire a row lock. The query executes immediately,
              failing with an error if a requested row is locked.
            - `"SKIP LOCKED"`: Never waits to acquire a row lock. The query executes immediately,
              removing locked rows from the result set.

        ## Explanation
        - Does `NOT` block non-locking reads (plain SELECT under MVCC).
        - Blocks other transactions from obtaining exclusive locks (e.g., INSERT, UPDATE or DELETE).
        - Blocks other transactions from obtaining both the FOR SHARE and FOR UPDATE locks.
        - Therefore, no other transaction can modify or do a locking reads on the rows until
          the transaction commits.
        - Use when you plan to read and then update the same rows, and you must avoid lost-update
          or counter anomalies.
        """
        return self._gen_locking_reads_clause("FOR UPDATE", tables, option)

    @cython.ccall
    def _gen_for_share_clause(
        self,
        tables: tuple,
        option: object = None,
    ) -> LOCKING_READS:
        """(internal) Generate the FOR SHARE clause `<'LOCKING_READS'>`.

        :param tables `<'tuple[str/Table]'>`: The specific table(s) to lock FOR SHARE.
            If omitted, all tables in the query are locked FOR SHARE.
        :param option `<'str/None'>`: The option of the FOR SHARE lock. Defaults to `None`. Accepts:
            - `"NOWAIT"`: Never waits to acquire a row lock. The query executes immediately,
              failing with an error if a requested row is locked.
            - `"SKIP LOCKED"`: Never waits to acquire a row lock. The query executes immediately,
              removing locked rows from the result set.

        ## Explanation
        - Does `NOT` block non-locking reads (plain SELECT under MVCC).
        - Blocks other transactions from obtaining exclusive locks (e.g., INSERT, UPDATE or DELETE).
        - Allows other transactions to also acquire FOR SHARE locks on the same rows,
          for multiple concurrent readers.
        - Use when you need to read data and ensure it cannot be changed by others before
          your transaction ends, but you do not intend to modify it yourself.
        """
        return self._gen_locking_reads_clause("FOR SHARE", tables, option)

    # . into (variables)
    @cython.ccall
    def _gen_into_variables_clause(self, variables: tuple) -> INTO_VARIABLES:
        """(internal) Generate the INTO (variables) clause `<'INTO_VARIABLES'>`.

        :param variables `<'tuple[str]'>`: The variable(s) to store the result set.
            Each variables can be a user-defined variable, stored procedure or function parameter,
            or stored program local variable. (Within a prepared SELECT ... INTO var_list statement, only
            user-defined variables are permitted. For more information, please refer to MySQL documentation
            [Section 15.6.4.2, "Local Variable Scope and Resolution"](https://dev.mysql.com/doc/refman/8.4/en/local-variable-scope.html).

            The selected values are assigned to the variables. The number of variables must match
            the number of columns. The query should return a single row. If the query returns no rows,
            a warning with error code 1329 occurs (No data), and the variable values remain unchanged.
            If the query returns multiple rows, error 1172 occurs
        """
        # Validate
        if tuple_len(variables) == 0:
            self._raise_argument_error(
                "INTO (variables) clause must specify at least one variable."
            )

        # Clause
        clause = INTO_VARIABLES().setup(
            self._validate_elements(variables, "INTO 'variables'")
        )
        self._clause_id = utils.DML_CLAUSE.INTO
        return clause

    # INSERT Clause ------------------------------------------------------------------------
    # . insert
    @cython.ccall
    def _gen_insert_clause(
        self,
        table: object,
        partition: object = None,
        ignore: cython.bint = False,
        priority: object = None,
    ) -> INSERT:
        """(internal) Generate the INSERT clause `<'INSERT'>`.

        :param table `<'str/Table'>`: The table to insert the data.

        :param partition `<'str/Partition/list/tuple/None'>`: The partition(s) of the table. Defaults to `None`.

        :param ignore `<'bool'>`: Whether to ignore the (ignorable) errors. Defaults to `False`.
            When 'ignore=True', ignorable errors—such as duplicate-key or primary-key violations,
            unmatched partitions, and data conversion issues—are converted to warnings. Rows
            causing these errors are skipped or adjusted rather than aborting the statement.

        :param priority `<'str/None'>`: Optional INSERT prioirty modifier. Defaults to `None`.
            Only applies to table-locking engines (MyISAM, MEMORY, MERGE). Accepts:

            - `"LOW_PRIORITY"`: Delays the INSERT until no other clients are reading the table
                (even those who start reading while your insert is waiting). Disables concurrent
                inserts—so it can block for a very long time and is normally not recommended on
                MyISAM tables.
            - `"HIGH_PRIORITY"`: Overrides any server setting that forces low-priority updates
                and likewise disables concurrent inserts.
        """
        # Validate
        tb: str = self._validate_table(table, "INSERT 'table'")
        if partition is not None:
            pts: list = self._validate_elements(partition, "INSERT 'partition'")
        else:
            pts: list = None
        try:
            pri: str = utils.validate_insert_priority(priority)
        except Exception as err:
            self._raise_argument_error(str(err), err)

        # Clause
        clause = INSERT().setup(tb, pts, ignore, pri)
        self._clause_id = utils.DML_CLAUSE.INSERT
        return clause

    @cython.ccall
    def _gen_insert_columns_clause(self, columns: tuple) -> INSERT_COLUMNS:
        """(internal) Generate the INSERT COLUMNS clause `<'INSERT_COLUMNS'>`.

        :param columns `<'tuple[str/Column]'>`: The column(s) to insert the data.
        """
        # Validate
        if tuple_len(columns) == 0:
            self._raise_argument_error(
                "COLUMNS clause must specify at least one column."
            )

        # Clause
        clause = INSERT_COLUMNS().setup(
            self._validate_elements(columns, "INSERT 'columns'")
        )
        self._clause_id = utils.DML_CLAUSE.INSERT_COLUMNS
        return clause

    @cython.ccall
    def _gen_insert_values_clause(self, placeholders: cython.int) -> INSERT_VALUES:
        """(internal) Generate the INSERT VALUES clause `<'INSERT_VALUES'>`.

        :param placeholders `<'int'>`: The number of placeholders for each row of the insert data.
            If the `Columns()` method is used by the INSERT statement, the number of placeholders
            must match the specified columns. Otherwise, the number of placeholders must match
            the number of columns in the target insert table.
        """
        # Validate
        if placeholders <= 0:
            self._raise_argument_error(
                "VALUES 'placeholders' must be an integer greater than 0, "
                "instead got %d." % placeholders
            )

        # Clause
        clause = INSERT_VALUES().setup(placeholders)
        self._clause_id = utils.DML_CLAUSE.INSERT_VALUES
        return clause

    @cython.ccall
    def _gen_set_clause(self, assignments: tuple) -> SET:
        """(internal) Generate the SET clause `<'SET'>`.

        :param assignments `<'tuple[str]'>`: The assignment(s) on how to set the data.
        """
        # Validate
        if tuple_len(assignments) == 0:
            self._raise_argument_error(
                "SET clause must specify at least one assignment."
            )

        # Clause
        clause = SET().setup(
            self._validate_expressions(assignments, "SET 'assignments'")
        )
        self._clause_id = utils.DML_CLAUSE.SET
        return clause

    @cython.ccall
    def _gen_row_alias_clause(
        self,
        row_alias: object,
        col_alias: tuple,
    ) -> ROW_ALIAS:
        """(internal) Generate the INSERT ROW ALIAS clause `<'ROW_ALIAS'>`.

        :param row_alias `<'str'>`: The alias of the insert row.
        :param col_alias `<'tuple[str]'>`: The alias of the column(s) in the insert row.
        """
        # Validate
        if len(col_alias) > 0:
            column_alias: list = [
                self._validate_element(i, "INSERT 'col_alias'") for i in col_alias
            ]
        else:
            column_alias: list = None

        # Clause
        return ROW_ALIAS().setup(
            self._validate_element(row_alias, "INSERT 'row_alias'"), column_alias
        )

    @cython.ccall
    def _gen_on_duplicate_clause(self, assignments: tuple) -> ON_DUPLICATE:
        """(internal) Generate the ON DUPLICATE KEY UPDATE clause `<'ON_DUPLICATE'>`.

        :param assignments `<'tuple[str]'>`: The assignment(s) on how to update the duplicated rows.
        """
        # Validate
        if tuple_len(assignments) == 0:
            self._raise_argument_error(
                "ON DUPLICATE KEY UPDATE clause must specify at least one assignment."
            )

        # Clause
        clause = ON_DUPLICATE().setup(
            self._validate_expressions(assignments, "ON DUPLICATE KEY 'assignments'")
        )
        self._clause_id = utils.DML_CLAUSE.ON_DUPLICATE
        return clause

    # UPDATE Clause ------------------------------------------------------------------------
    # . update
    @cython.ccall
    def _gen_update_clause(
        self,
        table: object,
        partition: object = None,
        ignore: cython.bint = False,
        low_priority: cython.bint = False,
        alias: object = None,
    ) -> UPDATE:
        """(internal) Generate the UPDATE clause `<'UPDATE'>`.

        :param table `<'str/Table/SelectDML'>`: The table (reference) from which to update data.
            Only accepts one table. For multiple-table, please use the explicit JOIN clause instead.

        :param partition `<'str/Partition/list/tuple/None'>`: The partition(s) of the table. Defaults to `None`.

        :param ignore `<'bool'>`: Whether to ignore the (ignorable) errors. Defaults to `False`.
            When 'ignore=True', the update statement does not abort even if errors occur during
            the update. Rows for which duplicate-key conflicts occur on a unique key value are
            not updated. Rows updated to values that would cause data conversion errors are
            updated to the closest valid values instead.

        :param low_priority `<'bool'>`: Whether to enable the optional `LOW_PRIORITY` modifier. Defaults to `False`.
            When 'low_priority=True', execution of the UPDATE is delayed until no other
            clients are reading from the table. This affects only storage engines that
            use only table-level locking (such as MyISAM, MEMORY, and MERGE).

        :param alias `<'str/None'>`: The alias of the table (reference). Defaults to `None`.
            When 'alias=None', a default alias is automatically generated based on the count
            of the tables adopting default alias in the same query block (starting from 't0').
        """
        # Validate
        pts: list = None
        if isinstance(table, SelectDML):
            tb: object = table
        else:
            tb: object = self._validate_table(table, "UPDATE 'table'")
            if partition is not None:
                pts = self._validate_elements(partition, "UPDATE 'partition'")

        # Clause
        # fmt: off
        clause = UPDATE().setup(
            tb, pts, ignore, low_priority,
            self._validate_table_alias(alias, "UPDATE 'alias'"),
        )
        # fmt: on
        self._clause_id = utils.DML_CLAUSE.UPDATE
        return clause

    # DELETE Clause ------------------------------------------------------------------------
    # . delete
    @cython.ccall
    def _gen_delete_clause(
        self,
        table: object,
        partition: object = None,
        ignore: cython.bint = False,
        low_priority: cython.bint = False,
        quick: cython.bint = False,
        alias: object = None,
        multi_tables: object = None,
    ) -> DELETE:
        """(internal) Generate the DELETE clause `<'DELETE'>`.

        :param table `<'str/Table/SelectDML'>`: The table (reference) from which to delete data.
            Only accepts one table. For multiple-table, please use the explicit JOIN clause instead.

        :param partition `<'str/Partition/list/tuple/None'>`: The partition(s) of the table. Defaults to `None`.

        :param ignore `<'bool'>`: Whether to ignore the (ignorable) errors. Defaults to `False`.
            When 'ignore=True', causes MySQL to ignore errors during the process of deleting rows.

        :param low_priority `<'bool'>`: Whether to enable the optional `LOW_PRIORITY` modifier. Defaults to `False`.
            When 'low_priority=True', the server delays execution of the DELETE until no
            other clients are reading from the table. This affects only storage engines
            that use only table-level locking (such as MyISAM, MEMORY, and MERGE).

        :param quick `<'bool'>`: Whether to enable the optional `QUICK` modifier. Defaults to `False`.
            When 'quick=True', MyISAM storage engine does not merge index leaves during
            delete, which may speed up some kinds of delete operations.

        :param alias `<'str/None'>`: The alias of the table (reference). Defaults to `None`.
            When 'alias=None', a default alias is automatically generated based on the count
            of the tables adopting default alias in the same query block (starting from 't0').

        :param multi_tables `<'str/list/tuple/None'>`: The the table alias(es) for multi-table delete. Defaults to `None`.
            This argument should be used in combination with the `JOIN` clauses. Only
            the data of the table(s) specified in this argument will be deleted for
            multi-table DELETE operation when the condition is met.
        """
        # Validate
        pts: list = None
        if isinstance(table, SelectDML):
            tb: object = table
        else:
            tb: object = self._validate_table(table, "DELETE 'table'")
            if partition is not None:
                pts = self._validate_elements(partition, "DELETE 'partition'")
        if multi_tables is not None:
            multi_tbs: list = self._validate_elements(
                multi_tables, "DELETE 'multi_tables'"
            )
        else:
            multi_tbs: list = None

        # Clause
        # fmt: off
        clause = DELETE().setup(
            tb, pts, ignore, low_priority, quick,
            self._validate_table_alias(alias, "DELETE 'alias'"), multi_tbs,
        )
        # fmt: on
        self._clause_id = utils.DML_CLAUSE.DELETE
        return clause

    # WITH Clause --------------------------------------------------------------------------
    # . with
    @cython.ccall
    def _gen_with_clause(
        self,
        name: object,
        subquery: object,
        columns: tuple,
        recursive: cython.bint = False,
    ) -> WITH:
        """(internal) Generate the WITH (Common Table Expressions) clause `<'WITH'>`.

        :param name `<'str'>`: The name of the CTE, which can be used as a table reference in the statement.
        :param subquery `<'str/SelectDML'>`: The subquery that produces the CTE result set.
        :param columns `<'tuple[str]'>`: The column names of the result set.
            If specified, the number of columns must be the same as the result set of the subquery.
        :param recursive `<'bool'>`: Whether the CTE is recursive. Defaults to `False`.
            A CTE is recursive if its subquery refers to its own name.
            For more information, refer to MySQL documentation
            ["Recursive Common Table Expressions"](https://dev.mysql.com/doc/refman/8.4/en/with.html#common-table-expressions-recursive).
        """
        if isinstance(subquery, SelectDML) or (
            isinstance(subquery, str) and str_len(subquery) > 0
        ):
            pass
        else:
            self._raise_argument_error(
                "WITH clause subquery (%s %r) is invalid." % (type(subquery), subquery)
            )
        clause = WITH().setup(
            self._validate_element(name, "WITH clause CTE"),
            subquery,
            self._validate_elements(columns, "WITH clause CTE columns"),
            recursive,
        )
        self._clause_id = utils.DML_CLAUSE.WITH
        return clause

    # Statement ----------------------------------------------------------------------------
    @cython.ccall
    def statement(self, indent: cython.int = 0) -> str:
        """Compose the DML statement `<'str'>`.

        :param indent `<'int'>`: The indentation level of the statement. Defaults to 0.
        """
        self._raise_not_implemented_error("statement")

    @cython.cfunc
    @cython.inline(True)
    def _gen_select_statement(self, pad: str = None) -> str:
        """(internal) Generate the SELECT statement `<'str'>`.

        :param pad `<'str/None'>`: The indentation padding. Defaults to `None`.
        """
        # . with
        i: CLAUSE
        if self._with_clauses is not None:
            if list_len(self._with_clauses) == 1:
                i = self._with_clauses[0]
                clauses: list = [i.clause(pad)]
            else:
                l = [i.clause(pad) for i in self._with_clauses]
                clauses: list = [",\n".join(l)]
        else:
            clauses: list = []

        # . select
        if self._select_clause is None:
            self._raise_clause_error("SELECT clause is not set.")
        clauses.append(self._select_clause.clause(pad))

        # . from
        if self._from_clause is not None:
            clauses.append(self._from_clause.clause(pad))

        # . join
        if self._join_clauses is not None:
            for i in self._join_clauses:
                clauses.append(i.clause(pad))

        # . where
        if self._where_clause is not None:
            clauses.append(self._where_clause.clause(pad))

        # . group by
        if self._group_by_clause is not None:
            clauses.append(self._group_by_clause.clause(pad))

        # . having
        if self._having_clause is not None:
            clauses.append(self._having_clause.clause(pad))

        # . window
        if self._window_clauses is not None:
            if list_len(self._window_clauses) == 1:
                i = self._window_clauses[0]
                clauses.append(i.clause(pad))
            else:
                l = [i.clause(pad) for i in self._window_clauses]
                clauses.append(", ".join(l))

        # . set operation
        if self._set_op_clauses is not None:
            for i in self._set_op_clauses:
                clauses.append(i.clause(pad))

        # . order by
        if self._order_by_clause is not None:
            clauses.append(self._order_by_clause.clause(pad))

        # . limit
        if self._limit_clause is not None:
            clauses.append(self._limit_clause.clause(pad))

        # . locking reads
        if self._locking_reads_clauses is not None:
            for i in self._locking_reads_clauses:
                clauses.append(i.clause(pad))

        # . into
        if self._into_clause is not None:
            clauses.append(self._into_clause.clause(pad))

        # Compose
        return "\n".join(clauses)

    @cython.cfunc
    @cython.inline(True)
    def _gen_select_subquery(self, pad: str = None) -> str:
        """(internal) Generate the SELECT statement as a subquery `<'str'>`.

        :param pad `<'str/None'>`: The indentation padding. Defaults to `None`.

        ## Explanation
        This method wraps the standard SELECT statement in parentheses
        and applies optional line indentation for use as a subquery.
        """
        if pad is None:
            return "(\n%s\n)" % self._gen_select_statement("\t")
        else:
            return "(\n%s\n%s)" % (self._gen_select_statement(pad + "\t"), pad)

    # Execute ------------------------------------------------------------------------------
    @cython.ccall
    def _Execute(
        self,
        args: object = None,
        cursor: type | None = None,
        fetch: cython.bint = True,
        many: cython.bint = False,
        conn: object | None = None,
    ) -> object:
        """(internal) [sync] Execute the DML statement.

        :param args `<'list/tuple/DataFrame/Any'>`: The arguments for the placeholders in the statement. Defaults to `None`.

        :param cursor `<'type[Cursor]/None'>`: The cursor type (class) to use. Defaults to `None` (use pool default).
            Determines the data type of the fetched result set after executing the statement.
            Also accepts:
            1. `tuple` => `Cursor`;
            2. `dict` => `DictCursor`;
            3. `DataFrame` => `DfCursor`;

        :param fetch `<'bool'>`: Whether to fetch the result set. Defaults to `True`.
            If 'fetch=False', the statement will be executed but no results will be fetched.
            Instead returns the number of affected rows.

        :param many `<'bool'>`: Whether the 'args' (data) is multi-rows. Determines how the data is escaped. Defaults to `False`.
            For a single-row data, set 'many=False'. For a multi-row data, set 'many=True'.

        :param conn `<'PoolSyncConnection/None'>`: The specific [sync] connection to execute the statement. Defaults to `None`.
            If 'conn=None', the statement will be executed by a (random) connection
            from the pool, and commit automatically after the statement execution.

        :returns `<'tuple[tuple]/tuple[dict]/DataFrame/int'>`: The result set of the statement.
            Returns the number of affected rows `<'int'>` only when 'fetch=False'.
        """
        # Compose statement
        stmt: str = self.statement()

        # Connection specified
        if self._sync_conn is not None:
            conn = self._sync_conn
        if conn is not None:
            if not isinstance(conn, PoolSyncConnection):
                self._raise_argument_error(
                    "'Execute' method only accept <'PoolSyncConnection'> "
                    "(sync connection) as the 'conn', instead got %s" % type(conn),
                )
            if self._multi_table:
                conn.select_database(self._db_name)
            with conn.cursor(cursor) as cur:
                rows = cur.execute(stmt, args, many)
                return cur.fetchall() if fetch else rows

        # Validate connection requirement
        if self._require_conn:
            self._raise_critical_error(
                "must specify a [sync] connection to execute the statement."
            )

        # Connection from pool
        with self._pool.acquire() as conn:
            if self._multi_table:
                conn.select_database(self._db_name)
            with conn.transaction() as cur:
                rows = cur.execute(stmt, args, many)
                return cur.fetchall() if fetch else rows

    async def _aioExecute(
        self,
        args: object = None,
        cursor: type | None = None,
        fetch: cython.bint = True,
        many: cython.bint = False,
        conn: object | None = None,
    ) -> object:
        """(internal) [async] Execute the DML statement.

        :param args `<'list/tuple/DataFrame/Any'>`: The arguments for the placeholders in the statement. Defaults to `None`.

        :param cursor `<'type[Cursor]/None'>`: The cursor type (class) to use. Defaults to `None` (use pool default).
            Determines the data type of the fetched result set after executing the statement.
            Also accepts:
            1. `tuple` => `Cursor`;
            2. `dict` => `DictCursor`;
            3. `DataFrame` => `DfCursor`;

        :param fetch `<'bool'>`: Whether to fetch the result set. Defaults to `True`.
            If 'fetch=False', the statement will be executed but no results will be fetched.
            Instead returns the number of affected rows.

        :param many `<'bool'>`: Whether the 'args' (data) is multi-rows. Determines how the data is escaped. Defaults to `False`.
            For a single-row data, set 'many=False'. For a multi-row data, set 'many=True'.

        :param conn `<'PoolConnection/None'>`: The specific [async] connection to execute the statement. Defaults to `None`.
            If 'conn=None', the statement will be executed by a (random) connection
            from the pool, and commit automatically after the statement execution.

        :returns `<'tuple[tuple]/tuple[dict]/DataFrame/int'>`: The result set of the statement.
            Returns the number of affected rows `<'int'>` only when 'fetch=False'.
        """
        # Compose statement
        stmt: str = self.statement()

        # Connection specified
        if self._async_conn is not None:
            conn = self._async_conn
        if conn is not None:
            if not isinstance(conn, PoolConnection):
                self._raise_argument_error(
                    "'aioExecute' method only accept <'PoolConnection'> "
                    "(async connection) as the 'conn', instead got %s" % type(conn),
                )
            if self._multi_table:
                await conn.select_database(self._db_name)
            async with conn.cursor(cursor) as cur:
                rows = await cur.execute(stmt, args, many)
                return await cur.fetchall() if fetch else rows

        # Validate connection requirement
        if self._require_conn:
            self._raise_critical_error(
                "must specify an [async] connection to execute the statement."
            )

        # Connection from pool
        # . no arguments / single-row
        if args is None or not many:
            async with self._pool.acquire() as conn:
                if self._multi_table:
                    await conn.select_database(self._db_name)
                async with conn.transaction(cursor) as cur:
                    rows = await cur.execute(stmt, args, many)
                    return await cur.fetchall() if fetch else rows

        # <-> single-row
        args_escaped = _escape(args, many, True)
        if not isinstance(args_escaped, list):
            stmt = _format_sql(stmt, args_escaped)
            return await self._aioExecuteStatement(stmt)

        # <=> multi-rows
        arguments: list = args_escaped
        tasks = [self._aioExecuteStatement(_format_sql(stmt, arg)) for arg in arguments]
        return sum(await _aio_gather(*tasks))

    async def _aioExecuteStatement(self, stmt: str) -> int:
        """(internal) [async] Execute a fully formated SQL statement
        with a random [async] connection from the pool `<'int'>`.

        :param stmt `<'str'>`: The fully formated SQL statement to be executed.
        :returns `<'int'>`: Number of affected rows.
        """
        async with self._pool.acquire() as conn:
            if self._multi_table:
                await conn.select_database(self._db_name)
            async with conn.transaction() as cur:
                return await cur.execute(stmt)

    # Validate -----------------------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    def _validate_element_name(self, name: str, msg: str) -> str:
        """(internal) Validate the name of an element `<'str'>`."""
        size: cython.Py_ssize_t = str_len(name)
        if size == 0:
            self._raise_argument_error("%s can not be an empty string." % msg)
        if size > utils.SCHEMA_ELEMENT_MAX_NAME_LENGTH:
            self._raise_argument_error(
                "%s cannot exceed %d characters."
                % (msg, utils.SCHEMA_ELEMENT_MAX_NAME_LENGTH)
            )
        return name

    @cython.cfunc
    @cython.inline(True)
    def _validate_element(self, element: object, msg: str) -> str:
        """(internal) Validate the element and return its name `<'str'>`."""
        if isinstance(element, str):
            return self._validate_element_name(element, msg)
        if isinstance(element, Element) and not isinstance(element, Elements):
            return str(element)
        self._raise_argument_error(
            "%s (%s %r) is invalid." % (msg, type(element), element)
        )

    @cython.cfunc
    @cython.inline(True)
    def _validate_elements(self, elements: object, msg: str) -> list[str]:
        """(internal) Validate the element(s) and return the name(s) `<'list[str]'>`."""
        if isinstance(elements, str):
            return [self._validate_element_name(elements, msg)]

        if isinstance(elements, Element):
            if isinstance(elements, Elements):
                els: Elements = elements
                return [str(i) for i in els._sorted_elements()]
            else:
                return [str(elements)]

        if isinstance(elements, (tuple, list)):
            res: list = []
            for i in elements:
                if isinstance(i, str):
                    res.append(self._validate_element_name(i, msg))
                elif isinstance(i, Element):
                    if isinstance(i, Elements):
                        els: Elements = i
                        for j in els._sorted_elements():
                            res.append(str(j))
                    else:
                        res.append(str(i))
                else:
                    res.extend(self._validate_elements(i, msg))
            return res

        self._raise_argument_error(
            "%s (%s %r) is invalid." % (msg, type(elements), elements)
        )

    @cython.cfunc
    @cython.inline(True)
    def _validate_table(self, table: object, msg: str) -> str:
        """(internal) Validate the table, and return its name `<'str'>`."""
        if isinstance(table, str):
            return self._validate_element_name(table, msg)

        if isinstance(table, Element):
            el: Element
            if isinstance(table, Elements):
                els: Elements = table
                for el in els._el_dict.values():
                    if el._tb_qualified_name is not None:
                        return el._tb_qualified_name
                    if el._tb_name is not None:
                        return el._tb_name
            else:
                el = table
                if el._tb_qualified_name is not None:
                    return el._tb_qualified_name
                if el._tb_name is not None:
                    return el._tb_name

        self._raise_argument_error("%s (%s %r) is invalid." % (msg, type(table), table))

    @cython.cfunc
    @cython.inline(True)
    def _validate_tables(self, tables: object, msg: str) -> list[str]:
        """(internal) Validate the table(s) and return the name(s) `<'list[str]'>`."""
        if isinstance(tables, str):
            return [self._validate_element_name(tables, msg)]

        name: str
        if isinstance(tables, Element):
            el: Element
            if isinstance(tables, Elements):
                res: list = []
                els: Elements = tables
                for el in els._sorted_elements():
                    if (name := el._tb_qualified_name) is None:
                        if (name := el._tb_name) is None:
                            self._raise_invalid_table_element_error(el)
                    res.append(name)
                return res
            else:
                el = tables
                if (name := el._tb_qualified_name) is None:
                    if (name := el._tb_name) is None:
                        self._raise_invalid_table_element_error(el)
                return [name]

        if isinstance(tables, (tuple, list)):
            res: list = []
            for i in tables:
                if isinstance(i, str):
                    res.append(self._validate_element_name(i, msg))
                elif isinstance(i, Element):
                    el: Element
                    if isinstance(i, Elements):
                        els: Elements = i
                        for el in els._sorted_elements():
                            if (name := el._tb_qualified_name) is None:
                                if (name := el._tb_name) is None:
                                    self._raise_invalid_table_element_error(el)
                            res.append(name)
                    else:
                        el = i
                        if (name := el._tb_qualified_name) is None:
                            if (name := el._tb_name) is None:
                                self._raise_invalid_table_element_error(el)
                        res.append(name)
                else:
                    res.extend(self._validate_tables(i, msg))
            return res

        self._raise_argument_error(
            "%s (%s %r) is invalid." % (msg, type(tables), tables)
        )

    @cython.cfunc
    @cython.inline(True)
    def _validate_table_alias(self, alias: object, msg: str) -> str:
        """(internal) Validate the alias of a table `<'str'>`."""
        if alias is None:
            tb_id: cython.int = self._tb_id
            self._tb_id += 1
            return "t" + str(tb_id)
        return self._validate_element(alias, msg)

    @cython.cfunc
    @cython.inline(True)
    def _validate_expression(self, expression: object, msg: str) -> str:
        """(internal) Validate the expression `<'str'>`."""
        expr: str
        if isinstance(expression, str):
            expr = utils.cleanup_expression(expression)
            if expr is None:
                self._raise_argument_error("%s cannot be an empty string." % msg)
            return expr

        if isinstance(expression, SQLFunction):
            expr = utils.cleanup_expression(str(expression))
            if expr is None:
                self._raise_argument_error("%s cannot be an empty string." % msg)
            return expr

        if isinstance(expression, Element) and not isinstance(expression, Elements):
            return str(expression)

        self._raise_argument_error(
            "%s (%s %r) is invalid." % (msg, type(expression), expression)
        )

    @cython.cfunc
    @cython.inline(True)
    def _validate_expressions(self, expressions: object, msg: str) -> list[str]:
        """(internal) Validate the expression(s) `<'list[str]'>`."""
        expr: str
        if isinstance(expressions, str):
            expr = utils.cleanup_expression(expressions)
            if expr is None:
                self._raise_argument_error("%s cannot be an empty string." % msg)
            return [expr]

        if isinstance(expressions, SQLFunction):
            expr = utils.cleanup_expression(str(expressions))
            if expr is None:
                self._raise_argument_error("%s cannot be an empty string." % msg)
            return [expr]

        if isinstance(expressions, Element):
            if isinstance(expressions, Elements):
                els: Elements = expressions
                return [str(i) for i in els._sorted_elements()]
            else:
                return [str(expressions)]

        if isinstance(expressions, (tuple, list)):
            res: list = []
            for i in expressions:
                if isinstance(i, str):
                    expr = utils.cleanup_expression(i)
                    if expr is None:
                        self._raise_argument_error(
                            "%s cannot be an empty string." % msg
                        )
                    res.append(expr)
                elif isinstance(i, SQLFunction):
                    expr = utils.cleanup_expression(str(i))
                    if expr is None:
                        self._raise_argument_error(
                            "%s cannot be an empty string." % msg
                        )
                    res.append(expr)
                elif isinstance(i, Element):
                    if isinstance(i, Elements):
                        els: Elements = i
                        for j in els._sorted_elements():
                            res.append(str(j))
                    else:
                        res.append(str(i))
                else:
                    res.extend(self._validate_expressions(i, msg))
            return res

        self._raise_argument_error(
            "%s (%s %r) is invalid." % (msg, type(expressions), expressions)
        )

    @cython.cfunc
    @cython.inline(True)
    def _validate_in_conditions(
        self,
        in_conds: object,
        msg: str,
    ) -> dict[str, str | tuple | SelectDML]:
        """(internal) Validate the IN conditions `<'dict'>`."""
        if not isinstance(in_conds, dict):
            self._raise_argument_error(
                "%s must be a dictionary, instead got %s." % (msg, type(in_conds))
            )
        conds: dict = in_conds
        res: dict = {}
        for key, val in conds.items():
            key = self._validate_expression(key, msg)
            if isinstance(val, SelectDML):
                dict_setitem(res, key, val)
            else:
                dict_setitem(res, key, _escape(val, False, True))
        return res

    @cython.cfunc
    @cython.inline(True)
    def _validate_subquery(self, subquery: object, msg: str) -> SelectDML:
        """(internal) Validate the subquery `<'SelectDML'>`."""
        if not isinstance(subquery, SelectDML):
            self._raise_argument_error(
                "%s subquery must be an instance of <'SelectDML'>, "
                "instead got %s." % (msg, type(subquery))
            )
        return subquery

    @cython.cfunc
    @cython.inline(True)
    def _validate_indent(self, indent: cython.int) -> str:
        """(internal) Validate the indentation level of the
        statement, and returns the indent padding `<'str/None'>`.
        """
        if indent < 1:
            return None
        elif indent == 1:
            return "\t"
        else:
            return "\t" * indent

    # Error --------------------------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(-1, check=False)
    def _raise_error(
        self,
        err_type: type,
        msg: str,
        tb_exc: Exception = None,
    ) -> cython.bint:
        """(internal) Raise a DML error.

        :param err_type `<'type'>`: The error class to raise.
        :param msg `<'str'>`: The error message.
        :param tb_exc `<'Exception/None'>`: The traceback exception. Defaults to `None`.
        """
        msg: str = "<'%s'> %s statement %s" % (self.__class__.__name__, self._dml, msg)
        if tb_exc is None:
            raise err_type(msg)
        else:
            raise err_type(msg) from tb_exc

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(-1, check=False)
    def _raise_clause_error(self, msg: str, tb_exc: Exception = None) -> cython.bint:
        """(internal) Raise a `DMLClauseError`.

        :param msg `<'str'>`: The error message.
        :param tb_exc `<'Exception/None'>`: The traceback exception. Defaults to `None`.
        """
        return self._raise_error(errors.DMLClauseError, msg, tb_exc)

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(-1, check=False)
    def _raise_argument_error(self, msg: str, tb_exc: Exception = None) -> cython.bint:
        """(internal) Raise a `DMLArgumentError`.

        :param msg `<'str'>`: The error message.
        :param tb_exc `<'Exception/None'>`: The traceback exception. Defaults to `None`.
        """
        return self._raise_error(errors.DMLArgumentError, msg, tb_exc)

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(-1, check=False)
    def _raise_critical_error(self, msg: str, tb_exc: Exception = None) -> cython.bint:
        """(internal) Raise a `DMLCriticalError`.

        :param msg `<'str'>`: The error message.
        :param tb_exc `<'Exception/None'>`: The traceback exception. Defaults to `None`.
        """
        return self._raise_error(errors.DMLCriticalError, msg, tb_exc)

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(-1, check=False)
    def _raise_not_implemented_error(self, method_name: str) -> cython.bint:
        """(internal) Raise a `NotImplementedError`.

        :param method_name `<'str'>`: The name of the method that's not implemented.
        """
        self._raise_error(
            NotImplementedError, "'%s()' method is not implemented." % method_name
        )

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(-1, check=False)
    def _raise_invalid_table_element_error(
        self,
        element: Element,
        tb_exc: Exception = None,
    ) -> cython.bint:
        """(internal) Raise `DMLArgumentError` for element that does not contain any table information."""
        self._raise_argument_error(
            "element <'%s'> does not contain any table information."
            % element.__class__.__name__,
            tb_exc,
        )
        return True

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(-1, check=False)
    def _raise_clause_already_set_error(
        self,
        clause: CLAUSE,
        tb_exc: Exception = None,
    ) -> cython.bint:
        """(internal) Raise `DMLClauseError` for statement clause already been set."""
        self._raise_clause_error(
            "%s clause has already been set to:\n%s"
            % (clause.__class__.__name__, clause.clause(None)),
            tb_exc,
        )
        return True

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(-1, check=False)
    def _raise_clause_order_error(
        self,
        clause_name: str,
        preceding_clauses: list[str],
        tb_exc: Exception = None,
    ) -> cython.bint:
        """(internal) Raise `DMLClauseError` for statement clause used in the wrong order."""
        size: cython.Py_ssize_t = list_len(preceding_clauses)
        if size == 1:
            self._raise_clause_error(
                "%s clause must be placed after the %s clause."
                % (clause_name, preceding_clauses[0]),
                tb_exc,
            )
        elif size > 1:
            last = preceding_clauses.pop()
            rest = ", ".join(preceding_clauses)
            self._raise_clause_error(
                "%s clause must be placed after the %s or %s clause."
                % (clause_name, rest, last),
                tb_exc,
            )
        else:
            self._raise_critical_error(
                "error argument 'preceding_clauses' cannot be empty.", tb_exc
            )
        return True

    # Special method -----------------------------------------------------------------------
    def __repr__(self) -> str:
        return "<'%s' %s statement>" % (self.__class__.__name__, self._dml)

    def __hash__(self) -> int:
        if self._hashcode == -1:
            self._hashcode = id(self)
        return self._hashcode


# Select - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
@cython.cclass
class SelectDML(DML):
    """Represent the DML SELECT statement."""

    def __init__(self, db_name: str, pool: Pool):
        """The DML SELECT statement.

        :param db_name `<'str'>`: The name of the database.
        :param pool `<'Pool'>`: The connection pool to handle the statement execution.
        """
        super().__init__("SELECT", db_name, pool)

    # Clause -------------------------------------------------------------------------------
    # . select
    def Select(
        self,
        *expressions: object,
        distinct: cython.bint = False,
        high_priority: cython.bint = False,
        straight_join: cython.bint = False,
        sql_buffer_result: cython.bint = False,
    ) -> SelectDML:
        """The SELECT clause of the SELECT statement `<'SelectDML'>`.

        :param expressions `<'*str/SQLFunction/Column'>`: The (expression of) the column(s) to retrieve.

        :param distinct `<'bool'>`: If `True`, specifies removal of duplicate rows from
            the result set. Defaults to `False`.

        :param high_priority `<'bool'>`: If `True`, gives the SELECT higher priority than
            a statement that updates a table. Defaults to `False`. Should only be used for
            queries that are very fast and must be done at once. A SELECT HIGH_PRIORITY
            query that is issued while the table is locked for reading runs even if there
            is an update statement waiting for the table to be free. This affects only storage
            engines that use only table-level locking (such as MyISAM, MEMORY, and MERGE).

        :param straight_join `<'bool'>`: If `True`, forces the optimizer to join the tables
            in the order in which they are listed in the FROM clause. Defaults to `False`.
            Can be used to speed up a query if the optimizer joins the tables in nonoptimal
            order.

        :param sql_buffer_result `<'bool'>`: If `True`, forces the result to be put into a
            temporary table. Defaults to `False`. This helps MySQL free the table locks early
            and helps in cases where it takes a long time to send the result set to the client.
            This modifier can be used only for top-level SELECT statements, not for subqueries
            or following UNION.

        :returns `<'SelectDML'>`: The DML Select statement.

        ## Example
        >>> db.Select("col1", db.tb.col2).From(db.tb)
            # Equivalent to:
            SELECT col1, col2 FROM db.tb
        """
        return self._Select(
            expressions,
            distinct,
            high_priority,
            straight_join,
            sql_buffer_result,
        )

    @cython.ccall
    def _Select(
        self,
        expressions: tuple,
        distinct: cython.bint = False,
        high_priority: cython.bint = False,
        straight_join: cython.bint = False,
        sql_buffer_result: cython.bint = False,
    ) -> SelectDML:
        """(internal) The SELECT clause of the SELECT statement `<'SelectDML'>`.

        :param expressions `<'tuple[str/SQLFunction/Column]'>`: The (expression of) the column(s) to retrieve.

        :param distinct `<'bool'>`: If `True`, specifies removal of duplicate rows from
            the result set. Defaults to `False`.

        :param high_priority `<'bool'>`: If `True`, gives the SELECT higher priority than
            a statement that updates a table. Defaults to `False`. Should only be used for
            queries that are very fast and must be done at once. A SELECT HIGH_PRIORITY
            query that is issued while the table is locked for reading runs even if there
            is an update statement waiting for the table to be free. This affects only storage
            engines that use only table-level locking (such as MyISAM, MEMORY, and MERGE).

        :param straight_join `<'bool'>`: If `True`, forces the optimizer to join the tables
            in the order in which they are listed in the FROM clause. Defaults to `False`.
            Can be used to speed up a query if the optimizer joins the tables in nonoptimal
            order.

        :param sql_buffer_result `<'bool'>`: If `True`, forces the result to be put into a
            temporary table. Defaults to `False`. This helps MySQL free the table locks early
            and helps in cases where it takes a long time to send the result set to the client.
            This modifier can be used only for top-level SELECT statements, not for subqueries
            or following UNION.

        :returns `<'SelectDML'>`: The DML Select statement.
        """
        if self._select_clause is not None:
            self._raise_clause_already_set_error(self._select_clause)
        if self._clause_id != utils.DML_CLAUSE.NONE:
            self._raise_clause_error("must start with the SELECT clause.")
        self._select_clause = self._gen_select_clause(
            expressions, distinct, high_priority, straight_join, sql_buffer_result
        )
        return self

    # . from
    @cython.ccall
    def From(
        self,
        table: object,
        partition: object = None,
        alias: object = None,
    ) -> SelectDML:
        """The FROM clause of the SELECT statement `<'SelectDML'>`.

        :param table `<'str/Table/SelectDML'>`: The table (reference) from which to retrieve data.
            Only accepts one table reference. For multiple-table, please use the explicit JOIN clause instead.
        :param partition `<'str/Partition/list/tuple/None'>`: The partition(s) of the table. Defaults to `None`.
        :param alias `<'str/None'>`: The alias of the table (reference). Defaults to `None`.
            When 'alias=None', a default alias is automatically generated based on the count
            of the tables adopting default alias in the same query block (starting from 't0').
        :returns `<'SelectDML'>`: The DML Select statement.

        ## Notice
        - The FROM clause must be placed after the SELECT clause.
        - To set index hints, chain the `UseIndex()`, `IgnoreIndex()` and
          `ForceIndex()` methods after From().

        ## Example (table)
        >>> db.Select("col1", db.tb.col2).From(db.tb)
            # Equivalent to:
            SELECT col1, col2 FROM db.tb AS t0

        ## Example (subquery)
        >>> db.Select("col1", "col2").From(db.Select("*").From(db.tb))
            # Equivalent to:
            SELECT col1, col2 FROM (SELECT * FROM db.tb AS t0) AS t0
        """
        if self._clause_id != utils.DML_CLAUSE.SELECT:
            self._raise_clause_order_error("FROM", ["SELECT"])
        if self._from_clause is not None:
            self._raise_clause_already_set_error(self._from_clause)
        self._from_clause = self._gen_from_clause(table, partition, alias)
        return self

    # . join
    def Join(
        self,
        table: object,
        *on: str,
        using: object = None,
        partition: object = None,
        alias: object = None,
    ) -> SelectDML:
        """The (INNER) JOIN clause of the SELECT statement `<'SelectDML'>`.

        :param table `<'str/Table/SelectDML'>`: The table (reference) to join.
        :param on `<'*str'>`: The ON condition(s) specify how to join the table.
        :param using `<'str/Column/list/tuple/None'>`: The USING condition(s) of the common column(s) between the tables. Defaults to `None`.
            When on is specified, using is ignored.
        :param partition `<'str/Partition/list/tuple/None'>`: The partition(s) of the join table. Defaults to `None`.
        :param alias `<'str/None'>`: The alias of the join table. Defaults to `None`.
            When 'alias=None', a default alias is automatically generated based on the count
            of the tables adopting default alias in the same query block (starting from 't0').
        :returns `<'SelectDML'>`: The DML Select statement.

        ## Notice
        - The JOIN clause must be placed after the SELECT or FROM clause.
        - For multiple joins, chain the `Join()` method after another `Join()`.
        - To set index hints, chain the `UseIndex()`, `IgnoreIndex()` and
          `ForceIndex()` methods after `Join()`.

        ## Example (table ON)
        >>> db.Select("*").From(db.tb1).Join(db.tb2, "t0.id = t1.id")
            # Equivalent to:
            SELECT * FROM db.tb1 AS t0
            INNER JOIN db.tb2 AS t1
                ON t0.id = t1.id

        ## Example (table USING)
        >>> db.Select("*").From(db.tb1).Join(db.tb2, using=["id", db.tb.name])
            # Equivalent to:
            SELECT * FROM db.tb1 AS t0
            INNER JOIN db.tb2 AS t1
                USING (id, name)

        ## Example (subquery)
        >>> db.Select("*").From(db.tb1).Join(
                db.Select("id", "name").From(db.tb2), "t0.id = t1.id"
            )
            # Equivalent to:
            SELECT * FROM db.tb1 AS t0
            INNER JOIN (
                SELECT id, name FROM db.tb2 AS t0
            ) AS t1
                ON t0.id = t1.id
        """
        self._validate_join_clause_order()
        clause: JOIN = self._gen_inner_join_clause(table, on, using, partition, alias)
        if self._join_clauses is None:
            self._join_clauses = [clause]
        else:
            list_append(self._join_clauses, clause)
        return self

    def LeftJoin(
        self,
        table: object,
        *on: object,
        using: object = None,
        partition: object = None,
        alias: object = None,
    ) -> SelectDML:
        """The LEFT JOIN clause of the SELECT statement `<'SelectDML'>`.

        :param table `<'str/Table/SelectDML'>`: The table (reference) to join.
        :param on `<'*str'>`: The ON condition(s) specify how to join the table.
        :param using `<'str/Column/list/tuple/None'>`: The USING condition(s) of the common column(s) between the tables. Defaults to `None`.
            When on is specified, using is ignored.
        :param partition `<'str/Partition/list/tuple/None'>`: The partition(s) of the join table. Defaults to `None`.
        :param alias `<'str/None'>`: The alias of the join table. Defaults to `None`.
            When 'alias=None', a default alias is automatically generated based on the count
            of the tables adopting default alias in the same query block (starting from 't0').
        :returns `<'SelectDML'>`: The DML Select statement.

        ## Notice
        - The JOIN clause must be placed after the SELECT or FROM clause.
        - For multiple joins, chain the `Join()` method after another `Join()`.
        - To set index hints, chain the `UseIndex()`, `IgnoreIndex()` and
          `ForceIndex()` methods after `Join()`.

        ## Example (table ON)
        >>> db.Select("*").From(db.tb1).LeftJoin(db.tb2, "t0.id = t1.id")
            # Equivalent to:
            SELECT * FROM db.tb1 AS t0
            LEFT JOIN db.tb2 AS t1
                ON t0.id = t1.id

        ## Example (table USING)
        >>> db.Select("*").From(db.tb1).LeftJoin(db.tb2, using=["id", db.tb.name])
            # Equivalent to:
            SELECT * FROM db.tb1 AS t0
            LEFT JOIN db.tb2 AS t1
                USING (id, name)

        ## Example (subquery)
        >>> db.Select("*").From(db.tb1).LeftJoin(
                db.Select("id", "name").From(db.tb2), "t0.id = t1.id"
            )
            # Equivalent to:
            SELECT * FROM db.tb1 AS t0
            LEFT JOIN (
                SELECT id, name FROM db.tb2 AS t0
            ) AS t1
                ON t0.id = t1.id
        """
        self._validate_join_clause_order()
        clause: JOIN = self._gen_left_join_clause(table, on, using, partition, alias)
        if self._join_clauses is None:
            self._join_clauses = [clause]
        else:
            list_append(self._join_clauses, clause)
        return self

    def RightJoin(
        self,
        table: object,
        *on: object,
        using: object = None,
        partition: object = None,
        alias: object = None,
    ) -> SelectDML:
        """The RIGHT JOIN clause of the SELECT statement `<'SelectDML'>`.

        :param table `<'str/Table/SelectDML'>`: The table (reference) to join.
        :param on `<'*str'>`: The ON condition(s) specify how to join the table.
        :param using `<'str/Column/list/tuple/None'>`: The USING condition(s) of the common column(s) between the tables. Defaults to `None`.
            When on is specified, using is ignored.
        :param partition `<'str/Partition/list/tuple/None'>`: The partition(s) of the join table. Defaults to `None`.
        :param alias `<'str/None'>`: The alias of the join table. Defaults to `None`.
            When 'alias=None', a default alias is automatically generated based on the count
            of the tables adopting default alias in the same query block (starting from 't0').
        :returns `<'SelectDML'>`: The DML Select statement.

        ## Notice
        - The JOIN clause must be placed after the SELECT or FROM clause.
        - For multiple joins, chain the `Join()` method after another `Join()`.
        - To set index hints, chain the `UseIndex()`, `IgnoreIndex()` and
          `ForceIndex()` methods after `Join()`.

        ## Example (table ON)
        >>> db.Select("*").From(db.tb1).RightJoin(db.tb2, "t0.id = t1.id")
            # Equivalent to:
            SELECT * FROM db.tb1 AS t0
            RIGHT JOIN db.tb2 AS t1
                ON t0.id = t1.id

        ## Example (table USING)
        >>> db.Select("*").From(db.tb1).RightJoin(db.tb2, using=["id", db.tb.name])
            # Equivalent to:
            SELECT * FROM db.tb1 AS t0
            RIGHT JOIN db.tb2 AS t1
                USING (id, name)

        ## Example (subquery)
        >>> db.Select("*").From(db.tb1).RightJoin(
                db.Select("id", "name").From(db.tb2), "t0.id = t1.id"
            )
            # Equivalent to:
            SELECT * FROM db.tb1 AS t0
            RIGHT JOIN (
                SELECT id, name FROM db.tb2 AS t0
            ) AS t1
                ON t0.id = t1.id
        """
        self._validate_join_clause_order()
        clause: JOIN = self._gen_right_join_clause(table, on, using, partition, alias)
        if self._join_clauses is None:
            self._join_clauses = [clause]
        else:
            list_append(self._join_clauses, clause)
        return self

    def StraightJoin(
        self,
        table: object,
        *on: object,
        using: object = None,
        partition: object = None,
        alias: object = None,
    ) -> SelectDML:
        """The STRAIGHT_JOIN clause of the SELECT statement `<'SelectDML'>`.

        :param table `<'str/Table/SelectDML'>`: The table (reference) to join.
        :param on `<'*str'>`: The ON condition(s) specify how to join the table.
        :param using `<'str/Column/list/tuple/None'>`: The USING condition(s) of the common column(s) between the tables. Defaults to `None`.
            When on is specified, using is ignored.
        :param partition `<'str/Partition/list/tuple/None'>`: The partition(s) of the join table. Defaults to `None`.
        :param alias `<'str/None'>`: The alias of the join table. Defaults to `None`.
            When 'alias=None', a default alias is automatically generated based on the count
            of the tables adopting default alias in the same query block (starting from 't0').
        :returns `<'SelectDML'>`: The DML Select statement.

        ## Notice
        - The JOIN clause must be placed after the SELECT or FROM clause.
        - For multiple joins, chain the `Join()` method after another `Join()`.
        - To set index hints, chain the `UseIndex()`, `IgnoreIndex()` and
          `ForceIndex()` methods after `Join()`.

        ## Example (table ON)
        >>> db.Select("*").From(db.tb1).StraightJoin(db.tb2, "t0.id = t1.id")
            # Equivalent to:
            SELECT * FROM db.tb1 AS t0
            STRAIGHT_JOIN db.tb2 AS t1
                ON t0.id = t1.id

        ## Example (table USING)
        >>> db.Select("*").From(db.tb1).StraightJoin(db.tb2, using=["id", db.tb.name])
            # Equivalent to:
            SELECT * FROM db.tb1 AS t0
            STRAIGHT_JOIN db.tb2 AS t1
                USING (id, name)

        ## Example (subquery)
        >>> db.Select("*").From(db.tb1).StraightJoin(
                db.Select("id", "name").From(db.tb2), "t0.id = t1.id"
            )
            # Equivalent to:
            SELECT * FROM db.tb1 AS t0
            STRAIGHT_JOIN (
                SELECT id, name FROM db.tb2 AS t0
            ) AS t1
                ON t0.id = t1.id
        """
        self._validate_join_clause_order()
        clause: JOIN = self._gen_straight_join_clause(
            table, on, using, partition, alias
        )
        if self._join_clauses is None:
            self._join_clauses = [clause]
        else:
            list_append(self._join_clauses, clause)
        return self

    def CrossJoin(
        self,
        table: object,
        *on: object,
        using: object = None,
        partition: object = None,
        alias: object = None,
    ) -> SelectDML:
        """The CROSS JOIN clause of the SELECT statement `<'SelectDML'>`.

        :param table `<'str/Table/SelectDML'>`: The table (reference) to join.
        :param on `<'*str'>`: The ON condition(s) specify how to join the table.
        :param using `<'str/Column/list/tuple/None'>`: The USING condition(s) of the common column(s) between the tables. Defaults to `None`.
            When on is specified, using is ignored.
        :param partition `<'str/Partition/list/tuple/None'>`: The partition(s) of the join table. Defaults to `None`.
        :param alias `<'str/None'>`: The alias of the join table. Defaults to `None`.
            When 'alias=None', a default alias is automatically generated based on the count
            of the tables adopting default alias in the same query block (starting from 't0').
        :returns `<'SelectDML'>`: The DML Select statement.

        ## Notice
        - The JOIN clause must be placed after the SELECT or FROM clause.
        - For multiple joins, chain the `Join()` method after another `Join()`.
        - To set index hints, chain the `UseIndex()`, `IgnoreIndex()` and
          `ForceIndex()` methods after `Join()`.

        ## Example (table)
        >>> db.Select("*").From(db.tb1).CrossJoin(db.tb2)
            # Equivalent to:
            SELECT * FROM db.tb1 AS t0
            CROSS JOIN db.tb2 AS t1

        ## Example (subquery)
        >>> db.Select("*").From(db.tb1).CrossJoin(
                db.Select("id", "name").From(db.tb2)
            )
            # Equivalent to:
            SELECT * FROM db.tb1 AS t0
            CROSS JOIN (
                SELECT id, name FROM db.tb2 AS t0
            ) AS t1
        """
        self._validate_join_clause_order()
        clause: JOIN = self._gen_cross_join_clause(table, on, using, partition, alias)
        if self._join_clauses is None:
            self._join_clauses = [clause]
        else:
            list_append(self._join_clauses, clause)
        return self

    def NaturalJoin(
        self,
        table: object,
        join_method: object = "INNER",
        partition: object = None,
        alias: object = None,
    ) -> SelectDML:
        """The NATURAL JOIN clause of the SELECT statement `<'SelectDML'>`.

        :param table `<'str/Table/SelectDML'>`: The table (reference) to join.
        :param join_method `<'str'>`: The join method. Defaults to `"INNER"`.
            Accepts: `"INNER"`, `"LEFT"` and `"RIGHT"`.
        :param partition `<'str/Partition/list/tuple/None'>`: The partition(s) of the join table. Defaults to `None`.
        :param alias `<'str/None'>`: The alias of the join table. Defaults to `None`.
            When 'alias=None', a default alias is automatically generated based on the count
            of the tables adopting default alias in the same query block (starting from 't0').
        :returns `<'SelectDML'>`: The DML Select statement.

        ## Notice
        - The JOIN clause must be placed after the SELECT or FROM clause.
        - For multiple joins, chain the `Join()` method after another `Join()`.
        - To set index hints, chain the `UseIndex()`, `IgnoreIndex()` and
          `ForceIndex()` methods after `Join()`.

        ## Example (table)
        >>> db.Select("*").From(db.tb1).NaturalJoin(db.tb2)
            # Equivalent to:
            SELECT * FROM db.tb1 AS t0
            NATURAL INNER JOIN db.tb2 AS t1

        ## Example (subquery)
        >>> db.Select("*").From(db.tb1).NaturalJoin(
                db.Select("id", "name").From(db.tb2), "LEFT"
            )
            # Equivalent to:
            SELECT * FROM db.tb1 AS t0
            NATURAL LEFT JOIN (
                SELECT id, name FROM db.tb2 AS t0
            ) AS t1
        """
        self._validate_join_clause_order()
        clause: JOIN = self._gen_natural_join_clause(
            table, join_method, partition, alias
        )
        if self._join_clauses is None:
            self._join_clauses = [clause]
        else:
            list_append(self._join_clauses, clause)
        return self

    # . index hints
    def UseIndex(self, *indexes: object, scope: object = None) -> SelectDML:
        """The USE INDEX clause of the SELECT statement `<'SelectDML'>`.

        :param indexes `<'*str/Index'>`: The index(es) to use by the INDEX HINTS.
        :param scope `<'str/None'>`: The scope of the INDEX HINTS. Defaults to `None`.
            Accepts: `"JOIN"`, `"ORDER BY"` and `"GROUP BY"`.
            This provides more fine-grained control over optimizer selection of an execution plan
            for various phases of query processing. To affect only the indexes used when MySQL
            decides how to find rows in the table and how to process joins, use FOR JOIN. To
            influence index usage for sorting or grouping rows, use FOR ORDER BY or FOR GROUP BY.
        :returns `<'SelectDML'>`: The DML Select statement.

        ## Notice
        - The USE INDEX clause must be placed after the FROM or JOIN
          clause or chain after another INDEX HINTS clause.

        ## Example
        >>> (
                db.Select("*")
                .From(db.tb)
                .UseIndex("idx1", "idx2")
                .ForceIndex(db.tb.pk, scope="ORDER BY")
                .Join(db.tb2, using="id")
                .UseIndex("idx3", scope="JOIN")
                .IgnoreIndex("idx4")
                .OrderBy("col1")
            )
            # Equivalent to:
            SELECT * FROM db.tb AS t0
                USE INDEX (idx1, idx2)
                FORCE INDEX FOR ORDER BY (PRIMARY)
            INNER JOIN db.tb2 AS t1
                USE INDEX FOR JOIN (idx3)
                IGNORE INDEX (idx4)
                USING (id)
            ORDER BY col1
        """
        # fmt: off
        if self._clause_id == utils.DML_CLAUSE.FROM:
            clause: CLAUSE = self._from_clause
        elif self._clause_id == utils.DML_CLAUSE.JOIN and self._join_clauses is not None:
            clause: CLAUSE = self._join_clauses[list_len(self._join_clauses) - 1]
        else:
            self._raise_clause_order_error(
                "USE INDEX", ["FROM", "JOIN", "another INDEX HINTS"]
            )
        # fmt: on
        clause._add_index_hints(self._gen_use_index_clause(indexes, scope))
        return self

    def IgnoreIndex(self, *indexes: object, scope: object = None) -> SelectDML:
        """The IGNORE INDEX clause of the SELECT statement `<'SelectDML'>`.

        :param indexes `<'*str/Index'>`: The index(es) to ignore by the INDEX HINTS.
        :param scope `<'str/None'>`: The scope of the INDEX HINTS. Defaults to `None`.
            Accepts: `"JOIN"`, `"ORDER BY"` and `"GROUP BY"`.
            This provides more fine-grained control over optimizer selection of an execution plan
            for various phases of query processing. To affect only the indexes used when MySQL
            decides how to find rows in the table and how to process joins, use FOR JOIN. To
            influence index usage for sorting or grouping rows, use FOR ORDER BY or FOR GROUP BY.
        :returns `<'SelectDML'>`: The DML Select statement.

        ## Notice
        - The IGNORE INDEX clause must be placed after the FROM or JOIN
          clause or chain after another INDEX HINTS clause.

        ## Example
        >>> (
                db.Select("*")
                .From(db.tb)
                .UseIndex("idx1", "idx2")
                .ForceIndex(db.tb.pk, scope="ORDER BY")
                .Join(db.tb2, using="id")
                .UseIndex("idx3", scope="JOIN")
                .IgnoreIndex("idx4")
                .OrderBy("col1")
            )
            # Equivalent to:
            SELECT * FROM db.tb AS t0
                USE INDEX (idx1, idx2)
                FORCE INDEX FOR ORDER BY (PRIMARY)
            INNER JOIN db.tb2 AS t1
                USE INDEX FOR JOIN (idx3)
                IGNORE INDEX (idx4)
                USING (id)
            ORDER BY col1
        """
        # fmt: off
        if self._clause_id == utils.DML_CLAUSE.FROM:
            clause: CLAUSE = self._from_clause
        elif self._clause_id == utils.DML_CLAUSE.JOIN and self._join_clauses is not None:
            clause: CLAUSE = self._join_clauses[list_len(self._join_clauses) - 1]
        else:
            self._raise_clause_order_error(
                "IGNORE INDEX", ["FROM", "JOIN", "another INDEX HINTS"]
            )
        # fmt: on
        clause._add_index_hints(self._gen_ignore_index_clause(indexes, scope))
        return self

    def ForceIndex(self, *indexes: object, scope: object = None) -> SelectDML:
        """The FORCE INDEX clause of the SELECT statement `<'SelectDML'>`.

        :param indexes `<'*str/Index'>`: The index(es) to force by the INDEX HINTS.
        :param scope `<'str/None'>`: The scope of the INDEX HINTS. Defaults to `None`.
            Accepts: `"JOIN"`, `"ORDER BY"` and `"GROUP BY"`.
            This provides more fine-grained control over optimizer selection of an execution plan
            for various phases of query processing. To affect only the indexes used when MySQL
            decides how to find rows in the table and how to process joins, use FOR JOIN. To
            influence index usage for sorting or grouping rows, use FOR ORDER BY or FOR GROUP BY.
        :returns `<'SelectDML'>`: The DML Select statement.

        ## Notice
        - The FORCE INDEX clause must be placed after the FROM or JOIN
          clause or chain after another INDEX HINTS clause.

        ## Example
        >>> (
                db.Select("*")
                .From(db.tb)
                .UseIndex("idx1", "idx2")
                .ForceIndex(db.tb.pk, scope="ORDER BY")
                .Join(db.tb2, using="id")
                .UseIndex("idx3", scope="JOIN")
                .IgnoreIndex("idx4")
                .OrderBy("col1")
            )
            # Equivalent to:
            SELECT * FROM db.tb AS t0
                USE INDEX (idx1, idx2)
                FORCE INDEX FOR ORDER BY (PRIMARY)
            INNER JOIN db.tb2 AS t1
                USE INDEX FOR JOIN (idx3)
                IGNORE INDEX (idx4)
                USING (id)
            ORDER BY col1
        """
        # fmt: off
        if self._clause_id == utils.DML_CLAUSE.FROM:
            clause: CLAUSE = self._from_clause
        elif self._clause_id == utils.DML_CLAUSE.JOIN and self._join_clauses is not None:
            clause: CLAUSE = self._join_clauses[list_len(self._join_clauses) - 1]
        else:
            self._raise_clause_order_error(
                "FORCE INDEX", ["FROM", "JOIN", "another INDEX HINTS"]
            )
        # fmt: on
        clause._add_index_hints(self._gen_force_index_clause(indexes, scope))
        return self

    # . where
    def Where(
        self,
        *conds: str,
        in_conds: dict | None = None,
        not_in_conds: dict | None = None,
    ) -> SelectDML:
        """The WHERE clause of the SELECT statement `<'SelectDML'>`.

        :param conds `<'*str'>`: The condition(s) that rows must satisfy to be selected.
        :param in_conds `<'dict/None'>`: The IN condition(s). Defaults to `None`.
        :param not_in_conds `<'dict/None'>`: The NOT IN condition(s). Defaults to `None`.
        :returns `<'SelectDML'>`: The DML Select statement.

        ## Explanation
        - All conditions are combined with `AND` logic by default.
        - Prepend the condition with "`OR `" to alter the default behavior.
        - The 'in_conds' and 'not_in_conds' arguments must be a dictionary,
          where the keys should be the columns and values the desired ones
          to check against. Values can be any escapable objects or a subquery
          `<'SelectDML'>` instance, but `NOT` strings with placeholders.

        ## Example
        >>> db.Select("*").From(db.tb).Where("id > 1", "name = 'John'")
            # Equivalent to:
            SELECT * FROM db.tb AS t0
            WHERE id > 1 AND name = 'John'

        ## Example (OR)
        >>> db.Select("*").From(db.tb).Where("id > 1", "OR name = 'John'")
            # Equivalent to:
            SELECT * FROM db.tb AS t0
            WHERE id > 1 OR name = 'John'

        ## Example (IN)
        >>> db.Select("*").From(db.tb).Where(
                in_conds={"id": [1, 2, 3], "name": ["John", "Doe"]}
            )
            # Equivalent to:
            SELECT * FROM db.tb AS t0
            WHERE id IN (1,2,3) AND name IN ('John','Doe')

        ## Example (NOT IN)
        >>> db.Select("*").From(db.tb1).Where(
                not_in_conds={
                    "id": db.Select("id").From(db.tb2),
                    "OR id": [1, 2, 3],
                }
            )
            # Equivalent to:
            SELECT * FROM db.tb1 AS t0
            WHERE id NOT IN (SELECT id FROM db.tb2 AS t0)
                OR id NOT IN (1,2,3)
        """
        if self._where_clause is not None:
            self._raise_clause_already_set_error(self._where_clause)
        self._where_clause = self._gen_where_clause(conds, in_conds, not_in_conds)
        return self

    # . group by
    def GroupBy(self, *columns: object, with_rollup: cython.bint = False) -> SelectDML:
        """The GROUP BY clause of the SELECT statement `<'SelectDML'>`.

        :param columns `<'*str/Column'>`: The (expression of) column(s) to group by with.
        :param with_rollup `<'bool'>`: Whether to summary output to include extra rows
            that represent higher-level (super-aggregate) grand total.
        :returns `<'SelectDML'>`: The DML Select statement.

        ## Example
        >>> db.Select("*").From(db.tb).GroupBy("id", "name", with_rollup=True)
            # Equivalent to:
            SELECT * FROM db.tb AS t0
            GROUP BY id, name WITH ROLLUP
        """
        if self._group_by_clause is not None:
            self._raise_clause_already_set_error(self._group_by_clause)
        self._group_by_clause = self._gen_group_by_clause(columns, with_rollup)
        return self

    # . having
    def Having(
        self,
        *conds: str,
        in_conds: dict | None = None,
        not_in_conds: dict | None = None,
    ) -> SelectDML:
        """The HAVING clause of the SELECT statement `<'SelectDML'>`.

        :param conds `<'*str'>`: The condition(s) that rows must satisfy to be selected.
        :param in_conds `<'dict/None'>`: The IN condition(s). Defaults to `None`.
        :param not_in_conds `<'dict/None'>`: The NOT IN condition(s). Defaults to `None`.
        :returns `<'SelectDML'>`: The DML Select statement.

        ## Explanation
        - All conditions are combined with `AND` logic by default.
        - Prepend the condition with "`OR `" to alter the default behavior.
        - The 'in_conds' and 'not_in_conds' arguments must be a dictionary,
          where the keys should be the columns and values the desired ones
          to check against. Values can be any escapable objects or a subquery
          `<'SelectDML'>` instance, but `NOT` strings with placeholders.

        ## Notice
        The HAVING clause, like the WHERE clause, specifies selection conditions.
        The WHERE clause specifies conditions on columns in the select list, but
        cannot refer to aggregate functions. The HAVING clause specifies conditions
        on groups, typically formed by the GROUP BY clause. The query result includes
        only groups satisfying the HAVING conditions.

        ## Example
        - Please refer to the `Where()` method for similar usage.
        """
        if self._having_clause is not None:
            self._raise_clause_already_set_error(self._having_clause)
        self._having_clause = self._gen_having_clause(conds, in_conds, not_in_conds)
        return self

    # . window
    def Window(
        self,
        name: object,
        partition_by: object | None = None,
        order_by: object | None = None,
        frame_clause: str | None = None,
    ) -> SelectDML:
        """The WINDOW clause of the SELECT statement `<'SelectDML'>`.

        :param name `<'str'>`: The name of the window.

        :param partition_by `<'str/Column/list/tuple/None'>`: Specifies how to divide the query rows into groups. Defaults to `None`.
            The window function result for a given row is based on the rows of the partition
            that contains the row. If 'partition_by=None', there is a single partition
            consisting of all query rows.

        :param order_by `<'str/Column/list/tuple/None'>`: Specifies how to sort rows in each partition. Defaults to `None`.
            Partition rows that are equal according to the ORDER BY clause are considered peers.
            If 'order_by=None', partition rows are unordered, with no processing order implied,
            and all partition rows are peers.

        :param frame_clause `<'str/None'>`: Specifies how to define the frame (subset of the current partition). Defaults to `None`.
            Frames are determined with respect to the current row, which enables a frame to move
            within a partition depending on the location of the current row within its partition.

            Examples: By defining a frame to be all rows from the partition start to the current
            row, you can compute running totals for each row. By defining a frame as extending N
            rows on either side of the current row, you can compute rolling averages.

            For more information, please refer to the MySQL documentation
            [Section 14.20.3, "Window Function Frame Specification"](https://dev.mysql.com/doc/refman/8.0/en/window-function-descriptions.html).

        :returns `<'SelectDML'>`: The DML Select statement.

        ## Notice
        - For multiple windows, chain the Window() method after another Window().

        ## Example
        >>> (
                db.Select(
                    "time",
                    "subject",
                    "val",
                    "FIRST_VALUE(val) OVER w AS 'first'",
                    "LAST_VALUE(val) OVER w AS 'last'",
                    "NTH_VALUE(val, 2) OVER w AS 'second'",
                    "NTH_VALUE(val, 4) OVER w AS 'fourth'",
                )
                .From(db.tb)
                .Window(
                    "w",
                    partition_by="subject",
                    order_by="time",
                    frame_clause="ROWS UNBOUNDED PRECEDING"
                )
            )
            # Equivalent to:
            SELECT
                time,
                subject,
                val,
                FIRST_VALUE(val) OVER w AS 'first',
                LAST_VALUE(val) OVER w AS 'last',
                NTH_VALUE(val, 2) OVER w AS 'second',
                NTH_VALUE(val, 4) OVER w AS 'fourth'
            FROM db.tb AS t0
            WINDOW w AS (
                PARTITION BY subject
                ORDER BY time
                ROWS UNBOUNDED PRECEDING
            )
        """
        clause: WINDOW = self._gen_window_clause(
            name, partition_by, order_by, frame_clause
        )
        if self._window_clauses is None:
            self._window_clauses = [clause]
        else:
            clause._primary = False
            list_append(self._window_clauses, clause)
        return self

    # . set operations
    def Union(
        self,
        subquery: object,
        all: cython.bint = False,
    ) -> SelectDML:
        """The UNION clause of the SELECT statement `<'SelectDML'>`.

        `UNION` (SET OPERATION) combines the result from multiple
        query blocks into a single result set.

        :param subquery `<'SelectDML'>`: The subquery to union with the main query block.
        :param all `<'bool'>`: Keep all the result if `True`, else sort & deduplicate the result set. Defaults to `False`.
        :returns `<'SelectDML'>`: The DML Select statement.

        ## Notice
        - For multiple subqueries, chain the Union() method after another SET OPERATION.

        ## Example:
        >>> (
                db.Select("*")
                .From(db.tb1)
                .Union(db.Select("*").From(db.tb2))
                .Union(db.Select("*").From(db.tb3), all=True)
                .Limit(10)
            )
            # Equivalent to:
            SELECT * FROM db.tb1 AS t0
            UNION DISTINCT (
                SELECT * FROM db.tb2 AS t0
            )
            UNION ALL (
                SELECT * FROM db.tb3 AS t0
            )
            LIMIT 10
        """
        clause: CLAUSE = self._gen_union_clause(subquery, all)
        if self._set_op_clauses is None:
            self._set_op_clauses = [clause]
        else:
            list_append(self._set_op_clauses, clause)
        return self

    def Intersect(
        self,
        subquery: object,
        all: cython.bint = False,
    ) -> SelectDML:
        """The INTERSECT clause of the SELECT statement `<'SelectDML'>`.

        `INTERSECT` (SET OPERATION) limits the result from multiple
        query blocks to those rows which are common to all.

        :param subquery `<'SelectDML'>`: The subquery to intersect with the main query block.
        :param all `<'bool'>`: Keep all the result if `True`, else sort & deduplicate the result set. Defaults to `False`.
        :returns `<'SelectDML'>`: The DML Select statement.

        ## Notice
        - For multiple subqueries, chain the Intersect() method after another SET OPERATION.
        - You should be aware that INTERSECT is evaluated before UNION or EXCEPT. This means
          that, for example, `TABLE x UNION TABLE y INTERSECT TABLE z` is always evaluated as
          `TABLE x UNION (TABLE y INTERSECT TABLE z)`. For more information, please refer to
          [Section 15.2.8, "INTERSECT Clause"](https://dev.mysql.com/doc/refman/8.4/en/intersect.html)

        ## Example:
        >>> (
                db.Select("*")
                .From(db.tb1)
                .Intersect(db.Select("*").From(db.tb2))
                .Intersect(db.Select("*").From(db.tb3), all=True)
                .Limit(10)
            )
            # Equivalent to:
            SELECT * FROM db.tb1 AS t0
            INTERSECT DISTINCT (
                SELECT * FROM db.tb2 AS t0
            )
            INTERSECT ALL (
                SELECT * FROM db.tb3 AS t0
            )
            LIMIT 10
        """
        clause: CLAUSE = self._gen_intersect_clause(subquery, all)
        if self._set_op_clauses is None:
            self._set_op_clauses = [clause]
        else:
            list_append(self._set_op_clauses, clause)
        return self

    def Except(
        self,
        subquery: object,
        all: cython.bint = False,
    ) -> SelectDML:
        """The EXCEPT clause of the SELECT statement `<'SelectDML'>`.

        `EXCEPT` (SET OPERATION) limits the result from the first query
        block to those rows which are (also) not found in the second.

        :param subquery `<'SelectDML'>`: The subquery to except with the main query block.
        :param all `<'bool'>`: Keep all the result if `True`, else sort & deduplicate the result set. Defaults to `False`.
        :returns `<'SelectDML'>`: The DML Select statement.

        ## Notice
        - For multiple subqueries, chain the Except() method after another SET OPERATION.

        ## Example:
        >>> (
                db.Select("*")
                .From(db.tb1)
                .Except(db.Select("*").From(db.tb2))
                .Except(db.Select("*").From(db.tb3), all=True)
                .Limit(10)
            )
            # Equivalent to:
            SELECT * FROM db.tb1 AS t0
            EXCEPT DISTINCT (
                SELECT * FROM db.tb2 AS t0
            )
            EXCEPT ALL (
                SELECT * FROM db.tb3 AS t0
            )
            LIMIT 10
        """
        clause: CLAUSE = self._gen_except_clause(subquery, all)
        if self._set_op_clauses is None:
            self._set_op_clauses = [clause]
        else:
            list_append(self._set_op_clauses, clause)
        return self

    # . order by
    def OrderBy(self, *columns: object, with_rollup: cython.bint = False) -> SelectDML:
        """The ORDER BY clause of the SELECT statement `<'SelectDML'>`.

        :param columns `<'*str/Column'>`: The ordering (expression of) column(s).
            Each element can be a column name or any SQL expression,
            optionally suffixed with 'ASC' or 'DESC'.
        :param with_rollup `<'bool'>`: Whether to append the `WITH ROLLUP` modifier. Defaults to `False`.
            When 'with_rollup=True', any super-aggregate rows produced (e.g. by a preceding
            `GROUP BY … WITH ROLLUP`) are included in this sort; those summary rows participate
            in the same `ORDER BY`, appearing last under `ASC` or first under `DESC`.

            Only supported by MySQL 8.0.12+.

        :returns `<'SelectDML'>`: The DML Select statement.

        ## Example
        >>> db.Select("*").From(db.tb).OrderBy("id", db.tb.name)
            # Equivalent to:
            SELECT * FROM db.tb AS t0
            ORDER BY id, name

        ## Example (WITH ROLLUP)
        >>> (
                db.Select("*")
                .From(db.tb)
                .GroupBy("name", with_rollup=True)
                .OrderBy("name DESC", with_rollup=True)
            )
            # Equivalent to:
            SELECT * FROM db.tb AS t0
            GROUP BY name WITH ROLLUP
            ORDER BY name DESC WITH ROLLUP
        """
        if self._order_by_clause is not None:
            self._raise_clause_already_set_error(self._order_by_clause)
        self._order_by_clause = self._gen_order_by_clause(columns, with_rollup)
        return self

    # . limit
    def Limit(self, row_count: object, offset: object = None) -> SelectDML:
        """The LIMIT clause of the SELECT statement `<'SelectDML'>`.

        :param row_count `<'int'>`: The number of limited rows to return.
        :param offset `<'int/None'>`: The offset to the first row. Defaults to `None`.
            When setting offset to a positive integer, the LIMIT clause
            will skip the specified number of offset rows before returning
            the remaining desired rows (row_count).
        :returns `<'SelectDML'>`: The DML Select statement.

        ## Example
        >>> db.Select("*").From(db.tb).Limit(10)
            # Equivalent to:
            SELECT * FROM db.tb AS t0
            LIMIT 10

        ## Example (with offset)
        >>> db.Select("*").From(db.tb).Limit(10, 5)
            # Equivalent to:
            SELECT * FROM db.tb AS t0
            LIMIT 5, 10
        """
        if self._limit_clause is not None:
            self._raise_clause_already_set_error(self._limit_clause)
        self._limit_clause = self._gen_limit_clause(row_count, offset)
        return self

    # . locking reads
    def ForUpdate(self, *tables: object, option: object = None) -> SelectDML:
        """The FOR UPDATE clause of the SELECT statement `<'SelectDML'>`.

        :param tables `<'*str/Table'>`: The specific table(s) to lock FOR UPDATE.
            If omitted, all tables in the query are locked FOR UPDATE.
        :param option `<'str/None'>`: The option of the FOR UPDATE lock. Defaults to `None`. Accepts:
            - `"NOWAIT"`: Never waits to acquire a row lock. The query executes immediately,
              failing with an error if a requested row is locked.
            - `"SKIP LOCKED"`: Never waits to acquire a row lock. The query executes immediately,
              removing locked rows from the result set.
        :returns `<'SelectDML'>`: The DML Select statement.

        ## Explanation
        - Does `NOT` block non-locking reads (plain SELECT under MVCC).
        - Blocks other transactions from obtaining exclusive locks (e.g., INSERT, UPDATE or DELETE).
        - Blocks other transactions from obtaining both the FOR SHARE and FOR UPDATE locks.
        - Therefore, no other transaction can modify or do a locking reads on the rows until
          the transaction commits.
        - Use when you plan to read and then update the same rows, and you must avoid lost-update
          or counter anomalies.

        ## Notice
        For multiple locking reads, chain the ForUpdate() method after another ForUpdate()/ForShare().

        ## Example
        >>> db.Select("*").From(db.tb).ForUpdate()
            # Equivalent to:
            SELECT * FROM db.tb AS t0
            FOR UPDATE

        ## Example (with tables)
        >>> (
                db.Select("*")
                .From(db.tb1)
                .Join(db.tb2, using="id")
                .ForUpdate(db.tb1, option="NOWAIT")
                .ForShare(db.tb2)
            )
            # Equivalent to:
            SELECT * FROM db.tb1 AS t0
            INNER JOIN db.tb2 AS t1
                USING (id)
            FOR UPDATE OF db.tb1 NOWAIT
            FOR SHARE OF db.tb2
        """
        clause: LOCKING_READS = self._gen_for_update_clause(tables, option)
        if self._locking_reads_clauses is None:
            self._locking_reads_clauses = [clause]
        else:
            list_append(self._locking_reads_clauses, clause)
        return self

    def ForShare(self, *tables: object, option: object = None) -> SelectDML:
        """The FOR SHARE clause of the SELECT statement `<'SelectDML'>`.

        :param tables `<'*str/Table'>`: The specific table(s) to lock FOR SHARE.
            If omitted, all tables in the query are locked FOR SHARE.
        :param option `<'str/None'>`: The option of the FOR SHARE lock. Defaults to `None`. Accepts:
            - `"NOWAIT"`: Never waits to acquire a row lock. The query executes immediately,
              failing with an error if a requested row is locked.
            - `"SKIP LOCKED"`: Never waits to acquire a row lock. The query executes immediately,
              removing locked rows from the result set.
        :returns `<'SelectDML'>`: The DML Select statement.

        ## Explanation
        - Does `NOT` block non-locking reads (plain SELECT under MVCC).
        - Blocks other transactions from obtaining exclusive locks (e.g., INSERT, UPDATE or DELETE).
        - Allows other transactions to also acquire FOR SHARE locks on the same rows,
          for multiple concurrent readers.
        - Use when you need to read data and ensure it cannot be changed by others before
          your transaction ends, but you do not intend to modify it yourself.

        ## Notice
        For multiple locking reads, chain the ForShare() method after another ForUpdate()/ForShare().

        ## Example
        >>> db.Select("*").From(db.tb).ForShare()
            # Equivalent to:
            SELECT * FROM db.tb AS t0
            FOR SHARE

        ## Example (with tables)
        >>> (
                db.Select("*")
                .From(db.tb1)
                .Join(db.tb2, using="id")
                .ForUpdate(db.tb1, option="NOWAIT")
                .ForShare(db.tb2)
            )
            # Equivalent to:
            SELECT * FROM db.tb1 AS t0
            INNER JOIN db.tb2 AS t1
                USING (id)
            FOR UPDATE OF db.tb1 NOWAIT
            FOR SHARE OF db.tb2
        """
        clause: LOCKING_READS = self._gen_for_share_clause(tables, option)
        if self._locking_reads_clauses is None:
            self._locking_reads_clauses = [clause]
        else:
            list_append(self._locking_reads_clauses, clause)
        return self

    # . into
    def Into(self, *variables: str) -> SelectDML:
        """The INTO (variables) clause of the SELECT statement `<'SelectDML'>`.

        :param variables `<'*str'>`: The variable(s) to store the result set.
            Each variables can be a user-defined variable, stored procedure or function parameter,
            or stored program local variable. (Within a prepared SELECT ... INTO var_list statement, only
            user-defined variables are permitted. For more information, please refer to MySQL documentation
            [Section 15.6.4.2, "Local Variable Scope and Resolution"](https://dev.mysql.com/doc/refman/8.4/en/local-variable-scope.html).

            The selected values are assigned to the variables. The number of variables must match
            the number of columns. The query should return a single row. If the query returns no rows,
            a warning with error code 1329 occurs (No data), and the variable values remain unchanged.
            If the query returns multiple rows, error 1172 occurs

        :returns `<'SelectDML'>`: The DML Select statement.

        ## Example
        >>> db.Select("id", "name").From(db.tb).Into("@id", "@name")
            # Equivalent to:
            SELECT id, name FROM db.tb AS t0
            INTO @id, @name
        """
        if self._into_clause is not None:
            self._raise_clause_already_set_error(self._into_clause)
        self._into_clause = self._gen_into_variables_clause(variables)
        return self

    # Statement ----------------------------------------------------------------------------
    @cython.ccall
    def statement(self, indent: cython.int = 0) -> str:
        """Compose the SELECT statement `<'str'>`.

        :param indent `<'int'>`: The indentation level of the statement. Defaults to 0.

        ## Example
        >>> db.Select("id", "name").From(db.tb).Where("id > 1").statement()
        >>> "SELECT id, name FROM db.tb AS t0 WHERE id > 1"
        """
        pad: str = self._validate_indent(indent)
        return self._gen_select_statement(pad)

    # Execute ------------------------------------------------------------------------------
    @cython.ccall
    def Execute(
        self,
        args: object | None = None,
        cursor: type | None = None,
        fetch: cython.bint = True,
        conn: object | None = None,
    ) -> object:
        """[sync] Execute the SELECT statement, and fetch all the results.

        :param args `<'list/tuple/DataFrame/Any'>`: The arguments for the placeholders in the statement. Defaults to `None`.

        :param cursor `<'type[Cursor]/None'>`: The cursor type (class) to use. Defaults to `None` (use pool default).
            Determines the data type of the fetched result set after executing the statement.
            Also accepts:
            1. `tuple` => `Cursor`;
            2. `dict` => `DictCursor`;
            3. `DataFrame` => `DfCursor`;

        :param fetch `<'bool'>`: Whether to fetch the result set. Defaults to `True`.
            If 'fetch=False', the SELECT statement will be executed but no results will be fetched.
            Instead returns the number of selected rows. This is useful when you want to execute a
            statement that the result set is not needed (e.g., FOR UPDATE). This is normally used
            in a transaction with 'conn' specified.

        :param conn `<'PoolSyncConnection/None'>`: The specific [sync] connection to execute the statement. Defaults to `None`.
            If 'conn=None', the statement will be executed by a (random) connection
            from the pool, and commit automatically after the statement execution.

        :returns `<'tuple[tuple]/tuple[dict]/DataFrame/int'>`: The result set of the SELECT statement.
            Returns the number of selected rows `<'int'>` only when 'fetch=False'.

        ## Example
        >>> data = (
                db.Select("t0.id", "t0.name", "COUNT(*) AS count")
                .From(db.tb1)
                .Join(db.tb2, "t0.name = t1.name")
                .Where("t0.id > %s")
                .GroupBy("t0.name")
                .Having("t0.name = %s")
                .Execute([10, "a0"])
            )
            # Equivalent to:
            SELECT t0.id, t0.name, COUNT(*) AS count
            FROM db.tb1 AS t0
            INNER JOIN db.tb2 AS t1
                ON t0.name = t1.name
            WHERE t0.id > 10
            GROUP BY t0.name
            HAVING t0.name = 'a0';

        ## Example (connection)
        >>> with db.transaction() as conn:
                (
                    db.Select("*")
                    .From(db.tb1)
                    .Where(name = %s)
                    .ForUpdate()
                    .Execute("test", conn=conn, fetch=False)
                )
                db.Update(...)...
            # Equivalent to:
            BEGIN;
            SELECT * FROM db.tb1 AS t0
            WHERE name = 'test' FOR UPDATE;
            UPDATE ...;
            COMMIT;
        """
        return self._Execute(args, cursor, fetch, False, conn)

    async def aioExecute(
        self,
        args: object | None = None,
        cursor: type | None = None,
        fetch: cython.bint = True,
        conn: object | None = None,
    ) -> object:
        """[async] Execute the SELECT statement, and fetch all the results.

        :param args `<'list/tuple/DataFrame/Any'>`: The arguments for the placeholders in the statement. Defaults to `None`.

        :param cursor `<'type[Cursor]/None'>`: The cursor type (class) to use. Defaults to `None` (use pool default).
            Determines the data type of the fetched result set after executing the statement.
            Also accepts:
            1. `tuple` => `Cursor`;
            2. `dict` => `DictCursor`;
            3. `DataFrame` => `DfCursor`;

        :param fetch `<'bool'>`: Whether to fetch the result set. Defaults to `True`.
            If 'fetch=False', the SELECT statement will be executed but no results will be fetched.
            Instead returns the number of selected rows. This is useful when you want to execute a
            statement that the result set is not needed (e.g., FOR UPDATE). This is normally used
            in a transaction with 'conn' specified.

        :param conn `<'PoolConnection/None'>`: The specific [async] connection to execute the statement. Defaults to `None`.
            If 'conn=None', the statement will be executed by a (random) connection
            from the pool, and commit automatically after the statement execution.

        :returns `<'tuple[tuple]/tuple[dict]/DataFrame/int'>`: The result set of the SELECT statement.
            Returns the number of selected rows `<'int'>` only when 'fetch=False'.

        ## Example
        >>> data = await (
                db.Select("t0.id", "t0.name", "COUNT(*) AS count")
                .From(db.tb1)
                .Join(db.tb2, "t0.name = t1.name")
                .Where("t0.id > %s")
                .GroupBy("t0.name")
                .Having("t0.name = %s")
                .aioExecute([10, "a0"])
            )
            # Equivalent to:
            SELECT t0.id, t0.name, COUNT(*) AS count FROM db.tb1 AS t0
            INNER JOIN db.tb2 AS t1
                ON t0.name = t1.name
            WHERE t0.id > 10
            GROUP BY t0.name
            HAVING t0.name = 'a0';

        ## Example (connection)
        >>> async with db.transaction() as conn:
                await (
                    db.Select("*")
                    .From(db.tb1)
                    .Where(name = %s)
                    .ForUpdate()
                    .aioExecute("test", conn=conn, fetch=False)
                )
                await db.Update(...)...
            # Equivalent to:
            BEGIN;
            SELECT * FROM db.tb1 AS t0
            WHERE name = 'test' FOR UPDATE;
            UPDATE ...;
            COMMIT;
        """
        return await self._aioExecute(args, cursor, fetch, False, conn)

    # Validate -----------------------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(-1, check=False)
    def _validate_join_clause_order(self) -> cython.bint:
        """(internal) Validate if the order of the JOIN cluase is correct."""
        if self._clause_id not in (
            utils.DML_CLAUSE.SELECT,
            utils.DML_CLAUSE.FROM,
            utils.DML_CLAUSE.JOIN,
        ):
            self._raise_clause_order_error("JOIN", ["SELECT", "FROM", "another JOIN"])
        return True


# Insert - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
@cython.cclass
class InsertDML(DML):
    """Represents the DML INSERT statement."""

    # clauses
    _insert_clause: INSERT
    _columns_clause: INSERT_COLUMNS
    _values_clause: INSERT_VALUES
    _set_clause: SET
    _on_dup_key_update_clause: ON_DUPLICATE
    # internal
    _insert_mode: cython.int

    def __init__(self, db_name: str, pool: Pool):
        """The DML INSERT statement.

        :param db_name `<'str'>`: The name of the database.
        :param pool `<'Pool'>`: The connection pool to handle the statement execution.
        """
        super().__init__("INSERT", db_name, pool)
        # clauses
        self._insert_clause = None
        self._columns_clause = None
        self._values_clause = None
        self._set_clause = None
        self._on_dup_key_update_clause = None
        # internal
        self._insert_mode = utils.INSERT_MODE.INCOMPLETE

    # Clause -------------------------------------------------------------------------------
    # . insert
    @cython.ccall
    def Insert(
        self,
        table: object,
        partition: object = None,
        ignore: cython.bint = False,
        priority: object = None,
    ) -> InsertDML:
        """The INSERT clause of the INSERT statement `<'InsertDML'>`.

        :param table `<'str/Table'>`: The table to insert the data.

        :param partition `<'str/Partition/list/tuple/None'>`: The partition(s) of the table. Defaults to `None`.

        :param ignore `<'bool'>`: Whether to ignore the (ignorable) errors. Defaults to `False`.
            When 'ignore=True', ignorable errors—such as duplicate-key or primary-key violations,
            unmatched partitions, and data conversion issues—are converted to warnings. Rows
            causing these errors are skipped or adjusted rather than aborting the statement.

        :param priority `<'str/None'>`: Optional INSERT prioirty modifier. Defaults to `None`.
            Only applies to table-locking engines (MyISAM, MEMORY, MERGE). Accepts:

            - `"LOW_PRIORITY"`: Delays the INSERT until no other clients are reading the table
                (even those who start reading while your insert is waiting). Disables concurrent
                inserts—so it can block for a very long time and is normally not recommended on
                MyISAM tables.
            - `"HIGH_PRIORITY"`: Overrides any server setting that forces low-priority updates
                and likewise disables concurrent inserts.

        :returns `<'InsertDML'>`: The DML Insert statement.

        ## Example (VALUES)
        >>> db.Insert(db.tb).Columns("id", "name").Values(2)
            # Equivalent to:
            INSERT INTO db.tb
                (id, name)
            VALUES (%s,%s)

        ## Example (SET)
        >>> db.Insert(db.tb).Set("id = 2", "name = 'test'")
            # Equivalent to:
            INSERT INTO db.tb
            SET id = 2, name = 'test'

        ## Example (SELECT)
        >>> (
                db.Insert(db.tb1)
                .Select("id", "name")
                .From(db.tb2)
                .Where("name = 'test'")
            )
            # Equivalent to:
            INSERT INTO db.tb1
            SELECT id, name
            FROM db.tb2 AS t0
            WHERE name = 'test'
        """
        if self._insert_clause is not None:
            self._raise_clause_already_set_error(self._insert_clause)
        if self._clause_id != utils.DML_CLAUSE.NONE:
            self._raise_clause_error("must start with the INSERT clause.")
        self._insert_clause = self._gen_insert_clause(
            table, partition, ignore, priority
        )
        return self

    # . columns
    def Columns(self, *columns: object) -> InsertDML:
        """The COLUMNS clause of the INSERT statement `<'InsertDML'>`.

        :param columns `<'*str/Column'>`: The column(s) to insert the data.
        :returns `<'InsertDML'>`: The DML Insert statement.

        ## Notice
        - The COLUMNS clause must be placed after the INSERT clause.

        ## Example
        >>> db.Insert(db.tb).Columns(db.tb.id, db.tb.name).Values(2)
            # Equivalent to:
            INSERT INTO db.tb
                (id, name)
            VALUES (%s,%s)
        """
        if self._clause_id != utils.DML_CLAUSE.INSERT:
            self._raise_clause_order_error("COLUMNS", ["INSERT"])
        if self._columns_clause is not None:
            self._raise_clause_already_set_error(self._columns_clause)
        self._columns_clause = self._gen_insert_columns_clause(columns)
        return self

    # . values
    def Values(self, placeholders: cython.int) -> InsertDML:
        """The VALUES clause of the INSERT statement `<'InsertDML'>`.

        :param placeholders `<'int'>`: The number of placeholders for each row of the insert data.
            If the `Columns()` method is used by the INSERT statement, the number of placeholders
            must match the specified columns. Otherwise, the number of placeholders must match
            the number of columns in the target insert table.
        :returns `<'InsertDML'>`: The DML Insert statement.

        ## Notice
        - The VALUES clause must be placed after the INSERT or COLUMNS clause.

        ## Example
        >>> db.Insert(db.tb).Values(2)
            # Equivalent to:
            INSERT INTO db.tb
            VALUES (%s,%s)
        """
        if self._clause_id not in (
            utils.DML_CLAUSE.INSERT,
            utils.DML_CLAUSE.INSERT_COLUMNS,
        ):
            self._raise_clause_order_error("VALUES", ["INSERT", "COLUMNS"])
        if self._values_clause is not None:
            self._raise_clause_already_set_error(self._values_clause)
        self._values_clause = self._gen_insert_values_clause(placeholders)
        return self

    # . set
    def Set(self, *assignments: object) -> InsertDML:
        """The SET clause of the INSERT statement `<'InsertDML'>`.

        :param assignments `<'*str'>`: The assignment(s) on how to insert the data.
        :returns `<'InsertDML'>`: The DML Insert statement.

        ## Notice
        - The SET clause must be placed after the INSERT or COLUMNS clause.

        ## Example
        >>> db.Insert(db.tb).Set("id=%s", "name=%s")
            # Equivalent to:
            INSERT INTO db.tb
            SET id=%s, name=%s
        """
        if self._clause_id != utils.DML_CLAUSE.INSERT:
            self._raise_clause_order_error("SET", ["INSERT"])
        if self._set_clause is not None:
            self._raise_clause_already_set_error(self._set_clause)
        self._set_clause = self._gen_set_clause(assignments)
        return self

    # . row alias
    def RowAlias(self, row_alias: object, *col_alias: object) -> InsertDML:
        """The ROW ALIAS clause of the INSERT statement `<'InsertDML'>`.

        :param row_alias `<'str'>`: The alias of the insert row.
        :param col_alias `<'*str'>`: The alias of the column(s) in the insert row.
        :returns `<'InsertDML'>`: The DML Insert statement.

        ## Notice
        - The ROW ALIAS clause must be placed after the VALUES or SET clause.

        ## Example (VALUES)
        >>> db.Insert(db.tb).Values(2).RowAlias("new")
            # Equivalent to:
            INSERT INTO db.tb
            VALUES (%s,%s) AS new

        ## Example (SET)
        >>> db.Insert(db.tb).Set("id=%s", "name=%s").RowAlias("new", "i", "n")
            # Equivalent to:
            INSERT INTO db.tb
            SET id=%s, name=%s
            AS new (i, n)
        """
        clause: ROW_ALIAS = self._gen_row_alias_clause(row_alias, col_alias)
        if self._clause_id == utils.DML_CLAUSE.INSERT_VALUES:
            self._values_clause._add_row_alias(clause)
        elif self._clause_id == utils.DML_CLAUSE.SET:
            self._set_clause._add_row_alias(clause)
        else:
            self._raise_clause_order_error("ROW ALIAS", ["VALUES", "SET"])
        return self

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # . with
    def With(
        self,
        name: object,
        subquery: object,
        *columns: object,
        recursive: cython.bint = False,
    ) -> InsertDML:
        """The WITH (Common Table Expressions) of the INSERT statement `<'InsertDML'>`.

        :param name `<'str'>`: The name of the CTE, which can be used as a table reference in the statement.
        :param subquery `<'str/SelectDML'>`: The subquery that produces the CTE result set.
        :param columns `<'*str/Column'>`: The column names of the result set.
            If specified, the number of columns must be the same as the result set of the subquery.
        :param recursive `<'bool'>`: Whether the CTE is recursive. Defaults to `False`.
            A CTE is recursive if its subquery refers to its own name.
            For more information, refer to MySQL documentation
            ["Recursive Common Table Expressions"](https://dev.mysql.com/doc/refman/8.4/en/with.html#common-table-expressions-recursive).
        :returns `<'InsertDML'>`: The DML Insert statement.

        ## Notice
        - The WITH clause must be placed after the INSERT or COLUMNS clause.
        - For multiple CTEs, chain the With() method after another With().

        ## Example
        >>> (
                db.Insert(db.tb1)
                .Columns("id", "name")
                .With("cte1", db.Select("id", "name").From(db.tb2))
                .With("cte2", db.Select("id", "name").From(db.tb3))
                .Select("*")
                .From("cte1")
                .Union(db.Select("*").From("cte2"))
            )
            # Equivalent to:
            INSERT INTO db.tb1 (id, name)
            WITH cte1 AS (
                SELECT id, name FROM db.tb2 AS t0
            ), cte2 AS (
                SELECT id, name FROM db.tb3 AS t0
            )
            SELECT * FROM cte1 AS t0
            UNION DISTINCT (
                SELECT * FROM cte2 AS t0
            )
        """
        if self._clause_id not in (
            utils.DML_CLAUSE.INSERT,
            utils.DML_CLAUSE.INSERT_COLUMNS,
            utils.DML_CLAUSE.WITH,
        ):
            self._raise_clause_order_error(
                "WITH", ["INSERT", "COLUMNS", "another WITH"]
            )
        clause: WITH = self._gen_with_clause(name, subquery, columns, recursive)
        if self._with_clauses is None:
            self._with_clauses = [clause]
        else:
            clause._primary = False
            list_append(self._with_clauses, clause)
        return self

    # . select
    def Select(
        self,
        *expressions: object,
        distinct: cython.bint = False,
        high_priority: cython.bint = False,
        straight_join: cython.bint = False,
        sql_buffer_result: cython.bint = False,
    ) -> InsertDML:
        """The SELECT clause of the INSERT statement `<'InsertDML'>`.

        :param expressions `<'*str/SQLFunction/Column'>`: The (expression of) the column(s) to retrieve.

        :param distinct `<'bool'>`: If `True`, specifies removal of duplicate rows from
            the result set. Defaults to `False`.

        :param high_priority `<'bool'>`: If `True`, gives the SELECT higher priority than
            a statement that updates a table. Defaults to `False`. Should only be used for
            queries that are very fast and must be done at once. A SELECT HIGH_PRIORITY
            query that is issued while the table is locked for reading runs even if there
            is an update statement waiting for the table to be free. This affects only storage
            engines that use only table-level locking (such as MyISAM, MEMORY, and MERGE).

        :param straight_join `<'bool'>`: If `True`, forces the optimizer to join the tables
            in the order in which they are listed in the FROM clause. Defaults to `False`.
            Can be used to speed up a query if the optimizer joins the tables in nonoptimal
            order.

        :param sql_buffer_result `<'bool'>`: If `True`, forces the result to be put into a
            temporary table. Defaults to `False`. This helps MySQL free the table locks early
            and helps in cases where it takes a long time to send the result set to the client.
            This modifier can be used only for top-level SELECT statements, not for subqueries
            or following UNION.

        :returns `<'InsertDML'>`: The DML Insert statement.

        ## Notice
        - The SELECT clause must be placed after the INSERT, COLUMNS or WITH clause.

        ## Example
        >>> db.Insert(db.tb1).Select("*").From(db.tb2)
            # Equivalent to:
            INSERT INTO db.tb1
            SELECT * FROM db.tb2 AS t0
        """
        if self._clause_id not in (
            utils.DML_CLAUSE.INSERT,
            utils.DML_CLAUSE.INSERT_COLUMNS,
            utils.DML_CLAUSE.WITH,
        ):
            self._raise_clause_order_error("SELECT", ["INSERT", "COLUMNS", "WITH"])
        if self._select_clause is not None:
            self._raise_clause_already_set_error(self._select_clause)
        self._select_clause = self._gen_select_clause(
            expressions, distinct, high_priority, straight_join, sql_buffer_result
        )
        return self

    # . from
    def From(
        self,
        table: object,
        partition: object = None,
        alias: object = None,
    ) -> InsertDML:
        """The FROM clause of the INSERT statement `<'InsertDML'>`.

        :param table `<'str/Table/SelectDML'>`: The table (reference) from which to retrieve data.
            Only accepts one table reference. For multiple-table, please use the explicit JOIN clause instead.
        :param partition `<'str/Partition/list/tuple/None'>`: The partition(s) of the table. Defaults to `None`.
        :param alias `<'str/None'>`: The alias of the table (reference). Defaults to `None`.
            When 'alias=None', a default alias is automatically generated based on the count
            of the tables adopting default alias in the same query block (starting from 't0').
        :returns `<'InsertDML'>`: The DML Insert statement.

        ## Notice
        - The FROM clause must be placed after the SELECT clause.
        - To set index hints, chain the `UseIndex()`, `IgnoreIndex()` and
          `ForceIndex()` methods after From().

        ## Example
        - Please refer to the <'SelectDML'> statment `From()` method.
        """
        if self._clause_id != utils.DML_CLAUSE.SELECT:
            self._raise_clause_order_error("FROM", ["SELECT"])
        if self._from_clause is not None:
            self._raise_clause_already_set_error(self._from_clause)
        self._from_clause = self._gen_from_clause(table, partition, alias)
        return self

    # . join
    def Join(
        self,
        table: object,
        *on: str,
        using: object = None,
        partition: object = None,
        alias: object = None,
    ) -> InsertDML:
        """The (INNER) JOIN clause of the INSERT statement `<'InsertDML'>`.

        :param table `<'str/Table/SelectDML'>`: The table (reference) to join.
        :param on `<'*str'>`: The ON condition(s) specify how to join the table.
        :param using `<'str/Column/list/tuple/None'>`: The USING condition(s) of the common column(s) between the tables. Defaults to `None`.
            When on is specified, using is ignored.
        :param partition `<'str/Partition/list/tuple/None'>`: The partition(s) of the join table. Defaults to `None`.
        :param alias `<'str/None'>`: The alias of the join table. Defaults to `None`.
            When 'alias=None', a default alias is automatically generated based on the count
            of the tables adopting default alias in the same query block (starting from 't0').
        :returns `<'InsertDML'>`: The DML Insert statement.

        ## Notice
        - The JOIN clause must be placed after the SELECT or FROM clause.
        - For multiple joins, chain the `Join()` method after another `Join()`.
        - To set index hints, chain the `UseIndex()`, `IgnoreIndex()` and
          `ForceIndex()` methods after `Join()`.

        ## Example
        - Please refer to the <'SelectDML'> statment `Join()` method.
        """
        self._validate_join_clause_order()
        clause: JOIN = self._gen_inner_join_clause(table, on, using, partition, alias)
        if self._join_clauses is None:
            self._join_clauses = [clause]
        else:
            list_append(self._join_clauses, clause)
        return self

    def LeftJoin(
        self,
        table: object,
        *on: object,
        using: object = None,
        partition: object = None,
        alias: object = None,
    ) -> InsertDML:
        """The LEFT JOIN clause of the INSERT statement `<'InsertDML'>`.

        :param table `<'str/Table/SelectDML'>`: The table (reference) to join.
        :param on `<'*str'>`: The ON condition(s) specify how to join the table.
        :param using `<'str/Column/list/tuple/None'>`: The USING condition(s) of the common column(s) between the tables. Defaults to `None`.
            When on is specified, using is ignored.
        :param partition `<'str/Partition/list/tuple/None'>`: The partition(s) of the join table. Defaults to `None`.
        :param alias `<'str/None'>`: The alias of the join table. Defaults to `None`.
            When 'alias=None', a default alias is automatically generated based on the count
            of the tables adopting default alias in the same query block (starting from 't0').
        :returns `<'InsertDML'>`: The DML Insert statement.

        ## Notice
        - The JOIN clause must be placed after the SELECT or FROM clause.
        - For multiple joins, chain the `Join()` method after another `Join()`.
        - To set index hints, chain the `UseIndex()`, `IgnoreIndex()` and
          `ForceIndex()` methods after `Join()`.

        ## Example
        - Please refer to the <'SelectDML'> statment `LeftJoin()` method.
        """
        self._validate_join_clause_order()
        clause: JOIN = self._gen_left_join_clause(table, on, using, partition, alias)
        if self._join_clauses is None:
            self._join_clauses = [clause]
        else:
            list_append(self._join_clauses, clause)
        return self

    def RightJoin(
        self,
        table: object,
        *on: object,
        using: object = None,
        partition: object = None,
        alias: object = None,
    ) -> InsertDML:
        """The RIGHT JOIN clause of the INSERT statement `<'InsertDML'>`.

        :param table `<'str/Table/SelectDML'>`: The table (reference) to join.
        :param on `<'*str'>`: The ON condition(s) specify how to join the table.
        :param using `<'str/Column/list/tuple/None'>`: The USING condition(s) of the common column(s) between the tables. Defaults to `None`.
            When on is specified, using is ignored.
        :param partition `<'str/Partition/list/tuple/None'>`: The partition(s) of the join table. Defaults to `None`.
        :param alias `<'str/None'>`: The alias of the join table. Defaults to `None`.
            When 'alias=None', a default alias is automatically generated based on the count
            of the tables adopting default alias in the same query block (starting from 't0').
        :returns `<'InsertDML'>`: The DML Insert statement.

        ## Notice
        - The JOIN clause must be placed after the SELECT or FROM clause.
        - For multiple joins, chain the `Join()` method after another `Join()`.
        - To set index hints, chain the `UseIndex()`, `IgnoreIndex()` and
          `ForceIndex()` methods after `Join()`.

        ## Example
        - Please refer to the <'SelectDML'> statment `RightJoin()` method.
        """
        self._validate_join_clause_order()
        clause: JOIN = self._gen_right_join_clause(table, on, using, partition, alias)
        if self._join_clauses is None:
            self._join_clauses = [clause]
        else:
            list_append(self._join_clauses, clause)
        return self

    def StraightJoin(
        self,
        table: object,
        *on: object,
        using: object = None,
        partition: object = None,
        alias: object = None,
    ) -> InsertDML:
        """The STRAIGHT_JOIN clause of the INSERT statement `<'InsertDML'>`.

        :param table `<'str/Table/SelectDML'>`: The table (reference) to join.
        :param on `<'*str'>`: The ON condition(s) specify how to join the table.
        :param using `<'str/Column/list/tuple/None'>`: The USING condition(s) of the common column(s) between the tables. Defaults to `None`.
            When on is specified, using is ignored.
        :param partition `<'str/Partition/list/tuple/None'>`: The partition(s) of the join table. Defaults to `None`.
        :param alias `<'str/None'>`: The alias of the join table. Defaults to `None`.
            When 'alias=None', a default alias is automatically generated based on the count
            of the tables adopting default alias in the same query block (starting from 't0').
        :returns `<'InsertDML'>`: The DML Insert statement.

        ## Notice
        - The JOIN clause must be placed after the SELECT or FROM clause.
        - For multiple joins, chain the `Join()` method after another `Join()`.
        - To set index hints, chain the `UseIndex()`, `IgnoreIndex()` and
          `ForceIndex()` methods after `Join()`.

        ## Example
        - Please refer to the <'SelectDML'> statment `StraightJoin()` method.
        """
        self._validate_join_clause_order()
        clause: JOIN = self._gen_straight_join_clause(
            table, on, using, partition, alias
        )
        if self._join_clauses is None:
            self._join_clauses = [clause]
        else:
            list_append(self._join_clauses, clause)
        return self

    def CrossJoin(
        self,
        table: object,
        *on: object,
        using: object = None,
        partition: object = None,
        alias: object = None,
    ) -> InsertDML:
        """The CROSS JOIN clause of the INSERT statement `<'InsertDML'>`.

        :param table `<'str/Table/SelectDML'>`: The table (reference) to join.
        :param on `<'*str'>`: The ON condition(s) specify how to join the table.
        :param using `<'str/Column/list/tuple/None'>`: The USING condition(s) of the common column(s) between the tables. Defaults to `None`.
            When on is specified, using is ignored.
        :param partition `<'str/Partition/list/tuple/None'>`: The partition(s) of the join table. Defaults to `None`.
        :param alias `<'str/None'>`: The alias of the join table. Defaults to `None`.
            When 'alias=None', a default alias is automatically generated based on the count
            of the tables adopting default alias in the same query block (starting from 't0').
        :returns `<'InsertDML'>`: The DML Insert statement.

        ## Notice
        - The JOIN clause must be placed after the SELECT or FROM clause.
        - For multiple joins, chain the `Join()` method after another `Join()`.
        - To set index hints, chain the `UseIndex()`, `IgnoreIndex()` and
          `ForceIndex()` methods after `Join()`.

        ## Example
        - Please refer to the <'SelectDML'> statment `CrossJoin()` method.
        """
        self._validate_join_clause_order()
        clause: JOIN = self._gen_cross_join_clause(table, on, using, partition, alias)
        if self._join_clauses is None:
            self._join_clauses = [clause]
        else:
            list_append(self._join_clauses, clause)
        return self

    def NaturalJoin(
        self,
        table: object,
        join_method: object = "INNER",
        partition: object = None,
        alias: object = None,
    ) -> InsertDML:
        """The NATURAL JOIN clause of the INSERT statement `<'InsertDML'>`.

        :param table `<'str/Table/SelectDML'>`: The table (reference) to join.
        :param join_method `<'str'>`: The join method. Defaults to `"INNER"`.
            Accepts: `"INNER"`, `"LEFT"`, `"RIGHT"`.
        :param partition `<'str/Partition/list/tuple/None'>`: The partition(s) of the join table. Defaults to `None`.
        :param alias `<'str/None'>`: The alias of the join table. Defaults to `None`.
            When 'alias=None', a default alias is automatically generated based on the count
            of the tables adopting default alias in the same query block (starting from 't0').
        :returns `<'InsertDML'>`: The DML Insert statement.

        ## Notice
        - The JOIN clause must be placed after the SELECT or FROM clause.
        - For multiple joins, chain the `Join()` method after another `Join()`.
        - To set index hints, chain the `UseIndex()`, `IgnoreIndex()` and
          `ForceIndex()` methods after `Join()`.

        ## Example
        - Please refer to the <'SelectDML'> statment `NaturalJoin()` method.
        """
        self._validate_join_clause_order()
        clause: JOIN = self._gen_natural_join_clause(
            table, join_method, partition, alias
        )
        if self._join_clauses is None:
            self._join_clauses = [clause]
        else:
            list_append(self._join_clauses, clause)
        return self

    # . index hints
    def UseIndex(self, *indexes: object, scope: object = None) -> InsertDML:
        """The USE INDEX clause of the INSERT statement `<'InsertDML'>`.

        :param indexes `<'*str/Index'>`: The index(es) to use by the INDEX HINTS.
        :param scope `<'str/None'>`: The scope of the INDEX HINTS. Defaults to `None`.
            Accepts: `"JOIN"`, `"ORDER BY"` and `"GROUP BY"`.
            This provides more fine-grained control over optimizer selection of an execution plan
            for various phases of query processing. To affect only the indexes used when MySQL
            decides how to find rows in the table and how to process joins, use FOR JOIN. To
            influence index usage for sorting or grouping rows, use FOR ORDER BY or FOR GROUP BY.
        :returns `<'InsertDML'>`: The DML Insert statement.

        ## Notice
        - The USE INDEX clause must be placed after the FROM or JOIN
          clause or chain after another INDEX HINTS clause.

        ## Example
        - Please refer to the <'SelectDML'> statment `UseIndex()` method.
        """
        # fmt: off
        if self._clause_id == utils.DML_CLAUSE.FROM:
            clause: CLAUSE = self._from_clause
        elif self._clause_id == utils.DML_CLAUSE.JOIN and self._join_clauses is not None:
            clause: CLAUSE = self._join_clauses[list_len(self._join_clauses) - 1]
        else:
            self._raise_clause_order_error(
                "USE INDEX", ["FROM", "JOIN", "another INDEX HINTS"]
            )
        # fmt: on
        clause._add_index_hints(self._gen_use_index_clause(indexes, scope))
        return self

    def IgnoreIndex(self, *indexes: object, scope: object = None) -> InsertDML:
        """The IGNORE INDEX clause of the INSERT statement `<'InsertDML'>`.

        :param indexes `<'*str/Index'>`: The index(es) to ignore by the INDEX HINTS.
        :param scope `<'str/None'>`: The scope of the INDEX HINTS. Defaults to `None`.
            Accepts: `"JOIN"`, `"ORDER BY"` and `"GROUP BY"`.
            This provides more fine-grained control over optimizer selection of an execution plan
            for various phases of query processing. To affect only the indexes used when MySQL
            decides how to find rows in the table and how to process joins, use FOR JOIN. To
            influence index usage for sorting or grouping rows, use FOR ORDER BY or FOR GROUP BY.
        :returns `<'InsertDML'>`: The DML Insert statement.

        ## Notice
        - The IGNORE INDEX clause must be placed after the FROM or JOIN
          clause or chain after another INDEX HINTS clause.

        ## Example
        - Please refer to the <'SelectDML'> statment `IgnoreIndex()` method.
        """
        # fmt: off
        if self._clause_id == utils.DML_CLAUSE.FROM:
            clause: CLAUSE = self._from_clause
        elif self._clause_id == utils.DML_CLAUSE.JOIN and self._join_clauses is not None:
            clause: CLAUSE = self._join_clauses[list_len(self._join_clauses) - 1]
        else:
            self._raise_clause_order_error(
                "IGNORE INDEX", ["FROM", "JOIN", "another INDEX HINTS"]
            )
        # fmt: on
        clause._add_index_hints(self._gen_ignore_index_clause(indexes, scope))
        return self

    def ForceIndex(self, *indexes: object, scope: object = None) -> InsertDML:
        """The FORCE INDEX clause of the INSERT statement `<'InsertDML'>`.

        :param indexes `<'*str/Index'>`: The index(es) to force by the INDEX HINTS.
        :param scope `<'str/None'>`: The scope of the INDEX HINTS. Defaults to `None`.
            Accepts: `"JOIN"`, `"ORDER BY"` and `"GROUP BY"`.
            This provides more fine-grained control over optimizer selection of an execution plan
            for various phases of query processing. To affect only the indexes used when MySQL
            decides how to find rows in the table and how to process joins, use FOR JOIN. To
            influence index usage for sorting or grouping rows, use FOR ORDER BY or FOR GROUP BY.
        :returns `<'InsertDML'>`: The DML Insert statement.

        ## Notice
        - The FORCE INDEX clause must be placed after the FROM or JOIN
          clause or chain after another INDEX HINTS clause.

        ## Example
        - Please refer to the <'SelectDML'> statment `ForceIndex()` method.
        """
        # fmt: off
        if self._clause_id == utils.DML_CLAUSE.FROM:
            clause: CLAUSE = self._from_clause
        elif self._clause_id == utils.DML_CLAUSE.JOIN and self._join_clauses is not None:
            clause: CLAUSE = self._join_clauses[list_len(self._join_clauses) - 1]
        else:
            self._raise_clause_order_error(
                "FORCE INDEX", ["FROM", "JOIN", "another INDEX HINTS"]
            )
        # fmt: on
        clause._add_index_hints(self._gen_force_index_clause(indexes, scope))
        return self

    # . where
    def Where(
        self,
        *conds: str,
        in_conds: dict | None = None,
        not_in_conds: dict | None = None,
    ) -> InsertDML:
        """The WHERE clause of the INSERT statement `<'InsertDML'>`.

        :param conds `<'*str'>`: The condition(s) that rows must satisfy to be selected.
        :param in_conds `<'dict/None'>`: The IN condition(s). Defaults to `None`.
        :param not_in_conds `<'dict/None'>`: The NOT IN condition(s). Defaults to `None`.
        :returns `<'InsertDML'>`: The DML Insert statement.

        ## Explanation
        - All conditions are combined with `AND` logic by default.
        - Prepend the condition with "`OR `" to alter the default behavior.
        - The 'in_conds' and 'not_in_conds' arguments must be a dictionary,
          where the keys should be the columns and values the desired ones
          to check against. Values can be any escapable objects or a subquery
          `<'SelectDML'>` instance, but `NOT` strings with placeholders.

        ## Example
        - Please refer to the <'SelectDML'> statment `Where()` method.
        """
        if self._where_clause is not None:
            self._raise_clause_already_set_error(self._where_clause)
        self._where_clause = self._gen_where_clause(conds, in_conds, not_in_conds)
        return self

    # . group by
    def GroupBy(self, *columns: object, with_rollup: cython.bint = False) -> InsertDML:
        """The GROUP BY clause of the INSERT statement `<'InsertDML'>`.

        :param columns `<'*str/Column'>`: The (expression of) column(s) to group by with.
        :param with_rollup `<'bool'>`: Whether to summary output to include extra rows
            that represent higher-level (super-aggregate) grand total.
        :returns `<'InsertDML'>`: The DML Insert statement.

        ## Example
        - Please refer to the <'SelectDML'> statment `GroupBy()` method.
        """
        if self._group_by_clause is not None:
            self._raise_clause_already_set_error(self._group_by_clause)
        self._group_by_clause = self._gen_group_by_clause(columns, with_rollup)
        return self

    # . having
    def Having(
        self,
        *conds: str,
        in_conds: dict | None = None,
        not_in_conds: dict | None = None,
    ) -> InsertDML:
        """The HAVING clause of the INSERT statement `<'InsertDML'>`.

        :param conds `<'*str'>`: The condition(s) that rows must satisfy to be selected.
        :param in_conds `<'dict/None'>`: The IN condition(s). Defaults to `None`.
        :param not_in_conds `<'dict/None'>`: The NOT IN condition(s). Defaults to `None`.
        :returns `<'InsertDML'>`: The DML Insert statement.

        ## Explanation
        - All conditions are combined with `AND` logic by default.
        - Prepend the condition with "`OR `" to alter the default behavior.
        - The 'in_conds' and 'not_in_conds' arguments must be a dictionary,
          where the keys should be the columns and values the desired ones
          to check against. Values can be any escapable objects or a subquery
          `<'SelectDML'>` instance, but `NOT` strings with placeholders.

        ## Notice
        The HAVING clause, like the WHERE clause, specifies selection conditions.
        The WHERE clause specifies conditions on columns in the select list, but
        cannot refer to aggregate functions. The HAVING clause specifies conditions
        on groups, typically formed by the GROUP BY clause. The query result includes
        only groups satisfying the HAVING conditions.

        ## Example
        - Please refer to the <'SelectDML'> statment `Where()` method for similar usage.
        """
        if self._having_clause is not None:
            self._raise_clause_already_set_error(self._having_clause)
        self._having_clause = self._gen_having_clause(conds, in_conds, not_in_conds)
        return self

    # . window
    def Window(
        self,
        name: object,
        partition_by: object | None = None,
        order_by: object | None = None,
        frame_clause: str | None = None,
    ) -> InsertDML:
        """The WINDOW clause of the INSERT statement `<'InsertDML'>`.

        :param name `<'str'>`: The name of the window.

        :param partition_by `<'str/Column/list/tuple/None'>`: Specifies how to divide the query rows into groups. Defaults to `None`.
            The window function result for a given row is based on the rows of the partition
            that contains the row. If 'partition_by=None', there is a single partition
            consisting of all query rows.

        :param order_by `<'str/Column/list/tuple/None'>`: Specifies how to sort rows in each partition. Defaults to `None`.
            Partition rows that are equal according to the ORDER BY clause are considered peers.
            If 'order_by=None', partition rows are unordered, with no processing order implied,
            and all partition rows are peers.

        :param frame_clause `<'str/None'>`: Specifies how to define the frame (subset of the current partition). Defaults to `None`.
            Frames are determined with respect to the current row, which enables a frame to move
            within a partition depending on the location of the current row within its partition.

            Examples: By defining a frame to be all rows from the partition start to the current
            row, you can compute running totals for each row. By defining a frame as extending N
            rows on either side of the current row, you can compute rolling averages.

            For more information, please refer to the MySQL documentation
            [Section 14.20.3, "Window Function Frame Specification"](https://dev.mysql.com/doc/refman/8.0/en/window-function-descriptions.html).

        :returns `<'InsertDML'>`: The DML Insert statement.

        ## Notice
        - For multiple windows, chain the Window() method after another Window().

        ## Example
        - Please refer to the <'SelectDML'> statment `Window()` method.
        """
        clause: WINDOW = self._gen_window_clause(
            name, partition_by, order_by, frame_clause
        )
        if self._window_clauses is None:
            self._window_clauses = [clause]
        else:
            clause._primary = False
            list_append(self._window_clauses, clause)
        return self

    # . set operations
    def Union(
        self,
        subquery: object,
        all: cython.bint = False,
    ) -> InsertDML:
        """The UNION clause of the INSERT statement `<'InsertDML'>`.

        `UNION` (SET OPERATION) combines the result from multiple
        query blocks into a single result set.

        :param subquery `<'SelectDML'>`: The subquery to union with the main query block.
        :param all `<'bool'>`: Keep all the result if `True`, else sort & deduplicate the result set. Defaults to `False`.
        :returns `<'InsertDML'>`: The DML Insert statement.

        ## Notice
        - For multiple subqueries, chain the Union() method after another SET OPERATION.

        ## Example
        - Please refer to the <'SelectDML'> statment `Union()` method.
        """
        clause: CLAUSE = self._gen_union_clause(subquery, all)
        if self._set_op_clauses is None:
            self._set_op_clauses = [clause]
        else:
            list_append(self._set_op_clauses, clause)
        return self

    def Intersect(
        self,
        subquery: object,
        all: cython.bint = False,
    ) -> InsertDML:
        """The INTERSECT clause of the INSERT statement `<'InsertDML'>`.

        `INTERSECT` (SET OPERATION) limits the result from multiple
        query blocks to those rows which are common to all.

        :param subquery `<'SelectDML'>`: The subquery to intersect with the main query block.
        :param all `<'bool'>`: Keep all the result if `True`, else sort & deduplicate the result set. Defaults to `False`.
        :returns `<'InsertDML'>`: The DML Insert statement.

        ## Notice
        - For multiple subqueries, chain the Intersect() method after another SET OPERATION.
        - You should be aware that INTERSECT is evaluated before UNION or EXCEPT. This means
          that, for example, `TABLE x UNION TABLE y INTERSECT TABLE z` is always evaluated as
          `TABLE x UNION (TABLE y INTERSECT TABLE z)`. For more information, please refer to
          [Section 15.2.8, "INTERSECT Clause"](https://dev.mysql.com/doc/refman/8.4/en/intersect.html)

        ## Example
        - Please refer to the <'SelectDML'> statment `Intersect()` method.
        """
        clause: CLAUSE = self._gen_intersect_clause(subquery, all)
        if self._set_op_clauses is None:
            self._set_op_clauses = [clause]
        else:
            list_append(self._set_op_clauses, clause)
        return self

    def Except(
        self,
        subquery: object,
        all: cython.bint = False,
    ) -> InsertDML:
        """The EXCEPT clause of the INSERT statement `<'InsertDML'>`.

        `EXCEPT` (SET OPERATION) limits the result from the first query
        block to those rows which are (also) not found in the second.

        :param subquery `<'SelectDML'>`: The subquery to except with the main query block.
        :param all `<'bool'>`: Keep all the result if `True`, else sort & deduplicate the result set. Defaults to `False`.
        :returns `<'InsertDML'>`: The DML Insert statement.

        ## Notice
        - For multiple subqueries, chain the Except() method after another SET OPERATION.

        ## Example
        - Please refer to the <'SelectDML'> statment `Except()` method.
        """
        clause: CLAUSE = self._gen_except_clause(subquery, all)
        if self._set_op_clauses is None:
            self._set_op_clauses = [clause]
        else:
            list_append(self._set_op_clauses, clause)
        return self

    # . order by
    def OrderBy(self, *columns: object, with_rollup: cython.bint = False) -> InsertDML:
        """The ORDER BY clause of the INSERT statement `<'InsertDML'>`.

        :param columns `<'*str/Column'>`: The ordering (expression of) column(s).
            Each element can be a column name or any SQL expression,
            optionally suffixed with 'ASC' or 'DESC'.
        :param with_rollup `<'bool'>`: Whether to append the `WITH ROLLUP` modifier. Defaults to `False`.
            When 'with_rollup=True', any super-aggregate rows produced (e.g. by a preceding
            `GROUP BY … WITH ROLLUP`) are included in this sort; those summary rows participate
            in the same `ORDER BY`, appearing last under `ASC` or first under `DESC`.

            Only supported by MySQL 8.0.12+.

        :returns `<'InsertDML'>`: The DML Insert statement.

        ## Example
        - Please refer to the <'SelectDML'> statment `OrderBy()` method.
        """
        if self._order_by_clause is not None:
            self._raise_clause_already_set_error(self._order_by_clause)
        self._order_by_clause = self._gen_order_by_clause(columns, with_rollup)
        return self

    # . limit
    def Limit(self, row_count: object, offset: object = None) -> InsertDML:
        """The LIMIT clause of the INSERT statement `<'InsertDML'>`.

        :param row_count `<'int'>`: The number of limited rows to return.
        :param offset `<'int/None'>`: The offset to the first row. Defaults to `None`.
            When setting offset to a positive integer, the LIMIT clause
            will skip the specified number of offset rows before returning
            the remaining desired rows (row_count).
        :returns `<'InsertDML'>`: The DML Insert statement.

        ## Example
        - Please refer to the <'SelectDML'> statment `Limit()` method.
        """
        if self._limit_clause is not None:
            self._raise_clause_already_set_error(self._limit_clause)
        self._limit_clause = self._gen_limit_clause(row_count, offset)
        return self

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # . on duplicate
    def OnDuplicate(self, *assignments: object) -> InsertDML:
        """The ON DUPLICATE KEY UPDATE clause of the INSERT statement `<'InsertDML'>`.

        :param assignments `<'*str'>`: The assignment(s) on how to update the duplicated rows.
        :returns `<'InsertDML'>`: The DML Insert statement.

        ## Example (VALUES)
        >>> (
                db.Insert(db.tb)
                .Columns("id", "name")
                .Values(2)
                .OnDuplicate("name=VALUES(name)")
            )
            # Equivalent to:
            INSERT INTO db.tb (id, name)
            VALUES (%s,%s)
            ON DUPLICATE KEY UPDATE name=VALUES(name)

        ## Example (VALUES - ROW ALIAS)
        >>> (
                db.Insert(db.tb)
                .Columns("id", "name")
                .Values(2)
                .RowAlias("new")
                .OnDuplicate("name=new.name")
            )
            # Equivalent to:
            INSERT INTO db.tb (id, name)
            VALUES (%s,%s) AS new
            ON DUPLICATE KEY UPDATE name=new.name

        ## Example (SET)
        >>> (
                db.Insert(db.tb)
                .Set("id=%s", "name=%s")
                .RowAlias("new", "i", "n")
                .OnDuplicate("name=n")
            )
            # Equivalent to:
            INSERT INTO db.tb
            SET id=%s, name=%s
            AS new (i, n)
            ON DUPLICATE KEY UPDATE name=n

        ## Example (SELECT)
        >>> (
                db.Insert(db.tb1)
                .Columns("id", "name")
                .Select("id", "name")
                .From(db.tb2)
                .OnDuplicate("name=t0.name")
            )
            # Equivalent to:
            INSERT INTO db.tb1 (id, name)
            SELECT id, name FROM db.tb2 AS t0
            ON DUPLICATE KEY UPDATE name=t0.name
        """
        if self._on_dup_key_update_clause is not None:
            self._raise_clause_already_set_error(self._on_dup_key_update_clause)
        self._on_dup_key_update_clause = self._gen_on_duplicate_clause(assignments)
        return self

    # Statement ----------------------------------------------------------------------------
    @cython.ccall
    def statement(self, indent: cython.int = 0) -> str:
        """Compose the INSERT statement `<'str'>`.

        :param indent `<'int'>`: The indentation level of the statement. Defaults to 0.

        ## Example
        >>> db.Insert(db.tb).Columns("id", "name").Values(2).statement()
        >>> "INSERT INTO db.tb (id, name) VALUES (%s,%s)"
        """
        pad: str = self._validate_indent(indent)
        # . insert
        if self._insert_clause is None:
            self._raise_clause_error("INSERT clause is not set.")
        clauses: list = [self._insert_clause.clause(pad)]

        # . columns
        if self._columns_clause is not None:
            clauses.append(self._columns_clause.clause(pad))

        # . values
        if self._values_clause is not None:
            clauses.append(self._values_clause.clause(pad))
            self._insert_mode = utils.INSERT_MODE.VALUES_MODE

        # . set
        elif self._set_clause is not None:
            clauses.append(self._set_clause.clause(pad))
            self._insert_mode = utils.INSERT_MODE.SET_MODE

        # . select
        elif self._select_clause is not None:
            clauses.append(self._gen_select_statement(pad))
            self._insert_mode = utils.INSERT_MODE.SELECT_MODE

        # . on duplicate key update
        if self._on_dup_key_update_clause is not None:
            clauses.append(self._on_dup_key_update_clause.clause(pad))

        # Compose
        stmt: str = "\n".join(clauses)
        if self._insert_mode == utils.INSERT_MODE.INCOMPLETE:
            self._raise_clause_error("is incomplete:\n" + stmt)
        return stmt

    # Execute ------------------------------------------------------------------------------
    @cython.ccall
    def Execute(
        self,
        args: object = None,
        many: cython.bint = False,
        conn: object | None = None,
    ) -> object:
        """[sync] Execute the INSERT statement, and returns the affected rows `<'int'>`

        :param args `<'list/tuple/DataFrame/Any'>`: The arguments (data) for the placeholders in the statement. Defaults to `None`.

        :param many `<'bool'>`: Whether the 'args' (data) is multi-rows. Determines how the data is escaped. Defaults to `False`.
            For a single-row data, set 'many=False'. For a multi-row data, set 'many=True'.

        :param conn `<'PoolSyncConnection/None'>`: The specific [sync] connection to execute the statement. Defaults to `None`.
            If 'conn=None', the statement will be executed by a (random) connection
            from the pool, and commit automatically after the statement execution.

        :returns `<'int'>`: Number of affected rows (sum of rows inserted and updated).

        ## Example (single-row)
        >>> (
                db.Insert(db.tb)
                .Columns("first_name", "last_name")
                .Values(2)
                .Execute(["John", "Ivy"], many=False)
            )
            # Equivalent to:
            INSERT INTO db.tb (first_name, last_name)
            VALUES ('John','Ivy')

        ## Example (multi-rows)
        >>> (
                db.Insert(db.tb)
                .Columns("first_name", "last_name")
                .Values(2)
                .Execute([("John", "Ivy"), ("Sarah", "Kaye")], many=True)
            )
            # Equivalent to:
            INSERT INTO db.tb (first_name, last_name)
            VALUES ('John','Ivy'),('Sarah','Kaye')

        ## Example (connection)
        >>> with db.transaction() as conn:
                db.Select("*").From(db.tb).ForUpdate().Execute(conn=conn)
                db.Insert(db.tb).Values(4).Execute(data, many=True, conn=conn)
            # Equivalent to:
            BEGIN;
            SELECT * FROM db.tb AS t0 FOR UPDATE;
            INSERT INTO db.tb VALUES (...);
            COMMIT;
        """
        return self._Execute(args, None, False, many, conn)

    async def aioExecute(
        self,
        args: object = None,
        many: cython.bint = False,
        conn: object | None = None,
    ) -> object:
        """[async] Execute the INSERT statement, and returns the affected rows `<'int'>`

        :param args `<'list/tuple/DataFrame/Any'>`: The arguments (data) for the placeholders in the statement. Defaults to `None`.

        :param many `<'bool'>`: Whether the 'args' (data) is multi-rows. Determines how the data is escaped. Defaults to `False`.
            For a single-row data, set 'many=False'. For a multi-row data, set 'many=True'.

        :param conn `<'PoolConnection/None'>`: The specific [async] connection to execute the statement. Defaults to `None`.
            If 'conn=None', the statement will be executed by a (random) connection
            from the pool, and commit automatically after the statement execution.

        :returns `<'int'>`: Number of affected rows (sum of rows inserted and updated).

        ## Example (single-row)
        >>> (
                await db.Insert(db.tb)
                .Columns("first_name", "last_name")
                .Values(2)
                .aioExecute(["John", "Ivy"], many=False)
            )
            # Equivalent to:
            INSERT INTO db.tb (first_name, last_name)
            VALUES ('John','Ivy')

        ## Example (multi-rows)
        >>> (
                await db.Insert(db.tb)
                .Columns("first_name", "last_name")
                .Values(2)
                .aioExecute([("John", "Ivy"), ("Sarah", "Kaye")], many=True)
            )
            # Equivalent to:
            INSERT INTO db.tb (first_name, last_name)
            VALUES ('John','Ivy'),('Sarah','Kaye')

        ## Example (connection)
        >>> async with db.transaction() as conn:
                await db.Select("*").From(db.tb).ForUpdate().Execute(conn=conn)
                await db.Insert(db.tb).Values(4).Execute(data, many=True, conn=conn)
            # Equivalent to:
            BEGIN;
            SELECT * FROM db.tb AS t0 FOR UPDATE;
            INSERT INTO db.tb VALUES (...);
            COMMIT;
        """
        return await self._aioExecute(args, None, False, many, conn)

    # Validate -----------------------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(-1, check=False)
    def _validate_join_clause_order(self) -> cython.bint:
        """(internal) Validate if the order of the JOIN cluase is correct."""
        if self._clause_id not in (
            utils.DML_CLAUSE.SELECT,
            utils.DML_CLAUSE.FROM,
            utils.DML_CLAUSE.JOIN,
        ):
            self._raise_clause_order_error("JOIN", ["SELECT", "FROM", "another JOIN"])
        return True


# Replace - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
@cython.cclass
class ReplaceDML(DML):
    """Represents the DML Replace statement.

    `REPLACE` is a MySQL extension to the SQL standard and works exactly like
    INSERT, except that if an old row in the table has the same value as a
    new row for a PRIMARY KEY or a UNIQUE index, the old row is deleted
    before the new row is inserted.
    """

    # clauses
    _replace_clause: INSERT
    _columns_clause: INSERT_COLUMNS
    _values_clause: INSERT_VALUES
    _set_clause: SET
    # internal
    _insert_mode: cython.int

    def __init__(self, db_name: str, pool: Pool):
        """The DML REPLACE statement.

        `REPLACE` is a MySQL extension to the SQL standard and works exactly like
        INSERT, except that if an old row in the table has the same value as a
        new row for a PRIMARY KEY or a UNIQUE index, the old row is deleted
        before the new row is inserted.

        :param db_name `<'str'>`: The name of the database.
        :param pool `<'Pool'>`: The connection pool to handle the statement execution.
        """
        super().__init__("REPLACE", db_name, pool)
        # clauses
        self._replace_clause = None
        self._columns_clause = None
        self._values_clause = None
        self._set_clause = None
        # internal
        self._insert_mode = utils.INSERT_MODE.INCOMPLETE

    # Clause -------------------------------------------------------------------------------
    # . replace
    @cython.ccall
    def Replace(
        self,
        table: object,
        partition: object = None,
        low_priority: cython.bint = False,
    ) -> ReplaceDML:
        """The REPLACE clause of the REPLACE statement `<'ReplaceDML'>`.

        REPLACE is a MySQL extension to the SQL standard and works exactly like
        INSERT, except that if an old row in the table has the same value as a
        new row for a PRIMARY KEY or a UNIQUE index, the old row is deleted
        before the new row is inserted.

        :param table `<'str/Table'>`: The table to replace the data.

        :param partition `<'str/Partition/list/tuple/None'>`: The partition(s) of the table. Defaults to `None`.

        :param low_priority `<'bool'>`: Whether to enable the optional `LOW_PRIORITY` modifier. Defaults to `False`.

            `LOW_PRIORITY`: Delays the REPLACE until no other clients are reading the table
            (even those who start reading while your REPLACE is waiting). Disables concurrent
            inserts—so it can block for a very long time and is normally not recommended on
            MyISAM tables. Only applies to table-locking engines (MyISAM, MEMORY, MERGE).

        :returns `<'ReplaceDML'>`: The DML Replace statement.

        ## Example
        - Please refer to the <'InsertDML'> statment `Insert()` method.
        """
        if self._replace_clause is not None:
            self._raise_clause_already_set_error(self._replace_clause)
        if self._clause_id != utils.DML_CLAUSE.NONE:
            self._raise_clause_error("must start with the REPLACE clause.")
        self._replace_clause = self._gen_insert_clause(
            table, partition, False, "LOW" if low_priority else None
        )
        self._replace_clause._set_replace_mode(True)
        return self

    # . columns
    def Columns(self, *columns: object) -> ReplaceDML:
        """The COLUMNS clause of the REPLACE statement `<'ReplaceDML'>`.

        :param columns `<'*str/Column'>`: The column(s) to replace the data.
        :returns `<'ReplaceDML'>`: The DML Replace statement.

        ## Notice
        - The COLUMNS clause must be placed after the REPLACE clause.

        ## Example
        - Please refer to the <'InsertDML'> statment `Columns()` method.
        """
        if self._clause_id != utils.DML_CLAUSE.INSERT:
            self._raise_clause_order_error("COLUMNS", ["REPLACE"])
        if self._columns_clause is not None:
            self._raise_clause_already_set_error(self._columns_clause)
        self._columns_clause = self._gen_insert_columns_clause(columns)
        return self

    # . values
    def Values(self, placeholders: cython.int) -> ReplaceDML:
        """The VALUES clause of the REPLACE statement `<'ReplaceDML'>`.

        :param placeholders `<'int'>`: The number of placeholders for each row of the replace data.
            If the `Columns()` method is used by the INSERT statement, the number of placeholders
            must match the specified columns. Otherwise, the number of placeholders must match
            the number of columns in the target insert table.
        :returns `<'ReplaceDML'>`: The DML Replace statement.

        ## Notice
        - The VALUES clause must be placed after the REPLACE or COLUMNS clause.

        ## Example
        - Please refer to the <'InsertDML'> statment `Values()` method.
        """
        if self._clause_id not in (
            utils.DML_CLAUSE.INSERT,
            utils.DML_CLAUSE.INSERT_COLUMNS,
        ):
            self._raise_clause_order_error("VALUES", ["REPLACE", "COLUMNS"])
        if self._values_clause is not None:
            self._raise_clause_already_set_error(self._values_clause)
        self._values_clause = self._gen_insert_values_clause(placeholders)
        return self

    # . set
    def Set(self, *assignments: object) -> ReplaceDML:
        """The SET clause of the REPLACE statement `<'ReplaceDML'>`.

        :param assignments `<'*str'>`: The assignment(s) on how to replace the data.
        :returns `<'ReplaceDML'>`: The DML Replace statement.

        ## Notice
        - The SET clause must be placed after the REPLACE or COLUMNS clause.

        ## Example
        - Please refer to the <'InsertDML'> statment `Set()` method.
        """
        if self._clause_id != utils.DML_CLAUSE.INSERT:
            self._raise_clause_order_error("SET", ["REPLACE"])
        if self._set_clause is not None:
            self._raise_clause_already_set_error(self._set_clause)
        self._set_clause = self._gen_set_clause(assignments)
        return self

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # . with
    def With(
        self,
        name: object,
        subquery: object,
        *columns: object,
        recursive: cython.bint = False,
    ) -> ReplaceDML:
        """The WITH (Common Table Expressions) of the REPLACE statement `<'ReplaceDML'>`.

        :param name `<'str'>`: The name of the CTE, which can be used as a table reference in the statement.
        :param subquery `<'str/SelectDML'>`: The subquery that produces the CTE result set.
        :param columns `<'*str/Column'>`: The column names of the result set.
            If specified, the number of columns must be the same as the result set of the subquery.
        :param recursive `<'bool'>`: Whether the CTE is recursive. Defaults to `False`.
            A CTE is recursive if its subquery refers to its own name.
            For more information, refer to MySQL documentation
            ["Recursive Common Table Expressions"](https://dev.mysql.com/doc/refman/8.4/en/with.html#common-table-expressions-recursive).
        :returns `<'ReplaceDML'>`: The DML Replace statement.

        ## Notice
        - The WITH clause must be placed after the REPLACE or COLUMNS clause.
        - For multiple CTEs, chain the With() method after another With().

        ## Example
        >>> (
                db.Replace(db.tb1)
                .Columns("id", "name")
                .With("cte1", db.Select("id", "name").From(db.tb2))
                .With("cte2", db.Select("id", "name").From(db.tb3))
                .Select("*")
                .From("cte1")
                .Union(db.Select("*").From("cte2"))
            )
            # Equivalent to:
            REPLACE INTO db.tb1 (id, name)
            WITH cte1 AS (
                SELECT id, name FROM db.tb2 AS t0
            ), cte2 AS (
                SELECT id, name FROM db.tb3 AS t0
            )
            SELECT * FROM cte1 AS t0
            UNION DISTINCT (
                SELECT * FROM cte2 AS t0
            )
        """
        if self._clause_id not in (
            utils.DML_CLAUSE.INSERT,
            utils.DML_CLAUSE.INSERT_COLUMNS,
            utils.DML_CLAUSE.WITH,
        ):
            self._raise_clause_order_error(
                "WITH", ["REPLACE", "COLUMNS", "another WITH"]
            )
        clause: WITH = self._gen_with_clause(name, subquery, columns, recursive)
        if self._with_clauses is None:
            self._with_clauses = [clause]
        else:
            clause._primary = False
            list_append(self._with_clauses, clause)
        return self

    # . select
    def Select(
        self,
        *expressions: object,
        distinct: cython.bint = False,
        high_priority: cython.bint = False,
        straight_join: cython.bint = False,
        sql_buffer_result: cython.bint = False,
    ) -> ReplaceDML:
        """The SELECT clause of the REPLACE statement `<'ReplaceDML'>`.

        :param expressions `<'*str/SQLFunction/Column'>`: The (expression of) the column(s) to retrieve.

        :param distinct `<'bool'>`: If `True`, specifies removal of duplicate rows from
            the result set. Defaults to `False`.

        :param high_priority `<'bool'>`: If `True`, gives the SELECT higher priority than
            a statement that updates a table. Defaults to `False`. Should only be used for
            queries that are very fast and must be done at once. A SELECT HIGH_PRIORITY
            query that is issued while the table is locked for reading runs even if there
            is an update statement waiting for the table to be free. This affects only storage
            engines that use only table-level locking (such as MyISAM, MEMORY, and MERGE).

        :param straight_join `<'bool'>`: If `True`, forces the optimizer to join the tables
            in the order in which they are listed in the FROM clause. Defaults to `False`.
            Can be used to speed up a query if the optimizer joins the tables in nonoptimal
            order.

        :param sql_buffer_result `<'bool'>`: If `True`, forces the result to be put into a
            temporary table. Defaults to `False`. This helps MySQL free the table locks early
            and helps in cases where it takes a long time to send the result set to the client.
            This modifier can be used only for top-level SELECT statements, not for subqueries
            or following UNION.

        :returns `<'ReplaceDML'>`: The DML Replace statement.

        ## Notice
        - The SELECT clause must be placed after the REPLACE, COLUMNS or WITH clause.

        ## Example
        >>> db.Replace(db.tb1).Select("*").From(db.tb2)
            # Equivalent to:
            REPLACE INTO db.tb1
            SELECT * FROM db.tb2 AS t0
        """
        if self._clause_id not in (
            utils.DML_CLAUSE.INSERT,
            utils.DML_CLAUSE.INSERT_COLUMNS,
            utils.DML_CLAUSE.WITH,
        ):
            self._raise_clause_order_error("SELECT", ["REPLACE", "COLUMNS", "WITH"])
        if self._select_clause is not None:
            self._raise_clause_already_set_error(self._select_clause)
        self._select_clause = self._gen_select_clause(
            expressions, distinct, high_priority, straight_join, sql_buffer_result
        )
        return self

    # . from
    def From(
        self,
        table: object,
        partition: object = None,
        alias: object = None,
    ) -> ReplaceDML:
        """The FROM clause of the REPLACE statement `<'ReplaceDML'>`.

        :param table `<'str/Table/SelectDML'>`: The table (reference) from which to retrieve data.
            Only accepts one table reference. For multiple-table, please use the explicit JOIN clause instead.
        :param partition `<'str/Partition/list/tuple/None'>`: The partition(s) of the table. Defaults to `None`.
        :param alias `<'str/None'>`: The alias of the table (reference). Defaults to `None`.
            When 'alias=None', a default alias is automatically generated based on the count
            of the tables adopting default alias in the same query block (starting from 't0').
        :returns `<'ReplaceDML'>`: The DML Replace statement.

        ## Notice
        - The FROM clause must be placed after the SELECT clause.
        - To set index hints, chain the `UseIndex()`, `IgnoreIndex()` and
          `ForceIndex()` methods after From().

        ## Example
        - Please refer to the <'SelectDML'> statment `From()` method.
        """
        if self._clause_id != utils.DML_CLAUSE.SELECT:
            self._raise_clause_order_error("FROM", ["SELECT"])
        if self._from_clause is not None:
            self._raise_clause_already_set_error(self._from_clause)
        self._from_clause = self._gen_from_clause(table, partition, alias)
        return self

    # . join
    def Join(
        self,
        table: object,
        *on: str,
        using: object = None,
        partition: object = None,
        alias: object = None,
    ) -> ReplaceDML:
        """The (INNER) JOIN clause of the REPLACE statement `<'ReplaceDML'>`.

        :param table `<'str/Table/SelectDML'>`: The table (reference) to join.
        :param on `<'*str'>`: The ON condition(s) specify how to join the table.
        :param using `<'str/Column/list/tuple/None'>`: The USING condition(s) of the common column(s) between the tables. Defaults to `None`.
            When on is specified, using is ignored.
        :param partition `<'str/Partition/list/tuple/None'>`: The partition(s) of the join table. Defaults to `None`.
        :param alias `<'str/None'>`: The alias of the join table. Defaults to `None`.
            When 'alias=None', a default alias is automatically generated based on the count
            of the tables adopting default alias in the same query block (starting from 't0').
        :returns `<'ReplaceDML'>`: The DML Replace statement.

        ## Notice
        - The JOIN clause must be placed after the SELECT or FROM clause.
        - For multiple joins, chain the `Join()` method after another `Join()`.
        - To set index hints, chain the `UseIndex()`, `IgnoreIndex()` and
          `ForceIndex()` methods after `Join()`.

        ## Example
        - Please refer to the <'SelectDML'> statment `Join()` method.
        """
        self._validate_join_clause_order()
        clause: JOIN = self._gen_inner_join_clause(table, on, using, partition, alias)
        if self._join_clauses is None:
            self._join_clauses = [clause]
        else:
            list_append(self._join_clauses, clause)
        return self

    def LeftJoin(
        self,
        table: object,
        *on: object,
        using: object = None,
        partition: object = None,
        alias: object = None,
    ) -> ReplaceDML:
        """The LEFT JOIN clause of the REPLACE statement `<'ReplaceDML'>`.

        :param table `<'str/Table/SelectDML'>`: The table (reference) to join.
        :param on `<'*str'>`: The ON condition(s) specify how to join the table.
        :param using `<'str/Column/list/tuple/None'>`: The USING condition(s) of the common column(s) between the tables. Defaults to `None`.
            When on is specified, using is ignored.
        :param partition `<'str/Partition/list/tuple/None'>`: The partition(s) of the join table. Defaults to `None`.
        :param alias `<'str/None'>`: The alias of the join table. Defaults to `None`.
            When 'alias=None', a default alias is automatically generated based on the count
            of the tables adopting default alias in the same query block (starting from 't0').
        :returns `<'ReplaceDML'>`: The DML Replace statement.

        ## Notice
        - The JOIN clause must be placed after the SELECT or FROM clause.
        - For multiple joins, chain the `Join()` method after another `Join()`.
        - To set index hints, chain the `UseIndex()`, `IgnoreIndex()` and
          `ForceIndex()` methods after `Join()`.

        ## Example
        - Please refer to the <'SelectDML'> statment `LeftJoin()` method.
        """
        self._validate_join_clause_order()
        clause: JOIN = self._gen_left_join_clause(table, on, using, partition, alias)
        if self._join_clauses is None:
            self._join_clauses = [clause]
        else:
            list_append(self._join_clauses, clause)
        return self

    def RightJoin(
        self,
        table: object,
        *on: object,
        using: object = None,
        partition: object = None,
        alias: object = None,
    ) -> ReplaceDML:
        """The RIGHT JOIN clause of the REPLACE statement `<'ReplaceDML'>`.

        :param table `<'str/Table/SelectDML'>`: The table (reference) to join.
        :param on `<'*str'>`: The ON condition(s) specify how to join the table.
        :param using `<'str/Column/list/tuple/None'>`: The USING condition(s) of the common column(s) between the tables. Defaults to `None`.
            When on is specified, using is ignored.
        :param partition `<'str/Partition/list/tuple/None'>`: The partition(s) of the join table. Defaults to `None`.
        :param alias `<'str/None'>`: The alias of the join table. Defaults to `None`.
            When 'alias=None', a default alias is automatically generated based on the count
            of the tables adopting default alias in the same query block (starting from 't0').
        :returns `<'ReplaceDML'>`: The DML Replace statement.

        ## Notice
        - The JOIN clause must be placed after the SELECT or FROM clause.
        - For multiple joins, chain the `Join()` method after another `Join()`.
        - To set index hints, chain the `UseIndex()`, `IgnoreIndex()` and
          `ForceIndex()` methods after `Join()`.

        ## Example
        - Please refer to the <'SelectDML'> statment `RightJoin()` method.
        """
        self._validate_join_clause_order()
        clause: JOIN = self._gen_right_join_clause(table, on, using, partition, alias)
        if self._join_clauses is None:
            self._join_clauses = [clause]
        else:
            list_append(self._join_clauses, clause)
        return self

    def StraightJoin(
        self,
        table: object,
        *on: object,
        using: object = None,
        partition: object = None,
        alias: object = None,
    ) -> ReplaceDML:
        """The STRAIGHT_JOIN clause of the REPLACE statement `<'ReplaceDML'>`.

        :param table `<'str/Table/SelectDML'>`: The table (reference) to join.
        :param on `<'*str'>`: The ON condition(s) specify how to join the table.
        :param using `<'str/Column/list/tuple/None'>`: The USING condition(s) of the common column(s) between the tables. Defaults to `None`.
            When on is specified, using is ignored.
        :param partition `<'str/Partition/list/tuple/None'>`: The partition(s) of the join table. Defaults to `None`.
        :param alias `<'str/None'>`: The alias of the join table. Defaults to `None`.
            When 'alias=None', a default alias is automatically generated based on the count
            of the tables adopting default alias in the same query block (starting from 't0').
        :returns `<'ReplaceDML'>`: The DML Replace statement.

        ## Notice
        - The JOIN clause must be placed after the SELECT or FROM clause.
        - For multiple joins, chain the `Join()` method after another `Join()`.
        - To set index hints, chain the `UseIndex()`, `IgnoreIndex()` and
          `ForceIndex()` methods after `Join()`.

        ## Example
        - Please refer to the <'SelectDML'> statment `StraightJoin()` method.
        """
        self._validate_join_clause_order()
        clause: JOIN = self._gen_straight_join_clause(
            table, on, using, partition, alias
        )
        if self._join_clauses is None:
            self._join_clauses = [clause]
        else:
            list_append(self._join_clauses, clause)
        return self

    def CrossJoin(
        self,
        table: object,
        *on: object,
        using: object = None,
        partition: object = None,
        alias: object = None,
    ) -> ReplaceDML:
        """The CROSS JOIN clause of the REPLACE statement `<'ReplaceDML'>`.

        :param table `<'str/Table/SelectDML'>`: The table (reference) to join.
        :param on `<'*str'>`: The ON condition(s) specify how to join the table.
        :param using `<'str/Column/list/tuple/None'>`: The USING condition(s) of the common column(s) between the tables. Defaults to `None`.
            When on is specified, using is ignored.
        :param partition `<'str/Partition/list/tuple/None'>`: The partition(s) of the join table. Defaults to `None`.
        :param alias `<'str/None'>`: The alias of the join table. Defaults to `None`.
            When 'alias=None', a default alias is automatically generated based on the count
            of the tables adopting default alias in the same query block (starting from 't0').
        :returns `<'ReplaceDML'>`: The DML Replace statement.

        ## Notice
        - The JOIN clause must be placed after the SELECT or FROM clause.
        - For multiple joins, chain the `Join()` method after another `Join()`.
        - To set index hints, chain the `UseIndex()`, `IgnoreIndex()` and
          `ForceIndex()` methods after `Join()`.

        ## Example
        - Please refer to the <'SelectDML'> statment `CrossJoin()` method.
        """
        self._validate_join_clause_order()
        clause: JOIN = self._gen_cross_join_clause(table, on, using, partition, alias)
        if self._join_clauses is None:
            self._join_clauses = [clause]
        else:
            list_append(self._join_clauses, clause)
        return self

    def NaturalJoin(
        self,
        table: object,
        join_method: object = "INNER",
        partition: object = None,
        alias: object = None,
    ) -> ReplaceDML:
        """The NATURAL JOIN clause of the REPLACE statement `<'ReplaceDML'>`.

        :param table `<'str/Table/SelectDML'>`: The table (reference) to join.
        :param join_method `<'str'>`: The join method. Defaults to `"INNER"`.
            Accepts: `"INNER"`, `"LEFT"`, `"RIGHT"`.
        :param partition `<'str/Partition/list/tuple/None'>`: The partition(s) of the join table. Defaults to `None`.
        :param alias `<'str/None'>`: The alias of the join table. Defaults to `None`.
            When 'alias=None', a default alias is automatically generated based on the count
            of the tables adopting default alias in the same query block (starting from 't0').
        :returns `<'ReplaceDML'>`: The DML Replace statement.

        ## Notice
        - The JOIN clause must be placed after the SELECT or FROM clause.
        - For multiple joins, chain the `Join()` method after another `Join()`.
        - To set index hints, chain the `UseIndex()`, `IgnoreIndex()` and
          `ForceIndex()` methods after `Join()`.

        ## Example
        - Please refer to the <'SelectDML'> statment `NaturalJoin()` method.
        """
        self._validate_join_clause_order()
        clause: JOIN = self._gen_natural_join_clause(
            table, join_method, partition, alias
        )
        if self._join_clauses is None:
            self._join_clauses = [clause]
        else:
            list_append(self._join_clauses, clause)
        return self

    # . index hints
    def UseIndex(self, *indexes: object, scope: object = None) -> ReplaceDML:
        """The USE INDEX clause of the REPLACE statement `<'ReplaceDML'>`.

        :param indexes `<'*str/Index'>`: The index(es) to use by the INDEX HINTS.
        :param scope `<'str/None'>`: The scope of the INDEX HINTS. Defaults to `None`.
            Accepts: `"JOIN"`, `"ORDER BY"` and `"GROUP BY"`.
            This provides more fine-grained control over optimizer selection of an execution plan
            for various phases of query processing. To affect only the indexes used when MySQL
            decides how to find rows in the table and how to process joins, use FOR JOIN. To
            influence index usage for sorting or grouping rows, use FOR ORDER BY or FOR GROUP BY.
        :returns `<'ReplaceDML'>`: The DML Replace statement.

        ## Notice
        - The USE INDEX clause must be placed after the FROM or JOIN
          clause or chain after another INDEX HINTS clause.

        ## Example
        - Please refer to the <'SelectDML'> statment `UseIndex()` method.
        """
        # fmt: off
        if self._clause_id == utils.DML_CLAUSE.FROM:
            clause: CLAUSE = self._from_clause
        elif self._clause_id == utils.DML_CLAUSE.JOIN and self._join_clauses is not None:
            clause: CLAUSE = self._join_clauses[list_len(self._join_clauses) - 1]
        else:
            self._raise_clause_order_error(
                "USE INDEX", ["FROM", "JOIN", "another INDEX HINTS"]
            )
        # fmt: on
        clause._add_index_hints(self._gen_use_index_clause(indexes, scope))
        return self

    def IgnoreIndex(self, *indexes: object, scope: object = None) -> ReplaceDML:
        """The IGNORE INDEX clause of the REPLACE statement `<'ReplaceDML'>`.

        :param indexes `<'*str/Index'>`: The index(es) to ignore by the INDEX HINTS.
        :param scope `<'str/None'>`: The scope of the INDEX HINTS. Defaults to `None`.
            Accepts: `"JOIN"`, `"ORDER BY"` and `"GROUP BY"`.
            This provides more fine-grained control over optimizer selection of an execution plan
            for various phases of query processing. To affect only the indexes used when MySQL
            decides how to find rows in the table and how to process joins, use FOR JOIN. To
            influence index usage for sorting or grouping rows, use FOR ORDER BY or FOR GROUP BY.
        :returns `<'ReplaceDML'>`: The DML Replace statement.

        ## Notice
        - The IGNORE INDEX clause must be placed after the FROM or JOIN
          clause or chain after another INDEX HINTS clause.

        ## Example
        - Please refer to the <'SelectDML'> statment `IgnoreIndex()` method.
        """
        # fmt: off
        if self._clause_id == utils.DML_CLAUSE.FROM:
            clause: CLAUSE = self._from_clause
        elif self._clause_id == utils.DML_CLAUSE.JOIN and self._join_clauses is not None:
            clause: CLAUSE = self._join_clauses[list_len(self._join_clauses) - 1]
        else:
            self._raise_clause_order_error(
                "IGNORE INDEX", ["FROM", "JOIN", "another INDEX HINTS"]
            )
        # fmt: on
        clause._add_index_hints(self._gen_ignore_index_clause(indexes, scope))
        return self

    def ForceIndex(self, *indexes: object, scope: object = None) -> ReplaceDML:
        """The FORCE INDEX clause of the REPLACE statement `<'ReplaceDML'>`.

        :param indexes `<'*str/Index'>`: The index(es) to force by the INDEX HINTS.
        :param scope `<'str/None'>`: The scope of the INDEX HINTS. Defaults to `None`.
            Accepts: `"JOIN"`, `"ORDER BY"` and `"GROUP BY"`.
            This provides more fine-grained control over optimizer selection of an execution plan
            for various phases of query processing. To affect only the indexes used when MySQL
            decides how to find rows in the table and how to process joins, use FOR JOIN. To
            influence index usage for sorting or grouping rows, use FOR ORDER BY or FOR GROUP BY.
        :returns `<'ReplaceDML'>`: The DML Replace statement.

        ## Notice
        - The FORCE INDEX clause must be placed after the FROM or JOIN
          clause or chain after another INDEX HINTS clause.

        ## Example
        - Please refer to the <'SelectDML'> statment `ForceIndex()` method.
        """
        # fmt: off
        if self._clause_id == utils.DML_CLAUSE.FROM:
            clause: CLAUSE = self._from_clause
        elif self._clause_id == utils.DML_CLAUSE.JOIN and self._join_clauses is not None:
            clause: CLAUSE = self._join_clauses[list_len(self._join_clauses) - 1]
        else:
            self._raise_clause_order_error(
                "FORCE INDEX", ["FROM", "JOIN", "another INDEX HINTS"]
            )
        # fmt: on
        clause._add_index_hints(self._gen_force_index_clause(indexes, scope))
        return self

    # . where
    def Where(
        self,
        *conds: str,
        in_conds: dict | None = None,
        not_in_conds: dict | None = None,
    ) -> ReplaceDML:
        """The WHERE clause of the REPLACE statement `<'ReplaceDML'>`.

        :param conds `<'*str'>`: The condition(s) that rows must satisfy to be selected.
        :param in_conds `<'dict/None'>`: The IN condition(s). Defaults to `None`.
        :param not_in_conds `<'dict/None'>`: The NOT IN condition(s). Defaults to `None`.
        :returns `<'ReplaceDML'>`: The DML Replace statement.

        ## Explanation
        - All conditions are combined with `AND` logic by default.
        - Prepend the condition with "`OR `" to alter the default behavior.
        - The 'in_conds' and 'not_in_conds' arguments must be a dictionary,
          where the keys should be the columns and values the desired ones
          to check against. Values can be any escapable objects or a subquery
          `<'SelectDML'>` instance, but `NOT` strings with placeholders.

        ## Example
        - Please refer to the <'SelectDML'> statment `Where()` method.
        """
        if self._where_clause is not None:
            self._raise_clause_already_set_error(self._where_clause)
        self._where_clause = self._gen_where_clause(conds, in_conds, not_in_conds)
        return self

    # . group by
    def GroupBy(
        self,
        *columns: object,
        with_rollup: cython.bint = False,
    ) -> ReplaceDML:
        """The GROUP BY clause of the REPLACE statement `<'ReplaceDML'>`.

        :param columns `<'*str/Column'>`: The (expression of) column(s) to group by with.
        :param with_rollup `<'bool'>`: Whether to summary output to include extra rows
            that represent higher-level (super-aggregate) grand total.
        :returns `<'ReplaceDML'>`: The DML Replace statement.

        ## Example
        - Please refer to the <'SelectDML'> statment `GroupBy()` method.
        """
        if self._group_by_clause is not None:
            self._raise_clause_already_set_error(self._group_by_clause)
        self._group_by_clause = self._gen_group_by_clause(columns, with_rollup)
        return self

    # . having
    def Having(
        self,
        *conds: str,
        in_conds: dict | None = None,
        not_in_conds: dict | None = None,
    ) -> ReplaceDML:
        """The HAVING clause of the REPLACE statement `<'ReplaceDML'>`.

        :param conds `<'*str'>`: The condition(s) that rows must satisfy to be selected.
        :param in_conds `<'dict/None'>`: The IN condition(s). Defaults to `None`.
        :param not_in_conds `<'dict/None'>`: The NOT IN condition(s). Defaults to `None`.
        :returns `<'ReplaceDML'>`: The DML Replace statement.

        ## Explanation
        - All conditions are combined with `AND` logic by default.
        - Prepend the condition with "`OR `" to alter the default behavior.
        - The 'in_conds' and 'not_in_conds' arguments must be a dictionary,
          where the keys should be the columns and values the desired ones
          to check against. Values can be any escapable objects or a subquery
          `<'SelectDML'>` instance, but `NOT` strings with placeholders.

        ## Notice
        The HAVING clause, like the WHERE clause, specifies selection conditions.
        The WHERE clause specifies conditions on columns in the select list, but
        cannot refer to aggregate functions. The HAVING clause specifies conditions
        on groups, typically formed by the GROUP BY clause. The query result includes
        only groups satisfying the HAVING conditions.

        ## Example
        - Please refer to the <'SelectDML'> statment `Where()` method for similar usage.
        """
        if self._having_clause is not None:
            self._raise_clause_already_set_error(self._having_clause)
        self._having_clause = self._gen_having_clause(conds, in_conds, not_in_conds)
        return self

    # . window
    def Window(
        self,
        name: object,
        partition_by: object | None = None,
        order_by: object | None = None,
        frame_clause: str | None = None,
    ) -> ReplaceDML:
        """The WINDOW clause of the REPLACE statement `<'ReplaceDML'>`.

        :param name `<'str'>`: The name of the window.

        :param partition_by `<'str/Column/list/tuple/None'>`: Specifies how to divide the query rows into groups. Defaults to `None`.
            The window function result for a given row is based on the rows of the partition
            that contains the row. If 'partition_by=None', there is a single partition
            consisting of all query rows.

        :param order_by `<'str/Column/list/tuple/None'>`: Specifies how to sort rows in each partition. Defaults to `None`.
            Partition rows that are equal according to the ORDER BY clause are considered peers.
            If 'order_by=None', partition rows are unordered, with no processing order implied,
            and all partition rows are peers.

        :param frame_clause `<'str/None'>`: Specifies how to define the frame (subset of the current partition). Defaults to `None`.
            Frames are determined with respect to the current row, which enables a frame to move
            within a partition depending on the location of the current row within its partition.

            Examples: By defining a frame to be all rows from the partition start to the current
            row, you can compute running totals for each row. By defining a frame as extending N
            rows on either side of the current row, you can compute rolling averages.

            For more information, please refer to the MySQL documentation
            [Section 14.20.3, "Window Function Frame Specification"](https://dev.mysql.com/doc/refman/8.0/en/window-function-descriptions.html).

        :returns `<'ReplaceDML'>`: The DML Replace statement.

        ## Notice
        - For multiple windows, chain the Window() method after another Window().

        ## Example
        - Please refer to the <'SelectDML'> statment `Window()` method.
        """
        clause: WINDOW = self._gen_window_clause(
            name, partition_by, order_by, frame_clause
        )
        if self._window_clauses is None:
            self._window_clauses = [clause]
        else:
            clause._primary = False
            list_append(self._window_clauses, clause)
        return self

    # . set operations
    def Union(
        self,
        subquery: object,
        all: cython.bint = False,
    ) -> ReplaceDML:
        """The UNION clause of the REPLACE statement `<'ReplaceDML'>`.

        `UNION` (SET OPERATION) combines the result from multiple
        query blocks into a single result set.

        :param subquery `<'SelectDML'>`: The subquery to union with the main query block.
        :param all `<'bool'>`: Keep all the result if `True`, else sort & deduplicate the result set. Defaults to `False`.
        :returns `<'ReplaceDML'>`: The DML Replace statement.

        ## Notice
        - For multiple subqueries, chain the Union() method after another SET OPERATION.

        ## Example
        - Please refer to the <'SelectDML'> statment `Union()` method.
        """
        clause: CLAUSE = self._gen_union_clause(subquery, all)
        if self._set_op_clauses is None:
            self._set_op_clauses = [clause]
        else:
            list_append(self._set_op_clauses, clause)
        return self

    def Intersect(
        self,
        subquery: object,
        all: cython.bint = False,
    ) -> ReplaceDML:
        """The INTERSECT clause of the REPLACE statement `<'ReplaceDML'>`.

        `INTERSECT` (SET OPERATION) limits the result from multiple
        query blocks to those rows which are common to all.

        :param subquery `<'SelectDML'>`: The subquery to intersect with the main query block.
        :param all `<'bool'>`: Keep all the result if `True`, else sort & deduplicate the result set. Defaults to `False`.
        :returns `<'ReplaceDML'>`: The DML Replace statement.

        ## Notice
        - For multiple subqueries, chain the Intersect() method after another SET OPERATION.
        - You should be aware that INTERSECT is evaluated before UNION or EXCEPT. This means
          that, for example, `TABLE x UNION TABLE y INTERSECT TABLE z` is always evaluated as
          `TABLE x UNION (TABLE y INTERSECT TABLE z)`. For more information, please refer to
          [Section 15.2.8, "INTERSECT Clause"](https://dev.mysql.com/doc/refman/8.4/en/intersect.html)

        ## Example
        - Please refer to the <'SelectDML'> statment `Intersect()` method.
        """
        clause: CLAUSE = self._gen_intersect_clause(subquery, all)
        if self._set_op_clauses is None:
            self._set_op_clauses = [clause]
        else:
            list_append(self._set_op_clauses, clause)
        return self

    def Except(
        self,
        subquery: object,
        all: cython.bint = False,
    ) -> ReplaceDML:
        """The EXCEPT clause of the REPLACE statement `<'ReplaceDML'>`.

        `EXCEPT` (SET OPERATION) limits the result from the first query
        block to those rows which are (also) not found in the second.

        :param subquery `<'SelectDML'>`: The subquery to except with the main query block.
        :param all `<'bool'>`: Keep all the result if `True`, else sort & deduplicate the result set. Defaults to `False`.
        :returns `<'ReplaceDML'>`: The DML Replace statement.

        ## Notice
        - For multiple subqueries, chain the Except() method after another SET OPERATION.

        ## Example
        - Please refer to the <'SelectDML'> statment `Except()` method.
        """
        clause: CLAUSE = self._gen_except_clause(subquery, all)
        if self._set_op_clauses is None:
            self._set_op_clauses = [clause]
        else:
            list_append(self._set_op_clauses, clause)
        return self

    # . order by
    def OrderBy(self, *columns: object, with_rollup: cython.bint = False) -> ReplaceDML:
        """The ORDER BY clause of the REPLACE statement `<'ReplaceDML'>`.

        :param columns `<'*str/Column'>`: The ordering (expression of) column(s).
            Each element can be a column name or any SQL expression,
            optionally suffixed with 'ASC' or 'DESC'.
        :param with_rollup `<'bool'>`: Whether to append the `WITH ROLLUP` modifier. Defaults to `False`.
            When 'with_rollup=True', any super-aggregate rows produced (e.g. by a preceding
            `GROUP BY … WITH ROLLUP`) are included in this sort; those summary rows participate
            in the same `ORDER BY`, appearing last under `ASC` or first under `DESC`.

            Only supported by MySQL 8.0.12+.

        :returns `<'ReplaceDML'>`: The DML Replace statement.

        ## Example
        - Please refer to the <'SelectDML'> statment `OrderBy()` method.
        """
        if self._order_by_clause is not None:
            self._raise_clause_already_set_error(self._order_by_clause)
        self._order_by_clause = self._gen_order_by_clause(columns, with_rollup)
        return self

    # . limit
    def Limit(self, row_count: object, offset: object = None) -> ReplaceDML:
        """The LIMIT clause of the REPLACE statement `<'ReplaceDML'>`.

        :param row_count `<'int'>`: The number of limited rows to return.
        :param offset `<'int/None'>`: The offset to the first row. Defaults to `None`.
            When setting offset to a positive integer, the LIMIT clause
            will skip the specified number of offset rows before returning
            the remaining desired rows (row_count).
        :returns `<'ReplaceDML'>`: The DML Replace statement.

        ## Example
        - Please refer to the <'SelectDML'> statment `Limit()` method.
        """
        if self._limit_clause is not None:
            self._raise_clause_already_set_error(self._limit_clause)
        self._limit_clause = self._gen_limit_clause(row_count, offset)
        return self

    # Statement ----------------------------------------------------------------------------
    @cython.ccall
    def statement(self, indent: cython.int = 0) -> str:
        """Compose the REPLACE statement `<'str'>`.

        :param indent `<'int'>`: The indentation level of the statement. Defaults to 0.

        ## Example
        >>> db.Replace(db.tb).Columns("first_name", "last_name").Values(2).statement()
        >>> "REPLACE INTO db.tb (first_name, last_name) VALUES (%s,%s)"
        """
        pad: str = self._validate_indent(indent)
        # . replace
        if self._replace_clause is None:
            self._raise_clause_error("REPLACE clause is not set.")
        clauses: list = [self._replace_clause.clause(pad)]

        # . columns
        if self._columns_clause is not None:
            clauses.append(self._columns_clause.clause(pad))

        # . values
        if self._values_clause is not None:
            clauses.append(self._values_clause.clause(pad))
            self._insert_mode = utils.INSERT_MODE.VALUES_MODE

        # . set
        elif self._set_clause is not None:
            clauses.append(self._set_clause.clause(pad))
            self._insert_mode = utils.INSERT_MODE.SET_MODE

        # . select
        elif self._select_clause is not None:
            clauses.append(self._gen_select_statement(pad))
            self._insert_mode = utils.INSERT_MODE.SELECT_MODE

        # Compose
        stmt = "\n".join(clauses)
        if self._insert_mode == utils.INSERT_MODE.INCOMPLETE:
            self._raise_clause_error("is incomplete:\n" + stmt)
        return stmt

    # Execute ------------------------------------------------------------------------------
    @cython.ccall
    def Execute(
        self,
        args: object = None,
        many: cython.bint = False,
        conn: object | None = None,
    ) -> object:
        """[sync] Execute the REPLACE statement, and returns the affected rows `<'int'>`

        :param args `<'list/tuple/DataFrame/Any'>`: The arguments (data) for the placeholders in the statement. Defaults to `None`.

        :param many `<'bool'>`: Whether the 'args' (data) is multi-rows. Determines how the data is escaped. Defaults to `False`.
            For a single-row data, set 'many=False'. For a multi-row data, set 'many=True'.

        :param conn `<'PoolSyncConnection/None'>`: The specific [sync] connection to execute the statement. Defaults to `None`.
            If 'conn=None', the statement will be executed by a (random) connection
            from the pool, and commit automatically after the statement execution.

        :returns `<'int'>`: Number of affected rows (sum of rows inserted and deleted).

        ## Example (single-row)
        >>> (
                db.Replace(db.tb)
                .Columns("first_name", "last_name")
                .Values(2)
                .Execute(["John", "Ivy"], many=False)
            )
            # Equivalent to:
            REPLACE INTO db.tb (first_name, last_name)
            VALUES ('John','Ivy')

        ## Example (multi-rows)
        >>> (
                db.Replace(db.tb)
                .Columns("first_name", "last_name")
                .Values(2)
                .Execute([("John", "Ivy"), ("Sarah", "Kaye")], many=True)
            )
            # Equivalent to:
            REPLACE INTO db.tb (first_name, last_name)
            VALUES ('John','Ivy'),('Sarah','Kaye')

        ## Example (connection)
        >>> with db.transaction() as conn:
                db.Select("*").From(db.tb).ForUpdate().Execute(conn=conn)
                db.Replace(db.tb).Values(4).Execute(data, many=True, conn=conn)
            # Equivalent to:
            BEGIN;
            SELECT * FROM db.tb AS t0 FOR UPDATE;
            REPLACE INTO db.tb VALUES (...);
            COMMIT;
        """
        return self._Execute(args, None, False, many, conn)

    async def aioExecute(
        self,
        args: object = None,
        many: cython.bint = False,
        conn: object | None = None,
    ) -> object:
        """[async] Execute the REPLACE statement, and returns the affected rows `<'int'>`

        :param args `<'list/tuple/DataFrame/Any'>`: The arguments (data) for the placeholders in the statement. Defaults to `None`.

        :param many `<'bool'>`: Whether the 'args' (data) is multi-rows. Determines how the data is escaped. Defaults to `False`.
            For a single-row data, set 'many=False'. For a multi-row data, set 'many=True'.

        :param conn `<'PoolConnection/None'>`: The specific [async] connection to execute the statement. Defaults to `None`.
            If 'conn=None', the statement will be executed by a (random) connection
            from the pool, and commit automatically after the statement execution.

        :returns `<'int'>`: Number of affected rows (sum of rows inserted and deleted).

        ## Example (single-row)
        >>> (
                await db.Replace(db.tb)
                .Columns("first_name", "last_name")
                .Values(2)
                .aioExecute(["John", "Ivy"], many=False)
            )
            # Equivalent to:
            REPLACE INTO db.tb (first_name, last_name)
            VALUES ('John','Ivy')

        ## Example (multi-rows)
        >>> (
                await db.Replace(db.tb)
                .Columns("first_name", "last_name")
                .Values(2)
                .aioExecute([("John", "Ivy"), ("Sarah", "Kaye")], many=True)
            )
            # Equivalent to:
            REPLACE INTO db.tb (first_name, last_name)
            VALUES ('John','Ivy'),('Sarah','Kaye')

        ## Example (connection)
        >>> async with db.transaction() as conn:
                await db.Select("*").From(db.tb).ForUpdate().Execute(conn=conn)
                await db.Replace(db.tb).Values(4).Execute(data, many=True, conn=conn)
            # Equivalent to:
            BEGIN;
            SELECT * FROM db.tb AS t0 FOR UPDATE;
            REPLACE INTO db.tb VALUES (...);
            COMMIT;
        """
        return await self._aioExecute(args, None, False, many, conn)

    # Validate -----------------------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(-1, check=False)
    def _validate_join_clause_order(self) -> cython.bint:
        """(internal) Validate if the order of the JOIN cluase is correct."""
        if self._clause_id not in (
            utils.DML_CLAUSE.SELECT,
            utils.DML_CLAUSE.FROM,
            utils.DML_CLAUSE.JOIN,
        ):
            self._raise_clause_order_error("JOIN", ["SELECT", "FROM", "another JOIN"])
        return True


# Update - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
@cython.cclass
class UpdateDML(DML):
    """Represents the DML UPDATE statement."""

    # clauses
    _update_clause: UPDATE
    _set_clause: SET

    def __init__(self, db_name: str, pool: Pool):
        """The DML UPDATE statement.

        :param db_name `<'str'>`: The name of the database.
        :param pool `<'Pool'>`: The connection pool to handle the statement execution.
        """
        super().__init__("UPDATE", db_name, pool)
        # clauses
        self._update_clause = None
        self._set_clause = None

    # Clause -------------------------------------------------------------------------------
    # . update
    @cython.ccall
    def Update(
        self,
        table: object,
        partition: object = None,
        ignore: cython.bint = False,
        low_priority: cython.bint = False,
        alias: object = None,
    ) -> UpdateDML:
        """The UPDATE clause of the UPDATE statement `<'UpdateDML'>`.

        :param table `<'str/Table'>`: The table from which to update data.
            Only accepts one table. For multiple-table, please use the explicit JOIN clause instead.

        :param partition `<'str/Partition/list/tuple/None'>`: The partition(s) of the table. Defaults to `None`.

        :param ignore `<'bool'>`: Whether to ignore the (ignorable) errors. Defaults to `False`.
            When 'ignore=True', the update statement does not abort even if errors occur during
            the update. Rows for which duplicate-key conflicts occur on a unique key value are
            not updated. Rows updated to values that would cause data conversion errors are
            updated to the closest valid values instead.

        :param low_priority `<'bool'>`: Whether to enable the optional `LOW_PRIORITY` modifier. Defaults to `False`.
            When 'low_priority=True', execution of the UPDATE is delayed until no other
            clients are reading from the table. This affects only storage engines that
            use only table-level locking (such as MyISAM, MEMORY, and MERGE).

        :param alias `<'str/None'>`: The alias of the table. Defaults to `None`.
            When 'alias=None', a default alias is automatically generated based on the count
            of the tables adopting default alias in the same query block (starting from 't0').

        :returns `<'UpdateDML'>`: The DML Update statement.

        ## Notice
        - To set index hints, chain the `UseIndex()`, `IgnoreIndex()` and
          `ForceIndex()` methods after Update().

        ## Example (single-table)
        >>> (
                db.Update(db.tb)
                .Set("first_name='John'")
                .Where("id=1")
            )
            # Equivalent to:
            UPDATE db.tb AS t0
            SET first_name='John'
            WHERE id=1

        ## Example (multi-table)
        >>> (
                db.Update(db.tb1)
                .Join(db.tb2, using="id")
                .Set("t0.first_name=t1.first_name")
                .Where("t0.id=1")
            )
            # Equivalent to:
            UPDATE db.tb1 AS t0
            JOIN db.tb2 AS t1 USING (id)
            SET t0.first_name=t1.first_name
            WHERE t0.id=1
        """
        if self._update_clause is not None:
            self._raise_clause_already_set_error(self._update_clause)
        if self._clause_id != utils.DML_CLAUSE.NONE:
            self._raise_clause_error("must start with the UPDATE clause.")
        self._update_clause = self._gen_update_clause(
            table, partition, ignore, low_priority, alias
        )
        return self

    # . set
    def Set(self, *assignments: object) -> UpdateDML:
        """The SET clause of the UPDATE statement `<'UpdateDML'>`.

        :param assignments `<'*str'>`: The assignment(s) on how to update the data.
        :returns `<'UpdateDML'>`: The DML Update statement.

        ## Notice
        - The SET clause must be placed after the UPDATE or JOIN clause.

        ## Example
        >>> (
                db.Update(db.tb)
                .Set("first_name='John'", "last_name='Doe'")
                .Where("id=1")
            )
            # Equivalent to:
            UPDATE db.tb AS t0
            SET first_name='John', last_name='Doe'
            WHERE id=1
        """
        if self._clause_id not in (utils.DML_CLAUSE.UPDATE, utils.DML_CLAUSE.JOIN):
            self._raise_clause_order_error("SET", ["UPDATE", "JOIN"])
        if self._set_clause is not None:
            self._raise_clause_already_set_error(self._set_clause)
        self._set_clause = self._gen_set_clause(assignments)
        return self

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # . join
    def Join(
        self,
        table: object,
        *on: str,
        using: object = None,
        partition: object = None,
        alias: object = None,
    ) -> UpdateDML:
        """The (INNER) JOIN clause of the UPDATE statement `<'UpdateDML'>`.

        :param table `<'str/Table/SelectDML'>`: The table (reference) to join.
        :param on `<'*str'>`: The ON condition(s) specify how to join the table.
        :param using `<'str/Column/list/tuple/None'>`: The USING condition(s) of the common column(s) between the tables. Defaults to `None`.
            When on is specified, using is ignored.
        :param partition `<'str/Partition/list/tuple/None'>`: The partition(s) of the join table. Defaults to `None`.
        :param alias `<'str/None'>`: The alias of the join table. Defaults to `None`.
            When 'alias=None', a default alias is automatically generated based on the count
            of the tables adopting default alias in the same query block (starting from 't0').
        :returns `<'UpdateDML'>`: The DML Update statement.

        ## Notice
        - The JOIN clause must be placed after the UPDATE clause.
        - For multiple joins, chain the `Join()` method after another `Join()`.
        - To set index hints, chain the `UseIndex()`, `IgnoreIndex()` and
          `ForceIndex()` methods after `Join()`.

        ## Example
        - Please refer to the <'SelectDML'> statment `Join()` method.
        """
        self._validate_join_clause_order()
        clause: JOIN = self._gen_inner_join_clause(table, on, using, partition, alias)
        if self._join_clauses is None:
            self._join_clauses = [clause]
        else:
            list_append(self._join_clauses, clause)
        return self

    def LeftJoin(
        self,
        table: object,
        *on: object,
        using: object = None,
        partition: object = None,
        alias: object = None,
    ) -> UpdateDML:
        """The LEFT JOIN clause of the UPDATE statement `<'UpdateDML'>`.

        :param table `<'str/Table/SelectDML'>`: The table (reference) to join.
        :param on `<'*str'>`: The ON condition(s) specify how to join the table.
        :param using `<'str/Column/list/tuple/None'>`: The USING condition(s) of the common column(s) between the tables. Defaults to `None`.
            When on is specified, using is ignored.
        :param partition `<'str/Partition/list/tuple/None'>`: The partition(s) of the join table. Defaults to `None`.
        :param alias `<'str/None'>`: The alias of the join table. Defaults to `None`.
            When 'alias=None', a default alias is automatically generated based on the count
            of the tables adopting default alias in the same query block (starting from 't0').
        :returns `<'UpdateDML'>`: The DML Update statement.

        ## Notice
        - The JOIN clause must be placed after the UPDATE clause.
        - For multiple joins, chain the `Join()` method after another `Join()`.
        - To set index hints, chain the `UseIndex()`, `IgnoreIndex()` and
          `ForceIndex()` methods after `Join()`.

        ## Example
        - Please refer to the <'SelectDML'> statment `LeftJoin()` method.
        """
        self._validate_join_clause_order()
        clause: JOIN = self._gen_left_join_clause(table, on, using, partition, alias)
        if self._join_clauses is None:
            self._join_clauses = [clause]
        else:
            list_append(self._join_clauses, clause)
        return self

    def RightJoin(
        self,
        table: object,
        *on: object,
        using: object = None,
        partition: object = None,
        alias: object = None,
    ) -> UpdateDML:
        """The RIGHT JOIN clause of the UPDATE statement `<'UpdateDML'>`.

        :param table `<'str/Table/SelectDML'>`: The table (reference) to join.
        :param on `<'*str'>`: The ON condition(s) specify how to join the table.
        :param using `<'str/Column/list/tuple/None'>`: The USING condition(s) of the common column(s) between the tables. Defaults to `None`.
            When on is specified, using is ignored.
        :param partition `<'str/Partition/list/tuple/None'>`: The partition(s) of the join table. Defaults to `None`.
        :param alias `<'str/None'>`: The alias of the join table. Defaults to `None`.
            When 'alias=None', a default alias is automatically generated based on the count
            of the tables adopting default alias in the same query block (starting from 't0').
        :returns `<'UpdateDML'>`: The DML Update statement.

        ## Notice
        - The JOIN clause must be placed after the UPDATE clause.
        - For multiple joins, chain the `Join()` method after another `Join()`.
        - To set index hints, chain the `UseIndex()`, `IgnoreIndex()` and
          `ForceIndex()` methods after `Join()`.

        ## Example
        - Please refer to the <'SelectDML'> statment `RightJoin()` method.
        """
        self._validate_join_clause_order()
        clause: JOIN = self._gen_right_join_clause(table, on, using, partition, alias)
        if self._join_clauses is None:
            self._join_clauses = [clause]
        else:
            list_append(self._join_clauses, clause)
        return self

    def StraightJoin(
        self,
        table: object,
        *on: object,
        using: object = None,
        partition: object = None,
        alias: object = None,
    ) -> UpdateDML:
        """The STRAIGHT_JOIN clause of the UPDATE statement `<'UpdateDML'>`.

        :param table `<'str/Table/SelectDML'>`: The table (reference) to join.
        :param on `<'*str'>`: The ON condition(s) specify how to join the table.
        :param using `<'str/Column/list/tuple/None'>`: The USING condition(s) of the common column(s) between the tables. Defaults to `None`.
            When on is specified, using is ignored.
        :param partition `<'str/Partition/list/tuple/None'>`: The partition(s) of the join table. Defaults to `None`.
        :param alias `<'str/None'>`: The alias of the join table. Defaults to `None`.
            When 'alias=None', a default alias is automatically generated based on the count
            of the tables adopting default alias in the same query block (starting from 't0').
        :returns `<'UpdateDML'>`: The DML Update statement.

        ## Notice
        - The JOIN clause must be placed after the UPDATE clause.
        - For multiple joins, chain the `Join()` method after another `Join()`.
        - To set index hints, chain the `UseIndex()`, `IgnoreIndex()` and
          `ForceIndex()` methods after `Join()`.

        ## Example
        - Please refer to the <'SelectDML'> statment `StraightJoin()` method.
        """
        self._validate_join_clause_order()
        clause: JOIN = self._gen_straight_join_clause(
            table, on, using, partition, alias
        )
        if self._join_clauses is None:
            self._join_clauses = [clause]
        else:
            list_append(self._join_clauses, clause)
        return self

    def CrossJoin(
        self,
        table: object,
        *on: object,
        using: object = None,
        partition: object = None,
        alias: object = None,
    ) -> UpdateDML:
        """The CROSS JOIN clause of the UPDATE statement `<'UpdateDML'>`.

        :param table `<'str/Table/SelectDML'>`: The table (reference) to join.
        :param on `<'*str'>`: The ON condition(s) specify how to join the table.
        :param using `<'str/Column/list/tuple/None'>`: The USING condition(s) of the common column(s) between the tables. Defaults to `None`.
            When on is specified, using is ignored.
        :param partition `<'str/Partition/list/tuple/None'>`: The partition(s) of the join table. Defaults to `None`.
        :param alias `<'str/None'>`: The alias of the join table. Defaults to `None`.
            When 'alias=None', a default alias is automatically generated based on the count
            of the tables adopting default alias in the same query block (starting from 't0').
        :returns `<'UpdateDML'>`: The DML Update statement.

        ## Notice
        - The JOIN clause must be placed after the UPDATE clause.
        - For multiple joins, chain the `Join()` method after another `Join()`.
        - To set index hints, chain the `UseIndex()`, `IgnoreIndex()` and
          `ForceIndex()` methods after `Join()`.

        ## Example
        - Please refer to the <'SelectDML'> statment `CrossJoin()` method.
        """
        self._validate_join_clause_order()
        clause: JOIN = self._gen_cross_join_clause(table, on, using, partition, alias)
        if self._join_clauses is None:
            self._join_clauses = [clause]
        else:
            list_append(self._join_clauses, clause)
        return self

    def NaturalJoin(
        self,
        table: object,
        join_method: object = "INNER",
        partition: object = None,
        alias: object = None,
    ) -> UpdateDML:
        """The NATURAL JOIN clause of the UPDATE statement `<'UpdateDML'>`.

        :param table `<'str/Table/SelectDML'>`: The table (reference) to join.
        :param join_method `<'str'>`: The join method. Defaults to `"INNER"`.
            Accepts: `"INNER"`, `"LEFT"`, `"RIGHT"`.
        :param partition `<'str/Partition/list/tuple/None'>`: The partition(s) of the join table. Defaults to `None`.
        :param alias `<'str/None'>`: The alias of the join table. Defaults to `None`.
            When 'alias=None', a default alias is automatically generated based on the count
            of the tables adopting default alias in the same query block (starting from 't0').
        :returns `<'UpdateDML'>`: The DML Update statement.

        ## Notice
        - The JOIN clause must be placed after the UPDATE clause.
        - For multiple joins, chain the `Join()` method after another `Join()`.
        - To set index hints, chain the `UseIndex()`, `IgnoreIndex()` and
          `ForceIndex()` methods after `Join()`.

        ## Example
        - Please refer to the <'SelectDML'> statment `NaturalJoin()` method.
        """
        self._validate_join_clause_order()
        clause: JOIN = self._gen_natural_join_clause(
            table, join_method, partition, alias
        )
        if self._join_clauses is None:
            self._join_clauses = [clause]
        else:
            list_append(self._join_clauses, clause)
        return self

    # . index hints
    def UseIndex(self, *indexes: object, scope: object = None) -> UpdateDML:
        """The USE INDEX clause of the UPDATE statement `<'UpdateDML'>`.

        :param indexes `<'*str/Index'>`: The index(es) to use by the INDEX HINTS.
        :param scope `<'str/None'>`: The scope of the INDEX HINTS. Defaults to `None`.
            Accepts: `"JOIN"`, `"ORDER BY"` and `"GROUP BY"`.
            This provides more fine-grained control over optimizer selection of an execution plan
            for various phases of query processing. To affect only the indexes used when MySQL
            decides how to find rows in the table and how to process joins, use FOR JOIN. To
            influence index usage for sorting or grouping rows, use FOR ORDER BY or FOR GROUP BY.
        :returns `<'UpdateDML'>`: The DML Update statement.

        ## Notice
        - The USE INDEX clause must be placed after the UPDATE or JOIN
          clause or chain after another INDEX HINTS clause.

        ## Example
        - Please refer to the <'SelectDML'> statment `UseIndex()` method.
        """
        # fmt: off
        if self._clause_id == utils.DML_CLAUSE.UPDATE:
            clause: CLAUSE = self._update_clause
        elif self._clause_id == utils.DML_CLAUSE.JOIN and self._join_clauses is not None:
            clause: CLAUSE = self._join_clauses[list_len(self._join_clauses) - 1]
        else:
            self._raise_clause_order_error(
                "USE INDEX", ["UPDATE", "JOIN", "another INDEX HINTS"]
            )
        # fmt: on
        clause._add_index_hints(self._gen_use_index_clause(indexes, scope))
        return self

    def IgnoreIndex(self, *indexes: object, scope: object = None) -> UpdateDML:
        """The IGNORE INDEX clause of the UPDATE statement `<'UpdateDML'>`.

        :param indexes `<'*str/Index'>`: The index(es) to ignore by the INDEX HINTS.
        :param scope `<'str/None'>`: The scope of the INDEX HINTS. Defaults to `None`.
            Accepts: `"JOIN"`, `"ORDER BY"` and `"GROUP BY"`.
            This provides more fine-grained control over optimizer selection of an execution plan
            for various phases of query processing. To affect only the indexes used when MySQL
            decides how to find rows in the table and how to process joins, use FOR JOIN. To
            influence index usage for sorting or grouping rows, use FOR ORDER BY or FOR GROUP BY.
        :returns `<'UpdateDML'>`: The DML Update statement.

        ## Notice
        - The IGNORE INDEX clause must be placed after the UPDATE or JOIN
          clause or chain after another INDEX HINTS clause.

        ## Example
        - Please refer to the <'SelectDML'> statment `IgnoreIndex()` method.
        """
        # fmt: off
        if self._clause_id == utils.DML_CLAUSE.UPDATE:
            clause: CLAUSE = self._update_clause
        elif self._clause_id == utils.DML_CLAUSE.JOIN and self._join_clauses is not None:
            clause: CLAUSE = self._join_clauses[list_len(self._join_clauses) - 1]
        else:
            self._raise_clause_order_error(
                "IGNORE INDEX", ["UPDATE", "JOIN", "another INDEX HINTS"]
            )
        # fmt: on
        clause._add_index_hints(self._gen_ignore_index_clause(indexes, scope))
        return self

    def ForceIndex(self, *indexes: object, scope: object = None) -> UpdateDML:
        """The FORCE INDEX clause of the UPDATE statement `<'UpdateDML'>`.

        :param indexes `<'*str/Index'>`: The index(es) to force by the INDEX HINTS.
        :param scope `<'str/None'>`: The scope of the INDEX HINTS. Defaults to `None`.
            Accepts: `"JOIN"`, `"ORDER BY"` and `"GROUP BY"`.
            This provides more fine-grained control over optimizer selection of an execution plan
            for various phases of query processing. To affect only the indexes used when MySQL
            decides how to find rows in the table and how to process joins, use FOR JOIN. To
            influence index usage for sorting or grouping rows, use FOR ORDER BY or FOR GROUP BY.
        :returns `<'UpdateDML'>`: The DML Update statement.

        ## Notice
        - The FORCE INDEX clause must be placed after the UPDATE or JOIN
          clause or chain after another INDEX HINTS clause.

        ## Example
        - Please refer to the <'SelectDML'> statment `ForceIndex()` method.
        """
        # fmt: off
        if self._clause_id == utils.DML_CLAUSE.UPDATE:
            clause: CLAUSE = self._update_clause
        elif self._clause_id == utils.DML_CLAUSE.JOIN and self._join_clauses is not None:
            clause: CLAUSE = self._join_clauses[list_len(self._join_clauses) - 1]
        else:
            self._raise_clause_order_error(
                "FORCE INDEX", ["UPDATE", "JOIN", "another INDEX HINTS"]
            )
        # fmt: on
        clause._add_index_hints(self._gen_force_index_clause(indexes, scope))
        return self

    # . where
    def Where(
        self,
        *conds: str,
        in_conds: dict | None = None,
        not_in_conds: dict | None = None,
    ) -> UpdateDML:
        """The WHERE clause of the UPDATE statement `<'UpdateDML'>`.

        :param conds `<'*str'>`: The condition(s) that rows must satisfy to be selected.
        :param in_conds `<'dict/None'>`: The IN condition(s). Defaults to `None`.
        :param not_in_conds `<'dict/None'>`: The NOT IN condition(s). Defaults to `None`.
        :returns `<'UpdateDML'>`: The DML Update statement.

        ## Explanation
        - All conditions are combined with `AND` logic by default.
        - Prepend the condition with "`OR `" to alter the default behavior.
        - The 'in_conds' and 'not_in_conds' arguments must be a dictionary,
          where the keys should be the columns and values the desired ones
          to check against. Values can be any escapable objects or a subquery
          `<'SelectDML'>` instance, but `NOT` strings with placeholders.

        ## Example
        - Please refer to the <'SelectDML'> statment `Where()` method.
        """
        if self._where_clause is not None:
            self._raise_clause_already_set_error(self._where_clause)
        self._where_clause = self._gen_where_clause(conds, in_conds, not_in_conds)
        return self

    # . order by
    def OrderBy(self, *columns: object) -> UpdateDML:
        """The ORDER BY clause of the UPDATE statement `<'UpdateDML'>`.

        :param columns `<'*str/Column'>`: The ordering (expression of) column(s).
            Determines the ordering of the rows to be updated.
            Each element can be a column name or any SQL expression,
            optionally suffixed with 'ASC' or 'DESC'.
        :returns `<'UpdateDML'>`: The DML Update statement.

        ## Notice
        - Only applicable to single-table update.

        ## Example
        - Please refer to the <'SelectDML'> statment `OrderBy()` method.
        """
        if self._join_clauses is not None:
            self._raise_clause_error(
                "ORDER BY clause is not compatible with multi-table UPDATE."
            )
        if self._order_by_clause is not None:
            self._raise_clause_already_set_error(self._order_by_clause)
        self._order_by_clause = self._gen_order_by_clause(columns, False)
        return self

    # . limit
    def Limit(self, row_count: object) -> UpdateDML:
        """The LIMIT clause of the UPDATE statement `<'UpdateDML'>`.

        :param row_count `<'int'>`: The number of limited rows to update.
            The statement stops as soon as it has found 'row_count' rows that
            satisfy the WHERE clause, whether or not they actually were changed.
        :returns `<'UpdateDML'>`: The DML Update statement.

        ## Notice
        - Only applicable to single-table update.

        ## Example
        - Please refer to the <'SelectDML'> statment `Limit()` method.
        """
        if self._join_clauses is not None:
            self._raise_clause_error(
                "LIMIT clause is not compatible with multi-table UPDATE."
            )
        if self._limit_clause is not None:
            self._raise_clause_already_set_error(self._limit_clause)
        self._limit_clause = self._gen_limit_clause(row_count, None)
        return self

    # Statement ----------------------------------------------------------------------------
    @cython.ccall
    def statement(self, indent: cython.int = 0) -> str:
        """Compose the UPDATE statement `<'str'>`.

        :param indent `<'int'>`: The indentation level of the statement. Defaults to 0.

        ## Example
        >>> db.Update(db.tb).Set("first_name=%s").Where("id=%s").statement()
        >>> "UPDATE db.tb AS t0 SET first_name=%s WHERE id=%s"
        """
        pad: str = self._validate_indent(indent)
        # . with
        i: CLAUSE
        if self._with_clauses is not None:
            if list_len(self._with_clauses) == 1:
                i = self._with_clauses[0]
                clauses: list = [i.clause(pad)]
            else:
                l = [i.clause(pad) for i in self._with_clauses]
                clauses: list = [",\n".join(l)]
        else:
            clauses: list = []

        # . update
        if self._update_clause is None:
            self._raise_clause_error("UPDATE clause is not set.")
        clauses.append(self._update_clause.clause(pad))

        # . join
        i: CLAUSE
        if self._join_clauses is not None:
            for i in self._join_clauses:
                clauses.append(i.clause(pad))

        # . set
        if self._set_clause is None:
            self._raise_clause_error("SET clause is not set.")
        clauses.append(self._set_clause.clause(pad))

        # . where
        if self._where_clause is not None:
            clauses.append(self._where_clause.clause(pad))

        # . order by
        if self._order_by_clause is not None:
            clauses.append(self._order_by_clause.clause(pad))

        # . limit
        if self._limit_clause is not None:
            clauses.append(self._limit_clause.clause(pad))

        # Compose
        return "\n".join(clauses)

    # Execute ------------------------------------------------------------------------------
    @cython.ccall
    def Execute(
        self,
        args: object = None,
        many: cython.bint = False,
        conn: object | None = None,
    ) -> object:
        """[sync] Execute the UPDATE statement, and returns the affected rows `<'int'>`

        :param args `<'list/tuple/DataFrame/Any'>`: The arguments (data) for the placeholders in the statement. Defaults to `None`.

        :param many `<'bool'>`: Whether the 'args' (data) is multi-rows. Determines how the data is escaped. Defaults to `False`.
            For a single-row data, set 'many=False'. For a multi-row data, set 'many=True'.

        :param conn `<'PoolSyncConnection/None'>`: The specific [sync] connection to execute the statement. Defaults to `None`.
            If 'conn=None', the statement will be executed by a (random) connection
            from the pool, and commit automatically after the statement execution.

        :returns `<'int'>`: Number of affected rows (sum of rows actually updated).

        ## Example (single-row)
        >>> (
                db.Update(db.tb)
                .Set("first_name=%s, last_name=%s")
                .Where("id=%s")
                .Execute(["John", "Ivy", 1], many=False)
            )
            # Equivalent to:
            UPDATE db.tb AS t0
            SET first_name='John', last_name='Ivy'
            WHERE id=1

        ## Example (multi-rows)
        >>> (
                db.Update(db.tb)
                .Set("first_name=%s, last_name=%s")
                .Where("id=%s")
                .Execute([("John", "Ivy", 1), ("Sarah", "Kaye", 2)], many=True)
            )
            # Equivalent to (two queries):
            UPDATE db.tb AS t0
            SET first_name='John', last_name='Ivy'
            WHERE id=1;
            UPDATE db.tb AS t0
            SET first_name='Sarah', last_name='Kaye'
            WHERE id=2;

        ## Example (connection)
        >>> with db.transaction() as conn:
                (
                    db.Select("*")
                    .From(db.tb)
                    .Where("id=%s")
                    .ForUpdate()
                    .Execute(1, conn=conn)
                )
                )
                    db.Update(db.tb)
                    .Set("first_name=%s, last_name=%s")
                    .Where("id=%s")
                    .Execute([("John", "Ivy", 1)], many=False, conn=conn)
                )
            # Equivalent to:
            BEGIN;
            SELECT * FROM db.tb AS t0 WHERE id=1 FOR UPDATE;
            UPDATE db.tb AS t0
            SET first_name='John', last_name='Ivy'
            WHERE id=1;
            COMMIT;
        """
        return self._Execute(args, None, False, many, conn)

    async def aioExecute(
        self,
        args: object = None,
        many: cython.bint = False,
        conn: object | None = None,
    ) -> object:
        """[async] Execute the UPDATE statement, and returns the affected rows `<'int'>`

        :param args `<'list/tuple/DataFrame/Any'>`: The arguments (data) for the placeholders in the statement. Defaults to `None`.

        :param many `<'bool'>`: Whether the 'args' (data) is multi-rows. Determines how the data is escaped. Defaults to `False`.
            For a single-row data, set 'many=False'. For a multi-row data, set 'many=True'.

        :param conn `<'PoolConnection/None'>`: The specific [async] connection to execute the statement. Defaults to `None`.
            If 'conn=None', the statement will be executed by a (random) connection
            from the pool, and commit automatically after the statement execution.

        :returns `<'int'>`: Number of affected rows (sum of rows actually updated).

        ## Example (single-row)
        >>> (
                await db.Update(db.tb)
                .Set("first_name=%s, last_name=%s")
                .Where("id=%s")
                .aioExecute(["John", "Ivy", 1], many=False)
            )
            # Equivalent to:
            UPDATE db.tb AS t0
            SET first_name='John', last_name='Ivy'
            WHERE id=1

        ## Example (multi-rows)
        >>> (
                await db.Update(db.tb)
                .Set("first_name=%s, last_name=%s")
                .Where("id=%s")
                .aioExecute([("John", "Ivy", 1), ("Sarah", "Kaye", 2)], many=True)
            )
            # Equivalent to (concurrent):
            UPDATE db.tb AS t0
            SET first_name='John', last_name='Ivy'
            WHERE id=1;
            UPDATE db.tb AS t0
            SET first_name='Sarah', last_name='Kaye'
            WHERE id=2;

        ## Example (connection)
        >>> async with db.transaction() as conn:
                (
                    await db.Select("*")
                    .From(db.tb)
                    .Where("id=%s")
                    .ForUpdate()
                    .aioExecute(1, conn=conn)
                )
                )
                    await db.Update(db.tb)
                    .Set("first_name=%s, last_name=%s")
                    .Where("id=%s")
                    .aioExecute([("John", "Ivy", 1)], many=False, conn=conn)
                )
            # Equivalent to:
            BEGIN;
            SELECT * FROM db.tb AS t0 WHERE id=1 FOR UPDATE;
            UPDATE db.tb AS t0
            SET first_name='John', last_name='Ivy'
            WHERE id=1;
            COMMIT;
        """
        return await self._aioExecute(args, None, False, many, conn)

    # Validate -----------------------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(-1, check=False)
    def _validate_join_clause_order(self) -> cython.bint:
        """(internal) Validate if the order of the JOIN cluase is correct."""
        if self._clause_id not in (utils.DML_CLAUSE.UPDATE, utils.DML_CLAUSE.JOIN):
            self._raise_clause_order_error("JOIN", ["UPDATE", "another JOIN"])
        return True


# Delete - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
@cython.cclass
class DeleteDML(DML):
    """Represents the DML DELETE statement."""

    # clauses
    _delete_clause: DELETE

    def __init__(self, db_name: str, pool: Pool):
        """The DML DELETE statement.

        :param db_name `<'str'>`: The name of the database.
        :param pool `<'Pool'>`: The connection pool to handle the statement execution.
        """
        super().__init__("DELETE", db_name, pool)
        # clauses
        self._delete_clause = None

    # Clause -------------------------------------------------------------------------------
    # . delete
    @cython.ccall
    def Delete(
        self,
        table: object,
        partition: object = None,
        ignore: cython.bint = False,
        low_priority: cython.bint = False,
        quick: cython.bint = False,
        alias: object = None,
        multi_tables: object = None,
    ) -> DeleteDML:
        """The DELETE clause of the DELETE statement `<'DeleteDML'>`.

        :param table `<'str/Table'>`: The table from which to delete data.
            Only accepts one table. For multiple-table, please use the explicit JOIN clause instead.

        :param partition `<'str/Partition/list/tuple/None'>`: The partition(s) of the table. Defaults to `None`.

        :param ignore `<'bool'>`: Whether to ignore the (ignorable) errors. Defaults to `False`.
            When 'ignore=True', causes MySQL to ignore errors during the process of deleting rows.

        :param low_priority `<'bool'>`: Whether to enable the optional `LOW_PRIORITY` modifier. Defaults to `False`.
            When 'low_priority=True', the server delays execution of the DELETE until no
            other clients are reading from the table. This affects only storage engines
            that use only table-level locking (such as MyISAM, MEMORY, and MERGE).

        :param quick `<'bool'>`: Whether to enable the optional `QUICK` modifier. Defaults to `False`.
            When 'quick=True', MyISAM storage engine does not merge index leaves during
            delete, which may speed up some kinds of delete operations.

        :param alias `<'str/None'>`: The alias of the table. Defaults to `None`.
            When 'alias=None', a default alias is automatically generated based on the count
            of the tables adopting default alias in the same query block (starting from 't0').

        :param multi_tables `<'str/list/tuple/None'>`: The the table alias(es) for multi-table delete. Defaults to `None`.
            This argument should be used in combination with the `JOIN` clauses. Only
            the data of the table(s) specified in this argument will be deleted for
            multi-table DELETE operation when the condition is met.

        :returns `<'DeleteDML'>`: The DML Delete statement.

        ## Notice
        - To set index hints, chain the `UseIndex()`, `IgnoreIndex()` and
          `ForceIndex()` methods after Delete(). Only applicable to
          multi-table delete statement.

        ## Example (single-table)
        >>> db.Delete(db.tb).Where("id=1")
            # Equivalent to:
            DELETE FROM db.tb AS t0 WHERE id=1

        ## Example (multi-table)
        >>> (
                db.Delete(db.tb1, multi_tables=["t0", "t1"])
                .Join(db.tb2, "t0.id=t1.id")
                .Where("t0.id=1")
            )
            # Equivalent to:
            DELETE t0, t1 FROM db.tb1 AS t0
            JOIN db.tb2 AS t1 ON t0.id=t1.id
            WHERE t0.id=1
        """
        if self._delete_clause is not None:
            self._raise_clause_already_set_error(self._delete_clause)
        if self._clause_id != utils.DML_CLAUSE.NONE:
            self._raise_clause_error("must start with the DELETE clause.")
        self._delete_clause = self._gen_delete_clause(
            table, partition, ignore, low_priority, quick, alias, multi_tables
        )
        return self

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # . join
    def Join(
        self,
        table: object,
        *on: str,
        using: object = None,
        partition: object = None,
        alias: object = None,
    ) -> DeleteDML:
        """The (INNER) JOIN clause of the DELETE statement `<'DeleteDML'>`.

        :param table `<'str/Table/SelectDML'>`: The table (reference) to join.
        :param on `<'*str'>`: The ON condition(s) specify how to join the table.
        :param using `<'str/Column/list/tuple/None'>`: The USING condition(s) of the common column(s) between the tables. Defaults to `None`.
            When on is specified, using is ignored.
        :param partition `<'str/Partition/list/tuple/None'>`: The partition(s) of the join table. Defaults to `None`.
        :param alias `<'str/None'>`: The alias of the join table. Defaults to `None`.
            When 'alias=None', a default alias is automatically generated based on the count
            of the tables adopting default alias in the same query block (starting from 't0').
        :returns `<'DeleteDML'>`: The DML Delete statement.

        ## Notice
        - The JOIN clause must be placed after the DELETE clause.
        - For multiple joins, chain the `Join()` method after another `Join()`.
        - To set index hints, chain the `UseIndex()`, `IgnoreIndex()` and
          `ForceIndex()` methods after `Join()`.

        ## Example
        - Please refer to the <'SelectDML'> statment `Join()` method.
        """
        self._validate_join_clause_order()
        clause: JOIN = self._gen_inner_join_clause(table, on, using, partition, alias)
        if self._join_clauses is None:
            self._join_clauses = [clause]
        else:
            list_append(self._join_clauses, clause)
        return self

    def LeftJoin(
        self,
        table: object,
        *on: object,
        using: object = None,
        partition: object = None,
        alias: object = None,
    ) -> DeleteDML:
        """The LEFT JOIN clause of the DELETE statement `<'DeleteDML'>`.

        :param table `<'str/Table/SelectDML'>`: The table (reference) to join.
        :param on `<'*str'>`: The ON condition(s) specify how to join the table.
        :param using `<'str/Column/list/tuple/None'>`: The USING condition(s) of the common column(s) between the tables. Defaults to `None`.
            When on is specified, using is ignored.
        :param partition `<'str/Partition/list/tuple/None'>`: The partition(s) of the join table. Defaults to `None`.
        :param alias `<'str/None'>`: The alias of the join table. Defaults to `None`.
            When 'alias=None', a default alias is automatically generated based on the count
            of the tables adopting default alias in the same query block (starting from 't0').
        :returns `<'DeleteDML'>`: The DML Delete statement.

        ## Notice
        - The JOIN clause must be placed after the DELETE clause.
        - For multiple joins, chain the `Join()` method after another `Join()`.
        - To set index hints, chain the `UseIndex()`, `IgnoreIndex()` and
          `ForceIndex()` methods after `Join()`.

        ## Example
        - Please refer to the <'SelectDML'> statment `LeftJoin()` method.
        """
        self._validate_join_clause_order()
        clause: JOIN = self._gen_left_join_clause(table, on, using, partition, alias)
        if self._join_clauses is None:
            self._join_clauses = [clause]
        else:
            list_append(self._join_clauses, clause)
        return self

    def RightJoin(
        self,
        table: object,
        *on: object,
        using: object = None,
        partition: object = None,
        alias: object = None,
    ) -> DeleteDML:
        """The RIGHT JOIN clause of the DELETE statement `<'DeleteDML'>`.

        :param table `<'str/Table/SelectDML'>`: The table (reference) to join.
        :param on `<'*str'>`: The ON condition(s) specify how to join the table.
        :param using `<'str/Column/list/tuple/None'>`: The USING condition(s) of the common column(s) between the tables. Defaults to `None`.
            When on is specified, using is ignored.
        :param partition `<'str/Partition/list/tuple/None'>`: The partition(s) of the join table. Defaults to `None`.
        :param alias `<'str/None'>`: The alias of the join table. Defaults to `None`.
            When 'alias=None', a default alias is automatically generated based on the count
            of the tables adopting default alias in the same query block (starting from 't0').
        :returns `<'DeleteDML'>`: The DML Delete statement.

        ## Notice
        - The JOIN clause must be placed after the DELETE clause.
        - For multiple joins, chain the `Join()` method after another `Join()`.
        - To set index hints, chain the `UseIndex()`, `IgnoreIndex()` and
          `ForceIndex()` methods after `Join()`.

        ## Example
        - Please refer to the <'SelectDML'> statment `RightJoin()` method.
        """
        self._validate_join_clause_order()
        clause: JOIN = self._gen_right_join_clause(table, on, using, partition, alias)
        if self._join_clauses is None:
            self._join_clauses = [clause]
        else:
            list_append(self._join_clauses, clause)
        return self

    def StraightJoin(
        self,
        table: object,
        *on: object,
        using: object = None,
        partition: object = None,
        alias: object = None,
    ) -> DeleteDML:
        """The STRAIGHT_JOIN clause of the DELETE statement `<'DeleteDML'>`.

        :param table `<'str/Table/SelectDML'>`: The table (reference) to join.
        :param on `<'*str'>`: The ON condition(s) specify how to join the table.
        :param using `<'str/Column/list/tuple/None'>`: The USING condition(s) of the common column(s) between the tables. Defaults to `None`.
            When on is specified, using is ignored.
        :param partition `<'str/Partition/list/tuple/None'>`: The partition(s) of the join table. Defaults to `None`.
        :param alias `<'str/None'>`: The alias of the join table. Defaults to `None`.
            When 'alias=None', a default alias is automatically generated based on the count
            of the tables adopting default alias in the same query block (starting from 't0').
        :returns `<'DeleteDML'>`: The DML Delete statement.

        ## Notice
        - The JOIN clause must be placed after the DELETE clause.
        - For multiple joins, chain the `Join()` method after another `Join()`.
        - To set index hints, chain the `UseIndex()`, `IgnoreIndex()` and
          `ForceIndex()` methods after `Join()`.

        ## Example
        - Please refer to the <'SelectDML'> statment `StraightJoin()` method.
        """
        self._validate_join_clause_order()
        clause: JOIN = self._gen_straight_join_clause(
            table, on, using, partition, alias
        )
        if self._join_clauses is None:
            self._join_clauses = [clause]
        else:
            list_append(self._join_clauses, clause)
        return self

    def CrossJoin(
        self,
        table: object,
        *on: object,
        using: object = None,
        partition: object = None,
        alias: object = None,
    ) -> DeleteDML:
        """The CROSS JOIN clause of the DELETE statement `<'DeleteDML'>`.

        :param table `<'str/Table/SelectDML'>`: The table (reference) to join.
        :param on `<'*str'>`: The ON condition(s) specify how to join the table.
        :param using `<'str/Column/list/tuple/None'>`: The USING condition(s) of the common column(s) between the tables. Defaults to `None`.
            When on is specified, using is ignored.
        :param partition `<'str/Partition/list/tuple/None'>`: The partition(s) of the join table. Defaults to `None`.
        :param alias `<'str/None'>`: The alias of the join table. Defaults to `None`.
            When 'alias=None', a default alias is automatically generated based on the count
            of the tables adopting default alias in the same query block (starting from 't0').
        :returns `<'DeleteDML'>`: The DML Delete statement.

        ## Notice
        - The JOIN clause must be placed after the DELETE clause.
        - For multiple joins, chain the `Join()` method after another `Join()`.
        - To set index hints, chain the `UseIndex()`, `IgnoreIndex()` and
          `ForceIndex()` methods after `Join()`.

        ## Example
        - Please refer to the <'SelectDML'> statment `CrossJoin()` method.
        """
        self._validate_join_clause_order()
        clause: JOIN = self._gen_cross_join_clause(table, on, using, partition, alias)
        if self._join_clauses is None:
            self._join_clauses = [clause]
        else:
            list_append(self._join_clauses, clause)
        return self

    def NaturalJoin(
        self,
        table: object,
        join_method: object = "INNER",
        partition: object = None,
        alias: object = None,
    ) -> DeleteDML:
        """The NATURAL JOIN clause of the DELETE statement `<'DeleteDML'>`.

        :param table `<'str/Table/SelectDML'>`: The table (reference) to join.
        :param join_method `<'str'>`: The join method. Defaults to `"INNER"`.
            Accepts: `"INNER"`, `"LEFT"`, `"RIGHT"`.
        :param partition `<'str/Partition/list/tuple/None'>`: The partition(s) of the join table. Defaults to `None`.
        :param alias `<'str/None'>`: The alias of the join table. Defaults to `None`.
            When 'alias=None', a default alias is automatically generated based on the count
            of the tables adopting default alias in the same query block (starting from 't0').
        :returns `<'DeleteDML'>`: The DML Delete statement.

        ## Notice
        - The JOIN clause must be placed after the DELETE clause.
        - For multiple joins, chain the `Join()` method after another `Join()`.
        - To set index hints, chain the `UseIndex()`, `IgnoreIndex()` and
          `ForceIndex()` methods after `Join()`.

        ## Example
        - Please refer to the <'SelectDML'> statment `NaturalJoin()` method.
        """
        self._validate_join_clause_order()
        clause: JOIN = self._gen_natural_join_clause(
            table, join_method, partition, alias
        )
        if self._join_clauses is None:
            self._join_clauses = [clause]
        else:
            list_append(self._join_clauses, clause)
        return self

    # . index hints
    def UseIndex(self, *indexes: object, scope: object = None) -> DeleteDML:
        """The USE INDEX clause of the DELETE statement `<'DeleteDML'>`.

        :param indexes `<'*str/Index'>`: The index(es) to use by the INDEX HINTS.
        :param scope `<'str/None'>`: The scope of the INDEX HINTS. Defaults to `None`.
            Accepts: `"JOIN"`, `"ORDER BY"` and `"GROUP BY"`.
            This provides more fine-grained control over optimizer selection of an execution plan
            for various phases of query processing. To affect only the indexes used when MySQL
            decides how to find rows in the table and how to process joins, use FOR JOIN. To
            influence index usage for sorting or grouping rows, use FOR ORDER BY or FOR GROUP BY.
        :returns `<'DeleteDML'>`: The DML Delete statement.

        ## Notice
        - The USE INDEX clause must be placed after the DELETE or JOIN
          clause or chain after another INDEX HINTS clause.

        ## Example
        - Please refer to the <'SelectDML'> statment `UseIndex()` method.
        """
        # fmt: off
        if self._clause_id == utils.DML_CLAUSE.DELETE:
            clause: CLAUSE = self._delete_clause
        elif self._clause_id == utils.DML_CLAUSE.JOIN and self._join_clauses is not None:
            clause: CLAUSE = self._join_clauses[list_len(self._join_clauses) - 1]
        else:
            self._raise_clause_order_error(
                "USE INDEX", ["DELETE", "JOIN", "another INDEX HINTS"]
            )
        # fmt: on
        clause._add_index_hints(self._gen_use_index_clause(indexes, scope))
        return self

    def IgnoreIndex(self, *indexes: object, scope: object = None) -> DeleteDML:
        """The IGNORE INDEX clause of the DELETE statement `<'DeleteDML'>`.

        :param indexes `<'*str/Index'>`: The index(es) to ignore by the INDEX HINTS.
        :param scope `<'str/None'>`: The scope of the INDEX HINTS. Defaults to `None`.
            Accepts: `"JOIN"`, `"ORDER BY"` and `"GROUP BY"`.
            This provides more fine-grained control over optimizer selection of an execution plan
            for various phases of query processing. To affect only the indexes used when MySQL
            decides how to find rows in the table and how to process joins, use FOR JOIN. To
            influence index usage for sorting or grouping rows, use FOR ORDER BY or FOR GROUP BY.
        :returns `<'DeleteDML'>`: The DML Delete statement.

        ## Notice
        - The IGNORE INDEX clause must be placed after the DELETE or JOIN
          clause or chain after another INDEX HINTS clause.

        ## Example
        - Please refer to the <'SelectDML'> statment `IgnoreIndex()` method.
        """
        # fmt: off
        if self._clause_id == utils.DML_CLAUSE.DELETE:
            clause: CLAUSE = self._delete_clause
        elif self._clause_id == utils.DML_CLAUSE.JOIN and self._join_clauses is not None:
            clause: CLAUSE = self._join_clauses[list_len(self._join_clauses) - 1]
        else:
            self._raise_clause_order_error(
                "IGNORE INDEX", ["DELETE", "JOIN", "another INDEX HINTS"]
            )
        # fmt: on
        clause._add_index_hints(self._gen_ignore_index_clause(indexes, scope))
        return self

    def ForceIndex(self, *indexes: object, scope: object = None) -> DeleteDML:
        """The FORCE INDEX clause of the DELETE statement `<'DeleteDML'>`.

        :param indexes `<'*str/Index'>`: The index(es) to force by the INDEX HINTS.
        :param scope `<'str/None'>`: The scope of the INDEX HINTS. Defaults to `None`.
            Accepts: `"JOIN"`, `"ORDER BY"` and `"GROUP BY"`.
            This provides more fine-grained control over optimizer selection of an execution plan
            for various phases of query processing. To affect only the indexes used when MySQL
            decides how to find rows in the table and how to process joins, use FOR JOIN. To
            influence index usage for sorting or grouping rows, use FOR ORDER BY or FOR GROUP BY.
        :returns `<'DeleteDML'>`: The DML Delete statement.

        ## Notice
        - The FORCE INDEX clause must be placed after the DELETE or JOIN
          clause or chain after another INDEX HINTS clause.

        ## Example
        - Please refer to the <'SelectDML'> statment `Index()` method.
        """
        # fmt: off
        if self._clause_id == utils.DML_CLAUSE.DELETE:
            clause: CLAUSE = self._delete_clause
        elif self._clause_id == utils.DML_CLAUSE.JOIN and self._join_clauses is not None:
            clause: CLAUSE = self._join_clauses[list_len(self._join_clauses) - 1]
        else:
            self._raise_clause_order_error(
                "FORCE INDEX", ["DELETE", "JOIN", "another INDEX HINTS"]
            )
        # fmt: on
        clause._add_index_hints(self._gen_force_index_clause(indexes, scope))
        return self

    # . where
    def Where(
        self,
        *conds: str,
        in_conds: dict | None = None,
        not_in_conds: dict | None = None,
    ) -> DeleteDML:
        """The WHERE clause of the DELETE statement `<'DeleteDML'>`.

        :param conds `<'*str'>`: The condition(s) that rows must satisfy to be selected.
        :param in_conds `<'dict/None'>`: The IN condition(s). Defaults to `None`.
        :param not_in_conds `<'dict/None'>`: The NOT IN condition(s). Defaults to `None`.
        :returns `<'DeleteDML'>`: The DML Delete statement.

        ## Explanation
        - All conditions are combined with `AND` logic by default.
        - Prepend the condition with "`OR `" to alter the default behavior.
        - The 'in_conds' and 'not_in_conds' arguments must be a dictionary,
          where the keys should be the columns and values the desired ones
          to check against. Values can be any escapable objects or a subquery
          `<'SelectDML'>` instance, but `NOT` strings with placeholders.

        ## Example
        - Please refer to the <'SelectDML'> statment `Where()` method.
        """
        if self._where_clause is not None:
            self._raise_clause_already_set_error(self._where_clause)
        self._where_clause = self._gen_where_clause(conds, in_conds, not_in_conds)
        return self

    # . order by
    def OrderBy(self, *columns: object) -> DeleteDML:
        """The ORDER BY clause of the DELETE statement `<'DeleteDML'>`.

        :param columns `<'*str/Column'>`: The ordering (expression of) column(s).
            Determines the ordering of the rows to be deleted.
            Each element can be a column name or any SQL expression,
            optionally suffixed with 'ASC' or 'DESC'.
        :returns `<'DeleteDML'>`: The DML Delete statement.

        ## Notice
        - Only applicable to single-table delete.

        ## Example
        - Please refer to the <'SelectDML'> statment `OrderBy()` method.
        """
        if self._join_clauses is not None:
            self._raise_clause_error(
                "ORDER BY clause is not compatible with multi-table DELETE."
            )
        if self._order_by_clause is not None:
            self._raise_clause_already_set_error(self._order_by_clause)
        self._order_by_clause = self._gen_order_by_clause(columns, False)
        return self

    # . limit
    def Limit(self, row_count: object) -> DeleteDML:
        """The LIMIT clause of the DELETE statement `<'DeleteDML'>`.

        :param row_count `<'int'>`: The number of limited rows to delete.
            Specify the maximum number of rows that can
            be deleted by the DELETE statement.
        :returns `<'DeleteDML'>`: The DML Delete statement.

        ## Notice
        - Only applicable to single-table delete.

        ## Example
        - Please refer to the <'SelectDML'> statment `Limit()` method.
        """
        if self._join_clauses is not None:
            self._raise_clause_error(
                "LIMITE clause is not compatible with multi-table DELETE."
            )
        if self._limit_clause is not None:
            self._raise_clause_already_set_error(self._limit_clause)
        self._limit_clause = self._gen_limit_clause(row_count, None)
        return self

    # Statement ----------------------------------------------------------------------------
    @cython.ccall
    def statement(self, indent: cython.int = 0) -> str:
        """Compose the UPDATE statement `<'str'>`.

        :param indent `<'int'>`: The indentation level of the statement. Defaults to 0.

        >>> Example
        >>> db.Delete(db.tb).Where("id=%s").statement()
        >>> "DELETE FROM db.tb AS t0 WHERE id=%s"
        """
        pad: str = self._validate_indent(indent)
        # . with
        i: CLAUSE
        if self._with_clauses is not None:
            if list_len(self._with_clauses) == 1:
                i = self._with_clauses[0]
                clauses: list = [i.clause(pad)]
            else:
                l = [i.clause(pad) for i in self._with_clauses]
                clauses: list = [",\n".join(l)]
        else:
            clauses: list = []

        # . delete & from
        if self._delete_clause is None:
            self._raise_clause_error("DELETE clause is not set.")
        clauses.append(self._delete_clause.clause(pad))

        # . join
        i: CLAUSE
        if self._join_clauses is not None:
            if not self._delete_clause._has_multi_tables():
                self._raise_clause_error(
                    "multi-table DELETE (with JOIN cluase) must configurate "
                    "the 'multi_tables' argument in the 'Delete()' method."
                )
            for i in self._join_clauses:
                clauses.append(i.clause(pad))
            self._multi_table = True
        else:
            self._multi_table = False

        # . where
        if self._where_clause is not None:
            clauses.append(self._where_clause.clause(pad))

        # . order by
        if self._order_by_clause is not None:
            clauses.append(self._order_by_clause.clause(pad))

        # . limit
        if self._limit_clause is not None:
            clauses.append(self._limit_clause.clause(pad))

        # Compose
        return "\n".join(clauses)

    # Execute ------------------------------------------------------------------------------
    @cython.ccall
    def Execute(
        self,
        args: object = None,
        many: cython.bint = False,
        conn: object | None = None,
    ) -> object:
        """[sync] Execute the DELETE statement, and returns the affected rows `<'int'>`

        :param args `<'list/tuple/DataFrame/Any'>`: The arguments (data) for the placeholders in the statement. Defaults to `None`.

        :param many `<'bool'>`: Whether the 'args' (data) is multi-rows. Determines how the data is escaped. Defaults to `False`.
            For a single-row data, set 'many=False'. For a multi-row data, set 'many=True'.

        :param conn `<'PoolSyncConnection/None'>`: The specific [sync] connection to execute the statement. Defaults to `None`.
            If 'conn=None', the statement will be executed by a (random) connection
            from the pool, and commit automatically after the statement execution.

        :returns `<'int'>`: Number of affected rows (sum of rows actually deleted).

        ## Example (single-row & single-table)
        >>> db.Delete(db.tb).Where("id=%s").Execute(1, many=False)
            # Equivalent to:
            DELETE FROM db.tb AS t0 WHERE id=1

        ## Example (multi-rows & multi-table)
        >>> (
                db.Delete(db.tb1, multi_tables=["t0", "t1"])
                .Join(db.tb2, "t0.id=t1.id")
                .Where("t0.id=%s")
                .Execute([1, 2], many=True)
            )
            # Equivalent to (concurrent):
            DELETE t0, t1 FROM db.tb1 AS t0
            JOIN db.tb2 AS t1 ON t0.id=t1.id
            WHERE t0.id=1;
            DELETE t0, t1 FROM db.tb1 AS t0
            JOIN db.tb2 AS t1 ON t0.id=t1.id
            WHERE t0.id=2;

        ## Example (connection)
        >>> with db.transaction() as conn:
                (
                    db.Select("*")
                    .From(db.tb)
                    .Where("id=%s")
                    .ForUpdate()
                    .Execute(1, conn=conn)
                )
                (
                    db.Delete(db.tb)
                    .Where("id=%s")
                    .Execute(1, many=False, conn=conn)
                )
            # Equivalent to:
            BEGIN;
            SELECT * FROM db.tb AS t0 WHERE id=1 FOR UPDATE;
            DELETE FROM db.tb AS t0 WHERE id=1;
            COMMIT;
        """
        return self._Execute(args, None, False, many, conn)

    async def aioExecute(
        self,
        args: object = None,
        many: cython.bint = False,
        conn: object | None = None,
    ) -> object:
        """[async] Execute the DELETE statement, and returns the affected rows `<'int'>`

        :param args `<'list/tuple/DataFrame/Any'>`: The arguments (data) for the placeholders in the statement. Defaults to `None`.

        :param many `<'bool'>`: Whether the 'args' (data) is multi-rows. Determines how the data is escaped. Defaults to `False`.
            For a single-row data, set 'many=False'. For a multi-row data, set 'many=True'.

        :param conn `<'PoolConnection/None'>`: The specific [async] connection to execute the statement. Defaults to `None`.
            If 'conn=None', the statement will be executed by a (random) connection
            from the pool, and commit automatically after the statement execution.

        :returns `<'int'>`: Number of affected rows (sum of rows actually deleted).

        ## Example (single-row & single-table)
        >>> await db.Delete(db.tb).Where("id=%s").aioExecute(1, many=False)
            # Equivalent to:
            DELETE FROM db.tb AS t0 WHERE id=1

        ## Example (multi-rows & multi-table)
        >>> (
                await db.Delete(db.tb1, multi_tables=["t0", "t1"])
                .Join(db.tb2, "t0.id=t1.id")
                .Where("t0.id=%s")
                .aioExecute([1, 2], many=True)
            )
            # Equivalent to (concurrent):
            DELETE t0, t1 FROM db.tb1 AS t0
            JOIN db.tb2 AS t1 ON t0.id=t1.id
            WHERE t0.id=1;
            DELETE t0, t1 FROM db.tb1 AS t0
            JOIN db.tb2 AS t1 ON t0.id=t1.id
            WHERE t0.id=2;

        ## Example (connection)
        >>> async with db.transaction() as conn:
                (
                    await db.Select("*")
                    .From(db.tb)
                    .Where("id=%s")
                    .ForUpdate()
                    .aioExecute(1, conn=conn)
                )
                (
                    await db.Delete(db.tb)
                    .Where("id=%s")
                    .aioExecute(1, many=False, conn=conn)
                )
            # Equivalent to:
            BEGIN;
            SELECT * FROM db.tb AS t0 WHERE id=1 FOR UPDATE;
            DELETE FROM db.tb AS t0 WHERE id=1;
            COMMIT;
        """
        return await self._aioExecute(args, None, False, many, conn)

    # Validate -----------------------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(-1, check=False)
    def _validate_join_clause_order(self) -> cython.bint:
        """(internal) Validate if the order of the JOIN cluase is correct."""
        if self._clause_id not in (utils.DML_CLAUSE.DELETE, utils.DML_CLAUSE.JOIN):
            self._raise_clause_order_error("JOIN", ["DELETE", "another JOIN"])
        return True


# With - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
@cython.cclass
class WithDML(DML):
    """Represents the DML statement starts with CTE (Common Table Expressions)."""

    def __init__(self, db_name: str, pool: Pool):
        """The DML statement starts with CTE (Common Table Expressions).

        :param db_name `<'str'>`: The name of the database.
        :param pool `<'Pool'>`: The connection pool to handle the statement execution.
        """
        super().__init__("WITH", db_name, pool)

    # Clause -------------------------------------------------------------------------------
    # . with
    def With(
        self,
        name: object,
        subquery: object,
        *columns: object,
        recursive: cython.bint = False,
    ) -> WithDML:
        """The WITH (Common Table Expressions) clause `<'WithDML'>`.

        :param name `<'str'>`: The name of the CTE, which can be used as a table reference in the statement.
        :param subquery `<'str/SelectDML'>`: The subquery that produces the CTE result set.
        :param columns `<'*str/Column'>`: The column names of the result set.
            If specified, the number of columns must be the same as the result set of the subquery.
        :param recursive `<'bool'>`: Whether the CTE is recursive. Defaults to `False`.
            A CTE is recursive if its subquery refers to its own name.
            For more information, refer to MySQL documentation
            ["Recursive Common Table Expressions"](https://dev.mysql.com/doc/refman/8.4/en/with.html#common-table-expressions-recursive).
        :returns `<'WithDML'>`: The DML With statement.

        ## Notice
        - For multiple CTEs, chain the With() method after another With().

        ## Example (SELECT)
        >>> (
                db.With("cte1", db.Select("id", "name").From(db.tb1))
                .With("cte2", db.Select("id", "name").From(db.tb2))
                .Select("*")
                .From("cte1")
                .Union(db.Select("*").From("cte2"))
            )
            # Equivalent to:
            WITH cte1 AS (
                SELECT id, name FROM db.tb1 AS t0
            ), cte2 AS (
                SELECT id, name FROM db.tb2 AS t0
            )
            SELECT * FROM cte1 AS t0
            UNION DISTINCT (
                SELECT * FROM cte2 AS t0
            )

        ## Example (UPDATE)
        >>> (
                db.With("cte", db.Select("*").From(db.tb2))
                .Update(db.tb1)
                .Join("cte", "t0.id=t1.id")
                .Set("t0.name=t1.name", "t0.price=t1.price")
            )
            # Equivalent to:
            WITH cte AS (
                SELECT * FROM db.tb2 AS t0
            )
            UPDATE db.tb1 AS t0
            INNER JOIN cte AS t1 ON t0.id=t1.id
            SET t0.name=t1.name, t0.price=t1.price

        ### Example (DELETE)
        >>> (
                db.With("cte", db.Select("*").From(db.tb2))
                .Delete(db.tb1, multi_tables=["t0"])
                .Join("cte", "t0.id=t1.id")
            )
            # Equivalent to:
            WITH cte AS (
                SELECT * FROM db.tb2 AS t0
            )
            DELETE t0 FROM db.tb1 AS t0
            INNER JOIN cte AS t1 ON t0.id=t1.id
        """
        return self._With(name, subquery, columns, recursive)

    @cython.ccall
    def _With(
        self,
        name: object,
        subquery: object,
        columns: tuple,
        recursive: cython.bint = False,
    ) -> WithDML:
        """(internal) The WITH (Common Table Expressions) clause `<'WithDML'>`.

        :param name `<'str'>`: The name of the CTE, which can be used as a table reference in the statement.
        :param subquery `<'str/SelectDML'>`: The subquery that produces the CTE result set.
        :param columns `<'tuple[str/Column]'>`: The column names of the result set.
            If specified, the number of columns must be the same as the result set of the subquery.
        :param recursive `<'bool'>`: Whether the CTE is recursive. Defaults to `False`.
            A CTE is recursive if its subquery refers to its own name.
            For more information, refer to MySQL documentation
            ["Recursive Common Table Expressions"](https://dev.mysql.com/doc/refman/8.4/en/with.html#common-table-expressions-recursive).
        :returns `<'WithDML'>`: The DML With statement.

        ## Notice
        - For multiple CTEs, chain the With() method after another With().
        """
        if self._clause_id not in (utils.DML_CLAUSE.NONE, utils.DML_CLAUSE.WITH):
            self._raise_clause_error("must start with the WITH clause.")
        clause: WITH = self._gen_with_clause(name, subquery, columns, recursive)
        if self._with_clauses is None:
            self._with_clauses = [clause]
        else:
            clause._primary = False
            list_append(self._with_clauses, clause)
        return self

    # . select
    def Select(
        self,
        *expressions: object,
        distinct: cython.bint = False,
        high_priority: cython.bint = False,
        straight_join: cython.bint = False,
        sql_buffer_result: cython.bint = False,
    ) -> SelectDML:
        """The SELECT clause of the SELECT statement `<'SelectDML'>`.

        :param expressions `<'*str/SQLFunction/Column'>`: The (expression of) the column(s) to retrieve.

        :param distinct `<'bool'>`: If `True`, specifies removal of duplicate rows from
            the result set. Defaults to `False`.

        :param high_priority `<'bool'>`: If `True`, gives the SELECT higher priority than
            a statement that updates a table. Defaults to `False`. Should only be used for
            queries that are very fast and must be done at once. A SELECT HIGH_PRIORITY
            query that is issued while the table is locked for reading runs even if there
            is an update statement waiting for the table to be free. This affects only storage
            engines that use only table-level locking (such as MyISAM, MEMORY, and MERGE).

        :param straight_join `<'bool'>`: If `True`, forces the optimizer to join the tables
            in the order in which they are listed in the FROM clause. Defaults to `False`.
            Can be used to speed up a query if the optimizer joins the tables in nonoptimal
            order.

        :param sql_buffer_result `<'bool'>`: If `True`, forces the result to be put into a
            temporary table. Defaults to `False`. This helps MySQL free the table locks early
            and helps in cases where it takes a long time to send the result set to the client.
            This modifier can be used only for top-level SELECT statements, not for subqueries
            or following UNION.

        :returns `<'SelectDML'>`: The DML Select statement.

        ## Example
        - Please refer to the <'SelectDML'> statment `Select()` method.
        """
        dml: SelectDML = SelectDML(self._db_name, self._pool)._Select(
            expressions,
            distinct,
            high_priority,
            straight_join,
            sql_buffer_result,
        )
        dml._with_clauses = self._with_clauses
        return dml

    # . update
    def Update(
        self,
        table: object,
        partition: object = None,
        ignore: cython.bint = False,
        low_priority: cython.bint = False,
        alias: object = None,
    ) -> UpdateDML:
        """The UPDATE clause of the UPDATE statement `<'UpdateDML'>`.

        :param table `<'str/Table'>`: The table (reference) from which to update data.
            Only accepts one table. For multiple-table, please use the explicit JOIN clause instead.

        :param partition `<'str/Partition/list/tuple/None'>`: The partition(s) of the table. Defaults to `None`.

        :param ignore `<'bool'>`: Whether to ignore the (ignorable) errors. Defaults to `False`.
            When 'ignore=True', the update statement does not abort even if errors occur during
            the update. Rows for which duplicate-key conflicts occur on a unique key value are
            not updated. Rows updated to values that would cause data conversion errors are
            updated to the closest valid values instead.

        :param low_priority `<'bool'>`: Whether to enable the optional `LOW_PRIORITY` modifier. Defaults to `False`.
            When 'low_priority=True', execution of the UPDATE is delayed until no other
            clients are reading from the table. This affects only storage engines that
            use only table-level locking (such as MyISAM, MEMORY, and MERGE).

        :param alias `<'str/None'>`: The alias of the table (reference). Defaults to `None`.
            When 'alias=None', a default alias is automatically generated based on the count
            of the tables adopting default alias in the same query block (starting from 't0').

        :returns `<'UpdateDML'>`: The DML Update statement.

        ## Notice
        - To set index hints, chain the `UseIndex()`, `IgnoreIndex()` and
          `ForceIndex()` methods after Update().

        ## Example
        - Please refer to the <'UpdateDML'> statment `Update()` method.
        """
        dml: UpdateDML = UpdateDML(self._db_name, self._pool).Update(
            table,
            partition=partition,
            ignore=ignore,
            low_priority=low_priority,
            alias=alias,
        )
        dml._with_clauses = self._with_clauses
        return dml

    # . delete
    def Delete(
        self,
        table: object,
        partition: object = None,
        ignore: cython.bint = False,
        low_priority: cython.bint = False,
        quick: cython.bint = False,
        alias: object = None,
        multi_tables: object = None,
    ) -> DeleteDML:
        """The DELETE clause of the DELETE statement `<'DeleteDML'>`.

        :param table `<'str/Table'>`: The table (reference) from which to delete data.
            Only accepts one table. For multiple-table, please use the explicit JOIN clause instead.

        :param partition `<'str/Partition/list/tuple/None'>`: The partition(s) of the table. Defaults to `None`.

        :param ignore `<'bool'>`: Whether to ignore the (ignorable) errors. Defaults to `False`.
            When 'ignore=True', causes MySQL to ignore errors during the process of deleting rows.

        :param low_priority `<'bool'>`: Whether to enable the optional `LOW_PRIORITY` modifier. Defaults to `False`.
            When 'low_priority=True', the server delays execution of the DELETE until no
            other clients are reading from the table. This affects only storage engines
            that use only table-level locking (such as MyISAM, MEMORY, and MERGE).

        :param quick `<'bool'>`: Whether to enable the optional `QUICK` modifier. Defaults to `False`.
            When 'quick=True', MyISAM storage engine does not merge index leaves during
            delete, which may speed up some kinds of delete operations.

        :param alias `<'str/None'>`: The alias of the table (reference). Defaults to `None`.
            When 'alias=None', a default alias is automatically generated based on the count
            of the tables adopting default alias in the same query block (starting from 't0').

        :param multi_tables `<'str/list/tuple/None'>`: The the table alias(es) for multi-table delete. Defaults to `None`.
            This argument should be used in combination with the `JOIN` clauses. Only
            the data of the table(s) specified in this argument will be deleted for
            multi-table DELETE operation when the condition is met.

        :returns `<'DeleteDML'>`: The DML Delete statement.

        ## Notice
        - To set index hints, chain the `UseIndex()`, `IgnoreIndex()` and
          `ForceIndex()` methods after Delete(). Only applicable to
          multi-table delete statement.

        ## Example
        - Please refer to the <'DeleteDML'> statment `Delete()` method.
        """
        dml: DeleteDML = DeleteDML(self._db_name, self._pool).Delete(
            table,
            partition=partition,
            ignore=ignore,
            low_priority=low_priority,
            quick=quick,
            alias=alias,
            multi_tables=multi_tables,
        )
        dml._with_clauses = self._with_clauses
        return dml
