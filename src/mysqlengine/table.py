# cython: language_level=3
# cython: wraparound=False
# cython: boundscheck=False
from __future__ import annotations

# Cython imports
import cython
from cython.cimports.cpython import datetime  # type: ignore
from cython.cimports.cpython.time import time as unix_time  # type: ignore
from cython.cimports.cpython.tuple import PyTuple_GET_SIZE as tuple_len  # type: ignore
from cython.cimports.cpython.dict import PyDict_SetItem as dict_setitem  # type: ignore
from cython.cimports.cpython.dict import PyDict_Contains as dict_contains  # type: ignore
from cython.cimports.cpython.unicode import PyUnicode_GET_LENGTH as str_len  # type: ignore
from cython.cimports.cpython.unicode import PyUnicode_READ_CHAR as str_read  # type: ignore
from cython.cimports.cpython.unicode import PyUnicode_Substring as str_substr  # type: ignore
from cython.cimports.cytimes.pydt import _Pydt  # type: ignore
from cython.cimports.sqlcycli.charset import Charset  # type: ignore
from cython.cimports.sqlcycli.aio.pool import PoolConnection, PoolSyncConnection  # type: ignore
from cython.cimports.mysqlengine.element import Element, Elements, Logs, Metadata, Query  # type: ignore
from cython.cimports.mysqlengine.index import Index, Indexes  # type: ignore
from cython.cimports.mysqlengine.column import Column, Columns  # type: ignore
from cython.cimports.mysqlengine.partition import Partition, Partitioning  # type: ignore
from cython.cimports.mysqlengine.constraint import Constraint, Constraints, UniqueKey  # type: ignore
from cython.cimports.mysqlengine.dml import SelectDML, InsertDML, ReplaceDML, UpdateDML, DeleteDML  # type: ignore
from cython.cimports.mysqlengine import utils  # type: ignore

datetime.import_datetime()

# Python imports
import datetime
from typing import Iterator
from asyncio import gather as _aio_gather
from cytimes.pydt import _Pydt, Pydt
from sqlcycli import sqlfunc
from sqlcycli.charset import Charset
from sqlcycli import errors as sqlerrors, DictCursor
from sqlcycli.aio import DictCursor as AioDictCursor
from sqlcycli.aio.pool import Pool, PoolConnection, PoolSyncConnection
from mysqlengine.index import Index, Indexes
from mysqlengine.column import Column, Columns, Define
from mysqlengine.partition import Partition, Partitioning
from mysqlengine.constraint import Constraint, Constraints, UniqueKey
from mysqlengine.dml import SelectDML, InsertDML, ReplaceDML, UpdateDML, DeleteDML
from mysqlengine.element import Element, Elements, Logs, Metadata, Query
from mysqlengine import utils, errors


__all__ = [
    "Table",
    "TimeTable",
    "TempTable",
    "TempTableManager",
    "Tables",
    "TableMetadata",
]


# BaseTable --------------------------------------------------------------------------------------------------
@cython.cclass
class BaseTable(Element):
    """The base class for a table in a database."""

    # . options
    _engine: str
    _comment: str
    _encryption: cython.int
    _row_format: str
    _partitioned: cython.int
    # . internal
    _columns: Columns
    _indexes: Indexes
    _constraints: Constraints
    _partitioning: Partitioning
    _temporary: cython.bint
    _sync_conn: PoolSyncConnection
    _async_conn: PoolConnection
    _setup_finished: cython.bint

    def __init__(
        self,
        engine: str = None,
        charset: object | None = None,
        collate: object | None = None,
        comment: str = None,
        encryption: bool | None = None,
        row_format: object = None,
    ):
        """The table in a database.

        :param engine `<'str/None'>`: The storage ENGINE of the table. Defaults to `None`.
        :param charset `<'str/Charset/None'>`: The CHARACTER SET of the table. Defaults to `None`.
            If not specified (None), use the charset of the database.
        :param collate `<'str/None'>`: The COLLATION of the table. Defaults to `None`.
            If not specified (None), use the collate of the database.
        :param comment `<'str/None'>`: The COMMENT of the table. Defaults to `None`.
        :param encryption `<'bool/None'>`: The table ENCRYPTION behavior. Defaults to `None`.
            - **None**: use the encryption setting of the database.
            - **True/False**: enabled/disable per table encryption.
        :param row_format `<'str/None'>`: The physical format in which the rows are stored. Defaults to `None`.
            Accepts: `"COMPACT"`, `"COMPRESSED"`, `"DYNAMIC"`, `"FIXED"`, `"REDUNDANT"`, `"PAGED"`.
        """
        super().__init__("TABLE", "TABLE")
        # . options
        self._engine = self._validate_engine(engine)
        self._charset = self._validate_charset(charset, collate)
        self._comment = self._validate_comment(comment)
        self._encryption = self._validate_encryption(encryption)
        self._row_format = self._validate_row_format(row_format)
        self._partitioned = -1
        # . internal
        self._columns = None
        self._indexes = None
        self._constraints = None
        self._partitioning = None
        self._temporary = False
        self._sync_conn = None
        self._async_conn = None
        self._setup_finished = False

    # Property -----------------------------------------------------------------------------
    @property
    def db_name(self) -> str:
        """The database name of the table `<'str'>`."""
        self._assure_ready()
        return self._db_name

    @property
    def tb_name(self) -> str:
        """The name of the table `<'str'>`."""
        self._assure_ready()
        return self._name

    @property
    def tb_qualified_name(self) -> str:
        """The qualified table name '{db_name}.{tb_name}' `<'str'>`."""
        self._assure_ready()
        return self._tb_qualified_name

    # . columns
    @property
    def columns(self) -> Columns:
        """The column collection of the table `<'Columns'>`."""
        self._assure_ready()
        return self._columns

    # . indexes
    @property
    def indexes(self) -> Indexes:
        """The index collection of the table `<'Indexes'>`."""
        self._assure_ready()
        return self._indexes

    # . constraints
    @property
    def constraints(self) -> Constraints:
        """The constraint collection of the table `<'Constraints'>`."""
        self._assure_ready()
        return self._constraints

    # . options
    @property
    def engine(self) -> str | None:
        """The storage ENGINE of the table `<'str/None'>`."""
        return self._engine

    @property
    def comment(self) -> str | None:
        """The COMMENT of the table `<'str/None'>`."""
        return self._comment

    @property
    def row_format(self) -> str | None:
        """The physical format in which the rows are stored `<'str/None'>`."""
        return self._row_format

    # DML ----------------------------------------------------------------------------------
    def Select(
        self,
        *expressions: object,
        partition: object = None,
        distinct: cython.bint = False,
        high_priority: cython.bint = False,
        straight_join: cython.bint = False,
        sql_buffer_result: cython.bint = False,
        alias: object = None,
    ) -> SelectDML:
        """Construct a SELECT statement of the table `<'SelectDML'>`.

        :param expressions `<'*str/SQLFunction/Column'>`: The (expression of) the column(s) to retrieve.

        :param partition `<'str/Partition/list/tuple/None'>`: The partition(s) of the table. Defaults to `None`.

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

        :param alias `<'str/None'>`: The alias of the table (reference). Defaults to `None`.
            When 'alias=None', a default alias is automatically generated based on the count
            of the tables adopting default alias in the same query block (starting from 't0').

        :returns `<'SelectDML'>`: The DML Select statement.

        ## Notice
        - Calling `db.tb.Select(...)` is equivalent to `db.Select(...).From(db.tb, ...)`.
        - Do not chain the `From()` method after `Select()`.
        - To set index hints, chain the `UseIndex()`, `IgnoreIndex()` and
          `ForceIndex()` methods after `Select()`.

        At the end of the chain:
        - Call `statement()` method to compose the statement <'str'>.
        - Call `Execute()` method to execute the statement synchronously.
        - Call `aioExecute()` method to execute the statement asynchronously.

        ## Example [sync]
        >>> data = (
                db.tb1.Select("t0.id", "t0.name", "COUNT(*) AS count")
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

        ## Example [async]
        >>> data = await db.tb.Select("*").aioExecute()
            # Equivalent to:
            SELECT * FROM db.tb;
        """
        self._assure_ready()
        dml: SelectDML = (
            SelectDML(self._db_name, self._pool)
            ._Select(
                expressions,
                distinct,
                high_priority,
                straight_join,
                sql_buffer_result,
            )
            .From(self, partition, alias)
        )
        if self._temporary:
            dml._set_connection(self._sync_conn, self._async_conn)
        return dml

    @cython.ccall
    def Insert(
        self,
        partition: object = None,
        ignore: cython.bint = False,
        priority: object = None,
    ) -> InsertDML:
        """Construct an INSERT statement of the table `<'InsertDML'>`.

        :param partition `<'str/Partition/list/tuple/None'>`: The partition(s) of the table to insert the data. Defaults to `None`.

        :param ignore `<'bool'>`: Whether to ignore the ignorable errors that occurs while executing the statement. Defaults to `False`.
            With 'ignore=True', ignorable errors—such as duplicate-key or primary-key violations,
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

        ## Notice
        - Calling `db.tb.Insert(...)` is equivalent to `db.Insert(db.tb, ...)`.

        At the end of the chain:
        - Call `statement()` method to compose the statement <'str'>.
        - Call `Execute()` method to execute the statement synchronously.
        - Call `aioExecute()` method to execute the statement asynchronously.

        ## Example [sync] (VALUES)
        >>> (
                db.tb.Insert()
                .Columns("id", "name")
                .Values(2)
                .OnDuplicate("name=VALUES(name)")
                .Execute([1, "John"], many=False)
            )
            # Equivalent to:
            INSERT INTO db.tb (id, name)
            VALUES (1,'John')
            ON DUPLICATE KEY UPDATE name=VALUES(name)

        ## Example [sync] (VALUES - ROW ALIAS)
        >>> (
                db.tb.Insert()
                .Columns("id", "name")
                .Values(2)
                .RowAlias("new")
                .OnDuplicate("name=new.name")
                .Execute([(1, "John"), (2, "Sarah")], many=True)
            )
            # Equivalent to:
            INSERT INTO db.tb (id, name)
            VALUES (1,'John'),(2,'Sarah') AS new
            ON DUPLICATE KEY UPDATE name=new.name

        ## Example [async] (SET)
        >>> (
                await db.tb.Insert()
                .Set("id=%s", "name=%s")
                .RowAlias("new", "i", "n")
                .OnDuplicate("name=n")
                .aioExecute([1, "John"], many=False)
            )
            # Equivalent to:
            INSERT INTO db.tb
            SET id=1, name='John'
            AS new (i, n)
            ON DUPLICATE KEY UPDATE name=n

        ## Example [async] (SELECT)
        >>> (
                await db.tb1.Insert()
                .Columns("id", "name")
                .Select("id", "name")
                .From(db.tb2)
                .OnDuplicate("name=t0.name")
                .aioExecute(many=False)
            )
            # Equivalent to:
            INSERT INTO db.tb1 (id, name)
            SELECT id, name FROM db.tb2 AS t0
            ON DUPLICATE KEY UPDATE name=t0.name
        """
        self._assure_ready()
        dml: InsertDML = InsertDML(self._db_name, self._pool).Insert(
            self, partition, ignore, priority
        )
        if self._temporary:
            dml._set_connection(self._sync_conn, self._async_conn)
        return dml

    @cython.ccall
    def Replace(
        self,
        partition: object = None,
        low_priority: cython.bint = False,
    ) -> ReplaceDML:
        """Construct a REPLACE statement of the table `<'ReplaceDML'>`.

        REPLACE is a MySQL extension to the SQL standard and works exactly like
        INSERT, except that if an old row in the table has the same value as a
        new row for a PRIMARY KEY or a UNIQUE index, the old row is deleted
        before the new row is inserted.

        :param partition `<'str/Partition/list/tuple/None'>`: The partition(s) of the table to replace the data. Defaults to `None`.

        :param low_priority `<'bool'>`: Whether to enable the optional `LOW_PRIORITY` modifier. Defaults to `False`.
            `LOW_PRIORITY`: Delays the REPLACE until no other clients are reading the table
            (even those who start reading while your REPLACE is waiting). Disables concurrent
            inserts—so it can block for a very long time and is normally not recommended on
            MyISAM tables. Only applies to table-locking engines (MyISAM, MEMORY, MERGE).

        :returns `<'ReplaceDML'>`: The DML Replace statement.

        ## Notice
        - Calling `db.tb.Replace(...)` is equivalent to `db.Replace(db.tb, ...)`.

        At the end of the chain:
        - Call `statement()` method to compose the statement <'str'>.
        - Call `Execute()` method to execute the statement synchronously.
        - Call `aioExecute()` method to execute the statement asynchronously.

        ## Example [sync] (VALUES)
        >>> (
                db.tb.Replace()
                .Columns("id", "name")
                .Values(2)
                .Execute([(1, "John"), (2, "Sarah")], many=True)
            )
            # Equivalent to:
            REPLACE INTO db.tb (id, name)
            VALUES (1,'John'),(2,'Sarah')

        ## Example [async] (SET)
        >>> (
                await db.tb.Replace()
                .Set("id=%s", "name=%s")
                .aioExecute([1, "John"], many=False)
            )
            # Equivalent to:
            REPLACE INTO db.tb
            SET id=1, name='John'

        ## Example [async] (SELECT)
        >>> (
                await db.tb1.Replace()
                .Columns("id", "name")
                .Select("id", "name")
                .From(db.tb2)
                .aioExecute(many=False)
            )
            # Equivalent to:
            REPLACE INTO db.tb1 (id, name)
            SELECT id, name FROM db.tb2 AS t0
        """
        self._assure_ready()
        dml: ReplaceDML = ReplaceDML(self._db_name, self._pool).Replace(
            self, partition, low_priority
        )
        if self._temporary:
            dml._set_connection(self._sync_conn, self._async_conn)
        return dml

    @cython.ccall
    def Update(
        self,
        partition: object = None,
        ignore: cython.bint = False,
        low_priority: cython.bint = False,
        alias: object = None,
    ) -> UpdateDML:
        """Construct a UPDATE statement of the table `<'UpdateDML'>`.

        :param partition `<'str/Partition/list/tuple/None'>`: The partition(s) of the table to update. Defaults to `None`.

        :param ignore `<'bool'>`: Whether to ignore the (ignorable) errors. Defaults to `False`.
            With 'ignore=True', the update statement does not abort even if errors occur during
            the update. Rows for which duplicate-key conflicts occur on a unique key value are
            not updated. Rows updated to values that would cause data conversion errors are
            updated to the closest valid values instead.

        :param low_priority `<'bool'>`: Whether to enable the optional `LOW_PRIORITY` modifier. Defaults to `False`.
            With 'low_priority=True', execution of the UPDATE is delayed until no other
            clients are reading from the table. This affects only storage engines that
            use only table-level locking (such as MyISAM, MEMORY, and MERGE).

        :param alias `<'str/None'>`: The alias of the table (reference). Defaults to `None`.
            When 'alias=None', a default alias is automatically generated based on the count
            of the tables adopting default alias in the same query block (starting from 't0').

        :returns `<'UpdateDML'>`: The DML Update statement.

        ## Notice
        - Calling `db.tb.Update(...)` is equivalent to `db.Update(db.tb, ...)`.
        - To set index hints, chain the `UseIndex()`, `IgnoreIndex()` and
          `ForceIndex()` methods after `Update()`.

        At the end of the chain:
        - Call `statement()` method to compose the statement <'str'>.
        - Call `Execute()` method to execute the statement synchronously.
        - Call `aioExecute()` method to execute the statement asynchronously.

        ## Example [async] (single-table)
        >>> (
                await db.tb.Update()
                .Set("first_name=%s")
                .Where("id=%s")
                .Execute([("John", 1, "Sarah", 2)], many=True)
            )
            # Equivalent to (concurrent):
            UPDATE db.tb AS t0
            SET first_name='John'
            WHERE id=1;
            UPDATE db.tb AS t0
            SET first_name='Sarah'
            WHERE id=2;

        ## Example [sync] (multi-table)
        >>> (
                db.tb1.Update()
                .Join(db.tb2, "t0.id = t1.id")
                .Set("t0.first_name=t1.first_name")
                .Where("t0.id=%s")
                .Execute(1, many=False)
            )
            # Equivalent to:
            UPDATE db.tb1 AS t0
            INNER JOIN db.tb2 AS t1
                ON t0.id = t1.id
            SET t0.first_name=t1.first_name
            WHERE t0.id=1;
        """
        self._assure_ready()
        dml: UpdateDML = UpdateDML(self._db_name, self._pool).Update(
            self, partition, ignore, low_priority, alias
        )
        if self._temporary:
            dml._set_connection(self._sync_conn, self._async_conn)
        return dml

    @cython.ccall
    def Delete(
        self,
        partition: object = None,
        ignore: cython.bint = False,
        low_priority: cython.bint = False,
        quick: cython.bint = False,
        alias: object = None,
        multi_tables: object = None,
    ) -> DeleteDML:
        """Construct a DELETE statement of the table `<'DeleteDML'>`.

        :param partition `<'str/Partition/list/tuple/None'>`: The partition(s) of the table. Defaults to `None`.

        :param ignore `<'bool'>`: Whether to ignore the (ignorable) errors. Defaults to `False`.
            With 'ignore=True', causes MySQL to ignore errors during the process of deleting rows.

        :param low_priority `<'bool'>`: Whether to enable the optional `LOW_PRIORITY` modifier. Defaults to `False`.
            With 'low_priority=True', the server delays execution of the DELETE until no
            other clients are reading from the table. This affects only storage engines
            that use only table-level locking (such as MyISAM, MEMORY, and MERGE).

        :param quick `<'bool'>`: Whether to enable the optional `QUICK` modifier. Defaults to `False`.
            With 'quick=True', MyISAM storage engine does not merge index leaves during
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
        - Calling `db.tb.Delete(...)` is equivalent to `db.Delete(db.tb, ...)`.
        - To set index hints, chain the `UseIndex()`, `IgnoreIndex()` and
          `ForceIndex()` methods after `Delete()`. This is only applicable
          to multi-table delete statement.

        At the end of the chain:
        - Call `statement()` method to compose the statement <'str'>.
        - Call `Execute()` method to execute the statement synchronously.
        - Call `aioExecute()` method to execute the statement asynchronously.

        ## Example [sync] (single-table)
        >>> (
                db.tb.Delete()
                .Where("date BETWEEN %s AND %s")
                .OrderBy("id")
                .Limit(10)
                .Execute([date(2023, 1, 1), date(2023, 1, 31)])
            )
            # Equivalent to:
            DELETE FROM db.tb AS t0
            WHERE date BETWEEN '2023-01-01' AND '2023-01-31'
            ORDER BY id
            LIMIT 10;

        ## Example [async] (multi-table)
        >>> (
                await db.tb1.Delete(multi_tables=["t0", "t1"])
                .Join(db.tb2, "t0.id = t1.id")
                .Where("t0.id=%s")
                .Execute([1, 2], many=True)
            )
            # Equivalent to:
            DELETE t0, t1 FROM db.tb1 AS t0
            INNER JOIN db.tb2 AS t1
                ON t0.id = t1.id
            WHERE t0.id=1;
            DELETE t0, t1 FROM db.tb1 AS t0
            INNER JOIN db.tb2 AS t1
                ON t0.id = t1.id
            WHERE t0.id=2;
        """
        self._assure_ready()
        dml: DeleteDML = DeleteDML(self._db_name, self._pool).Delete(
            self, partition, ignore, low_priority, quick, alias, multi_tables
        )
        if self._temporary:
            dml._set_connection(self._sync_conn, self._async_conn)
        return dml

    # Generate SQL -------------------------------------------------------------------------
    @cython.ccall
    def _gen_create_sql(self, if_not_exists: cython.bint) -> str:
        """(internal) Generate SQL to create the table `<'str'>`."""
        self._assure_ready()
        # Create SQL
        elements = [self._columns._gen_definition_sql(1)]
        if self._indexes:
            elements.append(self._indexes._gen_definition_sql(1))
        if self._constraints:
            elements.append(self._constraints._gen_definition_sql(1))
        # fmt: off
        definition: str = ",\n".join(elements)
        if self._temporary:
            if if_not_exists:
                sql: str = "CREATE TEMPORARY TABLE IF NOT EXISTS %s (\n%s\n)" % (
                    self._tb_qualified_name, definition
                )
            else:
                sql: str = "CREATE TEMPORARY TABLE %s (\n%s\n)" % (
                    self._tb_qualified_name, definition
                )
        elif if_not_exists:
            sql: str = "CREATE TABLE IF NOT EXISTS %s (\n%s\n)" % (
                self._tb_qualified_name, definition
            )
        else:
            sql: str = "CREATE TABLE %s (\n%s\n)" % (
                self._tb_qualified_name, definition
            )
        # fmt: on

        # Options
        if self._engine is not None:
            sql += "\nENGINE %s" % self._engine
        if self._charset is not None:
            sql += "\nCHARACTER SET %s" % self._charset._name
            sql += "\nCOLLATE %s" % self._charset._collation
        if self._comment is not None:
            sql += self._format_sql("\nCOMMENT %s", self._comment)
        if self._encryption == 1:
            sql += "\nENCRYPTION 'Y'"
        elif self._encryption == 0:
            sql += "\nENCRYPTION 'N'"
        if self._row_format is not None:
            sql += "\nROW_FORMAT %s" % self._row_format

        # Partitioning
        if self._partitioning is not None:
            sql += "\n%s" % self._partitioning._gen_definition_sql()

        # Compose SQL
        return sql + ";"

    @cython.ccall
    def _gen_exists_sql(self) -> str:
        """(internal) Generate SQL to check if the table exists `<'str'>`."""
        self._assure_ready()
        return (
            "SELECT 1 "
            "FROM INFORMATION_SCHEMA.TABLES "
            "WHERE TABLE_NAME = '%s' "
            "AND TABLE_SCHEMA = '%s' "
            "AND TABLE_TYPE = 'BASE TABLE' "
            "LIMIT 1;" % (self._tb_name, self._db_name)
        )

    @cython.ccall
    def _gen_truncate_sql(self) -> str:
        """(internal) Generate SQL to truncate the table `<'str'>`."""
        self._assure_ready()
        return "TRUNCATE %s;" % self._tb_qualified_name

    @cython.ccall
    def _gen_drop_sql(self, if_exists: cython.bint) -> str:
        """(internal) Generate SQL to drop the table `<'str'>`."""
        self._assure_ready()
        if self._temporary:
            if if_exists:
                return "DROP TEMPORARY TABLE IF EXISTS %s;" % self._tb_qualified_name
            else:
                return "DROP TEMPORARY TABLE %s;" % self._tb_qualified_name
        elif if_exists:
            return "DROP TABLE IF EXISTS %s;" % self._tb_qualified_name
        else:
            return "DROP TABLE %s;" % self._tb_qualified_name

    @cython.ccall
    def _gen_empty_sql(self) -> str:
        """(internal) Generate SQL to check if the table is empty `<'str'>."""
        self._assure_ready()
        return "SELECT 1 FROM %s LIMIT 1;" % self._tb_qualified_name

    @cython.ccall
    def _gen_alter_query(
        self,
        meta: TableMetadata,
        engine: object | None,
        charset: object | None,
        collate: object | None,
        comment: object | None,
        encryption: bool | None,
        row_format: object | None,
    ) -> Query:
        """(interal) Generate the query to alter the table `<'Query'>`."""
        self._assure_ready()
        query = Query()
        altered: cython.bint = False
        sql: str = "ALTER TABLE " + self._tb_qualified_name

        # Engine
        _engine: str = self._validate_engine(engine)
        if _engine is not None and _engine != meta._engine:
            sql += " ENGINE %s" % _engine
            altered = True  # set flag

        # Charset
        _charset = self._validate_charset(charset, collate)
        if _charset is not None and _charset is not meta._charset:
            _charset = self._validate_encoding(_charset)
            sql += " CHARACTER SET %s COLLATE %s" % (
                _charset._name,
                _charset._collation,
            )
            altered = True  # set flag

        # Comment
        if (
            comment is not None
            and (comment := self._validate_comment(comment)) != meta._comment
        ):
            if comment is None:
                sql += " COMMENT ''"
            else:
                sql += self._format_sql(" COMMENT %s", comment)
            altered = True  # set flag

        # Encryption
        _encrypt = self._validate_encryption(encryption)
        if _encrypt != -1 and _encrypt != meta._encryption:
            sql += " ENCRYPTION 'Y'" if _encrypt == 1 else " ENCRYPTION 'N'"
            altered = True  # set flag

        # Row Format
        _row_format: str = self._validate_row_format(row_format)
        if _row_format is not None and _row_format != meta._row_format:
            sql += " ROW_FORMAT %s" % _row_format
            altered = True  # set flag

        # Compose & Set SQL
        if altered:
            query.set_sql(self, sql + ";")
        return query

    @cython.ccall
    def _gen_show_metadata_sql(self) -> str:
        """(internal) Generate SQL to show table metadata `<'str'>`."""
        self._assure_ready()
        return (
            "SELECT "
            # . columns [t1]
            "t1.TABLE_CATALOG AS CATALOG_NAME, "
            "t1.TABLE_SCHEMA AS SCHEMA_NAME, "
            "t1.TABLE_NAME AS TABLE_NAME, "
            "t1.TABLE_TYPE AS TABLE_TYPE, "
            "t1.ENGINE AS ENGINE, "
            "t1.VERSION AS VERSION, "
            "t1.ROW_FORMAT AS ROW_FORMAT, "
            "t1.TABLE_ROWS AS TABLE_ROWS, "
            "t1.AVG_ROW_LENGTH AS AVG_ROW_LENGTH, "
            "t1.DATA_LENGTH AS DATA_LENGTH, "
            "t1.MAX_DATA_LENGTH AS MAX_DATA_LENGTH, "
            "t1.INDEX_LENGTH AS INDEX_LENGTH, "
            "t1.DATA_FREE AS DATA_FREE, "
            "t1.AUTO_INCREMENT AS AUTO_INCREMENT, "
            "t1.CREATE_TIME AS CREATE_TIME, "
            "t1.UPDATE_TIME AS UPDATE_TIME, "
            "t1.CHECK_TIME AS CHECK_TIME, "
            "t1.TABLE_COLLATION AS TABLE_COLLATION, "
            "t1.CHECKSUM AS CHECKSUM, "
            "UPPER(t1.CREATE_OPTIONS) AS CREATE_OPTIONS, "
            "t1.TABLE_COMMENT AS TABLE_COMMENT, "
            # . columns [t2]
            "t2.ENGINE_ATTRIBUTE AS ENGINE_ATTRIBUTE, "
            "t2.SECONDARY_ENGINE_ATTRIBUTE AS SECONDARY_ENGINE_ATTRIBUTE "
            # . information_schema.tables
            "FROM INFORMATION_SCHEMA.TABLES AS t1 "
            # . information_schema.tables_extensions
            "LEFT JOIN INFORMATION_SCHEMA.TABLES_EXTENSIONS AS t2 "
            "ON t1.TABLE_NAME = t2.TABLE_NAME "
            "AND t1.TABLE_SCHEMA = t2.TABLE_SCHEMA "
            # . conditions
            "WHERE t1.TABLE_NAME = '%s' "
            "AND t1.TABLE_SCHEMA = '%s' "
            "AND t1.TABLE_TYPE = 'BASE TABLE';" % (self._tb_name, self._db_name)
        )

    @cython.ccall
    def _gen_show_column_names_sql(self) -> str:
        """(internal) Generate SQL to select all the column
        names of the table (sorted by ordinal position) `<'str'>`.
        """
        self._assure_ready()
        return (
            "SELECT COLUMN_NAME AS i "
            "FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_NAME = '%s' "
            "AND TABLE_SCHEMA = '%s' "
            "ORDER BY ORDINAL_POSITION ASC;" % (self._tb_name, self._db_name)
        )

    @cython.ccall
    def _gen_show_index_names_sql(self) -> str:
        """(internal) Generate SQL to select all the index names of the table `<'str'>`."""
        self._assure_ready()
        return (
            "SELECT INDEX_NAME AS i "
            "FROM INFORMATION_SCHEMA.STATISTICS "
            "WHERE TABLE_NAME = '%s' "
            "AND INDEX_SCHEMA = '%s' "
            "AND INDEX_SCHEMA = TABLE_SCHEMA "
            "AND SEQ_IN_INDEX = 1;" % (self._tb_name, self._db_name)
        )

    @cython.ccall
    def _gen_show_constraint_symbols_sql(self) -> str:
        """(internal) Generate SQL to select all the constraint names of the table `<'str'>`."""
        self._assure_ready()
        return (
            "SELECT "
            "CONSTRAINT_NAME AS i "
            "FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS "
            "WHERE TABLE_NAME = '%s' "
            "AND CONSTRAINT_SCHEMA = '%s' "
            "AND CONSTRAINT_SCHEMA = TABLE_SCHEMA;" % (self._tb_name, self._db_name)
        )

    @cython.ccall
    def _gen_lock_sql(self, lock_for_read: cython.bint) -> str:
        """(internal) Generate SQL to lock the table `<'str'>`."""
        self._assure_ready()
        if lock_for_read:
            return "LOCK TABLES %s READ;" % self._tb_qualified_name
        else:
            return "LOCK TABLES %s WRITE;" % self._tb_qualified_name

    @cython.ccall
    def _gen_analyze_sql(self, write_to_binlog: cython.bint) -> str:
        """(internal) Generate SQL to analyze the table `<'str'>`."""
        self._assure_ready()
        if not write_to_binlog:
            return "ANALYZE TABLE %s;" % self._tb_qualified_name
        else:
            return "ANALYZE NO_WRITE_TO_BINLOG TABLE %s;" % self._tb_qualified_name

    @cython.ccall
    def _gen_check_sql(self, options: tuple) -> str:
        """(internal) Generate SQL to check the table `<'str'>`."""
        self._assure_ready()
        opts: list = []
        opt_count: cython.uint = 0
        for i in options:
            opt = self._validate_check_option(i)
            if opt is not None:
                opts.append(opt)
                opt_count += 1
        if opt_count == 0:
            return "CHECK TABLE %s;" % self._tb_qualified_name
        else:
            return "CHECK TABLE %s %s;" % (self._tb_qualified_name, " ".join(opts))

    @cython.ccall
    def _gen_optimize_sql(self, write_to_binlog: cython.bint) -> str:
        """(internal) Generate SQL to optimize the table `<'str'>`."""
        self._assure_ready()
        if not write_to_binlog:
            return "OPTIMIZE TABLE %s;" % self._tb_qualified_name
        else:
            return "OPTIMIZE NO_WRITE_TO_BINLOG TABLE %s;" % self._tb_qualified_name

    @cython.ccall
    def _gen_repair_sql(self, write_to_binlog: cython.bint, option: object) -> str:
        """(internal) Generate SQL to repair the table `<'str'>`."""
        self._assure_ready()
        opt = self._validate_repair_option(option)
        if opt is None:
            if not write_to_binlog:
                return "REPAIR TABLE %s;" % self._tb_qualified_name
            return "REPAIR NO_WRITE_TO_BINLOG TABLE %s;" % self._tb_qualified_name
        if not write_to_binlog:
            return "REPAIR TABLE %s %s;" % (self._tb_qualified_name, opt)
        return "REPAIR NO_WRITE_TO_BINLOG TABLE %s %s;" % (self._tb_qualified_name, opt)

    # Metadata -----------------------------------------------------------------------------
    @cython.ccall
    def _sync_from_metadata(self, meta: TableMetadata, logs: Logs = None) -> Logs:
        """(internal) Synchronize local configs with the remote table metadata `<'Logs'>`."""
        self._assure_ready()
        if logs is None:
            logs = Logs()

        # Validate
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

        # Engine
        if self._engine != meta._engine:
            logs.log_config_obj(self, "engine", self._engine, meta._engine)
            self._engine = meta._engine

        # Charset
        if self._charset is not meta._charset:
            charset = self._validate_encoding(meta._charset)
            logs.log_charset(self, self._charset, charset)
            self._charset = charset

        # Comment
        if self._comment != meta._comment:
            logs.log_config_obj(self, "comment", self._comment, meta._comment)
            self._comment = meta._comment

        # Encryption
        if self._encryption != meta._encryption:
            logs.log_config_bool(self, "encryption", self._encryption, meta._encryption)
            self._encryption = meta._encryption

        # Row Format
        if self._row_format != meta._row_format:
            logs.log_config_obj(self, "row_format", self._row_format, meta._row_format)
            self._row_format = meta._row_format

        # Partitioned
        if self._partitioned != meta._partitioned:
            if meta._partitioned and self._partitioning is None:
                self._warn(
                    "is partitioned on the remote server, but local does "
                    "not have a partitioning configuration."
                )
            logs.log_config_bool(
                self, "partitioned", self._partitioned, meta._partitioned
            )
            self._partitioned = meta._partitioned

        # Return Logs
        return logs

    # Setter -------------------------------------------------------------------------------
    @cython.ccall
    @cython.exceptval(-1, check=False)
    def setup(
        self,
        db_name: str,
        charset: str | Charset,
        collate: str | None,
        pool: Pool,
    ) -> cython.bint:
        """Setup the table.

        :param db_name `<'str'>`: The database name of the table.
        :param charset `<'str/Charset'>`: The charset of the table.
        :param collate `<'str/None'>`: The collation of the table.
        :param pool `<'Pool'>`: The pool of the table.
        """
        # Setup settings
        self._set_db_name(db_name)
        self._set_charset(charset, collate)
        self._set_pool(pool)
        self._assure_setup_ready()

        # Setup elements
        cols, idxs, cnsts = [], [], []
        try:
            _annotations: dict = self.__annotations__
        except AttributeError:
            _annotations: dict = {}
        # -------------------------------
        tb_name = self._tb_name
        db_name = self._db_name
        charset = self._charset
        pool = self._pool
        # -------------------------------
        name: str
        for name in _annotations.keys():
            try:
                obj = getattr(self, name)
            except Exception:
                continue
            if not isinstance(obj, Element):
                continue
            el: Element = obj
            # . column
            if isinstance(el, Column):
                el = el.copy()
                el.set_name(name)
                cols.append(el)
            # . index
            elif isinstance(el, Index):
                el = el.copy()
                el.set_name(name)
                idxs.append(el)
            # . constraint
            elif isinstance(el, Constraint):
                el = el.copy()
                el.set_name(name)
                cnsts.append(el)
            # . partitioning
            elif isinstance(el, Partitioning):
                if self._temporary:
                    self._raise_definition_error(
                        "cannot have partitioning configuration for a TEMPORARY TABLE."
                    )
                if self._partitioning is not None:
                    self._raise_definition_error(
                        "can only have one partitioning configuration, "
                        "instead got duplicate:\n%s\n%s." % (self._partitioning, el)
                    )
                el = el.copy()
                self._partitioning = el
                self._partitioning.setup(tb_name, db_name, charset, None, pool)
                self._partitioned = 1
            # . others
            else:
                self._raise_definition_error(
                    "is annotated with an unsupported element "
                    "('%s' %s)." % (name, type(el))
                )
            setattr(self, name, el)

        # Construct elements
        self._columns = Columns(*cols)
        if self._columns._size == 0:
            self._raise_definition_error("must have at least one column.")
        self._columns.setup(tb_name, db_name, charset, None, pool)
        self._indexes = Indexes(*idxs)
        self._indexes.setup(tb_name, db_name, charset, None, pool)
        self._constraints = Constraints(*cnsts)
        self._constraints.setup(tb_name, db_name, charset, None, pool)

        # Switch setup flag
        self._setup_finished = True
        return self._assure_ready()

    @cython.ccall
    @cython.exceptval(-1, check=False)
    def set_name(self, name: object) -> cython.bint:
        """Set the name of Table."""
        if Element.set_name(self, name):
            self._name = self._validate_table_name(name)
            self._tb_name = self._name
        return self._set_tb_qualified_name()

    # Assure Ready -------------------------------------------------------------------------
    @cython.ccall
    @cython.exceptval(-1, check=False)
    def _assure_ready(self) -> cython.bint:
        """(internal) Assure the table is ready."""
        if not self._el_ready:
            self._assure_setup_ready()
            if not self._setup_finished:
                self._raise_critical_error(
                    "has not been properly setup yet.\n"
                    "Please call the 'setup()' method to complete the configuration."
                )
            self._el_ready = True
        return True

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(-1, check=False)
    def _assure_setup_ready(self) -> cython.bint:
        """(internal) Assure the table is ready for the 'setup()' process."""
        self._assure_name_ready()
        self._assure_tb_name_ready()
        self._assure_db_name_ready()
        self._assure_encoding_ready()
        return True

    # Validate -----------------------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    def _validate_engine(self, engine: object) -> str:
        """(internal) Validate the storage ENGINE `<'str/None'>`."""
        try:
            return utils.validate_engine(engine)
        except Exception as err:
            self._raise_definition_error(str(err), err)

    @cython.cfunc
    @cython.inline(True)
    def _validate_row_format(self, row_format: object) -> str:
        """(internal) Validate the ROW_FORMAT `<'str/None'>`."""
        try:
            return utils.validate_row_format(row_format)
        except Exception as err:
            self._raise_definition_error(str(err), err)

    @cython.cfunc
    @cython.inline(True)
    def _validate_check_option(self, option: object) -> str:
        """(internal) Validate the CHECK TABLE option `<'str/None'>`."""
        try:
            return utils.validate_check_table_option(option)
        except Exception as err:
            self._raise_argument_error(str(err), err)

    @cython.cfunc
    @cython.inline(True)
    def _validate_repair_option(self, option: object) -> str:
        """(internal) Validate the REPAIR TABLE option `<'str/None'>`."""
        try:
            return utils.validate_repair_table_option(option)
        except Exception as err:
            self._raise_argument_error(str(err), err)

    # Internal -----------------------------------------------------------------------------
    @cython.ccall
    @cython.exceptval(-1, check=False)
    def _set_initialized(self, flag: cython.bint) -> cython.bint:
        """(internal) Set the initialized flag of the table."""
        # Self
        Element._set_initialized(self, flag)
        # Columns
        if self._columns is not None:
            self._columns._set_initialized(flag)
        # Indexes
        if self._indexes is not None:
            self._indexes._set_initialized(flag)
        # Constraints
        if self._constraints is not None:
            self._constraints._set_initialized(flag)
        # Partitioning
        if self._partitioning is not None:
            self._partitioning._set_initialized(flag)
        # Finished
        return True

    # Special Methods ----------------------------------------------------------------------
    def __getitem__(self, col: str | Column) -> Column:
        self._assure_ready()
        return self._columns[col]

    def __contains__(self, col: str | Column) -> bool:
        self._assure_ready()
        return col in self._columns

    def __iter__(self) -> Iterator[Column]:
        self._assure_ready()
        return iter(self._columns)

    def __repr__(self) -> str:
        self._assure_ready()
        # Reprs
        reprs = [
            "name=%r" % self._name,
            "db_name=%r" % self._db_name,
            "columns=%s" % self._columns,
        ]
        if self._indexes:
            reprs.append("indexes=%s" % self._indexes)
        if self._constraints:
            reprs.append("constraints=%s" % self._constraints)
        if self._partitioning is not None:
            reprs.append("partitioning=%s" % self._partitioning)
        if self._engine is not None:
            reprs.append("engine=%r" % self._engine)
        if self._charset is not None:
            reprs.append("charset=%r" % self._charset._name)
            reprs.append("collate=%r" % self._charset._collation)
        if self._comment is not None:
            reprs.append(self._format_sql("comment=%s", self._comment))
        if self._encryption != -1:
            reprs.append("encryption=%s" % utils.read_bool_config(self._encryption))
        if self._row_format is not None:
            reprs.append("row_format=%r" % self._row_format)

        # Compose
        return "<%s (\n\t%s\n)>" % (self._el_type.title(), ",\n\t".join(reprs))

    def __str__(self) -> str:
        self._assure_ready()
        return self._tb_qualified_name

    def __len__(self) -> int:
        self._assure_ready()
        return self._columns._size


# Prohibited names from BaseTable class
utils.SCHEMA_ELEMENT_SETTINGS.add_prohibited_names(dir(BaseTable()))


# Table ------------------------------------------------------------------------------------------------------
@cython.cclass
class Table(BaseTable):
    """Represents a table in a database."""

    # Property -----------------------------------------------------------------------------
    # . partitioning
    @property
    def partitioning(self) -> Partitioning | None:
        """The partitioning of the table `<'Partitioning/None'>`."""
        self._assure_ready()
        return self._partitioning

    @property
    def partitioned(self) -> bool | None:
        """Whether the table is partitioned `<'bool/None'>`."""
        return utils.read_bool_config(self._partitioned)

    # . options
    @property
    def encryption(self) -> bool | None:
        """The ENCRYPTION behavior of the table `<'bool/None'>`."""
        return utils.read_bool_config(self._encryption)

    # Sync ---------------------------------------------------------------------------------
    @cython.ccall
    def Initialize(self, force: cython.bint = False) -> Logs:
        """[sync] Initialize the table `<'Logs'>`.

        :param force `<'bool'>`: Whether to force the initialization even
            the table has already been initialized. Defaults to `False`.

        ## Explanation (difference from create)
        - 1. Create the table if not exists.
        - 2. Initialize all the columns.
        - 3. Initialize all the indexes (if exists).
        - 4. Initialize all the constraints (if exists).
        - 5. Initialize the partitioning (if exists).
        """
        logs: Logs = Logs()
        # Skip
        if self._initialized and not force:
            return logs
        # Initialize: table
        if not self.Exists():
            logs.extend(self.Create(True))
        else:
            logs.extend(self.SyncFromRemote())
        # Initialize: columns
        col: Column
        for col in self._columns._el_dict.values():
            logs.extend(col.Initialize(force))
        # Initialize: indexes
        idx: Index
        for idx in self._indexes._el_dict.values():
            logs.extend(idx.Initialize(force))
        # Initialize: constraints
        cnst: Constraint
        for cnst in self._constraints._el_dict.values():
            logs.extend(cnst.Initialize(force))
        # Initialize: partitioning
        if self._partitioning is not None:
            logs.extend(self._partitioning.Initialize(force))
        # Finished
        self._set_initialized(True)
        return logs

    @cython.ccall
    def Create(self, if_not_exists: cython.bint = False) -> Logs:
        """[sync] Create the table `<'Logs'>`.

        :param if_not_exists `<'bool'>`: Create the table only if it does not exist. Defaults to `False`.
        :raises `<'OperationalError'>`: If the table already exists and 'if_not_exists=False'.
        """
        # Execute creation
        sql: str = self._gen_create_sql(if_not_exists)
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
        """[sync] Check if the table exists `<'bool'>`."""
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
    def Truncate(self) -> Logs:
        """[sync] Truncate the table `<'Logs'>`.

        ## Explanation
        - Truncate operations drop and re-create the table, which is much faster than deleting
          rows one by one, particularly for large tables.
        - Truncate operations cause an implicit commit, and so cannot be rolled back.
        - Truncation operations cannot be performed if the session holds an active table lock.
        - TRUNCATE TABLE fails for an InnoDB table or NDB table if there are any
          FOREIGN KEY constraints from other tables that reference the table. Foreign key
          constraints between columns of the same table are permitted.
        - Truncation operations do not return a meaningful value for the number of deleted rows.
          The usual result is "0 rows affected", which should be interpreted as "no information."
        - As long as the table definition is valid, the table can be re-created as an empty table
          with TRUNCATE TABLE, even if the data or index files have become corrupted.
        - Any AUTO_INCREMENT value is reset to its start value. This is true even for MyISAM and
          InnoDB, which normally do not reuse sequence values.
        - When used with partitioned tables, TRUNCATE TABLE preserves the partitioning; that is,
          the data and index files are dropped and re-created, while the partition definitions
          are unaffected.
        - The TRUNCATE TABLE statement does not invoke ON DELETE triggers.
        - Truncating a corrupted InnoDB table is supported.
        """
        sql: str = self._gen_truncate_sql()
        with self._pool.acquire() as conn:
            with conn.transaction() as cur:
                cur.execute(sql)
        return Logs().log_sql(self, sql)

    @cython.ccall
    def Drop(self, if_exists: cython.bint = False) -> Logs:
        """[sync] Drop the table `<'Logs'>`.

        :param if_exists `<'bool'>`: Drop the table only if it exists. Defaults to `False`.
        :raises `<'OperationalError'>`: If the table does not exists and 'if_exists=False'.

        ## Explanation
        - DROP TABLE removes the table definition and all table data. If the table is partitioned,
          the statement removes the table definition, all its partitions, all data stored in those
          partitions, and all partition definitions associated with the dropped table.
        - Dropping a table also drops any triggers for the table.
        - DROP TABLE causes an implicit commit, except when used with the TEMPORARY keyword.
        """
        sql: str = self._gen_drop_sql(if_exists)
        with self._pool.acquire() as conn:
            with conn.transaction() as cur:
                cur.execute(sql)
        self._set_initialized(False)
        logs = Logs().log_sql(self, sql)
        return logs.log_element_deletion(self, False)

    @cython.ccall
    @cython.exceptval(-1, check=False)
    def Empty(self) -> cython.bint:
        """[sync] Check if the table is empty `<'bool'>`."""
        sql: str = self._gen_empty_sql()
        with self._pool.acquire() as conn:
            with conn.transaction() as cur:
                cur.execute(sql)
                res = cur.fetchone()
        return res is None

    @cython.ccall
    def Alter(
        self,
        engine: str = None,
        charset: object = None,
        collate: object = None,
        comment: str = None,
        encryption: bool | None = None,
        row_format: object = None,
    ) -> Logs:
        """[sync] Alter the table `<'Logs'>`.

        ## Notice
        - Default value `None` means to retain the corresponding settings of the table.

        :param engine `<'str'>`: The storage ENGINE of the table. Defaults to `None`.
        :param charset `<'str/Charset/None'>`: The CHARACTER SET of the table. Defaults to `None`.
        :param collate `<'str/None'>`: The COLLATION of the table. Defaults to `None`.
        :param comment `<'str/None'>`: The COMMENT of the table. Defaults to `None`.
            To remove existing comment, set `comment=''`.
        :param encryption `<'bool/None'>`: The table ENCRYPTION behavior. Defaults to `None`.
            Enable encryption if `True`, else `False` to disable.
        :param row_format `<'str/None'>`: The physical format in which the rows are stored. Defaults to `None`.
            Accepts: `"COMPACT"`, `"COMPRESSED"`, `"DYNAMIC"`, `"FIXED"`, `"REDUNDANT"`, `"PAGED"`.
        """
        # Generate alter query
        meta = self.ShowMetadata()
        query = self._gen_alter_query(
            meta, engine, charset, collate, comment, encryption, row_format
        )
        # Execute alteration
        if query.executable():
            with self._pool.acquire() as conn:
                with conn.transaction() as cur:
                    query.execute(cur)
            # . refresh metadata
            meta = self.ShowMetadata()
        # Sync local configs
        return self._sync_from_metadata(meta, query._logs)

    @cython.ccall
    def ShowMetadata(self) -> TableMetadata:
        """[sync] Show the table metadata from the remote server `<'TableMetadata'>`.

        :raises `<'OperationalError'>`: If the table does not exist.
        """
        sql: str = self._gen_show_metadata_sql()
        with self._pool.acquire() as conn:
            conn.set_decode_json(False)
            with conn.transaction(DictCursor) as cur:
                cur.execute(sql)
                res = cur.fetchone()
        if res is None:
            self._raise_operational_error(1050, "does not exist")
        return TableMetadata(res)

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
    def ShowIndexNames(self) -> tuple[str]:
        """[sync] Show all the index names of the table `<'tuple[str]'>`."""
        sql: str = self._gen_show_index_names_sql()
        with self._pool.acquire() as conn:
            with conn.transaction() as cur:
                cur.execute(sql)
                res = cur.fetchall()
        return self._flatten_rows(res)

    @cython.ccall
    def ShowConstraintSymbols(self) -> tuple[str]:
        """[sync] Show all the constraint symbols of the table `<'tuple[str]'>`."""
        sql: str = self._gen_show_constraint_symbols_sql()
        with self._pool.acquire() as conn:
            with conn.transaction() as cur:
                cur.execute(sql)
                res = cur.fetchall()
        return self._flatten_rows(res)

    @cython.ccall
    def Lock(
        self,
        conn: PoolSyncConnection,
        lock_for_read: cython.bint = True,
    ) -> PoolSyncConnection:
        """[sync] Lock this table `<'PoolSyncConnection'>`.

        :param conn `<'PoolSyncConnection'>`: The [sync] connection to acquire the lock of this table.
        :param lock_for_read `<'bool'>`: Lock for `READ` if True, else lock for `WRITE`. Defaults to `True`.
            - **READ mode**: Allows multiple threads to read from the table but
              prevents any thread from modifying it (i.e., no updates, deletes,
              or inserts are allowed).
            - **WRITE mode**: Prevents both read and write operations by any
              other threads.

        :returns `<'PoolSyncConnection'>`: The passed-in [sync] connection that acquired the table lock.

        ## Notice
        - This method locks this table only.
        - If the passed-in [sync] connection already holding locks,
          its existing locks are released implicitly before the new
          locks are granted.
        """
        sql: str = self._gen_lock_sql(lock_for_read)
        with conn.cursor() as cur:
            cur.execute(sql)
        return conn

    @cython.ccall
    def Analyze(self, write_to_binlog: cython.bint = True) -> tuple[dict]:
        """[sync] Analyze the table `<'tuple[dict]'>`.

        :param write_to_binlog `<'bool'>`: Whether to write to the binary log. Defaults to `True`.
            - **True**: the server writes ANALYZE TABLE statements
              to the binary log so that they replicate to replicas.
            - **False**: binary logging will be suppressed.

        :returns `<'tuple[dict]'>`: The result of the analyze operation.
        """
        sql: str = self._gen_analyze_sql(write_to_binlog)
        with self._pool.acquire() as conn:
            with conn.transaction(DictCursor) as cur:
                cur.execute(sql)
                return cur.fetchall()

    def Check(self, *options: str) -> tuple[dict]:
        """[sync] Check the table `<'tuple[dict]'>`.

        :param options `<'*str'>`: The option(s) for the check operation.

            - Accepts: `"FOR UPGRADE"`, `"QUICK"`, `"FAST"`, `"MEDIUM"`, `"EXTENDED"`, `"CHANGED"`.
            - Support multiple options in combination.

        :returns `<'tuple[dict]'>`: The result of the check operation.

        ## Notice
        - `FOR UPGRADE`: Check the table compatibility with the current version of MySQL.
        - `QUICK`: Do not scan the rows to check for incorrect links. Applies to InnoDB
            and MyISAM tables and views.
        - `FAST`: Check only tables that have not been closed properly. Ignored for InnoDB;
            applies only to MyISAM tables and views.
        - `MEDIUM`: Check only tables that have been changed since the last check or that have
            not been closed properly. Ignored for InnoDB; applies only to MyISAM tables and views.
        - `EXTENDED`: Scan rows to verify that deleted links are valid. This also calculates
            a key checksum for the rows and verifies this with a calculated checksum for the
            keys. Ignored for InnoDB; applies only to MyISAM tables and views.
        - `CHANGED`: Do a full key lookup for all keys for each row. This ensures that the table
            is 100% consistent, but takes a long time. Ignored for InnoDB; applies only to MyISAM
            tables and views.
        """
        sql: str = self._gen_check_sql(options)
        with self._pool.acquire() as conn:
            with conn.transaction(DictCursor) as cur:
                cur.execute(sql)
                return cur.fetchall()

    @cython.ccall
    def Optimize(self, write_to_binlog: cython.bint = True) -> tuple[dict]:
        """[sync] Optimize the table `<'tuple[dict]'>`.

        :param write_to_binlog `<'bool'>`: Whether to write to the binary log. Defaults to `True`.
            - **True**: the server writes OPTIMIZE TABLE statements
              to the binary log so that they replicate to replicas.
            - **False**: binary logging will be suppressed.

        :returns `<'tuple[dict]'>`: The result of the optimize operation.
        """
        sql: str = self._gen_optimize_sql(write_to_binlog)
        with self._pool.acquire() as conn:
            with conn.transaction(DictCursor) as cur:
                cur.execute(sql)
                return cur.fetchall()

    @cython.ccall
    def Repair(
        self,
        write_to_binlog: cython.bint = True,
        option: str = None,
    ) -> tuple[dict]:
        """[sync] Repair the table `<'tuple[dict]'>`.

        :param write_to_binlog `<'bool'>`: Whether to write to the binary log. Defaults to `True`.
            - **True**:, the server writes REPAIR TABLE statements
              to the binary log so that they replicate to replicas.
            - **False**: binary logging will be suppressed.

        :param option `<'str/None'>`: The option for the repair operation. Defaults to `None`.
            - Accepts: `"QUICK"`, `"EXTENDED"`, `"USE_FRM"`.

        :returns `<'tuple[dict]'>`: The result of the repair operation.

        ## Notice
        - `QUICK`: Tries to repair only the index file, and not the data file. This type of
           repair is like that done by myisamchk --recover --quick.
        - `EXTENDED`: MySQL creates the index row by row instead of creating one index at a
           time with sorting. This type of repair is like that done by myisamchk --safe-recover.
        - `USE_FRM`: Option is available for use if the .MYI index file is missing or if its
           header is corrupted. This option tells MySQL not to trust the information in the
           .MYI file header and to re-create it using information from the data dictionary.
           This kind of repair cannot be done with myisamchk.
        """
        sql: str = self._gen_repair_sql(write_to_binlog, option)
        with self._pool.acquire() as conn:
            with conn.transaction(DictCursor) as cur:
                cur.execute(sql)
                return cur.fetchall()

    @cython.ccall
    def SyncFromRemote(self, thorough: cython.bint = False) -> Logs:
        """[sync] Synchronize the local table configs with the remote server `<'Logs'>`.

        :param thorough `<'bool'>`: Synchronize with the remote server thoroughly. Defaults to `False`.
            - **False**: only synchronize the local table configs.
            - **True**: also synchronize the local table elements configs
              (columns, indexes, constraints, partitioning).

        ## Explanation
        - This method does `NOT` alter the remote server table,
          but only changes the local table configurations to match
          the remote server metadata.
        """
        # Sync: table
        try:
            meta = self.ShowMetadata()
        except sqlerrors.OperationalError as err:
            if err.args[0] == 1050:
                return Logs().log_sync_failed_not_exist(self)
            raise err
        logs: Logs = self._sync_from_metadata(meta)
        if not thorough:
            return logs  # exit

        # Sync: columns
        col: Column
        for col in self._columns._el_dict.values():
            logs.extend(col.SyncFromRemote())
        # Initialize: indexes
        idx: Index
        for idx in self._indexes._el_dict.values():
            logs.extend(idx.SyncFromRemote())
        # Initialize: constraints
        cnst: Constraint
        for cnst in self._constraints._el_dict.values():
            logs.extend(cnst.SyncFromRemote())
        # Initialize: partitioning
        if self._partitioning is not None:
            logs.extend(self._partitioning.SyncFromRemote())
        # Finished
        return logs

    @cython.ccall
    def SyncToRemote(self) -> Logs:
        """[sync] Synchronize the remote server with the local table configs `<'Logs'>`.

        ## Explanation
        - This method compares the local table configurations with the
          remote server metadata and issues the necessary ALTER TABLE
          statements so that the remote one matches the local settings.
        - Different from `SyncFromRemote()`, this method does not provide
          the `thorough` option. To synchronize the table elements, you
          need to call the `SyncToRemote()` from the element itself.
        """
        # Check existence
        if not self.Exists():
            return self.Create(True)
        # Sync to remote
        return self.Alter(
            self._engine,
            self._charset,
            None,
            self._comment,
            utils.read_bool_config(self._encryption),
            self._row_format,
        )

    # Async --------------------------------------------------------------------------------
    async def aioInitialize(self, force: cython.bint = False) -> Logs:
        """[async] Initialize the table `<'Logs'>`.

        :param force `<'bool'>`: Whether to force the initialization even
            the table has already been initialized. Defaults to `False`.

        ## Explanation (difference from create)
        - 1. Create the table if not exists.
        - 2. Initialize all the columns.
        - 3. Initialize all the indexes (if exists).
        - 4. Initialize all the constraints (if exists).
        - 5. Initialize the partitioning (if exists).
        """
        logs: Logs = Logs()
        # Skip
        if self._initialized and not force:
            return logs
        # Initialize: table
        if not await self.aioExists():
            logs.extend(await self.aioCreate(True))
        else:
            logs.extend(await self.aioSyncFromRemote())
        # Initialize: columns
        tasks: list = []
        for col in self._columns._el_dict.values():
            tasks.append(col.aioInitialize(force))
        # Initialize: indexes
        for idx in self._indexes._el_dict.values():
            tasks.append(idx.aioInitialize(force))
        # Initialize: constraints
        for cnst in self._constraints._el_dict.values():
            tasks.append(cnst.aioInitialize(force))
        # Initialize: partitioning
        if self._partitioning is not None:
            tasks.append(self._partitioning.aioInitialize(force))
        # Await all tasks
        for l in await _aio_gather(*tasks):
            logs.extend(l)
        # Finished
        self._set_initialized(True)
        return logs

    async def aioCreate(self, if_not_exists: cython.bint = False) -> Logs:
        """[async] Create the table `<'Logs'>`.

        :param if_not_exists `<'bool'>`: Create the table only if it does not exist. Defaults to `False`.
        :raises `<'OperationalError'>`: If the table already exists and 'if_not_exists=False'.
        """
        # Execute creation
        sql: str = self._gen_create_sql(if_not_exists)
        async with self._pool.acquire() as conn:
            async with conn.transaction() as cur:
                await cur.execute(sql)
        logs = Logs().log_sql(self, sql)
        logs.log_element_creation(self, False)
        # Sync from remote
        return logs.extend(await self.aioSyncFromRemote())

    async def aioExists(self) -> bool:
        """[async] Check if the table exists `<'bool'>`."""
        sql: str = self._gen_exists_sql()
        async with self._pool.acquire() as conn:
            async with conn.transaction() as cur:
                await cur.execute(sql)
                res = await cur.fetchone()
        if res is None:
            self._set_initialized(False)
            return False
        return True

    async def aioTruncate(self) -> Logs:
        """[async] Truncate the table `<'Logs'>`.

        ## Explanation
        - Truncate operations drop and re-create the table, which is much faster than deleting
          rows one by one, particularly for large tables.
        - Truncate operations cause an implicit commit, and so cannot be rolled back.
        - Truncation operations cannot be performed if the session holds an active table lock.
        - TRUNCATE TABLE fails for an InnoDB table or NDB table if there are any
          FOREIGN KEY constraints from other tables that reference the table. Foreign key
          constraints between columns of the same table are permitted.
        - Truncation operations do not return a meaningful value for the number of deleted rows.
          The usual result is "0 rows affected", which should be interpreted as "no information."
        - As long as the table definition is valid, the table can be re-created as an empty table
          with TRUNCATE TABLE, even if the data or index files have become corrupted.
        - Any AUTO_INCREMENT value is reset to its start value. This is true even for MyISAM and
          InnoDB, which normally do not reuse sequence values.
        - When used with partitioned tables, TRUNCATE TABLE preserves the partitioning; that is,
          the data and index files are dropped and re-created, while the partition definitions
          are unaffected.
        - The TRUNCATE TABLE statement does not invoke ON DELETE triggers.
        - Truncating a corrupted InnoDB table is supported.
        """
        sql: str = self._gen_truncate_sql()
        async with self._pool.acquire() as conn:
            async with conn.transaction() as cur:
                await cur.execute(sql)
        return Logs().log_sql(self, sql)

    async def aioDrop(self, if_exists: cython.bint = False) -> Logs:
        """[async] Drop the table `<'Logs'>`.

        :param if_exists `<'bool'>`: Drop the table only if it exists. Defaults to `False`.
        :raises `<'OperationalError'>`: If the table does not exists and 'if_exists=False'.

        ## Explanation
        - DROP TABLE removes the table definition and all table data. If the table is partitioned,
          the statement removes the table definition, all its partitions, all data stored in those
          partitions, and all partition definitions associated with the dropped table.
        - Dropping a table also drops any triggers for the table.
        - DROP TABLE causes an implicit commit, except when used with the TEMPORARY keyword.
        """
        sql: str = self._gen_drop_sql(if_exists)
        async with self._pool.acquire() as conn:
            async with conn.transaction() as cur:
                await cur.execute(sql)
        self._set_initialized(False)
        logs = Logs().log_sql(self, sql)
        return logs.log_element_deletion(self, False)

    async def aioEmpty(self) -> bool:
        """[async] Check if the table is empty `<'bool'>`."""
        sql: str = self._gen_empty_sql()
        async with self._pool.acquire() as conn:
            async with conn.transaction() as cur:
                await cur.execute(sql)
                res = await cur.fetchone()
        return res is None

    async def aioAlter(
        self,
        engine: str = None,
        charset: object = None,
        collate: object = None,
        comment: str = None,
        encryption: bool | None = None,
        row_format: object = None,
    ) -> Logs:
        """[async] Alter the table `<'Logs'>`.

        ## Notice
        - Default value `None` means to retain the corresponding settings of the table.

        :param engine `<'str'>`: The storage ENGINE of the table. Defaults to `None`.
        :param charset `<'str/Charset/None'>`: The CHARACTER SET of the table. Defaults to `None`.
        :param collate `<'str/None'>`: The COLLATION of the table. Defaults to `None`.
        :param comment `<'str/None'>`: The COMMENT of the table. Defaults to `None`.
            To remove existing comment, set `comment=''`.
        :param encryption `<'bool/None'>`: The table ENCRYPTION behavior. Defaults to `None`.
            Enable encryption if `True`, else `False` to disable.
        :param row_format `<'str/None'>`: The physical format in which the rows are stored. Defaults to `None`.
            Accepts: `"COMPACT"`, `"COMPRESSED"`, `"DYNAMIC"`, `"FIXED"`, `"REDUNDANT"`, `"PAGED"`.
        """
        # Generate alter query
        meta = await self.aioShowMetadata()
        query = self._gen_alter_query(
            meta, engine, charset, collate, comment, encryption, row_format
        )
        # Execute alteration
        if query.executable():
            async with self._pool.acquire() as conn:
                async with conn.transaction() as cur:
                    await query.aio_execute(cur)
            # . refresh metadata
            meta = await self.aioShowMetadata()
        # Sync from remote
        return self._sync_from_metadata(meta, query._logs)

    async def aioShowMetadata(self) -> TableMetadata:
        """[async] Show the table metadata from the remote server `<'TableMetadata'>`.

        :raises `<'OperationalError'>`: If the table does not exist.
        """
        sql: str = self._gen_show_metadata_sql()
        async with self._pool.acquire() as conn:
            conn.set_decode_json(False)
            async with conn.transaction(AioDictCursor) as cur:
                await cur.execute(sql)
                res = await cur.fetchone()
        if res is None:
            self._raise_operational_error(1050, "does not exist")
        return TableMetadata(res)

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

    async def aioShowIndexNames(self) -> tuple[str]:
        """[async] Show all the index names of the table `<'tuple[str]'>`."""
        sql: str = self._gen_show_index_names_sql()
        async with self._pool.acquire() as conn:
            async with conn.transaction() as cur:
                await cur.execute(sql)
                res = await cur.fetchall()
        return self._flatten_rows(res)

    async def aioShowConstraintSymbols(self) -> tuple[str]:
        """[sync] Show all the constraint symbols of the table `<'tuple[str]'>`."""
        sql: str = self._gen_show_constraint_symbols_sql()
        async with self._pool.acquire() as conn:
            async with conn.transaction() as cur:
                await cur.execute(sql)
                res = await cur.fetchall()
        return self._flatten_rows(res)

    async def aioLock(
        self,
        conn: PoolConnection,
        lock_for_read: cython.bint = True,
    ) -> PoolConnection:
        """[async] Lock this table `<'PoolConnection'>`.

        :param conn `<'PoolConnection'>`: The [sync] connection to acquire the lock of this table.
        :param lock_for_read `<'bool'>`: Lock for `READ` if True, else lock for `WRITE`. Defaults to `True`.
            - **READ mode**: Allows multiple threads to read from the table but
              prevents any thread from modifying it (i.e., no updates, deletes,
              or inserts are allowed).
            - **WRITE mode**: Prevents both read and write operations by any
              other threads.

        :returns `<'PoolConnection'>`: The passed-in [sync] connection that acquired the table lock.

        ## Notice
        - This method locks this table only.
        - If the passed-in [sync] connection already holding locks,
          its existing locks are released implicitly before the new
          locks are granted.
        """
        sql: str = self._gen_lock_sql(lock_for_read)
        async with conn.cursor() as cur:
            await cur.execute(sql)
        return conn

    async def aioAnalyze(self, write_to_binlog: cython.bint = True) -> tuple[dict]:
        """[async] Analyze the table `<'tuple[dict]'>`.

        :param write_to_binlog `<'bool'>`: Whether to write to the binary log. Defaults to `True`.
            - **True**: the server writes ANALYZE TABLE statements
              to the binary log so that they replicate to replicas.
            - **False**: binary logging will be suppressed.

        :returns `<'tuple[dict]'>`: The result of the analyze operation.
        """
        sql: str = self._gen_analyze_sql(write_to_binlog)
        async with self._pool.acquire() as conn:
            async with conn.transaction(AioDictCursor) as cur:
                await cur.execute(sql)
                return await cur.fetchall()

    async def aioCheck(self, *options: str) -> tuple[dict]:
        """[async] Check the table `<'tuple[dict]'>`.

        :param options `<'*str'>`: The option(s) for the check operation.

            - Accepts: `"FOR UPGRADE"`, `"QUICK"`, `"FAST"`, `"MEDIUM"`, `"EXTENDED"`, `"CHANGED"`.
            - Support multiple options in combination.

        :returns `<'tuple[dict]'>`: The result of the check operation.

        ## Notice
        - `FOR UPGRADE`: Check the table compatibility with the current version of MySQL.
        - `QUICK`: Do not scan the rows to check for incorrect links. Applies to InnoDB
            and MyISAM tables and views.
        - `FAST`: Check only tables that have not been closed properly. Ignored for InnoDB;
            applies only to MyISAM tables and views.
        - `MEDIUM`: Check only tables that have been changed since the last check or that have
            not been closed properly. Ignored for InnoDB; applies only to MyISAM tables and views.
        - `EXTENDED`: Scan rows to verify that deleted links are valid. This also calculates
            a key checksum for the rows and verifies this with a calculated checksum for the
            keys. Ignored for InnoDB; applies only to MyISAM tables and views.
        - `CHANGED`: Do a full key lookup for all keys for each row. This ensures that the table
            is 100% consistent, but takes a long time. Ignored for InnoDB; applies only to MyISAM
            tables and views.
        """
        sql: str = self._gen_check_sql(options)
        async with self._pool.acquire() as conn:
            async with conn.transaction(AioDictCursor) as cur:
                await cur.execute(sql)
                return await cur.fetchall()

    async def aioOptimize(self, write_to_binlog: cython.bint = True) -> tuple[dict]:
        """[async] Optimize the table `<'tuple[dict]'>`.

        :param write_to_binlog `<'bool'>`: Whether to write the OPTIMIZE TABLE statements to the binary log. Defaults to `True`.
            - **True**: the server writes OPTIMIZE TABLE statements
              to the binary log so that they replicate to replicas.
            - **False**: binary logging will be suppressed.

        :returns `<'tuple[dict]'>`: The result of the optimize operation.
        """
        sql: str = self._gen_optimize_sql(write_to_binlog)
        async with self._pool.acquire() as conn:
            async with conn.transaction(AioDictCursor) as cur:
                await cur.execute(sql)
                return await cur.fetchall()

    async def aioRepair(
        self,
        write_to_binlog: cython.bint = True,
        option: str = None,
    ) -> tuple[dict]:
        """[async] Repair the table `<'tuple[dict]'>`.

        :param write_to_binlog `<'bool'>`: Whether to write to the binary log. Defaults to `True`.
            - **True**:, the server writes REPAIR TABLE statements
              to the binary log so that they replicate to replicas.
            - **False**: binary logging will be suppressed.

        :param option `<'str/None'>`: The option for the repair operation. Defaults to `None`.
            - Accepts: `"QUICK"`, `"EXTENDED"`, `"USE_FRM"`.

        :returns `<'tuple[dict]'>`: The result of the repair operation.

        ## Notice
        - `QUICK`: Tries to repair only the index file, and not the data file. This type of
           repair is like that done by myisamchk --recover --quick.
        - `EXTENDED`: MySQL creates the index row by row instead of creating one index at a
           time with sorting. This type of repair is like that done by myisamchk --safe-recover.
        - `USE_FRM`: Option is available for use if the .MYI index file is missing or if its
           header is corrupted. This option tells MySQL not to trust the information in the
           .MYI file header and to re-create it using information from the data dictionary.
           This kind of repair cannot be done with myisamchk.
        """
        sql: str = self._gen_repair_sql(write_to_binlog, option)
        async with self._pool.acquire() as conn:
            async with conn.transaction(AioDictCursor) as cur:
                await cur.execute(sql)
                return await cur.fetchall()

    async def aioSyncFromRemote(self, thorough: cython.bint = False) -> Logs:
        """[async] Synchronize the local table configs with the remote server `<'Logs'>`.

        :param thorough `<'bool'>`: Synchronize with the remote server thoroughly. Defaults to `False`.
            - **False**: only synchronize the local table configs.
            - **True**: also synchronize the local table elements configs
              (columns, indexes, constraints, partitioning).

        ## Explanation
        - This method does `NOT` alter the remote server table,
          but only changes the local table configurations to match
          the remote server metadata.
        """
        # Sync: table
        try:
            meta = await self.aioShowMetadata()
        except sqlerrors.OperationalError as err:
            if err.args[0] == 1050:
                return Logs().log_sync_failed_not_exist(self)
            raise err
        logs: Logs = self._sync_from_metadata(meta)
        if not thorough:
            return logs  # exit

        # Sync: columns
        tasks: list = []
        for col in self._columns._el_dict.values():
            tasks.append(col.aioSyncFromRemote())
        # Initialize: indexes
        for idx in self._indexes._el_dict.values():
            tasks.append(idx.aioSyncFromRemote())
        # Initialize: constraints
        for cnst in self._constraints._el_dict.values():
            tasks.append(cnst.aioSyncFromRemote())
        # Initialize: partitioning
        if self._partitioning is not None:
            tasks.append(self._partitioning.aioSyncFromRemote())
        # Await all tasks
        for l in await _aio_gather(*tasks):
            logs.extend(l)
        # Finished
        return logs

    async def aioSyncToRemote(self) -> Logs:
        """[async] Synchronize the remote server with the local table configs `<'Logs'>`.

        ## Explanation
        - This method compares the local table configurations with the
          remote server metadata and issues the necessary ALTER TABLE
          statements so that the remote one matches the local settings.
        - Different from `SyncFromRemote()`, this method does not provide
          the `thorough` option. To synchronize the table elements, you
          need to call the `SyncToRemote()` from the element itself.
        """
        # Check existence
        if not await self.aioExists():
            return await self.aioCreate(True)
        # Sync to remote
        return await self.aioAlter(
            self._engine,
            self._charset,
            None,
            self._comment,
            utils.read_bool_config(self._encryption),
            self._row_format,
        )

    # Copy ---------------------------------------------------------------------------------
    @cython.ccall
    def copy(self) -> Table:
        """Make a copy of the table `<'Table'>`."""
        try:
            tb: Table = self.__class__(
                self._engine,
                self._charset,
                None,
                self._comment,
                utils.read_bool_config(self._encryption),
                self._row_format,
            )
        except TypeError as err:
            if "positional arguments" in str(err):
                self._raise_critical_error(
                    "inherits from the base <'Table'> class, "
                    "must not override its '__init__' method.",
                    err,
                )
            raise err
        tb.set_name(self._name)
        return tb


# Prohibited names from Table class
utils.SCHEMA_ELEMENT_SETTINGS.add_prohibited_names(dir(Table()))


# Time Table --------------------------------------------------------------------------------------------------
@cython.cclass
class TimeTable(Table):
    """Represents a time-series partitioned table with automatic range partitions.

    ## Explanation
    TimeTable (InnoDB) extends Table to provide MySQL RANGE partitioning on a specified
    datetime column and unit. It always maintains at least three partitions:
        - **past**: overflow for rows older than the earliest in-range period
        - **in-range**: one or more partitions covering the configured time window(s)
        - **future**: overflow for rows newer than the latest in-range period

    In-range Partition naming follows the below rules:
    ```python
    unit: YEAR    ->   'yYYYY'             (e.g., 'y2023')
    unit: QUARTER ->   'qYYYYQ'            (e.g., 'q20231')
    unit: MONTH   ->   'mYYYYMM'           (e.g., 'm202301')
    unit: WEEK    ->   'wYYYYMMDD'         (e.g., 'w20230102')
    unit: DAY     ->   'dYYYYMMDD'         (e.g., 'd20230101')
    unit: HOUR    ->   'hYYYYMMDD_HH'      (e.g., 'h20230201_12')
    unit: MINUTE  ->   'iYYYYMMDD_HHMM'    (e.g., 'i20230201_1230')
    unit: SECOND  ->   'sYYYYMMDD_HHMMSS'  (e.g., 's20230201_123045')
    ```
    TimeTable also provides methods to easily manage the time-based partitions,
    such as extending, coalescing and dropping the 'in-range' partitions, as well
    as reorganizing the overflow catchers ('past' & 'future' partitions).
    """

    _time_column: str
    _timestamp_based: cython.bint
    _time_unit: cython.int
    _pydt_unit: str
    _start_from: _Pydt
    _end_with: _Pydt

    def __init__(
        self,
        time_column: object,
        time_unit: object,
        start_from: object,
        end_with: object,
        charset: object | None = None,
        collate: object | None = None,
        comment: str = None,
        encryption: bool | None = None,
        row_format: object = None,
    ):
        """The time-series partitioned table with automatic range partitions.

        ## Explanation
        TimeTable (InnoDB) extends Table to provide MySQL RANGE partitioning on a specified
        datetime column and unit. It always maintains at least three partitions:
            - **past**: overflow for rows older than the earliest in-range period
            - **in-range**: one or more partitions covering the configured time window(s)
            - **future**: overflow for rows newer than the latest in-range period

        In-range Partition naming follows the below rules:
        ```python
        unit: YEAR    ->   'yYYYY'             (e.g., 'y2023')
        unit: QUARTER ->   'qYYYYQ'            (e.g., 'q20231')
        unit: MONTH   ->   'mYYYYMM'           (e.g., 'm202301')
        unit: WEEK    ->   'wYYYYMMDD'         (e.g., 'w20230102')
        unit: DAY     ->   'dYYYYMMDD'         (e.g., 'd20230101')
        unit: HOUR    ->   'hYYYYMMDD_HH'      (e.g., 'h20230201_12')
        unit: MINUTE  ->   'iYYYYMMDD_HHMM'    (e.g., 'i20230201_1230')
        unit: SECOND  ->   'sYYYYMMDD_HHMMSS'  (e.g., 's20230201_123045')
        ```
        TimeTable also provides methods to easily manage the time-based partitions,
        such as extending, coalescing and dropping the 'in-range' partitions, as well
        as reorganizing the overflow catchers ('past' & 'future' partitions).

        :param time_column `<'str'>`: Name of the column used as the RANGE partition key.
            Must be one of DATE, DATETIME, or TIMESTAMP data types, and included
            in the table's PRIMARY or UNIQUE key (if configured).

        :param time_unit `<'str'>`: The time unit for time-series partitioning interval.
            Accepts: `"YEAR"`, `"QUARTER"`, `"MONTH"`, `"WEEK"`, `"DAY"`, `"HOUR"`, `"MINUTE"`, `"SECOND"`.

        :param start_from `<'str/date/datetime'>`: The start time (inclusive) of the first 'in-range' partition.
            Time value is automatically aligned to the specified time_unit.

        :param end_with `<'str/date/datetime/None'>`: The end time (inclusive) of the last 'in-range' partition.
            If `None`, defaults to the current datetime. Time value is
            automatically aligned to the specified time_unit.

        :param charset `<'str/Charset/None'>`: The CHARACTER SET of the table. Defaults to `None`.
            If not specified (None), use the charset of the database.

        :param collate `<'str/None'>`: The COLLATION of the table. Defaults to `None`.
            If not specified (None), use the collate of the database.

        :param comment `<'str/None'>`: The COMMENT of the table. Defaults to `None`.

        :param encryption `<'bool/None'>`: The table ENCRYPTION behavior. Defaults to `None`.
            - **None**: use the encryption setting of the database.
            - **True/False**: enabled/disable per table encryption.

        :param row_format `<'str/None'>`: The physical format in which the rows are stored. Defaults to `None`.
            Accepts: `"COMPACT"`, `"COMPRESSED"`, `"DYNAMIC"`, `"FIXED"`, `"REDUNDANT"`, `"PAGED"`.
        """
        super().__init__("InnoDB", charset, collate, comment, encryption, row_format)
        self._set_el_type("TIME TABLE")
        self._time_column = self._validate_column_name(time_column)
        self._timestamp_based = False
        self._time_unit = self._validate_time_unit(time_unit)
        self._pydt_unit = self._validate_pydt_unit(self._time_unit)
        # Time values
        self._start_from = self._validate_partition_time(start_from, "start_from")
        if end_with is None:
            self._end_with = self._validate_partition_time(
                datetime.datetime_from_timestamp(unix_time(), None), "end_with"
            )
        else:
            self._end_with = self._validate_partition_time(end_with, "end_with")

    # Property -----------------------------------------------------------------------------
    @property
    def time_column(self) -> Column:
        """The column used as the RANGE partitioning key `<'Column'>`."""
        self._assure_ready()
        return self._columns[self._time_column]

    @property
    def time_unit(self) -> str:
        """The time unit of the time-series partitioning interval `<'str'>`."""
        return utils.timetable_unit_flag_to_str(self._time_unit)

    # Sync ---------------------------------------------------------------------------------
    def ExtendToTime(
        self,
        *,
        start_from: object = None,
        end_with: object = None,
    ) -> Logs:
        """[sync] Extend the TimeTable 'in-range' partitions to cover a specified time window `<'Logs'>`.

        ## Explanation
        - This method reorganizing the overflow partitions ('past' and 'future') into
          named 'in-range' partitions spanning from `start_from` to `end_with`.
        - Existing data remains intact; only the partition structure is modified.

        :param start_from `<'str/date/datetime/None'>`: New lower bound (inclusive) for 'in-range' partitions. Defaults to `None`.
            Time value is automatically aligned to the table's time unit. If `None` or
            later than the current lower bound, the start boundary is left unchanged.

        :param end_with `<'str/date/datetime/None'>`: New upper bound (inclusive) for 'in-range' partitions. Defaults to `None`.
            Time value is automatically aligned to the table's time unit. If `None` or
            earlier than the current upper bound, the end boundary is left unchanged.

        ## Example
        >>> db.tb.ShowPartitionRows()
        >>> {"past": 0, "y2024": 1, "future": 1}
        >>> db.tb.ExtendToTime(start_from="2023-01-01", end_with="2025-01-01")
            # Add 'in-range' partitions to cover data from year 2022 to 2025.
        >>> db.tb.ShowPartitionRows()
        >>> {"past": 0, "y2023": 0, "y2024": 1, "y2025": 1, "future": 0}
        """
        logs: Logs = Logs()

        # Extend to past
        if start_from is not None:
            logs.extend(self._ExtendToTime(False, start_from))

        # Extend to future
        if end_with is not None:
            logs.extend(self._ExtendToTime(True, end_with))

        # Finished
        return logs

    @cython.ccall
    def _ExtendToTime(self, future: cython.bint, to_time: object) -> Logs:
        """(internal) [sync] Extend the TimeTable 'in-range' partitions to
        cover a specified time in one direction `<'Logs'>`.

        ## Explanation
        - This method reorganizes the corresponding overflow partition ('past' or 'future')
          into one or more 'in-range' partitions up to the given boundary time.
        - Existing data remains intact; only the partition structure is modified.

        :param future `<'bool'>`: Extend the upper 'in-range' partition if `True`, else exetend the lower one.
        :param to_time `<'str/date/datetime'>`: New inclusive boundary for the 'in-range' partitions.
            Time value is automatically aligned to the table's time unit.
        """
        logs: Logs = Logs()

        # Extend partitions [future]
        if future:
            t_time: _Pydt = self._validate_partition_time(to_time, "end_with")
            p_time: _Pydt = self.GetBoundaryPartitionTime(True)
            num: cython.longlong = self._cal_partition_time_diff(t_time, p_time)
            if num < 1:
                return logs  # exit
            part: Partitioning = self._partitioning
            futr = self._create_future_partition()
            i: cython.longlong
            for i in range(num):
                next_time: _Pydt = self._shift_partition_time(p_time, i + 1)
                pt = self._create_in_range_partition(next_time)
                logs.extend(part.ReorganizePartition("future", pt, futr))
            return logs

        # Extend partitions [past]
        else:
            t_time: _Pydt = self._validate_partition_time(to_time, "start_with")
            p_time: _Pydt = self.GetBoundaryPartitionTime(False)
            num: cython.longlong = self._cal_partition_time_diff(p_time, t_time)
            if num < 1:
                return logs  # exit
            part: Partitioning = self._partitioning
            i: cython.longlong
            for i in range(num):
                prev_time: _Pydt = self._shift_partition_time(p_time, -(i + 1))
                past = self._create_past_partition(prev_time)
                pt = self._create_in_range_partition(prev_time)
                logs.extend(part.ReorganizePartition("past", past, pt))
            return logs

    def CoalesceToTime(
        self,
        *,
        start_from: object = None,
        end_with: object = None,
    ) -> Logs:
        """[sync] Coalesce the TimeTable 'in-range' partitions to a specified time window `<'Logs'>`.

        ## Explanation
        - The method reorganizes the 'in-range' partitions into overflow partitions
          ("past" & "future"), so that the 'in-range' partitions only cover the
          time range from `start_from` to `end_with`.
        - Existing data remains intact; only the partition structure is modified.

        :param start_from `<'str/date/datetime/None'>`: New lower bound (inclusive) for 'in-range' partitions. Defaults to `None`.
            Time value is automatically aligned to the table's time unit. If `None` or
            eariler than the current lower bound, the start boundary is left unchanged.

        :param end_with `<'str/date/datetime/None'>`: New upper bound (inclusive) for 'in-range' partitions. Defaults to `None`.
            Time value is automatically aligned to the table's time unit. If `None` or
            later than the current upper bound, the end boundary is left unchanged.

        ## Example
        >>> db.tb.ShowPartitionRows()
        >>> {"past": 0, "y2023": 1, "y2024": 1, "y2025": 1, "future": 0}
        >>> db.tb.CoalesceToTime(start_from="2024-01-01", end_with="2024-01-01")
            # Coalesce 'y2023' and 'y2025' into 'past' and 'future' partitions.
        >>> db.tb.ShowPartitionRows()
        >>> {"past": 1, "y2024": 1, "future": 1}
        """
        logs: Logs = Logs()

        # Coalesce from past
        if start_from is not None:
            logs.extend(self._CoalesceToTime(False, start_from))

        # Coalesce from future
        if end_with is not None:
            logs.extend(self._CoalesceToTime(True, end_with))

        # Finished
        return logs

    @cython.ccall
    def _CoalesceToTime(self, future: cython.bint, to_time: object) -> Logs:
        """(internal) [sync] Coalesce the TimeTable 'in-range' partitions
        to a specific time in one direction `<'Logs'>`.

        ## Explanation
        - This method reorganizes 'in-range' partitions beyond the specified boundary
          in to the appropriate overflow partition ('past' or 'future').
        - Existing data remains intact; only the partition structure is modified.

        :param future `<'bool'>`: Coalesce 'in-range' partitions later than the 'to_time' into 'future'
            if `True`, else the ones eariler than the 'to_time' into 'past'.
        :param to_time `<'str/date/datetime'>`: New inclusive boundary for the 'in-range' partitions.
            Time value is automatically aligned to the table's time unit.
        """
        logs: Logs = Logs()

        # Validate partition count
        pt_count: cython.int = self._GetInRangePartitionCount()
        if pt_count == 1:
            return logs  # exit

        # Coalesce partitions [future]
        if future:
            t_time: _Pydt = self._validate_partition_time(to_time, "end_with")
            p_time: _Pydt = self.GetBoundaryPartitionTime(True)
            num: cython.longlong = self._cal_partition_time_diff(p_time, t_time)
            if num < 1:
                return logs  # exit
            if num >= pt_count:
                num = pt_count - 1  # limit to the latest partition
            part: Partitioning = self._partitioning
            futr = self._create_future_partition()
            i: cython.longlong
            for i in range(num):
                prev_time: _Pydt = self._shift_partition_time(p_time, -i)
                prev_name: str = self._gen_partition_name(prev_time)
                logs.extend(part.ReorganizePartition([prev_name, "future"], futr))
            return logs

        # Coalesce partitions [past]
        else:
            t_time: _Pydt = self._validate_partition_time(to_time, "start_from")
            p_time: _Pydt = self.GetBoundaryPartitionTime(False)
            num: cython.longlong = self._cal_partition_time_diff(t_time, p_time)
            if num < 1:
                return logs  # exit
            if num >= pt_count:
                num = pt_count - 1  # limit to the latest partition
            part: Partitioning = self._partitioning
            i: cython.longlong
            for i in range(num):
                next_time: _Pydt = self._shift_partition_time(p_time, i)
                next_name: str = self._gen_partition_name(next_time)
                past = self._create_past_partition(self._gen_partition_time(next_time))
                logs.extend(part.ReorganizePartition(["past", next_name], past))
            return logs

    def DropToTime(
        self,
        *,
        start_from: object = None,
        end_with: object = None,
    ) -> Logs:
        """[sync] Drop the TimeTable 'in-range' partitions outside a specified time window `<'Logs'>`.

        ## Explanation
        - This method drops any 'in-range' partitions earlier than `start_from`
          or later than `end_with`, so that the 'in-range' partitions only
          cover the specifed time range.
        - When dropping in the same direction as an overflow partition
          ('past' for `start_from`, 'future' for `end_with`), the
          corresponding overflow partition is truncated rather than dropped.
        - Deleted data is lost and partition structure is modified.

        :param start_from `<'str/date/datetime/None'>`: New lower bound (inclusive) for 'in-range' partitions. Defaults to `None`.
            Time value is automatically aligned to the table's time unit. If `None` or
            earlier than the current lower bound, no earlier partitions are dropped.

        :param end_with `<'str/date/datetime/None'>`: New upper bound (inclusive) for 'in-range' partitions. Defaults to `None`.
            Time value is automatically aligned to the table's time unit. If `None` or
            later than the current upper bound, no later partitions are dropped.

        ## Example
        >>> db.tb.ShowPartitionRows()
        >>> {"past": 1, "y2023": 1, "y2024": 1, "y2025": 1, "future": 1}
        >>> db.tb.DropToTime(start_from="2024-01-01", end_with="2024-01-01")
            # Drop 'y2023' and 'y2025' partitions and truncate both overflow partitions.
        >>> db.tb.ShowPartitionRows()
        >>> {"past": 0, "y2024": 1, "future": 0}
        """
        logs: Logs = Logs()

        # Drop from past
        if start_from is not None:
            logs.extend(self._DropToTime(False, start_from))

        # Drop from future
        if end_with is not None:
            logs.extend(self._DropToTime(True, end_with))

        # Finished
        return logs

    @cython.ccall
    def _DropToTime(self, future: cython.bint, to_time: object) -> Logs:
        """(internal) [sync] Drop the TimeTable 'in-range' partitions
        to a specific time in one direction `<'Logs'>`.

        ## Explanation
        - This method drops 'in-range' partitions later than 'to_time' (if 'future=False')
          or eariler than 'to_time' (if 'future=True').
        - When dropping in the same direction as an overflow partition
          ('past' for `start_from`, 'future' for `end_with`), the
          corresponding overflow partition is truncated rather than dropped.
        - Deleted data is lost and partition structure is modified.

        :param future `<'bool'>`: Drop 'in-range' partitions later than 'to_time' if `True`,
            else the ones eariler than the 'to_time'.
        :param to_time `<'str/date/datetime'>`: New inclusive boundary for the 'in-range' partitions.
            Time value is automatically aligned to the table's time unit.
        """
        logs: Logs = Logs()

        # Validate partition count
        pt_count: cython.int = self._GetInRangePartitionCount()
        if pt_count == 1:
            return logs  # exit

        # Drop partitions [future]
        if future:
            t_time: _Pydt = self._validate_partition_time(to_time, "end_with")
            p_time: _Pydt = self.GetBoundaryPartitionTime(True)
            num: cython.longlong = self._cal_partition_time_diff(p_time, t_time)
            if num < 1:
                return logs  # exit
            if num >= pt_count:
                num = pt_count - 1  # limit to the latest partition
            part: Partitioning = self._partitioning
            pt_names = []
            i: cython.longlong
            for i in range(num):
                prev_time: _Pydt = self._shift_partition_time(p_time, -i)
                pt_names.append(self._gen_partition_name(prev_time))
            logs.extend(part.DropPartition(*pt_names))
            logs.extend(part.TruncatePartition("future"))
            return logs

        # Drop partitions [past]
        else:
            t_time: _Pydt = self._validate_partition_time(to_time, "start_from")
            p_time: _Pydt = self.GetBoundaryPartitionTime(False)
            num: cython.longlong = self._cal_partition_time_diff(t_time, p_time)
            if num < 1:
                return logs  # exit
            if num >= pt_count:
                num = pt_count - 1  # limit to the latest partition
            part: Partitioning = self._partitioning
            pt_names = ["past"]
            i: cython.longlong
            for i in range(num - 1):
                next_time: _Pydt = self._shift_partition_time(p_time, i)
                pt_names.append(self._gen_partition_name(next_time))
            new_past_time: _Pydt = self._shift_partition_time(p_time, num - 1)
            old_past_name: str = self._gen_partition_name(new_past_time)
            past = self._create_past_partition(self._gen_partition_time(new_past_time))
            logs.extend(part.DropPartition(*pt_names))
            logs.extend(part.TruncatePartition(old_past_name))
            logs.extend(part.ReorganizePartition(old_past_name, past))
            return logs

    @cython.ccall
    def ReorganizeOverflow(self, catcher: str = "future") -> Logs:
        """[sync] Reorganize the overflow partitions by extending into
        'in-range' partitions based on their boundary values `<'Logs'>`.

        ## Explanation
        - This method reorganizes one or both overflow partitions ('past', 'future') into
          named 'in-range' partitions. For each specified catcher:
            - **future**: uses the maximum value in the future partition as a new upper boundary
            - **past**: uses the minimum value in the past partition as a new lower boundary
        - No existing data is lost; only the partition structure is modified.

        :param catcher `<'str'>`: Which overflow partition(s) to reorganize. Must be one of:
            - **"future"**: reorganize only the upper overflow partition.
            - **"past"**: reorganize only the lower overflow partition.
            - **"both"**: reorganize both overflow partitions.

        ## Example
        >>> db.tb.ShowPartitionRows()
        >>> {"past": 1, "y2024": 1, "future": 1}
        >>> db.tb.ReorganizeOverflow("both")
            # Reorganize 'past' and 'future' partitions into 'in-range' partitions.
        >>> db.tb.ShowPartitionRows()
        >>> {"past": 0, "y2023": 1, "y2024": 1, "y2025": 1, "future": 0}
        """
        # Reorganize Future
        if catcher == "future":
            time = self._GetOverflowBoundaryValue(True)
            if time is None:
                return Logs()
            return self._ExtendToTime(True, time)

        # Reorganize Past
        if catcher == "past":
            time = self._GetOverflowBoundaryValue(False)
            if time is None:
                return Logs()
            return self._ExtendToTime(False, time)

        # Reorganize Both
        if catcher == "both":
            logs: Logs = Logs()
            # . past
            time = self._GetOverflowBoundaryValue(False)
            if time is not None:
                logs.extend(self._ExtendToTime(False, time))
            # . future
            time = self._GetOverflowBoundaryValue(True)
            if time is not None:
                logs.extend(self._ExtendToTime(True, time))
            return logs

        # Invalid catcher
        self._raise_argument_error(
            "overflow 'catcher' must be 'future', 'past', "
            "or 'both', instead got '%s'." % catcher
        )

    @cython.ccall
    def ShowPartitionNames(self) -> tuple[str]:
        """[sync] Show all the partition names of the TimeTable
        (sorted by partition ordinal position) `<'tuple[str]'>`.

        ## Example
        >>> db.tb.ShowPartitionNames()
        >>> ('past', 'y2023', 'y2024', 'future')
        """
        self._assure_ready()
        res: tuple = self._partitioning.ShowPartitionNames()
        if "future" in res and "past" in res:
            return res
        self._raise_partitioning_broken_error()

    @cython.ccall
    def ShowPartitionRows(self) -> dict[str, int]:
        """[sync] Show the number of estimated rows in
        each partition of the TimeTable `<'dict[str, int]'>`.

        ## Notice
        - This method will first execute `ANALYZE TABLE`, so it might
          take some time for a large TimeTable.
        - The rows count is an estimate, (possibly) not the exact value.

        ## Example
        >>> db.tb.ShowPartitionRows()
        >>> {'past': 0, 'y2023': 1, 'y2024': 1, 'future': 0}
        """
        self._assure_ready()
        res: dict = self._partitioning.ShowPartitionRows()
        if dict_contains(res, "future") and dict_contains(res, "past"):
            return res
        self._raise_partitioning_broken_error()

    @cython.ccall
    def GetBoundaryPartitionTime(self, future: cython.bint = True) -> _Pydt:
        """[sync] Retrieve the time value that the boundary 'in-range' partition represents `<'Pydt'>`.

        :param future `<'bool'>`: Retrieve the latest 'in-range' partition, else the earliest.
        :returns `<'Pydt'>`: The time value that the boundary 'in-range' partition
            represents. Should not be interpreted as the data value in the time column.
        """
        pt_names: tuple = self.ShowPartitionNames()
        pt_count: cython.Py_ssize_t = tuple_len(pt_names)
        if pt_count < 3:
            self._raise_partitioning_broken_error()
        if future:
            return self._parse_partition_time(pt_names[pt_count - 2])
        else:
            return self._parse_partition_time(pt_names[1])

    @cython.ccall
    @cython.exceptval(-1, check=False)
    def _GetInRangePartitionCount(self) -> cython.int:
        """(internal) [sync] Get the number of existing 'in-range' partitions `<'int'>`."""
        pt_names: tuple = self.ShowPartitionNames()
        pt_count: cython.Py_ssize_t = tuple_len(pt_names)
        if pt_count < 3:
            self._raise_partitioning_broken_error()
        return pt_count - 2  # exclude 'past' and 'future'

    @cython.ccall
    def _GetOverflowBoundaryValue(self, future: cython.bint) -> object:
        """(internal) [sync] Retrieve the min/max time column data
        value from an overflow partition `<'date/datetime'>`.

        :param future `<'bool'>`: Return the maximum time column data value from
            the 'future' overflow partition if `True`, else the minimum time column
            value from the 'past' overflow partition.
        """
        sql: str = self._gen_get_overflow_boundary_value_sql(future)
        with self._pool.acquire() as conn:
            with conn.transaction() as cur:
                cur.execute(sql)
                res: tuple = cur.fetchone()
        return None if res is None else res[0]

    # Async --------------------------------------------------------------------------------
    async def aioExtendToTime(
        self,
        *,
        start_from: object = None,
        end_with: object = None,
    ) -> Logs:
        """[async] Extend the TimeTable 'in-range' partitions to cover a specified time window `<'Logs'>`.

        ## Explanation
        - This method reorganizing the overflow partitions ('past' and 'future') into
          named 'in-range' partitions spanning from `start_from` to `end_with`.
        - Existing data remains intact; only the partition structure is modified.

        :param start_from `<'str/date/datetime/None'>`: New lower bound (inclusive) for 'in-range' partitions. Defaults to `None`.
            Time value is automatically aligned to the table's time unit. If `None` or
            later than the current lower bound, the start boundary is left unchanged.

        :param end_with `<'str/date/datetime/None'>`: New upper bound (inclusive) for 'in-range' partitions. Defaults to `None`.
            Time value is automatically aligned to the table's time unit. If `None` or
            earlier than the current upper bound, the end boundary is left unchanged.

        ## Example
        >>> await db.tb.aioShowPartitionRows()
        >>> {"past": 0, "y2024": 1, "future": 1}
        >>> await db.tb.aioExtendToTime(start_from="2023-01-01", end_with="2025-01-01")
            # Add 'in-range' partitions to cover data from year 2022 to 2025.
        >>> await db.tb.aioShowPartitionRows()
        >>> {"past": 0, "y2023": 0, "y2024": 1, "y2025": 1, "future": 0}
        """
        logs: Logs = Logs()

        # Extend to past
        if start_from is not None:
            logs.extend(await self._aioExtendToTime(False, start_from))

        # Extend to future
        if end_with is not None:
            logs.extend(await self._aioExtendToTime(True, end_with))

        # Finished
        return logs

    async def _aioExtendToTime(self, future: cython.bint, to_time: object) -> Logs:
        """(internal) [async] Extend the TimeTable 'in-range' partitions to
        cover a specified time in one direction `<'Logs'>`.

        ## Explanation
        - This method reorganizes the corresponding overflow partition ('past' or 'future')
          into one or more 'in-range' partitions up to the given boundary time.
        - Existing data remains intact; only the partition structure is modified.

        :param future `<'bool'>`: Extend the upper 'in-range' partition if `True`, else exetend the lower one.
        :param to_time `<'str/date/datetime'>`: New inclusive boundary for the 'in-range' partitions.
            Time value is automatically aligned to the table's time unit.
        """
        logs: Logs = Logs()

        # Extend partitions [future]
        if future:
            t_time: _Pydt = self._validate_partition_time(to_time, "end_with")
            p_time: _Pydt = await self.aioGetBoundaryPartitionTime(True)
            num: cython.longlong = self._cal_partition_time_diff(t_time, p_time)
            if num < 1:
                return logs  # exit
            part: Partitioning = self._partitioning
            futr = self._create_future_partition()
            i: cython.longlong
            for i in range(num):
                next_time: _Pydt = self._shift_partition_time(p_time, i + 1)
                pt = self._create_in_range_partition(next_time)
                logs.extend(await part.aioReorganizePartition("future", pt, futr))
            return logs

        # Extend partitions [past]
        else:
            t_time: _Pydt = self._validate_partition_time(to_time, "start_with")
            p_time: _Pydt = await self.aioGetBoundaryPartitionTime(False)
            num: cython.longlong = self._cal_partition_time_diff(p_time, t_time)
            if num < 1:
                return logs  # exit
            part: Partitioning = self._partitioning
            i: cython.longlong
            for i in range(num):
                prev_time: _Pydt = self._shift_partition_time(p_time, -(i + 1))
                past = self._create_past_partition(prev_time)
                pt = self._create_in_range_partition(prev_time)
                logs.extend(await part.aioReorganizePartition("past", past, pt))
            return logs

    async def aioCoalesceToTime(
        self,
        *,
        start_from: object = None,
        end_with: object = None,
    ) -> Logs:
        """[async] Coalesce the TimeTable 'in-range' partitions to a specified time window `<'Logs'>`.

        ## Explanation
        - The method reorganizes the 'in-range' partitions into overflow partitions
          ("past" & "future"), so that the 'in-range' partitions only cover the
          time range from `start_from` to `end_with`.
        - Existing data remains intact; only the partition structure is modified.

        :param start_from `<'str/date/datetime/None'>`: New lower bound (inclusive) for 'in-range' partitions. Defaults to `None`.
            Time value is automatically aligned to the table's time unit. If `None` or
            eariler than the current lower bound, the start boundary is left unchanged.

        :param end_with `<'str/date/datetime/None'>`: New upper bound (inclusive) for 'in-range' partitions. Defaults to `None`.
            Time value is automatically aligned to the table's time unit. If `None` or
            later than the current upper bound, the end boundary is left unchanged.

        ## Example
        >>> await db.tb.aioShowPartitionRows()
        >>> {"past": 0, "y2023": 1, "y2024": 1, "y2025": 1, "future": 0}
        >>> await db.tb.aioCoalesceToTime(start_from="2024-01-01", end_with="2024-01-01")
            # Coalesce 'y2023' and 'y2025' into 'past' and 'future' partitions.
        >>> await db.tb.aioShowPartitionRows()
        >>> {"past": 1, "y2024": 1, "future": 1}
        """
        logs: Logs = Logs()

        # Coalesce from past
        if start_from is not None:
            logs.extend(await self._aioCoalesceToTime(False, start_from))

        # Coalesce from future
        if end_with is not None:
            logs.extend(await self._aioCoalesceToTime(True, end_with))

        # Finished
        return logs

    async def _aioCoalesceToTime(self, future: cython.bint, to_time: object) -> Logs:
        """(internal) [async] Coalesce the TimeTable 'in-range' partitions
        to a specific time in one direction `<'Logs'>`.

        ## Explanation
        - This method reorganizes 'in-range' partitions beyond the specified boundary
          in to the appropriate overflow partition ('past' or 'future').
        - Existing data remains intact; only the partition structure is modified.

        :param future `<'bool'>`: Coalesce 'in-range' partitions later than the 'to_time' into 'future'
            if `True`, else the ones eariler than the 'to_time' into 'past'.
        :param to_time `<'str/date/datetime'>`: New inclusive boundary for the 'in-range' partitions.
            Time value is automatically aligned to the table's time unit.
        """
        logs: Logs = Logs()

        # Validate partition count
        pt_count: cython.int = await self._aioGetInRangePartitionCount()
        if pt_count == 1:
            return logs

        # Coalesce partitions [future]
        if future:
            t_time: _Pydt = self._validate_partition_time(to_time, "end_with")
            p_time: _Pydt = await self.aioGetBoundaryPartitionTime(True)
            num: cython.longlong = self._cal_partition_time_diff(p_time, t_time)
            if num < 1:
                return logs  # exit
            if num >= pt_count:
                num = pt_count - 1  # limit to the latest partition
            part: Partitioning = self._partitioning
            futr = self._create_future_partition()
            for i in range(num):
                prev_time: _Pydt = self._shift_partition_time(p_time, -i)
                prev_name: str = self._gen_partition_name(prev_time)
                logs.extend(
                    await part.aioReorganizePartition([prev_name, "future"], futr)
                )
            return logs

        # Coalesce partitions [past]
        else:
            t_time: _Pydt = self._validate_partition_time(to_time, "start_from")
            p_time: _Pydt = await self.aioGetBoundaryPartitionTime(False)
            num: cython.longlong = self._cal_partition_time_diff(t_time, p_time)
            if num < 1:
                return logs  # exit
            if num >= pt_count:
                num = pt_count - 1  # limit to the latest partition
            part: Partitioning = self._partitioning
            for i in range(num):
                next_time: _Pydt = self._shift_partition_time(p_time, i)
                next_name: str = self._gen_partition_name(next_time)
                past = self._create_past_partition(self._gen_partition_time(next_time))
                logs.extend(
                    await part.aioReorganizePartition(["past", next_name], past)
                )
            return logs

    async def aioDropToTime(
        self,
        *,
        start_from: object = None,
        end_with: object = None,
    ) -> Logs:
        """[async] Drop the TimeTable 'in-range' partitions outside a specified time window `<'Logs'>`.

        ## Explanation
        - This method drops any 'in-range' partitions earlier than `start_from`
          or later than `end_with`, so that the 'in-range' partitions only
          cover the specifed time range.
        - When dropping in the same direction as an overflow partition
          ('past' for `start_from`, 'future' for `end_with`), the
          corresponding overflow partition is truncated rather than dropped.
        - Deleted data is lost and partition structure is modified.

        :param start_from `<'str/date/datetime/None'>`: New lower bound (inclusive) for 'in-range' partitions. Defaults to `None`.
            Time value is automatically aligned to the table's time unit. If `None` or
            earlier than the current lower bound, no earlier partitions are dropped.

        :param end_with `<'str/date/datetime/None'>`: New upper bound (inclusive) for 'in-range' partitions. Defaults to `None`.
            Time value is automatically aligned to the table's time unit. If `None` or
            later than the current upper bound, no later partitions are dropped.

        ## Example
        >>> await db.tb.aioShowPartitionRows()
        >>> {"past": 1, "y2023": 1, "y2024": 1, "y2025": 1, "future": 1}
        >>> await db.tb.aioDropToTime(start_from="2024-01-01", end_with="2024-01-01")
            # Drop 'y2023' and 'y2025' partitions and truncate both overflow partitions.
        >>> await db.tb.aioShowPartitionRows()
        >>> {"past": 0, "y2024": 1, "future": 0}
        """
        logs: Logs = Logs()

        # Drop from past
        if start_from is not None:
            logs.extend(await self._aioDropToTime(False, start_from))

        # Drop from future
        if end_with is not None:
            logs.extend(await self._aioDropToTime(True, end_with))

        # Finished
        return logs

    async def _aioDropToTime(self, future: cython.bint, to_time: object) -> Logs:
        """(internal) [async] Drop the TimeTable 'in-range' partitions outside
        of a specific time in one direction `<'Logs'>`.

        ## Explanation
        - This method drops 'in-range' partitions later than 'to_time' (if 'future=False')
          or eariler than 'to_time' (if 'future=True').
        - When dropping in the same direction as an overflow partition
          ('past' for `start_from`, 'future' for `end_with`), the
          corresponding overflow partition is truncated rather than dropped.
        - Deleted data is lost and partition structure is modified.

        :param future `<'bool'>`: Drop 'in-range' partitions later than 'to_time' if `True`,
            else the ones eariler than the 'to_time'.
        :param to_time `<'str/date/datetime'>`: New inclusive boundary for the 'in-range' partitions.
            Time value is automatically aligned to the table's time unit.
        """
        logs: Logs = Logs()

        # Validate partition count
        pt_count: cython.int = await self._aioGetInRangePartitionCount()
        if pt_count == 1:
            return logs

        # Drop partitions [future]
        if future:
            t_time: _Pydt = self._validate_partition_time(to_time, "end_with")
            p_time: _Pydt = await self.aioGetBoundaryPartitionTime(True)
            num: cython.longlong = self._cal_partition_time_diff(p_time, t_time)
            if num < 1:
                return logs  # exit
            if num >= pt_count:
                num = pt_count - 1  # limit to the latest partition
            part: Partitioning = self._partitioning
            pt_names = []
            for i in range(num):
                prev_time: _Pydt = self._shift_partition_time(p_time, -i)
                pt_names.append(self._gen_partition_name(prev_time))
            logs.extend(await part.aioDropPartition(*pt_names))
            logs.extend(await part.aioTruncatePartition("future"))
            return logs

        # Drop partitions [past]
        else:
            t_time: _Pydt = self._validate_partition_time(to_time, "start_from")
            p_time: _Pydt = await self.aioGetBoundaryPartitionTime(False)
            num: cython.longlong = self._cal_partition_time_diff(t_time, p_time)
            if num < 1:
                return logs  # exit
            if num >= pt_count:
                num = pt_count - 1  # limit to the latest partition
            part: Partitioning = self._partitioning
            pt_names = ["past"]
            for i in range(num - 1):
                next_time: _Pydt = self._shift_partition_time(p_time, i)
                pt_names.append(self._gen_partition_name(next_time))
            new_past_time: _Pydt = self._shift_partition_time(p_time, num - 1)
            old_past_name: str = self._gen_partition_name(new_past_time)
            past = self._create_past_partition(self._gen_partition_time(new_past_time))
            logs.extend(await part.aioDropPartition(*pt_names))
            logs.extend(await part.aioTruncatePartition(old_past_name))
            logs.extend(await part.aioReorganizePartition(old_past_name, past))
            return logs

    async def aioReorganizeOverflow(self, catcher: str = "future") -> Logs:
        """[async] Reorganize the overflow partitions by extending into
        'in-range' partitions based on their boundary values `<'Logs'>`.

        ## Explanation
        - This method reorganizes one or both overflow partitions ('past', 'future') into
          named 'in-range' partitions. For each specified catcher:
            - **future**: uses the maximum value in the future partition as a new upper boundary
            - **past**: uses the minimum value in the past partition as a new lower boundary
        - No existing data is lost; only the partition structure is modified.

        :param catcher `<'str'>`: Which overflow partition(s) to reorganize. Must be one of:
            - **"future"**: reorganize only the upper overflow partition.
            - **"past"**: reorganize only the lower overflow partition.
            - **"both"**: reorganize both overflow partitions.

        ## Example
        >>> await db.tb.aioShowPartitionRows()
        >>> {"past": 1, "y2024": 1, "future": 1}
        >>> await db.tb.aioReorganizeOverflow("both")
            # Reorganize 'past' and 'future' partitions into 'in-range' partitions.
        >>> await db.tb.aioShowPartitionRows()
        >>> {"past": 0, "y2023": 1, "y2024": 1, "y2025": 1, "future": 0}
        """
        # Reorganize Future
        if catcher == "future":
            time = await self._aioGetOverflowBoundaryValue(True)
            if time is None:
                return Logs()
            return await self._aioExtendToTime(True, time)

        # Reorganize Past
        if catcher == "past":
            time = await self._aioGetOverflowBoundaryValue(False)
            if time is None:
                return Logs()
            return await self._aioExtendToTime(False, time)

        # Reorganize Both
        if catcher == "both":
            logs: Logs = Logs()
            time = await self._aioGetOverflowBoundaryValue(False)
            if time is not None:
                logs.extend(await self._aioExtendToTime(False, time))
            time = await self._aioGetOverflowBoundaryValue(True)
            if time is not None:
                logs.extend(await self._aioExtendToTime(True, time))
            return logs

        # Invalid catcher
        self._raise_argument_error(
            "overflow 'catcher' must be 'future', 'past', "
            "or 'both', instead got '%s'." % catcher
        )

    async def aioShowPartitionNames(self) -> tuple[str]:
        """[async] Show all the partition names of the TimeTable
        (sorted by partition ordinal position) `<'tuple[str]'>`.

        ## Example
        >>> await db.tb.aioShowPartitionNames()
        >>> ('past', 'y2023', 'y2024', 'future')
        """
        self._assure_ready()
        res: tuple = await self._partitioning.aioShowPartitionNames()
        if "future" in res and "past" in res:
            return res
        self._raise_partitioning_broken_error()

    async def aioShowPartitionRows(self) -> dict[str, int]:
        """[async] Show the number of estimated rows in
        each partition of the TimeTable `<'dict[str, int]'>`.

        ## Notice
        - This method will first execute `ANALYZE TABLE`, so it might
          take some time for a large TimeTable.
        - The rows count is an estimate, (possibly) not the exact value.

        ## Example
        >>> await db.tb.aioShowPartitionRows()
        >>> {'past': 0, 'y2023': 1, 'y2024': 1, 'future': 0}
        """
        self._assure_ready()
        res: dict = await self._partitioning.aioShowPartitionRows()
        if dict_contains(res, "future") and dict_contains(res, "past"):
            return res
        self._raise_partitioning_broken_error()

    async def aioGetBoundaryPartitionTime(self, future: cython.bint) -> _Pydt:
        """[async] Retrieve the time value that the boundary 'in-range' partition represents `<'Pydt'>`.

        :param future `<'bool'>`: Retrieve the latest 'in-range' partition, else the earliest.
        :returns `<'Pydt'>`: The time value that the boundary 'in-range' partition
            represents. Should not be interpreted as the data value in the time column.
        """
        pt_names: tuple = await self.aioShowPartitionNames()
        pt_count: cython.Py_ssize_t = tuple_len(pt_names)
        if pt_count < 3:
            self._raise_partitioning_broken_error()
        if future:
            return self._parse_partition_time(pt_names[pt_count - 2])
        else:
            return self._parse_partition_time(pt_names[1])

    async def _aioGetInRangePartitionCount(self) -> int:
        """(internal) [async] Get the number of existing 'in-range' partitions `<'int'>`."""
        pt_names: tuple = await self.aioShowPartitionNames()
        pt_count: cython.Py_ssize_t = tuple_len(pt_names)
        if pt_count < 3:
            self._raise_partitioning_broken_error()
        return pt_count - 2  # exclude 'past' and 'future'

    async def _aioGetOverflowBoundaryValue(self, future: cython.bint) -> object:
        """(internal) [async] Retrieve the min/max time column data
        value from an overflow partition `<'date/datetime'>`.

        :param future `<'bool'>`: Return the maximum time column data value from
            the 'future' overflow partition if `True`, else the minimum time column
            value from the 'past' overflow partition.
        """
        sql: str = self._gen_get_overflow_boundary_value_sql(future)
        async with self._pool.acquire() as conn:
            async with conn.transaction() as cur:
                await cur.execute(sql)
                res: tuple = await cur.fetchone()
        return None if res is None else res[0]

    # Generate SQL -------------------------------------------------------------------------
    @cython.ccall
    def _gen_get_overflow_boundary_value_sql(self, future: cython.bint) -> str:
        """(internal) Generate SQL to select the min/max time column
        data value from an overflow partition (minimum for 'past'
        and maximum for 'future') `<'str'>`.
        """
        self._assure_ready()
        if future:
            return "SELECT MAX(%s) AS i FROM %s PARTITION (future)" % (
                self._time_column,
                self._tb_qualified_name,
            )
        else:
            return "SELECT MIN(%s) AS i FROM %s PARTITION (past)" % (
                self._time_column,
                self._tb_qualified_name,
            )

    # Time Tools ---------------------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    def _gen_partition_name(self, time: _Pydt) -> str:
        """(internal) Generate the name for a 'in-range' partition based on the given 'time' `<'str'>`."""
        if self._time_unit == utils.TIMETABLE_UNIT.YEAR:
            return "y%04d" % datetime.datetime_year(time)
        if self._time_unit == utils.TIMETABLE_UNIT.QUARTER:
            return "q%04d%d" % (
                datetime.datetime_year(time),
                time._prop_quarter(),
            )
        if self._time_unit == utils.TIMETABLE_UNIT.MONTH:
            return "m%04d%02d" % (
                datetime.datetime_year(time),
                datetime.datetime_month(time),
            )
        if self._time_unit == utils.TIMETABLE_UNIT.WEEK:
            return "w%04d%02d%02d" % (
                datetime.datetime_year(time),
                datetime.datetime_month(time),
                datetime.datetime_day(time),
            )
        if self._time_unit == utils.TIMETABLE_UNIT.DAY:
            return "d%04d%02d%02d" % (
                datetime.datetime_year(time),
                datetime.datetime_month(time),
                datetime.datetime_day(time),
            )
        if self._time_unit == utils.TIMETABLE_UNIT.HOUR:
            return "h%04d%02d%02d_%02d" % (
                datetime.datetime_year(time),
                datetime.datetime_month(time),
                datetime.datetime_day(time),
                datetime.datetime_hour(time),
            )
        if self._time_unit == utils.TIMETABLE_UNIT.MINUTE:
            return "i%04d%02d%02d_%02d%02d" % (
                datetime.datetime_year(time),
                datetime.datetime_month(time),
                datetime.datetime_day(time),
                datetime.datetime_hour(time),
                datetime.datetime_minute(time),
            )
        if self._time_unit == utils.TIMETABLE_UNIT.SECOND:
            return "s%04d%02d%02d_%02d%02d%02d" % (
                datetime.datetime_year(time),
                datetime.datetime_month(time),
                datetime.datetime_day(time),
                datetime.datetime_hour(time),
                datetime.datetime_minute(time),
                datetime.datetime_second(time),
            )
        self._raise_critical_error("time unit flag '%d' is invalid." % self._time_unit)

    @cython.cfunc
    @cython.inline(True)
    def _gen_partition_time(self, time: _Pydt) -> _Pydt:
        """(internal) Generate the exclusive upper-bound time for a TimeTable partition `<'Pydt'>`.

        ## Explanation
        - This method aligns the given time to the end of its current interval based
          on the TimeTable's time unit, then advances by one smallest unit. So that
          the partition will include all data up to but not including this time.
        - Used in the RANGE partition creation process as the LESS THAN value.

        ## Alignment Example:
        ```python
        Pydt(2024, 4, 25)               ->  Pydt(2025, 1, 1)            # time unit: 'YEAR'
        Pydt(2024, 4, 25)               ->  Pydt(2024, 7, 1)            # time unit: 'QUARTER'
        Pydt(2024, 4, 25)               ->  Pydt(2024, 5, 1)            # time unit: 'MONTH'
        Pydt(2024, 4, 25)               ->  Pydt(2024, 4, 22)           # time unit: 'WEEK'
        Pydt(2024, 4, 25, 2)            ->  Pydt(2024, 4, 26)           # time unit: 'DAY'
        Pydt(2024, 4, 25, 0, 2)         ->  Pydt(2024, 4, 25, 1)        # time unit: 'HOUR'
        Pydt(2024, 4, 25, 0, 0, 2)      ->  Pydt(2024, 4, 25, 0, 1)     # time unit: 'MINUTE'
        Pydt(2024, 4, 25, 0, 0, 0, 2)   ->  Pydt(2024, 4, 25, 0, 0, 1)  # time unit: 'SECOND'
        ```
        """
        return time.to_end_of(self._pydt_unit).add(0, 0, 0, 0, 0, 0, 0, 0, 0, 1)

    @cython.cfunc
    @cython.inline(True)
    def _parse_partition_time(self, name: str) -> _Pydt:
        """(internal) Parse partition time from the 'in-range' partition name `<'Pydt'>`.

        ## Conversion Example:
        ```python
        'y2024'             -> Pydt(2024, 1, 1)             # time unit: 'YEAR'
        'q20243'            -> Pydt(2024, 7, 1)             # time unit: 'QUARTER'
        'm202404'           -> Pydt(2024, 4, 1)             # time unit: 'MONTH'
        'w20240422'         -> Pydt(2024, 4, 22)            # time unit: 'WEEK'
        'd20240425'         -> Pydt(2024, 4, 25)            # time unit: 'DAY'
        'h20240425_02'      -> Pydt(2024, 4, 25, 2)         # time unit: 'HOUR'
        'i20240425_0202'    -> Pydt(2024, 4, 25, 2, 2)      # time unit: 'MINUTE'
        's20240425_020202'  -> Pydt(2024, 4, 25, 2, 2, 2)   # time unit: 'SECOND'
        ```
        """
        if name is None:
            self._raise_critical_error("partition name cannot be 'None'.")
        size: cython.Py_ssize_t = str_len(name)
        if size < 5:
            self._raise_critical_error("partition name '%s' is invalid." % name)

        ch: cython.Py_UCS4 = str_read(name, 0)
        try:
            # . [Y] Year
            yy = int(str_substr(name, 1, 5))
            if ch == "y":
                return Pydt(yy, 1, 1)
            # . [Q] Quarter
            if ch == "q":
                qq: cython.int = int(str_substr(name, 5, 6))
                return Pydt(yy, (qq - 1) * 3 + 1, 1)
            # . [M] Month
            mm = int(str_substr(name, 5, 7))
            if ch == "m":
                return Pydt(yy, mm, 1)
            # . [W/D] Week/Day
            dd = int(str_substr(name, 7, 9))
            if ch in ("w", "d"):
                return Pydt(yy, mm, dd)
            # . [H] Hour
            hh = int(str_substr(name, 10, 12))
            if ch == "h":
                return Pydt(yy, mm, dd, hh)
            # . [M] Minute
            mi = int(str_substr(name, 12, 14))
            if ch == "i":
                return Pydt(yy, mm, dd, hh, mi)
            # . [S] Second
            ss = int(str_substr(name, 14, 16))
            if ch == "s":
                return Pydt(yy, mm, dd, hh, mi, ss)
            # . Invalid
            raise ValueError("unsupported time identifier '%s'." % ch)
        except Exception as err:
            self._raise_critical_error("partition name '%s' is invalid." % name, err)

    @cython.cfunc
    @cython.inline(True)
    def _shift_partition_time(self, time: _Pydt, offset: cython.int) -> _Pydt:
        """(internal) Shift the partition time by a number of intervals
        according to internal time unit `<'Pydt'>`.

        :param time `<'Pydt'>`: The time value of the partition.
        :param offset `<'int'>`: The number of intervals to shift the time.
            - If positive, advance the time forward.
            - If negative, rewind the time backward.
            - If zero, returns the original time unmodified.
        """
        # No change
        if offset == 0:
            return time
        # . [Y] Year
        if self._time_unit == utils.TIMETABLE_UNIT.YEAR:
            return time.to_year(offset)
        # . [Q] Quarter
        if self._time_unit == utils.TIMETABLE_UNIT.QUARTER:
            return time.to_quarter(offset)
        # . [M] Month
        if self._time_unit == utils.TIMETABLE_UNIT.MONTH:
            return time.to_month(offset)
        # . [W] Week
        if self._time_unit == utils.TIMETABLE_UNIT.WEEK:
            return time.to_day(offset * 7)
        # . [D] Day
        if self._time_unit == utils.TIMETABLE_UNIT.DAY:
            return time.to_day(offset)
        # . [H] Hour
        if self._time_unit == utils.TIMETABLE_UNIT.HOUR:
            return time.add(0, 0, 0, 0, 0, offset)
        # . [M] Minute
        if self._time_unit == utils.TIMETABLE_UNIT.MINUTE:
            return time.add(0, 0, 0, 0, 0, 0, offset)
        # . [S] Second
        if self._time_unit == utils.TIMETABLE_UNIT.SECOND:
            return time.add(0, 0, 0, 0, 0, 0, 0, offset)
        # . Invalid
        self._raise_critical_error("time unit flag '%d' is invalid." % self._time_unit)

    @cython.cfunc
    @cython.inline(True)
    def _create_partition(self, name: str, value: _Pydt, comment: str) -> object:
        """(internal) Create a partition for the TimeTable `<'Partition'>`.

        :param name `<'str'>`: The name of the partition.
        :param value `<'Pydt'>`: The RANGE partition LESS THAN value.
        :param comment `<'str'>`: The comment of the partition.
        """
        if self._timestamp_based:
            return Partition(name, sqlfunc.UNIX_TIMESTAMP(value), comment=comment)
        else:
            return Partition(name, value, comment=comment)

    @cython.cfunc
    @cython.inline(True)
    def _create_past_partition(self, time: _Pydt) -> object:
        """(internal) Create the 'past' overflow partition for the TimeTable `<'Partition'>`."""
        return self._create_partition("past", time, "overflow")

    @cython.cfunc
    @cython.inline(True)
    def _create_future_partition(self) -> object:
        """(internal) Create the 'future' overflow partition for the TimeTable `<'Partition'>`."""
        return Partition("future", "MAXVALUE", comment="overflow")

    @cython.cfunc
    @cython.inline(True)
    def _create_in_range_partition(self, time: _Pydt) -> object:
        """(internal) Create a 'in-range' partition for the TimeTable `<'Partition'>`.

        :param time `<'Pydt'>`: The time value that the 'in-range' partition represents.
        """
        return self._create_partition(
            self._gen_partition_name(time),
            self._gen_partition_time(time),
            "in-range",
        )

    @cython.cfunc
    @cython.inline(True)
    def _cal_partition_time_diff(self, time1: _Pydt, time2: _Pydt) -> cython.longlong:
        """(internal) Compute the signed interval count between two partition times `<'int'>`.

        ## Explanation
        - This method calculates how many of this TimeTable's time-unit
          intervals separate 'time1' and 'time2' `(time1 - time2)`.
        - A positive result means 'time1' is later than 'time2', a negative
          result means it's earlier.

        :param time1 `<'Pydt'>`: The first partition time.
        :param time2 `<'Pydt'>`: The second partition time.
        """
        return time1.diff(time2, self._pydt_unit, False, "one")

    # Setter -------------------------------------------------------------------------------
    @cython.ccall
    @cython.exceptval(-1, check=False)
    def setup(
        self,
        db_name: str,
        charset: str | Charset,
        collate: str | None,
        pool: Pool,
    ) -> cython.bint:
        """Setup the table.

        :param db_name `<'str'>`: The database name of the table.
        :param charset `<'str/Charset'>`: The charset of the table.
        :param collate `<'str/None'>`: The collation of the table.
        :param pool `<'Pool'>`: The pool of the table.
        """
        BaseTable.setup(self, db_name, charset, collate, pool)
        self._el_ready = False
        self._setup_finished = False
        # Validate time column
        try:
            col: Column = self._columns[self._time_column]
        except Exception as err:
            self._raise_definition_error(
                "does not contain the specified time column '%s'." % self._time_column,
                err,
            )
        self._time_column = col._name
        if isinstance(col.definition, Define.DATE):
            if self._time_unit in (
                utils.TIMETABLE_UNIT.HOUR,
                utils.TIMETABLE_UNIT.MINUTE,
                utils.TIMETABLE_UNIT.SECOND,
            ):
                self._raise_definition_error(
                    "does not support partitioning by time unit '%s' with DATE column '%s'. "
                    "Please change the time unit to 'YEAR/QUARTER/MONTH/WEEK/DAY' or use "
                    "a DATETIME or TIMESTAMP column instead."
                    % (
                        utils.timetable_unit_flag_to_str(self._time_unit),
                        self._time_column,
                    )
                )
            self._timestamp_based = False
        elif isinstance(col.definition, Define.DATETIME):
            self._timestamp_based = False
        elif isinstance(col.definition, Define.TIMESTAMP):
            self._timestamp_based = True
        else:
            self._raise_definition_error(
                "only support 'DATE/DATETIME/TIMESTAMP' data type as "
                "the time column (partitioning key), not %s column '%s'."
                % (col.definition.data_type, self._time_column)
            )

        # Validate constraint
        cnst: Constraint
        for cnst in self._constraints._el_dict.values():
            if not isinstance(cnst, UniqueKey):
                continue
            if self._time_column not in cnst._columns:
                self._raise_definition_error(
                    "must set the time column '%s' (partitioning key) "
                    "as part of all the PRIMARY/UNIQUE KEY(s). "
                    "<'%s'> '%s' does not contain the time column."
                    % (self._time_column, cnst.__class__.__name__, cnst.symbol)
                )

        # Setup partitioning
        if self._partitioning is not None:
            self._raise_definition_error(
                "must NOT configure its own partitions, the partitioning "
                "configuration will be automatically generated."
            )
        num: cython.longlong = self._cal_partition_time_diff(
            self._end_with, self._start_from
        )
        if num < 0:
            self._end_with = self._start_from
            num = 0
        time: _Pydt = self._start_from
        # . TIMESTAMP
        if self._timestamp_based:
            part_expr = sqlfunc.UNIX_TIMESTAMP(self._time_column)
            by_columns: cython.bint = False
        # . DATE/DATETIME
        else:
            part_expr = self._time_column
            by_columns: cython.bint = True
        # . partitions
        pts = [
            self._create_past_partition(time),
            self._create_in_range_partition(time),
        ]
        i: cython.longlong
        for i in range(num):
            next_time: _Pydt = self._shift_partition_time(time, i + 1)
            pts.append(self._create_in_range_partition(next_time))
        pts.append(self._create_future_partition())
        # . setup
        part = Partitioning(part_expr).by_range(*pts, columns=by_columns)
        part.setup(self._tb_name, self._db_name, self._charset, None, self._pool)
        self._partitioning = part
        self._partitioned = 1
        # . cleanup
        self._start_from, self._end_with = None, None

        # Switch setup flag
        self._setup_finished = True
        return self._assure_ready()

    # Validate -----------------------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(-1, check=False)
    def _validate_time_unit(self, time_unit: object) -> cython.int:
        """(internal) Validate the time unit of the TimeTable,
        returns the time unit flag `<'int'>`.
        """
        try:
            return utils.timetable_unit_str_to_flag(
                utils.validate_timetable_unit(time_unit)
            )
        except Exception as err:
            self._raise_definition_error(str(err), err)

    @cython.cfunc
    @cython.inline(True)
    def _validate_pydt_unit(self, time_unit: cython.int) -> str:
        """(internal) Convert the time unit flag of the TimeTable
        to the corresponding unit of Pydt `<'str'>`.
        """
        # . [Y] Year
        if time_unit == utils.TIMETABLE_UNIT.YEAR:
            return "Y"
        # . [Q] Quarter
        if time_unit == utils.TIMETABLE_UNIT.QUARTER:
            return "Q"
        # . [M] Month
        if time_unit == utils.TIMETABLE_UNIT.MONTH:
            return "M"
        # . [W] Week
        if time_unit == utils.TIMETABLE_UNIT.WEEK:
            return "W"
        # . [D] Day
        if time_unit == utils.TIMETABLE_UNIT.DAY:
            return "D"
        # . [H] Hour
        if time_unit == utils.TIMETABLE_UNIT.HOUR:
            return "h"
        # . [M] Minute
        if time_unit == utils.TIMETABLE_UNIT.MINUTE:
            return "m"
        # . [S] Second
        if time_unit == utils.TIMETABLE_UNIT.SECOND:
            return "s"
        # . Invalid
        self._raise_critical_error("time unit flag '%d' is invalid." % self._time_unit)

    @cython.cfunc
    @cython.inline(True)
    def _validate_partition_time(self, dtobj: object, arg_name: str) -> _Pydt:
        """(internal) Validate and normalize a partition time value `<'Pydt'>`.

        This method checks if the provided time value is valid and converts it
        to a <'Pydt'> object, adjusting it to the start of the interval according
        to the internal time unit.
        """
        try:
            if dtobj is None:
                raise ValueError("time value '%s' cannot be 'None'." % arg_name)
            time: _Pydt = Pydt.parse(dtobj, ignoretz=True)
        except Exception as err:
            self._raise_definition_error(
                "time value '%s' (%s %r) is invalid." % (arg_name, type(dtobj), dtobj),
                err,
            )
        # Adjustment
        time = time.replace(-1, -1, -1, -1, -1, -1, -1, None)
        return time.to_start_of(self._pydt_unit)

    # Error --------------------------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(-1, check=False)
    def _raise_partitioning_broken_error(self, tb_exc: Exception = None) -> cython.bint:
        """(internal) Raise TimeTable partitioning broken error."""
        self._raise_operational_error(
            1505,
            "partitoning configuration is broken or the table is not created yet.\n"
            "There must be at least 3 partitions for the TimeTable: "
            "'past', 'future' and at least one 'in-range' partition in between.",
            tb_exc,
        )

    # Copy ---------------------------------------------------------------------------------
    @cython.ccall
    def copy(self) -> TimeTable:
        """Make a copy of the time table `<'TimeTable'>`."""
        try:
            tb: TimeTable = self.__class__(
                self._time_column,
                utils.timetable_unit_flag_to_str(self._time_unit),
                self._start_from,
                self._end_with,
                self._charset,
                None,
                self._comment,
                utils.read_bool_config(self._encryption),
                self._row_format,
            )
        except TypeError as err:
            if "positional arguments" in str(err):
                self._raise_critical_error(
                    "inherits from the base <'TimeTable'> class, "
                    "must not override its '__init__' method.",
                    err,
                )
            raise err
        tb.set_name(self._name)
        return tb


# Prohibited names from TimeTable class
utils.SCHEMA_ELEMENT_SETTINGS.add_prohibited_names(
    dir(TimeTable("__dummy__", "Y", "1970-01-01", "1971-01-01"))
)


# Temporary Table --------------------------------------------------------------------------------------------
@cython.cclass
class TempTable(BaseTable):
    """Represents a temporary table in a database."""

    def __init__(
        self,
        engine: str = None,
        charset: object | None = None,
        collate: object | None = None,
        comment: str = None,
        row_format: object = None,
    ):
        """The temporary table in a database.

        :param engine `<'str/None'>`: The storage ENGINE for the temporary table. Defaults to `None`.
        :param charset `<'str/Charset/None'>`: The CHARACTER SET of the temporary table. Defaults to `None`.
            If not specified (None), use the charset of the database.
        :param collate `<'str/None'>`: The COLLATION of the temporary table. Defaults to `None`.
            If not specified (None), use the collate of the database.
        :param comment `<'str/None'>`: The COMMENT of the temporary table. Defaults to `None`.
        :param row_format `<'str/None'>`: The physical format in which the rows are stored. Defaults to `None`.
            Accepts: `"COMPACT"`, `"COMPRESSED"`, `"DYNAMIC"`, `"FIXED"`, `"REDUNDANT"`, `"PAGED"`.
        """
        super().__init__(engine, charset, collate, comment, None, row_format)
        self._set_el_type("TEMPORARY TABLE")
        self._temporary = True

    # Sync ---------------------------------------------------------------------------------
    @cython.ccall
    def Create(self, if_not_exists: cython.bint = False) -> Logs:
        """[sync] Create the temporary table `<'Logs'>`.

        :param if_not_exists `<'bool'>`: Create the temporary table only if it does not exist. Defaults to `False`.
        :raises `<'OperationalError'>`: If the temporary table already exists and 'if_not_exists=False'.

        ## Notice
        - This method is guaranteed to only affects the TEMPORARY table:
          operations on this method (and its corresponding `Drop()`)
          will not touch any permanent table with the same name.
        """
        self._assure_sync_connection_ready()
        sql: str = self._gen_create_sql(if_not_exists)
        with self._sync_conn.cursor() as cur:
            cur.execute(sql)
        logs = Logs().log_sql(self, sql)
        return logs.log_element_creation(self, False)

    @cython.ccall
    def Drop(self, if_exists: cython.bint = False) -> Logs:
        """[sync] Drop the temporary table `<'Logs'>`.

        :param if_exists `<'bool'>`: Drop the temporary table only if it exists. Defaults to `False`.
        :raises `<'OperationalError'>`: If the temporary table does not exists and 'if_exists=False'.

        ## Notice
        - This method is guarantee to only affects the TEMPORARY table:
          operations on this method (and the corresponding `Create()`)
          will not touch any permanent table with the same name.
        """
        self._assure_sync_connection_ready()
        sql: str = self._gen_drop_sql(if_exists)
        with self._sync_conn.cursor() as cur:
            try:
                cur.execute(sql)
            except:  # noqa
                self._sync_conn.schedule_close()
                raise
        logs = Logs().log_sql(self, sql)
        return logs.log_element_deletion(self, False)

    @cython.ccall
    @cython.exceptval(-1, check=False)
    def Empty(self) -> cython.bint:
        """[sync] Check if the temporary table is empty `<'bool'>`.

        ## Notice
        - This method does `NOT` guarantee to only check the TEMPORARY table:
          If the temporary table does not exist and a permanent table with the
          same name is in the database, it will checks the permanent table and
          return its empty state instead.
        """
        self._assure_sync_connection_ready()
        sql: str = self._gen_empty_sql()
        with self._sync_conn.cursor() as cur:
            cur.execute(sql)
            res = cur.fetchone()
        return res is None

    # Async --------------------------------------------------------------------------------
    async def aioCreate(self, if_not_exists: cython.bint = False) -> Logs:
        """[async] Create the temporary table `<'Logs'>`.

        :param if_not_exists `<'bool'>`: Create the temporary table only if it does not exist. Defaults to `False`.
        :raises `<'OperationalError'>`: If the temporary table already exists and 'if_not_exists=False'.

        ## Notice
        - This method is guaranteed to only affects the TEMPORARY table:
          operations on this method (and its corresponding `Drop()`)
          will not touch any permanent table with the same name.
        """
        self._assure_async_connection_ready()
        sql: str = self._gen_create_sql(if_not_exists)
        async with self._async_conn.cursor() as cur:
            await cur.execute(sql)
        logs = Logs().log_sql(self, sql)
        return logs.log_element_creation(self, False)

    async def aioDrop(self, if_exists: cython.bint = False) -> Logs:
        """[async] Drop the temporary table `<'Logs'>`.

        :param if_exists `<'bool'>`: Drop the temporary table only if it exists. Defaults to `False`.
        :raises `<'OperationalError'>`: If the temporary table does not exists and 'if_exists=False'.

        ## Notice
        - This method is guarantee to only affects the TEMPORARY table:
          operations on this method (and the corresponding `Create()`)
          will not touch any permanent table with the same name.
        """
        self._assure_async_connection_ready()
        sql: str = self._gen_drop_sql(if_exists)
        async with self._async_conn.cursor() as cur:
            try:
                await cur.execute(sql)
            except:  # noqa
                self._async_conn.schedule_close()
                raise
        logs = Logs().log_sql(self, sql)
        return logs.log_element_deletion(self, False)

    async def aioEmpty(self) -> bool:
        """[async] Check if the temporary table is empty `<'bool'>`.

        ## Notice
        - This method does `NOT` guarantee to only check the TEMPORARY table:
          If the temporary table does not exist and a permanent table with the
          same name is in the database, it will checks the permanent table and
          return its empty state instead.
        """
        self._assure_async_connection_ready()
        sql: str = self._gen_empty_sql()
        async with self._async_conn.cursor() as cur:
            await cur.execute(sql)
            res = await cur.fetchone()
        return res is None

    # Connection ---------------------------------------------------------------------------
    @cython.ccall
    @cython.exceptval(-1, check=False)
    def _set_connection(self, conn: PoolConnection | PoolSyncConnection) -> cython.bint:
        """(internal) Set (assign) the connection of the TEMPOARAY TABLE.

        :param conn `<'PoolConnection/PoolSyncConnection'>`: The connection of the temporary table.
        - `<'PoolConnection'>`: The [async] connection for asynchronized operation.
        - `<'PoolSyncConnection'>`: The [sync] connection for synchronized operation.
        """
        if isinstance(conn, PoolConnection):
            self._async_conn = conn
        elif isinstance(conn, PoolSyncConnection):
            self._sync_conn = conn
        else:
            raise self._raise_argument_error(
                "expects an instance of <'PoolSyncConnection/PoolConnection'> "
                "as its connection, instead got %s." % type(conn)
            )
        return True

    @cython.ccall
    @cython.exceptval(-1, check=False)
    def _del_connection(self) -> cython.bint:
        """(internal) Delete the connection assigned to the temporary table."""
        if self._sync_conn is not None:
            self._sync_conn = None
        if self._async_conn is not None:
            self._async_conn = None
        return True

    # Assure Ready -------------------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(-1, check=False)
    def _assure_sync_connection_ready(self) -> cython.bint:
        """Assure the [sync] connection has been assigned."""
        if self._sync_conn is None:
            self._raise_critical_error("has not been assigned a [sync] connection.")
        return True

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(-1, check=False)
    def _assure_async_connection_ready(self) -> cython.bint:
        """Assure the [async] connection has been assigned."""
        if self._async_conn is None:
            self._raise_critical_error("has not been assigned an [async] connection.")
        return True

    # Copy ---------------------------------------------------------------------------------
    @cython.ccall
    def copy(self) -> TempTable:
        """Make a copy of the TempTable `<'TempTable'>`."""
        try:
            tb: TempTable = self.__class__(
                self._engine,
                self._charset,
                None,
                self._comment,
                self._row_format,
            )
        except TypeError as err:
            if "positional arguments" in str(err):
                self._raise_critical_error(
                    "inherits from the base <'TempTable'> class, "
                    "must not override its '__init__' method.",
                    err,
                )
            raise err
        tb.set_name(self._name)
        return tb

    # Special Method -----------------------------------------------------------------------
    def __del__(self):
        self._del_connection()


# Prohibited names from TempTable class
utils.SCHEMA_ELEMENT_SETTINGS.add_prohibited_names(dir(TempTable()))


@cython.cclass
class TempTableManager:
    """The context manager for creating and cleaning up a TempTable.

    ## Explanation
    - 1. On entering the context, create the TEMPORARY TABLE.
    - 2. On exiting the context, drop the TEMPORARY TABLE. If the drop fails,
         close the connection to ensure the relating resourse is released.
    """

    _temp_table: TempTable
    _sync_mode: cython.bint

    def __init__(
        self,
        temp_table: TempTable,
        conn: PoolConnection | PoolSyncConnection,
    ):
        """The Context manager for creating and cleaning up a TempTable.

        ## Explanation
        - 1. On entering the context, create the TEMPORARY TABLE.
        - 2. On exiting the context, drop the TEMPORARY TABLE. If the drop fails,
             close the connection to ensure the relating resourse is released.

        :param temp_table `<'TempTable'>`: The temporary table instance.
        :param conn `<'PoolConnection/PoolSyncConnection'>`: The connection to assign to the temporary table.
        - `<'PoolConnection'>`: The [async] connection for asynchronized operation.
        - `<'PoolSyncConnection'>`: The [sync] connection for synchronized operation.
        """
        if temp_table is None:
            raise errors.TableCriticalError(
                "<'%s'> expects an instance of <'TempTable'> for the argument 'temp_table', "
                "instead got %s." % (self.__class__.__name__, type(temp_table))
            )
        temp_table._set_connection(conn)
        self._sync_mode = temp_table._sync_conn is not None
        self._temp_table = temp_table

    # Sync --------------------------------------------------------------------------------
    def __enter__(self) -> TempTable:
        if not self._sync_mode:
            raise errors.TableCriticalError(
                "<'%s'> The connection assigned to the <'TempTable'> is an [async] connection, "
                "please use Python 'async with' statement to create the TEMPORARY TABLE."
                % self._temp_table.__class__.__name__
            )
        self._temp_table.Create(False)
        return self._temp_table

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self._temp_table.Drop(True)
        finally:
            self._cleanup()

    # Async -------------------------------------------------------------------------------
    async def __aenter__(self) -> TempTable:
        if self._sync_mode:
            raise errors.TableCriticalError(
                "<'%s'> The connection assigned to the <'TempTable'> is a [sync] connection, "
                "please use Python 'with' statement to create the TEMPORARY TABLE."
                % self._temp_table.__class__.__name__
            )
        await self._temp_table.aioCreate(False)
        return self._temp_table

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        try:
            await self._temp_table.aioDrop(True)
        finally:
            self._cleanup()

    # Special Method ----------------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(-1, check=False)
    def _cleanup(self) -> cython.bint:
        """(internal) Cleanup the context manager."""
        if self._temp_table is not None:
            self._temp_table._del_connection()
            self._temp_table = None
        return True

    def __del__(self):
        self._cleanup()


# Tables -----------------------------------------------------------------------------------------------------
@cython.cclass
class Tables(Elements):
    """Represents a collection of tables in a database.

    Works as a dictionary where keys are the table names
    and values the table instances.
    """

    def __init__(self, *tables: Table):
        """The collection of tables in a database.

        Works as a dictionary where keys are the table names
        and values the table instances.

        :param tables `<'*Table'>`: The tables in a database.
        """
        super().__init__("TABLE", "TABLES", Table, *tables)

    # Property -----------------------------------------------------------------------------
    @property
    def qualified_names(self) -> tuple[str]:
        """The qualified table names '{db_name}.{tb_name}' `<'tuple[str]'>`."""
        tb: Table
        return tuple([tb._tb_qualified_name for tb in self._sorted_elements()])

    # Setter -------------------------------------------------------------------------------
    @cython.ccall
    @cython.exceptval(-1, check=False)
    def setup(
        self,
        db_name: str,
        charset: str | Charset,
        collate: str | None,
        pool: Pool,
    ) -> cython.bint:
        """Setup the table collection.

        :param db_name `<'str'>`: The database name of the table collection.
        :param charset `<'str/Charset'>`: The charset of the table collection.
        :param collate `<'str/None'>`: The collation of the table collection.
        :param pool `<'Pool'>`: The pool of the table collection.
        """
        # Collection
        self._set_db_name(db_name)
        self._set_charset(charset, collate)
        self._set_pool(pool)
        # Elements
        db_name = self._db_name
        charset = self._charset
        pool = self._pool
        el: Table
        for el in self._el_dict.values():
            if not el._el_ready:
                el.setup(db_name, charset, None, pool)
        return self._assure_ready()

    # Copy ---------------------------------------------------------------------------------
    @cython.ccall
    def copy(self) -> Tables:
        """Make a copy of the table collection `<'Tables'>`."""
        el: Table
        return Tables(*[el.copy() for el in self._el_dict.values()])


# Metadata ---------------------------------------------------------------------------------------------------
@cython.cclass
class TableMetadata(Metadata):
    """Represents the metadata from the remote of a table."""

    # Base data
    _db_name: str
    _tb_name: str
    _engine: str
    _row_format: str
    _charset: Charset
    _options: str
    _comment: str
    # Addtional data
    _encryption: cython.bint
    _partitioned: cython.bint

    def __init__(self, meta: dict):
        """The metadata from the remote server of a table.

        :param meta `<'dict'>`: A dictionary contains the following database metadata:
        ```python
        - "CATALOG_NAME"
        - "SCHEMA_NAME"
        - "TABLE_NAME"
        - "TABLE_TYPE"
        - "ENGINE"
        - "VERSION"
        - "ROW_FORMAT"
        - "TABLE_ROWS"
        - "AVG_ROW_LENGTH"
        - "DATA_LENGTH"
        - "MAX_DATA_LENGTH"
        - "INDEX_LENGTH"
        - "DATA_FREE"
        - "AUTO_INCREMENT"
        - "CREATE_TIME"
        - "UPDATE_TIME"
        - "CHECK_TIME"
        - "TABLE_COLLATION"
        - "CHECKSUM"
        - "CREATE_OPTIONS"
        - "TABLE_COMMENT"
        - "ENGINE_ATTRIBUTE"
        - "SECONDARY_ENGINE_ATTRIBUTE"
        ```
        """
        super().__init__("TABLE", meta, 23)
        try:
            # Base data
            self._db_name = meta["SCHEMA_NAME"]
            self._tb_name = meta["TABLE_NAME"]
            self._engine = utils.validate_engine(meta["ENGINE"])
            self._row_format = utils.validate_row_format(meta["ROW_FORMAT"])
            self._charset = utils.validate_charset(None, meta["TABLE_COLLATION"])
            self._options = meta["CREATE_OPTIONS"]
            self._comment = utils.validate_comment(meta["TABLE_COMMENT"])
            # Additional data
            if self._options is None or self._options == "":
                self._encryption = False
                self._partitioned = False
            else:
                self._encryption = utils.validate_str_contains(
                    self._options, "ENCRYPTION='Y'"
                )
                self._partitioned = utils.validate_str_contains(
                    self._options, "PARTITIONED"
                )
        except Exception as err:
            self._raise_invalid_metadata_error(meta, err)
        # Adjustment
        dict_setitem(self._meta, "ENGINE", self._engine)
        dict_setitem(self._meta, "ROW_FORMAT", self._row_format)

    # Property -----------------------------------------------------------------------------
    @property
    def catelog_name(self) -> str:
        """The catalog name of the table `<'str'>`."""
        return self._meta["CATALOG_NAME"]

    @property
    def db_name(self) -> str:
        """The schema name of the table `<'str'>`."""
        return self._db_name

    @property
    def tb_name(self) -> str:
        """The name of the table `<'str'>`."""
        return self._tb_name

    @property
    def tb_type(self) -> str:
        """The type of the table `<'str'>`.

        Expects one of: "BASE TABLE", "VIEW", "SYSTEM VIEW"
        """
        return self._meta["TABLE_TYPE"]

    @property
    def engine(self) -> str:
        """The storage ENGINE of the table `<'str'>`."""
        return self._engine

    @property
    def version(self) -> int:
        """The internal version number of the table's structure
        (unrelated to MySQL server version) `<'int'>`.
        """
        return self._meta["VERSION"]

    @property
    def row_format(self) -> str | None:
        """The physical format in which the rows are stored (InnoDB) `<'str/None'>`."""
        return self._row_format

    @property
    def table_rows(self) -> int:
        """The esitimated or stored number of rows in the table `<'int'>`.

        - For InnoDB, this is typically an approximate count, especially if
          there hasn't been any data inserted or if statistics haven't been
          updated recently.
        """
        return self._meta["TABLE_ROWS"]

    @property
    def avg_row_length(self) -> int:
        """The average row length (in bytes). Calculated (in a rough sense)
        as DATA_LENGTH / TABLE_ROWS. `<'int'>`.

        - With TABLE_ROWS = 0, this defaults to 0.
        - If there were rows, this would be a rough estimate of how many
          bytes each row occupies on average.
        """
        return self._meta["AVG_ROW_LENGTH"]

    @property
    def data_length(self) -> int:
        """The size (in bytes) of the data portion of the table on disk `<'int'>`."""
        return self._meta["DATA_LENGTH"]

    @property
    def max_data_length(self) -> int | None:
        """The maximum data length possible for the table `<'int/None'>`.

        - In some storage engines, this can show how large the table can grow
          (for example, MyISAM might show a nonzero value).
        - For InnoDB, it is often 0, as InnoDB can grow dynamically based on
          the file or tablespace size.
        """
        return self._meta["MAX_DATA_LENGTH"]

    @property
    def index_length(self) -> int:
        """The amount of space (in bytes) used by indexes for the table `<'int'>`."""
        return self._meta["INDEX_LENGTH"]

    @property
    def data_free(self) -> int:
        """The amount of free space (in bytes) within the tablespace
        allocated to this table `<'int/None'>`.

        - For InnoDB, this is often 0 or not particularly meaningful until
          data is inserted and then deleted, creating free extents inside
          the tablespace.
        """
        return self._meta["DATA_FREE"]

    @property
    def auto_increment(self) -> int | None:
        """The next AUTO_INCREMENT value to be assigned to a new row `<'int/None'>`.

        - If the table has an auto-increment column, the next inserted
          record will get this value.
        - If no such column exists, this might be a default or unused.
        """
        return self._meta["AUTO_INCREMENT"]

    @property
    def create_time(self) -> object:
        """The timestamp when the table was created `<'datetime.datetime'>`."""
        return self._meta["CREATE_TIME"]

    @property
    def update_time(self) -> object | None:
        """The timestamp when the table was last updated `<'datetime.datetime/None'>`."""
        return self._meta["UPDATE_TIME"]

    @property
    def check_time(self) -> object | None:
        """The timestamp when the table was last checked for
        errors or repaired `<'datetime.datetime/None'>`.
        """
        return self._meta["CHECK_TIME"]

    @property
    def charset(self) -> Charset:
        """The character set of the table `<'Charset'>`."""
        return self._charset

    @property
    def check_sum(self) -> int | None:
        """Indicates whether a table-level checksum is being maintained `<'int/None'>`.

        - Typically None unless the table/engine explicitly
          supports and has checksums enabled.
        """
        return self._meta["CHECKSUM"]

    @property
    def options(self) -> str:
        """Shows any additional options specified at table creation `<'str'>`."""
        return self._options

    @property
    def comment(self) -> str:
        """The comment of the table `<'str'>`."""
        return self._comment

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
    def encryption(self) -> bool:
        """The encryption behavior of the table `<'bool'>`.

        - Indicates whether this table is configured to use encryption
          at rest if the MySQL server supports encryption.
        - `True` means that the server will attempt to encrypt the table
          (assuming the server's encryption settings are properly configured).
        """
        return self._encryption

    @property
    def partitioned(self) -> bool:
        """Whether the table is partitioned `<'bool'>`."""
        return self._partitioned
