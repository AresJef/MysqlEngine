# cython: language_level=3
# cython: wraparound=False
# cython: boundscheck=False
from __future__ import annotations

# Cython imports
import cython
from cython.cimports.sqlcycli.charset import Charset  # type: ignore
from cython.cimports.sqlcycli.aio.pool import PoolConnection, PoolSyncConnection  # type: ignore
from cython.cimports.mysqlengine.element import Element, Elements, Logs, Metadata, Query  # type: ignore
from cython.cimports.mysqlengine.table import Table, TimeTable, TempTable, TempTableManager, Tables  # type: ignore
from cython.cimports.mysqlengine.dml import SelectDML, InsertDML, ReplaceDML, UpdateDML, DeleteDML, WithDML  # type: ignore
from cython.cimports.mysqlengine import utils  # type: ignore

# Python imports
from typing import Iterator
from asyncio import gather as _aio_gather
from sqlcycli.charset import Charset
from sqlcycli.aio import DictCursor as AioDictCursor
from sqlcycli import errors as sqlerrors, Pool, DictCursor
from sqlcycli.aio.pool import PoolConnection, PoolSyncConnection
from mysqlengine.element import Element, Elements, Logs, Metadata, Query
from mysqlengine.table import Table, TimeTable, TempTable, TempTableManager, Tables
from mysqlengine.dml import (
    SelectDML,
    InsertDML,
    ReplaceDML,
    UpdateDML,
    DeleteDML,
    WithDML,
)
from mysqlengine import utils


__all__ = [
    "Database",
    "DatabaseMetadata",
]


# Database ---------------------------------------------------------------------------------------------------
@cython.cclass
class Database(Element):
    """Represents a database."""

    # . configs
    _encryption: cython.int
    _read_only: cython.bint
    # . internal
    _tables: Tables
    _setup_finished: cython.bint

    def __init__(
        self,
        name: str,
        pool: Pool,
        charset: object = "utf8mb4",
        collate: object = None,
        encryption: bool | None = None,
    ):
        """The database.

        :param name `<'str'>`: The name of the database.
        :param pool `<'Pool'>`: The connection Pool for the remote server hosting the database.
        :param charset `<'str/Charset'>`: The CHARACTER SET of the database. Defaults to `'utf8mb4'`.
        :param collate `<'str/None'>`: The COLLATION of the database. Defaults to `None`.
        :param encryption `<'bool/None'>`: The default database ENCRYPTION behavior. Defaults to `None`.
            - `None`: use remote server default settings 'default_table_encryption'.
            - `True/False`: enabled/disable encryption.
        """
        super().__init__("DATABASE", "DATABASE")
        # . settings
        self.set_name(name)
        self._charset = self._validate_charset(charset, collate)
        if self._charset is None:
            self._raise_definition_error(
                "charset or collate is invalid.\n"
                "Please specify a valid CHARACTER SET and COLLATION (optional)."
            )
        # . configs
        self._encryption = self._validate_encryption(encryption)
        self._read_only = False
        # . internal
        self._setup_finished = False
        self._pool = self._validate_pool(pool)
        # . setup
        if self._name is not None:
            self.setup()

    # Property -----------------------------------------------------------------------------
    @property
    def db_name(self) -> str:
        """The name of the database `<'str'>`."""
        self._assure_ready()
        return self._name

    @property
    def encryption(self) -> bool | None:
        """The default database ENCRYPTION behavior `<'bool/None'>`."""
        return utils.read_bool_config(self._encryption)

    @property
    def read_only(self) -> bool:
        """Whether the database is in READ_ONLY mode `<'bool'>`."""
        return self._read_only

    # . tables
    @property
    def tables(self) -> Tables:
        """The table collection of the database `<'Tables'>`."""
        self._assure_ready()
        return self._tables

    # DML ----------------------------------------------------------------------------------
    def Select(
        self,
        *expressions: object,
        distinct: cython.bint = False,
        high_priority: cython.bint = False,
        straight_join: cython.bint = False,
        sql_buffer_result: cython.bint = False,
    ) -> SelectDML:
        """Construct a SELECT statement `<'SelectDML'>`.

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

        ## Notice
        At the end of the chain:
        - Call `statement()` method to compose the statement <'str'>.
        - Call `Execute()` method to execute the statement synchronously.
        - Call `aioExecute()` method to execute the statement asynchronously.

        ## Example [sync]
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

        ## Example [async]
        >>> data = await db.Select("*").From(db.tb).aioExecute()
            # Equivalent to:
            SELECT * FROM db.tb;
        """
        self._assure_ready()
        return SelectDML(self._db_name, self._pool)._Select(
            expressions,
            distinct,
            high_priority,
            straight_join,
            sql_buffer_result,
        )

    @cython.ccall
    def Insert(
        self,
        table: object,
        partition: object = None,
        ignore: cython.bint = False,
        priority: object = None,
    ) -> InsertDML:
        """Construct an INSERT statement `<'InsertDML'>`.

        :param table `<'str/Table'>`: The table to insert the data.

        :param partition `<'str/Partition/list/tuple/None'>`: The partition(s) of the table to insert the data. Defaults to `None`.

        :param ignore `<'bool'>`: Whether to ignore the ignorable errors that occurs while executing the statement. Defaults to `False`.
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

        ## Notice
        At the end of the chain:
        - Call `statement()` method to compose the statement <'str'>.
        - Call `Execute()` method to execute the statement synchronously.
        - Call `aioExecute()` method to execute the statement asynchronously.

        ## Example [sync] (VALUES)
        >>> (
                db.Insert(db.tb)
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
                db.Insert(db.tb)
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
                await db.Insert(db.tb)
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
                await db.Insert(db.tb1)
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
        return InsertDML(self._db_name, self._pool).Insert(
            table, partition, ignore, priority
        )

    @cython.ccall
    def Replace(
        self,
        table: object,
        partition: object = None,
        low_priority: cython.bint = False,
    ) -> ReplaceDML:
        """Construct a REPLACE statement `<'ReplaceDML'>`.

        REPLACE is a MySQL extension to the SQL standard and works exactly like
        INSERT, except that if an old row in the table has the same value as a
        new row for a PRIMARY KEY or a UNIQUE index, the old row is deleted
        before the new row is inserted.

        :param table `<'str/Table'>`: The table to replace the data.

        :param partition `<'str/Partition/list/tuple/None'>`: The partition(s) of the table to replace the data. Defaults to `None`.

        :param low_priority `<'bool'>`: Whether to enable the optional `LOW_PRIORITY` modifier. Defaults to `False`.
            `LOW_PRIORITY`: Delays the REPLACE until no other clients are reading the table
            (even those who start reading while your REPLACE is waiting). Disables concurrent
            inserts—so it can block for a very long time and is normally not recommended on
            MyISAM tables. Only applies to table-locking engines (MyISAM, MEMORY, MERGE).

        :returns `<'ReplaceDML'>`: The DML Replace statement.

        ## Notice
        At the end of the chain:
        - Call `statement()` method to compose the statement <'str'>.
        - Call `Execute()` method to execute the statement synchronously.
        - Call `aioExecute()` method to execute the statement asynchronously.

        ## Example [sync] (VALUES)
        >>> (
                db.Replace(db.tb)
                .Columns("id", "name")
                .Values(2)
                .Execute([(1, "John"), (2, "Sarah")], many=True)
            )
            # Equivalent to:
            REPLACE INTO db.tb (id, name)
            VALUES (1,'John'),(2,'Sarah')

        ## Example [async] (SET)
        >>> (
                await db.Replace(db.tb)
                .Set("id=%s", "name=%s")
                .aioExecute([1, "John"], many=False)
            )
            # Equivalent to:
            REPLACE INTO db.tb
            SET id=1, name='John'

        ## Example [async] (SELECT)
        >>> (
                await db.Replace(db.tb1)
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
        return ReplaceDML(self._db_name, self._pool).Replace(
            table, partition, low_priority
        )

    @cython.ccall
    def Update(
        self,
        table: object,
        partition: object = None,
        ignore: cython.bint = False,
        low_priority: cython.bint = False,
        alias: object = None,
    ) -> UpdateDML:
        """Construct a UPDATE statement `<'UpdateDML'>`.

        :param table `<'str/Table'>`: The table from which to update data.
            Only accepts one table. For multiple-table JOIN, please use the explicit JOIN clause instead.

        :param partition `<'str/Partition/list/tuple/None'>`: The partition(s) of the table to update. Defaults to `None`.

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

        At the end of the chain:
        - Call `statement()` method to compose the statement <'str'>.
        - Call `Execute()` method to execute the statement synchronously.
        - Call `aioExecute()` method to execute the statement asynchronously.

        ## Example [async] (single-table)
        >>> (
                await db.Update(db.tb)
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
                db.Update(db.tb1)
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
        return UpdateDML(self._db_name, self._pool).Update(
            table, partition, ignore, low_priority, alias
        )

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
        """Construct a DELETE statement `<'DeleteDML'>`.

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

        At the end of the chain:
        - Call `statement()` method to compose the statement <'str'>.
        - Call `Execute()` method to execute the statement synchronously.
        - Call `aioExecute()` method to execute the statement asynchronously.

        ## Example [sync] (single-table)
        >>> (
                db.Delete(db.tb)
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
                await db.Delete(db.tb1, multi_tables=["t0", "t1"])
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
        return DeleteDML(self._db_name, self._pool).Delete(
            table, partition, ignore, low_priority, quick, alias, multi_tables
        )

    def With(
        self,
        name: object,
        subquery: object,
        *columns: object,
        recursive: cython.bint = False,
    ) -> WithDML:
        """Construct a DML statement starts with CTE (Common Table Expressions) `<'WithDML'>`.

        :param name `<'str'>`: The name of the CTE, which can be used as a table reference in the statement.
        :param subquery `<'str/SelectDML'>`: The subquery that produces the CTE result set.
        :param columns `<'*str/Column'>`: The column names of the result set.
            If specified, the number of columns must be the same as the result set of the subquery.
        :param recursive `<'bool'>`: Whether the CTE is recursive. Defaults to `False`.
            A CTE is recursive if its subquery refers to its own name.
            For more information, refer to MySQL documentation
            ["Recursive Common Table Expressions"](https://dev.mysql.com/doc/refman/8.4/en/with.html#common-table-expressions-recursive).
        :returns `<'WithDML'>`: The DML statement starts with CTE.

        ## Notice
        - For multiple CTEs, chain the With() method after another With().

        At the end of the chain:
        - Call `statement()` method to compose the statement <'str'>.
        - Call `Execute()` method to execute the statement synchronously.
        - Call `aioExecute()` method to execute the statement asynchronously.

        ## Example [sync] (SELECT)
        >>> (
                db.With("cte1", db.Select("id", "name").From(db.tb1))
                .With("cte2", db.Select("id", "name").From(db.tb2))
                .Select("*")
                .From("cte1")
                .Union(db.Select("*").From("cte2"))
                .Execute()
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

        ## Example [async] (UPDATE)
        >>> (
                await db.With("cte", db.Select("*").From(db.tb2))
                .Update(db.tb1)
                .Join("cte", "t0.id=t1.id")
                .Set("t0.name=t1.name", "t0.price=t1.price")
                .aioExecute()
            )
            # Equivalent to:
            WITH cte AS (
                SELECT * FROM db.tb2 AS t0
            )
            UPDATE db.tb1 AS t0
            INNER JOIN cte AS t1 ON t0.id=t1.id
            SET t0.name=t1.name, t0.price=t1.price

        ### Example [async] (DELETE)
        >>> (
                await db.With("cte", db.Select("*").From(db.tb2))
                .Delete(db.tb1, multi_tables=["t0"])
                .Join("cte", "t0.id=t1.id")
                .aioExecute()
            )
            # Equivalent to:
            WITH cte AS (
                    SELECT * FROM db.tb2 AS t0
            )
            DELETE t0 FROM db.tb1 AS t0
            INNER JOIN cte AS t1 ON t0.id=t1.id
        """
        self._assure_ready()
        return WithDML(self._db_name, self._pool)._With(
            name, subquery, columns, recursive
        )

    @cython.ccall
    def CreateTempTable(
        self,
        conn: object,
        name: object,
        temp_table: object,
    ) -> TempTableManager:
        """Create a temporary table `<'TempTableManager'>`.

        :param conn `<'PoolConnection/PoolSyncConnection'>`: The connection to assign to the temporary table.
        :param name `<'str'>`: The name of the temporary table.
        :param temp_table `<'TempTable'>`: The temporary table instance.

        :returns `<'TempTableManager'>`: The temporary table context manager.

        ## Explanation
        - A connection must be assigned to the temporary table and will be used
          to execute all the built-in methods during the temporary table life-cycle.
        - Choosing the correct [sync/async] connection type is important. Once
          the connection is set, only the corresponding [sync/async] methods
          can be used to interact with the temporary table. Calling the wrong
          [sync/async] methods will raise an error.
        - Only create the temporary table through context manager.
        - The temporary will be automatically created and dropped when the context
          manager is entered and exited. So the temporary table only exists within
          the `[with / async with]` block.
        - If the context manager fails to drop the temporary table, the connection
          will be closed when release back to pool, to ensure the relating resources
          is always released.

        ## Example [sync]
        >>> with db.transaction() as conn:
                with db.CreateTempTable(conn, "tmp", MyTempTable()) as tmp:
                    tmp.Insert().Select("id", "name").From(db.tb).Execute()
                    ... # do something with the temporary table

        ## Example [async]
        >>> async with db.transaction() as conn:
                async with db.CreateTempTable(conn, "tmp", MyTempTable()) as tmp:
                    await tmp.Insert().Select("id", "name").From(db.tb).aioExecute()
                    ... # do something with the temporary table
            # Equivalent to:
            BEGIN;
            CREATE TEMPORARY TABLE db.tmp ...;
            INSERT INTO db.tmp SELECT id, name FROM db.tb AS t0;
            ...;
            DROP TEMPORARY TABLE IF EXISTS db.tmp;
            COMMIT;
        """
        dtype = type(temp_table)
        if dtype is TempTable:
            self._raise_argument_error(
                "DML method 'CreateTempTable' expects an instance of the "
                "<'TempTable'> subclass, instead got the base class %s itself." % dtype
            )
        if not issubclass(dtype, TempTable):
            self._raise_argument_error(
                "DML method 'CreateTempTable' expects an instance of the "
                "<'TempTable'> subclass, instead got %s." % dtype
            )
        tmp: TempTable = temp_table
        tmp = tmp.copy()
        tmp.set_name(name)
        tmp.setup(self._db_name, self._charset, None, self._pool)
        return TempTableManager(tmp, conn)

    # Sync ---------------------------------------------------------------------------------
    @cython.ccall
    def Initialize(self, force: cython.bint = False) -> Logs:
        """[sync] Initialize the database `<'Logs'>`.

        :param force `<'bool'>`: Whether to force the initialization even
            the database has already been initialized. Defaults to `False`.

        ## Explanation (difference from create)
        - 1. Create the database if not exists.
        - 2. Initialize all the tables. For more information about the
             table initialization, please refer to the 'Initialize()'
             method in the <'Table'> class.
        """
        logs: Logs = Logs()
        # Skip
        if self._initialized and not force:
            return logs
        # Initialize: database
        if not self.Exists():
            logs.extend(self.Create(True))
        else:
            logs.extend(self.SyncFromRemote())
        # Initialize: tables
        tb: Table
        for tb in self._tables._el_dict.values():
            logs.extend(tb.Initialize(force))
        # Finished
        self._set_initialized(True)
        return logs

    @cython.ccall
    def Create(self, if_not_exists: cython.bint = False) -> Logs:
        """[sync] Create the database `<'Logs'>`.

        :param if_not_exists `<'bool'>`: Create the database only if it does not exist. Defaults to `False`.
        :raises `<'OperationalError'>`: If the database already exists when 'if_not_exists=False'.
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
        """[sync] Check if the database exists `<'bool'>`."""
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
    def Drop(self, if_exists: cython.bint = False) -> Logs:
        """[sync] Drop the database `<'Logs'>`.

        :param if_exists `<'bool'>`: Drop the database only if it exists. Defaults to `False`.
        :raises `<'OperationalError'>`: If the database does not exists when 'if_exists=False'.
        """
        sql: str = self._gen_drop_sql(if_exists)
        with self._pool.acquire() as conn:
            with conn.transaction() as cur:
                cur.execute(sql)
        self._set_initialized(False)
        logs = Logs().log_sql(self, sql)
        return logs.log_element_deletion(self, False)

    @cython.ccall
    def Alter(
        self,
        charset: object = None,
        collate: object = None,
        encryption: bool | None = None,
        read_only: bool | None = None,
    ) -> Logs:
        """[sync] Alter the database `<'Logs'>`.

        ## Notice
        - Default value `None` means to retain the corresponding settings of the database.

        :param charset `<'str/Charset/None'>`: The CHARACTER SET of the database. Defaults to `None`.
        :param collate `<'str/None'>`: The COLLATE of the database. Defaults to `None`.
        :param encryption `<'bool/None'>`: The default database ENCRYPTION behavior. Defaults to `None`.
            Enable encryption if `True`, else `False` to disable.
        :param read_only `<'bool/None'>`: Whether to permit modification of the database and its objects. Defaults to `None`.
            Enable read only mode if `True`, else `False` disable.
        """
        # Generate alter query
        meta = self.ShowMetadata()
        query = self._gen_alter_query(meta, charset, collate, encryption, read_only)
        # Execute alteration
        if query.executable():
            with self._pool.acquire() as conn:
                with conn.transaction() as cur:
                    query.execute(cur)
            # . refresh metadata
            meta = self.ShowMetadata()
        # Sync from remote
        return self._sync_from_metadata(meta, query._logs)

    @cython.ccall
    def ShowMetadata(self) -> DatabaseMetadata:
        """[sync] Show the database metadata from the remote server `<'DatabaseMetadata'>`.

        :raises `<'OperationalError'>`: If the database does not exist.
        """
        sql: str = self._gen_show_metadata_sql()
        with self._pool.acquire() as conn:
            conn.set_decode_json(False)
            with conn.transaction(DictCursor) as cur:
                cur.execute(sql)
                res = cur.fetchone()
        if res is None:
            self._raise_operational_error(1046, "does not exist")
        return DatabaseMetadata(res)

    def Lock(
        self,
        conn: PoolSyncConnection,
        *tables: str | Table,
        lock_for_read: cython.bint = True,
    ) -> PoolSyncConnection:
        """[sync] Lock tables in the database `<'PoolSyncConnection'>`.

        :param conn `<'PoolSyncConnection'>`: The [sync] connection to acquire the table locks.
        :param tables `<'*str/Table'>`: The tables in the current database to be locked.
            Tables not belong to the current databases will be skipped and
            ignored. If none of the passed-in tables found in the database,
            raise `<'TableNotExistsError'>`.
        :param lock_for_read `<'bool'>`: Use lock for `READ` mode if `True`, else lock for `WRITE`. Defaults to `True`.
            - **READ mode**: Allows multiple threads to read from the table but
              prevents any thread from modifying it (i.e., no updates, deletes,
              or inserts are allowed).
            - **WRITE mode**: Prevents both read and write operations by any
              other threads.

        :returns `<'PoolSyncConnection'>`: The passed-in [sync] connection that acquired the table locks.

        ## Notice
        - This method only lock the tables in the this database.
        - If the passed-in [sync] connection already holding locks,
          its existing locks are released implicitly before the new
          locks are granted.
        """
        sql: str = self._gen_lock_sql(tables, lock_for_read)
        with conn.cursor() as cur:
            cur.execute(sql)
        return conn

    @cython.ccall
    def SyncFromRemote(self, thorough: cython.bint = False) -> Logs:
        """[sync] Synchronize the local database configs with the remote server `<'Logs'>`.

        :param thorough `<'bool'>`: Synchronize with the remote server thoroughly. Defaults to `False`.
            - **False**: only synchronize the local database configs.
            - **True**: also synchronize the local database's tables configs.

        ## Explanation
        - This method does `NOT` alter the remote server database,
          but only changes the local database configurations to match
          the remote server metadata.
        """
        # Sync: database
        try:
            meta = self.ShowMetadata()
        except sqlerrors.OperationalError as err:
            if err.args[0] == 1046:
                return Logs().log_sync_failed_not_exist(self)
            raise err
        logs: Logs = self._sync_from_metadata(meta)
        if not thorough:
            return logs  # exit

        # Sync: tables
        tb: Table
        for tb in self._tables._el_dict.values():
            logs.extend(tb.SyncFromRemote(True))
        # Finished
        return logs

    @cython.ccall
    def SyncToRemote(self) -> Logs:
        """[sync] Synchronize the remote server with the local database configs `<'Logs'>`.

        ## Explanation
        - This method compares the local database configurations with the
          remote server metadata and issues the necessary ALTER DATABASE
          statements so that the remote one matches the local settings.
        - Different from `SyncFromRemote()`, this method does not provide
          the `thorough` option. To synchronize the tables of the database,
          you need to call the `SyncToRemote()` from the table itself.
        """
        # Check existence
        if not self.Exists():
            return self.Create(True)
        # Sync to remote
        return self.Alter(
            self._charset,
            None,
            utils.read_bool_config(self._encryption),
            self._read_only,
        )

    # Async --------------------------------------------------------------------------------
    async def aioInitialize(self, force: cython.bint = False) -> Logs:
        """[async] Initialize the database `<'Logs'>`.

        :param force `<'bool'>`: Whether to force the initialization even
            the database has already been initialized. Defaults to `False`.

        ## Explanation (difference from create)
        - 1. Create the database if not exists.
        - 2. Initialize all the tables. For more information about the
             table initialization, please refer to the 'Initialize()'
             method in the <'Table'> class.
        """
        logs: Logs = Logs()
        # Skip
        if self._initialized and not force:
            return logs
        # Initialize: database
        if not await self.aioExists():
            logs.extend(await self.aioCreate(True))
        else:
            logs.extend(await self.aioSyncFromRemote())
        # Initialize: tables
        tasks: list = []
        for tb in self._tables._el_dict.values():
            tasks.append(tb.aioInitialize(force))
        # Await all tasks
        for l in await _aio_gather(*tasks):
            logs.extend(l)
        # Finished
        self._set_initialized(True)
        return logs

    async def aioCreate(self, if_not_exists: cython.bint = False) -> Logs:
        """[async] Create the database `<'Logs'>`.

        :param if_not_exists `<'bool'>`: Create the database only if it does not exist. Defaults to `False`.
        :raises `<'OperationalError'>`: If the database already exists when 'if_not_exists=False'.
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
        """[async] Check if the database exists `<'bool'>`."""
        sql: str = self._gen_exists_sql()
        async with self._pool.acquire() as conn:
            async with conn.transaction() as cur:
                await cur.execute(sql)
                res = await cur.fetchone()
        if res is None:
            self._set_initialized(False)
            return False
        return True

    async def aioDrop(self, if_exists: cython.bint = False) -> Logs:
        """[async] Drop the database `<'Logs'>`.

        :param if_exists `<'bool'>`: Drop the database only if it exists. Defaults to `False`.
        :raises `<'OperationalError'>`: If the database does not exists when 'if_exists=False'.
        """
        sql: str = self._gen_drop_sql(if_exists)
        async with self._pool.acquire() as conn:
            async with conn.transaction() as cur:
                await cur.execute(sql)
        self._set_initialized(False)
        logs = Logs().log_sql(self, sql)
        return logs.log_element_deletion(self, False)

    async def aioAlter(
        self,
        charset: object = None,
        collate: object = None,
        encryption: bool | None = None,
        read_only: bool | None = None,
    ) -> Logs:
        """[async] Alter the database `<'Logs'>`.

        ## Notice
        - Default value `None` means to retain the corresponding settings of the database.

        :param charset `<'str/Charset/None'>`: The CHARACTER SET of the database. Defaults to `None`.
        :param collate `<'str/None'>`: The COLLATE of the database. Defaults to `None`.
        :param encryption `<'bool/None'>`: The default database ENCRYPTION behavior. Defaults to `None`.
            Enable encryption if `True`, else `False` to disable.
        :param read_only `<'bool/None'>`: Whether to permit modification of the database and its objects. Defaults to `None`.
            Enable read only mode if `True`, else `False` disable.
        """
        # Generate alter query
        meta = await self.aioShowMetadata()
        query = self._gen_alter_query(meta, charset, collate, encryption, read_only)
        # Execute alteration
        if query.executable():
            async with self._pool.acquire() as conn:
                async with conn.transaction() as cur:
                    await query.aio_execute(cur)
            # . refresh metadata
            meta = await self.aioShowMetadata()
        # Sync from remote
        return self._sync_from_metadata(meta, query._logs)

    async def aioShowMetadata(self) -> DatabaseMetadata:
        """[async] Show the database metadata from the remote server `<'DatabaseMetadata'>`.

        :raises `<'OperationalError'>`: If the database does not exist.
        """
        sql: str = self._gen_show_metadata_sql()
        async with self._pool.acquire() as conn:
            conn.set_decode_json(False)
            async with conn.transaction(AioDictCursor) as cur:
                await cur.execute(sql)
                res = await cur.fetchone()
        if res is None:
            self._raise_operational_error(1046, "does not exist")
        return DatabaseMetadata(res)

    async def aioLock(
        self,
        conn: PoolConnection,
        *tables: str | Table,
        lock_for_read: cython.bint = True,
    ) -> PoolConnection:
        """[async] Lock tables in the database `<'PoolConnection'>`.

        :param conn `<'PoolConnection'>`: The [async] connection to acquire the table locks.
        :param tables `<'*str/Table'>`: The tables in the current database to be locked.
            Tables not belong to the current databases will be skipped and
            ignored. If none of the passed-in tables found in the database,
            raise `<'TableNotExistsError'>`.
        :param lock_for_read `<'bool'>`: Use lock for `READ` mode if `True`, else lock for `WRITE`. Defaults to `True`.
            - **READ mode**: Allows multiple threads to read from the table but
              prevents any thread from modifying it (i.e., no updates, deletes,
              or inserts are allowed).
            - **WRITE mode**: Prevents both read and write operations by any
              other threads.

        :returns `<'PoolConnection'>`: The passed-in [async] connection that acquired the table locks.

        ## Notice
        - This method only lock the tables in the this database.
        - If the passed-in [async] connection already holding locks,
          its existing locks are released implicitly before the new
          locks are granted.
        """
        sql: str = self._gen_lock_sql(tables, lock_for_read)
        async with conn.cursor() as cur:
            await cur.execute(sql)
        return conn

    async def aioSyncFromRemote(self, thorough: cython.bint = False) -> Logs:
        """[async] Synchronize the local database configs with the remote server `<'Logs'>`.

        :param thorough `<'bool'>`: Synchronize with the remote server thoroughly. Defaults to `False`.
            - **False**: only synchronize the local database configs.
            - **True**: also synchronize the local database's tables configs.

        ## Explanation
        - This method does `NOT` alter the remote server database,
          but only changes the local database configurations to match
          the remote server metadata.
        """
        # Sync: database
        try:
            meta = await self.aioShowMetadata()
        except sqlerrors.OperationalError as err:
            if err.args[0] == 1046:
                return Logs().log_sync_failed_not_exist(self)
            raise err
        logs: Logs = self._sync_from_metadata(meta)
        if not thorough:
            return logs  # exit

        # Sync: tables
        tasks: list = []
        for tb in self._tables._el_dict.values():
            tasks.append(tb.aioSyncFromRemote(True))
        # Await all tasks
        for l in await _aio_gather(*tasks):
            logs.extend(l)
        # Finished
        return logs

    async def aioSyncToRemote(self) -> Logs:
        """[async] Synchronize the remote server with the local database configs `<'Logs'>`.

        ## Explanation
        - This method compares the local database configurations with the
          remote server metadata and issues the necessary ALTER DATABASE
          statements so that the remote one matches the local settings.
        - Different from `SyncFromRemote()`, this method does not provide
          the `thorough` option. To synchronize the tables of the database,
          you need to call the `SyncToRemote()` from the table itself.
        """
        # Check existence
        if not await self.aioExists():
            return await self.aioCreate(True)
        # Sync to remote
        return await self.aioAlter(
            self._charset,
            None,
            utils.read_bool_config(self._encryption),
            self._read_only,
        )

    # Generate SQL -------------------------------------------------------------------------
    @cython.ccall
    def _gen_create_sql(self, if_not_exists: cython.bint) -> str:
        """(internal) Generate SQL to create the database `<'str'>`."""
        self._assure_ready()
        # fmt: off
        if if_not_exists:
            sql: str = (
                "CREATE DATABASE IF NOT EXISTS %s CHARACTER SET %s COLLATE %s"
                % (self._name, self._charset._name, self._charset._collation)
            )
        else:
            sql: str = (
                "CREATE DATABASE %s CHARACTER SET %s COLLATE %s" 
                % (self._name, self._charset._name, self._charset._collation)
            )
        # fmt: on
        if self._encryption != -1:
            sql += " ENCRYPTION 'Y'" if self._encryption == 1 else " ENCRYPTION 'N'"
        return sql + ";"

    @cython.ccall
    def _gen_exists_sql(self) -> str:
        """(internal) Generate SQL to check if the database exists `<'str'>`."""
        self._assure_ready()
        return (
            "SELECT 1 "
            "FROM INFORMATION_SCHEMA.SCHEMATA "
            "WHERE SCHEMA_NAME = '%s' "
            "LIMIT 1;" % self._db_name
        )

    @cython.ccall
    def _gen_drop_sql(self, if_exists: cython.bint) -> str:
        """(internal) Generate SQL to drop the database `<'str'>`."""
        self._assure_ready()
        if if_exists:
            return "DROP DATABASE IF EXISTS %s;" % self._name
        else:
            return "DROP DATABASE %s;" % self._name

    @cython.ccall
    def _gen_alter_query(
        self,
        meta: DatabaseMetadata,
        charset: object | None,
        collate: object | None,
        encryption: bool | None,
        read_only: bool | None,
    ) -> Query:
        """(interal) Generate the query to alter the database `<'Query'>`."""
        self._assure_ready()
        query = Query()
        altered: cython.bint = False
        sql: str = "ALTER DATABASE %s" % self._name

        # Charset
        _charset = self._validate_charset(charset, collate)
        if _charset is not None and _charset is not meta._charset:
            _charset = self._validate_encoding(_charset)
            sql += " CHARACTER SET %s COLLATE %s" % (
                _charset._name,
                _charset._collation,
            )
            altered = True  # set flag

        # Encryption
        _encrypt: cython.int = self._validate_encryption(encryption)
        if _encrypt != -1 and _encrypt != meta._encryption:
            sql += " ENCRYPTION 'Y'" if _encrypt == 1 else " ENCRYPTION 'N'"
            altered = True  # set flag

        # Read Only
        _read_only: cython.int = self._validate_read_only(read_only)
        if _read_only != -1 and _read_only != meta._read_only:
            sql += " READ ONLY 1" if _read_only == 1 else " READ ONLY 0"
            altered = True  # set flag

        # Compose & Set SQL
        if altered:
            query.set_sql(self, sql + ";")
        return query

    @cython.ccall
    def _gen_show_metadata_sql(self) -> str:
        """(internal) Generate SQL to show the database metadata `<'str'>`."""
        self._assure_ready()
        return (
            "SELECT "
            # . columns [t1]
            "t1.CATALOG_NAME AS CATALOG_NAME, "
            "t1.SCHEMA_NAME AS SCHEMA_NAME, "
            "t1.DEFAULT_CHARACTER_SET_NAME AS DEFAULT_CHARACTER_SET_NAME, "
            "t1.DEFAULT_COLLATION_NAME AS DEFAULT_COLLATION_NAME, "
            "t1.SQL_PATH AS SQL_PATH, "
            "t1.DEFAULT_ENCRYPTION AS DEFAULT_ENCRYPTION, "
            # . columns [t2]
            "UPPER(t2.OPTIONS) AS OPTIONS "
            # . information_schema.schemata
            "FROM INFORMATION_SCHEMA.SCHEMATA AS t1 "
            # . information_schema.schemata_extensions
            "JOIN INFORMATION_SCHEMA.SCHEMATA_EXTENSIONS AS t2 "
            "ON t1.SCHEMA_NAME = t2.SCHEMA_NAME "
            # . conditions
            "WHERE t1.SCHEMA_NAME = '%s';" % self._db_name
        )

    @cython.ccall
    def _gen_lock_sql(
        self,
        tables: tuple[str | Table],
        lock_for_read: cython.bint,
    ) -> str:
        """(internal) Generate SQL to lock tables in the database `<'str'>`."""
        self._assure_ready()
        tbs: Elements = self._tables._filter(tables)
        if tbs._size == 0:
            self._raise_operational_error(
                1050,
                "does not contain tables: %s." % ", ".join([str(i) for i in tables]),
            )
        tb: Element
        if lock_for_read:
            return "LOCK TABLES %s;" % ", ".join(
                ["%s READ" % tb._tb_qualified_name for tb in tbs._el_dict.values()]
            )
        else:
            return "LOCK TABLES %s;" % ", ".join(
                ["%s WRITE" % tb._tb_qualified_name for tb in tbs._el_dict.values()]
            )

    # Metadata -----------------------------------------------------------------------------
    @cython.ccall
    def _sync_from_metadata(self, meta: DatabaseMetadata, logs: Logs = None) -> Logs:
        """(internal) Synchronize local database configs with its remote metadata `<'Logs'>`."""
        self._assure_ready()
        if logs is None:
            logs = Logs()

        # Validate metadata
        if self._db_name != meta._db_name:
            logs.log_sync_failed_mismatch(
                self, "database name", self._db_name, meta._db_name
            )
            return logs._skip()  # exit

        # Charset
        if self._charset is not meta._charset:
            charset = self._validate_encoding(meta._charset)
            logs.log_charset(self, self._charset, charset)
            self._charset = charset

        # Encryption
        if self._encryption != meta._encryption:
            logs.log_config_bool(self, "encryption", self._encryption, meta._encryption)
            self._encryption = meta._encryption

        # Read Only
        if self._read_only != meta._read_only:
            logs.log_config_bool(self, "read_only", self._read_only, meta._read_only)
            self._read_only = meta._read_only

        # Return Logs
        return logs

    # Setter -------------------------------------------------------------------------------
    @cython.ccall
    @cython.exceptval(-1, check=False)
    def setup(self) -> cython.bint:
        """Setup the database."""
        # Assure setup ready
        self._assure_setup_ready()

        # Setup elements
        tbs: list = []
        try:
            _annotations: dict = self.__annotations__
        except AttributeError:
            _annotations: dict = {}
        # -------------------------------
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
            # . table
            if isinstance(el, Table):
                dtype = type(el)
                if dtype is Table or dtype is TimeTable:
                    self._raise_definition_error(
                        "expects an instance of the <'Table'> subclass, "
                        "instead got the base class %s itself." % dtype
                    )
                el = el.copy()
                el.set_name(name)
                tbs.append(el)
            # . others
            else:
                self._raise_definition_error(
                    "is annotated with an unsupported element "
                    "('%s' %s)." % (name, dtype)
                )
            setattr(self, name, el)

        # Construct elements
        self._tables = Tables(*tbs)
        self._tables.setup(db_name, charset, None, pool)

        # Switch setup flag
        self._setup_finished = True
        return self._assure_ready()

    @cython.ccall
    @cython.exceptval(-1, check=False)
    def set_name(self, name: object) -> cython.bint:
        """Set the name of the Database."""
        if Element.set_name(self, name):
            self._name = self._validate_database_name(name)
            self._db_name = self._name
        return True

    # Assure Ready -------------------------------------------------------------------------
    @cython.ccall
    @cython.exceptval(-1, check=False)
    def _assure_ready(self) -> cython.bint:
        """(internal) Assure the database is ready."""
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
        """(internal) Assure the database is ready for the 'setup()' process."""
        self._assure_name_ready()
        self._assure_db_name_ready()
        self._assure_encoding_ready()
        return True

    # Validate -----------------------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(-2, check=False)
    def _validate_read_only(self, read_only: object) -> cython.int:
        """(internal) Validate the READ ONLY config `<'int'>.

        :returns `<'int'>`:
        - `-1`: read only mode is unknown.
        - `1`: read only mode enabled.
        - `0`: read only mode disabled.
        """
        try:
            return utils.validate_encryption(read_only)
        except Exception as err:
            self._raise_definition_error(str(err), err)

    # Internal -----------------------------------------------------------------------------
    @cython.ccall
    @cython.exceptval(-1, check=False)
    def _set_initialized(self, flag: cython.bint) -> cython.bint:
        """(internal) Set the initialized flag of the database."""
        # Self
        Element._set_initialized(self, flag)
        # Tables
        if self._tables is not None:
            self._tables._set_initialized(flag)
        # Finished
        return True

    # Copy ---------------------------------------------------------------------------------
    @cython.ccall
    def copy(self) -> Database:
        """Make a copy of the database `<'Database'>`."""
        try:
            db: Database = self.__class__(
                self._name,
                self._pool,
                self._charset,
                None,
                utils.read_bool_config(self._encryption),
            )
        except TypeError as err:
            if "positional arguments" in str(err):
                self._raise_critical_error(
                    "inherits from the base <'Database'> class, "
                    "must not override its '__init__' method.",
                    err,
                )
            raise err
        db.set_name(self._name)
        return db

    # Special Methods ----------------------------------------------------------------------
    def __getitem__(self, tb: str | Table) -> Table:
        self._assure_ready()
        return self._tables[tb]

    def __contains__(self, tb: str | Table) -> bool:
        self._assure_ready()
        return tb in self._tables

    def __iter__(self) -> Iterator[Table]:
        self._assure_ready()
        return iter(self._tables)

    def __repr__(self) -> str:
        self._assure_ready()
        # Reprs
        reprs = [
            "name=%r" % self._name,
            "charset=%r" % self._charset._name,
            "collate=%r" % self._charset._collation,
            "encryption=%s" % utils.read_bool_config(self._encryption),
            "read_only=%s" % self._read_only,
            "tables=%s" % self._tables,
        ]
        # Compose
        return "<Database (\n\t%s\n)>" % ",\n\t".join(reprs)

    def __str__(self) -> str:
        self._assure_ready()
        return self._db_name

    def __len__(self) -> int:
        self._assure_ready()
        return self._tables._size


# Prohibited names from Database class
utils.SCHEMA_ELEMENT_SETTINGS.add_prohibited_names(dir(Database("__dummy__", Pool())))


# Metadata ---------------------------------------------------------------------------------------------------
@cython.cclass
class DatabaseMetadata(Metadata):
    """Represents the metadata from the remote server of a database."""

    # Base data
    _db_name: str
    _charset: Charset
    _encryption: cython.bint
    _options: str
    # Additional data
    _read_only: cython.bint

    def __init__(self, meta: dict):
        """The metadata from the remote server of a database.

        :param meta `<'dict'>`: A dictionary contains the following database information:
        ```python
        - "CATALOG_NAME"
        - "SCHEMA_NAME"
        - "DEFAULT_CHARACTER_SET_NAME"
        - "DEFAULT_COLLATION_NAME"
        - "SQL_PATH"
        - "DEFAULT_ENCRYPTION"
        - "OPTIONS"
        ```
        """
        super().__init__("DATABASE", meta, 7)
        try:
            # Base data
            self._db_name = meta["SCHEMA_NAME"]
            self._charset = utils.validate_charset(
                meta["DEFAULT_CHARACTER_SET_NAME"], meta["DEFAULT_COLLATION_NAME"]
            )
            self._encryption = utils.validate_encryption(meta["DEFAULT_ENCRYPTION"])
            self._options = meta["OPTIONS"]
            # Additional data
            self._read_only = utils.validate_str_contains(self._options, "READ ONLY=1")
        except Exception as err:
            self._raise_invalid_metadata_error(meta, err)

    # Property -----------------------------------------------------------------------------
    @property
    def catelog_name(self) -> str:
        """The catelog name of the database `<'str'>`."""
        return self._meta["CATALOG_NAME"]

    @property
    def db_name(self) -> str:
        """The name of the database `<'str'>`."""
        return self._db_name

    @property
    def charset(self) -> Charset:
        """The default charset of the database `<'Charset'>`."""
        return self._charset

    @property
    def encryption(self) -> bool:
        """The default database encryption behavior `<'bool'>`.

        - Indicates whether the database is configured to use encryption
          at rest if the MySQL server supports encryption.
        - 'True' means that the server will attempt to encrypt all InnoDB tables
          created within this database by default (assuming the server's encryption
          settings are properly configured).
        """
        return self._encryption

    @property
    def read_only(self) -> bool:
        """Whether the database is in read-only mode `<'bool'>`."""
        return self._read_only

    @property
    def options(self) -> str:
        """The additional database-related options `<'str'>`."""
        return self._options
