# cython: language_level=3
# cython: wraparound=False
# cython: boundscheck=False
from __future__ import annotations

# Cython imports
import cython
from cython.cimports.cpython.list import PyList_Size as list_len  # type: ignore
from cython.cimports.cpython.set import PySet_Contains as set_contains  # type: ignore
from cython.cimports.cpython.tuple import PyTuple_GET_SIZE as tuple_len  # type: ignore
from cython.cimports.cpython.dict import PyDict_SetItem as dict_setitem  # type: ignore
from cython.cimports.cpython.dict import PyDict_Contains as dict_contains  # type: ignore
from cython.cimports.mysqlengine.element import Element, Elements, Logs, Metadata  # type: ignore
from cython.cimports.mysqlengine import utils  # type: ignore

# Python imports
from typing import Iterator
from sqlcycli.aio.pool import Pool
from sqlcycli.charset import Charset
from sqlcycli import errors as sqlerrors, DictCursor
from sqlcycli.aio import DictCursor as AioDictCursor
from mysqlengine.element import Element, Elements, Logs, Metadata
from mysqlengine import utils


__all__ = [
    "Partitioning",
    "Partition",
    "Partitions",
    "PartitioningMetadata",
]


# Partitioning -----------------------------------------------------------------------------------------------
@cython.cclass
class Partitioning(Element):
    """Represents the partitioning configuration of a database table.

    ## Notice
    To complete the configuration, call the following methods after instantiation:
    ```python
    - by_key()      ->  PARTITION BY [LINEAR] KEY
    - by_hash()     ->  PARTITION BY [LINEAR] HASH
    - by_list()     ->  PARTITION BY LIST [COLUMNS]
    - by_range()    ->  PARTITION BY RANGE [COLUMNS]
    ```
    For configuration examples, please refer to the method docstrings.
    """

    # partitioning
    _partitioning_flag: cython.int
    _partitioning_expression: str
    # subpartitioning
    _subpartitioning_flag: cython.int
    _subpartitioning_expression: str
    # partitions
    _partitions: Partitions

    def __init__(self, *expressions: object):
        """The partitioning configuration of a database table.

        ## Notice
        To complete the configuration, call the following methods after instantiation:
        ```python
        - by_key()      ->  PARTITION BY [LINEAR] KEY
        - by_hash()     ->  PARTITION BY [LINEAR] HASH
        - by_list()     ->  PARTITION BY LIST [COLUMNS]
        - by_range()    ->  PARTITION BY RANGE [COLUMNS]
        ```
        For configuration examples, please refer to the method docstrings.

        :param expressions `<'*str/SQLFunction'>`: The expressions or column names of the partitioning.
        """
        super().__init__("PARTITION", "PARTITIONING")
        # partitioning
        self._partitioning_flag = utils.PARTITIONING_METHOD.NONSET
        self._partitioning_expression = self._validate_partitioning_expression(
            expressions, subpartitioning=False
        )
        # subpartitioning
        self._reset_subpartitioning()
        # partitions
        self._partitions = None

    # Property -----------------------------------------------------------------------------
    @property
    def db_name(self) -> str:
        """The database name of the partitioning `<'str'>`."""
        self._assure_ready()
        return self._db_name

    @property
    def tb_name(self) -> str:
        """The table name of the partitioning `<'str'>`."""
        self._assure_ready()
        return self._tb_name

    @property
    def tb_qualified_name(self) -> str:
        """The qualified table name '{db_name}.{tb_name}' `<'str'>`."""
        self._assure_ready()
        return self._tb_qualified_name

    @property
    def partitioning_method(self) -> str | None:
        """The partitioning method `<'str/None'>`.

        e.g., `"RANGE"`, `"LIST"`, `"HASH"`, `"KEY"`, etc.
        """
        if self._partitioning_flag == utils.PARTITIONING_METHOD.NONSET:
            return None
        return self._partitioning_flag_to_method(self._partitioning_flag)

    @property
    def partitioning_expression(self) -> str:
        """The expressions or column names of the partitioning. `<'str'>`."""
        return self._partitioning_expression

    @property
    def subpartitioning_method(self) -> str | None:
        """The subpartitioning method `<'str/None'>`.

        e.g., `"HASH"`, `"LINEAR HASH"`,`"KEY"`,`"LINEAR KEY"`, etc.
        """
        if self._subpartitioning_flag == utils.PARTITIONING_METHOD.NONSET:
            return None
        return self._partitioning_flag_to_method(self._subpartitioning_flag)

    @property
    def subpartitioning_expression(self) -> str | None:
        """The expressions or column names of the subpartitioning `<'str/None'>`."""
        return self._subpartitioning_expression

    @property
    def partitions(self) -> Partitions:
        """The partitions `<'Partitions'>`."""
        return self._partitions

    # Configure ----------------------------------------------------------------------------
    def by_range(
        self,
        *partitions: Partition,
        columns: cython.bint = False,
    ) -> Partitioning:
        """Configure to PARTITION BY RANGE [COLUMNS] `<'Partitioning'>`.

        :param partitions `<'*Partition'>`: The partitions of the table.
        :param columns `<'bool'>`: If `True`, configure to BY RANGE `COLUMNS`. Defaults to `False`.

        ## Notice
        To setup subpartitions, chain the following method afterwards:
        ```python
        - subpartition_by_hash()    ->  SUBPARTITION BY [LINEAR] HASH
        - subpartition_by_key()     ->  SUBPARTITION BY [LINEAR] KEY
        ```

        ## Example (BY RANGE):
        ```python
        from mysqlengine import Partitioning, Partition, sqlfunc

        pt = Partitioning(sqlfunc.TO_DAYS("dt")).by_range(
            Partition("p1", sqlfunc.TO_DAYS(datetime.date(2020, 1, 1))),
            Partition("p2", sqlfunc.TO_DAYS(datetime.date(2021, 1, 1))),
            Partition("p3", "MAXVALUE"),
        )
        # Equivalent to:
        PARTITION BY RANGE (TO_DAYS(dt)) (
            PARTITION p1 VALUES LESS THAN (TO_DAYS('2020-01-01')),
            PARTITION p2 VALUES LESS THAN (TO_DAYS('2021-01-01')),
            PARTITION p3 VALUES LESS THAN (MAXVALUE)
        )
        ```

        ## Example (BY RANGE COLUMNS)
        ```python
        from mysqlengine import Partitioning, Partition

        pt = Partitioning("dt", "time").by_range(
            Partition("p1", datetime.date(2020, 1, 1), datetime.time(0, 0, 0)),
            Partition("p2", datetime.date(2021, 1, 1), datetime.time(0, 0, 0)),
            Partition("p3", "MAXVALUE", "MAXVALUE"),
            columns=True,
        )
        # Equivalent to:
        PARTITION BY RANGE COLUMNS (dt,time) (
            PARTITION p1 VALUES LESS THAN ('2020-01-01','00:00:00'),
            PARTITION p2 VALUES LESS THAN ('2021-01-01','00:00:00'),
            PARTITION p3 VALUES LESS THAN (MAXVALUE,MAXVALUE)
        )
        ```
        """
        self._set_partitioning_flag(
            utils.PARTITIONING_METHOD.RANGE_COLUMNS
            if columns
            else utils.PARTITIONING_METHOD.RANGE
        )
        self._set_partitions_by_instance(partitions)
        self._reset_subpartitioning()
        return self

    def by_list(
        self,
        *partitions: Partition,
        columns: cython.bint = False,
    ) -> Partitioning:
        """Configure to PARTITION BY LIST [COLUMNS] `<'Partitioning'>`.

        :param partitions `<'*Partition'>`: The partitions of the table.
        :param columns `<'bool'>`: If `True`, configure to BY LIST `COLUMNS`. Defaults to `False`.

        ## Notice
        To setup subpartitions, chain the following method afterwards:
        ```python
        - subpartition_by_hash()    ->  SUBPARTITION BY [LINEAR] HASH
        - subpartition_by_key()     ->  SUBPARTITION BY [LINEAR] KEY
        ```

        ## Example (BY LIST):
        ```python
        from mysqlengine import Partitioning, Partition

        pt = Partitioning("store_id").by_list(
            Partition("p1", 3, 5, 6, 9, 17),
            Partition("p2", 1, 2, 10, 11, 19, 20),
        )
        # Equivalent to:
        PARTITION BY LIST (store_id) (
            PARTITION p1 VALUES IN (3,5,6,9,17),
            PARTITION p2 VALUES IN (1,2,10,11,19,20)
        )
        ```

        ## Example (BY LIST COLUMNS)
        ```python
        from mysqlengine import Partitioning, Partition

        pt = Partitioning("country", "city").by_list(
            Partition("p1", ("USA", "CA"), ("USA", "NY"), ("CAN", "ON"), ("CAN", "QC")),
            Partition("p2", ("FRA", "IDF"), ("DEU", "BE"), ("ESP", "CAT")),
            columns=True,
        )
        # Equivalent to:
        PARTITION BY LIST COLUMNS (country,city) (
            PARTITION p1 VALUES IN (('USA','CA'),('USA','NY'),('CAN','ON'),('CAN','QC')),
            PARTITION p2 VALUES IN (('FRA','IDF'),('DEU','BE'),('ESP','CAT'))
        )
        ```
        """
        self._set_partitioning_flag(
            utils.PARTITIONING_METHOD.LIST_COLUMNS
            if columns
            else utils.PARTITIONING_METHOD.LIST
        )
        self._set_partitions_by_instance(partitions)
        self._reset_subpartitioning()
        return self

    @cython.ccall
    def by_hash(
        self,
        partitions: cython.int,
        linear: cython.bint = False,
    ) -> Partitioning:
        """Configure to PARTITION BY [LINEAR] HASH `<'Partitioning'>`.

        :param partitions `<'int'>`: Total number of HASH partitions.
        :param linear `<'bool'>`: If `True`, configure to BY `LINEAR` HASH. Defaults to `False`.

        ## Example (BY HASH):
        ```python
        from mysqlengine import Partitioning

        pt = Partitioning("store_id").by_hash(4)
        # Equivalent to:
        PARTITION BY HASH (store_id) PARTITIONS 4
        ```

        ## EXAMPLE (BY LINEAR HASH)
        ```python
        from mysqlengine import Partitioning

        pt = Partitioning("store_id").by_hash(4, linear=True)
        # Equivalent to:
        PARTITION BY LINEAR HASH (store_id) PARTITIONS 4
        ```
        """
        self._set_partitioning_flag(
            utils.PARTITIONING_METHOD.LINEAR_HASH
            if linear
            else utils.PARTITIONING_METHOD.HASH
        )
        self._set_partitions_by_integer(partitions)
        self._reset_subpartitioning()
        return self

    @cython.ccall
    def by_key(
        self,
        partitions: cython.int,
        linear: cython.bint = False,
    ) -> Partitioning:
        """Configure to PARTITION BY [LINEAR] KEY `<'Partitioning'>`.

        :param partitions `<'int'>`: Total number of KEY partitions.
        :param linear `<'bool'>`: If `True`, configure to BY `LINEAR` KEY. Defaults to `False`.

        ## Example (BY KEY):
        ```python
        from mysqlengine import Partitioning

        pt = Partitioning("store_id").by_key(4)
        # Equivalent to:
        PARTITION BY KEY (store_id) PARTITIONS 4
        ```

        ## EXAMPLE (BY LINEAR KEY)
        ```python
        from mysqlengine import Partitioning

        pt = Partitioning("store_id").by_key(4, linear=True)
        # Equivalent to:
        PARTITION BY LINEAR KEY (store_id) PARTITIONS 4
        ```
        """
        self._set_partitioning_flag(
            utils.PARTITIONING_METHOD.LINEAR_KEY
            if linear
            else utils.PARTITIONING_METHOD.KEY
        )
        self._set_partitions_by_integer(partitions)
        self._reset_subpartitioning()
        return self

    def subpartition_by_hash(
        self,
        subpartitions: cython.int,
        *expressions: object,
        linear: cython.bint = False,
    ) -> Partitioning:
        """Further configure to SUBPARTITION BY [LINEAR] HASH `<'Partitioning'>.

        :param subpartitions `<'int'>`: Total number of HASH subpartitions for each of the main partitions.
        :param expressions `<'*str/SQLFunction'>`: The expressions or column names of the subpartitioning.
        :param linear `<'bool'>`: If `True`, configure to SUBPARTITION BY `LINEAR` HASH. Defaults to `False`.

        ## Notice
        - This method should only be chained after the `by_list()` or `by_range()` method.

        ## Example:
        ```python
        from mysqlengine import Partitioning, Partition, sqlfunc

        pt = (
            Partitioning(sqlfunc.YEAR("dt"))
            .by_range(
                Partition("p1", 1990),
                Partition("p2", 2000),
                Partition("p3", "MAXVALUE"),
            )
            .subpartition_by_hash(2, "name")
        )
        # Equivalent to:
        PARTITION BY RANGE (YEAR(dt))
        SUBPARTITION BY HASH (name) SUBPARTITIONS 2 (
            PARTITION p1 VALUES LESS THAN (1990),
            PARTITION p2 VALUES LESS THAN (2000),
            PARTITION p3 VALUES LESS THAN (MAXVALUE)
        )
        ```
        """
        self._assure_partitions_ready()
        if linear:
            flag: cython.int = utils.PARTITIONING_METHOD.LINEAR_HASH
        else:
            flag: cython.int = utils.PARTITIONING_METHOD.HASH
        self._subpartitioning_flag = flag
        self._subpartitioning_expression = self._validate_partitioning_expression(
            expressions, subpartitioning=True
        )
        self._partitions._setup_subpartitions(flag, subpartitions)
        return self

    def subpartition_by_key(
        self,
        subpartitions: cython.int,
        *expressions: object,
        linear: cython.bint = False,
    ) -> Partitioning:
        """Further configure to SUBPARTITION BY [LINEAR] KEY `<'Partitioning'>.

        :param subpartitions `<'int'>`: Total number of KEY subpartitions for each of the main partitions.
        :param expressions `<'*str/SQLFunction'>`: The expressions or column names of the subpartitioning.
        :param linear `<'bool'>`: If `True`, configure to SUBPARTITION BY `LINEAR` KEY. Defaults to `False`.

        ## Notice
        - This method should only be chained after the `by_list()` or `by_range()` method.

        ## Example:
        ```python
        from mysqlengine import Partitioning, Partition, sqlfunc

        pt = (
            Partitioning(sqlfunc.YEAR("dt"))
            .by_range(
                Partition("p1", 1990),
                Partition("p2", 2000),
                Partition("p3", "MAXVALUE"),
            )
            .subpartition_by_key(2, "name")
        )
        # Equivalent to:
        PARTITION BY RANGE (YEAR(dt))
        SUBPARTITION BY KEY (name) SUBPARTITIONS 2 (
            PARTITION p1 VALUES LESS THAN (1990),
            PARTITION p2 VALUES LESS THAN (2000),
            PARTITION p3 VALUES LESS THAN (MAXVALUE)
        )
        ```
        """
        self._assure_partitions_ready()
        if linear:
            flag: cython.int = utils.PARTITIONING_METHOD.LINEAR_KEY
        else:
            flag: cython.int = utils.PARTITIONING_METHOD.KEY
        self._subpartitioning_flag = flag
        self._subpartitioning_expression = self._validate_partitioning_expression(
            expressions, subpartitioning=True
        )
        self._partitions._setup_subpartitions(flag, subpartitions)
        return self

    # Sync ---------------------------------------------------------------------------------
    @cython.ccall
    def Initialize(self, force: cython.bint = False) -> Logs:
        """[sync] Initialize the table partitioning `<'Logs'>`.

        :param force `<'bool'>`: Whether to force the initialization even
            the table partitioning has already been initialized. Defaults to `False`.

        ## Explanation (difference from create)
        - Create the partitioning of the table if not exists.
        """
        logs: Logs = Logs()
        # Skip
        if self._initialized and not force:
            return logs
        # Initizlize: partitioning
        if not self.Exists():
            logs.extend(self.Create())
        else:
            logs.extend(self.SyncFromRemote())
        # Finished
        self._set_initialized(True)
        return logs

    @cython.ccall
    def Create(self) -> Logs:
        """[sync] Create the partitioning of the table `<'Logs'>`.

        :raises `<'OperationalError'>`: If the table is already partitioned.
        """
        # Check existence
        if self.Exists():
            self._raise_operational_error(1734, "already exists")
        # Execute creation
        sql: str = self._gen_create_sql()
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
        """[sync] Check if the table is partitioned `<'str'>`."""
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
    def Remove(self) -> Logs:
        """[sync] Remove the partitioning of the table `<'Logs'>`.

        :raises `<'OperationalError'>`: If the table is not partitioned.
        """
        sql: str = self._gen_remove_sql()
        with self._pool.acquire() as conn:
            with conn.transaction() as cur:
                cur.execute(sql)
        self._set_initialized(False)
        logs = Logs().log_sql(self, sql)
        return logs.log_element_deletion(self, False)

    @cython.ccall
    def ShowMetadata(self) -> PartitioningMetadata:
        """[sync] Show the partitioning metadata from the remote server `<'PartitioningMetadata'>`.

        :raises `<'OperationalError'>`: If the table is not partitioned.
        """
        sql: str = self._gen_show_metadata_sql()
        with self._pool.acquire() as conn:
            conn.set_decode_json(False)
            with conn.transaction(DictCursor) as cur:
                cur.execute(sql)
                res = cur.fetchall()
        if tuple_len(res) == 0:
            self._raise_operational_error(1505, "does not exist")
        return PartitioningMetadata(res)

    @cython.ccall
    def ShowPartitionNames(self) -> tuple[str]:
        """[sync] Show all the main partition names of the table
        (sorted by partition ordinal position) `<'tuple[str]'>`.

        ## Notice
        - Only returns the main partition names (not subpartitions).
        """
        sql: str = self._gen_show_partition_names_sql()
        with self._pool.acquire() as conn:
            with conn.transaction() as cur:
                cur.execute(sql)
                res: tuple = cur.fetchall()
        res = self._flatten_rows(res)
        if tuple_len(res) != self._partitions._size:
            self.SyncFromRemote()
        return res

    @cython.ccall
    def ShowPartitionRows(self) -> dict[str, int]:
        """[sync] Show the number of estimated rows in
        each of the main partitions `<'dict[str, int]'>`.

        ## Notice
        - Only returns the main partitions' estimated row counts (not subpartitions).
        - This method will first execute `ANALYZE TABLE`, so it might take some
          time for a large partitioned table.
        """
        sql: str = self._gen_show_partition_rows_sql()
        with self._pool.acquire() as conn:
            with conn.transaction() as cur:
                cur.execute("ANALYZE TABLE %s;" % self._tb_qualified_name)
                cur.execute(sql)
                res: tuple = cur.fetchall()
        r: tuple
        return {r[0]: r[1] for r in res}

    @cython.ccall
    def ShowSubpartitionNames(self) -> tuple[str]:
        """[sync] Show all the subpartition names of the table
        (sorted by subpartition ordinal position) `<'tuple[str]'>`.

        ## Notice
        - Only returns the subpartition names (not main partitions).
        """
        sql: str = self._gen_show_subpartition_names_sql()
        with self._pool.acquire() as conn:
            with conn.transaction() as cur:
                cur.execute(sql)
                res = cur.fetchall()
        return self._flatten_rows(res)

    @cython.ccall
    def ShowSubpartitionRows(self) -> dict[str, int]:
        """[sync] Show the number of estimated rows in
        each of the subpartitions `<'dict[str, int]'>`.

        ## Notice
        - Only returns the subpartitions' estimated row counts (not main partitions).
        - This method will first execute `ANALYZE TABLE`, so it might take some
          time for a large partitioned table.
        """
        sql: str = self._gen_show_subpartition_rows_sql()
        with self._pool.acquire() as conn:
            with conn.transaction() as cur:
                cur.execute("ANALYZE TABLE %s;" % self._tb_qualified_name)
                cur.execute(sql)
                res: tuple = cur.fetchall()
        r: tuple
        return {r[0]: r[1] for r in res}

    @cython.ccall
    def SyncFromRemote(self) -> Logs:
        """[sync] Synchronize the local partitioning configs with the remote server `<'Logs'>`.

        ## Explanation
        - This method does `NOT` alter the remote server partitioning
          configurations, but only changes the local partitioning to match
          the remote server metadata.
        """
        try:
            meta: PartitioningMetadata = self.ShowMetadata()
        except sqlerrors.OperationalError as err:
            if err.args[0] == 1505:
                return Logs().log_sync_failed_not_exist(self)
            raise err
        return self._sync_from_metadata(meta)

    @cython.ccall
    def SyncToRemote(self) -> Logs:
        """[sync] Synchronize the remote server partitioning with local configs `<'Logs'>`.

        ## Explanation
        - This method compares the local partitioning configurations with the
          remote server metadata and issues the necessary ALTER TABLE statements
          so that the remote one matches the local settings.
        """
        # Check existence
        if not self.Exists():
            return self.Create()
        # Check difference
        meta = self.ShowMetadata()
        if not self._diff_from_metadata(meta):
            return Logs()  # exit
        # Sync to remote
        sql: str = self._gen_create_sql()
        with self._pool.acquire() as conn:
            with conn.transaction() as cur:
                cur.execute(sql)
        logs = Logs().log_sql(self, sql)
        # Sync from remote
        return logs.extend(self.SyncFromRemote())

    def AddPartition(self, *partitions: int | Partition) -> Logs:
        """[sync] Add partitions to the table `<'Logs'>`.

        :param partitions `<'*int/Partition'>`: The number of partitions or the partition instances to add to the table.

        - For PARTITION BY [LINEAR] HASH/KEY, argument `partitions` should be an integer greater than 0.
        - For PARTITION BY RANGE/LIST [COLUMNS], argument `partitions` should be <'Partition'> instances.
        """
        # Execute alteration
        sql: str = self._gen_add_partition_sql(partitions)
        with self._pool.acquire() as conn:
            with conn.transaction() as cur:
                cur.execute(sql)
        logs = Logs().log_sql(self, sql)
        # Sync from remote
        return logs.extend(self.SyncFromRemote())

    @cython.ccall
    def ExistsPartition(self, partition: str | Partition) -> cython.bint:
        """[sync] Check if a specific partition exists `<'bool'>`.

        :param partition `<'str/Partition'>`: The name or the instance of the partition to check.
        """
        sql: str = self._gen_exists_partition_sql(partition)
        with self._pool.acquire() as conn:
            with conn.transaction() as cur:
                cur.execute(sql)
                res = cur.fetchone()
        return res is not None

    def TruncatePartition(self, *partitions: str | Partition) -> Logs:
        """[sync] Truncate partitions of the table `<'Logs'>`.

        :param partitions `<'*str/Partition'>`: The name or instance of the partition(s) to truncate.

        ## Notice
        - To truncate all partitions, set partitions to `"ALL"`.
        """
        # Execute alteration
        sql: str = self._gen_general_partition_sql(partitions, "TRUNCATE")
        with self._pool.acquire() as conn:
            with conn.transaction() as cur:
                cur.execute(sql)
        logs = Logs().log_sql(self, sql)
        # Sync from remote
        return logs.extend(self.SyncFromRemote())

    def DropPartition(self, *partitions: str | Partition) -> Logs:
        """[sync] Drop partitions of the table `<'Logs'>`.

        :param partitions `<'*str/Partition'>`: The name or instance of the partition(s) to drop.

        ## Notice
        - This method is only applicable to PARTITION BY RANGE/LIST [COLUMNS].
        """
        # Execute alteration
        sql: str = self._gen_drop_partition_sql(partitions)
        with self._pool.acquire() as conn:
            with conn.transaction() as cur:
                cur.execute(sql)
        logs = Logs().log_sql(self, sql)
        # Sync from remote
        return logs.extend(self.SyncFromRemote())

    @cython.ccall
    def EmptyPartition(self, partition: str | Partition) -> cython.bint:
        """[sync] Check if a specific partition is empty `<'bool'>`.

        :param partition `<'str/Partition'>`: The name or the instance of the partition to check.
        """
        sql: str = self._gen_empty_partition_sql(partition)
        with self._pool.acquire() as conn:
            with conn.transaction() as cur:
                cur.execute(sql)
                res = cur.fetchone()
        return res is None

    def ReorganizePartition(
        self,
        old_partitions: object,
        *new_partitions: Partition,
    ) -> Logs:
        """[sync] Reorganize old partitions into new ones `<'Logs'>`.

        :param old_partitions `<'str/Partition/list/tuple'>`: The name or instance of existing partition(s) to reorganize.
        :param new_partitions `<'*Partition'>`: New partition(s) to reorganize the existing one(s) into.

        ## Notice
        - This method is only applicable to PARTITION BY RANGE/LIST [COLUMNS].
        - It changes the partition structure, but data remains intact.
        """
        # Execute alteration
        sql: str = self._gen_reorganize_partition_sql(old_partitions, new_partitions)
        with self._pool.acquire() as conn:
            with conn.transaction() as cur:
                cur.execute(sql)
        logs = Logs().log_sql(self, sql)
        # Sync from remote
        return logs.extend(self.SyncFromRemote())

    @cython.ccall
    def CoalescePartition(self, number: cython.int) -> Logs:
        """[sync] Reduce the specified number of partitions `<'Logs'>`.

        :param number `<'int'>`: The number of partitions to removed.

        ## Notice
        - This method is only applicable to PARTITION BY [LINEAR] HASH/KEY.
        - It changes the partition number, but data remains intact.
        """
        # Execute alteration
        sql: str = self._gen_coalesce_partition_sql(number)
        with self._pool.acquire() as conn:
            with conn.transaction() as cur:
                cur.execute(sql)
        logs = Logs().log_sql(self, sql)
        # Sync from remote
        return logs.extend(self.SyncFromRemote())

    def RebuildPartition(self, *partitions: str | Partition) -> Logs:
        """[sync] Rebuild partitions of the table `<'Logs'>`.

        :param partitions `<'*str/Partition'>`: The name or instance of the partition(s) to rebuild.

        ## Notice
        - To rebuild all partitions, set partitions to `"ALL"`.
        """
        sql: str = self._gen_general_partition_sql(partitions, "REBUILD")
        with self._pool.acquire() as conn:
            with conn.transaction(DictCursor) as cur:
                cur.execute(sql)
        return Logs().log_sql(self, sql)

    def AnalyzePartition(self, *partitions: str | Partition) -> tuple[dict]:
        """[sync] Analyze partitions of the table `<'tuple[dict]'>`.

        :param partitions `<'*str/Partition'>`: The name or instance of the partition(s) to analyze.
        :returns `<'tuple[dict]'>`: The analyze operation result.

        ## Notice
        - To analyze all partitions, set partitions to `"ALL"`.
        """
        sql: str = self._gen_general_partition_sql(partitions, "ANALYZE")
        with self._pool.acquire() as conn:
            with conn.transaction(DictCursor) as cur:
                cur.execute(sql)
                return cur.fetchall()

    def CheckPartition(self, *partitions: str | Partition) -> tuple[dict]:
        """[sync] Check partitions of the table `<'tuple[dict]'>`.

        :param partitions `<'*str/Partition'>`: The name or instance of the partition(s) to check.
        :returns `<'tuple[dict]'>`: The check operation result.

        ## Notice
        - To check all partitions, set partitions to `"ALL"`.
        """
        sql: str = self._gen_general_partition_sql(partitions, "CHECK")
        with self._pool.acquire() as conn:
            with conn.transaction(DictCursor) as cur:
                cur.execute(sql)
                return cur.fetchall()

    def OptimizePartition(self, *partitions: str | Partition) -> tuple[dict]:
        """[sync] Optimize partition(s) of the table `<'tuple[dict]'>`.

        :param partitions `<'*str/Partition'>`: The name or instance of the partition(s) to optimize.
        :returns `<'tuple[dict]'>`: The optimize operation result.

        ## Notice
        - To optimize all partitions, set partitions to `"ALL"`.
        """
        sql: str = self._gen_general_partition_sql(partitions, "OPTIMIZE")
        with self._pool.acquire() as conn:
            with conn.transaction(DictCursor) as cur:
                cur.execute(sql)
                return cur.fetchall()

    def RepairPartition(self, *partitions: str | Partition) -> tuple[dict]:
        """[sync] Repair partition(s) of the table `<'tuple[dict]'>`.

        :param partitions `<'*str/Partition'>`: The name or instance of the partition(s) to repair.
        :returns `<'tuple[dict]'>`: The repair operation result.

        ## Notice
        - To repair all partitions, set partitions to `"ALL"`.
        """
        sql: str = self._gen_general_partition_sql(partitions, "REPAIR")
        with self._pool.acquire() as conn:
            with conn.transaction(DictCursor) as cur:
                cur.execute(sql)
                return cur.fetchall()

    # Async --------------------------------------------------------------------------------
    async def aioInitialize(self, force: cython.bint = False) -> Logs:
        """[async] Initialize the table partitioning `<'Logs'>`.

        :param force `<'bool'>`: Whether to force the initialization even
            the table partitioning has already been initialized. Defaults to `False`.

        ## Explanation (difference from create)
        - Create the partitioning of the table if not exists.
        """
        logs: Logs = Logs()
        # Skip
        if self._initialized and not force:
            return logs
        # Initizlize: partitioning
        if not await self.aioExists():
            logs.extend(await self.aioCreate())
        else:
            logs.extend(await self.aioSyncFromRemote())
        # Finished
        self._set_initialized(True)
        return logs

    async def aioCreate(self) -> Logs:
        """[async] Create the partitioning of the table `<'Logs'>`.

        :raises `<'OperationalError'>`: If the table is already partitioned.
        """
        # Check existence
        if await self.aioExists():
            self._raise_operational_error(1734, "already exists")
        # Execute creation
        sql: str = self._gen_create_sql()
        async with self._pool.acquire() as conn:
            async with conn.transaction() as cur:
                await cur.execute(sql)
        logs = Logs().log_sql(self, sql)
        logs.log_element_creation(self, False)
        # Sync from remote
        return logs.extend(await self.aioSyncFromRemote())

    async def aioExists(self) -> bool:
        """[sync] Check if the table is partitioned `<'str'>`."""
        sql: str = self._gen_exists_sql()
        async with self._pool.acquire() as conn:
            async with conn.transaction() as cur:
                await cur.execute(sql)
                res = await cur.fetchone()
        if res is None:
            self._set_initialized(False)
            return False
        return True

    async def aioRemove(self) -> Logs:
        """[async] Remove the partitioning of the table `<'Logs'>`.

        :raises `<'OperationalError'>`: If the table is not partitioned.
        """
        sql: str = self._gen_remove_sql()
        async with self._pool.acquire() as conn:
            async with conn.transaction() as cur:
                await cur.execute(sql)
        self._set_initialized(False)
        logs = Logs().log_sql(self, sql)
        return logs.log_element_deletion(self, False)

    async def aioShowMetadata(self) -> PartitioningMetadata:
        """[async] Show the partitioning metadata from the remote server `<'PartitioningMetadata'>`.

        :raises `<'OperationalError'>`: If the table is not partitioned.
        """
        sql: str = self._gen_show_metadata_sql()
        async with self._pool.acquire() as conn:
            conn.set_decode_json(False)
            async with conn.transaction(AioDictCursor) as cur:
                await cur.execute(sql)
                res = await cur.fetchall()
        if tuple_len(res) == 0:
            self._raise_operational_error(1505, "does not exist")
        return PartitioningMetadata(res)

    async def aioShowPartitionNames(self) -> tuple[str]:
        """[async] Show all the main partition names of the table
        (sorted by partition ordinal position) `<'tuple[str]'>`.

        ## Notice
        - Only returns the main partition names (not subpartitions).
        """
        sql: str = self._gen_show_partition_names_sql()
        async with self._pool.acquire() as conn:
            async with conn.transaction() as cur:
                await cur.execute(sql)
                res: tuple = await cur.fetchall()
        res = self._flatten_rows(res)
        if tuple_len(res) != self._partitions._size:
            await self.aioSyncFromRemote()
        return res

    async def aioShowPartitionRows(self) -> dict[str, int]:
        """[async] Show the number of estimated rows in
        each of the main partitions `<'dict[str, int]'>`.

        ## Notice
        - Only returns the main partitions' estimated row counts (not subpartitions).
        - This method will first execute `ANALYZE TABLE`, so it might take some
          time for a large partitioned table.
        """
        sql: str = self._gen_show_partition_rows_sql()
        async with self._pool.acquire() as conn:
            async with conn.transaction() as cur:
                await cur.execute("ANALYZE TABLE %s;" % self._tb_qualified_name)
                await cur.execute(sql)
                res: tuple = await cur.fetchall()
        r: tuple
        return {r[0]: r[1] for r in res}

    async def aioShowSubpartitionNames(self) -> tuple[str]:
        """[async] Show all the subpartition names of the table
        (sorted by subpartition ordinal position) `<'tuple[str]'>`.

        ## Notice
        - Only returns the subpartition names (not main partitions).
        """
        sql: str = self._gen_show_subpartition_names_sql()
        async with self._pool.acquire() as conn:
            async with conn.transaction() as cur:
                await cur.execute(sql)
                res = await cur.fetchall()
        return self._flatten_rows(res)

    async def aioShowSubpartitionRows(self) -> dict[str, int]:
        """[async] Show the number of estimated rows in
        each of the subpartitions `<'dict[str, int]'>`.

        ## Notice
        - Only returns the subpartitions' estimated row counts (not main partitions).
        - This method will first execute `ANALYZE TABLE`, so it might take some
          time for a large partitioned table.
        """
        sql: str = self._gen_show_subpartition_rows_sql()
        async with self._pool.acquire() as conn:
            async with conn.transaction() as cur:
                await cur.execute("ANALYZE TABLE %s;" % self._tb_qualified_name)
                await cur.execute(sql)
                res: tuple = await cur.fetchall()
        r: tuple
        return {r[0]: r[1] for r in res}

    async def aioSyncFromRemote(self) -> Logs:
        """[async] Synchronize the local partitioning configs with the remote server `<'Logs'>`.

        ## Explanation
        - This method does `NOT` alter the remote server partitioning
          configurations, but only changes the local partitioning to match
          the remote server metadata.
        """
        try:
            meta: PartitioningMetadata = await self.aioShowMetadata()
        except sqlerrors.OperationalError as err:
            if err.args[0] == 1505:
                return Logs().log_sync_failed_not_exist(self)
            raise err
        return self._sync_from_metadata(meta)

    async def aioSyncToRemote(self) -> Logs:
        """[async] Synchronize the remote server partitioning with local configs `<'Logs'>`.

        ## Explanation
        - This method compares the local partitioning configurations with the
          remote server metadata and issues the necessary ALTER TABLE statements
          so that the remote one matches the local settings.
        """
        # Check existence
        if not await self.aioExists():
            return await self.aioCreate()
        # Check difference
        meta = await self.aioShowMetadata()
        if not self._diff_from_metadata(meta):
            return Logs()  # exit
        # Sync to remote
        sql: str = self._gen_create_sql()
        async with self._pool.acquire() as conn:
            async with conn.transaction() as cur:
                await cur.execute(sql)
        logs = Logs().log_sql(self, sql)
        # Sync from remote
        return logs.extend(await self.aioSyncFromRemote())

    async def aioAddPartition(self, *partitions: int | Partition) -> Logs:
        """[async] Add partitions to the table `<'Logs'>`.

        :param partitions `<'*int/Partition'>`: The number of partitions or the partition instances to add to the table.

        - For PARTITION BY [LINEAR] HASH/KEY, argument `partitions` should be an integer greater than 0.
        - For PARTITION BY RANGE/LIST [COLUMNS], argument `partitions` should be <'Partition'> instances.
        """
        # Execute alteration
        sql: str = self._gen_add_partition_sql(partitions)
        async with self._pool.acquire() as conn:
            async with conn.transaction() as cur:
                await cur.execute(sql)
        logs = Logs().log_sql(self, sql)
        # Sync from remote
        return logs.extend(await self.aioSyncFromRemote())

    async def aioExistsPartition(self, partition: str | Partition) -> bool:
        """[async] Check if a specific partition exists `<'bool'>`.

        :param partition `<'str/Partition'>`: The name or the instance of the partition to check.
        """
        sql: str = self._gen_exists_partition_sql(partition)
        async with self._pool.acquire() as conn:
            async with conn.transaction() as cur:
                await cur.execute(sql)
                res = await cur.fetchone()
        return res is not None

    async def aioTruncatePartition(self, *partitions: str | Partition) -> Logs:
        """[async] Truncate partitions of the table `<'Logs'>`.

        :param partitions `<'*str/Partition'>`: The name or instance of the partition(s) to truncate.

        ## Notice
        - To truncate all partitions, set partitions to `"ALL"`.
        """
        # Execute alteration
        sql: str = self._gen_general_partition_sql(partitions, "TRUNCATE")
        async with self._pool.acquire() as conn:
            async with conn.transaction() as cur:
                await cur.execute(sql)
        logs = Logs().log_sql(self, sql)
        # Sync from remote
        return logs.extend(await self.aioSyncFromRemote())

    async def aioDropPartition(self, *partitions: str | Partition) -> Logs:
        """[async] Drop partitions of the table `<'Logs'>`.

        :param partitions `<'*str/Partition'>`: The name or instance of the partition(s) to drop.

        ## Notice
        - This method is only applicable to PARTITION BY RANGE/LIST [COLUMNS].
        """
        # Execute alteration
        sql: str = self._gen_drop_partition_sql(partitions)
        async with self._pool.acquire() as conn:
            async with conn.transaction() as cur:
                await cur.execute(sql)
        logs = Logs().log_sql(self, sql)
        # Sync from remote
        return logs.extend(await self.aioSyncFromRemote())

    async def aioEmptyPartition(self, partition: str | Partition) -> bool:
        """[async] Check if a specific partition is empty `<'bool'>`.

        :param partition `<'str/Partition'>`: The name or the instance of the partition to check.
        """
        sql: str = self._gen_empty_partition_sql(partition)
        async with self._pool.acquire() as conn:
            async with conn.transaction() as cur:
                await cur.execute(sql)
                res = await cur.fetchone()
        return res is None

    async def aioReorganizePartition(
        self,
        old_partitions: object,
        *new_partitions: Partition,
    ) -> Logs:
        """[async] Reorganize old partitions into new ones `<'Logs'>`.

        :param old_partitions `<'str/Partition/list/tuple'>`: The name or instance of existing partition(s) to reorganize.
        :param new_partitions `<'*Partition'>`: New partition(s) to reorganize the existing one(s) into.

        ## Notice
        - This method is only applicable to PARTITION BY RANGE/LIST [COLUMNS].
        - It changes the partition structure, but data remains intact.
        """
        # Execute alteration
        sql: str = self._gen_reorganize_partition_sql(old_partitions, new_partitions)
        async with self._pool.acquire() as conn:
            async with conn.transaction() as cur:
                await cur.execute(sql)
        logs = Logs().log_sql(self, sql)
        # Sync from remote
        return logs.extend(await self.aioSyncFromRemote())

    async def aioCoalescePartition(self, number: cython.int) -> Logs:
        """[async] Reduce the specified number of partitions `<'Logs'>`.

        :param number `<'int'>`: The number of partitions to removed.

        ## Notice
        - This method is only applicable to PARTITION BY [LINEAR] HASH/KEY.
        - It changes the partition number, but data remains intact.
        """
        # Execute alteration
        sql: str = self._gen_coalesce_partition_sql(number)
        async with self._pool.acquire() as conn:
            async with conn.transaction() as cur:
                await cur.execute(sql)
        logs = Logs().log_sql(self, sql)
        # Sync from remote
        return logs.extend(await self.aioSyncFromRemote())

    async def aioRebuildPartition(self, *partitions: str | Partition) -> Logs:
        """[async] Rebuild partitions of the table `<'Logs'>`.

        :param partitions `<'*str/Partition'>`: The name or instance of the partition(s) to rebuild.

        ## Notice
        - To rebuild all partitions, set partitions to `"ALL"`.
        """
        sql: str = self._gen_general_partition_sql(partitions, "REBUILD")
        async with self._pool.acquire() as conn:
            async with conn.transaction(AioDictCursor) as cur:
                await cur.execute(sql)
        return Logs().log_sql(self, sql)

    async def aioAnalyzePartition(self, *partitions: str | Partition) -> tuple[dict]:
        """[async] Analyze partitions of the table `<'tuple[dict]'>`.

        :param partitions `<'*str/Partition'>`: The name or instance of the partition(s) to analyze.
        :returns `<'tuple[dict]'>`: The analyze operation result.

        ## Notice
        - To analyze all partitions, set partitions to `"ALL"`.
        """
        sql: str = self._gen_general_partition_sql(partitions, "ANALYZE")
        async with self._pool.acquire() as conn:
            async with conn.transaction(AioDictCursor) as cur:
                await cur.execute(sql)
                return await cur.fetchall()

    async def aioCheckPartition(self, *partitions: str | Partition) -> tuple[dict]:
        """[async] Check partitions of the table `<'tuple[dict]'>`.

        :param partitions `<'*str/Partition'>`: The name or instance of the partition(s) to check.
        :returns `<'tuple[dict]'>`: The check operation result.

        ## Notice
        - To check all partitions, set partitions to `"ALL"`.
        """
        sql: str = self._gen_general_partition_sql(partitions, "CHECK")
        async with self._pool.acquire() as conn:
            async with conn.transaction(AioDictCursor) as cur:
                await cur.execute(sql)
                return await cur.fetchall()

    async def aioOptimizePartition(self, *partitions: str | Partition) -> tuple[dict]:
        """[async] Optimize partition(s) of the table `<'tuple[dict]'>`.

        :param partitions `<'*str/Partition'>`: The name or instance of the partition(s) to optimize.
        :returns `<'tuple[dict]'>`: The optimize operation result.

        ## Notice
        - To optimize all partitions, set partitions to `"ALL"`.
        """
        sql: str = self._gen_general_partition_sql(partitions, "OPTIMIZE")
        async with self._pool.acquire() as conn:
            async with conn.transaction(AioDictCursor) as cur:
                await cur.execute(sql)
                return await cur.fetchall()

    async def aioRepairPartition(self, *partitions: str | Partition) -> tuple[dict]:
        """[sync] Repair partition(s) of the table `<'tuple[dict]'>`.

        :param partitions `<'*str/Partition'>`: The name or instance of the partition(s) to repair.
        :returns `<'tuple[dict]'>`: The repair operation result.

        ## Notice
        - To repair all partitions, set partitions to `"ALL"`.
        """
        sql: str = self._gen_general_partition_sql(partitions, "REPAIR")
        async with self._pool.acquire() as conn:
            async with conn.transaction(AioDictCursor) as cur:
                await cur.execute(sql)
                return await cur.fetchall()

    # Generate SQL -------------------------------------------------------------------------
    @cython.ccall
    def _gen_definition_sql(self) -> str:
        """(internal) Generate the definition SQL of the partitioning `<'str'>`."""
        self._assure_ready()
        # . BY [LINEAR] HASH/KEY
        if utils.is_partition_by_hashkey(self._partitioning_flag):
            return "PARTITION BY %s (%s) PARTITIONS %d" % (
                self._partitioning_flag_to_method(self._partitioning_flag),
                self._partitioning_expression,
                self._partitions._size,
            )

        # . BY RANGE/LIST [COLUMNS]
        if self._subpartitioning_flag == utils.PARTITIONING_METHOD.NONSET:
            # . normal partitions
            return "PARTITION BY %s (%s) (\n%s\n)" % (
                self._partitioning_flag_to_method(self._partitioning_flag),
                self._partitioning_expression,
                self._partitions._gen_definition_sql(1),
            )
        else:
            # . subpartitions
            return (
                "PARTITION BY %s (%s)\n"
                "SUBPARTITION BY %s (%s) SUBPARTITIONS %d (\n%s\n)"
                % (
                    self._partitioning_flag_to_method(self._partitioning_flag),
                    self._partitioning_expression,
                    self._partitioning_flag_to_method(self._subpartitioning_flag),
                    self._subpartitioning_expression,
                    self._partitions._subpartition_count,
                    self._partitions._gen_definition_sql(1),
                )
            )

    @cython.ccall
    def _gen_create_sql(self) -> str:
        """(internal) Generate SQL to create the partitioning of the table `<'str'>`."""
        self._assure_ready()
        return "ALTER TABLE %s \n%s;" % (
            self._tb_qualified_name,
            self._gen_definition_sql(),
        )

    @cython.ccall
    def _gen_exists_sql(self) -> str:
        """(internal) Generate SQL to check if the table is partitioned `<'str'>`."""
        self._assure_ready()
        return (
            "SELECT 1 "
            "FROM INFORMATION_SCHEMA.PARTITIONS "
            "WHERE TABLE_NAME = '%s' "
            "AND TABLE_SCHEMA = '%s' "
            "AND PARTITION_ORDINAL_POSITION = 1 "
            "LIMIT 1;" % (self._tb_name, self._db_name)
        )

    @cython.ccall
    def _gen_remove_sql(self) -> str:
        """(internal) Generate SQL to remove the partitioning of the table `<'str'>`."""
        self._assure_ready()
        return "ALTER TABLE %s REMOVE PARTITIONING;" % self._tb_qualified_name

    @cython.ccall
    def _gen_show_metadata_sql(self) -> str:
        """(internal) Generate SQL to show the partitioning metadata `<'str'>`."""
        self._assure_ready()
        return (
            "SELECT "
            # . columns
            "TABLE_CATALOG AS CATALOG_NAME, "
            "TABLE_SCHEMA AS SCHEMA_NAME, "
            "TABLE_NAME AS TABLE_NAME, "
            "PARTITION_NAME AS PARTITION_NAME, "
            "PARTITION_ORDINAL_POSITION AS PARTITION_ORDINAL_POSITION, "
            "UPPER(PARTITION_METHOD) AS PARTITION_METHOD, "
            "PARTITION_EXPRESSION AS PARTITION_EXPRESSION, "
            "SUBPARTITION_NAME AS SUBPARTITION_NAME, "
            "SUBPARTITION_ORDINAL_POSITION AS SUBPARTITION_ORDINAL_POSITION, "
            "UPPER(SUBPARTITION_METHOD) AS SUBPARTITION_METHOD, "
            "SUBPARTITION_EXPRESSION AS SUBPARTITION_EXPRESSION, "
            "PARTITION_DESCRIPTION AS PARTITION_DESCRIPTION, "
            "TABLE_ROWS AS TABLE_ROWS, "
            "AVG_ROW_LENGTH AS AVG_ROW_LENGTH, "
            "DATA_LENGTH AS DATA_LENGTH, "
            "MAX_DATA_LENGTH AS MAX_DATA_LENGTH, "
            "INDEX_LENGTH AS INDEX_LENGTH, "
            "DATA_FREE AS DATA_FREE, "
            "CREATE_TIME AS CREATE_TIME, "
            "UPDATE_TIME AS UPDATE_TIME, "
            "CHECK_TIME AS CHECK_TIME, "
            "CHECKSUM AS CHECKSUM, "
            "PARTITION_COMMENT AS PARTITION_COMMENT, "
            "NODEGROUP AS NODEGROUP, "
            "TABLESPACE_NAME AS TABLESPACE_NAME "
            # . information_schema.partitions
            "FROM INFORMATION_SCHEMA.PARTITIONS "
            # . conditions
            "WHERE TABLE_NAME = '%s' "
            "AND TABLE_SCHEMA = '%s' "
            "AND PARTITION_NAME IS NOT NULL "
            "ORDER BY PARTITION_ORDINAL_POSITION ASC, SUBPARTITION_ORDINAL_POSITION ASC;"
            % (self._tb_name, self._db_name)
        )

    @cython.ccall
    def _gen_show_partition_names_sql(self) -> str:
        """(internal) Generate SQL to select all the main partition names
        of the table (sorted by partition ordinal position) `<'str'>`.

        ## Notice
        - Only selects the main partition names (not subpartitions).
        """
        self._assure_ready()
        return (
            "SELECT PARTITION_NAME AS i "
            "FROM INFORMATION_SCHEMA.PARTITIONS "
            "WHERE TABLE_NAME = '%s' "
            "AND TABLE_SCHEMA = '%s' "
            "AND PARTITION_NAME IS NOT NULL "
            "GROUP BY PARTITION_NAME "
            "ORDER BY MIN(PARTITION_ORDINAL_POSITION) ASC;"
            % (self._tb_name, self._db_name)
        )

    @cython.ccall
    def _gen_show_partition_rows_sql(self) -> str:
        """(internal) Generate SQL to select the number of estimated
        rows in each of the main partitions `<'str'>`.

        ## Notice
        - Only selects the main partitions (not subpartitions).
        """
        self._assure_ready()
        return (
            "SELECT "
            "PARTITION_NAME AS i, "
            "CAST(SUM(TABLE_ROWS) AS UNSIGNED) AS j "
            "FROM INFORMATION_SCHEMA.PARTITIONS "
            "WHERE TABLE_NAME = '%s' "
            "AND TABLE_SCHEMA = '%s' "
            "AND PARTITION_NAME IS NOT NULL "
            "GROUP BY PARTITION_NAME "
            "ORDER BY MIN(PARTITION_ORDINAL_POSITION) ASC;"
            % (self._tb_name, self._db_name)
        )

    @cython.ccall
    def _gen_show_subpartition_names_sql(self) -> str:
        """(internal) Generate SQL to select all the subpartition names
        of the table (sorted by subpartition ordinal position) `<'str'>`.

        ## Notice
        - Only selects the subpartition names (not main partitions).
        """
        self._assure_ready()
        return (
            "SELECT SUBPARTITION_NAME AS i "
            "FROM INFORMATION_SCHEMA.PARTITIONS "
            "WHERE TABLE_NAME = '%s' "
            "AND TABLE_SCHEMA = '%s' "
            "AND SUBPARTITION_NAME IS NOT NULL "
            "ORDER BY PARTITION_ORDINAL_POSITION ASC, SUBPARTITION_ORDINAL_POSITION ASC;"
            % (self._tb_name, self._db_name)
        )

    @cython.ccall
    def _gen_show_subpartition_rows_sql(self) -> str:
        """(internal) Generate SQL to select the number of estimated
        rows in each of the subpartitions `<'str'>`.

        ## Notice
        - Only selects the subpartitions (not main partitions).
        """
        self._assure_ready()
        return (
            "SELECT "
            "SUBPARTITION_NAME AS i, "
            "TABLE_ROWS AS j "
            "FROM INFORMATION_SCHEMA.PARTITIONS "
            "WHERE TABLE_NAME = '%s' "
            "AND TABLE_SCHEMA = '%s' "
            "AND SUBPARTITION_NAME IS NOT NULL "
            "ORDER BY PARTITION_ORDINAL_POSITION ASC, SUBPARTITION_ORDINAL_POSITION ASC;"
            % (self._tb_name, self._db_name)
        )

    @cython.ccall
    def _gen_add_partition_sql(self, partitions: tuple) -> str:
        """(internal) Generate SQL to add partitions to the table `<'str'>`."""
        self._assure_ready()
        if tuple_len(partitions) == 0:
            self._raise_argument_error("must specify at least one partition to add.")

        # . [LINEAR] HASH/KEY
        if utils.is_partition_by_hashkey(self._partitioning_flag):
            i = partitions[0]
            if not isinstance(i, int):
                self._raise_argument_error(
                    "expects an integer as the number of partitions to add, "
                    "instead got %s %r." % (type(i), i)
                )
            num: cython.int = i
            if num < 1:
                self._raise_argument_error(
                    "expects an integer greater than 0 as the number of "
                    "partitions to add, instead got %d." % num
                )
            return "ALTER TABLE %s ADD PARTITION PARTITIONS %d;" % (
                self._tb_qualified_name,
                num,
            )

        # . RANGE/LIST [COLUMNS]
        else:
            pts: Partitions = Partitions(*partitions)
            pts._set_partitioning_flag(self._partitioning_flag)
            pts.setup(self._tb_name, self._db_name, self._charset, None, self._pool)
            return "ALTER TABLE %s ADD PARTITION (\n%s\n);" % (
                self._tb_qualified_name,
                pts._gen_definition_sql(1),
            )

    @cython.ccall
    def _gen_exists_partition_sql(self, partition: object) -> str:
        """(internal) Generate SQL to check if a specific partition exists `<'str'>`."""
        self._assure_ready()
        return (
            "SELECT 1 "
            "FROM INFORMATION_SCHEMA.PARTITIONS "
            "WHERE TABLE_NAME = '%s' "
            "AND TABLE_SCHEMA = '%s' "
            "AND PARTITION_NAME = '%s' "
            "LIMIT 1;"
            % (self._tb_name, self._db_name, self._validate_partition(partition))
        )

    @cython.ccall
    def _gen_drop_partition_sql(self, partitions: tuple) -> str:
        """(internal) Generate SQL to drop partitions of the table `<'str'>`."""
        self._assure_ready()
        partitions = self._validate_partitions(partitions)
        if tuple_len(partitions) == 0:
            self._raise_argument_error(
                "must specify at least one partition to perform the DROP operation.\n"
                "Invalid 'partitions' argument: %s." % repr(partitions)
            )
        if not utils.is_partition_by_rangelist(self._partitioning_flag):
            self._raise_operational_error(1512, "does not support DROP PARTITION")
        return "ALTER TABLE %s DROP PARTITION %s;" % (
            self._tb_qualified_name,
            ", ".join(partitions),
        )

    @cython.ccall
    def _gen_empty_partition_sql(self, partition: object) -> str:
        """(internal) Generate SQL to check if a specific partition is empty `<'str'>`."""
        self._assure_ready()
        return "SELECT 1 FROM %s PARTITION (%s) LIMIT 1;" % (
            self._tb_qualified_name,
            self._validate_partition(partition),
        )

    @cython.ccall
    def _gen_reorganize_partition_sql(
        self,
        old_partitions: object,
        new_partitions: tuple,
    ) -> str:
        """(internal) Generate SQL to reorganize old partitions into new ones `<'str'>`."""
        self._assure_ready()
        if not utils.is_partition_by_rangelist(self._partitioning_flag):
            self._raise_operational_error(1510, "does not support REORGANIZE PARTITION")
        # Old partitions to reorganize
        old_pts = self._validate_partitions(old_partitions)
        if tuple_len(old_pts) == 0:
            self._raise_argument_error(
                "must specify at least one existing partition to reorganize.\n"
                "Invalid 'old_partitions' argument: %s." % repr(old_partitions)
            )
        # New partitions to reorganize into
        new_pts: Partitions = Partitions(*new_partitions)
        if new_pts._size == 0:
            self._raise_argument_error(
                "must specify at least one new partition to reorganize into.\n"
                "Invalid 'new_partitions' argument: %s." % repr(new_partitions)
            )
        new_pts._set_partitioning_flag(self._partitioning_flag)
        new_pts.setup(self._tb_name, self._db_name, self._charset, None, self._pool)
        return "ALTER TABLE %s REORGANIZE PARTITION %s INTO (\n%s\n);" % (
            self._tb_qualified_name,
            ", ".join(old_pts),
            new_pts._gen_definition_sql(1),
        )

    @cython.ccall
    def _gen_coalesce_partition_sql(self, number: cython.int) -> str:
        """(internal) Generate SQL to reduce the specified number of partitions `<'str'>`."""
        self._assure_ready()
        if not utils.is_partition_by_hashkey(self._partitioning_flag):
            self._raise_operational_error(1509, "does not support COALESCE PARTITION")
        if number < 1:
            self._raise_argument_error(
                "coalesce partition number must be greater than 0, "
                "instead got %d." % number
            )
        return "ALTER TABLE %s COALESCE PARTITION %d;" % (
            self._tb_qualified_name,
            number,
        )

    @cython.ccall
    def _gen_general_partition_sql(self, partitions: tuple, operation: str) -> str:
        """(internal) Generate SQL to perform general partition operation `<'str'>`

        Supported general partition operations:
        - `"TRUNCATE"`
        - `"ANALYZE"`
        - `"OPTIMIZE"`
        - `"REBUILD"`
        - `"REPAIR"`
        """
        self._assure_ready()
        # ALL partitions
        if tuple_len(partitions) == 1 and partitions[0] == "ALL":
            return "ALTER TABLE %s %s PARTITION ALL;" % (
                self._tb_qualified_name,
                operation,
            )

        # Specific partitions
        partitions = self._validate_partitions(partitions)
        if tuple_len(partitions) == 0:
            self._raise_argument_error(
                "must specify at least one partition to perform the %s operation.\n"
                "Invalid 'partitions' argument: %r." % (operation, partitions)
            )
        return "ALTER TABLE %s %s PARTITION %s;" % (
            self._tb_qualified_name,
            operation,
            ", ".join(partitions),
        )

    # Metadata -----------------------------------------------------------------------------
    @cython.ccall
    def _sync_from_metadata(
        self,
        meta: PartitioningMetadata,
        logs: Logs = None,
    ) -> Logs:
        """(internal) Synchronize local configs with the remote partitioning metadata `<'Logs'>`."""
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

        # Partitioning flag
        pt_flag: cython.int = self._partitioning_method_to_flag(
            meta._partitioning_method
        )
        if self._partitioning_flag != pt_flag:
            logs.log_config_obj(
                self,
                "partitioning_method",
                self._partitioning_flag_to_method(self._partitioning_flag),
                meta._partitioning_method,
            )
            self._partitioning_flag = pt_flag
            self._partitions._partitioning_flag = pt_flag

        # Partitioning expression
        if self._partitioning_expression != meta._partitioning_expression:
            logs.log_config_obj(
                self,
                "partitioning_expression",
                self._partitioning_expression,
                meta._partitioning_expression,
            )
            self._partitioning_expression = meta._partitioning_expression

        # Reset Subpartitioning
        if meta._subpartitioning_method is None:
            if self._subpartitioning_flag != utils.PARTITIONING_METHOD.NONSET:
                # fmt: off
                # . subpartitioning flag
                logs.log_config_obj(
                    self, "subpartitioning_method",
                    self._partitioning_flag_to_method(self._subpartitioning_flag), None,
                )
                self._subpartitioning_flag = utils.PARTITIONING_METHOD.NONSET
                self._partitions._subpartitioning_flag = utils.PARTITIONING_METHOD.NONSET
                # . subpartitioning expression
                logs.log_config_obj(
                    self, "subpartitioning_expression",
                    self._subpartitioning_expression, None,
                )
                self._subpartitioning_expression = None
                # . reset subpartitions
                logs.log_config_int(
                    self, "subpartitions",
                    self._partitions._subpartition_count, 0,
                )
                self._partitions._reset_subpartitions()
                # fmt: on

        # Update Subpartitioning
        else:
            pt_flag = self._partitioning_method_to_flag(meta._subpartitioning_method)
            # . subpartitioning flag
            if self._subpartitioning_flag != pt_flag:
                logs.log_config_obj(
                    self,
                    "subpartitioning_method",
                    self._partitioning_flag_to_method(self._subpartitioning_flag),
                    meta._subpartitioning_method,
                )
                self._subpartitioning_flag = pt_flag
                self._partitions._subpartitioning_flag = pt_flag
            # . subpartitioning expression
            if self._subpartitioning_expression != meta._subpartitioning_expression:
                logs.log_config_obj(
                    self,
                    "subpartitioning_expression",
                    self._subpartitioning_expression,
                    meta._subpartitioning_expression,
                )
                self._subpartitioning_expression = meta._subpartitioning_expression

        # Sync Partitions
        subpt_count: cython.int = meta._subpartition_count
        has_subpts: cython.bint = subpt_count > 0
        step: cython.int = max(1, subpt_count)
        i: cython.int = 0
        pt_meta: dict
        # - - - - - - - - - - - - - - - - - - - - - - - - - - -
        tb_name = self._tb_name
        db_name = self._db_name
        charset = self._charset
        pool = self._pool
        # - - - - - - - - - - - - - - - - - - - - - - - - - - -
        self._partitions._subpartition_count = subpt_count
        for pt_meta in meta._partitions:
            with cython.cdivision(True):
                is_mstr_pt: cython.bint = i % step == 0
            # . Main Partition
            if is_mstr_pt:
                pt_name: str = pt_meta["PARTITION_NAME"]
                # . update old partition
                if dict_contains(self._partitions._el_dict, pt_name):
                    pt: Partition = self._partitions._el_dict[pt_name]
                    # (partitioning)
                    pt._partitioning_flag = self._partitioning_flag
                    pt._subpartitioning_flag = self._subpartitioning_flag
                    pt._is_subpartition = False
                    # (subpartitions)
                    if has_subpts and pt._subpartitions is None:
                        pt._setup_subpartitions(self._subpartitioning_flag, subpt_count)
                        pt._subpartitions.setup(tb_name, db_name, charset, None, pool)
                        logs.log_config_int(pt, "subpartitions", 0, subpt_count)
                    # (sync)
                    logs = pt._sync_from_metadata(pt_meta, logs)
                # . add new partition
                else:
                    pt: Partition = Partition(pt_name, None)
                    # (partitioning)
                    pt._partitioning_flag = self._partitioning_flag
                    pt._subpartitioning_flag = self._subpartitioning_flag
                    pt._is_subpartition = False
                    # (subpartitions)
                    if has_subpts:
                        pt._setup_subpartitions(self._subpartitioning_flag, subpt_count)
                    # (add)
                    pt.setup(tb_name, db_name, charset, None, pool)
                    self._partitions.add(pt)
                    logs.log_element_creation(pt, True)
                    # (sync)
                    pt._sync_from_metadata(pt_meta, None)

            # . Subpartition
            if has_subpts:
                subpt_name: str = pt_meta["SUBPARTITION_NAME"]
                # . update old subpartition
                if dict_contains(pt._subpartitions._el_dict, subpt_name):
                    subpt: Partition = pt._subpartitions._el_dict[subpt_name]
                    # (subpartitioning)
                    subpt._partitioning_flag = self._subpartitioning_flag
                    subpt._set_as_subpartition()
                    # (sync)
                    logs = subpt._sync_from_metadata(pt_meta, logs)
                # . add new subpartition
                else:
                    subpt: Partition = Partition(subpt_name, None)
                    # (subpartitioning)
                    subpt._partitioning_flag = self._subpartitioning_flag
                    subpt._set_as_subpartition()
                    # (add)
                    subpt.setup(tb_name, db_name, charset, None, pool)
                    pt._subpartitions.add(subpt)
                    logs.log_element_creation(subpt, True)
                    # (sync)
                    subpt._sync_from_metadata(pt_meta, None)

            # . Next Iteration
            i += 1

        # Drop Partitions
        pts_names: set = set(meta._partition_names)
        subpts_names: set = set(meta._subpartition_names)
        for pt in list(self._partitions._el_dict.values()):
            # . Remove main partition
            if not set_contains(pts_names, pt._name):
                self._partitions.remove(pt)
                logs.log_element_deletion(pt, True)
            # . Remove subpartition
            elif pt._subpartitions is not None:
                for subpt in list(pt._subpartitions._el_dict.values()):
                    if not set_contains(subpts_names, subpt._name):
                        pt._subpartitions.remove(subpt)
                        logs.log_element_deletion(subpt, True)

        # Finished
        return logs

    @cython.ccall
    @cython.exceptval(-1, check=False)
    def _diff_from_metadata(self, meta: PartitioningMetadata) -> cython.bint:
        """(internal) Check if the partitioning configurations are different
        from the remote server metadata `<'bool'>`."""
        # Partitioning Method & Expression
        if (
            self._partitioning_method_to_flag(meta._partitioning_method)
            != self._partitioning_flag
        ):
            return True
        if self._partitioning_expression != meta._partitioning_expression:
            return True

        # Subpartitioning Method & Expression
        if meta._subpartitioning_method is None:
            if self._subpartitioning_flag != utils.PARTITIONING_METHOD.NONSET:
                return True
            if self._subpartitioning_expression is not None:
                return True
        else:
            if (
                self._partitioning_method_to_flag(meta._subpartitioning_method)
                != self._subpartitioning_flag
            ):
                return True
            if self._subpartitioning_expression != meta._subpartitioning_expression:
                return True

        # Partitions
        if self._partitions is None:
            return True
        if self._partitions._size != meta._partition_count:
            return True
        subpt_count: cython.int = meta._subpartition_count
        has_subpts: cython.bint = subpt_count > 0
        step: cython.int = max(1, subpt_count)
        i: cython.int = 0
        pt_meta: dict
        for pt_meta in meta._partitions:
            with cython.cdivision(True):
                is_mstr_pt: cython.bint = i % step == 0
            # . Main Partition
            if is_mstr_pt:
                # . existance
                pt: Partition = self._partitions.get(pt_meta["PARTITION_NAME"])
                if pt is None:
                    return True
                # . ordinal position
                if pt._el_position != pt_meta["PARTITION_ORDINAL_POSITION"]:
                    return True
                # . values
                values = pt_meta["PARTITION_DESCRIPTION"]
                if values is not None:
                    values = self._validate_expression(values)
                if pt._values != values:
                    return True
                #: comment
                comment = self._validate_comment(pt_meta["PARTITION_COMMENT"])
                if pt._comment != comment:
                    return True

            # . Subpartition
            if has_subpts:
                #  . subpartitions existance
                if pt._subpartitions is None:
                    return True
                # . subpartition size
                if pt._subpartitions._size != subpt_count:
                    return True
                # . existance
                subpt: Partition = pt._subpartitions.get(pt_meta["SUBPARTITION_NAME"])
                if subpt is None:
                    return True
                #: ordinal position
                if subpt._el_position != pt_meta["SUBPARTITION_ORDINAL_POSITION"]:
                    return True
                #: comment
                comment = self._validate_comment(pt_meta["PARTITION_COMMENT"])
                if subpt._comment != comment:
                    return True

            # . Next Iteration
            i += 1

        # Same Metadata
        return False

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
        """Setup the partitioning.

        :param tb_name `<'str'>`: The table name of the partitioning.
        :param db_name `<'str'>`: The database name of the partitioning.
        :param charset `<'str/Charset'>`: The charset of the partitioning.
        :param collate `<'str/None'>`: The collation of the partitioning.
        :param pool `<'Pool'>`: The pool of the partitioning.
        """
        self._set_tb_name(tb_name)
        self._set_db_name(db_name)
        self._set_charset(charset, collate)
        self._set_pool(pool)
        if self._partitions is not None:
            self._partitions.setup(tb_name, db_name, charset, None, pool)
        return self._assure_ready()

    @cython.ccall
    @cython.exceptval(-1, check=False)
    def _set_partitioning_flag(self, flag: cython.int) -> cython.bint:
        """(internal) Set the partitioning (method) flag.

        :param flag `<'int'>`: The partitioning (method) flag (1-8).

        ## Conversion table:
        ```python
        - 1  <=>  "HASH"
        - 2  <=>  "LINEAR HASH"
        - 3  <=>  "KEY"
        - 4  <=>  "LINEAR KEY"
        - 5  <=>  "RANGE"
        - 6  <=>  "RANGE COLUMNS"
        - 7  <=>  "LIST"
        - 8  <=>  "LIST COLUMNS"
        ```
        """
        if not utils.is_partitioning_flag_valid(flag):
            self._raise_critical_error(
                "partitioning flag must be between 1 and 8, " "instead got %d." % flag
            )
        if self._partitioning_flag == flag:
            return True
        self._partitioning_flag = flag
        self._set_el_type("PARTITION BY " + self._partitioning_flag_to_method(flag))
        if self._partitions is not None:
            self._partitions._set_partitioning_flag(flag)
        return True

    @cython.ccall
    @cython.exceptval(-1, check=False)
    def _set_partitions_by_instance(self, partitions: tuple[Partition]) -> cython.bint:
        """(internal) Set the partitions by <'Partition'> instances.

        :param partitions `<'tuple[Partition]'>`: The partitions of the table.

        ## Notice
        - Only applicable to PARTITION BY RANGE/LIST [COLUMNS].
        """
        # Validate partitioning flag
        self._assure_partitioning_flag_ready()
        if not utils.is_partition_by_rangelist(self._partitioning_flag):
            self._raise_critical_error(
                "'_set_partitions_by_instance()' is only applicable to "
                "PARTITION BY RANGE/LIST [COLUMNS], not '%s'."
                % self._partitioning_flag_to_method(self._partitioning_flag)
            )

        # Construct partitions
        self._partitions = Partitions(*partitions)
        if self._partitions._size == 0:
            self._raise_definition_error("must have at least one partition.")
        self._partitions._set_partitioning_flag(self._partitioning_flag)
        return True

    @cython.ccall
    @cython.exceptval(-1, check=False)
    def _set_partitions_by_integer(self, partitions: cython.int) -> cython.bint:
        """(internal) Set the partitions by an integer number.

        :param partitions `<'int'>`: The number of the partitions of the table.

        ## Notice
        - Only applicable to PARTITION BY [LINEAR] HASH/KEY.
        """
        # Validate partitioning flag
        self._assure_partitioning_flag_ready()
        if not utils.is_partition_by_hashkey(self._partitioning_flag):
            self._raise_critical_error(
                "'_set_partitions_by_integer()' is only applicable to "
                "PARTITION BY [LINEAR] HASH/KEY, not '%s'."
                % self._partitioning_flag_to_method(self._partitioning_flag)
            )

        # Construct partitions
        if partitions < 1:
            self._raise_definition_error(
                "partitioning number must be greater than 0, "
                "instead got %d." % partitions
            )
        self._partitions = Partitions(
            *[Partition("p%d" % i, None) for i in range(partitions)]
        )
        self._partitions._set_partitioning_flag(self._partitioning_flag)
        return True

    @cython.ccall
    @cython.exceptval(-1, check=False)
    def _reset_subpartitioning(self) -> cython.bint:
        """(internal) Reset subpartitions configurations."""
        self._subpartitioning_flag = utils.PARTITIONING_METHOD.NONSET
        self._subpartitioning_expression = None
        if self._partitions is not None:
            self._partitions._reset_subpartitions()
        return True

    @cython.ccall
    @cython.exceptval(-1, check=False)
    def _set_initialized(self, flag: cython.bint) -> cython.bint:
        """(internal) Set the initialized flag of the table partitioning."""
        # Self
        Element._set_initialized(self, flag)
        # Partitions
        if self._partitions is not None:
            self._partitions._set_initialized(flag)
        # Finished
        return True

    # Assure Ready ------------------------------------------------------------------------
    @cython.ccall
    @cython.exceptval(-1, check=False)
    def _assure_ready(self) -> cython.bint:
        """(internal) Assure the partitioning of the table is ready."""
        if not self._el_ready:
            self._assure_partitioning_flag_ready()
            self._assure_partitions_ready()
            self._partitions._assure_ready()
            self._assure_tb_name_ready()
            Element._assure_ready(self)
        return True

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(-1, check=False)
    def _assure_partitioning_flag_ready(self) -> cython.bint:
        """(internal) Assure the partitioning (method) flag is ready."""
        if self._partitioning_flag == utils.PARTITIONING_METHOD.NONSET:
            self._raise_definition_error(
                "is required to configure its partitioning method.\n"
                "Please call the 'by_*()' (e.g., by_range, by_hash) "
                "methods to complete the configuration."
            )
        return True

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(-1, check=False)
    def _assure_partitions_ready(self) -> cython.bint:
        """(internal) Assure partitions is ready."""
        if self._partitions is None:
            self._raise_definition_error(
                "is required to configure its partitions.\n"
                "Please call the 'by_*()' (e.g., by_range, by_hash) "
                "methods to complete the configuration."
            )
        if self._partitions._size == 0:
            self._raise_definition_error("must have at least one partition.")
        return True

    # Validate -----------------------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    def _validate_partitioning_expression(
        self,
        expressions: tuple,
        subpartitioning: cython.bint = False,
    ) -> str:
        """(internal) Validate the expressions or column names of the partitioning`<'str'>`.

        :param expressions `<'tuple'>`: The expressions or column names of the partitioning.
        :param subpartitioning `<'bool'>`: Whether the validation if for subpartitioning. Defualts to `False`.
        """
        res: list = []
        for expr in expressions:
            try:
                res.append(self._validate_expression(expr))
            except Exception as err:
                msg: str = "expression (%s %r) is invalid." % (type(expr), expr)
                if subpartitioning:
                    msg = "subpartitioning " + msg
                self._raise_definition_error(msg, err)
        if list_len(res) == 0:
            msg: str = "expression can not be empty."
            if subpartitioning:
                msg = "subpartitioning " + msg
            self._raise_definition_error(msg)
        return ",".join(res)

    @cython.cfunc
    @cython.inline(True)
    def _validate_partition(self, partition: object) -> str:
        """(internal) Validate the partition `<'str'>`.

        :param partition `<'str/Partition'>`: The partition name or instance.
        :returns `<'str'>`: The validated partition name.
        """
        self._assure_ready()
        pt: Partition = self._partitions.get(partition)
        if pt is None:
            self._raise_argument_error("does not contain partition '%s'." % str(partition))
        return pt._name

    @cython.cfunc
    @cython.inline(True)
    def _validate_partitions(self, partitions: object) -> tuple[str]:
        """(internal) Validate the partitions `<'tuple[str]'>`.

        :param partitions `<'tuple/Partitions'>`: The partitions to validate.
        :returns `<'tuple[str]'>`: The validated partition names.
        """
        self._assure_ready()
        pt: Partition
        if isinstance(partitions, (tuple, list)):
            res: list = []
            seen: set = set()
            for i in partitions:
                pt = self._partitions.get(i)
                if pt is None:
                    self._raise_argument_error("does not contain partition '%s'." % str(i))
                name: str = pt._name
                if not set_contains(seen, name):
                    res.append(name)
                    seen.add(name)
            return tuple(res)

        elif isinstance(partitions, Partitions):
            pts: Partitions = partitions
            res: list = []
            for pt in pts._el_dict.values():
                name: str = pt._name
                if not dict_contains(self._partitions._el_dict, name):
                    self._raise_argument_error("does not contain partition '%s'." % str(pt))
                res.append(name)
            return tuple(res)

        else:
            return (self._validate_partition(partitions),)

    # Copy ---------------------------------------------------------------------------------
    @cython.ccall
    def copy(self) -> Partitioning:
        """Make a copy of the table partitioning `<'Partitioning'>`."""
        # Partitioning
        pt: Partitioning = Partitioning(self._partitioning_expression)
        pt._set_partitioning_flag(self._partitioning_flag)
        pt._subpartitioning_flag = self._subpartitioning_flag
        pt._subpartitioning_expression = self._subpartitioning_expression
        # Partitions
        if self._partitions is not None:
            pt._partitions = self._partitions.copy()
        return pt

    # Special Methods ----------------------------------------------------------------------
    def __getitem__(self, col: str | Partition) -> Partition:
        self._assure_ready()
        return self._partitions[col]

    def __contains__(self, col: str | Partition) -> bool:
        self._assure_ready()
        return col in self._partitions

    def __iter__(self) -> Iterator[Partition]:
        self._assure_ready()
        return iter(self._partitions)

    def __repr__(self) -> str:
        self._assure_ready()
        # Reprs
        reprs = [
            "partitioning_method=%r" % self.partitioning_method,
            "partitioning_expression=%r" % self._partitioning_expression,
        ]
        if self._subpartitioning_flag != utils.PARTITIONING_METHOD.NONSET:
            # fmt: off
            reprs.append("subpartitioning_method=%r" % self.subpartitioning_method)
            reprs.append("subpartitioning_expression=%r" % self._subpartitioning_expression)
            # fmt: on
        reprs.append("partitions=%s" % self._partitions)

        # Compose
        return "<Partitioning (\n\t%s\n)>" % ",\n\t".join(reprs)

    def __str__(self) -> str:
        self._assure_ready()
        return str(self._partitions)

    def __len__(self) -> int:
        self._assure_ready()
        return self._partitions._size


# Partition --------------------------------------------------------------------------------------------------
@cython.cclass
class Partition(Element):
    """Represents a partition in a database table."""

    _values: object
    _comment: str
    _partitioning_flag: cython.int
    _subpartitioning_flag: cython.int
    _subpartitions: Partitions
    _is_subpartition: cython.bint

    def __init__(self, name: object, *values: object, comment: str = None):
        """The partition in a database table.

        :param name `<'str'>`: The name of the partition.
        :param values `<'str/Any'>`: The values of the partition.
        :param comment `<'str'>`: The COMMENT of the partition. Defaults to `None`.
        """
        super().__init__("PARTITION", "PARTITION")
        self._name = self._validate_partition_name(name)
        self._values = self._validate_partition_values(values)
        self._comment = self._validate_comment(comment)
        self._partitioning_flag = utils.PARTITIONING_METHOD.NONSET
        self._subpartitioning_flag = utils.PARTITIONING_METHOD.NONSET
        self._subpartitions = None
        self._is_subpartition = False

    # Property -----------------------------------------------------------------------------
    @property
    def name(self) -> str:
        """The name of the partition `<'str'>`."""
        return self._name

    @property
    def values(self) -> object | None:
        """The values of the partition `<'object/None'>`.

        ## Notice
        - Only applicable to PARTITION BY RANGE/LIST [COLUMNS].
        - Always returns `None` for PARTITION BY [LINEAR] HASH/KEY.
        """
        return self._values

    @property
    def comment(self) -> str | None:
        """The COMMENT of the partition `<'str/None'>`."""
        return self._comment

    @property
    def position(self) -> int | None:
        """The ordinal position of the partition `<'int/None'>`."""
        return None if self._el_position == -1 else self._el_position

    @property
    def partitioning_method(self) -> str | None:
        """The partitioning method `<'str/None'>`.

        e.g., `"RANGE"`, `"LIST"`, `"HASH"`, `"KEY"`, etc.
        """
        if self._partitioning_flag == utils.PARTITIONING_METHOD.NONSET:
            return None
        return self._partitioning_flag_to_method(self._partitioning_flag)

    @property
    def subpartitioning_method(self) -> str | None:
        """The subpartitioning method `<'str/None'>`.

        e.g., `"HASH"`, `"LINEAR HASH"`,`"KEY"`,`"LINEAR KEY"`, etc.
        """
        if self._subpartitioning_flag == utils.PARTITIONING_METHOD.NONSET:
            return None
        return self._partitioning_flag_to_method(self._subpartitioning_flag)

    @property
    def subpartitions(self) -> Partitions | None:
        """The subpartitions of the partition `<'Partitions/None'>`."""
        return self._subpartitions

    @property
    def is_subpartition(self) -> bool:
        """Whether the partition is a subpartition `<'bool'>`."""
        return self._is_subpartition

    # Generate SQL -------------------------------------------------------------------------
    @cython.ccall
    def _gen_definition_sql(self) -> str:
        """(internal) Generate the definition SQL of the partition `<'str'>`."""
        self._assure_ready()
        # Partitioning flag => keyword
        if utils.is_partition_by_range(self._partitioning_flag):
            keyword: str = "VALUES LESS THAN"
        elif utils.is_partition_by_list(self._partitioning_flag):
            keyword: str = "VALUES IN"
        else:
            self._raise_critical_error(
                "with '%s' partitioning method does not support "
                "partition level definition SQL generation."
                % self._partitioning_flag_to_method(self._partitioning_flag)
            )
        # Format values
        if isinstance(self._values, tuple):
            phs: str = ",".join(["%s" for _ in range(tuple_len(self._values))])
            sql: str = "PARTITION %s %s (%s)" % (self._name, keyword, phs)
            sql = self._format_sql(sql, self._values)
        elif isinstance(self._values, str):
            sql: str = "PARTITION %s %s (%s)" % (self._name, keyword, self._values)
        else:
            self._raise_critical_error(
                "values is invalid: %s %r." % (type(self._values), self._values)
            )
        # Comment
        if self._comment is not None:
            sql += self._format_sql(" COMMENT %s", self._comment)
        return sql

    # Metadata -----------------------------------------------------------------------------
    @cython.ccall
    def _sync_from_metadata(self, meta: dict, logs: Logs = None) -> Logs:
        """(internal) Synchronize local configs with the remote partition metadata `<'Logs'>`."""
        self._assure_ready()
        if logs is None:
            logs = Logs()

        # Ordinal position
        if not self._is_subpartition:
            pos: cython.int = meta["PARTITION_ORDINAL_POSITION"]
        else:
            pos: cython.int = meta["SUBPARTITION_ORDINAL_POSITION"]
        if self._el_position != pos:
            logs.log_config_int(self, "position", self._el_position, pos)
            self._el_position = pos

        # Partition values
        if not self._is_subpartition:
            values = meta["PARTITION_DESCRIPTION"]
            if values is not None:
                values = self._validate_expression(values)
            if self._values != values:
                logs.log_config_obj(self, "values", self._values, values)
                self._values = values

        # Partition comment
        comment = self._validate_comment(meta["PARTITION_COMMENT"])
        if self._comment != comment:
            logs.log_config_obj(self, "comment", self._comment, comment)
            self._comment = comment

        # Return logs
        return logs

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
        """Setup the partition.

        :param tb_name `<'str'>`: The table name of the partition.
        :param db_name `<'str'>`: The database name of the partition.
        :param charset `<'str/Charset'>`: The charset of the partition.
        :param collate `<'str/None'>`: The collation of the partition.
        :param pool `<'Pool'>`: The pool of the partition.
        """
        self._set_tb_name(tb_name)
        self._set_db_name(db_name)
        self._set_charset(charset, collate)
        self._set_pool(pool)
        if self._subpartitions is not None:
            self._subpartitions.setup(tb_name, db_name, charset, None, pool)
        return True

    @cython.ccall
    @cython.exceptval(-1, check=False)
    def _set_partitioning_flag(self, flag: cython.int) -> cython.bint:
        """(internal) Set the partitioning (method) flag.

        :param flag `<'int'>`: The partitioning (method) flag (1-8).

        ## Conversion table:
        ```python
        - 1  <=>  "HASH"
        - 2  <=>  "LINEAR HASH"
        - 3  <=>  "KEY"
        - 4  <=>  "LINEAR KEY"
        - 5  <=>  "RANGE"
        - 6  <=>  "RANGE COLUMNS"
        - 7  <=>  "LIST"
        - 8  <=>  "LIST COLUMNS"
        ```
        """
        # Validate
        # . BY RANGE/LIST [COLUMNS]
        if utils.is_partition_by_rangelist(flag):
            if self._values is None:
                self._raise_critical_error(
                    "with '%s' partitioning method must specify its partition values."
                    % self._partitioning_flag_to_method(flag)
                )
        # . BY [LINEAR] HASH/KEY
        elif utils.is_partition_by_hashkey(flag):
            if self._values is not None:
                self._raise_critical_error(
                    "with '%s' partitioning method should not have any partition values."
                    % self._partitioning_flag_to_method(flag)
                )
        # . Invalid flag
        else:
            self._raise_critical_error(
                "partitioning flag must be between 1 and 8, instead got %d." % flag
            )

        # Set flag
        self._partitioning_flag = flag
        return True

    @cython.ccall
    @cython.exceptval(-1, check=False)
    def _setup_subpartitions(
        self,
        flag: cython.int,
        subpartitions: cython.int,
    ) -> cython.bint:
        """(internal) Setup the subpartitions of the partition.

        :param flag `<'int'>`: The subpartitioning (method) flag (1-4).
        :param subpartitions `<'int'>`: The number of subpartitions of the partition.
        """
        self._assure_partitioning_flag_ready()
        # Validate
        if not utils.is_partition_by_hashkey(flag):
            self._raise_critical_error(
                "subpartitioning flag must be between 1 and 4, "
                "instead got %d." % flag
            )
        if not utils.is_partition_by_rangelist(self._partitioning_flag):
            self._raise_definition_error(
                "with '%s' partitioning method does not support subpartitioning."
                % self._partitioning_flag_to_method(self._partitioning_flag)
            )
        if subpartitions <= 0:
            self._raise_definition_error(
                "subpartition number must be greater than 0, "
                "instead got %d." % subpartitions
            )

        # Setup subpartitions
        name: str = self._name
        pts: list = []
        i: cython.int
        for i in range(subpartitions):
            pt: Partition = Partition("%ssp%d" % (name, i), None)
            pt._set_as_subpartition()
            pts.append(pt)
        self._subpartitions = Partitions(*pts)
        self._subpartitions._set_partitioning_flag(flag)
        self._subpartitioning_flag = flag
        return True

    @cython.ccall
    @cython.exceptval(-1, check=False)
    def _reset_subpartitions(self) -> cython.bint:
        """(internal) Reset (remove) all the subpartitions of the partition."""
        self._subpartitioning_flag = utils.PARTITIONING_METHOD.NONSET
        if self._subpartitions is not None:
            self._subpartitions = None
        return True

    @cython.ccall
    @cython.exceptval(-1, check=False)
    def _set_as_subpartition(self) -> cython.bint:
        """(internal) Set this partition as a subpartition."""
        if self._is_subpartition:
            return True
        self._reset_subpartitions()
        self._is_subpartition = True
        return self._set_el_type("SUBPARTITION")

    @cython.ccall
    @cython.exceptval(-1, check=False)
    def _set_initialized(self, flag: cython.bint) -> cython.bint:
        """(internal) Set the initialized flag of the partition."""
        # Self
        Element._set_initialized(self, flag)
        # Subpartitions
        if self._subpartitions is not None:
            self._subpartitions._set_initialized(flag)
        # Finished
        return True

    # Assure Ready -------------------------------------------------------------------------
    @cython.ccall
    @cython.exceptval(-1, check=False)
    def _assure_ready(self) -> cython.bint:
        """(internal) Assure the partition is ready."""
        if not self._el_ready:
            self._assure_partitioning_flag_ready()
            self._assure_position_ready()
            self._assure_name_ready()
            self._assure_tb_name_ready()
            if self._subpartitions is not None:
                self._subpartitions._assure_ready()
            Element._assure_ready(self)
        return True

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(-1, check=False)
    def _assure_partitioning_flag_ready(self) -> cython.bint:
        """(internal) Assure the partitioning (method) flag is ready."""
        if self._partitioning_flag == utils.PARTITIONING_METHOD.NONSET:
            self._raise_critical_error(
                "is required to setup its partitioning method.\n"
                "Please call the '_set_partitioning_flag()' method to complete the configuration."
            )
        return True

    # Validate -----------------------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    def _validate_partition_values(self, values: tuple) -> tuple:
        """(internal) Validate the partition values `<'tuple/None'>`."""
        # Invalid
        count: cython.Py_ssize_t = tuple_len(values)
        if count == 0:
            self._raise_definition_error("must have at least one partition value.")

        # No values
        if count == 1 and values[0] is None:
            return None

        # Validate
        res = []
        for val in values:
            # . MAXVALUE
            if isinstance(val, str):
                v: str = val
                if v == "MAXVALUE":
                    res.append(utils.MAXVALUE)
                elif v in ("-MAXVALUE", "MINVALUE"):
                    res.append(utils.MINVALUE)
                else:
                    res.append(val)
            # . None
            elif val is None:
                self._raise_argument_error("partition value cannot be 'None'.")
            # . Rest
            else:
                res.append(val)
        return tuple(res)

    # Copy ---------------------------------------------------------------------------------
    @cython.ccall
    def copy(self) -> Partition:
        """Make a copy of the partition `<'Partition'>`."""
        # Partition
        pt: Partition = Partition(self._name, None)
        pt._values = self._values
        pt._comment = self._comment
        # Partitioning
        pt._partitioning_flag = self._partitioning_flag
        pt._subpartitioning_flag = self._subpartitioning_flag
        pt._is_subpartition = self._is_subpartition
        # Subpartitions
        if self._subpartitions is not None:
            pt._subpartitions = self._subpartitions.copy()
        return pt

    # Special Methods ----------------------------------------------------------------------
    def __repr__(self) -> str:
        self._assure_ready()
        # Reprs
        reprs = [
            "name=%r" % self._name,
            "values=" + repr(self._values),
            "comment=%r" % self._comment,
            "partitioning_method=%r" % self.partitioning_method,
        ]
        if self._is_subpartition:
            reprs.append("is_subpartition=True")
        elif self._subpartitions is not None:
            # fmt: off
            reprs.append("subpartitioning_method=%r" % self._subpartitions.partitioning_method)
            reprs.append("subpartitions=%s" % self._subpartitions)
            # fmt: on
        # Compose
        return "<Partition (\n\t%s\n)>" % ",\n\t".join(reprs)

    def __str__(self) -> str:
        return self._name


# Partitions -------------------------------------------------------------------------------------------------
@cython.cclass
class Partitions(Elements):
    """Represents a collection of partitions in a table.

    Works as a dictionary where keys are the partition names
    and values the partition instances.
    """

    _partitioning_flag: cython.int
    _subpartitioning_flag: cython.int
    _subpartition_count: cython.int

    def __init__(self, *partitions: Partition):
        """The collection of partitions in a table.

        Works as a dictionary where keys are the partition names
        and values the partition instances.

        :param partitions `<'*Partition'>`: The partitions in a table.
        """
        super().__init__("PARTITION", "PARTITIONS", Partition, *partitions)
        self._partitioning_flag = utils.PARTITIONING_METHOD.NONSET
        self._subpartitioning_flag = utils.PARTITIONING_METHOD.NONSET
        self._subpartition_count = 0

    # Property -----------------------------------------------------------------------------
    @property
    def partitioning_method(self) -> str | None:
        """The partitioning method of the partition collection `<'str/None'>`."""
        if self._partitioning_flag == utils.PARTITIONING_METHOD.NONSET:
            return None
        return self._partitioning_flag_to_method(self._partitioning_flag)

    @property
    def subpartitioning_method(self) -> str | None:
        """The subpartitioning method of the partition collection `<'str/None'>`."""
        if self._subpartitioning_flag == utils.PARTITIONING_METHOD.NONSET:
            return None
        return self._partitioning_flag_to_method(self._subpartitioning_flag)

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
        if utils.is_partition_by_hashkey(self._partitioning_flag):
            self._raise_critical_error(
                "with '%s' partitioning method does not support "
                "partitions level definition SQL generation."
                % self._partitioning_flag_to_method(self._partitioning_flag)
            )
        pt: Partition
        sqls = [pt._gen_definition_sql() for pt in self._sorted_elements()]
        # . without indent
        nxt: str = ",\n"
        if indent == 0:
            return nxt.join(sqls)
        # . with indent
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
        """Setup the partition collection.

        :param tb_name `<'str'>`: The table name of the partition collection.
        :param db_name `<'str'>`: The database name of the partition collection.
        :param charset `<'str/Charset'>`: The charset of the partition collection.
        :param collate `<'str/None'>`: The collation of the partition collection.
        :param pool `<'Pool'>`: The pool of the partition collection.
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
        el: Partition
        for el in self._el_dict.values():
            if not el._el_ready:
                el.setup(tb_name, db_name, charset, None, pool)
        return self._assure_ready()

    @cython.ccall
    @cython.exceptval(-1, check=False)
    def _set_partitioning_flag(self, flag: cython.int) -> cython.bint:
        """(internal) Set the partitioning (method) flag.

        :param flag `<'int'>`: The partitioning flag (1-8).

        ## Conversion table:
        ```python
        - 1  <=>  "HASH"
        - 2  <=>  "LINEAR HASH"
        - 3  <=>  "KEY"
        - 4  <=>  "LINEAR KEY"
        - 5  <=>  "RANGE"
        - 6  <=>  "RANGE COLUMNS"
        - 7  <=>  "LIST"
        - 8  <=>  "LIST COLUMNS"
        ```
        """
        pt: Partition
        for pt in self._el_dict.values():
            pt._set_partitioning_flag(flag)
        self._partitioning_flag = flag
        return True

    @cython.ccall
    @cython.exceptval(-1, check=False)
    def _setup_subpartitions(
        self,
        flag: cython.int,
        subpartitions: cython.int,
    ) -> cython.bint:
        """(internal) Setup the subpartitions for all the partitions.

        :param flag `<'int'>`: The subpartitioning (method) flag (1-4).
        :param subpartitions `<'int'>`: The number of subpartitions each of the partitions.
        """
        self._assure_partitioning_flag_ready()
        pt: Partition
        for pt in self._el_dict.values():
            pt._setup_subpartitions(flag, subpartitions)
        self._subpartitioning_flag = flag
        self._subpartition_count = subpartitions
        return True

    @cython.ccall
    @cython.exceptval(-1, check=False)
    def _reset_subpartitions(self) -> cython.bint:
        """(internal) Reset (remove) all the subpartitions of the partitions."""
        pt: Partition
        for pt in self._el_dict.values():
            pt._reset_subpartitions()
        self._subpartitioning_flag = utils.PARTITIONING_METHOD.NONSET
        self._subpartition_count = 0
        return True

    # Assure Ready -------------------------------------------------------------------------
    @cython.ccall
    @cython.exceptval(-1, check=False)
    def _assure_ready(self) -> cython.bint:
        """(internal) Assure the partition collection is ready."""
        if not self._el_ready:
            self._assure_partitioning_flag_ready()
            self._assure_tb_name_ready()
            Elements._assure_ready(self)
        return True

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(-1, check=False)
    def _assure_partitioning_flag_ready(self) -> cython.bint:
        """(internal) Assure the partitioning (method) flag is ready."""
        if self._partitioning_flag == utils.PARTITIONING_METHOD.NONSET:
            self._raise_critical_error(
                "is required to setup its partitioning method.\n"
                "Please call the '_set_partitioning_flag()' method to complete the configuration."
            )
        return True

    # Copy ---------------------------------------------------------------------------------
    @cython.ccall
    def copy(self) -> Partitions:
        """Make a copy of the partition collection `<'Partitions'>`."""
        # Partitions
        el: Partition
        pts = Partitions(*[el.copy() for el in self._el_dict.values()])
        # Partitioning
        pts._partitioning_flag = self._partitioning_flag
        pts._subpartitioning_flag = self._subpartitioning_flag
        pts._subpartition_count = self._subpartition_count
        return pts


# Metadata ---------------------------------------------------------------------------------------------------
@cython.cclass
class PartitioningMetadata(Metadata):
    """Represents the metadata from the remote server of the table partitioning."""

    # Base data
    _db_name: str
    _tb_name: str
    _partitioning_method: str
    _partitioning_expression: str
    _subpartitioning_method: str
    _subpartitioning_expression: str
    _partitions: list[dict]
    # Additional data
    _partition_names: tuple[str]
    _partition_count: cython.int
    _subpartition_names: tuple[str]
    _subpartition_count: cython.int

    def __init__(self, meta: tuple[dict]):
        """The metadata from the remote server of the table partitioning.

        :param meta `<'tuple[dict]'>`: A tuple of dicts containing the following partitioning metadata:
        ```python
        - "CATALOG_NAME"
        - "SCHEMA_NAME"
        - "TABLE_NAME"
        - "PARTITION_NAME"
        - "PARTITION_ORDINAL_POSITION"
        - "PARTITION_METHOD"
        - "PARTITION_EXPRESSION"
        - "SUBPARTITION_NAME"
        - "SUBPARTITION_ORDINAL_POSITION"
        - "SUBPARTITION_METHOD"
        - "SUBPARTITION_EXPRESSION"
        - "PARTITION_DESCRIPTION"
        - "TABLE_ROWS"
        - "AVG_ROW_LENGTH"
        - "DATA_LENGTH"
        - "MAX_DATA_LENGTH"
        - "INDEX_LENGTH"
        - "DATA_FREE"
        - "CREATE_TIME"
        - "UPDATE_TIME"
        - "CHECK_TIME"
        - "CHECKSUM"
        - "PARTITION_COMMENT"
        - "NODEGROUP"
        - "TABLESPACE_NAME"
        ```
        """
        # Re-construct
        self._el_cate = "PARTITION"
        _meta: dict = None
        _pts: list = []
        row: dict
        try:
            for row in meta:
                if _meta is None:
                    _meta = {
                        "CATALOG_NAME": row["CATALOG_NAME"],
                        "SCHEMA_NAME": row["SCHEMA_NAME"],
                        "TABLE_NAME": row["TABLE_NAME"],
                        "PARTITION_METHOD": row["PARTITION_METHOD"],
                        "PARTITION_EXPRESSION": row["PARTITION_EXPRESSION"],
                        "SUBPARTITION_METHOD": row["SUBPARTITION_METHOD"],
                        "SUBPARTITION_EXPRESSION": row["SUBPARTITION_EXPRESSION"],
                    }
                _pts.append(
                    # fmt: off
                    {
                        "PARTITION_NAME": row["PARTITION_NAME"],
                        "PARTITION_ORDINAL_POSITION": row["PARTITION_ORDINAL_POSITION"],
                        "SUBPARTITION_NAME": row["SUBPARTITION_NAME"],
                        "SUBPARTITION_ORDINAL_POSITION": row["SUBPARTITION_ORDINAL_POSITION"],
                        "PARTITION_DESCRIPTION": row["PARTITION_DESCRIPTION"],
                        "PARTITION_COMMENT": row["PARTITION_COMMENT"],
                        "TABLE_ROWS": row["TABLE_ROWS"],
                        "AVG_ROW_LENGTH": row["AVG_ROW_LENGTH"],
                        "DATA_LENGTH": row["DATA_LENGTH"],
                        "MAX_DATA_LENGTH": row["MAX_DATA_LENGTH"],
                        "INDEX_LENGTH": row["INDEX_LENGTH"],
                        "DATA_FREE": row["DATA_FREE"],
                        "CREATE_TIME": row["CREATE_TIME"],
                        "UPDATE_TIME": row["UPDATE_TIME"],
                        "CHECK_TIME": row["CHECK_TIME"],
                        "CHECKSUM": row["CHECKSUM"],
                        "NODEGROUP": row["NODEGROUP"],
                        "TABLESPACE_NAME": row["TABLESPACE_NAME"],
                    }
                    # fmt: on
                )
                if _meta is None:
                    raise ValueError("partitioning metadata is empty.")
        except Exception as err:
            self._raise_invalid_metadata_error(meta, err)
        dict_setitem(_meta, "PARTITIONS", _pts)

        # Initialize
        super().__init__("PARTITION", _meta, 8)
        try:
            # Base data
            self._db_name = self._meta["SCHEMA_NAME"]
            self._tb_name = self._meta["TABLE_NAME"]
            self._partitioning_method = self._meta["PARTITION_METHOD"]
            self._partitioning_expression = utils.validate_expression(
                self._meta["PARTITION_EXPRESSION"]
            )
            self._subpartitioning_method = self._meta["SUBPARTITION_METHOD"]
            self._subpartitioning_expression = utils.validate_expression(
                self._meta["SUBPARTITION_EXPRESSION"]
            )
            self._partitions = self._meta["PARTITIONS"]
            # Additional data
            has_subpts: cython.bint = self._subpartitioning_method is not None
            pts_seen: set = set()
            pts_names: list = []
            subpts_names: list = []
            for row in self._partitions:
                pt_name = row["PARTITION_NAME"]
                if not set_contains(pts_seen, pt_name):
                    pts_names.append(pt_name)
                    pts_seen.add(pt_name)
                if has_subpts:
                    subpts_names.append(row["SUBPARTITION_NAME"])
            self._partition_names = tuple(pts_names)
            self._partition_count = list_len(pts_names)
            if has_subpts:
                self._subpartition_names = tuple(subpts_names)
                with cython.cdivision(True):
                    count: cython.int = int(
                        list_len(subpts_names) / self._partition_count
                    )
                self._subpartition_count = count
            else:
                self._subpartition_names = ()
                self._subpartition_count = 0
        except Exception as err:
            self._raise_invalid_metadata_error(self._meta, err)

    # Property -----------------------------------------------------------------------------
    @property
    def catelog_name(self) -> str:
        """The catalog name of the partitioning `<'str'>`."""
        return self._meta["CATALOG_NAME"]

    @property
    def db_name(self) -> str:
        """The schema name of the partitioning `<'str'>`."""
        return self._db_name

    @property
    def tb_name(self) -> str:
        """The table name of the partitioning `<'str'>`."""
        return self._tb_name

    @property
    def partitioning_method(self) -> str:
        """The partitioning method `<'str'>`."""
        return self._partitioning_method

    @property
    def partitioning_expression(self) -> str:
        """The partitioning expressions or column names `<'str'>`."""
        return self._partitioning_expression

    @property
    def subpartitioning_method(self) -> str | None:
        """The subpartitioning method `<'str/None'>`."""
        return self._subpartitioning_method

    @property
    def subpartitioning_expression(self) -> str | None:
        """The subpartitioning expressions or column names `<'str/None'>`."""
        return self._subpartitioning_expression

    @property
    def partitions(self) -> list[dict]:
        """The partitions `<'list[dict]'>`."""
        return self._partitions

    @property
    def partition_names(self) -> tuple[str]:
        """The name of the main partitions `<'tuple[str]'>`."""
        return self._partition_names

    @property
    def partition_count(self) -> int:
        """Total number of the main partitions `<'int'>`."""
        return self._partition_count

    @property
    def subpartition_names(self) -> tuple[str]:
        """The name of the subpartitions `<'tuple[str]'>`."""
        return self._subpartition_names

    @property
    def subpartition_count(self) -> int:
        """Total number of the subpartitions for each of the main partitions `<'int'>`."""
        return self._subpartition_count
