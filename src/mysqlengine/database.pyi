from typing_extensions import Self
from typing import Iterator, Literal
from sqlcycli.charset import Charset
from sqlcycli.sqlfunc import SQLFunction
from sqlcycli.aio.pool import Pool, PoolConnection, PoolSyncConnection
from mysqlengine.table import Table, TempTable, TempTableManager, Tables
from mysqlengine.column import Column
from mysqlengine.partition import Partition
from mysqlengine.element import Element, Metadata, Logs
from mysqlengine.dml import (
    SelectDML,
    InsertDML,
    ReplaceDML,
    UpdateDML,
    DeleteDML,
    WithDML,
)

# Database ---------------------------------------------------------------------------------------------------
class Database(Element):
    def __init__(
        self,
        name: str,
        pool: Pool,
        charset: str | Charset = "utf8mb4",
        collate: str | None = None,
        encryption: bool | None = None,
    ): ...
    # Property
    @property
    def db_name(self) -> str: ...
    @property
    def encryption(self) -> bool | None: ...
    @property
    def read_only(self) -> bool: ...
    @property
    def tables(self) -> Tables: ...
    # DML
    def Select(
        self,
        *expressions: str | SQLFunction | Column,
        distinct: bool = False,
        high_priority: bool = False,
        straight_join: bool = False,
        sql_buffer_result: bool = False,
    ) -> SelectDML: ...
    def Insert(
        self,
        table: str | Table,
        partition: str | Partition | list | tuple = None,
        ignore: bool = False,
        priority: Literal["LOW_PRIORITY", "HIGH_PRIORITY"] | None = None,
    ) -> InsertDML: ...
    def Replace(
        self,
        table: str | Table,
        partition: str | Partition | list | tuple = None,
        low_priority: bool = False,
    ) -> ReplaceDML: ...
    def Update(
        self,
        table: str | Table,
        partition: str | Partition | list | tuple = None,
        ignore: bool = False,
        low_priority: bool = False,
        alias: str | None = None,
    ) -> UpdateDML: ...
    def Delete(
        self,
        table: str | Table,
        partition: str | Partition | list | tuple = None,
        ignore: bool = False,
        low_priority: bool = False,
        quick: bool = False,
        alias: str | None = None,
        multi_tables: str | list | tuple | None = None,
    ) -> DeleteDML: ...
    def With(
        self,
        name: str,
        subquery: str | SelectDML,
        *columns: str | Column,
        recursive: bool = False,
    ) -> WithDML: ...
    def CreateTempTable(
        self,
        conn: PoolConnection | PoolSyncConnection,
        name: str,
        temp_table: TempTable,
    ) -> TempTableManager: ...
    # Sync
    def Initialize(self, force: bool = False) -> Logs: ...
    def Create(self, if_not_exists: bool = False) -> Logs: ...
    def Exists(self) -> bool: ...
    def Drop(self, if_exists: bool = False) -> Logs: ...
    def Alter(
        self,
        charset: str | Charset | None = None,
        collate: str | None = None,
        encryption: bool | None = None,
        read_only: bool | None = None,
    ) -> Logs: ...
    def ShowMetadata(self) -> DatabaseMetadata: ...
    def Lock(
        self,
        conn: PoolSyncConnection,
        *tables: str | Table,
        lock_for_read: bool = True,
    ) -> PoolSyncConnection: ...
    def SyncFromRemote(self, thorough: bool = False) -> Logs: ...
    def SyncToRemote(self) -> Logs: ...
    # Async
    async def aioInitialize(self, force: bool = False) -> Logs: ...
    async def aioCreate(self, if_not_exists: bool = False) -> Logs: ...
    async def aioExists(self) -> bool: ...
    async def aioDrop(self, if_exists: bool = False) -> Logs: ...
    async def aioAlter(
        self,
        charset: str | Charset | None = None,
        collate: str | None = None,
        encryption: bool | None = None,
        read_only: bool | None = None,
    ) -> Logs: ...
    async def aioShowMetadata(self) -> DatabaseMetadata: ...
    async def aioLock(
        self,
        conn: PoolConnection,
        *tables: str | Table,
        lock_for_read: bool = True,
    ) -> PoolConnection: ...
    async def aioSyncFromRemote(self, thorough: bool = False) -> Logs: ...
    async def aioSyncToRemote(self) -> Logs: ...
    # Setter
    def setup(self) -> bool: ...
    def set_name(self, name: str): ...
    # Copy
    def copy(self) -> Self: ...
    # Special Methods
    def __getitem__(self, tb: str | Table) -> Table: ...
    def __contains__(self, tb: str | Table) -> bool: ...
    def __iter__(self) -> Iterator[Table]: ...

# Metadata ---------------------------------------------------------------------------------------------------
class DatabaseMetadata(Metadata):
    def __init__(self, meta: dict): ...
    # Property
    @property
    def catelog_name(self) -> str: ...
    @property
    def db_name(self) -> str: ...
    @property
    def charset(self) -> Charset: ...
    @property
    def encryption(self) -> bool: ...
    @property
    def read_only(self) -> bool: ...
    @property
    def options(self) -> str: ...
