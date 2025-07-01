from typing import Any, Literal
from typing_extensions import Self
from pandas import DataFrame
from sqlcycli.sqlfunc import SQLFunction
from sqlcycli.aio.pool import Pool, PoolSyncConnection, PoolConnection
from sqlcycli.connection import Cursor, DictCursor, DfCursor
from sqlcycli.aio.connection import (
    Cursor as AioCursor,
    DictCursor as AioDictCursor,
    DfCursor as AioDfCursor,
)
from mysqlengine.table import Table
from mysqlengine.index import Index
from mysqlengine.column import Column
from mysqlengine.partition import Partition

# DML --------------------------------------------------------------------------------------------------------
class DML:
    def __init__(self, dml: str, db_name: str, pool: Pool): ...
    # Property
    @property
    def db_name(self) -> str: ...
    @property
    def pool(self) -> Pool: ...
    # Connection
    def _set_connection(
        self,
        sync_conn: PoolSyncConnection = None,
        async_conn: PoolConnection = None,
    ) -> bool: ...
    # Statement
    def statement(self, indent: int = 0) -> str: ...

# Select - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
class SelectDML(DML):
    def __init__(self, db_name: str, pool: Pool): ...
    # Clause
    def Select(
        self,
        *expressions: str | SQLFunction | Column,
        distinct: bool = False,
        high_priority: bool = False,
        straight_join: bool = False,
        sql_buffer_result: bool = False,
    ) -> Self: ...
    def _Select(
        self,
        expressions: tuple[str | SQLFunction | Column],
        distinct: bool = False,
        high_priority: bool = False,
        straight_join: bool = False,
        sql_buffer_result: bool = False,
    ) -> Self: ...
    def From(
        self,
        table: str | Table | SelectDML,
        partition: str | Partition | list | tuple | None = None,
        alias: str | None = None,
    ) -> Self: ...
    def Join(
        self,
        table: str | Table | SelectDML,
        *on: str,
        using: str | Column | list | tuple | None = None,
        partition: str | Partition | list | tuple | None = None,
        alias: str | None = None,
    ) -> Self: ...
    def LeftJoin(
        self,
        table: str | Table | SelectDML,
        *on: str,
        using: str | Column | list | tuple | None = None,
        partition: str | Partition | list | tuple | None = None,
        alias: str | None = None,
    ) -> Self: ...
    def RightJoin(
        self,
        table: str | Table | SelectDML,
        *on: str,
        using: str | Column | list | tuple | None = None,
        partition: str | Partition | list | tuple | None = None,
        alias: str | None = None,
    ) -> Self: ...
    def StraightJoin(
        self,
        table: str | Table | SelectDML,
        *on: str,
        using: str | Column | list | tuple | None = None,
        partition: str | Partition | list | tuple | None = None,
        alias: str | None = None,
    ) -> Self: ...
    def CrossJoin(
        self,
        table: str | Table | SelectDML,
        *on: str,
        using: str | Column | list | tuple | None = None,
        partition: str | Partition | list | tuple | None = None,
        alias: str | None = None,
    ) -> Self: ...
    def NaturalJoin(
        self,
        table: str | Table | SelectDML,
        join_method: Literal["INNER", "LEFT", "RIGHT"] = "INNER",
        partition: str | Partition | list | tuple | None = None,
        alias: str | None = None,
    ) -> Self: ...
    def UseIndex(
        self,
        *indexes: str | Index,
        scope: Literal["JOIN", "ORDER BY", "GROUP BY"] | None = None,
    ) -> Self: ...
    def IgnoreIndex(
        self,
        *indexes: str | Index,
        scope: Literal["JOIN", "ORDER BY", "GROUP BY"] | None = None,
    ) -> Self: ...
    def ForceIndex(
        self,
        *indexes: str | Index,
        scope: Literal["JOIN", "ORDER BY", "GROUP BY"] | None = None,
    ) -> Self: ...
    def Where(
        self,
        *conds: str,
        in_conds: dict | None = None,
        not_in_conds: dict | None = None,
    ) -> Self: ...
    def GroupBy(self, *columns: str | Column, with_rollup: bool = False) -> Self: ...
    def Having(
        self,
        *conds: str,
        in_conds: dict | None = None,
        not_in_conds: dict | None = None,
    ) -> Self: ...
    def Window(
        self,
        name: str,
        partition_by: str | Column | list | tuple | None = None,
        order_by: str | Column | list | tuple | None = None,
        frame_clause: str | None = None,
    ) -> Self: ...
    def Union(self, subquery: SelectDML, all: bool = False) -> Self: ...
    def Intersect(self, subquery: SelectDML, all: bool = False) -> Self: ...
    def Except(self, subquery: SelectDML, all: bool = False) -> Self: ...
    def OrderBy(self, *columns: str | Column, with_rollup: bool = False) -> Self: ...
    def Limit(self, row_count: int, offset: int | None = None) -> Self: ...
    def ForUpdate(
        self,
        *tables: str | Table,
        option: Literal["NOWAIT", "SKIP LOCKED"] | None = None,
    ) -> Self: ...
    def ForShare(
        self,
        *tables: str | Table,
        option: Literal["NOWAIT", "SKIP LOCKED"] | None = None,
    ) -> Self: ...
    def Into(self, *variables: str) -> Self: ...
    # Statement
    def statement(self, indent: int = 0) -> str: ...
    # Execute
    def Execute(
        self,
        args: list | tuple | DataFrame | Any | None = None,
        cursor: (
            type[Cursor | DictCursor | DfCursor | tuple | dict | DataFrame] | None
        ) = None,
        fetch: bool = True,
        conn: PoolSyncConnection | None = None,
    ) -> tuple[tuple] | tuple[dict] | DataFrame | int: ...
    async def aioExecute(
        self,
        args: list | tuple | DataFrame | Any | None = None,
        cursor: (
            type[AioCursor | AioDictCursor | AioDfCursor | tuple | dict | DataFrame]
            | None
        ) = None,
        fetch: bool = True,
        conn: PoolConnection | None = None,
    ) -> tuple[tuple] | tuple[dict] | DataFrame | int: ...

# Insert - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
class InsertDML(DML):
    def __init__(self, db_name: str, pool: Pool): ...
    # Clause
    def Insert(
        self,
        table: str | Table,
        partition: str | Partition | list | tuple | None = None,
        ignore: bool = False,
        priority: Literal["LOW_PRIORITY", "HIGH_PRIORITY"] | None = None,
    ) -> Self: ...
    def Columns(self, *columns: str | Column) -> Self: ...
    def Values(self, placeholders: int) -> Self: ...
    def Set(self, *assignments: str) -> Self: ...
    def RowAlias(self, row_alias: str, *col_alias: str) -> Self: ...
    def With(
        self,
        name: str,
        subquery: str | SelectDML,
        *columns: str | Column,
        recursive: bool = False,
    ) -> Self: ...
    def Select(
        self,
        *expressions: str | SQLFunction | Column,
        distinct: bool = False,
        high_priority: bool = False,
        straight_join: bool = False,
        sql_buffer_result: bool = False,
    ) -> Self: ...
    def From(
        self,
        table: str | Table | SelectDML,
        partition: str | Partition | list | tuple | None = None,
        alias: str | None = None,
    ) -> Self: ...
    def Join(
        self,
        table: str | Table | SelectDML,
        *on: str,
        using: str | Column | list | tuple | None = None,
        partition: str | Partition | list | tuple | None = None,
        alias: str | None = None,
    ) -> Self: ...
    def LeftJoin(
        self,
        table: str | Table | SelectDML,
        *on: str,
        using: str | Column | list | tuple | None = None,
        partition: str | Partition | list | tuple | None = None,
        alias: str | None = None,
    ) -> Self: ...
    def RightJoin(
        self,
        table: str | Table | SelectDML,
        *on: str,
        using: str | Column | list | tuple | None = None,
        partition: str | Partition | list | tuple | None = None,
        alias: str | None = None,
    ) -> Self: ...
    def StraightJoin(
        self,
        table: str | Table | SelectDML,
        *on: str,
        using: str | Column | list | tuple | None = None,
        partition: str | Partition | list | tuple | None = None,
        alias: str | None = None,
    ) -> Self: ...
    def CrossJoin(
        self,
        table: str | Table | SelectDML,
        *on: str,
        using: str | Column | list | tuple | None = None,
        partition: str | Partition | list | tuple | None = None,
        alias: str | None = None,
    ) -> Self: ...
    def NaturalJoin(
        self,
        table: str | Table | SelectDML,
        join_method: Literal["INNER", "LEFT", "RIGHT"] = "INNER",
        partition: str | Partition | list | tuple | None = None,
        alias: str | None = None,
    ) -> Self: ...
    def UseIndex(
        self,
        *indexes: str | Index,
        scope: Literal["JOIN", "ORDER BY", "GROUP BY"] | None = None,
    ) -> Self: ...
    def IgnoreIndex(
        self,
        *indexes: str | Index,
        scope: Literal["JOIN", "ORDER BY", "GROUP BY"] | None = None,
    ) -> Self: ...
    def ForceIndex(
        self,
        *indexes: str | Index,
        scope: Literal["JOIN", "ORDER BY", "GROUP BY"] | None = None,
    ) -> Self: ...
    def Where(
        self,
        *conds: str,
        in_conds: dict | None = None,
        not_in_conds: dict | None = None,
    ) -> Self: ...
    def GroupBy(self, *columns: str | Column, with_rollup: bool = False) -> Self: ...
    def Having(
        self,
        *conds: str,
        in_conds: dict | None = None,
        not_in_conds: dict | None = None,
    ) -> Self: ...
    def Window(
        self,
        name: str,
        partition_by: str | Column | list | tuple | None = None,
        order_by: str | Column | list | tuple | None = None,
        frame_clause: str | None = None,
    ) -> Self: ...
    def Union(self, subquery: SelectDML, all: bool = False) -> Self: ...
    def Intersect(self, subquery: SelectDML, all: bool = False) -> Self: ...
    def Except(self, subquery: SelectDML, all: bool = False) -> Self: ...
    def OrderBy(self, *columns: str | Column, with_rollup: bool = False) -> Self: ...
    def Limit(self, row_count: int, offset: int | None = None) -> Self: ...
    def OnDuplicate(self, *assignments: str) -> Self: ...
    # Statement
    def statement(self, indent: int = 0) -> str: ...
    # Execute
    def Execute(
        self,
        args: list | tuple | DataFrame | Any = None,
        many: bool = False,
        conn: PoolSyncConnection | None = None,
    ) -> int: ...
    async def aioExecute(
        self,
        args: list | tuple | DataFrame | Any = None,
        many: bool = False,
        conn: PoolConnection | None = None,
    ) -> int: ...

# Replace - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
class ReplaceDML(DML):
    def __init__(self, db_name: str, pool: Pool): ...
    # Clause
    def Replace(
        self,
        table: str | Table,
        partition: str | Partition | list | tuple | None = None,
        low_priority: bool = False,
    ) -> Self: ...
    def Columns(self, *columns: str | Column) -> Self: ...
    def Values(self, placeholders: int) -> Self: ...
    def Set(self, *assignments: str) -> Self: ...
    def With(
        self,
        name: str,
        subquery: str | SelectDML,
        *columns: str | Column,
        recursive: bool = False,
    ) -> Self: ...
    def Select(
        self,
        *expressions: str | SQLFunction | Column,
        distinct: bool = False,
        high_priority: bool = False,
        straight_join: bool = False,
        sql_buffer_result: bool = False,
    ) -> Self: ...
    def From(
        self,
        table: str | Table | SelectDML,
        partition: str | Partition | list | tuple | None = None,
        alias: str | None = None,
    ) -> Self: ...
    def Join(
        self,
        table: str | Table | SelectDML,
        *on: str,
        using: str | Column | list | tuple | None = None,
        partition: str | Partition | list | tuple | None = None,
        alias: str | None = None,
    ) -> Self: ...
    def LeftJoin(
        self,
        table: str | Table | SelectDML,
        *on: str,
        using: str | Column | list | tuple | None = None,
        partition: str | Partition | list | tuple | None = None,
        alias: str | None = None,
    ) -> Self: ...
    def RightJoin(
        self,
        table: str | Table | SelectDML,
        *on: str,
        using: str | Column | list | tuple | None = None,
        partition: str | Partition | list | tuple | None = None,
        alias: str | None = None,
    ) -> Self: ...
    def StraightJoin(
        self,
        table: str | Table | SelectDML,
        *on: str,
        using: str | Column | list | tuple | None = None,
        partition: str | Partition | list | tuple | None = None,
        alias: str | None = None,
    ) -> Self: ...
    def CrossJoin(
        self,
        table: str | Table | SelectDML,
        *on: str,
        using: str | Column | list | tuple | None = None,
        partition: str | Partition | list | tuple | None = None,
        alias: str | None = None,
    ) -> Self: ...
    def NaturalJoin(
        self,
        table: str | Table | SelectDML,
        join_method: Literal["INNER", "LEFT", "RIGHT"] = "INNER",
        partition: str | Partition | list | tuple | None = None,
        alias: str | None = None,
    ) -> Self: ...
    def UseIndex(
        self,
        *indexes: str | Index,
        scope: Literal["JOIN", "ORDER BY", "GROUP BY"] | None = None,
    ) -> Self: ...
    def IgnoreIndex(
        self,
        *indexes: str | Index,
        scope: Literal["JOIN", "ORDER BY", "GROUP BY"] | None = None,
    ) -> Self: ...
    def ForceIndex(
        self,
        *indexes: str | Index,
        scope: Literal["JOIN", "ORDER BY", "GROUP BY"] | None = None,
    ) -> Self: ...
    def Where(
        self,
        *conds: str,
        in_conds: dict | None = None,
        not_in_conds: dict | None = None,
    ) -> Self: ...
    def GroupBy(self, *columns: str | Column, with_rollup: bool = False) -> Self: ...
    def Having(
        self,
        *conds: str,
        in_conds: dict | None = None,
        not_in_conds: dict | None = None,
    ) -> Self: ...
    def Window(
        self,
        name: str,
        partition_by: str | Column | list | tuple | None = None,
        order_by: str | Column | list | tuple | None = None,
        frame_clause: str | None = None,
    ) -> Self: ...
    def Union(self, subquery: SelectDML, all: bool = False) -> Self: ...
    def Intersect(self, subquery: SelectDML, all: bool = False) -> Self: ...
    def Except(self, subquery: SelectDML, all: bool = False) -> Self: ...
    def OrderBy(self, *columns: str | Column, with_rollup: bool = False) -> Self: ...
    def Limit(self, row_count: int, offset: int | None = None) -> Self: ...
    # Statement
    def statement(self, indent: int = 0) -> str: ...
    # Execute
    def Execute(
        self,
        args: list | tuple | DataFrame | Any = None,
        many: bool = False,
        conn: PoolSyncConnection | None = None,
    ) -> int: ...
    async def aioExecute(
        self,
        args: list | tuple | DataFrame | Any = None,
        many: bool = False,
        conn: PoolConnection | None = None,
    ) -> int: ...

# Update - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
class UpdateDML(DML):
    def __init__(self, db_name: str, pool: Pool): ...
    # Clause
    def Update(
        self,
        table: str | Table,
        partition: str | Partition | list | tuple | None = None,
        ignore: bool = False,
        low_priority: bool = False,
        alias: str | None = None,
    ) -> Self: ...
    def Set(self, *assignments: str) -> Self: ...
    def Join(
        self,
        table: str | Table | SelectDML,
        *on: str,
        using: str | Column | list | tuple | None = None,
        partition: str | Partition | list | tuple | None = None,
        alias: str | None = None,
    ) -> Self: ...
    def LeftJoin(
        self,
        table: str | Table | SelectDML,
        *on: str,
        using: str | Column | list | tuple | None = None,
        partition: str | Partition | list | tuple | None = None,
        alias: str | None = None,
    ) -> Self: ...
    def RightJoin(
        self,
        table: str | Table | SelectDML,
        *on: str,
        using: str | Column | list | tuple | None = None,
        partition: str | Partition | list | tuple | None = None,
        alias: str | None = None,
    ) -> Self: ...
    def StraightJoin(
        self,
        table: str | Table | SelectDML,
        *on: str,
        using: str | Column | list | tuple | None = None,
        partition: str | Partition | list | tuple | None = None,
        alias: str | None = None,
    ) -> Self: ...
    def CrossJoin(
        self,
        table: str | Table | SelectDML,
        *on: str,
        using: str | Column | list | tuple | None = None,
        partition: str | Partition | list | tuple | None = None,
        alias: str | None = None,
    ) -> Self: ...
    def NaturalJoin(
        self,
        table: str | Table | SelectDML,
        join_method: Literal["INNER", "LEFT", "RIGHT"] = "INNER",
        partition: str | Partition | list | tuple | None = None,
        alias: str | None = None,
    ) -> Self: ...
    def UseIndex(
        self,
        *indexes: str | Index,
        scope: Literal["JOIN", "ORDER BY", "GROUP BY"] | None = None,
    ) -> Self: ...
    def IgnoreIndex(
        self,
        *indexes: str | Index,
        scope: Literal["JOIN", "ORDER BY", "GROUP BY"] | None = None,
    ) -> Self: ...
    def ForceIndex(
        self,
        *indexes: str | Index,
        scope: Literal["JOIN", "ORDER BY", "GROUP BY"] | None = None,
    ) -> Self: ...
    def Where(
        self,
        *conds: str,
        in_conds: dict | None = None,
        not_in_conds: dict | None = None,
    ) -> Self: ...
    def OrderBy(self, *columns: str | Column) -> Self: ...
    def Limit(self, row_count: int) -> Self: ...
    # Statement
    def statement(self, indent: int = 0) -> str: ...
    # Execute
    def Execute(
        self,
        args: list | tuple | DataFrame | Any = None,
        many: bool = False,
        conn: PoolSyncConnection | None = None,
    ) -> int: ...
    async def aioExecute(
        self,
        args: list | tuple | DataFrame | Any = None,
        many: bool = False,
        conn: PoolConnection | None = None,
    ) -> int: ...

# Delete - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
class DeleteDML(DML):
    def __init__(self, db_name: str, pool: Pool): ...
    # Clause
    def Delete(
        self,
        table: str | Table,
        partition: str | Partition | list | tuple | None = None,
        ignore: bool = False,
        low_priority: bool = False,
        quick: bool = False,
        alias: str | None = None,
        multi_tables: str | list | tuple | None = None,
    ) -> Self: ...
    def Join(
        self,
        table: str | Table | SelectDML,
        *on: str,
        using: str | Column | list | tuple | None = None,
        partition: str | Partition | list | tuple | None = None,
        alias: str | None = None,
    ) -> Self: ...
    def LeftJoin(
        self,
        table: str | Table | SelectDML,
        *on: str,
        using: str | Column | list | tuple | None = None,
        partition: str | Partition | list | tuple | None = None,
        alias: str | None = None,
    ) -> Self: ...
    def RightJoin(
        self,
        table: str | Table | SelectDML,
        *on: str,
        using: str | Column | list | tuple | None = None,
        partition: str | Partition | list | tuple | None = None,
        alias: str | None = None,
    ) -> Self: ...
    def StraightJoin(
        self,
        table: str | Table | SelectDML,
        *on: str,
        using: str | Column | list | tuple | None = None,
        partition: str | Partition | list | tuple | None = None,
        alias: str | None = None,
    ) -> Self: ...
    def CrossJoin(
        self,
        table: str | Table | SelectDML,
        *on: str,
        using: str | Column | list | tuple | None = None,
        partition: str | Partition | list | tuple | None = None,
        alias: str | None = None,
    ) -> Self: ...
    def NaturalJoin(
        self,
        table: str | Table | SelectDML,
        join_method: Literal["INNER", "LEFT", "RIGHT"] = "INNER",
        partition: str | Partition | list | tuple | None = None,
        alias: str | None = None,
    ) -> Self: ...
    def UseIndex(
        self,
        *indexes: str | Index,
        scope: Literal["JOIN", "ORDER BY", "GROUP BY"] | None = None,
    ) -> Self: ...
    def IgnoreIndex(
        self,
        *indexes: str | Index,
        scope: Literal["JOIN", "ORDER BY", "GROUP BY"] | None = None,
    ) -> Self: ...
    def ForceIndex(
        self,
        *indexes: str | Index,
        scope: Literal["JOIN", "ORDER BY", "GROUP BY"] | None = None,
    ) -> Self: ...
    def Where(
        self,
        *conds: str,
        in_conds: dict | None = None,
        not_in_conds: dict | None = None,
    ) -> Self: ...
    def OrderBy(self, *columns: str | Column) -> Self: ...
    def Limit(self, row_count: int) -> Self: ...
    # Statement
    def statement(self, indent: int = 0) -> str: ...
    # Execute
    def Execute(
        self,
        args: list | tuple | DataFrame | Any = None,
        many: bool = False,
        conn: PoolSyncConnection | None = None,
    ) -> int: ...
    async def aioExecute(
        self,
        args: list | tuple | DataFrame | Any = None,
        many: bool = False,
        conn: PoolConnection | None = None,
    ) -> int: ...

# With - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
class WithDML(DML):
    def __init__(self, db_name: str, pool: Pool): ...
    # Clause
    def With(
        self,
        name: str,
        subquery: str | SelectDML,
        *columns: str | Column,
        recursive: bool = False,
    ) -> Self: ...
    def _With(
        self,
        name: str,
        subquery: str | SelectDML,
        columns: tuple[str | Column],
        recursive: bool = False,
    ) -> Self: ...
    def Select(
        self,
        *expressions: str | SQLFunction | Column,
        distinct: bool = False,
        high_priority: bool = False,
        straight_join: bool = False,
        sql_buffer_result: bool = False,
    ) -> SelectDML: ...
    def Update(
        self,
        table: str | Table,
        partition: str | Partition | list | tuple | None = None,
        ignore: bool = False,
        low_priority: bool = False,
        alias: str | None = None,
    ) -> UpdateDML: ...
    def Delete(
        self,
        table: str | Table,
        partition: str | Partition | list | tuple | None = None,
        ignore: bool = False,
        low_priority: bool = False,
        quick: bool = False,
        alias: str | None = None,
        multi_tables: str | list | tuple | None = None,
    ) -> DeleteDML: ...
