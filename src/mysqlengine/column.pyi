import datetime
from decimal import Decimal
from typing import Any, Iterator
from typing_extensions import Self
from sqlcycli.aio.pool import Pool
from sqlcycli.charset import Charset
from sqlcycli.sqlfunc import SQLFunction
from mysqlengine.element import Element, Elements, Metadata, Logs

# Definition -------------------------------------------------------------------------------------------------
class Definition(Element):
    def __init__(
        self,
        data_type: str,
        python_type: type,
        null: bool = False,
        default: Any | None = None,
        comment: str | None = None,
        visible: bool = True,
    ): ...
    # Property -----------------------------------------------------------------------------
    # . definition
    @property
    def data_type(self) -> str: ...
    @property
    def python_type(self) -> type: ...
    @property
    def null(self) -> bool: ...
    @property
    def default(self) -> Any | None: ...
    @property
    def primary_key(self) -> bool: ...
    @property
    def unique_key(self) -> bool: ...
    @property
    def indexed(self) -> bool: ...
    @property
    def comment(self) -> str | None: ...
    @property
    def visible(self) -> bool: ...
    # . integer
    @property
    def unsigned(self) -> bool: ...
    @property
    def auto_increment(self) -> bool: ...
    # . floating/fixed point
    @property
    def precision(self) -> int | None: ...
    @property
    def scale(self) -> int | None: ...
    # . temporal
    @property
    def fsp(self) -> int | None: ...
    @property
    def format(self) -> str | None: ...
    @property
    def auto_init(self) -> bool: ...
    @property
    def auto_update(self) -> bool: ...
    # . string
    @property
    def length(self) -> int | None: ...
    @property
    def elements(self) -> tuple[str] | None: ...
    # Setter
    def setup(self, col: Column) -> bool: ...
    # Copy
    def copy(self) -> Self: ...

# . numeric
class NumericType(Definition):
    def __init__(
        self,
        data_type: str,
        python_type: type,
        null: bool = False,
        default: Any | None = None,
        comment: str | None = None,
        visible: bool = True,
    ): ...

class IntegerType(NumericType):
    def __init__(
        self,
        data_type: str,
        unsigned: bool = False,
        null: bool = False,
        default: int | None = None,
        auto_increment: bool = False,
        comment: str | None = None,
        visible: bool = True,
    ): ...
    # Property
    @property
    def default(self) -> int | None: ...

class FloatingPointType(NumericType):
    def __init__(
        self,
        data_type: str,
        precision: int | None = None,
        null: bool = False,
        default: float | None = None,
        comment: str | None = None,
        visible: bool = True,
    ): ...
    # Property
    @property
    def default(self) -> float | None: ...

class FixedPointType(NumericType):
    def __init__(
        self,
        data_type: str,
        default_precision: int,
        default_scale: int,
        precision: int | None = None,
        scale: int | None = None,
        null: bool = False,
        default: Decimal | None = None,
        comment: str | None = None,
        visible: bool = True,
    ): ...
    # Property
    @property
    def default(self) -> Decimal | None: ...

# . temporal
class TemporalType(Definition):
    def __init__(
        self,
        data_type: str,
        python_type: type,
        default_fsp: int,
        fsp: int | None = None,
        null: bool = False,
        default: Any | None = None,
        comment: str | None = None,
        visible: bool = True,
    ): ...

class DateAndTimeType(TemporalType):
    def __init__(
        self,
        data_type: str,
        python_type: type,
        default_fsp: int,
        fsp: int | None = None,
        null: bool = False,
        auto_init: bool = False,
        auto_update: bool = False,
        default: Any | None = None,
        comment: str | None = None,
        visible: bool = True,
    ): ...

class DateType(DateAndTimeType):
    def __init__(
        self,
        data_type: str,
        null: bool = False,
        default: datetime.date | None = None,
        comment: str | None = None,
        visible: bool = True,
    ): ...
    # Property
    @property
    def default(self) -> datetime.date | None: ...

class DateTimeType(DateAndTimeType):
    def __init__(
        self,
        data_type: str,
        fsp: int | None = None,
        null: bool = False,
        auto_init: bool = False,
        auto_update: bool = False,
        default: datetime.datetime | None = None,
        comment: str | None = None,
        visible: bool = True,
    ): ...
    # Property
    @property
    def default(self) -> datetime.datetime | None: ...

class TimeType(TemporalType):
    def __init__(
        self,
        data_type: str,
        fsp: int | None = None,
        null: bool = False,
        default: datetime.time | None = None,
        comment: str | None = None,
        visible: bool = True,
    ): ...
    # Property
    @property
    def default(self) -> datetime.time | None: ...

class YearType(TemporalType):
    def __init__(
        self,
        data_type: str,
        null: bool = False,
        default: int | datetime.date | datetime.datetime | None = None,
        comment: str | None = None,
        visible: bool = True,
    ): ...
    # Property
    @property
    def default(self) -> int | None: ...

# . string
class StringType(Definition):
    def __init__(
        self,
        data_type: str,
        python_type: type,
        default_length: int,
        length: int | None = None,
        null: bool = False,
        default: Any | None = None,
        comment: str | None = None,
        visible: bool = True,
    ): ...

class ChStringType(StringType):
    def __init__(
        self,
        data_type: str,
        default_length: int,
        length: int | None = None,
        null: bool = False,
        default: str | None = None,
        charset: str | Charset | None = None,
        collate: str | None = None,
        comment: str | None = None,
        visible: bool = True,
    ): ...
    # Property
    @property
    def default(self) -> str | None: ...

class CharType(ChStringType): ...

class TextType(ChStringType):
    def __init__(
        self,
        data_type: str,
        null: bool = False,
        charset: str | Charset | None = None,
        collate: str | None = None,
        comment: str | None = None,
        visible: bool = True,
    ): ...

class EnumType(ChStringType):
    def __init__(
        self,
        data_type: str,
        elements: tuple[str],
        null: bool = False,
        default: str | None = None,
        charset: str | Charset | None = None,
        collate: str | None = None,
        comment: str | None = None,
        visible: bool = True,
    ): ...

class BiStringType(StringType):
    def __init__(
        self,
        data_type: str,
        default_length: int,
        length: int | None = None,
        null: bool = False,
        default: bytes | None = None,
        comment: str | None = None,
        visible: bool = True,
    ): ...

class BinaryType(BiStringType): ...

class BlobType(BiStringType):
    def __init__(
        self,
        data_type: str,
        null: bool = False,
        comment: str | None = None,
        visible: bool = True,
    ): ...

class Define:
    # Numeric
    class TINYINT(IntegerType):
        def __init__(
            self,
            unsigned: bool = False,
            null: bool = False,
            default: int | None = None,
            auto_increment: bool = False,
            comment: str | None = None,
            visible: bool = True,
        ): ...

    class SMALLINT(IntegerType):
        def __init__(
            self,
            unsigned: bool = False,
            null: bool = False,
            default: int | None = None,
            auto_increment: bool = False,
            comment: str | None = None,
            visible: bool = True,
        ): ...

    class MEDIUMINT(IntegerType):
        def __init__(
            self,
            unsigned: bool = False,
            null: bool = False,
            default: int | None = None,
            auto_increment: bool = False,
            comment: str | None = None,
            visible: bool = True,
        ): ...

    class INT(IntegerType):
        def __init__(
            self,
            unsigned: bool = False,
            null: bool = False,
            default: int | None = None,
            auto_increment: bool = False,
            comment: str | None = None,
            visible: bool = True,
        ): ...

    class BIGINT(IntegerType):
        def __init__(
            self,
            unsigned: bool = False,
            null: bool = False,
            default: int | None = None,
            auto_increment: bool = False,
            comment: str | None = None,
            visible: bool = True,
        ): ...

    class FLOAT(FloatingPointType):
        def __init__(
            self,
            null: bool = False,
            default: float | None = None,
            comment: str | None = None,
            visible: bool = True,
        ): ...

    class DOUBLE(FloatingPointType):
        def __init__(
            self,
            null: bool = False,
            default: float | None = None,
            comment: str | None = None,
            visible: bool = True,
        ): ...

    class DECIMAL(FixedPointType):
        def __init__(
            self,
            precision: int | None = None,
            scale: int | None = None,
            null: bool = False,
            default: Decimal | None = None,
            comment: str | None = None,
            visible: bool = True,
        ): ...

    class DATE(DateType):
        def __init__(
            self,
            null: bool = False,
            default: datetime.date | None = None,
            comment: str | None = None,
            visible: bool = True,
        ): ...

    class DATETIME(DateTimeType):
        def __init__(
            self,
            fsp: int | None = None,
            null: bool = False,
            auto_init: bool = False,
            auto_update: bool = False,
            default: datetime.datetime | None = None,
            comment: str | None = None,
            visible: bool = True,
        ): ...

    class TIMESTAMP(DateTimeType):
        def __init__(
            self,
            fsp: int | None = None,
            null: bool = False,
            auto_init: bool = False,
            auto_update: bool = False,
            default: datetime.datetime | None = None,
            comment: str | None = None,
            visible: bool = True,
        ): ...

    class TIME(TimeType):
        def __init__(
            self,
            fsp: int | None = None,
            null: bool = False,
            default: datetime.time | None = None,
            comment: str | None = None,
            visible: bool = True,
        ): ...

    class YEAR(YearType):
        def __init__(
            self,
            null: bool = False,
            default: int | datetime.date | datetime.datetime | None = None,
            comment: str | None = None,
            visible: bool = True,
        ): ...

    # Character String
    class CHAR(CharType):
        def __init__(
            self,
            length: int | None = None,
            null: bool = False,
            default: str | None = None,
            charset: str | Charset | None = None,
            collate: str | None = None,
            comment: str | None = None,
            visible: bool = True,
        ): ...

    class VARCHAR(CharType):
        def __init__(
            self,
            length: int,
            null: bool = False,
            default: str | None = None,
            charset: str | Charset | None = None,
            collate: str | None = None,
            comment: str | None = None,
            visible: bool = True,
        ): ...

    class TINYTEXT(TextType):
        def __init__(
            self,
            null: bool = False,
            charset: str | Charset | None = None,
            collate: str | None = None,
            comment: str | None = None,
            visible: bool = True,
        ): ...

    class TEXT(TextType):
        def __init__(
            self,
            null: bool = False,
            charset: str | Charset | None = None,
            collate: str | None = None,
            comment: str | None = None,
            visible: bool = True,
        ): ...

    class MEDIUMTEXT(TextType):
        def __init__(
            self,
            null: bool = False,
            charset: str | Charset | None = None,
            collate: str | None = None,
            comment: str | None = None,
            visible: bool = True,
        ): ...

    class LONGTEXT(TextType):
        def __init__(
            self,
            null: bool = False,
            charset: str | Charset | None = None,
            collate: str | None = None,
            comment: str | None = None,
            visible: bool = True,
        ): ...

    class ENUM(EnumType):
        def __init__(
            self,
            *elements: str,
            null: bool = False,
            default: str | None = None,
            charset: str | Charset | None = None,
            collate: str | None = None,
            comment: str | None = None,
            visible: bool = True,
        ): ...

    # Binary String
    class BINARY(BinaryType):
        def __init__(
            self,
            length: int | None = None,
            null: bool = False,
            default: bytes | None = None,
            comment: str | None = None,
            visible: bool = True,
        ): ...

    class VARBINARY(BinaryType):
        def __init__(
            self,
            length: int,
            null: bool = False,
            default: bytes | None = None,
            comment: str | None = None,
            visible: bool = True,
        ): ...

    class TINYBLOB(BlobType):
        def __init__(
            self,
            null: bool = False,
            comment: str | None = None,
            visible: bool = True,
        ): ...

    class BLOB(BlobType):
        def __init__(
            self,
            null: bool = False,
            comment: str | None = None,
            visible: bool = True,
        ): ...

    class MEDIUMBLOB(BlobType):
        def __init__(
            self,
            null: bool = False,
            comment: str | None = None,
            visible: bool = True,
        ): ...

    class LONGBLOB(BlobType):
        def __init__(
            self,
            null: bool = False,
            comment: str | None = None,
            visible: bool = True,
        ): ...

# Column -----------------------------------------------------------------------------------------------------
class Column(Element):
    def __init__(self, definition: Definition): ...
    # Property
    @property
    def name(self) -> str: ...
    @property
    def db_name(self) -> str: ...
    @property
    def tb_name(self) -> str: ...
    @property
    def tb_qualified_name(self) -> str: ...
    @property
    def definition(self) -> Definition: ...
    @property
    def primary_key(self) -> bool: ...
    @property
    def unique_key(self) -> bool: ...
    @property
    def indexed(self) -> bool: ...
    @property
    def position(self) -> int: ...
    # Sync
    def Initialize(self, force: bool = False) -> Logs: ...
    def Add(self, position: int = 0) -> Logs: ...
    def Exists(self) -> bool: ...
    def Drop(self) -> Logs: ...
    def Modify(
        self,
        definition: Definition | None = None,
        position: int = 0,
    ) -> Logs: ...
    def SetVisible(self, visible: bool) -> Logs: ...
    def SetDefault(self, default: Any | None) -> Logs: ...
    def ShowMetadata(self) -> ColumnMetadata: ...
    def ShowColumnNames(self) -> tuple[str]: ...
    def SyncFromRemote(self) -> Logs: ...
    def SyncToRemote(self) -> Logs: ...
    # Async
    async def aioInitialize(self, force: bool = False) -> Logs: ...
    async def aioAdd(self, position: int = 0) -> Logs: ...
    async def aioExists(self) -> bool: ...
    async def aioDrop(self) -> Logs: ...
    async def aioModify(
        self,
        definition: Definition | None = None,
        position: int = 0,
    ) -> Logs: ...
    async def aioSetVisible(self, visible: bool) -> Logs: ...
    async def aioSetDefault(self, default: Any | None) -> Logs: ...
    async def aioShowMetadata(self) -> ColumnMetadata: ...
    async def aioShowColumnNames(self) -> tuple[str]: ...
    async def aioSyncFromRemote(self) -> Logs: ...
    async def aioSyncToRemote(self) -> Logs: ...
    # Setter
    def setup(
        self,
        tb_name: str,
        db_name: str,
        charset: str | Charset,
        collate: str | None,
        pool: Pool,
    ) -> bool: ...
    def set_name(self, name: str): ...
    # Copy
    def copy(self) -> Self: ...

class GeneratedColumn(Column):
    def __init__(
        self,
        definition: Definition,
        expression: str | SQLFunction,
        virtual: bool = True,
    ): ...
    # Property
    @property
    def expression(self) -> str: ...
    @property
    def virtual(self) -> bool: ...
    # Sync
    def Modify(
        self,
        definition: Definition | None = None,
        expression: str | SQLFunction | None = None,
        position: int = 0,
    ) -> Logs: ...
    def SetDefault(self, default: Any | None) -> Logs: ...
    # Async
    async def aioModify(
        self,
        definition: Definition | None = None,
        expression: str | SQLFunction | None = None,
        position: int = 0,
    ) -> Logs: ...
    async def aioSetDefault(self, default: Any | None) -> Logs: ...

# Columns ----------------------------------------------------------------------------------------------------
class Columns(Elements):
    def __init__(self, *columns: Column): ...
    # Property
    @property
    def elements(self) -> tuple[Column]: ...
    # Collection
    def search_name(
        self,
        *names: str | Column,
        exact: bool = True,
    ) -> Columns: ...
    def search_type(
        self,
        *types: str | type | Definition | Column,
        exact: bool = True,
    ) -> Columns: ...
    def filter(self, *elements: str | Column | Columns) -> Columns: ...
    def issubset(self, *elements: str | Column | Columns) -> bool: ...
    def add(self, element: Column) -> bool: ...
    def remove(self, element: str | Column) -> bool: ...
    # Setter
    def setup(
        self,
        tb_name: str,
        db_name: str,
        charset: str | Charset,
        collate: str | None,
        pool: Pool,
    ) -> bool: ...
    # Accessors
    def values(self) -> tuple[Column]: ...
    def items(self) -> tuple[tuple[str, Column]]: ...
    def get(self, key: str | Column, default: Any = None) -> Column | Any: ...
    # Copy
    def copy(self) -> Self: ...
    # Special Methods
    def __getitem__(self, key: str | Column) -> Column: ...
    def __contains__(self, key: str | Column) -> bool: ...
    def __iter__(self) -> Iterator[Column]: ...

# Metadata ---------------------------------------------------------------------------------------------------
class ColumnMetadata(Metadata):
    def __init__(self, meta: dict): ...
    # Property
    @property
    def catelog_name(self) -> str: ...
    @property
    def db_name(self) -> str: ...
    @property
    def tb_name(self) -> str: ...
    @property
    def column_name(self) -> str: ...
    @property
    def column_type(self) -> str: ...
    @property
    def column_key(self) -> str | None: ...
    @property
    def position(self) -> int: ...
    @property
    def default(self) -> str | None: ...
    @property
    def null(self) -> bool: ...
    @property
    def data_type(self) -> str: ...
    @property
    def character_maximum_length(self) -> int | None: ...
    @property
    def character_octet_length(self) -> int | None: ...
    @property
    def numeric_precision(self) -> int | None: ...
    @property
    def numeric_scale(self) -> int | None: ...
    @property
    def datetime_precision(self) -> int | None: ...
    @property
    def charset(self) -> Charset | None: ...
    @property
    def extra(self) -> str: ...
    @property
    def privileges(self) -> str: ...
    @property
    def comment(self) -> str: ...
    @property
    def expression(self) -> str | None: ...
    @property
    def virtual(self) -> bool | None: ...
    @property
    def srs_id(self) -> int | None: ...
    @property
    def engine_attribute(self) -> str | None: ...
    @property
    def secondary_engine_attribute(self) -> str | None: ...
    @property
    def visible(self) -> bool: ...
    # . integer
    @property
    def auto_increment(self) -> bool: ...
    @property
    def unsigned(self) -> bool: ...
    # . datetime
    @property
    def auto_init(self) -> bool: ...
    @property
    def auto_update(self) -> bool: ...
    # . index
    @property
    def primary_key(self) -> bool: ...
    @property
    def unique_key(self) -> bool: ...
    @property
    def indexed(self) -> bool: ...
    @property
    def column_index_name(self) -> str | None: ...
    @property
    def column_index_seq(self) -> int | None: ...
    @property
    def column_index_length(self) -> int | None: ...
