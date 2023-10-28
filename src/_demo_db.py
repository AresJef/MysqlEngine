from mysqlengine.connection import Server
from mysqlengine.database import Database, Table, TimeTable
from mysqlengine.column import Column
from mysqlengine.index import Index
from mysqlengine.dtype import MysqlTypes


# Database pre-defined name
class MyDatabase1(Database):
    def __init__(self, server: Server) -> None:
        super().__init__(server, "mydb1")

    # Metadata
    def metadata(self) -> None:
        self.my_table1 = MyTable(self, "my_table1")
        self.my_table2 = MyTable(self, "my_table2")
        self.my_timetable_d = MyTimeTable(self, "my_timetable_d", "D")
        self.my_timetable_w = MyTimeTable(self, "my_timetable_w", "week")
        self.my_timetable_m = MyTimeTable(self, "my_timetable_m", "month")
        self.my_timetable_m2 = MyTimeTable(self, "my_timetable_m2", "month")
        self.my_timetable_y = MyTimeTable(self, "my_timetable_y", "year")


# Table
class MyTable(Table):
    def __init__(self, database: Database, name: str) -> None:
        super().__init__(database, name)

    # Metadata
    def metadata(self) -> None:
        # fmt: off
        # Columns
        self.columns_metadata(
            Column("id", MysqlTypes.BIGINT(primary_key=True)),
            Column("bigint_type", MysqlTypes.BIGINT()),
            Column("int_type", MysqlTypes.INT()),
            Column("mediumint_type", MysqlTypes.MEDIUMINT()),
            Column("smallint_type", MysqlTypes.SMALLINT()),
            Column("tinyint_type", MysqlTypes.TINYINT()),
            Column("float_type", MysqlTypes.FLOAT()),
            Column("double_type", MysqlTypes.DOUBLE()),
            Column("decimal_type", MysqlTypes.DECIMAL(10, 2)),
            Column("timestamp_type", MysqlTypes.TIMESTAMP()),
            Column("datetime_type", MysqlTypes.DATETIME()),
            Column("date_type", MysqlTypes.DATE()),
            Column("time_type", MysqlTypes.TIME()),
            Column("year_type", MysqlTypes.YEAR()),
            Column("char_type", MysqlTypes.CHAR(10)),
            Column("varchar_type", MysqlTypes.VARCHAR(20)),
            Column("enum_type", MysqlTypes.ENUM("a", "b", "c", default="a")),
            Column("binary_type", MysqlTypes.BINARY(10)),
            Column("varbinary_type", MysqlTypes.VARBINARY(20)),
            Column("create_dt", MysqlTypes.DATETIME(auto_init=True, tabletime=True)),
            Column("update_dt", MysqlTypes.DATETIME(auto_init=True, auto_update=True)),
        )
        # Indexes
        self.indexes_metadata(
            Index(self.columns["bigint_type"], unique=True, primary_unique=True),
        )
        # fmt: on


# TimeTable
class MyTimeTable(TimeTable):
    def __init__(self, database: Database, name: str, time_unit: str) -> None:
        super().__init__(database, name, time_unit)

    # Metadata
    def metadata(self) -> None:
        # fmt: off
        # Columns
        self.columns_metadata(
            Column("id", MysqlTypes.BIGINT(primary_key=True)),
            Column("bigint_type", MysqlTypes.BIGINT()),
            Column("int_type", MysqlTypes.INT()),
            Column("mediumint_type", MysqlTypes.MEDIUMINT()),
            Column("smallint_type", MysqlTypes.SMALLINT()),
            Column("tinyint_type", MysqlTypes.TINYINT()),
            Column("float_type", MysqlTypes.FLOAT()),
            Column("double_type", MysqlTypes.DOUBLE()),
            Column("decimal_type", MysqlTypes.DECIMAL(10, 2)),
            Column("timestamp_type", MysqlTypes.TIMESTAMP()),
            Column("datetime_type", MysqlTypes.DATETIME()),
            Column("date_type", MysqlTypes.DATE()),
            Column("time_type", MysqlTypes.TIME()),
            Column("year_type", MysqlTypes.YEAR()),
            Column("char_type", MysqlTypes.CHAR(10)),
            Column("varchar_type", MysqlTypes.VARCHAR(20)),
            Column("enum_type", MysqlTypes.ENUM("a", "b", "c", default="a")),
            Column("binary_type", MysqlTypes.BINARY(10)),
            Column("varbinary_type", MysqlTypes.VARBINARY(20)),
            Column("create_dt", MysqlTypes.DATETIME(auto_init=True, tabletime=True)),
            Column("update_dt", MysqlTypes.DATETIME(auto_init=True, auto_update=True)),
        )
        # Indexes
        self.indexes_metadata(
            Index(self.columns["bigint_type"], unique=True, primary_unique=True),
        )
        # fmt: on


# Database dynamic name
class MyDatabase2(Database):
    def __init__(self, server: Server, country: str) -> None:
        super().__init__(server, "mydb2_" + country)

    # Metadata
    def metadata(self) -> None:
        self.my_table1 = MyTable(self, "my_table1")
        self.my_table2 = MyTable(self, "my_table2")
        self.my_timetable_d = MyTimeTable(self, "my_timetable_d", "D")
        self.my_timetable_w = MyTimeTable(self, "my_timetable_w", "week")
        self.my_timetable_m = MyTimeTable(self, "my_timetable_m", "month")
        self.my_timetable_m2 = MyTimeTable(self, "my_timetable_m2", "month")
        self.my_timetable_y = MyTimeTable(self, "my_timetable_y", "year")