from timeit import timeit
import asyncio, datetime, time, random, pandas as pd
from mysqlengine.engine import Engine
from mysqlengine.connection import Server
from _demo_db import MyDatabase1, MyDatabase2


def validate_column_dtype() -> None:
    import datetime
    from decimal import Decimal
    from mysqlengine.dtype import MysqlTypes
    from mysqlengine.column import Column, Columns
    from mysqlengine.column import DummyColumn, TableColumns

    print(" Integers ".center(100, "="))
    print(MysqlTypes.BIGINT(True, True, True, True, 1))
    print(MysqlTypes.BIGINT(False, True, True, True, 1))
    print(MysqlTypes.BIGINT(False, False, True, True, 1))
    print(MysqlTypes.BIGINT(False, False, False, True, 1))
    print(MysqlTypes.BIGINT(False, False, False, False, 1))
    print(MysqlTypes.BIGINT(False, False, False, False, "1"))
    print(MysqlTypes.BIGINT(False, False, False, False, 1.0))
    print(MysqlTypes.BIGINT(False, False, False, False, None))
    print(x := MysqlTypes.BIGINT(primary_key=True))
    print(repr(x))
    print("BIGINT.mysql".ljust(30), x.mysql, type(x.mysql))
    print("BIGINT.python".ljust(30), x.python, type(x.python))
    print("BIGINT.primary_key".ljust(30), x.primary_key, type(x.primary_key))
    print("BIGINT.auto_increment".ljust(30), x.auto_increment, type(x.auto_increment))
    print("BIGINT.null".ljust(30), x.null, type(x.null))
    print("BIGINT.default".ljust(30), x.default, type(x.default))
    print("BIGINT.signed".ljust(30), x.signed, type(x.signed))
    print("BIGINT.min".ljust(30), x.min, type(x.min))
    print("BIGINT.max".ljust(30), x.max, type(x.max))
    print(c1 := Column("bigint", x))
    print(repr(c1))
    print()

    print(" FLOAT ".center(100, "="))
    print(MysqlTypes.FLOAT(23, True, 1.0))
    print(MysqlTypes.FLOAT(23, False, 1.0))
    print(MysqlTypes.FLOAT(23, False, 1))
    print(MysqlTypes.FLOAT(23, False, "1"))
    print(MysqlTypes.FLOAT(23, False, None))
    print(x := MysqlTypes.FLOAT())
    print(repr(x))
    print("FLOAT.mysql".ljust(30), x.mysql, type(x.mysql))
    print("FLOAT.python".ljust(30), x.python, type(x.python))
    print("FLOAT.primary_key".ljust(30), x.primary_key, type(x.primary_key))
    print("FLOAT.auto_increment".ljust(30), x.auto_increment, type(x.auto_increment))
    print("FLOAT.null".ljust(30), x.null, type(x.null))
    print("FLOAT.default".ljust(30), x.default, type(x.default))
    print("FLOAT.precision".ljust(30), x.precision, type(x.precision))
    print(c2 := Column("float", x))
    print(repr(c2))
    print()

    print(" DOUBLE ".center(100, "="))
    print(MysqlTypes.DOUBLE(True, 1.0))
    print(MysqlTypes.DOUBLE(False, 1.0))
    print(MysqlTypes.DOUBLE(False, 1))
    print(MysqlTypes.DOUBLE(False, "1"))
    print(MysqlTypes.DOUBLE(False, None))
    print(x := MysqlTypes.DOUBLE())
    print(repr(x))
    print("DOUBLE.mysql".ljust(30), x.mysql, type(x.mysql))
    print("DOUBLE.python".ljust(30), x.python, type(x.python))
    print("DOUBLE.primary_key".ljust(30), x.primary_key, type(x.primary_key))
    print("DOUBLE.auto_increment".ljust(30), x.auto_increment, type(x.auto_increment))
    print("DOUBLE.null".ljust(30), x.null, type(x.null))
    print("DOUBLE.default".ljust(30), x.default, type(x.default))
    print("DOUBLE.precision".ljust(30), x.precision, type(x.precision))
    print(c3 := Column("double", x))
    print(repr(c3))
    print()

    print(" DECIMAL ".center(100, "="))
    print(MysqlTypes.DECIMAL(12, 4, True, 1.0))
    print(MysqlTypes.DECIMAL(12, 4, False, 1.0))
    print(MysqlTypes.DECIMAL(12, 4, False, None))
    print(x := MysqlTypes.DECIMAL())
    print(repr(x))
    print("DECIMAL.mysql".ljust(30), x.mysql, type(x.mysql))
    print("DECIMAL.python".ljust(30), x.python, type(x.python))
    print("DECIMAL.primary_key".ljust(30), x.primary_key, type(x.primary_key))
    print("DECIMAL.auto_increment".ljust(30), x.auto_increment, type(x.auto_increment))
    print("DECIMAL.null".ljust(30), x.null, type(x.null))
    print("DECIMAL.default".ljust(30), x.default, type(x.default))
    print("DECIMAL.default_float".ljust(30), x.default_float, type(x.default_float))
    print("DECIMAL.precision".ljust(30), x.precision, type(x.precision))
    print("DECIMAL.scale".ljust(30), x.scale, type(x.scale))
    print("DECIMAL.min".ljust(30), x.min, type(x.min))
    print("DECIMAL.min_float".ljust(30), x.min_float, type(x.min_float))
    print("DECIMAL.max".ljust(30), x.max, type(x.max))
    print("DECIMAL.max_float".ljust(30), x.max_float, type(x.max_float))
    print(c4 := Column("decimal", x))
    print(repr(c4))
    print()

    print(" DATE ".center(100, "="))
    print(MysqlTypes.DATE(True, datetime.datetime.now()))
    print(MysqlTypes.DATE(False, datetime.datetime.now()))
    print(MysqlTypes.DATE(False, None))
    print(x := MysqlTypes.DATE())
    print(repr(x))
    print("DATE.mysql".ljust(30), x.mysql, type(x.mysql))
    print("DATE.python".ljust(30), x.python, type(x.python))
    print("DATE.primary_key".ljust(30), x.primary_key, type(x.primary_key))
    print("DATE.auto_increment".ljust(30), x.auto_increment, type(x.auto_increment))
    print("DATE.null".ljust(30), x.null, type(x.null))
    print("DATE.default".ljust(30), x.default, type(x.default))
    print("DATE.default_str".ljust(30), x.default_str, type(x.default_str))
    print("DATE.format".ljust(30), x.format, type(x.format))
    print("DATE.tabletime".ljust(30), x.tabletime, type(x.tabletime))
    print(c5 := Column("date", x))
    print(repr(c5))
    print()

    print(" DATETIME ".center(100, "="))
    print(MysqlTypes.DATETIME(6, True, True, True, datetime.datetime.now()))
    print(MysqlTypes.DATETIME(6, False, True, True, datetime.datetime.now()))
    print(MysqlTypes.DATETIME(6, False, False, True, datetime.datetime.now()))
    print(MysqlTypes.DATETIME(6, False, False, False, datetime.datetime.now()))
    print(MysqlTypes.DATETIME(0, False, False, False, datetime.datetime.now()))
    print(MysqlTypes.DATETIME(0, False, False, False, None))
    print(x := MysqlTypes.DATETIME(tabletime=True))
    print(repr(x))
    print("DATETIME.mysql".ljust(30), x.mysql, type(x.mysql))
    print("DATETIME.python".ljust(30), x.python, type(x.python))
    print("DATETIME.primary_key".ljust(30), x.primary_key, type(x.primary_key))
    print("DATETIME.auto_increment".ljust(30), x.auto_increment, type(x.auto_increment))
    print("DATETIME.null".ljust(30), x.null, type(x.null))
    print("DATETIME.default".ljust(30), x.default, type(x.default))
    print("DATETIME.default_str".ljust(30), x.default_str, type(x.default_str))
    print("DATETIME.format".ljust(30), x.format, type(x.format))
    print("DATETIME.fraction".ljust(30), x.fraction, type(x.fraction))
    print("DATETIME.auto_init".ljust(30), x.auto_init, type(x.auto_init))
    print("DATETIME.auto_update".ljust(30), x.auto_update, type(x.auto_update))
    print("DATETIME.tabletime".ljust(30), x.tabletime, type(x.tabletime))
    print(c6 := Column("datetime", x))
    print(repr(c6))
    print()

    # fmt: off
    print(" TIMESTAMP ".center(100, "="))
    print(MysqlTypes.TIMESTAMP(6, True, True, True))
    print(MysqlTypes.TIMESTAMP(6, False, True, True))
    print(MysqlTypes.TIMESTAMP(6, False, False, True))
    print(MysqlTypes.TIMESTAMP(6, False, False, False))
    print(x := MysqlTypes.TIMESTAMP())
    print(repr(x))
    print("TIMESTAMP.mysql".ljust(30), x.mysql, type(x.mysql))
    print("TIMESTAMP.python".ljust(30), x.python, type(x.python))
    print("TIMESTAMP.primary_key".ljust(30), x.primary_key, type(x.primary_key))
    print("TIMESTAMP.auto_increment".ljust(30), x.auto_increment, type(x.auto_increment))
    print("TIMESTAMP.null".ljust(30), x.null, type(x.null))
    print("TIMESTAMP.default".ljust(30), x.default, type(x.default))
    print("TIMESTAMP.format".ljust(30), x.format, type(x.format))
    print("TIMESTAMP.fraction".ljust(30), x.fraction, type(x.fraction))
    print("TIMESTAMP.auto_init".ljust(30), x.auto_init, type(x.auto_init))
    print("TIMESTAMP.auto_update".ljust(30), x.auto_update, type(x.auto_update))
    print("TIMESTAMP.tabletime".ljust(30), x.tabletime, type(x.tabletime))
    print(c7 := Column("timestamp", x))
    print(repr(c7))
    print()
    # fmt: on

    print(" TIME ".center(100, "="))
    print(MysqlTypes.TIME(6, True, datetime.datetime.now()))
    print(MysqlTypes.TIME(6, False, datetime.datetime.now()))
    print(MysqlTypes.TIME(0, False, datetime.datetime.now()))
    print(MysqlTypes.TIME(0, False, None))
    print(x := MysqlTypes.TIME())
    print(repr(x))
    print("TIME.mysql".ljust(30), x.mysql, type(x.mysql))
    print("TIME.python".ljust(30), x.python, type(x.python))
    print("TIME.primary_key".ljust(30), x.primary_key, type(x.primary_key))
    print("TIME.auto_increment".ljust(30), x.auto_increment, type(x.auto_increment))
    print("TIME.null".ljust(30), x.null, type(x.null))
    print("TIME.default".ljust(30), x.default, type(x.default))
    print("TIME.default_str".ljust(30), x.default_str, type(x.default_str))
    print("TIME.format".ljust(30), x.format, type(x.format))
    print("TIME.fraction".ljust(30), x.fraction, type(x.fraction))
    print(c8 := Column("time", x))
    print(repr(c8))
    print()

    print(" YEAR ".center(100, "="))
    print(MysqlTypes.YEAR(True, 1970))
    print(MysqlTypes.YEAR(True, "1970"))
    print(MysqlTypes.YEAR(False, "1970"))
    print(x := MysqlTypes.YEAR())
    print(repr(x))
    print("YEAR.mysql".ljust(30), x.mysql, type(x.mysql))
    print("YEAR.python".ljust(30), x.python, type(x.python))
    print("YEAR.primary_key".ljust(30), x.primary_key, type(x.primary_key))
    print("YEAR.auto_increment".ljust(30), x.auto_increment, type(x.auto_increment))
    print("YEAR.null".ljust(30), x.null, type(x.null))
    print("YEAR.default".ljust(30), x.default, type(x.default))
    print("YEAR.min".ljust(30), x.min, type(x.min))
    print("YEAR.max".ljust(30), x.max, type(x.max))
    print(c9 := Column("year", x))
    print(repr(c9))
    print()

    print(" CHAR ".center(100, "="))
    print(MysqlTypes.CHAR(255, True, "char"))
    print(MysqlTypes.CHAR(255, False, "char"))
    print(MysqlTypes.CHAR(10, False, None))
    print(x := MysqlTypes.CHAR())
    print(repr(x))
    print("CHAR.mysql".ljust(30), x.mysql, type(x.mysql))
    print("CHAR.python".ljust(30), x.python, type(x.python))
    print("CHAR.primary_key".ljust(30), x.primary_key, type(x.primary_key))
    print("CHAR.auto_increment".ljust(30), x.auto_increment, type(x.auto_increment))
    print("CHAR.null".ljust(30), x.null, type(x.null))
    print("CHAR.default".ljust(30), x.default, type(x.default))
    print("CHAR.length".ljust(30), x.length, type(x.length))
    print("CHAR.max_length".ljust(30), x.max_length, type(x.max_length))
    print(c10 := Column("char", x))
    print(repr(c10))
    print()

    print(" VARCHAR ".center(100, "="))
    print(MysqlTypes.VARCHAR(255, True, "varchar"))
    print(MysqlTypes.VARCHAR(255, False, "varchar"))
    print(MysqlTypes.VARCHAR(10, False, None))
    print(x := MysqlTypes.VARCHAR())
    print(repr(x))
    print("VARCHAR.mysql".ljust(30), x.mysql, type(x.mysql))
    print("VARCHAR.python".ljust(30), x.python, type(x.python))
    print("VARCHAR.primary_key".ljust(30), x.primary_key, type(x.primary_key))
    print("VARCHAR.auto_increment".ljust(30), x.auto_increment, type(x.auto_increment))
    print("VARCHAR.null".ljust(30), x.null, type(x.null))
    print("VARCHAR.default".ljust(30), x.default, type(x.default))
    print("VARCHAR.length".ljust(30), x.length, type(x.length))
    print("VARCHAR.max_length".ljust(30), x.max_length, type(x.max_length))
    print(c11 := Column("varchar", x))
    print(repr(c11))
    print()

    print(" LONGTEXT ".center(100, "="))
    print(MysqlTypes.LONGTEXT(True))
    print(MysqlTypes.LONGTEXT(False))
    print(x := MysqlTypes.LONGTEXT())
    print(repr(x))
    print("LONGTEXT.mysql".ljust(30), x.mysql, type(x.mysql))
    print("LONGTEXT.python".ljust(30), x.python, type(x.python))
    print("LONGTEXT.primary_key".ljust(30), x.primary_key, type(x.primary_key))
    print("LONGTEXT.auto_increment".ljust(30), x.auto_increment, type(x.auto_increment))
    print("LONGTEXT.null".ljust(30), x.null, type(x.null))
    print("LONGTEXT.default".ljust(30), x.default, type(x.default))
    print("LONGTEXT.length".ljust(30), x.length, type(x.length))
    print("LONGTEXT.max_length".ljust(30), x.max_length, type(x.max_length))
    print(c12 := Column("longtext", x))
    print(repr(c12))
    print()

    print(" ENUM ".center(100, "="))
    print(MysqlTypes.ENUM("a", "b", "c", null=True, default="a"))
    print(MysqlTypes.ENUM("a", "b", "c", null=False, default="a"))
    print(MysqlTypes.ENUM("a", "b", "c", null=False, default=None))
    print(x := MysqlTypes.ENUM("a", "b", "c"))
    print(repr(x))
    print("ENUM.mysql".ljust(30), x.mysql, type(x.mysql))
    print("ENUM.python".ljust(30), x.python, type(x.python))
    print("ENUM.primary_key".ljust(30), x.primary_key, type(x.primary_key))
    print("ENUM.auto_increment".ljust(30), x.auto_increment, type(x.auto_increment))
    print("ENUM.null".ljust(30), x.null, type(x.null))
    print("ENUM.default".ljust(30), x.default, type(x.default))
    print("ENUM.enums".ljust(30), x.enums, type(x.enums))
    print("ENUM.enums_set".ljust(30), x.enums_set, type(x.enums_set))
    print(c13 := Column("enum", x))
    print(repr(c13))
    print()

    print(" BINARY ".center(100, "="))
    print(MysqlTypes.BINARY(255, True, b"char"))
    print(MysqlTypes.BINARY(255, False, b"char"))
    print(MysqlTypes.BINARY(10, False, None))
    print(x := MysqlTypes.BINARY())
    print(repr(x))
    print("BINARY.mysql".ljust(30), x.mysql, type(x.mysql))
    print("BINARY.python".ljust(30), x.python, type(x.python))
    print("BINARY.primary_key".ljust(30), x.primary_key, type(x.primary_key))
    print("BINARY.auto_increment".ljust(30), x.auto_increment, type(x.auto_increment))
    print("BINARY.null".ljust(30), x.null, type(x.null))
    print("BINARY.default".ljust(30), x.default, type(x.default))
    print("BINARY.length".ljust(30), x.length, type(x.length))
    print("BINARY.max_length".ljust(30), x.max_length, type(x.max_length))
    print(c14 := Column("binary", x))
    print(repr(c14))
    print()

    # fmt: off
    print(" VARBINARY ".center(100, "="))
    print(MysqlTypes.VARBINARY(255, True, b"varchar"))
    print(MysqlTypes.VARBINARY(255, False, b"varchar"))
    print(MysqlTypes.VARBINARY(10, False, None))
    print(x := MysqlTypes.VARBINARY())
    print(repr(x))
    print("VARBINARY.mysql".ljust(30), x.mysql, type(x.mysql))
    print("VARBINARY.python".ljust(30), x.python, type(x.python))
    print("VARBINARY.primary_key".ljust(30), x.primary_key, type(x.primary_key))
    print("VARBINARY.auto_increment".ljust(30), x.auto_increment, type(x.auto_increment))
    print("VARBINARY.null".ljust(30), x.null, type(x.null))
    print("VARBINARY.default".ljust(30), x.default, type(x.default))
    print("VARBINARY.length".ljust(30), x.length, type(x.length))
    print("VARBINARY.max_length".ljust(30), x.max_length, type(x.max_length))
    print(c15 := Column("varbinary", x))
    print(repr(c15))
    print()
    # fmt: on

    print(" LONGBLOB ".center(100, "="))
    print(MysqlTypes.LONGBLOB(True))
    print(MysqlTypes.LONGBLOB(False))
    print(x := MysqlTypes.LONGBLOB())
    print(repr(x))
    print("LONGBLOB.mysql".ljust(30), x.mysql, type(x.mysql))
    print("LONGBLOB.python".ljust(30), x.python, type(x.python))
    print("LONGBLOB.primary_key".ljust(30), x.primary_key, type(x.primary_key))
    print("LONGBLOB.auto_increment".ljust(30), x.auto_increment, type(x.auto_increment))
    print("LONGBLOB.null".ljust(30), x.null, type(x.null))
    print("LONGBLOB.default".ljust(30), x.default, type(x.default))
    print("LONGBLOB.length".ljust(30), x.length, type(x.length))
    print("LONGBLOB.max_length".ljust(30), x.max_length, type(x.max_length))
    print(c16 := Column("longblob", x))
    print(repr(c16))
    print()

    # Dummy Column
    print(" DummyColumn ".center(100, "="))
    dc = DummyColumn()
    print("DummyColumn.repr".ljust(30), repr(dc))
    print("DummyColumn.name".ljust(30), dc.name)
    print()

    # Columns
    # fmt: off
    print(" Columns ".center(100, "="))
    cols = Columns(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16)
    print("Columns.repr".ljust(30), repr(cols))
    print("Columns.str".ljust(30), cols)
    print("Columns.hash".ljust(30), hash(cols))
    print("Columns.len".ljust(30), len(cols))
    print("Columns.bool".ljust(30), bool(cols))
    print("Columns.getitem".ljust(30), cols["bigint"])
    print("Columns.contains".ljust(30), "bigint" in cols)
    print("Columns.contains".ljust(30), "apple" in cols)
    print("Columns.keys".ljust(30), cols.keys(), type(cols.keys()))
    print("Columns.values".ljust(30), cols.values(), type(cols.values()))
    print("Columns.items".ljust(30), cols.items(), type(cols.items()))
    print("Columns.get".ljust(30), cols.get("bigint"))
    print("Columns.get".ljust(30), cols.get("apple"))
    print("Columns.search_by_name".ljust(30), cols.search_by_name("int", False))
    print("Columns.search_by_name".ljust(30), cols.search_by_name("int", True))
    print("Columns.search_by_name".ljust(30), cols.search_by_name("bigint", True))
    print("Columns.search_by_mysql_dtype".ljust(30), cols.search_by_mysql_dtype("INT", False))
    print("Columns.search_by_mysql_dtype".ljust(30), cols.search_by_mysql_dtype("INT", True))
    print("Columns.search_by_mysql_dtype".ljust(30), cols.search_by_mysql_dtype("BIGINT", True))
    print("Columns.search_by_python_dtype".ljust(30), cols.search_by_python_dtype(int))
    print("Columns.search_by_python_dtype".ljust(30), cols.search_by_python_dtype())
    print("Columns.search_by_python_dtype".ljust(30), cols.search_by_python_dtype(int, float))
    print("Columns.search_by_python_dtype".ljust(30), cols.search_by_python_dtype(int, float, Decimal))
    print()

    # Empty Columns
    print(" EmptyColumns ".center(100, "="))
    cols = Columns()
    print("Columns.repr".ljust(30), repr(cols))
    print("Columns.str".ljust(30), cols)
    print("Columns.hash".ljust(30), hash(cols))
    print("Columns.len".ljust(30), len(cols))
    print("Columns.bool".ljust(30), bool(cols))
    print("Columns.contains".ljust(30), "bigint" in cols)
    print("Columns.contains".ljust(30), "apple" in cols)
    print("Columns.keys".ljust(30), cols.keys(), type(cols.keys()))
    print("Columns.values".ljust(30), cols.values(), type(cols.values()))
    print("Columns.items".ljust(30), cols.items(), type(cols.items()))
    print("Columns.get".ljust(30), cols.get("bigint"))
    print("Columns.get".ljust(30), cols.get("apple"))
    print("Columns.search_by_name".ljust(30), cols.search_by_name("int", False))
    print("Columns.search_by_name".ljust(30), cols.search_by_name("int", True))
    print("Columns.search_by_name".ljust(30), cols.search_by_name("bigint", True))
    print("Columns.search_by_mysql_dtype".ljust(30), cols.search_by_mysql_dtype("INT", False))
    print("Columns.search_by_mysql_dtype".ljust(30), cols.search_by_mysql_dtype("INT", True))
    print("Columns.search_by_mysql_dtype".ljust(30), cols.search_by_mysql_dtype("BIGINT", True))
    print("Columns.search_by_python_dtype".ljust(30), cols.search_by_python_dtype(int))
    print("Columns.search_by_python_dtype".ljust(30), cols.search_by_python_dtype())
    print("Columns.search_by_python_dtype".ljust(30), cols.search_by_python_dtype(int, float))
    print("Columns.search_by_python_dtype".ljust(30), cols.search_by_python_dtype(int, float, Decimal))
    print()

    # Table Columns
    print(" TableColumns ".center(100, "="))
    cols = TableColumns(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16)
    print("TableColumns.repr".ljust(30), repr(cols))
    print("TableColumns.str".ljust(30), cols)
    print("TableColumns.hash".ljust(30), hash(cols))
    print("TableColumns.len".ljust(30), len(cols))
    print("TableColumns.bool".ljust(30), bool(cols))
    print("TableColumns.getitem".ljust(30), cols["bigint"])
    print("TableColumns.contains".ljust(30), "bigint" in cols)
    print("TableColumns.contains".ljust(30), "apple" in cols)
    print("TableColumns.keys".ljust(30), cols.keys(), type(cols.keys()))
    print("TableColumns.values".ljust(30), cols.values(), type(cols.values()))
    print("TableColumns.items".ljust(30), cols.items(), type(cols.items()))
    print("TableColumns.get".ljust(30), cols.get("bigint"))
    print("TableColumns.get".ljust(30), cols.get("apple"))
    print("TableColumns.search_by_name".ljust(30), cols.search_by_name("int", False))
    print("TableColumns.search_by_name".ljust(30), cols.search_by_name("int", True))
    print("TableColumns.search_by_name".ljust(30), cols.search_by_name("bigint", True))
    print("TableColumns.search_by_mysql_dtype".ljust(30), cols.search_by_mysql_dtype("INT", False))
    print("TableColumns.search_by_mysql_dtype".ljust(30), cols.search_by_mysql_dtype("INT", True))
    print("TableColumns.search_by_mysql_dtype".ljust(30), cols.search_by_mysql_dtype("BIGINT", True))
    print("TableColumns.search_by_python_dtype".ljust(30), cols.search_by_python_dtype(int))
    print("TableColumns.search_by_python_dtype".ljust(30), cols.search_by_python_dtype())
    print("TableColumns.search_by_python_dtype".ljust(30), cols.search_by_python_dtype(int, float))
    print("TableColumns.search_by_python_dtype".ljust(30), cols.search_by_python_dtype(int, float, Decimal))
    print("TableColumns.primary_key".ljust(30), cols.primary_key)
    print("TableColumns.tabletime".ljust(30), cols.tabletime)
    print("TableColumns.auto_increments".ljust(30), cols.auto_increments)
    print("TableColumns.non_auto_increments".ljust(30), cols.non_auto_increments)
    print("TableColumns.syntax".ljust(30), cols.syntax)
    print("TableColumns.gen_syntax".ljust(30), cols.gen_syntax(c3, c2, c1))
    print("TableColumns.gen_syntax".ljust(30), cols.gen_syntax())
    print()
    # fmt: on


def validate_index() -> None:
    from mysqlengine.column import Column
    from mysqlengine.dtype import MysqlTypes
    from mysqlengine.index import Index, Indexes
    from mysqlengine.index import DummyIndex, TableIndexes

    # Create columns
    user_id = Column("user_id", MysqlTypes.BIGINT())
    user_name = Column("user_name", MysqlTypes.VARCHAR())
    dt = Column("dt", MysqlTypes.DATETIME(6))

    # Create index
    # fmt: off
    print(" Index ".center(100, "="))
    idx = Index(user_id, user_name, dt, unique=True)
    print("INDEX.repr".ljust(30), repr(idx))
    print("INDEX.str".ljust(30), idx)
    print("INDEX.hash".ljust(30), hash(idx))
    print("INDEX.len".ljust(30), len(idx))
    print("INDEX.getitem".ljust(30), idx["user_id"])
    print("INDEX.contains".ljust(30), "user_id" in idx)
    print("INDEX.contains".ljust(30), "apple" in idx)
    print("INDEX.name".ljust(30), idx.name, type(idx.name))
    print("INDEX.unique".ljust(30), idx.unique, type(idx.unique))
    idx.set_primary(True)
    print("INDEX.primary_unique".ljust(30), idx.primary_unique, type(idx.primary_unique))
    print("INDEX.columns".ljust(30), idx.columns, type(idx.columns))
    print("INDEX.issubset".ljust(30), x:=idx.issubset("user_id", "user_name", dt), type(x))
    print("INDEX.issubset".ljust(30), x:=idx.issubset("user_id", "user_name"), type(x))
    print("INDEX.keys".ljust(30), x:=idx.keys(), type(x))
    print("INDEX.values".ljust(30), x:=idx.values(), type(x))
    print("INDEX.items".ljust(30), x:=idx.items(), type(x))
    print("INDEX.get".ljust(30), x:=idx.get("user_id"), type(x))
    print("INDEX.get".ljust(30), x:=idx.get(user_id), type(x))
    print("INDEX.get".ljust(30), x:=idx.get("apple"), type(x))
    print("INDEX.search_by_name".ljust(30), idx.search_by_name("id", False))
    print("INDEX.search_by_name".ljust(30), idx.search_by_name("id", True))
    print("INDEX.search_by_name".ljust(30), idx.search_by_name("user_id", True))
    print("INDEX.search_by_mysql_dtype".ljust(30), idx.search_by_mysql_dtype("INT", False))
    print("INDEX.search_by_mysql_dtype".ljust(30), idx.search_by_mysql_dtype("INT", True))
    print("INDEX.search_by_mysql_dtype".ljust(30), idx.search_by_mysql_dtype("BIGINT", True))
    print("INDEX.search_by_python_dtype".ljust(30), idx.search_by_python_dtype(int))
    print("INDEX.search_by_python_dtype".ljust(30), idx.search_by_python_dtype())
    print("INDEX.search_by_python_dtype".ljust(30), idx.search_by_python_dtype(int, str))
    print()
    # fmt: on

    # Dummy Index
    print(" DummyIndex ".center(100, "="))
    didx = DummyIndex()
    print("DummyIndex.repr".ljust(30), repr(didx))
    print("DummyIndex.name".ljust(30), didx.name)
    print()

    # Indexes
    # fmt: off
    cus_id = Column("cus_id", MysqlTypes.BIGINT())
    cus_name = Column("cus_name", MysqlTypes.VARCHAR())
    dt = Column("dt", MysqlTypes.DATETIME(6))
    idx2 = Index(cus_id, cus_name, dt, unique=False)
    print(" Indexes ".center(100, "="))
    idxs = Indexes(idx, idx2)
    print("Indexes.repr".ljust(30), repr(idxs))
    print("Indexes.str".ljust(30), idxs)
    print("Indexes.hash".ljust(30), hash(idxs))
    print("Indexes.len".ljust(30), len(idxs))
    print("Indexes.bool".ljust(30), bool(idxs))
    print("Indexes.getitem".ljust(30), idxs["uixUseridUsernameDt"])
    print("Indexes.contains".ljust(30), "uixUseridUsernameDt" in idxs)
    print("Indexes.contains".ljust(30), "uixUseridUsername" in idxs)
    print("Indexes.keys".ljust(30), x := idxs.keys(), type(x))
    print("Indexes.values".ljust(30), x := idxs.values(), type(x))
    print("Indexes.items".ljust(30), x := idxs.items(), type(x))
    print("Indexes.get".ljust(30), idxs.get("uixUseridUsernameDt"))
    print("Indexes.get".ljust(30), idxs.get("apple"))
    print("Indexes.search_by_name".ljust(30), idxs.search_by_name("userid", False))
    print("Indexes.search_by_name".ljust(30), idxs.search_by_name("userid", True))
    print("Indexes.search_by_name".ljust(30), idxs.search_by_name("uixUseridUsernameDt", True))
    print("Indexes.search_by_columns".ljust(30), idxs.search_by_columns("user_id", match_all=False))
    print("Indexes.search_by_columns".ljust(30), idxs.search_by_columns("cus_id", "user_id", match_all=False))
    print("Indexes.search_by_columns".ljust(30), idxs.search_by_columns("user_id", match_all=True))
    print("Indexes.search_by_columns".ljust(30), idxs.search_by_columns("cus_id", "cus_name", "dt", match_all=True))
    print("Indexes.issubset".ljust(30), idxs.issubset("uixUseridUsernameDt", "uixCusidCusnameDt", "apple"))
    print("Indexes.issubset".ljust(30), idxs.issubset("uixUseridUsernameDt", "apple"))
    print()

    # Empty Indexes
    print(" EmptyIndexes ".center(100, "="))
    idxs = Indexes()
    print("Indexes.repr".ljust(30), repr(idxs))
    print("Indexes.str".ljust(30), idxs)
    print("Indexes.hash".ljust(30), hash(idxs))
    print("Indexes.len".ljust(30), len(idxs))
    print("Indexes.bool".ljust(30), bool(idxs))
    print("Indexes.contains".ljust(30), "uixUseridUsernameDt" in idxs)
    print("Indexes.contains".ljust(30), "uixUseridUsername" in idxs)
    print("Indexes.keys".ljust(30), x := idxs.keys(), type(x))
    print("Indexes.values".ljust(30), x := idxs.values(), type(x))
    print("Indexes.items".ljust(30), x := idxs.items(), type(x))
    print("Indexes.get".ljust(30), idxs.get("uixUseridUsernameDt"))
    print("Indexes.get".ljust(30), idxs.get("apple"))
    print("Indexes.search_by_name".ljust(30), idxs.search_by_name("userid", False))
    print("Indexes.search_by_name".ljust(30), idxs.search_by_name("userid", True))
    print("Indexes.search_by_name".ljust(30), idxs.search_by_name("uixUseridUsernameDt", True))
    print("Indexes.search_by_columns".ljust(30), idxs.search_by_columns("user_id", match_all=False))
    print("Indexes.search_by_columns".ljust(30), idxs.search_by_columns("cus_id", "user_id", match_all=False))
    print("Indexes.search_by_columns".ljust(30), idxs.search_by_columns("user_id", match_all=True))
    print("Indexes.search_by_columns".ljust(30), idxs.search_by_columns("cus_id", "cus_name", "dt", match_all=True))
    print("Indexes.issubset".ljust(30), idxs.issubset("uixUseridUsernameDt", "uixCusidCusnameDt", "apple"))
    print()

    # Table Indexes
    print(" TableIndexes ".center(100, "="))
    idxs = TableIndexes(idx, idx2)
    print("TableIndexes.repr".ljust(30), repr(idxs))
    print("TableIndexes.str".ljust(30), idxs)
    print("TableIndexes.hash".ljust(30), hash(idxs))
    print("TableIndexes.len".ljust(30), len(idxs))
    print("TableIndexes.bool".ljust(30), bool(idxs))
    print("TableIndexes.getitem".ljust(30), idxs["uixUseridUsernameDt"])
    print("TableIndexes.contains".ljust(30), "uixUseridUsernameDt" in idxs)
    print("TableIndexes.contains".ljust(30), "uixUseridUsername" in idxs)
    print("TableIndexes.keys".ljust(30), x := idxs.keys(), type(x))
    print("TableIndexes.values".ljust(30), x := idxs.values(), type(x))
    print("TableIndexes.items".ljust(30), x := idxs.items(), type(x))
    print("TableIndexes.get".ljust(30), idxs.get("uixUseridUsernameDt"))
    print("TableIndexes.get".ljust(30), idxs.get("apple"))
    print("TableIndexes.search_by_name".ljust(30), idxs.search_by_name("userid", False))
    print("TableIndexes.search_by_name".ljust(30), idxs.search_by_name("userid", True))
    print("TableIndexes.search_by_name".ljust(30), idxs.search_by_name("uixUseridUsernameDt", True))
    print("TableIndexes.search_by_columns".ljust(30), idxs.search_by_columns("user_id", match_all=False))
    print("TableIndexes.search_by_columns".ljust(30), idxs.search_by_columns("cus_id", "user_id", match_all=False))
    print("TableIndexes.search_by_columns".ljust(30), idxs.search_by_columns("user_id", match_all=True))
    print("TableIndexes.search_by_columns".ljust(30), idxs.search_by_columns("cus_id", "cus_name", "dt", match_all=True))
    print("TableIndexes.issubset".ljust(30), idxs.issubset("uixUseridUsernameDt", "uixCusidCusnameDt", "apple"))
    print("TableIndexes.issubset".ljust(30), idxs.issubset("uixUseridUsernameDt", "apple"))
    print("TableIndexes.uniques".ljust(30), idxs.uniques)
    print("TableIndexes.uniques.primery".ljust(30), idxs.uniques.primary)
    print("TableIndexes.syntax".ljust(30), idxs.syntax)
    print("TableIndexes.gen_syntax".ljust(30), idxs.gen_syntax("user_id", "user_name", "dt"))
    print("TableIndexes.gen_syntax".ljust(30), idxs.gen_syntax("user_id", "user_name"))
    print("TableIndexes.gen_syntax".ljust(30), idxs.gen_syntax())
    print()
    # fmt: on


def gen_data(rows: int, include_id: bool = True) -> list[dict]:
    # fmt: off
    if include_id:
        return [
            {
                "id": i + 1,
                "bigint_type": i + 1,
                "int_type": i + 1,
                "mediumint_type": i + 1,
                "smallint_type": random.randint(1, 65535),
                "tinyint_type": random.randint(1, 255),
                "float_type": i + 1 + 0.1,
                "double_type": i + 1 + 0.1,
                "decimal_type": i + 1 + 0.1,
                "timestamp_type": datetime.datetime(random.randint(1971, 2038), 1, 1, 1, 1, 1),
                "datetime_type": datetime.datetime(random.randint(1000, 9999), 1, 1, 1, 1, 1),
                "date_type": datetime.date(random.randint(1000, 9999), 1, 1),
                "time_type": datetime.time(random.randint(0, 23), random.randint(0, 59), random.randint(0, 59)),
                "year_type": random.randint(1901, 2155),
                "char_type": random.choice(["CHAR's 1", "CHAR's 2", "CHAR's 3", "CHAR's 4", "CHAR's 5"]),
                "varchar_type": random.choice(["VARCHAR's 1", "VARCHAR's 2", "VARCHAR's 3", "VARCHAR's 4", "VARCHAR's 5"]),
                "enum_type": random.choice(["a", "b", "c"]),
                "binary_type": random.choice([b"BINARY's 1", b"BINARY's 2", b"BINARY's 3", b"BINARY's 4", b"BINARY's 5"]),
                "varbinary_type": random.choice([b"VARBINARY's 1", b"VARBINARY's 2", b"VARBINARY's 3", b"VARBINARY's 4", b"VARBINARY's 5"]),
                "create_dt": random.choice([datetime.datetime(2023, 8, 1), datetime.datetime(2023, 8, 2), datetime.datetime(2023, 10, 1)]),
                "update_dt": random.choice([datetime.datetime(2023, 8, 1), datetime.datetime(2023, 8, 2), datetime.datetime(2023, 10, 1)]),
            }
            for i in range(rows)
        ]
    else:
        return [
            {
                "bigint_type": i + 1,
                "int_type": i + 1,
                "mediumint_type": i + 1,
                "smallint_type": random.randint(1, 65535),
                "tinyint_type": random.randint(1, 255),
                "float_type": i + 1 + 0.1,
                "double_type": i + 1 + 0.1,
                "decimal_type": i + 1 + 0.1,
                "timestamp_type": datetime.datetime(random.randint(1971, 2038), 1, 1, 1, 1, 1),
                "datetime_type": datetime.datetime(random.randint(1000, 9999), 1, 1, 1, 1, 1),
                "date_type": datetime.date(random.randint(1000, 9999), 1, 1),
                "time_type": datetime.time(random.randint(0, 23), random.randint(0, 59), random.randint(0, 59)),
                "year_type": random.randint(1901, 2155),
                "char_type": random.choice(["CHAR's 1", "CHAR's 2", "CHAR's 3", "CHAR's 4", "CHAR's 5"]),
                "varchar_type": random.choice(["VARCHAR's 1", "VARCHAR's 2", "VARCHAR's 3", "VARCHAR's 4", "VARCHAR's 5"]),
                "enum_type": random.choice(["a", "b", "c"]),
                "binary_type": random.choice([b"BINARY's 1", b"BINARY's 2", b"BINARY's 3", b"BINARY's 4", b"BINARY's 5"]),
                "varbinary_type": random.choice([b"VARBINARY's 1", b"VARBINARY's 2", b"VARBINARY's 3", b"VARBINARY's 4", b"VARBINARY's 5"]),
                "create_dt": random.choice([datetime.datetime(2023, 8, 1), datetime.datetime(2023, 8, 2), datetime.datetime(2023, 10, 1)]),
                "update_dt": random.choice([datetime.datetime(2023, 8, 1), datetime.datetime(2023, 8, 2), datetime.datetime(2023, 10, 1)]),
            }
            for i in range(rows)
        ]
    # fmt: on


async def validate_database() -> None:
    from mysqlengine.connection import DictCursor, DfCursor

    user = "root"
    password = "Password_123456"

    # Instanciate Server
    server = Server(user=user, password=password, max_size=20)
    await server.fill(10)

    # Instanciate Database
    db = MyDatabase1(server)

    # Validate server
    print(" Representation ".center(100, "="))
    print()
    print(server)
    print()

    # Validate database
    # fmt: off
    print(" Database ".center(100, "="))
    print("Database.repr:".ljust(20), repr(db))
    print("Database.name:".ljust(20), db.name)
    print("Database.tables:".ljust(20), db.tables)
    print("Database.syntax:".ljust(20), db.syntax)
    print("Database.initiated:".ljust(20), db.initiated)
    print("Database.keys".ljust(20), db.keys())
    print("Database.values".ljust(20), db.values())
    print("Database.items".ljust(20), db.items())
    print()
    print("Database.drop:".ljust(20), await db.drop())
    print("Database.exists:".ljust(20), await db.exists())
    print("Database.initiate:".ljust(20), await db.initiate())
    print("Database.exists:".ljust(20), await db.exists())
    print("Database.charset:".ljust(20), db.charset)
    print("Database.collate:".ljust(20), db.collate)
    print("Database.exists:".ljust(20), await db.exists())
    print("Database.alter_charset_collate:".ljust(20), await db.alter_charset_collate("utf8mb4", "utf8mb4_unicode_ci"))
    print("Database.information:\n", await db.information("*", cursor=DfCursor))
    print("Database.charset:".ljust(20), db.charset)
    print("Database.collate:".ljust(20), db.collate)
    print("Database.alter_charset_collate:".ljust(20), await db.alter_charset_collate("utf8mb4", "utf8mb4_0900_as_cs"))
    print("Database.information:\n", await db.information("*", cursor=DfCursor))
    print("Database.charset:".ljust(20), db.charset)
    print("Database.collate:".ljust(20), db.collate)
    print("Database.optimize:".ljust(20), await db.optimize())
    print()
    print(repr(db.tables))
    print(repr(db.tables.standards))
    print(repr(db.tables.timetables))
    print()
    # fmt: on

    # Validate table
    # fmt: off
    print(" Table ".center(100, "="))
    print(tb := db.get("my_table1"), type(tb))
    print("Table.columns:".ljust(20), tb.columns)
    print("Table.indexes:".ljust(20), tb.indexes)
    print("Table.syntax:\n", tb.syntax)
    print()
    print("Table.name:".ljust(20), tb.name)
    print("Table.fname:".ljust(20), tb.fname)
    print("Table.tname:".ljust(20), tb.tname)
    print("Table.pname:".ljust(20), tb.pname)
    print("Table.charset:".ljust(20), tb.charset)
    print("Table.collate:".ljust(20), tb.collate)
    print("Table.engine:".ljust(20), tb.engine)
    print("Table.type:".ljust(20), tb.type)
    print("Table.is_timetable:".ljust(20), tb.is_timetable)
    print("Table.regex:".ljust(20), tb.regex)
    print("Table.primary_key:".ljust(20), tb.primary_key)
    print("Table.tabletime:".ljust(20), tb.tabletime)
    print("Table.initiated:".ljust(20), tb.initiated)
    print("Table.initiated_tables:".ljust(20), tb.initiated_tables)
    print("Table.keys".ljust(20), tb.keys())
    print("Table.values".ljust(20), tb.values())
    print("Table.items".ljust(20), tb.items())
    print()
    print("Table.drop:".ljust(20), await tb.drop())
    print("Table.exists:".ljust(20), await tb.exists())
    print("Table.initiate:".ljust(20), await tb.initiate())
    print("Table.exists:".ljust(20), await tb.exists())
    print("Table.empty:".ljust(20), await tb.empty())
    print("Table.truncate:".ljust(20), await tb.truncate())
    print("Table.information:\n", await tb.information("*", cursor=DfCursor))
    print("Table.describe:\n", await tb.describe(cursor=DfCursor))
    print("Table.optimize:".ljust(20), await tb.optimize())
    print("Table.alter_charset_collate:".ljust(20), await tb.alter_charset_collate("utf8mb4", "utf8mb4_unicode_ci"))
    print("Table.information:\n", await tb.information("*", cursor=DfCursor))
    print("Table.charset:".ljust(20), tb.charset)
    print("Table.collate:".ljust(20), tb.collate)
    print("Table.alter_charset_collate:".ljust(20), await tb.alter_charset_collate("utf8mb4", "utf8mb4_0900_as_cs"))
    print("Table.information:\n", await tb.information("*", cursor=DfCursor))
    print("Table.charset:".ljust(20), tb.charset)
    print("Table.collate:".ljust(20), tb.collate)
    print("Table.show_index:\n", await tb.show_index(cursor=DfCursor))
    print("Table.reset_indexes", await tb.reset_indexes())
    print("Table.show_index:\n", await tb.show_index(cursor=DfCursor))
    print()
    # fmt: on

    # fmt: off
    # Validate query
    print(" Table Query ".center(100, "="))
    print("Fetchall from my_table1:")
    async with db.acquire() as conn:
        async with conn.cursor(cursor=DictCursor) as cur:
            await cur.execute(f"SELECT * FROM {db.my_table1};")
            print(await cur.fetchall())
    print("Table Insert into my_table1:")
    async with db.transaction() as conn:
        async with conn.cursor() as cur:
            rows = await cur.execute(
                f"""INSERT INTO {db.my_table1} (
                    bigint_type, int_type, mediumint_type, smallint_type, tinyint_type,
                    float_type, double_type, decimal_type, timestamp_type, datetime_type,
                    date_type, time_type, year_type, char_type, varchar_type,
                    enum_type, binary_type, varbinary_type
                ) VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s
                );""",
                (
                    1, 2, 3, 4, 5,
                    6.1, 7.1, 8.1, datetime.datetime(2023, 1, 1, 1, 1, 1), datetime.datetime(2024, 1, 1, 1, 1, 1),
                    datetime.date(2025, 1, 1), datetime.time(12, 12, 12), 2013, "CHAR's", "VARCHAR's",
                    "a", b"BINARY's", b"VARBINARY's"
                ),
            )
    print(rows)
    print("Table Fetchall from my_table1:")
    async with db.acquire() as conn:
        async with conn.cursor(cursor=DictCursor) as cur:
            await cur.execute(f"SELECT * FROM {db.my_table1};")
            print(await cur.fetchall())
    print("Table Conditional 1 fetch from my_table1:")
    async with db.acquire() as conn:
        async with conn.cursor(cursor=DictCursor) as cur:
            await cur.execute(f"SELECT * FROM {db.my_table1} WHERE id = %s;", 1)
            print(await cur.fetchall())
    print("Table Conditional 2 fetch from my_table1:")
    async with db.acquire() as conn:
        async with conn.cursor(cursor=DictCursor) as cur:
            await cur.execute(f"SELECT * FROM {db.my_table1} WHERE decimal_type = %s;", 8.1)
            print(await cur.fetchall())
    print("Table Conditional 3 fetch from my_table1:")
    async with db.acquire() as conn:
        async with conn.cursor(cursor=DictCursor) as cur:
            await cur.execute(f"SELECT * FROM {db.my_table1} WHERE timestamp_type = %s;", 
                              datetime.datetime(2023, 1, 1, 1, 1, 1))
            print(await cur.fetchall())
    print("Table Conditional 4 fetch from my_table1:")
    async with db.acquire() as conn:
        async with conn.cursor(cursor=DictCursor) as cur:
            await cur.execute(f"SELECT * FROM {db.my_table1} WHERE date_type = %s;", 
                              datetime.date(2025, 1, 1))
            print(await cur.fetchall())
    print("Table Conditional 5 fetch from my_table1:")
    async with db.acquire() as conn:
        async with conn.cursor(cursor=DictCursor) as cur:
            await cur.execute(f"SELECT * FROM {db.my_table1} WHERE time_type = %s;", 
                              datetime.time(12, 12, 12))
            print(await cur.fetchall())
    print("Table Conditional 6 fetch from my_table1:")
    async with db.acquire() as conn:
        async with conn.cursor(cursor=DictCursor) as cur:
            await cur.execute(f"SELECT * FROM {db.my_table1} WHERE year_type = %s;", 2013)
            print(await cur.fetchall())
    print("Table Conditional 7 fetch from my_table1:")
    async with db.acquire() as conn:
        async with conn.cursor(cursor=DictCursor) as cur:
            await cur.execute(f"SELECT * FROM {db.my_table1} WHERE char_type = %s;", "CHAR's")
            print(await cur.fetchall())
    print("Table Conditional 8 fetch from my_table1:")
    async with db.acquire() as conn:
        async with conn.cursor(cursor=DictCursor) as cur:
            await cur.execute(f"SELECT * FROM {db.my_table1} WHERE varchar_type = %s;", "VARCHAR's")
            print(await cur.fetchall())
    print("Table Conditional 9 fetch from my_table1:")
    async with db.acquire() as conn:
        async with conn.cursor(cursor=DictCursor) as cur:
            await cur.execute(f"SELECT * FROM {db.my_table1} WHERE enum_type = %s;", "a")
            print(await cur.fetchall())
    print("Table Conditional 10 fetch from my_table1:")
    async with db.acquire() as conn:
        async with conn.cursor(cursor=DictCursor) as cur:
            await cur.execute(f"SELECT * FROM {db.my_table1} WHERE binary_type = %s;", b"BINARY's\0\0")
            print(await cur.fetchall())
    print("Table Conditional 11 fetch from my_table1:")
    async with db.acquire() as conn:
        async with conn.cursor(cursor=DictCursor) as cur:
            await cur.execute(f"SELECT * FROM {db.my_table1} WHERE varbinary_type = %s;", b"VARBINARY's")
            print(await cur.fetchall())
    print()
    # fmt: on

    # Table Export & Import
    print(" Table Export & Import ".center(100, "="))
    path = "/Users/jef/Desktop/PlayGround/export_data"
    print("Table.export_data:", await tb.export_data(path))
    print("Table.import_data:", await tb.import_data(path))
    print()

    # Validate timetable
    # fmt: off
    print(" TimeTable ".center(100, "="))
    print(tb := db.tables.timetables.get("my_timetable_d"), type(tb))
    print("TimeTable.columns:".ljust(20), tb.columns)
    print("TimeTable.indexes:".ljust(20), tb.indexes)
    print("TimeTable.syntax:\n", tb.syntax)
    print()
    print()
    t1, t2, t3 = "2023-01-01", "2024-01-01", "2025-01-01"
    print("TimeTable.name:".ljust(20), tb.name)
    print("TimeTable.get_name:".ljust(20), tb.get_name(t1))
    print("TimeTable.fname:".ljust(20), tb.fname)
    print("TimeTable.get_fname:".ljust(20), tb.get_fname(t1))
    print("TimeTable.tname:".ljust(20), tb.tname)
    print("TimeTable.pname:".ljust(20), tb.pname)
    print("TimeTable.charset:".ljust(20), tb.charset)
    print("TimeTable.collate:".ljust(20), tb.collate)
    print("TimeTable.engine:".ljust(20), tb.engine)
    print("TimeTable.type:".ljust(20), tb.type)
    print("TimeTable.is_timetable:".ljust(20), tb.is_timetable)
    print("TimeTable.time_unit:".ljust(20), tb.time_unit)
    print("TimeTable.time_format:".ljust(20), tb.time_format)
    print("TimeTable.name_format:".ljust(20), tb.name_format)
    print("TimeTable.regex:".ljust(20), tb.regex)
    print("TimeTable.primary_key:".ljust(20), tb.primary_key)
    print("TimeTable.tabletime:".ljust(20), tb.tabletime)
    print("TimeTable.initiated:".ljust(20), tb.initiated)
    print("TimeTable.initiated_tables:".ljust(20), tb.initiated_tables)
    print("TimeTable.keys".ljust(20), tb.keys())
    print("TimeTable.values".ljust(20), tb.values())
    print("TimeTable.items".ljust(20), tb.items())
    print()
    print("TimeTable.drop:".ljust(20), await tb.drop(t1, t2, t3))
    print("TimeTable.exists:".ljust(20), await tb.exists(t1), await tb.exists(t2), await tb.exists(t3))
    print("TimeTable.initiate:".ljust(20), await tb.initiate(t1), await tb.initiate(t2), await tb.initiate(t3))
    print("TimeTable.exists:".ljust(20), await tb.exists(t1), await tb.exists(t2), await tb.exists(t3))
    print("TimeTable.empty:".ljust(20), await tb.empty(t1, t2, t3))
    print("TimeTable.truncate:".ljust(20), await tb.truncate(t1, t2, t3))
    print("TimeTable.information:\n", await tb.information("*", cursor=DfCursor))
    print("TimeTable.describe:\n", await tb.describe(cursor=DfCursor))
    print("TimeTable.optimize:".ljust(20), await tb.optimize())
    print("TimeTable.alter_charset_collate:".ljust(20), await tb.alter_charset_collate("utf8mb4", "utf8mb4_unicode_ci"))
    print("TimeTable.information:\n", await tb.information("*", cursor=DfCursor))
    print("TimeTable.charset:".ljust(20), tb.charset)
    print("TimeTable.collate:".ljust(20), tb.collate)
    print("TimeTable.alter_charset_collate:".ljust(20), await tb.alter_charset_collate("utf8mb4", "utf8mb4_0900_as_cs"))
    print("TimeTable.information:\n", await tb.information("*", cursor=DfCursor))
    print("TimeTable.charset:".ljust(20), tb.charset)
    print("TimeTable.collate:".ljust(20), tb.collate)
    print("TimeTable.show_index:\n", await tb.show_index(cursor=DfCursor))
    print("TimeTable.reset_indexes", await tb.reset_indexes())
    print("TimeTable.show_index:\n", await tb.show_index(cursor=DfCursor))
    print()
    # fmt: on

    # fmt: off
    # Validate query
    print(" TimeTable Query ".center(100, "="))
    print("TimeTable Insert into my_timetable_d:")
    async with db.transaction() as conn:
        tb_name = tb.get_fname(t1)
        async with conn.cursor() as cur:
            rows1 = await cur.execute(
                f"""INSERT INTO {tb_name} (
                    bigint_type, int_type, mediumint_type, smallint_type, tinyint_type,
                    float_type, double_type, decimal_type, timestamp_type, datetime_type,
                    date_type, time_type, year_type, char_type, varchar_type,
                    enum_type, binary_type, varbinary_type
                ) VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s
                );""",
                (
                    1, 2, 3, 4, 5,
                    6.1, 7.1, 8.1, datetime.datetime(2023, 1, 1, 1, 1, 1), datetime.datetime(2024, 1, 1, 1, 1, 1),
                    datetime.date(2025, 1, 1), datetime.time(12, 12, 12), 2013, "CHAR's", "VARCHAR's",
                    "a", b"BINARY's", b"VARBINARY's"
                ),
            )
        tb_name = tb.get_fname(t2)
        async with conn.cursor() as cur:
            rows2 = await cur.execute(
                f"""INSERT INTO {tb_name} (
                    bigint_type, int_type, mediumint_type, smallint_type, tinyint_type,
                    float_type, double_type, decimal_type, timestamp_type, datetime_type,
                    date_type, time_type, year_type, char_type, varchar_type,
                    enum_type, binary_type, varbinary_type
                ) VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s
                );""",
                (
                    1, 2, 3, 4, 5,
                    6.1, 7.1, 8.1, datetime.datetime(2023, 1, 1, 1, 1, 1), datetime.datetime(2024, 1, 1, 1, 1, 1),
                    datetime.date(2025, 1, 1), datetime.time(12, 12, 12), 2013, "CHAR's", "VARCHAR's",
                    "a", b"BINARY's", b"VARBINARY's"
                ),
            )
    print(rows1 + rows2)
    print()
    # fmt: on

    # TimeTable Export & Import
    print(" TimeTable Export & Import ".center(100, "="))
    path = "/Users/jef/Desktop/PlayGround/export_data"
    print("Table.export_data:", await tb.export_data(path))
    print("Table.import_data:", await tb.import_data(path))
    print()

    # Database Export & Import
    print(" Database Export & Import ".center(100, "="))
    path = "/Users/jef/Desktop/PlayGround/export_data"
    print("Database.export_data:", await db.export_data(path))
    print("Database.import_data:", await db.import_data(path))
    print()

    # Database reset index
    print(" Database Reset Index ".center(100, "="))
    print("Database.tables.reset_indexes:", await db.tables.reset_indexes())
    print()

    # Done
    await server.close()


async def validate_insert_query() -> None:
    user = "root"
    password = "Password_123456"

    # Instanciate Server
    server = Server(user=user, password=password, max_size=20)
    await server.fill(10)

    # Instanciate Database
    print()
    db = MyDatabase1(server)
    await db.drop()
    await db.initiate()
    print(" Database ".center(100, "="))
    print(repr(db))
    rows = 1000
    data_dc = gen_data(rows, True)
    data_df = pd.DataFrame(data_dc)
    data_sq = [dic.values() for dic in gen_data(rows, False)]
    t1 = time.perf_counter()
    print()

    # fmt: off
    # Normal Table Insert Values w/t temp conn
    tb = db.my_table1
    print("Normal Table.insert.values(dicts * partial) (temp conn)".center(100))
    await tb.truncate()
    await tb.insert("bigint_type").values(data_dc).execute(stats=True)

    print("Normal Table.insert.values(dfs * partial) (temp conn)".center(100))
    await tb.truncate()
    await tb.insert("bigint_type").values(data_df).execute(stats=True)

    print("Normal Table.insert.values(dicts * all) (temp conn)".center(100))
    await tb.truncate()
    await tb.insert().values(data_dc).execute(stats=True)

    print("Normal Table.insert.values(dfs * all) (temp conn)".center(100))
    await tb.truncate()
    await tb.insert().values(data_df).execute(stats=True)

    print("Normal Table.insert.values(sequences * all) (temp conn)".center(100))
    await tb.truncate()
    await tb.insert().values(data_df).execute(stats=True)

    print("Normal Table.insert.values IGNORE (temp conn)".center(100))
    await tb.insert(ignore=True).values(data_dc).execute(warnings=False, stats=True)

    print("Normal Table.insert.values.on_dup (temp conn)".center(100))
    await tb.insert().values(gen_data(rows, True), alias="val").on_duplicate_key(
            "tinyint_type = val.tinyint_type",
        ).execute(warnings=True, stats=True)
    print()

    # Normal Table Insert Values w/t spec conn
    await tb.truncate()
    async with tb.transaction() as conn:
        print("Normal Table.insert.values(dicts * all) (spec conn)".center(100))
        await tb.insert().values(data_dc).execute(conn, stats=True)

        print("Normal Table.insert.values IGNORE (spec conn)".center(100))
        await tb.insert(ignore=True).values(data_dc).execute(conn, warnings=False, stats=True)

        print("Normal Table.insert.values.on_dup (spec conn)".center(100))
        await tb.insert().values(gen_data(rows, True), alias="val").on_duplicate_key(
                "tinyint_type = val.tinyint_type",
            ).execute(conn, warnings=False, stats=True)

    # Normal Table Insert Select w/t temp conn
    print("Normal Table.insert.select * partial (temp conn)".center(100))
    await db.my_table2.truncate()
    await db.my_table2.insert("bigint_type").select(tb).execute(stats=True)

    print("Normal Table.insert.select * all (temp conn)".center(100))
    await db.my_table2.truncate()
    await db.my_table2.insert().select(tb).execute(stats=True)

    print("Normal Table.insert.select IGNORE (temp conn)".center(100))
    await db.my_table2.insert(ignore=True).select(tb).execute(warnings=False, stats=True)

    # Normal Table Insert Select w/t spec conn
    await db.my_table2.truncate()
    async with tb.transaction() as conn:
        print("Normal Table.insert.select * all (spec conn)".center(100))
        await db.my_table2.insert().select(tb).execute(conn, stats=True)

        print("Normal Table.insert.select IGNORE (spec conn)".center(100))
        await db.my_table2.insert(ignore=True).select(tb).execute(conn, warnings=False, stats=True)

    # Normal Table Replace
    print("Normal Table.replace * all (temp conn)".center(100))
    await tb.replace().values(data_dc).execute(stats=True)

    # TimeTable Insert Values w/t temp conn
    s, e = "2023-08-01", "2023-10-01"
    tb = db.my_timetable_m
    print("Normal Table.insert.values(dicts) specific tabletime (temp conn)".center(100))
    await tb.truncate(start="2000-01-01", end="2025-01-01")
    await tb.insert("bigint_type", "create_dt", tabletime="2023-08-01").values(data_dc).execute(stats=True)

    print("TimeTable.insert.values(dicts * partial) (temp conn)".center(100))
    await tb.truncate(start="2000-01-01", end="2025-01-01")
    await tb.insert("bigint_type", "create_dt").values(data_dc).execute(stats=True)

    print("TimeTable.insert.values(dfs * partial) (temp conn)".center(100))
    await tb.truncate(start="2000-01-01", end="2025-01-01")
    await tb.insert("bigint_type", "create_dt").values(data_df).execute(stats=True)

    print("TimeTable.insert.values(dicts * all) (temp conn)".center(100))
    await tb.truncate(start="2000-01-01", end="2025-01-01")
    await tb.insert().values(data_dc).execute(stats=True)

    print("TimeTable.insert.values(dfs * all) (temp conn)".center(100))
    await tb.truncate(start="2000-01-01", end="2025-01-01")
    await tb.insert().values(data_df).execute(stats=True)

    print("TimeTable.insert.values(sequences * all) (temp conn)".center(100))
    await tb.truncate(start="2000-01-01", end="2025-01-01")
    await tb.insert().values(data_df).execute(stats=True)

    print("TimeTable.insert.values IGNORE (temp conn)".center(100))
    await tb.insert(ignore=True).values(data_dc).execute(warnings=False, stats=True)

    print("TimeTable.insert.values.on_dup (temp conn)".center(100))
    await tb.insert().values(gen_data(rows, True), alias="val").on_duplicate_key(
            "tinyint_type = val.tinyint_type",
        ).execute(warnings=False, stats=True)
    print()

    # TimeTable Insert Values w/t spec conn
    await tb.truncate(start="2000-01-01", end="2025-01-01")
    async with tb.transaction() as conn:
        print("TimeTable.insert.values(dicts * all) (spec conn)".center(100))
        await tb.insert().values(data_dc).execute(conn, stats=True)

        print("TimeTable.insert.values IGNORE (spec conn)".center(100))
        await tb.insert(ignore=True).values(data_dc).execute(conn, warnings=False, stats=True)

        print("TimeTable.insert.values.on_dup (spec conn)".center(100))
        await tb.insert().values(gen_data(rows, True), alias="val").on_duplicate_key(
                "tinyint_type = val.tinyint_type",
            ).execute(conn, warnings=False, stats=True)

    # TimeTable Insert Select w/t temp conn
    print("TimeTable.insert.select * partial from Normal Table (temp conn)".center(100))
    await db.my_timetable_m2.truncate(start="2000-01-01", end="2025-01-01")
    await db.my_timetable_m2.insert("bigint_type", "create_dt", tabletime="2023-08-01").select(db.my_table1).execute(stats=True)

    print("TimeTable.insert.select * all from Normal Table (temp conn)".center(100))
    await db.my_timetable_m2.truncate(start="2000-01-01", end="2025-01-01")
    await db.my_timetable_m2.insert(tabletime="2023-08-01").select(db.my_table1).execute(stats=True)

    print("TimeTable.insert.select * all from TimeTable (time) (temp conn)".center(100))
    await db.my_timetable_m2.truncate(start="2000-01-01", end="2025-01-01")
    await db.my_timetable_m2.insert(tabletime=s).select(db.my_timetable_m, tabletime=s).execute(stats=True)

    print("TimeTable.insert.select * all from TimeTable (time mix) (temp conn)".center(100))
    await db.my_timetable_m2.truncate(start="2000-01-01", end="2025-01-01")
    await db.my_timetable_m2.insert(ignore=True, tabletime=s).select(db.my_timetable_m).tabletimes(start=s, end=e).execute(warnings=False, stats=True)

    print("TimeTable.insert.select * all from TimeTable (times) (temp conn)".center(100))
    await db.my_timetable_m2.truncate(start="2000-01-01", end="2025-01-01")
    await db.my_timetable_m2.insert().select(db.my_timetable_m).tabletimes(start=s, end=e).execute(stats=True)

    # TimeTable Insert Select w/t spec conn
    await db.my_timetable_m2.truncate(start="2000-01-01", end="2025-01-01")
    async with tb.transaction() as conn:
        print("TimeTable.insert.select * all from TimeTable (times) (spec conn)".center(100))
        await db.my_timetable_m2.insert().select(db.my_timetable_m).tabletimes(start=s, end=e).execute(conn, stats=True)

    # TimeTable Replace
    print("TimeTable.replace * all (temp conn)".center(100))
    await tb.replace().values(data_dc).execute(stats=True)

    # TimeTable time unit
    print("TimeTable 'year'".center(100))
    await db.my_timetable_y.insert().values(data_dc).execute(stats=True)

    print("TimeTable 'week'".center(100))
    await db.my_timetable_w.insert().values(data_dc).execute(stats=True)

    print("TimeTable 'day'".center(100))
    await db.my_timetable_d.insert().values(data_dc).execute(stats=True)

    # fmt: on
    # Done
    t2 = time.perf_counter()
    print("Total time:", t2 - t1)
    await server.close()


async def validate_select_query() -> None:
    from mysqlengine.connection import Cursor, DictCursor, DfCursor
    from mysqlengine.connection import SSCursor, SSDictCursor, SSDfCursor

    user = "root"
    password = "Password_123456"

    # Instanciate Server
    server = Server(user=user, password=password, max_size=20)
    await server.fill(10)

    # Instanciate Database
    print()
    db = MyDatabase1(server)
    print(" Database ".center(100, "="))
    print(repr(db))
    t1 = time.perf_counter()
    print()

    # fmt: off
    # Normal Table Select
    s, e = "2023-08-01", "2023-10-01"
    tb = db.my_table1
    print("Normal Table.select(* all)".center(100))
    print(await tb.select().execute(cursor=SSDfCursor, stats=True), "\n")

    print("Normal Table.select(column)".center(100))
    print(await tb.select("tinyint_type").execute(cursor=SSDfCursor, stats=True), "\n")

    print("Normal Table.select(column distinct)".center(100))
    print(await tb.select("tinyint_type", distinct=True)
        .execute(cursor=SSDfCursor, stats=True), "\n")

    print("Normal Table.select(column distinct buffer)".center(100))
    print(await tb.select("tinyint_type", distinct=True, buffer_result=True)
        .execute(cursor=SSDfCursor, stats=True), "\n")

    print("Normal Table.select(column distinct buffer explain)".center(100))
    print(await tb.select("tinyint_type", distinct=True, buffer_result=True, explain=True)
        .execute(cursor=SSDfCursor, stats=True), "\n")

    print("Normal Table.select(column) from subquery(normal Table)".center(100))
    print(await tb.select("tinyint_type", from_=db.my_table2.select("tinyint_type"))
        .execute(cursor=SSDfCursor, stats=True), "\n")

    print("Normal Table.select(column) from subquery(TimeTable [time])".center(100))
    print(await tb.select("tinyint_type", from_=db.my_timetable_m.select("tinyint_type", tabletime=s))
        .execute(cursor=SSDfCursor, stats=True), "\n")

    print("Normal Table.select(column) from subquery(TimeTable [times])".center(100))
    print(await tb.select("tinyint_type", from_=db.my_timetable_m.select("tinyint_type")
        .tabletimes(start=s, end=e)).execute(cursor=SSDfCursor, stats=True), "\n")
    print()

    # Normal Table Select & Join
    print("Normal Table.select.join(normal Table INNER)".center(100))
    print(await tb.select("t1.tinyint_type")
        .join(db.my_table2, "t1.bigint_type = t2.bigint_type")
        .execute(cursor=SSDfCursor, stats=True), "\n")

    print("Normal Table.select.join(normal Table LEFT)".center(100))
    print(await tb.select("t1.tinyint_type")
        .join(db.my_table2, "t1.bigint_type = t2.tinyint_type", method="LEFT")
        .execute(cursor=SSDfCursor, stats=True), "\n")

    print("Normal Table.select.join(normal Table RIGHT)".center(100))
    print(await tb.select("t1.tinyint_type")
        .join(db.my_table2, "t1.tinyint_type = t2.bigint_type", method="RIGHT")
        .execute(cursor=SSDfCursor, stats=True), "\n")

    print("Normal Table.select.join(normal Table complex)".center(100))
    print(await tb.select("t1.id", "t1.tinyint_type", "t2.smallint_type", "t2.varchar_type", "t2.varbinary_type")
        .join(db.my_table2,
            "t1.bigint_type = t2.bigint_type", 
            "t2.varchar_type = %s", "t2.varbinary_type = %s", args=["VARCHAR's 5", b"VARBINARY's 3"],
            ins={"t2.tinyint_type": [i for i in range(1, 256)]},
            not_ins={"t2.tinyint_type": [i + 255 for i in range(1, 256)]},
            subqueries={
                "t2.int_type IN": db.my_timetable_y.select("int_type", tabletime="2023-10-01"),
                "t2.bigint_type IN": db.my_timetable_y.select("bigint_type", tabletime="2023-10-01"),})
        .join_time("create_dt", start="2020-01-01", end="2024-01-01", unit="Y")
        .join_time("update_dt", start="2020-01-01", end="2024-01-01", unit="M")
        .execute(cursor=SSDfCursor, stats=True), "\n")
    
    print("Normal Table.select.join(TimeTable [time])".center(100))
    print(await tb.select("t1.tinyint_type")
        .join(db.my_timetable_y, "t1.bigint_type = t2.bigint_type", tabletime=s)
        .execute(cursor=SSDfCursor, stats=True), "\n")

    print("Normal Table.select.join(TimeTable [times])".center(100))
    print(await tb.select("t1.tinyint_type")
        .join(db.my_timetable_m, "t1.bigint_type = t2.bigint_type")
        .tabletimes(start=s, end=e).execute(cursor=SSDfCursor, stats=True), "\n")

    print("Normal Table.select.join(TimeTable [times & spec conn])".center(100))
    async with tb.acquire() as conn:
        print(await tb.select("t1.tinyint_type")
            .join(db.my_timetable_m, "t1.bigint_type = t2.bigint_type")
            .tabletimes(start=s, end=e).execute(conn, cursor=SSDfCursor, stats=True), "\n")

    # Norm Table Select & Join & Where
    print("Normal Table.select.join.where".center(100))
    print(await tb.select("t1.id", "t2.varchar_type", "t2.varbinary_type")
        .join(db.my_table2, "t1.bigint_type = t2.bigint_type")
        .where(
            "t1.varchar_type = %s", "t1.varbinary_type = %s", args=["VARCHAR's 5", b"VARBINARY's 3"],
            ins={"t1.tinyint_type": [i for i in range(1, 256)]},
            not_ins={"t1.tinyint_type": [i + 255 for i in range(1, 256)]},
            subqueries={
                "t1.int_type IN": db.my_timetable_y.select("int_type", tabletime="2023-10-01"),
                "t1.bigint_type IN": db.my_timetable_y.select("bigint_type", tabletime="2023-10-01"),}
            )
        .where_time("create_dt", start="2020-01-01", end="2024-01-01", unit="Y")
        .where_time("update_dt", start="2020-01-01", end="2024-01-01", unit="M")
        .execute(cursor=SSDfCursor, stats=True), "\n")

    # Normal Table Select & Join & Where & Group
    print("Normal Table.select.join.where.group".center(100))
    print(await tb.select("t1.id", "t2.varchar_type", "t2.varbinary_type")
        .join(db.my_table2, "t1.bigint_type = t2.bigint_type")
        .where(ins={"t1.tinyint_type": [i for i in range(1, 256)]},)
        .group_by("t2.varchar_type", "t2.varbinary_type")
        .execute(cursor=SSDfCursor, stats=True), "\n")

    # Normal Table Select & Join & Where & Group & Having
    print("Normal Table.select.join.where.group.having".center(100))
    print(await tb.select("t1.id", "t2.varchar_type", "t2.varbinary_type", "t1.create_dt", "t1.update_dt")
        .join(db.my_table2, "t1.bigint_type = t2.bigint_type")
        .where(ins={"t1.tinyint_type": [i for i in range(1, 256)]},)
        .group_by("t2.varchar_type", "t2.varbinary_type")
        .having("varbinary_type = %s", args=b"VARBINARY's 1")
        .having_time("create_dt", start="2020-01-01", end="2024-01-01", unit="Y")
        .having_time("update_dt", start="2020-01-01", end="2024-01-01", unit="M")
        .execute(cursor=SSDfCursor, stats=True), "\n")

    # Normal Table Select & Join & Where & Order & Limit
    print("Normal Table.select.join.where.order".center(100))
    print(await tb.select("t1.id", "t2.varchar_type", "t2.varbinary_type", "t1.create_dt", "t1.update_dt")
        .join(db.my_table2, "t1.bigint_type = t2.bigint_type")
        .where(ins={"t1.tinyint_type": [i for i in range(1, 256)]},)
        .order_by("create_dt DESC", "update_dt DESC")
        .limit(10, 10)
        .execute(cursor=SSDfCursor, stats=True), "\n")

    # Normal Table Select & Where & Update
    print("Normal Table.select.join.where.update".center(100))
    print(await tb.select("t1.id", "t2.varchar_type", "t2.varbinary_type")
        .join(db.my_table2, "t1.bigint_type = t2.bigint_type")
        .where(ins={"t1.tinyint_type": [i for i in range(1, 256)]},)
        .for_update()
        .execute(cursor=SSDfCursor, stats=True), "\n")

    # Normal Table Select & Where & Lock
    print("Normal Table.select.join.where.lock".center(100))
    print(await tb.select("t1.id", "t2.varchar_type", "t2.varbinary_type")
        .join(db.my_table2, "t1.bigint_type = t2.bigint_type")
        .where(ins={"t1.tinyint_type": [i for i in range(1, 256)]},)
        .lock_in_share_mode()
        .execute(cursor=SSDfCursor, stats=True), "\n")

    # TimeTable Select
    print("TimeTable.select".center(100))
    tb = db.my_timetable_m
    print("TimeTable.select(* all) [time]".center(100))
    print(await tb.select(tabletime=s).execute(cursor=SSDfCursor, stats=True), "\n")

    print("TimeTable.select(* all) [times]".center(100))
    print(await tb.select().tabletimes(start=s, end=e)
        .execute(cursor=SSDfCursor, stats=True), "\n")

    print("TimeTable.select(* all) [times] spec conn".center(100))
    async with tb.acquire() as conn:
        print(await tb.select().tabletimes(start=s, end=e)
            .execute(conn, cursor=SSDfCursor, stats=True), "\n")

    print("TimeTable.select(column) [time]".center(100))
    print(await tb.select("tinyint_type", tabletime=s)
        .execute(cursor=SSDfCursor, stats=True), "\n")

    print("TimeTable.select(column) [times]".center(100))
    print(await tb.select("tinyint_type").tabletimes(start=s, end=e)
        .execute(cursor=SSDfCursor, stats=True), "\n")

    print("TimeTable.select(column) [times] spec conn".center(100))
    async with tb.acquire() as conn:
        print(await tb.select("tinyint_type").tabletimes(start=s, end=e)
            .execute(conn, cursor=SSDfCursor, stats=True), "\n")

    print("TimeTable.select(column distict) [time]".center(100))
    print(await tb.select("tinyint_type", distinct=True, tabletime=s)
        .execute(cursor=SSDfCursor, stats=True), "\n")

    print("TimeTable.select(column distict) [times]".center(100))
    print(await tb.select("tinyint_type", distinct=True).tabletimes(start=s, end=e)
        .execute(cursor=SSDfCursor, stats=True), "\n")

    print("TimeTable.select(column distict) [times] spec conn".center(100))
    async with tb.acquire() as conn:
        print(await tb.select("tinyint_type", distinct=True).tabletimes(start=s, end=e)
            .execute(conn, cursor=SSDfCursor, stats=True), "\n")

    print("TimeTable.select(column distict buffer) [time]".center(100))
    print(await tb.select("tinyint_type", distinct=True, buffer_result=True, tabletime=s)
        .execute(cursor=SSDfCursor, stats=True), "\n")

    print("TimeTable.select(column distict buffer) [times]".center(100))
    print(await tb.select("tinyint_type", distinct=True, buffer_result=True)
        .tabletimes(start=s, end=e).execute(cursor=SSDfCursor, stats=True), "\n")

    print("TimeTable.select(column distict buffer) [times] spec conn".center(100))
    async with tb.acquire() as conn:
        print(await tb.select("tinyint_type", distinct=True, buffer_result=True)
            .tabletimes(start=s, end=e).execute(conn, cursor=SSDfCursor, stats=True), "\n")

    print("TimeTable.select(column distict buffer explain) [time]".center(100))
    print(await tb.select("tinyint_type", distinct=True, buffer_result=True, explain=True, tabletime=s)
        .execute(cursor=SSDfCursor, stats=True), "\n")

    print("TimeTable.select(column distict buffer expain) [times]".center(100))
    print(await tb.select("tinyint_type", distinct=True, buffer_result=True, explain=True)
        .tabletimes(start=s, end=e).execute(cursor=SSDfCursor, stats=True), "\n")
    
    print("TimeTable.select(column distict buffer expain) [times] spec conn".center(100))
    async with tb.acquire() as conn:
        print(await tb.select("tinyint_type", distinct=True, buffer_result=True, explain=True)
            .tabletimes(start=s, end=e).execute(conn, cursor=SSDfCursor, stats=True), "\n")

    print("TimeTable.select(column) from subquery(normal Table) [time]".center(100))
    print(await tb.select("tinyint_type", from_=db.my_table2.select("tinyint_type"))
        .execute(cursor=SSDfCursor, stats=True), "\n")

    print("TimeTable.select(column) from subquery(TimeTable [time])".center(100))
    print(await tb.select("tinyint_type", from_=db.my_timetable_m.select("tinyint_type", tabletime=s))
        .execute(cursor=SSDfCursor, stats=True), "\n")

    print("TimeTable.select(column) from subquery(TimeTable [times])".center(100))
    print(await tb.select("tinyint_type", from_=db.my_timetable_m.select("tinyint_type")
        .tabletimes(start=s, end=e)).execute(cursor=SSDfCursor, stats=True), "\n")
    
    print("TimeTable.select(column) from subquery(TimeTable [times]) spec conn".center(100))
    async with tb.acquire() as conn:
        print(await tb.select("tinyint_type", from_=db.my_timetable_m.select("tinyint_type")
            .tabletimes(start=s, end=e)).execute(conn, cursor=SSDfCursor, stats=True), "\n")

    # TimeTable Select & Join
    print("TimeTable.select.join(normal Table INNER) [time]".center(100))
    print(await tb.select("t1.tinyint_type", tabletime=s)
        .join(db.my_table2, "t1.bigint_type = t2.bigint_type")
        .execute(cursor=SSDfCursor, stats=True), "\n")

    print("TimeTable.select.join(normal Table INNER) [times]".center(100))
    print(await tb.select("t1.tinyint_type")
        .join(db.my_table2, "t1.bigint_type = t2.bigint_type")
        .tabletimes(start=s, end=e).execute(cursor=SSDfCursor, stats=True), "\n")
    
    print("TimeTable.select.join(normal Table INNER) [times] spec conn".center(100))
    async with tb.acquire() as conn:
        print(await tb.select("t1.tinyint_type")
            .join(db.my_table2, "t1.bigint_type = t2.bigint_type")
            .tabletimes(start=s, end=e).execute(conn, cursor=SSDfCursor, stats=True), "\n")

    print("TimeTable.select.join(normal Table LEFT)".center(100))
    print(await tb.select("t1.tinyint_type", tabletime=s)
        .join(db.my_table2, "t1.bigint_type = t2.tinyint_type", method="LEFT")
        .execute(cursor=SSDfCursor, stats=True), "\n")

    print("TimeTable.select.join(normal Table LEFT) [times]".center(100))
    print(await tb.select("t1.tinyint_type")
        .join(db.my_table2, "t1.bigint_type = t2.bigint_type", method="LEFT")
        .tabletimes(start=s, end=e).execute(cursor=SSDfCursor, stats=True), "\n")
    
    print("TimeTable.select.join(normal Table LEFT) [times] spec conn".center(100))
    async with tb.acquire() as conn:
        print(await tb.select("t1.tinyint_type")
            .join(db.my_table2, "t1.bigint_type = t2.bigint_type", method="LEFT")
            .tabletimes(start=s, end=e).execute(conn, cursor=SSDfCursor, stats=True), "\n")

    print("TimeTable.select.join(normal Table RIGHT)".center(100))
    print(await tb.select("t1.tinyint_type", tabletime=s)
        .join(db.my_table2, "t1.bigint_type = t2.tinyint_type", method="RIGHT")
        .execute(cursor=SSDfCursor, stats=True), "\n")

    print("TimeTable.select.join(normal Table RIGHT) [times]".center(100))
    print(await tb.select("t1.tinyint_type")
        .join(db.my_table2, "t1.bigint_type = t2.bigint_type", method="RIGHT")
        .tabletimes(start=s, end=e).execute(cursor=SSDfCursor, stats=True), "\n")
    
    print("TimeTable.select.join(normal Table RIGHT) [times] spec conn".center(100))
    async with tb.acquire() as conn:
        print(await tb.select("t1.tinyint_type")
            .join(db.my_table2, "t1.bigint_type = t2.bigint_type", method="RIGHT")
            .tabletimes(start=s, end=e).execute(conn, cursor=SSDfCursor, stats=True), "\n")

    print("TimeTable.select.join(TimeTable INNER) [time]".center(100))
    print(await tb.select("t1.tinyint_type", tabletime=s)
        .join(db.my_timetable_m2, "t1.bigint_type = t2.bigint_type", tabletime=s)
        .execute(cursor=SSDfCursor, stats=True), "\n")

    print("TimeTable.select.join(TimeTable LEFT) [time]".center(100))
    print(await tb.select("t1.tinyint_type", tabletime=s)
        .join(db.my_timetable_m2, "t1.bigint_type = t2.bigint_type", method="LEFT", tabletime=s)
        .execute(cursor=SSDfCursor, stats=True), "\n")

    print("TimeTable.select.join(TimeTable RIGHT) [time]".center(100))
    print(await tb.select("t1.tinyint_type", tabletime=s)
        .join(db.my_timetable_m2, "t1.bigint_type = t2.bigint_type", method="RIGHT", tabletime=s)
        .execute(cursor=SSDfCursor, stats=True), "\n")

    print("TimeTable.select.join(TimeTable) [times]".center(100))
    print(await tb.select("t1.tinyint_type")
        .join(db.my_timetable_m2, "t1.bigint_type = t2.bigint_type")
        .tabletimes(start=s, end=e)
        .execute(cursor=SSDfCursor, stats=True), "\n")
    
    print("TimeTable.select.join(TimeTable) [times] spec conn".center(100))
    async with tb.acquire() as conn:
        print(await tb.select("t1.tinyint_type")
            .join(db.my_timetable_m2, "t1.bigint_type = t2.bigint_type")
            .tabletimes(start=s, end=e)
            .execute(conn, cursor=SSDfCursor, stats=True), "\n")

    print("TimeTable.select.join(normal Table complex) [time]".center(100))
    print(await tb.select("t1.tinyint_type", "t2.int_type", tabletime=s)
        .join(db.my_table1,
            "t1.bigint_type = t2.bigint_type",
            "t2.varchar_type = %s", "t2.varbinary_type = %s", args=["VARCHAR's 5", b"VARBINARY's 3"],
            ins={"t2.tinyint_type": [i for i in range(1, 256)]},
            not_ins={"t2.tinyint_type": [i + 255 for i in range(1, 256)]},
            subqueries={
                "t2.int_type IN": db.my_timetable_y.select("int_type", tabletime="2023-10-01"),
                "t2.bigint_type IN": db.my_timetable_y.select("bigint_type", tabletime="2023-10-01"),})
        .join_time("create_dt", start="2020-01-01", end="2024-01-01", unit="Y")
        .join_time("update_dt", start="2020-01-01", end="2024-01-01", unit="M")
        .execute(cursor=SSDfCursor, stats=True), "\n")

    print("TimeTable.select.join(TimeTable complex) [time]".center(100))
    print(await tb.select("t1.tinyint_type", "t2.int_type", tabletime=s)
        .join(db.my_timetable_m2,
            "t1.bigint_type = t2.bigint_type",
            "t2.varchar_type = %s", "t2.varbinary_type = %s", args=["VARCHAR's 5", b"VARBINARY's 3"],
            ins={"t2.tinyint_type": [i for i in range(1, 256)]},
            not_ins={"t2.tinyint_type": [i + 255 for i in range(1, 256)]},
            subqueries={
                "t2.int_type IN": db.my_timetable_y.select("int_type", tabletime="2023-10-01"),
                "t2.bigint_type IN": db.my_timetable_y.select("bigint_type", tabletime="2023-10-01"),},
            tabletime=s)
        .join_time("create_dt", start="2020-01-01", end="2024-01-01", unit="Y")
        .join_time("update_dt", start="2020-01-01", end="2024-01-01", unit="M")
        .execute(cursor=SSDfCursor, stats=True), "\n")

    print("TimeTable.select.join(TimeTable complex) [times]".center(100))
    print(await tb.select("t1.tinyint_type", "t2.int_type")
        .join(db.my_timetable_m2,
            "t1.bigint_type = t2.bigint_type",
            "t2.varchar_type = %s", "t2.varbinary_type = %s", args=["VARCHAR's 5", b"VARBINARY's 3"],
            ins={"t2.tinyint_type": [i for i in range(1, 256)]},
            not_ins={"t2.tinyint_type": [i + 255 for i in range(1, 256)]},
            subqueries={
                "t2.int_type IN": db.my_timetable_y.select("int_type", tabletime="2023-10-01"),
                "t2.bigint_type IN": db.my_timetable_y.select("bigint_type", tabletime="2023-10-01"),})
        .join_time("create_dt", start="2020-01-01", end="2024-01-01", unit="Y")
        .join_time("update_dt", start="2020-01-01", end="2024-01-01", unit="M")
        .tabletimes(start=s, end=e)
        .execute(cursor=SSDfCursor, stats=True), "\n")
    
    print("TimeTable.select.join(TimeTable complex) [times] spec conn".center(100))
    async with tb.acquire() as conn:
        print(await tb.select("t1.tinyint_type", "t2.int_type")
            .join(db.my_timetable_m2,
                "t1.bigint_type = t2.bigint_type",
                "t2.varchar_type = %s", "t2.varbinary_type = %s", args=["VARCHAR's 5", b"VARBINARY's 3"],
                ins={"t2.tinyint_type": [i for i in range(1, 256)]},
                not_ins={"t2.tinyint_type": [i + 255 for i in range(1, 256)]},
                subqueries={
                    "t2.int_type IN": db.my_timetable_y.select("int_type", tabletime="2023-10-01"),
                    "t2.bigint_type IN": db.my_timetable_y.select("bigint_type", tabletime="2023-10-01"),})
            .join_time("create_dt", start="2020-01-01", end="2024-01-01", unit="Y")
            .join_time("update_dt", start="2020-01-01", end="2024-01-01", unit="M")
            .tabletimes(start=s, end=e)
            .execute(conn, cursor=SSDfCursor, stats=True), "\n")

    # TimeTable Select & Join & Where
    print("TimeTable.select.join.where [time]".center(100))
    print(await tb.select("t1.id", "t1.tinyint_type", "t2.smallint_type", "t2.varchar_type", "t2.varbinary_type", tabletime=s)
        .join(db.my_timetable_m2, "t1.bigint_type = t2.bigint_type", tabletime=s)
        .where(
            "t1.varchar_type = %s", "t1.varbinary_type = %s", args=["VARCHAR's 5", b"VARBINARY's 3"],
            ins={"t1.tinyint_type": [i for i in range(1, 256)]},
            not_ins={"t1.tinyint_type": [i + 255 for i in range(1, 256)]},
            subqueries={
                "t1.int_type IN": db.my_timetable_y.select("int_type", tabletime="2023-10-01"),
                "t1.bigint_type IN": db.my_timetable_y.select("bigint_type", tabletime="2023-10-01"),}
            )
        .where_time("create_dt", start="2020-01-01", end="2024-01-01", unit="Y")
        .where_time("update_dt", start="2020-01-01", end="2024-01-01", unit="M")
        .execute(cursor=SSDfCursor, stats=True), "\n")

    print("TimeTable.select.join.where [times]".center(100))
    print(await tb.select("t1.id", "t1.tinyint_type", "t2.smallint_type", "t2.varchar_type", "t2.varbinary_type")
        .join(db.my_table2, "t1.bigint_type = t2.bigint_type")
        .where(
            "t1.varchar_type = %s", "t1.varbinary_type = %s", args=["VARCHAR's 5", b"VARBINARY's 3"],
            ins={"t1.tinyint_type": [i for i in range(1, 256)]},
            not_ins={"t1.tinyint_type": [i + 255 for i in range(1, 256)]},
            subqueries={
                "t1.int_type IN": db.my_timetable_y.select("int_type", tabletime="2023-10-01"),
                "t1.bigint_type IN": db.my_timetable_y.select("bigint_type", tabletime="2023-10-01"),}
            )
        .where_time("create_dt", start="2020-01-01", end="2024-01-01", unit="Y")
        .where_time("update_dt", start="2020-01-01", end="2024-01-01", unit="M")
        .tabletimes(start=s, end=e)
        .execute(cursor=SSDfCursor, stats=True), "\n")
    
    print("TimeTable.select.join.where [times] spec conn".center(100))
    async with tb.acquire() as conn:
        print(await tb.select("t1.id", "t1.tinyint_type", "t2.smallint_type", "t2.varchar_type", "t2.varbinary_type")
            .join(db.my_table2, "t1.bigint_type = t2.bigint_type")
            .where(
                "t1.varchar_type = %s", "t1.varbinary_type = %s", args=["VARCHAR's 5", b"VARBINARY's 3"],
                ins={"t1.tinyint_type": [i for i in range(1, 256)]},
                not_ins={"t1.tinyint_type": [i + 255 for i in range(1, 256)]},
                subqueries={
                    "t1.int_type IN": db.my_timetable_y.select("int_type", tabletime="2023-10-01"),
                    "t1.bigint_type IN": db.my_timetable_y.select("bigint_type", tabletime="2023-10-01"),}
                )
            .where_time("create_dt", start="2020-01-01", end="2024-01-01", unit="Y")
            .where_time("update_dt", start="2020-01-01", end="2024-01-01", unit="M")
            .tabletimes(start=s, end=e)
            .execute(conn, cursor=SSDfCursor, stats=True), "\n")

    # TimeTable Select & Join & Where & Group
    print("TimeTable.select.join.where.group [time]".center(100))
    print(await tb.select("t1.id", "t1.tinyint_type", "t2.smallint_type", "t2.varchar_type", "t2.varbinary_type", tabletime=s)
        .join(db.my_timetable_m2, "t1.bigint_type = t2.bigint_type", tabletime=s)
        .where(ins={"t1.tinyint_type": [i for i in range(1, 256)]})
        .group_by("t2.varchar_type", "t2.varbinary_type")
        .execute(cursor=SSDfCursor, stats=True), "\n")

    print("TimeTable.select.join.where.group [times] sub-table level".center(100))
    print(await tb.select("t1.id", "t1.tinyint_type", "t2.smallint_type", "t2.varchar_type", "t2.varbinary_type")
        .join(db.my_timetable_m2, "t1.bigint_type = t2.bigint_type")
        .where(ins={"t1.tinyint_type": [i for i in range(1, 256)]})
        .group_by("t2.varchar_type", "t2.varbinary_type")
        .tabletimes(start=s, end=e)
        .execute(cursor=SSDfCursor, stats=True), "\n")
    
    print("TimeTable.select.join.where.group [times] sub-table level spec conn".center(100))
    async with tb.acquire() as conn:
        print(await tb.select("t1.id", "t1.tinyint_type", "t2.smallint_type", "t2.varchar_type", "t2.varbinary_type")
            .join(db.my_timetable_m2, "t1.bigint_type = t2.bigint_type")
            .where(ins={"t1.tinyint_type": [i for i in range(1, 256)]})
            .group_by("t2.varchar_type", "t2.varbinary_type")
            .tabletimes(start=s, end=e)
            .execute(conn, cursor=SSDfCursor, stats=True), "\n")

    print("TimeTable.select.join.where.group [times] entire table".center(100))
    print(await tb.select("id", "tinyint_type", "smallint_type", "varchar_type", "varbinary_type", "COUNT(*) AS count",
            from_=tb.select("t1.id", "t1.tinyint_type", "t2.smallint_type", "t2.varchar_type", "t2.varbinary_type")
            .join(db.my_timetable_m2, "t1.bigint_type = t2.bigint_type")
            .where(ins={"t1.tinyint_type": [i for i in range(1, 256)]})
            .tabletimes(start=s, end=e))
        .group_by("varchar_type", "varbinary_type")
        .execute(cursor=SSDfCursor, stats=True), "\n")
    
    print("TimeTable.select.join.where.group [times] entire table spec conn".center(100))
    async with tb.acquire() as conn:
        print(await tb.select("id", "tinyint_type", "smallint_type", "varchar_type", "varbinary_type", "COUNT(*) AS count",
                from_=tb.select("t1.id", "t1.tinyint_type", "t2.smallint_type", "t2.varchar_type", "t2.varbinary_type")
                .join(db.my_timetable_m2, "t1.bigint_type = t2.bigint_type")
                .where(ins={"t1.tinyint_type": [i for i in range(1, 256)]})
                .tabletimes(start=s, end=e))
            .group_by("varchar_type", "varbinary_type")
            .execute(conn, cursor=SSDfCursor, stats=True), "\n")

    # TimeTable Select & Join & Where & Group & Having
    print("TimeTable.select.join.where.group.having [time]".center(100))
    print(await tb.select("t1.id", "t2.varchar_type", "t2.varbinary_type", "t1.create_dt", "t1.update_dt", tabletime=s)
        .join(db.my_timetable_m2, "t1.bigint_type = t2.bigint_type", tabletime=s)
        .where(ins={"t1.tinyint_type": [i for i in range(1, 256)]})
        .group_by("t2.varchar_type", "t2.varbinary_type")
        .having("varbinary_type = %s", args=b"VARBINARY's 1")
        .having_time("create_dt", start="2020-01-01", end="2024-01-01", unit="Y")
        .having_time("update_dt", start="2020-01-01", end="2024-01-01", unit="M")
        .execute(cursor=SSDfCursor, stats=True), "\n")

    print("TimeTable.select.join.where.group.having [times] sub-table level".center(100))
    print(await tb.select("t1.id", "t2.varchar_type", "t2.varbinary_type", "t1.create_dt", "t1.update_dt")
        .join(db.my_timetable_m2, "t1.bigint_type = t2.bigint_type")
        .where(ins={"t1.tinyint_type": [i for i in range(1, 256)]})
        .group_by("t2.varchar_type", "t2.varbinary_type")
        .having("varbinary_type = %s", args=b"VARBINARY's 1")
        .having_time("create_dt", start="2020-01-01", end="2024-01-01", unit="Y")
        .having_time("update_dt", start="2020-01-01", end="2024-01-01", unit="M")
        .tabletimes(start=s, end=e)
        .execute(cursor=SSDfCursor, stats=True), "\n")
    
    print("TimeTable.select.join.where.group.having [times] sub-table level spec conn".center(100))
    async with tb.acquire() as conn:
        print(await tb.select("t1.id", "t2.varchar_type", "t2.varbinary_type", "t1.create_dt", "t1.update_dt")
            .join(db.my_timetable_m2, "t1.bigint_type = t2.bigint_type")
            .where(ins={"t1.tinyint_type": [i for i in range(1, 256)]})
            .group_by("t2.varchar_type", "t2.varbinary_type")
            .having("varbinary_type = %s", args=b"VARBINARY's 1")
            .having_time("create_dt", start="2020-01-01", end="2024-01-01", unit="Y")
            .having_time("update_dt", start="2020-01-01", end="2024-01-01", unit="M")
            .tabletimes(start=s, end=e)
            .execute(conn, cursor=SSDfCursor, stats=True), "\n")

    print("TimeTable.select.join.where.group.having [times] entire table".center(100))
    print(await tb.select("id", "varchar_type", "varbinary_type", "create_dt", "update_dt", "COUNT(*) AS count",
            from_=tb.select("t1.id", "t2.varchar_type", "t2.varbinary_type", "t1.create_dt", "t1.update_dt")
            .join(db.my_timetable_m2, "t1.bigint_type = t2.bigint_type")
            .where(ins={"t1.tinyint_type": [i for i in range(1, 256)]})
            .tabletimes(start=s, end=e))
        .group_by("varchar_type", "varbinary_type")
        .having("varbinary_type = %s", args=b"VARBINARY's 1")
        .having_time("create_dt", start="2020-01-01", end="2024-01-01", unit="Y")
        .having_time("update_dt", start="2020-01-01", end="2024-01-01", unit="M")
        .execute(cursor=SSDfCursor, stats=True), "\n")

    print("TimeTable.select.join.where.group.having [times] entire table spec conn".center(100))
    async with tb.acquire() as conn:
        print(await tb.select("id", "varchar_type", "varbinary_type", "create_dt", "update_dt", "COUNT(*) AS count",
                from_=tb.select("t1.id", "t2.varchar_type", "t2.varbinary_type", "t1.create_dt", "t1.update_dt")
                .join(db.my_timetable_m2, "t1.bigint_type = t2.bigint_type")
                .where(ins={"t1.tinyint_type": [i for i in range(1, 256)]})
                .tabletimes(start=s, end=e))
            .group_by("varchar_type", "varbinary_type")
            .having("varbinary_type = %s", args=b"VARBINARY's 1")
            .having_time("create_dt", start="2020-01-01", end="2024-01-01", unit="Y")
            .having_time("update_dt", start="2020-01-01", end="2024-01-01", unit="M")
            .execute(conn, cursor=SSDfCursor, stats=True), "\n")

    # TimeTable Select & Join & Where & Order & Limit
    print("TimeTable.select.join.where.order.limit [time]".center(100))
    print(await tb.select("t1.id", "t2.varchar_type", "t2.varbinary_type", "t1.create_dt", "t1.update_dt", tabletime=s)
        .join(db.my_timetable_m2, "t1.bigint_type = t2.bigint_type", tabletime=s)
        .where(ins={"t1.tinyint_type": [i for i in range(1, 256)]})
        .order_by("create_dt DESC", "update_dt DESC")
        .limit(10, 10)
        .execute(cursor=SSDfCursor, stats=True), "\n")

    print("TimeTable.select.join.where.order.limit [times] entire table".center(100))
    print(await tb.select("t1.id", "t2.varchar_type", "t2.varbinary_type", "t1.create_dt", "t1.update_dt")
        .join(db.my_timetable_m2, "t1.bigint_type = t2.bigint_type")
        .where(ins={"t1.tinyint_type": [i for i in range(1, 256)]})
        .order_by("create_dt DESC", "update_dt DESC")
        .limit(10, 10)
        .tabletimes(start=s, end=e)
        .execute(cursor=SSDfCursor, stats=True), "\n")

    print("TimeTable.select.join.where.order.limit [times] entire table spec conn".center(100))
    async with tb.acquire() as conn:
        print(await tb.select("t1.id", "t2.varchar_type", "t2.varbinary_type", "t1.create_dt", "t1.update_dt")
            .join(db.my_timetable_m2, "t1.bigint_type = t2.bigint_type")
            .where(ins={"t1.tinyint_type": [i for i in range(1, 256)]})
            .order_by("create_dt DESC", "update_dt DESC")
            .limit(10, 10)
            .tabletimes(start=s, end=e)
            .execute(conn, cursor=SSDfCursor, stats=True), "\n")

    # TimeTable Select & Where & Update
    print("TimeTable.select.where.update".center(100))
    print(await tb.select("varchar_type", "varbinary_type", tabletime=s)
        .where(ins={"tinyint_type": [i for i in range(10)]})
        .for_update()
        .execute(cursor=SSDfCursor, stats=True), "\n")

    # TimeTable Select & Where & Lock
    print("TimeTable.select.where.lock".center(100))
    print(await tb.select("varchar_type", "varbinary_type", tabletime=s)
        .where(ins={"tinyint_type": [i for i in range(10)]})
        .lock_in_share_mode()
        .execute(cursor=SSDfCursor, stats=True), "\n")

    # fmt: on
    # Done
    t2 = time.perf_counter()
    print("Total time:", t2 - t1)
    await server.close()


async def validate_update_query() -> None:
    user = "root"
    password = "Password_123456"

    # Instanciate Server
    server = Server(user=user, password=password, max_size=20)
    await server.fill(10)

    # Instanciate Database
    print()
    db = MyDatabase1(server)
    print(" Database ".center(100, "="))
    print(repr(db))
    rows = 1000
    data_dc = gen_data(rows, True)
    data_df = pd.DataFrame(data_dc)
    t1 = time.perf_counter()
    print()

    # fmt: off
    # Normal Table Update Basic
    tb = db.my_table1
    s, e = "2023-08-01", "2023-10-01"
    print("Normal Table.update.set.where".center(100))
    print(await tb.update()
        .set("smallint_type = 1", "varchar_type = %s", "varbinary_type = %s", 
             args=["S's UPDATE", b"B's UPDATE"])
        .where("bigint_type = %s", args=1)
        .execute(stats=True))

    print("Normal Table.update.set.order.limit".center(100))
    print(await tb.update()
        .set("smallint_type = 1", "varchar_type = %s", "varbinary_type = %s", 
             args=["S's UPDATE", b"B's UPDATE"])
        .where(ins={"bigint_type": [i for i in range(1, 11)]})
        .order_by("bigint_type DESC")
        .limit(5)
        .execute(stats=True))

    print("Normal Table.update.join( INNER ).set.where No.1".center(100))
    print(await tb.update()
        .join(db.my_table2, "t1.bigint_type = t2.bigint_type")
        .join(db.my_timetable_y, "t1.bigint_type = t3.bigint_type", tabletime=s)
        .set("t2.smallint_type = 1", "t2.varchar_type = %s", "t2.varbinary_type = %s", 
             args=["S's UPDATE", b"B's UPDATE"])
        .where("t1.bigint_type = %s", args=1)
        .execute(stats=True))

    print("Normal Table.update.join( INNER ).set.where No.2".center(100))
    print(await tb.update()
        .join(db.my_table2, "t1.bigint_type = t2.bigint_type", "t2.bigint_type = %s", args=2)
        .join(db.my_timetable_y, "t1.bigint_type = t3.bigint_type", tabletime=s)
        .set("t1.smallint_type = 1", "t1.varchar_type = %s", "t1.varbinary_type = %s", 
             args=["S's UPDATE", b"B's UPDATE"])
        .execute(stats=True))

    print("Normal Table.update.join( INNER ).set.where No.3".center(100))
    print(await tb.update()
        .join(db.my_table2, "t1.bigint_type = t2.bigint_type")
        .join(db.my_timetable_y, "t1.bigint_type = t3.bigint_type", tabletime=s)
        .set("t2.smallint_type = t1.smallint_type", 
             "t2.varchar_type = t1.varchar_type", 
             "t2.varbinary_type = t1.varbinary_type")
        .where("t1.bigint_type = %s", args=2)
        .execute(stats=True))

    print("Normal Table.update.join( LEFT ).set.where".center(100))
    print(await tb.update()
        .join(db.my_timetable_m, "t1.bigint_type = t2.bigint_type", method="LEFT", tabletime=s)
        .set("t1.smallint_type = 1", 
             "t1.varchar_type = IFNULL(t2.varchar_type, %s)", 
             "t1.varbinary_type = IFNULL(t2.varbinary_type, %s)", 
             args=["S's UPDATE", b"B's UPDATE"])
        .where(ins={"t1.bigint_type": [i for i in range(21, 31)]})
        .execute(stats=True))

    print("Normal Table.update.join( RIGHT ).set.where".center(100))
    print(await tb.update()
        .join(db.my_timetable_m, "t1.bigint_type = t2.bigint_type", method="RIGHT", tabletime=s)
        .set("t2.smallint_type = 1",
             "t2.varchar_type = IFNULL(t1.varchar_type, %s)",
             "t2.varbinary_type = IFNULL(t1.varbinary_type, %s)",
             args=["S's UPDATE", b"B's UPDATE"])
        .where(ins={"t2.bigint_type": [i for i in range(20, 31)]})
        .execute(stats=True))
    
    # Normal Table Update Values
    print("Normal Table.update.values [dicts]".center(100))
    print(await tb.update()
          .values(data_dc,
                  ["smallint_type", "varchar_type", "varbinary_type"],
                  "bigint_type")
          .execute(stats=True))
    
    print("Normal Table.update.values [dicts] spec conn".center(100))
    async with tb.transaction() as conn:
        print(await tb.update()
            .values(gen_data(rows, True),
                  ["smallint_type", "varchar_type", "varbinary_type"],
                  "bigint_type")
            .execute(conn, stats=True))

    print("Normal Table.update.values [DataFrame]".center(100))
    print(await tb.update()
          .values(data_df,
                  ["smallint_type", "varchar_type", "varbinary_type"],
                  "bigint_type")
          .execute(stats=True))

    print("Normal Table.update.set.where.values [DataFrame]".center(100))
    print(await tb.update()
          .set("char_type = %s", args=["CHAR's UPD"])
          .where(ins={"tinyint_type": [i for i in range(1, 256)]})
          .values(data_df,
                  ["smallint_type", "varchar_type", "varbinary_type"],
                  "bigint_type")
          .execute(stats=True))

    # TimeTable Update Basic
    tb = db.my_timetable_m
    print("TimeTable.update.set.where [not exists]".center(100))
    print(await tb.update(tabletime="2023-09-01")
        .set("smallint_type = 1", "varchar_type = %s", "varbinary_type = %s",
             args=["S's UPDATE", b"B's UPDATE"])
        .where("bigint_type = %s", args=1)
        .execute(stats=True))

    print("TimeTable.update.set.where [time]".center(100))
    print(await tb.update(tabletime=s)
        .set("smallint_type = 1", "varchar_type = %s", "varbinary_type = %s",
             args=["S's UPDATE", b"B's UPDATE"])
        .where("bigint_type = %s", args=1)
        .execute(stats=True))

    print("TimeTable.update.set.where [times]".center(100))
    print(await tb.update()
        .set("smallint_type = 1", "varchar_type = %s", "varbinary_type = %s",
             args=["S's UPDATE", b"B's UPDATE"])
        .where("bigint_type = %s", args=2)
        .tabletimes(start=s, end=e)
        .execute(stats=True))

    print("TimeTable.update.set.where [times] spec conn".center(100))
    async with tb.transaction() as conn:
        print(await tb.update()
            .set("smallint_type = 1", "varchar_type = %s", "varbinary_type = %s",
                args=["S's UPDATE", b"B's UPDATE"])
            .where("bigint_type = %s", args=2)
            .tabletimes(start=s, end=e)
            .execute(conn, stats=True))

    print("TimeTable.update.set.order.limit [time]".center(100))
    print(await tb.update(tabletime=s)
        .set("smallint_type = 1", "varchar_type = %s", "varbinary_type = %s",
             args=["S's UPDATE", b"B's UPDATE"])
        .where(ins={"bigint_type": [i for i in range(1, 11)]})
        .order_by("bigint_type DESC")
        .limit(5)
        .execute(stats=True))

    print("TimeTable.update.set.order.limit [times]".center(100))
    print(await tb.update()
        .set("smallint_type = 1", "varchar_type = %s", "varbinary_type = %s",
             args=["S's UPDATE", b"B's UPDATE"])
        .where(ins={"bigint_type": [i for i in range(11, 21)]})
        .order_by("bigint_type DESC")
        .limit(5)
        .tabletimes(start=s, end=e)
        .execute(stats=True))

    print("TimeTable.update.set.order.limit [times] spec conn".center(100))
    async with tb.transaction() as conn:
        print(await tb.update()
            .set("smallint_type = 1", "varchar_type = %s", "varbinary_type = %s",
                args=["S's UPDATE", b"B's UPDATE"])
            .where(ins={"bigint_type": [i for i in range(11, 21)]})
            .order_by("bigint_type DESC")
            .limit(5)
            .tabletimes(start=s, end=e)
            .execute(conn, stats=True))

    print("TimeTable.update.join( INNER ).set.where [not exists] No.1".center(100))
    print(await tb.update(tabletime=s)
        .join(db.my_timetable_m2, "t1.bigint_type = t2.bigint_type", tabletime="2023-09-01")
        .set("t2.smallint_type = 1", "t2.varchar_type = %s", "t2.varbinary_type = %s",
             args=["S's UPDATE", b"B's UPDATE"])
        .where("t1.bigint_type = %s", args=1)
        .execute(stats=True))

    print("TimeTable.update.join( INNER ).set.where [time] No.1".center(100))
    print(await tb.update(tabletime=s)
        .join(db.my_timetable_m2, "t1.bigint_type = t2.bigint_type", tabletime=s)
        .set("t2.smallint_type = 1", "t2.varchar_type = %s", "t2.varbinary_type = %s",
             args=["S's UPDATE", b"B's UPDATE"])
        .where("t1.bigint_type = %s", args=1)
        .execute(stats=True))

    print("TimeTable.update.join( INNER ).set.where [times] No.1".center(100))
    print(await tb.update()
        .join(db.my_timetable_m2, "t1.bigint_type = t2.bigint_type")
        .set("t2.smallint_type = 1", "t2.varchar_type = %s", "t2.varbinary_type = %s",
             args=["S's UPDATE", b"B's UPDATE"])
        .where("t1.bigint_type = %s", args=2)
        .tabletimes(start=s, end=e)
        .execute(stats=True))
    
    print("TimeTable.update.join( INNER ).set.where [times] No.1 spec conn".center(100))
    async with tb.transaction() as conn:
        print(await tb.update()
            .join(db.my_timetable_m2, "t1.bigint_type = t2.bigint_type")
            .set("t2.smallint_type = 1", "t2.varchar_type = %s", "t2.varbinary_type = %s",
                args=["S's UPDATE", b"B's UPDATE"])
            .where("t1.bigint_type = %s", args=2)
            .tabletimes(start=s, end=e)
            .execute(conn, stats=True))

    print("TimeTable.update.join( INNER ).set.where [time] No.2".center(100))
    print(await tb.update(tabletime=e)
        .join(db.my_timetable_m2, "t1.bigint_type = t2.bigint_type", "t2.bigint_type = %s", args=10, tabletime=e)
        .set("t1.smallint_type = 1", "t1.varchar_type = %s", "t1.varbinary_type = %s",
             args=["S's UPDATE", b"B's UPDATE"])
        .execute(stats=True))

    print("TimeTable.update.join( INNER ).set.where [times] No.2".center(100))
    print(await tb.update()
        .join(db.my_timetable_m2, "t1.bigint_type = t2.bigint_type", "t2.bigint_type = %s", args=40)
        .set("t1.smallint_type = 1", "t1.varchar_type = %s", "t1.varbinary_type = %s",
             args=["S's UPDATE", b"B's UPDATE"])
        .tabletimes(start=s, end=e)
        .execute(stats=True))
    
    print("TimeTable.update.join( INNER ).set.where [times] No.2 spec conn".center(100))
    async with tb.transaction() as conn:
        print(await tb.update()
            .join(db.my_timetable_m2, "t1.bigint_type = t2.bigint_type", "t2.bigint_type = %s", args=40)
            .set("t1.smallint_type = 1", "t1.varchar_type = %s", "t1.varbinary_type = %s",
                args=["S's UPDATE", b"B's UPDATE"])
            .tabletimes(start=s, end=e)
            .execute(conn, stats=True))

    print("TimeTable.update.join( INNER ).set.where [time] No.3".center(100))
    print(await tb.update(tabletime=s)
        .join(db.my_timetable_m2, "t1.bigint_type = t2.bigint_type", tabletime=s)
        .set("t2.smallint_type = t1.smallint_type",
             "t2.varchar_type = t1.varchar_type",
             "t2.varbinary_type = t1.varbinary_type")
        .where("t1.bigint_type = %s", args=30)
        .execute(stats=True))

    print("TimeTable.update.join( INNER ).set.where [times] No.3".center(100))
    print(await tb.update()
        .join(db.my_timetable_m2, "t1.bigint_type = t2.bigint_type")
        .set("t2.smallint_type = t1.smallint_type",
             "t2.varchar_type = t1.varchar_type",
             "t2.varbinary_type = t1.varbinary_type")
        .where("t1.bigint_type = %s", args=40)
        .tabletimes(start=s, end=e)
        .execute(stats=True))
    
    print("TimeTable.update.join( INNER ).set.where [times] No.3 spec conn".center(100))
    async with tb.transaction() as conn:
        print(await tb.update()
            .join(db.my_timetable_m2, "t1.bigint_type = t2.bigint_type")
            .set("t2.smallint_type = t1.smallint_type",
                "t2.varchar_type = t1.varchar_type",
                "t2.varbinary_type = t1.varbinary_type")
            .where("t1.bigint_type = %s", args=40)
            .tabletimes(start=s, end=e)
            .execute(conn, stats=True))

    print("TimeTable.update.join( LEFT ).set.where [not exists]".center(100))
    print(await tb.update(tabletime=s)
        .join(db.my_timetable_m2, "t1.bigint_type = t2.bigint_type", method="LEFT", tabletime="2023-09-01")
        .set("t1.smallint_type = 1",
             "t1.varchar_type = IFNULL(t2.varchar_type, %s)",
             "t1.varbinary_type = IFNULL(t2.varbinary_type, %s)",
             args=["S's UPDATE", b"B's UPDATE"])
        .where(ins={"t1.bigint_type": [i for i in range(51, 61)]})
        .execute(stats=True))

    print("TimeTable.update.join( LEFT ).set.where [time]".center(100))
    print(await tb.update(tabletime=s)
        .join(db.my_timetable_m2, "t1.bigint_type = t2.bigint_type", method="LEFT", tabletime=e)
        .set("t1.smallint_type = 1",
             "t1.varchar_type = IFNULL(t2.varchar_type, %s)",
             "t1.varbinary_type = IFNULL(t2.varbinary_type, %s)",
             args=["S's UPDATE", b"B's UPDATE"])
        .where(ins={"t1.bigint_type": [i for i in range(51, 61)]})
        .execute(stats=True))

    print("TimeTable.update.join( LEFT ).set.where [times]".center(100))
    print(await tb.update()
        .join(db.my_timetable_m2, "t1.bigint_type = t2.bigint_type", method="LEFT", tabletime=e)
        .set("t1.smallint_type = 1",
             "t1.varchar_type = IFNULL(t2.varchar_type, %s)",
             "t1.varbinary_type = IFNULL(t2.varbinary_type, %s)",
             args=["S's UPDATE", b"B's UPDATE"])
        .where(ins={"t1.bigint_type": [i for i in range(51, 61)]})
        .tabletimes(start=s, end=e)
        .execute(stats=True))
    
    print("TimeTable.update.join( LEFT ).set.where [times] spec conn".center(100))
    async with tb.transaction() as conn:
        print(await tb.update()
            .join(db.my_timetable_m2, "t1.bigint_type = t2.bigint_type", method="LEFT", tabletime=e)
            .set("t1.smallint_type = 1",
                "t1.varchar_type = IFNULL(t2.varchar_type, %s)",
                "t1.varbinary_type = IFNULL(t2.varbinary_type, %s)",
                args=["S's UPDATE", b"B's UPDATE"])
            .where(ins={"t1.bigint_type": [i for i in range(51, 61)]})
            .tabletimes(start=s, end=e)
            .execute(conn, stats=True))

    print("TimeTable.update.join( RIGHT ).set.where [not exists]".center(100))
    print(await tb.update(tabletime=s)
        .join(db.my_timetable_m2, "t1.bigint_type = t2.bigint_type", method="RIGHT", tabletime="2023-09-1")
        .set("t2.smallint_type = 1",
             "t2.varchar_type = IFNULL(t1.varchar_type, %s)",
             "t2.varbinary_type = IFNULL(t1.varbinary_type, %s)",
             args=["S's UPDATE", b"B's UPDATE"])
        .where(ins={"t2.bigint_type": [i for i in range(20, 31)]})
        .execute(stats=True))

    print("TimeTable.update.join( RIGHT ).set.where [time]".center(100))
    print(await tb.update(tabletime=s)
        .join(db.my_timetable_m2, "t1.bigint_type = t2.bigint_type", method="RIGHT", tabletime=s)
        .set("t2.smallint_type = 1",
             "t2.varchar_type = IFNULL(t1.varchar_type, %s)",
             "t2.varbinary_type = IFNULL(t1.varbinary_type, %s)",
             args=["S's UPDATE", b"B's UPDATE"])
        .where(ins={"t2.bigint_type": [i for i in range(20, 31)]})
        .execute(stats=True))

    print("TimeTable.update.join( RIGHT ).set.where [times]".center(100))
    print(await tb.update()
        .join(db.my_timetable_m2, "t1.bigint_type = t2.bigint_type", method="RIGHT", tabletime=s)
        .set("t2.smallint_type = 1",
             "t2.varchar_type = IFNULL(t1.varchar_type, %s)",
             "t2.varbinary_type = IFNULL(t1.varbinary_type, %s)",
             args=["S's UPDATE", b"B's UPDATE"])
        .where(ins={"t2.bigint_type": [i for i in range(20, 31)]})
        .tabletimes(start=s, end=e)
        .execute(stats=True))

    print("TimeTable.update.join( RIGHT ).set.where [times] spec conn".center(100))
    async with tb.transaction() as conn:
        print(await tb.update()
            .join(db.my_timetable_m2, "t1.bigint_type = t2.bigint_type", method="RIGHT", tabletime=s)
            .set("t2.smallint_type = 1",
                "t2.varchar_type = IFNULL(t1.varchar_type, %s)",
                "t2.varbinary_type = IFNULL(t1.varbinary_type, %s)",
                args=["S's UPDATE", b"B's UPDATE"])
            .where(ins={"t2.bigint_type": [i for i in range(20, 31)]})
            .tabletimes(start=s, end=e)
            .execute(conn, stats=True))

    # TimeTable Update Values
    print("TimeTable.update.values [dicts]".center(100))
    print(await tb.update()
        .values(data_dc,
                ["smallint_type", "varchar_type", "varbinary_type"],
                "bigint_type")
        .execute(stats=True))
    
    print("TimeTable.update.values [dicts] spec conn".center(100))
    async with tb.transaction() as conn:
        print(await tb.update()
            .values(gen_data(rows, True),
                    ["smallint_type", "varchar_type", "varbinary_type"],
                    "bigint_type")
            .execute(conn, stats=True))

    print("TimeTable.update.values [DataFrame]".center(100))
    print(await tb.update()
        .values(data_df,
                ["smallint_type", "varchar_type", "varbinary_type"],
                "bigint_type")
        .execute(stats=True))

    print("TimeTable.update.set.where.values [DataFrame]".center(100))
    print(await tb.update()
        .set("char_type = %s", args=["CHAR's UPD"])
        .where(ins={"tinyint_type": [i for i in range(1, 256)]})
        .values(data_df,
                ["smallint_type", "varchar_type", "varbinary_type"],
                "bigint_type")
        .execute(stats=True))

    # fmt: on
    # Done
    t2 = time.perf_counter()
    print("Total time:", t2 - t1)
    await server.close()


async def validate_delete_query() -> None:
    user = "root"
    password = "Password_123456"

    # Instanciate Server
    server = Server(user=user, password=password, max_size=20)
    await server.fill(10)

    # Instanciate Database
    print()
    db = MyDatabase1(server)
    print(" Database ".center(100, "="))
    print(repr(db))
    rows = 1000
    data_dc = gen_data(rows, True)
    data_df = pd.DataFrame(data_dc)
    t1 = time.perf_counter()
    print()

    # fmt: off
    # Normal Table Basic
    tb = db.my_table1
    s, e = "2023-08-01", "2023-10-01"
    print("Normal Table.delete.where".center(100))
    print(await tb.delete()
        .where("bigint_type = %s", args=1)
        .where_time("create_dt", start="2020-01-01", end="2024-01-01", unit="Y")
        .where_time("update_dt", start="2020-01-01", end="2024-01-01", unit="M")
        .execute(stats=True))
    
    print("Normal Table.delete.where.order.limit".center(100))
    print(await tb.delete()
        .where(ins={"bigint_type": [i for i in range(1, 11)]})
        .order_by("bigint_type DESC")
        .limit(5)
        .execute(stats=True))
    
    print("Normal Table.delete.join( INNER ).where [delete 't1']".center(100))
    print(await tb.delete("t1")
        .join(db.my_table2, "t1.bigint_type = t2.bigint_type", ins={"t2.bigint_type": [i for i in range(21, 31)]})
        .where("t1.bigint_type = %s", args=21)
        .execute(stats=True))
    
    print("Normal Table.delete.join( INNER ).where [delete 't2']".center(100))
    print(await tb.delete("t2")
        .join(db.my_table2, "t1.bigint_type = t2.bigint_type", ins={"t2.bigint_type": [i for i in range(21, 31)]})
        .where("t1.bigint_type = %s", args=22)
        .execute(stats=True))
    
    print("Normal Table.delete.join( INNER ).where [delete 'all']".center(100))
    print(await tb.delete()
        .join(db.my_table2, "t1.bigint_type = t2.bigint_type", ins={"t2.bigint_type": [i for i in range(21, 31)]})
        .where("t1.bigint_type = %s", args=23)
        .execute(stats=True))
    
    print("Normal Table.delete.join( LEFT ).where [delete 'all']".center(100))
    print(await tb.delete()
        .join(
            db.my_table2, "t1.bigint_type = t2.bigint_type", 
            ins={"t2.bigint_type": [i for i in range(21, 31)]},
            method="LEFT")
        .where("t1.bigint_type = %s", args=24)
        .execute(stats=True))
    
    print("Normal Table.delete.join( RIGHT ).where [delete 'all']".center(100))
    print(await tb.delete()
        .join(
            db.my_table2, "t1.bigint_type = t2.bigint_type", 
            ins={"t2.bigint_type": [i for i in range(21, 31)]},
            method="RIGHT")
        .where("t1.bigint_type = %s", args=25)
        .execute(stats=True))
    
    # Normal Table Delete Values
    print("Normal Table.delete.values [dicts]".center(100))
    print(await tb.delete()
        .values(data_dc, ["bigint_type", "varchar_type"])
        .execute(stats=True))
    
    print("Normal Table.delete.values [dicts] spec conn".center(100))
    async with tb.transaction() as conn:
        print(await tb.delete()
            .values(gen_data(rows, True), ["bigint_type", "varchar_type"])
            .execute(conn, stats=True))

    print("Normal Table.delete.where.values [DataFrame]".center(100))
    print(await tb.delete()
        .where(ins={"tinyint_type": [i for i in range(1, 256)]})
        .values(data_df, ["bigint_type", "varbinary_type"])
        .execute(stats=True))
    
    # TimeTable Delete Basic
    tb = db.my_timetable_m
    print("TimeTable.delete.where [not exists]".center(100))
    print(await tb.delete(tabletime="2023-09-01")
        .where("bigint_type = %s", args=1)
        .where_time("create_dt", start="2020-01-01", end="2024-01-01", unit="Y")
        .where_time("update_dt", start="2020-01-01", end="2024-01-01", unit="M")
        .execute(stats=True))
    
    print("TimeTable.delete.where [time]".center(100))
    print(await tb.delete(tabletime=s)
        .where("bigint_type = %s", args=1)
        .where_time("create_dt", start="2020-01-01", end="2024-01-01", unit="Y")
        .where_time("update_dt", start="2020-01-01", end="2024-01-01", unit="M")
        .execute(stats=True))
    
    print("TimeTable.delete.where [times]".center(100))
    print(await tb.delete()
        .where("bigint_type = %s", args=2)
        .where_time("create_dt", start="2020-01-01", end="2024-01-01", unit="Y")
        .where_time("update_dt", start="2020-01-01", end="2024-01-01", unit="M")
        .tabletimes(start=s, end=e)
        .execute(stats=True))
    
    print("TimeTable.delete.where [times] spec conn".center(100))
    async with tb.transaction() as conn:
        print(await tb.delete()
            .where("bigint_type = %s", args=2)
            .where_time("create_dt", start="2020-01-01", end="2024-01-01", unit="Y")
            .where_time("update_dt", start="2020-01-01", end="2024-01-01", unit="M")
            .tabletimes(start=s, end=e)
            .execute(conn, stats=True))
        
    print("TimeTable.delete.where.order.limit [time]".center(100))
    print(await tb.delete(tabletime=s)
        .where(ins={"bigint_type": [i for i in range(1, 11)]})
        .order_by("bigint_type DESC")
        .limit(5)
        .execute(stats=True))
    
    print("TimeTable.delete.where.order.limit [times]".center(100))
    print(await tb.delete()
        .where(ins={"bigint_type": [i for i in range(1, 11)]})
        .order_by("bigint_type DESC")
        .limit(5)
        .tabletimes(start=s, end=e)
        .execute(stats=True))
    
    print("TimeTable.delete.where.order.limit [times] spec conn".center(100))
    async with tb.transaction() as conn:
        print(await tb.delete()
            .where(ins={"bigint_type": [i for i in range(1, 11)]})
            .order_by("bigint_type DESC")
            .limit(5)
            .tabletimes(start=s, end=e)
            .execute(conn, stats=True))
        
    print("TimeTable.delete.join( INNER ).where [not exists]".center(100))
    print(await tb.delete(tabletime="2023-09-01")
        .join(db.my_timetable_m2, 
            "t1.bigint_type = t2.bigint_type", 
            ins={"t2.bigint_type": [i for i in range(21, 31)]},
            tabletime=s)
        .where("t1.bigint_type = %s", args=21)
        .execute(stats=True))

    print("TimeTable.delete.join( INNER ).where [time delete 't1']".center(100))
    print(await tb.delete("t1", tabletime=s)
        .join(db.my_timetable_m2, 
            "t1.bigint_type = t2.bigint_type", 
            ins={"t2.bigint_type": [i for i in range(21, 31)]},
            tabletime=s)
        .where(ins={"t1.bigint_type": [i for i in range(21, 31)]})
        .execute(stats=True))

    print("TimeTable.delete.join( INNER ).where [time delete 't2']".center(100))
    print(await tb.delete("t2", tabletime=s)
        .join(db.my_timetable_m2,
            "t1.bigint_type = t2.bigint_type",
            ins={"t2.bigint_type": [i for i in range(31, 41)]},
            tabletime=s)
        .where(ins={"t1.bigint_type": [i for i in range(31, 41)]})
        .execute(stats=True))

    print("TimeTable.delete.join( INNER ).where [time delete 'all']".center(100))
    print(await tb.delete(tabletime=s)
        .join(db.my_timetable_m2,
            "t1.bigint_type = t2.bigint_type",
            ins={"t2.bigint_type": [i for i in range(41, 51)]},
            tabletime=s)
        .where(ins={"t1.bigint_type": [i for i in range(41, 51)]})
        .execute(stats=True))
    
    print("TimeTable.delete.join( INNER ).where [times delete 'all']".center(100))
    print(await tb.delete()
        .join(db.my_timetable_m2,
            "t1.bigint_type = t2.bigint_type",
            ins={"t2.bigint_type": [i for i in range(41, 51)]})
        .where(ins={"t1.bigint_type": [i for i in range(41, 51)]})
        .tabletimes(start=s, end=e)
        .execute(stats=True))
    
    print("TimeTable.delete.join( INNER ).where [times delete 'all'] spec conn".center(100))
    async with tb.transaction() as conn:
        print(await tb.delete()
            .join(db.my_timetable_m2,
                "t1.bigint_type = t2.bigint_type",
                ins={"t2.bigint_type": [i for i in range(41, 51)]})
            .where(ins={"t1.bigint_type": [i for i in range(41, 51)]})
            .tabletimes(start=s, end=e)
            .execute(conn, stats=True))
        
    print("TimeTable.delete.join( LEFT ).where [not exist]".center(100))
    print(await tb.delete(tabletime=s)
        .join(
            db.my_timetable_m2, "t1.bigint_type = t2.bigint_type",
            ins={"t2.bigint_type": [i for i in range(51, 61)]},
            method="LEFT", tabletime="2023-09-01")
        .where(ins={"t1.bigint_type": [i for i in range(51, 61)]})
        .execute(stats=True))

    print("TimeTable.delete.join( LEFT ).where [time delete 'all']".center(100))
    print(await tb.delete(tabletime=s)
        .join(
            db.my_timetable_m2, "t1.bigint_type = t2.bigint_type",
            ins={"t2.bigint_type": [i for i in range(51, 61)]},
            method="LEFT", tabletime=s)
        .where(ins={"t1.bigint_type": [i for i in range(51, 61)]})
        .execute(stats=True))

    print("TimeTable.delete.join( LEFT ).where [times delete 'all']".center(100))
    print(await tb.delete()
        .join(
            db.my_timetable_m2, "t1.bigint_type = t2.bigint_type",
            ins={"t2.bigint_type": [i for i in range(61, 71)]},
            method="LEFT", tabletime=s)
        .where(ins={"t1.bigint_type": [i for i in range(61, 71)]})
        .tabletimes(start=s, end=e)
        .execute(stats=True))

    print("TimeTable.delete.join( LEFT ).where [times delete 'all'] spec conn".center(100))
    async with tb.transaction() as conn:
        print(await tb.delete()
            .join(
                db.my_timetable_m2, "t1.bigint_type = t2.bigint_type",
                ins={"t2.bigint_type": [i for i in range(61, 71)]},
                method="LEFT", tabletime=s)
            .where(ins={"t1.bigint_type": [i for i in range(61, 71)]})
            .tabletimes(start=s, end=e)
            .execute(conn, stats=True))
        
    print("TimeTable.delete.join( RIGHT ).where [not exist]".center(100))
    print(await tb.delete(tabletime="2023-09-01")
        .join(
            db.my_timetable_m2, "t1.bigint_type = t2.bigint_type",
            ins={"t2.bigint_type": [i for i in range(71, 81)]},
            method="RIGHT", tabletime=s)
        .where(ins={"t1.bigint_type": [i for i in range(71, 81)]})
        .execute(stats=True))

    print("TimeTable.delete.join( RIGHT ).where [time delete 'all']".center(100))
    print(await tb.delete(tabletime=s)
        .join(
            db.my_timetable_m2, "t1.bigint_type = t2.bigint_type",
            ins={"t2.bigint_type": [i for i in range(71, 81)]},
            method="RIGHT", tabletime=s)
        .where(ins={"t1.bigint_type": [i for i in range(71, 81)]})
        .execute(stats=True))

    print("TimeTable.delete.join( RIGHT ).where [times delete 'all']".center(100))
    print(await tb.delete()
        .join(
            db.my_timetable_m2, "t1.bigint_type = t2.bigint_type",
            ins={"t2.bigint_type": [i for i in range(81, 91)]},
            method="RIGHT", tabletime=s)
        .where(ins={"t1.bigint_type": [i for i in range(81, 91)]})
        .tabletimes(start=s, end=e)
        .execute(stats=True))

    print("TimeTable.delete.join( RIGHT ).where [time delete 'all'] spec conn".center(100))
    async with tb.transaction() as conn:
        print(await tb.delete()
            .join(
                db.my_timetable_m2, "t1.bigint_type = t2.bigint_type",
                ins={"t2.bigint_type": [i for i in range(81, 91)]},
                method="RIGHT", tabletime=s)
            .where(ins={"t1.bigint_type": [i for i in range(81, 91)]})
            .tabletimes(start=s, end=e)
            .execute(conn, stats=True))
        
    # TimeTable Delete Values
    print("TimeTable.delete.values [dicts]".center(100))
    print(await tb.delete()
        .values(data_dc, ["bigint_type", "varchar_type"])
        .execute(stats=True))
    
    print("TimeTable.delete.values [dicts] spec conn".center(100))
    async with tb.transaction() as conn:
        print(await tb.delete()
            .values(gen_data(rows, True), ["bigint_type", "varchar_type"])
            .execute(conn, stats=True))

    print("TimeTable.delete.where.values [DataFrame]".center(100))
    print(await tb.delete()
        .where(ins={"tinyint_type": [i for i in range(1, 256)]})
        .values(data_df, ["bigint_type", "varbinary_type"])
        .execute(stats=True))

    # fmt: on
    # Done
    t2 = time.perf_counter()
    print("Total time:", t2 - t1)
    await server.close()


async def validate_create_temp_query() -> None:
    from mysqlengine.connection import Cursor, DictCursor, DfCursor
    from mysqlengine.connection import SSCursor, SSDictCursor, SSDfCursor

    user = "root"
    password = "Password_123456"

    # Instanciate Server
    server = Server(user=user, password=password, max_size=20)
    await server.fill(10)

    # Instanciate Database
    print()
    db = MyDatabase1(server)
    print(" Database ".center(100, "="))
    print(repr(db))
    rows = 1000
    data_dc = gen_data(rows, True)
    data_df = pd.DataFrame(data_dc)
    t1 = time.perf_counter()
    print()

    # fmt: off
    # Temporary Table Basic
    tb = db.my_table1
    s, e = "2023-08-01", "2023-10-01"
    print("Table.create_temp [Memory]".center(100))
    async with tb.transaction() as conn:
        tmp = await tb.create_temp().execute(conn, stats=True)

    print("Table.create_temp [InnoDB]".center(100))
    async with tb.transaction() as conn:
        tmp = await tb.create_temp(engine="InnoDB").execute(conn, stats=True)

    print("Table.create_temp [MyISAM]".center(100))
    async with tb.transaction() as conn:
        tmp = await tb.create_temp(engine="MyISAM").execute(conn, stats=True)

    print("Table.create_temp [Charset]".center(100))
    async with tb.transaction() as conn:
        tmp = await tb.create_temp(charset="utf8mb4", collate="utf8mb4_czech_ci").execute(conn, stats=True)

    print("Table.create_temp [no index]".center(100))
    async with tb.transaction() as conn:
        tmp = (await tb.create_temp("bigint_type", "smallint_type")
            .execute(conn, stats=True))

    print("Table.create_temp [auto index]".center(100))
    async with tb.transaction() as conn:
        tmp = (await tb.create_temp("bigint_type", "smallint_type", indexes="auto")
            .execute(conn, stats=True))

    print("Table.create_temp [index instance]".center(100))
    async with tb.transaction() as conn:
        tmp = (await tb.create_temp("bigint_type", "smallint_type", 
                                    indexes=tb.indexes.search_by_columns("bigint_type"))
            .execute(conn, stats=True))

    print("Table.create_temp [index name]".center(100))
    async with tb.transaction() as conn:
        tmp = (await tb.create_temp("bigint_type", "smallint_type", indexes="uixBiginttype")
            .execute(conn, stats=True))

    print("Table.create_temp [custom index]".center(100))
    async with tb.transaction() as conn:
        tmp = (await tb.create_temp("bigint_type", "smallint_type", 
                                    indexes=["INDEX idx1 (bigint_type, smallint_type)"])
            .execute(conn, stats=True))
        
    # Normal Table Select
    print("Normal Table.create_temp.select( all )".center(100))
    async with tb.transaction() as conn:
        tmp = await tb.create_temp(indexes="auto").select(tb).execute(conn, stats=True)
        print(await tb.select(from_=tmp).execute(conn, cursor=SSDfCursor), "\n")

    print("Normal Table.create_temp.select( partial )".center(100))
    async with tb.transaction() as conn:
        tmp = (await tb.create_temp("id", "bigint_type", "varchar_type", indexes="auto")
            .select(tb, "bigint_type", "varchar_type").execute(conn, stats=True))
        print(await tb.select(from_=tmp).execute(conn, cursor=SSDfCursor), "\n")

    # Normal Table Values
    print("Normal Table.create_temp.values( all ) [dicts]".center(100))
    async with tb.transaction() as conn:
        tmp = await tb.create_temp(indexes="auto").values(data_dc).execute(conn, stats=True)
        print(await tb.select(from_=tmp).execute(conn, cursor=SSDfCursor), "\n")

    print("Normal Table.create_temp.values( partial ) [DataFrame]".center(100))
    async with tb.transaction() as conn:
        tmp = (await tb.create_temp("id", "bigint_type", "varchar_type", indexes="auto")
            .values(data_df, "bigint_type", "varchar_type").execute(conn, stats=True))
        print(await tb.select(from_=tmp).execute(conn, cursor=SSDfCursor), "\n")

    # TimeTable Select
    tb = db.my_timetable_m
    print("TimeTable.create_temp.select( all ) [non-exist]".center(100))
    async with tb.transaction() as conn:
        tmp = (await tb.create_temp(indexes="auto")
            .select(tb, tabletime="2023-09-01").execute(conn, stats=True))
        print(await tb.select(from_=tmp).execute(conn, cursor=SSDfCursor), "\n")

    print("TimeTable.create_temp.select( all ) [time]".center(100))
    async with tb.transaction() as conn:
        tmp = (await tb.create_temp(indexes="auto")
            .select(tb, tabletime=s).execute(conn, stats=True))
        print(await tb.select(from_=tmp).execute(conn, cursor=SSDfCursor), "\n")

    print("TimeTable.create_temp.select( partial )".center(100))
    async with tb.transaction() as conn:
        tmp = (await tb.create_temp("id", "bigint_type", "varchar_type")
            .select(tb, "bigint_type", "varchar_type")
            .tabletimes(start=s, end=e).execute(conn, stats=True))
        print(await tb.select(from_=tmp).execute(conn, cursor=SSDfCursor), "\n")

    # TimeTable Values
    print("TimeTable.create_temp.values( all ) [dicts]".center(100))
    async with tb.transaction() as conn:
        tmp = await tb.create_temp(indexes="auto").values(data_dc).execute(conn, stats=True)
        print(await tb.select(from_=tmp).execute(conn, cursor=SSDfCursor), "\n")

    print("TimeTable.create_temp.values( partial ) [DataFrame]".center(100))
    async with tb.transaction() as conn:
        tmp = (await tb.create_temp("id", "bigint_type", "varchar_type", indexes="auto")
            .values(data_df, "bigint_type", "varchar_type").execute(conn, stats=True))
        print(await tb.select(from_=tmp).execute(conn, cursor=SSDfCursor), "\n")

    # fmt: on
    # Done
    t2 = time.perf_counter()
    print("Total time:", t2 - t1)
    await server.close()


async def validate_compare_query() -> None:
    from mysqlengine.connection import Cursor, DictCursor, DfCursor
    from mysqlengine.connection import SSCursor, SSDictCursor, SSDfCursor

    user = "root"
    password = "Password_123456"

    # Instanciate Server
    server = Server(user=user, password=password, max_size=20)
    await server.fill(10)

    # Instanciate Database
    print()
    db = MyDatabase1(server)
    print(" Database ".center(100, "="))
    print(repr(db))
    rows = 1000
    t1 = time.perf_counter()
    print()

    # fmt: off
    # Normal Table Compare
    tb = db.my_table1
    s, e = "2023-08-01", "2023-10-01"
    print("Normal Table.compare [spec conn]".center(100))
    async with tb.transaction() as conn:
        bsl = await tb.select().for_update().execute(conn, cursor=SSDfCursor)
        print(await tb.compare(pd.DataFrame(gen_data(rows, True))[100:rows], bsl)
            .operations(find_common=True, find_identical=True)
            .execute(conn, stats=True), "\n")

    print("Normal Table.compare [temp conn]".center(100))
    bsl = await tb.select().execute(cursor=SSDfCursor)
    print(await tb.compare(pd.DataFrame(gen_data(rows, True)), bsl)
        .operations(find_common=True, find_identical=True)
        .execute(stats=True), "\n")

    print("Normal Table.compare [temp conn]".center(100))
    bsl = await tb.select().execute(cursor=SSDfCursor)
    print(await tb.compare(pd.DataFrame(gen_data(rows, True)), bsl,
            compare_columns=["tinyint_type", "varchar_type"])
        .operations(find_common=True, find_identical=True)
        .execute(stats=True), "\n")

    # TimeTable Compare
    tb = db.my_timetable_m
    s, e = "2023-08-01", "2023-10-01"
    print("TimeTable.compare [spec conn]".center(100))
    bsl = await tb.select().tabletimes(start=s, end=e).execute(cursor=SSDictCursor)
    async with tb.transaction() as conn:
        print(await tb.compare(pd.DataFrame(gen_data(rows, True))[100:rows], bsl)
            .operations(find_common=True, find_identical=True)
            .execute(conn, stats=True), "\n")

    print("TimeTable.compare [temp conn]".center(100))
    bsl = await tb.select().tabletimes(start=s, end=e).execute(cursor=SSDictCursor)
    print(await tb.compare(pd.DataFrame(gen_data(rows, True)), bsl)
        .operations(find_common=True, find_identical=True)
        .execute(stats=True), "\n")

    print("TimeTable.compare [temp conn]".center(100))
    bsl = await tb.select().tabletimes(start=s, end=e).execute(cursor=SSDictCursor)
    print(await tb.compare(pd.DataFrame(gen_data(rows, True)), bsl,
            compare_columns=["tinyint_type", "varchar_type"])
        .operations(find_common=True, find_identical=True)
        .execute(stats=True), "\n")

    # fmt: on
    # Done
    t2 = time.perf_counter()
    print("Total time:", t2 - t1)
    await server.close()


async def validate_engine() -> None:
    from mysqlengine.connection import Cursor, DictCursor, DfCursor
    from mysqlengine.connection import SSCursor, SSDictCursor, SSDfCursor

    user = "root"
    password = "Password_123456"

    # Instanciate Server
    server = Server(user=user, password=password, max_size=20)
    await server.fill(10)

    # Instanciate Engine
    engine = Engine(server, mydb1=MyDatabase1, mydb2=MyDatabase2)
    t1 = time.perf_counter()

    # Instanciate Database
    print()
    db1 = await engine.access("mydb1")
    print(" Database ".center(100, "="))
    print(repr(db1))
    print()

    db2_us = await engine.access("mydb2", country="US")
    print(" Database ".center(100, "="))
    print(repr(db2_us))
    print()

    db2_ca = await engine.access("mydb2", country="CA")
    print(" Database ".center(100, "="))
    print(repr(db2_ca))
    print()

    # fmt: on
    # Done
    t2 = time.perf_counter()
    print("Total time:", t2 - t1)
    await engine.disconnect()


if __name__ == "__main__":
    validate_column_dtype()
    validate_index()
    asyncio.run(validate_database())
    asyncio.run(validate_insert_query())
    asyncio.run(validate_select_query())
    asyncio.run(validate_update_query())
    asyncio.run(validate_delete_query())
    asyncio.run(validate_create_temp_query())
    asyncio.run(validate_compare_query())
    asyncio.run(validate_engine())
