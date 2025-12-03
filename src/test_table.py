import asyncio, time, unittest, datetime
from inspect import iscoroutinefunction
from sqlcycli import errors as sqlerr, sqlfunc, Pool, Connection
from mysqlengine import errors
from mysqlengine.database import Database
from mysqlengine.table import Table, Tables, TempTable
from mysqlengine.index import Index, FullTextIndex, Indexes
from mysqlengine.column import Define, Definition, Column, Columns
from mysqlengine.partition import Partitioning, Partition, Partitions
from mysqlengine.constraint import Constraints, PrimaryKey, UniqueKey, ForeignKey, Check


class TestCase(unittest.TestCase):
    name: str = "Case"
    unix_socket: str = None
    db: str = "test"
    tb: str = "test_table"
    dtypes: tuple[Definition] = (
        # Integer
        Define.TINYINT(),
        Define.SMALLINT(),
        Define.MEDIUMINT(),
        Define.INT(),
        Define.BIGINT(),
        # Floating Point
        Define.FLOAT(),
        Define.DOUBLE(),
        # Fixed Point
        Define.DECIMAL(),
        # Temporal
        Define.DATE(),
        Define.DATETIME(),
        Define.TIMESTAMP(),
        Define.TIME(),
        Define.YEAR(),
        # Character String
        Define.CHAR(),
        Define.VARCHAR(255),
        Define.TINYTEXT(),
        Define.TEXT(),
        Define.MEDIUMTEXT(),
        Define.LONGTEXT(),
        Define.ENUM("a", "b", "c"),
        # Binary String
        Define.BINARY(),
        Define.VARBINARY(255),
        Define.TINYBLOB(),
        Define.BLOB(),
        Define.MEDIUMBLOB(),
        Define.LONGBLOB(),
    )

    def __init__(
        self,
        host: str = "localhost",
        port: int = 3306,
        user: str = "root",
        password: str = "password",
    ) -> None:
        super().__init__("runTest")
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self._start_time = None
        self._ended_time = None
        self._pool = None

    @property
    def table(self) -> str:
        if self.db is not None and self.tb is not None:
            return f"{self.db}.{self.tb}"
        return None

    def test_all(self) -> None:
        pass

    # utils
    def log_start(self, msg: str) -> None:
        msg = "START TEST '%s': %s" % (self.name, msg)
        print(msg.ljust(60), end="\r")
        self._start_time = time.perf_counter()

    def log_ended(self, msg: str, skip: bool = False) -> None:
        self._ended_time = time.perf_counter()
        msg = "%s TEST '%s': %s" % ("SKIP" if skip else "PASS", self.name, msg)
        if self._start_time is not None:
            msg += " (%.6fs)" % (self._ended_time - self._start_time)
        print(msg.ljust(60))

    def setup_table(self, tb: Table, name: str = "tb1") -> Table:
        tb.set_name(name)
        tb.setup("db1", "utf8", None, self.get_pool())
        return tb

    # sql
    def get_conn(self, **kwargs) -> Connection:
        conn = Connection(
            host=self.host,
            user=self.user,
            password=self.password,
            unix_socket=self.unix_socket,
            local_infile=True,
            **kwargs,
        )
        conn.connect()
        return conn

    def get_pool(self, **kwargs) -> Pool:
        if self._pool is None:
            self._pool = Pool(
                host=self.host,
                user=self.user,
                password=self.password,
                unix_socket=self.unix_socket,
                local_infile=True,
                autocommit=False,
                **kwargs,
                min_size=1,
            )
        return self._pool

    def setup(self, table: str = None, **kwargs) -> Connection:
        conn = self.get_conn(**kwargs)
        tb = self.tb if table is None else table
        with conn.cursor() as cur:
            cur.execute(f"CREATE DATABASE IF NOT EXISTS {self.db};")
            cur.execute(f"DROP TABLE IF EXISTS {self.db}.{tb}")
        return conn

    def drop(self, conn: Connection, table: str = None) -> None:
        tb = self.tb if table is None else table
        with conn.cursor() as cur:
            cur.execute(f"drop table if exists {self.db}.{tb}")

    def delete(self, conn: Connection, table: str = None) -> None:
        tb = self.tb if table is None else table
        with conn.cursor() as cur:
            cur.execute(f"delete from {self.db}.{tb}")


class TestTableDefinition(TestCase):
    name: str = "Table Definition"

    # Tests
    def test_all(self) -> None:
        self.test_options()
        self.test_table_columns()
        self.test_table_index()
        self.test_table_constraints()
        self.test_table_partitioning()
        self.test_table_all_elements()
        self.test_tables()

    def test_options(self) -> None:
        self.log_start("OPTIONS")

        class TestTable(Table):
            id: Column = Column(Define.BIGINT())

        # fmt: off
        # . engine
        self.assertEqual(self.setup_table(TestTable(engine="innodb")).engine, "InnoDB")
        with self.assertRaises(errors.TableDefinitionError):
            self.setup_table(TestTable(engine="innodb1"))

        # . charset
        self.assertEqual(self.setup_table(TestTable(charset="utf8")).charset.name, "utf8mb4")
        with self.assertRaises(errors.TableDefinitionError):
            self.setup_table(TestTable(charset="utf8x"))
        self.assertEqual(
            self.setup_table(TestTable(collate="utf8mb4_bin")).charset.collation, "utf8mb4_bin")
        with self.assertRaises(errors.TableDefinitionError):
            self.setup_table(TestTable(collate="utf8mb4_bin1"))
        self.assertEqual(
            self.setup_table(TestTable(charset="utf8", collate="utf8mb4_bin")).charset.collation, "utf8mb4_bin")

        # . comment
        self.assertIsNone(self.setup_table(TestTable()).comment)
        self.assertEqual(self.setup_table(TestTable(comment="Comment")).comment, "Comment")
        with self.assertRaises(TypeError):
            self.setup_table(TestTable(comment=1))

        # . encryption
        self.assertIsNone(self.setup_table(TestTable()).encryption)
        self.assertTrue(self.setup_table(TestTable(encryption=True)).encryption)
        self.assertTrue(self.setup_table(TestTable(encryption=1)).encryption)
        self.assertTrue(self.setup_table(TestTable(encryption="Y")).encryption)
        self.assertFalse(self.setup_table(TestTable(encryption=False)).encryption)
        self.assertFalse(self.setup_table(TestTable(encryption=0)).encryption)
        self.assertFalse(self.setup_table(TestTable(encryption="n")).encryption)
        with self.assertRaises(errors.TableDefinitionError):
            self.setup_table(TestTable(encryption="x"))

        # . row_format
        self.assertIsNone(self.setup_table(TestTable()).row_format)
        self.assertEqual(self.setup_table(TestTable(row_format="compact")).row_format, "COMPACT")
        self.assertEqual(self.setup_table(TestTable(row_format="COMPRESSED")).row_format, "COMPRESSED")
        with self.assertRaises(errors.TableDefinitionError):
            self.setup_table(TestTable(row_format="COMPACT1"))
        # fmt: on

        self.log_ended("OPTIONS")

    def test_table_columns(self) -> None:
        self.log_start("TABLE COLUMNS")

        class MyTable(Table):
            id: Column = Column(Define.BIGINT())
            user_name: Column = Column(Define.VARCHAR(255))
            height: Column = Column(Define.DECIMAL(5, 2))
            weight: Column = Column(Define.DOUBLE())
            birthday: Column = Column(Define.DATE())
            create_time: Column = Column(Define.DATETIME(auto_init=True))
            update_time: Column = Column(
                Define.TIMESTAMP(auto_init=True, auto_update=True)
            )

        tb = MyTable(
            engine="innoDB",
            comment="My Table",
            row_format="COMPRESSED",
        )
        tb = self.setup_table(tb)
        self.assertEqual(
            tb._gen_create_sql(False),
            "CREATE TABLE db1.tb1 (\n"
            "\tid BIGINT NOT NULL,\n"
            "\tuser_name VARCHAR(255) NOT NULL COLLATE utf8mb4_general_ci,\n"
            "\theight DECIMAL(5,2) NOT NULL,\n"
            "\tweight DOUBLE NOT NULL,\n"
            "\tbirthday DATE NOT NULL,\n"
            "\tcreate_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
            "\tupdate_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP\n)\n"
            "ENGINE InnoDB\n"
            "CHARACTER SET utf8mb4\n"
            "COLLATE utf8mb4_general_ci\n"
            "COMMENT 'My Table'\n"
            "ROW_FORMAT COMPRESSED;",
        )
        self.assertEqual(
            tb._gen_create_sql(True),
            "CREATE TABLE IF NOT EXISTS db1.tb1 (\n"
            "\tid BIGINT NOT NULL,\n"
            "\tuser_name VARCHAR(255) NOT NULL COLLATE utf8mb4_general_ci,\n"
            "\theight DECIMAL(5,2) NOT NULL,\n"
            "\tweight DOUBLE NOT NULL,\n"
            "\tbirthday DATE NOT NULL,\n"
            "\tcreate_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
            "\tupdate_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP\n)\n"
            "ENGINE InnoDB\n"
            "CHARACTER SET utf8mb4\n"
            "COLLATE utf8mb4_general_ci\n"
            "COMMENT 'My Table'\n"
            "ROW_FORMAT COMPRESSED;",
        )

        # encryption
        class MyTable2(Table):
            id: Column = Column(Define.BIGINT())

        tb = self.setup_table(
            MyTable2(
                engine="innoDB",
                comment="My Table",
                encryption=True,
            )
        )
        self.assertEqual(
            tb._gen_create_sql(False),
            "CREATE TABLE db1.tb1 (\n"
            "\tid BIGINT NOT NULL\n)\n"
            "ENGINE InnoDB\n"
            "CHARACTER SET utf8mb4\n"
            "COLLATE utf8mb4_general_ci\n"
            "COMMENT 'My Table'\n"
            "ENCRYPTION 'Y';",
        )

        class MyTable3(Table):
            id: Column = Column(Define.BIGINT())

        tb = self.setup_table(
            MyTable3(
                engine="innoDB",
                comment="My Table",
                encryption=False,
            )
        )
        self.assertEqual(
            tb._gen_create_sql(False),
            "CREATE TABLE db1.tb1 (\n"
            "\tid BIGINT NOT NULL\n)\n"
            "ENGINE InnoDB\n"
            "CHARACTER SET utf8mb4\n"
            "COLLATE utf8mb4_general_ci\n"
            "COMMENT 'My Table'\n"
            "ENCRYPTION 'N';",
        )

        # Test charset
        # utf8
        class MyTable4(Table):
            id: Column = Column(Define.BIGINT())

        tb = self.setup_table(MyTable4(charset="utf8"))
        self.assertEqual(tb.charset.name, "utf8mb4")
        self.assertEqual(tb.charset.collation, "utf8mb4_general_ci")
        self.assertEqual(
            tb._gen_create_sql(False),
            "CREATE TABLE db1.tb1 (\n"
            "\tid BIGINT NOT NULL\n)\n"
            "CHARACTER SET utf8mb4\n"
            "COLLATE utf8mb4_general_ci;",
        )

        # collate: utf8mb4_bin
        class MyTable4(Table):
            id: Column = Column(Define.BIGINT())

        tb = self.setup_table(MyTable4(collate="utf8mb4_bin"))
        self.assertEqual(tb.charset.name, "utf8mb4")
        self.assertEqual(tb.charset.collation, "utf8mb4_bin")
        self.assertEqual(
            tb._gen_create_sql(False),
            "CREATE TABLE db1.tb1 (\n"
            "\tid BIGINT NOT NULL\n)\n"
            "CHARACTER SET utf8mb4\n"
            "COLLATE utf8mb4_bin;",
        )

        # charset: utf8mb3
        class MyTable5(Table):
            id: Column = Column(Define.BIGINT())

        tb = self.setup_table(MyTable5(charset="utf8mb3"))
        self.assertEqual(tb.charset.name, "utf8mb3")
        self.assertEqual(tb.charset.collation, "utf8mb3_general_ci")
        self.assertEqual(
            tb._gen_create_sql(False),
            "CREATE TABLE db1.tb1 (\n"
            "\tid BIGINT NOT NULL\n)\n"
            "CHARACTER SET utf8mb3\n"
            "COLLATE utf8mb3_general_ci;",
        )

        # different encoding
        class MyTable6(Table):
            id: Column = Column(Define.BIGINT())

        with self.assertRaises(errors.TableDefinitionError):
            tb = self.setup_table(MyTable6(charset="latin1"))

        self.log_ended("TABLE COLUMNS")

    def test_table_index(self) -> None:
        self.log_start("TABLE INDEXES")

        class MyTable(Table):
            id: Column = Column(Define.BIGINT())
            user_name: Column = Column(Define.VARCHAR(255))
            height: Column = Column(Define.DECIMAL(5, 2))
            weight: Column = Column(Define.DOUBLE())
            description: Column = Column(Define.VARCHAR(5000))
            idxUserName: Index = Index("user_name")
            idxMeasure: Index = Index("height", "weight")
            idxTDesc: FullTextIndex = FullTextIndex(
                "description", comment="Full Text Indexed"
            )

        tb = self.setup_table(MyTable())
        self.assertEqual(
            tb._gen_create_sql(False),
            "CREATE TABLE db1.tb1 (\n"
            "\tid BIGINT NOT NULL,\n"
            "\tuser_name VARCHAR(255) NOT NULL COLLATE utf8mb4_general_ci,\n"
            "\theight DECIMAL(5,2) NOT NULL,\n"
            "\tweight DOUBLE NOT NULL,\n"
            "\tdescription VARCHAR(5000) NOT NULL COLLATE utf8mb4_general_ci,\n"
            "\tINDEX idxUserName (user_name),\n"
            "\tINDEX idxMeasure (height, weight),\n"
            "\tFULLTEXT INDEX idxTDesc (description) COMMENT 'Full Text Indexed'\n)\n"
            "CHARACTER SET utf8mb4\n"
            "COLLATE utf8mb4_general_ci;",
        )
        self.assertIsInstance(tb.indexes, Indexes)
        self.assertIs(tb.idxUserName, tb.indexes["idxUserName"])

        self.log_ended("TABLE INDEXES")

    def test_table_constraints(self) -> None:
        self.log_start("TABLE CONSTRAINTS")

        class MyTable(Table):
            id: Column = Column(Define.BIGINT())
            user_name: Column = Column(Define.VARCHAR(255))
            height: Column = Column(Define.DECIMAL(5, 2))
            weight: Column = Column(Define.DOUBLE())
            birthday: Column = Column(Define.DATE())
            create_time: Column = Column(Define.DATETIME(auto_init=True))
            update_time: Column = Column(
                Define.TIMESTAMP(auto_init=True, auto_update=True)
            )
            pk_key: PrimaryKey = PrimaryKey("id")
            uk_key: UniqueKey = UniqueKey("user_name")
            fk_key: ForeignKey = ForeignKey(
                "user_name", "user", "user_name", "CASCADE", "CASCADE"
            )
            ck1: Check = Check("height > 0")

        tb = self.setup_table(MyTable())
        self.assertEqual(
            tb._gen_create_sql(False),
            "CREATE TABLE db1.tb1 (\n"
            "\tid BIGINT NOT NULL,\n"
            "\tuser_name VARCHAR(255) NOT NULL COLLATE utf8mb4_general_ci,\n"
            "\theight DECIMAL(5,2) NOT NULL,\n"
            "\tweight DOUBLE NOT NULL,\n"
            "\tbirthday DATE NOT NULL,\n"
            "\tcreate_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
            "\tupdate_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n"
            "\tCONSTRAINT pk_key PRIMARY KEY (id),\n"
            "\tCONSTRAINT uk_key UNIQUE KEY (user_name),\n"
            "\tCONSTRAINT tb1_fk_key FOREIGN KEY (user_name) REFERENCES db1.user (user_name) ON DELETE CASCADE ON UPDATE CASCADE,\n"
            "\tCONSTRAINT tb1_ck1 CHECK (height > 0)\n)\n"
            "CHARACTER SET utf8mb4\n"
            "COLLATE utf8mb4_general_ci;",
        )
        self.assertIsInstance(tb.constraints, Constraints)
        self.assertIs(tb.pk_key, tb.constraints["pk_key"])
        self.assertIs(tb.uk_key, tb.constraints["uk_key"])
        self.assertIs(tb.fk_key, tb.constraints["fk_key"])
        self.assertIs(tb.ck1, tb.constraints["ck1"])
        self.log_ended("TABLE CONSTRAINTS")

    def test_table_partitioning(self) -> None:
        self.log_start("TABLE PARTITIONING")

        class MyTable(Table):
            id: Column = Column(Define.BIGINT())
            user_name: Column = Column(Define.VARCHAR(255))
            height: Column = Column(Define.DECIMAL(5, 2))
            weight: Column = Column(Define.DOUBLE())
            birthday: Column = Column(Define.DATE())
            create_time: Column = Column(Define.DATETIME(auto_init=True))
            update_time: Column = Column(
                Define.TIMESTAMP(auto_init=True, auto_update=True)
            )
            pt: Partitioning = Partitioning(sqlfunc.TO_DAYS("create_time")).by_range(
                Partition("p0", sqlfunc.TO_DAYS("2021-01-01")),
                Partition("p1", sqlfunc.TO_DAYS("2022-01-01")),
                Partition("p2", "MAXVALUE"),
            )

        tb = self.setup_table(MyTable())
        self.assertEqual(
            tb._gen_create_sql(False),
            "CREATE TABLE db1.tb1 (\n"
            "\tid BIGINT NOT NULL,\n"
            "\tuser_name VARCHAR(255) NOT NULL COLLATE utf8mb4_general_ci,\n"
            "\theight DECIMAL(5,2) NOT NULL,\n"
            "\tweight DOUBLE NOT NULL,\n"
            "\tbirthday DATE NOT NULL,\n"
            "\tcreate_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
            "\tupdate_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP\n)\n"
            "CHARACTER SET utf8mb4\n"
            "COLLATE utf8mb4_general_ci\n"
            "PARTITION BY RANGE (TO_DAYS(create_time)) (\n"
            "\tPARTITION p0 VALUES LESS THAN (TO_DAYS('2021-01-01')),\n"
            "\tPARTITION p1 VALUES LESS THAN (TO_DAYS('2022-01-01')),\n"
            "\tPARTITION p2 VALUES LESS THAN (MAXVALUE)\n);",
        )
        self.assertIsInstance(tb.partitioning, Partitioning)
        self.assertIs(tb.pt, tb.partitioning)
        self.assertIsInstance(tb.pt.partitions, Partitions)
        self.assertIsInstance(tb.pt.partitions["p0"], Partition)

        class MyTable(Table):
            id: Column = Column(Define.BIGINT())
            user_name: Column = Column(Define.VARCHAR(255))
            height: Column = Column(Define.DECIMAL(5, 2))
            weight: Column = Column(Define.DOUBLE())
            birthday: Column = Column(Define.DATE())
            create_time: Column = Column(Define.DATETIME(auto_init=True))
            update_time: Column = Column(
                Define.TIMESTAMP(auto_init=True, auto_update=True)
            )
            pt: Partitioning = Partitioning(sqlfunc.TO_DAYS("birthday")).by_hash(
                4, True
            )

        tb = self.setup_table(MyTable())
        self.assertEqual(
            tb._gen_create_sql(False),
            "CREATE TABLE db1.tb1 (\n"
            "\tid BIGINT NOT NULL,\n"
            "\tuser_name VARCHAR(255) NOT NULL COLLATE utf8mb4_general_ci,\n"
            "\theight DECIMAL(5,2) NOT NULL,\n"
            "\tweight DOUBLE NOT NULL,\n"
            "\tbirthday DATE NOT NULL,\n"
            "\tcreate_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
            "\tupdate_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP\n)\n"
            "CHARACTER SET utf8mb4\n"
            "COLLATE utf8mb4_general_ci\n"
            "PARTITION BY LINEAR HASH (TO_DAYS(birthday)) PARTITIONS 4;",
        )
        self.assertIsInstance(tb.partitioning, Partitioning)
        self.assertIs(tb.pt, tb.partitioning)
        self.assertIsInstance(tb.pt.partitions, Partitions)
        self.assertEqual(len(tb.pt.partitions), 4)

        class MyTable(Table):
            id: Column = Column(Define.BIGINT())
            user_name: Column = Column(Define.VARCHAR(255))
            height: Column = Column(Define.DECIMAL(5, 2))
            weight: Column = Column(Define.DOUBLE())
            birthday: Column = Column(Define.DATE())
            create_time: Column = Column(Define.DATETIME(auto_init=True))
            update_time: Column = Column(
                Define.TIMESTAMP(auto_init=True, auto_update=True)
            )
            pt: Partitioning = (
                Partitioning(sqlfunc.TO_DAYS("create_time"))
                .by_range(
                    Partition("p0", sqlfunc.TO_DAYS("2021-01-01")),
                    Partition("p1", sqlfunc.TO_DAYS("2022-01-01")),
                    Partition("p2", "MAXVALUE"),
                )
                .subpartition_by_key(4, "user_name")
            )

        tb = self.setup_table(MyTable())
        self.assertEqual(
            tb._gen_create_sql(False),
            "CREATE TABLE db1.tb1 (\n"
            "\tid BIGINT NOT NULL,\n"
            "\tuser_name VARCHAR(255) NOT NULL COLLATE utf8mb4_general_ci,\n"
            "\theight DECIMAL(5,2) NOT NULL,\n"
            "\tweight DOUBLE NOT NULL,\n"
            "\tbirthday DATE NOT NULL,\n"
            "\tcreate_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
            "\tupdate_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP\n)\n"
            "CHARACTER SET utf8mb4\n"
            "COLLATE utf8mb4_general_ci\n"
            "PARTITION BY RANGE (TO_DAYS(create_time))\n"
            "SUBPARTITION BY KEY (user_name) SUBPARTITIONS 4 (\n"
            "\tPARTITION p0 VALUES LESS THAN (TO_DAYS('2021-01-01')),\n"
            "\tPARTITION p1 VALUES LESS THAN (TO_DAYS('2022-01-01')),\n"
            "\tPARTITION p2 VALUES LESS THAN (MAXVALUE)\n);",
        )
        self.assertIsInstance(tb.partitioning, Partitioning)
        self.assertIs(tb.pt, tb.partitioning)
        self.assertIsInstance(tb.pt.partitions, Partitions)
        self.assertIsInstance(tb.pt.partitions["p0"], Partition)
        self.assertIsInstance(tb.pt.partitions, Partitions)
        self.assertEqual(len(tb.pt.partitions), 3)
        self.assertIsInstance(tb.pt.partitions["p0"].subpartitions, Partitions)
        self.assertEqual(len(tb.pt.partitions["p0"].subpartitions), 4)

        self.log_ended("TABLE PARTITIONING")

    def test_table_all_elements(self) -> None:
        self.log_start("TABLE ALL ELEMENTS")

        class MyTable(Table):
            id: Column = Column(Define.BIGINT())
            user_name: Column = Column(Define.VARCHAR(255))
            height: Column = Column(Define.DECIMAL(5, 2))
            weight: Column = Column(Define.DOUBLE())
            birthday: Column = Column(Define.DATE())
            create_time: Column = Column(Define.DATETIME(auto_init=True))
            update_time: Column = Column(
                Define.TIMESTAMP(auto_init=True, auto_update=True)
            )
            idx1: Index = Index("user_name")
            idx2: Index = Index("height", "weight")
            chk1: Check = Check("height > 0")
            chk2: Check = Check("weight > 0")
            pt: Partitioning = Partitioning(sqlfunc.TO_DAYS("create_time")).by_range(
                Partition("p0", sqlfunc.TO_DAYS("2021-01-01")),
                Partition("p1", sqlfunc.TO_DAYS("2022-01-01")),
                Partition("p2", "MAXVALUE"),
            )

        tb = self.setup_table(
            MyTable(
                engine="innoDB",
                comment="My Table",
                row_format="COMPRESSED",
            )
        )
        self.assertEqual(
            tb._gen_create_sql(True),
            "CREATE TABLE IF NOT EXISTS db1.tb1 (\n"
            "\tid BIGINT NOT NULL,\n"
            "\tuser_name VARCHAR(255) NOT NULL COLLATE utf8mb4_general_ci,\n"
            "\theight DECIMAL(5,2) NOT NULL,\n"
            "\tweight DOUBLE NOT NULL,\n"
            "\tbirthday DATE NOT NULL,\n"
            "\tcreate_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
            "\tupdate_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n"
            "\tINDEX idx1 (user_name),\n"
            "\tINDEX idx2 (height, weight),\n"
            "\tCONSTRAINT tb1_chk1 CHECK (height > 0),\n"
            "\tCONSTRAINT tb1_chk2 CHECK (weight > 0)\n)\n"
            "ENGINE InnoDB\n"
            "CHARACTER SET utf8mb4\n"
            "COLLATE utf8mb4_general_ci\n"
            "COMMENT 'My Table'\n"
            "ROW_FORMAT COMPRESSED\nPARTITION BY RANGE (TO_DAYS(create_time)) (\n"
            "\tPARTITION p0 VALUES LESS THAN (TO_DAYS('2021-01-01')),\n"
            "\tPARTITION p1 VALUES LESS THAN (TO_DAYS('2022-01-01')),\n"
            "\tPARTITION p2 VALUES LESS THAN (MAXVALUE)\n);",
        )
        self.assertIsInstance(tb.columns, Columns)
        self.assertIsInstance(tb.indexes, Indexes)
        self.assertIsInstance(tb.constraints, Constraints)
        self.assertIsInstance(tb.partitioning, Partitioning)

        self.log_ended("TABLE ALL ELEMENTS")

    def test_tables(self) -> None:
        self.log_start("TABLES")

        class Table1(Table):
            id_1: Column = Column(Define.BIGINT())
            user_name_1: Column = Column(Define.VARCHAR(255))
            height_1: Column = Column(Define.DECIMAL(5, 2))
            weight_1: Column = Column(Define.DOUBLE())
            birthday_1: Column = Column(Define.DATE())
            create_time_1: Column = Column(Define.DATETIME(auto_init=True))
            update_time_1: Column = Column(
                Define.TIMESTAMP(auto_init=True, auto_update=True)
            )
            idx1_1: Index = Index("user_name")
            idx2_1: Index = Index("height", "weight")
            chk1_1: Check = Check("height > 0")
            chk2_1: Check = Check("weight > 0")

        class Table2(Table):
            id_2: Column = Column(Define.BIGINT())
            user_name_2: Column = Column(Define.VARCHAR(255))
            height_2: Column = Column(Define.DECIMAL(5, 2))
            weight_2: Column = Column(Define.DOUBLE())
            birthday_2: Column = Column(Define.DATE())
            create_time_2: Column = Column(Define.DATETIME(auto_init=True))
            update_time_2: Column = Column(
                Define.TIMESTAMP(auto_init=True, auto_update=True)
            )
            idx1_2: Index = Index("user_name")
            idx2_2: Index = Index("height", "weight")
            chk1_2: Check = Check("height > 0")
            chk2_2: Check = Check("weight > 0")

        tb1 = self.setup_table(Table1(), "tb1")
        tb2 = self.setup_table(Table2(), "tb2")
        tbs = Tables(tb1, tb2)
        self.assertEqual(len(tbs.search_name("tb1")), 1)
        self.assertEqual(len(tbs.search_name(["tb1", "tb2"])), 2)
        self.assertEqual(len(tbs.search_name("tb", exact=False)), 2)
        self.assertEqual(len(tbs.search_name("xxx", exact=False)), 0)
        self.assertEqual(len(tbs.search_type("TABLE")), 2)
        self.assertEqual(len(tbs.search_type("TABL")), 0)
        self.assertEqual(len(tbs.search_type("table", exact=False)), 2)
        self.assertEqual(len(tbs.search_type(Table1)), 1)
        self.assertEqual(len(tbs.search_type(tb1)), 1)
        self.assertEqual(len(tbs.search_type([Table1, Table2])), 2)
        self.assertEqual(len(tbs.search_type([tb1, tb2])), 2)
        self.assertEqual(len(tbs.search_type(Table)), 0)
        self.assertEqual(len(tbs.search_type(Table, exact=False)), 2)
        self.assertEqual(len(tbs.search_type(["TABLE", Table1])), 2)
        self.assertEqual(len(tbs.search_type(["table", tb1])), 2)
        # self.assertEqual(len(tbs.search_column("id_1")), 1)
        # self.assertEqual(len(tbs.search_column("id_1", "id_2")), 2)
        # self.assertEqual(len(tbs.search_column("id", exact=False)), 2)
        # self.assertEqual(len(tbs.search_column("xxx", exact=False)), 0)
        # self.assertEqual(len(tbs.search_index("idx1_1")), 1)
        # self.assertEqual(len(tbs.search_index("idx1_1", "idx2_2")), 2)
        # self.assertEqual(len(tbs.search_index("idx", exact=False)), 2)
        # self.assertEqual(len(tbs.search_index("xxx", exact=False)), 0)
        # self.assertEqual(len(tbs.search_constraint("chk1_1")), 1)
        # self.assertEqual(len(tbs.search_constraint("chk1_1", "chk2_2")), 2)
        # self.assertEqual(len(tbs.search_constraint("chk", exact=False)), 2)
        # self.assertEqual(len(tbs.search_constraint("xxx", exact=False)), 0)
        self.assertTrue(tbs.issubset(["tb1", tb2, tbs]))
        self.assertFalse(tbs.issubset("tb1x"))
        self.assertEqual(len(tbs.filter("tbx")), 0)
        self.assertEqual(len(tbs.filter("tb1")), 1)
        self.assertEqual(len(tbs.filter(["tb1", tb2])), 2)
        self.assertEqual(len(tbs.filter(tbs)), len(tbs))
        self.assertIs(tbs.get("tb1"), tbs["tb1"])
        self.assertIs(tbs.get("tb1"), tb1)
        self.assertIs(tbs.get("tbx"), None)
        for tb in tbs:
            self.assertIsInstance(tb, Table)
        self.assertIn("tb1", tbs)
        self.assertIn(tb2, tbs)
        self.assertNotIn("tbx", tbs)

        # Conflicts
        with self.assertRaises(errors.TableArgumentError):
            Tables(None)
        with self.assertRaises(errors.TableArgumentError):
            Tables(tb1, tb1)

        self.log_ended("TABLES")


class TestTableCopy(TestCase):
    name = "Table Copy"

    def test_all(self) -> None:
        self.log_start("COPY")

        class TestTable(Table):
            id: Column = Column(Define.BIGINT())

        class TestDatabase(Database):
            tb1: TestTable = TestTable()
            tb2: TestTable = TestTable()

        db = TestDatabase("test_db", self.get_pool())
        self.assertIsNot(db.tb1, db.tb2)
        self.assertIsNot(db["tb1"], db["tb2"])

        self.log_ended("COPY")


class TestTableSyncSQL(TestCase):
    name: str = "Table Sync SQL"

    def test_all(self) -> None:
        self.test_table_basic_sql()
        self.test_table_sync_from_remote()
        self.test_table_initialize()

    def test_table_basic_sql(self) -> None:
        self.log_start("TABLE BASIC SQL")

        class User(Table):
            id: Column = Column(Define.INT())
            name: Column = Column(Define.VARCHAR(255))

        class TestDatabase(Database):
            user: User = User(
                engine="InnoDB",
                comment="Comment",
                encryption=False,
                row_format="COMPRESSED",
            )

        db = TestDatabase("test_db", self.get_pool())
        db.Drop(True)
        db.Create()

        # Create & Exists & Drop
        self.assertTrue(db.user.Drop(True))
        self.assertFalse(db.user.Exists())
        self.assertTrue(db.user.Create())
        self.assertTrue(db.user.Exists())
        with self.assertRaises(sqlerr.OperationalError):
            self.assertTrue(db.user.Create())
        self.assertTrue(db.user.Drop())
        with self.assertRaises(sqlerr.OperationalError):
            db.user.Drop()

        # Show metadata
        with self.assertRaises(sqlerr.OperationalError):
            db.user.ShowMetadata()
        self.assertTrue(db.user.Create())
        meta = db.user.ShowMetadata()
        self.assertEqual(meta["SCHEMA_NAME"], db.db_name)
        self.assertEqual(meta.db_name, db.db_name)
        self.assertEqual(meta["TABLE_NAME"], db.user.tb_name)
        self.assertEqual(meta.tb_name, db.user.tb_name)
        self.assertEqual(meta["ENGINE"], "InnoDB")
        self.assertEqual(meta.engine, "InnoDB")
        self.assertEqual(meta["TABLE_COMMENT"], "Comment")
        self.assertEqual(meta.comment, "Comment")
        self.assertEqual(meta["ROW_FORMAT"], "COMPRESSED")
        self.assertEqual(meta.row_format, "COMPRESSED")
        self.assertEqual(meta.charset.name, "utf8mb4")
        self.assertEqual(meta.charset.collation, "utf8mb4_general_ci")
        self.assertFalse(meta.encryption)
        self.assertTrue(db.user.Drop())

        # Empty / Truncate Table
        self.assertTrue(db.user.Create())
        self.assertTrue(db.user.Empty())
        with db.acquire() as conn:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO %s VALUES (1, 'Name')" % db.user)
            conn.commit()
        self.assertFalse(db.user.Empty())
        self.assertTrue(db.user.Truncate())
        self.assertTrue(db.user.Empty())

        # Analyze / Check / Optimze / Repair
        # . analyze
        for boolean in [True, False]:
            res = db.user.Analyze(boolean)
            self.assertTrue(len(res) > 0)
            self.assertEqual(res[0]["Op"], "analyze")
        # . check
        opts = [
            (None,),
            ("FOR UPGRADE",),
            ("Quick",),
            ("fast",),
            ("MEDIUM",),
            ("EXTENDED",),
            ("CHANGED",),
            ("QUICK", "fast"),
        ]
        for opt in opts:
            res = db.user.Check(*opt)
            self.assertTrue(len(res) > 0)
            self.assertEqual(res[0]["Op"], "check")
        with self.assertRaises(errors.TableArgumentError):
            res = db.user.Check("upgrade")
        # . optimize
        for boolean in [True, False]:
            res = db.user.Optimize(boolean)
            self.assertTrue(len(res) > 0)
            self.assertEqual(res[0]["Op"], "optimize")
        # . repair
        for opt in (None, "QUICK", "Extended", "USE_FRM"):
            for boolean in [True, False]:
                res = db.user.Repair(boolean, opt)
                self.assertTrue(len(res) > 0)
                self.assertEqual(res[0]["Op"], "repair")
        with self.assertRaises(errors.TableArgumentError):
            db.user.Repair(True, "extends")
        # . drop
        self.assertTrue(db.user.Drop())

        # Alter
        self.assertTrue(db.user.Initialize())
        # . no alteration
        self.assertFalse(db.user.Alter())
        # . alter engine & encryption
        self.assertEqual(db.user.engine, "InnoDB")
        self.assertFalse(db.user.encryption)
        self.assertTrue(db.user.Alter(engine="MyISAM", encryption=False))
        self.assertEqual(db.user.engine, "MyISAM")
        self.assertFalse(db.user.encryption)
        meta = db.user.ShowMetadata()
        self.assertEqual(meta.engine, "MyISAM")
        self.assertFalse(meta.encryption)
        self.assertTrue(
            db.user.Alter(engine="InnoDB", encryption=False, row_format="COMPRESSED")
        )
        # . alter charset
        self.assertEqual(db.user.charset.name, "utf8mb4")
        self.assertTrue(db.user.Alter(charset="utf8mb3"))
        self.assertEqual(db.user.charset.name, "utf8mb3")
        self.assertEqual(db.user.ShowMetadata().charset.name, "utf8mb3")
        with self.assertRaises(errors.TableDefinitionError):
            db.user.Alter(charset="utf8x")
        with self.assertRaises(errors.TableDefinitionError):
            db.user.Alter(charset="latin1")
        # . alter collate
        self.assertEqual(db.user.charset.collation, "utf8mb3_general_ci")
        self.assertTrue(db.user.Alter(collate="utf8mb3_bin"))
        self.assertEqual(db.user.charset.collation, "utf8mb3_bin")
        self.assertEqual(db.user.ShowMetadata().charset.collation, "utf8mb3_bin")
        # . alter comment
        self.assertEqual(db.user.comment, "Comment")
        self.assertTrue(db.user.Alter(comment="New Comment"))
        self.assertEqual(db.user.comment, "New Comment")
        self.assertEqual(db.user.ShowMetadata().comment, "New Comment")
        self.assertTrue(db.user.Alter(comment=""))
        self.assertIsNone(db.user.comment)
        self.assertIsNone(db.user.ShowMetadata().comment)
        # . alter row format
        self.assertEqual(db.user.row_format, "COMPRESSED")
        self.assertTrue(db.user.Alter(row_format="Dynamic"))
        self.assertEqual(db.user.row_format, "DYNAMIC")
        self.assertEqual(db.user.ShowMetadata().row_format, "DYNAMIC")
        # . alter all
        self.assertTrue(
            db.user.Alter(
                engine="InnoDB",
                charset="utf8mb4",
                collate="utf8mb4_general_ci",
                comment="Comment",
                row_format="COMPRESSED",
            )
        )
        self.assertEqual(db.user.engine, "InnoDB")
        self.assertEqual(db.user.charset.name, "utf8mb4")
        self.assertEqual(db.user.charset.collation, "utf8mb4_general_ci")
        self.assertEqual(db.user.comment, "Comment")
        self.assertFalse(db.user.encryption)
        self.assertEqual(db.user.row_format, "COMPRESSED")
        meta = db.user.ShowMetadata()
        self.assertEqual(meta.engine, "InnoDB")
        self.assertEqual(meta.charset.name, "utf8mb4")
        self.assertEqual(meta.charset.collation, "utf8mb4_general_ci")
        self.assertEqual(meta.comment, "Comment")
        self.assertFalse(meta.encryption)
        self.assertEqual(meta.row_format, "COMPRESSED")

        # . alter local-remote mismatch
        class User(Table):
            id: Column = Column(Define.INT())
            name: Column = Column(Define.VARCHAR(255))

        class TestDatabase2(Database):
            user: User = User(engine="MyISAM", row_format="FIXED")

        db2 = TestDatabase2("test_db", self.get_pool())
        self.assertEqual(db2.user.engine, "MyISAM")
        self.assertFalse(db2.user.encryption)
        self.assertEqual(db2.user.row_format, "FIXED")
        log = db2.user.Alter(engine="InnoDB", row_format="COMPRESSED")
        self.assertIn("(local) row_format: 'FIXED' => 'COMPRESSED'", repr(log))
        self.assertNotIn("(server) ENGINE: 'MyISAM' => 'InnoDB'", repr(log))
        self.assertNotIn("(server) row_format: 'FIXED' => 'COMPRESSED'", repr(log))
        self.assertTrue(db.user.Drop())

        # Lock / Unlock Table
        select_sql = "SELECT * FROM %s" % db.user
        self.assertTrue(db.user.Create())
        # lock for read
        with self.get_conn() as conn2:
            conn2.set_execution_timeout(100)  # timeout 100ms
            with db.acquire() as conn:
                db.user.Lock(conn, True)  # lock
                with conn2.cursor() as cur:
                    cur.execute(select_sql)  # should raise no error
                db.user.Unlock(conn)  # unlock
        # lock for write
        with self.get_conn() as conn3:
            conn3.set_execution_timeout(100)  # timeout 100ms
            with db.acquire() as conn:
                db.user.Lock(conn, False)  # lock
                with conn3.cursor() as cur:
                    with self.assertRaises(sqlerr.OperationalTimeoutError):
                        cur.execute(select_sql)  # raise timeout error
                db.user.Unlock(conn)  # unlock
                with conn3.cursor() as cur:
                    cur.execute(select_sql)  # should raise no error

        # Sync to remote
        self.assertTrue(db.user.Drop())
        self.assertTrue(db.user.SyncToRemote())

        class User(Table):
            id: Column = Column(Define.INT())
            name: Column = Column(Define.VARCHAR(255))

        class TestDatabase3(Database):
            user: User = User(
                engine="MyISAM",
                charset="utf8mb3",
                collate="utf8mb3_bin",
                comment="New Comment",
                encryption=False,
                row_format="FIXED",
            )

        db3 = TestDatabase3("test_db", self.get_pool())
        self.assertEqual(db3.user.engine, "MyISAM")
        self.assertEqual(db3.user.charset.name, "utf8mb3")
        self.assertEqual(db3.user.charset.collation, "utf8mb3_bin")
        self.assertEqual(db3.user.comment, "New Comment")
        self.assertFalse(db3.user.encryption)
        self.assertEqual(db3.user.row_format, "FIXED")
        self.assertTrue(db3.user.SyncToRemote())
        meta = db3.user.ShowMetadata()
        self.assertEqual(meta.engine, "MyISAM")
        self.assertEqual(meta.charset.name, "utf8mb3")
        self.assertEqual(meta.charset.collation, "utf8mb3_bin")
        self.assertEqual(meta.comment, "New Comment")
        self.assertFalse(meta.encryption)
        self.assertEqual(meta.row_format, "FIXED")
        self.assertTrue(db.user.Drop())

        # Finished
        self.assertTrue(db.Drop(True))
        self.log_ended("TABLE BASIC SQL")

    def test_table_sync_from_remote(self) -> None:
        self.log_start("TABLE SYNC FROM REMOTE")

        class RangeTable(Table):
            id: Column = Column(Define.BIGINT(auto_increment=True))
            name: Column = Column(Define.VARCHAR(255))
            dt: Column = Column(Define.DATETIME())
            pk: PrimaryKey = PrimaryKey("id", "dt")
            uk: UniqueKey = UniqueKey("name", "dt")
            pt: Partitioning = Partitioning(sqlfunc.TO_DAYS("dt")).by_range(
                Partition("start", 0),
                Partition("from201201", sqlfunc.TO_DAYS("2012-02-01")),
                Partition("from201202", sqlfunc.TO_DAYS("2012-03-01")),
                Partition("from201203", sqlfunc.TO_DAYS("2012-04-01")),
            )

        class TestDatabase(Database):
            tb: RangeTable = RangeTable()

        db = TestDatabase("test_db", self.get_pool())
        db.Drop(True)
        db.Create()

        import warnings

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            # fmt: off
            logs = db.tb.SyncFromRemote()
            self.assertIn("TABLE 'test_db.tb' (local) failed to sync from the remote server: it does NOT EXIST", repr(logs))
            self.assertTrue(db.tb.Create())
            self.assertFalse(db.tb.SyncFromRemote())
            logs = db.tb.SyncFromRemote(True)
            self.assertIn("COLUMN BIGINT 'id' in 'test_db.tb' (local) primary_key: False => True", repr(logs))
            self.assertIn("COLUMN DATETIME 'dt' in 'test_db.tb' (local) primary_key: False => True", repr(logs))
            self.assertIn("CONSTRAINT PRIMARY KEY 'pk' in 'test_db.tb' (local) index_type: None => 'BTREE'", repr(logs))
            self.assertIn("PARTITION 'start' in 'test_db.tb' (local) values: (0,) => '0'", repr(logs))
            db.tb.uk.Drop()
            logs = db.tb.SyncFromRemote(True)
            self.assertIn("CONSTRAINT UNIQUE KEY 'uk' in 'test_db.tb' (local) failed to sync from the remote server: it does NOT EXIST", repr(logs))
            db.tb.partitioning.Remove()
            logs = db.tb.SyncFromRemote(True)
            self.assertIn("CONSTRAINT UNIQUE KEY 'uk' in 'test_db.tb' (local) failed to sync from the remote server: it does NOT EXIST", repr(logs))
            self.assertIn("PARTITION BY RANGE in 'test_db.tb' (local) failed to sync from the remote server: it does NOT EXIST", repr(logs))
            # fmt: on
            self.assertEqual(len(caught), 4)
            warnings.simplefilter("ignore")

        # Finished
        db.tb.Drop(True)
        self.log_ended("TABLE SYNC FROM REMOTE")

    def test_table_initialize(self) -> None:
        self.log_start("TABLE INITIALIZE")

        # Initialization
        class User(Table):
            id: Column = Column(Define.INT())
            name: Column = Column(Define.VARCHAR(255))

        class TestDatabase(Database):
            user: User = User()

        db = TestDatabase(
            "test_db",
            self.get_pool(),
            charset="utf8mb4",
            collate="utf8mb4_general_ci",
        )

        db.Drop(True)
        db.Create()
        logs = db.user.Initialize()
        self.assertIn("(local) encryption: None => False", repr(logs))
        self.assertIn("(local) row_format: None => 'DYNAMIC'", repr(logs))
        self.assertFalse(db.user.Initialize())

        # sync from remote
        class User2(Table):
            id: Column = Column(Define.INT())
            name: Column = Column(Define.VARCHAR(255))

        class TestDatabase2(Database):
            user: User2 = User2(
                engine="MyISAM",
                charset="utf8mb3",
                collate="utf8mb3_bin",
                comment="New Comment",
                encryption=False,
                row_format="FIXED",
            )

        db2 = TestDatabase2(
            "test_db",
            self.get_pool(),
            charset="utf8mb4",
            collate="utf8mb4_general_ci",
        )
        logs = db2.user.Initialize()
        # fmt: off
        self.assertIn("(local) engine: 'MyISAM' => 'InnoDB'", repr(logs))
        self.assertIn("(local) charset: 'utf8mb3' => 'utf8mb4'", repr(logs))
        self.assertIn("(local) collate: 'utf8mb3_bin' => 'utf8mb4_general_ci'", repr(logs))
        self.assertIn("(local) comment: 'New Comment' => None", repr(logs))
        self.assertIn("(local) row_format: 'FIXED' => 'DYNAMIC'", repr(logs))
        self.assertFalse(db2.user.Initialize(force=False))
        self.assertFalse(db2.user.Initialize(force=True))
        # fmt: on

        # Finished
        self.assertTrue(db.Drop(True))
        self.log_ended("TABLE INITIALIZE")


class TestTableAsyncSQL(TestCase):
    name: str = "Table Async SQL"

    async def test_all(self) -> None:
        await self.test_table_basic_sql()
        await self.test_table_sync_from_remote()
        await self.test_table_initialize()

    async def test_table_basic_sql(self) -> None:
        self.log_start("TABLE BASIC SQL")

        class User(Table):
            id: Column = Column(Define.INT())
            name: Column = Column(Define.VARCHAR(255))

        class TestDatabase(Database):
            user: User = User(
                engine="InnoDB",
                comment="Comment",
                row_format="COMPRESSED",
            )

        db = TestDatabase("test_db", self.get_pool())
        await db.aioDrop(True)
        await db.aioCreate()

        # Create & Exists & Drop
        self.assertTrue(await db.user.aioDrop(True))
        self.assertFalse(await db.user.aioExists())
        self.assertTrue(await db.user.aioCreate())
        self.assertTrue(await db.user.aioExists())
        with self.assertRaises(sqlerr.OperationalError):
            self.assertTrue(await db.user.aioCreate())
        self.assertTrue(await db.user.aioDrop())
        with self.assertRaises(sqlerr.OperationalError):
            await db.user.aioDrop()

        # Show metadata
        with self.assertRaises(sqlerr.OperationalError):
            await db.user.aioShowMetadata()
        self.assertTrue(await db.user.aioCreate())
        meta = await db.user.aioShowMetadata()
        self.assertEqual(meta["SCHEMA_NAME"], db.db_name)
        self.assertEqual(meta.db_name, db.db_name)
        self.assertEqual(meta["TABLE_NAME"], db.user.tb_name)
        self.assertEqual(meta.tb_name, db.user.tb_name)
        self.assertEqual(meta["ENGINE"], "InnoDB")
        self.assertEqual(meta.engine, "InnoDB")
        self.assertEqual(meta["TABLE_COMMENT"], "Comment")
        self.assertEqual(meta.comment, "Comment")
        self.assertEqual(meta["ROW_FORMAT"], "COMPRESSED")
        self.assertEqual(meta.row_format, "COMPRESSED")
        self.assertEqual(meta.charset.name, "utf8mb4")
        self.assertEqual(meta.charset.collation, "utf8mb4_general_ci")
        self.assertFalse(meta.encryption)
        self.assertTrue(await db.user.aioDrop())

        # Empty / Truncate Table
        self.assertTrue(await db.user.aioCreate())
        self.assertTrue(await db.user.aioEmpty())
        async with db.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("INSERT INTO %s VALUES (1, 'Name')" % db.user)
            await conn.commit()
        self.assertFalse(await db.user.aioEmpty())
        self.assertEqual(len(await db.user.aioOptimize()), 2)
        self.assertTrue(await db.user.aioTruncate())
        self.assertTrue(await db.user.aioEmpty())

        # Analyze / Check / Optimze / Repair
        # . analyze
        for boolean in [True, False]:
            res = await db.user.aioAnalyze(boolean)
            self.assertTrue(len(res) > 0)
            self.assertEqual(res[0]["Op"], "analyze")
        # . check
        opts = [
            (None,),
            ("FOR UPGRADE",),
            ("Quick",),
            ("fast",),
            ("MEDIUM",),
            ("EXTENDED",),
            ("CHANGED",),
            ("QUICK", "fast"),
        ]
        for opt in opts:
            res = await db.user.aioCheck(*opt)
            self.assertTrue(len(res) > 0)
            self.assertEqual(res[0]["Op"], "check")
        with self.assertRaises(errors.TableArgumentError):
            res = await db.user.aioCheck("upgrade")
        # . optimize
        for boolean in [True, False]:
            res = await db.user.aioOptimize(boolean)
            self.assertTrue(len(res) > 0)
            self.assertEqual(res[0]["Op"], "optimize")
        # . repair
        for opt in (None, "QUICK", "Extended", "USE_FRM"):
            for boolean in [True, False]:
                res = await db.user.aioRepair(boolean, opt)
                self.assertTrue(len(res) > 0)
                self.assertEqual(res[0]["Op"], "repair")
        with self.assertRaises(errors.TableArgumentError):
            await db.user.aioRepair(True, "extends")
        # . drop
        self.assertTrue(await db.user.aioDrop())

        # Alter
        self.assertTrue(await db.user.aioInitialize())
        # . no alteration
        self.assertFalse(await db.user.aioAlter())
        # . alter engine & encryption
        self.assertEqual(db.user.engine, "InnoDB")
        self.assertFalse(db.user.encryption)
        self.assertTrue(await db.user.aioAlter(engine="MyISAM", encryption=False))
        self.assertEqual(db.user.engine, "MyISAM")
        self.assertFalse(db.user.encryption)
        meta = await db.user.aioShowMetadata()
        self.assertEqual(meta.engine, "MyISAM")
        self.assertFalse(meta.encryption)
        self.assertTrue(
            await db.user.aioAlter(
                engine="InnoDB", encryption=False, row_format="COMPRESSED"
            )
        )
        # . alter charset
        self.assertEqual(db.user.charset.name, "utf8mb4")
        self.assertTrue(await db.user.aioAlter(charset="utf8mb3"))
        self.assertEqual(db.user.charset.name, "utf8mb3")
        self.assertEqual((await db.user.aioShowMetadata()).charset.name, "utf8mb3")
        with self.assertRaises(errors.TableDefinitionError):
            await db.user.aioAlter(charset="utf8x")
        with self.assertRaises(errors.TableDefinitionError):
            await db.user.aioAlter(charset="latin1")
        # . alter collate
        self.assertEqual(db.user.charset.collation, "utf8mb3_general_ci")
        self.assertTrue(await db.user.aioAlter(collate="utf8mb3_bin"))
        self.assertEqual(db.user.charset.collation, "utf8mb3_bin")
        self.assertEqual(
            (await db.user.aioShowMetadata()).charset.collation, "utf8mb3_bin"
        )
        # . alter comment
        self.assertEqual(db.user.comment, "Comment")
        self.assertTrue(await db.user.aioAlter(comment="New Comment"))
        self.assertEqual(db.user.comment, "New Comment")
        self.assertEqual((await db.user.aioShowMetadata()).comment, "New Comment")
        self.assertTrue(await db.user.aioAlter(comment=""))
        self.assertIsNone(db.user.comment)
        self.assertIsNone((await db.user.aioShowMetadata()).comment)
        # . alter row format
        self.assertEqual(db.user.row_format, "COMPRESSED")
        self.assertTrue(await db.user.aioAlter(row_format="Dynamic"))
        self.assertEqual(db.user.row_format, "DYNAMIC")
        self.assertEqual((await db.user.aioShowMetadata()).row_format, "DYNAMIC")
        # . alter all
        self.assertTrue(
            await db.user.aioAlter(
                engine="InnoDB",
                charset="utf8mb4",
                collate="utf8mb4_general_ci",
                comment="Comment",
                row_format="COMPRESSED",
            )
        )
        self.assertEqual(db.user.engine, "InnoDB")
        self.assertEqual(db.user.charset.name, "utf8mb4")
        self.assertEqual(db.user.charset.collation, "utf8mb4_general_ci")
        self.assertEqual(db.user.comment, "Comment")
        self.assertFalse(db.user.encryption)
        self.assertEqual(db.user.row_format, "COMPRESSED")
        meta = await db.user.aioShowMetadata()
        self.assertEqual(meta.engine, "InnoDB")
        self.assertEqual(meta.charset.name, "utf8mb4")
        self.assertEqual(meta.charset.collation, "utf8mb4_general_ci")
        self.assertEqual(meta.comment, "Comment")
        self.assertFalse(meta.encryption)
        self.assertEqual(meta.row_format, "COMPRESSED")

        # . alter local-remote mismatch
        class User(Table):
            id: Column = Column(Define.INT())
            name: Column = Column(Define.VARCHAR(255))

        class TestDatabase2(Database):
            user: User = User(
                engine="MyISAM",
                encryption=False,
                row_format="FIXED",
            )

        db2 = TestDatabase2("test_db", self.get_pool())
        self.assertEqual(db2.user.engine, "MyISAM")
        self.assertFalse(db2.user.encryption)
        self.assertEqual(db2.user.row_format, "FIXED")
        log = await db2.user.aioAlter(engine="InnoDB", row_format="COMPRESSED")
        self.assertIn("(local) row_format: 'FIXED' => 'COMPRESSED'", repr(log))
        self.assertNotIn("(server) engine: 'MyISAM' => 'InnoDB'", repr(log))
        self.assertNotIn("(server) row_format: 'FIXED' => 'COMPRESSED'", repr(log))
        self.assertTrue(await db.user.aioDrop())

        # Lock / Unlock Table
        select_sql = "SELECT * FROM %s" % db.user
        self.assertTrue(await db.user.aioCreate())
        # lock for read
        with self.get_conn() as conn2:
            conn2.set_execution_timeout(100)  # timeout 100ms
            async with db.acquire() as conn:
                await db.user.aioLock(conn, True)  # lock
                with conn2.cursor() as cur:
                    cur.execute(select_sql)  # should raise no error
                await db.user.aioUnlock(conn)  # unlock
        # lock for write
        with self.get_conn() as conn3:
            conn3.set_execution_timeout(100)  # timeout 100ms
            async with db.acquire() as conn:
                await db.user.aioLock(conn, False)
                with conn3.cursor() as cur:
                    with self.assertRaises(sqlerr.OperationalTimeoutError):
                        cur.execute(select_sql)  # raise timeout error
                await db.user.aioUnlock(conn)  # unlock
                with conn3.cursor() as cur:
                    cur.execute(select_sql)  # should raise no error

        # Sync to remote
        self.assertTrue(await db.user.aioDrop())
        self.assertTrue(await db.user.aioSyncToRemote())  # re-create

        class User(Table):
            id: Column = Column(Define.INT())
            name: Column = Column(Define.VARCHAR(255))

        class TestDatabase3(Database):
            user: User = User(
                engine="MyISAM",
                charset="utf8mb3",
                collate="utf8mb3_bin",
                comment="New Comment",
                encryption=False,
                row_format="FIXED",
            )

        db3 = TestDatabase3("test_db", self.get_pool())
        self.assertEqual(db3.user.engine, "MyISAM")
        self.assertEqual(db3.user.charset.name, "utf8mb3")
        self.assertEqual(db3.user.charset.collation, "utf8mb3_bin")
        self.assertEqual(db3.user.comment, "New Comment")
        self.assertFalse(db3.user.encryption)
        self.assertEqual(db3.user.row_format, "FIXED")
        self.assertTrue(await db3.user.aioSyncToRemote())
        meta = await db3.user.aioShowMetadata()
        self.assertEqual(meta.engine, "MyISAM")
        self.assertEqual(meta.charset.name, "utf8mb3")
        self.assertEqual(meta.charset.collation, "utf8mb3_bin")
        self.assertEqual(meta.comment, "New Comment")
        self.assertFalse(meta.encryption)
        self.assertEqual(meta.row_format, "FIXED")
        self.assertTrue(await db.user.aioDrop())

        # Finished
        self.assertTrue(await db.aioDrop(True))
        self.log_ended("TABLE BASIC SQL")

    async def test_table_sync_from_remote(self) -> None:
        self.log_start("TABLE SYNC FROM REMOTE")

        class RangeTable(Table):
            id: Column = Column(Define.BIGINT(auto_increment=True))
            name: Column = Column(Define.VARCHAR(255))
            dt: Column = Column(Define.DATETIME())
            pk: PrimaryKey = PrimaryKey("id", "dt")
            uk: UniqueKey = UniqueKey("name", "dt")
            pt: Partitioning = Partitioning(sqlfunc.TO_DAYS("dt")).by_range(
                Partition("start", 0),
                Partition("from201201", sqlfunc.TO_DAYS("2012-02-01")),
                Partition("from201202", sqlfunc.TO_DAYS("2012-03-01")),
                Partition("from201203", sqlfunc.TO_DAYS("2012-04-01")),
            )

        class TestDatabase(Database):
            tb: RangeTable = RangeTable()

        db = TestDatabase("test_db", self.get_pool())
        await db.aioDrop(True)
        await db.aioCreate()

        import warnings

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            # fmt: off
            logs = await db.tb.aioSyncFromRemote()
            self.assertIn("TABLE 'test_db.tb' (local) failed to sync from the remote server: it does NOT EXIST", repr(logs))
            self.assertTrue(await db.tb.aioCreate())
            self.assertFalse(await db.tb.aioSyncFromRemote())
            logs = await db.tb.aioSyncFromRemote(True)
            self.assertIn("COLUMN BIGINT 'id' in 'test_db.tb' (local) primary_key: False => True", repr(logs))
            self.assertIn("COLUMN DATETIME 'dt' in 'test_db.tb' (local) primary_key: False => True", repr(logs))
            self.assertIn("CONSTRAINT PRIMARY KEY 'pk' in 'test_db.tb' (local) index_type: None => 'BTREE'", repr(logs))
            self.assertIn("PARTITION 'start' in 'test_db.tb' (local) values: (0,) => '0'", repr(logs))
            await db.tb.uk.aioDrop()
            logs = await db.tb.aioSyncFromRemote(True)
            self.assertIn("CONSTRAINT UNIQUE KEY 'uk' in 'test_db.tb' (local) failed to sync from the remote server: it does NOT EXIST", repr(logs))
            await db.tb.partitioning.aioRemove()
            logs = await db.tb.aioSyncFromRemote(True)
            self.assertIn("CONSTRAINT UNIQUE KEY 'uk' in 'test_db.tb' (local) failed to sync from the remote server: it does NOT EXIST", repr(logs))
            self.assertIn("PARTITION BY RANGE in 'test_db.tb' (local) failed to sync from the remote server: it does NOT EXIST", repr(logs))
            # fmt: on
            self.assertEqual(len(caught), 4)
            warnings.simplefilter("ignore")

        # Finished
        await db.tb.aioDrop(True)
        self.log_ended("TABLE SYNC FROM REMOTE")

    async def test_table_initialize(self) -> None:
        self.log_start("TABLE INITIALIZE")

        # Initialization
        class User(Table):
            id: Column = Column(Define.INT())
            name: Column = Column(Define.VARCHAR(255))

        class TestDatabase(Database):
            user: User = User()

        db = TestDatabase(
            "test_db",
            self.get_pool(),
            charset="utf8mb4",
            collate="utf8mb4_general_ci",
        )

        await db.aioDrop(True)
        await db.aioCreate()
        logs = await db.user.aioInitialize()
        self.assertIn("(local) encryption: None => False", repr(logs))
        self.assertIn("(local) row_format: None => 'DYNAMIC'", repr(logs))
        self.assertFalse(await db.user.aioInitialize())

        # sync from remote
        class User2(Table):
            id: Column = Column(Define.INT())
            name: Column = Column(Define.VARCHAR(255))

        class TestDatabase2(Database):
            user: User2 = User2(
                engine="MyISAM",
                charset="utf8mb3",
                collate="utf8mb3_bin",
                comment="New Comment",
                encryption=False,
                row_format="FIXED",
            )

        db2 = TestDatabase2(
            "test_db",
            self.get_pool(),
            charset="utf8mb4",
            collate="utf8mb4_general_ci",
        )
        logs = await db2.user.aioInitialize()
        # fmt: off
        self.assertIn("(local) engine: 'MyISAM' => 'InnoDB'", repr(logs))
        self.assertIn("(local) charset: 'utf8mb3' => 'utf8mb4'", repr(logs))
        self.assertIn("(local) collate: 'utf8mb3_bin' => 'utf8mb4_general_ci'", repr(logs))
        self.assertIn("(local) comment: 'New Comment' => None", repr(logs))
        self.assertIn("(local) row_format: 'FIXED' => 'DYNAMIC'", repr(logs))
        self.assertFalse(await db2.user.aioInitialize(force=False))
        self.assertFalse(await db2.user.aioInitialize(force=True))
        # fmt: on

        # Finished
        self.assertTrue(await db.aioDrop(True))
        self.log_ended("TABLE INITIALIZE")


class TestTempTableSQL(TestCase):
    name: str = "Temp Table SQL"
    TEST_DATA: tuple = (
        (1, "a0", 1.0, datetime.datetime(2012, 1, 1, 0, 0)),
        (2, "a1", 1.1, datetime.datetime(2012, 1, 1, 0, 0)),
        (3, "b0", 2.0, datetime.datetime(2012, 2, 1, 0, 0)),
        (4, "b1", 2.1, datetime.datetime(2012, 2, 1, 0, 0)),
        (5, "c1", 3.0, datetime.datetime(2012, 3, 1, 0, 0)),
        (6, "c2", 3.1, datetime.datetime(2012, 3, 1, 0, 0)),
    )

    async def test_all(self) -> None:
        await self.test_temp_table()

    async def test_temp_table(self) -> None:
        self.log_start("TEMPORARY TABLE SQL")

        class TestTable(Table):
            id: Column = Column(Define.BIGINT(auto_increment=True))
            name: Column = Column(Define.VARCHAR(255))
            price: Column = Column(Define.DECIMAL(12, 2))
            dt: Column = Column(Define.DATETIME())
            pk: PrimaryKey = PrimaryKey("id", "dt")

        class TestDatabase(Database):
            tb: TestTable = TestTable()

        class TestTempTable(TempTable):
            id: Column = Column(Define.BIGINT(auto_increment=True))
            name: Column = Column(Define.VARCHAR(255))
            price: Column = Column(Define.DECIMAL(12, 2))
            dt: Column = Column(Define.DATETIME())
            pk: PrimaryKey = PrimaryKey("id", "dt")

        db = TestDatabase("test_db", self.get_pool())
        db.Initialize()

        for name in ("tb", "tmp"):
            # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
            with db.transaction() as conn:
                with db.CreateTempTable(conn, name, TestTempTable()) as tmp:
                    self.assertEqual(str(tmp), "test_db." + name)
                    self.assertTrue(tmp.Empty())
                    with self.assertRaises(errors.TableCriticalError):
                        await tmp.aioEmpty()
                    self.assertEqual(
                        tmp.Insert().Values(4).Execute(self.TEST_DATA, True), 6
                    )
                    with self.assertRaises(errors.DMLCriticalError):
                        await tmp.Insert().Values(4).aioExecute(self.TEST_DATA, True)
                    with self.assertRaises(errors.DMLArgumentError):
                        await tmp.Insert().Values(4).aioExecute(
                            self.TEST_DATA, True, conn=conn
                        )
                    self.assertFalse(tmp.Empty())
                    self.assertEqual(tmp.Select("*").Execute(), self.TEST_DATA)
                    with self.assertRaises(errors.DMLCriticalError):
                        await tmp.Select("*").aioExecute()
                    with self.assertRaises(errors.DMLArgumentError):
                        await tmp.Select("*").aioExecute(conn=conn)
                with self.assertRaises(errors.TableCriticalError):
                    tmp.Empty()

            # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
            with db.transaction() as conn:
                with self.assertRaises(errors.TableCriticalError):
                    async with db.CreateTempTable(conn, name, TestTempTable()) as tmp:
                        pass
                with self.assertRaises(errors.TableArgumentError):
                    with db.CreateTempTable(1, name, TestTempTable()) as tmp:
                        pass
                with db.CreateTempTable(conn, name, TestTempTable()) as tmp:
                    tmp.Drop()
                    with self.assertRaises(sqlerr.OperationalError):
                        tmp.Drop()

            # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
            async with db.transaction() as conn:
                async with db.CreateTempTable(conn, name, TestTempTable()) as tmp:
                    self.assertEqual(str(tmp), "test_db." + name)
                    self.assertTrue(await tmp.aioEmpty())
                    with self.assertRaises(errors.TableCriticalError):
                        tmp.Empty()
                    self.assertEqual(
                        await tmp.Insert().Values(4).aioExecute(self.TEST_DATA, True), 6
                    )
                    with self.assertRaises(errors.DMLCriticalError):
                        tmp.Insert().Values(4).Execute(self.TEST_DATA, True)
                    with self.assertRaises(errors.DMLArgumentError):
                        tmp.Insert().Values(4).Execute(self.TEST_DATA, True, conn=conn)
                    self.assertFalse(await tmp.aioEmpty())
                    self.assertEqual(await tmp.Select("*").aioExecute(), self.TEST_DATA)
                    with self.assertRaises(errors.DMLCriticalError):
                        tmp.Select("*").Execute()
                    with self.assertRaises(errors.DMLArgumentError):
                        tmp.Select("*").Execute(conn=conn)
                with self.assertRaises(errors.TableCriticalError):
                    await tmp.aioEmpty()

            # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
            async with db.transaction() as conn:
                with self.assertRaises(errors.TableCriticalError):
                    with db.CreateTempTable(conn, name, TestTempTable()) as tmp:
                        pass
                with self.assertRaises(errors.TableArgumentError):
                    async with db.CreateTempTable(1, name, TestTempTable()) as tmp:
                        pass
                async with db.CreateTempTable(conn, name, TestTempTable()) as tmp:
                    await tmp.aioDrop()
                    with self.assertRaises(sqlerr.OperationalError):
                        await tmp.aioDrop()

            # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
            async with db.transaction() as conn:
                with self.assertRaises(errors.TableArgumentError):
                    async with db.CreateTempTable(1, name, TestTempTable()) as tmp:
                        pass
                with self.assertRaises(errors.DatabaseArgumentError):
                    async with db.CreateTempTable(conn, name, TempTable()) as tmp:
                        pass
                with self.assertRaises(errors.DatabaseArgumentError):
                    async with db.CreateTempTable(conn, name, Table()) as tmp:
                        pass

        # Finished
        self.assertTrue(db.Drop(True))
        self.log_ended("TEMPORARY TABLE SQL")


if __name__ == "__main__":
    HOST = "localhost"
    PORT = 3306
    USER = "root"
    PSWD = "Password_123456"

    for case in (
        TestTableDefinition,
        TestTableCopy,
        TestTableSyncSQL,
        TestTableAsyncSQL,
        TestTempTableSQL,
    ):
        test_case = case(HOST, PORT, USER, PSWD)
        if not iscoroutinefunction(test_case.test_all):
            test_case.test_all()
        else:
            asyncio.run(test_case.test_all())
