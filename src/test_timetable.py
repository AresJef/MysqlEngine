import asyncio, time, unittest, datetime
from inspect import iscoroutinefunction
from cytimes import Pydt
from sqlcycli import sqlfunc, Pool, Connection
from mysqlengine.column import Define, Definition, Column, Columns
from mysqlengine.constraint import Constraint, PrimaryKey, UniqueKey
from mysqlengine import errors
from mysqlengine.index import Index
from mysqlengine.database import Database
from mysqlengine.table import Table, TimeTable
from mysqlengine.partition import Partitioning, Partition


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

    def setup_column(self, col: Column, name: str = "col") -> Column:
        col._set_position(1)
        col.setup(name, "tb1", "db1", "utf8", None, self.get_pool())
        return col

    def setup_constraint(self, cnst: Constraint, name: str = "cnst") -> Constraint:
        cnst.setup(name, "tb1", "db1", "utf8", None, self.get_pool())
        return cnst

    def setup_index(self, idx: Index, name: str = "idx") -> Index:
        idx.setup(name, "tb1", "db1", "utf8", None, self.get_pool())
        return idx

    def setup_partitioning(self, pt: Partitioning) -> Partitioning:
        pt.setup("tb1", "db1", "utf8", None, self.get_pool())
        return pt

    def setup_table(self, tb: Table, name: str = "tb1") -> Table:
        tb.setup(name, "db1", "utf8", None, self.get_pool())
        return tb

    def gen_columns(self) -> Columns:
        return Columns(
            *[
                self.setup_column(Column(d), f"col_{d.data_type.lower()}")
                for d in self.dtypes
            ]
        )

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


class TestTimeTable(TestCase):
    name: str = "TimeTable"

    async def test_all(self) -> None:
        await self.test_initialize()
        await self.test_manipulate()
        await self.test_manipulate_other_units()
        await self.test_with_data()
        await self.test_insert_auto_init()
        await self.test_insert_auto_update()

    async def test_initialize(self) -> None:
        self.log_start("TEST INITIALIZE")

        # Errors
        class TestTable(TimeTable):
            id: Column = Column(Define.BIGINT(auto_increment=True))
            name: Column = Column(Define.VARCHAR(255))
            price: Column = Column(Define.DECIMAL(12, 2))
            dt: Column = Column(Define.DATETIME())

        with self.assertRaises(errors.TableDefinitionError):

            class TestDatabase(Database):
                tb: TestTable = TestTable("dtx", "H", "2023-01-01", None)

            TestDatabase("db", self.get_pool())

        with self.assertRaises(errors.TableDefinitionError):

            class TestDatabase(Database):
                tb: TestTable = TestTable("name", "H", "2023-01-01", None)

            TestDatabase("db", self.get_pool())

        with self.assertRaises(errors.TableDefinitionError):

            class TestDatabase(Database):
                tb: TestTable = TestTable("dt", "x", "2023-01-01", None)

            TestDatabase("db", self.get_pool())

        with self.assertRaises(errors.TableDefinitionError):

            class TestDatabase(Database):
                tb: TestTable = TestTable("dt", "H", "", None)

            TestDatabase("db", self.get_pool())

        with self.assertRaises(errors.TableDefinitionError):

            class TestDatabase(Database):
                tb: TestTable = TestTable("dt", "H", "2023-01-01", "2023")

            TestDatabase("db", self.get_pool())

        with self.assertRaises(errors.TableDefinitionError):

            class TestTable(TimeTable):
                id: Column = Column(Define.BIGINT(auto_increment=True))
                name: Column = Column(Define.VARCHAR(255))
                price: Column = Column(Define.DECIMAL(12, 2))
                dt: Column = Column(Define.DATETIME())
                pk: PrimaryKey = PrimaryKey("id")

            class TestDatabase(Database):
                tb: TestTable = TestTable("dt", "Y", "2023-01-01", None)

            TestDatabase("db", self.get_pool())

        with self.assertRaises(errors.TableDefinitionError):

            class TestTable(TimeTable):
                id: Column = Column(Define.BIGINT(auto_increment=True))
                name: Column = Column(Define.VARCHAR(255))
                price: Column = Column(Define.DECIMAL(12, 2))
                dt: Column = Column(Define.DATETIME())
                uk: UniqueKey = UniqueKey("name")

            class TestDatabase(Database):
                tb: TestTable = TestTable("dt", "Y", "2023-01-01", None)

            TestDatabase("db", self.get_pool())

        with self.assertRaises(errors.TableDefinitionError):

            class TestTable(TimeTable):
                id: Column = Column(Define.BIGINT(auto_increment=True))
                name: Column = Column(Define.VARCHAR(255))
                price: Column = Column(Define.DECIMAL(12, 2))
                dt: Column = Column(Define.DATETIME())
                pt: Partitioning = Partitioning(sqlfunc.TO_DAYS("dt")).by_range(
                    Partition("start", 0),
                    Partition("from201201", sqlfunc.TO_DAYS("2012-02-01")),
                    Partition("from201202", sqlfunc.TO_DAYS("2012-03-01")),
                    Partition("from201203", sqlfunc.TO_DAYS("2012-04-01")),
                )

            class TestDatabase(Database):
                tb: TestTable = TestTable("dt", "Y", "2023-01-01", None)

            TestDatabase("db", self.get_pool())

        # [Y] Year
        class TestTable(TimeTable):
            id: Column = Column(Define.BIGINT(auto_increment=True))
            name: Column = Column(Define.VARCHAR(255))
            price: Column = Column(Define.DECIMAL(12, 2))
            dt: Column = Column(Define.DATETIME())
            pk: PrimaryKey = PrimaryKey("id", "dt")
            uk: UniqueKey = UniqueKey("name", "price", "dt")

        class TestDatabase(Database):
            tb: TestTable = TestTable("dt", "Y", "2023-03-15", "2025-06-15")

        db = TestDatabase("db", self.get_pool())
        db.Drop(True)
        await db.aioInitialize()
        pts = db.tb.partitioning
        self.assertEqual(len(pts), 5)
        self.assertIn("past", pts)
        self.assertEqual(pts["past"].values, "'2023-01-01 00:00:00'")
        self.assertIn("y2023", pts)
        self.assertEqual(pts["y2023"].values, "'2024-01-01 00:00:00'")
        self.assertIn("y2024", pts)
        self.assertEqual(pts["y2024"].values, "'2025-01-01 00:00:00'")
        self.assertIn("y2025", pts)
        self.assertEqual(pts["y2025"].values, "'2026-01-01 00:00:00'")
        self.assertIn("future", pts)
        self.assertEqual(pts["future"].values, "MAXVALUE")

        # [Q] Quarter
        class TestDatabase(Database):
            tb: TestTable = TestTable("dt", "Q", "2024-12-15", "2025-06-15")

        db = TestDatabase("db", self.get_pool())
        db.Drop(True)
        await db.aioInitialize()
        pts = db.tb.partitioning
        self.assertEqual(len(pts), 5)
        self.assertIn("past", pts)
        self.assertEqual(pts["past"].values, "'2024-10-01 00:00:00'")
        self.assertIn("q20244", pts)
        self.assertEqual(pts["q20244"].values, "'2025-01-01 00:00:00'")
        self.assertIn("q20251", pts)
        self.assertEqual(pts["q20251"].values, "'2025-04-01 00:00:00'")
        self.assertIn("q20252", pts)
        self.assertEqual(pts["q20252"].values, "'2025-07-01 00:00:00'")
        self.assertIn("future", pts)
        self.assertEqual(pts["future"].values, "MAXVALUE")

        # [M] Month
        class TestDatabase(Database):
            tb: TestTable = TestTable("dt", "M", "2025-01-12", "2025-03-15")

        db = TestDatabase("db", self.get_pool())
        db.Drop(True)
        await db.aioInitialize()
        pts = db.tb.partitioning
        self.assertEqual(len(pts), 5)
        self.assertIn("past", pts)
        self.assertEqual(pts["past"].values, "'2025-01-01 00:00:00'")
        self.assertIn("m202501", pts)
        self.assertEqual(pts["m202501"].values, "'2025-02-01 00:00:00'")
        self.assertIn("m202502", pts)
        self.assertEqual(pts["m202502"].values, "'2025-03-01 00:00:00'")
        self.assertIn("m202503", pts)
        self.assertEqual(pts["m202503"].values, "'2025-04-01 00:00:00'")
        self.assertIn("future", pts)
        self.assertEqual(pts["future"].values, "MAXVALUE")

        # [W] Week
        class TestDatabase(Database):
            tb: TestTable = TestTable("dt", "W", "2025-02-25", "2025-03-15")

        db = TestDatabase("db", self.get_pool())
        db.Drop(True)
        await db.aioInitialize()
        pts = db.tb.partitioning
        self.assertEqual(len(pts), 5)
        self.assertIn("past", pts)
        self.assertEqual(pts["past"].values, "'2025-02-24 00:00:00'")
        self.assertIn("w20250224", pts)
        self.assertEqual(pts["w20250224"].values, "'2025-03-03 00:00:00'")
        self.assertIn("w20250303", pts)
        self.assertEqual(pts["w20250303"].values, "'2025-03-10 00:00:00'")
        self.assertIn("w20250310", pts)
        self.assertEqual(pts["w20250310"].values, "'2025-03-17 00:00:00'")
        self.assertIn("future", pts)
        self.assertEqual(pts["future"].values, "MAXVALUE")

        # [D] Day
        class TestDatabase(Database):
            tb: TestTable = TestTable("dt", "D", "2025-03-07 12:12:12", "2025-03-09")

        db = TestDatabase("db", self.get_pool())
        db.Drop(True)
        await db.aioInitialize()
        pts = db.tb.partitioning
        self.assertEqual(len(pts), 5)
        self.assertIn("past", pts)
        self.assertEqual(pts["past"].values, "'2025-03-07 00:00:00'")
        self.assertIn("d20250307", pts)
        self.assertEqual(pts["d20250307"].values, "'2025-03-08 00:00:00'")
        self.assertIn("d20250308", pts)
        self.assertEqual(pts["d20250308"].values, "'2025-03-09 00:00:00'")
        self.assertIn("d20250309", pts)
        self.assertEqual(pts["d20250309"].values, "'2025-03-10 00:00:00'")
        self.assertIn("future", pts)
        self.assertEqual(pts["future"].values, "MAXVALUE")

        # [H] Hour
        class TestDatabase(Database):
            tb: TestTable = TestTable(
                "dt", "h", "2025-03-07 12:12:12", "2025-03-07 14:59:59"
            )

        db = TestDatabase("db", self.get_pool())
        db.Drop(True)
        await db.aioInitialize()
        pts = db.tb.partitioning
        self.assertEqual(len(pts), 5)
        self.assertIn("past", pts)
        self.assertEqual(pts["past"].values, "'2025-03-07 12:00:00'")
        self.assertIn("h20250307_12", pts)
        self.assertEqual(pts["h20250307_12"].values, "'2025-03-07 13:00:00'")
        self.assertIn("h20250307_13", pts)
        self.assertEqual(pts["h20250307_13"].values, "'2025-03-07 14:00:00'")
        self.assertIn("h20250307_14", pts)
        self.assertEqual(pts["h20250307_14"].values, "'2025-03-07 15:00:00'")
        self.assertIn("future", pts)
        self.assertEqual(pts["future"].values, "MAXVALUE")

        # [M] Minute
        class TestDatabase(Database):
            tb: TestTable = TestTable(
                "dt", "i", "2025-03-07 12:12:12", "2025-03-07 12:14:59"
            )

        db = TestDatabase("db", self.get_pool())
        db.Drop(True)
        await db.aioInitialize()
        pts = db.tb.partitioning
        self.assertEqual(len(pts), 5)
        self.assertIn("past", pts)
        self.assertEqual(pts["past"].values, "'2025-03-07 12:12:00'")
        self.assertIn("i20250307_1212", pts)
        self.assertEqual(pts["i20250307_1212"].values, "'2025-03-07 12:13:00'")
        self.assertIn("i20250307_1213", pts)
        self.assertEqual(pts["i20250307_1213"].values, "'2025-03-07 12:14:00'")
        self.assertIn("i20250307_1214", pts)
        self.assertEqual(pts["i20250307_1214"].values, "'2025-03-07 12:15:00'")
        self.assertIn("future", pts)
        self.assertEqual(pts["future"].values, "MAXVALUE")

        # [S] Second
        class TestDatabase(Database):
            tb: TestTable = TestTable(
                "dt", "s", "2025-03-07 12:12:12.111", "2025-03-07 12:12:14.666"
            )

        db = TestDatabase("db", self.get_pool())
        db.Drop(True)
        await db.aioInitialize()
        pts = db.tb.partitioning
        self.assertEqual(len(pts), 5)
        self.assertIn("past", pts)
        self.assertEqual(pts["past"].values, "'2025-03-07 12:12:12'")
        self.assertIn("s20250307_121212", pts)
        self.assertEqual(pts["s20250307_121212"].values, "'2025-03-07 12:12:13'")
        self.assertIn("s20250307_121213", pts)
        self.assertEqual(pts["s20250307_121213"].values, "'2025-03-07 12:12:14'")
        self.assertIn("s20250307_121214", pts)
        self.assertEqual(pts["s20250307_121214"].values, "'2025-03-07 12:12:15'")
        self.assertIn("future", pts)
        self.assertEqual(pts["future"].values, "MAXVALUE")

        # DATE
        with self.assertRaises(errors.TableDefinitionError):

            class TestTable(TimeTable):
                id: Column = Column(Define.BIGINT(auto_increment=True))
                name: Column = Column(Define.VARCHAR(255))
                price: Column = Column(Define.DECIMAL(12, 2))
                dt: Column = Column(Define.DATE())
                pk: PrimaryKey = PrimaryKey("id", "dt")
                uk: UniqueKey = UniqueKey("name", "price", "dt")

            class TestDatabase(Database):
                tb: TestTable = TestTable("dt", "S", "2023-03-15", "2025-06-15")

            TestDatabase("db", self.get_pool())

        class TestTable(TimeTable):
            id: Column = Column(Define.BIGINT(auto_increment=True))
            name: Column = Column(Define.VARCHAR(255))
            price: Column = Column(Define.DECIMAL(12, 2))
            dt: Column = Column(Define.DATE())
            pk: PrimaryKey = PrimaryKey("id", "dt")
            uk: UniqueKey = UniqueKey("name", "price", "dt")

        class TestDatabase(Database):
            tb: TestTable = TestTable("dt", "Y", "2023-03-15", "2025-06-15")

        db = TestDatabase("db", self.get_pool())
        db.Drop(True)
        await db.aioInitialize()
        pts = db.tb.partitioning
        self.assertEqual(len(pts), 5)
        self.assertIn("past", pts)
        self.assertEqual(pts["past"].values, "'2023-01-01 00:00:00'")
        self.assertIn("y2023", pts)
        self.assertEqual(pts["y2023"].values, "'2024-01-01 00:00:00'")
        self.assertIn("y2024", pts)
        self.assertEqual(pts["y2024"].values, "'2025-01-01 00:00:00'")
        self.assertIn("y2025", pts)
        self.assertEqual(pts["y2025"].values, "'2026-01-01 00:00:00'")
        self.assertIn("future", pts)
        self.assertEqual(pts["future"].values, "MAXVALUE")

        # TIMESTMAP
        class TestTable(TimeTable):
            id: Column = Column(Define.BIGINT(auto_increment=True))
            name: Column = Column(Define.VARCHAR(255))
            price: Column = Column(Define.DECIMAL(12, 2))
            dt: Column = Column(Define.TIMESTAMP())
            pk: PrimaryKey = PrimaryKey("id", "dt")
            uk: UniqueKey = UniqueKey("name", "price", "dt")

        class TestDatabase(Database):
            tb: TestTable = TestTable("dt", "Y", "2023-03-15", "2025-06-15")

        db = TestDatabase("db", self.get_pool())
        db.Drop(True)
        await db.aioInitialize()
        pts = db.tb.partitioning
        self.assertEqual(len(pts), 5)
        self.assertIn("past", pts)
        self.assertEqual(pts["past"].values, "1672502400")
        self.assertIn("y2023", pts)
        self.assertEqual(pts["y2023"].values, "1704038400")
        self.assertIn("y2024", pts)
        self.assertEqual(pts["y2024"].values, "1735660800")
        self.assertIn("y2025", pts)
        self.assertEqual(pts["y2025"].values, "1767196800")
        self.assertIn("future", pts)
        self.assertEqual(pts["future"].values, "MAXVALUE")

        # Finished
        db.Drop(True)
        self.log_ended("TEST INITIALIZE")

    async def test_manipulate(self) -> None:
        self.log_start("TEST MANIPULATE")

        class TestTable(TimeTable):
            id: Column = Column(Define.BIGINT(auto_increment=True))
            name: Column = Column(Define.VARCHAR(255))
            price: Column = Column(Define.DECIMAL(12, 2))
            dt: Column = Column(Define.DATETIME())
            pk: PrimaryKey = PrimaryKey("id", "dt")

        class TestDatabase(Database):
            tb: TestTable = TestTable("dt", "Y", "2023-03-15", "2025-06-15")

        db = TestDatabase("db", self.get_pool())
        db.Drop(True)
        await db.aioInitialize()

        # Get bound partition name
        self.assertEqual(db.tb.GetBoundaryPartitionTime(True), Pydt(2025))
        self.assertEqual(await db.tb.aioGetBoundaryPartitionTime(True), Pydt(2025))
        self.assertEqual(db.tb.GetBoundaryPartitionTime(False), Pydt(2023))
        self.assertEqual(await db.tb.aioGetBoundaryPartitionTime(False), Pydt(2023))

        pts = db.tb.partitioning
        # [sync] Extend partition to time
        db.tb.ExtendToTime(start_from="2023-01-01", end_with="2025-01-01")
        self.assertEqual(len(pts), 5)
        self.assertTrue(db.tb.ExtendToTime(end_with="2026-12-31"))
        self.assertEqual(len(pts), 6)
        self.assertIn("y2026", pts)
        self.assertEqual(pts["y2026"].values, "'2027-01-01 00:00:00'")
        self.assertTrue(db.tb.ExtendToTime(end_with="2028-06-30"))
        self.assertEqual(len(pts), 8)
        self.assertIn("y2027", pts)
        self.assertEqual(pts["y2027"].values, "'2028-01-01 00:00:00'")
        self.assertIn("y2028", pts)
        self.assertEqual(pts["y2028"].values, "'2029-01-01 00:00:00'")
        self.assertTrue(db.tb.ExtendToTime(start_from="2022-12-31"))
        self.assertEqual(len(pts), 9)
        self.assertIn("y2022", pts)
        self.assertEqual(pts["y2022"].values, "'2023-01-01 00:00:00'")
        self.assertTrue(db.tb.ExtendToTime(start_from="2020-01-31"))
        self.assertEqual(len(pts), 11)
        self.assertIn("y2020", pts)
        self.assertEqual(pts["y2020"].values, "'2021-01-01 00:00:00'")
        self.assertIn("y2021", pts)
        self.assertEqual(pts["y2021"].values, "'2022-01-01 00:00:00'")
        self.assertTrue(
            db.tb.ExtendToTime(start_from="2019-01-31", end_with="2029-12-31")
        )
        self.assertEqual(len(pts), 13)
        self.assertIn("y2019", pts)
        self.assertEqual(pts["y2019"].values, "'2020-01-01 00:00:00'")
        self.assertIn("y2029", pts)
        self.assertEqual(pts["y2029"].values, "'2030-01-01 00:00:00'")
        self.assertFalse(db.tb.ExtendToTime(start_from="2024-12-31"))
        self.assertFalse(db.tb.ExtendToTime(end_with="2025-12-31"))
        with self.assertRaises(errors.TableDefinitionError):
            db.tb.ExtendToTime(start_from="2023")
        with self.assertRaises(errors.TableDefinitionError):
            db.tb.ExtendToTime(end_with="2023")
        # [sync] Coalease partition to time
        db.tb.ExtendToTime(start_from="2019-01-01", end_with="2029-01-01")
        self.assertEqual(len(pts), 13)
        self.assertTrue(db.tb.CoalesceToTime(start_from="2020-12-31"))
        self.assertEqual(len(pts), 12)
        self.assertNotIn("y2019", pts)
        self.assertEqual(pts["past"].values, "'2020-01-01 00:00:00'")
        self.assertTrue(db.tb.CoalesceToTime(start_from="2022-12-31"))
        self.assertEqual(len(pts), 10)
        self.assertNotIn("y2020", pts)
        self.assertNotIn("y2021", pts)
        self.assertEqual(pts["past"].values, "'2022-01-01 00:00:00'")
        self.assertTrue(db.tb.CoalesceToTime(end_with="2028-06-30"))
        self.assertEqual(len(pts), 9)
        self.assertNotIn("y2029", pts)
        self.assertTrue(db.tb.CoalesceToTime(end_with="2026-06-30"))
        self.assertEqual(len(pts), 7)
        self.assertNotIn("y2027", pts)
        self.assertNotIn("y2028", pts)
        self.assertTrue(
            db.tb.CoalesceToTime(start_from="2023-06-30", end_with="2025-01-01")
        )
        self.assertEqual(len(pts), 5)
        self.assertNotIn("y2022", pts)
        self.assertNotIn("y2026", pts)
        self.assertTrue(
            db.tb.CoalesceToTime(start_from="2030-06-30", end_with="2000-01-01")
        )
        self.assertEqual(len(pts), 3)
        self.assertFalse(db.tb.CoalesceToTime(start_from="2030-06-30"))
        self.assertFalse(db.tb.CoalesceToTime(end_with="2000-06-30"))
        db.tb.ExtendToTime(start_from="2019-01-01", end_with="2029-01-01")
        with self.assertRaises(errors.TableDefinitionError):
            db.tb.CoalesceToTime(start_from="2023")
        with self.assertRaises(errors.TableDefinitionError):
            db.tb.CoalesceToTime(end_with="2023")
        # [sync] Drop partition to time
        db.tb.ExtendToTime(start_from="2019-01-01", end_with="2029-01-01")
        self.assertEqual(len(pts), 13)
        self.assertTrue(db.tb.DropToTime(start_from="2020-12-31"))
        self.assertEqual(len(pts), 12)
        self.assertNotIn("y2019", pts)
        self.assertEqual(pts["past"].values, "'2020-01-01 00:00:00'")
        self.assertTrue(db.tb.DropToTime(start_from="2022-12-31"))
        self.assertEqual(len(pts), 10)
        self.assertNotIn("y2020", pts)
        self.assertNotIn("y2021", pts)
        self.assertEqual(pts["past"].values, "'2022-01-01 00:00:00'")
        self.assertTrue(db.tb.DropToTime(end_with="2028-06-30"))
        self.assertEqual(len(pts), 9)
        self.assertNotIn("y2029", pts)
        self.assertTrue(db.tb.DropToTime(end_with="2026-06-30"))
        self.assertEqual(len(pts), 7)
        self.assertNotIn("y2027", pts)
        self.assertNotIn("y2028", pts)
        self.assertTrue(
            db.tb.DropToTime(start_from="2023-06-30", end_with="2025-01-01")
        )
        self.assertEqual(len(pts), 5)
        self.assertNotIn("y2022", pts)
        self.assertNotIn("y2026", pts)
        self.assertTrue(
            db.tb.DropToTime(start_from="2030-06-30", end_with="2000-01-01")
        )
        self.assertEqual(len(pts), 3)
        self.assertFalse(db.tb.DropToTime(start_from="2030-06-30"))
        self.assertFalse(db.tb.DropToTime(end_with="2000-06-30"))
        await db.tb.aioExtendToTime(start_from="2023-01-01", end_with="2025-01-01")
        with self.assertRaises(errors.TableDefinitionError):
            db.tb.DropToTime(start_from="2023")
        with self.assertRaises(errors.TableDefinitionError):
            db.tb.DropToTime(end_with="2023")

        # [async] Extend partition to time
        await db.tb.aioExtendToTime(start_from="2023-01-01", end_with="2025-01-01")
        self.assertEqual(len(pts), 5)
        self.assertTrue(await db.tb.aioExtendToTime(end_with="2026-12-31"))
        self.assertEqual(len(pts), 6)
        self.assertIn("y2026", pts)
        self.assertEqual(pts["y2026"].values, "'2027-01-01 00:00:00'")
        self.assertTrue(await db.tb.aioExtendToTime(end_with="2028-06-30"))
        self.assertEqual(len(pts), 8)
        self.assertIn("y2027", pts)
        self.assertEqual(pts["y2027"].values, "'2028-01-01 00:00:00'")
        self.assertIn("y2028", pts)
        self.assertEqual(pts["y2028"].values, "'2029-01-01 00:00:00'")
        self.assertTrue(await db.tb.aioExtendToTime(start_from="2022-12-31"))
        self.assertEqual(len(pts), 9)
        self.assertIn("y2022", pts)
        self.assertEqual(pts["y2022"].values, "'2023-01-01 00:00:00'")
        self.assertTrue(await db.tb.aioExtendToTime(start_from="2020-01-31"))
        self.assertEqual(len(pts), 11)
        self.assertIn("y2020", pts)
        self.assertEqual(pts["y2020"].values, "'2021-01-01 00:00:00'")
        self.assertIn("y2021", pts)
        self.assertEqual(pts["y2021"].values, "'2022-01-01 00:00:00'")
        self.assertTrue(
            await db.tb.aioExtendToTime(start_from="2019-01-31", end_with="2029-12-31")
        )
        self.assertEqual(len(pts), 13)
        self.assertIn("y2019", pts)
        self.assertEqual(pts["y2019"].values, "'2020-01-01 00:00:00'")
        self.assertIn("y2029", pts)
        self.assertEqual(pts["y2029"].values, "'2030-01-01 00:00:00'")
        self.assertFalse(await db.tb.aioExtendToTime(start_from="2024-12-31"))
        self.assertFalse(await db.tb.aioExtendToTime(end_with="2025-12-31"))
        with self.assertRaises(errors.TableDefinitionError):
            await db.tb.aioExtendToTime(start_from="2023")
        with self.assertRaises(errors.TableDefinitionError):
            await db.tb.aioExtendToTime(end_with="2023")
        # [async] Coalease partition to time
        await db.tb.aioExtendToTime(start_from="2019-01-01", end_with="2029-01-01")
        self.assertEqual(len(pts), 13)
        self.assertTrue(await db.tb.aioCoalesceToTime(start_from="2020-12-31"))
        self.assertEqual(len(pts), 12)
        self.assertNotIn("y2019", pts)
        self.assertEqual(pts["past"].values, "'2020-01-01 00:00:00'")
        self.assertTrue(await db.tb.aioCoalesceToTime(start_from="2022-12-31"))
        self.assertEqual(len(pts), 10)
        self.assertNotIn("y2020", pts)
        self.assertNotIn("y2021", pts)
        self.assertEqual(pts["past"].values, "'2022-01-01 00:00:00'")
        self.assertTrue(await db.tb.aioCoalesceToTime(end_with="2028-06-30"))
        self.assertEqual(len(pts), 9)
        self.assertNotIn("y2029", pts)
        self.assertTrue(await db.tb.aioCoalesceToTime(end_with="2026-06-30"))
        self.assertEqual(len(pts), 7)
        self.assertNotIn("y2027", pts)
        self.assertNotIn("y2028", pts)
        self.assertTrue(
            await db.tb.aioCoalesceToTime(
                start_from="2023-06-30", end_with="2025-01-01"
            )
        )
        self.assertEqual(len(pts), 5)
        self.assertNotIn("y2022", pts)
        self.assertNotIn("y2026", pts)
        self.assertTrue(
            await db.tb.aioCoalesceToTime(
                start_from="2030-06-30", end_with="2000-01-01"
            )
        )
        self.assertEqual(len(pts), 3)
        self.assertFalse(await db.tb.aioCoalesceToTime(start_from="2030-06-30"))
        self.assertFalse(await db.tb.aioCoalesceToTime(end_with="2000-06-30"))
        await db.tb.aioExtendToTime(start_from="2019-01-01", end_with="2029-01-01")
        with self.assertRaises(errors.TableDefinitionError):
            await db.tb.aioCoalesceToTime(start_from="2023")
        with self.assertRaises(errors.TableDefinitionError):
            await db.tb.aioCoalesceToTime(end_with="2023")
        # [async] Drop partition to time
        await db.tb.aioExtendToTime(start_from="2019-01-01", end_with="2029-01-01")
        self.assertEqual(len(pts), 13)
        self.assertTrue(await db.tb.aioDropToTime(start_from="2020-12-31"))
        self.assertEqual(len(pts), 12)
        self.assertNotIn("y2019", pts)
        self.assertEqual(pts["past"].values, "'2020-01-01 00:00:00'")
        self.assertTrue(await db.tb.aioDropToTime(start_from="2022-12-31"))
        self.assertEqual(len(pts), 10)
        self.assertNotIn("y2020", pts)
        self.assertNotIn("y2021", pts)
        self.assertEqual(pts["past"].values, "'2022-01-01 00:00:00'")
        self.assertTrue(await db.tb.aioDropToTime(end_with="2028-06-30"))
        self.assertEqual(len(pts), 9)
        self.assertNotIn("y2029", pts)
        self.assertTrue(await db.tb.aioDropToTime(end_with="2026-06-30"))
        self.assertEqual(len(pts), 7)
        self.assertNotIn("y2027", pts)
        self.assertNotIn("y2028", pts)
        self.assertTrue(
            await db.tb.aioDropToTime(start_from="2023-06-30", end_with="2025-01-01")
        )
        self.assertEqual(len(pts), 5)
        self.assertNotIn("y2022", pts)
        self.assertNotIn("y2026", pts)
        self.assertTrue(
            await db.tb.aioDropToTime(start_from="2030-06-30", end_with="2000-01-01")
        )
        self.assertEqual(len(pts), 3)
        self.assertFalse(await db.tb.aioDropToTime(start_from="2030-06-30"))
        self.assertFalse(await db.tb.aioDropToTime(end_with="2000-06-30"))
        await db.tb.aioExtendToTime(start_from="2019-01-01", end_with="2029-01-01")
        with self.assertRaises(errors.TableDefinitionError):
            await db.tb.aioDropToTime(start_from="2023")
        with self.assertRaises(errors.TableDefinitionError):
            await db.tb.aioDropToTime(end_with="2023")

        # Finished
        db.Drop(True)
        self.log_ended("TEST MANIPULATE")

    async def test_manipulate_other_units(self) -> None:
        self.log_start("TEST MANIPULATE OTHER UNITS")

        class TestTable(TimeTable):
            id: Column = Column(Define.BIGINT(auto_increment=True))
            name: Column = Column(Define.VARCHAR(255))
            price: Column = Column(Define.DECIMAL(12, 2))
            dt: Column = Column(Define.DATETIME())
            pk: PrimaryKey = PrimaryKey("id", "dt")

        # . [Q] Quarter ---------------------------------------------------------------------
        class TestDatabase(Database):
            tb: TestTable = TestTable("dt", "Q", "2024-11-15", "2025-05-15")

        db = TestDatabase("db", self.get_pool())
        db.Drop(True)
        await db.aioInitialize()
        pts: Partitioning = db.tb.partitioning
        # fmt: off
        pt_names = {
            1: ("past", "q20244", "q20251", "q20252", "future"),
            2: ("past", "q20242", "q20243", "q20244", "q20251", "q20252", "q20253", "q20254", "future"),
            3: ("past", "q20243", "q20244", "q20251", "q20252", "q20253", "future"),
        }
        # fmt: on
        self.assertEqual(pt_names[1], pts.partitions.names)
        # [sync] Extend partition to time
        db.tb.ExtendToTime(start_from="2024-04-01", end_with="2025-12-01")
        self.assertEqual(pt_names[2], pts.partitions.names)
        # [sync] Coalease partition to time
        db.tb.CoalesceToTime(start_from="2024-08-01", end_with="2025-09-01")
        self.assertEqual(pt_names[3], pts.partitions.names)
        # [sync] Drop partition to time
        db.tb.DropToTime(start_from="2024-11-01", end_with="2025-06-01")
        self.assertEqual(pt_names[1], pts.partitions.names)
        # [async] Extend partition to time
        await db.tb.aioExtendToTime(start_from="2024-04-01", end_with="2025-12-01")
        self.assertEqual(pt_names[2], pts.partitions.names)
        # [async] Coalease partition to time
        await db.tb.aioCoalesceToTime(start_from="2024-08-01", end_with="2025-09-01")
        self.assertEqual(pt_names[3], pts.partitions.names)
        # [async] Drop partition to time
        await db.tb.aioDropToTime(start_from="2024-11-01", end_with="2025-06-01")
        self.assertEqual(pt_names[1], pts.partitions.names)
        # fmt: on

        # . [M] Month ---------------------------------------------------------------------
        class TestDatabase(Database):
            tb: TestTable = TestTable("dt", "M", "2025-03-15", "2025-05-15")

        db = TestDatabase("db", self.get_pool())
        db.Drop(True)
        await db.aioInitialize()
        pts: Partitioning = db.tb.partitioning
        # fmt: off
        pt_names = {
            1: ("past", "m202503", "m202504", "m202505", "future"),
            2: ("past", "m202501", "m202502", "m202503", "m202504", "m202505", "m202506", "m202507", "future"),
            3: ("past", "m202502", "m202503", "m202504", "m202505", "m202506", "future"),
        }
        # fmt: on
        self.assertEqual(pt_names[1], pts.partitions.names)
        # [sync] Extend partition to time
        db.tb.ExtendToTime(start_from="2025-01-31", end_with="2025-07-15")
        self.assertEqual(pt_names[2], pts.partitions.names)
        # [sync] Coalease partition to time
        db.tb.CoalesceToTime(start_from="2025-02-28", end_with="2025-06-30")
        self.assertEqual(pt_names[3], pts.partitions.names)
        # [sync] Drop partition to time
        db.tb.DropToTime(start_from="2025-03-30", end_with="2025-05-15")
        self.assertEqual(pt_names[1], pts.partitions.names)
        # [async] Extend partition to time
        await db.tb.aioExtendToTime(start_from="2025-01-31", end_with="2025-07-15")
        self.assertEqual(pt_names[2], pts.partitions.names)
        # [async] Coalease partition to time
        await db.tb.aioCoalesceToTime(start_from="2025-02-28", end_with="2025-06-30")
        self.assertEqual(pt_names[3], pts.partitions.names)
        # [async] Drop partition to time
        await db.tb.aioDropToTime(start_from="2025-03-30", end_with="2025-05-15")
        self.assertEqual(pt_names[1], pts.partitions.names)

        # . [W] Week ----------------------------------------------------------------------
        class TestDatabase(Database):
            tb: TestTable = TestTable("dt", "W", "2025-05-15", "2025-05-29")

        db = TestDatabase("db", self.get_pool())
        db.Drop(True)
        await db.aioInitialize()
        pts: Partitioning = db.tb.partitioning
        # fmt: off
        pt_names = {
            1: ("past", "w20250512", "w20250519", "w20250526", "future"),
            2: ("past", "w20250428", "w20250505", "w20250512", "w20250519", "w20250526", "w20250602", "w20250609", "future"),
            3: ("past", "w20250505", "w20250512", "w20250519", "w20250526", "w20250602", "future"),
        }
        # fmt: on
        self.assertEqual(pt_names[1], pts.partitions.names)
        # [sync] Extend partition to time
        db.tb.ExtendToTime(start_from="2025-04-30", end_with="2025-06-12")
        self.assertEqual(pt_names[2], pts.partitions.names)
        # [sync] Coalease partition to time
        db.tb.CoalesceToTime(start_from="2025-05-06", end_with="2025-06-04")
        self.assertEqual(pt_names[3], pts.partitions.names)
        # [sync] Drop partition to time
        db.tb.DropToTime(start_from="2025-05-18", end_with="2025-05-28")
        self.assertEqual(pt_names[1], pts.partitions.names)
        # [async] Extend partition to time
        await db.tb.aioExtendToTime(start_from="2025-04-30", end_with="2025-06-12")
        self.assertEqual(pt_names[2], pts.partitions.names)
        # [async] Coalease partition to time
        await db.tb.aioCoalesceToTime(start_from="2025-05-06", end_with="2025-06-04")
        self.assertEqual(pt_names[3], pts.partitions.names)
        # [async] Drop partition to time
        await db.tb.aioDropToTime(start_from="2025-05-18", end_with="2025-05-28")
        self.assertEqual(pt_names[1], pts.partitions.names)

        # . [D] Day -----------------------------------------------------------------------
        class TestDatabase(Database):
            tb: TestTable = TestTable("dt", "D", "2025-05-15", "2025-05-17")

        db = TestDatabase("db", self.get_pool())
        db.Drop(True)
        await db.aioInitialize()
        pts: Partitioning = db.tb.partitioning
        # fmt: off
        pt_names = {
            1: ("past", "d20250515", "d20250516", "d20250517", "future"),
            2: ("past", "d20250513", "d20250514", "d20250515", "d20250516", "d20250517", "d20250518", "d20250519", "future"),
            3: ("past", "d20250514", "d20250515", "d20250516", "d20250517", "d20250518", "future"),
        }
        # fmt: on
        self.assertEqual(pt_names[1], pts.partitions.names)
        # [sync] Extend partition to time
        db.tb.ExtendToTime(
            start_from="2025-05-13 12:00:00", end_with="2025-05-19 12:00:00"
        )
        self.assertEqual(pt_names[2], pts.partitions.names)
        # [sync] Coalease partition to time
        db.tb.CoalesceToTime(
            start_from="2025-05-14 12:00:00", end_with="2025-05-18 12:00:00"
        )
        self.assertEqual(pt_names[3], pts.partitions.names)
        # [sync] Drop partition to time
        db.tb.DropToTime(
            start_from="2025-05-15 12:00:00", end_with="2025-05-17 12:00:00"
        )
        self.assertEqual(pt_names[1], pts.partitions.names)
        # [async] Extend partition to time
        await db.tb.aioExtendToTime(
            start_from="2025-05-13 12:00:00", end_with="2025-05-19 12:00:00"
        )
        self.assertEqual(pt_names[2], pts.partitions.names)
        # [async] Coalease partition to time
        await db.tb.aioCoalesceToTime(
            start_from="2025-05-14 12:00:00", end_with="2025-05-18 12:00:00"
        )
        self.assertEqual(pt_names[3], pts.partitions.names)
        # [async] Drop partition to time
        await db.tb.aioDropToTime(
            start_from="2025-05-15 12:00:00", end_with="2025-05-17 12:00:00"
        )
        self.assertEqual(pt_names[1], pts.partitions.names)

        # . [H] Hour -----------------------------------------------------------------------
        class TestDatabase(Database):
            tb: TestTable = TestTable(
                "dt", "H", "2025-05-15 12:00:02", "2025-05-15 14:59:59"
            )

        db = TestDatabase("db", self.get_pool())
        db.Drop(True)
        await db.aioInitialize()
        pts: Partitioning = db.tb.partitioning
        # fmt: off
        pt_names = {
            1: ("past", "h20250515_12", "h20250515_13", "h20250515_14", "future"),
            2: ("past", "h20250515_10", "h20250515_11", "h20250515_12", "h20250515_13", "h20250515_14", "h20250515_15", "h20250515_16", "future"),
            3: ("past", "h20250515_11", "h20250515_12", "h20250515_13", "h20250515_14", "h20250515_15", "future"),
        }
        # fmt: on
        self.assertEqual(pt_names[1], pts.partitions.names)
        # [sync] Extend partition to time
        db.tb.ExtendToTime(
            start_from="2025-05-15 10:00:02", end_with="2025-05-15 16:20:00"
        )
        self.assertEqual(pt_names[2], pts.partitions.names)
        # [sync] Coalease partition to time
        db.tb.CoalesceToTime(
            start_from="2025-05-15 11:00:02", end_with="2025-05-15 15:20:00"
        )
        self.assertEqual(pt_names[3], pts.partitions.names)
        # [sync] Drop partition to time
        db.tb.DropToTime(
            start_from="2025-05-15 12:00:02", end_with="2025-05-15 14:59:59"
        )
        self.assertEqual(pt_names[1], pts.partitions.names)
        # [async] Extend partition to time
        await db.tb.aioExtendToTime(
            start_from="2025-05-15 10:00:02", end_with="2025-05-15 16:20:00"
        )
        self.assertEqual(pt_names[2], pts.partitions.names)
        # [async] Coalease partition to time
        await db.tb.aioCoalesceToTime(
            start_from="2025-05-15 11:00:02", end_with="2025-05-15 15:20:00"
        )
        self.assertEqual(pt_names[3], pts.partitions.names)
        # [async] Drop partition to time
        await db.tb.aioDropToTime(
            start_from="2025-05-15 12:00:02", end_with="2025-05-15 14:59:59"
        )
        self.assertEqual(pt_names[1], pts.partitions.names)

        # . [M] Minute ---------------------------------------------------------------------
        class TestDatabase(Database):
            tb: TestTable = TestTable(
                "dt", "I", "2025-05-15 12:00:02", "2025-05-15 12:02:59"
            )

        db = TestDatabase("db", self.get_pool())
        db.Drop(True)
        await db.aioInitialize()
        pts: Partitioning = db.tb.partitioning
        # fmt: off
        pt_names = {
            1: ("past", "i20250515_1200", "i20250515_1201", "i20250515_1202", "future"),
            2: ("past", "i20250515_1158", "i20250515_1159", "i20250515_1200", "i20250515_1201", "i20250515_1202", "i20250515_1203", "i20250515_1204", "future"),
            3: ("past", "i20250515_1159", "i20250515_1200", "i20250515_1201", "i20250515_1202", "i20250515_1203", "future"),
        }
        # fmt: on
        self.assertEqual(pt_names[1], pts.partitions.names)
        # [sync] Extend partition to time
        db.tb.ExtendToTime(
            start_from="2025-05-15 11:58:02", end_with="2025-05-15 12:04:59"
        )
        self.assertEqual(pt_names[2], pts.partitions.names)
        # [sync] Coalease partition to time
        db.tb.CoalesceToTime(
            start_from="2025-05-15 11:59:02", end_with="2025-05-15 12:03:59"
        )
        self.assertEqual(pt_names[3], pts.partitions.names)
        # [sync] Drop partition to time
        db.tb.DropToTime(
            start_from="2025-05-15 12:00:02", end_with="2025-05-15 12:02:59"
        )
        self.assertEqual(pt_names[1], pts.partitions.names)
        # [async] Extend partition to time
        await db.tb.aioExtendToTime(
            start_from="2025-05-15 11:58:02", end_with="2025-05-15 12:04:59"
        )
        self.assertEqual(pt_names[2], pts.partitions.names)
        # [async] Coalease partition to time
        await db.tb.aioCoalesceToTime(
            start_from="2025-05-15 11:59:02", end_with="2025-05-15 12:03:59"
        )
        self.assertEqual(pt_names[3], pts.partitions.names)
        # [async] Drop partition to time
        await db.tb.aioDropToTime(
            start_from="2025-05-15 12:00:02", end_with="2025-05-15 12:02:59"
        )
        self.assertEqual(pt_names[1], pts.partitions.names)

        # . [S] Second ---------------------------------------------------------------------
        class TestDatabase(Database):
            tb: TestTable = TestTable(
                "dt", "S", "2025-05-15 12:00:00.9", "2025-05-15 12:00:02.1"
            )

        db = TestDatabase("db", self.get_pool())
        db.Drop(True)
        await db.aioInitialize()
        pts: Partitioning = db.tb.partitioning
        # fmt: off
        pt_names = {
            1: ("past", "s20250515_120000", "s20250515_120001", "s20250515_120002", "future"),
            2: ("past", "s20250515_115958", "s20250515_115959", "s20250515_120000", "s20250515_120001", "s20250515_120002", "s20250515_120003", "s20250515_120004", "future"),
            3: ("past", "s20250515_115959", "s20250515_120000", "s20250515_120001", "s20250515_120002", "s20250515_120003", "future"),
        }
        # fmt: on
        self.assertEqual(pt_names[1], pts.partitions.names)
        # [sync] Extend partition to time
        db.tb.ExtendToTime(
            start_from="2025-05-15 11:59:58.9", end_with="2025-05-15 12:00:04.1"
        )
        self.assertEqual(pt_names[2], pts.partitions.names)
        # [sync] Coalease partition to time
        db.tb.CoalesceToTime(
            start_from="2025-05-15 11:59:59.9", end_with="2025-05-15 12:00:03.1"
        )
        self.assertEqual(pt_names[3], pts.partitions.names)
        # [sync] Drop partition to time
        db.tb.DropToTime(
            start_from="2025-05-15 12:00:00.9", end_with="2025-05-15 12:00:02.1"
        )
        self.assertEqual(pt_names[1], pts.partitions.names)
        # [async] Extend partition to time
        await db.tb.aioExtendToTime(
            start_from="2025-05-15 11:59:58.9", end_with="2025-05-15 12:00:04.1"
        )
        self.assertEqual(pt_names[2], pts.partitions.names)
        # [async] Coalease partition to time
        await db.tb.aioCoalesceToTime(
            start_from="2025-05-15 11:59:59.9", end_with="2025-05-15 12:00:03.1"
        )
        self.assertEqual(pt_names[3], pts.partitions.names)
        # [async] Drop partition to time
        await db.tb.aioDropToTime(
            start_from="2025-05-15 12:00:00.9", end_with="2025-05-15 12:00:02.1"
        )
        self.assertEqual(pt_names[1], pts.partitions.names)

        # Finished
        db.Drop(True)
        self.log_ended("TEST MANIPULATE OTHER UNITS")

    async def test_with_data(self) -> None:
        self.log_start("TEST WITH DATA")

        class TestTable(TimeTable):
            id: Column = Column(Define.BIGINT(auto_increment=True))
            name: Column = Column(Define.VARCHAR(255))
            dt: Column = Column(Define.DATETIME())
            pk: PrimaryKey = PrimaryKey("id", "dt")

        class TestDatabase(Database):
            tb: TestTable = TestTable("dt", "Y", "2023-03-15", "2025-06-15")

        # [sync] ----------------------------------------------------------------------
        db = TestDatabase("db", self.get_pool())
        db.Drop(True)
        self.assertFalse(db.tb.partitioning.ExistsPartition("future"))
        await db.aioInitialize()
        self.assertTrue(db.tb.partitioning.ExistsPartition("future"))
        self.assertTrue(db.tb.partitioning.EmptyPartition("future"))
        with db.acquire() as conn:
            with conn.transaction() as cur:
                cur.executemany(
                    f"INSERT INTO {db.tb} (name, dt) VALUES (%s, %s)",
                    [
                        ("2022", datetime.datetime(2021, 1, 1)),
                        ("2022", datetime.datetime(2022, 1, 1)),
                        ("2023", datetime.datetime(2023, 1, 1)),
                        ("2024", datetime.datetime(2024, 1, 1)),
                        ("2025", datetime.datetime(2025, 1, 1)),
                        ("2026", datetime.datetime(2026, 1, 1)),
                        ("2026", datetime.datetime(2027, 1, 1)),
                    ],
                )

        # fmt: off
        rows = {"past": 2, "y2023": 1, "y2024": 1, "y2025": 1, "future": 2}
        self.assertEqual(db.tb.ShowPartitionRows(), rows)
        self.assertFalse(db.tb.partitioning.EmptyPartition("future"))
        # Coalesce Partition to time
        rows = {"past": 3, "y2024": 1, "y2025": 1, "future": 2}
        self.assertTrue(db.tb.CoalesceToTime(start_from="2024-01-01"))
        self.assertEqual(db.tb.ShowPartitionRows(), rows)
        rows = {"past": 3, "y2024": 1, "future": 3}
        self.assertTrue(db.tb.CoalesceToTime(end_with="2024-01-01"))
        self.assertEqual(db.tb.ShowPartitionRows(), rows)
        # Extend Partition to time
        rows = {'past': 1, 'y2022': 1, 'y2023': 1, 'y2024': 1, 'future': 3}
        self.assertTrue(db.tb.ExtendToTime(start_from="2022-01-01"))
        self.assertEqual(db.tb.ShowPartitionRows(), rows)
        rows = {'past': 1, 'y2022': 1, 'y2023': 1, 'y2024': 1, 'y2025': 1, 'y2026': 1, 'future': 1}
        self.assertTrue(db.tb.ExtendToTime(end_with="2026-01-01"))
        self.assertEqual(db.tb.ShowPartitionRows(), rows)
        # Reorganize Overflow
        db.tb.CoalesceToTime(start_from="2023-01-01", end_with="2025-01-01")
        rows = {'past': 2, 'y2023': 1, 'y2024': 1, 'y2025': 1, 'y2026': 1, 'y2027': 1, 'future': 0}
        self.assertTrue(db.tb.ReorganizeOverflow("future"))
        self.assertEqual(db.tb.ShowPartitionRows(), rows)
        self.assertFalse(db.tb.ReorganizeOverflow("future"))
        rows = {'past': 0, 'y2021': 1, 'y2022': 1, 'y2023': 1, 'y2024': 1, 'y2025': 1, 'y2026': 1, 'y2027': 1, 'future': 0}
        self.assertTrue(db.tb.ReorganizeOverflow("past"))
        self.assertEqual(db.tb.ShowPartitionRows(), rows)
        self.assertFalse(db.tb.ReorganizeOverflow("past"))
        # Drop to time
        rows = {'past': 0, 'y2023': 1, 'y2024': 1, 'y2025': 1, 'y2026': 1, 'y2027': 1, 'future': 0}
        self.assertTrue(db.tb.DropToTime(start_from="2023-01-01"))
        self.assertEqual(db.tb.ShowPartitionRows(), rows)
        rows = {'past': 0, 'y2023': 1, 'y2024': 1, 'y2025': 1, 'future': 0}
        self.assertTrue(db.tb.DropToTime(end_with="2025-01-01"))
        self.assertEqual(db.tb.ShowPartitionRows(), rows)
        rows = {'past': 0, 'y2024': 1, 'future': 0}
        self.assertTrue(db.tb.DropToTime(start_from="2024-01-01", end_with="2024-01-01"))
        self.assertEqual(db.tb.ShowPartitionRows(), rows)
        # fmt: on

        # [async] ----------------------------------------------------------------------
        db = TestDatabase("db", self.get_pool())
        await db.aioDrop(True)
        self.assertFalse(await db.tb.partitioning.aioExistsPartition("future"))
        await db.aioInitialize()
        self.assertTrue(await db.tb.partitioning.aioExistsPartition("future"))
        self.assertTrue(await db.tb.partitioning.aioEmptyPartition("future"))

        async with db.acquire() as conn:
            async with conn.transaction() as cur:
                await cur.executemany(
                    f"INSERT INTO {db.tb} (name, dt) VALUES (%s, %s)",
                    [
                        ("2022", datetime.datetime(2021, 1, 1)),
                        ("2022", datetime.datetime(2022, 1, 1)),
                        ("2023", datetime.datetime(2023, 1, 1)),
                        ("2024", datetime.datetime(2024, 1, 1)),
                        ("2025", datetime.datetime(2025, 1, 1)),
                        ("2026", datetime.datetime(2026, 1, 1)),
                        ("2026", datetime.datetime(2027, 1, 1)),
                    ],
                )

        # fmt: off
        rows = {"past": 2, "y2023": 1, "y2024": 1, "y2025": 1, "future": 2}
        self.assertEqual(await db.tb.aioShowPartitionRows(), rows)
        self.assertFalse(await db.tb.partitioning.aioEmptyPartition("future"))
        # Coalesce Partition to time
        rows = {"past": 3, "y2024": 1, "y2025": 1, "future": 2}
        self.assertTrue(await db.tb.aioCoalesceToTime(start_from="2024-01-01"))
        self.assertEqual(await db.tb.aioShowPartitionRows(), rows)
        rows = {"past": 3, "y2024": 1, "future": 3}
        self.assertTrue(await db.tb.aioCoalesceToTime(end_with="2024-01-01"))
        self.assertEqual(await db.tb.aioShowPartitionRows(), rows)
        # Extend Partition to time
        rows = {'past': 1, 'y2022': 1, 'y2023': 1, 'y2024': 1, 'future': 3}
        self.assertTrue(await db.tb.aioExtendToTime(start_from="2022-01-01"))
        self.assertEqual(await db.tb.aioShowPartitionRows(), rows)
        rows = {'past': 1, 'y2022': 1, 'y2023': 1, 'y2024': 1, 'y2025': 1, 'y2026': 1, 'future': 1}
        self.assertTrue(await db.tb.aioExtendToTime(end_with="2026-01-01"))
        self.assertEqual(await db.tb.aioShowPartitionRows(), rows)
        # Reorganize Overflow
        await db.tb.aioCoalesceToTime(start_from="2023-01-01", end_with="2025-01-01")
        rows = {'past': 2, 'y2023': 1, 'y2024': 1, 'y2025': 1, 'y2026': 1, 'y2027': 1, 'future': 0}
        self.assertTrue(await db.tb.aioReorganizeOverflow("future"))
        self.assertEqual(await db.tb.aioShowPartitionRows(), rows)
        self.assertFalse(await db.tb.aioReorganizeOverflow("future"))
        rows = {'past': 0, 'y2021': 1, 'y2022': 1, 'y2023': 1, 'y2024': 1, 'y2025': 1, 'y2026': 1, 'y2027': 1, 'future': 0}
        self.assertTrue(await db.tb.aioReorganizeOverflow("past"))
        self.assertEqual(await db.tb.aioShowPartitionRows(), rows)
        self.assertFalse(await db.tb.aioReorganizeOverflow("past"))
        # # Drop to time
        rows = {'past': 0, 'y2023': 1, 'y2024': 1, 'y2025': 1, 'y2026': 1, 'y2027': 1, 'future': 0}
        self.assertTrue(await db.tb.aioDropToTime(start_from="2023-01-01"))
        self.assertEqual(await db.tb.aioShowPartitionRows(), rows)
        rows = {'past': 0, 'y2023': 1, 'y2024': 1, 'y2025': 1, 'future': 0}
        self.assertTrue(await db.tb.aioDropToTime(end_with="2025-01-01"))
        self.assertEqual(await db.tb.aioShowPartitionRows(), rows)
        rows = {'past': 0, 'y2024': 1, 'future': 0}
        self.assertTrue(await db.tb.aioDropToTime(start_from="2024-01-01", end_with="2024-01-01"))
        self.assertEqual(await db.tb.aioShowPartitionRows(), rows)
        # fmt: on

        # Finsihed
        db.Drop(True)
        self.log_ended("TEST WITH DATA")

    async def test_insert_auto_init(self) -> None:
        self.log_start("TEST AUTO INIT TABLE TIME")

        while (dt := Pydt.now()).microsecond > 100:
            await asyncio.sleep(0.00001)

        class TestTable(TimeTable):
            id: Column = Column(Define.BIGINT(auto_increment=True))
            name: Column = Column(Define.VARCHAR(255))
            dt: Column = Column(Define.DATETIME(auto_init=True))
            pk: PrimaryKey = PrimaryKey("id", "dt")

        class TestDatabase(Database):
            tb: TestTable = TestTable("dt", "S", dt, dt.add(seconds=1))

        db = TestDatabase("db", self.get_pool())
        db.Drop(True)
        db.Initialize()

        with db.acquire() as conn:
            sql = f"INSERT INTO {db.tb} (name) VALUES (%s)"
            with conn.transaction() as cur:
                for i in range(3):
                    cur.execute(sql, str(i + 1))
                    await asyncio.sleep(1)

        rows = db.tb.ShowPartitionRows()
        self.assertEqual(tuple(rows.values()), (0, 1, 1, 1))

        # Finished
        db.Drop(True)
        self.log_ended("TEST AUTO INIT TABLE TIME")

    async def test_insert_auto_update(self) -> None:
        self.log_start("TEST UPDATE TABLE TIME")

        now = Pydt.now()
        prev = now.to_year(-1)
        next = now.to_year(1)

        class TestTable(TimeTable):
            id: Column = Column(Define.BIGINT(auto_increment=True))
            name: Column = Column(Define.VARCHAR(255))
            dt: Column = Column(Define.DATETIME(auto_update=True))
            pk: PrimaryKey = PrimaryKey("id", "dt")

        class TestDatabase(Database):
            tb: TestTable = TestTable("dt", "Y", prev, next)

        db = TestDatabase("db", self.get_pool())
        db.Drop(True)
        db.Initialize()

        # Base
        with db.acquire() as conn:
            with conn.transaction() as cur:
                sql = f"INSERT INTO {db.tb} (id, name, dt) VALUES (%s, %s, %s)"
                cur.execute(sql, (1, str(prev.year), prev))
        rows = {
            "past": 0,
            f"y{prev.year}": 1,
            f"y{now.year}": 0,
            f"y{next.year}": 0,
            "future": 0,
        }
        self.assertEqual(db.tb.ShowPartitionRows(), rows)

        # Auto init
        with db.acquire() as conn:
            with conn.transaction() as cur:
                sql = f"UPDATE {db.tb} SET name = %s WHERE id = %s"
                cur.execute(sql, (str(now.year), 1))
        rows = {
            "past": 0,
            f"y{prev.year}": 0,
            f"y{now.year}": 1,
            f"y{next.year}": 0,
            "future": 0,
        }
        self.assertEqual(db.tb.ShowPartitionRows(), rows)

        # Update
        with db.acquire() as conn:
            with conn.transaction() as cur:
                sql = f"UPDATE {db.tb} SET name = %s, dt = %s WHERE id = %s"
                cur.execute(sql, (str(now.year), next, 1))
        rows = {
            "past": 0,
            f"y{prev.year}": 0,
            f"y{now.year}": 0,
            f"y{next.year}": 1,
            "future": 0,
        }
        self.assertEqual(db.tb.ShowPartitionRows(), rows)

        # Insert on duplicate key (with conflict)
        with db.acquire() as conn:
            with conn.transaction() as cur:
                sql = (
                    f"INSERT INTO {db.tb} (id, name, dt) "
                    "VALUES (%s, %s, %s) AS new_row "
                    "ON DUPLICATE KEY UPDATE name = new_row.name, dt = %s"
                )
                args = (1, str(now.year), next, now)
                cur.execute(sql, args)
        rows = {
            "past": 0,
            f"y{prev.year}": 0,
            f"y{now.year}": 1,
            f"y{next.year}": 0,
            "future": 0,
        }
        self.assertEqual(db.tb.ShowPartitionRows(), rows)

        # Insert on duplicate key (without conflict)
        with db.acquire() as conn:
            with conn.transaction() as cur:
                sql = (
                    f"INSERT INTO {db.tb} (id, name, dt) "
                    "VALUES (%s, %s, %s) AS new_row "
                    "ON DUPLICATE KEY UPDATE name = new_row.name, dt = new_row.dt"
                )
                args = (1, str(next.year), next)
                cur.execute(sql, args)
        rows = {
            "past": 0,
            f"y{prev.year}": 0,
            f"y{now.year}": 1,
            f"y{next.year}": 1,
            "future": 0,
        }
        self.assertEqual(db.tb.ShowPartitionRows(), rows)

        # Replace (without conflict)
        with db.acquire() as conn:
            with conn.transaction() as cur:
                sql = f"REPLACE INTO {db.tb} (id, name, dt) VALUES (%s, %s, %s)"
                cur.execute(sql, (1, str(prev.year), prev))
        rows = {
            "past": 0,
            f"y{prev.year}": 1,
            f"y{now.year}": 1,
            f"y{next.year}": 1,
            "future": 0,
        }
        self.assertEqual(db.tb.ShowPartitionRows(), rows)

        # Finished
        db.Drop(True)
        self.log_ended("TEST UPDATE TABLE TIME")


if __name__ == "__main__":
    HOST = "localhost"
    PORT = 3306
    USER = "root"
    PSWD = "Password_123456"

    for case in (TestTimeTable,):
        test_case = case(HOST, PORT, USER, PSWD)
        if not iscoroutinefunction(test_case.test_all):
            test_case.test_all()
        else:
            asyncio.run(test_case.test_all())
