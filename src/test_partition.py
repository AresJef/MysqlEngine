import asyncio, time, unittest, datetime
from inspect import iscoroutinefunction
from sqlcycli import errors as sqlerr, sqlfunc, Pool, Connection
from mysqlengine import errors
from mysqlengine.table import Table
from mysqlengine.database import Database
from mysqlengine.constraint import PrimaryKey
from mysqlengine.column import Define, Definition, Column
from mysqlengine.partition import Partition, Partitions, Partitioning


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

    def reset_hashs(self) -> None:
        self._hashs.clear()

    def setup_partitioning(self, pt: Partitioning) -> Partitioning:
        pt.setup("tb1", "db1", "utf8", None, self.get_pool())
        return pt

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


class TestPartitionDefinition(TestCase):
    name: str = "Partition Definition"

    def test_all(self) -> None:
        self.test_partitions()
        self.test_partition_by_range()
        self.test_partition_by_range_columns()
        self.test_partition_by_list()
        self.test_partition_by_list_columns()
        self.test_partition_by_hash()
        self.test_partition_by_key()

    def test_partitions(self) -> None:
        self.log_start("Partitions")

        p1 = Partition("p1", datetime.date(2021, 1, 1))
        p2 = Partition("p2", datetime.date(2022, 1, 1))
        p3 = Partition("p3", datetime.date(2023, 1, 1))
        p4 = Partition("p4", datetime.date(2024, 1, 1))
        pts = Partitions(p1, p2, p3, p4)
        self.assertEqual(len(pts.search_name("p1")), 1)
        self.assertEqual(len(pts.search_name(["p1", "p2"])), 2)
        self.assertEqual(len(pts.search_name("p", exact=False)), 4)
        self.assertTrue(pts.issubset(["p1", pts["p2"], pts]))
        self.assertFalse(pts.issubset("px"))
        self.assertEqual(len(pts.filter("px")), 0)
        self.assertEqual(len(pts.filter("p1")), 1)
        self.assertEqual(len(pts.filter(("p1", p2, pts["p3"]))), 3)
        self.assertEqual(len(pts.filter(pts)), len(pts))
        self.assertIs(pts.get("p1"), p1)
        self.assertIs(pts.get(p1), p1)
        self.assertIs(pts.get("px"), None)
        for p in pts:
            self.assertIsInstance(p, Partition)
        self.assertIn("p1", pts)
        self.assertIn(p2, pts)
        self.assertNotIn("px", pts)

        # Error
        with self.assertRaises(errors.PartitionCriticalError):
            p = Partition("p1", None)
            p._set_partitioning_flag(1)
            p._set_position(1)
            p.setup("tb1", "db1", "utf8", None, self.get_pool())
            p._gen_definition_sql()
        with self.assertRaises(errors.PartitionCriticalError):
            p = Partition("p1", None)
            p._set_partitioning_flag(5)
        with self.assertRaises(errors.PartitionCriticalError):
            p = Partition("p1", "MAXVALUE")
            p._set_partitioning_flag(1)
        with self.assertRaises(errors.PartitionDefinitionError):
            Partition("p1")
        with self.assertRaises(errors.PartitionArgumentError):
            Partitions(None)
        with self.assertRaises(errors.PartitionArgumentError):
            Partitions(p1, p1)

        self.log_ended("Partitions")

    def test_partition_by_range(self) -> None:
        self.log_start("Partition By Range")
        pt = Partitioning(sqlfunc.TO_DAYS("col1")).by_range(
            Partition("p1", sqlfunc.TO_DAYS(datetime.date(2020, 1, 1))),
            Partition("p2", sqlfunc.TO_DAYS(datetime.date(2021, 1, 1))),
            Partition("p3", "MAXVALUE", comment="Last Record"),
        )
        self.setup_partitioning(pt)
        self.assertEqual(
            pt._gen_definition_sql(),
            "PARTITION BY RANGE (TO_DAYS(col1)) (\n\t"
            "PARTITION p1 VALUES LESS THAN (TO_DAYS('2020-01-01')),\n\t"
            "PARTITION p2 VALUES LESS THAN (TO_DAYS('2021-01-01')),\n\t"
            "PARTITION p3 VALUES LESS THAN (MAXVALUE) COMMENT 'Last Record'\n)",
        )
        # Subpartition by hash
        pt = (
            Partitioning(sqlfunc.YEAR("purchased"))
            .by_range(
                Partition("p1", 1990),
                Partition("p2", 2000),
                Partition("p3", "MAXVALUE"),
            )
            .subpartition_by_hash(2, sqlfunc.TO_DAYS("purchased"))
        )
        self.setup_partitioning(pt)
        self.assertEqual(
            pt._gen_definition_sql(),
            "PARTITION BY RANGE (YEAR(purchased))\n"
            "SUBPARTITION BY HASH (TO_DAYS(purchased)) SUBPARTITIONS 2 (\n\t"
            "PARTITION p1 VALUES LESS THAN (1990),\n\t"
            "PARTITION p2 VALUES LESS THAN (2000),\n\t"
            "PARTITION p3 VALUES LESS THAN (MAXVALUE)\n)",
        )

        # Subpartition by key (linear)
        pt = (
            Partitioning(sqlfunc.YEAR("purchased"))
            .by_range(
                Partition("p1", 1990),
                Partition("p2", 2000),
                Partition("p3", "MAXVALUE"),
            )
            .subpartition_by_key(2, sqlfunc.TO_DAYS("purchased"), linear=True)
        )
        self.setup_partitioning(pt)
        self.assertEqual(
            pt._gen_definition_sql(),
            "PARTITION BY RANGE (YEAR(purchased))\n"
            "SUBPARTITION BY LINEAR KEY (TO_DAYS(purchased)) SUBPARTITIONS 2 (\n\t"
            "PARTITION p1 VALUES LESS THAN (1990),\n\t"
            "PARTITION p2 VALUES LESS THAN (2000),\n\t"
            "PARTITION p3 VALUES LESS THAN (MAXVALUE)\n)",
        )

        # Errors
        # fmt: off
        with self.assertRaises(errors.PartitionDefinitionError):
            Partitioning()
        with self.assertRaises(errors.PartitionDefinitionError):
            Partitioning(None)
        with self.assertRaises(errors.PartitionDefinitionError):
            self.setup_partitioning(Partitioning("col1"))._gen_definition_sql()
        with self.assertRaises(errors.PartitionDefinitionError):
            Partitioning("col1").by_range()
        with self.assertRaises(errors.PartitionDefinitionError):
            Partitioning("col1").by_range(Partition("p1", 1)).subpartition_by_hash(-1, "col1")
        with self.assertRaises(errors.PartitionDefinitionError):
            Partitioning("col1").by_range(Partition("p1", 1)).subpartition_by_hash(2)
        with self.assertRaises(errors.PartitionDefinitionError):
            Partitioning("col1").by_range(Partition("p1", 1)).subpartition_by_hash(2, None)
        # fmt: on

        self.log_ended("Partition By Range")

    def test_partition_by_range_columns(self) -> None:
        self.log_start("Partition By Range Columns")

        pt = Partitioning("a", "d", "c").by_range(
            Partition("p1", 5, 10, "ggg"),
            Partition("p2", 10, 20, "mmm"),
            Partition("p3", "MAXVALUE", "MAXVALUE", "MAXVALUE"),
            columns=True,
        )
        self.setup_partitioning(pt)
        self.assertEqual(
            pt._gen_definition_sql(),
            "PARTITION BY RANGE COLUMNS (a,d,c) (\n\t"
            "PARTITION p1 VALUES LESS THAN (5,10,'ggg'),\n\t"
            "PARTITION p2 VALUES LESS THAN (10,20,'mmm'),\n\t"
            "PARTITION p3 VALUES LESS THAN (MAXVALUE,MAXVALUE,MAXVALUE)\n)",
        )

        # Subpartition by hash (linear)
        pt = (
            Partitioning("a", "d", "c")
            .by_range(
                Partition("p1", 5, 10, "ggg"),
                Partition("p2", 10, 20, "mmm"),
                Partition("p3", "MAXVALUE", "MAXVALUE", "MAXVALUE"),
                columns=True,
            )
            .subpartition_by_hash(2, "a", linear=True)
        )
        self.setup_partitioning(pt)
        self.assertEqual(
            pt._gen_definition_sql(),
            "PARTITION BY RANGE COLUMNS (a,d,c)\n"
            "SUBPARTITION BY LINEAR HASH (a) SUBPARTITIONS 2 (\n\t"
            "PARTITION p1 VALUES LESS THAN (5,10,'ggg'),\n\t"
            "PARTITION p2 VALUES LESS THAN (10,20,'mmm'),\n\t"
            "PARTITION p3 VALUES LESS THAN (MAXVALUE,MAXVALUE,MAXVALUE)\n)",
        )

        # Subpartition by key
        pt = (
            Partitioning("a", "d", "c")
            .by_range(
                Partition("p1", 5, 10, "ggg"),
                Partition("p2", 10, 20, "mmm"),
                Partition("p3", "MAXVALUE", "MAXVALUE", "MAXVALUE"),
                columns=True,
            )
            .subpartition_by_key(2, "a")
        )
        self.setup_partitioning(pt)
        self.assertEqual(
            pt._gen_definition_sql(),
            "PARTITION BY RANGE COLUMNS (a,d,c)\n"
            "SUBPARTITION BY KEY (a) SUBPARTITIONS 2 (\n\t"
            "PARTITION p1 VALUES LESS THAN (5,10,'ggg'),\n\t"
            "PARTITION p2 VALUES LESS THAN (10,20,'mmm'),\n\t"
            "PARTITION p3 VALUES LESS THAN (MAXVALUE,MAXVALUE,MAXVALUE)\n)",
        )

        # Errors
        with self.assertRaises(errors.PartitionDefinitionError):
            Partitioning("col1").by_range(columns=True)

        self.log_ended("Partition By Range Columns")

    def test_partition_by_list(self) -> None:
        self.log_start("Partition By List")

        pt = Partitioning("store_id").by_list(
            Partition("p1", 3, 5, 6, 9, 17, comment="p1"),
            Partition("p2", 1, 2, 10, 11, 19, 20, comment="p2"),
        )
        self.setup_partitioning(pt)
        self.assertEqual(
            pt._gen_definition_sql(),
            "PARTITION BY LIST (store_id) (\n\t"
            "PARTITION p1 VALUES IN (3,5,6,9,17) COMMENT 'p1',\n\t"
            "PARTITION p2 VALUES IN (1,2,10,11,19,20) COMMENT 'p2'\n)",
        )

        # Subpartition by hash
        pt = (
            Partitioning("store_id")
            .by_list(
                Partition("p1", 3, 5, 6, 9, 17),
                Partition("p2", 1, 2, 10, 11, 19, 20),
            )
            .subpartition_by_hash(2, "store_id")
        )
        self.setup_partitioning(pt)
        self.assertEqual(
            pt._gen_definition_sql(),
            "PARTITION BY LIST (store_id)\n"
            "SUBPARTITION BY HASH (store_id) SUBPARTITIONS 2 (\n\t"
            "PARTITION p1 VALUES IN (3,5,6,9,17),\n\t"
            "PARTITION p2 VALUES IN (1,2,10,11,19,20)\n)",
        )

        # Subpartition by key (linear)
        pt = (
            Partitioning("store_id")
            .by_list(
                Partition("p1", 3, 5, 6, 9, 17),
                Partition("p2", 1, 2, 10, 11, 19, 20),
            )
            .subpartition_by_key(2, "store_id", linear=True)
        )
        self.setup_partitioning(pt)
        self.assertEqual(
            pt._gen_definition_sql(),
            "PARTITION BY LIST (store_id)\n"
            "SUBPARTITION BY LINEAR KEY (store_id) SUBPARTITIONS 2 (\n\t"
            "PARTITION p1 VALUES IN (3,5,6,9,17),\n\t"
            "PARTITION p2 VALUES IN (1,2,10,11,19,20)\n)",
        )

        # Errors
        with self.assertRaises(errors.PartitionDefinitionError):
            Partitioning("col1").by_list()

        self.log_ended("Partition By List")

    def test_partition_by_list_columns(self) -> None:
        self.log_start("Partition By List Columns")

        pt = Partitioning("c1", "c2").by_list(
            Partition("p1", ("USA", "CA"), ("USA", "NY"), ("CAN", "ON"), ("CAN", "QC")),
            Partition("p2", ("FRA", "IDF"), ("DEU", "BE"), ("ESP", "CAT")),
            columns=True,
        )
        self.setup_partitioning(pt)
        self.assertEqual(
            pt._gen_definition_sql(),
            "PARTITION BY LIST COLUMNS (c1,c2) (\n\t"
            "PARTITION p1 VALUES IN (('USA','CA'),('USA','NY'),('CAN','ON'),('CAN','QC')),\n\t"
            "PARTITION p2 VALUES IN (('FRA','IDF'),('DEU','BE'),('ESP','CAT'))\n)",
        )

        # Subpartition by hash (linear)
        pt = (
            Partitioning("c1", "c2")
            .by_list(
                Partition(
                    "p1", ("USA", "CA"), ("USA", "NY"), ("CAN", "ON"), ("CAN", "QC")
                ),
                Partition("p2", ("FRA", "IDF"), ("DEU", "BE"), ("ESP", "CAT")),
                columns=True,
            )
            .subpartition_by_hash(2, "c1", linear=True)
        )
        self.setup_partitioning(pt)
        self.assertEqual(
            pt._gen_definition_sql(),
            "PARTITION BY LIST COLUMNS (c1,c2)\n"
            "SUBPARTITION BY LINEAR HASH (c1) SUBPARTITIONS 2 (\n\t"
            "PARTITION p1 VALUES IN (('USA','CA'),('USA','NY'),('CAN','ON'),('CAN','QC')),\n\t"
            "PARTITION p2 VALUES IN (('FRA','IDF'),('DEU','BE'),('ESP','CAT'))\n)",
        )

        # Subpartition by key
        pt = (
            Partitioning("c1", "c2")
            .by_list(
                Partition(
                    "p1", ("USA", "CA"), ("USA", "NY"), ("CAN", "ON"), ("CAN", "QC")
                ),
                Partition("p2", ("FRA", "IDF"), ("DEU", "BE"), ("ESP", "CAT")),
                columns=True,
            )
            .subpartition_by_key(2, "c1")
        )
        self.setup_partitioning(pt)
        self.assertEqual(
            pt._gen_definition_sql(),
            "PARTITION BY LIST COLUMNS (c1,c2)\n"
            "SUBPARTITION BY KEY (c1) SUBPARTITIONS 2 (\n\t"
            "PARTITION p1 VALUES IN (('USA','CA'),('USA','NY'),('CAN','ON'),('CAN','QC')),\n\t"
            "PARTITION p2 VALUES IN (('FRA','IDF'),('DEU','BE'),('ESP','CAT'))\n)",
        )

        # Errors
        with self.assertRaises(errors.PartitionDefinitionError):
            Partitioning("col1").by_list(columns=True)

        self.log_ended("Partition By List Columns")

    def test_partition_by_hash(self) -> None:
        self.log_start("Partition By Hash")

        pt = Partitioning("store_id").by_hash(4)
        self.setup_partitioning(pt)
        self.assertEqual(
            pt._gen_definition_sql(),
            "PARTITION BY HASH (store_id) PARTITIONS 4",
        )

        pt = Partitioning(sqlfunc.YEAR("hired")).by_hash(4, linear=True)
        self.setup_partitioning(pt)
        self.assertEqual(
            pt._gen_definition_sql(),
            "PARTITION BY LINEAR HASH (YEAR(hired)) PARTITIONS 4",
        )

        # Errors
        with self.assertRaises(errors.PartitionDefinitionError):
            Partitioning("col1").by_hash(-1)
        with self.assertRaises(errors.PartitionDefinitionError):
            Partitioning("col1").by_hash(0, True)
        with self.assertRaises(errors.PartitionDefinitionError):
            Partitioning("col1").by_hash(2).subpartition_by_hash(2, "col1")
        with self.assertRaises(errors.PartitionDefinitionError):
            Partitioning("col1").by_hash(2).subpartition_by_key(2, "col1")

        self.log_ended("Partition By Hash")

    def test_partition_by_key(self) -> None:
        self.log_start("Partition By Key")

        pt = Partitioning("col1").by_key(3)
        self.setup_partitioning(pt)
        self.assertEqual(
            pt._gen_definition_sql(),
            "PARTITION BY KEY (col1) PARTITIONS 3",
        )

        pt = Partitioning("col1").by_key(3, True)
        self.setup_partitioning(pt)
        self.assertEqual(
            pt._gen_definition_sql(),
            "PARTITION BY LINEAR KEY (col1) PARTITIONS 3",
        )

        # Errors
        with self.assertRaises(errors.PartitionDefinitionError):
            Partitioning("col1").by_key(-1)
        with self.assertRaises(errors.PartitionDefinitionError):
            Partitioning("col1").by_key(0, True)
        with self.assertRaises(errors.PartitionDefinitionError):
            Partitioning("col1").by_key(1).subpartition_by_hash(2, "col1")
        with self.assertRaises(errors.PartitionDefinitionError):
            Partitioning("col1").by_key(1).subpartition_by_key(2, "col1")

        self.log_ended("Partition By Key")


class TestPartitionCopy(TestCase):
    name = "Partition Copy"

    def test_all(self) -> None:
        self.log_start("COPY")

        class TestTable(Table):
            id: Column = Column(Define.BIGINT())
            pid: Column = Column(Define.BIGINT())
            name: Column = Column(Define.VARCHAR(255))
            price: Column = Column(Define.DECIMAL(12, 2))
            pt: Partitioning = Partitioning("id").by_hash(4)

        class TestDatabase(Database):
            tb1: TestTable = TestTable()
            tb2: TestTable = TestTable()

        db = TestDatabase("test_db", self.get_pool())
        self.assertIsNot(db.tb1.pt, db.tb2.pt)
        self.assertIsNot(db.tb1.pt["p0"], db.tb2.pt["p0"])
        self.assertIsNot(db.tb1.pt["p1"], db.tb2.pt["p1"])

        self.log_ended("COPY")


class TestPartitioningSyncSQL(TestCase):
    name: str = "Partition Sync SQL"

    def test_all(self) -> None:
        self.test_partition_by_hash()
        self.test_partition_by_key()
        self.test_partition_by_range()
        self.test_partition_by_list()

    def test_partition_by_hash(self) -> None:
        self.log_start("PARTITION BY HASH")

        # Create & Exists & Drop
        class HashTable(Table):
            id: Column = Column(Define.BIGINT(auto_increment=True))
            name: Column = Column(Define.VARCHAR(255))
            pk: PrimaryKey = PrimaryKey("id")
            pt: Partitioning = Partitioning("id").by_hash(4)

        class LinearHashTable(Table):
            id: Column = Column(Define.BIGINT(auto_increment=True))
            name: Column = Column(Define.VARCHAR(255))
            pk: PrimaryKey = PrimaryKey("id")
            pt: Partitioning = Partitioning("id").by_hash(4, linear=True)

        class TestDatabase(Database):
            hash_tb: HashTable = HashTable()
            l_hash_tb: LinearHashTable = LinearHashTable()

        db = TestDatabase("test_db", self.get_pool())
        self.assertTrue(db.Drop(True))
        self.assertTrue(db.Initialize())
        for tb in db.tables:
            pt: Partitioning = tb.partitioning
            self.assertTrue(pt.Exists())
            self.assertTrue(pt.Remove())
            self.assertFalse(pt.Exists())
            self.assertTrue(pt.Create())
            self.assertTrue(pt.Exists())
        self.assertTrue(db.Drop())

        # Show metadata
        with self.assertRaises(sqlerr.OperationalError):
            db.hash_tb.partitioning.ShowMetadata()
        self.assertTrue(db.Initialize())
        for tb in db.tables:
            pt: Partitioning = tb.partitioning
            meta = pt.ShowMetadata()
            self.assertEqual(meta.db_name, db.db_name)
            self.assertEqual(meta.tb_name, tb.tb_name)
            self.assertIn(meta.partitioning_method, ("HASH", "LINEAR HASH"))
            self.assertEqual(meta.partitioning_expression, "`id`")
            self.assertIsNone(meta.subpartitioning_method)
            self.assertIsNone(meta.subpartitioning_expression)
            self.assertEqual(meta.partition_names, ("p0", "p1", "p2", "p3"))
            self.assertEqual(meta.partition_count, 4)
            self.assertEqual(meta.subpartition_names, ())
            self.assertEqual(meta.subpartition_count, 0)

        # Management: Add & Drop & Truncate & Coalesce & Reorganize & Rebuild
        for tb in db.tables:
            pt: Partitioning = tb.partitioning
            self.assertEqual(len(pt), 4)
            self.assertEqual(pt.partitions.names, ("p0", "p1", "p2", "p3"))
            # . coalesce
            with self.assertRaises(errors.PartitionArgumentError):
                pt.CoalescePartition(0)
            self.assertTrue(pt.CoalescePartition(1))
            self.assertEqual(len(pt.partitions), 3)
            self.assertEqual(pt.partitions.names, ("p0", "p1", "p2"))
            # . add
            with self.assertRaises(errors.PartitionArgumentError):
                pt.AddPartition()
            with self.assertRaises(errors.PartitionArgumentError):
                pt.AddPartition(0)
            self.assertTrue(pt.AddPartition(2))
            self.assertEqual(len(pt), 5)
            self.assertEqual(pt.partitions.names, ("p0", "p1", "p2", "p3", "p4"))
            # . truncate
            with self.assertRaises(errors.PartitionArgumentError):
                pt.TruncatePartition()
            with self.assertRaises(errors.PartitionArgumentError):
                pt.TruncatePartition("xxx")
            self.assertTrue(pt.TruncatePartition("p0", pt["p1"]))
            self.assertTrue(pt.TruncatePartition("ALL"))
            # . drop [not supported]
            with self.assertRaises(sqlerr.OperationalError):
                pt.DropPartition("p0")
            # . reorganize [not supported]
            with self.assertRaises(sqlerr.OperationalError):
                pt.ReorganizePartition("p0", Partition("px", 1), Partition("py", 2))
            # . rebuild
            self.assertTrue(pt.RebuildPartition("ALL"))
            # . coalesce [back to 4 partitions]
            self.assertTrue(pt.CoalescePartition(1))

        # Maitenance: Analyze & Check & OPTIMIZE & REPAIR
        for tb in db.tables:
            pt: Partitioning = tb.partitioning
            for _pts in [("p0",), ("p0", "p1"), ("p0", pt["p1"]), ("ALL",)]:
                # . analyze
                res = pt.AnalyzePartition(*_pts)
                self.assertTrue(len(res) > 0)
                self.assertEqual(res[0]["Op"], "analyze")
                # . check
                res = pt.CheckPartition(*_pts)
                self.assertTrue(len(res) > 0)
                self.assertEqual(res[0]["Op"], "check")
                # . optimize
                res = pt.OptimizePartition(*_pts)
                self.assertTrue(len(res) > 0)
                self.assertEqual(res[0]["Op"], "optimize")
                # . repair
                res = pt.RepairPartition(*_pts)
                self.assertTrue(len(res) > 0)
                self.assertEqual(res[0]["Op"], "repair")

        # Sync from remote
        class HashTable2(Table):
            id: Column = Column(Define.BIGINT(auto_increment=True))
            name: Column = Column(Define.VARCHAR(255))
            pk: PrimaryKey = PrimaryKey("id")
            pt: Partitioning = Partitioning("id").by_hash(2)

        class TestDatabase2(Database):
            l_hash_tb: HashTable2 = HashTable2()

        db2 = TestDatabase2("test_db", self.get_pool())
        pt = db2.l_hash_tb.partitioning
        # . before sync
        self.assertEqual(pt.partitioning_method, "HASH")
        self.assertEqual(pt.partitioning_expression, "id")
        self.assertEqual(pt.partitions.names, ("p0", "p1"))
        # . after sync
        self.assertTrue(pt.SyncFromRemote())
        self.assertEqual(pt.partitioning_method, "LINEAR HASH")
        self.assertEqual(pt.partitioning_expression, "`id`")
        self.assertEqual(pt.partitions.names, ("p0", "p1", "p2", "p3"))

        # Sync to remote
        pt = db.l_hash_tb.partitioning
        self.assertTrue(pt.Remove())
        self.assertTrue(pt.SyncToRemote())  # re-create

        class HashTable2(Table):
            id: Column = Column(Define.BIGINT(auto_increment=True))
            name: Column = Column(Define.VARCHAR(255))
            pk: PrimaryKey = PrimaryKey("id")
            pt: Partitioning = Partitioning("id").by_hash(2)

        class TestDatabase2(Database):
            l_hash_tb: HashTable2 = HashTable2()

        db2 = TestDatabase2("test_db", self.get_pool())
        pt = db2.l_hash_tb.partitioning
        # . before sync
        meta = pt.ShowMetadata()
        self.assertEqual(meta.partitioning_method, "LINEAR HASH")
        self.assertEqual(meta.partitioning_expression, "`id`")
        self.assertEqual(meta.partition_names, ("p0", "p1", "p2", "p3"))
        # . after sync
        self.assertTrue(pt.SyncToRemote())
        meta = pt.ShowMetadata()
        self.assertEqual(meta.partitioning_method, "HASH")
        self.assertEqual(meta.partitioning_expression, "`id`")
        self.assertEqual(meta.partition_names, ("p0", "p1"))

        # Finished
        self.assertTrue(db.Drop(True))
        self.log_ended("PARTITION BY HASH")

    def test_partition_by_key(self) -> None:
        self.log_start("PARTITION BY KEY")

        # Create & Exists & Drop
        class KeyTable(Table):
            id: Column = Column(Define.BIGINT(auto_increment=True))
            name: Column = Column(Define.VARCHAR(255))
            pk: PrimaryKey = PrimaryKey("id")
            pt: Partitioning = Partitioning("id").by_key(4)

        class LinearKeyTable(Table):
            id: Column = Column(Define.BIGINT(auto_increment=True))
            name: Column = Column(Define.VARCHAR(255))
            pk: PrimaryKey = PrimaryKey("id")
            pt: Partitioning = Partitioning("id").by_key(4, linear=True)

        class TestDatabase(Database):
            key_tb: KeyTable = KeyTable()
            l_key_tb: LinearKeyTable = LinearKeyTable()

        db = TestDatabase("test_db", self.get_pool())
        self.assertTrue(db.Drop(True))
        self.assertTrue(db.Initialize())
        for tb in db.tables:
            pt: Partitioning = tb.partitioning
            self.assertTrue(pt.Exists())
            self.assertTrue(pt.Remove())
            self.assertFalse(pt.Exists())
            self.assertTrue(pt.Create())
            self.assertTrue(pt.Exists())
        self.assertTrue(db.Drop())

        # Show metadata
        with self.assertRaises(sqlerr.OperationalError):
            db.key_tb.partitioning.ShowMetadata()
        self.assertTrue(db.Initialize())
        for tb in db.tables:
            pt: Partitioning = tb.partitioning
            meta = pt.ShowMetadata()
            self.assertEqual(meta.db_name, db.db_name)
            self.assertEqual(meta.tb_name, tb.tb_name)
            self.assertIn(meta.partitioning_method, ("KEY", "LINEAR KEY"))
            self.assertEqual(meta.partitioning_expression, "`id`")
            self.assertIsNone(meta.subpartitioning_method)
            self.assertIsNone(meta.subpartitioning_expression)
            self.assertEqual(meta.partition_names, ("p0", "p1", "p2", "p3"))
            self.assertEqual(meta.partition_count, 4)
            self.assertEqual(meta.subpartition_names, ())
            self.assertEqual(meta.subpartition_count, 0)

        # Management: Add & Drop & Truncate & Coalesce & Reorganize & Rebuild
        for tb in db.tables:
            pt: Partitioning = tb.partitioning
            self.assertEqual(len(pt), 4)
            self.assertEqual(pt.partitions.names, ("p0", "p1", "p2", "p3"))
            # . coalesce
            with self.assertRaises(errors.PartitionArgumentError):
                pt.CoalescePartition(0)
            self.assertTrue(pt.CoalescePartition(1))
            self.assertEqual(len(pt.partitions), 3)
            self.assertEqual(pt.partitions.names, ("p0", "p1", "p2"))
            # . add
            with self.assertRaises(errors.PartitionArgumentError):
                pt.AddPartition()
            with self.assertRaises(errors.PartitionArgumentError):
                pt.AddPartition(0)
            self.assertTrue(pt.AddPartition(2))
            self.assertEqual(len(pt), 5)
            self.assertEqual(pt.partitions.names, ("p0", "p1", "p2", "p3", "p4"))
            # . truncate
            with self.assertRaises(errors.PartitionArgumentError):
                pt.TruncatePartition()
            with self.assertRaises(errors.PartitionArgumentError):
                pt.TruncatePartition("xxx")
            self.assertTrue(pt.TruncatePartition("p0", pt["p1"]))
            self.assertTrue(pt.TruncatePartition("ALL"))
            # . drop [not supported]
            with self.assertRaises(sqlerr.OperationalError):
                pt.DropPartition("p0")
            # . reorganize [not supported]
            with self.assertRaises(sqlerr.OperationalError):
                pt.ReorganizePartition("p0", Partition("px", 1), Partition("py", 2))
            # . rebuild
            self.assertTrue(pt.RebuildPartition("ALL"))
            # . coalesce [back to 4 partitions]
            self.assertTrue(pt.CoalescePartition(1))

        # Maitenance: Analyze & Check & OPTIMIZE & REPAIR
        for tb in db.tables:
            pt: Partitioning = tb.partitioning
            for _pts in [("p0",), ("p0", "p1"), ("p0", pt["p1"]), ("ALL",)]:
                # . analyze
                res = pt.AnalyzePartition(*_pts)
                self.assertTrue(len(res) > 0)
                self.assertEqual(res[0]["Op"], "analyze")
                # . check
                res = pt.CheckPartition(*_pts)
                self.assertTrue(len(res) > 0)
                self.assertEqual(res[0]["Op"], "check")
                # . optimize
                res = pt.OptimizePartition(*_pts)
                self.assertTrue(len(res) > 0)
                self.assertEqual(res[0]["Op"], "optimize")
                # . repair
                res = pt.RepairPartition(*_pts)
                self.assertTrue(len(res) > 0)
                self.assertEqual(res[0]["Op"], "repair")

        # Sync from remote
        class KeyTable2(Table):
            id: Column = Column(Define.BIGINT(auto_increment=True))
            name: Column = Column(Define.VARCHAR(255))
            pk: PrimaryKey = PrimaryKey("id")
            pt: Partitioning = Partitioning("id").by_key(2)

        class TestDatabase2(Database):
            l_key_tb: KeyTable2 = KeyTable2()

        db2 = TestDatabase2("test_db", self.get_pool())
        pt = db2.l_key_tb.partitioning
        # . before sync
        self.assertEqual(pt.partitioning_method, "KEY")
        self.assertEqual(pt.partitioning_expression, "id")
        self.assertEqual(pt.partitions.names, ("p0", "p1"))
        # . after sync
        self.assertTrue(pt.SyncFromRemote())
        self.assertEqual(pt.partitioning_method, "LINEAR KEY")
        self.assertEqual(pt.partitioning_expression, "`id`")
        self.assertEqual(pt.partitions.names, ("p0", "p1", "p2", "p3"))

        # Sync to remote
        pt = db.l_key_tb.partitioning
        self.assertTrue(pt.Remove())
        self.assertTrue(pt.SyncToRemote())  # re-create

        class KeyTable2(Table):
            id: Column = Column(Define.BIGINT(auto_increment=True))
            name: Column = Column(Define.VARCHAR(255))
            pk: PrimaryKey = PrimaryKey("id")
            pt: Partitioning = Partitioning("id").by_key(2)

        class TestDatabase2(Database):
            l_key_tb: KeyTable2 = KeyTable2()

        db2 = TestDatabase2("test_db", self.get_pool())
        pt = db2.l_key_tb.partitioning
        # . before sync
        meta = pt.ShowMetadata()
        self.assertEqual(meta.partitioning_method, "LINEAR KEY")
        self.assertEqual(meta.partitioning_expression, "`id`")
        self.assertEqual(meta.partition_names, ("p0", "p1", "p2", "p3"))
        # . after sync
        self.assertTrue(pt.SyncToRemote())
        meta = pt.ShowMetadata()
        self.assertEqual(meta.partitioning_method, "KEY")
        self.assertEqual(meta.partitioning_expression, "`id`")
        self.assertEqual(meta.partition_names, ("p0", "p1"))

        # Finished
        self.assertTrue(db.Drop(True))
        self.log_ended("PARTITION BY KEY")

    def test_partition_by_range(self) -> None:
        self.log_start("PARTITION BY RANGE")

        # Create & Exists & Drop
        class RangeTable(Table):
            id: Column = Column(Define.BIGINT(auto_increment=True))
            name: Column = Column(Define.VARCHAR(255))
            dt: Column = Column(Define.DATETIME())
            pk: PrimaryKey = PrimaryKey("id", "dt")
            pt: Partitioning = Partitioning(sqlfunc.TO_DAYS("dt")).by_range(
                Partition("start", 0),
                Partition("from201201", sqlfunc.TO_DAYS("2012-02-01")),
                Partition("from201202", sqlfunc.TO_DAYS("2012-03-01")),
                Partition("from201203", sqlfunc.TO_DAYS("2012-04-01")),
            )

        class RangeColumnsTable(Table):
            id: Column = Column(Define.BIGINT(auto_increment=True))
            name: Column = Column(Define.VARCHAR(255))
            dt: Column = Column(Define.DATETIME())
            pk: PrimaryKey = PrimaryKey("id", "dt")
            pt: Partitioning = Partitioning("dt").by_range(
                Partition("start", datetime.date(2012, 1, 1)),
                Partition("from201201", datetime.date(2012, 2, 1)),
                Partition("from201202", datetime.date(2012, 3, 1)),
                Partition("from201203", datetime.date(2012, 4, 1)),
                columns=True,
            )

        class TestDatabase(Database):
            range_tb: RangeTable = RangeTable()
            range_c_tb: RangeColumnsTable = RangeColumnsTable()

        db = TestDatabase("test_db", self.get_pool())
        self.assertTrue(db.Drop(True))
        self.assertTrue(db.Initialize())
        for tb in db.tables:
            pt: Partitioning = tb.partitioning
            self.assertTrue(pt.Exists())
            self.assertTrue(pt.Remove())
            self.assertFalse(pt.Exists())
            self.assertTrue(pt.Create())
            self.assertTrue(pt.Exists())
        self.assertTrue(db.Drop())

        # Show metadata
        with self.assertRaises(sqlerr.OperationalError):
            db.range_tb.partitioning.ShowMetadata()
        self.assertTrue(db.Initialize())
        pt_names = ("start", "from201201", "from201202", "from201203")
        for tb in db.tables:
            pt: Partitioning = tb.partitioning
            meta = pt.ShowMetadata()
            self.assertEqual(meta.db_name, db.db_name)
            self.assertEqual(meta.tb_name, tb.tb_name)
            self.assertIn(meta.partitioning_method, ("RANGE", "RANGE COLUMNS"))
            self.assertIn(meta.partitioning_expression, ("to_days(`dt`)", "`dt`"))
            self.assertIsNone(meta.subpartitioning_method)
            self.assertIsNone(meta.subpartitioning_expression)
            self.assertEqual(meta.partition_names, pt_names)
            self.assertEqual(meta.partition_count, 4)
            self.assertEqual(meta.subpartition_names, ())
            self.assertEqual(meta.subpartition_count, 0)

        # Management: Add & Drop & Truncate & Coalesce & Reorganize & Rebuild
        for tb in db.tables:
            pt: Partitioning = tb.partitioning
            self.assertEqual(len(pt), 4)
            self.assertEqual(meta.partition_names, pt_names)
            # . drop
            with self.assertRaises(errors.PartitionArgumentError):
                pt.DropPartition()
            with self.assertRaises(errors.PartitionArgumentError):
                pt.DropPartition("xxx")
            self.assertTrue(pt.DropPartition("from201203", pt["from201202"]))
            self.assertEqual(len(pt), 2)
            self.assertEqual(pt.partitions.names, ("start", "from201201"))
            # . add
            with self.assertRaises(errors.PartitionArgumentError):
                pt.AddPartition()
            with self.assertRaises(errors.PartitionArgumentError):
                pt.AddPartition("from201202")
            if tb is db.range_tb:
                pts = (
                    Partition("from201202", sqlfunc.TO_DAYS("2012-03-01")),
                    Partition("from201203", sqlfunc.TO_DAYS("2012-04-01")),
                )
            else:
                pts = (
                    Partition("from201202", datetime.date(2012, 3, 1)),
                    Partition("from201203", datetime.date(2012, 4, 1)),
                )
            self.assertTrue(pt.AddPartition(*pts))
            self.assertEqual(len(pt), 4)
            self.assertEqual(pt.partitions.names, pt_names)
            # . truncate
            with self.assertRaises(errors.PartitionArgumentError):
                pt.TruncatePartition()
            with self.assertRaises(errors.PartitionArgumentError):
                pt.TruncatePartition("xxx")
            self.assertTrue(pt.TruncatePartition("from201202", pt["from201203"]))
            self.assertTrue(pt.TruncatePartition("ALL"))
            # . coalesce [not supported]
            with self.assertRaises(sqlerr.OperationalError):
                pt.CoalescePartition(3)
            # . reorganize
            with self.assertRaises(errors.PartitionArgumentError):
                pt.ReorganizePartition(None)
            with self.assertRaises(errors.PartitionArgumentError):
                pt.ReorganizePartition("xxx")
            if tb is db.range_tb:
                with self.assertRaises(errors.PartitionArgumentError):
                    pt.ReorganizePartition(db.range_c_tb.pt["from201203"])
                # . one to many
                self.assertTrue(
                    pt.ReorganizePartition(
                        "from201203",
                        Partition("from201203", sqlfunc.TO_DAYS("2012-04-01")),
                        Partition("future", "MAXVALUE"),
                    )
                )
                self.assertEqual(len(pt), 5)
                self.assertEqual(pt.partitions.names, pt_names + ("future",))
                # . many to one
                self.assertTrue(
                    pt.ReorganizePartition(
                        ["from201203", pt["future"]],
                        Partition("from201203", "MAXVALUE"),
                    )
                )
                self.assertEqual(len(pt), 4)
                self.assertEqual(pt.partitions.names, pt_names)
                # . many to many
                self.assertTrue(
                    pt.ReorganizePartition(
                        ["from201202", pt["from201203"]],
                        Partition("from201202", sqlfunc.TO_DAYS("2012-03-01")),
                        Partition("from201203", sqlfunc.TO_DAYS("2012-04-01")),
                        Partition("future", "MAXVALUE"),
                    )
                )
                self.assertEqual(len(pt), 5)
                self.assertEqual(pt.partitions.names, pt_names + ("future",))
            else:
                with self.assertRaises(errors.PartitionArgumentError):
                    pt.ReorganizePartition(db.range_tb.pt["from201203"])
                # . one to many
                self.assertTrue(
                    pt.ReorganizePartition(
                        "from201203",
                        Partition("from201203", datetime.date(2012, 4, 1)),
                        Partition("future", "MAXVALUE"),
                    )
                )
                self.assertEqual(len(pt), 5)
                self.assertEqual(pt.partitions.names, pt_names + ("future",))
                # . many to one
                self.assertTrue(
                    pt.ReorganizePartition(
                        ("from201203", pt["future"]),
                        Partition("from201203", "MAXVALUE"),
                    )
                )
                self.assertEqual(len(pt), 4)
                self.assertEqual(pt.partitions.names, pt_names)
                # . many to many
                self.assertTrue(
                    pt.ReorganizePartition(
                        ("from201202", pt["from201203"]),
                        Partition("from201202", datetime.date(2012, 3, 1)),
                        Partition("from201203", datetime.date(2012, 4, 1)),
                        Partition("future", "MAXVALUE"),
                    )
                )
                self.assertEqual(len(pt), 5)
                self.assertEqual(pt.partitions.names, pt_names + ("future",))
            self.assertTrue(pt.DropPartition("future"))
            # . rebuild
            self.assertTrue(pt.RebuildPartition("ALL"))

        # Maitenance: Analyze & Check & OPTIMIZE & REPAIR
        for tb in db.tables:
            pt: Partitioning = tb.partitioning
            for _pts in [
                ("from201202",),
                ("from201202", "from201203"),
                ("from201202", pt["from201203"]),
                ("ALL",),
            ]:
                # . analyze
                res = pt.AnalyzePartition(*_pts)
                self.assertTrue(len(res) > 0)
                self.assertEqual(res[0]["Op"], "analyze")
                # . check
                res = pt.CheckPartition(*_pts)
                self.assertTrue(len(res) > 0)
                self.assertEqual(res[0]["Op"], "check")
                # . optimize
                res = pt.OptimizePartition(*_pts)
                self.assertTrue(len(res) > 0)
                self.assertEqual(res[0]["Op"], "optimize")
                # . repair
                res = pt.RepairPartition(*_pts)
                self.assertTrue(len(res) > 0)
                self.assertEqual(res[0]["Op"], "repair")

        # Sync from remote
        class RangeTable2(Table):
            id: Column = Column(Define.BIGINT(auto_increment=True))
            name: Column = Column(Define.VARCHAR(255))
            dt: Column = Column(Define.DATETIME())
            pk: PrimaryKey = PrimaryKey("id", "dt")
            pt: Partitioning = (
                Partitioning(sqlfunc.TO_DAYS("dt"))
                .by_range(
                    Partition("start", 0),
                    Partition("from201301", sqlfunc.TO_DAYS("2013-02-01")),
                    Partition("from201302", sqlfunc.TO_DAYS("2013-03-01")),
                )
                .subpartition_by_hash(2, "id", linear=True)
            )

        class TestDatabase2(Database):
            range_c_tb: RangeTable2 = RangeTable2()

        db2 = TestDatabase2("test_db", self.get_pool())
        pt = db2.range_c_tb.partitioning

        # . before sync
        self.assertEqual(pt.partitioning_method, "RANGE")
        self.assertEqual(pt.partitioning_expression, "TO_DAYS(dt)")
        self.assertEqual(pt.subpartitioning_method, "LINEAR HASH")
        self.assertEqual(pt.subpartitioning_expression, "id")
        self.assertEqual(pt.partitions.names, ("start", "from201301", "from201302"))
        self.assertEqual(
            pt.partitions["start"].subpartitions.names, ("startsp0", "startsp1")
        )
        # . after sync
        pt_names = ("start", "from201201", "from201202", "from201203")
        self.assertTrue(pt.SyncFromRemote())
        self.assertEqual(pt.partitioning_method, "RANGE COLUMNS")
        self.assertEqual(pt.partitioning_expression, "`dt`")
        self.assertIsNone(pt.subpartitioning_method)
        self.assertIsNone(pt.subpartitioning_expression)
        self.assertEqual(pt.partitions.names, pt_names)
        self.assertIsNone(pt.partitions["start"].subpartitions)
        # . partition count
        self.assertEqual(pt.ShowPartitionNames(), pt_names)
        self.assertTrue(pt.ShowPartitionRows())
        self.assertEqual(pt.ShowSubpartitionNames(), ())
        self.assertEqual(pt.ShowSubpartitionRows(), {})

        # Sync to remote
        pt: Partitioning = db.range_c_tb.partitioning
        self.assertTrue(pt.Remove())
        self.assertTrue(pt.SyncToRemote())  # re-create

        class RangeTable2(Table):
            id: Column = Column(Define.BIGINT(auto_increment=True))
            name: Column = Column(Define.VARCHAR(255))
            dt: Column = Column(Define.DATETIME())
            pk: PrimaryKey = PrimaryKey("id", "dt")
            pt: Partitioning = (
                Partitioning(sqlfunc.TO_DAYS("dt"))
                .by_range(
                    Partition("start", 0),
                    Partition("from201301", sqlfunc.TO_DAYS("2013-02-01")),
                    Partition("from201302", sqlfunc.TO_DAYS("2013-03-01")),
                    Partition("from201303", sqlfunc.TO_DAYS("2013-04-01")),
                    Partition("from201304", sqlfunc.TO_DAYS("2013-05-01")),
                )
                .subpartition_by_hash(2, "id", linear=True)
            )

        class TestDatabase2(Database):
            range_c_tb: RangeTable2 = RangeTable2()

        db2 = TestDatabase2("test_db", self.get_pool())
        pt = db2.range_c_tb.partitioning
        # . before sync
        meta = pt.ShowMetadata()
        self.assertTrue(meta["PARTITION_METHOD"], "RANGE COLUMNS")
        self.assertTrue(meta["PARTITION_EXPRESSION"], "`dt`")
        self.assertIsNone(meta["SUBPARTITION_METHOD"])
        self.assertIsNone(meta["SUBPARTITION_EXPRESSION"])
        self.assertEqual(
            meta.partition_names, ("start", "from201201", "from201202", "from201203")
        )
        self.assertEqual(meta.subpartition_names, ())
        # . after sync
        pt_names = ("start", "from201301", "from201302", "from201303", "from201304")
        subpt_names = (
            "startsp0",
            "startsp1",
            "from201301sp0",
            "from201301sp1",
            "from201302sp0",
            "from201302sp1",
            "from201303sp0",
            "from201303sp1",
            "from201304sp0",
            "from201304sp1",
        )
        self.assertTrue(pt.SyncToRemote())
        meta = pt.ShowMetadata()
        self.assertTrue(meta["PARTITION_METHOD"], "RANGE")
        self.assertTrue(meta["PARTITION_EXPRESSION"], "to_days(`dt`)")
        self.assertTrue(meta["SUBPARTITION_METHOD"], "LINEAR HASH")
        self.assertTrue(meta["SUBPARTITION_EXPRESSION"], "`id`")
        self.assertEqual(meta.partition_names, pt_names)
        self.assertEqual(meta.subpartition_names, subpt_names)
        # . partition count
        self.assertEqual(pt.ShowPartitionNames(), pt_names)
        self.assertTrue(pt.ShowPartitionRows())
        self.assertEqual(pt.ShowSubpartitionNames(), subpt_names)
        self.assertTrue(pt.ShowSubpartitionRows())

        # Finished
        self.assertTrue(db.Drop(True))
        self.log_ended("PARTITION BY RANGE")

    def test_partition_by_list(self) -> None:
        self.log_start("PARTITION BY LIST")

        # Create & Exists & Drop
        class ListTable(Table):
            id: Column = Column(Define.BIGINT(auto_increment=True))
            store_id: Column = Column(Define.INT())
            pk: PrimaryKey = PrimaryKey("id", "store_id")
            pt: Partitioning = Partitioning("store_id").by_list(
                Partition("p0", 1, 3, 5, 7, 9),
                Partition("p1", 2, 4, 6, 8, 10),
                Partition("p2", 11, 13, 15, 17, 19),
                Partition("p3", 12, 14, 16, 18, 20),
            )

        class ListColumnsTable(Table):
            id: Column = Column(Define.BIGINT(auto_increment=True))
            store_id: Column = Column(Define.INT())
            pk: PrimaryKey = PrimaryKey("id", "store_id")
            pt: Partitioning = Partitioning("store_id").by_list(
                Partition("p0", 1, 3, 5, 7, 9),
                Partition("p1", 2, 4, 6, 8, 10),
                Partition("p2", 11, 13, 15, 17, 19),
                Partition("p3", 12, 14, 16, 18, 20),
                columns=True,
            )

        class TestDatabase(Database):
            list_tb: ListTable = ListTable()
            list_c_tb: ListColumnsTable = ListColumnsTable()

        db = TestDatabase("test_db", self.get_pool())
        self.assertTrue(db.Drop(True))
        self.assertTrue(db.Initialize())
        for tb in db.tables:
            pt: Partitioning = tb.partitioning
            self.assertTrue(pt.Exists())
            self.assertTrue(pt.Remove())
            self.assertFalse(pt.Exists())
            self.assertTrue(pt.Create())
            self.assertTrue(pt.Exists())
        self.assertTrue(db.Drop())

        # Show metadata
        with self.assertRaises(sqlerr.OperationalError):
            db.list_tb.partitioning.ShowMetadata()
        self.assertTrue(db.Initialize())
        pt_names = ("p0", "p1", "p2", "p3")
        for tb in db.tables:
            pt: Partitioning = tb.partitioning
            meta = pt.ShowMetadata()
            self.assertEqual(meta.db_name, db.db_name)
            self.assertEqual(meta.tb_name, tb.tb_name)
            self.assertIn(meta.partitioning_method, ("LIST", "LIST COLUMNS"))
            self.assertEqual(meta.partitioning_expression, ("`store_id`"))
            self.assertIsNone(meta.subpartitioning_method)
            self.assertIsNone(meta.subpartitioning_expression)
            self.assertEqual(meta.partition_names, pt_names)
            self.assertEqual(meta.partition_count, 4)
            self.assertEqual(meta.subpartition_names, ())
            self.assertEqual(meta.subpartition_count, 0)

        # Management: Add & Drop & Truncate & Coalesce & Reorganize & Rebuild
        for tb in db.tables:
            pt: Partitioning = tb.partitioning
            self.assertEqual(len(pt), 4)
            self.assertEqual(meta.partition_names, pt_names)
            # . drop
            with self.assertRaises(errors.PartitionArgumentError):
                pt.DropPartition()
            with self.assertRaises(errors.PartitionArgumentError):
                pt.DropPartition("xxx")
            self.assertTrue(pt.DropPartition("p2", pt["p3"]))
            self.assertEqual(len(pt), 2)
            self.assertEqual(pt.partitions.names, ("p0", "p1"))
            # . add
            with self.assertRaises(errors.PartitionArgumentError):
                pt.AddPartition()
            with self.assertRaises(errors.PartitionArgumentError):
                pt.AddPartition("p2")
            self.assertTrue(
                pt.AddPartition(
                    Partition("p2", 11, 13, 15, 17, 19),
                    Partition("p3", 12, 14, 16, 18, 20),
                )
            )
            self.assertEqual(len(pt), 4)
            self.assertEqual(pt.partitions.names, pt_names)
            # . truncate
            with self.assertRaises(errors.PartitionArgumentError):
                pt.TruncatePartition()
            with self.assertRaises(errors.PartitionArgumentError):
                pt.TruncatePartition("xxx")
            self.assertTrue(pt.TruncatePartition("p0", pt["p1"]))
            self.assertTrue(pt.TruncatePartition("ALL"))
            # . coalesce [not supported]
            with self.assertRaises(sqlerr.OperationalError):
                pt.CoalescePartition(3)
            # . reorganize
            with self.assertRaises(errors.PartitionArgumentError):
                pt.ReorganizePartition(None)
            with self.assertRaises(errors.PartitionArgumentError):
                pt.ReorganizePartition("xxx")
            if tb is db.list_tb:
                with self.assertRaises(errors.PartitionArgumentError):
                    pt.ReorganizePartition(db.list_c_tb.pt["p0"])
            else:
                with self.assertRaises(errors.PartitionArgumentError):
                    pt.ReorganizePartition(db.list_tb.pt["p0"])
            # . one to many
            self.assertTrue(
                pt.ReorganizePartition(
                    "p3",
                    Partition("p3", 12, 14, 16),
                    Partition("p4", 18, 20),
                )
            )
            self.assertEqual(len(pt), 5)
            self.assertEqual(pt.partitions.names, pt_names + ("p4",))
            # . many to one
            self.assertTrue(
                pt.ReorganizePartition(
                    ["p3", pt["p4"]],
                    Partition("p3", 12, 14, 16, 18, 20),
                )
            )
            self.assertEqual(len(pt), 4)
            self.assertEqual(pt.partitions.names, pt_names)
            # . many to many
            self.assertTrue(
                pt.ReorganizePartition(
                    ["p2", pt["p3"]],
                    Partition("p2", 11, 13, 15, 17, 19),
                    Partition("p3", 12, 14, 16),
                    Partition("p4", 18, 20),
                )
            )
            self.assertEqual(len(pt), 5)
            self.assertEqual(pt.partitions.names, pt_names + ("p4",))
            self.assertTrue(pt.DropPartition("p4"))
            # . rebuild
            self.assertTrue(pt.RebuildPartition("ALL"))

        # Maitenance: Analyze & Check & OPTIMIZE & REPAIR
        for tb in db.tables:
            pt: Partitioning = tb.partitioning
            for _pts in [("p0",), ("p0", "p1"), ("p0", pt["p1"]), ("ALL",)]:
                # . analyze
                res = pt.AnalyzePartition(*_pts)
                self.assertTrue(len(res) > 0)
                self.assertEqual(res[0]["Op"], "analyze")
                # . check
                res = pt.CheckPartition(*_pts)
                self.assertTrue(len(res) > 0)
                self.assertEqual(res[0]["Op"], "check")
                # . optimize
                res = pt.OptimizePartition(*_pts)
                self.assertTrue(len(res) > 0)
                self.assertEqual(res[0]["Op"], "optimize")
                # . repair
                res = pt.RepairPartition(*_pts)
                self.assertTrue(len(res) > 0)
                self.assertEqual(res[0]["Op"], "repair")

        # Sync from remote
        class ListTable2(Table):
            id: Column = Column(Define.BIGINT(auto_increment=True))
            store_id: Column = Column(Define.INT())
            pk: PrimaryKey = PrimaryKey("id", "store_id")
            pt: Partitioning = (
                Partitioning("store_id")
                .by_list(
                    Partition("p0", 1, 3, 5),
                    Partition("p1", 2, 4, 6),
                    Partition("p2", 11, 13, 15),
                )
                .subpartition_by_hash(2, "id", linear=True)
            )

        class TestDatabase2(Database):
            list_c_tb: ListTable2 = ListTable2()

        db2 = TestDatabase2("test_db", self.get_pool())
        pt = db2.list_c_tb.partitioning

        # . before sync
        self.assertEqual(pt.partitioning_method, "LIST")
        self.assertEqual(pt.partitioning_expression, "store_id")
        self.assertEqual(pt.subpartitioning_method, "LINEAR HASH")
        self.assertEqual(pt.subpartitioning_expression, "id")
        self.assertEqual(pt.partitions.names, ("p0", "p1", "p2"))
        self.assertEqual(pt.partitions["p0"].subpartitions.names, ("p0sp0", "p0sp1"))
        # . after sync
        self.assertTrue(pt.SyncFromRemote())
        self.assertEqual(pt.partitioning_method, "LIST COLUMNS")
        self.assertEqual(pt.partitioning_expression, "`store_id`")
        self.assertIsNone(pt.subpartitioning_method)
        self.assertIsNone(pt.subpartitioning_expression)
        self.assertEqual(pt.partitions.names, ("p0", "p1", "p2", "p3"))
        self.assertIsNone(pt.partitions["p0"].subpartitions)

        # Sync to remote
        pt: Partitioning = db.list_c_tb.partitioning
        self.assertTrue(pt.Remove())
        self.assertTrue(pt.SyncToRemote())  # re-create

        class ListTable2(Table):
            id: Column = Column(Define.BIGINT(auto_increment=True))
            store_id: Column = Column(Define.INT())
            pk: PrimaryKey = PrimaryKey("id", "store_id")
            pt: Partitioning = (
                Partitioning("store_id")
                .by_list(
                    Partition("p0", 1, 3, 5),
                    Partition("p1", 2, 4, 6),
                    Partition("p2", 11, 13, 15),
                    Partition("p3", 12, 14, 16),
                    Partition("p4", 18, 20),
                )
                .subpartition_by_hash(2, "id", linear=True)
            )

        class TestDatabase2(Database):
            list_c_tb: ListTable2 = ListTable2()

        db2 = TestDatabase2("test_db", self.get_pool())
        pt = db2.list_c_tb.partitioning
        # . before sync
        meta = pt.ShowMetadata()
        self.assertTrue(meta["PARTITION_METHOD"], "LIST COLUMNS")
        self.assertTrue(meta["PARTITION_EXPRESSION"], "`store_id`")
        self.assertIsNone(meta["SUBPARTITION_METHOD"])
        self.assertIsNone(meta["SUBPARTITION_EXPRESSION"])
        self.assertEqual(meta.partition_names, ("p0", "p1", "p2", "p3"))
        self.assertEqual(meta.subpartition_names, ())
        # . after sync
        self.assertTrue(pt.SyncToRemote())
        meta = pt.ShowMetadata()
        self.assertTrue(meta["PARTITION_METHOD"], "LIST")
        self.assertTrue(meta["PARTITION_EXPRESSION"], "`store_id`")
        self.assertTrue(meta["SUBPARTITION_METHOD"], "LINEAR HASH")
        self.assertTrue(meta["SUBPARTITION_EXPRESSION"], "`id`")
        self.assertEqual(
            meta.partition_names,
            ("p0", "p1", "p2", "p3", "p4"),
        )
        self.assertEqual(
            meta.subpartition_names,
            (
                "p0sp0",
                "p0sp1",
                "p1sp0",
                "p1sp1",
                "p2sp0",
                "p2sp1",
                "p3sp0",
                "p3sp1",
                "p4sp0",
                "p4sp1",
            ),
        )

        # Finished
        self.assertTrue(db.Drop(True))
        self.log_ended("PARTITION BY LIST")

    def test_partitioning_basic_sql(self) -> None:
        self.log_start("PARTITIONING BASIC SQL")

        class HashTable(Table):
            id: Column = Column(Define.BIGINT(auto_increment=True))
            name: Column = Column(Define.VARCHAR(255))
            pk: PrimaryKey = PrimaryKey("id")
            pt: Partitioning = Partitioning("id").by_hash(2)

        class LinearHashTable(Table):
            id: Column = Column(Define.BIGINT(auto_increment=True))
            name: Column = Column(Define.VARCHAR(255))
            pk: PrimaryKey = PrimaryKey("id")
            pt: Partitioning = Partitioning("id").by_hash(2, linear=True)

        class KeyTable(Table):
            id: Column = Column(Define.BIGINT(auto_increment=True))
            name: Column = Column(Define.VARCHAR(255))
            pk: PrimaryKey = PrimaryKey("id")
            pt: Partitioning = Partitioning("id").by_key(2)

        class LinearKeyTable(Table):
            id: Column = Column(Define.BIGINT(auto_increment=True))
            name: Column = Column(Define.VARCHAR(255))
            pk: PrimaryKey = PrimaryKey("id")
            pt: Partitioning = Partitioning("id").by_key(2, linear=True)

        class RangeTable(Table):
            id: Column = Column(Define.BIGINT(auto_increment=True))
            name: Column = Column(Define.VARCHAR(255))
            dt: Column = Column(Define.DATETIME())
            pk: PrimaryKey = PrimaryKey("id", "dt")
            pt: Partitioning = Partitioning(sqlfunc.TO_DAYS("dt")).by_range(
                Partition("start", 0),
                Partition("from201201", sqlfunc.TO_DAYS("2012-02-01")),
                Partition("from201202", sqlfunc.TO_DAYS("2012-03-01")),
                Partition("future", "MAXVALUE"),
            )

        class RangeSubHashTable(Table):
            id: Column = Column(Define.BIGINT(auto_increment=True))
            name: Column = Column(Define.VARCHAR(255))
            dt: Column = Column(Define.DATETIME())
            pk: PrimaryKey = PrimaryKey("id", "dt")
            pt: Partitioning = (
                Partitioning(sqlfunc.TO_DAYS("dt"))
                .by_range(
                    Partition("start", 0),
                    Partition("from201201", sqlfunc.TO_DAYS("2012-02-01")),
                    Partition("from201202", sqlfunc.TO_DAYS("2012-03-01")),
                    Partition("future", "MAXVALUE"),
                )
                .subpartition_by_hash(2, "id")
            )

        class RangeColsTable(Table):
            id: Column = Column(Define.BIGINT(auto_increment=True))
            name: Column = Column(Define.VARCHAR(255))
            dt: Column = Column(Define.DATETIME())
            pk: PrimaryKey = PrimaryKey("id", "dt")
            pt: Partitioning = Partitioning("dt").by_range(
                Partition("start", datetime.date(2012, 1, 1)),
                Partition("from201201", datetime.date(2012, 2, 1)),
                Partition("from201202", datetime.date(2012, 3, 1)),
                Partition("future", "MAXVALUE"),
                columns=True,
            )

        class RangeColsSubKeyTable(Table):
            id: Column = Column(Define.BIGINT(auto_increment=True))
            name: Column = Column(Define.VARCHAR(255))
            dt: Column = Column(Define.DATETIME())
            pk: PrimaryKey = PrimaryKey("id", "dt")
            pt: Partitioning = (
                Partitioning("dt")
                # fmt: off
                .by_range(
                    Partition("start", datetime.date(2012, 1, 1), comment="start_comment"),
                    Partition("from201201", datetime.date(2012, 2, 1), comment="from201201_comment"),
                    Partition("from201202", datetime.date(2012, 3, 1), comment="from201202_comment"),
                    Partition("future", "MAXVALUE", comment="future_comment"),
                    columns=True,
                )
                .subpartition_by_key(2, "id")
                # fmt: on
            )

        class ListTable(Table):
            id: Column = Column(Define.BIGINT(auto_increment=True))
            name: Column = Column(Define.VARCHAR(255))
            dt: Column = Column(Define.DATETIME())
            pk: PrimaryKey = PrimaryKey("id", "dt")
            pt: Partitioning = Partitioning(sqlfunc.TO_DAYS("dt")).by_list(
                Partition("l1", sqlfunc.TO_DAYS(datetime.date(2012, 1, 1))),
                Partition("l2", sqlfunc.TO_DAYS(datetime.date(2012, 2, 1))),
                Partition("l3", sqlfunc.TO_DAYS(datetime.date(2012, 3, 1))),
            )

        class ListSubLinearHashTable(Table):
            id: Column = Column(Define.BIGINT(auto_increment=True))
            name: Column = Column(Define.VARCHAR(255))
            dt: Column = Column(Define.DATETIME())
            pk: PrimaryKey = PrimaryKey("id", "dt")
            pt: Partitioning = (
                Partitioning(sqlfunc.TO_DAYS("dt"))
                .by_list(
                    Partition("l1", sqlfunc.TO_DAYS(datetime.date(2012, 1, 1))),
                    Partition("l2", sqlfunc.TO_DAYS(datetime.date(2012, 2, 1))),
                    Partition("l3", sqlfunc.TO_DAYS(datetime.date(2012, 3, 1))),
                )
                .subpartition_by_hash(2, "id", linear=True)
            )

        class ListColsTable(Table):
            id: Column = Column(Define.BIGINT(auto_increment=True))
            name: Column = Column(Define.VARCHAR(255))
            dt: Column = Column(Define.DATETIME())
            pk: PrimaryKey = PrimaryKey("id", "dt")
            pt: Partitioning = Partitioning("dt").by_list(
                Partition("l1", datetime.date(2012, 1, 1), datetime.date(2013, 1, 1)),
                Partition("l2", datetime.date(2012, 2, 1), datetime.date(2013, 2, 1)),
                Partition("l3", datetime.date(2012, 3, 1), datetime.date(2013, 3, 1)),
                columns=True,
            )

        class ListColsSubLinearKeyTable(Table):
            id: Column = Column(Define.BIGINT(auto_increment=True))
            name: Column = Column(Define.VARCHAR(255))
            dt: Column = Column(Define.DATETIME())
            pk: PrimaryKey = PrimaryKey("id", "dt")
            pt: Partitioning = (
                Partitioning("dt")
                .by_list(
                    Partition("l1", datetime.date(2012, 1, 1)),
                    Partition("l2", datetime.date(2012, 2, 1)),
                    Partition("l3", datetime.date(2012, 3, 1), comment="l3_comment"),
                    columns=True,
                )
                .subpartition_by_key(2, "id", linear=True)
            )

        class TestDatabase(Database):
            # Hash
            hash_tb: HashTable = HashTable()
            lhash_tb: LinearHashTable = LinearHashTable()
            # Key
            key_tb: KeyTable = KeyTable()
            lkey_tb: LinearKeyTable = LinearKeyTable()
            # Range
            range_tb: RangeTable = RangeTable()
            range_subhash_tb: RangeSubHashTable = RangeSubHashTable()
            range_col_tb: RangeColsTable = RangeColsTable()
            range_col_subkey_tb: RangeColsSubKeyTable = RangeColsSubKeyTable()
            # List
            list_tb: ListTable = ListTable()
            list_sublhash_tb: ListSubLinearHashTable = ListSubLinearHashTable()
            list_col_tb: ListColsTable = ListColsTable()
            list_col_sublkey_tb: ListColsSubLinearKeyTable = ListColsSubLinearKeyTable()

        db = TestDatabase("test_db", self.get_pool())
        self.assertTrue(db.Drop(True))
        self.assertTrue(db.Initialize())

        with db.acquire() as conn:
            with conn.cursor() as cur:
                for tb in db.tables:
                    if "dt" in tb.columns:
                        cur.executemany(
                            f"INSERT INTO {tb} (name, dt) VALUES (%s, %s)",
                            [
                                ("v1", datetime.datetime(2012, 1, 1)),
                                ("v2", datetime.datetime(2012, 2, 1)),
                                ("v3", datetime.datetime(2012, 3, 1)),
                            ],
                        )
                    else:
                        cur.executemany(
                            f"INSERT INTO {tb} (name) VALUES (%s)",
                            [("v1",), ("v2",), ("v3",)],
                        )
            conn.commit()

        print()
        print()

        for tb in db:
            meta = tb.partitioning.ShowMetadata()
            print("-" * 100)
            print(meta.catelog_name)
            print(meta.db_name)
            print(meta.tb_name)
            print(meta.partitioning_method)
            print(meta.partitioning_expression)
            print(meta.subpartitioning_method)
            print(meta.subpartitioning_expression)
            print(meta.partition_names)
            print(meta.partition_count)
            print(meta.subpartition_names)
            print(meta.subpartition_count)
            print(tb.partitioning.SyncFromRemote())
            print(tb.partitioning.partitions.rows())
            with tb.acquire() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"SELECT COUNT(*) AS i FROM {tb}")
                    print(cur.fetchone())
            print(tb.partitioning.Exists())
            print(tb.partitioning._gen_definition_sql())
            print()

        self.log_ended("PARTITIONING BASIC SQL")


class TestPartitioningAsyncSQL(TestCase):
    name: str = "Partition Async SQL"

    async def test_all(self) -> None:
        await self.test_partition_by_hash()
        await self.test_partition_by_key()
        await self.test_partition_by_range()
        await self.test_partition_by_list()

    async def test_partition_by_hash(self) -> None:
        self.log_start("PARTITION BY HASH")

        # Create & Exists & Drop
        class HashTable(Table):
            id: Column = Column(Define.BIGINT(auto_increment=True))
            name: Column = Column(Define.VARCHAR(255))
            pk: PrimaryKey = PrimaryKey("id")
            pt: Partitioning = Partitioning("id").by_hash(4)

        class LinearHashTable(Table):
            id: Column = Column(Define.BIGINT(auto_increment=True))
            name: Column = Column(Define.VARCHAR(255))
            pk: PrimaryKey = PrimaryKey("id")
            pt: Partitioning = Partitioning("id").by_hash(4, linear=True)

        class TestDatabase(Database):
            hash_tb: HashTable = HashTable()
            l_hash_tb: LinearHashTable = LinearHashTable()

        db = TestDatabase("test_db", self.get_pool())
        self.assertTrue(await db.aioDrop(True))
        self.assertTrue(await db.aioInitialize())
        for tb in db.tables:
            pt: Partitioning = tb.partitioning
            self.assertTrue(await pt.aioExists())
            self.assertTrue(await pt.aioRemove())
            self.assertFalse(await pt.aioExists())
            self.assertTrue(await pt.aioCreate())
            self.assertTrue(await pt.aioExists())
        self.assertTrue(await db.aioDrop())

        # Show metadata
        with self.assertRaises(sqlerr.OperationalError):
            await db.hash_tb.partitioning.aioShowMetadata()
        self.assertTrue(await db.aioInitialize())
        for tb in db.tables:
            pt: Partitioning = tb.partitioning
            meta = await pt.aioShowMetadata()
            self.assertEqual(meta.db_name, db.db_name)
            self.assertEqual(meta.tb_name, tb.tb_name)
            self.assertIn(meta.partitioning_method, ("HASH", "LINEAR HASH"))
            self.assertEqual(meta.partitioning_expression, "`id`")
            self.assertIsNone(meta.subpartitioning_method)
            self.assertIsNone(meta.subpartitioning_expression)
            self.assertEqual(meta.partition_names, ("p0", "p1", "p2", "p3"))
            self.assertEqual(meta.partition_count, 4)
            self.assertEqual(meta.subpartition_names, ())
            self.assertEqual(meta.subpartition_count, 0)

        # Management: Add & Drop & Truncate & Coalesce & Reorganize & Rebuild
        for tb in db.tables:
            pt: Partitioning = tb.partitioning
            self.assertEqual(len(pt), 4)
            self.assertEqual(pt.partitions.names, ("p0", "p1", "p2", "p3"))
            # . coalesce
            with self.assertRaises(errors.PartitionArgumentError):
                await pt.aioCoalescePartition(0)
            self.assertTrue(await pt.aioCoalescePartition(1))
            self.assertEqual(len(pt.partitions), 3)
            self.assertEqual(pt.partitions.names, ("p0", "p1", "p2"))
            # . add
            with self.assertRaises(errors.PartitionArgumentError):
                await pt.aioAddPartition()
            with self.assertRaises(errors.PartitionArgumentError):
                await pt.aioAddPartition(0)
            self.assertTrue(await pt.aioAddPartition(2))
            self.assertEqual(len(pt), 5)
            self.assertEqual(pt.partitions.names, ("p0", "p1", "p2", "p3", "p4"))
            # . truncate
            with self.assertRaises(errors.PartitionArgumentError):
                await pt.aioTruncatePartition()
            with self.assertRaises(errors.PartitionArgumentError):
                await pt.aioTruncatePartition("xxx")
            self.assertTrue(await pt.aioTruncatePartition("p0", pt["p1"]))
            self.assertTrue(await pt.aioTruncatePartition("ALL"))
            # . drop [not supported]
            with self.assertRaises(sqlerr.OperationalError):
                await pt.aioDropPartition("p0")
            # . reorganize [not supported]
            with self.assertRaises(sqlerr.OperationalError):
                await pt.aioReorganizePartition(
                    "p0", Partition("px", 1), Partition("py", 2)
                )
            # . rebuild
            self.assertTrue(await pt.aioRebuildPartition("ALL"))
            # . coalesce [back to 4 partitions]
            self.assertTrue(await pt.aioCoalescePartition(1))

        # Maitenance: Analyze & Check & OPTIMIZE & REPAIR
        for tb in db.tables:
            pt: Partitioning = tb.partitioning
            for _pts in [("p0",), ("p0", "p1"), ("p0", pt["p1"]), ("ALL",)]:
                # . analyze
                res = await pt.aioAnalyzePartition(*_pts)
                self.assertTrue(len(res) > 0)
                self.assertEqual(res[0]["Op"], "analyze")
                # . check
                res = await pt.aioCheckPartition(*_pts)
                self.assertTrue(len(res) > 0)
                self.assertEqual(res[0]["Op"], "check")
                # . optimize
                res = await pt.aioOptimizePartition(*_pts)
                self.assertTrue(len(res) > 0)
                self.assertEqual(res[0]["Op"], "optimize")
                # . repair
                res = await pt.aioRepairPartition(*_pts)
                self.assertTrue(len(res) > 0)
                self.assertEqual(res[0]["Op"], "repair")

        # Sync from remote
        class HashTable2(Table):
            id: Column = Column(Define.BIGINT(auto_increment=True))
            name: Column = Column(Define.VARCHAR(255))
            pk: PrimaryKey = PrimaryKey("id")
            pt: Partitioning = Partitioning("id").by_hash(2)

        class TestDatabase2(Database):
            l_hash_tb: HashTable2 = HashTable2()

        db2 = TestDatabase2("test_db", self.get_pool())
        pt = db2.l_hash_tb.partitioning
        # . before sync
        self.assertEqual(pt.partitioning_method, "HASH")
        self.assertEqual(pt.partitioning_expression, "id")
        self.assertEqual(pt.partitions.names, ("p0", "p1"))
        # . after sync
        self.assertTrue(await pt.aioSyncFromRemote())
        self.assertEqual(pt.partitioning_method, "LINEAR HASH")
        self.assertEqual(pt.partitioning_expression, "`id`")
        self.assertEqual(pt.partitions.names, ("p0", "p1", "p2", "p3"))

        # Sync to remote
        pt = db.l_hash_tb.partitioning
        self.assertTrue(await pt.aioRemove())
        self.assertTrue(await pt.aioSyncToRemote())  # re-create

        class HashTable2(Table):
            id: Column = Column(Define.BIGINT(auto_increment=True))
            name: Column = Column(Define.VARCHAR(255))
            pk: PrimaryKey = PrimaryKey("id")
            pt: Partitioning = Partitioning("id").by_hash(2)

        class TestDatabase2(Database):
            l_hash_tb: HashTable2 = HashTable2()

        db2 = TestDatabase2("test_db", self.get_pool())
        pt = db2.l_hash_tb.partitioning
        # . before sync
        meta = await pt.aioShowMetadata()
        self.assertEqual(meta.partitioning_method, "LINEAR HASH")
        self.assertEqual(meta.partitioning_expression, "`id`")
        self.assertEqual(meta.partition_names, ("p0", "p1", "p2", "p3"))
        # . after sync
        self.assertTrue(await pt.aioSyncToRemote())
        meta = await pt.aioShowMetadata()
        self.assertEqual(meta.partitioning_method, "HASH")
        self.assertEqual(meta.partitioning_expression, "`id`")
        self.assertEqual(meta.partition_names, ("p0", "p1"))

        # Finished
        self.assertTrue(await db.aioDrop(True))
        self.log_ended("PARTITION BY HASH")

    async def test_partition_by_key(self) -> None:
        self.log_start("PARTITION BY KEY")

        # Create & Exists & Drop
        class KeyTable(Table):
            id: Column = Column(Define.BIGINT(auto_increment=True))
            name: Column = Column(Define.VARCHAR(255))
            pk: PrimaryKey = PrimaryKey("id")
            pt: Partitioning = Partitioning("id").by_key(4)

        class LinearKeyTable(Table):
            id: Column = Column(Define.BIGINT(auto_increment=True))
            name: Column = Column(Define.VARCHAR(255))
            pk: PrimaryKey = PrimaryKey("id")
            pt: Partitioning = Partitioning("id").by_key(4, linear=True)

        class TestDatabase(Database):
            key_tb: KeyTable = KeyTable()
            l_key_tb: LinearKeyTable = LinearKeyTable()

        db = TestDatabase("test_db", self.get_pool())
        self.assertTrue(await db.aioDrop(True))
        self.assertTrue(await db.aioInitialize())
        for tb in db.tables:
            pt: Partitioning = tb.partitioning
            self.assertTrue(await pt.aioExists())
            self.assertTrue(await pt.aioRemove())
            self.assertFalse(await pt.aioExists())
            self.assertTrue(await pt.aioCreate())
            self.assertTrue(await pt.aioExists())
        self.assertTrue(await db.aioDrop())

        # Show metadata
        with self.assertRaises(sqlerr.OperationalError):
            await db.key_tb.partitioning.aioShowMetadata()
        self.assertTrue(await db.aioInitialize())
        for tb in db.tables:
            pt: Partitioning = tb.partitioning
            meta = await pt.aioShowMetadata()
            self.assertEqual(meta.db_name, db.db_name)
            self.assertEqual(meta.tb_name, tb.tb_name)
            self.assertIn(meta.partitioning_method, ("KEY", "LINEAR KEY"))
            self.assertEqual(meta.partitioning_expression, "`id`")
            self.assertIsNone(meta.subpartitioning_method)
            self.assertIsNone(meta.subpartitioning_expression)
            self.assertEqual(meta.partition_names, ("p0", "p1", "p2", "p3"))
            self.assertEqual(meta.partition_count, 4)
            self.assertEqual(meta.subpartition_names, ())
            self.assertEqual(meta.subpartition_count, 0)

        # Management: Add & Drop & Truncate & Coalesce & Reorganize & Rebuild
        for tb in db.tables:
            pt: Partitioning = tb.partitioning
            self.assertEqual(len(pt), 4)
            self.assertEqual(pt.partitions.names, ("p0", "p1", "p2", "p3"))
            # . coalesce
            with self.assertRaises(errors.PartitionArgumentError):
                await pt.aioCoalescePartition(0)
            self.assertTrue(await pt.aioCoalescePartition(1))
            self.assertEqual(len(pt.partitions), 3)
            self.assertEqual(pt.partitions.names, ("p0", "p1", "p2"))
            # . add
            with self.assertRaises(errors.PartitionArgumentError):
                await pt.aioAddPartition()
            with self.assertRaises(errors.PartitionArgumentError):
                await pt.aioAddPartition(0)
            self.assertTrue(await pt.aioAddPartition(2))
            self.assertEqual(len(pt), 5)
            self.assertEqual(pt.partitions.names, ("p0", "p1", "p2", "p3", "p4"))
            # . truncate
            with self.assertRaises(errors.PartitionArgumentError):
                await pt.aioTruncatePartition()
            with self.assertRaises(errors.PartitionArgumentError):
                await pt.aioTruncatePartition("xxx")
            self.assertTrue(await pt.aioTruncatePartition("p0", pt["p1"]))
            self.assertTrue(await pt.aioTruncatePartition("ALL"))
            # . drop [not supported]
            with self.assertRaises(sqlerr.OperationalError):
                await pt.aioDropPartition("p0")
            # . reorganize [not supported]
            with self.assertRaises(sqlerr.OperationalError):
                await pt.aioReorganizePartition(
                    "p0", Partition("px", 1), Partition("py", 2)
                )
            # . rebuild
            self.assertTrue(await pt.aioRebuildPartition("ALL"))
            # . coalesce [back to 4 partitions]
            self.assertTrue(await pt.aioCoalescePartition(1))

        # Maitenance: Analyze & Check & OPTIMIZE & REPAIR
        for tb in db.tables:
            pt: Partitioning = tb.partitioning
            for _pts in [("p0",), ("p0", "p1"), ("p0", pt["p1"]), ("ALL",)]:
                # . analyze
                res = await pt.aioAnalyzePartition(*_pts)
                self.assertTrue(len(res) > 0)
                self.assertEqual(res[0]["Op"], "analyze")
                # . check
                res = await pt.aioCheckPartition(*_pts)
                self.assertTrue(len(res) > 0)
                self.assertEqual(res[0]["Op"], "check")
                # . optimize
                res = await pt.aioOptimizePartition(*_pts)
                self.assertTrue(len(res) > 0)
                self.assertEqual(res[0]["Op"], "optimize")
                # . repair
                res = await pt.aioRepairPartition(*_pts)
                self.assertTrue(len(res) > 0)
                self.assertEqual(res[0]["Op"], "repair")

        # Sync from remote
        class KeyTable2(Table):
            id: Column = Column(Define.BIGINT(auto_increment=True))
            name: Column = Column(Define.VARCHAR(255))
            pk: PrimaryKey = PrimaryKey("id")
            pt: Partitioning = Partitioning("id").by_key(2)

        class TestDatabase2(Database):
            l_key_tb: KeyTable2 = KeyTable2()

        db2 = TestDatabase2("test_db", self.get_pool())
        pt = db2.l_key_tb.partitioning
        # . before sync
        self.assertEqual(pt.partitioning_method, "KEY")
        self.assertEqual(pt.partitioning_expression, "id")
        self.assertEqual(pt.partitions.names, ("p0", "p1"))
        # . after sync
        self.assertTrue(await pt.aioSyncFromRemote())
        self.assertEqual(pt.partitioning_method, "LINEAR KEY")
        self.assertEqual(pt.partitioning_expression, "`id`")
        self.assertEqual(pt.partitions.names, ("p0", "p1", "p2", "p3"))

        # Sync to remote
        pt = db.l_key_tb.partitioning
        self.assertTrue(await pt.aioRemove())
        self.assertTrue(await pt.aioSyncToRemote())  # re-create

        class KeyTable2(Table):
            id: Column = Column(Define.BIGINT(auto_increment=True))
            name: Column = Column(Define.VARCHAR(255))
            pk: PrimaryKey = PrimaryKey("id")
            pt: Partitioning = Partitioning("id").by_key(2)

        class TestDatabase2(Database):
            l_key_tb: KeyTable2 = KeyTable2()

        db2 = TestDatabase2("test_db", self.get_pool())
        pt = db2.l_key_tb.partitioning
        # . before sync
        meta = await pt.aioShowMetadata()
        self.assertEqual(meta.partitioning_method, "LINEAR KEY")
        self.assertEqual(meta.partitioning_expression, "`id`")
        self.assertEqual(meta.partition_names, ("p0", "p1", "p2", "p3"))
        # . after sync
        self.assertTrue(await pt.aioSyncToRemote())
        meta = await pt.aioShowMetadata()
        self.assertEqual(meta.partitioning_method, "KEY")
        self.assertEqual(meta.partitioning_expression, "`id`")
        self.assertEqual(meta.partition_names, ("p0", "p1"))

        # Finished
        self.assertTrue(await db.aioDrop(True))
        self.log_ended("PARTITION BY KEY")

    async def test_partition_by_range(self) -> None:
        self.log_start("PARTITION BY RANGE")

        # Create & Exists & Drop
        class RangeTable(Table):
            id: Column = Column(Define.BIGINT(auto_increment=True))
            name: Column = Column(Define.VARCHAR(255))
            dt: Column = Column(Define.DATETIME())
            pk: PrimaryKey = PrimaryKey("id", "dt")
            pt: Partitioning = Partitioning(sqlfunc.TO_DAYS("dt")).by_range(
                Partition("start", 0),
                Partition("from201201", sqlfunc.TO_DAYS("2012-02-01")),
                Partition("from201202", sqlfunc.TO_DAYS("2012-03-01")),
                Partition("from201203", sqlfunc.TO_DAYS("2012-04-01")),
            )

        class RangeColumnsTable(Table):
            id: Column = Column(Define.BIGINT(auto_increment=True))
            name: Column = Column(Define.VARCHAR(255))
            dt: Column = Column(Define.DATETIME())
            pk: PrimaryKey = PrimaryKey("id", "dt")
            pt: Partitioning = Partitioning("dt").by_range(
                Partition("start", datetime.date(2012, 1, 1)),
                Partition("from201201", datetime.date(2012, 2, 1)),
                Partition("from201202", datetime.date(2012, 3, 1)),
                Partition("from201203", datetime.date(2012, 4, 1)),
                columns=True,
            )

        class TestDatabase(Database):
            range_tb: RangeTable = RangeTable()
            range_c_tb: RangeColumnsTable = RangeColumnsTable()

        db = TestDatabase("test_db", self.get_pool())
        self.assertTrue(await db.aioDrop(True))
        self.assertTrue(await db.aioInitialize())
        for tb in db.tables:
            pt: Partitioning = tb.partitioning
            self.assertTrue(await pt.aioExists())
            self.assertTrue(await pt.aioRemove())
            self.assertFalse(await pt.aioExists())
            self.assertTrue(await pt.aioCreate())
            self.assertTrue(await pt.aioExists())
        self.assertTrue(await db.aioDrop())

        # Show metadata
        with self.assertRaises(sqlerr.OperationalError):
            await db.range_tb.partitioning.aioShowMetadata()
        self.assertTrue(await db.aioInitialize())
        pt_names = ("start", "from201201", "from201202", "from201203")
        for tb in db.tables:
            pt: Partitioning = tb.partitioning
            meta = await pt.aioShowMetadata()
            self.assertEqual(meta.db_name, db.db_name)
            self.assertEqual(meta.tb_name, tb.tb_name)
            self.assertIn(meta.partitioning_method, ("RANGE", "RANGE COLUMNS"))
            self.assertIn(meta.partitioning_expression, ("to_days(`dt`)", "`dt`"))
            self.assertIsNone(meta.subpartitioning_method)
            self.assertIsNone(meta.subpartitioning_expression)
            self.assertEqual(meta.partition_names, pt_names)
            self.assertEqual(meta.partition_count, 4)
            self.assertEqual(meta.subpartition_names, ())
            self.assertEqual(meta.subpartition_count, 0)

        # Management: Add & Drop & Truncate & Coalesce & Reorganize & Rebuild
        for tb in db.tables:
            pt: Partitioning = tb.partitioning
            self.assertEqual(len(pt), 4)
            self.assertEqual(meta.partition_names, pt_names)
            # . drop
            with self.assertRaises(errors.PartitionArgumentError):
                await pt.aioDropPartition()
            with self.assertRaises(errors.PartitionArgumentError):
                await pt.aioDropPartition("xxx")
            self.assertTrue(await pt.aioDropPartition("from201203", pt["from201202"]))
            self.assertEqual(len(pt), 2)
            self.assertEqual(pt.partitions.names, ("start", "from201201"))
            # . add
            with self.assertRaises(errors.PartitionArgumentError):
                await pt.aioAddPartition()
            with self.assertRaises(errors.PartitionArgumentError):
                await pt.aioAddPartition("from201202")
            if tb is db.range_tb:
                pts = (
                    Partition("from201202", sqlfunc.TO_DAYS("2012-03-01")),
                    Partition("from201203", sqlfunc.TO_DAYS("2012-04-01")),
                )
            else:
                pts = (
                    Partition("from201202", datetime.date(2012, 3, 1)),
                    Partition("from201203", datetime.date(2012, 4, 1)),
                )
            self.assertTrue(await pt.aioAddPartition(*pts))
            self.assertEqual(len(pt), 4)
            self.assertEqual(pt.partitions.names, pt_names)
            # . truncate
            with self.assertRaises(errors.PartitionArgumentError):
                await pt.aioTruncatePartition()
            with self.assertRaises(errors.PartitionArgumentError):
                await pt.aioTruncatePartition("xxx")
            self.assertTrue(
                await pt.aioTruncatePartition("from201202", pt["from201203"])
            )
            self.assertTrue(await pt.aioTruncatePartition("ALL"))
            # . coalesce [not supported]
            with self.assertRaises(sqlerr.OperationalError):
                await pt.aioCoalescePartition(3)
            # . reorganize
            with self.assertRaises(errors.PartitionArgumentError):
                await pt.aioReorganizePartition(None)
            with self.assertRaises(errors.PartitionArgumentError):
                await pt.aioReorganizePartition("xxx")
            if tb is db.range_tb:
                with self.assertRaises(errors.PartitionArgumentError):
                    await pt.aioReorganizePartition(db.range_c_tb.pt["from201203"])
                # . one to many
                self.assertTrue(
                    await pt.aioReorganizePartition(
                        "from201203",
                        Partition("from201203", sqlfunc.TO_DAYS("2012-04-01")),
                        Partition("future", "MAXVALUE"),
                    )
                )
                self.assertEqual(len(pt), 5)
                self.assertEqual(pt.partitions.names, pt_names + ("future",))
                # . many to one
                self.assertTrue(
                    await pt.aioReorganizePartition(
                        ["from201203", pt["future"]],
                        Partition("from201203", "MAXVALUE"),
                    )
                )
                self.assertEqual(len(pt), 4)
                self.assertEqual(pt.partitions.names, pt_names)
                # . many to many
                self.assertTrue(
                    await pt.aioReorganizePartition(
                        ["from201202", pt["from201203"]],
                        Partition("from201202", sqlfunc.TO_DAYS("2012-03-01")),
                        Partition("from201203", sqlfunc.TO_DAYS("2012-04-01")),
                        Partition("future", "MAXVALUE"),
                    )
                )
                self.assertEqual(len(pt), 5)
                self.assertEqual(pt.partitions.names, pt_names + ("future",))
            else:
                with self.assertRaises(errors.PartitionArgumentError):
                    await pt.aioReorganizePartition(db.range_tb.pt["from201203"])
                # . one to many
                self.assertTrue(
                    await pt.aioReorganizePartition(
                        "from201203",
                        Partition("from201203", datetime.date(2012, 4, 1)),
                        Partition("future", "MAXVALUE"),
                    )
                )
                self.assertEqual(len(pt), 5)
                self.assertEqual(pt.partitions.names, pt_names + ("future",))
                # . many to one
                self.assertTrue(
                    await pt.aioReorganizePartition(
                        ("from201203", pt["future"]),
                        Partition("from201203", "MAXVALUE"),
                    )
                )
                self.assertEqual(len(pt), 4)
                self.assertEqual(pt.partitions.names, pt_names)
                # . many to many
                self.assertTrue(
                    await pt.aioReorganizePartition(
                        ("from201202", pt["from201203"]),
                        Partition("from201202", datetime.date(2012, 3, 1)),
                        Partition("from201203", datetime.date(2012, 4, 1)),
                        Partition("future", "MAXVALUE"),
                    )
                )
                self.assertEqual(len(pt), 5)
                self.assertEqual(pt.partitions.names, pt_names + ("future",))
            self.assertTrue(await pt.aioDropPartition("future"))
            # . rebuild
            self.assertTrue(await pt.aioRebuildPartition("ALL"))

        # Maitenance: Analyze & Check & OPTIMIZE & REPAIR
        for tb in db.tables:
            pt: Partitioning = tb.partitioning
            for _pts in [
                ("from201202",),
                ("from201202", "from201203"),
                ("from201202", pt["from201203"]),
                ("ALL",),
            ]:
                # . analyze
                res = await pt.aioAnalyzePartition(*_pts)
                self.assertTrue(len(res) > 0)
                self.assertEqual(res[0]["Op"], "analyze")
                # . check
                res = await pt.aioCheckPartition(*_pts)
                self.assertTrue(len(res) > 0)
                self.assertEqual(res[0]["Op"], "check")
                # . optimize
                res = await pt.aioOptimizePartition(*_pts)
                self.assertTrue(len(res) > 0)
                self.assertEqual(res[0]["Op"], "optimize")
                # . repair
                res = await pt.aioRepairPartition(*_pts)
                self.assertTrue(len(res) > 0)
                self.assertEqual(res[0]["Op"], "repair")

        # Sync from remote
        class RangeTable2(Table):
            id: Column = Column(Define.BIGINT(auto_increment=True))
            name: Column = Column(Define.VARCHAR(255))
            dt: Column = Column(Define.DATETIME())
            pk: PrimaryKey = PrimaryKey("id", "dt")
            pt: Partitioning = (
                Partitioning(sqlfunc.TO_DAYS("dt"))
                .by_range(
                    Partition("start", 0),
                    Partition("from201301", sqlfunc.TO_DAYS("2013-02-01")),
                    Partition("from201302", sqlfunc.TO_DAYS("2013-03-01")),
                )
                .subpartition_by_hash(2, "id", linear=True)
            )

        class TestDatabase2(Database):
            range_c_tb: RangeTable2 = RangeTable2()

        db2 = TestDatabase2("test_db", self.get_pool())
        pt = db2.range_c_tb.partitioning

        # . before sync
        self.assertEqual(pt.partitioning_method, "RANGE")
        self.assertEqual(pt.partitioning_expression, "TO_DAYS(dt)")
        self.assertEqual(pt.subpartitioning_method, "LINEAR HASH")
        self.assertEqual(pt.subpartitioning_expression, "id")
        self.assertEqual(pt.partitions.names, ("start", "from201301", "from201302"))
        self.assertEqual(
            pt.partitions["start"].subpartitions.names, ("startsp0", "startsp1")
        )
        # . after sync
        pt_names = ("start", "from201201", "from201202", "from201203")
        self.assertTrue(await pt.aioSyncFromRemote())
        self.assertEqual(pt.partitioning_method, "RANGE COLUMNS")
        self.assertEqual(pt.partitioning_expression, "`dt`")
        self.assertIsNone(pt.subpartitioning_method)
        self.assertIsNone(pt.subpartitioning_expression)
        self.assertEqual(pt.partitions.names, pt_names)
        self.assertIsNone(pt.partitions["start"].subpartitions)
        # . partition count
        self.assertEqual(await pt.aioShowPartitionNames(), pt_names)
        self.assertTrue(await pt.aioShowPartitionRows())
        self.assertEqual(await pt.aioShowSubpartitionNames(), ())
        self.assertEqual(await pt.aioShowSubpartitionRows(), {})

        # Sync to remote
        pt: Partitioning = db.range_c_tb.partitioning
        self.assertTrue(await pt.aioRemove())
        self.assertTrue(await pt.aioSyncToRemote())  # re-create

        class RangeTable2(Table):
            id: Column = Column(Define.BIGINT(auto_increment=True))
            name: Column = Column(Define.VARCHAR(255))
            dt: Column = Column(Define.DATETIME())
            pk: PrimaryKey = PrimaryKey("id", "dt")
            pt: Partitioning = (
                Partitioning(sqlfunc.TO_DAYS("dt"))
                .by_range(
                    Partition("start", 0),
                    Partition("from201301", sqlfunc.TO_DAYS("2013-02-01")),
                    Partition("from201302", sqlfunc.TO_DAYS("2013-03-01")),
                    Partition("from201303", sqlfunc.TO_DAYS("2013-04-01")),
                    Partition("from201304", sqlfunc.TO_DAYS("2013-05-01")),
                )
                .subpartition_by_hash(2, "id", linear=True)
            )

        class TestDatabase2(Database):
            range_c_tb: RangeTable2 = RangeTable2()

        db2 = TestDatabase2("test_db", self.get_pool())
        pt = db2.range_c_tb.partitioning
        # . before sync
        meta = await pt.aioShowMetadata()
        self.assertTrue(meta["PARTITION_METHOD"], "RANGE COLUMNS")
        self.assertTrue(meta["PARTITION_EXPRESSION"], "`dt`")
        self.assertIsNone(meta["SUBPARTITION_METHOD"])
        self.assertIsNone(meta["SUBPARTITION_EXPRESSION"])
        self.assertEqual(
            meta.partition_names, ("start", "from201201", "from201202", "from201203")
        )
        self.assertEqual(meta.subpartition_names, ())
        # . after sync
        pt_names = ("start", "from201301", "from201302", "from201303", "from201304")
        subpt_names = (
            "startsp0",
            "startsp1",
            "from201301sp0",
            "from201301sp1",
            "from201302sp0",
            "from201302sp1",
            "from201303sp0",
            "from201303sp1",
            "from201304sp0",
            "from201304sp1",
        )
        self.assertTrue(await pt.aioSyncToRemote())
        meta = await pt.aioShowMetadata()
        self.assertTrue(meta["PARTITION_METHOD"], "RANGE")
        self.assertTrue(meta["PARTITION_EXPRESSION"], "to_days(`dt`)")
        self.assertTrue(meta["SUBPARTITION_METHOD"], "LINEAR HASH")
        self.assertTrue(meta["SUBPARTITION_EXPRESSION"], "`id`")
        self.assertEqual(meta.partition_names, pt_names)
        self.assertEqual(meta.subpartition_names, subpt_names)
        # . partition count
        self.assertEqual(await pt.aioShowPartitionNames(), pt_names)
        self.assertTrue(await pt.aioShowPartitionRows())
        self.assertEqual(await pt.aioShowSubpartitionNames(), subpt_names)
        self.assertTrue(await pt.aioShowSubpartitionRows())

        # Finished
        self.assertTrue(await db.aioDrop(True))
        self.log_ended("PARTITION BY RANGE")

    async def test_partition_by_list(self) -> None:
        self.log_start("PARTITION BY LIST")

        # Create & Exists & Drop
        class ListTable(Table):
            id: Column = Column(Define.BIGINT(auto_increment=True))
            store_id: Column = Column(Define.INT())
            pk: PrimaryKey = PrimaryKey("id", "store_id")
            pt: Partitioning = Partitioning("store_id").by_list(
                Partition("p0", 1, 3, 5, 7, 9),
                Partition("p1", 2, 4, 6, 8, 10),
                Partition("p2", 11, 13, 15, 17, 19),
                Partition("p3", 12, 14, 16, 18, 20),
            )

        class ListColumnsTable(Table):
            id: Column = Column(Define.BIGINT(auto_increment=True))
            store_id: Column = Column(Define.INT())
            pk: PrimaryKey = PrimaryKey("id", "store_id")
            pt: Partitioning = Partitioning("store_id").by_list(
                Partition("p0", 1, 3, 5, 7, 9),
                Partition("p1", 2, 4, 6, 8, 10),
                Partition("p2", 11, 13, 15, 17, 19),
                Partition("p3", 12, 14, 16, 18, 20),
                columns=True,
            )

        class TestDatabase(Database):
            list_tb: ListTable = ListTable()
            list_c_tb: ListColumnsTable = ListColumnsTable()

        db = TestDatabase("test_db", self.get_pool())
        self.assertTrue(await db.aioDrop(True))
        self.assertTrue(await db.aioInitialize())
        for tb in db.tables:
            pt: Partitioning = tb.partitioning
            self.assertTrue(await pt.aioExists())
            self.assertTrue(await pt.aioRemove())
            self.assertFalse(await pt.aioExists())
            self.assertTrue(await pt.aioCreate())
            self.assertTrue(await pt.aioExists())
        self.assertTrue(await db.aioDrop())

        # Show metadata
        with self.assertRaises(sqlerr.OperationalError):
            await db.list_tb.partitioning.aioShowMetadata()
        self.assertTrue(await db.aioInitialize())
        pt_names = ("p0", "p1", "p2", "p3")
        for tb in db.tables:
            pt: Partitioning = tb.partitioning
            meta = await pt.aioShowMetadata()
            self.assertEqual(meta.db_name, db.db_name)
            self.assertEqual(meta.tb_name, tb.tb_name)
            self.assertIn(meta.partitioning_method, ("LIST", "LIST COLUMNS"))
            self.assertEqual(meta.partitioning_expression, ("`store_id`"))
            self.assertIsNone(meta.subpartitioning_method)
            self.assertIsNone(meta.subpartitioning_expression)
            self.assertEqual(meta.partition_names, pt_names)
            self.assertEqual(meta.partition_count, 4)
            self.assertEqual(meta.subpartition_names, ())
            self.assertEqual(meta.subpartition_count, 0)

        # Management: Add & Drop & Truncate & Coalesce & Reorganize & Rebuild
        for tb in db.tables:
            pt: Partitioning = tb.partitioning
            self.assertEqual(len(pt), 4)
            self.assertEqual(meta.partition_names, pt_names)
            # . drop
            with self.assertRaises(errors.PartitionArgumentError):
                await pt.aioDropPartition()
            with self.assertRaises(errors.PartitionArgumentError):
                await pt.aioDropPartition("xxx")
            self.assertTrue(await pt.aioDropPartition("p2", pt["p3"]))
            self.assertEqual(len(pt), 2)
            self.assertEqual(pt.partitions.names, ("p0", "p1"))
            # . add
            with self.assertRaises(errors.PartitionArgumentError):
                await pt.aioAddPartition()
            with self.assertRaises(errors.PartitionArgumentError):
                await pt.aioAddPartition("p2")
            self.assertTrue(
                await pt.aioAddPartition(
                    Partition("p2", 11, 13, 15, 17, 19),
                    Partition("p3", 12, 14, 16, 18, 20),
                )
            )
            self.assertEqual(len(pt), 4)
            self.assertEqual(pt.partitions.names, pt_names)
            # . truncate
            with self.assertRaises(errors.PartitionArgumentError):
                await pt.aioTruncatePartition()
            with self.assertRaises(errors.PartitionArgumentError):
                await pt.aioTruncatePartition("xxx")
            self.assertTrue(await pt.aioTruncatePartition("p0", pt["p1"]))
            self.assertTrue(await pt.aioTruncatePartition("ALL"))
            # . coalesce [not supported]
            with self.assertRaises(sqlerr.OperationalError):
                await pt.aioCoalescePartition(3)
            # . reorganize
            with self.assertRaises(errors.PartitionArgumentError):
                await pt.aioReorganizePartition(None)
            with self.assertRaises(errors.PartitionArgumentError):
                await pt.aioReorganizePartition("xxx")
            if tb is db.list_tb:
                with self.assertRaises(errors.PartitionArgumentError):
                    await pt.aioReorganizePartition(db.list_c_tb.pt["p0"])
            else:
                with self.assertRaises(errors.PartitionArgumentError):
                    await pt.aioReorganizePartition(db.list_tb.pt["p0"])
            # . one to many
            self.assertTrue(
                await pt.aioReorganizePartition(
                    "p3",
                    Partition("p3", 12, 14, 16),
                    Partition("p4", 18, 20),
                )
            )
            self.assertEqual(len(pt), 5)
            self.assertEqual(pt.partitions.names, pt_names + ("p4",))
            # . many to one
            self.assertTrue(
                await pt.aioReorganizePartition(
                    ["p3", pt["p4"]],
                    Partition("p3", 12, 14, 16, 18, 20),
                )
            )
            self.assertEqual(len(pt), 4)
            self.assertEqual(pt.partitions.names, pt_names)
            # . many to many
            self.assertTrue(
                await pt.aioReorganizePartition(
                    ["p2", pt["p3"]],
                    Partition("p2", 11, 13, 15, 17, 19),
                    Partition("p3", 12, 14, 16),
                    Partition("p4", 18, 20),
                )
            )
            self.assertEqual(len(pt), 5)
            self.assertEqual(pt.partitions.names, pt_names + ("p4",))
            self.assertTrue(await pt.aioDropPartition("p4"))
            # . rebuild
            self.assertTrue(await pt.aioRebuildPartition("ALL"))

        # Maitenance: Analyze & Check & OPTIMIZE & REPAIR
        for tb in db.tables:
            pt: Partitioning = tb.partitioning
            for _pts in [("p0",), ("p0", "p1"), ("p0", pt["p1"]), ("ALL",)]:
                # . analyze
                res = await pt.aioAnalyzePartition(*_pts)
                self.assertTrue(len(res) > 0)
                self.assertEqual(res[0]["Op"], "analyze")
                # . check
                res = await pt.aioCheckPartition(*_pts)
                self.assertTrue(len(res) > 0)
                self.assertEqual(res[0]["Op"], "check")
                # . optimize
                res = await pt.aioOptimizePartition(*_pts)
                self.assertTrue(len(res) > 0)
                self.assertEqual(res[0]["Op"], "optimize")
                # . repair
                res = await pt.aioRepairPartition(*_pts)
                self.assertTrue(len(res) > 0)
                self.assertEqual(res[0]["Op"], "repair")

        # Sync from remote
        class ListTable2(Table):
            id: Column = Column(Define.BIGINT(auto_increment=True))
            store_id: Column = Column(Define.INT())
            pk: PrimaryKey = PrimaryKey("id", "store_id")
            pt: Partitioning = (
                Partitioning("store_id")
                .by_list(
                    Partition("p0", 1, 3, 5),
                    Partition("p1", 2, 4, 6),
                    Partition("p2", 11, 13, 15),
                )
                .subpartition_by_hash(2, "id", linear=True)
            )

        class TestDatabase2(Database):
            list_c_tb: ListTable2 = ListTable2()

        db2 = TestDatabase2("test_db", self.get_pool())
        pt = db2.list_c_tb.partitioning

        # . before sync
        self.assertEqual(pt.partitioning_method, "LIST")
        self.assertEqual(pt.partitioning_expression, "store_id")
        self.assertEqual(pt.subpartitioning_method, "LINEAR HASH")
        self.assertEqual(pt.subpartitioning_expression, "id")
        self.assertEqual(pt.partitions.names, ("p0", "p1", "p2"))
        self.assertEqual(pt.partitions["p0"].subpartitions.names, ("p0sp0", "p0sp1"))
        # . after sync
        self.assertTrue(await pt.aioSyncFromRemote())
        self.assertEqual(pt.partitioning_method, "LIST COLUMNS")
        self.assertEqual(pt.partitioning_expression, "`store_id`")
        self.assertIsNone(pt.subpartitioning_method)
        self.assertIsNone(pt.subpartitioning_expression)
        self.assertEqual(pt.partitions.names, ("p0", "p1", "p2", "p3"))
        self.assertIsNone(pt.partitions["p0"].subpartitions)

        # Sync to remote
        pt: Partitioning = db.list_c_tb.partitioning
        self.assertTrue(await pt.aioRemove())
        self.assertTrue(await pt.aioSyncToRemote())  # re-create

        class ListTable2(Table):
            id: Column = Column(Define.BIGINT(auto_increment=True))
            store_id: Column = Column(Define.INT())
            pk: PrimaryKey = PrimaryKey("id", "store_id")
            pt: Partitioning = (
                Partitioning("store_id")
                .by_list(
                    Partition("p0", 1, 3, 5),
                    Partition("p1", 2, 4, 6),
                    Partition("p2", 11, 13, 15),
                    Partition("p3", 12, 14, 16),
                    Partition("p4", 18, 20),
                )
                .subpartition_by_hash(2, "id", linear=True)
            )

        class TestDatabase2(Database):
            list_c_tb: ListTable2 = ListTable2()

        db2 = TestDatabase2("test_db", self.get_pool())
        pt = db2.list_c_tb.partitioning
        # . before sync
        meta = await pt.aioShowMetadata()
        self.assertTrue(meta["PARTITION_METHOD"], "LIST COLUMNS")
        self.assertTrue(meta["PARTITION_EXPRESSION"], "`store_id`")
        self.assertIsNone(meta["SUBPARTITION_METHOD"])
        self.assertIsNone(meta["SUBPARTITION_EXPRESSION"])
        self.assertEqual(meta.partition_names, ("p0", "p1", "p2", "p3"))
        self.assertEqual(meta.subpartition_names, ())
        # . after sync
        self.assertTrue(await pt.aioSyncToRemote())
        meta = await pt.aioShowMetadata()
        self.assertTrue(meta["PARTITION_METHOD"], "LIST")
        self.assertTrue(meta["PARTITION_EXPRESSION"], "`store_id`")
        self.assertTrue(meta["SUBPARTITION_METHOD"], "LINEAR HASH")
        self.assertTrue(meta["SUBPARTITION_EXPRESSION"], "`id`")
        self.assertEqual(
            meta.partition_names,
            ("p0", "p1", "p2", "p3", "p4"),
        )
        self.assertEqual(
            meta.subpartition_names,
            (
                "p0sp0",
                "p0sp1",
                "p1sp0",
                "p1sp1",
                "p2sp0",
                "p2sp1",
                "p3sp0",
                "p3sp1",
                "p4sp0",
                "p4sp1",
            ),
        )

        # Finished
        self.assertTrue(await db.aioDrop(True))
        self.log_ended("PARTITION BY LIST")


if __name__ == "__main__":
    HOST = "localhost"
    PORT = 3306
    USER = "root"
    PSWD = "Password_123456"

    for case in (
        TestPartitionDefinition,
        TestPartitionCopy,
        TestPartitioningSyncSQL,
        TestPartitioningAsyncSQL,
    ):
        test_case = case(HOST, PORT, USER, PSWD)
        if not iscoroutinefunction(test_case.test_all):
            test_case.test_all()
        else:
            asyncio.run(test_case.test_all())
