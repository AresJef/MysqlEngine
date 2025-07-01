import asyncio, time, unittest, datetime
from inspect import iscoroutinefunction
from sqlcycli import sqlfunc, Pool, Connection
from mysqlengine import errors
from mysqlengine.index import Index
from mysqlengine.table import Table
from mysqlengine.database import Database
from mysqlengine.constraint import PrimaryKey
from mysqlengine.partition import Partition, Partitioning
from mysqlengine.column import Define, Definition, Column, Columns
from mysqlengine.dml import SelectDML, InsertDML, UpdateDML, DeleteDML, WithDML


# Test Database & Table -----------------------------------------------------------------------------------
class TestTable(Table):
    id: Column = Column(Define.BIGINT(auto_increment=True))
    name: Column = Column(Define.VARCHAR(255))
    price: Column = Column(Define.DECIMAL(12, 2))
    dt: Column = Column(Define.DATETIME())
    pk: PrimaryKey = PrimaryKey("id", "dt")
    idx1: Index = Index("name", "price", "dt")
    idx2: Index = Index("price", "name", "dt")
    pt: Partitioning = Partitioning(sqlfunc.TO_DAYS("dt")).by_range(
        Partition("start", 0),
        Partition("from201201", sqlfunc.TO_DAYS("2012-02-01")),
        Partition("from201202", sqlfunc.TO_DAYS("2012-03-01")),
        Partition("from201203", sqlfunc.TO_DAYS("2012-04-01")),
    )


class TestDatabase(Database):
    tb1: TestTable = TestTable()
    tb2: TestTable = TestTable()
    tb3: TestTable = TestTable()


# TestCase ------------------------------------------------------------------------------------------------
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

    def setup_column(self, col: Column, name: str = "col") -> Column:
        col._set_position(1)
        col.set_name(name)
        col.setup("tb1", "db1", "utf8", None, self.get_pool())
        return col

    def gen_columns(self) -> Columns:
        return Columns(
            *[
                self.setup_column(Column(d), f"col_{d.data_type.lower()}")
                for d in self.dtypes
            ]
        )

    def prepareDatabase(self) -> TestDatabase:
        db = TestDatabase(
            "db",
            self.get_pool(
                sql_mode="STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION"
            ),
        )
        db.Drop(True)
        db.Initialize(True)
        return db

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

    # assert
    def assertClauseEqual(self, clause: object, sql: str, indent: int = 0) -> None:
        """Assert that the generated SQL clause is equal to the expected SQL string."""
        sql_c = clause.clause(None if indent == 0 else indent * "\t")
        self.assertEqual(sql_c, sql)


class TestSelectClause(TestCase):
    name: str = "SELECT Clause"

    # Utils
    def setupDML(self) -> SelectDML:
        return SelectDML("db", self.get_pool())

    # Tests
    def test_all(self) -> None:
        self.test_select_clause()
        self.test_from_clause()
        self.test_join_clause()
        self.test_index_hint_clause()
        self.test_where_clause()
        self.test_group_by_clause()
        self.test_having_clause()
        self.test_window_clause()
        self.test_order_by_clause()
        self.test_limit_clause()
        self.test_locking_read_clause()
        self.test_into_clause()

    def test_select_clause(self) -> None:
        self.log_start("SELECT")

        cols = self.gen_columns()
        dml = self.setupDML()

        # fmt: off
        # . expression: str
        self.assertClauseEqual(dml._gen_select_clause(("col1",)), "SELECT col1")
        # . expression: Element
        self.assertClauseEqual(dml._gen_select_clause((cols["col_bigint"],)), "SELECT col_bigint")
        # . expression: Elements
        self.assertClauseEqual(
            dml._gen_select_clause((cols.search_type(str),)),
            "SELECT\n\t"
            "col_char,\n\t"
            "col_enum,\n\t"
            "col_longtext,\n\t"
            "col_mediumtext,\n\t"
            "col_text,\n\t"
            "col_tinytext,\n\t"
            "col_varchar"
        )
        # . expression: SQLFunction
        self.assertClauseEqual(dml._gen_select_clause((sqlfunc.TO_DAYS(cols["col_date"]),)), "SELECT TO_DAYS(col_date)")
        # . expression: list
        self.assertClauseEqual(dml._gen_select_clause(("col1", "col2")), "SELECT\n\tcol1,\n\tcol2")
        # . expression: tuple
        self.assertClauseEqual(dml._gen_select_clause(("col1", "col2")), "SELECT\n\tcol1,\n\tcol2")
        # . expression: mixed
        self.assertClauseEqual(
            dml._gen_select_clause(
                (
                    "col1",
                    cols["col_bigint"],
                    cols.search_type(str),
                    sqlfunc.TO_DAYS(cols["col_date"]),
                    ["col2", "col3"],
                    ["col4", "col5"],
                )
            ),
            "SELECT\n\t"
            "col1,\n\t"
            "col_bigint,\n\t"
            "col_char,\n\t"
            "col_enum,\n\t"
            "col_longtext,\n\t"
            "col_mediumtext,\n\t"
            "col_text,\n\t"
            "col_tinytext,\n\t"
            "col_varchar,\n\t"
            "TO_DAYS(col_date),\n\t"
            "col2,\n\t"
            "col3,\n\t"
            "col4,\n\t"
            "col5"
        )
        # . expression: invalid
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_select_clause(())
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_select_clause((1,))
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_select_clause((None,))
        # . distinct
        self.assertClauseEqual(
            dml._gen_select_clause(("col1",), distinct=True), 
            "SELECT DISTINCT col1",
        )
        # . high_priority
        self.assertClauseEqual(
            dml._gen_select_clause(("col1",), high_priority=True), 
            "SELECT HIGH_PRIORITY col1",
        )
        # . straight_join
        self.assertClauseEqual(
            dml._gen_select_clause(("col1",), straight_join=True), 
            "SELECT STRAIGHT_JOIN col1",
        )
        # . sql_buffer_result
        self.assertClauseEqual(
            dml._gen_select_clause(("col1",), sql_buffer_result=True), 
            "SELECT SQL_BUFFER_RESULT col1",
        )
        # . mixed
        self.assertClauseEqual(
            dml._gen_select_clause(
                ("col1",),
                distinct=True,
                high_priority=True,
                straight_join=True,
                sql_buffer_result=True,
            ),
            "SELECT DISTINCT HIGH_PRIORITY STRAIGHT_JOIN SQL_BUFFER_RESULT col1",
        )
        # . indent
        self.assertClauseEqual(dml._gen_select_clause(("col1",)), "\tSELECT col1", 1)
        self.assertClauseEqual(
            dml._gen_select_clause(("col1", "col2", "col3", "col4")),
            "\tSELECT\n\t\tcol1,\n\t\tcol2,\n\t\tcol3,\n\t\tcol4", 1
        )
        self.assertClauseEqual(dml._gen_select_clause(("col1",)), "\t\tSELECT col1", 2)
        self.assertClauseEqual(
            dml._gen_select_clause(("col1", "col2", "col3", "col4")),
            "\t\tSELECT\n\t\t\tcol1,\n\t\t\tcol2,\n\t\t\tcol3,\n\t\t\tcol4", 2
        )
        self.assertClauseEqual(dml._gen_select_clause(("col1",)), "SELECT col1", 0)
        self.assertClauseEqual(
            dml._gen_select_clause(("col1", "col2", "col3", "col4")),
            "SELECT\n\tcol1,\n\tcol2,\n\tcol3,\n\tcol4", 0
        )
        # fmt: on

        self.log_ended("SELECT")

    def test_from_clause(self) -> None:
        self.log_start("FROM")

        db = TestDatabase("db", self.get_pool())
        dml = self.setupDML()

        # fmt: off
        # . table: str
        self.assertClauseEqual(dml._gen_from_clause("tb", ()), "FROM tb AS t0")
        self.assertClauseEqual(dml._gen_from_clause("db.tb", ()), "FROM db.tb AS t1")
        self.assertClauseEqual(dml._gen_from_clause("`tb`", ()), "FROM `tb` AS t2")
        self.assertClauseEqual(dml._gen_from_clause("tb", (), "t"), "FROM tb AS t")
        # . table: Table
        self.assertClauseEqual(dml._gen_from_clause(db.tb1, ()), "FROM db.tb1 AS t3")
        self.assertClauseEqual(dml._gen_from_clause(db.tb1, (), "t"), "FROM db.tb1 AS t")
        # . table error
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_from_clause(None, ())
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_from_clause("", ())
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_from_clause(("t1", "t2"), ())
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_from_clause((), ())
        # . partition
        self.assertClauseEqual(dml._gen_from_clause(db.tb1, ("p0", "p1")), "FROM db.tb1 PARTITION (p0, p1) AS t4")
        self.assertClauseEqual(dml._gen_from_clause(db.tb1, ("p0", "p1"), "t"), "FROM db.tb1 PARTITION (p0, p1) AS t")
        # . partition error
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_from_clause(db.tb1, ("",))
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_from_clause(db.tb1, ("p0", 2))
        # . alias error
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_from_clause(db.tb1, (), 1)
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_from_clause(db.tb1, (), "")

        # . indent
        self.assertClauseEqual(dml._gen_from_clause("tb", (), "t"), "\tFROM tb AS t", 1)
        self.assertClauseEqual(dml._gen_from_clause("tb", (), "t"), "\t\tFROM tb AS t", 2)
        self.assertClauseEqual(dml._gen_from_clause("tb", (), "t"), "FROM tb AS t", 0)
        # fmt: on

        self.log_ended("FROM")

    def test_join_clause(self) -> None:
        self.log_start("JOIN")

        db = TestDatabase("db", self.get_pool())
        dml = self.setupDML()

        # fmt: off
        # . table
        self.assertClauseEqual(dml._gen_inner_join_clause("tb", ()), "INNER JOIN tb AS t0")
        self.assertClauseEqual(dml._gen_inner_join_clause("db.tb", ()), "INNER JOIN db.tb AS t1")
        self.assertClauseEqual(dml._gen_inner_join_clause("`tb`", ()), "INNER JOIN `tb` AS t2")
        self.assertClauseEqual(dml._gen_inner_join_clause("tb", (), alias="t"), "INNER JOIN tb AS t")
        self.assertClauseEqual(dml._gen_inner_join_clause(db.tb1, ()), "INNER JOIN db.tb1 AS t3")
        self.assertClauseEqual(dml._gen_inner_join_clause(db.tb1, (), alias="t"), "INNER JOIN db.tb1 AS t")
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_inner_join_clause(None, ())
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_inner_join_clause("", ())
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_inner_join_clause(("t1", "t2"), ())
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_inner_join_clause((), ())
        # . on
        self.assertClauseEqual(
            dml._gen_inner_join_clause(db.tb1, ("tb.id = t.id",)),
            "INNER JOIN db.tb1 AS t4\n\tON tb.id = t.id",
        )
        self.assertClauseEqual(
            dml._gen_inner_join_clause(db.tb1, ("tb.id = t.id", "tb.name = t.name")),
            "INNER JOIN db.tb1 AS t5\n\tON tb.id = t.id\n\tAND tb.name = t.name",
        )
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_inner_join_clause(db.tb1, (2,))
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_inner_join_clause(db.tb1, (None,))
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_inner_join_clause(db.tb1, ("",))
        # . using
        self.assertClauseEqual(
            dml._gen_inner_join_clause(db.tb1, (), ("id", "name")),
            "INNER JOIN db.tb1 AS t6\n\tUSING (id, name)",
        )
        self.assertClauseEqual(
            dml._gen_inner_join_clause(db.tb1, (), (db.tb1.id, db.tb1.name)),
            "INNER JOIN db.tb1 AS t7\n\tUSING (id, name)",
        )
        self.assertClauseEqual(
            dml._gen_inner_join_clause(db.tb1, (), (db.tb1.columns,)),
            "INNER JOIN db.tb1 AS t8\n\tUSING (id, name, price, dt)",
        )
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_inner_join_clause(db.tb1, (), (2,))
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_inner_join_clause(db.tb1, (), (None,))
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_inner_join_clause(db.tb1, (), ("",))
        # . partition
        self.assertClauseEqual(
            dml._gen_inner_join_clause(db.tb1, (), None, "p0"),
            "INNER JOIN db.tb1 PARTITION (p0) AS t9",
        )
        self.assertClauseEqual(
            dml._gen_inner_join_clause(db.tb1, (), None, ["p0", "p1"]),
            "INNER JOIN db.tb1 PARTITION (p0, p1) AS t10",
        )
        self.assertClauseEqual(
            dml._gen_inner_join_clause(db.tb1, (), None, ["p0", "p1"], "t"),
            "INNER JOIN db.tb1 PARTITION (p0, p1) AS t",
        )
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_inner_join_clause(db.tb1, (), None, "")
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_inner_join_clause(db.tb1, (), None, ("",))
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_inner_join_clause(db.tb1, (), None, 2)
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_inner_join_clause(db.tb1, (), None, (2,))
        # . alias error
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_inner_join_clause(db.tb1, (), alias=1)
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_inner_join_clause(db.tb1, (), alias="")
        # . left join
        self.assertClauseEqual(
            dml._gen_left_join_clause(db.tb1, ("t1.id = t2.id",), alias="t"),
            "LEFT JOIN db.tb1 AS t\n\tON t1.id = t2.id",
        )
        self.assertClauseEqual(
            dml._gen_left_join_clause(db.tb1, (), "id", alias="t"),
            "LEFT JOIN db.tb1 AS t\n\tUSING (id)",
        )
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_left_join_clause(db.tb1, (), None, alias="t")
        # . right join
        self.assertClauseEqual(
            dml._gen_right_join_clause(db.tb1, ("t1.id = t2.id",), alias="t"),
            "RIGHT JOIN db.tb1 AS t\n\tON t1.id = t2.id",
        )
        self.assertClauseEqual(
            dml._gen_right_join_clause(db.tb1, (), "id", alias="t"),
            "RIGHT JOIN db.tb1 AS t\n\tUSING (id)",
        )
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_right_join_clause(db.tb1, (), None, alias="t")
        # . straight join
        self.assertClauseEqual(
            dml._gen_straight_join_clause(db.tb1, ("t1.id = t2.id",), None, alias="t"),
            "STRAIGHT_JOIN db.tb1 AS t\n\tON t1.id = t2.id",
        )
        self.assertClauseEqual(
            dml._gen_straight_join_clause(db.tb1, (), "id", alias="t"),
            "STRAIGHT_JOIN db.tb1 AS t\n\tUSING (id)",
        )
        self.assertClauseEqual(
            dml._gen_straight_join_clause(db.tb1, (), None, alias="t"),
            "STRAIGHT_JOIN db.tb1 AS t",
        )
        # . cross join
        self.assertClauseEqual(
            dml._gen_cross_join_clause(db.tb1, ("t1.id = t2.id",), None, alias="t"),
            "CROSS JOIN db.tb1 AS t\n\tON t1.id = t2.id",
        )
        self.assertClauseEqual(
            dml._gen_cross_join_clause(db.tb1, (), "id", alias="t"),
            "CROSS JOIN db.tb1 AS t\n\tUSING (id)",
        )
        self.assertClauseEqual(
            dml._gen_cross_join_clause(db.tb1, (), None, alias="t"),
            "CROSS JOIN db.tb1 AS t",
        )
        # . natural join
        self.assertClauseEqual(
            dml._gen_natural_join_clause(db.tb1, "INNER", alias="t"),
            "NATURAL INNER JOIN db.tb1 AS t",
        )
        self.assertClauseEqual(
            dml._gen_natural_join_clause(db.tb1, "INNER JOIN", alias="t"),
            "NATURAL INNER JOIN db.tb1 AS t",
        )
        self.assertClauseEqual(
            dml._gen_natural_join_clause(db.tb1, "LEFT", alias="t"),
            "NATURAL LEFT JOIN db.tb1 AS t",
        )
        self.assertClauseEqual(
            dml._gen_natural_join_clause(db.tb1, "LEFT JOIN", alias="t"),
            "NATURAL LEFT JOIN db.tb1 AS t",
        )
        self.assertClauseEqual(
            dml._gen_natural_join_clause(db.tb1, "RIGHT", alias="t"),
            "NATURAL RIGHT JOIN db.tb1 AS t",
        )
        self.assertClauseEqual(
            dml._gen_natural_join_clause(db.tb1, "RIGHT JOIN", alias="t"),
            "NATURAL RIGHT JOIN db.tb1 AS t",
        )
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_natural_join_clause(db.tb1, "", alias="t")
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_natural_join_clause(db.tb1, 1, alias="t")
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_natural_join_clause(db.tb1, "x", alias="t")
        # . indent
        self.assertClauseEqual(
            dml._gen_inner_join_clause(db.tb1, ("tb.i = t.i", "tb.d = t.d"), alias="t"),
            "\tINNER JOIN db.tb1 AS t\n\t\tON tb.i = t.i\n\t\tAND tb.d = t.d", 1
        )
        self.assertClauseEqual(
            dml._gen_inner_join_clause(db.tb1, (), using=("id", "name"), alias="t"),
            "\tINNER JOIN db.tb1 AS t\n\t\tUSING (id, name)", 1
        )
        self.assertClauseEqual(
            dml._gen_inner_join_clause(db.tb1, ("tb.i = t.i", "tb.d = t.d"), alias="t"),
            "\t\tINNER JOIN db.tb1 AS t\n\t\t\tON tb.i = t.i\n\t\t\tAND tb.d = t.d", 2
        )
        self.assertClauseEqual(
            dml._gen_inner_join_clause(db.tb1, (), ("id", "name"), alias="t"),
            "\t\tINNER JOIN db.tb1 AS t\n\t\t\tUSING (id, name)", 2
        )
        self.assertClauseEqual(
            dml._gen_inner_join_clause(db.tb1, ("tb.i = t.i", "tb.d = t.d"), alias="t"),
            "INNER JOIN db.tb1 AS t\n\tON tb.i = t.i\n\tAND tb.d = t.d", 0
        )
        self.assertClauseEqual(
            dml._gen_inner_join_clause(db.tb1, (), using=("id", "name"), alias="t"),
            "INNER JOIN db.tb1 AS t\n\tUSING (id, name)", 0
        )
        # fmt: on

        self.log_ended("JOIN")

    def test_index_hint_clause(self) -> None:
        self.log_start("INDEX HINT")

        db = TestDatabase("db", self.get_pool())
        dml = self.setupDML()

        # fmt: off
        # . hint type
        self.assertClauseEqual(dml._gen_use_index_clause(("idx",)), "USE INDEX (idx)")
        self.assertClauseEqual(dml._gen_force_index_clause(("idx",)), "FORCE INDEX (idx)")
        self.assertClauseEqual(dml._gen_ignore_index_clause(("idx",)), "IGNORE INDEX (idx)")
        # . hint scope
        self.assertClauseEqual(dml._gen_use_index_clause(("idx",), "JOIN"), "USE INDEX FOR JOIN (idx)")
        self.assertClauseEqual(dml._gen_use_index_clause(("idx",), "Order By"), "USE INDEX FOR ORDER BY (idx)")
        self.assertClauseEqual(dml._gen_use_index_clause(("idx",), "group by"), "USE INDEX FOR GROUP BY (idx)")
        self.assertClauseEqual(dml._gen_use_index_clause(("idx1", "idx2"), "group by"), "USE INDEX FOR GROUP BY (idx1, idx2)")
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_use_index_clause(("idx",), 1)
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_use_index_clause(("idx",), "x")
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_use_index_clause(("idx",), "group")
        # . hint index
        self.assertClauseEqual(dml._gen_use_index_clause((db.tb1.idx1,)), "USE INDEX (idx1)")
        self.assertClauseEqual(dml._gen_use_index_clause((db.tb1.idx1, "idx2")), "USE INDEX (idx1, idx2)")
        self.assertClauseEqual(dml._gen_use_index_clause((db.tb1.idx1, db.tb1.idx2)), "USE INDEX (idx1, idx2)")
        self.assertClauseEqual(dml._gen_use_index_clause((db.tb1.indexes,)), "USE INDEX (idx1, idx2)")
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_use_index_clause(())
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_use_index_clause(("",))
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_use_index_clause((1,))
        # . indent
        self.assertClauseEqual(dml._gen_use_index_clause(("idx",)), "\tUSE INDEX (idx)", 1)
        self.assertClauseEqual(dml._gen_use_index_clause(("idx",), "JOIN"), "\tUSE INDEX FOR JOIN (idx)", 1)
        self.assertClauseEqual(dml._gen_use_index_clause(("idx",)), "\t\tUSE INDEX (idx)", 2)
        self.assertClauseEqual(dml._gen_use_index_clause(("idx",), "JOIN"), "\t\tUSE INDEX FOR JOIN (idx)", 2)
        self.assertClauseEqual(dml._gen_use_index_clause(("idx",)), "USE INDEX (idx)", 0)
        self.assertClauseEqual(dml._gen_use_index_clause(("idx",), "JOIN"), "USE INDEX FOR JOIN (idx)", 0)
        # fmt: on

        self.log_ended("INDEX HINT")

    def test_where_clause(self) -> None:
        self.log_start("WHERE")

        db = TestDatabase("db", self.get_pool())
        dml = self.setupDML()
        # fmt: off
        # . conditions
        self.assertClauseEqual(dml._gen_where_clause(("col1 > 1",)), "WHERE col1 > 1")
        self.assertClauseEqual(
            dml._gen_where_clause(("col1 > 1", "col2 < 2")), 
            "WHERE col1 > 1\n\tAND col2 < 2"
        )
        self.assertClauseEqual(
            dml._gen_where_clause(("col1 > 1", "col2 < 2", "col3 = 3")), 
            "WHERE col1 > 1\n\tAND col2 < 2\n\tAND col3 = 3"
        )
        self.assertClauseEqual(
            dml._gen_where_clause(("col1 > 1", "col2 = 2", "OR col3 = 3")), 
            "WHERE col1 > 1\n\tAND col2 = 2\n\tOR col3 = 3"
        )
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_where_clause(())
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_where_clause((2,))
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_where_clause((None,))
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_where_clause(("",))
        # . IN operator
        self.assertClauseEqual(
            dml._gen_where_clause((), in_conds={"id": (1, 2, 3)}),
            "WHERE id IN (1,2,3)"
        )
        self.assertClauseEqual(
            dml._gen_where_clause((), in_conds={"id": (1, 2, 3), "OR name": "a"}),
            "WHERE id IN (1,2,3)\n\tOR name IN ('a')"
        )
        self.assertClauseEqual(
            dml._gen_where_clause(("col1 > 1",), in_conds={"id": (1, 2, 3), "OR name": "a"}),
            "WHERE col1 > 1\n\tAND id IN (1,2,3)\n\tOR name IN ('a')"
        )
        self.assertClauseEqual(
            dml._gen_where_clause((), in_conds={"id": db.Select("*").From(db.tb1).Where("id > 1"), "OR name": "a"}),
            "WHERE id IN (\n\t\t"
                "SELECT *\n\t\t"
                "FROM db.tb1 AS t0\n\t\t"
                "WHERE id > 1\n\t"
            ")\n\t"
            "OR name IN ('a')",
        )
        self.assertClauseEqual(
            dml._gen_where_clause(("col1 > 1",), in_conds={"id": db.Select("*").From(db.tb1).Where("id > 1"), "OR name": "a"}),
            "WHERE col1 > 1\n\t"
            "AND id IN (\n\t\t"
                "SELECT *\n\t\t"
                "FROM db.tb1 AS t0\n\t\t"
                "WHERE id > 1\n\t"
            ")\n\t"
            "OR name IN ('a')",
        )
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_where_clause((), in_conds="id")
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_where_clause((), in_conds={1: (1,2,3)})
        # . NOT IN operator
        self.assertClauseEqual(
            dml._gen_where_clause((), not_in_conds={"id": (1, 2, 3)}),
            "WHERE id NOT IN (1,2,3)"
        )
        self.assertClauseEqual(
            dml._gen_where_clause((), not_in_conds={"id": (1, 2, 3), "OR name": "a"}),
            "WHERE id NOT IN (1,2,3)\n\tOR name NOT IN ('a')"
        )
        self.assertClauseEqual(
            dml._gen_where_clause(("col1 > 1",), not_in_conds={"id": (1, 2, 3), "OR name": "a"}),
            "WHERE col1 > 1\n\tAND id NOT IN (1,2,3)\n\tOR name NOT IN ('a')"
        )
        self.assertClauseEqual(
            dml._gen_where_clause((), not_in_conds={"id": db.Select("*").From(db.tb1).Where("id > 1"), "OR name": "a"}),
            "WHERE id NOT IN (\n\t\t"
                "SELECT *\n\t\t"
                "FROM db.tb1 AS t0\n\t\t"
                "WHERE id > 1\n\t"
            ")\n\t"
            "OR name NOT IN ('a')",
        )
        self.assertClauseEqual(
            dml._gen_where_clause(("col1 > 1",), not_in_conds={"id": db.Select("*").From(db.tb1).Where("id > 1"), "OR name": "a"}),
            "WHERE col1 > 1\n\t"
            "AND id NOT IN (\n\t\t"
                "SELECT *\n\t\t"
                "FROM db.tb1 AS t0\n\t\t"
                "WHERE id > 1\n\t"
            ")\n\t"
            "OR name NOT IN ('a')",
        )
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_where_clause((), not_in_conds="id")
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_where_clause((), not_in_conds={1: (1,2,3)})
        # . indent
        self.assertClauseEqual(
            dml._gen_where_clause(("col1 > 1", "col2 < 2", "col3 = 3")),
            "\tWHERE col1 > 1\n\t\tAND col2 < 2\n\t\tAND col3 = 3", 1
        )
        self.assertClauseEqual(
            dml._gen_where_clause(("col1 > 1",), not_in_conds={"id": db.Select("*").From(db.tb1).Where("id > 1"), "OR name": "a"}),
            "\tWHERE col1 > 1\n\t"
            "\tAND id NOT IN (\n\t\t"
                "\tSELECT *\n\t\t"
                "\tFROM db.tb1 AS t0\n\t\t"
                "\tWHERE id > 1\n\t"
            "\t)\n\t"
            "\tOR name NOT IN ('a')", 1
        )
        self.assertClauseEqual(
            dml._gen_where_clause(("col1 > 1", "col2 < 2", "col3 = 3")),
            "\t\tWHERE col1 > 1\n\t\t\tAND col2 < 2\n\t\t\tAND col3 = 3", 2
        )
        self.assertClauseEqual(
            dml._gen_where_clause(("col1 > 1",), not_in_conds={"id": db.Select("*").From(db.tb1).Where("id > 1"), "OR name": "a"}),
            "\t\tWHERE col1 > 1\n\t"
            "\t\tAND id NOT IN (\n\t\t"
                "\t\tSELECT *\n\t\t"
                "\t\tFROM db.tb1 AS t0\n\t\t"
                "\t\tWHERE id > 1\n\t"
            "\t\t)\n\t"
            "\t\tOR name NOT IN ('a')", 2
        )
        # fmt: on

        self.log_ended("WHERE")

    def test_group_by_clause(self) -> None:
        self.log_start("GROUP BY")

        db = TestDatabase("db", self.get_pool())
        dml = self.setupDML()
        # fmt: off
        # . column
        self.assertClauseEqual(dml._gen_group_by_clause(("col1",)), "GROUP BY col1")
        self.assertClauseEqual(dml._gen_group_by_clause((db.tb1.name,)), "GROUP BY name")
        self.assertClauseEqual(dml._gen_group_by_clause(("col1", db.tb1.name)), "GROUP BY col1, name")
        self.assertClauseEqual(dml._gen_group_by_clause((sqlfunc.YEAR("col1"),)), "GROUP BY YEAR(col1)")
        # . column error
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_group_by_clause((2,))
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_group_by_clause((None,))
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_group_by_clause(("",))
        # . with rollup
        self.assertClauseEqual(dml._gen_group_by_clause(("col1",), with_rollup=True), "GROUP BY col1 WITH ROLLUP")
        self.assertClauseEqual(
            dml._gen_group_by_clause(("col1", db.tb1.name), with_rollup=True),
            "GROUP BY col1, name WITH ROLLUP"
        )
        self.assertClauseEqual(
            dml._gen_group_by_clause((sqlfunc.YEAR("col1"),), with_rollup=True),
            "GROUP BY YEAR(col1) WITH ROLLUP"
        )
        # . indent
        self.assertClauseEqual(dml._gen_group_by_clause(("col1",)), "\tGROUP BY col1", 1)
        self.assertClauseEqual(dml._gen_group_by_clause(("col1",), with_rollup=True), "\t\tGROUP BY col1 WITH ROLLUP", 2)
        self.assertClauseEqual(dml._gen_group_by_clause(("col1",), with_rollup=True), "GROUP BY col1 WITH ROLLUP", 0)
        # fmt: on

        self.log_ended("GROUP BY")

    def test_having_clause(self) -> None:
        self.log_start("HAVING")

        db = TestDatabase("db", self.get_pool())
        dml = self.setupDML()
        # fmt: off
        # . condition
        self.assertClauseEqual(dml._gen_having_clause(("col1 > 1",)), "HAVING col1 > 1")
        self.assertClauseEqual(
            dml._gen_having_clause(("col1 > 1", "col2 < 2")), 
            "HAVING col1 > 1\n\tAND col2 < 2"
        )
        self.assertClauseEqual(
            dml._gen_having_clause(("col1 > 1", "col2 < 2", "col3 = 3")), 
            "HAVING col1 > 1\n\tAND col2 < 2\n\tAND col3 = 3"
        )
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_having_clause(())
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_having_clause((2,))
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_having_clause((None,))
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_having_clause(("",))
        # . IN operator
        self.assertClauseEqual(
            dml._gen_having_clause((), in_conds={"id": (1, 2, 3)}),
            "HAVING id IN (1,2,3)"
        )
        self.assertClauseEqual(
            dml._gen_having_clause((), in_conds={"id": (1, 2, 3), "OR name": "a"}),
            "HAVING id IN (1,2,3)\n\tOR name IN ('a')"
        )
        self.assertClauseEqual(
            dml._gen_having_clause(("col1 > 1",), in_conds={"id": (1, 2, 3), "OR name": "a"}),
            "HAVING col1 > 1\n\tAND id IN (1,2,3)\n\tOR name IN ('a')"
        )
        self.assertClauseEqual(
            dml._gen_having_clause((), in_conds={"id": db.Select("*").From(db.tb1).Where("id > 1"), "OR name": "a"}),
            "HAVING id IN (\n\t\t"
                "SELECT *\n\t\t"
                "FROM db.tb1 AS t0\n\t\t"
                "WHERE id > 1\n\t"
            ")\n\t"
            "OR name IN ('a')",
        )
        self.assertClauseEqual(
            dml._gen_having_clause(("col1 > 1",), in_conds={"id": db.Select("*").From(db.tb1).Where("id > 1"), "OR name": "a"}),
            "HAVING col1 > 1\n\t"
            "AND id IN (\n\t\t"
                "SELECT *\n\t\t"
                "FROM db.tb1 AS t0\n\t\t"
                "WHERE id > 1\n\t"
            ")\n\t"
            "OR name IN ('a')",
        )
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_having_clause((), in_conds="id")
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_having_clause((), in_conds={1: (1,2,3)})
        # . NOT IN operator
        self.assertClauseEqual(
            dml._gen_having_clause((), not_in_conds={"id": (1, 2, 3)}),
            "HAVING id NOT IN (1,2,3)"
        )
        self.assertClauseEqual(
            dml._gen_having_clause((), not_in_conds={"id": (1, 2, 3), "OR name": "a"}),
            "HAVING id NOT IN (1,2,3)\n\tOR name NOT IN ('a')"
        )
        self.assertClauseEqual(
            dml._gen_having_clause(("col1 > 1",), not_in_conds={"id": (1, 2, 3), "OR name": "a"}),
            "HAVING col1 > 1\n\tAND id NOT IN (1,2,3)\n\tOR name NOT IN ('a')"
        )
        self.assertClauseEqual(
            dml._gen_having_clause((), not_in_conds={"id": db.Select("*").From(db.tb1).Where("id > 1"), "OR name": "a"}),
            "HAVING id NOT IN (\n\t\t"
                "SELECT *\n\t\t"
                "FROM db.tb1 AS t0\n\t\t"
                "WHERE id > 1\n\t"
            ")\n\t"
            "OR name NOT IN ('a')",
        )
        self.assertClauseEqual(
            dml._gen_having_clause(("col1 > 1",), not_in_conds={"id": db.Select("*").From(db.tb1).Where("id > 1"), "OR name": "a"}),
            "HAVING col1 > 1\n\t"
            "AND id NOT IN (\n\t\t"
                "SELECT *\n\t\t"
                "FROM db.tb1 AS t0\n\t\t"
                "WHERE id > 1\n\t"
            ")\n\t"
            "OR name NOT IN ('a')",
        )
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_having_clause((), not_in_conds="id")
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_having_clause((), not_in_conds={1: (1,2,3)})
        # . indent
        self.assertClauseEqual(
            dml._gen_having_clause(("col1 > 1", "col2 < 2", "col3 = 3")),
            "\tHAVING col1 > 1\n\t\tAND col2 < 2\n\t\tAND col3 = 3", 1
        )
        self.assertClauseEqual(
            dml._gen_having_clause(("col1 > 1",), not_in_conds={"id": db.Select("*").From(db.tb1).Where("id > 1"), "OR name": "a"}),
            "\tHAVING col1 > 1\n\t"
            "\tAND id NOT IN (\n\t\t"
                "\tSELECT *\n\t\t"
                "\tFROM db.tb1 AS t0\n\t\t"
                "\tWHERE id > 1\n\t"
            "\t)\n\t"
            "\tOR name NOT IN ('a')", 1
        )
        self.assertClauseEqual(
            dml._gen_having_clause(("col1 > 1", "col2 < 2", "col3 = 3")),
            "\t\tHAVING col1 > 1\n\t\t\tAND col2 < 2\n\t\t\tAND col3 = 3", 2
        )
        self.assertClauseEqual(
            dml._gen_having_clause(("col1 > 1",), not_in_conds={"id": db.Select("*").From(db.tb1).Where("id > 1"), "OR name": "a"}),
            "\t\tHAVING col1 > 1\n\t"
            "\t\tAND id NOT IN (\n\t\t"
                "\t\tSELECT *\n\t\t"
                "\t\tFROM db.tb1 AS t0\n\t\t"
                "\t\tWHERE id > 1\n\t"
            "\t\t)\n\t"
            "\t\tOR name NOT IN ('a')", 2
        )
        # fmt: on

        self.log_ended("HAVING")

    def test_window_clause(self) -> None:
        self.log_start("WINDOW")

        db = TestDatabase("db", self.get_pool())
        dml = self.setupDML()

        # . window name
        self.assertClauseEqual(
            dml._gen_window_clause("w1", None, None, None),
            "WINDOW w1 AS ()",
        )
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_window_clause(2, None, None, None)
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_window_clause(None, None, None, None)
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_window_clause("", None, None, None)
        # . partition by
        self.assertClauseEqual(
            dml._gen_window_clause("w1", "col1", None, None),
            "WINDOW w1 AS (PARTITION BY col1)",
        )
        self.assertClauseEqual(
            dml._gen_window_clause("w1", ["col1", "col2"], None, None),
            "WINDOW w1 AS (PARTITION BY col1, col2)",
        )
        self.assertClauseEqual(
            dml._gen_window_clause("w1", db.tb1.id, None, None),
            "WINDOW w1 AS (PARTITION BY id)",
        )
        self.assertClauseEqual(
            dml._gen_window_clause("w1", [db.tb1.id, db.tb1.name], None, None),
            "WINDOW w1 AS (PARTITION BY id, name)",
        )
        self.assertClauseEqual(
            dml._gen_window_clause("w1", db.tb1.columns, None, None),
            "WINDOW w1 AS (PARTITION BY id, name, price, dt)",
        )
        self.assertClauseEqual(
            dml._gen_window_clause("w1", [db.tb1.id, "col2"], None, None),
            "WINDOW w1 AS (PARTITION BY id, col2)",
        )
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_window_clause("w1", [None], None, None)
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_window_clause("w1", 2, None, None)
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_window_clause("w1", [2], None, None)
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_window_clause("w1", "", None, None)
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_window_clause("w1", [""], None, None)
        # . order by
        self.assertClauseEqual(
            dml._gen_window_clause("w1", None, "col1", None),
            "WINDOW w1 AS (ORDER BY col1)",
        )
        self.assertClauseEqual(
            dml._gen_window_clause("w1", None, "col1 DESC", None),
            "WINDOW w1 AS (ORDER BY col1 DESC)",
        )
        self.assertClauseEqual(
            dml._gen_window_clause("w1", None, ("col1 ASC", "col2 DESC"), None),
            "WINDOW w1 AS (ORDER BY col1 ASC, col2 DESC)",
        )
        self.assertClauseEqual(
            dml._gen_window_clause("w1", None, db.tb1.id, None),
            "WINDOW w1 AS (ORDER BY id)",
        )
        self.assertClauseEqual(
            dml._gen_window_clause("w1", None, [db.tb1.id, db.tb1.name], None),
            "WINDOW w1 AS (ORDER BY id, name)",
        )
        self.assertClauseEqual(
            dml._gen_window_clause("w1", None, [db.tb1.id, "col2"], None),
            "WINDOW w1 AS (ORDER BY id, col2)",
        )
        self.assertClauseEqual(
            dml._gen_window_clause("w1", None, db.tb1.columns, None),
            "WINDOW w1 AS (ORDER BY id, name, price, dt)",
        )
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_window_clause("w1", None, [None], None)
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_window_clause("w1", None, 2, None)
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_window_clause("w1", None, [2], None)
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_window_clause("w1", None, "", None)
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_window_clause("w1", None, [""], None)
        # . frame clause
        self.assertClauseEqual(
            dml._gen_window_clause("w1", None, None, "ROWS UNBOUNDED PRECEDING"),
            "WINDOW w1 AS (ROWS UNBOUNDED PRECEDING)",
        )
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_window_clause("w1", None, None, 2)
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_window_clause("w1", None, None, "")
        # . mixed
        self.assertClauseEqual(
            dml._gen_window_clause(
                "w1",
                ["col1", "col2"],
                ["col1 ASC", "col2 DESC"],
                "ROWS UNBOUNDED PRECEDING",
            ),
            "WINDOW w1 AS (\n\tPARTITION BY col1, col2\n\tORDER BY col1 ASC, col2 DESC\n\tROWS UNBOUNDED PRECEDING\n)",
        )
        # . indent
        # fmt: off
        self.assertClauseEqual(
            dml._gen_window_clause("w1", None, None, None), 
            "\tWINDOW w1 AS ()", 1
        )
        self.assertClauseEqual(
            dml._gen_window_clause("w1", "col1", None, None),
            "\tWINDOW w1 AS (PARTITION BY col1)", 1
        )
        self.assertClauseEqual(
            dml._gen_window_clause("w1", None, "col1", None),
            "\tWINDOW w1 AS (ORDER BY col1)", 1
        )
        self.assertClauseEqual(
            dml._gen_window_clause("w1", None, None, "ROWS UNBOUNDED PRECEDING"),
            "\tWINDOW w1 AS (ROWS UNBOUNDED PRECEDING)", 1
        )
        self.assertClauseEqual(
            dml._gen_window_clause(
                "w1",
                ["col1", "col2"],
                ["col1 ASC", "col2 DESC"],
                "ROWS UNBOUNDED PRECEDING",
            ),
            "\tWINDOW w1 AS (\n\t\t"
            "PARTITION BY col1, col2\n\t\t"
            "ORDER BY col1 ASC, col2 DESC\n\t\t"
            "ROWS UNBOUNDED PRECEDING\n\t)", 1
        )
        self.assertClauseEqual(
            dml._gen_window_clause("w1", None, None, None), 
            "\t\tWINDOW w1 AS ()", 2
        )
        self.assertClauseEqual(
            dml._gen_window_clause("w1", "col1", None, None),
            "\t\tWINDOW w1 AS (PARTITION BY col1)", 2
        )
        self.assertClauseEqual(
            dml._gen_window_clause("w1", None, "col1", None),
            "\t\tWINDOW w1 AS (ORDER BY col1)", 2
        )
        self.assertClauseEqual(
            dml._gen_window_clause("w1", None, None, "ROWS UNBOUNDED PRECEDING"),
            "\t\tWINDOW w1 AS (ROWS UNBOUNDED PRECEDING)", 2
        )
        self.assertClauseEqual(
            dml._gen_window_clause(
                "w1",
                ["col1", "col2"],
                ["col1 ASC", "col2 DESC"],
                "ROWS UNBOUNDED PRECEDING",
            ),
            "\t\tWINDOW w1 AS (\n\t\t\t"
            "PARTITION BY col1, col2\n\t\t\t"
            "ORDER BY col1 ASC, col2 DESC\n\t\t\t"
            "ROWS UNBOUNDED PRECEDING\n\t\t)", 2
        )
        self.assertClauseEqual(
            dml._gen_window_clause("w1", None, None, None), 
            "WINDOW w1 AS ()", 0
        )
        self.assertClauseEqual(
            dml._gen_window_clause("w1", "col1", None, None),
            "WINDOW w1 AS (PARTITION BY col1)", 0
        )
        self.assertClauseEqual(
            dml._gen_window_clause("w1", None, "col1", None),
            "WINDOW w1 AS (ORDER BY col1)", 0
        )
        self.assertClauseEqual(
            dml._gen_window_clause("w1", None, None, "ROWS UNBOUNDED PRECEDING"),
            "WINDOW w1 AS (ROWS UNBOUNDED PRECEDING)", 0
        )
        self.assertClauseEqual(
            dml._gen_window_clause(
                "w1",
                ["col1", "col2"],
                ["col1 ASC", "col2 DESC"],
                "ROWS UNBOUNDED PRECEDING",
            ),
            "WINDOW w1 AS (\n\t"
            "PARTITION BY col1, col2\n\t"
            "ORDER BY col1 ASC, col2 DESC\n\t"
            "ROWS UNBOUNDED PRECEDING\n)", 0
        )
        # fmt: on

        self.log_ended("WINDOW")

    def test_order_by_clause(self) -> None:
        self.log_start("ORDER BY")

        db = TestDatabase("db", self.get_pool())
        dml = self.setupDML()

        # fmt: off
        # . str
        self.assertClauseEqual(dml._gen_order_by_clause(("col1",)), "ORDER BY col1")
        self.assertClauseEqual(dml._gen_order_by_clause(("col1", "col2")), "ORDER BY col1, col2")
        # . column
        self.assertClauseEqual(dml._gen_order_by_clause((db.tb1.id,)), "ORDER BY id")
        self.assertClauseEqual(dml._gen_order_by_clause((db.tb1.id, db.tb1.name,)), "ORDER BY id, name")
        # . columns
        self.assertClauseEqual(dml._gen_order_by_clause((db.tb1.columns,)), "ORDER BY id, name, price, dt")
        # . sqlfunc
        self.assertClauseEqual(dml._gen_order_by_clause((sqlfunc.YEAR("col1"),)), "ORDER BY YEAR(col1)")
        # . with rollup
        self.assertClauseEqual(
            dml._gen_order_by_clause((db.tb1.id, db.tb1.name), with_rollup=True),
            "ORDER BY id, name WITH ROLLUP"
        )
        # . error
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_order_by_clause((2,))
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_order_by_clause((None,))
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_order_by_clause(("",))
        # . indent
        self.assertClauseEqual(dml._gen_order_by_clause(("col1",)), "\tORDER BY col1", 1)
        self.assertClauseEqual(dml._gen_order_by_clause(("col1",)), "\t\tORDER BY col1", 2)
        self.assertClauseEqual(dml._gen_order_by_clause(("col1",)), "ORDER BY col1", 0)

        self.log_ended("ORDER BY")

    def test_limit_clause(self) -> None:
        self.log_start("LIMIT")

        dml = self.setupDML()
        # . limit
        self.assertClauseEqual(dml._gen_limit_clause(10, 0), "LIMIT 10")
        self.assertClauseEqual(dml._gen_limit_clause(10, None), "LIMIT 10")
        self.assertClauseEqual(dml._gen_limit_clause("10", "0"), "LIMIT 10")
        self.assertClauseEqual(dml._gen_limit_clause(10, 20), "LIMIT 20, 10")
        self.assertClauseEqual(dml._gen_limit_clause(10, "20"), "LIMIT 20, 10")
        # . error
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_limit_clause(-1, None)
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_limit_clause(None, None)
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_limit_clause("", None)
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_limit_clause(1, -1)
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_limit_clause(1, "")
        # . indent
        self.assertClauseEqual(dml._gen_limit_clause(10, 1), "\tLIMIT 1, 10", 1)
        self.assertClauseEqual(dml._gen_limit_clause(10, 1), "\t\tLIMIT 1, 10", 2)
        self.assertClauseEqual(dml._gen_limit_clause(10, 1), "LIMIT 1, 10", 0)

        self.log_ended("LIMIT")

    def test_locking_read_clause(self) -> None:
        self.log_start("LOCKING READ")

        db = TestDatabase("db", self.get_pool())
        dml = self.setupDML()

        # fmt: off
        # . mode
        self.assertClauseEqual(dml._gen_for_update_clause(()), "FOR UPDATE")
        self.assertClauseEqual(dml._gen_for_share_clause(()), "FOR SHARE")
        # . tables
        self.assertClauseEqual(dml._gen_for_update_clause(("t1",)), "FOR UPDATE OF t1")
        self.assertClauseEqual(dml._gen_for_update_clause(("t1", "t2")), "FOR UPDATE OF t1, t2")
        self.assertClauseEqual(dml._gen_for_update_clause((db.tb1,)), "FOR UPDATE OF db.tb1")
        self.assertClauseEqual(dml._gen_for_update_clause((db.tb1, db.tb2)), "FOR UPDATE OF db.tb1, db.tb2")
        self.assertClauseEqual(dml._gen_for_update_clause((db.tables,)), "FOR UPDATE OF db.tb1, db.tb2, db.tb3")
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_for_update_clause((2,))
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_for_update_clause((None,))
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_for_update_clause(("",))
        # . options
        self.assertClauseEqual(dml._gen_for_update_clause((), "NOWAIT"), "FOR UPDATE NOWAIT")
        self.assertClauseEqual(dml._gen_for_update_clause((), "SKIP LOCKED"), "FOR UPDATE SKIP LOCKED")
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_for_update_clause((), 1)
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_for_update_clause((), "x")
        # . mixed
        self.assertClauseEqual(
            dml._gen_for_update_clause(("t1", "t2"), "NOWAIT"),
            "FOR UPDATE OF t1, t2 NOWAIT",
        )
        # . indent
        self.assertClauseEqual(dml._gen_for_update_clause(()), "\tFOR UPDATE", 1)
        self.assertClauseEqual(dml._gen_for_update_clause(()), "\t\tFOR UPDATE", 2)
        self.assertClauseEqual(dml._gen_for_update_clause(()), "FOR UPDATE", 0)
        # fmt: on

        self.log_ended("LOCKING READ")

    def test_into_clause(self) -> None:
        self.log_start("INTO")

        dml = self.setupDML()
        # . into (variables)
        self.assertClauseEqual(
            dml._gen_into_variables_clause(("@x", "@y")), "INTO @x, @y"
        )
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_into_variables_clause((None,))
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_into_variables_clause((2,))
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_into_variables_clause(("",))
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_into_variables_clause(())
        self.assertClauseEqual(dml._gen_into_variables_clause(("@x",)), "\tINTO @x", 1)
        self.assertClauseEqual(
            dml._gen_into_variables_clause(("@x",)), "\t\tINTO @x", 2
        )
        self.assertClauseEqual(dml._gen_into_variables_clause(("@x",)), "INTO @x", 0)

        self.log_ended("INTO")


class TestSelectStatement(TestCase):
    name: str = "SELECT Statement"
    TEST_DATA: tuple = (
        (1, "a0", 1.0, datetime.datetime(2012, 1, 1, 0, 0)),
        (2, "a1", 1.1, datetime.datetime(2012, 1, 1, 0, 0)),
        (3, "b0", 2.0, datetime.datetime(2012, 2, 1, 0, 0)),
        (4, "b1", 2.1, datetime.datetime(2012, 2, 1, 0, 0)),
        (5, "c1", 3.0, datetime.datetime(2012, 3, 1, 0, 0)),
        (6, "c2", 3.1, datetime.datetime(2012, 3, 1, 0, 0)),
    )

    def prepareDatabase(self) -> TestDatabase:
        db = super().prepareDatabase()
        with db.acquire() as conn:
            with conn.transaction() as cur:
                cur.executemany(
                    f"INSERT INTO {db.tb1} (id, name, price, dt) VALUES (%s, %s, %s, %s)",
                    self.TEST_DATA,
                )
                cur.executemany(
                    f"INSERT INTO {db.tb2} (id, name, price, dt) VALUES (%s, %s, %s, %s)",
                    self.TEST_DATA,
                )
        return db

    # Test
    async def test_all(self) -> None:
        await self.test_select_clause()
        await self.test_from_clause()
        await self.test_join_clause()
        await self.test_where_clause()
        await self.test_group_by_clause()
        await self.test_having_clause()
        await self.test_window_clause()
        await self.test_order_by_clause()
        await self.test_limit_cluase()
        await self.test_locking_reads_cluase()
        await self.test_into_cluase()
        await self.test_set_operation_clause()
        await self.test_from_subquery()
        await self.test_join_subquery()
        await self.test_table_select()

    async def test_select_clause(self) -> None:
        self.log_start("SELECT")

        # Select
        db = self.prepareDatabase()
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = db.Select("1")
        self.assertEqual(dml.statement(), "SELECT 1")
        self.assertEqual(dml.Execute(), ((1,),))
        self.assertEqual(await dml.aioExecute(), ((1,),))
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = db.Select("1", "2", "3")
        self.assertEqual(dml.statement(), "SELECT\n\t1,\n\t2,\n\t3")
        self.assertEqual(dml.Execute(), ((1, 2, 3),))
        self.assertEqual(await dml.aioExecute(), ((1, 2, 3),))
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = db.Select(
            "1",
            distinct=True,
            high_priority=True,
            straight_join=True,
            sql_buffer_result=True,
        )
        self.assertEqual(
            dml.statement(),
            "SELECT DISTINCT HIGH_PRIORITY STRAIGHT_JOIN SQL_BUFFER_RESULT 1",
        )
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = db.Select(
            "c1",
            "c2",
            distinct=True,
            high_priority=True,
            straight_join=True,
            sql_buffer_result=True,
        )
        self.assertEqual(
            dml.statement(),
            "SELECT DISTINCT HIGH_PRIORITY STRAIGHT_JOIN SQL_BUFFER_RESULT\n\tc1,\n\tc2",
        )
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        with self.assertRaises(errors.DMLClauseError):
            db.Select("1").Select("1").statement()

        # Finished
        db.Drop(True)
        self.log_ended("SELECT")

    async def test_from_clause(self) -> None:
        self.log_start("FROM")

        # From
        db = self.prepareDatabase()
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = db.Select("*").From(db.tb1)
        self.assertEqual(dml.statement(), "SELECT *\nFROM db.tb1 AS t0")
        self.assertEqual(dml.Execute(), self.TEST_DATA)
        self.assertEqual(await dml.aioExecute(), self.TEST_DATA)
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = db.Select("*").From(db.tb1, "from201201")
        self.assertEqual(
            dml.statement(), "SELECT *\nFROM db.tb1 PARTITION (from201201) AS t0"
        )
        res = (
            (1, "a0", 1.0, datetime.datetime(2012, 1, 1, 0, 0)),
            (2, "a1", 1.1, datetime.datetime(2012, 1, 1, 0, 0)),
        )
        self.assertEqual(dml.Execute(), res)
        self.assertEqual(await dml.aioExecute(), res)
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = db.Select("*").From(
            db.tb1, [db.tb1.partitioning["start"], "from201201"], alias="tb1"
        )
        res = (
            (1, "a0", 1.0, datetime.datetime(2012, 1, 1, 0, 0)),
            (2, "a1", 1.1, datetime.datetime(2012, 1, 1, 0, 0)),
        )
        self.assertEqual(dml.Execute(), res)
        self.assertEqual(await dml.aioExecute(), res)
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = db.Select("*").From(db.tb1, ["from201201", "from201202"])
        self.assertEqual(
            dml.statement(),
            "SELECT *\nFROM db.tb1 PARTITION (from201201, from201202) AS t0",
        )
        res = (
            (1, "a0", 1.0, datetime.datetime(2012, 1, 1, 0, 0)),
            (2, "a1", 1.1, datetime.datetime(2012, 1, 1, 0, 0)),
            (3, "b0", 2.0, datetime.datetime(2012, 2, 1, 0, 0)),
            (4, "b1", 2.1, datetime.datetime(2012, 2, 1, 0, 0)),
        )
        self.assertEqual(dml.Execute(), res)
        self.assertEqual(await dml.aioExecute(), res)
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        with self.assertRaises(errors.DMLClauseError):
            db.Select("*").From(db.tb1).From("db.tb1").statement()

        # . with index hint
        dml = db.Select("*").From(db.tb1).UseIndex("idx1")
        self.assertEqual(
            dml.statement(), "SELECT *\nFROM db.tb1 AS t0\n\tUSE INDEX (idx1)"
        )
        self.assertEqual(dml.Execute(), self.TEST_DATA)
        self.assertEqual(await dml.aioExecute(), self.TEST_DATA)
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = db.Select("*").From(db.tb1).UseIndex(db.tb1.pk, "idx1")
        self.assertEqual(
            dml.statement(),
            "SELECT *\nFROM db.tb1 AS t0\n\tUSE INDEX (PRIMARY, idx1)",
        )
        self.assertEqual(dml.Execute(), self.TEST_DATA)
        self.assertEqual(await dml.aioExecute(), self.TEST_DATA)
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = db.Select("*").From(db.tb1).UseIndex("idx1", scope="JOIN")
        self.assertEqual(
            dml.statement(),
            "SELECT *\nFROM db.tb1 AS t0\n\tUSE INDEX FOR JOIN (idx1)",
        )
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = (
            db.Select("*")
            .From(db.tb1)
            .UseIndex("idx1", scope="JOIN")
            .ForceIndex("idx2", scope="GROUP BY")
            .IgnoreIndex(db.tb1.pk, scope="ORDER BY")
        )
        self.assertEqual(
            dml.statement(),
            "SELECT *\nFROM db.tb1 AS t0\n\t"
            "USE INDEX FOR JOIN (idx1)\n\t"
            "FORCE INDEX FOR GROUP BY (idx2)\n\t"
            "IGNORE INDEX FOR ORDER BY (PRIMARY)",
        )
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        with self.assertRaises(errors.DMLClauseError):
            db.Select("*").UseIndex("idx1").statement()
        with self.assertRaises(errors.DMLClauseError):
            db.Select("*").ForceIndex("idx1").statement()
        with self.assertRaises(errors.DMLClauseError):
            db.Select("*").IgnoreIndex("idx1").statement()

        # Finished
        db.Drop(True)
        self.log_ended("FROM")

    async def test_join_clause(self) -> None:
        self.log_start("JOIN")

        # Join
        db = self.prepareDatabase()
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = db.Select("t0.*").From(db.tb1).Join(db.tb2, using="id")
        self.assertEqual(
            dml.statement(),
            "SELECT t0.*\n"
            "FROM db.tb1 AS t0\n"
            "INNER JOIN db.tb2 AS t1\n\tUSING (id)",
        )
        self.assertEqual(dml.Execute(), self.TEST_DATA)
        self.assertEqual(await dml.aioExecute(), self.TEST_DATA)
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = db.Select("t0.*").From(db.tb1).LeftJoin(db.tb2, using="id")
        self.assertEqual(
            dml.statement(),
            "SELECT t0.*\n"
            "FROM db.tb1 AS t0\n"
            "LEFT JOIN db.tb2 AS t1\n\tUSING (id)",
        )
        self.assertEqual(dml.Execute(), self.TEST_DATA)
        self.assertEqual(await dml.aioExecute(), self.TEST_DATA)
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = db.Select("t0.*").From(db.tb1).RightJoin(db.tb2, using="id")
        self.assertEqual(
            dml.statement(),
            "SELECT t0.*\n"
            "FROM db.tb1 AS t0\n"
            "RIGHT JOIN db.tb2 AS t1\n\tUSING (id)",
        )
        self.assertEqual(dml.Execute(), self.TEST_DATA)
        self.assertEqual(await dml.aioExecute(), self.TEST_DATA)
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = db.Select("t0.*").From(db.tb1).StraightJoin(db.tb2, using="id")
        self.assertEqual(
            dml.statement(),
            "SELECT t0.*\n"
            "FROM db.tb1 AS t0\n"
            "STRAIGHT_JOIN db.tb2 AS t1\n\tUSING (id)",
        )
        self.assertEqual(dml.Execute(), self.TEST_DATA)
        self.assertEqual(await dml.aioExecute(), self.TEST_DATA)
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = db.Select("t0.*").From(db.tb1).CrossJoin(db.tb2, using="id")
        self.assertEqual(
            dml.statement(),
            "SELECT t0.*\n"
            "FROM db.tb1 AS t0\n"
            "CROSS JOIN db.tb2 AS t1\n\tUSING (id)",
        )
        self.assertEqual(dml.Execute(), self.TEST_DATA)
        self.assertEqual(await dml.aioExecute(), self.TEST_DATA)
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = db.Select("t0.*").From(db.tb1).NaturalJoin(db.tb2, "INNER")
        self.assertEqual(
            dml.statement(),
            "SELECT t0.*\n" "FROM db.tb1 AS t0\n" "NATURAL INNER JOIN db.tb2 AS t1",
        )
        self.assertEqual(dml.Execute(), self.TEST_DATA)
        self.assertEqual(await dml.aioExecute(), self.TEST_DATA)
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = db.Select("t0.*").From(db.tb1).NaturalJoin(db.tb2, "LEFT")
        self.assertEqual(
            dml.statement(),
            "SELECT t0.*\n" "FROM db.tb1 AS t0\n" "NATURAL LEFT JOIN db.tb2 AS t1",
        )
        self.assertEqual(dml.Execute(), self.TEST_DATA)
        self.assertEqual(await dml.aioExecute(), self.TEST_DATA)
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = db.Select("t0.*").From(db.tb1).NaturalJoin(db.tb2, "RIGHT")
        self.assertEqual(
            dml.statement(),
            "SELECT t0.*\n" "FROM db.tb1 AS t0\n" "NATURAL RIGHT JOIN db.tb2 AS t1",
        )
        self.assertEqual(dml.Execute(), self.TEST_DATA)
        self.assertEqual(await dml.aioExecute(), self.TEST_DATA)
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = db.Select("t0.*").From(db.tb1).Join(db.tb2, "t0.id = t1.id")
        self.assertEqual(
            dml.statement(),
            "SELECT t0.*\nFROM db.tb1 AS t0\n"
            "INNER JOIN db.tb2 AS t1\n\tON t0.id = t1.id",
        )
        self.assertEqual(dml.Execute(), self.TEST_DATA)
        self.assertEqual(await dml.aioExecute(), self.TEST_DATA)
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = (
            db.Select("t0.*")
            .From(db.tb1)
            .Join(db.tb2, "t0.id = t1.id", "t0.name = t1.name")
        )
        self.assertEqual(
            dml.statement(),
            "SELECT t0.*\nFROM db.tb1 AS t0\n"
            "INNER JOIN db.tb2 AS t1\n\tON t0.id = t1.id\n\tAND t0.name = t1.name",
        )
        self.assertEqual(dml.Execute(), self.TEST_DATA)
        self.assertEqual(await dml.aioExecute(), self.TEST_DATA)
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = (
            db.Select("t0.*")
            .From(db.tb1)
            .Join(db.tb2, "t0.id = t1.id", "t0.name = t1.name", partition="from201201")
        )
        self.assertEqual(
            dml.statement(),
            "SELECT t0.*\nFROM db.tb1 AS t0\n"
            "INNER JOIN db.tb2 PARTITION (from201201) AS t1\n\tON t0.id = t1.id\n\tAND t0.name = t1.name",
        )
        res = (
            (1, "a0", 1.0, datetime.datetime(2012, 1, 1, 0, 0)),
            (2, "a1", 1.1, datetime.datetime(2012, 1, 1, 0, 0)),
        )
        self.assertEqual(dml.Execute(), res)
        self.assertEqual(await dml.aioExecute(), res)
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = (
            db.Select("t0.*")
            .From(db.tb1)
            .Join(
                db.tb2,
                "t0.id = t1.id",
                "t0.name = t1.name",
                partition=["start", "from201201"],
            )
        )
        self.assertEqual(
            dml.statement(),
            "SELECT t0.*\nFROM db.tb1 AS t0\n"
            "INNER JOIN db.tb2 PARTITION (start, from201201) AS t1\n\tON t0.id = t1.id\n\tAND t0.name = t1.name",
        )
        res = (
            (1, "a0", 1.0, datetime.datetime(2012, 1, 1, 0, 0)),
            (2, "a1", 1.1, datetime.datetime(2012, 1, 1, 0, 0)),
        )
        self.assertEqual(dml.Execute(), res)
        self.assertEqual(await dml.aioExecute(), res)
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = db.Select("t0.*").From(db.tb1).Join(db.tb2, using="id")
        self.assertEqual(
            dml.statement(),
            "SELECT t0.*\nFROM db.tb1 AS t0\n" "INNER JOIN db.tb2 AS t1\n\tUSING (id)",
        )
        self.assertEqual(dml.Execute(), self.TEST_DATA)
        self.assertEqual(await dml.aioExecute(), self.TEST_DATA)
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = db.Select("t0.*").From(db.tb1).Join(db.tb2, using=["id", "name"])
        self.assertEqual(
            dml.statement(),
            "SELECT t0.*\nFROM db.tb1 AS t0\n"
            "INNER JOIN db.tb2 AS t1\n\tUSING (id, name)",
        )
        self.assertEqual(dml.Execute(), self.TEST_DATA)
        self.assertEqual(await dml.aioExecute(), self.TEST_DATA)
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = (
            db.Select("t0.*")
            .From(db.tb1)
            .Join(db.tb2, using=["id", "name"], partition=["from201201"])
        )
        self.assertEqual(
            dml.statement(),
            "SELECT t0.*\nFROM db.tb1 AS t0\n"
            "INNER JOIN db.tb2 PARTITION (from201201) AS t1\n\tUSING (id, name)",
        )
        res = (
            (1, "a0", 1.0, datetime.datetime(2012, 1, 1, 0, 0)),
            (2, "a1", 1.1, datetime.datetime(2012, 1, 1, 0, 0)),
        )
        self.assertEqual(dml.Execute(), res)
        self.assertEqual(await dml.aioExecute(), res)
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = (
            db.Select("t0.*")
            .From(db.tb1)
            .Join(db.tb2, using=["id", "name"], partition=["start", "from201201"])
        )
        self.assertEqual(
            dml.statement(),
            "SELECT t0.*\nFROM db.tb1 AS t0\n"
            "INNER JOIN db.tb2 PARTITION (start, from201201) AS t1\n\tUSING (id, name)",
        )
        res = (
            (1, "a0", 1.0, datetime.datetime(2012, 1, 1, 0, 0)),
            (2, "a1", 1.1, datetime.datetime(2012, 1, 1, 0, 0)),
        )
        self.assertEqual(dml.Execute(), res)
        self.assertEqual(await dml.aioExecute(), res)
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = db.Select("*").From(db.tb1).Join(db.tb2, using="id", alias="tb1")
        self.assertEqual(
            dml.statement(),
            "SELECT *\nFROM db.tb1 AS t0\n" "INNER JOIN db.tb2 AS tb1\n\tUSING (id)",
        )
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = (
            db.Select("*")
            .From(db.tb1)
            .Join(db.tb2, "t0.id = t1.id")
            .LeftJoin(db.tb2, "t0.id = t2.id", "t0.name = t2.name")
            .RightJoin(db.tb2, using="id")
            .StraightJoin(db.tb2, using=["id", "name"])
            .CrossJoin(db.tb2, "t0.id = t5.id")
            .NaturalJoin(db.tb2, "INNER")
            .NaturalJoin(db.tb2, "LEFT")
            .NaturalJoin(db.tb2, "RIGHT")
        )
        self.assertEqual(
            dml.statement(),
            "SELECT *\nFROM db.tb1 AS t0\n"
            "INNER JOIN db.tb2 AS t1\n\tON t0.id = t1.id\n"
            "LEFT JOIN db.tb2 AS t2\n\tON t0.id = t2.id\n\tAND t0.name = t2.name\n"
            "RIGHT JOIN db.tb2 AS t3\n\tUSING (id)\n"
            "STRAIGHT_JOIN db.tb2 AS t4\n\tUSING (id, name)\n"
            "CROSS JOIN db.tb2 AS t5\n\tON t0.id = t5.id\n"
            "NATURAL INNER JOIN db.tb2 AS t6\n"
            "NATURAL LEFT JOIN db.tb2 AS t7\n"
            "NATURAL RIGHT JOIN db.tb2 AS t8",
        )

        # . with index hint
        dml = (
            db.Select("t0.*")
            .From(db.tb1)
            .Join(db.tb2, "t0.id = t1.id")
            .UseIndex(db.tb2.idx1, scope="JOIN")
        )
        self.assertEqual(
            dml.statement(),
            "SELECT t0.*\nFROM db.tb1 AS t0\n"
            "INNER JOIN db.tb2 AS t1\n\tUSE INDEX FOR JOIN (idx1)\n\tON t0.id = t1.id",
        )
        self.assertEqual(dml.Execute(), self.TEST_DATA)
        self.assertEqual(await dml.aioExecute(), self.TEST_DATA)
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = (
            db.Select("t0.*")
            .From(db.tb1)
            .Join(db.tb2, "t0.id = t1.id")
            .UseIndex(db.tb2.idx1, scope="JOIN")
            .IgnoreIndex(db.tb2.idx2, scope="JOIN")
        )
        self.assertEqual(
            dml.statement(),
            "SELECT t0.*\nFROM db.tb1 AS t0\n"
            "INNER JOIN db.tb2 AS t1\n\tUSE INDEX FOR JOIN (idx1)\n\tIGNORE INDEX FOR JOIN (idx2)\n\tON t0.id = t1.id",
        )
        self.assertEqual(dml.Execute(), self.TEST_DATA)
        self.assertEqual(await dml.aioExecute(), self.TEST_DATA)
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = (
            db.Select("*")
            .From(db.tb1)
            .Join(db.tb2, "t0.id = t1.id")
            .UseIndex(db.tb2.idx1, scope="JOIN")
            .LeftJoin(db.tb2, using="id")
            .UseIndex(db.tb2.idx2, scope="JOIN")
        )
        self.assertEqual(
            dml.statement(),
            "SELECT *\nFROM db.tb1 AS t0\n"
            "INNER JOIN db.tb2 AS t1\n\tUSE INDEX FOR JOIN (idx1)\n\tON t0.id = t1.id\n"
            "LEFT JOIN db.tb2 AS t2\n\tUSE INDEX FOR JOIN (idx2)\n\tUSING (id)",
        )
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = (
            db.Select("*")
            .From(db.tb1)
            .Join(db.tb2, "t0.id = t1.id")
            .UseIndex(db.tb2.idx1, scope="JOIN")
            .IgnoreIndex(db.tb2.idx2, scope="JOIN")
            .LeftJoin(db.tb2, using="id")
            .UseIndex(db.tb2.idx2, scope="JOIN")
            .IgnoreIndex(db.tb2.idx1, scope="JOIN")
        )
        self.assertEqual(
            dml.statement(),
            "SELECT *\nFROM db.tb1 AS t0\n"
            "INNER JOIN db.tb2 AS t1\n\tUSE INDEX FOR JOIN (idx1)\n\tIGNORE INDEX FOR JOIN (idx2)\n\tON t0.id = t1.id\n"
            "LEFT JOIN db.tb2 AS t2\n\tUSE INDEX FOR JOIN (idx2)\n\tIGNORE INDEX FOR JOIN (idx1)\n\tUSING (id)",
        )

        with self.assertRaises(errors.DMLClauseError):
            db.Select("*").From(db.tb1).Where(
                "id = 1", "name = 'test'", "price = 1.1"
            ).Join(db.tb2, "t0.id = t1.id").statement()

        # Finished
        db.Drop(True)
        self.log_ended("JOIN")

    async def test_where_clause(self) -> None:
        self.log_start("WHERE")

        # Where
        db = self.prepareDatabase()
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = db.Select("*").From(db.tb1).Where("t0.id > 0")
        self.assertEqual(
            dml.statement(), "SELECT *\nFROM db.tb1 AS t0\nWHERE t0.id > 0"
        )
        self.assertEqual(dml.Execute(), self.TEST_DATA)
        self.assertEqual(await dml.aioExecute(), self.TEST_DATA)
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = db.Select("*").From(db.tb1).Where("t0.id > %s")
        self.assertEqual(
            dml.statement(), "SELECT *\nFROM db.tb1 AS t0\nWHERE t0.id > %s"
        )
        self.assertEqual(dml.Execute(0), self.TEST_DATA)
        self.assertEqual(await dml.aioExecute(0), self.TEST_DATA)
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = (
            db.Select("*")
            .From(db.tb1)
            .Where("t0.id > 0", in_conds={"t0.name": ["a0", "a1"]})
        )
        self.assertEqual(
            dml.statement(),
            "SELECT *\nFROM db.tb1 AS t0\n"
            "WHERE t0.id > 0\n\tAND t0.name IN ('a0','a1')",
        )
        res = (
            (1, "a0", 1.0, datetime.datetime(2012, 1, 1, 0, 0)),
            (2, "a1", 1.1, datetime.datetime(2012, 1, 1, 0, 0)),
        )
        self.assertEqual(dml.Execute(), res)
        self.assertEqual(await dml.aioExecute(), res)
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = (
            db.Select("*")
            .From(db.tb1)
            .Where(
                in_conds={
                    "t0.name": ["a0", "a1"],
                    "t0.id": db.Select("id").From(db.tb2),
                }
            )
        )
        self.assertEqual(
            dml.statement(),
            "SELECT *\n"
            "FROM db.tb1 AS t0\n"
            "WHERE t0.name IN ('a0','a1')\n\t"
            "AND t0.id IN (\n\t\t"
            "SELECT id\n\t\t"
            "FROM db.tb2 AS t0\n\t"
            ")",
        )
        res = (
            (1, "a0", 1.0, datetime.datetime(2012, 1, 1, 0, 0)),
            (2, "a1", 1.1, datetime.datetime(2012, 1, 1, 0, 0)),
        )
        self.assertEqual(dml.Execute(), res)
        self.assertEqual(await dml.aioExecute(), res)
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        with self.assertRaises(errors.DMLClauseError):
            (
                db.Select("*")
                .From(db.tb1)
                .Where("t0.id > 0")
                .Where("t0.id > 0")
                .statement()
            )

        # Finished
        db.Drop(True)
        self.log_ended("WHERE")

    async def test_group_by_clause(self) -> None:
        self.log_start("GROUP BY")

        # Group By
        db = self.prepareDatabase()
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = db.Select("*").From(db.tb1).Where("id > 0").GroupBy("id")
        self.assertEqual(
            dml.statement(),
            "SELECT *\nFROM db.tb1 AS t0\nWHERE id > 0\nGROUP BY id",
        )
        self.assertEqual(dml.Execute(), self.TEST_DATA)
        self.assertEqual(await dml.aioExecute(), self.TEST_DATA)
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = db.Select("*").From(db.tb1).Where("id > 0").GroupBy("id", "name")
        self.assertEqual(
            dml.statement(),
            "SELECT *\nFROM db.tb1 AS t0\nWHERE id > 0\nGROUP BY id, name",
        )
        self.assertEqual(dml.Execute(), self.TEST_DATA)
        self.assertEqual(await dml.aioExecute(), self.TEST_DATA)
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = (
            db.Select("*")
            .From(db.tb1)
            .Where("id > 0")
            .GroupBy("id", "name", with_rollup=True)
        )
        self.assertEqual(
            dml.statement(),
            "SELECT *\nFROM db.tb1 AS t0\nWHERE id > 0\nGROUP BY id, name WITH ROLLUP",
        )
        res = (
            (1, "a0", 1.0, datetime.datetime(2012, 1, 1, 0, 0)),
            (1, None, 1.0, datetime.datetime(2012, 1, 1, 0, 0)),
            (2, "a1", 1.1, datetime.datetime(2012, 1, 1, 0, 0)),
            (2, None, 1.1, datetime.datetime(2012, 1, 1, 0, 0)),
            (3, "b0", 2.0, datetime.datetime(2012, 2, 1, 0, 0)),
            (3, None, 2.0, datetime.datetime(2012, 2, 1, 0, 0)),
            (4, "b1", 2.1, datetime.datetime(2012, 2, 1, 0, 0)),
            (4, None, 2.1, datetime.datetime(2012, 2, 1, 0, 0)),
            (5, "c1", 3.0, datetime.datetime(2012, 3, 1, 0, 0)),
            (5, None, 3.0, datetime.datetime(2012, 3, 1, 0, 0)),
            (6, "c2", 3.1, datetime.datetime(2012, 3, 1, 0, 0)),
            (6, None, 3.1, datetime.datetime(2012, 3, 1, 0, 0)),
            (None, None, 3.1, datetime.datetime(2012, 3, 1, 0, 0)),
        )
        self.assertEqual(dml.Execute(), res)
        self.assertEqual(await dml.aioExecute(), res)
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        with self.assertRaises(errors.DMLClauseError):
            (
                db.Select("*")
                .From(db.tb1)
                .Where("id > 10")
                .GroupBy("id")
                .GroupBy("id")
                .statement()
            )

        # Finished
        db.Drop(True)
        self.log_ended("GROUP BY")

    async def test_having_clause(self) -> None:
        self.log_start("HAVING")

        # Having
        db = self.prepareDatabase()
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = db.Select("*").From(db.tb1).Where("id > 0").GroupBy("id").Having("id = 1")
        self.assertEqual(
            dml.statement(),
            "SELECT *\nFROM db.tb1 AS t0\n"
            "WHERE id > 0\n"
            "GROUP BY id\n"
            "HAVING id = 1",
        )
        res = ((1, "a0", 1.0, datetime.datetime(2012, 1, 1, 0, 0)),)
        self.assertEqual(dml.Execute(), res)
        self.assertEqual(await dml.aioExecute(), res)
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = (
            db.Select("*")
            .From(db.tb1)
            .Where("id > 0")
            .GroupBy("id")
            .Having("id = 1", "name = 'a0'")
        )
        self.assertEqual(
            dml.statement(),
            "SELECT *\nFROM db.tb1 AS t0\n"
            "WHERE id > 0\n"
            "GROUP BY id\n"
            "HAVING id = 1\n\tAND name = 'a0'",
        )
        res = ((1, "a0", 1.0, datetime.datetime(2012, 1, 1, 0, 0)),)
        self.assertEqual(dml.Execute(), res)
        self.assertEqual(await dml.aioExecute(), res)
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = (
            db.Select("*")
            .From(db.tb1)
            .Where("id > %s")
            .GroupBy("id")
            .Having("id = %s", "name = %s")
        )
        self.assertEqual(
            dml.statement(),
            "SELECT *\nFROM db.tb1 AS t0\n"
            "WHERE id > %s\n"
            "GROUP BY id\n"
            "HAVING id = %s\n\tAND name = %s",
        )
        res = ((1, "a0", 1.0, datetime.datetime(2012, 1, 1, 0, 0)),)
        self.assertEqual(dml.Execute([0, 1, "a0"]), res)
        self.assertEqual(await dml.aioExecute([0, 1, "a0"]), res)
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = (
            db.Select("t0.*")
            .From(db.tb1)
            .Join(db.tb2, "t0.name = t1.name", "t0.id = %s")
            .Where("t0.id > %s")
            .GroupBy("t0.id")
            .Having("t0.id = %s", "t0.name = %s")
        )
        self.assertEqual(
            dml.statement(),
            "SELECT t0.*\nFROM db.tb1 AS t0\n"
            "INNER JOIN db.tb2 AS t1\n\tON t0.name = t1.name\n\tAND t0.id = %s\n"
            "WHERE t0.id > %s\n"
            "GROUP BY t0.id\n"
            "HAVING t0.id = %s\n\tAND t0.name = %s",
        )
        res = ((1, "a0", 1.0, datetime.datetime(2012, 1, 1, 0, 0)),)
        self.assertEqual(dml.Execute([1, 0, 1, "a0"]), res)
        self.assertEqual(await dml.aioExecute([1, 0, 1, "a0"]), res)
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        with self.assertRaises(errors.DMLClauseError):
            (
                db.Select("*")
                .From(db.tb1)
                .Where("t0.id > 10")
                .GroupBy("t0.id")
                .Having("id > 5")
                .Having("id > 5")
                .statement()
            )

        # Finished
        db.Drop(True)
        self.log_ended("HAVING")

    async def test_window_clause(self) -> None:
        self.log_start("WINDOW")

        # Window
        db = self.prepareDatabase()
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = (
            db.Select(
                "name",
                "FIRST_VALUE(name) OVER w1 AS name_1st",
                "LAST_VALUE(name) OVER w1 AS name_lst",
            )
            .From(db.tb1)
            .Window("w1")
        )
        self.assertEqual(
            dml.statement(),
            "SELECT\n\t"
            "name,\n\t"
            "FIRST_VALUE(name) OVER w1 AS name_1st,\n\t"
            "LAST_VALUE(name) OVER w1 AS name_lst\n"
            "FROM db.tb1 AS t0\n"
            "WINDOW w1 AS ()",
        )
        res = (
            ("a0", "a0", "c2"),
            ("a1", "a0", "c2"),
            ("b0", "a0", "c2"),
            ("b1", "a0", "c2"),
            ("c1", "a0", "c2"),
            ("c2", "a0", "c2"),
        )
        self.assertEqual(dml.Execute(), res)
        self.assertEqual(await dml.aioExecute(), res)
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = (
            db.Select(
                "name",
                "FIRST_VALUE(name) OVER w1 AS name_1st",
                "LAST_VALUE(name) OVER w1 AS name_lst",
            )
            .From(db.tb1)
            .Window("w1", partition_by="name")
        )
        self.assertEqual(
            dml.statement(),
            "SELECT\n\t"
            "name,\n\t"
            "FIRST_VALUE(name) OVER w1 AS name_1st,\n\t"
            "LAST_VALUE(name) OVER w1 AS name_lst\n"
            "FROM db.tb1 AS t0\n"
            "WINDOW w1 AS (PARTITION BY name)",
        )
        res = (
            ("a0", "a0", "a0"),
            ("a1", "a1", "a1"),
            ("b0", "b0", "b0"),
            ("b1", "b1", "b1"),
            ("c1", "c1", "c1"),
            ("c2", "c2", "c2"),
        )
        self.assertEqual(dml.Execute(), res)
        self.assertEqual(await dml.aioExecute(), res)
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = (
            db.Select(
                "name",
                "FIRST_VALUE(name) OVER w1 AS name_1st",
                "LAST_VALUE(name) OVER w1 AS name_lst",
            )
            .From(db.tb1)
            .Window("w1", order_by="name DESC")
        )
        self.assertEqual(
            dml.statement(),
            "SELECT\n\t"
            "name,\n\t"
            "FIRST_VALUE(name) OVER w1 AS name_1st,\n\t"
            "LAST_VALUE(name) OVER w1 AS name_lst\n"
            "FROM db.tb1 AS t0\n"
            "WINDOW w1 AS (ORDER BY name DESC)",
        )
        res = (
            ("c2", "c2", "c2"),
            ("c1", "c2", "c1"),
            ("b1", "c2", "b1"),
            ("b0", "c2", "b0"),
            ("a1", "c2", "a1"),
            ("a0", "c2", "a0"),
        )
        self.assertEqual(dml.Execute(), res)
        self.assertEqual(await dml.aioExecute(), res)
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = (
            db.Select(
                "name",
                "FIRST_VALUE(name) OVER w1 AS name_1st",
                "LAST_VALUE(name) OVER w1 AS name_lst",
            )
            .From(db.tb1)
            .Window("w1", frame_clause="ROWS UNBOUNDED PRECEDING")
        )
        self.assertEqual(
            dml.statement(),
            "SELECT\n\t"
            "name,\n\t"
            "FIRST_VALUE(name) OVER w1 AS name_1st,\n\t"
            "LAST_VALUE(name) OVER w1 AS name_lst\n"
            "FROM db.tb1 AS t0\n"
            "WINDOW w1 AS (ROWS UNBOUNDED PRECEDING)",
        )
        res = (
            ("a0", "a0", "a0"),
            ("a1", "a0", "a1"),
            ("b0", "a0", "b0"),
            ("b1", "a0", "b1"),
            ("c1", "a0", "c1"),
            ("c2", "a0", "c2"),
        )
        self.assertEqual(dml.Execute(), res)
        self.assertEqual(await dml.aioExecute(), res)
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = (
            db.Select("*")
            .From(db.tb1)
            .Window("w1", "id", "id DESC", "ROWS UNBOUNDED PRECEDING")
        )
        self.assertEqual(
            dml.statement(),
            "SELECT *\nFROM db.tb1 AS t0\n"
            "WINDOW w1 AS (\n\t"
            "PARTITION BY id\n\t"
            "ORDER BY id DESC\n\t"
            "ROWS UNBOUNDED PRECEDING\n)",
        )
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = (
            db.Select("*")
            .From(db.tb1)
            .Window("w1", ["id", "name"], ["name", "id"], "ROWS UNBOUNDED PRECEDING")
        )
        self.assertEqual(
            dml.statement(),
            "SELECT *\nFROM db.tb1 AS t0\n"
            "WINDOW w1 AS (\n\t"
            "PARTITION BY id, name\n\t"
            "ORDER BY name, id\n\t"
            "ROWS UNBOUNDED PRECEDING\n)",
        )
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = (
            db.Select(
                "name",
                "FIRST_VALUE(name) OVER w1 AS w1_name_1st",
                "LAST_VALUE(name) OVER w1 AS w1_name_lst",
                "FIRST_VALUE(name) OVER w2 AS w2_name_1st",
                "LAST_VALUE(name) OVER w2 AS w2_name_lst",
            )
            .From(db.tb1)
            .Window("w1", order_by="name ASC")
            .Window("w2", order_by="name DESC")
        )
        self.assertEqual(
            dml.statement(),
            "SELECT\n\t"
            "name,\n\t"
            "FIRST_VALUE(name) OVER w1 AS w1_name_1st,\n\t"
            "LAST_VALUE(name) OVER w1 AS w1_name_lst,\n\t"
            "FIRST_VALUE(name) OVER w2 AS w2_name_1st,\n\t"
            "LAST_VALUE(name) OVER w2 AS w2_name_lst\n"
            "FROM db.tb1 AS t0\n"
            "WINDOW w1 AS (ORDER BY name ASC), w2 AS (ORDER BY name DESC)",
        )
        res = (
            ("c2", "a0", "c2", "c2", "c2"),
            ("c1", "a0", "c1", "c2", "c1"),
            ("b1", "a0", "b1", "c2", "b1"),
            ("b0", "a0", "b0", "c2", "b0"),
            ("a1", "a0", "a1", "c2", "a1"),
            ("a0", "a0", "a0", "c2", "a0"),
        )
        self.assertEqual(dml.Execute(), res)
        self.assertEqual(await dml.aioExecute(), res)

        # Finished
        db.Drop(True)
        self.log_ended("WINDOW")

    async def test_order_by_clause(self) -> None:
        self.log_start("ORDER BY")

        # Order By
        db = self.prepareDatabase()
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = db.Select("id").From(db.tb1).OrderBy("id DESC")
        self.assertEqual(
            dml.statement(), "SELECT id\nFROM db.tb1 AS t0\nORDER BY id DESC"
        )
        res = ((6,), (5,), (4,), (3,), (2,), (1,))
        self.assertEqual(dml.Execute(), res)
        self.assertEqual(await dml.aioExecute(), res)
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = db.Select("id").From(db.tb1).OrderBy("id DESC", "name ASC")
        self.assertEqual(
            dml.statement(), "SELECT id\nFROM db.tb1 AS t0\nORDER BY id DESC, name ASC"
        )
        res = ((6,), (5,), (4,), (3,), (2,), (1,))
        self.assertEqual(dml.Execute(), res)
        self.assertEqual(await dml.aioExecute(), res)
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = (
            db.Select("*")
            .From(db.tb1)
            .GroupBy("id", "name", with_rollup=True)
            .OrderBy("id DESC", "name ASC", with_rollup=True)
        )
        self.assertEqual(
            dml.statement(),
            "SELECT *\n"
            "FROM db.tb1 AS t0\n"
            "GROUP BY id, name WITH ROLLUP\n"
            "ORDER BY id DESC, name ASC WITH ROLLUP",
        )
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        with self.assertRaises(errors.DMLClauseError):
            (
                db.Select("*")
                .From(db.tb1)
                .OrderBy("t0.id", "t0.name DESC")
                .OrderBy("t0.id", "t0.name DESC")
                .statement()
            )

        # Finished
        db.Drop(True)
        self.log_ended("ORDER BY")

    async def test_limit_cluase(self) -> None:
        self.log_start("LIMIT")

        # Limit
        db = self.prepareDatabase()
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = db.Select("*").From(db.tb1).Limit(10)
        self.assertEqual(dml.statement(), "SELECT *\nFROM db.tb1 AS t0\nLIMIT 10")
        self.assertEqual(dml.Execute(), self.TEST_DATA)
        self.assertEqual(await dml.aioExecute(), self.TEST_DATA)
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = db.Select("*").From(db.tb1).Limit(2, 1)
        self.assertEqual(dml.statement(), "SELECT *\nFROM db.tb1 AS t0\nLIMIT 1, 2")
        res = (
            (2, "a1", 1.1, datetime.datetime(2012, 1, 1, 0, 0)),
            (3, "b0", 2.0, datetime.datetime(2012, 2, 1, 0, 0)),
        )
        self.assertEqual(dml.Execute(), res)
        self.assertEqual(await dml.aioExecute(), res)
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        with self.assertRaises(errors.DMLClauseError):
            db.Select("*").From(db.tb1).Limit(10, 5).Limit(10, 5).statement()

        # Finished
        db.Drop(True)
        self.log_ended("LIMIT")

    async def test_locking_reads_cluase(self) -> None:
        self.log_start("LOCKING READS")

        # Limit
        db = self.prepareDatabase()
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = db.Select("*").From(db.tb1).Limit(10).ForUpdate()
        self.assertEqual(
            dml.statement(), "SELECT *\nFROM db.tb1 AS t0\nLIMIT 10\nFOR UPDATE"
        )
        self.assertEqual(dml.Execute(), self.TEST_DATA)
        self.assertEqual(await dml.aioExecute(), self.TEST_DATA)
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = db.Select("*").From(db.tb1).Limit(10).ForUpdate(option="NOWAIT")
        self.assertEqual(
            dml.statement(),
            "SELECT *\nFROM db.tb1 AS t0\nLIMIT 10\nFOR UPDATE NOWAIT",
        )
        self.assertEqual(dml.Execute(), self.TEST_DATA)
        self.assertEqual(await dml.aioExecute(), self.TEST_DATA)
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = db.Select("*").From(db.tb1).Limit(10).ForUpdate(option="SKIP LOCKED")
        self.assertEqual(
            dml.statement(),
            "SELECT *\nFROM db.tb1 AS t0\nLIMIT 10\nFOR UPDATE SKIP LOCKED",
        )
        self.assertEqual(dml.Execute(), self.TEST_DATA)
        self.assertEqual(await dml.aioExecute(), self.TEST_DATA)
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = (
            db.Select("t0.*")
            .From(db.tb1)
            .Join(db.tb2, using="id")
            .Limit(10)
            .ForUpdate("t0", "t1")
        )
        self.assertEqual(
            dml.statement(),
            "SELECT t0.*\n"
            "FROM db.tb1 AS t0\n"
            "INNER JOIN db.tb2 AS t1\n\tUSING (id)\n"
            "LIMIT 10\n"
            "FOR UPDATE OF t0, t1",
        )
        self.assertEqual(dml.Execute(), self.TEST_DATA)
        self.assertEqual(await dml.aioExecute(), self.TEST_DATA)
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = (
            db.Select("t0.*")
            .From(db.tb1)
            .Join(db.tb2, using="id")
            .Limit(10)
            .ForUpdate("t0")
            .ForShare("t1")
        )
        self.assertEqual(
            dml.statement(),
            "SELECT t0.*\n"
            "FROM db.tb1 AS t0\n"
            "INNER JOIN db.tb2 AS t1\n\tUSING (id)\n"
            "LIMIT 10\n"
            "FOR UPDATE OF t0\n"
            "FOR SHARE OF t1",
        )
        self.assertEqual(dml.Execute(), self.TEST_DATA)
        self.assertEqual(await dml.aioExecute(), self.TEST_DATA)
        with db.transaction() as conn:
            self.assertEqual(dml.Execute(conn=conn, fetch=False), 6)
        async with db.transaction() as conn:
            self.assertEqual(await dml.aioExecute(conn=conn, fetch=False), 6)
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        with self.assertRaises(errors.DMLArgumentError):
            async with db.transaction() as conn:
                dml.Execute(conn=conn)
        with self.assertRaises(errors.DMLArgumentError):
            with db.transaction() as conn:
                await dml.aioExecute(conn=conn)

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        with self.assertRaises(errors.DMLClauseError):
            db.Select("*").UseIndex("idx1")

        # Finished
        db.Drop(True)
        self.log_ended("LOCKING READS")

    async def test_into_cluase(self) -> None:
        self.log_start("INTO")

        # Into
        db = self.prepareDatabase()
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = db.Select("id").From(db.tb1).OrderBy("id DESC").Limit(1).Into("@var")
        self.assertEqual(
            dml.statement(),
            "SELECT id\nFROM db.tb1 AS t0\nORDER BY id DESC\nLIMIT 1\nINTO @var",
        )
        res = ((6,),)
        with db.acquire() as conn:
            with conn.cursor() as cur:
                cur.execute("SET @var = 0")
            dml.Execute(conn=conn)
            self.assertEqual(db.Select("@var").Execute(conn=conn), res)
        async with db.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SET @var = 0")
            await dml.aioExecute(conn=conn)
            self.assertEqual(await db.Select("@var").aioExecute(conn=conn), res)
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = (
            db.Select("id", "name")
            .From(db.tb1)
            .OrderBy("id DESC")
            .Limit(1)
            .Into("@var1", "@var2")
        )
        self.assertEqual(
            dml.statement(),
            "SELECT\n\tid,\n\tname\n"
            "FROM db.tb1 AS t0\n"
            "ORDER BY id DESC\n"
            "LIMIT 1\nINTO @var1, @var2",
        )
        res = ((6, "c2"),)
        vars = ("@var1", "@var2")
        with db.acquire() as conn:
            with conn.cursor() as cur:
                cur.execute("SET @var1 = 0, @var2 = 'NONE'")
            dml.Execute(conn=conn)
            self.assertEqual(db.Select(vars).Execute(conn=conn), res)
        async with db.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SET @var1 = 0, @var2 = 'NONE'")
            await dml.aioExecute(conn=conn)
            self.assertEqual(await db.Select(vars).aioExecute(conn=conn), res)
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        with self.assertRaises(errors.DMLClauseError):
            (
                db.Select("id")
                .From(db.tb1)
                .Limit(1)
                .Into("@var")
                .Into("@var")
                .statement()
            )

        # Finished
        db.Drop(True)
        self.log_ended("INTO")

    async def test_set_operation_clause(self) -> None:
        self.log_start("SET OPERATION")

        # Union
        db = self.prepareDatabase()
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = db.Select("*").From(db.tb1).Union(db.Select("*").From(db.tb2))
        self.assertEqual(
            dml.statement(),
            "SELECT *\n"
            "FROM db.tb1 AS t0\n"
            "UNION DISTINCT\n"
            "(\n\t"
            "SELECT *\n\t"
            "FROM db.tb2 AS t0\n"
            ")",
        )
        self.assertEqual(dml.Execute(), self.TEST_DATA)
        self.assertEqual(await dml.aioExecute(), self.TEST_DATA)
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = db.Select("*").From(db.tb1).Union(db.Select("*").From(db.tb2), all=True)
        self.assertEqual(
            dml.statement(),
            "SELECT *\n"
            "FROM db.tb1 AS t0\n"
            "UNION ALL\n"
            "(\n\t"
            "SELECT *\n\t"
            "FROM db.tb2 AS t0\n"
            ")",
        )
        res = self.TEST_DATA + self.TEST_DATA
        self.assertEqual(dml.Execute(), res)
        self.assertEqual(await dml.aioExecute(), res)
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = (
            db.Select("*")
            .From(db.tb1)
            .Union(
                db.Select("*")
                .From(db.tb1)
                .UseIndex("idx1", "ORDER BY")
                .Join(db.tb2, using="id")
                .UseIndex("PRIMARY", "JOIN")
                .Where("t0.id > 10")
                .GroupBy("t0.id")
                .Having("id > 5")
                .Window("w1", "id", "name")
                .Window("w2", "name", "id")
                .OrderBy("id")
                .Limit(10)
            )
        )
        self.assertEqual(
            dml.statement(),
            "SELECT *\n"
            "FROM db.tb1 AS t0\n"
            "UNION DISTINCT\n"
            "(\n\t"
            "SELECT *\n\t"
            "FROM db.tb1 AS t0\n\t\t"
            "USE INDEX (idx1, ORDER BY)\n\t"
            "INNER JOIN db.tb2 AS t1\n\t\t"
            "USE INDEX (PRIMARY, JOIN)\n\t\t"
            "USING (id)\n\t"
            "WHERE t0.id > 10\n\t"
            "GROUP BY t0.id\n\t"
            "HAVING id > 5\n\t"
            "WINDOW w1 AS (\n\t\t"
            "PARTITION BY id\n\t\t"
            "ORDER BY name\n\t"
            "), w2 AS (\n\t\t"
            "PARTITION BY name\n\t\t"
            "ORDER BY id\n\t"
            ")\n\t"
            "ORDER BY id\n\t"
            "LIMIT 10\n"
            ")",
        )
        self.assertEqual(
            dml.statement(1),
            "\tSELECT *\n"
            "\tFROM db.tb1 AS t0\n"
            "\tUNION DISTINCT\n"
            "\t(\n\t"
            "\tSELECT *\n\t"
            "\tFROM db.tb1 AS t0\n\t\t"
            "\tUSE INDEX (idx1, ORDER BY)\n\t"
            "\tINNER JOIN db.tb2 AS t1\n\t\t"
            "\tUSE INDEX (PRIMARY, JOIN)\n\t\t"
            "\tUSING (id)\n\t"
            "\tWHERE t0.id > 10\n\t"
            "\tGROUP BY t0.id\n\t"
            "\tHAVING id > 5\n\t"
            "\tWINDOW w1 AS (\n\t\t"
            "\tPARTITION BY id\n\t\t"
            "\tORDER BY name\n\t"
            "\t), w2 AS (\n\t\t"
            "\tPARTITION BY name\n\t\t"
            "\tORDER BY id\n\t"
            "\t)\n\t"
            "\tORDER BY id\n\t"
            "\tLIMIT 10\n"
            "\t)",
        )
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = (
            db.Select("*")
            .From(db.tb1)
            .Union(
                db.Select("*")
                .From(db.tb1)
                .UseIndex("idx1", "ORDER BY")
                .Join(db.tb2, using="id")
                .UseIndex("PRIMARY", "JOIN")
                .Where("t0.id > 10")
                .GroupBy("t0.id")
                .Having("id > 5")
                .Window("w1", "id", "name")
                .Window("w2", "name", "id")
                .OrderBy("id")
                .Limit(10)
            )
            .Union(
                db.Select("*")
                .From(db.tb1)
                .UseIndex("idx1", "ORDER BY")
                .Join(db.tb2, using="id")
                .UseIndex("PRIMARY", "JOIN")
                .Where("t0.id > 10")
                .GroupBy("t0.id")
                .Having("id > 5")
                .Window("w1", "id", "name")
                .Window("w2", "name", "id")
                .OrderBy("id")
                .Limit(10),
                True,
            )
        )
        self.assertEqual(
            dml.statement(),
            "SELECT *\n"
            "FROM db.tb1 AS t0\n"
            "UNION DISTINCT\n"
            "(\n\t"
            "SELECT *\n\t"
            "FROM db.tb1 AS t0\n\t\t"
            "USE INDEX (idx1, ORDER BY)\n\t"
            "INNER JOIN db.tb2 AS t1\n\t\t"
            "USE INDEX (PRIMARY, JOIN)\n\t\t"
            "USING (id)\n\t"
            "WHERE t0.id > 10\n\t"
            "GROUP BY t0.id\n\t"
            "HAVING id > 5\n\t"
            "WINDOW w1 AS (\n\t\t"
            "PARTITION BY id\n\t\t"
            "ORDER BY name\n\t"
            "), w2 AS (\n\t\t"
            "PARTITION BY name\n\t\t"
            "ORDER BY id\n\t"
            ")\n\t"
            "ORDER BY id\n\t"
            "LIMIT 10\n"
            ")\n"
            "UNION ALL\n"
            "(\n\t"
            "SELECT *\n\t"
            "FROM db.tb1 AS t0\n\t\t"
            "USE INDEX (idx1, ORDER BY)\n\t"
            "INNER JOIN db.tb2 AS t1\n\t\t"
            "USE INDEX (PRIMARY, JOIN)\n\t\t"
            "USING (id)\n\t"
            "WHERE t0.id > 10\n\t"
            "GROUP BY t0.id\n\t"
            "HAVING id > 5\n\t"
            "WINDOW w1 AS (\n\t\t"
            "PARTITION BY id\n\t\t"
            "ORDER BY name\n\t"
            "), w2 AS (\n\t\t"
            "PARTITION BY name\n\t\t"
            "ORDER BY id\n\t"
            ")\n\t"
            "ORDER BY id\n\t"
            "LIMIT 10\n"
            ")",
        )
        self.assertEqual(
            dml.statement(1),
            "\tSELECT *\n"
            "\tFROM db.tb1 AS t0\n"
            "\tUNION DISTINCT\n"
            "\t(\n\t"
            "\tSELECT *\n\t"
            "\tFROM db.tb1 AS t0\n\t\t"
            "\tUSE INDEX (idx1, ORDER BY)\n\t"
            "\tINNER JOIN db.tb2 AS t1\n\t\t"
            "\tUSE INDEX (PRIMARY, JOIN)\n\t\t"
            "\tUSING (id)\n\t"
            "\tWHERE t0.id > 10\n\t"
            "\tGROUP BY t0.id\n\t"
            "\tHAVING id > 5\n\t"
            "\tWINDOW w1 AS (\n\t\t"
            "\tPARTITION BY id\n\t\t"
            "\tORDER BY name\n\t"
            "\t), w2 AS (\n\t\t"
            "\tPARTITION BY name\n\t\t"
            "\tORDER BY id\n\t"
            "\t)\n\t"
            "\tORDER BY id\n\t"
            "\tLIMIT 10\n"
            "\t)\n"
            "\tUNION ALL\n"
            "\t(\n\t"
            "\tSELECT *\n\t"
            "\tFROM db.tb1 AS t0\n\t\t"
            "\tUSE INDEX (idx1, ORDER BY)\n\t"
            "\tINNER JOIN db.tb2 AS t1\n\t\t"
            "\tUSE INDEX (PRIMARY, JOIN)\n\t\t"
            "\tUSING (id)\n\t"
            "\tWHERE t0.id > 10\n\t"
            "\tGROUP BY t0.id\n\t"
            "\tHAVING id > 5\n\t"
            "\tWINDOW w1 AS (\n\t\t"
            "\tPARTITION BY id\n\t\t"
            "\tORDER BY name\n\t"
            "\t), w2 AS (\n\t\t"
            "\tPARTITION BY name\n\t\t"
            "\tORDER BY id\n\t"
            "\t)\n\t"
            "\tORDER BY id\n\t"
            "\tLIMIT 10\n"
            "\t)",
        )
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = (
            db.Select("*")
            .From(db.tb1)
            .Union(
                db.Select("*")
                .From(db.tb1)
                .Union(
                    db.Select("*")
                    .From(db.tb1)
                    .UseIndex("idx1", "ORDER BY")
                    .Join(db.tb2, using="id")
                    .UseIndex("PRIMARY", "JOIN")
                    .Where("t0.id > 10")
                    .GroupBy("t0.id")
                    .Having("id > 5")
                    .Window("w1", "id", "name")
                    .Window("w2", "name", "id")
                    .OrderBy("id")
                    .Limit(10)
                )
            )
        )
        self.assertEqual(
            dml.statement(),
            "SELECT *\n"
            "FROM db.tb1 AS t0\n"
            "UNION DISTINCT\n"
            "(\n\t"
            "SELECT *\n\t"
            "FROM db.tb1 AS t0\n\t"
            "UNION DISTINCT\n\t"
            "(\n\t\t"
            "SELECT *\n\t\t"
            "FROM db.tb1 AS t0\n\t\t\t"
            "USE INDEX (idx1, ORDER BY)\n\t\t"
            "INNER JOIN db.tb2 AS t1\n\t\t\t"
            "USE INDEX (PRIMARY, JOIN)\n\t\t\t"
            "USING (id)\n\t\t"
            "WHERE t0.id > 10\n\t\t"
            "GROUP BY t0.id\n\t\t"
            "HAVING id > 5\n\t\t"
            "WINDOW w1 AS (\n\t\t\t"
            "PARTITION BY id\n\t\t\t"
            "ORDER BY name\n\t\t"
            "), w2 AS (\n\t\t\t"
            "PARTITION BY name\n\t\t\t"
            "ORDER BY id\n\t\t"
            ")\n\t\t"
            "ORDER BY id\n\t\t"
            "LIMIT 10\n\t"
            ")\n"
            ")",
        )
        self.assertEqual(
            dml.statement(1),
            "\tSELECT *\n"
            "\tFROM db.tb1 AS t0\n"
            "\tUNION DISTINCT\n"
            "\t(\n\t"
            "\tSELECT *\n\t"
            "\tFROM db.tb1 AS t0\n\t"
            "\tUNION DISTINCT\n\t"
            "\t(\n\t\t"
            "\tSELECT *\n\t\t"
            "\tFROM db.tb1 AS t0\n\t\t\t"
            "\tUSE INDEX (idx1, ORDER BY)\n\t\t"
            "\tINNER JOIN db.tb2 AS t1\n\t\t\t"
            "\tUSE INDEX (PRIMARY, JOIN)\n\t\t\t"
            "\tUSING (id)\n\t\t"
            "\tWHERE t0.id > 10\n\t\t"
            "\tGROUP BY t0.id\n\t\t"
            "\tHAVING id > 5\n\t\t"
            "\tWINDOW w1 AS (\n\t\t\t"
            "\tPARTITION BY id\n\t\t\t"
            "\tORDER BY name\n\t\t"
            "\t), w2 AS (\n\t\t\t"
            "\tPARTITION BY name\n\t\t\t"
            "\tORDER BY id\n\t\t"
            "\t)\n\t\t"
            "\tORDER BY id\n\t\t"
            "\tLIMIT 10\n\t"
            "\t)\n"
            "\t)",
        )
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        with self.assertRaises(errors.DMLArgumentError):
            db.Select("*").Union(1)

        # Intersect
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = db.Select("*").From(db.tb1).Intersect(db.Select("*").From(db.tb2))
        self.assertEqual(
            dml.statement(),
            "SELECT *\n"
            "FROM db.tb1 AS t0\n"
            "INTERSECT DISTINCT\n"
            "(\n\t"
            "SELECT *\n\t"
            "FROM db.tb2 AS t0\n"
            ")",
        )
        self.assertEqual(dml.Execute(), self.TEST_DATA)
        self.assertEqual(await dml.aioExecute(), self.TEST_DATA)

        # Except
        dml = db.Select("*").From(db.tb1).Except(db.Select("*").From(db.tb2))
        self.assertEqual(
            dml.statement(),
            "SELECT *\n"
            "FROM db.tb1 AS t0\n"
            "EXCEPT DISTINCT\n"
            "(\n\t"
            "SELECT *\n\t"
            "FROM db.tb2 AS t0\n"
            ")",
        )
        self.assertEqual(dml.Execute(), ())
        self.assertEqual(await dml.aioExecute(), ())

        # Finished
        db.Drop(True)
        self.log_ended("SET OPERATION")

    async def test_from_subquery(self) -> None:
        self.log_start("FROM subquery")

        # From Subquery
        db = self.prepareDatabase()
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = db.Select("*").From(db.Select("*").From(db.tb1))
        self.assertEqual(
            dml.statement(),
            "SELECT *\nFROM (\n\tSELECT *\n\tFROM db.tb1 AS t0\n) AS t0",
        )
        self.assertEqual(dml.Execute(), self.TEST_DATA)
        self.assertEqual(await dml.aioExecute(), self.TEST_DATA)
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = db.Select("*").From(db.Select("*").From(db.Select("*").From(db.tb1)))
        self.assertEqual(
            dml.statement(),
            "SELECT *\n"
            "FROM (\n\t"
            "SELECT *\n\t"
            "FROM (\n\t\t"
            "SELECT *\n\t\t"
            "FROM db.tb1 AS t0\n\t"
            ") AS t0\n"
            ") AS t0",
        )
        self.assertEqual(dml.Execute(), self.TEST_DATA)
        self.assertEqual(await dml.aioExecute(), self.TEST_DATA)
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = db.Select("*").From(
            db.Select("*")
            .From(db.tb1)
            .UseIndex("idx1", "ORDER BY")
            .Join(db.tb2, using="id")
            .UseIndex("PRIMARY", "JOIN")
            .Where("t0.id > 10")
            .GroupBy("t0.id")
            .Having("id > 5")
            .Window("w1", "id", "name")
            .Window("w2", "name", "id")
            .OrderBy("id")
            .Limit(10),
        )
        self.assertEqual(
            dml.statement(),
            "SELECT *\n"
            "FROM (\n\t"
            "SELECT *\n\t"
            "FROM db.tb1 AS t0\n\t\t"
            "USE INDEX (idx1, ORDER BY)\n\t"
            "INNER JOIN db.tb2 AS t1\n\t\t"
            "USE INDEX (PRIMARY, JOIN)\n\t\t"
            "USING (id)\n\t"
            "WHERE t0.id > 10\n\t"
            "GROUP BY t0.id\n\t"
            "HAVING id > 5\n\t"
            "WINDOW w1 AS (\n\t\t"
            "PARTITION BY id\n\t\t"
            "ORDER BY name\n\t"
            "), w2 AS (\n\t\t"
            "PARTITION BY name\n\t\t"
            "ORDER BY id\n\t"
            ")\n\t"
            "ORDER BY id\n\t"
            "LIMIT 10\n"
            ") AS t0",
        )
        self.assertEqual(
            dml.statement(1),
            "\tSELECT *\n"
            "\tFROM (\n\t"
            "\tSELECT *\n\t"
            "\tFROM db.tb1 AS t0\n\t\t"
            "\tUSE INDEX (idx1, ORDER BY)\n\t"
            "\tINNER JOIN db.tb2 AS t1\n\t\t"
            "\tUSE INDEX (PRIMARY, JOIN)\n\t\t"
            "\tUSING (id)\n\t"
            "\tWHERE t0.id > 10\n\t"
            "\tGROUP BY t0.id\n\t"
            "\tHAVING id > 5\n\t"
            "\tWINDOW w1 AS (\n\t\t"
            "\tPARTITION BY id\n\t\t"
            "\tORDER BY name\n\t"
            "\t), w2 AS (\n\t\t"
            "\tPARTITION BY name\n\t\t"
            "\tORDER BY id\n\t"
            "\t)\n\t"
            "\tORDER BY id\n\t"
            "\tLIMIT 10\n"
            "\t) AS t0",
        )
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = db.Select("*").From(
            db.Select("*").From(
                db.Select("*")
                .From(db.tb1)
                .UseIndex("idx1", "ORDER BY")
                .Join(db.tb2, using="id")
                .UseIndex("PRIMARY", "JOIN")
                .Where("t0.id > 10")
                .GroupBy("t0.id")
                .Having("id > 5")
                .Window("w1", "id", "name")
                .Window("w2", "name", "id")
                .OrderBy("id")
                .Limit(10),
            )
        )
        self.assertEqual(
            dml.statement(),
            "SELECT *\n"
            "FROM (\n\t"
            "SELECT *\n\t"
            "FROM (\n\t\t"
            "SELECT *\n\t\t"
            "FROM db.tb1 AS t0\n\t\t\t"
            "USE INDEX (idx1, ORDER BY)\n\t\t"
            "INNER JOIN db.tb2 AS t1\n\t\t\t"
            "USE INDEX (PRIMARY, JOIN)\n\t\t\t"
            "USING (id)\n\t\t"
            "WHERE t0.id > 10\n\t\t"
            "GROUP BY t0.id\n\t\t"
            "HAVING id > 5\n\t\t"
            "WINDOW w1 AS (\n\t\t\t"
            "PARTITION BY id\n\t\t\t"
            "ORDER BY name\n\t\t"
            "), w2 AS (\n\t\t\t"
            "PARTITION BY name\n\t\t\t"
            "ORDER BY id\n\t\t"
            ")\n\t\t"
            "ORDER BY id\n\t\t"
            "LIMIT 10\n\t"
            ") AS t0\n"
            ") AS t0",
        )
        self.assertEqual(
            dml.statement(1),
            "\tSELECT *\n"
            "\tFROM (\n\t"
            "\tSELECT *\n\t"
            "\tFROM (\n\t\t"
            "\tSELECT *\n\t\t"
            "\tFROM db.tb1 AS t0\n\t\t\t"
            "\tUSE INDEX (idx1, ORDER BY)\n\t\t"
            "\tINNER JOIN db.tb2 AS t1\n\t\t\t"
            "\tUSE INDEX (PRIMARY, JOIN)\n\t\t\t"
            "\tUSING (id)\n\t\t"
            "\tWHERE t0.id > 10\n\t\t"
            "\tGROUP BY t0.id\n\t\t"
            "\tHAVING id > 5\n\t\t"
            "\tWINDOW w1 AS (\n\t\t\t"
            "\tPARTITION BY id\n\t\t\t"
            "\tORDER BY name\n\t\t"
            "\t), w2 AS (\n\t\t\t"
            "\tPARTITION BY name\n\t\t\t"
            "\tORDER BY id\n\t\t"
            "\t)\n\t\t"
            "\tORDER BY id\n\t\t"
            "\tLIMIT 10\n\t"
            "\t) AS t0\n"
            "\t) AS t0",
        )

        # Finished
        db.Drop(True)
        self.log_ended("FROM subquery")

    async def test_join_subquery(self) -> None:
        self.log_start("JOIN subquery")

        # Join Subquery
        db = self.prepareDatabase()

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = (
            db.Select("t0.*").From(db.tb1).Join(db.Select("*").From(db.tb2), using="id")
        )
        self.assertEqual(
            dml.statement(),
            "SELECT t0.*\n"
            "FROM db.tb1 AS t0\n"
            "INNER JOIN (\n\t"
            "SELECT *\n\t"
            "FROM db.tb2 AS t0\n"
            ") AS t1\n\t"
            "USING (id)",
        )
        self.assertEqual(dml.Execute(), self.TEST_DATA)
        self.assertEqual(await dml.aioExecute(), self.TEST_DATA)

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = (
            db.Select("t0.*")
            .From(db.tb1)
            .Join(db.Select("*").From(db.tb1), using="id")
            .Join(db.Select("*").From(db.tb2), using="id")
        )
        self.assertEqual(
            dml.statement(),
            "SELECT t0.*\n"
            "FROM db.tb1 AS t0\n"
            "INNER JOIN (\n\t"
            "SELECT *\n\t"
            "FROM db.tb1 AS t0\n"
            ") AS t1\n\t"
            "USING (id)\n"
            "INNER JOIN (\n\t"
            "SELECT *\n\t"
            "FROM db.tb2 AS t0\n"
            ") AS t2\n\t"
            "USING (id)",
        )
        self.assertEqual(dml.Execute(), self.TEST_DATA)
        self.assertEqual(await dml.aioExecute(), self.TEST_DATA)

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = (
            db.Select("*")
            .From(db.tb1)
            .Join(
                db.Select("*")
                .From(db.tb1)
                .UseIndex("idx1", "ORDER BY")
                .Join(db.tb2, using="id")
                .UseIndex("PRIMARY", "JOIN")
                .Where("t0.id > 10")
                .GroupBy("t0.id")
                .Having("id > 5")
                .Window("w1", "id", "name")
                .Window("w2", "name", "id")
                .OrderBy("id")
                .Limit(10),
                using="id",
            )
        )
        self.assertEqual(
            dml.statement(),
            "SELECT *\n"
            "FROM db.tb1 AS t0\n"
            "INNER JOIN (\n\t"
            "SELECT *\n\t"
            "FROM db.tb1 AS t0\n\t\t"
            "USE INDEX (idx1, ORDER BY)\n\t"
            "INNER JOIN db.tb2 AS t1\n\t\t"
            "USE INDEX (PRIMARY, JOIN)\n\t\t"
            "USING (id)\n\t"
            "WHERE t0.id > 10\n\t"
            "GROUP BY t0.id\n\t"
            "HAVING id > 5\n\t"
            "WINDOW w1 AS (\n\t\t"
            "PARTITION BY id\n\t\t"
            "ORDER BY name\n\t"
            "), w2 AS (\n\t\t"
            "PARTITION BY name\n\t\t"
            "ORDER BY id\n\t"
            ")\n\t"
            "ORDER BY id\n\t"
            "LIMIT 10\n"
            ") AS t1\n\t"
            "USING (id)",
        )
        self.assertEqual(
            dml.statement(1),
            "\tSELECT *\n"
            "\tFROM db.tb1 AS t0\n"
            "\tINNER JOIN (\n\t"
            "\tSELECT *\n\t"
            "\tFROM db.tb1 AS t0\n\t\t"
            "\tUSE INDEX (idx1, ORDER BY)\n\t"
            "\tINNER JOIN db.tb2 AS t1\n\t\t"
            "\tUSE INDEX (PRIMARY, JOIN)\n\t\t"
            "\tUSING (id)\n\t"
            "\tWHERE t0.id > 10\n\t"
            "\tGROUP BY t0.id\n\t"
            "\tHAVING id > 5\n\t"
            "\tWINDOW w1 AS (\n\t\t"
            "\tPARTITION BY id\n\t\t"
            "\tORDER BY name\n\t"
            "\t), w2 AS (\n\t\t"
            "\tPARTITION BY name\n\t\t"
            "\tORDER BY id\n\t"
            "\t)\n\t"
            "\tORDER BY id\n\t"
            "\tLIMIT 10\n"
            "\t) AS t1\n\t"
            "\tUSING (id)",
        )

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = (
            db.Select("*")
            .From(db.tb1)
            .Join(
                db.Select("*")
                .From(db.tb1)
                .Join(
                    db.Select("*")
                    .From(db.tb1)
                    .UseIndex("idx1", "ORDER BY")
                    .Join(db.tb2, using="id")
                    .UseIndex("PRIMARY", "JOIN")
                    .Where("t0.id > 10")
                    .GroupBy("t0.id")
                    .Having("id > 5")
                    .Window("w1", "id", "name")
                    .Window("w2", "name", "id")
                    .OrderBy("id")
                    .Limit(10),
                    using="id",
                )
            )
        )
        self.assertEqual(
            dml.statement(),
            "SELECT *\n"
            "FROM db.tb1 AS t0\n"
            "INNER JOIN (\n\t"
            "SELECT *\n\t"
            "FROM db.tb1 AS t0\n\t"
            "INNER JOIN (\n\t\t"
            "SELECT *\n\t\t"
            "FROM db.tb1 AS t0\n\t\t\t"
            "USE INDEX (idx1, ORDER BY)\n\t\t"
            "INNER JOIN db.tb2 AS t1\n\t\t\t"
            "USE INDEX (PRIMARY, JOIN)\n\t\t\t"
            "USING (id)\n\t\t"
            "WHERE t0.id > 10\n\t\t"
            "GROUP BY t0.id\n\t\t"
            "HAVING id > 5\n\t\t"
            "WINDOW w1 AS (\n\t\t\t"
            "PARTITION BY id\n\t\t\t"
            "ORDER BY name\n\t\t"
            "), w2 AS (\n\t\t\t"
            "PARTITION BY name\n\t\t\t"
            "ORDER BY id\n\t\t"
            ")\n\t\t"
            "ORDER BY id\n\t\t"
            "LIMIT 10\n\t"
            ") AS t1\n\t\t"
            "USING (id)\n"
            ") AS t1",
        )
        self.assertEqual(
            dml.statement(1),
            "\tSELECT *\n"
            "\tFROM db.tb1 AS t0\n"
            "\tINNER JOIN (\n\t"
            "\tSELECT *\n\t"
            "\tFROM db.tb1 AS t0\n\t"
            "\tINNER JOIN (\n\t\t"
            "\tSELECT *\n\t\t"
            "\tFROM db.tb1 AS t0\n\t\t\t"
            "\tUSE INDEX (idx1, ORDER BY)\n\t\t"
            "\tINNER JOIN db.tb2 AS t1\n\t\t\t"
            "\tUSE INDEX (PRIMARY, JOIN)\n\t\t\t"
            "\tUSING (id)\n\t\t"
            "\tWHERE t0.id > 10\n\t\t"
            "\tGROUP BY t0.id\n\t\t"
            "\tHAVING id > 5\n\t\t"
            "\tWINDOW w1 AS (\n\t\t\t"
            "\tPARTITION BY id\n\t\t\t"
            "\tORDER BY name\n\t\t"
            "\t), w2 AS (\n\t\t\t"
            "\tPARTITION BY name\n\t\t\t"
            "\tORDER BY id\n\t\t"
            "\t)\n\t\t"
            "\tORDER BY id\n\t\t"
            "\tLIMIT 10\n\t"
            "\t) AS t1\n\t\t"
            "\tUSING (id)\n"
            "\t) AS t1",
        )

        # Finished
        db.Drop(True)
        self.log_ended("JOIN subquery")

    async def test_table_select(self) -> None:
        self.log_start("TABLE SELECT")

        # Table Select
        db = self.prepareDatabase()
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = db.tb1.Select("t0.*").Join(db.Select("*").From(db.tb2), using="id")
        self.assertEqual(
            dml.statement(),
            "SELECT t0.*\n"
            "FROM db.tb1 AS t0\n"
            "INNER JOIN (\n\t"
            "SELECT *\n\t"
            "FROM db.tb2 AS t0\n"
            ") AS t1\n\t"
            "USING (id)",
        )
        self.assertEqual(dml.Execute(), self.TEST_DATA)
        self.assertEqual(await dml.aioExecute(), self.TEST_DATA)

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = (
            db.tb1.Select("t0.*")
            .Join(db.Select("*").From(db.tb1), using="id")
            .Join(db.Select("*").From(db.tb2), using="id")
        )
        self.assertEqual(
            dml.statement(),
            "SELECT t0.*\n"
            "FROM db.tb1 AS t0\n"
            "INNER JOIN (\n\t"
            "SELECT *\n\t"
            "FROM db.tb1 AS t0\n"
            ") AS t1\n\t"
            "USING (id)\n"
            "INNER JOIN (\n\t"
            "SELECT *\n\t"
            "FROM db.tb2 AS t0\n"
            ") AS t2\n\t"
            "USING (id)",
        )
        self.assertEqual(dml.Execute(), self.TEST_DATA)
        self.assertEqual(await dml.aioExecute(), self.TEST_DATA)

        # Finished
        db.Drop(True)
        self.log_ended("TABLE SELECT")


class TestInsertClause(TestCase):
    name: str = "INSERT Clause"

    # Utils
    def setupDML(self) -> InsertDML:
        return InsertDML("db", self.get_pool())

    # Tests
    def test_all(self) -> None:
        self.test_insert_clause()
        self.test_columns_clause()
        self.test_values_clause()
        self.test_row_alias_clause()
        self.test_set_clause()
        self.test_on_dup_key_update_clause()

    def test_insert_clause(self) -> None:
        self.log_start("INSERT")

        db = TestDatabase("db", self.get_pool())
        dml = self.setupDML()

        # . table: str
        self.assertClauseEqual(dml._gen_insert_clause("tb"), "INSERT INTO tb")
        self.assertClauseEqual(dml._gen_insert_clause("db.tb"), "INSERT INTO db.tb")
        # . table: Table
        self.assertClauseEqual(dml._gen_insert_clause(db.tb1), "INSERT INTO db.tb1")
        # . table: invalid
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_insert_clause("")
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_insert_clause(None)
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_insert_clause(1)
        # . partition
        self.assertClauseEqual(
            dml._gen_insert_clause("tb", "p0"),
            "INSERT INTO tb PARTITION (p0)",
        )
        self.assertClauseEqual(
            dml._gen_insert_clause("tb", ["p0", "p1"]),
            "INSERT INTO tb PARTITION (p0, p1)",
        )
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_insert_clause("tb", 1)
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_insert_clause("tb", "")
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_insert_clause("tb", [None])
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_insert_clause("tb", [""])
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_insert_clause("tb", [1])
        # . ignore
        self.assertClauseEqual(
            dml._gen_insert_clause("tb", ignore=True),
            "INSERT IGNORE INTO tb",
        )
        # . priority
        self.assertClauseEqual(
            dml._gen_insert_clause("tb", priority="HIGH"),
            "INSERT HIGH_PRIORITY INTO tb",
        )
        self.assertClauseEqual(
            dml._gen_insert_clause("tb", priority="low"),
            "INSERT LOW_PRIORITY INTO tb",
        )
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_insert_clause("tb", priority="lowx")
        # . mixed
        self.assertClauseEqual(
            dml._gen_insert_clause("tb", ["p0", "p1"], True, "HIGH"),
            "INSERT HIGH_PRIORITY IGNORE INTO tb PARTITION (p0, p1)",
        )
        # . indent
        # fmt: off
        self.assertClauseEqual(
            dml._gen_insert_clause("tb", ["p0", "p1"], True, "HIGH"),
            "\tINSERT HIGH_PRIORITY IGNORE INTO tb PARTITION (p0, p1)", 1
        )
        self.assertClauseEqual(
            dml._gen_insert_clause("tb", ["p0", "p1"], True, "HIGH"),
            "\t\tINSERT HIGH_PRIORITY IGNORE INTO tb PARTITION (p0, p1)", 2
        )
        # fmt: on

        self.log_ended("INSERT")

    def test_columns_clause(self) -> None:
        self.log_start("COLUMNS")

        db = TestDatabase("db", self.get_pool())
        dml = self.setupDML()

        # fmt: off
        # . columns: str
        self.assertClauseEqual(dml._gen_insert_columns_clause(("id",)), "\t(id)")
        self.assertClauseEqual(dml._gen_insert_columns_clause(("id", "name")), "\t(id, name)")
        # . columns: Column
        self.assertClauseEqual(dml._gen_insert_columns_clause((db.tb1.id,)), "\t(id)")
        self.assertClauseEqual(dml._gen_insert_columns_clause((db.tb1.id, db.tb1.name)), "\t(id, name)")
        # . columns: Columns
        self.assertClauseEqual(dml._gen_insert_columns_clause((db.tb1.columns,)), "\t(id, name, price, dt)")
        # . invalid
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_insert_columns_clause(("",))
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_insert_columns_clause((None,))
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_insert_columns_clause((1,))
        # . indent
        self.assertClauseEqual(
            dml._gen_insert_columns_clause((db.tb1.columns,)), 
            "\t\t(id, name, price, dt)", 1
        )
        self.assertClauseEqual(
            dml._gen_insert_columns_clause((db.tb1.columns,)), 
            "\t\t\t(id, name, price, dt)", 2
        )
        # fmt: on

        self.log_ended("COLUMNS")

    def test_values_clause(self) -> None:
        self.log_start("VALUES")

        dml = self.setupDML()
        # . values
        self.assertClauseEqual(dml._gen_insert_values_clause(1), "VALUES (%s)")
        self.assertClauseEqual(dml._gen_insert_values_clause(2), "VALUES (%s,%s)")
        self.assertClauseEqual(dml._gen_insert_values_clause(3), "VALUES (%s,%s,%s)")
        # . invalid
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_insert_values_clause(0)
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_insert_values_clause(-1)
        # . indent
        self.assertClauseEqual(dml._gen_insert_values_clause(1), "\tVALUES (%s)", 1)
        self.assertClauseEqual(dml._gen_insert_values_clause(1), "\t\tVALUES (%s)", 2)

        self.log_ended("VALUES")

    def test_set_clause(self) -> None:
        self.log_start("SET")

        dml = self.setupDML()
        # . assignments
        self.assertClauseEqual(dml._gen_set_clause(("a=1",)), "SET a=1")
        self.assertClauseEqual(
            dml._gen_set_clause(("a=1", "b=2")), "SET\n\ta=1,\n\tb=2"
        )
        # . invalid
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_set_clause(("",))
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_set_clause((None,))
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_set_clause((1,))
        # . indent
        # fmt: off
        self.assertClauseEqual(dml._gen_set_clause(("a=1",)), "\tSET a=1", 1)
        self.assertClauseEqual(
            dml._gen_set_clause(("a=1", "b=2")), 
            "\tSET\n\t\ta=1,\n\t\tb=2", 1
        )
        self.assertClauseEqual(dml._gen_set_clause(("a=1",)), "\t\tSET a=1", 2)
        self.assertClauseEqual(
            dml._gen_set_clause(("a=1", "b=2")),
            "\t\tSET\n\t\t\ta=1,\n\t\t\tb=2", 2
        )
        # fmt: on

        self.log_ended("SET")

    def test_row_alias_clause(self) -> None:
        self.log_start("ROW ALIAS")

        dml = self.setupDML()
        # . row alias
        self.assertClauseEqual(dml._gen_row_alias_clause("r", ()), "AS r")
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_row_alias_clause("", ())
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_row_alias_clause(None, ())
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_row_alias_clause(1, ())
        # . columns alias
        self.assertClauseEqual(dml._gen_row_alias_clause("r", ("a",)), "AS r (a)")
        self.assertClauseEqual(
            dml._gen_row_alias_clause("r", ("a", "b")), "AS r (a, b)"
        )
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_row_alias_clause("r", ("",))
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_row_alias_clause("r", (None,))
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_row_alias_clause("r", (1,))
        # . indent
        self.assertClauseEqual(dml._gen_row_alias_clause("r", ("a",)), "\tAS r (a)", 1)
        self.assertClauseEqual(
            dml._gen_row_alias_clause("r", ("a",)), "\t\tAS r (a)", 2
        )

        self.log_ended("ROW ALIAS")

    def test_on_dup_key_update_clause(self) -> None:
        self.log_start("ON DUPLICATE KEY UPDATE")

        dml = self.setupDML()
        # . assignments
        self.assertClauseEqual(
            dml._gen_on_duplicate_clause(("c=VALUES(a)+VALUES(b)",)),
            "ON DUPLICATE KEY UPDATE\n\tc=VALUES(a)+VALUES(b)",
        )
        self.assertClauseEqual(
            dml._gen_on_duplicate_clause(("c=VALUES(a)+VALUES(b)", "f=VALUES(d)")),
            "ON DUPLICATE KEY UPDATE\n\tc=VALUES(a)+VALUES(b),\n\tf=VALUES(d)",
        )
        # . invalid
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_on_duplicate_clause(("",))
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_on_duplicate_clause((None,))
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_on_duplicate_clause((1,))
        # . indent
        # fmt: off
        self.assertClauseEqual(
            dml._gen_on_duplicate_clause(("c=VALUES(a)+VALUES(b)",)),
            "\tON DUPLICATE KEY UPDATE\n\t\tc=VALUES(a)+VALUES(b)", 1
        )
        self.assertClauseEqual(
            dml._gen_on_duplicate_clause(("c=VALUES(a)+VALUES(b)", "f=VALUES(d)")),
            "\tON DUPLICATE KEY UPDATE\n\t\tc=VALUES(a)+VALUES(b),\n\t\tf=VALUES(d)", 1
        )
        self.assertClauseEqual(
            dml._gen_on_duplicate_clause(("c=VALUES(a)+VALUES(b)",)),
            "\t\tON DUPLICATE KEY UPDATE\n\t\t\tc=VALUES(a)+VALUES(b)", 2
        )
        self.assertClauseEqual(
            dml._gen_on_duplicate_clause(("c=VALUES(a)+VALUES(b)", "f=VALUES(d)")),
            "\t\tON DUPLICATE KEY UPDATE\n\t\t\tc=VALUES(a)+VALUES(b),\n\t\t\tf=VALUES(d)", 2
        )
        # fmt: on

        self.log_ended("ON DUPLICATE KEY UPDATE")


class TestInsertStatement(TestCase):
    name: str = "INSERT Statement"

    TEST_DATA1: tuple = (
        (1, "a0", 1.0, datetime.datetime(2012, 1, 1, 0, 0)),
        (2, "a1", 1.1, datetime.datetime(2012, 1, 1, 0, 0)),
        (3, "b0", 2.0, datetime.datetime(2012, 2, 1, 0, 0)),
        (4, "b1", 2.1, datetime.datetime(2012, 2, 1, 0, 0)),
        (5, "c1", 3.0, datetime.datetime(2012, 3, 1, 0, 0)),
        (6, "c2", 3.1, datetime.datetime(2012, 3, 1, 0, 0)),
    )
    TEST_DATA2: tuple = (
        (1, "xa0", 11.0, datetime.datetime(2012, 1, 1, 0, 0)),
        (2, "xa1", 11.1, datetime.datetime(2012, 1, 1, 0, 0)),
        (3, "xb0", 12.0, datetime.datetime(2012, 2, 1, 0, 0)),
        (4, "xb1", 12.1, datetime.datetime(2012, 2, 1, 0, 0)),
        (5, "xc1", 13.0, datetime.datetime(2012, 3, 1, 0, 0)),
        (6, "xc2", 13.1, datetime.datetime(2012, 3, 1, 0, 0)),
    )

    # Test
    async def test_all(self) -> None:
        await self.test_insert_values()
        await self.test_insert_set()
        await self.test_insert_select()
        await self.test_table_insert()

    async def test_insert_values(self) -> None:
        self.log_start("INSERT VALUES")

        from sqlcycli.utils import RE_INSERT_VALUES as RE

        db = self.prepareDatabase()
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = db.Insert(db.tb1).Values(4)
        stmt = dml.statement()
        self.assertEqual(stmt, "INSERT INTO db.tb1\nVALUES (%s,%s,%s,%s)")
        m = RE.match(stmt)
        self.assertIsNotNone(m, "RE match failed")
        self.assertEqual(m.group(3), "")
        self.assertEqual(dml.Execute(self.TEST_DATA1, True), 6)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA1)
        db.tb1.Truncate()
        self.assertEqual(await dml.aioExecute(self.TEST_DATA1, True), 6)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA1)
        db.tb1.Truncate()

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = db.Insert(db.tb1).Columns(db.tb1.id, "name", "price", "dt").Values(4)
        stmt = dml.statement()
        self.assertEqual(
            stmt, "INSERT INTO db.tb1\n\t(id, name, price, dt)\nVALUES (%s,%s,%s,%s)"
        )
        m = RE.match(stmt)
        self.assertIsNotNone(m, "RE match failed")
        self.assertEqual(m.group(3), "")
        self.assertEqual(dml.Execute(self.TEST_DATA1, True), 6)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA1)
        db.tb1.Truncate()
        self.assertEqual(await dml.aioExecute(self.TEST_DATA1, True), 6)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA1)
        db.tb1.Truncate()

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = (
            db.Insert(db.tb1)
            .Columns(db.tb1.id, "name", "price", "dt")
            .Values(4)
            .OnDuplicate("name=VALUES(name)", "price=VALUES(price)")
        )
        stmt = dml.statement()
        self.assertEqual(
            stmt,
            "INSERT INTO db.tb1\n\t"
            "(id, name, price, dt)\n"
            "VALUES (%s,%s,%s,%s)\n"
            "ON DUPLICATE KEY UPDATE\n\t"
            "name=VALUES(name),\n\t"
            "price=VALUES(price)",
        )
        m = RE.match(stmt)
        self.assertIsNotNone(m, "RE match failed")
        self.assertEqual(
            m.group(3),
            "\nON DUPLICATE KEY UPDATE\n\tname=VALUES(name),\n\tprice=VALUES(price)",
        )
        self.assertEqual(dml.Execute(self.TEST_DATA1, True), 6)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA1)
        self.assertEqual(dml.Execute(self.TEST_DATA2, True), 12)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA2)
        db.tb1.Truncate()
        self.assertEqual(await dml.aioExecute(self.TEST_DATA1, True), 6)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA1)
        self.assertEqual(await dml.aioExecute(self.TEST_DATA2, True), 12)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA2)
        db.tb1.Truncate()

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = (
            db.Insert(db.tb1, "from201201", True, "HIGH")
            .Columns(db.tb1.id, "name", "price", "dt")
            .Values(4)
        )
        stmt = dml.statement()
        self.assertEqual(
            stmt,
            "INSERT HIGH_PRIORITY IGNORE INTO db.tb1 PARTITION (from201201)\n\t"
            "(id, name, price, dt)\n"
            "VALUES (%s,%s,%s,%s)",
        )
        m = RE.match(stmt)
        self.assertIsNotNone(m, "RE match failed")
        self.assertEqual(m.group(3), "")
        self.assertEqual(dml.Execute(self.TEST_DATA1, True), 2)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA1[:2])
        db.tb1.Truncate()
        self.assertEqual(await dml.aioExecute(self.TEST_DATA1, True), 2)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA1[:2])
        db.tb1.Truncate()

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = db.Insert(db.tb1).Values(4).RowAlias("new")
        stmt = dml.statement()
        self.assertEqual(stmt, "INSERT INTO db.tb1\nVALUES (%s,%s,%s,%s) AS new")
        m = RE.match(stmt)
        self.assertIsNotNone(m, "RE match failed")
        self.assertEqual(m.group(3), " AS new")
        self.assertEqual(dml.Execute(self.TEST_DATA1, True), 6)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA1)
        db.tb1.Truncate()
        self.assertEqual(await dml.aioExecute(self.TEST_DATA1, True), 6)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA1)
        db.tb1.Truncate()

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = (
            db.Insert(db.tb1)
            .Columns(db.tb1.id, "name", "price", "dt")
            .Values(4)
            .RowAlias("new")
            .OnDuplicate("name=new.name", "price=new.price")
        )
        stmt = dml.statement()
        self.assertEqual(
            stmt,
            "INSERT INTO db.tb1\n\t"
            "(id, name, price, dt)\n"
            "VALUES (%s,%s,%s,%s) AS new\n"
            "ON DUPLICATE KEY UPDATE\n\t"
            "name=new.name,\n\t"
            "price=new.price",
        )
        m = RE.match(stmt)
        self.assertIsNotNone(m, "RE match failed")
        self.assertEqual(
            m.group(3),
            " AS new\nON DUPLICATE KEY UPDATE\n\tname=new.name,\n\tprice=new.price",
        )
        self.assertEqual(dml.Execute(self.TEST_DATA1, True), 6)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA1)
        self.assertEqual(dml.Execute(self.TEST_DATA2, True), 12)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA2)
        db.tb1.Truncate()
        self.assertEqual(await dml.aioExecute(self.TEST_DATA1, True), 6)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA1)
        self.assertEqual(await dml.aioExecute(self.TEST_DATA2, True), 12)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA2)
        db.tb1.Truncate()

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = (
            db.Insert(db.tb1)
            .Columns(db.tb1.id, "name", "price", "dt")
            .Values(4)
            .RowAlias("new", "i", "n", "p", "d")
        )
        stmt = dml.statement()
        self.assertEqual(
            stmt,
            "INSERT INTO db.tb1\n\t(id, name, price, dt)\nVALUES (%s,%s,%s,%s) AS new (i, n, p, d)",
        )
        m = RE.match(stmt)
        self.assertIsNotNone(m, "RE match failed")
        self.assertEqual(m.group(3), " AS new (i, n, p, d)")
        self.assertEqual(dml.Execute(self.TEST_DATA1, True), 6)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA1)
        db.tb1.Truncate()
        self.assertEqual(await dml.aioExecute(self.TEST_DATA1, True), 6)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA1)
        db.tb1.Truncate()

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = (
            db.Insert(db.tb1)
            .Columns(db.tb1.id, "name", "price", "dt")
            .Values(4)
            .RowAlias("new", "i", "n", "p", "d")
            .OnDuplicate("name=n", "price=p")
        )
        stmt = dml.statement()
        self.assertEqual(
            stmt,
            "INSERT INTO db.tb1\n\t"
            "(id, name, price, dt)\n"
            "VALUES (%s,%s,%s,%s) AS new (i, n, p, d)\n"
            "ON DUPLICATE KEY UPDATE\n\t"
            "name=n,\n\t"
            "price=p",
        )
        m = RE.match(stmt)
        self.assertIsNotNone(m, "RE match failed")
        self.assertEqual(
            m.group(3),
            " AS new (i, n, p, d)\nON DUPLICATE KEY UPDATE\n\tname=n,\n\tprice=p",
        )
        self.assertEqual(dml.Execute(self.TEST_DATA1, True), 6)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA1)
        self.assertEqual(dml.Execute(self.TEST_DATA2, True), 12)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA2)
        db.tb1.Truncate()
        self.assertEqual(await dml.aioExecute(self.TEST_DATA1, True), 6)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA1)
        self.assertEqual(await dml.aioExecute(self.TEST_DATA2, True), 12)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA2)
        db.tb1.Truncate()

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        with self.assertRaises(errors.DMLClauseError):
            db.Insert(db.tb1).statement()
        with self.assertRaises(errors.DMLClauseError):
            db.Insert(db.tb1).Columns("id", "name").statement()
        with self.assertRaises(errors.DMLClauseError):
            db.Insert(db.tb1).Where("id = 1").Values(2).statement()

        # Finished
        db.Drop(True)
        self.log_ended("INSERT VALUES")

    async def test_insert_set(self) -> None:
        self.log_start("INSERT SET")

        db = self.prepareDatabase()
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = db.Insert(db.tb1).Set("id=%s", "name=%s", "price=%s", "dt=%s")
        self.assertEqual(
            dml.statement(),
            "INSERT INTO db.tb1\nSET\n\tid=%s,\n\tname=%s,\n\tprice=%s,\n\tdt=%s",
        )
        self.assertEqual(dml.Execute(self.TEST_DATA1, True), 6)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA1)
        db.tb1.Truncate()
        self.assertEqual(await dml.aioExecute(self.TEST_DATA1, True), 6)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA1)
        db.tb1.Truncate()

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = (
            db.Insert(db.tb1)
            .Set("id=%s", "name=%s", "price=%s", "dt=%s")
            .RowAlias("new")
        )
        self.assertEqual(
            dml.statement(),
            "INSERT INTO db.tb1\n"
            "SET\n\tid=%s,\n\tname=%s,\n\tprice=%s,\n\tdt=%s\nAS new",
        )
        self.assertEqual(dml.Execute(self.TEST_DATA1, True), 6)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA1)
        db.tb1.Truncate()
        self.assertEqual(await dml.aioExecute(self.TEST_DATA1, True), 6)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA1)
        db.tb1.Truncate()

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = (
            db.Insert(db.tb1)
            .Set("id=%s", "name=%s", "price=%s", "dt=%s")
            .RowAlias("new")
            .OnDuplicate("name=new.name", "price=new.price")
        )
        self.assertEqual(
            dml.statement(),
            "INSERT INTO db.tb1\n"
            "SET\n\tid=%s,\n\tname=%s,\n\tprice=%s,\n\tdt=%s\nAS new\n"
            "ON DUPLICATE KEY UPDATE\n\tname=new.name,\n\tprice=new.price",
        )
        self.assertEqual(dml.Execute(self.TEST_DATA1, True), 6)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA1)
        self.assertEqual(dml.Execute(self.TEST_DATA2, True), 12)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA2)
        db.tb1.Truncate()
        self.assertEqual(await dml.aioExecute(self.TEST_DATA1, True), 6)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA1)
        self.assertEqual(await dml.aioExecute(self.TEST_DATA2, True), 12)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA2)
        db.tb1.Truncate()

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = (
            db.Insert(db.tb1)
            .Set("id=%s", "name=%s", "price=%s", "dt=%s")
            .RowAlias("new", "i", "n", "p", "d")
        )
        self.assertEqual(
            dml.statement(),
            "INSERT INTO db.tb1\n"
            "SET\n\tid=%s,\n\tname=%s,\n\tprice=%s,\n\tdt=%s\nAS new (i, n, p, d)",
        )
        self.assertEqual(dml.Execute(self.TEST_DATA1, True), 6)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA1)
        db.tb1.Truncate()
        self.assertEqual(await dml.aioExecute(self.TEST_DATA1, True), 6)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA1)
        db.tb1.Truncate()

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = (
            db.Insert(db.tb1)
            .Set("id=%s", "name=%s", "price=%s", "dt=%s")
            .RowAlias("new", "i", "n", "p", "d")
            .OnDuplicate("name=n", "price=p")
        )
        self.assertEqual(
            dml.statement(),
            "INSERT INTO db.tb1\n"
            "SET\n\tid=%s,\n\tname=%s,\n\tprice=%s,\n\tdt=%s\nAS new (i, n, p, d)\n"
            "ON DUPLICATE KEY UPDATE\n\tname=n,\n\tprice=p",
        )
        self.assertEqual(dml.Execute(self.TEST_DATA1, True), 6)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA1)
        self.assertEqual(dml.Execute(self.TEST_DATA2, True), 12)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA2)
        db.tb1.Truncate()
        self.assertEqual(await dml.aioExecute(self.TEST_DATA1, True), 6)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA1)
        self.assertEqual(await dml.aioExecute(self.TEST_DATA2, True), 12)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA2)
        db.tb1.Truncate()

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        with self.assertRaises(errors.DMLClauseError):
            db.Insert(db.tb1).Columns("id").Set("id = 1").statement()

        # Finished
        db.Drop(True)
        self.log_ended("INSERT SET")

    async def test_insert_select(self) -> None:
        self.log_start("INSERT SELECT")

        db = self.prepareDatabase()
        db.Insert(db.tb2).Values(4).Execute(self.TEST_DATA1, True)
        db.Insert(db.tb3).Values(4).Execute(self.TEST_DATA2, True)
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = db.Insert(db.tb1).Select("*").From(db.tb2)
        self.assertEqual(
            dml.statement(),
            "INSERT INTO db.tb1\nSELECT *\nFROM db.tb2 AS t0",
        )
        self.assertEqual(dml.Execute(), 6)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA1)
        db.tb1.Truncate()
        self.assertEqual(await dml.aioExecute(), 6)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA1)
        db.tb1.Truncate()

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = db.Insert(db.tb1).Select("*").From(db.tb2).Where("id=%s")
        self.assertEqual(
            dml.statement(),
            "INSERT INTO db.tb1\nSELECT *\nFROM db.tb2 AS t0\nWHERE id=%s",
        )
        self.assertEqual(dml.Execute(1), 1)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), (self.TEST_DATA1[0],))
        db.tb1.Truncate()
        self.assertEqual(await dml.aioExecute(1), 1)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), (self.TEST_DATA1[0],))
        db.tb1.Truncate()
        self.assertEqual(dml.Execute(([1], [2]), True), 2)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA1[:2])
        db.tb1.Truncate()
        self.assertEqual(await dml.aioExecute(([1], [2]), True), 2)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA1[:2])
        db.tb1.Truncate()

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = db.Insert(db.tb1).Select("t0.*").From(db.tb2).Join(db.tb3, "t0.id=t1.id")
        self.assertEqual(
            dml.statement(),
            "INSERT INTO db.tb1\n"
            "SELECT t0.*\n"
            "FROM db.tb2 AS t0\n"
            "INNER JOIN db.tb3 AS t1\n\t"
            "ON t0.id=t1.id",
        )
        self.assertEqual(dml.Execute(), 6)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA1)
        db.tb1.Truncate()
        self.assertEqual(await dml.aioExecute(), 6)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA1)
        db.tb1.Truncate()

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = (
            db.Insert(db.tb1)
            .Select("*")
            .From(db.tb3)
            .OnDuplicate(
                "name=t0.name",
                "price=t0.price",
            )
        )
        self.assertEqual(
            dml.statement(),
            "INSERT INTO db.tb1\n"
            "SELECT *\n"
            "FROM db.tb3 AS t0\n"
            "ON DUPLICATE KEY UPDATE\n\t"
            "name=t0.name,\n\t"
            "price=t0.price",
        )
        db.Insert(db.tb1).Select("*").From(db.tb2).Execute()
        self.assertEqual(dml.Execute(), 12)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA2)
        db.tb1.Truncate()
        db.Insert(db.tb1).Select("*").From(db.tb2).Execute()
        self.assertEqual(await dml.aioExecute(), 12)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA2)
        db.tb1.Truncate()

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = (
            db.Insert(db.tb1)
            .Columns(db.tb1.columns)
            .Select(db.tb2.columns)
            .From(db.tb2)
        )
        self.assertEqual(
            dml.statement(),
            "INSERT INTO db.tb1\n\t"
            "(id, name, price, dt)\n"
            "SELECT\n\tid,\n\tname,\n\tprice,\n\tdt\n"
            "FROM db.tb2 AS t0",
        )
        self.assertEqual(dml.Execute(), 6)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA1)
        db.tb1.Truncate()
        self.assertEqual(await dml.aioExecute(), 6)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA1)
        db.tb1.Truncate()

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = (
            db.Insert(db.tb1)
            .Columns(db.tb1.columns)
            .Select(db.tb2.columns)
            .From(db.tb2)
            .Where("id=%s")
        )
        self.assertEqual(
            dml.statement(),
            "INSERT INTO db.tb1\n\t"
            "(id, name, price, dt)\n"
            "SELECT\n\tid,\n\tname,\n\tprice,\n\tdt\n"
            "FROM db.tb2 AS t0\n"
            "WHERE id=%s",
        )
        self.assertEqual(dml.Execute(1), 1)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), (self.TEST_DATA1[0],))
        db.tb1.Truncate()
        self.assertEqual(await dml.aioExecute(1), 1)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), (self.TEST_DATA1[0],))
        db.tb1.Truncate()
        self.assertEqual(dml.Execute(([1], [2]), True), 2)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA1[:2])
        db.tb1.Truncate()
        self.assertEqual(await dml.aioExecute(([1], [2]), True), 2)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA1[:2])
        db.tb1.Truncate()

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = (
            db.Insert(db.tb1)
            .Columns(db.tb1.columns)
            .Select("t0.*")
            .From(db.tb2)
            .Join(db.tb3, "t0.id=t1.id")
        )
        self.assertEqual(
            dml.statement(),
            "INSERT INTO db.tb1\n\t"
            "(id, name, price, dt)\n"
            "SELECT t0.*\n"
            "FROM db.tb2 AS t0\n"
            "INNER JOIN db.tb3 AS t1\n\t"
            "ON t0.id=t1.id",
        )
        self.assertEqual(dml.Execute(), 6)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA1)
        db.tb1.Truncate()
        self.assertEqual(await dml.aioExecute(), 6)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA1)
        db.tb1.Truncate()

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = (
            db.Insert(db.tb1)
            .Columns(db.tb1.columns)
            .Select("*")
            .From(db.tb3)
            .OnDuplicate(
                "name=t0.name",
                "price=t0.price",
            )
        )
        self.assertEqual(
            dml.statement(),
            "INSERT INTO db.tb1\n\t"
            "(id, name, price, dt)\n"
            "SELECT *\n"
            "FROM db.tb3 AS t0\n"
            "ON DUPLICATE KEY UPDATE\n\t"
            "name=t0.name,\n\t"
            "price=t0.price",
        )
        db.Insert(db.tb1).Select("*").From(db.tb2).Execute()
        self.assertEqual(dml.Execute(), 12)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA2)
        db.tb1.Truncate()
        db.Insert(db.tb1).Select("*").From(db.tb2).Execute()
        self.assertEqual(await dml.aioExecute(), 12)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA2)
        db.tb1.Truncate()

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = (
            db.Insert(db.tb1)
            .Columns(db.tb1.columns)
            .With("c1", db.Select("*").From(db.tb2))
            .Select("*")
            .From("c1")
        )
        self.assertEqual(
            dml.statement(),
            "INSERT INTO db.tb1\n\t"
            "(id, name, price, dt)\n"
            "WITH c1 AS (\n\t"
            "SELECT *\n\t"
            "FROM db.tb2 AS t0\n"
            ")\n"
            "SELECT *\n"
            "FROM c1 AS t0",
        )
        self.assertEqual(dml.Execute(), 6)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA1)
        db.tb1.Truncate()
        self.assertEqual(await dml.aioExecute(), 6)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA1)
        db.tb1.Truncate()

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        with self.assertRaises(errors.DMLClauseError):
            db.Insert(db.tb1).Where("id = 1").Select("*").statement()
        with self.assertRaises(errors.DMLClauseError):
            db.Insert(db.tb1).Where("id = 1").With("c1", "apple").statement()

        # Finished
        db.Drop(True)
        self.log_ended("INSERT SELECT")

    async def test_table_insert(self) -> None:
        self.log_start("TABLE INSERT")

        db = self.prepareDatabase()
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = db.tb1.Insert().Columns(db.tb1.id, "name", "price", "dt").Values(4)
        self.assertEqual(
            dml.statement(),
            "INSERT INTO db.tb1\n\t(id, name, price, dt)\nVALUES (%s,%s,%s,%s)",
        )
        self.assertEqual(dml.Execute(self.TEST_DATA1, True), 6)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA1)
        db.tb1.Truncate()
        self.assertEqual(await dml.aioExecute(self.TEST_DATA1, True), 6)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA1)
        db.tb1.Truncate()

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = (
            db.tb1.Insert()
            .Set("id=%s", "name=%s", "price=%s", "dt=%s")
            .RowAlias("new", "i", "n", "p", "d")
        )
        self.assertEqual(
            dml.statement(),
            "INSERT INTO db.tb1\n"
            "SET\n\tid=%s,\n\tname=%s,\n\tprice=%s,\n\tdt=%s\nAS new (i, n, p, d)",
        )
        self.assertEqual(dml.Execute(self.TEST_DATA1, True), 6)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA1)
        db.tb1.Truncate()
        self.assertEqual(await dml.aioExecute(self.TEST_DATA1, True), 6)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA1)
        db.tb1.Truncate()

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        db.tb2.Insert().Values(4).Execute(self.TEST_DATA1, True)
        db.tb3.Insert().Values(4).Execute(self.TEST_DATA2, True)
        dml = (
            db.tb1.Insert().Columns(db.tb1.columns).Select(db.tb2.columns).From(db.tb2)
        )
        self.assertEqual(
            dml.statement(),
            "INSERT INTO db.tb1\n\t"
            "(id, name, price, dt)\n"
            "SELECT\n\tid,\n\tname,\n\tprice,\n\tdt\n"
            "FROM db.tb2 AS t0",
        )
        self.assertEqual(dml.Execute(), 6)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA1)
        db.tb1.Truncate()
        self.assertEqual(await dml.aioExecute(), 6)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA1)
        db.tb1.Truncate()

        # Finished
        db.Drop(True)
        self.log_ended("TABLE INSERT")


class TestReplaceStatement(TestCase):
    name: str = "REPLACE Statement"

    TEST_DATA1: tuple = (
        (1, "a0", 1.0, datetime.datetime(2012, 1, 1, 0, 0)),
        (2, "a1", 1.1, datetime.datetime(2012, 1, 1, 0, 0)),
        (3, "b0", 2.0, datetime.datetime(2012, 2, 1, 0, 0)),
        (4, "b1", 2.1, datetime.datetime(2012, 2, 1, 0, 0)),
        (5, "c1", 3.0, datetime.datetime(2012, 3, 1, 0, 0)),
        (6, "c2", 3.1, datetime.datetime(2012, 3, 1, 0, 0)),
    )
    TEST_DATA2: tuple = (
        (1, "xa0", 11.0, datetime.datetime(2012, 1, 1, 0, 0)),
        (2, "xa1", 11.1, datetime.datetime(2012, 1, 1, 0, 0)),
        (3, "xb0", 12.0, datetime.datetime(2012, 2, 1, 0, 0)),
        (4, "xb1", 12.1, datetime.datetime(2012, 2, 1, 0, 0)),
        (5, "xc1", 13.0, datetime.datetime(2012, 3, 1, 0, 0)),
        (6, "xc2", 13.1, datetime.datetime(2012, 3, 1, 0, 0)),
    )

    # Test
    async def test_all(self) -> None:
        await self.test_replace_values()
        await self.test_replace_set()
        await self.test_replace_select()
        await self.test_table_replace()

    async def test_replace_values(self) -> None:
        self.log_start("REPLACE VALUES")

        from sqlcycli.utils import RE_INSERT_VALUES as RE

        db = self.prepareDatabase()
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = db.Replace(db.tb1).Values(4)
        stmt = dml.statement()
        self.assertEqual(stmt, "REPLACE INTO db.tb1\nVALUES (%s,%s,%s,%s)")
        m = RE.match(stmt)
        self.assertIsNotNone(m, "RE match failed")
        self.assertEqual(m.group(3), "")
        self.assertEqual(dml.Execute(self.TEST_DATA1, True), 6)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA1)
        self.assertEqual(dml.Execute(self.TEST_DATA2, True), 12)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA2)
        db.tb1.Truncate()
        self.assertEqual(await dml.aioExecute(self.TEST_DATA1, True), 6)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA1)
        self.assertEqual(await dml.aioExecute(self.TEST_DATA2, True), 12)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA2)
        db.tb1.Truncate()

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = db.Replace(db.tb1).Columns(db.tb1.id, "name", "price", "dt").Values(4)
        stmt = dml.statement()
        self.assertEqual(
            stmt, "REPLACE INTO db.tb1\n\t(id, name, price, dt)\nVALUES (%s,%s,%s,%s)"
        )
        m = RE.match(stmt)
        self.assertIsNotNone(m, "RE match failed")
        self.assertEqual(m.group(3), "")
        self.assertEqual(dml.Execute(self.TEST_DATA1, True), 6)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA1)
        self.assertEqual(dml.Execute(self.TEST_DATA2, True), 12)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA2)
        db.tb1.Truncate()
        self.assertEqual(await dml.aioExecute(self.TEST_DATA1, True), 6)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA1)
        self.assertEqual(await dml.aioExecute(self.TEST_DATA2, True), 12)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA2)
        db.tb1.Truncate()

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = (
            db.Replace(db.tb1, "from201201", True)
            .Columns(db.tb1.id, "name", "price", "dt")
            .Values(4)
        )
        stmt = dml.statement()
        self.assertEqual(
            stmt,
            "REPLACE LOW_PRIORITY INTO db.tb1 PARTITION (from201201)\n\t"
            "(id, name, price, dt)\n"
            "VALUES (%s,%s,%s,%s)",
        )
        m = RE.match(stmt)
        self.assertIsNotNone(m, "RE match failed")
        self.assertEqual(m.group(3), "")
        self.assertEqual(dml.Execute(self.TEST_DATA1[:2], True), 2)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA1[:2])
        self.assertEqual(dml.Execute(self.TEST_DATA2[:2], True), 4)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA2[:2])
        db.tb1.Truncate()
        self.assertEqual(await dml.aioExecute(self.TEST_DATA1[:2], True), 2)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA1[:2])
        self.assertEqual(await dml.aioExecute(self.TEST_DATA2[:2], True), 4)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA2[:2])
        db.tb1.Truncate()

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        with self.assertRaises(errors.DMLClauseError):
            db.Replace(db.tb1).statement()
        with self.assertRaises(errors.DMLClauseError):
            db.Replace(db.tb1).Columns("id", "name").statement()
        with self.assertRaises(errors.DMLClauseError):
            db.Replace(db.tb1).Where("id = 1").Values(2).statement()

        # Finished
        db.Drop(True)
        self.log_ended("REPLACE VALUES")

    async def test_replace_set(self) -> None:
        self.log_start("REPLACE SET")

        db = self.prepareDatabase()
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = db.Replace(db.tb1).Set("id=%s", "name=%s", "price=%s", "dt=%s")
        self.assertEqual(
            dml.statement(),
            "REPLACE INTO db.tb1\nSET\n\tid=%s,\n\tname=%s,\n\tprice=%s,\n\tdt=%s",
        )
        self.assertEqual(dml.Execute(self.TEST_DATA1, True), 6)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA1)
        self.assertEqual(dml.Execute(self.TEST_DATA2, True), 12)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA2)
        db.tb1.Truncate()
        self.assertEqual(await dml.aioExecute(self.TEST_DATA1, True), 6)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA1)
        self.assertEqual(await dml.aioExecute(self.TEST_DATA2, True), 12)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA2)
        db.tb1.Truncate()

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        with self.assertRaises(errors.DMLClauseError):
            db.Replace(db.tb1).Columns("id").Set("id = 1").statement()

        # Finished
        db.Drop(True)
        self.log_ended("REPLACE SET")

    async def test_replace_select(self) -> None:
        self.log_start("REPLACE SELECT")

        db = self.prepareDatabase()
        db.Insert(db.tb2).Values(4).Execute(self.TEST_DATA1, True)
        db.Insert(db.tb3).Values(4).Execute(self.TEST_DATA2, True)
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = db.Replace(db.tb1).Select("*").From(db.tb2)
        dml2 = db.Replace(db.tb1).Select("*").From(db.tb3)
        self.assertEqual(
            dml.statement(),
            "REPLACE INTO db.tb1\nSELECT *\nFROM db.tb2 AS t0",
        )
        self.assertEqual(dml.Execute(), 6)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA1)
        self.assertEqual(dml2.Execute(), 12)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA2)
        db.tb1.Truncate()
        self.assertEqual(await dml.aioExecute(), 6)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA1)
        self.assertEqual(await dml2.aioExecute(), 12)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA2)
        db.tb1.Truncate()

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = db.Replace(db.tb1).Select("*").From(db.tb2).Where("id=%s")
        dml2 = db.Replace(db.tb1).Select("*").From(db.tb3).Where("id=%s")
        self.assertEqual(
            dml.statement(),
            "REPLACE INTO db.tb1\nSELECT *\nFROM db.tb2 AS t0\nWHERE id=%s",
        )
        self.assertEqual(dml.Execute(1), 1)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), (self.TEST_DATA1[0],))
        self.assertEqual(dml2.Execute(1), 2)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), (self.TEST_DATA2[0],))
        db.tb1.Truncate()
        self.assertEqual(await dml.aioExecute(1), 1)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), (self.TEST_DATA1[0],))
        self.assertEqual(await dml2.aioExecute(1), 2)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), (self.TEST_DATA2[0],))
        db.tb1.Truncate()

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = db.Replace(db.tb1).Select("t0.*").From(db.tb2).Join(db.tb3, "t0.id=t1.id")
        dml2 = (
            db.Replace(db.tb1).Select("t0.*").From(db.tb3).Join(db.tb3, "t0.id=t1.id")
        )
        self.assertEqual(
            dml.statement(),
            "REPLACE INTO db.tb1\n"
            "SELECT t0.*\n"
            "FROM db.tb2 AS t0\n"
            "INNER JOIN db.tb3 AS t1\n\t"
            "ON t0.id=t1.id",
        )
        self.assertEqual(dml.Execute(), 6)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA1)
        self.assertEqual(dml2.Execute(), 12)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA2)
        db.tb1.Truncate()
        self.assertEqual(await dml.aioExecute(), 6)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA1)
        self.assertEqual(await dml2.aioExecute(), 12)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA2)
        db.tb1.Truncate()

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = (
            db.Replace(db.tb1)
            .Columns(db.tb1.columns)
            .Select(db.tb2.columns)
            .From(db.tb2)
        )
        dml2 = (
            db.Replace(db.tb1)
            .Columns(db.tb1.columns)
            .Select(db.tb3.columns)
            .From(db.tb3)
        )
        self.assertEqual(
            dml.statement(),
            "REPLACE INTO db.tb1\n\t"
            "(id, name, price, dt)\n"
            "SELECT\n\tid,\n\tname,\n\tprice,\n\tdt\n"
            "FROM db.tb2 AS t0",
        )
        self.assertEqual(dml.Execute(), 6)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA1)
        self.assertEqual(dml2.Execute(), 12)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA2)
        db.tb1.Truncate()
        self.assertEqual(await dml.aioExecute(), 6)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA1)
        self.assertEqual(await dml2.aioExecute(), 12)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA2)
        db.tb1.Truncate()

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = (
            db.Replace(db.tb1)
            .Columns(db.tb1.columns)
            .With("c1", db.Select("*").From(db.tb2))
            .Select("*")
            .From("c1")
        )
        self.assertEqual(
            dml.statement(),
            "REPLACE INTO db.tb1\n\t"
            "(id, name, price, dt)\n"
            "WITH c1 AS (\n\t"
            "SELECT *\n\t"
            "FROM db.tb2 AS t0\n"
            ")\n"
            "SELECT *\n"
            "FROM c1 AS t0",
        )
        self.assertEqual(dml.Execute(), 6)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA1)
        db.tb1.Truncate()
        self.assertEqual(await dml.aioExecute(), 6)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA1)
        db.tb1.Truncate()

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        with self.assertRaises(errors.DMLClauseError):
            db.Replace(db.tb1).Where("id = 1").Select("*").statement()
        with self.assertRaises(errors.DMLClauseError):
            db.Replace(db.tb1).Where("id = 1").With("c1", "apple").statement()

        # Finished
        db.Drop(True)
        self.log_ended("REPLACE SELECT")

    async def test_table_replace(self) -> None:
        self.log_start("TABLE REPLACE")

        db = self.prepareDatabase()
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = db.tb1.Replace().Columns(db.tb1.id, "name", "price", "dt").Values(4)
        self.assertEqual(
            dml.statement(),
            "REPLACE INTO db.tb1\n\t(id, name, price, dt)\nVALUES (%s,%s,%s,%s)",
        )
        self.assertEqual(dml.Execute(self.TEST_DATA1, True), 6)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA1)
        self.assertEqual(dml.Execute(self.TEST_DATA2, True), 12)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA2)
        db.tb1.Truncate()
        self.assertEqual(await dml.aioExecute(self.TEST_DATA1, True), 6)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA1)
        self.assertEqual(await dml.aioExecute(self.TEST_DATA2, True), 12)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA2)
        db.tb1.Truncate()

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = db.tb1.Replace().Set("id=%s", "name=%s", "price=%s", "dt=%s")
        self.assertEqual(
            dml.statement(),
            "REPLACE INTO db.tb1\nSET\n\tid=%s,\n\tname=%s,\n\tprice=%s,\n\tdt=%s",
        )
        self.assertEqual(dml.Execute(self.TEST_DATA1, True), 6)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA1)
        self.assertEqual(dml.Execute(self.TEST_DATA2, True), 12)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA2)
        db.tb1.Truncate()
        self.assertEqual(await dml.aioExecute(self.TEST_DATA1, True), 6)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA1)
        self.assertEqual(await dml.aioExecute(self.TEST_DATA2, True), 12)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA2)
        db.tb1.Truncate()

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        db.tb2.Insert().Values(4).Execute(self.TEST_DATA1, True)
        db.tb3.Insert().Values(4).Execute(self.TEST_DATA2, True)
        dml = db.tb1.Replace().Select("*").From(db.tb2)
        dml2 = db.tb1.Replace().Select("*").From(db.tb3)
        self.assertEqual(
            dml.statement(),
            "REPLACE INTO db.tb1\nSELECT *\nFROM db.tb2 AS t0",
        )
        self.assertEqual(dml.Execute(), 6)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA1)
        self.assertEqual(dml2.Execute(), 12)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA2)
        db.tb1.Truncate()
        self.assertEqual(await dml.aioExecute(), 6)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA1)
        self.assertEqual(await dml2.aioExecute(), 12)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA2)
        db.tb1.Truncate()

        # Finished
        db.Drop(True)
        self.log_ended("TABLE REPLACE")


class TestUpdateClause(TestCase):
    name: str = "UPDATE Clause"

    # Utils
    def setupDML(self) -> InsertDML:
        return UpdateDML("db", self.get_pool())

    # Test
    def test_all(self) -> None:
        self.test_update_clause()

    def test_update_clause(self) -> None:
        self.log_start("UPDATE Clause")

        db = TestDatabase("db", self.get_pool())
        dml = self.setupDML()

        # . table: str
        self.assertClauseEqual(dml._gen_update_clause("tb"), "UPDATE tb AS t0")
        self.assertClauseEqual(dml._gen_update_clause("db.tb"), "UPDATE db.tb AS t1")
        self.assertClauseEqual(
            dml._gen_update_clause("db.tb", alias="t"), "UPDATE db.tb AS t"
        )
        # . table: Table
        self.assertClauseEqual(dml._gen_update_clause(db.tb1), "UPDATE db.tb1 AS t2")
        # . table: invalid
        with self.assertRaises(errors.DMLClauseError):
            dml._gen_update_clause("")
        with self.assertRaises(errors.DMLClauseError):
            dml._gen_update_clause(None)
        with self.assertRaises(errors.DMLClauseError):
            dml._gen_update_clause(1)
        # . partition
        self.assertClauseEqual(
            dml._gen_update_clause("tb", "p0", alias="t"),
            "UPDATE tb PARTITION (p0) AS t",
        )
        self.assertClauseEqual(
            dml._gen_update_clause("tb", ["p0", "p1"], alias="t"),
            "UPDATE tb PARTITION (p0, p1) AS t",
        )
        with self.assertRaises(errors.DMLClauseError):
            dml._gen_update_clause("tb", 1)
        with self.assertRaises(errors.DMLClauseError):
            dml._gen_update_clause("tb", "")
        with self.assertRaises(errors.DMLClauseError):
            dml._gen_update_clause("tb", [None])
        with self.assertRaises(errors.DMLClauseError):
            dml._gen_update_clause("tb", [""])
        with self.assertRaises(errors.DMLClauseError):
            dml._gen_update_clause("tb", [1])
        # . ignore
        self.assertClauseEqual(
            dml._gen_update_clause("tb", ignore=True, alias="t"),
            "UPDATE IGNORE tb AS t",
        )
        # . low_priority
        self.assertClauseEqual(
            dml._gen_update_clause("tb", low_priority=True, alias="t"),
            "UPDATE LOW_PRIORITY tb AS t",
        )
        # . mixed
        self.assertClauseEqual(
            dml._gen_update_clause("tb", ["p0", "p1"], True, True, "t"),
            "UPDATE LOW_PRIORITY IGNORE tb PARTITION (p0, p1) AS t",
        )
        # . indent
        # fmt: off
        self.assertClauseEqual(
            dml._gen_update_clause("tb", ["p0", "p1"], True, True, "t"),
            "\tUPDATE LOW_PRIORITY IGNORE tb PARTITION (p0, p1) AS t", 1
        )
        self.assertClauseEqual(
            dml._gen_update_clause("tb", ["p0", "p1"], True, True, "t"),
            "\t\tUPDATE LOW_PRIORITY IGNORE tb PARTITION (p0, p1) AS t", 2
        )
        # fmt: on

        self.log_ended("UPDATE Clause")


class TestUpdateStatement(TestCase):
    name: str = "UPDATE Statement"
    TEST_DATA1: tuple = (
        (1, "a0", 1.0, datetime.datetime(2012, 1, 1, 0, 0)),
        (2, "a1", 1.1, datetime.datetime(2012, 1, 1, 0, 0)),
        (3, "b0", 2.0, datetime.datetime(2012, 2, 1, 0, 0)),
        (4, "b1", 2.1, datetime.datetime(2012, 2, 1, 0, 0)),
        (5, "c1", 3.0, datetime.datetime(2012, 3, 1, 0, 0)),
        (6, "c2", 3.1, datetime.datetime(2012, 3, 1, 0, 0)),
    )
    TEST_DATA2: tuple = (
        (1, "xa0", 11.0, datetime.datetime(2012, 1, 1, 0, 0)),
        (2, "xa1", 11.1, datetime.datetime(2012, 1, 1, 0, 0)),
        (3, "xb0", 12.0, datetime.datetime(2012, 2, 1, 0, 0)),
        (4, "xb1", 12.1, datetime.datetime(2012, 2, 1, 0, 0)),
        (5, "xc1", 13.0, datetime.datetime(2012, 3, 1, 0, 0)),
        (6, "xc2", 13.1, datetime.datetime(2012, 3, 1, 0, 0)),
    )
    UPDATE_DATA1: tuple = list((i[1], i[2], i[0]) for i in TEST_DATA1)
    UPDATE_DATA2: tuple = list((i[1], i[2], i[0]) for i in TEST_DATA2)

    def prepareDatabase(self) -> TestDatabase:
        db = super().prepareDatabase()
        with db.acquire() as conn:
            with conn.transaction() as cur:
                cur.executemany(
                    f"INSERT INTO {db.tb1} (id, name, price, dt) VALUES (%s, %s, %s, %s)",
                    self.TEST_DATA1,
                )
                cur.executemany(
                    f"INSERT INTO {db.tb2} (id, name, price, dt) VALUES (%s, %s, %s, %s)",
                    self.TEST_DATA2,
                )
                cur.executemany(
                    f"INSERT INTO {db.tb3} (id, name, price, dt) VALUES (%s, %s, %s, %s)",
                    self.TEST_DATA1,
                )
        return db

    # Test
    async def test_all(self) -> None:
        await self.test_update_statement()
        await self.test_update_join_statement()
        await self.test_table_update()

    async def test_update_statement(self) -> None:
        self.log_start("UPDATE")

        db = self.prepareDatabase()
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = db.Update(db.tb1).Set("name=%s")
        self.assertEqual(dml.statement(), "UPDATE db.tb1 AS t0\nSET name=%s")
        self.assertEqual(dml.Execute("n1"), 6)
        self.assertEqual(
            db.Select("name", distinct=True).From(db.tb1).Execute(), (("n1",),)
        )
        self.assertEqual(await dml.aioExecute("n2"), 6)
        self.assertEqual(
            db.Select("name", distinct=True).From(db.tb1).Execute(), (("n2",),)
        )

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = db.Update(db.tb1).Set("name=%s", "price=%s")
        self.assertEqual(
            dml.statement(), "UPDATE db.tb1 AS t0\nSET\n\tname=%s,\n\tprice=%s"
        )
        self.assertEqual(dml.Execute(["m1", 10.0]), 6)
        self.assertEqual(
            db.Select("name", "price", distinct=True).From(db.tb1).Execute(),
            (("m1", 10.0),),
        )
        self.assertEqual(await dml.aioExecute(["m2", 20.0]), 6)
        self.assertEqual(
            db.Select("name", "price", distinct=True).From(db.tb1).Execute(),
            (("m2", 20.0),),
        )

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = db.Update(db.tb1).Set("name=%s", "price=%s").Where("id=%s")
        self.assertEqual(
            dml.statement(),
            "UPDATE db.tb1 AS t0\nSET\n\tname=%s,\n\tprice=%s\nWHERE id=%s",
        )
        self.assertEqual(dml.Execute(["o1", 30.0, 1]), 1)
        self.assertEqual(
            db.Select("name", "price").From(db.tb1).Where("id=1").Execute(),
            (("o1", 30.0),),
        )
        self.assertEqual(await dml.aioExecute(["p1", 40.0, 1]), 1)
        self.assertEqual(
            db.Select("name", "price").From(db.tb1).Where("id=1").Execute(),
            (("p1", 40.0),),
        )
        self.assertEqual(dml.Execute(self.UPDATE_DATA2, True), 6)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA2)
        self.assertEqual(await dml.aioExecute(self.UPDATE_DATA1, True), 6)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA1)

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = (
            db.Update(db.tb1)
            .Set("name=%s", "price=%s")
            .Where("id=%s")
            .OrderBy("id DESC")
        )
        self.assertEqual(
            dml.statement(),
            "UPDATE db.tb1 AS t0\n"
            "SET\n\tname=%s,\n\tprice=%s\n"
            "WHERE id=%s\n"
            "ORDER BY id DESC",
        )
        self.assertEqual(dml.Execute(self.UPDATE_DATA2, True), 6)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA2)
        self.assertEqual(await dml.aioExecute(self.UPDATE_DATA1, True), 6)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA1)

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = db.Update(db.tb1).Set("name=%s", "price=%s").OrderBy("id DESC").Limit(1)
        self.assertEqual(
            dml.statement(),
            "UPDATE db.tb1 AS t0\n"
            "SET\n\tname=%s,\n\tprice=%s\n"
            "ORDER BY id DESC\n"
            "LIMIT 1",
        )
        self.assertEqual(dml.Execute(["n1", 10.0]), 1)
        self.assertEqual(
            db.Select("*").From(db.tb1).Limit(5).Execute(), self.TEST_DATA1[:5]
        )
        self.assertEqual(
            db.Select("name", "price").From(db.tb1).Where("id=6").Execute(),
            (("n1", 10.0),),
        )
        self.assertEqual(await dml.aioExecute(["m1", 20.0]), 1)
        self.assertEqual(
            db.Select("*").From(db.tb1).Limit(5).Execute(), self.TEST_DATA1[:5]
        )
        self.assertEqual(
            db.Select("name", "price").From(db.tb1).Where("id=6").Execute(),
            (("m1", 20.0),),
        )

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = (
            db.Update(db.tb1, "from201201", True, True)
            .Set("name=%s", "price=%s")
            .Where("id=%s")
        )
        self.assertEqual(
            dml.statement(),
            "UPDATE LOW_PRIORITY IGNORE db.tb1 PARTITION (from201201) AS t0\n"
            "SET\n\tname=%s,\n\tprice=%s\n"
            "WHERE id=%s",
        )
        self.assertEqual(dml.Execute(self.UPDATE_DATA2, True), 2)
        self.assertEqual(
            db.Select("*").From(db.tb1, "from201201").Execute(), self.TEST_DATA2[:2]
        )
        self.assertEqual(await dml.aioExecute(self.UPDATE_DATA1, True), 2)
        self.assertEqual(
            db.Select("*").From(db.tb1, "from201201").Execute(), self.TEST_DATA1[:2]
        )
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = (
            db.Update(db.tb1)
            .UseIndex(db.tb1.idx1)
            .Set("name=%s", "price=%s")
            .Where("id=%s")
        )
        self.assertEqual(
            dml.statement(),
            "UPDATE db.tb1 AS t0\n\tUSE INDEX (idx1)\n"
            "SET\n\tname=%s,\n\tprice=%s\n"
            "WHERE id=%s",
        )
        self.assertEqual(dml.Execute(self.UPDATE_DATA2, True), 6)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA2)
        self.assertEqual(dml.Execute(self.UPDATE_DATA1, True), 6)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA1)

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        with self.assertRaises(errors.DMLClauseError):
            db.Update(db.tb1).Where("id=1").statement()
        with self.assertRaises(errors.DMLClauseError):
            db.Update(db.tb1).Where("id=1").Set("name=%s").statement()
        with self.assertRaises(errors.DMLClauseError):
            db.Update(db.tb1).Where("id=1").UseIndex("idx1").statement()

        # Finished
        db.Drop(True)
        self.log_ended("UPDATE")

    async def test_update_join_statement(self) -> None:
        self.log_start("UPDATE JOIN")

        db = self.prepareDatabase()
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = (
            db.Update(db.tb1)
            .Join(db.tb2, using="id")
            .Set("t0.name=t1.name", "t0.price=t1.price")
        )
        self.assertEqual(
            dml.statement(),
            "UPDATE db.tb1 AS t0\n"
            "INNER JOIN db.tb2 AS t1\n\tUSING (id)\n"
            "SET\n\tt0.name=t1.name,\n\tt0.price=t1.price",
        )
        self.assertEqual(dml.Execute(), 6)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA2)
        self.assertEqual(await dml.aioExecute(), 0)

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = (
            db.Update(db.tb1)
            .Join(db.tb3, "t0.id=t1.id")
            .Set("t0.name=t1.name", "t0.price=t1.price")
        )
        self.assertEqual(
            dml.statement(),
            "UPDATE db.tb1 AS t0\n"
            "INNER JOIN db.tb3 AS t1\n\tON t0.id=t1.id\n"
            "SET\n\tt0.name=t1.name,\n\tt0.price=t1.price",
        )
        self.assertEqual(dml.Execute(), 6)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA1)
        self.assertEqual(await dml.aioExecute(), 0)

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = (
            db.Update(db.tb1)
            .Join(db.tb2, using="id")
            .Join(db.tb3, using="id")
            .Set("t0.name=t1.name", "t0.price=t2.price")
            .Where("t0.id=1")
        )
        self.assertEqual(
            dml.statement(),
            "UPDATE db.tb1 AS t0\n"
            "INNER JOIN db.tb2 AS t1\n\tUSING (id)\n"
            "INNER JOIN db.tb3 AS t2\n\tUSING (id)\n"
            "SET\n\tt0.name=t1.name,\n\tt0.price=t2.price\n"
            "WHERE t0.id=1",
        )
        self.assertEqual(await dml.aioExecute(), 1)
        self.assertEqual(
            db.Select("name", "price").From(db.tb1).Where("id=1").Execute(),
            (("xa0", 1.0),),
        )
        self.assertEqual(dml.Execute(), 0)

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = (
            db.Update(db.tb1)
            .Join(db.tb2, using="id")
            .Join(db.tb3, using="id")
            .Set("t0.name=t2.name", "t0.price=t1.price")
            .Where("t0.id=1")
        )
        self.assertEqual(
            dml.statement(),
            "UPDATE db.tb1 AS t0\n"
            "INNER JOIN db.tb2 AS t1\n\tUSING (id)\n"
            "INNER JOIN db.tb3 AS t2\n\tUSING (id)\n"
            "SET\n\tt0.name=t2.name,\n\tt0.price=t1.price\n"
            "WHERE t0.id=1",
        )
        self.assertEqual(await dml.aioExecute(), 1)
        self.assertEqual(
            db.Select("name", "price").From(db.tb1).Where("id=1").Execute(),
            (("a0", 11.0),),
        )
        self.assertEqual(dml.Execute(), 0)

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = (
            db.Update(db.tb1)
            .UseIndex(db.tb1.idx1)
            .Join(db.tb2, using="id")
            .UseIndex(db.tb2.idx1)
            .Set("t0.name=t1.name", "t0.price=t1.price")
        )
        self.assertEqual(
            dml.statement(),
            "UPDATE db.tb1 AS t0\n\tUSE INDEX (idx1)\n"
            "INNER JOIN db.tb2 AS t1\n\tUSE INDEX (idx1)\n\tUSING (id)\n"
            "SET\n\tt0.name=t1.name,\n\tt0.price=t1.price",
        )
        self.assertEqual(dml.Execute(), 6)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA2)

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = (
            db.Update(db.tb1)
            .UseIndex(db.tb1.idx1)
            .Join(db.tb3, using="id")
            .UseIndex(db.tb3.idx1)
            .Set("t0.name=t1.name", "t0.price=t1.price")
        )
        self.assertEqual(
            dml.statement(),
            "UPDATE db.tb1 AS t0\n\tUSE INDEX (idx1)\n"
            "INNER JOIN db.tb3 AS t1\n\tUSE INDEX (idx1)\n\tUSING (id)\n"
            "SET\n\tt0.name=t1.name,\n\tt0.price=t1.price",
        )
        self.assertEqual(await dml.aioExecute(), 6)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA1)

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        with self.assertRaises(errors.DMLClauseError):
            db.Update(db.tb1).Where("id=1").Join(db.tb1).statement()

        # Finished
        db.Drop(True)
        self.log_ended("UPDATE JOIN")

    async def test_table_update(self) -> None:
        self.log_start("TABLE UPDATE")

        db = self.prepareDatabase()
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = (
            db.tb1.Update()
            .Join(db.tb2, using="id")
            .Set("t0.name=t1.name", "t0.price=t1.price")
        )
        self.assertEqual(
            dml.statement(),
            "UPDATE db.tb1 AS t0\n"
            "INNER JOIN db.tb2 AS t1\n\tUSING (id)\n"
            "SET\n\tt0.name=t1.name,\n\tt0.price=t1.price",
        )
        self.assertEqual(dml.Execute(), 6)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA2)
        self.assertEqual(await dml.aioExecute(), 0)

        # Finished
        db.Drop(True)
        self.log_ended("TABLE UPDATE")


class TestDeleteClause(TestCase):
    name: str = "DELETE Clause"

    # Utils
    def setupDML(self) -> InsertDML:
        return DeleteDML("db", self.get_pool())

    # Test
    def test_all(self) -> None:
        self.test_delete_clause()

    def test_delete_clause(self) -> None:
        self.log_start("DELETE Clause")

        db = TestDatabase("db", self.get_pool())
        dml = self.setupDML()

        # . table: str
        self.assertClauseEqual(dml._gen_delete_clause("tb"), "DELETE FROM tb AS t0")
        self.assertClauseEqual(
            dml._gen_delete_clause("db.tb"), "DELETE FROM db.tb AS t1"
        )
        self.assertClauseEqual(
            dml._gen_delete_clause("db.tb", alias="t"), "DELETE FROM db.tb AS t"
        )
        # . table: Table
        self.assertClauseEqual(
            dml._gen_delete_clause(db.tb1), "DELETE FROM db.tb1 AS t2"
        )
        # . table: invalid
        with self.assertRaises(errors.DMLClauseError):
            dml._gen_delete_clause("")
        with self.assertRaises(errors.DMLClauseError):
            dml._gen_delete_clause(None)
        with self.assertRaises(errors.DMLClauseError):
            dml._gen_delete_clause(1)
        # . partition (single-table)
        self.assertClauseEqual(
            dml._gen_delete_clause(db.tb1, "p0", alias="t"),
            "DELETE FROM db.tb1 AS t PARTITION (p0)",
        )
        self.assertClauseEqual(
            dml._gen_delete_clause(db.tb1, ["p0", "p1"], alias="t"),
            "DELETE FROM db.tb1 AS t PARTITION (p0, p1)",
        )
        # . partition (multi-table)
        self.assertClauseEqual(
            dml._gen_delete_clause(db.tb1, "p0", alias="t", multi_tables="t"),
            "DELETE t FROM db.tb1 PARTITION (p0) AS t",
        )
        self.assertClauseEqual(
            dml._gen_delete_clause(
                db.tb1, ["p0", "p1"], alias="t0", multi_tables=["t0", "t1"]
            ),
            "DELETE t0, t1 FROM db.tb1 PARTITION (p0, p1) AS t0",
        )
        with self.assertRaises(errors.DMLClauseError):
            dml._gen_delete_clause(db.tb1, "p0", alias="t", multi_tables="")
        with self.assertRaises(errors.DMLClauseError):
            dml._gen_delete_clause(db.tb1, "p0", alias="t", multi_tables=1)
        with self.assertRaises(errors.DMLClauseError):
            dml._gen_delete_clause(db.tb1, "p0", alias="t", multi_tables=[None])
        with self.assertRaises(errors.DMLClauseError):
            dml._gen_delete_clause(db.tb1, "p0", alias="t", multi_tables=[""])
        with self.assertRaises(errors.DMLClauseError):
            dml._gen_delete_clause(db.tb1, "p0", alias="t", multi_tables=[1])
        # . ignore
        self.assertClauseEqual(
            dml._gen_delete_clause(db.tb1, ignore=True, alias="t"),
            "DELETE IGNORE FROM db.tb1 AS t",
        )
        # . low_priority
        self.assertClauseEqual(
            dml._gen_delete_clause(db.tb1, low_priority=True, alias="t"),
            "DELETE LOW_PRIORITY FROM db.tb1 AS t",
        )
        # . quick
        self.assertClauseEqual(
            dml._gen_delete_clause(db.tb1, quick=True, alias="t"),
            "DELETE QUICK FROM db.tb1 AS t",
        )
        # . mixed (single-table)
        self.assertClauseEqual(
            dml._gen_delete_clause(db.tb1, ["p0", "p1"], True, True, True, "t"),
            "DELETE LOW_PRIORITY QUICK IGNORE FROM db.tb1 AS t PARTITION (p0, p1)",
        )
        # . mixed (multi-table)
        self.assertClauseEqual(
            dml._gen_delete_clause(
                db.tb1, ["p0", "p1"], True, True, True, "t", ["t", "t1"]
            ),
            "DELETE LOW_PRIORITY QUICK IGNORE t, t1 FROM db.tb1 PARTITION (p0, p1) AS t",
        )
        # . indent
        # fmt: off
        self.assertClauseEqual(
            dml._gen_delete_clause(
                db.tb1, ["p0", "p1"], True, True, True, "t", ["t", "t1"]
            ),
            "\tDELETE LOW_PRIORITY QUICK IGNORE t, t1 FROM db.tb1 PARTITION (p0, p1) AS t", 1
        )
        self.assertClauseEqual(
            dml._gen_delete_clause(
                db.tb1, ["p0", "p1"], True, True, True, "t", ["t", "t1"]
            ),
            "\t\tDELETE LOW_PRIORITY QUICK IGNORE t, t1 FROM db.tb1 PARTITION (p0, p1) AS t", 2
        )
        # fmt: on

        self.log_ended("DELETE Clause")


class TestDeleteStatement(TestCase):
    name: str = "DELETE statement"

    TEST_DATA: tuple = (
        (1, "a0", 1.0, datetime.datetime(2012, 1, 1, 0, 0)),
        (2, "a1", 1.1, datetime.datetime(2012, 1, 1, 0, 0)),
        (3, "b0", 2.0, datetime.datetime(2012, 2, 1, 0, 0)),
        (4, "b1", 2.1, datetime.datetime(2012, 2, 1, 0, 0)),
        (5, "c1", 3.0, datetime.datetime(2012, 3, 1, 0, 0)),
        (6, "c2", 3.1, datetime.datetime(2012, 3, 1, 0, 0)),
    )

    def prepareDatabase(self) -> TestDatabase:
        db = super().prepareDatabase()
        with db.acquire() as conn:
            with conn.transaction() as cur:
                cur.executemany(
                    f"INSERT INTO {db.tb1} (id, name, price, dt) VALUES (%s, %s, %s, %s)",
                    self.TEST_DATA,
                )
                cur.executemany(
                    f"INSERT INTO {db.tb2} (id, name, price, dt) VALUES (%s, %s, %s, %s)",
                    self.TEST_DATA,
                )
                cur.executemany(
                    f"INSERT INTO {db.tb3} (id, name, price, dt) VALUES (%s, %s, %s, %s)",
                    self.TEST_DATA,
                )
        return db

    # Test
    async def test_all(self) -> None:
        await self.test_delete_sinlge_table_statement()
        await self.test_delete_multi_table_statement()
        await self.test_table_delete()

    async def test_delete_sinlge_table_statement(self) -> None:
        self.log_start("DELETE single-table")

        db = self.prepareDatabase()
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = db.Delete(db.tb1).Where("id=%s")
        self.assertEqual(dml.statement(), "DELETE FROM db.tb1 AS t0\nWHERE id=%s")
        self.assertEqual(dml.Execute(1), 1)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA[1:])
        self.assertEqual(await dml.aioExecute(2), 1)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA[2:])

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = db.Delete(db.tb1, "from201202").Where("id=%s")
        self.assertEqual(
            dml.statement(),
            "DELETE FROM db.tb1 AS t0 PARTITION (from201202)\nWHERE id=%s",
        )
        self.assertEqual(dml.Execute(3), 1)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA[3:])
        self.assertEqual(await dml.aioExecute(4), 1)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA[4:])

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = db.Delete(db.tb1, None, True, True, True, "t").OrderBy("id DESC").Limit(1)
        self.assertEqual(
            dml.statement(),
            "DELETE LOW_PRIORITY QUICK IGNORE FROM db.tb1 AS t\n"
            "ORDER BY id DESC\n"
            "LIMIT 1",
        )
        self.assertEqual(dml.Execute(), 1)
        self.assertEqual(
            db.Select("*").From(db.tb1).Limit(5).Execute(), self.TEST_DATA[4:5]
        )
        self.assertEqual(await dml.aioExecute(), 1)
        self.assertEqual(db.Select("*").From(db.tb1).Limit(5).Execute(), ())

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = db.Delete(db.tb2).UseIndex(db.tb2.idx1).Where("id=%s")
        self.assertEqual(dml.statement(), "DELETE FROM db.tb2 AS t0\nWHERE id=%s")
        self.assertEqual(dml.Execute(1), 1)
        self.assertEqual(db.Select("*").From(db.tb2).Execute(), self.TEST_DATA[1:])
        self.assertEqual(await dml.aioExecute(2), 1)
        self.assertEqual(db.Select("*").From(db.tb2).Execute(), self.TEST_DATA[2:])

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = db.Delete(db.tb2)
        self.assertEqual(dml.statement(), "DELETE FROM db.tb2 AS t0")
        self.assertEqual(dml.Execute(), 4)
        dml = db.Delete(db.tb3)
        self.assertEqual(await dml.aioExecute(), 6)

        # Finished
        db.Drop(True)
        self.log_ended("DELETE single-table")

    async def test_delete_multi_table_statement(self) -> None:
        self.log_start("DELETE multi-table")

        db = self.prepareDatabase()
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = (
            db.Delete(db.tb1, multi_tables="t0")
            .Join(db.tb2, "t0.id=t1.id")
            .Where("t1.id=%s")
        )
        self.assertEqual(
            dml.statement(),
            "DELETE t0 FROM db.tb1 AS t0\n"
            "INNER JOIN db.tb2 AS t1\n\t"
            "ON t0.id=t1.id\n"
            "WHERE t1.id=%s",
        )
        self.assertEqual(dml.Execute(1), 1)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA[1:])
        self.assertEqual(await dml.aioExecute(2), 1)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA[2:])
        self.assertEqual(dml.Execute([3, 4], many=True), 2)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA[4:])
        self.assertEqual(await dml.aioExecute([5, 6], many=True), 2)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), ())

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = (
            db.Delete(db.tb2, multi_tables=["t0", "t1"])
            .Join(db.tb3, "t0.id=t1.id")
            .Where("t0.id=%s")
        )
        self.assertEqual(
            dml.statement(),
            "DELETE t0, t1 FROM db.tb2 AS t0\n"
            "INNER JOIN db.tb3 AS t1\n\t"
            "ON t0.id=t1.id\n"
            "WHERE t0.id=%s",
        )
        self.assertEqual(dml.Execute(1), 2)
        self.assertEqual(db.Select("*").From(db.tb2).Execute(), self.TEST_DATA[1:])
        self.assertEqual(db.Select("*").From(db.tb3).Execute(), self.TEST_DATA[1:])
        self.assertEqual(await dml.aioExecute(2), 2)
        self.assertEqual(db.Select("*").From(db.tb2).Execute(), self.TEST_DATA[2:])
        self.assertEqual(db.Select("*").From(db.tb3).Execute(), self.TEST_DATA[2:])

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = (
            db.Delete(db.tb2, "from201202", multi_tables=["t0", "t1"])
            .Join(db.tb3, "t0.id=t1.id")
            .Where("t0.id=%s")
        )
        self.assertEqual(
            dml.statement(),
            "DELETE t0, t1 FROM db.tb2 PARTITION (from201202) AS t0\n"
            "INNER JOIN db.tb3 AS t1\n\t"
            "ON t0.id=t1.id\n"
            "WHERE t0.id=%s",
        )
        self.assertEqual(dml.Execute(3), 2)
        self.assertEqual(db.Select("*").From(db.tb2).Execute(), self.TEST_DATA[3:])
        self.assertEqual(db.Select("*").From(db.tb3).Execute(), self.TEST_DATA[3:])
        self.assertEqual(await dml.aioExecute(4), 2)
        self.assertEqual(db.Select("*").From(db.tb2).Execute(), self.TEST_DATA[4:])
        self.assertEqual(db.Select("*").From(db.tb3).Execute(), self.TEST_DATA[4:])

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = (
            db.Delete(db.tb2, "from201203", multi_tables=["t0", "t1"])
            .Join(db.tb3, "t0.id=t1.id", partition="from201202")
            .Where("t0.id=%s")
        )
        self.assertEqual(
            dml.statement(),
            "DELETE t0, t1 FROM db.tb2 PARTITION (from201203) AS t0\n"
            "INNER JOIN db.tb3 PARTITION (from201202) AS t1\n\t"
            "ON t0.id=t1.id\n"
            "WHERE t0.id=%s",
        )
        self.assertEqual(dml.Execute(5), 0)
        self.assertEqual(db.Select("*").From(db.tb2).Execute(), self.TEST_DATA[4:])
        self.assertEqual(db.Select("*").From(db.tb3).Execute(), self.TEST_DATA[4:])
        self.assertEqual(await dml.aioExecute(5), 0)
        self.assertEqual(db.Select("*").From(db.tb2).Execute(), self.TEST_DATA[4:])
        self.assertEqual(db.Select("*").From(db.tb3).Execute(), self.TEST_DATA[4:])

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = (
            db.Delete(db.tb2, multi_tables=["t0", "t1"])
            .UseIndex(db.tb2.idx1)
            .Join(db.tb3, "t0.id=t1.id")
            .UseIndex(db.tb3.idx1)
            .Where("t0.id=%s")
        )
        self.assertEqual(
            dml.statement(),
            "DELETE t0, t1 FROM db.tb2 AS t0\n\t"
            "USE INDEX (idx1)\n"
            "INNER JOIN db.tb3 AS t1\n\t"
            "USE INDEX (idx1)\n\t"
            "ON t0.id=t1.id\n"
            "WHERE t0.id=%s",
        )
        self.assertEqual(dml.Execute(5), 2)
        self.assertEqual(db.Select("*").From(db.tb2).Execute(), self.TEST_DATA[5:])
        self.assertEqual(db.Select("*").From(db.tb3).Execute(), self.TEST_DATA[5:])
        self.assertEqual(await dml.aioExecute(6), 2)
        self.assertEqual(db.Select("*").From(db.tb2).Execute(), ())
        self.assertEqual(db.Select("*").From(db.tb3).Execute(), ())

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        with self.assertRaises(errors.DMLClauseError):
            dml = (
                db.Delete(db.tb2, "from201203", multi_tables=["t0", "t1"])
                .Join(db.tb3, "t0.id=t1.id", partition="from201202")
                .Where("t0.id=%s")
                .OrderBy("t0.id DESC")
            )

        with self.assertRaises(errors.DMLClauseError):
            dml = (
                db.Delete(db.tb2, "from201203", multi_tables=["t0", "t1"])
                .Join(db.tb3, "t0.id=t1.id", partition="from201202")
                .Where("t0.id=%s")
                .Limit(1)
            )
        with self.assertRaises(errors.DMLClauseError):
            db.Delete(db.tb1).Join(db.tb2, "t0.id=t1.id").Where("t0.id=1").statement()
        with self.assertRaises(errors.DMLClauseError):
            db.Delete(db.tb1).Where("id=1").Join(db.tb2, "t0.id=t1.id").statement()

        # Finished
        db.Drop(True)
        self.log_ended("DELETE multi-table")

    async def test_table_delete(self) -> None:
        self.log_start("TABLE DELETE")

        db = self.prepareDatabase()
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = db.tb1.Delete().Where("id=%s")
        self.assertEqual(dml.statement(), "DELETE FROM db.tb1 AS t0\nWHERE id=%s")
        self.assertEqual(dml.Execute(1), 1)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA[1:])
        self.assertEqual(await dml.aioExecute(2), 1)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA[2:])

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        db = self.prepareDatabase()
        dml = (
            db.tb1.Delete(multi_tables="t0")
            .Join(db.tb2, "t0.id=t1.id")
            .Where("t1.id=%s")
        )
        self.assertEqual(
            dml.statement(),
            "DELETE t0 FROM db.tb1 AS t0\n"
            "INNER JOIN db.tb2 AS t1\n\t"
            "ON t0.id=t1.id\n"
            "WHERE t1.id=%s",
        )
        self.assertEqual(dml.Execute(1), 1)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA[1:])
        self.assertEqual(await dml.aioExecute(2), 1)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA[2:])
        self.assertEqual(dml.Execute([3, 4], many=True), 2)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), self.TEST_DATA[4:])
        self.assertEqual(await dml.aioExecute([5, 6], many=True), 2)
        self.assertEqual(db.Select("*").From(db.tb1).Execute(), ())

        # Finished
        db.Drop(True)
        self.log_ended("TABLE DELETE")


class TestWithClause(TestCase):
    name: str = "WITH Clause"

    # Utils
    def setupDML(self) -> WithDML:
        return WithDML("db", self.get_pool())

    # Test
    def test_all(self) -> None:
        self.test_with_clause()

    def test_with_clause(self) -> None:
        self.log_start("WITH Clause")

        db = TestDatabase("db", self.get_pool())
        dml = self.setupDML()

        # . name
        self.assertClauseEqual(
            dml._gen_with_clause("cte", "apple", ()),
            "WITH cte AS (apple)",
        )
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_with_clause(1, "apple", ())
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_with_clause(None, "apple", ())
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_with_clause("", "apple", ())
        # . columns
        self.assertClauseEqual(
            dml._gen_with_clause("cte", "apple", ("a",)),
            "WITH cte (a) AS (apple)",
        )
        self.assertClauseEqual(
            dml._gen_with_clause("cte", "apple", ("a", "b")),
            "WITH cte (a, b) AS (apple)",
        )
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_with_clause("cte", "apple", ("",))
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_with_clause("cte", "apple", (1,))
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_with_clause("cte", "apple", (None,))
        # . recursive
        self.assertClauseEqual(
            dml._gen_with_clause("cte", "apple", ("a", "b"), True),
            "WITH RECURSIVE cte (a, b) AS (apple)",
        )
        # . subquery
        self.assertClauseEqual(
            dml._gen_with_clause("cte", db.Select("*").From(db.tb1), ()),
            "WITH cte AS (\n\tSELECT *\n\tFROM db.tb1 AS t0\n)",
        )
        self.assertClauseEqual(
            dml._gen_with_clause("cte", db.Select("*").From(db.tb1), ("a", "b")),
            "WITH cte (a, b) AS (\n\tSELECT *\n\tFROM db.tb1 AS t0\n)",
        )
        self.assertClauseEqual(
            dml._gen_with_clause("cte", db.Select("*").From(db.tb1), ("a", "b"), True),
            "WITH RECURSIVE cte (a, b) AS (\n\tSELECT *\n\tFROM db.tb1 AS t0\n)",
        )
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_with_clause("cte", 1, ())
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_with_clause("cte", None, ())
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_with_clause("cte", "", ())
        with self.assertRaises(errors.DMLArgumentError):
            dml._gen_with_clause("cte", db.Delete(db.tb1), ())

        self.log_ended("WITH Clause")


class TestWithStatement(TestCase):
    name: str = "WITH Statement"
    TEST_DATA1: tuple = (
        (1, "a0", 1.0, datetime.datetime(2012, 1, 1, 0, 0)),
        (2, "a1", 1.1, datetime.datetime(2012, 1, 1, 0, 0)),
        (3, "b0", 2.0, datetime.datetime(2012, 2, 1, 0, 0)),
        (4, "b1", 2.1, datetime.datetime(2012, 2, 1, 0, 0)),
        (5, "c1", 3.0, datetime.datetime(2012, 3, 1, 0, 0)),
        (6, "c2", 3.1, datetime.datetime(2012, 3, 1, 0, 0)),
    )
    TEST_DATA2: tuple = (
        (1, "xa0", 11.0, datetime.datetime(2012, 1, 1, 0, 0)),
        (2, "xa1", 11.1, datetime.datetime(2012, 1, 1, 0, 0)),
        (3, "xb0", 12.0, datetime.datetime(2012, 2, 1, 0, 0)),
        (4, "xb1", 12.1, datetime.datetime(2012, 2, 1, 0, 0)),
        (5, "xc1", 13.0, datetime.datetime(2012, 3, 1, 0, 0)),
        (6, "xc2", 13.1, datetime.datetime(2012, 3, 1, 0, 0)),
    )

    def prepareDatabase(self) -> TestDatabase:
        db = super().prepareDatabase()
        with db.acquire() as conn:
            with conn.transaction() as cur:
                cur.executemany(
                    f"INSERT INTO {db.tb1} (id, name, price, dt) VALUES (%s, %s, %s, %s)",
                    self.TEST_DATA1,
                )
                cur.executemany(
                    f"INSERT INTO {db.tb2} (id, name, price, dt) VALUES (%s, %s, %s, %s)",
                    self.TEST_DATA1,
                )
                cur.executemany(
                    f"INSERT INTO {db.tb3} (id, name, price, dt) VALUES (%s, %s, %s, %s)",
                    self.TEST_DATA2,
                )
        return db

    # Test
    async def test_all(self) -> None:
        await self.test_with_recursive()
        await self.test_with_select()
        await self.test_with_update()
        await self.test_with_delete()

    async def test_with_recursive(self) -> None:
        self.log_start("WITH RECURSIVE")

        db = self.prepareDatabase()
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = (
            db.With(
                "cte (n)",
                db.Select("1").Union(
                    db.Select("n + 1").From("cte").Where("n < 5"), True
                ),
                recursive=True,
            )
            .Select("*")
            .From("cte")
        )
        self.assertEqual(
            dml.statement(),
            "WITH RECURSIVE cte (n) AS (\n\t"
            "SELECT 1\n\t"
            "UNION ALL\n\t"
            "(\n\t\t"
            "SELECT n + 1\n\t\t"
            "FROM cte AS t0\n\t\t"
            "WHERE n < 5\n\t"
            ")\n"
            ")\n"
            "SELECT *\n"
            "FROM cte AS t0",
        )
        self.assertEqual(dml.Execute(), ((1,), (2,), (3,), (4,), (5,)))
        self.assertEqual(await dml.aioExecute(), ((1,), (2,), (3,), (4,), (5,)))

        # Finished
        db.Drop(True)
        self.log_ended("WITH RECURSIVE")

    async def test_with_select(self) -> None:
        self.log_start("WITH SELECT")

        db = self.prepareDatabase()
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = db.With("c1", db.Select("*").From(db.tb2)).Select("*").From("c1")
        self.assertEqual(
            dml.statement(),
            "WITH c1 AS (\n\t"
            "SELECT *\n\t"
            "FROM db.tb2 AS t0\n"
            ")\n"
            "SELECT *\n"
            "FROM c1 AS t0",
        )
        self.assertEqual(dml.Execute(), self.TEST_DATA1)
        self.assertEqual(await dml.aioExecute(), self.TEST_DATA1)

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = (
            db.With("c1", db.Select("*").From(db.tb2))
            .With("c2", db.Select("*").From(db.tb3))
            .Select("t0.*")
            .From("c1")
            .Join("c2", "t0.id=t1.id")
        )
        self.assertEqual(
            dml.statement(),
            "WITH c1 AS (\n\t"
            "SELECT *\n\t"
            "FROM db.tb2 AS t0\n"
            "),\n"
            "c2 AS (\n\t"
            "SELECT *\n\t"
            "FROM db.tb3 AS t0\n"
            ")\n"
            "SELECT t0.*\n"
            "FROM c1 AS t0\n"
            "INNER JOIN c2 AS t1\n\t"
            "ON t0.id=t1.id",
        )
        self.assertEqual(dml.Execute(), self.TEST_DATA1)
        self.assertEqual(await dml.aioExecute(), self.TEST_DATA1)

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = (
            db.With("c1", db.Select("*").From(db.tb2))
            .With("c2", db.Select("*").From(db.tb3))
            .Select("*")
            .From("c1")
            .Union(db.Select("*").From("c2"), True)
        )
        self.assertEqual(
            dml.statement(),
            "WITH c1 AS (\n\t"
            "SELECT *\n\t"
            "FROM db.tb2 AS t0\n"
            "),\n"
            "c2 AS (\n\t"
            "SELECT *\n\t"
            "FROM db.tb3 AS t0\n"
            ")\n"
            "SELECT *\n"
            "FROM c1 AS t0\n"
            "UNION ALL\n"
            "(\n\t"
            "SELECT *\n\t"
            "FROM c2 AS t0\n"
            ")",
        )
        res = self.TEST_DATA1 + self.TEST_DATA2
        self.assertEqual(dml.Execute(), res)
        self.assertEqual(await dml.aioExecute(), res)

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = (
            db.With("c1", db.Select("*").From(db.tb2))
            .Select("t1.*")
            .From("c1")
            .Join(db.tb3, "t0.id=t1.id")
        )
        self.assertEqual(
            dml.statement(),
            "WITH c1 AS (\n\t"
            "SELECT *\n\t"
            "FROM db.tb2 AS t0\n"
            ")\n"
            "SELECT t1.*\n"
            "FROM c1 AS t0\n"
            "INNER JOIN db.tb3 AS t1\n\t"
            "ON t0.id=t1.id",
        )
        self.assertEqual(dml.Execute(), self.TEST_DATA2)
        self.assertEqual(await dml.aioExecute(), self.TEST_DATA2)

        # Finished
        db.Drop(True)
        self.log_ended("WITH SELECT")

    async def test_with_update(self) -> None:
        self.log_start("WITH UPDATE")

        db = self.prepareDatabase()
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = (
            db.With("c1", db.Select("*").From(db.tb3))
            .Update(db.tb1)
            .Join("c1", "t0.id=t1.id")
            .Set("t0.name=t1.name", "t0.price=t1.price")
            .Where("t0.id=%s")
        )
        self.assertEqual(
            dml.statement(),
            "WITH c1 AS (\n\t"
            "SELECT *\n\t"
            "FROM db.tb3 AS t0\n"
            ")\n"
            "UPDATE db.tb1 AS t0\n"
            "INNER JOIN c1 AS t1\n\t"
            "ON t0.id=t1.id\n"
            "SET\n\t"
            "t0.name=t1.name,\n\t"
            "t0.price=t1.price\n"
            "WHERE t0.id=%s",
        )
        self.assertEqual(dml.Execute(1), 1)
        self.assertEqual(
            db.Select("*").From(db.tb1).Where("id=%s").Execute(1),
            self.TEST_DATA2[0:1],
        )
        self.assertEqual(await dml.aioExecute(2), 1)
        self.assertEqual(
            db.Select("*").From(db.tb1).Where("id=%s").Execute(2),
            self.TEST_DATA2[1:2],
        )
        self.assertEqual(dml.Execute([3, 4], many=True), 2)
        self.assertEqual(
            db.Select("*").From(db.tb1).Where(in_conds={"id": [3, 4]}).Execute(),
            self.TEST_DATA2[2:4],
        )
        self.assertEqual(await dml.aioExecute([5, 6], many=True), 2)
        self.assertEqual(
            db.Select("*").From(db.tb1).Where(in_conds={"id": [5, 6]}).Execute(),
            self.TEST_DATA2[4:6],
        )

        # Finished
        db.Drop(True)
        self.log_ended("WITH UPDATE")

    async def test_with_delete(self) -> None:
        self.log_start("WITH DELETE")

        db = self.prepareDatabase()
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = (
            db.With("c1", db.Select("*").From(db.tb2))
            .Delete(db.tb1, multi_tables=["t0"])
            .Join("c1", "t0.id=t1.id")
            .Where("t0.id=%s")
        )
        self.assertEqual(
            dml.statement(),
            "WITH c1 AS (\n\t"
            "SELECT *\n\t"
            "FROM db.tb2 AS t0\n"
            ")\n"
            "DELETE t0 FROM db.tb1 AS t0\n"
            "INNER JOIN c1 AS t1\n\t"
            "ON t0.id=t1.id\n"
            "WHERE t0.id=%s",
        )
        self.assertEqual(dml.Execute(1), 1)
        self.assertEqual(db.Select("COUNT(*)").From(db.tb1).Execute(), ((5,),))
        self.assertEqual(await dml.aioExecute(2), 1)
        self.assertEqual(db.Select("COUNT(*)").From(db.tb1).Execute(), ((4,),))

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        dml = (
            db.With("c1", db.Select("*").From(db.tb2))
            .Delete(db.tb1, multi_tables=["t0", "t1"])
            .Join(db.tb3, "t0.id=t1.id")
            .Join("c1", "t0.id=t2.id")
            .Where("t0.id=%s")
        )
        self.assertEqual(
            dml.statement(),
            "WITH c1 AS (\n\t"
            "SELECT *\n\t"
            "FROM db.tb2 AS t0\n"
            ")\n"
            "DELETE t0, t1 FROM db.tb1 AS t0\n"
            "INNER JOIN db.tb3 AS t1\n\t"
            "ON t0.id=t1.id\n"
            "INNER JOIN c1 AS t2\n\t"
            "ON t0.id=t2.id\n"
            "WHERE t0.id=%s",
        )
        self.assertEqual(dml.Execute([3, 4], many=True), 4)
        self.assertEqual(db.Select("COUNT(*)").From(db.tb1).Execute(), ((2,),))
        self.assertEqual(db.Select("COUNT(*)").From(db.tb3).Execute(), ((4,),))
        self.assertEqual(dml.Execute([5, 6], many=True), 4)
        self.assertEqual(db.Select("COUNT(*)").From(db.tb1).Execute(), ((0,),))
        self.assertEqual(db.Select("COUNT(*)").From(db.tb3).Execute(), ((2,),))

        # Finished
        db.Drop(True)
        self.log_ended("WITH DELETE")


if __name__ == "__main__":
    HOST = "localhost"
    PORT = 3306
    USER = "root"
    PSWD = "Password_123456"

    for case in (
        TestSelectClause,
        TestSelectStatement,
        TestInsertClause,
        TestInsertStatement,
        TestReplaceStatement,
        TestUpdateClause,
        TestUpdateStatement,
        TestDeleteClause,
        TestDeleteStatement,
        TestWithClause,
        TestWithStatement,
    ):
        test_case = case(HOST, PORT, USER, PSWD)
        if not iscoroutinefunction(test_case.test_all):
            test_case.test_all()
        else:
            asyncio.run(test_case.test_all())
