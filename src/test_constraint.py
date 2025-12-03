import asyncio, time, unittest
from inspect import iscoroutinefunction
from sqlcycli import errors as sqlerr, Pool, Connection
from mysqlengine import errors
from mysqlengine.table import Table
from mysqlengine.database import Database
from mysqlengine.column import Define, Definition, Column
from mysqlengine.constraint import (
    Constraint,
    Constraints,
    PrimaryKey,
    UniqueKey,
    ForeignKey,
    Check,
)


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

    def setup_constraint(self, cnst: Constraint, name: str = "cnst") -> Constraint:
        cnst.set_name(name)
        cnst.setup("tb1", "db1", "utf8", None, self.get_pool())
        return cnst

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


class TestConstraintDefinition(TestCase):
    name = "Constraint Definition"

    def test_all(self) -> None:
        self.test_primary_key()
        self.test_unique_key()
        self.test_foreign_key()
        self.test_check()
        self.test_constraints()

    def test_primary_key(self) -> None:
        self.log_start("Primary Key")

        args = [
            # string
            (
                ["col1", "col2", "col3"],
                "CONSTRAINT pk_key PRIMARY KEY (col1, col2, col3)",
            ),
            # Column
            (
                ["col_int", "col_float", "col_date"],
                "CONSTRAINT pk_key PRIMARY KEY (col_int, col_float, col_date)",
            ),
            # Columns
            (
                ["col_tinyint", "col_smallint", "col_mediumint"],
                "CONSTRAINT pk_key PRIMARY KEY (col_tinyint, col_smallint, col_mediumint)",
            ),
        ]
        for cols, cmp in args:
            pk = self.setup_constraint(PrimaryKey(*cols), "pk_key")
            self.assertEqual(pk._gen_definition_sql(), cmp)

        # Index Type
        pk = self.setup_constraint(
            PrimaryKey("col1", "col2", "col3", index_type="HASH"), "pk_key"
        )
        self.assertEqual(
            pk._gen_definition_sql(),
            "CONSTRAINT pk_key PRIMARY KEY (col1, col2, col3) USING HASH",
        )

        # Comment
        pk = self.setup_constraint(
            PrimaryKey("col1", "col2", "col3", comment="Comment"), "pk_key"
        )
        self.assertEqual(
            pk._gen_definition_sql(),
            "CONSTRAINT pk_key PRIMARY KEY (col1, col2, col3) COMMENT 'Comment'",
        )

        # Mixed
        pk = self.setup_constraint(
            PrimaryKey("col1", "col2", "col3", index_type="HASH", comment="Comment"),
            "pk_key",
        )
        self.assertEqual(
            pk._gen_definition_sql(),
            "CONSTRAINT pk_key PRIMARY KEY (col1, col2, col3) USING HASH COMMENT 'Comment'",
        )

        # Symbol
        pk = self.setup_constraint(
            PrimaryKey("col1", "col2", "col3", index_type="HASH", comment="Comment"),
            "pk_key",
        )
        self.assertEqual(pk.symbol, "PRIMARY")
        pk = self.setup_constraint(
            PrimaryKey("col1", "col2", "col3", index_type="HASH", comment="Comment"),
            "xxx",
        )
        self.assertEqual(pk.symbol, "PRIMARY")

        # Error
        with self.assertRaises(errors.ConstraintDefinitionError):
            PrimaryKey()
        with self.assertRaises(errors.ConstraintDefinitionError):
            PrimaryKey(None)
        with self.assertRaises(errors.ConstraintDefinitionError):
            PrimaryKey("col", None)
        with self.assertRaises(errors.ConstraintDefinitionError):
            PrimaryKey("col", [None])
        with self.assertRaises(errors.ConstraintDefinitionError):
            PrimaryKey("col", [1])

        self.log_ended("Primary Key")

    def test_unique_key(self) -> None:
        self.log_start("Unique Key")

        args = [
            # string
            (
                ["col1", "col2", "col3"],
                "CONSTRAINT uk_key UNIQUE KEY (col1, col2, col3)",
            ),
            # Column
            (
                ["col_int", "col_float", "col_date"],
                "CONSTRAINT uk_key UNIQUE KEY (col_int, col_float, col_date)",
            ),
            # Columns
            (
                ["col_tinyint", "col_smallint", "col_mediumint"],
                "CONSTRAINT uk_key UNIQUE KEY (col_tinyint, col_smallint, col_mediumint)",
            ),
        ]
        for cols, cmp in args:
            uk = self.setup_constraint(UniqueKey(*cols), "uk_key")
            self.assertEqual(uk._gen_definition_sql(), cmp)

        uk = self.setup_constraint(
            UniqueKey("col1", "col2", "col3", comment="Comment"), "uk_key"
        )
        self.assertEqual(
            uk._gen_definition_sql(),
            "CONSTRAINT uk_key UNIQUE KEY (col1, col2, col3) COMMENT 'Comment'",
        )

        uk = self.setup_constraint(
            UniqueKey("col1", "col2", "col3", visible=False), "uk_key"
        )
        self.assertEqual(
            uk._gen_definition_sql(),
            "CONSTRAINT uk_key UNIQUE KEY (col1, col2, col3) INVISIBLE",
        )

        uk = self.setup_constraint(
            UniqueKey("col1", "col2", "col3", comment="Comment", visible=False),
            "uk_key",
        )
        self.assertEqual(
            uk._gen_definition_sql(),
            "CONSTRAINT uk_key UNIQUE KEY (col1, col2, col3) COMMENT 'Comment' INVISIBLE",
        )

        # Index Type
        uk = self.setup_constraint(
            UniqueKey("col1", "col2", "col3", index_type="HASH"), "uk_key"
        )
        self.assertEqual(
            uk._gen_definition_sql(),
            "CONSTRAINT uk_key UNIQUE KEY (col1, col2, col3) USING HASH",
        )

        #  Comment
        uk = self.setup_constraint(
            UniqueKey("col1", "col2", "col3", comment="Comment"), "uk_key"
        )
        self.assertEqual(
            uk._gen_definition_sql(),
            "CONSTRAINT uk_key UNIQUE KEY (col1, col2, col3) COMMENT 'Comment'",
        )

        # Visible
        uk = self.setup_constraint(
            UniqueKey("col1", "col2", "col3", visible=False), "uk_key"
        )
        self.assertEqual(
            uk._gen_definition_sql(),
            "CONSTRAINT uk_key UNIQUE KEY (col1, col2, col3) INVISIBLE",
        )

        # Mixed
        uk = self.setup_constraint(
            UniqueKey(
                "col1",
                "col2",
                "col3",
                index_type="HASH",
                comment="Comment",
                visible=False,
            ),
            "uk_key",
        )
        self.assertEqual(
            uk._gen_definition_sql(),
            "CONSTRAINT uk_key UNIQUE KEY (col1, col2, col3) USING HASH COMMENT 'Comment' INVISIBLE",
        )

        # Symbol
        uk = self.setup_constraint(
            UniqueKey(
                "col1",
                "col2",
                "col3",
                index_type="HASH",
                comment="Comment",
                visible=False,
            ),
            "uk_key",
        )
        self.assertEqual(uk.symbol, "uk_key")

        # Error
        with self.assertRaises(errors.ConstraintDefinitionError):
            UniqueKey()
        with self.assertRaises(errors.ConstraintDefinitionError):
            UniqueKey(None)
        with self.assertRaises(errors.ConstraintDefinitionError):
            UniqueKey("col", None)
        with self.assertRaises(errors.ConstraintDefinitionError):
            UniqueKey("col", [None])
        with self.assertRaises(errors.ConstraintDefinitionError):
            UniqueKey("col", [1])

        self.log_ended("Unique Key")

    def test_foreign_key(self) -> None:
        self.log_start("Foreign Key")

        args = [
            # string
            (
                ["col1", "col2", "col3"],
                "CONSTRAINT tb1_fk_key FOREIGN KEY (col1, col2, col3) REFERENCES db1.tb2 (col1, col2, col3)",
            ),
            # Column
            (
                ["col_int", "col_float", "col_date"],
                "CONSTRAINT tb1_fk_key FOREIGN KEY (col_int, col_float, col_date) REFERENCES db1.tb2 (col_int, col_float, col_date)",
            ),
            # Columns
            (
                ["col_tinyint", "col_smallint", "col_mediumint"],
                "CONSTRAINT tb1_fk_key FOREIGN KEY (col_tinyint, col_smallint, col_mediumint) REFERENCES db1.tb2 (col_tinyint, col_smallint, col_mediumint)",
            ),
        ]
        for cols, cmp in args:
            fk = self.setup_constraint(ForeignKey(cols, "tb2", cols), "fk_key")
            self.assertEqual(fk._gen_definition_sql(), cmp)

        cols = ["col1", "col2", "col3"]
        fk = self.setup_constraint(
            ForeignKey(cols, "db1.tb2", cols, on_delete="CASCADE"), "fk_key"
        )
        self.assertEqual(
            fk._gen_definition_sql(),
            "CONSTRAINT tb1_fk_key FOREIGN KEY (col1, col2, col3) REFERENCES db1.tb2 (col1, col2, col3) ON DELETE CASCADE",
        )

        fk = self.setup_constraint(
            ForeignKey(cols, "tb2", cols, on_update="RESTRICT"), "fk_key"
        )
        self.assertEqual(
            fk._gen_definition_sql(),
            "CONSTRAINT tb1_fk_key FOREIGN KEY (col1, col2, col3) REFERENCES db1.tb2 (col1, col2, col3) ON UPDATE RESTRICT",
        )

        fk = self.setup_constraint(
            ForeignKey(cols, "tb2", cols, on_delete="CASCADE", on_update="RESTRICT"),
            "fk_key",
        )
        self.assertEqual(
            fk._gen_definition_sql(),
            "CONSTRAINT tb1_fk_key FOREIGN KEY (col1, col2, col3) REFERENCES db1.tb2 (col1, col2, col3) ON DELETE CASCADE ON UPDATE RESTRICT",
        )

        # Symbol
        fk = self.setup_constraint(
            ForeignKey(cols, "tb2", cols, on_delete="CASCADE", on_update="RESTRICT"),
            "fk_key",
        )
        self.assertEqual(fk.symbol, "tb1_fk_key")
        fk = self.setup_constraint(
            ForeignKey(cols, "tb2", cols, on_delete="CASCADE", on_update="RESTRICT"),
            "tb1fk_key",
        )
        self.assertEqual(fk.symbol, "tb1_fk_key")
        fk = self.setup_constraint(
            ForeignKey(cols, "tb2", cols, on_delete="CASCADE", on_update="RESTRICT"),
            "tb1_fk_key",
        )
        self.assertEqual(fk.symbol, "tb1_fk_key")

        # Error
        with self.assertRaises(errors.ConstraintDefinitionError):
            ForeignKey([], "tb2", cols)
        with self.assertRaises(errors.ConstraintDefinitionError):
            ForeignKey(None, "tb2", cols)
        with self.assertRaises(errors.ConstraintDefinitionError):
            ForeignKey("col1", "tb2", None)
        with self.assertRaises(errors.ConstraintDefinitionError):
            ForeignKey("col1", "tb2", "col2", on_delete="C")

        self.log_ended("Foreign Key")

    def test_check(self) -> None:
        self.log_start("Check")

        # Expression
        ck = self.setup_constraint(Check("col1 > 0"), "ck_key")
        self.assertEqual(
            ck._gen_definition_sql(), "CONSTRAINT tb1_ck_key CHECK (col1 > 0)"
        )

        # Enforced
        ck = self.setup_constraint(Check("col1 > 0", enforced=False), "ck_key")
        self.assertEqual(
            ck._gen_definition_sql(),
            "CONSTRAINT tb1_ck_key CHECK (col1 > 0) NOT ENFORCED",
        )

        # Symbol
        ck = self.setup_constraint(Check("col1 > 0", enforced=False), "ck_key")
        self.assertEqual(ck.symbol, "tb1_ck_key")
        ck = self.setup_constraint(Check("col1 > 0", enforced=False), "tb1ck_key")
        self.assertEqual(ck.symbol, "tb1_ck_key")
        ck = self.setup_constraint(Check("col1 > 0", enforced=False), "tb1_ck_key")
        self.assertEqual(ck.symbol, "tb1_ck_key")

        self.log_ended("Check")

    def test_constraints(self) -> None:
        self.log_start("Constraints")

        cols = ["cola", "colb", "colc"]
        pk = self.setup_constraint(PrimaryKey(*cols), "pk_key")
        uk = self.setup_constraint(UniqueKey(*cols), "uk_key")
        fk = self.setup_constraint(ForeignKey(cols, "tb2", cols), "fk_key")
        ck = self.setup_constraint(Check("cola > 0"), "ck_key")

        cnsts = Constraints(*[pk, uk, fk, ck])
        self.assertEqual(len(cnsts.search_name("pk_key")), 1)
        self.assertEqual(len(cnsts.search_name("ck_key")), 1)
        self.assertEqual(len(cnsts.search_name("tb1_ck_key")), 1)
        self.assertEqual(len(cnsts.search_name("pk", exact=False)), 1)
        self.assertEqual(len(cnsts.search_name("key", exact=False)), 4)
        self.assertEqual(len(cnsts.search_type("primary key")), 1)
        self.assertEqual(len(cnsts.search_type(UniqueKey)), 1)
        self.assertEqual(len(cnsts.search_type([PrimaryKey, UniqueKey])), 2)
        self.assertEqual(len(cnsts.search_type("PRIMARY", exact=False)), 1)
        self.assertEqual(len(cnsts.search_type("KEY", exact=False)), 3)
        self.assertTrue(cnsts.issubset(["pk_key", cnsts["uk_key"], cnsts]))
        self.assertFalse(cnsts.issubset("pk_keyx"))
        self.assertEqual(len(cnsts.filter("pk_keyx")), 0)
        self.assertEqual(len(cnsts.filter("pk_key")), 1)
        self.assertEqual(len(cnsts.filter(["pk_key", uk, cnsts["ck_key"]])), 3)
        self.assertEqual(len(cnsts.filter(cnsts)), len(cnsts))
        self.assertIs(cnsts.get("pk_key"), pk)
        self.assertIs(cnsts.get(pk), pk)
        self.assertIs(cnsts.get("ck_key"), ck)
        self.assertIs(cnsts.get("uk_keyx"), None)
        for c in cnsts:
            self.assertIsInstance(c, Constraint)
        self.assertIn("pk_key", cnsts)
        self.assertIn(pk, cnsts)
        self.assertNotIn("pk_keyx", cnsts)
        cnsts._gen_definition_sql()

        # Conflicts
        with self.assertRaises(errors.ConstraintArgumentError):
            Constraints(pk, pk)

        self.log_ended("Constraints")


class TestConstraintCopy(TestCase):
    name = "Constraint Copy"

    def test_all(self) -> None:
        self.log_start("COPY")

        class ParenetTable(Table):
            id: Column = Column(Define.BIGINT())

        class TestTable(Table):
            id: Column = Column(Define.BIGINT())
            pid: Column = Column(Define.BIGINT())
            name: Column = Column(Define.VARCHAR(255))
            price: Column = Column(Define.DECIMAL(12, 2))
            pk: PrimaryKey = PrimaryKey("id")
            uk: UniqueKey = UniqueKey("name", "price DESC")
            fk: ForeignKey = ForeignKey("pid", "ptb", "id")
            ck: Check = Check("price > 0")

        class TestDatabase(Database):
            ptb: ParenetTable = ParenetTable()
            tb1: TestTable = TestTable()
            tb2: TestTable = TestTable()

        db = TestDatabase("test_db", self.get_pool())
        self.assertIsNot(db.tb1.pk, db.tb2.pk)
        self.assertIsNot(db.tb1.uk, db.tb2.uk)
        self.assertIsNot(db.tb1.fk, db.tb2.fk)
        self.assertIsNot(db.tb1.ck, db.tb2.ck)
        self.assertIsNot(db.tb1.constraints["pk"], db.tb2.constraints["pk"])
        self.assertIsNot(db.tb1.constraints["uk"], db.tb2.constraints["uk"])
        self.assertIsNot(db.tb1.constraints["fk"], db.tb2.constraints["fk"])
        self.assertIsNot(db.tb1.constraints["ck"], db.tb2.constraints["ck"])

        self.log_ended("COPY")


class TestConstraintSyncSQL(TestCase):
    name: str = "Constraint Sync SQL"

    def test_all(self) -> None:
        self.test_unique_key_basic_sql()
        self.test_primary_key_basic_sql()
        self.test_foreign_key_basic_sql()
        self.test_check_constraint_basic_sql()
        self.test_mixed_constraints_sql()

    def test_unique_key_basic_sql(self) -> None:
        self.log_start("UNIQUE KEY BASIC SQL")

        # Add & Exists & Drop
        class TestTable(Table):
            id: Column = Column(Define.BIGINT())
            name: Column = Column(Define.VARCHAR(255))
            price: Column = Column(Define.DECIMAL(12, 2))
            pk: PrimaryKey = PrimaryKey("id")
            uk1: UniqueKey = UniqueKey("name", "price DESC")
            uk2: UniqueKey = UniqueKey("price")

        class TestDatabase(Database):
            tb: TestTable = TestTable()

        db = TestDatabase("test_db", self.get_pool())
        self.assertTrue(db.Drop(True))
        self.assertTrue(db.Create())
        self.assertTrue(db.tb.Create())

        for cnst in db.tb.constraints:
            self.assertTrue(cnst.Exists())
            self.assertTrue(cnst.Drop())
            self.assertFalse(cnst.Exists())
            self.assertTrue(cnst.Add())
            self.assertTrue(cnst.Exists())
        self.assertTrue(db.Drop())

        # Show metadata
        with self.assertRaises(sqlerr.OperationalError):
            db.tb.uk1.ShowMetadata()
        self.assertTrue(db.Create())
        self.assertTrue(db.tb.Create())
        for cnst in db.tb.constraints:
            if cnst is db.tb.pk:
                continue
            meta = cnst.ShowMetadata()
            self.assertEqual(meta.db_name, db.db_name)
            self.assertEqual(meta.tb_name, db.tb.tb_name)
            self.assertEqual(meta.constraint_name, cnst.name)
            self.assertEqual(meta.constraint_name, cnst.symbol)
            self.assertTrue(meta.enforced)
            self.assertEqual(meta.index_type, "BTREE")
            self.assertTrue(meta.unique)
            self.assertIsNone(meta.comment)
            self.assertTrue(meta.visible)
            if cnst is db.tb.uk1:
                self.assertEqual(meta.columns, ("name", "price DESC"))
            else:
                self.assertEqual(meta.columns, ("price",))

        # Alter
        cnst = db.tb.uk1
        # . default state
        orig_cols = cnst.columns
        orig_type = cnst.index_type
        orig_comment = cnst.comment
        orig_visible = cnst.visible
        # . columns: only
        cnst.Alter(columns="price")
        meta = cnst.ShowMetadata()
        self.assertEqual(cnst.columns, ("price",))
        self.assertEqual(meta.columns, ("price",))
        self.assertEqual(cnst.index_type, orig_type)
        self.assertEqual(meta.index_type, orig_type)
        self.assertEqual(cnst.comment, orig_comment)
        self.assertEqual(meta.comment, orig_comment)
        self.assertEqual(cnst.visible, orig_visible)
        self.assertEqual(meta.visible, orig_visible)
        # . index type: only (Should not change)
        cnst.Alter(index_type="HASH")
        meta = cnst.ShowMetadata()
        self.assertEqual(cnst.columns, ("price",))
        self.assertEqual(meta.columns, ("price",))
        self.assertEqual(cnst.index_type, orig_type)
        self.assertEqual(meta.index_type, orig_type)
        self.assertEqual(cnst.comment, orig_comment)
        self.assertEqual(meta.comment, orig_comment)
        self.assertEqual(cnst.visible, orig_visible)
        self.assertEqual(meta.visible, orig_visible)
        # . comment: only
        cnst.Alter(comment="描述")
        meta = cnst.ShowMetadata()
        self.assertEqual(cnst.columns, ("price",))
        self.assertEqual(meta.columns, ("price",))
        self.assertEqual(cnst.index_type, orig_type)
        self.assertEqual(meta.index_type, orig_type)
        self.assertEqual(cnst.comment, "描述")
        self.assertEqual(meta.comment, "描述")
        self.assertEqual(cnst.visible, orig_visible)
        self.assertEqual(meta.visible, orig_visible)
        # . visible: only
        cnst.Alter(visible=False)
        meta = cnst.ShowMetadata()
        self.assertEqual(cnst.columns, ("price",))
        self.assertEqual(meta.columns, ("price",))
        self.assertEqual(cnst.index_type, orig_type)
        self.assertEqual(meta.index_type, orig_type)
        self.assertEqual(cnst.comment, "描述")
        self.assertEqual(meta.comment, "描述")
        self.assertEqual(cnst.visible, False)
        self.assertEqual(meta.visible, False)
        self.assertTrue(cnst.SetVisible(True))
        self.assertTrue(cnst.ShowMetadata().visible)
        self.assertTrue(cnst.SetVisible(False))
        self.assertFalse(cnst.ShowMetadata().visible)
        # . mixed
        cnst.Alter(orig_cols, orig_type, "", orig_visible)
        meta = cnst.ShowMetadata()
        self.assertEqual(cnst.columns, orig_cols)
        self.assertEqual(meta.columns, orig_cols)
        self.assertEqual(cnst.index_type, orig_type)
        self.assertEqual(meta.index_type, orig_type)
        self.assertEqual(cnst.comment, orig_comment)
        self.assertEqual(meta.comment, orig_comment)
        self.assertEqual(cnst.visible, orig_visible)
        self.assertEqual(meta.visible, orig_visible)

        # Alter: local-remote mismatch
        class TestTable2(Table):
            id: Column = Column(Define.BIGINT())
            name: Column = Column(Define.VARCHAR(255))
            price: Column = Column(Define.DECIMAL(12, 2))
            pk: PrimaryKey = PrimaryKey("id")
            uk1: UniqueKey = UniqueKey("name", index_type="HASH", visible=False)

        class TestDatabase2(Database):
            tb: TestTable2 = TestTable2()

        db2 = TestDatabase2("test_db", self.get_pool())
        cnst = db2.tb.uk1
        self.assertEqual(cnst.columns, ("name",))
        self.assertEqual(cnst.index_type, "HASH")
        self.assertIsNone(cnst.comment)
        self.assertFalse(cnst.visible)
        meta = cnst.ShowMetadata()
        self.assertEqual(meta.columns, ("name", "price DESC"))
        self.assertEqual(meta.index_type, "BTREE")
        self.assertIsNone(meta.comment)
        self.assertTrue(meta.visible)
        cnst.Alter(comment="Comment", visible=True)
        self.assertEqual(cnst.columns, ("name",))
        self.assertEqual(cnst.index_type, "BTREE")
        self.assertEqual(cnst.comment, "Comment")
        self.assertTrue(cnst.visible)
        meta = cnst.ShowMetadata()
        self.assertEqual(meta.columns, ("name",))
        self.assertEqual(meta.index_type, "BTREE")
        self.assertEqual(meta.comment, "Comment")
        self.assertTrue(meta.visible)
        self.assertTrue(cnst.Drop())

        # Sync from remote
        db.tb.uk1.Add()

        class TestTable2(Table):
            id: Column = Column(Define.BIGINT())
            name: Column = Column(Define.VARCHAR(255))
            price: Column = Column(Define.DECIMAL(12, 2))
            pk: PrimaryKey = PrimaryKey("id")
            uk1: UniqueKey = UniqueKey(
                "name", index_type="HASH", comment="Comment", visible=False
            )

        class TestDatabase2(Database):
            tb: TestTable2 = TestTable2()

        db2 = TestDatabase2("test_db", self.get_pool())
        cnst = db2.tb.uk1
        # . before sync
        self.assertEqual(cnst.columns, ("name",))
        self.assertEqual(cnst.index_type, "HASH")
        self.assertEqual(cnst.comment, "Comment")
        self.assertFalse(cnst.visible)
        # . after sync
        cnst.SyncFromRemote()
        self.assertEqual(cnst.columns, ("name", "price DESC"))
        self.assertEqual(cnst.index_type, "BTREE")
        self.assertIsNone(cnst.comment)
        self.assertTrue(cnst.visible)
        meta = cnst.ShowMetadata()
        self.assertEqual(meta.columns, ("name", "price DESC"))
        self.assertEqual(meta.index_type, "BTREE")
        self.assertIsNone(meta.comment)
        self.assertTrue(meta.visible)

        # Sync to remote
        self.assertTrue(cnst.Drop())
        self.assertTrue(db.tb.uk1.SyncToRemote())  # re-create

        class TestTable2(Table):
            id: Column = Column(Define.BIGINT())
            name: Column = Column(Define.VARCHAR(255))
            price: Column = Column(Define.DECIMAL(12, 2))
            pk: PrimaryKey = PrimaryKey("id")
            uk1: UniqueKey = UniqueKey(
                "name", index_type="HASH", comment="Comment", visible=False
            )

        class TestDatabase2(Database):
            tb: TestTable2 = TestTable2()

        db2 = TestDatabase2("test_db", self.get_pool())
        cnst = db2.tb.uk1
        # . before sync
        meta = cnst.ShowMetadata()
        self.assertEqual(meta.columns, ("name", "price DESC"))
        self.assertEqual(meta.index_type, "BTREE")
        self.assertIsNone(meta.comment)
        self.assertTrue(meta.visible)
        # . after sync
        cnst.SyncToRemote()
        self.assertEqual(cnst.columns, ("name",))
        self.assertEqual(cnst.index_type, "BTREE")
        self.assertEqual(cnst.comment, "Comment")
        self.assertFalse(cnst.visible)
        meta = cnst.ShowMetadata()
        self.assertEqual(meta.columns, ("name",))
        self.assertEqual(meta.index_type, "BTREE")
        self.assertEqual(meta.comment, "Comment")
        self.assertFalse(meta.visible)
        self.assertTrue(cnst.Drop())

        # Finished
        self.assertTrue(db.Drop(True))
        self.log_ended("UNIQUE KEY BASIC SQL")

    def test_primary_key_basic_sql(self) -> None:
        self.log_start("PRIMARY KEY BASIC SQL")

        # Add & Exists & Drop
        class TestTable(Table):
            id: Column = Column(Define.BIGINT())
            name: Column = Column(Define.VARCHAR(255))
            price: Column = Column(Define.DECIMAL(12, 2))
            pk: PrimaryKey = PrimaryKey("id", "price DESC")

        class TestDatabase(Database):
            tb: TestTable = TestTable()

        db = TestDatabase("test_db", self.get_pool())
        self.assertTrue(db.Drop(True))
        self.assertTrue(db.Create())
        self.assertTrue(db.tb.Create())

        cnst = db.tb.pk
        self.assertTrue(cnst.Exists())
        self.assertTrue(cnst.Drop())
        self.assertFalse(cnst.Exists())
        self.assertTrue(cnst.Add())
        self.assertTrue(cnst.Exists())
        self.assertTrue(db.Drop())

        # Show metadata
        with self.assertRaises(sqlerr.OperationalError):
            db.tb.pk.ShowMetadata()
        self.assertTrue(db.Create())
        self.assertTrue(db.tb.Create())
        cnst = db.tb.pk
        meta = cnst.ShowMetadata()
        self.assertEqual(meta.db_name, db.db_name)
        self.assertEqual(meta.tb_name, db.tb.tb_name)
        self.assertEqual(meta.constraint_name, "PRIMARY")
        self.assertEqual(meta.constraint_name, cnst.symbol)
        self.assertTrue(meta.enforced)
        self.assertEqual(meta.index_type, "BTREE")
        self.assertTrue(meta.unique)
        self.assertIsNone(meta.comment)
        self.assertTrue(meta.visible)
        self.assertEqual(meta.columns, ("id", "price DESC"))

        # Alter
        cnst = db.tb.pk
        # . default state
        orig_cols = cnst.columns
        orig_type = cnst.index_type
        orig_comment = cnst.comment
        # . columns: only
        cnst.Alter(columns="price")
        meta = cnst.ShowMetadata()
        self.assertEqual(cnst.columns, ("price",))
        self.assertEqual(meta.columns, ("price",))
        self.assertEqual(cnst.index_type, orig_type)
        self.assertEqual(meta.index_type, orig_type)
        self.assertEqual(cnst.comment, orig_comment)
        self.assertEqual(meta.comment, orig_comment)
        # . index type: only (Should not change)
        cnst.Alter(index_type="HASH")
        meta = cnst.ShowMetadata()
        self.assertEqual(cnst.columns, ("price",))
        self.assertEqual(meta.columns, ("price",))
        self.assertEqual(cnst.index_type, orig_type)
        self.assertEqual(meta.index_type, orig_type)
        self.assertEqual(cnst.comment, orig_comment)
        self.assertEqual(meta.comment, orig_comment)
        # . comment: only
        cnst.Alter(comment="描述")
        meta = cnst.ShowMetadata()
        self.assertEqual(cnst.columns, ("price",))
        self.assertEqual(meta.columns, ("price",))
        self.assertEqual(cnst.index_type, orig_type)
        self.assertEqual(meta.index_type, orig_type)
        self.assertEqual(cnst.comment, "描述")
        self.assertEqual(meta.comment, "描述")
        # . visibility
        self.assertFalse(cnst.SetVisible(True))
        with self.assertRaises(sqlerr.OperationalError):
            cnst.SetVisible(False)
        # . mixed
        cnst.Alter(orig_cols, orig_type, "")
        meta = cnst.ShowMetadata()
        self.assertEqual(cnst.columns, orig_cols)
        self.assertEqual(meta.columns, orig_cols)
        self.assertEqual(cnst.index_type, orig_type)
        self.assertEqual(meta.index_type, orig_type)
        self.assertEqual(cnst.comment, orig_comment)
        self.assertEqual(meta.comment, orig_comment)

        # Alter: local-remote mismatch
        class TestTable2(Table):
            id: Column = Column(Define.BIGINT())
            name: Column = Column(Define.VARCHAR(255))
            price: Column = Column(Define.DECIMAL(12, 2))
            pk: PrimaryKey = PrimaryKey("id", index_type="HASH", comment="Comment")

        class TestDatabase2(Database):
            tb: TestTable2 = TestTable2()

        db2 = TestDatabase2("test_db", self.get_pool())
        cnst = db2.tb.pk
        self.assertEqual(cnst.columns, ("id",))
        self.assertEqual(cnst.index_type, "HASH")
        self.assertEqual(cnst.comment, "Comment")
        meta = cnst.ShowMetadata()
        self.assertEqual(meta.columns, ("id", "price DESC"))
        self.assertEqual(meta.index_type, "BTREE")
        self.assertIsNone(meta.comment)
        cnst.Alter(comment="Comment")
        self.assertEqual(cnst.columns, ("id",))
        self.assertEqual(cnst.index_type, "BTREE")
        self.assertEqual(cnst.comment, "Comment")
        self.assertTrue(cnst.visible)
        meta = cnst.ShowMetadata()
        self.assertEqual(meta.columns, ("id",))
        self.assertEqual(meta.index_type, "BTREE")
        self.assertEqual(meta.comment, "Comment")
        self.assertTrue(cnst.Drop())

        # Sync from remote
        db.tb.pk.Add()

        class TestTable2(Table):
            id: Column = Column(Define.BIGINT())
            name: Column = Column(Define.VARCHAR(255))
            price: Column = Column(Define.DECIMAL(12, 2))
            pk: PrimaryKey = PrimaryKey("id", index_type="HASH", comment="Comment")

        class TestDatabase2(Database):
            tb: TestTable2 = TestTable2()

        db2 = TestDatabase2("test_db", self.get_pool())
        cnst = db2.tb.pk
        # . before sync
        self.assertEqual(cnst.columns, ("id",))
        self.assertEqual(cnst.index_type, "HASH")
        self.assertEqual(cnst.comment, "Comment")
        # . after sync
        cnst.SyncFromRemote()
        self.assertEqual(cnst.columns, ("id", "price DESC"))
        self.assertEqual(cnst.index_type, "BTREE")
        self.assertIsNone(cnst.comment)
        meta = cnst.ShowMetadata()
        self.assertEqual(meta.columns, ("id", "price DESC"))
        self.assertEqual(meta.index_type, "BTREE")
        self.assertIsNone(meta.comment)

        # Sync to remote
        self.assertTrue(cnst.Drop())
        self.assertTrue(db.tb.pk.SyncToRemote())  # re-create

        class TestTable2(Table):
            id: Column = Column(Define.BIGINT())
            name: Column = Column(Define.VARCHAR(255))
            price: Column = Column(Define.DECIMAL(12, 2))
            pk: PrimaryKey = PrimaryKey("id", index_type="HASH", comment="Comment")

        class TestDatabase2(Database):
            tb: TestTable2 = TestTable2()

        db2 = TestDatabase2("test_db", self.get_pool())
        cnst = db2.tb.pk
        # . before sync
        meta = cnst.ShowMetadata()
        self.assertEqual(meta.columns, ("id", "price DESC"))
        self.assertEqual(meta.index_type, "BTREE")
        self.assertIsNone(meta.comment)
        # . after sync
        cnst.SyncToRemote()
        self.assertEqual(cnst.columns, ("id",))
        self.assertEqual(cnst.index_type, "BTREE")
        self.assertEqual(cnst.comment, "Comment")
        meta = cnst.ShowMetadata()
        self.assertEqual(meta.columns, ("id",))
        self.assertEqual(meta.index_type, "BTREE")
        self.assertEqual(meta.comment, "Comment")
        self.assertTrue(cnst.Drop())

        # Finished
        self.assertTrue(db.Drop(True))
        self.log_ended("PRIMARY KEY BASIC SQL")

    def test_foreign_key_basic_sql(self) -> None:
        self.log_start("FOREIGN KEY BASIC SQL")

        # Add & Exists & Drop
        class TestTable1(Table):
            id: Column = Column(Define.BIGINT())
            name: Column = Column(Define.VARCHAR(255))
            pk: PrimaryKey = PrimaryKey("id")
            uk: UniqueKey = UniqueKey("id", "name")

        class TestTable2(Table):
            id: Column = Column(Define.BIGINT())
            name_id: Column = Column(Define.BIGINT())
            name: Column = Column(Define.VARCHAR(255))
            price: Column = Column(Define.DECIMAL(12, 2))
            pk: PrimaryKey = PrimaryKey("id")
            fk: ForeignKey = ForeignKey(("name_id", "name"), "tb1", ("id", "name"))

        class TestTable3(Table):
            id: Column = Column(Define.BIGINT())
            name: Column = Column(Define.VARCHAR(255))
            pk: PrimaryKey = PrimaryKey("id")
            uk: UniqueKey = UniqueKey("id", "name")

        class TestDatabase(Database):
            tb1: TestTable1 = TestTable1()
            tb2: TestTable2 = TestTable2()
            tb3: TestTable3 = TestTable3()

        db = TestDatabase("test_db", self.get_pool())
        self.assertTrue(db.Drop(True))
        self.assertTrue(db.Create())
        self.assertTrue(db.tb1.Create())
        self.assertTrue(db.tb2.Create())
        self.assertTrue(db.tb3.Create())
        cnst = db.tb2.fk
        self.assertTrue(cnst.Exists())
        self.assertTrue(cnst.Drop())
        self.assertFalse(cnst.Exists())
        self.assertTrue(cnst.Add())
        self.assertTrue(cnst.Exists())
        self.assertTrue(db.Drop())

        # Show metadata
        with self.assertRaises(sqlerr.OperationalError):
            db.tb2.fk.ShowMetadata()
        self.assertTrue(db.Create())
        self.assertTrue(db.tb1.Create())
        self.assertTrue(db.tb2.Create())
        self.assertTrue(db.tb3.Create())
        cnst = db.tb2.fk
        meta = cnst.ShowMetadata()
        self.assertEqual(meta.db_name, db.db_name)
        self.assertEqual(meta.tb_name, db.tb2.tb_name)
        self.assertEqual(meta.constraint_name, cnst.symbol)
        self.assertTrue(meta.enforced)
        self.assertEqual(meta.columns, ("name_id", "name"))
        self.assertEqual(meta.reference_table, "tb1")
        self.assertEqual(meta.reference_columns, ("id", "name"))
        self.assertEqual(meta.on_delete, "NO ACTION")
        self.assertEqual(meta.on_update, "NO ACTION")

        # Alter
        cnst = db.tb2.fk
        # . default state
        orig_cols = cnst.columns
        orig_ref_tb = cnst.reference_table
        orig_ref_cols = cnst.reference_columns
        orig_on_delete = cnst.on_delete
        orig_on_update = cnst.on_update
        # . columns: only
        cnst.Alter(columns="name_id", reference_columns=db.tb1.id)
        meta = cnst.ShowMetadata()
        self.assertEqual(cnst.columns, ("name_id",))
        self.assertEqual(meta.columns, ("name_id",))
        self.assertEqual(cnst.reference_table, orig_ref_tb)
        self.assertEqual(meta.reference_table, orig_ref_tb)
        self.assertEqual(cnst.reference_columns, ("id",))
        self.assertEqual(meta.reference_columns, ("id",))
        self.assertEqual(cnst.on_delete, orig_on_delete)
        self.assertEqual(meta.on_delete, orig_on_delete)
        self.assertEqual(cnst.on_update, orig_on_update)
        self.assertEqual(meta.on_update, orig_on_update)
        # . table: only
        cnst.Alter(reference_table=db.tb3)
        meta = cnst.ShowMetadata()
        self.assertEqual(cnst.columns, ("name_id",))
        self.assertEqual(meta.columns, ("name_id",))
        self.assertEqual(cnst.reference_table, "tb3")
        self.assertEqual(meta.reference_table, "tb3")
        self.assertEqual(cnst.reference_columns, ("id",))
        self.assertEqual(meta.reference_columns, ("id",))
        self.assertEqual(cnst.on_delete, orig_on_delete)
        self.assertEqual(meta.on_delete, orig_on_delete)
        self.assertEqual(cnst.on_update, orig_on_update)
        self.assertEqual(meta.on_update, orig_on_update)
        # . actions: only
        cnst.Alter(on_delete="CASCADE", on_update="RESTRICT")
        meta = cnst.ShowMetadata()
        self.assertEqual(cnst.columns, ("name_id",))
        self.assertEqual(meta.columns, ("name_id",))
        self.assertEqual(cnst.reference_table, "tb3")
        self.assertEqual(meta.reference_table, "tb3")
        self.assertEqual(cnst.reference_columns, ("id",))
        self.assertEqual(meta.reference_columns, ("id",))
        self.assertEqual(cnst.on_delete, "CASCADE")
        self.assertEqual(meta.on_delete, "CASCADE")
        self.assertEqual(cnst.on_update, "RESTRICT")
        self.assertEqual(cnst.on_update, "RESTRICT")
        # . mixed
        cnst.Alter(
            orig_cols, orig_ref_tb, orig_ref_cols, orig_on_delete, orig_on_update
        )
        meta = cnst.ShowMetadata()
        self.assertEqual(cnst.columns, orig_cols)
        self.assertEqual(meta.columns, orig_cols)
        self.assertEqual(cnst.reference_table, orig_ref_tb)
        self.assertEqual(meta.reference_table, orig_ref_tb)
        self.assertEqual(cnst.reference_columns, orig_ref_cols)
        self.assertEqual(meta.reference_columns, orig_ref_cols)
        self.assertEqual(cnst.on_delete, orig_on_delete)
        self.assertEqual(meta.on_delete, orig_on_delete)
        self.assertEqual(cnst.on_update, orig_on_update)
        self.assertEqual(meta.on_update, orig_on_update)

        # Alter: local-remote mismatch
        class TestTable_x1(Table):
            id: Column = Column(Define.BIGINT())
            name: Column = Column(Define.VARCHAR(255))
            pk: PrimaryKey = PrimaryKey("id", "name")

        class TestTable_x2(Table):
            id: Column = Column(Define.BIGINT())
            name_id: Column = Column(Define.BIGINT())
            name: Column = Column(Define.VARCHAR(255))
            price: Column = Column(Define.DECIMAL(12, 2))
            pk: PrimaryKey = PrimaryKey("id")
            fk: ForeignKey = ForeignKey(
                ("name_id", "name"),
                "tb1",
                ("id",),
                on_delete="CASCADE",
                on_update="RESTRICT",
            )

        class TestTable_x3(Table):
            id: Column = Column(Define.BIGINT())
            name: Column = Column(Define.VARCHAR(255))
            pk: PrimaryKey = PrimaryKey("id", "name")

        class TestDatabase2(Database):
            tb1: TestTable_x1 = TestTable_x1()
            tb2: TestTable_x2 = TestTable_x2()
            tb3: TestTable_x3 = TestTable_x3()

        db2 = TestDatabase2("test_db", self.get_pool())
        cnst = db2.tb2.fk
        self.assertEqual(cnst.columns, ("name_id", "name"))
        self.assertEqual(cnst.reference_table, "tb1")
        self.assertEqual(cnst.reference_columns, ("id",))
        self.assertEqual(cnst.on_delete, "CASCADE")
        self.assertEqual(cnst.on_update, "RESTRICT")
        meta = cnst.ShowMetadata()
        self.assertEqual(meta.columns, ("name_id", "name"))
        self.assertEqual(meta.reference_table, "tb1")
        self.assertEqual(meta.reference_columns, ("id", "name"))
        self.assertEqual(meta.on_delete, "NO ACTION")
        self.assertEqual(meta.on_update, "NO ACTION")
        cnst.Alter(
            reference_table="tb3",
            reference_columns=("id", "name"),
            on_delete="NO ACTION",
        )
        self.assertEqual(cnst.columns, ("name_id", "name"))
        self.assertEqual(cnst.reference_table, "tb3")
        self.assertEqual(cnst.reference_columns, ("id", "name"))
        self.assertEqual(cnst.on_delete, "NO ACTION")
        self.assertEqual(cnst.on_update, "RESTRICT")
        meta = cnst.ShowMetadata()
        self.assertEqual(meta.columns, ("name_id", "name"))
        self.assertEqual(meta.reference_table, "tb3")
        self.assertEqual(meta.reference_columns, ("id", "name"))
        self.assertEqual(meta.on_delete, "NO ACTION")
        self.assertEqual(meta.on_update, "RESTRICT")
        self.assertTrue(cnst.Drop())

        # Sync from remote
        db.tb2.fk.Add()

        class TestTable_x1(Table):
            id: Column = Column(Define.BIGINT())
            name: Column = Column(Define.VARCHAR(255))
            pk: PrimaryKey = PrimaryKey("id", "name")

        class TestTable_x2(Table):
            id: Column = Column(Define.BIGINT())
            name_id: Column = Column(Define.BIGINT())
            name: Column = Column(Define.VARCHAR(255))
            price: Column = Column(Define.DECIMAL(12, 2))
            pk: PrimaryKey = PrimaryKey("id")
            fk: ForeignKey = ForeignKey(
                ("name_id"),
                "tb3",
                ("id",),
                on_delete="CASCADE",
                on_update="RESTRICT",
            )

        class TestDatabase2(Database):
            tb1: TestTable_x1 = TestTable_x1()
            tb2: TestTable_x2 = TestTable_x2()

        db2 = TestDatabase2("test_db", self.get_pool())
        cnst = db2.tb2.fk
        # . before sync
        self.assertEqual(cnst.columns, ("name_id",))
        self.assertEqual(cnst.reference_table, "tb3")
        self.assertEqual(cnst.reference_columns, ("id",))
        self.assertEqual(cnst.on_delete, "CASCADE")
        self.assertEqual(cnst.on_update, "RESTRICT")
        # . after sync
        cnst.SyncFromRemote()
        self.assertEqual(cnst.columns, ("name_id", "name"))
        self.assertEqual(cnst.reference_table, "tb1")
        self.assertEqual(cnst.reference_columns, ("id", "name"))
        self.assertEqual(cnst.on_delete, "NO ACTION")
        self.assertEqual(cnst.on_update, "NO ACTION")
        meta = cnst.ShowMetadata()
        self.assertEqual(meta.columns, ("name_id", "name"))
        self.assertEqual(meta.reference_table, "tb1")
        self.assertEqual(meta.reference_columns, ("id", "name"))
        self.assertEqual(meta.on_delete, "NO ACTION")
        self.assertEqual(meta.on_update, "NO ACTION")

        # Sync to remote
        self.assertTrue(cnst.Drop())
        self.assertTrue(db.tb2.fk.SyncToRemote())  # re-create

        class TestTable_x1(Table):
            id: Column = Column(Define.BIGINT())
            name: Column = Column(Define.VARCHAR(255))
            pk: PrimaryKey = PrimaryKey("id", "name")

        class TestTable_x2(Table):
            id: Column = Column(Define.BIGINT())
            name_id: Column = Column(Define.BIGINT())
            name: Column = Column(Define.VARCHAR(255))
            price: Column = Column(Define.DECIMAL(12, 2))
            pk: PrimaryKey = PrimaryKey("id")
            fk: ForeignKey = ForeignKey(
                ("name_id"),
                "tb3",
                ("id",),
                on_delete="CASCADE",
                on_update="RESTRICT",
            )

        class TestTable_x3(Table):
            id: Column = Column(Define.BIGINT())
            name: Column = Column(Define.VARCHAR(255))
            pk: PrimaryKey = PrimaryKey("id", "name")

        class TestDatabase2(Database):
            tb1: TestTable_x1 = TestTable_x1()
            tb2: TestTable_x2 = TestTable_x2()
            tb3: TestTable_x3 = TestTable_x3()

        db2 = TestDatabase2("test_db", self.get_pool())
        cnst = db2.tb2.fk
        # . before sync
        meta = cnst.ShowMetadata()
        self.assertEqual(meta.columns, ("name_id", "name"))
        self.assertEqual(meta.reference_table, "tb1")
        self.assertEqual(meta.reference_columns, ("id", "name"))
        self.assertEqual(meta.on_delete, "NO ACTION")
        self.assertEqual(meta.on_update, "NO ACTION")
        # . after sync
        cnst.SyncToRemote()
        self.assertEqual(cnst.columns, ("name_id",))
        self.assertEqual(cnst.reference_table, "tb3")
        self.assertEqual(cnst.reference_columns, ("id",))
        self.assertEqual(cnst.on_delete, "CASCADE")
        self.assertEqual(cnst.on_update, "RESTRICT")
        meta = cnst.ShowMetadata()
        self.assertEqual(meta.columns, ("name_id",))
        self.assertEqual(meta.reference_table, "tb3")
        self.assertEqual(meta.reference_columns, ("id",))
        self.assertEqual(meta.on_delete, "CASCADE")
        self.assertEqual(meta.on_update, "RESTRICT")
        self.assertTrue(cnst.Drop())

        # Finished
        self.assertTrue(db.Drop(True))
        self.log_ended("FOREIGN KEY BASIC SQL")

    def test_check_constraint_basic_sql(self) -> None:
        self.log_start("CHECK CONSTRAINT BASIC SQL")

        # Add & Exists & Drop
        class TestTable(Table):
            id: Column = Column(Define.BIGINT())
            price: Column = Column(Define.DECIMAL(12, 2))
            pk: PrimaryKey = PrimaryKey("id")
            ck1: Check = Check("price > 0")
            ck2: Check = Check("price < 1000")

        class TestDatabase(Database):
            tb: TestTable = TestTable()

        db = TestDatabase("test_db", self.get_pool())
        self.assertTrue(db.Drop(True))
        self.assertTrue(db.Create())
        self.assertTrue(db.tb.Create())

        for cnst in (db.tb.ck1, db.tb.ck2):
            self.assertTrue(cnst.Exists())
            self.assertTrue(cnst.Drop())
            self.assertFalse(cnst.Exists())
            self.assertTrue(cnst.Add())
            self.assertTrue(cnst.Exists())
        self.assertTrue(db.Drop())

        # Show metadata
        with self.assertRaises(sqlerr.OperationalError):
            db.tb.ck1.ShowMetadata()
        self.assertTrue(db.Create())
        self.assertTrue(db.tb.Create())
        cnst = db.tb.ck1
        meta = cnst.ShowMetadata()
        self.assertEqual(meta.db_name, db.db_name)
        self.assertEqual(meta.tb_name, db.tb.tb_name)
        self.assertEqual(meta.constraint_name, cnst.symbol)
        self.assertTrue(meta.enforced)
        self.assertEqual(meta.expression, "`price` > 0")

        # Alter
        # . default state
        orig_expr = cnst.expression
        orig_enforced = cnst.enforced
        # . expression: only
        cnst.Alter(expression="price > 1")
        meta = cnst.ShowMetadata()
        self.assertEqual(cnst.expression, "`price` > 1")
        self.assertEqual(meta.expression, "`price` > 1")
        self.assertEqual(cnst.enforced, orig_enforced)
        self.assertEqual(meta.enforced, orig_enforced)
        # . enforced: only
        cnst.Alter(enforced=False)
        meta = cnst.ShowMetadata()
        self.assertEqual(cnst.expression, "`price` > 1")
        self.assertEqual(meta.expression, "`price` > 1")
        self.assertEqual(cnst.enforced, False)
        self.assertEqual(meta.enforced, False)
        self.assertTrue(cnst.SetEnforced(True))
        self.assertTrue(cnst.ShowMetadata().enforced)
        self.assertTrue(cnst.SetEnforced(False))
        self.assertFalse(cnst.ShowMetadata().enforced)
        # . mixed
        cnst.Alter(orig_expr, orig_enforced)
        meta = cnst.ShowMetadata()
        self.assertEqual(cnst.expression, orig_expr)
        self.assertEqual(meta.expression, orig_expr)
        self.assertEqual(cnst.enforced, orig_enforced)
        self.assertEqual(meta.enforced, orig_enforced)

        # Alter: local-remote mismatch
        class TestTable2(Table):
            id: Column = Column(Define.BIGINT())
            price: Column = Column(Define.DECIMAL(12, 2))
            pk: PrimaryKey = PrimaryKey("id")
            ck1: Check = Check("price > 1", enforced=True)

        class TestDatabase2(Database):
            tb: TestTable2 = TestTable2()

        db2 = TestDatabase2("test_db", self.get_pool())
        cnst = db2.tb.ck1
        self.assertEqual(cnst.expression, "price > 1")
        self.assertEqual(cnst.enforced, True)
        meta = cnst.ShowMetadata()
        self.assertEqual(meta.expression, "`price` > 0")
        self.assertEqual(meta.enforced, True)
        cnst.Alter(expression="price > 2", enforced=False)
        self.assertEqual(cnst.expression, "`price` > 2")
        self.assertEqual(cnst.enforced, False)
        meta = cnst.ShowMetadata()
        self.assertEqual(meta.expression, "`price` > 2")
        self.assertEqual(meta.enforced, False)
        self.assertTrue(cnst.Drop())

        # Sync from remote
        db.tb.ck1.Add()

        class TestTable2(Table):
            id: Column = Column(Define.BIGINT())
            price: Column = Column(Define.DECIMAL(12, 2))
            pk: PrimaryKey = PrimaryKey("id")
            ck1: Check = Check("price > 1", enforced=False)

        class TestDatabase2(Database):
            tb: TestTable2 = TestTable2()

        db2 = TestDatabase2("test_db", self.get_pool())
        cnst = db2.tb.ck1
        # . before sync
        self.assertEqual(cnst.expression, "price > 1")
        self.assertEqual(cnst.enforced, False)
        # . after sync
        cnst.SyncFromRemote()
        self.assertEqual(cnst.expression, "`price` > 0")
        self.assertEqual(cnst.enforced, True)
        meta = cnst.ShowMetadata()
        self.assertEqual(meta.expression, "`price` > 0")
        self.assertEqual(meta.enforced, True)

        # Sync to remote
        self.assertTrue(cnst.Drop())
        self.assertTrue(db.tb.ck1.SyncToRemote())  # re-create

        class TestTable2(Table):
            id: Column = Column(Define.BIGINT())
            price: Column = Column(Define.DECIMAL(12, 2))
            pk: PrimaryKey = PrimaryKey("id")
            ck1: Check = Check("price > 1", enforced=False)

        class TestDatabase2(Database):
            tb: TestTable2 = TestTable2()

        db2 = TestDatabase2("test_db", self.get_pool())
        cnst = db2.tb.ck1
        # . before sync
        meta = cnst.ShowMetadata()
        self.assertEqual(meta.expression, "`price` > 0")
        self.assertEqual(meta.enforced, True)
        # . after sync
        cnst.SyncToRemote()
        self.assertEqual(cnst.expression, "`price` > 1")
        self.assertEqual(cnst.enforced, False)
        meta = cnst.ShowMetadata()
        self.assertEqual(meta.expression, "`price` > 1")
        self.assertEqual(meta.enforced, False)
        self.assertTrue(cnst.Drop())

        # Finished
        self.assertTrue(db.Drop(True))
        self.log_ended("CHECK CONSTRAINT BASIC SQL")

    def test_mixed_constraints_sql(self) -> None:
        self.log_start("MIXED CONSTRAINTS SQL (symbol conflicts)")

        class TestTable1(Table):
            id: Column = Column(Define.BIGINT())
            name: Column = Column(Define.VARCHAR(255))
            price: Column = Column(Define.DECIMAL(12, 2))
            pk: PrimaryKey = PrimaryKey(
                "id", "name", index_type="HASH", comment="Comment"
            )
            uk: UniqueKey = UniqueKey(
                "name", "price", index_type="HASH", comment="Comment"
            )
            uk2: UniqueKey = UniqueKey("name")
            ck: Check = Check("price > 0")

        class TestTable2(Table):
            id: Column = Column(Define.BIGINT())
            name: Column = Column(Define.VARCHAR(255))
            price: Column = Column(Define.DECIMAL(12, 2))
            pk: PrimaryKey = PrimaryKey(
                "id", "name", index_type="HASH", comment="Comment"
            )
            uk: UniqueKey = UniqueKey(
                "name", "price", index_type="HASH", comment="Comment"
            )
            ck: Check = Check("price > 0")
            fk: ForeignKey = ForeignKey(("id", "name"), "tb1", ("id", "name"))

        class TestTable3(Table):
            id: Column = Column(Define.BIGINT())
            name: Column = Column(Define.VARCHAR(255))
            price: Column = Column(Define.DECIMAL(12, 2))
            pk: PrimaryKey = PrimaryKey(
                "id", "name", index_type="HASH", comment="Comment"
            )
            uk: UniqueKey = UniqueKey(
                "name", "price", index_type="HASH", comment="Comment"
            )
            ck: Check = Check("price > 0")
            fk: ForeignKey = ForeignKey("name", "tb1", "name")

        class TestDatabase(Database):
            tb1: TestTable1 = TestTable1()
            tb2: TestTable2 = TestTable2()
            tb3: TestTable3 = TestTable3()

        db = TestDatabase("test_db", self.get_pool())
        self.assertTrue(db.Drop(True))
        self.assertTrue(db.Initialize())
        self.assertEqual(
            db.tb1.pk.ShowConstraintSymbols(),
            ("PRIMARY", "uk", "uk2", "tb1_ck"),
        )

        # Finished
        self.assertTrue(db.Drop(True))
        self.log_ended("MIXED CONSTRAINTS SQL (symbol conflicts)")


class TestConstraintAyncSQL(TestCase):
    name: str = "Constraint Async SQL"

    async def test_all(self) -> None:
        await self.test_unique_key_basic_sql()
        await self.test_primary_key_basic_sql()
        await self.test_foreign_key_basic_sql()
        await self.test_check_constraint_basic_sql()
        await self.test_mixed_constraints_sql()

    async def test_unique_key_basic_sql(self) -> None:
        self.log_start("UNIQUE KEY BASIC SQL")

        # Add & Exists & Drop
        class TestTable(Table):
            id: Column = Column(Define.BIGINT())
            name: Column = Column(Define.VARCHAR(255))
            price: Column = Column(Define.DECIMAL(12, 2))
            pk: PrimaryKey = PrimaryKey("id")
            uk1: UniqueKey = UniqueKey("name", "price DESC")
            uk2: UniqueKey = UniqueKey("price")

        class TestDatabase(Database):
            tb: TestTable = TestTable()

        db = TestDatabase("test_db", self.get_pool())
        self.assertTrue(await db.aioDrop(True))
        self.assertTrue(await db.aioCreate())
        self.assertTrue(await db.tb.aioCreate())

        for cnst in db.tb.constraints:
            self.assertTrue(await cnst.aioExists())
            self.assertTrue(await cnst.aioDrop())
            self.assertFalse(await cnst.aioExists())
            self.assertTrue(await cnst.aioAdd())
            self.assertTrue(await cnst.aioExists())
        self.assertTrue(await db.aioDrop())

        # Show metadata
        with self.assertRaises(sqlerr.OperationalError):
            await db.tb.uk1.aioShowMetadata()
        self.assertTrue(await db.aioCreate())
        self.assertTrue(await db.tb.aioCreate())
        for cnst in db.tb.constraints:
            if cnst is db.tb.pk:
                continue
            meta = await cnst.aioShowMetadata()
            self.assertEqual(meta.db_name, db.db_name)
            self.assertEqual(meta.tb_name, db.tb.tb_name)
            self.assertEqual(meta.constraint_name, cnst.name)
            self.assertEqual(meta.constraint_name, cnst.symbol)
            self.assertTrue(meta.enforced)
            self.assertEqual(meta.index_type, "BTREE")
            self.assertTrue(meta.unique)
            self.assertIsNone(meta.comment)
            self.assertTrue(meta.visible)
            if cnst is db.tb.uk1:
                self.assertEqual(meta.columns, ("name", "price DESC"))
            else:
                self.assertEqual(meta.columns, ("price",))

        # Alter
        cnst = db.tb.uk1
        # . default state
        orig_cols = cnst.columns
        orig_type = cnst.index_type
        orig_comment = cnst.comment
        orig_visible = cnst.visible
        # . columns: only
        await cnst.aioAlter(columns="price")
        meta = await cnst.aioShowMetadata()
        self.assertEqual(cnst.columns, ("price",))
        self.assertEqual(meta.columns, ("price",))
        self.assertEqual(cnst.index_type, orig_type)
        self.assertEqual(meta.index_type, orig_type)
        self.assertEqual(cnst.comment, orig_comment)
        self.assertEqual(meta.comment, orig_comment)
        self.assertEqual(cnst.visible, orig_visible)
        self.assertEqual(meta.visible, orig_visible)
        # . index type: only (Should not change)
        await cnst.aioAlter(index_type="HASH")
        meta = await cnst.aioShowMetadata()
        self.assertEqual(cnst.columns, ("price",))
        self.assertEqual(meta.columns, ("price",))
        self.assertEqual(cnst.index_type, orig_type)
        self.assertEqual(meta.index_type, orig_type)
        self.assertEqual(cnst.comment, orig_comment)
        self.assertEqual(meta.comment, orig_comment)
        self.assertEqual(cnst.visible, orig_visible)
        self.assertEqual(meta.visible, orig_visible)
        # . comment: only
        await cnst.aioAlter(comment="描述")
        meta = await cnst.aioShowMetadata()
        self.assertEqual(cnst.columns, ("price",))
        self.assertEqual(meta.columns, ("price",))
        self.assertEqual(cnst.index_type, orig_type)
        self.assertEqual(meta.index_type, orig_type)
        self.assertEqual(cnst.comment, "描述")
        self.assertEqual(meta.comment, "描述")
        self.assertEqual(cnst.visible, orig_visible)
        self.assertEqual(meta.visible, orig_visible)
        # . visible: only
        await cnst.aioAlter(visible=False)
        meta = await cnst.aioShowMetadata()
        self.assertEqual(cnst.columns, ("price",))
        self.assertEqual(meta.columns, ("price",))
        self.assertEqual(cnst.index_type, orig_type)
        self.assertEqual(meta.index_type, orig_type)
        self.assertEqual(cnst.comment, "描述")
        self.assertEqual(meta.comment, "描述")
        self.assertEqual(cnst.visible, False)
        self.assertEqual(meta.visible, False)
        self.assertTrue(await cnst.aioSetVisible(True))
        self.assertTrue((await cnst.aioShowMetadata()).visible)
        self.assertTrue(await cnst.aioSetVisible(False))
        self.assertFalse((await cnst.aioShowMetadata()).visible)
        # . mixed
        await cnst.aioAlter(orig_cols, orig_type, "", orig_visible)
        meta = await cnst.aioShowMetadata()
        self.assertEqual(cnst.columns, orig_cols)
        self.assertEqual(meta.columns, orig_cols)
        self.assertEqual(cnst.index_type, orig_type)
        self.assertEqual(meta.index_type, orig_type)
        self.assertEqual(cnst.comment, orig_comment)
        self.assertEqual(meta.comment, orig_comment)
        self.assertEqual(cnst.visible, orig_visible)
        self.assertEqual(meta.visible, orig_visible)

        # Alter: local-remote mismatch
        class TestTable2(Table):
            id: Column = Column(Define.BIGINT())
            name: Column = Column(Define.VARCHAR(255))
            price: Column = Column(Define.DECIMAL(12, 2))
            pk: PrimaryKey = PrimaryKey("id")
            uk1: UniqueKey = UniqueKey("name", index_type="HASH", visible=False)

        class TestDatabase2(Database):
            tb: TestTable2 = TestTable2()

        db2 = TestDatabase2("test_db", self.get_pool())
        cnst = db2.tb.uk1
        self.assertEqual(cnst.columns, ("name",))
        self.assertEqual(cnst.index_type, "HASH")
        self.assertIsNone(cnst.comment)
        self.assertFalse(cnst.visible)
        meta = await cnst.aioShowMetadata()
        self.assertEqual(meta.columns, ("name", "price DESC"))
        self.assertEqual(meta.index_type, "BTREE")
        self.assertIsNone(meta.comment)
        self.assertTrue(meta.visible)
        await cnst.aioAlter(comment="Comment", visible=True)
        self.assertEqual(cnst.columns, ("name",))
        self.assertEqual(cnst.index_type, "BTREE")
        self.assertEqual(cnst.comment, "Comment")
        self.assertTrue(cnst.visible)
        meta = await cnst.aioShowMetadata()
        self.assertEqual(meta.columns, ("name",))
        self.assertEqual(meta.index_type, "BTREE")
        self.assertEqual(meta.comment, "Comment")
        self.assertTrue(meta.visible)
        self.assertTrue(await cnst.aioDrop())

        # Sync from remote
        await db.tb.uk1.aioAdd()

        class TestTable2(Table):
            id: Column = Column(Define.BIGINT())
            name: Column = Column(Define.VARCHAR(255))
            price: Column = Column(Define.DECIMAL(12, 2))
            pk: PrimaryKey = PrimaryKey("id")
            uk1: UniqueKey = UniqueKey(
                "name", index_type="HASH", comment="Comment", visible=False
            )

        class TestDatabase2(Database):
            tb: TestTable2 = TestTable2()

        db2 = TestDatabase2("test_db", self.get_pool())
        cnst = db2.tb.uk1
        # . before sync
        self.assertEqual(cnst.columns, ("name",))
        self.assertEqual(cnst.index_type, "HASH")
        self.assertEqual(cnst.comment, "Comment")
        self.assertFalse(cnst.visible)
        # . after sync
        await cnst.aioSyncFromRemote()
        self.assertEqual(cnst.columns, ("name", "price DESC"))
        self.assertEqual(cnst.index_type, "BTREE")
        self.assertIsNone(cnst.comment)
        self.assertTrue(cnst.visible)
        meta = await cnst.aioShowMetadata()
        self.assertEqual(meta.columns, ("name", "price DESC"))
        self.assertEqual(meta.index_type, "BTREE")
        self.assertIsNone(meta.comment)
        self.assertTrue(meta.visible)

        # Sync to remote
        self.assertTrue(await cnst.aioDrop())
        self.assertTrue(await db.tb.uk1.aioSyncToRemote())  # re-create

        class TestTable2(Table):
            id: Column = Column(Define.BIGINT())
            name: Column = Column(Define.VARCHAR(255))
            price: Column = Column(Define.DECIMAL(12, 2))
            pk: PrimaryKey = PrimaryKey("id")
            uk1: UniqueKey = UniqueKey(
                "name", index_type="HASH", comment="Comment", visible=False
            )

        class TestDatabase2(Database):
            tb: TestTable2 = TestTable2()

        db2 = TestDatabase2("test_db", self.get_pool())
        cnst = db2.tb.uk1
        # . before sync
        meta = await cnst.aioShowMetadata()
        self.assertEqual(meta.columns, ("name", "price DESC"))
        self.assertEqual(meta.index_type, "BTREE")
        self.assertIsNone(meta.comment)
        self.assertTrue(meta.visible)
        # . after sync
        await cnst.aioSyncToRemote()
        self.assertEqual(cnst.columns, ("name",))
        self.assertEqual(cnst.index_type, "BTREE")
        self.assertEqual(cnst.comment, "Comment")
        self.assertFalse(cnst.visible)
        meta = await cnst.aioShowMetadata()
        self.assertEqual(meta.columns, ("name",))
        self.assertEqual(meta.index_type, "BTREE")
        self.assertEqual(meta.comment, "Comment")
        self.assertFalse(meta.visible)
        self.assertTrue(await cnst.aioDrop())

        # Finished
        self.assertTrue(await db.aioDrop(True))
        self.log_ended("UNIQUE KEY BASIC SQL")

    async def test_primary_key_basic_sql(self) -> None:
        self.log_start("PRIMARY KEY BASIC SQL")

        # Add & Exists & Drop
        class TestTable(Table):
            id: Column = Column(Define.BIGINT())
            name: Column = Column(Define.VARCHAR(255))
            price: Column = Column(Define.DECIMAL(12, 2))
            pk: PrimaryKey = PrimaryKey("id", "price DESC")

        class TestDatabase(Database):
            tb: TestTable = TestTable()

        db = TestDatabase("test_db", self.get_pool())
        self.assertTrue(await db.aioDrop(True))
        self.assertTrue(await db.aioCreate())
        self.assertTrue(await db.tb.aioCreate())

        cnst = db.tb.pk
        self.assertTrue(await cnst.aioExists())
        self.assertTrue(await cnst.aioDrop())
        self.assertFalse(await cnst.aioExists())
        self.assertTrue(await cnst.aioAdd())
        self.assertTrue(await cnst.aioExists())
        self.assertTrue(await db.aioDrop())

        # Show metadata
        with self.assertRaises(sqlerr.OperationalError):
            await db.tb.pk.aioShowMetadata()
        self.assertTrue(await db.aioCreate())
        self.assertTrue(await db.tb.aioCreate())
        cnst = db.tb.pk
        meta = await cnst.aioShowMetadata()
        self.assertEqual(meta.db_name, db.db_name)
        self.assertEqual(meta.tb_name, db.tb.tb_name)
        self.assertEqual(meta.constraint_name, "PRIMARY")
        self.assertEqual(meta.constraint_name, cnst.symbol)
        self.assertTrue(meta.enforced)
        self.assertEqual(meta.index_type, "BTREE")
        self.assertTrue(meta.unique)
        self.assertIsNone(meta.comment)
        self.assertTrue(meta.visible)
        self.assertEqual(meta.columns, ("id", "price DESC"))

        # Alter
        cnst = db.tb.pk
        # . default state
        orig_cols = cnst.columns
        orig_type = cnst.index_type
        orig_comment = cnst.comment
        # . columns: only
        await cnst.aioAlter(columns="price")
        meta = await cnst.aioShowMetadata()
        self.assertEqual(cnst.columns, ("price",))
        self.assertEqual(meta.columns, ("price",))
        self.assertEqual(cnst.index_type, orig_type)
        self.assertEqual(meta.index_type, orig_type)
        self.assertEqual(cnst.comment, orig_comment)
        self.assertEqual(meta.comment, orig_comment)
        # . index type: only (Should not change)
        await cnst.aioAlter(index_type="HASH")
        meta = await cnst.aioShowMetadata()
        self.assertEqual(cnst.columns, ("price",))
        self.assertEqual(meta.columns, ("price",))
        self.assertEqual(cnst.index_type, orig_type)
        self.assertEqual(meta.index_type, orig_type)
        self.assertEqual(cnst.comment, orig_comment)
        self.assertEqual(meta.comment, orig_comment)
        # . comment: only
        await cnst.aioAlter(comment="描述")
        meta = await cnst.aioShowMetadata()
        self.assertEqual(cnst.columns, ("price",))
        self.assertEqual(meta.columns, ("price",))
        self.assertEqual(cnst.index_type, orig_type)
        self.assertEqual(meta.index_type, orig_type)
        self.assertEqual(cnst.comment, "描述")
        self.assertEqual(meta.comment, "描述")
        # . visibility
        self.assertFalse(await cnst.aioSetVisible(True))
        with self.assertRaises(sqlerr.OperationalError):
            await cnst.aioSetVisible(False)
        # . mixed
        await cnst.aioAlter(orig_cols, orig_type, "")
        meta = await cnst.aioShowMetadata()
        self.assertEqual(cnst.columns, orig_cols)
        self.assertEqual(meta.columns, orig_cols)
        self.assertEqual(cnst.index_type, orig_type)
        self.assertEqual(meta.index_type, orig_type)
        self.assertEqual(cnst.comment, orig_comment)
        self.assertEqual(meta.comment, orig_comment)

        # Alter: local-remote mismatch
        class TestTable2(Table):
            id: Column = Column(Define.BIGINT())
            name: Column = Column(Define.VARCHAR(255))
            price: Column = Column(Define.DECIMAL(12, 2))
            pk: PrimaryKey = PrimaryKey("id", index_type="HASH", comment="Comment")

        class TestDatabase2(Database):
            tb: TestTable2 = TestTable2()

        db2 = TestDatabase2("test_db", self.get_pool())
        cnst = db2.tb.pk
        self.assertEqual(cnst.columns, ("id",))
        self.assertEqual(cnst.index_type, "HASH")
        self.assertEqual(cnst.comment, "Comment")
        meta = await cnst.aioShowMetadata()
        self.assertEqual(meta.columns, ("id", "price DESC"))
        self.assertEqual(meta.index_type, "BTREE")
        self.assertIsNone(meta.comment)
        await cnst.aioAlter(comment="Comment")
        self.assertEqual(cnst.columns, ("id",))
        self.assertEqual(cnst.index_type, "BTREE")
        self.assertEqual(cnst.comment, "Comment")
        self.assertTrue(cnst.visible)
        meta = await cnst.aioShowMetadata()
        self.assertEqual(meta.columns, ("id",))
        self.assertEqual(meta.index_type, "BTREE")
        self.assertEqual(meta.comment, "Comment")
        self.assertTrue(await cnst.aioDrop())

        # Sync from remote
        await db.tb.pk.aioAdd()

        class TestTable2(Table):
            id: Column = Column(Define.BIGINT())
            name: Column = Column(Define.VARCHAR(255))
            price: Column = Column(Define.DECIMAL(12, 2))
            pk: PrimaryKey = PrimaryKey("id", index_type="HASH", comment="Comment")

        class TestDatabase2(Database):
            tb: TestTable2 = TestTable2()

        db2 = TestDatabase2("test_db", self.get_pool())
        cnst = db2.tb.pk
        # . before sync
        self.assertEqual(cnst.columns, ("id",))
        self.assertEqual(cnst.index_type, "HASH")
        self.assertEqual(cnst.comment, "Comment")
        # . after sync
        await cnst.aioSyncFromRemote()
        self.assertEqual(cnst.columns, ("id", "price DESC"))
        self.assertEqual(cnst.index_type, "BTREE")
        self.assertIsNone(cnst.comment)
        meta = await cnst.aioShowMetadata()
        self.assertEqual(meta.columns, ("id", "price DESC"))
        self.assertEqual(meta.index_type, "BTREE")
        self.assertIsNone(meta.comment)

        # Sync to remote
        self.assertTrue(await cnst.aioDrop())
        self.assertTrue(await db.tb.pk.aioSyncToRemote())  # re-create

        class TestTable2(Table):
            id: Column = Column(Define.BIGINT())
            name: Column = Column(Define.VARCHAR(255))
            price: Column = Column(Define.DECIMAL(12, 2))
            pk: PrimaryKey = PrimaryKey("id", index_type="HASH", comment="Comment")

        class TestDatabase2(Database):
            tb: TestTable2 = TestTable2()

        db2 = TestDatabase2("test_db", self.get_pool())
        cnst = db2.tb.pk
        # . before sync
        meta = await cnst.aioShowMetadata()
        self.assertEqual(meta.columns, ("id", "price DESC"))
        self.assertEqual(meta.index_type, "BTREE")
        self.assertIsNone(meta.comment)
        # . after sync
        await cnst.aioSyncToRemote()
        self.assertEqual(cnst.columns, ("id",))
        self.assertEqual(cnst.index_type, "BTREE")
        self.assertEqual(cnst.comment, "Comment")
        meta = await cnst.aioShowMetadata()
        self.assertEqual(meta.columns, ("id",))
        self.assertEqual(meta.index_type, "BTREE")
        self.assertEqual(meta.comment, "Comment")
        self.assertTrue(await cnst.aioDrop())

        # Finished
        self.assertTrue(await db.aioDrop(True))
        self.log_ended("PRIMARY KEY BASIC SQL")

    async def test_foreign_key_basic_sql(self) -> None:
        self.log_start("FOREIGN KEY BASIC SQL")

        # Add & Exists & Drop
        class TestTable1(Table):
            id: Column = Column(Define.BIGINT())
            name: Column = Column(Define.VARCHAR(255))
            pk: PrimaryKey = PrimaryKey("id")
            uk: UniqueKey = UniqueKey("id", "name")

        class TestTable2(Table):
            id: Column = Column(Define.BIGINT())
            name_id: Column = Column(Define.BIGINT())
            name: Column = Column(Define.VARCHAR(255))
            price: Column = Column(Define.DECIMAL(12, 2))
            pk: PrimaryKey = PrimaryKey("id")
            fk: ForeignKey = ForeignKey(("name_id", "name"), "tb1", ("id", "name"))

        class TestTable3(Table):
            id: Column = Column(Define.BIGINT())
            name: Column = Column(Define.VARCHAR(255))
            pk: PrimaryKey = PrimaryKey("id")
            uk: UniqueKey = UniqueKey("id", "name")

        class TestDatabase(Database):
            tb1: TestTable1 = TestTable1()
            tb2: TestTable2 = TestTable2()
            tb3: TestTable3 = TestTable3()

        db = TestDatabase("test_db", self.get_pool())
        self.assertTrue(await db.aioDrop(True))
        self.assertTrue(await db.aioCreate())
        self.assertTrue(await db.tb1.aioCreate())
        self.assertTrue(await db.tb2.aioCreate())
        self.assertTrue(await db.tb3.aioCreate())
        cnst = db.tb2.fk
        self.assertTrue(await cnst.aioExists())
        self.assertTrue(await cnst.aioDrop())
        self.assertFalse(await cnst.aioExists())
        self.assertTrue(await cnst.aioAdd())
        self.assertTrue(await cnst.aioExists())
        self.assertTrue(await db.aioDrop())

        # Show metadata
        with self.assertRaises(sqlerr.OperationalError):
            await db.tb2.fk.aioShowMetadata()
        self.assertTrue(await db.aioCreate())
        self.assertTrue(await db.tb1.aioCreate())
        self.assertTrue(await db.tb2.aioCreate())
        self.assertTrue(await db.tb3.aioCreate())
        cnst = db.tb2.fk
        meta = await cnst.aioShowMetadata()
        self.assertEqual(meta.db_name, db.db_name)
        self.assertEqual(meta.tb_name, db.tb2.tb_name)
        self.assertEqual(meta.constraint_name, cnst.symbol)
        self.assertTrue(meta.enforced)
        self.assertEqual(meta.columns, ("name_id", "name"))
        self.assertEqual(meta.reference_table, "tb1")
        self.assertEqual(meta.reference_columns, ("id", "name"))
        self.assertEqual(meta.on_delete, "NO ACTION")
        self.assertEqual(meta.on_update, "NO ACTION")

        # Alter
        cnst = db.tb2.fk
        # . default state
        orig_cols = cnst.columns
        orig_ref_tb = cnst.reference_table
        orig_ref_cols = cnst.reference_columns
        orig_on_delete = cnst.on_delete
        orig_on_update = cnst.on_update
        # . columns: only
        await cnst.aioAlter(columns="name_id", reference_columns="id")
        meta = await cnst.aioShowMetadata()
        self.assertEqual(cnst.columns, ("name_id",))
        self.assertEqual(meta.columns, ("name_id",))
        self.assertEqual(cnst.reference_table, orig_ref_tb)
        self.assertEqual(meta.reference_table, orig_ref_tb)
        self.assertEqual(cnst.reference_columns, ("id",))
        self.assertEqual(meta.reference_columns, ("id",))
        self.assertEqual(cnst.on_delete, orig_on_delete)
        self.assertEqual(meta.on_delete, orig_on_delete)
        self.assertEqual(cnst.on_update, orig_on_update)
        self.assertEqual(meta.on_update, orig_on_update)
        # . table: only
        await cnst.aioAlter(reference_table="tb3")
        meta = await cnst.aioShowMetadata()
        self.assertEqual(cnst.columns, ("name_id",))
        self.assertEqual(meta.columns, ("name_id",))
        self.assertEqual(cnst.reference_table, "tb3")
        self.assertEqual(meta.reference_table, "tb3")
        self.assertEqual(cnst.reference_columns, ("id",))
        self.assertEqual(meta.reference_columns, ("id",))
        self.assertEqual(cnst.on_delete, orig_on_delete)
        self.assertEqual(meta.on_delete, orig_on_delete)
        self.assertEqual(cnst.on_update, orig_on_update)
        self.assertEqual(meta.on_update, orig_on_update)
        # . actions: only
        await cnst.aioAlter(on_delete="CASCADE", on_update="RESTRICT")
        meta = await cnst.aioShowMetadata()
        self.assertEqual(cnst.columns, ("name_id",))
        self.assertEqual(meta.columns, ("name_id",))
        self.assertEqual(cnst.reference_table, "tb3")
        self.assertEqual(meta.reference_table, "tb3")
        self.assertEqual(cnst.reference_columns, ("id",))
        self.assertEqual(meta.reference_columns, ("id",))
        self.assertEqual(cnst.on_delete, "CASCADE")
        self.assertEqual(meta.on_delete, "CASCADE")
        self.assertEqual(cnst.on_update, "RESTRICT")
        self.assertEqual(cnst.on_update, "RESTRICT")
        # . mixed
        await cnst.aioAlter(
            orig_cols, orig_ref_tb, orig_ref_cols, orig_on_delete, orig_on_update
        )
        meta = await cnst.aioShowMetadata()
        self.assertEqual(cnst.columns, orig_cols)
        self.assertEqual(meta.columns, orig_cols)
        self.assertEqual(cnst.reference_table, orig_ref_tb)
        self.assertEqual(meta.reference_table, orig_ref_tb)
        self.assertEqual(cnst.reference_columns, orig_ref_cols)
        self.assertEqual(meta.reference_columns, orig_ref_cols)
        self.assertEqual(cnst.on_delete, orig_on_delete)
        self.assertEqual(meta.on_delete, orig_on_delete)
        self.assertEqual(cnst.on_update, orig_on_update)
        self.assertEqual(meta.on_update, orig_on_update)

        # Alter: local-remote mismatch
        class TestTable_x1(Table):
            id: Column = Column(Define.BIGINT())
            name: Column = Column(Define.VARCHAR(255))
            pk: PrimaryKey = PrimaryKey("id", "name")

        class TestTable_x2(Table):
            id: Column = Column(Define.BIGINT())
            name_id: Column = Column(Define.BIGINT())
            name: Column = Column(Define.VARCHAR(255))
            price: Column = Column(Define.DECIMAL(12, 2))
            pk: PrimaryKey = PrimaryKey("id")
            fk: ForeignKey = ForeignKey(
                ("name_id", "name"),
                "tb1",
                ("id",),
                on_delete="CASCADE",
                on_update="RESTRICT",
            )

        class TestTable_x3(Table):
            id: Column = Column(Define.BIGINT())
            name: Column = Column(Define.VARCHAR(255))
            pk: PrimaryKey = PrimaryKey("id", "name")

        class TestDatabase2(Database):
            tb1: TestTable_x1 = TestTable_x1()
            tb2: TestTable_x2 = TestTable_x2()
            tb3: TestTable_x3 = TestTable_x3()

        db2 = TestDatabase2("test_db", self.get_pool())
        cnst = db2.tb2.fk
        self.assertEqual(cnst.columns, ("name_id", "name"))
        self.assertEqual(cnst.reference_table, "tb1")
        self.assertEqual(cnst.reference_columns, ("id",))
        self.assertEqual(cnst.on_delete, "CASCADE")
        self.assertEqual(cnst.on_update, "RESTRICT")
        meta = await cnst.aioShowMetadata()
        self.assertEqual(meta.columns, ("name_id", "name"))
        self.assertEqual(meta.reference_table, "tb1")
        self.assertEqual(meta.reference_columns, ("id", "name"))
        self.assertEqual(meta.on_delete, "NO ACTION")
        self.assertEqual(meta.on_update, "NO ACTION")
        await cnst.aioAlter(
            reference_table="tb3",
            reference_columns=("id", "name"),
            on_delete="NO ACTION",
        )
        self.assertEqual(cnst.columns, ("name_id", "name"))
        self.assertEqual(cnst.reference_table, "tb3")
        self.assertEqual(cnst.reference_columns, ("id", "name"))
        self.assertEqual(cnst.on_delete, "NO ACTION")
        self.assertEqual(cnst.on_update, "RESTRICT")
        meta = await cnst.aioShowMetadata()
        self.assertEqual(meta.columns, ("name_id", "name"))
        self.assertEqual(meta.reference_table, "tb3")
        self.assertEqual(meta.reference_columns, ("id", "name"))
        self.assertEqual(meta.on_delete, "NO ACTION")
        self.assertEqual(meta.on_update, "RESTRICT")
        self.assertTrue(await cnst.aioDrop())

        # Sync from remote
        await db.tb2.fk.aioAdd()

        class TestTable_x1(Table):
            id: Column = Column(Define.BIGINT())
            name: Column = Column(Define.VARCHAR(255))
            pk: PrimaryKey = PrimaryKey("id", "name")

        class TestTable_x2(Table):
            id: Column = Column(Define.BIGINT())
            name_id: Column = Column(Define.BIGINT())
            name: Column = Column(Define.VARCHAR(255))
            price: Column = Column(Define.DECIMAL(12, 2))
            pk: PrimaryKey = PrimaryKey("id")
            fk: ForeignKey = ForeignKey(
                ("name_id"),
                "tb3",
                ("id",),
                on_delete="CASCADE",
                on_update="RESTRICT",
            )

        class TestDatabase2(Database):
            tb1: TestTable_x1 = TestTable_x1()
            tb2: TestTable_x2 = TestTable_x2()

        db2 = TestDatabase2("test_db", self.get_pool())
        cnst = db2.tb2.fk
        # . before sync
        self.assertEqual(cnst.columns, ("name_id",))
        self.assertEqual(cnst.reference_table, "tb3")
        self.assertEqual(cnst.reference_columns, ("id",))
        self.assertEqual(cnst.on_delete, "CASCADE")
        self.assertEqual(cnst.on_update, "RESTRICT")
        # . after sync
        await cnst.aioSyncFromRemote()
        self.assertEqual(cnst.columns, ("name_id", "name"))
        self.assertEqual(cnst.reference_table, "tb1")
        self.assertEqual(cnst.reference_columns, ("id", "name"))
        self.assertEqual(cnst.on_delete, "NO ACTION")
        self.assertEqual(cnst.on_update, "NO ACTION")
        meta = await cnst.aioShowMetadata()
        self.assertEqual(meta.columns, ("name_id", "name"))
        self.assertEqual(meta.reference_table, "tb1")
        self.assertEqual(meta.reference_columns, ("id", "name"))
        self.assertEqual(meta.on_delete, "NO ACTION")
        self.assertEqual(meta.on_update, "NO ACTION")

        # Sync to remote
        self.assertTrue(await cnst.aioDrop())
        self.assertTrue(await db.tb2.fk.aioSyncToRemote())  # re-create

        class TestTable_x1(Table):
            id: Column = Column(Define.BIGINT())
            name: Column = Column(Define.VARCHAR(255))
            pk: PrimaryKey = PrimaryKey("id", "name")

        class TestTable_x2(Table):
            id: Column = Column(Define.BIGINT())
            name_id: Column = Column(Define.BIGINT())
            name: Column = Column(Define.VARCHAR(255))
            price: Column = Column(Define.DECIMAL(12, 2))
            pk: PrimaryKey = PrimaryKey("id")
            fk: ForeignKey = ForeignKey(
                ("name_id"),
                "tb3",
                ("id",),
                on_delete="CASCADE",
                on_update="RESTRICT",
            )

        class TestTable_x3(Table):
            id: Column = Column(Define.BIGINT())
            name: Column = Column(Define.VARCHAR(255))
            pk: PrimaryKey = PrimaryKey("id", "name")

        class TestDatabase2(Database):
            tb1: TestTable_x1 = TestTable_x1()
            tb2: TestTable_x2 = TestTable_x2()
            tb3: TestTable_x3 = TestTable_x3()

        db2 = TestDatabase2("test_db", self.get_pool())
        cnst = db2.tb2.fk
        # . before sync
        meta = await cnst.aioShowMetadata()
        self.assertEqual(meta.columns, ("name_id", "name"))
        self.assertEqual(meta.reference_table, "tb1")
        self.assertEqual(meta.reference_columns, ("id", "name"))
        self.assertEqual(meta.on_delete, "NO ACTION")
        self.assertEqual(meta.on_update, "NO ACTION")
        # . after sync
        await cnst.aioSyncToRemote()
        self.assertEqual(cnst.columns, ("name_id",))
        self.assertEqual(cnst.reference_table, "tb3")
        self.assertEqual(cnst.reference_columns, ("id",))
        self.assertEqual(cnst.on_delete, "CASCADE")
        self.assertEqual(cnst.on_update, "RESTRICT")
        meta = await cnst.aioShowMetadata()
        self.assertEqual(meta.columns, ("name_id",))
        self.assertEqual(meta.reference_table, "tb3")
        self.assertEqual(meta.reference_columns, ("id",))
        self.assertEqual(meta.on_delete, "CASCADE")
        self.assertEqual(meta.on_update, "RESTRICT")
        self.assertTrue(await cnst.aioDrop())

        # Finished
        self.assertTrue(await db.aioDrop(True))
        self.log_ended("FOREIGN KEY BASIC SQL")

    async def test_check_constraint_basic_sql(self) -> None:
        self.log_start("CHECK CONSTRAINT BASIC SQL")

        # Add & Exists & Drop
        class TestTable(Table):
            id: Column = Column(Define.BIGINT())
            price: Column = Column(Define.DECIMAL(12, 2))
            pk: PrimaryKey = PrimaryKey("id")
            ck1: Check = Check("price > 0")
            ck2: Check = Check("price < 1000")

        class TestDatabase(Database):
            tb: TestTable = TestTable()

        db = TestDatabase("test_db", self.get_pool())
        self.assertTrue(await db.aioDrop(True))
        self.assertTrue(await db.aioCreate())
        self.assertTrue(await db.tb.aioCreate())

        for cnst in (db.tb.ck1, db.tb.ck2):
            self.assertTrue(await cnst.aioExists())
            self.assertTrue(await cnst.aioDrop())
            self.assertFalse(await cnst.aioExists())
            self.assertTrue(await cnst.aioAdd())
            self.assertTrue(await cnst.aioExists())
        self.assertTrue(await db.aioDrop())

        # Show metadata
        with self.assertRaises(sqlerr.OperationalError):
            await db.tb.ck1.aioShowMetadata()
        self.assertTrue(await db.aioCreate())
        self.assertTrue(await db.tb.aioCreate())
        cnst = db.tb.ck1
        meta = await cnst.aioShowMetadata()
        self.assertEqual(meta.db_name, db.db_name)
        self.assertEqual(meta.tb_name, db.tb.tb_name)
        self.assertEqual(meta.constraint_name, cnst.symbol)
        self.assertTrue(meta.enforced)
        self.assertEqual(meta.expression, "`price` > 0")

        # Alter
        # . default state
        orig_expr = cnst.expression
        orig_enforced = cnst.enforced
        # . expression: only
        await cnst.aioAlter(expression="price > 1")
        meta = await cnst.aioShowMetadata()
        self.assertEqual(cnst.expression, "`price` > 1")
        self.assertEqual(meta.expression, "`price` > 1")
        self.assertEqual(cnst.enforced, orig_enforced)
        self.assertEqual(meta.enforced, orig_enforced)
        # . enforced: only
        await cnst.aioAlter(enforced=False)
        meta = await cnst.aioShowMetadata()
        self.assertEqual(cnst.expression, "`price` > 1")
        self.assertEqual(meta.expression, "`price` > 1")
        self.assertEqual(cnst.enforced, False)
        self.assertEqual(meta.enforced, False)
        self.assertTrue(await cnst.aioSetEnforced(True))
        self.assertTrue((await cnst.aioShowMetadata()).enforced)
        self.assertTrue(await cnst.aioSetEnforced(False))
        self.assertFalse((await cnst.aioShowMetadata()).enforced)
        # . mixed
        await cnst.aioAlter(orig_expr, orig_enforced)
        meta = await cnst.aioShowMetadata()
        self.assertEqual(cnst.expression, orig_expr)
        self.assertEqual(meta.expression, orig_expr)
        self.assertEqual(cnst.enforced, orig_enforced)
        self.assertEqual(meta.enforced, orig_enforced)

        # Alter: local-remote mismatch
        class TestTable2(Table):
            id: Column = Column(Define.BIGINT())
            price: Column = Column(Define.DECIMAL(12, 2))
            pk: PrimaryKey = PrimaryKey("id")
            ck1: Check = Check("price > 1", enforced=True)

        class TestDatabase2(Database):
            tb: TestTable2 = TestTable2()

        db2 = TestDatabase2("test_db", self.get_pool())
        cnst = db2.tb.ck1
        self.assertEqual(cnst.expression, "price > 1")
        self.assertEqual(cnst.enforced, True)
        meta = await cnst.aioShowMetadata()
        self.assertEqual(meta.expression, "`price` > 0")
        self.assertEqual(meta.enforced, True)
        await cnst.aioAlter(expression="price > 2", enforced=False)
        self.assertEqual(cnst.expression, "`price` > 2")
        self.assertEqual(cnst.enforced, False)
        meta = await cnst.aioShowMetadata()
        self.assertEqual(meta.expression, "`price` > 2")
        self.assertEqual(meta.enforced, False)
        self.assertTrue(await cnst.aioDrop())

        # Sync from remote
        await db.tb.ck1.aioAdd()

        class TestTable2(Table):
            id: Column = Column(Define.BIGINT())
            price: Column = Column(Define.DECIMAL(12, 2))
            pk: PrimaryKey = PrimaryKey("id")
            ck1: Check = Check("price > 1", enforced=False)

        class TestDatabase2(Database):
            tb: TestTable2 = TestTable2()

        db2 = TestDatabase2("test_db", self.get_pool())
        cnst = db2.tb.ck1
        # . before sync
        self.assertEqual(cnst.expression, "price > 1")
        self.assertEqual(cnst.enforced, False)
        # . after sync
        await cnst.aioSyncFromRemote()
        self.assertEqual(cnst.expression, "`price` > 0")
        self.assertEqual(cnst.enforced, True)
        meta = await cnst.aioShowMetadata()
        self.assertEqual(meta.expression, "`price` > 0")
        self.assertEqual(meta.enforced, True)

        # Sync to remote
        self.assertTrue(await cnst.aioDrop())
        self.assertTrue(await db.tb.ck1.aioSyncToRemote())  # re-create

        class TestTable2(Table):
            id: Column = Column(Define.BIGINT())
            price: Column = Column(Define.DECIMAL(12, 2))
            pk: PrimaryKey = PrimaryKey("id")
            ck1: Check = Check("price > 1", enforced=False)

        class TestDatabase2(Database):
            tb: TestTable2 = TestTable2()

        db2 = TestDatabase2("test_db", self.get_pool())
        cnst = db2.tb.ck1
        # . before sync
        meta = await cnst.aioShowMetadata()
        self.assertEqual(meta.expression, "`price` > 0")
        self.assertEqual(meta.enforced, True)
        # . after sync
        await cnst.aioSyncToRemote()
        self.assertEqual(cnst.expression, "`price` > 1")
        self.assertEqual(cnst.enforced, False)
        meta = await cnst.aioShowMetadata()
        self.assertEqual(meta.expression, "`price` > 1")
        self.assertEqual(meta.enforced, False)
        self.assertTrue(await cnst.aioDrop())

        # Finished
        self.assertTrue(await db.aioDrop(True))
        self.log_ended("CHECK CONSTRAINT BASIC SQL")

    async def test_mixed_constraints_sql(self) -> None:
        self.log_start("MIXED CONSTRAINTS SQL (symbol conflicts)")

        class TestTable1(Table):
            id: Column = Column(Define.BIGINT())
            name: Column = Column(Define.VARCHAR(255))
            price: Column = Column(Define.DECIMAL(12, 2))
            pk: PrimaryKey = PrimaryKey(
                "id", "name", index_type="HASH", comment="Comment"
            )
            uk: UniqueKey = UniqueKey(
                "name", "price", index_type="HASH", comment="Comment"
            )
            uk2: UniqueKey = UniqueKey("name")
            ck: Check = Check("price > 0")

        class TestTable2(Table):
            id: Column = Column(Define.BIGINT())
            name: Column = Column(Define.VARCHAR(255))
            price: Column = Column(Define.DECIMAL(12, 2))
            pk: PrimaryKey = PrimaryKey(
                "id", "name", index_type="HASH", comment="Comment"
            )
            uk: UniqueKey = UniqueKey(
                "name", "price", index_type="HASH", comment="Comment"
            )
            ck: Check = Check("price > 0")
            fk: ForeignKey = ForeignKey(("id", "name"), "tb1", ("id", "name"))

        class TestTable3(Table):
            id: Column = Column(Define.BIGINT())
            name: Column = Column(Define.VARCHAR(255))
            price: Column = Column(Define.DECIMAL(12, 2))
            pk: PrimaryKey = PrimaryKey(
                "id", "name", index_type="HASH", comment="Comment"
            )
            uk: UniqueKey = UniqueKey(
                "name", "price", index_type="HASH", comment="Comment"
            )
            ck: Check = Check("price > 0")
            fk: ForeignKey = ForeignKey("name", "tb1", "name")

        class TestDatabase(Database):
            tb1: TestTable1 = TestTable1()
            tb2: TestTable2 = TestTable2()
            tb3: TestTable3 = TestTable3()

        db = TestDatabase("test_db", self.get_pool())
        self.assertTrue(await db.aioDrop(True))
        self.assertTrue(await db.aioInitialize())
        self.assertEqual(
            await db.tb1.pk.aioShowConstraintSymbols(),
            ("PRIMARY", "uk", "uk2", "tb1_ck"),
        )

        # Finished
        self.assertTrue(await db.aioDrop(True))
        self.log_ended("MIXED CONSTRAINTS SQL (symbol conflicts)")


if __name__ == "__main__":
    HOST = "localhost"
    PORT = 3306
    USER = "root"
    PSWD = "Password_123456"

    for case in (
        TestConstraintDefinition,
        TestConstraintCopy,
        TestConstraintSyncSQL,
        TestConstraintAyncSQL,
    ):
        test_case = case(HOST, PORT, USER, PSWD)
        if not iscoroutinefunction(test_case.test_all):
            test_case.test_all()
        else:
            asyncio.run(test_case.test_all())
