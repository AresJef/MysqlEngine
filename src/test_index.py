import asyncio, time, unittest
from inspect import iscoroutinefunction
from sqlcycli import errors as sqlerr, Pool, Connection
from mysqlengine import errors
from mysqlengine.table import Table
from mysqlengine.database import Database
from mysqlengine.column import Define, Definition, Column
from mysqlengine.index import Index, Index, FullTextIndex, Indexes


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

    def setup_index(self, idx: Index, name: str = "idx") -> Index:
        idx.set_name(name)
        idx.setup("tb1", "db1", "utf8", None, self.get_pool())
        return idx

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


class TestIndexDefinition(TestCase):
    name = "Index Definition"

    def test_all(self) -> None:
        self.test_index()
        self.test_fulltext_index()
        self.test_indexs()

    def test_index(self) -> None:
        self.log_start("Index")

        args = [
            # string
            (
                ["col1", "col2", "col3"],
                "INDEX idx (col1, col2, col3)",
            ),
            # Column
            (
                ["col_int", "col_float", "col_date"],
                "INDEX idx (col_int, col_float, col_date)",
            ),
            # Columns
            (
                ["col_tinyint", "col_smallint", "col_mediumint", "col_int"],
                "INDEX idx (col_tinyint, col_smallint, col_mediumint, col_int)",
            ),
        ]
        for cols, cmp in args:
            idx = self.setup_index(Index(*cols), "idx")
            self.assertEqual(idx._gen_definition_sql(), cmp)

        # Index Type
        idx = self.setup_index(Index("col1", "col2", "col3", index_type="HASH"))
        self.assertEqual(
            idx._gen_definition_sql(),
            "INDEX idx (col1, col2, col3) USING HASH",
        )

        # Comment
        idx = self.setup_index(Index("col1", "col2", "col3", comment="Comment"))
        self.assertEqual(
            idx._gen_definition_sql(),
            "INDEX idx (col1, col2, col3) COMMENT 'Comment'",
        )

        # Visible
        idx = self.setup_index(Index("col1", "col2", "col3", visible=False))
        self.assertEqual(
            idx._gen_definition_sql(),
            "INDEX idx (col1, col2, col3) INVISIBLE",
        )

        # Mixed
        idx = self.setup_index(
            Index("col1", "col2", "col3", comment="Comment", visible=False)
        )
        self.assertEqual(
            idx._gen_definition_sql(),
            "INDEX idx (col1, col2, col3) COMMENT 'Comment' INVISIBLE",
        )

        # Error
        with self.assertRaises(errors.IndexDefinitionError):
            Index()
        with self.assertRaises(errors.IndexDefinitionError):
            Index(None)
        with self.assertRaises(errors.IndexDefinitionError):
            Index("col", index_type="xHASH")
        with self.assertRaises(errors.IndexDefinitionError):
            Index("col", None)
        with self.assertRaises(errors.IndexDefinitionError):
            Index("col", [None])
        with self.assertRaises(errors.IndexDefinitionError):
            Index("col", [1])

        self.log_ended("Index")

    def test_fulltext_index(self) -> None:
        self.log_start("FullText Index")

        args = [
            # string
            (
                ["col1", "col2", "col3"],
                "FULLTEXT INDEX idx (col1, col2, col3)",
            ),
            # Column
            (
                ["col_int", "col_float", "col_date"],
                "FULLTEXT INDEX idx (col_int, col_float, col_date)",
            ),
            # Columns
            (
                ["col_tinyint", "col_smallint", "col_mediumint", "col_int"],
                "FULLTEXT INDEX idx (col_tinyint, col_smallint, col_mediumint, col_int)",
            ),
        ]
        for cols, cmp in args:
            idx = self.setup_index(FullTextIndex(*cols))
            self.assertEqual(idx._gen_definition_sql(), cmp)

        idx = self.setup_index(FullTextIndex("col1", "col2", "col3", parser="Parser"))
        self.assertEqual(
            idx._gen_definition_sql(),
            "FULLTEXT INDEX idx (col1, col2, col3) WITH PARSER Parser",
        )

        idx = self.setup_index(FullTextIndex("col1", "col2", "col3", comment="Comment"))
        self.assertEqual(
            idx._gen_definition_sql(),
            "FULLTEXT INDEX idx (col1, col2, col3) COMMENT 'Comment'",
        )

        idx = self.setup_index(FullTextIndex("col1", "col2", "col3", visible=False))
        self.assertEqual(
            idx._gen_definition_sql(),
            "FULLTEXT INDEX idx (col1, col2, col3) INVISIBLE",
        )

        idx = self.setup_index(
            FullTextIndex("col1", "col2", "col3", comment="Comment", visible=False)
        )
        self.assertEqual(
            idx._gen_definition_sql(),
            "FULLTEXT INDEX idx (col1, col2, col3) COMMENT 'Comment' INVISIBLE",
        )

        # Error
        with self.assertRaises(errors.IndexDefinitionError):
            FullTextIndex()
        with self.assertRaises(errors.IndexDefinitionError):
            FullTextIndex(None)
        with self.assertRaises(errors.IndexDefinitionError):
            FullTextIndex("col", None)
        with self.assertRaises(errors.IndexDefinitionError):
            FullTextIndex("col", [None])
        with self.assertRaises(errors.IndexDefinitionError):
            FullTextIndex("col", [1])

        self.log_ended("FullText Index")

    def test_indexs(self) -> None:
        self.log_start("Indexes")

        cols = ["cola", "colb", "colc"]
        idx = self.setup_index(Index(*cols), "idx")
        fidx = self.setup_index(FullTextIndex(*cols), "fidx")
        idxs = Indexes(*[idx, fidx])
        self.assertEqual(len(idxs.search_name("idx")), 1)
        self.assertEqual(len(idxs.search_name("idx", exact=False)), 2)
        self.assertEqual(len(idxs.search_type("index")), 1)
        self.assertEqual(len(idxs.search_type(FullTextIndex)), 1)
        self.assertEqual(len(idxs.search_type([Index, FullTextIndex])), 2)
        self.assertEqual(len(idxs.search_type("FULLTEXT INDEX")), 1)
        self.assertEqual(len(idxs.search_type("INDEX", exact=False)), 2)
        self.assertTrue(idxs.issubset(["idx", idxs["fidx"], idxs]))
        self.assertFalse(idxs.issubset("idxx"))
        self.assertEqual(len(idxs.filter("idxx")), 0)
        self.assertEqual(len(idxs.filter("idx")), 1)
        self.assertEqual(len(idxs.filter(["idx", fidx])), 2)
        self.assertEqual(len(idxs.filter(idxs)), len(idxs))
        self.assertIs(idxs.get("idx"), idx)
        self.assertIs(idxs.get(idx), idx)
        self.assertIs(idxs.get("idxx"), None)
        for i in idxs:
            self.assertIsInstance(i, Index)
        self.assertIn("idx", idxs)
        self.assertIn(fidx, idxs)
        self.assertNotIn("idxx", idxs)
        idxs._gen_definition_sql()

        # Conflicts
        with self.assertRaises(errors.IndexArgumentError):
            Indexes(idx, idx)

        self.log_ended("Indexes")


class TestIndexCopy(TestCase):
    name = "Index Copy"

    def test_all(self) -> None:
        self.log_start("COPY")

        class TestTable(Table):
            id: Column = Column(Define.BIGINT())
            name: Column = Column(Define.VARCHAR(255))
            price: Column = Column(Define.DECIMAL(12, 2))
            idx: Index = Index("name", "price DESC")
            ftidx: FullTextIndex = FullTextIndex("name(10)")

        class TestDatabase(Database):
            tb1: TestTable = TestTable()
            tb2: TestTable = TestTable()

        db = TestDatabase("test_db", self.get_pool())
        self.assertIsNot(db.tb1.idx, db.tb2.idx)
        self.assertIsNot(db.tb1.ftidx, db.tb2.ftidx)
        self.assertIsNot(db.tb1.indexes["idx"], db.tb2.indexes["idx"])
        self.assertIsNot(db.tb1.indexes["ftidx"], db.tb2.indexes["ftidx"])

        self.log_ended("COPY")


class TestIndexSyncSQL(TestCase):
    name: str = "Index Sync SQL"

    def test_all(self) -> None:
        self.test_index_basic_sql()
        self.test_fulltext_index_basic_sql()

    def test_index_basic_sql(self) -> None:
        self.log_start("INDEX BASIC SQL")

        # Add & Exists & Drop
        class TestTable(Table):
            id: Column = Column(Define.BIGINT())
            name: Column = Column(Define.VARCHAR(255))
            price: Column = Column(Define.DECIMAL(12, 2))
            idx1: Index = Index("name", "price DESC")
            idx2: Index = Index("name(10)")
            idx3: Index = Index("(ABS(price) + 1)")
            idx4: Index = Index("(ABS(price) + 1) DESC")

        class TestDatabase(Database):
            tb: TestTable = TestTable()

        db = TestDatabase("test_db", self.get_pool())
        self.assertTrue(db.Drop(True))
        self.assertTrue(db.Create())
        self.assertTrue(db.tb.Create())

        for idx in db.tb.indexes:
            self.assertTrue(idx.Exists())
            self.assertTrue(idx.Drop())
            self.assertFalse(idx.Exists())
            self.assertTrue(idx.Add())
            self.assertTrue(idx.Exists())
        self.assertTrue(db.Drop())

        # Show metadata
        with self.assertRaises(sqlerr.OperationalError):
            db.tb.idx1.ShowMetadata()
        self.assertTrue(db.Create())
        self.assertTrue(db.tb.Create())
        for idx in db.tb.indexes:
            meta = idx.ShowMetadata()
            self.assertEqual(meta.db_name, db.db_name)
            self.assertEqual(meta.tb_name, db.tb.tb_name)
            self.assertEqual(meta.index_name, idx.name)
            self.assertEqual(meta.index_type, "BTREE")
            self.assertFalse(meta.unique)
            self.assertIsNone(meta.comment)
            self.assertTrue(meta.visible)
            if idx is db.tb.idx1:
                self.assertEqual(len(meta.columns), 2)
            else:
                self.assertEqual(len(meta.columns), 1)

        # Alter
        idx = db.tb.idx1
        # . default state
        orig_cols = idx.columns
        orig_type = idx.index_type
        orig_comment = idx.comment
        orig_visible = idx.visible
        # . columns: only
        idx.Alter(columns="name")
        meta = idx.ShowMetadata()
        self.assertEqual(idx.columns, ("name",))
        self.assertEqual(meta.columns, ("name",))
        self.assertEqual(idx.index_type, orig_type)
        self.assertEqual(meta.index_type, orig_type)
        self.assertEqual(idx.comment, orig_comment)
        self.assertEqual(meta.comment, orig_comment)
        self.assertEqual(idx.visible, orig_visible)
        self.assertEqual(meta.visible, orig_visible)
        # . index type: only (Should not change)
        idx.Alter(index_type="HASH")
        meta = idx.ShowMetadata()
        self.assertEqual(idx.columns, ("name",))
        self.assertEqual(meta.columns, ("name",))
        self.assertEqual(idx.index_type, orig_type)
        self.assertEqual(meta.index_type, orig_type)
        self.assertEqual(idx.comment, orig_comment)
        self.assertEqual(meta.comment, orig_comment)
        self.assertEqual(idx.visible, orig_visible)
        self.assertEqual(meta.visible, orig_visible)
        # . comment: only
        idx.Alter(comment="描述")
        meta = idx.ShowMetadata()
        self.assertEqual(idx.columns, ("name",))
        self.assertEqual(meta.columns, ("name",))
        self.assertEqual(idx.index_type, orig_type)
        self.assertEqual(meta.index_type, orig_type)
        self.assertEqual(idx.comment, "描述")
        self.assertEqual(meta.comment, "描述")
        self.assertEqual(idx.visible, orig_visible)
        self.assertEqual(meta.visible, orig_visible)
        # . visible: only
        idx.Alter(visible=False)
        meta = idx.ShowMetadata()
        self.assertEqual(idx.columns, ("name",))
        self.assertEqual(meta.columns, ("name",))
        self.assertEqual(idx.index_type, orig_type)
        self.assertEqual(meta.index_type, orig_type)
        self.assertEqual(idx.comment, "描述")
        self.assertEqual(meta.comment, "描述")
        self.assertEqual(idx.visible, False)
        self.assertEqual(meta.visible, False)
        self.assertTrue(idx.SetVisible(True))
        self.assertTrue(idx.ShowMetadata().visible)
        self.assertTrue(idx.SetVisible(False))
        self.assertFalse(idx.ShowMetadata().visible)
        # . mixed
        idx.Alter(orig_cols, orig_type, "", orig_visible)
        meta = idx.ShowMetadata()
        self.assertEqual(idx.columns, orig_cols)
        self.assertEqual(meta.columns, orig_cols)
        self.assertEqual(idx.index_type, orig_type)
        self.assertEqual(meta.index_type, orig_type)
        self.assertEqual(idx.comment, orig_comment)
        self.assertEqual(meta.comment, orig_comment)
        self.assertEqual(idx.visible, orig_visible)
        self.assertEqual(meta.visible, orig_visible)

        # Alter: local-remote mismatch
        class TestTable2(Table):
            id: Column = Column(Define.BIGINT())
            name: Column = Column(Define.VARCHAR(255))
            price: Column = Column(Define.DECIMAL(12, 2))
            idx1: Index = Index("name", index_type="HASH", visible=False)

        class TestDatabase2(Database):
            tb: TestTable2 = TestTable2()

        db2 = TestDatabase2("test_db", self.get_pool())
        idx = db2.tb.idx1
        self.assertEqual(idx.columns, ("name",))
        self.assertEqual(idx.index_type, "HASH")
        self.assertIsNone(idx.comment)
        self.assertFalse(idx.visible)
        meta = idx.ShowMetadata()
        self.assertEqual(meta.columns, ("name", "price DESC"))
        self.assertEqual(meta.index_type, "BTREE")
        self.assertIsNone(meta.comment)
        self.assertTrue(meta.visible)
        idx.Alter(comment="Comment", visible=True)
        self.assertEqual(idx.columns, ("name",))
        self.assertEqual(idx.index_type, "BTREE")
        self.assertEqual(idx.comment, "Comment")
        self.assertTrue(idx.visible)
        meta = idx.ShowMetadata()
        self.assertEqual(meta.columns, ("name",))
        self.assertEqual(meta.index_type, "BTREE")
        self.assertEqual(meta.comment, "Comment")
        self.assertTrue(meta.visible)
        self.assertTrue(idx.Drop())

        # Sync from remote
        db.tb.idx1.Add()

        class TestTable2(Table):
            id: Column = Column(Define.BIGINT())
            name: Column = Column(Define.VARCHAR(255))
            price: Column = Column(Define.DECIMAL(12, 2))
            idx1: Index = Index(
                "name", index_type="HASH", comment="Comment", visible=False
            )

        class TestDatabase2(Database):
            tb: TestTable2 = TestTable2()

        db2 = TestDatabase2("test_db", self.get_pool())
        idx = db2.tb.idx1

        # . before sync
        self.assertEqual(idx.columns, ("name",))
        self.assertEqual(idx.index_type, "HASH")
        self.assertEqual(idx.comment, "Comment")
        self.assertFalse(idx.visible)
        # . after sync
        idx.SyncFromRemote()
        self.assertEqual(idx.columns, ("name", "price DESC"))
        self.assertEqual(idx.index_type, "BTREE")
        self.assertIsNone(idx.comment)
        self.assertTrue(idx.visible)
        meta = idx.ShowMetadata()
        self.assertEqual(meta.columns, ("name", "price DESC"))
        self.assertEqual(meta.index_type, "BTREE")
        self.assertIsNone(meta.comment)
        self.assertTrue(meta.visible)

        # Sync to remote
        self.assertTrue(idx.Drop())
        self.assertTrue(db.tb.idx1.SyncToRemote())  # re-create

        class TestTable2(Table):
            id: Column = Column(Define.BIGINT())
            name: Column = Column(Define.VARCHAR(255))
            price: Column = Column(Define.DECIMAL(12, 2))
            idx1: Index = Index(
                "name", index_type="HASH", comment="Comment", visible=False
            )

        class TestDatabase2(Database):
            tb: TestTable2 = TestTable2()

        db2 = TestDatabase2("test_db", self.get_pool())
        idx = db2.tb.idx1
        # . before sync
        meta = idx.ShowMetadata()
        self.assertEqual(meta.columns, ("name", "price DESC"))
        self.assertEqual(meta.index_type, "BTREE")
        self.assertIsNone(meta.comment)
        self.assertTrue(meta.visible)
        # . after sync
        idx.SyncToRemote()
        self.assertEqual(idx.columns, ("name",))
        self.assertEqual(idx.index_type, "BTREE")
        self.assertEqual(idx.comment, "Comment")
        self.assertFalse(idx.visible)
        meta = idx.ShowMetadata()
        self.assertEqual(meta.columns, ("name",))
        self.assertEqual(meta.index_type, "BTREE")
        self.assertEqual(meta.comment, "Comment")
        self.assertFalse(meta.visible)
        self.assertTrue(idx.Drop())

        # Finished
        self.assertTrue(db.Drop(True))
        self.log_ended("INDEX BASIC SQL")

    def test_fulltext_index_basic_sql(self) -> None:
        self.log_start("FULLTEXT INDEX BASIC SQL")

        # Add & Exists & Drop
        class TestTable(Table):
            id: Column = Column(Define.BIGINT())
            title1: Column = Column(Define.TEXT())
            title2: Column = Column(Define.TEXT())
            ftidx: FullTextIndex = FullTextIndex("title1")

        class TestDatabase(Database):
            tb: TestTable = TestTable()

        db = TestDatabase("test_db", self.get_pool())
        self.assertTrue(db.Drop(True))
        self.assertTrue(db.Create())
        self.assertTrue(db.tb.Create())

        for idx in db.tb.indexes:
            self.assertTrue(idx.Exists())
            self.assertTrue(idx.Drop())
            self.assertFalse(idx.Exists())
            idx.Add()
            self.assertTrue(idx.Exists())

        # Show metadata
        for idx in db.tb.indexes:
            meta = idx.ShowMetadata()
            self.assertEqual(meta.db_name, db.db_name)
            self.assertEqual(meta.tb_name, db.tb.tb_name)
            self.assertEqual(meta.index_name, idx.name)
            self.assertEqual(meta.index_type, "FULLTEXT")
            self.assertFalse(meta.unique)
            self.assertIsNone(meta.comment)
            self.assertTrue(meta.visible)
            self.assertEqual(len(meta.columns), 1)

        # Alter
        idx = db.tb.ftidx
        # . default state
        orig_cols = idx.columns
        orig_type = idx.index_type
        orig_parser = idx.parser
        orig_comment = idx.comment
        orig_visible = idx.visible
        # . columns: only
        idx.Alter(columns="title2")
        meta = idx.ShowMetadata()
        self.assertEqual(idx.columns, ("title2",))
        self.assertEqual(meta.columns, ("title2",))
        self.assertEqual(idx.index_type, orig_type)
        self.assertEqual(meta.index_type, orig_type)
        self.assertEqual(idx.parser, orig_parser)
        self.assertEqual(idx.comment, orig_comment)
        self.assertEqual(meta.comment, orig_comment)
        self.assertEqual(idx.visible, orig_visible)
        self.assertEqual(meta.visible, orig_visible)
        # . parser & comment
        idx.Alter(parser="ngram", comment="描述")
        meta = idx.ShowMetadata()
        self.assertEqual(idx.columns, ("title2",))
        self.assertEqual(meta.columns, ("title2",))
        self.assertEqual(idx.index_type, orig_type)
        self.assertEqual(meta.index_type, orig_type)
        self.assertEqual(idx.parser, "ngram")
        self.assertEqual(idx.comment, "描述")
        self.assertEqual(meta.comment, "描述")
        self.assertEqual(idx.visible, orig_visible)
        self.assertEqual(meta.visible, orig_visible)
        # . visible: only
        idx.Alter(visible=False)
        meta = idx.ShowMetadata()
        self.assertEqual(idx.columns, ("title2",))
        self.assertEqual(meta.columns, ("title2",))
        self.assertEqual(idx.index_type, orig_type)
        self.assertEqual(meta.index_type, orig_type)
        self.assertEqual(idx.parser, "ngram")
        self.assertEqual(idx.comment, "描述")
        self.assertEqual(meta.comment, "描述")
        self.assertEqual(idx.visible, False)
        self.assertEqual(meta.visible, False)
        self.assertTrue(idx.SetVisible(True))
        self.assertTrue(idx.ShowMetadata().visible)
        self.assertTrue(idx.SetVisible(False))
        self.assertFalse(idx.ShowMetadata().visible)
        # . mixed
        idx.Alter(orig_cols, "", "", orig_visible)
        meta = idx.ShowMetadata()
        self.assertEqual(idx.columns, orig_cols)
        self.assertEqual(meta.columns, orig_cols)
        self.assertEqual(idx.index_type, orig_type)
        self.assertEqual(meta.index_type, orig_type)
        self.assertEqual(idx.parser, orig_parser)
        self.assertEqual(idx.comment, orig_comment)
        self.assertEqual(meta.comment, orig_comment)
        self.assertEqual(idx.visible, orig_visible)
        self.assertEqual(meta.visible, orig_visible)

        # Alter: local-remote mismatch
        class TestTable2(Table):
            id: Column = Column(Define.BIGINT())
            title1: Column = Column(Define.TEXT())
            title2: Column = Column(Define.TEXT())
            ftidx: FullTextIndex = FullTextIndex(
                "title2", parser="ngram", visible=False
            )

        class TestDatabase2(Database):
            tb: TestTable2 = TestTable2()

        db2 = TestDatabase2("test_db", self.get_pool())
        idx = db2.tb.ftidx
        self.assertEqual(idx.columns, ("title2",))
        self.assertEqual(idx.parser, "ngram")
        self.assertIsNone(idx.comment)
        self.assertFalse(idx.visible)
        meta = idx.ShowMetadata()
        self.assertEqual(meta.columns, ("title1",))
        self.assertIsNone(meta.parser)
        self.assertIsNone(meta.comment)
        self.assertTrue(meta.visible)
        idx.Alter(comment="Comment", visible=True)
        self.assertEqual(idx.columns, ("title2",))
        self.assertEqual(idx.parser, "ngram")
        self.assertEqual(idx.comment, "Comment")
        self.assertTrue(idx.visible)
        meta = idx.ShowMetadata()
        self.assertEqual(meta.columns, ("title2",))
        self.assertEqual(meta.comment, "Comment")
        self.assertTrue(meta.visible)
        self.assertTrue(idx.Drop())

        # Sync from remote
        db.tb.ftidx.Add()

        class TestTable2(Table):
            id: Column = Column(Define.BIGINT())
            title1: Column = Column(Define.TEXT())
            title2: Column = Column(Define.TEXT())
            ftidx: FullTextIndex = FullTextIndex(
                "title2", parser="ngram", comment="Comment", visible=False
            )

        class TestDatabase2(Database):
            tb: TestTable2 = TestTable2()

        db2 = TestDatabase2("test_db", self.get_pool())
        idx = db2.tb.ftidx

        # . before sync
        self.assertEqual(idx.columns, ("title2",))
        self.assertEqual(idx.parser, "ngram")
        self.assertEqual(idx.comment, "Comment")
        self.assertFalse(idx.visible)
        # . after sync
        idx.SyncFromRemote()
        self.assertEqual(idx.columns, ("title1",))
        self.assertIsNone(idx.parser)
        self.assertIsNone(idx.comment)
        self.assertTrue(idx.visible)
        meta = idx.ShowMetadata()
        self.assertEqual(meta.columns, ("title1",))
        self.assertIsNone(meta.parser)
        self.assertIsNone(meta.comment)
        self.assertTrue(meta.visible)

        # Sync to remote
        self.assertTrue(idx.Drop())
        self.assertTrue(db.tb.ftidx.SyncToRemote())  # re-create

        class TestTable2(Table):
            id: Column = Column(Define.BIGINT())
            title1: Column = Column(Define.TEXT())
            title2: Column = Column(Define.TEXT())
            ftidx: FullTextIndex = FullTextIndex(
                "title2", parser="ngram", comment="Comment", visible=False
            )

        class TestDatabase2(Database):
            tb: TestTable2 = TestTable2()

        db2 = TestDatabase2("test_db", self.get_pool())
        idx = db2.tb.ftidx
        # . before sync
        meta = idx.ShowMetadata()
        self.assertEqual(meta.columns, ("title1",))
        self.assertIsNone(meta.comment)
        self.assertTrue(meta.visible)
        # . after sync
        idx.SyncToRemote()
        self.assertEqual(idx.columns, ("title2",))
        self.assertEqual(idx.parser, "ngram")
        self.assertEqual(idx.comment, "Comment")
        self.assertFalse(idx.visible)
        meta = idx.ShowMetadata()
        self.assertEqual(meta.columns, ("title2",))
        self.assertEqual(meta.parser, "ngram")
        self.assertEqual(meta.comment, "Comment")
        self.assertFalse(meta.visible)
        self.assertTrue(idx.Drop())

        # Finished
        self.assertTrue(db.Drop(True))
        self.log_ended("FULLTEXT INDEX BASIC SQL")


class TestIndexAsyncSQL(TestCase):
    name: str = "Index Async SQL"

    async def test_all(self) -> None:
        await self.test_index_basic_sql()
        await self.test_fulltext_index_basic_sql()

    async def test_index_basic_sql(self) -> None:
        self.log_start("INDEX BASIC SQL")

        # Add & Exists & Drop
        class TestTable(Table):
            id: Column = Column(Define.BIGINT())
            name: Column = Column(Define.VARCHAR(255))
            price: Column = Column(Define.DECIMAL(12, 2))
            idx1: Index = Index("name", "price DESC")
            idx2: Index = Index("name(10)")
            idx3: Index = Index("(ABS(price) + 1)")
            idx4: Index = Index("(ABS(price) + 1) DESC")

        class TestDatabase(Database):
            tb: TestTable = TestTable()

        db = TestDatabase("test_db", self.get_pool())
        self.assertTrue(await db.aioDrop(True))
        self.assertTrue(await db.aioCreate())
        self.assertTrue(await db.tb.aioCreate())

        for idx in db.tb.indexes:
            self.assertTrue(await idx.aioExists())
            self.assertTrue(await idx.aioDrop())
            self.assertFalse(await idx.aioExists())
            self.assertTrue(await idx.aioAdd())
            self.assertTrue(await idx.aioExists())
        self.assertTrue(await db.aioDrop())

        # Show metadata
        with self.assertRaises(sqlerr.OperationalError):
            await db.tb.idx1.aioShowMetadata()
        self.assertTrue(await db.aioCreate())
        self.assertTrue(await db.tb.aioCreate())
        for idx in db.tb.indexes:
            meta = await idx.aioShowMetadata()
            self.assertEqual(meta.db_name, db.db_name)
            self.assertEqual(meta.tb_name, db.tb.tb_name)
            self.assertEqual(meta.index_name, idx.name)
            self.assertEqual(meta.index_type, "BTREE")
            self.assertFalse(meta.unique)
            self.assertIsNone(meta.comment)
            self.assertTrue(meta.visible)
            if idx is db.tb.idx1:
                self.assertEqual(len(meta.columns), 2)
            else:
                self.assertEqual(len(meta.columns), 1)

        # Alter
        idx = db.tb.idx1
        # . default state
        orig_cols = idx.columns
        orig_type = idx.index_type
        orig_comment = idx.comment
        orig_visible = idx.visible
        # . columns: only
        await idx.aioAlter(columns="name")
        meta = await idx.aioShowMetadata()
        self.assertEqual(idx.columns, ("name",))
        self.assertEqual(meta.columns, ("name",))
        self.assertEqual(idx.index_type, orig_type)
        self.assertEqual(meta.index_type, orig_type)
        self.assertEqual(idx.comment, orig_comment)
        self.assertEqual(meta.comment, orig_comment)
        self.assertEqual(idx.visible, orig_visible)
        self.assertEqual(meta.visible, orig_visible)
        # . index type: only (Should not change)
        await idx.aioAlter(index_type="HASH")
        meta = await idx.aioShowMetadata()
        self.assertEqual(idx.columns, ("name",))
        self.assertEqual(meta.columns, ("name",))
        self.assertEqual(idx.index_type, orig_type)
        self.assertEqual(meta.index_type, orig_type)
        self.assertEqual(idx.comment, orig_comment)
        self.assertEqual(meta.comment, orig_comment)
        self.assertEqual(idx.visible, orig_visible)
        self.assertEqual(meta.visible, orig_visible)
        # . comment: only
        await idx.aioAlter(comment="描述")
        meta = await idx.aioShowMetadata()
        self.assertEqual(idx.columns, ("name",))
        self.assertEqual(meta.columns, ("name",))
        self.assertEqual(idx.index_type, orig_type)
        self.assertEqual(meta.index_type, orig_type)
        self.assertEqual(idx.comment, "描述")
        self.assertEqual(meta.comment, "描述")
        self.assertEqual(idx.visible, orig_visible)
        self.assertEqual(meta.visible, orig_visible)
        # . visible: only
        await idx.aioAlter(visible=False)
        meta = await idx.aioShowMetadata()
        self.assertEqual(idx.columns, ("name",))
        self.assertEqual(meta.columns, ("name",))
        self.assertEqual(idx.index_type, orig_type)
        self.assertEqual(meta.index_type, orig_type)
        self.assertEqual(idx.comment, "描述")
        self.assertEqual(meta.comment, "描述")
        self.assertEqual(idx.visible, False)
        self.assertEqual(meta.visible, False)
        self.assertTrue(await idx.aioSetVisible(True))
        self.assertTrue((await idx.aioShowMetadata()).visible)
        self.assertTrue(await idx.aioSetVisible(False))
        self.assertFalse((await idx.aioShowMetadata()).visible)
        # . mixed
        await idx.aioAlter(orig_cols, orig_type, "", orig_visible)
        meta = await idx.aioShowMetadata()
        self.assertEqual(idx.columns, orig_cols)
        self.assertEqual(meta.columns, orig_cols)
        self.assertEqual(idx.index_type, orig_type)
        self.assertEqual(meta.index_type, orig_type)
        self.assertEqual(idx.comment, orig_comment)
        self.assertEqual(meta.comment, orig_comment)
        self.assertEqual(idx.visible, orig_visible)
        self.assertEqual(meta.visible, orig_visible)

        # Alter: local-remote mismatch
        class TestTable2(Table):
            id: Column = Column(Define.BIGINT())
            name: Column = Column(Define.VARCHAR(255))
            price: Column = Column(Define.DECIMAL(12, 2))
            idx1: Index = Index("name", index_type="HASH", visible=False)

        class TestDatabase2(Database):
            tb: TestTable2 = TestTable2()

        db2 = TestDatabase2("test_db", self.get_pool())
        idx = db2.tb.idx1
        self.assertEqual(idx.columns, ("name",))
        self.assertEqual(idx.index_type, "HASH")
        self.assertIsNone(idx.comment)
        self.assertFalse(idx.visible)
        meta = await idx.aioShowMetadata()
        self.assertEqual(meta.columns, ("name", "price DESC"))
        self.assertEqual(meta.index_type, "BTREE")
        self.assertIsNone(meta.comment)
        self.assertTrue(meta.visible)
        await idx.aioAlter(comment="Comment", visible=True)
        self.assertEqual(idx.columns, ("name",))
        self.assertEqual(idx.index_type, "BTREE")
        self.assertEqual(idx.comment, "Comment")
        self.assertTrue(idx.visible)
        meta = await idx.aioShowMetadata()
        self.assertEqual(meta.columns, ("name",))
        self.assertEqual(meta.index_type, "BTREE")
        self.assertEqual(meta.comment, "Comment")
        self.assertTrue(meta.visible)
        self.assertTrue(await idx.aioDrop())

        # Sync from remote
        db.tb.idx1.Add()

        class TestTable2(Table):
            id: Column = Column(Define.BIGINT())
            name: Column = Column(Define.VARCHAR(255))
            price: Column = Column(Define.DECIMAL(12, 2))
            idx1: Index = Index(
                "name", index_type="HASH", comment="Comment", visible=False
            )

        class TestDatabase2(Database):
            tb: TestTable2 = TestTable2()

        db2 = TestDatabase2("test_db", self.get_pool())
        idx = db2.tb.idx1

        # . before sync
        self.assertEqual(idx.columns, ("name",))
        self.assertEqual(idx.index_type, "HASH")
        self.assertEqual(idx.comment, "Comment")
        self.assertFalse(idx.visible)
        # . after sync
        await idx.aioSyncFromRemote()
        self.assertEqual(idx.columns, ("name", "price DESC"))
        self.assertEqual(idx.index_type, "BTREE")
        self.assertIsNone(idx.comment)
        self.assertTrue(idx.visible)
        meta = await idx.aioShowMetadata()
        self.assertEqual(meta.columns, ("name", "price DESC"))
        self.assertEqual(meta.index_type, "BTREE")
        self.assertIsNone(meta.comment)
        self.assertTrue(meta.visible)

        # Sync to remote
        self.assertTrue(await idx.aioDrop())
        self.assertTrue(await db.tb.idx1.aioSyncToRemote())  # re-create

        class TestTable2(Table):
            id: Column = Column(Define.BIGINT())
            name: Column = Column(Define.VARCHAR(255))
            price: Column = Column(Define.DECIMAL(12, 2))
            idx1: Index = Index(
                "name", index_type="HASH", comment="Comment", visible=False
            )

        class TestDatabase2(Database):
            tb: TestTable2 = TestTable2()

        db2 = TestDatabase2("test_db", self.get_pool())
        idx = db2.tb.idx1
        # . before sync
        meta = await idx.aioShowMetadata()
        self.assertEqual(meta.columns, ("name", "price DESC"))
        self.assertEqual(meta.index_type, "BTREE")
        self.assertIsNone(meta.comment)
        self.assertTrue(meta.visible)
        # . after sync
        await idx.aioSyncToRemote()
        self.assertEqual(idx.columns, ("name",))
        self.assertEqual(idx.index_type, "BTREE")
        self.assertEqual(idx.comment, "Comment")
        self.assertFalse(idx.visible)
        meta = await idx.aioShowMetadata()
        self.assertEqual(meta.columns, ("name",))
        self.assertEqual(meta.index_type, "BTREE")
        self.assertEqual(meta.comment, "Comment")
        self.assertFalse(meta.visible)
        self.assertTrue(await db.aioDrop())

        # Finished
        self.assertTrue(await db.aioDrop(True))
        self.log_ended("INDEX BASIC SQL")

    async def test_fulltext_index_basic_sql(self) -> None:
        self.log_start("FULLTEXT INDEX BASIC SQL")

        # Add & Exists & Drop
        class TestTable(Table):
            id: Column = Column(Define.BIGINT())
            title1: Column = Column(Define.TEXT())
            title2: Column = Column(Define.TEXT())
            ftidx: FullTextIndex = FullTextIndex("title1")

        class TestDatabase(Database):
            tb: TestTable = TestTable()

        db = TestDatabase("test_db", self.get_pool())
        self.assertTrue(await db.aioDrop(True))
        self.assertTrue(await db.aioCreate())
        self.assertTrue(await db.tb.aioCreate())

        for idx in db.tb.indexes:
            self.assertTrue(await idx.aioExists())
            self.assertTrue(await idx.aioDrop())
            self.assertFalse(await idx.aioExists())
            await idx.aioAdd()
            self.assertTrue(await idx.aioExists())

        # Show metadata
        for idx in db.tb.indexes:
            meta = await idx.aioShowMetadata()
            self.assertEqual(meta.db_name, db.db_name)
            self.assertEqual(meta.tb_name, db.tb.tb_name)
            self.assertEqual(meta.index_name, idx.name)
            self.assertEqual(meta.index_type, "FULLTEXT")
            self.assertFalse(meta.unique)
            self.assertIsNone(meta.comment)
            self.assertTrue(meta.visible)
            self.assertEqual(len(meta.columns), 1)

        # Alter
        idx = db.tb.ftidx
        # . default state
        orig_cols = idx.columns
        orig_type = idx.index_type
        orig_parser = idx.parser
        orig_comment = idx.comment
        orig_visible = idx.visible
        # . columns: only
        await idx.aioAlter(columns="title2")
        meta = await idx.aioShowMetadata()
        self.assertEqual(idx.columns, ("title2",))
        self.assertEqual(meta.columns, ("title2",))
        self.assertEqual(idx.index_type, orig_type)
        self.assertEqual(meta.index_type, orig_type)
        self.assertEqual(idx.parser, orig_parser)
        self.assertEqual(idx.comment, orig_comment)
        self.assertEqual(meta.comment, orig_comment)
        self.assertEqual(idx.visible, orig_visible)
        self.assertEqual(meta.visible, orig_visible)
        # . parser & comment
        await idx.aioAlter(parser="ngram", comment="描述")
        meta = await idx.aioShowMetadata()
        self.assertEqual(idx.columns, ("title2",))
        self.assertEqual(meta.columns, ("title2",))
        self.assertEqual(idx.index_type, orig_type)
        self.assertEqual(meta.index_type, orig_type)
        self.assertEqual(idx.parser, "ngram")
        self.assertEqual(idx.comment, "描述")
        self.assertEqual(meta.comment, "描述")
        self.assertEqual(idx.visible, orig_visible)
        self.assertEqual(meta.visible, orig_visible)
        # . visible: only
        await idx.aioAlter(visible=False)
        meta = await idx.aioShowMetadata()
        self.assertEqual(idx.columns, ("title2",))
        self.assertEqual(meta.columns, ("title2",))
        self.assertEqual(idx.index_type, orig_type)
        self.assertEqual(meta.index_type, orig_type)
        self.assertEqual(idx.parser, "ngram")
        self.assertEqual(idx.comment, "描述")
        self.assertEqual(meta.comment, "描述")
        self.assertEqual(idx.visible, False)
        self.assertEqual(meta.visible, False)
        self.assertTrue(await idx.aioSetVisible(True))
        self.assertTrue((await idx.aioShowMetadata()).visible)
        self.assertTrue(await idx.aioSetVisible(False))
        self.assertFalse((await idx.aioShowMetadata()).visible)
        # . mixed
        await idx.aioAlter(orig_cols, "", "", orig_visible)
        meta = await idx.aioShowMetadata()
        self.assertEqual(idx.columns, orig_cols)
        self.assertEqual(meta.columns, orig_cols)
        self.assertEqual(idx.index_type, orig_type)
        self.assertEqual(meta.index_type, orig_type)
        self.assertEqual(idx.parser, orig_parser)
        self.assertEqual(idx.comment, orig_comment)
        self.assertEqual(meta.comment, orig_comment)
        self.assertEqual(idx.visible, orig_visible)
        self.assertEqual(meta.visible, orig_visible)

        # Alter: local-remote mismatch
        class TestTable2(Table):
            id: Column = Column(Define.BIGINT())
            title1: Column = Column(Define.TEXT())
            title2: Column = Column(Define.TEXT())
            ftidx: FullTextIndex = FullTextIndex(
                "title2", parser="ngram", visible=False
            )

        class TestDatabase2(Database):
            tb: TestTable2 = TestTable2()

        db2 = TestDatabase2("test_db", self.get_pool())
        idx = db2.tb.ftidx
        self.assertEqual(idx.columns, ("title2",))
        self.assertEqual(idx.parser, "ngram")
        self.assertIsNone(idx.comment)
        self.assertFalse(idx.visible)
        meta = await idx.aioShowMetadata()
        self.assertEqual(meta.columns, ("title1",))
        self.assertIsNone(meta.parser)
        self.assertIsNone(meta.comment)
        self.assertTrue(meta.visible)
        await idx.aioAlter(comment="Comment", visible=True)
        self.assertEqual(idx.columns, ("title2",))
        self.assertEqual(idx.parser, "ngram")
        self.assertEqual(idx.comment, "Comment")
        self.assertTrue(idx.visible)
        meta = await idx.aioShowMetadata()
        self.assertEqual(meta.columns, ("title2",))
        self.assertEqual(meta.comment, "Comment")
        self.assertTrue(meta.visible)
        self.assertTrue(await idx.aioDrop())

        # Sync from remote
        db.tb.ftidx.Add()

        class TestTable2(Table):
            id: Column = Column(Define.BIGINT())
            title1: Column = Column(Define.TEXT())
            title2: Column = Column(Define.TEXT())
            ftidx: FullTextIndex = FullTextIndex(
                "title2", parser="ngram", comment="Comment", visible=False
            )

        class TestDatabase2(Database):
            tb: TestTable2 = TestTable2()

        db2 = TestDatabase2("test_db", self.get_pool())
        idx = db2.tb.ftidx

        # . before sync
        self.assertEqual(idx.columns, ("title2",))
        self.assertEqual(idx.parser, "ngram")
        self.assertEqual(idx.comment, "Comment")
        self.assertFalse(idx.visible)
        # . after sync
        await idx.aioSyncFromRemote()
        self.assertEqual(idx.columns, ("title1",))
        self.assertIsNone(idx.parser)
        self.assertIsNone(idx.comment)
        self.assertTrue(idx.visible)
        meta = await idx.aioShowMetadata()
        self.assertEqual(meta.columns, ("title1",))
        self.assertIsNone(meta.parser)
        self.assertIsNone(meta.comment)
        self.assertTrue(meta.visible)

        # Sync to remote
        self.assertTrue(await idx.aioDrop())
        self.assertTrue(await db.tb.ftidx.aioSyncToRemote())  # re-create

        class TestTable2(Table):
            id: Column = Column(Define.BIGINT())
            title1: Column = Column(Define.TEXT())
            title2: Column = Column(Define.TEXT())
            ftidx: FullTextIndex = FullTextIndex(
                "title2", parser="ngram", comment="Comment", visible=False
            )

        class TestDatabase2(Database):
            tb: TestTable2 = TestTable2()

        db2 = TestDatabase2("test_db", self.get_pool())
        idx = db2.tb.ftidx
        # . before sync
        meta = await idx.aioShowMetadata()
        self.assertEqual(meta.columns, ("title1",))
        self.assertIsNone(meta.comment)
        self.assertTrue(meta.visible)
        # . after sync
        await idx.aioSyncToRemote()
        self.assertEqual(idx.columns, ("title2",))
        self.assertEqual(idx.parser, "ngram")
        self.assertEqual(idx.comment, "Comment")
        self.assertFalse(idx.visible)
        meta = await idx.aioShowMetadata()
        self.assertEqual(meta.columns, ("title2",))
        self.assertEqual(meta.parser, "ngram")
        self.assertEqual(meta.comment, "Comment")
        self.assertFalse(meta.visible)
        self.assertTrue(await idx.aioDrop())

        # Finished
        self.assertTrue(await db.aioDrop(True))
        self.log_ended("FULLTEXT INDEX BASIC SQL")


if __name__ == "__main__":
    HOST = "localhost"
    PORT = 3306
    USER = "root"
    PSWD = "Password_123456"

    for case in (
        TestIndexDefinition,
        TestIndexCopy,
        TestIndexSyncSQL,
        TestIndexAsyncSQL,
    ):
        test_case = case(HOST, PORT, USER, PSWD)
        if not iscoroutinefunction(test_case.test_all):
            test_case.test_all()
        else:
            asyncio.run(test_case.test_all())
