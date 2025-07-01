import asyncio
from inspect import iscoroutinefunction
import time, unittest
from sqlcycli import errors as sqlerr, sqlfunc, Pool, Connection
from mysqlengine import errors
from mysqlengine.table import Table
from mysqlengine.index import Index
from mysqlengine.database import Database
from mysqlengine.partition import Partitioning
from mysqlengine.constraint import PrimaryKey, ForeignKey

from mysqlengine.column import Define, Definition, Column, GeneratedColumn


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


class TestDatabaseDefinition(TestCase):
    name: str = "Database Definition"

    def test_all(self) -> None:
        self.test_options()
        self.test_create_statement()
        self.test_database_tables()

    def test_options(self) -> None:
        self.log_start("OPTIONS")

        # . name
        with self.assertRaises(errors.DatabaseDefinitionError):
            Database("db_name", self.get_pool())

        # . pool
        with self.assertRaises(errors.DatabaseArgumentError):
            Database("db1", 1)

        # . charset
        # fmt: off
        self.assertEqual(Database("db1", self.get_pool()).charset.name, "utf8mb4")
        self.assertEqual(Database("db1", self.get_pool(), charset="utf8").charset.name, "utf8mb4")
        with self.assertRaises(errors.DatabaseDefinitionError):
            Database("db1", self.get_pool(), charset="utf8x")
        with self.assertRaises(errors.DatabaseDefinitionError):
            Database("db1", self.get_pool(), charset="latin1")
        self.assertEqual(Database("db1", self.get_pool(), collate="utf8mb4_bin").charset.collation, "utf8mb4_bin")
        with self.assertRaises(errors.DatabaseDefinitionError):
            Database("db1", self.get_pool(), charset="utf8mb4_bin")
        self.assertEqual(Database("db1", self.get_pool(), "utf8", "utf8mb4_bin").charset.collation, "utf8mb4_bin")
        # fmt: on

        # . encryption
        self.assertIsNone(Database("db1", self.get_pool()).encryption)
        self.assertTrue(Database("db1", self.get_pool(), encryption=True).encryption)
        self.assertTrue(Database("db1", self.get_pool(), encryption=1).encryption)
        self.assertTrue(Database("db1", self.get_pool(), encryption="Y").encryption)
        self.assertFalse(Database("db1", self.get_pool(), encryption=False).encryption)
        self.assertFalse(Database("db1", self.get_pool(), encryption=0).encryption)
        self.assertFalse(Database("db1", self.get_pool(), encryption="n").encryption)
        with self.assertRaises(errors.DatabaseDefinitionError):
            Database("db1", self.get_pool(), encryption="x")

        self.log_ended("OPTIONS")

    def test_create_statement(self) -> None:
        self.log_start("CREATE STATEMENT")

        self.assertEqual(
            Database("db1", self.get_pool())._gen_create_sql(False),
            "CREATE DATABASE db1 CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;",
        )
        self.assertEqual(
            Database("db1", self.get_pool())._gen_create_sql(True),
            "CREATE DATABASE IF NOT EXISTS db1 CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;",
        )
        self.assertEqual(
            Database(
                "db1",
                self.get_pool(),
                collate="utf8mb4_bin",
            )._gen_create_sql(False),
            "CREATE DATABASE db1 CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;",
        )
        self.assertEqual(
            Database(
                "db1",
                self.get_pool(),
                collate="utf8mb4_bin",
            )._gen_create_sql(True),
            "CREATE DATABASE IF NOT EXISTS db1 CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;",
        )
        self.assertEqual(
            Database(
                "db1",
                self.get_pool(),
                charset="utf8",
                collate="utf8mb4_bin",
            )._gen_create_sql(False),
            "CREATE DATABASE db1 CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;",
        )
        self.assertEqual(
            Database(
                "db1",
                self.get_pool(),
                charset="utf8",
                collate="utf8mb4_bin",
            )._gen_create_sql(True),
            "CREATE DATABASE IF NOT EXISTS db1 CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;",
        )
        self.assertEqual(
            Database("db1", self.get_pool(), encryption=True)._gen_create_sql(False),
            "CREATE DATABASE db1 CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci ENCRYPTION 'Y';",
        )
        self.assertEqual(
            Database("db1", self.get_pool(), encryption=False)._gen_create_sql(False),
            "CREATE DATABASE db1 CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci ENCRYPTION 'N';",
        )
        self.assertEqual(
            Database("db1", self.get_pool(), encryption=True)._gen_create_sql(True),
            "CREATE DATABASE IF NOT EXISTS db1 CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci ENCRYPTION 'Y';",
        )

        self.log_ended("CREATE STATEMENT")

    def test_database_tables(self) -> None:
        self.log_start("DATABASE TABLES")

        # Database 1
        class User(Table):
            id: Column = Column(Define.INT())
            name: Column = Column(Define.VARCHAR(255))

        class Post(Table):
            id: Column = Column(Define.INT())
            user_id: Column = Column(Define.INT())
            title: Column = Column(Define.VARCHAR(255))
            fk_key: ForeignKey = ForeignKey("user_id", "user", "id")

        class Database1(Database):
            user: User = User()
            post: Post = Post()

        db1 = Database1("test_db1", self.get_pool())
        self.assertEqual(
            repr(db1),
            "<Database (\n\t"
            "name='test_db1',\n\t"
            "charset='utf8mb4',\n\t"
            "collate='utf8mb4_general_ci',\n\t"
            "encryption=None,\n\t"
            "read_only=False,\n\t"
            "tables=<Tables {'user': TABLE, 'post': TABLE}>\n"
            ")>",
        )
        self.assertEqual(str(db1), "test_db1")
        self.assertEqual(len(db1.tables), 2)
        self.assertTrue(db1.user in db1)
        self.assertTrue(db1["user"] in db1)
        self.assertIs(db1.post, db1["post"])
        self.assertIs(db1.post, db1.tables["post"])
        self.assertIs(db1.post.id, db1["post"]["id"])
        self.assertIs(db1.post.id, db1.tables["post"].columns["id"])
        for i in db1:
            self.assertIsInstance(i, Table)

        # Database 2
        class Product(Table):
            id: Column = Column(Define.INT())
            name: Column = Column(Define.VARCHAR(255))

        class Price(Table):
            id: Column = Column(Define.INT())
            product_id: Column = Column(Define.INT())
            price: Column = Column(Define.DECIMAL())
            fk_key: ForeignKey = ForeignKey("product_id", "product", "id")

        class Database2(Database):
            product: Product = Product()
            price: Price = Price()

        db2 = Database2("test_db2", self.get_pool())
        self.assertEqual(
            repr(db2),
            "<Database (\n\t"
            "name='test_db2',\n\t"
            "charset='utf8mb4',\n\t"
            "collate='utf8mb4_general_ci',\n\t"
            "encryption=None,\n\t"
            "read_only=False,\n\t"
            "tables=<Tables {'product': TABLE, 'price': TABLE}>\n"
            ")>",
        )
        self.assertEqual(str(db2), "test_db2")
        self.assertEqual(len(db2.tables), 2)
        self.assertTrue(db2.product in db2)
        self.assertTrue(db2["product"] in db2)
        self.assertIs(db2.price, db2["price"])
        self.assertIs(db2.price, db2.tables["price"])
        self.assertIs(db2.price.id, db2["price"]["id"])
        self.assertIs(db2.price.id, db2.tables["price"].columns["id"])
        for i in db2:
            self.assertIsInstance(i, Table)

        self.log_ended("DATABASE TABLES")


class TestDatabaseCopy(TestCase):
    name = "Database Copy"

    def test_all(self) -> None:
        self.log_start("COPY")

        class TestTable(Table):
            id: Column = Column(Define.BIGINT())

        class TestDatabase(Database):
            tb: TestTable = TestTable()

        db1 = TestDatabase("db1", self.get_pool())
        db2 = TestDatabase("db2", self.get_pool())
        db3 = TestDatabase("db3", self.get_pool())
        self.assertIsNot(db1, db2)
        self.assertIsNot(db1, db3)
        self.assertNotEqual(db1.db_name, db2.db_name)
        self.assertNotEqual(db1.db_name, db3.db_name)
        self.assertNotEqual(str(db1.tb), str(db2.tb))
        self.assertNotEqual(str(db1.tb), str(db3.tb))

        self.log_ended("COPY")


class TestDatabaseBaseClass(TestCase):
    name: str = "Database Base Class"

    def test_all(self) -> None:
        self.log_start("BASE CLASS ERROR")

        class TestTable(Table):
            id: Column = Column(Define.INT())

        class TestDataBase(Database):
            tb: TestTable = TestTable()
            db: Database = Database("test_db", self.get_pool())

        with self.assertRaises(errors.DatabaseDefinitionError):
            TestDataBase("test_db", self.get_pool())

        class TestTable(Table):
            id: Column = Column(Define.INT())
            db: Database = Database("test_db", self.get_pool())

        class TestDataBase(Database):
            tb: TestTable = TestTable()

        with self.assertRaises(errors.TableDefinitionError):
            TestDataBase("test_db", self.get_pool())

        self.log_ended("BASE CLASS ERROR")


class TestDatabaseSyncSQL(TestCase):
    name: str = "Database Sync SQL"

    def test_all(self) -> None:
        self.test_database_basic_sql()
        self.test_database_sync_from_remote()
        self.test_database_initialize()

    def test_database_basic_sql(self) -> None:
        self.log_start("DATABASE BASIC SQL")

        # Test Database
        class User(Table):
            id: Column = Column(Define.INT())
            name: Column = Column(Define.VARCHAR(255))

        class Post(Table):
            id: Column = Column(Define.INT())
            user_id: Column = Column(Define.INT())
            title: Column = Column(Define.VARCHAR(255))

        class TestDatabase(Database):
            user: User = User()
            post: Post = Post()

        db = TestDatabase(
            "test_db",
            self.get_pool(),
            charset="utf8mb4",
            collate="utf8mb4_general_ci",
            encryption=False,
        )

        # Create & Exists & Drop
        self.assertTrue(db.Drop(True))
        self.assertFalse(db.Exists())
        self.assertTrue(db.Create())
        self.assertTrue(db.Exists())
        with self.assertRaises(sqlerr.OperationalError):
            db.Create()
        self.assertTrue(db.Drop())
        with self.assertRaises(sqlerr.OperationalError):
            db.Drop()

        # Show databases / metadata
        with self.assertRaises(sqlerr.OperationalError):
            db.ShowMetadata()
        self.assertTrue(db.Create())
        self.assertIn(db.db_name, db.ShowDatabases())
        meta = db.ShowMetadata()
        self.assertEqual(meta["SCHEMA_NAME"], db.db_name)
        self.assertEqual(meta.db_name, db.db_name)
        self.assertEqual(meta["DEFAULT_CHARACTER_SET_NAME"], "utf8mb4")
        self.assertEqual(meta.charset.name, "utf8mb4")
        self.assertEqual(meta["DEFAULT_COLLATION_NAME"], "utf8mb4_general_ci")
        self.assertEqual(meta.charset.collation, "utf8mb4_general_ci")
        self.assertIsNone(meta["SQL_PATH"])
        self.assertEqual(meta["DEFAULT_ENCRYPTION"], "NO")
        self.assertFalse(meta.encryption)
        self.assertTrue(db.Drop())
        self.assertNotIn(db.db_name, db.ShowDatabases())

        # Alter
        self.assertTrue(db.Create())
        # . no alteration
        self.assertFalse(db.Alter())
        # . alter charset
        self.assertEqual(db.charset.name, "utf8mb4")
        self.assertTrue(db.Alter(charset="utf8mb3"))
        self.assertEqual(db.charset.name, "utf8mb3")
        self.assertEqual(db.ShowMetadata().charset.name, "utf8mb3")
        with self.assertRaises(errors.DatabaseDefinitionError):
            db.Alter(charset="utf8x")
        with self.assertRaises(errors.DatabaseDefinitionError):
            db.Alter(charset="latin1")
        # . alter collate
        self.assertEqual(db.charset.collation, "utf8mb3_general_ci")
        self.assertTrue(db.Alter(collate="utf8mb3_bin"))
        self.assertEqual(db.charset.collation, "utf8mb3_bin")
        self.assertEqual(db.ShowMetadata().charset.collation, "utf8mb3_bin")
        # . alter encryption
        self.assertFalse(db.encryption)
        self.assertTrue(db.Alter(encryption=True))
        self.assertTrue(db.encryption)
        self.assertTrue(db.ShowMetadata().encryption)
        # . alter ready only
        self.assertFalse(db.read_only)
        self.assertTrue(db.Alter(read_only=True))
        self.assertTrue(db.read_only)
        self.assertTrue(db.ShowMetadata().read_only)
        # . alter all
        self.assertTrue(db.Alter("utf8mb4", "utf8mb4_general_ci", False, False))
        self.assertEqual(db.charset.name, "utf8mb4")
        self.assertEqual(db.charset.collation, "utf8mb4_general_ci")
        self.assertFalse(db.encryption)
        self.assertFalse(db.read_only)
        meta = db.ShowMetadata()
        self.assertEqual(meta.charset.name, "utf8mb4")
        self.assertEqual(meta.charset.collation, "utf8mb4_general_ci")
        self.assertFalse(meta.encryption)
        self.assertFalse(meta.read_only)

        # . alter local remote mismatch
        class TestDatabase2(Database):
            pass

        db2 = TestDatabase2(
            "test_db",
            self.get_pool(),
            charset="utf8mb3",
            collate="utf8mb3_general_ci",
            encryption=True,
        )
        self.assertEqual(db2.charset.name, "utf8mb3")
        self.assertEqual(db2.charset.collation, "utf8mb3_general_ci")
        self.assertEqual(db2.encryption, True)
        log = db2.Alter("utf8mb4", "utf8mb4_general_ci", False, False)
        # fmt: off
        self.assertIn("(local) charset: 'utf8mb3' => 'utf8mb4'", repr(log))
        self.assertIn("(local) collate: 'utf8mb3_general_ci' => 'utf8mb4_general_ci'", repr(log))
        self.assertIn("(local) encryption: True => False", repr(log))
        self.assertNotIn("(server) CHARACTER SET: 'utf8mb3' => 'utf8mb4'", repr(log))
        self.assertNotIn("(server) COLLATION: 'utf8mb3_general_ci' => 'utf8mb4_general_ci'", repr(log))
        self.assertNotIn("(server) encryption: True => False", repr(log))
        # fmt: on
        self.assertTrue(db.Drop())

        # Lock / Unlock Tables
        select_user_sql = "SELECT * FROM %s" % (db.user)
        select_post_sql = "SELECT * FROM %s" % (db.post)
        self.assertTrue(db.Create())
        self.assertTrue(db.user.Create())
        self.assertTrue(db.post.Create())
        # lock for read
        with self.get_conn() as conn2:
            conn2.set_execution_timeout(100)  # timeout 100ms
            with db.acquire() as conn:
                db.Lock(conn, "user", db.post, lock_for_read=True)  # lock
                with conn2.cursor() as cur:
                    cur.execute(select_user_sql)  # should raise no error
                    cur.execute(select_post_sql)  # should raise no error
                db.Unlock(conn)  # unlock
        # lock for write
        with self.get_conn() as conn3:
            conn3.set_execution_timeout(100)  # timeout 100ms
            with db.acquire() as conn:
                db.Lock(conn, "user", db.post, lock_for_read=False)  # lock
                with conn3.cursor() as cur:
                    with self.assertRaises(sqlerr.OperationalTimeoutError):
                        cur.execute(select_user_sql)  # raise timeout error
                with conn3.cursor() as cur:
                    with self.assertRaises(sqlerr.OperationalTimeoutError):
                        cur.execute(select_post_sql)  # raise timeout error
                db.Unlock(conn)  # unlock
                with conn3.cursor() as cur:
                    cur.execute(select_user_sql)  # should raise no error
                    cur.execute(select_post_sql)  # should raise no error

        # Sync to remote
        self.assertTrue(db.Drop())
        self.assertTrue(db.SyncToRemote())  # re-create

        class TestDatabase3(Database):
            pass

        db3 = TestDatabase3(
            "test_db",
            self.get_pool(),
            charset="utf8mb3",
            collate="utf8mb3_general_ci",
            encryption=True,
        )
        self.assertEqual(db3.charset.name, "utf8mb3")
        self.assertEqual(db3.charset.collation, "utf8mb3_general_ci")
        self.assertEqual(db3.encryption, True)
        self.assertTrue(db3.SyncToRemote())
        meta = db3.ShowMetadata()
        self.assertEqual(meta.charset.name, "utf8mb3")
        self.assertEqual(meta.charset.collation, "utf8mb3_general_ci")
        self.assertEqual(meta.encryption, True)
        self.assertTrue(db.Drop())

        # Finished
        self.assertTrue(db.Drop(True))
        self.log_ended("DATABASE BASIC SQL")

    def test_database_sync_from_remote(self) -> None:
        self.log_start("DATABASE SYNC FROM REMOTE")

        # Test Database
        class User(Table):
            id: Column = Column(Define.INT())
            name: Column = Column(Define.VARCHAR(255))

        class Post(Table):
            id: Column = Column(Define.INT())
            user_id: Column = Column(Define.INT())
            title: Column = Column(Define.VARCHAR(255))

        class TestDatabase(Database):
            user: User = User()
            post: Post = Post()

        db = TestDatabase(
            "test_db",
            self.get_pool(),
            charset="utf8mb4",
            collate="utf8mb4_general_ci",
            encryption=False,
        )

        self.assertTrue(db.Drop(True))

        import warnings

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            # fmt: off
            logs = db.SyncFromRemote()
            self.assertIn("DATABASE 'test_db' (local) failed to sync from the remote server: it does NOT EXIST", repr(logs))
            self.assertTrue(db.Create())
            self.assertFalse(db.SyncFromRemote())
            logs = db.SyncFromRemote(True)
            self.assertIn("TABLE 'test_db.user' (local) failed to sync from the remote server: it does NOT EXIST", repr(logs))
            self.assertIn("TABLE 'test_db.post' (local) failed to sync from the remote server: it does NOT EXIST", repr(logs))
            # fmt: on
            self.assertEqual(len(caught), 3)
            warnings.simplefilter("ignore")

        # Finsihed
        db.Drop(True)
        self.log_ended("DATABASE SYNC FROM REMOTE")

    def test_database_initialize(self) -> None:
        self.log_start("DATABASE INITIALIZE")

        # Initialization
        class User(Table):
            id: Column = Column(Define.INT())
            name: Column = Column(Define.VARCHAR(255))
            g_col: GeneratedColumn = GeneratedColumn(
                Define.VARCHAR(255),
                sqlfunc.CONCAT(sqlfunc.RawText("name"), "'_gen'"),
            )
            idx_name: Index = Index("name")
            pk: PrimaryKey = PrimaryKey("id")

        class Price(Table):
            id: Column = Column(Define.INT())
            price: Column = Column(Define.DECIMAL(12, 2))
            g_col: GeneratedColumn = GeneratedColumn(
                Define.DECIMAL(12, 2),
                sqlfunc.ROUND(sqlfunc.RawText("price * 1.1"), 2),
            )
            idx_price: Index = Index("price")

        class TestDatabase(Database):
            user: User = User()
            price: Price = Price()

        db = TestDatabase(
            "test_db",
            self.get_pool(),
            charset="utf8mb4",
            collate="utf8mb4_general_ci",
        )
        db.Drop(True)
        logs = db.Initialize()
        self.assertIn("(local) encryption: None => False", repr(logs))
        self.assertFalse(db.Initialize())

        # sync from remote
        class User2(Table):
            id: Column = Column(Define.INT())
            name: Column = Column(Define.VARCHAR(255))

        class Price2(Table):
            id: Column = Column(Define.INT())
            price: Column = Column(Define.DECIMAL(12, 2))

        class TestDatabase2(Database):
            user: User2 = User2()
            price: Price2 = Price2()

        db2 = TestDatabase2(
            "test_db",
            self.get_pool(),
            charset="utf8mb3",
            collate="utf8mb3_bin",
            encryption=True,
        )
        logs = db2.Initialize()
        # fmt: off
        self.assertIn("(local) charset: 'utf8mb3' => 'utf8mb4'", repr(logs))
        self.assertIn("(local) collate: 'utf8mb3_bin' => 'utf8mb4_general_ci'", repr(logs))
        self.assertIn("(local) encryption: True => False", repr(logs))
        self.assertFalse(db2.Initialize(force=False))
        self.assertFalse(db2.Initialize(force=True))
        # fmt: on

        # Finished
        self.assertTrue(db.Drop(True))
        self.log_ended("DATABASE INITIALIZE")


class TestDatabaseAsyncSQL(TestCase):
    name: str = "Database Async SQL"

    async def test_all(self) -> None:
        await self.test_database_basic_sql()
        await self.test_database_sync_from_remote()
        await self.test_database_initialize()

    async def test_database_basic_sql(self) -> None:
        self.log_start("DATABASE BASIC SQL")

        # Test Database
        class User(Table):
            id: Column = Column(Define.INT())
            name: Column = Column(Define.VARCHAR(255))

        class Post(Table):
            id: Column = Column(Define.INT())
            user_id: Column = Column(Define.INT())
            title: Column = Column(Define.VARCHAR(255))

        class TestDataBase(Database):
            user: User = User()
            post: Post = Post()

        db = TestDataBase(
            "test_db",
            self.get_pool(),
            charset="utf8mb4",
            collate="utf8mb4_general_ci",
            encryption=False,
        )

        # Create & Exists & Drop
        self.assertTrue(await db.aioDrop(True))
        self.assertFalse(await db.aioExists())
        self.assertTrue(await db.aioCreate())
        self.assertTrue(await db.aioExists())
        with self.assertRaises(sqlerr.OperationalError):
            await db.aioCreate()
        self.assertTrue(await db.aioDrop())
        with self.assertRaises(sqlerr.OperationalError):
            await db.aioDrop()

        # Show databases / metadata
        with self.assertRaises(sqlerr.OperationalError):
            await db.aioShowMetadata()
        self.assertTrue(await db.aioCreate())
        self.assertIn(db.db_name, await db.aioShowDatabases())
        meta = await db.aioShowMetadata()
        self.assertEqual(meta["SCHEMA_NAME"], db.db_name)
        self.assertEqual(meta.db_name, db.db_name)
        self.assertEqual(meta["DEFAULT_CHARACTER_SET_NAME"], "utf8mb4")
        self.assertEqual(meta.charset.name, "utf8mb4")
        self.assertEqual(meta["DEFAULT_COLLATION_NAME"], "utf8mb4_general_ci")
        self.assertEqual(meta.charset.collation, "utf8mb4_general_ci")
        self.assertIsNone(meta["SQL_PATH"])
        self.assertEqual(meta["DEFAULT_ENCRYPTION"], "NO")
        self.assertFalse(meta.encryption)
        self.assertTrue(await db.aioDrop())
        self.assertNotIn(db.db_name, await db.aioShowDatabases())

        # Alter
        self.assertTrue(await db.aioCreate())
        # . no alteration
        self.assertFalse(await db.aioAlter())
        # . alter charset
        self.assertEqual(db.charset.name, "utf8mb4")
        self.assertTrue(await db.aioAlter(charset="utf8mb3"))
        self.assertEqual(db.charset.name, "utf8mb3")
        with self.assertRaises(errors.DatabaseDefinitionError):
            await db.aioAlter(charset="utf8x")
        with self.assertRaises(errors.DatabaseDefinitionError):
            await db.aioAlter(charset="latin1")
        # . alter collate
        self.assertEqual(db.charset.collation, "utf8mb3_general_ci")
        self.assertTrue(await db.aioAlter(collate="utf8mb3_bin"))
        self.assertEqual(db.charset.collation, "utf8mb3_bin")
        self.assertEqual((await db.aioShowMetadata()).charset.collation, "utf8mb3_bin")
        # . alter encryption
        self.assertFalse(db.encryption)
        self.assertTrue(await db.aioAlter(encryption=True))
        self.assertTrue(db.encryption)
        self.assertTrue((await db.aioShowMetadata()).encryption)
        # . alter ready only
        self.assertFalse(db.read_only)
        self.assertTrue(await db.aioAlter(read_only=True))
        self.assertTrue(db.read_only)
        self.assertTrue((await db.aioShowMetadata()).read_only)
        # . alter all
        self.assertTrue(
            await db.aioAlter("utf8mb4", "utf8mb4_general_ci", False, False)
        )
        self.assertEqual(db.charset.name, "utf8mb4")
        self.assertEqual(db.charset.collation, "utf8mb4_general_ci")
        self.assertFalse(db.encryption)
        self.assertFalse(db.read_only)
        meta = await db.aioShowMetadata()
        self.assertEqual(meta.charset.name, "utf8mb4")
        self.assertEqual(meta.charset.collation, "utf8mb4_general_ci")
        self.assertFalse(meta.encryption)
        self.assertFalse(meta.read_only)

        # . alter local remote mismatch
        class TestDatabase2(Database):
            pass

        db2 = TestDatabase2(
            "test_db",
            self.get_pool(),
            charset="utf8mb3",
            collate="utf8mb3_general_ci",
            encryption=True,
        )
        self.assertEqual(db2.charset.name, "utf8mb3")
        self.assertEqual(db2.charset.collation, "utf8mb3_general_ci")
        self.assertEqual(db2.encryption, True)
        log = await db2.aioAlter("utf8mb4", "utf8mb4_general_ci", False, False)
        # fmt: off
        self.assertIn("(local) charset: 'utf8mb3' => 'utf8mb4'", repr(log))
        self.assertIn("(local) collate: 'utf8mb3_general_ci' => 'utf8mb4_general_ci'", repr(log))
        self.assertIn("(local) encryption: True => False", repr(log))
        self.assertNotIn("(server) CHARACTER SET: 'utf8mb3' => 'utf8mb4'", repr(log))
        self.assertNotIn("(server) COLLATION: 'utf8mb3_general_ci' => 'utf8mb4_general_ci'", repr(log))
        self.assertNotIn("(server) encryption: True => False", repr(log))
        # fmt: on
        self.assertTrue(await db.aioDrop())

        # Lock / Unlock Tables
        select_user_sql = "SELECT * FROM %s" % (db.user)
        select_post_sql = "SELECT * FROM %s" % (db.post)
        self.assertTrue(await db.aioCreate())
        self.assertTrue(await db.user.aioCreate())
        self.assertTrue(await db.post.aioCreate())
        # lock for read
        with self.get_conn() as conn2:
            conn2.set_execution_timeout(100)  # timeout 100ms
            async with db.acquire() as conn:
                await db.aioLock(conn, "user", db.post, lock_for_read=True)  # lock
                with conn2.cursor() as cur:
                    cur.execute(select_user_sql)  # should raise no error
                    cur.execute(select_post_sql)  # should raise no error
                await db.aioUnlock(conn)  # unlock
        # lock for write
        with self.get_conn() as conn3:
            conn3.set_execution_timeout(100)  # timeout 100ms
            async with db.acquire() as conn:
                await db.aioLock(conn, "user", db.post, lock_for_read=False)  # lock
                with conn3.cursor() as cur:
                    with self.assertRaises(sqlerr.OperationalTimeoutError):
                        cur.execute(select_user_sql)
                with conn3.cursor() as cur:
                    with self.assertRaises(sqlerr.OperationalTimeoutError):
                        cur.execute(select_post_sql)
                await db.aioUnlock(conn)  # unlock
                with conn3.cursor() as cur:
                    cur.execute(select_user_sql)  # should raise no error
                    cur.execute(select_post_sql)  # should raise no error

        # Sync to remote
        self.assertTrue(await db.aioDrop())
        self.assertTrue(await db.aioSyncToRemote())  # re-create

        class TestDatabase3(Database):
            pass

        db3 = TestDatabase3(
            "test_db",
            self.get_pool(),
            charset="utf8mb3",
            collate="utf8mb3_general_ci",
            encryption=True,
        )
        self.assertEqual(db3.charset.name, "utf8mb3")
        self.assertEqual(db3.charset.collation, "utf8mb3_general_ci")
        self.assertEqual(db3.encryption, True)
        self.assertTrue(await db3.aioSyncToRemote())
        meta = await db3.aioShowMetadata()
        self.assertEqual(meta.charset.name, "utf8mb3")
        self.assertEqual(meta.charset.collation, "utf8mb3_general_ci")
        self.assertEqual(meta.encryption, True)
        self.assertTrue(await db.aioDrop())

        # Finished
        self.assertTrue(await db.aioDrop(True))
        self.log_ended("DATABASE BASIC SQL")

    async def test_database_sync_from_remote(self) -> None:
        self.log_start("DATABASE SYNC FROM REMOTE")

        # Test Database
        class User(Table):
            id: Column = Column(Define.INT())
            name: Column = Column(Define.VARCHAR(255))

        class Post(Table):
            id: Column = Column(Define.INT())
            user_id: Column = Column(Define.INT())
            title: Column = Column(Define.VARCHAR(255))

        class TestDatabase(Database):
            user: User = User()
            post: Post = Post()

        db = TestDatabase(
            "test_db",
            self.get_pool(),
            charset="utf8mb4",
            collate="utf8mb4_general_ci",
            encryption=False,
        )

        import warnings

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            # fmt: off
            logs = await db.aioSyncFromRemote()
            self.assertIn("DATABASE 'test_db' (local) failed to sync from the remote server: it does NOT EXIST", repr(logs))
            self.assertTrue(await db.aioCreate())
            self.assertFalse(await db.aioSyncFromRemote())
            logs = await db.aioSyncFromRemote(True)
            self.assertIn("TABLE 'test_db.user' (local) failed to sync from the remote server: it does NOT EXIST", repr(logs))
            self.assertIn("TABLE 'test_db.post' (local) failed to sync from the remote server: it does NOT EXIST", repr(logs))
            # fmt: on
            self.assertEqual(len(caught), 3)
            warnings.simplefilter("ignore")

        # Finsihed
        await db.aioDrop(True)
        self.log_ended("DATABASE SYNC FROM REMOTE")

    async def test_database_initialize(self) -> None:
        self.log_start("DATABASE INITIALIZE")

        # Initialization
        class User(Table):
            id: Column = Column(Define.INT())
            name: Column = Column(Define.VARCHAR(255))
            g_col: GeneratedColumn = GeneratedColumn(
                Define.VARCHAR(255),
                sqlfunc.CONCAT(sqlfunc.RawText("name"), "'_gen'"),
            )
            idx_name: Index = Index("name")
            pk: PrimaryKey = PrimaryKey("id")
            pt: Partitioning = Partitioning("id").by_key(2, linear=True)

        class Price(Table):
            id: Column = Column(Define.INT())
            price: Column = Column(Define.DECIMAL(12, 2))
            g_col: GeneratedColumn = GeneratedColumn(
                Define.DECIMAL(12, 2),
                sqlfunc.ROUND(sqlfunc.RawText("price * 1.1"), 2),
            )
            idx_price: Index = Index("price")

        class TestDatabase(Database):
            user: User = User()
            price: Price = Price()

        db = TestDatabase(
            "test_db",
            self.get_pool(),
            charset="utf8mb4",
            collate="utf8mb4_general_ci",
        )
        await db.aioDrop(True)
        logs = await db.aioInitialize()
        self.assertIn("(local) encryption: None => False", repr(logs))
        self.assertFalse(await db.aioInitialize())

        # sync from remote
        class User2(Table):
            id: Column = Column(Define.INT())
            name: Column = Column(Define.VARCHAR(255))
            pt: Partitioning = Partitioning("id").by_key(3)

        class Price2(Table):
            id: Column = Column(Define.INT())
            price: Column = Column(Define.DECIMAL(12, 2))

        class TestDatabase2(Database):
            user: User2 = User2()
            price: Price2 = Price2()

        db2 = TestDatabase2(
            "test_db",
            self.get_pool(),
            charset="utf8mb3",
            collate="utf8mb3_bin",
            encryption=True,
        )
        logs = await db2.aioInitialize()
        # fmt: off
        self.assertIn("(local) charset: 'utf8mb3' => 'utf8mb4'", repr(logs))
        self.assertIn("(local) collate: 'utf8mb3_bin' => 'utf8mb4_general_ci'", repr(logs))
        self.assertIn("(local) encryption: True => False", repr(logs))
        # fmt: on
        self.assertFalse(await db2.aioInitialize(force=False))
        self.assertFalse(await db2.aioInitialize(force=True))
        # Finished
        self.assertTrue(await db.aioDrop(True))
        self.log_ended("DATABASE INITIALIZE")


if __name__ == "__main__":
    HOST = "localhost"
    PORT = 3306
    USER = "root"
    PSWD = "Password_123456"

    for case in (
        TestDatabaseDefinition,
        TestDatabaseCopy,
        TestDatabaseBaseClass,
        TestDatabaseSyncSQL,
        TestDatabaseAsyncSQL,
    ):
        test_case = case(HOST, PORT, USER, PSWD)
        if not iscoroutinefunction(test_case.test_all):
            test_case.test_all()
        else:
            asyncio.run(test_case.test_all())
