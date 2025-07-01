import asyncio, time, unittest, datetime
from decimal import Decimal
from inspect import iscoroutinefunction
from sqlcycli import errors as sqlerr, sqlfunc, Pool, Connection
from mysqlengine import errors
from mysqlengine.table import Table
from mysqlengine.database import Database
from mysqlengine.constraint import UniqueKey
from mysqlengine.column import Define, Definition, Column, Columns, GeneratedColumn


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

    def setup_definition(self, define: Definition) -> Definition:
        repr(define)
        define._set_tb_name("tb1")
        define._set_db_name("db1")
        define._set_pool(self.get_pool())
        define._set_charset("utf8")
        return define

    def setup_column(self, col: Column, name: str = "col") -> Column:
        col._set_position(1)
        col.set_name(name)
        col.setup("tb1", "db1", "utf8", None, self.get_pool())
        return col

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


class TestColumnDefinition(TestCase):
    name: str = "Column Definition"

    # utils
    def assertDefinition(self, define: Definition, compare_sql: str) -> None:
        define._set_charset("utf8")
        self.assertEqual(define._gen_definition_sql(), compare_sql)

    def assertCharset(self, d: Definition, charset: str, collate: str) -> None:
        col = self.setup_column(Column(d))
        self.assertEqual(col.charset.name, charset)
        self.assertEqual(col.charset.collation, collate)
        col._gen_definition_sql()

    # tests
    def test_all(self) -> None:
        self.test_integer()
        self.test_floating_point()
        self.test_fixed_point()
        self.test_date()
        self.test_datetime_n_timestamp()
        self.test_time()
        self.test_year()
        self.test_char_n_varchar()
        self.test_text()
        self.test_enum()
        self.test_binary_n_varbinary()
        self.test_blob()
        self.test_generated_column()
        self.test_columns()

    def test_integer(self) -> None:
        self.log_start("INTEGER")

        args_base = {
            "unsigned": False,
            "null": False,
            "default": None,
            "auto_increment": False,
            "comment": None,
            "visible": True,
        }
        args_all = {
            "unsigned": True,
            "null": True,
            "default": 1,
            "auto_increment": True,
            "comment": "Comment",
            "visible": False,
        }
        for dtype in (
            Define.TINYINT,
            Define.SMALLINT,
            Define.MEDIUMINT,
            Define.INT,
            Define.BIGINT,
        ):
            # fmt: off
            # base
            d = self.setup_definition(dtype(**args_base))
            self.assertDefinition(d, f"{d.data_type} NOT NULL")
            # unsigned
            d = self.setup_definition(dtype(**(args_base | {"unsigned": True})))
            self.assertDefinition(d, f"{d.data_type} UNSIGNED NOT NULL")
            # null
            d = self.setup_definition(dtype(**(args_base | {"null": True})))
            self.assertDefinition(d, f"{d.data_type}")
            # default
            d = self.setup_definition(dtype(**(args_base | {"default": 1})))
            self.assertDefinition(d, f"{d.data_type} NOT NULL DEFAULT 1")
            # auto_increment
            d = self.setup_definition(dtype(**(args_base | {"auto_increment": True})))
            self.assertDefinition(d, f"{d.data_type} UNSIGNED NOT NULL AUTO_INCREMENT")
            # comment
            d = self.setup_definition(dtype(**(args_base | {"comment": "Comment"})))
            self.assertDefinition(d, f"{d.data_type} NOT NULL COMMENT 'Comment'")
            # visible
            d = self.setup_definition(dtype(**(args_base | {"visible": False})))
            self.assertDefinition(d, f"{d.data_type} NOT NULL INVISIBLE")
            # all
            d = self.setup_definition(dtype(**(args_all)))
            self.assertDefinition(d, f"{d.data_type} UNSIGNED AUTO_INCREMENT COMMENT 'Comment' INVISIBLE")
            # signed + auto_increment + default
            d = self.setup_definition(dtype(**(args_base | {"unsigned": False, "auto_increment": True, "default": 1})))
            self.assertDefinition(d, f"{d.data_type} UNSIGNED NOT NULL AUTO_INCREMENT")
            # fmt: on

        self.log_ended("INTEGER")

    def test_floating_point(self) -> None:
        self.log_start("FLOATING POINT")

        args_base = {
            "null": False,
            "default": None,
            "comment": None,
            "visible": True,
        }
        args_all = {
            "null": True,
            "default": 1.0,
            "comment": "Comment",
            "visible": False,
        }
        for dtype in (
            Define.FLOAT,
            Define.DOUBLE,
        ):
            # fmt: off
            # base
            d = self.setup_definition(dtype(**args_base))
            self.assertDefinition(d, f"{d.data_type} NOT NULL")
            # null
            d = self.setup_definition(dtype(**(args_base | {"null": True})))
            self.assertDefinition(d, f"{d.data_type}")
            # default
            d = self.setup_definition(dtype(**(args_base | {"default": 1.0})))
            self.assertDefinition(d, f"{d.data_type} NOT NULL DEFAULT 1.0")
            # comment
            d = self.setup_definition(dtype(**(args_base | {"comment": "Comment"})))
            self.assertDefinition(d, f"{d.data_type} NOT NULL COMMENT 'Comment'")
            # visible
            d = self.setup_definition(dtype(**(args_base | {"visible": False})))
            self.assertDefinition(d, f"{d.data_type} NOT NULL INVISIBLE")
            # all
            d = self.setup_definition(dtype(**(args_all)))
            self.assertDefinition(d, f"{d.data_type} DEFAULT 1.0 COMMENT 'Comment' INVISIBLE")
            # fmt: on

        self.log_ended("FLOATING POINT")

    def test_fixed_point(self) -> None:
        self.log_start("FIXED POINT")

        args_base = {
            "precision": None,
            "scale": None,
            "null": False,
            "default": None,
            "comment": None,
            "visible": True,
        }
        args_all = {
            "precision": 12,
            "scale": 2,
            "null": True,
            "default": 1.0,
            "comment": "Comment",
            "visible": False,
        }
        for dtype in (Define.DECIMAL,):
            # fmt: off
            # base
            d = self.setup_definition(dtype(**args_base))
            self.assertDefinition(d, f"{d.data_type}(10,0) NOT NULL")
            # precision + scale
            d = self.setup_definition(dtype(**(args_base | {"precision": 12, "scale": 2})))
            self.assertDefinition(d, f"{d.data_type}(12,2) NOT NULL")
            # null
            d = self.setup_definition(dtype(**(args_base | {"null": True})))
            self.assertDefinition(d, f"{d.data_type}(10,0)")
            # default
            d = self.setup_definition(dtype(**(args_base | {"default": "1.0"})))
            self.assertDefinition(d, f"{d.data_type}(10,0) NOT NULL DEFAULT 1")
            # comment
            d = self.setup_definition(dtype(**(args_base | {"comment": "Comment"})))
            self.assertDefinition(d, f"{d.data_type}(10,0) NOT NULL COMMENT 'Comment'")
            # visible
            d = self.setup_definition(dtype(**(args_base | {"visible": False})))
            self.assertDefinition(d, f"{d.data_type}(10,0) NOT NULL INVISIBLE")
            # all
            d = self.setup_definition(dtype(**(args_all)))
            self.assertDefinition(d, f"{d.data_type}(12,2) DEFAULT 1.00 COMMENT 'Comment' INVISIBLE")
            # fmt: on
            with self.assertRaises(errors.ColumnDefinitionError):
                self.setup_definition(dtype(precision="0"))
            with self.assertRaises(errors.ColumnDefinitionError):
                self.setup_definition(dtype(precision=0))
            with self.assertRaises(errors.ColumnDefinitionError):
                self.setup_definition(dtype(precision=66))
            with self.assertRaises(errors.ColumnDefinitionError):
                self.setup_definition(dtype(precision=10, scale=-1))
            with self.assertRaises(errors.ColumnDefinitionError):
                self.setup_definition(dtype(precision=10, scale=11))
            with self.assertRaises(errors.ColumnDefinitionError):
                self.setup_definition(dtype(default="inf"))

        self.log_ended("FIXED POINT")

    def test_date(self) -> None:
        self.log_start("DATE")

        args_base = {
            "null": False,
            "default": None,
            "comment": None,
            "visible": True,
        }
        args_all = {
            "null": True,
            "default": datetime.date(1970, 1, 1),
            "comment": "Comment",
            "visible": False,
        }
        for dtype in (Define.DATE,):
            # fmt: off
            # base
            d = self.setup_definition(dtype(**args_base))
            self.assertDefinition(d, f"{d.data_type} NOT NULL")
            # null
            d = self.setup_definition(dtype(**(args_base | {"null": True})))
            self.assertDefinition(d, f"{d.data_type}")
            # default
            d = self.setup_definition(dtype(**(args_base | {"default": datetime.date(1970, 1, 1)})))
            self.assertDefinition(d, f"{d.data_type} NOT NULL DEFAULT '1970-01-01'")
            # comment
            d = self.setup_definition(dtype(**(args_base | {"comment": "Comment"})))
            self.assertDefinition(d, f"{d.data_type} NOT NULL COMMENT 'Comment'")
            # visible
            d = self.setup_definition(dtype(**(args_base | {"visible": False})))
            self.assertDefinition(d, f"{d.data_type} NOT NULL INVISIBLE")
            # all
            d = self.setup_definition(dtype(**(args_all)))
            self.assertDefinition(d, f"{d.data_type} DEFAULT '1970-01-01' COMMENT 'Comment' INVISIBLE")
            # fmt: on

        self.log_ended("DATE")

    def test_datetime_n_timestamp(self) -> None:
        self.log_start("DATETIME & TIMESTAMP")

        args_base = {
            "fsp": None,
            "null": False,
            "auto_init": False,
            "auto_update": False,
            "default": None,
            "comment": None,
            "visible": True,
        }
        args_all = {
            "fsp": 6,
            "null": True,
            "auto_init": True,
            "auto_update": True,
            "default": datetime.datetime(1970, 1, 1),
            "comment": "Comment",
            "visible": False,
        }
        for dtype in (Define.DATETIME, Define.TIMESTAMP):
            # fmt: off
            # base
            d = self.setup_definition(dtype(**args_base))
            self.assertDefinition(d, f"{d.data_type} NOT NULL")
            # fsp
            d = self.setup_definition(dtype(**(args_base | {"fsp": 6})))
            self.assertDefinition(d, f"{d.data_type}(6) NOT NULL")
            # null
            d = self.setup_definition(dtype(**(args_base | {"null": True})))
            self.assertDefinition(d, f"{d.data_type}")
            # auto_init
            d = self.setup_definition(dtype(**(args_base | {"auto_init": True})))
            self.assertDefinition(d, f"{d.data_type} NOT NULL DEFAULT CURRENT_TIMESTAMP")
            # auto_update
            d = self.setup_definition(dtype(**(args_base | {"auto_update": True})))
            self.assertDefinition(d, f"{d.data_type} NOT NULL ON UPDATE CURRENT_TIMESTAMP")
            # default
            d = self.setup_definition(dtype(**(args_base | {"default": datetime.datetime(1970, 1, 1)})))
            self.assertDefinition(d, f"{d.data_type} NOT NULL DEFAULT '1970-01-01 00:00:00'")
            # comment
            d = self.setup_definition(dtype(**(args_base | {"comment": "Comment"})))
            self.assertDefinition(d, f"{d.data_type} NOT NULL COMMENT 'Comment'")
            # visible
            d = self.setup_definition(dtype(**(args_base | {"visible": False})))
            self.assertDefinition(d, f"{d.data_type} NOT NULL INVISIBLE")
            # all
            d = self.setup_definition(dtype(**(args_all)))
            self.assertDefinition(d, f"{d.data_type}(6) DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6) COMMENT 'Comment' INVISIBLE")
            # auto_init + default
            d = self.setup_definition(dtype(**(args_base | {"auto_init": True, "default": datetime.datetime(1970, 1, 1)})))
            self.assertDefinition(d, f"{d.data_type} NOT NULL DEFAULT CURRENT_TIMESTAMP")
            # fmt: on
            with self.assertRaises(errors.ColumnDefinitionError):
                self.setup_definition(dtype(fsp="0"))
            with self.assertRaises(errors.ColumnDefinitionError):
                self.setup_definition(dtype(fsp=7))

        self.log_ended("DATETIME & TIMESTAMP")

    def test_time(self) -> None:
        self.log_start("TIME")

        args_base = {
            "fsp": None,
            "null": False,
            "default": None,
            "comment": None,
            "visible": True,
        }
        args_all = {
            "fsp": 6,
            "null": True,
            "default": datetime.time(1, 1, 1, 1),
            "comment": "Comment",
            "visible": False,
        }
        for dtype in (Define.TIME,):
            # fmt: off
            # base
            d = self.setup_definition(dtype(**args_base))
            self.assertDefinition(d, f"{d.data_type} NOT NULL")
            # fsp
            d = self.setup_definition(dtype(**(args_base | {"fsp": 6})))
            self.assertDefinition(d, f"{d.data_type}(6) NOT NULL")
            # null
            d = self.setup_definition(dtype(**(args_base | {"null": True})))
            self.assertDefinition(d, f"{d.data_type}")
            # default
            d = self.setup_definition(dtype(**(args_base | {"default": datetime.time(1, 1, 1, 1)})))
            self.assertDefinition(d, f"{d.data_type} NOT NULL DEFAULT '01:01:01'")
            # comment
            d = self.setup_definition(dtype(**(args_base | {"comment": "Comment"})))
            self.assertDefinition(d, f"{d.data_type} NOT NULL COMMENT 'Comment'")
            # visible
            d = self.setup_definition(dtype(**(args_base | {"visible": False})))
            self.assertDefinition(d, f"{d.data_type} NOT NULL INVISIBLE")
            # all
            d = self.setup_definition(dtype(**(args_all)))
            self.assertDefinition(d, f"{d.data_type}(6) DEFAULT '01:01:01.000001' COMMENT 'Comment' INVISIBLE")
            # fmt: on
            with self.assertRaises(errors.ColumnDefinitionError):
                self.setup_definition(dtype(fsp="0"))
            with self.assertRaises(errors.ColumnDefinitionError):
                self.setup_definition(dtype(fsp=7))

        self.log_ended("TIME")

    def test_year(self) -> None:
        self.log_start("YEAR")

        args_base = {
            "null": False,
            "default": None,
            "comment": None,
            "visible": True,
        }
        args_all = {
            "null": True,
            "default": 1970,
            "comment": "Comment",
            "visible": False,
        }
        for dtype in (Define.YEAR,):
            # fmt: off
            # base
            d = self.setup_definition(dtype(**args_base))
            self.assertDefinition(d, f"{d.data_type} NOT NULL")
            # null
            d = self.setup_definition(dtype(**(args_base | {"null": True})))
            self.assertDefinition(d, f"{d.data_type}")
            # default
            d = self.setup_definition(dtype(**(args_base | {"default": 1970})))
            self.assertDefinition(d, f"{d.data_type} NOT NULL DEFAULT 1970")
            d = self.setup_definition(dtype(**(args_base | {"default": 10})))
            self.assertDefinition(d, f"{d.data_type} NOT NULL DEFAULT 2010")
            # comment
            d = self.setup_definition(dtype(**(args_base | {"comment": "Comment"})))
            self.assertDefinition(d, f"{d.data_type} NOT NULL COMMENT 'Comment'")
            # visible
            d = self.setup_definition(dtype(**(args_base | {"visible": False})))
            self.assertDefinition(d, f"{d.data_type} NOT NULL INVISIBLE")
            # all
            d = self.setup_definition(dtype(**(args_all)))
            self.assertDefinition(d, f"{d.data_type} DEFAULT 1970 COMMENT 'Comment' INVISIBLE")
            # fmt: on
            with self.assertRaises(errors.ColumnDefinitionError):
                self.setup_definition(dtype(default="-1"))
            with self.assertRaises(errors.ColumnDefinitionError):
                self.setup_definition(dtype(default=2156))
            with self.assertRaises(errors.ColumnDefinitionError):
                self.setup_definition(dtype(default=100))

        self.log_ended("YEAR")

    def test_char_n_varchar(self) -> None:
        self.log_start("CHAR & VARCHAR")

        args_base = {
            "length": 1,
            "null": False,
            "default": None,
            "comment": None,
            "visible": True,
        }
        args_all = {
            "length": 255,
            "null": True,
            "default": "Char&Varchar",
            "comment": "Comment",
            "visible": False,
        }
        for dtype in (Define.CHAR, Define.VARCHAR):
            # fmt: off
            # base
            d = self.setup_definition(dtype(**args_base))
            self.assertDefinition(d, f"{d.data_type}(1) NOT NULL COLLATE utf8mb4_general_ci")
            # length
            d = self.setup_definition(dtype(**(args_base | {"length": 255})))
            self.assertDefinition(d, f"{d.data_type}(255) NOT NULL COLLATE utf8mb4_general_ci")
            # null
            d = self.setup_definition(dtype(**(args_base | {"null": True})))
            self.assertDefinition(d, f"{d.data_type}(1) COLLATE utf8mb4_general_ci")
            # default
            d = self.setup_definition(dtype(**(args_base | {"default": "Char&Varchar"})))
            self.assertDefinition(d, f"{d.data_type}(1) NOT NULL DEFAULT 'Char&Varchar' COLLATE utf8mb4_general_ci")
            # comment
            d = self.setup_definition(dtype(**(args_base | {"comment": "Comment"})))
            self.assertDefinition(d, f"{d.data_type}(1) NOT NULL COLLATE utf8mb4_general_ci COMMENT 'Comment'")
            # visible
            d = self.setup_definition(dtype(**(args_base | {"visible": False})))
            self.assertDefinition(d, f"{d.data_type}(1) NOT NULL COLLATE utf8mb4_general_ci INVISIBLE")
            # all
            d = self.setup_definition(dtype(**(args_all)))
            self.assertDefinition(d, f"{d.data_type}(255) DEFAULT 'Char&Varchar' COLLATE utf8mb4_general_ci COMMENT 'Comment' INVISIBLE")
            # fmt: on

        # Test charset
        for dtype in (Define.CHAR, Define.VARCHAR):
            # fmt: off
            # utf8
            d = self.setup_definition(dtype(255, False, None, "utf8"))
            self.assertDefinition(d, f"{d.data_type}(255) NOT NULL COLLATE utf8mb4_general_ci")
            self.assertCharset(d, "utf8mb4", "utf8mb4_general_ci")
            # collate: utf8mb4_bin
            d = self.setup_definition(dtype(255, False, None, None, "utf8mb4_bin"))
            self.assertDefinition(d, f"{d.data_type}(255) NOT NULL COLLATE utf8mb4_bin")
            self.assertCharset(d, "utf8mb4", "utf8mb4_bin")
            # charset: utf8mb3
            d = self.setup_definition(dtype(255, False, None, "utf8mb3"))
            self.assertDefinition(d, f"{d.data_type}(255) NOT NULL COLLATE utf8mb3_general_ci")
            self.assertCharset(d, "utf8mb3", "utf8mb3_general_ci")
            # different encoding
            d = self.setup_definition(dtype(255, False, None, "latin1"))
            with self.assertRaises(errors.ColumnDefinitionError):
                self.assertCharset(d, "latin1", "latin1_swedish_ci")
            # fmt: on

        self.log_ended("CHAR & VARCHAR")

    def test_text(self) -> None:
        self.log_start("TEXT")

        args_base = {
            "null": False,
            "comment": None,
            "visible": True,
        }
        args_all = {
            "null": True,
            "comment": "Comment",
            "visible": False,
        }
        for dtype in (
            Define.TINYTEXT,
            Define.TEXT,
            Define.MEDIUMTEXT,
            Define.LONGTEXT,
        ):
            # fmt: off
            # base
            d = self.setup_definition(dtype(**args_base))
            self.assertDefinition(d, f"{d.data_type} NOT NULL COLLATE utf8mb4_general_ci")
            # null
            d = self.setup_definition(dtype(**(args_base | {"null": True})))
            self.assertDefinition(d, f"{d.data_type} COLLATE utf8mb4_general_ci")
            # comment
            d = self.setup_definition(dtype(**(args_base | {"comment": "Comment"})))
            self.assertDefinition(d, f"{d.data_type} NOT NULL COLLATE utf8mb4_general_ci COMMENT 'Comment'")
            # visible
            d = self.setup_definition(dtype(**(args_base | {"visible": False})))
            self.assertDefinition(d, f"{d.data_type} NOT NULL COLLATE utf8mb4_general_ci INVISIBLE")
            # all
            d = self.setup_definition(dtype(**(args_all)))
            self.assertDefinition(d, f"{d.data_type} COLLATE utf8mb4_general_ci COMMENT 'Comment' INVISIBLE")
            # fmt: on

        # Test charset
        for dtype in (
            Define.TINYTEXT,
            Define.TEXT,
            Define.MEDIUMTEXT,
            Define.LONGTEXT,
        ):
            # fmt: off
            # utf8
            d = self.setup_definition(dtype(charset="utf8"))
            self.assertDefinition(d, f"{d.data_type} NOT NULL COLLATE utf8mb4_general_ci")
            self.assertCharset(d, "utf8mb4", "utf8mb4_general_ci")
            # collate: utf8mb4_bin
            d = self.setup_definition(dtype(collate="utf8mb4_bin"))
            self.assertDefinition(d, f"{d.data_type} NOT NULL COLLATE utf8mb4_bin")
            self.assertCharset(d, "utf8mb4", "utf8mb4_bin")
            # charset: utf8mb3
            d = self.setup_definition(dtype(charset="utf8mb3"))
            self.assertDefinition(d, f"{d.data_type} NOT NULL COLLATE utf8mb3_general_ci")
            self.assertCharset(d, "utf8mb3", "utf8mb3_general_ci")
            # different encoding
            d = self.setup_definition(dtype(charset="latin1"))
            with self.assertRaises(errors.ColumnDefinitionError):
                self.assertCharset(d, "latin1", "latin1_swedish_ci")
            # fmt: on

        self.log_ended("TEXT")

    def test_enum(self) -> None:
        self.log_start("ENUM")

        args_base = {
            "null": False,
            "default": None,
            "comment": None,
            "visible": True,
        }
        args_all = {
            "null": True,
            "default": "b",
            "comment": "Comment",
            "visible": False,
        }
        for dtype in (Define.ENUM,):
            # fmt: off
            # base
            d = self.setup_definition(dtype("a", "b", "c", **args_base))
            self.assertDefinition(d, f"{d.data_type}('a','b','c') NOT NULL COLLATE utf8mb4_general_ci")
            # null
            d = self.setup_definition(dtype(["a", "b", "c"], **(args_base | {"null": True})))
            self.assertDefinition(d, f"{d.data_type}('a','b','c') COLLATE utf8mb4_general_ci")
            # default
            d = self.setup_definition(dtype(("a", "b", "c"), **(args_base | {"default": "b"})))
            self.assertDefinition(d, f"{d.data_type}('a','b','c') NOT NULL DEFAULT 'b' COLLATE utf8mb4_general_ci")
            # comment
            d = self.setup_definition(dtype(("a", "b", "c"), **(args_base | {"comment": "Comment"})))
            self.assertDefinition(d, f"{d.data_type}('a','b','c') NOT NULL COLLATE utf8mb4_general_ci COMMENT 'Comment'")
            # visible
            d = self.setup_definition(dtype(("a", "b", "c"), **(args_base | {"visible": False})))
            self.assertDefinition(d, f"{d.data_type}('a','b','c') NOT NULL COLLATE utf8mb4_general_ci INVISIBLE")
            # all
            d = self.setup_definition(dtype(("a", "b", "c"), **(args_all)))
            self.assertDefinition(d, f"{d.data_type}('a','b','c') DEFAULT 'b' COLLATE utf8mb4_general_ci COMMENT 'Comment' INVISIBLE")
            # fmt: on
            with self.assertRaises(errors.ColumnDefinitionError):
                self.setup_definition(dtype())
            with self.assertRaises(errors.ColumnDefinitionError):
                self.setup_definition(dtype("a", 1))
            with self.assertRaises(errors.ColumnDefinitionError):
                self.setup_definition(dtype("a", "b", default="c"))

        # Test charset
        for dtype in (Define.ENUM,):
            # fmt: off
            # utf8
            d = self.setup_definition(dtype("a", "b", "c", charset="utf8"))
            self.assertDefinition(d, "ENUM('a','b','c') NOT NULL COLLATE utf8mb4_general_ci")
            self.assertCharset(d, "utf8mb4", "utf8mb4_general_ci")
            # collate: utf8mb4_bin
            d = self.setup_definition(dtype("a", "b", "c", collate="utf8mb4_bin"))
            self.assertDefinition(d, "ENUM('a','b','c') NOT NULL COLLATE utf8mb4_bin")
            self.assertCharset(d, "utf8mb4", "utf8mb4_bin")
            # charset: utf8mb3
            d = self.setup_definition(dtype("a", "b", "c", charset="utf8mb3"))
            self.assertDefinition(d, "ENUM('a','b','c') NOT NULL COLLATE utf8mb3_general_ci")
            self.assertCharset(d, "utf8mb3", "utf8mb3_general_ci")
            # different encoding
            d = self.setup_definition(dtype("a", "b", "c", charset="latin1"))
            with self.assertRaises(errors.ColumnDefinitionError):
                self.assertCharset(d, "latin1", "latin1_swedish_ci")
            # fmt: on

        self.log_ended("ENUM")

    def test_binary_n_varbinary(self) -> None:
        self.log_start("BINARY & VARBINARY")

        args_base = {
            "length": 1,
            "null": False,
            "default": None,
            "comment": None,
            "visible": True,
        }
        args_all = {
            "length": 255,
            "null": True,
            "default": b"Binary&Varbinary",
            "comment": "Comment",
            "visible": False,
        }
        for dtype in (Define.BINARY, Define.VARBINARY):
            # fmt: off
            # base
            d = self.setup_definition(dtype(**args_base))
            self.assertDefinition(d, f"{d.data_type}(1) NOT NULL")
            # length
            d = self.setup_definition(dtype(**(args_base | {"length": 255})))
            self.assertDefinition(d, f"{d.data_type}(255) NOT NULL")
            # null
            d = self.setup_definition(dtype(**(args_base | {"null": True})))
            self.assertDefinition(d, f"{d.data_type}(1)")
            # default
            d = self.setup_definition(dtype(**(args_base | {"default": b"Binary&Varbinary"})))
            self.assertDefinition(d, f"{d.data_type}(1) NOT NULL DEFAULT _binary'Binary&Varbinary'")
            # comment
            d = self.setup_definition(dtype(**(args_base | {"comment": "Comment"})))
            self.assertDefinition(d, f"{d.data_type}(1) NOT NULL COMMENT 'Comment'")
            # visible
            d = self.setup_definition(dtype(**(args_base | {"visible": False})))
            self.assertDefinition(d, f"{d.data_type}(1) NOT NULL INVISIBLE")
            # all
            d = self.setup_definition(dtype(**(args_all)))
            self.assertDefinition(d, f"{d.data_type}(255) DEFAULT _binary'Binary&Varbinary' COMMENT 'Comment' INVISIBLE")
            # fmt: on

        self.log_ended("BINARY & VARBINARY")

    def test_blob(self) -> None:
        self.log_start("BLOB")

        args_base = {
            "null": False,
            "comment": None,
            "visible": True,
        }
        args_all = {
            "null": True,
            "comment": "Comment",
            "visible": False,
        }
        for dtype in (
            Define.TINYBLOB,
            Define.BLOB,
            Define.MEDIUMBLOB,
            Define.LONGBLOB,
        ):
            # fmt: off
            # base
            d = self.setup_definition(dtype(**args_base))
            self.assertDefinition(d, f"{d.data_type} NOT NULL")
            # null
            d = self.setup_definition(dtype(**(args_base | {"null": True})))
            self.assertDefinition(d, f"{d.data_type}")
            # comment
            d = self.setup_definition(dtype(**(args_base | {"comment": "Comment"})))
            self.assertDefinition(d, f"{d.data_type} NOT NULL COMMENT 'Comment'")
            # visible
            d = self.setup_definition(dtype(**(args_base | {"visible": False})))
            self.assertDefinition(d, f"{d.data_type} NOT NULL INVISIBLE")
            # all
            d = self.setup_definition(dtype(**(args_all)))
            self.assertDefinition(d, f"{d.data_type} COMMENT 'Comment' INVISIBLE")
            # fmt: on

        self.log_ended("BLOB")

    def test_generated_column(self) -> None:
        self.log_start("GENERATED COLUMN")

        for dtype, cmp in (
            # Integer
            (Define.TINYINT(), "TINYINT"),
            (Define.SMALLINT(), "SMALLINT"),
            (Define.MEDIUMINT(), "MEDIUMINT"),
            (Define.INT(), "INT"),
            (Define.BIGINT(), "BIGINT"),
            (Define.BIGINT(unsigned=True), "BIGINT UNSIGNED"),
            # Floating Point
            (Define.FLOAT(), "FLOAT"),
            (Define.DOUBLE(), "DOUBLE"),
            # Fixed Point
            (Define.DECIMAL(12, 2), "DECIMAL(12,2)"),
            # Temporal
            (Define.DATE(), "DATE"),
            (Define.DATETIME(), "DATETIME"),
            (Define.DATETIME(3), "DATETIME(3)"),
            (Define.TIMESTAMP(), "TIMESTAMP"),
            (Define.TIMESTAMP(3), "TIMESTAMP(3)"),
            (Define.TIME(), "TIME"),
            (Define.TIME(3), "TIME(3)"),
            (Define.YEAR(), "YEAR"),
            # Char String
            (Define.CHAR(), "CHAR"),
            (Define.VARCHAR(255), "VARCHAR(255)"),
            (Define.TINYTEXT(), "TINYTEXT"),
            (Define.TEXT(), "TEXT"),
            (Define.MEDIUMTEXT(), "MEDIUMTEXT"),
            (Define.LONGTEXT(), "LONGTEXT"),
            (Define.ENUM("a", "b", "c"), "ENUM('a','b','c')"),
            # Binary String
            (Define.BINARY(), "BINARY"),
            (Define.VARBINARY(255), "VARBINARY(255)"),
            (Define.TINYBLOB(), "TINYBLOB"),
            (Define.BLOB(), "BLOB"),
            (Define.MEDIUMBLOB(), "MEDIUMBLOB"),
            (Define.LONGBLOB(), "LONGBLOB"),
        ):
            col = self.setup_column(GeneratedColumn(dtype, "col1 + col2"), "col_g")
            self.assertEqual(
                col._gen_definition_sql(),
                f"col_g {cmp} GENERATED ALWAYS AS (col1 + col2) NOT NULL",
            )

        # Stored
        col = self.setup_column(
            GeneratedColumn(Define.INT(), "col1 + col2", virtual=False), "col_g"
        )
        self.assertEqual(
            col._gen_definition_sql(),
            "col_g INT GENERATED ALWAYS AS (col1 + col2) STORED NOT NULL",
        )

        # NULL
        col = self.setup_column(
            GeneratedColumn(Define.INT(null=True), "col1 + col2", virtual=False),
            "col_g",
        )
        self.assertEqual(
            col._gen_definition_sql(),
            "col_g INT GENERATED ALWAYS AS (col1 + col2) STORED",
        )

        # Comment
        col = self.setup_column(
            GeneratedColumn(
                Define.INT(comment="Comment"), "col1 + col2", virtual=False
            ),
            "col_g",
        )
        self.assertEqual(
            col._gen_definition_sql(),
            "col_g INT GENERATED ALWAYS AS (col1 + col2) STORED NOT NULL COMMENT 'Comment'",
        )

        # Invisible
        col = self.setup_column(
            GeneratedColumn(Define.INT(visible=False), "col1 + col2", virtual=False),
            "col_g",
        )
        self.assertEqual(
            col._gen_definition_sql(),
            "col_g INT GENERATED ALWAYS AS (col1 + col2) STORED NOT NULL INVISIBLE",
        )

        # All
        col = self.setup_column(
            GeneratedColumn(
                Define.INT(True, True, 1, True, "Comment", False),
                "col1 + col2",
                virtual=False,
            ),
            "col_g",
        )
        self.assertEqual(
            col._gen_definition_sql(),
            "col_g INT UNSIGNED GENERATED ALWAYS AS (col1 + col2) STORED COMMENT 'Comment' INVISIBLE",
        )

        # SQLfunction
        col = self.setup_column(
            GeneratedColumn(Define.INT(), sqlfunc.SQRT("col1 * col2")), "col_g"
        )
        self.assertEqual(
            col._gen_definition_sql(),
            "col_g INT GENERATED ALWAYS AS (SQRT(col1 * col2)) NOT NULL",
        )

        col = self.setup_column(
            GeneratedColumn(Define.INT(), sqlfunc.CONCAT("col1", "' '", "col2")),
            "col_g",
        )
        self.assertEqual(
            col._gen_definition_sql(),
            "col_g INT GENERATED ALWAYS AS (CONCAT(col1,' ',col2)) NOT NULL",
        )

        self.log_ended("GENERATED COLUMN")

    def test_columns(self) -> None:
        self.log_start("Columns")

        _cols = []
        for dtype in self.dtypes:
            _cols.append(
                self.setup_column(
                    Column(dtype),
                    f"col_{dtype.data_type.lower()}",
                )
            )

        col_g = self.setup_column(
            GeneratedColumn(
                Define.INT(), sqlfunc.CONCAT("col1", "' '", "col2", "' '", "col3")
            ),
            "col_g",
        )
        _cols.append(col_g)

        cols = Columns(*_cols)
        self.assertEqual(len(cols.search_name("col_int")), 1)
        self.assertEqual(len(cols.search_name("int", exact=False)), 5)
        self.assertEqual(len(cols.search_name(["col_int", "col_char"])), 2)
        self.assertEqual(len(cols.search_name(["int", "char"], exact=False)), 7)
        self.assertEqual(len(cols.search_type("INT")), 2)
        self.assertEqual(len(cols.search_type("INT", exact=False)), 6)
        self.assertEqual(
            len(cols.search_type(["INT", Define.BIGINT, Define.TINYINT])), 4
        )
        self.assertTrue(cols.issubset(["col_int", cols["col_char"], cols]))
        self.assertFalse(cols.issubset("col_intx"))
        self.assertEqual(len(cols.filter("col_intx")), 0)
        self.assertEqual(len(cols.filter("col_int")), 1)
        self.assertEqual(len(cols.filter(["col_int", cols["col_char"]])), 2)
        self.assertEqual(len(cols.filter(cols)), len(cols))
        self.assertIs(cols.get("col_int"), cols["col_int"])
        self.assertIs(cols.get("intx", None), None)
        for c in cols:
            self.assertIsInstance(c, Column)
        self.assertIn("col_int", cols)
        self.assertIn(cols["col_int"], cols)
        self.assertNotIn("col_intx", cols)
        # cols.setup("tb1", "db1", "utf8", None, self.get_pool())
        cols._gen_definition_sql()

        # Conflicts
        with self.assertRaises(errors.ColumnArgumentError):
            cols = Columns(None)
        with self.assertRaises(errors.ColumnArgumentError):
            cols = Columns(*cols, cols["col_int"])

        self.log_ended("Columns")


class TestColumnCopy(TestCase):
    name = "Column Copy"

    def test_all(self) -> None:
        self.log_start("COPY")

        class TestTable(Table):
            id: Column = Column(Define.BIGINT())
            g_col: GeneratedColumn = GeneratedColumn(Define.INT(), "id")

        class TestDatabase(Database):
            tb1: TestTable = TestTable()
            tb2: TestTable = TestTable()

        db = TestDatabase("test_db", self.get_pool())
        self.assertIsNot(db.tb1.id, db.tb2.id)
        self.assertIsNot(db.tb1.g_col, db.tb2.g_col)
        self.assertIsNot(db.tb1["id"], db.tb2["id"])
        self.assertIsNot(db.tb1["g_col"], db.tb2["g_col"])
        self.assertIsNot(db.tb1.id.definition, db.tb2.id.definition)
        self.assertIsNot(db.tb1.g_col.definition, db.tb2.g_col.definition)

        self.log_ended("COPY")


class TestColumnSyncSQL(TestCase):
    name: str = "Column Sync SQL"

    def test_all(self) -> None:
        self.test_column_basic_sql()
        self.test_generated_column_basic_sql()

    def test_column_basic_sql(self) -> None:
        self.log_start("COLUMN BASIC SQL")

        # Add & Exists & Drop
        class Integer(Table):
            # Basic
            t_int: Column = Column(Define.TINYINT())
            s_int: Column = Column(Define.SMALLINT())
            m_int: Column = Column(Define.MEDIUMINT())
            i_int: Column = Column(Define.INT())
            b_int: Column = Column(Define.BIGINT())

        class TestDatabase(Database):
            tb: Integer = Integer()

        db = TestDatabase("test_db", self.get_pool())
        self.assertTrue(db.Drop(True))
        self.assertTrue(db.Create())
        self.assertTrue(db.tb.Create())
        size = len(db.tb)
        for col in db.tb:
            for pos in range(size + 2):
                self.assertTrue(col.Exists())
                self.assertTrue(col.Drop())
                self.assertFalse(col.Exists())
                col.Add(pos)
                self.assertTrue(col.Exists())
                meta = col.ShowMetadata()
                if pos < 1 or pos > size:
                    self.assertEqual(col.position, size)
                    self.assertEqual(meta.position, size)
                else:
                    self.assertEqual(col.position, pos)
                    self.assertEqual(meta.position, pos)
        self.assertTrue(db.Drop())

        # Validate metadata
        class Integer(Table):
            # Basic
            t_int: Column = Column(Define.TINYINT())
            s_int: Column = Column(Define.SMALLINT())
            m_int: Column = Column(Define.MEDIUMINT())
            i_int: Column = Column(Define.INT())
            b_int: Column = Column(Define.BIGINT())
            # Unsigned
            t_int_u: Column = Column(Define.TINYINT(unsigned=True))
            s_int_u: Column = Column(Define.SMALLINT(unsigned=True))
            m_int_u: Column = Column(Define.MEDIUMINT(unsigned=True))
            i_int_u: Column = Column(Define.INT(unsigned=True))
            b_int_u: Column = Column(Define.BIGINT(unsigned=True))
            # Null
            t_int_n: Column = Column(Define.TINYINT(null=True))
            s_int_n: Column = Column(Define.SMALLINT(null=True))
            m_int_n: Column = Column(Define.MEDIUMINT(null=True))
            i_int_n: Column = Column(Define.INT(null=True))
            b_int_n: Column = Column(Define.BIGINT(null=True))
            # Default
            t_int_d: Column = Column(Define.TINYINT(default=1))
            s_int_d: Column = Column(Define.SMALLINT(default=1))
            m_int_d: Column = Column(Define.MEDIUMINT(default=1))
            i_int_d: Column = Column(Define.INT(default=1))
            b_int_d: Column = Column(Define.BIGINT(default=1))
            # Comment
            t_int_c: Column = Column(Define.TINYINT(comment="Comment"))
            s_int_c: Column = Column(Define.SMALLINT(comment="Comment"))
            m_int_c: Column = Column(Define.MEDIUMINT(comment="Comment"))
            i_int_c: Column = Column(Define.INT(comment="Comment"))
            b_int_c: Column = Column(Define.BIGINT(comment="描述"))
            # Visible
            t_int_v: Column = Column(Define.TINYINT(visible=False))
            s_int_v: Column = Column(Define.SMALLINT(visible=False))
            m_int_v: Column = Column(Define.MEDIUMINT(visible=False))
            i_int_v: Column = Column(Define.INT(visible=False))
            b_int_v: Column = Column(Define.BIGINT(visible=False))
            # AUTO_INCREATMENT
            int_k: Column = Column(Define.INT(auto_increment=True))
            uk: UniqueKey = UniqueKey("int_k")

        class FPoint(Table):
            # Basic
            float_c: Column = Column(Define.FLOAT())
            double_c: Column = Column(Define.DOUBLE())
            decimal_c: Column = Column(Define.DECIMAL())
            # Null
            float_c_n: Column = Column(Define.FLOAT(null=True))
            double_c_n: Column = Column(Define.DOUBLE(null=True))
            decimal_c_n: Column = Column(Define.DECIMAL(null=True))
            # Default
            float_c_d: Column = Column(Define.FLOAT(default="1.1"))
            double_c_d: Column = Column(Define.DOUBLE(default="1.1"))
            decimal_c_d: Column = Column(Define.DECIMAL(10, 2, default="1.1"))
            # Comment
            float_c_c: Column = Column(Define.FLOAT(comment="Comment"))
            double_c_c: Column = Column(Define.DOUBLE(comment="Comment"))
            decimal_c_c: Column = Column(Define.DECIMAL(comment="描述"))
            # Visible
            float_c_v: Column = Column(Define.FLOAT(visible=False))
            double_c_v: Column = Column(Define.DOUBLE(visible=False))
            decimal_c_v: Column = Column(Define.DECIMAL(visible=False))
            # Precision
            double_c_p: Column = Column(Define.DOUBLE(default=1.289127389123897912783))
            decimal_c_p1: Column = Column(Define.DECIMAL(default="1.4"))
            decimal_c_p2: Column = Column(Define.DECIMAL(default="1.6"))
            decimal_c_p3: Column = Column(Define.DECIMAL(10, 2, default="1.444"))
            decimal_c_p4: Column = Column(Define.DECIMAL(10, 2, default="1.555"))
            decimal_c_p5: Column = Column(Define.DECIMAL(10, 2, default="1.666"))

        class Temporal(Table):
            # fmt: off
            # Basic
            date_c: Column = Column(Define.DATE())
            dt: Column = Column(Define.DATETIME())
            ts: Column = Column(Define.TIMESTAMP())
            time_c: Column = Column(Define.TIME())
            year_c: Column = Column(Define.YEAR())
            # Null
            date_c_n: Column = Column(Define.DATE(null=True))
            dt_n: Column = Column(Define.DATETIME(null=True))
            ts_n: Column = Column(Define.TIMESTAMP(null=True))
            time_c_n: Column = Column(Define.TIME(null=True))
            year_c_n: Column = Column(Define.YEAR(null=True))
            # Default
            date_c_d1: Column = Column(Define.DATE(default=datetime.datetime(2000, 1, 1)))
            date_c_d2: Column = Column(Define.DATE(default="2000-01-01 01:01:01"))
            dt_d: Column = Column(Define.DATETIME(default=datetime.datetime(2000, 1, 1, 1, 1, 1, 440000)))
            ts_d: Column = Column(Define.TIMESTAMP(default="2000-01-01 01:01:01.66"))
            time_c_d1: Column = Column(Define.TIME(default=datetime.time(1, 1, 1, 440000)))
            time_c_d2: Column = Column(Define.TIME(default="01:01:01.66"))
            year_c_d1: Column = Column(Define.YEAR(default=69))
            year_c_d2: Column = Column(Define.YEAR(default=99))
            year_c_d3: Column = Column(Define.YEAR(default="2100"))
            # Comment
            date_c_c: Column = Column(Define.DATE(comment="Comment"))
            dt_c: Column = Column(Define.DATETIME(comment="Comment"))
            ts_c: Column = Column(Define.TIMESTAMP(comment="Comment"))
            time_c_c: Column = Column(Define.TIME(comment="Comment"))
            year_c_c: Column = Column(Define.YEAR(comment="描述"))
            # Visible
            date_c_v: Column = Column(Define.DATE(visible=False))
            dt_v: Column = Column(Define.DATETIME(visible=False))
            ts_v: Column = Column(Define.TIMESTAMP(visible=False))
            time_c_v: Column = Column(Define.TIME(visible=False))
            year_c_v: Column = Column(Define.YEAR(visible=False))
            # Auto init & Auto update
            dt_auto_i: Column = Column(Define.DATETIME(auto_init=True))
            ts_auto_i: Column = Column(Define.TIMESTAMP(auto_init=True))
            dt_auto_u: Column = Column(Define.DATETIME(auto_update=True))
            ts_auto_u: Column = Column(Define.TIMESTAMP(auto_update=True))
            dt_auto_iu: Column = Column(Define.DATETIME(auto_init=True, auto_update=True))
            ts_auto_iu: Column = Column(Define.TIMESTAMP(auto_init=True, auto_update=True))
            # Precision
            dt_p1: Column = Column(Define.DATETIME(default="2000-01-01 01:01:01.6"))
            dt_p2: Column = Column(Define.DATETIME(0, default="2000-01-01 01:01:01.666666"))
            dt_p3: Column = Column(Define.DATETIME(1, default="2000-01-01 01:01:01.666666"))
            dt_p4: Column = Column(Define.DATETIME(2, default="2000-01-01 01:01:01.666666"))
            dt_p5: Column = Column(Define.DATETIME(3, default="2000-01-01 01:01:01.666666"))
            dt_p6: Column = Column(Define.DATETIME(4, default="2000-01-01 01:01:01.666666"))
            dt_p7: Column = Column(Define.DATETIME(5, default="2000-01-01 01:01:01.666666"))
            dt_p8: Column = Column(Define.DATETIME(6, default="2000-01-01 01:01:01.666666"))
            ts_p1: Column = Column(Define.TIMESTAMP(default="2000-01-01 01:01:01.6"))
            ts_p2: Column = Column(Define.TIMESTAMP(0, default="2000-01-01 01:01:01.666666"))
            ts_p3: Column = Column(Define.TIMESTAMP(1, default="2000-01-01 01:01:01.666666"))
            ts_p4: Column = Column(Define.TIMESTAMP(2, default="2000-01-01 01:01:01.666666"))
            ts_p5: Column = Column(Define.TIMESTAMP(3, default="2000-01-01 01:01:01.666666"))
            ts_p6: Column = Column(Define.TIMESTAMP(4, default="2000-01-01 01:01:01.666666"))
            ts_p7: Column = Column(Define.TIMESTAMP(5, default="2000-01-01 01:01:01.666666"))
            ts_p8: Column = Column(Define.TIMESTAMP(6, default="2000-01-01 01:01:01.666666"))
            time_p1: Column = Column(Define.TIME(default="01:01:01.6"))
            time_p2: Column = Column(Define.TIME(0, default="01:01:01.666666"))
            time_p3: Column = Column(Define.TIME(1, default="01:01:01.666666"))
            time_p4: Column = Column(Define.TIME(2, default="01:01:01.666666"))
            time_p5: Column = Column(Define.TIME(3, default="01:01:01.666666"))
            time_p6: Column = Column(Define.TIME(4, default="01:01:01.666666"))
            time_p7: Column = Column(Define.TIME(5, default="01:01:01.666666"))
            time_p8: Column = Column(Define.TIME(6, default="01:01:01.666666"))
            # fmt: on

        class ChString(Table):
            # Basic
            ch: Column = Column(Define.CHAR())
            vch: Column = Column(Define.VARCHAR(100))
            tt: Column = Column(Define.TINYTEXT())
            tx: Column = Column(Define.TEXT())
            mt: Column = Column(Define.MEDIUMTEXT())
            lt: Column = Column(Define.LONGTEXT())
            en: Column = Column(Define.ENUM("A", "B", "C"))
            # Null
            ch_n: Column = Column(Define.CHAR(null=True))
            vch_n: Column = Column(Define.VARCHAR(255, null=True))
            tt_n: Column = Column(Define.TINYTEXT(null=True))
            tx_n: Column = Column(Define.TEXT(null=True))
            mt_n: Column = Column(Define.MEDIUMTEXT(null=True))
            lt_n: Column = Column(Define.LONGTEXT(null=True))
            en_n: Column = Column(Define.ENUM("A", "B", "C", null=True))
            # Default
            ch_d: Column = Column(Define.CHAR(default="D"))
            vch_d: Column = Column(Define.VARCHAR(255, default="Default"))
            en_d: Column = Column(Define.ENUM("A", "B", "C", default="A"))
            # Comment
            ch_c: Column = Column(Define.CHAR(comment="Comment"))
            vch_c: Column = Column(Define.VARCHAR(255, comment="Comment"))
            tt_c: Column = Column(Define.TINYTEXT(comment="Comment"))
            tx_c: Column = Column(Define.TEXT(comment="Comment"))
            mt_c: Column = Column(Define.MEDIUMTEXT(comment="Comment"))
            lt_c: Column = Column(Define.LONGTEXT(comment="Comment"))
            en_c: Column = Column(Define.ENUM("A", "B", "C", comment="描述"))
            # Visible
            ch_v: Column = Column(Define.CHAR(visible=False))
            vch_v: Column = Column(Define.VARCHAR(255, visible=False))
            tt_v: Column = Column(Define.TINYTEXT(visible=False))
            tx_v: Column = Column(Define.TEXT(visible=False))
            mt_v: Column = Column(Define.MEDIUMTEXT(visible=False))
            lt_v: Column = Column(Define.LONGTEXT(visible=False))
            en_v: Column = Column(Define.ENUM("A", "B", "C", visible=False))
            # Length
            ch_l1: Column = Column(Define.CHAR(1))
            ch_l2: Column = Column(Define.CHAR(255))
            vch_l1: Column = Column(Define.VARCHAR(1))
            vch_l2: Column = Column(Define.VARCHAR(255))

        class BiString(Table):
            # Basic
            bi: Column = Column(Define.BINARY())
            vbi: Column = Column(Define.VARBINARY(100))
            tb: Column = Column(Define.TINYBLOB())
            bl: Column = Column(Define.BLOB())
            mb: Column = Column(Define.MEDIUMBLOB())
            lb: Column = Column(Define.LONGBLOB())
            # Null
            bi_n: Column = Column(Define.BINARY(null=True))
            vbi_n: Column = Column(Define.VARBINARY(100, null=True))
            tb_n: Column = Column(Define.TINYBLOB(null=True))
            bl_n: Column = Column(Define.BLOB(null=True))
            mb_n: Column = Column(Define.MEDIUMBLOB(null=True))
            lb_n: Column = Column(Define.LONGBLOB(null=True))
            # Default
            bi_d: Column = Column(Define.BINARY(default=b"D"))
            vbi_d: Column = Column(Define.VARBINARY(100, default="默认".encode()))
            # Comment
            bi_c: Column = Column(Define.BINARY(comment="Comment"))
            vbi_c: Column = Column(Define.VARBINARY(100, comment="Comment"))
            tb_c: Column = Column(Define.TINYBLOB(comment="Comment"))
            bl_c: Column = Column(Define.BLOB(comment="Comment"))
            mb_c: Column = Column(Define.MEDIUMBLOB(comment="Comment"))
            lb_c: Column = Column(Define.LONGBLOB(comment="描述"))
            # Visible
            bi_v: Column = Column(Define.BINARY(visible=False))
            vbi_v: Column = Column(Define.VARBINARY(100, visible=False))
            tb_v: Column = Column(Define.TINYBLOB(visible=False))
            bl_v: Column = Column(Define.BLOB(visible=False))
            mb_v: Column = Column(Define.MEDIUMBLOB(visible=False))
            lb_v: Column = Column(Define.LONGBLOB(visible=False))
            # Length
            bi_l1: Column = Column(Define.BINARY(1))
            bi_l2: Column = Column(Define.BINARY(255))
            vbi_l1: Column = Column(Define.VARBINARY(1))
            vbi_l2: Column = Column(Define.VARBINARY(255))

        class TestDatabase(Database):
            integer: Integer = Integer()
            fpoint: FPoint = FPoint()
            temporal: Temporal = Temporal()
            char: ChString = ChString()
            binary: BiString = BiString()

        db = TestDatabase("test_db", self.get_pool())
        with self.assertRaises(sqlerr.OperationalError):
            db.integer.int_k.ShowMetadata()
        self.assertTrue(db.Drop(True))
        self.assertTrue(db.Create())
        [tb.Create() for tb in db]
        for col in (
            *db.integer,
            *db.fpoint,
            *db.temporal,
            *db.char,
            *db.binary,
        ):
            # metadata should be identical
            meta = col.ShowMetadata()
            self.assertFalse(col._diff_from_metadata(meta))
        self.assertTrue(db.Drop())
        with self.assertRaises(sqlerr.OperationalError):
            db.integer.t_int.ShowMetadata()

        # Modify
        class TestTable(Table):
            int_c: Column = Column(Define.INT())
            float_c: Column = Column(Define.FLOAT())
            decimal_c: Column = Column(Define.DECIMAL())
            date_c: Column = Column(Define.DATE())
            dt_c: Column = Column(Define.DATETIME())
            ts_c: Column = Column(Define.TIMESTAMP())
            time_c: Column = Column(Define.TIME())
            year_c: Column = Column(Define.YEAR())
            char_c: Column = Column(Define.CHAR())
            vchar_c: Column = Column(Define.VARCHAR(255))
            text_c: Column = Column(Define.TEXT())
            enum_c: Column = Column(Define.ENUM("A", "B", "C"))
            bin_c: Column = Column(Define.BINARY())
            vbin_c: Column = Column(Define.VARBINARY(255))
            blob_c: Column = Column(Define.BLOB())

        class TestDatabase(Database):
            tb: TestTable = TestTable()

        db = TestDatabase("test_db", self.get_pool())
        self.assertTrue(db.Drop(True))
        self.assertTrue(db.Create())
        self.assertTrue(db.tb.Create())
        for col in db.tb:
            size = len(db.tb)
            # . position only
            ori_pos = col.position
            for pos in range(size + 3):
                col.Modify(position=pos)
                meta = col.ShowMetadata()
                if pos < 1:
                    self.assertEqual(ori_pos, col.position)
                    self.assertEqual(ori_pos, meta.position)
                elif pos >= size:
                    self.assertEqual(col.position, size)
                    self.assertEqual(meta.position, size)
                else:
                    self.assertEqual(col.position, pos)
                    self.assertEqual(meta.position, pos)
            # . visible on
            meta = col.ShowMetadata()
            self.assertTrue(col.definition.visible)
            self.assertTrue(meta.visible)
            # . default off
            self.assertIsNone(col.definition.default)
            self.assertIsNone(meta.default)
            # . definition only
            ori_def = col.definition
            for new_def in (
                # fmt: off
                Define.BIGINT(unsigned=True, null=True, default=1, comment="Comment", visible=False),
                Define.DOUBLE(null=True, default=1.1, comment="Comment", visible=False),
                Define.DECIMAL(12, 2, null=True, default=1.1, comment="Comment", visible=False),
                Define.DATE(null=True, default="1970-01-01", comment="Comment", visible=False),
                Define.DATETIME(6, null=True, default="1970-01-01", comment="Comment", visible=False),
                Define.TIME(6, null=True, default="01:01:01.666666", comment="Comment", visible=False),
                Define.YEAR(null=False, default=69, comment="Comment", visible=False),
                Define.CHAR(100, null=True, default="default", charset="utf8mb3", comment="Comment", visible=False),
                Define.VARCHAR(100, null=True, default="default", collate="utf8mb3_bin", comment="Comment", visible=False),
                Define.MEDIUMTEXT(null=True, collate="utf8mb4_bin", comment="Comment", visible=False),
                Define.ENUM("A", "B", "C", "D", null=True, default="A", charset="utf8mb4", comment="Comment", visible=False),
                Define.BINARY(100, null=True, default=b"default", comment="Comment", visible=False),
                Define.MEDIUMBLOB(null=True, comment="Comment", visible=False),
                Define.VARBINARY(100, null=True, default=b"default", comment="Comment", visible=False),
                # fmt: on
            ):
                col.Modify(new_def)
                self.assertEqual(col.definition, new_def)
            # . visible off
            meta = col.ShowMetadata()
            self.assertFalse(col.definition.visible)
            self.assertFalse(meta.visible)
            # . default on
            self.assertIsNotNone(col.definition.default)
            self.assertIsNotNone(meta.default)
            # . comment on
            self.assertEqual(col.definition.comment, "Comment")
            self.assertEqual(meta.comment, "Comment")
            # . visible on
            col.Modify(Define.INT())
            meta = col.ShowMetadata()
            self.assertTrue(col.definition.visible)
            self.assertTrue(meta.visible)
            # . default off
            self.assertIsNone(col.definition.default)
            self.assertIsNone(meta.default)
            # . comment off
            self.assertIsNone(col.definition.comment)
            self.assertIsNone(meta.comment)
            # . position & definition
            col.Modify(ori_def, ori_pos)
            self.assertEqual(col.definition, ori_def)
            self.assertEqual(col.position, ori_pos)
            meta = col.ShowMetadata()
            self.assertEqual(meta.position, ori_pos)

        # . modify: toggle visibility only
        # fmt: off
        col = db.tb.int_c
        logs = col.Modify(Define.INT(visible=False))
        self.assertIn("ALTER TABLE test_db.tb ALTER COLUMN int_c SET INVISIBLE", repr(logs))
        self.assertFalse(col.definition.visible)
        logs = col.SetVisible(True)
        self.assertIn("ALTER TABLE test_db.tb ALTER COLUMN int_c SET VISIBLE", repr(logs))
        self.assertTrue(col.definition.visible)
        # fmt: on

        # . modify: set default
        for col, dft, err in (
            (db.tb.int_c, 2, "x"),
            (db.tb.float_c, 2.1, "x"),
            (db.tb.decimal_c, Decimal("2"), "x"),
            (db.tb.date_c, datetime.date(2012, 1, 1), "x"),
            (db.tb.dt_c, datetime.datetime(2012, 1, 1), "x"),
            (db.tb.ts_c, datetime.datetime(2012, 1, 1), "x"),
            (db.tb.time_c, datetime.time(1, 1, 1), "x"),
            (db.tb.year_c, 2012, "x"),
            (db.tb.char_c, "d", "x"),
            (db.tb.vchar_c, "default", "x"),
            (db.tb.text_c, "default", "x"),
            (db.tb.enum_c, "A", "x"),
            (db.tb.bin_c, b"d", "x"),
            (db.tb.vbin_c, b"default", "x"),
            (db.tb.blob_c, b"default", "x"),
        ):
            self.assertIsNone(col.definition.default)
            if col in (db.tb.text_c, db.tb.blob_c):
                with self.assertRaises(sqlerr.OperationalError):
                    col.SetDefault(dft)
            else:
                logs = col.SetDefault(dft)
                self.assertEqual(col.definition.default, dft)
                if not isinstance(dft, str):
                    with self.assertRaises(errors.ColumnDefinitionError):
                        col.SetDefault(err)
                logs = col.SetDefault(None)
                self.assertIsNone(col.definition.default)

        # . modify: local-remote mismatch
        class TestTable2(Table):
            int_c: Column = Column(
                Define.INT(
                    unsigned=True,
                    null=True,
                    default=1,
                    comment="Comment",
                    visible=False,
                )
            )

        class TestDatabase2(Database):
            tb: TestTable2 = TestTable2()

        db2 = TestDatabase2("test_db", self.get_pool())
        col = db2.tb.int_c
        self.assertTrue(col.definition.unsigned)
        self.assertTrue(col.definition.null)
        self.assertEqual(col.definition.default, 1)
        self.assertEqual(col.definition.comment, "Comment")
        self.assertFalse(col.definition.visible)
        meta = col.ShowMetadata()
        self.assertFalse(meta.unsigned)
        self.assertFalse(meta.null)
        self.assertIsNone(meta.default)
        self.assertIsNone(meta.comment)
        self.assertTrue(meta.visible)
        col.Modify(Define.INT(null=True, comment="Comment"))
        self.assertFalse(col.definition.unsigned)
        self.assertTrue(col.definition.null)
        self.assertIsNone(col.definition.default)
        self.assertEqual(col.definition.comment, "Comment")
        self.assertTrue(col.definition.visible)
        meta = col.ShowMetadata()
        self.assertFalse(meta.unsigned)
        self.assertTrue(meta.null)
        self.assertIsNone(meta.default)
        self.assertEqual(meta.comment, "Comment")
        self.assertTrue(meta.visible)
        self.assertTrue(col.Drop())

        # Sync from remote
        db.tb.int_c.Add(1)

        class TestTable2(Table):
            int_c: Column = Column(
                Define.INT(
                    unsigned=True,
                    null=True,
                    default=1,
                    comment="Comment",
                    visible=False,
                )
            )

        class TestDatabase2(Database):
            tb: TestTable2 = TestTable2()

        db2 = TestDatabase2("test_db", self.get_pool())
        col = db2.tb.int_c
        # . before sync
        self.assertTrue(col.definition.unsigned)
        self.assertTrue(col.definition.null)
        self.assertEqual(col.definition.default, 1)
        self.assertEqual(col.definition.comment, "Comment")
        self.assertFalse(col.definition.visible)
        # . after sync
        col.SyncFromRemote()
        self.assertFalse(col.definition.unsigned)
        self.assertFalse(col.definition.null)
        self.assertIsNone(col.definition.default)
        self.assertIsNone(col.definition.comment)
        self.assertTrue(col.definition.visible)
        meta = col.ShowMetadata()
        self.assertFalse(meta.unsigned)
        self.assertFalse(meta.null)
        self.assertIsNone(meta.default)
        self.assertIsNone(meta.comment)
        self.assertTrue(meta.visible)

        # Sync to remote
        self.assertTrue(col.Drop())
        self.assertTrue(db.tb.int_c.SyncToRemote())  # re-create

        class TestTable2(Table):
            int_c: Column = Column(
                Define.INT(
                    unsigned=True,
                    null=True,
                    default=1,
                    comment="Comment",
                    visible=False,
                )
            )

        class TestDatabase2(Database):
            tb: TestTable2 = TestTable2()

        db2 = TestDatabase2("test_db", self.get_pool())
        col = db2.tb.int_c
        # . before sync
        meta = col.ShowMetadata()
        self.assertFalse(meta.unsigned)
        self.assertFalse(meta.null)
        self.assertIsNone(meta.default)
        self.assertIsNone(meta.comment)
        self.assertTrue(meta.visible)
        # . after sync
        col.SyncToRemote()
        self.assertTrue(col.definition.unsigned)
        self.assertTrue(col.definition.null)
        self.assertEqual(col.definition.default, 1)
        self.assertEqual(col.definition.comment, "Comment")
        self.assertFalse(col.definition.visible)
        meta = col.ShowMetadata()
        self.assertTrue(meta.unsigned)
        self.assertTrue(meta.null)
        self.assertEqual(meta.default, "1")
        self.assertEqual(meta.comment, "Comment")
        self.assertFalse(meta.visible)
        self.assertTrue(col.Drop())

        # Finished
        self.assertTrue(db.Drop(True))
        self.log_ended("COLUMN BASIC SQL")

    def test_generated_column_basic_sql(self) -> None:
        self.log_start("GENERATED COLUMN BASIC SQL")

        # Add & Exists & Drop
        class TestTable(Table):
            # fmt: off
            int_1: Column = Column(Define.INT())
            int_2: Column = Column(Define.INT())
            gen_1: GeneratedColumn = GeneratedColumn(Define.INT(), "int_1 + int_2")
            gen_2: GeneratedColumn = GeneratedColumn(Define.INT(), "int_1 * int_2", virtual=True)
            # fmt: on

        class TestDatabase(Database):
            tb: TestTable = TestTable()

        db = TestDatabase("test_db", self.get_pool())
        db.Drop(True)
        db.Create()
        db.tb.Create()
        size = len(db.tb)
        for col in (db.tb.gen_1, db.tb.gen_2):
            for pos in range(size + 2):
                self.assertTrue(col.Exists())
                self.assertTrue(col.Drop())
                self.assertFalse(col.Exists())
                col.Add(pos)
                self.assertTrue(col.Exists())
                meta = col.ShowMetadata()
                if pos < 1 or pos > size:
                    self.assertEqual(col.position, size)
                    self.assertEqual(meta.position, size)
                else:
                    self.assertEqual(col.position, pos)
                    self.assertEqual(meta.position, pos)
        db.Drop()

        # Validate metadata
        class Integer(Table):
            expr = "c_int1 + c_int2"
            # fmt: off
            # Real Column
            c_int1: Column = Column(Define.TINYINT())
            c_int2: Column = Column(Define.TINYINT())
            # Basic
            g_int1: GeneratedColumn = GeneratedColumn(Define.TINYINT(), expr)
            g_int2: GeneratedColumn = GeneratedColumn(Define.SMALLINT(), expr)
            g_int3: GeneratedColumn = GeneratedColumn(Define.MEDIUMINT(), expr)
            g_int4: GeneratedColumn = GeneratedColumn(Define.INT(), expr)
            g_int5: GeneratedColumn = GeneratedColumn(Define.BIGINT(), expr)
            # Unsigned
            g_int1_u: GeneratedColumn = GeneratedColumn(Define.TINYINT(unsigned=True), expr)
            g_int2_u: GeneratedColumn = GeneratedColumn(Define.SMALLINT(unsigned=True), expr)
            g_int3_u: GeneratedColumn = GeneratedColumn(Define.MEDIUMINT(unsigned=True), expr)
            g_int4_u: GeneratedColumn = GeneratedColumn(Define.INT(unsigned=True), expr)
            g_int5_u: GeneratedColumn = GeneratedColumn(Define.BIGINT(unsigned=True), expr)
            # Null
            g_int1_n: GeneratedColumn = GeneratedColumn(Define.TINYINT(null=True), expr)
            g_int2_n: GeneratedColumn = GeneratedColumn(Define.SMALLINT(null=True), expr)
            g_int3_n: GeneratedColumn = GeneratedColumn(Define.MEDIUMINT(null=True), expr)
            g_int4_n: GeneratedColumn = GeneratedColumn(Define.INT(null=True), expr)
            g_int5_n: GeneratedColumn = GeneratedColumn(Define.BIGINT(null=True), expr)
            # Comment
            g_int1_c: GeneratedColumn = GeneratedColumn(Define.TINYINT(comment="Comment"), expr)
            g_int2_c: GeneratedColumn = GeneratedColumn(Define.SMALLINT(comment="Comment"), expr)
            g_int3_c: GeneratedColumn = GeneratedColumn(Define.MEDIUMINT(comment="Comment"), expr)
            g_int4_c: GeneratedColumn = GeneratedColumn(Define.INT(comment="Comment"), expr)
            g_int5_c: GeneratedColumn = GeneratedColumn(Define.BIGINT(comment="Comment"), expr)
            # Visible
            g_int1_v: GeneratedColumn = GeneratedColumn(Define.TINYINT(visible=False), expr)
            g_int2_v: GeneratedColumn = GeneratedColumn(Define.SMALLINT(visible=False), expr)
            g_int3_v: GeneratedColumn = GeneratedColumn(Define.MEDIUMINT(visible=False), expr)
            g_int4_v: GeneratedColumn = GeneratedColumn(Define.INT(visible=False), expr)
            g_int5_v: GeneratedColumn = GeneratedColumn(Define.BIGINT(visible=False), expr)
            # Default (default should be omitted)
            g_int_d: GeneratedColumn = GeneratedColumn(Define.INT(default=1), expr)
            # Auto Increment
            g_int_auto: GeneratedColumn = GeneratedColumn(Define.INT(auto_increment=True), expr)
            # fmt: on

        class FPoint(Table):
            expr = "c_flt + c_dbl + c_dec"
            # fmt: off
            # Real Column
            c_flt: Column = Column(Define.FLOAT())
            c_dbl: Column = Column(Define.DOUBLE())
            c_dec: Column = Column(Define.DECIMAL())
            # Basic
            g_flt: GeneratedColumn = GeneratedColumn(Define.FLOAT(), expr)
            g_dbl: GeneratedColumn = GeneratedColumn(Define.DOUBLE(), expr)
            g_dec: GeneratedColumn = GeneratedColumn(Define.DECIMAL(), expr)
            # Null
            g_flt_n: GeneratedColumn = GeneratedColumn(Define.FLOAT(null=True), expr)
            g_dbl_n: GeneratedColumn = GeneratedColumn(Define.DOUBLE(null=True), expr)
            g_dec_n: GeneratedColumn = GeneratedColumn(Define.DECIMAL(null=True), expr)
            # Comment
            g_flt_c: GeneratedColumn = GeneratedColumn(Define.FLOAT(comment="Comment"), expr)
            g_dbl_c: GeneratedColumn = GeneratedColumn(Define.DOUBLE(comment="Comment"), expr)
            g_dec_c: GeneratedColumn = GeneratedColumn(Define.DECIMAL(comment="Comment"), expr)
            # Visible
            g_flt_v: GeneratedColumn = GeneratedColumn(Define.FLOAT(visible=False), expr)
            g_dbl_v: GeneratedColumn = GeneratedColumn(Define.DOUBLE(visible=False), expr)
            g_dec_v: GeneratedColumn = GeneratedColumn(Define.DECIMAL(visible=False), expr)
            # Default (default should be omitted)
            g_flt_d: GeneratedColumn = GeneratedColumn(Define.FLOAT(default="1.1"), expr)
            g_dbl_d: GeneratedColumn = GeneratedColumn(Define.DOUBLE(default="1.1"), expr)
            g_dec_d: GeneratedColumn = GeneratedColumn(Define.DECIMAL(10, 2, default="1.1"), expr)
            # Precision
            g_dec_precision: GeneratedColumn = GeneratedColumn(Define.DECIMAL(10, 2), expr)
            # fmt: on

        class Temporal(Table):
            # fmt: off
            # Real Column
            c_date: Column = Column(Define.DATE())
            c_dt: Column = Column(Define.DATETIME())
            c_ts: Column = Column(Define.TIMESTAMP())
            c_time: Column = Column(Define.TIME())
            c_year: Column = Column(Define.YEAR())
            # Basic
            g_date: GeneratedColumn = GeneratedColumn(Define.DATE(), "c_date")
            g_dt: GeneratedColumn = GeneratedColumn(Define.DATETIME(), "c_dt")
            g_ts: GeneratedColumn = GeneratedColumn(Define.TIMESTAMP(), "c_ts")
            g_time: GeneratedColumn = GeneratedColumn(Define.TIME(), "c_time")
            g_year: GeneratedColumn = GeneratedColumn(Define.YEAR(), "c_year")
            # Null
            g_date_n: GeneratedColumn = GeneratedColumn(Define.DATE(null=True), "c_date")
            g_dt_n: GeneratedColumn = GeneratedColumn(Define.DATETIME(null=True), "c_dt")
            g_ts_n: GeneratedColumn = GeneratedColumn(Define.TIMESTAMP(null=True), "c_ts")
            g_time_n: GeneratedColumn = GeneratedColumn(Define.TIME(null=True), "c_time")
            g_year_n: GeneratedColumn = GeneratedColumn(Define.YEAR(null=True), "c_year")
            # Comment
            g_date_c: GeneratedColumn = GeneratedColumn(Define.DATE(comment="Comment"), "c_date")
            g_dt_c: GeneratedColumn = GeneratedColumn(Define.DATETIME(comment="Comment"), "c_dt")
            g_ts_c: GeneratedColumn = GeneratedColumn(Define.TIMESTAMP(comment="Comment"), "c_ts")
            g_time_c: GeneratedColumn = GeneratedColumn(Define.TIME(comment="Comment"), "c_time")
            g_year_c: GeneratedColumn = GeneratedColumn(Define.YEAR(comment="Comment"), "c_year")
            # Visible
            g_date_v: GeneratedColumn = GeneratedColumn(Define.DATE(visible=False), "c_date")
            g_dt_v: GeneratedColumn = GeneratedColumn(Define.DATETIME(visible=False), "c_dt")
            g_ts_v: GeneratedColumn = GeneratedColumn(Define.TIMESTAMP(visible=False), "c_ts")
            g_time_v: GeneratedColumn = GeneratedColumn(Define.TIME(visible=False), "c_time")
            g_year_v: GeneratedColumn = GeneratedColumn(Define.YEAR(visible=False), "c_year")
            # Default (default should be omitted)
            g_date_d: GeneratedColumn = GeneratedColumn(Define.DATE(default=datetime.datetime(2000, 1, 1)), "c_date")
            g_dt_d: GeneratedColumn = GeneratedColumn(Define.DATETIME(default="2000-01-01 01:01:01"), "c_dt")
            g_ts_d: GeneratedColumn = GeneratedColumn(Define.TIMESTAMP(default="2000-01-01 01:01:01.66"), "c_ts")
            g_time_d: GeneratedColumn = GeneratedColumn(Define.TIME(default=datetime.time(1, 1, 1, 440000)), "c_time")
            g_year_d: GeneratedColumn = GeneratedColumn(Define.YEAR(default=69), "c_year")
            # Auto init & Auto update (should be omitted)
            g_dt_auto: GeneratedColumn = GeneratedColumn(Define.DATETIME(auto_init=True, auto_update=True), "c_dt")
            # Precision
            g_dt_precision0: GeneratedColumn = GeneratedColumn(Define.DATETIME(0), "c_dt")
            g_dt_precision6: GeneratedColumn = GeneratedColumn(Define.DATETIME(6), "c_dt")
            g_ts_precision0: GeneratedColumn = GeneratedColumn(Define.TIMESTAMP(0), "c_ts")
            g_ts_precision6: GeneratedColumn = GeneratedColumn(Define.TIMESTAMP(6), "c_ts")
            g_time_precision0: GeneratedColumn = GeneratedColumn(Define.TIME(0), "c_time")
            g_time_precision6: GeneratedColumn = GeneratedColumn(Define.TIME(6), "c_time")
            # fmt: on

        class ChString(Table):
            # fmt: off
            # Normal Column
            c_ch: Column = Column(Define.CHAR())
            c_vch: Column = Column(Define.VARCHAR(100))
            c_tt: Column = Column(Define.TINYTEXT())
            c_tx: Column = Column(Define.TEXT())
            c_mt: Column = Column(Define.MEDIUMTEXT())
            c_lt: Column = Column(Define.LONGTEXT())
            c_en: Column = Column(Define.ENUM("A", "B", "C"))
            # Basic
            g_ch: GeneratedColumn = GeneratedColumn(Define.CHAR(), "c_ch")
            g_vch: GeneratedColumn = GeneratedColumn(Define.VARCHAR(100), "c_vch")
            g_tt: GeneratedColumn = GeneratedColumn(Define.TINYTEXT(), "c_tt")
            g_tx: GeneratedColumn = GeneratedColumn(Define.TEXT(), "c_tx")
            g_mt: GeneratedColumn = GeneratedColumn(Define.MEDIUMTEXT(), "c_mt")
            g_lt: GeneratedColumn = GeneratedColumn(Define.LONGTEXT(), "c_lt")
            g_en: GeneratedColumn = GeneratedColumn(Define.ENUM("A", "B", "C"), "c_en")
            # Null
            g_ch_n: GeneratedColumn = GeneratedColumn(Define.CHAR(null=True), "c_ch")
            g_vch_n: GeneratedColumn = GeneratedColumn(Define.VARCHAR(100, null=True), "c_vch")
            g_tt_n: GeneratedColumn = GeneratedColumn(Define.TINYTEXT(null=True), "c_tt")
            g_tx_n: GeneratedColumn = GeneratedColumn(Define.TEXT(null=True), "c_tx")
            g_mt_n: GeneratedColumn = GeneratedColumn(Define.MEDIUMTEXT(null=True), "c_mt")
            g_lt_n: GeneratedColumn = GeneratedColumn(Define.LONGTEXT(null=True), "c_lt")
            g_en_n: GeneratedColumn = GeneratedColumn(Define.ENUM("A", "B", "C", null=True), "c_en")
            # Comment
            g_ch_c: GeneratedColumn = GeneratedColumn(Define.CHAR(comment="Comment"), "c_ch")
            g_vch_c: GeneratedColumn = GeneratedColumn(Define.VARCHAR(255, comment="Comment"), "c_vch")
            g_tt_c: GeneratedColumn = GeneratedColumn(Define.TINYTEXT(comment="Comment"), "c_tt")
            g_tx_c: GeneratedColumn = GeneratedColumn(Define.TEXT(comment="Comment"), "c_tx")
            g_mt_c: GeneratedColumn = GeneratedColumn(Define.MEDIUMTEXT(comment="Comment"), "c_mt")
            g_lt_c: GeneratedColumn = GeneratedColumn(Define.LONGTEXT(comment="Comment"), "c_lt")
            g_en_c: GeneratedColumn = GeneratedColumn(Define.ENUM("A", "B", "C", comment="Comment"), "c_en")
            # Visible
            g_ch_v: GeneratedColumn = GeneratedColumn(Define.CHAR(visible=False), "c_ch")
            g_vch_v: GeneratedColumn = GeneratedColumn(Define.VARCHAR(255, visible=False), "c_vch")
            g_tt_v: GeneratedColumn = GeneratedColumn(Define.TINYTEXT(visible=False), "c_tt")
            g_tx_v: GeneratedColumn = GeneratedColumn(Define.TEXT(visible=False), "c_tx")
            g_mt_v: GeneratedColumn = GeneratedColumn(Define.MEDIUMTEXT(visible=False), "c_mt")
            g_lt_v: GeneratedColumn = GeneratedColumn(Define.LONGTEXT(visible=False), "c_lt")
            g_en_v: GeneratedColumn = GeneratedColumn(Define.ENUM("A", "B", "C", visible=False), "c_en")
            # Default (default should be omitted)
            g_ch_d: GeneratedColumn = GeneratedColumn(Define.CHAR(default="D"), "c_ch")
            g_vch_d: GeneratedColumn = GeneratedColumn(Define.VARCHAR(255, default="Default"), "c_vch")
            g_en_d: GeneratedColumn = GeneratedColumn(Define.ENUM("A", "B", "C", default="A"), "c_en")
            # Length
            g_ch_l1: GeneratedColumn = GeneratedColumn(Define.CHAR(1), "c_ch")
            g_ch_l2: GeneratedColumn = GeneratedColumn(Define.CHAR(255), "c_ch")
            g_vch_l1: GeneratedColumn = GeneratedColumn(Define.VARCHAR(1), "c_ch")
            g_vch_l2: GeneratedColumn = GeneratedColumn(Define.VARCHAR(255), "c_ch")
            # fmt: on

        class BiString(Table):
            # fmt: off
            # Real Column
            c_bi: Column = Column(Define.BINARY())
            c_vbi: Column = Column(Define.VARBINARY(100))
            c_tb: Column = Column(Define.TINYBLOB())
            c_bl: Column = Column(Define.BLOB())
            c_mb: Column = Column(Define.MEDIUMBLOB())
            c_lb: Column = Column(Define.LONGBLOB())
            # Basic
            g_bi: GeneratedColumn = GeneratedColumn(Define.BINARY(), "c_bi")
            g_vbi: GeneratedColumn = GeneratedColumn(Define.VARBINARY(100), "c_vbi")
            g_tb: GeneratedColumn = GeneratedColumn(Define.TINYBLOB(), "c_tb")
            g_bl: GeneratedColumn = GeneratedColumn(Define.BLOB(), "c_bl")
            g_mb: GeneratedColumn = GeneratedColumn(Define.MEDIUMBLOB(), "c_mb")
            g_lb: GeneratedColumn = GeneratedColumn(Define.LONGBLOB(), "c_lb")
            # Null
            g_bi_n: GeneratedColumn = GeneratedColumn(Define.BINARY(null=True), "c_bi")
            g_vbi_n: GeneratedColumn = GeneratedColumn(Define.VARBINARY(100, null=True), "c_vbi")
            g_tb_n: GeneratedColumn = GeneratedColumn(Define.TINYBLOB(null=True), "c_tb")
            g_bl_n: GeneratedColumn = GeneratedColumn(Define.BLOB(null=True), "c_bl")
            g_mb_n: GeneratedColumn = GeneratedColumn(Define.MEDIUMBLOB(null=True), "c_mb")
            g_lb_n: GeneratedColumn = GeneratedColumn(Define.LONGBLOB(null=True), "c_lb")
            # Comment
            g_bi_c: GeneratedColumn = GeneratedColumn(Define.BINARY(comment="Comment"), "c_bi")
            g_vbi_c: GeneratedColumn = GeneratedColumn(Define.VARBINARY(100, comment="Comment"), "c_vbi")
            g_tb_c: GeneratedColumn = GeneratedColumn(Define.TINYBLOB(comment="Comment"), "c_tb")
            g_bl_c: GeneratedColumn = GeneratedColumn(Define.BLOB(comment="Comment"), "c_bl")
            g_mb_c: GeneratedColumn = GeneratedColumn(Define.MEDIUMBLOB(comment="Comment"), "c_mb")
            g_lb_c: GeneratedColumn = GeneratedColumn(Define.LONGBLOB(comment="Comment"), "c_lb")
            # Visible
            g_bi_v: GeneratedColumn = GeneratedColumn(Define.BINARY(visible=False), "c_bi")
            g_vbi_v: GeneratedColumn = GeneratedColumn(Define.VARBINARY(100, visible=False), "c_vbi")
            g_tb_v: GeneratedColumn = GeneratedColumn(Define.TINYBLOB(visible=False), "c_tb")
            g_bl_v: GeneratedColumn = GeneratedColumn(Define.BLOB(visible=False), "c_bl")
            g_mb_v: GeneratedColumn = GeneratedColumn(Define.MEDIUMBLOB(visible=False), "c_mb")
            g_lb_v: GeneratedColumn = GeneratedColumn(Define.LONGBLOB(visible=False), "c_lb")
            # Default (default should be omitted)
            g_bi_d: GeneratedColumn = GeneratedColumn(Define.BINARY(default=b"D"), "c_bi")
            g_vbi_d: GeneratedColumn = GeneratedColumn(Define.VARBINARY(100, default="默认".encode()), "c_vbi")
            # Length
            g_bi_l1: GeneratedColumn = GeneratedColumn(Define.BINARY(1), "c_bi")
            g_bi_l2: GeneratedColumn = GeneratedColumn(Define.BINARY(255), "c_bi")
            g_vbi_l1: GeneratedColumn = GeneratedColumn(Define.VARBINARY(1), "c_bi")
            g_vbi_l2: GeneratedColumn = GeneratedColumn(Define.VARBINARY(255), "c_bi")
            # fmt: on

        class TestDatabase(Database):
            integer: Integer = Integer()
            fpoint: FPoint = FPoint()
            temporal: Temporal = Temporal()
            char: ChString = ChString()
            binary: BiString = BiString()

        db = TestDatabase("test_db", self.get_pool())
        db.Drop(True)
        db.Create()
        for tb in (
            db.integer,
            db.fpoint,
            db.temporal,
            db.char,
            db.binary,
        ):
            tb.Create()
            for col in tb:
                col_name = col.name
                if not col_name.startswith("g_"):
                    continue
                # unisgned
                meta = col.ShowMetadata()
                if col_name.endswith("_u"):
                    self.assertTrue(col.definition.unsigned)
                    self.assertTrue(meta.unsigned)
                elif col_name.endswith("_n"):
                    self.assertTrue(col.definition.null)
                    self.assertTrue(meta.null)
                elif col_name.endswith("_c"):
                    self.assertEqual(col.definition.comment, "Comment")
                    self.assertEqual(meta.comment, "Comment")
                elif col_name.endswith("_v"):
                    self.assertFalse(col.definition.visible)
                    self.assertFalse(meta.visible)
                # default should be ommited
                elif col_name.endswith("_d"):
                    self.assertIsNone(col.definition.default)
                    self.assertIsNone(meta.default)
                # auto options should be ommited
                elif col_name.endswith("_auto"):
                    self.assertFalse(col.definition.auto_increment)
                    self.assertFalse(col.definition.auto_init)
                    self.assertFalse(col.definition.auto_update)
                    self.assertFalse(meta.auto_increment)
                    self.assertFalse(meta.auto_init)
                    self.assertFalse(meta.auto_update)
                # metadata should be identical (after syncing the expression)
                logs = col.SyncFromRemote()
                self.assertEqual(len(logs), 1)
                self.assertIn("(local) expression:", repr(logs))
                self.assertFalse(col._diff_from_metadata(meta))
        db.Drop()

        # Modify
        class TestTable(Table):
            # Normal Column
            int_c: Column = Column(Define.INT())
            float_c: Column = Column(Define.FLOAT())
            decimal_c: Column = Column(Define.DECIMAL())
            date_c: Column = Column(Define.DATE())
            dt_c: Column = Column(Define.DATETIME())
            ts_c: Column = Column(Define.TIMESTAMP())
            time_c: Column = Column(Define.TIME())
            year_c: Column = Column(Define.YEAR())
            char_c: Column = Column(Define.CHAR())
            vchar_c: Column = Column(Define.VARCHAR(255))
            text_c: Column = Column(Define.TEXT())
            enum_c: Column = Column(Define.ENUM("A", "B", "C"))
            bin_c: Column = Column(Define.BINARY())
            vbin_c: Column = Column(Define.VARBINARY(255))
            blob_c: Column = Column(Define.BLOB())
            # Generated Column
            g_col: GeneratedColumn = GeneratedColumn(Define.INT(), "int_c", False)

        class TestDatabase(Database):
            tb: TestTable = TestTable()

        db = TestDatabase("test_db", self.get_pool())
        self.assertTrue(db.Drop(True))
        self.assertTrue(db.Initialize())
        size = len(db.tb)
        col = db.tb.g_col
        # . position only
        ori_pos = col.position
        for pos in range(size + 3):
            col.Modify(position=pos)
            meta = col.ShowMetadata()
            if pos < 1:
                self.assertEqual(ori_pos, col.position)
                self.assertEqual(ori_pos, meta.position)
            elif pos >= size:
                self.assertEqual(col.position, size)
                self.assertEqual(meta.position, size)
            else:
                self.assertEqual(col.position, pos)
                self.assertEqual(meta.position, pos)
        # . visible on
        meta = col.ShowMetadata()
        self.assertTrue(col.definition.visible)
        self.assertTrue(meta.visible)
        # . default off
        self.assertIsNone(col.definition.default)
        self.assertIsNone(meta.default)
        # . definition only
        ori_def = col.definition
        ori_expr = col.expression
        for args in [
            # fmt: off
            (Define.BIGINT(comment="Comment", visible=False), "int_c + 1"),
            (Define.DOUBLE(comment="Comment", visible=False), "float_c + 1"),
            (Define.DECIMAL(comment="Comment", visible=False), "decimal_c + 1"),
            (Define.INT(comment="Comment", visible=False), sqlfunc.TO_DAYS("date_c")),
            (Define.DATE(comment="Comment", visible=False), "date_c"),
            (Define.DATETIME(6, comment="Comment", visible=False), "dt_c"),
            (Define.TIME(6, comment="Comment", visible=False), "time_c"),
            (Define.YEAR(comment="Comment", visible=False), "year_c"),
            (Define.CHAR(comment="Comment", visible=False), sqlfunc.CONCAT("char_c", "'_C'")),
            (Define.VARCHAR(255, comment="Comment", visible=False), sqlfunc.CONCAT("vchar_c", "'_V'")),
            (Define.MEDIUMTEXT(comment="Comment", visible=False), "text_c"),
            (Define.ENUM("A", "B", "C", "D", comment="Comment", visible=False), "enum_c"),
            (Define.BINARY(comment="Comment", visible=False), "bin_c"),
            (Define.MEDIUMBLOB(comment="Comment", visible=False), "blob_c"),
            (Define.VARBINARY(255, comment="Comment", visible=False), "vbin_c"),
            # fmt: on
        ]:
            self.assertTrue(col.Modify(*args))
            self.assertIs(type(col.definition), type(args[0]))
        # . visible off
        meta = col.ShowMetadata()
        self.assertFalse(col.definition.visible)
        self.assertFalse(meta.visible)
        # . comment on
        self.assertEqual(col.definition.comment, "Comment")
        self.assertEqual(meta.comment, "Comment")
        # . visible on
        col.Modify(Define.BIGINT(), "int_c", position=1)
        meta = col.ShowMetadata()
        self.assertTrue(col.definition.visible)
        self.assertTrue(meta.visible)
        # . comment off
        self.assertIsNone(col.definition.comment)
        self.assertIsNone(meta.comment)
        # . position & definition
        col.Modify(ori_def, ori_expr, ori_pos)
        self.assertEqual(col.definition, ori_def)
        self.assertEqual(col.position, ori_pos)
        meta = col.ShowMetadata()
        self.assertEqual(meta.position, ori_pos)

        # . modify: toggle visibility only
        # fmt: off
        col = db.tb.g_col
        logs = col.Modify(Define.INT(visible=False))
        self.assertIn("ALTER TABLE test_db.tb ALTER COLUMN g_col SET INVISIBLE", repr(logs))
        self.assertFalse(col.definition.visible)
        logs = col.SetVisible(True)
        self.assertIn("ALTER TABLE test_db.tb ALTER COLUMN g_col SET VISIBLE", repr(logs))
        self.assertTrue(col.definition.visible)
        # fmt: on

        # . modify: set default
        with self.assertRaises(sqlerr.OperationalError):
            col.SetDefault(1)

        # . modify: local-remote mismatch
        class TestTable2(Table):
            # Normal Column
            int_c: Column = Column(Define.INT())
            # Generated Column
            g_col: GeneratedColumn = GeneratedColumn(
                Define.INT(
                    unsigned=True,
                    null=True,
                    default=1,
                    comment="Comment",
                    visible=False,
                ),
                "int_c",
                False,
            )

        class TestDatabase2(Database):
            tb: TestTable2 = TestTable2()

        db2 = TestDatabase2("test_db", self.get_pool())
        col = db2.tb.g_col
        self.assertTrue(col.definition.unsigned)
        self.assertTrue(col.definition.null)
        self.assertEqual(col.definition.comment, "Comment")
        self.assertFalse(col.definition.visible)
        meta = col.ShowMetadata()
        self.assertFalse(meta.unsigned)
        self.assertFalse(meta.null)
        self.assertIsNone(meta.comment)
        self.assertTrue(meta.visible)
        col.Modify(Define.INT(null=True, comment="Comment"))
        self.assertFalse(col.definition.unsigned)
        self.assertTrue(col.definition.null)
        self.assertEqual(col.definition.comment, "Comment")
        self.assertTrue(col.definition.visible)
        meta = col.ShowMetadata()
        self.assertFalse(meta.unsigned)
        self.assertTrue(meta.null)
        self.assertEqual(meta.comment, "Comment")
        self.assertTrue(meta.visible)
        self.assertTrue(col.Drop())

        # Sync from remote
        db.tb.g_col.Add(1)

        class TestTable2(Table):
            # Normal Column
            int_c: Column = Column(Define.INT())
            # Generated Column
            g_col: GeneratedColumn = GeneratedColumn(
                Define.INT(
                    unsigned=True,
                    null=True,
                    default=1,
                    comment="Comment",
                    visible=False,
                ),
                "int_c",
                False,
            )

        class TestDatabase2(Database):
            tb: TestTable2 = TestTable2()

        db2 = TestDatabase2("test_db", self.get_pool())
        col = db2.tb.g_col
        # . before sync
        self.assertTrue(col.definition.unsigned)
        self.assertTrue(col.definition.null)
        self.assertEqual(col.definition.comment, "Comment")
        self.assertFalse(col.definition.visible)
        # . after sync
        col.SyncFromRemote()
        self.assertFalse(col.definition.unsigned)
        self.assertFalse(col.definition.null)
        self.assertIsNone(col.definition.comment)
        self.assertTrue(col.definition.visible)
        meta = col.ShowMetadata()
        self.assertFalse(meta.unsigned)
        self.assertFalse(meta.null)
        self.assertIsNone(meta.comment)
        self.assertTrue(meta.visible)

        # Sync to remote
        self.assertTrue(col.Drop())
        self.assertTrue(db.tb.g_col.SyncToRemote())  # re-create

        class TestTable2(Table):
            # Normal Column
            int_c: Column = Column(Define.INT())
            # Generated Column
            g_col: GeneratedColumn = GeneratedColumn(
                Define.INT(
                    unsigned=True,
                    null=True,
                    default=1,
                    comment="Comment",
                    visible=False,
                ),
                "int_c",
                False,
            )

        class TestDatabase2(Database):
            tb: TestTable2 = TestTable2()

        db2 = TestDatabase2("test_db", self.get_pool())
        col = db2.tb.g_col
        # . before sync
        meta = col.ShowMetadata()
        self.assertFalse(meta.unsigned)
        self.assertFalse(meta.null)
        self.assertIsNone(meta.comment)
        self.assertTrue(meta.visible)
        # . after sync
        col.SyncToRemote()
        self.assertTrue(col.definition.unsigned)
        self.assertTrue(col.definition.null)
        self.assertEqual(col.definition.comment, "Comment")
        self.assertFalse(col.definition.visible)
        meta = col.ShowMetadata()
        self.assertTrue(meta.unsigned)
        self.assertTrue(meta.null)
        self.assertEqual(meta.comment, "Comment")
        self.assertFalse(meta.visible)
        self.assertTrue(col.Drop())

        # Finished
        self.assertTrue(db.Drop(True))
        self.log_ended("GENERATED COLUMN BASIC SQL")


class TestColumnAsyncSQL(TestCase):
    name: str = "Column Async SQL"

    async def test_all(self) -> None:
        await self.test_column_basic_sql()
        await self.test_generated_column_basic_sql()

    async def test_column_basic_sql(self) -> None:
        self.log_start("COLUMN BASIC SQL")

        # Add & Exists & Drop
        class Integer(Table):
            # Basic
            t_int: Column = Column(Define.TINYINT())
            s_int: Column = Column(Define.SMALLINT())
            m_int: Column = Column(Define.MEDIUMINT())
            i_int: Column = Column(Define.INT())
            b_int: Column = Column(Define.BIGINT())

        class TestDatabase(Database):
            tb: Integer = Integer()

        db = TestDatabase("test_db", self.get_pool())
        self.assertTrue(await db.aioDrop(True))
        self.assertTrue(await db.aioCreate())
        self.assertTrue(await db.tb.aioCreate())
        size = len(db.tb)
        for col in db.tb:
            for pos in range(size + 2):
                self.assertTrue(await col.aioExists())
                self.assertTrue(await col.aioDrop())
                self.assertFalse(await col.aioExists())
                await col.aioAdd(pos)
                self.assertTrue(await col.aioExists())
                meta = await col.aioShowMetadata()
                if pos < 1 or pos > size:
                    self.assertEqual(col.position, size)
                    self.assertEqual(meta.position, size)
                else:
                    self.assertEqual(col.position, pos)
                    self.assertEqual(meta.position, pos)
        self.assertTrue(await db.aioDrop())

        # Validate metadata
        class Integer(Table):
            # Basic
            t_int: Column = Column(Define.TINYINT())
            s_int: Column = Column(Define.SMALLINT())
            m_int: Column = Column(Define.MEDIUMINT())
            i_int: Column = Column(Define.INT())
            b_int: Column = Column(Define.BIGINT())
            # Unsigned
            t_int_u: Column = Column(Define.TINYINT(unsigned=True))
            s_int_u: Column = Column(Define.SMALLINT(unsigned=True))
            m_int_u: Column = Column(Define.MEDIUMINT(unsigned=True))
            i_int_u: Column = Column(Define.INT(unsigned=True))
            b_int_u: Column = Column(Define.BIGINT(unsigned=True))
            # Null
            t_int_n: Column = Column(Define.TINYINT(null=True))
            s_int_n: Column = Column(Define.SMALLINT(null=True))
            m_int_n: Column = Column(Define.MEDIUMINT(null=True))
            i_int_n: Column = Column(Define.INT(null=True))
            b_int_n: Column = Column(Define.BIGINT(null=True))
            # Default
            t_int_d: Column = Column(Define.TINYINT(default=1))
            s_int_d: Column = Column(Define.SMALLINT(default=1))
            m_int_d: Column = Column(Define.MEDIUMINT(default=1))
            i_int_d: Column = Column(Define.INT(default=1))
            b_int_d: Column = Column(Define.BIGINT(default=1))
            # Comment
            t_int_c: Column = Column(Define.TINYINT(comment="Comment"))
            s_int_c: Column = Column(Define.SMALLINT(comment="Comment"))
            m_int_c: Column = Column(Define.MEDIUMINT(comment="Comment"))
            i_int_c: Column = Column(Define.INT(comment="Comment"))
            b_int_c: Column = Column(Define.BIGINT(comment="描述"))
            # Visible
            t_int_v: Column = Column(Define.TINYINT(visible=False))
            s_int_v: Column = Column(Define.SMALLINT(visible=False))
            m_int_v: Column = Column(Define.MEDIUMINT(visible=False))
            i_int_v: Column = Column(Define.INT(visible=False))
            b_int_v: Column = Column(Define.BIGINT(visible=False))
            # AUTO_INCREMENT
            int_k: Column = Column(Define.INT(auto_increment=True))
            uk: UniqueKey = UniqueKey("int_k")

        class FPoint(Table):
            # Basic
            float_c: Column = Column(Define.FLOAT())
            double_c: Column = Column(Define.DOUBLE())
            decimal_c: Column = Column(Define.DECIMAL())
            # Null
            float_c_n: Column = Column(Define.FLOAT(null=True))
            double_c_n: Column = Column(Define.DOUBLE(null=True))
            decimal_c_n: Column = Column(Define.DECIMAL(null=True))
            # Default
            float_c_d: Column = Column(Define.FLOAT(default="1.1"))
            double_c_d: Column = Column(Define.DOUBLE(default="1.1"))
            decimal_c_d: Column = Column(Define.DECIMAL(10, 2, default="1.1"))
            # Comment
            float_c_c: Column = Column(Define.FLOAT(comment="Comment"))
            double_c_c: Column = Column(Define.DOUBLE(comment="Comment"))
            decimal_c_c: Column = Column(Define.DECIMAL(comment="描述"))
            # Visible
            float_c_v: Column = Column(Define.FLOAT(visible=False))
            double_c_v: Column = Column(Define.DOUBLE(visible=False))
            decimal_c_v: Column = Column(Define.DECIMAL(visible=False))
            # Precision
            double_c_p: Column = Column(Define.DOUBLE(default=1.289127389123897912783))
            decimal_c_p1: Column = Column(Define.DECIMAL(default="1.4"))
            decimal_c_p2: Column = Column(Define.DECIMAL(default="1.6"))
            decimal_c_p3: Column = Column(Define.DECIMAL(10, 2, default="1.444"))
            decimal_c_p4: Column = Column(Define.DECIMAL(10, 2, default="1.555"))
            decimal_c_p5: Column = Column(Define.DECIMAL(10, 2, default="1.666"))

        class Temporal(Table):
            # fmt: off
            # Basic
            date_c: Column = Column(Define.DATE())
            dt: Column = Column(Define.DATETIME())
            ts: Column = Column(Define.TIMESTAMP())
            time_c: Column = Column(Define.TIME())
            year_c: Column = Column(Define.YEAR())
            # Null
            date_c_n: Column = Column(Define.DATE(null=True))
            dt_n: Column = Column(Define.DATETIME(null=True))
            ts_n: Column = Column(Define.TIMESTAMP(null=True))
            time_c_n: Column = Column(Define.TIME(null=True))
            year_c_n: Column = Column(Define.YEAR(null=True))
            # Default
            date_c_d1: Column = Column(Define.DATE(default=datetime.datetime(2000, 1, 1)))
            date_c_d2: Column = Column(Define.DATE(default="2000-01-01 01:01:01"))
            dt_d: Column = Column(Define.DATETIME(default=datetime.datetime(2000, 1, 1, 1, 1, 1, 440000)))
            ts_d: Column = Column(Define.TIMESTAMP(default="2000-01-01 01:01:01.66"))
            time_c_d1: Column = Column(Define.TIME(default=datetime.time(1, 1, 1, 440000)))
            time_c_d2: Column = Column(Define.TIME(default="01:01:01.66"))
            year_c_d1: Column = Column(Define.YEAR(default=69))
            year_c_d2: Column = Column(Define.YEAR(default=99))
            year_c_d3: Column = Column(Define.YEAR(default="2100"))
            # Comment
            date_c_c: Column = Column(Define.DATE(comment="Comment"))
            dt_c: Column = Column(Define.DATETIME(comment="Comment"))
            ts_c: Column = Column(Define.TIMESTAMP(comment="Comment"))
            time_c_c: Column = Column(Define.TIME(comment="Comment"))
            year_c_c: Column = Column(Define.YEAR(comment="描述"))
            # Visible
            date_c_v: Column = Column(Define.DATE(visible=False))
            dt_v: Column = Column(Define.DATETIME(visible=False))
            ts_v: Column = Column(Define.TIMESTAMP(visible=False))
            time_c_v: Column = Column(Define.TIME(visible=False))
            year_c_v: Column = Column(Define.YEAR(visible=False))
            # Auto init & Auto update
            dt_auto_i: Column = Column(Define.DATETIME(auto_init=True))
            ts_auto_i: Column = Column(Define.TIMESTAMP(auto_init=True))
            dt_auto_u: Column = Column(Define.DATETIME(auto_update=True))
            ts_auto_u: Column = Column(Define.TIMESTAMP(auto_update=True))
            dt_auto_iu: Column = Column(Define.DATETIME(auto_init=True, auto_update=True))
            ts_auto_iu: Column = Column(Define.TIMESTAMP(auto_init=True, auto_update=True))
            # Precision
            dt_p1: Column = Column(Define.DATETIME(default="2000-01-01 01:01:01.6"))
            dt_p2: Column = Column(Define.DATETIME(0, default="2000-01-01 01:01:01.666666"))
            dt_p3: Column = Column(Define.DATETIME(1, default="2000-01-01 01:01:01.666666"))
            dt_p4: Column = Column(Define.DATETIME(2, default="2000-01-01 01:01:01.666666"))
            dt_p5: Column = Column(Define.DATETIME(3, default="2000-01-01 01:01:01.666666"))
            dt_p6: Column = Column(Define.DATETIME(4, default="2000-01-01 01:01:01.666666"))
            dt_p7: Column = Column(Define.DATETIME(5, default="2000-01-01 01:01:01.666666"))
            dt_p8: Column = Column(Define.DATETIME(6, default="2000-01-01 01:01:01.666666"))
            ts_p1: Column = Column(Define.TIMESTAMP(default="2000-01-01 01:01:01.6"))
            ts_p2: Column = Column(Define.TIMESTAMP(0, default="2000-01-01 01:01:01.666666"))
            ts_p3: Column = Column(Define.TIMESTAMP(1, default="2000-01-01 01:01:01.666666"))
            ts_p4: Column = Column(Define.TIMESTAMP(2, default="2000-01-01 01:01:01.666666"))
            ts_p5: Column = Column(Define.TIMESTAMP(3, default="2000-01-01 01:01:01.666666"))
            ts_p6: Column = Column(Define.TIMESTAMP(4, default="2000-01-01 01:01:01.666666"))
            ts_p7: Column = Column(Define.TIMESTAMP(5, default="2000-01-01 01:01:01.666666"))
            ts_p8: Column = Column(Define.TIMESTAMP(6, default="2000-01-01 01:01:01.666666"))
            time_p1: Column = Column(Define.TIME(default="01:01:01.6"))
            time_p2: Column = Column(Define.TIME(0, default="01:01:01.666666"))
            time_p3: Column = Column(Define.TIME(1, default="01:01:01.666666"))
            time_p4: Column = Column(Define.TIME(2, default="01:01:01.666666"))
            time_p5: Column = Column(Define.TIME(3, default="01:01:01.666666"))
            time_p6: Column = Column(Define.TIME(4, default="01:01:01.666666"))
            time_p7: Column = Column(Define.TIME(5, default="01:01:01.666666"))
            time_p8: Column = Column(Define.TIME(6, default="01:01:01.666666"))
            # fmt: on

        class ChString(Table):
            # Basic
            ch: Column = Column(Define.CHAR())
            vch: Column = Column(Define.VARCHAR(100))
            tt: Column = Column(Define.TINYTEXT())
            tx: Column = Column(Define.TEXT())
            mt: Column = Column(Define.MEDIUMTEXT())
            lt: Column = Column(Define.LONGTEXT())
            en: Column = Column(Define.ENUM("A", "B", "C"))
            # Null
            ch_n: Column = Column(Define.CHAR(null=True))
            vch_n: Column = Column(Define.VARCHAR(255, null=True))
            tt_n: Column = Column(Define.TINYTEXT(null=True))
            tx_n: Column = Column(Define.TEXT(null=True))
            mt_n: Column = Column(Define.MEDIUMTEXT(null=True))
            lt_n: Column = Column(Define.LONGTEXT(null=True))
            en_n: Column = Column(Define.ENUM("A", "B", "C", null=True))
            # Default
            ch_d: Column = Column(Define.CHAR(default="D"))
            vch_d: Column = Column(Define.VARCHAR(255, default="Default"))
            en_d: Column = Column(Define.ENUM("A", "B", "C", default="A"))
            # Comment
            ch_c: Column = Column(Define.CHAR(comment="Comment"))
            vch_c: Column = Column(Define.VARCHAR(255, comment="Comment"))
            tt_c: Column = Column(Define.TINYTEXT(comment="Comment"))
            tx_c: Column = Column(Define.TEXT(comment="Comment"))
            mt_c: Column = Column(Define.MEDIUMTEXT(comment="Comment"))
            lt_c: Column = Column(Define.LONGTEXT(comment="Comment"))
            en_c: Column = Column(Define.ENUM("A", "B", "C", comment="描述"))
            # Visible
            ch_v: Column = Column(Define.CHAR(visible=False))
            vch_v: Column = Column(Define.VARCHAR(255, visible=False))
            tt_v: Column = Column(Define.TINYTEXT(visible=False))
            tx_v: Column = Column(Define.TEXT(visible=False))
            mt_v: Column = Column(Define.MEDIUMTEXT(visible=False))
            lt_v: Column = Column(Define.LONGTEXT(visible=False))
            en_v: Column = Column(Define.ENUM("A", "B", "C", visible=False))
            # Length
            ch_l1: Column = Column(Define.CHAR(1))
            ch_l2: Column = Column(Define.CHAR(255))
            vch_l1: Column = Column(Define.VARCHAR(1))
            vch_l2: Column = Column(Define.VARCHAR(255))

        class BiString(Table):
            # Basic
            bi: Column = Column(Define.BINARY())
            vbi: Column = Column(Define.VARBINARY(100))
            tb: Column = Column(Define.TINYBLOB())
            bl: Column = Column(Define.BLOB())
            mb: Column = Column(Define.MEDIUMBLOB())
            lb: Column = Column(Define.LONGBLOB())
            # Null
            bi_n: Column = Column(Define.BINARY(null=True))
            vbi_n: Column = Column(Define.VARBINARY(100, null=True))
            tb_n: Column = Column(Define.TINYBLOB(null=True))
            bl_n: Column = Column(Define.BLOB(null=True))
            mb_n: Column = Column(Define.MEDIUMBLOB(null=True))
            lb_n: Column = Column(Define.LONGBLOB(null=True))
            # Default
            bi_d: Column = Column(Define.BINARY(default=b"D"))
            vbi_d: Column = Column(Define.VARBINARY(100, default="默认".encode()))
            # Comment
            bi_c: Column = Column(Define.BINARY(comment="Comment"))
            vbi_c: Column = Column(Define.VARBINARY(100, comment="Comment"))
            tb_c: Column = Column(Define.TINYBLOB(comment="Comment"))
            bl_c: Column = Column(Define.BLOB(comment="Comment"))
            mb_c: Column = Column(Define.MEDIUMBLOB(comment="Comment"))
            lb_c: Column = Column(Define.LONGBLOB(comment="描述"))
            # Visible
            bi_v: Column = Column(Define.BINARY(visible=False))
            vbi_v: Column = Column(Define.VARBINARY(100, visible=False))
            tb_v: Column = Column(Define.TINYBLOB(visible=False))
            bl_v: Column = Column(Define.BLOB(visible=False))
            mb_v: Column = Column(Define.MEDIUMBLOB(visible=False))
            lb_v: Column = Column(Define.LONGBLOB(visible=False))
            # Length
            bi_l1: Column = Column(Define.BINARY(1))
            bi_l2: Column = Column(Define.BINARY(255))
            vbi_l1: Column = Column(Define.VARBINARY(1))
            vbi_l2: Column = Column(Define.VARBINARY(255))

        class TestDatabase(Database):
            integer: Integer = Integer()
            fpoint: FPoint = FPoint()
            temporal: Temporal = Temporal()
            char: ChString = ChString()
            binary: BiString = BiString()

        db = TestDatabase("test_db", self.get_pool())
        with self.assertRaises(sqlerr.OperationalError):
            await db.integer.int_k.aioShowMetadata()
        self.assertTrue(await db.aioDrop(True))
        self.assertTrue(await db.aioCreate())
        [await tb.aioCreate() for tb in db]
        for col in (
            *db.integer,
            *db.fpoint,
            *db.temporal,
            *db.char,
            *db.binary,
        ):
            # metadata should be identical
            meta = await col.aioShowMetadata()
            self.assertFalse(col._diff_from_metadata(meta))
        self.assertTrue(await db.aioDrop())
        with self.assertRaises(sqlerr.OperationalError):
            await db.integer.t_int.aioShowMetadata()

        # Modify
        class TestTable(Table):
            int_c: Column = Column(Define.INT())
            float_c: Column = Column(Define.FLOAT())
            decimal_c: Column = Column(Define.DECIMAL())
            date_c: Column = Column(Define.DATE())
            dt_c: Column = Column(Define.DATETIME())
            ts_c: Column = Column(Define.TIMESTAMP())
            time_c: Column = Column(Define.TIME())
            year_c: Column = Column(Define.YEAR())
            char_c: Column = Column(Define.CHAR())
            vchar_c: Column = Column(Define.VARCHAR(255))
            text_c: Column = Column(Define.TEXT())
            enum_c: Column = Column(Define.ENUM("A", "B", "C"))
            bin_c: Column = Column(Define.BINARY())
            vbin_c: Column = Column(Define.VARBINARY(255))
            blob_c: Column = Column(Define.BLOB())

        class TestDatabase(Database):
            tb: TestTable = TestTable()

        db = TestDatabase("test_db", self.get_pool())
        self.assertTrue(await db.aioDrop(True))
        self.assertTrue(await db.aioCreate())
        self.assertTrue(await db.tb.aioCreate())
        for col in db.tb:
            size = len(db.tb)
            # . position only
            ori_pos = col.position
            for pos in range(size + 3):
                await col.aioModify(position=pos)
                meta = await col.aioShowMetadata()
                if pos < 1:
                    self.assertEqual(ori_pos, col.position)
                    self.assertEqual(ori_pos, meta.position)
                elif pos >= size:
                    self.assertEqual(col.position, size)
                    self.assertEqual(meta.position, size)
                else:
                    self.assertEqual(col.position, pos)
                    self.assertEqual(meta.position, pos)
            # . visible on
            meta = await col.aioShowMetadata()
            self.assertTrue(col.definition.visible)
            self.assertTrue(meta.visible)
            # . default off
            self.assertIsNone(col.definition.default)
            self.assertIsNone(meta.default)
            # . definition only
            ori_def = col.definition
            for new_def in (
                # fmt: off
                Define.BIGINT(unsigned=True, null=True, default=1, comment="Comment", visible=False),
                Define.DOUBLE(null=True, default=1.1, comment="Comment", visible=False),
                Define.DECIMAL(12, 2, null=True, default=1.1, comment="Comment", visible=False),
                Define.DATE(null=True, default="1970-01-01", comment="Comment", visible=False),
                Define.DATETIME(6, null=True, default="1970-01-01", comment="Comment", visible=False),
                Define.TIME(6, null=True, default="01:01:01.666666", comment="Comment", visible=False),
                Define.YEAR(null=False, default=69, comment="Comment", visible=False),
                Define.CHAR(100, null=True, default="default", charset="utf8mb3", comment="Comment", visible=False),
                Define.VARCHAR(100, null=True, default="default", collate="utf8mb3_bin", comment="Comment", visible=False),
                Define.MEDIUMTEXT(null=True, collate="utf8mb4_bin", comment="Comment", visible=False),
                Define.ENUM("A", "B", "C", "D", null=True, default="A", charset="utf8mb4", comment="Comment", visible=False),
                Define.BINARY(100, null=True, default=b"default", comment="Comment", visible=False),
                Define.MEDIUMBLOB(null=True, comment="Comment", visible=False),
                Define.VARBINARY(100, null=True, default=b"default", comment="Comment", visible=False),
                # fmt: on
            ):
                await col.aioModify(new_def)
                self.assertEqual(col.definition, new_def)
            # . visible off
            meta = await col.aioShowMetadata()
            self.assertFalse(col.definition.visible)
            self.assertFalse(meta.visible)
            # . default on
            self.assertIsNotNone(col.definition.default)
            self.assertIsNotNone(meta.default)
            # . comment on
            self.assertEqual(col.definition.comment, "Comment")
            self.assertEqual(meta.comment, "Comment")
            # . visible on
            await col.aioModify(Define.INT())
            meta = await col.aioShowMetadata()
            self.assertTrue(col.definition.visible)
            self.assertTrue(meta.visible)
            # . default off
            self.assertIsNone(col.definition.default)
            self.assertIsNone(meta.default)
            # . comment off
            self.assertIsNone(col.definition.comment)
            self.assertIsNone(meta.comment)
            # . position & definition
            await col.aioModify(ori_def, ori_pos)
            self.assertEqual(col.definition, ori_def)
            self.assertEqual(col.position, ori_pos)
            meta = await col.aioShowMetadata()
            self.assertEqual(meta.position, ori_pos)

        # . modify: toggle visibility only
        # fmt: off
        col = db.tb.int_c
        logs = await col.aioModify(Define.INT(visible=False))
        self.assertIn("ALTER TABLE test_db.tb ALTER COLUMN int_c SET INVISIBLE", repr(logs))
        self.assertFalse(col.definition.visible)
        logs = await col.aioSetVisible(True)
        self.assertIn("ALTER TABLE test_db.tb ALTER COLUMN int_c SET VISIBLE", repr(logs))
        self.assertTrue(col.definition.visible)
        # fmt: on

        # . modify: set default
        for col, dft, err in (
            (db.tb.int_c, 2, "x"),
            (db.tb.float_c, 2.1, "x"),
            (db.tb.decimal_c, Decimal("2"), "x"),
            (db.tb.date_c, datetime.date(2012, 1, 1), "x"),
            (db.tb.dt_c, datetime.datetime(2012, 1, 1), "x"),
            (db.tb.ts_c, datetime.datetime(2012, 1, 1), "x"),
            (db.tb.time_c, datetime.time(1, 1, 1), "x"),
            (db.tb.year_c, 2012, "x"),
            (db.tb.char_c, "d", "x"),
            (db.tb.vchar_c, "default", "x"),
            (db.tb.text_c, "default", "x"),
            (db.tb.enum_c, "A", "x"),
            (db.tb.bin_c, b"d", "x"),
            (db.tb.vbin_c, b"default", "x"),
            (db.tb.blob_c, b"default", "x"),
        ):
            self.assertIsNone(col.definition.default)
            if col in (db.tb.text_c, db.tb.blob_c):
                with self.assertRaises(sqlerr.OperationalError):
                    await col.aioSetDefault(dft)
            else:
                logs = await col.aioSetDefault(dft)
                self.assertEqual(col.definition.default, dft)
                if not isinstance(dft, str):
                    with self.assertRaises(errors.ColumnDefinitionError):
                        await col.aioSetDefault(err)
                logs = await col.aioSetDefault(None)
                self.assertIsNone(col.definition.default)

        # . modify: local-remote mismatch
        class TestTable2(Table):
            int_c: Column = Column(
                Define.INT(
                    unsigned=True,
                    null=True,
                    default=1,
                    comment="Comment",
                    visible=False,
                )
            )

        class TestDatabase2(Database):
            tb: TestTable2 = TestTable2()

        db2 = TestDatabase2("test_db", self.get_pool())
        col = db2.tb.int_c
        self.assertTrue(col.definition.unsigned)
        self.assertTrue(col.definition.null)
        self.assertEqual(col.definition.default, 1)
        self.assertEqual(col.definition.comment, "Comment")
        self.assertFalse(col.definition.visible)
        meta = await col.aioShowMetadata()
        self.assertFalse(meta.unsigned)
        self.assertFalse(meta.null)
        self.assertIsNone(meta.default)
        self.assertIsNone(meta.comment)
        self.assertTrue(meta.visible)
        await col.aioModify(Define.INT(null=True, comment="Comment"))
        self.assertFalse(col.definition.unsigned)
        self.assertTrue(col.definition.null)
        self.assertIsNone(col.definition.default)
        self.assertEqual(col.definition.comment, "Comment")
        self.assertTrue(col.definition.visible)
        meta = await col.aioShowMetadata()
        self.assertFalse(meta.unsigned)
        self.assertTrue(meta.null)
        self.assertIsNone(meta.default)
        self.assertEqual(meta.comment, "Comment")
        self.assertTrue(meta.visible)
        self.assertTrue(await col.aioDrop())

        # Sync from remote
        db.tb.int_c.Add(1)

        class TestTable2(Table):
            int_c: Column = Column(
                Define.INT(
                    unsigned=True,
                    null=True,
                    default=1,
                    comment="Comment",
                    visible=False,
                )
            )

        class TestDatabase2(Database):
            tb: TestTable2 = TestTable2()

        db2 = TestDatabase2("test_db", self.get_pool())
        col = db2.tb.int_c
        # . before sync
        self.assertTrue(col.definition.unsigned)
        self.assertTrue(col.definition.null)
        self.assertEqual(col.definition.default, 1)
        self.assertEqual(col.definition.comment, "Comment")
        self.assertFalse(col.definition.visible)
        # . after sync
        await col.aioSyncFromRemote()
        self.assertFalse(col.definition.unsigned)
        self.assertFalse(col.definition.null)
        self.assertIsNone(col.definition.default)
        self.assertIsNone(col.definition.comment)
        self.assertTrue(col.definition.visible)
        meta = await col.aioShowMetadata()
        self.assertFalse(meta.unsigned)
        self.assertFalse(meta.null)
        self.assertIsNone(meta.default)
        self.assertIsNone(meta.comment)
        self.assertTrue(meta.visible)

        # Sync to remote
        self.assertTrue(await col.aioDrop())
        self.assertTrue(await db.tb.int_c.aioSyncToRemote())  # re-create

        class TestTable2(Table):
            int_c: Column = Column(
                Define.INT(
                    unsigned=True,
                    null=True,
                    default=1,
                    comment="Comment",
                    visible=False,
                )
            )

        class TestDatabase2(Database):
            tb: TestTable2 = TestTable2()

        db2 = TestDatabase2("test_db", self.get_pool())
        col = db2.tb.int_c
        # . before sync
        meta = await col.aioShowMetadata()
        self.assertFalse(meta.unsigned)
        self.assertFalse(meta.null)
        self.assertIsNone(meta.default)
        self.assertIsNone(meta.comment)
        self.assertTrue(meta.visible)
        # . after sync
        await col.aioSyncToRemote()
        self.assertTrue(col.definition.unsigned)
        self.assertTrue(col.definition.null)
        self.assertEqual(col.definition.default, 1)
        self.assertEqual(col.definition.comment, "Comment")
        self.assertFalse(col.definition.visible)
        meta = await col.aioShowMetadata()
        self.assertTrue(meta.unsigned)
        self.assertTrue(meta.null)
        self.assertEqual(meta.default, "1")
        self.assertEqual(meta.comment, "Comment")
        self.assertFalse(meta.visible)
        self.assertTrue(await col.aioDrop())

        # Finished
        self.assertTrue(await db.aioDrop(True))
        self.log_ended("COLUMN BASIC SQL")

    async def test_generated_column_basic_sql(self) -> None:
        self.log_start("GENERATED COLUMN BASIC SQL")

        # Add & Exists & Drop
        class TestTable(Table):
            # fmt: off
            int_1: Column = Column(Define.INT())
            int_2: Column = Column(Define.INT())
            gen_1: GeneratedColumn = GeneratedColumn(Define.INT(), "int_1 + int_2")
            gen_2: GeneratedColumn = GeneratedColumn(Define.INT(), "int_1 * int_2", virtual=True)
            # fmt: on

        class TestDatabase(Database):
            tb: TestTable = TestTable()

        db = TestDatabase("test_db", self.get_pool())
        await db.aioDrop(True)
        await db.aioCreate()
        await db.tb.aioCreate()
        size = len(db.tb)
        for col in (db.tb.gen_1, db.tb.gen_2):
            for pos in range(size + 2):
                self.assertTrue(await col.aioExists())
                self.assertTrue(await col.aioDrop())
                self.assertFalse(await col.aioExists())
                await col.aioAdd(pos)
                self.assertTrue(await col.aioExists())
                meta = await col.aioShowMetadata()
                if pos < 1 or pos > size:
                    self.assertEqual(col.position, size)
                    self.assertEqual(meta.position, size)
                else:
                    self.assertEqual(col.position, pos)
                    self.assertEqual(meta.position, pos)
        await db.aioDrop()

        # Validate metadata
        class Integer(Table):
            expr = "c_int1 + c_int2"
            # fmt: off
            # Real Column
            c_int1: Column = Column(Define.TINYINT())
            c_int2: Column = Column(Define.TINYINT())
            # Basic
            g_int1: GeneratedColumn = GeneratedColumn(Define.TINYINT(), expr)
            g_int2: GeneratedColumn = GeneratedColumn(Define.SMALLINT(), expr)
            g_int3: GeneratedColumn = GeneratedColumn(Define.MEDIUMINT(), expr)
            g_int4: GeneratedColumn = GeneratedColumn(Define.INT(), expr)
            g_int5: GeneratedColumn = GeneratedColumn(Define.BIGINT(), expr)
            # Unsigned
            g_int1_u: GeneratedColumn = GeneratedColumn(Define.TINYINT(unsigned=True), expr)
            g_int2_u: GeneratedColumn = GeneratedColumn(Define.SMALLINT(unsigned=True), expr)
            g_int3_u: GeneratedColumn = GeneratedColumn(Define.MEDIUMINT(unsigned=True), expr)
            g_int4_u: GeneratedColumn = GeneratedColumn(Define.INT(unsigned=True), expr)
            g_int5_u: GeneratedColumn = GeneratedColumn(Define.BIGINT(unsigned=True), expr)
            # Null
            g_int1_n: GeneratedColumn = GeneratedColumn(Define.TINYINT(null=True), expr)
            g_int2_n: GeneratedColumn = GeneratedColumn(Define.SMALLINT(null=True), expr)
            g_int3_n: GeneratedColumn = GeneratedColumn(Define.MEDIUMINT(null=True), expr)
            g_int4_n: GeneratedColumn = GeneratedColumn(Define.INT(null=True), expr)
            g_int5_n: GeneratedColumn = GeneratedColumn(Define.BIGINT(null=True), expr)
            # Comment
            g_int1_c: GeneratedColumn = GeneratedColumn(Define.TINYINT(comment="Comment"), expr)
            g_int2_c: GeneratedColumn = GeneratedColumn(Define.SMALLINT(comment="Comment"), expr)
            g_int3_c: GeneratedColumn = GeneratedColumn(Define.MEDIUMINT(comment="Comment"), expr)
            g_int4_c: GeneratedColumn = GeneratedColumn(Define.INT(comment="Comment"), expr)
            g_int5_c: GeneratedColumn = GeneratedColumn(Define.BIGINT(comment="Comment"), expr)
            # Visible
            g_int1_v: GeneratedColumn = GeneratedColumn(Define.TINYINT(visible=False), expr)
            g_int2_v: GeneratedColumn = GeneratedColumn(Define.SMALLINT(visible=False), expr)
            g_int3_v: GeneratedColumn = GeneratedColumn(Define.MEDIUMINT(visible=False), expr)
            g_int4_v: GeneratedColumn = GeneratedColumn(Define.INT(visible=False), expr)
            g_int5_v: GeneratedColumn = GeneratedColumn(Define.BIGINT(visible=False), expr)
            # Default (default should be omitted)
            g_int_d: GeneratedColumn = GeneratedColumn(Define.INT(default=1), expr)
            # Auto Increment
            g_int_auto: GeneratedColumn = GeneratedColumn(Define.INT(auto_increment=True), expr)
            # fmt: on

        class FPoint(Table):
            expr = "c_flt + c_dbl + c_dec"
            # fmt: off
            # Real Column
            c_flt: Column = Column(Define.FLOAT())
            c_dbl: Column = Column(Define.DOUBLE())
            c_dec: Column = Column(Define.DECIMAL())
            # Basic
            g_flt: GeneratedColumn = GeneratedColumn(Define.FLOAT(), expr)
            g_dbl: GeneratedColumn = GeneratedColumn(Define.DOUBLE(), expr)
            g_dec: GeneratedColumn = GeneratedColumn(Define.DECIMAL(), expr)
            # Null
            g_flt_n: GeneratedColumn = GeneratedColumn(Define.FLOAT(null=True), expr)
            g_dbl_n: GeneratedColumn = GeneratedColumn(Define.DOUBLE(null=True), expr)
            g_dec_n: GeneratedColumn = GeneratedColumn(Define.DECIMAL(null=True), expr)
            # Comment
            g_flt_c: GeneratedColumn = GeneratedColumn(Define.FLOAT(comment="Comment"), expr)
            g_dbl_c: GeneratedColumn = GeneratedColumn(Define.DOUBLE(comment="Comment"), expr)
            g_dec_c: GeneratedColumn = GeneratedColumn(Define.DECIMAL(comment="Comment"), expr)
            # Visible
            g_flt_v: GeneratedColumn = GeneratedColumn(Define.FLOAT(visible=False), expr)
            g_dbl_v: GeneratedColumn = GeneratedColumn(Define.DOUBLE(visible=False), expr)
            g_dec_v: GeneratedColumn = GeneratedColumn(Define.DECIMAL(visible=False), expr)
            # Default (default should be omitted)
            g_flt_d: GeneratedColumn = GeneratedColumn(Define.FLOAT(default="1.1"), expr)
            g_dbl_d: GeneratedColumn = GeneratedColumn(Define.DOUBLE(default="1.1"), expr)
            g_dec_d: GeneratedColumn = GeneratedColumn(Define.DECIMAL(10, 2, default="1.1"), expr)
            # Precision
            g_dec_precision: GeneratedColumn = GeneratedColumn(Define.DECIMAL(10, 2), expr)
            # fmt: on

        class Temporal(Table):
            # fmt: off
            # Real Column
            c_date: Column = Column(Define.DATE())
            c_dt: Column = Column(Define.DATETIME())
            c_ts: Column = Column(Define.TIMESTAMP())
            c_time: Column = Column(Define.TIME())
            c_year: Column = Column(Define.YEAR())
            # Basic
            g_date: GeneratedColumn = GeneratedColumn(Define.DATE(), "c_date")
            g_dt: GeneratedColumn = GeneratedColumn(Define.DATETIME(), "c_dt")
            g_ts: GeneratedColumn = GeneratedColumn(Define.TIMESTAMP(), "c_ts")
            g_time: GeneratedColumn = GeneratedColumn(Define.TIME(), "c_time")
            g_year: GeneratedColumn = GeneratedColumn(Define.YEAR(), "c_year")
            # Null
            g_date_n: GeneratedColumn = GeneratedColumn(Define.DATE(null=True), "c_date")
            g_dt_n: GeneratedColumn = GeneratedColumn(Define.DATETIME(null=True), "c_dt")
            g_ts_n: GeneratedColumn = GeneratedColumn(Define.TIMESTAMP(null=True), "c_ts")
            g_time_n: GeneratedColumn = GeneratedColumn(Define.TIME(null=True), "c_time")
            g_year_n: GeneratedColumn = GeneratedColumn(Define.YEAR(null=True), "c_year")
            # Comment
            g_date_c: GeneratedColumn = GeneratedColumn(Define.DATE(comment="Comment"), "c_date")
            g_dt_c: GeneratedColumn = GeneratedColumn(Define.DATETIME(comment="Comment"), "c_dt")
            g_ts_c: GeneratedColumn = GeneratedColumn(Define.TIMESTAMP(comment="Comment"), "c_ts")
            g_time_c: GeneratedColumn = GeneratedColumn(Define.TIME(comment="Comment"), "c_time")
            g_year_c: GeneratedColumn = GeneratedColumn(Define.YEAR(comment="Comment"), "c_year")
            # Visible
            g_date_v: GeneratedColumn = GeneratedColumn(Define.DATE(visible=False), "c_date")
            g_dt_v: GeneratedColumn = GeneratedColumn(Define.DATETIME(visible=False), "c_dt")
            g_ts_v: GeneratedColumn = GeneratedColumn(Define.TIMESTAMP(visible=False), "c_ts")
            g_time_v: GeneratedColumn = GeneratedColumn(Define.TIME(visible=False), "c_time")
            g_year_v: GeneratedColumn = GeneratedColumn(Define.YEAR(visible=False), "c_year")
            # Default (default should be omitted)
            g_date_d: GeneratedColumn = GeneratedColumn(Define.DATE(default=datetime.datetime(2000, 1, 1)), "c_date")
            g_dt_d: GeneratedColumn = GeneratedColumn(Define.DATETIME(default="2000-01-01 01:01:01"), "c_dt")
            g_ts_d: GeneratedColumn = GeneratedColumn(Define.TIMESTAMP(default="2000-01-01 01:01:01.66"), "c_ts")
            g_time_d: GeneratedColumn = GeneratedColumn(Define.TIME(default=datetime.time(1, 1, 1, 440000)), "c_time")
            g_year_d: GeneratedColumn = GeneratedColumn(Define.YEAR(default=69), "c_year")
            # Auto init & Auto update (should be omitted)
            g_dt_auto: GeneratedColumn = GeneratedColumn(Define.DATETIME(auto_init=True, auto_update=True), "c_dt")
            # Precision
            g_dt_precision0: GeneratedColumn = GeneratedColumn(Define.DATETIME(0), "c_dt")
            g_dt_precision6: GeneratedColumn = GeneratedColumn(Define.DATETIME(6), "c_dt")
            g_ts_precision0: GeneratedColumn = GeneratedColumn(Define.TIMESTAMP(0), "c_ts")
            g_ts_precision6: GeneratedColumn = GeneratedColumn(Define.TIMESTAMP(6), "c_ts")
            g_time_precision0: GeneratedColumn = GeneratedColumn(Define.TIME(0), "c_time")
            g_time_precision6: GeneratedColumn = GeneratedColumn(Define.TIME(6), "c_time")
            # fmt: on

        class ChString(Table):
            # fmt: off
            # Normal Column
            c_ch: Column = Column(Define.CHAR())
            c_vch: Column = Column(Define.VARCHAR(100))
            c_tt: Column = Column(Define.TINYTEXT())
            c_tx: Column = Column(Define.TEXT())
            c_mt: Column = Column(Define.MEDIUMTEXT())
            c_lt: Column = Column(Define.LONGTEXT())
            c_en: Column = Column(Define.ENUM("A", "B", "C"))
            # Basic
            g_ch: GeneratedColumn = GeneratedColumn(Define.CHAR(), "c_ch")
            g_vch: GeneratedColumn = GeneratedColumn(Define.VARCHAR(100), "c_vch")
            g_tt: GeneratedColumn = GeneratedColumn(Define.TINYTEXT(), "c_tt")
            g_tx: GeneratedColumn = GeneratedColumn(Define.TEXT(), "c_tx")
            g_mt: GeneratedColumn = GeneratedColumn(Define.MEDIUMTEXT(), "c_mt")
            g_lt: GeneratedColumn = GeneratedColumn(Define.LONGTEXT(), "c_lt")
            g_en: GeneratedColumn = GeneratedColumn(Define.ENUM("A", "B", "C"), "c_en")
            # Null
            g_ch_n: GeneratedColumn = GeneratedColumn(Define.CHAR(null=True), "c_ch")
            g_vch_n: GeneratedColumn = GeneratedColumn(Define.VARCHAR(100, null=True), "c_vch")
            g_tt_n: GeneratedColumn = GeneratedColumn(Define.TINYTEXT(null=True), "c_tt")
            g_tx_n: GeneratedColumn = GeneratedColumn(Define.TEXT(null=True), "c_tx")
            g_mt_n: GeneratedColumn = GeneratedColumn(Define.MEDIUMTEXT(null=True), "c_mt")
            g_lt_n: GeneratedColumn = GeneratedColumn(Define.LONGTEXT(null=True), "c_lt")
            g_en_n: GeneratedColumn = GeneratedColumn(Define.ENUM("A", "B", "C", null=True), "c_en")
            # Comment
            g_ch_c: GeneratedColumn = GeneratedColumn(Define.CHAR(comment="Comment"), "c_ch")
            g_vch_c: GeneratedColumn = GeneratedColumn(Define.VARCHAR(255, comment="Comment"), "c_vch")
            g_tt_c: GeneratedColumn = GeneratedColumn(Define.TINYTEXT(comment="Comment"), "c_tt")
            g_tx_c: GeneratedColumn = GeneratedColumn(Define.TEXT(comment="Comment"), "c_tx")
            g_mt_c: GeneratedColumn = GeneratedColumn(Define.MEDIUMTEXT(comment="Comment"), "c_mt")
            g_lt_c: GeneratedColumn = GeneratedColumn(Define.LONGTEXT(comment="Comment"), "c_lt")
            g_en_c: GeneratedColumn = GeneratedColumn(Define.ENUM("A", "B", "C", comment="Comment"), "c_en")
            # Visible
            g_ch_v: GeneratedColumn = GeneratedColumn(Define.CHAR(visible=False), "c_ch")
            g_vch_v: GeneratedColumn = GeneratedColumn(Define.VARCHAR(255, visible=False), "c_vch")
            g_tt_v: GeneratedColumn = GeneratedColumn(Define.TINYTEXT(visible=False), "c_tt")
            g_tx_v: GeneratedColumn = GeneratedColumn(Define.TEXT(visible=False), "c_tx")
            g_mt_v: GeneratedColumn = GeneratedColumn(Define.MEDIUMTEXT(visible=False), "c_mt")
            g_lt_v: GeneratedColumn = GeneratedColumn(Define.LONGTEXT(visible=False), "c_lt")
            g_en_v: GeneratedColumn = GeneratedColumn(Define.ENUM("A", "B", "C", visible=False), "c_en")
            # Default (default should be omitted)
            g_ch_d: GeneratedColumn = GeneratedColumn(Define.CHAR(default="D"), "c_ch")
            g_vch_d: GeneratedColumn = GeneratedColumn(Define.VARCHAR(255, default="Default"), "c_vch")
            g_en_d: GeneratedColumn = GeneratedColumn(Define.ENUM("A", "B", "C", default="A"), "c_en")
            # Length
            g_ch_l1: GeneratedColumn = GeneratedColumn(Define.CHAR(1), "c_ch")
            g_ch_l2: GeneratedColumn = GeneratedColumn(Define.CHAR(255), "c_ch")
            g_vch_l1: GeneratedColumn = GeneratedColumn(Define.VARCHAR(1), "c_ch")
            g_vch_l2: GeneratedColumn = GeneratedColumn(Define.VARCHAR(255), "c_ch")
            # fmt: on

        class BiString(Table):
            # fmt: off
            # Real Column
            c_bi: Column = Column(Define.BINARY())
            c_vbi: Column = Column(Define.VARBINARY(100))
            c_tb: Column = Column(Define.TINYBLOB())
            c_bl: Column = Column(Define.BLOB())
            c_mb: Column = Column(Define.MEDIUMBLOB())
            c_lb: Column = Column(Define.LONGBLOB())
            # Basic
            g_bi: GeneratedColumn = GeneratedColumn(Define.BINARY(), "c_bi")
            g_vbi: GeneratedColumn = GeneratedColumn(Define.VARBINARY(100), "c_vbi")
            g_tb: GeneratedColumn = GeneratedColumn(Define.TINYBLOB(), "c_tb")
            g_bl: GeneratedColumn = GeneratedColumn(Define.BLOB(), "c_bl")
            g_mb: GeneratedColumn = GeneratedColumn(Define.MEDIUMBLOB(), "c_mb")
            g_lb: GeneratedColumn = GeneratedColumn(Define.LONGBLOB(), "c_lb")
            # Null
            g_bi_n: GeneratedColumn = GeneratedColumn(Define.BINARY(null=True), "c_bi")
            g_vbi_n: GeneratedColumn = GeneratedColumn(Define.VARBINARY(100, null=True), "c_vbi")
            g_tb_n: GeneratedColumn = GeneratedColumn(Define.TINYBLOB(null=True), "c_tb")
            g_bl_n: GeneratedColumn = GeneratedColumn(Define.BLOB(null=True), "c_bl")
            g_mb_n: GeneratedColumn = GeneratedColumn(Define.MEDIUMBLOB(null=True), "c_mb")
            g_lb_n: GeneratedColumn = GeneratedColumn(Define.LONGBLOB(null=True), "c_lb")
            # Comment
            g_bi_c: GeneratedColumn = GeneratedColumn(Define.BINARY(comment="Comment"), "c_bi")
            g_vbi_c: GeneratedColumn = GeneratedColumn(Define.VARBINARY(100, comment="Comment"), "c_vbi")
            g_tb_c: GeneratedColumn = GeneratedColumn(Define.TINYBLOB(comment="Comment"), "c_tb")
            g_bl_c: GeneratedColumn = GeneratedColumn(Define.BLOB(comment="Comment"), "c_bl")
            g_mb_c: GeneratedColumn = GeneratedColumn(Define.MEDIUMBLOB(comment="Comment"), "c_mb")
            g_lb_c: GeneratedColumn = GeneratedColumn(Define.LONGBLOB(comment="Comment"), "c_lb")
            # Visible
            g_bi_v: GeneratedColumn = GeneratedColumn(Define.BINARY(visible=False), "c_bi")
            g_vbi_v: GeneratedColumn = GeneratedColumn(Define.VARBINARY(100, visible=False), "c_vbi")
            g_tb_v: GeneratedColumn = GeneratedColumn(Define.TINYBLOB(visible=False), "c_tb")
            g_bl_v: GeneratedColumn = GeneratedColumn(Define.BLOB(visible=False), "c_bl")
            g_mb_v: GeneratedColumn = GeneratedColumn(Define.MEDIUMBLOB(visible=False), "c_mb")
            g_lb_v: GeneratedColumn = GeneratedColumn(Define.LONGBLOB(visible=False), "c_lb")
            # Default (default should be omitted)
            g_bi_d: GeneratedColumn = GeneratedColumn(Define.BINARY(default=b"D"), "c_bi")
            g_vbi_d: GeneratedColumn = GeneratedColumn(Define.VARBINARY(100, default="默认".encode()), "c_vbi")
            # Length
            g_bi_l1: GeneratedColumn = GeneratedColumn(Define.BINARY(1), "c_bi")
            g_bi_l2: GeneratedColumn = GeneratedColumn(Define.BINARY(255), "c_bi")
            g_vbi_l1: GeneratedColumn = GeneratedColumn(Define.VARBINARY(1), "c_bi")
            g_vbi_l2: GeneratedColumn = GeneratedColumn(Define.VARBINARY(255), "c_bi")
            # fmt: on

        class TestDatabase(Database):
            integer: Integer = Integer()
            fpoint: FPoint = FPoint()
            temporal: Temporal = Temporal()
            char: ChString = ChString()
            binary: BiString = BiString()

        db = TestDatabase("test_db", self.get_pool())
        await db.aioDrop(True)
        await db.aioCreate()
        for tb in (
            db.integer,
            db.fpoint,
            db.temporal,
            db.char,
            db.binary,
        ):
            await tb.aioCreate()
            for col in tb:
                col_name = col.name
                if not col_name.startswith("g_"):
                    continue
                # unisgned
                meta = await col.aioShowMetadata()
                if col_name.endswith("_u"):
                    self.assertTrue(col.definition.unsigned)
                    self.assertTrue(meta.unsigned)
                elif col_name.endswith("_n"):
                    self.assertTrue(col.definition.null)
                    self.assertTrue(meta.null)
                elif col_name.endswith("_c"):
                    self.assertEqual(col.definition.comment, "Comment")
                    self.assertEqual(meta.comment, "Comment")
                elif col_name.endswith("_v"):
                    self.assertFalse(col.definition.visible)
                    self.assertFalse(meta.visible)
                # default should be ommited
                elif col_name.endswith("_d"):
                    self.assertIsNone(col.definition.default)
                    self.assertIsNone(meta.default)
                # auto options should be ommited
                elif col_name.endswith("_auto"):
                    self.assertFalse(col.definition.auto_increment)
                    self.assertFalse(col.definition.auto_init)
                    self.assertFalse(col.definition.auto_update)
                    self.assertFalse(meta.auto_increment)
                    self.assertFalse(meta.auto_init)
                    self.assertFalse(meta.auto_update)
                # metadata should be identical (after syncing the expression)
                logs = await col.aioSyncFromRemote()
                self.assertEqual(len(logs), 1)
                self.assertIn("(local) expression:", repr(logs))
                self.assertFalse(col._diff_from_metadata(meta))
        await db.aioDrop()

        # Modify
        class TestTable(Table):
            # Normal Column
            int_c: Column = Column(Define.INT())
            float_c: Column = Column(Define.FLOAT())
            decimal_c: Column = Column(Define.DECIMAL())
            date_c: Column = Column(Define.DATE())
            dt_c: Column = Column(Define.DATETIME())
            ts_c: Column = Column(Define.TIMESTAMP())
            time_c: Column = Column(Define.TIME())
            year_c: Column = Column(Define.YEAR())
            char_c: Column = Column(Define.CHAR())
            vchar_c: Column = Column(Define.VARCHAR(255))
            text_c: Column = Column(Define.TEXT())
            enum_c: Column = Column(Define.ENUM("A", "B", "C"))
            bin_c: Column = Column(Define.BINARY())
            vbin_c: Column = Column(Define.VARBINARY(255))
            blob_c: Column = Column(Define.BLOB())
            # Generated Column
            g_col: GeneratedColumn = GeneratedColumn(Define.INT(), "int_c", False)

        class TestDatabase(Database):
            tb: TestTable = TestTable()

        db = TestDatabase("test_db", self.get_pool())
        self.assertTrue(await db.aioDrop(True))
        self.assertTrue(await db.aioInitialize())
        size = len(db.tb)
        col = db.tb.g_col
        # . position only
        ori_pos = col.position
        for pos in range(size + 3):
            await col.aioModify(position=pos)
            meta = await col.aioShowMetadata()
            if pos < 1:
                self.assertEqual(ori_pos, col.position)
                self.assertEqual(ori_pos, meta.position)
            elif pos >= size:
                self.assertEqual(col.position, size)
                self.assertEqual(meta.position, size)
            else:
                self.assertEqual(col.position, pos)
                self.assertEqual(meta.position, pos)
        # . visible on
        meta = await col.aioShowMetadata()
        self.assertTrue(col.definition.visible)
        self.assertTrue(meta.visible)
        # . default off
        self.assertIsNone(col.definition.default)
        self.assertIsNone(meta.default)
        # . definition only
        ori_def = col.definition
        ori_expr = col.expression
        for args in [
            # fmt: off
            (Define.BIGINT(comment="Comment", visible=False), "int_c + 1"),
            (Define.DOUBLE(comment="Comment", visible=False), "float_c + 1"),
            (Define.DECIMAL(comment="Comment", visible=False), "decimal_c + 1"),
            (Define.INT(comment="Comment", visible=False), sqlfunc.TO_DAYS("date_c")),
            (Define.DATE(comment="Comment", visible=False), "date_c"),
            (Define.DATETIME(6, comment="Comment", visible=False), "dt_c"),
            (Define.TIME(6, comment="Comment", visible=False), "time_c"),
            (Define.YEAR(comment="Comment", visible=False), "year_c"),
            (Define.CHAR(comment="Comment", visible=False), sqlfunc.CONCAT("char_c", "'_C'")),
            (Define.VARCHAR(255, comment="Comment", visible=False), sqlfunc.CONCAT("vchar_c", "'_V'")),
            (Define.MEDIUMTEXT(comment="Comment", visible=False), "text_c"),
            (Define.ENUM("A", "B", "C", "D", comment="Comment", visible=False), "enum_c"),
            (Define.BINARY(comment="Comment", visible=False), "bin_c"),
            (Define.MEDIUMBLOB(comment="Comment", visible=False), "blob_c"),
            (Define.VARBINARY(255, comment="Comment", visible=False), "vbin_c"),
            # fmt: on
        ]:
            self.assertTrue(await col.aioModify(*args))
            self.assertIs(type(col.definition), type(args[0]))
        # . visible off
        meta = await col.aioShowMetadata()
        self.assertFalse(col.definition.visible)
        self.assertFalse(meta.visible)
        # . comment on
        self.assertEqual(col.definition.comment, "Comment")
        self.assertEqual(meta.comment, "Comment")
        # . visible on
        await col.aioModify(Define.BIGINT(), "int_c", position=1)
        meta = await col.aioShowMetadata()
        self.assertTrue(col.definition.visible)
        self.assertTrue(meta.visible)
        # . comment off
        self.assertIsNone(col.definition.comment)
        self.assertIsNone(meta.comment)
        # . position & definition
        await col.aioModify(ori_def, ori_expr, ori_pos)
        self.assertEqual(col.definition, ori_def)
        self.assertEqual(col.position, ori_pos)
        meta = await col.aioShowMetadata()
        self.assertEqual(meta.position, ori_pos)

        # . modify: toggle visibility only
        # fmt: off
        col = db.tb.g_col
        logs = await col.aioModify(Define.INT(visible=False))
        self.assertIn("ALTER TABLE test_db.tb ALTER COLUMN g_col SET INVISIBLE", repr(logs))
        self.assertFalse(col.definition.visible)
        logs = await col.aioSetVisible(True)
        self.assertIn("ALTER TABLE test_db.tb ALTER COLUMN g_col SET VISIBLE", repr(logs))
        self.assertTrue(col.definition.visible)
        # fmt: on

        # . modify: set default
        with self.assertRaises(sqlerr.OperationalError):
            await col.aioSetDefault(1)

        # . modify: local-remote mismatch
        class TestTable2(Table):
            # Normal Column
            int_c: Column = Column(Define.INT())
            # Generated Column
            g_col: GeneratedColumn = GeneratedColumn(
                Define.INT(
                    unsigned=True,
                    null=True,
                    default=1,
                    comment="Comment",
                    visible=False,
                ),
                "int_c",
                False,
            )

        class TestDatabase2(Database):
            tb: TestTable2 = TestTable2()

        db2 = TestDatabase2("test_db", self.get_pool())
        col = db2.tb.g_col
        self.assertTrue(col.definition.unsigned)
        self.assertTrue(col.definition.null)
        self.assertEqual(col.definition.comment, "Comment")
        self.assertFalse(col.definition.visible)
        meta = await col.aioShowMetadata()
        self.assertFalse(meta.unsigned)
        self.assertFalse(meta.null)
        self.assertIsNone(meta.comment)
        self.assertTrue(meta.visible)
        await col.aioModify(Define.INT(null=True, comment="Comment"))
        self.assertFalse(col.definition.unsigned)
        self.assertTrue(col.definition.null)
        self.assertEqual(col.definition.comment, "Comment")
        self.assertTrue(col.definition.visible)
        meta = await col.aioShowMetadata()
        self.assertFalse(meta.unsigned)
        self.assertTrue(meta.null)
        self.assertEqual(meta.comment, "Comment")
        self.assertTrue(meta.visible)
        self.assertTrue(await col.aioDrop())

        # Sync from remote
        db.tb.g_col.Add(1)

        class TestTable2(Table):
            # Normal Column
            int_c: Column = Column(Define.INT())
            # Generated Column
            g_col: GeneratedColumn = GeneratedColumn(
                Define.INT(
                    unsigned=True,
                    null=True,
                    default=1,
                    comment="Comment",
                    visible=False,
                ),
                "int_c",
                False,
            )

        class TestDatabase2(Database):
            tb: TestTable2 = TestTable2()

        db2 = TestDatabase2("test_db", self.get_pool())
        col = db2.tb.g_col
        # . before sync
        self.assertTrue(col.definition.unsigned)
        self.assertTrue(col.definition.null)
        self.assertEqual(col.definition.comment, "Comment")
        self.assertFalse(col.definition.visible)
        # . after sync
        await col.aioSyncFromRemote()
        self.assertFalse(col.definition.unsigned)
        self.assertFalse(col.definition.null)
        self.assertIsNone(col.definition.comment)
        self.assertTrue(col.definition.visible)
        meta = await col.aioShowMetadata()
        self.assertFalse(meta.unsigned)
        self.assertFalse(meta.null)
        self.assertIsNone(meta.comment)
        self.assertTrue(meta.visible)

        # Sync to remote
        self.assertTrue(await col.aioDrop())
        self.assertTrue(await db.tb.g_col.aioSyncToRemote())  # re-create

        class TestTable2(Table):
            # Normal Column
            int_c: Column = Column(Define.INT())
            # Generated Column
            g_col: GeneratedColumn = GeneratedColumn(
                Define.INT(
                    unsigned=True,
                    null=True,
                    default=1,
                    comment="Comment",
                    visible=False,
                ),
                "int_c",
                False,
            )

        class TestDatabase2(Database):
            tb: TestTable2 = TestTable2()

        db2 = TestDatabase2("test_db", self.get_pool())
        col = db2.tb.g_col
        # . before sync
        meta = await col.aioShowMetadata()
        self.assertFalse(meta.unsigned)
        self.assertFalse(meta.null)
        self.assertIsNone(meta.comment)
        self.assertTrue(meta.visible)
        # . after sync
        self.assertTrue(col.definition.unsigned)
        self.assertTrue(col.definition.null)
        self.assertEqual(col.definition.comment, "Comment")
        self.assertFalse(col.definition.visible)
        await col.aioSyncToRemote()
        meta = await col.aioShowMetadata()
        self.assertTrue(meta.unsigned)
        self.assertTrue(meta.null)
        self.assertEqual(meta.comment, "Comment")
        self.assertFalse(meta.visible)
        self.assertTrue(await col.aioDrop())

        # Finished
        self.assertTrue(await db.aioDrop(True))
        self.log_ended("GENERATED COLUMN BASIC SQL")


if __name__ == "__main__":
    HOST = "localhost"
    PORT = 3306
    USER = "root"
    PSWD = "Password_123456"

    for case in (
        TestColumnDefinition,
        TestColumnCopy,
        TestColumnSyncSQL,
        TestColumnAsyncSQL,
    ):
        test_case = case(HOST, PORT, USER, PSWD)
        if not iscoroutinefunction(test_case.test_all):
            test_case.test_all()
        else:
            asyncio.run(test_case.test_all())
