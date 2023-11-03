# cython: language_level=3
# cython: wraparound=False
# cython: boundscheck=False

# Cython imports
import cython
from cython.cimports.cpython.dict import PyDict_Size as dict_len  # type: ignore
from cython.cimports.cpython.dict import PyDict_Keys as dict_keys  # type: ignore
from cython.cimports.cpython.dict import PyDict_Values as dict_values  # type: ignore
from cython.cimports.cpython.dict import PyDict_SetItem as dict_setitem  # type: ignore
from cython.cimports.cpython.dict import PyDict_Contains as dict_contains  # type: ignore
from cython.cimports.mysqlengine import errors, utils  # type: ignore
from cython.cimports.mysqlengine.connection import Server  # type: ignore

# Python imports
from typing import Any, Union
from mysqlengine.logs import logger
from mysqlengine import errors, utils
from mysqlengine.connection import Server
from mysqlengine.database import Database

__all__ = ["Engine"]


# Engine ========================================================================================
@cython.cclass
class Engine:
    """Represents a proxy for accessing Databases hosted on a Server."""

    _server: Server
    _databases_cls: dict[str, type[Database]]
    "Map of the access name to the database class."
    _databases: dict[str, Database]
    "Map of the unique cache key to the database instance."
    _length: cython.int

    def __init__(
        self,
        server: Server,
        **databases: type[Database],
    ) -> None:
        """The proxy for accessing Databases hosted on a Server.

        :param server: `<Server>` The server that hosts the databases.
        :param databases [keyword arguments]: `<type[Database]>` The the databases of the server.
            * Key: `<str>` Access name of the Database, used in the `access()`
              method to access & instantiate the Database.
            * Value: Subclass of `<Database>` Represents a specific database
              on the Server (NOT an instance). `<Database>` subclass' `__init__`
              method must accept 'server' as an argument.

        For more information about accessing & instantiating databases,
        please refer to the Example section below.

        ### Example
        >>> from mysqlengine import Server, Engine, Database

        >>> # Server
            server = Server(host="localhost", user="root", password="password")

        >>> # MyDatabase1 (only takes 'server' as an argument)
            class MyDatabase1(Database):
                def __init__(self, server: Server):
                    super().__init__(server, name="mydb1")
                ...

        >>> # MyDatabase2 (takes 'server' and 'country' as arguments)
            class MyDatabase2(Database):
                def __init__(server: Server, country: str):
                    name = "mydb2_" + country
                    super().__init__(server, name=name)

        >>> # Engine
            engine = Engine(server, mydb1=MyDatabase1, mydb2=MyDatabase2)

        >>> # Access MyDatabase1
            db1 = await engine.access("mydb1")

        >>> # Access MyDatabase2
            db2_us = await engine.access("mydb2", country="us")
            db2_ca = await engine.access("mydb2", country="ca")
        """
        # Server
        self._server = server
        self._databases = {}
        self._validate_databases(databases)

    # Properties ---------------------------------------------------------------------------------
    @property
    def server(self) -> Server:
        "The underlying server of the engine `<Server>`"
        return self._server

    # Validation ---------------------------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    def _validate_databases(self, databases: dict):
        "(cfunc) Validate the 'databases' argument."
        if not databases:
            raise errors.EngineDatabaseError(
                "<Engine> The 'databases' argument cannot be empty."
            )
        for db_cls in dict_values(databases):
            # . validate database class
            if type(db_cls) is not type or not issubclass(db_cls, Database):
                raise errors.EngineDatabaseError(
                    "<Engine> The value of the 'databases' dictionary "
                    "must be a subclass of `<Database>`, instead of: {} {}.".format(
                        type(db_cls), repr(db_cls)
                    )
                )
        self._databases_cls = databases
        self._length = dict_len(databases)

    # Access -------------------------------------------------------------------------------------
    async def access(self, db: str, **init_args: Any) -> Database:
        """Accesses a specific database on the Server.

        :param db: `<str>` The access name of the Database.
            This corresponds to the keys provided in the 'databases'
            keyword arguments of the Engine's constructor.

        :param init_args [keyword arguments]: `<Any>` Additional initialization arguments for the `<Database>`.
            Necessary only if the `<Database>` subclass constructor
            takes arguments other than 'server'.

        :raises `MysqlEngineError`: If accessing the database fails.
        :return `<Database>`: The Database instance for the given access name.
            If accessed before with the same arguments, the cached
            instance will be returned.

        ### Example
        >>> from mysqlengine import Server, Engine, Database

        >>> # Server
            server = Server(host="localhost", user="root", password="password")

        >>> # MyDatabase1 (only takes 'server' as an argument)
            class MyDatabase1(Database):
                def __init__(self, server: Server):
                    super().__init__(server, name="mydb1")
                ...

        >>> # MyDatabase2 (takes 'server' and 'country' as arguments)
            class MyDatabase2(Database):
                def __init__(server: Server, country: str):
                    name = "mydb2_" + country
                    super().__init__(server, name=name)

        >>> # Engine
            engine = Engine(server, mydb1=MyDatabase1, mydb2=MyDatabase2)

        >>> # Access MyDatabase1
            db1 = await engine.access("mydb1")

        >>> # Access MyDatabase2
            db2_us = await engine.access("mydb2", country="us")
            db2_ca = await engine.access("mydb2", country="ca")

        ### *Notice* ###
        In the example, `MyDatabase2` needs a 'country' argument which is used
        to generate the final database name. Accessing it with different countries
        maps to different databases ("mydb2_us" & "mydb2_ca") on the Server but
        with identical database schemas (based on MyDatabase2 configurations).
        """
        # Generate cache key
        if init_args:
            cache_key = db + ":" + utils._hash_sha256(init_args)
        else:
            cache_key = db

        # Access database from cache
        if dict_contains(self._databases, cache_key):
            return self._databases[cache_key]

        # Get database class
        if not dict_contains(self._databases_cls, db):
            raise errors.EngineDatabaseAccessKeyError(
                "<Engine> The database access name '{}' does not belong to this Engine.\n"
                "Available access names: {}.".format(db, dict_keys(self._databases_cls))
            )
        db_cls = self._databases_cls[db]

        # Instantiate database
        dict_setitem(init_args, "server", self._server)
        try:
            db_ins = db_cls(**init_args)
        except errors.MysqlEngineError as err:
            err.add_note(
                "-> <Engine> Failed to instantiate database {}.\n"
                "Initiate arguments: {}".format(db_cls, init_args)
            )
            raise err
        except Exception as err:
            raise errors.EngineDatabaseInstanciateError(
                "<Engine> Failed to instantiate database {}.\n"
                "Initiate arguments: {}.\nError: {}".format(db_cls, init_args, err)
            ) from err

        # Initiate & cache database
        await db_ins.initiate()
        dict_setitem(self._databases, cache_key, db_ins)

        # Return database
        return db_ins

    # Disconnect ---------------------------------------------------------------------------------
    async def disconnect(self, countdown: Union[int, None] = None) -> None:
        """Disconnect from the Server (close gracefully).

        ### *Notice* ###
        This method should normally be called when the Engine is no longer in use,
        (all operations of the underlying databases are completed).

        Calling this method will close all free & invalid connections maintained
        by the Server pool, then wait for all in use connections to be released
        before shutdown (gentle close). However, a countdown can be set to force
        the Server to terminate all connections after certain seconds (force close).

        :param countdown: `<int/None>` The number of seconds to wait before termination. Defaults to `None`.
            - If `countdown <= 0 or = None`, the Server pool will wait
              util all connections are released.
            - If `countdown > 0`, the Server pool will terminate all
              connections (regardless state) after the countdown.
        """
        await self._server.close(countdown)
        self._databases = {}

    def quit(self) -> None:
        """Forcefully terminates all connections from the Server.

        This method immediately stops the Server managed by the Engine,
        and terminates all active connections in the pool. For a graceful
        shutdown, use the `disconnect()` method instead.

        Side Effects:
            - All managed server processes will be interrupted.
            - All network connections in the pool will be closed.
            - This operation is irreversible and should be used with caution.
        """
        self._server.quit()

    # Speical Methods -----------------------------------------------------
    def __repr__(self) -> str:
        return "<Engine (databases=%s)>" % self._databases_cls

    def __hash__(self) -> int:
        return hash((self._server, self._databases_cls))

    def __eq__(self, __o: object) -> bool:
        return hash(self) == hash(__o) if isinstance(__o, type(self)) else False

    def __len__(self) -> int:
        return self._length

    def __del__(self):
        if not self._server._closed:
            logger.error(
                "%s is not closed properly. Please call `disconnect()` "
                "to gracefully shutdown the Engine." % self
            )
            self._server.quit()
        self._server = None
        self._databases_cls = None
        self._databases = None
