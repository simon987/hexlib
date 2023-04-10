import base64
import sqlite3
import traceback
from datetime import datetime
from enum import Enum

import psycopg2
import umsgpack
from psycopg2.errorcodes import UNIQUE_VIOLATION
from pydantic import BaseModel

from hexlib.env import get_redis


def _json_encoder(x):
    if isinstance(x, datetime):
        return x.isoformat()
    if isinstance(x, Enum):
        return x.value

    raise Exception(f"I don't know how to JSON encode {x} ({type(x)})")


class VolatileState:
    """Quick and dirty volatile dict-like redis wrapper"""

    def __init__(self, prefix, redis_db=None, sep=""):
        if redis_db is None:
            redis_db = get_redis()
        self.rdb = redis_db
        self.prefix = prefix
        self._sep = sep

    def __getitem__(self, table):
        return RedisTable(self, table, self._sep)

    def __delitem__(self, key):
        self.rdb.delete(f"{self.prefix}{self._sep}{key}")


class VolatileQueue:
    """Quick and dirty volatile queue-like redis wrapper"""

    def __init__(self, key, redis_db=None):
        if redis_db is None:
            redis_db = get_redis()
        self.rdb = redis_db
        self.key = key

    def put(self, item):
        self.rdb.sadd(self.key, umsgpack.dumps(item))

    def get(self):
        v = self.rdb.spop(self.key)
        if v:
            return umsgpack.loads(v)


class VolatileBooleanState:
    """Quick and dirty volatile dict-like redis wrapper for boolean values"""

    def __init__(self, prefix, redis_db=None, sep=""):
        if redis_db is None:
            redis_db = get_redis()
        self.rdb = redis_db
        self.prefix = prefix
        self._sep = sep

    def __getitem__(self, table):
        return RedisBooleanTable(self, table, self._sep)

    def __delitem__(self, table):
        self.rdb.delete(f"{self.prefix}{self._sep}{table}")


class RedisTable:
    def __init__(self, state, table, sep=""):
        self._state = state
        self._table = table
        self._sep = sep
        self._key = f"{self._state.prefix}{self._sep}{self._table}"

    def __setitem__(self, key, value):
        self._state.rdb.hset(self._key, str(key), umsgpack.dumps(value))

    def __getitem__(self, key):
        val = self._state.rdb.hget(self._key, str(key))
        if val:
            return umsgpack.loads(val)
        return None

    def __delitem__(self, key):
        self._state.rdb.hdel(self._key, str(key))

    def __iter__(self):
        for val in self._state.rdb.hscan(self._key):
            if val:
                return ((k, umsgpack.loads(v)) for k, v in val.items())


class RedisBooleanTable:
    def __init__(self, state, table, sep=""):
        self._state = state
        self._table = table
        self._sep = sep
        self._key = f"{self._state.prefix}{self._sep}{self._table}"

    def __setitem__(self, key, value):
        if value:
            self._state.rdb.sadd(self._key, str(key))
        else:
            self.__delitem__(key)

    def __getitem__(self, key):
        return self._state.rdb.sismember(self._key, str(key))

    def __delitem__(self, key):
        self._state.rdb.srem(self._key, str(key))

    def __iter__(self):
        yield from self._state.rdb.sscan_iter(self._key)


class Table:
    def __init__(self, state, table):
        self._state = state
        self._table = table

    def _sql_dict(self, where_clause, *params):
        with sqlite3.connect(self._state.dbfile, **self._state.dbargs) as conn:
            conn.row_factory = sqlite3.Row
            try:
                col_types = conn.execute("PRAGMA table_info(%s)" % self._table).fetchall()
                cur = conn.execute("SELECT * FROM %s %s" % (self._table, where_clause), params)
                for row in cur:
                    yield dict(
                        (col[0], _deserialize(row[col[0]], col_types[i]["type"]))
                        for i, col in enumerate(cur.description)
                    )
            except:
                return None

    def sql(self, where_clause, *params):
        for row in self._sql_dict(where_clause, *params):
            if row and "__pydantic" in row:
                yield self._deserialize_pydantic(row)
            else:
                yield row

    def _iter_dict(self):
        with sqlite3.connect(self._state.dbfile, **self._state.dbargs) as conn:
            conn.row_factory = sqlite3.Row
            try:
                col_types = conn.execute("PRAGMA table_info(%s)" % self._table).fetchall()
                cur = conn.execute("SELECT * FROM %s" % (self._table,))
                for row in cur:
                    yield dict(
                        (col[0], _deserialize(row[col[0]], col_types[i]["type"]))
                        for i, col in enumerate(cur.description)
                    )
            except:
                return None

    def __iter__(self):
        for row in self._iter_dict():
            if row and "__pydantic" in row:
                yield self._deserialize_pydantic(row)
            else:
                yield row

    def _getitem_dict(self, key):
        with sqlite3.connect(self._state.dbfile, **self._state.dbargs) as conn:
            conn.row_factory = sqlite3.Row
            try:
                col_types = conn.execute("PRAGMA table_info(%s)" % self._table).fetchall()
                cur = conn.execute("SELECT * FROM %s WHERE id=?" % (self._table,), (key,))

                row = cur.fetchone()
                if row:
                    return dict(
                        (col[0], _deserialize(row[col[0]], col_types[i]["type"]))
                        for i, col in enumerate(cur.description)
                    )
            except:
                return None

    @staticmethod
    def _deserialize_pydantic(row):
        module = __import__(row["__module"])
        cls = getattr(module, row["__class"])
        return cls.parse_raw(row["json"])

    def __getitem__(self, key):
        row = self._getitem_dict(key)
        if row and "__pydantic" in row:
            return self._deserialize_pydantic(row)
        return row

    def setitem_pydantic(self, key, value: BaseModel):
        self.__setitem__(key, {
            "json": value.json(encoder=_json_encoder, indent=2),
            "__class": value.__class__.__name__,
            "__module": value.__class__.__module__,
            "__pydantic": 1
        })

    def __setitem__(self, key, value):

        if isinstance(value, BaseModel):
            self.setitem_pydantic(key, value)
            return

        with sqlite3.connect(self._state.dbfile, **self._state.dbargs) as conn:
            conn.row_factory = sqlite3.Row

            sql = "INSERT INTO %s (id,%s) VALUES ('%s',%s)" % \
                  (self._table, ",".join(value.keys()), key, ",".join("?" for _ in value.values()))
            try:
                conn.execute(sql, list(_serialize(v) for v in value.values()))
            except sqlite3.OperationalError:
                if isinstance(key, int):
                    key_type = "integer"
                else:
                    key_type = "text"
                conn.execute(
                    "create table if not exists %s (id %s primary key,%s)" %
                    (self._table, key_type, ",".join("%s %s" % (k, _sqlite_type(v)) for k, v in value.items()))
                )
                conn.execute(sql, list(_serialize(v) for v in value.values()))

            except sqlite3.IntegrityError:
                sql = "UPDATE %s SET (%s) = (%s) WHERE id=?" \
                      % (self._table, ",".join(value.keys()), ",".join("?" for _ in value.values()))
                args = list(_serialize(v) for v in value.values())
                args.append(key)
                conn.execute(
                    sql,
                    args
                )

    def __delitem__(self, key):
        with sqlite3.connect(self._state.dbfile, **self._state.dbargs) as conn:
            try:
                conn.execute("DELETE FROM %s WHERE id=?" % self._table, (key,))
            except sqlite3.OperationalError:
                pass


def _sqlite_type(value):
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "real"
    if isinstance(value, bytes):
        return "blob"
    return "text"


def _serialize(value):
    if isinstance(value, bytes):
        return base64.b64encode(value)
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    return str(value)


def _deserialize(value, col_type):
    if col_type.lower() == "blob":
        return base64.b64decode(value)
    return value


class PersistentState:
    """Quick and dirty persistent dict-like SQLite wrapper"""

    def __init__(self, dbfile="state.db", logger=None, table_factory=Table, **dbargs):
        self.dbfile = dbfile
        self.logger = logger
        if dbargs is None or dbargs == {}:
            dbargs = {"timeout": 30000}
        self.dbargs = dbargs
        self._table_factory = table_factory

    def __getitem__(self, table):
        return self._table_factory(self, table)

    def __delitem__(self, key):
        with sqlite3.connect(self.dbfile, **self.dbargs) as conn:
            try:
                conn.execute(f"DROP TABLE {key}")
            except:
                pass


def pg_fetch_cursor_all(cur, name, batch_size=1000):
    while True:
        cur.execute("FETCH FORWARD %d FROM %s" % (batch_size, name))
        cnt = 0

        for row in cur:
            cnt += 1
            yield row

        if cnt != batch_size:
            cur.execute("FETCH ALL FROM %s" % (name,))
            for row in cur:
                yield row
            break


class PgConn:
    """Wrapper for PostgreSQL connection"""

    def __init__(self, logger=None, **kwargs):
        self._conn_args = kwargs
        self.conn = psycopg2.connect(**kwargs)
        self.cur = self.conn.cursor()
        self._logger = logger

    def __enter__(self):
        return self

    def exec(self, query_string, args=None):
        while True:
            try:
                if self._logger:
                    self._logger.debug(query_string)
                    self._logger.debug("With args " + str(args))

                self.cur.execute(query_string, args)
                break
            except psycopg2.Error as e:
                if e.pgcode == UNIQUE_VIOLATION:
                    break
                traceback.print_stack()
                self._handle_err(e, query_string, args)

    def query(self, query_string, args=None, max_retries=1):
        retries = max_retries
        while retries > 0:
            try:
                if self._logger:
                    self._logger.debug(query_string)
                    self._logger.debug("With args " + str(args))

                self.cur.execute(query_string, args)
                res = self.cur.fetchall()

                if self._logger:
                    self._logger.debug("result: " + str(res))

                return res
            except psycopg2.Error as e:
                if e.pgcode == UNIQUE_VIOLATION:
                    break
                self._handle_err(e, query_string, args)
                retries -= 1

    def _handle_err(self, err, query, args):
        if self._logger:
            self._logger.warning(
                "Error during query '%s' with args %s: %s %s (%s)" % (query, args, type(err), err, err.pgcode))
        self.conn = psycopg2.connect(**self._conn_args)
        self.cur = self.conn.cursor()

    def __exit__(self, type, value, traceback):
        try:
            self.conn.commit()
            self.cur.close()
        except:
            pass
