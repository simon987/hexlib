import base64
import sqlite3
import redis
import orjson as json
import umsgpack


class PersistentState:
    """Quick and dirty persistent dict-like SQLite wrapper"""

    def __init__(self, dbfile="state.db", **dbargs):
        self.dbfile = dbfile
        if dbargs is None:
            dbargs = {"timeout": 30000}
        self.dbargs = dbargs

    def __getitem__(self, table):
        return Table(self, table)


class VolatileState:
    """Quick and dirty volatile dict-like redis wrapper"""

    def __init__(self, prefix, **redis_args):
        self.rdb = redis.Redis(**redis_args)
        self.prefix = prefix

    def __getitem__(self, table):
        return RedisTable(self, table)


class VolatileBooleanState:
    """Quick and dirty volatile dict-like redis wrapper for boolean values"""

    def __init__(self, prefix, **redis_args):
        self.rdb = redis.Redis(**redis_args)
        self.prefix = prefix

    def __getitem__(self, table):
        return RedisBooleanTable(self, table)


class RedisTable:
    def __init__(self, state, table):
        self._state = state
        self._table = table

    def __setitem__(self, key, value):
        self._state.rdb.hset(self._state.prefix + self._table, str(key), umsgpack.dumps(value))

    def __getitem__(self, key):
        val = self._state.rdb.hget(self._state.prefix + self._table, str(key))
        if val:
            return umsgpack.loads(val)
        return None

    def __delitem__(self, key):
        self._state.rdb.hdel(self._state.prefix + self._table, str(key))

    def __iter__(self):
        val = self._state.rdb.hgetall(self._state.prefix + self._table)
        if val:
            return ((k, umsgpack.loads(v)) for k, v in
                    self._state.rdb.hgetall(self._state.prefix + self._table).items())


class RedisBooleanTable:
    def __init__(self, state, table):
        self._state = state
        self._table = table

    def __setitem__(self, key, value):
        if value:
            self._state.rdb.sadd(self._state.prefix + self._table, str(key))
        else:
            self.__delitem__(key)

    def __getitem__(self, key):
        return self._state.rdb.sismember(self._state.prefix + self._table, str(key))

    def __delitem__(self, key):
        self._state.rdb.srem(self._state.prefix + self._table, str(key))

    def __iter__(self):
        return iter(self._state.rdb.smembers(self._state.prefix + self._table))


class Table:
    def __init__(self, state, table):
        self._state = state
        self._table = table

    def __iter__(self):
        with sqlite3.connect(self._state.dbfile, **self._state.dbargs) as conn:
            conn.row_factory = sqlite3.Row
            try:
                cur = conn.execute("SELECT * FROM %s" % (self._table,))
                for row in cur:
                    yield dict(row)
            except:
                return None

    def __getitem__(self, item):
        with sqlite3.connect(self._state.dbfile, **self._state.dbargs) as conn:
            conn.row_factory = sqlite3.Row
            try:
                col_types = conn.execute("PRAGMA table_info(%s)" % self._table).fetchall()
                cur = conn.execute("SELECT * FROM %s WHERE id=?" % (self._table,), (item,))

                row = cur.fetchone()
                if row:
                    return dict(
                        (col[0], _deserialize(row[col[0]], col_types[i]["type"]))
                        for i, col in enumerate(cur.description)
                    )
            except:
                return None

    def __setitem__(self, key, value):

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
    return str(value)


def _deserialize(value, col_type):
    if col_type == "blob":
        return base64.b64decode(value)
    return value


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
