import base64
import sqlite3
import redis
import ujson as json


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
    def __init__(self, prefix, ttl=3600, **redis_args):
        self.rdb = redis.Redis(**redis_args)
        self.prefix = prefix
        self.ttl = 3600

    def __getitem__(self, table):
        return RedisTable(self, table)


class RedisTable:
    def __init__(self, state, table):
        self._state = state
        self._table = table

    def __setitem__(self, key, value):
        self._state.rdb.set(self._state.prefix + self._table + ":" + str(key), json.dumps(value), ex=self._state.ttl)

    def __getitem__(self, key):
        val = self._state.rdb.get(self._state.prefix + self._table + ":" + str(key))
        if val:
            return json.loads(val)
        return None

    def __delitem__(self, key):
        self._state.rdb.delete(self._state.prefix + self._table + ":" + str(key))

    def __iter__(self):
        for key in self._state.rdb.scan_iter(self._state.prefix + self._table + "*"):
            val = self._state.rdb.get(key)
            yield json.loads(val) if val else None


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
                conn.execute(
                    "create table if not exists %s (id text primary key,%s)" %
                    (self._table, ",".join("%s %s" % (k, _sqlite_type(v)) for k, v in value.items()))
                )
                conn.execute(sql, list(_serialize(v) for v in value.values()))

            except sqlite3.IntegrityError:
                sql = "UPDATE %s SET (%s) = (%s) WHERE id=?" \
                      % (self._table, ",".join(value.keys()), ",".join("?" for _ in value.values()))
                args = [key]
                args.extend(_serialize(v) for v in value.values())
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
    return str(value)


def _deserialize(value, col_type):
    if col_type == "blob":
        return base64.b64decode(value)
    return value
