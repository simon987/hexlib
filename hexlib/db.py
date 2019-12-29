import sqlite3


class PersistentState:
    """Quick and dirty persistent dict-like SQLite wrapper"""

    def __init__(self, dbfile="state.db"):
        self.dbfile = dbfile

    def __getitem__(self, table):
        return Table(self, table)


class Table:
    def __init__(self, state, table):
        self._state = state
        self._table = table

    def __iter__(self):
        with sqlite3.connect(self._state.dbfile) as conn:
            conn.row_factory = sqlite3.Row
            try:
                cur = conn.execute("SELECT * FROM %s" % (self._table,))
                for row in cur:
                    yield dict(row)
            except:
                return None

    def __getitem__(self, item):
        with sqlite3.connect(self._state.dbfile) as conn:
            conn.row_factory = sqlite3.Row
            try:
                cur = conn.execute("SELECT * FROM %s WHERE id=?" % (self._table,), (item,))

                row = cur.fetchone()
                if row:
                    return dict(row)
            except:
                return None

    def __setitem__(self, key, value):

        with sqlite3.connect(self._state.dbfile) as conn:
            conn.row_factory = sqlite3.Row

            sql = "INSERT INTO %s (id,%s) VALUES ('%s',%s)" % \
                  (self._table, ",".join(value.keys()), key, ",".join("?" for _ in value.values()))
            try:
                conn.execute(sql, list(_sqlite_value(v) for v in value.values()))
            except sqlite3.OperationalError:
                conn.execute(
                    "create table if not exists %s (id text primary key,%s)" %
                    (self._table, ",".join("%s %s" % (k, _sqlite_type(v)) for k, v in value.items()))
                )
                conn.execute(sql, list(_sqlite_value(v) for v in value.values()))

            except sqlite3.IntegrityError:
                sql = "UPDATE %s SET (%s) = (%s) WHERE id=?" \
                      % (self._table, ",".join(value.keys()), ",".join("?" for _ in value.values()))
                args = [key]
                args.extend(_sqlite_value(v) for v in value.values())
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


def _sqlite_value(value):
    if isinstance(value, bytes):
        return _sqlite_value(value.decode("unicode_escape"))
    return str(value)

