import logging
import traceback

from influxdb import InfluxDBClient

from hexlib.misc import buffered


class Monitoring:
    def __init__(self, db, host="localhost", logger=logging.getLogger("default"), batch_size=1, flush_on_exit=False):
        self._db = db
        self._client = InfluxDBClient(host, 8086, "", "", db)
        self._logger = logger

        self._init()

        @buffered(batch_size, flush_on_exit)
        def log(points):
            self._log(points)
        self.log = log

    def db_exists(self, name):
        for db in self._client.get_list_database():
            if db["name"] == name:
                return True
        return False

    def _init(self):
        if not self.db_exists(self._db):
            self._client.create_database(self._db)

    def _log(self, points):
        try:
            self._client.write_points(points)
            self._logger.debug("InfluxDB: Wrote %d points" % len(points))
        except Exception as e:
            self._logger.debug(traceback.format_exc())
            self._logger.error(str(e))
