import traceback
from abc import ABC

from influxdb import InfluxDBClient

from hexlib.misc import buffered


class Monitoring(ABC):
    def log(self, points):
        raise NotImplementedError()


class BufferedInfluxDBMonitoring(Monitoring):
    def __init__(self, db_name, host="localhost", port=8086, logger=None, batch_size=1, flush_on_exit=False):
        self._db = db_name
        self._client = InfluxDBClient(host, port, "", "", db_name)
        self._logger = logger

        if not self.db_exists(self._db):
            self._client.create_database(self._db)

        @buffered(batch_size, flush_on_exit)
        def log(points):
            self._log(points)

        self.log = log

    def db_exists(self, name):
        for db in self._client.get_list_database():
            if db["name"] == name:
                return True
        return False

    def log(self, points):
        # Is overwritten in __init__()
        pass

    def _log(self, points):
        try:
            self._client.write_points(points)
            if self._logger:
                self._logger.debug("InfluxDB: Wrote %d points" % len(points))
        except Exception as e:
            if self._logger:
                self._logger.debug(traceback.format_exc())
                self._logger.error(str(e))
