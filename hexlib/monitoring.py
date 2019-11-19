import logging
import traceback

from influxdb import InfluxDBClient


class Monitoring:
    def __init__(self, db, host="localhost", logger=logging.getLogger("default")):
        self._db = db
        self._client = InfluxDBClient(host, 8086, "", "", db)
        self._logger = logger

        self._init()

    def db_exists(self, name):
        for db in self._client.get_list_database():
            if db["name"] == name:
                return True
        return False

    def _init(self):
        if not self.db_exists(self._db):
            self._client.create_database(self._db)

    def log(self, event):
        try:
            self._client.write_points(event)
        except Exception as e:
            self._logger.debug(traceback.format_exc())
            self._logger.error(str(e))
