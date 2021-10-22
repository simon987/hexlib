import json
from collections import namedtuple
from functools import partial
from itertools import islice
from time import sleep, time

from orjson import orjson
from redis import Redis

RoutingKeyParts = namedtuple(
    "RoutingKeyParts",
    ["arc_list", "project", "subproject", "type", "category"]
)


def parse_routing_key(key):
    tokens = key.split(".")

    if len(tokens) == 4:
        arc_list, project, type_, category = tokens
        return RoutingKeyParts(
            arc_list=arc_list,
            project=project,
            subproject=None,
            type=type_,
            category=category
        )
    else:
        arc_list, project, subproject, type_, category = tokens
        return RoutingKeyParts(
            arc_list=arc_list,
            project=project,
            subproject=subproject,
            type=type_,
            category=category
        )


class MessageQueue:
    def read_messages(self, topics):
        raise NotImplementedError()

    def publish(self, item, item_project, item_type, item_subproject, item_category):
        raise NotImplementedError()


class RedisMQ(MessageQueue):
    _MAX_KEYS = 30

    def __init__(self, rdb, consumer_name="redis_mq", sep=".", max_pending_time=120, logger=None, publish_channel=None,
                 arc_lists=None, wait=1):
        self._rdb: Redis = rdb
        self._key_cache = None
        self._consumer_id = consumer_name
        self._pending_list = f"pending{sep}{consumer_name}"
        self._max_pending_time = max_pending_time
        self._logger = logger
        self._publish_channel = publish_channel
        self._arc_lists = arc_lists
        self._wait = wait

    def _get_keys(self, pattern):
        if self._key_cache:
            return self._key_cache

        keys = list(islice(
            self._rdb.scan_iter(match=pattern, count=RedisMQ._MAX_KEYS), RedisMQ._MAX_KEYS
        ))
        self._key_cache = keys

        return keys

    def _get_pending_tasks(self):
        for task_id, pending_task in self._rdb.hscan_iter(self._pending_list):

            pending_task_json = orjson.loads(pending_task)

            if time() >= pending_task_json["resubmit_at"]:
                yield pending_task_json["topic"], pending_task_json["task"], partial(self._ack, task_id)

    def _ack(self, task_id):
        self._rdb.hdel(self._pending_list, task_id)

    def read_messages(self, topics):
        """
        Assumes json-encoded tasks with an _id field

        Tasks are automatically put into a pending list until ack() is called.
        When a task has been in the pending list for at least max_pending_time seconds, it
        gets submitted again
        """

        assert len(topics) == 1, "RedisMQ only supports 1 topic pattern"

        pattern = topics[0]
        counter = 0

        if self._logger:
            self._logger.info(f"MQ>Listening for new messages in {pattern}")

        while True:
            counter += 1

            if counter % 1000 == 0:
                yield from self._get_pending_tasks()

            keys = self._get_keys(pattern)
            if not keys:
                sleep(self._wait)
                self._key_cache = None
                continue

            result = self._rdb.blpop(keys, timeout=1)
            if not result:
                self._key_cache = None
                continue

            topic, task = result

            task_json = orjson.loads(task)
            topic = topic.decode()

            if "_id" not in task_json:
                raise ValueError(f"Task doesn't have _id field: {task}")

            # Immediately put in pending queue
            self._rdb.hset(
                self._pending_list, task_json["_id"],
                orjson.dumps({
                    "resubmit_at": time() + self._max_pending_time,
                    "topic": topic,
                    "task": task_json
                })
            )

            yield topic, task_json, partial(self._ack, task_json["_id"])

    def publish(self, item, item_project, item_type, item_subproject=None, item_category="x"):

        if "_id" not in item:
            raise ValueError("_id field must be set for item")

        item = json.dumps(item, separators=(',', ':'), ensure_ascii=False, sort_keys=True)

        item_project = item_project.replace(".", "-")
        item_subproject = item_subproject.replace(".", "-") if item_subproject else None

        item_source = item_project if not item_subproject else f"{item_project}.{item_subproject}"

        item_type = item_type.replace(".", "-")
        item_category = item_category.replace(".", "-")

        # If specified, fan-out to pub/sub channel
        if self._publish_channel is not None:
            routing_key = f"{self._publish_channel}.{item_source}.{item_type}.{item_category}"
            self._rdb.publish(routing_key, item)

        # Save to list
        for arc_list in self._arc_lists:
            routing_key = f"{arc_list}.{item_source}.{item_type}.{item_category}"
            self._rdb.lpush(routing_key, item)
