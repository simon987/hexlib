import os

import redis
from fake_useragent import UserAgent

from hexlib.log import stdout_logger
from hexlib.web import Web

ARC_LISTS = os.environ.get("ARC_LISTS", "arc").split(",")
PUBLISH_CHANNEL = os.environ.get("PUBLISH_CHANNEL", None)


def get_redis():
    return redis.Redis(
        host=os.environ.get("REDIS_HOST", "localhost"),
        port=int(os.environ.get("REDIS_PORT", 6379))
    )


def redis_publish(rdb, item, item_project,  item_type, item_subproject=None, item_category="x"):

    item_project = item_project.replace(".", "-")
    item_subproject = item_subproject.replace(".", "-")

    item_source = item_project if not item_subproject else f"{item_project}.{item_subproject}"

    item_type = item_type.replace(".", "-")
    item_category = item_category.replace(".", "-")

    if PUBLISH_CHANNEL is not None:
        routing_key = f"{PUBLISH_CHANNEL}.{item_source}.{item_type}.{item_category}"
        rdb.publish(routing_key, item)
    for arc_list in ARC_LISTS:
        routing_key = f"{arc_list}.{item_source}.{item_type}.{item_category}"
        rdb.lpush(routing_key, item)


def get_web():
    ua = UserAgent()

    return Web(
        proxy=os.environ.get("PROXY", None),
        rps=os.environ.get("RPS", 1),
        logger=stdout_logger,
        cookie_file=os.environ.get("COOKIE_FILE", None),
        retry_codes=set(int(x) if x else None for x in os.environ.get("RETRY_CODES", "").split(",")),
        ua=ua[os.environ.get("USER_AGENT")] if os.environ.get("USER_AGENT", None) is not None else None
    )
