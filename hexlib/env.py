import redis
import os

from hexlib.log import stdout_logger
from hexlib.web import Web


def get_redis():
    return redis.Redis(
        host=os.environ.get("REDIS_HOST", "localhost"),
        port=int(os.environ.get("REDIS_PORT", 6379))
    )


def get_web():
    return Web(
        proxy=os.environ.get("PROXY", None),
        rps=os.environ.get("RPS", 1),
        logger=stdout_logger,
        cookie_file=os.environ.get("COOKIE_FILE", None),
        retry_codes=set(int(x) for x in os.environ.get("RETRY_CODES", "").split(","))
    )
