import os
import sys
import time
from threading import Lock

import atexit
import siphash

last_time_called = dict()


def retry(attempts, callback=None):
    def decorate(func):
        retries = attempts
        while retries > 0:
            try:
                func()
            except Exception as e:
                if callback:
                    callback(e)
    return decorate


def chunks(lst: list, chunk_len: int):
    for i in range(0, len(lst), chunk_len):
        yield lst[i:i + chunk_len]


def rate_limit(per_second):
    min_interval = 1.0 / float(per_second)

    def decorate(func):
        last_time_called[func] = 0

        def wrapper(*args, **kwargs):
            elapsed = time.perf_counter() - last_time_called[func]
            wait_time = min_interval - elapsed
            if wait_time > 0:
                time.sleep(wait_time)

            last_time_called[func] = time.perf_counter()
            return func(*args, **kwargs)

        return wrapper

    return decorate


def buffered(batch_size: int, flush_on_exit: bool = False):
    def decorate(func):
        buffer = []
        lock = Lock()
        if flush_on_exit:
            atexit.register(func, buffer)

        def wrapper(items):
            with lock:
                for item in items:
                    buffer.append(item)

                    if len(buffer) >= batch_size:
                        func(buffer)
                        buffer.clear()

        return wrapper

    return decorate


Key = b"0123456789ABCDEF"


def strhash(str):
    return siphash.SipHash24(Key, str.encode()).hash()


def signed64(i):
    return -(i & 0x8000000000000000) | (i & 0x7fffffffffffffff)


class CustomStdOut:
    original_stdout = sys.stdout

    def __init__(self, fname):
        self.fname = fname

    def __enter__(self):
        self.fp = open(self.fname, "w")
        sys.stdout = self.fp

    def __exit__(self, exc_type, exc_val, exc_tb):
        sys.stdout = CustomStdOut.original_stdout
        self.fp.close()


silent_stdout = CustomStdOut(os.devnull)
