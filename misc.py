import time

last_time_called = dict()


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

