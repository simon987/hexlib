from queue import Queue, Empty
from multiprocessing import Process
from threading import Thread


def queue_iter(q: Queue, **get_args):
    while True:
        try:
            task = q.get(**get_args)

            if task is None:
                break

            yield task
            q.task_done()
        except Empty:
            break
        except KeyboardInterrupt:
            break


def queue_thread_exec(q, func, thread_count=4):
    threads = []

    for _ in range(thread_count):
        t = Thread(target=func, args=(q,))
        threads.append(t)
        t.start()

    q.join()

    for t in threads:
        t.join()


def queue_process_exec(q, func, process_count=4):
    processes = []

    for _ in range(process_count):
        p = Process(target=func, args=(q,))
        processes.append(p)
        p.start()

    q.join()

    for p in processes:
        p.join()
