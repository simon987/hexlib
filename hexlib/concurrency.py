from queue import Queue, Empty
from multiprocessing import Process
from multiprocessing import Queue as MPQueue
from threading import Thread

from hexlib.misc import ichunks


class StatefulStreamWorker:

    def __init__(self):
        pass

    def run(self, q: Queue):
        for chunk in queue_iter(q, joinable=False, timeout=3):
            self.process_chunk(chunk)

    def process_chunk(self, chunk):
        for item in chunk:
            self.process(item)

    def process(self, item) -> None:
        raise NotImplementedError


class StatefulStreamProcessor:
    def __init__(self, worker_factory, chunk_size=128, processes=1):
        self._chunk_size = 128
        self._queue = MPQueue(maxsize=chunk_size)
        self._process_count = processes
        self._processes = []
        self._factory = worker_factory
        self._workers = []

        if processes > 1:
            for _ in range(processes):
                worker = self._factory()
                p = Process(target=worker.run, args=(self._queue,))
                p.start()
                self._processes.append(p)
                self._workers.append(worker)
        else:
            self._workers.append(self._factory())

    def injest(self, iterable):

        if self._process_count > 1:
            for chunk in ichunks(iterable, self._chunk_size):
                self._queue.put(chunk)

            for p in self._processes:
                p.join()
        else:
            for item in iterable:
                self._workers[0].process(item)

    def get_results(self):
        for worker in self._workers:
            yield worker.results()


def queue_iter(q: Queue, joinable=True, **get_args):
    while True:
        try:
            task = q.get(**get_args)

            if task is None:
                break

            yield task
            if joinable:
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
