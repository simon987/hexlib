from multiprocessing import Process
from multiprocessing import Queue as MPQueue
from queue import Queue, Empty
from threading import Thread

from hexlib.misc import ichunks


class StatelessStreamWorker:

    def __init__(self):
        self._q_out = None

    def run(self, q: Queue, q_out: Queue):

        self._q_out: Queue = q_out

        for chunk in queue_iter(q, joinable=False, timeout=10):
            self._process_chunk(chunk)

    def _process_chunk(self, chunk):
        results = []

        for item in chunk:
            result = self.process(item)
            if result is not None:
                results.append(result)

        if results:
            self._q_out.put(results)

    def process(self, item):
        raise NotImplementedError


class StatelessStreamProcessor:
    def __init__(self, worker_factory, chunk_size=128, processes=1):
        self._chunk_size = 128
        self._queue = MPQueue(maxsize=chunk_size)
        self._queue_out = MPQueue(maxsize=processes * 2)
        self._process_count = processes
        self._processes = []
        self._factory = worker_factory
        self._workers = []

        if processes > 1:
            for _ in range(processes):
                worker = self._factory()
                p = Process(target=worker.run, args=(self._queue, self._queue_out))
                p.start()

                self._processes.append(p)
                self._workers.append(worker)
        else:
            self._workers.append(self._factory())

    def _ingest(self, iterable):
        if self._process_count > 1:
            for chunk in ichunks(iterable, self._chunk_size):
                self._queue.put(chunk)
        else:
            for item in iterable:
                self._workers[0].process(item)

    def ingest(self, iterable):

        ingest_thread = Thread(target=self._ingest, args=(iterable,))
        ingest_thread.start()

        for results in queue_iter(self._queue_out, joinable=False, timeout=10):
            yield from results

        ingest_thread.join()


class StatefulStreamWorker:

    def __init__(self):
        pass

    def run(self, q: Queue, q_out: Queue):
        for chunk in queue_iter(q, joinable=False, timeout=3):
            self._process_chunk(chunk)

        q_out.put(self.results())

    def _process_chunk(self, chunk):
        for item in chunk:
            self.process(item)

    def process(self, item) -> None:
        raise NotImplementedError

    def results(self):
        raise NotImplementedError


class StatefulStreamProcessor:
    def __init__(self, worker_factory, chunk_size=128, processes=1):
        self._chunk_size = 128
        self._queue = MPQueue(maxsize=chunk_size)
        self._queue_out = MPQueue()
        self._process_count = processes
        self._processes = []
        self._factory = worker_factory
        self._workers = []

        if processes > 1:
            for _ in range(processes):
                worker = self._factory()
                p = Process(target=worker.run, args=(self._queue, self._queue_out))
                p.start()

                self._processes.append(p)
                self._workers.append(worker)
        else:
            self._workers.append(self._factory())

    def ingest(self, iterable):

        if self._process_count > 1:
            for chunk in ichunks(iterable, self._chunk_size):
                self._queue.put(chunk)
        else:
            for item in iterable:
                self._workers[0].process(item)

    def get_results(self):
        for _ in range(self._process_count):
            yield self._queue_out.get()
        for p in self._processes:
            p.join()


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
