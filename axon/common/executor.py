from concurrent.futures import ThreadPoolExecutor
from threading import BoundedSemaphore


class _BoundedPoolExecutor(object):
    semaphore = None

    def acquire(self):
        self.semaphore.acquire()

    def release(self, fn):
        self.semaphore.release()

    def submit(self, fn, *args, **kwargs):
        self.acquire()
        future = super(_BoundedPoolExecutor, self).submit(fn, *args, **kwargs)
        future.add_done_callback(self.release)
        return future


class BoundedThreadPoolExecutor(_BoundedPoolExecutor, ThreadPoolExecutor):

    def __init__(self, max_workers=None):
        super(BoundedThreadPoolExecutor, self).__init__(max_workers)
        self.semaphore = BoundedSemaphore(max_workers)