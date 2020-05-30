from collections import defaultdict
from contextlib import contextmanager
from multiprocessing import Queue
import os
import selectors
import socket
from threading import Event, Thread
import time


if hasattr(selectors, 'PollSelector'):
    _QueuePoller = selectors.PollSelector
else:
    _QueuePoller = selectors.SelectSelector


_exchanges = {}


class Exchange(object):
    """
    Exchange based on multiprocessing queue
    """

    def __init__(self, name):
        self._queue = Queue()
        self._name = name
        if os.name == 'posix':
            self._putsocket, self._getsocket = socket.socketpair()
        else:
            # Compatibility on non-POSIX systems
            server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server.bind(('127.0.0.1', 0))
            server.listen(1)
            self._putsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._putsocket.connect(server.getsockname())
            self._getsocket, _ = server.accept()
            server.close()
        self._subscribers = defaultdict(
            lambda: {
                'queue': list(),
                'buffer_interval': 0,
                'next_fire_time': 0})

    def _send(self):
        current_time = int(time.time())
        for subscriber, details in self._subscribers.items():
            print(current_time, " ", details['next_fire_time'])
            if current_time >= details['next_fire_time']:
                details['next_fire_time'] = (
                        current_time + details['buffer_interval'])
                messages = details['queue']
                sending_thread = Thread(target=subscriber.handle,
                                        args=(messages,))
                sending_thread.daemon = True
                sending_thread.start()
                details['queue'] = list()

    def _recieve(self):
        self._getsocket.recv(1)
        message = self._queue.get()
        for subscriber, details in self._subscribers.items():
            details['queue'].append(message)
        # TODO Change this into scheduling
        self._send()

    def fileno(self):
        return self._getsocket.fileno()

    def send(self, item):
        self._queue.put(item)
        self._putsocket.send(b'x')

    def attach(self, task, buffer_interval=30):
        self._subscribers[task]['buffer_interval'] = buffer_interval
        self._subscribers[task]['next_fire_time'] = \
            (int(time.time()) + buffer_interval)

    def detach(self, task):
        del self._subscribers[task]

    @contextmanager
    def subscribe(self, task):
        self.attach(task)
        try:
            yield
        finally:
            self.detach(task)

    @property
    def name(self):
        return self._name

    def __str__(self):
        return self.name

    def __repr__(self):
        return self.__str__()


class ExchangeManager(Thread):
    def __init__(self):
        super().__init__()
        self.daemon = True
        self._poller = _QueuePoller()
        self._stopped = Event()
        self._registered_queues = {}

    def _handle_read_queus(self, ready_queues):
        for selector_key, event in ready_queues:
            selector_key.fileobj._recieve()

    def run(self):
        try:
            while not self._stopped.is_set():
                ready_queues = self._poller.select(.5)
                if self._stopped.is_set():
                    break
                if ready_queues:
                    self._handle_read_queus(ready_queues)
        except Exception as ex:
            print(ex)

    def create_exchange(self, name):
        if name not in _exchanges:
            queue = Exchange(name)
            self._poller.register(queue, selectors.EVENT_READ)
            _exchanges[name] = queue
            return queue
        else:
            print("Queue already exists")

    def delete_exchange(self, name):
        try:
            queue = _exchanges[name]
            self._poller.unregister(queue)
            del _exchanges[name]
        except KeyError:
            print("Queue not registered with poller")


exchng_mngr = ExchangeManager()
exchng_mngr.start()


def get_exchange(name):
    if name not in _exchanges:
        return exchng_mngr.create_exchange(name)
    return _exchanges[name]


def delete_exchange(name):
    exchng_mngr.delete_exchange(name)
