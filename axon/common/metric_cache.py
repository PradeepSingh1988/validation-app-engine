import abc
import logging
from threading import Event, Lock, Thread
import time


class Counter(object):

    def __init__(self):
        self._lock = Lock()
        self._count = 0

    def inc(self, val=1):
        """Increase the value of the counter."""
        with self._lock:
            self._count += val

    def count(self):
        """Get the value of the counter."""
        with self._lock:
            return self._count

    def clear(self):
        """Reset the counter."""
        with self._lock:
            self._count = 0

    def dec(self, val=1):
        self.inc(-val)


class MetricsCache(object):

    def __init__(self):
        self._counters = {}

    def counter(self, key):
        if key not in self._counters:
            self._counters[key] = Counter()
        return self._counters[key]

    def clear(self):
        self._counters.clear()

    def dump_metrics(self):
        metrics = {}
        for key in self._counters.keys():
            metrics[key] = self._counters[key]
        return metrics


class Reporter(abc.ABC):
    def __init__(self, cache, reporting_interval=30):
        self._cache = cache
        self._reporting_interval = reporting_interval
        self._stopped_event = Event()
        self._reporting_thread = Thread(target=self._report_loop)
        self._reporting_thread.daemon = True
        self._reporting_thread.start()

    def stop(self):
        self._stopped_event.set()

    def _report_loop(self):
        next_run_time = time.time()
        try:
            while not self._stopped_event.is_set():
                try:
                    self.report(self._cache)
                except Exception as e:
                    print("here", e)
                next_run_time += self._reporting_interval
                wait = max(0, next_run_time - time.time())
                time.sleep(wait)
        except Exception as e:
            print(e)

    @abc.abstractmethod
    def report(self, cache):
        pass


class ExchangeReporter(Reporter):
    log = logging.getLogger(__name__)

    def __init__(self, cache, exchange, reporting_interval=30):
        super().__init__(cache, reporting_interval)
        self._exchange = exchange

    def report(self, cache):
        metrics = cache.dump_metrics()
        count_dict = {}
        for key in metrics.keys():
            counter = metrics[key]
            count = counter.count()
            if count == 0:
                continue
            count_dict.update({key: count})
            counter.dec(count)
        if count_dict:
            self._exchange.send(count_dict)