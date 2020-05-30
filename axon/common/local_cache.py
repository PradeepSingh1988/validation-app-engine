import abc
from threading import Lock


class BaseCache(abc.ABC):

    @abc.abstractmethod
    def add(self, key, value):
        pass

    @abc.abstractmethod
    def get(self, key, default=None):
        pass

    @abc.abstractmethod
    def delete(self, key):
        pass

    def get_many(self, keys):
        d = {}
        for k in keys:
            val = self.get(k)
            if val is not None:
                d[k] = val
        return d

    def has_key(self, key):
        return self.get(key) is not None

    def __contains__(self, key):
        return key in self

    def set_many(self, data):
        for key, value in data.items():
            self.add(key, value)
        return []

    def delete_many(self, keys):
        for key in keys:
            self.delete(key)

    @abc.abstractmethod
    def clear(self):
        pass


class MemCache(BaseCache):

    def __init__(self):
        self._cache = dict()
        self._lock = Lock()

    def add(self, key, value):
        with self._lock:
            self._cache[key] = value

    def get(self, key, default=None):
        with self._lock:
            if key not in self._cache:
                return default
            return self._cache[key]

    def _delete(self, key):
        try:
            del self._cache[key]
        except KeyError:
            pass

    def delete(self, key):
        with self._lock:
            self._delete(key)

    def clear(self):
        with self._lock:
            self._cache.clear()

    def get_all_keys(self):
        with self._lock:
            return list(self._cache.keys())
