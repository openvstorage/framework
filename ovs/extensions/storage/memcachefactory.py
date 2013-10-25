import memcache


class MemcacheFactory(object):
    @staticmethod
    def load():
        return MemcacheStore(['10.100.138.253:11211'])


class MemcacheStore(object):
    def __init__(self, nodes):
        self._nodes = nodes
        self._client = memcache.Client(self._nodes)

    def get(self, key, default=None):
        value = self._client.get(str(key))
        return value if value is not None else default

    def set(self, key, value, time=0):
        return self._client.set(str(key), value, time)

    def add(self, key, value, time=0):
        return self._client.add(str(key), value, time)

    def delete(self, key):
        return self._client.delete(str(key))