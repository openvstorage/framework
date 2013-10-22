import memcache


class MemcacheStore(object):
    @staticmethod
    def load():
        return MemcacheWrapper(['10.100.138.253:11211'])


class MemcacheWrapper(object):
    def __init__(self, nodes):
        self._nodes = nodes
        self._client = memcache.Client(self._nodes)

    def get(self, key):
        return self._client.get(str(key))

    def set(self, key, value):
        return self._client.set(str(key), value)

    def delete(self, key):
        return self._client.delete(str(key))