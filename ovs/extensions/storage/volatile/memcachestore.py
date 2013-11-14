"""
Memcache store module
"""
import memcache


class MemcacheStore(object):
    """
    Memcache client wrapper:
    * stringifies the keys
    """

    def __init__(self, nodes):
        """
        Initializes the client
        """
        self._nodes = nodes
        self._client = memcache.Client(self._nodes)

    def get(self, key, default=None):
        """
        Retrieves a certain value for a given key
        """
        value = self._client.get(str(key))
        return value if value is not None else default

    def set(self, key, value, time=0):
        """
        Sets the value for a key to a given value
        """
        return self._client.set(str(key), value, time)

    def add(self, key, value, time=0):
        """
        Adds a given key to the store, expecting the key does not exists yet
        """
        return self._client.add(str(key), value, time)

    def delete(self, key):
        """
        Deletes a given key from the store
        """
        return self._client.delete(str(key))
