# license see http://www.openvstorage.com/licenses/opensource/
"""
Arakoon store module
"""

import json
import time

from ovs.extensions.db.arakoon.ArakoonManagement import ArakoonManagement
from ovs.extensions.db.arakoon.ArakoonExceptions import ArakoonNotFound, ArakoonException
from ovs.extensions.storage.exceptions import KeyNotFoundException


class ArakoonStore(object):
    """
    Arakoon client wrapper:
    * Uses json serialisation
    * Raises generic exception
    """

    def __init__(self, cluster):
        """
        Initializes the client
        """
        self._cluster_name = cluster
        self._cluster = None
        self._client = None
        self._load_client()
        self._max_call_length = 30

    def _load_client(self):
        """
        Loads the client
        """
        self._cluster = ArakoonManagement().getCluster(self._cluster_name)
        self._client = self._cluster.getClient()

    def _try(self, method, args, kwargs):
        """
        Executes a given method with automatic retry in case of troubles
        """
        start = time.time()
        delay = 0.1
        exception = None
        while (time.time() - start) < self._max_call_length:
            try:
                return method(*args, **kwargs)
            except ArakoonException as aex:
                exception = aex
                delay *= 2
                self._load_client()
                time.sleep(delay)
        raise exception

    def get(self, *args, **kwargs):
        """
        Retrieves a certain value for a given key
        """
        return self._try(self._get, args, kwargs)

    def _get(self, key):
        """
        Retrieves a certain value for a given key
        """
        try:
            return json.loads(self._client.get(key))
        except ArakoonNotFound as field:
            raise KeyNotFoundException(field)

    def set(self, *args, **kwargs):
        """
        Sets the value for a key to a given value
        """
        return self._try(self._set, args, kwargs)

    def _set(self, key, value):
        """
        Sets the value for a key to a given value
        """
        return self._client.set(key, json.dumps(value))

    def prefix(self, *args, **kwargs):
        """
        Lists all keys starting with the given prefix
        """
        return self._try(self._prefix, args, kwargs)

    def _prefix(self, prefix):
        """
        Lists all keys starting with the given prefix
        """
        return self._client.prefix(prefix)

    def delete(self, *args, **kwargs):
        """
        Deletes a given key from the store
        """
        return self._try(self._delete, args, kwargs)

    def _delete(self, key):
        """
        Deletes a given key from the store
        """
        try:
            return self._client.delete(key)
        except ArakoonNotFound as field:
            raise KeyNotFoundException(field)
