# license see http://www.openvstorage.com/licenses/opensource/
"""
Arakoon store module
"""

import json

from arakoon import Arakoon
from arakoon.ArakoonExceptions import ArakoonNotFound
from ovs.extensions.storage.exceptions import KeyNotFoundException


class ArakoonStore(object):
    """
    Arakoon client wrapper:
    * Uses json serialisation
    * Raises generic exception
    """

    def __init__(self, cluster, node_config):
        """
        Initializes the client
        """
        self._cluster = cluster
        self._node_config = node_config
        self._config = Arakoon.ArakoonClientConfig(self._cluster, self._node_config)
        self._client = Arakoon.ArakoonClient(config=self._config)

    def get(self, key):
        """
        Retrieves a certain value for a given key
        """
        try:
            return json.loads(self._client.get(key))
        except ArakoonNotFound as field:
            raise KeyNotFoundException(field)

    def set(self, key, value):
        """
        Sets the value for a key to a given value
        """
        return self._client.set(key, json.dumps(value))

    def prefix(self, prefix):
        """
        Lists all keys starting with the given prefix
        """
        return self._client.prefix(prefix)

    def delete(self, key):
        """
        Deletes a given key from the store
        """
        try:
            return self._client.delete(key)
        except ArakoonNotFound as field:
            raise KeyNotFoundException(field)
