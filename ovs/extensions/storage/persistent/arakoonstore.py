# Copyright 2014 iNuron NV
#
# Licensed under the Open vStorage Modified Apache License (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.openvstorage.org/license
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Arakoon store module
"""

import os
import json
import time
import random
from threading import Lock, current_thread
from ovs.extensions.db.arakoon.ArakoonManagement import ArakoonManagementEx
from ovs.extensions.db.arakoon.arakoon.ArakoonExceptions import ArakoonNotFound, ArakoonSockReadNoBytes
from ovs.extensions.storage.exceptions import KeyNotFoundException
from ovs.log.logHandler import LogHandler

logger = LogHandler.get('extensions', name='arakoon_store')


def locked():
    """
    Locking decorator.
    """
    def wrap(f):
        """
        Returns a wrapped function
        """
        def new_function(self, *args, **kw):
            """
            Executes the decorated function in a locked context
            """
            with self._lock:
                return f(self, *args, **kw)
        return new_function
    return wrap


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
        self._cluster = ArakoonManagementEx().getCluster(cluster)
        self._client = self._cluster.getClient()
        self._identifier = int(round(random.random() * 10000000))
        self._lock = Lock()
        self._batch_size = 100

    @locked()
    def get(self, key):
        """
        Retrieves a certain value for a given key
        """
        try:
            return json.loads(ArakoonStore._try(self._identifier, self._client.get, key))
        except ValueError:
            raise KeyNotFoundException('Could not parse JSON stored for {0}'.format(key))
        except ArakoonNotFound as field:
            raise KeyNotFoundException(field)

    @locked()
    def set(self, key, value):
        """
        Sets the value for a key to a given value
        """
        return ArakoonStore._try(self._identifier, self._client.set, key, json.dumps(value))

    @locked()
    def prefix(self, prefix):
        """
        Lists all keys starting with the given prefix
        """
        next_prefix = ArakoonStore._next_key(prefix)
        batch = None
        while batch is None or len(batch) > 0:
            batch = ArakoonStore._try(self._identifier,
                                      self._client.range,
                                      beginKey=prefix if batch is None else batch[-1],
                                      beginKeyIncluded=batch is None,
                                      endKey=next_prefix,
                                      endKeyIncluded=False,
                                      maxElements=self._batch_size)
            for item in batch:
                yield item

    @locked()
    def delete(self, key):
        """
        Deletes a given key from the store
        """
        try:
            return ArakoonStore._try(self._identifier, self._client.delete, key)
        except ArakoonNotFound as field:
            raise KeyNotFoundException(field)

    @locked()
    def nop(self):
        """
        Executes a nop command
        """
        return ArakoonStore._try(self._identifier, self._client.nop)

    @locked()
    def exists(self, key):
        """
        Check if key exists
        """
        return ArakoonStore._try(self._identifier, self._client.exists, key)

    @staticmethod
    def _try(identifier, method, *args, **kwargs):
        """
        Tries to call a given method, retry-ing if Arakoon is temporary unavailable
        """
        try:
            last_exception = None
            tries = 5
            while tries > 0:
                try:
                    return method(*args, **kwargs)
                except ArakoonSockReadNoBytes as exception:
                    logger.debug('Error during {0}, retry'.format(method.__name__))
                    last_exception = exception
                    tries -= 1
                    time.sleep(1)
            raise last_exception
        except ArakoonNotFound:
            # No extra logging for ArakoonNotFound
            raise
        except Exception:
            logger.error('Error during {0}. Process {1}, thread {2}, clientid {3}'.format(
                method.__name__, os.getpid(), current_thread().ident, identifier
            ))
            raise

    @staticmethod
    def _next_key(key):
        """
        Calculates the next key (to be used in range queries)
        """
        encoding = 'ascii'  # For future python 3 compatibility
        array = bytearray(str(key), encoding)
        for index in range(len(array) - 1, -1, -1):
            array[index] += 1
            if array[index] < 128:
                while array[-1] == 0:
                    array = array[:-1]
                return str(array.decode(encoding))
            array[index] = 0
        return '\xff'
