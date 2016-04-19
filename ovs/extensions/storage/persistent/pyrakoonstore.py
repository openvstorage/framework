# Copyright 2016 iNuron NV
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Arakoon store module, using pyrakoon
"""

import os
import time
import ujson
import uuid
import random
from StringIO import StringIO
from threading import Lock, current_thread
from ConfigParser import RawConfigParser
from ovs.extensions.db.etcd.configuration import EtcdConfiguration
from ovs.extensions.db.arakoon.pyrakoon.pyrakoon.compat import ArakoonClient, ArakoonClientConfig
from ovs.extensions.db.arakoon.pyrakoon.pyrakoon.compat import ArakoonNotFound, ArakoonSockNotReadable, ArakoonSockReadNoBytes, ArakoonSockSendError, ArakoonAssertionFailed
from ovs.extensions.storage.exceptions import KeyNotFoundException, AssertException
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


class PyrakoonStore(object):
    """
    Arakoon client wrapper:
    * Uses json serialisation
    * Raises generic exception
    """

    ETCD_CONFIG_KEY = '/ovs/arakoon/{0}/config'

    def __init__(self, cluster):
        """
        Initializes the client
        """
        contents = EtcdConfiguration.get(PyrakoonStore.ETCD_CONFIG_KEY.format(cluster), raw=True)
        parser = RawConfigParser()
        parser.readfp(StringIO(contents))
        nodes = {}
        for node in parser.get('global', 'cluster').split(','):
            node = node.strip()
            nodes[node] = ([str(parser.get(node, 'ip'))], int(parser.get(node, 'client_port')))
        self._config = ArakoonClientConfig(str(cluster), nodes)
        self._client = ArakoonClient(self._config)
        self._identifier = int(round(random.random() * 10000000))
        self._lock = Lock()
        self._batch_size = 500
        self._sequences = {}

    @locked()
    def get(self, key):
        """
        Retrieves a certain value for a given key
        """
        try:
            return ujson.loads(PyrakoonStore._try(self._identifier, self._client.get, key))
        except ValueError:
            raise KeyNotFoundException('Could not parse JSON stored for {0}'.format(key))
        except ArakoonNotFound as field:
            raise KeyNotFoundException(field)

    @locked()
    def get_multi(self, keys):
        """
        Get multiple keys at once
        """
        try:
            for item in PyrakoonStore._try(self._identifier, self._client.multiGet, keys):
                yield ujson.loads(item)
        except ValueError:
            raise KeyNotFoundException('Could not parse JSON stored')
        except ArakoonNotFound as field:
            raise KeyNotFoundException(field)

    @locked()
    def set(self, key, value, transaction=None):
        """
        Sets the value for a key to a given value
        """
        if transaction is not None:
            return self._sequences[transaction].addSet(key, ujson.dumps(value, sort_keys=True))
        return PyrakoonStore._try(self._identifier, self._client.set, key, ujson.dumps(value, sort_keys=True))

    @locked()
    def prefix(self, prefix):
        """
        Lists all keys starting with the given prefix
        """
        next_prefix = PyrakoonStore._next_key(prefix)
        batch = None
        while batch is None or len(batch) > 0:
            batch = PyrakoonStore._try(self._identifier,
                                       self._client.range,
                                       beginKey=prefix if batch is None else batch[-1],
                                       beginKeyIncluded=batch is None,
                                       endKey=next_prefix,
                                       endKeyIncluded=False,
                                       maxElements=self._batch_size)
            for item in batch:
                yield item

    @locked()
    def prefix_entries(self, prefix):
        """
        Lists all keys starting with the given prefix
        """
        next_prefix = PyrakoonStore._next_key(prefix)
        batch = None
        while batch is None or len(batch) > 0:
            batch = PyrakoonStore._try(self._identifier,
                                       self._client.range_entries,
                                       beginKey=prefix if batch is None else batch[-1][0],
                                       beginKeyIncluded=batch is None,
                                       endKey=next_prefix,
                                       endKeyIncluded=False,
                                       maxElements=self._batch_size)
            for item in batch:
                yield [item[0], ujson.loads(item[1])]

    @locked()
    def delete(self, key, must_exist=True, transaction=None):
        """
        Deletes a given key from the store
        """
        if transaction is not None:
            if must_exist is True:
                return self._sequences[transaction].addDelete(key)
            else:
                return self._sequences[transaction].addReplace(key, None)
        try:
            if must_exist is True:
                return PyrakoonStore._try(self._identifier, self._client.delete, key)
            else:
                return PyrakoonStore._try(self._identifier, self._client.replace, key, None)
        except ArakoonNotFound as field:
            raise KeyNotFoundException(field)

    @locked()
    def nop(self):
        """
        Executes a nop command
        """
        return PyrakoonStore._try(self._identifier, self._client.nop)

    @locked()
    def exists(self, key):
        """
        Check if key exists
        """
        return PyrakoonStore._try(self._identifier, self._client.exists, key)

    @locked()
    def assert_value(self, key, value, transaction=None):
        """
        Asserts a key-value pair
        """
        if transaction is not None:
            return self._sequences[transaction].addAssert(key, ujson.dumps(value, sort_keys=True))
        try:
            return PyrakoonStore._try(self._identifier, self._client.aSSert, key, ujson.dumps(value, sort_keys=True))
        except ArakoonAssertionFailed as assertion:
            raise AssertException(assertion)

    @locked()
    def assert_exists(self, key, transaction=None):
        """
        Asserts that a given key exists
        """
        if transaction is not None:
            return self._sequences[transaction].addAssertExists(key)
        try:
            return PyrakoonStore._try(self._identifier, self._client.aSSert_exists, key)
        except ArakoonAssertionFailed as assertion:
            raise AssertException(assertion)

    def begin_transaction(self):
        """
        Creates a transaction (wrapper around Arakoon sequences)
        """
        key = str(uuid.uuid4())
        self._sequences[key] = self._client.makeSequence()
        return key

    def apply_transaction(self, transaction):
        """
        Applies a transaction
        """
        try:
            return PyrakoonStore._try(self._identifier, self._client.sequence, self._sequences[transaction])
        except ArakoonAssertionFailed as assertion:
            raise AssertException(assertion)
        except ArakoonNotFound as field:
            raise KeyNotFoundException(field)

    @staticmethod
    def _try(identifier, method, *args, **kwargs):
        """
        Tries to call a given method, retry-ing if Arakoon is temporary unavailable
        """
        try:
            start = time.time()
            try:
                return_value = method(*args, **kwargs)
            except (ArakoonSockNotReadable, ArakoonSockReadNoBytes, ArakoonSockSendError):
                logger.debug('Error during arakoon call {0}, retry'.format(method.__name__))
                time.sleep(1)
                return_value = method(*args, **kwargs)
            duration = time.time() - start
            if duration > 0.5:
                logger.warning('Arakoon call {0} took {1}s'.format(method.__name__, round(duration, 2)))
            return return_value
        except (ArakoonNotFound, ArakoonAssertionFailed):
            # No extra logging for some errors
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
