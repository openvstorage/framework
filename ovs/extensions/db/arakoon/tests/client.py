# Copyright (C) 2016 iNuron NV
#
# This file is part of Open vStorage Open Source Edition (OSE),
# as available from
#
#      http://www.openvstorage.org and
#      http://www.openvstorage.com.
#
# This file is free software; you can redistribute it and/or modify it
# under the terms of the GNU Affero General Public License v3 (GNU AGPLv3)
# as published by the Free Software Foundation, in version 3 as it comes
# in the LICENSE.txt file of the Open vStorage OSE distribution.
#
# Open vStorage is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY of any kind.

"""
Arakoon store module, using pyrakoon
"""

import os
import time
import uuid
import copy
import ujson
import random
from threading import Lock, current_thread
from ovs.extensions.db.arakoon.pyrakoon.pyrakoon.compat import ArakoonClient, ArakoonClientConfig
from ovs.extensions.db.arakoon.pyrakoon.pyrakoon.compat import ArakoonNotFound, ArakoonSockNotReadable, ArakoonSockReadNoBytes, ArakoonSockSendError, ArakoonAssertionFailed
from ovs.log.log_handler import LogHandler


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


class MockPyrakoonClient(object):
    """
    Arakoon client wrapper:
    * Uses json serialisation
    * Raises generic exception
    """
    _data = {}
    _sequences = {}

    def __init__(self, cluster, nodes):
        """
        Initializes the client
        """
        _ = nodes
        self._lock = Lock()
        self._cluster = cluster
        if cluster not in self._sequences:
            self._sequences[cluster] = {}
        if cluster not in self._data:
            self._data[cluster] = {}
        self._keep_in_memory_only = True

    def _read(self):
        return self._data.get(self._cluster, {})

    def _write(self, data):
        self._data[self._cluster] = data

    @locked()
    def get(self, key, consistency=None):
        """
        Retrieves a certain value for a given key
        """
        _ = consistency
        data = self._read()
        if key in data:
            return copy.deepcopy(data[key])
        else:
            raise ArakoonNotFound(key)

    @locked()
    def get_multi(self, keys):
        """
        Get multiple keys at once
        """
        data = self._read()
        for key in keys:
            if key in data:
                yield copy.deepcopy(data[key])
            else:
                raise ArakoonNotFound(key)

    @locked()
    def set(self, key, value, transaction=None):
        """
        Sets the value for a key to a given value
        """
        if transaction is not None:
            return self._sequences[transaction].append([self.set, {'key': key, 'value': value}])
        data = self._read()
        data[key] = copy.deepcopy(value)
        self._write(data)

    @locked()
    def prefix(self, prefix):
        """
        Lists all keys starting with the given prefix
        """
        data = self._read()
        return [k for k in data.keys() if k.startswith(prefix)]

    @locked()
    def prefix_entries(self, prefix):
        """
        Lists all keys starting with the given prefix
        """
        data = self._read()
        return [(k, v) for k, v in data.iteritems() if k.startswith(prefix)]

    @locked()
    def delete(self, key, must_exist=True, transaction=None):
        """
        Deletes a given key from the store
        """
        if transaction is not None:
            return self._sequences[transaction].append([self.delete, {'key': key, 'must_exist': must_exist}])
        data = self._read()
        if key in data:
            del data[key]
            self._write(data)
        elif must_exist is True:
            raise ArakoonNotFound(key)

    @locked()
    def delete_prefix(self, prefix):
        """
        Removes a given prefix from the store
        """
        data = self._read()
        for key in data.keys():
            if key.startswith(prefix):
                del data[key]
        self._write(data)

    @locked()
    def nop(self):
        """
        Executes a nop command
        """
        pass

    def exists(self, key):
        """
        Check if key exists
        """
        try:
            self.get(key)
            return True
        except ArakoonNotFound:
            return False

    @locked()
    def assert_value(self, key, value, transaction=None):
        """
        Asserts a key-value pair
        """
        if transaction is not None:
            return self._sequences[transaction].append([self.assert_value, {'key': key, 'value': value}])
        data = self._read()
        if key not in data:
            raise ArakoonNotFound(key)
        if ujson.dumps(data[key], sort_keys=True) != ujson.dumps(value, sort_keys=True):
            raise ArakoonNotFound(key)

    @locked()
    def assert_exists(self, key, transaction=None):
        """
        Asserts that a given key exists
        """
        if transaction is not None:
            return self._sequences[transaction].append([self.assert_exists, {'key': key}])
        data = self._read()
        if key not in data:
            raise ArakoonNotFound(key)

    def begin_transaction(self):
        """
        Creates a transaction (wrapper around Arakoon sequences)
        """
        key = str(uuid.uuid4())
        self._sequences[self._cluster][key] = []
        return key

    def apply_transaction(self, transaction):
        """
        Applies a transaction
        """
        for item in self._sequences[transaction]:
            item[0](**item[1])

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
