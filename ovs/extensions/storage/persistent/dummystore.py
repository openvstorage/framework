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
Dummy persistent module
"""
import os
import json
import uuid
from ovs.extensions.storage.exceptions import KeyNotFoundException, AssertException


class DummyPersistentStore(object):
    """
    This is a dummy persistent store that makes use of a local json file
    """
    _path = '/run/dummypersistent.json'
    _data = {}

    def __init__(self):
        self._sequences = {}
        self._keep_in_memory_only = False

    def clean(self):
        """
        Empties the store
        """
        if self._keep_in_memory_only is True:
            DummyPersistentStore._data = {}
        else:
            try:
                os.remove(DummyPersistentStore._path)
            except OSError:
                pass

    def _read(self):
        """
        Reads the local json file
        """
        if self._keep_in_memory_only is True:
            return DummyPersistentStore._data

        try:
            f = open(self._path, 'r')
            data = json.loads(f.read())
            f.close()
        except IOError:
            data = {}
        return data

    def get(self, key):
        """
        Retrieves a certain value for a given key
        """
        data = self._read()
        if key in data:
            return data[key]
        else:
            raise KeyNotFoundException(key)

    def get_multi(self, keys):
        """
        Retrieves values for all given keys
        """
        data = self._read()
        for key in keys:
            if key in data:
                yield data[key]
            else:
                raise KeyNotFoundException(key)

    def prefix(self, key):
        """
        Lists all keys starting with the given prefix
        """
        data = self._read()
        return [k for k in data.keys() if k.startswith(key)]

    def prefix_entries(self, key):
        """
        Returns all key-values starting with the given prefix
        """
        data = self._read()
        return [(k, v) for k, v in data.iteritems() if k.startswith(key)]

    def set(self, key, value, transaction=None):
        """
        Sets the value for a key to a given value
        """
        if transaction is not None:
            return self._sequences[transaction].append([self.set, {'key': key, 'value': value}])
        data = self._read()
        data[key] = value
        self._save(data)

    def delete(self, key, must_exist=True, transaction=None):
        """
        Deletes a given key from the store
        """
        if transaction is not None:
            return self._sequences[transaction].append([self.delete, {'key': key, 'must_exist': must_exist}])
        data = self._read()
        if key in data:
            del data[key]
            self._save(data)
        elif must_exist is True:
            raise KeyNotFoundException(key)

    def exists(self, key):
        """
        Check if key exists
        """
        try:
            self.get(key)
            return True
        except KeyNotFoundException:
            return False

    def nop(self):
        """
        Executes a nop command
        """
        _ = self
        pass

    def assert_value(self, key, value, transaction=None):
        """
        Asserts a key-value pair
        """
        if transaction is not None:
            return self._sequences[transaction].append([self.assert_value, {'key': key, 'value': value}])
        data = self._read()
        if key not in data:
            raise AssertException(key)
        if json.dumps(data[key], sort_keys=True) != json.dumps(value, sort_keys=True):
            raise AssertException(key)

    def assert_exists(self, key, transaction=None):
        """
        Asserts whether a given key exists
        """
        if transaction is not None:
            return self._sequences[transaction].append([self.assert_exists, {'key': key}])
        data = self._read()
        if key not in data:
            raise AssertException(key)

    def begin_transaction(self):
        """
        Creates a transaction (wrapper around Arakoon sequences)
        """
        key = str(uuid.uuid4())
        self._sequences[key] = []
        return key

    def apply_transaction(self, transaction):
        """
        Applies a transaction
        """
        for item in self._sequences[transaction]:
            item[0](**item[1])

    def _save(self, data):
        """
        Saves the local json file
        """
        if self._keep_in_memory_only is True:
            DummyPersistentStore._data = data
        else:
            f = open(self._path, 'w+')
            f.write(json.dumps(data, sort_keys=True, indent=2))
            f.close()
