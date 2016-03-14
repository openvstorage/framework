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
    _keep_in_memory_only = False
    _data = {}

    def __init__(self):
        self._sequences = {}

    @staticmethod
    def clean():
        """
        Empties the store
        """
        if DummyPersistentStore._keep_in_memory_only is True:
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
        if DummyPersistentStore._keep_in_memory_only is True:
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
        Retreives values for all given keys
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

    def delete(self, key, transaction=None):
        """
        Deletes a given key from the store
        """
        if transaction is not None:
            return self._sequences[transaction].append([self.delete, {'key': key}])
        data = self._read()
        if key in data:
            del data[key]
            self._save(data)
        else:
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
        self._save(data)

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
        if DummyPersistentStore._keep_in_memory_only is True:
            DummyPersistentStore._data = data
        else:
            f = open(self._path, 'w+')
            f.write(json.dumps(data, sort_keys=True, indent=2))
            f.close()
