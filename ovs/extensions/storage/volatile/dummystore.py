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
Dummy volatile module
"""
import os
import copy
import json
import time as time_module


class DummyVolatileStore(object):
    """
    This is a dummy volatile store that makes use of a local json file
    """
    _path = '/run/dummyvolatile.json'
    _storage = {}
    _timeout = {}
    _data = {'t': {}, 's': {}}

    def __init__(self):
        """
        Init method
        """
        self._keep_in_memory_only = True

    def _clean(self):
        """
        Empties the store
        """
        if self._keep_in_memory_only is True:
            DummyVolatileStore._data = {'t': {}, 's': {}}
        else:
            try:
                os.remove(DummyVolatileStore._path)
            except OSError:
                pass

    def _read(self):
        """
        Reads the local json file
        """
        if self._keep_in_memory_only is True:
            return DummyVolatileStore._data

        try:
            f = open(self._path, 'r')
            data = json.loads(f.read())
            f.close()
        except IOError:
            data = {'t': {}, 's': {}}
        return data

    def get(self, key, default=None):
        """
        Retrieves a certain value for a given key
        """
        data = self._read()
        if key in data['t'] and data['t'][key] > time_module.time():
            value = data['s'].get(key)
            return copy.deepcopy(value)
        return default

    def gets(self, key, default=None):
        """
        Retrieves a certain value for a given key
        """
        data = self._read()
        if key in data['t'] and data['t'][key] > time_module.time():
            value = data['s'].get(key)
            return copy.deepcopy(value)
        return default

    def set(self, key, value, time=99999999):
        """
        Sets the value for a key to a given value
        """
        data = self._read()
        data['s'][key] = copy.deepcopy(value)
        data['t'][key] = time_module.time() + time
        self._save(data)

    def add(self, key, value, time=99999999):
        """
        Adds a given key to the store, expecting the key does not exists yet
        """
        data = self._read()
        if key not in data['s']:
            self.set(key, value, time)
            return True
        else:
            return False

    def delete(self, key):
        """
        Deletes a given key from the store
        """
        data = self._read()
        if key in data['s']:
            del data['s'][key]
            del data['t'][key]
            self._save(data)

    def incr(self, key, delta=1):
        """
        Increments the value of the key, expecting it exists
        """
        data = self._read()
        if key in data['s']:
            data['s'][key] += delta
            self._save(data)
            return True
        return False

    def _save(self, data):
        """
        Saves the local json file
        """
        if self._keep_in_memory_only is True:
            DummyVolatileStore._data = data
        else:
            rawdata = json.dumps(data, sort_keys=True, indent=2)
            f = open(self._path, 'w+')
            f.write(rawdata)
            f.close()
