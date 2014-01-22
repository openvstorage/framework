# Copyright 2014 CloudFounders NV
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
Dummy volatile module
"""
import time
import json


class DummyVolatileStore(object):
    """
    This is a dummy volatile store that makes use of a local json file
    """
    _path = '/tmp/dummyvolatile.json'
    _storage = {}
    _timeout = {}

    @staticmethod
    def clean():
        """
        Empties the store
        """
        import os

        try:
            os.remove(DummyVolatileStore._path)
        except OSError:
            pass

    def _read(self):
        """
        Reads the local json file
        """
        try:
            f = open(self._path, 'r')
            data = json.loads(f.read())
            f.close()
        except IOError:
            data = {'t': {}, 's': {}}
        return data

    def get(self, key):
        """
        Retrieves a certain value for a given key
        """
        data = self._read()
        if key in data['t'] and data['t'][key] > time.time():
            value = data['s'].get(key)
            if 'ovs_primarykeys_' in key:
                value = set(value)
            return value
        return None

    def set(self, key, value, timeout=99999999):
        """
        Sets the value for a key to a given value
        """
        if 'ovs_primarykeys_' in key:
            value = list(value)
        data = self._read()
        data['s'][key] = value
        data['t'][key] = time.time() + timeout
        self._save(data)

    def add(self, key, value, timeout=99999999):
        """
        Adds a given key to the store, expecting the key does not exists yet
        """
        data = self._read()
        if key not in data['s']:
            self.set(key, value, timeout)
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
        rawdata = json.dumps(data, sort_keys=True, indent=2)
        f = open(self._path, 'w+')
        f.write(rawdata)
        f.close()
