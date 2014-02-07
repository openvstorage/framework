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
Memcache store module
"""
import memcache
import re
import os

from threading import Lock


def locked():
    """
    Locking decorator.
    """
    def wrap(f):
        """
        Returns a wrapped function
        """
        def new_function(self, *args, **kwargs):
            """
            Executes the decorated function in a locked context
            """
            lock = kwargs.get('lock', True)
            if 'lock' in kwargs:
                del kwargs['lock']
            if lock:
                with self._lock:
                    return f(self, *args, **kwargs)
            else:
                return f(self, *args, **kwargs)
        return new_function
    return wrap


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
        self._client = memcache.Client(self._nodes, dead_retry=1, cache_cas=True)
        self._lock = Lock()
        self._validate = True

    def _get(self, action, key, default=None):
        """
        Retrieves a certain value for a given key (get or gets)
        """
        key = MemcacheStore._clean_key(key)
        if action == 'get':
            data = self._client.get(key)
        else:
            data = self._client.gets(key)
        if data is None:
            # Cache miss
            return default
        if self._validate:
            if data['key'] == key:
                return data['value']
            error = 'Invalid data received'
            os.system("echo '" + error + "' >> /var/log/ovs/memcache.log")
            os.system("echo '" + 'Got key {0}, requested key {1}'.format(data['key'], key) + "' >> /var/log/ovs/memcache.log")
            raise RuntimeError(error)
        else:
            return data

    @locked()
    def get(self, key, default=None):
        """
        Retrieves a certain value for a given key (get)
        """
        return self._get('get', key, default=default)

    @locked()
    def gets(self, key, default=None):
        """
        Retrieves a certain value for a given key (gets)
        """
        return self._get('gets', key, default=default)

    def _set(self, action, key, value, time=0):
        """
        Sets the value for a key to a given value
        """
        key = MemcacheStore._clean_key(key)
        if self._validate:
            data = {'value': value,
                    'key': key}
        else:
            data = value
        if action == 'set':
            return self._client.set(key, data, time)
        return self._client.cas(key, data, time)

    @locked()
    def set(self, key, value, time=0):
        """
        Sets the value for a key to a given value (set)
        """
        return self._set('set', key, value, time=time)

    @locked()
    def cas(self, key, value, time=0):
        """
        Sets the value for a key to a given value (cas)
        """
        return self._set('cas', key, value, time=time)

    @locked()
    def add(self, key, value, time=0):
        """
        Adds a given key to the store, expecting the key does not exists yet
        """
        key = MemcacheStore._clean_key(key)
        if self._validate:
            data = {'value': value,
                    'key': key}
        else:
            data = value
        return self._client.add(key, data, time)

    @locked()
    def incr(self, key, delta=1):
        """
        Increments the value of the key, expecting it exists
        """
        if self._validate:
            value = self.get(key, lock=False)
            if value is not None:
                value += delta
            else:
                value = 1
            self.set(key, value, 60, lock=False)
            return True
        else:
            return self._client.incr(MemcacheStore._clean_key(key), delta)

    @locked()
    def delete(self, key):
        """
        Deletes a given key from the store
        """
        return self._client.delete(MemcacheStore._clean_key(key))

    @staticmethod
    def _clean_key(key):
        return re.sub('[^\x21-\x7e\x80-\xff]', '', str(key))
