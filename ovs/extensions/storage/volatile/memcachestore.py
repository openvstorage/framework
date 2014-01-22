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
from threading import Lock


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
            try:
                self._lock.acquire()
                return f(self, *args, **kw)
            finally:
                self._lock.release()
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
        self._client = memcache.Client(self._nodes, dead_retry=1)
        self._lock = Lock()

    @locked()
    def get(self, key, default=None):
        """
        Retrieves a certain value for a given key
        """
        value = self._client.get(MemcacheStore._clean_key(key))
        return value if value is not None else default

    @locked()
    def set(self, key, value, time=0):
        """
        Sets the value for a key to a given value
        """
        return self._client.set(MemcacheStore._clean_key(key), value, time)

    @locked()
    def add(self, key, value, time=0):
        """
        Adds a given key to the store, expecting the key does not exists yet
        """
        return self._client.add(MemcacheStore._clean_key(key), value, time)

    @locked()
    def incr(self, key, delta=1):
        """
        Increments the value of the key, expecting it exists
        """
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
