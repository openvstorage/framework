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
Arakoon store module
"""

import json
from threading import Lock

from ovs.extensions.db.arakoon.ArakoonManagement import ArakoonManagement
from ovs.extensions.db.arakoon.ArakoonExceptions import ArakoonNotFound
from ovs.extensions.storage.exceptions import KeyNotFoundException


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
        self._cluster = ArakoonManagement().getCluster(cluster)
        self._client = self._cluster.getClient()
        self._lock = Lock()

    @locked()
    def get(self, key):
        """
        Retrieves a certain value for a given key
        """
        try:
            return json.loads(self._client.get(key))
        except ValueError:
            raise KeyNotFoundException('Could not parse JSON stored for {0}'.format(key))
        except ArakoonNotFound as field:
            raise KeyNotFoundException(field)

    @locked()
    def set(self, key, value):
        """
        Sets the value for a key to a given value
        """
        return self._client.set(key, json.dumps(value))

    @locked()
    def prefix(self, prefix):
        """
        Lists all keys starting with the given prefix
        """
        return self._client.prefix(prefix)

    @locked()
    def delete(self, key):
        """
        Deletes a given key from the store
        """
        try:
            return self._client.delete(key)
        except ArakoonNotFound as field:
            raise KeyNotFoundException(field)
