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
Volatile mutex module
"""
import time
from ovs.extensions.storage.volatilefactory import VolatileFactory


class VolatileMutex(object):
    """
    This is a volatile, distributed mutex to provide cross thread, cross process and cross node
    locking. However, this mutex is volatile and thus can fail. You want to make sure you don't
    lock for longer than a few hundred milliseconds to prevent this.
    """

    def __init__(self, name):
        """
        Creates a volatile mutex object
        """
        self._volatile = VolatileFactory.get_client()
        self.name = name
        self._has_lock = False
        self._start = 0

    def acquire(self, wait=None):
        """
        Aquire a lock on the mutex, optionally given a maximum wait timeout
        """
        if self._has_lock:
            return True
        self._start = time.time()
        while not self._volatile.add(self.key(), 1, 60):
            time.sleep(0.005)
            passed = time.time() - self._start
            if wait is not None and passed > wait:
                raise RuntimeError('Could not aquire lock %s' % self.key())
        passed = time.time() - self._start
        if passed > 0.025:  # More than 25 ms is a long time to wait!
            print 'Waited %s seconds for lock %s' % (passed, self.key())
        self._start = time.time()
        self._has_lock = True
        return True

    def release(self):
        """
        Releases the lock
        """
        if self._has_lock:
            self._volatile.delete(self.key())
            passed = time.time() - self._start
            if passed > 0.25:  # More than 250 ms is a long time to hold a lock
                print 'A lock on %s was kept for %s seconds' % (self.key(), passed)
            self._has_lock = False

    def key(self):
        """
        Lock key
        """
        return 'ovs_lock_%s' % self.name

    def __del__(self):
        """
        __del__ hook, releasing the lock
        """
        self.release()
