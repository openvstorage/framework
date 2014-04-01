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
File mutex module
"""
import time
import fcntl
import os
import stat


class FileMutex(object):
    """
    This is mutex backed on the filesystem. It's cross thread and cross process. However
    its limited to the boundaries of a filesystem
    """

    def __init__(self, name):
        """
        Creates a file mutex object
        """
        self.name = name
        self._has_lock = False
        self._start = 0
        self._handle = open(self.key(), 'w')
        try:
            os.chmod(
                self.key(),
                stat.S_IRUSR | stat.S_IWUSR |
                stat.S_IRGRP | stat.S_IWGRP |
                stat.S_IROTH | stat.S_IWOTH
            )
        except OSError:
            pass

    def acquire(self, wait=None):
        """
        Aquire a lock on the mutex, optionally given a maximum wait timeout
        """
        if self._has_lock:
            return True
        self._start = time.time()
        if wait is None:
            fcntl.flock(self._handle, fcntl.LOCK_EX)
            passed = time.time() - self._start
        else:
            passed = time.time() - self._start
            while True:
                passed = time.time() - self._start
                if passed > wait:
                    raise RuntimeError('Could not aquire lock %s' % self.key())
                try:
                    fcntl.flock(self._handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except IOError:
                    time.sleep(0.005)
        if passed > 1:  # More than 1 s is a long time to wait!
            print 'Waited %s seconds for lock %s' % (passed, self.key())
        self._start = time.time()
        self._has_lock = True
        return True

    def release(self):
        """
        Releases the lock
        """
        if self._has_lock:
            fcntl.flock(self._handle, fcntl.LOCK_UN)
            passed = time.time() - self._start
            if passed > 2.5:  # More than 2.5 s is a long time to hold a lock
                print 'A lock on %s was kept for %s seconds' % (self.key(), passed)
            self._has_lock = False

    def key(self):
        """
        Lock key
        """
        return '/var/lock/ovs_flock_%s' % self.name

    def __del__(self):
        """
        __del__ hook, releasing the lock
        """
        self.release()
        self._handle.close()
