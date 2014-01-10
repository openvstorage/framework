# license see http://www.openvstorage.com/licenses/opensource/
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
