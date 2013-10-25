import time
from ovs.extensions.storage.memcachefactory import MemcacheFactory


class VolatileMutex(object):
    """
    This is a volatile, distributed mutex to provide cross thread, cross process and cross node locking.
    However, this mutex is volatile and thus can fail. You want to make sure you don't lock for longer than
    a few hundred milliseconds to prevent this.
    """

    def __init__(self, name):
        self._volatile = MemcacheFactory.load()
        self.name = name
        self._has_lock = False
        self._start = 0

    def acquire(self):
        if self._has_lock:
            return True
        self._start = time.time()
        while not self._volatile.add(self.key(), 1):
            pass
        passed = time.time() - self._start
        if passed > 0.025:  # More than 25 ms is a long time to wait!
            print 'Waited %s seconds for lock %s' % (passed, self.key())
        self._has_lock = True
        return True

    def release(self):
        if self._has_lock:
            self._volatile.delete(self.key())
            passed = time.time() - self._start
            if passed > 0.25:  # More than 250 ms is a long time to hold a lock
                print 'A lock on %s was kept for %s seconds' % (self.key(), passed)
            self._has_lock = False

    def key(self):
        return 'ovs_lock_%s' % self.name

    def __del__(self):
        self.release()