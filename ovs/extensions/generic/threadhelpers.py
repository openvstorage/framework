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
Waiter module
"""

import time
from threading import Lock


class Waiter(object):
    """
    This class allows to synchronize threads
    """

    def __init__(self, target, auto_reset=False):
        """
        Waiter initializer
        :param target: The amount of threads that need to wait before all waits are released
        :type target: int
        :param auto_reset: Whether the counter should be reset automatically after the wait is finished
        :type auto_reset: bool
        """
        self._target = target
        self._counter = 0
        self._lock = Lock()
        self._auto_release_lock = Lock()
        self._auto_reset = auto_reset
        self._auto_release_counter = 0

    def wait(self, timeout=5):
        """
        Wait for synchronization for a max amount of seconds
        :param timeout: The time to wait
        :type timeout: int
        :return: None
        :rtype: NoneType
        """
        with self._lock:
            self._counter += 1
            reached = self._counter == self._target
            if reached is True and self._auto_reset is True:
                while self._auto_release_counter < self._target - 1:
                    time.sleep(0.05)
                self._counter = 0
                with self._auto_release_lock:
                    self._auto_release_counter = 0
        if reached is False:
            start = time.time()
            while self._counter < self._target:
                time.sleep(0.05)
                if time.time() - start > timeout:
                    raise RuntimeError('Not all peers were available within {0}s'.format(timeout))
            with self._auto_release_lock:
                self._auto_release_counter += 1

    def get_counter(self):
        """
        Current counter value
        :return: Counter
        :rtype: int
        """
        return self._counter
