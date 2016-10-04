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

import time
from threading import Lock


class Waiter(object):

    def __init__(self, target):
        self._target = target
        self._counter = 0
        self._lock = Lock()

    def wait(self, timeout=5):
        with self._lock:
            self._counter += 1
        start = time.time()
        while self._counter < self._target:
            time.sleep(0.05)
            if time.time() - start > timeout:
                raise RuntimeError('Not all peers were available within {0}s'.format(timeout))
