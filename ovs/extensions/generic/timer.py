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
Module with debugging timer
"""

import time


class timer(object):
    """
    Timer is a context-manager that prints the time it took to execute a piece of code.
    """

    def __init__(self, identification, force_ms=False):
        """
        Initializes the context
        """
        self.start = None
        self.identification = identification
        self.force_ms = force_ms

    def __enter__(self):
        self.start = time.time()

    def __exit__(self, *args):
        duration = time.time() - self.start
        if duration > 2 and self.force_ms is not True:
            print '{0} took {1:.5f}s'.format(self.identification, duration)
        else:
            print '{0} took {1:.5f}ms'.format(self.identification, duration * 1000)
