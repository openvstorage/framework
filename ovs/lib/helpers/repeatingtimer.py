# Copyright (C) 2018 iNuron NV
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
Repeated Timer module
"""
from threading import Timer


class RepeatingTimer(Timer):
    """
    RepeatedTimer class. Used to run a function every x seconds
    Does not handle any concurrency: just runs the function on a timeloop
    """
    def __init__(self, interval, func, *args, **kwargs):
        """
        Initialize a new RepeatedTimer
        :param interval: Number of seconds between each execution
        :type interval: float
        :param func: The callback function
        :type func: callable
        :param run_instantly_on_start: Run the passed function instantly on start and wait for it to complete
        :type run_instantly_on_start: bool
        :param args: Arguments to pass on to the function
        :param kwargs: Keyword arguments to pass on to the function
        """
        super(RepeatingTimer, self).__init__(interval, func, *args, **kwargs)

    def run(self):
        while not self.finished.is_set():
            self.finished.wait(self.interval)
            self.function(*self.args, **self.kwargs)

        self.finished.set()
