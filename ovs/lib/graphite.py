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

import time
from ovs.extensions.generic.graphiteclient import GraphiteClient
from ovs.extensions.generic.configuration import Configuration


class GraphiteController(GraphiteClient):
    """
    Graphite Controller that sends or does not send, depending on the saved config setting
    """

    _send_statistics = Configuration.get('/ovs/framework/support|fwk_statistics', default=False)

    @staticmethod
    def fire_duration(start):
        # type: (float) -> None
        """
        Fire the duration of the scrubjob to Graphite
        :param start: starttime in seconds
        :type start: float
        :return:
        """
        if GraphiteController._send_statistics:
            delta = time.time() - start
            GraphiteClient().send(path='scrubber.duration_jobs', data=delta)

    @staticmethod
    def fire_number(nr_of_jobs):
        # type: (int) -> None
        """
        Fire the number of jobs the scrubjob is divided in to Graphite
        :param nr_of_jobs: number of jobs
        :type nr_of_jobs: int
        :return: None
        """
        if GraphiteController._send_statistics:
            GraphiteClient().send(path='scrubber.nr_of_jobs', data=nr_of_jobs)

    @staticmethod
    def fire_nsm_capacity(capacity):
        # type: (int) -> None
        """
        Fire the current NSM capacity to Graphite
        :param capacity: current capacity
        :type capacity: int
        :return:
        """
        if GraphiteController._send_statistics:
            GraphiteClient().send(path='nsm.capacity', data=capacity)