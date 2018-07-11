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


class GraphiteController():
    """
    Graphite Controller that sends or does not send, depending on the saved config setting
    """

    def __init__(self, database=None):
        celery_scheduling = Configuration.get(key='/ovs/framework/scheduling/celery', default={})
        self._send_statistics = any(celery_scheduling.get(key) is not None for key in ['ovs.stats_monkey.run_all', 'alba.stats_monkey.run_all'])
        if self._send_statistics:
            self._client = GraphiteClient(database=database)

    def send_scrubjob_duration(self, vdisk_guid, start):
        # type: (str, float) -> None
        """
        Fire the duration of the scrubjob to Graphite
        :param vdisk_guid: guid of the scrubbed vdisk
        :type vdisk_guid: str
        :param start: number of jobs
        :type start: int
        :return: None
        :return:
        """
        if self._send_statistics:
            delta = time.time() - start
            self._client.send(path='duration_of_jobs.{0}'.format(vdisk_guid), data=delta)

    def send_scrubjob_worker_units(self, vdisk_guid, nr_of_workers):
        # type: (str, int) -> None
        """
        Fire the number of jobs the scrubjob is divided in to Graphite
        :param vdisk_guid: guid of the scrubbed vdisk
        :type vdisk_guid: str
        :param nr_of_workers: number of jobs
        :type nr_of_workers: int
        :return: None
        """
        if self._send_statistics:
            self._client.send(path='nr_of_worker_units.{0}'.format(vdisk_guid), data=nr_of_workers)

    def send_scrubjob_success(self, vdisk_guid, success):
        # type: (str, int) -> None
        """
        Fire whether the scrubjob succeeded or not
        :param vdisk_guid: guid of the scrubbed vdisk
        :type vdisk_guid: str
        :param success: whether or not the scrubjob succeeded
        :type success: int
        :return: None
        """
        if self._send_statistics:
            self._client.send(path='succeeded.{0}'.format(vdisk_guid), data=1 if success else 0)

    def send_scrubjob_batch_size(self, batch):
        # type: (int) -> None
        """
        Send the scrubjob batch to Graphite
        :param batch: list of VDisks in the scrubjob
        :type batch: list
        :return: None
        """
        if self._send_statistics:
            self._client.send(path='batch_size', data=batch)
