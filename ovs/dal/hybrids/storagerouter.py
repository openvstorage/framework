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
StorageRouter module
"""

import time
from ovs.dal.dataobject import DataObject
from ovs.dal.structures import Dynamic, Property
from ovs.extensions.storageserver.storagedriver import StorageDriverClient


class StorageRouter(DataObject):
    """
    A StorageRouter represents the Open vStorage software stack, any (v)machine on which it is installed
    """
    __properties = [Property('name', str, doc='Name of the Storage Router.'),
                    Property('description', str, mandatory=False, doc='Description of the Storage Router.'),
                    Property('machine_id', str, unique=True, mandatory=False, doc='The hardware identifier of the Storage Router'),
                    Property('ip', str, unique=True, doc='IP Address of the Storage Router, if available'),
                    Property('heartbeats', dict, default={}, doc='Heartbeat information of various monitors'),
                    Property('node_type', ['MASTER', 'EXTRA'], default='EXTRA', doc='Indicates the node\'s type'),
                    Property('rdma_capable', bool, doc='Is this Storage Router RDMA capable'),
                    Property('last_heartbeat', float, mandatory=False, doc='When was the last (external) heartbeat send/received')]
    __relations = []
    __dynamics = [Dynamic('statistics', dict, 4),
                  Dynamic('vpools_guids', list, 15),
                  Dynamic('vdisks_guids', list, 15),
                  Dynamic('status', str, 10),
                  Dynamic('partition_config', dict, 3600),
                  Dynamic('regular_domains', list, 60),
                  Dynamic('recovery_domains', list, 60)]

    def _statistics(self, dynamic):
        """
        Aggregates the Statistics (IOPS, Bandwidth, ...) of each vDisk.
        """
        from ovs.dal.hybrids.vdisk import VDisk
        statistics = {}
        for key in StorageDriverClient.STAT_KEYS:
            statistics[key] = 0
            statistics['{0}_ps'.format(key)] = 0
        for storagedriver in self.storagedrivers:
            for key, value in storagedriver.fetch_statistics().iteritems():
                statistics[key] += value
        statistics['timestamp'] = time.time()
        VDisk.calculate_delta(self._key, dynamic, statistics)
        return statistics

    def _vdisks_guids(self):
        """
        Gets the vDisk guids served by this StorageRouter.
        """
        from ovs.dal.lists.vdisklist import VDiskList
        volume_ids = []
        vpools = set()
        storagedriver_ids = []
        for storagedriver in self.storagedrivers:
            vpools.add(storagedriver.vpool)
            storagedriver_ids.append(storagedriver.storagedriver_id)
        for vpool in vpools:
            for entry in vpool.objectregistry_client.get_all_registrations():
                if entry.node_id() in storagedriver_ids:
                    volume_ids.append(entry.object_id())
        return VDiskList.get_in_volume_ids(volume_ids).guids

    def _vpools_guids(self):
        """
        Gets the vPool guids linked to this StorageRouter (trough StorageDriver)
        """
        vpool_guids = set()
        for storagedriver in self.storagedrivers:
            vpool_guids.add(storagedriver.vpool_guid)
        return list(vpool_guids)

    def _status(self):
        """
        Calculates the current Storage Router status based on various heartbeats
        """
        pointer = 0
        statusses = ['OK', 'WARNING', 'FAILURE']
        current_time = time.time()
        if self.heartbeats is not None:
            process_delay = abs(self.heartbeats.get('process', 0) - current_time)
            if process_delay > 60 * 5:
                pointer = max(pointer, 2)
            else:
                delay = abs(self.heartbeats.get('celery', 0) - current_time)
                if delay > 60 * 5:
                    pointer = max(pointer, 2)
                elif delay > 60 * 2:
                    pointer = max(pointer, 1)
        for disk in self.disks:
            if disk.state == 'MISSING':
                pointer = max(pointer, 2)
            for partition in disk.partitions:
                if partition.state == 'MISSING':
                    pointer = max(pointer, 2)
        return statusses[pointer]

    def _partition_config(self):
        """
        Returns a dict with all partition information of a given storagerouter
        """
        from ovs.dal.hybrids.diskpartition import DiskPartition
        dataset = dict((role, []) for role in DiskPartition.ROLES)
        for disk in self.disks:
            for partition in disk.partitions:
                for role in partition.roles:
                    dataset[role].append(partition.guid)
        return dataset

    def _regular_domains(self):
        """
        Returns a list of domain guids with backup flag False
        :return: List of domain guids
        """
        return [junction.domain_guid for junction in self.domains if junction.backup is False]

    def _recovery_domains(self):
        """
        Returns a list of domain guids with backup flag True
        :return: List of domain guids
        """
        return [junction.domain_guid for junction in self.domains if junction.backup is True]
