# Copyright 2016 iNuron NV
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
StorageRouter module
"""

import time
from ovs.extensions.storageserver.storagedriver import StorageDriverClient
from ovs.dal.dataobject import DataObject
from ovs.dal.structures import Property, Relation, Dynamic
from ovs.dal.hybrids.failuredomain import FailureDomain
from ovs.dal.hybrids.pmachine import PMachine


class StorageRouter(DataObject):
    """
    A StorageRouter represents the Open vStorage software stack, any (v)machine on which it is installed
    """
    __properties = [Property('name', str, doc='Name of the vMachine.'),
                    Property('description', str, mandatory=False, doc='Description of the vMachine.'),
                    Property('machine_id', str, mandatory=False, doc='The hardware identifier of the vMachine'),
                    Property('ip', str, doc='IP Address of the vMachine, if available'),
                    Property('heartbeats', dict, default={}, doc='Heartbeat information of various monitors'),
                    Property('node_type', ['MASTER', 'EXTRA'], default='EXTRA', doc='Indicates the node\'s type'),
                    Property('rdma_capable', bool, doc='Is this StorageRouter RDMA capable'),
                    Property('last_heartbeat', float, mandatory=False, doc='When was the last (external) heartbeat send/received')]
    __relations = [Relation('pmachine', PMachine, 'storagerouters'),
                   Relation('primary_failure_domain', FailureDomain, 'primary_storagerouters'),
                   Relation('secondary_failure_domain', FailureDomain, 'secondary_storagerouters', mandatory=False)]
    __dynamics = [Dynamic('statistics', dict, 4, locked=True),
                  Dynamic('stored_data', int, 60),
                  Dynamic('vmachines_guids', list, 15),
                  Dynamic('vpools_guids', list, 15),
                  Dynamic('vdisks_guids', list, 15),
                  Dynamic('status', str, 10),
                  Dynamic('partition_config', dict, 3600)]

    def _statistics(self, dynamic):
        """
        Aggregates the Statistics (IOPS, Bandwidth, ...) of each vDisk of the vMachine.
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

    def _stored_data(self):
        """
        Aggregates the Stored Data of each vDisk of the vMachine.
        """
        return self.statistics['stored']

    def _vmachines_guids(self):
        """
        Gets the vMachine guids served by this StorageRouter.
        Definition of "served by": vMachine whose disks are served by a given StorageRouter
        """
        from ovs.dal.lists.vdisklist import VDiskList
        vmachine_guids = set()
        for storagedriver in self.storagedrivers:
            storagedriver_client = storagedriver.vpool.storagedriver_client
            for vdisk in VDiskList.get_in_volume_ids(storagedriver_client.list_volumes(str(storagedriver.storagedriver_id))):
                if vdisk.vmachine_guid is not None:
                    vmachine_guids.add(vdisk.vmachine_guid)
        return list(vmachine_guids)

    def _vdisks_guids(self):
        """
        Gets the vDisk guids served by this StorageRouter.
        """
        from ovs.dal.lists.vdisklist import VDiskList
        vdisk_guids = []
        for storagedriver in self.storagedrivers:
            storagedriver_client = storagedriver.vpool.storagedriver_client
            vdisk_guids += VDiskList.get_in_volume_ids(storagedriver_client.list_volumes(str(storagedriver.storagedriver_id))).guids
        return vdisk_guids

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
        current_time = time.time()
        if self.heartbeats is not None:
            process_delay = abs(self.heartbeats.get('process', 0) - current_time)
            if process_delay > 60 * 5:
                return 'FAILURE'
            else:
                delay = abs(self.heartbeats.get('celery', 0) - current_time)
                if delay > 60 * 5:
                    return 'FAILURE'
                elif delay > 60 * 2:
                    return 'WARNING'
                else:
                    return 'OK'
        return 'UNKNOWN'

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
