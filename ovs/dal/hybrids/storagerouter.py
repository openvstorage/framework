# Copyright 2014 CloudFounders NV
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
from ovs.dal.hybrids.pmachine import PMachine


class StorageRouter(DataObject):
    """
    A StorageRouter represents the Open vStorage software stack, any (v)machine on which it is installed
    """
    __properties = [Property('name', str, doc='Name of the vMachine.'),
                    Property('description', str, mandatory=False, doc='Description of the vMachine.'),
                    Property('machine_id', str, mandatory=False, doc='The hardware identifier of the vMachine'),
                    Property('ip', str, doc='IP Address of the vMachine, if available'),
                    Property('heartbeats', dict, default={}, doc='Heartbeat information of various monitors')]
    __relations = [Relation('pmachine', PMachine, 'storagerouters')]
    __dynamics = [Dynamic('statistics', dict, 0),
                  Dynamic('stored_data', int, 60),
                  Dynamic('failover_mode', str, 60),
                  Dynamic('vmachines_guids', list, 15),
                  Dynamic('vpools_guids', list, 15),
                  Dynamic('vdisks_guids', list, 15),
                  Dynamic('status', str, 10)]

    def _statistics(self):
        """
        Aggregates the Statistics (IOPS, Bandwidth, ...) of each vDisk of the vMachine.
        """
        client = StorageDriverClient()
        vdiskstatsdict = {}
        for key in client.stat_keys:
            vdiskstatsdict[key] = 0
            vdiskstatsdict['{0}_ps'.format(key)] = 0
        for storagedriver in self.storagedrivers:
            for vdisk in storagedriver.vpool.vdisks:
                if vdisk.storagedriver_id == storagedriver.storagedriver_id:
                    for key, value in vdisk.statistics.iteritems():
                        if key != 'timestamp':
                            vdiskstatsdict[key] += value
        vdiskstatsdict['timestamp'] = time.time()
        return vdiskstatsdict

    def _stored_data(self):
        """
        Aggregates the Stored Data of each vDisk of the vMachine.
        """
        data = 0
        for storagedriver in self.storagedrivers:
            for vdisk in storagedriver.vpool.vdisks:
                if vdisk.storagedriver_id == storagedriver.storagedriver_id:
                    data += vdisk.info['stored']
        return data

    def _failover_mode(self):
        """
        Gets the aggregated failover mode
        """
        status = 'UNKNOWN'
        status_code = 0
        for storagedriver in self.storagedrivers:
            for vdisk in storagedriver.vpool.vdisks:
                if vdisk.storagedriver_id == storagedriver.storagedriver_id:
                    mode = vdisk.info['failover_mode']
                    current_status_code = StorageDriverClient.FOC_STATUS[mode.lower()]
                    if current_status_code > status_code:
                        status = mode
                        status_code = current_status_code
        return status

    def _vmachines_guids(self):
        """
        Gets the vMachine guids served by this StorageRouter.
        Definition of "served by": vMachine whose disks are served by a given StorageRouter
        """
        vmachine_guids = set()
        for storagedriver in self.storagedrivers:
            for vdisk in storagedriver.vpool.vdisks:
                if vdisk.storagedriver_id == storagedriver.storagedriver_id:
                    if vdisk.vmachine_guid is not None:
                        vmachine_guids.add(vdisk.vmachine_guid)
        return list(vmachine_guids)

    def _vdisks_guids(self):
        """
        Gets the vDisk guids served by this StorageRouter.
        """
        vdisk_guids = []
        for storagedriver in self.storagedrivers:
            for vdisk in storagedriver.vpool.vdisks:
                if vdisk.storagedriver_id == storagedriver.storagedriver_id:
                    vdisk_guids.append(vdisk.guid)
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
