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
VMachine module
"""
from ovs.dal.dataobject import DataObject
from ovs.dal.structures import Property, Relation, Dynamic
from ovs.dal.datalist import DataList
from ovs.dal.dataobjectlist import DataObjectList
from ovs.dal.hybrids.pmachine import PMachine
from ovs.dal.hybrids.vpool import VPool
from ovs.extensions.storageserver.storagedriver import StorageDriverClient
from ovs.extensions.hypervisor.factory import Factory as hvFactory
import time


class VMachine(DataObject):
    """
    The VMachine class represents a vMachine. A vMachine is a Virtual Machine with vDisks
    or a Virtual Machine running the Open vStorage software.
    """
    __properties = [Property('name', str, mandatory=False, doc='Name of the vMachine.'),
                    Property('description', str, mandatory=False, doc='Description of the vMachine.'),
                    Property('hypervisor_id', str, mandatory=False, doc='The identifier of the vMachine on the Hypervisor.'),
                    Property('devicename', str, doc='The name of the container file (e.g. the VMX-file) describing the vMachine.'),
                    Property('is_vtemplate', bool, default=False, doc='Indicates whether this vMachine is a vTemplate.'),
                    Property('status', ['OK', 'NOK', 'CREATED', 'SYNC', 'SYNC_NOK'], default='OK', doc='Internal status of the vMachine')]
    __relations = [Relation('pmachine', PMachine, 'vmachines'),
                   Relation('vpool', VPool, 'vmachines')]
    __dynamics = [Dynamic('snapshots', list, 60),
                  Dynamic('hypervisor_status', str, 300),
                  Dynamic('statistics', dict, 5),
                  Dynamic('stored_data', int, 60),
                  Dynamic('failover_mode', str, 60),
                  Dynamic('storagerouters_guids', list, 15),
                  Dynamic('vpools_guids', list, 15)]

    def _snapshots(self):
        """
        Fetches a list of Snapshots for the vMachine.
        """
        snapshots_structure = {}
        for disk in self.vdisks:
            for snapshot in disk.snapshots:
                timestamp = snapshot['timestamp']
                if timestamp not in snapshots_structure:
                    snapshots_structure[timestamp] = {'label': snapshot['label'],
                                                      'is_consistent': snapshot['is_consistent'],
                                                      'is_automatic': snapshot.get('is_automatic', True),
                                                      'stored': 0,
                                                      'snapshots': {}}
                snapshots_structure[timestamp]['snapshots'][disk.guid] = snapshot['guid']
                snapshots_structure[timestamp]['stored'] = snapshots_structure[timestamp]['stored'] + snapshot['stored']

        snapshots = []
        for timestamp in sorted(snapshots_structure.keys()):
            item = snapshots_structure[timestamp]
            snapshots.append({'timestamp': timestamp,
                              'label': item['label'],
                              'is_consistent': item['is_consistent'],
                              'is_automatic': item.get('is_automatic', True),
                              'stored': item['stored'],
                              'snapshots': item['snapshots']})
        return snapshots

    def _hypervisor_status(self):
        """
        Fetches the Status of the vMachine.
        """
        if self.hypervisor_id is None or self.pmachine is None:
            return 'UNKNOWN'
        hv = hvFactory.get(self.pmachine)
        try:
            return hv.get_state(self.hypervisor_id)
        except:
            return 'UNKNOWN'

    def _statistics(self):
        """
        Aggregates the Statistics (IOPS, Bandwidth, ...) of each vDisk of the vMachine.
        """
        client = StorageDriverClient()
        vdiskstatsdict = {}
        for key in client.stat_keys:
            vdiskstatsdict[key] = 0
            vdiskstatsdict['{0}_ps'.format(key)] = 0
        for vdisk in self.vdisks:
            vdisk.invalidate_dynamics('statistics')  # Prevent double caching
            for key, value in vdisk.statistics.iteritems():
                if key != 'timestamp':
                    vdiskstatsdict[key] += value
        vdiskstatsdict['timestamp'] = time.time()
        return vdiskstatsdict

    def _stored_data(self):
        """
        Aggregates the Stored Data of each vDisk of the vMachine.
        """
        return sum([vdisk.info['stored'] for vdisk in self.vdisks])

    def _failover_mode(self):
        """
        Gets the aggregated failover mode
        """
        status = 'UNKNOWN'
        status_code = 0
        for vdisk in self.vdisks:
            mode = vdisk.info['failover_mode']
            current_status_code = StorageDriverClient.FOC_STATUS[mode.lower()]
            if current_status_code > status_code:
                status = mode
                status_code = current_status_code
        return status

    def _storagerouters_guids(self):
        """
        Gets the StorageRouter guids linked to this vMachine
        """
        storagerouter_guids = set()
        from ovs.dal.hybrids.storagedriver import StorageDriver
        storagedriver_ids = [vdisk.storagedriver_id for vdisk in self.vdisks if vdisk.storagedriver_id is not None]
        storagedrivers = DataList({'object': StorageDriver,
                                         'data': DataList.select.DESCRIPTOR,
                                         'query': {'type': DataList.where_operator.AND,
                                                   'items': [('storagedriver_id', DataList.operator.IN, storagedriver_ids)]}}).data  # noqa
        for storagedriver in DataObjectList(storagedrivers, StorageDriver):
            storagerouter_guids.add(storagedriver.storagerouter_guid)
        return list(storagerouter_guids)

    def _vpools_guids(self):
        """
        Gets the vPool guids linked to this vMachine
        """
        vpool_guids = set()
        for vdisk in self.vdisks:
            vpool_guids.add(vdisk.vpool_guid)
        return list(vpool_guids)
