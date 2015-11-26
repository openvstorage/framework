# Copyright 2014 iNuron NV
#
# Licensed under the Open vStorage Modified Apache License (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.openvstorage.org/license
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
                   Relation('vpool', VPool, 'vmachines', mandatory=False)]
    __dynamics = [Dynamic('snapshots', list, 60),
                  Dynamic('hypervisor_status', str, 300),
                  Dynamic('statistics', dict, 4, locked=True),
                  Dynamic('stored_data', int, 60),
                  Dynamic('dtl_mode', str, 60),
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
                                                      'in_backend': snapshot['in_backend'],
                                                      'snapshots': {}}
                snapshots_structure[timestamp]['snapshots'][disk.guid] = snapshot['guid']
                snapshots_structure[timestamp]['stored'] = snapshots_structure[timestamp]['stored'] + snapshot['stored']
                snapshots_structure[timestamp]['in_backend'] &= snapshot['in_backend']

        snapshots = []
        for timestamp in sorted(snapshots_structure.keys()):
            item = snapshots_structure[timestamp]
            snapshots.append({'timestamp': timestamp,
                              'label': item['label'],
                              'is_consistent': item['is_consistent'],
                              'is_automatic': item.get('is_automatic', True),
                              'stored': item['stored'],
                              'in_backend': item['in_backend'],
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

    def _statistics(self, dynamic):
        """
        Aggregates the Statistics (IOPS, Bandwidth, ...) of each vDisk of the vMachine.
        """
        from ovs.dal.hybrids.vdisk import VDisk
        statistics = {}
        for key in StorageDriverClient.STAT_KEYS:
            statistics[key] = 0
            statistics['{0}_ps'.format(key)] = 0
        for vdisk in self.vdisks:
            for key, value in vdisk.fetch_statistics().iteritems():
                statistics[key] += value
        statistics['timestamp'] = time.time()
        VDisk.calculate_delta(self._key, dynamic, statistics)
        return statistics

    def _stored_data(self):
        """
        Aggregates the Stored Data of each vDisk of the vMachine.
        """
        return sum([vdisk.info['stored'] for vdisk in self.vdisks])

    def _dtl_mode(self):
        """
        Gets the aggregated DTL mode
        """
        status = 'UNKNOWN'
        status_code = 0
        for vdisk in self.vdisks:
            mode = vdisk.info['failover_mode']
            current_status_code = StorageDriverClient.DTL_STATUS[mode.lower()]
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
                                   'data': DataList.select.GUIDS,
                                   'query': {'type': DataList.where_operator.AND,
                                             'items': [('storagedriver_id', DataList.operator.IN, storagedriver_ids)]}}).data
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
