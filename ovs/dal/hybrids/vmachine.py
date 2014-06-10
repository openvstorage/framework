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
from ovs.dal.datalist import DataList
from ovs.dal.dataobjectlist import DataObjectList
from ovs.dal.hybrids.pmachine import PMachine
from ovs.dal.hybrids.vpool import VPool
from ovs.extensions.storageserver.volumestoragerouter import VolumeStorageRouterClient
from ovs.extensions.hypervisor.factory import Factory as hvFactory
import time


class VMachine(DataObject):
    """
    The VMachine class represents a vMachine. A vMachine is a Virtual Machine with vDisks
    or a Virtual Machine running the Open vStorage software.
    """
    # pylint: disable=line-too-long
    _blueprint = {'name':         (None,  str,  'Name of the vMachine.'),
                  'description':  (None,  str,  'Description of the vMachine.'),
                  'hypervisorid': (None,  str,  'The identifier of the vMachine on the Hypervisor.'),
                  'machineid':    (None,  str,  'The hardware identifier of the vMachine'),
                  'devicename':   (None,  str,  'The name of the container file (e.g. the VMX-file) describing the vMachine.'),
                  'is_vtemplate': (False, bool, 'Indicates whether this vMachine is a vTemplate.'),
                  'is_internal':  (False, bool, 'Indicates whether this vMachine is a Management VM for the Open vStorage Framework.'),
                  'ip':           (None,  str,  'IP Address of the vMachine, if available'),
                  'status':       ('OK',  ['OK', 'NOK', 'CREATED', 'SYNC', 'SYNC_NOK'], 'Internal status of the vMachine')}
    _relations = {'pmachine': (PMachine, 'vmachines'),
                  'vpool':    (VPool, 'vmachines')}
    _expiry = {'snapshots':          (60, list),
               'hypervisor_status': (300, str),
               'statistics':          (5, dict),
               'stored_data':        (60, int),
               'failover_mode':      (60, str),
               'vsas_guids':         (15, list),
               'vpools_guids':       (15, list)}
    # pylint: enable=line-too-long

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
        if self.hypervisorid is None or self.pmachine is None:
            return 'UNKNOWN'
        hv = hvFactory.get(self.pmachine)
        try:
            return hv.get_state(self.hypervisorid)
        except:
            return 'UNKNOWN'

    def _statistics(self):
        """
        Aggregates the Statistics (IOPS, Bandwidth, ...) of each vDisk of the vMachine.
        """
        client = VolumeStorageRouterClient()
        vdiskstatsdict = {}
        for key in client.stat_keys:
            vdiskstatsdict[key] = 0
            vdiskstatsdict['%s_ps' % key] = 0
        if self.is_internal:
            vdisks = []
            for vsr in self.served_vsrs:
                for vdisk in vsr.vpool.vdisks:
                    if vdisk.vsrid == vsr.vsrid:
                        vdisks.append(vdisk)
        else:
            vdisks = self.vdisks
        for disk in vdisks:
            statistics = disk._statistics()  # Prevent double caching
            for key, value in statistics.iteritems():
                if key != 'timestamp':
                    vdiskstatsdict[key] += value
        vdiskstatsdict['timestamp'] = time.time()
        return vdiskstatsdict

    def _stored_data(self):
        """
        Aggregates the Stored Data of each vDisk of the vMachine.
        """
        vdisks = self.vdisks
        if self.is_internal:
            vdisks = []
            for vsr in self.served_vsrs:
                for vdisk in vsr.vpool.vdisks:
                    if vdisk.vsrid == vsr.vsrid:
                        vdisks.append(vdisk)
        return sum([disk.info['stored'] for disk in vdisks])

    def _failover_mode(self):
        """
        Gets the aggregated failover mode
        """
        status = 'UNKNOWN'
        status_code = 0
        vdisks = self.vdisks
        if self.is_internal:
            for vsr in self.served_vsrs:
                vdisks += vsr.vpool.vdisks
        for disk in vdisks:
            mode = disk.info['failover_mode']
            current_status_code = VolumeStorageRouterClient.FOC_STATUS[mode.lower()]
            if current_status_code > status_code:
                status = mode
                status_code = current_status_code
        return status

    def _vsas_guids(self):
        """
        Gets the VSA guids linked to this vMachine
        """
        vsa_guids = set()
        from ovs.dal.hybrids.volumestoragerouter import VolumeStorageRouter
        vsr_ids = [vdisk.vsrid for vdisk in self.vdisks if vdisk.vsrid]
        volumestoragerouters = DataList({'object': VolumeStorageRouter,
                                         'data': DataList.select.DESCRIPTOR,
                                         'query': {'type': DataList.where_operator.AND,
                                                   'items': [('vsrid', DataList.operator.IN, vsr_ids)]}}).data  # noqa
        for vsr in DataObjectList(volumestoragerouters, VolumeStorageRouter):
            vsa_guids.add(vsr.serving_vmachine_guid)
        return list(vsa_guids)

    def _vpools_guids(self):
        """
        Gets the vPool guids linked to this vMachine
        """
        vpool_guids = set()
        if self.is_internal:
            for vsr in self.served_vsrs:
                vpool_guids.add(vsr.vpool_guid)
        for vdisk in self.vdisks:
            vpool_guids.add(vdisk.vpool_guid)
        return list(vpool_guids)
