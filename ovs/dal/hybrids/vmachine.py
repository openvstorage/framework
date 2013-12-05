# license see http://www.openvstorage.com/licenses/opensource/
"""
VMachine module
"""
from ovs.dal.dataobject import DataObject
from ovs.dal.hybrids.pmachine import PMachine
from ovs.extensions.storageserver.volumestoragerouter import VolumeStorageRouterClient
from collections import OrderedDict

_vsrClient = VolumeStorageRouterClient().load()

import pickle


class VMachine(DataObject):
    """
    The VMachine class represents a vMachine. A vMachine is a Virtual Machine with vDisks
    or a Virtual Machine running the Open vStorage software.
    """
    # pylint: disable=line-too-long
    _blueprint = {'name':         (None, str, 'Name of the vMachine.'),
                  'description':  (None, str, 'Description of the vMachine.'),
                  'hypervisorid': (None, str, 'The Identifier of the vMachine on the Hypervisor.'),
                  'is_vtemplate': (False, bool, 'Indicates whether this vMachine is a vTemplate.'),
                  'is_internal':  (False, bool, 'Indicates whether this vMachine is a Management VM for the Open vStorage Framework.'),
                  'ip_address':   (None,  str, 'IP Address of the vMachine, if available'),
                  'hvtype':       (None, ['HYPERV', 'VMWARE', 'XEN'], 'Hypervisor type serving the vMachine.')}
    _relations = {'pmachine': (PMachine, 'vmachines')}
    _expiry = {'snapshots':     (60, list),
               'status':        (30, str),
               'statistics':     (5, dict),
               'stored_data':   (60, int),
               'failover_mode': (60, str)}
    # pylint: enable=line-too-long

    def _snapshots(self):
        """
        Fetches a list of Snapshots for the vMachine.
        @return: list
        """
        snapshots = list()
        _tmp_snapshots = OrderedDict()
        for disk in self.vdisks:
            for guid in disk.snapshots:
                snapshot = _vsrClient.info_snapshot(str(disk.volumeid), guid)
                metadata = pickle.loads(snapshot.metadata)
                timestamp = metadata['timestamp']
                if timestamp in _tmp_snapshots:
                    _tmp_snapshots[timestamp]['snapshots'][disk.guid] = guid
                else:
                    snapshot_default = {'label' : metadata['label'],
                                        'is_consistent' : metadata['is_consistent'],
                                        'snapshots' : dict()
                                        }
                    snapshot_default['snapshots'][disk.guid] = guid
                    _tmp_snapshots[timestamp] = snapshot_default
        OrderedDict(sorted(_tmp_snapshots.items(), key=lambda k: k[0]))
        for k, v in _tmp_snapshots.iteritems():
            entry = dict()
            entry['timestamp'] = k
            entry['label'] = v['label']
            entry['is_consistent'] = v['is_consistent']
            entry['snapshots'] = v['snapshots']
            snapshots.append(entry)

        return snapshots

    def _status(self):
        """
        Fetches the Status of the vMachine.
        @return: dict
        """
        _ = self
        return None

    def _statistics(self):
        """
        Aggregates the Statistics (IOPS, Bandwidth, ...) of each vDisk of the vMachine.
        @return: dict
        """
        data = dict()
        for disk in self.vdisks:
            statistics = disk.statistics
            for key, value in statistics.iteritems():
                data[key] = data.get(key, 0) + value
        return data

    def _stored_data(self):
        """
        Aggregates the Stored Data of each vDisk of the vMachine.
        @return: int
        """
        return sum([disk.info['stored'] for disk in self.vdisks])

    def _failover_mode(self):
        """
        Gets the aggregated failover mode
        """
        status = None
        for disk in self.vdisks:
            if status is None or 'OK' not in disk.info['failover_mode']:
                status = disk.info['failover_mode']
        return status
