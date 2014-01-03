# license see http://www.openvstorage.com/licenses/opensource/
"""
VMachine module
"""
from ovs.dal.dataobject import DataObject
from ovs.dal.hybrids.pmachine import PMachine
from ovs.extensions.storageserver.volumestoragerouter import VolumeStorageRouterClient
from ovs.extensions.hypervisor.factory import Factory as hvFactory

class VMachine(DataObject):
    """
    The VMachine class represents a vMachine. A vMachine is a Virtual Machine with vDisks
    or a Virtual Machine running the Open vStorage software.
    """
    # pylint: disable=line-too-long
    _blueprint = {'name':         (None,  str,  'Name of the vMachine.'),
                  'description':  (None,  str,  'Description of the vMachine.'),
                  'hypervisorid': (None,  str,  'The Identifier of the vMachine on the Hypervisor.'),
                  'devicename':   (None,  str,  'The name of the container file (e.g. the VMX-file) describing the vMachine.'),
                  'is_vtemplate': (False, bool, 'Indicates whether this vMachine is a vTemplate.'),
                  'is_internal':  (False, bool, 'Indicates whether this vMachine is a Management VM for the Open vStorage Framework.'),
                  'ip':           (None,  str,  'IP Address of the vMachine, if available'),
                  'hvtype':       (None,  ['HYPERV', 'VMWARE', 'XEN'], 'Hypervisor type serving the vMachine.'),
                  'status':       ('OK',  ['OK', 'NOK', 'CREATED', 'SYNC', 'SYNC_NOK'], 'Internal status of the vMachine')}
    _relations = {'pmachine': (PMachine, 'vmachines')}
    _expiry = {'snapshots':          (60, list),
               'hypervisor_status': (300, str, True),  # The cache is invalidated on start/stop
               'statistics':          (4, dict),
               'stored_data':        (60, int),
               'failover_mode':      (60, str)}
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
                                                      'snapshots': {}}
                snapshots_structure[timestamp]['snapshots'][disk.guid] = snapshot['guid']

        snapshots = []
        for timestamp in sorted(snapshots_structure.keys()):
            item = snapshots_structure[timestamp]
            snapshots.append({'timestamp': timestamp,
                              'label': item['label'],
                              'is_consistent': item['is_consistent'],
                              'snapshots': item['snapshots']})
        return snapshots

    def _hypervisor_status(self):
        """
        Fetches the Status of the vMachine.
        """
        if self.hypervisorid is None:
            return 'UNKNOWN'
        hv = hvFactory.get(self.pmachine)
        return hv.get_state(self.hypervisorid)

    def _statistics(self):
        """
        Aggregates the Statistics (IOPS, Bandwidth, ...) of each vDisk of the vMachine.
        """
        data = dict([(key, 0) for key in VolumeStorageRouterClient.STATISTICS_KEYS])
        for disk in self.vdisks:
            statistics = disk.statistics
            for key, value in statistics.iteritems():
                data[key] = data.get(key, 0) + value
        return data

    def _stored_data(self):
        """
        Aggregates the Stored Data of each vDisk of the vMachine.
        """
        return sum([disk.info['stored'] for disk in self.vdisks])

    def _failover_mode(self):
        """
        Gets the aggregated failover mode
        """
        status = 'OK_STANDALONE'
        status_code = 0
        for disk in self.vdisks:
            mode = disk.info['failover_mode']
            current_status_code = VolumeStorageRouterClient.FOC_STATUS[mode.lower()]
            if current_status_code > status_code:
                status = mode
                status_code = current_status_code
        return status
