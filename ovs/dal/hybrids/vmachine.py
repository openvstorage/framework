# license see http://www.openvstorage.com/licenses/opensource/
"""
VMachine module
"""
from ovs.dal.dataobject import DataObject
from ovs.dal.hybrids.pmachine import PMachine


class VMachine(DataObject):
    """
    The VMachine class represents a vMachine. A vMachine is a Virtual Machine with vDisks
    or a Virtual Machine running the Open vStorage software.
    """
    # pylint: disable=line-too-long
    _blueprint = {'name':         (None,  str,  'Name of the vMachine.'),
                  'description':  (None,  str,  'Description of the vMachine.'),
                  'hypervisorid': (None,  str,  'The Identifier of the vMachine on the Hypervisor.'),
                  'is_vtemplate': (True,  bool, 'Indicates whether this vMachine is a vTemplate.'),
                  'is_internal':  (False, bool, 'Indicates whether this vMachine is a Management VM for the Open vStorage Framework.'),
                  'hvtype':       (None,  ['HYPERV', 'VMWARE', 'XEN'], 'Hypervisor type serving the vMachine.')}
    _relations = {'pmachine': (PMachine, 'vmachines')}
    _expiry = {'snapshots':   (60, list),
               'status':      (30, str),
               'statistics':   (5, dict),
               'stored_data': (60, int)}
    # pylint: enable=line-too-long

    def _snapshots(self):
        """
        Fetches a list of Snapshots for the vMachine.
        @return: list
        """
        _ = self
        import time
        import uuid
        first = time.time() - (60 * 60 * 24 * 3)
        second = time.time() - (60 * 60 * 24 * 2)
        third = time.time() - (60 * 60 * 24)
        return [{'timestamp': first,
                 'label': 'My first snapshot',
                 'is_consistent': True,
                 'snapshots': {str(uuid.uuid4()): uuid.uuid4(),
                               str(uuid.uuid4()): uuid.uuid4()}},
                {'timestamp': second,
                 'label': None,
                 'is_consistent': False,
                 'snapshots': {str(uuid.uuid4()): uuid.uuid4(),
                               str(uuid.uuid4()): uuid.uuid4()}},
                {'timestamp': third,
                 'label': 'My Third snapshot',
                 'is_consistent': True,
                 'snapshots': {str(uuid.uuid4()): uuid.uuid4(),
                               str(uuid.uuid4()): uuid.uuid4()}}]

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
