# license see http://www.openvstorage.com/licenses/opensource/
"""
VMachine module
"""
from ovs.dal.dataobject import DataObject
from ovs.dal.hybrids.pmachine import PMachine


class VMachine(DataObject):
    """
    A VMachine represents a virtual machine in the model. A virtual machine is
    always served by a hypervisor
    """
    # pylint: disable=line-too-long
    _blueprint = {'name':         (None,  str,  'Name of the virtual machine'),
                  'description':  (None,  str,  'Description of the virtual machine'),
                  'hypervisorid': (None,  str,  'Identifier of the VMachine on the hypervisor'),
                  'is_vtemplate': (True,  bool, 'Indicates whether this virtual machine is a template'),
                  'is_internal':  (False, bool, 'Indicates whether this virtual machine represents an internal machine'),
                  'hvtype':       (None,  ['HYPERV', 'VMWARE', 'XEN'], 'Hypervisor type serving the VMachine')}
    _relations = {'pmachine': (PMachine, 'vmachines')}
    _expiry = {'snapshots':   (60, list),
               'status':      (30, str),
               'statistics':   (5, dict),
               'stored_data': (60, int)}
    # pylint: enable=line-too-long

    def _snapshots(self):
        """
        Fetches a list of snapshots for this virtual machine
        """
        _ = self
        return None

    def _status(self):
        """
        Fetches the status of the volume
        """
        _ = self
        return None

    def _statistics(self):
        """
        Agregates the statistics for this machine
        """
        data = dict()
        for disk in self.vdisks:
            statistics = disk.statistics
            for key, value in statistics.iteritems():
                data[key] = data.get(key, 0) + value
        return data

    def _stored_data(self):
        """
        Agregates the stored data for this vmachine
        """
        return sum([disk.info['stored'] for disk in self.vdisks])
