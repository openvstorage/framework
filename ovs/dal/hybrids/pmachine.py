# license see http://www.openvstorage.com/licenses/opensource/
"""
PMachine module
"""
from ovs.dal.dataobject import DataObject


class PMachine(DataObject):
    """
    The PMachine class represents a pMachine. A pMachine is the physical machine 
	running the Hypervisor.
    """
    # pylint: disable=line-too-long
    _blueprint = {'name':        (None, str, 'Name of the pMachine.'),
                  'description': (None, str, 'Description of the pMachine.'),
                  'username':    (None, str, 'Username of the pMachine.'),
                  'password':    (None, str, 'Password of the pMachine.'),
                  'ip':          (None, str, 'IP address of the pMachine.'),
                  'hvtype':      (None, ['HYPERV', 'VMWARE', 'XEN'], 'Hypervisor type running on the pMachine.')}
    _relations = {}
    _expiry = {}
    # pylint: enable=line-too-long
