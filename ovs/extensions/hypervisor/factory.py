# license see http://www.openvstorage.com/licenses/opensource/
"""
Hypervisor factory module
"""
from hypervisors.vmware import VMware
from hypervisors.hyperv import HyperV
from hypervisors.xen import Xen


class Factory(object):
    """
    Factory class provides functionality to get abstracted hypervisor
    """

    hypervisors = {}

    @staticmethod
    def get(node):
        """
        Returns the appropriate hypervisor client class for a given VMachine
        """
        hvtype   = node.hvtype
        ip       = node.ip
        username = node.username
        password = node.password
        key = '{0}_{1}_{2}'.format(ip, username, password)
        if key not in Factory.hypervisors:
            if hvtype == 'HYPERV':
                hypervisor = HyperV(ip, username, password)
            elif hvtype == 'VMWARE':
                hypervisor = VMware(ip, username, password)
            elif hvtype == 'XEN':
                hypervisor = Xen(ip, username, password)
            else:
                raise Exception('Invalid hypervisor')
            Factory.hypervisors[key] = hypervisor
        return Factory.hypervisors[key]
