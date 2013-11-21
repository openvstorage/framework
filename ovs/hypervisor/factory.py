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
    @staticmethod
    def get(node):
        """
        Returns the appropriate hypervisor client class for a given VMachine
        """
        hvtype   = node.hvtype
        ip       = node.ip
        username = node.username
        password = node.password
        if hvtype == 'HYPERV':
            return HyperV(ip, username, password)
        if hvtype == 'VMWARE':
            return VMware(ip, username, password)
        if hvtype == 'XEN':
            return Xen(ip, username, password)
        raise Exception('Invalid hypervisor')
