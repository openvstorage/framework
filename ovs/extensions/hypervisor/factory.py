# license see http://www.openvstorage.com/licenses/opensource/
"""
Hypervisor factory module
"""
from hypervisors.vmware import VMware
from hypervisors.hyperv import HyperV
from hypervisors.xen import Xen
from ovs.extensions.generic.filemutex import FileMutex


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
        key = '{0}_{1}'.format(ip, username)
        if key not in Factory.hypervisors:
            mutex = FileMutex('hypervisor_{0}'.format(key))
            try:
                mutex.acquire(30)
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
            finally:
                mutex.release()
        return Factory.hypervisors[key]
