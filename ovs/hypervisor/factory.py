from hypervisors.vmware import VMware
from hypervisors.hyperv import HyperV
from hypervisors.xen import Xen


class Factory(object):
    @staticmethod
    def get(node):
        hvtype   = node.hvtype
        ip       = node.ip
        username = node.username
        password = node.password
        if hvtype == 'hyperv':
            return HyperV(ip, username, password)
        if hvtype == 'vmware':
            return VMware(ip, username, password)
        if hvtype == 'xen':
            return Xen(ip, username, password)
        raise Exception('Invalid hypervisor')