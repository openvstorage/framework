from hypervisors.vmware import VMware
from hypervisors.hyperv import HyperV
from hypervisors.xen import Xen


class HVFactory(object):
    @staticmethod
    def get(computenode):
        hvtype   = computenode.hvtype
        ip       = computenode.ip
        username = computenode.username
        password = computenode.password
        if hvtype == 'hyperv':
            return HyperV(ip, username, password)
        if hvtype == 'vmware':
            return VMware(ip, username, password)
        if hvtype == 'xen':
            return Xen(ip, username, password)
        raise Exception('Invalid hypervisor')