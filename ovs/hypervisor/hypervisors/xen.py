"""
Module for the XEN hypervisor client
"""

from ovs.hypervisor.hypervisor import Hypervisor
import time


class Xen(Hypervisor):
    """
    Represents the hypervisor client for XEN
    """

    def _connect(self):
        """
        Dummy connect method
        """
        print '[XEN] connecting to {0}'.format(self._ip)

    @Hypervisor.connected
    def start(self, vmid):
        """
        Dummy start method
        """
        print '[XEN] starting machine {0}...'.format(str(vmid))
        time.sleep(3)
        print '[XEN] started machine {0}'.format(str(vmid))

    @Hypervisor.connected
    def stop(self, vmid):
        """
        Dummy stop method
        """
        print '[XEN] stopping machine {0}...'.format(str(vmid))
        time.sleep(3)
        print '[XEN] stopped machine {0}'.format(str(vmid))
