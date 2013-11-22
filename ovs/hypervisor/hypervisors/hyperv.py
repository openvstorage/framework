# license see http://www.openvstorage.com/licenses/opensource/
"""
Module for the HyperV hypervisor client
"""

from ovs.hypervisor.hypervisor import Hypervisor
import time


class HyperV(Hypervisor):
    """
    Represents the hypervisor client for HyperV
    """

    def _connect(self):
        """
        Dummy connect method
        """
        print '[HV] connecting to {0}'.format(self._ip)

    @Hypervisor.connected
    def start(self, vmid):
        """
        Dummy start method
        """
        print '[HV] starting machine {0}...'.format(str(vmid))
        time.sleep(3)
        print '[HV] started machine {0}'.format(str(vmid))

    @Hypervisor.connected
    def stop(self, vmid):
        """
        Dummy stop method
        """
        print '[HV] stopping machine {0}...'.format(str(vmid))
        time.sleep(3)
        print '[HV] stopped machine {0}'.format(str(vmid))
