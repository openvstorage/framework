from ovs.hypervisor.hypervisor import Hypervisor
import time


class HyperV(Hypervisor):
    def _connect(self):
        print '[HV] connecting to {0}'.format(self._ip)

    @Hypervisor.connected
    def start(self, vmid):
        print '[HV] starting machine {0}...'.format(str(vmid))
        time.sleep(3)
        print '[HV] started machine {0}'.format(str(vmid))

    @Hypervisor.connected
    def stop(self, vmid):
        print '[HV] stopping machine {0}...'.format(str(vmid))
        time.sleep(3)
        print '[HV] stopped machine {0}'.format(str(vmid))
