from ovs.hypervisor.hypervisor import Hypervisor
import time


class HyperV(Hypervisor):
    def _connect(self):
        print '[HV] connecting to {0} with {1}/{2}'.format(self._ip, self._username, self._password)

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
