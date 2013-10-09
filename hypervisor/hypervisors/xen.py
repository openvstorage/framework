from hypervisor.hypervisor import Hypervisor
import time


class Xen(Hypervisor):
    def _connect(self):
        print '[XEN] connecting to {0} with {1}/{2}'.format(self._ip, self._username, self._password)

    @Hypervisor.connected
    def start(self, vmid):
        print '[XEN] starting machine {0}...'.format(str(vmid))
        time.sleep(3)
        print '[XEN] started machine {0}'.format(str(vmid))

    @Hypervisor.connected
    def stop(self, vmid):
        print '[XEN] stopping machine {0}...'.format(str(vmid))
        time.sleep(3)
        print '[XEN] stopped machine {0}'.format(str(vmid))
