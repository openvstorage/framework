from ovs.hypervisor.hypervisor import Hypervisor
import time


class VMware(Hypervisor):
    def _connect(self):
        print '[VMW] connecting to {0} with {1}/{2}'.format(self._ip, self._username, self._password)

    @Hypervisor.connected
    def start(self, vmid):
        print '[VMW] starting machine {0}...'.format(str(vmid))
        time.sleep(3)
        print '[VMW] started machine {0}'.format(str(vmid))

    @Hypervisor.connected
    def stop(self, vmid):
        print '[VMW] stopping machine {0}...'.format(str(vmid))
        time.sleep(3)
        print '[VMW] stopped machine {0}'.format(str(vmid))
