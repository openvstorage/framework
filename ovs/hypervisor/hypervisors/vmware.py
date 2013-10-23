from ovs.celery import celery
from ovs.hypervisor.hypervisor import Hypervisor
from ovs.extensions.hypervisor.vmware import sdk
import time


class VMware(Hypervisor):
    def _connect(self):
        print '[VMW] connecting to {0} with {1}/{2}'.format(self._ip, self._username, self._password)
        self.connection = sdk.SdkConnection().connect(self._ip, self._username, self._password)

    @celery.task(name='ovs.hypervisor.vmware.startVM')
    @Hypervisor.connected
    def start(self, *args, **kwargs):
        print '[VMW] starting machine {0}...'.format(str(vmid))
        time.sleep(3)
        print '[VMW] started machine {0}'.format(str(vmid))

    @celery.task(name='ovs.hypervisor.vmware.stopVM')
    @Hypervisor.connected
    def stop(self, *args, **kwargs):
        print '[VMW] stopping machine {0}...'.format(str(vmid))
        time.sleep(3)
        print '[VMW] stopped machine {0}'.format(str(vmid))

    @celery.task(name='ovs.hypervisor.vmware.createVM')
    @Hypervisor.connected
    def createVM(self, *args, **kwargs):
        """
        Configure the vmachine on the hypervisor
        
        @param vmid
        """

    @celery.task(name='ovs.hypervisor.vmware.deleteVM')
    @Hypervisor.connected
    def delete(self, *args, **kwargs):
        """
        Remove the vmachine from the hypervisor
        
        @param vmid
        """
        self.connection.deleteVM(vmid, wait=False)

    @celery.task(name='ovs.hypervisor.vmware.cloneVM')
    @Hypervisor.connected
    def cloneVM(self, vmid, name, disks, esxHost=None, wait=False):
        """
        Clone a vmachine
        
        """
        print '[VMW] Cloning machine {0} to {1} ...'.format(str(vmid), name)
        self.connection.cloneVM(vmid, name, disks, esxHost, wait)
        print '[VMW] Cloned machine {0} to {1} ...'.format(str(vmid), name)