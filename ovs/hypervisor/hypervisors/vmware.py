from ovs.celery import celery
from ovs.hypervisor.hypervisor import Hypervisor
from ovs.extensions.hypervisor.vmware import sdk
import time


class VMware(Hypervisor):
    def _connect(self):
        print '[VMW] connecting to {0}'.format(self._ip)
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
        """

    @celery.task(name='ovs.hypervisor.vmware.deleteVM')
    @Hypervisor.connected
    def deleteVM(self, vmid, esxHost=None, wait=False):
        """
        Remove the vmachine from the hypervisor

        @param vmid: hypervisor id of the virtual machine
        @param esxHost: esx host identifier
        @param wait: wait for action to complete
        """
        if vmid and self.connection.exists(key=vmid):
            self.connection.deleteVM(vmid, wait)

    @celery.task(name='ovs.hypervisor.vmware.cloneVM')
    @Hypervisor.connected
    def cloneVM(self, vmid, name, disks, esxHost=None, wait=False, *args, **kwargs):
        """
        Clone a vmachine

        @param vmid: hypvervisor id of the virtual machine
        @param name: name of the virtual machine
        @param disks: list of disk information
        @param esxHost: esx host identifier
        @param wait: wait for action to complete
        """
        print '[VMW] Cloning machine {0} to {1} ...'.format(str(vmid), name)
        task = self.connection.cloneVM(vmid, name, disks, esxHost, wait)
        print '[VMW] Cloned machine {0} to {1} ...'.format(str(vmid), name)
        if wait == True:
            if self.connection.validateResult(task):
                taskInfo = self.connection.getTaskInfo(task)
                return taskInfo.info.result.value
        return None

    @celery.task(name='ovs.hypervisor.vmware.setAsTemplate')
    @Hypervisor.connected
    def setAsTemplate(self, vmid, disks, esxHost=None, wait=False):
        """
        Configure a vm as template
        This lets the machine exist on the hypervisor but configures all disks as "Independent Non-persistent"

        @param vmid: hypervisor id of the virtual machine
        """
        task = self.connection.setDiskMode(vmid, disks, 'independent_nonpersistent', esxHost, wait)
