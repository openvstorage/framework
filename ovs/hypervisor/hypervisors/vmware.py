"""
Module for the VMware hypervisor client
"""

from ovs.celery import celery
from ovs.hypervisor.hypervisor import Hypervisor
from ovs.extensions.hypervisor.vmware.sdk import Sdk
import time


class VMware(Hypervisor):
    """
    Represents the hypervisor client for VMware
    """

    def __init__(self, ip, username, password):
        """
        Initializes the object with credentials and connection information
        """
        super(VMware, self).__init__(ip, username, password)
        self.sdk = Sdk(self._ip, self._username, self._password)

    def _connect(self):
        """
        Dummy connect implementation, since the SDK handles connection internally
        """
        return True

    @celery.task(name='ovs.hypervisor.vmware.startVM')
    @Hypervisor.connected
    def start(self, *args, **kwargs):
        """
        Starts a vm
        """
        vmid = 0
        print '[VMW] starting machine {0}...'.format(str(vmid))
        time.sleep(3)
        print '[VMW] started machine {0}'.format(str(vmid))

    @celery.task(name='ovs.hypervisor.vmware.stopVM')
    @Hypervisor.connected
    def stop(self, *args, **kwargs):
        """
        Stops a vm
        """
        vmid = 0
        print '[VMW] stopping machine {0}...'.format(str(vmid))
        time.sleep(3)
        print '[VMW] stopped machine {0}'.format(str(vmid))

    @celery.task(name='ovs.hypervisor.vmware.createVM')
    @Hypervisor.connected
    def create_vm(self, *args, **kwargs):
        """
        Configure the vmachine on the hypervisor
        """
        pass

    @celery.task(name='ovs.hypervisor.vmware.deleteVM')
    @Hypervisor.connected
    def delete_vm(self, vmid, esxhost=None, wait=False):
        """
        Remove the vmachine from the hypervisor

        @param vmid: hypervisor id of the virtual machine
        @param esxhost: esx host identifier
        @param wait: wait for action to complete
        """
        if vmid and self.sdk.exists(key=vmid):
            self.sdk.delete_vm(vmid, wait)

    @celery.task(name='ovs.hypervisor.vmware.cloneVM')
    @Hypervisor.connected
    def clone_vm(self, vmid, name, disks, esxhost=None, wait=False):
        """
        Clone a vmachine

        @param vmid: hypvervisor id of the virtual machine
        @param name: name of the virtual machine
        @param disks: list of disk information
        @param esxhost: esx host identifier
        @param wait: wait for action to complete
        """
        print '[VMW] Cloning machine {0} to {1} ...'.format(str(vmid), name)
        task = self.sdk.clone_vm(vmid, name, disks, esxhost, wait)
        print '[VMW] Cloned machine {0} to {1} ...'.format(str(vmid), name)
        if wait == True:
            if self.sdk.validate_result(task):
                taskInfo = self.sdk.get_task_info(task)
                return taskInfo.info.result.value
        return None

    @celery.task(name='ovs.hypervisor.vmware.setAsTemplate')
    @Hypervisor.connected
    def set_as_template(self, vmid, disks, esxhost=None, wait=False):
        """
        Configure a vm as template
        This lets the machine exist on the hypervisor but configures all disks as "Independent Non-persistent"

        @param vmid: hypervisor id of the virtual machine
        """
        task = self.sdk.set_disk_mode(vmid, disks, 'independent_nonpersistent', esxhost, wait)
