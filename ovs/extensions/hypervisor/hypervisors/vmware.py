# license see http://www.openvstorage.com/licenses/opensource/
"""
Module for the VMware hypervisor client
"""

from ovs.celery import celery
from ovs.extensions.hypervisor.hypervisor import Hypervisor
from ovs.extensions.hypervisor.apis.vmware.sdk import Sdk


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

    @celery.task(name='ovs.hypervisor.vmware.deleteVM')
    @Hypervisor.connected
    def delete_vm(self, vmid, wait=False):
        """
        Remove the vmachine from the hypervisor

        @param vmid: hypervisor id of the virtual machine
        @param wait: wait for action to complete
        """
        if vmid and self.sdk.exists(key=vmid):
            self.sdk.delete_vm(vmid, wait)

    @celery.task(name='ovs.hypervisor.vmware.cloneVM')
    @Hypervisor.connected
    def clone_vm(self, vmid, name, disks, wait=False):
        """
        Clone a vmachine

        @param vmid: hypvervisor id of the virtual machine
        @param name: name of the virtual machine
        @param disks: list of disk information
        @param wait: wait for action to complete
        """
        task = self.sdk.clone_vm(vmid, name, disks, wait)
        if wait is True:
            if self.sdk.validate_result(task):
                task_info = self.sdk.get_task_info(task)
                return task_info.info.result.value
        return None

    @celery.task(name='ovs.hypervisor.vmware.setAsTemplate')
    @Hypervisor.connected
    def set_as_template(self, vmid, disks, wait=False):
        """
        Configure a vm as template
        This lets the machine exist on the hypervisor but configures all disks as "Independent Non-persistent"

        @param vmid: hypervisor id of the virtual machine
        """
        return self.sdk.set_disk_mode(vmid, disks, 'independent_nonpersistent', wait)

    @Hypervisor.connected
    def get_vm_object(self, vmid):
        """
        Gets the VMware virtual machine object from VMware by its identifier
        """
        return self.sdk.make_agnostic_config(self.sdk.get_vm(vmid))

    @Hypervisor.connected
    def get_vm_object_by_devicename(self, devicename, ip, mountpoint):
        """
        Gets the VMware virtual machine object from VMware by devicename and datastore identifiers
        """
        return self.sdk.make_agnostic_config(self.sdk.get_nfs_datastore_object(ip, mountpoint, devicename)[0])
