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
        self.STATE_MAPPING = {'poweredOn' : 'RUNNING',
                              'poweredOff': 'HALTED',
                              'suspended' : 'PAUSED'}

    def _connect(self):
        """
        Dummy connect implementation, SDK handles connection internally
        """
        return True

    @Hypervisor.connected
    @celery.task(name='ovs.hypervisor.vmware.get_state')
    def get_state(self, vmid):
        """
        Get the current power state of a virtual machine
        @param vmid: hypervisor id of the virtual machine
        """
        return self.STATE_MAPPING[self.sdk.get_power_state(vmid)]

    @celery.task(name='ovs.hypervisor.vmware.create_vm')
    @Hypervisor.connected
    def create_vm(self, *args, **kwargs):
        """
        Configure the vmachine on the hypervisor
        """
        pass

    @celery.task(name='ovs.hypervisor.vmware.create_vm_from_template')
    @Hypervisor.connected
    def create_vm_from_template(self, name, source_vm, disks, esxhost=None, wait=True):
        """
        Create a new vmachine from an existing template
        @param name:
        @param template_vm: template object to create new vmachine from
        @param target_pm: hypervisor object to create new vmachine on
        @return: celery task
        """
        task = self.sdk.create_vm_from_template(name, source_vm, disks, esxhost, wait)
        if wait is True:
            if self.sdk.validate_result(task):
                task_info = self.sdk.get_task_info(task)
                return task_info.info.result.value
        return None

    @celery.task(name='ovs.hypervisor.vmware.clone_vm')
    @Hypervisor.connected
    def clone_vm(self, vmid, name, disks, esxhost=None, wait=False):
        """
        Clone a vmachine

        @param vmid: hypervisor id of the virtual machine
        @param name: name of the virtual machine
        @param disks: list of disk information
        @param esxhost: esx host identifier
        @param wait: wait for action to complete
        """
        task = self.sdk.clone_vm(vmid, name, disks, esxhost, wait)
        if wait is True:
            if self.sdk.validate_result(task):
                task_info = self.sdk.get_task_info(task)
                return task_info.info.result.value
        return None

    @celery.task(name='ovs.hypervisor.vmware.delete_vm')
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

    @Hypervisor.connected
    def get_vm_object(self, vmid):
        """
        Gets the VMware virtual machine object from VMware by its identifier
        """

        return self.sdk.get_vm(vmid)

    @Hypervisor.connected
    def get_vm_agnostic_object(self, vmid):
        """
        Gets the VMware virtual machine object from VMware by its identifier
        """

        return self.sdk.make_agnostic_config(self.sdk.get_vm(vmid))

    @Hypervisor.connected
    def get_vm_object_by_devicename(self, devicename, ip, mountpoint):
        """
        Gets the VMware virtual machine object from VMware by devicename
        and datastore identifiers
        """
        return self.sdk.make_agnostic_config(self.sdk.get_nfs_datastore_object(ip, mountpoint, devicename)[0])

    @Hypervisor.connected
    def is_datastore_available(self, ip, mountpoint, esxhost=None):
        """
        @param ip : hypervisor ip to query for datastore presence
        @param mountpoint: nfs mountpoint on hypervisor
        @rtype: boolean
        @return: True | False
        """

        return self.sdk.is_datastore_available(ip, mountpoint, esxhost)

    @celery.task(name='ovs.hypervisor.vmware.set_as_template')
    @Hypervisor.connected
    def set_as_template(self, vmid, disks, esxhost=None, wait=False):
        """
        Configure a vm as template
        This lets the machine exist on the hypervisor but configures
        all disks as "Independent Non-persistent"

        @param vmid: hypervisor id of the virtual machine
        """
        task = self.sdk.set_disk_mode(
            vmid, disks, 'independent_nonpersistent', esxhost, wait)
