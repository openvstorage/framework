# license see http://www.openvstorage.com/licenses/opensource/
"""
Module for the XEN hypervisor client
"""

from ovs.extensions.hypervisor.hypervisor import Hypervisor


class Xen(Hypervisor):
    """
    Represents the hypervisor client for XEN
    """

    def _connect(self):
        """
        Dummy method
        """
        raise NotImplementedError()

    @Hypervisor.connected
    def delete_vm(self, vmid, wait=False):
        """
        Dummy method
        """
        raise NotImplementedError()

    @Hypervisor.connected
    def clone_vm(self, vmid, name, disks, wait=False):
        """
        Dummy method
        """
        raise NotImplementedError()

    @Hypervisor.connected
    def set_as_template(self, vmid, disks, wait=False):
        """
        Dummy method
        """
        raise NotImplementedError()

    @Hypervisor.connected
    def get_vm_object(self, vmid):
        """
        Dummy method
        """
        raise NotImplementedError()

    @Hypervisor.connected
    def get_vm_object_by_devicename(self, devicename, ip, mountpoint):
        """
        Dummy method
        """
        raise NotImplementedError()
