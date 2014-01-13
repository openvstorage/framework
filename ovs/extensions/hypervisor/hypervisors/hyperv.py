# license see http://www.openvstorage.com/licenses/opensource/
"""
Module for the HyperV hypervisor client
"""

from ovs.extensions.hypervisor.hypervisor import Hypervisor


class HyperV(Hypervisor):
    """
    Represents the hypervisor client for HyperV
    """

    def _connect(self):
        """
        Dummy method
        """
        raise NotImplementedError()

    @Hypervisor.connected
    def get_state(self, vmid):
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

    @Hypervisor.connected
    def mount_nfs_datastore(self, name, remote_host, remote_path):
        """
        Dummy method
        """
        raise NotImplementedError()
