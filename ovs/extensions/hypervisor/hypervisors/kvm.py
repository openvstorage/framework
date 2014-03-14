# Copyright 2014 CloudFounders NV
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Module for the KVM hypervisor client
"""

from ovs.extensions.hypervisor.hypervisor import Hypervisor
from ovs.extensions.hypervisor.apis.kvm.sdk import Sdk


class KVM(Hypervisor):
    """
    Represents the hypervisor client for KVM
    """

    def __init__(self, ip, username, password):
        """
        Initializes the object with credentials and connection information
        """
        super(KVM, self).__init__(ip, username, password)
        self.sdk = Sdk(self._ip, self._username, self._password)

    def _connect(self):
        """
        Dummy connect implementation, SDK handles connection internally
        """
        return True

    @Hypervisor.connected
    def get_state(self, vmid):
        """
        Dummy method
        """
        return self.sdk.get_power_state(vmid)

    @Hypervisor.connected
    def create_vm_from_template(self, name, source_vm, disks, storage_ip, mountpoint, wait=True):
        """
        create vm from template
        TODO:
        storage_ip and mountpoint refer to target vsr
        but on kvm vsr.storage_ip is 127.0.0.1
        """
        _ = wait  # For compatibility purposes only
        return self.sdk.create_vm_from_template(name, source_vm, disks)

    @Hypervisor.connected
    def delete_vm(self, vmid, wait=True):
        """
        Deletes a given VM
        """
        _ = wait  # For compatibility purposes only
        return self.sdk.delete_vm(vmid)

    @Hypervisor.connected
    def get_vm_agnostic_object(self, vmid):
        """
        Loads a VM and returns a hypervisor agnostic representation
        """
        return self.sdk.make_agnostic_config(self.sdk.get_vm_object(vmid))

    @Hypervisor.connected
    def get_vms_by_nfs_mountinfo(self, ip, mountpoint):
        """
        Gets a list of agnostic vm objects for a given ip and mountpoint
        """
        _ = ip, mountpoint  # @TODO: These should be used to only fetch the correct vMachines
        for vm in self.sdk.get_vms():
            yield self.sdk.make_agnostic_config(vm)

    @Hypervisor.connected
    def is_datastore_available(self, ip, mountpoint):
        """
        Check whether a given datastore is in use on the hypervisor
        """
        _ = ip, mountpoint  # @TODO: Check whether the mountpoint is available
        return True

    @Hypervisor.connected
    def clone_vm(self, vmid, name, disks, wait=False):
        """
        create a clone at vmachine level
        #disks are cloned by VDiskController
        """
        _ = wait  # For compatibility purposes only
        return self.sdk.clone_vm(vmid, name, disks)

    @Hypervisor.connected
    def set_as_template(self, vmid, disks, wait=False):
        """
        Dummy method
        TODO: Not yet implemented, setting an existing kvm guest as template
        """
        _ = wait  # For compatibility purposes only
        raise NotImplementedError()

    @Hypervisor.connected
    def get_vm_object(self, vmid):
        """
        Dummy method
        """
        return self.sdk.get_vm_object(vmid)

    @Hypervisor.connected
    def get_vm_object_by_devicename(self, devicename, ip, mountpoint):
        """
        devicename = vmachines/template/template.xml # relative to mountpoint
        """
        return self.sdk.make_agnostic_config(self.sdk.get_vm_object_by_filename(devicename))

    @Hypervisor.connected
    def mount_nfs_datastore(self, name, remote_host, remote_path):
        """
        Dummy method
        """
        raise NotImplementedError()

