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
    def get_state(self, vmid, **kwargs):
        """
        Dummy method
        """
        return self.sdk.get_power_state(vmid)

    @Hypervisor.connected
    def create_vm_from_template(self, name, source_vm, disks, **kwargs):
        return self.sdk.create_vm_from_template(name, source_vm, disks)

    @Hypervisor.connected
    def delete_vm(self, vmid, **kwargs):
        return self.sdk.delete_vm(vmid)

    @Hypervisor.connected
    def get_vm_agnostic_object(self, vmid, **kwargs):
        return self.sdk.make_agnostic_config(self.sdk.get_vm_object(vmid))

    @Hypervisor.connected
    def get_vms_by_nfs_mountinfo(self):
        """
        Gets a list of agnostic vm objects for a given ip and mountpoint
        """
        for vm in self.sdk.get_vms():
            yield self.sdk.make_agnostic_config(vm)

    @Hypervisor.connected
    def is_datastore_available(self, **kwargs):
        return True

    @Hypervisor.connected
    def clone_vm(self, vmid, name, disks):
        """
        create a clone at vmachine level
        #disks are cloned by VDiskController
        """
        return self.sdk.clone_vm(vmid, name, disks)

    @Hypervisor.connected
    def set_as_template(self, vmid, disks):
        """
        Dummy method
        TODO: Not yet implemented, setting an existing kvm guest as template
        """
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
        TODO
        """
        raise NotImplementedError()

    @Hypervisor.connected
    def mount_nfs_datastore(self, name, remote_host, remote_path):
        """
        Dummy method
        """
        raise NotImplementedError()

