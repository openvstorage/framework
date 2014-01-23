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

