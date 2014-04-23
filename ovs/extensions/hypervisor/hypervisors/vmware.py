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
Module for the VMware hypervisor client
"""

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

    def _disconnect(self):
        """
        Dummy connect implementation, SDK handles connection internally
        """
        return True

    def get_host_status(self, host):
        """
        Return host status from vCenter Server
        Must be connected to vCenter
        """
        return self.sdk.get_host_status(host)

    @Hypervisor.connected
    def get_state(self, vmid):
        """
        Get the current power state of a virtual machine
        @param vmid: hypervisor id of the virtual machine
        """
        return self.STATE_MAPPING[self.sdk.get_power_state(vmid)]

    @Hypervisor.connected
    def create_vm_from_template(self, name, source_vm, disks, ip, mountpoint, wait=True):
        """
        Create a new vmachine from an existing template
        @param name:
        @param template_vm: template object to create new vmachine from
        @param target_pm: hypervisor object to create new vmachine on
        @return: celery task
        """
        task = self.sdk.create_vm_from_template(name, source_vm, disks, ip, mountpoint, wait)
        if wait is True:
            if self.sdk.validate_result(task):
                task_info = self.sdk.get_task_info(task)
                return task_info.info.result.value
        return None

    @Hypervisor.connected
    def clone_vm(self, vmid, name, disks, wait=False):
        """
        Clone a vmachine

        @param vmid: hypervisor id of the virtual machine
        @param name: name of the virtual machine
        @param disks: list of disk information
        @param esxhost: esx host identifier
        @param wait: wait for action to complete
        """
        task = self.sdk.clone_vm(vmid, name, disks, wait)
        if wait is True:
            if self.sdk.validate_result(task):
                task_info = self.sdk.get_task_info(task)
                return task_info.info.result.value
        return None

    @Hypervisor.connected
    def delete_vm(self, vmid, wait=False):
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
    def get_vms_by_nfs_mountinfo(self, ip, mountpoint):
        """
        Gets a list of agnostic vm objects for a given ip and mountpoint
        """
        for vm in self.sdk.get_vms(ip, mountpoint):
            yield self.sdk.make_agnostic_config(vm)

    @Hypervisor.connected
    def is_datastore_available(self, ip, mountpoint):
        """
        @param ip : hypervisor ip to query for datastore presence
        @param mountpoint: nfs mountpoint on hypervisor
        @rtype: boolean
        @return: True | False
        """
        return self.sdk.is_datastore_available(ip, mountpoint)

    @Hypervisor.connected
    def set_as_template(self, vmid, disks, wait=False):
        """
        Configure a vm as template
        This lets the machine exist on the hypervisor but configures
        all disks as "Independent Non-persistent"

        @param vmid: hypervisor id of the virtual machine
        """
        return self.sdk.set_disk_mode(vmid, disks, 'independent_nonpersistent', wait)

    @Hypervisor.connected
    def mount_nfs_datastore(self, name, remote_host, remote_path):
        """
        Mounts a given NFS export as a datastore
        """
        return self.sdk.mount_nfs_datastore(name, remote_host, remote_path)

    @Hypervisor.connected
    def is_management_center(self):
        """
        Checks whether this node is a vCenter
        """
        return self.sdk.is_management_center()

    def clean_backing_disk_filename(self, path):
        """
        Cleans a backing disk filename to the corresponding disk filename
        """
        return path.replace('-flat.vmdk', '.vmdk').strip('/')

    def get_backing_disk_path(self, machinename, devicename):
        """
        Builds the path for the file backing a given device/disk
        """
        return '/{}/{}-flat.vmdk'.format(machinename.replace(' ', '_'), devicename)

    def get_disk_path(self, machinename, devicename):
        """
        Builds the path for the file backing a given device/disk
        """
        return '/{}/{}.vmdk'.format(machinename.replace(' ', '_'), devicename)

    def clean_vmachine_filename(self, path):
        """
        Cleans a VM filename
        """
        return path.strip('/')

    def get_vmachine_path(self, machinename, vsa_machineid):
        """
        Builds the path for the file representing a given vmachine
        """
        _ = vsa_machineid  # For compatibility purposes only
        machinename = machinename.replace(' ', '_')
        return '/{}/{}.vmx'.format(machinename, machinename)

    def get_rename_scenario(self, old_name, new_name):
        """
        Gets the rename scenario based on the old and new name
        """
        if old_name.endswith('.vmx') and new_name.endswith('.vmx'):
            return 'RENAME'
        elif old_name.endswith('.vmx~') and new_name.endswith('.vmx'):
            return 'UPDATE'
        return 'UNSUPPORTED'

    def should_process(self, devicename):
        """
        Checks whether a given device should be processed
        """
        _ = devicename
        return True
