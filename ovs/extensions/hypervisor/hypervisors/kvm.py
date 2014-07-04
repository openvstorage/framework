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

from ovs.extensions.hypervisor.apis.kvm.sdk import Sdk


class KVM(object):
    """
    Represents the hypervisor client for KVM
    """

    def __init__(self, ip, username, password):
        """
        Initializes the object with credentials and connection information
        """
        _ = password
        self.sdk = Sdk(ip, username)

    def get_state(self, vmid):
        """
        Dummy method
        """
        return self.sdk.get_power_state(vmid)

    def create_vm_from_template(self, name, source_vm, disks, storage_ip, mountpoint, wait=True):
        """
        create vm from template
        TODO:
        storage_ip and mountpoint refer to target Storage Router
        but on kvm storagerouter.storage_ip is 127.0.0.1
        """
        _ = storage_ip, wait  # For compatibility purposes only
        return self.sdk.create_vm_from_template(name, source_vm, disks, mountpoint)

    def delete_vm(self, vmid, storagerouter_mountpoint=None, storagerouter_storage_ip=None, devicename=None, disks_info=[], wait=True):
        """
        Deletes a given VM and its disks
        """
        _ = wait  # For compatibility purposes only
        _ = storagerouter_mountpoint  # No vpool mountpoint on kvm, use different logic
        _ = storagerouter_storage_ip  # 127.0.0.1 always
        return self.sdk.delete_vm(vmid, devicename, disks_info)

    def get_vm_agnostic_object(self, vmid):
        """
        Loads a VM and returns a hypervisor agnostic representation
        """
        return self.sdk.make_agnostic_config(self.sdk.get_vm_object(vmid))

    def get_vms_by_nfs_mountinfo(self, ip, mountpoint):
        """
        Gets a list of agnostic vm objects for a given ip and mountpoint
        """
        _ = ip
        vms = []
        for vm in self.sdk.get_vms():
            config = self.sdk.make_agnostic_config(vm)
            if mountpoint in config['datastores']:
                vms.append(config)
        return vms

    def test_connection(self):
        """
        Tests the connection
        """
        return self.sdk.test_connection()

    def is_datastore_available(self, ip, mountpoint):
        """
        Check whether a given datastore is in use on the hypervisor
        """
        _ = ip
        return self.sdk.ssh_run("[ -d {0} ] && echo 'yes' || echo 'no'".format(mountpoint)) == 'yes'

    def clone_vm(self, vmid, name, disks, wait=False):
        """
        create a clone at vmachine level
        #disks are cloned by VDiskController
        """
        _ = wait  # For compatibility purposes only
        return self.sdk.clone_vm(vmid, name, disks)

    def set_as_template(self, vmid, disks, wait=False):
        """
        Dummy method
        TODO: Not yet implemented, setting an existing kvm guest as template
        """
        _ = vmid, disks, wait  # For compatibility purposes only
        raise NotImplementedError()

    def get_vm_object(self, vmid):
        """
        Dummy method
        """
        return self.sdk.get_vm_object(vmid)

    def get_vm_object_by_devicename(self, devicename, ip, mountpoint):
        """
        devicename = vmachines/template/template.xml # relative to mountpoint
        """
        _ = ip, mountpoint
        return self.sdk.make_agnostic_config(self.sdk.get_vm_object_by_filename(devicename))

    def mount_nfs_datastore(self, name, remote_host, remote_path):
        """
        Dummy method
        """
        raise NotImplementedError()

    def clean_backing_disk_filename(self, path):
        """
        Cleans a backing disk filename to the corresponding disk filename
        """
        _ = self
        return path.strip('/')

    def get_backing_disk_path(self, machinename, devicename):
        """
        Builds the path for the file backing a given device/disk
        """
        return self.get_disk_path(machinename.replace(' ', '_'), devicename)

    def get_disk_path(self, machinename, devicename):
        """
        Builds the path for the file backing a given device/disk
        """
        _ = self
        return '/{}_{}.raw'.format(machinename.replace(' ', '_'), devicename)

    def clean_vmachine_filename(self, path):
        """
        Cleans a VM filename
        """
        _ = self
        return path.strip('/')

    def get_vmachine_path(self, machinename, storageappliance_machineid):
        """
        Builds the path for the file representing a given vmachine
        """
        _ = self
        machinename = machinename.replace(' ', '_')
        return '/{}/{}.xml'.format(storageappliance_machineid, machinename)

    def get_rename_scenario(self, old_name, new_name):
        """
        Gets the rename scenario based on the old and new name
        """
        _ = self
        if old_name.endswith('.xml') and new_name.endswith('.xml'):
            return 'RENAME'
        return 'UNSUPPORTED'

    def should_process(self, devicename):
        """
        Checks whether a given device should be processed
        """
        _ = self, devicename
        return devicename.strip('/') not in ['vmcasts/rss.xml']

    def file_exists(self, devicename):
        """
        Checks whether a file (devicename .xml) exists
        """
        return self.sdk.file_exists(devicename)
