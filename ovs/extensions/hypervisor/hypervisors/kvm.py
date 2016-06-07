# Copyright (C) 2016 iNuron NV
#
# This file is part of Open vStorage Open Source Edition (OSE),
# as available from
#
#      http://www.openvstorage.org and
#      http://www.openvstorage.com.
#
# This file is free software; you can redistribute it and/or modify it
# under the terms of the GNU Affero General Public License v3 (GNU AGPLv3)
# as published by the Free Software Foundation, in version 3 as it comes
# in the LICENSE.txt file of the Open vStorage OSE distribution.
#
# Open vStorage is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY of any kind.

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

    def create_vm_from_template(self, name, source_vm, disks, ip, mountpoint, wait=True):
        """
        create vm from template
        """
        _ = ip, wait  # For compatibility purposes only
        return self.sdk.create_vm_from_template(name, source_vm, disks, mountpoint)

    def delete_vm(self, vmid, storagedriver_mountpoint=None, storagedriver_storage_ip=None, devicename=None, disks_info=None, wait=True):
        """
        Deletes a given VM and its disks
        """
        _ = wait  # For compatibility purposes only
        _ = storagedriver_mountpoint  # No vpool mountpoint on kvm, use different logic
        _ = storagedriver_storage_ip  # 127.0.0.1 always
        if disks_info is None:
            disks_info = []
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
        return self.sdk.is_datastore_available(mountpoint)

    def clone_vm(self, vmid, name, disks, mountpoint, wait=False):
        """
        create a clone at vmachine level
        #disks are cloned by VDiskController
        """
        _ = wait  # For compatibility purposes only
        return self.sdk.clone_vm(vmid, name, disks, mountpoint)

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
        return self.get_disk_path(machinename, devicename)

    def get_disk_path(self, machinename, devicename):
        """
        Builds the path for the file backing a given device/disk
        """
        _ = self
        if machinename:
            return '/{}_{}.raw'.format(machinename.replace(' ', '_'), devicename)
        return '/{}.raw'.format(devicename)

    def clean_vmachine_filename(self, path):
        """
        Cleans a VM filename
        """
        _ = self
        return path.strip('/')

    def get_vmachine_path(self, machinename, storagerouter_machineid):
        """
        Builds the path for the file representing a given vmachine
        """
        _ = self
        machinename = machinename.replace(' ', '_')
        return '/{}/{}.xml'.format(storagerouter_machineid, machinename)

    def get_rename_scenario(self, old_name, new_name):
        """
        Gets the rename scenario based on the old and new name
        """
        _ = self
        if old_name.endswith('.xml') and new_name.endswith('.xml'):
            return 'RENAME'
        return 'UNSUPPORTED'

    def should_process(self, devicename, machine_ids=None):
        """
        Checks whether a given device should be processed
        """
        _ = self
        valid = devicename.strip('/') not in ['vmcasts/rss.xml']
        if not valid:
            return False
        if machine_ids is not None:
            return any(machine_id for machine_id in machine_ids if devicename.strip('/').startswith(machine_id))
        return True

    def file_exists(self, storagedriver, devicename):
        """
        Check if devicename exists
        """
        _ = storagedriver
        matches = self.sdk.find_devicename(devicename)
        return matches is not None

    def create_volume(self, vpool_mountpoint, storage_ip, diskname, size):
        """
        Create new volume - this is a truncate command
        :param vpool_mountpoint: mountpoint of the vpool
        :param storage_ip: IP of the storagerouter
        :param diskname: name of the disk
        :param size: size in GB
        """
        _ = storage_ip
        disk_path = self.clean_backing_disk_filename(self.get_disk_path(None, diskname))
        location = '/'.join([vpool_mountpoint, disk_path])
        self.sdk.create_volume(location, size)
        return disk_path

    def delete_volume(self, vpool_mountpoint, storage_ip, diskname):
        """
        Delete volume - this is a rm command
        :param vpool_mountpoint: mountpoint of the vpool
        :param storage_ip: IP of the storagerouter
        :param diskname: name of the disk
        """
        _ = storage_ip
        disk_path = self.clean_backing_disk_filename(self.get_disk_path(None, diskname))
        location = '/'.join([vpool_mountpoint, disk_path])
        self.sdk.delete_volume(location)

    def extend_volume(self, vpool_mountpoint, storage_ip, diskname, size):
        """
        Extend volume - this is a truncate command
        :param vpool_mountpoint: mountpoint of the vpool
        :param storage_ip: IP of the storagerouter
        :param diskname: name of the disk
        :param size: size in GB
        """
        _ = storage_ip
        disk_path = self.clean_backing_disk_filename(self.get_disk_path(None, diskname))
        location = '/'.join([vpool_mountpoint, disk_path])
        self.sdk.extend_volume(location, size)
