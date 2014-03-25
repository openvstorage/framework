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
Module for the abstract Hypervisor object
"""
import abc


class Hypervisor(object):
    """
    Hypervisor abstract class, providing a mandatory set of methods
    """
    __metaclass__ = abc.ABCMeta

    def __init__(self, ip, username, password):
        """
        Initializes the class, storing an ip, username and password
        """
        self._ip = ip
        self._username = username
        self._password = password
        self._connected = False

    @staticmethod
    def connected(function):
        """
        Decorator method for making sure the client is connected
        """
        def new_function(self, *args, **kwargs):
            """
            Decorator wrapped function
            """
            if not self._connected:
                self._connect()
                self._connected = True
            return function(self, *args, **kwargs)
        return new_function

    @abc.abstractmethod
    def _connect(self):
        """
        Abstract method
        """
        pass

    @abc.abstractmethod
    def get_state(self, vmid):
        """
        Abstract method
        """
        pass

    @abc.abstractmethod
    def create_vm_from_template(self, name, source_vm, disks, ip, mountpoint, wait=True):
        """
        Abstract method
        """
        pass

    @abc.abstractmethod
    def clone_vm(self, vmid, name, disks, wait=False):
        """
        Abstract method
        """
        pass

    @abc.abstractmethod
    def delete_vm(self, vmid, wait=False):
        """
        Abstract method
        """
        pass

    @abc.abstractmethod
    def get_vm_object(self, vmid):
        """
        Abstract method
        """
        pass

    @abc.abstractmethod
    def get_vm_agnostic_object(self, vmid):
        """
        Abstract method
        """
        pass

    @abc.abstractmethod
    def get_vm_object_by_devicename(self, devicename, ip, mountpoint):
        """
        Abstract method
        """
        pass

    @abc.abstractmethod
    def get_vms_by_nfs_mountinfo(self, ip, mountpoint):
        """
        Abstract method
        """
        pass

    @abc.abstractmethod
    def is_datastore_available(self, ip, mountpoint):
        """
        Abstract method
        """
        pass

    @abc.abstractmethod
    def set_as_template(self, vmid, disks, wait=False):
        """
        Abstract method
        """
        pass

    @abc.abstractmethod
    def mount_nfs_datastore(self, name, remote_host, remote_path):
        """
        Mounts a given NFS export as a datastore
        """
        pass

    @abc.abstractmethod
    def clean_backing_disk_filename(self, path):
        """
        Cleans a backing disk filename to the corresponding disk filename
        """
        pass

    @abc.abstractmethod
    def get_backing_disk_path(self, machinename, devicename):
        """
        Builds the path for the file backing a given device/disk
        """
        pass

    @abc.abstractmethod
    def get_disk_path(self, machinename, devicename):
        """
        Builds the path for the file representing a given device/disk
        """
        pass

    @abc.abstractmethod
    def clean_vmachine_filename(self, path):
        """
        Cleans a VM filename
        """
        pass

    @abc.abstractmethod
    def get_vmachine_path(self, machinename, vsa_machineid):
        """
        Builds the path for the file representing a given vmachine
        """
        pass

    @abc.abstractmethod
    def get_rename_scenario(self, old_name, new_name):
        """
        Gets the rename scenario based on the old and new name
        """
        pass

    @abc.abstractmethod
    def should_process(self, devicename):
        """
        Checks whether a given device should be processed
        """
        pass
