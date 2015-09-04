# Copyright 2014 Open vStorage NV
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
Module for the vcenter management center client
"""

from ovs.extensions.hypervisor.apis.vmware.sdk import Sdk
from ovs.log.logHandler import LogHandler

logger = LogHandler.get('extensions', name='mgmtcenter')


class VCenter(object):
    """
    Represents the management center for vcenter server
    """

    def __init__(self, ip, username, password):
        """
        Initializes the object with credentials and connection information
        """
        self.metadata = None
        self.sdk = Sdk(ip, username, password)
        self.STATE_MAPPING = {'poweredOn': 'RUNNING',
                              'poweredOff': 'HALTED',
                              'suspended': 'PAUSED'}

    def get_host_status_by_ip(self, host_ip):
        """
        Return host status from vCenter Server
        Must be connected to vCenter
        """
        return self.STATE_MAPPING.get(self.sdk.get_host_status_by_ip(host_ip), 'UNKNOWN')

    def get_host_status_by_pk(self, pk):
        """
        Return host status from vCenter Server
        Must be connected to vCenter
        """
        return self.STATE_MAPPING.get(self.sdk.get_host_status_by_pk(pk), 'UNKNOWN')

    def get_host_primary_key(self, host_ip):
        """
        Return host status from vCenter Server
        Must be connected to vCenter
        """
        return self.sdk.get_host_primary_key(host_ip)

    def test_connection(self):
        """
        Checks whether this node is a vCenter
         Should always be True (depends on ip)
        Test connection returns True even if connecting to an Esx host directly,
         extra check to make sure sdk points to vCenter
        """
        self.sdk.test_connection()
        return self.sdk._is_vcenter

    def get_hosts(self):
        """
        Gets a list of all hosts/hypervisors
        """
        return self.sdk.get_hosts()

    @staticmethod
    def get_metadata(metadata, parameters):
        """
        Get specific config values:
        - integratemgmt: True/False
        """
        _ = metadata
        _ = parameters
        return {}

    def set_metadata(self, metadata):
        """
        Update local metadata
        """
        self.metadata = metadata

    def configure_vpool(self, vpool_name, ip):
        pass

    def unconfigure_vpool(self, vpool_name, remove_volume_type, ip):
        pass

    def get_guests(self):
        """
        Gets a list of all guests
        Return: dict
        {hypervisor_hostname: [{id: vm_id, name: vm_name}... ] ...}
        """
        return self.sdk.get_all_vms()

    def get_guest_by_guid(self, guid):
        """
        Return guest info by guid
        :param guid: UUID
        Return: dict
        {attr: value}
        """
        vm = self.sdk.get_vm(guid)
        return {'id': vm.obj_identifier.value,
                'name': vm.name}

    def get_vdisk_model_by_devicepath(self, devicepath):
        """
        Return vdisk model info (name)
        :param devicepath: full device path
        :return: dict
        """
        vds = self.sdk.get_all_vdisks()
        for vd in vds:
            if vd['filename'] == devicepath:
                return vd

    def get_vdisk_device_info(self, volumeid):
        """
        This method does not make sense for vCenter as you cannot retrieve a Virtual Disk by uuid
        """
        raise NotImplementedError('Method <get_vdisk_device_info> not implemented for vCenter ManagementCenter')

    def get_vmachine_device_info(self, instanceid):
        """
        Return device info
        """
        return self.sdk.get_vm_device_info(instanceid)

    def get_vm_agnostic_object(self, devicename, ip, mountpoint):
        """
        devicename: clHp75aS65QhsAHy/instance-00000001.xml
        ip: 127.0.0.1
        mountpoint: /mnt/saio

        Return vm agnostic object
        {'backing': {'datastore': '/mnt/saio',
                     'filename': 'clHp75aS65QhsAHy/instance-00000001.xml'},
        'datastores': {'/mnt/saio': '127.0.0.1:/mnt/saio'},
        'disks': [{'backingfilename': 'volume1.raw',
                   'datastore': '/mnt/saio',
                   'filename': 'volume1.raw',
                   'name': 'volume1',
                   'order': 0}],
        'id': '4a607820-202c-496b-b942-591a9a67fe0f',
        'name': 'instance1'}
        """
        return self.sdk.make_agnostic_config(self.sdk.get_nfs_datastore_object(ip, mountpoint, devicename)[0])

    def is_host_configured(self, ip):
        _ = self
        return False

    def configure_host(self, ip):
        pass

    def unconfigure_host(self, ip):
        pass
