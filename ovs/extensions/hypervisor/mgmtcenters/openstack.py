# Copyright 2014 iNuron NV
#
# Licensed under the Open vStorage Non-Commercial License, Version 1.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.openvstorage.org/OVS_NON_COMMERCIAL
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Module for the OpenStack Controller API
"""
import os
from ovs.log.logHandler import LogHandler
from ovs.extensions.hypervisor.mgmtcenters.management.openstack_mgmt import OpenStackManagement

logger = LogHandler.get('extensions', name='openstack_mgmt')


class OpenStack(object):
    """
    Represents the management center for OpenStack
    """

    def __init__(self, ip, username, password):
        """
        Initializes the object with credentials and connection information
        Requires novaclient library to be installed on the node this code is executed
        Uses v2 api in Kilo/Liberty (v1 is deprecated in Kilo/Liberty)
        Uses v1_1 api in Juno
        """
        try:
            from novaclient.v2 import client as nova_client
        except ImportError:
            from novaclient.v1_1 import client as nova_client

        try:
            from cinderclient.v2 import client as cinder_client
        except ImportError:
            from cinderclient.v1 import client as cinder_client
        from novaclient import exceptions
        self._novaclientexceptions = exceptions
        self.nova_client = nova_client.Client(username = username,
                                              api_key = password,
                                              project_id = 'admin',
                                              auth_url = 'http://{0}:35357/v2.0'.format(ip),
                                              service_type="compute")
        self.cinder_client = cinder_client.Client(username = username,
                                                  api_key = password,
                                                  project_id = 'admin',
                                                  auth_url = 'http://{0}:35357/v2.0'.format(ip),
                                                  service_type="volumev2")
        self.management = OpenStackManagement(cinder_client = self.cinder_client)
        self.STATE_MAPPING = {'up': 'RUNNING'}

        logger.debug('Init complete')

    def configure_vpool_for_host(self, vpool_guid, ip):
        try:
            return self.management.configure_vpool_for_host(vpool_guid, ip)
        except (SystemExit, Exception) as ex:
            logger.error('Management action "configure_vpool_for_host" failed {0}'.format(ex))
            raise ex

    def unconfigure_vpool_for_host(self, vpool_guid, remove_volume_type, ip):
        try:
            return self.management.unconfigure_vpool_for_host(vpool_guid, remove_volume_type, ip)
        except (SystemExit, Exception) as ex:
            logger.error('Management action "unconfigure_vpool_for_host" failed {0}'.format(ex))
            raise ex

    def configure_host(self, ip):
        try:
            return self.management.configure_host(ip)
        except (SystemExit, Exception) as ex:
            logger.error('Management action "configure_host" failed {0}'.format(ex))
            raise ex

    def unconfigure_host(self, ip):
        try:
            return self.management.unconfigure_host(ip)
        except (SystemExit, Exception) as ex:
            logger.error('Management action "unconfigure_host" failed {0}'.format(ex))
            raise ex

    def get_host_status_by_ip(self, host_ip):
        """
        Return host status
        """
        host_id = self.get_host_primary_key(host_ip)
        host = self.nova_client.hypervisors.get(host_id)
        return self.STATE_MAPPING.get(host.state, 'UNKNOWN')

    def get_host_status_by_pk(self, pk):
        """
        Return host status
        """
        host = self.nova_client.hypervisors.get(pk)
        return self.STATE_MAPPING.get(host.state, 'UNKNOWN')

    def get_host_primary_key(self, host_ip):
        """
        Get hypervisor id based on host_ip
        """
        hosts = [hv for hv in self.nova_client.hypervisors.list() if hv.host_ip == host_ip]
        if not hosts:
            raise RuntimeError('Host with ip {0} not found in datacenter info'.format(host_ip))
        return hosts[0].id

    def test_connection(self):
        """
        Test connection
        """
        try:
            self.nova_client.authenticate()
            return True
        except:
            return False

    def get_hosts(self):
        """
        Gets a list of all hosts/hypervisors
        Expected output: dict
        {host-10: {'ips': [10.130.10.251, 172.22.1.2], 'name': 10.130.10.251},

        """
        hosts = {}
        hvs = self.nova_client.hypervisors.list()  # We are interested in compute nodes
        for hv in hvs:
            hosts[hv.hypervisor_hostname] = {'ips': [hv.host_ip],
                                             'name': hv.hypervisor_hostname}
        return hosts

    def get_guests(self):
        """
        Gets a list of all guests
        Return: dict
        {hypervisor_hostname: [{id: vm_id, name: vm_name}... ] ...}
        """
        hosts = {}
        guests = self.nova_client.servers.list()
        for guest in guests:
            hostname = getattr(guest, 'OS-EXT-SRV-ATTR:hypervisor_hostname', 'N/A')
            hosts.setdefault(hostname, [])
            hosts[hostname].append({'id': guest.id,
                                    'name': guest.name,
                                    'instance_name': getattr(guest, 'OS-EXT-SRV-ATTR:instance_name')})
        return hosts

    def get_guest_by_guid(self, guid):
        """
        Return guest info by guid
        :param guid: UUID
        Return: dict
        {attr: value}
        """
        try:
            guest_object = self.nova_client.servers.get(guid)
        except self._novaclientexceptions.NotFound:
            raise RuntimeError('Guest with guid {0} not found'.format(guid))
        else:
            return {'id': guest_object.id,
                    'name': guest_object.name}

    def _get_vmachine_vdisks(self, guest_object):
        """
        Return dict: list of volume info
        """
        disks = []
        attachments = getattr(guest_object, 'os-extended-volumes:volumes_attached')
        for attachment in attachments:
            volume = self.cinder_client.volumes.get(attachment['id'])
            connection_info = volume.initialize_connection(None, {})
            device_path = connection_info['data']['device_path']
            file_name = os.path.basename(device_path)
            dir_name = os.path.dirname(device_path)
            disks.append({'name': volume.name,
                          'device_path': device_path,
                          'file_name': file_name,
                          'dir_name': dir_name,
                          'vpool_name': connection_info['data']['vpoolname']})
        return disks

    def get_vdisk_model_by_devicepath(self, devicepath):
        """
        Return vdisk model info (name)
        :param devicepath: full device path
        :return: dict
        """
        for volume in self.cinder_client.volumes.list():
            info = volume.initialize_connection(None, {})
            if info['data']['device_path'] == devicepath:
                return {'name': volume.name,
                        'id': volume.id}

    def get_vdisk_device_info(self, volumeid):
        """
        Returns devicename (full path, including vpool) and vpool name
        """
        info = self.cinder_client.volumes.get(volumeid).initialize_connection(None, {})
        return {'device_path': info['data']['device_path'],
                'vpool_name': info['data']['vpoolname']}

    def get_vmachine_device_info(self, instanceid):
        """
        Return devicename (.xml filename)
        """
        instance = self.nova_client.servers.get(instanceid)
        instance_name = getattr(instance, 'OS-EXT-SRV-ATTR:instance_name')
        instance_host = getattr(instance, 'OS-EXT-SRV-ATTR:host')
        disks = self._get_vmachine_vdisks(instance)
        vpool_name = None
        if len(disks) > 0:
            vpool_name = disks[0]['vpool_name']
        return {'file_name': '{0}.xml'.format(instance_name),
                'host_name': instance_host,
                'vpool_name': vpool_name,
                'disks': disks}

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
        instance_name = devicename.split('/')[-1].split('.')[0]
        vmid = None
        for host, guests in self.get_guests().iteritems():
            for guest in guests:
                if guest['instance_name'] == instance_name:
                    vmid = guest['id']
                    break
        if not vmid:
            raise RuntimeError('Guest with devicename {0} not found'.format(instance_name))
        guest_object = self.nova_client.servers.get(vmid)

        vm_info = {'id': vmid,
                   'name': guest_object.name,
                   'backing': {'datastore': mountpoint,
                               'filename': devicename},
                   'datastores': {mountpoint: '{0}:{1}'.format(ip, mountpoint)},
                   'disks': []}
        order = 0
        for disk in self._get_vmachine_vdisks(guest_object):
            vm_info['disks'].append({'datastore': disk['dir_name'],
                                     'backingfilename': disk['file_name'],
                                     'filename': disk['file_name'],
                                     'name': disk['name'],
                                     'order': order})
            order += 1
        return vm_info

    def is_host_configured(self, ip):
        try:
            return self.management.is_host_configured(ip)
        except (SystemExit, Exception) as ex:
            logger.error('Management action "is_host_configured" failed {0}'.format(ex))
        return False

    def is_host_configured_for_vpool(self, vpool_guid, ip):
        try:
            return self.management.is_host_configured_for_vpool(vpool_guid, ip)
        except (SystemExit, Exception) as ex:
            logger.error('Management action "is_host_configured_for_vpool" failed {0}'.format(ex))
        return False
