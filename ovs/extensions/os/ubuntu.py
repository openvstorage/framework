# Copyright 2015 iNuron NV
#
# Licensed under the Open vStorage Modified Apache License (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.openvstorage.org/license
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Ubuntu OS module
"""

from subprocess import CalledProcessError
from subprocess import check_output
from ovs.extensions.db.etcd.configuration import EtcdConfiguration
from ovs.extensions.generic.system import System


class Ubuntu(object):
    """
    Contains all logic related to Ubuntu specific
    """

    @staticmethod
    def get_path(binary_name):
        """
        Retrieve the absolute path for binary
        :param binary_name: Binary to get path for
        :return: Path
        """
        machine_id = System.get_my_machine_id()
        config_location = '/ovs/framework/hosts/{0}/paths|{1}'.format(machine_id, binary_name)
        if not EtcdConfiguration.exists(config_location):
            try:
                path = check_output('which {0}'.format(binary_name), shell=True).strip()
                EtcdConfiguration.set(config_location, path)
            except CalledProcessError:
                return None
        else:
            path = EtcdConfiguration.get(config_location)
        return path

    @staticmethod
    def get_fstab_entry(device, mp, filesystem='ext4'):
        """
        Retrieve fstab entry for mountpoint
        :param device: Device in fstab
        :param mp: Mountpoint
        :param filesystem: Filesystem of entry
        :return: Fstab entry
        """
        return '{0}    {1}         {2}    defaults,nobootwait,noatime,discard    0    2'.format(device, mp, filesystem)

    @staticmethod
    def get_ssh_service_name():
        """
        Retrieve SSH service name
        :return: SSH service name
        """
        return 'ssh'

    @staticmethod
    def get_openstack_web_service_name():
        """
        Retrieve openstack webservice name
        :return: Openstack webservice name
        """
        return 'apache2'

    @staticmethod
    def get_openstack_cinder_service_name():
        """
        Retrieve openstack cinder service name
        :return: Openstack cinder service name
        """
        return 'cinder-volume'

    @staticmethod
    def get_openstack_services():
        """
        Retrieve openstack services
        :return: Openstack services
        """
        return ['nova-compute', 'nova-api-os-compute', 'cinder-volume', 'cinder-api']

    @staticmethod
    def get_openstack_users():
        """
        Retrieve openstack users
        :return: Openstack users
        """
        return ['libvirt-qemu', 'cinder']

    @staticmethod
    def get_openstack_package_base_path():
        """
        Retrieve openstack package base path
        :return: Openstack package base path
        """
        return '/usr/lib/python2.7/dist-packages'
