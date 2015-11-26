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
from ovs.extensions.generic.configuration import Configuration


class Ubuntu(object):
    """
    Contains all logic related to Ubuntu specific
    """

    @staticmethod
    def get_path(binary_name):
        config_location = 'ovs.path.{0}'.format(binary_name)
        path = Configuration.get(config_location)
        if not path:
            try:
                path = check_output('which {0}'.format(binary_name), shell=True).strip()
                Configuration.set(config_location, path)
            except CalledProcessError:
                return None
        return path

    @staticmethod
    def get_fstab_entry(device, mp, filesystem='ext4'):
        return '{0}    {1}         {2}    defaults,nobootwait,noatime,discard    0    2'.format(device, mp, filesystem)

    @staticmethod
    def get_ssh_service_name():
        return 'ssh'

    @staticmethod
    def get_openstack_web_service_name():
        return 'apache2'

    @staticmethod
    def get_openstack_cinder_service_name():
        return 'cinder-volume'

    @staticmethod
    def get_openstack_services():
        return ['nova-compute', 'nova-api-os-compute', 'cinder-volume', 'cinder-api']

    @staticmethod
    def get_openstack_users():
        return ['libvirt-qemu','cinder']

    @staticmethod
    def get_openstack_package_base_path():
        return '/usr/lib/python2.7/dist-packages'
