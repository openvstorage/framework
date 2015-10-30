# Copyright 2015 CloudFounders NV
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
Centos OS module
"""

from subprocess import CalledProcessError
from subprocess import check_output
from ovs.extensions.generic.configuration import Configuration


class Centos(object):
    """
    Contains all logic related to Centos specific
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
        return '{0}    {1}         {2}    defaults,nofail,noatime,discard    0    2'.format(device, mp, filesystem)

    @staticmethod
    def get_ssh_service_name():
        return 'sshd'

    @staticmethod
    def get_openstack_web_service_name():
        return 'httpd'

    @staticmethod
    def get_openstack_cinder_service_name():
        return 'openstack-cinder-volume'

    @staticmethod
    def get_openstack_services():
        return ['openstack-nova-compute', 'openstack-cinder-volume', 'openstack-cinder-api']

    @staticmethod
    def get_openstack_users():
        return ['qemu', 'cinder', 'nova']

    @staticmethod
    def get_openstack_package_base_path():
        return '/usr/lib/python2.7/site-packages'
