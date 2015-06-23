# Copyright 2015 CloudFounders NV
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
    def get_fstab_entry(label, mp):
        return 'LABEL={0}    {1}         ext4    defaults,nofail,noatime,discard    0    2'.format(label, mp)

    @staticmethod
    def get_ssh_service_name():
        return 'sshd'

    @staticmethod
    def get_openstack_web_service_name():
        return 'httpd'
