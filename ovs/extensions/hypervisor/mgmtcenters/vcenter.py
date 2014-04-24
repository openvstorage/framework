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
Module for the vcenter management center client
"""

from ovs.extensions.hypervisor.apis.vmware.sdk import Sdk


class VCenter():
    """
    Represents the management center for vcenter server
    """

    def __init__(self, ip, username, password):
        """
        Initializes the object with credentials and connection information
        """
        self.sdk = Sdk(ip, username, password)

    def get_host_status_by_ip(self, host_ip):
        """
        Return host status from vCenter Server
        Must be connected to vCenter
        """
        return self.sdk.get_host_status_by_ip(host_ip)

    def get_host_status_by_pk(self, pk):
        """
        Return host status from vCenter Server
        Must be connected to vCenter
        """
        return self.sdk.get_host_status_by_pk(pk)

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
        """
        return self.sdk.test_connection()
