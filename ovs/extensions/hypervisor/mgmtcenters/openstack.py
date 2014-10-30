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
Module for the OpenStack Controller API
"""

class OpenStack(object):
    """
    Represents the management center for OpenStack
    """

    def __init__(self, ip, username, password):
        """
        Initializes the object with credentials and connection information
        Requires novaclient library to be installed on the node this code is executed
        """
        from novaclient.v1_1 import client
        self.client = client.Client(username = username,
                                    api_key = password,
                                    project_id = 'admin',
                                    auth_url = 'http://{}:35357/v2.0'.format(ip),
                                    service_type="compute")
        self.STATE_MAPPING = {'up' : 'RUNNING'}

    def get_host_status_by_ip(self, host_ip):
        """
        Return host status
        """
        host_id = self.get_host_primary_key(host_ip)
        host = self.client.hypervisors.get(host_id)
        return self.STATE_MAPPING.get(host.state, 'UNKNOWN')

    def get_host_status_by_pk(self, pk):
        """
        Return host status
        """
        host = self.client.hypervisors.get(pk)
        return self.STATE_MAPPING.get(host.state, 'UNKNOWN')

    def get_host_primary_key(self, host_ip):
        """
        Get hypervisor id based on host_ip
        """
        hosts = [hv for hv in self.client.hypervisors.list() if hv.host_ip == host_ip]
        if not hosts:
            raise RuntimeError('Host with ip {0} not found in datacenter info'.format(host_ip))
        return hosts[0].id

    def test_connection(self):
        """
        Test connection
        """
        try:
            self.client.authenticate()
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
        hvs = self.client.hypervisors.list()  # We are interested in compute nodes
        for hv in hvs:
            hosts[hv.hypervisor_hostname] = {'ips': [hv.host_ip],
                                             'name': hv.hypervisor_hostname}
        return hosts