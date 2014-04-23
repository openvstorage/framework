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
PMachine module
"""
from ovs.dal.dataobject import DataObject
from ovs.dal.hybrids.mgmtcenter import MgmtCenter
from ovs.extensions.hypervisor.factory import Factory as hvFactory


class PMachine(DataObject):
    """
    The PMachine class represents a pMachine. A pMachine is the physical machine
    running the Hypervisor.
    """
    # pylint: disable=line-too-long
    _blueprint = {'name':        (None, str, 'Name of the pMachine.'),
                  'description': (None, str, 'Description of the pMachine.'),
                  'username':    (None, str, 'Username of the pMachine.'),
                  'password':    (None, str, 'Password of the pMachine.'),
                  'ip':          (None, str, 'IP address of the pMachine.'),
                  'hvtype':      (None, ['HYPERV', 'VMWARE', 'XEN', 'KVM'], 'Hypervisor type running on the pMachine.')}
    _relations = {'mgmtcenter': (MgmtCenter, 'pmachines')}
    _expiry = {'host_status': (60, str)}
    # pylint: enable=line-too-long

    def _host_status(self):
        """
        Returns the host status as reported by the management center (e.g. vCenter Server)
        """
        mgmtcentersdk = hvFactory.get_mgmtcenter(self)
        if mgmtcentersdk:
            return mgmtcentersdk.get_host_status(self.ip)
        return 'UNKNOWN'



