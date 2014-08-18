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
from ovs.dal.structures import Property, Relation, Dynamic
from ovs.dal.hybrids.mgmtcenter import MgmtCenter
from ovs.extensions.hypervisor.factory import Factory as hvFactory


class PMachine(DataObject):
    """
    The PMachine class represents a pMachine. A pMachine is the physical machine
    running the Hypervisor.
    """
    __properties = {Property('name', str, doc='Name of the pMachine.'),
                    Property('description', str, mandatory=False, doc='Description of the pMachine.'),
                    Property('username', str, doc='Username of the pMachine.'),
                    Property('password', str, doc='Password of the pMachine.'),
                    Property('ip', str, doc='IP address of the pMachine.'),
                    Property('hvtype', ['HYPERV', 'VMWARE', 'XEN', 'KVM'], doc='Hypervisor type running on the pMachine.'),
                    Property('hypervisor_id', str, mandatory=False, doc='Hypervisor id - primary key on Management Center')}
    __relations = [Relation('mgmtcenter', MgmtCenter, 'pmachines', mandatory=False)]
    __dynamics = [Dynamic('host_status', str, 60)]

    def _host_status(self):
        """
        Returns the host status as reported by the management center (e.g. vCenter Server)
        """
        mgmtcentersdk = hvFactory.get_mgmtcenter(self)
        if mgmtcentersdk:
            if self.hypervisor_id:
                return mgmtcentersdk.get_host_status_by_pk(self.hypervisor_id)
            if self.ip:
                return mgmtcentersdk.get_host_status_by_ip(self.ip)
        return 'UNKNOWN'



