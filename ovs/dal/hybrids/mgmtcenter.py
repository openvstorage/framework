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
Management center module
"""
from ovs.dal.dataobject import DataObject
from ovs.extensions.hypervisor.factory import Factory


class MgmtCenter(DataObject):
    """
    The MgmtCenter represents a management center (e.g. vCenter Server for VMware)
    """
    # pylint: disable=line-too-long
    _blueprint = {'name':        (None, str, 'Name of the Management Center.'),
                  'description': (None, str, 'Description of the Management Center.'),
                  'username':    (None, str, 'Username of the Management Center.'),
                  'password':    (None, str, 'Password of the Management Center.'),
                  'ip':          (None, str, 'IP address of the Management Center.'),
                  'port':        (None, int, 'Port of the Management Center.'),
                  'type':        (None, ['VCENTER'], 'Management Center type.')}
    _relations = {}
    _expiry = {'hosts': (60, dict)}
    # pylint: enable=line-too-long

    def _hosts(self):
        """
        Returns all hosts (not only those known to OVS) managed by the Management center
        """
        mgmt_center = Factory.get_mgmtcenter(mgmt_center=self)
        if mgmt_center is not None:
            return mgmt_center.get_hosts()
        else:
            return {}
