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
                  'port':        (None, int, 'IP address of the Management Center.')}
    _relations = {}
    _expiry = {}
    # pylint: enable=line-too-long
