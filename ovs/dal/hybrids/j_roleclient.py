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
RoleClient module
"""
from ovs.dal.dataobject import DataObject
from ovs.dal.hybrids.role import Role
from ovs.dal.hybrids.client import Client


class RoleClient(DataObject):
    """
    The RoleClient class represents the junction table between Role and Client.
    """
    # pylint: disable=line-too-long
    __blueprint = {}
    __relations = {'scope':  (Role,  'clients'),
                   'client': (Client, 'scopes')}
    __expiry = {}
    # pylint: enable=line-too-long
