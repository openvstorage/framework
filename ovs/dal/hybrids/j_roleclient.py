# Copyright 2014 iNuron NV
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
RoleClient module
"""
from ovs.dal.dataobject import DataObject
from ovs.dal.structures import Relation
from ovs.dal.hybrids.role import Role
from ovs.dal.hybrids.client import Client


class RoleClient(DataObject):
    """
    The RoleClient class represents the junction table between Role and Client.
    Examples:
    * my_role.clients[0].client
    * my_client.roles[0].role
    """
    __properties = []
    __relations = [Relation('role', Role, 'clients'),
                   Relation('client', Client, 'roles')]
    __dynamics = []
