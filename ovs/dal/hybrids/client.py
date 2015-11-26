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
Client module
"""
from ovs.dal.dataobject import DataObject
from ovs.dal.structures import Property, Relation, Dynamic
from ovs.dal.hybrids.user import User


class Client(DataObject):
    """
    The Client class represents a client (application) used by the User. A user might use multiple clients and
    will at least have one default application (frontend GUI)
    """
    __properties = [Property('name', str, mandatory=False, doc='Name of the client'),
                    Property('client_secret', str, mandatory=False, doc='Client secret (application password)'),
                    Property('grant_type', ['PASSWORD', 'CLIENT_CREDENTIALS'], doc='Grant type of the Client'),
                    Property('ovs_type', ['INTERNAL', 'REPLICATION', 'USER'], doc='The type of the client within Open vStorage')]
    __relations = [Relation('user', User, 'clients')]
    __dynamics = [Dynamic('client_id', str, 86400)]

    def _client_id(self):
        """
        The client_id is in fact our model's guid
        """
        return self.guid
