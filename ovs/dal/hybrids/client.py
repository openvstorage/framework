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
Client module
"""
from ovs.dal.dataobject import DataObject
from ovs.dal.hybrids.user import User


class Client(DataObject):
    """
    The Client class represents a client (application) used by the User. A user might use multiple clients and
    will at least have one default application (frontend GUI)
    """
    # pylint: disable=line-too-long
    __blueprint = {'client_secret':      (None, str, 'Client secret (application password)'),
                   'grant_type':         (None, ['PASSWORD', 'CLIENT_CREDENTIALS'], 'Grant type of the Client'),
                   'ovs_type':           (None, ['FRONTEND', 'REPLICATION'], 'The type of the client within Open vStorage')}
    __relations = {'user': (User, 'clients')}
    __expiry = {'client_id': (86400, str)}
    # pylint: enable=line-too-long

    def _client_id(self):
        """
        The client_id is in fact our model's guid
        """
        return self.guid
