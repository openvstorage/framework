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
Bearer Token module
"""
from ovs.dal.dataobject import DataObject
from ovs.dal.structures import Property, Relation
from ovs.dal.hybrids.client import Client


class BearerToken(DataObject):
    """
    The Bearer Token class represents the Bearer tokens used by the API by means of OAuth 2.0
    """
    __properties = [Property('access_token', str, mandatory=False, doc='Access token'),
                    Property('refresh_token', str, mandatory=False, doc='Refresh token'),
                    Property('expiration', int, doc='Expiration timestamp')]
    __relations = [Relation('client', Client, 'tokens')]
    __dynamics = []
