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
Backend module
"""
from ovs.dal.dataobject import DataObject
from ovs.dal.structures import Property, Relation, Dynamic
from ovs.dal.hybrids.backendtype import BackendType


class Backend(DataObject):
    """
    A Backend represents an instance of the supported backend types that has been setup with the OVS GUI
    """
    __properties = [Property('name', str, doc='Name of the Backend.'),
                    Property('status', ['NEW', 'INSTALLING', 'RUNNING', 'STOPPED', 'FAILURE', 'UNKNOWN'], default='NEW', doc='State of the backend')]
    __relations = [Relation('backend_type', BackendType, 'backends', doc='Type of the backend.')]
    __dynamics = [Dynamic('linked_guid', str, 3600)]

    def _linked_guid(self):
        """
        Returns the GUID of the detail object that's linked to this particular backend. This depends on the backend type.
        This requires that the backlink from that object to this object is named <backend_type>_backend and is a
        one-to-one relation
        """
        return getattr(self, '{0}_backend_guid'.format(self.backend_type.code))
