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
from ovs.dal.structures import Property, Relation
from ovs.dal.hybrids.backendtype import BackendType


class Backend(DataObject):
    """
    A Backend represents an instance of the supported backend types that has been setup with the OVS GUI
    """
    __properties = [Property('name', str, doc='Name of the Backend.'),
                    Property('status', ['NEW', 'INSTALLING', 'RUNNING', 'STOPPED', 'FAILURE', 'UNKNOWN'], default='NEW', doc='State of the backend')]
    __relations = [Relation('backend_type', BackendType, 'backends', doc='Type of the backend.')]
    __dynamics = []
