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
BackendType module
"""
from ovs.dal.dataobject import DataObject
from ovs.dal.structures import Property


class BackendType(DataObject):
    """
    A BackendType represents one of the OVS supported backend types. Each backend type can - optionally - provide extra things
    like a GUI management interface
    """
    __properties = [Property('name', str, doc='Name of the BackendType'),
                    Property('code', str, doc='Code representing the BackendType'),
                    Property('has_gui', bool, default=False, doc='Indicates whether the backend type has a GUI (provided by means of a plugin)')]
    __relations = []
    __dynamics = []
