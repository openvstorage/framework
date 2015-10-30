# Copyright 2014 iNuron NV
#
# Licensed under the Open vStorage Non-Commercial License, Version 1.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.openvstorage.org/OVS_NON_COMMERCIAL
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
from ovs.dal.structures import Property, Dynamic
from ovs.extensions.generic.configuration import Configuration


class BackendType(DataObject):
    """
    A BackendType represents one of the OVS supported backend types. Each backend type can - optionally - provide extra things
    like a GUI management interface
    """
    __properties = [Property('name', str, doc='Name of the BackendType'),
                    Property('code', str, doc='Code representing the BackendType')]
    __relations = []
    __dynamics = [Dynamic('has_plugin', bool, 600)]

    def _has_plugin(self):
        """
        Checks whether this BackendType has a plugin installed
        """
        try:
            return self.code in Configuration.get('ovs.plugins.backends')
        except:
            return False
