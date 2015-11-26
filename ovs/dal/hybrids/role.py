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
Role module
"""
from ovs.dal.dataobject import DataObject
from ovs.dal.structures import Property


class Role(DataObject):
    """
    The Role class represents a Role. A Role is used to allow execution of a certain set of
    actions. E.g. a "Viewer" Role can view all data but has no update/write permission.
    """
    __properties = [Property('name', str, doc='Name of the Role'),
                    Property('code', str, doc='Contains a code which is referenced from the API code'),
                    Property('description', str, mandatory=False, doc='Description of the Role')]
    __relations = []
    __dynamics = []
