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
Branding module
"""
from ovs.dal.dataobject import DataObject
from ovs.dal.structures import Property


class Branding(DataObject):
    """
    The Branding class represents the specific OEM information.
    """
    __properties = {Property('name', str, doc='Name of the Brand.'),
                    Property('description', str, mandatory=False, doc='Description of the Brand.'),
                    Property('css', str, doc='CSS file used by the Brand.'),
                    Property('productname', str, doc='Commercial product name.'),
                    Property('is_default', bool, doc='Indicates whether this Brand is the default one.')}
    __relations = []
    __dynamics = []
