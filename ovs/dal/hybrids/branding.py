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


class Branding(DataObject):
    """
    The Branding class represents the specific OEM information.
    """
    # pylint: disable=line-too-long
    __blueprint = {'name':        (None,  str,  'Name of the Brand.'),
                   'description': (None,  str,  'Description of the Brand.'),
                   'css':         (None,  str,  'CSS file used by the Brand.'),
                   'productname': (None,  str,  'Commercial product name.'),
                   'is_default':  (False, bool, 'Indicates whether this Brand is the default one.')}
    __relations = {}
    __expiry = {}
    # pylint: enable=line-too-long
