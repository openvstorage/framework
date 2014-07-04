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
Role module
"""
from ovs.dal.dataobject import DataObject


class Role(DataObject):
    """
    The Role class represents a Role. A Role is used to allow execution of a certain set of
    actions. E.g. a "Viewer" Role can view all data but has no update/write permission.
    """
    # pylint: disable=line-too-long
    __blueprint = {'name':        (None, str, 'Name of the Role'),
                   'code':        (None, str, 'Contains a code which is referenced from the API code'),
                   'description': (None, str, 'Description of the Role')}
    __relations = {}
    __expiry = {}
    # pylint: enable=line-too-long
