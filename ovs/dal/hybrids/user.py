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
User module
"""
from ovs.dal.dataobject import DataObject
from ovs.dal.structures import Property, Relation
from ovs.dal.hybrids.group import Group


class User(DataObject):
    """
    The User class represents a User.  A user is an individual who can perform actions
    on objects in Open vStorage.
    """
    __properties = [Property('username', str, doc='Username of the User.'),
                    Property('password', str, doc='Password of the User.'),
                    Property('is_active', bool, doc='Indicates whether the User is active.'),
                    Property('language', ['en-US', 'nl-NL'], default='en-US', doc='Language of the User.')]
    __relations = [Relation('group', Group, 'users')]
    __dynamics = []
