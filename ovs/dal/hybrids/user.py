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
from ovs.dal.hybrids.group import Group


class User(DataObject):
    """
    The User class represents a User.  A user is an individual who can perform actions
    on objects in Open vStorage.
    """
    # pylint: disable=line-too-long
    _blueprint = {'username':  (None,    str,  'Username of the User.'),
                  'password':  (None,    str,  'Password of the User.'),
                  'email':     (None,    str,  'Email address of the User.'),
                  'is_active': (False,   bool, 'Indicates whether the User is active.'),
                  'language':  ('en-US', ['en-US', 'nl-NL'], 'Language of the User.')}
    _relations = {'group': (Group, 'users')}
    _expiry = {}
    # pylint: enable=line-too-long
