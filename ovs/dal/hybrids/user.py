# Copyright (C) 2016 iNuron NV
#
# This file is part of Open vStorage Open Source Edition (OSE),
# as available from
#
#      http://www.openvstorage.org and
#      http://www.openvstorage.com.
#
# This file is free software; you can redistribute it and/or modify it
# under the terms of the GNU Affero General Public License v3 (GNU AGPLv3)
# as published by the Free Software Foundation, in version 3 as it comes
# in the LICENSE.txt file of the Open vStorage OSE distribution.
#
# Open vStorage is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY of any kind.

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
    __properties = [Property('username', str, unique=True, doc='Username of the User.'),
                    Property('password', str, doc='Password of the User.'),
                    Property('is_active', bool, doc='Indicates whether the User is active.'),
                    Property('language', ['en-US', 'nl-NL'], default='en-US', doc='Language of the User.')]
    __relations = [Relation('group', Group, 'users')]
    __dynamics = []
