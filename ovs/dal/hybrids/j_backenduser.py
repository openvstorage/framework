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
BackendUser module
"""
from ovs.dal.dataobject import DataObject
from ovs.dal.structures import Property, Relation
from ovs.dal.hybrids.backend import Backend
from ovs.dal.hybrids.user import User


class BackendUser(DataObject):
    """
    The BackendUser class represents the junction table between a User and Backend, setting granted/deny rights
    Examples:
    * my_backend.user_rights[0].user
    * my_user.backend_rights[0].backend
    """
    __properties = [Property('grant', bool, doc='Whether the rights is granted (True) or denied (False)')]
    __relations = [Relation('backend', Backend, 'user_rights'),
                   Relation('user', User, 'backend_rights')]
    __dynamics = []
