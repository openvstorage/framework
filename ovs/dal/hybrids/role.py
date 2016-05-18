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
