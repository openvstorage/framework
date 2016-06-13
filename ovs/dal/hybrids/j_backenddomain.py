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
BackendDomain module
"""
from ovs.dal.dataobject import DataObject
from ovs.dal.structures import Relation
from ovs.dal.hybrids.backend import Backend
from ovs.dal.hybrids.domain import Domain


class BackendDomain(DataObject):
    """
    The BackendDomain class represents the junction table between Backend and Domain.
    """
    __properties = []
    __relations = [Relation('domain', Domain, 'backends'),
                   Relation('backend', Backend, 'domains')]
    __dynamics = []
