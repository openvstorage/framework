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
StorageRouterDomain module
"""
from ovs.dal.dataobject import DataObject
from ovs.dal.structures import Property, Relation
from ovs.dal.hybrids.domain import Domain
from ovs.dal.hybrids.storagerouter import StorageRouter


class StorageRouterDomain(DataObject):
    """
    The StorageRouterDomain class represents the junction table between StorageRouter and Domain.
    """
    __properties = [Property('backup', bool, doc='Indicator whether the StorageRouterDomain is used as failure domain or regular domain')]
    __relations = [Relation('domain', Domain, 'storagerouters'),
                   Relation('storagerouter', StorageRouter, 'domains')]
    __dynamics = []
