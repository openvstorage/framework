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
AlbaProxy module
"""
from ovs.dal.dataobject import DataObject
from ovs.dal.structures import Relation
from ovs.dal.hybrids.storagedriver import StorageDriver
from ovs.dal.hybrids.service import Service


class AlbaProxy(DataObject):
    """
    The AlbaProxy class represents the junction table between the (alba)Service and VPool.
    Examples:
    * my_storagedriver.alba_proxies[0].service
    * my_service.alba_proxy.storagedriver
    """
    __properties = []
    __relations = [Relation('storagedriver', StorageDriver, 'alba_proxies'),
                   Relation('service', Service, 'alba_proxy', onetoone=True)]
    __dynamics = []
