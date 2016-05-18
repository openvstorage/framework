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
MDSServiceList module
"""
from ovs.dal.hybrids.servicetype import ServiceType
from ovs.dal.hybrids.storagedriver import StorageDriver
from ovs.dal.lists.servicetypelist import ServiceTypeList


class MDSServiceList(object):
    """
    This MDSServiceList class contains various lists regarding to the MDSService class
    """

    @staticmethod
    def get_by_storagedriver(storagedriver_guid):
        """
        Returns a list of MDSServices based on the StorageDriver (via StorageRouter > Service and Vpool)
        * This list uses object relations instead of queries for better performance
        """
        mdsservice_type = ServiceTypeList.get_by_name(ServiceType.SERVICE_TYPES.MD_SERVER)
        storagedriver = StorageDriver(storagedriver_guid)
        for service in storagedriver.storagerouter.services:
            if service.type_guid == mdsservice_type.guid and service.mds_service.vpool_guid == storagedriver.vpool_guid:
                return service.mds_service
        return None
