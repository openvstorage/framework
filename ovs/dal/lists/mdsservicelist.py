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
MDSServiceList module
"""
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
        mdsservice_type = ServiceTypeList.get_by_name('MetadataServer')
        storagedriver = StorageDriver(storagedriver_guid)
        for service in storagedriver.storagerouter.services:
            if service.type_guid == mdsservice_type.guid and service.mds_service.vpool_guid == storagedriver.vpool_guid:
                return service.mds_service
        return None
