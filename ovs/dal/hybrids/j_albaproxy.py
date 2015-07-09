# Copyright 2015 CloudFounders NV
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
    * my_vpool.alba_proxies[0].service
    * my_service.alba_proxy.vpool
    """
    __properties = []
    __relations = [Relation('storagedriver', StorageDriver, 'alba_proxy', onetoone=True),
                   Relation('service', Service, 'alba_proxy', onetoone=True)]
    __dynamics = []
