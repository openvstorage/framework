# Copyright 2014 iNuron NV
#
# Licensed under the Open vStorage Modified Apache License (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.openvstorage.org/license
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Service module
"""
from ovs.dal.dataobject import DataObject
from ovs.dal.structures import Dynamic, Property, Relation
from ovs.dal.hybrids.storagerouter import StorageRouter
from ovs.dal.hybrids.servicetype import ServiceType


class Service(DataObject):
    """
    A Service represents some kind of service that needs to be managed by the framework.
    """
    __properties = [Property('name', str, doc='Name of the Service.'),
                    Property('ports', list, doc='Port(s) of the Service.')]
    __relations = [Relation('storagerouter', StorageRouter, 'services', mandatory=False,
                            doc='The StorageRouter running the Service.'),
                   Relation('type', ServiceType, 'services', doc='The type of the Service.')]
    __dynamics = [Dynamic('is_internal', bool, 3600)]

    def _is_internal(self):
        """
        Returns whether a service is internally managed by OVS or externally managed by customer
        """
        return self.storagerouter is not None
