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
Service module
"""
from ovs.dal.dataobject import DataObject
from ovs.dal.structures import Property, Relation
from ovs.dal.hybrids.storagerouter import StorageRouter
from ovs.dal.hybrids.servicetype import ServiceType


class Service(DataObject):
    """
    A Service represents some kind of service that needs to be managed by the framework.
    """
    __properties = [Property('name', str, doc='Name of the Service.'),
                    Property('ports', list, doc='Ip of the Service.')]
    __relations = [Relation('storagerouter', StorageRouter, 'services', doc='The StorageRouter running the Service.'),
                   Relation('type', ServiceType, 'services', doc='The type of the Service.')]
    __dynamics = []
