# Copyright 2016 iNuron NV
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
ServiceType module
"""
from ovs.dal.dataobject import DataObject
from ovs.dal.structures import Property


class ServiceType(DataObject):
    """
    A ServiceType represents some kind of service that needs to be managed by the framework.
    """
    SERVICE_TYPES = DataObject.enumerator('Service_type', {'NS_MGR': 'NamespaceManager',
                                                           'ARAKOON': 'Arakoon',
                                                           'ALBA_MGR': 'AlbaManager',
                                                           'MD_SERVER': 'MetadataServer',
                                                           'ALBA_PROXY': 'AlbaProxy'})
    ARAKOON_CLUSTER_TYPES = DataObject.enumerator('Arakoon_cluster_type', ['ABM', 'FWK', 'NSM', 'SD'])

    __properties = [Property('name', str, doc='Name of the ServiceType.')]
    __relations = []
    __dynamics = []
