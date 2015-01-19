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
MDSService module
"""
from ovs.dal.dataobject import DataObject
from ovs.dal.structures import Property, Relation
from ovs.dal.hybrids.vpool import VPool
from ovs.dal.hybrids.service import Service
from ovs.extensions.storageserver.storagedriver import MetadataServerClient


class MDSService(DataObject):
    """
    The MDSService class represents the junction table between the (metadataserver)Service and VPool.
    Examples:
    * my_vpool.mds_services[0].service
    * my_service.mds_service.vpool
    """
    __properties = [Property('number', int, doc='The number of the service in case there are more than one'),
                    Property('capacity', int, default=-1, doc='The capacity of this MDS, negative means infinite')]
    __relations = [Relation('vpool', VPool, 'mds_services'),
                   Relation('service', Service, 'mds_service', onetoone=True)]
    __dynamics = []

    def __init__(self, *args, **kwargs):
        """
        Initializes a MDSService, setting up its additional helpers
        """
        DataObject.__init__(self, *args, **kwargs)
        self._frozen = False
        self.metadataserver_client = None
        self._frozen = True
        self.reload_client()

    def reload_client(self):
        """
        Reloads the StorageDriver Client
        """
        if self.service:
            self._frozen = False
            self.metadataserver_client = MetadataServerClient.load(self.service)
            self._frozen = True
