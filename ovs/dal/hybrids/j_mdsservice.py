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
MDSService module
"""

from ovs.dal.dataobject import DataObject
from ovs.dal.structures import Property, Relation
from ovs.dal.hybrids.vpool import VPool
from ovs.dal.hybrids.service import Service
from ovs.extensions.storageserver.storagedriver import MetadataServerClient


class MDSService(DataObject):
    """
    The MDSService class represents the junction table between the (metadata server) Service and VPool.
    Examples:
    * my_vpool.mds_services[0].service
    * my_service.mds_service.vpool
    """
    __properties = [Property('number', int, doc='The number of the service in case there is more than 1'),
                    Property('capacity', int, default=100, doc='The capacity of this MDS, negative means infinite')]
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
