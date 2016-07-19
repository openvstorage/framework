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
Module for domains
"""

from backend.decorators import load, log, required_roles, return_list
from backend.exceptions import HttpNotImplementedException, HttpNotAcceptableException
from ovs.dal.hybrids.edgeclient import EdgeClient
from ovs.dal.lists.storagedriverlist import StorageDriverList
from ovs.dal.lists.vdisklist import VDiskList
from rest_framework import viewsets
from rest_framework.permissions import IsAuthenticated


class EdgeClientViewSet(viewsets.ViewSet):
    """
    Information about Domains
    """
    permission_classes = (IsAuthenticated,)
    prefix = r'edgeclients'
    base_name = 'edgeclients'

    @log()
    @required_roles(['read'])
    @return_list(EdgeClient)
    @load()
    def list(self, volume_id=None):
        """
        Lists all available EdgeClients
        """
        clients = []
        if volume_id is None:
            storagedrivers = StorageDriverList.get_storagedrivers()
        else:
            vdisk = VDiskList.get_vdisk_by_volume_id(volume_id)
            if vdisk is None:
                raise HttpNotAcceptableException(error_description='The given volume_id does not link to a vDisk',
                                                 error='object_not_found')
            storagedrivers = vdisk.vpool.storagedrivers
        for sd in storagedrivers:
            for item in sd.vpool.storagedriver_client.list_client_connections(str(sd.storagedriver_id)):
                if volume_id is not None and item.object_id != volume_id:
                    continue
                client = EdgeClient(data={'object_id': item.object_id,
                                          'ip': item.ip,
                                          'port': item.port},
                                    volatile=True)
                clients.append(client)
        clients.sort(key=lambda e: ('ip', 'port'))
        return clients

    @log()
    @required_roles(['read'])
    @load()
    def retrieve(self):
        """
        Load information about a given Domain
        """
        raise HttpNotImplementedException(error_description='EdgeClients cannot be requested individually',
                                          error='unavailable')
