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
Domain module
"""
from ovs.dal.dataobject import DataObject
from ovs.dal.lists.storagerouterlist import StorageRouterList
from ovs.dal.structures import Dynamic, Property


class Domain(DataObject):
    """
    The Domain class represents tags given to Storage Routers and backends. These tags or domains can be named anything, but for ease of use,
    its recommended they represent a physical location or specific name. Eg: 'datacenter1', 'datacenter1 - rack1'

    Eg:
    || Storage Router ||                Domains            ||          Failure Domains          ||
    |  storagerouter1  |  datacenter1                       |  datacenter2                       |
    |  storagerouter2  |  datacenter1, datacenter1 - rack1  |  datacenter2                       |
    |  storagerouter3  |  datacenter2, datacenter2 - rack1  |  datacenter1, datacenter1 - rack1  |
    |  storagerouter4  |  datacenter2, datacenter2 - rack2  |  datacenter1, datacenter1 - rack1  |

        - Storage router 1 is part of datacenter1, the failure domain is part of datacenter2
        - Storage router 2 is part of rack1 in datacenter1, the failure domain is part of datacenter2
        - Storage router 3 is part of rack1 in datacenter2, the failure domain is part of rack1 in datacenter1
        - Storage router 4 is part of rack2 in datacenter2, the failure domain is part of rack1 in datacenter1

    Storagerouter1 will have its DTL configured on storagerouters within its own domains (by default), which means storagerouter2 in this example
    If storagerouter2 would go down, the failure domain will be used, which means storagerouter3 and storagerouter4 can be used for DTL of storagerouter1

    Each storage router CAN also have several failure domains
    For storagerouter1 this will be all the storagerouters part of datacenter2 (storagerouter3 and storagerouter4)
    For storagerouter2 this will be all the storagerouters part of rack1 in datacenter2 (storagerouter3)
    For storagerouter3 this will be all the storagerouters part of rack1 in datacenter1 (storagerouter2)
    For storagerouter4 this will be all the storagerouters part of rack1 in datacenter1 (storagerouter2)
    """
    __properties = [Property('name', str, indexed=True, doc='The name of the domain')]
    __relations = []
    __dynamics = [Dynamic('storage_router_layout', dict, 5)]

    def _storage_router_layout(self):
        """
        Creates a dictionary with information about which Storage Routers use this domain as its normal and recovery domain
        :return: Information about Storage Routers using this domain
        :rtype: dict
        """
        layout = {'regular': [],
                  'recovery': []}
        for sr in StorageRouterList.get_storagerouters():
            for junction in sr.domains:
                if junction.domain_guid == self.guid:
                    if junction.backup is True:
                        layout['recovery'].append(sr.guid)
                    else:
                        layout['regular'].append(sr.guid)
        return layout
