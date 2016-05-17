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
MDSServiceVDisk module
"""
from ovs.dal.dataobject import DataObject
from ovs.dal.structures import Property, Relation
from ovs.dal.hybrids.vdisk import VDisk
from ovs.dal.hybrids.j_mdsservice import MDSService


class MDSServiceVDisk(DataObject):
    """
    The MDSServiceVDisk class represents the junction table between the MetadataServerService and VDisk.
    Examples:
    * my_vdisk.mds_services[0].mds_service
    * my_mds_service.vdisks[0].vdisk
    """
    __properties = [Property('is_master', bool, default=False, doc='Is this the master MDSService for this VDisk.')]
    __relations = [Relation('vdisk', VDisk, 'mds_services'),
                   Relation('mds_service', MDSService, 'vdisks')]
    __dynamics = []
