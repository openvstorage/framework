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
                                                           'ALBA_PROXY': 'AlbaProxy',
                                                           'ALBA_S3_TRANSACTION': 'AlbaS3Transaction'})
    ARAKOON_CLUSTER_TYPES = DataObject.enumerator('Arakoon_cluster_type', ['ABM', 'FWK', 'NSM', 'SD', 'CFG'])

    __properties = [Property('name', str, unique=True, indexed=True, doc='Name of the ServiceType.')]
    __relations = []
    __dynamics = []
