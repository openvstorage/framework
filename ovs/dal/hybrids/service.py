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
    __relations = [Relation('storagerouter', StorageRouter, 'services', mandatory=False, doc='The Storage Router running the Service.'),
                   Relation('type', ServiceType, 'services', doc='The type of the Service.')]
    __dynamics = [Dynamic('is_internal', bool, 3600)]

    def _is_internal(self):
        """
        Returns whether a service is internally managed by OVS or externally managed by customer
        """
        return self.storagerouter is not None
