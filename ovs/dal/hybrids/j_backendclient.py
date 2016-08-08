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
BackendClient module
"""
from ovs.dal.dataobject import DataObject
from ovs.dal.structures import Property, Relation
from ovs.dal.hybrids.backend import Backend
from ovs.dal.hybrids.client import Client


class BackendClient(DataObject):
    """
    The BackendClient class represents the junction table between a Client and a Backend, setting granted/deny rights
    Examples:
    * my_backend.client_rights[0].client
    * my_client.backend_rights[0].backend
    """
    __properties = [Property('grant', bool, doc='Whether the rights are granted (True) or denied (False)')]
    __relations = [Relation('backend', Backend, 'client_rights'),
                   Relation('client', Client, 'backend_rights')]
    __dynamics = []
