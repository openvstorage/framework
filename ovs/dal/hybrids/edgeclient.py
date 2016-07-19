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
EdgeClient module
"""
from ovs.dal.dataobject import DataObject
from ovs.dal.structures import Property


class EdgeClient(DataObject):
    """
    The EdgeClient class represents an Edge Client.

    IT SHOULD NOT BE USED TO SAVE AN EDGE CLIENT TO THE STORAGE, BUT ONLY TO REPRESENT AN IN MEMORY INSTANCE
    """
    __properties = [Property('object_id', str, doc='Identifier of the volume'),
                    Property('ip', str, doc='IP of the client'),
                    Property('port', int, doc='Port of the client')]
    __relations = []
    __dynamics = []

    def save(self, recursive=False, skip=None, _hook=None):
        _ = recursive, skip, _hook
        raise NotImplemented('An Edge client should not be stored')
