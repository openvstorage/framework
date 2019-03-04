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
BackendType module
"""
from ovs.dal.dataobject import DataObject
from ovs.dal.structures import Property, Dynamic
from ovs_extensions.constants.framework import PLUGINS_INSTALLED
from ovs.extensions.generic.configuration import Configuration


class BackendType(DataObject):
    """
    A BackendType represents one of the OVS supported backend types. Each backend type can - optionally - provide extra things
    like a GUI management interface
    """
    __properties = [Property('name', str, doc='Name of the BackendType'),
                    Property('code', str, unique=True, indexed=True, doc='Code representing the BackendType')]
    __relations = []
    __dynamics = [Dynamic('has_plugin', bool, 600)]

    def _has_plugin(self):
        """
        Checks whether this BackendType has a plugin installed
        """
        try:
            return self.code in Configuration.get('{0}|backends'.format(PLUGINS_INSTALLED))
        except:
            return False
