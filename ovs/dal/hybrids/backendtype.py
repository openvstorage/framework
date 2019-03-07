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
from ovs.extensions.generic.configuration import Configuration
from ovs.dal.dataobject.attributes import Property, Dynamic, Relation, RelationGuid, RelationTypes


class BackendType(DataObject):
    """
    A BackendType represents one of the OVS supported backend types. Each backend type can - optionally - provide extra things
    like a GUI management interface
    """
    name = Property(str, doc='Name of the BackendType')
    code = Property(str, indexed=True, doc='Code representing the BackendType')

    has_plugin = Dynamic(bool, 600)

    # Non dynamic relations
    backends = Relation('Backend', relation_type=RelationTypes.MANYTOONE)
    backends_guids = RelationGuid(backends)

    # __properties = [Property('name', str, doc='Name of the BackendType'),
    #                 Property('code', str, unique=True, indexed=True, doc='Code representing the BackendType')]
    # __relations = []
    # __dynamics = [Dynamic('has_plugin', bool, 600)]

    @has_plugin.associate_function
    def _has_plugin(self):
        """
        Checks whether this BackendType has a plugin installed
        """
        try:
            return self.code in Configuration.get('/ovs/framework/plugins/installed|backends')
        except:
            return False
