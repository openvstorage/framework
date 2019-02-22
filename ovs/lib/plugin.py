# Copyright (C) 2019 iNuron NV
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
Plugincontroller parent class module
"""

from ovs_extensions.constants.modules import OVS_DAL_HYBRIDS, OVS_LIB, API_VIEWS
from ovs_extensions.generic.plugin import _PluginController

class PluginController(_PluginController):

    def get_tasks(self):
        pass

    @classmethod
    def get_hybrids(cls):
        return cls._fetch_classes(OVS_DAL_HYBRIDS)

    @classmethod
    def get_lib(cls):
        return cls._fetch_classes(OVS_LIB)

    @classmethod
    def get_webapps(cls):
        for c in cls._fetch_classes(API_VIEWS).itervalues():
            if 'ViewSet' not in [base.__name__ for base in c[1].__bases__]:
                #todo verwijder key uit dict, check of wel dict moet zijn!!
                raise NotImplementedError

    @classmethod
    def get_migration(cls):
        for c in cls._fetch_classes(API_VIEWS).itervalues():
            if 'object' in [base.__name__ for base in c[1].__bases__]:
                #todo verwijder key uit dict, check of wel dict moet zijn!!
                raise NotImplementedError

