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
Plugincontroller class module
"""

from ovs_extensions.constants.modules import OVS_DAL_HYBRIDS, OVS_LIB, API_VIEWS, OVS_DAL_MIGRATION, OVS_LIB_HELPERS, RABBIT_MQ_MAPPINGS
from ovs_extensions.generic.plugin import PluginController as _PluginController


class PluginController(_PluginController):
    """
    Plugincontroller to fetch ovs core classes
    """

    def __init__(self):
        pass

    @classmethod
    def get_hybrids(cls):
        # type: () -> List[ovs.dal.dataobject.MetaClass]
        """
        Fetch the hybrids module in the given folder.
        :return: list with hybrid DAL DataObjects
        """
        from ovs.dal.dataobject import DataObject  # Circumvent circular dependencies
        return cls._fetch_classes(OVS_DAL_HYBRIDS, filter_class=DataObject)

    @classmethod
    def get_lib(cls):
        # type: () -> List[type]
        """
        Fetch the controllers in the lib module
        :return: List of controller objects
        """
        return cls._fetch_classes(OVS_LIB)

    @classmethod
    def get_lib_helpers(cls):
        # type: () -> List[type]
        """
        Fetch lib helper objects
        :return: List of these helper objects
        """
        return cls._fetch_classes(OVS_LIB_HELPERS)

    @classmethod
    def get_webapps(cls):
        # type: () -> List[type]
        """
        Fetch webapp viewsets
        :return: List with djano viewset objects
        """
        return [c for c in cls._fetch_classes(API_VIEWS) if 'ViewSet' in [base.__name__ for base in c.__bases__]]

    @classmethod
    def get_migration(cls):
        # type: () -> List[type]
        """
        Fetch ovs migration objects
        :return: List of these migration objects
        """
        return cls._fetch_classes(OVS_DAL_MIGRATION, filter_class=object)

    @classmethod
    def get_rabbitmq_mapping(cls):
        # type: () -> List[type]
        """
        Fetch rabbitmq mapping objects
        :return: List of these mapping objects
        """
        return cls._fetch_classes(RABBIT_MQ_MAPPINGS, filter_class=object)