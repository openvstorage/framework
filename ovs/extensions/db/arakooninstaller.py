# Copyright (C) 2017 iNuron NV
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
Generic module for managing configuration somewhere
"""
from ovs_extensions.db.arakoon.arakooninstaller import ArakoonClusterConfig as _ArakoonClusterConfig, ArakoonInstaller as _ArakoonInstaller
from ovs.extensions.generic.configuration import Configuration
from ovs.extensions.generic.system import System
from ovs.extensions.generic.volatilemutex import volatile_mutex
from ovs.extensions.services.servicefactory import ServiceFactory


class ArakoonClusterConfig(_ArakoonClusterConfig):
    """
    Extends the 'default' ArakoonClusterConfig
    """

    def __init__(self, *args, **kwargs):
        super(ArakoonClusterConfig, self).__init__(*args, **kwargs)

    @classmethod
    def _get_configuration(cls):
        return Configuration


class ArakoonInstaller(_ArakoonInstaller):
    """
    Class to dynamically install/(re)configure Arakoon cluster
    """

    def __init__(self, *args, **kwargs):
        super(ArakoonInstaller, self).__init__(*args, **kwargs)

    @classmethod
    def _get_configuration(cls):
        return Configuration

    @classmethod
    def _get_service_manager(cls):
        return ServiceFactory.get_manager()

    @classmethod
    def _get_system(cls):
        return System

    @classmethod
    def _get_volatile_mutex(cls):
        return volatile_mutex
