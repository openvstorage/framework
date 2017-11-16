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
Package Factory module
"""

from ovs_extensions.db.arakoon.pyrakoon.pyrakoon.compat import ArakoonException
from ovs.extensions.generic.configuration import Configuration
from ovs.extensions.generic.logger import Logger
from ovs_extensions.packages.packagefactory import PackageFactory as _PackageFactory


class PackageFactory(_PackageFactory):
    """
    Factory class returning specialized classes
    """
    _logger = Logger('extensions-packages')

    SUPPORTED_COMPONENTS = {_PackageFactory.COMP_FWK, _PackageFactory.COMP_SD}

    def __init__(self):
        """
        Initialization method
        """
        super(PackageFactory, self).__init__()

    @classmethod
    def get_package_info(cls):
        """
        Retrieve the package information related to the framework
        This must return a dictionary with keys: 'names', 'edition', 'binaries', 'blocking', 'version_commands' and 'mutually_exclusive'
            Names: These are the names of the packages split up per component related to this repository (framework)
                * Framework
                    * PKG_ARAKOON            --> Used for arakoon-config cluster and arakoon-ovsdb cluster
                    * PKG_OVS                --> Code itself ... duh
                    * PKG_OVS_EXTENSIONS     --> Extensions code is used by the framework repository
                * StorageDriver
                    * PKG_ARAKOON            --> StorageDrivers make use of the arakoon-voldrv cluster
                    * PKG_VOLDRV_BASE(_EE)   --> Code for StorageDriver itself
                    * PKG_VOLDRV_SERVER(_EE) --> Code for StorageDriver itself
            Edition: Used for different purposes
            Binaries: The names of the packages that come with a binary (also split up per component)
            Blocking: Update will stop (block) in case the update fails for any of these packages
            Version_commands: The commandos used to determine which binary version is currently active
            Mutually_exclusive: Packages which are not allowed to be installed depending on the edition. Eg: ALBA_EE cannot be installed on a 'community' edition
        :raises ValueError: * When configuration management key 'ovs/framework/edition' does not exists
                            * When 'ovs/framework/edition' does not contain 'community' or 'enterprise'
        :return: A dictionary containing information about the expected packages to be installed
        :rtype: dict
        """
        edition_key = '/ovs/framework/edition'
        try:
            if Configuration.exists(key=edition_key) is False:
                raise ValueError('Edition configuration key "{0}" does not exist'.format(edition_key))
            edition = str(Configuration.get(key=edition_key))
        except ArakoonException:
            cls._logger.exception('Unable to connect to the configuration Arakoon. Returning all packages/binaries')
            return {'names': {cls.COMP_FWK: {cls.PKG_ARAKOON, cls.PKG_OVS, cls.PKG_OVS_EXTENSIONS},
                              cls.COMP_SD: {cls.PKG_ARAKOON, cls.PKG_VOLDRV_BASE, cls.PKG_VOLDRV_SERVER, cls.PKG_VOLDRV_BASE_EE, cls.PKG_VOLDRV_SERVER_EE}},
                    'edition': '',
                    'binaries': {cls.COMP_FWK: {cls.PKG_ARAKOON},
                                 cls.COMP_SD: {cls.PKG_ARAKOON, cls.PKG_VOLDRV_SERVER, cls.PKG_VOLDRV_SERVER_EE}},
                    'blocking': {cls.PKG_OVS, cls.PKG_OVS_EXTENSIONS},
                    'version_commands': {cls.PKG_ARAKOON: cls.VERSION_CMD_ARAKOON,
                                         cls.PKG_VOLDRV_BASE: cls.VERSION_CMD_SD,
                                         cls.PKG_VOLDRV_SERVER: cls.VERSION_CMD_SD,
                                         cls.PKG_VOLDRV_BASE_EE: cls.VERSION_CMD_SD,
                                         cls.PKG_VOLDRV_SERVER_EE: cls.VERSION_CMD_SD},
                    'mutually_exclusive': set()}

        if edition == 'community':
            return {'names': {cls.COMP_FWK: {cls.PKG_ARAKOON, cls.PKG_OVS, cls.PKG_OVS_EXTENSIONS},
                              cls.COMP_SD: {cls.PKG_ARAKOON, cls.PKG_VOLDRV_BASE, cls.PKG_VOLDRV_SERVER}},
                    'edition': edition,
                    'binaries': {cls.COMP_FWK: {cls.PKG_ARAKOON},
                                 cls.COMP_SD: {cls.PKG_ARAKOON, cls.PKG_VOLDRV_SERVER}},
                    'blocking': {cls.PKG_OVS, cls.PKG_OVS_EXTENSIONS},
                    'version_commands': {cls.PKG_ARAKOON: cls.VERSION_CMD_ARAKOON,
                                         cls.PKG_VOLDRV_BASE: cls.VERSION_CMD_SD,
                                         cls.PKG_VOLDRV_SERVER: cls.VERSION_CMD_SD},
                    'mutually_exclusive': {cls.PKG_VOLDRV_BASE_EE, cls.PKG_VOLDRV_SERVER_EE}}
        elif edition == 'enterprise':
            return {'names': {cls.COMP_FWK: {cls.PKG_ARAKOON, cls.PKG_OVS, cls.PKG_OVS_EXTENSIONS},
                              cls.COMP_SD: {cls.PKG_ARAKOON, cls.PKG_VOLDRV_BASE_EE, cls.PKG_VOLDRV_SERVER_EE}},
                    'edition': edition,
                    'binaries': {cls.COMP_FWK: {cls.PKG_ARAKOON},
                                 cls.COMP_SD: {cls.PKG_ARAKOON, cls.PKG_VOLDRV_SERVER_EE}},
                    'blocking': {cls.PKG_OVS, cls.PKG_OVS_EXTENSIONS},
                    'version_commands': {cls.PKG_ARAKOON: cls.VERSION_CMD_ARAKOON,
                                         cls.PKG_VOLDRV_BASE_EE: cls.VERSION_CMD_SD,
                                         cls.PKG_VOLDRV_SERVER_EE: cls.VERSION_CMD_SD},
                    'mutually_exclusive': {cls.PKG_VOLDRV_BASE, cls.PKG_VOLDRV_SERVER}}
        else:
            raise ValueError('Unsupported edition found in "{0}"'.format(edition_key))
