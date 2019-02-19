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

import logging
from ovs.extensions.generic.configuration import Configuration
from ovs_extensions.packages.packagefactory import PackageFactory as _PackageFactory


class PackageFactory(_PackageFactory):
    """
    Factory class returning specialized classes
    """
    # Allow the user to add a custom release name
    RELEASE_NAME_FILE = '/opt/OpenvStorage/config/release_name'
    _logger = logging.getLogger(__name__)

    def __init__(self):
        """
        Initialization method
        """
        super(PackageFactory, self).__init__()

    @classmethod
    def get_components(cls):
        """
        Retrieve the components which relate to this repository
        :return: A set of components
        :rtype: set
        """
        return {cls.COMP_FWK, cls.COMP_SD}

    @classmethod
    def get_package_info(cls):
        """
        Retrieve the package information related to the framework
        This must return a dictionary with keys: 'names', 'edition', 'binaries', 'non_blocking', 'version_commands' and 'mutually_exclusive'
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
            Non Blocking: Packages which are potentially not yet available on all releases. These should be removed once every release contains these packages by default
            Version Commands: The commandos used to determine which binary version is currently active
            Mutually Exclusive: Packages which are not allowed to be installed depending on the edition. Eg: ALBA_EE cannot be installed on a 'community' edition
        :return: A dictionary containing information about the expected packages to be installed
        :rtype: dict
        """
        edition = Configuration.get_edition()
        if edition == cls.EDITION_COMMUNITY:
            return {'names': {cls.COMP_FWK: {cls.PKG_ARAKOON, cls.PKG_OVS, cls.PKG_OVS_EXTENSIONS},
                              cls.COMP_SD: {cls.PKG_ARAKOON, cls.PKG_VOLDRV_BASE, cls.PKG_VOLDRV_SERVER}},
                    'edition': edition,
                    'binaries': {cls.COMP_FWK: {cls.PKG_ARAKOON},
                                 cls.COMP_SD: {cls.PKG_ARAKOON, cls.PKG_VOLDRV_SERVER}},
                    'non_blocking': {cls.PKG_OVS_EXTENSIONS},
                    'version_commands': {cls.PKG_ARAKOON: cls.VERSION_CMD_ARAKOON,
                                         cls.PKG_VOLDRV_BASE: cls.VERSION_CMD_SD,
                                         cls.PKG_VOLDRV_SERVER: cls.VERSION_CMD_SD},
                    'mutually_exclusive': {cls.PKG_VOLDRV_BASE_EE, cls.PKG_VOLDRV_SERVER_EE}}
        elif edition == cls.EDITION_ENTERPRISE:
            return {'names': {cls.COMP_FWK: {cls.PKG_ARAKOON, cls.PKG_OVS, cls.PKG_OVS_EXTENSIONS},
                              cls.COMP_SD: {cls.PKG_ARAKOON, cls.PKG_VOLDRV_BASE_EE, cls.PKG_VOLDRV_SERVER_EE}},
                    'edition': edition,
                    'binaries': {cls.COMP_FWK: {cls.PKG_ARAKOON},
                                 cls.COMP_SD: {cls.PKG_ARAKOON, cls.PKG_VOLDRV_SERVER_EE}},
                    'non_blocking': {cls.PKG_OVS_EXTENSIONS},
                    'version_commands': {cls.PKG_ARAKOON: cls.VERSION_CMD_ARAKOON,
                                         cls.PKG_VOLDRV_BASE_EE: cls.VERSION_CMD_SD,
                                         cls.PKG_VOLDRV_SERVER_EE: cls.VERSION_CMD_SD},
                    'mutually_exclusive': {cls.PKG_VOLDRV_BASE, cls.PKG_VOLDRV_SERVER}}
        else:
            raise ValueError('Unsupported edition found: "{0}"'.format(edition))

    @classmethod
    def get_release_name(cls, client=None):
        """
        Retrieve the release name
        :param client: Client on which to retrieve the release name
        :type client: ovs_extensions.generic.sshclient.SSHClient
        :return: The name of the release
        :rtype: str
        """
        try:
            if client is not None:
                return client.run(['cat', cls.RELEASE_NAME_FILE]).strip()
            with open(cls.RELEASE_NAME_FILE, 'r') as the_file:
                return the_file.read().strip()
        except:
            manager = cls.get_manager()
            return manager.get_release_name()
