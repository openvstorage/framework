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
OVS migration module
"""

import logging
from ovs.constants.logging import UPDATE_LOGGER
from ovs.extensions.packages.packagefactory import PackageFactory


class ExtensionMigrator(object):
    """
    Handles all model related migrations
    """
    identifier = PackageFactory.COMP_MIGRATION_FWK
    THIS_VERSION = 15

    _logger = logging.getLogger(UPDATE_LOGGER)

    def __init__(self):
        """ Init method """
        pass

    @staticmethod
    def migrate_critical():
        """
        This migrate reflects the ovs core related changes in the config management, where raw is removed as parameter of get and set methods.
        Instead, files that are to be interpreted as raw need a suffix of either .ini or .raw. Everything else will be default read and
        interpreted as a JSON file.
        ASD config locations and ovs/arakoon/xxx/config locations are to be read as .raw and .ini, so we migrate these. This change is critical
        because it will totally render the config mgmt useless if not run first, due to changing the ovsdb path.
        :return:
        """
        try:
            from ovs.constants.albanode import ASD_CONFIG, ASD_BASE_PATH
            from ovs_extensions.constants.arakoon import ARAKOON_BASE, ARAKOON_CONFIG
            from ovs.extensions.generic.configuration import Configuration

            def _easy_rename(old_name, new_name):
                try:
                    if not Configuration.exists(new_name):
                        Configuration.rename(old_name, new_name)
                except:
                    ExtensionMigrator._logger.info('Something went wrong during renaming of {0} to {1}'.format(old_name, new_name))

            ExtensionMigrator._logger.info('Critical migrate to the new config management, where raw files have `.raw` or `.ini` extensions')
            for name in Configuration.list(ARAKOON_BASE):
                _easy_rename(ARAKOON_CONFIG.format(name).rstrip('.ini'), ARAKOON_CONFIG.format(name))
            for asd in Configuration.list(ASD_BASE_PATH):
                _easy_rename(ASD_CONFIG.format(asd).rstrip('.raw'), ASD_CONFIG.format(asd))
            ExtensionMigrator._logger.info('Succesfully finished migrating config management')

        except ImportError:
            ExtensionMigrator._logger.info('Arakoon constants file not found, not migrating to new config management')
            pass
        except Exception as ex:
            ExtensionMigrator._logger.info('Unexpected error encountered during migrating config management: {0}'.format(ex))

    @staticmethod
    def migrate(previous_version, master_ips=None, extra_ips=None):
        """
        Migrates from a given version to the current version. It uses 'previous_version' to be smart
        wherever possible, but the code should be able to migrate any version towards the expected version.
        When this is not possible, the code can set a minimum version and raise when it is not met.
        :param previous_version: The previous version from which to start the migration
        :type previous_version: float
        :param master_ips: IP addresses of the MASTER nodes
        :type master_ips: list or None
        :param extra_ips: IP addresses of the EXTRA nodes
        :type extra_ips: list or None
        """

        _ = master_ips, extra_ips
        working_version = previous_version

        # From here on, all actual migration should happen to get to the expected state for THIS RELEASE
        if working_version < ExtensionMigrator.THIS_VERSION:
            ExtensionMigrator.migrate_critical()
            try:
                from ovs.dal.lists.storagerouterlist import StorageRouterList
                from ovs.dal.lists.vpoollist import VPoolList
                from ovs_extensions.constants.vpools import VPOOL_BASE_PATH
                from ovs.extensions.generic.configuration import Configuration
                from ovs.extensions.services.servicefactory import ServiceFactory
                from ovs.extensions.generic.sshclient import SSHClient
                from ovs.extensions.generic.system import System
                local_machine_id = System.get_my_machine_id()
                local_ip = Configuration.get('/ovs/framework/hosts/{0}/ip'.format(local_machine_id))
                local_client = SSHClient(endpoint=local_ip, username='root')

                # Multiple Proxies
                if local_client.dir_exists(directory='/opt/OpenvStorage/config/storagedriver/storagedriver'):
                    local_client.dir_delete(directories=['/opt/OpenvStorage/config/storagedriver/storagedriver'])

                # MDS safety granularity on vPool level
                mds_safety_key = '/ovs/framework/storagedriver'
                if Configuration.exists(key=mds_safety_key):
                    current_mds_settings = Configuration.get(key=mds_safety_key)
                    for vpool in VPoolList.get_vpools():
                        vpool_key = VPOOL_BASE_PATH.format(vpool.guid)
                        if Configuration.dir_exists(key=vpool_key):
                            Configuration.set(key='{0}/mds_config'.format(vpool_key),
                                              value=current_mds_settings)
                    Configuration.delete(key=mds_safety_key)

                # Introduction of edition key
                if Configuration.get(key=Configuration.EDITION_KEY, default=None) not in [PackageFactory.EDITION_COMMUNITY, PackageFactory.EDITION_ENTERPRISE]:
                    for storagerouter in StorageRouterList.get_storagerouters():
                        try:
                            Configuration.set(key=Configuration.EDITION_KEY, value=storagerouter.features['alba']['edition'])
                            break
                        except:
                            continue

            except:
                ExtensionMigrator._logger.exception('Error occurred while executing the migration code')
                # Don't update migration version with latest version, resulting in next migration trying again to execute this code
                return ExtensionMigrator.THIS_VERSION - 1

        return ExtensionMigrator.THIS_VERSION
