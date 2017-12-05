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
Service Factory for the OVS Framework
"""

from ovs.dal.hybrids.diskpartition import DiskPartition
from ovs.extensions.generic.configuration import Configuration
from ovs.extensions.generic.logger import Logger
from ovs.extensions.generic.system import System
from ovs.extensions.packages.packagefactory import PackageFactory
from ovs_extensions.services.servicefactory import ServiceFactory as _ServiceFactory


class ServiceFactory(_ServiceFactory):
    """
    Service Factory for the OVS Framework
    """
    RUN_FILE_DIR = '/opt/OpenvStorage/run'
    CONFIG_TEMPLATE_DIR = '/opt/OpenvStorage/config/templates/{0}'
    MONITOR_PREFIXES = ['ovs-']
    SERVICE_CONFIG_KEY = '/ovs/framework/hosts/{0}/services/{1}'
    SERVICE_WATCHER_VOLDRV = 'watcher-volumedriver'

    _logger = Logger('extensions')

    def __init__(self):
        """Init method"""
        raise Exception('This class cannot be instantiated')

    @classmethod
    def _get_system(cls):
        return System

    @classmethod
    def _get_configuration(cls):
        return Configuration

    @classmethod
    def _get_logger_instance(cls):
        return cls._logger

    @classmethod
    def get_services_with_version_files(cls, storagerouter):
        """
        Retrieve the services which have a version file in RUN_FILE_DIR on the specified StorageRouter
        This takes the components into account defined in the PackageFactory for this repository
        :param storagerouter: The StorageRouter for which to retrieve the services with a version file
        :type storagerouter: ovs.dal.hybrids.storagerouter.StorageRouter
        :return: Services split up by component and related package with a version file
                 {<component>: <pkg_name>: {10: [<service1>, <service2>], 20: [<service3>] } } }
        :rtype: dict
        """
        from ovs.extensions.db.arakooninstaller import ArakoonInstaller  # Import here to prevent from circular references

        # Retrieve Arakoon information
        arakoons = {}
        if storagerouter.node_type == 'MASTER' and DiskPartition.ROLES.DB in storagerouter.partition_config:
            for cluster, component in {'cacc': PackageFactory.COMP_FWK,
                                       'ovsdb': PackageFactory.COMP_FWK,
                                       'voldrv': PackageFactory.COMP_SD}.iteritems():
                update_info = ArakoonInstaller.get_arakoon_update_info(internal_cluster_name=cluster, ip=storagerouter.ip if cluster == 'cacc' else None)
                if update_info['internal'] is True:
                    if component not in arakoons:
                        arakoons[component] = []
                    arakoons[component].append(update_info['service_name'])

        # Retrieve StorageDriver services
        storagedriver_services = {10: [], 20: []}  # Keys are the weight given to the services for restart order. The lower, the sooner they get restarted
        for sd in storagerouter.storagedrivers:
            storagedriver_services[20].append('dtl_{0}'.format(sd.vpool.name))
            storagedriver_services[10].append('volumedriver_{0}'.format(sd.vpool.name))

        # Retrieve the services which might require a restart
        service_info = {}
        for component, package_names in PackageFactory.get_package_info()['names'].iteritems():
            service_info[component] = {}
            for package_name in package_names:
                if package_name == PackageFactory.PKG_ARAKOON:
                    services = {10: arakoons.get(component, [])}  # 10 is the priority for restart, the lower the sooner they get restarted
                elif package_name in [PackageFactory.PKG_VOLDRV_BASE, PackageFactory.PKG_VOLDRV_BASE_EE,
                                      PackageFactory.PKG_VOLDRV_SERVER, PackageFactory.PKG_VOLDRV_SERVER_EE]:
                    services = storagedriver_services
                else:
                    services = {}
                service_info[component][package_name] = services
        return service_info
