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
Statsmonkey module responsible for retrieving certain statistics from the cluster and send them to an Influx DB or Redis DB
Classes: StatsMonkeyController
"""

from ovs.dal.hybrids.storagerouter import StorageRouter
from ovs.dal.hybrids.vpool import VPool
from ovs.dal.lists.servicetypelist import ServiceTypeList
from ovs.dal.lists.storagerouterlist import StorageRouterList
from ovs.dal.lists.vpoollist import VPoolList
from ovs.extensions.generic.logger import Logger
from ovs_extensions.monitoring.statsmonkey import StatsMonkey
from ovs.lib.helpers.decorators import ovs_task
from ovs.lib.helpers.toolbox import Schedule
from ovs.lib.mdsservice import MDSServiceController


class StatsMonkeyController(StatsMonkey):
    """
    Stats Monkey class which retrieves statistics for the cluster
    Methods:
        * run_all
        * get_stats_mds
        * get_stats_vpools
        * get_stats_storagerouters
    """
    _logger = Logger(name='lib')
    _dynamic_dependencies = {'get_stats_vpools': {VPool: ['statistics']},  # The statistics being retrieved depend on the caching timeouts of these properties
                             'get_stats_storagerouters': {StorageRouter: ['statistics']}}

    def __init__(self):
        """
        Init method. This class is a completely static class, so cannot be instantiated
        """
        raise RuntimeError('StatsMonkeyController is a static class')

    @staticmethod
    def run_all():
        """
        Run all the get stats methods from StatsMonkeyController
        Prerequisites when adding content:
            * New methods which need to be picked up by this method need to start with 'get_stats_'
            * New methods need to collect the information and return a bool and list of stats. Then 'run_all_get_stat_methods' method, will send the stats to the configured instance (influx / redis)
            * The frequency each method needs to be executed can be configured via the configuration management by setting the function name as key and the interval in seconds as value
            *    Eg: {'get_stats_mds': 20}  --> Every 20 seconds, the MDS statistics will be checked upon
        """
        StatsMonkeyController.run_all_get_stat_methods()

    @classmethod
    def get_stats_mds(cls):
        """
        Retrieve how many vDisks each MDS service is serving, whether as master or slave
        """
        if cls._config is None:
            cls.validate_and_retrieve_config()

        stats = []
        environment = cls._config['environment']
        service_type = ServiceTypeList.get_by_name('MetadataServer')
        if service_type is None:
            raise RuntimeError('MetadataServer service not found in the model')

        for service in service_type.services:
            slaves = 0
            masters = 0
            mds_service = service.mds_service
            for junction in mds_service.vdisks:
                if junction.is_master is True:
                    masters += 1
                else:
                    slaves += 1
            stats.append({'tags': {'vpool_name': mds_service.vpool.name,
                                   'mds_number': mds_service.number,
                                   'environment': environment,
                                   'storagerouter_name': service.storagerouter.name},
                          'fields': {'load': MDSServiceController.get_mds_load(mds_service)[0],
                                     'capacity': mds_service.capacity if mds_service.capacity != -1 else 'infinite',
                                     'masters': masters,
                                     'slaves': slaves},
                          'measurement': 'mds'})
        return False, stats

    @classmethod
    def get_stats_storagerouters(cls):
        """
        Retrieve amount of vDisks and some read/write statistics for all StorageRouters
        """
        if cls._config is None:
            cls.validate_and_retrieve_config()

        stats = []
        errors = False
        environment = cls._config['environment']
        for storagerouter in StorageRouterList.get_storagerouters():
            if len(storagerouter.storagedrivers) == 0:
                cls._logger.debug('StorageRouter {0} does not have any StorageDrivers linked to it, skipping'.format(storagerouter.name))
                continue
            try:
                statistics = storagerouter.statistics
                stats.append({'tags': {'environment': environment,
                                       'storagerouter_name': storagerouter.name},
                              'fields': {'read_byte': statistics['data_read'],
                                         'write_byte': statistics['data_written'],
                                         'operations': statistics['4k_operations'],
                                         'amount_vdisks': len(storagerouter.vdisks_guids),
                                         'read_operations': statistics['4k_read_operations'],
                                         'write_operations': statistics['4k_write_operations']},
                              'measurement': 'storagerouter'})
            except Exception:
                errors = True
                cls._logger.exception('Retrieving statistics for StorageRouter {0} failed'.format(storagerouter.name))
        return errors, stats

    @classmethod
    def get_stats_vpools(cls):
        """
        Retrieve statistics for each vPool
        """
        if cls._config is None:
            cls.validate_and_retrieve_config()

        stats = []
        errors = False
        environment = cls._config['environment']
        for vpool in VPoolList.get_vpools():
            try:
                stats.append({'tags': {'vpool_name': vpool.name,
                                       'environment': environment},
                              'fields': cls._convert_to_float_values(cls._pop_realtime_info(vpool.statistics)),
                              'measurement': 'vpool'})
            except Exception:
                errors = True
                cls._logger.exception('Retrieving statistics for vPool {0} failed'.format(vpool.name))
        return errors, stats


if __name__ == '__main__':
    StatsMonkeyController.run_all()