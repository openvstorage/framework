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

import os
import time
import itertools
from ovs.celery_run import celery
from celery import chain, group
from celery.exceptions import TimeoutError
from ovs.constants.vpool import VPOOL_UPDATE_KEY, STORAGEDRIVER_SERVICE_BASE, VOLUMEDRIVER_BIN_PATH, VOLUMEDRIVER_CMD_NAME, PACKAGES_EE
from ovs.extensions.generic.configuration import Configuration
from ovs.dal.hybrids.vpool import VPool
from ovs.dal.hybrids.vdisk import VDisk
from ovs.dal.hybrids.storagerouter import StorageRouter
from ovs.extensions.generic.system import System
from ovs_extensions.update.base import ComponentUpdater, UpdateException
from ovs.lib.helpers.vdisk.rebalancer import VDiskRebalancer, VDiskBalance
from ovs.lib.storagedriver import StorageDriverController
from ovs.extensions.storage.persistentfactory import PersistentFactory
from ovs.lib.mdsservice import MDSServiceController
from ovs.lib.vpool import VPoolController
from ovs.log.log_handler import LogHandler

# noinspection PyUnreachableCode
if False:
    from typing import List, Dict, Tuple, Optional


class FailedToMigrateException(UpdateException):
    """
    Thrown when not all volumes would be able to move away
    """
    exit_code = 21


class FailureDuringMigrateException(UpdateException):
    """
    Thrown when certain volumes failed to move away
    """
    exit_code = 22


class LocalMastersRemaining(RuntimeError):
    """
    Thrown when local masters are still present on the machine
    """


class VolumeDriverUpdater(ComponentUpdater):

    """
    Responsible for updating the volumedriver of a single node
    """

    logger = LogHandler.get('update', 'volumedriver')

    COMPONENT = 'volumedriver'
    # List with tuples. [(package_name, binary_name, binary_location, [service_prefix_0]]
    BINARIES = [(PACKAGES_EE, VOLUMEDRIVER_CMD_NAME, VOLUMEDRIVER_BIN_PATH, [STORAGEDRIVER_SERVICE_BASE])] # type: List[Tuple[List[str], str, str, List[str]]]
    LOCAL_SR = System.get_my_storagerouter()
    EDGE_SYNC_TIME = 5 * 60

    @classmethod
    def restart_services(cls):
        """
        Override the service restart. The volumedrivers should be prepared for shutdown
        """
        cls.logger.info("Preparing to restart the related services")
        initial_run_steps = True
        try:
            run_number = 0
            while True:
                cls.logger.info('Attempt {0} to prepare the restart'.format(run_number))
                # Get the migration plans for every volume on this host. If there are no plans for certain volumes, it will raise
                balances_by_vpool = cls.get_vpool_balances_for_evacuating_storagerouter(cls.LOCAL_SR)
                if initial_run_steps:
                    cls.mark_storagerouter_unreachable_for_ha(cls.LOCAL_SR)
                    initial_run_steps = False
                try:
                    cls.migrate_away(balances_by_vpool, cls.LOCAL_SR)
                    cls.migrate_master_mds(cls.LOCAL_SR)
                    all_prefixes = tuple(itertools.chain.from_iterable(b[3] for b in cls.BINARIES))
                    cls.logger.info("Restarting all related services")
                    return cls.restart_services_by_prefixes(all_prefixes)
                except LocalMastersRemaining:
                    # Swallow and retry
                    cls.logger.warning('Local masters still found on the machine. Will try to migrate them away')
                run_number += 1
        finally:
            if not initial_run_steps:
                cls.mark_storagerouter_reachable_for_ha(cls.LOCAL_SR)

    @staticmethod
    def get_vpool_balances_for_evacuating_storagerouter(storagerouter):
        # type: (StorageRouter) -> Dict[VPool, List[VDiskBalance]]
        """
        Retrieve the balances for every vpool on the local machine
        :param storagerouter: Storagerouter to migrate away from
        :type storagerouter: StorageRouter
        :return: Dict with vpool and balances
        :rtype: Dict[VPool, VDiskBalance]
        :raises FailedToMigrateException if not all vdisks would be able to move out
        """
        errors = []
        evacuate_srs = [storagerouter.guid]
        balances_by_vpool = {}
        for storagedriver in storagerouter.storagedrivers:
            vpool = storagedriver.vpool
            try:
                balances = VDiskRebalancer.get_rebalanced_layout(storagedriver.vpool_guid,
                                                                 ignore_domains=False,
                                                                 excluded_storagerouters=None,
                                                                 evacuate_storagerouters=evacuate_srs,
                                                                 base_on_volume_potential=True)
                balances_sorted = sorted(balances, key=lambda b: b.storagedriver.storagerouter_guid in evacuate_srs,
                                         reverse=True)
                balances_by_vpool[vpool] = balances_sorted
            except Exception as ex:
                errors.append((vpool, ex))
        if errors:
            formatted_errors = '\n - {0}'.format('\n - '.join('VPool {0}: {1}'.format(vpool.name, error) for vpool, error in errors))
            raise FailedToMigrateException('Unable to migrate all volumes away from this machine: {}'.format(formatted_errors))
        return balances_by_vpool

    @classmethod
    def mark_storagerouter_unreachable_for_ha(cls, storagerouter):
        """
        Update the node distance maps to
        Current code paths that update the node distance map on the volumedriver side are:
        - Update of domains
        - Update of vpool layout (extend/shrink)
        - cluster registry checkup (ran periodically)
        :return: None
        :rtype: NoneType
        """
        cls.logger.info("Marking Storagerouter {} as unavailable for HA".format(storagerouter.name))
        # Set the value used in the storagedriver cluster node config path
        # This holds for all mentioned paths in the docstrings
        Configuration.set(os.path.join(VPOOL_UPDATE_KEY, storagerouter.guid), 0)
        # Trigger a complete reload of node distance maps
        StorageDriverController.cluster_registry_checkup()
        # Wait a few moment for the edge to catch up all the configs
        sleep_time = cls.get_edge_sync_time()
        cls.logger.info("Waiting {} to sync up all edge clients".format(sleep_time))
        time.sleep(sleep_time)

    @classmethod
    def mark_storagerouter_reachable_for_ha(cls, storagerouter):
        # type: (StorageRouter) -> None
        """
        Update the node distance map to add the storagerouter back into the HA pool
        :param storagerouter: Storagerouter to put back into the distance map
        :type storagerouter: StorageRouter
        :return: None
        """
        cls.logger.info("Marking Storagerouter {} as available for HA".format(storagerouter.name))
        Configuration.delete(os.path.join(VPOOL_UPDATE_KEY, storagerouter.guid))
        # Trigger a complete reload of node distance maps
        StorageDriverController.cluster_registry_checkup()
        # Wait a few moment for the edge to catch up all the configs
        sleep_time = cls.get_edge_sync_time()
        cls.logger.info("Waiting {} to sync up all edge clients".format(sleep_time))
        time.sleep(sleep_time)

    @classmethod
    def migrate_away(cls, balances_by_vpool, storagerouter):
        # type: (Dict[VPool, List[VDiskBalance]], StorageRouter) -> None
        """
        Migrate all volumes away
        :param balances_by_vpool: Dict with VPool as key and List of vdisk balances to execute
        :type balances_by_vpool: Dict[VPool, List[VDiskBalance]]
        :param storagerouter: Storagerouter to move away from
        :type storagerouter: StorageRouter
        :return: None
        :raises: FailureDuringMigrateException if any volumes failed to move
        """
        evacuate_srs = [storagerouter.guid]
        for vpool, balances in balances_by_vpool.iteritems():
            for balance in balances:  # type: VDiskBalance
                if balance.storagedriver.storagerouter_guid in evacuate_srs:
                    successfull_moves, failed_moves = balance.execute_balance_change_through_overflow(balances,
                                                                                                      user_input=False,
                                                                                                      abort_on_error=False)
                    if failed_moves:
                        raise FailureDuringMigrateException('Could not move volumes {} away'.format(', '.join(failed_moves)))

    @classmethod
    def migrate_master_mds(cls, storagerouter, max_chain_size=100, group_timeout=10 * 60):
        # type: (StorageRouter, Optional[int], Optional[int]) -> None
        """
        Migrate away all master mds from the given storagerouter
        :param storagerouter: Storagerouter to migrate away from
        :type storagerouter: StorageRouter
        :param max_chain_size: Maximum number of tasks within a chain. Set because https://github.com/celery/celery/issues/1078
        :type max_chain_size: int
        :param group_timeout: Timeout for the complete group. Will abort all pending tasks afterwards. Defaults to 10 mins
        :type group_timeout: int
        :return: None
        :rtype: NoneType
        """
        cls.logger.info("Starting MDS migrations")
        while True:
            vpool_mds_master_vdisks = cls.get_vdisks_mds_masters_on_storagerouter(storagerouter)
            all_masters_gone = sum(len(vds) for vds in vpool_mds_master_vdisks.values()) == 0
            if all_masters_gone:
                break
            chains = []
            for vpool_guid, vdisk_guids in vpool_mds_master_vdisks.iteritems():
                signatures = []
                tasks = []
                for vdisk_guid in vdisk_guids[0:max_chain_size]:
                    cls.logger.info('Ensuring safety for {}'.format(vdisk_guid))
                    signature = MDSServiceController.ensure_safety.si(vdisk_guid)
                    # Freeze freezes the task into its final form. This will net the async result object we'd normally get from delaying it
                    tasks.append(signature.freeze())
                    signatures.append(signature)
                if signatures:
                    cls.logger.info('Adding chain for VPool {} with tasks {}'.format(vpool_guid, ', '.join(t.id for t in tasks)))
                    chains.append(chain(signatures))
            # Add all chain signatures to a group for parallel execution
            task_group = group(chains)
            # Wait for the group result
            async_result = task_group.apply_async()
            cls.logger.info('Waiting for all tasks of group {}'.format(async_result.id))
            _ = async_result.get()
        cls.logger.info("MDS migration finished")

    @staticmethod
    def get_vdisks_mds_masters_on_storagerouter(storagerouter):
        # type: (StorageRouter) -> Dict[VPool, List[VDisk]]
        """
        Retrieve all vdisks with the MDS master on the given storagerouter
        :param storagerouter: Storagerouter to list MDS masters on
        :type storagerouter: StorageRouter
        :return: Dict with VPool as key and vdisks with the MDS master on the storagerouter as value
        :rtype: Dict[VPool, List[VDisk]
        """
        mds_masters = {}
        vpools = set(sd.vpool for sd in storagerouter.storagedrivers)
        for vpool in sorted(vpools, key=lambda k: k.name):
            masters = []
            for mds_service in sorted(vpool.mds_services, key=lambda k: k.number):
                if mds_service.service.storagerouter_guid == storagerouter.guid:
                    for junction in mds_service.vdisks:
                        if junction.is_master:
                            masters.append(junction.vdisk_guid)
            mds_masters[vpool.name] = masters
        return mds_masters

    @staticmethod
    def get_persistent_client():
        return PersistentFactory.get_client()

    @classmethod
    def get_edge_sync_time(cls):
        # type: () -> int
        """
        Get the time required for all edge clients to do a complete sync
        :return: Time for a complete edge sync
        :rtype: int
        """
        return 2 * cls.EDGE_SYNC_TIME
