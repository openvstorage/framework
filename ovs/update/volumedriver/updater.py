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
from celery import chain, group
from ovs.dal.hybrids.vpool import VPool
from ovs.dal.hybrids.vdisk import VDisk
from ovs.dal.hybrids.storagerouter import StorageRouter
from ovs.dal.hybrids.storagedriver import StorageDriver
from ovs.extensions.generic.system import System
from ovs_extensions.update.base import ComponentUpdater
from ovs.lib.helpers.vdisk.rebalancer import VDiskRebalancer, VDiskBalance
from ovs_extensions.log.logger import Logger
from ovs.extensions.storage.persistentfactory import PersistentFactory
from ovs.lib.mdsservice import MDSServiceController
# noinspection PyUnreachableCode
if False:
    from typing import List, Dict


class VolumeDriverUpdater(ComponentUpdater):

    """
    Responsible for updating the volumedriver of a single node
    """

    LOCAL_SR = System.get_my_storagerouter()
    EDGE_SYNC_TIME = 5 * 60

    @classmethod
    def restart_services(cls):
        """
        Override the service restart. The volumedrivers should be prepared for shutdown
        :return:
        """
        # Get the migration plans for every volume on this host. If there are no plans for certain volumes, it will raise
        balances_by_vpool = cls.get_vpool_balances_for_evacuating_storagerouter(cls.LOCAL_SR)
        # Plan to execute migrate. Avoid the VPool from being an HA target
        cls.mark_storagerouter_unreachable_for_ha(cls.LOCAL_SR)
        try:
            # @todo Go concurrently?
            for vpool, balances in balances_by_vpool.iteritems():
                cls.migrate_away(balances, cls.LOCAL_SR)
            cls.migrate_master_mds(cls.LOCAL_SR)
        finally:
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
        :raises RuntimeError if not all vdisks would be able to move out
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
            raise RuntimeError('Unable to migrate all volumes away from this machine: {}'.format(formatted_errors))
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
        # Mark the storagerouter as excluded for any checkups
        # @todo implement
        pass
        # Checkup to adjust the node distance map
        # todo
        pass
        # Wait for a period of time to let all clients sync up
        # @todo

    @staticmethod
    def mark_storagerouter_reachable_for_ha(storagerouter):
        # type: (StorageRouter) -> None
        """
        Update the node distance map to add the storagerouter back into the HA pool
        :param storagerouter: Storagerouter to put back into the distance map
        :type storagerouter: StorageRouter
        :return: None
        """

    @staticmethod
    def migrate_away(balances, storagerouter):
        # type: (List[VDiskBalance], StorageRouter) -> None
        """
        Migrate all volumes away
        :param balances: List of vdisk balances to execute
        :type balances: List[VDiskBalance]
        :param storagerouter: Storagerouter to move away from
        :type storagerouter: StorageRouter
        :return: None
        """
        evacuate_srs = [storagerouter.guid]
        for balance in balances:  # type: VDiskBalance
            if balance.storagedriver.storagerouter_guid in evacuate_srs:
                successfull_moves, failed_moves = balance.execute_balance_change_through_overflow(balances,
                                                                                                  user_input=False,
                                                                                                  abort_on_error=False)

    @classmethod
    def migrate_master_mds(cls, storagerouter):
        """
        Migrate away all master mds from the given storagerouter
        :param storagerouter: Storagerouter to migrate away from
        :type storagerouter: StorageRouter
        :return: None
        :rtype: NoneType
        """
        all_masters_gone = False
        while not all_masters_gone:
            vpool_mds_master_vdisks = cls.get_vdisks_mds_masters_on_storagerouter(storagerouter)
            all_masters_gone = sum(len(vds) for vds in vpool_mds_master_vdisks.values()) == 0
            chains = []
            for vpool, vdisks in vpool_mds_master_vdisks.iteritems():
                chains.append(chain(MDSServiceController.ensure_safety.si(vdisk.guid) for vdisk in vdisks))
            # Add all chain signatures to a group for parallel execution
            task_group = group(c.s() for c in chains)
            # Wait for the group result
            result = task_group().get()
            print result

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
