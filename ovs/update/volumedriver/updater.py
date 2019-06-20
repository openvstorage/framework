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

from ovs.dal.hybrids.vpool import VPool
from ovs.dal.hybrids.vdisk import VDisk
from ovs.dal.hybrids.storagerouter import StorageRouter
from ovs.dal.hybrids.storagedriver import StorageDriver
from ovs.extensions.generic.system import System
from ovs_extensions.update.base import ComponentUpdater
from ovs.lib.helpers.vdisk.rebalancer import VDiskRebalancer, VDiskBalance
from ovs_extensions.log.logger import Logger

# noinspection PyUnreachableCode
if False:
    from typing import List, Dict


class VolumeDriverUpdater(ComponentUpdater):

    """
    Responsible for updating the volumedriver of a single node
    """

    LOCAL_SR = System.get_my_storagerouter()

    @classmethod
    def restart_services(cls):
        """
        Override the service restart. The volumedrivers should be prepared for shutdown
        :return:
        """
        # Get the migration plans for every volume on this host. If there are no plans for certain volumes, it will raise
        balances_by_vpool = cls.get_vpool_balances_for_evacuating_storagerouter(cls.LOCAL_SR)
        # Plan to execute migrate. Avoid the VPool from being an HA target
        cls.make_sr_unreachable_for_ha(cls.LOCAL_SR)

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
    def make_sr_unreachable_for_ha(cls, storagerouter):
        """
        Update the node distance maps to
        :return: None
        :rtype: NoneType
        """

    @classmethod
    def migrate_awy(cls, storagedriver):
        """
        Migrate all volumes away
        :param storagedriver:
        :return:
        """
