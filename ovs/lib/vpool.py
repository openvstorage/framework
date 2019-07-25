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
VPool module
"""

from ovs.dal.lists.storagedriverlist import StorageDriverList
from ovs.lib.helpers.decorators import log, ovs_task
from ovs.lib.helpers.vdisk.rebalancer import VDiskBalance, FailedMovesException

# noinspection PyUnreachableCode
if False:
    from typing import List, Optional


class VPoolController(object):
    """
    Contains all BLL related to VPools
    """

    @staticmethod
    @ovs_task(name='ovs.vpool.up_and_running')
    @log('VOLUMEDRIVER_TASK')
    def up_and_running(storagedriver_id):
        """
        Volumedriver informs us that the service is completely started. Post-start events can be executed
        :param storagedriver_id: ID of the storagedriver
        """
        storagedriver = StorageDriverList.get_by_storagedriver_id(storagedriver_id)
        if storagedriver is None:
            raise RuntimeError('A Storage Driver with id {0} could not be found.'.format(storagedriver_id))
        storagedriver.startup_counter += 1
        storagedriver.save()

    @staticmethod
    @ovs_task(name='ovs.vpool.balance_change')
    def execute_balance_change(vpool_guid, exported_balances, execute_only_for_srs=None):
        # type: (str, List[dict], Optional[List[str]]) -> None
        """
        Execute a balance change. Balances can be calculated through ovs.lib.helpers.vdisk.rebalancer.VDiskRebalancer
        This task is created to offload the balance change to Celery to get concurrency across VPools
        :param vpool_guid: Guid of the VPool to execute the balance changes for. Used for ensure_single and validation
        :type vpool_guid: str
        :param execute_only_for_srs: Guids of StorageRouters to perform the balance change for (if not specified, executed for all)
        :type execute_only_for_srs: Optional[List[str]]
        :param exported_balances: List of exported balances
        :type exported_balances: List[dict]
        :return:
        """
        if execute_only_for_srs is None:
            execute_only_for_srs = []
        balances = [VDiskBalance.from_dict(b) for b in exported_balances]
        if not all(b.storagedriver.vpool_guid == vpool_guid for b in balances):
            raise ValueError("Not all balances are part of the same vpool")
        for balance in balances:  # type: VDiskBalance
            if len(execute_only_for_srs) > 0 and balance.storagedriver.storagerouter_guid in execute_only_for_srs:
                successful_moves, failed_moves = balance.execute_balance_change_through_overflow(balances,
                                                                                                 user_input=False,
                                                                                                 abort_on_error=False)
                if failed_moves:
                    raise FailedMovesException('Could not move volumes {} away'.format(', '.join(failed_moves)))
