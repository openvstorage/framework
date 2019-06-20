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

from ovs.dal.hybrids.vdisk import VDisk
from ovs.dal.hybrids.storagerouter import StorageRouter
from ovs.dal.hybrids.storagedriver import StorageDriver
from ovs.extensions.generic.system import System
from ovs_extensions.update.base import ComponentUpdater
from ovs_extensions.log.logger import Logger

# noinspection PyUnreachableCode
if False:
    from typing import List


class VolumeDriverUpdater(ComponentUpdater):

    """
    Responsible for updating the volumedriver of a single node
    """

    def restart_services(cls):
        """
        Override the service restart. The volumedrivers should be prepared for shutdown
        :return:
        """

    @classmethod
    def can_migrate_storagerouter(cls, storagerouter):
        """
        Determine if all volumedrivers on the storagerouter can be migrated away
        :param storagerouter:
        :return:
        """

    @classmethod
    def can_migrate_away(cls, storagedriver):
        """
        Determine if all volumes of the storagedriver can be migrated away
        :param storagedriver:
        :return:
        """

    @classmethod
    def migrate_awy(cls, storagedriver):
        """
        Migrate all volumes away
        :param storagedriver:
        :return:
        """


class VDiskBalance(object):

    logger = Logger('vdisk_balance')

    def __init__(self, storagedriver, vdisk_limit):
        # type: (StorageDriver, int) -> None
        """
        Represents the vdisk balance of a storagedriver
        :param storagedriver: StorageDriver to balance for
        :type storagedriver: StorageDriver
        :param vdisk_limit: Maximum amount of vdisks to host. -1 means no limit
        :type vdisk_limit: int
        """
        self.storagedriver = storagedriver
        self.hosted_guids = storagedriver.vdisks_guids
        self.limit = vdisk_limit

        self.balance, self.overflow = self.impose_limit()
        self.added = []

    def __add__(self, other):
        if not isinstance(other, VDiskBalance) or self.storagedriver != other.storagedriver:
            raise ValueError('Different objects cannot be added')
        limit = self.limit + other.limit
        self.set_limit(limit)
        self.added += other.added

    def set_limit(self, limit):
        """
        Set a new limit
        :param limit: Limit to set
        :return: The guids of vdisks that can fit and the guids that cannot fit in on the current host
        :rtype: Tuple(List[str], List[str])
        """
        self.limit = limit
        self.balance, self.overflow = self.impose_limit()
        return self.balance, self.overflow

    def impose_limit(self):
        # type: () -> Tuple[List[str], List[str]]
        """
        Impose the set limit. Returns the max amount of vdisks that can be hosted and the vdisks that need to go
        :return: The guids of vdisks that can fit and the guids that cannot fit in on the current host
        :rtype: Tuple(List[str], List[str])
        """
        if self.limit == -1:
            return self.hosted_guids, []
        overflow = self.hosted_guids[self.limit:]
        balance = self.hosted_guids[:self.limit]
        return balance, overflow

    def fill(self, vdisk_guids):
        # type: (List[str]) -> Tuple[List[str], List[str]]
        """
        Fill this balance until the limit is reached
        :param vdisk_guids: Guids to add
        :type vdisk_guids: List[str]
        :return: The guids that could be added to this balanced and the guids that couldn't be added
        :rtype: Tuple[List[str], List[str]]
        """
        amount_to_add = self.limit - len(self.balance)
        added = []
        overflow = vdisk_guids
        if amount_to_add:
            added = vdisk_guids[:amount_to_add]
            overflow = vdisk_guids[amount_to_add:]
        self.balance.extend(added)
        self.added.extend(added)
        return added, overflow

    def generate_overview(self):
        # type: () -> dict
        """
        Generate the move overview depending on the current state
        :return: The overview from where the disks are coming from
        :rtype: dict
        """
        added_source_overview = {}
        for vdisk_guid in self.added:
            storagedriver_id = VDisk(vdisk_guid).storagedriver_id
            if storagedriver_id not in added_source_overview:
                added_source_overview[storagedriver_id] = []
            added_source_overview[storagedriver_id].append(vdisk_guid)
        overview = {'added': self.added,
                    'balance': self.balance,
                    'overflow': self.overflow,
                    'add_source_overview': added_source_overview}
        return overview

    def execute_balance_change(self, force=False, user_input=False, abort_on_error=False):
        # type: (Optional[bool], Optional[bool], Optional[bool]) -> Tuple[List[str], List[str]]
        """
        Execute the necessary steps to balance out
        :param force: Indicates whether to force the migration or not (forcing can lead to data loss)
        :type force: bool
        :param user_input: require user input to proceed to next vDisk
        :type user_input: bool
        :param abort_on_error: Abort script when error occurs during migration
        :type abort_on_error: bool
        :return: List with all successful moves, list with all failed moves
        :rtype: NoneType
        """
        failed_moves = []
        successful_moves = []
        vdisk_guid = None
        try:
            for vdisk_guid in self.added:
                try:
                    self._execute_move(vdisk_guid, self.storagedriver, force, user_input)
                    successful_moves.append(vdisk_guid)
                except:
                    self.logger.exception('Unable to move VDisk {0} to {1}'.format(vdisk_guid, self.storagedriver.storagerouter_guid))
                    if abort_on_error:
                        raise RuntimeError("Something went wrong during moving VDisk {0} to {1}".format(vdisk_guid, self.storagedriver.storagerouter_guid))
                    failed_moves.append(vdisk_guid)
        except KeyboardInterrupt:
            interrupt_msg = 'You have interrupted while moving vdisks. The last move (vDisk {0}) might be in an inconsistent state.'.format(vdisk_guid)
            self.logger.warning(interrupt_msg)
            if user_input:
                if successful_moves:
                    print('Succesfully moved vDisks: \n {0}'.format(', '.join(successful_moves)))
                if failed_moves:
                    print('\nFailed to move vDisks:\n {0}'.format(', '.join(failed_moves)))
            raise

        return successful_moves, failed_moves

    def execute_balance_change_through_overflow(self, balances, force=False, user_input=False, abort_on_error=False):
        # type: (List[VDiskBalance], bool, bool, bool) -> Tuple[List[str], List[str]]
        """
        Execute the necessary steps to balance out. Starts from the overflow to move all vdisks from the container away first
        Other balances must be passed on to see where they'd have to move to
        :param balances: Other balances to work with. Used to find the owner of this balance its overflow
        :type balances: List[VDiskBalance]
        :param force: Indicates whether to force the migration or not (forcing can lead to data loss)
        :type force: bool
        :param user_input: require user input to proceed to next vDisk
        :type user_input: bool
        :param abort_on_error: Abort script when error occurs during migration
        :type abort_on_error: bool
        :return: List with all successful moves, list with all failed moves
        :rtype: NoneType
        """
        failed_moves = []
        successful_moves = []
        vdisk_guid = None
        try:
            vdisk_balance_map = self.map_vdisk_to_destination(balances)
            for vdisk_guid in self.overflow:
                add_balance = vdisk_balance_map[vdisk_guid]
                destination_std = add_balance.storagedriver
                try:
                    self._execute_move(vdisk_guid, destination_std, force, user_input)
                    successful_moves.append(vdisk_guid)
                except:
                    self.logger.exception('Unable to move VDisk {0} to {1}'.format(vdisk_guid, destination_std.storagerouter_guid))
                    if abort_on_error:
                        raise RuntimeError("Something went wrong during moving VDisk {0} to {1}".format(vdisk_guid, self.storagedriver.storagerouter_guid))
                    failed_moves.append(vdisk_guid)
        except KeyboardInterrupt:
            interrupt_msg = 'You have interrupted while moving vdisks. The last move (vDisk {0}) might be in an inconsistent state.'.format(vdisk_guid)
            self.logger.warning(interrupt_msg)
            if user_input:
                if successful_moves:
                    print('Succesfully moved vDisks: \n {0}'.format(', '.join(successful_moves)))
                if failed_moves:
                    print('\nFailed to move vDisks:\n {0}'.format(', '.join(failed_moves)))
            raise

        return successful_moves, failed_moves

    def _execute_move(self, vdisk_guid, destination_std, force, interactive, minimum_potential=1):
        """
        Perform a move
        :param vdisk_guid: VDisk to move
        :param destination_std: Destination to move to
        :param force: Use force when moving
        :param interactive: Prompt for user input before moving
        :return: None
        """
        vd = VDisk(vdisk_guid)
        current_sr = StorageRouter(vd.storagerouter_guid).name
        next_sr = destination_std.storagerouter.name
        if vd.storagerouter_guid == destination_std.storagerouter_guid:
            # Ownership changed in meantime
            self.logger.info('No longer need to move VDisk {0} to {1}'.format(vdisk_guid, destination_std.storagerouter.name))
            return
        rebalance_message = 'Rebalancing vPool by moving vDisk {0} from {1} to {2}'.format(vdisk_guid, current_sr, next_sr)
        if interactive:
            retry = True
            while retry:
                proceed = raw_input('{0}. Continue? (press Enter)'.format(rebalance_message))
                if proceed == '':  # Mock 'Enter' key
                    retry = False
        try:
            volume_potential = destination_std.vpool.storagedriver_client.volume_potential(str(destination_std.storagedriver_id))
        except:
            self.logger.exception('Unable to retrieve volume potential. Aborting')
            raise
        if volume_potential > minimum_potential:
            self.logger.info(rebalance_message)
            try:
                vd.storagedriver_client.migrate(str(vd.volume_id), str(destination_std.name), False)
            except RuntimeError:
                # When a RunTimeError occurs. Try restarting the volume locally for safety measures.
                self.logger.warning('Encountered RunTimeError. Checking if vdisk({0}) is not running and restarting it.'.format(vd.guid))
                vd.discard()
                if vd.info['live_status'] != vd.STATUSES.RUNNING:
                    vd.storagedriver_client.restart_object(str(vd.volume_id), False)
                    # Now check if the migration succeeded and if the volume is running on the correct storagedriver.
                    if vd.storagedriver_id == destination_std.name:
                        self.logger.info('Vdisk({0}) got restarted and runs on destination storagedriver. Previous error can be ignored.'.format(vd.guid))
                    else:
                        self.logger.warning('Vdisk({0}) got restarted but doesn\'t run on destination storagedriver.'.format(vd.guid))

        else:
            raise ValueError('Volume potential is lower than {0}. Not moving anymore!'.format(minimum_potential))

    @staticmethod
    def map_vdisk_to_destination(balances):
        # type: (List[VDiskBalance]) -> Dict[str, VDiskBalance]
        """
        Map all vdisks to destinations of balances
        :param balances: Balances to map for
        :return: guid - balance map
        """
        vdisk_balance_map = {}
        for balance in balances:  # type: VDiskBalance
            for vdisk_guid in balance.added:
                if vdisk_guid in vdisk_balance_map:
                    raise RuntimeError('Vdisk {} has multiple destinations'.format(vdisk_guid))
                vdisk_balance_map[vdisk_guid] = balance
        return vdisk_balance_map

    def __str__(self):
        return 'StorageRouter {} of VPool {}: hosting prior to changes: {}, imposed limit {}, hosting after changes: {}'\
            .format(self.storagedriver.storagerouter.name,
                    self.storagedriver.vpool.name,
                    len(self.hosted_guids),
                    self.limit,
                    len(self.balance))
