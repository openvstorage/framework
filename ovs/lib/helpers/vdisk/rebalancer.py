# Copyright (C) 2018 iNuron NV
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
Rebalances volumes across nodes
Taken from the support-tools
"""

from __future__ import division

import pprint
import itertools
from math import ceil
from ovs.dal.exceptions import ObjectNotFoundException
from ovs.dal.hybrids.storagerouter import StorageRouter
from ovs.dal.hybrids.vpool import VPool
from ovs.dal.hybrids.storagedriver import StorageDriver
from ovs.dal.hybrids.vdisk import VDisk
from ovs_extensions.log.logger import Logger

# noinspection PyUnreachableCode
if False:
    from typing import List, Dict, Tuple, Optional, Union


class FailedMovesException(Exception):
    """
    Thrown when volumes could not be moved
    """


class VDiskRebalancer(object):

    _volume_potentials = {}
    logger = Logger('vdisk_rebalance')

    @classmethod
    def print_balances(cls, balances):
        # type: (List[VDiskBalance]) -> None
        """
        Prints out balances
        :return: None
        :rtype: NoneType
        """
        balances_by_vpool = {}
        for balance in balances:  # type: VDiskBalance
            vpool = balance.storagedriver.vpool
            if vpool not in balances_by_vpool:
                balances_by_vpool[vpool] = []
            balances_by_vpool[vpool].append(balance)
        for vpool, vpool_balances in balances_by_vpool.viewitems():
            print('Balance for VPool {0}'.format(vpool.name))
            for balance in vpool_balances:  # type: VDiskBalance
                storagerouter = balance.storagedriver.storagerouter
                print(' Storagerouter {0}, vdisks now: {1}, vdisks afterwards {2}, added {3}'.format(storagerouter.name, len(balance.hosted_guids), len(balance.balance), len(balance.added)))
                if balance.added:
                    added_source_overview = {}
                    for vdisk_guid in balance.added:
                        current_storagerouter = StorageRouter(VDisk(vdisk_guid).storagerouter_guid)
                        if current_storagerouter not in added_source_overview:
                            added_source_overview[current_storagerouter] = []
                        added_source_overview[current_storagerouter].append(vdisk_guid)
                    print('  Vdisks added from:')
                    for current_storagerouter, moved_vdisk_guids in added_source_overview.iteritems():
                        print('    StorageRouter {0}: {1}'.format(current_storagerouter.name, len(moved_vdisk_guids)))

    @classmethod
    def get_rebalanced_layout(cls, vpool_guid, excluded_storagerouters=None, ignore_domains=False, evacuate_storagerouters=None, base_on_volume_potential=True):
        # type: (str, Optional[List[str]], Optional[bool], Optional[List[str]], Optional[bool]) -> List[VDiskBalance]
        """
        Retrieve the layout of how to optimal spread would look like
        :param evacuate_storagerouters: Migrate all vdisks from this hosts
        :type evacuate_storagerouters: List[str]
        :param vpool_guid: Guid of the VPool to rebalance
        :type vpool_guid: str
        :param excluded_storagerouters: Guids of StorageRouters to avoid
        :type excluded_storagerouters: List[str]
        :param ignore_domains: Ignore the domains (rebalance across everything)
        :type ignore_domains: bool
        :param base_on_volume_potential: Base the movement of the volume potential instead of a linear distribution
        :type base_on_volume_potential: bool
        :return: List of balances
        :rtype: List[VDiskBalance]
        """
        if evacuate_storagerouters is None:
            evacuate_storagerouters = []
        if excluded_storagerouters is None:
            excluded_storagerouters = []

        vpool = VPool(vpool_guid)
        if ignore_domains:
            return cls._get_rebalances_layout(vpool, excluded_storagerouters, evacuate_storagerouters, base_on_volume_potential)
        return cls._get_rebalanced_layout_by_domain(vpool, excluded_storagerouters, evacuate_storagerouters, base_on_volume_potential)

    @classmethod
    def get_volume_potentials(cls, storagedrivers, cache=True):
        potentials = {}
        for storagedriver in storagedrivers:
            if cache:
                potential = cls._volume_potentials.get(storagedriver, -1)
                if potential == -1:
                    potential = storagedriver.vpool.storagedriver_client.volume_potential(str(storagedriver.storagedriver_id))
                    cls._volume_potentials[storagedriver] = potential
            else:
                potential = storagedriver.vpool.storagedriver_client.volume_potential(str(storagedriver.storagedriver_id))
            potentials[storagedriver] = potential
        return potentials

    @classmethod
    def _get_rebalances_layout(cls, vpool, excluded_storagerouters, evacuate_storagerouters, base_on_volume_potential):
        # type: (VPool, List[str], List[str], bool) -> List[VDiskBalance]
        """
        Rebalance volumes and stay without domains
        :param vpool: VPool to rebalance
        :type vpool: VPool
        :param excluded_storagerouters: Guids of StorageRouters to avoid
        :type excluded_storagerouters: List[str]
        :param evacuate_storagerouters: Migrate all vdisks from this hosts
        :type evacuate_storagerouters: List[str]
        :param base_on_volume_potential: Base the limit calculation of the volume potential ratio
        :type base_on_volume_potential: bool
        :return: List of balances
        :rtype: List[VDiskBalance]
        """
        storagerouters_to_avoid = set(itertools.chain(excluded_storagerouters, evacuate_storagerouters))
        destination_storagedrivers = [std for std in vpool.storagedrivers if std.storagerouter_guid not in storagerouters_to_avoid]
        destination_storagedrivers_by_ip = dict((storagedriver.storagerouter.ip, storagedriver) for storagedriver in destination_storagedrivers)

        volume_potentials = {}
        if base_on_volume_potential:
            volume_potentials = cls.get_volume_potentials(destination_storagedrivers)
            total_potential = sum(p for p in volume_potentials.itervalues())
            vdisks_within_destination_storagedrivers = list(
                itertools.chain(*(sd.vdisks_guids for sd in destination_storagedrivers)))
            volume_total_capacity = total_potential + len(vdisks_within_destination_storagedrivers)

        # Default limit. Simple distribution
        storagedriver_vdisk_limit = int(ceil(len(vpool.vdisks_guids) / len(destination_storagedrivers)))
        balances = {}
        overflow = []
        for storagedriver in vpool.storagedrivers:
            if base_on_volume_potential:
                # Use the ratio between volume potential max and current to distribute
                volume_potential = volume_potentials[storagedriver]
                storagedriver_vdisk_limit = int(ceil(len(vpool.vdisks_guids) * (volume_potential + len(storagedriver.vdisks_guids)) / volume_total_capacity))

            limit = 0 if storagedriver.storagerouter_guid in evacuate_storagerouters else storagedriver_vdisk_limit
            balance = VDiskBalance(storagedriver, limit)
            overflow.extend(balance.overflow)
            balances[storagedriver] = balance
        # Attempt to move to current mds hosts
        for vdisk_guid in overflow:
            vdisk = VDisk(vdisk_guid)
            # If only set was ordered :D
            preferred_destinations = [destination_storagedrivers_by_ip[mds_entry['ip']] for mds_entry in vdisk.info['metadata_backend_config'] if mds_entry['ip'] in destination_storagedrivers_by_ip]
            # Try to fill in these storagedriver first
            destinations = preferred_destinations + [storagedriver for storagedriver in destination_storagedrivers if storagedriver not in preferred_destinations]
            added = False
            for storagedriver in destinations:
                balance = balances[storagedriver]
                added = cls.add_to_balance(vdisk_guid, balance)
                if added:
                    try:
                        index = preferred_destinations.index(storagedriver)
                        mds_type = 'master' if index == 0 else 'slave'
                        cls.logger.info('Appointing {0} to {1} (index {2})'.format(vdisk_guid, mds_type, index))
                    except ValueError:
                        # Index query didn't find the storagedriver
                        cls.logger.info('Appointing to non-mds host')
                    break
            if not added:
                raise NotImplementedError('Vdisk couldnt be added to any destination. Might be faulty implementation here')
        return balances.values()

    @classmethod
    def _get_rebalanced_layout_by_domain(cls, vpool, excluded_storagerouters, evacuate_storagerouters, base_on_volume_potential):
        # type: (VPool, List[str], List[str], bool) -> List[VDiskBalance]
        """
        Rebalance volumes and stay within the primary domain
        :param vpool: VPool to rebalance
        :type vpool: VPool
        :param excluded_storagerouters: Guids of StorageRouters to avoid
        :type excluded_storagerouters: List[str]
        :param evacuate_storagerouters: Migrate all vdisks from this hosts
        :type evacuate_storagerouters: List[str]
        :param base_on_volume_potential: Base the limit calculation of the volume potential ratio
        :type base_on_volume_potential: bool
        :return: List of balances
        :rtype: List[VDiskBalance]
        """
        # Calculate balance cap for every storagedriver
        # Every storagedriver can share disks between other storagedriver within the same primary domain
        # Certain storagedrivers add their disks to the pool but can't take disks themselves
        balances = {}
        storagedriver_limits = {}
        storagedriver_domain_relation = {}
        for storagedriver in vpool.storagedrivers:
            cls.logger.info('Calculating the limit for {} in VPool {}'.format(storagedriver.storagerouter.name, vpool.name))
            # Create the disk pool for the current storagedriver in the domain
            storagedrivers_in_domain = cls.get_storagedrivers_in_same_primary_domain_as_storagedriver(storagedriver, excluded_storagerouters)
            cls.logger.info('{} shares primary domains with {}'.format(storagedriver.storagerouter.name, ', '.join(d.storagerouter.name for d in storagedrivers_in_domain)))
            storagedriver_domain_relation[storagedriver] = storagedrivers_in_domain
            vdisks_within_domain = []
            for storagedriver_in_domain in storagedrivers_in_domain:
                vdisks_within_domain.extend(storagedriver_in_domain.vdisks_guids)
            cls.logger.info('VDisks within the primary domain of {}: {}'.format(storagedriver.storagerouter.name, len(vdisks_within_domain)))
            # Think about the disk distribution
            if storagedriver.storagerouter_guid in evacuate_storagerouters:
                limit = 0
            else:
                # Remove the evacuations from the limit
                usable_storagedrivers_in_domain = [std for std in storagedrivers_in_domain if std.storagerouter_guid not in evacuate_storagerouters]
                cls.logger.info('Can move volumes to {} within the primary domain storagedrivers'.format(', '.join(d.storagerouter.name for d in usable_storagedrivers_in_domain)))
                if base_on_volume_potential:
                    volume_potentials = cls.get_volume_potentials(usable_storagedrivers_in_domain)
                    total_potential = sum(p for p in volume_potentials.itervalues())
                    volume_potentials_sr = dict((storagedriver.storagerouter.name, potential) for storagedriver, potential in volume_potentials.iteritems())
                    cls.logger.info('Volume potential overview: {}. Total potential: {}'.format(pprint.pformat(volume_potentials_sr), total_potential))
                    # len should be adjusted with evacuates
                    vdisks_within_domain_usable = list(itertools.chain(*(sd.vdisks_guids for sd in usable_storagedrivers_in_domain)))
                    volume_total_capacity = total_potential + len(vdisks_within_domain_usable)
                    if len(vdisks_within_domain) > volume_total_capacity:
                        cls.logger.error('The total capacity with the usuable storagedrivers in the domain is not large enough. vdisks_within_domain {0} > volume_total_capacity {1}'
                                         .format(len(vdisks_within_domain), volume_total_capacity))
                        raise RuntimeError('Migration with given params is not possible. Too many vdisks for the usuable storagedrivers within the domain .')
                    cls.logger.info('Total capacity within this domain subset is {}'.format(volume_total_capacity))
                    # Use the ratio between volume potential max and current to distribute
                    volume_potential = volume_potentials[storagedriver]
                    volume_ratio = (volume_potential + len(storagedriver.vdisks_guids)) / volume_total_capacity
                    cls.logger.info('{} can take {}% of the volumes'.format(storagedriver.storagerouter.name, volume_ratio * 100))
                    limit = int(ceil(len(vdisks_within_domain) * volume_ratio))
                else:
                    limit = int(ceil(len(vdisks_within_domain) / len(usable_storagedrivers_in_domain)))
            cls.logger.info('Limit imposed for {}: {}'.format(storagedriver.storagerouter.name, limit))
            storagedriver_limits[storagedriver] = limit

        for storagedriver in vpool.storagedrivers:
            balance = VDiskBalance(storagedriver, storagedriver_limits[storagedriver])
            balances[storagedriver] = balance
            cls.logger.info('Balance overview {}'.format(balance))

        for storagedriver in vpool.storagedrivers:
            storagedrivers_in_domain = [std for std in storagedriver_domain_relation[storagedriver] if std != storagedriver]
            storagedrivers_in_domain_by_ip = dict((storagedriver.storagerouter.ip, storagedriver) for storagedriver in storagedrivers_in_domain)
            balance = balances[storagedriver]
            cls.logger.info('Migrating {} vdisks from {} of VPool {}. Limit: {}, hosting {}'.format(len(balance.overflow), storagedriver.storagerouter.name, vpool.name,
                                                                                                    balance.limit, len(balance.hosted_guids)))
            for vdisk_guid in balance.overflow:
                vdisk = VDisk(vdisk_guid)
                preferred_destinations = [storagedrivers_in_domain_by_ip[mds_entry['ip']] for mds_entry in vdisk.info['metadata_backend_config']
                                          if mds_entry['ip'] in storagedrivers_in_domain_by_ip]
                # Try to fill in these storagedriver first
                destinations = preferred_destinations + [storagedriver for storagedriver in storagedrivers_in_domain if storagedriver not in preferred_destinations]
                cls.logger.info('Destination overview for migrations: {}'.format(', '.join(d.storagerouter.name for d in destinations)))
                added = False
                while not added and destinations:
                    destination = destinations.pop()
                    balance = balances[destination]
                    added = cls.add_to_balance(vdisk_guid, balance)
                    if added:
                        cls.logger.info('Added vdisk {} to {}'.format(vdisk_guid, destination.storagerouter.name))
                        if destination.storagedriver_id == vdisk.storagedriver_id:
                            raise RuntimeError('Moving to current host ERROR')
                        try:
                            index = preferred_destinations.index(destination)
                            mds_type = 'master' if index == 0 else 'slave'
                            cls.logger.info('Appointing {0} to {1} (index {2})'.format(vdisk_guid, mds_type, index))
                        except ValueError:
                            # Index query didn't find the storagedriver
                            cls.logger.info('Appointing to non-mds host')
                    else:
                        cls.logger.info('Did not add vdisks to {}. Its limit: {}, currently hosting {}'.format(destination.storagerouter.name, balance.limit, len(balance.balance)))
                if not added:
                    raise NotImplementedError('Vdisk couldnt be added to any destination. Might be faulty implementation here')
        return balances.values()

    @classmethod
    def get_storagedrivers_in_same_primary_domain_as_storagedriver(cls, storagedriver, excluded_storagerouters=None):
        # type: (StorageDriver, Optional[List[str]]) -> List[StorageDriver]
        """
        Retrieve all storagedrivers within the same primary domain as the given storagedriver
        :param storagedriver: StorageDriver to check other domain relations for
        :param excluded_storagerouters: Storagerouters that are excluded for the search
        :type excluded_storagerouters: Optional[List[str]]
        :return: List of storagedrivers
        :rtype: List[StorageDriver]
        """
        if excluded_storagerouters is None:
            excluded_storagerouters = []
        primary_domains = cls.get_primary_domain_guids_storagedriver(storagedriver)
        if not primary_domains:
            return list(storagedriver.vpool.storagedrivers)
        return [std for std in storagedriver.vpool.storagedrivers
                if std.storagerouter_guid not in excluded_storagerouters
                and any(domain_guid in primary_domains for domain_guid in cls.get_primary_domain_guids_storagedriver(std))]

    @staticmethod
    def get_primary_domain_guids_storagedriver(storagedriver):
        # type: (StorageDriver) -> List[str]
        """
        Retrieve all primary domains of the StorageDriver
        :param storagedriver: Storagedriver to get domains from
        :type storagedriver: StorageDriver
        :return: List of primary domain guids
        :rtype: List[str]
        """
        primary_domains = []
        storagerouter = storagedriver.storagerouter
        for junction in storagerouter.domains:
            if not junction.backup:
                primary_domains.append(junction.domain_guid)
        return primary_domains

    @classmethod
    def add_to_balance(cls, vdisk_guid, balance):
        # type: (str, VDiskBalance) -> bool
        """
        Try to add a vdisk to a balance
        :param vdisk_guid: Guid to add
        :param balance: Balance to add guid to
        :return: True if vdisk was added, else False
        :rtype: bool
        """
        added, overflowed = balance.fill([vdisk_guid])
        return vdisk_guid in added


class VDiskBalance(object):

    logger = Logger('vdisk_balance')

    def __init__(self, storagedriver, vdisk_limit, balance=None, overflow=None, added=None):
        # type: (StorageDriver, int, Optional[List[str]], Optional[List[str]], Optional[List[str]]) -> None
        """
        Represents the vdisk balance of a storagedriver
        :param storagedriver: StorageDriver to balance for
        :type storagedriver: StorageDriver
        :param vdisk_limit: Maximum amount of vdisks to host. -1 means no limit
        :type vdisk_limit: int
        :param balance: Balance of vdisk guids to use. Used primarily in serializing/deserializing
        :type balance: Optional[List[str]]
        :param overflow: Overflow of vdisk guids to use. Used primarily in serializing/deserializing
        :type overflow: Optional[List[str]]
        :param added: List of vdisk guids added to the balance. Used primarily in serializing/deserializing
        :type added: Optional[List[str]]
        """
        self.storagedriver = storagedriver
        self.hosted_guids = storagedriver.vdisks_guids
        self.limit = vdisk_limit

        combination_vars = [balance, overflow, added]
        combination_vars_given = all(v is not None for v in combination_vars)
        if any(v is not None for v in combination_vars) and not combination_vars_given:
            raise ValueError('When providing any of the variables {}, all should be provided'.format(', '.join(['balance', 'overflow', 'added'])))
        if combination_vars_given:
            self.balance = balance
            self.overflow = overflow
            self.added = added
        else:
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
                except KeyboardInterrupt:
                    raise
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
        _ = force
        try:
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
                    vd.invalidate_dynamics('info')
                    if vd.info['live_status'] != vd.STATUSES.RUNNING:
                        vd.storagedriver_client.restart_object(str(vd.volume_id), False)
                        # Now check if the migration succeeded and if the volume is running on the correct storagedriver.
                        if vd.storagedriver_id == destination_std.name:
                            self.logger.info('Vdisk({0}) got restarted and runs on destination storagedriver. Previous error can be ignored.'.format(vd.guid))
                        else:
                            self.logger.warning('Vdisk({0}) got restarted but doesn\'t run on destination storagedriver.'.format(vd.guid))

            else:
                raise ValueError('Volume potential is lower than {0}. Not moving anymore!'.format(minimum_potential))
        except ObjectNotFoundException as ex:
            self.logger.warning('Could not retrieve an object. Assuming it\'s a vDisk: {}'.format(ex))

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

    def to_dict(self):
        """
        Export the VDiskBalance object. Workaround to being unable to pickle/serialize a DataObject
        Use the associated import function to cast it back to an object
        :return:
        """
        return {'storagedriver_guid': self.storagedriver.guid,
                'hosted_guids': self.hosted_guids,
                'limit': self.limit,
                'balance': self.balance,
                'overflow': self.overflow,
                'added': self.added}

    @staticmethod
    def from_dict(data):
        # type: (Dict[str, Union[str, int, List[str]]]) -> VDiskBalance
        """
        Instantiate a VDiskBalance through a dict. See to_dict method to check it's form
        :param data: Data dict
        :return:
        """
        kwargs = data.copy()
        kwargs['storagedriver'] = StorageDriver(kwargs.pop('storagedriver_guid'))
        return VDiskBalance(**kwargs)
