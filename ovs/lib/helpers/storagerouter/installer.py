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
StorageRouterInstaller class used to validate / configure / edit StorageRouter settings when setting up a vPool on it
"""

import logging
from ovs.dal.hybrids.diskpartition import DiskPartition
from ovs.lib.storagerouter import StorageRouterController


class StorageRouterInstaller(object):
    """
    Class used to add a StorageDriver on a StorageRouter
    This class will be responsible for
        - validate_global_write_buffer: Validate the requested amount of global write buffer size can be supplied
        - validate_local_cache_size: Validate if fragment or block cache is local, whether enough size is available for the caching
        - validate_vpool_extendable: Validate whether the StorageRouter is eligible to have a/another vPool on it
    """

    _logger = logging.getLogger(__name__)

    def __init__(self, root_client, storagerouter, vp_installer, sd_installer):
        """
        Initialize a StorageRouterInstaller class instance containing information about:
            - Which StorageRouter to make changes on
            - SSHClient to the StorageRouter
            - vPool information on which a new StorageDriver is going to be deployed, eg: global vPool configurations, vPool name, ...
            - StorageDriver configurations, eg: backend information, connection information, caching information, configuration information, ...
        """
        self.root_client = root_client
        self.sd_installer = sd_installer
        self.vp_installer = vp_installer
        self.storagerouter = storagerouter

        self.created_dirs = []  # Contains directories which are being created during vPool creation/extension
        self.requested_proxies = 0
        self.block_cache_supported = False
        self.requested_local_proxies = 0  # When using local caching for both fragment AND block cache, this value is used for local cache size calculation
        self.largest_write_partition = None  # Used for cache size calculation (When using local fragment or local block cache)
        self.smallest_write_partition_size = None  # Used for trigger gap and backoff gap calculation
        self.global_write_buffer_requested_size = None

        # Be aware that below information always needs to be the latest when making calculations for adding StorageDriver partitions
        self.partition_info = None
        self.write_partitions = []
        self.global_write_buffer_available_size = None

        # Cross reference
        self.vp_installer.sr_installer = self
        self.sd_installer.sr_installer = self

    def validate_global_write_buffer(self, requested_size):
        """
        Validate whether the requested write buffer size can be supplied using all the partitions with a WRITE role assigned to it
        :param requested_size: The requested size in GiB for global write buffer usage
        :type requested_size: int
        :return: None
        :rtype: NoneType
        """
        if self.partition_info is None:
            raise RuntimeError('Partition information has not been retrieved yet')

        if not 1 <= requested_size <= 10240:
            raise RuntimeError('The requested global WRITE buffer size should be between 1GiB and 10240GiB')

        usable_partitions = [part for part in self.partition_info.get(DiskPartition.ROLES.WRITE, []) if part['usable'] is True]
        available_size = sum(part['available'] for part in usable_partitions)
        requested_size *= 1024.0 ** 3

        if requested_size > available_size:
            requested_gib = requested_size / 1024.0 ** 3
            available_gib = available_size / 1024.0 ** 3
            raise RuntimeError('Too much space requested for {0} cache. Available: {1:.2f} GiB, Requested: {2:.2f} GiB'.format(DiskPartition.ROLES.WRITE, available_gib, requested_gib))

        self.write_partitions = usable_partitions
        self.global_write_buffer_available_size = available_size
        self.global_write_buffer_requested_size = requested_size
        self._logger.debug('Global write buffer has been validated. Available size: {0}, requested size: {1}, write_partitions: {2}'.format(available_size, requested_size, usable_partitions))

    def validate_local_cache_size(self, requested_proxies):
        """
        Validate whether the requested amount of proxies can be deployed on local StorageRouter partitions having the WRITE role
        :param requested_proxies: Amount of proxies that have been requested for deployment
        :type requested_proxies: int
        :return: None
        :rtype: NoneType
        """
        if not 1 <= requested_proxies <= 16:
            raise RuntimeError('The requested amount of proxies to deploy should be a value between 1 and 16')

        if len(self.write_partitions) == 0 or self.global_write_buffer_requested_size is None or self.global_write_buffer_available_size is None:
            raise RuntimeError('Global write buffer calculation has not been done yet')

        # Calculate available write cache size
        largest_ssd_size = 0
        largest_sata_size = 0
        largest_ssd_write_partition = None
        largest_sata_write_partition = None
        for info in self.write_partitions:
            if info['ssd'] is True and info['available'] > largest_ssd_size:
                largest_ssd_size = info['available']
                largest_ssd_write_partition = info['guid']
            elif info['ssd'] is False and info['available'] > largest_sata_size:
                largest_sata_size = info['available']
                largest_sata_write_partition = info['guid']

        if largest_ssd_write_partition is None and largest_sata_write_partition is None:
            raise RuntimeError('No {0} partition found to put the local caches on'.format(DiskPartition.ROLES.WRITE))

        self.requested_proxies = requested_proxies
        self.largest_write_partition = DiskPartition(largest_ssd_write_partition or largest_sata_write_partition)
        if self.sd_installer.block_cache_local is True:
            self.requested_local_proxies += requested_proxies
        if self.sd_installer.fragment_cache_local is True:
            self.requested_local_proxies += requested_proxies

        if self.requested_local_proxies > 0:
            proportion = float(largest_ssd_size or largest_sata_size) / self.global_write_buffer_available_size
            available_size = proportion * self.global_write_buffer_requested_size * 0.10  # Only 10% is used on the largest WRITE partition for fragment caching
            available_size_gib = available_size / 1024.0 ** 3
            if available_size / self.requested_local_proxies < 1024 ** 3:
                raise RuntimeError('Not enough space available ({0}GiB) on largest local WRITE partition to deploy {1} prox{2}'.format(available_size_gib, requested_proxies, 'y' if requested_proxies == 1 else 'ies'))
        self._logger.debug('Local cache size validated. Requested vpool proxies: {0}, requested cache proxies: {1}, Largest write partition: {1}'.format(self.requested_proxies, self.requested_local_proxies, str(self.largest_write_partition)))

    def validate_vpool_extendable(self):
        """
        Perform some validations on the specified StorageRouter to verify whether a vPool can be created or extended on it
        :return: None
        :rtype: NoneType
        """
        if self.partition_info is None:
            raise RuntimeError('Partition information has not been retrieved yet')

        # Validate RDMA capabilities
        if self.sd_installer.rdma_enabled is True and self.storagerouter.rdma_capable is False:
            raise RuntimeError('DTL transport over RDMA is not supported by StorageRouter with IP {0}'.format(self.storagerouter.ip))

        # Validate block cache is allowed to be used
        if self.storagerouter.features is None:
            raise RuntimeError('Could not load available features')
        self.block_cache_supported = 'block-cache' in self.storagerouter.features.get('alba', {}).get('features', [])
        if self.block_cache_supported is False and (self.sd_installer.block_cache_on_read is True or self.sd_installer.block_cache_on_write is True):
            raise RuntimeError('Block cache is not a supported feature')

        # Validate mount point for the vPool to be created does not exist yet
        if StorageRouterController.mountpoint_exists(name=self.vp_installer.name, storagerouter_guid=self.storagerouter.guid):
            raise RuntimeError('The mount point for vPool {0} already exists'.format(self.vp_installer.name))

        # Validate SCRUB role available on any StorageRouter
        if StorageRouterController.check_scrub_partition_present() is False:
            raise RuntimeError('At least 1 StorageRouter must have a partition with a {0} role'.format(DiskPartition.ROLES.SCRUB))

        # Validate required roles present
        for required_role in [DiskPartition.ROLES.DB, DiskPartition.ROLES.DTL, DiskPartition.ROLES.WRITE]:
            if required_role not in self.partition_info:
                raise RuntimeError('Missing required partition with a {0} role'.format(required_role))
            elif len(self.partition_info[required_role]) == 0:
                raise RuntimeError('At least 1 partition with a {0} role is required per StorageRouter'.format(required_role))
            elif required_role in [DiskPartition.ROLES.DB, DiskPartition.ROLES.DTL]:
                if len(self.partition_info[required_role]) > 1:
                    raise RuntimeError('Only 1 partition with a {0} role is allowed per StorageRouter'.format(required_role))
            else:
                total_available = [part['available'] for part in self.partition_info[required_role]]
                if total_available == 0:
                    raise RuntimeError('Not enough available space for {0}'.format(required_role))

        # Validate mount points are mounted
        for role, part_info in self.partition_info.iteritems():
            if role not in [DiskPartition.ROLES.DB, DiskPartition.ROLES.DTL, DiskPartition.ROLES.WRITE, DiskPartition.ROLES.SCRUB]:
                continue

            for part in part_info:
                mount_point = part['mountpoint']
                if mount_point == DiskPartition.VIRTUAL_STORAGE_LOCATION:
                    continue
                if self.root_client.is_mounted(path=mount_point) is False:
                    raise RuntimeError('Mount point {0} is not mounted'.format(mount_point))
