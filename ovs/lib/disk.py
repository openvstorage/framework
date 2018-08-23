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
DiskController module
"""
from ovs.dal.hybrids.disk import Disk
from ovs.dal.hybrids.diskpartition import DiskPartition
from ovs.dal.hybrids.storagerouter import StorageRouter
from ovs_extensions.generic.disk import DiskTools, Disk as GenericDisk, Partition as GenericPartition
from ovs.extensions.generic.logger import Logger
from ovs.extensions.generic.sshclient import SSHClient, UnableToConnectException
from ovs.lib.helpers.decorators import ovs_task


class DiskController(object):
    """
    Contains all BLL wrt physical Disks
    """
    _logger = Logger('lib')

    @staticmethod
    @ovs_task(name='ovs.disk.sync_with_reality', ensure_single_info={'mode': 'CHAINED'})
    def sync_with_reality(storagerouter_guid):
        """
        Syncs the Disks from the StorageRouter specified with the reality.
        CHANGES MADE TO THIS CODE SHOULD BE REFLECTED IN THE ASD-MANAGER list_disks CALL TOO!
        :param storagerouter_guid: Guid of the Storage Router to synchronize
        :type storagerouter_guid: str
        :return: None
        """
        storagerouter = StorageRouter(storagerouter_guid)
        try:
            client = SSHClient(storagerouter, username='root')
        except UnableToConnectException:
            DiskController._logger.exception('Could not connect to StorageRouter {0}'.format(storagerouter.ip))
            raise

        disks, name_alias_mapping = DiskTools.model_devices(client)
        disks_by_name = dict((disk.name, disk) for disk in disks)
        alias_name_mapping = name_alias_mapping.reverse_mapping()
        # Sync the model
        for disk in storagerouter.disks:
            generic_disk_model = None  # type: GenericDisk
            for alias in disk.aliases:
                if alias in alias_name_mapping:
                    name = alias_name_mapping[alias].replace('/dev/', '')
                    if name in disks_by_name:
                        generic_disk_model = disks_by_name.pop(name)
                        break
            # Partitioned loop, nvme devices no longer show up in alias_name_mapping
            if generic_disk_model is None and disk.name in disks_by_name and (disk.name.startswith(tuple(['fio', 'loop', 'nvme']))):
                generic_disk_model = disks_by_name.pop(disk.name)

            if not generic_disk_model:
                # Remove disk / partitions if not reported by 'lsblk'
                DiskController._remove_disk_model(disk)
            else:
                # Update existing disks and their partitions
                DiskController._sync_disk_with_model(disk, generic_disk_model)
        # Create all disks and their partitions not yet modeled
        for disk_name, generic_disk_model in disks_by_name.iteritems():
            DiskController._model_disk(generic_disk_model, storagerouter)

    @classmethod
    def _remove_disk_model(cls, modeled_disk):
        # type: (Disk) -> None
        """
        Remove the modeled disk
        :param modeled_disk: The modeled disk
        :type modeled_disk: Disk
        :return: None
        :rtype: NoneType
        """
        DiskController._logger.info('Disk {0} - No longer found'.format(modeled_disk.name))
        delete = True
        for partition in modeled_disk.partitions:
            if len(partition.roles) > 0:
                delete = False
                cls._logger.warning('Disk {0} - Partition with offset {1} - Has roles, will not delete'.format(modeled_disk.name, partition.offset))
                break
        if delete is True:
            for partition in modeled_disk.partitions:
                partition.delete()
            modeled_disk.delete()
            DiskController._logger.info('Disk {0} - Deleted'.format(modeled_disk.name))
        else:
            for partition in modeled_disk.partitions:
                partition.state = 'MISSING'
                cls._logger.warning('Disk {0} - Partition with offset {1} - Updated status to MISSING'.format(modeled_disk.name, partition.offset))
            modeled_disk.state = 'MISSING'
            DiskController._logger.warning('Disk {0} - Updated status to MISSING'.format(modeled_disk.name))

    @classmethod
    def _sync_disk_with_model(cls, modeled_disk, generic_modeled_disk):
        # type: (Disk, GenericDisk) -> None
        """
        Sync a generic disk with the modeled disk
        :param modeled_disk: The modeled disk
        :type modeled_disk: Disk
        :param generic_modeled_disk: The generic modeled disk (returned by Disktools)
        :type generic_modeled_disk: GenericDisk
        :return: None
        :rtype NoneType
        """
        cls._logger.info('Disk {0} - Found, updating'.format(modeled_disk.name))
        cls._update_disk(modeled_disk, generic_modeled_disk)
        partitions_by_offset = dict((partition.offset, partition) for partition in generic_modeled_disk.partitions)
        for partition in modeled_disk.partitions:
            if partition.offset not in partitions_by_offset:
                cls._logger.info('Disk {0} - Partition with offset {1} - No longer found'.format(modeled_disk.name, partition.offset))
                if len(partition.roles) > 0:
                    cls._logger.warning('Disk {0} - Partition with offset {1} - Update status to MISSING'.format(modeled_disk.name,partition.offset))
                    partition.state = 'MISSING'
                else:
                    cls._logger.info('Disk {0} - Partition with offset {1} - Deleting'.format(modeled_disk.name, partition.offset))
                    partition.delete()
            else:
                cls._update_partition(partition, partitions_by_offset.pop(partition.offset))
        for partition_offset, generic_partition in partitions_by_offset.iteritems():
            cls._logger.info('Disk {0} - Creating partition - {1}'.format(modeled_disk.name, generic_partition.__dict__))
            cls._model_partition(partitions_by_offset[partition_offset], modeled_disk)

    @classmethod
    def _model_partition(cls, generic_partition_model, disk):
        # type: (GenericPartition, Disk) -> DiskPartition
        """
        Models a partition
        :param generic_partition_model: The generic modeled partition (returned by Disktools)
        :type generic_partition_model: GenericPartition
        :param disk: The modeled disk
        :type disk: Disk
        :return: The newly modeled partition
        :rtype: DiskPartition
        """
        partition = DiskPartition()
        partition.disk = disk
        cls._update_partition(partition, generic_partition_model)
        return partition

    @classmethod
    def _model_disk(cls, generic_disk_model, storagerouter):
        # type: (GenericDisk, StorageRouter) -> Disk
        """
        Models a disk
        :param generic_disk_model: The generic modeled disk (returned by Disktools)
        :type generic_disk_model: GenericDisk
        :param storagerouter: Storagerouter to which this disk belongs
        :type storagerouter: StorageRouter
        :return: The newly modeled disk
        :rtype: Disk
        """
        DiskController._logger.info('Disk {0} - Creating disk - {1}'.format(generic_disk_model.name, generic_disk_model.__dict__))
        disk = Disk()
        disk.storagerouter = storagerouter
        disk.name = generic_disk_model.name
        DiskController._update_disk(disk, generic_disk_model)
        for partition in generic_disk_model.partitions:  # type: GenericPartition
            cls._model_partition(partition, disk)
        return disk

    @staticmethod
    def _update_partition(modeled_partition, generic_partition_model):
        # type: (DiskPartition, GenericPartition) -> None
        """
        Updates a partition
        Copies all properties from the generic modeled partition to the own model
        :param modeled_partition: The modeled partition
        :type modeled_partition: DiskPartition
        :param generic_partition_model: The generic modeled partition (returned by Disktools)
        :type generic_partition_model: GenericPartition
        :return: None
        :rtype NoneType
        """
        for prop in ['filesystem', 'offset', 'state', 'aliases', 'mountpoint', 'size']:
            if hasattr(generic_partition_model, prop):
                setattr(modeled_partition, prop, getattr(generic_partition_model, prop))
        modeled_partition.save()

    @staticmethod
    def _update_disk(modeled_disk, generic_disk_model):
        # type: (Disk, GenericDisk) -> None
        """
        Updates a disk
        Copies all properties from the generic modeled disk to the own model
        :param modeled_disk: The modeled disk
        :type modeled_disk: Disk
        :param generic_disk_model: The generic modeled disk (returned by Disktools)
        :type generic_disk_model: GenericDisk
        :return: None
        :rtype NoneType
        """
        for prop in ['state', 'aliases', 'is_ssd', 'model', 'size', 'name', 'serial']:
            if hasattr(generic_disk_model, prop):
                setattr(modeled_disk, prop, getattr(generic_disk_model, prop))
        modeled_disk.save()
