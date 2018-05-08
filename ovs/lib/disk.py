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
import re
import time
from ovs.dal.hybrids.disk import Disk
from ovs.dal.hybrids.diskpartition import DiskPartition
from ovs.dal.hybrids.storagerouter import StorageRouter
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

        # Retrieve all symlinks for all devices
        # Example of name_alias_mapping:
        # {'/dev/md0': ['/dev/disk/by-id/md-uuid-ad2de634:26d97253:5eda0a23:96986b76', '/dev/disk/by-id/md-name-OVS-1:0'],
        #  '/dev/sda': ['/dev/disk/by-path/pci-0000:03:00.0-sas-0x5000c295fe2ff771-lun-0'],
        #  '/dev/sda1': ['/dev/disk/by-uuid/e3e0bc62-4edc-4c6b-a6ce-1f39e8f27e41', '/dev/disk/by-path/pci-0000:03:00.0-sas-0x5000c295fe2ff771-lun-0-part1']}
        name_alias_mapping = {}
        alias_name_mapping = {}
        for path_type in client.dir_list(directory='/dev/disk'):
            if path_type in ['by-uuid', 'by-partuuid']:  # UUIDs can change after creating a filesystem on a partition
                continue
            directory = '/dev/disk/{0}'.format(path_type)
            for symlink in client.dir_list(directory=directory):
                symlink_path = '{0}/{1}'.format(directory, symlink)
                link = client.file_read_link(symlink_path)
                if link not in name_alias_mapping:
                    name_alias_mapping[link] = []
                name_alias_mapping[link].append(symlink_path)
                alias_name_mapping[symlink_path] = link

        # Parse 'lsblk' output
        # --exclude 1 for RAM devices, 2 for floppy devices, 11 for CD-ROM devices, 43 for nbd devices (See https://www.kernel.org/doc/html/v4.15/admin-guide/devices.html)
        command = ['lsblk', '--pairs', '--bytes', '--noheadings', '--exclude', '1,2,11,43']
        output = '--output=KNAME,SIZE,MODEL,STATE,MAJ:MIN,FSTYPE,TYPE,ROTA,MOUNTPOINT,LOG-SEC{0}'
        regex = '^KNAME="(?P<name>.*)" SIZE="(?P<size>\d*)" MODEL="(?P<model>.*)" STATE="(?P<state>.*)" MAJ:MIN="(?P<dev_nr>.*)" FSTYPE="(?P<fstype>.*)" TYPE="(?P<type>.*)" ROTA="(?P<rota>[0,1])" MOUNTPOINT="(?P<mtpt>.*)" LOG-SEC="(?P<sector_size>\d*)"( SERIAL="(?P<serial>.*)")?$'
        try:
            devices = client.run(command + [output.format(',SERIAL')]).splitlines()
        except:
            devices = client.run(command + [output.format('')]).splitlines()
        device_regex = re.compile(regex)
        configuration = {}
        parsed_devices = []
        for device in devices:
            match = re.match(device_regex, device)
            if match is None:
                DiskController._logger.error('Device regex did not match for {0}. Please investigate'.format(device))
                raise Exception('Failed to parse \'lsblk\' output')

            groupdict = match.groupdict()
            name = groupdict['name'].strip()
            size = groupdict['size'].strip()
            model = groupdict['model'].strip()
            state = groupdict['state'].strip()
            dev_nr = groupdict['dev_nr'].strip()
            serial = (groupdict['serial'] or '').strip()
            fs_type = groupdict['fstype'].strip()
            dev_type = groupdict['type'].strip()
            rotational = groupdict['rota'].strip()
            mount_point = groupdict['mtpt'].strip()
            sector_size = groupdict['sector_size'].strip()

            if dev_type == 'rom':
                continue

            link = client.file_read_link('/sys/block/{0}'.format(name))
            device_state = None
            friendly_path = '/dev/{0}'.format(name)
            if not client.path_exists(friendly_path):
                DiskController._logger.warning('Skipping {0} as path {1} does not exist'.format(name, friendly_path))
                continue
            system_aliases = name_alias_mapping.get(friendly_path, [friendly_path])
            device_is_also_partition = False
            if link is not None:  # If this returns, it means its a device and not a partition
                device_is_also_partition = mount_point != ''  # LVM, RAID1, ... have the tendency to be a device with a partition on it, but the partition is not reported by 'lsblk'
                device_state = Disk.STATES.OK if state == 'running' or dev_nr.split(':')[0] != '8' else Disk.STATES.FAILURE
                parsed_devices.append({'name': name,
                                       'state': device_state})
                configuration[name] = {'name': name,
                                       'size': int(size),
                                       'model': model if model != '' else None,
                                       'serial': serial if serial != '' else None,
                                       'state': device_state,
                                       'is_ssd': rotational == '0',
                                       'aliases': system_aliases,
                                       'partitions': {}}
            if link is None or device_is_also_partition is True:
                current_device = None
                current_device_state = None
                if device_is_also_partition is True:
                    offset = 0
                    current_device = name
                    current_device_state = device_state
                else:
                    offset = 0
                    for device_info in reversed(parsed_devices):
                        try:
                            current_device = device_info['name']
                            current_device_state = device_info['state']
                            offset = int(client.file_read('/sys/block/{0}/{1}/start'.format(current_device, name))) * int(sector_size)
                            break
                        except Exception:
                            pass
                if current_device is None:
                    raise RuntimeError('Failed to retrieve the device information for current partition')
                mount_point = mount_point if mount_point != '' else None
                partition_state = Disk.STATES.OK if current_device_state == Disk.STATES.OK else Disk.STATES.FAILURE
                if mount_point is not None and fs_type != 'swap':
                    try:
                        filename = '{0}/{1}'.format(mount_point, str(time.time()))
                        client.run(['touch', filename])
                        client.run(['rm', filename])
                    except Exception:
                        partition_state = Disk.STATES.FAILURE

                configuration[current_device]['partitions'][offset] = {'size': int(size),
                                                                       'state': partition_state,
                                                                       'offset': offset,
                                                                       'aliases': system_aliases,
                                                                       'filesystem': fs_type if fs_type != '' else None,
                                                                       'mountpoint': mount_point}

        # Sync the model
        for disk in storagerouter.disks:
            disk_info = None
            for alias in disk.aliases:
                if alias in alias_name_mapping:
                    name = alias_name_mapping[alias].replace('/dev/', '')
                    if name in configuration:
                        disk_info = configuration.pop(name)
                        break

            if disk_info is None and disk.name in configuration and (disk.name.startswith('fio') or
                                                                     disk.name.startswith('loop') or
                                                                     disk.name.startswith('nvme')):  # Partitioned loop, nvme devices no longer show up in alias_name_mapping
                disk_info = configuration.pop(disk.name)

            # Remove disk / partitions if not reported by 'lsblk'
            if disk_info is None:
                DiskController._logger.info('Disk {0} - No longer found'.format(disk.name))
                delete = True
                for partition in disk.partitions:
                    if len(partition.roles) > 0:
                        delete = False
                        DiskController._logger.warning('Disk {0} - Partition with offset {1} - Has roles, will not delete'.format(disk.name, partition.offset))
                        break
                if delete is True:
                    for partition in disk.partitions:
                        partition.delete()
                    disk.delete()
                    DiskController._logger.info('Disk {0} - Deleted'.format(disk.name))
                else:
                    for partition in disk.partitions:
                        DiskController._update_partition(partition, {'state': 'MISSING'})
                        DiskController._logger.warning('Disk {0} - Partition with offset {1} - Updated status to MISSING'.format(disk.name, partition.offset))
                    DiskController._update_disk(disk, {'state': 'MISSING'})
                    DiskController._logger.warning('Disk {0} - Updated status to MISSING'.format(disk.name))

            else:  # Update existing disks and their partitions
                DiskController._logger.info('Disk {0} - Found, updating'.format(disk.name))
                DiskController._update_disk(disk, disk_info)
                partition_info = disk_info['partitions']
                for partition in disk.partitions:
                    if partition.offset not in partition_info:
                        DiskController._logger.info('Disk {0} - Partition with offset {1} - No longer found'.format(disk.name, partition.offset))
                        if len(partition.roles) > 0:
                            DiskController._logger.warning('Disk {0} - Partition with offset {1} - Update status to MISSING'.format(disk.name, partition.offset))
                            DiskController._update_partition(partition, {'state': 'MISSING'})
                        else:
                            DiskController._logger.info('Disk {0} - Partition with offset {1} - Deleting'.format(disk.name, partition.offset))
                            partition.delete()
                    else:
                        DiskController._update_partition(partition, partition_info.pop(partition.offset))
                for partition_offset in partition_info:
                    DiskController._logger.info('Disk {0} - Creating partition - {1}'.format(disk.name, partition_info[partition_offset]))
                    DiskController._create_partition(partition_info[partition_offset], disk)
        # Create all disks and their partitions not yet modeled
        for disk_name in configuration:
            DiskController._logger.info('Disk {0} - Creating disk - {1}'.format(disk_name, configuration[disk_name]))
            disk = Disk()
            disk.storagerouter = storagerouter
            disk.name = disk_name
            DiskController._update_disk(disk, configuration[disk_name])
            partition_info = configuration[disk_name]['partitions']
            for partition_offset in partition_info:
                DiskController._create_partition(partition_info[partition_offset], disk)

    @staticmethod
    def _create_partition(container, disk):
        """
        Models a partition
        """
        partition = DiskPartition()
        partition.disk = disk
        DiskController._update_partition(partition, container)

    @staticmethod
    def _update_partition(partition, container):
        """
        Updates a partition
        """
        for prop in ['filesystem', 'offset', 'state', 'aliases', 'mountpoint', 'size']:
            if prop in container:
                setattr(partition, prop, container[prop])
        partition.save()

    @staticmethod
    def _update_disk(disk, container):
        """
        Updates a disk
        """
        for prop in ['state', 'aliases', 'is_ssd', 'model', 'size', 'name', 'serial']:
            if prop in container:
                setattr(disk, prop, container[prop])
        disk.save()
