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
OVS migration module
"""


class OVSMigrator(object):
    """
    Handles all model related migrations
    """

    identifier = 'ovs'  # Used by migrator.py, so don't remove
    THIS_VERSION = 11

    def __init__(self):
        """ Init method """
        pass

    @staticmethod
    def migrate(previous_version, master_ips=None, extra_ips=None):
        """
        Migrates from a given version to the current version. It uses 'previous_version' to be smart
        wherever possible, but the code should be able to migrate any version towards the expected version.
        When this is not possible, the code can set a minimum version and raise when it is not met.
        :param previous_version: The previous version from which to start the migration
        :type previous_version: float
        :param master_ips: IP addresses of the MASTER nodes
        :type master_ips: list or None
        :param extra_ips: IP addresses of the EXTRA nodes
        :type extra_ips: list or None
        """

        _ = master_ips, extra_ips
        working_version = previous_version

        # From here on, all actual migration should happen to get to the expected state for THIS RELEASE
        if working_version < OVSMigrator.THIS_VERSION:
            # Complete rework of the way we detect devices to assign roles or use as ASD
            # Allow loop-, raid-, nvme-, ??-devices and logical volumes as ASD (https://github.com/openvstorage/framework/issues/792)
            from ovs.dal.lists.storagerouterlist import StorageRouterList
            from ovs.extensions.generic.sshclient import SSHClient, UnableToConnectException
            from ovs.lib.disk import DiskController

            for storagerouter in StorageRouterList.get_storagerouters():
                try:
                    client = SSHClient(storagerouter, username='root')
                except UnableToConnectException:
                    raise

                # Retrieve all symlinks for all devices
                # Example of name_alias_mapping:
                # {'/dev/md0': ['/dev/disk/by-id/md-uuid-ad2de634:26d97253:5eda0a23:96986b76', '/dev/disk/by-id/md-name-OVS-1:0'],
                #  '/dev/sda': ['/dev/disk/by-path/pci-0000:03:00.0-sas-0x5000c295fe2ff771-lun-0'],
                #  '/dev/sda1': ['/dev/disk/by-uuid/e3e0bc62-4edc-4c6b-a6ce-1f39e8f27e41', '/dev/disk/by-path/pci-0000:03:00.0-sas-0x5000c295fe2ff771-lun-0-part1']}
                name_alias_mapping = {}
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

                for disk in storagerouter.disks:
                    if disk.aliases is None:
                        device_path = '/dev/{0}'.format(disk.name)
                        disk.aliases = name_alias_mapping.get(device_path, [device_path])
                        disk.save()

                DiskController.sync_with_reality(storagerouter_guid=storagerouter.guid)

        return OVSMigrator.THIS_VERSION
