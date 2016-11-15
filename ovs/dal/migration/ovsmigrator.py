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

import hashlib
import random
import string


class OVSMigrator(object):
    """
    Handles all model related migrations
    """

    identifier = 'ovs'
    THIS_VERSION = 12

    def __init__(self):
        """ Init method """
        pass

    @staticmethod
    def migrate(previous_version):
        """
        Migrates from a given version to the current version. It uses 'previous_version' to be smart
        wherever possible, but the code should be able to migrate any version towards the expected version.
        When this is not possible, the code can set a minimum version and raise when it is not met.
        :param previous_version: The previous version from which to start the migration
        :type previous_version: float
        """

        working_version = previous_version

        if working_version == 0:
            # Initial version:
            # * Set the version to THIS RELEASE version

            from ovs.dal.hybrids.user import User
            from ovs.dal.hybrids.group import Group
            from ovs.dal.hybrids.role import Role
            from ovs.dal.hybrids.client import Client
            from ovs.dal.hybrids.j_rolegroup import RoleGroup
            from ovs.dal.hybrids.j_roleclient import RoleClient
            from ovs.dal.hybrids.servicetype import ServiceType
            from ovs.dal.hybrids.branding import Branding
            from ovs.dal.lists.backendtypelist import BackendTypeList

            # Create groups
            admin_group = Group()
            admin_group.name = 'administrators'
            admin_group.description = 'Administrators'
            admin_group.save()
            viewers_group = Group()
            viewers_group.name = 'viewers'
            viewers_group.description = 'Viewers'
            viewers_group.save()

            # Create users
            admin = User()
            admin.username = 'admin'
            admin.password = hashlib.sha256('admin').hexdigest()
            admin.is_active = True
            admin.group = admin_group
            admin.save()

            # Create internal OAuth 2 clients
            admin_pw_client = Client()
            admin_pw_client.ovs_type = 'INTERNAL'
            admin_pw_client.grant_type = 'PASSWORD'
            admin_pw_client.user = admin
            admin_pw_client.save()
            admin_cc_client = Client()
            admin_cc_client.ovs_type = 'INTERNAL'
            admin_cc_client.grant_type = 'CLIENT_CREDENTIALS'
            admin_cc_client.client_secret = ''.join(random.choice(string.ascii_letters +
                                                                  string.digits +
                                                                  '|_=+*#@!/-[]{}<>.?,\'";:~')
                                                    for _ in range(128))
            admin_cc_client.user = admin
            admin_cc_client.save()

            # Create roles
            read_role = Role()
            read_role.code = 'read'
            read_role.name = 'Read'
            read_role.description = 'Can read objects'
            read_role.save()
            write_role = Role()
            write_role.code = 'write'
            write_role.name = 'Write'
            write_role.description = 'Can write objects'
            write_role.save()
            manage_role = Role()
            manage_role.code = 'manage'
            manage_role.name = 'Manage'
            manage_role.description = 'Can manage the system'
            manage_role.save()

            # Attach groups to roles
            mapping = [
                (admin_group, [read_role, write_role, manage_role]),
                (viewers_group, [read_role])
            ]
            for setting in mapping:
                for role in setting[1]:
                    rolegroup = RoleGroup()
                    rolegroup.group = setting[0]
                    rolegroup.role = role
                    rolegroup.save()
                for user in setting[0].users:
                    for role in setting[1]:
                        for client in user.clients:
                            roleclient = RoleClient()
                            roleclient.client = client
                            roleclient.role = role
                            roleclient.save()

            # Add service types
            for service_type_info in [ServiceType.SERVICE_TYPES.MD_SERVER, ServiceType.SERVICE_TYPES.ALBA_PROXY, ServiceType.SERVICE_TYPES.ARAKOON]:
                service_type = ServiceType()
                service_type.name = service_type_info
                service_type.save()

            # Branding
            branding = Branding()
            branding.name = 'Default'
            branding.description = 'Default bootstrap theme'
            branding.css = 'bootstrap-default.min.css'
            branding.productname = 'Open vStorage'
            branding.is_default = True
            branding.save()
            slate = Branding()
            slate.name = 'Slate'
            slate.description = 'Dark bootstrap theme'
            slate.css = 'bootstrap-slate.min.css'
            slate.productname = 'Open vStorage'
            slate.is_default = False
            slate.save()

        # From here on, all actual migration should happen to get to the expected state for THIS RELEASE
        elif working_version < OVSMigrator.THIS_VERSION:
            # Migrate unique constraints
            from ovs.dal.helpers import HybridRunner, Descriptor
            from ovs.extensions.storage.persistentfactory import PersistentFactory
            client = PersistentFactory.get_client()
            hybrid_structure = HybridRunner.get_hybrids()
            for class_descriptor in hybrid_structure.values():
                cls = Descriptor().load(class_descriptor).get_object()
                classname = cls.__name__.lower()
                unique_key = 'ovs_unique_{0}_{{0}}_'.format(classname)
                uniques = []
                # noinspection PyProtectedMember
                for prop in cls._properties:
                    if prop.unique is True and len([k for k in client.prefix(unique_key.format(prop.name))]) == 0:
                        uniques.append(prop.name)
                if len(uniques) > 0:
                    prefix = 'ovs_data_{0}_'.format(classname)
                    for key in client.prefix(prefix):
                        data = client.get(key)
                        for property_name in uniques:
                            ukey = '{0}{1}'.format(unique_key.format(property_name), hashlib.sha1(str(data[property_name])).hexdigest())
                            client.set(ukey, key)

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

                for disk in storagerouter.disks:
                    if disk.aliases is None:
                        # noinspection PyProtectedMember
                        device_path = '/dev/{0}'.format(disk.name)
                        disk.aliases = name_alias_mapping.get(device_path, [device_path])
                        disk.save()
                    for partition in disk.partitions:
                        if partition.aliases is None:
                            # noinspection PyProtectedMember
                            partition_device = alias_name_mapping.get(partition._data.get('path'))
                            if partition_device is None:
                                partition.aliases = []
                                partition.save()
                                continue
                            partition.aliases = name_alias_mapping.get(partition_device, [])
                            partition.save()

                DiskController.sync_with_reality(storagerouter_guid=storagerouter.guid)

            # Only support ALBA backend type
            from ovs.dal.lists.backendtypelist import BackendTypeList
            for backend_type in BackendTypeList.get_backend_types():
                if backend_type.code != 'alba':
                    backend_type.delete()

            # Reformat the vpool.metadata information
            from ovs.dal.lists.vpoollist import VPoolList
            for vpool in VPoolList.get_vpools():
                new_metadata = {}
                for metadata_key, value in vpool.metadata.items():
                    new_info = {}
                    storagerouter_guids = [key for key in vpool.metadata.keys() if not key.startswith('backend')]
                    if isinstance(value, dict):
                        read_cache = value.get('backend_info', {}).get('fragment_cache_on_read', True)
                        write_cache = value.get('backend_info', {}).get('fragment_cache_on_write', False)
                        new_info['backend_info'] = {'alba_backend_guid': value.get('backend_guid'),
                                                    'backend_guid': None,
                                                    'frag_size': value.get('backend_info', {}).get('frag_size'),
                                                    'name': value.get('name'),
                                                    'policies': value.get('backend_info', {}).get('policies'),
                                                    'preset': value.get('preset'),
                                                    'sco_size': value.get('backend_info', {}).get('sco_size'),
                                                    'total_size': value.get('backend_info', {}).get('total_size')}
                        new_info['arakoon_config'] = value.get('arakoon_config')
                        new_info['connection_info'] = {'host': value.get('connection', {}).get('host', ''),
                                                       'port': value.get('connection', {}).get('port', ''),
                                                       'local': value.get('connection', {}).get('local', ''),
                                                       'client_id': value.get('connection', {}).get('client_id', ''),
                                                       'client_secret': value.get('connection', {}).get('client_secret', '')}
                        if metadata_key == 'backend':
                            new_info['caching_info'] = dict((sr_guid, {'fragment_cache_on_read': read_cache, 'fragment_cache_on_write': write_cache}) for sr_guid in storagerouter_guids)
                    if metadata_key in storagerouter_guids:
                        metadata_key = 'backend_aa_{0}'.format(metadata_key)
                    new_metadata[metadata_key] = new_info
                vpool.metadata = new_metadata
                vpool.save()

            # Removal of READ role
            from ovs.dal.lists.diskpartitionlist import DiskPartitionList
            for partition in DiskPartitionList.get_partitions():
                if 'READ' in partition.roles:
                    partition.roles.remove('READ')
                    partition.save()

        return OVSMigrator.THIS_VERSION
