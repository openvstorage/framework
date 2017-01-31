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
import copy
import json
import random
import string
import hashlib


class OVSMigrator(object):
    """
    Handles all model related migrations
    """

    identifier = 'ovs'
    THIS_VERSION = 15

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
            from ovs.dal.helpers import HybridRunner, Descriptor
            from ovs.dal.hybrids.diskpartition import DiskPartition
            from ovs.dal.hybrids.j_storagedriverpartition import StorageDriverPartition
            from ovs.dal.lists.backendtypelist import BackendTypeList
            from ovs.dal.lists.diskpartitionlist import DiskPartitionList
            from ovs.dal.lists.storagedriverlist import StorageDriverList
            from ovs.dal.lists.storagerouterlist import StorageRouterList
            from ovs.dal.lists.vpoollist import VPoolList
            from ovs.extensions.generic.configuration import Configuration
            from ovs.extensions.generic.sshclient import SSHClient
            from ovs.extensions.generic.toolbox import Toolbox
            from ovs.extensions.services.service import ServiceManager
            from ovs.extensions.services.systemd import Systemd
            from ovs.extensions.storage.persistentfactory import PersistentFactory
            from ovs.extensions.storageserver.storagedriver import StorageDriverConfiguration
            from ovs.lib.disk import DiskController

            # Migrate unique constraints & indexes
            client = PersistentFactory.get_client()
            hybrid_structure = HybridRunner.get_hybrids()
            for class_descriptor in hybrid_structure.values():
                cls = Descriptor().load(class_descriptor).get_object()
                classname = cls.__name__.lower()
                unique_key = 'ovs_unique_{0}_{{0}}_'.format(classname)
                index_prefix = 'ovs_index_{0}|{{0}}|'.format(classname)
                index_key = 'ovs_index_{0}|{{0}}|{{1}}'.format(classname)
                uniques = []
                indexes = []
                # noinspection PyProtectedMember
                for prop in cls._properties:
                    if prop.unique is True and len([k for k in client.prefix(unique_key.format(prop.name))]) == 0:
                        uniques.append(prop.name)
                    if prop.indexed is True and len([k for k in client.prefix(index_prefix.format(prop.name))]) == 0:
                        indexes.append(prop.name)
                if len(uniques) > 0 or len(indexes) > 0:
                    prefix = 'ovs_data_{0}_'.format(classname)
                    for key, data in client.prefix_entries(prefix):
                        for property_name in uniques:
                            ukey = '{0}{1}'.format(unique_key.format(property_name), hashlib.sha1(str(data[property_name])).hexdigest())
                            client.set(ukey, key)
                        for property_name in indexes:
                            if property_name not in data:
                                continue  # This is the case when there's a new indexed property added.
                            ikey = index_key.format(property_name, hashlib.sha1(str(data[property_name])).hexdigest())
                            index = list(client.get_multi([ikey], must_exist=False))[0]
                            transaction = client.begin_transaction()
                            if index is None:
                                client.assert_value(ikey, None, transaction=transaction)
                                client.set(ikey, [key], transaction=transaction)
                            elif key not in index:
                                client.assert_value(ikey, index[:], transaction=transaction)
                                client.set(ikey, index + [key], transaction=transaction)
                            client.apply_transaction(transaction)

            # Complete rework of the way we detect devices to assign roles or use as ASD
            # Allow loop-, raid-, nvme-, ??-devices and logical volumes as ASD (https://github.com/openvstorage/framework/issues/792)
            for storagerouter in StorageRouterList.get_storagerouters():
                client = SSHClient(storagerouter.ip, username='root')

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

            # Only support ALBA Backend type
            for backend_type in BackendTypeList.get_backend_types():
                if backend_type.code != 'alba':
                    backend_type.delete()

            # Reformat the vpool.metadata information
            vpools = VPoolList.get_vpools()
            for vpool in vpools:
                if vpool.metadata.get('backend', {}).get('backend_info', {}).get('name') is not None:  # Already new format
                    continue
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
            for partition in DiskPartitionList.get_partitions():
                if 'READ' in partition.roles:
                    partition.roles.remove('READ')
                    partition.save()

            ####################
            # Multiple Proxies #
            ####################
            # Clean up - removal of obsolete 'cfgdir'
            paths = Configuration.get(key='/ovs/framework/paths')
            if 'cfgdir' in paths:
                paths.pop('cfgdir')
                Configuration.set(key='/ovs/framework/paths', value=paths)

            # Rewrite indices 'alba_proxy' --> 'alba_proxies'
            changes = False
            persistent_client = PersistentFactory.get_client()
            transaction = persistent_client.begin_transaction()
            for old_key in persistent_client.prefix('ovs_reverseindex_storagedriver'):
                if '|alba_proxy|' in old_key:
                    changes = True
                    new_key = old_key.replace('|alba_proxy|', '|alba_proxies|')
                    persistent_client.set(key=new_key, value=0, transaction=transaction)
                    persistent_client.delete(key=old_key, transaction=transaction)
            if changes is True:
                persistent_client.apply_transaction(transaction=transaction)

            sr_client_map = {}
            for storagedriver in StorageDriverList.get_storagedrivers():
                vpool = storagedriver.vpool
                root_client = None
                for alba_proxy in storagedriver.alba_proxies:
                    # Rename alba_proxy service in model
                    service = alba_proxy.service
                    old_service_name = 'albaproxy_{0}'.format(vpool.name)
                    new_service_name = 'albaproxy_{0}_0'.format(vpool.name)
                    if old_service_name != service.name:
                        continue
                    service.name = new_service_name
                    service.save()

                    if storagedriver.storagerouter_guid not in sr_client_map:
                        sr_client_map[storagedriver.storagerouter_guid] = SSHClient(endpoint=storagedriver.storagerouter.ip, username='root')
                    root_client = sr_client_map[storagedriver.storagerouter_guid]

                    # Add '-reboot' to alba_proxy services (because of newly created services and removal of old service)
                    if not ServiceManager.has_service(name=old_service_name, client=root_client):
                        continue
                    old_configuration_key = '/ovs/framework/hosts/{0}/services/{1}'.format(storagedriver.storagerouter.machine_id, old_service_name)
                    if not Configuration.exists(key=old_configuration_key):
                        continue

                    Toolbox.edit_version_file(client=root_client, package_name='alba', old_service_name=old_service_name, new_service_name=new_service_name)

                    # Register new service and remove old service
                    ServiceManager.add_service(name='ovs-albaproxy',
                                               client=root_client,
                                               params=Configuration.get(old_configuration_key),
                                               target_name='ovs-{0}'.format(new_service_name))

                    # Update scrub proxy config
                    proxy_config_key = '/ovs/vpools/{0}/proxies/{1}/config/main'.format(vpool.guid, alba_proxy.guid)
                    proxy_config = None if Configuration.exists(key=proxy_config_key) is False else Configuration.get(proxy_config_key)
                    if proxy_config is not None:
                        fragment_cache = proxy_config.get('fragment_cache', ['none', {}])
                        if fragment_cache[0] == 'alba' and fragment_cache[1].get('cache_on_write') is True:  # Accelerated ALBA configured
                            fragment_cache_scrub_info = copy.deepcopy(fragment_cache)
                            fragment_cache_scrub_info[1]['cache_on_read'] = False
                            proxy_scrub_config_key = '/ovs/vpools/{0}/proxies/scrub/generic_scrub'.format(vpool.guid)
                            proxy_scrub_config = None if Configuration.exists(key=proxy_scrub_config_key) is False else Configuration.get(proxy_scrub_config_key)
                            if proxy_scrub_config is not None and proxy_scrub_config['fragment_cache'] == ['none']:
                                proxy_scrub_config['fragment_cache'] = fragment_cache_scrub_info
                                Configuration.set(proxy_scrub_config_key, json.dumps(proxy_scrub_config, indent=4), raw=True)

                # Update 'backend_connection_manager' section
                storagedriver_config = StorageDriverConfiguration('storagedriver', vpool.guid, storagedriver.storagedriver_id)
                storagedriver_config.load()
                if 'backend_connection_manager' not in storagedriver_config.configuration or storagedriver_config.configuration.get('backend_connection_manager', {}).get('backend_type') == 'MULTI':
                    continue

                backend_connection_manager = {'backend_type': 'MULTI'}
                for index, proxy in enumerate(sorted(storagedriver.alba_proxies, key=lambda pr: pr.service.ports[0])):
                    backend_connection_manager[str(index)] = copy.deepcopy(storagedriver_config.configuration['backend_connection_manager'])
                    # noinspection PyUnresolvedReferences
                    backend_connection_manager[str(index)]['alba_connection_use_rora'] = True
                    # noinspection PyUnresolvedReferences
                    backend_connection_manager[str(index)]['alba_connection_rora_manifest_cache_capacity'] = 16 * 1024 ** 3
                storagedriver_config.clear_backend_connection_manager()
                storagedriver_config.configure_backend_connection_manager(**backend_connection_manager)
                storagedriver_config.save(root_client)

                # Add '-reboot' to volumedriver services (because of updated 'backend_connection_manager' section)
                Toolbox.edit_version_file(client=root_client, package_name='volumedriver', old_service_name='volumedriver_{0}'.format(vpool.name))
                if ServiceManager.ImplementationClass == Systemd:
                    root_client.run(['systemctl', 'daemon-reload'])

            # Introduction of DTL role (Replaces DTL sub_role)
            for vpool in vpools:
                for storagedriver in vpool.storagedrivers:
                    # noinspection PyProtectedMember
                    for junction_partition_guid, junction_partition_data in storagedriver.partitions._data.iteritems():
                        junction_partition_data = junction_partition_data.get('data', {})
                        if junction_partition_data.get('role') == DiskPartition.ROLES.WRITE and junction_partition_data.get('sub_role') == 'DTL':
                            junction_partition = StorageDriverPartition(junction_partition_guid)
                            junction_partition.role = DiskPartition.ROLES.DTL
                            junction_partition.sub_role = None
                            junction_partition.save()
                            if DiskPartition.ROLES.DTL not in junction_partition.partition.roles:
                                junction_partition.partition.roles.append(DiskPartition.ROLES.DTL)
                                junction_partition.partition.save()

        return OVSMigrator.THIS_VERSION
