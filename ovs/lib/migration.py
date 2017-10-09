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
MigrationController module
"""
import copy
from ovs.extensions.generic.logger import Logger
from ovs.lib.helpers.decorators import ovs_task
from ovs.lib.helpers.toolbox import Schedule


class MigrationController(object):
    """
    This controller contains (part of the) migration code. It runs out-of-band with the updater so we reduce the risk of
    failures during the update
    """
    _logger = Logger('lib')

    @staticmethod
    @ovs_task(name='ovs.migration.migrate', schedule=Schedule(minute='0', hour='6'), ensure_single_info={'mode': 'DEFAULT'})
    def migrate():
        """
        Executes async migrations. It doesn't matter too much when they are executed, as long as they get eventually
        executed. This code will typically contain:
        * "dangerous" migration code (it needs certain running services)
        * Migration code depending on a cluster-wide state
        * ...
        """
        MigrationController._logger.info('Preparing out of band migrations...')

        from ovs.dal.lists.storagedriverlist import StorageDriverList
        from ovs.dal.lists.storagerouterlist import StorageRouterList
        from ovs.dal.lists.vpoollist import VPoolList
        from ovs.extensions.generic.configuration import Configuration
        from ovs.extensions.generic.sshclient import SSHClient
        from ovs_extensions.generic.toolbox import ExtensionsToolbox
        from ovs.extensions.migration.migration.ovsmigrator import OVSMigrator
        from ovs_extensions.services.interfaces.systemd import Systemd
        from ovs.extensions.services.servicefactory import ServiceFactory
        from ovs.extensions.storageserver.storagedriver import StorageDriverConfiguration

        MigrationController._logger.info('Start out of band migrations...')
        service_manager = ServiceFactory.get_manager()

        sr_client_map = {}
        for storagerouter in StorageRouterList.get_storagerouters():
            sr_client_map[storagerouter.guid] = SSHClient(endpoint=storagerouter,
                                                          username='root')

        #########################################################
        # Addition of 'ExecReload' for AlbaProxy SystemD services
        if ServiceFactory.get_service_type() == 'systemd':
            changed_clients = set()
            for storagedriver in StorageDriverList.get_storagedrivers():
                root_client = sr_client_map[storagedriver.storagerouter_guid]
                for alba_proxy in storagedriver.alba_proxies:
                    service = alba_proxy.service
                    service_name = 'ovs-{0}'.format(service.name)
                    if not service_manager.has_service(name=service_name, client=root_client):
                        continue
                    if 'ExecReload=' in root_client.file_read(filename='/lib/systemd/system/{0}.service'.format(service_name)):
                        continue

                    try:
                        service_manager.regenerate_service(name='ovs-albaproxy', client=root_client, target_name=service_name)
                        changed_clients.add(root_client)
                    except:
                        MigrationController._logger.exception('Error rebuilding service {0}'.format(service_name))
            for root_client in changed_clients:
                root_client.run(['systemctl', 'daemon-reload'])

        ##################################################################
        # Adjustment of open file descriptors for Arakoon services to 8192
        changed_clients = set()
        for storagerouter in StorageRouterList.get_storagerouters():
            root_client = sr_client_map[storagerouter.guid]
            for service_name in service_manager.list_services(client=root_client):
                if not service_name.startswith('ovs-arakoon-'):
                    continue

                if ServiceFactory.get_service_type() == 'systemd':
                    path = '/lib/systemd/system/{0}.service'.format(service_name)
                    check = 'LimitNOFILE=8192'
                else:
                    path = '/etc/init/{0}.conf'.format(service_name)
                    check = 'limit nofile 8192 8192'

                if not root_client.file_exists(path):
                    continue
                if check in root_client.file_read(path):
                    continue

                try:
                    service_manager.regenerate_service(name='ovs-arakoon', client=root_client, target_name=service_name)
                    changed_clients.add(root_client)
                    ExtensionsToolbox.edit_version_file(client=root_client, package_name='arakoon', old_service_name=service_name)
                except:
                    MigrationController._logger.exception('Error rebuilding service {0}'.format(service_name))
        for root_client in changed_clients:
            root_client.run(['systemctl', 'daemon-reload'])

        #############################
        # Migrate to multiple proxies
        for storagedriver in StorageDriverList.get_storagedrivers():
            vpool = storagedriver.vpool
            root_client = sr_client_map[storagedriver.storagerouter_guid]
            for alba_proxy in storagedriver.alba_proxies:
                # Rename alba_proxy service in model
                service = alba_proxy.service
                old_service_name = 'albaproxy_{0}'.format(vpool.name)
                new_service_name = 'albaproxy_{0}_0'.format(vpool.name)
                if old_service_name != service.name:
                    continue
                service.name = new_service_name
                service.save()

                if not service_manager.has_service(name=old_service_name, client=root_client):
                    continue
                old_configuration_key = '/ovs/framework/hosts/{0}/services/{1}'.format(storagedriver.storagerouter.machine_id, old_service_name)
                if not Configuration.exists(key=old_configuration_key):
                    continue

                # Add '-reboot' to alba_proxy services (because of newly created services and removal of old service)
                ExtensionsToolbox.edit_version_file(client=root_client,
                                                    package_name='alba',
                                                    old_service_name=old_service_name,
                                                    new_service_name=new_service_name)

                # Register new service and remove old service
                service_manager.add_service(name='ovs-albaproxy',
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
                            Configuration.set(key=proxy_scrub_config_key, value=proxy_scrub_config)

            # Update 'backend_connection_manager' section
            changes = False
            storagedriver_config = StorageDriverConfiguration(vpool.guid, storagedriver.storagedriver_id)
            if 'backend_connection_manager' not in storagedriver_config.configuration:
                continue

            current_config = storagedriver_config.configuration['backend_connection_manager']
            if current_config.get('backend_type') != 'MULTI':
                changes = True
                backend_connection_manager = {'backend_type': 'MULTI'}
                for index, proxy in enumerate(sorted(storagedriver.alba_proxies, key=lambda pr: pr.service.ports[0])):
                    backend_connection_manager[str(index)] = copy.deepcopy(current_config)
                    # noinspection PyUnresolvedReferences
                    backend_connection_manager[str(index)]['alba_connection_use_rora'] = True
                    # noinspection PyUnresolvedReferences
                    backend_connection_manager[str(index)]['alba_connection_rora_manifest_cache_capacity'] = 5000
                    # noinspection PyUnresolvedReferences
                    for key, value in backend_connection_manager[str(index)].items():
                        if key.startswith('backend_interface'):
                            backend_connection_manager[key] = value
                            # noinspection PyUnresolvedReferences
                            del backend_connection_manager[str(index)][key]
                for key, value in {'backend_interface_retries_on_error': 5,
                                   'backend_interface_retry_interval_secs': 1,
                                   'backend_interface_retry_backoff_multiplier': 2.0}.iteritems():
                    if key not in backend_connection_manager:
                        backend_connection_manager[key] = value
            else:
                backend_connection_manager = current_config
                for value in backend_connection_manager.values():
                    if isinstance(value, dict):
                        for key, val in value.items():
                            if key.startswith('backend_interface'):
                                backend_connection_manager[key] = val
                                changes = True
                                del value[key]
                for key, value in {'backend_interface_retries_on_error': 5,
                                   'backend_interface_retry_interval_secs': 1,
                                   'backend_interface_retry_backoff_multiplier': 2.0}.iteritems():
                    if key not in backend_connection_manager:
                        changes = True
                        backend_connection_manager[key] = value

            if changes is True:
                storagedriver_config.clear_backend_connection_manager()
                storagedriver_config.configure_backend_connection_manager(**backend_connection_manager)
                storagedriver_config.save(root_client)

                # Add '-reboot' to volumedriver services (because of updated 'backend_connection_manager' section)
                ExtensionsToolbox.edit_version_file(client=root_client,
                                                    package_name='volumedriver',
                                                    old_service_name='volumedriver_{0}'.format(vpool.name))
                if service_manager.__class__ == Systemd:
                    root_client.run(['systemctl', 'daemon-reload'])

        ########################################
        # Update metadata_store_bits information
        vpools = VPoolList.get_vpools()
        for vpool in vpools:
            bits = None
            for storagedriver in vpool.storagedrivers:
                key = '/ovs/framework/hosts/{0}/services/volumedriver_{1}'.format(storagedriver.storagerouter.machine_id, vpool.name)
                if Configuration.exists(key=key) and 'METADATASTORE_BITS' not in Configuration.get(key=key):
                    if bits is None:
                        entries = service_manager.extract_from_service_file(name='ovs-volumedriver_{0}'.format(vpool.name),
                                                                            client=sr_client_map[storagedriver.storagerouter_guid],
                                                                            entries=['METADATASTORE_BITS='])
                        if len(entries) == 1:
                            bits = entries[0].split('=')[-1]
                            bits = int(bits) if bits.isdigit() else 5
                    if bits is not None:
                        try:
                            content = Configuration.get(key=key)
                            content['METADATASTORE_BITS'] = bits
                            Configuration.set(key=key, value=content)
                        except:
                            MigrationController._logger.exception('Error updating volumedriver info for vPool {0} on StorageRouter {1}'.format(vpool.name, storagedriver.storagerouter.name))

            if bits is not None:
                vpool.metadata_store_bits = bits
                vpool.save()

        #####################################
        # Update the vPool metadata structure
        def _update_metadata_structure(metadata):
            metadata = copy.deepcopy(metadata)
            cache_structure = {'read': False,
                               'write': False,
                               'is_backend': False,
                               'quota': None,
                               'backend_info': {'name': None,  # Will be filled in when isBackend is true
                                                'backend_guid': None,
                                                'alba_backend_guid': None,
                                                'policies': None,
                                                'preset': None,
                                                'arakoon_config': None,
                                                'connection_info': {'client_id': None,
                                                                    'client_secret': None,
                                                                    'host': None,
                                                                    'port': None,
                                                                    'local': None}}
                               }
            structure_map = {'fragment_cache': {'read': 'fragment_cache_on_read',
                                                'write': 'fragment_cache_on_write',
                                                'quota': 'quota_fc',
                                                'backend_prefix': 'backend_aa_{0}'},
                             'block_cache': {'read': 'block_cache_on_read',
                                             'write': 'block_cache_on_write',
                                             'quota': 'quota_bc',
                                             'backend_prefix': 'backend_bc_{0}'}}
            if 'arakoon_config' in metadata['backend']:  # Arakoon config should be placed under the backend info
                metadata['backend']['backend_info']['arakoon_config'] = metadata['backend'].pop('arakoon_config')
            if 'connection_info' in metadata['backend']:  # Connection info sohuld be placed under the backend info
                metadata['backend']['backend_info']['connection_info'] = metadata['backend'].pop('connection_info')
            if 'caching_info' not in metadata:  # Caching info is the new key
                would_be_caching_info = {}
                metadata['caching_info'] = would_be_caching_info
                # Extract all caching data for every storagerouter
                current_caching_info = metadata['backend'].pop('caching_info')  # Pop to mutate metadata
                for storagerouter_guid in current_caching_info.iterkeys():
                    current_cache_data = current_caching_info[storagerouter_guid]
                    storagerouter_caching_info = {}
                    would_be_caching_info[storagerouter_guid] = storagerouter_caching_info
                    for cache_type, cache_type_mapping in structure_map.iteritems():
                        new_cache_structure = copy.deepcopy(cache_structure)
                        storagerouter_caching_info[cache_type] = new_cache_structure
                        for new_structure_key, old_structure_key in cache_type_mapping.iteritems():
                            if new_structure_key == 'backend_prefix':
                                # Get possible backend related info
                                metadata_key = old_structure_key.format(storagerouter_guid)
                                if metadata_key not in metadata:
                                    continue
                                backend_data = metadata.pop(metadata_key)  # Pop to mutate metadata
                                new_cache_structure['is_backend'] = True
                                # Copy over the old data
                                new_cache_structure['backend_info']['arakoon_config'] = backend_data['arakoon_config']
                                new_cache_structure['backend_info'].update(backend_data['backend_info'])
                                new_cache_structure['backend_info']['connection_info'].update(backend_data['connection_info'])
                            else:
                                new_cache_structure[new_structure_key] = current_cache_data.get(old_structure_key)
            return metadata

        vpools = VPoolList.get_vpools()
        for vpool in vpools:
            new_metadata = _update_metadata_structure(vpool.metadata)
            vpool.metadata = new_metadata
            vpool.save()

        ##############################################
        # Always use indent=4 during Configuration set
        def _resave_all_config_entries(config_path='/ovs'):
            """
            Recursive functions which checks every config management key if its a directory or not.
            If not a directory, we retrieve the config and just save it again using the new indentation logic
            """
            for item in Configuration.list(config_path):
                new_path = config_path + '/' + item
                print new_path
                if Configuration.dir_exists(new_path) is True:
                    _resave_all_config_entries(config_path=new_path)
                else:
                    try:
                        config = Configuration.get(new_path)
                        Configuration.set(new_path, config)
                    except:
                        config = Configuration.get(new_path, raw=True)
                        Configuration.set(new_path, config, raw=True)
        if OVSMigrator.THIS_VERSION <= 13:  # There is no way of checking whether this new indentation logic has been applied, so we only perform this for version 13 and lower
            MigrationController._logger.info('Re-saving every configuration setting with new indentation rules')
            _resave_all_config_entries()

        MigrationController._logger.info('Finished out of band migrations')
