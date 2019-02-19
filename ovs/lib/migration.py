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

import os
import copy
import json
import logging
from ovs.constants.logging import UPDATE_LOGGER
from ovs.extensions.storageserver.storagedriverconfig import AlbaConnectionConfig
from ovs.lib.helpers.decorators import ovs_task
from ovs.lib.helpers.toolbox import Schedule


class MigrationController(object):
    """
    This controller contains (part of the) migration code. It runs out-of-band with the updater so we reduce the risk of
    failures during the update
    """
    _logger = logging.getLogger(UPDATE_LOGGER)

    @staticmethod
    @ovs_task(name='ovs.migration.migrate', schedule=Schedule(minute='0', hour='6'), ensure_single_info={'mode': 'DEFAULT'})
    def migrate():
        """
        Executes async migrations. It doesn't matter too much when they are executed, as long as they get eventually
        executed. This code will typically contain:
        * "dangerous" migration code (it needs certain running services)
        * Migration code depending on a cluster-wide state
        * ...
        * Successfully finishing a piece of migration code, should create an entry in /ovs/framework/migration in case it should not be executed again
        *     Eg: /ovs/framework/migration|stats_monkey_integration: True
        """
        MigrationController._logger.info('Preparing out of band migrations...')

        from ovs.dal.lists.servicetypelist import ServiceTypeList
        from ovs.dal.lists.storagedriverlist import StorageDriverList
        from ovs.dal.lists.storagerouterlist import StorageRouterList
        from ovs.dal.lists.vpoollist import VPoolList
        from ovs_extensions.api.client import OVSClient
        from ovs.constants.storagedriver import CACHE_BLOCK, CACHE_FRAGMENT
        from ovs_extensions.constants.config import CONFIG_STORE_LOCATION
        from ovs_extensions.constants.framework import REMOTE_CONFIG_BACKEND_INI, REMOTE_CONFIG_BACKEND_BASE
        from ovs_extensions.constants.vpools import PROXY_CONFIG_MAIN, PROXY_CONFIG_PATH, GENERIC_SCRUB
        from ovs.extensions.db.arakooninstaller import ArakoonInstaller
        from ovs.extensions.generic.configuration import Configuration
        from ovs.extensions.generic.sshclient import SSHClient
        from ovs.extensions.generic.system import System
        from ovs_extensions.generic.toolbox import ExtensionsToolbox
        from ovs.extensions.migration.migration.ovsmigrator import ExtensionMigrator
        from ovs.extensions.packages.packagefactory import PackageFactory
        from ovs_extensions.services.interfaces.systemd import Systemd
        from ovs.extensions.services.servicefactory import ServiceFactory
        from ovs.extensions.storageserver.storagedriver import StorageDriverConfiguration
        from ovs.extensions.storage.volatilefactory import VolatileFactory
        from ovs.lib.helpers.storagedriver.installer import StorageDriverInstaller
        from ovs.lib.helpers.vpool.shared import VPoolShared
        from ovs.lib.nodeinstallation import NodeInstallationController

        MigrationController._logger.info('Start out of band migrations...')
        service_manager = ServiceFactory.get_manager()

        sr_client_map = {}
        for storagerouter in StorageRouterList.get_storagerouters():
            sr_client_map[storagerouter.guid] = SSHClient(endpoint=storagerouter.ip,  # Is triggered during post-update code too during which the ovs-watcher-framework service is still down and thus not refreshing the heartbeat --> use IP i/o StorageRouter
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
                        service_manager.regenerate_service(name=StorageDriverInstaller.SERVICE_TEMPLATE_PROXY, client=root_client, target_name=service_name)
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
                    ExtensionsToolbox.edit_version_file(client=root_client,
                                                        package_name='arakoon',
                                                        old_run_file='{0}/{1}.version'.format(ServiceFactory.RUN_FILE_DIR, service_name))
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
                                                    old_run_file='{0}/{1}.version'.format(ServiceFactory.RUN_FILE_DIR, old_service_name),
                                                    new_run_file='{0}/{1}.version'.format(ServiceFactory.RUN_FILE_DIR, new_service_name))

                # Register new service and remove old service
                service_manager.add_service(name=StorageDriverInstaller.SERVICE_TEMPLATE_PROXY,
                                            client=root_client,
                                            params=Configuration.get(old_configuration_key),
                                            target_name='ovs-{0}'.format(new_service_name))

                # Update scrub proxy config
                proxy_config_key = PROXY_CONFIG_MAIN.format(vpool.guid, alba_proxy.guid)
                proxy_config = None if Configuration.exists(key=proxy_config_key) is False else Configuration.get(proxy_config_key)
                if proxy_config is not None:
                    fragment_cache = proxy_config.get(CACHE_FRAGMENT, ['none', {}])
                    if fragment_cache[0] == 'alba' and fragment_cache[1].get('cache_on_write') is True:  # Accelerated ALBA configured
                        fragment_cache_scrub_info = copy.deepcopy(fragment_cache)
                        fragment_cache_scrub_info[1]['cache_on_read'] = False
                        proxy_scrub_config_key = GENERIC_SCRUB.format(vpool.guid)
                        proxy_scrub_config = None if Configuration.exists(key=proxy_scrub_config_key) is False else Configuration.get(proxy_scrub_config_key)
                        if proxy_scrub_config is not None and proxy_scrub_config[CACHE_FRAGMENT] == ['none']:
                            proxy_scrub_config[CACHE_FRAGMENT] = fragment_cache_scrub_info
                            Configuration.set(key=proxy_scrub_config_key, value=proxy_scrub_config)

            # Update 'backend_connection_manager' section
            changes = False
            storagedriver_config = StorageDriverConfiguration(vpool.guid, storagedriver.storagedriver_id)
            if 'backend_connection_manager' not in storagedriver_config.configuration:
                continue

            current_config = storagedriver_config.configuration.backend_config
            if current_config.backend_type != 'MULTI':
                changes = True
                backend_connection_manager = {'backend_type': 'MULTI'}
                for index, proxy in enumerate(sorted(storagedriver.alba_proxies, key=lambda pr: pr.service.ports[0])):
                    alba_config = AlbaConnectionConfig(alba_connection_use_rora=True,
                                  alba_connection_rora_manifest_cache_capacity=5000,
                                  **current_config.to_dict()[str(index)])
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
                storagedriver_config.configuration.backend_config = current_config
                storagedriver_config.save(root_client)

                # Add '-reboot' to volumedriver services (because of updated 'backend_connection_manager' section)
                ExtensionsToolbox.edit_version_file(client=root_client,
                                                    package_name='volumedriver',
                                                    old_run_file='{0}/{1}.version'.format(ServiceFactory.RUN_FILE_DIR, 'volumedriver_{0}'.format(vpool.name)))
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
                               'backend_info': {'name': None,  # Will be filled in when is_backend is true
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
            structure_map = {CACHE_BLOCK: {'read': 'block_cache_on_read',
                                                                      'write': 'block_cache_on_write',
                                                                      'quota': 'quota_bc',
                                                                      'backend_prefix': 'backend_bc_{0}'},
                             CACHE_FRAGMENT: {'read': 'fragment_cache_on_read',
                                                                         'write': 'fragment_cache_on_write',
                                                                         'quota': 'quota_fc',
                                                                         'backend_prefix': 'backend_aa_{0}'}}
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
            try:
                new_metadata = _update_metadata_structure(vpool.metadata)
                vpool.metadata = new_metadata
                vpool.save()
            except KeyError:
                MigrationController._logger.exception('Exceptions occurred when updating the metadata for vPool {0}'.format(vpool.name))

        ##############################################
        # Always use indent=4 during Configuration set
        def _resave_all_config_entries(config_path='/ovs'):
            """
            Recursive functions which checks every config management key if its a directory or not.
            If not a directory, we retrieve the config and just save it again using the new indentation logic
            """
            for item in Configuration.list(config_path):
                new_path = config_path + '/' + item
                if Configuration.dir_exists(new_path) is True:
                    _resave_all_config_entries(config_path=new_path)
                else:
                    try:
                        _config = Configuration.get(new_path)
                        Configuration.set(new_path, _config)
                    except:
                        _config = Configuration.get(new_path)
                        Configuration.set(new_path, _config)
        if ExtensionMigrator.THIS_VERSION <= 13:  # There is no way of checking whether this new indentation logic has been applied, so we only perform this for version 13 and lower
            MigrationController._logger.info('Re-saving every configuration setting with new indentation rules')
            _resave_all_config_entries()

        ############################
        # Update some default values
        def _update_manifest_cache_size(_proxy_config_key):
            updated = False
            manifest_cache_size = 500 * 1024 * 1024
            if Configuration.exists(key=_proxy_config_key):
                _proxy_config = Configuration.get(key=_proxy_config_key)
                for cache_type in [CACHE_BLOCK, CACHE_FRAGMENT]:
                    if cache_type in _proxy_config and _proxy_config[cache_type][0] == 'alba':
                        if _proxy_config[cache_type][1]['manifest_cache_size'] != manifest_cache_size:
                            updated = True
                            _proxy_config[cache_type][1]['manifest_cache_size'] = manifest_cache_size
                if _proxy_config['manifest_cache_size'] != manifest_cache_size:
                    updated = True
                    _proxy_config['manifest_cache_size'] = manifest_cache_size

                if updated is True:
                    Configuration.set(key=_proxy_config_key, value=_proxy_config)
            return updated

        for storagedriver in StorageDriverList.get_storagedrivers():
            try:
                vpool = storagedriver.vpool
                root_client = sr_client_map[storagedriver.storagerouter_guid]
                _update_manifest_cache_size(GENERIC_SCRUB.format(vpool.guid))  # Generic scrub proxy is deployed every time scrubbing kicks in, so no need to restart these services
                for alba_proxy in storagedriver.alba_proxies:
                    if _update_manifest_cache_size(PROXY_CONFIG_MAIN.format(vpool.guid, alba_proxy.guid)) is True:
                        # Add '-reboot' to alba_proxy services (because of newly created services and removal of old service)
                        ExtensionsToolbox.edit_version_file(client=root_client,
                                                            package_name='alba',
                                                            old_run_file='{0}/{1}.version'.format(ServiceFactory.RUN_FILE_DIR, alba_proxy.service.name))

                # Update 'backend_connection_manager' section
                changes = False
                storagedriver_config = StorageDriverConfiguration(vpool.guid, storagedriver.storagedriver_id)
                if 'backend_connection_manager' not in storagedriver_config.configuration:
                    continue

                current_config = storagedriver_config.configuration.backend_config
                for key, value in current_config.iteritems():
                    if key.isdigit() is True:
                        if value.get('alba_connection_asd_connection_pool_capacity') != 10:
                            changes = True
                            value['alba_connection_asd_connection_pool_capacity'] = 10
                        if value.get('alba_connection_timeout') != 30:
                            changes = True
                            value['alba_connection_timeout'] = 30
                        if value.get('alba_connection_rora_manifest_cache_capacity') != 25000:
                            changes = True
                            value['alba_connection_rora_manifest_cache_capacity'] = 25000

                if changes is True:
                    storagedriver_config.configuration.backend_config = current_config
                    storagedriver_config.save(root_client)

                    # Add '-reboot' to volumedriver services (because of updated 'backend_connection_manager' section)
                    ExtensionsToolbox.edit_version_file(client=root_client,
                                                        package_name='volumedriver',
                                                        old_run_file='{0}/{1}.version'.format(ServiceFactory.RUN_FILE_DIR, 'volumedriver_{0}'.format(vpool.name)))
            except Exception:
                MigrationController._logger.exception('Updating default configuration values failed for StorageDriver {0}'.format(storagedriver.storagedriver_id))

        ####################################################
        # Remove proxy fail fast as env variable for proxies
        changed_clients = set()
        for storagerouter in StorageRouterList.get_storagerouters():
            root_client = sr_client_map[storagerouter.guid]
            for service_name in service_manager.list_services(client=root_client):
                if not service_name.startswith('ovs-albaproxy_'):
                    continue

                if ServiceFactory.get_service_type() == 'systemd':
                    path = '/lib/systemd/system/{0}.service'.format(service_name)
                    check = 'Environment=ALBA_FAIL_FAST=true'
                else:
                    path = '/etc/init/{0}.conf'.format(service_name)
                    check = 'env ALBA_FAIL_FAST=true'

                if not root_client.file_exists(path):
                    continue
                if check not in root_client.file_read(path):
                    continue

                try:
                    service_manager.regenerate_service(name=StorageDriverInstaller.SERVICE_TEMPLATE_PROXY, client=root_client, target_name=service_name)
                    changed_clients.add(root_client)
                    ExtensionsToolbox.edit_version_file(client=root_client,
                                                        package_name='alba',
                                                        old_run_file='{0}/{1}.version'.format(ServiceFactory.RUN_FILE_DIR, service_name))
                except:
                    MigrationController._logger.exception('Error rebuilding service {0}'.format(service_name))
        for root_client in changed_clients:
            root_client.run(['systemctl', 'daemon-reload'])

        ######################################
        # Integration of stats monkey (2.10.2)
        if Configuration.get(key='/ovs/framework/migration|stats_monkey_integration', default=False) is False:
            try:
                # Get content of old key into new key
                old_stats_monkey_key = '/statsmonkey/statsmonkey'
                if Configuration.exists(key=old_stats_monkey_key) is True:
                    Configuration.set(key='/ovs/framework/monitoring/stats_monkey', value=Configuration.get(key=old_stats_monkey_key))
                    Configuration.delete(key=old_stats_monkey_key)

                # Make sure to disable the stats monkey by default or take over the current schedule if it was configured manually before
                celery_key = '/ovs/framework/scheduling/celery'
                current_value = None
                scheduling_config = Configuration.get(key=celery_key, default={})
                if 'statsmonkey.run_all_stats' in scheduling_config:  # Old celery task name of the stats monkey
                    current_value = scheduling_config.pop('statsmonkey.run_all_stats')
                scheduling_config['ovs.stats_monkey.run_all'] = current_value
                scheduling_config['alba.stats_monkey.run_all'] = current_value
                Configuration.set(key=celery_key, value=scheduling_config)

                support_key = '/ovs/framework/support'
                support_config = Configuration.get(key=support_key)
                support_config['support_agent'] = support_config.pop('enabled', True)
                support_config['remote_access'] = support_config.pop('enablesupport', False)
                Configuration.set(key=support_key, value=support_config)

                # Make sure once this finished, it never runs again by setting this key to True
                Configuration.set(key='/ovs/framework/migration|stats_monkey_integration', value=True)
            except Exception:
                MigrationController._logger.exception('Integration of stats monkey failed')

        ######################################################
        # Write away cluster ID to a file for back-up purposes
        try:
            cluster_id = Configuration.get(key='/ovs/framework/cluster_id', default=None)
            with open(CONFIG_STORE_LOCATION, 'r') as config_file:
                config = json.load(config_file)
            if cluster_id is not None and config.get('cluster_id', None) is None:
                config['cluster_id'] = cluster_id
                with open(CONFIG_STORE_LOCATION, 'w') as config_file:
                    json.dump(config, config_file, indent=4)
        except Exception:
            MigrationController._logger.exception('Writing cluster id to a file failed.')

        #########################################################
        # Additional string formatting in Arakoon services (2.11)
        try:
            if Configuration.get(key='/ovs/framework/migration|arakoon_service_update', default=False) is False:
                arakoon_service_names = [ArakoonInstaller.get_service_name_for_cluster(cluster_name=cluster_name) for cluster_name in Configuration.list(key='ovs/arakoon')]
                for storagerouter in StorageRouterList.get_masters():
                    for service_name in arakoon_service_names:
                        config_key = ServiceFactory.SERVICE_CONFIG_KEY.format(storagerouter.machine_id, service_name)
                        if Configuration.exists(key=config_key):
                            config = Configuration.get(key=config_key)
                            config['RUN_FILE_DIR'] = ServiceFactory.RUN_FILE_DIR
                            config['ARAKOON_PKG_NAME'] = PackageFactory.PKG_ARAKOON
                            config['ARAKOON_VERSION_CMD'] = PackageFactory.VERSION_CMD_ARAKOON
                            Configuration.set(key=config_key, value=config)
                # Make sure once this finished, it never runs again by setting this key to True
                Configuration.set(key='/ovs/framework/migration|arakoon_service_update', value=True)
        except Exception:
            MigrationController._logger.exception('Updating the string formatting for the Arakoon services failed')

        ############################################################
        # Additional string formatting in ALBA proxy services (2.11)
        changed_clients = set()
        try:
            if Configuration.get(key='/ovs/framework/migration|alba_proxy_service_update', default=False) is False:
                alba_pkg_name, alba_version_cmd = PackageFactory.get_package_and_version_cmd_for(component=PackageFactory.COMP_ALBA)
                for service in ServiceTypeList.get_by_name('AlbaProxy').services:
                    root_client = sr_client_map[service.storagerouter_guid]
                    config_key = ServiceFactory.SERVICE_CONFIG_KEY.format(service.storagerouter.machine_id, service.name)
                    if Configuration.exists(key=config_key):
                        config = Configuration.get(key=config_key)
                        config['RUN_FILE_DIR'] = ServiceFactory.RUN_FILE_DIR
                        config['ALBA_PKG_NAME'] = alba_pkg_name
                        config['ALBA_VERSION_CMD'] = alba_version_cmd
                        Configuration.set(key=config_key, value=config)
                        service_manager.regenerate_service(name=StorageDriverInstaller.SERVICE_TEMPLATE_PROXY,
                                                           client=root_client,
                                                           target_name='ovs-{0}'.format(service.name))
                        changed_clients.add(root_client)

                # Make sure once this finished, it never runs again by setting this key to True
                Configuration.set(key='/ovs/framework/migration|alba_proxy_service_update', value=True)
        except Exception:
            MigrationController._logger.exception('Updating the string formatting for the Arakoon services failed')

        ############################################################
        # Additional string formatting in DTL/VOLDRV services (2.11)
        try:
            if Configuration.get(key='/ovs/framework/migration|voldrv_service_update', default=False) is False:
                sd_pkg_name, sd_version_cmd = PackageFactory.get_package_and_version_cmd_for(component=PackageFactory.COMP_SD)
                for vpool in VPoolList.get_vpools():
                    for storagedriver in vpool.storagedrivers:
                        root_client = sr_client_map[storagedriver.storagerouter_guid]
                        for entry in ['dtl', 'volumedriver']:
                            service_name = '{0}_{1}'.format(entry, vpool.name)
                            service_template = StorageDriverInstaller.SERVICE_TEMPLATE_DTL if entry == 'dtl' else StorageDriverInstaller.SERVICE_TEMPLATE_SD
                            config_key = ServiceFactory.SERVICE_CONFIG_KEY.format(storagedriver.storagerouter.machine_id, service_name)
                            if Configuration.exists(key=config_key):
                                config = Configuration.get(key=config_key)
                                config['RUN_FILE_DIR'] = ServiceFactory.RUN_FILE_DIR
                                config['VOLDRV_PKG_NAME'] = sd_pkg_name
                                config['VOLDRV_VERSION_CMD'] = sd_version_cmd
                                Configuration.set(key=config_key, value=config)
                                service_manager.regenerate_service(name=service_template,
                                                                   client=root_client,
                                                                   target_name='ovs-{0}'.format(service_name))
                                changed_clients.add(root_client)

                # Make sure once this finished, it never runs again by setting this key to True
                Configuration.set(key='/ovs/framework/migration|voldrv_service_update', value=True)
        except Exception:
            MigrationController._logger.exception('Updating the string formatting for the Arakoon services failed')

        #######################################################
        # Storing actual package name in version files (2.11.0) (https://github.com/openvstorage/framework/issues/1876)
        if Configuration.get(key='/ovs/framework/migration|actual_package_name_in_version_file', default=False) is False:
            try:
                voldrv_pkg_name, _ = PackageFactory.get_package_and_version_cmd_for(component=PackageFactory.COMP_SD)
                for storagerouter in StorageRouterList.get_storagerouters():
                    root_client = sr_client_map.get(storagerouter.guid)
                    if root_client is None:
                        continue

                    for file_name in root_client.file_list(directory=ServiceFactory.RUN_FILE_DIR):
                        if not file_name.endswith('.version'):
                            continue
                        file_path = '{0}/{1}'.format(ServiceFactory.RUN_FILE_DIR, file_name)
                        contents = root_client.file_read(filename=file_path)
                        regenerate = False
                        if voldrv_pkg_name == PackageFactory.PKG_VOLDRV_SERVER:
                            if 'volumedriver-server' in contents:
                                regenerate = True
                                contents = contents.replace('volumedriver-server', PackageFactory.PKG_VOLDRV_SERVER)
                                root_client.file_write(filename=file_path, contents=contents)
                        elif voldrv_pkg_name == PackageFactory.PKG_VOLDRV_SERVER_EE:
                            if 'volumedriver-server' in contents or PackageFactory.PKG_VOLDRV_SERVER in contents:
                                regenerate = True
                                contents = contents.replace('volumedriver-server', PackageFactory.PKG_VOLDRV_SERVER_EE)
                                contents = contents.replace(PackageFactory.PKG_VOLDRV_SERVER, PackageFactory.PKG_VOLDRV_SERVER_EE)
                                root_client.file_write(filename=file_path, contents=contents)

                        if regenerate is True:
                            service_manager.regenerate_service(name=StorageDriverInstaller.SERVICE_TEMPLATE_DTL if file_name.startswith('dtl') else StorageDriverInstaller.SERVICE_TEMPLATE_SD,
                                                               client=root_client,
                                                               target_name='ovs-{0}'.format(file_name.split('.')[0]))  # Leave out .version
                            changed_clients.add(root_client)
                Configuration.set(key='/ovs/framework/migration|actual_package_name_in_version_file', value=True)
            except Exception:
                MigrationController._logger.exception('Updating actual package name for version files failed')

        for root_client in changed_clients:
            try:
                root_client.run(['systemctl', 'daemon-reload'])
            except Exception:
                MigrationController._logger.exception('Executing command "systemctl daemon-reload" failed')

        #########################################################
        # Addition of 'Environment=OCAMLRUNPARAM='b,a=1,s=4096k,O=50' for AlbaProxy SystemD services
        if ServiceFactory.get_service_type() == 'systemd':
            changed_clients = set()
            for storagedriver in StorageDriverList.get_storagedrivers():
                root_client = sr_client_map[storagedriver.storagerouter_guid]
                for alba_proxy in storagedriver.alba_proxies:
                    service = alba_proxy.service
                    service_name = 'ovs-{0}'.format(service.name)
                    if not service_manager.has_service(name=service_name, client=root_client):
                        continue
                    if "Environment=OCAMLRUNPARAM='b,a=1,s=4096k,O=50" in root_client.file_read(filename='/lib/systemd/system/{0}.service'.format(service_name)):
                        continue
                    try:
                        service_manager.regenerate_service(name='ovs-albaproxy', client=root_client, target_name=service_name)
                        changed_clients.add(root_client)
                    except:
                        MigrationController._logger.exception('Error rebuilding service {0}'.format(service_name))
            for root_client in changed_clients:
                root_client.run(['systemctl', 'daemon-reload'])
        #########################################################
        # Addition of 'Environment=OCAMLRUNPARAM='b,a=1,s=4096k,O=50' for Arakoon SystemD services
        if ServiceFactory.get_service_type() == 'systemd':
            changed_clients = set()
            for storagerouter in StorageRouterList.get_storagerouters():
                root_client = sr_client_map[storagerouter.guid]
                for service_name in service_manager.list_services(client=root_client):
                    if not service_name.startswith('ovs-arakoon-'):
                        continue
                    if not service_manager.has_service(name=service_name, client=root_client):
                        continue
                    if "Environment=OCAMLRUNPARAM='b,a=1,s=4096k,O=50" in root_client.file_read(filename='/lib/systemd/system/{0}.service'.format(service_name)):
                        continue
                    try:
                        service_manager.regenerate_service(name='ovs-arakoon', client=root_client, target_name=service_name)
                        changed_clients.add(root_client)
                    except:
                        MigrationController._logger.exception('Error rebuilding service {0}'.format(service_name))
            for root_client in changed_clients:
                root_client.run(['systemctl', 'daemon-reload'])

        ###################################################
        # Disable MDS checkup job by default
        mds_catch_up_migration_key = '/ovs/framework/migration|mds_catch_up'
        if Configuration.get(key=mds_catch_up_migration_key, default=False) is False:
            try:
                celery_key = '/ovs/framework/scheduling/celery'
                catch_up_key = 'ovs.mds.mds_catchup'
                scheduling_config = Configuration.get(key=celery_key, default={})
                if catch_up_key not in scheduling_config:
                    scheduling_config[catch_up_key] = None  # Disable
                    Configuration.set(key=celery_key, value=scheduling_config)
                Configuration.set(mds_catch_up_migration_key, True)
            except:
                MigrationController._logger.exception('Integration of mds_catch_up failed')

        ###################################################
        # The components need to register themselves to avoid throwing the configuration away
        for storagerouter in StorageRouterList.get_storagerouters():
            registration_key = Configuration.generate_registration_key(storagerouter.machine_id)
            if System.get_component_identifier() not in Configuration.get(registration_key, default=[]):
                Configuration.register_usage(System.get_component_identifier(), registration_key)

        ####################################################
        # Deduplicate configs of proxies: now one config per remote alba-backend
        errors = []
        try:
            present_remote_configs = list(Configuration.list(REMOTE_CONFIG_BACKEND_BASE))

            for vpool in VPoolList.get_vpools():
                vpool_errors = []
                metadata = vpool.metadata
                backend_info = metadata['backend']['backend_info']
                main_backend_guid = backend_info['alba_backend_guid']
                connection_info = backend_info['connection_info']

                if main_backend_guid not in present_remote_configs:
                    try:
                        client = OVSClient.get_instance(connection_info=connection_info, cache_store=VolatileFactory.get_client())
                        VPoolShared.sync_alba_arakoon_config(main_backend_guid, client)
                        present_remote_configs.append(main_backend_guid)
                    except Exception:
                        vpool_errors.append('Failed to sync up remote config of remote backend {0} of vPool {1}'.format(main_backend_guid, vpool.guid))
                        if main_backend_guid not in present_remote_configs:
                            # skip this update of proxies, cause no change of configs can happen
                            continue

                for storagedriver in vpool.storagedrivers:
                    storagerouter = storagedriver.storagerouter
                    storagerouter_info = metadata['caching_info'][storagerouter.guid]
                    for proxy in storagedriver.alba_proxies:
                        proxy_errors = False
                        try:
                            main_proxy_config_path = PROXY_CONFIG_MAIN.format(vpool.guid, proxy.guid)
                            main_proxy_config = Configuration.get(main_proxy_config_path)
                            # No other configs should be deleted in the config mgmt or if errors occurred: don't remove the old configs just yet, as they might be of use for manual intervention.
                            # update config of main general backend of the vPool
                            if not REMOTE_CONFIG_BACKEND_BASE in main_proxy_config['albamgr_cfg_url']:
                                main_proxy_config['albamgr_cfg_url'] = Configuration.get_configuration_path(REMOTE_CONFIG_BACKEND_INI.format(main_backend_guid))

                            # Update caching info
                            for cache_type in [CACHE_FRAGMENT, CACHE_BLOCK]:
                                if storagerouter_info[cache_type]['is_backend']:
                                    backend_info = storagerouter_info[cache_type]['backend_info']
                                    # Make sure replacing config is present
                                    alba_guid = backend_info['alba_backend_guid']
                                    connection_info = backend_info['connection_info']
                                    try:
                                        if REMOTE_CONFIG_BACKEND_BASE in main_proxy_config[cache_type][1]['albamgr_cfg_url']:
                                            client = OVSClient.get_instance(connection_info=connection_info, cache_store=VolatileFactory.get_client())
                                            if alba_guid not in present_remote_configs:
                                                # If the proxy is already in this list, we can safely assume that it has been recently added and does not need resyncing.
                                                # Only if it is absent, connect to the backend and fetch it.
                                                VPoolShared.sync_alba_arakoon_config(alba_guid, client)
                                            main_proxy_config[cache_type][1]['albamgr_cfg_url'] = Configuration.get_configuration_path(REMOTE_CONFIG_BACKEND_INI.format(alba_guid))
                                    except Exception as ex:
                                        proxy_errors = True
                                        errors.append('Failed to update proxy {0} of vPool {1}: {2}'.format(proxy.guid, vpool.guid, ex))
                                        continue
                            # Set updated config.
                            transaction_id = Configuration.begin_transaction()
                            Configuration.set(main_proxy_config_path, main_proxy_config, transaction=transaction_id)

                            # Now remove old configs of proxies
                            config_path = PROXY_CONFIG_PATH.format(vpool.guid, proxy.guid)
                            other_cache_configs = list(Configuration.list(config_path))
                            if other_cache_configs and not proxy_errors:
                                other_cache_configs.remove('main')
                                for other_cache_config in other_cache_configs:
                                    config_to_delete = os.path.join(config_path, other_cache_config)
                                    Configuration.delete(config_to_delete, transaction=transaction_id)
                            Configuration.apply_transaction(transaction=transaction_id)

                        except Exception as ex:
                            errors.append('Failed to update config for proxy {0} of vPool {1}: {2}'.format(proxy.guid, vpool.guid, ex))
                            continue
                if vpool_errors:
                    MigrationController._logger.info('Failed to update vPool info: {0}'.format(vpool_errors))
        except Exception as ex:
            errors.append('Unexpected exception occured while migrating proxies: {0}'.format(ex))
        finally:
            if errors:
                MigrationController._logger.info('Failed to update migration of proxies: {0}'.format('\n'.join(errors)))

            else:
                MigrationController._logger.info('Finished migration of all proxies on environment')

        ####################################################
        # Introduce ipython profile for logging
        errors = []
        try:
            for storagerouter_guid, client in sr_client_map.iteritems():
                try:
                    NodeInstallationController.setup_ipython_logging([client])
                except Exception as ex:
                    errors.append('Failed to configure Ipython profile for {0}: {1}'.format(storagerouter_guid, ex))
        except Exception as ex:
            errors.append('Unexpected exception occured while configuring ipython profile: {0}'.format(ex))
        finally:
            if errors:
                MigrationController._logger.info('Failed to update migration of proxies: {0}'.format('\n'.join(errors)))

            else:
                MigrationController._logger.info('Finished migration of all proxies on environment')
        MigrationController._logger.info('Finished out of band migrations')