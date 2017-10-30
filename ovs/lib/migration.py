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
    _logger = Logger(name='update', forced_target_type='file')

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

        ############################
        # Update some default values
        def _update_manifest_cache_size(_proxy_config_key):
            updated = False
            manifest_cache_size = 500 * 1024 * 1024
            if Configuration.exists(key=_proxy_config_key):
                _proxy_config = Configuration.get(key=_proxy_config_key)
                for cache_type in ['block_cache', 'fragment_cache']:
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
                _update_manifest_cache_size('/ovs/vpools/{0}/proxies/scrub/generic_scrub'.format(vpool.guid))  # Generic scrub proxy is deployed every time scrubbing kicks in, so no need to restart these services
                for alba_proxy in storagedriver.alba_proxies:
                    if _update_manifest_cache_size('/ovs/vpools/{0}/proxies/{1}/config/main'.format(vpool.guid, alba_proxy.guid)) is True:
                        # Add '-reboot' to alba_proxy services (because of newly created services and removal of old service)
                        ExtensionsToolbox.edit_version_file(client=root_client, package_name='alba', old_service_name=alba_proxy.service.name)

                # Update 'backend_connection_manager' section
                changes = False
                storagedriver_config = StorageDriverConfiguration(vpool.guid, storagedriver.storagedriver_id)
                if 'backend_connection_manager' not in storagedriver_config.configuration:
                    continue

                current_config = storagedriver_config.configuration['backend_connection_manager']
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
                    storagedriver_config.clear_backend_connection_manager()
                    storagedriver_config.configure_backend_connection_manager(**current_config)
                    storagedriver_config.save(root_client)

                    # Add '-reboot' to volumedriver services (because of updated 'backend_connection_manager' section)
                    ExtensionsToolbox.edit_version_file(client=root_client,
                                                        package_name='volumedriver',
                                                        old_service_name='volumedriver_{0}'.format(vpool.name))
            except Exception:
                MigrationController._logger.exception('Updating default configuration values failed for StorageDriver {0}'.format(storagedriver.storagedriver_id))

        ####################################################
        # Adding proxy fail fast as env variable for proxies
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
                if check in root_client.file_read(path):
                    continue

                try:
                    service_manager.regenerate_service(name='ovs-albaproxy', client=root_client, target_name=service_name)
                    changed_clients.add(root_client)
                    ExtensionsToolbox.edit_version_file(client=root_client, package_name='alba', old_service_name=service_name)
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
                support_config['support_agent'] = support_config.pop('enabled')
                support_config['remote_access'] = support_config.pop('enablesupport')
                Configuration.set(key=support_key, value=support_config)

                # Make sure once this finished, it never runs again by setting this key to True
                Configuration.set(key='/ovs/framework/migration|stats_monkey_integration', value=True)
            except Exception:
                MigrationController._logger.exception('Integration of stats monkey failed')


        ######################################
        # Introduction of version key
        if Configuration.get('key', default=False) not in ['community', 'enterprise']:
            try:
                storagerouters = StorageRouterList.get_storagerouters()
                for sr in storagerouters:
                    try:
                        val = sr.features['alba']['edition']
                        Configuration.set('/ovs/framework/edition', val)
                        break
                    except:
                        MigrationController._logger.exception('StorageRouter {0} did not yield edition value'.format(sr.name))
                        continue
            except Exception:
                MigrationController._logger.exception('Introduction of version key failed')
        MigrationController._logger.info('Finished out of band migrations')
