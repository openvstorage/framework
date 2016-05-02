# Copyright 2016 iNuron NV
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Module for UpdateController
"""

import os
import json
import subprocess
from ovs.extensions.generic.filemutex import file_mutex
from ovs.extensions.generic.filemutex import NoLockAvailableException
from ovs.extensions.generic.remote import remote
from ovs.extensions.generic.sshclient import SSHClient
from ovs.extensions.migration.migrator import Migrator
from ovs.extensions.packages.package import PackageManager
from ovs.extensions.services.service import ServiceManager
from ovs.lib.helpers.decorators import add_hooks
from ovs.lib.helpers.toolbox import Toolbox
from ovs.log.logHandler import LogHandler


class UpdateController(object):
    """
    This class contains all logic for updating an environment
    """
    _logger = LogHandler.get('lib', name='update')
    _logger.logger.propagate = False
    model_services = ['memcached', 'arakoon-ovsdb']

    @staticmethod
    def update_framework():
        """
        Update the framework
        :return: None
        """
        filemutex = file_mutex('system_update', wait=2)
        upgrade_file = '/etc/ready_for_upgrade'
        upgrade_ongoing_check_file = '/etc/upgrade_ongoing'
        ssh_clients = []
        try:
            filemutex.acquire()
            UpdateController._log_message('+++ Starting framework update +++')

            from ovs.dal.lists.storagerouterlist import StorageRouterList

            UpdateController._log_message('Generating SSH client connections for each storage router')
            upgrade_file = '/etc/ready_for_upgrade'
            upgrade_ongoing_check_file = '/etc/upgrade_ongoing'
            storage_routers = StorageRouterList.get_storagerouters()
            ssh_clients = []
            master_ips = []
            extra_ips = []
            for sr in storage_routers:
                ssh_clients.append(SSHClient(sr.ip, username='root'))
                if sr.node_type == 'MASTER':
                    master_ips.append(sr.ip)
                elif sr.node_type == 'EXTRA':
                    extra_ips.append(sr.ip)
            this_client = [client for client in ssh_clients if client.is_local is True][0]

            # Create locks
            UpdateController._log_message('Creating lock files', client_ip=this_client.ip)
            for client in ssh_clients:
                client.run('touch {0}'.format(upgrade_file))  # Prevents manual install or upgrade individual packages
                client.run('touch {0}'.format(upgrade_ongoing_check_file))  # Prevents clicking x times on 'Update' btn

            # Check requirements
            packages_to_update = set()
            all_services_to_restart = []
            for client in ssh_clients:
                for function in Toolbox.fetch_hooks('update', 'metadata'):
                    UpdateController._log_message('Executing function {0}'.format(function.__name__),
                                                  client_ip=client.ip)
                    output = function(client)
                    for key, value in output.iteritems():
                        if key != 'framework':
                            continue
                        for package_info in value:
                            packages_to_update.update(package_info['packages'])
                            all_services_to_restart += package_info['services']

            services_to_restart = []
            for service in all_services_to_restart:
                if service not in services_to_restart:
                    services_to_restart.append(service)  # Filter out duplicates maintaining the order of services (eg: watcher-framework before memcached)

            UpdateController._log_message('Services which will be restarted --> {0}'.format(', '.join(services_to_restart)))
            UpdateController._log_message('Packages which will be installed --> {0}'.format(', '.join(packages_to_update)))

            # Stop services
            if UpdateController._change_services_state(services=services_to_restart,
                                                       ssh_clients=ssh_clients,
                                                       action='stop') is False:
                UpdateController._log_message('Stopping all services on every node failed, cannot continue',
                                              client_ip=this_client.ip, severity='warning')
                UpdateController._remove_lock_files([upgrade_file, upgrade_ongoing_check_file], ssh_clients)

                # Start services again if a service could not be stopped
                UpdateController._log_message('Attempting to start the services again', client_ip=this_client.ip)
                UpdateController._change_services_state(services=services_to_restart,
                                                        ssh_clients=ssh_clients,
                                                        action='start')

                UpdateController._log_message('Failed to stop all required services, aborting update',
                                              client_ip=this_client.ip, severity='error')
                return

            # Update packages
            failed_clients = []
            for client in ssh_clients:
                PackageManager.update(client=client)
                try:
                    UpdateController._log_message('Installing latest packages', client.ip)
                    for package in packages_to_update:
                        UpdateController._log_message('Installing {0}'.format(package), client.ip)
                        PackageManager.install(package_name=package,
                                               client=client,
                                               force=True)
                        UpdateController._log_message('Installed {0}'.format(package), client.ip)
                    client.file_delete(upgrade_file)
                except subprocess.CalledProcessError as cpe:
                    UpdateController._log_message('Upgrade failed with error: {0}'.format(cpe.output), client.ip,
                                                  'error')
                    failed_clients.append(client)
                    break

            if failed_clients:
                UpdateController._remove_lock_files([upgrade_file, upgrade_ongoing_check_file], ssh_clients)
                UpdateController._log_message('Error occurred. Attempting to start all services again',
                                              client_ip=this_client.ip, severity='error')
                UpdateController._change_services_state(services=services_to_restart,
                                                        ssh_clients=ssh_clients,
                                                        action='start')
                UpdateController._log_message('Failed to upgrade following nodes:\n - {0}\nPlease check /var/log/ovs/lib.log on {1} for more information'.format('\n - '.join([client.ip for client in failed_clients]), this_client.ip),
                                              this_client.ip,
                                              'error')
                return

            # Migrate code
            for client in ssh_clients:
                try:
                    UpdateController._log_message('Started code migration', client.ip)
                    try:
                        with remote(client.ip, [Migrator]) as rem:
                            rem.Migrator.migrate(master_ips, extra_ips)
                    except EOFError as eof:
                        UpdateController._log_message('EOFError during code migration, retrying {0}'.format(eof), client.ip, 'warning')
                        with remote(client.ip, [Migrator]) as rem:
                            rem.Migrator.migrate(master_ips, extra_ips)
                    UpdateController._log_message('Finished code migration', client.ip)
                except Exception as ex:
                    UpdateController._remove_lock_files([upgrade_ongoing_check_file], ssh_clients)
                    UpdateController._log_message('Code migration failed with error: {0}'.format(ex), client.ip, 'error')
                    return

            # Start services
            UpdateController._log_message('Starting services', client_ip=this_client.ip)
            model_services = []
            if 'arakoon-ovsdb' in services_to_restart:
                model_services.append('arakoon-ovsdb')
                services_to_restart.remove('arakoon-ovsdb')
            if 'memcached' in services_to_restart:
                model_services.append('memcached')
                services_to_restart.remove('memcached')
            UpdateController._change_services_state(services=model_services,
                                                    ssh_clients=ssh_clients,
                                                    action='start')

            # Migrate model
            UpdateController._log_message('Started model migration', client_ip=this_client.ip)
            try:
                from ovs.dal.helpers import Migration
                with remote(ssh_clients[0].ip, [Migration]) as rem:
                    rem.Migration.migrate()
                UpdateController._log_message('Finished model migration', client_ip=this_client.ip)
            except Exception as ex:
                UpdateController._remove_lock_files([upgrade_ongoing_check_file], ssh_clients)
                UpdateController._log_message('An unexpected error occurred: {0}'.format(ex), client_ip=this_client.ip,
                                              severity='error')
                return

            # Post upgrade actions
            UpdateController._log_message('Executing post upgrade actions', client_ip=this_client.ip)
            for client in ssh_clients:
                with remote(client.ip, [Toolbox, SSHClient]) as rem:
                    for function in rem.Toolbox.fetch_hooks('update', 'postupgrade'):
                        UpdateController._log_message('Executing action {0}'.format(function.__name__),
                                                      client_ip=client.ip)
                        try:
                            function(rem.SSHClient(client.ip, username='root'))
                            UpdateController._log_message('Executing action {0} completed'.format(function.__name__),
                                                          client_ip=client.ip)
                        except Exception as ex:
                            UpdateController._log_message('Post upgrade action failed with error: {0}'.format(ex),
                                                          client.ip, 'error')

            # Start watcher and restart support-agent
            UpdateController._change_services_state(services=services_to_restart,
                                                    ssh_clients=ssh_clients,
                                                    action='start')
            UpdateController._change_services_state(services=['support-agent'],
                                                    ssh_clients=ssh_clients,
                                                    action='restart')

            UpdateController._remove_lock_files([upgrade_ongoing_check_file], ssh_clients)
            UpdateController._log_message('+++ Finished updating +++')
        except RuntimeError as rte:
            UpdateController._log_message('Error during framework update: {0}'.format(rte), severity='error')
            UpdateController._remove_lock_files([upgrade_file, upgrade_ongoing_check_file], ssh_clients)
        except NoLockAvailableException:
            UpdateController._log_message('Another framework update is currently in progress!')
        except Exception as ex:
            UpdateController._log_message('Error during framework update: {0}'.format(ex), severity='error')
            UpdateController._remove_lock_files([upgrade_file, upgrade_ongoing_check_file], ssh_clients)
        finally:
            filemutex.release()

    @staticmethod
    def update_volumedriver():
        """
        Update the volumedriver
        :return: None
        """
        filemutex = file_mutex('system_update', wait=2)
        upgrade_file = '/etc/ready_for_upgrade'
        upgrade_ongoing_check_file = '/etc/upgrade_ongoing'
        ssh_clients = []
        try:
            filemutex.acquire()
            UpdateController._log_message('+++ Starting volumedriver update +++')

            from ovs.dal.lists.storagerouterlist import StorageRouterList

            UpdateController._log_message('Generating SSH client connections for each storage router')
            storage_routers = StorageRouterList.get_storagerouters()
            ssh_clients = [SSHClient(storage_router.ip, 'root') for storage_router in storage_routers]
            this_client = [client for client in ssh_clients if client.is_local is True][0]

            # Commence update !!!!!!!
            # 0. Create locks
            UpdateController._log_message('Creating lock files', client_ip=this_client.ip)
            for client in ssh_clients:
                client.run('touch {0}'.format(upgrade_file))  # Prevents manual install or upgrade individual packages
                client.run('touch {0}'.format(upgrade_ongoing_check_file))  # Prevents clicking x times on 'Update' btn

            # 1. Check requirements
            packages_to_update = set()
            all_services_to_restart = []
            for client in ssh_clients:
                for function in Toolbox.fetch_hooks('update', 'metadata'):
                    UpdateController._log_message('Executing function {0}'.format(function.__name__),
                                                  client_ip=client.ip)
                    output = function(client)
                    for key, value in output.iteritems():
                        if key != 'volumedriver':
                            continue
                        for package_info in value:
                            packages_to_update.update(package_info['packages'])
                            all_services_to_restart += package_info['services']

            services_to_restart = []
            for service in all_services_to_restart:
                if service not in services_to_restart:
                    services_to_restart.append(service)  # Filter out duplicates keeping the order of services (eg: watcher-framework before memcached)

            UpdateController._log_message('Services which will be restarted --> {0}'.format(', '.join(services_to_restart)))
            UpdateController._log_message('Packages which will be installed --> {0}'.format(', '.join(packages_to_update)))

            # 1. Stop services
            if UpdateController._change_services_state(services=services_to_restart,
                                                       ssh_clients=ssh_clients,
                                                       action='stop') is False:
                UpdateController._log_message('Stopping all services on every node failed, cannot continue',
                                              client_ip=this_client.ip, severity='warning')
                UpdateController._remove_lock_files([upgrade_file, upgrade_ongoing_check_file], ssh_clients)

                UpdateController._log_message('Attempting to start the services again', client_ip=this_client.ip)
                UpdateController._change_services_state(services=services_to_restart,
                                                        ssh_clients=ssh_clients,
                                                        action='start')
                UpdateController._log_message('Failed to stop all required services, update aborted',
                                              client_ip=this_client.ip, severity='error')
                return

            # 2. Update packages
            failed_clients = []
            for client in ssh_clients:
                PackageManager.update(client=client)
                try:
                    for package_name in packages_to_update:
                        UpdateController._log_message('Installing {0}'.format(package_name), client.ip)
                        PackageManager.install(package_name=package_name,
                                               client=client,
                                               force=True)
                        UpdateController._log_message('Installed {0}'.format(package_name), client.ip)
                    client.file_delete(upgrade_file)
                except subprocess.CalledProcessError as cpe:
                    UpdateController._log_message('Upgrade failed with error: {0}'.format(cpe.output), client.ip,
                                                  'error')
                    failed_clients.append(client)
                    break

            if failed_clients:
                UpdateController._remove_lock_files([upgrade_file, upgrade_ongoing_check_file], ssh_clients)
                UpdateController._log_message('Error occurred. Attempting to start all services again',
                                              client_ip=this_client.ip, severity='error')
                UpdateController._change_services_state(services=services_to_restart,
                                                        ssh_clients=ssh_clients,
                                                        action='start')
                UpdateController._log_message('Failed to upgrade following nodes:\n - {0}\nPlease check /var/log/ovs/lib.log on {1} for more information'.format('\n - '.join([client.ip for client in failed_clients]), this_client.ip),
                                              this_client.ip,
                                              'error')
                return

            # 3. Post upgrade actions
            UpdateController._log_message('Executing post upgrade actions', client_ip=this_client.ip)
            for client in ssh_clients:
                for function in Toolbox.fetch_hooks('update', 'postupgrade'):
                    UpdateController._log_message('Executing action: {0}'.format(function.__name__), client_ip=client.ip)
                    try:
                        function(client)
                    except Exception as ex:
                        UpdateController._log_message('Post upgrade action failed with error: {0}'.format(ex),
                                                      client.ip, 'error')

            # 4. Start services
            UpdateController._log_message('Starting services', client_ip=this_client.ip)
            UpdateController._change_services_state(services=services_to_restart,
                                                    ssh_clients=ssh_clients,
                                                    action='start')

            UpdateController._remove_lock_files([upgrade_ongoing_check_file], ssh_clients)
            UpdateController._log_message('+++ Finished updating +++')
        except RuntimeError as rte:
            UpdateController._log_message('Error during volumedriver update: {0}'.format(rte), severity='error')
            UpdateController._remove_lock_files([upgrade_file, upgrade_ongoing_check_file], ssh_clients)
        except NoLockAvailableException:
            UpdateController._log_message('Another volumedriver update is currently in progress!')
        except Exception as ex:
            UpdateController._log_message('Error during volumedriver update: {0}'.format(ex), severity='error')
            UpdateController._remove_lock_files([upgrade_file, upgrade_ongoing_check_file], ssh_clients)
        finally:
            filemutex.release()

    @staticmethod
    def _log_message(message, client_ip=None, severity='info'):
        if client_ip is not None:
            message = '{0:<15}: {1}'.format(client_ip, message)
        if severity == 'info':
            UpdateController._logger.info(message, print_msg=True)
        elif severity == 'warning':
            UpdateController._logger.warning(message, print_msg=True)
        elif severity == 'error':
            UpdateController._logger.error(message, print_msg=True)

    @staticmethod
    def _remove_lock_files(files, ssh_clients):
        for ssh_client in ssh_clients:
            for file_name in files:
                if ssh_client.file_exists(file_name):
                    ssh_client.file_delete(file_name)

    @staticmethod
    def _change_services_state(services, ssh_clients, action):
        """
        Stop/start services on SSH clients
        If action is start, we ignore errors and try to start other services on other nodes
        """
        if action == 'start':
            services.reverse()  # Start services again in reverse order of stopping
        for service_name in services:
            for ssh_client in ssh_clients:
                description = 'stopping' if action == 'stop' else 'starting' if action == 'start' else 'restarting'
                try:
                    if ServiceManager.has_service(service_name, client=ssh_client):
                        UpdateController._log_message('{0} service {1}'.format(description.capitalize(), service_name),
                                                      ssh_client.ip)
                        Toolbox.change_service_state(client=ssh_client,
                                                     name=service_name,
                                                     state=action,
                                                     logger=UpdateController._logger)
                        UpdateController._log_message('{0} service {1}'.format('Stopped' if action == 'stop' else 'Started' if action == 'start' else 'Restarted', service_name), ssh_client.ip)
                except Exception as exc:
                    UpdateController._log_message('Something went wrong {0} service {1}: {2}'.format(description, service_name, exc), ssh_client.ip, severity='warning')
                    if action == 'stop':
                        return False
        return True

    @staticmethod
    @add_hooks('update', 'postupgrade')
    def post_upgrade(client):
        """
        Upgrade actions after the new packages have actually been installed
        :param client: SSHClient object
        :return: None
        """
        # If we can reach Etcd with a valid config, and there's still an old config file present, delete it
        from ovs.extensions.db.etcd.configuration import EtcdConfiguration
        path = '/opt/OpenvStorage/config/ovs.json'
        if EtcdConfiguration.exists('/ovs/framework/cluster_id') and client.file_exists(path):
            client.file_delete(path)
        # Migrate volumedriver & albaproxy configuration files
        import uuid
        from ovs.extensions.storageserver.storagedriver import StorageDriverConfiguration
        from ovs.dal.lists.storagedriverlist import StorageDriverList
        from ovs.extensions.generic.system import System
        with remote(client.ip, [StorageDriverConfiguration, os, open, json, System], username='ovs') as rem:
            configuration_dir = '{0}/storagedriver/storagedriver'.format(EtcdConfiguration.get('/ovs/framework/paths|cfgdir'))
            host_id = rem.System.get_my_machine_id()
            if rem.os.path.exists(configuration_dir):
                for storagedriver in StorageDriverList.get_storagedrivers_by_storagerouter(rem.System.get_my_storagerouter().guid):
                    vpool = storagedriver.vpool
                    if storagedriver.alba_proxy is not None:
                        config_tree = '/ovs/vpools/{0}/proxies/{1}/config/{{0}}'.format(vpool.guid, storagedriver.alba_proxy.guid)
                        # ABM config
                        abm_config = '{0}/{1}_alba.cfg'.format(configuration_dir, vpool.name)
                        if rem.os.path.exists(abm_config):
                            with rem.open(abm_config) as config_file:
                                EtcdConfiguration.set(config_tree.format('abm'), config_file.read(), raw=True)
                            rem.os.remove(abm_config)
                        # Albaproxy config
                        alba_config = '{0}/{1}_alba.json'.format(configuration_dir, vpool.name)
                        if rem.os.path.exists(alba_config):
                            with rem.open(alba_config) as config_file:
                                config = rem.json.load(config_file)
                                del config['albamgr_cfg_file']
                                config['albamgr_cfg_url'] = 'etcd://127.0.0.1:2379{0}'.format(config_tree.format('abm'))
                                EtcdConfiguration.set(config_tree.format('main'), json.dumps(config, indent=4), raw=True)
                            params = {'VPOOL_NAME': vpool.name,
                                      'VPOOL_GUID': vpool.guid,
                                      'PROXY_ID': storagedriver.alba_proxy.guid}
                            alba_proxy_service = 'ovs-albaproxy_{0}'.format(vpool.name)
                            ServiceManager.add_service(name='ovs-albaproxy', params=params, client=client, target_name=alba_proxy_service)
                            rem.os.remove(alba_config)
                    # Volumedriver config
                    current_file = '{0}/{1}.json'.format(configuration_dir, vpool.name)
                    if rem.os.path.exists(current_file):
                        readcache_size = 0
                        with rem.open(current_file) as config_file:
                            config = rem.json.load(config_file)
                        config['distributed_transaction_log'] = {}
                        config['distributed_transaction_log']['dtl_transport'] = config['failovercache']['failovercache_transport']
                        config['distributed_transaction_log']['dtl_path'] = config['failovercache']['failovercache_path']
                        config['volume_manager']['dtl_throttle_usecs'] = config['volume_manager']['foc_throttle_usecs']
                        del config['failovercache']
                        del config['volume_manager']['foc_throttle_usecs']
                        sdc = rem.StorageDriverConfiguration('storagedriver', vpool.guid, storagedriver.storagedriver_id)
                        sdc.configuration = config
                        sdc.save(reload_config=False)
                        for mountpoint in config['content_addressed_cache']['clustercache_mount_points']:
                            readcache_size += int(mountpoint['size'].replace('KiB', ''))
                        params = {'VPOOL_MOUNTPOINT': storagedriver.mountpoint,
                                  'HYPERVISOR_TYPE': storagedriver.storagerouter.pmachine.hvtype,
                                  'VPOOL_NAME': vpool.name,
                                  'CONFIG_PATH': sdc.remote_path,
                                  'UUID': str(uuid.uuid4()),
                                  'OVS_UID': client.run('id -u ovs').strip(),
                                  'OVS_GID': client.run('id -g ovs').strip(),
                                  'KILL_TIMEOUT': str(int(readcache_size / 1024.0 / 1024.0 / 6.0 + 30))}
                        vmware_mode = EtcdConfiguration.get('/ovs/framework/hosts/{0}/storagedriver|vmware_mode'.format(host_id))
                        dtl_service = 'ovs-dtl_{0}'.format(vpool.name)
                        ServiceManager.add_service(name='ovs-dtl', params=params, client=client, target_name=dtl_service)
                        if vpool.backend_type.code == 'alba':
                            alba_proxy_service = 'ovs-albaproxy_{0}'.format(vpool.name)
                            dependencies = [alba_proxy_service]
                        else:
                            dependencies = None
                        if vmware_mode == 'ganesha':
                            template_name = 'ovs-ganesha'
                        else:
                            template_name = 'ovs-volumedriver'
                        voldrv_service = 'ovs-volumedriver_{0}'.format(vpool.name)
                        ServiceManager.add_service(name=template_name, params=params, client=client, target_name=voldrv_service, additional_dependencies=dependencies)
                        rem.os.remove(current_file)
                    # Ganesha config, if available
                    current_file = '{0}/{1}_ganesha.conf'.format(configuration_dir, vpool.name)
                    if rem.os.path.exists(current_file):
                        sdc = rem.StorageDriverConfiguration('storagedriver', vpool.guid, storagedriver.storagedriver_id)
                        contents = ''
                        for template in ['ganesha-core', 'ganesha-export']:
                            contents += client.file_read('/opt/OpenvStorage/config/templates/{0}.conf'.format(template))
                        params = {'VPOOL_NAME': vpool.name,
                                  'VPOOL_MOUNTPOINT': '/mnt/{0}'.format(vpool.name),
                                  'CONFIG_PATH': sdc.remote_path,
                                  'NFS_FILESYSTEM_ID': storagedriver.storagerouter.ip.split('.', 2)[-1]}
                        for key, value in params.iteritems():
                            contents = contents.replace('<{0}>'.format(key), value)
                        client.file_write(current_file, contents)
