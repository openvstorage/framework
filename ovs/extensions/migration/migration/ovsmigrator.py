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
OVS migration module
"""

from ovs.log.logHandler import LogHandler
logger = LogHandler.get('extensions', name='migration')


class OVSMigrator(object):
    """
    Handles all model related migrations
    """

    identifier = 'ovs'  # Used by migrator.py, so don't remove

    def __init__(self):
        """ Init method """
        pass

    @staticmethod
    def migrate(previous_version, master_ips=None, extra_ips=None):
        """
        Migrates from any version to any version, running all migrations required
        If previous_version is for example 0 and this script is at
        verison 3 it will execute two steps:
          - 1 > 2
          - 2 > 3
        :param previous_version: The previous version from which to start the migration.
        :param master_ips: IP addresses of the MASTER nodes
        :param extra_ips: IP addresses of the EXTRA nodes
        """

        working_version = previous_version

        # Version 1 introduced:
        # - Flexible SSD layout
        if working_version < 1:
            try:
                from ovs.extensions.generic.configuration import Configuration
                if Configuration.exists('ovs.arakoon'):
                    Configuration.delete('ovs.arakoon', remove_root=True)
                Configuration.set('ovs.core.ovsdb', '/opt/OpenvStorage/db')
            except:
                logger.exception('Error migrating to version 1')

            working_version = 1

        # Version 2 introduced:
        # - Registration
        if working_version < 2:
            try:
                import time
                from ovs.extensions.generic.configuration import Configuration
                if not Configuration.exists('ovs.core.registered'):
                    Configuration.set('ovs.core.registered', False)
                    Configuration.set('ovs.core.install_time', time.time())
            except:
                logger.exception('Error migrating to version 2')

        working_version = 2

        # Version 3 introduced:
        # - New arakoon clients
        if working_version < 3:
            try:
                from ovs.extensions.db.arakoon import ArakoonInstaller
                reload(ArakoonInstaller)
                from ovs.extensions.db.arakoon.ArakoonInstaller import ArakoonInstaller
                from ovs.extensions.generic.sshclient import SSHClient
                from ovs.extensions.generic.configuration import Configuration
                if master_ips is not None:
                    for ip in master_ips:
                        client = SSHClient(ip)
                        if client.dir_exists(ArakoonInstaller.ARAKOON_CONFIG_DIR):
                            for cluster_name in client.dir_list(ArakoonInstaller.ARAKOON_CONFIG_DIR):
                                try:
                                    ArakoonInstaller.deploy_cluster(cluster_name, ip)
                                except:
                                    pass
                if Configuration.exists('ovs.core.storage.persistent'):
                    Configuration.set('ovs.core.storage.persistent', 'pyrakoon')
            except:
                logger.exception('Error migrating to version 3')

            working_version = 3

        # Version 4 introduced:
        # - Etcd
        if working_version < 4:
            try:
                import os
                import json
                from ConfigParser import RawConfigParser
                from ovs.extensions.db.etcd import installer
                reload(installer)
                from ovs.extensions.db.etcd.installer import EtcdInstaller
                from ovs.extensions.db.etcd.configuration import EtcdConfiguration
                from ovs.extensions.generic.system import System
                host_id = System.get_my_machine_id()
                etcd_migrate = False
                if EtcdInstaller.has_cluster('127.0.0.1', 'config'):
                    etcd_migrate = True
                else:
                    if master_ips is not None and extra_ips is not None:
                        cluster_ip = None
                        for ip in master_ips + extra_ips:
                            if EtcdInstaller.has_cluster(ip, 'config'):
                                cluster_ip = ip
                                break
                        node_ip = None
                        path = '/opt/OpenvStorage/config/ovs.json'
                        if os.path.exists(path):
                            with open(path) as config_file:
                                config = json.load(config_file)
                                node_ip = config['grid']['ip']
                        if node_ip is not None:
                            if cluster_ip is None:
                                EtcdInstaller.create_cluster('config', node_ip)
                                EtcdConfiguration.initialize()
                                EtcdConfiguration.initialize_host(host_id)
                            else:
                                EtcdInstaller.extend_cluster(cluster_ip, node_ip, 'config')
                                EtcdConfiguration.initialize_host(host_id)
                            etcd_migrate = True
                if etcd_migrate is True:
                    # Migrating configuration files
                    path = '/opt/OpenvStorage/config/ovs.json'
                    if os.path.exists(path):
                        with open(path) as config_file:
                            config = json.load(config_file)
                            EtcdConfiguration.set('/ovs/framework/cluster_id', config['support']['cid'])
                            if not EtcdConfiguration.exists('/ovs/framework/install_time'):
                                EtcdConfiguration.set('/ovs/framework/install_time', config['core']['install_time'])
                            else:
                                EtcdConfiguration.set('/ovs/framework/install_time', min(EtcdConfiguration.get('/ovs/framework/install_time'), config['core']['install_time']))
                            EtcdConfiguration.set('/ovs/framework/registered', config['core']['registered'])
                            EtcdConfiguration.set('/ovs/framework/plugins/installed', config['plugins'])
                            EtcdConfiguration.set('/ovs/framework/stores', config['core']['storage'])
                            EtcdConfiguration.set('/ovs/framework/paths', {'cfgdir': config['core']['cfgdir'],
                                                                           'basedir': config['core']['basedir'],
                                                                           'ovsdb': config['core']['ovsdb']})
                            EtcdConfiguration.set('/ovs/framework/support', {'enablesupport': config['support']['enablesupport'],
                                                                             'enabled': config['support']['enabled'],
                                                                             'interval': config['support']['interval']})
                            EtcdConfiguration.set('/ovs/framework/storagedriver', {'mds_safety': config['storagedriver']['mds']['safety'],
                                                                                   'mds_tlogs': config['storagedriver']['mds']['tlogs'],
                                                                                   'mds_maxload': config['storagedriver']['mds']['maxload']})
                            EtcdConfiguration.set('/ovs/framework/webapps', {'html_endpoint': config['webapps']['html_endpoint'],
                                                                             'oauth2': config['webapps']['oauth2']})
                            EtcdConfiguration.set('/ovs/framework/messagequeue', {'endpoints': [],
                                                                                  'protocol': config['core']['broker']['protocol'],
                                                                                  'user': config['core']['broker']['login'],
                                                                                  'port': config['core']['broker']['port'],
                                                                                  'password': config['core']['broker']['password'],
                                                                                  'queues': config['core']['broker']['queues']})
                            host_key = '/ovs/framework/hosts/{0}{{0}}'.format(host_id)
                            EtcdConfiguration.set(host_key.format('/storagedriver'), {'rsp': config['storagedriver']['rsp'],
                                                                                      'vmware_mode': config['storagedriver']['vmware_mode']})
                            EtcdConfiguration.set(host_key.format('/ports'), config['ports'])
                            EtcdConfiguration.set(host_key.format('/setupcompleted'), config['core']['setupcompleted'])
                            EtcdConfiguration.set(host_key.format('/versions'), config['core'].get('versions', {}))
                            EtcdConfiguration.set(host_key.format('/type'), config['core']['nodetype'])
                            EtcdConfiguration.set(host_key.format('/ip'), config['grid']['ip'])
                    path = '{0}/memcacheclient.cfg'.format(EtcdConfiguration.get('/ovs/framework/paths|cfgdir'))
                    if os.path.exists(path):
                        config = RawConfigParser()
                        config.read(path)
                        nodes = [config.get(node.strip(), 'location').strip()
                                 for node in config.get('main', 'nodes').split(',')]
                        EtcdConfiguration.set('/ovs/framework/memcache|endpoints', nodes)
                        os.remove(path)
                    path = '{0}/rabbitmqclient.cfg'.format(EtcdConfiguration.get('/ovs/framework/paths|cfgdir'))
                    if os.path.exists(path):
                        config = RawConfigParser()
                        config.read(path)
                        nodes = [config.get(node.strip(), 'location').strip()
                                 for node in config.get('main', 'nodes').split(',')]
                        EtcdConfiguration.set('/ovs/framework/messagequeue|endpoints', nodes)
                        os.remove(path)
                    # Migrate arakoon configuration files
                    from ovs.extensions.db.arakoon import ArakoonInstaller
                    reload(ArakoonInstaller)
                    from ovs.extensions.db.arakoon.ArakoonInstaller import ArakoonInstaller, ArakoonClusterConfig
                    from ovs.extensions.generic.sshclient import SSHClient
                    if master_ips is not None:
                        config_dir = '/opt/OpenvStorage/config/arakoon/'
                        for ip in master_ips:
                            client = SSHClient(ip)
                            if client.dir_exists(config_dir):
                                for cluster_name in client.dir_list(config_dir):
                                    try:
                                        with open('{0}/{1}/{1}.cfg'.format(config_dir, cluster_name)) as config_file:
                                            EtcdConfiguration.set(ArakoonClusterConfig.ETCD_CONFIG_KEY.format(cluster_name),
                                                                  config_file.read(),
                                                                  raw=True)
                                            ArakoonInstaller.deploy_cluster(cluster_name, ip)
                                    except:
                                        logger.exception('Error migrating {0} on {1}'.format(cluster_name, ip))
                                client.dir_delete(config_dir)
            except:
                logger.exception('Error migrating to version 4')

            working_version = 4

        return working_version
