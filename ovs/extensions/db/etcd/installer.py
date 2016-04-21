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
ETCD Installer module
"""

import re
import etcd
import time
from subprocess import CalledProcessError
from ovs.extensions.generic.sshclient import SSHClient
from ovs.extensions.generic.system import System
from ovs.extensions.services.service import ServiceManager
from ovs.log.logHandler import LogHandler

logger = LogHandler.get('extensions', name='etcd_installer')


class EtcdInstaller(object):
    """
    class to dynamically install/(re)configure etcd cluster
    """
    DB_DIR = '/opt/OpenvStorage/db'
    DATA_DIR = '{0}/etcd/{{0}}/data'.format(DB_DIR)
    WAL_DIR = '{0}/etcd/{{0}}/wal'.format(DB_DIR)
    DEFAULT_SERVER_PORT = 2380
    DEFAULT_CLIENT_PORT = 2379
    SERVER_URL = 'http://{0}:{1}'
    CLIENT_URL = 'http://{0}:{1}'
    MEMBER_REGEX = re.compile(ur'^(?P<id>[^:]+): name=(?P<name>[^ ]+) peerURLs=(?P<peer>[^ ]+) clientURLs=(?P<client>[^ ]+)$')

    def __init__(self):
        """
        EtcdInstaller should not be instantiated
        """
        raise RuntimeError('EtcdInstaller is a complete static helper class')

    @staticmethod
    def create_cluster(cluster_name, ip, server_port=DEFAULT_SERVER_PORT, client_port=DEFAULT_CLIENT_PORT):
        """
        Creates a cluster
        :param cluster_name: Name of the cluster
        :type cluster_name: str

        :param ip: IP address of the first node of the new cluster
        :type ip: str

        :param server_port: Port to be used by server
        :type server_port: int

        :param client_port: Port to be used by client
        :type client_port: int

        :return: None
        """
        logger.debug('Creating cluster "{0}" on {1}'.format(cluster_name, ip))

        client = SSHClient(ip, username='root')
        target_name = 'ovs-etcd-{0}'.format(cluster_name)
        if ServiceManager.has_service(target_name, client) and ServiceManager.get_service_status(target_name, client) is True:
            logger.info('Service {0} already configured and running'.format(target_name))
            return

        node_name = System.get_my_machine_id(client)
        data_dir = EtcdInstaller.DATA_DIR.format(cluster_name)
        wal_dir = EtcdInstaller.WAL_DIR.format(cluster_name)
        abs_paths = [data_dir, wal_dir]
        client.dir_delete(abs_paths)
        client.dir_create(abs_paths)
        client.dir_chmod(abs_paths, 0755, recursive=True)
        client.dir_chown(abs_paths, 'ovs', 'ovs', recursive=True)

        base_name = 'ovs-etcd'
        ServiceManager.add_service(base_name, client,
                                   params={'CLUSTER': cluster_name,
                                           'NODE_ID': node_name,
                                           'DATA_DIR': data_dir,
                                           'WAL_DIR': wal_dir,
                                           'SERVER_URL': EtcdInstaller.SERVER_URL.format(ip, server_port),
                                           'CLIENT_URL': EtcdInstaller.CLIENT_URL.format(ip, client_port),
                                           'LOCAL_CLIENT_URL': EtcdInstaller.CLIENT_URL.format('127.0.0.1', client_port),
                                           'INITIAL_CLUSTER': '{0}={1}'.format(node_name, EtcdInstaller.SERVER_URL.format(ip, server_port)),
                                           'INITIAL_STATE': 'new',
                                           'INITIAL_PEERS': '-initial-advertise-peer-urls {0}'.format(EtcdInstaller.SERVER_URL.format(ip, server_port))},
                                   target_name=target_name)
        EtcdInstaller.start(cluster_name, client)
        EtcdInstaller.wait_for_cluster(cluster_name, client, client_port=client_port)

        logger.debug('Creating cluster "{0}" on {1} completed'.format(cluster_name, ip))

    @staticmethod
    def extend_cluster(master_ip, new_ip, cluster_name, server_port=DEFAULT_SERVER_PORT, client_port=DEFAULT_CLIENT_PORT):
        """
        Extends a cluster to a given new node
        :param master_ip: IP of one of the already existing nodes
        :type master_ip: str

        :param new_ip: IP address of the node to be added
        :type new_ip: str

        :param cluster_name: Name of the cluster to be extended
        :type cluster_name: str

        :param server_port: Port to be used by server
        :type server_port: int

        :param client_port: Port to be used by client
        :type client_port: int
        """
        logger.debug('Extending cluster "{0}" from {1} to {2}'.format(cluster_name, master_ip, new_ip))

        master_client = SSHClient(master_ip, username='root')
        if not EtcdInstaller._is_healty(cluster_name, master_client, client_port=client_port):
            raise RuntimeError('Cluster "{0}" unhealthy, aborting extend'.format(cluster_name))

        command = 'etcdctl member list'
        new_server_url = EtcdInstaller.SERVER_URL.format(new_ip, server_port)
        if client_port != EtcdInstaller.DEFAULT_CLIENT_PORT:
            command = 'etcdctl --peers={0}:{1} member list'.format(master_ip, client_port)
        cluster_members = master_client.run(command).splitlines()
        for cluster_member in cluster_members:
            if new_server_url in cluster_member:
                logger.info('Node {0} already member of etcd cluster'.format(new_ip))
                return

        current_cluster = []
        for item in cluster_members:
            info = re.search(EtcdInstaller.MEMBER_REGEX, item).groupdict()
            current_cluster.append('{0}={1}'.format(info['name'], info['peer']))

        new_client = SSHClient(new_ip, username='root')
        node_name = System.get_my_machine_id(new_client)
        current_cluster.append('{0}={1}'.format(node_name, new_server_url))

        data_dir = EtcdInstaller.DATA_DIR.format(cluster_name)
        wal_dir = EtcdInstaller.WAL_DIR.format(cluster_name)
        abs_paths = [data_dir, wal_dir]
        new_client.dir_delete(abs_paths)
        new_client.dir_create(abs_paths)
        new_client.dir_chmod(abs_paths, 0755, recursive=True)
        new_client.dir_chown(abs_paths, 'ovs', 'ovs', recursive=True)

        base_name = 'ovs-etcd'
        target_name = 'ovs-etcd-{0}'.format(cluster_name)
        EtcdInstaller.stop(cluster_name, new_client)  # Stop a possible proxy service
        ServiceManager.add_service(base_name, new_client,
                                   params={'CLUSTER': cluster_name,
                                           'NODE_ID': node_name,
                                           'DATA_DIR': data_dir,
                                           'WAL_DIR': wal_dir,
                                           'SERVER_URL': new_server_url,
                                           'CLIENT_URL': EtcdInstaller.CLIENT_URL.format(new_ip, client_port),
                                           'LOCAL_CLIENT_URL': EtcdInstaller.CLIENT_URL.format('127.0.0.1', client_port),
                                           'INITIAL_CLUSTER': ','.join(current_cluster),
                                           'INITIAL_STATE': 'existing',
                                           'INITIAL_PEERS': ''},
                                   target_name=target_name)

        add_command = 'etcdctl member add {0} {1}'.format(node_name, new_server_url)
        if client_port != EtcdInstaller.DEFAULT_CLIENT_PORT:
            add_command = 'etcdctl --peers={0}:{1} member add {2} {3}'.format(master_ip, client_port, node_name, new_server_url)
        master_client.run(add_command)
        EtcdInstaller.start(cluster_name, new_client)
        EtcdInstaller.wait_for_cluster(cluster_name, new_client, client_port=client_port)

        logger.debug('Extending cluster "{0}" from {1} to {2} completed'.format(cluster_name, master_ip, new_ip))

    @staticmethod
    def shrink_cluster(remaining_node_ip, ip_to_remove, cluster_name, offline_node_ips=None, client_port=DEFAULT_CLIENT_PORT):
        """
        Removes a node from a cluster, the old node will become a slave
        :param remaining_node_ip: The ip of a remaining node in the cluster
        :type remaining_node_ip: str

        :param ip_to_remove: The ip of the node that should be removed from the cluster
        :type ip_to_remove: str

        :param cluster_name: The name of the cluster to shrink
        :type cluster_name: str

        :param offline_node_ips: IPs of offline nodes
        :type offline_node_ips: list

        :param client_port: Port to be used by client
        :type client_port: int

        :return: None
        """
        logger.debug('Shrinking cluster "{0}" from {1}'.format(cluster_name, ip_to_remove))

        current_client = SSHClient(remaining_node_ip, username='root')
        if not EtcdInstaller._is_healty(cluster_name, current_client, client_port=client_port):
            raise RuntimeError('Cluster "{0}" unhealthy, aborting shrink'.format(cluster_name))

        node_id = None
        list_command = 'etcdctl member list'
        if client_port != EtcdInstaller.DEFAULT_CLIENT_PORT:
            list_command = 'etcdctl --peers={0}:{1} member list'.format(remaining_node_ip, client_port)
        for item in current_client.run(list_command).splitlines():
            info = re.search(EtcdInstaller.MEMBER_REGEX, item).groupdict()
            if EtcdInstaller.CLIENT_URL.format(ip_to_remove, client_port) == info['client']:
                node_id = info['id']
        if node_id is not None:
            remove_command = 'etcdctl member remove {0}'.format(node_id)
            if client_port != EtcdInstaller.DEFAULT_CLIENT_PORT:
                remove_command = 'etcdctl --peers={0}:{1} member remove {2}'.format(remaining_node_ip, client_port, node_id)
            current_client.run(remove_command)
        if ip_to_remove not in offline_node_ips:
            EtcdInstaller.deploy_to_slave(remaining_node_ip, ip_to_remove, cluster_name)
        EtcdInstaller.wait_for_cluster(cluster_name, current_client, client_port=client_port)

        logger.debug('Shrinking cluster "{0}" from {1} completed'.format(cluster_name, ip_to_remove))

    @staticmethod
    def has_cluster(ip, cluster_name):
        """
        Verify if IP has an ETCD cluster with 'cluster_name' running
        :param ip: IP on which to check for the ETCD cluster
        :type ip: str

        :param cluster_name: Name of the ETCD cluster
        :type cluster_name: str

        :return: True or False
        :rtype: bool
        """
        logger.debug('Checking whether {0} has cluster "{1}" running'.format(ip, cluster_name))
        client = SSHClient(ip, username='root')
        try:
            return client.run('etcdctl member list').strip() != ''
        except CalledProcessError:
            return False

    @staticmethod
    def deploy_to_slave(master_ip, slave_ip, cluster_name):
        """
        Deploys the configuration file to a slave
        :param master_ip: IP of the node to deploy from
        :type master_ip: str

        :param slave_ip: IP of the slave to deploy to
        :type slave_ip: str

        :param cluster_name: Name of the cluster of which to deploy the configuration file
        :type cluster_name: str

        :return: None
        """
        logger.debug('  Setting up proxy "{0}" from {1} to {2}'.format(cluster_name, master_ip, slave_ip))
        master_client = SSHClient(master_ip, username='root')
        slave_client = SSHClient(slave_ip, username='root')

        current_cluster = []
        for item in master_client.run('etcdctl member list').splitlines():
            info = re.search(EtcdInstaller.MEMBER_REGEX, item).groupdict()
            current_cluster.append('{0}={1}'.format(info['name'], info['peer']))

        EtcdInstaller._setup_proxy(','.join(current_cluster), slave_client, cluster_name, force=True)
        logger.debug('  Setting up proxy "{0}" from {1} to {2} completed'.format(cluster_name, master_ip, slave_ip))

    @staticmethod
    def use_external(external, slave_ip, cluster_name):
        """
        Setup proxy for external etcd
        :param external: External etcd info
        :type external: str

        :param slave_ip: IP of slave
        :type slave_ip: str

        :param cluster_name: Name of cluster
        :type cluster_name: str

        :return: None
        """
        logger.debug('Setting up proxy "{0}" from {1} to {2}'.format(cluster_name, external, slave_ip))
        EtcdInstaller._setup_proxy(external, SSHClient(slave_ip, username='root'), cluster_name)
        logger.debug('Setting up proxy "{0}" from {1} to {2} completed'.format(cluster_name, external, slave_ip))

    @staticmethod
    def remove_proxy(cluster_name, ip):
        """
        Remove a proxy
        :param cluster_name: Name of cluster
        :type cluster_name: str

        :param ip: IP of the node on which to remove the proxy
        :type ip: str

        :return: None
        """
        root_client = SSHClient(ip, username='root')
        EtcdInstaller.stop(cluster_name=cluster_name, client=root_client)
        EtcdInstaller.remove(cluster_name=cluster_name, client=root_client)
        data_dir = EtcdInstaller.DATA_DIR.format(cluster_name)
        wal_dir = EtcdInstaller.WAL_DIR.format(cluster_name)
        root_client.dir_delete([wal_dir, data_dir])

    @staticmethod
    def _setup_proxy(initial_cluster, slave_client, cluster_name, force=False, client_port=DEFAULT_CLIENT_PORT):
        base_name = 'ovs-etcd-proxy'
        target_name = 'ovs-etcd-{0}'.format(cluster_name)
        if force is False and ServiceManager.has_service(target_name, slave_client) and ServiceManager.get_service_status(target_name, slave_client) is True:
            logger.info('Service {0} already configured and running'.format(target_name))
            return
        EtcdInstaller.stop(cluster_name, slave_client)

        data_dir = EtcdInstaller.DATA_DIR.format(cluster_name)
        wal_dir = EtcdInstaller.WAL_DIR.format(cluster_name)
        abs_paths = [data_dir, wal_dir]
        slave_client.dir_delete(abs_paths)
        slave_client.dir_create(data_dir)
        slave_client.dir_chmod(data_dir, 0755, recursive=True)
        slave_client.dir_chown(data_dir, 'ovs', 'ovs', recursive=True)

        ServiceManager.add_service(base_name, slave_client,
                                   params={'CLUSTER': cluster_name,
                                           'DATA_DIR': data_dir,
                                           'LOCAL_CLIENT_URL': EtcdInstaller.CLIENT_URL.format('127.0.0.1', client_port),
                                           'INITIAL_CLUSTER': initial_cluster},
                                   target_name=target_name)
        EtcdInstaller.start(cluster_name, slave_client)
        EtcdInstaller.wait_for_cluster(cluster_name, slave_client, client_port=client_port)

    @staticmethod
    def start(cluster_name, client):
        """
        Starts an etcd cluster
        :param cluster_name: The name of the cluster service to start
        :type cluster_name: str

        :param client: Client on which to start the service
        :type client: SSHClient

        :return: None
        """
        if ServiceManager.has_service('etcd-{0}'.format(cluster_name), client=client) is True:
            ServiceManager.start_service('etcd-{0}'.format(cluster_name), client=client)

    @staticmethod
    def stop(cluster_name, client):
        """
        Stops an etcd service
        :param cluster_name: The name of the cluster service to stop
        :type cluster_name: str

        :param client: Client on which to stop the service
        :type client: SSHClient

        :return: None
        """
        if ServiceManager.has_service('etcd-{0}'.format(cluster_name), client=client) is True:
            ServiceManager.stop_service('etcd-{0}'.format(cluster_name), client=client)

    @staticmethod
    def remove(cluster_name, client):
        """
        Removes an etcd service
        :param cluster_name: The name of the cluster service to remove
        :type cluster_name: str

        :param client: Client on which to remove the service
        :type client: SSHClient

        :return: None
        """
        if ServiceManager.has_service('etcd-{0}'.format(cluster_name), client=client) is True:
            ServiceManager.remove_service('etcd-{0}'.format(cluster_name), client=client)

    @staticmethod
    def wait_for_cluster(cluster_name, client, client_port=DEFAULT_CLIENT_PORT):
        """
        Validates the health of the etcd cluster is healthy
        :param cluster_name: Name of the cluster
        :type cluster_name: str

        :param client: The client on which to validate the cluster
        :type client: SSHClient

        :param client_port: Port to be used by client
        :type client_port: int

        :return: None
        """
        logger.debug('Waiting for cluster "{0}"'.format(cluster_name))
        tries = 5
        healthy = EtcdInstaller._is_healty(cluster_name, client, client_port=client_port)
        while healthy is False and tries > 0:
            tries -= 1
            time.sleep(5 - tries)
            healthy = EtcdInstaller._is_healty(cluster_name, client, client_port=client_port)
        if healthy is False:
            raise etcd.EtcdConnectionFailed('Etcd cluster "{0}" could not be started correctly'.format(cluster_name))
        logger.debug('Cluster "{0}" running'.format(cluster_name))

    @staticmethod
    def _is_healty(cluster_name, client, client_port):
        """
        Indicates whether a given cluster is healthy
        :param cluster_name: name of the cluster
        :type cluster_name: str

        :param client: client on which to check
        :type client: SSHClient

        :return: True or False
        :rtype: bool
        """
        try:
            command = 'etcdctl cluster-health'
            if client_port != EtcdInstaller.DEFAULT_CLIENT_PORT:
                command = 'etcdctl --peers={0}:{1} cluster-health'.format(client.ip, client_port)
            output = client.run(command)
            if 'cluster is healthy' not in output:
                logger.debug('  Cluster "{0}" is not healthy: {1}'.format(cluster_name, ' - '.join(output.splitlines())))
                return False
            logger.debug('  Cluster "{0}" is healthy'.format(cluster_name))
            return True
        except Exception as ex:
            logger.debug('  Cluster "{0}" is not healthy: {1}'.format(cluster_name, ex))
            return False
