# Copyright 2015 iNuron NV
#
# Licensed under the Open vStorage Modified Apache License (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.openvstorage.org/license
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import re
from ovs.extensions.generic.sshclient import SSHClient
from ovs.extensions.generic.system import System
from ovs.extensions.services.service import ServiceManager
from ovs.log.logHandler import LogHandler

logger = LogHandler.get('extensions', name='etcd_installer')


class EtcdInstaller(object):
    """
    class to dynamically install/(re)configure etcd cluster
    """
    DATA_DIR = '{0}/etcd/{1}/data'
    WAL_DIR = '{0}/etcd/{1}/wal'
    SERVER_URL = 'http://{0}:2380'
    CLIENT_URL = 'http://{0}:2379'
    MEMBER_REGEX = re.compile(ur'^(?P<id>[^:]+): name=(?P<name>[^ ]+) peerURLs=(?P<peer>[^ ]+) clientURLs=(?P<client>[^ ]+)$')

    def __init__(self):
        """
        EtcdInstaller should not be instantiated
        """
        raise RuntimeError('EtcdInstaller is a complete static helper class')

    @staticmethod
    def create_cluster(cluster_name, ip, base_dir):
        """
        Creates a cluster
        :param base_dir: Base directory that should contain the data
        :param ip: IP address of the first node of the new cluster
        :param cluster_name: Name of the cluster
        """
        logger.debug('Creating cluster {0} on {1}'.format(cluster_name, ip))
        base_dir = base_dir.rstrip('/')

        client = SSHClient(ip, username='root')
        node_name = System.get_my_machine_id(client)

        data_dir = EtcdInstaller.DATA_DIR.format(base_dir, cluster_name)
        wal_dir = EtcdInstaller.WAL_DIR.format(base_dir, cluster_name)
        abs_paths = [data_dir, wal_dir]
        client.dir_create(abs_paths)
        client.dir_chmod(abs_paths, 0755, recursive=True)
        client.dir_chown(abs_paths, 'ovs', 'ovs', recursive=True)

        base_name = 'ovs-etcd'
        target_name = 'ovs-etcd-{0}'.format(cluster_name)
        ServiceManager.add_service(base_name, client,
                                   params={'CLUSTER': cluster_name,
                                           'NODE_ID': node_name,
                                           'DATA_DIR': data_dir,
                                           'WAL_DIR': wal_dir,
                                           'SERVER_URL': EtcdInstaller.SERVER_URL.format(ip),
                                           'CLIENT_URL': EtcdInstaller.CLIENT_URL.format(ip),
                                           'LOCAL_CLIENT_URL': EtcdInstaller.CLIENT_URL.format('127.0.0.1'),
                                           'INITIAL_CLUSTER': '{0}={1}'.format(node_name, EtcdInstaller.SERVER_URL.format(ip)),
                                           'INITIAL_STATE': 'new',
                                           'INITIAL_PEERS': '-initial-advertise-peer-urls {0}'.format(EtcdInstaller.SERVER_URL.format(ip))},
                                   target_name=target_name)

        logger.debug('Creating cluster {0} on {1} completed'.format(cluster_name, ip))

    @staticmethod
    def extend_cluster(master_ip, new_ip, cluster_name, base_dir):
        """
        Extends a cluster to a given new node
        :param base_dir: Base directory that will hold the data
        :param cluster_name: Name of the cluster to be extended
        :param new_ip: IP address of the node to be added
        :param master_ip: IP of one of the already existing nodes
        """
        logger.debug('Extending cluster {0} from {1} to {2}'.format(cluster_name, master_ip, new_ip))
        base_dir = base_dir.rstrip('/')

        client = SSHClient(master_ip, username='root')
        current_cluster = []
        for item in client.run('etcdctl member list').splitlines():
            info = re.search(EtcdInstaller.MEMBER_REGEX, item).groupdict()
            current_cluster.append('{0}={1}'.format(info['name'], info['peer']))

        client = SSHClient(new_ip, username='root')
        node_name = System.get_my_machine_id(client)
        current_cluster.append('{0}={1}'.format(node_name, EtcdInstaller.SERVER_URL.format(new_ip)))

        data_dir = EtcdInstaller.DATA_DIR.format(base_dir, cluster_name)
        wal_dir = EtcdInstaller.WAL_DIR.format(base_dir, cluster_name)
        abs_paths = [data_dir, wal_dir]
        client.dir_create(abs_paths)
        client.dir_chmod(abs_paths, 0755, recursive=True)
        client.dir_chown(abs_paths, 'ovs', 'ovs', recursive=True)

        base_name = 'ovs-etcd'
        target_name = 'ovs-etcd-{0}'.format(cluster_name)
        EtcdInstaller.stop(cluster_name, client)  # Stop a possible proxy service
        ServiceManager.add_service(base_name, client,
                                   params={'CLUSTER': cluster_name,
                                           'NODE_ID': node_name,
                                           'DATA_DIR': data_dir,
                                           'WAL_DIR': wal_dir,
                                           'SERVER_URL': EtcdInstaller.SERVER_URL.format(new_ip),
                                           'CLIENT_URL': EtcdInstaller.CLIENT_URL.format(new_ip),
                                           'LOCAL_CLIENT_URL': EtcdInstaller.CLIENT_URL.format('127.0.0.1'),
                                           'INITIAL_CLUSTER': ','.join(current_cluster),
                                           'INITIAL_STATE': 'existing',
                                           'INITIAL_PEERS': ''},
                                   target_name=target_name)

        logger.debug('Extending cluster {0} from {1} to {2} completed'.format(cluster_name, master_ip, new_ip))

    @staticmethod
    def shrink_cluster(remaining_node_ip, deleted_node_ip, cluster_name):
        """
        Removes a node from a cluster, the old node will become a slave
        :param cluster_name: The name of the cluster to shrink
        :param deleted_node_ip: The ip of the node that should be deleted
        :param remaining_node_ip: The ip of a remaining node
        """
        logger.debug('Shrinking cluster {0} from {1}'.format(cluster_name, deleted_node_ip))

        EtcdInstaller.deploy_to_slave(remaining_node_ip, deleted_node_ip, cluster_name)

        logger.debug('Shrinking cluster {0} from {1} completed'.format(cluster_name, deleted_node_ip))

    @staticmethod
    def deploy_to_slave(master_ip, slave_ip, cluster_name):
        """
        Deploys the configuration file to a slave
        :param cluster_name: Name of the cluster of which to deploy the configuration file
        :param slave_ip: IP of the slave to deploy to
        :param master_ip: IP of the node to deploy from
        """
        master_client = SSHClient(master_ip, username='root')
        slave_client = SSHClient(slave_ip, username='root')

        current_cluster = []
        for item in master_client.run('etcdctl member list').splitlines():
            info = re.search(EtcdInstaller.MEMBER_REGEX, item).groupdict()
            current_cluster.append('{0}={1}'.format(info['name'], info['peer']))

        base_name = 'ovs-etcd-proxy'
        target_name = 'ovs-etcd-{0}'.format(cluster_name)
        EtcdInstaller.stop(cluster_name, slave_client)
        ServiceManager.add_service(base_name, slave_client,
                                   params={'CLUSTER': cluster_name,
                                           'LOCAL_CLIENT_URL': EtcdInstaller.CLIENT_URL.format('127.0.0.1'),
                                           'INITIAL_CLUSTER': ','.join(current_cluster)},
                                   target_name=target_name)
        EtcdInstaller.start(cluster_name, slave_client)

    @staticmethod
    def start(cluster_name, client):
        """
        Starts an etcd cluster
        :param client: Client on which to start the service
        :param cluster_name: The name of the cluster service to start
        """
        if ServiceManager.has_service('etcd-{0}'.format(cluster_name), client=client) is True and \
                ServiceManager.get_service_status('etcd-{0}'.format(cluster_name), client=client) is False:
            ServiceManager.start_service('etcd-{0}'.format(cluster_name), client=client)

    @staticmethod
    def stop(cluster_name, client):
        """
        Stops an etcd service
        :param client: Client on which to stop the service
        :param cluster_name: The name of the cluster service to stop
        """
        if ServiceManager.has_service('etcd-{0}'.format(cluster_name), client=client) is True and \
                ServiceManager.get_service_status('etcd-{0}'.format(cluster_name), client=client) is True:
            ServiceManager.stop_service('etcd-{0}'.format(cluster_name), client=client)

    @staticmethod
    def remove(cluster_name, client):
        """
        Removes an etcd service
        :param client: Client on which to remove the service
        :param cluster_name: The name of the cluster service to remove
        """
        if ServiceManager.has_service('etcd-{0}'.format(cluster_name), client=client) is True:
            ServiceManager.remove_service('etcd-{0}'.format(cluster_name), client=client)

    @staticmethod
    def restart_cluster_add(cluster_name, current_ip, new_ip):
        """
        Execute a (re)start sequence after adding a new node to a cluster.
        :param new_ip: IP of the newly added node
        :param current_ip: IP of one of the existing nodes
        :param cluster_name: Name of the cluster to restart
        """
        logger.debug('Restart sequence (add) for {0}'.format(cluster_name))
        logger.debug('Current ip: {0}'.format(current_ip))
        logger.debug('New ip: {0}'.format(new_ip))

        new_client = SSHClient(new_ip, username='root')
        node_name = System.get_my_machine_id(new_client)
        current_client = SSHClient(current_ip, username='root')

        current_client.run('etcdctl member add {0} {1}'.format(node_name, EtcdInstaller.SERVER_URL.format(new_ip)))
        EtcdInstaller.start(cluster_name, new_client)

        logger.debug('Started node {0} for cluster {1}'.format(new_ip, cluster_name))

    @staticmethod
    def restart_cluster_remove(cluster_name, remaining_ip, removed_ip):
        """
        Execute a restart sequence after removing a node from a cluster
        :param remaining_ip: IPs of the one of the remaining nodes after shrink
        :param removed_ip: IP of the node which is removed
        :param cluster_name: Name of the cluster to restart
        """
        logger.debug('Restart sequence (remove) for {0}'.format(cluster_name))
        logger.debug('Remaining ip: {0}'.format(remaining_ip))
        logger.debug('Removed ip: {0}'.format(removed_ip))

        old_client = SSHClient(removed_ip, username='root')
        node_name = System.get_my_machine_id(old_client)
        current_client = SSHClient(remaining_ip, username='root')
        node_id = None
        for item in current_client.run('etcdctl member list').splitlines():
            info = re.search(EtcdInstaller.MEMBER_REGEX, item).groupdict()
            if info['name'] == node_name:
                node_id = info['id']

        EtcdInstaller.stop(cluster_name, old_client)
        current_client.run('etcdctl member remove {0}'.format(node_id))
        EtcdInstaller.deploy_to_slave(remaining_ip, removed_ip, cluster_name)

        logger.debug('Restart sequence (remove) for {0} completed'.format(cluster_name))
