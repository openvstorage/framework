# Copyright 2014 CloudFounders NV
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

# This file violates a lot of PEP8-rules which are required to work with the non-PEP8 compliant Arakoon client files
# Do not correct the violations unless you're sure what you're doing.

import os
import time
import signal
import subprocess
import ConfigParser

from arakoon.Arakoon import ArakoonClientConfig, ArakoonClient
from arakoon.ArakoonManagement import ArakoonManagement, ArakoonCluster, logging

config_dir = '/opt/OpenvStorage/config'


def which_arakoon():
    return 'arakoon'


class ArakoonManagementEx(ArakoonManagement):
    """
    Overrides Incubaid's ArakoonManagement class
    """

    def __init__(self):
        """
        Dummy initializer
        """
        pass

    def getCluster(self, cluster_name):
        """
        @type cluster_name: string
        @return a helper to config that cluster
        """
        return ArakoonClusterEx(cluster_name)

    def listClusters(self):
        """
        Returns a list with the existing clusters.
        """
        return os.listdir('{0}/arakoon'.format(config_dir))


class ArakoonClusterEx(ArakoonCluster):
    """
    Overrides Incbaid's ArakoonCluster class.

    A few remarks:
    * Don't call super, as it makes a lot of assumptions
    * Make sure to validate all inherited calls before usage, as they might not work, or make wrong assuptions
    """

    def __init__(self, cluster_name):
        """
        Intitialize cluster constructor.
        """

        self.__validateName(cluster_name)
        # There's a difference between the clusterId and the cluster's name.
        # The name is used to construct the path to find the config file.
        # the id is what's inside the cfg file and what you need to provide to a client that want's to talk to the cluster.
        self._clusterName = cluster_name
        self._binary = which_arakoon()
        self._arakoonDir = '{0}/arakoon'.format(config_dir)

    def __validateName(self, name):
        if name is None or name.strip() == '':
            raise Exception('A name should be passed.  An empty name is not an option')
        if not isinstance(name, str):
            raise Exception('Name should be of type string')
        for char in [' ', ',', '#']:
            if char in name:
                raise Exception('Name should not contain %s' % char)

    @staticmethod
    def _read_config_file(path):
        """
        Reads a config file
        """
        c_parser = ConfigParser.ConfigParser()
        c_parser.read(path)
        c_parser._path = path
        return c_parser

    @staticmethod
    def _write_config_file(config_object):
        """
        Writes a configuration object
        """
        if not hasattr(config_object, '_path'):
            raise RuntimeError('The given configuration object does not contain a _path')
        with open(config_object._path, 'wb') as config_file:
            config_object.write(config_file)

    def _getConfigFilePath(self):
        return '{0}/{1}'.format(self._arakoonDir, self._clusterName)

    def _getConfigFile(self):
        return ArakoonClusterEx._read_config_file('{0}/{1}.cfg'.format(self._getConfigFilePath(), self._clusterName))

    def _getClientConfigFile(self):
        return ArakoonClusterEx._read_config_file('{0}/{1}_client.cfg'.format(self._getConfigFilePath(), self._clusterName))

    def _changeTlogCompression(self, nodes, value):
        if nodes is None:
            nodes = self.listNodes()
        else:
            for n in nodes:
                self.__validateName(n)
        config = self._getConfigFile()
        for n in nodes:
            if 'disable_tlog_compression' in config.options(n):
                config.remove_option(n, 'disable_tlog_compression')
            config.set(n, 'tlog_compression', value)

        ArakoonClusterEx._write_config_file(config)

    def enableTlogCompression(self, nodes=None, compressor='bz2'):
        """
        Enables tlog compression for the given nodes (this is enabled by default)
        @param nodes List of node names
        @param compressor one of 'bz2', 'snappy', 'none'
        """
        self._changeTlogCompression(nodes, compressor)

    def _changeFsync(self, nodes, value):
        if nodes is None:
            nodes = self.listNodes()
        else:
            for n in nodes:
                self.__validateName(n)

        config = self._getConfigFile()

        for node in nodes:
            config.set(node, 'fsync', value)

        ArakoonClusterEx._write_config_file(config)

    def getClientConfig(self):
        """
        Get an object that contains all node information in the supplied cluster
        @return dict the dict can be used as param for the ArakoonConfig object
        """
        config = self._getClientConfigFile()
        clientconfig = dict()

        nodes = self.__getNodes(config)

        for name in nodes:
            ips = config.get(name, 'ip')
            ip_list = ips.split(',')
            port = int(config.get(name, 'client_port'))
            clientconfig[name] = (ip_list, port)

        return clientconfig

    def getClient(self):
        config = self.getClientConfig()
        client = ArakoonClient(ArakoonClientConfig(self._clusterName, config))
        return client

    def getNodeConfig(self, name):
        """
        Get the parameters of a node section

        @param name the name of the node
        @return dict keys and values of the nodes parameters
        """
        self.__validateName(name)
        config = self._getConfigFile()

        if config.has_section(name):
            return dict(config.items(name))
        else:
            raise Exception('No node with name %s configured' % name)

    def createDirs(self, name):
        """
        Create the Directories for a local arakoon node in the supplied cluster

        @param name: the name of the node as configured in the config file
        """
        self.__validateName(name)

        config = self._getConfigFile()

        if config.has_section(name):
            home = config.get(name, 'home')
            subprocess.call(['mkdir', '-p', home])

            for option in ['tlog_dir', 'tlf_dir', 'head_dir']:
                if config.has_option(name, option):
                    option_dir = config.get(name, option)
                    subprocess.call(['mkdir', '-p', option_dir])

            log_dir = config.get(name, 'log_dir')
            subprocess.call(['mkdir', '-p', log_dir])

            return

        msg = 'No node %s configured' % name
        raise Exception(msg)

    def addLocalNode(self, name, config_filename=None):
        """
        Add a node to the list of nodes that have to be started locally
        from the supplied cluster

        @param name: the name of the node as configured in the config file
        @param config_filename: the filename to store the new config to (if none, the existing one is updated)
        """
        self.__validateName(name)

        config = self._getConfigFile()
        config_name = self._servernodes()
        if name in config:
            config_name_path = os.path.join(self._clusterPath, config_name)
            nodesconfig = ArakoonClusterEx._read_config_file(config_name_path)

            if not nodesconfig.has_section('global'):
                nodesconfig.add_section('global')
                nodesconfig.set('global', 'cluster', '')

            nodes = self.__getNodes(nodesconfig)
            if name in nodes:
                raise Exception('node %s already present' % name)
            nodes.append(name)
            nodesconfig.set('global', 'cluster', nodes)

            if config_filename:
                nodesconfig._path = config_filename
                if not os.path.exists(os.path.dirname(config_filename)):
                    os.makedirs(os.path.dirname(config_filename))

            ArakoonClusterEx._write_config_file(nodesconfig)
            return

        raise Exception('No node %s' % name)

    def listLocalNodes(self):
        """
        Get a list of the local nodes in the supplied cluster

        @return list of strings containing the node names
        """
        config_name = self._servernodes()
        config_name_path = '{0}/{1}.cfg'.format(self._getConfigFilePath(), config_name)
        config = ArakoonClusterEx._read_config_file(config_name_path)

        return self.__getNodes(config)

    def __getNodes(self, config):
        if config.has_section('global') and config.has_option('global', 'cluster'):
            return [node.strip() for node in config.get('global', 'cluster').split(',')]
        return []

    def start(self, daemon=True):
        """
        start all nodes in the cluster
        """
        rcs = {}
        from ovs.extensions.db.arakoon.CheckArakoonTlogMark import CheckArakoonTlogMark
        CheckArakoonTlogMark().fixtlogs(self._clusterName, always_stop=True)
        for name in self.listLocalNodes():
            rcs[name] = self._startOneEx(name, daemon)

        return rcs

    def _cmd(self, name):
        r = [self._binary, '--node', name, '-config',
             '%s/%s.cfg' % (self._getConfigFilePath(), self._clusterName),
             '-start']
        return r

    def _startOneEx(self, name, daemon):
        if self._getStatusOne(name):
            return

        config = self.getNodeConfig(name)
        cmd = []
        if 'wrapper' in config:
            cmd = config['wrapper'].split(' ')

        command = self._cmd(name)
        cmd.extend(command)
        if daemon:
            cmd.append('-daemonize')
        logging.debug('calling: %s', str(cmd))
        return subprocess.call(cmd, close_fds=True)

    def _stopOne(self, name):
        line = self._cmdLine(name)
        cmd = ['pkill', '-f', line]
        logging.debug("stopping '%s' with: %s" % (name, ' '.join(cmd)))
        rc = subprocess.call(cmd, close_fds=True)
        logging.debug('%s=>rc=%i' % (cmd, rc))
        i = 0
        while self._getStatusOne(name):
            rc = subprocess.call(cmd, close_fds=True)
            logging.debug('%s=>rc=%i' % (cmd, rc))
            time.sleep(1)
            i += 1
            logging.debug("'%s' is still running... waiting" % name)

            if i == 10:
                msg = "Requesting '%s' to dump crash log information" % name
                logging.debug(msg)
                subprocess.call(['pkill', '-%d' % signal.SIGUSR2, '-f', line], close_fds=True)
                time.sleep(1)

                logging.debug("stopping '%s' with kill -9" % name)
                rc = subprocess.call(['pkill', '-9', '-f', line], close_fds=True)
                if rc == 0:
                    rc = 9
                cnt = 0
                while self._getStatusOne(name):
                    logging.debug("'%s' is STILL running... waiting" % name)
                    time.sleep(1)
                    cnt += 1
                    if cnt > 10:
                        break
                break
            else:
                subprocess.call(cmd, close_fds=True)
        if rc < 9:
            rc = 0  # might be we looped one time too many.
        return rc

    def _getStatusOne(self, name):
        line = self._cmdLine(name)
        cmd = ['pgrep', '-fn', line]
        proc = subprocess.Popen(cmd, close_fds=True, stdout=subprocess.PIPE)
        pids = proc.communicate()[0]
        pid_list = pids.split()
        lenp = len(pid_list)
        if lenp == 1:
            result = True
        elif lenp == 0:
            result = False
        else:
            for pid in pid_list:
                try:
                    f = open('/proc/%s/cmdline' % pid, 'r')
                    startup = f.read()
                    f.close()
                    logging.debug('pid=%s; cmdline=%s', pid, startup)
                except:
                    pass
            raise Exception('multiple matches', pid_list)
        return result

    def writeClientConfig(self, config=None, config_filename=None):
        """
        Write Arakoon Cluster client config to file

        @param config: arakoon client config for this cluster (if none, will be retrieved from current cluster config)
        @param config_filename: the filename to store the config to (if none, the existing one is updated)
        """
        if not config_filename:
            client_config = self._getClientConfigFile()
        else:
            client_config = ArakoonClusterEx._read_config_file(config_filename)

        if not config:
            config = self.getClientConfig()

        if not client_config.has_section('global'):
            client_config.add_section('global')
            client_config.set('global', 'cluster_id', self._clusterName)
            client_config.set('global', 'cluster', config.keys())

        for node, node_config in config.iteritems():
            if not client_config.has_section(node):
                client_config.add_section(node)
            client_config.set(node, 'name', node)
            client_config.set(node, 'ip', node_config[0][0])
            client_config.set(node, 'client_port', node_config[1])

        ArakoonClusterEx._write_config_file(client_config)

if __name__ == '__main__':
    from optparse import OptionParser

    parser = OptionParser(description='Arakoon Management')
    parser.add_option('--stop', dest='start_stop', action='store_false', default=None, help='Stop arakoon')
    parser.add_option('--start', dest='start_stop', action='store_true', default=None, help='Start arakoon')
    parser.add_option('-c', '--cluster', dest='cluster', help='Name of arakoon cluster')
    (options, args) = parser.parse_args()

    if not options.cluster:
        parser.error('No arakoon cluster specified')
    if options.start_stop is None:
        parser.error('No action specified')

    arakoonManagement = ArakoonManagementEx()
    arakoon_cluster = arakoonManagement.getCluster(options.cluster)
    if options.start_stop:
        arakoon_cluster.start(False)
    else:
        arakoon_cluster.stop()
