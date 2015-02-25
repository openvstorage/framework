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
from ConfigParser import RawConfigParser

from arakoon.Arakoon import ArakoonClientConfig, ArakoonClient
from arakoon.ArakoonManagement import ArakoonManagement, ArakoonCluster, logging
from ovs.extensions.generic.system import System

config_dir = '/opt/OpenvStorage/config'


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
    Overrides Incubaid's ArakoonCluster class.

    A few remarks:
    * Don't call super, as it makes a lot of assumptions
    * Make sure to validate all inherited calls before usage, as they might not work, or make wrong assumptions
    """

    def __init__(self, cluster_name):
        """
        Intitialize cluster constructor.
        """

        self.__validateName(cluster_name)
        # There's a difference between the clusterId and the cluster's name.
        # The name is used to construct the path to find the config file.
        # the id is what's inside the cfg file and what you need to provide
        # to a client that want's to talk to the cluster.
        self._clusterName = cluster_name
        self._binary = 'arakoon'
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
        c_parser = RawConfigParser()
        c_parser.read(path)
        c_parser._path = path
        return c_parser

    def _get_config_file_path(self):
        return '{0}/{1}'.format(self._arakoonDir, self._clusterName)

    def _getConfigFile(self):
        return ArakoonClusterEx._read_config_file('{0}/{1}.cfg'.format(self._get_config_file_path(), self._clusterName))

    def getClientConfig(self):
        """
        Get an object that contains all node information in the supplied cluster
        @return dict the dict can be used as param for the ArakoonConfig object
        """
        config = self._getConfigFile()
        clientconfig = {}

        nodes = self.__getNodes(config)
        for name in nodes:
            ips = [ip.strip() for ip in config.get(name, 'ip').strip().split(',') if ip.strip() != '']
            port = int(config.get(name, 'client_port'))
            clientconfig[name] = (ips, port)
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
            raise Exception('No node with name {0} configured'.format(name))

    def createDirs(self, name):
        """
        Create the Directories for a local arakoon node in the supplied cluster

        @param name: the name of the node as configured in the config file
        """
        self.__validateName(name)
        config = self._getConfigFile()
        if not config.has_section(name):
            raise Exception('No node {0} configured'.format(name))

        home = config.get(name, 'home')
        if not os.path.exists(home):
            os.makedirs(home)

        for option in ['tlog_dir', 'tlf_dir', 'head_dir']:
            if config.has_option(name, option):
                option_dir = config.get(name, option)
                if not os.path.exists(option_dir):
                    os.makedirs(option_dir)

        log_dir = config.get(name, 'log_dir')
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)

    def __getNodes(self, config):
        if config.has_section('global') and config.has_option('global', 'cluster'):
            return [node.strip() for node in config.get('global', 'cluster').split(',')]
        return []

    def start(self, daemon=True):
        """
        start all nodes in the cluster
        first do a catchup
        """
        from ovs.extensions.db.arakoon.CheckArakoonTlogMark import CheckArakoonTlogMark

        CheckArakoonTlogMark().fixtlogs(self._clusterName, always_stop=True)
        node_name = System.get_my_machine_id()
        self._catchup_node(node_name)
        self._start_one_ex(node_name, daemon)

    def _cmd(self, name):
        return [self._binary,
                '--node', name,
                '-config', '{0}/{1}.cfg'.format(self._get_config_file_path(), self._clusterName),
                '-start']

    def _start_one_ex(self, name, daemon):
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

    def _catchup_node(self, name):
        """
        arakoon -catchup-only --node <NODEID> -config <CONFIGFILE>
        """
        status = self._getStatusOne(name)
        if status is True:
            self._stopOne(name)
        status = self._getStatusOne(name)
        if status is True:
            raise RuntimeError('Cannot stop node %s' % name)
        cmd = self._cmd(name)
        cmd.remove('-start')
        cmd.append('-catchup-only')
        rc = subprocess.call(cmd)
        status = self._getStatusOne(name)
        return rc

if __name__ == '__main__':
    from optparse import OptionParser

    parser = OptionParser(description='Arakoon Management')
    parser.add_option('--stop', dest='start', action='store_false', default=None, help='Stop arakoon')
    parser.add_option('--start', dest='start', action='store_true', default=None, help='Start arakoon')
    parser.add_option('-c', '--cluster', dest='cluster', help='Name of arakoon cluster')
    (options, args) = parser.parse_args()

    if not options.cluster:
        parser.error('No arakoon cluster specified')
    if options.start is None:
        parser.error('No action specified')

    arakoonManagement = ArakoonManagementEx()
    arakoon_cluster = arakoonManagement.getCluster(options.cluster)
    if options.start:
        arakoon_cluster.start(False)
    else:
        arakoon_cluster.stop()
