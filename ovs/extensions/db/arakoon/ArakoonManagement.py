"""
This file is part of Arakoon, a distributed key-value store. Copyright
(C) 2010 Incubaid BVBA

Licensees holding a valid Incubaid license may use this file in
accordance with Incubaid's Arakoon commercial license agreement. For
more information on how to enter into this agreement, please contact
Incubaid (contact details can be found on www.arakoon.org/licensing).

Alternatively, this file may be redistributed and/or modified under
the terms of the GNU Affero General Public License version 3, as
published by the Free Software Foundation. Under this license, this
file is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
FITNESS FOR A PARTICULAR PURPOSE.

See the GNU Affero General Public License for more details.
You should have received a copy of the
GNU Affero General Public License along with this program (file "COPYING").
If not, see <http://www.gnu.org/licenses/>.

Changes applied by CloudFounders NV:
- Remove dependencies on pymonkey and/or pylabs
- Various changes to make this library work with the OpenvStorage product
"""

import os
import time
import subprocess
import types
import signal
import string
import logging

import Arakoon
from ovs.extensions.db.arakoon import ArakoonRemoteControl
from configobj import ConfigObj

cfgDir = '/opt/OpenvStorage/config'

def which_arakoon():
    return "arakoon"

class ArakoonManagement:
    def getCluster(self, clusterName):
        """
        @type clusterName: string
        @return a helper to config that cluster
        """
        return ArakoonCluster(clusterName)

    def listClusters(self):
        """
        Returns a list with the existing clusters.
        """
        return os.listdir('{0}/arakoon'.format(cfgDir))

    def start(self):
        """
        Starts all clusters.
        """
        [clus.start() for clus in [self.getCluster(cluster) for cluster in self.listClusters()]]

    def stop(self):
        """
        Stops all clusters.
        """
        [clus.stop() for clus in [self.getCluster(cluster) for cluster in self.listClusters()]]

    def restart(self):
        """
        Restarts all clusters.
        """
        self.stop()
        self.start()

class ArakoonCluster:

    def __init__(self, clusterName):
        self.__validateName(clusterName)
        self._clusterName = clusterName
        self._binary = which_arakoon()
        self._arakoonDir = '{0}/arakoon'.format(cfgDir)

#         clusterConfig = q.config.getInifile("arakoonclusters")
#         if not clusterConfig.checkSection(self._clusterName):
#
#             clusterPath = q.system.fs.joinPaths(q.dirs.cfgDir,"qconfig", "arakoon", clusterName)
#             clusterConfig.addSection(clusterName)
#             clusterConfig.addParam(clusterName, "path", clusterPath)
#
#             if not q.system.fs.exists(self._arakoonDir):
#                 q.system.fs.createDir(self._arakoonDir)
#
#             if not q.system.fs.exists(clusterPath):
#                 q.system.fs.createDir(clusterPath)
#
#         self._clusterPath = clusterConfig.getValue( clusterName, "path" )

    def _servernodes(self):
        return '%s_local_nodes' % self._clusterName

    def __repr__(self):
        return "<ArakoonCluster:%s>" % self._clusterName

    def _getConfigFilePath(self):
        return '{0}/{1}'.format(self._arakoonDir,self._clusterName)

    def _getConfigFile(self):
        path = self._getConfigFilePath()
        return ConfigObj('{0}/{1}.cfg'.format(self._getConfigFilePath(),self._clusterName))

    def _getClientConfigFile(self):
        path = self._getConfigFilePath()
        return ConfigObj('{0}/{1}_client.cfg'.format(self._getConfigFilePath(),self._clusterName))

    def _getClusterId(self):
        clusterId = self._clusterName
        try:
            config = self._getConfigFile()
            clusterId = config["global"]["cluster_id"]
            return clusterId
        except:
            logging.info("setting cluster_id to %s", clusterId)
            config["global"]["cluster_id"] = clusterId
            config.write()

    def _changeTlogCompression(self, nodes, value):
        if nodes is None:
            nodes = self.listNodes()
        else:
            for n in nodes :
                self.__validateName( n )
        config = self._getConfigFile()
        for n in nodes:
            config[n]["disable_tlog_compression"] = value

        config.write()

    def enableTlogCompression(self, nodes=None):
        """
        Enables tlog compression for the given nodes (this is enabled by default)
        @param nodes List of node names
        """
        self._changeTlogCompression(nodes, 'false')

    def disableTlogCompression(self, nodes=None):
        """
        Disables tlog compression for the given nodes
        @param nodes List of node names
        """
        self._changeTlogCompression(nodes, 'true')


    def _changeFsync(self, nodes, value):
        if nodes is None:
            nodes = self.listNodes()
        else:
            for n in nodes:
                self.__validateName(n)

        config = self._getConfigFile()

        for node in nodes:
            config[node]['fsync'] = value

        config.write()

    def enableFsync(self, nodes=None):
        '''Enable fsync'ing of tlogs after every operation'''
        self._changeFsync(nodes, 'true')

    def disableFsync(self, nodes=None):
        '''Disable fsync'ing of tlogs after every operation'''
        self._changeFsync(nodes, 'false')

    def getClientConfig(self):
        """
        Get an object that contains all node information in the supplied cluster
        @return dict the dict can be used as param for the ArakoonConfig object
        """
        config = self._getConfigFile()
        clientconfig = dict()

        nodes = self.__getNodes(config)

        for name in nodes:
            ips = config[name]["ip"]
            ip_list = ips.split(',')
            port = int(config[name]["client_port"])
            clientconfig[name] = (ip_list, port)

        return clientconfig

    def getClient(self):
        config = self.getClientConfig()
        client = Arakoon.ArakoonClient(Arakoon.ArakoonClientConfig(self._clusterName, config))
        return client

    def listNodes(self):
        """
        Get a list of all node names in the supplied cluster
        @return list of strings containing the node names
        """
        config = self._getConfigFile()
        return self.__getNodes(config)

    def getNodeConfig(self,name):
        """
        Get the parameters of a node section

        @param name the name of the node
        @return dict keys and values of the nodes parameters
        """
        self.__validateName(name)

        config = self._getConfigFile()

        nodes = self.__getNodes(config)

        if config.has_key(name):
            return config[name]
        else:
            raise Exception("No node with name %s configured" % name)

    def listLocalNodes(self):
        """
        Get a list of the local nodes in the supplied cluster

        @return list of strings containing the node names
        """
        config_name = self._servernodes()
        config_name_path = '{0}/{1}.cfg'.format(self._getConfigFilePath(), config_name)
        config = ConfigObj(config_name_path)

        return self.__getNodes(config)

    def __getNodes(self, config):
        if not config.has_key("global"):
            return []
        nodes = []
        try:
            if type(config['global']['cluster']) == list:
                nodes = map(lambda x: x.strip(), config['global']['cluster'])
            else:
                nodes = [config['global']['cluster'].strip(),]
        except LookupError:
            pass
        return nodes

    def __validateInt(self,name, value):
        typ = type(value)
        if not typ == type(1):
            raise Exception("%s=%s (type = %s) but should be an int" % (name, value, typ))

    def __validateName(self, name):
        if name is None or name.strip() == "":
            raise Exception("A name should be passed.  An empty name is not an option")

        if not type(name) == type(str()):
            raise Exception("Name should be of type string")

        for char in [' ', ',', '#']:
            if char in name:
                raise Exception("name should not contain %s" % char)

    def start(self, daemon=True):
        """
        start all nodes in the cluster
        """
        rcs = {}

        for name in self.listLocalNodes():
            rcs[name] = self._startOne(name, daemon)

        return rcs

    def stop(self):
        """
        stop all nodes in the supplied cluster

        @param cluster the arakoon cluster name
        """
        rcs = {}
        for name in self.listLocalNodes():
            rcs[name] = self._stopOne(name)

        return rcs

    def getStatus(self):
        """
        Get the status the cluster's nodes running on this machine

        @return dict node name -> status (q.enumerators.AppStatusType)
        """
        status = {}
        for name in self.listLocalNodes():
            status[name] = self._getStatusOne(name)

        return status

    def _cmd(self, name):
        r =  [self._binary,'--node',name,'-config',
              '%s/%s.cfg' % (self._getConfigFilePath(), self._clusterName),
              '-start']
        return r

    def _cmdLine(self, name):
        cmd = self._cmd(name)
        cmdLine = string.join(cmd, ' ')
        return cmdLine

    def _getStatusOne(self,name):
        line = self._cmdLine(name)
        cmd = ['pgrep','-fn', line]
        proc = subprocess.Popen(cmd,
                                close_fds = True,
                                stdout=subprocess.PIPE)
        pids = proc.communicate()[0]
        pid_list = pids.split()
        lenp = len(pid_list)
        result = None
        if lenp == 1:
            result = True
        elif lenp == 0:
            result = False
        else:
            for pid in pid_list:
                try:
                    f = open('/proc/%s/cmdline' % pid,'r')
                    startup = f.read()
                    f.close()
                    logging.debug("pid=%s; cmdline=%s", pid, startup)
                except:
                    pass
            raise Exception("multiple matches", pid_list)
        return result

    def _startOne(self, name, daemon):
        if self._getStatusOne(name):
            return

        config = self.getNodeConfig(name)
        cmd = []
        if 'wrapper' in config :
            wrapperLine = config['wrapper']
            cmd = wrapperLine.split(' ')

        command = self._cmd(name)
        cmd.extend(command)
        if daemon: cmd.append('-daemonize')
        logging.debug('calling: %s', str(cmd))
        return subprocess.call(cmd, close_fds = True)

    def _getIp(self, ip_mess):
        t_mess = type(ip_mess)
        if t_mess == types.StringType:
            parts = ip_mess.split(',')
            ip = string.strip(parts[0])
            return ip
        elif t_mess == types.ListType:
            return ip_mess[0]
        else:
            raise Exception("should '%s' be a string or string list")

    def _stopOne(self, name):
        line = self._cmdLine(name)
        cmd = ['pkill', '-f',  line]
        logging.debug("stopping '%s' with: %s"%(name, string.join(cmd, ' ')))
        rc = subprocess.call(cmd, close_fds = True)
        i = 0
        while self._getStatusOne(name):
            rc = subprocess.call(cmd, close_fds = True)
            logging.debug("%s=>rc=%i" % (cmd,rc))
            time.sleep(1)
            i += 1
            logging.debug("'%s' is still running... waiting" % name)

            if i == 10:
                msg = "Requesting '%s' to dump crash log information" % name
                logging.debug(msg)
                subprocess.call(['pkill', '-%d' % signal.SIGUSR2, '-f', line], close_fds=True)
                time.sleep(1)

                logging.debug("stopping '%s' with kill -9" % name)
                rc = subprocess.call(['pkill', '-9', '-f', line], close_fds = True)
                if rc == 0:
                    rc = 9
                cnt = 0
                while self._getStatusOne(name) :
                    logging.debug("'%s' is STILL running... waiting" % name)
                    time.sleep(1)
                    cnt += 1
                    if( cnt > 10):
                        break
                break
            else:
                subprocess.call(cmd, close_fds=True)
        if rc < 9:
            rc = 0 # might be we looped one time too many.
        return rc

    def remoteCollapse(self, nodeName, n):
        """
        Tell the targetted node to collapse all but n tlog files
        @type nodeName: string
        @type n: int
        """
        config = self.getNodeConfig(nodeName)
        ip_mess = config['ip']
        ip = self._getIp(ip_mess)
        port = int(config['client_port'])
        clusterId = self._getClusterId()
        ArakoonRemoteControl.collapse(ip,port,clusterId, n)

    def createDirs(self, name):
        """
        Create the Directories for a local arakoon node in the supplied cluster

        @param name: the name of the node as configured in the config file
        """
        self.__validateName(name)

        config = self._getConfigFile()

        if config.has_key(name):
            home = config[name]["home"]
            subprocess.call(['mkdir', '-p', home])

            if config[name].has_key("tlog_dir"):
                tlogDir = config[name]["tlog_dir"]
                subprocess.call(['mkdir', '-p', tlogDir])

            if config[name].has_key("tlf_dir"):
                tlfDir = config[name]["tlf_dir"]
                subprocess.call(['mkdir', '-p', tlfDir])

            if config[name].has_key("head_dir"):
                headDir = config[name]["head_dir"]
                subprocess.call(['mkdir', '-p', headDir])

            logDir = config[name]["log_dir"]
            subprocess.call(['mkdir', '-p', logDir])

            return

        msg = "No node %s configured" % name
        raise Exception(msg)

    def addNode(self,
                name,
                ip = "127.0.0.1",
                clientPort = 7080,
                messagingPort = 10000,
                logLevel = "info",
                logDir = None,
                home = None,
                tlogDir = None,
                wrapper = None,
                isLearner = False,
                targets = None,
                isLocal = False,
                logConfig = None,
                batchedTransactionConfig = None,
                tlfDir = None,
                headDir = None,
                configFilename = None):
        """
        Add a node to the configuration of the supplied cluster

        @param name : the name of the node, should be unique across the environment
        @param ip : the ip(s) this node should be contacted on (string or string list)
        @param clientPort : the port the clients should use to contact this node
        @param messagingPort : the port the other nodes should use to contact this node
        @param logLevel : the loglevel (debug info notice warning error fatal)
        @param logDir : the directory used for logging
        @param home : the directory used for the nodes data
        @param tlogDir : the directory used for tlogs (if none, home will be used)
        @param wrapper : wrapper line for the executable (for example 'softlimit -o 8192')
        @param isLearner : whether this node is a learner node or not
        @param targets : for a learner node the targets (string list) it learns from
        @param isLocal : whether this node is a local node and should be added to the local nodes list
        @param logConfig : specifies the log config to be used for this node
        @param batchedTransactionConfig : specifies the batched transaction config to be used for this node
        @param tlfDir : the directory used for tlfs (if none, tlogDir will be used)
        @param headDir : the directory used for head.db (if none, tlfDir will be used)
        @param configFilename: the filename to store the new config to (if none, the existing one is updated)
        """
        self.__validateName(name)

        config = self._getConfigFile()
        nodes = self.__getNodes(config)

        if name in nodes:
            raise Exception("node %s already present" % name )
        if not isLearner:
            nodes.append(name)

        config[name] = {"name" : name}

        if type(ip) == types.StringType:
            config[name].update({"ip": ip})
        elif type(ip) == types.ListType:
            line = string.join(ip,',')
            config[name].update({"ip": line})
        else:
            raise Exception("ip parameter needs string or string list type")

        self.__validateInt("clientPort", clientPort)
        config[name].update({"client_port": clientPort})
        self.__validateInt("messagingPort", messagingPort)
        config[name].update({"messaging_port": messagingPort})
        config[name].update({"log_level": logLevel})

        if logConfig is not None:
            config[name].update({"log_config": logConfig})

        if batchedTransactionConfig is not None:
            config[name].update({"batched_transaction_config": batchedTransactionConfig})

        if wrapper is not None:
            config[name].update({"wrapper": wrapper})

        if logDir is None:
            logDir = os.path.join(os.sep, 'var', 'log', 'arakoon', self._clusterName, name)
        config[name].update({"log_dir": logDir})

        if home is None:
            home = os.path.join(os.sep, 'var', "db", self._clusterName, name)
        config[name].update({"home": home})

        if tlogDir:
            config[name].update({"tlog_dir": tlogDir})

        if tlfDir:
            config[name].update({"tlf_dir": tlfDir})

        if headDir:
            config[name].update({"head_dir": headDir})

        if isLearner:
            config[name].update({"learner": "true"})
            if targets is None:
                targets = self.listNodes()
            config[name].update({"targets": targets})

        if not config.has_key("global") :
            config["global"] = dict()
            config["global"].update({"cluster_id": self._clusterName})
        config["global"].update({"cluster": nodes})

        if configFilename:
            config.filename = configFilename
        config.write()

        if isLocal:
            self.addLocalNode(name)

    def addLocalNode(self, name, configFilename=None):
        """
        Add a node to the list of nodes that have to be started locally
        from the supplied cluster

        @param name: the name of the node as configured in the config file
        @param configFilename: the filename to store the new config to (if none, the existing one is updated)
        """
        self.__validateName(name)

        config = self._getConfigFile()
        nodes = self.__getNodes(config)
        config_name = self._servernodes()
        if config.has_key(name):
            config_name_path = os.path.join(self._clusterPath, config_name)
            nodesconfig = ConfigObj(config_name_path)

            if not nodesconfig.has_key("global"):
                nodesconfig["global"] = dict()
                nodesconfig["global"].update({"cluster": ""})

            nodes = self.__getNodes(nodesconfig)
            if name in nodes:
                raise Exception("node %s already present" % name)
            nodes.append(name)
            nodesconfig["global"].update({"cluster": nodes})

            if configFilename:
                nodesconfig.filename = configFilename
            nodesconfig.write()

            return
        
        raise Exception("No node %s" % name)

    def writeClientConfig(self, config=None, configFilename=None):
        """
        Write Arakoon Cluster client config to file
        
        @param config: arakoon client config for this cluster (if none, will be retrieved from current cluster config)
        @param configFilename: the filename to store the config to (if none, the existing one is updated)
        """
        if not configFilename:
            clientConfig = self._getClientConfigFile()
        else:
            clientConfig = ConfigObj(configFilename)

        if not config:
            config = self.getClientConfig()

        if not clientConfig.has_key('global'):
            clientConfig['global'] = dict()
            clientConfig['global'].update({'cluster_id': self._clusterName,
                                           'cluster': config.keys()})

        for node,node_config in config.iteritems():
            if not clientConfig.has_key(node):
                clientConfig[node] = dict()
            clientConfig[node].update({'name': node,
                                       'ip': node_config[0][0],
                                       'client_port': node_config[1]})

        clientConfig.write()

if __name__ == '__main__':
    from optparse import OptionParser
    
    parser = OptionParser(description='Arakoon Management')
    parser.add_option('--stop', dest='start_stop', action="store_false", default=None,
                      help="Stop arakoon")
    parser.add_option('--start', dest='start_stop', action="store_true", default=None,
                      help="Start arakoon")
    parser.add_option('-c', '--cluster', dest="cluster",
                      help="Name of arakoon cluster")
    (options, args) = parser.parse_args()
    
    if not options.cluster:
        parser.error("No arakoon cluster specified")
    if options.start_stop == None:
        parser.error("No action specified")
    
    ArakoonManagement = ArakoonManagement()
    arakoon_cluster = ArakoonManagement.getCluster(options.cluster)
    if options.start_stop:
        arakoon_cluster.start()
    else:
        arakoon_cluster.stop()
