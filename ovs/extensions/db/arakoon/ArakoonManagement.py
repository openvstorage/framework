import os
import subprocess
import signal
import string
import logging

import Arakoon
from ArakoonExceptions import ArakoonNodeNotLocal
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

    def _getClusterId(self):
        clusterId = self._clusterName
        try:
            config = self._getConfigFile()
            clusterId = config["global"]["cluster_id"]
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
            line = config["global"]["cluster"].strip()
            # "".split(",") -> ['']
            if line == "":
                nodes =  []
            else:
                nodes = line.split(",")
                nodes = map(lambda x: x.strip(), nodes)
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
