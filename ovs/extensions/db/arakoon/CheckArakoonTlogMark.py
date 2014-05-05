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


import sys
import os
import time
import subprocess
import shutil
from ovs.extensions.db.arakoon.EnsureArakoonWorks import EnsureArakoonWorks
from ovs.extensions.db.arakoon.ArakoonManagement import ArakoonManagement
from ovs.plugin.provider.logger import Logger
from ovs.plugin.provider.process import Process

manager = ArakoonManagement()
ensurearakoonworks = EnsureArakoonWorks()


class CheckArakoonError(Exception):

    def __init__(self, message):
        self.message = message

    def __speak__(self):
        print "{0}".format(self.message)


class CheckArakoonTlogMark():

    """
    check if tlogs need marking
    mark tlogs that are unmarked
    start arakoon and check if running
    if not, move db and try again
    if so, stop arakoon

    Need to know localnode name to get
        - tlog file name to get
            - tlog file dump
    Then check if dump ends with string 'closed:localnode name'
        - if exists, log a message
        - if does not exist, mark and log a message

    Dictionary used througout:
        self._localnodesfiles =
        {
            'localnode0':
            {
                'tlogfile':<tlogpath>,
                'tlogdir':<tlogdirpath>,
                'dbfile':<dbpath>,
                'cluster':clusterobject,
                <tlogpath>:tlogdump
            },
            'localnode1':....same as localnode0
            'localnode2':....same as localnode0
            ...
        }
    """

    def __init__(self):
        clusters = manager.listClusters()
        if not clusters:
            self._lockfile()
            raise CheckArakoonError("No  clusters found")
        self._clusters = clusters
        self._isgrid = False
        self._localnodesfiles = dict()
        self._waitduration = 5
        self._initialwait = 120
        self._marklength = 26  # Including the newline
        self._dumpcount = 0
        self._finalmessage = "Node {0} Good Status"
        self._stoplockfile = os.path.join(
            os.sep, 'opt', 'OpenvStorage', '.startupfaillock')

    def _speak(self, message):
        """ log to standard output """
        leader = "[arakoon_startup]:"
        logmessage = "{0} {1}".format(leader, message)
        print(logmessage)
        Logger.log(logmessage, 1)
        sys.stdout.flush()

    def _wait(self, duration=None):
        """ wait for a specific duration """

        if duration is None:
            duration = self._waitduration
        self._speak("Waiting for {0} Seconds".format(duration))
        time.sleep(duration)

    def _lockfile(self):
        open(self._stoplockfile, 'a').close()

    def _checkarakoonstatus(self, localnode, cluster):
        """
        various checks for running arakoon
        return after all checks are good
        """

        extensionstatus = cluster._getStatusOne(localnode)
        self._speak(
            "Arakoon getStatusOne for Localnode {1}: {0}".format(extensionstatus, localnode))
        if extensionstatus is True:
            processstatus = Process.checkProcess('arakoon')
            if processstatus:
                self._speak("Arakoon Process is not Running")
                return False
            else:
                self._speak("Arakoon Process is Running")
                return True
        else:
            return False

    def _waitandcheck(self, duration, localnode, cluster):
        """ wait granularly and check in between """

        loops = duration / self._waitduration
        # initially wait longer as an early arakoon can report a
        # false positive if replay of tlogs is happening
        self._speak("Waiting for possible startup and then checking status")
        self._wait(duration=10)
        arakoonstatus = self._checkarakoonstatus(localnode, cluster)
        for loop in range(loops):
            duration -= self._waitduration
            if arakoonstatus:
                continue
            else:
                self._speak(
                    "Remaining Wait Duration: {0} Seconds".format(duration))
                self._wait()
                arakoonstatus = self._checkarakoonstatus(localnode, cluster)

        if duration > 0:
            time.sleep(duration)
            arakoonstatus = self._checkarakoonstatus(localnode, cluster)

        return arakoonstatus

    def _gatherlocalnodes(self, cluster):
        """ gather all localnodes for all clusters """

        localnodes = cluster.listLocalNodes()
        self._speak("Found local nodes {0}".format(localnodes))

        for localnode in localnodes:
            self._localnodesfiles[localnode] = dict()
            self._localnodesfiles[localnode]['cluster'] = cluster

    def _gettlogdir(self, localnode, cluster):
        """ get tlog dir and return """

        self._localnodesfiles[localnode][
            'tlogdir'] = cluster.getNodeConfig(localnode)['tlog_dir']
        tlogdirmessage = "Tlog directory for {0} is {1}"
        self._speak(tlogdirmessage.format(
            localnode, self._localnodesfiles[localnode]['tlogdir']))

    def _gettlogfile(self, tlogfilelist, localnode):
        """ get tlogfile and return """

        tlogfilelist.sort(reverse=True)
        self._localnodesfiles[localnode]['tlogfile'] = tlogfilelist[0]
        tlogfilemessage = "Tlogfile for {0} is {1}"
        self._speak(tlogfilemessage.format(
            localnode, self._localnodesfiles[localnode]['tlogfile']))

    def _getdbdir(self, localnode, cluster):
        """ get db dir and return """

        self._localnodesfiles[localnode][
            'dbdir'] = cluster.getNodeConfig(localnode)['home']
        dbdirmessage = "Db directory for {0} is {1}"
        self._speak(dbdirmessage.format(
            localnode, self._localnodesfiles[localnode]['dbdir']))

    def _truncatetlog(self, localnode, cluster):
        """ truncate possibly corrupted tlog and try dump again """

        tlogfile = self._localnodesfiles[localnode]['tlogfile']
        self._speak(
            "Truncating tlog file {0} for localnode {1}".format(tlogfile, localnode))
        tlogtruncatecommand = ['arakoon', '--truncate-tlog', tlogfile]
        try:
            subprocess.check_call(tlogtruncatecommand)
        except subprocess.CalledProcessError as e:
            truncatemessage = "Truncating Tlog failed with exit status {0}"
            self._speak(truncatemessage.format(e.returncode))

    def _dumptlog(self, localnode, cluster):
        """ set tlog name and dump for the current localnode """

        _ = cluster
        tlogfile = self._localnodesfiles[localnode]['tlogfile']
        self._speak(
            "Dumping tlog file {0} for localnode {1}".format(tlogfile, localnode))
        tlogdumpcommand = ['arakoon', '--dump-tlog', tlogfile]
        try:
            tlogdump = subprocess.check_output(tlogdumpcommand)
        except subprocess.CalledProcessError as e:
            dumpmessage = "Dumping tlog failed with exit status {0}"
            self._speak(dumpmessage.format(e.returncode))
            return False

        self._localnodesfiles[localnode][tlogfile] = tlogdump

        displaylength = self._marklength * 4
        tlogdumplength = len(tlogdump)
        tlogdumpsnippet = tlogdump[tlogdumplength - displaylength:]
        self._speak(
            "Got tlog dump - last {1} chars:\n{0}".format(tlogdumpsnippet, displaylength))

        return True

    def _marktlogfile(self, localnode, cluster):
        """ mark tlog file for a local localnode """
        tlogfile = self._localnodesfiles[localnode]['tlogfile']
        self._speak(
            "Marking tlog file {0} for localnode {1}".format(tlogfile, localnode))
        markcommand = "arakoon --mark-tlog {0} 'closed:{1}'".format(
            tlogfile, localnode)
        try:
            subprocess.call(markcommand, shell=True)
        except subprocess.CalledProcessError as e:
            # debugging errors
            self._dumptlog(localnode, cluster)
            markmessage = "Marking tlog failed with exit status {0}"
            self._speak(markmessage.format(e.returncode))
            return False

        self._speak("Marked tlog file with status 0, {0}".format(markcommand))
        return True

    def _checkmark(self, localnode, cluster):
        """ test end of tlogdump for presence of mark and mark if not there """

        tlogfile = self._localnodesfiles[localnode]['tlogfile']
        tlogdump = self._localnodesfiles[localnode][tlogfile]

        localnodemark = '"closed:{0}"\n'.format(localnode)
        markfound = tlogdump.endswith(localnodemark)
        tlogdumplength = len(tlogdump)
        if not markfound:
            self._speak("Tlog file {0} needs marking".format(tlogfile))
            return False

        foundmark = tlogdump[(tlogdumplength - self._marklength):]
        self._speak(
            "Tlog file {0} for localnode {1} has mark {2}".format(tlogfile, localnode, foundmark))
        return True

    def _movearakoondb(self, localnode, cluster, headdb=False, failover=False):
        """ move possibly corrupt files """

        dbdir = self._localnodesfiles[localnode]['dbdir']
        tlogdir = self._localnodesfiles[localnode]['tlogdir']
        nodedb = os.path.join(dbdir, '{0}.db'.format(localnode))
        budir = '/tmp/arakoonbu_{0}-{1}'.format(localnode, time.time())
        os.makedirs(budir)
        dbfiles = [f for f in os.listdir(dbdir) if os.path.isfile(f)]
        if dbfiles:
            self._speak(
                "Moving db files to temporary directory {0}".format(budir))
            for dbfile in dbfiles:
                os.rename(dbfile, budir)
        else:
            self._speak("Db files do not exist")

        if headdb:
            headdb = os.path.join(tlogdir, 'head.db')
            if os.path.exists(headdb):
                self._speak(
                    "head.db Exists, Moving to replace {0}.db".format(localnode))
                shutil.copy(headdb, nodedb)
                self._speak(
                    "Copied Head db Starting Arakoon Node {0}".format(localnode))
            else:
                self._speak("head.db Does Not Exist".format(localnode))

        if failover:
            tlogfiles = [f for f in os.listdir(tlogdir) if os.path.isfile(f)]
            if tlogfiles:
                self._speak(
                    "Moving tlog files to temporary directory {0}".format(budir))
                for tlogfile in tlogfiles:
                    os.rename(tlogfile, budir)
            else:
                self._speak("Tlog files do not exist")

    def _startreturnstatus(self, localnode, cluster):
        """ start one local node get and return status """

        if cluster._getStatusOne(localnode) == 'running':
            estatus = self._waitandcheck(self._initialwait, localnode, cluster)
        else:
            cluster._startOne(localnode, True)
            estatus = self._waitandcheck(self._initialwait, localnode, cluster)
        return estatus

    def _catchuparakoondb(self, localnode, cluster):
        """ run catchup on this local node """

        # self._speak("Waiting for catchup on node {0}".format(localnode))
        #
        # catchup = cluster.catchupOnly(localnode)
        #!!! ArakoonCluster instance has no attribute 'catchupOnly'
        #
        # self._speak("Waiting for catchup to settle")
        # self._wait(duration=15)
        # if catchup:
        #    return False
        # else:
        return True

    def _failover(self, localnode, cluster):
        """ failover arakoon """

        if not self._isgrid:
            self._speak("Failover cannot be accmoplished without grid env")
            return False

        self._movearakoondb(localnode, cluster, failover=True)

        catchup = self._catchuparakoondb(localnode, cluster)
        if not catchup:
            failmessage = "Failed Catchup on localnode {0} requires manual intervention to start"
            self._speak(failmessage.format(localnode))
            return False

        self._speak(
            "Cluster Failover: Starting Arakoon Node {0}".format(localnode))
        status = self._startreturnstatus(localnode, cluster)
        if status:
            ensurearakoonworks.runtest()
            self._speak("Cluster Failover: {0} {1}".format(localnode, status))
            return True
        else:
            failmessage = "Failed localnode {0} requires manual intervention to start"
            self._speak(failmessage.format(localnode))
            return False

    def _managetlog(self, localnode, cluster):
        """ check tlog dump and mark while truncating along the way """

        failednode = list()
        tlogfile = self._localnodesfiles[localnode]['tlogfile']

        if not self._dumptlog(localnode, cluster):
            if not self._failover(localnode, cluster):
                failednode.append(localnode)
        elif not self._checkmark(localnode, cluster):
            if not self._marktlogfile(localnode, cluster):
                self._truncatetlog(localnode, cluster)
                if not self._dumptlog(localnode, cluster):
                    self._speak(
                        "Dumping tlog {0} failed during mark even after truncate".format(tlogfile))
                    if not self._marktlogfile(localnode, cluster):
                        self._speak(
                            "Marking tlog {0} failed even after truncate".format(tlogfile))
                        if not self._failover(localnode, cluster):
                            failednode.append(localnode)
            if not self._dumptlog(localnode, cluster):
                if not self._failover(localnode, cluster):
                    failednode.append(localnode)
            elif not self._checkmark(localnode, cluster):
                self._speak(
                    "Tlog {0} failed even after marking".format(tlogfile))
                if not self._failover(localnode, cluster):
                    failednode.append(localnode)

        return failednode

    def _startcheckmove(self, localnode, cluster):
        """ moving and removing dbs to get arakoon to start """

        failednode = list()
        finalmessage = self._finalmessage
        self._speak("Initial Starting Arakoon Node {0}".format(localnode))
        status = self._startreturnstatus(localnode, cluster)
        if status:
            self._speak(finalmessage.format(localnode))
        else:
            message = "Node {0} not running"
            self._speak(message.format(localnode))

            self._movearakoondb(localnode, cluster)

            self._speak(
                "Tlog Replay: Catching up on Arakoon Node {0}".format(localnode))
            catchup = self._catchuparakoondb(localnode, cluster)
            if catchup:
                self._managetlog(localnode, cluster)
            else:
                if not self._failover(localnode, cluster):
                    failednode.append(localnode)
                    return failednode

            self._speak(
                "Tlog Replay: Starting Arakoon Node {0}".format(localnode))
            status = self._startreturnstatus(localnode, cluster)
            if status:
                self._speak("Tlog Replay: " + finalmessage.format(localnode))
            else:
                self._movearakoondb(localnode, cluster, headdb=True)
                failednode.extend(self._managetlog(localnode, cluster))
                if failednode:
                    return failednode

                status = self._startreturnstatus(localnode, cluster)
                if status:
                    self._speak("Head Db: " + finalmessage.format(localnode))
                else:
                    failmessage = "Moving DB failed"
                    self._speak(failmessage)

                    if not self._failover(localnode, cluster):
                        failednode.append(localnode)
                        return failednode

    def checksetmarkedtlog(self):
        """
        retrieve information about arakoon and check all tlogs for marking
        mark if marking is needed
        log a message if not
        """
        failednodes = list()
        for clustername in self._clusters:
            failednodes.extend(self.fixtlogs(clustername))

        failednodesset = set(failednodes)
        if failednodesset:
            self._lockfile()
            raise CheckArakoonError(
                "Starting Arakoon Failed on these Nodes:\n * {0}".format("\n * ".join(failednodesset)))

    def fixtlogs(self, clustername):
        """
        fix tlog for a specific cluster
        returns list of nodes for which it was not possible to fix tlogs
        """
        failednodes = list()
        self._speak("Getting Cluster Object for {0}".format(clustername))
        acluster = manager.getCluster(clustername)
        self._gatherlocalnodes(acluster)

        allnodes = acluster.listNodes()
        if len(allnodes) > 2:
            self._isgrid = True
            gridmessage = "Grid environment detected for cluster {0} with nodes {1}"
            self._speak(gridmessage.format(clustername, allnodes))

        for localnode in self._localnodesfiles.iterkeys():
            cluster = self._localnodesfiles[localnode]['cluster']
            initstatus = self._checkarakoonstatus(localnode, cluster)
            if initstatus:
                self._speak(
                    "{0} already running, not checking for marked tlog".format(localnode))
                continue
            else:
                self._gettlogdir(localnode, cluster)
                self._getdbdir(localnode, cluster)
                tlogdir = self._localnodesfiles[localnode]['tlogdir']
                self._speak(
                    "Localnode {1}, Tlogdir: {0}".format(tlogdir, localnode))
                # there can be many .tlog files for each localnode
                # but only the last one is relevant for checking
                tlogfilelist = [os.path.join(tlogdir, f)
                                for f in os.listdir(tlogdir) if os.path.isfile(os.path.join(tlogdir, f)) and f.endswith('.tlog')]
                if not tlogfilelist:
                    failmessage = "Tlogs are missing - now attempting failover"
                    self._speak(failmessage)
                    if self._failover(localnode, cluster):
                        continue

                else:
                    self._gettlogfile(tlogfilelist, localnode)
                    failednodes.extend(self._managetlog(localnode, cluster))
                    nodestart = self._startcheckmove(localnode, cluster)
                    if nodestart:
                        failednodes.extend(nodestart)

            if failednodes:
                for listiterator, node in enumerate(failednodes):
                    for _cluster in self._clusters:
                        acluster = manager.getCluster(_cluster)
                        status = acluster.getStatus()
                        if node in status and status[node]:
                            del failednodes[listiterator]
            cluster._stopOne(localnode)
        return failednodes
