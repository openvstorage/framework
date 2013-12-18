# license see http://www.openvstorage.com/licenses/opensource/

import sys
import time
from ovs.extensions.storage.persistentfactory import PersistentFactory
from JumpScale import j


class EnsureArakoonError(Exception):
    def __init__(self, message):
        self.message = message

    def __speak__(self):
        print "{0}".format(self.message)

class EnsureArakoonWorks():
    """
    Wait for the following operation to be possible:
    1) Set a value
    2) Get a value
    3) Delete a value
    """

    def __init__(self):
        self._works = False
        self._getepoch = j.base.time.getTimeEpoch
        self._begintime = self._getepoch()
        self._waitlimit = 1800
        self._client = PersistentFactory.get_client('arakoon')
        self._key = 'ensureworks'
        self._valueguid = j.base.idgenerator.generateGUID()
        self._value = "{0}{1}"

    def _speak(self, message):
        """ log to standard output """

        leader = "[arakoon_check]:"
        logmessage = "{0} {1}".format(leader, message)
        j.logger.log(logmessage, 1)
        sys.stdout.flush()

    def _set(self):
        """
        set the test key value with the arakoon client

        This operation will block until a value can be set
        During this time the startup.log will recieve arakoon logs that look like this:

            3 starting 08:57:48: [root] Node 'node_0050560A0001' does not know who the master is
            2 starting 08:57:48: [root] Could not determine master.
            3 starting 08:57:48: [root] Master not found (Could not determine the Arakoon master node). Retrying in 0.00 sec.
            [...]
            3 starting 08:57:50: [root] Node 'node_0050560A0001' does not know who the master is
            2 starting 08:57:50: [root] Could not determine master.
            3 starting 08:57:50: [root] Master not found (Could not determine the Arakoon master node). Retrying in 1.00 sec.
        """

        epoch = self._getepoch()
        testvalue = self._value.format(self._valueguid, epoch)
        self._client.set(self._key, testvalue)
        return testvalue

    def _get(self):
        """ get the test key value with the arakoon client """

        retrieved = self._client.get(self._key)
        self._retrieved = retrieved

    def _delete(self):
        """ set the test key value with the arakoon client """

        self._client.delete(self._key)

    def runtest(self):
        """ run the methods in order and wait """

        time.sleep(1)
        testvalue = self._set()
        self._get()
        if self._retrieved == testvalue:
            self._speak("Arakoon value sucessfully set and retrieved")
            self._works = True
        self._delete()
        self._speak("set {0}".format(testvalue))
        self._speak("get {0}".format(self._retrieved))

    def _setlockfile(self):
        """ set a lock file to indicate arakoon is not running """

        jp = j.system.fs.joinPaths
        lockfilename = jp(j.dirs.varDir, "startupfailurelock")
        j.system.fs.createEmptyFile(lockfilename)

    def checktestresults(self):
        """ loop and wait for arakoon to work """

        self._speak("Testing arakoon for availability")
        runstart = self._getepoch()
        while True:
            works = self._works
            if works != True:
                try:
                    self.runtest()
                except:
                    notreadyerror = "running arakoon test failed - still trying"
                    self._speak(notreadyerror)

                elapsedtime = runstart - self._begintime
                waitmsg = "Waiting for arakoon.. {0} seconds passed"
                self._speak(waitmsg.format(elapsedtime))
                runstart = self._getepoch()

                if elapsedtime >= self._waitlimit:
                    timeoutmessage = "Arakoon wait time exceeded: {0} seconds"
                    message = timeoutmessage.format(elapsedtime)
                    self._setlockfile()
                    raise RuntimeError(message)

            else:
                waitseconds = 15
                self._speak("Arakoon is now ready")

                arakoonbudirs = list()
                for arakoonbudir in j.system.fs.listDirsInDir(j.dirs.tmpDir):
                    if 'arakoonbu_' in arakoonbudir:
                        arakoonbudirs.append(arakoonbudir)

                if len(arakoonbudirs) > 0:
                    arakoonbudirs.sort()
                    latesttokeep = arakoonbudirs.pop()
                    self._speak("Waiting {0} seconds and then removing any extraneous backup directories".format(waitseconds))
                    time.sleep(waitseconds)
                    for arakoonbudir in arakoonbudirs:
                        j.system.fs.removeDirTree(arakoonbudir)
                        self._speak("Arakoon startup backup dir removed: {0}".format(arakoonbudir))
                    self._speak("Kept last backup of {0}".format(latesttokeep))
                break
