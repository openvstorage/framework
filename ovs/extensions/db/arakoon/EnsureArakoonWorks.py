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
import time
import uuid
import os
import shutil
from ovs.extensions.storage.persistentfactory import PersistentFactory
from ovs.log.logHandler import LogHandler

logger = LogHandler('arakoon', name='validator')


class EnsureArakoonError(Exception):

    def __init__(self, message):
        self.message = message

    def __speak__(self):
        print '{0}'.format(self.message)


class EnsureArakoonWorks():
    """
    Wait for the following operation to be possible:
    1) Set a value
    2) Get a value
    3) Delete a value
    """

    def __init__(self):
        self._works = False
        self._getepoch = lambda: int(time.time())
        self._begintime = self._getepoch()
        self._waitlimit = 1800
        self._client = PersistentFactory.get_client('arakoon')
        self._key = 'ensureworks'
        self._valueguid = str(uuid.uuid4())
        self._value = '{0}{1}'

    @staticmethod
    def _speak(message):
        """ log to standard output """

        leader = '[arakoon_check]:'
        logmessage = '{0} {1}'.format(leader, message)
        logger.debug(logmessage)
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
            EnsureArakoonWorks._speak('Arakoon value sucessfully set and retrieved')
            self._works = True
        self._delete()
        EnsureArakoonWorks._speak('set {0}'.format(testvalue))
        EnsureArakoonWorks._speak('get {0}'.format(self._retrieved))

    @staticmethod
    def _setlockfile():
        """ set a lock file to indicate arakoon is not running """

        lockfilename = '/var/startupfailurelock'
        open(lockfilename, 'a').close()

    def checktestresults(self):
        """ loop and wait for arakoon to work """

        EnsureArakoonWorks._speak('Testing arakoon for availability')
        runstart = self._getepoch()
        while True:
            works = self._works
            if works is not True:
                try:
                    self.runtest()
                except:
                    notreadyerror = 'running arakoon test failed - still trying'
                    EnsureArakoonWorks._speak(notreadyerror)

                elapsedtime = runstart - self._begintime
                waitmsg = 'Waiting for arakoon.. {0} seconds passed'
                EnsureArakoonWorks._speak(waitmsg.format(elapsedtime))
                runstart = self._getepoch()

                if elapsedtime >= self._waitlimit:
                    timeoutmessage = 'Arakoon wait time exceeded: {0} seconds'
                    message = timeoutmessage.format(elapsedtime)
                    EnsureArakoonWorks._setlockfile()
                    raise RuntimeError(message)

            else:
                waitseconds = 15
                EnsureArakoonWorks._speak('Arakoon is now ready')

                arakoonbudirs = list()
                for arakoonbudir in os.walk('/tmp').next()[1]:
                    if 'arakoonbu_' in arakoonbudir:
                        arakoonbudirs.append(arakoonbudir)

                if len(arakoonbudirs) > 0:
                    arakoonbudirs.sort()
                    latesttokeep = arakoonbudirs.pop()
                    EnsureArakoonWorks._speak(
                        'Waiting {0} seconds and then removing any extraneous backup directories'.format(waitseconds))
                    time.sleep(waitseconds)
                    for arakoonbudir in arakoonbudirs:
                        shutil.rmtree(arakoonbudir)
                        EnsureArakoonWorks._speak(
                            'Arakoon startup backup dir removed: {0}'.format(arakoonbudir))
                    EnsureArakoonWorks._speak('Kept last backup of {0}'.format(latesttokeep))
                break
