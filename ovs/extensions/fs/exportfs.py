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

import re
import subprocess
from ovs.log.logHandler import LogHandler

logger = LogHandler('extensions', name='exportfs')


class Nfsexports(object):
    """
    Basic management for /etc/exports
    """
    def __init__(self):
        self._exportsFile = '/etc/exports'
        self._cmd = ['/usr/bin/sudo', '-u', 'root', '/usr/sbin/exportfs']
        self._restart = ['/usr/bin/sudo', '-u', 'root', '/etc/init.d/nfs-kernel-server', 'restart']

    def _slurp(self):
        """
        Read from /etc/exports
        """
        f = open(self._exportsFile, 'r')
        dlist = []
        for line in f:
            if not re.match('^\s*$', line):
                dlist.append(line)
        f.close()
        dlist = [i.strip() for i in dlist if not i.startswith('#')]
        dlist = [re.split('\s+|\(|\)', i) for i in dlist]
        keys = ['dir', 'network', 'params']
        ldict = [dict(zip(keys, line)) for line in dlist]

        return ldict

    def add(self, directory, network, params):
        """
        Add entry to /etc/exports

        @param directory: directory to export
        @param network: network range allowed
        @param params: params for export (eg, 'ro,async,no_root_squash,no_subtree_check')
        """
        l = self._slurp()
        for i in l:
            if i['dir'] == directory:
                logger.info('Directory already exported, to export with different params please first remove')
                return
        f = open(self._exportsFile, 'a')
        f.write('%s %s(%s)\n' % (directory, network, params))
        f.close()

    def remove(self, directory):
        """
        Remove entry from /etc/exports
        """
        l = self._slurp()
        for i in l:
            if i['dir'] == directory:
                l.remove(i)
                f = open(self._exportsFile, 'w')
                for i in l:
                    f.write("%s %s(%s) \n" % (i['dir'], i['network'], i['params']))
                f.close()
                return

    def list_exported(self):
        """
        List the current exported filesystems
        """
        exports = {}
        output = subprocess.check_output(self._cmd)
        for export in re.finditer('(\S+?)[\s\n]+(\S+)\n?', output):
            exports[export.group(1)] = export.group(2)
        return exports

    def unexport(self, directory):
        """
        Unexport a filesystem
        """
        cmd = list(self._cmd)
        exports = self.list_exported()
        if not directory in exports.keys():
            logger.info('Directory %s currently not exported' % directory)
            return
            logger.info('Unexporting {}:{}'.format(exports[directory] if exports[directory] != '<world>' else '*', directory))
        cmd.extend(['-u', '{}:{}'.format(exports[directory] if exports[directory] != '<world>' else '*', directory)])
        subprocess.call(cmd)

    def export(self, directory, network='*'):
        """
        Export a filesystem
        """
        cmd = list(self._cmd)
        exports = self.list_exported()
        if directory in exports.keys():
            logger.info('Directory already exported with options %s' % exports[directory])
            return
        logger.info('Exporting {}:{}'.format(network, directory))
        cmd.extend(['-v', '{}:{}'.format(network, directory)])
        subprocess.call(cmd)
        subprocess.call(self._restart)
