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

from sys import *
import re
import pprint

class Fstab(object):
    """
    /etc/fstab manager
    """
    def __init__(self, fstabFile=None):
        """
        Init
        """
        self.fstabFile = '/etc/fstab'

    def _slurp(self):
        """
        Read from /etc/fstab
        """
        f = open(self.fstabFile, 'r')
        dlist = []
        for line in f:
            if not re.match('^\s*$', line): dlist.append(line)
        f.close()
        dlist = [ i.strip() for i in dlist if not i.startswith('#') ]
        dlist = [ re.split('\ +|\t+',i) for i in dlist ]
        keys=['device','directory','fstype','options','dump','fsck']
        ldict = [ dict(zip(keys,line)) for line in dlist ]

        return ldict

    def showConfig(self):
        """
        Print the content of /etc/fstab
        """
        l = self._slurp()
        for i in l:
            print "%s %s %s %s %s %s" % ( i['device'], i['directory'], i['fstype'], i['options'], i['dump'], i['fsck'] )

    def addConfig(self, fs_spec, fs_file, fs_vfstype, fs_mntops='defaults', fs_freq='0', fs_passno='0'):
        """
        Add an entry to /etc/fstab

        @param fs_spec: device
        @param fs_file: directory or mount point
        @param fs_vfstype: Type of filesystem
        @param fs_mntops: options
        @param fs_freq: dump value
        @param fs_passno: fsck value
        """
        print "/etc/fstab: appending entry %s %s %s %s %s %s to %s" % (fs_spec, fs_file, fs_vfstype, fs_mntops, fs_freq, fs_passno, self.fstabFile)
        f = open(self.fstabFile, 'a')
        f.write('%s %s %s %s %s %s\n' % (fs_spec, fs_file, fs_vfstype, fs_mntops, fs_freq, fs_passno))
        f.close()


    def modifyConfigByDevice(self, device, fs_file = '', fs_vfstype = '', fs_mntops='', fs_freq='', fs_passno = ''):
        """
        Modify an entry to /etc/fstab

        @param fs_spec: device
        @param fs_file: directory or mount point
        @param fs_vfstype: Type of filesystem
        @param fs_mntops: options
        @param fs_freq: dump value
        @param fs_passno: fsck value
        """
        print "%s: modifying entry %s to %s %s %s %s %s to %s" % (self.fstabFile, device, fs_file, fs_vfstype, fs_mntops, fs_freq, fs_passno, self.fstabFile)

        l = self._slurp()
        f = open(self.fstabFile, 'w')
        for i in l:
            if i['device'] == device:
                new_fs_file    = fs_file    if fs_file    else i['directory']
                new_fs_vfstype = fs_vfstype if fs_vfstype else i['fstype']
                new_fs_mntops  = fs_mntops  if fs_mntops  else i['options']
                new_fs_freq    = fs_freq    if fs_freq    else i['dump']
                new_fs_passno  = fs_passno  if fs_passno  else i['fsck']

                f.write('%s %s %s %s %s %s\n' % (device, new_fs_file, new_fs_vfstype, new_fs_mntops, new_fs_freq, new_fs_passno))
            else:
                f.write("%s %s %s %s %s %s\n" % ( i['device'], i['directory'], i['fstype'], i['options'], i['dump'], i['fsck'] ))
        f.close()


    def removeConfigByDevice(self, device):
        """
        Remove an entry from /etc/fstab

        @param device: Device to remove config by
        @param device: string
        """
        l = self._slurp()
        for i in l:
            if i['device'] == device:
                l.remove(i)
                f = open(self.fstabFile, 'w')
                for i in l:
                    f.write("%s %s %s %s %s %s\n" % ( i['device'], i['directory'], i['fstype'], i['options'], i['dump'], i['fsck'] ))
                f.close()
                return


    def removeConfigByDirectory(self, dir):
        """
        Remove an from /etc/fstab
        """
        l = self._slurp()
        for i in l:
            if i['directory'] == dir:
                l.remove(i)
                f = open(self.fstabFile, 'w')
                for i in l:
                    f.write("%s %s %s %s %s %s\n" % ( i['device'], i['directory'], i['fstype'], i['options'], i['dump'], i['fsck'] ))
                f.close()
                return
        print "%s: no such entry %s found" % (self.fstabFile, dir)
