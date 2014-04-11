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
from ovs.log.logHandler import LogHandler

logger = LogHandler('ovs.extensions', name='fstab')


class Fstab(object):
    """
    /etc/fstab manager
    """
    def __init__(self):
        """
        Init
        """
        self.fstab_file = '/etc/fstab'

    def _slurp(self):
        """
        Read from /etc/fstab
        """
        f = open(self.fstab_file, 'r')
        dlist = []
        for line in f:
            if not re.match('^\s*$', line):
                dlist.append(line)
        f.close()
        dlist = [i.strip() for i in dlist if not i.startswith('#')]
        dlist = [re.split(' +|\t+', i) for i in dlist]
        keys = ['device', 'directory', 'fstype', 'options', 'dump', 'fsck']
        ldict = [dict(zip(keys, line)) for line in dlist]

        return ldict

    def show_config(self):
        """
        Print the content of /etc/fstab
        """
        l = self._slurp()
        for i in l:
            logger.debug("%s %s %s %s %s %s" % (i['device'], i['directory'], i['fstype'], i['options'], i['dump'], i['fsck']))

    def add_config(self, fs_spec, fs_file, fs_vfstype, fs_mntops='defaults', fs_freq='0', fs_passno='0'):
        """
        Add an entry to /etc/fstab

        @param fs_spec: device
        @param fs_file: directory or mount point
        @param fs_vfstype: Type of filesystem
        @param fs_mntops: options
        @param fs_freq: dump value
        @param fs_passno: fsck value
        """
        logger.debug(
            "/etc/fstab: appending entry %s %s %s %s %s %s to %s" % \
            (fs_spec, fs_file, fs_vfstype, fs_mntops, fs_freq, fs_passno, self.fstab_file)
        )
        f = open(self.fstab_file, 'a')
        f.write('%s %s %s %s %s %s\n' % (fs_spec, fs_file, fs_vfstype, fs_mntops, fs_freq, fs_passno))
        f.close()

    def modify_config_by_device(self, device, fs_file = '', fs_vfstype = '', fs_mntops='', fs_freq='', fs_passno = ''):
        """
        Modify an entry to /etc/fstab

        @param device: device
        @param fs_file: directory or mount point
        @param fs_vfstype: Type of filesystem
        @param fs_mntops: options
        @param fs_freq: dump value
        @param fs_passno: fsck value
        """
        logger.debug(
            "%s: modifying entry %s to %s %s %s %s %s to %s" % \
            (self.fstab_file, device, fs_file, fs_vfstype, fs_mntops, fs_freq, fs_passno, self.fstab_file)
        )

        def x_if_x_else_key(x, dictionary, key):
            """ Small helper function """
            return x if x else dictionary[key]

        l = self._slurp()
        f = open(self.fstab_file, 'w')
        for i in l:
            if i['device'] == device:
                new_fs_file = x_if_x_else_key(fs_file, i, 'directory')
                new_fs_vfstype = x_if_x_else_key(fs_vfstype, i, 'fstype')
                new_fs_mntops = x_if_x_else_key(fs_mntops, i, 'options')
                new_fs_freq = x_if_x_else_key(fs_freq, i, 'dump')
                new_fs_passno = x_if_x_else_key(fs_passno, i, 'fsck')

                f.write('%s %s %s %s %s %s\n' %
                        (device, new_fs_file, new_fs_vfstype, new_fs_mntops, new_fs_freq, new_fs_passno))
            else:
                f.write("%s %s %s %s %s %s\n" %
                        (i['device'], i['directory'], i['fstype'], i['options'], i['dump'], i['fsck']))
        f.close()

    def remove_config_by_device(self, device):
        """
        Remove an entry from /etc/fstab based on the device
        """
        return self._remove_config_by_('device', device)

    def remove_config_by_directory(self, directory):
        """
        Removes an entry from /etc/fstab based on directory
        """
        return self._remove_config_by_('directory', directory)

    def _remove_config_by_(self, match_type, match_value):
        """
        Remove a line from /etc/fstab
        """
        lines = self._slurp()
        line_removed = False
        for line in lines:
            if line[match_type] == match_value:
                lines.remove(line)
                line_removed = True
        if line_removed:
            with open(self.fstab_file, 'w') as fstab_file:
                for line in lines:
                    fstab_file.write("%s %s %s %s %s %s\n" %
                                     (line['device'], line['directory'], line['fstype'], line['options'], line['dump'], line['fsck']))
        else:
            logger.debug("%s: no such entry %s found" % (self.fstab_file, match_value))
