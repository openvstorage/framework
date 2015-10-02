# Copyright 2015 Open vStorage NV
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
"""
Disk module
"""

import re
from subprocess import check_output
from ovs.extensions.os.os import OSManager


class DiskTools(object):
    """
    This class contains various helper methods wrt Disk maintenance
    """

    @staticmethod
    def create_partition(disk, offset, size):
        """
        Creates a partition
        """
        check_output('parted {0} -s mkpart {0} {1} {2}'.format(disk, offset, size), shell=True)

    @staticmethod
    def make_fs(partition, filesystem='ext4'):
        """
        Creates a filesystem
        """
        if filesystem == 'xfs':
            check_output('mkfs.xfs -qf {0}'.format(partition), shell=True)
        elif filesystem == 'ext4':
            check_output('mkfs.ext4 -q {0}'.format(partition), shell=True)
        else:
            raise RuntimeError('Unsupported filesystem')

    @staticmethod
    def add_fstab(device, mountpoint):
        new_content = []
        with open('/etc/fstab', 'r') as fstab_file:
            lines = [line.strip() for line in fstab_file.readlines()]
        for line in lines:
            if line.startswith(device) and re.match('^{0}\s+'.format(re.escape(device)), line):
                new_content.append(OSManager.get_fstab_entry(device, mountpoint))
            else:
                new_content.append(line)
        with open('/etc/fstab', 'w') as fstab_file:
            fstab_file.write('\n'.join(new_content))

    @staticmethod
    def mountpoint_exists(mountpoint):
        with open('/etc/fstab', 'r') as fstab_file:
            for line in fstab_file.readlines():
                if re.match('\s+{0}\s+'.format(re.escape(mountpoint)), line):
                    return True
        return False

    @staticmethod
    def mount(mountpoint):
        check_output('mkdir -p {0}'.format(mountpoint), shell=True)
        check_output('mount {0}'.format(mountpoint), shell=True)
