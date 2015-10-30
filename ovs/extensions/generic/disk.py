# Copyright 2015 iNuron NV
#
# Licensed under the Open vStorage Non-Commercial License, Version 1.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.openvstorage.org/OVS_NON_COMMERCIAL
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
from subprocess import check_output, CalledProcessError
from ovs.extensions.os.os import OSManager
from ovs.log.logHandler import LogHandler

logger = LogHandler.get('extensions', name='disktools')


class DiskTools(object):
    """
    This class contains various helper methods wrt Disk maintenance
    """

    @staticmethod
    def create_partition(disk, offset, size):
        """
        Creates a partition
        """
        try:
            label = disk.split('/')[-1]
            check_output('parted {0} -s mkpart {1} {2}B {3}B'.format(disk, label, offset, offset + size), shell=True)
        except CalledProcessError as ex:
            if 'unrecognised disk label' in ex.output:
                try:
                    check_output('parted {0} -s mklabel gpt'.format(disk), shell=True)
                    label = disk.split('/')[-1]
                    check_output('parted {0} -s mkpart {1} {2}B 100%'.format(disk, label, offset), shell=True)
                except Exception as iex:
                    logger.exception('Error during label/partition creation: {0}'.format(iex))
                    raise
            else:
                logger.exception('Error during partition creation: {0}'.format(ex.output))
                raise

    @staticmethod
    def make_fs(partition):
        """
        Creates a filesystem
        """
        try:
            check_output('mkfs.ext4 -q {0}'.format(partition), shell=True)
        except Exception as ex:
            logger.exception('Error during filesystem creation: {0}'.format(ex))
            raise

    @staticmethod
    def add_fstab(device, mountpoint, filesystem):
        new_content = []
        with open('/etc/fstab', 'r') as fstab_file:
            lines = [line.strip() for line in fstab_file.readlines()]
        found = False
        for line in lines:
            if line.startswith(device) and re.match('^{0}\s+'.format(re.escape(device)), line):
                new_content.append(OSManager.get_fstab_entry(device, mountpoint, filesystem))
                found = True
            else:
                new_content.append(line)
        if found is False:
            new_content.append(OSManager.get_fstab_entry(device, mountpoint, filesystem))
        with open('/etc/fstab', 'w') as fstab_file:
            fstab_file.write('{0}\n'.format('\n'.join(new_content)))

    @staticmethod
    def mountpoint_exists(mountpoint):
        with open('/etc/fstab', 'r') as fstab_file:
            for line in fstab_file.readlines():
                if re.search('\s+{0}\s+'.format(re.escape(mountpoint)), line):
                    return True
        return False

    @staticmethod
    def mount(mountpoint):
        try:
            check_output('mkdir -p {0}'.format(mountpoint), shell=True)
            check_output('mount {0}'.format(mountpoint), shell=True)
        except Exception as ex:
            logger.exception('Error during mount: {0}'.format(ex))
            raise

