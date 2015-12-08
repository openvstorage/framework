# Copyright 2015 iNuron NV
#
# Licensed under the Open vStorage Modified Apache License (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.openvstorage.org/license
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
    def create_partition(disk_path, disk_size, partition_start, partition_size):
        """
        Creates a partition
        :param disk_path:       Path of disk device
        :type disk_path:        str

        :param disk_size:       Total size of disk
        :type disk_size:        int

        :param partition_start: Start of partition in bytes
        :type partition_start:  int

        :param partition_size:  End of partition in bytes
        :type partition_size:   int

        :return:                None
        """
        # 1. Verify current label type and add GPT label if none present
        label_type = 'gpt'
        try:
            command_1 = 'parted {0} print | grep "Partition Table"'.format(disk_path)
            logger.info('Checking partition label-type with command: {0}'.format(command_1))
            label_type = check_output(command_1, shell=True).strip().split(': ')[1]
        except CalledProcessError:
            try:
                logger.info('Adding GPT label and trying to create partition again')
                check_output('parted {0} -s mklabel gpt'.format(disk_path), shell=True)
            except Exception as ex:
                logger.exception('Error during label creation: {0}'.format(ex))
                raise

        # 2. Determine command to use based upon label type
        start = int(round(float(partition_start) / disk_size * 100))
        end = int(round(float(partition_size) / disk_size * 100)) + start
        if end > 100:
            end = 100

        if label_type == 'gpt':
            command_2 = 'parted {0} -a optimal -s mkpart {1} {2}% {3}%'.format(disk_path, disk_path.split('/')[-1], start, end)
        elif label_type == 'msdos':
            command_2 = 'parted {0} -a optimal -s mkpart primary ext4 {1}% {2}%'.format(disk_path, start, end)
        elif label_type == 'bsd':
            command_2 = 'parted {0} -a optimal -s mkpart ext4 {1}% {2}%'.format(disk_path, start, end)
        else:
            raise ValueError('Unsupported label-type detected: {0}'.format(label_type))

        # 3. Create partition
        logger.info('Label type detected: {0}'.format(label_type))
        logger.info('Command to create partition: {0}'.format(command_2))
        check_output(command_2, shell=True)

    @staticmethod
    def make_fs(partition):
        """
        Creates a filesystem
        :param partition: Path of partition
        :type partition:  str

        :return:          None
        """
        try:
            check_output('mkfs.ext4 -q {0}'.format(partition), shell=True)
        except Exception as ex:
            logger.exception('Error during filesystem creation: {0}'.format(ex))
            raise

    @staticmethod
    def add_fstab(device, mountpoint, filesystem):
        """
        Add entry to /etc/fstab for mountpoint
        :param device:     Device to add
        :type device:      str

        :param mountpoint: Mountpoint on which device is mounted
        :type mountpoint:  str

        :param filesystem: Filesystem used
        :type filesystem:  str

        :return:           None
        """
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
        """
        Verify whether a mountpoint exists by browsing /etc/fstab
        :param mountpoint: Mountpoint to check
        :type mountpoint:  str

        :return:           True if mountpoint exists, False otherwise
        :rtype:            bool
        """
        with open('/etc/fstab', 'r') as fstab_file:
            for line in fstab_file.readlines():
                if re.search('\s+{0}\s+'.format(re.escape(mountpoint)), line):
                    return True
        return False

    @staticmethod
    def mount(mountpoint):
        """
        Mount a partition
        :param mountpoint: Mountpoint on which to mount the partition
        :type mountpoint:  str

        :return:           None
        """
        try:
            check_output('mkdir -p {0}'.format(mountpoint), shell=True)
            check_output('mount {0}'.format(mountpoint), shell=True)
        except Exception as ex:
            logger.exception('Error during mount: {0}'.format(ex))
            raise
