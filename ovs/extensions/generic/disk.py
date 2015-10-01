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

from subprocess import check_output


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
