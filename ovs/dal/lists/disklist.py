# Copyright 2016 iNuron NV
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
DiskList module
"""
from ovs.dal.datalist import DataList
from ovs.dal.hybrids.disk import Disk


class DiskList(object):
    """
    This DiskList class contains various lists regarding to the Disk class
    """

    @staticmethod
    def get_disks():
        """
        Returns a list of all Disks
        """
        return DataList(Disk, {'type': DataList.where_operator.AND,
                               'items': []})
