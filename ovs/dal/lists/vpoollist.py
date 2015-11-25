# Copyright 2014 iNuron NV
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
VPoolList
"""
from ovs.dal.datalist import DataList
from ovs.dal.dataobjectlist import DataObjectList
from ovs.dal.hybrids.vpool import VPool


class VPoolList(object):
    """
    This VPoolList class contains various lists regarding to the VPool class
    """

    @staticmethod
    def get_vpools():
        """
        Returns a list of all VPools
        """
        vpools = DataList({'object': VPool,
                           'data': DataList.select.GUIDS,
                           'query': {'type': DataList.where_operator.AND,
                                     'items': []}}).data
        return DataObjectList(vpools, VPool)

    @staticmethod
    def get_vpool_by_name(vpool_name):
        """
        Returns all VPools which have a given name
        """
        vpools = DataList({'object': VPool,
                           'data': DataList.select.GUIDS,
                           'query': {'type': DataList.where_operator.AND,
                                     'items': [('name', DataList.operator.EQUALS, vpool_name)]}}).data
        if len(vpools) == 0:
            return None
        if len(vpools) == 1:
            return DataObjectList(vpools, VPool)[0]
        else:
            raise RuntimeError('Only one vPool with name {0} should exist.'.format(vpool_name))
