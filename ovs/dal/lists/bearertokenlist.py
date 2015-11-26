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
BearerTokenList module
"""
from ovs.dal.datalist import DataList
from ovs.dal.dataobject import DataObjectList
from ovs.dal.hybrids.bearertoken import BearerToken


class BearerTokenList(object):
    """
    This BearerTokenList class contains various lists regarding to the BearerToken class
    """

    @staticmethod
    def get_by_access_token(access_token):
        """
        Returns a single BearerToken for the given token. Returns None if no BearerToken was found
        """
        tokens = DataList({'object': BearerToken,
                           'data': DataList.select.GUIDS,
                           'query': {'type': DataList.where_operator.AND,
                                     'items': [('access_token', DataList.operator.EQUALS, access_token)]}}).data
        return DataObjectList(tokens, BearerToken)

    @staticmethod
    def get_by_refresh_token(refresh_token):
        """
        Returns a single BearerToken for the given token. Returns None if no BearerToken was found
        """
        tokens = DataList({'object': BearerToken,
                           'data': DataList.select.GUIDS,
                           'query': {'type': DataList.where_operator.AND,
                                     'items': [('refresh_token', DataList.operator.EQUALS, refresh_token)]}}).data
        return DataObjectList(tokens, BearerToken)
