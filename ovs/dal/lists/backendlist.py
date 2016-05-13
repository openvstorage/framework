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
BackendList module
"""
from ovs.dal.datalist import DataList
from ovs.dal.hybrids.backend import Backend


class BackendList(object):
    """
    This BackendList class contains various lists regarding to the Backend class
    """

    @staticmethod
    def get_backends():
        """
        Returns a list of all Backends
        """
        return DataList(Backend, {'type': DataList.where_operator.AND,
                                  'items': []})

    @staticmethod
    def get_by_name(name):
        backends = DataList(Backend, {'type': DataList.where_operator.AND,
                                      'items': [('name', DataList.operator.EQUALS, name)]})
        if len(backends) > 1:
            raise RuntimeError('Invalid amount of Backends found: {0}'.format(len(backends)))
        if len(backends) == 0:
            return None
        return backends[0]
