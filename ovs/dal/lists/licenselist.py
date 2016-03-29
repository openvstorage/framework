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
LicenseList module
"""
from ovs.dal.datalist import DataList
from ovs.dal.hybrids.license import License


class LicenseList(object):
    """
    This LicenseList class contains various lists regarding to the License class
    """

    @staticmethod
    def get_by_component(component, return_as_list=False):
        """
        Returns a single License for the given name. Returns None if no license was found
        """
        licenses = DataList(License, {'type': DataList.where_operator.AND,
                                      'items': [('component', DataList.operator.EQUALS, component)]})
        if return_as_list is True:
            return licenses
        if len(licenses) == 1:
            return licenses[0]
        return None

    @staticmethod
    def get_licenses():
        """
        Returns a list of all Licenses
        """
        return DataList(License, {'type': DataList.where_operator.AND,
                                  'items': []})
