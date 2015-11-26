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
LicenseList module
"""
from ovs.dal.datalist import DataList
from ovs.dal.dataobject import DataObjectList
from ovs.dal.hybrids.license import License
from ovs.dal.helpers import Descriptor


class LicenseList(object):
    """
    This LicenseList class contains various lists regarding to the License class
    """

    @staticmethod
    def get_by_component(component, return_as_list=False):
        """
        Returns a single License for the given name. Returns None if no license was found
        """
        # pylint: disable=line-too-long
        licenses = DataList({'object': License,
                             'data': DataList.select.GUIDS,
                             'query': {'type': DataList.where_operator.AND,
                                       'items': [('component', DataList.operator.EQUALS, component)]}}).data  # noqa
        # pylint: enable=line-too-long
        if return_as_list is True:
            return DataObjectList(licenses, License)
        if len(licenses) == 1:
            return Descriptor(License, licenses[0]).get_object(True)
        return None

    @staticmethod
    def get_licenses():
        """
        Returns a list of all Licenses
        """
        licenses = DataList({'object': License,
                             'data': DataList.select.GUIDS,
                             'query': {'type': DataList.where_operator.AND,
                                       'items': []}}).data
        return DataObjectList(licenses, License)
