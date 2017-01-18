# Copyright (C) 2016 iNuron NV
#
# This file is part of Open vStorage Open Source Edition (OSE),
# as available from
#
#      http://www.openvstorage.org and
#      http://www.openvstorage.com.
#
# This file is free software; you can redistribute it and/or modify it
# under the terms of the GNU Affero General Public License v3 (GNU AGPLv3)
# as published by the Free Software Foundation, in version 3 as it comes
# in the LICENSE.txt file of the Open vStorage OSE distribution.
#
# Open vStorage is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY of any kind.

"""
DomainList module
"""
from ovs.dal.datalist import DataList
from ovs.dal.hybrids.domain import Domain


class DomainList(object):
    """
    This DomainList class contains various lists related to the Domain class
    """

    @staticmethod
    def get_domains():
        """
        Returns a list of all domains
        :return: All Domains
        :rtype: ovs.dal.datalist.DataList
        """
        return DataList(Domain, {'type': DataList.where_operator.AND,
                                 'items': []})

    @staticmethod
    def get_by_name(name):
        """
        Returns a list of Domains with a given name
        :param name: Name of the Domain(s) to search
        :type name: str
        :return: List of Domains with a given name
        :rtype: ovs.dal.datalist.DataList
        """
        return DataList(Domain, {'type': DataList.where_operator.AND,
                                 'items': [('name', DataList.operator.EQUALS, name)]})
