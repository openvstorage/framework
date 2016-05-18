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
ServiceList module
"""
from ovs.dal.datalist import DataList
from ovs.dal.hybrids.service import Service


class ServiceList(object):
    """
    This ServiceList class contains various lists regarding to the Service class
    """

    @staticmethod
    def get_services():
        """
        Get all services of all types
        """
        return DataList(Service, {'type': DataList.where_operator.AND,
                                  'items': []})

    @staticmethod
    def get_by_ip_ports(ip, ports):
        """
        Returns a single Service for the ip/ports. Returns None if no Service was found
        """
        services = DataList(Service, {'type': DataList.where_operator.AND,
                                      'items': [('storagerouter.ip', DataList.operator.EQUALS, ip),
                                                ('ports', DataList.operator.EQUALS, ports)]})
        if len(services) == 1:
            return services[0]
        return None

    @staticmethod
    def get_ports_for_ip(ip):
        """
        Returns a list of ports for all services on a given (StorageRouter) ip
        """
        services = DataList(Service, {'type': DataList.where_operator.AND,
                                      'items': [('storagerouter.ip', DataList.operator.EQUALS, ip)]})
        ports = []
        for service in services:
            ports.extend(service.ports)
        return list(set(ports))
