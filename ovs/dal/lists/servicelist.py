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
