# Copyright 2014 CloudFounders NV
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
ServiceList module
"""
from ovs.dal.datalist import DataList
from ovs.dal.dataobjectlist import DataObjectList
from ovs.dal.hybrids.service import Service
from ovs.dal.helpers import Descriptor


class ServiceList(object):
    """
    This ServiceList class contains various lists regarding to the Service class
    """

    @staticmethod
    def get_services():
        """
        Get all services of all types
        """
        services = DataList({'object': Service,
                             'data': DataList.select.GUIDS,
                             'query': {'type': DataList.where_operator.AND,
                                       'items': []}}).data
        return DataObjectList(services, Service)

    @staticmethod
    def get_by_ip_ports(ip, ports):
        """
        Returns a single Service for the ip/ports. Returns None if no Service was found
        """
        services = DataList({'object': Service,
                             'data': DataList.select.GUIDS,
                             'query': {'type': DataList.where_operator.AND,
                                       'items': [('storagerouter.ip', DataList.operator.EQUALS, ip),
                                                 ('ports', DataList.operator.EQUALS, ports)]}}).data
        if len(services) == 1:
            return Descriptor(Service, services[0]).get_object(True)
        return None

    @staticmethod
    def get_service_ports_in_use():
        """
        Returns a list of ports in use by all registered servcies. Returns empty list if no Service was found
        """
        services = DataList({'object': Service,
                             'data': DataList.select.GUIDS,
                             'query': {'type': DataList.where_operator.AND,
                                       'items': []}}).data
        services = DataObjectList(services, Service)
        ports_in_use = set()
        for service in services:
            for port in service.ports:
                ports_in_use.add(int(port))

        result = list(ports_in_use)
        result.sort()

        return result
