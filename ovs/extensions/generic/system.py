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
Generic system module, executing statements on local node
"""

import os
import uuid
import time
from ConfigParser import RawConfigParser
from StringIO import StringIO

from ovs.log.logHandler import LogHandler
logger = LogHandler('lib', name='system')


class System(object):
    """
    Generic helper class
    """

    OVS_CONFIG = '/opt/OpenvStorage/config/ovs.cfg'
    OVS_ID_FILE = '/etc/openvstorage_id'

    my_storagerouter_guid = ''
    my_storagedriver_id = ''

    def __init__(self):
        """
        Dummy init method
        """
        _ = self

    @staticmethod
    def get_my_machine_id(client=None):
        """
        Returns unique machine id, generated at install time.
        """
        if client is not None:
            return client.run('cat {0}'.format(System.OVS_ID_FILE)).strip()
        with open(System.OVS_ID_FILE, 'r') as the_file:
            return the_file.read().strip()

    @staticmethod
    def get_my_storagerouter():
        """
        Returns unique machine storagerouter id
        """

        from ovs.dal.hybrids.storagerouter import StorageRouter
        from ovs.dal.lists.storagerouterlist import StorageRouterList

        if not System.my_storagerouter_guid:
            for storagerouter in StorageRouterList.get_storagerouters():
                if storagerouter.machine_id == System.get_my_machine_id():
                    System.my_storagerouter_guid = storagerouter.guid
        return StorageRouter(System.my_storagerouter_guid)

    @staticmethod
    def update_hosts_file(hostname, ip, client):
        """
        Update/add entry for hostname ip in /etc/hosts
        """
        import re

        contents = client.file_read('/etc/hosts')

        if isinstance(hostname, list):
            hostnames = ' '.join(hostname)
        else:
            hostnames = hostname

        result = re.search('^{0}\s.*\n'.format(ip), contents, re.MULTILINE)
        if result:
            contents = contents.replace(result.group(0), '{0} {1}\n'.format(ip, hostnames))
        else:
            contents += '{0} {1}\n'.format(ip, hostnames)

        client.file_write('/etc/hosts', contents, mode='wb')

    @staticmethod
    def ports_in_use(client):
        """
        Returns the ports in use
        """
        cmd = """netstat -ln4 | sed 1,2d | sed 's/\s\s*/ /g' | cut -d ' ' -f 4 | cut -d ':' -f 2"""
        output = client.run(cmd)
        for found_port in output.splitlines():
            yield int(found_port.strip())

    @staticmethod
    def get_free_ports(selected_range, exclude=None, nr=1, client=None):
        """
        Return requested nr of free ports not currently in use and not within excluded range
        :param selected_range: e.g. '2000-2010' or '50000-6000, 8000-8999' ; note single port extends to [port -> 65535]
        :param exclude: excluded list
        :param nr: nr of free ports requested
        :return: sorted incrementing list of nr of free ports
        """

        requested_range = list()
        selected_range = str(selected_range)
        for port_range in str(selected_range).split(','):
            port_range = port_range.strip()
            if '-' in port_range:
                current_range = (int(port_range.split('-')[0]), int(port_range.split('-')[1]))
            else:
                current_range = (int(port_range), 65535)
            if 0 <= current_range[0] <= 1024:
                current_range = (1025, current_range[1])
            requested_range.extend(xrange(current_range[0], current_range[1] + 1))
        free_ports = list()

        if exclude is None:
            exclude = list()
        exclude_list = list(exclude)

        ports_in_use = System.ports_in_use(client)
        for port in ports_in_use:
            exclude_list.append(port)

        cmd = """cat /proc/sys/net/ipv4/ip_local_port_range"""
        output = client.run(cmd)
        start_end = list(output.split())
        ephemeral_port_range = xrange(int(min(start_end)), int(max(start_end)))

        for possible_free_port in requested_range:
            if possible_free_port not in ephemeral_port_range and possible_free_port not in exclude_list:
                free_ports.append(possible_free_port)
            if len(free_ports) == nr:
                return free_ports
        raise ValueError('Unable to find requested nr of free ports')

    @staticmethod
    def read_config(filename, client=None):
        if client is None:
            cp = RawConfigParser()
            with open(filename, 'r') as config_file:
                cfg = config_file.read()
            cp.readfp(StringIO(cfg))
            return cp
        else:
            contents = client.file_read(filename)
            cp = RawConfigParser()
            cp.readfp(StringIO(contents))
            return cp

    @staticmethod
    def write_config(config, filename, client=None):
        if client is None:
            with open(filename, 'w') as config_file:
                config.write(config_file)
        else:
            temp_filename = '/var/tmp/{0}'.format(str(uuid.uuid4()).replace('-', ''))
            with open(temp_filename, 'w') as config_file:
                config.write(config_file)
            time.sleep(1)
            client.file_upload(filename, temp_filename)
            os.remove(temp_filename)
