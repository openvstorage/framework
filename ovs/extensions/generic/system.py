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
Generic system module, executing statements on local node
"""

import os
import re
import time
import uuid
from ConfigParser import RawConfigParser
from StringIO import StringIO
from subprocess import check_output


class System(object):
    """
    Generic helper class
    """

    OVS_ID_FILE = '/etc/openvstorage_id'
    _machine_id = {}

    def __init__(self):
        """
        Dummy init method
        """
        raise RuntimeError('System is a static class')

    @staticmethod
    def get_my_machine_id(client=None):
        """
        Returns unique machine id, generated at install time.
        :param client: Remote client on which to retrieve the machine ID
        :type client: SSHClient

        :return: Machine ID
        :rtype: str
        """
        if os.environ.get('RUNNING_UNITTESTS') == 'True':
            return System._machine_id.get('none' if client is None else client.ip)
        if client is not None:
            return client.run(['cat', System.OVS_ID_FILE]).strip()
        with open(System.OVS_ID_FILE, 'r') as the_file:
            return the_file.read().strip()

    @staticmethod
    def get_my_storagerouter():
        """
        Returns unique machine storagerouter id
        :return: Storage Router this is executed on
        :rtype: StorageRouter
        """
        from ovs.dal.lists.storagerouterlist import StorageRouterList
        storagerouter = StorageRouterList.get_by_machine_id(System.get_my_machine_id())
        if storagerouter is None:
            raise RuntimeError('Could not find the local StorageRouter')
        return storagerouter

    @staticmethod
    def update_hosts_file(ip_hostname_map, client):
        """
        Update/add entry for hostname ip in /etc/hosts
        :param ip_hostname_map: Mapping between IPs and their host names
        :type ip_hostname_map: dict

        :param client: Remote client on which to update the hosts file
        :type client: SSHClient

        :return: None
        """
        for ip, hostnames in ip_hostname_map.iteritems():
            if re.match(r'^localhost$|^127(?:\.[0-9]{1,3}){3}$|^::1$', ip):
                # Never update loopback addresses
                continue

            contents = client.file_read('/etc/hosts').strip() + '\n'

            if isinstance(hostnames, list):
                hostnames = ' '.join(hostnames)

            result = re.search('^{0}\s.*\n'.format(ip), contents, re.MULTILINE)
            if result:
                if hostnames not in result.groups(0):
                    contents = contents.replace(result.group(0), '{0} {1}\n'.format(ip, hostnames))
            else:
                contents += '{0} {1}\n'.format(ip, hostnames)

            client.file_write('/etc/hosts', contents, mode='wb')

    @staticmethod
    def ports_in_use(client=None):
        """
        Returns the ports in use
        :param client: Remote client on which to retrieve the ports in use
        :type client: SSHClient

        :return: Ports in use
        :rtype: list
        """
        cmd = "netstat -ln | sed 1,2d | sed 's/\s\s*/ /g' | cut -d ' ' -f 4 | cut -d ':' -f 2"
        if client is None:
            output = check_output(cmd, shell=True)
        else:
            output = client.run(cmd, allow_insecure=True)
        for found_port in output.splitlines():
            if found_port.isdigit():
                yield int(found_port.strip())

    @staticmethod
    def get_free_ports(selected_range, exclude=None, nr=1, client=None):
        """
        Return requested nr of free ports not currently in use and not within excluded range
        :param selected_range: e.g. '2000-2010' or '50000-6000, 8000-8999' ; note single port extends to [port -> 65535]
        :type selected_range: list

        :param exclude: excluded list
        :type exclude: list

        :param nr: nr of free ports requested
        :type nr: int

        :param client: SSHClient to node
        :type client: SSHClient

        :return: sorted incrementing list of nr of free ports
        :rtype: list
        """

        requested_range = []
        for port_range in selected_range:
            if isinstance(port_range, list):
                current_range = [port_range[0], port_range[1]]
            else:
                current_range = [port_range, 65535]
            if 0 <= current_range[0] <= 1024:
                current_range = [1025, current_range[1]]
            requested_range += range(current_range[0], current_range[1] + 1)

        free_ports = []
        if exclude is None:
            exclude = []
        exclude_list = list(exclude)

        ports_in_use = System.ports_in_use(client)
        exclude_list += ports_in_use

        cmd = 'cat /proc/sys/net/ipv4/ip_local_port_range'
        if client is None:
            output = check_output(cmd, shell=True)
        else:
            output = client.run(cmd.split(' '))
        start_end = map(int, output.split())
        ephemeral_port_range = xrange(min(start_end), max(start_end))

        for possible_free_port in requested_range:
            if possible_free_port not in ephemeral_port_range and possible_free_port not in exclude_list:
                free_ports.append(possible_free_port)
            if len(free_ports) == nr:
                return free_ports
        raise ValueError('Unable to find requested nr of free ports')

    @staticmethod
    def read_config(filename, client=None):
        """
        Read a configuration file
        :param filename: File to read
        :type filename: str

        :param client: SSHClient to node
        :type client: SSHClient

        :return: Content of configuration file
        :rtype: RawConfigParser
        """
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
        """
        Write a configuration file
        :param config: Contents to write
        :type config: RawConfigParser

        :param filename: File to write
        :type filename: str

        :param client: SSHClient to node
        :type client: SSHClient

        :return: None
        """
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
