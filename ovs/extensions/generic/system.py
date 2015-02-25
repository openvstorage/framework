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
from subprocess import check_output
from StringIO import StringIO

from ovs.log.logHandler import LogHandler
logger = LogHandler('lib', name='system')


class System(object):
    """
    Generic helper class
    """

    OVS_CONFIG = '/opt/OpenvStorage/config/ovs.cfg'

    my_machine_id = ''
    my_storagerouter_guid = ''
    my_storagedriver_id = ''

    def __init__(self):
        """
        Dummy init method
        """
        _ = self

    @staticmethod
    def get_my_ips(client=None):
        """
        Returns configured ip addresses for this host
        """

        cmd = "ip a | grep 'inet ' | sed 's/\s\s*/ /g' | cut -d ' ' -f 3 | cut -d '/' -f 1"
        output = System.run(cmd, client)
        my_ips = output.split('\n')
        my_ips = [found_ip.strip() for found_ip in my_ips if found_ip.strip() != '127.0.0.1']

        return my_ips

    @staticmethod
    def get_my_machine_id(client=None):
        """
        Returns unique machine id based on mac address
        """
        if not System.my_machine_id or client:
            cmd = """ip a | grep link/ether | sed 's/\s\s*/ /g' | cut -d ' ' -f 3 | sed 's/://g' | sort"""
            output = System.run(cmd, client)
            for mac in output.split('\n'):
                if mac.strip() != '000000000000':
                    if client:
                        return mac.strip()
                    else:
                        System.my_machine_id = mac.strip()
                        break
        return System.my_machine_id

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
    def get_my_storagedriver_id(vpool_name):
        """
        Returns unique machine storagedriver_id based on vpool_name and machineid
        """
        return vpool_name + System.get_my_machine_id()

    @staticmethod
    def get_storagedriver(vpool_name):
        """
        Returns storagedriver object based on vpool_name
        """
        my_storagedriver_id = System.get_my_storagedriver_id(vpool_name)
        my_storagerouter = System.get_my_storagerouter()
        for storagedriver in my_storagerouter.storagedrivers:
            if storagedriver.name == my_storagedriver_id:
                return storagedriver
        raise ValueError('No storagedriver found for vpool_name: {0}'.format(vpool_name))

    @staticmethod
    def update_hosts_file(hostname, ip):
        """
        Update/add entry for hostname ip in /etc/hosts
        """
        import re

        with open('/etc/hosts', 'r') as hosts_file:
            contents = hosts_file.read()

        if isinstance(hostname, list):
            hostnames = ' '.join(hostname)
        else:
            hostnames = hostname

        result = re.search('^{0}\s.*\n'.format(ip), contents, re.MULTILINE)
        if result:
            contents = contents.replace(result.group(0), '{0} {1}\n'.format(ip, hostnames))
        else:
            contents += '{0} {1}\n'.format(ip, hostnames)

        with open('/etc/hosts', 'wb') as hosts_file:
            hosts_file.write(contents)

    @staticmethod
    def exec_remote_python(client, script):
        """
        Executes a python script on a client
        """
        return client.run('python -c """{0}"""'.format(script))

    @staticmethod
    def read_remote_config(client, key):
        """
        Reads remote configuration key
        """
        read = """
from ovs.plugin.provider.configuration import Configuration
print Configuration.get('{0}')
""".format(key)
        return System.exec_remote_python(client, read)

    @staticmethod
    def ports_in_use(client=None):
        """
        Returns the ports in use
        """
        cmd = """netstat -ln4 | sed 1,2d | sed 's/\s\s*/ /g' | cut -d ' ' -f 4 | cut -d ':' -f 2"""
        output = System.run(cmd, client)
        for found_port in output.split('\n'):
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
        output = System.run(cmd, client)
        start_end = list(output.split())
        ephemeral_port_range = xrange(int(min(start_end)), int(max(start_end)))

        for possible_free_port in requested_range:
            if possible_free_port not in ephemeral_port_range and possible_free_port not in exclude_list:
                free_ports.append(possible_free_port)
            if len(free_ports) == nr:
                return free_ports
        raise ValueError('Unable to find requested nr of free ports')

    @staticmethod
    def run(cmd, client=None):
        if client is None:
            output = check_output(cmd, shell=True).strip()
        else:
            output = client.run(cmd).strip()
        return output

    @staticmethod
    def get_arakoon_cluster_names(client=None, arakoon_config_dir=None):
        """
        :param client: optional remote client
        :param arakoon_config_dir: default /opt/OpenvStorage/config/arakoon for ovs
        :return: list of configured arakoon cluster names on this client
        """

        if arakoon_config_dir is None:
            arakoon_config_dir = '/opt/OpenvStorage/config/arakoon'

        cmd = """ls {0} """.format(arakoon_config_dir)
        output = System.run(cmd, client)
        return list(output.split())

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

    @staticmethod
    def read_ovs_config():
        return System.read_config(System.OVS_CONFIG)
