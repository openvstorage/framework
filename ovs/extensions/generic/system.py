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

from subprocess import check_output


class System(object):
    """
    Generic helper class
    """

    my_machine_id = ''
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
        Returns unique machine id based on mac address
        """
        if not System.my_machine_id:
            cmd = """ip a | grep link/ether | sed 's/\s\s*/ /g' | cut -d ' ' -f 3 | sed 's/://g' | sort"""
            if client is None:
                output = check_output(cmd, shell=True).strip()
            else:
                output = client.run(cmd).strip()
            for mac in output.split('\n'):
                if mac.strip() != '000000000000':
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
