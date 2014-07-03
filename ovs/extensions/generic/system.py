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

import subprocess


class Ovs():

    my_machine_id = ''
    my_storagerouter_guid = ''
    my_vsr_id = ''

    @staticmethod
    def execute_command(cmd, catch_output=True):
        """
        Executes a command
        """
        if catch_output:
            process = subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            output = process.stdout.readlines()
            process.wait()
            return process.returncode, output
        else:
            process = subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
            output = process.communicate()
            return process.returncode, output

    @staticmethod
    def get_my_machine_id():
        """
        Returns unique machine id based on mac address
        """
        if not Ovs.my_machine_id:
            cmd = """ip a | grep link/ether | sed 's/\s\s*/ /g' | cut -d ' ' -f 3 | sed 's/://g' | sort"""
            rcode, output = Ovs.execute_command(cmd, catch_output=False)
            for mac in output[0].strip().split('\n'):
                if mac.strip() != '000000000000':
                    Ovs.my_machine_id = mac.strip()
                    break
        return Ovs.my_machine_id

    @staticmethod
    def get_my_storagerouter():
        """
        Returns unique machine storagerouter id
        """

        from ovs.dal.hybrids.storagerouter import StorageRouter
        from ovs.dal.lists.storagerouterlist import StorageRouterList

        if not Ovs.my_storagerouter_guid:
            for storagerouter in StorageRouterList.get_storagerouters():
                if storagerouter.machineid == Ovs.get_my_machine_id():
                    Ovs.my_storagerouter_guid = storagerouter.guid
        return StorageRouter(Ovs.my_storagerouter_guid)

    @staticmethod
    def get_my_vsr_id(vpool_name):
        """
        Returns unique machine vsrid based on vpool_name and machineid
        """
        return vpool_name + Ovs.get_my_machine_id()

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
