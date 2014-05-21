#!/usr/bin/env python
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

import ConfigParser
import os

from subprocess import check_output

SECRET_KEY_LENGTH = 50

ip_list = check_output("ip a | grep link/ether | sed 's/\s\s*/ /g' | cut -d ' ' -f 3 | sed 's/://g'", shell=True)
UNIQUE_ID = sorted(ip_list.strip().split('\n'))[0]


def replace_param_in_config(config_file, old_value, new_value, add=False):
    def file_read(filename):
        with open(filename, 'r') as the_file:
            return the_file.read()

    def file_write(filename, contents):
        with open(filename, 'w') as the_file:
            the_file.write(contents)

    if os.path.isfile(config_file):
        contents = file_read(config_file)
        if new_value in contents and new_value.find(old_value) > 0:
            pass
        elif old_value in contents:
            contents = contents.replace(old_value, new_value)
        else:
            if add:
                contents += new_value + '\n'
        file_write(config_file, contents)


def update_ssh_settings():
    replace_param_in_config('/etc/ssh/sshd_config',
                            'AcceptEnv',
                            '#AcceptEnv')
    replace_param_in_config('/etc/ssh/sshd_config',
                            'UseDNS yes',
                            'UseDNS no',
                            add=True)

    check_output('service ssh restart', shell=True)


def update_ovs_cfg():

    config_filename = '/opt/OpenvStorage/config/ovs.cfg'
    config = ConfigParser.ConfigParser()
    config.read(config_filename)
    config.set('core', 'uniqueid', UNIQUE_ID)
    with open(config_filename, 'wb') as config_file:
        config.write(config_file)


def update_arakoon_upstart():
    replace_param_in_config('/etc/init/ovs-arakoon-ovsdb.conf',
                            '<ARAKOON_NODE_ID>',
                            UNIQUE_ID)
    replace_param_in_config('/etc/init/ovs-arakoon-voldrv.conf',
                            '<ARAKOON_NODE_ID>',
                            UNIQUE_ID)


def configure_coredump():
    replace_param_in_config('/etc/security/limits.conf',
                            '\nroot soft core  unlimited\novs  soft core  unlimited\n',
                            '\nroot soft core  unlimited\novs  soft core  unlimited\n',
                            add=True)


def setup_ssh_key_authentication():
    root_ssh_folder = '{0}/.ssh'.format(check_output('echo ~', shell=True).strip())
    ovs_ssh_folder = '{0}/.ssh'.format(check_output('su - ovs -c "echo ~"', shell=True).strip())
    private_key_filename = '{0}/id_rsa'
    public_key_filename = '{0}/id_rsa.pub'
    authorized_keys_filename = '{0}/authorized_keys'
    known_hosts_filename = '{0}/known_hosts'
    # Generate keys for root
    check_output("ssh-keygen -t rsa -b 4096 -f {0} -N ''".format(private_key_filename.format(root_ssh_folder)), shell=True)
    # Generate keys for ovs
    check_output('su - ovs -c "mkdir -p {0}"'.format(ovs_ssh_folder), shell=True)
    check_output('su - ovs -c "ssh-keygen -t rsa -b 4096 -f {0} -N \'\'"'.format(private_key_filename.format(ovs_ssh_folder)), shell=True)
    root_authorized_keys = authorized_keys_filename.format(root_ssh_folder)
    ovs_authorized_keys = authorized_keys_filename.format(ovs_ssh_folder)
    root_known_hosts = known_hosts_filename.format(root_ssh_folder)
    ovs_known_hosts = known_hosts_filename.format(ovs_ssh_folder)

    for filename in [root_authorized_keys, root_known_hosts]:
        check_output('touch {0}'.format(filename), shell=True)
        check_output('chmod 600 {0}'.format(filename), shell=True)

    for filename in [ovs_authorized_keys, ovs_known_hosts]:
        check_output('su - ovs -c "touch {0}"'.format(filename), shell=True)
        check_output('su - ovs -c "chmod 600 {0}"'.format(filename), shell=True)


update_ssh_settings()
update_ovs_cfg()
update_arakoon_upstart()
configure_coredump()
setup_ssh_key_authentication()
