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

import os
import re
from subprocess import check_output

SECRET_KEY_LENGTH = 50


def file_read(fn):
    with open(fn, 'r') as the_file:
        return the_file.read()


def file_write(fn, cts):
    with open(fn, 'w') as the_file:
        the_file.write(cts)


config_file = '/etc/ssh/sshd_config'
if os.path.isfile(config_file):
    use_dns = False
    new_contents = []
    contents = file_read(config_file).strip().split('\n')
    for line in contents:
        if 'AcceptEnv' in line:
            new_contents.append('#{0}'.format(line.strip().strip('#').strip()))
        elif 'UseDNS' in line:
            new_contents.append('UseDNS no')
            use_dns = True
        elif line.strip().startswith('Match'):
            if use_dns is False:
                new_contents.append('UseDNS no')
                use_dns = True
            new_contents.append(line)
        else:
            new_contents.append(line)
    if use_dns is False:
        new_contents.append('UseDNS no')
    file_write(config_file, '{0}\n'.format('\n'.join(new_contents)))
check_output('service ssh restart', shell=True)

limits_file = '/etc/security/limits.conf'
if os.path.isfile(limits_file):
    contents = file_read(limits_file).strip()
    if not re.search('\s?root\s+soft\s+core\s+unlimited\s?', contents):
        contents += '\nroot soft core unlimited'
    if not re.search('\s?ovs\s+soft\s+core\s+unlimited\s?', contents):
        contents += '\novs soft core unlimited'
    file_write(limits_file, '{0}\n'.format(contents))

root_ssh_folder = '{0}/.ssh'.format(check_output('echo ~root', shell=True).strip())
ovs_ssh_folder = '{0}/.ssh'.format(check_output('echo ~ovs', shell=True).strip())
private_key_filename = '{0}/id_rsa'
authorized_keys_filename = '{0}/authorized_keys'
known_hosts_filename = '{0}/known_hosts'
# Generate keys for root
if not os.path.exists(private_key_filename.format(root_ssh_folder)):
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
