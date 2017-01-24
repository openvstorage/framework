#!/usr/bin/env python2
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

""" OpenvStorage core post installation module """

import os
import re
from subprocess import check_output

if __name__ == '__main__':
    SECRET_KEY_LENGTH = 50

    dist_info = check_output('cat /etc/os-release', shell=True)
    if 'CentOS Linux' in dist_info:
        ssh_service = 'sshd'
    else:  # Default fallback to Ubuntu in this case
        ssh_service = 'ssh'

    def file_read(fn):
        """ Read file """
        with open(fn, 'r') as the_file:
            return the_file.read().strip()

    def file_write(fn, cts):
        """ Write file """
        with open(fn, 'w') as the_file:
            the_file.write(cts)

    # TODO: set owner:group only where it is really needed
    check_output('chown -R ovs:ovs /opt/OpenvStorage', shell=True)
    # Cleanup *.pyc files to make sure that on update old obsolete pyc files are removed
    check_output('find /opt/OpenvStorage -name *.pyc -exec rm -f {} \;', shell=True)

    # Configure logging
    check_output('chmod 755 /opt/OpenvStorage/scripts/system/rotate-storagedriver-logs.sh', shell=True)
    if not os.path.exists('/etc/rsyslog.d/90-ovs.conf') or '$KLogPermitNonKernelFacility on' not in file_read('/etc/rsyslog.d/90-ovs.conf'):
        check_output('echo "\$KLogPermitNonKernelFacility on" > /etc/rsyslog.d/90-ovs.conf', shell=True)
        check_output('service rsyslog restart', shell=True)

    # Configure SSH
    config_file = '/etc/ssh/sshd_config'
    ssh_content_before = None
    if os.path.isfile(config_file):
        ssh_content_before = file_read(config_file)
        use_dns = False
        new_contents = []
        for line in file_read(config_file).splitlines():
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
    ssh_content_after = file_read(config_file)
    if ssh_content_after != ssh_content_before:
        check_output('service {0} restart'.format(ssh_service), shell=True)

    # Configure core-dumps
    limits_file = '/etc/security/limits.conf'
    if os.path.isfile(limits_file):
        contents = file_read(limits_file)
        if not re.search('\s?root\s+soft\s+core\s+unlimited\s?', contents):
            contents += '\nroot soft core unlimited'
        if not re.search('\s?ovs\s+soft\s+core\s+unlimited\s?', contents):
            contents += '\novs soft core unlimited'
        file_write(limits_file, '{0}\n'.format(contents))

