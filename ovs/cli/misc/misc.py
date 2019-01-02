# Copyright (C) 2019 iNuron NV
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

import os
import click
import datetime
import subprocess


@click.command('collect', help='Collect all ovs logs to a tarball (for support purposes)')
def collect_logs():
    from ovs.extensions.generic.system import System

    sr = System.get_my_storagerouter()
    time_string = datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
    tmp_path = '/tmp/ovs-{0}-{1}-logs.tar'.format(sr.name, time_string)
    open(tmp_path, 'a')
    if os.path.isfile(tmp_path):
        os.remove(tmp_path)
    gz_path = os.path.join(tmp_path, 'gz')
    if os.path.isfile(gz_path):
        os.remove(gz_path)
    open(tmp_path, 'a')

    # Make sure all folders exist (tar might make trouble otherwise)
    log_list = ['/var/log/arakoon', '/var/log/nginx', '/var/log/ovs', '/var/log/rabbitmq', '/var/log/upstart', '/var/log/dmesg']
    parsed_string = ''
    for path in log_list:
        if os.path.isfile(path) or os.path.isdir(path):
            parsed_string += '{0}* '.format(path)

    with open('/var/log/journald.log', 'w+') as fh:
        subprocess.check_call('journalctl -u ovs-* -u asd-* -u alba-* --no-pager', stderr=fh, stdout=fh, shell=True)
    subprocess.check_call('tar czfP {0} {1} /var/log/*log --exclude=syslog'.format(tmp_path, parsed_string), shell=True)
    print 'Files stored in {0}'.format(tmp_path)


