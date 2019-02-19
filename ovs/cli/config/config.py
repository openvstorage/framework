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
import uuid
import click
import subprocess
from ..commands import OVSCommand


extra_options = {'command_parameter_help': '[some/key]', 'cls': OVSCommand}


@click.command('edit', help='Opens the contents of \'some/key\' in your $EDITOR, and updates it after editing',
               **extra_options)
@click.argument('path')
def edit(path):
    from ovs.extensions.generic.configuration import Configuration

    path = str(path)
    tmp_path = '/tmp/{0}'.format(uuid.uuid4())
    with open(tmp_path, 'w') as f:
        if Configuration.exists(path, raw=True):
            f.write(Configuration.get(path, raw=True))
        else:
            f.write('')
    subprocess.call(['nano', tmp_path])
    with open(tmp_path, 'r') as f:
        Configuration.set(path, f.read(), raw=True)
    os.remove(tmp_path)


@click.command('list', help='Lists all keys [under \'some\']', **extra_options)
@click.argument('path', required=False, default=None)
def list(path):
    from ovs.extensions.generic.configuration import Configuration

    if path:
        path = str(path)
    else:
        path = '/'
    for entry in Configuration.list(path):
        print entry


@click.command('list-recursive', help='Lists all keys recursively [under \'some\']', **extra_options)
@click.argument('path', required=False, default=None)
def list_recursive(path):
    from ovs.extensions.generic.configuration import Configuration

    if path:
        path = str(path)
    else:
        path = '/'
    for entry in Configuration.list(path, recursive=True):
        print entry


@click.command('get', help='Prints the contents of \'some/key\'', **extra_options)
@click.argument('path')
def get(path):
    from ovs.extensions.generic.configuration import Configuration

    path = str(path)
    if Configuration.exists(path, raw=True):
        print Configuration.get(path, raw=True)
