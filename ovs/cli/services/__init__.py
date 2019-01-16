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

import click
from .services import framework_start as _framework_start, framework_stop as _framework_stop
from ovs_extensions.cli import OVSCommand

extra_options = {'command_parameter_help': 'framework [ip|all]',
                 'cls': OVSCommand}


@click.command('stop', help='(Re)start Open vStorage Framework services on this node, on all nodes, or on the given ip',
               section_header='Services', **extra_options)
@click.argument('framework', required=True, type=click.STRING)
@click.argument('host', required=False, default=None, type=click.STRING)
def framework_stop(framework, host):
    _framework_stop(host)


@click.command('start', help='Stop Open vStorage Framework services on this node, on all nodes, or on the given ip',
               section_header='Services', **extra_options)
@click.argument('framework', required=True, type=click.STRING)
@click.argument('host', required=False, default=None, type=click.STRING)
def framework_start(framework, host):
    _framework_start(host)
