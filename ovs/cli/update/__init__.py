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
from ovs_extensions.cli import OVSCommand


@click.command('update', help='Update specified components on all nodes in cluster',
               command_parameter_help='<components>', cls=OVSCommand)
@click.argument('components', nargs=-1)
def update_command(components):
    from ovs.lib.update import UpdateController

    if len(components) == 1:
        components = components[0].split(',')  # for backwards compatiblity: comma-separated list

    components = [str(i) for i in components]
    UpdateController.execute_update(components)
