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

"""
Not something that we're proud of but it has to be this way :(
The unittest do not require any implementation to run, everything gets mocked
However when loading in all other commands, the imports might/do fetch instances of real implementation
Which don't do anything or cannot be instantiated
Thus we have to import controllers whenever we invoke a command :(
"""

import os
# All CLI commands should output logging to the file to avoid cluttering
os.environ['OVS_LOGTYPE_OVERRIDE'] = 'file'

import click
from .setup import setup_group
from .config import config_group
from .misc import misc_group
from .remove import remove_group
from .monitor import monitor_group
from .services import services_group
from .update import update_command
from .rollback import rollback_command
from ovs_extensions.cli import OVSCLI, unittest_command
from IPython import embed


@click.group(name='ovs', help='Open the OVS python shell or run an ovs command', invoke_without_command=True, cls=OVSCLI)
@click.pass_context
def ovs(ctx):
    if ctx.invoked_subcommand is None:
        embed()
    # Documentation purposes:
    # else:
    # Do nothing: invoke subcommand


# groups = [setup_group, config_group, rollback_command, update_command, remove_group, monitor_group, unittest_command, services_group, misc_group]
groups = [setup_group, config_group, rollback_command, update_command, remove_group, monitor_group, unittest_command, misc_group]
for group in groups:
    print group.name
    ovs.add_command(group)
