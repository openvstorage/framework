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
from .setup import master
from .setup import extra
from .setup import promote
from .setup import demote
from ..commands import configure_cli_logging
from ovs_extensions.cli import OVSGroup
# This group will be exported to the main CLI interface


@click.group('setup',
             help='Launch Open vStorage setup and autodetect required node role (master/extra) and optionally rollback if setup would fail',
             invoke_without_command=True, short_help='ovs setup [--rollback-on-failure]', cls=OVSGroup)
@click.option('--rollback-on-failure', help='Rollback on failure', flag_value=True, default=False)
@click.pass_context
def setup_group(ctx, rollback_on_failure):
    if ctx.invoked_subcommand is None:
        configure_cli_logging()
        from ovs.lib.nodeinstallation import NodeInstallationController
        NodeInstallationController.setup_node(execute_rollback=rollback_on_failure)
    # else:
        # Do nothing: invoke subcommand


# Attach commands to this group
setup_group.add_command(master)
setup_group.add_command(extra)
setup_group.add_command(promote)
setup_group.add_command(demote)
