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

extra_options = {'command_parameter_help': '[--rollback-on-failure]', 'cls': OVSCommand}


@click.command('master', help='Launch Open vStorage setup and install master node and optionally rollback if setup would fail',
               **extra_options)
@click.option('--rollback-on-failure', help='Rollback on failure', flag_value=True, default=False)
def master(rollback_on_failure):
    from ovs.lib.nodeinstallation import NodeInstallationController
    NodeInstallationController.setup_node(execute_rollback=rollback_on_failure, node_type='master')


@click.command('extra', help='Launch Open vStorage setup and install extra node and optionally rollback if setup would fail',
               **extra_options)
@click.option('--rollback-on-failure', help='Rollback on failure', flag_value=True, default=False)
def extra(rollback_on_failure):
    from ovs.lib.nodeinstallation import NodeInstallationController
    NodeInstallationController.setup_node(execute_rollback=rollback_on_failure, node_type='extra')


@click.command('promote', help='Promote this node (extra -> master) and optionally rollback if promote would fail',
               **extra_options)
@click.option('--rollback-on-failure', help='Rollback on failure', flag_value=True, default=False)
def promote(rollback_on_failure):
    from ovs.lib.nodetype import NodeTypeController
    from ovs.extensions.generic.system import System
    NodeTypeController.promote_or_demote_node(node_action='promote', cluster_ip=System.get_my_storagerouter().ip, execute_rollback=rollback_on_failure)


@click.command('demote', help='Demote this node (master -> extra) and optionally rollback if demote would fail',
               **extra_options)
@click.option('--rollback-on-failure', help='Rollback on failure', flag_value=True, default=False)
def demote(rollback_on_failure):
    from ovs.lib.nodetype import NodeTypeController
    from ovs.extensions.generic.system import System
    NodeTypeController.promote_or_demote_node(node_action='demote', cluster_ip=System.get_my_storagerouter().ip, execute_rollback=rollback_on_failure)
