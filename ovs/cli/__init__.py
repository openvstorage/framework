import os
# All CLI commands should output logging to the file to avoid cluttering
os.environ['OVS_LOGTYPE_OVERRIDE'] = 'file'

import click
from .setup import setup_group
from .config import config_group
from .misc.misc import rollback, update, collect_logs
from .remove import remove_group
from .monitor import monitor_group
from .services import start_group, stop_group
from ovs_extensions.cli.cli import unittest
from IPython import embed


"""
Not something that we're proud of but it has to be this way :(
The unittest do not require any implementation to run, everything gets mocked
However when loading in all other commands, the imports might/do fetch instances of real implementation
Which don't do anything or cannot be instantiated
Thus we have to import controllers whenever we invoke a command :(
"""

@click.group(name='ovs', help='Open the OVS python shell', invoke_without_command=True)
@click.pass_context
def ovs(ctx):
    if ctx.invoked_subcommand is None:
        embed()
    base_path = '/opt/OpenvStorage/scripts/'
    print os.listdir(base_path)
    print ctx.invoked_subcommand
    if ctx.invoked_subcommand in os.listdir(base_path):
        file = os.path.join(base_path, ctx.invoked_subcommand, 'scripts')
        execfile(file)

    # else:
        # Do nothing: invoke subcommand


groups = [setup_group, config_group, rollback, update, remove_group, monitor_group, unittest, start_group, stop_group, collect_logs]
for group in groups:
    ovs.add_command(group)

 # Allow for plugins to be run as command
for extra_group in os.listdir('/opt/OpenvStorage/scripts/'):
    command = click.Command(name=extra_group)
    ovs.add_command(command)
