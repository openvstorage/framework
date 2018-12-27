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
from ovs_extensions.constants.scripts import BASE_SCRIPTS
from IPython import embed


"""
Not something that we're proud of but it has to be this way :(
The unittest do not require any implementation to run, everything gets mocked
However when loading in all other commands, the imports might/do fetch instances of real implementation
Which don't do anything or cannot be instantiated
Thus we have to import controllers whenever we invoke a command :(
"""
import subprocess


@click.group(name='ovs', help='Open the OVS python shell', invoke_without_command=True)
@click.pass_context
def ovs(ctx):
    if ctx.invoked_subcommand is None:
        embed()
    if ctx.invoked_subcommand in [i.rstrip('.sh') for i in os.listdir(BASE_SCRIPTS)]:
        file = os.path.join(BASE_SCRIPTS, '{0}.sh'.format(ctx.invoked_subcommand))
        print subprocess.check_output([file])

    # else:
    # Do nothing: invoke subcommand


groups = [setup_group, config_group, rollback, update, remove_group, monitor_group, unittest, start_group, stop_group, collect_logs]
for group in groups:
    ovs.add_command(group)

# Allow for external scripts to be run as command
for extra_group in [file for file in os.listdir(BASE_SCRIPTS) if os.path.isfile(os.path.join(BASE_SCRIPTS, file))]:
    script_path = os.path.join(BASE_SCRIPTS, extra_group)
    script_name = extra_group.strip('.sh')
    command = click.Command(name=script_name)
    command.allow_extra_args = True
    ignore_unknown_options=True
    ovs.add_command(command)
