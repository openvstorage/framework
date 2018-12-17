import click
from .setup import setup_group
from .config import config_group
from .misc.misc import rollback, update, unittest
from .remove import remove_group
from .monitor import monitor_group
from .collect import collect_group

import subprocess

@click.group('OVS CLI', help='Open the OVS python shell', invoke_without_command=True)
@click.pass_context
def ovs(ctx):
    if ctx.invoked_subcommand is None:
        subprocess.call(['ipython'])

    # else:
        # Do nothing: invoke subcommand


groups = [setup_group, config_group, rollback, update, remove_group, monitor_group, unittest, collect_group]
for group in groups:
    ovs.add_command(group)