import click
from .setup import master
from .setup import extra
from .setup import promote
from .setup import demote
from ovs.lib.nodeinstallation import NodeInstallationController

# This group will be exported to the main CLI interface


@click.group('setup', help='all setup related functionality', invoke_without_command=True)
@click.option('--rollback-on-failure', help='Rollback on failure', flag_value=True, default=False)
@click.pass_context
def setup_group(ctx, rollback_on_failure):
    if ctx.invoked_subcommand is None:
        NodeInstallationController.setup_node(execute_rollback=rollback_on_failure)
    # else:
        # Do nothing: invoke subcommand


# Attach commands to this group
setup_group.add_command(master)
setup_group.add_command(extra)
setup_group.add_command(promote)
setup_group.add_command(demote)
