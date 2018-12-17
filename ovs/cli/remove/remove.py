import click
from ovs.lib.noderemoval import NodeRemovalController


@click.command('node')
@click.argument('IP')
@click.argument('silent', required=False, default=False, type=click.BOOL)
def remove_node(ip, silent):
    silent = '--force-yes' if silent else None  #todo change in code van controller
    NodeRemovalController.remove_node(node_ip=ip, silent=silent)
