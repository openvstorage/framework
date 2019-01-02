import click
from ovs_extensions.cli import OVSCommand


@click.command('node', help='Remove node from cluster', command_parameter_help='<ip>', cls=OVSCommand)
@click.argument('IP')
@click.option('--force-yes', required=False, default=False, is_flag=True)
def remove_node(ip, silent):
    from ovs.lib.noderemoval import NodeRemovalController
    NodeRemovalController.remove_node(node_ip=ip, silent=silent)

