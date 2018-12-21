import click


@click.command('node')
@click.argument('IP')
@click.option('--force-yes', required=False, default=False, is_flag=True)
def remove_node(ip, silent):
    from ovs.lib.noderemoval import NodeRemovalController

    NodeRemovalController.remove_node(node_ip=ip, silent=silent)

