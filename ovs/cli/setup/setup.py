import click


@click.command('master', short_help='Assign this node a Master role')
@click.option('--rollback-on-failure', help='Rollback on failure', flag_value=True, default=False)
def master(rollback_on_failure):
    from ovs.lib.nodeinstallation import NodeInstallationController
    NodeInstallationController.setup_node(execute_rollback=rollback_on_failure, node_type='master')


@click.command('extra', help='Assign this node an Extra role')
@click.option('--rollback-on-failure', help='Rollback on failure', flag_value=True, default=False)
def extra(rollback_on_failure):
    from ovs.lib.nodeinstallation import NodeInstallationController
    NodeInstallationController.setup_node(execute_rollback=rollback_on_failure, node_type='extra')


@click.command('promote', help='Promote this or a provided hosts node')
@click.argument('host', required=False)
@click.option('--rollback-on-failure', help='Rollback on failure', flag_value=True, default=False)
def promote(host, rollback_on_failure):
    from ovs.lib.nodetype import NodeTypeController
    NodeTypeController.promote_or_demote_node(node_action='promote', cluster_ip=host, execute_rollback=rollback_on_failure)


@click.command('demote', help='Demote this or a provided hosts node')
@click.argument('host', required=False)
@click.option('--rollback-on-failure', help='Rollback on failure', flag_value=True, default=False)
def demote(host, rollback_on_failure):
    from ovs.lib.nodetype import NodeTypeController
    NodeTypeController.promote_or_demote_node(node_action='demote', cluster_ip=host, execute_rollback=rollback_on_failure)


