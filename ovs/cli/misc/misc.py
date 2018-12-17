import os
import click


from ovs.lib.nodeinstallation import NodeInstallationController
from ovs.lib.update import UpdateController

@click.command('rollback', help='Roll back a failed OVS install')
def rollback():
    NodeInstallationController.rollback_setup()


@click.command('update', help='Update specified components on all nodes in cluster ')
@click.argument('components', nargs=-1)
def update(components):
    if len(components) == 1:
        components = components[0].split(',')  # for backwards compatiblity: comma-separated list

    components = [str(i) for i in components]
    UpdateController.execute_update(components)  #todo at &>> /var/log/ovs/update.log;
    #todo         at -f /tmp/update now

    os.remove('/tmp/update')

@click.command('unittest', help='Run all or a part of the OVS unittest suite')
@click.argument('path', required=False)
def unittest(path):
    #todo run specific path
    pass

