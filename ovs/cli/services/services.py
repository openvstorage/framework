import click
import re
from ovs.extensions.generic.system import System
from ovs.dal.lists.storagerouterlist import StorageRouterList
from ovs.extensions.generic.sshclient import SSHClient, UnableToConnectException
from ovs.extensions.services.servicefactory import ServiceFactory
from ovs.lib.helpers.toolbox import Toolbox

@click.command('framework')
@click.argument('host', required=False, default=None, type=click.STRING)
def framework_stop(host):
    storagerouter_list = _get_storagerouter_list(host)
    print 'Stopping...'
    for storagerouter in storagerouter_list:
        try:
            client = SSHClient(storagerouter, username='root')
            ServiceFactory.get_manager().stop_service('watcher-framework', client)
        except UnableToConnectException:
            print '{0} on {1}... failed (Node unreachable)'.format('Stopping', storagerouter.name)
            continue
    print 'Done'

@click.command('framework')
@click.argument('host', required=False, default=None, type=click.STRING)
def framework_start(host):
    storagerouter_list = _get_storagerouter_list(host)
    print 'Starting...'
    for storagerouter in storagerouter_list:
        try:
            client = SSHClient(storagerouter, username='root')
            ServiceFactory.get_manager().start_service('watcher-framework', client)
        except UnableToConnectException:
            print '{0} on {1}... failed (Node unreachable)'.format('Starting', storagerouter.name)
            continue

    print 'Done'

def _get_storagerouter_list(host):
    storagerouter_list = []
    if not host:
        storagerouter_list = [System.get_my_storagerouter()]
    else:
        if re.match(Toolbox.regex_ip, host):
            sr = StorageRouterList.get_by_ip(host)
            storagerouter_list = [sr]
        if host == 'all':
            storagerouter_list = sorted(StorageRouterList.get_storagerouters(), key=lambda k: k.name)
        else:
            print 'Invalid argument given. `Host` should be `all|<IP>`'
    return storagerouter_list