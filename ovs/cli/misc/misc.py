import os
import click
import datetime
import subprocess


@click.command('rollback', help='Roll back a failed OVS install')
def rollback():
    from ovs.lib.nodeinstallation import NodeInstallationController

    NodeInstallationController.rollback_setup()


@click.command('update', help='Update specified components on all nodes in cluster ')
@click.argument('components', nargs=-1)
def update(components):
    from ovs.lib.update import UpdateController

    if len(components) == 1:
        components = components[0].split(',')  # for backwards compatiblity: comma-separated list

    components = [str(i) for i in components]
    UpdateController.execute_update(components)


@click.command('collect', help='Collect different logfiles from the environment and dump them in .gz')
@click.argument('logs')
def collect_logs(logs):
    from ovs.extensions.generic.system import System

    sr = System.get_my_storagerouter()
    time_string = datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
    tmp_path = '/tmp/ovs-{0}-{1}-logs.tar'.format(sr.name, time_string)
    open(tmp_path, 'a')
    if os.path.isfile(tmp_path):
        os.remove(tmp_path)
    gz_path = os.path.join(tmp_path, 'gz')
    if os.path.isfile(gz_path):
        os.remove(gz_path)
    open(tmp_path, 'a')

    # Make sure all folders exist (tar might make trouble otherwise)
    log_list = ['/var/log/arakoon', '/var/log/nginx', '/var/log/ovs', '/var/log/rabbitmq', '/var/log/upstart', '/var/log/dmesg']
    parsed_string = ''
    for path in log_list:
        if os.path.isfile(path) or os.path.isdir(path):
            parsed_string += '{0}* '.format(path)

    with open('/var/log/journald.log', 'w+') as fh:
        subprocess.check_call('journalctl -u ovs-* -u asd-* -u alba-* --no-pager', stderr=fh, stdout=fh, shell=True)
    subprocess.check_call('tar czfP {0} {1} /var/log/*log --exclude=syslog'.format(tmp_path, parsed_string), shell=True)
    print 'Files stored in {0}'.format(tmp_path)


