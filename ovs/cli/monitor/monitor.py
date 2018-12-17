import click
from ovs.extensions.generic.heartbeat import HeartBeat
from ovs.extensions.services.servicefactory import ServiceFactory
from ovs.lib.mdsservice import MDSServiceController

@click.command('mds')
def mds():
    MDSServiceController.monitor_mds_layout()


@click.command('services')
def services():
    ServiceFactory.get_manager().monitor_services()


@click.command('heartbeat')
def heartbeat():
    HeartBeat.pulse()