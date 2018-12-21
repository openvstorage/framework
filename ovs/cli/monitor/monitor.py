import click

@click.command('mds')
def mds():
    from ovs.lib.mdsservice import MDSServiceController
    MDSServiceController.monitor_mds_layout()


@click.command('services')
def services():
    from ovs.extensions.services.servicefactory import ServiceFactory

    ServiceFactory.get_manager().monitor_services()


@click.command('heartbeat')
def heartbeat():
    from ovs.extensions.generic.heartbeat import HeartBeat

    HeartBeat.pulse()
