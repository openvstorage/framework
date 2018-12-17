import click
from .monitor import mds, services, heartbeat

monitor_group = click.Group('monitor', help='Monitor several aspects of the framework')

monitor_group.add_command(mds)
monitor_group.add_command(services)
monitor_group.add_command(heartbeat)
