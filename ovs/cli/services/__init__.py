import click
from .services import framework_start, framework_stop

start_group = click.Group('start', help='(re)start framework services')
start_group.add_command(framework_start)

stop_group = click.Group('stop', help='stop framework services')
stop_group.add_command(framework_stop)
