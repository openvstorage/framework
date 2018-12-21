import click
from .services import framework_start, framework_stop

start_group = click.Group('start', help='(Re)Start framework services')
start_group.add_command(framework_start)

stop_group = click.Group('stop', help='Stop framework services')
stop_group.add_command(framework_stop)
