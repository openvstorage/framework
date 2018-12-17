import click
from .collect import logs

collect_group = click.Group('remove', help='Removal of nodes')

collect_group.add_command(logs)