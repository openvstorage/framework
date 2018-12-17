import click
from .remove import remove_node

remove_group = click.Group('remove', help='Removal of nodes')

remove_group.add_command(remove_node)