import click
from .config import edit, list, list_recursive, get

config_group = click.Group('config', help='Use OVS config management')
config_group.add_command(edit)
config_group.add_command(list)
config_group.add_command(list_recursive)
config_group.add_command(get)