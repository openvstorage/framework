import os
import uuid
import click
import subprocess

from ovs.extensions.generic.configuration import Configuration
#todo debug config managemtn
@click.command('edit', help='Use NANO te edit a config value manually')
@click.argument('path')
def edit(path):
    path = str(path)
    tmp_path = '/tmp/{0}'.format(uuid.uuid4())
    with open(tmp_path, 'w') as f:
        if Configuration.exists(path, raw=True):
            f.write(Configuration.get(path, raw=True))
        else:
            f.write('')
    subprocess.call(['nano', tmp_path])
    with open(tmp_path, 'r') as f:
        Configuration.set(path, f.read(), raw=True)
    os.remove(tmp_path)

@click.command('list', help='List values of the configmanagement from path')
@click.argument('path') #todo fix empty path
def list(path):
    path = str(path)
    for entry in Configuration.list(path):
        print entry

@click.command('list-recursive', help='Recursively list values of the configmanagement from path')
@click.argument('path')
def list_recursive(path):
    path = str(path)
    for entry in Configuration.list(path, recursive=True):
        print entry

@click.command('get', help='Fetch value of the configmanagement from path')
@click.argument('path')
def get(path):
    path = str(path)
    if Configuration.exists(path, raw=True):
        print Configuration.get(path, raw=True)