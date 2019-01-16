# Copyright (C) 2019 iNuron NV
#
# This file is part of Open vStorage Open Source Edition (OSE),
# as available from
#
#      http://www.openvstorage.org and
#      http://www.openvstorage.com.
#
# This file is free software; you can redistribute it and/or modify it
# under the terms of the GNU Affero General Public License v3 (GNU AGPLv3)
# as published by the Free Software Foundation, in version 3 as it comes
# in the LICENSE.txt file of the Open vStorage OSE distribution.
#
# Open vStorage is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY of any kind.

import click
from .misc import collect_logs as _collect_logs
from ovs_extensions.cli.commands import OVSGroup, OVSCommand


extra_options = {'section_header':'Miscellaneous',
                 'cls': OVSCommand}


@click.command('collect', help='Collect all ovs logs to a tarball (for support purposes)',
               command_parameter_help= 'logs', **extra_options)
@click.argument('logs', required=True, type=click.STRING)
def collect_logs(logs):
    _collect_logs()


@click.command('version', help='List all ovs packages and their respective versions',
               **extra_options)
def version_command():
    from ovs.extensions.packages.packagefactory import PackageFactory
    mgr = PackageFactory.get_manager()
    for package, version in mgr.get_installed_versions().iteritems():
        print '{0: <30} {1}'.format(package,version)
