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
from .misc import collect_logs
from ovs_extensions.cli.commands import OVSGroup, OVSCommand

collect_group = OVSGroup('collect', help='Collect resources from the cluster')
collect_group.add_command(collect_logs)


@click.command('version', help='List all ovs packages and their respective versions',
               section_header='Miscellaneous', cls=OVSCommand)
def version_command():
    from ovs.extensions.packages.packagefactory import PackageFactory
    mgr = PackageFactory.get_manager()
    for package, version in mgr.get_installed_versions().iteritems():
        print '{0: <30} {1}'.format(package,version)
