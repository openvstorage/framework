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
from ..commands import OVSCommand

extra_options = {'cls': OVSCommand}


@click.command('volumedriver', help='Update all volumedriver services on the current node', **extra_options)
def volumedriver():
    from ovs.update import VolumeDriverUpdater
    from ovs.extensions.generic.system import System
    VolumeDriverUpdater.do_update(System.get_my_machine_id(), True)

@click.command('alba', help='Update all alba services on the current node', **extra_options)
def alba():
    from ovs.update import AlbaComponentUpdater
    from ovs.extensions.generic.system import System
    AlbaComponentUpdater.do_update(System.get_my_machine_id(), True)
