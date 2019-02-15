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


@click.command('mds', help='Watch Open vStorage MDS clusters for all vPools per StorageRouter', **extra_options)
def mds():
    from ovs.lib.mdsservice import MDSServiceController
    MDSServiceController.monitor_mds_layout()


@click.command('services', help='Watch Open vStorage services', **extra_options)
def services():
    from ovs.extensions.services.servicefactory import ServiceFactory
    ServiceFactory.get_manager().monitor_services()


@click.command('heartbeat', help='Send an internal heartbeat', **extra_options)
def heartbeat():
    from ovs.extensions.generic.heartbeat import HeartBeat
    HeartBeat.pulse()
