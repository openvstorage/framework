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

from .services import framework_start, framework_stop
from ovs_extensions.cli import OVSGroup

start_group = OVSGroup('start', help='(Re)Start framework services')
start_group.add_command(framework_start)

stop_group = OVSGroup('stop', help='Stop framework services')
stop_group.add_command(framework_stop)

services_group = OVSGroup('services', help='Restart services')
services_group.add_command(start_group)
services_group.add_command(stop_group)
