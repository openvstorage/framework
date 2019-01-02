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

from .monitor import mds, services, heartbeat
from ovs_extensions.cli import OVSGroup

monitor_group = OVSGroup('monitor', help='Monitor several aspects of the framework')

monitor_group.add_command(mds)
monitor_group.add_command(services)
monitor_group.add_command(heartbeat)
